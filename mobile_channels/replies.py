"""Durably collect late reply fragments without resubmitting completed turns."""
from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Callable

from .delivery import DeliveryStore
from .models import OwnershipError, StateError
from .storage import Store


SCHEMA = '''
CREATE TABLE IF NOT EXISTS executor_reply_watches (
    task_id TEXT PRIMARY KEY REFERENCES tasks(id),
    attempt_id TEXT NOT NULL REFERENCES execution_attempts(id),
    descriptor_json TEXT NOT NULL,
    next_poll_at REAL NOT NULL, expires_at REAL NOT NULL,
    owner TEXT, token INTEGER NOT NULL DEFAULT 0, lease_until REAL
);
CREATE INDEX IF NOT EXISTS reply_watch_due ON executor_reply_watches(next_poll_at);
'''


class Replies:
    """Lease one history read per tick and append only unseen reply fragments."""

    def __init__(self, store: Store, *, poll_interval: float = 10, ttl: float = 900,
                 lease_seconds: float = 45, chunk_limit: int = 1000):
        """Create optional tables while preserving the base schema version."""
        if min(poll_interval, ttl, lease_seconds, chunk_limit) <= 0:
            raise ValueError('positive polling, retention, lease and chunk sizes are required')
        self.store = store
        self.connection = store.connection
        self.poll_interval, self.ttl = poll_interval, ttl
        self.lease_seconds, self.chunk_limit = lease_seconds, chunk_limit
        self.owner = 'replies-' + uuid.uuid4().hex
        self.delivery = DeliveryStore(store)
        with store.transaction():
            for statement in SCHEMA.split(';'):
                if statement.strip():
                    self.connection.execute(statement)

    def register(self, task_id: str, attempt_id: str, descriptor: dict) -> None:
        """Register alongside result completion in the caller's transaction.

        The descriptor binds session_id, track, baseline_seq and ack_id.
        It carries seen_ids from the initial result and linked_ids from
        its user ACK, confirmed folds and reply ancestry. anchor_seq and
        max_seq retain sequence information when old history ages out.
        """
        if (not isinstance(descriptor, dict) or not isinstance(descriptor.get('session_id'), str)
                or not descriptor['session_id'].strip()):
            raise ValueError('a bound history session is required')
        if descriptor.get('track') not in ('native', 'gadget'):
            raise ValueError('a supported execution track is required')
        state = dict(descriptor)
        for name in ('seen_ids', 'linked_ids'):
            values = state.get(name, [])
            if (not isinstance(values, (list, set, tuple))
                    or any(not isinstance(value, str) or not value for value in values)):
                raise ValueError(f'{name} must contain message identities')
            state[name] = sorted(set(values))
        if not state['seen_ids']:
            raise ValueError('initial reply identities are required to prevent replay')
        state.setdefault('baseline_seq', 0)
        state.setdefault('max_seq', state['baseline_seq'])
        state.setdefault('ack_id', None)
        if state['ack_id'] is not None and not isinstance(state['ack_id'], str):
            raise ValueError('the acknowledged user message identity must be a string')
        for name in ('baseline_seq', 'max_seq', 'anchor_seq', 'parentless_until_seq'):
            value = state.get(name)
            if value is not None and (not isinstance(value, int) or isinstance(value, bool)):
                raise ValueError(f'{name} must be an integer sequence')
        if state['ack_id']:
            state['linked_ids'] = sorted({*state['linked_ids'], state['ack_id']})
        with self.store.transaction():
            task = self.connection.execute(
                'SELECT state,attempt_id FROM tasks WHERE id=?', (task_id,),
            ).fetchone()
            if task is None or task['state'] != 'succeeded' or task['attempt_id'] != attempt_id:
                raise StateError('a reply watch must belong to the completed execution attempt')
            existing = self.connection.execute(
                'SELECT attempt_id FROM executor_reply_watches WHERE task_id=?', (task_id,),
            ).fetchone()
            if existing is not None and existing['attempt_id'] != attempt_id:
                raise StateError('a reply watch cannot replace its execution identity')
            self.connection.execute(
                'INSERT OR IGNORE INTO executor_reply_watches '
                '(task_id,attempt_id,descriptor_json,next_poll_at,expires_at) VALUES (?,?,?,?,?)',
                (task_id, attempt_id, json.dumps(state, ensure_ascii=False),
                 self.store.clock(), self.store.clock() + self.ttl),
            )

    def tick(self, fetch_history: Callable[[dict], list[dict]]) -> str:
        """Fetch one due history outside transactions, then atomically append it."""
        watch = self._claim()
        if watch is None:
            return 'idle'
        try:
            events = fetch_history(watch)
            if not isinstance(events, list):
                raise ValueError('history callback must return events')
            descriptor, fragments = self._collect(watch, events)
            with self.store.transaction():
                self._owned(watch)
                appended = self._append(watch['task_id'], fragments)
                self.connection.execute(
                    'UPDATE executor_reply_watches SET descriptor_json=?,owner=NULL,lease_until=NULL '
                    'WHERE task_id=? AND token=?',
                    (json.dumps(descriptor, ensure_ascii=False), watch['task_id'], watch['token']),
                )
            return 'appended' if appended else 'polled'
        except OwnershipError:
            return 'ownership_lost'
        except Exception:
            with self.store.transaction():
                self.connection.execute(
                    'UPDATE executor_reply_watches SET owner=NULL,lease_until=NULL '
                    'WHERE task_id=? AND owner=? AND token=?',
                    (watch['task_id'], self.owner, watch['token']),
                )
            return 'unknown'

    def _claim(self) -> dict | None:
        """Reserve the oldest due watch and schedule the next read before IO."""
        now = self.store.clock()
        with self.store.transaction():
            row = self.connection.execute(
                'SELECT w.* FROM executor_reply_watches w JOIN tasks t ON t.id=w.task_id '
                "WHERE t.state='succeeded' AND t.attempt_id=w.attempt_id "
                'AND w.next_poll_at<=? AND w.expires_at>? '
                'AND (w.owner IS NULL OR w.lease_until<=?) '
                'ORDER BY w.next_poll_at,t.sequence LIMIT 1', (now, now, now),
            ).fetchone()
            if row is None:
                return None
            token = row['token'] + 1
            self.connection.execute(
                'UPDATE executor_reply_watches SET owner=?,token=?,lease_until=?,next_poll_at=? '
                'WHERE task_id=?',
                (self.owner, token, now + self.lease_seconds,
                 now + self.poll_interval, row['task_id']),
            )
            return dict(json.loads(row['descriptor_json']), task_id=row['task_id'],
                        attempt_id=row['attempt_id'], token=token)

    def _owned(self, watch: dict) -> None:
        """Reject a slow fetch after its lease or execution identity changed."""
        row = self.connection.execute(
            'SELECT w.owner,w.token,w.lease_until,t.state,t.attempt_id '
            'FROM executor_reply_watches w JOIN tasks t ON t.id=w.task_id WHERE w.task_id=?',
            (watch['task_id'],),
        ).fetchone()
        if (row is None or row['owner'] != self.owner or row['token'] != watch['token']
                or row['lease_until'] is None or row['lease_until'] <= self.store.clock()
                or row['state'] != 'succeeded' or row['attempt_id'] != watch['attempt_id']):
            raise OwnershipError('reply watch ownership expired or changed')

    def _collect(self, watch: dict, events: list[dict]) -> tuple[dict, list[tuple]]:
        """Bind completed messages to ACK ancestry and a persistent next-user boundary."""
        # Executors import runtime, which integrates Replies. Resolve the
        # shared media parser only once those modules are fully loaded.
        from .executors import COMPLETED_REPLY_STATES, extract_file_media

        state = {key: value for key, value in watch.items()
                 if key not in ('task_id', 'attempt_id', 'token')}
        seen = set(state['seen_ids'])
        linked = set(state['linked_ids'])
        ack_id = state['ack_id']
        anchor = state.get('anchor_seq')
        ordered = sorted(events, key=lambda event: event.get('seq', 0))
        if anchor is None:
            for event in ordered:
                if (event.get('event_name') == 'message.user'
                        and event.get('seq', 0) > state['baseline_seq']
                        and (not ack_id or event.get('message_id') == ack_id)):
                    anchor = event.get('seq', 0)
                    ack_id = event.get('message_id') or ack_id
                    state.update(anchor_seq=anchor, ack_id=ack_id)
                    if ack_id:
                        linked.add(ack_id)
                    break
        if anchor is None:
            return state, []
        boundary = state.get('parentless_until_seq')
        fragments = []
        for event in ordered:
            seq = event.get('seq', 0)
            state['max_seq'] = max(state['max_seq'], seq)
            if seq <= anchor:
                continue
            payload = event.get('payload') or {}
            message_id = event.get('message_id') or payload.get('message_id')
            name = event.get('event_name')
            if name == 'message.user':
                if message_id not in linked:
                    boundary = min(boundary, seq) if boundary is not None else seq
                continue
            parent = (payload.get('reply_to_message_id') or payload.get('parent_message_id')
                      or event.get('reply_to_message_id') or event.get('parent_message_id'))
            if parent:
                if parent not in linked and parent != message_id:
                    continue
                if parent == message_id and boundary is not None and seq >= boundary:
                    continue
            elif boundary is not None and seq >= boundary:
                continue
            assistant = name == 'message.assistant'
            if message_id and (assistant or parent in linked):
                linked.add(message_id)
            if not assistant or payload.get('display_text_ready') is False:
                continue
            if payload.get('status') not in COMPLETED_REPLY_STATES:
                continue
            key = message_id or f'seq:{seq}'
            if key in seen:
                continue
            text, media = extract_file_media(payload.get('display_text'))
            if not (text or media):
                continue
            seen.add(key)
            fragments.append((key, text, media))
        state.update(seen_ids=sorted(seen), linked_ids=sorted(linked))
        if boundary is not None:
            state['parentless_until_seq'] = boundary
        return state, fragments

    def _append(self, task_id: str, fragments: list[tuple]) -> bool:
        """Append stable transport parts while keeping attempted payloads immutable."""
        from .executors import merge_media

        if not fragments:
            return False
        message = self.connection.execute(
            'SELECT * FROM outbound_messages WHERE task_id=?', (task_id,),
        ).fetchone()
        if message is None:
            raise StateError('completed execution has no outbound message')
        if message['state'] == 'cancelled':
            return False
        prepared = self.connection.execute(
            'SELECT MAX(part_index) FROM outbound_parts WHERE outbound_id=?', (message['id'],),
        ).fetchone()[0]
        if prepared is None:
            chunks = [message['text'][start:start + self.chunk_limit]
                      for start in range(0, len(message['text']), self.chunk_limit)] or ['']
            self.delivery.prepare(message['id'], chunks)
            prepared = len(chunks) - 1
        task = self.store.task(task_id)
        all_media = json.loads(task['media_json'] or '[]')
        known_paths = {item['path'] for item in all_media}
        texts = [task['result']] if task['result'] else []
        appended = False
        for key, text, media in fragments:
            media = [item for item in media if item['path'] not in known_paths]
            known_paths.update(item['path'] for item in media)
            if not (text or media):
                continue
            digest = hashlib.sha256(key.encode()).hexdigest()
            chunks = [text[start:start + self.chunk_limit]
                      for start in range(0, len(text), self.chunk_limit)] or ['']
            for index, chunk in enumerate(chunks):
                prepared += 1
                part_id = f"{message['id']}:late:{digest}:{index}"
                self.connection.execute(
                    'INSERT INTO outbound_parts(id,outbound_id,part_index,text,media_json) VALUES (?,?,?,?,?)',
                    (part_id, message['id'], prepared, chunk,
                     json.dumps(media if index == 0 else [], ensure_ascii=False)),
                )
            if text:
                texts.append(text)
            all_media = merge_media(all_media, media)
            self.store._event(task_id, 'late_reply_appended', key)
            appended = True
        self.connection.execute(
            'UPDATE tasks SET result=?,media_json=? WHERE id=?',
            ('\n'.join(texts), json.dumps(all_media, ensure_ascii=False), task_id),
        )
        self.delivery._aggregate(message['id'])
        return appended
