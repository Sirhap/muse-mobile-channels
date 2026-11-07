// One Gadget turn, driven as a short-lived worker process.
//
// The bridge executor cannot await a reply inside a tick: it submits once
// and polls. This worker owns the whole turn -- connect, send (which creates
// the side chat on first use), subscribe, follow the bound reply to
// completion -- and records the outcome in a result file the executor polls.
// The executor spawns one worker per attempt and kills it on cancel.
//
// Protocol facts (proven live 2026-11-06):
// - A fresh side chat must be created by sending its first message with
//   sendMessage({ sessionId }); subscribing to a not-yet-created side chat
//   fails with /chat/subscribe HTTP 404. So this worker sends first, then
//   subscribes -- the reverse of the library's chat() order.
// - Sending to the main chat (no session id) fails with /chat/stream HTTP
//   500 on this account; side chats work. This worker only ever drives
//   side chats.
// - The reply is bound through MuseTurn: with ownsChat the dedicated
//   session's assistant messages are bound; without it the reply's parent
//   must be the acknowledged user message id.
//
// The worker never prints the message text or any credential value. stderr
// carries lifecycle events only (connection, ack id, terminal state).
import { readFileSync, renameSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { MuseClient } from '../../reference/muse-client/src/client.js';
import { loadCredentials, saveCredentials } from '../../reference/muse-client/src/credentials.js';
import { MuseTurn } from '../../reference/muse-client/src/turn.js';

const TERMINAL_TASK = new Set(['completed', 'failed', 'errored', 'cancelled', 'canceled']);

interface Args {
  session: string;
  messageFile: string;
  out: string;
  timeoutSeconds: number;
  credentialsDir: string;
  vmId?: string;
  // Send-only mode (fold): connect, send the message to the existing
  // session, record the ack and exit WITHOUT subscribing. The running
  // turn's own worker keeps the subscription; a second subscriber
  // would race it. Used by the core fold (official-client steering).
  sendOnly?: boolean;
}

function parseArgs(argv: string[]): Args {
  const values = new Map<string, string>();
  let sendOnly = false;
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]!;
    if (arg === '--send-only') { sendOnly = true; continue; }
    if (!arg.startsWith('--')) throw new Error(`unexpected argument: ${arg}`);
    const key = arg.slice(2);
    const value = argv[++i];
    if (value === undefined || value.startsWith('--')) throw new Error(`missing value for --${key}`);
    values.set(key, value);
  }
  const session = values.get('session');
  const messageFile = values.get('message-file');
  const out = values.get('out');
  if (!session || !messageFile || !out) {
    throw new Error('usage: turn --session <uuid> --message-file <path> --out <path> [--timeout <seconds>] [--credentials <dir>] [--vm <id>] [--send-only]');
  }
  return {
    session,
    messageFile,
    out,
    timeoutSeconds: Number(values.get('timeout') ?? 900),
    credentialsDir: values.get('credentials') ?? join(homedir(), '.config', 'MuseGadgetPair'),
    vmId: values.get('vm'),
    sendOnly,
  };
}

type ResultState = 'running' | 'succeeded' | 'failed' | 'cancelled' | 'unknown' | 'sent';

interface MediaItem {
  kind: 'image' | 'video' | 'file';
  path: string;
  filename: string;
}

interface TurnResult {
  state: ResultState;
  text?: string;
  message_id?: string;
  session_id?: string;
  evidence?: string;
  // [[FILE:...]] references parsed out of the reply text (legacy bridge
  // format). Text never retains a marker once it is parsed into media.
  media?: MediaItem[];
  // True once any assistant message has been observed in this turn.
  // The executor reads it to decide whether a fold would still be
  // integrated (reply not started) or would arrive too late.
  reply_started?: boolean;
}

/** Split [[FILE:path]] markers out of reply text into media items. */
export function extractFileMedia(text: string): { text: string; media: MediaItem[] } {
  const media: MediaItem[] = [];
  const seen = new Set<string>();
  const collect = (raw: string): void => {
    const path = String(raw).trim();
    if (path.startsWith('/') && !seen.has(path)) {
      seen.add(path);
      const filename = path.split('/').at(-1) ?? path;
      const ext = filename.includes('.') ? filename.split('.').at(-1)!.toLowerCase() : '';
      const kind: MediaItem['kind'] = ['jpg', 'jpeg', 'png', 'gif', 'webp'].includes(ext)
        ? 'image'
        : ['mp4', 'mov', 'webm', 'mkv', 'avi'].includes(ext)
          ? 'video'
          : 'file';
      media.push({ kind, path, filename });
    }
  };
  const lines: string[] = [];
  for (const line of text.split('\n')) {
    const stripped = line.trim();
    const onlyMarker = /^\[\[FILE:([^\]\n]+)\]\]$/.exec(stripped);
    if (onlyMarker) {
      // A marker-only line vanishes entirely (legacy bridge behaviour);
      // inline markers are removed in place.
      collect(onlyMarker[1]!);
      continue;
    }
    lines.push(line.replace(/\[\[FILE:([^\]\n]+)\]\]/g, (_match, raw: string) => {
      collect(raw);
      return '';
    }).trimEnd());
  }
  return { text: lines.join('\n').trim(), media };
}

/** Merge every bound reply of one turn into a single text + media list. */
export function mergeBoundReplies(texts: string[]): { text: string; media: MediaItem[] } {
  const parts: string[] = [];
  const media: MediaItem[] = [];
  const seen = new Set<string>();
  for (const raw of texts) {
    const { text, media: found } = extractFileMedia(raw);
    if (text) parts.push(text);
    for (const item of found) {
      if (!seen.has(item.path)) {
        seen.add(item.path);
        media.push(item);
      }
    }
  }
  return { text: parts.join('\n'), media };
}

/** The ack's message id, or null when it is missing/empty.

 * sendMessage is supposed to guarantee a message id (the reference
 * client throws when it is absent), but an empty id must never be
 * treated as "definitely not sent": the caller records unknown, not
 * failed, so no other track re-sends a message that may have landed.
 */
export function validateAckMessageId(ack: { messageId?: unknown }): string | null {
  return typeof ack.messageId === 'string' && ack.messageId.trim() ? ack.messageId.trim() : null;
}

function writeResult(out: string, result: TurnResult): void {
  const tmp = `${out}.tmp-${process.pid}`;
  writeFileSync(tmp, JSON.stringify(result), 'utf8');
  renameSync(tmp, out);
}

export async function main(): Promise<number> {
  const args = parseArgs(process.argv.slice(2));
  if (!Number.isSafeInteger(args.timeoutSeconds) || args.timeoutSeconds < 1) {
    console.error('timeout must be a positive integer number of seconds');
    return 2;
  }
  const startedAt = Date.now();
  let settled: TurnResult | null = null;
  // True once the send has been attempted. A failure before the send
  // (connect / credentials) means nothing could have been delivered,
  // so it is a determinable 'failed'. Once the send was attempted, the
  // message may have reached the server even if the ack was lost, so
  // the outcome is 'unknown', never 'failed': a 'failed' without a
  // message id would make DualTrack re-send on the Native track and
  // double-deliver.
  let sendAttempted = false;
  const TERMINAL_RESULT = new Set(['succeeded', 'failed', 'cancelled', 'unknown']);
  const settle = (next: TurnResult): void => {
    // A terminal result is final. A 'running' placeholder may be overwritten
    // by the terminal outcome; without this the worker would exit leaving
    // the file saying 'running' forever (seen live 2026-11-06).
    if (settled && TERMINAL_RESULT.has(settled.state)) return;
    settled = next;
    try {
      writeResult(args.out, next);
    } catch (error) {
      console.error('cannot write result file:', error instanceof Error ? error.message : String(error));
    }
    console.error(`result: ${next.state}${next.evidence ? ` (${next.evidence})` : ''}`);
  };
  const onSignal = (): void => {
    if (!settled || !TERMINAL_RESULT.has(settled.state)) {
      // Local kill only: there is no server-side cancel in the reference
      // client, so the server turn may keep running after this worker
      // dies. Say so in the evidence; never claim the turn stopped.
      settle({ state: 'cancelled', evidence: 'worker_terminated_local_only_server_turn_may_continue', session_id: args.session });
    }
    process.exit(0);
  };
  process.on('SIGTERM', onSignal);
  process.on('SIGINT', onSignal);

  const text = readFileSync(args.messageFile, 'utf8');
  const deadline = Date.now() + args.timeoutSeconds * 1000;
  let client: MuseClient | null = null;
  try {
    const credentials = await loadCredentials(args.credentialsDir);
    // Persist rotated tokens: the refresh token rotates on every
    // refresh (the old one is invalidated server-side). Without this
    // callback the new tokens live only in this worker's memory and
    // die with the process, so the next worker reads the stale
    // refresh token from pairing.json and gets HTTP 401 -- the
    // pairing then looks dead after the first access token expires
    // (a few hours), even though the device identity is permanent.
    client = await MuseClient.connect({
      credentials,
      vmId: args.vmId,
      onCredentials: (next) => saveCredentials(next, args.credentialsDir),
    });
    console.error(`connected to ${client.vm.id}`);

    const ack = await (async () => {
      sendAttempted = true;
      try {
        return await client.sendMessage(text, { sessionId: args.session });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        settle({
          state: 'unknown',
          evidence: `send_unconfirmed: ${message}`.slice(0, 200),
          session_id: args.session,
        });
        return null;
      }
    })();
    if (ack === null) return 0;
    const messageId = validateAckMessageId(ack);
    if (messageId === null) {
      // No usable ack id: delivery is unconfirmed, not disproven.
      settle({ state: 'unknown', evidence: 'ack_missing_message_id', session_id: args.session });
      return 0;
    }
    console.error(`sent, message id ${messageId}`);
    if (args.sendOnly) {
      // Fold send: the message is in the session; the running turn's
      // own worker keeps the subscription and will see the reply.
      settle({ state: 'sent', message_id: messageId, session_id: args.session });
      return 0;
    }
    settle({ state: 'running', message_id: messageId, session_id: args.session });

    // A subscribe on a just-created side chat can stall server-side (seen
    // live 2026-11-06: the call hung past the turn deadline while the reply
    // was generated anyway). Retry it a few times, aborting only the stuck
    // OPEN: the abort signal stays armed for the subscription's whole life,
    // so a plain AbortSignal.timeout would kill a live subscription 20s in
    // (that is what "Muse request cancelled" was). The winning attempt's
    // controller is kept and never aborted; the connection close ends it.
    let sub: Awaited<ReturnType<MuseClient['subscribe']>> | null = null;
    let subscribeError: unknown = null;
    for (let attempt = 0; attempt < 3 && !sub; attempt++) {
      if (attempt > 0) console.error(`subscribe retry ${attempt + 1}/3`);
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), 20000);
      try {
        sub = await client.subscribe({ sessionId: args.session, signal: ctrl.signal });
      } catch (error) {
        subscribeError = error;
        if (Date.now() >= deadline) break;
      } finally {
        clearTimeout(timer);
      }
    }
    if (!sub) {
      const detail = subscribeError instanceof Error ? subscribeError.message : String(subscribeError ?? 'subscribe timed out');
      settle({ state: 'unknown', evidence: `subscribe_timeout: ${detail}`.slice(0, 200), message_id: messageId, session_id: args.session });
      return 0;
    }
    console.error('subscribed');
    const iterator = sub[Symbol.asyncIterator]();
    let turn = new MuseTurn({ sessionId: args.session, ownsChat: true });
    try {
      turn.acknowledge(ack);
    } catch (error) {
      console.error(`owned acknowledgement rejected (${error instanceof Error ? error.message : String(error)}); falling back to parent-linked binding`);
      turn = new MuseTurn({ sessionId: args.session });
      turn.acknowledge(ack);
    }

    let terminalTaskStatus: string | undefined;
    let timedOut = false;
    let subEnded = false;
    let replyStartedRecorded = false;
    let next = iterator.next();
    void next.catch(() => {});
    while (true) {
      if (turn.complete()) break;
      const remaining = deadline - Date.now();
      if (remaining <= 0) { timedOut = true; break; }
      const delay = turn.completionDelay();
      const wait = Number.isFinite(delay) ? Math.min(delay, remaining) : remaining;
      const raced = await Promise.race([
        next.then((value) => ({ kind: 'event' as const, value })),
        new Promise<{ kind: 'timer' }>((resolve) => setTimeout(() => resolve({ kind: 'timer' }), wait)),
      ]);
      if (raced.kind === 'timer') {
        if (turn.complete()) break;
        if (Date.now() >= deadline) { timedOut = true; break; }
        continue;
      }
      const { value, done } = raced.value;
      if (done) { subEnded = true; break; }
      const event = value;
      if (event.event === 'task.status' && typeof event.payload.status === 'string'
          && TERMINAL_TASK.has(event.payload.status)) {
        terminalTaskStatus = event.payload.status;
      }
      turn.feed(event);
      // Timestamp every subscription event (name + seq + elapsed only,
      // never text): the 2026-11-07 stall took 213-304s per turn while the
      // reply was already in history, and without per-event timings the
      // next stall cannot tell a late terminal event from a stalled stream.
      console.error(`event ${event.event} seq=${event.seq ?? ''} elapsed_ms=${Date.now() - startedAt}`);
      // The reply has started once any assistant message carries text,
      // even before it is done. Record it so the executor can refuse a
      // fold that would arrive too late to be integrated.
      if (!replyStartedRecorded) {
        const started = [...turn.replies.values()].some((r) => r.text && r.text.trim());
        if (started) {
          replyStartedRecorded = true;
          settle({ state: 'running', message_id: messageId, session_id: args.session, reply_started: true });
        }
      }
      next = iterator.next();
      void next.catch(() => {});
    }
    // Close the subscription BEFORE awaiting the iterator's return:
    // return() on an async generator suspended at an await waits for that
    // await to settle, so awaiting return first can hang forever when the
    // subscription is quiet -- a timed-out worker then never writes its
    // result and never exits (seen live in the stall repro: the 5s
    // deadline fired, the timer won the race, and main() still hung).
    // Closing ends the body queue, which unblocks the generator; the 5s
    // race is a belt-and-braces bound so settlement can never stick here.
    sub.close();
    try {
      await Promise.race([
        iterator.return?.() ?? Promise.resolve(),
        new Promise((resolve) => setTimeout(resolve, 5000)),
      ]);
    } catch { /* subscription already closed */ }

    if (timedOut) {
      settle({ state: 'unknown', evidence: 'turn_timeout', message_id: messageId, session_id: args.session });
      return 0;
    }
    if (subEnded && !turn.complete()) {
      settle({ state: 'failed', evidence: 'subscription_ended_before_reply_completed', message_id: messageId, session_id: args.session });
      return 0;
    }
    const bound = [...turn.replies.values()].filter((reply) => reply.bound && reply.done && reply.text.trim());
    if (bound.length) {
      // A turn may split its answer across several assistant messages
      // (the video reference + link arrived in a later message live):
      // merge them all into one text + media list instead of keeping
      // only the last one.
      const merged = mergeBoundReplies(bound.map((reply) => reply.text));
      settle({
        state: 'succeeded',
        text: merged.text,
        media: merged.media.length ? merged.media : undefined,
        message_id: bound.at(-1)!.messageId,
        session_id: args.session,
      });
      return 0;
    }
    if (terminalTaskStatus === 'cancelled' || terminalTaskStatus === 'canceled') {
      settle({ state: 'cancelled', evidence: 'platform_turn_cancelled', message_id: messageId, session_id: args.session });
      return 0;
    }
    if (terminalTaskStatus === 'failed' || terminalTaskStatus === 'errored') {
      settle({ state: 'failed', evidence: `platform_turn_${terminalTaskStatus}`, message_id: messageId, session_id: args.session });
      return 0;
    }
    settle({ state: 'unknown', evidence: 'turn_completed_without_bound_reply', message_id: messageId, session_id: args.session });
    return 0;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`turn failed: ${message}`);
    if (!settled) {
      // Before the send was attempted, nothing could have been
      // delivered: determinable 'failed', safe for a Native retry.
      // Once the send was attempted, delivery is unconfirmed: 'unknown',
      // never 'failed', so no other track re-sends a message that may
      // have landed.
      settle({
        state: sendAttempted ? 'unknown' : 'failed',
        evidence: sendAttempted ? `send_unconfirmed: ${message}`.slice(0, 200) : message.slice(0, 200),
        session_id: args.session,
      });
      return 0;
    }
    // A failure after the acknowledgement was recorded: the message was
    // sent, so the turn's fate is unknown, not failed.
    if (settled.state === 'running') {
      settle({ state: 'unknown', evidence: message.slice(0, 200), message_id: settled.message_id, session_id: args.session });
    }
    return 0;
  } finally {
    try { client?.close(); } catch { /* already closed */ }
    console.error(`elapsed_ms=${Date.now() - startedAt}`);
  }
}

if (import.meta.main) {
  main().then((code) => process.exit(code)).catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  });
}
