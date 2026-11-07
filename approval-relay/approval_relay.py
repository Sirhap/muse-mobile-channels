#!/usr/bin/env python3
"""Approval relay: surfaces muse.ai egress/connector approvals on the
user's WeChat + WeCom channels, where they can actually be decided.
Polls egress.approvals, notifies channels for new pending items using
prefer_source_else_both (explicit source / sticky recent decision /
env pin; missing evidence still notifies both), consumes
decisions.jsonl (written by the gateways' /a /b /c and aliases) and
submits them via egress.approval.decide.
Credentials: ~/.config/native-bridge/ only. Test mode:
APPROVAL_RELAY_TEST=1 redirects outbox writes to ./shadow/."""
import fcntl
import json, os, sys, time, uuid
import sqlite3
import hashlib
from pathlib import Path
from contextlib import nullcontext

_BASE_DIR = str(Path(__file__).resolve().parent)
if _BASE_DIR not in sys.path:
    sys.path.insert(0, _BASE_DIR)
_ROOT = str(Path(__file__).resolve().parents[1])
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
from channel_common import exclusive_file_lock
from credential_health import append_health_notice_once, credential_fingerprint, failure_kind
from mobile_channels.approvals import decision_allowed
from mobile_channels.modes import lease_from_env, ModeError
from approval_policy import (
    SecurityPolicyEngine,
    PolicyAction,
    RiskLevel,
    get_default_policy_engine,
)

GW2 = None

BASE = os.path.dirname(os.path.abspath(__file__))
CONF = os.environ.get("NATIVE_BRIDGE_CREDENTIALS", str(Path.home() / ".config/native-bridge"))
STATE_F = os.path.join(BASE, "state.json")
STATUS_F = os.path.join(BASE, "status.json")
DECISIONS_F = os.path.join(BASE, "decisions.jsonl")
OFFSET_F = os.path.join(BASE, "decisions.offset")
POLL_SECS = 20
AUTH_RETRY_SECS = 600
MAX_CONNECT_FAILURES = 3
# Debounce 2026-11-07: a single failed poll is network jitter, not an
# outage. Only MAX_CONNECT_FAILURES consecutive failures declare an
# outage (and send one user notice); the same count already exhausts
# the retry budget, so declaration and pause coincide.
STICKY_DECISION_SECS = 30 * 60  # prefer_source sticky window
CHANNEL_ORDER = ("weixin", "wecom")
SUPPORTED_DECISION_CHANNELS = ("weixin", "wecom", "feishu")
TEST = os.environ.get("APPROVAL_RELAY_TEST") == "1"

CHANNELS = {
    "weixin": {"outbox": str(Path(_ROOT) / "weixin-bot" / "state" / "outbox.jsonl"),
               "row": {"to_user_id": os.environ.get("APPROVAL_RELAY_WEIXIN_TO", "")}},
    "wecom": {"outbox": str(Path(_ROOT) / "wecom-bot" / "state" / "outbox.jsonl"),
              "row": {"chatid": os.environ.get("APPROVAL_RELAY_WECOM_CHATID", ""), "chat_type": 1}},
    "feishu": {"outbox": str(Path(_ROOT) / "feishu-bot" / "state" / "outbox.jsonl"),
               "row": {"chat_id": os.environ.get("APPROVAL_RELAY_FEISHU_CHAT_ID", ""), "msg_type": "text"}},
}
DECISION_LABEL = {"allow_once": "已批准（仅这次）",
                  "allow_always": "已批准（永久）",
                  "deny": "已拒绝"}


def log(*a):
    print(f"[approval-relay {time.strftime('%Y-%m-%d %H:%M:%S')}]", *a, flush=True)


def load_state():
    """Load durable decisions; corrupt existing state must stop processing."""
    try:
        with open(STATE_F, encoding="utf-8") as source:
            state = json.load(source)
    except FileNotFoundError:
        return {"next_num": 1, "items": {}}
    if not isinstance(state, dict) or not isinstance(state.get("items"), dict):
        raise ValueError("invalid approval relay state")
    return state


def _atomic_text(path, text):
    """Commit private state with file and directory durability barriers."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp." + uuid.uuid4().hex)
    try:
        with temporary.open("x", encoding="utf-8") as target:
            os.fchmod(target.fileno(), 0o600)
            target.write(text)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, destination)
        descriptor = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def save_state(st):
    """Persist approval attempts before any external decision request."""
    _atomic_text(STATE_F, json.dumps(st, ensure_ascii=False))


def _normalize_channel(name):
    """Return ``weixin`` / ``wecom`` or None when the name is useless."""
    if not isinstance(name, str):
        return None
    key = name.strip().lower()
    return key if key in CHANNELS else None


def _ordered_unique(names):
    """Keep CHANNEL_ORDER, drop unknowns and duplicates."""
    out = []
    for ch in CHANNEL_ORDER:
        if ch in names and ch not in out:
            out.append(ch)
    return out


def _parse_env_channels(raw):
    """Parse APPROVAL_NOTIFY_CHANNELS into CHANNELS subset, or None."""
    if raw is None or str(raw).strip() == "":
        return None
    found = []
    for part in str(raw).split(","):
        ch = _normalize_channel(part)
        if ch is not None:
            found.append(ch)
    found = _ordered_unique(found)
    return found or None


def _sticky_decision_channel(now=None):
    """Most recent decisions.jsonl channel inside the sticky window."""
    now = time.time() if now is None else now
    latest = None  # (ts, channel)
    try:
        with open(DECISIONS_F, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                ch = _normalize_channel(row.get("channel"))
                if ch is None:
                    continue
                try:
                    ts = float(row.get("ts"))
                except (TypeError, ValueError):
                    continue
                if now - ts > STICKY_DECISION_SECS or now - ts < 0:
                    continue
                if latest is None or ts >= latest[0]:
                    latest = (ts, ch)
    except OSError:
        return None
    return None if latest is None else latest[1]


def resolve_notify_channels(item=None, explicit=None):
    """Pick outbox targets: prefer_source_else_both.

    Priority: P0 item.source_channel|channel; P3 env
    APPROVAL_NOTIFY_CHANNELS (covers sticky); P1 sticky decision
    within STICKY_DECISION_SECS; else both. ``explicit`` is an
    optional pre-parsed list used by tests; notify's channels=
    bypasses this function entirely. Returns a CHANNELS subset in
    stable weixin-then-wecom order. Logs notify_reason.
    """
    both = list(CHANNEL_ORDER)
    scope = item.get("decision_scope") if isinstance(item, dict) else None
    if isinstance(scope, dict) and _normalize_channel(scope.get("channel")) and scope.get("chat_id"):
        return [_normalize_channel(scope["channel"])]

    # P0 — explicit source on the approval item (or future upstream).
    if isinstance(item, dict):
        for key in ("source_channel", "channel"):
            ch = _normalize_channel(item.get(key))
            if ch is not None:
                log("notify_reason=explicit", "targets=", ch)
                return [ch]
            raw = item.get(key)
            if raw not in (None, ""):
                # Illegal source: fall through (do not throw).
                break

    if explicit is not None:
        parsed = _ordered_unique(
            [c for c in explicit if _normalize_channel(c)])
        if parsed:
            log("notify_reason=explicit", "targets=", ",".join(parsed))
            return parsed
        log("notify_reason=both_fallback", "targets=", ",".join(both),
            "why=bad_explicit")
        return both

    # P3 — ops pin (overrides sticky, still after P0).
    env_targets = _parse_env_channels(
        os.environ.get("APPROVAL_NOTIFY_CHANNELS"))
    if env_targets is not None:
        log("notify_reason=env", "targets=", ",".join(env_targets))
        return env_targets

    # P1 — sticky last decision channel.
    sticky = _sticky_decision_channel()
    if sticky is not None:
        log("notify_reason=sticky_decision", "targets=", sticky)
        return [sticky]

    log("notify_reason=both_fallback", "targets=", ",".join(both))
    return both


APPROVAL_NOTICES_DDL = (
    "CREATE TABLE IF NOT EXISTS approval_notices ("
    "id TEXT PRIMARY KEY, channel TEXT NOT NULL, "
    "chat_id TEXT NOT NULL DEFAULT '', text TEXT NOT NULL, "
    "state TEXT NOT NULL DEFAULT 'pending' "
    "CHECK(state IN ('pending','delivered','failed')), "
    "created_at REAL NOT NULL)")


def _core_notice_chat_id(channel, item):
    """Chat route for a core-mode notice: explicit scope wins, else the env pin."""
    scope = item.get("decision_scope") if isinstance(item, dict) else None
    if isinstance(scope, dict) and scope.get("channel") == channel and scope.get("chat_id"):
        return str(scope["chat_id"])
    row = CHANNELS[channel]["row"]
    if channel == "wecom":
        return str(row.get("chatid") or "")
    if channel == "feishu":
        return str(row.get("chat_id") or "")
    return str(row.get("to_user_id") or "")


def _write_core_approval_notice(db_path, notice_id, channel, chat_id, text):
    """Append one pending approval notice, accepting replays of its stable ID.

    Raises OSError when the core database is missing or the write
    fails, so the caller keeps the item unnotified and retries.
    """
    if not isinstance(chat_id, str) or not chat_id.strip():
        raise OSError("core approval notice has no chat route")
    if not db_path or not os.path.exists(db_path):
        raise OSError(f"core database is missing: {db_path}")
    try:
        conn = sqlite3.connect(db_path, timeout=5)
    except sqlite3.Error as exc:
        raise OSError(f"core database could not be opened: {db_path}") from exc
    try:
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute(APPROVAL_NOTICES_DDL)
        conn.execute(
            "INSERT INTO approval_notices "
            "(id, channel, chat_id, text, state, created_at) "
            "VALUES (?, ?, ?, ?, 'pending', ?) ON CONFLICT(id) DO NOTHING",
            (notice_id, channel, chat_id, text, time.time()))
        conn.commit()
    except sqlite3.Error as exc:
        raise OSError("core approval notice could not be queued") from exc
    finally:
        conn.close()


def notify(text, tag, approval=None, channels=None, item=None, event_id=None, notice_key=None):
    """Push a notice to selected channels (prefer_source_else_both).

    ``channels`` if given is used as-is (after normalizing); otherwise
    targets come from resolve_notify_channels(item). With ``approval``
    (a new pending item), the WeCom row is a button_interaction
    template card instead of plain text. WeChat keeps the text form.
    The text is kept on the card row as fallback/audit content.
    Missing evidence still notifies both channels.
    ``event_id`` gives health retries a stable id per target; ordinary approval
    notifications use ``notice_key`` when their logical event can be retried.

    A core-mode channel has no legacy outbox loop, so its notice goes
    to the core database's approval_notices table (pending) for the
    core delivery_loop instead of its outbox.jsonl. Legacy-mode
    channels keep the outbox.jsonl path.

    各通道写入独立处理，单渠道部署也能收到通知。全部目标写入
    失败时抛出 OSError，调用方保留未通知状态，等待下一轮重试。
    """
    scope = item.get("decision_scope") if isinstance(item, dict) else None
    if scope is not None:
        # A notification preference can never widen an approval's private route.
        scoped_channel = _normalize_channel(scope.get("channel")) if isinstance(scope, dict) else None
        if scoped_channel is None or not isinstance(scope.get("chat_id"), str) or not scope["chat_id"]:
            raise OSError("approval scope has no valid notification route")
        targets = [scoped_channel]
    elif channels is not None:
        targets = _ordered_unique(
            [c for c in channels if _normalize_channel(c)])
        if not targets:
            targets = resolve_notify_channels(item=item)
        else:
            log("notify_reason=explicit", "targets=", ",".join(targets))
    else:
        targets = resolve_notify_channels(item=item)

    queued = False
    last_error = None
    for ch in targets:
        cfg = CHANNELS[ch]
        row_id = (f"appr-health-{event_id}-{ch}" if event_id is not None
                  else f"appr-{tag}-{uuid.uuid4().hex[:8]}")
        if event_id is None and notice_key is not None:
            identity = json.dumps([notice_key, ch, _core_notice_chat_id(ch, item)], ensure_ascii=False)
            digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]
            row_id = f"appr-core-{tag}-{digest}"
        row = {"id": row_id,
               "queued_at": int(time.time()), "content": text, "text": text}
        if ch == "wecom" and approval is not None:
            row["mode"] = "send_card"
            row["card"] = build_approval_card(**approval)
        elif ch == "feishu" and approval is not None:
            row["mode"] = "send_card"
            row["msg_type"] = "interactive"
            try:
                from feishu_bot.feishu_cards import build_approval_card as build_feishu_card
                exp_s = (approval.get("expires_ms") / 1000.0) if approval.get("expires_ms") else None
                row["card"] = build_feishu_card(
                    approval_id=str(approval.get("num", "")),
                    operator_ids=[],
                    prompt=approval.get("what", ""),
                    reason=approval.get("policy_reason") or approval.get("who", ""),
                    is_high_risk=bool(approval.get("is_high_risk")),
                    expires_at=exp_s,
                )
            except Exception:
                row["mode"] = "send"
                row["msg_type"] = "text"
        else:
            row["mode"] = "send"
        row.update(cfg["row"])
        scope = item.get("decision_scope") if isinstance(item, dict) else None
        if isinstance(scope, dict) and scope.get("channel") == ch and scope.get("chat_id"):
            if ch == "wecom":
                row.update(chatid=scope["chat_id"], chat_type=2 if scope.get("chat_type") == "group" else 1)
            elif ch == "feishu":
                row.update(chat_id=scope["chat_id"], chat_type=scope.get("chat_type", "single"))
            else:
                row["to_user_id"] = scope["chat_id"]
        path = cfg["outbox"]
        lease = None
        try:
            if TEST:
                path = os.path.join(BASE, "shadow", f"{ch}-outbox.jsonl")
                os.makedirs(os.path.dirname(path), exist_ok=True)
            else:
                try:
                    lease = lease_from_env(ch)
                except KeyError:
                    lease = None
            if lease is not None and lease.mode == "core":
                # Core-owned channel: the legacy outbox loop never runs,
                # so the notice goes to the core database for delivery_loop.
                _write_core_approval_notice(
                    lease.settings.get("database"), row_id, ch,
                    _core_notice_chat_id(ch, item), text)
            else:
                if event_id is not None or notice_key is not None:
                    append_health_notice_once(path, row)
                else:
                    with open(path, "a", encoding="utf-8") as output:
                        fcntl.flock(output, fcntl.LOCK_EX)
                        output.write(json.dumps(row, ensure_ascii=False) + "\n")
                        output.flush()
                        os.fsync(output.fileno())
            queued = True
        except (OSError, ValueError, ModeError) as exc:
            last_error = exc
            log("notify write disabled or failed", ch, type(exc).__name__)
        finally:
            if lease is not None:
                lease.close()
    if not queued:
        raise OSError("approval notice could not be queued to any selected channel") from last_error


def _what_field(what, label):
    for line in str(what or "").splitlines():
        if line.startswith(label + "："):
            return line[len(label) + 1:].strip()
    return ""


def build_approval_card(num, who, what, expires_ms, is_high_risk=False, policy_reason=""):
    """Template card for one pending approval (WeCom only).

    Card real estate is small (sub_title_text <= 112 chars, horizontal
    values kept short), so the card carries the decision-critical
    facts — requester, target, explanation, reason, the start of the
    command, and the expiry. The full text stays available via /审批.
    高危审批请求醒目标注 🚨 [高危审批]。
    """
    horiz = []
    if is_high_risk:
        horiz.append({"keyname": "风险", "value": "🚨 高危审批"})
    if policy_reason:
        horiz.append({"keyname": "策略", "value": str(policy_reason)[:60]})
    for label in ("目标", "说明", "原因"):
        value = _what_field(what, label)
        if value:
            horiz.append({"keyname": label, "value": value[:60]})
    cmd = _what_field(what, "命令")
    expiry = f"过期 {fmt_hm(expires_ms)}"
    # Reserve room for the expiry: a long command must not push it
    # past the 112-char sub_title_text limit.
    sub = f"{cmd[:88]}｜{expiry}" if cmd else expiry
    title = f"🚨 [高危审批] #{num}" if is_high_risk else f"待审批 #{num}"
    card = {
        "card_type": "button_interaction",
        "main_title": {"title": title[:26],
                       "desc": str(who or "")[:50]},
        "sub_title_text": sub[:112],
        "task_id": f"appr-{num}",
        "button_list": [
            {"text": "批准·仅这次", "style": 1, "key": "appr_once"},
            {"text": "批准·永久", "style": 1, "key": "appr_always"},
            {"text": "拒绝", "style": 2, "key": "appr_deny"},
        ],
    }
    if horiz:
        card["horizontal_content_list"] = horiz
    return card


def group_key(record):
    """Identity of an approval for de-duplication.

    One shell command can spawn several egress requests (CONNECT,
    then GET, then the same again for a follow-up tool) and each one
    used to push its own near-identical notice to both channels —
    the 2026-10-09 flood where one git push produced approvals
    #3/#4/#5 and #6/#7. Requests with the same command and the same
    host:port are the same decision in practice, so they group under
    the first notified item regardless of scheme/method.
    """
    a = unwrap_record(record)
    cmd = a.get("registered_command") or a.get("observed_command") or ""
    cmd = " ".join(str(cmd).split())
    host = str(a.get("host") or "")
    port = str(a.get("port") or "")
    if not host:
        # Some records carry the target only in path ("github.com:443").
        p = str(a.get("path") or "")
        if p and "/" not in p and ":" in p:
            host, _, pp = p.rpartition(":")
            port = port or pp
        elif p and "/" not in p:
            host = p
    if not cmd and not host:
        return ""
    return f"{cmd}|{host}:{port}"


def group_key_from_what(what):
    """Group key derived from the stored notice text (命令/目标 lines).

    Fallback for items whose record-based key stayed empty: live
    items #8/#9 (2026-10-09) had a fully populated ``what`` but
    group_key "" (sparse first-sight record, key never recomputed),
    so record-based grouping silently never matched them. The notice
    text is what was actually shown to the user, and it carries the
    same identity: the command plus the target host:port.
    """
    cmd = " ".join(_what_field(what, "命令").split())
    target = _what_field(what, "目标")
    host = port = ""
    for tok in target.split():
        if ":" in tok:
            h, _, p = tok.rpartition(":")
            if h and p.isdigit():
                host, port = h, p
        elif tok and "/" not in tok and "." in tok:
            host = tok
    if not cmd and not host:
        return ""
    return f"{cmd}|{host}:{port}"


def effective_group_key(item):
    """The item's group key, backfilled from its notice text if empty."""
    if not isinstance(item, dict):
        return ""
    return item.get("group_key") or group_key_from_what(item.get("what"))


# A follow-on egress request from the same operation (CONNECT, then
# GET, then a retry) often arrives only AFTER the user already
# decided the first one, so simultaneous-pending grouping can never
# catch it — live #8 -> #9, same command and target ~65 s apart, each
# pushed its own card. When the earlier decision was allow_always
# ("永久批准同类"), a same-group arrival inside this window is the
# same operation in practice and is decided automatically instead
# of pushing yet another card.
RECENT_ALWAYS_WINDOW_SECS = 600


def find_recent_always_primary(st, num, item):
    """Do not synthesize decisions for new approval IDs from matching text.

    Platform allow-always policy is enforced by the platform itself. Local
    matching of command/host does not prove authorization for another request.
    """
    return None


def find_group_primary(st, num, item):
    """Num of the already-notified pending item this one groups under."""
    key = effective_group_key(item)
    scope = item.get("decision_scope")
    if not key or not scope:
        return None
    for n, other in sorted(st["items"].items(), key=lambda kv: int(kv[0])):
        if n == num or not isinstance(other, dict):
            continue
        if effective_group_key(other) != key or other.get("decision_scope") != scope:
            continue
        if other.get("status") != "pending" or other.get("grouped_with"):
            continue
        # 安全分级防隐匿：高危审批不可折叠合并在非高危审批项之下
        if bool(item.get("is_high_risk")) != bool(other.get("is_high_risk")):
            continue
        if other.get("notified"):
            if not other.get("group_key"):
                other["group_key"] = key
            return n
    return None


def approval_notice_text(num, item):
    prefix = "🚨 [高危审批]\n" if (item.get("is_high_risk") or item.get("risk_level") == "high") else ""
    reason = item.get("policy_reason") or item.get("decision_reason") or ""
    reason_line = f"原因：{reason}\n" if reason else ""
    return (f"{prefix}{item.get('who', '')}\n{item.get('what', '')}\n"
            f"{reason_line}"
            f"过期：{fmt_hm(item.get('expires_ms'))}\n"
            f"a=仅这次  b=永久  c=拒绝\n"
            f"回 /a {num}｜/b {num}｜/c {num}")


def send_approval_notice(num, item):
    is_high_risk = bool(item.get("is_high_risk") or item.get("risk_level") == "high")
    notify(approval_notice_text(num, item), f"new{num}",
           approval={"num": num, "who": item.get("who", ""),
                     "what": item.get("what", ""),
                     "expires_ms": item.get("expires_ms"),
                     "policy_reason": item.get("policy_reason") or item.get("decision_reason") or "",
                     "is_high_risk": is_high_risk},
           item=item, notice_key=f"pending:{item.get('approval_id') or num}")


def queue_manual_approval(st, num, item):
    """Group or notify a manual approval; leave it unnotified on enqueue failure."""
    primary = find_group_primary(st, num, item)
    if primary is not None:
        item["grouped_with"] = primary
        item["notified"] = True
        log("grouped scoped pending", num, "->", primary)
        return
    try:
        send_approval_notice(num, item)
    except OSError:
        log("approval notice could not be queued", num)
    else:
        item["notified"] = True
        log("notified pending", num)


def fmt_hm(ms):
    try:
        from datetime import datetime
        from zoneinfo import ZoneInfo
        return datetime.fromtimestamp(int(ms) / 1000,
                                      ZoneInfo("Asia/Shanghai")).strftime("%H:%M")
    except (TypeError, ValueError):
        return "?"


def unwrap_record(record):
    """Return a flat approval record.

    Some responses wrap the record in an ``approval`` object
    (egress.approval does); pending list entries are flat. Non-empty
    values from the nested object win over the wrapper's.
    """
    if not isinstance(record, dict):
        return {}
    nested = record.get("approval")
    if not isinstance(nested, dict):
        return dict(record)
    merged = {k: v for k, v in record.items() if k != "approval"}
    for key, value in nested.items():
        if value not in (None, ""):
            merged[key] = value
    return merged


def merge_records(base, detail):
    """Merge a detail record over a sparse pending record.

    Pending entries are sparse when first emitted (the #1 incident on
    2026-10-08 carried only an id and an expiry), while the detail
    endpoint returns the full record. Non-empty detail values win.
    """
    merged = unwrap_record(base)
    for key, value in unwrap_record(detail).items():
        if value not in (None, ""):
            merged[key] = value
    return merged


def fetch_detail(gw, approval_id):
    """Fetch the full approval record via egress.approval.

    Returns an empty dict on any failure so the caller can hold the
    notification and retry on the next poll instead of sending an
    empty one.
    """
    if gw is None:
        return {}
    try:
        response = gw.call_json(
            "egress.approval",
            path_params={"approval_id": approval_id}, timeout=20)
        return unwrap_record(response)
    except Exception as exc:  # GatewayError and transport errors alike
        log("detail fetch failed", approval_id[:12],
            type(exc).__name__, str(exc)[:100])
        return {}


def summarize(record):
    """Build the (who, what) pair shown in an approval notification.

    ``who`` is the requester label; ``what`` carries every fact the
    user needs to decide without opening the app: the command, the
    target endpoint, the explanation, and the blocking reason.
    """
    a = unwrap_record(record)
    who = (a.get("display_name") or a.get("action_description")
           or a.get("policy_subject")
           or a.get("requester_policy_subject") or "未知请求方")
    lines = []
    command = a.get("registered_command") or a.get("observed_command")
    if command:
        lines.append(f"命令：{str(command)[:220]}")
    host = str(a.get("host") or "")
    path = str(a.get("path") or "")
    target = path or host
    if host and path and ":" not in path and a.get("port"):
        target = f"{host}:{a['port']}"
    endpoint_bits = [str(a.get("scheme") or ""), str(a.get("method") or ""),
                     target]
    endpoint = " ".join(bit for bit in endpoint_bits if bit)
    if endpoint:
        lines.append(f"目标：{endpoint}")
    explanation = a.get("rich_explanation") or a.get("short_explanation")
    if explanation:
        lines.append(f"说明：{explanation}")
    if a.get("reason"):
        lines.append(f"原因：{a['reason']}")
    return who, "\n".join(lines)


def has_content(who, what):
    """A notification is meaningful only when it says what is asked.

    An empty ``what`` is exactly the blind-approval bug: never notify
    in that case, however the requester label looks.
    """
    return bool(str(what or "").strip())


def connect():
    """Load optional platform dependencies only when an RPC client is needed."""
    global GW2
    if GW2 is None:
        probe = str(Path(__file__).resolve().parents[1] / "native-probe")
        if probe not in sys.path:
            sys.path.insert(0, probe)
        from gw2 import GW2 as gateway_class
        GW2 = gateway_class
    from muse_cli.gateway import load_cookies
    cookies = load_cookies(os.path.join(CONF, "cookies.txt"))
    with open(os.path.join(CONF, "vm_id.txt"), encoding="utf-8") as source:
        vm_id = source.read().strip()
    if not isinstance(cookies, str) or not cookies.strip() or not vm_id:
        raise ValueError("approval credentials are empty")
    return GW2(cookies=cookies, vm_id=vm_id)


def poll_once(gw, st, response=None, policy_engine=None):
    """Process approvals, optionally using a separately verified read response."""
    if policy_engine is None:
        policy_engine = get_default_policy_engine()
    d = gw.call_json("egress.approvals", timeout=20) if response is None else response
    pending = d.get("pending_approvals") or d.get("pending") or []
    live_ids = set()
    for a in pending:
        record = unwrap_record(a)
        aid = record.get("approval_id")
        if not aid:
            continue
        live_ids.add(aid)
        num = next((n for n, it in st["items"].items()
                    if it.get("approval_id") == aid), None)
        if num is None:
            num = str(st["next_num"])
            st["next_num"] += 1
            st["items"][num] = {
                "approval_id": aid, "who": "未知请求方", "what": "",
                "expires_ms": record.get("request_expires_at_ms"),
                "status": "pending", "first_seen": int(time.time()),
                "notified": False, "group_key": group_key(record),
            }
        item = st["items"][num]
        scope = record.get("decision_scope")
        if isinstance(scope, dict) and not item.get("decision_scope"):
            item["decision_scope"] = scope
        if not item.get("group_key"):
            item["group_key"] = group_key(record)
        if "notified" not in item:
            # Items stored before this fix count as notified only when
            # the stored content was actually meaningful.
            item["notified"] = has_content(item.get("who"),
                                            item.get("what"))
        if record.get("request_expires_at_ms"):
            item["expires_ms"] = record["request_expires_at_ms"]
        if item.get("notified") and not item.get("group_key"):
            # Backfill for items first seen sparse (or stored before
            # grouping existed): without a key they can never become
            # a group primary for later siblings.
            key = (group_key(record) or group_key(fetch_detail(gw, aid))
                   or group_key_from_what(item.get("what")))
            if key:
                item["group_key"] = key
        if not item["notified"] and item.get("status") == "pending":
            attempt = item.get("decision_attempt") or {}
            if attempt.get("operator_id") == "policy_engine" and attempt.get("decision") == "allow_once":
                # A pending item with an earlier auto-allow attempt has already
                # moved to manual review. Retry notification, never the RPC.
                queue_manual_approval(st, num, item)
                continue
            who, what = summarize(record)
            source = record
            if not has_content(who, what):
                # Sparse first-sight record: the command/target only
                # exist in the detail record, so the group key must
                # be (re)computed from the enriched record — computing
                # it from the sparse one yields "" and grouping would
                # silently never apply (seen live on #9, 2026-10-09).
                enriched = merge_records(record, fetch_detail(gw, aid))
                source = enriched
                who, what = summarize(enriched)
            if has_content(who, what):
                item["who"], item["what"] = who, what
                scope = source.get("decision_scope")
                if isinstance(scope, dict) and not item.get("decision_scope"):
                    item["decision_scope"] = scope
                if not item.get("group_key"):
                    item["group_key"] = (group_key(source)
                                         or group_key_from_what(what))

                # 安全策略分级评估
                decision = policy_engine.evaluate(source, what=what)
                item["risk_level"] = decision.risk_level
                item["is_high_risk"] = decision.is_high_risk
                item["policy_rule"] = decision.rule_name
                item["policy_reason"] = decision.reason
                item["decision_reason"] = decision.reason

                if decision.action == PolicyAction.AUTO_ALLOW:
                    # Level 0: 低危免密自动放行
                    # 遵循 ApprovalStore 与 relay 事务机制：RPC 提交前先持久化 decision_unknown 状态，防止网络超时或进程中断重入
                    item["status"] = "decision_unknown"
                    item["decision_attempt"] = {
                        "id": uuid.uuid4().hex,
                        "decision": "allow_once",
                        "operator_id": "policy_engine",
                        "chat_id": "auto",
                        "channel": "auto",
                        "started_at": time.time(),
                        "reason": decision.reason,
                    }
                    save_state(st)
                    try:
                        gw.call_json(
                            "egress.approval.decide",
                            path_params={"approval_id": aid},
                            body={"decision": "allow_once"}, timeout=20)
                    except Exception as exc:
                        # 自动放行提交失败：退到手动审批，不静默卡住。
                        # 状态回 pending（consume_decisions 只接受 pending
                        # 的手动决定；留 decision_unknown 会让用户 /a /b /c
                        # 被当作重复/不确定忽略），发人工通知（分组感知），
                        # decision_attempt 保留供审计。
                        item["status"] = "pending"
                        item["policy_reason"] = (
                            "自动放行提交失败，转人工审批"
                            f"（{type(exc).__name__}）"
                        )
                        save_state(st)
                        queue_manual_approval(st, num, item)
                    else:
                        item["status"] = "auto_approved"
                        item["decided_at"] = int(time.time())
                        item["notified"] = True
                        save_state(st)
                        log("auto approved pending", num, aid[:12], decision.reason)
                else:
                    # 黑名单、普通和高危人工审批都留给用户决定。
                    queue_manual_approval(st, num, item)
            else:
                # Hold the notification: an empty one would ask the
                # user to approve blind. The item stays pending and
                # unnotified, so the next poll retries the enrichment.
                log("holding pending (empty content)", num, aid[:12])
    # Promote grouped items whose primary is no longer pending (it
    # was decided without them, or it vanished): they still need
    # their own notice, otherwise they would expire unseen.
    for num, item in list(st["items"].items()):
        primary_num = item.get("grouped_with")
        if not primary_num or item.get("status") != "pending":
            continue
        if item["approval_id"] not in live_ids:
            continue
        primary = st["items"].get(str(primary_num))
        if primary is not None and primary.get("status") == "pending":
            continue
        item.pop("grouped_with", None)
        item["notified"] = False
        if has_content(item.get("who"), item.get("what")):
            try:
                send_approval_notice(num, item)
            except OSError:
                item["notified"] = False
                log("promoted grouped pending notice could not be queued", num)
            else:
                item["notified"] = True
                log("promoted grouped pending", num)
        else:
            item["notified"] = False
    for num, it in st["items"].items():
        if it.get("status") == "pending" and it["approval_id"] not in live_ids:
            it["status"] = "gone"
    save_state(st)
    return len(pending)


def consume_decisions(gw, st):
    """Consume complete rows; an external decision is never automatically replayed.

    Every decision carries the real inbound operator and route. Explicit source
    scope or configured administrators authorize it; guessed numbers do not.
    """
    with exclusive_file_lock(Path(DECISIONS_F).with_name("decisions.lock")):
        durable = load_state()
        for number, previous in durable["items"].items():
            if (previous.get("status") in ("decision_unknown", "auto_approved", "auto_denied")
                    or str(previous.get("status", "")).startswith("decided:")):
                st["items"][number] = previous
        _flush_decision_notices(st)
        try:
            with open(OFFSET_F, encoding="utf-8") as source:
                off = int(source.read().strip())
        except FileNotFoundError:
            off = 0
        if off < 0:
            raise ValueError("negative approval decision offset")
        try:
            with open(DECISIONS_F, "rb") as source:
                source.seek(off)
                data = source.read()
        except FileNotFoundError:
            return
        new_off = off
        for raw in data.splitlines(keepends=True):
            if not raw.endswith(b"\n"):
                break
            next_off = new_off + len(raw)
            try:
                row = json.loads(raw)
            except (ValueError, UnicodeError):
                log("invalid decision row retained at offset", new_off)
                break
            if not isinstance(row, dict):
                log("invalid decision shape retained at offset", new_off)
                break
            channel = str(row.get("channel", "")).removesuffix("-card")
            if channel not in SUPPORTED_DECISION_CHANNELS:
                log("approval decision rejected: unknown channel", channel)
                new_off = next_off
                continue
            try:
                # Decisions in decisions.jsonl are the single submission
                # path for legacy and core gateways alike: the lease is
                # taken without an expected mode (it still guards the
                # cutover while a submission is in flight) and the row
                # is consumed whatever the channel mode is.
                lease = lease_from_env(channel)
                with lease if lease is not None else nullcontext():
                    _consume_decision_row(gw, st, row)
            except ModeError:
                # The modes document is unreadable: keep this row at the
                # offset head so the next cycle retries it instead of
                # silently dropping the user's decision.
                log("channel mode unreadable; decision retained for retry", channel)
                break
            except KeyError:
                _consume_decision_row(gw, st, row)
            _flush_decision_notices(st)
            new_off = next_off
        save_state(st)
        _atomic_text(OFFSET_F, str(new_off))


def _consume_decision_row(gw, st, row):
    """Authorize and commit one decision while its legacy mode lease is held."""
    num = str(row.get("num", "")).strip()
    decision = row.get("decision")
    item = st["items"].get(num)
    if item is None:
        # 兼容按 approval_id 反查 item
        target_aid = str(row.get("approval_id") or num).strip()
        for k, it in st["items"].items():
            if it.get("approval_id") == target_aid:
                num = k
                item = it
                break
    if item is None or not decision_allowed(item, row, time.time()):
        log("approval decision rejected: unauthorized, expired, or invalid", num)
        try:
            notify(f"【审批 #{num}】未提交：来源、操作者或有效期无法验证，请在原会话或 Muse 应用里处理。",
                   f"rejected{num}", channels=[str(row.get("channel", "")).removesuffix("-card")],
                   notice_key=f"rejected:{json.dumps(row, sort_keys=True, ensure_ascii=False)}")
        except OSError:
            log("approval rejection notification could not be queued", num)
        return
    if item.get("status") != "pending":
        log("duplicate or uncertain decision ignored", num, item.get("status"))
        return
    item["status"] = "decision_unknown"
    item["decision_attempt"] = {
        "id": uuid.uuid4().hex,
        "decision": decision,
        "operator_id": str(row.get("operator_id") or ""),
        "chat_id": str(row.get("chat_id") or ""),
        "channel": str(row.get("channel", "")).removesuffix("-card"),
        "started_at": time.time(),
    }
    save_state(st)
    try:
        gw.call_json("egress.approval.decide", path_params={"approval_id": item["approval_id"]},
                     body={"decision": decision}, timeout=20)
    except Exception as error:
        log("approval decision result unknown", num, type(error).__name__)
        message = f"【审批 #{num}】提交结果尚未确认，不会自动重复提交；请在 Muse 应用中核查原审批。"
    else:
        item["status"] = "decided:" + decision
        item["decided_at"] = int(time.time())
        message = f"【审批 #{num}】{DECISION_LABEL[decision]}，平台已确认决定；操作最终结果请以执行反馈为准。"
    # Each approval ID requires its own authorization/attempt. Grouped
    # siblings will be promoted to individual notices by poll_once.
    item["pending_decision_notice"] = {
        "text": message, "channel": str(row["channel"]).removesuffix("-card"),
        "id": f"decision:{item['decision_attempt']['id']}",
    }
    save_state(st)


def _flush_decision_notices(st):
    """Retry confirmed or uncertain decision receipts without replaying decisions."""
    for num, item in st["items"].items():
        notice = item.get("pending_decision_notice")
        if not isinstance(notice, dict):
            continue
        try:
            notify(notice["text"], f"decision{num}", channels=[notice["channel"]],
                   item=item, notice_key=notice["id"])
        except OSError:
            log("decision status notification could not be queued", num)
        else:
            item.pop("pending_decision_notice")
            save_state(st)


def cycle():
    """Serialize the complete poll/decision cycle across supervised processes."""
    with exclusive_file_lock(Path(BASE) / "relay-runtime.lock"):
        _cycle_locked()


def _credential_health(st):
    """Load retry ownership, resetting the budget only when used files change."""
    stamp = credential_fingerprint(CONF, ("cookies.txt", "vm_id.txt"))
    health = st.setdefault("credential_health", {})
    if not isinstance(health, dict):
        raise ValueError("invalid approval credential health state")
    if health.get("fingerprint") != stamp:
        old_outage = bool(health.get("outage_active"))
        incident_id = health.get("incident_id") if old_outage else None
        failure_notice_created = old_outage and bool(health.get("failure_notice_created"))
        last_verified = health.get("last_verified_at")
        pending = health.get("pending_notices", [])
        health.clear()
        health.update(fingerprint=stamp, state="unknown", failures=0,
                      reconnect_exhausted=False, next_attempt_at=0,
                      last_verified_at=last_verified, outage_active=old_outage,
                      pending_notices=pending, incident_id=incident_id or uuid.uuid4().hex,
                      failure_notice_created=failure_notice_created, failure_kind=None)
        save_state(st)
    return health


def _health_notice(health, text, phase):
    """Retain a stable event until an outbox accepts it, including after restart.

    Only called for declared outages (see _connection_failed): a single
    network jitter never reaches this function, so the user sees at most
    one failed+recovered pair per real outage.
    """
    health.setdefault("pending_notices", []).append({
        "id": f"{health['incident_id']}-{phase}", "text": text,
    })


def _connection_failed(st, health, exc, now):
    """Persist bounded retries without storing potentially sensitive exceptions.

    Debounce: failures below MAX_CONNECT_FAILURES are jitter, not an
    outage -- they are counted for the retry budget but declare nothing
    and notify nobody. The outage is declared (and notified once) only
    when the streak reaches the threshold, which is also when retries
    pause. A new streak gets a fresh incident id so its notice can never
    collide with a previous outage's stable id.
    """
    kind = failure_kind(exc)
    count = int(health.get("failures", 0)) + 1
    outage_active = bool(health.get("outage_active"))
    if count == 1 and not outage_active:
        health["incident_id"] = uuid.uuid4().hex
        health["failure_notice_created"] = False
    delay = AUTH_RETRY_SECS if kind in {"auth", "configuration"} else (5 if count == 1 else 10)
    declared = count >= MAX_CONNECT_FAILURES
    health.update(state="failed", failure_kind=kind, failures=count,
                  reconnect_exhausted=declared,
                  next_attempt_at=now + delay, outage_active=outage_active or declared)
    if declared and not health.get("failure_notice_created"):
        if kind == "auth":
            text = ("审批监听认证失败，当前无法读取或提交审批。请在已登录 Muse 的浏览器安装凭证扩展，"
                    "重新导出完整 muse-credentials.zip 文件交给 Muse 安装并验活；只更新 token 无法恢复审批监听。"
                    "重试有次数上限，之后等待 Cookie 或 VM 凭证文件更新。")
        elif kind == "configuration":
            text = ("审批监听凭证文件缺失或格式无效。请把浏览器扩展导出的完整 muse-credentials.zip "
                    "文件交给 Muse 核对并安装；只更新 token 无法恢复审批监听。重试有次数上限。")
        else:
            text = ("审批监听连接异常，已连续 3 次无法读取审批列表，重试已暂停。"
                    "请让 Muse 检查网络、依赖和连接状态，排除问题后通过受控运维方式恢复。")
        _health_notice(health, text, "failed")
        health["failure_notice_created"] = True
    elif not outage_active and not declared:
        log("approval health below declare threshold", count)
    save_state(st)


def _connection_verified(st, health, now):
    """Record a real approval-list RPC independently of later local processing."""
    if health.get("outage_active"):
        _health_notice(health, "审批监听认证和审批列表读取已恢复；聊天收发仍需实际消息验证。", "recovered")
    health.update(state="healthy", failure_kind=None, failures=0,
                  reconnect_exhausted=False, next_attempt_at=0,
                  last_verified_at=now, outage_active=False)
    save_state(st)


def _flush_health_notices(st, health):
    """Retry outbox writes separately from transport health and keep stable ids."""
    notices = health.setdefault("pending_notices", [])
    while notices:
        event = notices[0]
        save_state(st)
        try:
            notify(event["text"], "credential", event_id=event["id"])
        except Exception as exc:
            log("credential health notice not queued", type(exc).__name__)
            return
        notices.pop(0)
        save_state(st)


def _health_status(health):
    """Expose verification evidence, never credential values or exception text."""
    state = health.get("state")
    missing = any(not Path(CONF, name).is_file() for name in ("cookies.txt", "vm_id.txt"))
    status = ("healthy" if state == "healthy" else
              "missing" if missing else "failed" if state == "failed" else "unknown")
    return {"status": status, "failure_kind": health.get("failure_kind"),
            "failures": health.get("failures", 0),
            "reconnect_exhausted": health.get("reconnect_exhausted", False),
            "next_attempt_at": health.get("next_attempt_at", 0),
            "last_verified_at": health.get("last_verified_at")}


def _cycle_locked():
    """Run one cycle while holding the relay process lock."""
    st = load_state()
    health = _credential_health(st)
    gw = None
    n, err = -1, None
    now = time.time()
    try:
        if not health.get("reconnect_exhausted") and now >= health.get("next_attempt_at", 0):
            try:
                gw = connect()
                response = gw.call_json("egress.approvals", timeout=20)
                if not isinstance(response, dict):
                    raise ValueError("invalid approval-list response")
                if response.get("error") or response.get("ok") is False:
                    raise RuntimeError("approval-list request was rejected")
            except Exception as exc:
                _connection_failed(st, health, exc, now)
                err = f"approval connection failed ({health['failure_kind']})"
            else:
                _connection_verified(st, health, now)
                try:
                    n = poll_once(gw, st, response=response)
                    consume_decisions(gw, st)
                except Exception as exc:
                    err = f"approval processing failed ({type(exc).__name__})"
        elif health.get("state") == "failed":
            err = f"approval connection paused ({health.get('failure_kind', 'unknown')})"
    finally:
        if gw is not None:
            try:
                gw.close()
            except Exception as exc:
                log("gateway close failed", type(exc).__name__)
    _flush_decision_notices(st)
    _flush_health_notices(st, health)
    if err:
        log("cycle error", err)
    _atomic_text(STATUS_F, json.dumps({
        "ts": int(time.time()), "pending": n, "last_error": err,
        "credential_sources": {"cookies": _health_status(health)},
        "active_source": "cookies" if health.get("state") == "healthy" else None,
        "last_verified_at": health.get("last_verified_at"),
    }))


def main():
    if "--once" in sys.argv:
        cycle()
        return
    log("approval relay started, poll", POLL_SECS, "s", "TEST" if TEST else "")
    while True:
        cycle()
        time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()
