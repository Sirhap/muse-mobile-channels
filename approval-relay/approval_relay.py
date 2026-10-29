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
import json, os, sys, time, uuid
sys.path.insert(0, "/home/hatch/workspace/native-probe")
from gw2 import GW2
from muse_cli.gateway import load_cookies, GatewayError

BASE = os.path.dirname(os.path.abspath(__file__))
CONF = "/home/hatch/.config/native-bridge"  # units run with $HOME unset; pin it
STATE_F = os.path.join(BASE, "state.json")
STATUS_F = os.path.join(BASE, "status.json")
DECISIONS_F = os.path.join(BASE, "decisions.jsonl")
OFFSET_F = os.path.join(BASE, "decisions.offset")
POLL_SECS = 20
STICKY_DECISION_SECS = 30 * 60  # prefer_source sticky window
CHANNEL_ORDER = ("weixin", "wecom")
TEST = os.environ.get("APPROVAL_RELAY_TEST") == "1"

CHANNELS = {
    "weixin": {"outbox": "/home/hatch/workspace/weixin-bot/state/outbox.jsonl",
               "row": {"to_user_id": os.environ.get("APPROVAL_RELAY_WEIXIN_TO", "")}},
    "wecom": {"outbox": "/home/hatch/workspace/wecom-bot/state/outbox.jsonl",
              "row": {"chatid": os.environ.get("APPROVAL_RELAY_WECOM_CHATID", ""), "chat_type": 1}},
}
DECISION_LABEL = {"allow_once": "已批准（仅这次）",
                  "allow_always": "已批准（永久）",
                  "deny": "已拒绝"}


def log(*a):
    print(f"[approval-relay {time.strftime('%Y-%m-%d %H:%M:%S')}]", *a, flush=True)


def load_state():
    try:
        return json.load(open(STATE_F))
    except (OSError, ValueError):
        return {"next_num": 1, "items": {}}


def save_state(st):
    tmp = STATE_F + ".tmp"
    json.dump(st, open(tmp, "w"), ensure_ascii=False)
    os.replace(tmp, STATE_F)


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


def notify(text, tag, approval=None, channels=None, item=None):
    """Push a notice to selected channels (prefer_source_else_both).

    ``channels`` if given is used as-is (after normalizing); otherwise
    targets come from resolve_notify_channels(item). With ``approval``
    (a new pending item), the WeCom row is a button_interaction
    template card instead of plain text. WeChat keeps the text form.
    The text is kept on the card row as fallback/audit content.
    Missing evidence still notifies both channels.

    各通道写入独立处理，单渠道部署也能收到通知。全部目标写入
    失败时抛出 OSError，调用方保留未通知状态，等待下一轮重试。
    """
    if channels is not None:
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
        row = {"id": f"appr-{tag}-{uuid.uuid4().hex[:8]}",
               "queued_at": int(time.time()), "content": text}
        if ch == "wecom" and approval is not None:
            row["mode"] = "send_card"
            row["card"] = build_approval_card(**approval)
        else:
            row["mode"] = "send"
        row.update(cfg["row"])
        path = cfg["outbox"]
        try:
            if TEST:
                path = os.path.join(BASE, "shadow", f"{ch}-outbox.jsonl")
                os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
            queued = True
        except OSError as exc:
            last_error = exc
            log("notify write failed", ch, type(exc).__name__)
    if not queued:
        raise OSError("approval notice could not be queued to any selected channel") from last_error


def _what_field(what, label):
    for line in str(what or "").splitlines():
        if line.startswith(label + "："):
            return line[len(label) + 1:].strip()
    return ""


def build_approval_card(num, who, what, expires_ms):
    """Template card for one pending approval (WeCom only).

    Card real estate is small (sub_title_text <= 112 chars, horizontal
    values kept short), so the card carries the decision-critical
    facts — requester, target, explanation, reason, the start of the
    command, and the expiry. The full text stays available via /审批.
    """
    horiz = []
    for label in ("目标", "说明", "原因"):
        value = _what_field(what, label)
        if value:
            horiz.append({"keyname": label, "value": value[:60]})
    cmd = _what_field(what, "命令")
    expiry = f"过期 {fmt_hm(expires_ms)}"
    # Reserve room for the expiry: a long command must not push it
    # past the 112-char sub_title_text limit.
    sub = f"{cmd[:88]}｜{expiry}" if cmd else expiry
    card = {
        "card_type": "button_interaction",
        "main_title": {"title": f"待审批 #{num}"[:26],
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
    """Num of a recently allow_always-decided item in the same group."""
    key = effective_group_key(item)
    if not key:
        return None
    now = int(time.time())
    for n, other in sorted(st["items"].items(), key=lambda kv: int(kv[0])):
        if n == num or not isinstance(other, dict):
            continue
        if other.get("status") != "decided:allow_always":
            continue
        if effective_group_key(other) != key:
            continue
        decided_at = other.get("decided_at") or 0
        if decided_at and now - int(decided_at) <= RECENT_ALWAYS_WINDOW_SECS:
            return n
    return None


def find_group_primary(st, num, item):
    """Num of the already-notified pending item this one groups under."""
    key = effective_group_key(item)
    if not key:
        return None
    for n, other in sorted(st["items"].items(), key=lambda kv: int(kv[0])):
        if n == num or not isinstance(other, dict):
            continue
        if effective_group_key(other) != key:
            continue
        if other.get("status") != "pending" or other.get("grouped_with"):
            continue
        if other.get("notified"):
            if not other.get("group_key"):
                other["group_key"] = key
            return n
    return None


def approval_notice_text(num, item):
    return (f"{item.get('who', '')}\n{item.get('what', '')}\n"
            f"过期：{fmt_hm(item.get('expires_ms'))}\n"
            f"a=仅这次  b=永久  c=拒绝\n"
            f"回 /a {num}｜/b {num}｜/c {num}")


def send_approval_notice(num, item):
    notify(approval_notice_text(num, item), f"new{num}",
           approval={"num": num, "who": item.get("who", ""),
                     "what": item.get("what", ""),
                     "expires_ms": item.get("expires_ms")},
           item=item)


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
    cookies = load_cookies(os.path.join(CONF, "cookies.txt"))
    vm_id = open(os.path.join(CONF, "vm_id.txt")).read().strip()
    return GW2(cookies=cookies, vm_id=vm_id)


def poll_once(gw, st):
    d = gw.call_json("egress.approvals", timeout=20)
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
        if not item["notified"]:
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
                if not item.get("group_key"):
                    item["group_key"] = (group_key(source)
                                         or group_key_from_what(what))
                always_from = find_recent_always_primary(st, num, item)
                auto_done = False
                if always_from is not None:
                    try:
                        gw.call_json(
                            "egress.approval.decide",
                            path_params={"approval_id": aid},
                            body={"decision": "allow_always"}, timeout=20)
                        item["status"] = "decided:allow_always"
                        item["decided_at"] = int(time.time())
                        item["auto_decided_from"] = always_from
                        auto_done = True
                        log("auto-decided pending", num, "from",
                            always_from, aid[:12])
                    except Exception as exc:
                        log("auto-decide failed", num,
                            type(exc).__name__, str(exc)[:100])
                primary = (None if auto_done
                           else find_group_primary(st, num, item))
                if auto_done:
                    item["notified"] = True
                elif primary is not None:
                    # Same command + target as an already-notified
                    # pending item: do not push another card/text.
                    # The primary's decision is applied to this item
                    # too (see consume_decisions).
                    item["grouped_with"] = primary
                    item["notified"] = True
                    log("grouped pending", num, "->", primary, aid[:12])
                else:
                    send_approval_notice(num, item)
                    item["notified"] = True
                    log("notified pending", num, aid[:12], who)
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
            send_approval_notice(num, item)
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
    try:
        off = int(open(OFFSET_F).read().strip())
    except (OSError, ValueError):
        off = 0
    try:
        with open(DECISIONS_F, encoding="utf-8") as f:
            f.seek(off)
            lines = f.readlines()
            new_off = f.tell()
    except OSError:
        return
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except ValueError:
            continue
        num = str(row.get("num", ""))
        decision = row.get("decision")
        it = st["items"].get(num)
        if not it or decision not in DECISION_LABEL:
            notify(f"【审批 #{num}】没找到这条待审批，无法提交。发 /审批 看当前列表。",
                   f"bad{num}")
            continue
        if it.get("status") != "pending":
            # Duplicate decision (same approval submitted on two
            # channels, or text first and a late card after): the
            # first decision already produced its done notice, so a
            # second push on BOTH channels is pure noise — live #8,
            # 2026-10-09, where the dup surfaced as "提交失败" to the
            # user. Log it and move on.
            log("duplicate decision ignored", num, it.get("status"))
            continue
        try:
            gw.call_json("egress.approval.decide",
                         path_params={"approval_id": it["approval_id"]},
                         body={"decision": decision}, timeout=20)
            it["status"] = "decided:" + decision
            it["decided_at"] = int(time.time())
            # Grouped siblings (same command + target, never
            # notified) get the same decision: they are the same
            # request in practice, and leaving them pending would
            # keep their operations blocked until expiry.
            grouped_done = 0
            for gnum, git in list(st["items"].items()):
                if gnum == num or not isinstance(git, dict):
                    continue
                if git.get("grouped_with") != num:
                    continue
                if git.get("status") != "pending":
                    continue
                try:
                    gw.call_json(
                        "egress.approval.decide",
                        path_params={"approval_id": git["approval_id"]},
                        body={"decision": decision}, timeout=20)
                    git["status"] = "decided:" + decision
                    git["decided_at"] = int(time.time())
                    git.pop("grouped_with", None)
                    grouped_done += 1
                except Exception as exc:
                    # Leave it pending and ungrouped: the next poll
                    # promotes it into its own notice.
                    git.pop("grouped_with", None)
                    git["notified"] = False
                    log("grouped decide failed", gnum,
                        type(exc).__name__, str(exc)[:100])
            suffix = (f"同类合并的另 {grouped_done} 条已一并处理。"
                      if grouped_done else "")
            notify(f"【审批 #{num}】{DECISION_LABEL[decision]}：{it.get('who', '')}。"
                   f"{suffix}被拦住的操作会继续。", f"done{num}")
            log("decided", num, decision, "grouped", grouped_done)
        except GatewayError as e:
            notify(f"【审批 #{num}】提交失败：服务端 {e.status} {str(e.payload)[:120]}。"
                   f"可稍后重发决定，或在 Muse 应用里处理。", f"err{num}")
            log("decide failed", num, e.status, str(e.payload)[:120])
        except Exception as e:
            notify(f"【审批 #{num}】提交出错：{type(e).__name__} {str(e)[:100]}",
                   f"err{num}")
            log("decide error", num, type(e).__name__, str(e)[:100])
    with open(OFFSET_F, "w") as f:
        f.write(str(new_off))
    save_state(st)


def cycle():
    st = load_state()
    gw = None
    try:
        gw = connect()
        n = poll_once(gw, st)
        consume_decisions(gw, st)
        err = None
    except Exception as e:
        n, err = -1, f"{type(e).__name__}: {str(e)[:150]}"
        log("cycle error", err)
    finally:
        if gw is not None:
            gw.close()
    json.dump({"ts": int(time.time()), "pending": n, "last_error": err},
              open(STATUS_F, "w"))


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
