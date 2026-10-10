# 冷通道判死演练计划（DEATH_WATCH_SECS=1800）

**STOP. Do not run this drill until the boss authorizes it in writing.**
Do not run it against the live hatch while preparing, reviewing, or merging
this plan. This file is the plan. `ops/cold_death_drill.py` defaults off,
talks only to a `/tmp` copy of the hook, and has no live mode. Nothing in
this plan changes production config, `DEATH_WATCH_SECS`, poll interval,
systemd units, or hook definitions.

授权记录（未填则视为未授权，步骤全部不做）：

| 项 | 填写 |
| --- | --- |
| 授权人 | |
| 时间 | |
| 渠道 | `wecom`（优先）或 `weixin` |
| 会收到演练通知的会话 | 企微 `chatid` + `chattype`，或微信 `from_user_id` |
| 演练 msgid | `drill-deathwatch-` 开头 |
| 范围 | 仅沙箱 / 沙箱 + 线上第二判 |
| 队列当时为空 | |

线上投递只在上一表填完、并且「范围」写明包含线上之后才做。沙箱脚本同样等这张表出现「仅沙箱」或更宽的范围再跑；写这份计划的人在开发机 `/tmp` 里验证过脚本本身，那一次不是演练，也没有连 hatch。

## 现在的判死到底看什么

`hooks/scripts/weixin-inbox.sh` 和 `hooks/scripts/wecom-inbox.sh` 里
`DEATH_WATCH_SECS = 1800`。它不是任务时长上限。`BATCH_CAP_SECS` 仍是
`float("inf")`，活着的 worker 可以一直跑。

算「还活着」的只有 worker 自己写进 outbox 的、带本批 msgid 的行（`update`、
`reply`、`reply_file`，以及任何带该 msgid 且 `queued_at`/`ts` 落在本批
`since` 之后的行）。`heartbeat.py` 留在 `state/heartbeats/<msgid>` 上的
mtime 不再算数。正式回复只要还在 `outbox_parked.json` 里，文件 mtime 会把
活动时间顶到现在，判死不会开火。

同一会话里，如果本批开始之后有一条后续消息的正式回复已经送达（`outbox_results.jsonl`
里 `ok=true` 的 `reply` / `reply_file`），本批走静默丢弃：写入
`covered_drops.jsonl`，不取消，不发通知。这是 2026-10-09 18:35 误报之后加的护栏。

没有后续回复盖住、又连续 1800 秒没有 worker 自己的推送时，先做一次真实活动探测，再按损失次数分流。探测说还活着，就停在这里：不续跑、不取消、不写 `resume_attempts.json`，批次留在原来的位置继续看。出站静默时钟不重置。活动信号一旦旧过 1800 秒，同一轮轮询就按已经累计的出站静默走下面的续跑或取消，不再另给 1800 秒。

探测源（`death_watch_activity.py`，钩子只读）：

1. 桥 `state.json` 里绑定了这个 msgid 的回合（`msgid` 本身，或合并进 `ids`）。下面任一落在 1800 秒内算活着，更具体的来源优先记入 `activity_holds.json`：`activities[].ts` / `activity_seen` 里的活动时间（`bridge-activity`）；`last_reply_at`（`bridge-reply`）；`sess_status` 为 `running` 且 `last_activity_poll` 也在窗口内（`bridge-running`）。只靠 `last_activity_poll`、队列快照、`status.json` 或心跳文件 mtime 都不算。`running` 但轮询时间已经旧过窗口，视为冻结状态，不当成还活着。
2. 渠道 state 目录的 `worker_activity.json`：`{"<msgid>": {"ts": <epoch>}}`，也接受裸 epoch。认文件里的时间戳，不认文件 mtime。这是 worker 自己写下的存活信号，不是 `heartbeat.py` 的代打卡。

失败降级：文件不存在等于没有信号，不是错误。文件在但解析不了，记入 `activity_probe.json` 的 `degraded`，这一路也不算活着。两路都没有新鲜信号时，才走原来的续跑 / 取消。一路坏了不会挡住另一路的新鲜信号。探针模块本身加载失败同样不算活着，钩子退回只看出站静默。1200 秒的停滞提醒在探测说还活着时也不发，避免通知里承诺「马上续跑」而这一轮其实按住了。

没有后续回复盖住、探测也说真死时，按损失次数分流：

1. 第一次：不取消。`resume_attempts.json` 记下这个 msgid（记录保留约 2 小时），
   走孤儿续跑把原消息再唤醒一次，并给用户发一条「♻️ 原任务已中断，已自动续跑一次」。
   续跑 worker 会占住冷通道的当前批。
2. 第二次：`resume_attempts.json` 里已经有这个 msgid。钩子调用渠道 CLI
   `cancel --msgid`，再发一条、且只发一条「⚠️ 任务已停止（续跑后仍没再收到它本人的推送）」。
   正文写明这不是已确认的执行失败。发送失败时通知留在 `failed_notices.json`，
   之后约 1 小时内每次轮询重试。不会第二次自动唤醒。

1200 秒的「⏳ 提醒」是另一条路（`STALL_NOTICE_SECS`）。它不取消、不算判死。
看见只有这条提醒，不能当成演练通过。

待办第 5 项要的「取消 + 失败通知」对应的是第二次，通知原文是「任务已停止」，
不是旧文案「任务执行失败」。若线上通知里出现「任务执行失败」，说明跑到的不是
这份钩子，演练立即停下。

钩子大约每 5 秒跑一次，读的是 hatch 上的真状态。所以下面任何「往生产状态里
写一行」的动作，下一轮就会生效。演练 msgid 必须带前缀 `drill-deathwatch-`，
免得和真消息缠在一起。

## 不许用来制造静默的办法

停 `heartbeat.py`、删心跳文件、停网关、停 `native-bridge`、停钩子、把
`DEATH_WATCH_SECS` 改小、把 `poll_interval_secs` 改掉、重启 systemd、
对一个正在跑的真 msgid 停止推送，都不是这次演练的手段。

停钩子会让两条渠道的入站一起停。改小判死秒数会波及真批次。停掉一个还在
`update` 的真 worker，伤的是用户的任务，不是演练。心跳文件现在根本不计入
存活，停它也不会让判死开火。

## 沙箱：唯一可以脚本化的一档

脚本：`ops/cold_death_drill.py`。

默认什么都不做，直接退出。要跑必须同时带上：

```sh
DEATH_WATCH_DRILL=1 \
DEATH_WATCH_DRILL_CONFIRM=sandbox-only \
python3 ops/cold_death_drill.py
```

可选 `--channel weixin`、`--channel wecom` 或 `--channel both`（默认 both）。
出现 `--live`、环境变量 `DEATH_WATCH_DRILL_LIVE`，或确认词不是
`sandbox-only`，脚本拒绝退出，不建目录。

它做的事：把仓库里的钩子复制到 `/tmp/death-watch-drill-*`，把
`/home/hatch/workspace/...` 换成这个临时目录，CLI 换成只记日志的 stub，
`HOME` 和 `HATCH_HOOK_RUNTIME` 也指向临时目录。复制件里如果还残留
`/home/hatch`，或者 `DEATH_WATCH_SECS = 1800` 不在了，脚本失败退出，
不会改常量再跑。

两边渠道各跑下面这些场景，都用合成 msgid，不读生产 inbox。前七个是原来的出站静默分流，后面是续跑 / 取消之前的真实活动探测：

| 场景 | 期望 |
| --- | --- |
| `hold-young-batch` | 批次才 30 秒，无推送：不取消、不通知 |
| `hold-recent-push` | 有一条 60 秒内的 bound outbox：不取消、不通知 |
| `hold-parked-reply` | 正式回复还在 park 车道：批次保持，不取消 |
| `drop-covered-followup` | 同会话后续回复已送达：静默丢弃，`covered_drops.jsonl` 有记录，不取消 |
| `first-loss-despite-heartbeat-file` | 心跳文件是新的，但没有 worker 推送：仍走第一次续跑，不取消 |
| `first-loss-resume` | 静默超过 1800 秒：CLI 出现「自动续跑一次」，没有 `cancel`，批次被重新挂上 |
| `second-loss-cancel-notice` | `resume_attempts.json` 里已有该 msgid：CLI 出现 `cancel --msgid` 和「任务已停止」，没有「自动续跑一次」，也没有「任务执行失败」，批次清空 |
| `alive-despite-outbox-silence` | 出站推送已旧过 1800 秒，但桥上该 msgid 的活动时间是新的：不续跑、不取消、不发停滞提醒，批次还在，`activity_holds.json` 的 source 是 `bridge-activity`，`resume_attempts` 没有这条 |
| `alive-muse-running` | 没有活动事件，但 `sess_status=running` 且 `last_activity_poll` 在窗口内：按住，source 是 `bridge-running` |
| `alive-worker-over-silence` | 桥上没有这个 msgid，`worker_activity.json` 里的 ts 是新的：按住，source 是 `worker-activity`。出站静默单独不够续跑 |
| `alive-merged-binding` | msgid 只出现在回合的 `ids` 里，`last_reply_at` 是新的：按住，source 是 `bridge-reply` |
| `alive-detached` | 同上，但是 detached 批次：条目留在 `detached`，不续跑、不取消 |
| `stale-activity-resumes` | 桥上有绑定，但活动、回复、轮询都旧过窗口：仍走第一次续跑 |
| `frozen-running-resumes` | `sess_status` 仍是 `running`，轮询时间已旧：不当成还活着，走第一次续跑 |
| `corrupt-bridge-degrades` | `state.json` 不是合法 JSON：`activity_probe.json` 记下 `unreadable:state.json`，然后走第一次续跑 |
| `second-stale-cancels` | 已有续跑记录，桥上的活动也旧了：取消并发「任务已停止」 |

可复跑的沙箱命令（这不是线上演练）：

```sh
python3 tests/test_death_watch_activity.py
DEATH_WATCH_DRILL=1 \
DEATH_WATCH_DRILL_CONFIRM=sandbox-only \
python3 ops/cold_death_drill.py
```

`DEATH_WATCH_SECS` 保持 1800。不要加 `--live`，不要设 `DEATH_WATCH_DRILL_LIVE`。

通过时进程退出码 0，并打印 `SANDBOX ONLY`。这证明的是仓库里这份钩子的分流，
不证明线上钩子进程、真 CLI、真网关投递。线上投递是下一节，而且默认不做。

沙箱失败时目录会留在 `/tmp` 便于看 `cli.log`。通过后删掉，除非设置了
`DEATH_WATCH_DRILL_KEEP=1`。

## 线上第二判：授权之后的人工步骤

只做「取消 + 停止通知」这一支。做法是预先写入 `resume_attempts.json`，
让下一轮轮询直接走 `_route_death` 的 fail 分支。这和真的第二次静默是同一个
分支，不必先唤醒一个续跑 worker，也不必干等 30 分钟再等 30 分钟。

预先写入的时间戳必须在 2 小时以内（代码会丢掉 `now - ts >= 7200` 的记录）。
写成更早的时间等于没写，下一轮会变成第一次续跑，真的唤醒 worker。

第一次续跑会占用冷通道并给用户发「已自动续跑一次」。本计划不把它放进默认识别
范围。若授权表的范围没有写明「含第一次」，下面步骤里的预写就不能省。

优先企微。待办第 1 项里微信入站当时是 0，微信出站本身还能发，但企微这条链
更完整。一次只做一个渠道。

路径按 hatch 用户写死，不要用操作者 shell 的 `$HOME`（root 会指到 `/root`，
快照会拍错目录）：

- 企微钩子状态：`/home/hatch/hooks/state/wecom-bot/`
- 企微网关状态：`/home/hatch/workspace/wecom-bot/state/`
- 微信钩子状态：`/home/hatch/hooks/state/weixin-bot/`
- 微信网关状态：`/home/hatch/workspace/weixin-bot/state/`

下面用企微举例。微信把 `chatid` 换成 `from_user_id`，钩子目录换成
`weixin-bot`。

### 1. 只读确认，不过就停

在写任何文件之前：

- `active_batch.json` 没有 `msgids`，`detached` 为空或不存在。
- `pending.json` 是 `{}` 或不存在。
- 授权会话在计划采用的 `since`（约 `now - 1900`）之后，没有已经送达的正式回复。
  有的话判死会被静默盖住，演练会假通过。
- `outbox_parked.json` 里没有本次 msgid。
- 本次 msgid 在 inbox、seen、carried、cancelled 里都不存在。
- 桥 `state.json` 里没有这个 msgid 的新鲜活动（`activities` / `last_reply_at` / `running` 加新鲜 `last_activity_poll`），渠道 state 里也没有它的 `worker_activity.json` 记录。有的话钩子会按住，不取消，演练会看起来没触发。文件读不了同样不会被当成还活着，但那是降级，不是本计划要的通过条件。

`since` 必须早于现在至少 1800 秒，否则判死不触发。用 `now - 1900`，给一轮
轮询留余量。同会话只要有 `ts > since - 2` 且正式回复已送达的消息，就换一个
更安静的窗口，不要把 `since` 再往前推去躲开它（越往前越容易被别的回复盖住），
也不要改钩子。

地址从该会话最近一条真实 inbox 行抄 `chatid` 和 `chattype`（微信抄
`from_user_id`）。只替换 `msgid`、`text`、`ts`。不要编一个新的 chatid，
通知会发到别的会话或发不出去。群聊 `chattype=group` 时 CLI 用 `--chat-type 2`，
单聊用 `1`。抄错类型，通知进错会话。

文案固定成一眼能认出来的演练句，例如
`【DRILL 冷判死】演练消息，不是用户任务。`
停止通知会截取前 20 个字，用户能看到 `DRILL`。

### 2. 快照

```sh
SNAP=/tmp/death-watch-drill-snap-$(date +%Y%m%d%H%M%S)
mkdir -p "$SNAP"
cp -a /home/hatch/hooks/state/wecom-bot/. "$SNAP/hook-state/"
cp -a /home/hatch/workspace/wecom-bot/state/cancelled.json "$SNAP/cancelled.json"
wc -c /home/hatch/workspace/wecom-bot/state/inbox.jsonl \
  /home/hatch/workspace/wecom-bot/state/outbox.jsonl \
  /home/hatch/workspace/wecom-bot/state/outbox_results.jsonl \
  > "$SNAP/sizes.txt"
tail -n 1 /home/hatch/workspace/wecom-bot/state/inbox.jsonl > "$SNAP/inbox-tail.txt"
```

快照之后、写入之前如果进来了真消息，停止，按回滚处理，不要继续栽种。

### 3. 写入顺序

下一轮轮询只要看见「静默批次」但还没看见 `resume_attempts` 记录，就会走第一次
续跑。所以记录必须先于 `active_batch.json` 落地。推荐顺序：

1. 在 `resume_attempts.json` 里合并进 `"<drill-msgid>": <now-60>`。已有别的
   msgid 就保留，不要整文件覆盖。值用现在附近的秒，不要用 `since` 那种
   1900 秒前的数，更不要用超过 7200 秒的数。
2. 把 drill msgid 追加到 `seen_msgids.txt` 和 `carried_msgids.txt`。
   未进 seen 的 inbox 行会被当成新消息立刻唤醒。
3. 向 `inbox.jsonl` 追加一行。企微字段至少包括 `msgid`、`from_userid`、
   `chattype`、`chatid`、`msgtype`（`text`）、`text`、`ts`（等于 `since`）、
   `media`（`[]`）。微信用 `from_user_id` 和 `text`，`ts` 同样等于 `since`。
4. 最后写 `active_batch.json`：`{"msgids": ["<drill-msgid>"], "since": <now-1900>, "detached": []}`。
   写之前再读一次，只要已经有别的 msgid 或非空 `detached`，就不要写，改走回滚。

不要写 outbox 行。有一行带这个 msgid 的推送，活动时间会被顶上去，判死不触发。
不要动 `pending.json`、`subagent_jobs.json`、网关进程或钩子定义。

### 4. 看一轮轮询然后收证据

钩子 5 秒一轮。写入 `active_batch.json` 之后等一轮，按下一节清单核对。
不要为了「再看一次」把批次改回去再写一遍，那会第二次取消、第二次发通知。

## 证据清单

线上第二判通过，要同时满足下面每一条。缺一条就是未通过，按回滚收场，不要改代码补救。

取消：

- 渠道 CLI 收到且仅收到这一条演练取消：`cancel --msgid drill-deathwatch-...`。
- `/home/hatch/workspace/<channel>-bot/state/cancelled.json` 比快照多出的 msgid
  只有这一条。多出来的别的 msgid 说明误伤，立即停止并按回滚处理。
- 钩子没有把这个 msgid 再放进新的 wake。第二判时期望本轮对它是静默
  （payload 的 `count` 为 0）。若侧会话被唤醒去处理这句演练文案，说明走成了
  第一次续跑或新消息，不是本计划的通过条件。

停止通知，一次：

- 企微 outbox 新行 `mode` 为 `send`，`content` 同时含 `任务已停止` 和 `DRILL`，
  不含 `任务执行失败`，不含 `自动续跑一次`。
- 微信对应行是发往授权 `from_user_id` 的 `send`，正文同样两条都要满足。
- `outbox_results.jsonl` 里同一 `id` 有 `ok=true`。只有入队、没有 `ok=true`，
  不算送达。
- `failed_notices.json` 里不再留着这句演练通知。还留着说明 CLI 发送失败，
  钩子会在约 1 小时内重试，必须在回滚时拿掉，否则用户会晚收到。
- 授权会话里的人确实看见这一条。`ok=true` 是网关回执，待办第 5 项要的是通知
  如实到达。

没有误伤：

- 快照里的其他 msgid 仍在它们原来的队列文件里，没有被取消。
- `covered_drops.jsonl` 没有这次 drill msgid。有的话是被后续回复盖住了，
  不是判死。
- 没有出现只有「⏳ 提醒」、没有取消的情况。那是 1200 秒停滞提醒。
- 网关、桥、两条钩子的进程都还在，轮询仍是 5 秒，`DEATH_WATCH_SECS` 仍是 1800。

沙箱脚本的 `second-loss-cancel-notice` 对应取消和通知文案这两条，但是 stub
CLI，没有 `outbox_results.jsonl` 的 `ok=true`，也不能代替用户看见。
`first-loss-resume` 对应续跑通知，不对应取消。

若授权范围另外包含第一次，另用一个新的 `drill-deathwatch-` msgid，不要预写
`resume_attempts.json`，并接受这些附带后果：一条「已自动续跑一次」会发给用户，
一个真 worker 会被唤醒。文案仍用演练句。worker 若开始调用工具或处理别的事，
立刻在该会话 `/stop`，并把这个 msgid 取消。第一次的通过条件是：有续跑通知且
`ok=true`，`cancelled.json` 没有这个 msgid，wake 的孤儿项带 `resume=true`。
它不能代替第二次的取消证据。

## 回滚

沙箱：删掉 `/tmp/death-watch-drill-*`。生产目录不该有变化。若有变化，说明
脚本没有按本文工作，先停，不要继续线上步骤。

线上栽种已经开始：

1. 不要 `systemctl stop` 钩子、网关或桥。钩子一停，两条渠道的新消息都停。
2. 不要从 `cancelled.json` 里删掉 drill msgid。留着，它才不会被丢消息对账
   重新唤醒。
3. 从 `failed_notices.json` 里去掉正文含这个 drill msgid 或 `【DRILL 冷判死】`
   的项，避免失败通知在下一小时重试。
4. `resume_attempts.json`、`resume_queue.json`、`starvation.json`、
   `orphan_claims.json` 里只删 drill msgid 这个键。文件里若有别的键，保留。
5. `active_batch.json`：若当前 `msgids` 只含 drill msgid 或已经为空，恢复快照
   里的这一份。若里面出现了别的 msgid，不要整文件覆盖，只把 drill msgid 去掉。
   `detached` 同理。
6. `seen_msgids.txt` 和 `carried_msgids.txt` 里的 drill 行可以留着。
   它已经在 cancelled 里，留着不会再唤醒。
7. `inbox.jsonl` 只在文件末尾仍是当初那一行、且字节数正好等于快照大小加上
   那一行时，才截回快照大小。其间若有真消息追加，不要截断，把演练行留在文件里。
8. 若用户已经收到「任务已停止」或「已自动续跑一次」，由操作者再发一条人能看懂的
   收尾，例如「【演练结束】刚才那条是冷判死演练，没有真实任务失败。」
   不要靠再触发一次判死来发这句话。
9. 若意外走成第一次、worker 已被唤醒：对该会话 `/stop`，确认 drill msgid
   已在 `cancelled.json`。不要让续跑 worker 把演练句当成真任务做完。
10. 对照快照看有没有非 drill msgid 被取消或被移出队列。有的话不要自行再改
    钩子，把 diff 留下来，按真事故处理。

回滚做完再删 `$SNAP`。

## 中止条件

下面任意一条成立就停手，做回滚，不要「再改一个状态文件试试」：

- 授权表是空的，或范围只写了沙箱却有人准备写 `/home/hatch`。
- 队列不是空的，或写入 `active_batch.json` 前发现它已经被真消息占用。
- drill msgid 不以 `drill-deathwatch-` 开头。
- 取消日志里出现任何非 drill msgid。
- 通知文案是「任务执行失败」，或同一轮又续跑又取消。
- 同会话在 `since` 之后已有送达的正式回复。
- 栽种过程中用户发来了新消息。

## 做完之后

把结论写回 `docs/open-issues-2026-10-09.md` 第 5 项：渠道、msgid、取消是否只打在
这条 msgid 上、停止通知的 `outbox` id、`ok=true` 的时间、用户是否看见、
有没有误伤。没做线上投递就不要把第 5 项划掉。沙箱通过只说明仓库钩子的分流还在，
生产实证仍是那一条真的 `ok=true`。
