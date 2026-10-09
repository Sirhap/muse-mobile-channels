# 待办与待修复清单

核查基准：2026-10-09 18:10 CST 实查（网关 status、桥 spool/outbox、git status、线上与仓库 cmp 对比）。已完成项不在此列，只记未决项；每项带证据与下一步，完成一项划掉一项并注明日期。

## P0 待修复

### 1. 微信入站消息为 0，桥实测无法开始
- 证据：微信网关自 2026-10-09 14:23 起多个会话（含 16:45 当前会话）`msgs_received` 恒为 0，状态却显示 connected；同期企微入站 10 条、出站正常，微信出站也正常（sent 6）。
- 证据：用户 17:06 下令的桥 LT2 实测句「长任务实测：先睡 150 秒…」经两轮监控（17:07–17:32、17:33 起 40 分钟）从未到达——桥 spool、冷通道 inbox、微信 outbox 三处零记录。
- 影响：微信桥 LT2 生产实测做不了；若不是用户没发，则 iLink 入站可能在 connected 假象下静默失效，所有微信入站都受影响。
- 下一步：用户发一条对照微信消息并同时盯网关入站日志；仍为 0 则查 iLink 订阅/重连，必要时重启网关或重新配对验证。
- 2026-10-09 代码复查：本仓库没有可对照的 iLink 入站失败日志，不能靠改重连代码修。这不是一行补丁。先有「用户确实发出、网关日志仍为 0」的对照，再决定重启、重新配对，或带着那次日志查订阅。

### 2. 主会话「Delivery not confirmed」（用户报告，根因未定位）
- 证据：在微信/企微网关 state、hooks state、approval-relay 代码中均未搜到该字样或对应记录，本仓库代码里没有这条状态。
- 影响：主会话消息投递可信度存疑——若正式回复也可能未确认送达，长任务「完成必达」承诺就有缺口。
- 下一步：请用户指出该提示出现的界面与对象（主会话 UI / 哪条消息）；再沿主会话→侧会话投递路径查投递回执的产生与丢失点。
- 2026-10-09：桥与两个网关里仍没有这条文案。桥关单时会在日志写 `outbox_ok=true|false|pending`（读网关 `outbox_results.jsonl`，不等待），那是渠道出站回执，不是这条「Delivery not confirmed」。没有出现位置之前不要在渠道代码里对这个字符串打补丁。

## P1 代码已上线、待 live 验收

### 3. 微信桥 LT2 长任务生产实测未完成
- 现状：LT2 已上生产（2026-10-08 23:25），test 渠道三题验收通过；微信生产渠道从未完整跑过一遍：约 120 秒进度推送、trailing 逐条投递、结果绑定回复、outbox `ok=true` 均无 live 证据。
- 2026-10-09 代码核对（仍非 live）：多段 trailing 是首条 bound reply、其后 unbound send，关单只看 status=completed 加静默（见第 6、7 项）。关单日志带 `outbox_ok=`，值来自网关 `outbox_results.jsonl` 里该 bound reply 的 `id`；没有结果行就是 `pending`。桥不等这条回执，避免微信发送延迟卡住队列。trailing 段的收件人优先用本回合的 from_user / chatid / chattype（idle 扫描没有回合时仍用 config）。
- 卡点：依赖第 1 项（入站恢复或确认用户发出消息）。
- 完成标准：指定实测句完整跑通并逐项对账（进度 1 条、结果 1 条、回执 ok=true、trailing 正常关单）。对账时可以同时看桥日志里的 `outbox_ok=true` 和 `outbox_results.jsonl`。

### 4. 企微 started / 久等提醒 live 未验证
- 现状：企微网关对齐已部署（2026-10-09 01:29），专项 26/26、回归全绿；但 started 只发一次、180 秒久等提醒、回复送达清记录这三点没有真实企微长任务验证过。
- 完成标准：一条真实企微长任务中三点各留一条送达证据。

### 5. 冷通道判死（DEATH_WATCH_SECS=1800）无生产实证
- 现状：2026-10-09 已部署，沙箱四场景 + 企微 A/B 共 31/31；部署后尚无真实冷通道 worker 静默死亡事件触发过它（无事件本身是好事，但机制未经生产检验）。
- 完成标准：首次真实触发时复盘其取消 + 失败通知是否如实送达，并把结论回填本项。

## P2 与 muse-cli 收尾循环的已知差距

参照实现：开源 muse-cli `chat.py` 的 `Chat.send`（WAIT=300s 首回复期限、每条新回复后续命 QUIET=10s 静默关单、3s 轮询 history、无总时长硬顶）。2026-10-09 三处代码差距已按下面的决定改完；微信生产跑通仍挂在第 3 项。

### ~~6. 静默关单窗口 30s vs 10s~~ （2026-10-09 已对齐）
- 原状：桥 `TRAILING_QUIET_SECS=30`，cli 为 10s。当初没有留下保留 30s 的理由。
- 决定：改为 10s。关单仍要 `sessions.get` status 为 completed，并且距上一条回复静默满 10s。status 还是 running 时不会因这 10 秒关单，工具调用的间隙不会被切断。关单之后才到的段仍走 `idle_push_scan`，游标不变。

### ~~7. trailing 600s 硬顶 vs cli 无硬顶~~ （2026-10-09 已去掉硬顶）
- 原状：桥 `TRAILING_MAX_SECS=600`，到点就拆回合，后面的回复改走 idle 推送。
- 决定：删除该常量。收尾只看静默续命，和 muse-cli 以及「没有上限」一致（同 5cb1fbc 去掉冷通道时长封顶）。冷通道 `DEATH_WATCH_SECS=1800` 不动，它看的是 worker 静默死亡，不是一条还在跑的 trailing 回合。session 一直停在 running 时不会被强制拆掉；满 1200 秒的升级提示仍写明可以 /stop。`sessions.get` 失败同样不关单（没有信号不算 completed）。代价是：状态若一直卡在 running，队列会一直被这个回合占着，直到 /stop 或进程重启。这是对齐「没有上限」的取舍，不再加第二顶。

### ~~8. 进度触发粒度更粗~~ （2026-10-09 已加细，通知间隔不变）
- 原状：`activity.list` 60s 轮询，另有 120s 定时兜底；不认 history 新内容。
- 决定：`ACTIVITY_POLL_SECS` 改为 12（10–15 秒区间）。history 本来每 2 秒读一次，现在把「还没完成的 assistant 行」和 tool/function 行当作第二触发源。用户自己的 `message.user` 回声不算。两种触发源共用 `LT_EVENT_PROG_MIN_SECS=60`，所以探测变快，气泡仍最多约一分钟一条；没有新动态时还是 120 秒定时兜底。已完成的回复只投递、不另发一条「还在处理中」。

## P3 一致性与运维

### 9. 企微网关线上已漂移仓库（并行会话在制）
- 证据：2026-10-09 18:09 实查，`wecom-bot/gateway.py` 在 18:01 提交（109289c）后又被改动 +65/−4（审批卡片点击直接决策 `approval_card_decide`），未提交、未 push；疑似并行会话正在施工，本清单不动它。
- 其余一致性同日 18:10 已核：hooks 线上脚本与仓库副本两渠道 cmp 一致；6 个 systemd unit（channel-restore/native-bridge/weixin-bot/wecom-bot/approval-relay）与仓库副本全一致；工作区除下条草稿外干净。
- 下一步：该线完工后由其归属会话补交；下次 push 前重跑一遍线上↔仓库 cmp。

### 10. VM 替换反复抹掉 /etc/systemd unit
- 证据：2026-10-09 17:04 心跳巡检再次发现 channel-restore.timer 缺失（当天第二次），已按 HEARTBEAT.md 修复并验证 all gateways healthy。
- 影响：自愈链依赖「定时器 + 心跳巡检」双保险，任一失效窗口内渠道可能长时间掉线无人知。
- 下一步：接受现状（文档化）或再加一层独立于 VM 的存活探测，二选一并记录。

### 11. 未跟踪草稿待处置
- `channel-restore/outbound-probe-draft-2026-10-08.md` 自 2026-10-08 起未跟踪悬置。hatch `/home/hatch/workspace` 另有约 21 条未跟踪 `??`（此前报告为该草稿 + `native-probe/*` 探针归档）。本克隆没有这些文件，逐条瓷单未到。
- 2026-10-09：不能在没有正文的情况下代为提交或删除，也不要在别的克隆里凭记忆重写。
- 处置计划与忽略规则：`docs/hatch-untracked-disposition.md`。`.gitignore` 已忽略 `native-probe/` 下除 `gw2.py` 与 `README.md` 以外的内容，以及任意目录的 `outbound-probe-draft-*.md`。忽略只让 git 不再把它们报成脏文件，磁盘上的文件还在。
- 下一步：在 hatch 上按该文档先导出瓷单再填表。keep 才补否定规则并提交；delete 只删表里点名的路径，且不在这项的准备提交里做。生产文件（网关、桥、hooks、`channel-restore/restore.sh` 与 unit）不删。

### 12. 企微合并消息指针回复（平台差异，非 bug，备查）
- 企微网关不消费 `feedback_clear`，被合并消息必须单独发一条「（已并入上一条处理）」指针回复关 think stream，微信侧是静默清除。平台限制，无修复计划，仅防误判为 bug 重查。

### 13. audit 测试套件非自包含（已修复 2026-10-09）
- 症状：Grok 从 PR#2（c4ecc99）全新克隆复跑 `test_bridge_audit_20261008.py` 得 22/30，8 条 FAIL 全在 2–6 节的投递内容断言；生产机上同套件 30/30。
- 根因：`native_bridge.py` 的 BASE 写死生产路径，`live_mode()` 读 BASE 下 `enabled-<渠道>` 开关、shadow 投递写 BASE/shadow/；audit 测试只重定向了 CFG/STATE_F/STATUS_F、没重定向 BASE，平时靠借用生产开关与 shadow 目录才过。全新克隆无开关 → 渠道判 shadow 模式 → 回复全进 shadow outbox，测试在沙箱 outbox 里查无此行。隔离命名空间已三段复现（无 shadow 崩、建 shadow 后 22/30 且 FAIL 清单逐字一致、补开关 30/30）。
- 修复：audit 测试按 progress 套件（13816b4）同款做法把 BASE 指向沙箱、自备 enabled 开关与 shadow 目录；修复后在同款隔离环境（克隆目录只剩 3 个跟踪文件）复跑 30/30，生产环境亦 30/30。
- 备查：Grok 在 PR#2 分支上的临时解法是在其克隆的 native-bridge/ 下 touch enabled-weixin/enabled-wecom 并保留 shadow 目录；PR#2 合并后以 main 的修法为准。
