# Worker 判死信号方案（2026-10-14，待用户拍板）

承接：网关场景研究（`docs/gateway-scenario-research-2026-10-14.md`）
里唯一值得动代码的一项、待办 5 的未关边界。本文只出方案，不动代码；
用户拍板后另开实施轮。

## 要解决的缺口

冷通道批次里 worker 单独死亡、但脱离的心跳循环还活着时：现有提前
判死（EARLY_RESUME，2026-10-14 上线）覆盖不到，只能等 worker 自推
静默满 DEATH_WATCH_SECS=1800 秒才进判死路由。也就是最坏 30 分钟
用户只会先收到一条 5 分钟首提醒（零推送批次），然后干等到 30 分钟
才续跑/停止通知。历史实证：2026-10-09 企微长视频 worker 静默死亡、
2026-10-13 微信两批次推理回合无产出，都属这一类。

## 为什么现有信号覆盖不到（2026-10-14 代码实查）

1. `heartbeats/<msgid>.pid` 里记的是**心跳循环自己的 pid**，不是
   worker 的 pid（`_hb_pid_alive` 注释写明：the hooks record no
   worker pid）。EARLY_RESUME 的条件是每个 msgid 的心跳 pid 已死
   + beat 过期 ≥180s + 自推静默 ≥300s；心跳循环幸存时 pid 活着，
   一票否决整个批次。
2. 心跳文件 mtime 同理：心跳循环是脱离 worker 的代打卡进程，它活
   着 beat 就一直新鲜，与 worker 生死无关（2026-10-09 用户已下令
   心跳不作存活证据，存活只认 worker 本人推送）。
3. hook 不 spawn worker 进程：hook 只产出唤醒载荷，worker 是平台
   侧的 agent 会话，本机没有它的 spawn 记录、退出记录可查。
4. 1800 秒本身是有意政策不是 bug：2026-10-07「没有上限」+ 2026-10-09
   「存活只认本人推送、不凭时间/文件推断」两道令的合成结果。任何
   「凭静默提前动手」的方案都必须先经用户点头，不能当普通优化做。

现有可复用的底子：判死路由 `_route_death`（覆盖→静默丢弃、探针判
活→hold、首次→续跑一次、二次→fail-stop）与活动探针
`death_watch_activity.py`（信号源：桥 state 的回合活动、
worker_activity.json 的 worker 自写时间戳；缺失/不可读 = 无信号，
不误判为死）。

## Phase 0：验证性 spike（只读，先做，结果决定路线）

不写生产代码，只回答四个问题，每项要有实查证据：

1. **worker 会话可定位吗**：冷通道批次有没有办法映射到平台侧的
   worker 会话 id？候选线索：唤醒载荷的投递记录、muse.db 里的
   executions/transcripts（待办 3 复盘就是靠 muse.db 找到两名工人
   的完整转录，说明记录存在且可查），看能不能按 msgid/时间窗稳定
   关联，而不是靠正文搜索碰运气。
2. **worker 会话有活动/终态字段吗**：平台记录里 worker 回合的
   最近活动时间、完成/失败/被杀状态是否可读、延迟多大。
3. **worker_activity.json 的真实写 cadence**：抽近两周真实批次，
   看 worker 实际多久写一次（决定探针信号的新鲜度阈值能不能压到
   5–10 分钟级）。
4. **本机有没有 worker 进程形态**：在一个真实冷批次运行期间抓
   `ps` 快照，确认 worker 在本机是否存在可识别的进程树（若有，
   路线 A/D 才成立；预计没有，此项为排除项）。

spike 产出：一页结论（C 可行 / C 不可行转 B），附证据，再请用户
在路线间拍板。spike 本身不改任何生产文件。

## 路线 C（首选，spike 通过才成立）：探针加第三信号源

- 做法：给 `death_watch_activity.py` 加第三个信号源——平台侧
  worker 会话活动记录。死亡侧的重定时仿 EARLY_RESUME 先例：当
  「worker 自推静默 ≥300s」且「探针确认 worker 会话已终态，或
  其平台活动已过期 ≥ 阈值（spike 定，量级 180–300s）」时，提前
  进**现有** `_route_death`，不新增任何续跑/取消 machinery。
- 为什么首选：它是「积极证据」路线——探针见到 worker 还在活动
  就 hold（现有语义），只有拿到平台侧的死亡/终态证据才提前动
  手，与「不凭时间文件推断生死」的既有令不冲突；误杀面最小。
- 风险：平台记录延迟或映射错位会造成假死信号。缓解：映射不到、
  记录不可读一律按「无信号」处理（沿用探针现有降级语义），且首
  次路由是续跑一次而非取消，续跑有 claim 机制防双 worker 抢同
  一批次。

## 路线 B（备选，政策变更，必须用户明示同意）：零推送批次提前续跑

- 做法：批次**从未有过 worker 自推**、活动探针也无任何鲜活信
  号、自批次开始静默满 T 秒（建议 T=600，约 5 个工人推送节拍）
  时，提前进 `_route_death`（首次仍是续跑一次 + 如实通知）。
  已推送过的批次不动，仍旧 1800s。
- 代价与风险：这是凭「静默 + 无信号」推断，与既有令有张力，所
  以单列为政策项。真实风险是 worker 正在跑一个长静默工具调用
  （如长渲染）且没写 worker_activity——探针判无信号、被提前续
  跑，形成双跑。缓解：claim/orphan 机制保证同一 msgid 只被一
  个 wake 认领；续跑通知写明「无推送满 T 秒自动续跑」，事后可
  对账。饶是如此，没有用户点头不做。

## 路线 A/D（仅 spike 第 4 项翻盘时考虑）

若发现 worker 在本机确有稳定可识别的进程：由起批侧把 worker
pid 落盘（类 `heartbeats/<msgid>.wpid`），EARLY_RESUME 增加一
路「wpid 已死」证据。预计不成立，列此只为完备。

## 不做什么

- 不改 DEATH_WATCH_SECS=1800 主判死、不改 BATCH_CAP 无上限。
- 不动桥、不动网关代码；改动面仅限两 hook 镜像 +
  `death_watch_activity.py` 及其仓库副本与套件。
- 不顺手处理待办 19（演练验收分歧，与本方案无关）。

## 测试计划（实施轮）

沙箱场景（沿用 hook 套件双口径：仓库副本 + 线上脚本各跑一遍）：

1. worker 死、心跳活（本方案目标场景）：在目标时限内进判死路
   由，续跑一次 + 恰一条通知；
2. worker 活但静默、探针有鲜活信号：hold，不续跑、不取消、不
   动 resume_attempts；
3. 长任务正常推送节拍：全程不触发任何提前路径；
4. 探针源缺失/损坏：按无信号降级，不崩、不误判；
5. detached 批次同场景复测一遍（与 active 同路由）。

渠道门控照旧例：沙箱全绿 → test 渠道 drill → 用户点头后上
weixin/wecom 生产 hook，部署后 dry-run + 观察窗确认无误报。
hook 是逐轮轮询脚本，无服务重启；回滚 = 还原备份文件。

## 待用户拍板

1. 是否先跑 Phase 0 spike（只读，约半天内出结论），还是直接按
   路线 B 谈政策？
2. 若走到路线 B：提前时限 T 取 600s 还是更保守的 900s？
3. 路线 C 若可行：平台活动过期阈值跟随 spike 数据定，还是用户
   先给一个上限（如不超过 10 分钟必须动手）？

## 社区与全面复查后的优解修正（2026-10-14 21:17 补）

用户要求再想想有没有别的优解、看社区并全面复查。本地深挖 + 社区
调研（BullMQ / Celery / Sidekiq / Temporal / Airflow / k8s /
OTP / LangGraph / muse-cli 协议）后，结论修正如下。

### 本地新实查（此前方案没用上的平台信号）

1. **平台有 agent 终态记录**：`agent.agents` 表（muse.db）每个
   agent 有平台维护的 status（现存量：running 8、completed
   7683、shutdown 3849、errored 6、interrupted 4）与 updated_at，
   `agent.runtime_state` 另有 last_event_seq / updated_at。这等价
   于 OTP 的 DOWN 终态事件在本地的对应物——是平台写的，不是
   worker 自报的。缺口仍在归属：这些 root 行的 session_id 为
   空、hook 没记批次 ↔ agent 的对应关系，目前只能靠 msgid 在
   转录里反查（待办 3 复盘就是这么找到工人的），能取证、但作
   为逐分钟探针太重太脆。
2. **协议层状态端点成套存在**（muse-cli routes.json，258 个方
   法）：`sessions.get`、`sessions.list`（会话带 updated_at_ms）、
   `activity.list` / `activity.get`、`subagents.status`、
   `vm.health`。且 history 事件带 status 字段：正在写一半的回复
   没有 status 字段，status 出现本身即回合终态信号（is_reply 的
   判别逻辑）。平台不可达时的官方语义是「unavailable、retryable」
   = 未知重试，不是判死——与本机探针「不可读 = 无信号」降级语
   义同构。
3. muse-cli 没有任何 worker 心跳/租约/看门狗机制：它的世界里
   会话生死由平台记录回答，客户端只轮询 history（3 秒游标增
   量）。这本身即结论——在 Muse 平台生态里判活的正道是查平台
   记录，不是造本地信号。

### 社区共识（三条，互相印证）

- **信号必须由被观察者本人从其执行路径内产生**。Sidekiq 演进史
  里「定时 healthcheck job 替进程打卡」被明确淘汰（分不清活着
  闲着还是已死、检测延迟可达 90 分钟），换成进程自打卡注册表。
  本机心跳循环正是这个被淘汰的反模式——给它再加任何 pid/mtime
  检查都不值得投入。
- **最成熟的组合是双探测**（Temporal activity heartbeat + Atlan
  生产 ADR）：死活看心跳（heartbeatTimeout 抓什么都不跳了），
  卡死看进度（有心跳无进度是另一级、等更久），且单个长操作由
  执行者显式声明 scoped hold 豁免——系统不替长操作发明时长。
- **固定租约与无上限任务结构性冲突**（BullMQ lockDuration /
  Celery visibility_timeout 的双跑复盘）：租约时长必须大于最长
  合法静默，而本机任务时长无上限——租约路线除非先改政策，否
  则不立项。Airflow 的 zombie 判定可借其规则形态：状态记录 ×
  本人心跳两份独立记录合取才判，单信号永不判死。

### 修正后的路线排序

1. **路线 C（维持首选，目标更具体）**：探针第三信号源 = 平台
   会话状态。spike 的靶子从「muse.db 能不能映射」收窄为三条
   可落地的映射法，按序验：
   a. **worker 自登记**：worker 在批次开始时把自己的会话/agent
      标识写进 worker_activity.json（Sidekiq 式自注册——注册动
      作出自本人执行路径），探针 thereafter 按 id 查平台状态；
      前提是 worker 在会话内能拿到自己的标识，spike 先验这一
      点；
   b. **桥侧导出文件**：桥本来就轮询平台（gw2），由桥把冷
      worker 会话的 status / updated_at_ms 摘要落成文件（类
      status.json 的 per-channel 摘要，2026-10-14 已有先例），
      探针只读文件——不给 hook 发平台凭证，契约面与现有探针一
      致；
   c. 内容反查（msgid → 转录 → agent）只作取证兜底，不作生产
      探针。
   判定规则按 Airflow 式合取：平台状态终态/冻结 **且** worker
   自推静默 ≥300s 才提前进 _route_death；任一源有鲜活信号即
   hold；平台不可读 = 未知。
2. **路线 E（新增，与 C 并行备好的兜底）**：进度感知推送 +
   显式 hold。worker 的 update 从「有行就算活」升级为带进度内
   容；worker 进入长静默操作（如长渲染）前，在
   worker_activity.json 留一条带声明时长的 hold 记录；判死口
   径区分「无推送」与「有 hold 的静默」——hold 期内不判死、
   hold 过期且无推送按无信号处理。它不依赖任何平台映射、与
   「存活只认本人推送」政策完全同向，是 C 的映射若不通时成
   本最低的真改进（压缩 wedged-but-alive 的判定时间）。
3. **路线 B（降级为最后手段）**：社区证据与政策张力都在它对
   面；只有 C、E 都不成立时才谈，且仍须用户明示同意。
4. **明确不做**：给代打卡心跳加检查（社区已淘汰）；固定租约
   （与无上限政策冲突）；从 agent 框架层找自律心跳协议
   （LangGraph 托管层最终也是平台 run 记录 + 租约回收，
   CrewAI/AutoGen 无此层）。

### 对拍板点的修正

原三问简化为两问：1) 批准按 C（先验自登记、再验桥导出）+ E
并行出 spike 结论；2) spike 若证明 C 的两种映射都不通，是否
接受只做 E、B 永久搁置。原「T 取 600/900」之问随 B 降级一并
搁置。

社区来源（主要）：Temporal activity heartbeat 教程与 Atlan
ADR-0018（progress-aware heartbeat）；BullMQ workers guide
（lock/stalled）；Sidekiq 进程注册表替换 healthcheck 的实录
（trade-tariff commit d73c6ed）；Airflow zombie task 文档；
muse-cli PROTOCOL.md 与 routes.json；LangGraph 托管租约实现
描述（graph-agents-cli）。

## 附录：live 验收三件触发清单（待办 1/11/13，代码已好、只缺实证）

与本方案并行，用户在手机上按下面发即可，我这边只读对账：

- **待办 11 + 12（同一轮，约 5 分钟）**：在微信发一个会同时产生
  文字回复和文件回复的长任务（例如「生成一个介绍 Muse 的
  txt 文件并说明内容」）。验：同 msgid 的 reply 与 reply_file
  两行都 ok=true、无 suppressed 记录、trailing 两拍关单
  outbox_ok=true。
- **待办 13（约 6 分钟）**：先发任务 A（耗时 5 分钟以上），约
  60 秒后发任务 B（与 A 无关、不要合并进 A）。验：B 排队满
  180 秒收到恰好 1 条久等提醒；A 完成后 B 收到恰好 1 条
  started；B 正式回复 ok=true、feedback 记录被清。微信企微
  任选一渠道先跑。
- **待办 1（约 2 分钟）**：在微信发一个大文件（>10MB 的视频最
  合适）。验：微信收到失败通知、文件本体转投企微且企微侧结果
  ok=true。
