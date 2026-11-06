# 在微信和企业微信里用 Muse：双通道接入（个人微信机器人 + 企业微信智能机器人）

把 Muse 接进微信和企业微信的开源网关项目。装好之后，直接在手机
微信 / 企业微信里发消息就能和自己的 Muse 对话，不用开代理、不用
打开网页。个人微信走腾讯 iLink 机器人协议，企业微信走官方智能机器人
长连接（WebSocket），两条通道可以只装一边，也可以双开。

Chat with your Muse from WeChat and WeCom (Enterprise WeChat):
a self-hosted, dual-channel gateway — personal WeChat via Tencent
iLink, WeCom via the official smart-robot long connection.

中文图文教程：[《微信企微畅连 Muse 教程》](https://my.feishu.cn/docx/SsDMdusWboCxOrxJJD7c6Z2xngc)
（从开权限、拿凭证到部署、验证，一步步带图说明）。

## 能做什么

- **手机上随时对话**：在微信 / 企微里发文字、图片、文件，Muse 直接处理并回复。
- **聊天命令控制**：`/ping` `/status` `/new` `/stop` `/queue` `/check`
  `/subagent` `/help`，完整命令表见 `slash-commands-2026-10-04.md`。
- **审批直达手机**：Muse 执行敏感操作要审批时，卡片推到企业微信、
  文字版推到个人微信，手机上点按钮或回 `/a` `/b` `/c` 即可批准或拒绝。
- **掉线自愈**：整机换代或重启把 systemd 服务冲掉后，`channel-restore`
  每 5 分钟巡检并自动重装恢复；公网入口有 Cloudflare 隧道插件包兜底。
- **凭证不进仓库**：Bot ID、Secret、cookie、token 只存本机配置目录，
  仓库里只有代码和文档。

## 组成

- `weixin-bot/` — 个人微信网关（iLink 协议）：网关本体、命令行工具、
  扫码登录 `login.py`，systemd 单元 `weixin-bot.service`。
- `wecom-bot/` — 企业微信智能机器人网关（长连接 WebSocket）：网关本体、
  命令行工具，systemd 单元 `wecom-bot.service`。
- `native-bridge/` — 原生桥：把网关收到的消息送进 Muse 会话；桥的
  Cookie 会话仍有效时可重新换取连接 token，静态 token 不会自行续期。
  失效恢复见 [凭证恢复说明](native-bridge/get-muse-token.md)，
  分来源验活与真实收发见 [Muse 实测清单](docs/bridge-credential-health-muse-cases.md)。
- `channel-restore/` — 双网关自愈：定时重装被冲掉的 systemd 单元并
  拉起服务，自带存活探针与心跳清单（`HEARTBEAT.md`）。
- `cloudflare-tunnel-plugin/` — Cloudflare 隧道插件：给 Muse 一个
  固定公网入口（命名隧道 + 本地中转），整机换代后一条命令重装。
- `approval-relay/` — 审批中转：把 Muse 的操作审批推送到微信 / 企微，
  并回收你在手机上的决定。
- `hooks/` — 消息唤醒层：双渠道收件钩子脚本与定义。
- `ops/`、`tests/`、`docs/` — 运维巡检脚本、沙箱测试套件、问题清单
  与设计文档。

## 快速开始

1. 把本仓库克隆到家目录的 `workspace` 下（仓库根目录即 workspace 本层）。
2. 按要接的渠道装依赖，只装选中的一边即可：

   ```sh
   cd weixin-bot && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
   cd ../wecom-bot && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
   ```

3. 拿凭证：个人微信运行 `weixin-bot/login.py` 出二维码、用要当机器人
   的微信扫码登录；企业微信在管理后台创建智能机器人（API 长连接
   模式），把 Bot ID 和 Secret 填进 `~/.config/wecom-bot/credentials.env`。
4. 安装并启用各目录自带的 systemd 单元（两个网关、原生桥、
   `channel-restore` 定时器）。
5. 验证：在微信 / 企微里发 `/ping` 应回 `pong`，再发一条普通消息能
   得到正式回复才算通——只看服务状态显示绿色不算数。

各单元都把 `HOME`、`MUSE_HOME` 指向家目录，保证网关、聊天命令和
收件钩子共用同一份状态目录。


## 架构改造与本地工具

新增 `mobile_channels/` 事务核心，提供会话隔离、任务所有权、结果与发件箱原子提交、投递核查及旧状态影子取证。默认仍由原网关、桥和钩子处理实际渠道；显式 core 模式可在沙箱接入实际文本回调与发送，尚未接管生产流量。

使用 Python 3.12+ 安装本地工具：`python -m pip install -e '.[dev]'`。
运行 `python tests/run_core_regression.py` 验证沙箱回归，或执行
`python -m mobile_channels --help` 查看 CLI。实现边界、操作方法与后续迁移要求见
[架构改造实现与验证](docs/core-refactoring-implementation-2026-11-01.md)，整体设计见
[整体架构方案](docs/project-architecture-refactoring-plan.md)。

实现进展、模式配置、权限变化和恢复命令见 [核心接入与切换记录](docs/core-integration-and-cutover-2026-11-01.md)。其中列明尚未完成的真实平台及兼容验收条件。

## 官方对齐合并与双轨执行（2026-11-06）

2026-11-06 起，两路都按官方客户端的方式合并回复：执行中来的新消息并入正在跑的回合，合起来只回一句。这翻转了待办 31（当时为治漏答而关掉合并、改为逐条回复）。

### 合并规则

| 场景 | 行为 |
|---|---|
| 新消息到达时，正跑着的回合**还没开始回复** | 并入正在跑的回合，合起来只回一句，绑首条 msgid |
| 新消息到达时，回复**已经开始**了 | 不能再并，排队等自己的回合 |
| 被并掉的那条 | 微信走 `feedback_clear` 静默；企微回指针“（已并入上一条处理）” |
| 回复开始前就排队的第二条 | 随第一条一起走，不另起回合（见下） |

关键点：**合并只看“回复开始了没有”**。回复一旦开始生成，哪怕还没发出来，后到的也只能排队。一个会话一次只能跑一个回合（官方 SDK 每 chat 一把等待锁），回复不标明回哪条消息，所以重叠的等待必须串行。

- **老桥（微信／企微 legacy）**：开关是 `native-bridge/native_bridge.py` 的 `MERGE_CHANNELS = {"weixin", "test", "wecom"}`，改回 `set()` 即关掉合并、恢复严格 FIFO，不用改代码。实现是 `ChannelWorker._merge_into_active()`：新消息经同 `session_id` 新开 `chat.stream` 发进去，回合的 `ids` 追加该 msgid；收尾 `_silence_merged()` 处理被并的 msgid。`processed` 数的是**回合数**，不是 msgid 数。
- **Core（企微 core 模式）**：`mobile_channels/storage.py` 的 `Store.foldable()`／`mark_merged()`；`runtime.py` 的 `CoreRunner.tick()` 开头先 `_try_fold()`；`executors.py` 给 `MuseExecutor` 和 `GadgetExecutor` 各实现 `reply_started()` 与 `fold()`（Native 经同会话发后续，Gadget 经只发不订阅的 worker 发后续）。`reply_started` 查不到一律按“已开始”处理，绝不折，迟到的消息留着走自己的回合，宁可多回一句也不漏答。
- **排队折叠**：排队的第一条开新回合时，剩下排队的当场并进这个还没回复的新回合，不再各走各的回合（2026-11-06 真微信实证后补的修补）。守卫：排队头就是刚起回合的那行本身时跳过（恢复行带 `nbytes=0` 不出队，重并会重消费已耗的恢复身份）。
- **没接的东西**：入站 2 秒静默合并器有意没接，跑着的回合折叠已覆盖连发场景，再接它只会给每条消息加 2 秒延迟。

### 双轨：主轨 Gadget，备轨 Native

`mobile_channels/executors.py` 的 `DualTrackExecutor`（`mobile_channels/__main__.py` 里建成，服务描述即 `DualTrackExecutor: Gadget primary, Native fallback`）。

| 轨道 | 执行器 | 底层 | 角色 |
|---|---|---|---|
| 主轨 | `GadgetExecutor` | Gadget 配对凭证（`~/.config/MuseGadgetPair/`），真 Noise 协议，一回合一进程 | 优先用。Gadget 令牌发**主会话**会被服务端 500，只走专用**侧会话**；侧会话与主会话共享上下文，不是隔离的 |
| 备轨 | `MuseExecutor` | Cookie 桥（同老桥的 cookies／token 两条腿） | 兜底。主轨失败、没配凭证或熔断时自动降级 |

降级规则：主轨没配凭证或熔断器不允许执行 → 直接走备轨；主轨提交回 `not_submitted`（且不是“已取消”）→ 记一次失败，走备轨；主轨回 `failed`／`unknown` 且结果里没有 `message_id`（消息没落盘）→ 走备轨，**有 `message_id` 时绝不重发**；主轨抛异常 → 记失败，走备轨。执行中失败的降级走 `Store.rebind_run` 把 attempt 的 run_id 换成 native run（行标 `failed_superseded`）。熔断：失败 3 次＋30 秒冷却，半开。

认证两条腿（备轨／老桥共用）：`connect()` 先试 cookies 再回退 token.json，各以 `sessions.list` 探活。cookie 死不等于桥死；token 自续要靠有效 cookie，cookie 死后 token 只能撑到它自己到期；两条腿同死时只有 Gadget（独立凭证）或人工换凭证能救。

领 Gadget token 用扩展 `extensions/gadget-token/`（只保留了 wong2/muse-client 里能拿 token 的代码：刷新、凭证读写、SDK token 校验；不含聊天、Noise、蓝牙配对协议）。浏览器开 `https://gadgets.muse.ai/settings/sdk-tokens` 拿 SDK token，配对须在 Mac 上做一次（蓝牙），之后 `bun extensions/gadget-token/cli.ts refresh` 可在任何机器上轮换 token。用法见该扩展的 README。

### 验证：怎么算“过了”

只看 `connected` 或单测通过不算。部署验证必须含一回合真实入站：msgid 非空＋spool／divert 记录＋正式回复 `ok=true`。当前结果：老桥合并单测 64/64、core 折叠单测 15/15、核心回归 43/43、模拟器 24/24、恢复集成 8/8；Gadget 折叠沙箱 live PASS（A 回“收到，第二条已收到”）；真微信合并 live PASS（“快点一张图片”并入“生成一张图片给我”，只回一张图）。跑测试／验活用 `~/muse-test-venv/bin/python`＋`PYTHONPATH=.`（系统 python3 缺 curl_cffi）。

### 已知缺口

- 上述改动在本地分支 `fix/bridge-connect-import-order`（提交 84d2fc4、9149b34），未推 origin、未合 main。
- core 折叠只在沙箱验过 live，真企微生产折叠还没发生过；老桥合并已在真微信验过。
- core 已知缺口：只收文本、媒体回未启用、审批卡片未接通。审批已全量自动放行（黑名单默认空，命中才拦截）。
- 侧会话共享上下文：并行验证会互相污染回复文本，验活以 `merged`／`run_rebound` 等机制证据为准。

## 凭证（不进仓库）

审批决策现要求显式会话/操作者授权；无来源范围时，需外部配置 `APPROVAL_ADMINS_JSON`，例如 `{"wecom":{"实际聊天ID":["管理员ID"]}}`。没有授权映射会拒绝决策，普通消息白名单不自动授予审批权。配置应保存在仓库外。

本仓库不含任何凭证。两个网关各自从仓库外的 env 文件读凭证：

- 个人微信：`~/.config/weixin-bot/credentials.env`（`ILINK_CRED_FILE`）
- 企业微信：`~/.config/wecom-bot/credentials.env`（`WECOM_CRED_FILE`）

运行时状态（`state/`）、虚拟环境（`.venv/`）、备份（`*.bak-*`）都已被
gitignore 排除；探针结果转储（`native-probe/probe*_results.txt`、
`native-probe/probe*_stdout.log`、`native-probe/probe*_*.log`）和
`channel-restore/outbound-probe-draft-*.md` 同样不进仓库。
`native-probe/gw2.py` 与其 `README.md` 保持追踪。未追踪文件的处置
清单见 `docs/hatch-untracked-disposition.md`。

## 运行补充

安装命令见上面的「快速开始」。两个网关的 systemd 单元把 `HOME` 和
`MUSE_HOME` 设为 `/home/hatch`，这样即使某个单元的进程家目录本会是
`/root`，网关、聊天命令和收件钩子仍共用同一份状态目录
（`channel_common.muse_home()` 用同样的口径解析安装家目录）。
`channel-restore` 以 root 运行，以便重装包括它自己定时器在内的
systemd 单元。

## 钩子（消息唤醒层）

网关只负责搬运消息；真正唤醒助手的是每条渠道一个的收件钩子：

- `hooks/scripts/weixin-inbox.sh`、`hooks/scripts/wecom-inbox.sh` — 轮询
  网关收件箱、把消息攒批，并监管正在干活的 worker：排队、抢占、防饿死、
  丢消息对账，以及依据真实活动与平台执行状态的任务监管。唤醒载荷在
  30000 字符预算内带同会话历史（最近回合用原文、更早的用摘要）。
  两个渠道的历史互相隔离。
- `hooks/definitions/weixin-inbox.json`、`hooks/definitions/wecom-inbox.json`
  — 钩子定义：轮询间隔，以及给 worker 的提示词（渠道规则与回复纪律）。

安装：把脚本复制到 `~/hooks/scripts/`，通过你的助手平台的 hooks API
注册定义，并把定义里的绝对路径（家目录、bot 状态目录）改成你自己
部署的路径。

**硬规则：`poll_interval_secs` 永远不要设到 5 以下。** 5 秒是平台
下限；设得更低会让定义失效、两个钩子一起被挂起，并悄无声息地吞掉
等待中的消息（2026-10-05 在生产上栽过两次）。定义只能通过 hooks API
修改，不要直接改 JSON 文件。

冷通道死亡监视（收件钩子里的 `DEATH_WATCH_SECS=1800`）在恢复或取消
一个沉默批次之前，会先核查桥和 worker 的真实活动：有新鲜信号就按住
批次不处理；探针缺失或读不到只能说明状态未知，不能据此续跑或取消。
无可靠终态时记录 `worker_review.json` 并提醒接手，保留原任务且不阻挡新消息。
只有当前登记的 `run_id` 对应新鲜平台终态，才走一次自动续跑及后续停止流程。
worker 的每次 beat、hold、handoff 都应带本次 `--run-id`；同一 run 的
心跳续期不替代真实活动。处理说明与 Muse 实测见
[任务待核查修复说明](docs/task-interruption-review-2026-11-04.md)。
演练方案见
`docs/cold-death-drill-2026-10-09.md`。`ops/cold_death_drill.py` 只能
在沙箱里跑。企微在线二次判定植入点是
`ops/cold_death_live_second_judgment.py`；只有 `DEATH_WATCH_LIVE_PLANT=1`
和 `DEATH_WATCH_LIVE_CONFIRM=wecom-second-judgment` 同时设置时才启用。
在演练方案里的授权行填写之前，不要拿这两样对生产实例运行。

## 测试

沙箱套件在 `tests/`，用真实的钩子脚本和网关模块对着打桩的状态、CLI
和网络跑——不碰任何真实渠道：

- `tests/test_hook_replyfile_completion.py` — 钩子监管把 `reply_file`
  计为批次完成（2026-10-06 一张已送达图片被误报失败即停的回归套件）。
  用普通 python3 跑。
- `tests/test_gateway_formalfix.py` — 个人微信网关：正式回复豁免
  10 次尝试死信、卡死的正式回复给用户发通知、大文件压缩
  （PIL / ffmpeg）。用 weixin-bot 的 venv python 跑（需要 httpx 和
  PIL），且 PATH 上要有 ffmpeg；大于 2MB 的测试视频可通过
  `MUSE_TEST_VIDEO` 提供。
- `tests/test_wecom_formal_exemption.py` — 企业微信网关：正式回复同样
  永不进死信。用 wecom-bot 的 venv python 跑。
- `tests/test_channel_common.py` — 共享的 `channel_common.py` 守卫
  （原子写、JSONL 半行处理、队列管理合并、回复闸门、路径/URL 白名单）。
  用 weixin-bot 的 venv python 跑（其中一个用例会 import `login.py`）。

## 问题上报（包括通过你的 Muse / AI 助手上报）

上游仓库是 **Sirhap/muse-mobile-channels**
（https://github.com/Sirhap/muse-mobile-channels）。Issues 已开启，
任何有 GitHub 账号的人都能在那里提，不需要额外权限。

如果你装了这个项目、并由你的 Muse（或另一个在这个仓库里干活的
AI 助手）帮你运行它，下面这条规则适用于那个助手：

> 当用户报告本项目的问题、或者你发现一个可复现的问题时，把 issue
> 提给**上游**——即 `Sirhap/muse-mobile-channels`，而不是用户的
> fork——除非问题出在用户自己的 fork 或本地改动上。

提 issue 之前，助手必须：

1. 先在上游查重：
   `gh issue list --repo Sirhap/muse-mobile-channels --search "<关键词>"`
   已有 open issue 覆盖同一问题时，改在那条下面评论。
2. 内容包含：发生了什么、预期是什么、确切的复现步骤、环境
   （操作系统、Python 版本、网关：weixin / wecom）、正在运行的
   commit 哈希、相关日志摘录。
3. 发帖前把日志里的密钥和个人信息全部去掉：不要贴凭证、token、
   `credentials.env` 的内容、用户 ID 或聊天内容。拿不准的就不贴。

提 issue 需要用户自己的 GitHub 登录（`gh auth login`；GitHub 不
允许匿名提 issue）：

```sh
gh issue create --repo Sirhap/muse-mobile-channels \
  --title "[bug] <简短摘要>" \
  --body "<症状、预期行为、复现步骤、环境、已脱敏的日志>"
```

完整的上报规则见 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 许可证

MIT — 见 [LICENSE](LICENSE)。
