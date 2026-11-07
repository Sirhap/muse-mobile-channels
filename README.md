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

前置要求：Linux + systemd 的机器、Python 3.12+、一个能正常登录的
Muse 账号；接个人微信需要一个能当 iLink 机器人的微信号，接企业微信
需要在企微管理后台有建智能机器人的权限。

1. 把本仓库克隆到家目录的 `workspace` 下（仓库根目录即 workspace 本层）。
2. 按要接的渠道装依赖，只装选中的一边即可：

   ```sh
   # 从仓库根目录执行，按需选一行。
   (cd weixin-bot && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt)
   (cd wecom-bot && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt)
   ```

3. 安装桥与审批中转共用的 Python 环境及 Muse SDK。以下命令从仓库根目录
   执行，SDK 源码放在被忽略的 `reference/` 中：

   ```sh
   python3 -m venv ~/muse-test-venv
   mkdir -p reference
   git clone https://github.com/nikships/muse-cli.git reference/muse-cli
   git -C reference/muse-cli checkout 2e42aeccbef71437f4ccd4faa626414f2461be7b
   ~/muse-test-venv/bin/pip install ./reference/muse-cli
   ```

   此处固定了 SDK 源码版本；实际账号的认证、协议兼容性仍按第 8 步验收。
   自定义源码位置时，把 `MUSE_CLI_SRC` 设为包含 `muse_cli/` 的
   `src/` 目录，并写入桥与审批服务的环境。

4. 拿渠道凭证：
   - 个人微信：运行 `weixin-bot/.venv/bin/python weixin-bot/login.py`，
     出二维码后用要当机器人的微信扫码登录；登录成功后 `login.py` 会
     自动把凭证写进 `~/.config/weixin-bot/credentials.env`
     （`ILINK_BOT_TOKEN`／`ILINK_BOT_ID`／`ILINK_USER_ID`，文件权限
     0600），不用手工填。
   - 企业微信：在管理后台创建智能机器人（API 长连接模式），把 Bot ID
     和 Secret 填进 `~/.config/wecom-bot/credentials.env`，格式如下
     （文件权限 0600；`WECOM_ALLOW_USERS` 是允许用机器人的企微用户
     ID，逗号分隔，留空则谁都用不了）：

     ```sh
     WECOM_BOT_ID=你的机器人ID
     WECOM_BOT_SECRET=你的机器人Secret
     WECOM_ALLOW_USERS=你的企微用户ID
     ```

5. 拿 Muse 桥凭证：按 [凭证恢复说明](native-bridge/get-muse-token.md)
   用浏览器扩展导出 `muse-credentials.zip`（内含 `cookies.txt`、
   `vm_id.txt`、`token.json`），装到 `~/.config/native-bridge/`
   （目录 0700、文件 0600）。没有这一步，网关能连上但桥发不出去消息。
6. 配置 legacy 桥。创建仓库外的配置文件，例如
   `~/.config/native-bridge/config.json`，其中只保留已安装的渠道，
   下面的家目录和聊天身份须替换成实际值：

   ```json
   {
     "channels": {
       "wecom": {
         "bot_state": "/home/你的用户名/workspace/wecom-bot/state",
         "session_title": "Muse 企业微信",
         "chatid": "实际聊天ID"
       },
       "weixin": {
         "bot_state": "/home/你的用户名/workspace/weixin-bot/state",
         "session_title": "Muse 微信",
         "from_user_id": "实际微信用户ID"
       }
     }
   }
   ```

   在 `native-bridge.service` 的 `[Service]` 中设置
   `Environment=NATIVE_BRIDGE_CONFIG=/你的家目录/.config/native-bridge/config.json`
   和 `Environment=NATIVE_BRIDGE_HOME=/你的家目录/workspace/native-bridge`。
   在仓库的 `native-bridge/` 下为选中的渠道创建 `enabled-wecom` 或
   `enabled-weixin` 空文件，桥才会把结果写入真实出站队列。配置文件设为
   `0600`。审批通知的目标身份也需要在
   `~/.config/approval-relay/notice.env` 配置，例如
   `APPROVAL_RELAY_WECOM_CHATID=实际聊天ID`，微信使用
   `APPROVAL_RELAY_WEIXIN_TO=实际微信用户ID`。

7. 安装 systemd 单元。**先把各 `.service` 文件里的 `/home/hatch`
   全换成你自己的家目录**（`WorkingDirectory`、`ExecStart`、
   `Environment=HOME`／`MUSE_HOME`、`Environment=*_CRED_FILE` 这几处
   都是写死的），再拷到系统目录并启用：

   ```sh
   # 把 /home/hatch 换成你的 $HOME，例如：
   # sed -i "s|/home/hatch|$HOME|g" weixin-bot/weixin-bot.service wecom-bot/wecom-bot.service native-bridge/native-bridge.service approval-relay/approval-relay.service channel-restore/channel-restore.service channel-restore/channel-restore.timer
   sudo cp weixin-bot/weixin-bot.service wecom-bot/wecom-bot.service native-bridge/native-bridge.service approval-relay/approval-relay.service /etc/systemd/system/
   sudo cp channel-restore/channel-restore.service channel-restore/channel-restore.timer /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now weixin-bot.service wecom-bot.service native-bridge.service approval-relay.service channel-restore.timer
   ```

   只装选中的一边时，只拷、只启用那一边的网关单元即可；原生桥、
   审批中转、`channel-restore` 定时器是共用的。`channel-restore`
   以 root 运行，以便重装包括它自己定时器在内的 systemd 单元。
   恢复脚本只管理已有单元或存在渠道凭证的网关。没有明确的企微
   Core 配置时，它不会安装 Core drop-in 或启动 `muse-core-run`。
8. 验证：在微信 / 企微里发 `/ping` 应回 `pong`，再发一条普通消息能
   得到正式回复才算通——只看服务状态显示绿色不算数。

各单元都把 `HOME`、`MUSE_HOME` 指向家目录，保证网关、聊天命令和
收件钩子共用同一份状态目录。


## 架构改造与本地工具

新增 `mobile_channels/` 事务核心，提供会话隔离、任务所有权、结果与发件箱原子提交、投递核查及旧状态影子取证。新部署默认由原网关、桥和钩子处理渠道；Core 模式通过外部模式文件显式启用。仓库的单元测试和沙箱结果不能说明某个部署已完成切换。

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
| 被并掉的那条 | 微信走 `feedback_clear` 静默（写入失败时改发指针）；企微回指针“（已并入上一条处理）” |
| 回复开始前就排队的第二条 | 随第一条一起走，不另起回合（见下） |

关键点：**合并只看“回复开始了没有”**。回复一旦开始生成，哪怕还没发出来，后到的也只能排队。一个会话一次只能跑一个回合（官方 SDK 每 chat 一把等待锁），回复不标明回哪条消息，所以重叠的等待必须串行。

- **老桥（微信／企微 legacy）**：开关是 `native-bridge/native_bridge.py` 的 `MERGE_CHANNELS = {"weixin", "test", "wecom"}`，改回 `set()` 即关掉合并、恢复严格 FIFO，不用改代码。实现是 `ChannelWorker._merge_into_active()`：新消息经同 `session_id` 新开 `chat.stream` 发进去，回合的 `ids` 追加该 msgid；收尾 `_silence_merged()` 处理被并的 msgid。`processed` 数的是**回合数**，不是 msgid 数。
- **Core（企微 core 模式）**：`mobile_channels/storage.py` 的 `Store.foldable()`／`mark_merged()`；`runtime.py` 的 `CoreRunner.tick()` 开头先 `_try_fold()`；`executors.py` 给 `MuseExecutor` 和 `GadgetExecutor` 各实现 `reply_started()` 与 `fold()`（Native 经同会话发后续，Gadget 经只发不订阅的 worker 发后续）。`reply_started` 查不到时按“已开始”处理（绑定缺失或结果读不到时不折；Gadget 结果文件尚未写入时按“未开始”，因 worker 刚起、还没发消息），迟到的消息留着走自己的回合，宁可多回一句也不漏答。
- **排队折叠**：排队的第一条开新回合时，剩下排队的当场并进这个还没回复的新回合，不再各走各的回合（2026-11-06 真微信实证后补的修补）。守卫：排队头就是刚起回合的那行本身时跳过（恢复行带 `nbytes=0` 不出队，重并会重消费已耗的恢复身份）。
- **没接的东西**：入站 2 秒静默合并器有意没接，跑着的回合折叠已覆盖连发场景，再接它只会给每条消息加 2 秒延迟。

### 双轨：主轨 Gadget，备轨 Native

**默认还是走 Cookie（Native 桥）。** Gadget 是可选的，不配也能跑，
不配时自动走 Native 备轨，不用改配置。

Gadget 是 Meta 的 Muse 设备凭证，跟浏览器 Cookie 完全独立：配对一次
后得到 `pairing.json`／`identity.json`／`sdk_token` 三个文件，存在
`~/.config/MuseGadgetPair/`，之后靠 refresh token 在任何机器上轮换
access token，不依赖浏览器登录态。所以 Cookie 死的时候 Gadget 还能用，
反过来也一样。配对须在 Mac 上做一次（蓝牙），步骤见扩展的 README。

`mobile_channels/executors.py` 的 `DualTrackExecutor`（`mobile_channels/__main__.py` 里建成，服务描述即 `DualTrackExecutor: Gadget primary, Native fallback`）。配了 Gadget 时优先走它，没配、熔断或失败时自动降级到 Native。

| 轨道 | 执行器 | 底层 | 角色 |
|---|---|---|---|
| 主轨 | `GadgetExecutor` | Gadget 配对凭证（`~/.config/MuseGadgetPair/`），真 Noise 协议，一回合一进程 | 优先用。Gadget 令牌发**主会话**会被服务端 500，只走专用**侧会话**；侧会话与主会话共享上下文，不是隔离的 |
| 备轨 | `MuseExecutor` | Cookie 桥（同老桥的 cookies／token 两条腿） | 兜底。主轨失败、没配凭证或熔断时自动降级 |

降级规则：主轨没配凭证或熔断器不允许执行 → 直接走备轨；主轨提交回 `not_submitted`（且不是“已取消”）→ 记一次失败，走备轨；主轨提交回 `failed`／`unknown` 且执行器绑定里没有 `platform_run_id`（消息没落盘）→ 走备轨；主轨抛异常 → 记失败，走备轨。执行中失败时（query 回 `failed` 且结果无 `message_id`）走备轨并 `rebind_run` 把 attempt 的 run_id 换成 native run（行标 `failed_superseded`），**有 `message_id` 时绝不重发**。熔断：失败 3 次＋30 秒冷却，半开。

认证两条腿（备轨／老桥共用）：`connect()` 先试 cookies 再回退 token.json，各以 `sessions.list` 探活（除非设了 `NATIVE_TOKEN_JSON`，此时只用 token）。cookie 死不等于桥死；token 自续要靠有效 cookie，cookie 死后 token 只能撑到它自己到期；两条腿同死时只有 Gadget（独立凭证）或人工换凭证能救。

Gadget 收到发送 ACK、Worker 仍在等待订阅终态时，执行器会借用有效的
Native 桥凭证读取同一 Gadget 侧会话的 `chat.history`。历史结果要晚于
本轮 ACK，明确的父消息必须属于本轮、已确认的合并消息或其回复链。
读到完成消息后最多做 3 次总查询，间隔 1 秒；仍有已知生成中的回复时
继续等待。首段结案后保留 15 分钟的历史观察记录，默认每条记录每 10 秒检查一次，
在独立线程查询历史。迟到文本和附件按消息 ID 去重后追加到原回复路由，
重启后继续检查。出现下一条用户消息后，只补发有明确父消息关联的旧回合片段，
无法归属的内容保留在 Muse 历史中。历史读取失败或没有匹配结果时保留原订阅，不重新发送已确认的
消息。成功结果与发件箱提交后才清理本地 Worker。

排查订阅延迟时查看
`~/.config/muse-gadget-runs/gadget-<attempt_id>.log` 中的
`event … seq=… elapsed_ms=…` 和 `result:`。历史兜底需要桥凭证，
Gadget 本身的发送和订阅使用独立设备凭证。取消默认停止本地等待。
可用 `MUSE_GADGET_SERVER_CANCEL=1` 启用 Native 辅助取消，只有读到关联终态才
确认服务端停止，其他情况回执说明 Muse 可能继续执行。

领 Gadget token 用扩展 `extensions/gadget-token/`（只保留了 wong2/muse-client 里能拿 token 的代码：刷新、凭证读写、SDK token 校验；不含聊天、Noise、蓝牙配对协议）。浏览器开 `https://gadgets.muse.ai/settings/sdk-tokens` 拿 SDK token，配对须在 Mac 上做一次（蓝牙），之后 `bun extensions/gadget-token/cli.ts refresh --credentials ~/.config/MuseGadgetPair` 可在任何机器上轮换 token。用法见该扩展的 README。

### 验证：怎么算“过了”

部署验证必须含一回合真实入站，记录 msgid、当前模式下的入站任务或 spool／divert 记录，以及正式回复的投递 `ok=true`。
历史验收记录包含 Gadget 折叠沙箱回合和微信合并回合，具体部署仍须核对运行版本并重新验活。
本地检查使用下方“测试”章节的统一入口；平台验活使用已安装 Muse SDK 的
`~/muse-test-venv/bin/python` 与 `PYTHONPATH=.`。

### 已知缺口

- 上述改动已合入 main。部署时通过 `git rev-parse HEAD` 和服务启动命令核对实际运行版本。
- core 折叠只在沙箱验过 live，真企微生产折叠还没发生过；老桥合并已在真微信验过。
- Core 接受文本与不超过 2 MiB 的 JPEG、PNG、GIF、WebP 图片，Gadget 可直接发送图片，
  设备不可用时回退 Native。排队图片会保留独立回合；
  微信与企微 Core 出站支持文本、图片、视频和文件，附件路径受网关文件策略限制。
  微信沿用原网关的压缩与 CDN 上传，支持类型超过 2,000,000 字节时尝试既有压缩流程。
  先完成附件上传再发送正文和媒体，部分发送结果未知时进入待核查，人工重试沿用稳定发送 ID。
- Core 审批通知以文本发送，支持 `/a /b /c` 与已有企微卡片的决定回调。
  legacy 审批通知支持模板卡。默认非黑名单请求自动放行；黑名单命中、
  策略要求人工或自动放行 RPC 失败时转人工，通知失败只重试通知。
- 侧会话共享上下文：并行验证会互相污染回复文本，验活以 `merged`／`run_rebound` 等机制证据为准。

### Core 的 hooks 接手

可为单个 Core 渠道显式设置 `"hook_handoff": true`，并注册独立的 Core
hook。Gadget 与 Native 都有明确证据未提交任务时，任务进入持久化交接账本。
hook 提供一次唤醒载荷，worker ACK 后才确认接收；结果仍由 Core 向原聊天投递。
未知提交不转交，已提供但未收到回执的载荷等待核查，避免重复执行。
安装、回执和取消命令见 [Core hook 交接](docs/core-hook-handoff.md)。

## 凭证（不进仓库）

审批决策现要求显式会话/操作者授权；无来源范围时，需外部配置 `APPROVAL_ADMINS_JSON`，例如 `{"wecom":{"实际聊天ID":["管理员ID"]}}`。没有授权映射会拒绝决策，普通消息白名单不自动授予审批权。配置应保存在仓库外。

无原始审批聊天范围的通知需要显式目标。企微在审批服务环境设置
`APPROVAL_RELAY_WECOM_CHATID`，微信设置 `APPROVAL_RELAY_WEIXIN_TO`。
缺少目标时通知保留待重试。健康通知连续 3 次查询失败后报告断线，
恢复查询后发同一事件的恢复通知。`APPROVAL_RELAY_TEST=1` 始终写影子队列。

本仓库不含任何凭证。凭证分三类，都只存本机配置目录：

- 渠道凭证（两个网关各自从仓库外的 env 文件读）：
  - 个人微信：`~/.config/weixin-bot/credentials.env`（`ILINK_CRED_FILE`）
  - 企业微信：`~/.config/wecom-bot/credentials.env`（`WECOM_CRED_FILE`）
- 桥凭证（默认走 Cookie，见下）：`~/.config/native-bridge/` 下的
  `cookies.txt`、`vm_id.txt`、`token.json`（目录 0700、文件 0600）。
  Cookie 就是你在浏览器登录 muse.ai 后的会话凭证，桥拿它去换连接
  token；`token.json` 是静态 token 回退。拿取与恢复见
  [凭证恢复说明](native-bridge/get-muse-token.md)。
- Gadget 凭证（可选，不配也能跑）：`~/.config/MuseGadgetPair/` 下的
  `pairing.json`、`identity.json`、`sdk_token`（目录 0700、文件
  0600）。配对与轮换见 `extensions/gadget-token/README.md`。

运行时状态（`state/`）、虚拟环境（`.venv/`）、备份（`*.bak-*`）都已被
gitignore 排除；探针结果转储（`native-probe/probe*_results.txt`、
`native-probe/probe*_stdout.log`、`native-probe/probe*_*.log`）和
`channel-restore/outbound-probe-draft-*.md` 同样不进仓库。
`native-probe/gw2.py` 与其 `README.md` 保持追踪。未追踪文件的处置
清单见 `docs/hatch-untracked-disposition.md`。

## 运行补充

两个网关的 systemd 单元把 `HOME` 和 `MUSE_HOME` 设为家目录（见快速
开始第 5 步的路径替换说明），这样即使某个单元的进程家目录本会是
`/root`，网关、聊天命令和收件钩子仍共用同一份状态目录
（`channel_common.muse_home()` 用同样的口径解析安装家目录）。

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
部署的路径。**注意：钩子本身只负责把消息攒批、唤醒助手，真正回
消息的是你的助手平台**（本项目是为 Muse 这类带 hooks API 的助手
平台写的）。没有能接 hooks 的助手平台，网关收到消息也不会有人回。

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

本地安装诊断、服务计划生成与真实链路验收步骤见
[安装检查与验收](docs/deployment-checks.md)。`doctor` 仅检查本地条件；
`service-plan` 生成可审查的服务文件和安装脚本，执行脚本会启动 Linux 服务。

安装开发依赖并按 [Gadget SDK 安装步骤](extensions/gadget-token/README.md#install-the-full-gadget-sdk)
准备固定版本的 SDK 与 Bun 1.4.4 后，从仓库根目录运行：

```sh
python -m pip install -e '.[dev]'
python tests/run_core_regression.py
bash tests/test_hook_heal_20261011.sh
bun test tests/gadget_turn.test.ts tests/gadget_turn_history_fallback.test.ts
```

统一回归入口包含执行器、媒体、审批和恢复脚本的新增回归。
CI 在 Ubuntu 和 macOS 安装同一 SDK 提交与 Bun 版本后运行这些检查。
上述 SDK 和依赖安装属于开发环境准备，测试本身使用临时状态和模拟传输。

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
