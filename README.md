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
  cookie / token 凭证可自动续期，失效时的恢复办法见
  `native-bridge/get-muse-token.md`。
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


## 凭证（不进仓库）

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
  丢消息对账，以及由 worker 内部心跳支撑的失败即停。唤醒载荷在
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
批次不处理；探针缺失或读不到不算活着。演练方案见
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
