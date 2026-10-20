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

---

以下为英文技术细节（凭证存放、钩子机制、测试与问题上报规则）：

## Secrets

No credentials are in this repo. Each gateway reads its credentials from an
env file outside the tree:

- Weixin: `~/.config/weixin-bot/credentials.env` (`ILINK_CRED_FILE`)
- WeCom: `~/.config/wecom-bot/credentials.env` (`WECOM_CRED_FILE`)

Runtime state (`state/`), virtualenvs (`.venv/`), and backups (`*.bak-*`)
are gitignored. So are probe result dumps
(`native-probe/probe*_results.txt`, `native-probe/probe*_stdout.log`,
`native-probe/probe*_*.log`) and
`channel-restore/outbound-probe-draft-*.md`. `native-probe/gw2.py` and
`README.md` stay tracked. See `docs/hatch-untracked-disposition.md`.

## Run

```sh
cd weixin-bot && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
cd ../wecom-bot && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
# then install the systemd units from each bot dir / channel-restore/
```

The gateway units set `HOME` and `MUSE_HOME` to `/home/hatch`, so the
gateways, slash commands, and the inbox hooks share one state
directory even when a unit's process home would otherwise be `/root`
(`channel_common.muse_home()` resolves the install home the same
way). `channel-restore` runs as root so it can reinstall systemd
units, including its own timer.

Syntax-verified 2026-10-04: all Python files compile, shell scripts pass
`bash -n`, hook JSON parses.

## Hooks (message wake layer)

The gateways only move messages; what wakes the agent is a pair of
inbox hooks, one per channel:

- `hooks/scripts/weixin-inbox.sh`, `hooks/scripts/wecom-inbox.sh` — poll
  the gateway inbox, batch messages, and supervise the running worker:
  queueing, preemption, starvation guard, lost-message reconciliation,
  and fail-stop backed by the worker's internal heartbeat. Wake payloads
  carry same-chat history under a 30000-char budget (recent turns
  verbatim, older turns digested). Channel histories stay isolated.
- `hooks/definitions/weixin-inbox.json`, `hooks/definitions/wecom-inbox.json`
  — the hook definitions: poll interval plus the worker prompt
  (channel rules and reply discipline).

Install: copy the scripts to `~/hooks/scripts/` and register the
definitions through your agent platform's hooks API, adjusting the
absolute paths (home dir, bot state dirs) to your deployment.

**Hard rule: never set `poll_interval_secs` below 5.** Five seconds is
the platform minimum; a lower value invalidates the definition,
suspends BOTH hooks, and silently swallows waiting messages (learned
twice in production, 2026-10-05). Change definitions only through the
hooks API, never by editing the JSON in place.

Cold-channel death watch (`DEATH_WATCH_SECS=1800` in the inbox hooks)
checks bridge and worker real activity before it resumes or cancels a
silent batch. A fresh signal holds the batch; a missing or unreadable
probe is not treated as life. The drill plan is
`docs/cold-death-drill-2026-10-09.md`. `ops/cold_death_drill.py` is
sandbox-only. The WeCom online second-judgment plant is
`ops/cold_death_live_second_judgment.py`; it stays off unless
`DEATH_WATCH_LIVE_PLANT=1` and
`DEATH_WATCH_LIVE_CONFIRM=wecom-second-judgment` are both set.
Do not run either against the live hatch until that plan's
authorization line is filled in.

## Tests

Sandbox suites live in `tests/`. They run the real hook scripts and
gateway module against stubbed state, CLI, and network — no live
channel is touched:

- `tests/test_hook_replyfile_completion.py` — hook supervision counts
  `reply_file` as batch completion (regression suite for the
  2026-10-06 false fail-stop of a delivered image). Plain python3.
- `tests/test_gateway_formalfix.py` — Weixin gateway: formal replies
  exempt from the 10-attempt dead-letter, the stuck-formal user
  notification, and large-file compression (PIL / ffmpeg). Run with
  the weixin-bot venv python (needs httpx + PIL) and ffmpeg on PATH;
  a >2MB test video can be supplied via `MUSE_TEST_VIDEO`.
- `tests/test_wecom_formal_exemption.py` — WeCom gateway: formal
  replies are likewise never dead-lettered. Run with the wecom-bot
  venv python.
- `tests/test_channel_common.py` — the shared `channel_common.py`
  guards (atomic writes, JSONL half-line handling, queue-admin
  merging, reply gate, path/URL allowlists). Run with the weixin-bot
  venv python (one case imports `login.py`).

## Reporting bugs — including via your own Muse / AI agent

The upstream repository is **Sirhap/muse-mobile-channels**
(https://github.com/Sirhap/muse-mobile-channels). Issues are enabled and
anyone with a GitHub account can file one there — no permission needed.

If you installed this project and your Muse (or another AI agent
working in this repo) helps you run it, this rule applies to that agent:

> When the user reports a bug in this project, or you find a reproducible
> bug in it, file the issue **upstream** — to
> `Sirhap/muse-mobile-channels`, not to the user's fork — unless the
> problem is specific to the user's own fork or local changes.

Before filing, the agent MUST:

1. Search upstream for duplicates first:
   `gh issue list --repo Sirhap/muse-mobile-channels --search "<keywords>"`
   If an open issue already covers it, add a comment there instead.
2. Include: what happened, what was expected, exact reproduction steps,
   environment (OS, Python version, gateway: weixin / wecom), the commit
   hash being run, and relevant log excerpts.
3. Strip all secrets and personal data from logs before posting: no
   credentials, tokens, `credentials.env` contents, user IDs, or chat
   content. When in doubt, leave it out.

Filing (requires the user's own GitHub login — `gh auth login`; GitHub does
not allow anonymous issues):

```sh
gh issue create --repo Sirhap/muse-mobile-channels \
  --title "[bug] <short summary>" \
  --body "<symptoms, expected behaviour, reproduction steps, environment, sanitized logs>"
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full reporting rules.

## License

MIT — see [LICENSE](LICENSE).
