# 「Delivery not confirmed」来源图（只读）

核查基准是 muse-mobile-channels 当前 main `ce644a6cac9e5cd51b0ae53af4bab060a671940b`（2026-10-09，`企微出站按时间顺序上屏：开始、进度、正式回复走同一条回复链` #7）。公开 muse-cli 对照是 `nikships/muse-cli` `ebcb6310432ad2d39927bcb59e686330b0538310`（2026-10-03，`Merge pull request #3 from serrebidev/interactive-chat`）。这次只读，没有改渠道代码，也没有给这句文案找一个还不存在的补丁点。

## 结论

这句英文在能读到的源码里没有生成位置。渠道仓库里它只出现在待办清单对自己的转述。公开 muse-cli 的交互聊天同样不打印它。桥关单日志写的是 `outbox_ok=true`、`outbox_ok=false` 或 `outbox_ok=pending`，那是网关出站回执的内部标记。

下一份有用的材料是一张能分清「界面状态」和「消息正文」的截图。截图落到某一屏之前，不要在微信、企微或桥的代码里为这个字符串打补丁。

## 检索证据

本仓库按文本文件扫过 49 个 `.py`、`.md`、`.json`、`.sh`、`.txt`、`.service` 以及 `weixin` / `wecom` 入口脚本。本仓库没有 `approval-relay` 目录，这次没有重扫那棵树；先前待办里已经记过那里没有这句。

| 检索式 | 命中 |
|---|---|
| `Delivery not confirmed` | 2，都在 `docs/open-issues-2026-10-09.md` 第 14、18 行（待办转述） |
| `delivery_confirmed` | 0 |
| `not confirmed`（不区分大小写） | 同上 2 行 |
| 大写开头的 `Delivery` | 上述 2 行，另加 `weixin-bot/weixin_cli.py:50` 与 `wecom-bot/wecom_cli.py:53` 的注释 `Delivery results live in` |
| `outbox_ok` | 6：待办 3 处，桥代码 3 处（`native-bridge/native_bridge.py` 272、1058、1163） |
| `delivered successfully` | `channel_common.py:175`，以及对应测试 |
| `待确认` | `weixin-bot/gateway.py:2728`，中文文件失败通知 |
| `发送状态` | `weixin-bot/gateway.py:2724` 与 `2731` |

公开 muse-cli 的 `src/muse_cli/chat.py`、`cli.py`、`gateway.py`、`docs/PROTOCOL.md`、`routes.json` 里没有 `Delivery not confirmed`、`delivery_confirmed`，也没有单词 `deliver`。`routes.json` 是从 muse.ai 网页包抽出的 258 个方法名，不是界面文案。其中最接近的名字是 `nodes.invoke_ack`（`/nodes/invoke-ack`）和 `pin.reminder_confirm`（`/api/pin-reminder/confirm`）。

GitHub 代码搜索这次返回 HTTP 429，所以没有做完全网普查。网页索引里，字面相同的「Delivery not confirmed」出现在无关项目 yetone/cumora 的界面文案；hermes-agent 的日志写的是 `final stream delivery not confirmed`。这两处都不是本渠道栈，不能当成 Muse 主会话的来源。

## 来源图

### 本仓库（微信 / 企微 / 钩子 / 桥）

用户能在手机上看到的失败提示是中文，而且写明的是微信上传或发送失败，不是这句英文。文件卡死时，微信网关直接发给用户的通知在 `weixin-bot/gateway.py` 的 `_notify_stuck_formal`：服务端尚未接受文件、已转投企微、请到企微确认收到。企微模板卡片点「确认」之后，卡片标题会改成「已确认 ✅」（`wecom-bot/gateway.py` 的 `build_confirm_card` 点击更新）。那是按钮回执。

`channel_common.reply_block_reason_for_row` 在拒绝第二条正式回复时，会给调用方一句英文理由：`delivered successfully (outbox id …)`。调用方是 `weixin_cli.py` / `wecom_cli.py` 的幂等检查，打印对象是正在排队回复的进程，不是主会话界面。

`ce644a6` 把企微进度改成绑定 `msgid` 的 `reply_notice`，让开始、进度、正式回复走同一条 `aibot_respond_msg` 链。这次全文检索在该提交上仍然没有「Delivery not confirmed」。

### 桥的 `outbox_ok` 路径

`outbox_result_ok`（`native-bridge/native_bridge.py:262`）读渠道 `outbox_results.jsonl`，按出站行 `id` 取最新一条带 `ok` 的记录。有行则返回 true 或 false。文件打不开、还没有该 `id`、或渠道不在 live 模式（目录里没有 `enabled-<channel>`）时返回 `None`。`None` 在日志里印成 `pending`。关单不等这条回执，避免微信发送延迟卡住队列。

`_outbox_ok_label`（1058 行）只查本回合第一条绑定回复的 id。id 算法是 `sha256("bridge:{msgid}:")` 的前 12 位十六进制（`bridge_row_id`，245 行，后缀为空字符串）。trailing 后续段走各自的 idle 去重 id，不进入这个标签。

这行日志只在长任务尾窗关单时出现（1162–1163 行）：`longtask-<channel>` 旗标存在，`sessions.get` 为 `completed`，并且距上一条回复已静默 `TRAILING_QUIET_SECS`（10 秒）。日志原文是：

`trailing window closed msgid=<msgid> outbox_ok=<true|false|pending>`

没有 `longtask-<channel>` 时，第一条完成回复打完 `reply delivered` 就结束回合，不打 `outbox_ok`。异常结束（`sessions.get` 连续两次 completed、历史里没有完成回复）打的是 `turn ended abnormally`，同样没有 `outbox_ok`。

桥日志前缀是渠道名，例如 `[weixin]`，由 systemd 单元 `native-bridge.service` 收到标准输出。会话标题在 `native-bridge/config.json`：微信 `native-bridge-weixin`，企微 `native-bridge-wecom`。这两个会话由 `session.start` 新建（`origin: fresh`），历史查询带 `session_id`。它们不是账号的主会话。

### muse-cli

`chat.py` 的 `Chat.send` 打开 `chat.stream` 后打印 `Muse is thinking...`，每 3 秒拉一次 history，把新的 `message.assistant` 打成 `Muse: …`。300 秒内没有任何回复时打印：

`(No reply within 300 seconds. Type /history later to check.)`

连接失败时打印 `Connection problem …` 或 `Can't sign in to muse.ai: …`。`/main` 把 `session_id` 设成空，后续请求不带 `session_id`，这才是 CLI 里的主会话。`/chats` 对 `is_thread` 为假的会话追加 `(main chat)`。带 `session_id` 的是侧会话。`docs/PROTOCOL.md` 写明：`chat.subscribe` 的实时事件只覆盖主会话；侧会话回复要靠 history 轮询；`chat.stream` 的响应流只有发送回声，然后结束。CLI 不根据回声显示投递确认文案。

### 主会话界面与侧会话

「主会话」在本项目的口径里是 muse.ai 上那条不带 `session_id` 的主聊天，不是微信对话，也不是标题为 `native-bridge-*` 的桥会话。桥不向主会话写消息。因此，如果截图是官网主会话，这句不会是桥或网关插进去的气泡。

剩下还没读到源码的表面，是 muse.ai 网页或桌面客户端自己的界面。`routes.json` 只有方法名，没有按钮和状态文案。网页包本身不在本仓库，也不在这次对照的 muse-cli 检出里。在截图证明这句话画在主会话气泡外面的状态条上之前，不能把网页客户端写成已定位的来源。

侧会话有两条已知路径，都不生成这句：muse-cli 里带 `session_id` 的线程，以及桥自建的 `native-bridge-weixin` / `native-bridge-wecom`。如果这句话出现在某条消息的正文里，它是那条消息的内容（模型写出，或用户自己打出），要沿转录查找，而不是沿出站回执查找。

## 容易拍错的近邻

这些字符串都在，但都不是用户报告的那句：

- 桥日志 `outbox_ok=pending`。内部标记，用户界面上没有这一行。
- 微信文件通知里的「③ 待确认：请在企微确认收到」。
- 企微卡片标题「已确认 ✅」。
- CLI 幂等理由 `delivered successfully`。
- muse-cli 超时句 `(No reply within 300 seconds. Type /history later to check.)`。

## 给老板的取证清单

按实际看得到的那一屏来拍。一句英文贴在不同表面上，后续要查的树不一样。

1. 整屏一张。要能看见会话标题，以及这句话贴着哪一条消息。
2. 近景一张。看清这句话是在气泡外面的状态（灰色小字、发送失败、时钟图标一类），还是写在气泡正文里面。记下该条的时间、发送者是你还是 Muse、正文前 20 个字。
3. 若是 muse.ai 网页或 App：在会话列表里看这条会话是主会话，还是侧栏线程。标题若是 `native-bridge-weixin` 或 `native-bridge-wecom`，那是桥的侧会话，不是账号主会话。
4. 若是手机微信或企微：把含这句话的气泡拍全。渠道发给用户的失败通知是上面列出的中文模板。英文原句和那些模板对不上。
5. 若是终端里的 `muse-cli chat`：贴出这句话上下各约 10 行。公开 CLI 的超时文案是上面那句 300 秒提示。

同一时间窗再拉日志。把时间换成截图上的时间，前后各 10 分钟。不要贴 token、cookie 或整段私聊。

```bash
journalctl -u native-bridge --since "2026-10-09 18:00:00" --until "2026-10-09 18:20:00" --no-pager \
  | grep -E 'reply delivered|trailing reply delivered|trailing window closed|turn ended abnormally|outbox_ok='

journalctl -u weixin-bot -u wecom-bot --since "2026-10-09 18:00:00" --until "2026-10-09 18:20:00" --no-pager \
  | grep -E 'outbox (reply|reply_file|reply_notice|send|update|send_file) '
```

旗标决定桥会不会打 `outbox_ok`。目录是生产机上的 `/home/hatch/workspace/native-bridge/`：

```bash
ls -l /home/hatch/workspace/native-bridge/enabled-* \
      /home/hatch/workspace/native-bridge/longtask-*
```

`enabled-<channel>` 不存在时，`outbox_result_ok` 直接返回空，日志只能是 `pending`，而且出站写进 shadow。`longtask-<channel>` 不存在时，回合在第一条回复后结束，日志里有 `reply delivered`，没有 `outbox_ok`。

若关单行里有 `msgid=`，把下面命令里的 `MSGID` 换成那个值。打印出来的 12 位 id 就是桥去 `outbox_results.jsonl` 里查的绑定回复。到这两个文件里搜它：`/home/hatch/workspace/weixin-bot/state/outbox_results.jsonl`，`/home/hatch/workspace/wecom-bot/state/outbox_results.jsonl`。

```bash
python3 -c 'import hashlib,sys; print(hashlib.sha256(("bridge:%s:" % sys.argv[1]).encode()).hexdigest()[:12])' MSGID
```

结果文件里该 id 的最新 `"ok"` 为 true，桥日志就是 `outbox_ok=true`。为 false 就是 `outbox_ok=false`。没有该 id，或渠道不是 live，就是 `pending`。网关写这行的日志形式是 `outbox <mode> <id>: ok=<true|false>`（微信 `weixin-bot/gateway.py:3081`，企微 `wecom-bot/gateway.py:3537`）。

回传四件事就够下一轮定位：出现在哪一屏、在状态条还是正文里、会话标题、消息时间或 msgid。外加上面两条 `journalctl` 的匹配行。有了「状态条 + 官网主会话」之后，下一轮才去对 muse.ai 网页包里的界面字符串。有了「正文里的一句英文」之后，下一轮去对那条会话的 history，不改出站代码。
