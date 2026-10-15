# fencing 双投 live 实验方案（仅 test 渠道，只预备）

写于 2026-10-15。本文是实验方案。写这份方案的过程没有栽种、没有向任何渠道发消息、没有重启服务，也没有改网关防护代码。授权表空白时，实验视为未授权，命令不要执行。

## 实查范围

检索了仓库全文、`tests/` 文件名和 git 历史里的 fencing、delivered-suppression、27/27。结论如下。

已落地的 `mode=reply` 已送达抑制在双网关的入队门，不在发送循环里。两边 CLI 都调用 `channel_common.reply_block_reason_for_row`：

- `channel_common.py` 的 `reply_block_reason_for_row`：`mode` 不是 `reply` 的行直接返回空；`result.ok is True` 时返回 “delivered successfully”。
- `weixin-bot/weixin_cli.py` 的 `reply_block_reason`，以及 `reply` 子命令在入队前的阻断（注释写明 fix #1，2026-10-04）。
- `wecom-bot/wecom_cli.py` 的同名函数和 `reply` 子命令，规则相同。

`update` 和 `reply-file` 子命令复用了这道门，但扫描对象仍只有 `mode=reply` 的行。一条已经送达的 `reply_file` 不会让这道门返回理由。这就是窄残余：`reply_file` 是另一种行，未进入已送达抑制。

发送循环是另一道门，不要和本实验混在一起。微信 `dispatch_outbox_item` 只在 `mode == "update"` 时调用 `_delivered_reply_before`；企微还包含 `reply_notice`。那道门会把排在前面的、已经 `ok=true` 的 `reply` 或 `reply_file` 当成“已有正式回复”，用来丢掉迟到的进度行。它不丢 `reply_file` 本身，也不丢已经写进 outbox 的第二条 `mode=reply`。所以本实验若把第二条 `reply` 写进出箱，网关仍会发送，实验就变成了它要防止的双投。

`tests/` 下没有文件名含 fencing 的套件，仓库里也没有 27/27 的计数记录或证据目录。和这道入队门直接对应的沙箱断言在 `tests/test_channel_common.py`，共 5 个：`test_recent_failure_blocks_a_second_reply`、`test_old_failure_without_retry_allows_another_reply`、`test_active_retry_record_keeps_blocking`、`test_dead_letter_allows_another_reply`、`test_delivered_reply_blocks`。其中没有 `reply_file` 行。

`docs/待办问题总清单-2026-10-13.md` 的待办 21 是冷通道 worker 单独死亡的判死信号，状态为待决策、方案已出、代码未动。它不能当作这道已送达抑制的上线记录。抑制代码的注释日期是 2026-10-04。

本机缺少 live 证据目录，方案不补造：

- `/tmp/bridge-test` 与 `/tmp/bridge-test/state` 不存在。仓库 `native-bridge/config.json` 里 test 渠道的 `bot_state` 就是这个路径。
- `/home/hatch` 不存在。因此看不到线上 `native-bridge` 的 `enabled-test`、`state.json`，也看不到微信、企微的 state。
- 仓库内 `weixin-bot/state` 与 `wecom-bot/state` 也不存在。

## 只允许 test 渠道

实验只碰 test 渠道的 outbox。微信、企微、production、prod、hatch、live、both，以及任何旁路参数（`--live`、`--force`、指向 `/home/hatch` 或 `weixin-bot` / `wecom-bot` 的参数）一律拒绝。

test 渠道没有自己的 `weixin` / `wecom` CLI。两边 CLI 的状态目录写死在各自 bot 目录下，不能靠环境变量改到 test。实验因此直接对 test 状态目录调用两边 CLI 共用的 `reply_block_reason_for_row`。一次通过说明的是这道共享入队门，不是某个微信或企微进程被跑过。

脚本 `ops/fencing_dual_deliver_experiment.py` 默认退出码 2。渠道参数必须恰好是 `--channel test`，并且同一条命令的环境里 `FENCING_DUAL_DELIVER_EXPERIMENT=1`。两个条件缺一个都拒绝。真实渠道在开关未打开时也拒绝。`FENCING_STATE_DIR` 或 `FENCING_DUAL_DELIVER_STATE` 一旦出现，视为改写栽种目录，同样拒绝。脚本不调用 systemctl，不调用渠道 CLI，不修改网关代码。

授权记录（空白即未授权，下面的栽种全部不做）：

| 项 | 填写 |
| --- | --- |
| 授权人 | |
| 时间 | |
| 渠道 | 只能填 `test` |
| 范围 | 只能填「仅 test 渠道状态目录」 |
| test 状态目录实查 | |
| 当时没有进行中的 test 回合 | |

## 前置条件

全部满足才可以执行脚本。有一条不满足，脚本应在写入之前退出，退出码 3。

1. 上一表已填写，渠道是 test，范围是仅 test 渠道状态目录。
2. `native-bridge/config.json` 的 `channels.test.bot_state` 仍是 `/tmp/bridge-test/state`。脚本把这个路径写死核对，配置若被改到别的目录，拒绝栽种。
3. 该目录存在，是目录，解析后的真实路径不落在 `/home/hatch`、`weixin-bot` 或 `wecom-bot` 下，也不是指向这些位置的符号链接。
4. `outbox.jsonl` 与 `outbox_results.jsonl` 若存在，末尾是完整一行，且文件里没有 `drill-fencing-`。已有残留时先做文末的清理，不要叠加上一次的栽种。
5. 操作者已确认 test 渠道没有进行中的桥回合。线上桥的状态在 `/home/hatch/workspace/native-bridge/state.json`，live 标志是同目录的 `enabled-test`。本机这两处都不存在，所以这一条目前无法在本机核销。
6. 不重启 `weixin-bot`、`wecom-bot`、`native-bridge`，不改 `DEATH_WATCH_SECS`、轮询间隔或 hook 定义。

2026-10-15 本机核对：第 2 条的配置值成立，第 3 条和第 5 条的目录缺失，授权表为空。live 实验现在不能开始。

## 栽种方式

只通过下面这一条命令栽种。不要手改微信或企微的 outbox，不要对真实 msgid 再入队一条 reply。

```sh
FENCING_DUAL_DELIVER_EXPERIMENT=1 \
  python3 ops/fencing_dual_deliver_experiment.py --channel test
```

脚本在通过两道门之后做这些事：

1. 再核对 test 状态目录。目录不存在则退出码 3，并且不创建该目录。
2. 把 `outbox.jsonl`、`outbox_results.jsonl`、`outbox_retry.json` 的原字节抄到 `/tmp/fencing-dual-deliver-snap-<时间>-<pid>/`。原来没有的文件在清单里记为不存在。
3. 追加一条栽种行，且只追加这一条 `mode=reply`：`msgid` 与 `id` 都以 `drill-fencing-` 开头，正文是「【DRILL fencing】双投实验，不是用户任务。」同时在 `outbox_results.jsonl` 写上同一 `id` 的 `ok=true`。
4. 用生产函数 `reply_block_reason_for_row` 扫描这个目录，问同一 msgid 的下一次 `mode=reply` 会不会被挡住。第二次 reply 不入队。
5. 用同一函数在内存里看一条 `mode=reply_file` 且 `ok=true` 的行。这一步不写盘。按当前代码它应返回空，报告里记为残余仍在。
6. 用快照字节把三个文件放回去。原来不存在的文件删除。确认栽种 `id` 已消失，再写 `report.json`。

读状态失败时实验不按“放行”处理。生产 CLI 在读失败时会放行，实验若照做，会把读失败记成抑制没生效。实验改为回滚并失败。

## 期望结果

- 扫描返回的理由包含 `delivered successfully`，并带有栽种行的 outbox id。
- 该 msgid 在 outbox 里始终只有栽种的那一行。观察结束后行数不增加。
- 报告里 `second_reply_queued` 为 false。
- `residual_reply_file_uncovered` 为 true。这表示窄残余还在：已送达的 `reply_file` 盖不住下一次 `mode=reply`。本实验不把这个残余修掉，也不把它算成实验失败。
- 回滚后，outbox 与 results 里都找不到这次的 `drill-fencing-` id。
- 微信、企微的状态目录没有新文件。本实验不读取、不写入那些目录。

## 回滚与清理

脚本用 `try/finally` 做回滚。正常结束时栽种行已经撤掉，快照留在 `/tmp` 供核对。核对完删除整个快照目录即可。

脚本中途被杀死、回滚没走完时，按快照目录里的 `manifest.json` 手工清理，仍然只动 test 状态目录：

1. 清单里 `existed` 为 true 的文件，用快照里的同名文件整文件覆盖回去。
2. 清单里 `existed` 为 false 的文件，若目录里新出现了该文件，删掉它。
3. 再查 `outbox.jsonl` 和 `outbox_results.jsonl`，确认没有 `drill-fencing-`。有则只删除含该前缀的行，保留其他行。
4. 确认没有第二个进程仍在往这个目录追加，然后删除快照目录。

不要为了清理去重启网关，也不要到微信或企微的 state 里找这行。那两个目录不应该出现 `drill-fencing-`。若出现了，说明跑错了目标，先停下来对照快照，不要继续栽种。

## 判定通过的标准

同时满足才记通过，脚本退出码 0，报告里 `pass` 为 true：

1. 渠道是 test，开关当时为 `FENCING_DUAL_DELIVER_EXPERIMENT=1`。
2. 栽种的 `mode=reply` 在 results 里为 `ok=true`。
3. 下一次 `mode=reply` 被挡住，理由含 `delivered successfully`。
4. 第二条 `mode=reply` 没有写入 outbox。
5. 回滚后栽种 id 不在 outbox，也不在 results。
6. 报告写明 `reply_file` 残余仍未被这道门覆盖。

任一发送类动作出现，实验作废：调用了微信或企微 CLI、往非 test 目录写了行、outbox 里出现第二条同一 msgid 的 reply、或网关日志里出现这次 msgid 的真实下发。退出码 1 表示栽种已尝试回滚但抑制没有生效，或回滚本身失败。退出码 2 是防护门拒绝，盘上不应有新栽种。退出码 3 是前置条件不满足，同样不应有新栽种。

沙箱只验证防护门和“目录缺失时不创建目录”。沙箱通过不能代替上面 6 条 live 标准。
