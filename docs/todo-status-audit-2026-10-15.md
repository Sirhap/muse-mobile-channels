# 待办状态审计（2026-10-15）

审计对象：`docs/待办问题总清单-2026-10-13.md`（仓库 HEAD `927a853`，分支 `cursor/4-61c8`）。
本次只新增本文件，没有改总清单，也没有改代码。

## 边界

生产环境不在审计范围。文档里的 live 送达 id、客户端是否真的收到、systemd 当前是不是 active、生产进程 HEAD 是不是某个提交，本审计都不判定，也不用「仓库里没有生产日志」去推翻或坐实这些说法。

只核对仓库里能定位的三样东西：功能代码在不在、点名的测试套件在不在且名字和条数对不对、点名的证据目录或证据文件在不在仓库树里。找不到就写「仓库内无此证据」。

判定用三个结果：

- 一致：点名的代码、测试文件、条数、仓库内证据路径都对得上。live 或生产运行状态另注「不判定」，不因此改判。
- 不一致：至少一条点名的仓库路径、测试文件或条数对不上。
- 仓库无法复核：关单依据是仓库外的原始日志、live 记录或磁盘文件，仓库里没有对应证据路径。按上面的边界，不把这种情况写成生产已通过或未通过。

条数以套件结束时打印的 `N/N passed` 为准。本环境没有现成的 `muse_cli`，桥套件是在仓库外放了一份最小桩之后跑的；网关套件额外安装了 `httpx`、`Pillow`、`websockets`。这些都没有写入仓库。`tests/test_hook_fnconfirm_20261014.py` 默认读 `~/hooks/scripts`，本审计把该路径指到仓库 `hooks/scripts` 后实跑。`tests/test_status_active_20261014.py` 与 `tests/test_status_fullcount_20261013.py` 的总览函数会读 `/home/hatch/workspace/native-bridge/state.json`（该文件被 `.gitignore` 忽略）；实跑前放了一份空的 `{"channels":{}}`，跑完已删除。

## 范围

总清单里仍未标关闭的是待办 1、2、11、12、13、14、19、20、21。其中 19、20 写的是「备查」，没有放进文末「已关闭项」。用户点名的待办 22、23 在总清单里没有条目，下面不单独对照代码。

文首「汇总依据」里点名、但仓库内无此证据的文件：

- `open-issues-reconcile-draft-2026-10-10.md`
- `open-issues-reconcile-draft-2026-10-11.md`
- `live-acceptance-runbook-2026-10-13.md`
- `acceptance-fix-2026-10-08/FIX-SPEC.md`（同目录只有 `wecom_outbound_coalesce_v2.py` 与 `test_wecom_coalesce_v2.py`）

已关闭项抽查了编号 3、4、5、6、7、8、9、10、15、16、17、18，以及文末已关闭清单的第 2、4、6、7、8、10 项。

## 未关闭项

### 待办 1：大文件转投企微

- 文档状态：未验。逻辑测试 `test_gateway_filefix_20261008.py` 34/34，覆盖上传失败不静默丢弃、通知用户、转投企微、自适应压缩阶梯。
- 仓库证据：`tests/test_gateway_filefix_20261008.py` 存在。本审计实跑 34/34。实现在 `weixin-bot/gateway.py`：`_notify_stuck_formal`、`FILE_COMPRESS_THRESHOLD`、`_compress_level_for`，上传阶段失败会通知并转投。
- 是否一致：一致。
- 差异说明：真实客户端发文件的结果不判定。

### 待办 2：/new 出口审计

- 文档状态：未验。`test_bridge_audit_20261008.py` 30/30。P4 已在代码里，旧会话原文命中会拦截。
- 仓库证据：`tests/test_bridge_audit_20261008.py` 存在。本审计实跑 30/30。拦截逻辑在 `native-bridge/native_bridge.py` 的 `build_audit_corpus`、`audit_reply`，以及 `/new` 时写入的 `fresh_session_id` / `audit_fingerprints`。
- 是否一致：一致。
- 差异说明：用户真实发送 `/new` 的呈现不判定。

### 待办 11：微信同回合文字加文件

- 文档状态：未验。微信侧修复已进仓库，沙箱 `test_reply_file_after_formal.py` 14/14。
- 仓库证据：`tests/test_reply_file_after_formal.py` 存在。本审计实跑 14/14。`weixin-bot/gateway.py` 的 `dispatch_outbox_item` 只对 `mode == "update"` 写 `suppressed: msgid already has a delivered formal reply`，同 msgid 的 `reply_file` 仍走发送。提交 `ea26885` 是当前 HEAD 的祖先。
- 是否一致：一致。
- 差异说明：微信 live 是否已经复验不判定。

### 待办 12：两拍关单

- 文档状态：未验。PR#21 两拍关单随 `2a058eb` 进入代码，文档另写生产现为 `ec4df80`。
- 仓库证据：`native-bridge/native_bridge.py` 有 `TRAILING_QUIET_SECS = 10`，以及 `trailing_quiet_seen` 连续两次确认后打印 `trailing window closed msgid=... outbox_ok=`。提交 `26c1ecd`、`2a058eb`、`ec4df80` 都在当前历史上。`tests/test_bridge_longtask_20261008.py` 本审计实跑 107/107，输出里有 `trailing window closed`。
- 是否一致：一致。
- 差异说明：生产进程现在停在哪个提交不判定。仓库 HEAD 是 `927a853`，晚于文档点名的 `ec4df80`。

### 待办 13：久等提醒 live

- 文档状态：未验。沙箱 `test_wecom_wait_remind` 35/35；双网关 feedback 闩锁改为落行成功后再置闩。
- 仓库证据：`tests/test_wecom_wait_remind_20261014.py` 存在。本审计实跑 35/35。`weixin-bot/gateway.py` 与 `wecom-bot/gateway.py` 都是 `WAIT_REMIND_SECS = 180`，`_feedback_scan_once` 在 `_queue_notice` 成功之后才写 `wait_reminded` / `started_notice`。对应提交 `6bc0925`。
- 是否一致：一致。
- 差异说明：B 轮 live 不判定。

### 待办 14：「Delivery not confirmed」

- 文档状态：待证据。只读全搜没有找到该字符串的生成点，要求拿到截图前不在渠道代码里打补丁。
- 仓库证据：全库检索，`Delivery not confirmed` 只出现在 `docs/待办问题总清单-2026-10-13.md`、`docs/open-issues-2026-10-09.md`、`docs/delivery-not-confirmed-origin-2026-10-09.md`。Python 与 hook 脚本里没有这个字符串。来源图文档在仓库里。
- 是否一致：一致。
- 差异说明：无。截图与生产界面不在本次范围。

### 待办 19：冷通道判死前三轮验收分歧

- 文档状态：备查，未明，不阻塞关单。演练 4b 已 PASS；前三轮分歧根因未明。正文是否含字面 DRILL 以 hatch outbox 原文为准。
- 仓库证据：分歧记录写在 `docs/open-issues-2026-10-09.md` 第 5 项和 `docs/cold-death-drill-2026-10-09.md`。配套脚本 `ops/cold_death_drill.py`、`ops/cold_death_live_second_judgment.py` 仍把 `DEATH_WATCH_SECS` 固定为 1800。仓库里没有新的代码把「展示链路吞前缀」定位并修掉。
- 是否一致：一致。
- 差异说明：演练当时的 hatch 原文不在本次范围。仓库里保留的是文档转写和 `CONTENT_B64`。

### 待办 20：整机换代抹掉 systemd unit

- 文档状态：备查。定时器加心跳巡检双保险；2026-10-13 实查 timer 为 enabled 且 active。
- 仓库证据：`HEARTBEAT.md` 写明 unit 被抹掉后的恢复规程。`channel-restore/restore.sh`、`channel-restore/liveness-probe.sh`、`channel-restore/channel-restore.timer` 都在仓库里。`HEARTBEAT.md` 同时写明 `approval-relay.service` 的 unit 文件不在本 git 树。
- 是否一致：一致。
- 差异说明：当前机器上 timer 是否 active 不判定。

### 待办 21：冷通道 worker 单独死亡的判死信号

- 文档状态：待决策。方案已出，未批准实施，代码未动。方案文档 `docs/worker-death-signal-plan-2026-10-14.md`（`aa40b18`，补遗 `18c9383`），研究文档 `docs/gateway-scenario-research-2026-10-14.md`（`0eeb93a`）。
- 仓库证据：上述三份文档和三个提交都在当前历史上。`hooks/scripts/weixin-inbox.sh` 与 `hooks/scripts/wecom-inbox.sh` 的 `_hb_pid_alive` 仍写明 `heartbeats/<msgid>.pid` 是心跳循环自己的 pid，hook 不记录 worker pid。仓库内搜不到 `wpid` 或 `worker_pid`。`DEATH_WATCH_SECS = 1800` 仍在两份 hook 里。
- 是否一致：一致。
- 差异说明：无。

## 已关闭项抽查

### 待办 3：查实工人卡死根因

- 文档状态：已查实（2026-10-14）。两批次停在读完 subscription-status 的 SKILL.md 之后；证据是网关 state、hooks state、桥 fallback 与 muse.db 转录。10-13 晚 hook 日志与 journal 已随整机换代丢失。
- 仓库证据：`docs/gateway-scenario-research-2026-10-14.md` 第 97–100 行转述了同一结论。`muse.db`、网关 state、hooks state、hook 日志在仓库内无此证据。
- 是否一致：仓库无法复核。
- 差异说明：总清单自己已经写明原始日志丢失。仓库里只有研究文档的转述，没有它所依据的 state 和转录。

### 待办 4：静默提醒提前

- 文档状态：已上线。`STALL_FIRST_NOTICE_SECS=300`，已推送过的仍是 1200 秒。沙箱 48/48 两轮，hook 套件双口径 30/30。证据 `~/workspace/deathwatch-early-2026-10-14/REPORT.md`。
- 仓库证据：两份 hook 都有 `STALL_FIRST_NOTICE_SECS = 300` 和 `STALL_NOTICE_SECS = 1200`，零推送走独立 `first:` 节流键（`hooks/scripts/weixin-inbox.sh`、`hooks/scripts/wecom-inbox.sh`）。`tests/test_hook_replyfile_completion.py` 本审计对仓库脚本实跑 30/30。`deathwatch-early-2026-10-14/` 与其中的 `REPORT.md`、`run_scenarios.py` 在仓库内无此证据。`tests/test_hook_fnconfirm_20261014.py` 的注释提到过这个目录。
- 是否一致：不一致。
- 差异说明：300 秒常量与 hook 套件 30 条在仓库里，并且 30/30 已实跑通过。文档点名的沙箱 48/48 和证据目录不在仓库。第二份「线上脚本」口径也没有单独的仓库副本。

### 待办 5：续跑信号提前

- 文档状态：已评估并上线。`EARLY_RESUME_MIN_SILENCE_SECS=300`，`EARLY_RESUME_HB_STALE_SECS=180`。fail-stop 文案去掉写死的「30 分钟无推送」。证据与待办 4 相同。
- 仓库证据：两份 hook 都有这两个常量，并在静默与心跳 pid 条件满足时进入原判死路由。fail-stop 正文是「长时间无推送，自动续跑后仍无推送」（`weixin-inbox.sh` 约第 1117 行，企微对应位置相同）。首提醒文案仍写「无推送满 30 分钟将自动续跑」，那是 stall 提醒，不是 fail-stop。证据目录与待办 4 相同，仓库内无此证据。
- 是否一致：不一致。
- 差异说明：提前续跑的代码和 fail-stop 文案与文档相符。文档把关单证据指到 `deathwatch-early-2026-10-14/REPORT.md`，该路径不在仓库。

### 待办 6：任务执行状态可见

- 文档状态：已实施并部署。桥 `status.json` 有每渠道 active 摘要，最多 3 条，每 15 秒刷新。双网关总览带「最后活动」。`test_status_active` 30/30，`fullcount` 14/14。
- 仓库证据：`native-bridge/native_bridge.py` 的 `_active_summary` 最多 3 行，`run` 里按 15 秒调用 `write_status`。两个网关的 `_task_overview_text` 渲染「最后活动」。套件全名是 `tests/test_status_active_20261014.py`、`tests/test_status_fullcount_20261013.py`。本审计实跑 30/30 与 14/14。
- 是否一致：一致。
- 差异说明：文档用了不带日期后缀的套件名，仓库文件名带 `_20261014` / `_20261013`，对应关系唯一。用户下一次 `/status` 的 live 呈现不判定。

### 待办 7：卡死场景测试并做生产实证

- 文档状态：已完成。企微 1:1 演练两条都 PASS，送达 id `4b2dd0523943`、`5aea7a83ba3e`。
- 仓库证据：提前提醒和提前判死的代码见待办 4、5。这两个 outbox id、演练快照和清理记录在仓库内无此证据。
- 是否一致：仓库无法复核。
- 差异说明：完成标准写的是 live `ok=true`。仓库里没有这份演练记录，本审计不判断那次演练是否发生。

### 待办 8：主动推送做成常态机制

- 文档状态：已审计并补缺。报告 `proactive-push-audit-2026-10-14/REPORT.md`。G1 `test_proactive_rows` 14/14，G2 企微 stuck-formal，G3 `test_hook_fnconfirm` 10/10，G5 去掉 PROTOTYPE。G4 巡检范围仍只含微信，规则在 `HEARTBEAT.md` 和 `MEMORY.md`。
- 仓库证据：`tests/test_proactive_rows_20261014.py` 本审计实跑 14/14。`native-bridge/native_bridge.py` 的 idle 与 alert 行带 `"proactive": True`。两个网关对 `proactive` 行不做 10 次死信。`wecom-bot/gateway.py` 的 `_notify_stuck_formal` 在 `FORMAL_STUCK_NOTIFY_ATTEMPTS = 3` 时写本渠道 send，并往微信 outbox 追加文本；文件行再追加 `send_file`。`tests/test_hook_fnconfirm_20261014.py` 在指向仓库 hook 后实跑 10/10。两份 hook 的失败通知按 `queued_id` 对 `outbox_results`，保留 86400 秒。仓库内搜不到 `PROTOTYPE`。`proactive-push-audit-2026-10-14/REPORT.md` 与 `MEMORY.md` 在仓库内无此证据。仓库根 `HEARTBEAT.md` 写的是 systemd unit 被整机换代抹掉之后的双保险，没有「主动推送巡检只覆盖微信」的条文。
- 是否一致：不一致。
- 差异说明：G1、G2、G3、G5 的代码和两个套件的条数与实跑结果相符。文档点名的审计报告、`MEMORY.md`，以及「HEARTBEAT 里只巡检微信」这一描述，在仓库里对不上。

### 待办 9：进度提示绑定真实清单

- 文档状态：已实施并部署。清单路径 `progress_lists/<channel>-<msgid>.json`，命令在 `native-bridge/progress_list.py`。`test_progress_list` 25/25，`progress` 48/48。
- 仓库证据：`native-bridge/progress_list.py` 提供 set / done / clear。`native-bridge/native_bridge.py` 的 `_progress_list_line` 折入「清单 k/n：当前项」，关单时 `_drop_progress_list`。套件全名 `tests/test_progress_list_20261014.py`、`tests/test_bridge_progress_20261008.py`。本审计实跑 25/25 与 48/48。
- 是否一致：一致。
- 差异说明：带清单长任务的 live 呈现不判定。

### 待办 10：桥进度首条时间

- 文档状态：已实施。`PROG_FIRST_SECS` 120 改为 45（`549ca31`）。当时记录 progress 44/44、statuspush 16/16、longtask 107/107。
- 仓库证据：`native-bridge/native_bridge.py` 第 42 行 `PROG_FIRST_SECS = 45`。提交 `549ca31` 在历史上，说明文字也写了 44/44。该提交之后 `tests/test_bridge_progress_20261008.py` 没有再改。本审计实跑这份文件是 48/48，`tests/test_statuspush_taskcode_20261009.py` 16/16，`tests/test_bridge_longtask_20261008.py` 107/107。48 比 44 多出来的 4 条，是「PREAMBLE drops」循环里的 4 次断言。
- 是否一致：不一致。
- 差异说明：45 秒、提交号、statuspush 16/16、longtask 107/107 都对得上。progress 套件的当前实跑条数是 48/48，与待办 9 一致，与待办 10 和 `549ca31` 说明里的 44/44 不一致。三渠道 `idle_scan_fail_streak=0` 是进程状态，不判定。

### 待办 15：test_statuspush_taskcode 去留

- 文档状态：已决策并执行。重写为无编号守卫，16/16，生产代码零改动。
- 仓库证据：`tests/test_statuspush_taskcode_20261009.py` 文件头写明 2026-10-14 重写，断言模板没有 `{code}` 和 `〔#〕`。本审计实跑 16/16。提交 `419c7b2`。
- 是否一致：一致。
- 差异说明：无。

### 待办 16：出站故障探针

- 文档状态：已立项并上线，提交 `2b44d8f`。连续失败不少于 3 次且跨度不少于 900 秒，或不少于 5 次；最近失败在 24 小时内判 BROKEN。沙箱 9/9。企微 CLI 用 `--chatid`。
- 仓库证据：`channel-restore/outbound_probe.py` 的 `MIN_STREAK = 3`、`MIN_STREAK_BIG = 5`、`MIN_SPAN_SECS = 900`、`FRESH_SECS = 86400`。对侧是企微时参数为 `--chatid`，对侧是微信时为 `--to`。`channel-restore/restore.sh` 每轮调用该脚本。`tests/test_outbound_probe_20261014.py` 本审计实跑 9/9。提交 `2b44d8f` 在历史上。原草案路径由 `.gitignore` 的 `channel-restore/outbound-probe-draft-*.md` 和 `docs/hatch-untracked-disposition.md` 覆盖，仓库里没有那份草案。
- 是否一致：一致。
- 差异说明：文档里的 live 首跑 id `2bae9f3c774a` 不判定。

### 待办 17：open-issues 回填

- 文档状态：已回填。`docs/open-issues-2026-10-09.md` 第 1、3、4 项补了 2026-10-14 的结论。需要回填的事实里还包括 P3#9 企微网关漂移已消解、P3#11 未跟踪草稿已处置。
- 仓库证据：`docs/open-issues-2026-10-09.md` 第 1 项有 2026-10-09 结案和 2026-10-14 复查（入站未证、出站 `prepare failed`）。第 3 项有机制通过和待办 11/12 的去向。第 4 项有 started 实证、`test_wecom_wait_remind` 35/35 和闩锁修复。第 5 项、P2 第 6–8 项已划掉。第 11 项指向 `docs/hatch-untracked-disposition.md`。第 9 项仍停在 2026-10-09 的「线上已漂移、未提交」描述，没有「已消解」的回填。
- 是否一致：不一致。
- 差异说明：状态段点名的第 1、3、4 项回填在原清单里。同文件要求回填的 P3#9 没有对应更新。

### 待办 18：教学视频追踪项

- 文档状态：已关闭。追踪项 `goal_58f03c4da19f` 已结案，`tutorial-video-fixed.mp4` 与 `tutorial.md` 在盘已核。
- 仓库证据：这三个名字在仓库内无此证据。
- 是否一致：仓库无法复核。
- 差异说明：关闭依据是仓库外的磁盘文件和追踪项。本审计不判断它们在不在别的机器上。

### 已关闭清单第 2 项：企微 Coalescer

- 文档状态：已关闭。`wecom-bot/gateway.py` 已接入 Coalescer v2。
- 仓库证据：`wecom-bot/gateway.py` 的 `_load_coalescer_cls` 加载 `Coalescer`，约第 3107 行有集成注释。类定义在 `acceptance-fix-2026-10-08/wecom_outbound_coalesce_v2.py`。
- 是否一致：一致。
- 差异说明：无。

### 已关闭清单第 4 项：桥改造 P1 至 P4

- 文档状态：已完成。P1 文件失败通知、转投和压缩；P2 企微合并；P3 长任务进度阶段；P4 `/new` 出口审计。
- 仓库证据：P1 见待办 1 的网关代码与 34/34。P2 见上一项 Coalescer，另有 `tests/test_bridge_merge_wecom_20261008.py`。P3 见 `native-bridge/native_bridge.py` 的进度阶段文案，progress 套件 48/48。P4 见待办 2，audit 套件 30/30。
- 是否一致：一致。
- 差异说明：「已上线」的生产运行状态不判定。

### 已关闭清单第 6 项：审批中转空内容

- 文档状态：已修复上线。字段补查加空内容拦截，测试 7/7。
- 仓库证据：`weixin-bot/gateway.py` 与 `wecom-bot/gateway.py` 都写明审批通知正文由 `approval_relay.py` 的 `approval_notice_text` 组成，并且该文件不在本仓库。仓库内没有 `prefer` 之外的空内容审批拦截实现，也没有名为 7/7 的对应测试。网关里另有 2026-10-14 的空 send 丢弃（`empty send content; dropped, not sent`），覆盖的是桥 alert 空正文，套件是 `tests/test_bridge_alertfix_20261014.py`，与「审批中转 7/7」不是同一项。
- 是否一致：不一致。
- 差异说明：文档点名的审批中转实现和 7/7 测试在仓库内无此证据。

### 已关闭清单第 7 项：审批通知路由

- 文档状态：已部署。路由名 `prefer_source_else_both`。
- 仓库证据：全库检索没有 `prefer_source_else_both`。审批中转目录不在本 git 树（`HEARTBEAT.md` 也这么写）。
- 是否一致：不一致。
- 差异说明：仓库内无此证据。

### 已关闭清单第 8 项：审批口令 `/a` `/b` `/c`

- 文档状态：已落地。
- 仓库证据：两个网关都有 `APPROVAL_LETTER_DECISIONS = {"a": "allow_once", "b": "allow_always", "c": "deny"}`。`tests/test_approval_slash_aliases_20261010.py` 本审计实跑 151/151。文档没有给这条写条数。
- 是否一致：一致。
- 差异说明：无。

### 已关闭清单第 10 项：引用消息内容丢失

- 文档状态：已修复并 live 确认。
- 仓库证据：`weixin-bot/gateway.py` 有引用摘录（`_quoted_text`，非文本引用不再只剩一个光秃词）。`tests/test_quote_extract_20261011.py` 在仓库里。本审计没有实跑这个套件。
- 是否一致：一致。
- 差异说明：live 确认不判定。

## 汇总

对照 27 项：未关闭 9 项（1、2、11、12、13、14、19、20、21），已关闭抽查 18 项。

- 一致 17 项：待办 1、2、6、9、11、12、13、14、15、16、19、20、21，以及已关闭清单第 2、4、8、10 项。
- 不一致 7 项：待办 4、5、8、10、17，以及已关闭清单第 6、7 项。
- 仓库无法复核 3 项：待办 3、7、18。

待办 22、23 不是这 27 项里的差异，总清单中没有这两条。待办 19、20 仍是未关的备查项。

差异摘要：

1. 待办 4、5 的代码在仓库里，hook 套件 30/30 已实跑通过；点名的 `deathwatch-early-2026-10-14` 报告和沙箱 48/48 套件不在仓库。
2. 待办 8 的 G1–G3 代码与 14/14、10/10 已实跑通过；点名的审计报告和 `MEMORY.md` 不在仓库，`HEARTBEAT.md` 也没有「巡检只含微信」的条文。
3. 待办 10 的 `PROG_FIRST_SECS=45` 在代码里，statuspush 16/16 与 longtask 107/107 已实跑通过；progress 套件实跑是 48/48，文档写的是 44/44。
4. 待办 17 点名的 open-issues 第 1、3、4 项已经回填；P3#9 仍停在 2026-10-09 的漂移描述。
5. 已关闭清单第 6、7 项的审批中转实现、7/7 测试和 `prefer_source_else_both` 都不在本仓库。
6. 待办 3、7、18 的关单依据是仓库外的 state、live id 或磁盘文件，仓库内无此证据，本审计不判断生产侧是否已经发生。
