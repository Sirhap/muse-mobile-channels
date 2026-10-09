# native-probe — Muse 原生网关协议探针套件

用途：复测 nikships/muse-cli 逆向的 Muse 原生网关协议（`wss://hatch.metaaivm.com/v1/noise` + Noise_XX + protobuf 信封），验证延迟、会话/上下文管理、生命周期可视性与取消能力。完整评估与每轮原始证据在 `~/workspace/muse-cli-protocol-eval-2026-10-07.md`；本目录是把 2026-10-07 四轮实测用过的脚本归档成可复跑的套件。结论不变：原生协议**不能**给微信/企微网关当生产入站（凭证托管、无指令注入入口、逆向未版本化接口），但只读观测能力已被逐项坐实。

## 文件

| 文件 | 内容 |
|---|---|
| `gw2.py` | 分片重组版客户端：`GW2` 子类（按 chunk_id/chunk_index 重组 NoiseTransportFrame，修复上游 muse-cli 的 DecodeError/串毒缺陷）+ 令牌 stdin 读取与校验码验证 |
| `probe1_latency.py` | 一次性延迟实测：建侧聊、三道小题、按 muse-cli 的真回复判别计时 |
| `probe_battery.py` | 四轮只读电池合并版（A–I 共九节，含参数课、fs/spaces/审批形状探针、订阅捕获、cancel 对照实验），结果增量落盘 |

## 令牌获取（一句话流程）

用户在自己已登录的 muse.ai 页面控制台运行 v4 脚本（页面内依次调 `/api/auth/check`、`/api/session`、`/api/hatch/token`，vmAddress 用 `wss://<vm_id>.metaaivm.com/`），把输出的 JSON 分 3 段连同每段校验码与总校验码贴回聊天；令牌是短期会话令牌（JWT 无 exp 字段，实测可多次连接），用完即弃。

## 校验码纪律（血换来的）

- 令牌 JSON 只走 stdin，**从不落盘、从不打印**；脚本先验 sha256（规范化重序列化后取前 10 位）与调用方给的期望校验码，**对不上立即退出、绝不连接**。
- 第一轮实测就是因为人工转录 ~700 字令牌抄错字符、且未先验码，连接被 403 拒；分段校验码（sha256[:6]/段）+ 总校验码（sha256[:10]）上线后转录一次全对。
- 对照基线（本机实测）：WSS 空令牌回 401、假令牌回 403、真令牌被拒=令牌无效，不是网络出口问题。

## 运行环境

- Python venv：`~/muse-test-venv`（依赖 `curl_cffi`、`noiseprotocol`、`protobuf`）。重建：`python3 -m venv ~/muse-test-venv && ~/muse-test-venv/bin/pip install "curl_cffi>=0.11" "noiseprotocol>=0.3" "protobuf>=5.0"`（放 home 而非 /tmp：虚拟机更换会清 /tmp，2026-10-07 已丢过一次）。
- 依赖包：`~/workspace/reference/muse-cli/src` 的 `muse_cli`（gw2.py 默认路径，可用环境变量 `MUSE_CLI_SRC` 覆盖）。
- 运行：`~/muse-test-venv/bin/python probe_battery.py <总校验码> [结果文件] < token.json`（probe1_latency.py 同理，少结果文件参数）。建议后台跑并让结果写文件，防中途打断丢结果。

## 安全纪律

- 令牌不落盘、不进日志、不复用跨任务；跑完即弃（本次的令牌在归档时已作废）。
- 探针默认**只读**：写类接口（审批决定、config/permissions 更新、model.set、fs 写删、memory.consolidate 等）一律不执行；审批决定只用全零假编号探形状（不产生任何决定）；cancel 实验只取消脚本自建的一次性会话。
- credentials.list 只记录条数与字段名，永不输出值。
- 每次连接都会在用户账号里留痕（新建的侧聊会话），属预期副作用，标题带 `native-probe`/`native-latency-test` 前缀便于识别清理。

## 每轮结论摘要（2026-10-07，细节见评估报告 addenda）

- 轮0 延迟：连接 0.97s，原生三道小题回复 3.8–7.3s（微信同题 72–95s）；慢的大头在 hook 唤醒+工人冷启动，不在助手本身。
- 轮0 对照：403=令牌无效（空令牌 401/假令牌 403/网页 GET 200），网络出口无辜；项目正道是进程内现取现连、凭证走文件、永不手抄。
- 轮1 电池：tasks.list/runs、activity.list、config.get（推理强度）、memory.search 全通；history_window 与 subagents.status 被服务端 400 教出必填参数（anchor_seq、agent_id）。
- 轮2 补参：history_window 需 {anchor_seq, session_id}、memory.get 认 memory_uri、sessions.get/chat.search/chat.message_get/permissions.settings/vm.enrollment_health 全通；**cancel 定案**：chat.cancel {session_id} 当场回 {"cancelled": true}，对照组只断连不取消则长文完整写完——取消是真入口、真生效。
- 轮3 横扫：check/model.get（Auto，35 万上下文）/identity/channels.status（WhatsApp 已连接）/connectors/spaces/artifacts/credentials（6 条，仅元信息）/egress.approvals（审批后台可读）全通；shared_agents.* 与 git.log 是死路由（404）；fs 不收绝对路径；subagents.status 是「查自己名下子助手」的内部接口（根编号回 outside 'descendants' scope）。
- 轮3 机制：vm.health/subagents.list 的 DecodeError 源于响应分片——原始帧抓到 200 且内容分片在后续帧，上游客户端不重组分片，残片还会毒到其后约 9 个调用。
- 轮4 补丁：GW2 分片重组后中毒集合（vm.health/subagents.list/egress 全家/voice/node/fs.stats 等）在大响应之后复测**全线一次通过**，机制实锤；subagents.list 可读（名下 13 个子助手）；fs 字段被 422 逐个教会（stats 要 paths、library 要 preset、read 要 offset、search 是 path/name/extensions）；egress 决定接口要 {decision}；activity.subscribe 侧聊触发 25 秒零事件（负结果）。
- 未竟（留待下一轮）：subagents.status 用真子助手编号查一次（battery C 节已含）、fs 按已学字段拿正面结果、chat.stream_events/tasks.subscribe 订阅再试、写类接口逐项验（需用户逐项点头）。
- 轮7（2026-10-08，probe7/7b/7c）：chat.stream_events 定案——一次性快照接口：返回 transcript_surface=all 的全局转录转储（message.user 为主＋delta.presentation/widget，无 message.assistant），单条记录后服务端关流；全局/scoped/回合进行中三轮一致，非实时流，桥会话步骤进度仍无源。脚本 probe7*_stream_events.py、结果 probe7*_results.txt 留本目录。
- 轮9（2026-10-08，probe9）：完成信号定案——sessions.get 的 status 字段是平台回合状态（completed→running→completed，与 history 判完成同刻、早 0.3s）；unread_count/snippet/title 为附带信号。activity 的 task_running 事件 details 内嵌每个子助手的 status 与结果预览，侧会话委派也可见（subagents.list 则看不见侧会话子助手，轮8定案）。
