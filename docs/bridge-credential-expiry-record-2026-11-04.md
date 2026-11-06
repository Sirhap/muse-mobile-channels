# 桥凭证断线问题记录（2026-11-04）

记录人：Muse（微信事件 hook，msgid 7523705408330965640）
记录目的：把 2026-11-04 当天断线的前后经过、原因、已恢复项和待修复项一次记清，后面按清单修，不再靠口头复述。
注意：本文只记字段名、文件规模、时间戳和服务端返回，不贴任何凭证值。时间线与线上结果来自当日 Muse 记录，本地代码核对不等于独立复核生产日志；以下修正区分历史观察、代码行为与待补证据。

## 一、事件总览

- 断线本质：不是微信网关断，是桥侧凭证被服务端作废。
- 两路凭证要分清：
  - cookies.txt：桥自动续签的源头；审批通知（approval-relay）只认这一路。
  - token.json：桥的静态回退路，连接必需字段为 vm_id、access_token、hatch_token；没有证实固定有效期，可能提前失效。
- 2026-11-04 13:40 已恢复认证（新 cookies.txt＋新 token.json 一并装回，verify 回 CONNECT_OK），但当时仍缺一次真实入站回合的 live 实证，属“认证恢复、未算收发实证”。

## 二、时间线

- 2026-10-23：装回 cookies.txt（旧份，7488 字节、13 条），此后桥靠它自动续签，一直撑到 11 月 1 日前，期间未再向用户要凭证。
- 2026-11-01 17:25：只装回了新 token.json（用户以文件形式投递），当时验活通过；cookies.txt 当天没换，仍是 10-23 份。
- 2026-11-04 12:18／12:28／12:38：桥 idle 扫描三轮同错；企微 idle_scan_fail_streak 累到 57，12:38 出现 reconnect_exhausted。
- 2026-11-04 13:37 复核：token.json 自 11-01 17:25 后未变、未自续；verify 由 CONNECT_OK 变 CONNECT_FAIL（cookies 先 401，随后 WebSocket 升级 403，与 10-15 过期时的表现一致）。
- 2026-11-04 13:40：用户以文件形式投递 muse-credentials.zip（cookies.txt＋vm_id.txt＋token.json）；本侧程序化装回，旧凭证备份于 ~/.config/native-bridge/backup-20261104-credentials/（0700），新文件均为 0600，vm_id 与旧的一致，token issued_at 为当日 13:30 CST。
- 2026-11-04 13:40 后：桥＋双网关＋approval-relay 同批重启，四服务 active、双网关 connected；企微 idle_scan_fail_streak 由 57 清到 0；relay 重启后约 47 秒内未再报 401（此前每 20 秒一报）。
- 2026-11-04 13:40 后残留：微信 idle_fail 仍为 1；重启后尚无真实入站回合，故只算认证恢复。
- 2026-11-04 当日 17:25 前后：用户在微信上追问断线原因，往来核心是“昨天不是刚给过 OK 的吗”“是不是删了没保存”“GS 脚本能不能导 cookie”。
- 2026-11-04 当日 17:25 之后（同日对话）：用户又问到 15:52 查桥接代码长任务中断的原因，见第五节。
- 2026-11-04 19:19 左右：用户指令“记录一下问题的前后，原因都记录一下，后面去修复”——即本文。

## 三、记录中的判断与核对修正

1. 只换了 token，没换 cookie。
   - 11-01 换的是 token.json 一路。11-01 17:25 至 11-04 17:25 约为三天，不能据此称为“一天正好满期”；缺少中间验活记录，实际失效时刻与有效期未知。
   - cookies.txt 仍是 10-23 份，11-01 没动过；审批中继当天仍报 auth/check 401。
2. 是服务端提前作废，不是本地时间戳到期，也不是文件被删。
   - 旧 cookies.txt 还在、非空；服务端回 auth/check 401，hatchAdmissionInvalidated，reason 为 session。
   - 旧份 hatch_sess 名义过期是 11-21，没到日子仍被作废；名义过期只能当上限，不能当寿命保证。
   - 前一份（10-23 装回、名义过期 11-21）的历史观察为：10-27 还确认近 2 小时无 401 且消息经桥送达，11-01 17:25 首次证实 cookies 401；10-30／31 无验活记录，精确死亡时刻查不到，至多 9 天 17 小时，比名义提前至少约 19 天。
   - 新份（11-04 装回，共 9 个 cookie）hatch_sess 名义过期为 2026-12-04 13:29:55 CST，同样只作名义上限参考；不能从旧份的一次观察推定新份能用一周或一个月，应以实际验证与失效提醒为准。
3. GS 脚本导不出 cookies.txt，这是结构性原因，不是脚本少写了字段名。
   - 脚本即 native-bridge/get-muse-token.js（2026-10-14 用户给原文入仓，10-20 只改过注释）。
   - 全文没有 document.cookie 这一行；它是用 fetch＋credentials:include 让浏览器自动带上 HttpOnly 的 cookie 去换 token，脚本本身看不见 cookie 值。
   - 输出包含 vm_id、access_token、hatch_token，以及 issued_at、issued_at_local、exp 等辅助字段；走的是 token 回退路，不会导出 cookies.txt，不据此推定有效期。
   - 控制台 document.cookie 按浏览器规范只能返回非 HttpOnly 的 cookie；旧份 13 条里 12 条是 HttpOnly（含 hatch_sess、hatch_gw、hatch_native_auth_device、datr），只有 _fbp 不是，所以控制台这条路天生拿不到会话本身，脚本写得再全也补不齐。
4. 最初“能跑很久”不是靠反复跑 GS 脚本。
   - 当初是用户在已登录 muse.ai 的浏览器里整份导出 cookies.txt 装回；桥每次连接先用这份 cookie 调 POST /api/auth/check 换 access_token，再带 Cookie 和 access_token 调 POST /api/hatch/token 换 hatch_token；前提是 Cookie 会话仍被服务端认可。这里的续签连接 token 不等于延长浏览器会话有效期。
   - Cookie 会话有效是持续换取连接 token 的前提；10-23 那份最后一次有明确验活证据为 10-27，11-01 已证实 401，期间何时失效未知。
   - 代码可证实脚本只导出 token、桥存在 token 回退；原先具体哪条凭证路径承担流量，应由当时的连接日志确认，不能单凭用户回忆或“能用”反推。
5. 续期机制本身没有“拿旧值续旧会话”的接口。
   - 审批中继 connect() 只读 cookies.txt 整份加 vm_id，拼成 Cookie 串发过去，本地不校验过期时间；代码里没有续旧值的接口。
   - 现有代码通过外部 Muse SDK 的 load_cookies 读取 Cookie，并和 vm_id 一起交给客户端。应使用扩展导出的完整当前会话；服务端必须哪些 Cookie、如何判定会话一致性，不能仅凭本仓库代码确认，也不手工删减猜测。
   - 完整恢复流程为：用户安装浏览器扩展、登录 Muse、导出包含 cookies.txt、vm_id.txt、token.json 的新 muse-credentials.zip，以安全文件方式交给 Muse；Muse 备份旧凭证、核对目标 VM、安装后分来源验活并补真实消息验收。单换 token 只能尝试恢复桥回退，审批中继仍需要有效 Cookie。值不贴聊天、不手工转录；文件投递是既定交接方式。

## 四、已确认的事实 vs 仍未证实

- 已确认：微信网关全程是通的，用户消息能收到；断的是桥凭证两路；11-04 13:40 装回后 verify CONNECT_OK、relay 401 停报、企微 streak 清零。
- 未证实（写进待修复，不算完成）：13:40 重启后真实入站回合的 live 收发实证；微信 idle_fail 残留 1 的归零；新 cookies 实际寿命（名义 12-04，仅作上限）。

## 五、同日另一问题：15:52 查桥接代码长任务中断

- 当日 Muse 记录的经过（待用线上日志独立核对）：15:52 那条查桥接代码的任务，工人只登记了一次，之后没发过进度；内部心跳两次到 15 分钟上限就停（16:07、16:39）；到 16:55 静默超限，系统把它记为取消；全程没留正式回复。
- 仓库提示词没有给普通任务设置总时长上限，要求约 2 分钟真实进度、长工具前 hold、约 13 分钟续期心跳和基础设施任务移交主会话。原任务为何被取消，仍需关联实际部署的 hook 定义、run_id、worker_activity、平台终态、取消记录与投递结果，不能仅由时间线确定。
- 当日记录提到普通静默窗口 1800 秒、有死亡证据时 300 秒、心跳循环约 15 分钟自停。这些须与线上当前定义核对；仓库存活探针还会检查真实 worker/桥活动、hold、交接保护和绑定 run_id 的平台记录，不能把心跳文件新鲜单独作为存活证据。
- 结论：单调大超时修不了，只会让僵尸挂更久、发现更晚。

## 六、历史待修复清单与本次验收入口

1. 凭证健康预警：不能只看名义过期时间；要在 idle_scan 失败 streak 刚起、relay 刚开始报 401 时就告警，而不是等到 streak 57、用户发现断线才处理；修复目标为首次明确认证失败即通知，同一服务同一故障通知一次，实际 RPC 恢复后再通知一次；沿用现有渠道通知路径。
2. 交接清单定稿：cookies.txt vs token.json 两路分开写清——各管哪一路、有效期未知或名义上限、失效表现、完整恢复时一并给；杜绝“给了 token 就等于给了 cookie”的误解。
3. GS 脚本定位纠正：get-muse-token.js 和 get-muse-token.md 明确标注“只换 token，导不出 cookie”；使用现有扩展一步导出三文件的 ZIP，按 [桥凭证恢复说明](../native-bridge/get-muse-token.md) 操作。
4. 恢复后验活标准：装回后除 verify CONNECT_OK 外，必须补一次真实入站回合 ok=true，才算收发实证；微信 idle_fail 残留要清零确认。
5. 长任务防中断：工人每约 2 分钟推一次进度、长静默前先声明 hold；改代码的长任务走主会话交接流程，不由工人长跑；不采用“调大默认超时”的修法。
6. 历史教训复用：进程活着、status.json 心跳新鲜、双网关 connected 均不等于桥已认证通过；报认证恢复前运行仓库内的 check_credentials.py 分来源验证；收发恢复还需真实消息投递结果。

本次本地修复和线上实测分开验收；所有待实测项按 [Muse 凭证健康实测清单](bridge-credential-health-muse-cases.md) 记录，不因代码测试通过就改写历史线上结果。

## 七、证据索引（不含凭证值）

- 旧凭证备份：~/.config/native-bridge/backup-20261104-credentials/；更早备份见 backup-20261014、backup-20261022-cookies/token、backup-20261101-token。
- 当日记录的文件（当前状态须重新核对）：~/.config/native-bridge/cookies.txt（9 条，2026-11-04 13:40 装回）、token.json（issued_at 2026-11-04T05:30:03Z）。
- 脚本：~/workspace/native-bridge/get-muse-token.js、get-muse-token.md。
- 既往结论来源：~/MEMORY.md 中桥 token 与 cookies 现行状态条目；本文件为 11-04 当日对话的正式记录版。
