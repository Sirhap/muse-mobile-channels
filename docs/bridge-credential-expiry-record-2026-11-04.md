# 桥凭证断线问题记录（2026-11-04）

记录人：Muse（微信事件 hook，msgid 7523705408330965640）
记录目的：把 2026-11-04 当天断线的前后经过、原因、已恢复项和待修复项一次记清，后面按清单修，不再靠口头复述。
注意：本文只记字段名、文件规模、时间戳和服务端返回，不贴任何凭证值。

## 一、事件总览

- 断线本质：不是微信网关断，是桥侧凭证被服务端作废。
- 两路凭证要分清：
  - cookies.txt：桥自动续签的源头；审批通知（approval-relay）只认这一路。
  - token.json：桥的静态回退路，只有 vm_id、access_token、hatch_token，寿命约一天。
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

## 三、根因（已确认）

1. 只换了 token，没换 cookie。
   - 11-01 换的是 token.json 一路，寿命约一天，到 11-04 17:25 前后正好满期。
   - cookies.txt 仍是 10-23 份，11-01 没动过；审批中继当天仍报 auth/check 401。
2. 是服务端提前作废，不是本地时间戳到期，也不是文件被删。
   - 旧 cookies.txt 还在、非空；服务端回 auth/check 401，hatchAdmissionInvalidated，reason 为 session。
   - 旧份 hatch_sess 名义过期是 11-21，没到日子仍被作废；名义过期只能当上限，不能当寿命保证。
   - 前一份（10-23 装回、名义过期 11-21）实际寿命约一周：10-27 还确认近 2 小时无 401 且消息经桥送达，11-01 17:25 首次证实 cookies 401；10-30／31 无验活记录，精确死亡时刻查不到，至多 9 天 17 小时，比名义提前至少约 19 天。
   - 新份（11-04 装回，共 9 个 cookie）hatch_sess 名义过期为 2026-12-04 13:29:55 CST，同样只当上限看，须按约一周的实际寿命预期去盯。
3. GS 脚本导不出 cookies.txt，这是结构性原因，不是脚本少写了字段名。
   - 脚本即 native-bridge/get-muse-token.js（2026-10-14 用户给原文入仓，10-20 只改过注释）。
   - 全文没有 document.cookie 这一行；它是用 fetch＋credentials:include 让浏览器自动带上 HttpOnly 的 cookie 去换 token，脚本本身看不见 cookie 值。
   - 输出只有 vm_id、access_token、hatch_token 三项，走的是桥约一天的 token 回退路。
   - 控制台 document.cookie 按浏览器规范只能返回非 HttpOnly 的 cookie；旧份 13 条里 12 条是 HttpOnly（含 hatch_sess、hatch_gw、hatch_native_auth_device、datr），只有 _fbp 不是，所以控制台这条路天生拿不到会话本身，脚本写得再全也补不齐。
4. 最初“能跑很久”不是靠反复跑 GS 脚本。
   - 当初是用户在已登录 muse.ai 的浏览器里整份导出 cookies.txt 装回；桥每次连接先用这份 cookie 调 POST /api/auth/check 换 access_token，再带 Cookie 和 access_token 调 POST /api/hatch/token 换 hatch_token，token 约一天到期也不怕，桥自己重签。
   - 只要 cookie 会话还被服务端认，就能一直跑；10-23 那份就是这样撑到 11-01 前。
   - 用户“最初就是用这个脚本导出来的、也能用”的记忆只对了一半：脚本恢复的是 token 路，不是 cookies.txt；当时感觉能用，是因为桥有 token 回退路顶着。
5. 续期机制本身没有“拿旧值续旧会话”的接口。
   - 审批中继 connect() 只读 cookies.txt 整份加 vm_id，拼成 Cookie 串发过去，本地不校验过期时间；代码里没有续旧值的接口。
   - 桥续期是整份拼串换新：load_cookies 把 cookies.txt 全拼成 name=value; … 的 Cookie 头，连同 vm_id 一起用；核心身份是 hatch_sess，但单给它不够，hatch_gw、hatch_native_auth_device、datr 等要整份配套，否则会被判会话不一致。
   - 所以恢复只能是：用户从已登录浏览器重新导出整份新鲜 cookies.txt，连同新鲜 token.json 一起以附件发来，本侧程序化装回、验活。值不贴聊天、不手工转录；文件投递是既定交接方式。

## 四、已确认的事实 vs 仍未证实

- 已确认：微信网关全程是通的，用户消息能收到；断的是桥凭证两路；11-04 13:40 装回后 verify CONNECT_OK、relay 401 停报、企微 streak 清零。
- 未证实（写进待修复，不算完成）：13:40 重启后真实入站回合的 live 收发实证；微信 idle_fail 残留 1 的归零；新 cookies 实际寿命（名义 12-04，仅作上限）。

## 五、同日另一问题：15:52 查桥接代码长任务中断

- 经过：15:52 那条查桥接代码的任务，工人只登记了一次，之后没发过进度；内部心跳两次到 15 分钟上限就停（16:07、16:39）；到 16:55 静默超限，系统把它记为取消；全程没留正式回复。
- 不是微信断了，也不是用户取消的；也不是任务时长到顶——现在本来就没有任务时长上限，2026-10-07 定下的“没有上限”仍在。
- 判死看的是工人自己推送静默：正常 1800 秒无推送才路由，有死亡证据时 300 秒就路由；内部心跳 15 分钟自停后也不再算活。
- 结论：单调大超时修不了，只会让僵尸挂更久、发现更晚。

## 六、待修复清单（后面按此修，本文只记录、不动手）

1. 凭证健康预警：不能只看名义过期时间；要在 idle_scan 失败 streak 刚起、relay 刚开始报 401 时就告警，而不是等到 streak 57、用户发现断线才处理；预警阈值和通知路待定。
2. 交接清单定稿：cookies.txt vs token.json 两路分开写清——各管哪一路、寿命、失效表现、恢复时必须一并给；杜绝“给了 token 就等于给了 cookie”的误解。
3. GS 脚本定位纠正：get-muse-token.js 和 get-muse-token.md 明确标注“只换 token，导不出 cookie”；另写一份浏览器扩展整份导出 cookies.txt 的步骤（已登录会话确认→扩展导出当前站点→文本格式存为 cookies.txt→附件发来）。
4. 恢复后验活标准：装回后除 verify CONNECT_OK 外，必须补一次真实入站回合 ok=true，才算收发实证；微信 idle_fail 残留要清零确认。
5. 长任务防中断：工人每约 2 分钟推一次进度、长静默前先声明 hold；改代码的长任务走主会话交接流程，不由工人长跑；不采用“调大默认超时”的修法。
6. 历史教训复用：进程活着、status.json 心跳新鲜、双网关 connected 均不等于桥已认证通过；报桥状态前必须先跑 verify。

## 七、证据索引（不含凭证值）

- 旧凭证备份：~/.config/native-bridge/backup-20261104-credentials/；更早备份见 backup-20261014、backup-20261022-cookies/token、backup-20261101-token。
- 现行文件：~/.config/native-bridge/cookies.txt（9 条，2026-11-04 13:40 装回）、token.json（issued_at 2026-11-04T05:30:03Z）。
- 脚本：~/workspace/native-bridge/get-muse-token.js、get-muse-token.md。
- 既往结论来源：~/MEMORY.md 中桥 token 与 cookies 现行状态条目；本文件为 11-04 当日对话的正式记录版。
