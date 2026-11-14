# 飞书教程《微信企微畅连Muse教程》镜像

> 来源：https://my.feishu.cn/docx/SsDMdusWboCxOrxJJD7c6Z2xngc（token：SsDMdusWboCxOrxJJD7c6Z2xngc）
> 抓取日期：2026-11-14，飞书 revision 184；正文为当时全文原样镜像，未改写。
> 修正记录：2026-11-14 已按项目现状修正 9 处（revision 158→167）：默认 legacy、版本与自动更新、凭证每日续期、企微 core 媒体路由、审批默认放行、故障转移限定 legacy、/new 回执。2026-11-14 晚补进第一步提示词 5 处（revision 167→173）：桥 token 每日续期、项目定时更新、Core 切换、Gadget 配对、保活核对。2026-11-14 晚再补第一步提示词 4 处（revision 173→177）：审批默认规则、VERSION 以文件为准、Core 切换点名文档、恢复演练加 sudo。2026-11-14 晚 E3 节版本号改为以 VERSION 文件为准（revision 177→178）。2026-11-14 晚再审其余章节改 6 处（revision 178→184）：企微视频按 core 改写（两处）、确认卡片只 legacy、core 暂不支持 /subagent、“思考中”文案、/ping 完整引用、/queue drop 补桥变体。
> 维护规则：以后项目有调整影响到这个文档，也要跟着修改。

---

<title>微信企微畅连Muse教程</title>

# 微信 / 企业微信接入 Muse（默认 Cookie）

仓库地址：[github.com/Sirhap/muse-mobile-channels](https://github.com/Sirhap/muse-mobile-channels)

手机上直接在微信或企业微信里与 Muse 对话，日常使用无需打开 muse.ai。你可以只接个人微信、只接企业微信，也可以两者都接。网关运行在 Muse 虚拟机或已部署的 Linux 主机上，负责接收消息并把回复送回原聊天。

本教程默认使用 Cookie 原生桥。Cookie 来自你在浏览器登录 muse.ai 的会话。两条渠道的生产模式不同：个人微信跑 legacy，企微跑 core（Gadget 主轨、Native 备轨）。两条渠道的消息记录与回复路由分别维护；Muse 平台的侧会话仍可能共享上下文，不把渠道分开理解为平台级信息隔离。

## 第一步，把安装提示词发给 Muse

先用下面的提示词完成默认 Cookie 路径。Muse 负责检查和配置，你需要配合微信扫码、企业微信身份确认及浏览器凭证导出。已有部署先检查当前模式和在途任务，再按明确的变更计划操作。

```
请帮我安装或检查 https://github.com/Sirhap/muse-mobile-channels，让我通过个人微信或企业微信与 Muse 对话。默认使用 Cookie 原生桥；生产模式是个人微信 legacy、企微 core（Gadget 主轨、Native 备轨），先完成 Cookie 路径验收。Gadget 首次配对须 macOS 加蓝牙，本机 Linux 配不了：配对前 core 先走 Native 备轨，Gadget 凭证在有蓝牙的 Mac 上配出后以文件方式交付；装完发一条真实消息走一回合，查 state/core.sqlite3 里 executor_runs 表最新一行的 track 字段并报出实际执行轨是 Gadget 还是 Native，不要只说“已配 Gadget”。

先确认我要接个人微信、企业微信还是两者。只配置选中的渠道。检查现有仓库版本、未提交改动、运行服务、当前模式、任务队列和投递状态；已有生产部署先列出改动范围与恢复方式，得到我的同意后再重启服务、切换模式或修改在途状态。

读取当前 main 的 README.md、docs/deployment-checks.md、所选渠道配置及 docs/bridge-failover-2026-11-01.md。按实际代码操作，保留完整仓库结构，核对家目录、workspace、服务和 hooks 路径。已有 Core 或 Gadget 部署先报告当前执行轨，再讨论是否调整。新装也一样：service-plan 默认装 legacy，企微要跑 core 须另走切换流程——先备好准确的数据库与 state/channel-modes.json，停旧写者，再按 docs/deployment-checks.md 里 Core 切换一段的流程启动（ops/install.sh --with-core 装 muse-core-run.service）；只生成 plan 不等于切了 core，切完核对企微实际跑在 core 模式。

每条依赖命令都从仓库根目录执行，只安装所选渠道：
个人微信：python3 -m venv weixin-bot/.venv && weixin-bot/.venv/bin/pip install -r weixin-bot/requirements.txt
企业微信：python3 -m venv wecom-bot/.venv && wecom-bot/.venv/bin/pip install -r wecom-bot/requirements.txt
同时按 README 安装 ~/muse-test-venv 与固定版本 muse-cli SDK，核对 MUSE_CLI_SRC、桥配置和实际运行用户。使用 doctor 检查本地条件；service-plan 只生成服务配置，执行安装脚本会启动服务。service-plan 生成的单元里没有 self-update.service/timer（只有网关、桥、relay、channel-restore），只生成 plan 不等于装了自动更新；要自动更新须走 README 第 7 步 sudo ops/install.sh，它装 channel-restore.timer 和 self-update.timer。项目版本唯一出处是 VERSION（以 VERSION 文件当前值为准），打版本 tag（形如 v2026.11.14.6）触发 Actions 发 release，ops/self_update.py 每天 03:30 只比版本号、变了才拉包重部署；装完核对 self-update.timer 已启用且 active、VERSION 当前值。

个人微信由我扫描 login.py 生成的二维码；验证码在同一登录会话提交。企业微信使用智能机器人 API 长连接，配置 Bot ID、Secret 和 WECOM_ALLOW_USERS，白名单为空会拒绝入站。凭证使用安全文件或本机配置方式提供，避免聊天明文。

先引导我本人安装 native-bridge/muse-cred-extension/ 浏览器扩展，在同一浏览器登录 muse.ai，并确认目标 VM。导出 muse-credentials.zip，确认包含 cookies.txt、vm_id.txt、token.json，再通过安全文件方式交给你。检查压缩包和目标 VM，备份原凭证后安装到运行桥的 ~/.config/native-bridge/，目录 0700、文件 0600。目标 VM 不一致时先停止并让我确认，不自动修改 vm_id.txt。

默认先试 Cookie，再按实现使用 token.json 回退；检查服务是否设置 NATIVE_TOKEN_JSON 覆盖。用 ~/muse-test-venv/bin/python native-bridge/check_credentials.py --source all --json 分别验证 Cookie、token 与审批连接。文件齐全、未到名义过期时间、服务 active 都不能代替认证和消息验收。

配置选中网关、桥、恢复服务与收件 hooks。按 README 核对 enabled/divert 开关和桥的实际接管状态；通过平台 hooks API 注册，poll_interval_secs 不小于 5。审批默认规则：自 2026-11-06 起除黑名单外一律自动放行，黑名单默认空；只有黑名单命中或自动放行提交失败时才发人工审批通知，人工决定为 /a 仅这次、/b 永久、/c 拒绝。如需手机审批，配置 approval-relay、通知目标与明确的会话和操作者授权；普通消息白名单不自动授予审批权。

完成后报告安装提交、所选渠道、实际执行路径和未完成项。让我在每条渠道先发 /ping，再发普通问题，并测试需要的附件。记录入站 msgid、回复队列 ID 和投递 ok=true。Cookie 失效时重新登录并导出完整 ZIP；提交结果不明时先核查原任务，不能因恢复连接而重复执行。桥 token 要每天自动续期：新建一个每天约 04:54 的定时任务，跑 ~/muse-test-venv/bin/python ~/workspace/native-bridge/renew_token.py（调接口换新 token，服务器轮换的 30 天会话票写回 cookies.txt，写后验活、不过回滚；服务器不是每次都发新票，没发时沿用旧票），跑完用 check_credentials.py 验三路；成功静默、失败汇报。注意 ops/install.sh 不装这个定时任务，须另建；NATIVE_TOKEN_JSON 已设置时 renew_token.py 会以 exit 2 跳过（续磁盘文件无用）。票彻底死了才重导完整 ZIP。
最后核对保活：systemctl 核对 channel-restore.timer 与 self-update.timer 都已启用且 active；再做一次恢复演练（例如用 sudo 删 /etc/systemd/system 下两份 drop-in，看钩子是否在约半分钟内恢复并重启服务），只写“注意保活”不算完成，演练结果写进报告。
```

## 第二步，确认默认连接方式

默认路径是“手机 → 微信／企微网关 → Cookie 原生桥 → Muse → 原聊天”。手机负责发消息，桥使用虚拟机上的凭证连接 Muse。初次安装和凭证失效时，需要本人在浏览器登录并导出凭证。

个人微信是 legacy 部署：启用对应分流后，符合条件的单聊文字走 Cookie 原生桥；图片、文件、语音和群聊沿用收件 hooks。桥在发送前失败且满足安全交接条件时，任务可交给 hooks；已经发送或状态未知的任务先核查。企业微信是 core 部署：Gadget 主轨、Native 备轨，故障转移按 core 的规则走，见“掉线了怎么看”一节。

![](https://feishu.cn/file/SH4HbvUvUo7gt4xuoS0cZDPDnab)

Muse 装的时候会叫你在手机上操作。个人微信和企业微信可以只做一边，也可以两边都做。

## 个人微信：设置里扫码

手机上的操作是：

**设置 → 插件 → 详情 → 连接我的 OpenClaw → 扫一扫**

Muse 会给出刚生成的 login-qr.png。微信页面沿用“OpenClaw”名称，本项目直接实现 iLink 机器人协议。按下面的截图扫码绑定即可；二维码是否过期以当前登录提示为准。

### 1. 打开设置

微信里进入「我」，再进「设置」。插件在「功能」这一组。

![](https://feishu.cn/file/OMgwbwotjoSRSYxrcZicwjAun2g)

### 2. 打开插件

点「插件」。列表里是「微信 ClawBot」，副标题是「连接 OpenClaw 与微信」。点右侧绿色的「详情」。

![](https://feishu.cn/file/MejObK2VOomTstxkfVFcOKfRnrc)

### 3. 连接我的 OpenClaw

详情页标题是「微信 ClawBot」。点「连接新的 OpenClaw」，这就是连接你自己这一路。已经连上的会出现在「我的 ClawBot」里。

截图中的微信页面说明回复存在时间窗口，实际以当前微信界面为准。超时后先核对旧任务是否已执行和已投递，再决定如何恢复回复。

![](https://feishu.cn/file/V4DcbRaMfoiC8kxadrXcTNyPnQh)

### 4. 扫一扫

下一页底部是「开始扫一扫」。点它，扫描虚拟机上的 `login-qr.png`，再在微信里点确认。

![](https://feishu.cn/file/Tt0pb1RLIofw3Sxahz1clz9MnZf)

终端出现 LOGIN CONFIRMED 后，先发 /ping，再发普通问题验收。要求验证码时，在同一登录会话提交；二维码过期，或状态变为 needs_relogin 时再重新扫码。

## 企业微信：通讯录里创建智能机器人

企业微信先找到配置，按这条路径做：

**通讯录 → 智能机器人 → 创建 → 创建智能机器人 → API 模式**

最后复制 Bot ID 和 Secret。网关用的是官方长连接 `wss://openws.work.weixin.qq.com`，协议说明见[企业微信文档 101463](https://developer.work.weixin.qq.com/document/path/101463)。

### 1. 通讯录里打开智能机器人

打开企业微信的通讯录。组织名显示在页面顶部。列表里点「智能机器人」。

![](https://feishu.cn/file/SHAXbLPHkoV5IHxEKRRcG4uynDm)

### 2. 点创建

进入智能机器人列表，右上角点「创建」。

![](https://feishu.cn/file/MLPAbocTFoqVlxx1gy6cU5n2n5X)

### 3. 创建智能机器人

点「创建智能机器人」。已经建好、并且走 API 模式的机器人，会在下面的「已创建」里标出来。

![](https://feishu.cn/file/Y4VFbQOdPoyKJ8xATH0cKA08nwb)

### 4. API 模式，复制 Bot ID 和 Secret

创建页里选「API 模式」。同一页把「使用长连接」保持勾选，不要改成「使用 URL 回调」。长连接不需要填域名或 IP，网关就是靠它收消息、回消息。

复制 Bot ID 和 Secret，通过安全文件或本机配置方式交给 Muse，写入运行用户的 \~/.config/wecom-bot/credentials.env，文件权限设为 0600。

![](https://feishu.cn/file/EtGTbjDoToxP81xEcEqcGfoZnwb)

网关每次重连都会重新读取凭证文件。企微还须配置 WECOM_ALLOW_USERS：允许发送消息的用户 ID，多个用逗号分隔；为空时网关拒绝入站。普通消息白名单不自动授予审批权限。机器人主动推送前，先确认平台要求及对应会话路由。

在企业微信里给机器人单独发 `ping`，网关会直接回 `pong ✅ 网关在线`。这只测通道，不会叫醒 Muse。

企业微信现在跑 core，视频暂不支持：不论大小都发不出去，聊天里会收到一条明确说明。10MB 先压缩的流程只适用于 legacy 模式。

## 版本、发版与自动更新

当前版本以仓库根目录的 `VERSION` 文件为准，唯一出处是该文件，格式为日期加序号（`年.月.日.序号`）；每版改了什么见 `CHANGELOG.md`。

安装用一键脚本：`sudo ops/install.sh`（装 systemd 单元；`--dry-run` 先预览，`--only weixin|wecom` 只装一边，`--with-core` 再装 `muse-core-run.service`）。

发版是维护者操作：改完先记 `CHANGELOG.md`，跑 `python3 ops/bump_version.py` 写新版本号进 `VERSION`，提交并打 `v<版本>` tag，GitHub Actions 会验版本、跑测试、打包并建 release。

部署机上的 `ops/self_update.py` 每天凌晨 3:30 由 `self-update.timer` 跑一次，只比版本号：和最新 release 一样就什么都不做，不一样才下载新包、验包、备份、解包、重启网关服务；它不碰凭证、配置和状态文件。装完后用 `systemctl is-active self-update.timer` 核对更新器是否已启用。手动只比对不改动：`python3 ops/self_update.py --check-only`。

## 第三步，导出 Cookie 原生桥凭证

<figure view-type="Card"><source name="muse-cred-extension-1.0.0.zip" mime="application/zip" size="27062" token="UnmbbGbh5oAImyx5Dqcc7701nlg"/></figure>

推荐使用本节附件中的浏览器扩展，或从仓库下载 native-bridge/muse-cred-extension/ 完整目录。安装说明见 [https://github.com/Sirhap/muse-mobile-channels/tree/main/native-bridge/muse-cred-extension](https://github.com/Sirhap/muse-mobile-channels/tree/main/native-bridge/muse-cred-extension) 。扩展需要读取 muse.ai Cookie，包括 HttpOnly Cookie，请先核对来源与版本。

Chrome 打开 chrome://extensions，开启开发者模式并选择“加载已解压的扩展程序”；Edge 打开 edge://extensions，开启开发人员模式并选择“加载解压缩的扩展”。两者都选择包含 manifest.json 的 muse-cred-extension 文件夹。Firefox 按扩展 README 临时载入，浏览器退出后可能需要重新加载。

在同一浏览器登录 [https://muse.ai](https://muse.ai) 并打开目标 VM，点击 Muse Bridge Credential Exporter。确认 Cookie、VM ID、token 均获取成功，再点击“一键打包下载 ZIP”。完整凭证包包含 cookies.txt、vm_id.txt、token.json；界面提示部分失败时先处理错误。

通过安全文件方式把 ZIP 交给 Muse。Muse 应先检查目标 VM、备份已有凭证，再安装到运行桥的 \~/.config/native-bridge/，目录权限 0700、文件 0600。VM 不一致时先核对，避免覆盖到另一个环境。检查 NATIVE_TOKEN_JSON 等环境配置是否仍覆盖新文件。

验活命令为 \~/muse-test-venv/bin/python native-bridge/check_credentials.py --source all --json。分别确认 Cookie、token 与审批连接，再发真实普通消息验收。Cookie 和 token 可能提前失效，不按导出日期承诺可用一个月。

凭证每天自动续一次：定时任务 `bridge-token-daily-renew` 每天约凌晨 4:54 跑 `native-bridge/renew_token.py`，调接口换新 token；服务器轮换会话票（30 天）时把新票写回 `cookies.txt`，写完验活，验不过就从备份恢复。服务器不是每次调用都发新票，没发时沿用旧票。成功不打扰，失败会汇报；票彻底死了、续期多日失败，才需要重新导出完整 ZIP。

下面是仅导出 token.json 的备用方式，适用于临时恢复桥的 token 回退。它无法读取 HttpOnly Cookie，不能代替完整 Cookie 恢复，也不能修复只使用 Cookie 的审批中继。

1. 用浏览器打开已经登录的 https://muse.ai 。
2. 按 `F12`，切到 Console（控制台）。
3. 浏览器提示粘贴保护时，先完整阅读并确认脚本来源，再按浏览器当前提示处理。
4. 把下面的脚本放在已登录 muse.ai 的页面控制台执行。脚本会打印并尝试复制 token JSON，控制台输出包含登录凭证，避免截图、录屏或转贴。

```JavaScript
(async () => {
  const H = { 'Content-Type': 'application/json' };
  const j = async (r) => {
    if (!r.ok) throw new Error(r.status + ' ' + (await r.text()).slice(0, 200));
    return r.json();
  };
  try {
    const auth = await j(await fetch('/api/auth/check', {
      method: 'POST', headers: H, body: '', credentials: 'include'
    }));
    const access_token = auth.access_token;
    if (!access_token) throw new Error('auth/check 没返回 access_token，可能未登录');
    const sess = await j(await fetch('/api/session', { credentials: 'include' }));
    const vm_id = sess.vm_id;
    if (!vm_id) throw new Error('session 没返回 vm_id（VM 可能在重启）');
    const ht = await j(await fetch('/api/hatch/token', {
      method: 'POST',
      headers: { ...H, 'Authorization': 'Bearer ' + access_token },
      body: JSON.stringify({ vmAddress: `wss://${vm_id}.metaaivm.com/`, vmName: vm_id }),
      credentials: 'include'
    }));
    const hatch_token = ht.token;
    if (!hatch_token) throw new Error('hatch/token 没返回 token');
    const seg = hatch_token.split('.')[1].replace(/-/g, '+').replace(/_/g, '/');
    const p = JSON.parse(atob(seg + '='.repeat((4 - seg.length % 4) % 4)));
    const out = {
      vm_id,
      access_token,
      hatch_token,
      issued_at: p.iat ? new Date(p.iat * 1000).toISOString() : null,
      issued_at_local: p.iat ? new Date(p.iat * 1000).toLocaleString() : null,
      exp: p.exp ?? null
    };
    const text = JSON.stringify(out, null, 2);
    console.log(text);
    try { copy(text); console.log('✅ 已复制到剪贴板'); } catch (e) {}
    return out;
  } catch (e) {
    console.error('❌ 出错了：', e.message);
  }
})();
```

1. 把导出的 JSON 保存为 token.json，以安全文件方式交给 Muse。避免把其中内容复制到聊天、日志或仓库。

把 token.json 文件交给 Muse 时，可附上下面的说明：

```
我已本人完成备用 token 导出，token.json 作为文件附上。请先检查当前部署和运行用户，再执行：
1. 备份原 ~/.config/native-bridge/token.json，检查新文件的预期字段与导出时间。
2. 核对 token 中的 vm_id、原 vm_id.txt 和实际目标 VM。任意一项不一致时停止安装并让我确认，不自动改 vm_id.txt。
3. 将确认过的 token.json 安装为 0600，检查 NATIVE_TOKEN_JSON 是否覆盖磁盘文件。
4. 使用 check_credentials.py --source token --json 验活，再测试真实普通消息。明确报告 Cookie 与审批是否仍未恢复；如需完整恢复，引导我重新导出包含 Cookie 的 ZIP。
5. 只报告脱敏状态和投递证据，不回贴 token 内容。涉及生产服务重启或切流时，先给出具体计划并获得我的同意。
```

## 日常怎么聊

绑定之后，把对应的聊天当成普通对话用。

直接发送文字即可。不同渠道和队列状态可能出现思考、排队或进度提示；这些提示不代表任务已完成。最终以正式回复和实际投递结果为准。

长任务可能收到“思考中”、排队或进度提示，具体频率取决于运行模式和配置。以正式结果及投递记录判断是否完成；需要停止时发送 /stop，并查看执行方确认。

可以发图片、文件、语音，也可以引用一条旧消息再问。引用会变成「（引用：…）」交给 Muse。这类消息，以及群聊，走冷车道，比纯文字私聊慢一点。

网关下载入站附件的本地保护上限为 1 GiB，平台自身的类型、大小和回复窗口限制仍需满足。个人微信出站的受支持图片、视频超过 2,000,000 字节时会尝试既有压缩流程。企业微信走 core 模式：图片 2 MiB 以内先下载再交给 core 执行器（Gadget 主轨支持图片输入，Native 备轨也能发图片）；视频、文件、语音暂不支持执行，会明确告诉你发不出去，而不是默默丢掉。

同时接入企微并配置好对应目标时，legacy 微信网关可在特定文件投递失败场景尝试转投企微，并报告结果。它依赖实际路由和开关，单独安装微信时不能依赖这条路径。

项目分别维护两条渠道的历史和回复路由。换话题时可发送：

```
/new
```

发送 /new 后回执就是“已开启新会话。”，后续消息使用新会话上下文；legacy 原生桥在下一条走桥的文字到来时创建新会话。默认达到 80 个回合或会话超过 7 天时也会轮换。在途旧任务保留原回复路由；Muse 平台侧会话仍可能共享上下文。

想让一件事在后台做、不挡住当前聊天，用副助手：

```
/subagent 把这周的会议纪要整理成三条结论
```

默认 legacy 流程支持副助手命令，用 /subagent list 查看状态。Core 命令迁移范围以当前 /help 为准，core 暂不支持 /subagent，发了会回“该命令尚未迁移”。手机审批走中转服务（approval-relay），需要配置通知目标和操作者授权；但自 2026-11-06 起，除黑名单外一律自动放行，黑名单默认是空的，只有黑名单命中或自动放行提交失败时才会发人工审批通知（/a 仅这次、/b 永久、/c 拒绝）。提交结果未知时先核查原审批。

## 聊天里的命令

本节命令以默认 legacy 部署为主，网关识别后直接处理。支持全角斜杠和大小写规范化。Core 的命令支持范围及未知命令处理与 legacy 有差异，遇到问题先发 /help 核对当前版本。

| 命令 | 别名 | 作用 |
|-|-|-|
| `/ping` |  | 回 pong，并带上网关状态和时间 |
| `/status` |  | 连接、最近进出、正在处理和排队的条数 |
| `/queue` |  | 逐条列出正在处理和排队的内容 |
| `/queue clear` |  | 清掉还没开工的排队消息 |
| `/queue drop N` |  | 只删排队里的第 N 条；`桥N` / `bN` 删桥排队第 N 条 |
| `/stop` |  | 请求取消正在执行的任务，等待执行方确认 |
| `/subagent 任务` | `/副助手` | 后台并行做一件事 |
| `/subagent list` |  | 看副助手进度 |
| `/subagent stop 编号` |  | 停掉一个副助手 |
| `/new` |  | 为后续消息切换新会话；在途任务仍按原路由回传 |
| `/check` | `/自检` | 检查当前部署状态，结果仍需结合真实消息验收 |
| `/help` | `/命令` | 查看命令表 |

`/ping` 的典型回复是：`pong ✅ 个人微信网关在线（状态 connected，无错误）· 时间 · 网关直接秒回，未唤醒任务`，其中时间是回复那一刻。

`/queue` 为空时回「队列空闲：没有正在处理或排队的任务。」删某一条之前先发一次 `/queue`，序号按你发命令这一刻的顺序算。`/queue clear` 和 `/queue drop` 只动还没开工的消息；正在跑的用 `/stop`。

`/jump` 和 `/插队` 已经取消。再发这两个词，会按普通消息处理。

## 掉线了怎么看

安装后先在每个已启用渠道发送 /ping，再发一条普通问题，分别确认网关连通和正式回复。完整实测用例见：https://github.com/Sirhap/muse-mobile-channels/blob/main/docs/muse-live-test-cases-2026-11-01.md 。故障注入应在隔离环境执行，涉及生产服务的操作须先取得同意。

个人微信需要重新登录时，让 Muse 生成新二维码，再按上面的截图路径扫码。新旧任务与投递状态应分别核查。

桥登录失效时，优先按“导出原生桥凭证”一节使用浏览器扩展；无法使用扩展时再考虑控制台导出 token 的备用方法。不要把 Cookie、token 或 Bot Secret 贴进聊天。

如果 /ping 通但普通消息没有正式回复，或看到桥断线、自动重连暂停、提交结果待核查，可以把下面这段话发给 Muse：  
  
“请先只读检查当前仓库版本和运行模式、选中渠道的网关/桥/hook 服务、桥心跳、降级标记、任务队列与投递结果。请区分已入队、平台已接收、实际显示和结果不明，汇报发现及建议。不要输出凭证、用户 ID 或聊天正文；不要重发可能已经执行的任务，不要删除状态、清除 fallback 标记或重启服务。任何可能影响现有任务的操作先给出计划并征得我同意。”  
  
桥切到备用路径后不会自动切回——这只适用于 legacy 桥。确认所有备用任务及投递已结案、没有未知提交或积压后，再按仓库 docs/bridge-failover-2026-11-01.md 制定恢复操作。Core 模式没有这条手动切回流程：Gadget 发送遇到 404（服务端会话已死）时，同回合删死会话、用新会话重试一次，再失败才走 Native 备轨，每回合最多重试一次；其他原因的结果未知不降级，避免重发。结果不明时先核查原任务，不能仅因为没看到回复就重发。

## 接一个还是两个

只接个人微信、只接企业微信，或两个都接，都可以。没接的那一边不用装服务，也不用准备凭证。以后想补上另一边，按对应章节再做一遍即可。

个人微信适合随手问一句、收一段文字或一张图。扫码绑定一次之后，日常只在那个 ClawBot 会话里说话。

企业微信适合工作会话。机器人可以回文字；审批通知也是文字，收到后回 /a /b /c 决定，确认卡片只在 legacy 模式下发送。视频暂不支持，不用压在 10MB 以内。主动推送之前，这个会话里要先有你发出去的一条消息。

两条渠道分别保存消息历史和回复路由。跨 App 接着处理同一件事时，把必要背景再说明；/new 可为后续消息切换会话，平台级上下文隔离仍取决于 Muse。
