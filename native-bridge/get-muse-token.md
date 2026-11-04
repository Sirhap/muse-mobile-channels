# Muse 桥凭证恢复与 token 导出

桥使用 `cookies.txt` + `vm_id.txt` 作为主路径，在 Cookie 会话仍有效时换取连接 token；`token.json` 是回退路径。审批中继只使用 Cookie。设置了 `NATIVE_TOKEN_JSON` 时，桥优先使用环境 token，跳过磁盘 Cookie 和 token 文件。

Cookie 和 token 都可能被服务端提前作废。文件里的 `exp` 或 Cookie 名义过期时间只能作为参考，不能保证还能使用一天、一周或一个月；缺少平台有效期时标记为未知，不按导出日期推算。

## 推荐恢复方式：浏览器扩展导出全套凭证

1. 用户本人按 [扩展安装说明](muse-cred-extension/README.md) 安装扩展；如果拿到的是扩展源码 ZIP，先解压，再加载其中含 `manifest.json` 的目录。扩展安装包与后续导出的凭证 ZIP 是两份不同文件。
2. 在自己的浏览器登录 <https://muse.ai>，确认目标 VM 的会话可正常打开，再点击扩展。
3. 确认 Cookie、VM ID 和 token 均读取成功，下载 `muse-credentials.zip`。如有部分读取失败，先解决错误，不把不完整压缩包当成恢复成功。
4. 通过环境支持的安全文件投递方式将压缩包交给 Muse，不把内容贴进聊天、日志或仓库。
5. Muse 先把压缩包解到临时目录，确认只有预期的 `cookies.txt`、`vm_id.txt`、`token.json`，核对两个 VM ID 与预期一致、导出时间是本次。VM 不一致时停止安装并向用户确认，不自动切到另一个 VM。
6. 在仓库外备份现有凭证，备份目录设为 `0700`；将三份文件安装到 `~/.config/native-bridge/`，目录 `0700`、文件 `0600`。部署配置了 `NATIVE_BRIDGE_CREDENTIALS` 时，以该目录为准。需要修改服务环境中的 `NATIVE_TOKEN_JSON` 时先检查其是否仍覆盖新文件，不输出其值。
7. 执行下面的只读验活，再完成真实消息验收。凭证文件变化会允许连接重新尝试，但不代表认证已通过，也不自动解除备用通道接管或处理未知提交；恢复切流按 [桥故障恢复说明](../docs/bridge-failover-2026-11-01.md) 操作。

```bash
~/muse-test-venv/bin/python ~/workspace/native-bridge/check_credentials.py --source all --json
```

验活会分别检查桥 Cookie 的 `sessions.list`、桥 token 的 `sessions.list` 和审批 Cookie 的 `egress.approvals`。token 检查遵循 `NATIVE_TOKEN_JSON` 优先，否则读 `token.json`；环境覆盖存在时，不代表磁盘 token 已验证。诊断仅输出状态、来源和检查时间等脱敏字段，不输出凭证或审批正文；缺 SDK、缺文件或认证失败都需要处理，不能用另一项通过来掩盖。

**认证通过之后**，让用户在每个启用的渠道发一条普通问题，记录入站 msgid、正式回复队列 ID 及对应投递 `ok=true`；这表示网关确认平台接收，不表示用户已读。再确认审批查询成功、失败计数已归零。完整步骤见 [Muse 凭证健康实测清单](../docs/bridge-credential-health-muse-cases.md)。

如果以后提示桥凭证失效，让用户重新登录 Muse，用扩展导出新的 `muse-credentials.zip`，再次以文件交给 Muse，重复备份、核对、安装和两阶段验收；不要只等到名义过期日再处理。

## 备用方式：控制台只导出 token

脚本 [`get-muse-token.js`](get-muse-token.js) 是浏览器控制台片段，不能用 Node 执行。它通过浏览器现有登录态请求 token，无法读取 `HttpOnly` Cookie，不能生成 `cookies.txt`。单换 token 可能恢复桥的回退路径，但不能恢复审批中继，也不能证明 Cookie 恢复。

用户本人在已登录 muse.ai 页面的开发者工具 Console 中运行整份脚本；如浏览器提示粘贴保护，应先确认自己理解并信任脚本。成功后把 JSON 存为 `token.json` 并以文件交给 Muse，不发送聊天明文。Muse 按上述流程备份、核对预期 VM、安装为 `0600`；VM 不一致时停止，不自动覆盖 `vm_id.txt`。仅检查这一来源可执行：

```bash
~/muse-test-venv/bin/python ~/workspace/native-bridge/check_credentials.py --source token --json
```

| 字段 | 说明 |
| --- | --- |
| `vm_id` | 目标 VM，必须与预期一致 |
| `access_token`、`hatch_token` | 实际连接凭证，不应显示在诊断或聊天中 |
| `issued_at`、`issued_at_local` | 签发时间，用于核对新旧，不是到期时间 |
| `exp` | 平台提供的名义到期时间；缺失为 `null`，实际也可能提前失效 |

## 出错与安全

- `auth/check` 没返回 token：确认页面已登录、目标会话可用后重新导出。
- `/api/session` 没返回 VM ID：等待目标 VM 就绪；不要猜测 ID。
- 控制台 `Failed to fetch`：确认执行上下文是 muse.ai 页面。
- token 验证通过但 Cookie 或审批仍失败：重新用扩展导出全套凭证，检查实际凭证目录及环境覆盖；不要重复换 token 来冒充整套恢复。
- 网络失败与认证失败分别排查；进程 active、心跳新鲜、网关 connected 和文件非空都不能单独证明凭证有效。

导出文件等同登录凭证，只在用户和目标 Muse 环境之间安全交接，保存在仓库外的私有目录。脚本及扩展源码可以分享，实际凭证压缩包不能作为安装包分享或提交。
