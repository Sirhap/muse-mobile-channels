# 导出 Muse token（桥凭证恢复）

脚本：[`get-muse-token.js`](./get-muse-token.js)（同目录）。这是一段跑在
浏览器控制台里的代码片段，不是 Node 脚本，不要用 `node` 执行。

## 什么时候需要它

桥（`native_bridge.py`）连 Muse 平台用两套凭证，优先级在 `connect()` 里：

1. 环境变量 `NATIVE_TOKEN_JSON`（临时注入时）；
2. `~/.config/native-bridge/cookies.txt` + `vm_id.txt`（主力，可自动续期）；
3. `~/.config/native-bridge/token.json`（回退，字段就是本脚本的输出）。

当 cookies 被服务端失效——桥日志出现 `auth/check -> 401
hatchAdmissionInvalidated`、旧 token WSS 403、各渠道
`idle_scan_fail_streak` 涨上去——而你手边有已登录的 muse.ai 浏览器时，
用本脚本导出一份新 token JSON 顶上。2026-10-14 就这样恢复过一次。

只想恢复 cookies 主力路径是另一件事：从已登录的 muse.ai 重新导出
cookies 存回 `cookies.txt`；本脚本只解决 token.json 这一路。

## 使用步骤

分工先说清：这段脚本**只能由用户本人在自己已登录的浏览器里执行**，
靠的是用户自己的登录态，Muse 没有这个登录态、无法代跑；用户导出
后把结果以文件形式交给 Muse，安装和验证由 Muse 在本机完成。

### 第一段：用户本人执行（在自己的浏览器里）

1. 浏览器打开 <https://muse.ai>，确认是已登录状态（能看到自己的会话）。
2. 按 `F12` 打开开发者工具，切到 **Console（控制台）**页签。
   - 浏览器若提示粘贴保护，按提示手动输入 `allow pasting` 后再粘贴
     （Chrome 的防自欺粘贴机制，只需第一次）。
3. 把 `get-muse-token.js` **整段**粘贴进控制台，回车。
4. 成功时控制台打印一段 JSON 并显示 `✅ 已复制到剪贴板`，内容形如：

   ```json
   {
     "vm_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
     "access_token": "...",
     "hatch_token": "...",
     "issued_at": "2026-10-14T11:15:46.000Z",
     "issued_at_local": "2026/10/14 19:15:46",
     "exp": null
   }
   ```

5. 把这段 JSON 存成一个文件（例如 `token.json`），**以文件形式交给
   Muse**（走文件投递，不要贴进聊天明文）。

### 第二段：Muse 接手（在本机安装与验证）

6. Muse 收到文件后安装到桥：先备份旧凭证
   `cp -a ~/.config/native-bridge/token.json ~/.config/native-bridge/token.json.bak`
   （旧文件可能已失效，备份只为回滚对照），再把新文件放到
   `~/.config/native-bridge/token.json` 并 `chmod 600`。装入前核对
   两点：输出 JSON 里的 `vm_id` 与预期 VM 一致；`issued_at` 与导出
   时刻一致（别把旧的一份又贴回来）。若输出的 vm_id 与现文件不同，
   同步更新 `vm_id.txt`。
7. 验证：桥靠凭证文件的 cred_stamp 变化自动感知，一般**无需重启**，
   三渠道 `idle_scan_fail_streak` 会自行清零。想要立刻实证，用桥
   自己的 `connect()` 验活（只打印成败、不打印凭证值）：

   ```bash
   ~/muse-test-venv/bin/python ~/workspace/cred-replace-2026-10-14/verify.py
   # 期望输出：CONNECT_OK sessions.list passed
   ```

## 输出字段

| 字段 | 来源 | 说明 |
| --- | --- | --- |
| `vm_id` | `/api/session` | 当前 VM 的 id，桥据此拼 WSS 地址 |
| `access_token` | `/api/auth/check` | 平台 access token |
| `hatch_token` | `/api/hatch/token` | 桥连 VM 用的 JWT，`connect()` 必需 |
| `issued_at` | hatch_token 的 `iat` | 签发时刻（UTC ISO），用来核对新旧 |
| `issued_at_local` | 同上 | 签发时刻的本地时间字符串 |
| `exp` | hatch_token 的 `exp` | 过期时刻（epoch 秒），平台不给时为 `null` |

桥的 `connect()` 实际只取 `vm_id`、`access_token`、`hatch_token` 三个
字段，其余是给人核对用的。

## 出错对照

- `auth/check 没返回 access_token，可能未登录`：当前页未登录或登录态
  已过期，先在页面上正常登录再重跑。
- `session 没返回 vm_id（VM 可能在重启）`：等 VM 起来、页面能正常打开
  会话后再重跑。
- 控制台报 `Failed to fetch` / 跨域错误：确认控制台是在 muse.ai 页面的
  上下文里打开的（脚本用的是相对路径 `/api/...`，在别的站点页面跑必然
  失败）。
- 装完桥仍旧 401：多半是贴回了旧 JSON 或存错路径；核对 `issued_at`
  是不是刚刚这次导出的时刻、路径是不是
  `~/.config/native-bridge/token.json`。

## 安全须知

- 输出的 JSON 是**等同登录凭证**的真 token：只存本机
  `~/.config/native-bridge/token.json`（0600），不要贴进聊天、issue、
  截图或任何会入仓/同步的位置；交给 Muse 时用文件投递，
  别经第三方渠道。
- 本脚本本身不含任何凭证，可以正常入仓和分享；要防的是它的**输出**。
- token 会被服务端失效或到期，失效就重跑本脚本导出一份新的，不需要
  改任何代码。
