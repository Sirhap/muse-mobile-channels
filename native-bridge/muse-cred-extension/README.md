# Muse 桥凭证导出浏览器扩展（兼容 Chrome / Edge / Firefox）

扩展用于导出包含 HttpOnly Cookie 的全套凭证。

- `muse.ai` 的登录会话 Cookie 带有 `HttpOnly` 标记，页面内 JS（`document.cookie`）无法读取，因此控制台脚本 [`get-muse-token.js`](../get-muse-token.js) 只能导出回退用的 `token.json`。
- 本浏览器扩展通过 WebExtensions 原生 `cookies` API 直接从浏览器存储层读取包含 `HttpOnly` 在内的完整 Cookie，并同时在已打开的 `muse.ai` 标签页中换出 `vm_id.txt` 和 `token.json`，一步到位导出桥所需的全套凭证：
  1. `cookies.txt`（主力凭证，会话有效时可换取连接 token，可选 Netscape 标准格式或单行 Header 格式）
  2. `vm_id.txt`（虚拟机 ID）
  3. `token.json`（回退凭证）

---

## 一、 安装方法

扩展源码目录：`native-bridge/muse-cred-extension/`

如果收到扩展源码 ZIP，先解压；要加载的是含 `manifest.json` 的扩展目录。它与扩展导出的 `muse-credentials.zip` 不同，后者含真实登录凭证，不能分享为安装包。

### 1. Google Chrome（谷歌浏览器）

1. 地址栏输入 `chrome://extensions/` 并回车。
2. 打开右上角的 **开发者模式（Developer mode）** 开关。
3. 点击左上角 **加载已解压的扩展程序（Load unpacked）**。
4. 选择本仓库下的 `native-bridge/muse-cred-extension` 文件夹即可。

### 2. Microsoft Edge（微软浏览器）

1. 地址栏输入 `edge://extensions/` 并回车。
2. 打开左侧或左下角的 **开发人员模式（Developer mode）** 开关。
3. 点击顶部 **加载解压缩的扩展（Load unpacked）**。
4. 选择本仓库下的 `native-bridge/muse-cred-extension` 文件夹即可。

### 3. Mozilla Firefox（火狐浏览器）

1. 地址栏输入 `about:debugging#/runtime/this-firefox` 并回车。
2. 点击 **临时载入附加组件（Load Temporary Add-on...）**。
3. 进入 `native-bridge/muse-cred-extension` 目录，选中里面的 `manifest.json` 文件即可。临时加载会在浏览器重启后移除，届时需重新加载。

---

## 二、 使用步骤

1. 在浏览器中打开 <https://muse.ai> 并确认处于**已登录状态**（能正常看到自己的会话）。
2. 点击浏览器工具栏右上角的 **Muse Bridge Credential Exporter** 扩展图标。
3. 弹窗打开后会自动读取：
   - **主力凭证 `cookies.txt`**：显示读取到的 Cookie 总数及其中 `HttpOnly` Cookie 的数量。
   - **虚拟机标识 `vm_id.txt`**：自动从 `/api/session` 获取当前 VM ID。
   - **回退凭证 `token.json`**：自动获取 `access_token` 与 `hatch_token`。
4. 确认三项均读取成功，再点击 **「📦 一键打包下载 ZIP (muse-credentials.zip)」**。部分失败时先处理错误，不把 ZIP 下载成功当成认证通过。
5. 将 `muse-credentials.zip` 通过环境支持的安全文件投递交给 Muse。Muse 先解压到临时目录，核对完整文件、目标 VM 和导出时间，再备份旧凭证（目录 `0700`），安装到实际凭证目录（默认 `~/.config/native-bridge/`，目录 `0700`、三文件 `0600`）。VM 不一致时停止并确认，不直接覆盖或切换 VM。
6. Muse 执行分来源验活：

```bash
~/muse-test-venv/bin/python ~/workspace/native-bridge/check_credentials.py --source all --json
```

Cookie、token 和审批查询分别确认。token 检查优先使用服务环境中的 `NATIVE_TOKEN_JSON`，存在覆盖时不代表磁盘 token 已通过。认证恢复后，还须在每个启用渠道完成真实入站问题和正式回复，核对 msgid、回复队列 ID 与投递 `ok=true`；进程在线或 ZIP 成功导出不能代替这一步。安装和恢复细节见 [桥凭证恢复说明](../get-muse-token.md) 与 [Muse 实测清单](../../docs/bridge-credential-health-muse-cases.md)。

如果桥或审批提示凭证失效，用户重新登录 Muse，通过扩展导出新 ZIP 交给 Muse，重复上述流程。名义过期日期只是参考，实际可能提前作废；不保证一天、一周或一个月可用。

---

## 三、 安全须知

- 扩展在本地读取浏览器 Cookie，并向 Muse 请求 VM 与 token；实际权限以 `manifest.json` 为准。
- 导出的 `cookies.txt` 和 `token.json` 等同于你的账号登录态，**严禁贴入聊天窗口或提交进代码仓库**，仅保存在运行桥服务的 `~/.config/native-bridge/` 目录（权限 `0600`）。
