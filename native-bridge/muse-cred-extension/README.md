# Muse 桥凭证导出浏览器扩展（兼容 Chrome / Edge / Firefox）

为什么需要扩展而不是控制台 JS？
- `muse.ai` 的登录会话 Cookie 带有 `HttpOnly` 标记，页面内 JS（`document.cookie`）无法读取，因此控制台脚本 [`get-muse-token.js`](../get-muse-token.js) 只能导出回退用的 `token.json`。
- 本浏览器扩展通过 WebExtensions 原生 `cookies` API 直接从浏览器存储层读取包含 `HttpOnly` 在内的完整 Cookie，并同时在已打开的 `muse.ai` 标签页中换出 `vm_id.txt` 和 `token.json`，一步到位导出桥所需的全套凭证：
  1. `cookies.txt`（主力凭证，支持自动续期，可选 Netscape 标准格式或单行 Header 格式）
  2. `vm_id.txt`（虚拟机 ID）
  3. `token.json`（回退凭证）

---

## 一、 安装方法

扩展源码目录：`native-bridge/muse-cred-extension/`

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
3. 进入 `native-bridge/muse-cred-extension` 目录，选中里面的 `manifest.json` 文件即可。

---

## 二、 使用步骤

1. 在浏览器中打开 <https://muse.ai> 并确认处于**已登录状态**（能正常看到自己的会话）。
2. 点击浏览器工具栏右上角的 **Muse Bridge Credential Exporter** 扩展图标。
3. 弹窗打开后会自动读取：
   - **主力凭证 `cookies.txt`**：显示读取到的 Cookie 总数及其中 `HttpOnly` Cookie 的数量。
   - **虚拟机标识 `vm_id.txt`**：自动从 `/api/session` 获取当前 VM ID。
   - **回退凭证 `token.json`**：自动获取 `access_token` 与 `hatch_token`。
4. 点击 **「📦 一键打包下载 ZIP (muse-credentials.zip)」**，即可一次性下载包含 `cookies.txt`、`vm_id.txt`、`token.json` 三个文件的标准压缩包。放到 Muse 机器上直接解压并设置 `0600` 权限即可：

```bash
mkdir -p ~/.config/native-bridge && chmod 700 ~/.config/native-bridge
unzip -o muse-credentials.zip -d ~/.config/native-bridge
chmod 600 ~/.config/native-bridge/cookies.txt \
          ~/.config/native-bridge/vm_id.txt \
          ~/.config/native-bridge/token.json
```

---

## 三、 安全须知

- 本扩展**纯本地运行**，不包含任何外部上报或第三方网络请求，只访问 `https://muse.ai`。
- 导出的 `cookies.txt` 和 `token.json` 等同于你的账号登录态，**严禁贴入聊天窗口或提交进代码仓库**，仅保存在运行桥服务的 `~/.config/native-bridge/` 目录（权限 `0600`）。
