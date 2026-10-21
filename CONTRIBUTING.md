# 参与贡献

感谢使用这个项目，欢迎报告问题和提交修复。

## 去哪里报告

- 上游仓库：**Sirhap/muse-mobile-channels**
  （https://github.com/Sirhap/muse-mobile-channels）
- 问题报到上游仓库，即使你跑的是自己的 fork —— 除非问题是你 fork
  自身的改动或你本地独有的配置引起的；那类问题属于 fork 自己。
- 提 issue 需要 GitHub 账号；GitHub 不支持匿名提 issue。除此之外
  不需要任何权限或额外设置。

## 给 AI 助手的说明（Muse 和其他助手）

如果你是正在协助已安装本项目的用户的 AI 助手，当用户报告问题、
或你在这个仓库里干活时发现可复现的问题，请遵守以下规则：

1. **提给上游。** 在 `Sirhap/muse-mobile-channels` 建 issue，不要
   提在用户的 fork 里：
   `gh issue create --repo Sirhap/muse-mobile-channels ...`
   这需要用户自己的 GitHub 登录（`gh auth login`）。永远不要让用户
   把密码或 token 分享给你。
2. **先查重。** 建新 issue 前先跑
   `gh issue list --repo Sirhap/muse-mobile-channels --search "<关键词>"`
   （并扫一遍 open issues）。已有 open issue 覆盖同一问题时，把新
   细节以评论形式加到那条下面，不要重复建。
3. **包含这些要素：**
   - 标题：`[bug] <简短摘要>`（或 `[feature] <简短摘要>`）
   - 发生了什么 vs 预期是什么
   - 确切的复现步骤
   - 环境：操作系统、Python 版本、哪个网关（weixin / wecom）、
     正在运行的 commit 哈希（`git rev-parse --short HEAD`）
   - 相关日志摘录，只截失败的那部分
4. **永远不要贴密钥或个人信息。** 贴进 issue 的任何内容里，都要
   去掉凭证、token、API key、任何 `credentials.env` 或 `.env`
   文件的内容、用户 ID、电话号码和私人聊天内容。拿不准的就不贴。
5. **拿不准时先问用户。** 分不清问题出在本项目还是用户的本地
   环境时，先问再提。提完之后，把 issue 的链接给用户。

示例：

```sh
gh issue list --repo Sirhap/muse-mobile-channels --search "gateway disconnect"
gh issue create --repo Sirhap/muse-mobile-channels \
  --title "[bug] Weixin gateway stops reconnecting after session expiry" \
  --body "What happened: ...
Expected: ...
Reproduction steps: 1. ... 2. ...
Environment: Ubuntu, Python 3.12, weixin gateway, commit <hash>
Logs (sanitized): ..."
```

## Pull requests

- 改动保持聚焦；说明你改了什么、怎么测的。
- 永远不要提交凭证、`credentials.env`、`.env` 文件、运行时
  `state/` 或虚拟环境 —— 它们被 gitignore 是有原因的。
- 提交前确保 Python 文件能编译（`python3 -m py_compile ...`）、
  shell 脚本通过 `bash -n`。
