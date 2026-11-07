# 安装检查与验收

从仓库根目录执行只读诊断。检查依赖、SDK 源码、凭证文件存在性与权限、模式配置及 Core 数据库。
诊断不读取凭证内容、不连接 Muse，ready 仅表示本地安装条件通过。

```sh
python -m mobile_channels doctor --workspace "$PWD" --home "$HOME"
```

在线认证检查使用已安装 Muse SDK 的 Python。

```sh
~/muse-test-venv/bin/python native-bridge/check_credentials.py --source all --json
```

Cookie 可能提前被服务端撤销。失效后本人重新登录浏览器，
按 [凭证恢复说明](../native-bridge/get-muse-token.md) 导出与安装。
Gadget 凭证独立工作，首次配对仍需 macOS 蓝牙。

## 生成服务安装计划

按 [快速开始](../README.md#快速开始) 准备依赖、渠道凭证和桥配置后，生成服务单元。

```sh
python -m mobile_channels service-plan --workspace "$PWD" --home "$HOME" \
  --channels wecom --output /tmp/muse-service-plan
```

output 必须是新目录，支持 --channels wecom weixin 同时生成两条渠道。
计划使用家目录下的 workspace 作为部署位置，源代码应已复制到该目录。
检查 ExecStart、WorkingDirectory、HOME、桥配置和凭证路径后，在已授权的 Linux
测试部署上执行输出目录中的 install.sh。脚本会安装和启动服务。生成计划本身只写指定目录。

计划默认安装 legacy。Core 切换需先准备准确的数据库与 state/channel-modes.json，
停止旧写者，再按模式切换流程启动。[Core hook 接手](core-hook-handoff.md)
需要注册独立 hook 并显式启用。

## Gadget 图片与取消

图片支持 JPEG、PNG、GIF、WebP，单张不超过 2 MiB。图片验证后写入权限 0600
的本地快照，由 Worker 与文字一起发送，运行记录目录为 0700。
超限或不支持的图片在发送前拒绝。设备不可用时可回退 Native。

Core runner 服务环境设置 MUSE_GADGET_SERVER_CANCEL=1 后，可借用 Native 桥
请求取消同一侧会话。请求前核对本轮 ACK、最新用户消息及任务身份，出现其他新回合时跳过会话取消。
只有查询到关联 task_id 或父消息的取消终态才确认平台取消。
历史不可用、接口失败或缺少终态时，仍停止本地等待，并说明 Muse 端可能继续执行。
本地模拟通过后，仍须用测试账号确认实际接口权限。

## 路径验收

| 路径 | 应核对的证据 |
|---|---|
| Gadget | 入站 msgid、发送 ACK、完成结果、原聊天投递；另测一张真实图片 |
| Native | Cookie/token 验活、发送记录、匹配历史回复、原聊天投递 |
| Core hook | 未提交证据、唯一 offered 记录、真实 worker ACK、完成记录、Core 投递 |
| 媒体出站 | 图片、视频、文件及纯附件各一次；上传成功与渠道接收分别确认 |
| 迟到回复 | 首段送达后出现第二段，重启后只补发一次；新用户回合后只接收明确关联的旧回复 |
| 取消 | 有关联终态才确认服务端取消，接口不可用时明确提示本地停止 |

真实验收需要部署地址、测试账号凭证和实际 hook 平台。服务重启、hook 注册、
模式切换与生产状态变化属于部署操作。
侧会话的上下文是否共享由 Muse 平台决定，本地能够约束消息路由和回复关联。
