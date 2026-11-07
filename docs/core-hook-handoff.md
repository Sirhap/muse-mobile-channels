# Core 向 hook 移交

Core 的 Gadget 与 Native 都明确没有提交用户任务时，可以把任务转交给助手平台的 hook worker。该路径使用独立的 `core_hook_handoffs` 账本，由 Core 向原会话投递结果，适用于现有 Core 模式。旧渠道收件钩子继续遵守其 legacy 模式约束。

开启前先在测试环境安装本项目，并注册 [示例定义](../hooks/definitions/core-handoff.example.json) 和 [轮询脚本](../hooks/scripts/core-handoff.sh)。通过平台 hooks API 创建定义，把 `delivery.to` 改为自己的侧会话 ID。平台 worker 必须提供真实的 `run_id`，用于确认本次移交。注册和生产启用属于部署操作。

在对应渠道的现有 modes 配置中增加 `"hook_handoff": true`，保留其 `mode: core`、数据库、账号和发送者配置。按现有模式切换流程停止写者后修改配置。未开启的渠道继续使用原执行路径。

为 hook 脚本设置以下环境变量，并确保选用的 Python 已安装本项目。每条渠道配置独立的 hook 定义，使用相同的数据库和对应渠道。

```sh
export MUSE_CHANNEL_MODES="$HOME/workspace/state/channel-modes.json"
export MUSE_CORE_HANDOFF_DATABASE="$HOME/workspace/state/core.sqlite3"
export MUSE_CORE_HANDOFF_CHANNEL=wecom
export MUSE_CORE_HANDOFF_PYTHON="$HOME/muse-test-venv/bin/python"
```

移交条件是 Native 返回带明确原因的 `not_submitted`。连接、认证或发送前检查失败可以提供这类证据。请求已经发出、没有收到 ACK、超时、执行状态未知，以及已有外部执行绑定的任务，都不会进入 hook 队列。输入不支持或媒体校验失败也不会借此重新执行。

轮询只把 queued 项目提供一次，在输出唤醒载荷前先持久化 offered。平台丢失这次输出、没有启动 worker 或 worker 没有回执时，项目保留待核对，不会因超时再次输出。Core 任务在收到 worker 回执前保持 reconciling，回执之后记为已提交。不能把排入账本说成平台已接收。

worker 使用本次顶层载荷里的数据库、modes、handoff ID 和 capability 调用本地命令。capability 是当前任务的凭据，应留在受控的 hook 载荷与命令环境中。不要复制到公开日志。任务正文和附件不能替换这些绑定值。

```sh
python -m mobile_channels.handoff ack --database "$DB" --modes "$MODES" \
  --handoff-id "$HANDOFF_ID" --capability "$CAPABILITY" --worker-id "$RUN_ID"
python -m mobile_channels.handoff status --database "$DB" --modes "$MODES" \
  --handoff-id "$HANDOFF_ID" --capability "$CAPABILITY"
python -m mobile_channels.handoff complete --database "$DB" --modes "$MODES" \
  --handoff-id "$HANDOFF_ID" --capability "$CAPABILITY" --worker-id "$RUN_ID" \
  --state succeeded --text-file /absolute/path/result.txt
```

先 ACK，再执行任务。ACK 失败或绑定了其他 worker 时，停止操作并核对。相同 worker 可以重复 ACK，相同完成结果可以重试写回，账本只保留一次结果。任务路由来自 Core 数据库的快照，worker 不接受调用方提供新的收件人。结果、附件进入 Core 原有 outbox，执行结果写回不代表平台已收取消息。

尚未 offered 的移交可以本地取消。已经 offered 或 ACK 的移交只登记取消请求，worker 必须通过 `status` 检查，停止实际操作后用 `complete --state cancelled --evidence worker_stopped` 确认。worker 在看到 `cancel_requested` 后继续工作、无法访问本地账本，或不能提供真实执行身份时，需要操作员核查。

本地回归命令为 `python -m unittest discover -s tests -p test_core_hook_handoff.py`。生产验收还需要验证真实 hook 唤醒、真实 run_id 回执、任务完成和 Core 向原会话投递四段记录。
