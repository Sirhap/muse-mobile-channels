# 核心接入、权限、恢复与切换记录

日期：2026-11-01；分支：`refactor/reliable-channel-core`；原始基线：`58537a0`。

本文保留 2026-11-01 的设计与验收记录。后续图片、附件、审批及 Gadget 历史兜底的
支持范围和当前安装步骤见 [README](../README.md#架构改造与本地工具)。
企微恢复脚本只在 `state/channel-modes.json` 明确配置 Core 且数据库存在时安装
模式 drop-in 和 Core runner；重启失败会保留待重试标记。

本次继续完成本地可验证的核心接入和故障处理。默认部署保持 legacy；core 是显式启用的文本实验链路，尚不能作为现有全部功能的生产替代。未部署、未迁移生产状态，未提交或推送。

## 已落实的功能

| 范围 | 实现及保护 |
| --- | --- |
| 实际入站 | 微信和企微真实回调可直接入 SQLite，不写旧 inbox/spool；按渠道、账户和聊天去重，不受旧全局 seen 记录影响 |
| 会话与命令 | session 按 conversation/generation 隔离；命令结果、上下文变更、回复路由同事务；群管理命令需独立管理员权限 |
| 执行与取消 | 提交前记录 unknown；真实 run 关联后才自动查询；执行、查询、取消前重新检查所有权，取消未知不释放任务 |
| 实际投递 | 网关的真实发送方法由 SQLite 投递 worker 调用；绑定原目标和微信 context_token，按账户领取及会话 FIFO，超时不自动重复发送 |
| 审批 | 原会话、操作者、有效期、原决策 attempt 校验；外部提交前可靠记录 unknown；不按相似文本扩散批准，不把通知广播到其他聊天 |
| 新旧互斥 | gateway、旧桥、旧 CLI、hooks 和审批 relay 遵守同一渠道模式 lease；运行中的参与写者阻止配置切换 |
| 旧桥恢复 | 必填原消息、会话、摘要及核查证据；已接受恢复原 turn，明确未提交进入 recovery_queue；偏移不倒退，恢复写者与 daemon 互斥 |
| 备用交接 | 使用原始消息路由；相同 msgid/路由的重复交接不再次追加 inbox |
| 运维工具 | SQLite backup API 包含 WAL，完整性检查和只向新目标恢复；影子取证、迁移预览和只读就绪报告 |
| 安装与检查 | 可编辑安装、wheel 安装、本地 CLI、独立沙箱运行入口、Linux/macOS CI 配置 |

新模块按配置、入站、任务存储、执行、投递、审批、模式、文件操作、恢复、备份、迁移及就绪检查划分。旧大型网关及钩子仍保留协议与兼容业务；尚未将所有旧代码拆解迁入新包。新增模块多数控制在 250 行以内，事务存储约 450 行。

## 模式配置及实验边界

模式文件是外部部署配置，通过 `MUSE_CHANNEL_MODES` 指定。以下是结构示例，路径及身份应替换为实际沙箱值：

```json
{
  "channels": {
    "weixin": {"mode": "legacy"},
    "wecom": {
      "mode": "core",
      "database": "/absolute/sandbox/core.sqlite3",
      "account_id": "sandbox-bot",
      "allowed_senders": ["alice", "admin"],
      "administrators": ["admin"]
    }
  }
}
```

模式文件中 account_id 是操作员显式绑定的渠道账户，不从消息正文读取。allowed_senders 允许提问，administrators 才允许群聊 `/new` 和 `/stop`。私人会话命令作用于自己的 conversation。`/status`、`/queue`、`/check` 查询限定当前聊天；其他未迁移命令返回明确反馈，不调用旧的全局管理路径。

当前 core 入站接受文本。媒体只保存类型等必要记录并返回未启用媒体适配的反馈，不自动降级到另一个执行者。旧审批卡片事件在 core 模式不再进入 legacy 执行链；新核心已有 ApprovalStore 权限与事务接口，但尚未打通真实平台审批事件及 `/a` 卡片的完整接入。core 模式因此不能直接替换需要这些功能的生产渠道。

模式 lease 只对已升级且持有相同配置文件的程序有效，不能阻止旧二进制或手工写文件。升级后必须先停止旧写者、核查在途任务、确认相关进程都读取同一 `MUSE_CHANNEL_MODES`，才讨论切换。不要手工编辑运行中的模式文件；使用 `mobile_channels.modes.write_modes` 会获取排他锁。整个模式文件共享锁，因此其他渠道仍运行时也会阻止修改，操作计划须包含这一点。

## 核查、备份和迁移命令

```sh
# 创建全新实验库；不是生产旧状态的替代品
muse-channels core-init --database /tmp/core-sandbox.sqlite3

# 对运行库进行一致备份；目标必须尚不存在
muse-channels database-backup --source /path/core.sqlite3 --destination /backup/core-01.sqlite3
muse-channels database-check --source /backup/core-01.sqlite3
muse-channels database-restore --source /backup/core-01.sqlite3 --destination /new-path/restored.sqlite3

# 旧状态语义预览；accounts.json 是明确的 channel -> account_id 映射
muse-channels migration-preview --workspace /stopped-snapshot \
  --database /new-path/migration-preview.sqlite3 --accounts /path/accounts.json --stopped-snapshot

# 停止旧桥后检查待核查记录；输出含原始内容，按敏感运维资料保管
muse-channels bridge-inspect --base /path/native-bridge --channel wecom

# 只在真实执行证据已核实后，使用上一步精确 msgid/session/digest
muse-channels bridge-resolve --base /path/native-bridge --channel wecom \
  --msgid ORIGINAL --session-id ORIGINAL_SESSION --digest INSPECTED_SHA256 \
  --outcome accepted --evidence-file /private/evidence.txt
```

`not_submitted` 仅在证据明确原任务未被接收时使用。结果缺失、网络超时、worker 失联均不能证明未提交。操作会记录审计并验证状态摘要，期间若状态变化或桥仍持锁，恢复会拒绝执行；不要删除 pending、清空状态或倒退偏移绕过保护。

迁移工具完整保存旧 offset、tombstone、partial、parked、审批等原始证据。可验证路由的消息保守导入 reconciling，其余记录报告为未解释，不把旧文件中的“已入队”判成送达。当前仍是非执行预览，尚无完成全部旧业务解释的迁移终结器及反向导出器。运行入口明确拒绝影子库和任何带 migration_report 的预览库，即使手工修改报告 executable 字段也不能直接激活。

## 执行与就绪检查

```sh
# wheel 安装需要指向另行部署的 Muse 桥源码，校验阶段不读凭证
export MUSE_BRIDGE_SOURCE=/absolute/workspace/native-bridge/native_bridge.py
muse-channels core-readiness --database /sandbox/core.sqlite3 --modes /sandbox/channel-modes.json

# 仅用于明确配置的实验账户；不会自动执行迁移预览
muse-channels core-run --database /sandbox/core.sqlite3 --modes /sandbox/channel-modes.json --once
```

也可使用 `MUSE_WORKSPACE` 定位原生桥。启动前会验证路径和数据库；真实调用时才加载 Muse SDK 和凭证。运行时缺少 SDK 或认证失败不会伪造正常状态。

旧 `chat.stream` 通常不返回可验证的 run_id。当前执行适配只接受平台明确提供的 run 关联，查询也只接收相同 run 的事件。没有关联时保留 unknown，不用“基线后的任意回复”猜测结果。因此尚不能宣称真实 Muse 自动闭环通过。`core-readiness` 会明确报告该阻碍，不允许用一个本地布尔开关宣称已经验收。

## 验证及剩余条件

本地 Python 3.12 沙箱覆盖真实入站、真实发送循环与线程转接、数据库、权限和故障恢复。统一入口为 `python tests/run_core_regression.py`。普通 wheel 安装已在独立虚拟环境、仓库外验证 CLI、初始化及完整性检查；缺少单独部署的 transport 会在领取执行任务前报错。

新增 CI 在 Ubuntu 和 macOS 配置相同沙箱入口，但本次未远端运行 CI，不能当作 Linux/systemd 生产验收。

生产接管仍需完成：真实 Muse run 关联验收；媒体及其余旧命令/审批的适配；旧状态全量语义迁移和反向导出；Linux 服务、杀进程及真实平台联调。保留 legacy 是当前的运行兼容措施，不能把文本实验链路的测试通过当成这些项目已完成。

最终本地验收：30/30 组沙箱套件通过，共 276 项 unittest 测试与 266 项脚本断言，总计 542 项；49 个新增或修改的 Python 文件编译通过，`git diff --check` 通过，本轮测试日志无 ResourceWarning。实际旧 hooks/CLI 在 core 模式下被阻止写入的入口级验证也已通过。

本机已检查常见的 `~/workspace/reference/muse-cli/src` 等部署位置，未找到可用于真实关联验收的 SDK。下一阶段需要操作员提供 SDK 源码路径及测试部署位置或连接方式；不需要在聊天中发送凭证。取得这些信息前，不执行生产切换、不声称真实平台闭环或全量迁移已经完成。
