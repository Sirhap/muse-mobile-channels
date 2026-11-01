# 架构改造实现与本地验证

日期：2026-11-01；基线：`58537a0`；分支：`refactor/reliable-channel-core`。

本次交付落实现有方案的可测试入口、事务核心及旧状态影子取证，并处理微信入站和隧道可靠性问题。现有网关继续使用原运行模式；新核心提供独立 API 和本地 CLI。SQLite 尚未接管实际渠道，影子工具不代表已完成旧状态的语义迁移。

## 已实现的职责边界

| 模块 | 职责 | 关键约束 |
| --- | --- | --- |
| `mobile_channels/config.py` | 启动配置、workspace 和数据库路径 | 显式环境映射；解析不创建文件 |
| `mobile_channels/models.py` | 入站消息、已提交标识和 worker 所有权 | 会话标识不用于拼接文件路径 |
| `mobile_channels/storage.py` | 接入、去重、任务、执行 attempt、结果与 outbox | 单事务；会话 FIFO；owner/token/lease 检查 |
| `mobile_channels/delivery.py` | 分段、发送 attempt、回执与核查 | 分段稳定；结果未知不自动重发 |
| `mobile_channels/shadow.py` | 旧状态按字节区间和摘要保存 | 只读旧文件；不创建执行任务 |
| `native-bridge/native_bridge.py` | Muse 执行适配 | 导入不读配置或加载 SDK；实际调用时加载 |

数据库连接启用外键、WAL、FULL 同步和 5 秒 busy timeout；新建数据库权限为 `0600`。事务只包含本地操作，网络由调用方在事务外执行。每个 worker 使用自己的 Store 连接。

## 行为清单与兼容性

| 场景 | 实现行为 |
| --- | --- |
| 微信接入写入失败 | 中止当前批次并保留同步游标；已入队消息由去重保护 |
| 明确坏消息 | 仅显式 `MalformedInboundMessage` 可以隔离；隔离写失败不推进游标 |
| 未分类异常 | 保留游标，避免把程序错误当作坏消息跳过 |
| 桥 spool 已写入但 seen 写失败 | 保持内存去重；重启从 spool 恢复 |
| 同消息并发接入新核心 | 唯一约束保证一个消息和一个任务，事务失败全部回滚 |
| 多会话 | 按 channel/account/chat_type/chat_id 隔离；同会话 FIFO，同会话最多一个活动任务 |
| `/new` 对应核心 API | 增加 generation，旧任务及回复保持原 generation |
| worker lease 过期 | 未提交可重新领取；提交过或结果未知进入 reconciling |
| 迟到 worker | owner、attempt、token、lease 校验阻止旧执行者提交 |
| 执行结果 | 与绑定会话的 outbox 在同一事务保存 |
| 取消 | 队列任务立即取消；活动任务保留位置至确认取消；成功结果不被迟到取消抹掉 |
| 投递 | 先落 sending attempt；全部分段确认后才标 delivered |
| 发送超时、发送者失联 | delivery_unknown；要求显式核查证据，不自动重复发送 |
| 隧道端口占用、监听失败 | 清理全部监听 socket 后退出，由进程管理器处理 |
| 代理握手失败 | 关闭未建立成功的上游 socket |

旧聊天命令、媒体处理、审批卡片、进度、队列和回复闸门保留现有入口。本次没有将这些业务搬到新核心；旧命令副作用与 seen 文件仍有非事务窗口。新核心的取消等接口要求调用方先完成操作者授权，不能直接把未验证的聊天命令接入。

## 本地安装和验证

使用 Python 3.12 或更新版本。原有两个 requirements 文件保持可用，版本与可选依赖组一致。

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/python tests/run_core_regression.py
.venv/bin/muse-channels core-init --database /tmp/muse-core-sandbox.sqlite3
```

`run_core_regression.py` 将支持的沙箱测试分别在子进程运行，覆盖新核心、影子证据、桥导入、微信故障、企微持久化、公共守卫及已有关键可靠性回归。完整旧测试集还包含导入时执行的历史脚本，部分需要生产 SDK、私人配置或 Linux Bash；不能把其中的环境失败当作本次代码通过或生产故障。

新核心典型执行顺序：`ingest` → `claim` → `begin_submission` → 外部提交 → `mark_submitted` → `complete`。必须在外部调用前调用 `begin_submission`。不明任务只能领取核查权，确认没有提交后带证据释放；找到原 run 或最终结果则绑定原 run 或保存结果。

投递顺序：`DeliveryStore.prepare` → `claim` → 外部发送 → `delivered` / `retry` / `reject` / `unknown`。`retry` 只接收确认的临时失败，超时交给 `unknown`。`resolve_unknown` 要求明确证据；人工选择重试可能重复发送，适配界面必须展示这个含义。

## 本次验证结果

macOS、Python 3.12.13 下，`python tests/run_core_regression.py` 的 13 组沙箱套件全部通过，共 178 项检查（142 项 unittest 测试及 36 项旧微信脚本断言）。17 个新增或修改的 Python 文件编译通过，`git diff --check` 通过。可编辑安装及仓库外调用 `muse-channels --help`、`core-init`、`shadow-import` 已验证。

首次完整旧目录扫描运行了 221 项 unittest 测试，出现 8 个失败和 23 个错误，包含本次随后修正的影子路径断言，以及旧 Bash 3.2 引号、私人配置和 SDK 导入依赖；该扫描还包含导入时执行的历史脚本，不作为统一绿色验收入口。此次最终验收以明确列出的 13 组沙箱套件为范围，不声称全量历史测试或真实渠道通过。Linux/systemd 和生产平台联调尚未执行。

## 影子取证

先对停止写入的旧状态制作一致快照，然后运行：

```sh
.venv/bin/muse-channels shadow-import \
  --workspace /path/to/legacy-snapshot \
  --database /tmp/muse-core-shadow.sqlite3
```

也可通过 `MUSE_WORKSPACE`、`MUSE_CORE_DB` 指定默认路径。工具保存 inbox、outbox、结果、offset、retry、partial、parked、桥 state（含 spool_offset/tombstones）、spool、管理指令与审批状态等已知来源的完整字节及摘要。重复导入同一修订不重复存储；半行、坏行、文件改写与截断保留为独立证据。输出只有统计信息，不包含正文。

工具检查单文件读取期间的改动，发现变化会回滚本轮导入；多文件在线读取没有跨文件一致性保证，因此报告始终标记 `reconciliation_required`。现阶段不解释墓碑、发送回执或在途运行的业务状态，也不创建 queued 任务。数据库内保存原始内容，按运行状态数据管理，不提交仓库。

## 生产切换前的后续工作

1. 将取证记录转换为可解释的消息、队列、审批、分段和执行状态，逐项核对旧新差异；不明状态必须待核查。
2. 实现 Muse 执行、实际渠道发送及审批授权的统一适配接口；保留现有业务特征测试。
3. 实现渠道模式配置、单写入者进程锁、完整备份恢复和反向导出工具；验证旧桥和钩子无法同时领取。
4. 在 Linux/systemd 沙箱演练杀进程、磁盘故障、重连及发送未知结果，再记录真实渠道验收。
5. 提交具体渠道切换和回退计划，经用户确认后才改生产服务或迁移生产状态。

本地分支可直接回到基线验证；生产切换后不能只恢复旧代码或旧数据库。必须保留新状态并核查在途请求，防止重复副作用。

后续风险修复见 [原生桥提交结果不明保护](bridge-submission-recovery-2026-11-01.md)，包含提交前意图、重启防重、取消失败保留与人工核查边界。
