# 平台核查 runbook（待办 21，路线 C 的裁决环节）

任何被唤醒来做平台核查的 agent 照此执行。核查 agent 的权限边界：
**只写裁决文件**。不给用户发消息、不动批次状态、不续跑、不取消、
不改 hook state 里的其他文件。

## 背景（一分钟版）

冷通道 worker 开工时自登记：把自己第一条消息头部的 `run_id`
（`event-hook:<uuid>`）连同 msgid 写进本渠道
`state/worker_activity.json`。hook 轮询发现某批次「自推静默 ≥300s、
无任何鲜活信号、无正式回复」的可疑形态时，在 hook state 目录落一
个核查请求标记 `platform_check_requests.json`（同 run 每 300s 最多
1 次，每 600s 窗口最多 2 次；更换 run 重新计数）。Muse 主会话或已确认
启用的核查任务读标记，按 run_id 查平台记录，把裁决写进同渠道的
`worker_platform_status.json`，供 `death_watch_activity.py` 的第三
信号源读取。2026-11-04 起，没有可靠裁决时不会因静默自动重派或取消；
hook 登记 `worker_review.json` 并提醒主会话或管理员核查。待核查不是
执行仍在运行或移交成功的证明。新消息仍能处理。

仓库只产生请求并读取裁决，不包含能直接访问 Muse 执行数据库的查询器。
部署时确认谁消费请求、用什么已授权工具查询；归档的 cron 配置不证明
核查任务正在运行。没有可用查询工具时如实报告阻碍，保留待核查。

## 输入：核查请求标记

每渠道一份，位于 hook state 目录：

- 微信：`~/hooks/state/weixin-bot/platform_check_requests.json`
- 企微：`~/hooks/state/wecom-bot/platform_check_requests.json`

格式：`{"requests": {"<msgid>": {"run_id": "event-hook:<uuid>",
"count": <已请求次数>, "last_request_ts": <epoch>,
"requested_at": <epoch>, "window_started_at": <epoch>,
"silence_secs": <int>}}, "updated": <epoch>}`

优先处理新鲜请求；旧请求不能据此推断任务已经停止。核查前读取当前
`worker_activity.json`，确认请求 run_id 仍与当前登记一致，避免为旧 run
写入可被误用的裁决；待核查任务继续按节流规则请求核查。

## 核查：按 run_id 查 agent 状态

对每个待处理条目，使用环境中实际可用的只读查询工具。下面是既有 Muse
部署的 SQL 示例；先核对当前 schema，再用参数绑定传入 run_id，不把
文件里的字符串直接拼入 SQL。`spawn_metadata_json` 为 text 时先转 jsonb：

```sql
SELECT agent_id, status, updated_at, created_at,
       jsonb_extract_path_text(spawn_metadata_json::jsonb,
                               'execute_resolve_generation') AS generation
FROM agent.agents
WHERE spawn_metadata_json IS NOT NULL
  AND jsonb_extract_path_text(spawn_metadata_json::jsonb,
        'execute_resolve_run_id') = '<请求里的 run_id>'
ORDER BY created_at DESC
LIMIT 5
```

判读：

- **恰好一行**：取该行的 `status` 与 `updated_at`（bigint epoch 秒）。
- **多行**（同 run 多代，spike 窗内未观测到，但字段存在）：取
  `created_at` 最新（即排序第一行）的一行，并在裁决里照常写；
  若各行 status 互相矛盾（有的 running 有的终态），写 `unknown`。
- **零行**：写 `unknown`（查不到 = 无信号，绝不猜）。
- 查询本身失败/超时：本轮跳过该条目，**不要写裁决**（写了反而
  把「没查」伪装成「查过」；hook 那边请求计数已消耗，下一次
  请求会再次核查，任务继续待核查，不能把没查伪装成已终止）。

status 原样透传，只认这组词：`running` / `completed` /
`shutdown` / `errored` / `interrupted`。出现组外新词时写
`unknown`，不要自创映射。

## 输出：裁决文件

每渠道一份，位于该渠道 bot state 目录（与 `worker_activity.json`
同级）：

- 微信：`~/workspace/weixin-bot/state/worker_platform_status.json`
- 企微：`~/workspace/wecom-bot/state/worker_platform_status.json`

格式（按 msgid 合并写，不要整表覆盖掉别的 msgid 的裁决）：

```json
{"<msgid>": {"run_id": "event-hook:<uuid>",
             "status": "shutdown",
             "agent_updated_at": 1792000000,
             "checked_at": 1792000300}}
```

- `status`：上一步判读的结果；查不到/对不上/多行矛盾时写
  `"unknown"`，此时 `agent_updated_at` 写 0。
- `run_id`：必须非空且与当前 worker 登记完全一致；没有当前登记或无法
  确定执行归属时，不写可作为终态证据的裁决。重跑任务登记新 run 后，旧
  run 的裁决不再可用。
- `checked_at`：写裁决这一刻的 epoch 秒。探针只信
  `checked_at` 距今 ≤600s 的裁决，过期自动降级为无信号——所以
  **不要回填旧时间、不要预写未来时间**。
- 写入方式：读现有文件 → 按 msgid 合并 → 写临时文件再
  `os.replace` 原子替换（与仓库其他 state 文件同一模式）。
  文件不存在就新建；文件损坏就整表重写（裁决是快照，不是历史）。
- 写完后把已处理条目从请求标记里删掉？**不删**。请求计数是
  hook 的节流账本，核查方只读不写标记文件；hook 自己按
  7200s 窗口老化它。

## 写完之后

什么都不用做：不通知用户、不写批次状态。hook 在后续轮询里会
读到裁决——终态 + 静默 ≥300s + 无 hold + 无正式回复时提前进
判死路由（续跑通知/fail-stop 通知会注明依据平台终态）；running
且 agent_updated_at 新鲜时按鲜活信号 hold。裁决迟迟不到或写了
unknown，任务继续待核查，不增加恢复次数，也不自动取消。核查方不是
重新执行任务的 worker；不能因为查到 running、unknown 或查不到记录，
擅自重跑原任务。完整验收见
[长任务待核查修复说明](task-interruption-review-2026-11-04.md)。
