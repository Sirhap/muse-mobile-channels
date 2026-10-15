# 平台核查 runbook（待办 21，路线 C 的裁决环节）

任何被唤醒来做平台核查的 agent 照此执行。核查 agent 的权限边界：
**只写裁决文件**。不给用户发消息、不动批次状态、不续跑、不取消、
不改 hook state 里的其他文件。

## 背景（一分钟版）

冷通道 worker 开工时自登记：把自己第一条消息头部的 `run_id`
（`event-hook:<uuid>`）连同 msgid 写进本渠道
`state/worker_activity.json`。hook 轮询发现某批次「自推静默 ≥300s、
无任何鲜活信号、无正式回复」的可疑形态时，在 hook state 目录落一
个核查请求标记 `platform_check_requests.json`（每批次每 300s 最多
1 次、最多 2 次）。本 runbook 的消费者（低频 agent cron，规格见
`death-signal-build-2026-10-15/platform-check-cron-spec.md`）读标记、
按 run_id 查平台记录、把裁决写进同渠道的
`worker_platform_status.json`，供 `death_watch_activity.py` 的第三
信号源读取。没有裁决时 hook 的行为与从前完全一致（1800s 原路径）。

## 输入：核查请求标记

每渠道一份，位于 hook state 目录：

- 微信：`~/hooks/state/weixin-bot/platform_check_requests.json`
- 企微：`~/hooks/state/wecom-bot/platform_check_requests.json`

格式：`{"requests": {"<msgid>": {"run_id": "event-hook:<uuid>",
"count": <已请求次数>, "last_request_ts": <epoch>,
"requested_at": <epoch>, "silence_secs": <int>}}, "updated": <epoch>}`

只处理 `requested_at` 距今 < 1800s 的条目；更老的条目忽略（批次
多半已经由原 1800s 路径收尾，裁决写了也没人读）。

## 核查：按 run_id 查 agent 状态

对每个待处理条目，用 `muse.db` 工具跑一条只读 SELECT（已实证
可用；`spawn_metadata_json` 是 text，先转 jsonb 再取键）：

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
  请求或 1800s 原路径会兜底）。

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
unknown，原 1800s 判死路径完全不变。
