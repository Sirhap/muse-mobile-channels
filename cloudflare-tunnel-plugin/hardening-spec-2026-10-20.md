# Cloudflare 隧道插件加固规格（2026-10-20 草稿，未实施）

状态：仅规格，未改任何生产文件。实施须用户批准后按既定分工派 Cursor（Grok 4.7 fast+xhigh）写码、本侧复核测试后部署。

## 实证背景（2026-10-20 心跳日志，~/memory/2026-10-20.md）

- 隧道三单元被整机换代冲掉并由心跳（30 分钟）跑 install.sh 重装的记录，一夜至少 6 次：01:34、03:34、05:04、07:04、08:04、09:04 CST；08:04 那轮巡检时公网为 530。
- 本轮实查（2026-10-20 01:16 UTC）：系统 boot 于 00:55 UTC；微信/企微/桥/审批中继四单元文件时间均为 00:55（开机即恢复），隧道四单元为 01:05（09:04 CST 心跳重装），即隧道恢复比其他服务晚约 10 分钟，最坏可晚约 30 分钟（心跳间隔）。
- 现行恢复链差异：channel-restore/restore.sh 由 channel-restore.timer 每 5 分钟（OnBootSec=3min）＋网关 hook 约 5 秒轮询驱动，已覆盖 wecom-bot、weixin-bot、native-bridge、approval-relay；唯独不覆盖隧道插件。隧道只靠 HEARTBEAT 30 分钟巡检兜底。

## 改动 1：把隧道纳入 channel-restore 的 5 分钟自愈链

在 channel-restore/restore.sh 的 main() 中、heal_relay 之后新增 heal_cloudflare()，仿 heal_relay 形态：

- 目标单元：intro-site、cloudflared-relay、cloudflared-tunnel（plugin.conf 中 ENABLE_PINGGY=1 时加 pinggy-tunnel）。
- 判定：任一目标单元文件在 /etc/systemd/system 缺失，或任一单元非 active → 调用 ~/workspace/cloudflare-tunnel-plugin/install.sh 一次（其本身幂等：渲染单元、daemon-reload、enable --now、跑 health.sh），并把结果记入 restore 输出。
- 全部在位且 active 时什么都不做、保持静默（install.sh 内含 health.sh 的公网多次重试，最坏约 90 秒，不能每 5 分钟无条件跑）。
- 约束：install.sh 失败不得让 restore.sh 整体失败口径误报网关故障——heal_cloudflare 的失败单独计数/单独一行输出，不翻转网关的 FAIL 结论（或明确分开计数，部署前在本侧复核时定死一种并写测试）。

预期收益：整机换代后隧道恢复从「≤30 分钟＋注册重试」缩到「开机约 3 分钟 timer 首跑＋注册重试」，公网 530 窗口同步缩短。

## 改动 2：修 health.sh 的 15 分钟注册窗口假阴性

现状：health.sh 用 journalctl --since "-15min" 找 "Registered tunnel connection"；长连接稳定超过 15 分钟后必然 FAIL。2026-10-20 夜间心跳至少 8 次记到 6/7 PASS 且注明同一假阴性（02:04、02:34、03:04、05:34、06:04、06:34、07:34、08:34 CST）。

改法：窗口改为自单元 ActiveEnterTimestamp 起查日志（systemctl show -p ActiveEnterTimestamp --value），判据为：自启动以来出现过 "Registered tunnel connection"，且其后没有该 connIndex 的断线/注销记录压过它；实现上可简化为「自启动以来有 Registered 记录」＋保留公网逐字节检查作端到端权威（公网检查已在脚本内且为最终判据）。若取不到 ActiveEnterTimestamp，回退到现在的 -15min 行为。

验收：造两个场景在本侧测——(a) 连接注册于 30 分钟前且稳定：health.sh 应 7/7 PASS；(b) 单元刚重启且尚未注册：该项仍应 FAIL。

## 不动的部分

- plugin.conf、relay.py、隧道凭据、Pinggy 链路均不改。
- HEARTBEAT 的隧道自愈条保留作第二层兜底（channel-restore 是第一层）。
- region1/region2 审批已均为 decided:allow_always（#38/#37），与本规格无关。
