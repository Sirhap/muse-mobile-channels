# HEARTBEAT — systemd units wiped by VM replacement

Decision for open-issues P3#10 (2026-10-09): keep the dual insurance, and add a read-only probe that does not live in the wiped units. The probe reports. It does not install, enable, start, or restart anything, and it does not change either gateway.

下文是巡检清单。worker 用的 `heartbeat.py`（批次保活，60 秒一拍）是另一回事，不能拿它代替这里的巡检。

## 双保险（接受的现状）

VM 替换会清掉 `/etc/systemd/system` 里的 unit，工作区副本还在。`channel-restore/restore.sh` 会从工作区把 unit 装回去，但它自己是被 `channel-restore.timer` 每 5 分钟调起来的。定时器文件也在 `/etc/systemd/system`，替换之后定时器本身不在，自愈不会自己回来，直到有人在 VM 上把 `restore.sh` 跑一次。

两条保险：

1. **定时器。** `channel-restore.timer`（`OnBootSec=3min`，`OnUnitActiveSec=5min`，`Persistent=true`）调用 `restore.sh`。健康时什么都不改，最后一行是 `channel-restore: all gateways healthy`。
2. **心跳巡检。** 不依赖这个定时器的一次检查：看 timer 还在不在，不在就跑恢复。2026-10-09 17:04 当天第二次抓住缺失的 `channel-restore.timer`，就是这条巡检，按本文件恢复的。

两条都在 VM 上。定时器被抹掉之后，到下一次巡检之前，渠道可以一直掉线而无人知。这是接受的窗口，不在本仓库里改 VM 镜像。

## VM 外的第三层

`channel-restore/liveness-probe.sh` 只读。从 VM 外面用 SSH 跑它，调用方不在被替换的机器上，所以这次抹掉带不走探测本身。探测失败不会自动修；修仍然是下面的 `restore.sh`。

VM 上也可以直接跑同一支脚本，当作心跳巡检的检查部分。

## 哪些 unit 要紧

生产路径以 `MUSE_HOME=/home/hatch` 为准。`restore.sh` 安装并维持这 6 个 unit。

| unit | 工作区副本 | 健康时 |
| --- | --- | --- |
| `channel-restore.timer` | `channel-restore/channel-restore.timer` | `loaded` + `enabled` + `active`。被抹掉时首先表现为 not-found |
| `channel-restore.service` | `channel-restore/channel-restore.service` | 已加载的 oneshot，两次运行之间是 inactive，这是正常的 |
| `weixin-bot.service` | `weixin-bot/weixin-bot.service` | `active` |
| `wecom-bot.service` | `wecom-bot/wecom-bot.service` | `active` |
| `native-bridge.service` | `native-bridge/native-bridge.service` | `active`，且 `native-bridge/status.json` 的 `ts` 不超过 180 秒（桥大约每 10 秒刷新；`restore.sh` 用同一阈值判断卡死） |
| `approval-relay.service` | VM 上的 `approval-relay/approval-relay.service`（ healer 读这个路径；文件不在本 git 树里） | `active` |

## 抹掉时长什么样

- `systemctl show channel-restore.timer` 的 `LoadState=not-found`（或 `is-active` 不是 `active`）。这是 2026-10-09 两次巡检看到的症状。
- 工作区里的 `channel-restore.timer` 和 `restore.sh` 还在。副本在、`/etc` 里没有，就是这次故障，不是仓库丢了。
- 两个网关 unit 变成 not-found 或 inactive，通道停止。
- 日志里不再出现 `channel-restore: all gateways healthy`，因为没有定时器去跑它。
- 网关 `state/status.json` 的 `updated_at` 在连接稳定、没有出站时本来就不会每分钟刷新（微信长轮询、企微心跳都不写这个文件）。所以单凭文件变旧不能判断网关死了。死了的样子是 unit 不 active，或者 `connected` 不是 true / `state` 不是 `connected` / `last_error` 非空。桥的 `ts` 才用 180 秒新鲜度。

## 巡检（在 VM 上）

```sh
/home/hatch/workspace/channel-restore/liveness-probe.sh
```

退出码 0 是通过。退出码 1 是有检查失败，按下一节恢复。退出码 2 是探测自己没查成（systemd 总线不在、`MUSE_HOME` 不对、SSH 失败），不要把它当成 unit 被删掉。

没有这支脚本时，等价的手工检查：

```sh
systemctl show -p LoadState -p ActiveState -p UnitFileState channel-restore.timer
systemctl is-active weixin-bot wecom-bot native-bridge approval-relay
```

`channel-restore.timer` 不是 `loaded/enabled/active`，就进入恢复。网关 unit 不 active 也进入恢复（定时器还在的话，5 分钟内它自己会拉起来；定时器不在就必须先恢复）。

## 恢复

只跑这一条。它会装回包括自己定时器在内的 unit，幂等，健康时不改文件。

```sh
sudo MUSE_HOME=/home/hatch /home/hatch/workspace/channel-restore/restore.sh
```

然后确认：

```sh
systemctl is-active channel-restore.timer
/home/hatch/workspace/channel-restore/liveness-probe.sh
```

期望定时器是 `active`，`restore.sh` 最后一行是 `channel-restore: all gateways healthy`，探测退出码 0。不要手抄 unit 到 `/etc/systemd/system`。

看最近一次自愈日志：

```sh
journalctl -u channel-restore.service -n 40 --no-pager
```

## 从 VM 外跑探测

探测机上要有这支脚本（仓库副本即可）。目标机要有 bash、systemd、python3，以及上面的工作区路径。SSH 使用 `BatchMode`，避免卡在密码提示。

```sh
channel-restore/liveness-probe.sh --ssh hatch@<vm-ssh-target>
```

可选：`--muse-home /home/hatch`（默认即此），`--bridge-max-age 180`。

建议在 VM 以外每 10 分钟跑一次。失败时告警，再登录 VM 执行上面的 `restore.sh`。不要把探测装成 VM 里的 systemd timer：那个 timer 会和 `channel-restore.timer` 一起被替换清掉。

退出码：0 健康，1 有失败项（timer 缺失或网关不健康），2 没查成。标准输出一行一项，`OK` 或 `FAIL`，最后一行是 `liveness-probe: OK` 或 `liveness-probe: FAIL <n>`。

网关项失败的含义：`status.json` 缺失、坏了、没有 `updated_at`，或者 `connected` 不为 true，或者 `state` 不是 `connected`，或者 `last_error` 非空。年龄会打印出来，但网关不因为文件旧而失败。桥的 `ts` 超过 `--bridge-max-age` 会失败。

## 仓库里怎么验证脚本

不连生产 VM：

```sh
bash channel-restore/liveness-probe.sh --self-test
```
