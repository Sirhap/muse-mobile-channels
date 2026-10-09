# Hatch 未跟踪文件处置计划（P3#11）

准备提交。本克隆的工作区是干净的，没有 hatch 上那约 21 条 `??` 的正文。下面按类别给默认处置，并留一张表，等 `/home/hatch/workspace` 的瓷单贴进来再逐条落定。

对应清单：`docs/open-issues-2026-10-09.md` 第 11 项。忽略规则已经写进仓库 `.gitignore`。本文件不授权在 hatch 上删除任何生产文件。

## 范围

处置对象只有两类 hatch 垃圾：

- `native-probe/` 里除已跟踪的 `gw2.py`、`README.md` 以外的探针脚本、结果归档、临时子目录。
- 文件名匹配 `outbound-probe-draft-*.md` 的草稿。此前点名的是 `channel-restore/outbound-probe-draft-2026-10-08.md`（2026-10-08 起未跟踪）。根目录或其他目录下的同名草稿同一规则。

不在范围内、本计划不动：

- `weixin-bot/`、`wecom-bot/`、`native-bridge/`（含已跟踪的 `native_bridge.py`、`config.json`、unit）、`hooks/`、`channel_common.py`。
- `channel-restore/` 里已跟踪的 `restore.sh`、`channel-restore.service`、`channel-restore.timer`。
- `native-bridge/probe_*.py` 早已忽略，维持原样，不要借这次清理去改桥。

## 三种处置

| 处置 | 含义 | 谁来做 |
|---|---|---|
| keep | 收进仓库。文件可复用，扫过没有令牌、用户标识或聊天正文，提交说明里写清它为什么要进版本库。 | 瓷单确认之后另一次提交。被忽略的路径要在 `.gitignore` 加一行否定，不能只靠 `git add -f` 把白名单弄脏。 |
| ignore | 文件留在 hatch 磁盘上，git 不再把它报成未跟踪。这是结果归档和尚未打开的草稿的默认。 | 本准备提交里的忽略规则。拉到 hatch 之后文件还在，只是 `git status` 不再列出。 |
| delete | 从 hatch 磁盘删掉这一个路径。结论已经写进 `native-probe/README.md` 或它指向的评估笔记，正文确认没有保留价值。 | 瓷单确认之后，在 hatch 上只 `rm` 表里写明 delete 的路径。不要在本准备提交里做。 |

拿不准就停在 ignore。ignore 不丢字节，后面改成 keep 或 delete 都还来得及。

## 类别默认

瓷单上的每一行先对上类别，再用最后一列改判。不要按通配符猜文件名；通配符只用来认类别。

| 类别 | 怎么认 | 默认 | 打开之后 |
|---|---|---|---|
| 可复用客户端 | 路径正好是 `native-probe/gw2.py` 或 `native-probe/README.md` | keep，已经在库里 | 保持跟踪 |
| native-probe 一次性脚本 | `native-probe/` 下其他 `*.py`（README 点过名的有 `probe1_latency.py`、`probe_battery.py`、`probe7*_stream_events.py`） | ignore | 只有 README 把它当成可复跑套件的一部分，并且扫过没有秘密，才改 keep。否则继续 ignore，或在结论已归档时改 delete |
| 探针结果归档 | `native-probe/` 下的 `*results*`、`*.txt`、`*.log`、`*.json`、`*.jsonl`、压缩包、`token*`、以及任何子目录里的转储（README 点过名的有 `probe7*_results.txt`） | ignore | 不提交。README 或 `~/workspace/muse-cli-protocol-eval-2026-10-07.md` 已经记下结论时，可以改 delete |
| outbound-probe 草稿 | 任意目录下的 `outbound-probe-draft-*.md` | ignore | 只在仍留着该文件的 VM 上打开。有用且无秘密则 keep；无用则 delete 这一个文件。不要在别的克隆里凭记忆重写 |
| 瓷单里对不上的路径 | 不属于上面两类 | hold | 先加一行再决定。若是网关、桥、hooks、自愈 unit，退出本计划，不当垃圾删 |

秘密扫描看这些：JWT、会话令牌、`credentials.env`、`.env`、API key、手机号、用户 id、聊天正文。有就不要 keep。结论若要留下，另写一份不含秘密的摘要。

## 瓷单（待填）

本表不是瓷单。前两行是此前报告过、本克隆里不存在的名字，用来占位。其余行等 hatch 导出之后粘贴，一行一个路径。约 21 条，多出来的继续往下加。

在 hatch 上、在拉取本忽略规则之前导出（拉取之后普通 `git status` 会把已忽略路径藏起来）：

```sh
cd /home/hatch/workspace
git status --porcelain=v1 -uall > ~/hatch-porcelain-p3-11.txt
```

`-uall` 会把未跟踪目录里的文件逐条展开，避免一条 `?? native-probe/` 把归档文件折起来。若规则已经拉下来，用下面这条把被忽略的路径找回来（`!!` 是已忽略）：

```sh
cd /home/hatch/workspace
git status --porcelain=v1 -uall --ignored > ~/hatch-porcelain-p3-11.txt
```

| # | 路径（原样粘贴，去掉 `??` / `!!` 前缀） | 类别 | 处置 | 确认 |
|---|---|---|---|---|
| 1 | `channel-restore/outbound-probe-draft-2026-10-08.md` | outbound-probe 草稿 | hold：在 hatch 上打开后再改 keep 或 delete | 未确认，本克隆没有这个文件 |
| 2 | `native-probe/` 下此前成组报告的探针归档（逐条粘贴，不要留这一行通配） | native-probe 脚本或结果归档 | ignore，打开并扫过秘密后再改 | 未确认 |
| 3 | | | | |
| 4 | | | | |
| 5 | | | | |
| 6 | | | | |
| 7 | | | | |
| 8 | | | | |
| 9 | | | | |
| 10 | | | | |
| 11 | | | | |
| 12 | | | | |
| 13 | | | | |
| 14 | | | | |
| 15 | | | | |
| 16 | | | | |
| 17 | | | | |
| 18 | | | | |
| 19 | | | | |
| 20 | | | | |
| 21 | | | | |

填完之后只数已经写上路径的行，和 `~/hatch-porcelain-p3-11.txt` 里的 `??`（或补救导出里的 `!!`）条数一致再动手。空行不算。对不上就停。

## 忽略规则

写在 `.gitignore` 里，和「只跟踪渠道代码」的白名单接在一起。`/*` 先挡住仓库根上未点名的文件；`native-probe/` 目录被重新放行之后，必须再忽略其内容，否则目录里的新文件仍会变成 `??`。否定行必须写在内容忽略之后，后一条匹配生效。

```
/native-probe/**
!/native-probe/gw2.py
!/native-probe/README.md

outbound-probe-draft-*.md
```

`outbound-probe-draft-*.md` 不带前导斜杠，`channel-restore/`、`docs/`、仓库根都算。根上的同名文件本来也会被 `/*` 挡住；写上这一条是为了在 `docs/*.md` 这类放行之后仍然挡住草稿。

这些规则不删除 hatch 上的文件。生产路径不匹配它们。已跟踪文件不受忽略影响，`gw2.py` 和 `README.md` 继续在索引里。

## 瓷单确认后怎么做

1. 按上一节导出瓷单，把每一条 `??` 填进表。导出文件留在 hatch 家目录，不要提交。
2. 拉取含本规则的 main（快进即可）。再跑一次 `git status --porcelain=v1 -uall`。两类垃圾应从 `??` 消失；磁盘上的文件还在。
3. 核对忽略打在垃圾上、打不在生产文件上。这两条都不要加 `-v`：`-v` 会把否定规则（以 `!` 开头、意思是放行）也打印出来，看起来像被忽略了。

```sh
cd /home/hatch/workspace
git check-ignore --no-index -- \
  native-probe/probe1_latency.py \
  native-probe/probe7_results.txt \
  channel-restore/outbound-probe-draft-2026-10-08.md \
  outbound-probe-draft-2026-10-08.md
echo "scratch_exit=$?"
git check-ignore --no-index -- \
  native-probe/gw2.py \
  native-probe/README.md \
  channel-restore/restore.sh \
  channel-restore/channel-restore.service \
  channel-restore/channel-restore.timer \
  native-bridge/native_bridge.py \
  native-bridge/config.json \
  native-bridge/native-bridge.service \
  weixin-bot/gateway.py \
  wecom-bot/gateway.py \
  hooks/scripts/weixin-inbox.sh \
  channel_common.py
echo "prod_exit=$?"
```

第一组四行都打印，`scratch_exit=0`。路径在这台机器上还不存在也没关系，`--no-index` 只查规则。第二组没有输出，`prod_exit=1`。第二组里真有输出就不要往下删，先改忽略规则。要看是哪一条规则打中垃圾，只对第一组加 `-v`。`--no-index` 是必要的：已跟踪文件默认不参加 `check-ignore`，不加它会把「gw2.py 没被忽略」误看成「规则没生效」。

4. 按表行动。
   - ignore：什么都不用删。规则已经生效。
   - keep：打开文件，做秘密扫描。通过之后在 `.gitignore` 里加一行否定，并且放在把它忽略的那一条之后（后一条匹配生效）。脚本例：`/native-probe/**` 之下写 `!/native-probe/probe_battery.py`。草稿例：`outbound-probe-draft-*.md` 之下写 `!/channel-restore/outbound-probe-draft-2026-10-08.md` 这种具体路径，不要放行整个 `outbound-probe-draft-*.md`。然后 `git add` 该文件和 `.gitignore`。
   - delete：只删除处置列写着 delete 的路径。下面是形状，不是待执行命令；把路径换成表里那一条，一次一条。

```sh
cd /home/hatch/workspace
rm -- path/from/the/table
```

用 `--`，不要加 `-r`，不要写目录。处置仍是 hold 或 ignore 的路径不要 rm。

不要执行的删除：

```sh
# 下面这些会伤到生产文件或已跟踪的客户端，不要跑
rm -rf native-probe channel-restore weixin-bot wecom-bot native-bridge hooks
rm native-probe/gw2.py native-probe/README.md channel-restore/restore.sh
```

5. 收尾核对：

```sh
cd /home/hatch/workspace
git status --porcelain=v1 -uall
git ls-files native-probe channel-restore native-bridge weixin-bot wecom-bot hooks
```

`??` 里不再出现已经填表的探针归档和 outbound-probe 草稿。`git ls-files` 仍列出 `native-probe/gw2.py`、`native-probe/README.md`、`channel-restore/restore.sh` 和两个 unit，以及网关、桥、hooks。然后把本表的「确认」列改成日期，并在 `docs/open-issues-2026-10-09.md` 第 11 项记下结果。表本身的更新跟那次 keep/delete 一起提交；若全部是 ignore 或 delete、仓库没有新文件，只提交填好的表和清单更新。
