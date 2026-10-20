# Cloudflare 隧道插件 · 使用教程

把本机上的一个静态站点，通过 Cloudflare 命名隧道发布到一个固定公网域名。本机出站被强制走 HTTP 代理、直连 Cloudflare 边缘的 TCP 会被劫持，这套插件就是为绕开这个环境限制而写的：cloudflared 不直连边缘，而是先把连接交给本地中转（relay），中转再经出站代理用 CONNECT 把流量送到边缘，TLS 全程端到端。

当前默认配置：域名 `your.domain.example` → 本机 `127.0.0.1:8899` 上的站点目录。

## 一、组成与链路

```
公网访客
  → Cloudflare 边缘（域名 your.domain.example 的 CNAME 指向隧道）
  → 隧道（cloudflared-tunnel.service 运行 cloudflared，隧道名见 plugin.conf 的 TUNNEL_NAME）
  → cloudflared 以 --edge 127.0.0.1:17844/17845 把边缘连接交给本地中转
  → cloudflared-relay.service（relay.py）：经出站代理 CONNECT 到
    region1/region2.v2.argotunnel.com:7844，字节双向转发
  → intro-site.service：python http.server 托管站点目录（127.0.0.1:8899）
备份链路：pinggy-tunnel.service（Pinggy 临时隧道，网址会变，应急用）
```

四个 systemd 服务：`intro-site`、`cloudflared-relay`、`cloudflared-tunnel`、`pinggy-tunnel`（可选）。全部资产都在本插件目录 + `~/.cloudflared/` + `~/.config/cloudflared-relay.env`，不在系统盘，所以整机换代后可一键恢复（见第四节）。

## 二、首次安装

前置条件（一次性）：

1. cloudflared 已安装（本机在 `/usr/local/bin/cloudflared`）。
2. 已在 Cloudflare 侧建好命名隧道并配好 DNS，`~/.cloudflared/` 下有隧道凭据 JSON、`cert.pem`，且存在 `~/.cloudflared/config-<隧道名>.yml`（ingress 把域名指到 `http://127.0.0.1:<本地端口>`）。格式样例见包内 `config.example.yml`。本机现有隧道已配好（名字见 plugin.conf 的 TUNNEL_NAME），这步无需重做；只有换账号/换域名时才需要重建。
3. 出站代理地址（含凭据）已写入 `~/.config/cloudflared-relay.env`（权限 600，键名 `HTTPS_PROXY` / `https_proxy`）。中转从这里读代理，凭据不进代码、不进日志。
4. 若出站守卫弹出审批（中转程序 relay.py 连 region1/region2 的 7844 端口），选「永久批准」。region1 的已批（#38）；region2 的已设预授权自动批，弹出会自动处理并通知你。

安装（幂等，可重复执行）：

```bash
cd ~/workspace/cloudflare-tunnel-plugin
./install.sh
```

它会按 `plugin.conf` 把四个单元渲染安装到 /etc/systemd/system、daemon-reload、enable 并启动，最后自动跑一遍健康检查。缺凭据或缺隧道配置时它会明确报错并停下，不会装一半。

## 三、改配置

所有可调项只在一处：包内 `plugin.conf`。

| 配置项 | 含义 | 当前值 |
|---|---|---|
| TUNNEL_NAME | Cloudflare 隧道名 | your-tunnel |
| HOSTNAME | 对外域名 | your.domain.example |
| LOCAL_PORT | 本地站点端口 | 8899 |
| SITE_DIR | 站点文件目录 | ~/workspace/cf-intro-site-2026-10-11 |
| ENABLE_PINGGY | 是否启用 Pinggy 备份隧道 | 1 |

改完配置（或换了 SITE_DIR 里的页面内容之外的任何参数）后，重新执行 `./install.sh` 生效。只改页面内容不用做任何事，http.server 直接读文件。

换域名/换隧道的完整动作：Cloudflare 侧新建隧道并 route dns → 按 config.example.yml 写新的 `~/.cloudflared/config-<新隧道名>.yml` → 改 plugin.conf 的 TUNNEL_NAME 与 HOSTNAME → `./install.sh`。

## 四、自愈与整机换代恢复

- 服务级：四个单元都是 Restart=always，进程死掉会被自动拉起。隧道断线后 cloudflared 会持续重试；经代理建 CONNECT 到 7844 比较慢，注册成功可能要等几分钟，这是正常现象，不是故障。
- 机器级：整机换代会抹掉 /etc/systemd/system 里的单元（发生过两次）。心跳巡检（HEARTBEAT.md）里已挂自愈条：发现单元缺失或未运行会自动执行本包的 `install.sh` 恢复并跑健康检查，恢复或失败都会通知你。
- 手动恢复就是一条命令：`./install.sh`。

## 五、健康检查

```bash
./health.sh
```

七项检查，全部 PASS 才算健康，退出码 0：

1. 四个单元均为 active；
2. 本地站点在 LOCAL_PORT 上有响应；
3. 最近 15 分钟内日志里有「Registered tunnel connection」（隧道已注册到边缘）；
4. 公网打开 `https://<HOSTNAME>/` 返回 200，且内容与 SITE_DIR/index.html 逐字节一致（比对 sha256；本机经代理测公网偶有截断，脚本会自动重试多次，任一次字节一致即通过）。

## 六、常见问题排查

| 现象 | 原因与处理 |
|---|---|
| 公网打开是 530 | 隧道没连上边缘。先跑 `./health.sh` 看卡在哪层；单元不在就 `./install.sh`；单元在但未注册，等几分钟让它熬过代理慢响应，还不行看 `journalctl -u cloudflared-tunnel.service` |
| health 报「no registered tunnel connection」但公网正常 | 检查的是最近 15 分钟日志窗口，连接是更早注册且一直没断时也会这样，属误报性提示；以公网字节比对那项为准 |
| relay 日志反复 `error: timed out` | 代理建 CONNECT 到 7844 慢/早期尝试超时，属已知脾气，持续重试后会通；若数小时不通，检查 `~/.config/cloudflared-relay.env` 里的代理是否还有效 |
| 站点内容更新了公网没变 | 确认改的是 SITE_DIR 里的文件；确认无误后多半是缓存，强刷或等边缘缓存过期 |
| 出站审批又弹了 relay.py | 同程序换端点（region1/region2）会各弹一次，批「永久」即可；region2 的已预授权自动批 |
| Pinggy 网址变了 | 免费匿名会话约 60 分钟到期自动重连并换址，属预期；当前网址查 `grep -aoE "https://[a-zA-Z0-9.-]*(pinggy\.net\|pinggy-free\.link)" /tmp/tunnel-pinggy-service.log \| tail -2` |

手动看日志：`journalctl -u cloudflared-tunnel.service -f`（隧道）、`journalctl -u cloudflared-relay.service -f`（中转）。

## 七、卸载

```bash
./uninstall.sh
```

停用并删除四个 systemd 单元；隧道凭据、`~/.cloudflared` 配置和本插件包都保留，日后 `./install.sh` 即可装回。

## 八、安全说明

- 代理凭据只在 `~/.config/cloudflared-relay.env`（600 权限）里，安装脚本在该文件缺失时从当前环境生成它，绝不把凭据写进单元文件或日志。
- 隧道凭据 JSON 与 cert.pem 在 `~/.cloudflared/`，安装脚本只检查存在性、从不生成或打印。
- 中转只监听 127.0.0.1（17844/17845/7844），不对局域网暴露。
- relay.py 不保存任何边缘 IP 表：region 域名在连接时动态解析（60 秒缓存）。本机环境的边缘假 IP 每代都变，写死 IP 表曾让这条链路坏过两次，插件版已从结构上根除。
