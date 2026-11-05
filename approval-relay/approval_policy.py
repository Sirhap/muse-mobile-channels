#!/usr/bin/env python3
"""审批中转安全分级策略引擎 (Security Policy Engine for Approval Relay).

纯 Python 标准库实现，无任何外部第三方依赖。
根据请求的目标 Host、IP 地址/网段、HTTP 方法以及绑定的执行命令进行多维安全分级评估：
- Level 0 (低危免密自动放行 Auto Allow-Once): 只读方法 (GET/HEAD) + 白名单基础设施/包管理器 + 无高危系统命令
- Level 1 (普通人工审批 Normal Approval): 未命中白名单但无明显威胁的常规外部网络请求
- Level 2 (高危强审 High Risk Mobile Approval): 裸 IP、局域网/私有网段 IP、系统提权或破坏性命令，需手机强审并醒目标注 🚨 [高危审批]
- Level 3 (自动拦截 Auto Deny): 命中配置的安全黑名单 (域名或命令)
"""
import fnmatch
import ipaddress
import json
import os
import re
import shlex
import socket
import urllib.parse
from typing import Any, Dict, List, Optional, Set, Tuple


class RiskLevel:
    """安全风险等级枚举。"""
    LOW = "low"            # 低危 (可免密自动放行)
    NORMAL = "normal"      # 普通 (需正常人工审批)
    HIGH = "high"          # 高危 (需手机强审，红标预警)
    CRITICAL = "critical"  # 严重危险/恶意 (自动拒绝拦截)


class PolicyAction:
    """策略判定动作枚举。"""
    AUTO_ALLOW = "auto_allow"            # 自动放行 (平台核销为 allow_once)
    REQUIRE_APPROVAL = "require_approval"# 需人工审批
    AUTO_DENY = "auto_deny"              # 自动拦截 (平台核销为 deny)


class PolicyDecision:
    """策略判定决策结果对象。"""

    def __init__(
        self,
        action: str,
        risk_level: str,
        reason: str,
        rule_name: str = "",
        is_high_risk: Optional[bool] = None,
    ):
        self.action = action
        self.risk_level = risk_level
        self.reason = reason
        self.rule_name = rule_name
        if is_high_risk is not None:
            self.is_high_risk = is_high_risk
        else:
            self.is_high_risk = risk_level in (RiskLevel.HIGH, RiskLevel.CRITICAL)

    def to_dict(self) -> Dict[str, Any]:
        """将决策结果序列化为字典。"""
        return {
            "action": self.action,
            "risk_level": self.risk_level,
            "reason": self.reason,
            "rule_name": self.rule_name,
            "is_high_risk": self.is_high_risk,
        }

    def __repr__(self) -> str:
        return (
            f"<PolicyDecision action={self.action} risk={self.risk_level} "
            f"high_risk={self.is_high_risk} rule={self.rule_name} reason='{self.reason}'>"
        )


# 默认开箱即用的公共基础设施与包管理器安全白名单域名
DEFAULT_WHITELIST_HOSTS = {
    # Python 生态
    "pypi.org",
    "*.pypi.org",
    "files.pythonhosted.org",
    "pypi.python.org",
    # Node / NPM / Yarn 生态
    "registry.npmjs.org",
    "*.npmjs.org",
    "registry.yarnpkg.com",
    "yarnpkg.com",
    # Go 模块生态
    "proxy.golang.org",
    "sum.golang.org",
    # Rust Crates 生态
    "crates.io",
    "static.crates.io",
    "index.crates.io",
    # 代码仓库官方端点与官方分发源 (只读拉取，已坚决剔除用户可控代码/脚本托管源)
    "github.com",
    "api.github.com",
    "codeload.github.com",
}

# 默认低危只读 HTTP 方法
DEFAULT_SAFE_METHODS = {"GET", "HEAD"}

# 常见的私有网段与保留地址 CIDR (含 IPv4 / IPv6)
PRIVATE_IP_NETWORKS = [
    # IPv4 私有网段
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    # IPv4 本地回环与运营商 CGNAT
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("100.64.0.0/10"),
    # IPv4 链路本地与保留
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("0.0.0.0/8"),
    # IPv6 私有与回环
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("::/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
]

# 高危/提权系统二进制可执行文件名
DANGEROUS_SYSTEM_BINARIES = {
    # 提权命令
    "sudo", "su", "pkexec", "doas",
    # 文件权限与所有权修改
    "chmod", "chown", "chgrp",
    # 系统与服务管理
    "systemctl", "service", "init", "reboot", "shutdown", "poweroff", "halt",
    # 网络与防火墙配置
    "iptables", "nftables", "ufw", "firewalld", "ifconfig",
    # 底层磁盘破坏与扇区修改
    "dd", "fdisk", "parted", "shred",
}

# 复杂高危命令正则表达式规则列表
DANGEROUS_COMMAND_REGEXES = [
    # 管道直接执行远程下载脚本: curl ... | bash / curl ... | env FOO=1 bash / wget ... | /bin/sh / curl ... | python3 等
    re.compile(
        r"(?:curl|wget)\s+[^|&;\n]+\|&?\s*"
        r"(?:(?:/[^\s|&;]+/)?(?:sudo|env|nohup|exec)\s+(?:-[a-zA-Z0-9_.-]+(?:\s+\S+)?\s+|[a-zA-Z_][a-zA-Z0-9_]*=\S*\s+)*)*"
        r"(?:(?:/[^\s|&;]+/)?)(?:(?:ba|z|da|k|a)?sh|python\d*(?:\.\d+)?|perl|ruby|node|php|pwsh)\b",
        re.IGNORECASE,
    ),
    # 管道传递混淆/解码内容至 Shell/解释器执行: base64 -d ... | bash / openssl ... | sh / xxd ... | sh 等
    re.compile(
        r"(?:base64\s+(?:-[a-zA-Z0-9_.-]+\s+)*-[a-zA-Z0-9_.-]*d|openssl\s+enc\s+-d|xxd\s+-r)\b[^|&;\n]*\|&?\s*"
        r"(?:(?:/[^\s|&;]+/)?(?:sudo|env|nohup|exec)\s+(?:-[a-zA-Z0-9_.-]+(?:\s+\S+)?\s+|[a-zA-Z_][a-zA-Z0-9_]*=\S*\s+)*)*"
        r"(?:(?:/[^\s|&;]+/)?)(?:(?:ba|z|da|k|a)?sh|python\d*(?:\.\d+)?|perl|ruby|node|php|pwsh)\b",
        re.IGNORECASE,
    ),
    # 进程替换方式执行远程下载内容: bash <(curl ...) / bash <(base64 -d ...)
    re.compile(r"(?:ba|z)?sh\s*<\s*\(\s*(?:curl|wget|base64)", re.IGNORECASE),
    # 反弹 Shell / 套接字直接外联
    re.compile(r"/dev/(?:tcp|udp)/", re.IGNORECASE),
    re.compile(r"(?:^|[\s;&|`$()])(?:nc|ncat|netcat)\s+.*-[a-zA-Z]*e", re.IGNORECASE),
    # 危险递归删除根目录或通配符: rm -rf / 或 /bin/rm -rf / 或 rm -fr ~ 等
    re.compile(r"(?:^|[\s;&|`$()])(?:/[^\s|&;]+/)?rm\s+(?:-[a-zA-Z]+\s+)*(?:-[a-zA-Z]*r[a-zA-Z]*)\b.*(?:\s+|/|\*|~|\$HOME)", re.IGNORECASE),
    # 文件系统格式化
    re.compile(r"(?:^|[\s;&|`$()])(?:/[^\s|&;]+/)?mkfs(?:\.\w+)?(?:$|[\s;&|`$()])", re.IGNORECASE),
    # 网络接口或路由修改
    re.compile(r"(?:^|[\s;&|`$()])(?:/[^\s|&;]+/)?ip\s+(?:link|addr|route|rule)\b", re.IGNORECASE),
]

# 明确包含代码推送或包发布的写意图命令 (即使底层发起 GET，也必须人工审批)
WRITE_INTENT_COMMAND_REGEXES = [
    re.compile(r"\bgit\s+push\b", re.IGNORECASE),
    re.compile(r"\bnpm\s+publish\b", re.IGNORECASE),
    re.compile(r"\bpnpm\s+publish\b", re.IGNORECASE),
    re.compile(r"\byarn\s+publish\b", re.IGNORECASE),
    re.compile(r"\bcargo\s+publish\b", re.IGNORECASE),
    re.compile(r"\btwine\s+upload\b", re.IGNORECASE),
    re.compile(r"\bdocker\s+push\b", re.IGNORECASE),
]


def _clean_host_port(target_str: str) -> str:
    """从目标字符串（可能包含协议、端口、IPv6括号、URL凭据）中提取干净的域名或 IP。"""
    s = str(target_str).strip()
    if not s:
        return ""

    # 0. 先对 URL 编码进行解码，防范形如 %40 的凭据欺骗绕过
    if "%" in s:
        try:
            s = urllib.parse.unquote(s)
        except Exception:
            pass

    # 1. 处理协议头
    if "://" in s:
        try:
            parsed = urllib.parse.urlsplit(s)
            if parsed.hostname:
                return parsed.hostname.strip("[] ").lower().rstrip(".")
        except Exception:
            pass
        s = s.split("://", 1)[1]

    # 2. 如果包含路径或参数分隔符，截取 authority 部分
    if "/" in s:
        if s.startswith("/"):
            return ""
        s = s.split("/", 1)[0]
    s = s.split("?", 1)[0].split("#", 1)[0]

    # 3. 修复 URL 凭据混淆与 Userinfo 欺骗 (如 raw.githubusercontent.com:evil@169.254.169.254)
    # 真正的目标主机在最后一个 '@' 之后
    if "@" in s:
        s = s.rsplit("@", 1)[1]

    # 4. 处理类似 [::1]:8080 或 [::1] 的带括号 IPv6
    if s.startswith("["):
        end_bracket = s.find("]")
        if end_bracket != -1:
            return s[1:end_bracket].strip().lower()

    # 5. 去除端口（如 host:port 或 1.2.3.4:80）
    if s.count(":") == 1:
        s = s.split(":")[0]

    return s.strip("[] ").lower().rstrip(".")


def extract_host(record: Any, what: str = "") -> str:
    """从审批记录或展示文本中提取目标 Host（去除端口和 IPv6 括号）。"""
    host = ""
    if isinstance(record, dict):
        raw_host = str(record.get("host") or "").strip()
        if raw_host:
            host = _clean_host_port(raw_host)
        if not host:
            path = str(record.get("path") or "").strip()
            if path:
                host = _clean_host_port(path)
        if not host and record.get("url"):
            host = _clean_host_port(str(record.get("url")))

    if not host and what:
        for line in str(what).splitlines():
            if line.startswith("目标："):
                target_str = line[len("目标："):].strip()
                for tok in target_str.split():
                    # 跳过常见协议与 HTTP 动词前缀
                    if tok.upper() in ("GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "PATCH", "OPTIONS", "TCP", "HTTP", "HTTPS"):
                        continue
                    cleaned = _clean_host_port(tok)
                    if cleaned and cleaned not in ("tcp", "http", "https"):
                        host = cleaned
                        break
                if host:
                    break

    return host


def parse_ip(host_str: str) -> Optional[Any]:
    """尝试将 host 字符串解析为 IPAddress 对象，若不是有效 IP 返回 None。

    支持 IPv4/IPv6 标准格式，以及整数 IP、八进制、十六进制等底层表示法。
    """
    if not host_str:
        return None
    cleaned = host_str.strip("[] ").lower().rstrip(".")
    # 1. 尝试直接标准 ip_address 解析
    try:
        return ipaddress.ip_address(cleaned)
    except ValueError:
        pass
    # 2. 尝试解析十进制整数、八进制、十六进制等表示法（如 2130706433, 0177.0.0.1）
    # 防范利用非常规进制绕过私有/内网 IP 检测进行 SSRF
    try:
        packed = socket.inet_aton(cleaned)
        return ipaddress.IPv4Address(packed)
    except (OSError, ValueError):
        pass
    return None


def is_private_ip(ip_obj: Any) -> bool:
    """判断 IP 是否属于内网私有网段、本地回环或保留网段。"""
    # 若为 IPv4 映射的 IPv6 地址（如 ::ffff:192.168.1.1 或 ::ffff:100.64.0.1），递归检查底层 IPv4
    ipv4_mapped = getattr(ip_obj, "ipv4_mapped", None)
    if ipv4_mapped is not None and is_private_ip(ipv4_mapped):
        return True

    if (ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local
            or ip_obj.is_reserved or ip_obj.is_multicast or ip_obj.is_unspecified):
        return True
    for network in PRIVATE_IP_NETWORKS:
        try:
            if ip_obj in network:
                return True
        except TypeError:
            continue
    return False


WILDCARD_DNS_DOMAINS = ("nip.io", "sslip.io", "xip.io")


def extract_ip_from_wildcard_domain(host_str: str) -> Optional[Any]:
    """尝试从泛解析 DNS 域名（如 *.nip.io, *.sslip.io, *.xip.io）中提取其内嵌的 IP 地址。

    支持点分十进制、短横线格式、8位十六进制 (如 7f000001.nip.io) 及内嵌 IPv6 格式。
    """
    if not host_str:
        return None
    cleaned = _clean_host_port(host_str)
    h = cleaned.strip().lower().rstrip(".")
    is_wildcard = any(h == dom or h.endswith("." + dom) for dom in WILDCARD_DNS_DOMAINS)
    if not is_wildcard:
        return None

    # 1. 匹配点分十进制 IPv4: 如 127.0.0.1.nip.io, app.192.168.1.1.nip.io
    m_dot = re.search(r"(?:^|\.)(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(?:\.|$)", h)
    if m_dot:
        ip = parse_ip(m_dot.group(1))
        if ip:
            return ip

    # 2. 匹配短横线分隔 IPv4: 如 127-0-0-1.sslip.io, customer.10-0-0-1.sslip.io
    m_dash = re.search(r"(?:^|[.-])(\d{1,3}-\d{1,3}-\d{1,3}-\d{1,3})(?:[.-]|$)", h)
    if m_dash:
        ip = parse_ip(m_dash.group(1).replace("-", "."))
        if ip:
            return ip

    # 3. 匹配 8 位十六进制表示的 IPv4: 如 7f000001.nip.io (127.0.0.1), a9fea9fe.nip.io (169.254.169.254)
    m_hex = re.search(r"(?:^|\.)([0-9a-fA-F]{8})(?:\.|$)", h)
    if m_hex:
        try:
            val = int(m_hex.group(1), 16)
            return ipaddress.IPv4Address(val)
        except (ValueError, ipaddress.AddressValueError):
            pass

    # 4. 匹配内嵌 IPv6 (sslip.io 格式，短横线代表冒号，双短横线代表 ::): 如 fe80--1.sslip.io, --1.sslip.io
    for dom in WILDCARD_DNS_DOMAINS:
        if h.endswith("." + dom):
            sub = h[:-len("." + dom)]
            labels = sub.split(".")
            for label in reversed(labels):
                if "-" in label or label.startswith("--"):
                    cand = label.replace("--", "::").replace("-", ":")
                    try:
                        return ipaddress.IPv6Address(cand)
                    except ValueError:
                        pass
            break

    return None


def match_domain_pattern(host: str, pattern: str) -> bool:
    """匹配域名（支持 *.domain.com 通配及精确域名匹配）。"""
    host = host.strip().lower().rstrip(".")
    pattern = pattern.strip().lower().rstrip(".")
    if not host or not pattern:
        return False
    if pattern == host:
        return True
    if pattern.startswith("*."):
        base = pattern[2:]
        if host == base or host.endswith("." + base):
            return True
    return False


def match_any_domain(host: str, patterns: Set[str]) -> bool:
    """检查 host 是否命中任一域名规则模式。"""
    for pat in patterns:
        if match_domain_pattern(host, pat):
            return True
    return False


# 常见 Shell 包装器程序
SHELL_WRAPPERS = {
    "sudo", "doas", "pkexec", "su", "env", "nohup", "time", "nice", "ionice", "exec",
    "xargs", "bash", "sh", "zsh", "dash", "ksh", "eval", "command", "builtin",
}

# 常见 Shell 与脚本解释器程序
SHELL_INTERPRETERS = {
    "bash", "sh", "zsh", "dash", "ksh",
    "python", "python3", "perl", "ruby", "node", "pwsh",
}

# Shell 结构与控制流关键字 (跳过这些关键字以获取紧随其后的真实命令)
SHELL_CONTROL_KEYWORDS = {
    "if", "then", "else", "elif", "fi", "while", "until", "for", "in", "do", "done",
    "case", "esac", "select", "{", "}", "!", "[[", "]]",
}

# 常见需要跳过其参数值的命令行选项
OPTIONS_WITH_ARGS = {
    "-u", "-g", "-p", "-o", "-e", "-C", "-t", "-S", "-I", "-n", "-L", "-s",
    "--user", "--group", "--unset", "--output", "--chdir", "--file",
}


def pad_shell_delimiters(s: str) -> str:
    """在未加引号的分隔符两端添加空格，确保分词器能正确识别复合命令边界。"""
    res = []
    in_single = False
    in_double = False
    escaped = False
    i = 0
    n = len(s)
    while i < n:
        ch = s[i]
        if escaped:
            res.append(ch)
            escaped = False
            i += 1
            continue
        if ch == "\\":
            escaped = True
            res.append(ch)
            i += 1
            continue
        if ch == "'" and not in_double:
            in_single = not in_single
            res.append(ch)
            i += 1
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            res.append(ch)
            i += 1
            continue
        if not in_single and not in_double:
            two = s[i:i+2]
            if two in ("&&", "||", "|&"):
                res.extend([" ", two, " "])
                i += 2
                continue
            if ch in (";", "|", "&", "\n", "(", ")"):
                res.extend([" ", ch, " "])
                i += 1
                continue
        res.append(ch)
        i += 1
    return "".join(res)


def extract_executables(cmd_line: str, _depth: int = 0) -> List[str]:
    """从命令行中深度解析提取所有可能被执行的主体可执行程序名称。

    特性：
    - 支持引号敏感的复合分隔符展开 (&&, ||, ;, |, &)；
    - 支持跳过 shell 控制流关键字 (then, do, if 等) 准确定位内嵌操作；
    - 支持 shell 包装器标志与参数跳过 (env -i, env --unset USER, xargs -I 等)；
    - 支持 sh -c / bash -c / zsh -c 及 eval 引号内部子命令的递归展开。
    """
    if not cmd_line or not str(cmd_line).strip() or _depth > 5:
        return []

    executables = []
    cmd_text = str(cmd_line).strip()

    # 1. 预先提取 $(...) 与反引号子命令替换
    for subshell in re.findall(r"\$\(([^()]+)\)", cmd_text):
        if subshell.strip():
            executables.extend(extract_executables(subshell, _depth=_depth + 1))
    for backtick in re.findall(r"`([^`]+)`", cmd_text):
        if backtick.strip():
            executables.extend(extract_executables(backtick, _depth=_depth + 1))

    # 2. 预先正则匹配 shell/解释器 -c 或 -e "..."（支持内层转义引号容错）
    shell_c_pattern = re.compile(
        r"(?:^|[\s;&|`$()])(?:/[a-zA-Z0-9_.-]+/)?(?:(?:ba|z|da|k)?sh|python\d*|perl|ruby|node)\s+(?:-[a-zA-Z0-9_.-]+(?:\s+\S+)?\s+)*-[a-zA-Z0-9_.-]*[ce]\s+(?:\"((?:[^\"\\]|\\.)*)\"|'((?:[^'\\]|\\.)*)')",
        re.IGNORECASE,
    )
    for m in shell_c_pattern.finditer(cmd_text):
        inner_cmd = m.group(1) or m.group(2) or ""
        inner_cmd = inner_cmd.replace('\\"', '"').replace("\\'", "'")
        if inner_cmd.strip():
            executables.extend(extract_executables(inner_cmd, _depth=_depth + 1))
            # 对解释器内联代码内部包含的嵌套字符串参数（如 os.system("chmod ...")）深度展开
            for quoted_lit in re.findall(r"""["']([^"']+)["']""", inner_cmd):
                if quoted_lit.strip():
                    executables.extend(extract_executables(quoted_lit, _depth=_depth + 1))

    # 3. 对命令未加引号的分隔符两端补空格并进行 POSIX 标准分词
    padded = pad_shell_delimiters(cmd_text)
    try:
        tokens = shlex.split(padded, posix=True)
    except Exception:
        tokens = padded.split()

    DELIMITERS = {";", "&&", "||", "|", "|&", "&", "\n", "(", ")"}

    i = 0
    in_command_start = True
    current_wrapper = None

    while i < len(tokens):
        raw_tok = tokens[i]
        i += 1

        # 遇到命令分隔符，重置命令边界
        if raw_tok in DELIMITERS:
            in_command_start = True
            current_wrapper = None
            continue

        # 遇到控制流关键字，重置并继续寻找实际命令
        if raw_tok in SHELL_CONTROL_KEYWORDS:
            in_command_start = True
            if raw_tok == "for":
                # 跳过 for 循环声明: for var in ... do
                while i < len(tokens) and tokens[i] not in DELIMITERS and tokens[i] != "do":
                    i += 1
            continue

        # 跳过 test 语法结构: [ condition ]
        if raw_tok in ("[", "[["):
            while i < len(tokens) and tokens[i] not in ("]", "]]") and tokens[i] not in DELIMITERS:
                i += 1
            if i < len(tokens) and tokens[i] in ("]", "]]"):
                i += 1
            in_command_start = True
            continue

        tok = raw_tok.strip("'\"()[]{}")
        if not tok:
            continue

        # 跳过形如 FOO=bar 的环境变量声明
        if "=" in tok and not tok.startswith("-"):
            continue

        # 跳过以 - 开头的选项标志
        if tok.startswith("-"):
            if current_wrapper in SHELL_INTERPRETERS and (
                tok in ("-c", "-e") or tok.endswith("c") or tok.endswith("e")
            ):
                if i < len(tokens):
                    sub_cmd_str = tokens[i]
                    i += 1
                    executables.extend(extract_executables(sub_cmd_str, _depth=_depth + 1))
                    for quoted_lit in re.findall(r"""["']([^"']+)["']""", sub_cmd_str):
                        if quoted_lit.strip():
                            executables.extend(extract_executables(quoted_lit, _depth=_depth + 1))
                    in_command_start = False
                    continue
            if tok in OPTIONS_WITH_ARGS:
                if i < len(tokens):
                    i += 1
            continue

        prog = tok.split("/")[-1].lstrip("\\").lower()
        if not prog:
            continue

        executables.append(prog)

        if prog in SHELL_INTERPRETERS:
            current_wrapper = prog
            in_command_start = False
            continue

        if prog == "eval":
            current_wrapper = "eval"
            remaining = " ".join(tokens[i:])
            if remaining.strip():
                executables.extend(extract_executables(remaining, _depth=_depth + 1))
            break

        if prog in SHELL_WRAPPERS:
            current_wrapper = prog
            in_command_start = False
            continue

        in_command_start = False
        while i < len(tokens) and tokens[i] not in DELIMITERS and tokens[i] not in SHELL_CONTROL_KEYWORDS:
            i += 1

    seen = set()
    result = []
    for p in executables:
        if p not in seen:
            seen.add(p)
            result.append(p)
    return result


def check_dangerous_command(cmd_line: str) -> Tuple[bool, str]:
    """检查命令是否包含系统提权、高危修改或破坏性操作。

    返回:
        (is_dangerous, matched_reason)
    """
    if not cmd_line or not str(cmd_line).strip():
        return False, ""

    cmd_text = str(cmd_line).strip()

    # 1. 检查已知高危系统二进制命令
    progs = extract_executables(cmd_text)
    for p in progs:
        if p in DANGEROUS_SYSTEM_BINARIES:
            return True, f"命令包含高危系统或提权操作 [{p}]"

    # 2. 检查复杂组合危险正则
    for reg in DANGEROUS_COMMAND_REGEXES:
        m = reg.search(cmd_text)
        if m:
            return True, f"命中危险命令特征 [{m.group(0).strip()}]"

    return False, ""


def check_write_intent_command(cmd_line: str) -> Tuple[bool, str]:
    """检查命令是否包含代码推送、包发布等明确写操作意图。"""
    if not cmd_line or not str(cmd_line).strip():
        return False, ""
    cmd_text = str(cmd_line).strip()
    for reg in WRITE_INTENT_COMMAND_REGEXES:
        m = reg.search(cmd_text)
        if m:
            return True, f"命令包含代码推送或包发布操作 [{m.group(0).strip()}]"
    return False, ""


class SecurityPolicyEngine:
    """审批中转安全分级策略引擎。

    纯标准库实现，支持自定义配置与开箱即用默认规则。
    """

    def __init__(
        self,
        whitelist_hosts: Optional[Set[str]] = None,
        blacklist_hosts: Optional[Set[str]] = None,
        blacklist_commands: Optional[List[str]] = None,
        dangerous_binaries: Optional[Set[str]] = None,
        safe_methods: Optional[Set[str]] = None,
        allow_private_ip: bool = False,
        allow_bare_ip: bool = False,
    ):
        self.whitelist_hosts = set(
            h.strip().lower() for h in (
                whitelist_hosts if whitelist_hosts is not None else DEFAULT_WHITELIST_HOSTS
            ) if h.strip()
        )
        self.blacklist_hosts = set(
            h.strip().lower() for h in (
                blacklist_hosts if blacklist_hosts is not None else []
            ) if h.strip()
        )
        self.blacklist_commands = [
            c.strip() for c in (
                blacklist_commands if blacklist_commands is not None else []
            ) if c.strip()
        ]
        self.safe_methods = set(
            m.strip().upper() for m in (
                safe_methods if safe_methods is not None else DEFAULT_SAFE_METHODS
            ) if m.strip()
        )
        self.allow_private_ip = allow_private_ip
        self.allow_bare_ip = allow_bare_ip

    def evaluate(self, record: Any, what: str = "") -> PolicyDecision:
        """评估单条审批记录的安全风险级别及动作。

        参数:
            record: dict，审批记录数据
            what: str，可选，用于在 record 缺失时从展示文本中补充信息
        返回:
            PolicyDecision 决策对象
        """
        if not isinstance(record, dict):
            record = {}

        # 提取关键字段
        host = extract_host(record, what=what)
        raw_cmd = (
            record.get("registered_command")
            or record.get("observed_command")
            or ""
        )
        if not raw_cmd and what:
            for line in str(what).splitlines():
                if line.startswith("命令："):
                    raw_cmd = line[len("命令："):].strip()
                    break

        method = str(record.get("method") or "").strip().upper()
        if not method and what:
            for line in str(what).splitlines():
                if line.startswith("目标："):
                    for tok in line[len("目标："):].strip().split():
                        if tok.upper() in ("GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "PATCH", "OPTIONS"):
                            method = tok.upper()
                            break

        # =========================================================================
        # 1. Level 3 自动拦截检查 (Auto Deny)
        # =========================================================================
        if host and self.blacklist_hosts and match_any_domain(host, self.blacklist_hosts):
            return PolicyDecision(
                action=PolicyAction.AUTO_DENY,
                risk_level=RiskLevel.CRITICAL,
                reason=f"目标 Host [{host}] 命中安全黑名单",
                rule_name="blacklist_host",
            )

        if raw_cmd and self.blacklist_commands:
            for b_cmd in self.blacklist_commands:
                if b_cmd in raw_cmd or fnmatch.fnmatch(raw_cmd, b_cmd):
                    return PolicyDecision(
                        action=PolicyAction.AUTO_DENY,
                        risk_level=RiskLevel.CRITICAL,
                        reason=f"执行命令命中黑名单规则 [{b_cmd}]",
                        rule_name="blacklist_command",
                    )

        # =========================================================================
        # 2. Level 2 高危检查 (Require Mobile Approval 🚨)
        # =========================================================================
        # 2.1 高危系统/提权命令检查
        is_dang, dang_reason = check_dangerous_command(raw_cmd)
        if is_dang:
            return PolicyDecision(
                action=PolicyAction.REQUIRE_APPROVAL,
                risk_level=RiskLevel.HIGH,
                reason=dang_reason,
                rule_name="dangerous_command",
            )

        # 2.2 IP 地址与局域网本地主机安全检查 (私有网段、本地回环、保留域名与裸 IP)
        if (
            host in {"localhost", "ip6-localhost", "ip6-loopback", "localtest.me", "lvh.me", "vcap.me"}
            or host.endswith((
                ".localhost", ".local", ".internal", ".lan", ".home.arpa",
                ".localtest.me", ".lvh.me", ".vcap.me",
            ))
        ):
            return PolicyDecision(
                action=PolicyAction.REQUIRE_APPROVAL,
                risk_level=RiskLevel.HIGH,
                reason=f"目标为本地回环或内网域名 [{host}]，防范横向渗透与 SSRF",
                rule_name="private_ip",
            )

        # 检查泛解析 DNS 域名（如 *.nip.io, *.sslip.io）是否指向私网或回环 IP
        wildcard_ip = extract_ip_from_wildcard_domain(host)
        if wildcard_ip is not None and is_private_ip(wildcard_ip):
            return PolicyDecision(
                action=PolicyAction.REQUIRE_APPROVAL,
                risk_level=RiskLevel.HIGH,
                reason=f"目标泛解析域名 [{host}] 指向私网/回环 IP [{wildcard_ip}]，防范横向渗透与 SSRF",
                rule_name="private_ip",
            )

        ip_obj = parse_ip(host)
        if ip_obj is not None:
            if not self.allow_private_ip and is_private_ip(ip_obj):
                return PolicyDecision(
                    action=PolicyAction.REQUIRE_APPROVAL,
                    risk_level=RiskLevel.HIGH,
                    reason=f"目标为局域网/内网私有网段 IP [{ip_obj}]，防范横向渗透与 SSRF",
                    rule_name="private_ip",
                )
            if not self.allow_bare_ip:
                return PolicyDecision(
                    action=PolicyAction.REQUIRE_APPROVAL,
                    risk_level=RiskLevel.HIGH,
                    reason=f"目标为未知裸 IP 直连 [{ip_obj}]，需人工核验",
                    rule_name="bare_ip",
                )

        # 2.3 检查是否包含代码推送/发布等写操作
        is_write, write_reason = check_write_intent_command(raw_cmd)
        if is_write:
            return PolicyDecision(
                action=PolicyAction.REQUIRE_APPROVAL,
                risk_level=RiskLevel.NORMAL,
                reason=write_reason,
                rule_name="write_intent",
                is_high_risk=False,
            )

        # =========================================================================
        # 3. Level 0 低危免密自动放行检查 (Auto Allow-Once)
        # =========================================================================
        # 条件:
        # 1. 目标 Host 匹配白名单
        # 2. 请求方法为只读安全方法 (GET / HEAD)
        # 3. 绑定的 registered_command 不包含危险系统操作或系统提权命令 (已在前面通过)
        if host and match_any_domain(host, self.whitelist_hosts):
            if method in self.safe_methods:
                return PolicyDecision(
                    action=PolicyAction.AUTO_ALLOW,
                    risk_level=RiskLevel.LOW,
                    reason=f"只读操作 [{method}] 且目标匹配安全白名单 [{host}]，免密自动放行",
                    rule_name="whitelist_auto_allow",
                )

        # =========================================================================
        # 4. Level 1 常规人工审批 (Normal Approval)
        # =========================================================================
        return PolicyDecision(
            action=PolicyAction.REQUIRE_APPROVAL,
            risk_level=RiskLevel.NORMAL,
            reason=f"常规外部网络请求 [{method} {host}]，需人工确认",
            rule_name="normal_approval",
            is_high_risk=False,
        )


# 单例与工厂接口
_DEFAULT_POLICY_ENGINE = None


def get_default_policy_engine() -> SecurityPolicyEngine:
    """获取或初始化默认安全分级策略引擎单例。支持通过环境变量补充黑白名单。"""
    global _DEFAULT_POLICY_ENGINE
    if _DEFAULT_POLICY_ENGINE is None:
        extra_whitelist = set()
        env_wl = os.environ.get("APPROVAL_POLICY_WHITELIST")
        if env_wl:
            extra_whitelist.update(h.strip().lower() for h in env_wl.split(",") if h.strip())

        extra_blacklist = set()
        env_bl = os.environ.get("APPROVAL_POLICY_BLACKLIST")
        if env_bl:
            extra_blacklist.update(h.strip().lower() for h in env_bl.split(",") if h.strip())

        _DEFAULT_POLICY_ENGINE = SecurityPolicyEngine(
            whitelist_hosts=DEFAULT_WHITELIST_HOSTS | extra_whitelist,
            blacklist_hosts=extra_blacklist,
        )
    return _DEFAULT_POLICY_ENGINE


def evaluate_approval(record: Any, what: str = "") -> PolicyDecision:
    """便捷评估函数，使用默认策略引擎对审批记录进行安全评估。"""
    return get_default_policy_engine().evaluate(record, what=what)
