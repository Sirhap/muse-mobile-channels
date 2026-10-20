#!/usr/bin/env python3
"""Cloudflare tunnel edge relay (plugin v2).

Forwards cloudflared edge connections through the explicit HTTP egress
proxy (CONNECT), because direct TCP to Cloudflare edge IPs is hijacked
in this environment. cloudflared is started with --edge 127.0.0.1:17844
and --edge 127.0.0.1:17845, so its two edge connections land on the
forced-host listeners below, which CONNECT through the proxy (address
and credentials from the https_proxy env var, never logged) to
region1/region2.v2.argotunnel.com:7844 and splice bytes both ways; the
TLS session is carried end to end.

A legacy listener on 7844 accepts connections addressed to the edge
fake IPs. Unlike v1 it keeps NO hardcoded IP table (the fake IPs change
with every environment generation and a stale table silently broke the
relay twice): region hostnames are re-resolved on demand and cached
for 60 seconds."""
import base64
import os
import select
import socket
import sys
import threading
import time
import urllib.parse

REGION_HOSTS = ("region1.v2.argotunnel.com", "region2.v2.argotunnel.com")
_ip_cache = {"ts": 0.0, "map": {}}


def ip_to_host():
    """Fake-IP -> region hostname, resolved fresh (60s cache)."""
    now = time.time()
    if now - _ip_cache["ts"] > 60:
        mapping = {}
        for host in REGION_HOSTS:
            try:
                mapping[socket.gethostbyname(host)] = host
            except OSError:
                pass
        if mapping:
            _ip_cache.update(ts=now, map=mapping)
    return _ip_cache["map"]


LISTEN_PORT = 7844


def proxy_target():
    u = urllib.parse.urlparse(os.environ.get("https_proxy") or os.environ.get("HTTPS_PROXY") or "")
    auth = None
    if u.username:
        raw = f"{urllib.parse.unquote(u.username)}:{urllib.parse.unquote(u.password or '')}"
        auth = base64.b64encode(raw.encode()).decode()
    return u.hostname, u.port or 3128, auth


def connect_via_proxy(hostname):
    phost, pport, auth = proxy_target()
    upstream = socket.create_connection((phost, pport), timeout=20)
    lines = [
        f"CONNECT {hostname}:7844 HTTP/1.1",
        f"Host: {hostname}:7844",
    ]
    if auth:
        lines.append(f"Proxy-Authorization: Basic {auth}")
    upstream.sendall(("\r\n".join(lines) + "\r\n\r\n").encode())
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = upstream.recv(4096)
        if not chunk:
            raise ConnectionError("proxy closed during CONNECT")
        buf += chunk
        if len(buf) > 65536:
            raise ConnectionError("proxy CONNECT response too large")
    status = buf.split(b"\r\n", 1)[0].decode(errors="ignore")
    if " 200" not in status:
        raise ConnectionError(f"proxy CONNECT failed: {status}")
    rest = buf.split(b"\r\n\r\n", 1)[1]
    if rest:
        # Should not happen for a tunnel endpoint, but keep bytes safe.
        raise ConnectionError("unexpected bytes after CONNECT response")
    return upstream


def splice(a, b):
    try:
        while True:
            r, _, _ = select.select([a, b], [], [], 300)
            if not r:
                break
            for src in r:
                data = src.recv(65536)
                if not data:
                    return
                (b if src is a else a).sendall(data)
    except OSError:
        pass
    finally:
        for s in (a, b):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                s.close()
            except OSError:
                pass


def handle(client, forced_host=None):
    try:
        dest_ip = client.getsockname()[0]
        hostname = forced_host or ip_to_host().get(dest_ip)
        if not hostname:
            print(f"relay: unknown local dest {dest_ip}, closing", flush=True)
            client.close()
            return
        upstream = connect_via_proxy(hostname)
        print(f"relay: connected {hostname} via proxy", flush=True)
        splice(client, upstream)
    except Exception as e:
        print(f"relay: error: {e}", flush=True)
        try:
            client.close()
        except OSError:
            pass


def serve(port, forced_host=None):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", port))
    srv.listen(128)
    print(f"relay: listening on 127.0.0.1:{port} -> {forced_host or 'by-dest-ip'}", flush=True)
    while True:
        client, _ = srv.accept()
        threading.Thread(target=handle, args=(client, forced_host), daemon=True).start()


def main():
    # Extra listeners used with iptables DNAT from the edge fake IPs:
    # .34 -> 17844 (region1), .35 -> 17845 (region2). Port encodes the target.
    threading.Thread(target=serve, args=(17844, "region1.v2.argotunnel.com"), daemon=True).start()
    threading.Thread(target=serve, args=(17845, "region2.v2.argotunnel.com"), daemon=True).start()
    serve(LISTEN_PORT)


if __name__ == "__main__":
    sys.exit(main())
