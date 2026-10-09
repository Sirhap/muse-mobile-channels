"""GW2: muse-cli Gateway + NoiseTransportFrame chunk reassembly.

Why this exists (proven in probe rounds 3-4, 2026-10-07):
nikships/muse-cli's Gateway._read_frame treats every WebSocket frame as one
complete NoiseTransportFrame.  Responses that the server splits into several
chunks (chunk_index / total_chunks in the protobuf) then fail protobuf
parsing ("DecodeError: Wire format was corrupt"), and the leftover fragments
poison subsequent calls on the same connection (failures cluster after large
responses, e.g. egress.approvals, and drain only after ~9 more calls).
GW2 reassembles chunks per chunk_id before parsing ServiceResponse, which
made every previously failing method work on first try (vm.health,
subagents.list, egress.*, voice.*, node.list, fs.stats, ...).

Dependency: the muse_cli package from the reference clone at
~/workspace/reference/muse-cli/src (override with env MUSE_CLI_SRC).
Requires: curl_cffi, noiseprotocol, protobuf (venv ~/muse-test-venv).

Token discipline: tokens arrive as JSON on stdin, are verified against a
sha256 checksum supplied by the caller, and are never written to disk or
printed.  See README.md.
"""
import hashlib
import json
import os
import sys

_SRC = os.environ.get(
    "MUSE_CLI_SRC",
    os.path.expanduser("~/workspace/reference/muse-cli/src"))
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from muse_cli.gateway import (  # noqa: E402
    Gateway,
    GatewayError,
    NoiseTransportFrame,
    ServiceFrame,
    ServiceResponse,
)

__all__ = ["GW2", "GatewayError", "canonical_token_json", "token_from_stdin"]


class GW2(Gateway):
    """Gateway with NoiseTransportFrame chunk reassembly."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._chunks = {}

    def _read_frame(self):
        while True:
            with self._recv_lock:
                data, _flags = self.ws.recv()
                pt = bytes(self.noise.decrypt(bytes(data)))
            ntf = NoiseTransportFrame()
            ntf.ParseFromString(pt)
            total = ntf.total_chunks or 1
            if total <= 1:
                payload = ntf.payload
            else:
                buf = self._chunks.setdefault(ntf.chunk_id, {})
                buf[ntf.chunk_index] = ntf.payload
                if len(buf) < total:
                    continue
                payload = b"".join(buf[i] for i in sorted(buf))
                del self._chunks[ntf.chunk_id]
            sr = ServiceResponse()
            sr.ParseFromString(payload)
            sf = ServiceFrame()
            sf.ParseFromString(sr.payload)
            return sf


def canonical_token_json(cfg):
    """Re-serialize a token dict exactly as the browser snippet produced it
    (JSON.stringify key order vm_id/access_token/hatch_token, no spaces), so
    the caller's sha256 checksum is comparable regardless of how the JSON was
    re-typed or re-ordered in transit."""
    return json.dumps(
        {"vm_id": cfg["vm_id"], "access_token": cfg["access_token"],
         "hatch_token": cfg["hatch_token"]},
        separators=(",", ":"))


def token_from_stdin(expected_checksum):
    """Read {vm_id, access_token, hatch_token} JSON from stdin and verify its
    sha256[:10] against expected_checksum (the value the browser snippet
    printed).  Exits with code 2 on mismatch WITHOUT connecting.  Returns the
    kwargs dict for GW2/Gateway."""
    cfg = json.loads(sys.stdin.read().strip())
    got = hashlib.sha256(canonical_token_json(cfg).encode()).hexdigest()[:10]
    if got != expected_checksum.strip().lower():
        print(f"CHECKSUM MISMATCH got={got} expected={expected_checksum} "
              "- transcription error, token NOT used", flush=True)
        sys.exit(2)
    print("checksum ok", flush=True)
    return dict(cookies="", vm_id=cfg["vm_id"],
                access_token=cfg["access_token"],
                hatch_token=cfg["hatch_token"])
