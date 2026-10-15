"""Inbound media body handling shared by the Weixin and WeCom gateways.

The download writes the ciphertext to a temp file as chunks arrive and
stops once ``INBOUND_MEDIA_MAX_BYTES`` is crossed, so a large file is
never held as one ``bytes`` object. Decryption is block-oriented for
the same reason: AES-ECB and AES-CBC only need a block at a time, and
the PKCS#7 tail is the only plaintext kept back. Outbound upload,
chunking, and compression stay in the gateways.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any, BinaryIO

from channel_common import media_filename

# Ciphertext cap for one inbound media item. Bodies above this are
# logged and skipped; the inbox row is still written, without local_path.
# 1 GiB replaces the old hardcoded 25 MiB check.
INBOUND_MEDIA_MAX_BYTES = 1 * 1024 * 1024 * 1024

# Read and write size for the streaming download and the streaming decrypt.
# Large enough that the loop is not syscall-bound, small enough that one
# chunk is not the whole object.
INBOUND_MEDIA_CHUNK_BYTES = 1024 * 1024

# sniff_ext on both gateways inspects at most the first 12 bytes.
# 32 leaves that prefix intact for a short file and for a long one.
SNIFF_HEADER_BYTES = 32


class InboundMediaTooLarge(Exception):
    """Raised when an inbound body exceeds ``INBOUND_MEDIA_MAX_BYTES``.

    ``nbytes`` is what the gateway logs: the Content-Length when that
    header is already over the cap, otherwise the number of body bytes
    seen when the running total crossed it. The partial file is gone.
    """

    def __init__(self, nbytes: int) -> None:
        self.nbytes = int(nbytes)
        super().__init__(f"inbound media too large ({self.nbytes} bytes)")


def content_length_bytes(headers: object) -> int | None:
    """Return Content-Length as an int, or None when it is absent or not an integer."""
    if headers is None or not hasattr(headers, "get"):
        return None
    raw = headers.get("content-length")
    if raw is None:
        return None
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return None
    if value < 0:
        return None
    return value


def remove_file_quietly(path: Path) -> None:
    """Delete a temp file if it is still on disk. Cleanup must not mask the real error."""
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def inbound_temp_path(directory: Path, role: str) -> Path:
    """A temp path inside ``directory`` that ``media_filename`` will not produce.

    The final name is ``{msgid}-{index}.{ext}``. This name starts with a
    dot and has no ``-{index}`` segment, so the publish step cannot
    replace the temp with itself.
    """
    if role not in ("cipher", "plain"):
        raise ValueError(f"unknown inbound temp role: {role}")
    return directory / f".inbound-{uuid.uuid4().hex}.{role}"


async def write_bounded_stream(
    chunks: AsyncIterator[bytes],
    dest: Path,
    max_bytes: int,
) -> int:
    """Write ``chunks`` to ``dest`` and stop once they pass ``max_bytes``.

    Returns the number of bytes kept. A chunk that would pass the cap
    is not written. ``dest`` is removed and ``InboundMediaTooLarge`` is
    raised with the bytes seen, including that chunk. No further chunks
    are pulled. Any other error also removes ``dest``.
    """
    written = 0
    try:
        with dest.open("wb") as handle:
            async for chunk in chunks:
                if not chunk:
                    continue
                seen = written + len(chunk)
                if seen > max_bytes:
                    raise InboundMediaTooLarge(seen)
                handle.write(chunk)
                written = seen
    except Exception:
        remove_file_quietly(dest)
        raise
    return written


async def save_inbound_body(response: Any, dest: Path, max_bytes: int) -> int:
    """Stream one HTTP response body onto ``dest``, capping at ``max_bytes``.

    ``response`` is the object from ``async with client.stream(...)``:
    ``raise_for_status()``, ``headers``, and ``aiter_bytes(chunk_size)``.
    A Content-Length already over the cap does not read the body. A
    missing or smaller length is not trusted; the bytes ``aiter_bytes``
    yields (the same decoded body ``Response.content`` used to be) are
    counted, and the write stops as soon as they pass the cap.
    """
    response.raise_for_status()
    declared = content_length_bytes(getattr(response, "headers", None))
    if declared is not None and declared > max_bytes:
        raise InboundMediaTooLarge(declared)
    return await write_bounded_stream(
        response.aiter_bytes(INBOUND_MEDIA_CHUNK_BYTES),
        dest,
        max_bytes,
    )


def _write_holding_tail(target: BinaryIO, pending: bytes, plain: bytes, hold: int) -> bytes:
    """Append ``plain`` and write everything except the last ``hold`` bytes."""
    buffered = pending + plain
    if len(buffered) <= hold:
        return buffered
    target.write(buffered[:-hold])
    return buffered[-hold:]


def stream_pkcs7_decrypt_to_file(
    src: Path,
    dst: Path,
    update: Callable[[bytes], bytes],
    finalize: Callable[[], bytes],
    max_pad: int,
    read_size: int = INBOUND_MEDIA_CHUNK_BYTES,
) -> None:
    """Decrypt ``src`` onto ``dst`` without holding the whole plaintext.

    ``update`` and ``finalize`` are one cipher decryptor, already seeded
    with its IV. Output is written as it arrives except the last
    ``max_pad`` bytes, which stay buffered so the PKCS#7 trailer can be
    checked. The predicate matches the in-memory decryptors: an empty
    decrypt is rejected, and the pad byte must repeat exactly ``pad``
    times with ``1 <= pad <= max_pad``. On any error ``dst`` is removed.

    ``max_pad`` is 16 for Weixin AES-ECB and 32 for WeCom AES-CBC.
    """
    if read_size < 1:
        raise ValueError("read_size must be positive")
    if max_pad < 1:
        raise ValueError("max_pad must be positive")
    pending = b""
    produced = 0
    try:
        with src.open("rb") as source, dst.open("wb") as target:
            while True:
                blob = source.read(read_size)
                if not blob:
                    break
                plain = update(blob)
                produced += len(plain)
                pending = _write_holding_tail(target, pending, plain, max_pad)
            tail = finalize()
            produced += len(tail)
            pending = pending + tail
            if produced == 0 or not pending:
                raise ValueError("empty plaintext")
            pad = pending[-1]
            if (
                pad < 1
                or pad > max_pad
                or pad > produced
                or pending[-pad:] != bytes([pad]) * pad
            ):
                raise ValueError(f"bad PKCS#7 padding: {pad}")
            target.write(pending[:-pad])
    except Exception:
        remove_file_quietly(dst)
        raise


def publish_inbound_plaintext(
    plain_path: Path,
    msgid: str,
    index: int,
    sniff_ext: Callable[[bytes], str],
) -> Path:
    """Move decrypted plaintext to ``media_filename`` for its sniffed extension.

    ``sniff_ext`` receives only the leading bytes. Both gateways' sniffers
    look at most 12 bytes in, so this name matches a sniff of the whole file.
    """
    with plain_path.open("rb") as handle:
        header = handle.read(SNIFF_HEADER_BYTES)
    destination = plain_path.parent / media_filename(msgid, index, sniff_ext(header))
    plain_path.replace(destination)
    return destination
