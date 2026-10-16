"""Inbound media body handling shared by the Weixin and WeCom gateways.

The download writes the ciphertext to a temp file as chunks arrive and
stops once ``INBOUND_MEDIA_MAX_BYTES`` is crossed, so a large file is
never held as one ``bytes`` object. A transport failure retries that
GET from byte 0, up to ``INBOUND_DOWNLOAD_MAX_ATTEMPTS``. Decryption is
block-oriented for the same reason: AES-ECB and AES-CBC only need a
block at a time, and the PKCS#7 tail is the only plaintext kept back.
Outbound upload, chunking, and compression stay in the gateways.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from pathlib import Path
from typing import Any, BinaryIO

import httpx

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

# A dropped CDN connection starts the GET over from byte 0. This is the
# first attempt plus two retries. The sleeps run after attempt 1 and
# attempt 2; the last failure is not followed by a sleep.
INBOUND_DOWNLOAD_MAX_ATTEMPTS = 3
INBOUND_DOWNLOAD_BACKOFF_SECONDS = (2.0, 5.0)


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


def _response_status(response: object) -> int | None:
    """Return ``response.status_code`` when it is an int."""
    status = getattr(response, "status_code", None)
    if isinstance(status, int):
        return status
    return None


def _status_code_from_exception(exc: BaseException) -> int | None:
    """Return an HTTP status carried on ``exc``, if the exception has one."""
    response = getattr(exc, "response", None)
    if response is None:
        return None
    return _response_status(response)


def _http_status_retryable(status: int) -> bool:
    """True for 408, 429, and 5xx. Other 4xx will not change on another GET."""
    return status == 408 or status == 429 or 500 <= status <= 599


def is_retryable_inbound_download_error(
    exc: BaseException,
    status_code: int | None = None,
) -> bool:
    """True when the download should start over from byte 0.

    Transport-class failures are httpx transport errors (including an
    incomplete body, connect and read timeouts, pool timeout, and
    network errors) and ``OSError`` raised while streaming. HTTP 408,
    429, and 5xx are the same kind of failure. ``InboundMediaTooLarge``
    is terminal. A finished body that fails decryption is not passed
    here. Any other 4xx is terminal. ``status_code`` is the response
    status when one was already read; otherwise the status is taken
    from ``exc`` when it carries a response.
    """
    if isinstance(exc, InboundMediaTooLarge):
        return False
    status = status_code if status_code is not None else _status_code_from_exception(exc)
    if status is not None and status >= 400:
        return _http_status_retryable(status)
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, OSError):
        return True
    return False


def _backoff_after_attempt(attempt: int, backoff_seconds: Sequence[float]) -> float:
    """Seconds to wait after 1-based ``attempt`` before the next GET.

    Attempt 1 uses the first delay, attempt 2 the second. A missing
    entry waits 0 so a test can pass an empty sequence.
    """
    index = attempt - 1
    if index < 0 or index >= len(backoff_seconds):
        return 0.0
    return float(backoff_seconds[index])


async def inbound_download_sleep(seconds: float) -> None:
    """Wait between inbound download attempts.

    Tests replace this, or pass ``sleep`` to ``download_inbound_cipher``,
    so the 2s/5s backoff does not stall the suite.
    """
    await asyncio.sleep(seconds)


async def download_inbound_cipher(
    client: Any,
    url: str,
    directory: Path,
    max_bytes: int,
    *,
    timeout: Any,
    log: Callable[[str], None],
    label: str,
    attempts: int | None = None,
    backoff_seconds: Sequence[float] | None = None,
    sleep: Callable[[float], Awaitable[object]] | None = None,
) -> Path:
    """Stream one ciphertext to a fresh temp file, retrying transport drops.

    Returns the cipher path after a complete body. The caller decrypts
    that file and deletes it. Each attempt has its own temp path. That
    path is removed before the backoff, including when the attempt is
    cancelled, so no ``.inbound-*`` cipher is left between attempts or
    after the final failure. ``InboundMediaTooLarge`` and any
    non-retryable error propagate on the attempt that raised them and
    are not tried again.

    A retry logs ``media {label}: download attempt 1/3 failed (...);
    retrying``. The last failure is not logged here; the gateway keeps
    the ``download/decrypt failed`` line. Decryption stays in the
    gateway so a bad completed body is not downloaded again.

    ``attempts`` and ``backoff_seconds`` default to
    ``INBOUND_DOWNLOAD_MAX_ATTEMPTS`` and
    ``INBOUND_DOWNLOAD_BACKOFF_SECONDS``. Tests pass ``(0, 0)`` and a
    ``sleep`` that returns immediately.
    """
    total = INBOUND_DOWNLOAD_MAX_ATTEMPTS if attempts is None else attempts
    if total < 1:
        raise ValueError("attempts must be positive")
    delays = (
        INBOUND_DOWNLOAD_BACKOFF_SECONDS
        if backoff_seconds is None
        else tuple(backoff_seconds)
    )
    pause = inbound_download_sleep if sleep is None else sleep
    for attempt in range(1, total + 1):
        cipher_path = inbound_temp_path(directory, "cipher")
        status_code: int | None = None
        keep_cipher = False
        retry_exc: BaseException | None = None
        try:
            async with client.stream("GET", url, timeout=timeout) as response:
                status_code = _response_status(response)
                await save_inbound_body(response, cipher_path, max_bytes)
            keep_cipher = True
            return cipher_path
        except InboundMediaTooLarge:
            raise
        except Exception as exc:
            if attempt < total and is_retryable_inbound_download_error(exc, status_code):
                retry_exc = exc
            else:
                raise
        finally:
            if not keep_cipher:
                remove_file_quietly(cipher_path)
        if retry_exc is None:
            raise RuntimeError("inbound download retry lost the attempt error")
        delay = _backoff_after_attempt(attempt, delays)
        log(
            f"media {label}: download attempt {attempt}/{total} "
            f"failed ({retry_exc}); retrying"
        )
        await pause(delay)
    raise RuntimeError("inbound download made no attempt")


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
