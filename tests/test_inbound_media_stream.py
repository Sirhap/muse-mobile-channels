"""Inbound media streaming, the shared 1GiB cap, decrypt equivalence, and retry.

Downloads are fake response objects and in-process httpx mocks. Nothing
in this file opens a socket.
"""

from __future__ import annotations

import asyncio
import base64
import gc
import importlib.util
import json
import os
import random
import sys
import tempfile
import tracemalloc
import unittest
from collections.abc import Awaitable, Callable
from pathlib import Path
from urllib.parse import quote

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import inbound_media as im  # noqa: E402
from channel_common import media_filename  # noqa: E402

import httpx  # noqa: E402

ECB_KEY = bytes(range(16))
CBC_KEY = bytes(range(32))
WX_URL = "https://novac2c.cdn.weixin.qq.com/c2c/obj"
IMPORT_SBX = Path(tempfile.mkdtemp(prefix="inbound-media-import-"))
_OLD_ENV: dict[str, str | None] = {}
wx = None
wc = None


def _load_gateway(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def setUpModule() -> None:
    global wx, wc
    for key in ("HOME", "MUSE_HOME"):
        _OLD_ENV[key] = os.environ.get(key)
    os.environ["HOME"] = str(IMPORT_SBX)
    os.environ["MUSE_HOME"] = str(IMPORT_SBX)
    wx = _load_gateway("weixin_gateway_inbound_test", ROOT / "weixin-bot" / "gateway.py")
    wc = _load_gateway("wecom_gateway_inbound_test", ROOT / "wecom-bot" / "gateway.py")


def tearDownModule() -> None:
    for key, value in _OLD_ENV.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _run(coro):
    return asyncio.run(coro)


def legacy_aes_ecb_decrypt(data: bytes, key: bytes) -> bytes:
    """The in-memory Weixin decrypt, copied so the stream path has an independent oracle."""
    dec = Cipher(algorithms.AES(key), modes.ECB()).decryptor()
    plain = dec.update(data) + dec.finalize()
    if not plain:
        raise ValueError("empty plaintext")
    pad = plain[-1]
    if pad < 1 or pad > 16 or pad > len(plain) or plain[-pad:] != bytes([pad]) * pad:
        raise ValueError(f"bad PKCS#7 padding: {pad}")
    return plain[:-pad]


def legacy_decrypt_wecom_media(data: bytes, aeskey_b64: str) -> bytes:
    """The in-memory WeCom decrypt, copied so the stream path has an independent oracle."""
    key_arg = aeskey_b64.strip()
    key_arg += "=" * (-len(key_arg) % 4)
    try:
        key = base64.b64decode(key_arg)
    except Exception:
        key = base64.urlsafe_b64decode(key_arg)
    iv = key[:16]
    decryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).decryptor()
    plain = decryptor.update(data) + decryptor.finalize()
    if not plain:
        raise ValueError("empty plaintext")
    pad = plain[-1]
    if pad < 1 or pad > 32 or pad > len(plain) or plain[-pad:] != bytes([pad]) * pad:
        raise ValueError(f"bad PKCS#7 padding: {pad}")
    return plain[:-pad]


def _encrypt_cbc(data: bytes, key: bytes, pad_size: int) -> bytes:
    pad = pad_size - (len(data) % pad_size)
    padded = data + bytes([pad]) * pad
    enc = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor()
    return enc.update(padded) + enc.finalize()


class FakeResponse:
    """Stand-in for the object ``async with client.stream()`` yields."""

    def __init__(self, chunks, status_code: int = 200, headers: dict | None = None) -> None:
        self._chunks = [bytes(chunk) for chunk in chunks]
        self.status_code = status_code
        self.headers = {} if headers is None else dict(headers)
        self.pulled = 0
        self.aiter_calls: list = []
        self.entered = 0
        self.exited = 0

    @property
    def content(self) -> bytes:
        raise AssertionError("response.content loads the whole body")

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"http {self.status_code}")

    def aiter_bytes(self, chunk_size: int | None = None):
        self.aiter_calls.append(chunk_size)

        async def _iterate():
            for chunk in self._chunks:
                self.pulled += 1
                yield chunk

        return _iterate()

    async def __aenter__(self):
        self.entered += 1
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.exited += 1
        return False


class FakeClient:
    def __init__(self, responses) -> None:
        if isinstance(responses, FakeResponse):
            responses = [responses]
        self._responses = list(responses)
        self.calls: list[dict] = []

    def stream(self, method, url, timeout=None):
        self.calls.append({"method": method, "url": url, "timeout": timeout})
        if not self._responses:
            raise AssertionError("no fake response left")
        return self._responses.pop(0)


def inbound_temp_names(directory: Path) -> list[str]:
    """``.inbound-*`` names still in ``directory``."""
    if not directory.exists():
        return []
    return sorted(
        path.name for path in directory.iterdir() if path.name.startswith(".inbound-")
    )


class BodyErrorResponse(FakeResponse):
    """Yields ``chunks``, then raises ``error`` from the body iterator."""

    def __init__(self, chunks, error: BaseException, status_code: int = 200, headers=None) -> None:
        super().__init__(chunks, status_code=status_code, headers=headers)
        self.error = error

    def aiter_bytes(self, chunk_size: int | None = None):
        self.aiter_calls.append(chunk_size)

        async def _iterate():
            for chunk in self._chunks:
                self.pulled += 1
                yield chunk
            raise self.error

        return _iterate()


class NotingBody(FakeResponse):
    """Records ``.inbound-*`` names once the cipher file has been opened."""

    def __init__(
        self,
        chunks,
        directory: Path,
        seen: list,
        error: BaseException | None = None,
        status_code: int = 200,
    ) -> None:
        super().__init__(chunks, status_code=status_code)
        self.directory = directory
        self.seen = seen
        self.error = error

    def aiter_bytes(self, chunk_size: int | None = None):
        self.aiter_calls.append(chunk_size)

        async def _iterate():
            self.seen.append(inbound_temp_names(self.directory))
            for chunk in self._chunks:
                self.pulled += 1
                yield chunk
            if self.error is not None:
                raise self.error

        return _iterate()


class DirectoryClient(FakeClient):
    """Records leftover temp names at the start of each ``stream`` call."""

    def __init__(self, responses, directory: Path) -> None:
        super().__init__(responses)
        self.directory = directory
        self.temps_at_start: list[list[str]] = []

    def stream(self, method, url, timeout=None):
        self.temps_at_start.append(inbound_temp_names(self.directory))
        return super().stream(method, url, timeout)


class ScriptedClient:
    """``stream`` raises a scripted exception or returns a scripted response."""

    def __init__(self, steps) -> None:
        self._steps = list(steps)
        self.calls: list[dict] = []

    def stream(self, method, url, timeout=None):
        self.calls.append({"method": method, "url": url, "timeout": timeout})
        if not self._steps:
            raise AssertionError("no scripted step left")
        step = self._steps.pop(0)
        if isinstance(step, BaseException):
            raise step
        return step


def _split(data: bytes, size: int) -> list[bytes]:
    return [data[index:index + size] for index in range(0, len(data), size)] or [b""]


class StreamWriteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dest = Path(self.tmp.name) / "body.bin"

    def test_keeps_a_body_at_the_cap_and_ignores_empty_chunks(self) -> None:
        async def chunks():
            yield b"0123"
            yield b""
            yield b"4567"

        written = _run(im.write_bounded_stream(chunks(), self.dest, 8))
        self.assertEqual(written, 8)
        self.assertEqual(self.dest.read_bytes(), b"01234567")

    def test_stops_on_the_chunk_that_crosses_the_cap_and_deletes_the_partial(self) -> None:
        pulled = 0

        async def chunks():
            nonlocal pulled
            for chunk in (b"aaaa", b"bbbb", b"cccc"):
                pulled += 1
                yield chunk

        with self.assertRaises(im.InboundMediaTooLarge) as caught:
            _run(im.write_bounded_stream(chunks(), self.dest, 6))
        self.assertEqual(caught.exception.nbytes, 8)
        self.assertEqual(pulled, 2)
        self.assertFalse(self.dest.exists())

    def test_a_single_oversized_chunk_is_not_written(self) -> None:
        async def chunks():
            yield b"abcdef"
            yield b"more"

        with self.assertRaises(im.InboundMediaTooLarge) as caught:
            _run(im.write_bounded_stream(chunks(), self.dest, 3))
        self.assertEqual(caught.exception.nbytes, 6)
        self.assertFalse(self.dest.exists())

    def test_stream_error_deletes_the_partial(self) -> None:
        async def chunks():
            yield b"abc"
            raise RuntimeError("net")

        with self.assertRaises(RuntimeError):
            _run(im.write_bounded_stream(chunks(), self.dest, 100))
        self.assertFalse(self.dest.exists())

    def test_content_length_over_the_cap_does_not_read_the_body(self) -> None:
        response = FakeResponse(
            [b"a" * 50],
            headers={"content-length": "1000"},
        )

        async def _save():
            async with response:
                return await im.save_inbound_body(response, self.dest, 100)

        with self.assertRaises(im.InboundMediaTooLarge) as caught:
            _run(_save())
        self.assertEqual(caught.exception.nbytes, 1000)
        self.assertEqual(response.aiter_calls, [])
        self.assertEqual(response.pulled, 0)
        self.assertEqual(response.entered, 1)
        self.assertEqual(response.exited, 1)
        self.assertFalse(self.dest.exists())

    def test_short_content_length_does_not_raise_the_cap(self) -> None:
        response = FakeResponse(
            [b"01234567", b"89abcdef", b"more"],
            headers={"content-length": "1"},
        )

        async def _save():
            async with response:
                return await im.save_inbound_body(response, self.dest, 10)

        with self.assertRaises(im.InboundMediaTooLarge) as caught:
            _run(_save())
        self.assertEqual(caught.exception.nbytes, 16)
        self.assertEqual(response.pulled, 2)
        self.assertEqual(response.aiter_calls, [im.INBOUND_MEDIA_CHUNK_BYTES])
        self.assertFalse(self.dest.exists())

    def test_save_reassembles_small_chunks_under_the_cap(self) -> None:
        body = b"\x89PNG\r\n\x1a\n" + b"payload"
        response = FakeResponse(_split(body, 3), headers={"content-length": str(len(body))})

        async def _save():
            async with response:
                return await im.save_inbound_body(response, self.dest, len(body))

        written = _run(_save())
        self.assertEqual(written, len(body))
        self.assertEqual(self.dest.read_bytes(), body)
        self.assertEqual(response.pulled, len(_split(body, 3)))

    def test_http_error_does_not_create_a_file(self) -> None:
        response = FakeResponse([b"nope"], status_code=500)

        async def _save():
            async with response:
                return await im.save_inbound_body(response, self.dest, 100)

        with self.assertRaises(RuntimeError):
            _run(_save())
        self.assertEqual(response.aiter_calls, [])
        self.assertFalse(self.dest.exists())

    def test_content_length_parsing(self) -> None:
        self.assertEqual(im.content_length_bytes({"content-length": " 12 "}), 12)
        self.assertEqual(im.content_length_bytes(httpx.Headers({"Content-Length": "42"})), 42)
        self.assertIsNone(im.content_length_bytes({"content-length": "nope"}))
        self.assertIsNone(im.content_length_bytes({"content-length": "-1"}))
        self.assertIsNone(im.content_length_bytes(None))
        self.assertIsNone(im.content_length_bytes({}))

    def test_streaming_write_does_not_accumulate_the_body(self) -> None:
        total = 4 * 1024 * 1024

        async def chunks():
            for _ in range(64):
                yield b"B" * (64 * 1024)

        gc.collect()
        tracemalloc.start()
        try:
            written = _run(im.write_bounded_stream(chunks(), self.dest, total))
            _current, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(written, total)
        self.assertEqual(self.dest.stat().st_size, total)
        self.assertLess(peak, 1024 * 1024)


class DecryptEquivalenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.rng = random.Random(24)

    def _paths(self, label: str) -> tuple[Path, Path]:
        directory = Path(self.tmp.name) / label
        directory.mkdir(parents=True, exist_ok=True)
        return directory / "cipher", directory / "plain"

    def _assert_same_error(self, legacy_call, stream_call) -> None:
        with self.assertRaises(Exception) as legacy_box:
            legacy_call()
        with self.assertRaises(type(legacy_box.exception)) as stream_box:
            stream_call()
        self.assertEqual(str(stream_box.exception), str(legacy_box.exception))

    def test_ecb_stream_matches_legacy_for_lengths_and_read_sizes(self) -> None:
        lengths = [0, 1, 15, 16, 17, 31, 32, 33, 63, 64, 100, 1000, 4095, 4096, 4097]
        for length in lengths:
            plain = self.rng.randbytes(length)
            cipher = wx.aes_ecb_encrypt(plain, ECB_KEY)
            expected = legacy_aes_ecb_decrypt(cipher, ECB_KEY)
            self.assertEqual(wx.aes_ecb_decrypt(cipher, ECB_KEY), expected)
            self.assertEqual(expected, plain)
            for read_size in (1, 16, 24, im.INBOUND_MEDIA_CHUNK_BYTES):
                with self.subTest(length=length, read_size=read_size):
                    src, dst = self._paths(f"ecb-{length}-{read_size}")
                    src.write_bytes(cipher)
                    im.stream_pkcs7_decrypt_to_file(
                        src,
                        dst,
                        *_ecb_calls(ECB_KEY),
                        max_pad=16,
                        read_size=read_size,
                    )
                    self.assertEqual(dst.read_bytes(), expected)

    def test_ecb_file_wrapper_matches_across_the_default_chunk(self) -> None:
        plain = self.rng.randbytes(im.INBOUND_MEDIA_CHUNK_BYTES + 17)
        cipher = wx.aes_ecb_encrypt(plain, ECB_KEY)
        expected = legacy_aes_ecb_decrypt(cipher, ECB_KEY)
        src, dst = self._paths("ecb-chunk")
        src.write_bytes(cipher)
        wx.aes_ecb_decrypt_file(src, dst, ECB_KEY)
        self.assertEqual(dst.read_bytes(), expected)
        self.assertEqual(expected, plain)

    def test_cbc_stream_matches_legacy_for_both_pad_widths(self) -> None:
        lengths = [0, 1, 15, 16, 17, 31, 32, 33, 63, 64, 100, 1000]
        for pad_size in (16, 32):
            for length in lengths:
                plain = self.rng.randbytes(length)
                cipher = _encrypt_cbc(plain, CBC_KEY, pad_size)
                token = base64.b64encode(CBC_KEY).decode("ascii").rstrip("=")
                expected = legacy_decrypt_wecom_media(cipher, f"  {token}  ")
                self.assertEqual(wc.decrypt_wecom_media(cipher, f"  {token}  "), expected)
                self.assertEqual(expected, plain)
                for read_size in (1, 15, 16, 32):
                    with self.subTest(pad_size=pad_size, length=length, read_size=read_size):
                        src, dst = self._paths(f"cbc-{pad_size}-{length}-{read_size}")
                        src.write_bytes(cipher)
                        im.stream_pkcs7_decrypt_to_file(
                            src,
                            dst,
                            *_cbc_calls(CBC_KEY),
                            max_pad=32,
                            read_size=read_size,
                        )
                        self.assertEqual(dst.read_bytes(), expected)

    def test_cbc_file_wrapper_and_urlsafe_token_match_legacy(self) -> None:
        plain = b"RIFF" + b"\x00\x00\x00\x00" + b"WEBP" + self.rng.randbytes(200)
        raw = bytes([0xfb, 0xef]) + CBC_KEY[2:]
        cipher = _encrypt_cbc(plain, raw, 32)
        token = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
        expected = legacy_decrypt_wecom_media(cipher, token)
        self.assertEqual(wc.decrypt_wecom_media(cipher, token), expected)
        src, dst = self._paths("cbc-urlsafe")
        src.write_bytes(cipher)
        wc.decrypt_wecom_media_file(src, dst, token)
        self.assertEqual(dst.read_bytes(), expected)

    def test_bad_padding_and_unaligned_ciphertext_match_and_leave_no_file(self) -> None:
        cipher = wx.aes_ecb_encrypt(b"hello", ECB_KEY)
        bad_pad = cipher[:-1] + bytes([cipher[-1] ^ 0x5A])
        cases = (
            ("pad", bad_pad),
            ("short", cipher + b"\x00"),
            ("empty", b""),
        )
        for label, blob in cases:
            with self.subTest(label=label):
                src, dst = self._paths(f"bad-ecb-{label}")
                src.write_bytes(blob)

                def legacy_call(blob=blob):
                    return legacy_aes_ecb_decrypt(blob, ECB_KEY)

                def stream_call(src=src, dst=dst):
                    wx.aes_ecb_decrypt_file(src, dst, ECB_KEY)

                self._assert_same_error(legacy_call, stream_call)
                self.assertFalse(dst.exists())

        token = base64.b64encode(CBC_KEY).decode("ascii")
        cbc_cipher = _encrypt_cbc(b"hello", CBC_KEY, 32)
        for label, blob in (
            ("pad", cbc_cipher[:-1] + bytes([cbc_cipher[-1] ^ 0x11])),
            ("short", cbc_cipher + b"\x00"),
            ("empty", b""),
        ):
            with self.subTest(label=f"cbc-{label}"):
                src, dst = self._paths(f"bad-cbc-{label}")
                src.write_bytes(blob)

                def legacy_call(blob=blob, token=token):
                    return legacy_decrypt_wecom_media(blob, token)

                def stream_call(src=src, dst=dst, token=token):
                    wc.decrypt_wecom_media_file(src, dst, token)

                self._assert_same_error(legacy_call, stream_call)
                self.assertFalse(dst.exists())

    def test_streaming_decrypt_does_not_hold_the_plaintext(self) -> None:
        plain = b"C" * (4 * 1024 * 1024)
        cipher = wx.aes_ecb_encrypt(plain, ECB_KEY)
        expected = legacy_aes_ecb_decrypt(cipher, ECB_KEY)
        src, dst = self._paths("mem")
        src.write_bytes(cipher)
        del plain, cipher
        gc.collect()
        tracemalloc.start()
        try:
            im.stream_pkcs7_decrypt_to_file(
                src,
                dst,
                *_ecb_calls(ECB_KEY),
                max_pad=16,
                read_size=64 * 1024,
            )
            _current, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(dst.read_bytes(), expected)
        self.assertLess(peak, 2 * 1024 * 1024)

    def test_sniff_prefix_matches_the_whole_plaintext(self) -> None:
        body = b"RIFF" + b"\x00\x00\x00\x00" + b"WEBP" + (b"Q" * 80)
        self.assertGreater(len(body), im.SNIFF_HEADER_BYTES)
        for sniffer in (wx.sniff_ext, wc.sniff_ext):
            self.assertEqual(sniffer(body), "webp")
            self.assertEqual(sniffer(body[:im.SNIFF_HEADER_BYTES]), sniffer(body))
        src = Path(self.tmp.name) / "plain.webp"
        src.write_bytes(body)
        published = im.publish_inbound_plaintext(src, "../../etc/passwd", 2, wx.sniff_ext)
        self.assertEqual(published.name, media_filename("../../etc/passwd", 2, "webp"))
        self.assertEqual(published.read_bytes(), body)
        self.assertFalse(src.exists())


def _ecb_calls(key: bytes):
    decryptor = Cipher(algorithms.AES(key), modes.ECB()).decryptor()
    return decryptor.update, decryptor.finalize


def _cbc_calls(key: bytes):
    decryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).decryptor()
    return decryptor.update, decryptor.finalize


class _GatewaySandbox(unittest.TestCase):
    module = None

    def setUp(self) -> None:
        self.logs: list[str] = []
        self._saved_cap = self.module.INBOUND_MEDIA_MAX_BYTES
        self._saved_media_dir = self.module.MEDIA_DIR
        self._saved_log = self.module.log
        self.module.log = self.logs.append
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.media_dir = Path(self.tmp.name) / "media"
        self.module.MEDIA_DIR = self.media_dir
        self.download_sleeps: list[float] = []
        self.temps_during_backoff: list[list[str]] = []
        self._saved_download_sleep = im.inbound_download_sleep

        async def _record_backoff(seconds: float) -> None:
            self.download_sleeps.append(seconds)
            self.temps_during_backoff.append(inbound_temp_names(self.media_dir))

        im.inbound_download_sleep = _record_backoff
        self.addCleanup(setattr, im, "inbound_download_sleep", self._saved_download_sleep)
        self.addCleanup(self._restore)

    def _restore(self) -> None:
        self.module.INBOUND_MEDIA_MAX_BYTES = self._saved_cap
        self.module.MEDIA_DIR = self._saved_media_dir
        self.module.log = self._saved_log

    def _names(self) -> list[str]:
        if not self.media_dir.exists():
            return []
        return sorted(path.name for path in self.media_dir.iterdir())


class _RetryDownloadMixin:
    """Retry cases shared by the Weixin and WeCom download paths."""

    def _invoke_download(self, client, media: list[dict], msgid: str) -> None:
        raise NotImplementedError

    def _media_and_cipher(self, plain: bytes) -> tuple[dict, bytes]:
        raise NotImplementedError

    def _spy_decrypt(self) -> list[bytes]:
        raise NotImplementedError

    def _assert_no_inbound_temps(self) -> None:
        self.assertEqual(inbound_temp_names(self.media_dir), [])
        self.assertTrue(all(names == [] for names in self.temps_during_backoff))

    def test_transport_drop_retries_then_publishes(self) -> None:
        plain = b"\x89PNG\r\n\x1a\n" + b"retry-body"
        media, cipher = self._media_and_cipher(plain)
        message = (
            "peer closed connection without sending complete message body "
            f"(received 8 bytes, expected {len(cipher)})"
        )
        seen_names: list[list[str]] = []
        partial = cipher[:8]
        responses = [
            NotingBody(
                [partial],
                self.media_dir,
                seen_names,
                error=httpx.RemoteProtocolError(message),
            ),
            NotingBody(
                [partial],
                self.media_dir,
                seen_names,
                error=httpx.RemoteProtocolError(message),
            ),
            NotingBody(_split(cipher, 5), self.media_dir, seen_names),
        ]
        client = DirectoryClient(responses, self.media_dir)
        decrypted = self._spy_decrypt()
        self._invoke_download(client, [media], "drop")
        path = Path(media["local_path"])
        names = [group[0] for group in seen_names]
        self.assertEqual(len(client.calls), 3)
        self.assertEqual(decrypted, [cipher])
        self.assertEqual(path.read_bytes(), plain)
        self.assertEqual(path.name, media_filename("drop", 0, "png"))
        self.assertEqual(self._names(), [path.name])
        self.assertEqual(self.download_sleeps, [2.0, 5.0])
        self.assertEqual(client.temps_at_start, [[], [], []])
        self.assertEqual(self.temps_during_backoff, [[], []])
        self.assertEqual([len(group) for group in seen_names], [1, 1, 1])
        self.assertEqual(len(names), 3)
        self.assertEqual(len(set(names)), 3)
        for name in names:
            self.assertTrue(name.startswith(".inbound-") and name.endswith(".cipher"))
        self.assertGreater(responses[0].pulled, 0)
        self.assertGreater(responses[1].pulled, 0)
        self.assertIn(
            f"media drop/0: download attempt 1/3 failed ({message}); retrying",
            self.logs,
        )
        self.assertIn(
            f"media drop/0: download attempt 2/3 failed ({message}); retrying",
            self.logs,
        )
        self.assertFalse(any("attempt 3/3" in line for line in self.logs))
        self.assertFalse(any("download/decrypt failed" in line for line in self.logs))
        self.assertTrue(any(f"saved {path} ({len(plain)} bytes)" in line for line in self.logs))
        self._assert_no_inbound_temps()

    def test_truncated_stream_stops_after_three_attempts(self) -> None:
        media, cipher = self._media_and_cipher(b"\x89PNG\r\n\x1a\n" + b"never")
        message = (
            "peer closed connection without sending complete message body "
            f"(received 8 bytes, expected {len(cipher)})"
        )
        seen_names: list[list[str]] = []
        responses = [
            NotingBody(
                [cipher[:8]],
                self.media_dir,
                seen_names,
                error=httpx.RemoteProtocolError(message),
            )
            for _ in range(3)
        ]
        client = DirectoryClient(responses, self.media_dir)
        decrypted = self._spy_decrypt()
        self._invoke_download(client, [media], "trunc")
        self.assertEqual(len(client.calls), 3)
        self.assertEqual(decrypted, [])
        self.assertNotIn("local_path", media)
        self.assertEqual(self._names(), [])
        self.assertEqual(self.download_sleeps, [2.0, 5.0])
        self.assertEqual(client.temps_at_start, [[], [], []])
        self.assertEqual([len(group) for group in seen_names], [1, 1, 1])
        self.assertEqual(len({group[0] for group in seen_names}), 3)
        self.assertTrue(all(response.pulled > 0 for response in responses))
        self.assertIn(
            f"media trunc/0: download attempt 1/3 failed ({message}); retrying",
            self.logs,
        )
        self.assertIn(
            f"media trunc/0: download attempt 2/3 failed ({message}); retrying",
            self.logs,
        )
        self.assertFalse(any("attempt 3/3" in line for line in self.logs))
        self.assertIn(f"media trunc/0: download/decrypt failed: {message}", self.logs)
        self._assert_no_inbound_temps()

    def test_too_large_is_not_retried(self) -> None:
        media, cipher = self._media_and_cipher(b"\x89PNG\r\n\x1a\n" + b"big")
        self.module.INBOUND_MEDIA_MAX_BYTES = 8
        self.assertGreater(len(cipher), 8)
        client = DirectoryClient(
            [FakeResponse([cipher]), FakeResponse([cipher])],
            self.media_dir,
        )
        decrypted = self._spy_decrypt()
        self._invoke_download(client, [media], "cap")
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(decrypted, [])
        self.assertNotIn("local_path", media)
        self.assertEqual(self._names(), [])
        self.assertEqual(self.download_sleeps, [])
        self.assertFalse(any("retrying" in line for line in self.logs))
        self.assertFalse(any("download/decrypt failed" in line for line in self.logs))
        self.assertTrue(any("too large" in line and "skipped" in line for line in self.logs))
        self._assert_no_inbound_temps()

    def test_http_404_is_not_retried(self) -> None:
        media, cipher = self._media_and_cipher(b"\x89PNG\r\n\x1a\n" + b"missing")
        missing = FakeResponse([b"missing"], status_code=404)
        client = DirectoryClient(
            [missing, FakeResponse([cipher])],
            self.media_dir,
        )
        self._invoke_download(client, [media], "e404")
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(missing.aiter_calls, [])
        self.assertEqual(missing.entered, 1)
        self.assertEqual(missing.exited, 1)
        self.assertNotIn("local_path", media)
        self.assertEqual(self._names(), [])
        self.assertEqual(self.download_sleeps, [])
        self.assertFalse(any("retrying" in line for line in self.logs))
        self.assertIn("media e404/0: download/decrypt failed: http 404", self.logs)
        self._assert_no_inbound_temps()

    def test_http_503_second_attempt_publishes(self) -> None:
        plain = b"\x89PNG\r\n\x1a\n" + b"503-ok"
        media, cipher = self._media_and_cipher(plain)
        client = DirectoryClient(
            [
                FakeResponse([b"busy"], status_code=503),
                FakeResponse(_split(cipher, 4)),
            ],
            self.media_dir,
        )
        self._invoke_download(client, [media], "s503")
        path = Path(media["local_path"])
        self.assertEqual(len(client.calls), 2)
        self.assertEqual(path.read_bytes(), plain)
        self.assertEqual(path.name, media_filename("s503", 0, "png"))
        self.assertEqual(self.download_sleeps, [2.0])
        self.assertEqual(client.temps_at_start, [[], []])
        self.assertIn(
            "media s503/0: download attempt 1/3 failed (http 503); retrying",
            self.logs,
        )
        self.assertFalse(any("download/decrypt failed" in line for line in self.logs))
        self._assert_no_inbound_temps()

    def test_http_503_stops_after_three_attempts(self) -> None:
        media, _cipher = self._media_and_cipher(b"\x89PNG\r\n\x1a\n" + b"still-busy")
        failures = [FakeResponse([b"x"], status_code=503) for _ in range(4)]
        client = DirectoryClient(failures, self.media_dir)
        self._invoke_download(client, [media], "e503")
        self.assertEqual(len(client.calls), 3)
        self.assertEqual(failures[3].entered, 0)
        self.assertNotIn("local_path", media)
        self.assertEqual(self._names(), [])
        self.assertEqual(self.download_sleeps, [2.0, 5.0])
        self.assertEqual([item.aiter_calls for item in failures[:3]], [[], [], []])
        self.assertIn(
            "media e503/0: download attempt 1/3 failed (http 503); retrying",
            self.logs,
        )
        self.assertIn(
            "media e503/0: download attempt 2/3 failed (http 503); retrying",
            self.logs,
        )
        self.assertFalse(any("attempt 3/3" in line for line in self.logs))
        self.assertIn("media e503/0: download/decrypt failed: http 503", self.logs)
        self._assert_no_inbound_temps()

    def test_decrypt_failure_of_a_complete_body_is_not_retried(self) -> None:
        media, _cipher = self._media_and_cipher(b"complete-body")
        bad = b"abc"
        decrypted = self._spy_decrypt()
        client = DirectoryClient(
            [FakeResponse([bad]), FakeResponse([bad]), FakeResponse([bad])],
            self.media_dir,
        )
        self._invoke_download(client, [media], "bad")
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(decrypted, [bad])
        self.assertNotIn("local_path", media)
        self.assertEqual(self._names(), [])
        self.assertEqual(self.download_sleeps, [])
        self.assertFalse(any("retrying" in line for line in self.logs))
        self.assertTrue(any(
            line.startswith("media bad/0: download/decrypt failed:") for line in self.logs
        ))
        self._assert_no_inbound_temps()


class WeixinDownloadTests(_RetryDownloadMixin, _GatewaySandbox):
    module = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.module = wx

    def _invoke_download(self, client, media: list[dict], msgid: str) -> None:
        _run(self.module.Gateway.__new__(self.module.Gateway)._download_media(client, media, msgid))

    def _media_and_cipher(self, plain: bytes) -> tuple[dict, bytes]:
        return self._image(ECB_KEY), self.module.aes_ecb_encrypt(plain, ECB_KEY)

    def _spy_decrypt(self) -> list[bytes]:
        seen: list[bytes] = []
        original = self.module.aes_ecb_decrypt_file

        def spy(src: Path, dst: Path, key: bytes) -> None:
            seen.append(src.read_bytes())
            original(src, dst, key)

        self.module.aes_ecb_decrypt_file = spy
        self.addCleanup(setattr, self.module, "aes_ecb_decrypt_file", original)
        return seen

    def _image(self, key: bytes, url: str = WX_URL, extra: dict | None = None) -> dict:
        item = {"aeskey": key.hex(), "media": {"full_url": url}}
        if extra:
            item.update(extra)
        return {"kind": "image", "item": item}

    def test_shared_cap_is_one_gibibyte(self) -> None:
        self.assertEqual(im.INBOUND_MEDIA_MAX_BYTES, 1 * 1024 * 1024 * 1024)
        self.assertIs(wx.INBOUND_MEDIA_MAX_BYTES, im.INBOUND_MEDIA_MAX_BYTES)
        self.assertIs(wc.INBOUND_MEDIA_MAX_BYTES, im.INBOUND_MEDIA_MAX_BYTES)
        self.assertGreater(im.INBOUND_MEDIA_MAX_BYTES, 25 * 1024 * 1024)

    def test_stream_saves_png_and_uses_media_filename(self) -> None:
        plain = b"\x89PNG\r\n\x1a\n" + b"hello-png"
        cipher = wx.aes_ecb_encrypt(plain, ECB_KEY)
        response = FakeResponse(_split(cipher, 5))
        client = FakeClient(response)
        media = [self._image(ECB_KEY)]
        _run(wx.Gateway.__new__(wx.Gateway)._download_media(client, media, "msg/1"))
        expected = media_filename("msg/1", 0, "png")
        self.assertEqual(self._names(), [expected])
        path = Path(media[0]["local_path"])
        self.assertEqual(path.name, expected)
        self.assertEqual(path.read_bytes(), plain)
        self.assertEqual(response.aiter_calls, [im.INBOUND_MEDIA_CHUNK_BYTES])
        self.assertEqual(response.entered, 1)
        self.assertEqual(response.exited, 1)
        self.assertEqual(client.calls[0]["method"], "GET")
        self.assertEqual(client.calls[0]["url"], WX_URL)
        self.assertEqual(client.calls[0]["timeout"], httpx.Timeout(30.0, connect=10.0))
        self.assertTrue(any(line.startswith(f"media msg/1/0: saved {path} ({len(plain)} bytes)") for line in self.logs))

    def test_empty_plaintext_is_saved_as_bin(self) -> None:
        cipher = wx.aes_ecb_encrypt(b"", ECB_KEY)
        client = FakeClient(FakeResponse([cipher]))
        media = [self._image(ECB_KEY)]
        _run(wx.Gateway.__new__(wx.Gateway)._download_media(client, media, "empty"))
        path = Path(media[0]["local_path"])
        self.assertEqual(path.name, media_filename("empty", 0, "bin"))
        self.assertEqual(path.read_bytes(), b"")

    def test_cap_boundary_and_early_stop(self) -> None:
        plain = b"boundary-body"
        cipher = wx.aes_ecb_encrypt(plain, ECB_KEY)
        wx.INBOUND_MEDIA_MAX_BYTES = len(cipher)
        client = FakeClient(FakeResponse([cipher]))
        media = [self._image(ECB_KEY)]
        _run(wx.Gateway.__new__(wx.Gateway)._download_media(client, media, "fit"))
        self.assertEqual(Path(media[0]["local_path"]).read_bytes(), plain)

        for path in list(self.media_dir.iterdir()):
            path.unlink()
        self.logs.clear()
        wx.INBOUND_MEDIA_MAX_BYTES = len(cipher) - 1
        chunks = _split(cipher, 7) + [b"extra-a", b"extra-b"]
        response = FakeResponse(chunks)
        client = FakeClient(response)
        media = [self._image(ECB_KEY)]
        _run(wx.Gateway.__new__(wx.Gateway)._download_media(client, media, "over"))
        self.assertNotIn("local_path", media[0])
        self.assertEqual(self._names(), [])
        self.assertLess(response.pulled, len(chunks))
        self.assertIn(
            f"media over/0: too large ({len(cipher)} bytes), skipped",
            self.logs,
        )
        self.assertFalse(any("download/decrypt failed" in line for line in self.logs))

    def test_declared_length_skips_without_a_body_read(self) -> None:
        wx.INBOUND_MEDIA_MAX_BYTES = 100
        response = FakeResponse([b"unused"], headers={"content-length": "488932880"})
        client = FakeClient(response)
        media = [self._image(ECB_KEY)]
        _run(wx.Gateway.__new__(wx.Gateway)._download_media(client, media, "big"))
        self.assertNotIn("local_path", media[0])
        self.assertEqual(response.aiter_calls, [])
        self.assertEqual(self._names(), [])
        self.assertIn("media big/0: too large (488932880 bytes), skipped", self.logs)

    def test_voice_is_not_downloaded_and_other_items_still_are(self) -> None:
        plain = b"\xff\xd8\xff" + b"jpg"
        cipher = wx.aes_ecb_encrypt(plain, ECB_KEY)
        voice = {
            "kind": "voice",
            "item": {"aeskey": ECB_KEY.hex(), "media": {"full_url": WX_URL + "?voice=1"}},
        }
        image = self._image(ECB_KEY)
        client = FakeClient([FakeResponse([cipher])])
        media = [voice, image]
        _run(wx.Gateway.__new__(wx.Gateway)._download_media(client, media, "mix"))
        self.assertNotIn("local_path", voice)
        self.assertEqual(Path(image["local_path"]).read_bytes(), plain)
        self.assertEqual(len(client.calls), 1)
        self.assertIn("media mix/0: voice audio skipped (not received by policy)", self.logs)

    def test_bad_key_bad_host_and_query_param_url(self) -> None:
        gateway = wx.Gateway.__new__(wx.Gateway)
        bad_key = [{"kind": "file", "item": {"aeskey": "zz", "media": {"full_url": WX_URL}}}]
        client = FakeClient([])
        _run(gateway._download_media(client, bad_key, "k"))
        self.assertEqual(client.calls, [])
        self.assertTrue(any("bad aes key" in line for line in self.logs))

        self.logs.clear()
        refused = [self._image(ECB_KEY, url="https://evil.example/secret")]
        _run(gateway._download_media(client, refused, "h"))
        self.assertEqual(client.calls, [])
        self.assertIn("media h/0: refused non-Tencent url host", self.logs)

        plain = b"%PDF-1.4"
        cipher = wx.aes_ecb_encrypt(plain, ECB_KEY)
        param = "a/b c"
        item = {"aeskey": ECB_KEY.hex(), "media": {"encrypt_query_param": param}}
        client = FakeClient(FakeResponse([cipher]))
        media = [{"kind": "file", "item": item}]
        _run(gateway._download_media(client, media, "q"))
        expected_url = f"{wx.CDN_BASE_URL}/download?encrypted_query_param={quote(param, safe='')}"
        self.assertEqual(client.calls[0]["url"], expected_url)
        self.assertEqual(Path(media[0]["local_path"]).read_bytes(), plain)
        self.assertTrue(Path(media[0]["local_path"]).name.endswith(".pdf"))

    def test_aes_key_field_still_decrypts(self) -> None:
        plain = b"PK\x03\x04" + b"zip"
        cipher = wx.aes_ecb_encrypt(plain, ECB_KEY)
        media = [{
            "kind": "file",
            "item": {"media": {"full_url": WX_URL, "aes_key": base64.b64encode(ECB_KEY).decode("ascii")}},
        }]
        client = FakeClient(FakeResponse([cipher]))
        _run(wx.Gateway.__new__(wx.Gateway)._download_media(client, media, "aes"))
        self.assertEqual(Path(media[0]["local_path"]).read_bytes(), plain)
        self.assertTrue(Path(media[0]["local_path"]).name.endswith(".zip"))

    def test_http_502_is_retried_and_keeps_the_final_log_line(self) -> None:
        client = FakeClient([FakeResponse([b"x"], status_code=502) for _ in range(3)])
        media = [self._image(ECB_KEY)]
        _run(wx.Gateway.__new__(wx.Gateway)._download_media(client, media, "err"))
        self.assertEqual(len(client.calls), 3)
        self.assertNotIn("local_path", media[0])
        self.assertEqual(self._names(), [])
        self.assertEqual(self.download_sleeps, [2.0, 5.0])
        self.assertEqual(inbound_temp_names(self.media_dir), [])
        self.assertIn("media err/0: download attempt 1/3 failed (http 502); retrying", self.logs)
        self.assertIn("media err/0: download attempt 2/3 failed (http 502); retrying", self.logs)
        self.assertFalse(any("attempt 3/3" in line for line in self.logs))
        self.assertTrue(any("download/decrypt failed: http 502" in line for line in self.logs))

    def test_real_httpx_stream_saves_the_plaintext(self) -> None:
        plain = b"\x89PNG\r\n\x1a\n" + b"via-httpx"
        cipher = wx.aes_ecb_encrypt(plain, ECB_KEY)

        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=cipher)

        async def _download():
            transport = httpx.MockTransport(handler)
            async with httpx.AsyncClient(transport=transport) as client:
                media = [self._image(ECB_KEY)]
                await wx.Gateway.__new__(wx.Gateway)._download_media(client, media, "httpx")
                return media

        media = _run(_download())
        self.assertEqual(Path(media[0]["local_path"]).read_bytes(), plain)

    def test_over_limit_still_records_the_inbox_row(self) -> None:
        inbox = Path(self.tmp.name) / "inbox.jsonl"
        previous = wx.INBOX
        wx.INBOX = inbox
        self.addCleanup(setattr, wx, "INBOX", previous)
        wx.INBOUND_MEDIA_MAX_BYTES = 32
        response = FakeResponse([b"z" * 40], headers={"content-length": "400"})
        client = FakeClient(response)
        gateway = wx.Gateway.__new__(wx.Gateway)
        gateway.seen_msgids = set()
        gateway.context = {}
        gateway.msgs_received = 0
        gateway.feedback_track = {}
        gateway.write_status = lambda: None
        gateway._save_context = lambda: None
        gateway._mark_seen = lambda msgid: gateway.seen_msgids.add(str(msgid))
        gateway._maybe_arrival_feedback = lambda *args, **kwargs: None

        async def _no_typing(*args, **kwargs):
            return None

        gateway._send_typing = _no_typing
        msg = {
            "message_type": 1,
            "message_id": "mid-over",
            "from_user_id": "owner",
            "context_token": "tok",
            "item_list": [{
                "type": 4,
                "file_item": {
                    "file_name": "big.bin",
                    "aeskey": ECB_KEY.hex(),
                    "media": {"full_url": WX_URL},
                },
            }],
        }
        _run(gateway.handle_inbound(client, {"user_id": "owner"}, msg))
        row = json.loads(inbox.read_text(encoding="utf-8"))
        self.assertEqual(row["msgid"], "mid-over")
        self.assertNotIn("local_path", row["media"][0])
        self.assertIn("big.bin", row["text"])
        self.assertEqual(gateway.msgs_received, 1)
        self.assertIn("media mid-over/0: too large (400 bytes), skipped", self.logs)
        self.assertEqual(self._names(), [])


class WecomDownloadTests(_RetryDownloadMixin, _GatewaySandbox):
    module = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.module = wc

    def _invoke_download(self, client, media: list[dict], msgid: str) -> None:
        _run(self._gateway(client)._download_media(media, msgid))

    def _media_and_cipher(self, plain: bytes) -> tuple[dict, bytes]:
        return self._item(plain)

    def _spy_decrypt(self) -> list[bytes]:
        seen: list[bytes] = []
        original = self.module.decrypt_wecom_media_file

        def spy(src: Path, dst: Path, aeskey: str) -> None:
            seen.append(src.read_bytes())
            original(src, dst, aeskey)

        self.module.decrypt_wecom_media_file = spy
        self.addCleanup(setattr, self.module, "decrypt_wecom_media_file", original)
        return seen

    def _item(self, plain: bytes, pad_size: int = 32) -> tuple[dict, bytes]:
        cipher = _encrypt_cbc(plain, CBC_KEY, pad_size)
        token = base64.b64encode(CBC_KEY).decode("ascii")
        return {"kind": "file", "url": "https://weixin.qq.com/file", "aeskey": token}, cipher

    def _gateway(self, client) -> object:
        gateway = wc.Gateway.__new__(wc.Gateway)
        gateway._http = client
        return gateway

    def test_stream_saves_and_voice_is_not_special_cased(self) -> None:
        plain = b"\x89PNG\r\n\x1a\n" + b"wecom"
        item, cipher = self._item(plain)
        item["kind"] = "voice"
        response = FakeResponse(_split(cipher, 9))
        client = FakeClient(response)
        media = [item]
        _run(self._gateway(client)._download_media(media, "wc1"))
        path = Path(media[0]["local_path"])
        self.assertEqual(path.read_bytes(), plain)
        self.assertEqual(path.name, media_filename("wc1", 0, "png"))
        self.assertEqual(self._names(), [path.name])
        self.assertEqual(client.calls[0]["timeout"], httpx.Timeout(20.0, connect=10.0))
        self.assertEqual(response.aiter_calls, [im.INBOUND_MEDIA_CHUNK_BYTES])
        self.assertTrue(any(f"saved {path} ({len(plain)} bytes)" in line for line in self.logs))

    def test_over_limit_does_not_retry_or_set_local_path(self) -> None:
        item, cipher = self._item(b"small")
        wc.INBOUND_MEDIA_MAX_BYTES = 20
        response = FakeResponse(_split(cipher, 8), headers={"content-length": str(len(cipher))})
        client = FakeClient([response, FakeResponse([cipher]), FakeResponse([cipher])])
        media = [item]
        _run(self._gateway(client)._download_media(media, "wc-big"))
        self.assertEqual(len(client.calls), 1)
        self.assertNotIn("local_path", media[0])
        self.assertEqual(self._names(), [])
        self.assertEqual(response.pulled, 0)
        self.assertIn(
            f"media wc-big/0: too large ({len(cipher)} bytes), skipped",
            self.logs,
        )
        self.assertFalse(any("download/decrypt failed" in line for line in self.logs))

    def test_http_500_retries_and_bad_ciphertext_does_not(self) -> None:
        item, _cipher = self._item(b"retry-me")
        failures = [FakeResponse([b"nope"], status_code=500) for _ in range(3)]
        client = FakeClient(failures)
        media = [dict(item)]
        _run(self._gateway(client)._download_media(media, "wc-http"))
        self.assertEqual(len(client.calls), 3)
        self.assertNotIn("local_path", media[0])
        self.assertEqual(self.download_sleeps, [2.0, 5.0])
        self.assertTrue(any("download/decrypt failed: http 500" in line for line in self.logs))
        self.assertEqual(self._names(), [])

        self.logs.clear()
        self.download_sleeps.clear()
        self.temps_during_backoff.clear()
        bad = [FakeResponse([b"abc"]) for _ in range(3)]
        client = FakeClient(bad)
        media = [dict(item)]
        _run(self._gateway(client)._download_media(media, "wc-bad"))
        self.assertEqual(len(client.calls), 1)
        self.assertNotIn("local_path", media[0])
        self.assertEqual(self._names(), [])
        self.assertEqual(self.download_sleeps, [])
        self.assertFalse(any("retrying" in line for line in self.logs))
        self.assertTrue(any("download/decrypt failed:" in line for line in self.logs))

    def test_second_attempt_saves_after_a_transport_error(self) -> None:
        plain = b"\xff\xd8\xff" + b"second"
        item, cipher = self._item(plain, pad_size=16)
        client = FakeClient([
            FakeResponse([b"x"], status_code=500),
            FakeResponse(_split(cipher, 4)),
        ])
        media = [item]
        _run(self._gateway(client)._download_media(media, "wc-retry"))
        self.assertEqual(len(client.calls), 2)
        self.assertEqual(self.download_sleeps, [2.0])
        self.assertEqual(self.temps_during_backoff, [[]])
        self.assertEqual(Path(media[0]["local_path"]).read_bytes(), plain)
        self.assertTrue(Path(media[0]["local_path"]).name.endswith(".jpg"))
        self.assertEqual(self._names(), [Path(media[0]["local_path"]).name])

    def test_real_httpx_stream_saves_the_plaintext(self) -> None:
        plain = b"%PDF" + b"-wecom"
        item, cipher = self._item(plain)

        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=cipher)

        async def _download():
            transport = httpx.MockTransport(handler)
            async with httpx.AsyncClient(transport=transport) as client:
                gateway = self._gateway(client)
                media = [item]
                await gateway._download_media(media, "wc-httpx")
                return media

        media = _run(_download())
        self.assertEqual(Path(media[0]["local_path"]).read_bytes(), plain)

    def test_over_limit_still_records_the_inbox_row(self) -> None:
        inbox = Path(self.tmp.name) / "inbox.jsonl"
        previous = wc.INBOX
        wc.INBOX = inbox
        self.addCleanup(setattr, wc, "INBOX", previous)
        wc.INBOUND_MEDIA_MAX_BYTES = 16
        item, cipher = self._item(b"inbox-body")
        response = FakeResponse([cipher], headers={"content-length": str(len(cipher))})
        client = FakeClient(response)
        gateway = self._gateway(client)
        gateway.allow_users = {"user-1"}
        gateway.seen_msgids = set()
        gateway.reqmap = {}
        gateway.open_streams = {}
        gateway.msgs_received = 0
        gateway.msgs_sent = 0
        gateway._save_reqmap = lambda: None
        gateway._mark_seen = lambda msgid: gateway.seen_msgids.add(str(msgid))
        gateway.write_status = lambda: None

        async def _respond(_req_id, _body):
            return {"errcode": 0}

        gateway.respond = _respond
        frame = {
            "headers": {"req_id": "req-1"},
            "body": {
                "msgid": "wc-over",
                "chattype": "single",
                "chatid": "chat-1",
                "from": {"userid": "user-1"},
                "msgtype": "file",
                "file": {"url": item["url"], "aeskey": item["aeskey"]},
            },
        }
        _run(gateway.handle_message_callback(frame))
        row = json.loads(inbox.read_text(encoding="utf-8"))
        self.assertEqual(row["msgid"], "wc-over")
        self.assertNotIn("local_path", row["media"][0])
        self.assertIn("[文件]", row["text"])
        self.assertEqual(gateway.msgs_received, 1)
        self.assertIn(
            f"media wc-over/0: too large ({len(cipher)} bytes), skipped",
            self.logs,
        )
        self.assertEqual(self._names(), [])


def _http_status_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://example.test/obj")
    response = httpx.Response(status, request=request)
    return httpx.HTTPStatusError(f"http {status}", request=request, response=response)


class InboundDownloadRetryTests(unittest.TestCase):
    """The shared retry helper, with no gateway around it."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name)
        self.logs: list[str] = []

    def _recorder(self) -> tuple[list[float], Callable[[float], Awaitable[None]]]:
        slept: list[float] = []

        async def _sleep(seconds: float) -> None:
            slept.append(seconds)
            self.assertEqual(inbound_temp_names(self.directory), [])

        return slept, _sleep

    async def _download(self, client, **kwargs):
        pause = kwargs.pop("sleep", None)
        return await im.download_inbound_cipher(
            client,
            "https://example.test/obj",
            self.directory,
            kwargs.pop("max_bytes", im.INBOUND_MEDIA_MAX_BYTES),
            timeout=kwargs.pop("timeout", httpx.Timeout(5.0)),
            log=self.logs.append,
            label=kwargs.pop("label", "h/0"),
            sleep=pause,
            **kwargs,
        )

    def test_retry_budget_constants(self) -> None:
        self.assertEqual(im.INBOUND_DOWNLOAD_MAX_ATTEMPTS, 3)
        self.assertEqual(im.INBOUND_DOWNLOAD_BACKOFF_SECONDS, (2.0, 5.0))

    def test_classifier_matches_the_transport_policy(self) -> None:
        transport = (
            httpx.RemoteProtocolError,
            httpx.ConnectError,
            httpx.ConnectTimeout,
            httpx.ReadTimeout,
            httpx.WriteTimeout,
            httpx.PoolTimeout,
            httpx.NetworkError,
            httpx.ReadError,
            httpx.ProtocolError,
        )
        for cls in transport:
            with self.subTest(error=cls.__name__):
                self.assertTrue(im.is_retryable_inbound_download_error(cls("net")))
        for exc in (ConnectionResetError("reset"), TimeoutError("timed out"), OSError("os")):
            with self.subTest(error=type(exc).__name__):
                self.assertTrue(im.is_retryable_inbound_download_error(exc))
        self.assertTrue(
            im.is_retryable_inbound_download_error(httpx.RemoteProtocolError("peer"), 200)
        )
        self.assertFalse(im.is_retryable_inbound_download_error(im.InboundMediaTooLarge(9)))
        self.assertFalse(im.is_retryable_inbound_download_error(im.InboundMediaTooLarge(9), 503))
        self.assertFalse(im.is_retryable_inbound_download_error(ValueError("pad")))
        self.assertFalse(im.is_retryable_inbound_download_error(RuntimeError("http 503")))
        for status in (408, 429, 500, 502, 503, 599):
            with self.subTest(status=status):
                self.assertTrue(im.is_retryable_inbound_download_error(_http_status_error(status)))
                self.assertTrue(
                    im.is_retryable_inbound_download_error(RuntimeError("http"), status)
                )
        for status in (400, 401, 403, 404, 409, 422):
            with self.subTest(status=status):
                self.assertFalse(im.is_retryable_inbound_download_error(_http_status_error(status)))
                self.assertFalse(
                    im.is_retryable_inbound_download_error(RuntimeError("http"), status)
                )

    def test_backoff_parameter_can_be_zero(self) -> None:
        slept, pause = self._recorder()
        client = ScriptedClient([
            httpx.ConnectError("connect"),
            httpx.ReadTimeout("read"),
            FakeResponse([b"abc"]),
        ])
        path = _run(self._download(client, backoff_seconds=(0, 0), sleep=pause))
        self.assertEqual(path.read_bytes(), b"abc")
        self.assertEqual(slept, [0.0, 0.0])
        self.assertEqual(len(client.calls), 3)
        self.assertEqual(inbound_temp_names(self.directory), [path.name])
        self.assertTrue(path.name.startswith(".inbound-") and path.name.endswith(".cipher"))

    def test_default_backoff_is_two_then_five(self) -> None:
        slept, pause = self._recorder()
        client = ScriptedClient([
            httpx.PoolTimeout("pool"),
            httpx.ConnectTimeout("connect"),
            FakeResponse([b"zz"]),
        ])
        path = _run(self._download(client, sleep=pause))
        self.assertEqual(path.read_bytes(), b"zz")
        self.assertEqual(slept, [2.0, 5.0])
        self.assertIn("media h/0: download attempt 1/3 failed (pool); retrying", self.logs)
        self.assertIn("media h/0: download attempt 2/3 failed (connect); retrying", self.logs)
        self.assertFalse(any("attempt 3/3" in line for line in self.logs))

    def test_attempts_parameter_of_one_does_not_retry(self) -> None:
        slept, pause = self._recorder()
        client = ScriptedClient([
            httpx.ConnectError("connect"),
            FakeResponse([b"later"]),
        ])
        with self.assertRaises(httpx.ConnectError):
            _run(self._download(client, attempts=1, backoff_seconds=(0, 0), sleep=pause))
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(slept, [])
        self.assertEqual(self.logs, [])
        self.assertEqual(inbound_temp_names(self.directory), [])

    def test_attempts_must_be_positive(self) -> None:
        client = FakeClient([])
        with self.assertRaises(ValueError):
            _run(self._download(client, attempts=0))
        self.assertEqual(client.calls, [])

    def test_non_transport_body_error_is_not_retried(self) -> None:
        slept, pause = self._recorder()
        client = FakeClient([
            BodyErrorResponse([b"partial"], RuntimeError("boom")),
            FakeResponse([b"later"]),
        ])
        with self.assertRaisesRegex(RuntimeError, "boom"):
            _run(self._download(client, backoff_seconds=(0, 0), sleep=pause))
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(slept, [])
        self.assertEqual(self.logs, [])
        self.assertEqual(inbound_temp_names(self.directory), [])

    def test_oserror_mid_body_is_retried(self) -> None:
        slept, pause = self._recorder()
        client = FakeClient([
            BodyErrorResponse([b"part"], ConnectionResetError("reset")),
            FakeResponse([b"os-ok"]),
        ])
        path = _run(self._download(client, backoff_seconds=(0, 0), sleep=pause))
        self.assertEqual(path.read_bytes(), b"os-ok")
        self.assertEqual(len(client.calls), 2)
        self.assertEqual(slept, [0.0])
        self.assertIn("media h/0: download attempt 1/3 failed (reset); retrying", self.logs)
        self.assertEqual(inbound_temp_names(self.directory), [path.name])

    def test_too_large_stops_on_the_first_attempt(self) -> None:
        slept, pause = self._recorder()
        first = FakeResponse([b"abcdef"])
        client = FakeClient([first, FakeResponse([b"ok"])])
        with self.assertRaises(im.InboundMediaTooLarge) as caught:
            _run(self._download(client, max_bytes=3, backoff_seconds=(0, 0), sleep=pause))
        self.assertEqual(caught.exception.nbytes, 6)
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(first.pulled, 1)
        self.assertEqual(slept, [])
        self.assertEqual(self.logs, [])
        self.assertEqual(inbound_temp_names(self.directory), [])

    def test_declared_length_over_the_cap_is_not_retried(self) -> None:
        slept, pause = self._recorder()
        response = FakeResponse([b"unused"], headers={"content-length": "1000"})
        client = FakeClient([response, FakeResponse([b"ok"])])
        with self.assertRaises(im.InboundMediaTooLarge) as caught:
            _run(self._download(client, max_bytes=10, backoff_seconds=(0, 0), sleep=pause))
        self.assertEqual(caught.exception.nbytes, 1000)
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(response.aiter_calls, [])
        self.assertEqual(slept, [])
        self.assertEqual(inbound_temp_names(self.directory), [])

    def test_http_408_and_429_are_retried(self) -> None:
        for status in (408, 429):
            with self.subTest(status=status):
                self.logs.clear()
                slept, pause = self._recorder()
                client = FakeClient([
                    FakeResponse([b"wait"], status_code=status),
                    FakeResponse([b"done"]),
                ])
                path = _run(self._download(
                    client,
                    backoff_seconds=(0, 0),
                    sleep=pause,
                    label=f"s{status}/0",
                ))
                self.assertEqual(path.read_bytes(), b"done")
                self.assertEqual(len(client.calls), 2)
                self.assertEqual(slept, [0.0])
                self.assertIn(
                    f"media s{status}/0: download attempt 1/3 failed (http {status}); retrying",
                    self.logs,
                )
                self.assertEqual(inbound_temp_names(self.directory), [path.name])
                path.unlink()

    def test_cancelled_attempt_leaves_no_temp(self) -> None:
        client = FakeClient([BodyErrorResponse([b"abcdef"], asyncio.CancelledError())])
        with self.assertRaises(asyncio.CancelledError):
            _run(self._download(client, backoff_seconds=(0, 0)))
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(self.logs, [])
        self.assertEqual(inbound_temp_names(self.directory), [])

    def test_real_httpx_503_is_retried_and_404_is_not(self) -> None:
        body = b"from-httpx"
        calls = {"n": 0}

        def handler(_request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            if calls["n"] < 3:
                return httpx.Response(503)
            return httpx.Response(200, content=body)

        async def _success():
            slept: list[float] = []

            async def _sleep(seconds: float) -> None:
                slept.append(seconds)
                self.assertEqual(inbound_temp_names(self.directory), [])

            transport = httpx.MockTransport(handler)
            async with httpx.AsyncClient(transport=transport) as client:
                path = await im.download_inbound_cipher(
                    client,
                    "https://cdn.example.test/obj",
                    self.directory,
                    im.INBOUND_MEDIA_MAX_BYTES,
                    timeout=httpx.Timeout(5.0),
                    log=self.logs.append,
                    label="real/0",
                    sleep=_sleep,
                )
                data = path.read_bytes()
                path.unlink()
                return data, slept

        data, slept = _run(_success())
        self.assertEqual(data, body)
        self.assertEqual(calls["n"], 3)
        self.assertEqual(slept, [2.0, 5.0])
        self.assertEqual(sum("retrying" in line for line in self.logs), 2)
        self.assertTrue(any("attempt 1/3" in line for line in self.logs))
        self.assertTrue(any("attempt 2/3" in line for line in self.logs))
        self.assertEqual(inbound_temp_names(self.directory), [])

        calls["n"] = 0
        self.logs.clear()

        def missing(_request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(404)

        async def _missing():
            transport = httpx.MockTransport(missing)
            async with httpx.AsyncClient(transport=transport) as client:
                await im.download_inbound_cipher(
                    client,
                    "https://cdn.example.test/missing",
                    self.directory,
                    im.INBOUND_MEDIA_MAX_BYTES,
                    timeout=httpx.Timeout(5.0),
                    log=self.logs.append,
                    label="miss/0",
                )

        with self.assertRaises(httpx.HTTPStatusError):
            _run(_missing())
        self.assertEqual(calls["n"], 1)
        self.assertEqual(self.logs, [])
        self.assertEqual(inbound_temp_names(self.directory), [])


if __name__ == "__main__":
    unittest.main()
