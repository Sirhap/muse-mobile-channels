"""Inbound media streaming, the shared 1GiB cap, and decrypt equivalence.

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
        self.addCleanup(self._restore)

    def _restore(self) -> None:
        self.module.INBOUND_MEDIA_MAX_BYTES = self._saved_cap
        self.module.MEDIA_DIR = self._saved_media_dir
        self.module.log = self._saved_log

    def _names(self) -> list[str]:
        if not self.media_dir.exists():
            return []
        return sorted(path.name for path in self.media_dir.iterdir())


class WeixinDownloadTests(_GatewaySandbox):
    module = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.module = wx

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

    def test_http_error_is_not_retried(self) -> None:
        response = FakeResponse([b"x"], status_code=502)
        client = FakeClient(response)
        media = [self._image(ECB_KEY)]
        _run(wx.Gateway.__new__(wx.Gateway)._download_media(client, media, "err"))
        self.assertEqual(len(client.calls), 1)
        self.assertNotIn("local_path", media[0])
        self.assertEqual(self._names(), [])
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


class WecomDownloadTests(_GatewaySandbox):
    module = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.module = wc

    def setUp(self) -> None:
        super().setUp()
        self._sleep = wc.asyncio.sleep

        async def _no_sleep(_seconds):
            return None

        wc.asyncio.sleep = _no_sleep
        self.addCleanup(setattr, wc.asyncio, "sleep", self._sleep)

    def _item(self, plain: bytes, pad_size: int = 32) -> tuple[dict, bytes]:
        cipher = _encrypt_cbc(plain, CBC_KEY, pad_size)
        token = base64.b64encode(CBC_KEY).decode("ascii")
        return {"kind": "file", "url": "https://example.test/file", "aeskey": token}, cipher

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

    def test_http_error_retries_then_bad_ciphertext_leaves_no_partial(self) -> None:
        item, _cipher = self._item(b"retry-me")
        failures = [FakeResponse([b"nope"], status_code=500) for _ in range(3)]
        client = FakeClient(failures)
        media = [dict(item)]
        _run(self._gateway(client)._download_media(media, "wc-http"))
        self.assertEqual(len(client.calls), 3)
        self.assertNotIn("local_path", media[0])
        self.assertTrue(any("download/decrypt failed:" in line for line in self.logs))
        self.assertEqual(self._names(), [])

        self.logs.clear()
        bad = [FakeResponse([b"abc"]) for _ in range(3)]
        client = FakeClient(bad)
        media = [dict(item)]
        _run(self._gateway(client)._download_media(media, "wc-bad"))
        self.assertEqual(len(client.calls), 3)
        self.assertNotIn("local_path", media[0])
        self.assertEqual(self._names(), [])

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
        gateway.allow_users = set()
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


if __name__ == "__main__":
    unittest.main()
