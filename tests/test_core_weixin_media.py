"""Exercise Core WeChat media at the real upload and send-message boundary."""
from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, Mock

from mobile_channels.delivery import DeliveryStore
from mobile_channels.storage import Store
from test_core_fix_gateway_wave2 import AdapterCase


class CoreWeixinMediaTests(AdapterCase):
    """Use temporary files and SQLite with fake HTTP responses only."""

    def setUp(self):
        """Keep real adapter encoding and sending while replacing HTTP."""
        super().setUp()
        self.gateway = self.gateways["weixin"]
        self.gateway.send_text = self.modules["weixin"].Gateway.send_text.__get__(self.gateway)
        self.requests = []

        async def post(client, base_url, endpoint, body, token, timeout):
            self.requests.append((endpoint, body))
            if endpoint.endswith("getuploadurl"):
                return {"upload_full_url": "https://cdn.invalid/upload"}
            return {"ret": 0}

        self.gateway._post = AsyncMock(side_effect=post)
        response = Mock(headers={"x-encrypted-param": "encrypted-download"})
        self.client = Mock(post=AsyncMock(return_value=response))
        self.credentials = {"base_url": "https://ilink.invalid", "token": "sandbox-token"}

    def complete(self, text="caption", media=None):
        """Generate one routed Core reply without contacting Muse."""
        binding = self.gateway._core_binding
        binding.receive(message_id="job1", sender_id="alice", chat_id="alice",
                        chat_type="single", text="make media", context_token="bound-token")
        with Store(self.database) as store:
            claim = store.claim("executor")
            store.begin_submission(claim)
            store.mark_submitted(claim, "run-1")
            store.complete(claim, text, media=media or [])

    async def deliver_until(self, state):
        """Drive the real delivery loop until the current message reaches state."""
        await self.run_delivery("weixin", lambda: self.rows("outbound_messages")[0]["state"] == state,
                                client=self.client, credentials=self.credentials)

    def sent_messages(self):
        """Return only platform send attempts, excluding CDN preparation."""
        return [body["msg"] for endpoint, body in self.requests if endpoint.endswith("sendmessage")]

    async def test_captionless_image_video_and_file_use_the_original_reply_context(self):
        """A pure media result sends three media bubbles and no empty text."""
        media = []
        for name, kind in (("image.png", "image"), ("clip.mp4", "video"), ("report.pdf", "file")):
            path = self.root / name
            path.write_bytes(b"temporary output")
            media.append({"kind": kind, "path": str(path), "filename": name})
        self.complete(text="", media=media)
        await self.deliver_until("delivered")
        messages = self.sent_messages()
        self.assertEqual([message["item_list"][0]["type"] for message in messages], [2, 5, 4])
        self.assertTrue(all(message["context_token"] == "bound-token" for message in messages))
        self.assertTrue(all(message["to_user_id"] == "alice" for message in messages))
        self.assertEqual(len({message["client_id"] for message in messages}), 3)

    async def test_uploads_finish_before_caption_and_each_same_kind_file_has_its_own_id(self):
        """Text and both files must use deterministic, distinct platform IDs."""
        second = self.root / "other.png"
        second.write_bytes(b"image two")
        self.complete(media=[{"kind": "image", "path": str(self.png)},
                             {"kind": "image", "path": str(second)}])
        await self.deliver_until("delivered")
        endpoints = [endpoint.rsplit("/", 1)[-1] for endpoint, _ in self.requests]
        self.assertEqual(endpoints, ["getuploadurl", "getuploadurl", "sendmessage", "sendmessage", "sendmessage"])
        messages = self.sent_messages()
        self.assertEqual([message["item_list"][0]["type"] for message in messages], [1, 2, 2])
        self.assertEqual(len({message["client_id"] for message in messages}), 3)

    async def test_upload_failure_retries_before_any_text_or_media_send(self):
        """An unsuccessful CDN preparation leaves the part safe to retry."""
        self.client.post = AsyncMock(side_effect=TimeoutError("CDN unavailable"))
        self.complete(media=[{"kind": "image", "path": str(self.png)}])
        await self.run_delivery("weixin", lambda: self.rows("outbound_parts")[0]["state"] == "retry_wait",
                                client=self.client, credentials=self.credentials)
        self.assertEqual(self.sent_messages(), [])

    async def test_slow_upload_budget_extends_only_the_owned_sending_lease(self):
        """The CDN's existing 300s timeout fits inside a live Core lease."""
        response = self.client.post.return_value

        async def inspect_lease(*args, **kwargs):
            with Store(self.database) as store:
                row = store.connection.execute(
                    "SELECT state,lease_until FROM outbound_parts").fetchone()
                self.assertEqual(row["state"], "sending")
                self.assertGreater(row["lease_until"] - store.clock(), 600)
            return response

        self.client.post.side_effect = inspect_lease
        self.complete(media=[{"kind": "image", "path": str(self.png)}])
        await self.deliver_until("delivered")

    async def test_compression_uses_the_existing_preparation_and_media_encoding(self):
        """A prepared JPEG is uploaded as an image, keeping its actual size."""
        compressed = self.root / "prepared.jpg"
        compressed.write_bytes(b"compressed bytes")
        self.gateway._prepare_file_for_send = AsyncMock(return_value=(str(compressed), True))
        self.complete(media=[{"kind": "image", "path": str(self.png)}])
        await self.deliver_until("delivered")
        self.gateway._prepare_file_for_send.assert_awaited_once_with(str(self.png.resolve()))
        upload_request = next(body for endpoint, body in self.requests if endpoint.endswith("getuploadurl"))
        self.assertEqual(upload_request["rawsize"], len(b"compressed bytes"))
        self.assertEqual(upload_request["media_type"], 1)

    async def test_partial_send_is_unknown_and_operator_retry_reuses_ids(self):
        """A lost file response never resends automatically after the caption."""
        self.complete(media=[{"kind": "image", "path": str(self.png)}])
        original = self.gateway._post.side_effect
        failed_once = False

        async def fail_media_once(*args):
            nonlocal failed_once
            endpoint, body = args[2], args[3]
            if (endpoint.endswith("sendmessage") and body["msg"]["item_list"][0]["type"] != 1
                    and not failed_once):
                failed_once = True
                self.requests.append((endpoint, body))
                raise TimeoutError("response lost")
            return await original(*args)

        self.gateway._post.side_effect = fail_media_once
        await self.deliver_until("delivery_unknown")
        first_ids = [message["client_id"] for message in self.sent_messages()]
        self.assertEqual(len(first_ids), 2)
        with Store(self.database) as store:
            DeliveryStore(store).resolve_unknown(self.rows("outbound_parts")[0]["id"], "retry_wait",
                                                 "sandbox operator requested retry")
        await self.deliver_until("delivered")
        self.assertEqual([message["client_id"] for message in self.sent_messages()][2:], first_ids)

    async def test_credential_alias_is_rejected_without_upload_or_caption(self):
        """The shared local-file policy applies before the upload begins."""
        secret = self.root / ".config" / "MuseGadgetPair" / "pairing.json"
        secret.parent.mkdir(parents=True)
        secret.write_text("FAKE_TEST_SECRET=private")
        alias = self.root / "report.txt"
        alias.symlink_to(secret)
        self.complete(media=[{"kind": "file", "path": str(alias)}])
        await self.deliver_until("rejected")
        self.client.post.assert_not_called()
        self.assertFalse(any(endpoint.endswith("getuploadurl") for endpoint, _ in self.requests))


if __name__ == "__main__":
    unittest.main()
