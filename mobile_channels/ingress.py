"""Validated channel ingress, independent of transport and deployment paths."""
from __future__ import annotations

from collections.abc import Collection

from .models import InboundMessage, IngestResult
from .storage import Store


class CoreIngress:
    """Bind an authenticated account and allowlist before accepting user content."""

    def __init__(self, store: Store, channel: str, account_id: str,
                 allowed_senders: Collection[str]):
        """Require an explicit account and nonempty allowlist; empty never means public."""
        if channel not in ('weixin', 'wecom') or not isinstance(account_id, str) or not account_id.strip():
            raise ValueError('a supported channel and explicit account_id are required')
        if not allowed_senders or any(not isinstance(item, str) or not item.strip() for item in allowed_senders):
            raise ValueError('an explicit sender allowlist is required')
        self.store, self.channel, self.account_id = store, channel, account_id
        self.allowed_senders = frozenset(allowed_senders)

    def receive(self, message_id: str, sender_id: str, chat_id: str,
                chat_type: str, text: str, received_at: float | None = None) -> IngestResult:
        """Commit one authenticated text message without changing its verified route.

        Adapters must pass transport-authenticated sender/chat identifiers. Media
        references and legacy commands require their own explicit adapter; they
        must not silently be converted to a text task by this entrypoint.
        """
        if sender_id not in self.allowed_senders:
            raise PermissionError('sender is not authorized for this account')
        if chat_type not in ('single', 'group'):
            raise ValueError('chat_type must be single or group')
        if not isinstance(text, str) or not text.strip():
            raise ValueError('nonempty text is required')
        if self.channel == 'weixin' and chat_type != 'single':
            raise ValueError('the iLink adapter currently supports private conversations only')
        return self.store.ingest(InboundMessage(
            self.channel, self.account_id, chat_type, chat_id,
            message_id, sender_id, text, received_at,
        ))
