"""Relay startup and proxy handshake failure regressions without networking."""
from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('tunnel_relay', ROOT / 'cloudflare-tunnel-plugin/relay.py')
relay = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(relay)


class RelayTests(unittest.TestCase):
    """All required listeners must survive together or fail together."""

    def test_occupied_listener_fails_startup_and_closes_prepared_sockets(self):
        """A failed secondary bind must reach the service supervisor."""
        first, occupied = MagicMock(), MagicMock()
        first.accept.side_effect = RuntimeError('main stayed alive')
        occupied.bind.side_effect = OSError('address already in use')
        with patch.object(relay.socket, 'socket', side_effect=[first, occupied]), patch.object(relay.threading, 'Thread'), patch.object(relay.selectors, 'DefaultSelector'):
            with self.assertRaisesRegex(OSError, 'address already in use'):
                relay.main()
        first.close.assert_called_once()
        occupied.close.assert_called_once()

    def test_accept_failure_reaches_supervisor_and_closes_listeners(self):
        """A runtime listener failure must terminate the service for restart."""
        listeners = [MagicMock() for _ in range(3)]
        selector = MagicMock()
        key = MagicMock(fileobj=listeners[1], data=relay.REGION_HOSTS[1])
        selector.select.return_value = [(key, 1)]
        listeners[1].accept.side_effect = OSError('listener failed')
        with patch.object(relay.socket, 'socket', side_effect=listeners), patch.object(relay.selectors, 'DefaultSelector') as factory:
            factory.return_value.__enter__.return_value = selector
            with self.assertRaisesRegex(OSError, 'listener failed'):
                relay.main()
        for listener in listeners:
            listener.close.assert_called_once()

    def test_client_dispatch_preserves_region_and_closes_failed_worker(self):
        """The selected listener routes clients; thread failure closes the client."""
        listeners = [MagicMock() for _ in range(3)]
        client = MagicMock()
        listeners[1].accept.return_value = (client, ('127.0.0.1', 1))
        key = MagicMock(fileobj=listeners[1], data=relay.REGION_HOSTS[1])
        with patch.object(relay.socket, 'socket', side_effect=listeners), patch.object(relay.selectors, 'DefaultSelector') as factory, patch.object(relay.threading, 'Thread') as worker:
            factory.return_value.__enter__.return_value.select.return_value = [(key, 1)]
            worker.return_value.start.side_effect = RuntimeError('thread unavailable')
            with self.assertRaisesRegex(RuntimeError, 'thread unavailable'):
                relay.main()
        worker.assert_called_once_with(target=relay.handle, args=(client, relay.REGION_HOSTS[1]), daemon=True)
        client.close.assert_called_once()
        for listener in listeners:
            listener.close.assert_called_once()

    def test_proxy_rejection_closes_upstream(self):
        """A failed CONNECT must release its upstream descriptor."""
        upstream = MagicMock()
        upstream.recv.return_value = b'HTTP/1.1 407 Proxy Authentication Required\r\n\r\n'
        with patch.object(relay, 'proxy_target', return_value=('proxy.invalid', 3128, None)), patch.object(relay.socket, 'create_connection', return_value=upstream):
            with self.assertRaises(ConnectionError):
                relay.connect_via_proxy('region1.v2.argotunnel.com')
        upstream.close.assert_called_once()

    def test_proxy_timeout_closes_upstream(self):
        """An interrupted handshake must not leak the connected socket."""
        upstream = MagicMock()
        upstream.recv.side_effect = TimeoutError('timeout')
        with patch.object(relay, 'proxy_target', return_value=('proxy.invalid', 3128, None)), patch.object(relay.socket, 'create_connection', return_value=upstream):
            with self.assertRaises(TimeoutError):
                relay.connect_via_proxy('region1.v2.argotunnel.com')
        upstream.close.assert_called_once()

    def test_successful_proxy_handshake_retains_socket(self):
        """The connection owner receives the open tunnel on success."""
        upstream = MagicMock()
        upstream.recv.return_value = b'HTTP/1.1 200 Connection established\r\n\r\n'
        with patch.object(relay, 'proxy_target', return_value=('proxy.invalid', 3128, None)), patch.object(relay.socket, 'create_connection', return_value=upstream):
            self.assertIs(relay.connect_via_proxy('region1.v2.argotunnel.com'), upstream)
        upstream.close.assert_not_called()


if __name__ == '__main__':
    unittest.main()
