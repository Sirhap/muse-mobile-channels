"""Native bridge imports and state storage must work without production files."""
from __future__ import annotations

import builtins
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

SOURCE = Path(__file__).resolve().parents[1] / 'native-bridge/native_bridge.py'


def load_bridge():
    """Import the actual bridge through its public module entrypoint."""
    spec = importlib.util.spec_from_file_location('bridge_runtime_test', SOURCE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class BridgeRuntimeTests(unittest.TestCase):
    """Keep runtime dependencies outside module import and state helpers."""

    def test_import_does_not_read_config_or_import_platform(self):
        """A clean machine can import the bridge without files or Muse SDK."""
        original = builtins.__import__

        def reject_platform(name, *args, **kwargs):
            if name == 'gw2' or name.startswith('muse_cli'):
                raise AssertionError('platform import during module load')
            return original(name, *args, **kwargs)

        before = list(sys.path)
        with patch('builtins.open', side_effect=AssertionError('file read during import')), patch('builtins.__import__', side_effect=reject_platform):
            bridge = load_bridge()
        self.assertIsNone(bridge.CFG)
        self.assertEqual(sys.path, before)

    def test_explicit_configuration_and_patch_compatibility(self):
        """Loading validates a requested file and supports sandbox overrides."""
        bridge = load_bridge()
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / 'config.json'
            expected = {'channels': {'test': {'bot_state': directory}}}
            config.write_text(json.dumps(expected))
            self.assertEqual(bridge.load_config(config), expected)
            bridge.CFG = {'channels': {'patched': {}}}
            self.assertEqual(bridge.runtime_config(), bridge.CFG)

    def test_missing_configuration_fails_only_when_requested(self):
        """Runtime use reports missing config rather than silently using defaults."""
        bridge = load_bridge()
        with tempfile.TemporaryDirectory() as directory, patch.dict('os.environ', {'NATIVE_BRIDGE_CONFIG': str(Path(directory) / 'absent.json')}):
            with self.assertRaises(FileNotFoundError):
                bridge.runtime_config()

    def test_state_round_trip_closes_write_before_replace(self):
        """Atomic state replacement happens only after its file is closed."""
        bridge = load_bridge()
        with tempfile.TemporaryDirectory() as directory:
            bridge.STATE_F = str(Path(directory) / 'state.json')
            original_open, original_replace = builtins.open, os.replace
            opened = []

            def tracked_open(*args, **kwargs):
                handle = original_open(*args, **kwargs)
                opened.append(handle)
                return handle

            def checked_replace(source, destination):
                self.assertTrue(opened[-1].closed)
                original_replace(source, destination)

            with patch('builtins.open', side_effect=tracked_open), patch.object(bridge.os, 'replace', side_effect=checked_replace):
                bridge.save_state({'channels': {'test': {'turns': []}}})
            self.assertEqual(bridge.load_state(), {'channels': {'test': {'turns': []}}})
            self.assertFalse(Path(bridge.STATE_F + '.tmp').exists())

    def test_connect_supports_injected_platform_client(self):
        """A supplied client bypasses optional SDK imports for contract tests."""
        bridge = load_bridge()
        bridge.GW2 = MagicMock()
        token = {'vm_id': 'sandbox', 'access_token': 'unused', 'hatch_token': 'unused'}
        with patch.dict('os.environ', {'NATIVE_TOKEN_JSON': json.dumps(token)}):
            gateway = bridge.connect()
        self.assertIs(gateway, bridge.GW2.return_value)
        gateway.call_json.assert_called_once_with('sessions.list', timeout=15)

    def test_invalid_configuration_does_not_replace_injected_config(self):
        """Invalid input cannot overwrite a previously validated configuration."""
        bridge = load_bridge()
        bridge.CFG = {'channels': {'sandbox': {}}}
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / 'config.json'
            config.write_text('{"channels": []}')
            with self.assertRaises(ValueError):
                bridge.load_config(config)
        self.assertEqual(bridge.CFG, {'channels': {'sandbox': {}}})


if __name__ == '__main__':
    unittest.main()
