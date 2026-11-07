#!/usr/bin/env python3
"""Run supported sandbox checks in isolated processes on macOS and Linux.

Legacy date-stamped scripts may execute assertions during import and mutate
module globals. Running each file in a subprocess preserves their isolation.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CHECKS = (
    'tests/test_core_storage.py',
    'tests/test_core_runtime.py',
    'tests/test_core_fold.py',
    'tests/test_gadget_executor.py',
    'tests/test_gadget_server_cancel.py',
    'tests/test_doctor.py',
    'tests/test_service_plan.py',
    'tests/test_core_fix_exec_media.py',
    'tests/test_core_fix_exec_multi_reply.py',
    'tests/test_core_fix_exec_cancel_rebind.py',
    'tests/test_core_fix_exec_turn_ts.py',
    'tests/test_core_fix_gateway_wave2.py',
    'tests/test_core_weixin_media.py',
    'tests/test_late_replies.py',
    'tests/test_reply_updates_integration.py',
    'tests/test_core_hook_handoff.py',
    'tests/test_core_fix_store_receipts.py',
    'tests/test_core_fix_store_runtime_delivery.py',
    'tests/test_core_fix_approval_consume.py',
    'tests/test_core_fix_approval_decisions.py',
    'tests/test_core_fix_approval_notify.py',
    'tests/test_core_ingress_rejection.py',
    'tests/test_core_late_delivery.py',
    'tests/test_executor_install.py',
    'tests/test_core_readiness.py',
    'tests/test_gateway_core_binding.py',
    'tests/test_channel_modes.py',
    'tests/test_recovery_ops.py',
    'tests/test_maintenance_migration.py',
    'tests/test_bridge_recovery_integration.py',
    'tests/test_approval_security.py',
    'approval-relay/test_approval_policy.py',
    'approval-relay/test_approval_relay.py',
    'approval-relay/test_approval_card_group_20261009.py',
    'tests/test_wecom_approval_card_20261009.py',
    'tests/test_approval_slash_aliases_20261010.py',
    'tests/test_slash_commands_audit_20261015.py',
    'tests/test_core_delivery.py',
    'tests/test_shadow_inventory.py',
    'tests/test_tunnel_relay.py',
    'tests/test_bridge_runtime.py',
    'tests/test_bridge_credential_health.py',
    'tests/test_credential_diagnostics.py',
    'tests/test_bridge_reconnect_policy.py',
    'tests/test_bridge_diversion_health.py',
    'tests/test_bridge_submission_recovery.py',
    'tests/test_bridge_check_health.py',
    'tests/test_weixin_inbound_checkpoint.py',
    'tests/test_channel_common.py',
    'tests/test_wecom_durable_handoff.py',
    'tests/test_bridge_queue_recovery.py',
    'tests/test_approval_notify_resilience.py',
    'tests/test_approval_credential_health.py',
    'tests/test_channel_restore_dropins.py',
    'tests/test_death_watch_activity.py',
    'tests/test_worker_death_signal_20261015.py',
    'tests/test_worker_lifecycle_scenario.py',
    'tests/test_worker_registration_history.py',
    'tests/test_worker_verdict_binding.py',
    'tests/test_hook_unknown_worker.py',
    'tests/test_gateway_worker_review.py',
    'tests/test_death_watch_reconcile.py',
    'tests/test_sonnet_fix_weixin_20261017.py',
)


def main() -> int:
    """Fail visibly on missing dependencies, timeouts or failed assertions."""
    environment = dict(os.environ, PYTHONPATH=str(ROOT), TMPDIR='/tmp',
                       PYTHONDONTWRITEBYTECODE='1')
    failures = []
    for relative in CHECKS:
        print(f'Running {relative}', flush=True)
        try:
            result = subprocess.run([sys.executable, str(ROOT / relative)], cwd=ROOT,
                                    env=environment, timeout=120, check=False)
            if result.returncode:
                failures.append(relative)
        except (OSError, subprocess.TimeoutExpired) as error:
            print(f'{relative}: {error}', file=sys.stderr)
            failures.append(relative)
    print(f'{len(CHECKS) - len(failures)}/{len(CHECKS)} sandbox suites passed', flush=True)
    if failures:
        print('Failed: ' + ', '.join(failures), file=sys.stderr)
    return int(bool(failures))


if __name__ == '__main__':
    raise SystemExit(main())
