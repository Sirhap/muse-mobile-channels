#!/usr/bin/env bash
# Core-owned hook queue. This script never opens legacy inboxes or outboxes.
set -euo pipefail
source "${HATCH_HOOK_RUNTIME:?hook runtime is required}"

if [[ -z "${MUSE_CORE_HANDOFF_DATABASE:-}" || -z "${MUSE_CHANNEL_MODES:-}" ]]; then
  silent "Core hook handoff is not configured" '{}'
  exit 0
fi

HANDOFF="$("${MUSE_CORE_HANDOFF_PYTHON:-python3}" -m mobile_channels.handoff poll \
  --database "$MUSE_CORE_HANDOFF_DATABASE" --modes "$MUSE_CHANNEL_MODES" \
  --channel "${MUSE_CORE_HANDOFF_CHANNEL:-wecom}")"
if [[ "$HANDOFF" == "null" ]]; then
  silent "No queued Core handoff" '{}'
else
  wake "Core task waiting for hook worker receipt" "$HANDOFF"
fi
