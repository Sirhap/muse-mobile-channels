#!/usr/bin/env bash
# Core-owned hook queue. This script never opens legacy inboxes or outboxes.
set -euo pipefail
source "${HATCH_HOOK_RUNTIME:?hook runtime is required}"

# Hooks run without service-specific variables and may start outside workspace.
# Preserve explicit paths and existing import paths for custom installations.
: "${MUSE_CORE_HANDOFF_DATABASE:=$HOME/workspace/state/core.sqlite3}"
: "${MUSE_CHANNEL_MODES:=$HOME/workspace/state/channel-modes.json}"
: "${MUSE_CORE_HANDOFF_PYTHON:=$HOME/muse-test-venv/bin/python}"
export PYTHONPATH="$HOME/workspace${PYTHONPATH:+:$PYTHONPATH}"

HANDOFF="$("${MUSE_CORE_HANDOFF_PYTHON:-python3}" -m mobile_channels.handoff poll \
  --database "$MUSE_CORE_HANDOFF_DATABASE" --modes "$MUSE_CHANNEL_MODES" \
  --channel "${MUSE_CORE_HANDOFF_CHANNEL:-wecom}")"
if [[ "$HANDOFF" == "null" ]]; then
  silent "No queued Core handoff" '{}'
else
  wake "Core task waiting for hook worker receipt" "$HANDOFF"
fi
