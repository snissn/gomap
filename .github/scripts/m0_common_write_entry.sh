#!/usr/bin/env bash
# Fixed guarded data-only stage. No Go, SDK, decoder or product execution.
set -euo pipefail
ulimit -f 262144
# Original archive once, fixed URL; token remains inherited environment only.
timeout --signal=TERM --kill-after=5s 90s gh api repos/snissn/gomap/actions/artifacts/13390292576/zip > "$RUNNER_TEMP/four-gate-readback/original.zip"
env -u GH_TOKEN -u GITHUB_TOKEN timeout --signal=TERM --kill-after=5s 30s python3 -B .github/scripts/four_gate_artifact_readback.py
