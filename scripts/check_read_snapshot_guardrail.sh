#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
guardrail_test='TestRunBenchmark_ReadSnapshotAppendOnlyGuardrail'
guardrail_run_regex="^${guardrail_test}$"

run_once() {
	local output_file="$1"
	cd "$repo_root/cmd/unified_bench"
	GOWORK=off GOMEMLIMIT=4GiB GOMAXPROCS=2 \
		go test -json -p 1 . -run "$guardrail_run_regex" -count=1 | tee "$output_file"
	local test_status="${PIPESTATUS[0]}"
	if [[ "$test_status" -ne 0 ]]; then
		return "$test_status"
	fi
	# Guard against false-green runs where no tests matched -run.
	if ! grep -Eq "\"Action\":\"pass\".*\"Test\":\"${guardrail_test}\"|\"Test\":\"${guardrail_test}\".*\"Action\":\"pass\"" "$output_file"; then
		echo "[guardrail] ERROR: target test '${guardrail_test}' did not run." >&2
		return 3
	fi
	return 0
}

tmp1="$(mktemp)"
tmp2="$(mktemp)"
trap 'rm -f "$tmp1" "$tmp2"' EXIT

if ! run_once "$tmp1"; then
	echo "[guardrail] first attempt failed; retrying once..." >&2
	if run_once "$tmp2"; then
		echo "[guardrail] ERROR: flaky guardrail run (first attempt failed, second passed)." >&2
		exit 4
	fi
	echo "[guardrail] ERROR: guardrail failed on both attempts." >&2
	exit 5
fi
