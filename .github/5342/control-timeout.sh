#!/usr/bin/env bash
set -euo pipefail
# Only grep exit 1 proves no match; errors are control failures.
require_no_match() {
  if grep "$@"; then
    echo 'FAIL: forbidden output found' >&2
    return 1
  else
    rc=$?
    if [[ $rc -ne 1 ]]; then
      echo "FAIL: grep error $rc" >&2
      return 1
    fi
  fi
}
# Run after review: simulate hung Go; keep the same timeout command as workflow.
tmp="$(mktemp -d "${TMPDIR:?set task-owned TMPDIR}/5342-control.XXXXXXXX")"
printf 'retained_control_dir=%s\n' "$tmp"
mkdir -p "$tmp/bin"
cat > "$tmp/bin/go" <<'STUB'
#!/usr/bin/env bash
sleep 30
STUB
chmod 755 "$tmp/bin/go"
base="$(cd "$(dirname "$0")" && pwd)"
start=$SECONDS
if PATH="$tmp/bin:$PATH" timeout --kill-after=1s 5s bash "$base/candidate/scripts/observe_read_snapshot_cache.sh" >"$tmp/output" 2>&1; then
  echo '[cache-observation] status=complete' >>"$tmp/output"
else
  echo '[cache-observation] status=incomplete' >>"$tmp/output"
fi
elapsed=$((SECONDS-start))
grep -q '^\[cache-observation\] status=incomplete$' "$tmp/output"
[[ $elapsed -lt 15 ]] || { echo 'FAIL: timeout control exceeded allowance' >&2; exit 1; }
require_no_match -F -q -- "$tmp" "$tmp/output"
echo 'PASS: hung observation is incomplete and returns to caller'
