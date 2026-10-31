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
# Local control only, after review; simulates stat failure on an existing regular file.
base="$(cd "$(dirname "$0")" && pwd)"
tmp="$(mktemp -d "${TMPDIR:?set task-owned TMPDIR}/5342-control.XXXXXXXX")"
printf 'retained_control_dir=%s\n' "$tmp"
mkdir -p "$tmp/bin" "$tmp/cache/cache/download/github.com/antlabs/stl/@v" "$tmp/cache/github.com/antlabs/stl@v0.0.2"
cat > "$tmp/bin/go" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "$TEST_MODULE_CACHE"
STUB
cat > "$tmp/bin/stat" <<'STUB'
#!/usr/bin/env bash
printf 'simulated stat error: %s\n' "$*" >&2
exit 1
STUB
chmod 755 "$tmp/bin/go" "$tmp/bin/stat"
touch "$tmp/cache/cache/download/github.com/antlabs/stl/@v/v0.0.2.zip"
out="$tmp/output"
env PATH="$tmp/bin:$PATH" TEST_MODULE_CACHE="$tmp/cache" SETUP_GO_CACHE_HIT=true bash "$base/candidate/scripts/observe_read_snapshot_cache.sh" >"$out" 2>&1
grep -q '^\[cache-observation\] extracted_module_dir=present$' "$out"
grep -q '^\[cache-observation\] zip=unreadable_or_not_regular$' "$out"
grep -q '^\[cache-observation\] ziphash=absent$' "$out"
require_no_match -F -q -- "$tmp" "$out"
printf 'PASS: failing stat is silent and classified\n'
