#!/usr/bin/env bash
set -euo pipefail
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
rg -q '^\[cache-observation\] extracted_module_dir=present$' "$out"
rg -q '^\[cache-observation\] zip=unreadable_or_not_regular$' "$out"
rg -q '^\[cache-observation\] ziphash=absent$' "$out"
if rg -F -q "$tmp" "$out"; then printf 'FAIL: cache path disclosed\n' >&2; exit 1; fi
printf 'PASS: failing stat is silent and classified\n'
