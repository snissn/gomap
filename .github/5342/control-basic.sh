#!/usr/bin/env bash
set -euo pipefail
base="$(cd "$(dirname "$0")" && pwd)"
tmp="$(mktemp -d "${TMPDIR:?set task-owned TMPDIR}/5342-basic.XXXXXXXX")"
printf 'retained_control_dir=%s\n' "$tmp"
mkdir -p "$tmp/bin" "$tmp/cache/github.com/antlabs/stl@v0.0.2" "$tmp/cache/cache/download/github.com/antlabs/stl/@v"
cat > "$tmp/bin/go" <<'STUB'
#!/usr/bin/env bash
if [[ ${TEST_GO_FAIL-} == yes ]]; then exit 1; fi
printf '%s\n' "$TEST_MODULE_CACHE"
STUB
chmod 755 "$tmp/bin/go"
download="$tmp/cache/cache/download/github.com/antlabs/stl/@v"
printf abc > "$download/v0.0.2.zip"
printf xyz12 > "$download/v0.0.2.ziphash"
env PATH="$tmp/bin:$PATH" TEST_MODULE_CACHE="$tmp/cache" SETUP_GO_CACHE_HIT=surprise bash "$base/candidate/scripts/observe_read_snapshot_cache.sh" > "$tmp/present.log" 2>&1
for expected in 'setup_go_cache_hit=unknown' 'extracted_module_dir=present' 'zip=present bytes=3' 'ziphash=present bytes=5'; do rg -q -x -F "[cache-observation] $expected" "$tmp/present.log"; done
mkdir -p "$tmp/empty"
env PATH="$tmp/bin:$PATH" TEST_MODULE_CACHE="$tmp/empty" SETUP_GO_CACHE_HIT=false bash "$base/candidate/scripts/observe_read_snapshot_cache.sh" > "$tmp/absent.log" 2>&1
for expected in 'setup_go_cache_hit=false' 'extracted_module_dir=absent' 'zip=absent' 'ziphash=absent'; do rg -q -x -F "[cache-observation] $expected" "$tmp/absent.log"; done
env PATH="$tmp/bin:$PATH" TEST_GO_FAIL=yes TEST_MODULE_CACHE="$tmp/cache" bash "$base/candidate/scripts/observe_read_snapshot_cache.sh" > "$tmp/unavailable.log" 2>&1
rg -q -x -F '[cache-observation] module_cache=unavailable' "$tmp/unavailable.log"
if rg -F -q "$tmp" "$tmp/present.log" "$tmp/absent.log" "$tmp/unavailable.log"; then echo 'FAIL: path disclosed' >&2; exit 1; fi
printf 'PASS: real stat, absent and unavailable observations\n'
