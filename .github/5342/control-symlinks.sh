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
base="$(cd "$(dirname "$0")" && pwd)"
tmp="$(mktemp -d "${TMPDIR:?set task-owned TMPDIR}/5342-control.XXXXXXXX")"
printf 'retained_control_dir=%s\n' "$tmp"
mkdir -p "$tmp/bin" "$tmp/cache/github.com/antlabs" "$tmp/cache/cache/download/github.com/antlabs/stl/@v" "$tmp/target"
printf '#!/usr/bin/env bash\nprintf "%%s\\n" "$TEST_MODULE_CACHE"\n' > "$tmp/bin/go"
chmod 755 "$tmp/bin/go"
download="$tmp/cache/cache/download/github.com/antlabs/stl/@v"
printf private > "$tmp/target/file"
ln -s "$tmp/target" "$tmp/cache/github.com/antlabs/stl@v0.0.2"
ln -s "$tmp/target/file" "$download/v0.0.2.zip"
ln -s "$tmp/target/missing" "$download/v0.0.2.ziphash"
env PATH="$tmp/bin:$PATH" TEST_MODULE_CACHE="$tmp/cache" SETUP_GO_CACHE_HIT=true bash "$base/candidate/scripts/observe_read_snapshot_cache.sh" >"$tmp/out" 2>&1
for expected in 'extracted_module_dir=symlink' 'zip=symlink' 'ziphash=symlink'; do
  grep -q -F "[cache-observation] $expected" "$tmp/out"
done
require_no_match -F -q -- "$tmp" "$tmp/out"
require_no_match -q -- 'bytes=' "$tmp/out"
printf 'PASS: directory, file and dangling symlink leaves are classified without target size\n'
