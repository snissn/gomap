#!/usr/bin/env bash
set -euo pipefail
qualification_dir=${1:?frozen capture directory required}
export GOROOT=/home/mikers/.gvm/gos/go1.26.3 GOWORK=off GOMAXPROCS=4 GOMEMLIMIT=1GiB TREEDB_HOT_PATH_STATS=1
export TMPDIR="$qualification_dir/tmp"
unset TREEDB_OWNED_VLOG_FIXTURE TREEDB_OWNED_VLOG_FIXTURE_EXPORT TREEDB_OWNED_VLOG_VALIDATE_ONLY
verify_freeze() {
 python3 - "$qualification_dir" <<'PY'
from pathlib import Path
import hashlib,json,sys
p=Path(sys.argv[1]); f=json.loads((p/'logs/repair-source-freeze.json').read_text())
expected={'base':'a68a84c7195e0c5d8c339343a385b40acf818d03','previous':'ab2e9124fcca72710b896cf78577fa6bd37a11b0','candidate':'9ab02b48ce2397410940a7e42cc22bbf95a96c5a'}
assert f['commits']==expected
assert f['toolchain']=='go version go1.26.3 linux/amd64'
for name,h in f['input_sha256'].items(): assert hashlib.sha256((p/name).read_bytes()).hexdigest()==h,name
required={'qualify-mmap-repair.sh','analyze-mmap-repair.py','mmap_qualification_overlay_test.go','logs/fixture-manifest.json','bin/valuelog-base.test','bin/valuelog-previous.test','bin/valuelog-candidate.test','bin/treedb-base.test','bin/treedb-candidate.test'}
assert required <= set(f['input_sha256']),required-set(f['input_sha256'])
manifest=json.loads((p/'logs/fixture-manifest.json').read_text())
files={str(x.relative_to(p/'fixture')) for x in (p/'fixture').rglob('*') if x.is_file()}
assert files==set(manifest['files'])
for name,item in manifest['files'].items():
 x=p/'fixture'/name
 assert x.stat().st_size==item['bytes'] and hashlib.sha256(x.read_bytes()).hexdigest()==item['sha256'],name
print(json.dumps({'source_freeze_sha256':hashlib.sha256((p/'logs/repair-source-freeze.json').read_bytes()).hexdigest(),'inputs':f['input_sha256'],'fixture':manifest},sort_keys=True))
PY
}
test ! -e "$qualification_dir/logs/repair-freeze-before.txt"
verify_freeze > "$qualification_dir/logs/repair-freeze-before.txt"
finish_capture() {
 capture_status=$?
 trap - EXIT
 set +e
 verify_freeze > "$qualification_dir/logs/repair-freeze-after.txt" 2> "$qualification_dir/logs/repair-freeze-after.err.txt"
 freeze_status=$?
 if (( freeze_status != 0 )); then capture_status=1; fi
 if (( capture_status == 0 )); then
  python3 "$qualification_dir/analyze-mmap-repair.py" "$qualification_dir/logs" > "$qualification_dir/logs/repair-parser-result.txt" 2>&1
  capture_status=$?
 fi
 printf 'capture_exit_status=%s\nfreeze_exit_status=%s\n' "$capture_status" "$freeze_status" > "$qualification_dir/logs/repair-capture-status.txt"
 exit "$capture_status"
}
trap finish_capture EXIT
for repetition in 1 2 3 4 5; do
 revisions=(base previous candidate)
 if (( repetition % 2 == 0 )); then revisions=(candidate previous base); fi
 for revision in "${revisions[@]}"; do
  internal_regex='^(BenchmarkFileReadAppendCompressedFallback|BenchmarkFileReadAppendOwned)$/^(nil_dst|reused_dst|mmap_decode|cold_open_map)$'
  overlay_regex='^BenchmarkOwnedMmap(BudgetDenied512|ConcurrentFirstAdmission)$'
  if [[ $revision == previous ]]; then
   internal_regex='^BenchmarkFileReadAppendOwned$/^cold_open_map$'
  fi
  if [[ $revision == base ]]; then overlay_regex='^BenchmarkOwnedMmapBudgetDenied512$'; fi
  /usr/bin/time -v -o "$qualification_dir/logs/repair-internal-$revision-$repetition.rss.txt" \
   "$qualification_dir/bin/valuelog-$revision.test" -test.run '^$' -test.bench "$internal_regex" -test.benchtime 1s -test.count 1 -test.benchmem \
   > "$qualification_dir/logs/repair-internal-$revision-$repetition.txt" 2> "$qualification_dir/logs/repair-internal-$revision-$repetition.stderr.txt"
  python3 "$qualification_dir/analyze-mmap-repair.py" "$qualification_dir/logs" --cell "$revision" internal "$repetition"
  /usr/bin/time -v -o "$qualification_dir/logs/repair-overlay-$revision-$repetition.rss.txt" \
   "$qualification_dir/bin/valuelog-$revision.test" -test.run '^$' -test.bench "$overlay_regex" -test.benchtime 1s -test.count 1 -test.benchmem \
   > "$qualification_dir/logs/repair-overlay-$revision-$repetition.txt" 2> "$qualification_dir/logs/repair-overlay-$revision-$repetition.stderr.txt"
  python3 "$qualification_dir/analyze-mmap-repair.py" "$qualification_dir/logs" --cell "$revision" overlay "$repetition"
  if [[ $revision != previous ]]; then
   TREEDB_OWNED_VLOG_FIXTURE="$qualification_dir/fixture" \
    /usr/bin/time -v -o "$qualification_dir/logs/repair-public-$revision-$repetition.rss.txt" \
    "$qualification_dir/bin/treedb-$revision.test" -test.run '^$' -test.bench '^BenchmarkDBOwnedValueLogRoute$' -test.benchtime 1s -test.count 1 -test.benchmem \
    > "$qualification_dir/logs/repair-public-$revision-$repetition.txt" 2> "$qualification_dir/logs/repair-public-$revision-$repetition.stderr.txt"
   python3 "$qualification_dir/analyze-mmap-repair.py" "$qualification_dir/logs" --cell "$revision" public "$repetition"
  fi
 done
done
