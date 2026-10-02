#!/usr/bin/env bash
set -euo pipefail
qualification_dir=${1:?pass the frozen qualification directory}
export GOROOT=/home/mikers/.gvm/gos/go1.26.3 GOWORK=off GOMAXPROCS=4 GOMEMLIMIT=1GiB TREEDB_HOT_PATH_STATS=1
export TMPDIR="$qualification_dir/tmp"
unset TREEDB_OWNED_VLOG_FIXTURE TREEDB_OWNED_VLOG_FIXTURE_EXPORT TREEDB_OWNED_VLOG_VALIDATE_ONLY
check_fixture() {
  python3 - "$qualification_dir" <<'PY'
from pathlib import Path
import hashlib,json,sys
root=Path(sys.argv[1]); manifest=json.loads((root/'logs/fixture-manifest.json').read_text())
files={str(p.relative_to(root/'fixture')) for p in (root/'fixture').rglob('*') if p.is_file()}
assert files==set(manifest['files']), 'fixture file inventory changed'
for name,record in manifest['files'].items():
    path=root/'fixture'/name
    assert path.stat().st_size==record['bytes'] and hashlib.sha256(path.read_bytes()).hexdigest()==record['sha256'], name
print('frozen fixture hashes verified')
PY
}
check_fixture > "$qualification_dir/logs/fixture-before.txt"
for repetition in 1 2 3 4 5; do
  revisions=(base candidate)
  if (( repetition % 2 == 0 )); then revisions=(candidate base); fi
  for revision in "${revisions[@]}"; do
    /usr/bin/time -v -o "$qualification_dir/logs/internal-$revision-$repetition.rss.txt" \
      "$qualification_dir/bin/valuelog-$revision.test" -test.run '^$' \
      -test.bench '^(BenchmarkFileReadAppendOwned|BenchmarkFileReadAppendCompressedFallback|BenchmarkValueLogRandomReadGroupedFrame_ReadUnsafeTo)$' \
      -test.benchtime 1s -test.count 1 -test.benchmem \
      > "$qualification_dir/logs/internal-$revision-$repetition.txt" 2>&1
    TREEDB_OWNED_VLOG_FIXTURE="$qualification_dir/fixture" \
      /usr/bin/time -v -o "$qualification_dir/logs/public-route-$revision-$repetition.rss.txt" \
      "$qualification_dir/bin/treedb-$revision.test" -test.run '^$' \
      -test.bench '^BenchmarkDBOwnedValueLogRoute$' -test.benchtime 1s -test.count 1 -test.benchmem \
      > "$qualification_dir/logs/public-route-$revision-$repetition.txt" 2>&1
    /usr/bin/time -v -o "$qualification_dir/logs/public-guard-$revision-$repetition.rss.txt" \
      "$qualification_dir/bin/treedb-$revision.test" -test.run '^$' \
      -test.bench '^BenchmarkDBValueLogGet$/^(Get|GetAppend)$' -test.benchtime 1s -test.count 1 -test.benchmem \
      > "$qualification_dir/logs/public-guard-$revision-$repetition.txt" 2>&1
  done
done
check_fixture > "$qualification_dir/logs/fixture-after.txt"
