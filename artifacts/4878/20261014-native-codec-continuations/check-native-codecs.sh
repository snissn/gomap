#!/usr/bin/env bash
set -eu
cd /home/mikers/benchmarks/mvcc-4871-repair-20261013/native-4878-traversal
export GOWORK=off GOMAXPROCS=2 CGO_ENABLED=1 GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local
codec_go=/home/mikers/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go
codec_mod=/home/mikers/benchmarks/mvcc-4871-repair-20261013/scratch/native-codec-worker.mod
codec_ev=/home/mikers/benchmarks/mvcc-4871-repair-20261013/scratch/native-codec-checkpoint-20261014
mkdir -p "$codec_ev"
git rev-parse HEAD > "$codec_ev/native-base.txt"
git -C ../compress-5129 rev-parse HEAD > "$codec_ev/dependency-head.txt"
python3 - <<'PYHASH'
import hashlib
from pathlib import Path
p=Path('../compress-5129/zstd/decoder_bounded.go')
assert hashlib.sha256(p.read_bytes()).hexdigest() == 'c27b29fd4f0707a1a2c6986171a267e71c36d5d1e6bb482840fefd79b96c054c'
PYHASH
sha256sum ../compress-5129/zstd/decoder_bounded.go > "$codec_ev/dependency-source.sha256"
git -C ../compress-5129 diff --binary -- zstd/decoder_bounded.go > "$codec_ev/dependency-patch.diff"
cp TreeDB/internal/valuelog/cow_leaf_continuation.go "$codec_ev/final-cow-leaf.go"
trap 'cp "$codec_ev/final-cow-leaf.go" TreeDB/internal/valuelog/cow_leaf_continuation.go' EXIT
python3 - <<'PY'
from pathlib import Path
p=Path('TreeDB/internal/valuelog/cow_leaf_continuation.go')
s=p.read_text().replace('func (c *COWLeafContinuation) Step(w *iterator.OrdinalScanWork) ([]byte, bool, error) {','func (c *COWLeafContinuation) Step(w *iterator.OrdinalScanWork) ([]byte, bool, error) {\n\tif c.ZSTD != nil { c.ZSTD.BindWork(w); defer c.ZSTD.DropWork() }',1)
p.write_text(s)
PY
sha256sum TreeDB/internal/valuelog/cow_leaf_continuation.go > "$codec_ev/red-source.sha256"
diff -u "$codec_ev/final-cow-leaf.go" TreeDB/internal/valuelog/cow_leaf_continuation.go > "$codec_ev/red-naive-binding.patch" || true
set +e
"$codec_go" test -p 2 -modfile="$codec_mod" ./TreeDB/internal/valuelog -run '^TestCOWLeafContinuationNoHiddenAllocations$' -count=1 -v > "$codec_ev/red-naive-binding.log" 2>&1
codec_red=$?
set -e
cp "$codec_ev/final-cow-leaf.go" TreeDB/internal/valuelog/cow_leaf_continuation.go
trap - EXIT
cat "$codec_ev/red-naive-binding.log"
if [ "$codec_red" -eq 0 ]; then echo 'Expected causal allocation RED did not fail'; exit 1; fi
"$codec_go" test -p 2 -modfile="$codec_mod" ./TreeDB/internal/valuelog ./TreeDB/tree -run '^(TestCOW|TestCharged|TestProjection)' -count=1 -v > "$codec_ev/focused-normal.log" 2>&1
cat "$codec_ev/focused-normal.log"
"$codec_go" test -race -p 2 -modfile="$codec_mod" ./TreeDB/internal/valuelog ./TreeDB/tree -run '^(TestCOW|TestCharged|TestProjection)' -count=1 -v > "$codec_ev/focused-race.log" 2>&1
cat "$codec_ev/focused-race.log"
sha256sum TreeDB/internal/valuelog/cow_leaf_continuation.go TreeDB/internal/valuelog/cow_leaf_projection.go TreeDB/internal/valuelog/cow_template_continuation.go TreeDB/internal/valuelog/cow_template_continuation_test.go TreeDB/internal/valuelog/cow_zstd_continuation.go TreeDB/internal/valuelog/cow_zstd_continuation_test.go TreeDB/tree/charged_external_leaf_test.go docs/agent-execution/mvcc-4871-20261013/native-codec-continuations.md > "$codec_ev/source.sha256"
git diff --check
