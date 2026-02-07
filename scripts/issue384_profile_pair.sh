#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

BASELINE_HASH="${BASELINE_HASH:-a2d8cbb802e0c611a82011a9ea18424817fcead8}"
KEYS="${KEYS:-4000000}"
VALSIZE="${VALSIZE:-256}"
BATCH="${BATCH:-1024}"
SEED="${SEED:-1}"
TESTS="${TESTS:-batch_write,random_read}"
PATTERN="${PATTERN:-medium_compressible_sparse}"
COMPRESSION="${COMPRESSION:-off}"
OUT_ROOT="${OUT_DIR:-$ROOT/artifacts/perf}"
TS=$(date +%Y%m%d%H%M%S)
OUT="$OUT_ROOT/issue384_profiles_${TS}"

TESTS_COMPACT=${TESTS//[[:space:]]/}
if [[ ",$TESTS_COMPACT," != *",batch_write,"* ]]; then
  echo "TESTS must include batch_write so cpu_batch_write_treedb.pprof is generated" >&2
  exit 2
fi
TESTS="$TESTS_COMPACT"

RUN_PREFIX="${RUN_PREFIX:-}"
if [[ -n "${CPUSET:-}" ]]; then
  RUN_PREFIX="taskset -c ${CPUSET} ${RUN_PREFIX}"
fi

export GOMAXPROCS="${GOMAXPROCS:-4}"
mkdir -p "$OUT/bin" "$OUT/profiles_baseline" "$OUT/profiles_candidate" "$OUT/worktrees"

BASELINE_WT="$OUT/worktrees/baseline"
if ! git cat-file -e "${BASELINE_HASH}^{commit}" >/dev/null 2>&1; then
  git fetch --no-tags --depth=1 origin "$BASELINE_HASH" || git fetch --no-tags origin "$BASELINE_HASH"
fi
git worktree add --detach "$BASELINE_WT" "$BASELINE_HASH" >/dev/null
cleanup() {
  git worktree remove --force "$BASELINE_WT" >/dev/null 2>&1 || true
}
trap cleanup EXIT

CAND_HASH=$(git rev-parse HEAD)
CAND_BRANCH=$(git rev-parse --abbrev-ref HEAD)

cat >"$OUT/meta.txt" <<META
ts=$TS
root=$ROOT
candidate_hash=$CAND_HASH
candidate_branch=$CAND_BRANCH
baseline_hash=$BASELINE_HASH
keys=$KEYS
valsize=$VALSIZE
batch=$BATCH
seed=$SEED
tests=$TESTS
pattern=$PATTERN
compression=$COMPRESSION
gomaxprocs=$GOMAXPROCS
run_prefix=$RUN_PREFIX
META

go build -o "$OUT/bin/unified-bench-candidate" ./cmd/unified_bench
(
  cd "$BASELINE_WT"
  go build -o "$OUT/bin/unified-bench-baseline" ./cmd/unified_bench
)

run_profile() {
  local bin="$1"
  local dest="$2"

  mkdir -p "$dest"
  local cmd=(
    "$bin"
    -dbs treedb
    -keys "$KEYS"
    -valsize "$VALSIZE"
    -batchsize "$BATCH"
    -test "$TESTS"
    -val-pattern "$PATTERN"
    -format markdown
    -progress=false
    -keep=false
    -seed "$SEED"
    -treedb-force-value-pointers=true
    -treedb-value-log-threshold=1
    -treedb-cache-stats-before-reads=true
    -treedb-vlog-compression "$COMPRESSION"
    -cpuprofile "$dest/cpu"
    -cpuprofile-tests batch_write
    -blockprofile "$dest/block.pprof"
    -mutexprofile "$dest/mutex.pprof"
    -blockprofilerate 1
    -mutexprofilefraction 1
  )

  if [[ -n "$RUN_PREFIX" ]]; then
    # shellcheck disable=SC2206
    local prefix=( $RUN_PREFIX )
    GODEBUG=gctrace=1 "${prefix[@]}" "${cmd[@]}" >"$dest/run.md" 2>"$dest/gctrace.log"
  else
    GODEBUG=gctrace=1 "${cmd[@]}" >"$dest/run.md" 2>"$dest/gctrace.log"
  fi
}

run_profile "$OUT/bin/unified-bench-baseline" "$OUT/profiles_baseline"
run_profile "$OUT/bin/unified-bench-candidate" "$OUT/profiles_candidate"

go tool pprof -top "$OUT/bin/unified-bench-baseline" "$OUT/profiles_baseline/cpu_batch_write_treedb.pprof" >"$OUT/profiles_baseline/cpu_top.txt"
go tool pprof -top "$OUT/bin/unified-bench-candidate" "$OUT/profiles_candidate/cpu_batch_write_treedb.pprof" >"$OUT/profiles_candidate/cpu_top.txt"
go tool pprof -top "$OUT/bin/unified-bench-baseline" "$OUT/profiles_baseline/block.pprof" >"$OUT/profiles_baseline/block_top.txt"
go tool pprof -top "$OUT/bin/unified-bench-candidate" "$OUT/profiles_candidate/block.pprof" >"$OUT/profiles_candidate/block_top.txt"
go tool pprof -top "$OUT/bin/unified-bench-baseline" "$OUT/profiles_baseline/mutex.pprof" >"$OUT/profiles_baseline/mutex_top.txt"
go tool pprof -top "$OUT/bin/unified-bench-candidate" "$OUT/profiles_candidate/mutex.pprof" >"$OUT/profiles_candidate/mutex_top.txt"

python3 - "$OUT" <<'PY'
import re
import sys
from pathlib import Path

out = Path(sys.argv[1])
meta = {}
for line in (out / "meta.txt").read_text().splitlines():
    if "=" in line:
        k, v = line.split("=", 1)
        meta[k] = v

pat_diag = re.compile(r"^Batch Write / TreeDB = ([0-9][0-9,]*(?:\.[0-9]+)?)\s*$", re.M)
pat_md = re.compile(r"^\s*Batch Write\s+([0-9][0-9,]*(?:\.[0-9]+)?)\s*$", re.M)

def parse_ops(path: Path) -> float:
    txt = path.read_text()
    m = pat_diag.search(txt)
    if m:
        return float(m.group(1).replace(",", ""))
    m = pat_md.search(txt)
    if not m:
        return float("nan")
    return float(m.group(1).replace(",", ""))

base_ops = parse_ops(out / "profiles_baseline" / "run.md")
cand_ops = parse_ops(out / "profiles_candidate" / "run.md")
delta = (cand_ops - base_ops) * 100.0 / base_ops if base_ops > 0 else float("nan")

md = []
md.append("# Issue 384 profile pair")
md.append("")
md.append(f"- candidate: `{meta.get('candidate_hash', '')}` ({meta.get('candidate_branch', '')})")
md.append(f"- baseline: `{meta.get('baseline_hash', '')}`")
md.append(f"- tests={meta.get('tests', '')} valsize={meta.get('valsize', '')} keys={meta.get('keys', '')} pattern={meta.get('pattern', '')} compression={meta.get('compression', '')}")
md.append(f"- batch_write ops/sec: baseline={base_ops:,.0f}, candidate={cand_ops:,.0f}, delta={delta:+.2f}%")
md.append("")
md.append("## Artifact paths")
md.append("")
md.append(f"- baseline cpu top: `{out / 'profiles_baseline' / 'cpu_top.txt'}`")
md.append(f"- candidate cpu top: `{out / 'profiles_candidate' / 'cpu_top.txt'}`")
md.append(f"- baseline block top: `{out / 'profiles_baseline' / 'block_top.txt'}`")
md.append(f"- candidate block top: `{out / 'profiles_candidate' / 'block_top.txt'}`")
md.append(f"- baseline mutex top: `{out / 'profiles_baseline' / 'mutex_top.txt'}`")
md.append(f"- candidate mutex top: `{out / 'profiles_candidate' / 'mutex_top.txt'}`")
md.append(f"- baseline gctrace: `{out / 'profiles_baseline' / 'gctrace.log'}`")
md.append(f"- candidate gctrace: `{out / 'profiles_candidate' / 'gctrace.log'}`")

(out / "summary.md").write_text("\n".join(md) + "\n")
print((out / "summary.md").read_text())
PY

echo "profile artifacts: $OUT"
