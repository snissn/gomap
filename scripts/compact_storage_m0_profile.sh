#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
RUN_DIR=${RUN_DIR:-$(mktemp -d /mnt/fast4tb/compact_storage_m0_XXXXXX)}
TMP_ROOT=${TMPDIR:-/mnt/fast4tb/tmp}
COUNT=${COUNT:-6}
CPU_SET=${CPU_SET:-2-3}
GOMAXPROCS=${GOMAXPROCS:-2}
GOMEMLIMIT=${GOMEMLIMIT:-8GiB}
BENCH='^BenchmarkCompactStorageM0$'
STRESS='^BenchmarkCompactStorageM0/one-generation-per-pass$'

mkdir -p "$RUN_DIR"/{canonical,profiles,syscalls,overhead/on,overhead/off}
cd "$ROOT"

run_go_test() {
  env \
    GOWORK=off \
    TMPDIR="$TMP_ROOT" \
    GOMAXPROCS="$GOMAXPROCS" \
    GOMEMLIMIT="$GOMEMLIMIT" \
    taskset -c "$CPU_SET" \
    go test ./TreeDB/db "$@"
}

{
  printf 'source_sha=%s\n' "$(git -C "$ROOT" rev-parse HEAD)"
  printf 'source_status=%s\n' "$(git -C "$ROOT" status --short | wc -l)"
  printf 'go_version=%s\n' "$(go version)"
  printf 'kernel=%s\n' "$(uname -srvmo)"
  printf 'cpu_set=%s\n' "$CPU_SET"
  printf 'gomaxprocs=%s\n' "$GOMAXPROCS"
  printf 'gomemlimit=%s\n' "$GOMEMLIMIT"
  printf 'tmpdir=%s\n' "$TMP_ROOT"
  printf 'filesystem=%s\n' "$(findmnt -no SOURCE,FSTYPE,OPTIONS --target "$TMP_ROOT")"
  printf 'cpu=%s\n' "$(lscpu | awk -F: '/Model name/{sub(/^[ \t]+/, "", $2); print $2; exit}')"
  printf 'memory=%s\n' "$(free -h | awk '/^Mem:/{print $2 " total, " $7 " available"}')"
  printf 'count=%s\n' "$COUNT"
  printf 'canonical_command=go test ./TreeDB/db -run ^$ -bench %s -benchtime=1x -count=1 -benchmem\n' "$BENCH"
} >"$RUN_DIR/environment.txt"

: >"$RUN_DIR/canonical/raw.txt"
for sample in $(seq 1 "$COUNT"); do
  TREEDB_COMPACT_STORAGE_M0_SAMPLE="$sample" \
  TREEDB_COMPACT_STORAGE_M0_ARTIFACT_DIR="$RUN_DIR/canonical" \
    run_go_test -run '^$' -bench "$BENCH" -benchtime=1x -count=1 -benchmem |
    tee -a "$RUN_DIR/canonical/raw.txt"
done

if command -v benchstat >/dev/null 2>&1; then
  benchstat "$RUN_DIR/canonical/raw.txt" >"$RUN_DIR/canonical/benchstat.txt"
else
  printf 'benchstat not found\n' >"$RUN_DIR/canonical/benchstat.txt"
fi

for sample in $(seq 1 "$COUNT"); do
  TREEDB_COMPACT_STORAGE_M0_SAMPLE="$sample" \
  TREEDB_COMPACT_STORAGE_M0_ARTIFACT_DIR="$RUN_DIR/overhead/on" \
    run_go_test -run '^$' -bench "$STRESS" -benchtime=1x -count=1 -benchmem |
    tee -a "$RUN_DIR/overhead/on/raw.txt"
  TREEDB_COMPACT_STORAGE_M0_SAMPLE="$sample" \
  TREEDB_COMPACT_STORAGE_M0_ARTIFACT_DIR="$RUN_DIR/overhead/off" \
  TREEDB_COMPACT_STORAGE_M0_INSTRUMENTATION=off \
    run_go_test -run '^$' -bench "$STRESS" -benchtime=1x -count=1 -benchmem |
    tee -a "$RUN_DIR/overhead/off/raw.txt"
done
if command -v benchstat >/dev/null 2>&1; then
  benchstat "$RUN_DIR/overhead/off/raw.txt" "$RUN_DIR/overhead/on/raw.txt" >"$RUN_DIR/overhead/benchstat.txt"
fi

TREEDB_COMPACT_STORAGE_M0_SAMPLE=101 \
TREEDB_COMPACT_STORAGE_M0_ARTIFACT_DIR="$RUN_DIR/profiles" \
  run_go_test -run '^$' -bench "$STRESS" -benchtime=3x -count=1 \
  -cpuprofile "$RUN_DIR/profiles/cpu.pprof" >"$RUN_DIR/profiles/cpu_raw.txt"
TREEDB_COMPACT_STORAGE_M0_SAMPLE=102 \
TREEDB_COMPACT_STORAGE_M0_ARTIFACT_DIR="$RUN_DIR/profiles" \
  run_go_test -run '^$' -bench "$STRESS" -benchtime=3x -count=1 \
  -memprofile "$RUN_DIR/profiles/allocs.pprof" -memprofilerate=1 >"$RUN_DIR/profiles/allocs_raw.txt"
TREEDB_COMPACT_STORAGE_M0_SAMPLE=103 \
TREEDB_COMPACT_STORAGE_M0_ARTIFACT_DIR="$RUN_DIR/profiles" \
  run_go_test -run '^$' -bench "$STRESS" -benchtime=3x -count=1 \
  -blockprofile "$RUN_DIR/profiles/block.pprof" >"$RUN_DIR/profiles/block_raw.txt"
TREEDB_COMPACT_STORAGE_M0_SAMPLE=104 \
TREEDB_COMPACT_STORAGE_M0_ARTIFACT_DIR="$RUN_DIR/profiles" \
  run_go_test -run '^$' -bench "$STRESS" -benchtime=3x -count=1 \
  -mutexprofile "$RUN_DIR/profiles/mutex.pprof" >"$RUN_DIR/profiles/mutex_raw.txt"
TREEDB_COMPACT_STORAGE_M0_SAMPLE=105 \
TREEDB_COMPACT_STORAGE_M0_ARTIFACT_DIR="$RUN_DIR/profiles" \
  run_go_test -run '^$' -bench "$STRESS" -benchtime=1x -count=1 \
  -trace "$RUN_DIR/profiles/trace.out" >"$RUN_DIR/profiles/trace_raw.txt"

go tool pprof -top -tagfocus='compact-storage-m0=one-generation-per-pass' \
  "$RUN_DIR/profiles/cpu.pprof" >"$RUN_DIR/profiles/cpu_top.txt"
go tool pprof -top -alloc_space \
  -ignore='writeLeafGenerationBenchKeyRange|openCompactStorageLeafPackBenchmarkFixture' \
  "$RUN_DIR/profiles/allocs.pprof" >"$RUN_DIR/profiles/allocs_top.txt"

TREEDB_COMPACT_STORAGE_M0_SAMPLE=106 \
TREEDB_COMPACT_STORAGE_M0_ARTIFACT_DIR="$RUN_DIR/syscalls" \
TREEDB_COMPACT_STORAGE_M0_STRACE_MARKERS=1 \
  env GOWORK=off TMPDIR="$TMP_ROOT" GOMAXPROCS="$GOMAXPROCS" GOMEMLIMIT="$GOMEMLIMIT" \
  taskset -c "$CPU_SET" strace -f -ttt -e trace=write,fsync,fdatasync,msync,syncfs \
  -o "$RUN_DIR/syscalls/strace_raw.txt" \
  go test ./TreeDB/db -run '^$' -bench "$STRESS" -benchtime=1x -count=1 -benchmem \
  >"$RUN_DIR/syscalls/raw.txt" 2>"$RUN_DIR/syscalls/stderr.txt"

awk '
  /TREEDB_M0_BEGIN/ { capture=1; next }
  /TREEDB_M0_END/ { capture=0 }
  capture && /(fsync|fdatasync|msync|syncfs)\(/ { print }
' "$RUN_DIR/syscalls/strace_raw.txt" >"$RUN_DIR/syscalls/operation_stable_calls.txt"
awk '
  /TREEDB_M0_PHASE_BEGIN checkpoint/ { capture=1; next }
  /TREEDB_M0_PHASE_END checkpoint/ { capture=0 }
  capture && /(fsync|fdatasync|msync|syncfs)\(/ { print }
' "$RUN_DIR/syscalls/strace_raw.txt" >"$RUN_DIR/syscalls/checkpoint_stable_calls.txt"
awk '
  {
    for (i = 1; i <= NF; i++) {
      if ($i ~ /^(fsync|fdatasync|msync|syncfs)\(/) {
        split($i, parts, "(")
        counts[parts[1]]++
      }
    }
  }
  END {
    total=0
    for (name in counts) {
      print name, counts[name]
      total += counts[name]
    }
    print "total", total
  }
' "$RUN_DIR/syscalls/operation_stable_calls.txt" | sort >"$RUN_DIR/syscalls/strace_summary.txt"
printf 'checkpoint_total %s\n' "$(wc -l <"$RUN_DIR/syscalls/checkpoint_stable_calls.txt")" \
  >>"$RUN_DIR/syscalls/strace_summary.txt"
jq '{
  recorder_stable_calls: ([.stable_calls[] | select(.call_type != "userspace-flush") | .count] | add // 0),
  checkpoint_stable_calls: ([.checkpoints[].stable_calls] | add // 0)
}' "$RUN_DIR/syscalls/compact-storage-m0/one-generation-per-pass/sample-106.json" \
  >"$RUN_DIR/syscalls/recorder_summary.json"

find "$RUN_DIR/canonical/compact-storage-m0" -name 'sample-*.json' -print0 |
  sort -z |
  xargs -0 jq -s '
    map({
      fixture: .fixture.name,
      sample: .artifact_name,
      total_wall_ns: .total_wall_time_nanos,
      apply_wall_ns: .apply_wall_time_nanos,
      stable_calls: ([.stable_calls[].count] | add // 0),
      checkpoint_stable_calls: ([.checkpoints[].stable_calls] | add // 0),
      alloc_bytes: .allocation.total_alloc_bytes,
      alloc_objects: .allocation.allocation_objects,
      foreground_p95_ns: .foreground_writes.p95_nanos,
      foreground_p99_ns: .foreground_writes.p99_nanos,
      idle_p95_ns: .idle_writes.p95_nanos,
      idle_p99_ns: .idle_writes.p99_nanos,
      vacuum_reason: .vacuum.plan_reason
    })' >"$RUN_DIR/canonical/summary.json"

printf '%s\n' "$RUN_DIR"
