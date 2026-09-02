#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TMP_ROOT=${TMPDIR:-/tmp}
mkdir -p "$TMP_ROOT"
RUN_DIR=${RUN_DIR:-$(mktemp -d "$TMP_ROOT/compact_storage_m0_XXXXXX")}
COUNT=${COUNT:-12}
ARTIFACT_SCHEMA_VERSION=3

default_cpu_set() {
  local allowed range start end cpu
  local -a selected=()
  allowed=$(awk '/^Cpus_allowed_list:/ { print $2; exit }' /proc/self/status)
  if [[ -z "$allowed" ]]; then
    printf 'unable to determine Cpus_allowed_list from /proc/self/status\n' >&2
    return 1
  fi
  IFS=',' read -ra ranges <<<"$allowed"
  for range in "${ranges[@]}"; do
    if [[ "$range" == *-* ]]; then
      start=${range%-*}
      end=${range#*-}
    else
      start=$range
      end=$range
    fi
    for ((cpu = start; cpu <= end && ${#selected[@]} < 2; cpu++)); do
      selected+=("$cpu")
    done
    if ((${#selected[@]} == 2)); then
      break
    fi
  done
  if ((${#selected[@]} == 0)); then
    printf 'Cpus_allowed_list did not contain an available CPU: %s\n' "$allowed" >&2
    return 1
  fi
  local joined
  joined=$(IFS=,; printf '%s' "${selected[*]}")
  printf '%s\n' "$joined"
}

CPU_SET=${CPU_SET:-$(default_cpu_set)}
GOMAXPROCS=${GOMAXPROCS:-2}
GOMEMLIMIT=${GOMEMLIMIT:-8GiB}
BENCH='^BenchmarkCompactStorageM0$'
STRESS='^BenchmarkCompactStorageM0/one-generation-per-pass$'
DECISION='^BenchmarkCompactStorageIndexVacuumDecisionNoDebt$'

mkdir -p "$RUN_DIR"/{canonical,decision,rss,profiles,syscalls,overhead/on,overhead/off}
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
  printf 'artifact_schema_version=%s\n' "$ARTIFACT_SCHEMA_VERSION"
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

: >"$RUN_DIR/decision/raw.txt"
for sample in $(seq 1 "$COUNT"); do
  run_go_test -run '^$' -bench "$DECISION" -benchtime=100x -count=1 -benchmem |
    tee -a "$RUN_DIR/decision/raw.txt"
done
if command -v benchstat >/dev/null 2>&1; then
  benchstat "$RUN_DIR/decision/raw.txt" >"$RUN_DIR/decision/benchstat.txt"
fi

if [[ -x /usr/bin/time ]]; then
  : >"$RUN_DIR/rss/max_rss_kib.txt"
  for sample in $(seq 1 "$COUNT"); do
    for fixture in full-high-debt exhaustive-control; do
      /usr/bin/time -f "$sample $fixture %M" -o "$RUN_DIR/rss/max_rss_kib.txt" -a \
        env GOWORK=off TMPDIR="$TMP_ROOT" GOMAXPROCS="$GOMAXPROCS" GOMEMLIMIT="$GOMEMLIMIT" \
        taskset -c "$CPU_SET" go test ./TreeDB/db -run '^$' \
        -bench "^BenchmarkCompactStorageM0/$fixture$" -benchtime=1x -count=1 >/dev/null
    done
  done
else
  printf '/usr/bin/time unavailable\n' >"$RUN_DIR/rss/max_rss_kib.txt"
fi

run_overhead_sample() {
  local mode=$1
  local sample=$2
  if [[ "$mode" == "off" ]]; then
    TREEDB_COMPACT_STORAGE_M0_SAMPLE="$sample" \
    TREEDB_COMPACT_STORAGE_M0_ARTIFACT_DIR="$RUN_DIR/overhead/off" \
    TREEDB_COMPACT_STORAGE_M0_INSTRUMENTATION=off \
      run_go_test -run '^$' -bench "$STRESS" -benchtime=1x -count=1 -benchmem |
      tee -a "$RUN_DIR/overhead/off/raw.txt"
    return
  fi
  TREEDB_COMPACT_STORAGE_M0_SAMPLE="$sample" \
  TREEDB_COMPACT_STORAGE_M0_ARTIFACT_DIR="$RUN_DIR/overhead/on" \
    run_go_test -run '^$' -bench "$STRESS" -benchtime=1x -count=1 -benchmem |
    tee -a "$RUN_DIR/overhead/on/raw.txt"
}

for sample in $(seq 1 "$COUNT"); do
  # Counterbalance pair order so the second write-heavy run cannot bias one mode.
  if (( sample % 2 == 1 )); then
    run_overhead_sample off "$sample"
    run_overhead_sample on "$sample"
  else
    run_overhead_sample on "$sample"
    run_overhead_sample off "$sample"
  fi
done
if command -v benchstat >/dev/null 2>&1; then
  benchstat "$RUN_DIR/overhead/off/raw.txt" "$RUN_DIR/overhead/on/raw.txt" >"$RUN_DIR/overhead/benchstat.txt"
fi

TREEDB_COMPACT_STORAGE_M0_SAMPLE=101 \
TREEDB_COMPACT_STORAGE_M0_ARTIFACT_DIR="$RUN_DIR/profiles" \
TREEDB_COMPACT_STORAGE_M0_INSTRUMENTATION=off \
  run_go_test -run '^$' -bench "$STRESS" -benchtime=3x -count=1 \
  -cpuprofile "$RUN_DIR/profiles/cpu.pprof" >"$RUN_DIR/profiles/cpu_raw.txt"
TREEDB_COMPACT_STORAGE_M0_SAMPLE=102 \
TREEDB_COMPACT_STORAGE_M0_ARTIFACT_DIR="$RUN_DIR/profiles" \
TREEDB_COMPACT_STORAGE_M0_INSTRUMENTATION=off \
TREEDB_COMPACT_STORAGE_M0_ALLOCS_PROFILE_DIR="$RUN_DIR/profiles" \
  run_go_test -run '^$' -bench "$STRESS" -benchtime=1x -count=1 \
  -memprofilerate=1 >"$RUN_DIR/profiles/allocs_raw.txt"
TREEDB_COMPACT_STORAGE_M0_SAMPLE=103 \
TREEDB_COMPACT_STORAGE_M0_ARTIFACT_DIR="$RUN_DIR/profiles" \
TREEDB_COMPACT_STORAGE_M0_INSTRUMENTATION=off \
  run_go_test -run '^$' -bench "$STRESS" -benchtime=3x -count=1 \
  -blockprofile "$RUN_DIR/profiles/block.pprof" >"$RUN_DIR/profiles/block_raw.txt"
TREEDB_COMPACT_STORAGE_M0_SAMPLE=104 \
TREEDB_COMPACT_STORAGE_M0_ARTIFACT_DIR="$RUN_DIR/profiles" \
TREEDB_COMPACT_STORAGE_M0_INSTRUMENTATION=off \
  run_go_test -run '^$' -bench "$STRESS" -benchtime=3x -count=1 \
  -mutexprofile "$RUN_DIR/profiles/mutex.pprof" >"$RUN_DIR/profiles/mutex_raw.txt"
TREEDB_COMPACT_STORAGE_M0_SAMPLE=105 \
TREEDB_COMPACT_STORAGE_M0_ARTIFACT_DIR="$RUN_DIR/profiles" \
TREEDB_COMPACT_STORAGE_M0_INSTRUMENTATION=off \
  run_go_test -run '^$' -bench "$STRESS" -benchtime=1x -count=1 \
  -trace "$RUN_DIR/profiles/trace.out" >"$RUN_DIR/profiles/trace_raw.txt"

go tool pprof -top -tagfocus='compact-storage-m0=one-generation-per-pass' \
  "$RUN_DIR/profiles/cpu.pprof" >"$RUN_DIR/profiles/cpu_top.txt"
go tool pprof -top -alloc_space \
  -relative_percentages \
  -base "$RUN_DIR/profiles/allocs_one-generation-per-pass_before.pprof" \
  -focus='TreeDB/(db|freelist|pager|zipper|internal/(rootpublication|valuelog))' \
  -ignore='writeCompactStorageM0AllocsProfile' \
  "$RUN_DIR/profiles/allocs_one-generation-per-pass_after.pprof" \
  >"$RUN_DIR/profiles/allocs_top.txt"
go tool pprof -top -alloc_objects \
  -relative_percentages \
  -base "$RUN_DIR/profiles/allocs_one-generation-per-pass_before.pprof" \
  -focus='TreeDB/(db|freelist|pager|zipper|internal/(rootpublication|valuelog))' \
  -ignore='writeCompactStorageM0AllocsProfile' \
  "$RUN_DIR/profiles/allocs_one-generation-per-pass_after.pprof" \
  >"$RUN_DIR/profiles/allocs_objects_top.txt"

TREEDB_COMPACT_STORAGE_M0_SAMPLE=106 \
TREEDB_COMPACT_STORAGE_M0_ARTIFACT_DIR="$RUN_DIR/syscalls" \
TREEDB_COMPACT_STORAGE_M0_STRACE_MARKERS=1 \
  env GOWORK=off TMPDIR="$TMP_ROOT" GOMAXPROCS="$GOMAXPROCS" GOMEMLIMIT="$GOMEMLIMIT" \
  taskset -c "$CPU_SET" strace -f -ttt -e trace=write,fsync,fdatasync,msync,syncfs \
  -o "$RUN_DIR/syscalls/strace_raw.txt" \
  go test ./TreeDB/db -run '^$' -bench "$STRESS" -benchtime=1x -count=1 -benchmem \
  >"$RUN_DIR/syscalls/raw.txt" 2>"$RUN_DIR/syscalls/stderr.txt"

while IFS= read -r -d '' artifact; do
  if ! jq -e --argjson want "$ARTIFACT_SCHEMA_VERSION" '.schema_version == $want' "$artifact" >/dev/null; then
    printf 'artifact schema mismatch: %s (want %s)\n' "$artifact" "$ARTIFACT_SCHEMA_VERSION" >&2
    exit 1
  fi
done < <(find "$RUN_DIR" -name 'sample-*.json' -print0)

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
  recorder_stable_calls: ((.stable_calls // []) | map(.count) | add // 0),
  checkpoint_stable_calls: ((.checkpoints // []) | map(.stable_calls) | add // 0)
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
      stable_calls: ((.stable_calls // []) | map(.count) | add // 0),
      checkpoint_stable_calls: ((.checkpoints // []) | map(.stable_calls) | add // 0),
      alloc_bytes: .allocation.total_alloc_bytes,
      alloc_objects: .allocation.allocation_objects,
      heap_alloc_after: .allocation.heap_alloc_after,
      heap_inuse_after: .allocation.heap_inuse_after,
      heap_sys_after: .allocation.heap_sys_after,
      foreground_p95_ns: .foreground_writes.p95_nanos,
      foreground_p99_ns: .foreground_writes.p99_nanos,
      idle_p95_ns: .idle_writes.p95_nanos,
      idle_p99_ns: .idle_writes.p99_nanos,
      vacuum_status: .vacuum.status,
      vacuum_required: .vacuum.required,
      vacuum_reason: .vacuum.plan_reason,
      vacuum_ran: .vacuum.ran,
      vacuum_reclaimed_bytes: .vacuum.reclaimed_bytes,
      vacuum_wall_ns: .vacuum.total_wall_time_nanos,
      vacuum_max_writer_pause_ns: .vacuum.max_writer_pause_nanos
    })' >"$RUN_DIR/canonical/summary.json"

printf '%s\n' "$RUN_DIR"
