#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
FIXTURE=${TREEDB_LEAFGEN_PLAN_FIXTURE:-${1:?usage: leafgen_plan_profile.sh <application.db> [output-dir]}}
OUT=${2:-${TMPDIR:-/tmp}/leafgen_plan_profile_$(date +%Y%m%d%H%M%S)}
COUNT=${LEAFGEN_PLAN_PROFILE_COUNT:-3}
WORK_FIXTURE="$OUT/work_fixture"

mkdir -p "$OUT"
cd "$ROOT"

GOWORK=off go test -c -o "$OUT/treedb_db.test" ./TreeDB/db

TREEDB_LEAFGEN_PLAN_FIXTURE="$FIXTURE"   "$OUT/treedb_db.test"   -test.run '^$'   -test.bench '^BenchmarkLeafGenerationPlan_SavedHome$'   -test.benchtime=1x   -test.count "$COUNT"   -test.benchmem   -test.cpuprofile "$OUT/cpu.pprof"   -test.memprofile "$OUT/allocs.pprof"   | tee "$OUT/bench.txt"

go tool pprof -top "$OUT/treedb_db.test" "$OUT/cpu.pprof" > "$OUT/cpu_top.txt"
go tool pprof -top -alloc_space "$OUT/treedb_db.test" "$OUT/allocs.pprof" > "$OUT/alloc_space_top.txt"
go tool pprof -top -alloc_objects "$OUT/treedb_db.test" "$OUT/allocs.pprof" > "$OUT/alloc_objects_top.txt"

rm -rf "$WORK_FIXTURE"
cp -a "$FIXTURE" "$WORK_FIXTURE"

TREEDB_LEAFGEN_PLAN_WORK_FIXTURE="$WORK_FIXTURE"   "$OUT/treedb_db.test"   -test.run '^$'   -test.bench '^BenchmarkLeafGenerationPlanRetainedSourceReopen_SavedHome$'   -test.benchtime=1x   -test.count "$COUNT"   -test.benchmem   -test.cpuprofile "$OUT/cpu_reopen.pprof"   -test.memprofile "$OUT/allocs_reopen.pprof"   | tee "$OUT/bench_reopen.txt"

go tool pprof -top "$OUT/treedb_db.test" "$OUT/cpu_reopen.pprof" > "$OUT/cpu_reopen_top.txt"
go tool pprof -top -alloc_space "$OUT/treedb_db.test" "$OUT/allocs_reopen.pprof" > "$OUT/alloc_space_reopen_top.txt"
go tool pprof -top -alloc_objects "$OUT/treedb_db.test" "$OUT/allocs_reopen.pprof" > "$OUT/alloc_objects_reopen_top.txt"

echo "leafgen plan profile complete"
echo "  fixture:           $FIXTURE"
echo "  reopen fixture:    $WORK_FIXTURE"
echo "  output:            $OUT"
echo "  cold bench:        $OUT/bench.txt"
echo "  reopen bench:      $OUT/bench_reopen.txt"
echo "  cold cpu:          $OUT/cpu.pprof"
echo "  reopen cpu:        $OUT/cpu_reopen.pprof"
echo "  cold allocs:       $OUT/allocs.pprof"
echo "  reopen allocs:     $OUT/allocs_reopen.pprof"
echo "  cold tops:         $OUT/cpu_top.txt, $OUT/alloc_space_top.txt, $OUT/alloc_objects_top.txt"
echo "  reopen tops:       $OUT/cpu_reopen_top.txt, $OUT/alloc_space_reopen_top.txt, $OUT/alloc_objects_reopen_top.txt"
