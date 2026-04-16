#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
FIXTURE=${TREEDB_LEAFGEN_PLAN_FIXTURE:-${1:?usage: leafgen_plan_profile.sh <application.db> [output-dir]}}
OUT=${2:-/tmp/leafgen_plan_profile_$(date +%Y%m%d%H%M%S)}
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

TREEDB_LEAFGEN_PLAN_WORK_FIXTURE="$WORK_FIXTURE"   "$OUT/treedb_db.test"   -test.run '^$'   -test.bench '^BenchmarkLeafGenerationPlanPersistedIndexReopen_SavedHome$'   -test.benchtime=1x   -test.count "$COUNT"   -test.benchmem   -test.cpuprofile "$OUT/cpu_persisted.pprof"   -test.memprofile "$OUT/allocs_persisted.pprof"   | tee "$OUT/bench_persisted.txt"

go tool pprof -top "$OUT/treedb_db.test" "$OUT/cpu_persisted.pprof" > "$OUT/cpu_persisted_top.txt"
go tool pprof -top -alloc_space "$OUT/treedb_db.test" "$OUT/allocs_persisted.pprof" > "$OUT/alloc_space_persisted_top.txt"
go tool pprof -top -alloc_objects "$OUT/treedb_db.test" "$OUT/allocs_persisted.pprof" > "$OUT/alloc_objects_persisted_top.txt"

echo "leafgen plan profile complete"
echo "  fixture:           $FIXTURE"
echo "  persisted fixture: $WORK_FIXTURE"
echo "  output:            $OUT"
echo "  cold bench:        $OUT/bench.txt"
echo "  persisted bench:   $OUT/bench_persisted.txt"
echo "  cold cpu:          $OUT/cpu.pprof"
echo "  persisted cpu:     $OUT/cpu_persisted.pprof"
echo "  cold allocs:       $OUT/allocs.pprof"
echo "  persisted allocs:  $OUT/allocs_persisted.pprof"
echo "  cold tops:         $OUT/cpu_top.txt, $OUT/alloc_space_top.txt, $OUT/alloc_objects_top.txt"
echo "  persisted tops:    $OUT/cpu_persisted_top.txt, $OUT/alloc_space_persisted_top.txt, $OUT/alloc_objects_persisted_top.txt"
