#!/bin/bash
set -uo pipefail
export GOROOT=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/OWNED/toolchains/go1.26.8
export PATH="$GOROOT/bin:$PATH" GOCACHE=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/OWNED/c2-default-path-repair/cache-go1.26.8 GOWORK=off GOMODCACHE=/home/mikers/.gvm/pkgsets/go1.25/global/pkg/mod
out=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/artifacts/c2/default-path-repair
run(){ name=$1; shift; "$@" > "$out/$name.stdout" 2> "$out/$name.stderr"; result=$?; echo "$result" > "$out/$name.exit"; return "$result"; }
for mode in base repaired-v3; do
 run "go1268-$mode-get-profile" taskset -c 0 env GOMAXPROCS=1 "$out/bin/go1268-$mode-db.test" -test.run '^$' -test.bench '^BenchmarkGetVersioned$' -test.benchmem -test.benchtime=5s -test.cpuprofile="$out/go1268-$mode-get.cpu" || exit $?
 go tool pprof -top "$out/bin/go1268-$mode-db.test" "$out/go1268-$mode-get.cpu" > "$out/go1268-$mode-get.cpu-top.txt" || exit $?
 go tool objdump -s 'captureSnapshot' "$out/bin/go1268-$mode-db.test" > "$out/go1268-$mode-capture-assembly.txt" || exit $?
 run "go1268-$mode-tiny-profile" taskset -c 0 env GOMAXPROCS=1 "$out/bin/go1268-$mode-public.test" -test.run '^$' -test.bench '^BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1$' -test.benchmem -test.benchtime=1000x -test.memprofile="$out/go1268-$mode-tiny.heap" -test.memprofilerate=1 || exit $?
 go tool pprof -alloc_space -top "$out/bin/go1268-$mode-public.test" "$out/go1268-$mode-tiny.heap" > "$out/go1268-$mode-tiny.heap-top.txt" || exit $?
done
