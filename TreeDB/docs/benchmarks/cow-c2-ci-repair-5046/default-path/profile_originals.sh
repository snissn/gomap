#!/bin/bash
set -euo pipefail
export GOROOT=/home/mikers/.gvm/gos/go1.26.3 PATH=/home/mikers/.gvm/gos/go1.26.3/bin:$PATH GOCACHE=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/cache GOWORK=off GOMODCACHE=/home/mikers/.gvm/pkgsets/go1.25/global/pkg/mod
out=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/artifacts/c2/default-path-repair
for mode in base original; do
 taskset -c 0 env GOMAXPROCS=1 "$out/bin/$mode-db.test" -test.run '^$' -test.bench '^BenchmarkGetVersioned$' -test.benchmem -test.benchtime=5s -test.cpuprofile="$out/$mode-get.cpu" > "$out/$mode-get-profile.stdout" 2> "$out/$mode-get-profile.stderr"
 go tool pprof -top "$out/bin/$mode-db.test" "$out/$mode-get.cpu" > "$out/$mode-get.cpu-top.txt"
 go tool nm "$out/bin/$mode-db.test" | rg 'captureSnapshot' > "$out/$mode-capture-symbols.txt"
 go tool objdump -s 'captureSnapshot' "$out/bin/$mode-db.test" > "$out/$mode-capture-assembly.txt"
 taskset -c 0 env GOMAXPROCS=1 "$out/bin/$mode-public.test" -test.run '^$' -test.bench '^BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1$' -test.benchmem -test.benchtime=1000x -test.memprofile="$out/$mode-tiny.heap" -test.memprofilerate=1 > "$out/$mode-tiny-profile.stdout" 2> "$out/$mode-tiny-profile.stderr"
 go tool pprof -alloc_space -top "$out/bin/$mode-public.test" "$out/$mode-tiny.heap" > "$out/$mode-tiny.heap-top.txt"
done
