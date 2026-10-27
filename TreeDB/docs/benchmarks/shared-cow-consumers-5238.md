# Ordinary service COW consumer diagnostic

This page records the historical fixture and results, not a current supported
benchmark recipe. The retired `native_runtime` control is unavailable, and
ordinary filtered ANN and filtered vector Hybrid now fail closed. Qualification
uses the separately frozen supported fixture; historical failures remain evidence.

`BenchmarkServiceSharedCOWConsumers` reuses the maintained dense visibility
fixture geometry (4096 ordinary JSON documents, two-dimensional embeddings,
top10, ef100), adding sixteen declared scalar tenants. It exercises public
collection Get and ordinary service keyword, Dense and combined Hybrid, including
final document decode/projection. The strategy is omitted deliberately. The
current cosine omission policy remains `column_graph`; ordinary native-storage
ColumnGraph service adoption is unmet, so the native arm currently refuses setup.
Do not reinterpret that refusal as a passing default or make this workload
explicit-runtime to replace a failed public-default comparison.

`GOMAP_SERVICE_BENCH_BACKEND=legacy` uses
`OpenBackendWithCachedLeafLog`; `native` uses `Open` plus
`NativeSemanticBackend`. Both request cow_btree and the selected profile, with
periodic/idle checkpointing disabled. The legacy opener substitutes ordinary
btree services; its requested memtable is not proof of native activation.
The native Hybrid response must carry a native capture. Owner/manager/service
cleanup runs in reverse order outside timing and reports errors.

`GOMAP_SERVICE_BENCH_PROFILE` accepts command_wal_durable,
command_wal_relaxed, or no_wal_fast; durable is the default. Invalid values fail
before setup. The old ordinary ColumnGraph default supports durable only here.
Setup, fixed public deferred loads of 128 documents, OptimizeIndex and teardown are outside the measured
read boundary. Each child reports ordinary Go ns/op, B/op and allocs/op; keyword
and Hybrid also report posting work, and Hybrid reports final documents fetched.
Filtered results must contain exactly10 tenant-00 documents with embeddings
projected out. No scan fallback or fail-closed Hybrid result is accepted.

Reproduce with the repository Go toolchain and a writable fast-disk TMPDIR:

```sh
GOWORK=off go test -c -o "$TMPDIR/service-consumers.test" ./TreeDB/documentservice
GOMAP_SERVICE_BENCH_BACKEND=legacy GOMAP_SERVICE_BENCH_PROFILE=command_wal_durable \
  "$TMPDIR/service-consumers.test" -test.run '^$' \
  -test.bench '^BenchmarkServiceSharedCOWConsumers$' -test.benchtime=100x -test.count=1 -test.v
GOMAP_SERVICE_BENCH_BACKEND=native GOMAP_SERVICE_BENCH_PROFILE=command_wal_durable \
  "$TMPDIR/service-consumers.test" -test.run '^$' \
  -test.bench '^BenchmarkServiceSharedCOWConsumers$' -test.benchtime=100x -test.count=1 -test.v
```

These are standalone Go benchmark rows, not benchprof inputs. Preserve full raw
stdout/stderr, exit status, commands, source/harness/binary hashes, toolchain,
profile and host/runtime isolation. Build outside timed collection; run fresh
processes serially. All failed setup or query outcomes remain evidence.

The historical early comparison used original first-slice legacy defaults
versus a candidate new-omitted native_runtime/ANN default, with one fixed
alternating trio per diagnostic pair. That blanket default change is retired;
its original source/binary bindings and every failed or partial result remain
historical evidence. Report resolved strategy and Dense route for every actual
run. This omitted-strategy benchmark cannot currently supply a valid native
same-default comparison. The existing explicit native_runtime
`BenchmarkServiceDenseNativeRuntimeVisibilityOverhead` remains a matched
algorithm control, not public-default acceptance. A few short processes are
viability evidence only. Retained qualification requires a reviewed landed
harness, frozen identities, explicit acceptance matrix and all original failures
retained. These read rows do not qualify write ACK, tail SLOs, held-reader
retention, or typed/quantized/partitioned and distributed service adoption.

The original relaxed/atomic4096 setup produced nine retained attempts: three
candidate legacy successes, three baseline Create failures, three native load
failures. It produced no valid comparison. Functional controls confirm baseline
ColumnGraph's durable-only guard and native text workspace capacity refusal with
zero rows and unchanged logical, physical, WAL and retained-owner frontiers.
This revised durable/fixed-batch fixture constructs the same 4096-document read corpus using
identical 128-document public request boundaries on every arm. It does not claim
atomic 4096-document load support or raise resource limits. Preserve the original attempts
and version/source bindings when reporting this revised diagnostic.
