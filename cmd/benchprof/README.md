# benchprof

Standalone checkpointed point-read allocation/throughput qualification:

```sh
GOWORK=off GOMAXPROCS=4 go test ./TreeDB -run '^$' -bench '^BenchmarkDBCheckpointedValueLogGet$' -benchmem -benchtime=1s -count=5
GOWORK=off GOMAXPROCS=4 go test ./TreeDB/db -run '^$' -bench '^BenchmarkGetVersioned$' -benchmem -benchtime=1s -count=5
```

The public benchmark uses 32,768 keys and 256-byte pointer values, then
checkpoints before timing so the cached API reaches backend capture. `Get` and
`GetUnsafe` return owned copies; `GetAppend` reuses caller storage. Backend
`BenchmarkGetVersioned` measures versioned appends into caller storage. Compare
identical fixtures and commands on the same host at exact source heads; warm
reads are the timed boundary. Output is Go benchmark text. Optional Go test
CPU/allocation profiles must be inspected directly with `go tool pprof`; they
are not `benchprof_results.json` inputs. No unified-bench artifact schema changes.


`BenchmarkDocumentSnapshotGrowthV1` and
`BenchmarkDocumentSnapshotForegroundV1` use the standalone
`scripts/treedb_document_snapshot_evidence.sh OUTPUT_DIRECTORY` capture flow.
It writes fresh-process Go test JSON plus `cpu_*.pprof` and `heap_*.pprof`
for each leaf. Inspect those profiles directly with `go tool pprof`; their
fixture population is outside the benchmark timer but inside the process
profiles. The script does not emit `benchprof_results.json`, so these files
are not inputs to `benchprof`.
Growth records use `source_directory_file_bytes_before_export` for the source
directory size before export and `archive_bytes` for the streamed archive size.
Foreground records use `archive_bytes` for the native snapshot size.

`benchprof` analyzes `unified-bench` profile artifacts (CPU + allocation sections) and emits:

- `insights.md` (human-readable summary)
- `insights.json` (machine-friendly summary)
- `insights.html` (browser view rendered from markdown)

It emits concrete investigation targets (function + `file:line`) for each section using
symbol/theme inference (iterator/seek, decode/read I/O, write/delete/flush, locking, alloc/copy, etc.),
so it can adapt as implementations evolve without hardcoding specific function names.

## Build

```bash
make benchprof
```

## Typical flow

1. Run `unified-bench` with profile outputs into one directory. `-profile-dir` defaults artifact metadata to `-path-label native-fastpath`; pass `-path-label oracle` for explicit oracle/comparator captures, `-path-label m8-m14-10mm-gate` for the #2768+ mandatory span-run gate shape, `-path-label span-native-default-gate` for the default span-native production closeout matrix, or `-path-label span-native-read-scan-guardrail` for the related settled read/scan guardrail matrix.
2. `unified-bench` auto-runs `benchprof` in-process when `-profile-dir` is enabled. You can still run `benchprof` manually if needed.

Example:

This example uses unified-bench's legacy `fast` benchmark-runner preset for a
no-WAL profiling ceiling; it is not a TreeDB server profile recommendation.

```bash
mkdir -p /tmp/scan-profiles

./bin/unified-bench \
  -dbs treedb \
  -keys 800000 \
  -profile fast \
  -checkpoint-between-tests \
  -treedb-vlog-compression-variant off \
  -test full_scan,prefix_scan \
  -profile-dir /tmp/scan-profiles \
  -path-label native-fastpath \
  -progress=false

./bin/benchprof \
  -profiles-dir /tmp/scan-profiles
```

Outputs:

- `/tmp/scan-profiles/insights.md`
- `/tmp/scan-profiles/insights.json`
- `/tmp/scan-profiles/insights.html`

`insights.html` is always generated. For `unified-bench -suite collection_storage`,
the same command also renders the suite's `runs[].collection_workloads` metadata
(mode/workload names, correctness and semantic-equivalence flags, asset-byte
splits, and per-workload counters) into a "Collection Workload Metadata" table.

## Notes

- `benchprof` currently reads:
  - `benchprof_results.json` (preferred; auto-written by `unified-bench -profile-dir`)
  - `benchprof_results.md` (fallback; auto-written by `unified-bench -profile-dir`)
  - `cpu_<test>_<db>.pprof` (all test sections)
  - `allocs_<test>_<db>.pprof` (allocation delta profiles by section; auto-written by `unified-bench -profile-dir`)
  - `block_<test>_<db>.pprof` (per-test block contention delta profiles; present when non-empty)
  - `mutex_<test>_<db>.pprof` (per-test mutex contention delta profiles; present when non-empty)
  - `checkpoint_cpu_checkpoint_<test>_<db>.pprof` (checkpoint CPU sections)
  - `block.pprof` / `mutex.pprof` (global run-level fallback/supplement)
  - `trace.out` (detected, but not deeply analyzed yet)
- `benchprof_results.json` preserves selected TreeDB stats under
  `runs[].treedb_stats` when the benchmark exposes them. Checkpoint-enabled
  runs also expose `runs[].checkpoint_durations_seconds`, optional
  `runs[].checkpoint_settle_seconds`, and checkpoint-local selected stats under
  `runs[].checkpoint_treedb_stats`. This is the raw
  counter metadata used for TreeDB root-apply/cache review artifacts. For
  value-log mmap reads, unified-bench selected displays prefer backend
  `treedb.vlog.mmap_read.*` counters over cache-prefixed aliases, and the
  metadata includes generic plus leaf-specific sealed mmap budget caps when
  TreeDB exposes them. Parallel-flush M0/M8 artifacts preserve
  `treedb.cache.flush_apply.*`, `treedb.cache.leaf_log_lanes.*`,
  `treedb.cache.flush_span_run.*`, `treedb.flush_apply.*`,
  `treedb.flush_apply.span_run.*`, and `treedb.flush_apply.span_native.*`
  counters so planning, leaf-log lane distribution, canonical run shape,
  old-leaf read/decode bytes/op, leaf merges/op, replacement pages/op,
  append frames/op, publish prepare/final-install, guarded publish,
  reducer/publish, checkpoint wait splits, fallback reasons, retry, and foreground-assist stages appear beside
  CPU/allocation/contention profiles. Value-log codec policy artifacts also preserve
  `treedb.cache.vlog_auto.*`, `treedb.cache.vlog_write_mode.*`,
  `treedb.cache.vlog_payload_kind.*`, `treedb.cache.vlog_payload_split.*`,
  `treedb.cache.vlog_outer_leaf_codec.*`, and `treedb.cache.vlog_block.*`
  counters so actual auto codec selection, outer-leaf codec distribution, and
  frame-K distribution remain available in benchprof output.
- For command-WAL adapter evidence, keep `treedb.command_wal.*`,
  `treedb.applied_command_lsn`, and `treedb.cache.checkpoint.*` counters
  together. The first group proves accepted/covered command frames; the
  checkpoint group measures explicit backend publication work and should not be
  conflated with `WriteSync`/`Batch.WriteSync` command-WAL sync cost.
- `benchprof_results.json` also preserves collection-storage suite metadata
  under `runs[].collection_workloads` when `unified-bench -suite
  collection_storage` is used. `benchprof` keeps those stable mode/workload names
  and semantic comparability fields in `insights.{md,json,html}` alongside the
  CPU/allocation profile summaries.
- Optional flags:
  - `-bin` if you want explicit symbolization target (otherwise profile-only mode is used)
  - `-run-md` to force a specific markdown log file for ops/sec parsing
  - `-out-html` to override the default HTML output path

## Maintenance Expectations

The standalone `BenchmarkAdaptiveMVCCSnapshotCommandWAL` in `TreeDB/caching`
captures public MVCC writes with a snapshot every 112 writes under relaxed and
durable command-WAL profiles. Run from the repo root:

```sh
GOWORK=off go test ./TreeDB/caching -run '^$' -bench '^BenchmarkAdaptiveMVCCSnapshotCommandWAL$' -benchtime=2240x -count=5 -benchmem
```

Use fixed counts of at least 1120 to reach adaptive selection. For a separate
single-row profiling run, restrict `-bench` to the desired profile/mode and add
`-cpuprofile cpu.pprof -memprofile allocs.pprof`; inspect with `go tool pprof`.
The text output reports `B/op`, `allocs/op`, `writes/s`, sampling/selection and
command-WAL counters. These Go test profiles are not benchprof inputs.

The standalone durable MVCC singleton route comparison is:

```sh
GOWORK=off go test ./TreeDB/mvcc -run '^$' -bench '^BenchmarkCommitAtCommandWALDurableSingleton$' -benchmem -benchtime=1000x -count=5
```

It compares public point and batch routes for inline, pointer and oversized
values, reporting `B/op`, `allocs/op`, frame/sync/checkpoint counters. Output is
Go benchmark text; optional Go test profiles are not benchprof inputs. Run
matched fresh-process controls on the same host for performance acceptance.

If `unified-bench` profile naming, profile-dir defaults, or benchmark test names
change, update `benchprof` in the same PR:

- filename parsing in `internal/benchprof/main.go`
- parser tests in `internal/benchprof/main_test.go`
- `cmd/unified_bench/profile_artifact_dir_test.go` expectations
- this README and `cmd/unified_bench/README.md`

### Public exact-key MVCC iteration diagnostic

```sh
GOWORK=off go test ./TreeDB/mvcc -run '^TestVersionIterationExactKeyFixture$' -count=1
GOWORK=off go test ./TreeDB/mvcc -run '^$' -bench '^BenchmarkVersionIterationExactKey$' -benchmem -benchtime=1x -count=1
```

The smoke exercises depths 1/8/64 over 128 logical keys and eight populated
shards, with non-exact timestamps and full open/seek/owned-consume/close cost.
The M1 fixture selects `VersionIteratorOptions.ExactKey`; a before/after packet
must declare the baseline's original logical `key` to `key+NUL` selection and
retain both fixture identities. Results and ownership work are equivalent;
canonical bounds and shard-local source opens are the intended difference.
FrozenQueue and PublishedRoot exclude setup; Interleaved includes eight
singleton replacements per completed read. `observe=false/true` isolates the
existing default-disabled iterator diagnostic switch. Cached cut/source metrics
exclude the public backend-only snapshot fast path. Heap end and sampled maxima
are process-wide (including setup); flush counters exclude cleanup/drain. See
`TreeDB/docs/spec/verification.md` section 10.4 for counter denominators.

For retained timing, select one leaf per fresh process, freeze the source/runtime
identities, host, toolchain, benchtime/count, matched AB/BA order and exclusion
policy before collection, and keep profiles in separate runs. Go benchmark text
and optional Go test profiles are standalone artifacts, not benchprof inputs or
rows of the matched TreeDB/Badger decision matrix. This harness does not alter
unified-bench producer filenames or the benchprof parser contract.

## Standalone accepted partition cost profiles

`TestMultiOwnerTCPAcceptedDisjointRetained100KV1` in `TreeDB/nativewire`
writes ten cumulative sampled allocation profiles when the comparative cost
opt-in is enabled: `accepted-cost-local-before.pprof`,
`accepted-cost-local-after.pprof`, and
`accepted-cost-node-{0,1,2,3}-{before,after}.pprof` (eight daemon files).
These standalone Go test profiles are **not benchprof inputs** and are not
`unified-bench -profile-dir` outputs. Retain all ten files with the 516 JSON
receipts, source/input pins and sampling-rate/boundary metadata.

After harness review/landing, exact-source freeze and runner admission, use
a reviewed retained-input descriptor and its independently pinned SHA256.
The descriptor must retain the accepted M3/source/DB provenance; the checksum
does not replace those guards. Use absolute input/receipt paths, create a fresh
receipt directory and run once
inside the admitted 8 GiB, zero-swap scope with the pinned offline toolchain:

```sh
mkdir -m 700 "$PARTITION_RECEIPTS"
unset GOMAP_FIXED_PEER_COST_SETUP_PREFLIGHT_V1
GOWORK=off GOMAXPROCS=4 GOFLAGS='-p=2 -mod=readonly' \
  GOMAP_SELECTED_LIVE_FIXTURE="$PARTITION_INPUT" \
  GOMAP_SELECTED_LIVE_FIXTURE_SHA256="$PARTITION_INPUT_SHA256" \
  GOMAP_SELECTED_LIVE_RECEIPTS="$PARTITION_RECEIPTS" \
  GOMAP_ACCEPTED_COMPARATIVE_COST_V1=1 \
  go test ./TreeDB/nativewire \
    -run '^TestMultiOwnerTCPAcceptedDisjointRetained100KV1$' \
    -count=1 -timeout=750s -v
```

Inspect each matched pair directly, for example
`go tool pprof -sample_index=alloc_space -base BEFORE.pprof AFTER.pprof`;
repeat with `alloc_objects`. Run analysis under runner admission as well.
Two GCs flush samples outside search timers. Differences include background,
observer and profile/control work; they are statistical estimates, not exact
query/navigation/merge costs. Instrumented timings and global allocations are
not an apples-to-apples comparison with older uninstrumented samples.
See [the full observation contract](../../docs/benchmarks/treedb_partition_accepted_cost_diagnostic.md)
for framed-byte/RSS boundaries, validation and retained-collection limits.

### MVCC ownership allocation diagnostic

`BenchmarkMVCCOwnership` compares public GetAt, borrowed inspection and owned
version collection at widths 8/16/4096/32768 and depths 1/8/64, with ordinary
and forced-pointer routing. Ordinary large values can use automatic pointers.
Setup/checkpoint are excluded; completed reads, validation, final owned output
allocations and Close are charged. Iterator rows include matched clock samples
and report `first_entry_ns/op` alongside complete-consumption ns/op. Baseline
source without EntryView uses Entry for inspection, dispatching once per opened
iterator. Existing Filtered benchmarks remain traversal guards.

```sh
GOWORK=off go test ./TreeDB/mvcc -run '^$' -bench '^BenchmarkMVCCOwnership$' -benchmem -benchtime=1x -count=1
GOWORK=off go test -c -o /tmp/mvcc-ownership.test ./TreeDB/mvcc
GOMAP_MVCC_RETAINED_OUTPUTS=1 /tmp/mvcc-ownership.test -test.run '^TestMVCCRetainedOwnedOutputs/width=8/pointers=false$' -test.v
```

The first command is fixture smoke, not retained performance acceptance. Use the
same fixture source on base/head binaries and predeclare measured rows/repeats
before collection. Run each retained-output width/routing subtest in a fresh
process. It validates 4096 outputs after Store/DB closure and two GCs, retains
them with KeepAlive, and logs raw heap gauges and equal payload bytes. Transferred
payloads retain the whole envelope backing allocation and its allocator size
class; slice-capacity reduction cannot reduce that live storage. Lower B/op
alone does not establish lower retained heap or throughput. These Go test
artifacts/profiles are standalone diagnostics, not benchprof inputs.

### Value-log mixed-frame decode reuse

This standalone package benchmark alternates compressed 64 KiB/4 KiB frames
through the shared bounded decoder, warming codecs before the timer. It reports
Go benchmark text with ns/op, MB/s, B/op, allocs/op, `backing_allocs/op`, and
`scratch_cap_B` (one buffer's apparent capacity, not process RSS).

```sh
GOWORK=off go test ./TreeDB/internal/valuelog -run '^$' \
  -bench '^BenchmarkValueLogDecodeFrame(Alloc|MixedReuse)$' \
  -benchmem -count 5
GOWORK=off go test ./TreeDB -run '^$' \
  -bench '^BenchmarkDBValueLogGet$/^Get$' -benchmem -count 5
```

Keep the same benchmark source on baseline and candidate and serialize timed
captures. The public owned-Get benchmark is a separate guardrail; scratch
allocation reduction alone does not prove faster reads. These are Go test
outputs, not unified-bench profile-dir artifacts or benchprof inputs. No
profile parser or on-disk format changes are required. See the
[typed-storage performance guide](../../TreeDB/docs/guides/typed-storage-performance.md#persistent-value-log-decode-scratch).

### Owned persistent value-log reads

The standalone `BenchmarkFileReadAppendOwned` and
`BenchmarkDBOwnedValueLogRoute` compare cache copies, decode, OS-cache-warm
open/map/read/close, and ordinary public owned Get. The capture also runs landed
`BenchmarkFileReadAppendCompressedFallback`,
`BenchmarkValueLogRandomReadGroupedFrame_ReadUnsafeTo`, and
`BenchmarkDBValueLogGet/Get` and `/GetAppend` guardrails.

Prepare the source/binaries/fixture described in the
[retained packet](../../docs/benchmarks/treedb_owned_values_20261001/README.md), then run:

```sh
bash docs/benchmarks/treedb_owned_values_20261001/qualify.sh FROZEN_CAPTURE_DIR
```

It writes fresh-process Go benchmark text, separate `/usr/bin/time -v` RSS text,
and source/fixture hash inventories. These are standalone artifacts, not
unified-bench profile-dir output or benchprof inputs.

The final shared mmap publication repair has a separate
[95-cell qualification and 20-cell repair control](../../docs/benchmarks/treedb_owned_values_20261001/repair-9ab02b48/README.md).
It adds `BenchmarkOwnedMmapBudgetDenied512` and
`BenchmarkOwnedMmapConcurrentFirstAdmission` in an identical test-only overlay.
Retained stdout, stderr and time-v RSS are separate streams; source/binary/fixture
hashes and successful process statuses are checked. Prepare a new reviewed
freeze following that packet, then run:

```sh
bash docs/benchmarks/treedb_owned_values_20261001/repair-9ab02b48/inputs/qualify-mmap-repair.sh NEW_FROZEN_CAPTURE_DIR
```

Ordinary pointer Get improves in that bounded comparison; capped nil fallback
and OS-warm first-map lifecycle costs increase and remain explicitly disclosed.
The earlier packet retains its original source identities.

### Main-cache memory/placement workflow

`BenchmarkMemoryBudgetWorkflow` is a fixed public load/checkpoint/read/GC/update/
reopen workflow. Its dedicated capture freezes existing main leaf/frame cache
limits in five 64 MiB combined configured-budget splits, with two value sizes
and pointer thresholds. It records phase allocations, full Stats owners,
heap/RSS/mapping observations and filename-level logical storage bytes.

```sh
GOWORK=off go test ./TreeDB -run '^TestMemoryBudgetFixture$' -count=1
python3 scripts/treedb_memory_budget_capture.py self-check
```

Use the [memory-budget runbook](../../docs/benchmarks/treedb_memory_budget/README.md)
for the prepare/run/validate commands and external source/overlay/binary freeze.
An 8,192-key pilot is unretained; full cells use 250,000 keys in fresh processes
and wait for the reviewed harness to land. Configured cache bytes are not equal
physical RAM. These standalone package benchmark packets/logs are not
unified-bench profile-dir artifacts or benchprof inputs.

## TreeDB algorithm-work package harness

`BenchmarkAlgorithmSparseUpdates` and `BenchmarkAlgorithmGetMany` emit ordinary
Go benchmark output and JSON diagnostic packets, as documented in
[`docs/benchmarks/treedb_algorithm_work_20261001`](../../docs/benchmarks/treedb_algorithm_work_20261001/README.md).
Their package-test profiles and counter-only overlays are not benchprof inputs.
The dedicated `capture.py prepare|capture|validate` flow emits `freeze.json`,
`execution.json`, `parsed.json`, and separately hashed stdout/stderr logs.

`BenchmarkAlgorithmSparseUpdatesSnapshotRotations` adds one checked public
snapshot/read/close per acknowledged batch. It emits
`algorithm-work-snapshot-rotations-v1` packets with snapshot counts and
`snapshot_ns`, using the same freeze and raw artifact format. After review,
landing, preparation and coordinator runner grant, select it explicitly:

```sh
python3 docs/benchmarks/treedb_algorithm_work_20261001/capture.py capture \
  --source "$PWD" --prepared /tmp/algorithm-prepared \
  --output /tmp/algorithm-snapshot-rotations-eligibility --family snapshot-rotations \
  --grant COORDINATOR_EXCLUSIVE_GRANT --freeze-sha256 "$NORMAL_FREEZE_SHA"
```
