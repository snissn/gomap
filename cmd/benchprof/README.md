# benchprof

Raw value-log frame read amplification has a standalone production-planner
microbenchmark:

```sh
GOWORK=off go test ./TreeDB/caching -run '^$' -bench '^BenchmarkValueLogRawFrameRead$' -benchmem -benchtime=1s -count=3
```

It reports owned sealed-mmap read allocations and CRC bytes/checks per result.
See [the lifecycle runbook](../../TreeDB/docs/spec/value-log-lifecycle.md#13-durable-raw-frame-read-cost)
for its boundaries. Its Go benchmark text/profiles are not benchprof inputs;
use the public native Quicksilver harness for durability and workload guardrails.


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

This example uses unified-bench's `fast` preset, which resolves TreeDB to the
production `no_wal_fast` profile with checksum verification enabled. Ordinary
write acknowledgements are volatile; explicit sync, checkpoint, and clean close
remain durable boundaries. Use `-profile bench_unsafe` for the explicit profiling
ceiling.

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

### Live partition canonical search cost

`BenchmarkVectorIndexPartitionLiveCanonicalSearchV1` measures one pinned live
partition domain with 16 base and 16 delta nodes, TopK 4 and EfSearch 16, at
2 and 128 dimensions. Fixture and pin creation are outside timing;
`SearchDomainV1` query preparation, candidate scoring and result allocations
are included. Compare identical benchmark source and options on both revisions:

```sh
GOWORK=off GOMAXPROCS=2 GOFLAGS='-p=1 -mod=readonly' \
  go test ./TreeDB/collections -run '^$' \
    -bench '^BenchmarkVectorIndexPartitionLiveCanonicalSearchV1$' \
    -count=5 -benchtime=500ms -benchmem -timeout=75s
```

Retain raw Go benchmark output (`ns/op`, `ops/s`, `B/op`, `allocs/op`, and
`scorecalls/op`) with source and runner identities. For a separate CPU capture,
use `-count=1 -benchtime=3s -cpuprofile=cpu.pprof -o collections.test` and
inspect `go tool pprof -top collections.test cpu.pprof`. These standalone Go
profiles are not benchprof inputs or `unified-bench -profile-dir` artifacts.

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

### Leaf point lookup microprofiles

Capture `BenchmarkLeafPointMetadata`, `BenchmarkLeafCommonPrefixSuffixSearch`
(`TreeDB/node`), `BenchmarkPointValueMetadata` (`TreeDB/tree`) and
`BenchmarkSnapshotPublishedOwnedRead` (`TreeDB/caching`) sequentially in fresh
Go test processes:

```sh
RUN_DIR=/tmp/treedb_point_lookup_profiles scripts/treedb_point_lookup_profile.sh
# Bounded artifact smoke check; this does not measure throughput:
BENCHTIME=1x RUN_DIR=/tmp/treedb_point_lookup_smoke scripts/treedb_point_lookup_profile.sh
```

The default is `BENCHTIME=200ms`, count 1, with `GOWORK=off`. Each benchmark family
writes `<name>.txt` (Go benchmark stdout/stderr), `<name>.test` (test binary),
`<name>_cpu.pprof`, `<name>_allocs.pprof`, and matching `_cpu_top.txt` /
`_allocs_top.txt` reports. Names are `node_metadata`, `node_search`,
`tree_metadata`, and `cached_owned`. `source-head.txt`, `source-status.txt`,
`source-diff.patch`, `go-version.txt`, and `settings.txt` record source/toolchain
provenance.
Profiles include fixture setup and the whole test process; compare unprofiled
benchmark timing separately. These standalone Go profiles are **not benchprof
inputs** or unified-bench profile-dir artifacts; inspect them with `go tool pprof`.

The cached-owned family compares caller-owned `Get` and reusable-destination
`GetAppend` on 4096 published 256-byte values while retaining a queued unrelated
write. Setup/checkpoint/warmup are excluded from benchmark timers, but included
in process profiles. It isolates the cached wrapper, not generic concurrent
workload acceptance. See the [fixture and artifact contract](../unified_bench/README.md#owned-cached-snapshot-microprofile).

The corresponding single-family Go test profile command, from the repository
root, is:

```sh
GOWORK=off go test ./TreeDB/caching -run '^$' -bench '^BenchmarkSnapshotPublishedOwnedRead$' \
  -benchmem -benchtime=200ms -count=1 -timeout=2m \
  -cpuprofile=/tmp/cached_owned_cpu.pprof -memprofile=/tmp/cached_owned_allocs.pprof \
  -o=/tmp/cached_owned.test
go tool pprof -top /tmp/cached_owned.test /tmp/cached_owned_cpu.pprof
go tool pprof -top -alloc_space /tmp/cached_owned.test /tmp/cached_owned_allocs.pprof
```

## Canonical Quicksilver workflow

The [canonical Quicksilver workflow](../../docs/benchmarks/treedb_quicksilver_workflow/README.md)
uses explicit public read/update phases and a package-build freeze. Its optional
standalone Go profiles use `go tool pprof`; their names/JSON/raw logs are not
benchprof or unified-bench profile-dir artifacts.

The standalone Quicksilver workflow packet also retains40 (pilot8)
WriteSync-only update acknowledgement samples, a separate fixed-present owned
Get concurrent reader, and explicitly scoped process IO counters. See
[its measured boundaries](../../docs/benchmarks/treedb_quicksilver_workflow/README.md);
these package packets are not benchprof profile-dir artifacts.

The native `unified-bench -suite quicksilver` exports ordinary results and
per-engine `quicksilver_hits/misses/mixed/concurrent` CPU/allocation captures.
Initial/final checkpoint labels are loaded from `checkpoint_durations_seconds`
so underscore-containing engine names parse correctly. `TreeDB` and
`TreeDB (bench_unsafe)` remain separate canonical result, stats, checkpoint and
throughput-table labels when both adapters are selected. `quicksilver_results.json`
contains the detailed workload oracle, timing, latency and process-allocation
observations; it is supplementary to the canonical benchprof inputs. Shared
block/mutex/trace artifacts cover the whole multi-engine suite, including setup
and verification. Throughput and profile-overhead observations must remain
separate. See the [suite contract and commands](../unified_bench/README.md#quicksilver-shaped-kv-workload).

Optional sparse maintenance churn retains the canonical read profile names and
labels restored-key/commit counts separately from default full-refresh stress.
Retained-final measurement (`-quicksilver-measure-dir`) runs the same reads and
an explicitly idempotent writer after pre-oracle proof, without load/restore;
post-checkpoint/reopen proof includes a live-key census. It exports the same
canonical read and final checkpoint artifacts, with zero initial checkpoint
duration and no initial checkpoint CPU file. An explicit initial profile request
is rejected. Existing parsers need no new phase names. Detailed state/fixture
labels and zero setup timings live in `quicksilver_results.json`.

The optional capture `rss_samples.jsonl` and its `.summary.json` sidecar are
external Linux process observations, not pprof/benchprof inputs. The reusable
owned-process helper also observes caller-attested native `treemap` invocations.
Absolute timestamps align these anonymous/file/RSS samples with existing churn
snapshots; sample maxima remain separate from `/usr/bin/time -v` HWM. Sampling
errors and absent samples invalidate capture and retain the failed packet. See
[the suite capture contract](../unified_bench/README.md#quicksilver-shaped-kv-workload).

Quicksilver canonical results also produce a point-read throughput table in
`insights.md/html` and `quicksilver_ops` rows in `insights.json`. These read
phases remain separate from the scan-specific throughput comparisons.

### Native fixed-quantum MVCC package profiles

The optional `BenchmarkNativePruneFixedQ` package benchmark uses
`scripts/mvcc_native_prune.py`, not unified-bench database adapters. Its schema 1,
exact fixture/cap contract, source freeze, fresh-process reproduction and
qualification limits are documented in [unified-bench's native harness section](../unified_bench/README.md#native-fixed-quantum-mvcc-package-harness).

Capture with the explicit `mvcc_native_prune,treedb_test` tags requires the
bounded API; untagged builds stay unchanged and pre-M7 tagged builds fail
compilation. `--profiles` produces `case-N.cpu.pprof`, `case-N.allocs.pprof`,
`case-N.mutex.pprof` and `case-N.block.pprof`, analyzed with ordinary
`go tool pprof` and the retained `mvcc.test` binary. These whole-process profiles
include setup/oracles/final Close; package JSON separately measures complete
fixed-Q passes and final Close. Allocation deltas are labeled combined process
traffic. These files are not benchprof inputs and do not change its parsers or
`benchprof_results.json` contract. Tooling smoke and valid packets cannot qualify
the runtime while actual sync, fence and owned retained/peak measurements remain
missing; `validate --qualify` refuses that verdict.

### Native prune memory lifecycle packets

`scripts/native_prune_memory.py` produces standalone fresh-process memory
lifecycle packets; see [the commands and measurement limits](../unified_bench/README.md#native-prune-memory-lifecycle-harness).
Its forced-GC heap cuts, sampled Linux RSS and allocation scopes include
process/fixture/observer work. Retained executable, build/case command and environment bindings are validated.
Native ownership counters prove actual partial
output and custody, but do not measure exclusive cursor/tree heap. The packets
are not benchprof inputs and cannot satisfy the native runtime's exclusive
retained/peak measurement gate.
The reported sampled maintenance RSS maximum must equal the maximum of
eligible named cuts and the retained periodic RSS/call witness; periodic
sample count and call alignment are validated. Memory schema v3 additionally
retains `RSSPeriodicPeak.ProcessHWM` from the same `/proc/self/status` observation
as its peak RSS and checks that paired RSS does not exceed its paired HWM. An
empty witness has exactly zero Call/RSS/ProcessHWM. Linux RSS and VmHWM are
approximate, independently accounted snapshots: ordered cut HWMs need not be
monotone, and a later/final HWM is not an upper bound for earlier RSS. Summary
maxima remain maxima of the observed samples, including fixture/observer work.
Historical v2 packets keep their original schema, validator and source identity;
missing paired witnesses fail current validation. Pair consistency and hashes
cannot authenticate a coordinated rewrite of an entire packet.

Validation recomputes `summary.json` from validated results and receipt labels;
missing, malformed or inconsistent summaries fail closed.

Memory v3 retains four fixed-size scalar peak witnesses for overall/source
retirement cells, preparation window and native frames, including the observing
call and custody phase. Every witness is checked against all reported memory
and descriptor bounds. InputCount can witness a window before a private build
exists; private counts require real build custody. Zero maxima claim no call or
owner. Missing witnesses remain historical and fail current validation.
Receipt and derived-summary scope labels must equal the canonical capture
labels, so editing both cannot change aggregate measurements into exclusive
owner claims. Self-tests refresh result checksums and derived summaries together
to exercise case-level refusals. Work-record/byte maxima are budget checks;
they are not attained-maximum memory witnesses. These fixed scalar observations
still contribute to aggregate memory and do not measure exclusive native heap.

## Native prune foreground pilot packets

Foreground overlap counters retain approximate caller-boundary samples of public-call envelopes.
Entry markers publish before peer sampling; return sampling precedes marker clearing.
These sampled envelopes do not establish continuous public-call overlap.
Options, payload construction, first-writer notification and result/latency bookkeeping
sit outside marked calls; each flag clears immediately after the return-boundary
observation. ACK/completion attribution keeps its first post-PruneVersions activity
load. Sampling can miss overlap and has unavoidable scheduling uncertainty between
adjacent caller instructions; it does not prove simultaneous internal critical-section
execution. The parser requires the complete typed 76-field producer schema, complete
histogram objects, representable counters, operation/work/phase/stop accounting (after-stop ACKs require drain calls) and
feasible histogram bounds. Coupled hostile fixtures refresh result checksums and
reach the schema/accounting boundary without changing retained measurements.


Foreground receipts retain `capture_out`, the original absolute capture directory.
Validation checks archived files in the current `--out` directory against their
hashes and checks recorded build/run paths against `capture_out`, so moving a
completed packet preserves validation. Missing, relative or inconsistent capture
paths fail closed. Original source bindings remain required.

`scripts/native_prune_foreground.py` emits standalone schema-v2 causal packets;
see [commands, source/binary bindings and limits](../unified_bench/README.md#native-prune-foreground-causal-pilot).
Real public read/write/quantum intervals include lock wait, with fixed buckets
and sampled ACK active/stop attribution. They do not qualify tail latency;
zero-work references fence foreground and have a different start cut. These
packets are not benchprof inputs. The fail-closed validator checks eight unique
continuing-reader cases with completed post-writer reads, real data oracles, caps and executable/source bindings.

Foreground ACK and completion attribution samples writer activity immediately
at the public prune return, before counter bookkeeping. The reader and prune loop
start after the writer announces it has started; overlap is established by the
sampled call-envelope witnesses. Writer duration begins inside its goroutine.

## Current source-population audit microbenchmarks

These standalone Go benchmarks time admitted source-vector proof work and
matched pre-existing iterator, JSON/vector and six-outcome audit-plan helpers:

```sh
GOWORK=off go test ./TreeDB/collections -run '^$' -bench '^Benchmark(VectorSourcePopulationProofV1|PopulationShared.*GuardV1|BufferedRootRunsIteratorBuildManyRuns)$' -benchmem
GOWORK=off go test ./TreeDB/nativewire -run '^$' -bench '^BenchmarkPopulationLegacyColocatedPlanGuardV1$' -benchmem
```

The proof uses 512x128 and 10000x128 current rows and reports inspected entries,
asset bytes and source-entry/projection bytes per operation. The legacy audit
plan guard uses the frozen six-write/four-ID ledger and times validation or
decoding, excluding authenticated transport, quorum and current-FSM fences.
Retain raw Go benchmark output and compare identical helper bytes in balanced
fresh processes. These package microbenchmarks are diagnostic artifacts, not
benchprof inputs or evidence of service throughput, recall or cluster capacity;
the unified-bench profile format and parser contract are unchanged.

### Standalone immutable memtable safe-build witness

`BenchmarkCOWLargeKeyLookup` is a package benchmark, outside the unified-bench
adapters and benchprof profile pipeline. Capture its Get/CursorSeek/Estimate/
RefusedPrepare sub-benchmarks as plain Go test logs:

```sh
GOWORK=off go test -tags treedb_safe ./TreeDB/internal/memtable -run '^$' \
  -bench '^BenchmarkCOWLargeKeyLookup$' -benchmem -benchtime=100x -count=3
```

Repeat without the tag for the default comparator. Fixture setup is excluded;
1 MiB keys prevent short-input conversion elision from hiding allocation costs.
See [ownership and qualification scope](../../TreeDB/docs/spec/cow-memtable-ownership.md).

### Standalone COW public integration diagnostics (#5046)

These Go package benchmarks compare `append_only`, `btree` and `cow_btree` on
identical resolved durability profiles. They emit plain Go benchmark rows
(ns/op, B/op, allocs/op), latency/counter metrics and ownership receipts; they
are not benchprof inputs and do not change the unified-bench profile format.

```sh
GOWORK=off go test ./TreeDB -run '^$' -bench '^BenchmarkCOWPublicDirtyCost$' \
  -benchmem -benchtime=16x -count=1
GOWORK=off go test ./TreeDB/mvcc -run '^$' -bench '^BenchmarkCOWIntegratedPublicMVCC$' \
  -benchmem -benchtime=128x -count=1
```

The raw KV fixture uses Inline64/Pointer4096 and N=1024/2048, with capture,
owned read, forward16, incremental write, actual dirty checkpoint and explicit
write-sync cases. Setup/reseed and final close are outside timing; fixture-total
COW budget/cut counters and a concrete final-drain receipt remain separate from
per-operation allocation results. The tiny MVCC fixture retains Store fences,
uses actual CommitAt/GetAt and full exact-key history, and limits fixed counts
to128/256. Balanced fresh-process repetitions and all failures/spread must be
retained before comparing costs. These are bounded integration diagnostics;
sustained C4 qualification requires its separately landed retained harness.
See the [current repair and cost packet](../../TreeDB/docs/benchmarks/cow-c2-ci-repair-5046/report.md)
for exact collection policies, commands and limitations. The
[earlier integration packet](../../TreeDB/docs/benchmarks/cow-c2-integration-5046/report.md)
retains its historical source identity.

### Native prune experiment environment and writer stop boundary

The native memory and foreground drivers construct a minimal environment and
record the exact non-null map passed to version, build and case processes. They
fix `GOENV=off`, `CGO_ENABLED=1`, `GOFLAGS=-p=2`, `GOWORK=off` and
`GOTOOLCHAIN=local`, record `HOME`, and use the resolved Go directory followed by
the platform's default system tool path. Default caches remain under recorded
`HOME`; inherited experiment, compiler, loader and GC overrides are excluded.
An explicitly supplied `GOMAXPROCS` must be a canonical positive decimal and is
recorded; when absent, the runtime uses its host-dependent default. Cases add
only the recorded test controls. Unknown, null or inconsistent environment keys
fail closed; older receipts with the former subset environment need their original
validator and cannot be treated as evidence captured by the revised driver.

Every foreground mode ends public writer activity at its final `CommitAt`
return, including cap, duration and error stops, before latency/counter cleanup.
Duration expiry is observed at public returns, preserving the existing approximate
duration policy. The same return time binds latency and current H2
`WriterDurationNS`, the elapsed writer duration at the observed terminal return.
The pending H3 retained extension (#5043) additionally records
`retained.WriterStopNS`, measured from `measurementStart`. `writerDone` remains
the later conservative worker completion and post-writer read fence. These
sampled phase witnesses do not qualify performance or prove continuous overlap.

The dedicated [R1 collection capture](../collection_workload_bench/README.md#r1-complete-local-row-comparison)
produces a `gomap-r1-row-v1` packet and summary through
`scripts/r1_collection_capture.sh`. These dedicated workload artifacts are not
benchprof inputs and do not change the unified-bench profile filename contract.

The [R1 mutation width and request-size sweep](../collection_workload_bench/README.md#r1-mutation-width-and-request-size-sweep)
uses `R1_MODE=r1-mutation-sweep scripts/r1_collection_capture.sh` and the separate
`gomap-r1-mutation-sweep-v1` packet. A nonqualifying rehearsal uses
`-documents 32 -operations 2 -repetitions 1 -qualification rehearsal`.
It measures public cached durable acknowledgement costs and separate flush
counters across field widths and actual mutation request sizes. Its JSON,
source manifest and validator output are separate from benchprof inputs;
profile filenames and parsers are unchanged.
The producer's `r1-mutation-sweep-validate -semantic-only` result is always
`UNQUALIFIED`. Retained replay requires independent source, exact landing,
executing-binary and original-packet receipt pins; see the linked sweep contract
for all six required flags. Recorded physical file-sync calls must cover the
serial requests and written WAL bytes must be positive.

Native capture build provenance uses two separate maps. `source-bindings.json`
is the offline Go/module/policy/driver preflight map. The drivers also require
`gomap-in-repo-build-inputs-v2`: exact-tool/environment/tag/race `go list -deps
-test -json` commands, raw metadata, and the resolved in-repo input hashes before
and after build and after collection. This includes ordinary/test/external-test
Go sources, cgo/native/assembly/SWIG/system-object fields, and main/test/external-test
embedded assets. Local module replacements outside the source root refuse.

Validation rehashes archived compiled files before any metadata subprocess, then
checks the current resolved graph for additions, removals or rerouting. H3's
invalid expected-source admission still makes no output or subprocess calls;
compiled-graph failures after admission retain the normal failed receipt. These
maps bind Go-enumerated repository inputs, not external module/toolchain/system
files or arbitrary compiler includes outside Go's package metadata. Original
receipts with only the offline map remain unchanged and require their archived
validator; they do not prove this newer compiled-input boundary. Runtime result
schemas and earlier measured results retain their original source identities.

Compiled-input contract v2 separately hashes each query’s JSON stdout in
`<query>.stdout` / `stdout_sha256`, while preserving the full combined
stdout/stderr diagnostic log and its `raw_sha256`. Offline validation parses
only the bound stdout; successful dependency-download diagnostics do not become
JSON metadata. Archive self-tests retain every required query command, stdout,
raw log, and input inventory. Historical compiled-input v1 packets require their
archived validator and are not migrated or relabeled as v2 evidence. Runtime
RESULT schemas and previously recorded measurements are unchanged.

The standalone `BenchmarkR1Lifecycle5060` uses `scripts/r1_lifecycle_capture.sh`
with the supported-profile `gomap-r1-lifecycle-packet-v3` format, raw calibration/final process logs
and strict source/count validation. For a small nonqualifying rehearsal, use
`--qualification rehearsal --out /tmp/r1-lifecycle --repetitions 2 --epochs 3
--documents 32 --calls-per-epoch 8`. See the [lifecycle capture contract](../../TreeDB/docs/spec/r1-row-lifecycle.md#source-bound-standalone-capture).
These artifacts are separate from unified-bench profiles and benchprof inputs.

Lifecycle v2 measures logical fold, conditionally eligible typed rewrite/GC and
live direct-backend online vacuum with before/after census; cached-wrapper
overhead is omitted. It pins Go 1.26.4 Linux amd64 and runtime settings
GOMAXPROCS=16, GOGC=100, GOMEMLIMIT=off, GOFLAGS empty.

The lifecycle v3 scope uses `OptionsFor(ProfileCommandWALDurable)+OpenBackend`
with background prune disabled. It checks effective/persisted settings at fresh
open and reopen, inventories the full profile root (including side stores and
immutable manifest metadata), and times exhaustive owned `CompactStorage`, final
fallback convergence, typed GC and leaf GC separately. Older off-profile packets
remain historical evidence and fail this schema; physical-growth acceptance is
pending. See the [lifecycle spec](../../TreeDB/docs/spec/r1-row-lifecycle.md).


### Opt-in retained foreground duration/fence capture

Issue #5036 extends this same standalone driver with
`--retained`; the default schema-v2 causal pilot and its eight cases remain
unchanged. Retained schema `gomap-native-foreground-retained-v1` requires
`treedb_test,mvcc_native_foreground,mvcc_native_prune`. The native observer
package and interfaces are currently an explicit unmerged M7 runtime
dependency: ordinary builds exclude the fixtures, the disabled bridge preserves
the v2 tag combination, and requesting retained mode without the observer tag
fails clearly. A candidate overlay smoke demonstrates tooling construction;
it does not qualify clean main or a candidate with outstanding product failures.

```sh
# Small construction smoke; deliberately insufficient for qualification.
python3 scripts/native_prune_foreground.py --go "$GO" --retained --smoke \
  --n 64 --repetitions 1 --out "$OUT"
python3 scripts/native_prune_foreground.py --out "$OUT" --validate
python3 scripts/native_prune_foreground.py --out "$OUT" --self-test

# Only after reviewed tooling/runtime land and the exact runtime manifest is
# independently accepted: fixed Q32/1MiB, N/2N, growth/churn, fresh process/run.
python3 scripts/native_prune_foreground.py --go "$GO" --retained --n 512 \
  --repetitions 3 --runtime-status integrated-reviewed \
  --expected-source "$EXPECTED_SOURCE_BINDINGS" --qualify --out "$QUAL_OUT"
```

The runtime-status declaration and expected source digest bind the reviewed
runtime supplied by the caller; they are not an independent review attestation.
`--validate` checks capture integrity; `--qualify` additionally applies the
predeclared coverage/noise policy. Race captures establish correctness only.
The native schema-1 qualification refusals remain unchanged; this separate
capture does not populate exclusive `owned_retained_bytes`/`owned_peak_bytes`
or certify the five native schema-1 gaps.

Five raw-duration buffers have fixed capacity 65,536 uint64 values each:
2,621,440 bytes of payload allocated before collection, plus fixed headers.
Only scalar timing data is retained, with no source/DB pointer registry.
Each worker owns its recorder. Overflow rejects capture integrity; no sampling
or silent truncation occurs. Recorder writes happen after the public-operation
end timestamp and affect scheduling; observer atomics/clock calls inside
instrumented operations are included in observed latency. The fixed buffers
remain resident during measurement and are part of the instrumented workload,
not an exclusive native-owner heap measurement.

Durations use Go monotonic `time.Now` intervals in nanoseconds. Read phases
are defined by whether `writerDone` has closed at operation start; quantum
phases sample the writer-active flag at entry; all writes belong to the active
phase. Cross-boundary operations retain their start classification. Setup
quanta and their total duration are excluded explicitly. Workers join before
the first cumulative per-site observer snapshot. Cleanup duration runs from
that join through cursor/old-reader release, correctness scans, checkpoint,
reopen/pointer proof and final DB close; the second snapshot is then taken.
The collector begins once after quiescent private-output setup and never
resets during activity. Snapshots retain cumulative physical attempt/failure
counts and all nine fence-site counts/maxima. Maxima are checked against the
per-site maximum, never summed or subtracted into phase maxima. Nested holds
may include waiting/I/O; physical attempts are not universal kernel syscall
counts, and dedicated Finish-lock coverage remains limited by existing hooks.

Per-run p95/p99 use nearest-rank `ceil(p*count)` over actual durations.
Qualification requires at least 1,000 samples in each of ReadActive,
ReadDrain and WriteActive in every run, at least three fresh repetitions per
N/2N growth/churn cell, nonrace captures, and max/min <=1.25 for each phase's
p95 and p99 across repetitions. Quantum distributions are descriptive and
still require exact phase/count/duration accounting. Missing drain coverage,
overflow, source/binary drift, missing sites/tags, false counts/maxima, failed
children/oracles, or excessive spread fail closed. A tiny smoke cannot relax
this policy. All failure artifacts are retained. The original immediate
writer-activity sample at public prune return, partial-output start, overlap,
finite writer/continuing-reader, survivor/pointer/old-reader and reopen
oracles remain in force. The unbounded v2 reference has unmatched starting
custody and stays descriptive. The driver verifies testing-owned temporary
DB disposal only after the child releases all consumers and exits. These
JSON/log packets are not benchprof inputs.

### Standalone external MVCC COW read admission fixture

`BenchmarkC3PublicReadAdmission` in `TreeDB/mvcc/cowbench` measures bounded public
CommitAt+GetAt, CommitGroupAt+actual exact-key all-version iteration, and ordinary
concurrent Store calls across three profiles and inline/forced-pointer values.
Use a fresh process per leaf with fixed `-benchtime=128x` for smoke or `1024x` for
matched collection; retain Go benchmark stdout, command, exit status, source,
module/toolchain and binary identities. Setup/Close are excluded and history/output
are fixed. Reproduction and allocation ownership are documented in
[the fixture contract](../../TreeDB/docs/benchmarks/cow-c3-read-evidence.md).
These Go package benchmark logs/profiles are not benchprof inputs.

```sh
GOWORK=off GOMAXPROCS=4 go test ./TreeDB/mvcc/cowbench -run '^$' \
  -bench '^BenchmarkC3PublicReadAdmission/command_wal_relaxed/cow_btree/inline/point$' \
  -benchtime=1024x -count=1 -benchmem
```

The C3-read capability and allocation audit are documented in
[the production source guide](../../TreeDB/docs/benchmarks/cow-c3-read-5076/README.md).

The dedicated test package imports the ordinary MVCC product. Frozen fixtures
bind every selected harness file and helper, including TestMain and file modes;
product regression tests are outside that harness closure. Canonical matched
counts remain 128 warmup and 1024 measured iterations per fresh process.

### Standalone sustained public MVCC lifecycle

`BenchmarkCOWSustainedPublicMVCC` in `./TreeDB/mvcc/cowsustained` is a bounded
Go package fixture with explicit
raw JSON lifecycle receipts and fixed 1..8 epoch counts. Its 36 leaves retain
grouped growth/replacement, tombstones, old pins, joined overlap, checkpoint,
Close and reopen oracles. Reproduction, allocation scope and qualification
limits are in [the C4 fixture contract](../../TreeDB/docs/benchmarks/cow-c4-sustained-evidence.md).
The standalone collector/analyzer shares C3 provenance machinery; these
artifacts are not benchprof profile-dir inputs. Native eligibility and whole
public maintenance charge remain PENDING, and qualification remains
`pending_native_observations`.

