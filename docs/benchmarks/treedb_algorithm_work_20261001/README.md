# TreeDB physical-work decision harness (#4893)

This is an instrumentation-only harness for [#4893](https://github.com/snissn/gomap/issues/4893), under #4888. It changes no production source, default, storage format, or durability boundary. The issue remains open until measured decisions and every activated implementation child complete. A reviewed, landed harness precedes expensive retained collection; bounded pilots only qualify the constructor.

Initial provisional source is the clean union `6fffa5412db4ec70a5db614bd52f9cf7d255b210`: V `ab2e9124fcca72710b896cf78577fa6bd37a11b0` plus N `a95ffc65cf4020b41cdbf90774473a6a469d0215`. Final acceptance must synchronize merged predecessors and audit the actual diff before freezing runtime and harness identities. Root/TreeDB AGENTS.md and CONTRIBUTING.md were read at this union. This bounded benchmark/instrumentation change uses the issue's test-first exception; public behavior checks protect its constructor.

## Fixed write workflow

`BenchmarkAlgorithmSparseUpdates` uses ordinary public `Open`, `NewBatchWithSize`, `Batch.Set`, synchronous `Batch.WriteSync`, and `Checkpoint`. It loads 250,000 even keys, updates exactly 40,000 keys with `(i*7919)%keys`, and verifies every byte and every interleaved odd-key miss after close/reopen. Both fixture sizes are coprime to 7919; updates do not repeat a key. Keys are 32 bytes with shared prefixes. Values are compressible 256 bytes with an explicit key ID and generation. One update generation is permitted: the concurrent reader accepts exactly generation 0 or 1 and checks all remaining bytes.

The pilot uses 8,192 keys and 8,000 updates. Every synchronous acknowledgement has 1,000 updates. Four checkpoints occur every 10,000 updates (pilot: every 2,000); one checkpoint occurs after all updates. All acknowledgement, batch close, and final checkpoint work stays inside the shared write interval. Worker preparation precedes the release barrier. Writer failure stops and joins the reader before database teardown; a reader validation/sample failure cancels subsequent writer batches. Every operation/checkpoint count and final Close must pass before a JSON packet is emitted. Process-crash recovery separately exits immediately after `WriteSync` without Close/checkpoint and verifies replay after reopening.

The controls are inline placement (`PointerThreshold=16384`) versus persistent pointers (`PointerThreshold=1`), and existing coalescing defaults (64 memtables, 2,097,152 operations, 512 MiB) versus widened existing budgets (128 memtables, 4,194,304 operations, 1 GiB). These are admission limits, not fixture sizes or actual allocations. `FlushThreshold=64 MiB` is fixed within the primary comparison; `TREEDB_ALGORITHM_SMALL_FLUSH=1` selects a separately labeled 1 MiB cohort if missing backlog eligibility prevents the decision (other nonempty values except 0 fail closed). Automatic checkpoints, idle checkpoints, maximum-WAL checkpoints, background index vacuum, and background pruning are disabled. Existing native/adaptive flush admission remains selected normally. Its effective routing, admission, background flush, and coalescing counters are retained. The widened budget may admit no extra work for this shape: zero admission is an eligibility result, not proof that coalescing or a larger algorithm cannot help. Flush-threshold comparisons change an explicit operational control and are not engine-only wins.

Different checkpoint cadence changes amortization, publication latency, and residency; it is not an engine-only optimization. Widened budgets can spend memory. Comparison requires identical logical bytes, acknowledgement cadence, RNG, generations, host, and toolchain. Existing #2922 and #2771 implementations are reused; their historical 10MM acceptance gates are neither inherited nor waived.

## Measurements and units

Go `ns/op` is one complete fixed-work write interval including stopping/joining the reader after the final checkpoint. `updates/s` and `reads/s` use that same interval. The reader measures ordinary owned `Get`, checks all bytes outside each individual read timer, and samples every 16th completed read across the entire interval. Full read count/max and sample count/stride are retained. Sample overflow fails closed instead of truncating the end. p99/p99.9 are sampled owned-read latencies; disclose sample support and do not claim precise p99.9 from tiny pilots. Full-value validation and reader scheduling influence workflow throughput. A single reader is a bounded workload, not a capacity claim.

Go B/op and allocs/op include timed foreground/reader work. The explicit process allocation delta additionally covers reader setup immediately before release. They are not engine-only allocation figures. Post-reopen one-GC heap is a diagnostic with the live database; the stats include memory/cache/mapping owners. RSS and mmap virtual bytes must be reported separately, never summed as independent physical owners. Peaks exposed by Stats are sampled engine/process peaks, not a continuous phase maximum.

Before/after complete flush/checkpoint/command-WAL/cache/memory stats bind old leaf loads, old node bytes, operations/spans, output index/leaf bytes, WAL bytes, stages, LSNs, and background/coalescing routing. Counters must be differenced, not read as update-only cumulative totals. Engine page/node bytes are logical physical-work counters, not disk-device I/O. `/proc/self/io` is labeled **kernel process I/O**: `write_bytes` counts kernel storage-layer accounting, `wchar` counts write syscall bytes, and `cancelled_write_bytes` remains separate. None alone proves total device writes. Missing Linux I/O is an unavailable observation, not zero. File sizes are logical retained bytes keyed by filename; redo WAL and persistent value/leaf logs must remain separately classified. Allocated filesystem bytes and recoverable/pinned-generation debt require the native debt audit or external runner manifest before a final storage claim.

## Naturally supplied batches and independent descent diagnostics

`BenchmarkAlgorithmGetMany` supplies sorted contiguous, unsorted clustered (128-key neighborhood), and uniform batches of 64 directly to public owned `GetMany` and callback `GetManyView`. There are 128 deterministic batches (seed 4893), duplicate inputs, and interleaved misses. The entire fixture and every supplied batch are verified before timing. Owned results preserve input indexes; callbacks identify inputs by index and may arrive out of order/concurrently. Both modes check identical found/missing/full-value semantics; their different ownership costs are reported separately. Batch timing includes **API plus full consumer validation**, not engine-only time; a traversal ceiling requires separate work/CPU attribution. The constructor test additionally proves old snapshot generation stability and that mutating owned output cannot alter storage. Existing contract tests continue to own callback-error, empty-value, and closed-snapshot behavior.

Existing leaf-group counters do not count internal descents. `scripts/treedb_algorithm_work_overlay.py` creates a disposable Go overlay that counts actual successful internal-page loads at `loadNodeViewWithLoadKindInto`, the common point/batch traversal seam, after integrity checks. The separately generated public-path diagnostic resets before each stable-root batch and reports actual visits and unique `(root,page)` identities. The latter is **potential union visits**, a work lower bound; it is not an implemented traversal and proves no throughput improvement. It covers fallback work too. The mutex and map overhead are isolated from timing. Do not run benchmarks through the overlay.

The script records original source SHA-256/Git blob, runtime HEAD/TreeDB tree, and generated source hashes. It fails if the instrumented function marker changes. Full collection compares the claimed runtime HEAD to actual checkout HEAD, rejects dirty/untracked relevant sources, and checks an external freeze JSON binding `runtime_head`, `runtime_tree` (the TreeDB Git tree identity, which binds its blobs), `harness_sha256`, and `binary_sha256`. The executable is hashed before timing. Build/freeze the executable from the reviewed clean source before collection and preserve that external manifest; recording an environment variable alone is insufficient. Pilot mode is explicitly unretained and does not require this production freeze.

## Reproduction

On the exclusively granted Linux runner, use Go 1.26.3, `GOWORK=off`, persistent build cache, and a dedicated source/tmp/log directory. No other timed run may overlap. Keep raw successful and failed logs. Package test profiles are ordinary Go profiles, not benchprof inputs.

```sh
GOWORK=off go test ./TreeDB -run '^TestAlgorithmWork(Fixture|AcknowledgedReplay)$' -count=1

# Bounded constructor pilot, only after runner grant and source preflight.
TREEDB_ALGORITHM_PILOT=1 GOWORK=off go test ./TreeDB -run '^$' -bench '^BenchmarkAlgorithmSparseUpdates$' -benchtime=1x -count=1 -benchmem -v
TREEDB_ALGORITHM_PILOT=1 GOWORK=off go test ./TreeDB -run '^$' -bench '^BenchmarkAlgorithmGetMany$' -benchtime=1000x -count=1 -benchmem -v

# Counter-only pass; use a fresh output directory and bind its manifest.
python3 scripts/treedb_algorithm_work_overlay.py "$PWD" /tmp/algorithm-overlay
TREEDB_ALGORITHM_PILOT=1 GOWORK=off go test -overlay /tmp/algorithm-overlay/overlay.json ./TreeDB -run '^TestAlgorithmWorkInternalVisits$' -count=1 -v

# Full retained run: build once from reviewed+landed clean frozen source,
# then independently record the external freeze JSON and run that binary.
GOWORK=off go test -c -o /tmp/algorithm-work.test ./TreeDB
TREEDB_ALGORITHM_RUNTIME_HEAD=FROZEN_RUNTIME_HEAD TREEDB_ALGORITHM_FREEZE_FILE=/tmp/algorithm-freeze.json /tmp/algorithm-work.test -test.run '^$' -test.bench '^BenchmarkAlgorithmSparseUpdates$' -test.benchtime=1x -test.count=1 -test.benchmem -test.v
```

Final decision collection needs repeated fresh-process serial cohorts, independently frozen profiles for internal traversal share, and all requested closure/debt observations. Report the actual repetition/noise policy before collection. Unmeasured larger-than-memory capacity stays open.

## Conditional implementation gate

Persistent deltas need reproducible whole-leaf materialization dominance after eligible existing checkpoint/coalescing controls, plus a design preserving overlay reads/scans, captured base/delta generation pins, acknowledged-LSN replay, sealed checkpoint closure, and reachability-based GC. Shared traversal needs actual internal visit reduction on naturally supplied batches and profiles showing that repeated internal work materially limits the equivalent owned/callback route. Both require a stable measured improvement target and allocation/read-tail guardrails before the coordinator activates a separate durable implementation child and updates the parent edges. Eligible children execute in this graph.

A negative decision requires measured cause/cost and a concrete revisit threshold. Counter opportunity alone, zero eligible backlog, noisy timing, or unmeasured capacity cannot close the decision as no-go. This harness makes no present go/no-go or speedup claim.
