# Direct bottleneck profiles: MVCC #4871

Captured 2026-10-15 on quiet Linux runner `192.168.0.111`, AMD Ryzen 7 9700X, Go1.26.3. **DIAGNOSTIC, not speedup qualification.** No production code changed during capture. Native bounded production still refuses before ACK; this native profile is of the explicitly named completed unbounded diagnostic.

## Measured bottlenecks

| Operation and denominator | Measured result | Engineering implication |
| --- | --- | --- |
| Ordinary public `db.Batch.Write`, **3.86s/386 CPU samples** carrying the exact Write frame | Allocator candidate preparation **54.92%** CPU; nested `FreelistTxn.MaterializeCandidate` **48.45%**. `mallocgc` **33.68%**, overlapping those paths. | Optimize actual allocator generation/page materialization rather than small resource wrappers. |
| Same exact public Write frame, separately observed Rate1 allocation stacks, **464,099,984B** aggregate | Allocator preparation **72.14%** bytes; nested materialization **67.84%**. Encoding functions **57.23%**; state cloning **20.35%**. `encodeIndexPage` alone **42.79%**. | Page encoding is the largest byte source, with persistent-state path copying second. These are aggregate stack shares, not bytes per operation. |
| Completed native prune diagnostic, **0.92s/92 foreground CPU samples** carrying `PruneVersionsUnboundedDiagnosticForTest` | Root-publication finalize **61.96%**; nested durable-resource capture **33.70%** and external/value-log reference scanning **31.52%**. `mallocgc` **20.65%**, overlapping. | Repeated publication/reference discovery is an architectural cost. Complete the genuine retained native path, then check that the actual production path avoids rediscovery; do not assume the diagnostic profile transfers to that path. |
| Public backend `GetVersioned`, **4.02s CPU**, 0B/0alloc benchmark | Snapshot acquisition+close union **56.22%**; mutex/atomic union **30.60%**, overlapping. Tree lookup **41.54%** cumulative. | Read lifetime synchronization is expensive even with zero allocation. Reducing allocations cannot fix this cost. These are backend microbenchmarks, not end-to-end Dgraph evidence. |
| Public Snapshot iterator Seek, **3.78s CPU**, 0B/0alloc benchmark | `Iterator.loadNodeRef` **42.86%** cumulative; nested by-value `loadChildRefView` **29.63%**. | Investigate repeated node-view load/copy plumbing and existing `...Into` seam. Copies are a source-backed hypothesis, not an isolated causal result yet. |

**Cumulative percentages overlap and must not be added.** CPU samples are 10ms each; the native foreground has only92 samples, adequate for ranking broad paths rather than precise percentages or speedup predictions. Native setup consumed most of its whole-process16.05s CPU, so whole-profile percentages are deliberately not used as foreground-prune attribution. Background GC cannot be causally assigned to prune by this stack filter.

For ordinary Batch CPU the whole process was5.28s: allocation/GC stack union was1.93s(36.55%); part is background or outside the exact Write frame. Inside the exact Write frame, allocator preparation2.12s, materialization1.87s, allocation1.30s. This establishes allocation-related CPU cost without attributing every background collection to a particular operation.

## Source explanation and pre-sprint comparison

The ordinary allocator bottleneck **predates the sprint**. Pinned pre-sprint `28aea01e` and current `177fb0c8` have byte-identical `generation_v1_tree.go`, `allocator_cow.go` and `conditional_kv_contract_bench_test.go`; exact copies are preserved. The transaction delta extracts append installation; this is not a newly introduced full-tree rebuild.

`generation_v1_tree.go:21-23,190-210` uses a256-page chunk and14-edge radix path plus terminal index. Changed nodes receive fresh4KiB encodings (`generation_v1_format.go:234-235`). Previous generation allocator metadata itself enters retirement (`generation_v1_txn.go:230-237,730-746`), so small user writes can generate appreciable metadata work. Existing batched retirement/pruning, unchanged-subtree sharing, candidate-owned sink and prepared retry are already used. Removing isolation or skipping dirty DATA materialization is not a valid fast-path selection change.

Current `root_publication_activation.go:733-735` already skips preparation for PRIMARY with no DATA delta; the original path was unconditional. This does not exempt ordinary DATA writes. The profile therefore supports an allocator optimization opportunity, not a claim that new MVCC code caused its entire cost or that an existing batching switch is unused.

The benchmark overwrites a single key among10,000 seeded keys and drains Checkpoint every8 writes outside the benchmark timer. Whole-process CPU/memprofile still sees seeding, calibration and drains. `stack-attribution.json` uses the **exact `db.(*Batch).Write` frame**, excluding `WriteSync` setup and Checkpoint stacks. The foreground still inherits the allocator history created by that fixture. Do not extrapolate these shares to all write shapes.

## Locks and blocking

Separate150-fixture diagnostic capture enabled mutex fraction1 and block rate1. All mutex delay was1,867,213ns summed; the foreground prune stack accounted for476,294ns. Foreground block delay was331,443ns. The whole-process36.535s block sum mainly contains idle background channels/selects and testing waits; it is not36.535s application latency.

This mostly serial diagnostic **does not test the original concurrent caching SeekGE/writeMu contention hypothesis**. GetVersioned mutex CPU is synchronization overhead, not measured waiting. A representative concurrent reader/writer capture is required to resolve that hypothesis; no claim that the lock issue is fixed is made here.

## Execution decisions

1. Native owner continues actual bounded production ACK/typed original Set/fresh publication binding/physical Finish; no diagnostic allocation micro-loop. Once viable, immediately collect clean public latency and separate outer allocation, then verify publication/reference discovery CPU on that actual route.
2. Ordinary #5111 performance work should first discriminate allocator metadata-path amplification using the existing fixed100x public benchmark plus existing `COWPrepareProfileV1`/candidate page/chunk/path counters outside timing. Choose the actual encoded-page-count or transaction-private clone seam from that evidence. Existing helper-path batching is already active. Do not open another broad correctness campaign or implement a speculative radix rewrite from this profile alone.
3. Retain zero-allocation read lifetime and iterator load costs as separate measured targets. Reuse maintained lifetime/load APIs only where the exact public semantics and branch-execution checks hold.
4. Original batch/seek/pointer regressions, matched pre-sprint/current-main controls and final Badger/M0/CI/review gates remain open. These observers perturb timing and are not clean comparative speedups.

## Reproduction and identities

`commands.json` contains exact capture and analysis argv, CPU affinity, environment, wall time and each raw output hash. Ordinary CPU captures use CPU0/GOMAXPROCS1 and3s; the separate allocation observer uses3000x with MemProfileRate1. Native diagnostic CPU uses CPUs0-3/GOMAXPROCS4 and1200 independently built test fixtures; contention uses150.

Ordinary committed source: `177fb0c8db1d19c79d3d975126b4b0b64019568e`. Its3735 Go/module inputs and clean Git state were checked before/after all captures. Native owned-source checkpoint: `d7c27d7829e51ad06822340fcc55f03c3831f027`; capture uses the frozen after-binary from the4399-input shared-workspace packet, not the mutable implementation worktree. That packet includes protected/foreign input census; native d7 alone is not asserted to reproduce its entire test binary. Native binary SHA256 `667990cf2be1cfa1fa1a85317f681e81a144bfc1d0515a33ddff0fc6e6a603d7`.

All1200 native CPU fixtures asserted diagnostic ACK1,90 prepared observations, exactly1 intervening write, old/current/post-ACK checks and checked Close/reopen. Production bounded ACK0 is unchanged by this measurement. Retained binary identities are in `ordinary-binaries.json`/`native-binary.json`; compiled binaries stay on111 and are excluded from Git. Raw CPU/mutex/block/alloc profiles, decoded stack tables, pprof top/cumulative output, source census and commands are published here. `analyze-direct-profiles-20261015.py` reproduces the aggregate stack filtering and overlap-safe unions from decoded tables.
