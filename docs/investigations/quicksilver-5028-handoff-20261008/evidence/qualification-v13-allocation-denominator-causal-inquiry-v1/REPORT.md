# Source-only allocation denominator inquiry

R completes more reads and still allocates more. Original repeated allocation regressions and all gates remain unchanged. The eight-second process-allocation numerator nevertheless covers radically different amounts of overlapping writer work: A is strongly reconstructed inside checkpoint1; R has reached checkpoint4. Neither these counters nor an R-only profile establishes an exclusive per-read regression.

Exact objects: A `192e5019e579af3b98ae4fbf4ea497bb9ccd672f`, R `6fa638b8bffa71750e1aefa0bb746bb747aa06c1`. Used read-only Git object commands, no checkout, runtime/tests/native access, product/Git/state changes, or acceptance. Scratch/CI integration is outside this measured evidence. Held #5004/PR5006 and adjacent c9f85 excluded. Source/object/input SHA custody and all selected exact scalars are in `source-bindings.json`; all runtime/decision flags are false.

## Actual original six runs

Root retained genuine native original run/stdout bodies: `actual-A-R-primary3M-original-run-scalar-readback.json`,44,902,310B, SHA256 `c666f698a6c6e814846734de8a8283b32d7380c4d8c6cbba1865ba5c03541184`. This inquiry programmatically selected scalar fields without copying engine stats. Observation-packet allocation-ledger B means candidate R in this R view. Ordinary ledger calibration values are not actual pair timings.

| Run | Read ops | Window TotalAlloc B | Window Mallocs | Reader s | Composition s | Update sum s | Checkpoint sum s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A1 | 2,960,384 | 557,799,632 | 616,198 | 8.000673 | 1194.663814 | 31.008847 | 1162.308829 |
| A2 | 2,968,576 | 557,195,304 | 618,511 | 8.002464 | 1191.883205 | 4.048588 | 1186.626112 |
| A3 | 2,982,912 | 561,743,008 | 619,631 | 8.002459 | 1217.560476 | 30.950214 | 1185.244976 |
| R1 | 4,191,232 | 1,871,917,760 | 2,397,015 | 8.000872 | 8.102033 | 1.879335 | 0.963815 |
| R2 | 4,170,752 | 1,879,635,112 | 2,407,874 | 8.001423 | 8.144564 | 1.971238 | 1.001526 |
| R3 | 4,176,896 | 1,874,751,600 | 2,413,267 | 8.001175 | 8.114726 | 2.000439 | 0.978634 |

R/A reads=1.416/1.405/1.400; byte numerators=3.356/3.373/3.337; malloc numerators=3.890/3.893/3.895. Normalized bytes=2.370/2.401/2.383 times A; objects=2.748/2.771/2.781 times A. More reads dilute R's larger numerator. Fewer reads or weaker amortization do not explain this result. Original repeated-regression flags and allocation/performance gates stand.

All six eventually complete the same work:3M keys, seed24, uniform90% misses, four readers,64reads/snapshot, three6M-read fixed phases and8s concurrent readers. Writer40,000 mutation records,40 groups,160 ordinary commits;10,000 updates/deletes/inserts/overwrite targets each,40,000 overwrite Sets (60,000 total Sets plus10,000 deletes). Initial checkpoint=0. Separate final checkpoint ms: A7394.415391/54.839089/562.980328; R51.173780/51.245744/54.614668; outside concurrent allocation window.

## Counter cut and reconstructed writer overlap

A/R harness files are byte-identical. `cmd/unified_bench/suite_quicksilver.go:580–609` takes MemStats before start, waits readers, takes final MemStats/stops profiling, then joins writer. Seconds is reader duration; CompositionSeconds and StatsAfter include writer tail. Allocation divides whole-process Go allocation by reader Ops (`:608–620,671–672`), including concurrent writer/drain/snapshot/GC activity but excluding later writer work and native allocations. StatsAfter does not recover progress at the cut. Final checkpoint is separate (`:1006–1013`).

Writer group i is paced at i*0.2s, with synchronous checkpoints after groups10/20/30/40 (`:960–997`); normal reader deadline does not cancel writer. Replaying `max(previous_end,i*.2)+update_ms`, plus quarter checkpoint duration, predicts measured composition within2.7–5.8ms in all six runs:

- A checkpoint1 begins1.8436–1.8447s and ends1073.859–1078.940s; its own duration1072.015–1077.096s spans the8s reader window.
- R checkpoints1/2/3 end2.063–2.069s /4.080–4.104s /6.098–6.117s. Group40 update completes7.844–7.867s; checkpoint4 ends8.099–8.141s.

Strong timing evidence therefore places about10 completed A mutation groups/checkpoint1 active versus40 R groups/three completed checkpoints/checkpoint4 active at the cut. Much more of R's eventual fixed writer work enters its numerator. This is source/timing reconstruction, not exact cut telemetry: launch/guard/accounting overhead omitted; exact cutoff progress remains UNKNOWN. Preserve this uncertainty and original gates.

Equal-work fixed mixed reads provide a useful control: A6M reads,79.36–80.14B/read and0.210865–0.210897objects/read; R6M reads,68.07–68.41B/read and0.131376–0.131388objects/read. A uniform new cost per read across all phases is unsupported; concurrent attribution remains UNKNOWN.

## Finite existing ownership seams

Frozen R diagnostic:4,640,512reads,1,958,098,032B,2,583,250mallocs. Sampled alloc_space displays153.76MB scratch refill (already selected refinement),134.52MB `cloneStateNode` +111.24MB `stateChunk.clone`,263.47MB `HashSorted.NewWithCapacity` chiefly snapshot rotation. Display units/samples are not exact component totals; cumulative stacks overlap. R-only cost evidence cannot establish A/R causal regression.

**Freelist repeated allocation paths are the best specific remaining writer inquiry.** A/R allocator and persistent-tree source files are byte-identical. `freelist/generation_v1_tree.go:124,190` clones trie nodes/chunk for each mutation (depth14/chunk256); `generation_v1_txn.go:277–298` performs one full-path mutation per reused allocation. Existing `allocator.go:248–265` AllocMany still loops single COW allocations and updates the hint to each chosen ID. Retirement/prune are already chunk-batched (`generation_v1_txn.go:413–455`); do not duplicate this optimization. Discriminate Allocate versus retireMany/materialization stack contributions and existing StateMutationPaths/Items, ReuseAllocations and ChangedChunks per equal completed writer group. If repeated allocations dominate, inspect whether existing finite AllocMany callers can coalesce chunk-path copies while preserving identical selected IDs, per-ID hint feedback, reservations/accounting and partial-error results. Actual safe caller coverage and gain are UNKNOWN; no implementation selected.

Do not infer exclusivity from pageID0: `cloneForAllocatorPrepare:198–215` shares a dirty persistent root with an isolated staged transaction. `materialize:724–740` consumes on success/failure and detaches shared unmaterialized nodes before durable annotation; retry starts from immutable base. Preserve old generations, prepared rollback, reservation ledger, two-slot/read horizons and independent fences. Existing tests: `TestFreelistGenerationV1_OlderGenerationAndPagesRemainImmutable`, `..._MaterializeDoesNotAssignIDsIntoBase`, `..._HorizonPreventsPrematureReuse`, `TestFreelistTxn_AllocateSkipsAnotherCandidateReservation`, `..._SinkFailureConsumesTransaction`, `..._PartialSinkFailureBurnsTailUntilRetryPublishesAbandonment`, plus batched-retirement byte equivalence. Read only; not run.

**Producer proof followed by Manager recapture is a finite new construction seam.** `db/apply_leaf_resources.go:30–66` allocates builder/binds callbacks; `:69` onwards validates complete raw inventory and owns failures. `db/durable_root_runtime.go:1090–1117` captures registered handles, then removes raw outer-leaf writer tokens from producer resources while retaining dictionary/template authority. R profile `cloneSharedPinnedDirectory` displays27.52MB flat/29.52MB cumulative; its independent pinned-file/directory/identity/namespace lifetime is required (`rootpublication/resource_token.go:598–652`). Existing unscoped immutable kind-view sharing already avoids entry rebuilding (`resource_set.go:2640–2682,3647–3672`). Count producer raw entries/aborts, fresh Manager entries, group finalizations and existing closure clone/share work with stacks. Ask whether repeated immutable proof construction can reuse an existing retained directory/kind view at the same authority boundary. Remaining safe redundancy is UNKNOWN. Preserve raw completeness, Manager generation pins, content sync, final root/descriptor coverage, scoped namespace/frontier validation, release/rollback/retry and independent custom/index barriers. No mutable sync certificate, registry or sync skipping proposed.

Snapshot rotation is pre-existing source, potentially made more frequent by different overlapping work: A/R `caching/snapshot.go` byte-identical; `AcquireSnapshot:249–278` freezes dirty mutable shards by rotation, while readers reacquire every64 reads (`suite_quicksilver.go:526–532`). More writer groups within8s can cause more rotations/preallocation. Measure actual rotations/shards/capacity/bytes at the cut and per matched mutation interval. Keep transient snapshot data immutable through all borrowed-view Close lifetimes; no freeze or pin removal supported.

## Discriminating next step

Root-owned matched A/R profiles should include actual group/checkpoint progress at the reader MemStats cut, and compare stack/counter costs at equal completed writer groups as well as original whole-window service. First verify removal of selected scratch stack on exact refinement, then investigate repeated freelist allocation-path clones if matched-work counts confirm dominance. Compare A evidence, not R alone. No gate changes or source acceptance. Fewer copies could reduce transient allocation/GC; retained scratch/path/views may prolong heap lifetime. No process-RSS or retained-memory benefit established.
