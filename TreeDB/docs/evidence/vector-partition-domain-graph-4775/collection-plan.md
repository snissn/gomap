# Prospective domain-graph comparison for #4775

Prepared before new outcomes. This uses the already selected real100K x768, 512-query population; it is not holdout evidence. #4775 remains open and #4753 remains the sole local scaling verdict.

Measured baseline remains landed `c6dcbcf0c0386fb156392319b2b1f10cc4b99fa3`, original per-pack database and clean binary at `.111:/home/mikers/gomap-4775-homes-c6dcbcf0c`. The new measured candidate and comparator will both be frozen to the landed correction for #4823 (`1cde6b84b4a2d94b84394dc5e0d949f20c831298`). This is a new prospective candidate after the first campaign stopped; the incomplete d9 report is never repaired or accepted. In addition to the domain-graph product #4804, this candidate includes landed prepared inserts #4820 and sparse catalog participation #4815. Construction and runtime results therefore qualify the complete pinned candidate; any attribution solely to one change remains limited. Full source/tree/blob inventories and binary/build/launch/input pins will be published before collection. No collection starts until #4823 is reviewed and landed.

Prior stopped attempt: https://github.com/snissn/gomap/issues/4775#issuecomment-5852080320 . The complete raw attempt, its outcome inventory, and the freeze compile telemetry gap remain preserved. The correction changes only report boundary validation and its regression; it changes no search parameter, measured gate, report schema or population. Fresh candidate construction and BOTH serving arms start at external block 1; no old successful arm substitutes for new measurements.
Only native source-specific producers and strict replay are used. The old database stays on its original filesystem with its physical identities unchanged. No relabeling, copied serving DB, metadata rebind, decoder bypass or synthetic combined reports. The comparator launches the two pinned producing binaries; its source is the same new landed commit as the measured candidate, with a separately recorded binary and identity inventory.

## Fixed construction and serving coordinates

Original fixture and anchored truth: `/home/mikers/gomap-4753-final-f406ad74e/inputs/real100k`. Preserve their published checksums, seed4017, source ordinals and parent artifact `c42c6a3437e4055178d3e41c86c8e1f82da9a4e228715ecda35191692a14fd33`. Fresh construction must reproduce the graph-home shard-generation digest `e7b6f24ebb60255b5a339800406549fbdadacc2fe3b07a93a593f8a83376b41d`. If identity, membership, plan, router or accepted graph parameters differ, retain the result and stop.

Build one fresh candidate with the existing M3 command: D16, four storage packs/domain, target hot bytes7,696,384, planned overlap allowance20%, actual overlap0, pinned graph-home adapter/KaHIP3.25, source HNSW degree16, accepted connectivity-preserving Vamana R64/L256/alpha1.2. The candidate builds one full logical-domain graph split into bounded colocated storage chunks. The baseline retains its graph per physical pack. Full source insertion and construction wall/CPU/RSS/storage costs are retained; new retained-open timing is not represented as fresh baseline build time. No new optimizer, seed sweep or search retuning.

Serving: fixed P5 and exhaustive P16; C256/EF96/top10/recall>=95%; four native groups, three nodes/group; c1,c32; warmup64; GOMAXPROCS16; GOMEMLIMIT3GiB; five internal measured windows and complete terminal attempts per report. Every runtime opens its own built assets. Candidate must show one traversal per selected logical domain, baseline four per selected domain; all required/opened/accessed chunks and actual memory/handle work remain charged.

Five external blocks, with serialized arms and fresh idle-host admission before each:

1. perpack, domain
2. domain, perpack
3. perpack, domain
4. domain, perpack
5. perpack, domain

Each arm produces20 cells (five windows x two probe coordinates x two concurrencies), totaling ten reports/200 cells. Each block has ten selected treatment ratios and both same-report selected/exhaustive controls. Internal repetitions are not independent external blocks. Retain every unsuccessful cell and actual chronology. Do not pool reports or substitute successful subsets.

## Gates, resource evidence and stop rule

The treatment requires >=1.15 paired QPS, no p95 regression and >=95% recall in every selected window at both concurrencies. Confirm reduced navigation and serving CPU consistent with the changed traversal unit. Use the existing `serving-resources` companion per external report, preserving one worker-window CPU/bytes/allocation observation for each coordinate and all five prospective external observations. Resource collection and replay are serialized outside serving timing. Resource arithmetic/receipt authentication reuse the unchanged frozen external reader against the report's own producing source. Publish actual CPU, allocation, RSS, mapped bytes, handles, physical bytes and build/storage costs; no significance claim from one observation.

Prospective futility rule: stop after any construction, identity, safety or quality rejection. After a complete strictly replayed external pair, any failed required QPS/p95 window decisively prevents the all50-window acceptance gate; preserve that negative result and mark remaining blocks unmeasured. Collect the completed pair's bounded resource observations to explain the mechanism if still safe. An early stop is rejection/incomplete, never PASS. Do not revise the seed, EF, routing budget, caps, query population, gate or stopping rule in response. All50 windows and resource observations are required for a positive claim.

This comparison does not accept the historical one-pack packing gate, fourfold probe reduction, overlap tradeoff, ordinary whole-index superiority, 250K scope, lifecycle/recovery or distributed/Raft qualification. Those remain #4753 and later graph obligations. Existing negative evidence remains immutable.

## Admission and retention

Use only freshly admitted shared LAN .111: load<2, MemAvailable>=16GiB, disk>=50GiB, and no active competing benchmark. No foreign process termination. Record `INFRASTRUCTURE_UNAVAILABLE: dedicated runner: using admitted shared LAN .111`; persistent Go cache and durable local artifact storage exist. Existing system swap occupancy is recorded, while each task has MemorySwapMax=0.

Construction: 12GiB cgroup, 60-minute deadline. Serving/resource/replay/comparison: 8GiB cgroup; existing wrapper deadlines40/40/45/60minutes, and comparator20minutes per producing-runtime replay. Preserve time, exit status, cgroup peak/events, admission snapshots and profiles. The executor grants exclusive heavy-lane ownership for each run; P2/P7 tests yield while retained timing runs.

Fresh output root: `/home/mikers/gomap-4775-domain-<first nine characters of the new landed candidate>`; it must not exist before preparation. Commands, external pins, source/binary inventories, launch wrappers, raw attempts, receipts, failures, comparisons and resource projections are retained without overwrite. Freeze invocation/fixture and normalized command pins before execution; report hashes only at publication. External wrappers change paths, arm/runtime selection and declared comparator kind, reusing the landed harness and unchanged command/resource readers. Any meaningful harness/schema correction requires review, a new frozen identity, and affected recollection.

Baseline preservation is checked against the original published inventory (SHA256 `85853ddfaf99c158da5574e5b21e111a847b705d5020ff11e3f75a5977a84e77`), restricted without rewriting entries to the original graph DB. Verify all of those files before construction and after the campaign, plus the original binary/dependency manifests; unexpected differences stop the campaign and are investigated, never silently re-pinned. The new freeze manifest also pins both baseline binaries, the baseline descriptor and the derived inventory.

The build wrapper enforces the declared shard generation, parent artifact and full graph/partition/router coordinates against the pinned baseline descriptor before publishing its accepted-descriptor pin; serving requires that pin. The comparison wrapper rechecks original baseline DB contents after each completed pair, outside the timed interval. Repeat the same baseline inventory check after the final resource reader; retain its exit status/output. Replay and comparison also enforce the stated disk admission threshold.

Strict replay is performed once per producing runtime by the landed cross-runtime comparator, which retains both receipts and rehashes report/executable inputs. The report freeze step only pins the finished producer output. There is no redundant standalone replay invocation before that comparison.

The new compile freeze invocation is itself retained and pinned. It applies an 8GiB/no-swap cgroup and 20-minute deadline, recording time, exit status, actual cgroup limits/peak/events, and outer invocation status. These are apparatus-build receipts, separate from construction/serving evidence.
