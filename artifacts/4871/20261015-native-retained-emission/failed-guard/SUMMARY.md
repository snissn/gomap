# Ordinary retained-materialization viability guard: allocation regression

The diagnostic guard FAILS its no-allocation-regression criteria. All six planned fixed-1000x samples exited zero and retained unchanged public fixture assertions; no timeout or rerun occurred. Median latency passes the <=5% guard, but bytes and objects increase consistently.

| Role | Median ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Baseline | 25502 | 46331 | 148 |
| Candidate | 25698 | 46467 | 149 |
| Change | +0.768567% | +136 (+0.293540%) | +1 (+0.675676%) |

| Pair/order | Baseline ns/B/objects | Candidate ns/B/objects |
|---|---|---|
| 1 baseline/candidate | 25689 / 46331 / 148 | 25698 / 46467 / 149 |
| 2 candidate/baseline | 25502 / 46330 / 148 | 25672 / 46467 / 149 |
| 3 baseline/candidate | 25181 / 46331 / 148 | 25755 / 46466 / 149 |

Independent parsing of raw stdout and Decimal ratios confirms the runner result. The candidate adds one object in every sample and 135..137 B/op in each planned pair. Do not silently relax the guard or claim ordinary allocation viability. The source owner must resolve the regression or obtain an explicit scoped residual-cost disposition. No profile or additional measurement was launched to attribute the cost. This does not challenge the separately passing correctness/ownership controls.

Baseline is the complete committed source `353a88fb160febce951dd4caa45ac079c261fca4`, tree `d407230a25418d307b382f3421b0604d3b8d74cb`. Candidate is exactly that source plus the five paths frozen in `native-retained-materialization-20261015/candidate-paths.json` (SHA256 `b9c5d1d81f22ab3bad890ffebab84d0db3787b955e97cfe003c47e43efba8c68`). Complete Go/module source censuses differ only at those five paths: two modified files and three additions. Each source was isolated with the maintained worktree helper using explicit ordinary Git. Native source/index and protected foreign hashes remained unchanged. No source was committed, pushed, staged in a foreign index, or edited outside the two owned roots.

The exact maintained public harness is `TreeDB/db/conditional_kv_contract_bench_test.go`, `BenchmarkConditionalTxnBaselineBatchWrite`: blob `ae3a6d3cd0ca244781e7843168dc5b35aac5d898`, SHA256 `e428911df6d694614dd7f0c5e58cb28490102d68c4262c77756b929c65f070b6`. It is byte-identical to the earlier triad harness at both source variants. It uses 10000 seed entries, a fixed 100ms root-publication delay, stopped-timer Checkpoint every eight writes, and asserts no foreground publisher calls or unpaid publication debt. Setup/seed, Checkpoints and final DB Close are excluded; each batch Close remains included. No BenchmarkBatch/WriteSync/1000-entry substitute, native selector, engine-default override or lifecycle amortization change was introduced.

Both standalone db test binaries were built serially using Go1.26.3, explicit GOROOT, GOTOOLCHAIN=local, GOWORK=off, CGO_ENABLED=1, GOMAXPROCS=1, and `go test -c -trimpath -buildvcs=false`. Each build was limited to120s, with parent aggregate180s; observed aggregate92.9276s. Exact six timing processes ran AB/BA/AB on CPU0 with GOMAXPROCS=1, run=^$, exact benchmark selector, count1, benchmem and1000x, under parent aggregate60s; observed0.8418s. Before/after source/module and binary censuses matched. Boundary process/load/stat snapshots show no competing Go benchmark/profile process; checks are not continuous interference monitoring.

Arena-first decision: existing batch arenas, caching copyArena/leases, memtable arenas and value-log scratch were inspected at the exact base. Candidate traversal/encoding controls reuse fixed depth-bounded state. Encoded 4KiB outputs may be retained by CandidatePageWriterV1 past preparation/publication and cannot share resettable batch/memtable/vlog scratch lifetime. Detached owning-safe page bytes and generic-sink isolation remain required. This guard changes no arena, allocator, pool, buffer lifetime or persistent value-log behavior; it measures the same canonical ordinary nil-policy core introduced by the candidate. Retained-page, alias, failure and public promotion correctness results belong to the frozen source packet. Retained/peak memory was not measured in this short guard.

This is a short ordinary default-engine foreground Batch.Write guard, not final eight-repeat/M0/Dgraph/native ACK qualification. All exact commands, exits and raw samples are retained. `retained-generated-inventory.json` and full metadata snapshots identify the two new worktrees and ELF outputs for later cleanup; no cleanup is authorized until root release after durable publication. `publication-files.json` excludes only the two ELFs; preserve them locally meanwhile.
