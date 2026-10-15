# Exact public Batch.Write triad comparison

Three fixed-1000x samples per exact source completed in a balanced rotating order, with every process exiting zero. This is the missing short #5130 component/pre-sprint diagnostic; it does not replace final eight-repeat micro, M0/Dgraph, sustained maintenance, or native qualification.

| Role | Exact source | Median ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| Pre-sprint | `28aea01e19f1d62743dec5625ec3d22c1acb6703` | 45144 | 152663 | 281 |
| Ordinary | `177fb0c8db1d19c79d3d975126b4b0b64019568e` | 47476 | 154839 | 286 |
| Combined | `a22078f59bfe9023787d5147c51c989a4010644d` | 21314 | 31943 | 127 |

Combined versus pre-sprint: -52.7866% ns/op, -79.0761% B/op, -54.8043% allocs/op. Combined versus ordinary: -55.1057% ns/op, -79.3702% B/op, -55.5944% allocs/op. Every combined sample is lower than every control sample for all three metrics. `independent-ratios.json` independently parses raw rows and derives exact Decimal ratios and per-triad changes. Latency remains directional for this short sample set.

The combined tree is `e448be5924b4a8e66a4894090460fb559f241354`, identical to merged `5bca87ffe6e1bfba2d693264d2dc82936adc234f`. All three full source trees, rather than partial product overlays, were checked out with the maintained worktree helper (ordinary Git explicitly selected), required clean including ignored/untracked files, and built serially using Go1.26.3, GOTOOLCHAIN=local, GOWORK=off, CGO_ENABLED=1, GOMAXPROCS=1, `go test -c -trimpath -buildvcs=false`. Before/after all-Go/module input censuses match. Pre-sprint and ordinary rebuilds reproduce retained binary hashes exactly.

Unchanged benchmark: `TreeDB/db/conditional_kv_contract_bench_test.go`, `BenchmarkConditionalTxnBaselineBatchWrite`, blob `ae3a6d3cd0ca244781e7843168dc5b35aac5d898`, SHA256 `e428911df6d694614dd7f0c5e58cb28490102d68c4262c77756b929c65f070b6`. It uses the public default-engine Open options, a temporary DB, 100ms root publication delay, 10000 seed entries, and groups of eight writes. No native selector, engine override, arena change, pool, or benchmark modification was introduced. The inherited environment had no TREEDB/GOMAP/GODEBUG/GOGC/GOMEMLIMIT overrides.

Setup/seed and stopped-timer Checkpoints are excluded from foreground Batch.Write timing. Each timed batch includes its own Close. The final DB Close is outside the benchmark timer. Existing fixture guards reject foreground root-publication work or unpaid publisher debt after Checkpoint. These guards and checked Close calls all passed. Default-engine results do not establish native activation, ACK latency, final sprint acceptance, or the final M0/Dgraph contract. Historical 100x measurements are not mixed with this 1000x packet.

Execution order: pre/ordinary/combined; ordinary/combined/pre; combined/pre/ordinary. All nine raw stdout/stderr and exits are retained, with no selective rerun. Each fresh process used CPU0 via taskset, GOMAXPROCS=1, run=^$, exact benchmark selector, count=1, benchmem, timeout=60s, benchtime=1000x. Coordinator quiet acknowledgements preceded the one execution; boundary snapshots show no other Go benchmark/profiler process and loadavg 0.12/0.28/0.50 at both boundaries. This is a boundary check, not continuous interference monitoring. The wrapper finished in 1.25s.

`publication-files.json` is the exact source/evidence publication allowlist. The three ELF binaries are excluded from Git publication but identified by size/hash in the inventory. `exact-retained-inventory.json`, the three full worktree metadata snapshots, and tracked Git-object inventories enumerate roots and members for later cleanup qualification. They do not authorize deletion. Preserve all three worktrees and binaries until durable publication and root release; common Git and all other lanes/evidence/toolchains/caches remain protected.
