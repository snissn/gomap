# PRIMARY captured-root empty proof: causal performance check

The only changed production source between these two compiled projections is `TreeDB/tree/owned_iterator.go`. Both use identical endpoint-only lane counter reporting added to the previously retained common harness; the timed workload and fixture are unchanged. Both run on the same quiet111 host with exact Go1.26.3, CPU0-3/GOMAXPROCS4, two balanced forward/reverse samples, fixed64x. All16 commands, logs, binary/source hashes, actual ACK/layout/output/capture/publication counters and lane endpoints are retained.

| RELAXED cell | Before ns/unit | After ns/unit | Paired time change | Before B/op | After B/op | Before allocs/op | After allocs/op |
|---|---:|---:|---:|---:|---:|---:|---:|
| inline/point | 17199.5 | 10060.5 | -41.33% | 68040 | 9296 | 101 | 78 |
| inline/group_versions | 23965 | 16284.5 | -32.03% | 75232.5 | 17649 | 136 | 133 |
| inline/concurrent | 18924 | 11799 | -36.49% | 134734 | 18404.5 | 167 | 141 |
| forced_pointer/point | 266948 | 221230 | -17.07% | 147787 | 89040 | 205 | 182 |

Focused nine tree cases and four existing COW tests passed. The new primary-empty case failed against baseline before correction; error propagation, zero allocation proof, nonempty/internal bases, inline absence, missing/digest-invalid base and captured-root stability are covered. Independent Sol61 review found no blocker; this is focused coverage, not exhaustive native/correctness qualification.

The ~58KB per-read regression is removed by reusing the existing empty-backend SuccessorTable path for a canonical zero-entry PRIMARY with a digest-validated empty leaf base. No root recapture, second authority, scratch pool, durability relaxation or pointer materialization deferral.

Forced-pointer samples contain exactly two new lane segments/rotations on lanes1 and2 among three effective lanes, and two directory syncs. Directory time8.74–8.89ms/64 accounts for about136–139us/unit (roughly49–63% of each measured unit). This proves a material initialization contribution; it does not explain all generic COW writer overhead or establish steady-state speedup. File-sync/WAL-sync/public-checkpoint deltas remain0. Next bounded causal seam: reuse maintained key-affine lane selection for eligible COW single-point publication, preserving visibility Flush and canonical publication/ACK lifecycle.

Fixed64x freshDB constant-history two balanced samples only, not fullDgraph/Badger M0/M9, sustained/native/steady-state qualification. Performance viability remains incomplete. Ordinary raw5 regressions unaffected/unwaived. Directory sync explains part of measured cost, not all pointer overhead.

Invocation wall total4.428s. Original >10% relative TreeDB/Badger improvement in BOTH modes remains unmeasured; graph remains open.
