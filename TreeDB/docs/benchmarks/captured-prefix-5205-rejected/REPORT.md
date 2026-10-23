# Captured-prefix component comparison

Decision: NOT ACCEPTED. All 120 benchmark rows completed and verified final values after clean reopen. All concurrent shapes are slower; the largest regression is COW Pointer4096/G16. This does not qualify or disqualify the independent external no-WAL recovery API or Dgraph consumer.

Exact source, harness blob, sample order and boundary are predeclared in PLAN.md. Five baseline/candidate pairs, fixed128x, GOMAXPROCS4; no profiles in decision rows. Numbers below use each arm's median ns/op, reciprocal throughput and median counters. Every raw pair, stderr, identity, command and host observation remains retained.

| Mode/layout/workers | Baseline ops/s | Candidate ops/s | Median time change | WAL sync/op base -> cand | B/op base -> cand | alloc/op base -> cand |
|---|---:|---:|---:|---:|---:|---:|
| btree/Inline64/G=1-4 | 200.6 | 198.5 | +1.07% | 1.0000 -> 1.0000 | 8596 -> 8613 | 6 -> 6 |
| btree/Inline64/G=16-4 | 2,097.9 | 1,498.0 | +40.05% | 0.0781 -> 0.1328 | 16676 -> 16688 | 6 -> 6 |
| btree/Inline64/G=4-4 | 646.0 | 493.3 | +30.94% | 0.2578 -> 0.4062 | 16654 -> 16665 | 6 -> 6 |
| btree/Pointer4096/G=1-4 | 189.6 | 188.5 | +0.59% | 1.0000 -> 1.0000 | 239742 -> 232695 | 41 -> 41 |
| btree/Pointer4096/G=16-4 | 1,388.9 | 1,009.1 | +37.64% | 0.0859 -> 0.1484 | 199244 -> 189963 | 28 -> 28 |
| btree/Pointer4096/G=4-4 | 539.7 | 386.1 | +39.81% | 0.2734 -> 0.4609 | 232486 -> 233351 | 44 -> 44 |
| cow_ordered/Inline64/G=1-4 | 201.0 | 200.5 | +0.23% | 1.0000 -> 1.0000 | 4164 -> 4180 | 53 -> 53 |
| cow_ordered/Inline64/G=16-4 | 2,009.9 | 1,615.6 | +24.41% | 0.0781 -> 0.1250 | 4477 -> 4491 | 53 -> 53 |
| cow_ordered/Inline64/G=4-4 | 555.9 | 423.3 | +31.31% | 0.2656 -> 0.4766 | 4315 -> 4324 | 53 -> 53 |
| cow_ordered/Pointer4096/G=1-4 | 97.2 | 96.8 | +0.42% | 1.0000 -> 1.0000 | 207817 -> 204666 | 193 -> 235 |
| cow_ordered/Pointer4096/G=16-4 | 301.3 | 164.6 | +83.06% | 0.2969 -> 0.7188 | 184104 -> 190077 | 176 -> 227 |
| cow_ordered/Pointer4096/G=4-4 | 168.4 | 166.8 | +0.96% | 0.5469 -> 0.7500 | 226729 -> 230654 | 194 -> 245 |

Quiet preflight: 2.009% host CPU over10s, limit10%; both workers held all Go tests/builds/server jobs. Local workstation/persistent caches/task artifacts, no dedicated benchmark runner. Host VM/I/O observations retained; this is finite component evidence, not sustained memory/disk equilibrium.

The sync-count increase accompanies the throughput regression, so the changed collector/capture timing is a concrete next attribution target. V2 per-mutation dependency stabilization is a separate pointer-path source candidate. These are hypotheses pending source/independent review, not yet claimed causal proof. No selective retry, tolerance change or accepted regression. Singleton times are within about1.1%, but small inline-byte growth and pointer-byte variability remain visible.

Prior local owning-suite failures and current PRIMARY gate failures remain separate retained failures; completed current-head runtime CI does not waive them. Existing #5205 explicitly permits an evidence-backed rejected/no-change candidate disposition; coordinator and independent source review accepted that rejected-current-candidate/no-change outcome. The removed maintained default collector wait is an identified grouping-policy confound, not proof that immutable prefix capture is inherently inferior.
