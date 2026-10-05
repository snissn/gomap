# Independent E final 43-cell numeric review

**Disposition: ACCEPT_FINAL_EVIDENCE.** No consequential numeric, coverage, provenance, or wording finding remains in the examined packet. This accepts the completed evidence and its presentation; optimization acceptance, current E CI/Codex, and publication/merge remain coordinator gates.

## Examined immutable identities

- Complete `final-assembly-complete/RESULTS.json`: `97a04839a57e523992087c772de3ed6fe0dcc1f8a2d138799eaba18c7ba7116c`.
- Complete `REPORT.md`: `8bf41f5f51992c75a745dde4a5a34e5fb6752cd7aa89f7b1fecbbd7b43fbb29c`.
- Reviewed `FINAL_FINDINGS.md`: `f9b1661eae69b965dd89896204168b60f7f9e67b645368027581cdff403ddeb2`; README: `c2f9d0ed1a2179092fd1d4fccf81625f17b87c8576d3cd28a22b81322ca0425b`.
- Reused first25 packet: `2ed48cd4b36eab8a8b83050d972e1b286a0028c95f24ca2b7800677f80434742`. Interim 41-record packet: `3ce83120f32b085fad4a592af06a051492cd7cbecfb3f481e069deb55f910c64`.

All 492 local raw-map hashes match actual bytes, with no missing files. Both earlier record projections and hash bindings are exact unchanged subsets of the completed packet. Documentation RESULTS/REPORT copies equal the immutable complete files byte for byte. The compact sorted full raw-hash-map digest is `c36d59f21470c263058ccc8734eafd29ba72dc9d24bddc06e644df48eeb7e001`.

## Coverage and source applicability

The canonical plans equal the packet plans. All 43 unique declared semantic cells are present, unprofiled PASS, with no missing/failed cells: 13 predeclared reused candidate captures and 30 fresh captures. Raw stdout projections equal packet records. Recorded run metadata has rc0, validated=true, and completed timestamps; recorded capture intervals do not overlap. This checks the retained capture schedule, without asserting absence of unrelated host activity.

The new 18 records received raw/config/oracle/argv/environment/receipt checks using the frozen collector validator and existing command checks. The last two LMDB/RocksDB ws1 cells actually use primary seed24, 1% working set, 90% misses, 3M keys/6M reads/40k updates, four readers, ordinary commit and an eight-second reader window. They are not seed173 holdout cells. All 22 matched pairs preserve actual semantic configuration and distributions across before/after.

Actual landed authority remains `17712b9cfcef2b90516419a34ccf3d464984d473`; measured producer remains `87eb344543709e7752c5f5c222f8a7d42f330dac`. The accepted final-landed binding preserves the exact TreeDB subtree, all 2131 compiled project inputs, and protected harness; the documented tooling/README differences do not relabel the measured source as the landed commit. Existing first25 machinery/native/source reviews apply unchanged. No remote compiler/library bytes were freshly rehashed in this review.

## Numeric and prose checks

The accepted primary/before-after/capacity/cost tables remain unchanged. Independently recomputed all 16 REPORT primary rows from raw before/after captures, including medians and descriptive min/max ranges for throughput, p99, B/op, and allocations/op. Independently recomputed every scaling table value. Concurrent Mops/s at 4/8/16/32/64 readers are:

| Engine/profile | 4 | 8 | 16 | 32 | 64 |
|---|---:|---:|---:|---:|---:|
| TreeDB durable | .629 | 1.062 | 1.272 | 1.984 | 1.347 |
| LMDB durable | 3.260 | 5.623 | 6.865 | 6.801 | 6.917 |
| RocksDB sync | .785 | 1.284 | 1.514 | 1.507 | 1.543 |
| TreeDB fast | .712 | 1.025 | 1.216 | 1.315 | 1.237 |

The scaling prose correctly describes single held-out observations, both TreeDB peaks at 32 readers and declines at 64, and avoids linear-scaling or production-optimality claims. These are reader-window throughputs, with writer completion and checkpoint costs separately disclosed.

The compression-off caveat matches raw measurements: HITS declines 6.85%, MISS declines 9.71%, mixed improves 0.64%, concurrent improves 37.75%; MISS B/op rises 9.39584 to 15.42635. The separate O2-only comparison decreases MISS B/op from 16.31669 to 15.42635. Both sides of that O2-only MISS window record one GC and one empty rewrite plan; counters do not establish attribution of all allocation or elapsed time. The paragraph retains the actual static losses without a noise waiver or causal overclaim.

README source/receipt/command/native/finalization requirements remain strict. Prior claim/profile/guard reviews remain applicable, including volatile versus durable ACK distinctions, conditional unsupported physical resources, sampled-profile attribution, storage/checkpoint costs, and absence of hardware-powerloss proof. The original censored baseline sync cell remains censored and is not a finite speedup denominator.

`COMPLETE_EVIDENCE` is accurate; `optimization_acceptance=PENDING_COORDINATOR_REVIEW` correctly keeps the separate tradeoff decision open. This review covers the exact hashes above and does not grant optimization or graph/merge acceptance.
