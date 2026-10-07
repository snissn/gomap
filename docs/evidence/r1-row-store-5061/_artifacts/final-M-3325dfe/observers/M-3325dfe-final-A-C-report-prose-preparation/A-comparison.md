For actual main `3325dfe77940fec8587d8b61b1ac4e0b2f72caca`, the frozen six-engine A profile uses 4096 live rows, 32-row batches, 1000 calls and five fresh-DB repetitions under the retained durable/flushed contract. It covers retained-document JSON, template-v1, BSON, typed rows and SQLite JSON/row adapters. The matched comparison has **20 stable groups**, all with M/baseline throughput ratio at least **0.95994**, **72 inconclusive groups**, and **one newly enabled group**. The unchanged 15% spread and throughput-loss guards retain every sample. Root accepts this finite profile in [the original A cost decision](../../../../docs/evidence/r1-row-store-5061/_artifacts/final-M-3325dfe/results/A/root-cost-acceptance-original.json).

Selected stable read paths are below. Timings are median microseconds per public call; each ratio compares the same route between baseline and M. These values do not establish universal SQLite dominance.

| Complete-row path | Baseline µs/call | M µs/call | M/baseline throughput |
| --- | ---: | ---: | ---: |
| Typed GetInto | 349.538 | 59.897 | 5.83567× |
| Typed prepared batch (32 rows) | 135.203 | 138.816 | 0.97398× |
| Typed prepared range (10 rows) | 48.152 | 48.619 | 0.99039× |
| Template GetInto | 5.677 | 4.725 | 1.20130× |
| Template complete batch (32 rows) | 70.006 | 68.102 | 1.02795× |
| BSON complete point | 4.417 | 4.399 | 1.00402× |
| BSON complete batch (32 rows) | 117.364 | 118.883 | 0.98722× |
| SQLite JSON complete range (10 rows) | 17.556 | 18.289 | 0.95994× |
| SQLite row complete batch (32 rows) | 212.163 | 219.618 | 0.96605× |
| SQLite row complete range (10 rows) | 76.118 | 77.779 | 0.97865× |

Typed GetInto improves **5.83567×**, with Go allocation bytes **89,175.104→91,750.352 B/call (+2.9%)** and **151.500→150.723 allocations/call**. Prepared batch and prepared complete range remain within the guard, with slightly lower allocation totals. The newly enabled ordinary typed public range costs **118.762 µs per 10-row call**, **150,486.968 B/call** and **891.178 allocations/call**. Its baseline was unsupported, so it has no enabled-baseline speedup ratio. All other inconclusive labels remain intact. See [the exact comparison](../../../../docs/evidence/r1-row-store-5061/_artifacts/final-M-3325dfe/results/A/comparison-v2.json) and [the complete A descriptive report](../../../../docs/evidence/r1-row-store-5061/_artifacts/final-M-3325dfe/results/A/descriptive-report.md).

The comparison uses [a separately approved exact-pair source-provenance extension](../../../../docs/evidence/r1-row-store-5061/_artifacts/final-M-3325dfe/results/A/root-approved-A-semantics-and-provenance-certificate-v2.json): only the own-module VCS-derived version and two exact Git/clean metadata lines are normalized. Full original buildinfo bytes, expected revision/time/version and every dependency, compiler/CGO/build setting and other environment identity remain pinned; all remaining identities match. The original failed comparison is retained. No remeasurement or generated-code/performance equivalence claim follows from this metadata exception.
