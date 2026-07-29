# Vector-partition V1 qualification: bounded 100k attribution

Date: 2026-07-29

Status: **experimental gate failures; not a V1 qualification pass.**

This report records a reproducible, production-shaped attribution run completed
while qualifying the #4015 contract.  It is deliberately narrower than the
required 1M qualification: the corpus has 100,000 vectors, not 1,000,000.
It must not be used to claim the required 1M row, an external-corpus result, or
enablement.

## Corpus and retained inputs

The deterministic embedding-mixture corpus has 100,000 128-dimensional cosine
vectors, 1,000 held-out queries, seed 4017, and fixture checksum
`ecc2224f386932e580e4956f2cfa852140d3134625971c3511bc0d5feddf9b95`.
It was generated outside the checkout at
`/mnt/fast4tb/gomap-4015-fixtures/embedding_mixture_100k`; it is therefore a
local calibration corpus rather than a committed qualification fixture.

Both rows use 16 partitions, four three-node data Raft groups, serialized
loopback M5 transport, `top_k=10`, `ef_search=64`, concurrency 1, and exact
FP32 canonical truth. The canonical-truth cache identity is
`3a8605017166fca50d4a85d28ad44e17e9abac9828a6795cdae25f238042624a`.
The disjoint run computed that truth in 46.53 s; the overlap run reused the
identity-bound cache in 6.76 ms.

The measured code head was `a2b2ad40b796`; the subsequent commit
`55a56473023e` adds only capacity-override tests. Retained artifacts are:

- Graph/disjoint:
  `/mnt/fast4tb/gomap-4015-embedding100k/m8-default/vector_partition_m8_a2b2ad40b796_ae37e880058f.json`
  (SHA-256 `123c9135f3374827c16e52946dde138860bcfbf49b3985b7a235e75c35c7198e`).
- Graph/overlap .20:
  `/mnt/fast4tb/gomap-4015-embedding100k/m8-overlap/vector_partition_m8_a2b2ad40b796_a5ddcbbac55f.json`
  (SHA-256 `4a6c794f78df3512de1b925769bd2c2f8eddb3ff05d80017e0bee3db5fa77cbd`).

The overlap asset realizes all 20,000 requested extra memberships (none were
rejected), with exactly 7,500 rows in each partition. Its persistent asset is
87,220,840 bytes versus 72,674,288 bytes disjoint, a 1.2002x ratio and below
the 1.35x storage target.

## Results

| Probes | Disjoint recall@10 | Overlap recall@10 | Disjoint QPS | Overlap QPS | Disjoint p95 | Overlap p95 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | .2323 | .3063 | 58.18 | 73.25 | 27.16 ms | 20.18 ms |
| 2 | .4072 | .5049 | 57.10 | 65.90 | 29.13 ms | 22.14 ms |
| 4 | .6211 | .7201 | 45.07 | 51.06 | 39.06 ms | 29.59 ms |
| 8 | .8316 | .8814 | 42.83 | 47.52 | 39.43 ms | 31.91 ms |
| 16 | .9971 | .9924 | 41.06 | 46.67 | 43.70 ms | 33.70 ms |

Exact partition-union ID and score parity pass in both rows. At four probes,
the overlap row's exact representative-routing recall is .7291 and its
partition-local-HNSW recall is .7201, so the observed loss is principally
routing/partition locality, with a smaller local-HNSW contribution. The full
16-probe overlap row's .9924 recall confirms a small remaining local-HNSW
loss; it does not alter the lower-probe attribution.

## Gate interpretation

The overlap row improves every measured low-probe recall, QPS, and p95 value,
and it passes exactness, failure honesty, partition reachability, balance,
overlap storage, and the configured resource limits (475 MB peak RSS; 87 MB
asset). It still fails the V1 target at the quarter-partition budget: four of
16 probes reach only .7201 recall@10, below .90. Consequently the M8 ledger
also correctly reports probe reduction, matched-recall QPS, and tail gates as
failed: no <=4-probe row reaches the target recall to make a matched-recall
comparison available.

This is useful evidence that full, capacity-respecting overlap is beneficial
on the embedding-shaped calibration corpus. It is not evidence that the graph
routing design qualifies at 1M or on the required high-entropy corpus. The
remaining 1M M2/M3/M8 rows require explicit scalar-work and M3-visit overrides
and a calibrated resource/time decision before they may be attempted.

## Minimum decisive attribution additions

The stable-ID-hash disjoint control measured recall@10 `.2578` at four probes
and `1.0000` at 16 probes (QPS 42.98/41.32; p95 44.32/45.20 ms). Graph overlap
improves the four-probe route materially, but does not meet `.90`.

| Probes | EF 64 | EF 128 | EF 256 | Exact routing recall |
| ---: | ---: | ---: | ---: | ---: |
| 4 | .7201 | .7279 | .7289 | .7291 |
| 16 | .9924 | .9990 | .9998 | 1.0000 |

Higher EF removes nearly all local-HNSW loss but cannot recover the missing
primary-partition coverage at four probes. The measured blocker is graph/router
locality, not EF-only tuning.

At EF 64, concurrency 16/64 preserves recall but changes the performance
boundary: four-probe QPS is 126.40/93.50 with p95 218.70/978.51 ms; exhaustive
QPS is 91.25/163.95 with p95 299.69/524.46 ms. These are separate load cells,
not substitutes for single-client matched-recall evidence.

Three independent single-client graph-overlap EF-64 runs give invariant recall
and the following median [min,max] variance:

| Probes | recall@10 | QPS median [min, max] | p95 median [min, max] |
| ---: | ---: | ---: | ---: |
| 4 | .7201 | 50.97 [38.96, 51.06] | 30.98 [29.59, 46.26] ms |
| 16 | .9924 | 45.44 [38.30, 46.67] | 34.49 [33.70, 47.06] ms |

Additional retained artifacts: `m8-stable-id-hash`, `m8-overlap-ef128-256`,
`m8-overlap-concurrency16-64`, `m8-overlap-repeat-2`, and
`m8-overlap-repeat-3` under `/mnt/fast4tb/gomap-4015-embedding100k`.
Their paths, hashes, gates, and exact commands are indexed in the JSON ledger.
