# Vector-partition V1 qualification: bounded 100k attribution

Date: 2026-07-29

Status: **experimental gate failures; not a V1 qualification pass.**

The live routing/locality follow-up is [#4022](https://github.com/snissn/gomap/issues/4022),
which blocks #4019. Parent #4012 acceptance is
[recorded](https://github.com/snissn/gomap/issues/4012#issuecomment-5123955990);
this report still does not claim a V1 enablement decision.

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

It was generated reproducibly as:

```sh
go run ./cmd/treedb_vector_partition_bench generate-fixture \
  -out /mnt/fast4tb/gomap-4015-fixtures/embedding_mixture_100k \
  -vectors 100000 -queries 1000 -dimensions 128 -seed 4017 \
  -generator treedb_vector_partition_embedding_mixture_v1
```

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

## Reproducibility and calibration context

The bounded M8 runs used an 11th Gen Intel Core i5-11400F (12 logical CPUs),
31.21 GiB RAM, Linux 6.8.0-124-generic amd64, and `/mnt/fast4tb` on
`/dev/nvme0n1p1` ext4 with `rw,noatime`. The topology is four three-node data
Raft groups over serialized loopback TCP M5 messages. Timed cells include
router/coordinator, M5 serialization, Raft read-index/apply, persistent HNSW,
merge, and caller scheduling; they exclude topology construction, canonical
truth computation, preflight, warmup, artifact encoding, attribution, and
shutdown.

The 100k calibration fixture uses the distinct-domain
`treedb_vector_partition_embedding_mixture_v1` generator (seed 4017) so held
out queries are not copied corpus rows. Its role is to expose a clustered,
embedding-shaped routing case; it is not a licensed external corpus and cannot
replace the committed 1M high-entropy identity
`treedb_vector_partition_high_entropy_synthetic_v1` (seed 4015, checksum
`08a920e81d8ce5a0b19d1a4d051d5f408a688192eee971cbfc09d1b8c362a3c3`).

An earlier 10k high-entropy calibration found `.3004` recall@10 at four probes
and `.9999` at 16 probes. It is a uniform/unclustered negative calibration,
not a qualification row and not pooled with the 100k result. The direct
profiled 10k capture command was:

```sh
treedb_vector_partition_bench -mode production_multi_group \
  -dataset /mnt/fast4tb/gomap-4015-fixtures/high_entropy_10k \
  -out /mnt/fast4tb/gomap-4015-calibration/out \
  -profiles /mnt/fast4tb/gomap-4015-calibration/profiles \
  -partitions 16 -raft-groups 4 -probes 1,2,4,8,16 -overlap 0 \
  -ef-search 64 -concurrency 1 -top-k 10 -seed 4015 \
  -m8-max-exact-truth-visits 20000000 -format text
```

Profile coverage is limited to that 10k calibration, not the 100k retained
rows. CPU profile (99.82 s wall, 22.62 s samples) is led by Linux syscall6
(23.12% flat), AVX512 dot product (3.93%), futex (3.49%), and prepared HNSW
cosine search (12.02% cumulative). The cumulative allocation snapshot totals
5.67 GiB; leading flat contributors are coordinator-response validation
(1.05 GiB), bbolt inode reads (367 MiB), candidate scratch growth (322 MiB),
and JSON marshal (252 MiB). The binary profiles and trace are intentionally
not committed: CPU `915f08ad41eb8f63ab5383bc6ccc7ca7830212696e6078301cf2a323d8b50ab0`,
allocs `95bb1016270299b63572bacd07d09c061d9671a557856f28ed2677b2ed7decb8`,
alloc baseline `1f4105f34057cd8172be60da6d91f98e8a881fecbf801eb1c57ef3a0590a4d51`,
heap `55161019ac9cf8e680b16b877d8e2c032750a090012b0ba7cbb348547d90d58c`,
block `d3af88821c7810d410359482be38ee4fdd0397bf90758c9139673216cd701df2`,
and mutex `d627a756f3908c3c674b7fd3cd558a722fdd224ff951e1034114959455068413`.

Every 100k raw JSON artifact is checked in under
`TreeDB/docs/spec/artifacts/vector-partition-v1-qualification-4015/`; each
contains its exact command. The command variants are graph/disjoint and
graph/overlap `.20` full sweeps; stable-hash probes `4,16`; graph-overlap EF
`128,256` at probes `4,16`; graph-overlap concurrency `16,64` at probes
`4,16`; and two graph-overlap EF-64 single-client repeats at probes `4,16`.
The ledger maps every published repo-relative path to its SHA-256, while `/mnt`
is retained only as provenance.

The published measurement artifacts predate cache hardening. Their historical
cache identity is preserved only as provenance; the current implementation
binds new cache files to the authoritative score-contract identity and refuses
those historical files rather than silently reusing them.
