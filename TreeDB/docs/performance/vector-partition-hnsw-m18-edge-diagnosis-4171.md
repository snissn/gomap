# Frozen M18 edge-quality diagnosis (#4171)

Status: **accepted frozen baseline diagnosis recorded 2026-08-16.** Pack
selection was committed before reading any new M18 query outcome. The accepted
run is diagnostic only: it does not change production construction or search.

## Frozen source lock

The diagnosis uses the useful-overlap M18/eFC256 retained asset on the Linux
host:

```
/mnt/fast4tb/gomap-4160-m18-default/materialized-m18-edc7f1430/m0-membership-3440095458
```

| Bound input | SHA-256 |
| --- | --- |
| fixture manifest | `14194cca83e94d776baf78897e423ba505d51b342cc189845e6b271945502025` |
| retained M18 descriptor | `d1bca744bbdb20c59f18397bd20f043955c2c71bce16bc34eaf1a9ad006923f2` |
| shard-generation record | `5aa286730362968994cde3c52daff6dcbe562e4377e08cc48ed1721abc4955e5` |
| calibration split | `077ec68492638dfe4f3cd589e125a769149130666533491e50143767f28ea46f` |
| canonical truth artifact | `5a518c1cb8182edc685ab692dc17a6974655572f426a4b97c10482fd1643f04e` |
| raw graph artifact | `57ad36d923c5fdb701a082727fd24efdcf0c6ac0e24efeda28ca11f232a65f1d` |
| graph-derived assignment artifact | `b07ab6272598447ee517d41665305af776ba806bb94033046b687e283a786040` |
| useful-overlap membership | `188f6f44fba75a086cd18749bd66be799489311fc111c44f66d86f5f988a56e3` |

The retained source has 40 packs of 7,500 rows, 250,000 source rows, 50,000
useful overlap memberships, M=18, eFC=256, and router candidate budget 256.
The prior M18 frontier is retained at
`/mnt/fast4tb/gomap-4160-m18-default/reports/m18-frontier-4235fe715.json`
(SHA-256 `0766825f044467553f8639135d6038e84294e349f36ad9a92c16d99cf2a14f3e`),
but its query outcomes were not used to select packs below.

## Predeclared representative packs

The selection is derived solely from the frozen shard-generation record. For
each pack, memberships were ordered by source ordinal; the fixture's
deterministic 32-topic label was calculated from that ordinal. `Entropy` is
the Shannon entropy of those labels and `transition rate` is the fraction of
adjacent labels that differ. Replica count is the number of non-home rows.
These are construction inputs/topology strata, not treatment measurements.

| pack | role | replicas | topic entropy (bits) | source-order transition rate |
| ---: | --- | ---: | ---: | ---: |
| 0 | homogeneous control | 939 | 0.000000 | 0.000000 |
| 3 | lightly mixed bridge | 939 | 0.664371 | 0.077210 |
| 16 | maximum source-order mixture | 2,488 | 1.437391 | 0.559541 |
| 36 | maximum replica-load mixed pack | 2,539 | 1.445282 | 0.262035 |
| 1 | independently high-mix fragmented pack | 2,492 | 1.444577 | 0.481264 |

This covers homogeneous and mixed strata, both extreme replica pressure and
the maximum source-order alternation. Packs 16 and 1 also provide the
historically plausibly difficult bridge/mixed category without using recall,
miss, or utility results to choose them.

## Accepted execution

The offline-only `local-hnsw-m18-edge-diagnosis` runner strictly reopened and
materialized a traced M18/eFC256 clone, then ran all 806 frozen calibration
ordinals at probes=2 and EF `80,81,88,96`. It did not access holdout outcomes
or change graph construction, routing, probes, top-k, query EF defaults, or
quantization.

Preparation and query execution have deliberately split provenance. The
immutable preparation checkpoint remains bound to the source head that created
it; the query report is independently bound to the clean executable head that
reopened it.

| Artifact | Identity |
| --- | --- |
| frozen base | `2a7d01443d3c842990c259b08bd442a4d0109511` |
| preparation head | `359f22a01cf13febba340a83b0e8e8e56d37373e` |
| query/validator head | `359f22a01cf13febba340a83b0e8e8e56d37373e` |
| checkpoint | `/mnt/fast4tb/gomap-4171-m18-diagnosis/reports/m18-edge-checkpoint-359f22a0.json` |
| checkpoint SHA-256 | `948ec9eb659706f3a8aae595cb073db06bc011674b29c49c74c9fce0d843a1a8` |
| compact report | `/mnt/fast4tb/gomap-4171-m18-diagnosis/reports/m18-edge-diagnosis-359f22a0.json` |
| compact report SHA-256 | `63558e7921fa09d5624456e4b564d3f95f9b384329001ff57806af6d4721c3ee` |
| bounded raw sidecar | `/mnt/fast4tb/gomap-4171-m18-diagnosis/reports/m18-edge-traces-359f22a0.json` |
| sidecar SHA-256 | `7726624a7c30641c230141dd6647ed3a564e8268a224b16650c3a5b699a8d94b` |

Full 40-pack preparation completed in 7:41.74 with 3,991,824 kB maximum RSS
and zero swap. Checkpoint reuse plus all four query cells and strict
sidecar/report reread completed in 2:05.50 with 3,918,228 kB maximum RSS,
zero swap, and 81 major faults. These Linux measurements qualify the
harness and persistence path; they are not cross-host latency claims.

The accepted query invocation was:

```sh
/mnt/fast4tb/gomap-4171-m18-diagnosis/source-359f22a0/bin/treedb-vector-partition-bench \
  local-hnsw-m18-edge-diagnosis \
  --dataset /mnt/fast4tb/gomap-4160-m18-default/source/testdata/vector_partition_qualification_embedding_mixture_250k \
  --retained-db /mnt/fast4tb/gomap-4160-m18-default/materialized-m18-edc7f1430/m0-membership-3440095458 \
  --calibration-split /mnt/fast4tb/gomap-hnsw-edge-quality-20260816/inputs/250k-query-calibration-manifest.json \
  --truth-artifact /mnt/fast4tb/gomap-hnsw-edge-quality-20260816/inputs/truth-cache/m8_canonical_truth_f1fab20b88cd3dcdd6e95a284400983230b1432b36bd4d73e321e251159795ab.json \
  --checkpoint /mnt/fast4tb/gomap-4171-m18-diagnosis/reports/m18-edge-checkpoint-359f22a0.json \
  --out /mnt/fast4tb/gomap-4171-m18-diagnosis/reports/m18-edge-diagnosis-359f22a0.json \
  --raw-sidecar /mnt/fast4tb/gomap-4171-m18-diagnosis/reports/m18-edge-traces-359f22a0.json \
  --base-sha 2a7d01443d3c842990c259b08bd442a4d0109511 \
  --head-sha 359f22a01cf13febba340a83b0e8e8e56d37373e \
  --preparation-head-sha 359f22a01cf13febba340a83b0e8e8e56d37373e \
  --source-checkout /mnt/fast4tb/gomap-4171-m18-diagnosis/source-359f22a0
```

## Baseline frontier

Work is the exact aggregate over the 806 calibration queries. Recall rises
monotonically, but each increment requires more comparisons; this is the
baseline against which fixed-budget construction variants must be compared.

| EF | recall@10 | candidates | edges examined | frontier admissions |
| ---: | ---: | ---: | ---: | ---: |
| 80 | 0.931017 | 2,747,541 | 4,809,980 | 441,831 |
| 81 | 0.933127 | 2,769,010 | 4,867,644 | 446,412 |
| 88 | 0.941439 | 2,912,578 | 5,268,459 | 477,535 |
| 96 | 0.949628 | 3,068,927 | 5,729,760 | 512,751 |

## Candidate coverage and final survival

The bounded exact oracle uses 32 deterministically selected construction nodes
per pack and exact predecessor-restricted top-10 neighbors. Candidate coverage
is essentially complete in every predeclared stratum, while final adjacency
loses a material fraction of those already-available neighbors.

| pack | candidate exact-10 coverage | final exact-10 overlap | diversity truth / 100 final edges | backfill truth / 100 final edges | reciprocal truth / 100 final edges |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 100.00% | 91.88% | 31.16 | 20.56 | 21.35 |
| 1 | 100.00% | 91.88% | 34.05 | 17.46 | 23.34 |
| 3 | 99.69% | 87.81% | 29.27 | 17.37 | 22.13 |
| 16 | 100.00% | 95.63% | 35.32 | 18.69 | 23.29 |
| 36 | 100.00% | 93.75% | 37.11 | 14.24 | 25.12 |

Across the five packs, construction candidate pools contain 1,599/1,600
exact neighbors (99.94%), while final adjacency retains 1,475/1,600 (92.19%).
Thus 124 sampled exact neighbors were available and subsequently displaced;
only one was absent from the candidate pool. All five final layer-0 graphs are
single strongly connected components with all 7,500 rows reachable, so this is
not explained by gross disconnection.

Backfill also shows the weakest aggregate query utility. At EF80 it accounts
for 8.72% of native edge examinations but only 5.04% of recovered truth:
903 recovered truth neighbors per million examined edges, versus 1,714 for
diversity-selected and 1,561 for reciprocal edges. The same ordering persists
through EF96 (739 versus 1,457 and 1,354). Pack 0 is a useful homogeneous
exception where local backfill is efficient, which argues for testing policy
variants rather than deleting backfill unconditionally.

The corrected exact-head validator also reconciles the compact final-origin
maps for all 40 packs rather than treating compact packs as zero-edge graphs.
Across the independently bounded 32-node sample in every pack, candidate pools
contain 12,777/12,793 available exact neighbors (99.87%) and final adjacency
contains 11,899/12,800 exact-neighbor slots (92.96%). This all-pack check
supports the same selection/survival diagnosis; the predeclared five-pack
counts and all query cells above are unchanged.

## Causal decision

Observed evidence places the leading loss at **selection/survival**, not
candidate coverage:

1. Exact useful candidates almost always enter the construction pool.
2. A material fraction disappears before final adjacency.
3. Backfill consumes edges and traversal work less efficiently overall, while
   retaining stratum-dependent value.
4. Connectivity and reachability are already complete.

Therefore #4172 should run the predeclared equal-capacity M18/eFC256 layer-0
matrix: initial selection `{M,2M}` by backfill `{off,on}`, with the current
`2M/backfill-on` graph as the exact control. The reciprocal/final degree cap
stays 2M, so this tests allocation and survival rather than simply adding
edges. If the best backfill-off arm underfills, apply exact-budget postfill only
to that finalist. Candidate-pool expansion remains conditional and is not the
next experiment. Generic insertion shuffling remains rejected by prior #4107
evidence. Full Vamana remains deferred until a same-budget robust-prune-style
refinement succeeds but HNSW construction constraints still visibly cap it.
