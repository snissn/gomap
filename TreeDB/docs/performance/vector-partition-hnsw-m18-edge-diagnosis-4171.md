# Frozen M18 edge-quality diagnosis (#4171)

Status: **pre-outcome pack selection recorded 2026-08-16; treatment not yet
run.** This record is intentionally committed before reading any new M18 query
outcome.

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

## Planned campaign

The new offline-only `local-hnsw-m18-edge-diagnosis` runner will compile from
the clean exact source head, strictly reopen/materialize a traced M18/eFC256
clone, and run exactly the 806 frozen calibration ordinals at probes=2 and EF
`80,81,88,96`. It will retain construction provenance and the bounded exact
candidate/final-neighbor oracle for all packs, aggregate per-origin utility
over every cell, and retain raw detailed traces only for deterministic bounded
hard misses. The compact report will bind source, fixture, descriptor,
membership, manifest, router, calibration, truth, and executable identities.

It will not access holdout outcomes or change graph construction, routing,
probes, top-k, query EF defaults, or quantization. Linux data establish
persistence/correctness; any M3 timing is explicitly within-host only and is
not compared absolutely with Linux timings.
