# Partition-local HNSW M18/M20 qualification

Date: 2026-08-15

Status: **measured gate failures; no production-default promotion.**

This report records the bounded successors to the M16/eFC128 failure in #4159.
M18/eFC256 failed the frozen calibration gate in #4160. M20/eFC256 passed
calibration but failed the protected product-throughput gate in #4161. Neither
variant is qualified to replace the current production default.

The experiment changes only the partition-local graph variant. Both rows use
the same 250,000-vector, 40-partition, 50,000 useful-overlap-membership asset;
the same graph-derived assignment, raw graph, router, truth, query splits,
probes, top-k, and candidate budget; and no candidate quantization.

## Frozen identities

| Input | SHA-256 |
| --- | --- |
| Dataset manifest | `14194cca83e94d776baf78897e423ba505d51b342cc189845e6b271945502025` |
| Calibration split | `077ec68492638dfe4f3cd589e125a769149130666533491e50143767f28ea46f` |
| Canonical truth | `5a518c1cb8182edc685ab692dc17a6974655572f426a4b97c10482fd1643f04e` |
| Raw graph artifact | `57ad36d923c5fdb701a082727fd24efdcf0c6ac0e24efeda28ca11f232a65f1d` |
| Derived assignment artifact | `b07ab6272598447ee517d41665305af776ba806bb94033046b687e283a786040` |
| Useful-overlap membership | `188f6f44fba75a086cd18749bd66be799489311fc111c44f66d86f5f988a56e3` |
| Membership report | `15ba48e82841c4154e97f88501721cd37b6afdb4243571561265575e92c0816e` |

The benchmark host was an x86-64 Linux 6.8.0-136 system with an Intel
i5-11400F (6 cores, 12 threads), 33,512,763,392 bytes of RAM, and
`/mnt/fast4tb` on ext4 with `noatime`.

## M18 calibration reject

M18 was built from clean source revision
`4235fe7156249234c213c0ffda00e6e77915bdc0` with executable SHA-256
`da4e83414cc1359bfb49d6dffe0da0f6e4041dc2b79d002bd528be17f7e1fa49`.
Materialization took 206.50 seconds wall time, used 1,526,828 KiB peak RSS,
produced 224,093,976 pack bytes, preserved the source-ordinal digest, and
passed a strict reopen.

The predeclared calibration grid used 806 queries, three serialized
order-balanced repetitions, probes `1,2,4`, EF `80,81,88,96`, router candidate
budget 256, and top-k 10. The decision coordinate was probes=2:

| EF | recall@10 | QPS | p95 |
| ---: | ---: | ---: | ---: |
| 80 | 0.9310173697 | 2,715.8 | 0.412764 ms |
| 81 | 0.9331265509 | 2,677.1 | 0.420187 ms |
| 88 | 0.9414392060 | 2,542.7 | 0.442321 ms |
| 96 | **0.9496277916** | 2,408.6 | 0.471217 ms |

EF96 recovered 7,654 of 8,060 truth slots, exactly three fewer than the
required recall of 0.9500. The protected final query union was not run for
M18.

## M20 calibration pass and product reject

M20 was built from clean source revision
`e638aec1d7baf7a83bb1503ed72ad89d8a2877d0` with executable SHA-256
`120258c74f2e1e34d737fee32b176123790b9b57aa3a909ec7a27af613746852`.
Materialization took 235.84 seconds wall time, used 1,498,492 KiB peak RSS,
produced 228,754,240 pack bytes (2.08% more than M18), and passed a strict
reopen.

At probes=2 the same frozen calibration selected EF88 as the smallest passing
point:

| EF | recall@10 | QPS | p95 |
| ---: | ---: | ---: | ---: |
| 80 | 0.9410669975 | 2,478.9 | 0.462140 ms |
| 81 | 0.9423076923 | 2,446.8 | 0.529576 ms |
| 88 | **0.9513647643** | 2,376.0 | 0.472964 ms |
| 96 | 0.9590570720 | 2,218.9 | 0.506245 ms |

Only after that selection, the protected 1,000-query product union ran three
serialized repetitions at probes=2/EF88:

| Repetition | recall@10 | c1 QPS | c1 p95 | c32 QPS | c32 p95 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.9505 | 1,918.7 | 0.611719 ms | 7,746.1 | 7.095691 ms |
| 2 | 0.9505 | 1,892.7 | 0.628354 ms | 7,708.5 | 6.590029 ms |
| 3 | 0.9505 | 1,762.7 | 0.721571 ms | 7,197.6 | 7.236444 ms |
| Median | **0.9505** | **1,892.7** | **0.628354 ms** | **7,708.5** | **7.095691 ms** |

All six cells were valid and completed 1,000 queries with zero errors or
timeouts. Recall and median latency passed, but median c1 missed 2,000 QPS,
median c32 missed 9,000 QPS, and repetition 3 missed the c1 0.700 ms p95 cap.

## Retained evidence

Raw databases and reports remain on the benchmark host and are not committed:

| Artifact | Retained path | SHA-256 |
| --- | --- | --- |
| M18 materialization | `/mnt/fast4tb/gomap-4160-m18-default/reports/m18-materialize-edc7f1430.json` | `065d0c1606eb1f57ee98e0aa2ddcf0016fc8b649b1e9e51422cdd031d833be92` |
| M18 calibration | `/mnt/fast4tb/gomap-4160-m18-default/reports/m18-frontier-4235fe715.json` | `0766825f044467553f8639135d6038e84294e349f36ad9a92c16d99cf2a14f3e` |
| M20 materialization | `/mnt/fast4tb/gomap-4160-m18-default/reports/m20-materialize-e638aec1d.json` | `44fe0f6d79535c65c8b025c420b816c5937a0713cf8b8fbf35773f1f35e83f18` |
| M20 calibration | `/mnt/fast4tb/gomap-4160-m18-default/reports/m20-frontier-e638aec1d.json` | `75e02287efdf0497516576c62b1e64c9dd573b2311567ef679e14ce52d455eb8` |
| M20 product r1 | `/mnt/fast4tb/gomap-4161-m20-default/verified-runs/treedb_single/250k/repeat-1/search-ef88.json` | `3bc0dc57e9406996d569b99e74dbee1201b12200632b2f26d408ed83754855b1` |
| M20 product r2 | `/mnt/fast4tb/gomap-4161-m20-default/verified-runs/treedb_single/250k/repeat-2/search-ef88.json` | `cb1ecb361e6c4cc195aa6f0c5e2faec3db35f210ed8860f9a918c3225f2c8ab3` |
| M20 product r3 | `/mnt/fast4tb/gomap-4161-m20-default/verified-runs/treedb_single/250k/repeat-3/search-ef88.json` | `b86fd6870e7d4816c76b8662d9f5924994d6644dc325cef9341c075eb07c8975` |
| Product runner | `/mnt/fast4tb/gomap-4161-m20-default/runners/run_m20_final.py` | `9d911552e1788de39fb20041df4271a2514974af7450e00c2f4d398678292d71` |

## Disposition

Increasing local graph degree repairs enough neighbor connectivity to cross the
recall gate, but this M20 construction does not meet the coupled throughput
objective. Because the protected final union has now been used, it must not be
used to select or tune another topology. A successor requires a newly governed
validation split or a construction-level connectivity change selected without
feedback from this final union.
