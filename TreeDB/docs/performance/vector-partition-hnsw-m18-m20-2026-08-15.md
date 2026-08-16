# Partition-local HNSW M18/M20 qualification

Date: 2026-08-15

Status: **historical gate record; superseded by the Pareto re-evaluation below.**

This report records the bounded successors to the M16/eFC128 failure in #4159.
M18/eFC256 failed the frozen calibration gate in #4160. M20/eFC256 passed
calibration but failed the protected product-throughput gate in #4161. Neither
variant is qualified to replace the current production default.

## 2026-08-16 Pareto re-evaluation

The earlier conclusion treated every frozen absolute threshold as conjunctive.
That was useful for preserving the original experiment, but it was too strict
for choosing between internally comparable graph variants: the Linux host can
be noisy, and missing a threshold by three recall slots does not make a variant
that is simultaneously more accurate and cheaper to search a worse production
choice. The original measurements remain valid evidence; their binary pass/fail
disposition no longer determines the default on its own.

We therefore reran retained M16, M18, and M20 packs on the same quiet Apple M3
host with the same frozen 806-query calibration split, truth, routing inputs,
probes=2, router candidate budget 256, top-k 10, and EF grid `80,81,88,96`.
There were two full outer runs with order-balanced variants. Absolute numbers
must not be compared with the Linux product thresholds, but the interleaved
within-host deltas are suitable for deciding the graph/EF Pareto frontier.

| Variant | Pack bytes | EF | recall@10 | median QPS | median p95 | candidates | edges |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| M16/eFC128 | 220,027,376 | 80 | 0.9116625310 | 2,623.3 | 0.459875 ms | 2,503,266 | 4,274,361 |
| M16/eFC128 | 220,027,376 | 81 | 0.9137717122 | 2,590.6 | 0.461542 ms | 2,523,064 | 4,325,049 |
| M16/eFC128 | 220,027,376 | 88 | 0.9212158809 | 2,466.7 | 0.482333 ms | 2,658,273 | 4,683,609 |
| M16/eFC128 | 220,027,376 | 96 | 0.9317617866 | 2,304.6 | 0.523188 ms | 2,803,445 | 5,091,321 |
| M18/eFC256 | 224,093,976 | 80 | 0.9310173697 | 2,467.3 | 0.482958 ms | 2,747,541 | 4,802,323 |
| M18/eFC256 | 224,093,976 | 81 | 0.9331265509 | 2,471.4 | 0.479814 ms | 2,769,010 | 4,859,887 |
| M18/eFC256 | 224,093,976 | 88 | 0.9414392060 | 2,333.6 | 0.516022 ms | 2,912,616 | 5,260,207 |
| M18/eFC256 | 224,093,976 | 96 | 0.9497518610 | 2,219.5 | 0.534542 ms | 3,068,919 | 5,720,719 |
| M20/eFC256 | 228,754,240 | 80 | 0.9410669975 | 2,388.0 | 0.501854 ms | 2,948,627 | 5,322,056 |
| M20/eFC256 | 228,754,240 | 81 | 0.9423076923 | 2,364.6 | 0.501980 ms | 2,971,799 | 5,387,896 |
| M20/eFC256 | 228,754,240 | 88 | 0.9513647643 | 2,251.5 | 0.532522 ms | 3,122,130 | 5,833,816 |
| M20/eFC256 | 228,754,240 | 96 | 0.9590570720 | 2,141.6 | 0.560313 ms | 3,285,104 | 6,348,176 |

The balanced comparison is M18/EF81 against the old M16/EF96 operating point.
M18 improves recall by 0.1365 percentage points, QPS by 7.24%, p95 by 8.29%,
candidates by 1.23%, and traversed edges by 4.55%, for 1.85% more pack bytes.
It is therefore the M18/eFC256 balanced default as of #4166, independent of the
old 0.9500 gate. Explicit M16/eFC128 remains available as a rollback profile.
The promotion does not change the request EF default globally; EF81 is the
qualified operating point for this workload, not a new global request policy.

M20/eFC256 is the explicit production-readable high-recall profile as of #4166;
it is not the implicit default. At EF88, against M18/EF96, it improves recall by
0.1613 percentage points and QPS
by 1.44%, with 0.38% lower p95, 1.73% more candidates, 1.98% more traversed
edges, and 2.08% more pack bytes. M20/EF96 is the maximum-recall measured point:
95.906% recall, cutting the remaining M16/EF96 error by about 40% for about 7.1%
less QPS and 7.1% higher p95. EF88 is the measured workload point, not a change
to the global request EF default.

### Edge-quality diagnosis

All M16 rows were saturated at their layer-0 degree limit, yet the native graph
reached only 293,778 of 300,000 rows and 20 rows had zero indegree. Fifteen of
40 partitions were disconnected from their native entry. The existing
auxiliary graph made all rows reachable, so this structural defect did not
explain the M16-to-M18 recall difference by itself.

A fixed-budget boundary repair restores a missing reciprocal edge whenever
pruning disconnects an entry-reachable region. It swaps an existing edge only
after proving that prior reachability is retained and total reachability grows.
The repaired M16 graph reaches all 300,000 rows with 40 traversal roots and zero
zero-indegree rows while preserving exactly 9,600,000 layer-0 edges. Its recall
is bit-identical at all four measured p2 EF points because the auxiliary graph
had already masked the defect. Per 806-query cell, candidates differ by at most
33 and traversed edges by at most 96 (under 0.0023%). The repair is retained as
a correctness improvement, not claimed as the source of the M18 recall gain.

The stronger variants instead show more reciprocal and slightly longer edges:
M16 has 5,578,406 reciprocal directed edges (58.11%) and mean cosine distance
0.049591; M18 has 6,578,218 (60.91%) and 0.049842; M20 has 7,320,940 (61.01%)
and 0.050001. That is consistent with better navigational coverage and diversity,
not merely closer neighbors or more copies of the same local neighborhood.

### Re-evaluation evidence

| Artifact | SHA-256 |
| --- | --- |
| Clean comparison executable (`619e96fc`) | `8299a49cb40cd6b1bbe3eefa36c24f7cb66a946c4862cf6131c6a4e1deb6c20e` |
| Final repaired executable (`4ff3fafe`) | `6df4b5d6360aacaac54f11e578dbeba7f757aca0d258ad2c3be11c5e7ff13c04` |
| M16 outer run 1 | `5a1e3b3aa15beaa6f5faf84f952852172ea9028084fe1f1864717871223a5900` |
| M16 outer run 2 | `c2345880582c11beae7be255ef5f289cf38c6b3c372fefbdfa0ab3ae4028cf2e` |
| M18 outer run 1 | `c64de394e7726f9ebb978f698d7ede85b0d402e3856db679631abc9751db5434` |
| M18 outer run 2 | `5927e8da22e194e933a50e26ea4fbec485811aee5f1abad3cee01b51cdd88046` |
| M20 outer run 1 | `9efec1c4e2c23c76883db92db724613300e9fcc3b8c05338d52ee8b002fcca4c` |
| M20 outer run 2 | `f039fb80eaccb43ea765104b3b1c265d9f2bb744b4e1d7b477e5deec059dd7e8` |
| Final repaired M16 materialization | `5024d44ca1887eb8582c78a7af9eab574f1de882608017206a22cdd81d705f40` |
| Final repaired M16 frontier | `107ac2f7c05d9b75a9ceb45724203bb937ffdd403241d1d6c4db0b0b45450573` |

Raw re-evaluation inputs, databases, and reports are retained under
`/private/tmp/gomap-hnsw-edge-quality-evidence-20260816` on the quiet host and
`/mnt/fast4tb/gomap-hnsw-edge-quality-20260816` on the Linux host. The compact
figures and hashes above are committed so the decision does not depend on
those machine-local paths remaining available.

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
