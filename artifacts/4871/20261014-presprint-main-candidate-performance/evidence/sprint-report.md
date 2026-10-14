# Bounded before-sprint and candidate comparison

105 invocations:3 setup/output/ACK/layout smoke invocations (36 rows),30 matched raw micro invocations,72 fixed64x public mechanism invocations. Same111 host and Go1.26.3; microCPU0/GOMAXPROCS1, publicCPU0-3/GOMAXPROCS4; balanced two forward/reverse samples. Timings are short observations, not statistical acceptance or original Dgraph/Badger M0 qualification.

## Matched microbenchmarks

| Row | Pre-sprint ns/op | Recent main ns/op | Repaired candidate ns/op | Paired time delta vs pre-sprint | B/op old→candidate | allocs/op old→candidate |
|---|---:|---:|---:|---:|---:|---:|
| get_versioned | 458.300 | 326.850 | 458.200 | -0.02% | 512→0 | 1→0 |
| batch_write | 64087.000 | 66764.500 | 68145.000 | +6.17% | 152722→154894 | 281→285 |
| snapshot_seek | 157.200 | 171.100 | 174.700 | +11.13% | 0→0 | 0→0 |
| repeated_iterator | 229.450 | 228.200 | 230.550 | +0.48% | 113→113 | 2→2 |
| durable_sync | 5264670.500 | 5201904.000 | 4649302.500 | -11.37% | 4784→4698 | 18→18 |

Raw concerns (unchanged5% time, no added allocs, bytes tolerance smallerof1%/64B;0Bstrict):
- get_versioned vs recent-main: paired time >5%
- batch_write vs before-sprint: paired time >5%; added allocations; added B/op above unchanged smaller-of1%-or64B tolerance
- batch_write vs recent-main: added allocations; added B/op above unchanged smaller-of1%-or64B tolerance
- snapshot_seek vs before-sprint: paired time >5%

## Public constant-history mechanism

Work unit:point=one CommitAt then oneGetAt;group_versions=one4-mutation CommitGroupAt then exact two-version owned history;concurrent=onegroup writer+onepoint reader+onehistory reader each64 calls. Equal overwrites cannot prove atomic torn-group behavior, sustained historical retention/native prune/reclaim, or growing histories. Common ownedEntry output and time.Now costs are included. Originaladaptive vs actualrecent/candidatecow_btree are intentionally distinct selectors.

| Profile/placement/workload | Pre-sprint ns/unit | Recent main ns/unit | Candidate ns/unit | Paired delta vs original | Paired delta vs recent |
|---|---:|---:|---:|---:|---:|
| command_wal_durable-forced_pointer-concurrent | 6499203.0 | 5248129.5 | 5166957.0 | -20.51% | -1.55% |
| command_wal_durable-forced_pointer-group_versions | 5864544.5 | 5073334.5 | 5865736.5 | -0.03% | +15.72% |
| command_wal_durable-forced_pointer-point | 4681990.5 | 4798715.0 | 5278791.5 | +12.75% | +10.00% |
| command_wal_durable-inline-concurrent | 5718664.0 | 4893425.5 | 4822382.5 | -15.68% | -1.48% |
| command_wal_durable-inline-group_versions | 5621801.5 | 4907302.5 | 5061681.0 | -9.98% | +3.16% |
| command_wal_durable-inline-point | 4711120.5 | 4633965.5 | 4564766.5 | -3.10% | -1.49% |
| command_wal_relaxed-forced_pointer-concurrent | 514888.5 | 250460.0 | 247454.0 | -51.18% | -1.36% |
| command_wal_relaxed-forced_pointer-group_versions | 624563.5 | 282908.5 | 277416.5 | -55.49% | -0.92% |
| command_wal_relaxed-forced_pointer-point | 7103.5 | 298836.5 | 319727.5 | +4396.44% | +7.09% |
| command_wal_relaxed-inline-concurrent | 150224.0 | 11400.0 | 18198.5 | -87.92% | +59.61% |
| command_wal_relaxed-inline-group_versions | 185609.5 | 12556.0 | 23753.5 | -87.21% | +89.03% |
| command_wal_relaxed-inline-point | 4415.5 | 7712.5 | 19067.0 | +331.76% | +147.73% |

Original M0 remains >10% relative TreeDB/Badger gains BOTH RELAXED+DURABLE, all3 aligned repeats improving and existing guards. No full Dgraph matrix, end-to-end conclusion, merged production or full native qualification from this packet.

Measured invocation wall total 56.93s (excludes3smokes, compilation, inspection). Raw and every sample retained.
