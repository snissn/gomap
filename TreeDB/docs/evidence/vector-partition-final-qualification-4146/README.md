# #4146 final TreeDB qualification

Disposition: **failed closed**. The frozen 250k EF96 point measured recall@10
`0.9304`, below `0.9500`. No post-result tuning or competitor rerun was done.

## Frozen matrix

TreeDB only, 100k/250k 128d cosine, top-k 10, probes 2, c1/c32, 1,000
warmup plus 1,000 measured queries, and three serialized counterbalanced
repetitions. EF64/80/96/128 were run. The owner-bounded execution omitted EF256
because EF128 already demonstrated recall above `0.9500` at 250k.

Medians across three repetitions:

| Corpus | EF | Recall@10 | c1 QPS | c1 p95 ms | c32 QPS | c32 p95 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 100k | 64 | .9384 | 2,236.4 | .527 | 10,557.4 | 4.962 |
| 100k | 80 | .9599 | 2,114.4 | .568 | 9,741.7 | 5.265 |
| 100k | 96 | .9738 | 1,998.7 | .608 | 9,214.9 | 5.802 |
| 100k | 128 | .9883 | 1,819.7 | .689 | 8,248.1 | 6.284 |
| 250k | 64 | .8778 | 1,950.1 | .605 | 7,001.9 | 7.998 |
| 250k | 80 | .9095 | 1,798.4 | .963 | 5,900.3 | 9.201 |
| 250k | 96 | **.9304** | **1,881.1** | **.649** | **8,275.0** | **6.508** |
| 250k | 128 | .9540 | 1,542.7 | .854 | 6,898.6 | 8.144 |

Recall, candidates, edges, requests, routes, and selected partitions were
bit-identical across repetitions and concurrency for every EF. All cells had
zero errors, timeouts, retries, and redirects.

## Gate

| 250k EF96 gate | Target | Result |
| --- | ---: | ---: |
| recall@10 | >=.9500 | **fail: .9304** |
| c1 QPS | >=2,000 | **fail: 1,881.1** |
| c32 QPS | >=9,000 | **fail: 8,275.0** |
| c1 p95 | <=.700 ms | pass: .649 ms |
| c32 p95 | <=7.250 ms | pass: 6.508 ms |
| query failures | zero | pass |
| overlap filler | zero | pass |

The single actionable blocker is local approximate-search recall at the highest
allowed EF. Throughput is also below its absolute target at that point, but it
is not useful to optimize until the deterministic recall gate is restored.

## Retained #4140 comparison

No external system was rerun. Against the retained #4140 TreeDB result
(`e9e6076f...ea6`):

- 100k matched recall: current EF80 recall `.9599` versus old EF64 `.9525`;
  c1/c32 QPS changed `-3.8%/-10.2%`. This is borderline at c32, not a win.
- 250k nearest matched point: current EF128 recall `.9540` versus old EF128
  `.9580`; c1/c32 QPS changed `+1.1%/+10.9%`. This is an improvement, but not
  the required EF<=96 result and not the 20% hoped-for gain.
- At identical 250k EF64, recall improved `.8647 -> .8778`, while median QPS
  regressed `-6.7%` at c1 and `-31.5%` at c32. The large timing ranges make
  those throughput deltas diagnostic rather than an optimization claim.

Thus the scale-aware substrate improved storage/layout correctness and some
quality/throughput points, but did not finish the final performance objective.

## Provenance

- measured executable source: `939c71b6357c41d569af681fd7b95aea705978a4`
  (`vcs.modified=false`)
- executable SHA-256: `07e6dea0470bff680ebfcb747e43d9282eb30c9d4d0819344a1da9723191f527`
- runner SHA-256: `a1d58567ca9f927817857cc4a13aadfb63178505740ea3d3b819587ebacbcae1`
- compact raw result: `/mnt/fast4tb/gomap-4146-final-evidence/result.json`,
  SHA-256 `4b8d37e61a09f18640543b9aa587653a11e013b973a79a5277c635e6cbab61f0`
- raw runs: `/mnt/fast4tb/gomap-4146-final-evidence/verified-runs/treedb_single`
- 100k/250k asset descriptor SHA-256: `30e42712...bacf` /
  `7b1c2497...f940`
- 100k layout: 16 partitions, zero replicas/filler, 73,211,456 pack bytes
- 250k layout: 40 partitions, 50,000 useful replicas, zero filler, 220,027,376
  pack bytes; every final partition has 7,500 memberships
- node peak RSS: 407,084 KiB at 100k; 1,211,024 KiB at 250k
- host: Linux 6.8.0-136-generic, i5-11400F, 12 logical CPUs

The merged planner exposed one benchmark integration defect: `system-node`
still assumed four partitions per group. Commit `939c71b63` makes the
existing-asset path derive the already-validated manifest partition count;
the focused reopen/relabel test covers that behavior. The failed p16 launch is
retained under `/mnt/fast4tb/gomap-4146-final-evidence/failed-attempts` and was
not included in reduction. The first clean 100k set used the pre-fix `8ad`
executable; it was preserved under `superseded/` and rerun on `939c` so every
reduced row has one source revision and executable digest.

## Reproduction

```sh
python3 TreeDB/docs/evidence/vector-partition-final-qualification-4146/tools/test_reduce.py
python3 TreeDB/docs/evidence/vector-partition-final-qualification-4146/tools/reduce.py \
  --runs /mnt/fast4tb/gomap-4146-final-evidence/verified-runs/treedb_single \
  --out /mnt/fast4tb/gomap-4146-final-evidence/result.json
```
