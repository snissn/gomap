# #4142 locality-matrix contract

This is the small, fail-closed M0 gate before expensive matrix execution. It
reuses the #4027 qualification campaign descriptor instead of inventing a
second benchmark format. `preflight_matrix.py` binds an explicit clean measured
source checkout and binary; the evidence-PR checkout is intentionally not the
measured source. It accepts the #4140 measured source `21a57f93...` only when
the binary's Go VCS build metadata names that clean revision with
`vcs.modified=false`, and pins the retained campaign/descriptor hashes.
Historical recall or overlap rows are not promoted to this issue.

The reducer schema reserves the required 16/32/40 × overlap × probe × EF ×
layout rows and binds source, executable, dataset, truth, graph, membership,
router, and query-union identities. It requires a ready pinned preflight,
every explicitly authorized coordinate, and stable membership/router identity
within each topology; it rejects nonterminal, mixed, duplicate-coordinate,
reordered, incomplete, invalid-numeric, or filler-containing rows. The
calibration frontier also requires the strict assignment artifact and rebuilds
the selected overlap before comparing it with the materialized manifest. All generated
rows, DBs, profiles, and logs belong under `/mnt/fast4tb/gomap-4142-locality-matrix-evidence`.

Run the reducer with `reduce_matrix.py --preflight preflight.json --out summary.json rows/*.json`.

Smoke preflight:

```sh
python3 TreeDB/docs/evidence/vector-partition-locality-matrix-4142/tools/preflight_matrix.py \
  --source-evidence TreeDB/docs/evidence/vector-partition-qualification-4027/eed54bc0 \
  --source-checkout /mnt/fast4tb/gomap-4135-combined-measure-runtime/e2e/source \
  --binary /mnt/fast4tb/gomap-4135-combined-measure-runtime/e2e/bin/treedb_vector_partition_bench \
  --dataset-manifest /mnt/fast4tb/gomap-4027-qualification-campaign-eed54bc0/250k/dataset/fixture_manifest.json \
  --truth-artifact /mnt/fast4tb/gomap-4027-qualification-campaign-eed54bc0/250k/truth-cache/m8_canonical_truth_f1fab20b88cd3dcdd6e95a284400983230b1432b36bd4d73e321e251159795ab.json \
  --calibration /mnt/fast4tb/gomap-4105-runtime/artifacts/250k-query-calibration-manifest.json \
  --holdout /mnt/fast4tb/gomap-4105-runtime/artifacts/250k-query-holdout-manifest.json \
  --out /mnt/fast4tb/gomap-4142-locality-matrix-evidence/preflight.json
```

The expected disposition is `ready` when the #4140 source and binary are
supplied. The retained #4027 descriptor is source-older and is used only as a
frozen input anchor; it is never passed off as fresh matrix evidence. No target
byte/row, probe, overlap, or page-objective contract is selected by this
scaffold.

## M0 page-objective checkpoint

The v4 offline trace/simulation is a held-out graph+vector page selection
only, not a recall, throughput, or production-layout result. It uses the
frozen 250k graph artifact and frozen calibration/holdout manifests; snapshots
bind each pack document ID to the artifact ordinal and the simulator fails
closed unless the trusted dataset manifest proves complete disjoint query-split
coverage and identity remapping reproduces every captured holdout page count.

| held-out objective | median pages | p95 pages |
| --- | ---: | ---: |
| identity current / BFS | 1892 | 2347 |
| source | 2496 | 3057 |
| edge window | 1843 | 2323 |
| Gorder-like | 1791 | 2224 |
| co-visitation | 1583 | 2057 |
| hybrid | 1604 | 2066 |

`co-visitation` is the provisional M0 page-objective leader: -16.33% held-out
median pages versus identity/BFS. Second-corpus/query-distribution sensitivity
and integrated construction remain required before selecting a contract. The raw calibration capture SHA-256 is
`84ca7f00e6e658ff951b841d8f367cab4d1809d106516900e4193e1a0c85ec5f`,
holdout is `2d07e255c7c4eecd04596215c523940fe77daec66ed76603ed65f595b3632dd1`,
and the reduced simulation is
`a170741fa6a2dfa6685b5a3a96b751e3ba20ea4f3743baafa259069ff78f5c1d`.
All raw artifacts remain under the task-specific `/mnt/fast4tb` root.

## Clean checkpoint: p32 membership and routing

Checkpoint branch/head before this documentation-only handoff:
`codex/issue-4142-locality-matrix` at
`c0a56597eb82658b4a1dac3d6f3b2f05d9ba2407`. Raw captures,
DB clones, binaries, profiles, timing files, and command files remain outside
the repository at `/mnt/fast4tb/gomap-4142-locality-matrix-evidence`.

The frozen 250k graph input is
`/mnt/fast4tb/gomap-4027-qualification-campaign-eed54bc0/250k/graph-disjoint-out/vector_partition_57ad36d923c5_03e7a26e5610_eed54bc0b9ec.json`
(`57ad36d923c5fdb701a082727fd24efdcf0c6ac0e24efeda28ca11f232a65f1d`).
Strict p32 zero-overlap accounting is
`membership/250k-p32-membership-v4.json`
(`508099904be0d85fa6ff65033adf50dbd342c16d79adaad1a4261c82d893d18b`),
bound to assignment
`membership/250k-p32-assignment-v4.json`
(`808c4dd300063cc0015d936c3f269b93d2a50989ae5aec483820db14fbe69816`).
It has capacity 9,375 and a 50,000 overlap budget: zero and useful-only are
canonical-membership equivalent because the strict cut is zero; exact-20 is
filler-only and rejected, not materialized.

The only materialized topology is the disposable p32 zero-overlap clone:
`materialized/p32-zero-v1/m0-membership-1163546352`. Its materialization report
SHA-256 is
`7e2d7e444006e3eb1f15e898702f4a9508bf2a3a93a8bad5501ff4bc76b0a679`:
generation 2, 32 ready partitions, zero overlap, 186,767,456 pack bytes, and
1,262,091,124 logical clone bytes. The source-ordinal digest before and after
is `79653ed96e52602ec25696de96ef2af4be933f1bbf7cbe18f62a46a18f60418a`.

The quiet-host c64 calibration frontier is retained as
`frontier/reports/250k-p32-calibration-v1.json`
(`cc2356af7968a11933eb55d91c9e39c0a909b13440321e4549284c7a96a62160`).
It contains all 12 `probes={1,2,4} × EF={64,80,96,128}` aggregates and three
counterbalanced repetitions per cell. At c64 all probe counts had the same
recall at each EF: 0.6660, 0.6942, 0.7138, and 0.7367 respectively. The
diagnosis found no pack/source-ID binding defect: approximate c64 missed 1,890
truth-membership slots, while exact routing missed none. The first failed
frontier reduction is deliberately retained at
`frontier/reports/250k-p32-calibration-v1.attempt-1.{stdout,time}`; it failed
closed with `M0 frontier duplicate or missing measurement`, and the succeeding
rerun used the fixed reducer.

The router sweep report is
`frontier/diagnostic/250k-p32-calibration-router-sweep-v1.json`
(`c99da61a5fa24e5eb0c74fa274b92fc5b564b5ccdd89f7cf21e116f926f1c68e`).
Its deterministic coverage results are authoritative: approximate c64/c128/c256
missed 1,890/20/0 truth-membership slots; exact routing missed zero. Thus c256
is the smallest tested approximate candidate budget that restores the exact
coverage ceiling. Its timing, latency, and rate fields are explicitly
non-authoritative: the sibling `.noise.txt` records sustained unrelated
Lean/lake CI load during the capture. This report is diagnostic only and makes
no QPS selection claim.

The completed p32 c256 calibration report is
`frontier/reports/250k-p32-c256-calibration-v1.json`
(`a65c9d397e7df0f8814095df3835388027c2e751ff804605059d1f44b58e0c86`).
It restored routing coverage (zero routing misses) but did not reach 0.950 at
EF96: recall was 0.9277915633, reaching 0.9589330025 only at EF128. Its later
Lean overlap means timing is not used for a QPS selection claim.

The issue dependency route is `#4142 -> #4143 -> #4144 -> #4146 -> #4141`,
with conditional route `#4144 -> #4145 -> #4146`. This checkpoint is not a
production-default, persistent-format, traversal, membership, or router change.

## p40 strict accounting checkpoint

After p32 c256 failed the EF96 recall gate, p40 was the remaining authorized
M0 calibration point. Its strict graph-preserving KaHIP assignment and account
used the same frozen graph artifact and the pinned KaHIP
Python 3.25 executable/adapter (SHA-256
`7d51cd6b48b521277f5caa4610a82126e315fa2be4df069823a8b1eeb5bd4a86` /
`ae4ca8f5f26bd510a507a0f4ba50adaf1e5514ee9e20340cb9d494aba8f54825`).

The persisted assignment is `membership/250k-p40-assignment-v1.json`
(`b07ab6272598447ee517d41665305af776ba806bb94033046b687e283a786040`);
the strict account is `membership/250k-p40-membership-v1.json`
(`15ba48e82841c4154e97f88501721cd37b6afdb4243571561265575e92c0816e`).
It validates the frozen source, IDs, graph, p40 request configuration, and the
pinned KaHIP backend/license identity embedded by the verified adapter.
Its max partition size is 6,561 under cap 6,563; edge cut is 1,046,273;
overlap capacity is 7,500 and exact-20 budget is 50,000. Zero has no overlap
(`3218f36b395897430cd34ee77504aa40a7f7aad5f747bbb00e50153abb2fcb33`).
Useful-only-20 has 50,000 useful and zero filler memberships
(`188f6f44fba75a086cd18749bd66be799489311fc111c44f66d86f5f988a56e3`);
exact-20 is byte-identical, so it is not a separate materialization candidate.

The exact command, stdout, time, and before/after host snapshots are retained
beside those files. It completed in 47.67 seconds with 1,368,896 KiB maximum
RSS. An unrelated Lean process overlapped the assignment, so those resource
figures are provenance only; the canonical assignment/account hashes and
dispositions are the authoritative outcome.

## Final M0 disposition: fail closed

Only p40 `useful_only_20` was promoted. Its disposable clone is
`materialized/p40-useful-v1/m0-membership-4223625656`; materialization report
SHA-256 is `3912fbb2545fe595f47e381a7d958712ed4cdc7bda35f8e97be2ba979c00df94`.
It reopened active/ready at generation 2 with 40 partitions and 50,000 overlap
memberships, selected membership SHA-256
`188f6f44fba75a086cd18749bd66be799489311fc111c44f66d86f5f988a56e3`,
224,093,976 pack bytes, 1,300,142,344 logical clone bytes, manifest digest
`e9cc640645c6506b73a8f04f610593657ed3063c6133c8ed4ad269b1317f52d5`,
and ready-set digest
`36cb1a9933b7d07709d8c44cc0b4646a41233ba132c55c24d4a714f76a7693d3`.
The retained source ordinal digest remained
`79653ed96e52602ec25696de96ef2af4be933f1bbf7cbe18f62a46a18f60418a`
before and after. P40 zero and duplicate exact-20 were not run: the promoted
useful topology failed its first local-recall gate.

The final review-hardened frozen-calibration c256 frontier is
`frontier/p40-useful-v3/250k-p40-useful-c256-calibration-v3.json`
(`0a7f9ddd5269816bae7460cc9f152817f8b9996fcc860872734cb70d29ad8200`).
It binds the p40 assignment/account above, binary
`bfa0bbdb52d52135aaa811feb4e80c11b25bdb9503061cfe2658839e4bd19096`,
frozen graph, calibration split, truth, manifest, and ready-set identities. It
also reconstructs the selected overlap from the strict assignment, compares
the concrete materialized topology, and includes overlap replicas in the
routing oracle. This rerun uses the ordinary non-attribution search path for
timing; its deterministic recall, work, and result fields exactly reproduce
the preceding review-hardened report.

| probes | EF | recall | correct / 8060 | routing misses |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 64 | 0.7678660049 | 6189 | 1069 |
| 1 | 80 | 0.7991315136 | 6441 | 1069 |
| 1 | 96 | 0.8176178660 | 6590 | 1069 |
| 1 | 128 | 0.8389578164 | 6762 | 1069 |
| 2 | 64 | 0.9031017370 | 7279 | 0 |
| 2 | 80 | 0.9310173697 | 7504 | 0 |
| 2 | 96 | 0.9496277916 | 7654 | 0 |
| 2 | 128 | 0.9700992556 | 7819 | 0 |
| 4 | 64 | 0.9031017370 | 7279 | 0 |
| 4 | 80 | 0.9310173697 | 7504 | 0 |
| 4 | 96 | 0.9496277916 | 7654 | 0 |
| 4 | 128 | 0.9700992556 | 7819 | 0 |

The required EF96 threshold is 0.9500 = 7657/8060 correct. Both p2 and p4
achieved 7654/8060, three short, despite zero routing misses. Therefore the
single M0 blocker is **local-pack approximate HNSW recall at the maximum
allowed EF96**. The report's three-repetition recall/work/result identities are
invariant, but QPS/latency are non-authoritative: the final rerun still had up
to 10.65% per-cell QPS spread. No QPS selection claim is made.

The final search holdout remains sealed. Do not start #4143, #4144, #4145, or
#4146: the dependency checkpoint remains
`#4142 -> #4143 -> #4144 -> #4146 -> #4141`, conditional
`#4144 -> #4145 -> #4146`, and #4142 has failed closed before its entry gate.
