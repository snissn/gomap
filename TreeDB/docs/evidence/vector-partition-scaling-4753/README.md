# Graph-partition scaling: F evidence (#4753)

The original comparisons, prospective resource collection and landed-source
verification of the separate D4 extension are complete.
This is negative/partial evidence, not final qualification or permission to
resume Raft work. [#4753](https://github.com/snissn/gomap/issues/4753) owns the
verdict; [#4772](https://github.com/snissn/gomap/issues/4772) owns the explicit
supported-envelope handoff. Apparatus merges do not discharge either gate.

Outcome: the 23 original reports yield 100 quality-passing pairs; all 70
selected/exhaustive and ten overlap pairs pass their performance gates. The
ten packing pairs fail both throughput and p95 gates, and all ten affinity
pairs fail the throughput gate. The separate D4 extension passes ten pairs
but does not rewrite the original D4 admission failure. Seven assets now have
28 independently validated, single-observation serving-resource cells. The
real standalone lifecycle passes; public/Raft, real250K and online-fold
qualification remain outside the demonstrated result.

## Frozen product and controls

Candidate source is `f406ad74e7e523088197c66768e3e6be19bbecad`, full tree
`c4504079be6270b6d38bfc16067b35784329fe16`. It selects kRt routing, the
connectivity-preserving Vamana extension and the accepted partition/membership
construction. Serving fixes C256/B256, EF96, top10 and recall at least 95%.
This is not unmodified paper Vamana, a paper-QPS reproduction, or the rejected
partition-local HNSW candidate.

The evidence-only affinity control is
`4dd437c3d71774d592c485c9135178ffbb40cbae`. It restores exactly
`TreeDB/internal/vectorpartition/vectorpartition.go` from candidate blob
`c03e3e2e2b8230be2b02aacd9cf583f29789fdf7` to predecessor blob
`ae407ce383eac4e5f2a26922f172ec110d9e859f`. Router, local search,
materialization, validation and measurement remain current. This isolates the
affinity construction; it is not a complete historical-release comparison.

The candidate and control Linux executable SHA256 values are respectively
`1c29b87f45dbf2a984df2a28968e7ad2a6ddec034f0bc09f69931a21ac0a2850` and
`9628cbff8b96e0a2fe9a25061b5f2d9b1eaebdfffbfed17a72c39d02217b3e2c`.
Both are built from clean exact sources; their common correctness checks pass.

## Completed structured comparisons (not the final verdict)

Both reports have 20 complete cells: five paired repetitions at c1/c32,
selected P1 versus exhaustive P16, with 1000 successes and no failures per
cell. Their ordinary-reference commands independently replayed the parent
reports, including exact-union ID and score parity. A separate read-only
reduction authenticated those original receipts and applied the frozen
`m8CompareScalingPairV1` helper; every paired quality/QPS/p95 gate passed.

Ranges below cover all five individual ratios, not a ratio of medians.

| Procedural corpus | Recall, both arms | QPS ratio c1 | QPS ratio c32 | p95 ratio c1 | p95 ratio c32 |
| --- | --- | --- | --- | --- | --- |
| 100K x128 |99.79% |4.387–4.420 |12.633–14.285 |0.218–0.223 |0.087–0.117 |
| 250K x128 |98.16% |5.005–5.144 |14.761–15.475 |0.182–0.195 |0.076–0.092 |

Ordinary whole-collection reference recall at EF96/256/512 is respectively
96.78%/99.42%/99.87% for 100K and 89.15%/97.34%/98.77% for 250K. The 250K EF96
reference fails 95% and remains visible. These reference rows are not the
causal baseline used for the ratios above.

Under each `assets/<asset>/m8-series1-block1-p1/` directory:

| Asset | Original report SHA256 | Ordinary-reference JSONL SHA256 |
| --- | --- | --- |
| `structured100k-d16-one` | `c0328ab13be796daebc63b8ba9dfbae8c29f8680ee96129f40eeae15a6a25d9c` | `7490061c98e07440772390874282a40cdb0f8637251264c4bbb0e8b95413ae70` |
| `structured250k-d16-one` | `2842c844f9b4214387b30b3dfc48f79e965627e4dcf4651e7b74ce2dc91b0dd9` | `e9815c40cb26f6ac5ccc75bd2d0d383ac92b1e554f670878ca6f77bf27882295` |

The independent-copy reduction log is
`analysis/structured-frozen-helper/run.log`, SHA256
`a758731f09cc52b215aba58b1c8a6be30e644221e59fa1ad231215d529e64f12`.
Its hostile missing/duplicate/identity/order/chronology/union/hash/failed-quality
controls also pass. The original real 768-dimensional comparisons and the
separately completed real lifecycle result are recorded below. These two
structured reports do not establish either result or final qualification.

## Real-data preparation (not serving results)

The old-affinity and candidate one-pack assets have both been built and copied
to the independent verification host. Their pinned canonical-truth membership
preflights require P5 of D16 for 95% routing availability. This is an upper-bound
availability check, not measured ANN recall or serving performance.

| One-pack asset | Available top10 hits at P4 | At P5 | M3 report SHA256 | Prefix receipt SHA256 |
| --- | --- | --- | --- | --- |
| Old affinity |4764/5120 |4881/5120 | `2a590e1ca8aef2d7fd2fd94ef37a69e07cd87dbe93ece4edb79f5215e89c993c` | `585a43c8a4172a8d8fdedd32ba431c76279de617e0cb7379d730973e0cb1c382` |
| Candidate |4798/5120 |4903/5120 | `e83a09f17cb14a32819e43be2acf072ed640727e3ca9a72314fc14a314ba944f` | `7f1d8465f18c166c47908ca5fdb9a321b822ce81d1956ad90359ed390fb42e5b` |

Their logical membership-union hashes differ as expected for an affinity
treatment: respectively `8328ceb0ac900cfddf32f0de7aadd142957f7c469b0b974e0a1f571eda889d37`
and `36bee3befe9e258617d364d860a740affc7a633a5b65fe8591383c6afec68403`.
The equal-union gate instead applies to candidate one-pack versus candidate
disjoint multi-pack. That gate passes: the independent multi-pack copy has
the same `36bee3be...68403` union and identical P1..16 availability curve,
with four packs per domain instead of one. Its M3 report SHA256 is
`a20dd31024310722415ab86f4c7c2383d8fe7f5e6b000e1e4dd173b68dc95ed3`;
prefix receipt SHA256 is
`2edcd318c9ceb86623821c51f1c9ccd630756fc26c34393ee5a20cc6afa6414d`.
The five serving blocks and their independent verification are complete below;
these earlier preflights alone do not establish serving outcomes.

The candidate overlap asset also built successfully. It realizes 20,000 useful
replicas with zero filler and zero unused budget. M3 reports zero missing,
corrupt or stale assets; these are construction checks, not the final serving
recall. Whole-command build wall time was 13m52.50s, maximum RSS 3,610,396 KiB,
with zero swaps. The derived-build timer is 124.681s and excludes earlier
construction work; it must not be substituted for whole-command build time.
Derived physical storage is 406,544,142 B beside 1,645,787,004 B source storage.
M3 report SHA256:
`9a6e214a9df7956fe7d7ce9c4234aacad9edbeaa1134ad10cd5e5d1fdffea1b1`.
Its independent-copy routing preflight selects P4/D16 (16/64 packs), with
4,927/5,120 available truth hits. P3 has 4,801 hits and fails 95%. This reproduces
the earlier widened-profile availability, without turning it into a new ANN
measurement. Prefix receipt SHA256 is
`d122170c8dc2337f394c8af34732994829d120156af87cd38dd52877f898004d`;
logical union is `968510559a398c0c37664d2eb664c5e0913c765d2af1ce12a3bad6fffe718427`.

## Membership, routing and navigation are separate

First-report selected-coordinate diagnostics use the same canonical 5,120 truth
slots. The membership oracle chooses the best allowed logical domains under
the joint physical-pack constraint; it is not the production selector.

| Real profile | Selected / total domains | Selected / total packs | Feasible membership ceiling | Actual routed availability | Returned ANN recall |
| --- | --- | --- | --- | --- | --- |
| D16 disjoint one-pack | 5 / 16 | 5 / 16 | 99.726563% | 95.761719% | 95.703125% |
| D16 disjoint multi-pack | 5 / 16 | 20 / 64 | 99.726563% | 95.761719% | 95.761719% |
| D16 overlap multi-pack | 4 / 16 | 16 / 64 | 99.785156% | 96.230469% | 96.230469% |
| D40 overlap multi-pack | 8 / 40 | 16 / 80 | 100% | 95.859375% | 95.859375% |

Splitting the disjoint domain unions leaves both membership feasibility and
routed availability unchanged; the smaller local graphs recover three truth
hits but cost more physical searches. At the two overlap operating points,
local search returns every available truth hit: remaining recall loss is in
domain selection, not local navigation or merge. All four report exact
exhaustive-union ID and score parity. Independent replay authenticates that
separate correctness check; exhaustive-domain ANN recall is not its oracle.
Legacy attribution field names contain `hnsw`; the pinned manifest and
selected graph variant identify connectivity-preserving **Vamana** throughout.
These are diagnostic stage measurements, not additional timed populations or
permission to reopen local-search tuning.

## Real serving blocks (five collected and independently verified)

The isolated affinity treatment has mixed placement diagnostics on this one
construction seed; five serving blocks are not five independent builds.

| Construction diagnostic | Old affinity | Candidate |
| --- | --- | --- |
| Best primary-home coverage, P1 |73.535156% |73.085938% |
| Best primary-home coverage, P2 |88.867188% |89.472656% |
| Best primary-home coverage, P4 |98.144531% |98.671875% |
| Truth-neighbor pair colocation |13,944/23,040 |13,798/23,040 |
| Approximate graph truth-neighbor edges |821 |878 |
| Those edges retained within homes |686 |721 |

Thus improved P2/P4 coverage and more retained graph truth edges do not imply
universal improvement in colocation or low-fanout coverage. These are offline
truth/graph diagnostics, not actual router recall. The old/candidate build
report SHA256 values are respectively
`18b3494a79700622974c12ce5d03b4fea871b08e42e456fd112f5a7e17cc7cde` and
`63479b2f0db0197289c662abab55cdffbd8cd1c94160f4b23d512120f58a3f8f`.

The first old-affinity, candidate one-pack, disjoint multi-pack and overlap reports each retain four
complete 512-query cells, with zero refusals, timeouts, cancellations, errors,
invalid responses or undispatched queries. Both one-pack and disjoint multi-pack
independent replays pass; overlap's completed ordinary-reference command supplies
its strict replay. Under the prospective inclusion rule, block 1 counts and the
four prefixes are frozen at 5/5/5/4 in control/one-pack/multi-pack/overlap order.
No reset or five-block claim follows.

| Asset | Selected recall | P16 recall | Selected/exhaustive QPS c1 | QPS c32 | p95 c1 | p95 c32 |
| --- | --- | --- | --- | --- | --- | --- |
| Old affinity | 95.292969% | 99.941406% | 2.292 | 2.819 | 0.488 | 0.346 |
| Candidate | 95.703125% | 99.902344% | 2.265 | 2.785 | 0.477 | 0.358 |
| Candidate disjoint multi-pack | 95.761719% | 99.980469% | 2.756 | 2.450 | 0.368 | 0.419 |
| Candidate overlap multi-pack (P4) | 96.230469% | 100% | 3.435 | 3.365 | 0.288 | 0.333 |

The three disjoint arms use P5; overlap uses P4. The overlap arm returns all
4,927 available truth hits at that prefix, versus 5,120 at P16. Its original is
`assets/real100k-d16-overlap-multi/m8-series1-block1-p4/vector_partition_m8_f406ad74e7e5_9570ab1894bb.json`,
SHA256 `980c89a9af257d6c3ece79bd092ddf67e383584f269cabc25db6625f158fff38`.
Whole-command wall time was 8m02.67s, user CPU 529.71s and maximum RSS
2,014,544 KiB, with zero swaps and exit zero. These include untimed harness work.
The ordinary-reference JSONL SHA256 is
`962f4ebfd08ed8b236436fe3003978a77e09b46dfc3f823d6c20ea281dc5eb78`,
command SHA256
`80e67f87a931a4ff9717460c306aa690535f16cbdc07b86247e51bb523bd0cbe`.
Its header accepts the exact parent report's four rows; its complete footer
retains all 30 reference cells, with 512 successes each and zero failures.

| Real ordinary HNSW EF | Recall | QPS c1, all five repeats | QPS c32, all five repeats |
| --- | --- | --- | --- |
| 96 | 97.5% | 4,192–4,269 | 10,928–11,219 |
| 256 | 99.335938% | 1,786–1,826 | 4,863–4,872 |
| 512 | 99.6875% | 993–1,004 | 2,664–2,684 |

The ordinary in-process HNSW reference is substantially faster on this host
than the selected partitioned TCP/Vamana observations. Its interface and
profiling differ, so this is context, not an isolated algorithmic effect or a
causal ratio. Passing selected-versus-exhaustive partition gates would **not**
establish superiority over an ordinary single index or multi-host capacity.

These are selected-versus-exhaustive ratios within each report. They are not
affinity-treatment improvement ratios. The old-affinity report is
`predecessor/assets/predecessor-real100k-d16-disjoint-one/m8-series1-block1-p5/vector_partition_m8_4dd437c3d717_53be7118ae93.json`,
SHA256 `436a796634bf7feda3b805d6dc89fb519ea81d103960ff440fa316fe234a0f19`.
The candidate report is
`assets/real100k-d16-disjoint-one/m8-series1-block1-p5/vector_partition_m8_f406ad74e7e5_cce3c0848797.json`,
SHA256 `a98d2d96a736c9761434d51d99165aafed439200d7a56d703e102dcd6406fd4c`.
Every raw report and its original red gate ledger remain unchanged.
The old-affinity `replay/run.log` SHA256 is
`80c6e1896eb256e6e360ef194cb39f5ed7c2703b359238782f6019428829806d`;
its command SHA256 is
`aa70527b4e83714fdc55cf1bc47c2b7c15ec8af9426e23e06849637c258764c3`.
It exits zero and states `REPLAY_ACCEPTED_NOT_QUALIFICATION` for the exact
original report and all four rows.
Candidate one-pack replay log SHA256 is
`4f8fbf14ed62f844c05ae01b0c5a09fa39a6a1a37c662bdfb0db87f472ed12b7`,
command SHA256
`45fc0093a5fcbbb31c45fdb87b878ca975aedfebd712bc9528d61643e4bd831d`;
it also exits zero with all four rows accepted.

The disjoint multi-pack original is
`assets/real100k-d16-disjoint-multi/m8-series1-block1-p5/vector_partition_m8_f406ad74e7e5_cd0da2053e82.json`,
SHA256 `a384e908ef45e9771b459b8e193508f92aff17384517f84b1d014dd3bc920550`.
Its successful independent replay log SHA256 is
`7892bc98a59ad2772e7fd38ef2172a17d5a179b6a7361e9a00b130888d3f6a70`,
command SHA256
`8b0d9dec502448ee8463f16136d819d2d685d1c1f6eeaadc091f2165af6f714d`.
Packing has a visible cost: its first-block selected QPS is 0.544x/0.462x the
one-pack result at c1/c32, with p95 ratios 1.659/2.214. Splitting into smaller
packs preserves the logical union but increases physical search work; it is
not a universal throughput improvement. The completed five-block comparison
below includes this first block and preserves that regression.
The retained counters support that interpretation: selected local scoring
increases from 6,189,176 to 13,398,842 calls over the same 512 queries
(2.165x), while router scoring remains exactly 256 calls/query. Mean RPCs
increase from 3.508 to 4/query; response payload rises from 1,645,952 to
4,775,936 bytes over the 512-query cell. These are counters, not inferred
allocation or CPU measurements.

The same first-block counters price the physical work; every denominator is
the complete 512-query cell. The persistent-asset column excludes separate
shard-generation metadata and the source DB, so it is not total database size.

| Real profile | Persistent assets (B) | Local scores/query | RPC response B/query |
| --- | ---: | ---: | ---: |
| D16 disjoint one-pack | 332,784,984 | 12,088.23 | 3,214.75 |
| D16 disjoint multi-pack | 333,098,128 | 26,169.61 | 9,328 |
| D16 overlap multi-pack | 398,955,504 | 21,950.42 | 7,744 |
| D40 overlap multi-pack | 398,223,400 | 19,443.55 | 7,744 |

All four perform 256 router scores/query. D16 overlap's complete derived
physical storage is 406,544,142 B versus 339,411,509 B disjoint (1.198x), with
the same 1,645,787,004 B source DB. The lower selected fanout trades that
additional storage for fewer local scores and returned bytes. These are not
isolated per-cell heap or CPU measurements, and D40 versus D16 is a descriptive
shape comparison, not a controlled causal speedup.

Block 2 finished at 2026-09-21T12:06:03Z in the prospectively reversed
arm/probe/concurrency order. All 16 cells again have 512 successes, zero
failures and the same recalls as block 1. All four independent strict replays
pass, each accepting its original four rows. Subsequent scheduled blocks ran
serially in their frozen order, without reselection or measured retries.
Block-2 originals are:

| Asset | Report SHA256 |
| --- | --- |
| Old affinity | `39e7457518d57206988c077ec4932557c9854ad96baa6c8f5efcab0e42f7fdd0` |
| Candidate one-pack | `f37e25138bb8efc61a4da1d6496336c63591590fececfbddb4228a2108be38bd` |
| Candidate disjoint multi-pack | `7d69e3321506d20aa731140588df6b250f27d8fff6ea4bf35a939e7dc82a4130` |
| Candidate overlap multi-pack | `ab2088fc2e3ae39cd61fc1d0571a17dd3f8d55d3e544618f9fd3829c90d54d18` |

Block 3 completed at 2026-09-21T12:52:32Z in the declared forward order.
Every cell again has 512 successes, zero failures and unchanged recall.
All four independent block-3 strict replays now pass. The overlap replay
was retried only after a prelaunch host-load refusal; that unmeasured refusal
remains under its block directory at
`replay-prelaunch-load-refusal-20260921T131021Z/`. The successful overlap
`replay/run.log` SHA256 is
`eece806b3dcc3997b8bcdce1f0e4b8a9a942203a8a7dc45fbd4536142a594abf`.
No measured serving block was retried or discarded.

| Block-3 asset | Report SHA256 |
| --- | --- |
| Old affinity | `ffb3f0f0185723561ce5bd32c729f04d1d33a2311f0305f2d32364cc8a1dc03d` |
| Candidate one-pack | `dff360d1a4e128563003d0c4e01bcd3e42d30bf25bad6292656e7a861de333c6` |
| Candidate disjoint multi-pack | `ba8244a4ed9212794d7852c6cccb5058cfc4c7169665839ed64e10d740cbc61c` |
| Candidate overlap multi-pack | `4d7fa5fb3b2c0917bc8b4af0b7e2dcdde32137591d0c90b606b234f82de19184` |

Block 4 completed at 2026-09-21T13:21:19Z in the declared reversed order.
All 16 cells retain 512 successes, zero failures and unchanged recalls.
Independent block-4 replays pass for all four shapes, including the
old-affinity control; each exits zero on `.185`.
The completed `block4-collection.log` SHA256 is
`3c9829c27ca128c39fe9bb6efe3d126e2b29bff4e88d7f785ac757e26a88e2fa`.

| Block-4 asset | Report SHA256 |
| --- | --- |
| Old affinity | `c55dfabd50cd05970d755dd0f95b7975bef53aaa2bda0941a2750a9ea3a89c39` |
| Candidate one-pack | `b4ff414e62de78a688e51e79c450508f5617d3261522f6c9f9ae62d561ebe620` |
| Candidate disjoint multi-pack | `f2b1fc9ea630f29ecb22d798f88f4b1adeefa21f003b27e404b62eebe3001ac4` |
| Candidate overlap multi-pack | `1b728e8e98a33cf7eebfccb3412d7c2848f6f95b6ecaa2e90077146e6a5128a1` |

Block 5 completed in the frozen forward order. The collection process exited
zero; its wrapper log ends at 2026-09-21T13:50:04Z. All four frozen shapes
retain four complete cells each: 512 declared/dispatched/successful requests,
with zero refusals, timeouts, cancellations, ordinary errors, invalid responses
or undispatched requests. Selected/exhaustive truth hits at both c1 and c32
are unchanged: old affinity 4,879/5,117, candidate one-pack 4,900/5,115,
disjoint multi-pack 4,903/5,119, and overlap 4,927/5,120, each over 5,120 slots.
The original reports below were resolved from each exact `replay-args.json`
`-report` path, excluding similarly named measurement transcripts.

| Block-5 asset | Report SHA256 |
| --- | --- |
| Old affinity | `961a2e81bb6531f96570238b087bcae65a3081a09698e8b4cd721e5100c3e1b1` |
| Candidate one-pack | `dffef672fc3cbe3085f74ce0fc7a3a204139d41388a6fc2b465855d2fd832a6c` |
| Candidate disjoint multi-pack | `158ce65f98115dbf988a6b67edd5b3e05997f1094381e91638e2b1030f7366b1` |
| Candidate overlap multi-pack | `982762185e0a2316a8ef5afd862c52b32149005089bd34d3ca560ba9f7a04101` |

Completed `block5-collection.log` SHA256:
`b8c6640839550a6c0241f19264929b571e3e8f90fb7240229e31ecb5b9c83485`.
All five D16 serving blocks are collected without reset and strictly verified.
The authenticated original comparison below retains every block. This does
not imply all-axis completion or that every controlled intervention won.

## Authenticated original comparison

The [root-attested inventory and chronology](https://github.com/snissn/gomap/issues/4753#issuecomment-5762088854)
were frozen before invoking the independently reviewed partial reducer.
`TestFReduceRetained` passed in 57.15s, process exit zero, on the independent
`.185` copy. It authenticated all 23 reports and prior verification receipts,
checked source/control identities, non-overlapping original chronology,
alternating external order and equal logical unions for the packing comparison.
Raw reducer output is `analysis/original-final-20260921/run.log`, SHA256
`da5f84b51a42e0a0ee5d1488ca376007263bc7bbc032a3f14f06bf8279a5e0dd`.
Inventory SHA256 is
`4f801faca5452a8f4b60515407e6f1b13fe59adac65ed49e971be2f466f12d94`;
chronology SHA256 is
`fe1f0edef2fc957a2c0b784e348826fcb0b8004144dd5cb11559a60f0c679045`.

[original-comparisons.json](original-comparisons.json) retains all 100 derived
pairs and the original report pins; full raw attempts remain in those reports.
All 100 pairs meet matched quality and complete-attempt requirements. All 70
selected/exhaustive pairs pass >=1.15x QPS and no p95 regression. Ranges below
cover five individual ratios; numerator is selected, denominator exhaustive.

| Profile | QPS ratio c1 | QPS ratio c32 | p95 ratio c1 | p95 ratio c32 |
| --- | --- | --- | --- | --- |
| Structured100K |4.387–4.420 |12.633–14.285 |0.218–0.223 |0.087–0.117 |
| Structured250K |5.005–5.144 |14.761–15.475 |0.182–0.195 |0.076–0.092 |
| Real D16 overlap multi-pack |3.395–3.435 |3.172–3.462 |0.288–0.292 |0.298–0.333 |
| Real D16 disjoint multi-pack |2.750–2.800 |2.450–2.510 |0.352–0.372 |0.391–0.429 |
| Real D16 disjoint one-pack |2.234–2.265 |2.692–2.886 |0.477–0.495 |0.336–0.388 |
| Real D40 overlap multi-pack |3.841–3.935 |3.999–4.121 |0.271–0.280 |0.226–0.260 |
| Real D16 old-affinity one-pack |2.292–2.312 |2.819–2.895 |0.467–0.488 |0.341–0.357 |

The 30 controlled cross-arm comparisons show materially different outcomes:

| Treatment / baseline | QPS ratio c1 | QPS ratio c32 | p95 ratio c1 | p95 ratio c32 | Frozen improvement gates |
| --- | --- | --- | --- | --- | --- |
| Current / old affinity, one-pack |0.993–1.008 |0.971–1.006 |0.987–1.020 |0.966–1.048 |0/10 meet QPS target; 4/10 meet p95 guard |
| Four packs / one pack per disjoint domain |0.542–0.548 |0.459–0.474 |1.607–1.684 |2.071–2.224 |0/10 pass; material cost increase |
| Overlap P4 / disjoint P5, both multi-pack |1.179–1.199 |1.152–1.194 |0.811–0.851 |0.821–0.923 |10/10 pass |

The packing treatment preserves logical membership and routed availability;
its three additional truth hits do not erase roughly halved throughput and
higher tail latency. Current affinity improves selected recall over the old
control but does not deliver the declared 15% throughput improvement. Overlap
buys better quality with lower serving fanout at the documented replication
cost; these data do not imply that replication or packing is universally free.
No failing pair is omitted or accepted as a residual here.

The reducer's status is
`PARTIAL_DERIVED_F_COMPARISON_D4_BUILD_ADMISSION_REJECTED`: 23/24 original
reports, 20 unmeasured D4 cells, ten unavailable pairs and
`AllAxisCollectionComplete=false`. The new-source D4 extension and resource
observations stay separate. A valid reduction is not the final F verdict.

### Physical-pack cost diagnosis

The original block-1 P5/D16 reports independently reproduce the same counters
at c1 and c32. Four packs/domain multiply local search work, not RPC count:

| Per selected query | One pack/domain | Four packs/domain |
| --- | ---: | ---: |
| Local ANN searches | 5 | 20 |
| Navigation candidates | 11,608.234375 | 24,249.61328125 |
| Canonical rescore calls | 480 | 1,920 |
| Total local score calls | 12,088.234375 | 26,169.61328125 |
| RPCs | 3.5078125 | 4 |
| Maximum per-pack results entering merge | 50 | 200 |
| Response bytes | 3,214.75 | 9,328 |

Navigation candidates are modeled `candidate_bytes / 64`, not allocation bytes.
`shard_plan.go` stripes home memberships by source-ordinal position modulo the
pack count. Each pack then builds an independent Vamana graph. The coordinator
expands selected domains to all their packs; the shard searches those packs
sequentially, each with its own EF96 and top10. The shared score ceiling is a
failure bound, not a shared frontier or cross-pack stopping rule. Production
placement maps the packs onto four groups, explaining why RPCs grow 14%, not 4x.

The extra 14,081.37890625 score calls/query comprise 12,641.37890625 navigation
candidates and 1,440 canonical rescoring calls. This proves extra local work;
it does not attribute its wall-time fraction to scoring, allocation, storage
or transport. The completed prospective receipts below add worker-window CPU
and allocation totals, not a causal breakdown of those wall-time fractions.

A bounded read-only inspection of the original block-1 CPU profiles, using
`go tool pprof -top -focus=searchWithOptionsV1` and the frozen f406 executable,
adds stack attribution without rerunning serving:

| Sampled CPU in original combined profile | One pack/domain | Four packs/domain |
| --- | ---: | ---: |
| Entire profile | 35.81 s | 67.46 s |
| Local `searchWithOptionsV1`, cumulative | 25.97 s | 55.85 s |
| Native traversal, cumulative | 20.93 s | 38.20 s |
| Canonical result processing, cumulative | 4.92 s | 17.32 s |
| Indexed FP32 dot kernel, flat | 18.35 s | 31.87 s |

These profiles combine selected/exhaustive and c1/c32 activity with the
original profiler boundary, including warmup and untimed harness work. The
cumulative rows overlap: do not sum them, normalize them into serving CPU/op,
or treat their ratio as a repeated performance gate. They support traversal
and canonical rescoring as material costs, not an isolated per-cell fraction.
Under the two assets' `m8-series1-block1-p5/profiles/run/cpu.pprof`, the SHA256
values are respectively
`85a44c80831bb24fc94813863411e319b69e8d94fcc14dbd73b3ae8add4e5d9e` and
`cbad22406425cbe5b08468dbe9c43e3e47726ba6ce46d3a75c0e4abe2bab2146`.
The native searcher already uses prepared packs and pooled traversal scratch;
this inspection does not justify speculative new caching or pooling.

Neighborhood-aware packing remains a testable construction intervention, not
a demonstrated fix. With twenty EF96 searches unchanged, navigation would
have to fall another 58.1%, to approximately 508.4 candidates/pack, just to match
the old total score count; this is not a QPS prediction. Any such treatment
must hold logical unions, four packs/domain, zero overlap, routing, EF96, placement
and queries fixed, reuse existing graph/affinity and byte-cap machinery, and
rebuild/reopen the actual production assets. A shared domain traversal or
admissible pack-skipping mechanism would change search contracts and requires
an explicit graph decision, not an implicit parameter adjustment.

## Inputs, boundaries and protocol

The historical structured profiles have 100K/250K vectors, 128 dimensions and
1000 queries, with seeds 4017/4016. They are procedural embedding mixtures,
not real embeddings and not nested identical-query corpus-growth treatments.
The real fixture contains 100K Cohere 768-dimensional vectors and 512 queries:
source train rows 0..99999 and test rows 200..711. Its queries were used during
fanout selection and are not a fresh holdout. Real 250K support is not measured here.

The successful-profile minimum is 24 original reports over eight once-built
assets. Every report retains its own source, command, execution identity,
attempts, transcript and seven independently pinned replay inputs.

| Asset | Corpus | Domains / packs | Overlap | Blocks |
| --- | --- | --- | --- | --- |
| 1 | structured100K | 16 / 16 | none | five internal |
| 2 | structured250K | 16 / 16 | none | five internal |
| 3 | real100K768 | 16 / 64 |20% | five external |
| 4 | real100K768 | 16 / 64 | none | five external |
| 5 | real100K768 | 16 / 16 | none | five external |
| 6 | real100K768 | 4 / 60 |20% | five internal |
| 7 | real100K768 | 40 / 80 |20% | five internal |
| 8 | real100K768, old affinity | 16 / 16 | none | five external |

External blocks alternate `[8,5,4,3]` and `[3,4,5,8]`; probe and concurrency
order reverse on even blocks. Internal reports retain their existing balanced
order. Every required selected/exhaustive pair must independently meet 95%
recall, zero failures, at least 1.15x QPS and no p95 regression, at c1 and c32.
Slow quality-passing blocks count. Prefixes can advance by exactly one only
for selected recall failure, under the prospectively published reset rule;
exhaustive quality failure requires diagnosis, not fanout widening.

Packing comparison requires equal logical membership unions. Overlap and
packing are separately priced tradeoffs, not automatic improvement claims.
Corpus/domain comparisons across different shapes are descriptive.
Ordinary whole-collection HNSW references at EF96/256/512 are unprofiled,
in-process context, not causal speedup baselines for profiled TCP/Vamana.
Their command already strictly replays its parent report.

All timed serving and ordinary references run on admitted LAN host
`192.168.0.111`; `192.168.0.185` handles independent read-only verification.
M8 serving uses four three-node groups on the same host: serialized M5 over
loopback TCP and real in-memory HashiCorp Raft read-index/apply, alongside the
router, coordinator, persistent partition-local graph search and result merge.
It is not merely an isolated local graph microbenchmark, but it is also not a
multi-host deployment or a durable/public Raft live-write qualification. The
separate standalone lifecycle test must not be conflated with that Raft path.
No AWS or cloud execution is used. No dedicated benchmark-only host was
supplied: shared-host admission, existing jobs, no-swap cgroup limits and raw
resource receipts remain part of interpretation. Task-owned heavy jobs and
transfers do not overlap the timed serving windows.

## Domain-axis admission failure (D4)

The declared real100K/D4 overlap build is **BUILD_ADMISSION_REJECTED**.
Its conservative router-construction bound is 95,201,280,000 scalar operations,
above both the configured and product maximum of 50,000,000,000. This is not
measured work, an OOM, a recall result or proof that D4 is intrinsically
infeasible. Serving quality and cost for this row are **not measured**.

The original command and all partial build artifacts remain under
`assets/real100k-d4-overlap-multi/`. The process exited 1 after 11m10.22s wall
time (894.74 user seconds, 6.32 system seconds), with maximum RSS
3,607,676 KiB, no swaps and zero cgroup OOM/high/max events. Cgroup peak was
5,254,283,264 bytes. It finished at 2026-09-21T11:12:32.005711964Z.

| D4 receipt (`m3/`) | SHA256 |
| --- | --- |
| `frozen-command.json` | `7aa214f93596eee0630ec475f942c843c190f8b37e2418e1f4e852e86a0f834f` |
| `run.log` | `70ab063c94fb29bbb9480039b6c686b823bcb046e1d167b0100af3b1d33e6f11` |
| `time.txt` | `cf5f23cdb1a179d4c955fdb17510c08d8e3d7cd99f9f5a4a5643d2ce23f25034` |
| `admission.txt` | `2bdc6da9c211afcf46c3540ddab0b669ec532d6d6b7b52ce3850c49ecad86273` |

The planned 24-report successful-profile matrix therefore cannot be described
as complete. D16's counted blocks and unchanged D40 collection remain
independent. Any construction-cap extension requires separate product review
and affected-source/config freeze; it cannot be silently spliced into this
homogeneous frozen matrix. No narrower support envelope is accepted here,
and the all-axis and Raft handoff gates remain open.

The unchanged D40 build succeeds: 80 physical packs, 20,000 useful replicas,
zero filler/unspent budget and zero missing/corrupt/stale assets. Whole-command
wall time is 14m14.20s (1078.52 user seconds, 7.80 system seconds), maximum RSS
3,651,908 KiB, no swaps and zero cgroup memory-limit/OOM events. Derived-build
time is 104.107s, derived physical storage 405,823,141 B and source storage
1,645,787,004 B. These construction results do not establish serving quality.
Its M3 report is
`assets/real100k-d40-overlap-multi/m3/vector_partition_m3_ceff7f34fe53_f406ad74e7e5_f406ad74e7e5.json`,
SHA256 `977f6c772f778db3fb78b0ea0554e5c9d9c550e566265a10c8b0b5e3d3e3c49e`;
command SHA256 is
`041b12f0bf166f944c8e6da08b9b520bea64dea06fcb629a56d7945d7effbd16`.
The independent-copy routing preflight selects P8/D40 (16/80 packs):
4,908/5,120 available hits. P7 has 4,858 hits, below 95%.
Prefix receipt SHA256 is
`7a4e702db537de17c065ee96c006dfbde87d86395dd479aeda91adb88679b9ab`,
logical union
`851f6c4cc03e40c79d4d1290f78efb99b451a95e9d74929b094138d208ff41b2`.
The initial restore was refused before opening the DB at host load 2.06;
`restore-rebind-prelaunch-load-refused/` preserves that admission. The retry
passed the unchanged load-2 limit and verified all non-index bytes unchanged.

The D40 serving report completed all 20 scheduled cells: five internal paired
repetitions at c1/c32, P8 versus P40. Every cell has 512 successes and zero
failures. Actual selected ANN recall is 4,908/5,120 (95.859375%); exhaustive
ANN recall is 100%. All five individual selected/exhaustive ratios meet the
declared quality, QPS and p95 thresholds:

| D40 comparison | QPS ratio range | p95 ratio range |
| --- | --- | --- |
| c1 | 3.841–3.935 | 0.271–0.280 |
| c32 | 3.999–4.121 | 0.226–0.260 |

Independent replay accepts all 20 original rows. The final authenticated
original-matrix reduction below also accepts these comparisons; they do not
discharge D4's missing original domain-axis row.
The original is
`assets/real100k-d40-overlap-multi/m8-series1-block1-p8/vector_partition_m8_f406ad74e7e5_059552ab98fe.json`,
SHA256 `01f03ca212d6ddd1266b61a3f929745110137eaea0d45feee2c633fb809eb775`,
execution `576e08e047a9ee5daadda9bd73e8e5dc`, generated at
2026-09-21T12:08:29.154714077Z. Whole-command wall time is 8m58.65s, user CPU
823.20s, system CPU 18.47s and maximum RSS 2,153,140 KiB, with no swaps or
cgroup limit/OOM events. Resource-receipt SHA256 is
`49e23f0077f85e97ea16bd62adaa27e32dfec135cb581d4b6e81a4049f84d14e`;
canonical command SHA256 is
`b5970802ea1b198a352ac53b5759f5b48591fceb3af0084e3f76c3296e24657b`.
The timings and resource figures retain the same harness-scope distinctions
as the D16 reports.
Independent `replay/run.log` SHA256 is
`eade642411cc8180ceed6a791da2077cc05a61c2e6a4f2a86047b61eac64627b`;
replay command SHA256 is
`af2adaf40a53c188b9652422f801eab56156be731d9b14abd7cfccda3a3e7928`.
Replay exits zero after 7m53.36s with zero swaps, accepting the exact original
report and explicitly not declaring qualification.

The derived-only partial inventory/reducer was adapted **after** the D4 outcome
to authenticate that exact rejection rather than fabricate its report. The
original strict 24-report helper remains retained. The revised consumer admits
only 23 reports plus six specifically pinned D4 failure receipts, retains all
other completeness/identity/chronology/quality checks, and emits 100 actual
comparisons with 20 unmeasured D4 cells and ten unavailable pairs. It cannot
declare all-axis completion, accept a narrower envelope or replace the final
verdict. Missing any other report remains an error. The revision has passed
local and Linux hostile-input checks and independent source review; final
retained-input execution now passes against all 23 available originals and
their successful verification receipts, as recorded above. Linux check log SHA256 is
`3b4814394eef138c5b5b9670615f05185b33f6e8032eea633dcd19593b9b337c`
at `analysis/reducer-r2-host-checks/run.log`. All six copied D4 receipts match
their published hashes and bounded sizes; the original rejection interval is
preserved.
Revised reducer SHA256:
`2c66c1e42f0ed56c4b280fb9f9b00c6806458dbe0186b49424a07c031d4e38f6`;
inventory SHA256:
`0195a38820e67b4c7bcdf98ec2926eecd8f347292ebc7ceccbe03825a56520e8`.

## Retained evidence and corrections

The retained root on both hosts is
`/home/mikers/gomap-4753-final-f406ad74e`; the affinity control has its own
`predecessor/` root. `source` resolves to `candidate/source` on the candidate.
All original failed and screening artifacts remain retained.

* Copied DBs on `.185` require the existing explicit durable-snapshot rebind
  because physical file identity changes. Each `restore-rebind/` receipt
  preserves original metadata and verifies all non-index bytes unchanged.
  Rebound `index.db` is not original producer metadata. `.111` originals are
  never rebound or overwritten by the verification copies.
* An initial serving preparation used incompatible jq NUL splitting. The
  command guard refused it before launch; the directory remains retained.
  Portable `$ARGS.positional` serialization replaced it before measurement.
* Replay pins bind the producer-normalized command, not its outer invocation's
  flag order. A read-only external helper uses the existing producer helpers
  to derive that command without reading the report. The 100K correction was
  made after collection, with the original prepublished invocation unchanged;
  the 250K retained-command pin was published before launch. No producer,
  report, transcript, timing, threshold or measured outcome was rewritten.
* An earlier transfer overlapped the first part of the 250K build. Its actual
  build wall time is retained but is not an isolated build-scaling comparison.
  The serving/reference windows do not have that overlap.
* The first real old-affinity replay was refused before execution at host
  load 2.39; `replay-prelaunch-load-refused/` preserves that admission and
  command. Two existing blocked storage tasks kept the otherwise roughly
  90%-idle verification host above load 2, with over 24 GiB available and no
  current swap traffic. Revised external wrapper `gomap-4753-final-consume-r5.sh`
  admits only untimed `.185` replay up to load 4 with `GOMAXPROCS=2`; its
  16 GiB available-memory requirement and 8 GiB no-swap cgroup are unchanged.
  Wrapper SHA256 is
  `5844b0027040cfaf76388bf9516b491ffd499455741648c957129b350a4ca3b5`.
  The verifier, replay inputs and original report bytes are unchanged.
  Timed `.111` serving/reference commands and admission limits are unchanged.
* Before its first preparation/run, the separate real-lifecycle wrapper was
  similarly revised to admit `.185` load up to 4. It retains four Go threads,
  16 GiB available-memory admission and a 12 GiB no-swap run cgroup. Existing
  blocked storage jobs and their I/O wait remain in the admission receipts;
  lifecycle correctness is not a serving-latency qualification. The original
  unused load-2 wrapper is retained. Revised
  `gomap-4753-real-lifecycle-r2.sh` SHA256 is
  `2f2e9d6af176807cf494228a73c8866ef8ac348bd1d2f5537e8b3f5b2c4caf7d`.

## Correctness and remaining scope

Strict replay must independently establish retained-source consistency,
complete attempts and exact-union ID **and score** parity. Exhaustive-domain
ANN recall is a separate measured quantity, not an exact-search oracle.
Raw `experimental_gate_failures` and each original ledger entry remain
unchanged; an absent cross-variant comparison is not silently relabelled PASS.
In particular, a standalone report leaves `overlap_storage=fail` because the
producer does not perform the disjoint/overlap matrix comparison in that
command. The final paired packet owns the actual durable-storage comparison;
that absence is distinct from a measured storage-limit violation. The disjoint
P5/D16 arms also genuinely miss the historical fourfold probe-reduction gate,
and the raw ledger's QPS/tail gates depend on that reduction. Authorized
widening does not erase those historical misses; selected/exhaustive ratios
under the prospectively declared wider envelope are separate evidence.
`existing_behavior=pending_full_required_suites` is likewise not self-cleared
by replay; relevant exact-head test/review receipts are external evidence.

CPU/allocation profiles include **untimed harness response validation** between
measured cells and the endpoint-loss fault. In particular,
`m8ValidateCoordinatorResponseV1` reconstructs selected membership cardinality
per query after the serving timer stops. In the 100K profile it accounts for
35.29% cumulative sampled CPU and 93.27% of the allocation-profile delta
(46.87 GB of 50.25 GB). Those allocations are not serving B/op, retained RSS or
durable storage. Do not divide the combined profile by the query count and
label the result product-query allocation. The frozen producer is unchanged;
its QPS timer excludes this validation, and its p95 uses successful coordinator
latencies. Profile scope and measured timing scope must remain distinct.
Historical serving-scoped B/op, allocs/op and isolated search CPU remain
**UNMEASURED**: M3 local-search or ordinary in-process reference measurements
cannot fill those differently scoped fields. The separate prospective
collection below now supplies whole-colocated-process CPU/B/op/allocs/op
during serving workers for seven assets, not historical or isolated search
metrics. D4 is outside that seven-asset resource collection. Whole-command
CPU/resource receipts and profiles retain their distinct boundaries.

The [prospective resource/cap completion plan](https://github.com/snissn/gomap/issues/4753#issuecomment-5761106560)
is implemented in merged [PR #4798](https://github.com/snissn/gomap/pull/4798),
merge `3dd18808108a9456cb30e245bfb23f67053323a0`, tree-identical to reviewed
head `59442c790`. Local/Linux/native/race and benchmark checks passed;
49/49 current-head CI checks, clean Codex/CodeRabbit and zero unresolved
threads preceded the 2026-09-21T14:06:33Z merge. It supplies a reviewed
explicit offline router ceiling of 100B distance-coordinates while preserving
the 20B default and historical 50B profiles, plus a separately pinned native
serving-resource companion. After the landed-source freeze below, that
companion observed all seven built assets at selected/exhaustive prefixes
and c1/c32 (28 cells), measuring whole co-located process CPU and allocation
around workers only. It does not reconstruct historical allocations or claim
isolated search resources. Any D4 retry is a separate new-source extension;
the original 95,201,280,000 > 50B rejection and frozen f406 campaign remain
unchanged. No qualification, residual waiver, closure or Raft handoff follows.

The [prospective collection design](https://github.com/snissn/gomap/issues/4753#issuecomment-5761541597)
is frozen before new observations, with plan SHA256
`84bdefcdedc5bdcc3e427eda4b50d096db9b137fc101810339e69821267ce627`.
It fixes block-1 parents and one observation of each of the 28 coordinates,
exact parent repetition-0 IDs/score bits, original full query populations,
warmup64, EF96, budget256, runtime/profiler settings and fresh bounded `.111`
admission. CPU/TotalAlloc/Mallocs deltas cover the co-located process during
serving, not isolated search or historical executions. The separate D4 100B
extension requires a landed-source/binary freeze and prospective prefix/command
freeze before timing; it cannot substitute for the original missing D4 cells.

The [landed completion freeze](https://github.com/snissn/gomap/issues/4753#issuecomment-5761966447)
was published before new collection. Completion root on both LAN hosts is
`/home/mikers/gomap-4753-final-f406ad74e/completion-3dd18808108a9456cb30e245bfb23f67053323a0`.
Clean source tree is `68cdc491b1fc550b8509e633e2b8a9f06ac8eba8`, TreeDB
subtree `c7d8244a0f4445a93d4fdb8eb2a2eb10521bf0d4`, benchmark subtree
`2caa2351442c58456b0e6f17a5cf3c5ee8bd5f0c`. Go1.26.0 linux/amd64 binary
SHA256 is `83fe5815cd0278aad1633cd574b3250d3dc24b1c100ca369e4050383f272b382`,
built on `.111` and copied identically to `.185`.
`frozen-source-and-binary-pins.txt` SHA256 is
`bbeb982ae44734464d0c60a7a168a48c61b9607201361e277805cba6dd803ec1`;
`frozen-observation-plan-pins.txt` SHA256 is
`e74951be9dfa90cc4215260e724e27ebddbfea049870259332223c5f81e67a5e`.
The latter binds all exact commands and independently checked 28-coordinate
expected headers, including lossless uint64 identities. The new D4 build
command SHA256 is `29f53fa2eba2322baeb64709ebecf59075fdc8b902be0339caf721a0743a87d5`.
The D4 extension build completed successfully in 14m06.58s, with maximum RSS
4,016,660 KiB, cgroup peak 5,754,073,088 B, zero swaps and zero memory-limit
events. It realizes D4/60 packs (15 per domain), 120,000 memberships including
20,000 overlap memberships, under the unchanged 7,696,384 B hot-pack ceiling.
M3 report SHA256 is
`bdce56ba800c7c68ada8ef754addc99db1eed5348ee320fb1b30cdfdd721252c`;
original database manifest SHA256 is
`443c9a5d690526029f273aa63d316afe9873a26c5a77d4293306be64dfa429d4`;
run and time receipts are respectively
`d4aaa491b7cae26ed9264c9c02c6997d39e3a80458b07da685887c3cf28f8bc6`
and `289ec8ab0334baa4764e46a0fa27d84bb83784308e509dfba1f2d4f71171eeb8`.
Build success is not serving qualification. The independent copy verified the
original database hashes before index-only restore. All non-index bytes stayed
unchanged. The prefix diagnostic returned [4588, 4992, 5102, 5120]/5120 for
P1..4, selecting P2/30 packs by the predeclared minimum-95% rule. Its result
SHA256 is `8515ab3907c566a69cfd8937e3eba55e2dc62941b967c4f52e6a29acd7bd8efa`.
The reviewed D4 P2/P4 serving wrapper has SHA256
`21f3cd59439ec54729b535a8cde95dfeb46f7a3d989cc3afc829658aa906e7cc`;
frozen command `61d56cb04f5afb38ecf75e23738dae5386967b602021dfab22f757e85b3492b9`.
The unchanged 3dd source/binary completed all 20 D4 cells on `.111` with
exit 0 in 9m59.56s, user/system CPU 929.09s/15.54s and zero swaps or cgroup
memory events. The report records 97.4609375% selected and 99.9609375%
exhaustive recall. Report SHA256 is
`10ae96a08d4e53cf28ac1b4a0b453b6e2f4a873cbbc93d44f017073ff89cb122`;
run log `51d8bd612c88e5b9b404113889bd0ded13e2e79b15267734294ba0633a0e9e14`,
time receipt `fdb9b46d87d28436989665b41c2463704ab8601f8642405ea364e9f274a73a06`.
These producer results were subsequently authenticated by the landed consumer
below; they are not final qualification.

Both strict-consumer refusals remain retained under completion `analysis/`:
`d4-selected-exhaustive` ran before the profile copy completed;
`d4-selected-exhaustive-complete-copy` then rejected the frozen nested source
layout. The shared validator assumes `ROOT/source`, but this completion uses
`ROOT/completion-3dd.../source` with original sibling inputs at `ROOT/inputs`.
The coherent consumer repair in [PR #4800](https://github.com/snissn/gomap/pull/4800)
binds the explicit canonical contained source
while preserving exact head/clean Git, command/executable and all seven pins;
historical campaign layout remains strict. It changes neither producer bytes
nor thresholds. Candidate checks are not a substitute for a landed-consumer
receipt with the unchanged raw pins. The candidate-overlay consumer did pass
the full unchanged D4 comparison: all20 rows replayed, exit0, wall9m20s,
CPU539.31s user +31.61s system, peak RSS1,967,568KiB and zero swaps. Its result
SHA256 is `8cdd7b50402248f6683c543ed63648f66aa2b67cc44914dacb00343e4a631284`.
This remains functional regression evidence; no extra producer run or relaxed
validator was used.

### Landed-source D4 comparison

PR #4800 merged as `8abd7c6af90f5378a6c1fc8e240463cdd9ef287f` with
48/48 Actions checks, the strict TreeDB gate and clean exact-head Codex review.
The [final consumer freeze](https://github.com/snissn/gomap/issues/4753#issuecomment-5764035092)
preceded the once-only independent run on `.185`. Its clean Go1.26.0 binary
SHA256 is `9db684635eef0fc946c6b5d3b03d6b4feb5af12aa028f90e3d5608cf676bed6a`;
source identities SHA256 is
`5b7bdd2bc89e67353dfbb4da5cb7492e2ade3cb4c3fd2e7b1de02826f36d479f`,
and frozen source/binary manifest SHA256 is
`0c269a3d0b9762ddb592b2d08562edb2c58f312af1f4d1377ddf18f4b8aee685`.
The original D4 producer, plan and seven input pins are unchanged.

Under `d4-consumer-8abd7c6af90f5378a6c1fc8e240463cdd9ef287f/reduction/`,
all 20 original rows replayed successfully. All ten paired comparisons pass
matched quality, >=1.15x QPS and no-p95-regression gates: c1 QPS ratios
1.777–1.805, p95 ratios 0.549–0.558; c32 QPS ratios 1.690–1.724, p95 ratios
0.581–0.622. Exit status is 0; verification wall time is 9m19.48s, CPU
538.99s user +31.58s system, maximum RSS 1,965,744 KiB, cgroup peak
1,960,378,368 B, zero swaps and zero memory-limit/OOM events. These are
verification-command resources, not fresh serving measurements.

The copied [d4-extension-comparison.json](d4-extension-comparison.json) has
SHA256 `8cdd7b50402248f6683c543ed63648f66aa2b67cc44914dacb00343e4a631284`,
identical to the earlier candidate reduction. Command SHA256 is
`76dc9e05a11b316cd6c306dccdd00673bc3b216c4126a05f3e8268dbbcc418fa`;
time receipt SHA256 is
`e9ce09d1c48537258b6f8802ca03975c1a3e55d5d07163a364fc893c065de4af`.
The run log is empty (SHA256
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`).
Its status remains `COMPARISON_REDUCED_NOT_QUALIFICATION`; this separate
extension does not fill or relabel the original rejected D4 coordinates.

No product
search/geometry change beyond the explicit cap is hidden in this source:
the other TreeDB changes since f406 are tests/docs, and harness changes add
the separately identified observer. Original producer identities stay intact.

The first prospective resource observation failed **before any measured cell**
with `nativewire: M8 missing digest for benchmark-group-000001`. It produced
only a header, no completion footer, and exited1. The other six observations
did not run. The collector used the raw retained-asset opener rather than the
existing validated serving opener, so it lacked runtime placement relabeling
and group digests. Its failed JSONL/log/time/exit hashes are retained in the
[failure and prospective ordering receipt](https://github.com/snissn/gomap/issues/4753#issuecomment-5762551183).
The repair in [PR #4799](https://github.com/snissn/gomap/pull/4799), candidate
`cad3620b3312df0f20c0eb7c478ab4066ef439da`, reuses the producer's multi-group
loader and adds a retained-M3 native-topology regression. Red/green, focused
race, vet/build and independent review pass. The original failing 100K asset
passes a full four-cell CLI rehearsal with exact parent IDs/score bits and
validated receipt/footer; this is nonqualifying functional regression evidence.
Its receipt SHA256 is
`0e51a90d5a225ce43c9a74e58758d060bbe0a50b3faed3a0536306d74adc3a10`.
The repair merged as `d0af4f73142bd27f08acedbd4545e750b136dc6b` after49/49
passing current-head checks, clean exact-head Codex review and zero unresolved
threads. Tree `d69aad6ab37c5d3332c8ec9cf7f0fd8b08c183da` matches the reviewed
candidate. One unrelated Windows nanosecond-resolution assertion failed its
first run and passed its targeted retry; no candidate change was needed.

The [repaired collector freeze](https://github.com/snissn/gomap/issues/4753#issuecomment-5763302040)
preceded all new timing under
`completion-d0af4f73142bd27f08acedbd4545e750b136dc6b/`. TreeDB subtree is
`a6ba6fb39fd22a35f00a0ad1b89cd3c8f54206c9`, benchmark subtree
`2d3681902f2827dd657d9e62a76aa95d5afdd6e0`, collector blob
`86ef56d68d465c09caa87bba5c617146ee3bafa3`, production-assets blob
`b985d6dd29c8191b6f0ba38ef924a3d242fa15dd`, and production-runner blob
`07e3bdb6872526e0e49c33eef2f921a07f3e969f`. Clean Go1.26.0 binary SHA256 is
`935da22bbe101ad466a0726e40ff064da174a557a7f22a7b2aa8f660d677a02f`.
`frozen-source-and-binary-pins.txt` SHA256 is
`8153466dbb57c3b929c934be79796673b80fe9fb1a7299d3838496ba8d0c9e3b`;
the all-seven command/header/source manifest `frozen-resource-series-pins.txt`
has SHA256 `66814bd99ace35d8f682e0b2b583790179d107d4501de164cd0d8378341ff99a`.
Collection followed the original fixed asset order with fresh admission and
fresh paths. The D4 producer remains separately frozen at3dd. All seven
observations and their independent copied-artifact validation completed.

The first five observations completed with exit0, zero swaps and zero cgroup
memory-limit/OOM events. At 16:12:43Z the next asset's admission saw one-minute
load2.36 above the frozen2.0 limit and stopped before creating a collector log,
receipt or profiles. Its rejected `admission-*/host.txt` remains retained.
After load fell to0.16, the untouched D16-overlap command resumed with the
same frozen source, binary, command, population and limits. This is a host
admission retry, not a measured-cell retry or selection among timing results.

### Completed prospective serving resources (28 validated cells)

The complete observed-file manifest was [published before independent
validation](https://github.com/snissn/gomap/issues/4753#issuecomment-5763991815):
`observed-resource-files-pins.txt` SHA256
`5f647559404e9ac5c7e3f15ad32910042f1e7504eba0f7e1dae284465d430b79`.
The copy on `.185` verifies every recorded file and the frozen command,
header, source/binary and reader manifests. All seven read-only consumers
exit zero, authenticate the original parent pins and independent replay
receipts, and accept exactly four cells each through the landed strict reader.
No query is re-served by that consumer. Full derived values, raw receipt and
parent hashes are in [resource-observations.json](resource-observations.json).

This is **one prospective observation per coordinate**, not repeated evidence
of resource-improvement significance. Every declared attempt succeeds: 1,000
per structured cell and 512 per real cell. Original repetition-0 output IDs
and score bits match. CPU and allocation deltas include the whole colocated
client/coordinator, TCP shards, background work and Go runtime/GC during the
bounded workers. They exclude setup, warmup, preallocated outcome slots,
post-response validation, attribution and receipt encoding. CPU includes the
final memory snapshot overhead; worker wall excludes both snapshots. There
is no forced GC or idle subtraction. Standard M8 profiling remains enabled.
The scope's internal Raft/background processes do not grant public/Raft
qualification. All seven collectors exit zero with zero swaps and cgroup
memory-limit/OOM events; launch receipts retain the 8 GiB/no-swap envelope.

Selected-arm figures below are **c1 / c32**. CPU is CPU-ms per attempted query,
not latency; B/op and allocs/op use every attempted query as denominator.

| Profile | Selected domains / packs | CPU-ms/op | B/op | allocs/op |
| --- | --- | --- | --- | --- |
| Structured100K | 1 / 1 | 0.284 / 0.341 | 41,315 / 66,964 | 415.7 / 351.6 |
| Structured250K | 1 / 1 | 0.334 / 0.642 | 42,214 / 72,256 | 417.9 / 355.2 |
| Old-affinity real100K | 5 / 5 | 3.597 / 10.572 | 240,431 / 315,627 | 1071.7 / 963.2 |
| Candidate real100K one-pack | 5 / 5 | 3.670 / 10.698 | 235,953 / 317,145 | 1067.6 / 959.1 |
| Candidate real100K disjoint multi-pack | 5 / 20 | 9.775 / 22.730 | 443,002 / 557,579 | 1612.3 / 1510.6 |
| Candidate real100K overlap D16 | 4 / 16 | 8.104 / 19.633 | 431,029 / 562,212 | 1490.8 / 1393.5 |
| Candidate real100K overlap D40 | 8 / 16 | 7.163 / 17.100 | 328,326 / 471,972 | 1464.3 / 1383.3 |

On this observation, the equal-union disjoint multi-pack/one-pack CPU ratios
are 2.663/2.125 and allocation-byte ratios 1.878/1.758. These corroborate the
physical-work diagnosis; they do not replace the original five-block timing
rejection, establish causality for a new mechanism, or justify a search reset.
The original combined allocation profiles are still not serving B/op.

Independent reader logs under `analysis/resource-reader/<asset>/run.log`:

| Asset | SHA256 |
| --- | --- |
| `structured100k-d16-one` | `1bf08420ac06949806209d466f6a1152b03c135c247a96214ffa1da0b48dc54c` |
| `structured250k-d16-one` | `304d5f53431b29eec0720c545af8ea079de345c4ac8871e914dbe61446dd8695` |
| `predecessor-real100k-d16-disjoint-one` | `4ce1d6a55464f6c07d35ea20aff770790ba04e1ae077d989012360b4fc0a1d79` |
| `real100k-d16-disjoint-one` | `110b7fd778b5f302414a82a61a51c23a6b5488518b4ff0b30f2f52d208a268f5` |
| `real100k-d16-disjoint-multi` | `05e802f0433e4ec3f53622d26c7a23476e47844a7101a058c1178ac1fafada7f` |
| `real100k-d16-overlap-multi` | `367b0f2e9be1d7ae70a3790d8581aa28b9ba3481d5ad79d20a4da5cf14e4823b` |
| `real100k-d40-overlap-multi` | `34a44badcdba3695e3850cb3cc9a173eb13a11efe8b5f7936684003f5ef03140` |

Before any D4 timing, the same receipt prospectively moved D4 serving ahead
of the blocked resource series. `.111` remains isolated from task-owned heavy
work; repair validation runs on `.185`. Prefix, commands, product, populations,
measurement settings and caps did not change. This operational amendment is
not timing-based selection and does not fill the original missing D4 cells.

The selected standalone lifecycle gate from #4793 proves its stated 512x768
procedural component scope: native TCP mutation/search, revision-bound
concurrent freshness, cold reload, reopen, active-generation preservation and
checkpoint-backed durable-ack crash recovery. It uses test read proofs, not
public/Raft live-read authority. Active-generation GC is not immutable fold
or retired-generation reclamation. The separate real-data run below now
exercises full administrative replacement and reclamation; neither component
result establishes public/Raft live-write qualification or final scaling.

[PR #4795](https://github.com/snissn/gomap/pull/4795) extends that same component
driver to a pinned real-data mutable copy and an explicit quiesced full-source,
partition and router replacement followed by pin-fenced reclamation. It is
merged apparatus at `b5a6ebc48811b9e001092a74f9682bf361cc0816`; that merge alone
was not a real lifecycle result. Final head `7b087c6769d39297bba4593168c26f9a19a17245`
passed all 49 checks, exact-head Codex review and the resolved CodeRabbit review.
The frozen M8 sources above
remain unchanged; the lifecycle receipts below identify their own
reviewed/landed source and copied-input provenance.

The first real run stopped at admission, before any lifecycle measurement:
the test compared the manifest's physical-pack capacity (1875) with the
logical-domain capacity (7500). The frozen original remained byte-identical.
The rejected run is retained at
`real-lifecycle-b5a6ebc48811b9e001092a74f9682bf361cc0816-p4/`, with run-log
SHA256 `c4464ce03cc0d4076969accda0c19831a498d692f5089b9d7ec3b49f4d86db96`
and input SHA256
`e385df827a06d62df068a0fe2128b9a49818533566037b89a5f0f0b758265718`.
[PR #4796](https://github.com/snissn/gomap/pull/4796) repairs that unit check
and enforces M3's actual encoded-pack byte envelope before replacement
publication. It does not change product or serving bytes. A fresh independent
copy and reviewed, landed lifecycle source are required for the next attempt.
It merged at 2026-09-21T11:45:11Z as
`2d1b3f4b98ae8ae1178a0f1dd8e322db33dee9f4`, tree
`1f1d4ea7b8f537209e237f100778a572382c9a34`, identical to reviewed head
`c64b9247ce8d7309689db72481b40d83a882fe74`. All 49 checks passed, Codex and
CodeRabbit were clean, with zero review threads.

The fresh landed-source run passed its initial 512-query native selected-search
phase with 4,927/5,120 truth hits, then failed its first native `InsertBatch`
with remote error 17 (internal error). No mutation, recovery or replacement
success follows. Preserve the full failed mutable copy at
`real-lifecycle-2d1b3f4b98ae8ae1178a0f1dd8e322db33dee9f4-p4/`.
Its run-log SHA256 is
`80be19af420ea85702dab7136fad16cb89cd834e96a6b87d919a00a3657e00e9`;
initial receipt SHA256 is
`0638dd841273bdbafd968641c2b4377a63c731e3b36ad0c8b65a9b5b1222c42d`.
Exit 1 was retained at 2026-09-21T11:54:14Z after 3m23.08s, user CPU 218.77s,
maximum RSS 2,403,936 KiB, zero swaps and zero cgroup limit/OOM events.
The original asset was verified byte-identical on exit (verification receipt
SHA256 `b03335f1c69afb73a977091191f84aa3ea82db9555e7f66afe1ac12787fdf79e`).
The mutation and later recovery/replacement compatibility audit is complete;
the failed copy will not be reused for a qualification attempt.

The concrete cause is the test document schema: M3 requires non-null
`time_us:int64` as a sort column, but lifecycle insert and replacement JSON
omitted it. The column extractor rejects that document before mutation; the
native error mapping masks the inner error as code 17.
[PR #4797](https://github.com/snissn/gomap/pull/4797), reviewed head
`000f104c2482f81b18bceb55613b3324c6e304c6`, repairs the common encoder and
exercises M3's sorted-column/non-column-payload schema through the entire
procedural lifecycle. A small regression reproduces the rejection and checks
native insert/replace/reconstructed reads. Focused Linux lifecycle, race and
vet pass. The initial full suite failed only its unchanged Unix socket test
because the external TMPDIR exceeded the socket path limit; the short-TMPDIR
targeted check passes and the original failure remains retained. A focused
typed-storage naming-inventory repair is included at the current head.
It merged as `5977864b8d7539e5f2da52f93ccdeb8a9f132137` at
2026-09-21T12:46:38Z with all 49 current-head checks passing, clean Codex review
and zero review threads. The merged tree
`a2784aa87ceb28b80d22a2215fbb4855e2bbaf59` is identical to the reviewed tree.
No product source or frozen serving evidence changes. A new independent
real-data lifecycle copy began its once-only run at 2026-09-21T12:51:17Z.
[Prospective source/input/environment freeze](https://github.com/snissn/gomap/issues/4753#issuecomment-5760769659)
pins its input, executable, command and complete provenance. Existing native
request/error logging is enabled only for this correctness run; its timings
also include overlap with one bounded replay on `.185`. Neither those timings
nor the apparatus merge establish isolated serving performance.

### Completed real lifecycle (standalone component correctness)

The fresh run at
`real-lifecycle-5977864b8d7539e5f2da52f93ccdeb8a9f132137-p4/` on `.185`
completed the full test with **PASS**, process exit 0 and wall time 24m41.55s.
Its clean Linux/amd64 Go 1.26.0 binary identifies landed source
`5977864b8d7539e5f2da52f93ccdeb8a9f132137`; executable SHA256 is
`422ec9b3f370ecebb58df41afaac1e39c1830b308c781b0801cd19d010e16cf7`.
The mutable copy uses the pinned real100K x768, D16/64-pack overlap asset at P4.

Initial, insert/delete/move, metadata-only, after-write, cold-source,
checkpoint/reopen, active-generation-after-GC and durable-ack crash/reopen
phases each returned 4,927/5,120 truth hits (96.230469%). Query-only and
concurrent-write populations each retained 4,096 attempts and 39,416/40,960
hits. The concurrent phase records eight writes, 258 overlapping queries and
nine revisions. Full quiesced source/partition/router replacement and the
subsequent new-generation-after-reclaim-reopen phase each returned
4,900/5,120 hits (95.703125%): lower than the old generation but still above
the unchanged 95% floor. The reclaim receipt records seven deleted segments,
767,303,126 bytes; the follow-up reclaim deletes none. This is explicit full
administrative replacement with pin-fenced reclamation, not an online or
incremental fold claim.

All 16 raw receipts remain under `receipts/`; their filenames, sizes and
SHA256 values are recorded in the authenticated run log. Key receipts are:

| Lifecycle receipt | SHA256 |
| --- | --- |
| `run.log` | `acba15019e7f668f0971a207d704fa3eb47471befa5b1eab6a5c493275526fb4` |
| `time.txt` | `314fb3d438a5764c7339f0fa83cda9d0ec868ef0b51b69e2b8cbb7625d0750ac` |
| `input.json` | `00a622c2147523ee6be8fc0c2e892d2f4a6ae20c9bfd0d683edc52f5f6d930fd` |
| `command.json` | `152535d546b9019412eed41e35970eb76825e63c76a7201e85155a06d283e61d` |
| `receipts/new-generation-after-reclaim-reopen-3697732642.json` | `d1a17a60fe5fba47ac56dc85a13059eb08f9397e0e753c7e7bbf634ab24532d0` |
| `original-after-verified.txt` | `b03335f1c69afb73a977091191f84aa3ea82db9555e7f66afe1ac12787fdf79e` |

The original asset verifies byte-identical after exit. Maximum RSS was
4,740,676 KiB and cgroup peak 5,372,145,664 B, with zero swaps and zero cgroup
memory events. User/system CPU was 1777.68s/29.68s. Native debug logging and
overlap with one bounded replay were enabled, so these are correctness-command
resource/timing context, not isolated performance measurements. This passes
the declared real standalone component lifecycle only: test read proofs are
not public/Raft authority, and no final scaling verdict or handoff is implied.
Both earlier failed runs and their mutable copies remain retained unchanged.

Earlier, from 11:52:46Z, the prior lifecycle attempt overlapped one strict replay on
`.185`, prospectively admitted at load at most 4 and at least 20 GiB available
memory, with aggregate limits of six Go threads and 20 GiB without swap.
These correctness-command resource figures are not isolated performance data.
Timed serving on `.111` remained separate. The overlapping block-2 overlap
replay subsequently accepted all four original rows (report SHA256
`ab2088fc2e3ae39cd61fc1d0571a17dd3f8d55d3e544618f9fd3829c90d54d18`).

Historical high-entropy membership infeasibility, real P2 rejection, rejected
HNSW and early Vamana candidates, compatibility controls and fourfold misses
remain limitations/evidence. Neither authorized widening nor later passing
profiles erases them.
