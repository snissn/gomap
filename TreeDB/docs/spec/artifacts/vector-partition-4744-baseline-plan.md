# #4744 baseline preregistration

Status: **planned, no measurement**. Owner: the #4743 coordinator; empirical
acceptance remains under #4744 after the #4755 source packet lands. This is a
bounded extension of the existing M3/M8 harness, not a qualification result or
a new campaign schema. #4745 stays blocked until the baseline and diagnosis are
accepted. #4753 alone owns the final representative-scaling verdict.

## Data and unopened query windows

Use canonical cosine FP32 scores, stable document-ID ties, and top-k 10.
Keep these existing procedural corpora, not a newly substituted easier corpus:

| Profile | Generator | Vectors | Dimensions | Seed |
| --- | --- | ---: | ---: | ---: |
| Structured 100K | `treedb_vector_partition_embedding_mixture_v1` | 100000 | 128 | 4017 |
| Primary structured 250K | `treedb_vector_partition_embedding_mixture_v1` | 250000 | 128 | 4016 |
| Additional high-entropy 100K | `treedb_vector_partition_high_entropy_synthetic_v1` | 100000 | 128 | 4015 |

The additional profile is a feasible bounded corpus, not evidence about the historical 1M
high-entropy profile or licensed real embeddings.

Each profile has 512 calibration queries starting at qualification query ordinal
1000000 and 512 evaluation queries starting at ordinal 2000000. These are query
counts, **not** 512-vector smoke corpora. Corpus generation is unchanged by the
query offset. Old qualification ordinals 0..999 remain opened regression data;
neither an old query split nor an old truth cache is a fresh holdout. New windows
are also beyond the legacy generator's maximum one-million-query ordinal range.
Retain distinct manifests, generated checksums, query-byte digests and canonical
truth-cache identities for each window. Verify actual corpus byte/ID equality
across windows before comparing them. A shifted checksum must never bypass an
existing M3 descriptor's fixture binding.

Calibration owns diagnosis and coordinate selection. Freeze the #4745 policy,
width and coordinate decision before opening evaluation query-level results.
Record evaluation exposure in the acceptance receipt; once opened, a window
cannot support further tuning. Baseline acceptance can use calibration diagnosis
and a sealed evaluation identity; it cannot claim an unseen evaluation result.

## Source/model freeze and execution order

1. Land the complete reviewed #4755 instrumentation/provenance/replay/docs
   packet before expensive representative collection.
2. In a clean standalone checkout, pin the landed commit, source tree, all
   TreeDB/vector-partition runtime and benchmark-harness blobs, module/dependency
   identities (this repository has no tracked vendor tree), Go toolchain,
   executable SHA-256 and clean embedded VCS revision. Publish those
   exact identities and fully expanded commands in #4744 **before collection**.
3. Recover the pinned existing KaHIP adapter/interpreter/backend, then construct
   current graph assets through the existing M3 graph build. Freeze source-vector,
   membership, generation, model, representative allocation, local-graph,
   descriptor and truth digests before treatment. The default development
   round-robin fixture builder is not a graph-partition baseline. A missing
backend or mismatched descriptor is a recovery failure, not permission to
substitute round-robin or weaken validation.
4. Collect serially, replay independently from retained regular artifacts, and
   accept the causal diagnosis under #4744. A matching hash or producer receipt
   alone does not authenticate the underlying attribution.

Unrelated later main commits need not invalidate evidence: compare the frozen
runtime/harness subtree/blob identities. Any relevant runtime, harness, source,
model, truth, build-option or protocol change requires a new freeze before the
affected collection; never relabel old evidence as the new candidate.

Backend recovery was checked on .111 with the unchanged adapter SHA-256
`ae4ca8f5f26bd510a507a0f4ba50adaf1e5514ee9e20340cb9d494aba8f54825`,
KaHIP 3.25 wheel SHA-256
`e6ea76524e9fc01b27e6f5c5f00b7eec71c94cbd1e84678ce2a14d64dfc9eda4`
and installed RECORD SHA-256
`7ff011253147286fcebc9185573662bf31dbcfbab1944f9b4940032f49ea5217`.
The task-local Python 3.10.12 executable SHA-256 is
`a2f33a6e006989270f4340528eb61f8f97366e00a5d1b602ac8672ea44fc56ae`;
it is not the historical interpreter identity. Freeze its task-owned prefix,
`PYTHONHOME`, `PYTHONPATH` and `PYTHONNOUSERSITE=1` in the expanded build
commands. A 16-node adapter smoke passed; this is infrastructure validation,
not representative graph-quality or performance evidence.

## Bounded coordinates and quality gates

The initial diagnosis uses current graph/useful-only overlap .20, 16 logical
domains, actual complete domain-to-pack expansion, ordinary min-distance
ranking and the current default representative/model configuration. Freeze the
actual M3 options and resulting model bytes, not merely a label. Do not restore
historical filler replicas or tune local graphs to make this baseline pass.
Graph-disjoint/stable-hash controls remain available in the existing harness;
their broader controlled experiment is owned by #4175, not silently required
for every instrumentation change here.

Freeze the ladder before treatment: logical probes 1,2,4,8,16; returned router
width 256; local ef_search 64,96,128; concurrency 1; warmup 1. Execute one
retained variant per process, with quality diagnostics enabled and quality trace
queries 0. The conservative trace envelope does not admit 16-pack sampling under
the current 200-million-unit limit; unsampled scored/retained stages stay
**unavailable**, not inferred. Independent observed available/returned and
coordinator masks still participate in the diagnosis.

Report both 90% and 95% mean recall@10 operating points; the primary is structured
250K at 95%, probes at most 2 and ef_search at most 96. Missing that point is an
explicit failed baseline, not grounds to replace it with 90% or interpolate an
unmeasured coordinate. EF128 and all-domain rows are diagnostic controls, not
substitutes for the primary target. Attribute membership concentration,
nearest-member objective, coarsening, retrieval and local/merge loss separately;
preserve signed gained/lost masks and every refusal in the original population.

The scaling axes are logical domains 4,16,40 and physical pack fanout measured
from the actual binding. The current baseline builder may couple packs and
domains: label coupled measurements as such. An independent pack-only expansion
at fixed membership/model requires a real supported retained asset construction
and parity proof before it is measured; until then it is unavailable, not an
invented independent scaling result. #4753 must not accept a final independent
scaling claim without that proof. Domain-cost and pack-cost oracle curves are
independent reporting axes, not joint feasibility. Every selected real route
reports and checks both actual counts; no optional joint optimizer is required
unless a later accepted contract demands it.

For the #4745 same-candidate decision, a promising coordinate requires at least
one percentage point absolute calibration available-truth coverage gain over
distance-only at the same actual domain **and pack** cost, without a greater
than 0.2-percentage-point loss on the other frozen structured profile at that
coordinate. Report all per-query gained/lost truth, not just the mean. Holdout
confirmation uses that frozen coordinate unchanged. A smaller gain, failure,
cost tradeoff or no-win remains an honest named-coordinate no-promote result;
this rule does not authorize a public routing policy or imply end-to-end recall.

## Host, timers, limits and noise

No AWS or cloud execution. Preferred LAN runner is .111 (Ryzen 7 9700X,
16 logical CPUs, approximately 29 GiB RAM), Go 1.26.0 linux/amd64,
GOWORK=off, GOTOOLCHAIN=local, GOMAXPROCS=2, GOMEMLIMIT=4GiB. Every collection
rechecks foreign jobs, ports, memory, disk, current swap activity and CPU load;
the box is not dedicated. Use only task-owned roots and temporary directories.
Do not stop or clean another owner's jobs, files, caches or services. .185 is
currently a read-only historical-input recovery source because its fast mount
is full. A runner substitution requires a new host freeze and matched local
baseline, never transplantation of historical absolute QPS.

Retain exact durability/backend/cache settings. Warm asset owners are reused
through the existing path; fixture construction, truth, M3 build, oracle DP,
exhaustive attribution and diagnostic reducers are outside serving QPS timers.
M8 timed loopback/coordinator observations are not Python/document/local-only
QPS. Report build/reopen/cold cost separately. Retain CPU, allocations, heap,
block, mutex and trace profiles under the existing documented filenames.

Admission limits: vectors at most 250000 for the primary profile, dimensions
128, top-k 10, fixture memory at most 4 GiB, peak process RSS at most 4 GiB,
derived persistent assets at most 2 GiB, diagnostic work at most 200000000
units, and at least 20 GiB free disk. Freeze these existing per-profile caps:

| Vectors | Graph distance work | Router scalar work | Router vectors | M3 visits | M8 exact-truth visits |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 100000 | 20000000000 | 20000000000 | 120000 | 400000000 | 600000000 |
| 250000 | 50000000000 | 50000000000 | 300000 | 900000000 | 1500000000 |

Canonical truth construction/generation is separately limited to
`vectors * 512` exact visits per window. Verify the complete existing planner
before each expanded command. An infeasible cap produces a named blocked coordinate;
do not shrink the corpus, discard failures or increase limits after seeing a
treatment. Retain ordinary-path B/op, allocs/op, peak/live memory and relevant
domain counters when shared code changes; setup-only query offsets cannot be
reported as a serving speedup.

Performance comparisons use five balanced paired same-host blocks with identical
commands/inputs/timers for equivalent behavior. Report median, full range and
paired ratios for QPS/p95, allocations, RSS and actual work/fanout. A >5% adverse
median change or extra ordinary-path allocation is blocking until profiled,
eliminated or explicitly accepted with a correctness rationale. A claimed
meaningful throughput improvement requires at least 10% median gain and every
paired ratio on the favorable side of 1; a >10% within-variant spread invalidates
the performance claim and requires a quiet-window rerun. Coverage decisions do
not become QPS claims. Failed/incomplete/refused requests remain visible; never
compute a success-only workload mean.

## Acceptance receipt

Retained replay takes only the existing M8 report, with independently recorded
SHA-256 pins for report bytes, canonical JSON fixture manifest, canonical JSON
argv, executable, retained M3 descriptor, truth-cache artifact and semantic truth.
Fixture/argv/build/model/truth pins are frozen before measurement; report bytes
are pinned only after foreground publication. A pin copied from the same report
is not an independent protocol anchor. Replay must reopen real assets/profiles,
verify clean embedded/source VCS, and reconstruct attribution/transcript parity;
its acceptance is not a qualification verdict. Historical campaign whitelist and
anchors remain unchanged.

Before closing #4744 retain: this frozen plan identity, expanded commands and
source/model/data/query/truth identities; fresh runner preflight; complete
100K/250K and feasible additional-profile baseline receipts; strict retained
replay results; profiles/resource/work ledgers; primary 90%/95% disposition;
causal diagnosis and the measured handoff to the existing #4743 child. Missing
artifacts or unavailable axes are named limitations, not a passing scaling gate.
An actual admission/merge defect requires one narrow correctness repair before
tuning. #4745 cannot activate on source landing alone. No outcome here releases
Raft or completes #4743/#4753.
