# MongoDB versus provisional repaired GoMap: retained October 30 evidence

The retained public Mongo BSON comparisons show lower GoMap throughput than
MongoDB in all nine complete pairs. They were executed on provisional GoMap
`802f7cfcf8b69eff1447a5f4833076d428e61e3f`, before final predecessor integration.
They are not measurements of the later maintained harness/integration head, a
sustained-throughput claim, or an isolated COW comparison. [#5335](https://github.com/snissn/gomap/issues/5335)
remains open pending its final-source, review, delivery and acceptance gates.

This maintained packet preserves the results without another benchmark attempt.
[Raw phase evidence](ycsb_2026_10_30_evidence/raw-phase-evidence.tar.gz),
[original-path and SHA256 index](ycsb_2026_10_30_evidence/index.json),
[all operation latency rows](ycsb_2026_10_30_evidence/latency.md),
[common-load measurements and lifecycle timings](ycsb_2026_10_30_evidence/common-load-data.json),
and [strict/relaxed measurements](ycsb_2026_10_30_evidence/strict-and-relaxed-results.json)
are checked in with this report. The archive contains original phase stdout,
stderr, checked Close and launch receipts, manifests and available runtime/lifecycle
summaries. Its index binds every member to its original bytes. Large typed
snapshots, databases, binaries and complete supporting service observations stay
at the retained paths indexed by the original manifests. This selective packet
is not a self-contained archive of all lifecycle evidence; durable access to those
remaining artifacts is an explicit delivery obligation.

## Workload, identities and acknowledgment boundaries

Host185 (192.168.0.185), 2026-10-30 UTC. Each arm loaded a fresh 100,000-record
database, then requested 10,000 operations with 16 workers, uniform keys,
95:5 READ:UPDATE, hashed insert order, ten binary 100-byte fields, all-field reads
and single-field updates. The requested proportions are not exact operation
counts; per-sample actual counts are in the latency appendix. Complete phases
report 100,000 successful inserts and 10,000 successful timed operations with
zero operation errors. Full typed census warmed the cache; there was no separate
warmup. Three independent fixtures per class used order M1→G1→G2→M2→M3→G3.
No retries selected the published complete samples.

Both arms used the same public Mongo driver client: merged `snissn/go-ycsb`
producer `3c8148495b8d4490cf0ec30e9eb92d14a7a71bac`, Go 1.26.10,
ELF SHA256 `2237d8aeec309b9b4b335b348234d9829594c9101043144a944d9f2868fbc95b`,
without a native protocol overlay. Build receipts and retained fake-peer
insert/update frames bind the actual request selection; URI text alone does not
prove server durability. Original DB/workload Close is checked after the original
TOTAL timer. Nativewire, no-WAL and historical GoMap cells are separate evidence.

GoMap source tree was `d6d8abe929c5ef001bb626a8226b6ac940184b60`, gateway ELF
SHA256 `d3475bdfc5796890c39b2c2bf2c9c27519ed94283939aa624f05ed1481233fe8`.
MongoDB was standalone WiredTiger 8.2.7, repository image digest
`mongo@sha256:7abfba0d07c9330373f8173981ea4d09cd8a82cdf0e86ccaf7008848d1d24f62`,
local image ID `sha256:690f1371c118a8389ecd6c82c9a7f0e37320732b1d56935a24b29dfca6933f54`,
mongod ELF SHA256 `d767fb752139fbc8e0e667d7e0c6fe7cdf690e0aa94a51502a32bad8c54a3672`,
reported GitOrigin `8f6785264a643d2eeb5e4046f7876a23e40e1a09`.

| Class | Setup load | Timed MongoDB request | Timed GoMap request / profile |
|---|---|---|---|
| Relaxed | w:1, j omitted | w:1, j omitted | w:1, j omitted / command_wal_relaxed |
| A: same timed request | w:1, j omitted; complete census then same-owner public w:1,j:true sentinel | w:1,j:true | w:1,j:true / command_wal_durable |
| B: scoped durable ACK | Same common setup and sentinel as A | w:1,j:true | w:1, j omitted / command_wal_durable |

The sentinel is outside TOTAL. Common setup changes the ingestion contract from
the failed strict-j:true experiment below. B has different timed requests and
separately reviewed ordinary durable-profile ACK/WAL replay controls. Neither A
nor B implies full MongoDB durability equivalence. Omitted j is not j:false;
standalone getDefaultRWConcern returned code 51300, so its actual default is not
inferred. The relaxed mechanisms differ and carry no equal crash-durability claim.

## Every complete pair

Ratios use paired printed TOTAL rates; percentage change is `(GoMap/MongoDB−1)×100`.
Per-phase printed workload durations differ from TOTAL's internal timing boundary;
do not reconstruct either by dividing counts by rates.

| Class/pair | Mongo load ops/s | GoMap load ops/s | Mongo run ops/s | GoMap run ops/s | G/M | Change |
|---|---:|---:|---:|---:|---:|---:|
| Relaxed/1 | 37,996.0 | 16,612.9 | 40,959.1 | 12,939.6 | 0.3159 | −68.41% |
| Relaxed/2 | 37,566.9 | 16,500.8 | 39,824.8 | 12,838.4 | 0.3224 | −67.76% |
| Relaxed/3 | 39,152.4 | 15,974.6 | 38,983.3 | 13,049.2 | 0.3347 | −66.53% |
| A/1 | 39,856.7 | 2,178.5 | 21,890.1 | 225.5 | 0.010301 | −98.97% |
| A/2 | 39,517.4 | 2,177.5 | 17,926.7 | 222.2 | 0.012395 | −98.76% |
| A/3 | 37,706.3 | 2,196.0 | 20,375.7 | 228.4 | 0.011209 | −98.88% |
| B/1 | 39,536.4 | 2,207.7 | 21,601.3 | 11,436.8 | 0.529450 | −47.06% |
| B/2 | 38,639.1 | 2,197.7 | 22,917.0 | 11,453.2 | 0.499769 | −50.02% |
| B/3 | 37,474.5 | 2,207.2 | 22,025.3 | 10,705.2 | 0.486041 | −51.40% |

Relaxed median arm rates are MongoDB 39,824.8 and GoMap 12,939.6 ops/s.
A medians are 20,375.7 and 225.5; B medians are 22,025.3 and 11,436.8.
Median paired ratios are respectively 0.3224, 0.011209 and 0.499769;
a ratio of arm medians need not equal the median paired ratio. Population run-rate
CV for A is MongoDB 8.139% / GoMap 1.124%; B is 2.472% / 3.115%.
Relaxed population run-rate CV is MongoDB 2.028% / GoMap 0.665%.
Three repeats provide limited uncertainty information, not a confidence interval.

A GoMap UPDATE p99 ranges 1,652,735–2,244,607 µs, versus MongoDB
12,351–26,335 µs. B GoMap UPDATE p99 is 14,343–18,255 µs, versus MongoDB
12,007–12,719 µs, while GoMap READ p99 is 2,051–2,545 µs versus
568–602 µs. These are each sample's operation quantiles, never pooled tails.
The [latency appendix](ycsb_2026_10_30_evidence/latency.md) includes every load/run
operation's count, average, p50, p95, p99 and max, including relaxed and strict Mongo.

## Failed strict-journaled load and other attempts

The earlier strict experiment requested w:1,j:true during every load insert.
MongoDB completed at 851.2 load ops/s and 20,611.4 timed ops/s with zero errors.
GoMap hit its 600-second deadline after 6,735 successful inserts and 16
cancellation-era errors, with no timed GoMap run. There is no paired rate or ratio.
The partial rate is diagnostic. Its [exact compressed failure disposition](ycsb_2026_10_30_evidence/strict-load-failure.json.gz)
and original phase output/receipts are retained; A/B do not replace this failure.

Earlier setup, observer, compile-limit and offline-test failures remain at their
original paths in the source crosswalk and secondary ledger below. They do not
become product passes. The never-launched 1,200-second native JSON continuation
is not an attempt. No further sample or comparator restart is implied here.

## Close, maintenance, correctness and resource scope

[Common-load report](ycsb_2026_10_30_evidence/common-load-report.md) tabulates every
load/run printed duration, sentinel interval, original load/run DB Close duration,
loaded census and preclose/reopen snapshot interval. The JSON appendix also retains
original workload Close nanoseconds and each maintenance helper interval. Original
TOTAL excludes Close. Helper launch wall intervals include connection/launch work;
they are not isolated server barrier cost. Explicit post-run maintenance is outside
TOTAL; background maintenance has not been exhaustively partitioned.

MongoDB uses public fsync, observed orderly shutdown and same-volume reopen with
exact typed readback. Its receipt checks numeric shutdown markers 4795901 then
8423404, process termination and removal; no direct MongoDB Close return is
observed. GoMap uses a private opt-in Collections.FlushAll then backend Checkpoint,
checked original owner Close and fresh-owner reopen. That private checkpoint does
not itself prove an owner-complete public barrier. Random preclose/reopened typed
state equality proves preservation of observed state, not every overlapping random
acknowledged update or linearizability. Separate deterministic exact-value fixtures
remain distinct. [Reviewed controls](ycsb_2026_10_30_evidence/control-review.md)
bind the public captured-prefix sentinel/persisted-pointer check and ordinary ACK
physical-WAL-sync/replay checks to their exact source, ELF and accepted scope.
They do not establish power-loss, GC-stress or all-pointer certification.

Actual A/B client startup samples had GOMAXPROCS absent, effective runtime 12,
affinity CPUs 0–11 and sampled ancestor cpu.max=max 100000. Six live GoMap owners
witnessed COW/durable/effective12. These are samples, not continuous observations
or a quiet-host claim. MongoDB cache/configuration differed: observed
journalCommitInterval=100ms and syncdelay=60s. MongoDB had a 12 GiB/no-swap
container and 12 GiB/1,800 CPU-second supervised children, with four sequential
mongod process limits of 449/450 CPU seconds. GoMap had a 24 GiB/3,600 CPU-second
supervisor. These are asymmetric split bounds, not an observed combined aggregate
watchdog. Container membership was sampled; resource JSON lacks its own timestamp;
Docker writable-layer disk lies outside the bind-data guard. No equal-memory,
equal-cache, sustained-load or power-loss comparison follows.

MongoDB and B timed phases can be subsecond; short duration, cache warming,
observer perturbation and three repeats limit inference. Final retained A/B cleanup
checked 442 PID/start identities and 24 containers absent, coordinators stopped
and ports 27018/27130 free. Earlier A-only audit saw legitimate B activity;
it is not retrospective proof of host isolation. Cleanup audits describe that
completed packet, not continuous future host state.

## Maintained implementation and remaining acceptance

The maintained runner documents separate source-bound MongoDB reference preparation
and dispatch in [the reference contract](../../scripts/ycsb_mongodb_reference.md).
Normal shell TARGETS=mongodb is deliberately refused. Reference manifests distinguish
j:true from j omitted; explicit j:false is unsupported. The accepted producer,
checked-close consumer, progress parsing and real-child runtime observation must
travel together. No new harness code gap was demonstrated while packaging these
retained results; unnecessary parallel tooling was not added.

Local selected overlay 844930b8 had 222 offline/synthetic tests pass; six Go
owner-evidence lifecycle tests passed at 326d, before its single Python test
correction. Initial failed offline controls remain retained. The exact 57-file
overlay was source-compared on later integration d64; those old test executions
are not d64 or bd070 runtime results. Candidate official-driver JSON binary refusal
passed all three profiles at 1ca58fa; historical control passed all three at
030311e (historical source plus disclosed lifecycle overlay). These are same-process
correctness controls with original Close/checkpoint/reopen, not external crash or
performance evidence; the historical test ELF was not retained/hash-bound.

The [source/evidence crosswalk](ycsb_2026_10_30_evidence/original-evidence-crosswalk.md),
[frozen 24-entry secondary ledger](ycsb_2026_10_30_evidence/original-secondary-status-ledger.md)
and [original integration checks](ycsb_2026_10_30_evidence/original-result.md)
are copied byte-for-byte and hash-bound in the packet index. Their original root is
`/home/mikers/gomap-5335-main34-overlay-qualification-20261030/`.
The crosswalk's append-only driver-control addenda supersede the frozen ledger's
six earlier "official-driver unproved" statements. All 24 secondary statuses and
missing legacy timings stay explicit; deferred legacy performance is not a
prerequisite to the primary MongoDB comparison. They are whole-version diagnostics.

| Obligation | Retained evidence | Remaining owner/action |
|---|---|---|
| Both actual-Mongo acknowledgment classes | Three relaxed pairs; A/B durable distinctions and strict load failure, all at 802f | #5225/ROOT accepts scope and residual regressions; do not relabel as final-head |
| Maintained harness integration | Source-selected overlay, 222 offline controls and six lifecycle tests under their original heads | Integration owner verifies final source applicability and runs changed-risk/final policy checks |
| Final product source/lifecycle qualification | Existing scoped controls, public lifecycle and deterministic/random fixtures remain reusable where applicable | Integration owner binds merged selected repairs, actual gateway/helper ELFs, owner, emitted concern, original Close and recovery; records precise evidence reuse versus missing proof |
| Cost/correctness dependencies | Original failures and selected repair evidence retained separately | Predecessor owners resolve #5333/#5334 and strict-cost/repair dispositions, then merge through current-head CI/review |
| Durable report delivery | Selective raw phase archive, original reports/data and hash index in this packet | #5335 owner preserves accessible full artifact locations, publishes reviewed packet and obtains #5225 acceptance |
| Final comparison applicability | No completed sample is attributed to bd070 or a future merged head | ROOT must resolve final-head applicability and any missing final-head measurements under the existing qualification contract; this packet launches no campaign |

The next consequential public proof is the selected final repair/integration
candidate's actual public owner/request/Close/lifecycle applicability, with
existing controls reused where their contracts still apply. Offline green and
this report do not establish final merged-head throughput. There is no README
speedup promotion; the June 3 MongoDB 26,494.1 ops/s row remains unmatched
historical context and is never used in an October ratio.

The [final-source plan](ycsb_5335_final_source_qualification.md) retains the
required six fresh matched pairs/twelve arms after repair and integration gates.
Its provisional build and two tiny correctness guards are source-bound to c660;
[actual readiness receipts](ycsb_2026_10_30_evidence/final-source-readiness.json)
record their owner/lifecycle/Close/runtime evidence and preserve the accepted792203
A/B control identities. These are not new performance results. A subsequent
production repair delta requires an explicit final build/source binding.
The [local artifact inventory](ycsb_2026_10_30_evidence/retained-artifact-inventory.json.gz)
records exact persistent host185 paths and sizes. A separate
[content verification receipt](ycsb_2026_10_30_evidence/retained-primary-snapshots-sha256.json)
rehashed all93 full primary typed snapshots,14238680662 bytes, against their
original Close artifact hashes. Originals remain in place and accessible locally;
this selective Git packet does not contain them or claim external publication.
[Packet verification](ycsb_2026_10_30_evidence/packet-verification.json) covers
all320 raw archive members and11 copied original documents, including the six
previously omitted relaxed GoMap load/run outputs and accompanying receipts.
