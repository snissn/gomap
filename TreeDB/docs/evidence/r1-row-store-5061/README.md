# R1 indexed row-store evidence — final qualification pending

The accepted baseline and final selected typed-read packet establish complete-row
correctness and measured read costs. Prepared reads reduce Go allocations; final
ordinary `GetInto` timing is inconclusive. Retained lifecycle qualification,
public artifact downloads and final [#5061](https://github.com/snissn/gomap/issues/5061)
/ [parent #5056](https://github.com/snissn/gomap/issues/5056) acceptance remain open.
A valid packet or a passing diagnostic does not close those gates.

The authoritative scopes are the [comparator contract](../../spec/r1-indexed-row-contract.md),
[complete-row reads](../../spec/r1-row-reads.md), [indexed mutations](../../spec/r1-indexed-mutations.md),
and [lifecycle contract](../../spec/r1-row-lifecycle.md). The
[user guide](../../guides/typed-row-store.md) and
[runnable example](../../../../examples/typed_rows/README.md) describe the supported APIs.

## Decisions and remaining gates

| Obligation | Actual state |
| --- | --- |
| Baseline harness and complete-row/current/historical oracles | Accepted [#5057 baseline](https://github.com/snissn/gomap/issues/5057#issuecomment-6009386341); tooling merged in [#5064](https://github.com/snissn/gomap/pull/5064) |
| Read/mutation behavior and integration | Focused normal/race and selected mutation/recovery checks pass; [#5065](https://github.com/snissn/gomap/pull/5065) has [clean hosted review](https://github.com/snissn/gomap/pull/5065#issuecomment-6011771195), required CI/merge pending |
| Affected final typed-read costs | Original `00d2c370` packet validated; three stable matched groups, twelve inconclusive groups, one newly enabled range; no stable matched group loses more than 15% throughput |
| Maintenance prerequisite and supported lifecycle tooling | Independently reviewed integration [#5071](https://github.com/snissn/gomap/pull/5071), `f79616f2`; hosted review, required CI and landing pending |
| Retained lifecycle / physical and memory interpretation | Pending reviewed landed source and accepted five-process packet; targeted reclamation diagnostics are supporting evidence only |
| Immutable public artifacts and fresh download/restore/replay | Pending; private staging and successful frozen-binary replay are not public publication |
| Final E / parent decision | Pending coordinator disposition of remaining gates |

## Frozen provenance and publication status

Original packet, source, executable and raw bytes retain their measured identity.
The final read runtime inventory and A harness match the reviewed read implementation in
`c8c54ccc6c9ded2020366571349820ae28cbc87d`; this applicability check does not
relabel the measured `00d2c370` packet as a later integration. Maintenance has
separate runtime/harness inputs and needs its own retained freeze.

| Evidence | Measured source | Packet SHA256 | Publication / validation |
| --- | --- | --- | --- |
| Accepted six-engine baseline | `0216e2e9ee701dee0eda96c569dc0c8facb37ca9` | `d3bcec0a4924267afeee67a13ec1e5f83c8ef930a12d1b46538c3cd71746013f` | All 30 engine/repetition cells and full oracles pass; private restored frozen-binary replay exit 0; public URL pending |
| Historical six-engine candidate | `f95ea1aac407537342214805930c8907775b777f` | `d6559eee1874a6cb37d718df161cf891d604a0e2016ae3794f9df4989f0f708b` | Validated historical comparison; original identity retained; public URL pending |
| Final affected typed-only reads | `00d2c370a5907225ff36f82bb92ca3ee0679ea08` | `a576f645566729c514b751d543261a67fa49b43a62dffd122e672630063164fb` | All five cells and oracles pass; private restored frozen-binary replay exit 0; public URL pending |
| Supported lifecycle retained candidate | Pending actual landed-source freeze | Pending | No retained packet or public acceptance recorded here |

Shared comparator harness SHA256:
`8fe73a72755c4d88ff83d8587a37692419ca6625f7b05d68497e0c558ada4efb`.
Baseline runtime SHA256:
`0bdca3684ca4de3ddfae5e17b96e910bc7abb86532c48c40087f453df2c1a2c1`.
Final-read runtime SHA256:
`53f92a2a08ddb0496e51734686b851790f79027df93e7d5a83d7f1258360248c`.
Their executable SHA256 values are respectively
`898457b63c4ba9a3897150b4c9cacf09222830f87cf3616dfe73f479e7596812`
and `1e52f91f7e1d2ed92e49c3ac7c103ee28cb67ec568bd2d5990ab33fd1d97e6be`.
Fixture SHA256:
`fd5d377dceaa2e53dd37bb3c2b56504f202e411988e17843ed3be1f257ee205c`.

Final publication must supply the populated manifest, complete file inventory,
checksums, immutable downloadable binary overlay and actual download/restore/replay
proof. Capture trees keep original relative paths and bytes; private retention
paths alone are not downloadable artifacts. Private baseline and final-read
stages preserve original binary overlays and compact replay proofs; those replays
used the frozen executable, not a rebuild or benchmark rerun. Pending public
files and URLs must not be represented by invented links.

## Measurement scope and noise

Comparator captures use 4,096 complete rows, 32-row batches, 1,000 calls per
phase, one worker, durable ACK and flushed read state. Five independent fresh
DB repetitions run **inside one process**, with serial engine cells. Baseline
and historical candidate use JSON, template-v1, BSON, typed-row, SQLite JSON and
SQLite row storage. The final packet explicitly selects typed-row only: engine
order and allocator carry differ. It is an affected typed-read comparison, not
a repeated full matrix or a new SQLite comparison.

Matched inputs retain Linux amd64 Go 1.26.4, the same host/device, compiler/CGO,
fixture, timer, warmup and A harness; GOMAXPROCS=16, GOGC=100, GOMEMLIMIT=off and
empty GOFLAGS/GODEBUG. Baseline records SQLite 3.51.3 WAL/FULL synchronous ACK,
separate from TreeDB durable command-WAL ACK. Typed relaxed admission and
retained-document atomic mixed upsert remain unsupported, not emulated work.
Runner activity and observations stay with the packets; absolute quietness is
not assumed.

Throughput spread `(max-min)/median > 15%` is inconclusive. Baseline has
69 inconclusive groups among 92 timing groups. Medians below are descriptive;
per-repetition p50/p95/p99 and sample counts remain in source-bound summaries.
A median of repetition p99s is not a pooled p99. Setup, first fetch, checkpoint,
common storage observation and trailing upsert remain separate phases.

## Final selected typed reads

| Route | Baseline ns/call | Final ns/call | Final calls/s | Final Go B/call | Final objects/call | Final timing spread |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Ordinary complete `GetInto` | 349,538.202 | 68,417.985 | 14,616.040 | 91,576.976 | 150.762 | **23.70%; inconclusive** |
| Prepared complete point | 7,246.610 | 6,738.723 | 148,396.069 | 8,158.320 | 92.987 | 4.47% |
| Prepared complete batch | 135,202.920 | 137,544.195 | 7,270.390 | 128,677.832 | 2,005.850 | 5.98% |
| Quiescent prepared range decomposition | 48,151.921 | 48,445.246 | 20,641.860 | 43,475.744 | 677.632 | 3.80% |
| Complete ordinary public range | Unsupported residual-only output | 114,638.652 | 8,723.061 | 150,442.680 | 891.162 | 7.92%; newly enabled |

Prepared point, batch and decomposed range each save about 1,536 Go B/call and
12 objects/call against the selected baseline. Ordinary `GetInto` retains
+2,401.872 Go B/call (+2.693%) while objects fall by 0.738/call (0.488%).
Avoidable full-part lookup maps and discarded statistics-map copies were removed;
scoped allocation diagnostics identify those savings. The coordinator accepts
the remaining modest byte cost for the existing snapshot-bound complete-row
materializer. Final ordinary timing remains inconclusive: its descriptive ratio
is not a final speedup claim.

Historical full-matrix `f95ea1a` ordinary `GetInto` measured a stable 4.820×
throughput ratio with 10.51% candidate spread. That finding remains bound to its
original source and route; it is not transferred to the repaired final head.
The newly complete ordinary range has no equivalent complete-output baseline
and no before/after speedup ratio. Prepared range decomposition selects IDs then
fetches full rows in a quiescent harness; it is a different route from the
ordinary single-publication full-row API.

Go allocation counters are process-wide runtime deltas, including background Go
activity and excluding SQLite C allocations. These results establish no SQLite
total-memory superiority, exact RSS peak or whole-database capacity claim.
Lifecycle heap measurements also include diagnostic oracle/sample objects;
sampled RSS, if later collected, needs its raw series and sampling scope and is
not an exact peak.

## Indexed mutations and recovery

The selected mutation matrix verifies complete rows and current/historical unique
email and nonunique city postings before flush, after flush and after reopen.
It covers insert, replace, mixed existing/missing upsert, generic complete-document
update, explicit source replacement and delete; result counts, conflicts, swaps,
duplicate rejection, input reuse and retained null/missing values are checked.
The typed carrier holds required strings; numeric, boolean, null and missing
values remain residual JSON. Generic updates return a complete replacement;
these are not native partial-column setters.

Eighteen selected process-crash cells exercise six public operations at rejection
before append, ambiguous post-sync/pre-ACK, and successful ACK. Recovery checks
primary/index agreement and command replay. This is process-crash evidence,
not physical power-loss certification. Ambiguous outcomes require reopening and
reconciliation; they are not automatically safe blind retries.

Final selected-scope mutation medians remain auditable, not performance wins:
all the following timing groups exceed the noise limit. Caller encoding through
ACK is timed; generic update callbacks return preencoded complete rows.

| Phase | Baseline ns/call | Final ns/call | Final spread | Interpretation |
| --- | ---: | ---: | ---: | --- |
| Nonindexed update | 19,676,968.399 | 11,311,130.871 | 58.59% | Inconclusive |
| Indexed update | 22,571,341.781 | 13,852,029.181 | 53.39% | Inconclusive |
| Replace | 22,047,770.338 | 12,600,624.832 | 56.48% | Inconclusive |
| Delete | 22,902,932.822 | 17,910,939.560 | 58.26% | Inconclusive |
| Mixed churn | 31,052,622.047 | 30,284,595.601 | 82.43% | Inconclusive |
| Atomic mixed upsert | 36,128,036.635 | 37,068,110.960 | 89.86% | Inconclusive; no retained-document equivalent |

## Lifecycle, physical storage and unresolved guardrails

Retained lifecycle qualification is **pending**. Its separate supported direct-backend
fixture must run five fresh OS processes, each with five final epochs, 4,096 live
rows and 1,024 calls/epoch. Timed churn revisits a deterministic 512-ID hot set;
it does not establish full-population stress or infinite capacity. Calibration
logs are preserved and excluded from final metrics. This fixture is not byte
identical to the comparator and excludes cached-wrapper overhead from direct
backend vacuum/compaction costs.

| Required retained observation | State |
| --- | --- |
| Five-process throughput, latency, allocation and descriptive spread | Pending |
| Operation/revisit counts, full rows/postings, held-view and reopen oracles | Pending retained record; focused correctness passes |
| Ingest/churn/checkpoint/maintenance/release/reopen component census | Pending; separate index, value-log, leaf-log, typed assets and other persistent bytes |
| Actual fold/rewrite/vacuum/GC work, pins, both recovery closures and remaining debt | Pending retained interpretation |
| Heap / retained-after-GC heap / optional sampled RSS and scope | Pending; no total-memory or exact-peak claim |
| Independent landed-source/clean-build/binary/completed-packet receipt | Pending actual reviewed landing and observed retained run |

Earlier actual-size `b53e3ae` evidence is retained as a negative physical-growth
packet. Logical `BytesDeleted` and zero debt did not establish unlink or bounded
storage. Owner diagnostics found obsolete packs retained by both recovery slots;
exact current rebuilt-root projection now removes only scan-unreachable packed
dependencies, while older fallback, held and prepared resources retain protection.
Actual pack plus length-index unlink and corrupt-newest-meta recovery pass.
Source-bound pre-hardening `d078` two-epoch diagnostic changed released leaf
bytes 189,498→190,671 (+1,173) with typed bytes stable at 691,175. It is a causal
discriminator, not retained-capacity acceptance or a packet for the hardened head.

Separate fixes cancel/join manager-owned retries and reclaim unreachable immutable
leaf-manifest revisions with exact namespace/identity authority. The scaled
`702ef2cc` primary-vlog test naturally rotates at a private 16 KiB threshold:
a cold/hot 16,112-byte segment is evacuated, held rows remain valid, both recovery
slots converge, actual unlink follows reader release and fallback reopen passes.
This tests the mechanism; it does not cross the production 4 GiB cap. A following
dead 1,344-byte rewrite output remains active under current/newest-lane policy.
That lawful retention and finite lane-window inquiry do not prove a total DB byte
bound or immediate deletion.

Native appender creation metadata previously retained eighteen records across
six primary and twelve leaf files. Exact manager handoff now retires successful
native records and backing storage; failed/ambiguous witnesses remain. Current,
held, cold rows and fallback recovery pass; standalone rewrite history remains
intact. Metadata retirement releases no resource pin or root closure and is not
physical reclamation. All original failures, off-profile rehearsals, source-guard
rejections and noisy packets remain under original identities.

The coordinator must still land reviewed prerequisites, collect and independently
validate the retained lifecycle packet, reconcile actual component growth and
remaining maintenance debt, publish immutable evidence, and complete fresh public
download/restore/replay before recording final acceptance. Existing shared
resource/rewrite owners retain authority; this report authorizes no unsafe reclaim
or new retention policy.
