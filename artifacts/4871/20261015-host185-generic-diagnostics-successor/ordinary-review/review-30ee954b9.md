# Independent source review — 30ee954b9

**ACCEPT for exact source and the unchanged previously reviewed measurement harness. No blocking source findings identified. Mergeable: false; hosted correctness and performance remain pending.**

- Review mode: `INTERNAL_SUBAGENT_REVIEW`; actual reviewer `/root/ordinary_review`, explicitly routed `gpt-6.1-sol`, high reasoning. No construction role, children, candidate edits, Git/GitHub mutations, or tests/builds/benchmarks by reviewer.
- Head: `30ee954b974077b8968cdfb836e03058d9934ed1`; tree: `1d449d3f13181f59b19505ae8be20b30c85d958a`.
- Reviewed parent: `616fd1497b3e483f40bfbf3cefe4538d250bb33a`; graph baseline: `5bca87ffe6e1bfba2d693264d2dc82936adc234f`; PR5127.
- Hosted manual review cannot pin the required exact Sol6.1 model and was not requested. No hosted quota response, failure, or review result is invented. This actual separate reviewer has remained read-only.
- Candidate clean and exact head/tree verified before and after review. Independently SHA256-checked all 10,246 manifest paths: zero mismatches. Successful final test freeze differs only at `.github/ci/ci_impact.json`; tested product and test bytes match.

## Disposition and scope

This continues the whole-foundation contract review preserved in `review-f0415773`, `review-a8c52ed66`, and the accepted `review-616fd1497` artifacts. All 21 successor paths (718 insertions/94 deletions) received fresh review, with adjacent construction, release, diagnostics, Union, CWAL and private read callers traced independently. Base-to-head is 536 files /86,972 insertions /5,274 deletions; this does not claim every line was read. Prior R1–R5 fixes remain valid in unchanged source. No new source blocker was found.

The earlier lifetime architecture disposition remains: publishers exclude capture under rootReuse then db.mu or proven private/offline exclusion; actual pointer-bearing readers retain the exact Set before unlocking and through last use; topology copies preserve the SAME primaryRoot current edge. The new hook carries only the selected Set and scalar root and does not reintroduce a DBState escape. Source guard and real Set/Refresh controls remain intact.

## Generic diagnostics and admitted ownership

Read the complete new `resource_generic_diagnostics.go`, its tests, and changes in `resource_token.go` and `resource_set.go`; traced constructors, original-observation recursion, shared clones, final cleanup, Stats/descriptors/registry queries, and namespace compatibility.

Generic tokens capture immutable copied namespace descriptor and linked identity, independently copied nonempty text/platform strings, shared atomic sync counters and exact registry identity. The diagnostic record has no operational namespace, pin, file or cleanup-owner backpointer. Registry statistics use its original locking. Absent namespace/registry creates no backing; selected token construction does not allocate generic backing. Counter initialization and updates use the original namespace mutex; both runtime sync sites update the same atomic identity, and initial batch/proof counts precede capture. Mixed generic/selected Stats deduplicate the shared metric identity.

Mixed compatibility still locks the actual selected namespace and preserves its metadata-scoped released check. Generic comparisons use immutable copies. Selected final namespace cleanup clears the counter pointer before containing refund. Added token/namespace pointer fields are included by existing actual `unsafe.Sizeof` admissions. Generic diagnostic heap allocations are explicitly scoped to canonical generic registrations, not claimed as newly PRIMARY-admitted or a zero-cost optimization. Physical ownership/refund and failed custody paths were not replaced by diagnostics.

Raw logical Union locks each source and refuses selected flags, admitted kind-view allocation, selected representatives and every selected pin before copying entries. Empty-kind checks reject scrubbed admitted tokens hidden in flat views; selected empty Sets retain their selected flag. Extension Union returns a genuinely nil interface on refusal. The new controls preserve bytes, ownership and original pins on refusal, and exercise hidden representative/pin and scrubbed-pin cases. Released genuine generic diagnostics remain supported.

Caller closure was checked against production searches and the maintained inventory:

| Consumer | Actual retained contract |
|---|---|
| Public/extension logical Union | Refuses admitted escapes; generic immutable diagnostic path remains. |
| Coordinator validated counts | Synchronous count only under candidate/coordinator custody; no exported view. |
| Durable grouped publication | Existing physical union validates consecutive transaction members and borrows their exact tokens through callback outcome. |
| Maintenance reachability | Existing physical clone acquires independent pins and contributing directory roots while coordinator owns source candidates. |
| Selected telemetry | Imports admitted metadata, acquires admitted diagnostics, copies canonical scalar metrics, then closes original views. |
| CWAL debt Union | Actual external-RID fence producer uses generic builder/registration without MetadataOwner. Debt/extra synchronization retains original source ownership through sync and namespace work; releases follow the serialized durable-prefix boundary. |

Remaining direct operational namespace/pin uses are construction, physically retained clone/sync/validation, or admitted observations, rather than detached generic diagnostics. No new unowned selected copy was identified.

## Private one-shot read and fixture assertions

Traced all four production `acquireOneShotReadOrErr` callers in api.go. They return owned values and expose neither Snapshot, iterator, alternate root nor historical rebind. Get/GetVersioned use the main Tree; append variants call the main Tree directly. The separate pool begins with unused alternate-root/rebind/iterator fields empty. Skipping resets for those permanently unused fields does not skip main Tree.Reset, reader/state/pager scrub, registry/Set/PRIMARY ownership, original final completion or failed-cleanup non-reuse. Exported Snapshot cleanup remains unchanged. The new control checks those fields before/after real reads and verifies required aliases disappear. This is a source simplification, not measured performance improvement.

The finish fixture now starts another goroutine that acquires c.mu while the SAME containing refund remains paused. It checks original coordinator state before ACK, preserving owner/phase, transaction mutex, CompleteStartup, Stop and WaitThrough boundaries under the unchanged five-second context. It no longer mistakes unrelated brief c.mu contention for retention by the refund caller. No ACK or deadline oracle was removed.

All changed platform fixture callers and the shared helper definitions were read. They invoke actual unsupported operations before fault installation and require typed refusal with unchanged state/pair, custody/charge or persistent input bytes as applicable. Supported platforms proceed to original injected fault/success/reopen/shrink assertions. Native capture uses actual pager capability and checks no admission/custody/epoch effect on refusal. Failed cleanup tests assert sticky original error, no refund/replay and retained LOCK before fixture-only process-shutdown closes; those closes are not accepted completion receipts. Collection rewrite's unsupported branch compares all retained non-LOCK file bytes and then reopens/reads the data. No supported-path oracle waiver was found.

## Workflow and measurement limits

PRIMARY runner, its seven failure controls and committed Go fixture are byte-identical to accepted616; hashes were independently compared. The workflow adds a named successful self-check prerequisite and an explicit `!cancelled()` condition so PRIMARY still runs sequentially after a failed raw comparison. It keeps raw failure status, exact resolved SHAs, fresh output, original five raw thresholds and all PRIMARY thresholds. This does not turn a raw threshold failure green. Refreshed CI inventory changes match workflow location/condition/fingerprint updates; original member inventory and command thresholds remain.

The prior full harness review remains applicable: exact clean full-SHA/binary provenance, SAME baseline-only harness overlay, retained executables/hashes, fixed eight AB/BA pairs/128 iterations/CPU0/GOMAXPROCS1, mandatory public lifecycle, zero candidate generation advances, <=5% paired time/no allocation growth, sampled quietness with exited-process accounting, timeout process-group kill/join, no retry/waiver and final drift checks. Sampling limitations remain explicit. No new timing was collected by reviewer. Static reset ELF comparison is not latency attribution.

**Predecessor616 GetVersioned +13.5938% remains RED. Fresh exact-head original-five raw and PRIMARY gates, applicable M0 variability, correctness/race/platform CI and required aggregate are still necessary.** Previous M0/C3 passes, failed older variability, old timeout investigations and local structural controls do not qualify this head. No merge authorization, native/Q32/C3 final graph or Dgraph qualification follows from this acceptance.

## Evidence actually inspected

Read HANDOFF, candidate/freeze manifests, root-validation, validation-final, failure-dispositions, Union/platform caller inventories, reset static comparison, metadata receipt and all six metadata logs. Parsed the retained raw logs below, independently recomputed their hashes/pass counts and checked for failures/race warnings; commands and exact toolchain/environment were read. Reviewer did not run those commands.

| Retained final run | Actual inspected result |
|---|---|
| union-repair-full-normal-04 | Entire rootpublication: 352 top-level /243 total subtests PASS; 12.021s package. |
| coherent-final-normal-02 | Focused DB+collections: 45 top-level /75 subtests PASS, including all eight allocation shapes and CWAL controls; packages20.671s/1.011s. |
| coherent-final-race-02 | Count3 DB+rootpublication:117 top-level /189 subtests PASS; no race warning. |
| generic-final-race-04 | Count3 original hosted concurrent-release plus new generic/Union and selected lifetime controls:27 top-level PASS; no race warning. |
| windows-final-build-01 | Go1.26.3 Windows amd64 CGO-disabled compile of DB/collections/rootpublication, retained exit0; not Windows execution. |
| root metadata00–05 | Refresh and check exit0; inventory15, impact21, CI contract6, Windows headroom16 controls PASS. |

Successful logs bind all 10,246 source paths to final freeze; only subsequently refreshed CI inventory differs in candidate. Earlier RED logs remain preserved and are not relabeled. Applicable exact-head policy inventory remains root AGENTS, TreeDB AGENTS and separately scoped unchanged HashDB AGENTS; root/TreeDB/CONTRIBUTING apply. No policy changed from accepted parent. Task-local mergeable fallback and Go performance skills were applied; no hard review cap was found.

**Final source/harness disposition: ACCEPT. Blocking source findings: none identified. candidate_edits_by_reviewer:false. Hosted correctness/performance: pending; mergeable:false; merge_authorized:false.** Review/runtime lease released on completion; no fix edits follow.

Changed paths reviewed:

- `.github/ci/ci_impact.json`
- `.github/workflows/treedb-tests.yml`
- `TreeDB/collections/api_test.go`
- `TreeDB/db/db.go`
- `TreeDB/db/native_mvcc_snapshot_capture_test.go`
- `TreeDB/db/one_shot_custody_test.go`
- `TreeDB/db/pools.go`
- `TreeDB/db/primary_joint_vacuum_v6_test.go`
- `TreeDB/db/primary_offline_pair_v6_test.go`
- `TreeDB/db/stable_pager_owned_operations_test.go`
- `TreeDB/db/state_value_log_lifetime.go`
- `TreeDB/db/state_value_log_lifetime_test.go`
- `TreeDB/db/vacuum_original_completion_test.go`
- `TreeDB/db/vlog_ref_incarnation_test.go`
- `TreeDB/docs/design/ordinary-primary-allocation-5111.md`
- `TreeDB/internal/rootpublication/primary_finish_containing_test.go`
- `TreeDB/internal/rootpublication/resource_contract_test.go`
- `TreeDB/internal/rootpublication/resource_generic_diagnostics.go`
- `TreeDB/internal/rootpublication/resource_generic_diagnostics_test.go`
- `TreeDB/internal/rootpublication/resource_set.go`
- `TreeDB/internal/rootpublication/resource_token.go`
