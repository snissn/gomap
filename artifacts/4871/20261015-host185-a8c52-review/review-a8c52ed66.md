# Independent ordinary foundation successor review

```text
review_mode: INTERNAL_SUBAGENT_REVIEW
reviewer: /root/ordinary_review (independent reviewer, no construction role)
model: gpt-6.1-sol
reasoning_effort: high
repository: snissn/gomap
pr: 5127
base_sha: 5bca87ffe6e1bfba2d693264d2dc82936adc234f
head_sha_or_manifest: a8c52ed66fc587d4823f020cf15ea1c3ee8c1ebc
head_tree: d363fab80b10dfa7804cea1a3daf4b951c1f5949
disposition: REJECT
candidate_edits_by_reviewer: false
construction_role: false
reviewer_runtime_campaigns: none
unavailability_evidence: The workstream requires exact GPT-6.1 Sol. Hosted manual review cannot pin that model and was not requested. No hosted failure, quota response, or hosted review is claimed. Coordinator reports a fresh classifier inventory with zero existing requests/findings/threads.
```

The successor fixes original findings R1–R3. Two remaining source blockers, R4 and R5, require another coherent repair and exact-candidate review. This rejection does not depend on pending CI or performance evidence. The prior frozen review, `review-f0415773.md/json`, remains unchanged. The reviewer wrote only these review artifacts and ran no tests, builds, benchmarks, Git mutations, or GitHub actions.

## R4 — P1: Set retirement still races unpinned state readers and omitted publishers

Primary locations: `TreeDB/db/vlog_rewrite.go:2784-2793`, `:2929-2933`; `TreeDB/db/compact_storage_audit.go:315-337`; `TreeDB/db/leaf_generation_gc.go:849-887`.

The full foundation changes ordinary production Sets with up to 17 files to use the SAME staged release implementation (`internal/valuelog/manager.go:2608-2614`). At zero Set references it deletes entries, clears `nativeFiles`, sets `Files = nil`, and becomes terminal (`native_mvcc_set_release.go:137-171`). `Manager.Acquire` only increments the atomic counter (`manager.go:2595-2599`); it cannot revive a terminal Set or reconstruct its original file borrows. Every live reader therefore needs its Set reference before the publication reference can be consumed.

The successor correctly adds `rootReuseMu` exclusion to `RefreshValueLogSet` and `publishValueLogSetNoRefresh`, and pins every nonnil exported/bounded/native Snapshot Set. The exclusion is incomplete in both directions:

| Consumer actually inspected | Gap and concrete use |
|---|---|
| `vlog_rewrite.go:2775-2803`, `:2919-2943` | Optimistic and serialized user-root rewrite drop `db.mu` and `rootReuseMu` before `Acquire(vlogSet)`. A refresh can consume the exact selected Set in that interval. The resulting tree and rewrite zipper still use it. |
| `vlog_rewrite.go:2494-2509`, `:2560-2575`, `:2674-2691` | System/collection rewrite capture current state, unlock, and use `state.ValueLogSet` in descriptor walks, pointer matching and zipper reads without acquiring it at all. |
| `db.go:5503-5526` | `CompactIndex` constructs its reader under `db.mu`, then unlocks and walks/builds without a Set pin. Its writer lock does not exclude refresh. |
| `ordered_root_publish.go:1924-1942` | `publishOrderedRootIterator` loads current state and constructs a base tree without a Set pin. Its public system/ordered-root callers hold writer serialization; `RefreshValueLogSet` does not join that serialization. |
| `compact_storage_audit.go:265-274` | The audit topology preparer reads current Set maps for comparison after an unguarded state load. The separately owned `managerSet` protects its own map, not this current Set's map. |
| `vlog_rewrite.go:1788-1789`; `leaf_generation_pack.go:317-318` | Online dictionary preparation receives `db.State()`, a shallow clone (`db.go:5209-5215`) with no Set borrow. `prepareRewriteLeafDict` scans that Set at `vlog_rewrite.go:768,780` and may train through its tree/reader at `:587`. |

| Publisher actually inspected | Missing capture exclusion |
|---|---|
| `compact_storage_audit.go:312-338` | `publishCompactStorageValueLogSet` replaces the published view and releases the old Set using `db.mu`, without `rootReuseMu` or exclusive `valueLogPublicationMu`. Ordinary Snapshot capture reads the atomic view without `db.mu`. |
| `leaf_generation_gc.go:846-889` | `publishLeafGenerationState(true)` refreshes/replaces/releases a Set using only `db.mu`. The false branch acquires the same Set and is not itself a zero-reference Set replacement, but still needs the R5 current-root correction. |
| `vacuum_online.go:1748-1789`, `:1844-1846` | Replacement publication uses the cutover writer/durable-publisher fences; old Set release follows outside them. There is no root-reuse or value-log-publication capture exclusion. A retained vacuum basis is not a universal pin on this exact old Set: a topology-only refresh can replace the Set without changing the logical commit/root token. |

Trigger: use a nonempty ordinary Set within the maintained 17-file domain, with only the DBState publication reference on that exact Set. A later rewrite batch can select such a Set after earlier publications have advanced beyond its outer scan Snapshot. Pause after selection/unlock and run exported `RefreshValueLogSet`; it publishes a fresh Set and fully consumes/scrubs the old one. Resume the late `Acquire` or unpinned reader. An outer-leaf pointer-projection traversal now sees missing files; overlapping map reads/deletes are a Go data race and can panic. An omitted topology publisher can cause the same race with an ordinary Snapshot acquiring from the old atomic view. Independent physical-resource tokens or another Set may keep files on disk but do not keep this Set map readable or its admission live.

Correction: inventory all published Set replacements and all current-state readers. Acquire the exact Set under the SAME capture exclusion, before releasing it; retain it through the last tree, iterator, zipper or dictionary-scan consumer and join the actual release result/custody. Make every replacement/last-publication release respect that exclusion. Preserve established lock order and avoid recursively taking the gate in helpers called from already-fenced publication paths. A retry after missing-file error or an atomic increment after zero is not a lifetime repair.

Controls should deterministically interleave capture/pin with actual refresh/publication for user/system/collection rewrite, CompactIndex, ordered roots and dictionary scans, with real outer-leaf pointers and a distinct current Set from any outer Snapshot. Cover the omitted topology publishers against public/bounded Snapshot capture. Verify old Set references cannot reach terminal cleanup while consumers remain; then verify cleanup/refund after their actual last release. This finding is established from source; the reviewer did not run a reproduction.

Safe inputs distinguished during this audit: Snapshot-backed fragmentation and maintenance reachability/live scans already hold the Snapshot's Set; fresh `CurrentSetNoRefresh` inputs already own a new Set reference; candidate dependency projections/rebinding explicitly own fresh Sets; offline vacuum/rewrite hold the exclusive offline source and acquired original Set. Scalar-only state readers, including leaf-pack copy-basis comparison, do not read mutable Set maps and are not findings. An external `State()` shallow copy is not itself a new ownership API.

## R5 — P1: Topology-only DBState replacements erase current PRIMARY ownership

Locations: `TreeDB/db/compact_storage_audit.go:322-331` and `TreeDB/db/leaf_generation_gc.go:867-875`.

Both replacement literals preserve the scalar `RootPageID` but omit `primaryRoot`. The full foundation introduced that field as the original current logical-root descriptor. The correctly repaired refresh helpers preserve it (`db.go:5371,5422`); these sibling paths do not.

Trigger: publish any ordinary PRIMARY root, then perform an actual audit topology replacement or leaf-generation state publication. The new current state still selects the PRIMARY root ID but has a nil ownership descriptor. Snapshot acquisition then skips `acquirePrimaryRoot` and omits the PRIMARY metadata owner from admission (`db.go:1832-1833,1963-1965,2042-2043`). The next real publication cannot drop the lost current edge through `post.oldState.primaryRoot` (`db.go:4392-4393`). Terminal cleanup explicitly obtains the current descriptor from the actual DBState and deliberately discards the potentially stale runtime alias (`durable_root_runtime.go:3224-3229`), so that fallback does not repair this loss.

This can strand admitted current-root/proof capacity and bypass the actual read-root/physical-owner capture contract. An old slot or coincidentally leaked bank is not a substitute for a current-root ownership edge.

Correction: transfer `oldState.primaryRoot` unchanged on every topology-only replacement, preserving the SAME single current-root edge and its allocator generation certificate. Do not create a duplicate independent root, transfer from a stale runtime projection, or add a matching Drop without a new acquired edge. Audit all published DBState literals, distinguishing private candidate projections and initial state construction. Add controls that exercise both helpers on a real PRIMARY current state, capture an admitted Snapshot/iterator, advance slots and parents, and prove old-root/physical completion and final governor drainage. Source review establishes the omission; no runtime result is invented.

## Original findings resolved in this successor

| Finding | Source disposition and inspected controls |
|---|---|
| R1: certificate cache before accepted custody | FIXED. Cache assignment moved after `member.install.activate` succeeds (`root_publication_activation.go:875-883`). Late preparation/activation refusal keeps the accepted cache unchanged. The accepted replacement root already has its conservative proof allowance before old-state post-work drops the previous root. `closeAfterHooks` clears the cache before durable-root/index withdrawal; runtime release also clears before resource disposal. The SAME zipper construction owner is captured into existing phase-aware abort custody before fallible descriptor admission (`:1088-1101`), with transferred constructor ownership cleared on candidate transfer. Read all seeded/unseeded preparation/activation refusal, terminal-order, root-drain and old-reader controls in `primary_review_repairs_test.go`. |
| R2: closed iterators retain refunded image and pooled child | FIXED. `tree/primary_iterator.go:241-253` consumes the child once and zeroes private directory/delta/pager/callback/scratch aliases before child Close, retaining Domain and sticky error observations. Backend bound Close clears `inner` before owner finalization (`snapshot_iterator_lifetime.go:169-175`). Direct and embedded owned controls inspect aliases, close twice, retain closed handles, advance both slots/parents and drain the original root. |
| R3: ReadAll intermediate/final scratch overlap | FIXED. One preadmitted 4141-byte buffer is allocated and passed to `io.ReadFull` (`primary_joint_swap_v6.go:380-389`). EOF/UnexpectedEOF normalize short reads; 4141 bytes still exceed the decision maximum and are rejected. Existing physical close/failure custody remains. Controls cover 4096/4140/4141/4142/8192 bytes and refusal before proof allocation. |

These local resolutions do not override R4/R5 elsewhere in the resulting foundation.

## Coverage and limits

This is a continued independent review of the resulting whole foundation, with the prior contract tracing recorded in `review-f0415773.md` and renewed adversarial depth across every one of the 35 successor paths. The resulting base-to-head diff is 524 files, 85,041 insertions and 5,126 deletions; this is not a claim to have read every line. The exact path list is retained in the companion JSON.

- Refreshed exact-head tracked policy inventory: root AGENTS, TreeDB AGENTS and CONTRIBUTING apply; the separately scoped HashDB AGENTS has no changed HashDB path. Read task-local mergeable skill, its internal review fallback, Go performance skill and its gomap reference. Prior whole-foundation tracing covers the 19-group allocation inventory, SAME allocator/transaction/publisher, current/capsule/embedded-parent independent physical custody, persistent vlog reachability, COW/CWAL basis finalization, shared cleanup, offline pair/COMMIT and bounded shared-base vacuum. Those source bytes remain in the successor except the reviewed changes.
- Read all successor production diffs and adjacent capture/publication/release callers; reviewed every successor test/docs/CI diff. R4/R5 came from following the changed lifetime contract into pre-existing sibling callers, beyond the 35-path repair list.
- Checked the new platform helpers call the actual unsupported operation, demand its typed refusal, and retain pair/state/charge/no-private-artifact assertions. Supported platforms retain the original success/fault/custody oracles. Exact retained LOCK closure occurs only after fatal-custody/no-refund/repeat assertions, as modeled process shutdown. Windows runtime qualification remains unavailable locally.
- Crash fixture callbacks now serialize model mutation and use an observer shutdown barrier. Chunked checkpoint accepts typed recovery poison only after a nonnil captured actual AfterMetaWrite/ResourceMeta image; otherwise the injected cause remains required. Variant roots and recovery expectations remain unchanged. No generic production lock or timeout increase was introduced by those test changes.
- Nil-reader Tree Reset zeroes the complete Tree and restores its two selected inputs. Ordinary local DATA Pager.Get reaches the SAME mapping kernel; PRIMARY bank, immutable read-root image, fallback, metadata-guard and finite-work paths still route through their existing accounting/locking. Snapshot cache cleanup clears active backing once and separately scrubs the populated inline alias after overflow. Exported Snapshot/iterator handles remain unpooled. These source observations are not measured speedups.
- Owned empty Sets remain pinned for exported/bounded/native captures. The private one-shot known-empty path gives its reader no Set/Manager edge and retains the documented legacy DBState/empty wrapper only until putRead. Canonical legacy DATA allocations were not treated as universally admitted to PRIMARY quota. R4 concerns actual newly mutable/scrubbed Set lifetime; R5 concerns the newly introduced current PRIMARY edge.
- Nativewire diagnostic changes record only existing polls and report after failure; request/Raft deadlines and retries are unchanged. The two prior Windows deadline failures remain unresolved diagnostics. Native actual producer/Q32 and final Dgraph qualification remain separate successors.

## Evidence inspected, not reviewer executions

Read `ordinary-repair-work/HANDOFF.md`, `candidate.json`, `batch-inventory.json`, `failure-disposition.md/json`, staged product patch, metadata results, selected raw result lines and source-binding receipts. The candidate records 10,240 frozen source files; only CI discovery fingerprint metadata differs from the product freeze. Reviewer revalidated exact head/tree and clean candidate status.

| Receipt | Honest scope |
|---|---|
| `review-repairs-03` | Recorded PASS for R1/R2/R3 controls; raw named pass leaves inspected. Earlier review-repairs-01/02 RED attempts remain recorded. This batch is not the final manifest: inventory enumerates later paths. |
| `lifetime-repairs-01` | Recorded DB/tree/pager PASS, raw package results inspected. Later db.go, one-shot test and inventory-doc changes are explicitly listed. |
| `platform-repairs-02`, `public-repairs-01` | Recorded focused DB/public PASS. Source-binding exceptions remain listed; neither is relabeled a final hosted platform gate. |
| `race-repairs-02` | Final-manifest count=2 race receipt PASS, including negative lookup and both crash families; package result 30.331s inspected. It does not reproduce or dismiss R4. |
| `pager-tree-full-01`, `empty-sidecar-controls-01` | Final-manifest package/empty-wrapper/all-eight-sidecar controls PASS; raw package/named-shape outcomes inspected. |
| Windows DB/public/nativewire compilation | Final-manifest recorded compile-only PASS; no Windows runtime or deadline-failure resolution claimed. |
| Root metadata | One inventory refresh and five subsequent checks recorded exit zero; CI-impact diff changes only discovery fingerprint. Reviewer did not rerun metadata checks. |

Coordinator-provided broader context, not new-head proof: f041 full normal completed 83 package passes/21 no-tests/2 failed packages (DB fixtures and Unix path; exact short-TMP Unix rerun passed). f041 hosted had 35 passes/17 failures and raw Get +13.09%, seek +6.54% RED, mapped by the repair disposition. Exact a8c hosted CI/raw-path performance were pending at this review. Host185 quietness preflights produced zero qualifying timing samples. Prior short host111 guards do not qualify this head. No current-head CI success, performance acceptance, mergeability or merge authorization is inferred.

Final coordinator relay: a8c `stable-legacy-public` failed its foreground-p99 variability guard (CV 0.24501479 exceeds 0.10); all ten operations completed without abort and public exposure/error classifications passed. Raw artifact is `performance-185/a8c52-hosted`. This is an unresolved variability gate, not an established latency regression or a waiver. Other hosted CI remains ongoing. Coordinator also traced `ValueReaderForState` callers in caching and template training/extraction to live backend Snapshots with deferred Close; those caller checks are coordinator evidence, not additional reviewer source reads.

Reviewer commands were read-only Git/source/evidence inspection and artifact generation. R4/R5 are source blockers independently of missing tests; focused green receipts cannot waive them. Repair the coherent lifetime/current-root inventory, renew exact-head review, and finish applicable hosted correctness/platform/race/performance gates before readiness can be claimed.

Named architecture disposition: **Shared Set lifetime and current-root ownership architecture**. Shared ordinary/native final Set cleanup clears mutable backing, so the repair must inventory every `state.Store` publisher and every consumer of `state.ValueLogSet` across lock release. Publishers obey `rootReuse` before `db.mu`, except proven initial/offline exclusion. Consumers acquire the exact Set under publication exclusion before unlocking and release it after the last use. Preserve final alias scrub; do not add a speculative pin after unlock. Topology-only copies preserve the SAME `primaryRoot` current ownership edge without adding or withdrawing it. `DB.State` remains shallow borrowed metadata: pointer-bearing readers require a live Snapshot/protected capture, and internal `db.State` tree readers must convert. The safe exceptions are live Snapshot-backed readers, explicitly retained offline Sets under exclusive ownership, initial unpublished states, and scalar-only basis checks, as distinguished above. This named disposition is root’s confirmed repair decision and agrees with R4/R5; it does not change this exact-head REJECT.
