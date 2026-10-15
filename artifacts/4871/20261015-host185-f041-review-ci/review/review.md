# Independent ordinary foundation review

```text
review_mode: INTERNAL_SUBAGENT_REVIEW
reviewer: /root/ordinary_review (fresh-context independent reviewer; no construction role)
model: gpt-6.1-sol
reasoning_effort: high
repository: snissn/gomap
pr: 5127
base_sha: 5bca87ffe6e1bfba2d693264d2dc82936adc234f
head_sha_or_manifest: f0415773f13a522dea5c1debdaecba09d15585dd
head_tree: 2a482daae31337697dedf42329e2597c344b3226
disposition: REJECT
candidate_edits_by_reviewer: false
reviewer_runtime_campaigns: none
unavailability_evidence: Workstream requires exact GPT-6.1 Sol routing. Hosted manual review cannot pin that model; it was not requested. No hosted failure, bot response, quota event, or hosted review is invented. Parent reports fresh inventory contained no existing Codex requests/findings/threads.
```

The frozen candidate has three source blockers. This disposition does not depend on pending CI or timing evidence. Repairs require renewed review bound to the successor SHA. The reviewer edited only this review directory, never candidate source, Git state, GitHub, or another worker's lane, and ran no tests, builds, benchmarks, or expensive validation campaigns.

## Blocking findings

### R1 — P1: Cache publication precedes successful read-root activation and can outlive its charge

Location: `TreeDB/db/root_publication_activation.go:794-798`.

`NewPreparedPrimaryReadRootCertificateV6` admits the prepared wrapper and DATA certificate onto the private `cert.Reference()` before creating or reusing the proof (`TreeDB/internal/rootpublication/primary_data_certificate_v6.go:63-86`). The caller immediately stores the proof in `runtime.primaryDataCertificateV6`. Preparation still has fallible root-descriptor admission, member admission, runtime capacity reservation, visibility binding and candidate creation ahead of it (`root_publication_activation.go:815-842,956-991`). Activation itself can also refuse.

Trigger: prepare a genuine new DATA certificate, then refuse one of those later stages and successfully abort the original private constructor. Last-root removal clears the bank and refunds its metadata (`TreeDB/internal/primaryarena/arena.go:793-801`), but the runtime still references the new proof until another successful preparation or terminal runtime release (`root_publication_activation.go:1983`). The proof retains the original generation, pager source and candidate membership; the membership includes a candidate and generation pointer (`TreeDB/freelist/candidate_root_membership_v1.go`). Consequently an aborted new proof and its outgoing aliases survive without their root-owned admission. A failed preparation also replaces the previously accepted cache despite never becoming visible.

Correction: publish the cache only after the SAME transaction's successful visibility activation, with the accepted current root already owning the proof allowance, and before old-state post-work can withdraw the old root. Keep the cache unchanged on every preparation/activation refusal. Explicitly detach the cache before terminal withdrawal of the last supplying root/arena charge, or give the runtime independent correctly sized custody through that point. Today `DB.closeAfterHooks` releases durable-root resources and closes indexes before `rootRuntime.release` clears this cache, so terminal ordering needs a control as well.

Successful reuse is not itself an undercharge: the constructor reserves proof bytes on every new root even when it reuses the identical proof pointer. The previous current root survives activation and the accepted replacement carries its own conservative allowance before `db.go:4370` drops `post.oldState`. No new proof allocation is required merely for reuse. Preserve that coverage while repairing cache publication and terminal detachment.

Needed controls: force a refusal after first certificate creation; abort and drain the original private root; assert the runtime cache is unchanged or nil and all live added proof/backing capacity remains covered. Exercise successful PRIMARY-only reuse, real DATA replacement, old readers and terminal disposal. The inspected `TestPrimarySharedTransactionV6ReusesExactDataCertificate` covers successful exact reuse/replacement/old Snapshot readability, not this late refusal.

Disposition: OPEN; coordinator acknowledged this as an in-scope blocker. No reviewer repair or runtime reproduction was performed.

### R2 — P2: Closed PRIMARY iterators retain refunded directory backing and pooled-child aliases

Location: `TreeDB/tree/primary_iterator.go:241-248`.

`primaryIterator.Close` sets flags and closes only `p.base`. It leaves `p.directory`, `p.delta`, `p.slot`, `p.baseTree`, `p.readLeaf`, `p.leafScratch` and `p.base` attached. `PrimaryDirectoryView` borrows its original `[]byte` (`TreeDB/node/primary_directory_v1.go:58-64,153-180`); the ordinary V6 logical root owns a heap image, not merely a scalar page identifier. The base close also returns a non-owned Iterator to the global pool while the closed PRIMARY wrapper keeps its pointer (`TreeDB/tree/iterator.go:1064-1077`).

Trigger: use a direct backend Snapshot iterator over a PRIMARY root, close the iterator and Snapshot, retain the closed exported iterator, then advance enough publications for the old current/slot/parent roots to drain. The backend `snapshotBoundIterator.Close` retains its `inner` field (`TreeDB/db/snapshot_iterator_lifetime.go:163-188`). Therefore the closed caller handle still holds the PRIMARY directory image and associated private aliases after arena last-root removal refunds their capacity. The owned projection variant embeds the same PRIMARY iterator in its single allocation and uses the same Close implementation. The recent caching wrapper's `inner = nil` change does not cover the direct backend path.

Correction: consume the base exactly once, detach all private borrowed data, node, pager/reader callback, scratch and base aliases before returning to owner finalization, and keep only the closed observation state and any contractually required Domain/error result. A closed wrapper must not alias a pooled child after returning it. Alternatively also detach the backend bound wrapper's inner under its applicable lifetime contract, but the shared PRIMARY iterator should scrub its own state for owned/direct callers.

Needed controls: retain a closed direct backend iterator while its original Snapshot and superseded logical root drain; inspect its private fields/backing retention, original root accounting and borrowed callback detachment. Cover the owned projection variant and repeat Close. This is a retention/admission finding; it does not claim closed iterator methods intentionally dereference the stale data.

Disposition: OPEN; coordinator acknowledged this as an in-scope blocker. No reviewer repair or runtime reproduction was performed.

### R3 — P2: Recovery marker admission omits ReadAll's simultaneous intermediate backing

Locations: `TreeDB/db/primary_joint_recovery_proof_v6.go:59` and `TreeDB/db/primary_joint_swap_v6.go:380`.

The recovery proof reserves one `AllocationCharge(4141)` marker image, then uses `io.ReadAll(io.LimitReader(proof.marker, 4141))`. The pinned Go 1.26.3 implementation was inspected directly at `toolchains/go1.26.3/src/io/io.go:709-751`: it allocates and retains multiple intermediate chunks, then allocates a final image and copies the chunks into it. The chunk-vector backing is additional storage too.

Trigger: an approximately 4 KiB or larger malformed COMMIT marker, within the 4141-byte input cap. Chunk storage and the final image overlap before decoding refuses the marker. Their total capacity exceeds the single-image allowance, and independently rounded allocations increase that requirement. The source correctly caps logical input and ultimately rejects malformed images; neither fact preadmits this introduced scratch overlap. Independent unused capsule image reservations cannot substitute for accounting the actual marker scratch.

Correction: allocate one fixed, preadmitted 4141-byte buffer and read directly into it with ReadFull/ReadAt semantics, normalizing EOF/UnexpectedEOF while preserving oversize rejection; or preadmit the full possible intermediate/final/vector overlap before ReadAll. Keep the original proof's failure custody and exact close behavior.

Needed controls: near-limit malformed and oversized markers under the actual recovery proof owner, refusal before allocation when capacity is insufficient, and exact cleanup after decode failure. Use the pinned toolchain's allocation behavior, not assumptions from an older ReadAll implementation.

Disposition: OPEN; coordinator acknowledged this as an in-scope blocker. No reviewer repair or runtime reproduction was performed.

## Review coverage and source conclusions

This was an adversarial review of the complete ordinary foundation candidate and its critical contracts, with selective deep tracing across the full 19-group inventory. It was not a claim to have inspected every line of all 514 changed files, nor a native producer/Q32/final-graph qualification. Full diff filenames/statistics and scoped policy were inventoried before tracing the following paths. Large combined command output was sometimes truncated; the coverage below names source seams actually inspected rather than claiming truncated files were read in full.

| Area | Paths and claims actually inspected |
|---|---|
| Policy and scope | Exact-head root `AGENTS.md`, `TreeDB/AGENTS.md`, `CONTRIBUTING.md`; tracked policy path inventory; task-local mergeable skill, internal review fallback, Go performance skill and gomap allocation reference; issue #5111 capture; `ordinary-primary-allocation-5111.md` inventory/status; PR draft and completion handoff. |
| SAME producer/allocator/transaction | `db/root_publication_activation.go` visible install, entire visible candidate preparation, publisher, commit and runtime release; `root_publication_build_group.go` relevant transfer paths; `primary_transaction_construction_v6.go`, `primary_terminal_completion_v6.go`, `primary_data_certificate_v6.go`; allocator generation obligation diff, membership and generation snapshot classifier. Ordinary PRIMARY-only writes reuse the actual DATA generation; DATA changes select the original allocator's actual prepared candidate. Constructor/candidate custody is not a parallel publisher. |
| Immutable logical roots versus slots/images | `db/primary_capsule_runtime_v6.go`, `primary_capsule_recovery_v6.go`, relevant `durable_primary_runtime_v5.go` construction/commit paths; `durable_root_recovery.go` selector; `rootpublication/primary_capsule_v6.go` projection/validation sections; `primaryarena/read_root_v6.go`, arena root removal and retention helpers. Both fixed slots and embedded parents have separate physical closures and images. Selector eligibility remains the two fixed current capsules; an embedded parent does not become a third selector. |
| Physical completion and failure custody | `primary_arena_owner_v5.go`, `primary_runtime_disposal_work_v6.go` phase/edge paths, `original_primary_runtime_cleanup.go`, Snapshot release/finalization, index generation and ghost/wrapper completion, retainedalloc owner; resource graph/set release work, physical-release portions, original cleanup custody; pager physical-close withdrawal/quiescence/unmap phases. Original first failure freezes payload/cause; ordinary callbacks do not establish native finite-work qualification. |
| Snapshot and iterator consumers | `db/db.go` Snapshot fields, capture, one-shot/public paths and finalization; `pools.go`, `snapshot_primary_retention.go`, `snapshot_read_lifetime.go`, `root_snapshot.go`; backend bound iterator and new tree PRIMARY/owned iterator. Inline first root-tree cache lives in the containing Snapshot charge and overflow clearing was inspected. Escaped Snapshot handles remain unpooled. R2 is the outstanding private iterator scrub gap. |
| COW/CWAL | `caching/cow_basis_completion.go`, `cow_cut.go`, relevant `cow_flush.go`, `cow_checkpoint.go`, `cow_maintenance.go`, maintenance writer fence in `caching/db.go`; `db/command_wal_publish.go` selected/fallback equivalence diff. Backend basis capacity is tied to actual Snapshot completion. Post-refresh settlement executes only after successful operation/refresh, before the SAME maintenance writer fence is released, and refreshes again after settlement. No per-foreground checkpoint was introduced in the inspected routes. |
| Offline pair and namespace authority | `primary_joint_swap_v6.go`, `primary_offline_pair_v6.go`, `primary_joint_recovery_proof_v6.go`; current/parent recovery validation and exact stable-child identities. COMMIT rollforward retains exact DATA/PRIMARY pair identity, validates both canonical outputs before deleting the decision, and separates original close failure from metadata uncertainty. R3 is the marker scratch gap. |
| Vacuum/M4 and source configuration | `vacuum_shared_base.go`, relevant `vacuum_online.go` source-root capture and handoff; fixed four-entry shared-base cache, delayed retirement and final reachability walk; `compact_storage.go` public/backend settlement. Latest M4 matrix diff uses three genuine CompactIndex DATA publications and asserts generation/page/debt changes; the 40% shrink and 8x output guards remain. Offline format fixture diff preserves actual source configuration and documented Windows fail-closed behavior. |
| Reopen/crash oracles | Source inspected in `primary_slot_custody_v6_test.go`, successful certificate-reuse test, relevant `power_loss_oracle_test.go` and `power_loss_certification_stale_build_test.go` diffs, offline format and M4 fixture diffs. Actual PRIMARY metadata/DATA dependency fences replace invented fixture cuts. Saved V5 and V6 stale ledgers remain distinct. No source evidence was found that the inspected 40%/8x or actual-root assertions were weakened to force success. This is not a runtime certification result. |
| Empty closure | `prepared_root_publication_profile.go`, `valuelog/manager.go:2390-2415`, `publication_set_test.go`, `publication_absent_closure_test.go`, reported failing fixture bodies. Registry limit validation, live-file test and acquisition share one lock. Nil means no live physical file edge; exported CurrentSet preserves its nonnil contract. No lost actual value pointer was established by those inline-only failures. |
| Docs and CI binding | Changed spec inventory/diffs for architecture, contracts, COW cache publication, recovery, storage format, vlog lifecycle, verification and write durability; inventory doc explicitly leaves integrated qualification open. `.github/ci/ci_impact.json` diff changes discovery fingerprint/go module blob bindings; metadata results record all five checks passing. Reviewer did not rerun these checks or independently inventory live hosted jobs. |

Canonical legacy DATA allocator graph allocations were not treated as universally enrolled in the PRIMARY quota. R1-R3 concern newly introduced proof, private iterator aliases/image backing and recovery scratch. The separate native producer, actual Q32 command runtime and final Dgraph gates remain open successors. No claim here expands this PR to cover them.

## Checks and evidence inspected

Reviewer commands were read-only `git rev-parse`, `git status --porcelain`, `git ls-tree`, `git diff --name-only/--stat/--numstat`, targeted `rg`, `sed`, `cat` and line-numbered source reads. Head was revalidated as exact f0415773 and candidate status remained clean at review completion. Reviewer-created files are confined to `ordinary-review/`.

Read evidence (not reviewer executions):

- `ordinary-final-candidate.json`: exact f0415773/tree 2a482daa; frozen product/test source 04cc8d02; only CI fingerprint metadata differs.
- `ordinary-final-preflight.json`: 486 changed Go files gofmt PASS; 10,237 normal-gate source files matched freeze; completion then pending.
- `ordinary-final-metadata/results.json`: five recorded metadata/CI contract commands exited zero, including CI inventory, TreeDB contract and Windows core-headroom checks.
- `ordinary-work/ordinary-completion-HANDOFF.md`: public/caching/collections normal passes plus focused lifetime, crash and M4 receipts; full normal gate still active at handoff. These receipts were not relabeled hosted exact-head CI.
- `ordinary-work/logs/normal-tree-22/command.json`: pinned Go1.26.3, `go test -json -p=1 ./TreeDB/... -count=1 -timeout=30m`, GOMAXPROCS=4, task-root tmp/cache/module paths, original root-owned process. Source equivalence to frozen product/test bytes is recorded by the candidate/preflight evidence.
- `ordinary-work/logs/normal-tree-22/db-new-failures.json`: exact raw test events show `TestInlineCommitSkipsValueLogRefresh` failing at currentset_refresh_test.go:625 and both v1/dpm2 leaves of `TestDurableRootPublicationAcceptedWaitFailureRunsPostWork` failing at durable_root_open_test.go:635. Both fixtures demand a nonnil Set after inline-only seeding. Correct fixtures must retain the original no-discovery/explicit-refresh assertions and use an actual pointer when testing Set ownership. They remain failing gates until corrected and rerun; this source explanation is not a waiver.
- Parent's latest progress report: 48 packages PASS, 18 no-tests, DB FAIL, remaining run ongoing. Not independently recomputed by this reviewer.
- At final handoff the coordinator confirmed all three source blockers, reported the full normal run had reached nativewire with the same two top-level DB fixture failures, and will repair only in a successor while leaving f041 immutable.
- Historical host111 short cost guard is explicitly not f041 proof. Host185 preflights refused unrelated busy services before collecting samples. Current hosted cost/CI evidence remains coordinator-owned and was pending when this review was finalized.

No live source finding was inferred merely from absent tests. Conversely focused receipts and pending hosted work cannot discharge the three source defects. Full normal/race, platform/Windows, format/fault, complete storage shrink/output and exact-current-head raw/default-path costs remain separate applicable gates. This REJECT grants neither readiness nor merge authorization.
