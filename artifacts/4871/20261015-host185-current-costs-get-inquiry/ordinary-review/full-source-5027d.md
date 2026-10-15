# Internal full-diff source review: PR5127 / ordinary #5111

**Conditional source ACCEPT; overall HOLD.** No demonstrated ordinary source correctness blocker was found in the inspected families. The actual current raw-path GetVersioned gate fails (+10.340872% paired; 443.1 to485.35ns/op, zero B/op and allocations), so this is not an overall acceptance or mergeability decision.

Exact base `5bca87ffe6e1bfba2d693264d2dc82936adc234f`; candidate `5027dc559f83dee04317910169c48d277fcce72f`; tree `a7710b6b41ebc62328a5282c959910193ce4cd0f`. Worktree `/mnt/fast4tb/gomap-mvcc-4871-20261015/ordinary-repair` is clean. Full diff: 539 paths; 539 files changed, 87255 insertions(+), 5311 deletions(-).

This is an early independent INTERNAL source audit. Root owns all construction, source/Git/GitHub writes, runtime authorization and integration. No children and no tests/builds/benchmarks or ELF execution were used. The machine-readable companion binds every changed path to its candidate Git blob and worktree SHA256.

## Coverage and inspected mechanisms

All539 paths were inventoried and risk-sorted. 123 existing source/tool paths received direct body or changed-hunk inspection, including a few unchanged call-path dependencies. Large files were examined by relevant functions and transitions; this report does not claim every line of every changed file was read. Supporting tests/fixtures/docs and bounded/native-only additions are explicit in the JSON inventory.

| Family | Changed paths | Review result |
|---|---:|---|
| authority-publication | 245 | Directly followed ordinary DATA certificate, primary directory construction, coordinator finish, two real capsules, barriers, and independent current/parent custody. No demonstrated correctness defect. |
| concurrency-platform | 1 | Inspected lock/callback release ordering on selected paths, typed unsupported Windows namespace/mapping operations and snapshot/iterator guards. Source inspection does not replace current-head race/Windows CI. |
| native-protocol | 27 | Inventory includes bounded/native continuation prerequisites and shared ordinary nil-work entry points. Actual native producer, terminal Finish and Q32 are separate #4878; no native qualification asserted. |
| physical-lifetime | 34 | Followed snapshot/iterator cuts, original physical owner, generation/manager wrapper borrows, callback completion and error custody. Successful cleanup/refund is distinct from consumed logical edge. |
| public-cow | 36 | Actual command-WAL + cow_btree route selects PRIMARY backend; basis refresh and physical ownership follow maintenance fence; cached retained snapshot/iterator uses original cut and original callback. |
| recovery-format | 13 | Decoded slot/header/UUID/checksum/CRC and physical closure checks; startup first-failure payload; unpublished capsule identity rebind; exact pair COMMIT recovery. No promoted embedded-parent eligibility. |
| retained-allocation | 92 | All 19 named allocation families audited at owner/constructor/release seams. This is source evidence, not a complete proof that every compiler closure/allocation equals a byte accounting formula. |
| support-tests-docs | 32 | Inventory includes inherited tests, fixtures and docs, generated CI metadata, profiling docs, gate harness and dependency pin. Tests/docs are supporting assertions, never evidence of runtime success by themselves. |
| vacuum-snapshot-gc | 14 | Shared DATA base copied once and exact root deltas rebuilt; retirement waits for all four closure construction and live reachability; recoverable roots revalidate before deletion; physical cache incarnation changes with DATA/PRIMARY identities. |
| value-log | 45 | Persistent File/Manager wrappers retain original first-error cleanup; exact state Set lifetime is independent; GC/rewrite excludes recoverable current/slot/parent closure and reader pins. No old-segment-is-WAL assumption used. |

## Allocation inventory: all19 named groups

The design inventory was used to locate claims, then constructors, owner transfer and release logic were inspected. Historical caveats in that design document were not treated as current defects. These are source conclusions, not a claim that compiler allocation behavior or complete byte accounting was measured here.

| Group | Source anchor | Mechanism checked |
|---|---|---|
| 1 Arena/read-root baseline | `TreeDB/pager/primary_read_root_v6.go:1` | Fixed metadata table, exact growth replacement admission, immutable root address registration and removal before refund. |
| 2 Banks, chunks and custody | `TreeDB/internal/primaryarena/retention.go:1` | Full chunk/group/control storage is charged before construction; bank edges and original failed queues retain their actual arena. |
| 3 Pager mappings and growth | `TreeDB/pager/grow_work.go:1` | Fresh original pager identity/count/dirty revision checked under growth locks; reserve before mapping effect; physical close retains first failed operand. |
| 4 Prepared DATA root and allocator activation | `TreeDB/db/root_publication_activation.go:408` | Original prepared allocator candidate binds DATA generation/freelist certificate; actual DATA work controls generation advance; activation precedes publication. |
| 5 Transaction, member and prepared state | `TreeDB/db/root_publication_activation.go:1050` | Original construction transaction becomes the visibility owner; member/state/callback contributions precede retention and lock handoff. |
| 6 Runtime visible/debt queues and slots | `TreeDB/db/primary_runtime_metadata.go:17` | Full replacement capacity admitted while old backing remains live; obsolete aliases cleared before old refund; untouched physical slot custody transferred. |
| 7 Capsules, recovery and snapshot rebind | `TreeDB/db/primary_capsule_recovery_v6.go:18` | Exactly two real slots are eligible; four possible current/parent closures retain independently; fixed scratch overlap preadmitted and rebound copies reselected. |
| 8 Seal, callback and prefix custody | `TreeDB/db/primary_capsule_runtime_v6.go:28` | Fresh finite parent and prepared proof form same-original promotion; capsule publication follows DATA/PRIMARY content and namespace barriers. |
| 9 Failure and snapshot physical owners | `TreeDB/db/snapshot_primary_retention.go:13` | RAM root and physical arena owner are distinct edges; snapshot keeps exact owners through readers/iterators, cleanup, scrub and completion. |
| 10 Startup, COW handoff and dictionary capture | `TreeDB/caching/cow_basis_completion.go:12` | Original startup generation owns LOCK/manager/partial pager failures; paired owner enrollment activated on successful capture; backend basis refund follows actual snapshot callback. |
| 11 Namespace files and proof operands | `TreeDB/db/primary_offline_pair_v6.go:1` | Parent/file/proof storage admitted before open; same original handles persist through close refusal; pair decision uses exact parent-relative child identities. |
| 12 PRIMARY token and owned operations | `TreeDB/db/stable_pager_owned_operations.go:36` | Admitted callback backing owns original snapshot, pager and physical owner; descriptor consumption and physical completion are independent. |
| 13 Namespace token, lease and sync cleanup | `TreeDB/internal/rootpublication/resource_original_cleanup.go:91` | Original cleanup custody preexists fallible construction; actual namespace parent/proof outcomes survive producer return and retain governing contribution on refusal. |
| 14 Identity/link registry capacity | `TreeDB/internal/rootpublication/registry_capacity.go:68` | Full open-addressed replacement and key payload admitted before make/clone; retained physical identity pins protect exact observed files. |
| 15 Dependency directories and leases | `TreeDB/db/primary_bank_construction_v5.go:1` | Read-root/dependency banks retain original physical owner; exact-directory serialization includes field/logical data and imports under same owner. |
| 16 Manifest codec, loading and rebinding | `TreeDB/internal/rootpublication/resource_manifest_load.go:31` | Canonical lengths/counts checked before allocation; parser/scratch overlap admitted; exclusive unpublished rebinding transfers storage before prior contribution refund. |
| 17 Builder, immutable set, kind/import storage | `TreeDB/internal/rootpublication/resource_builder_allocation.go:1` | Private arrays admitted before token claim; immutable kind backing retains real directory/view edges; selected constructors require common actual owner. |
| 18 Rope, logical/RID/history/obligation backing | `TreeDB/internal/rootpublication/resource_obligation_allocation.go:1` | Actual string clones, arrays and path-copy nodes charged before creation; exact membership belongs retained backing; duplicate integrity conflicts rejected. |
| 19 Release queues, scopes and diagnostics | `TreeDB/internal/rootpublication/resource_graph_release_work_v6.go:1` | Original intrusive cleanup frames retain ancestors through descendant failure; actual release clears borrowed aliases before refund; admitted diagnostics borrow real immutable backing. |

## Source findings and claim boundaries

No concrete source correctness finding survived the family audit. In particular:

- `internal/rootpublication/primary_capsule_v6.go:215` considers only the two real slot images. `:245` validates independent current/parent DATA and PRIMARY closure. Embedded parents retain reachability and rollback proof; they do not become extra recovery-eligible slots.
- `db/root_publication_activation.go:1050`, `db/durable_primary_runtime_v5.go:504` and `db/primary_capsule_runtime_v6.go:28` keep the original construction/certificate and predecessor custody through content barriers, selected-slot publication, acknowledgement and actual cleanup. PRIMARY-only ordinary publication can preserve DATA generation.
- `db/snapshot_primary_retention.go:13`, `db/db.go:2262`, `db/stable_pager_owned_operations.go:36` and `db/original_startup_generation.go:1` separate root, physical owner and containing wrapper edges. Cleanup failure remains on original owners; a callback return or consumed descriptor does not itself refund actual physical debt.
- `db/vacuum_shared_base.go:64` waits for all root constructions and verifies live reachability before freeing shared-source retirements. `db/primary_offline_pair_v6.go:1`, `db/primary_joint_swap_v6.go:119` and `db/primary_joint_recovery_proof_v6.go:1` carry exact DATA/PRIMARY identities and parent barriers through pair installation and interrupted roll-forward.
- `db/recoverable_root_set.go:235` captures current roots plus both physical slots and parents with exact resource/PRIMARY bank retention; `:902` revalidates original state/coordinator/durable basis before destructive maintenance. `db/vlog_ref_incarnation.go:38` binds derived counts to physical DATA/PRIMARY incarnation, including sequence-preserving vacuum.
- `tree/primary_directory.go:1` resolves selected components directly; `tree/primary_iterator.go:1` performs sorted base/component merge with replacement/absence semantics. The newest wrapper change defers allocation until the actual PRIMARY operand is established.
- The ordinary route does not inherit an assertion that actual native producer/Finish/Q32 is complete. Bounded native-only gaps are separate #4878 unless they demonstrably affect shared ordinary behavior; no such shared bug was demonstrated in this audit.

Potential concerns checked and not promoted to findings: failed manifest rebind mutates only an unpublished candidate that is unwound; sorted recovery inputs retain a separate actual-resource cleanup array; Batch.SetOps appends/normalizes instead of replacing prior operations; generic ungoverned helper constructors are not automatically on selected admitted routes; temporary template adapters transfer real snapshot cleanup to owned operations rather than retaining themselves as physical authority.

## Current evidence limitation and follow-up

The retained current packet establishes a raw-path failure. Previous72e timing PASS cannot qualify5027d. The Get benchmark is in package `db` (`db/conditional_kv_contract_bench_test.go:12`) and opens the internal DB; it bypasses TreeDB/public.go and public COW routing. The static causal follow-up and retained ELF inputs are recorded under `static-get-5027d/`; no new runtime work has been authorized here.

Read-only static comparison: all four current/prior retained db ELFs embed Go1.26.9. The current/prior baseline ELF is byte-identical.31 selected Get/benchmark/tree/registry/snapshot symbols in current versus72e candidate have identical instruction shape after direct branch/RIP relocation normalization; their entry addresses move -64bytes (same modulo64 alignment). This does not establish executable semantic identity, cache behavior, cost equality or a noise explanation.

## Explicit limitations

- Internal source review only; not hosted AI review, final fallback gate, mergeability proof or merge authorization.
- No source mutation, build, Go test, benchmark, load or measured ELF execution performed. Review artifacts and read-only ELF metadata/disassembly only.
- All 539 changed paths inventoried. Direct inspection list is explicit; inventory-only files are not represented as exhaustively read.
- No independent rerun of prior validations; parent-reported current 11 focused checks and whole-process wrapper144-to0 were not executed here.
- Native actual producer/Finish/Q32 #4878 is excluded from ordinary acceptance; shared ordinary behavior was considered.
- Current performance failure is binding and unresolved; static source review cannot waive it. Current full CI maturity remains parent-owned.
- M0 e88 CV0.517574 failure is preserved as unknown cause; predecessor cost results do not qualify current head.

The JSON companion contains the complete539-path inventory, explicit direct-inspection list, source hashes, family dispositions and19-group mapping. This report is local and must not be represented as a hosted AI review or final mergeable-readiness receipt.

## Causal follow-up and bounded cost option

Root authorized and completed6 original retained-ELF CPU profiles after this source audit. The reviewer decoded those profiles offline; no reviewer runtime was executed. Detailed route, original hashes, disjoint flat sample partitions and the panic-preserving capture-exit defer option are in `static-get-5027d/causal-inquiry.md` and `get-profiles-5027d/partitioned-flat.json`. These local instrumented profiles do not qualify the current hosted gate or waive its failure. M0 current workflow completion was reported PASS by root; independent packet recomputation remains parent-owned.

A separate narrow checkpoint option is `TreeDB/internal/rootpublication/resource_entry_view.go:607`: generic `rangeStableResourceKindViews` currently allocates/sorts all kind keys even when there is at most1 kind. Directly visiting the sole existing entry would preserve canonical order and actual owner/diagnostic/custody semantics. Whole-fixture attribution identifies allocations at stableResourceKindsSorted, but no per-checkpoint byte/object reduction has been measured for this option. Root's current residual +12,313B/+27objects is preserved as cost evidence, not accepted by asserting required custody. Inline batch.Entry.Input and detached immutable diagnostics were inspected; no broad representation rewrite is proposed.
