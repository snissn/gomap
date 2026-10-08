# Ordinary PRIMARY allocation and custody inventory

This inventory binds the ordinary route extracted at
`d4cbf23f9333eed79718845d3ac253c5d7fc5513` and its completion changes.
The completion packet supplies the final source identity and command receipts;
this document is a source/caller inventory, not a performance or review result.

The stale-build publication witnesses are format-specific. The historical
DATA V1 witness (generation 8) is preserved under
`testdata/power_loss_legacy_stale_build_v1` with exact original source blobs and
replay receipt, and has zero current runner coverage. The saved PRIMARY V5
fixture uses the existing production V5 initializer, then the public NoWALFast
open/retry/read-only-reopen route. Its independently named witness selects
commit 4, predecessor 3 and actual generation 4 at DATA META sync. A fresh
NoWALFast producer uses PRIMARY capsules: its separate V6 witness decodes the
exact immutable retry slot (current 4, parent 3), records the original file
identity, and observes the subsequent same-original PRIMARY file fence.
Both active witnesses retain stale rejection before acceptance, predecessor
visibility, distinguishable retry, public reopen, slot and dependency horizon
assertions. The V6 evidence replay window starts after the verified seal, so
preparatory file syncs cannot satisfy the terminal cut. Public reopen runs the
existing `BeginEvidenceFromEnv` path with the exact current selector, trace,
stable image and recovered statistics. No META event is manufactured for V6.

The scoped diagnostic optimization ends its borrow only after copying consumed
kind/generation scalars. The span-native worker captures the existing leaf-span
slice instead of the larger preparation result; validation and preparation
summaries still consume the original result. Complete-operation measurements
must include both changes and the previously documented containing-result cost.

`retainedalloc.AllocationCharge` rounds each actual allocation separately.
`Owner.Add`/`AddPending` precedes allocation. Replacement reserves the complete
new capacity while the old capacity remains charged. Aliases and outgoing edges
are cleared before refund. The physical arena and original identity registry
are the fixed owner pair enrolled in the SAME COW budget; this is not a second
publication, retirement, physical identity or deletion registry.

| Group | Actual constructor/backing and caller | Capacity, aliases and final action |
|---|---|---|
| 1. Arena indexes/read roots | `pager/primary_read_root_v6.go`; `primaryarena/arena.go` | Initial bucket/pager capacity is in the arena baseline; entry and replacement buckets are admitted independently. Root removal scrubs the exact entry before refund. |
| 2. Banks/chunks/logical custody | `primaryarena/retention.go`, `arena.go`, `bundle.go`; `zipper/primary_directory.go` | Chunk, group, Claim/BundleClaim wrappers and PRIMARY entry/image scratch use the arena owner. Private wrappers cancel on original Step failure, detach arena/chunk/growth aliases on terminal completion, and refund once. Installed bank/group capacity survives every current/slot/reader edge. |
| 3. Pager lookup/growth | `pager/primary_read_root_v6.go`, `pager/grow_work.go` Growth construction | The same arena owner covers RAM lookup/dirty/verification/growth backing. Full replacement capacity overlaps old backing. Physical mapping closure precedes owner close. |
| 4. DATA prepared roots | `db/root_publication_activation.go`; allocator `PrepareCOWCandidateRetiringWithLimitsV1` | Existing DATA generation/prepared-root custody stays with the original allocator. PRIMARY IDs are excluded from DATA retirement. Bulk/manual DATA output acquires an owned PRIMARY construction at the same queued boundary. |
| 5. Transaction/candidate/member controls | `primary_transaction_construction_v6.go`; `db/primary_runtime_metadata.go` | SAME transaction owns construction and promotion; transaction/member/callback environments are admitted before allocation. Refusal leaves original private custody. |
| 6. Current/slot/retired arrays | `db/primary_runtime_metadata.go`, `primary_capsule_runtime_v6.go`, `durable_primary_runtime_v5.go` | Runtime arrays admit full new capacity, check size overflow and clear old aliases. Current, both slots, proofs and retired roots retain independent real bank/resource edges. |
| 7. Capsule/recovery/selection images | `primary_capsule_recovery_v6.go`, `primary_capsule_snapshot_rebind_v6.go`, `durable_primary_rebuild_v5.go` | Owned images, root/ref vectors and rebind scratch are admitted before make; selection copies actual eligible slots/parents. Output capacity is distinct from transient scratch and is transferred or cleared before refund. |
| 8. Seal/prefix/callbacks | `db/primary_runtime_metadata.go`, `primary_capsule_runtime_v6.go`; original transaction callbacks | Seal/prefix capacity remains pending until original callbacks complete. Ordinary synchronous report releases the constructed transaction, never an imported copy of callback authority. |
| 9. Unlocked retirement/failure custody | `db/primary_arena_owner_v5.go`, `primary_bank_construction_v5.go`, `snapshot_primary_retention.go`, `pools.go` | Physical owner retains exact failed generation, bank construction, operation or Snapshot through intrusive links in admitted original storage. Failed one-shot cleanup cannot enter the reuse pool. A bank pin and physical mapping edge are separate obligations. |
| 10. Startup/handoff/dictionary | `caching/cow_cut.go`, `cow_flush.go`; `retainedalloc.EnrollPair` | Successful startup alone activates the real producer. Temporary basis/dictionary capture adopts a borrow and detaches its governor at completion. Pair admission refuses before transfer; live producer growth remains governed after temporary borrows close. |
| 11. Namespace/file owner/proofs | `db/primary_arena_owner_v5.go`; `rootpublication/file_metadata.go` | Original parent handle/name/proof capacity is admitted before open/capture. Existing physical owner releases the original proof/parent, retaining actual failure operands instead of replacing them. |
| 12. PRIMARY token/operations | `db/stable_pager_owned_operations.go`; `resource_token_allocation.go` | Independent token descriptor and callback environment capacity is admitted. Actual operation/foreground/physical-owner edges survive public release and are consumed only on original completion. |
| 13. Namespace token/lease/sync | `resource_namespace*`, `resource_original_cleanup.go` | Namespace metadata and original sync/close operands belong to the original registry/physical owner. Admitted preborn cleanup backing stores actual failed payload without a failure-time allocation or second authority. |
| 14. Identity registry | `registry_capacity.go`; `IdentityPinRegistry` | Typed table cells, owned key payload and full replacement table are admitted in one atomic reservation before clone/rehash. Failed key admission preserves old table capacity/count/charge. Tombstone rehash preserves the existing high-water policy. |
| 15. Dependency directories/leases | `db/primary_dependency_v5.go`, `primary_bank_construction_v5.go`; `resource_directory_records_allocation.go` | Private bank-ref and child-first frame vectors are admitted before effects. Intrusive pre-Open leases transfer at adoption; failed cleanup retains original unfinished banks and closure on the same physical owner. |
| 16. Manifest codecs/load/rebind | `resource_manifest_allocation.go`, `resource_manifest_load.go`; `db/primary_capsule_snapshot_rebind_v6.go` | Immutable encodings, contiguous images, canonical reusable encoder page and decode/callback scratch are separately admitted. Full output/scratch overlap is retained through actual serialization/action, including refusal unwind. |
| 17. Builder/set/Clone/import/scope | `resource_builder_allocation.go`, `resource_set_allocation.go`, `resource_kind_allocation.go`, import helpers | Constructor success transfers only original input edges; failure leaves caller custody. Set wrappers and shared kind backing have different allocation edges. Released public wrappers are inaccessible and refund their own exact capacity while an active scope retains descriptor/token/handle/directory backing. |
| 18. Rope/exact RID/history/proofs | `resource_rope_allocation.go`, `resource_logical_allocation.go`, `resource_rid_allocation.go`, obligation/proof helpers | Persistent canonical logical/physical index is selected truth. Same-identity append updates canonical frontier while sharing the immutable old physical rope. The old rope frontier remains unchanged; last aliases release original token and backing edges once. |
| 19. Release buffers/outcomes/diagnostics | `primaryarena/release_queue.go`, `resource_original_cleanup.go`, `resource_diagnostics_allocation.go`, scoped views | Admitted existing nodes supply release worklists. Consumed references, deferred work, completed callbacks and cleanup debt retain their original distinct outcomes. Scoped selected diagnostics retain real backing; generic diagnostics retain their original post-release behavior. |

Paths in the table are relative to `TreeDB`, or to
`TreeDB/internal/rootpublication` for resource/transaction helpers. Group 3's
Growth constructor is charged already; claiming it was unadmitted would confuse
its admitted replacement backing with the missing Claim wrapper.

Ordinary DATA materialization passes `batch.New`/`SetOps` mutation input through
the existing DATA builder contract. The packet does not re-enroll the legacy DATA
allocation graph in the PRIMARY quota. Newly added retained PRIMARY controls,
metadata and PRIMARY-only scratch are governed; complete-operation allocation
measurements include the actual DATA builder/input cost. Feasibility merges
sorted key vectors directly, and component encoding reuses one admitted image.
The unused slice-returning `ClaimMetadataRangeOrdinary` helper is removed; real
private manifest/dependency construction uses caller-admitted bank-ref backing
through `ClaimMetadataBanksIntoOrdinary`.

## Causal corrections and discriminators

The three migration RED assertions observed wrapper aggregates or the old rope,
not the selected live closure. In both borrower cases the public wrapper refunded
exactly 160 bytes while the descriptor had one remaining scoped reference and
its original tokens/handles/directory remained live. Coalescing returned Len=1,
one physical identity with frontier 8, and canonical logical/physical entries
with frontier 8 sharing the same entry. The separately retained immutable old
rope correctly kept frontier 4. The coordinator accepted these discriminators;
the tests now require exact wrapper refund, inaccessible released owners, live
scoped closure and once-only final cleanup. No artificial debt or mutable shared
rope is introduced.

Separate actual defects were reproduced and corrected: refused key payload had
already replaced the registry table; old Snapshot finalization ran after its
physical PRIMARY arena closed; CompactIndex handed plain DATA output to a
PRIMARY-only construction boundary. The dedicated tests retain refusal/rollback,
old-reader, actual last-read physical close and clean reopen invariants.
The public CompactIndex delete/cut fixture also exposed the canonical page
walker's inline-absence zero operand being misread as DATA page zero. Physical
enumeration now excludes that absent component while preserving directory,
base and real-component validation and existing old-cut/reopen assertions.
Sibling actual-caller tests reproduced the same zero operand in point-plan
census, replacement/consolidation retirement and the selected PRIMARY leaf-pack
consumer. Absence cells retain their logical revision and directory census,
while physical leaf visits, retirement and component validation skip their
absent operand. Real component validation remains unchanged; held cuts and
leaf-pack reopen preserve absence and retained component values.

Read-only planning records validated absence point positions in the existing
result using a fixed 49-int array and count. Strict cursor validation merges
these logical positions with real physical intervals; neither absence-only nor
mixed plans invent a physical leaf. The streaming chunk caller receives a
separate call-scoped logical-point callback and counts real operations/bytes,
while target-leaf/span/split metrics remain physical. Callbacks are recursive
arguments, not retained result or reuse state. Caching prefers the chunk API;
the older exact `PlanFlushSpanRun` metadata API cannot represent absence points
and stays failclosed, with its existing entry-count fallback.

On 64-bit targets the fixed array/count adds 400 bytes to each result, including
default-off embedded `ApplyResult`, ordered-root result vectors and value copies.
Containing capacity and any actual callback/method-value construction remain
under the existing DATA operation allocation contract. No independent proof
heap/vector or owner is introduced. Complete-operation/default-off measurements
must include these costs; this boundary does not claim zero allocation or copy
cost. Existing containing allocations use actual Go types, not an old fixed
size allowance. Original native/M7/M8 qualifications remain unchanged.

The borrowed-cohort fixture additionally reproduced a real private manifest
ownership hole at Vacuum rebuild. Each of three retained roots constructed an
owned manifest, copied/materialized its images into separately owned banks, and
left the original private manifest unreleased. Identity-bound traces accounted
for exactly three 288-byte allocations (864 bytes pending). Actual DB cleanup
closed the original arena and ghost generation successfully; its 41,632-byte
baseline and 32-byte binding remained governed because of that pending storage.
The PRIMARY rebuild caller and both sibling DATA rebuild callers now defer the
original `ReleaseOwnedMetadataV1` through complete success/error paths. Normal
and rejected-rebuild fixtures require original held readers/tokens to survive,
then require zero original metadata and external leases after actual cleanup.
Existing `Owner.Close` accounting is unchanged.

Pre-close residual charge is a separate observation: the original retired arena
keeps its exact governor through the existing generation ghost lifetime. The
fixture measures it after the last reader and selected descriptor scope instead
of forcing ghost age or substituting public Close return for physical cleanup.
Physical residual paths come from original descriptors and are checked with
`os.SameFile` against the pinned File.Stat identity; stable duplicated handle
Names are synthetic and cannot identify filesystem residuals.

`BenchmarkPublishPrimaryDurableRootV1` and
`BenchmarkPrimaryBorrowedValueLogCohort/files={8,16}` are development diagnostics
until their measurement harness is independently reviewed and landed. Both
use actual production callers and complete-operation allocation accounting.
The cohort's setup, overwrite, Vacuum, GC and terminal Close are outside its
capture timer. Exact reproduction/artifact boundaries are documented in both
tool READMEs. Normal/race fixture results establish correctness, not retained
performance acceptance.

This inventory preserves native #4878 negative source, original record/byte/
resource caps, Accepted/once-ACK and M3/M8 holds. It does not qualify native APIs,
power-loss behavior or any performance threshold. Review, current-head CI,
final-base qualification and matched retained measurements belong to the
coordinator's completion packet.
