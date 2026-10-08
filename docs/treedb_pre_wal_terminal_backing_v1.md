# Pre-WAL terminal backing checkpoint (source construction only)

Finite/native/composite admission stays CLOSED. This unformatted successor has
not compiled or run. Check07 remains immutable scoped Mac semantic evidence for
its earlier source. Fixed M=128MiB, V=64MiB raw cache, F=32, P=8192,
Q=237942, allocator H=63010304 and all request/profile limits are unchanged.

## Actual selected request horizon

`collections/typed_string_patch.go:57–83` acquires one of four slots BEFORE
owned input copying and defers its release until synchronous group execution
returns. `native_string_patch_group.go` holds those slots across preparation,
actual `publishUpdateBatchPlanLocked`, result distribution and errors/retries.
`root_publication_activation.go` waits through durable terminal completion when
`opts.preparedLimits != nil`; `completePublishedSealTerminal` retires the actual
member loan before the coordinator completes report/ACK. Thus the selected
single write-domain four-caller/64-request epoch does not intrinsically create
64 simultaneous loans. Its upper outstanding native operation count is four;
a combined group can contain four requests but publishes ONE existing member.

Ordinary command-WAL sync is different: it can return after admission because
WaitThrough is gated by prepared limits (or non-command-WAL sync). Different
collection write domains and ordinary foreign producers are not globally bound
by one domain's four slots. The immutable runtime envelope and shared resident
credit must refuse an unfit addition BEFORE WAL/member/allocator effects. The
64 member cap is a retained horizon ceiling, not evidence of 64 simultaneous
selected caller scopes and not a lowered cap.

## Constructor classes and overlap

The existing shared runtime owns one scratch/Manager-plan arena under an actual
resident creator independent of its first member. Each member/operation scope
borrows the whole reachable arena; request reserve facets never survive the
call. Registry histories/reservations are additionally charged by the SAME
registry borrower engine under registry.mu, including future foreign growth.

For M=64, S=64, V=64, the immutable role plan is 3M+2S+7=327 roles,
S+2=66 loose tokens, 327*64+66=20994 tokens. Publication accounts pending
members, overwritten target, retired seals and previous-visible ownership;
shutdown also accounts both slots, pending/ambiguous candidate pairs, runtime
visible, member pairs and recovery roles. Poison and pre-WAL admission prevent
more than one ambiguous candidate in this selected envelope.

The pinned 64-bit layout gives this distinct allocation plan. Runtime compiled
unsafe.Sizeof/class tests are still required; this table is source-derived.

| Actual allocation | Raw bytes | Go class bytes |
| --- | ---: | ---: |
| Runtime arena control | 96 | 96 |
| Scratch control | 208 | 208 |
| Scratch roles | 5232 | 5376 |
| Scratch extra roles | 5232 | 5376 |
| Scratch loose tokens | 528 | 576 |
| Scratch tokens | 167952 | 172032 |
| Scratch groups | 839760 | 843776 |
| Scratch owned pins | 335904 | 344064 |
| Manager storage control/inline plan | 160 | 160 |
| Manager entries/inline generation leases | 2015424 | 2023424 |
| Manager owned pins | 335904 | 344064 |
| Total arena | | 3739152 |

The exact constructors share allocation-free per-birth class planners:
rootpublication.StableTerminalScratchClassBytes and
valuelog.StableSegmentTerminalStorageClassBytes. DB's
terminalArenaClassBytesV1 adds their actual control. No rounding of an aggregate
raw sizeof is used. A raw lower bound is 176*20994=3694944 bytes, excluding
controls, roles and class rounding. Creator plus 64 distinct whole-arena loans
has class charge at least 65*3739152=243044880 bytes (231.79MiB), beyond M.
This is REFUSAL, never acceptance. The new actual constructor/loan regression
requires refusal to keep previously held owners/creator and cumulative request
debit intact, then close every exact admitted edge.

Creator plus four loans alone is 5*3739152=18695760 bytes. It does NOT prove
selected fit: member/scope/limits controls, registry whole history and claims,
Snapshot/index/VM/File/reader/cache loans, old-cut overlaps, source/descriptor/
manifest/parser/producer controls and all failed attempts are still additional.
The request pays cumulative birth/loan debit; the master resident ledger pays
simultaneous live scopes and never refunds a still-retained class on origin
request completion.

## Concrete same-engine terminal construction

Before WAL, exact installed Manager identities/namespaces are validated under
Manager.mu -> registry.mu. The SAME registry precreates immutable generation
lease storage, namespace AVL nodes/string backing and fixed reservation binding
capacity. Live registry borrowers pay the additional backing before birth.
Future borrowers census inactive reservations and retained histories too.
Unknown successor identities refuse before storage until actual producer
projection/install bindings and capacity are attached; no fresh request segment,
second registry or optimistic deletion authority is introduced.

Terminal report/shutdown reserves the actual single synchronous invocation,
prepares the COMPLETE ownership group, reserves all same-registry gates and
then consumes allocator COW once. Cleanup is outside engine locks. Failed
unlink/sync retains actual cell/holder/account and ownership fields for retry;
no COW reconsume. Immutable caller-owned lease generations prevent stale value
copies from aborting a later reservation. No Manager or callback is stored in
cells/reservations; plans and owned pin slices are cleared before Manager End.
All array capacities, including unused tails, are scrubbed on joined error/
panic paths. Runtime creator survives every member until exact last loan and
runtime teardown. Ordinary unselected allocation/terminal behavior is preserved.

## L constructor integration contract (not yet composed)

Allocator's actual successor exports AllocationRequestCreditV1 (Reserve only)
and AllocationResidentCreditV1 (Reserve/Retain/Release),
NewAllocationCreatorV1(request,resident), and
PrepareOwnedCOWCandidateRetiringWithAllocationRequestV1(request,scratchCreator,
generationID,commitSeq,candidateID,capability,retirements,auxCount,sink,limits).
Retained and short scratch creators must be DISTINCT scopes from the SAME
installed native resident master. Request debits precede resident admission;
no paired request adapter is stored. Current nativeResidentCreditFacet forwards
all allocator methods to its SAME metadata ledger. Credited Prepare prebirths
activationTxn and admitted activated-vector capacity. Activate performs no new
request/debit/birth; credited direct Publish refuses missing preborn next-
capability ownership. DB callers must integrate these real constructors and
terminal scrub before enabling; legacy naked credit remains refused.

## Remaining mandatory families and actual enabling condition

- Native dispatch does not yet supply complete TerminalRequest/Creator/Resident
  and exact installed TerminalDeleteBindings. Existing public guards refuse
  before finite effects. Descriptor/directory/export consumers still refuse
  generic accounted escapes; owned preencoded result construction is required.
- Coordinator pending/recovery/group/waiter/channel/map controls, durable-root
  transaction/candidate/member/seal callback environments and actual visible
  bookkeeping need full deterministic capacity/prebirth integration. Current
  Coordinator.captureRecoveryResourcesLocked still births two maps and grows
  arrays; TakeRecoveryHandoff births/copied arrays/control after Stop. The new
  bounded handoff append removes one defensive copy only, not these births.
  The coherent successor must precreate SAME coordinator recovery backing and
  actual handoff control under real creator scopes before WAL, then transfer
  into those without post-storage constructors. No second cleanup queue.
- Direct candidate retains broad index/pager and inherited teardown, and now
  refuses accounted complete input before capability/directory/FD/COW work.
  Production coordinator path is distinct. Direct finite construction remains
  unsupported until actual caller lock/lifetime/class certificate.
- Actual Manager maps, File/private_file/name/stat/error/FD/poll/finalizer births,
  reader remap/scratch histories and cache backing remain uncertified. Every
  engine-owned instance needs its own class/debit and actual last-edge proof.
- Cache default 2048 slots/file and fixed [MaxFrameK+1]uint32 with MaxFrameK=255
  means 32*2048*256*4=67108864 offset bytes alone. File decode stash ceiling
  adds up to 32*131072=4194304 bytes. Raw V64 does not pay these controls.
  Source-derived layout forecasts slot/shard class backing about 76MiB+1MiB,
  but exact lazy allocation and overlapping reset/checked-out old owners need
  proof. Offset/header sharing redesign remains unselected; no cache writes or
  borrowing V64 for M controls.
- Registry namespaces/history/parentproof handles are not bounded by F32.
  SAME live borrower growth admission is required throughout all mutations;
  census current history and reject fit before copying/Observe/new FD effects.
- Full owned-inline manifest projection uses SAME C13 captured Snapshot/read/
  pinned FD and SAME parser/encoder; fresh owned result copies alone escape.
  Consumer error/panic must join checked read/FD close and retain actual retry
  credit on errors. Parser aux/sort/ownership grammar remains unchanged; vector
  eligibility comes only from the preexisting selector. Old-cut reader/VM/index
  lifetimes must be charged alongside new result backing.
- Installed shared producer, failed suffix rollback debt, exact tentative LSN/
  FileID keys under actual serializers, constructor formatting/open/stat/error
  and full manifest/locator/operation-facts capacities remain in whole fit.
  No array-only, numeric-count-only, interface-only or projection-only fit claim.

Full Linux/public/finite correctness, 128MiB fit and performance remain OPEN.
