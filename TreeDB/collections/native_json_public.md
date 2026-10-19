# Experimental native JSON public checkpoint

> Integration status: this consumer closure is provisionally restored from frozen
> `d206fe1f7e1990a26a8c98d91fde6a5b72627a94` onto the current common DATA
> foundation. Historical test results below describe that preserved source, not
> qualification of this integrated head. The next selected gate is
> `TestNativeExistingCollectionPublicLifecycle` across all three profiles; the
> conditional, composite/crash-replay, physical-reader and suffix controls remain
> affected gates. Current common V2, resource admission and accepted-prefix
> corrections are preserved. Ordinary new stores remain DATA; existing explicit
> and persisted PRIMARY support is unchanged. Final producer migration, V2
> certification, PRIMARY retirement and public cost acceptance remain open.

This is provisional integration for issue #5142, not an accepted collection mode
or completion of the full native collection packet. `NewNativeJSONManager`
explicitly activates the fixed cache/backend owner. Its restricted methods never
embed or return a broad legacy manager or collection. Before activation, existing managers hold the same backend's legacy producer
admission and retain their original barriers. After explicit activation, ordinary
and hook-free CollectionManager construction select the same fixed native JSON
core without a legacy backend, write domain, coordinator or raw barrier. Unsupported
legacy producers remain fenced. Repeated activation uses the same live owner.

The selected public entry is an ordinary `treedb.Open` with
`Options.MemtableMode = "cow_btree"`, followed by
`database.NativeSemanticBackend()` and
`collections.NewNativeJSONManager(handle)`. The backend capability belongs to
that original public database. Keep the database open while managers and captured
reads use it; close captured reads first, then report the result of
`database.Close()`. Do not close the capability separately. Ordinary raw methods
on the public database continue through the same owner after native activation.
Backend-only `OpenBackendWithCachedLeafLog*` constructors serve the legacy
backend-authoritative route and do not provide this native capability.

Equivalent backend maintenance uses the exact retained before/after physical
Snapshots to rebind immutable native domain roots by their canonical qualified
catalog descriptors. It preserves every pending/captured logical source and held
old view, including when a rewrite moves aliased roots and persistent value-log
pointers. This optional participant capability is separate from the single-use
captured-flush build/handoff; unsupported semantic owners fail closed. Private
partial successors use the original off-gate cleanup and maintenance admission.
The named public online value-log rewrite carries only the original cache owner
through its maintained physical finalizers. Ordinary backend aliases and generic
maintenance callbacks do not acquire this role. Required writer reconciliation
and exact basis refresh complete under the original flush fence through pending
Close, including when a completed physical batch later reports an error. This
role creates no accepted cut and does not seal an unhanded-off suffix.
No on-disk format or canonical command changes are introduced.

The two-phase frontend is `manager.Begin(collection, additional...)`, then
transaction `InsertBatchInto`, `GetFrom`, `Commit()` and `Close()`. Single-collection
callers can use `InsertBatch(ids, documents)` and `Get(id)`. Each bound collection
accepts one insert batch (the existing immutable planner sorts its primary keys); a second staging call for that collection
refuses while preserving the first batch. Other staging failures
terminally invalidate the transaction. Always close the transaction, including
on refusal. One-shot `manager.InsertBatch` delegates to this frontend and no
longer holds the manager mutex through planning and commit.

The opening is the existing P1 conditional transaction's single Snapshot,
logical sequence, batch and shared journal. Staged IDs/documents, canonical
payload, Effects and read dependencies remain transaction-owned through commit.
Get observes the opening plus own staged rows; original point reads, including
missing rows, and catalog identity are validated along with write intent before
canonical assignment. Commit composes the same immutable staged mutations onto
the latest compatible complete engine only after that validation. It neither
reruns the body nor captures/retries another opening. The internal preparation
returns original partial ownership to existing off-gate cancellation on error;
there is no application callback or inline cleanup under publication exclusion.
Containing transaction admission is refunded after its mutex is unlocked.

`TestNativeJSONPublicConditionalTransactions` is the selected public proof:
two transactions begin before either commits, read opening and own staged values,
and commit disjoint inserts A then B without reopening or body retry. Write/write
and missing-read/write conflicts refuse before assigned LSN advances. A staged
transaction survives equivalent physical checkpoint/handoff. Exact values survive
checkpoint/reopen, and original cache accounting and identity pins reach zero in
all three profiles. This is the singleton collection proof; the selected composite
proof is described below. Other operations and the full acceptance packet remain open.

`TestNativeJSONExternalPublicOwnerLifecycle` is the external-package
entry proof for durable, relaxed and NoWAL profiles. It uses only exported APIs:
native Create/Insert, a later ordinary raw Set, identical State/Snapshot/sequence
from the two public capture routes, held native and raw reads across a checkpoint,
Close and reopen, and the first recovered native mutation. WAL profiles require a
nonzero recovered canonical LSN. Its original identity-pin registries must drain
without cleanup errors. The first run after the constructor-role checkpoint was
RED for durable and relaxed profiles: native Create/Insert, raw Set and exact
shared-capture checks succeed, then the mixed public checkpoint reports
`pending public command WAL starts at 3 after applied 0`. Close preserves that
failure. That original regression and failure log remain unchanged. After the
lower frozen-cut coverage fix, all three external profile cells pass normally
and with race detection. The internal tests below separately provide original
cache-accounting final-zero, true suffix overlap and unapplied-WAL replay evidence.

`TestNativeJSONPublicMixedOrdersCapturedFrontier` is an internal scheduling
companion, distinct from the external entry proof. It exercises native-first/raw-last
and raw-first/native-last commands. WAL cases pause the existing durability
observer at the original DATA file's META write, after frontier capture and
producer-lock release but before durable publication and handoff. Two newer
native/raw commands complete before the pause is released. The first physical
Snapshot must cover exactly the old complete prefix (LSN 3), exclude the newer
suffix (through LSN 5), and contain only old values; latest memory sees the suffix,
and held native/raw views remain old. A second drain covers the suffix, then reopen
checks all values. All six ordering/profile cells pass normally and with race
detection. NoWAL cells exercise sequential lifecycle, since they exclude
post-frontier admission. No new production scheduling hook is introduced.

`TestNativeJSONPublicCreateInsertCapturedViewCheckpointReplay` exercises actual
public native Create and InsertBatch, exact Get results, a held old read, and a
newer native insertion after the checkpoint cut was frozen. The same existing
durability observer pauses physical publication; the newer insertion proceeds
while that original cut is pending. Handoff preserves both
the held old row and the newer suffix. Public Close requires original cache
bytes, sources, views, external leases and generations to reach zero.

The same test exits a child process without checkpoint or Close after two creates
and two inserts. Reopen applies the four durable canonical singleton WAL frames.
Recovered reads borrow the exact captured Snapshot and inspect only the selected
collection metadata, overlay descriptor and primary root. The first mutation
imports the complete physical catalog into a private descriptor-only Engine,
checks every admitted descriptor/root, preserves the other collection, and uses the
same bridge and outer installer. It does not publish a bootstrap engine or invoke
singleton handlers as composite children. The existing singleton startup replay
dispatcher remains canonical.

`TestNativeJSONPublicRecoveredPointersDescriptorImport` separately verifies real
recovered pointer-valued rows in two JSON collections before native activation.
It inserts a new ID using descriptor-only import, rejects an existing pointer ID
before append, holds an old read across a newer frozen-cut suffix handoff, and
checks exact values after Close/reopen. Both original ValuePtrs and the untouched
collection's root ID stay unchanged. Both closes require original cache accounting
and identity-pin registry state to reach zero, without cleanup errors. The first
pre-repair import rejection is preserved in the task's pointer checkpoint logs.

Selected scratch cancellation is memory-only: Create/Insert reject an inherited
resource inventory, private preparations begin with build=nil, and partial domain,
map and schema edges belong to their original preparation. Cleanup errors are
joined. Read Close detaches and refunds its independent wrapper even when lower
capture cleanup reports an error; the original lower failure custody remains
lower-owned. Foreground planner/payload scratch is caller-owned through the
synchronous bridge call, then its containing lease is refunded after scratch
aliases end. Accepted COW rows and journal effects own independent copies.

Each captured JSON read owns a small scope. Pending/captured logical hits and
no-base misses need no page workspace. Physical metadata/base traversal lazily
admits one scope-owned workspace from the same original producer budget, then
reuses it under the scope mutex until Close. Borrowed transaction probes detach
that independent scratch receipt without closing their original P1 capture;
complete catalog import explicitly admits both scan and lookup workspaces.
Scratch aliases end before refund, and containing read admission still refunds
after mutex unlock. Returned inline and pointer values are exact-length owned
copies. A fresh disk-backed Get still pays for physical scratch; explicit held
reads amortize it without changing ordinary Get's fresh-cut visibility. No pool,
retained collection snapshot, decoder policy or pointer-file authority changes.

Known limitations and RED controls remain explicit:

- Recovered first mutation imports the complete catalog descriptors, without a
  row scan. Point reads and duplicate preflight inspect only the requested keys.
  Import cost remains proportional to catalog descriptors; metadata admission
  and broader storage-policy eligibility still require review.
- Indexed, typed, vector, unsupported composite members, live-overlay, dictionary and unsupported
  record-layout shapes remain outside this selected route. Ordinary value-log
  pointer reads use the exact captured Snapshot's original pinned file Set,
  bounded existing inspection/decoding, and admitted scratch. They do not capture
  a current manager or add an inventory. Full GC/storage coverage and complete
  resource/projection coverage remain open. Names use the existing validator.
- The transient CollectionMeta decode admission uses an unproved conservative
  16-times-payload estimate; raw substring guards are not a proof for escaped or
  case-varied JSON keys. These require review before acceptance.
- The registered insert preparation now transfers its coarse decode/control lease
  to the original sparse custody so returned Effects backing remains covered.
  Exact sizing and duplicate-preflight parity remain open. Create's returned
  publication and Effects controls also need covering admission. Startup
  composite replay does not call singleton handlers or these callbacks.
- Shared create identity/counter effects now use the existing serialized builders
  on both WAL and NoWAL paths. Historical legacy concurrent-create diagnostics
  reproduced NoWAL `durable-root candidate base changed` on both baseline and
  candidate. The former durable captured-basis failure used the obsolete
  constructor role; the corrected native public constructor now passes the
  selected lifecycle proofs. That evidence does not qualify general concurrent
  legacy create behavior. Original failure logs remain preserved.
- Full producer migration, unsupported-shape refusal coverage, resource closure,
  admission proofs, broader race/recovery suites and performance gates remain
  required. Hook-free replay manager lifetimes outside the selected handlers
  remain part of that work.
- Native JSON insert transactions now use original dependency validation before
  latest-state immutable preparation. Create and other unmigrated native producer
  plans retain their existing exact-prepared-Basis restrictions. The selected
  insert/composite proofs do not establish the full producer and operation matrix.

Empty physical frontiers use fresh immutable successor headers with build=nil;
the original captured build stays single-use. Both zero-count preparations,
including independent lease headers, are reserved before physical acceptance.
Both production handoff calls use the same lower group's preaccepted Snapshot;
no new upper Snapshot identity authority or registry was introduced.

The prior `TestNativeJSONPublicCreateActivation` lifecycle proof remains useful
for ordinary raw/native continuity and idle checkpoints. Neither that proof nor
the larger selected public proof establishes issue completion, merge readiness,
or performance acceptance. Exact source, commands and first failures are recorded
in the task's `artifacts/p3/public-json-rows01` manifest and logs.

The selected cross-domain frontend generalizes the same `NativeJSONTxn`: use
`Begin("a", "b")`, `GetFrom`/`InsertBatchInto` for bound collections and
`GetRaw`/`SetRaw` for the original conditional batch. Each collection accepts one
strictly ordered insert batch; the raw subset accepts one inline point Set.
Limits remain the existing composite member/payload and original budget limits.
Own-staged lookup uses binary search. No body is recaptured or retried.

A multi-member commit has one canonical composite command and one aggregate
logical install. The original raw finalizer binds actual placed/revisioned bytes
into the admitted outer image before append. RID/external raw, typed, update and
delete members fail closed; their full resource closure remains owed. Startup
replay prepares all members and builds raw, collection and system roots in one
existing RootPublicationBuildGroup, preserving the original DB-minted replay
token through its one final publication. It requests the original replay loop's
leaf/value-log support. The separate direct composite NativePrepare callback
explicitly refuses; the transaction uses its original immutable Reprepare plan.

`TestNativeJSONPublicRawTwoCollectionsAtomicTxn` proves opening/own staged reads,
one-frame increment, raw/native read conflicts before assignment, complete old
views, checkpoint/reopen and original accounting/pin zero in three profiles.
Its durable child exits after ACK without checkpoint/Close; recovery applies
exactly one unapplied composite frame containing all three effects. The first
replay failure (missing existing NeedsReplayLogSupport registration flag) and
its correction remain in artifacts/p3/composite01. This is a scoped integration
proof; the full issue and performance/review gates remain open.

## Existing collection API checkpoint

After `database.NativeSemanticBackend()` explicitly activates the original COW
owner, `collections.NewCollectionManager(handle)` and the hook-free replay-manager
constructor support JSON `CreateCollection`, `OpenCollection`, `Insert`,
`InsertBatch`, `Get`/`GetInto`, and `OpenCollectionReadView` with full-document
`FetchDocumentsByID`. Both frontends stage through the same `NativeJSONTxn` and
`insertBatchPlanner`; input IDs may be unsorted, returned IDs preserve input order,
and duplicate insertion fails before assignment. Captured reads retain the exact
complete cut and keep old rows visible across later native writes and checkpoint.
Close each read view and manager before closing the original public database.

This route does not auto-activate an ordinary Open or remove preactivation legacy
barriers. Indexed/typed collection creation, secondary-index DDL, update/delete,
prepared replay-intent application, projected fetches, list, standalone sync and
stats-specific insert variants remain unsupported on these native handles. Their
legacy backend/write-domain authority is absent; metadata-only vector registration
also refuses before changing its runtime maps. Manager configuration and Close
remain safe without creating a legacy producer.

`TestNativeExistingCollectionPublicLifecycle` proves the selected existing API in
all three profiles, including exact response IDs/values, refusal without WAL
assignment, held read visibility, checkpoint/reopen, and original final bytes,
sources, leases, views, generations and identity pins at zero. Its durable child
exits at sentinel 97 with disk LSN below the two acknowledged singleton frames;
reopen must apply exactly those create/insert frames and recover both rows.

Added lifetime costs are one native core per existing manager, caller-owned
Collection metadata/handle and result-ID arena, and one admitted read-view wrapper.
Recovered read metadata now stays under its original admitted decode lease until
read Close. No performance improvement is claimed; existing native metadata sizing,
full producer/type/index coverage and graph performance gates remain open.

Physical JSON point reads retain their opening complete cut and reuse bounded
traversal/codec buffers owned by its exact immutable backend basis. Physical
reads sharing one basis serialize while using those buffers and copying inline
results into caller-owned output. A physical handoff gives later reads a new
basis; held reads keep the old basis and its original admitted buffers until its
last owner closes. Pending/captured logical-source precedence and pointer-value
resolution are unchanged. Recovered catalog decoding still uses its existing
scope-owned metadata scratch; it is not amortized by this row-read workspace.

## Indexed planner extraction checkpoint (source only)

The native transaction now adapts the maintained `directBufferedInsertPlan`
through `extractNativeInsertEffects`. Its primary mutation/effect backing is
reused; additional template, index-state and secondary root arrays use the same
original staging receipt, admitted before construction. The existing immutable
collection replacement can compose all of those domain deltas into one engine
successor, adopting every partial domain into its original preparation ledger.
No backend protocol or atomic installer is added.

This checkpoint deliberately preserves the indexed eligibility fence. The
unchanged `TestNativeExistingIndexedJSONPublicLifecycle` still stops at indexed
Create. Before enabling indexed JSON, complete the captured scalar reader/domain
import, unique prefix-range absence dependencies, indexed planner admission before
planning, and singleton/composite replay from the same complete root effects.
Then the named public proof can advance to Update/Delete, whose shared maintained
old/new effect extraction is also unfinished. Schema operations, assigned Raft and
typed asset custody remain obligations of the full #5142 packet. This source-only
extraction is not public activation or qualification evidence.

## Ordered staging admission checkpoint

Ordered scalar JSON staging separates retained rows and canonical command members
from synchronous index validation. Each binding reuses the maintained
`indexEncodeArena` under one original high-water COW lease; replacement admits
old and new buffers together before detaching the old arena. The existing full
indexed-plan formula still covers decoder fallback, including nested JSON paths.
Validation never retains extracted states. Final old/new states use one separate
pre-sized arena: its encoded-byte, unescape, state-header and value-reference
capacities are admitted once before allocation from scalar bounds on the old and
new documents. After encoding establishes actual lengths, one census admits the
maintained primary ID copies, secondary key/entry buffers, index-state output,
plan/root/policy backing, and unique-conflict maps before those builders run.
Root arrays are pre-sized; sorting reuses their backing. Unique replacement maps
retain their conservative control/growth allowance for the old values actually
removed. No temporary decoder object graph is retained per row; its original
high-water receipt remains separate. Final backing stays on the original stage
receipt through synchronous publication and is refunded after all transaction
aliases detach. Callback inputs and normalized output copies remain
transaction-owned and admitted.

Fixed publication/read/inspection controls are charged once per command member,
not for every document copy. Canonical member lengths and possible sort-header
backing are admitted immediately before encoding. No frame, visibility, unique
constraint, callback, or replay policy changes. The touched-row ordering
representation is unchanged. Default-budget 2048-row admission and growth-refusal
atomicity controls are source-only until the coordinator executes them; this is
not a throughput or full-node acceptance claim.
