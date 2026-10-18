# Experimental native JSON public checkpoint

This is provisional integration for issue #5142, not an accepted collection mode
or completion of the full native collection packet. `NewNativeJSONManager`
explicitly activates the fixed cache/backend owner. Its restricted methods never
embed or return a broad legacy manager or collection. Legacy construction must
hold the same backend's producer admission; unsupported live producers remain
fenced. Repeated activation uses the same live owner.

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

The two-phase frontend is `manager.Begin(collection)`, then transaction
`InsertBatch(ids, documents)`, `Get(id)`, `Commit()` and `Close()`. It binds one
supported collection and accepts one strictly sorted insert batch; a second
staging call refuses while preserving the first batch. Other staging failures
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
all three profiles. This is a singleton collection proof; composite transactions,
other operations and the full acceptance packet remain open.

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
observer at the original PRIMARY file's data sync, after frontier capture and
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

Known limitations and RED controls remain explicit:

- Recovered first mutation imports the complete catalog descriptors, without a
  row scan. Point reads and duplicate preflight inspect only the requested keys.
  Import cost remains proportional to catalog descriptors; metadata admission
  and broader storage-policy eligibility still require review.
- Indexed, typed, vector, composite, live-overlay, dictionary and unsupported
  record-layout shapes remain outside this selected route. Ordinary value-log
  pointer reads use the exact captured Snapshot's original pinned file Set,
  bounded existing inspection/decoding, and admitted scratch. They do not capture
  a current manager or add an inventory. Full GC/storage coverage and complete
  resource/projection coverage remain open. Names use the existing validator.
- The transient CollectionMeta decode admission uses an unproved conservative
  16-times-payload estimate; raw substring guards are not a proof for escaped or
  case-varied JSON keys. These require review before acceptance.
- The registered insert preparation returns an Effects array after its temporary
  8-times-payload decode lease closes. Remapping keys to retained domain strings
  covers key bytes, not the escaping array backing. Create's returned publication
  and Effects controls also need covering admission. Exact sizing and duplicate
  preflight parity remain open. The foreground proof does not exercise these
  callbacks as composite children.
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
  plans retain their existing exact-prepared-Basis restrictions; this singleton
  proof does not establish cross-domain/composite rebase or the full matrix.

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
