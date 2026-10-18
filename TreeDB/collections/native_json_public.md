# Experimental native JSON public checkpoint

This is provisional integration for issue #5142, not an accepted collection mode
or completion of the full native collection packet. `NewNativeJSONManager`
explicitly activates the fixed cache/backend owner. Its restricted methods never
embed or return a broad legacy manager or collection. Legacy construction must
hold the same backend's producer admission; unsupported live producers remain
fenced. Repeated activation uses the same live owner.

`TestNativeJSONPublicCreateInsertCapturedViewCheckpointReplay` exercises actual
public native Create and InsertBatch, exact Get results, a held old read, and a
newer native insertion after the checkpoint cut was frozen. The existing public
checkpoint hook pauses before forwarding the captured publication; the newer
insertion proceeds while that original cut is pending. Handoff preserves both
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
  on both WAL and NoWAL paths. A new concurrent different-name create control is
  RED: durable Close reports `root publication captured basis mismatch`, and
  NoWAL Create reports `durable-root candidate base changed`. The control and
  first failure logs are preserved; no retries or lock workaround were added.
- Full producer migration, unsupported-shape refusal coverage, resource closure,
  admission proofs, broader race/recovery suites and performance gates remain
  required. Hook-free replay manager lifetimes outside the selected handlers
  remain part of that work.

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
