# Value-Log Lifecycle Specification

This document owns value-log and split leaf-log lifecycle. The active target for
future WAL-protected external refs is the user-command WAL in
`user-command-wal.md`. The deprecated collection root-delta WAL plan in
`collection-wal-durability-plan.md` remains useful historical context for
external-ref preparation, protection, cleanup, and column-file closure risks.

## 1. Persistent Pointer Model

TreeDB value-log pointers are durable storage references.

A pointer remains valid while its segment is reachable from any live index state.
Segments must not be deleted based only on age.

### 1.1 Decode scratch reuse

The shared compressed-frame decoder limits output to the admitted raw frame
length, even when the caller supplies a larger scratch buffer. After a
successful non-empty decode into the same starting backing allocation, its returned
slice preserves the caller's capacity for subsequent mixed-size frames.
Errors and newly allocated output do not restore capacity from the caller's
buffer. This is an in-memory reuse contract; it changes no on-disk format,
pointer lifetime, checksum requirement, or cache/scratch retention limit.

### 1.2 Owned append reads

Ordinary owned `Get` resolves published pointers through `ReadAppend`. After an
unmapped miss, this path uses the same sealed lazy-mmap eligibility and mapping
budgets as view reads. Current writable segments without persistent mapping,
mapping denials, unsupported platforms and unavailable mapped ranges retain the
existing file-read fallback. Mapped hits do not re-enter lazy-map admission.

Managed remaps hold the manager lock before the file remap lock through actual
file-size admission and mapping publication. They recheck the registered handle
and current/sealed state under those locks; only sealed files face the sealed
budgets. Dead-cap and unchanged-denial misses can return before locking, with
an authoritative recheck on attempts that proceed. Increased limits allow a
retry. A failed new mmap preserves the old mapping, already borrowed views and
retained-mapping accounting; retirement occurs only after a successful map.

Compressed grouped owned reads may admit decoded frames under the existing
entry, raw-size and manager-wide byte limits. A hit copies the selected bytes
directly into the final owned destination while holding the cache slot read
lock, including when an append prefix requires a larger destination. Encoded
templates first copy their encoded input, then release that lock before template
lookup and decode. Returned owned bytes never borrow cache or mapping storage.
Record CRC verification precedes a mapped cache hit; cache identity includes the
verification mode and frame shape. Physical identity pins, segment reachability,
eviction and close retain their existing lifetime rules.

### 1.3 Durable raw-frame read cost

Ordinary raw value batches protected by the cached redo/journal or the public
command WAL bound their grouping count by the normalized
`ValueLog.BlockTargetCompressedBytes` target (4096 bytes by default), treating
that target as raw payload bytes when compression is off or the selector chooses
raw. Both the queued request planner and ordinary batch append use the largest
actual encoded value in their batch to cap K. Mixed-size grouped raw payloads
therefore stay within the target; a single value larger than the target remains
one record. Existing smaller record-count limits still apply. Framing bytes are
additional to the payload target.

The public command-WAL layer disables the cached redo/journal because it owns
command append/sync durability; that is distinct from WAL-off benchmark ingest and remains
eligible for this byte bound.

This reduces whole-record CRC read amplification without skipping verification
or caching a prior verification result. WAL-off benchmark ingest retains its existing
throughput grouping, and the dedicated outer-leaf lane retains its own K policy.
Dictionary/block compression and their writer-level keep/reject decisions retain
their existing grouping policies. In particular, an initial auto/balanced
forced-pointer block batch may bootstrap a larger compressed group, reject the
compression, and persist that whole group raw. A local random 4 KiB-value batch
produced a 128 KiB raw payload through this route; it is outside the raw chooser
bound. Full-profile evidence would need to justify separately splitting rejected
writer/prepared compressed frames before changing that compression policy. Existing pointers and frame encodings remain
valid, and ACK, sync, retention, GC and rewrite authority are unchanged.

Measure isolated raw-frame read cost with:

```sh
GOWORK=off go test ./TreeDB/caching -run '^$' \
  -bench '^BenchmarkValueLogRawFrameRead$' -benchmem -benchtime=1s -count=3
```

The benchmark appends through the ordinary production raw planner, closes the
writer, and warms a sealed mapped read before timing owned `ReadAppend` results.
It reports allocation costs, CRC checks per read and physical record CRC bytes
per returned byte, including frame/header metadata. CRC-byte accounting derives
from each pointer's persisted record header; it is not an inferred grouping K.
Use matched heads on the same host. This package microbenchmark does not prove
public load/update/checkpoint/footprint guardrails or durable Quicksilver hit
throughput; qualify those with the public native harness. Go test benchmark text
and optional profiles are standalone evidence, not benchprof artifact inputs.

## 2. Segment States

Conceptually, a value-log segment can be:

1. active (currently written lane head),
2. retained (still required by live pointers, queued flush state, or snapshots),
3. eligible (unreferenced and not active),
4. deleted/zombied.

### 2.1 Physical-identity deletion and quarantine

All managers and writers that can publish or delete segments in one live DB
namespace share a physical-identity registry.
The registry must be installed at construction, before the initial segment
scan; attaching it afterward is forbidden because an already-open unregistered
handle could otherwise resurrect an identity another manager committed as
deleted. Stable publication pins and delete leases use the captured file-handle
identity, not a later pathname lookup.

Destructive segment removal first moves the canonical name into a private,
same-parent, identity-encoded quarantine directory. Validation and unlink then
operate on that exact renamed object. A replacement created at the old
canonical name is never selected for cleanup. An interrupted quarantine is
durable recovery state: read-write open reconciles it before scanning segments,
while read-only open returns `ErrRecoveryRequired` without mutation.

### 2.2 External-version logical pruning is not segment GC

`TreeDB/mvcc.PruneVersions` deletes obsolete physical index keys only after its
persisted external timestamp floor makes those historical reads invalid. It
does not delete, truncate, or select value-log segments by timestamp or age.
The ordinary raw delete path updates index reachability; existing snapshot
roots remain retention roots. Value-log GC/rewrite may reclaim payload segments
later only through the reachability rules in this document.

The MVCC prune scan is snapshot-bound and bounded by its iterator plus delete
batch size. A live pre-prune snapshot continues to protect and resolve its old
value pointers. The prune's visited/retained/pruned byte accounting describes
logical physical-record bytes, not immediately reclaimed segment bytes.

## 3. Reachability Source of Truth

Reachability is defined by pointer references found in index trees.

`ValueLogGC` computes this by scanning:

- user tree,
- system tree,
- collection root trees referenced by system-tree descriptors under
  `collections/root/...`,
- committed but unapplied command-WAL external refs, once command WAL is
  implemented.

Entries with `node.FlagPointer` and `IsValueLogFileID(ptr.FileID)` mark a segment as referenced.

Command-WAL external refs are retention roots before they are reachable from
published roots. GC and rewrite must consult the protected command-WAL
external-ref index and the external-ref prepare guard before deleting,
truncating, rewriting, or moving value-log bytes. The first implementation must
skip value-log records protected only by command WAL rather than patching WAL
records in place.

A `RawKVBatchV2` `SetMaterializedRID` is not an external-ref retention root:
the complete command frame contains the exact RID and value bytes needed to
recreate a missing record. Before apply it is protected by retaining the command
frame itself; after apply, the recreated or reused pointer participates in the
ordinary published-root and cached-memtable reachability rules. `SetRID` entries,
including those sharing the same V2 frame, retain the external-ref protections
above.

Command-WAL-only protection may be released only after the command frame is covered by a
durable `AppliedLSN`, the root descriptors containing the refs are durable, and
the value-log reachability tracker has incorporated those published roots or a
full reachability scan has completed.

Command-WAL protected value-log refs are also capacity charges. Admission,
GC, rewrite, checkpoint, and cleanup must charge both the logical referenced
bytes and the incremental retained segment bytes that cannot be deleted because
of the protected ref. A tiny protected byte range that pins an otherwise
collectible large value-log segment is charged by the retained segment debt, not
only by the byte range. When protected value-log debt reaches the command WAL
soft threshold, maintenance is triggered; at the stop threshold, new mutating
commands that would create value-log external refs block; at the hard threshold,
those commands fail before ack.

Collection read views are also retention roots. If a live `CollectionReadView`
can reach a pending mutable, queued, or publishing unit that references a
value-log record, GC and rewrite must retain that record even if the command-WAL
frame has already been applied and command-WAL-only protection is otherwise
eligible for release.

### 3.1 Command WAL Maintenance Barrier

Every physical maintenance operation that can delete, rewrite, move, truncate,
rename, or stop protecting value-log, leaf-log, external-ref payload, column,
dictionary, or template bytes must acquire the backend command WAL
maintenance barrier before computing candidates.

The barrier must:

1. wait for active external-ref prepare guards to either commit/protect or
   abort/classify;
2. rebuild or refresh the protected external-ref index if recovery has not already
   done so;
3. return an immutable protection snapshot containing command-WAL-only refs, read-view
   refs, pending publish refs, unclassified prepare groups, and active backup
   manifest refs;
4. hold a retention token until the operation has finished all destructive
   steps.

If the barrier cannot be acquired or recovery/protection state is incomplete,
maintenance must fail closed. Dry-run may report debt but must not delete.

Operation-specific rules:

| Operation | Command WAL precondition |
|---|---|
| `ValueLogGC` | Merge protected command-WAL value-log file IDs into the referenced set. A protected segment is not eligible even when no published root references it. |
| `ValueLogRewriteOnline` | Source records protected solely by command WAL must be skipped in PR1. Rewriting and patching command WAL refs is forbidden until a separate crash-tested redirect protocol exists. |
| `LeafGenerationGC` | Leaf-log generations referenced by command WAL or collection read views are live generations. |
| online index vacuum | Require command WAL debt zero for the roots being rewritten, or publish/checkpoint dirty command WAL first. A future root-remap maintenance command may relax this only with crash tests. |
| offline index vacuum | Reject dirty command WAL and unclassified prepared external refs before read-only open. |
| `CompactStorage` | The initial checkpoint must be command-WAL-aware. If it reports command WAL debt, compaction must abort before value-log rewrite, GC, leaf GC, index vacuum, or zero-byte cleanup. |
| zero-byte cleanup | Check protected external-ref index and backup manifest pins before unlinking. |

Maintenance lock order is: acquire backend `maintenanceMu`, acquire the
command WAL maintenance barrier, take the immutable protection snapshot and
retention token, compute candidates, perform the destructive phase, then release
the token. No command WAL append lock may be held while acquiring backend
publish locks.

### 3.2 Command WAL Operator Runbook

Operators must use `treemap command-wal health --json` before manual cleanup,
backup triage, compaction triage, or restart triage for a directory that has
`command_wal_v2` enabled.

Health states:

| State | Meaning | Operator rule |
|---|---|---|
| `clean` | no pending command WAL, cleanup debt below threshold | restart, backup, compaction, and ordinary maintenance are safe under normal rules |
| `pending` | committed WAL or protected external refs are required for recovery | restart and backup are safe only when the whole directory, command WAL, and protected external refs are included; manual deletion is unsafe |
| `recovery_required` | complete unapplied command WAL exists | read-only open fails; take a whole-directory copy, then run read-write recovery |
| `corrupt` | hard command WAL or required external-ref failure | no manual deletion; preserve WAL, external refs, cleanup manifests, and artifacts; restore missing files or escalate |
| `cleanup_debt` | data is durable but obsolete files remain | run checkpoint/cleanup; delete files only when `safe-delete` reports `safe_to_delete=true` |

`ValueLogGC` and value-log rewrite reports must include whether bytes are
blocked by command-WAL external refs. Required command-WAL blocker fields are
`gc_blocked`, `gc_blocked_bytes`, `gc_blocked_segments`,
`gc_blocked_external_refs`, `oldest_blocking_age_ms`, `blocking_command_ids`,
`blocking_external_refs`, `blocking_reason`,
`protected_external_ref_logical_bytes`, and
`protected_external_ref_retained_segment_bytes`.

### 3.3 Incremental Accounting Fast Path

TreeDB maintains commit-time reference counters per value-log segment.

- Counters are updated from commit deltas (old pointer removal / new pointer add).
- `ValueLogGC` may use these counters as a fast path instead of full-tree scans.
- A compact metadata snapshot (`vlog_ref_counts.meta`) is stored in the DB dir.

If counters are unavailable, stale, or corrupt, TreeDB rebuilds by scanning trees
and rewrites metadata.

A durable-root fallback scan may also supply exact counters for its candidate.
These counts remain private and are bound to the candidate index, sequence, and
user/system roots until that matching candidate activates. Publication then
installs the already computed counts without applying its commit delta twice.
Aborted or mismatched candidates cannot repair the live tracker. This reuses the
required scan; it does not add a scan or change the reachability authority for GC.

Raw outer-leaf created/current identities in an apply delta are physical
dependency membership only; they do not increment logical reference counters.
An additive publication conservatively retains predecessor raw segments and
all producer-created/current segments, including empty lanes and non-current
segments created by rotations within the apply. This can retain bytes not
reachable from the newest root and adds segment-frontier capture/sync work.
Inventory snapshots occur before registration consumes the created list.

The superset is durability protection, never permission to delete. Destructive
ordinary publication restores exact candidate projection; older recoverable
meta slots, read snapshots, and stable identity pins continue to protect their
resources until released. GC/rewrite uses the existing reachability and pin
rules, so this admission does not grant an independent indefinite-retention
or reclamation authority. No directory enumeration or new on-disk format is
part of this capture.

### 3.4 Ordinary destructive publication

Ordinary user-root overwrite, point-delete, and range-delete publication can use
successful COW Apply's exact removed-pointer counts instead of decoding values
from every compressed outer leaf in the candidate. This certificate is private
and process-local: it names one index, predecessor sequence/user root, final user
root, and unchanged system root. Optimistic and serialized writers construct it
only after successful Apply; publication still validates the expected predecessor.
Cached checkpoint's real physical chunk group fixes that basis and ANDs complete
Apply evidence across every chunk, including net-zero contributions. Generic,
ordered-root, and descriptor-changing callers cannot acquire this certificate
from a delta flag. No certificate is replayed or persisted.

The exact logical-count projection requires a valid tracker at that predecessor
sequence and checked subtraction/addition. Candidate capture checks a pinned live
predecessor, registers producer-owned segments, and rejects user-root aliases
with the system root or any descriptor-selected root. It then uses the existing
CRC-checking pager topology collector for exact raw membership, without decoding
ordinary outer-leaf value bodies. Unchanged nonaliased collection/system roots
remain included. Missing Apply evidence, stale/unknown/underflowing/overflowing
counts, mismatched bases, aliases, unsupported topology, and recovery before a
live snapshot use the unchanged full scanner. Fallback count repair remains
candidate-private until activation; rejected candidates cannot repair the tracker.
Chunk-delta arithmetic overflow rejects the private group before publication.

The shared COW stable producer capture forwards ordinary, prepared, batch,
ChildRef, and concurrent lane append APIs. It retains dictionary/template authority
and exact raw handles/frontiers across each private Apply chain, then transfers
ownership through finalize. Abort/conflict/partial failure abandons or releases
those resources through the existing ownership boundary. Known forwarding adapters
check their actual producers before choosing stable appends. Dual-mode producers
can declare that their configured legacy mode lacks raw stable append capability;
replay forwarding preserves this mode selection. This check precedes Apply and
ignores registry/dictionary/template readiness: an authority failure after selecting
stable append still aborts, without retrying legacy append. Strict rewrite capture
retains its original authority requirements. Legacy producers
retain their existing publication path; missing required dictionary/template authority still
fails closed. Produced raw segments used only by discarded private intermediate
roots are filtered from final membership; inherited dictionary/template and packed
manifest authority keep their existing closure rules. Additive publication keeps
its existing predecessor-reuse plan, recapturing registered raw handles before
filtering mutable producer tokens.

This removes repeated compressed outer value-body projection; it still performs
pager topology and descriptor work per publication. Temporary allocation depends
on visited pager pages, segment/count membership, and stable producer identities
for the private Apply chain, rather than retaining decoded outer pages. It adds
no persistent cache, raw reference tracker, cap, scheduler, format, or batch-size
change. `treedb.durable_root.candidate.*` counters witness the mechanism; matched
native workloads and relevant process memory remain performance acceptance gates.
Exact candidate membership leaves both recoverable slots, queued publication,
snapshot/replay pins, and GC's deletion authority intact.

## 4. GC Algorithm (`DB.ValueLogGC`)

For each segment in current value-log set:

1. determine referenced set (incremental counters when valid; otherwise full scan),
2. keep if referenced,
3. keep if current active segment for that lane,
4. otherwise mark eligible.

For eligible segments:

- `DryRun=true`: report only.
- `DryRun=false`:
  - mark segment zombie in manager,
  - refresh value-log set,
  - verify file removal and report bytes/segment counts.

GC must never delete a currently active lane head.

## 5. Cached-Layer Retention Interaction

Cached mode tracks retained value-log paths while memtables/flush queues may still point at them.

Checkpoint and retention pruning:

- preserve currently active value-log paths,
- preserve retained paths,
- drop only paths no longer live/retained,
- refresh manager segment set after changes.

Backend maintenance that creates new value-log or split leaf-log segments must
reconcile cached-mode split writers after the backend operation. Reconciliation
refreshes the value-log reader and advances each cached writer lane past the
maximum observed on-disk segment sequence, preventing later cached writes from
reusing segment filenames created by compaction, rewrite, leaf packing, or index
vacuum.

### 5.1 Immutable COW read ownership

Explicit `cow_btree` cuts retain registered value-log subsets and deletion pins
through the existing DB-scoped physical identity authority. Each newly
introduced distinct resource receives an independent generation owner; there
is no owner per historical RID and no second COW GC registry. Both `SetRID` and
self-contained `SetMaterializedRID` require live read owners. WAL dependency
custody remains separate even when a materialized frame has no external closure.

Actual produced frame dictionary IDs select retained definitions. Per-read
scratch, input/output backing and private decoder state are admitted before
allocation and retained through their real close/drain lifetime; a raw output
limit alone does not bound codec state or frame window. The native raw1,024-byte
ZSTD producer can use a 2,048-byte window, which must fit the charged envelope.
Reader checksum/encoded-payload failures propagate rather than resolving through
an unrelated global codec/definition cache.

Producer flush makes buffered pointers readable without claiming fsync. Old
cuts preserve index/vlog/dictionary authority across checkpoint and equivalent
backend maintenance. GC/rewrite/leaf packing serialize and refresh the backend
basis through the existing flush fence; retired cleanup runs outside
writer/admission/publication locks. A referenced or pinned persistent segment
cannot be deleted because it is old. See
[COW resource and handoff ownership](cow-cache-publication.md).

## 6. Rewrite/Compaction

### 6.1 Online rewrite (`DB.ValueLogRewriteOnline`)

Online rewrite copies live pointer-backed values into fresh value-log segments
and updates keys in bounded commit batches. Cached-mode callers must checkpoint
first, protect cached value-log paths, and allocate rewrite RIDs from the shared
cached allocator.

A supported rewrite publication combines the matched old/new logical pointer
count delta with the existing leaf-file-ID reachability collector in a private
candidate snapshot. The collector walks pager topology and discovers collection
root descriptors, but it does not project logical values from every compressed
outer leaf. The resulting raw leaf-file and logical value-segment membership is
exact for the candidate; rewrite explicitly replaces predecessor raw membership
rather than using the ordinary additive publication superset. Shared logical
pointers remain counted until their last matching reference disappears. An
unmatched swap contributes no count change.

Unknown, stale, underflowing, or overflowing reference counts use the original full candidate
scanner. Replacing collection descriptor aliases can remove pointer-backed
system descriptor references outside the collection's local delta, so the whole
publication currently uses that full scanner. A system descriptor rewrite also
uses it. Exact fallback counts remain private until successful activation;
aborts and publication conflicts cannot repair the live tracker.

Rewrite and certified ordinary COW Apply use the installed producer's stable append APIs. Its private
resource builder retains the exact raw handle/frontier and dictionary/template
closure while candidate projection captures fresh registered raw membership.
Producer dictionary/template authority then passes through the existing finalize
resource boundary. Byte lookup alone cannot authorize publication, including on
the full-scan fallback. An unsupported producer fails with unresolved resource
authority before publication. This preserves installed producer rotation and
sequence authority. The builder releases acquired resources on Apply failure;
frozen resources release on abort/conflict, and successful publication transfers
ownership to the existing slot/runtime resource sets. Dictionary/template proofs
and packed-generation authority retain their existing inherited-closure rules.

Within one private Apply chain, the installed rewrite producer captures each
immutable writer-owned dictionary definition once per identifiable provider that
explicitly certifies `GenerationScopedDictionaryResources`. Cloned lanes share
that definition. The attempt owner retains each original closure and provider
snapshot lease through freeze. It derives an independent exact token view using
the closure's complete dictionary-generation obligations, then releases the
original snapshot/read state and provider callbacks; abandon releases originals
without producing a view. A kind-only clone can share original token chunks and
their snapshot owner, so that clone alone does not provide this boundary.
Private append captures omit duplicate
dictionary closures; public stable append APIs keep their full closure contract.
A later attempt captures afresh.
Reconfiguration installs new immutable bytes, including reuse of a logical ID,
and provider replacement selects new authority. Uncertified or unidentifiable
providers and arbitrary definitions continue to capture and validate on every
append. Capture
errors are never retained. Provider relocation does not invalidate a pinned
immutable definition: the retained closure still names its exact prior physical
generation, without reopening paths or substituting a newer generation. Generic
stable producers keep their original append/capture behavior. This bounds repeated
dictionary decoding and hashing by definitions per attempt, rather than output
leaves; retention is limited to the closures needed by that attempt.

dictdb's external index authority explicitly uses
`NewStableIndexGenerationResourceToken`. Its original token still owns
`Snapshot.Close` and any caller `OnRelease`; only the stable-index maintenance
counter follows the exact shared file-handle family through
`OnLastPinnedRelease`. This lightweight fence prevents online vacuum from
renaming/unlinking the namespace generation while candidate, queued, pending,
or physical-only coordinator views still name it. Logical filtering preserves
the same handle family. Coalescing overlapping captures retains one physical
representative and ends the discarded capture's fence. If the existing
representative has namespace authority but no inherited generation fence,
coalescing selects the fresh fenced representative; certified append falls back
to exact composition for this replacement. Selection preserves both namespace
and generation authority. Incomparable generic authorities fail before source
ownership transfers rather than silently dropping either protection. Coalescing
never unions lists of prior Apply owners or retains prior snapshot state. The
surviving fence ends once at the final handle release. The DB's own `ResourceIndex` capture continues
to use the token-local maintenance lease, so durable candidate clones do not
introduce a persistent fence against the DB's own index vacuum.

Recovery recognizes dictdb's canonical mutable index digest before accepting its
lane, ID, path, namespace and single dictionary-generation reachability field.
The public, backend and restored Raft constructors install the concrete side
backend's expected-generation lease before main durable-slot selection; side
options clear inherited parent hooks. A missing owner or malformed canonical
claim fails closed. Custom immutable dictionary producers and template resources
keep their existing recovery contract.

Under the side backend's maintenance lock, this lease validates the currently
owned exact index handle, parent/child namespace identity and required frontier,
then reserves only the stable-index maintenance counter. Persisted generation
labels do not substitute for physical identity or have to match a reopened
backend's runtime generation number. The recovered token transfers the lease to
one exact shared handle family, including retained slots and physical-only
views. Acquisition or validation failures unwind it; the final handle release
ends it exactly once. This holds no snapshot, reader, DB state or historical
Apply group and performs no nested checkpoint, lookup or publication. Recovery
cannot let a registry pin alone authorize dictdb vacuum's namespace replacement.
The side owner remains open through main backend and resource-view teardown.

The Apply wrapper also forwards the installed producer's prepared-payload
capability and the zipper-compatible lane bridge. A hint wrapper exposing an
optional stable method does not authorize unsupported prepared output; concurrent
span output continues to use the actual selected lanes and shared Apply owner.

Registry-owned producer creation uses the narrower stable creation capability.
Windows can certify this operation by validating the exact retained-parent child
and flushing the exact child handle; it does not require rename, removal, or
parent-directory persistence support. The rotated-producer success tests and
compressed outer-leaf benchmark require this creation capability. Broader
namespace operations still return the existing typed namespace-persistence error
on unsupported platforms. No failure permits weaker path-based recapture.

Exact newest-candidate membership is not deletion permission. Both recoverable
meta slots, queued publication debt, replay references, snapshots, and identity
pins still contribute to `RecoverableRootSet`; existing GC/rewrite deletion gates
remain authoritative.

The new `treedb.durable_root.candidate.*` counters report full scans, leaf-only
scans, pager pages visited, outer bodies projected, and projected body bytes
(`bodies * page.PageSize`). They cover candidate reachability collection only;
matching, COW reads, and descriptor-discovery reads remain separate work. Body
bytes are expanded page bytes, not compressed bytes read from disk. Existing
value-log record CRC counters cover their actual read paths. The supported
projection removes repeated outer value-body projection; it still walks pager
topology and descriptor roots twice per publication. For K fixed-B publications,
residual work is O(K*(P+D+S)), where P is visited pager topology, D is descriptor
work, and S is registered segment/count membership. This is not an O(B) total
publication claim.

Allocation ownership is bounded by one Apply/candidate at a time: matched delta
entries and COW leaf outputs depend on B; the logical-count copy and unioned file
membership depend on S; the collector's visited-page and root lists depend on P
and D. The existing candidate snapshot owns its registered set and reader until
capture returns. Each collector result and temporary membership map is discarded
before the candidate completes. Lane wrappers share one mutex-protected stable
resource builder, consumed exactly once by freeze or abandon. No persistent raw
reference tracker or new retention cache is introduced; rewrite batching, scratch
caps, and scheduler defaults are unchanged.

`BenchmarkValueLogRewriteOnline_ValuePointers` retains the pager control and adds
compressed outer-leaf cells at fixed B=8192 for N=30k, 300k, and 3M live logical
keys, plus N=30k/B=256 as a causal control. Two logical value segments and two raw
producer generations seed each outer cell. Half the live keys are in the selected
rewrite source; each timed operation runs that source rewrite to completion.
Setup and cleanup are outside the timer. For example:

```sh
GOWORK=off go test ./TreeDB/db -run '^$' \
  -bench '^BenchmarkValueLogRewriteOnline_ValuePointers/CompressedOuter/N30000/B8192$' \
  -benchtime=1x -count=3 -benchmem
```

Reports include complete-run time, publications, candidate scans/pages/bodies,
record CRC checks, process-wide rewrite mallocs and allocated bytes, copied value
records/bytes, and refresh scans. Malloc/byte deltas include concurrent Go runtime
work and are not peak RSS. Qualification still requires paired frozen-source
public `ValueLogRewriteOnline` runs with real compressed outer leaves, retained
profiles and process RSS, correctness/checksum/reopen/retention checks, and the
N/B matrix above. No performance improvement is claimed solely from these
mechanism counters.

### 6.2 Online split leaf-generation pack (`DB.LeafGenerationPack`)

Published leaf-generation views retain an immutable source manifest. GC prunes
deleted generation records in a private copy and, after persisting that copy,
publishes the final runtime view. Existing views retain their original records;
later pack operations clone a coherent manifest with unique generation IDs.

Every producer of a persistent `leaf_vlog/value-l255-*.log` child uses one
installed-owner sequence authority. Internal lane groups reserve from their
shared leaf-log allocator; CommandWAL's replay-inline owner reserves from its
rewrite-writer allocator; cached groups and lanes reserve from `leafLogAppendSeq`.
A reservation atomically selects
`max(live authority, discovered filesystem/set floor)+1`. The scan is only a
lower bound, and every staged pack rotation reserves again from the live
authority. CommandWAL pack records also reserve from the installed value-log
appender's existing RID authority. Failed or abandoned work may leave a gap;
reserved sequences and RIDs are never reused. If a concurrent installed owner
cannot expose this capability, ordinary pack and stable-pack preparation fail
before creating either a `.leaf-pack-copy-*` or
`.leaf-pack-stable-prepare-*` namespace. This does not fence foreground writers
and does not weaken exact no-replace promotion.

Leaf-generation pack uses a two-phase copy/publish state machine:

1. **Copy, without `writeMu`:** acquire a coherent snapshot and its leaf-generation
   pins; discover source refs from that snapshot; read source leaf/value-log
   records; write recompressed frames below a hidden, non-scanned staging
   directory; allocate and write private COW index pages; and apply collection-root
   zipper deltas. Private pages use a disjoint logical ID namespace in a staging
   pager that reads unchanged children through the pinned source pager. They do
   not enlarge the live pager, enter committed `PageCount`, consume the foreground
   freelist, or require crash recovery accounting.
2. **Copy durability, without `writeMu`:** flush and fsync the staging leaf-log
   frames. The private index pages are in-memory relocation input and are never
   an authoritative publication candidate; the final live-pager pages are
   synchronized before meta publication.
   `LeafGenerationPackOptions.Sync` is retained for API compatibility but no
   longer weakens this durability contract. The copied records, in-memory
   staging pages,
   and copy-time retired-page list remain unreachable.
3. **Publish, under `writeMu`:** revalidate the snapshot's `CommitSeq`,
   `RootPageID`, `SystemRootPageID`, `LeafGenerationStateVersion`, index
   generation, and exact source-generation state/file lists. Any mismatch
   discards the whole attempt and performs one full retry; no copied root,
   retired-page list, or private allocation is reused.
4. **Install:** while holding `writeMu` and the value-log visibility gate, link
   each retained staged-file handle into `leaf_vlog` with no replacement,
   validate that every destination link is
   still the creation-handle identity, and fsync each distinct parent exactly
   once as one namespace batch. The batch freezes one
   `ResourceOuterLeafPack` token per reachable segment, bound to its immutable
   byte frontier, digest, physical identity, generation, and packed-pointer
   reachability. The original link remains in the private staging namespace
   until successful publication cleanup; promotion never unlinks a source
   pathname that may have been rebound. Capture and resource-set pins deny
   deletion through
   `RegisterSegment`; manager registration must report the same physical
   identities before publication can continue.
   Registration is tentative and publish-owned: existing snapshots retain their
   immutable old set, while `AcquireSnapshot` and `RefreshValueLogSet` cannot
   construct a set containing candidates before root publication. Rebase private
   page IDs by appending final COW pages to the live pager, synchronize those
   pages, publish the new roots and generation manifest through the alternate
   meta page, and only then make committed retired pages reclaimable. On
   platforms that require
   an explicit mapped-view flush, its address and length are aligned to the
   platform mapping granularity. Linux finishes with `fdatasync`; Darwin uses
   `F_FULLFSYNC`, Windows uses `FlushFileBuffers`, and unsupported stable-file
   adapters fail closed. `SyncPages` therefore promises durability of the
   requested pages, not that only those bytes reach stable storage.

If exact revalidation fails, the in-memory staging pager and staged records are
discarded and a full copy retries once; no private page or retired-page list is reused. A failure
before writing the alternate meta page rolls the live append cursor back and
removes candidates while visibility remains exclusive. A sync failure after the
alternate meta write is outcome-ambiguous: all candidate pages and segments are
retained, the handle fails closed with `ErrRecoveryRequired`, and reopen selects
the highest valid durable meta state. Startup removes orphan
`.leaf-pack-copy-*` directories only after acquiring the database lock, so it
cannot delete an active attempt.

Before promotion, leaf pack closes both staging append writers and the private
staging value-log manager, while retaining separately reopened exact segment
handles and identity pins. Any failure after a destination link is installed,
or after the alternate meta write has an ambiguous durability outcome, poisons
the live handle and retains those authorities until `Close`. Cleanup also
checks the creation identity before unlinking a pathname, so a rebound path is
never deleted.

Packed promotion currently fails closed before creating staging state on
platforms without the exact relative-parent, cross-parent no-replace, and
namespace-persistence primitives. In particular, Windows returns the typed
`ErrNamespacePersistenceUnsupported`; portable packed promotion there is
deferred rather than weakened to path-based rename evidence.

`maintenanceMu`, the snapshot generation pins, and `teardownMu` remain held
across copy and publish. Therefore leaf-generation GC cannot reclaim a source
before the replacement root and generation state are durable, and `Close`
cannot tear down the snapshot, pager, or private reader during copy. Ordinary
value-log pointers embedded in copied leaf pages remain persistent references;
leaf packing does not alter their reachability or lifetime.

Collection catalog relocation follows visible publication acceptance, not only
the finalizer's successful return. If `CommitPublicationAccepted(err)` is true,
the relocation completion installs the replacement root coordinates exactly
once before releasing its existing admission gates, even when a subsequent
durability/admission wait failed. A retained but not accepted candidate does not
authorize relocation. Existing ambiguous-outcome resource retention and
fail-closed/reopen behavior are unchanged.

The optional `LeafGenerationMaintenanceLimits` on planning, packing, and GC
admits the native input footprint before expensive indexing or reachability
work. `NativeEntries`, `NativeBytes`, and `PagerPages` must all be positive when
enabled; all-zero limits preserve the legacy manual API. Bounded directory
inspection precedes refresh/reconciliation, and each independently captured
snapshot is checked again, including a pack retry and GC's recoverable-root
scan. These are per-phase storage-footprint bounds, not a cumulative I/O meter
or quota on concurrent filesystem growth. An admission error never authorizes
reclamation from a partial scan.

Explicit typed graph fold passes its existing maintenance footprint envelope
to these native phases. Before reclamation it retires completed captured-base
cache keepers on registered handles of the same collection, across managers.
It does not take ownership of in-flight builds or independently admitted read
owners; their exact snapshot, holder, and asset pins continue to protect their
generation. After a warm completes, Ensure and Fold compare that exact keeper's
immutable base key with the healthy current publication and release an obsolete
keeper, including a first sibling warm that registered after the fold sweep.
Same-base suffix advances preserve the keeper. A successful no-op pack still
advances the fallback horizon and runs bounded native GC: wholly dead generations
need no copying and are deliberately excluded from pack candidates.

`BenchmarkLeafGenerationPackCopyPublish` provides the pinned before/after
performance fixture. Run five externally alternating base/head invocations with
`-benchtime=1x -count=1 -benchmem`; it reports copy bytes/second, frames, wall
time, foreground write p95/p99 and completed writes, exclusive publish hold
time, copy attempts, `B/op`, and `allocs/op`.

### 6.3 Offline rewrite (`ValueLogRewriteOffline`)

Offline rewrite rewrites live pointer records into fresh segments and swaps index.

#### Preconditions

- exclusive DB lock,
- clean commit log (no pending `commit-*.log`),
- readable existing value-log segments.

#### Procedure

1. Open DB read-only under exclusive lock.
2. Build new value-log segments by iterating current trees and copying referenced records.
3. Rebuild index file (`index.db.new`) with rewritten pointers.
4. Write ready marker and fsync.
5. Atomically swap `index.db.new` into `index.db`.
6. Remove obsolete value-log segments.
7. Report before/after size and segment counts.

#### Safety properties

- Only referenced records are copied.
- Pointer map deduplicates source record copies when needed.
- Old segments are removed only after index swap succeeds.

### 6.4 Full storage compaction (`DB.CompactStorage`)

`DB.CompactStorage` is the preferred online operator path for reclaiming TreeDB
storage. It composes:

1. checkpoint,
2. value-log rewrite,
3. value-log GC,
4. split leaf-generation pack,
5. split leaf-generation GC,
6. index vacuum,
7. settle GC passes,
8. zero-byte value-log cleanup,
9. final debt audit.

Applied full storage compaction holds backend maintenance serialization for the
whole sequence. Plan mode computes the same debt model without mutating storage.
The plan records index page/span/freelist debt and a typed `index-vacuum`
disposition. Full mode uses the bounded production thresholds; Exhaustive runs
for any measured reclaimable index debt. Apply re-probes immediately before the
phase, invokes the RecoverableRootSet-fenced online replacement on supported
writable platforms, and checkpoints only after a successful replacement.
One bounded settle replacement runs when later GC/checkpoint phases create new
policy debt. After a successful replacement, leaf pack/GC settles any debt from
rewritten system leaves within the same run-wide `LeafPackMaxPasses` budget;
completion is based on the subsequent audit.
Transient stale/mutation races are `deferred`, Windows is `unsupported`, and
permanent errors fail compaction. Deferred or unsupported required work keeps
all completion flags false. `PolicyFullyCompacted` means selected planner debt
converged; `ByteMinimized` additionally requires every Exhaustive byte phase to
complete.

For exclusive offline maintenance, close the public cached owner before
`OpenBackend`. The maintenance open retains dictionary byte lookups and their
stable resource provider from the same side-store owner until backend cleanup
finishes. Packed dictionary dependencies require that authority; lookup bytes
alone are insufficient and unresolved dependencies fail closed. Newly created
leaf segments are registered before each root publication captures their
identity, and their pending registration inventory is consumed before later
pack/GC phases can retire them.

A successful phase sequence can still leave resources retained by an older
durable slot or another recoverable root. Report that remaining debt and the
completion flags from the final audit; a successful call alone does not establish
`ByteMinimized`. Repeating compaction/GC must preserve all recovery-selectable
resources until the same reachability rules permit their retirement.

Each cold debt audit performs at most one page-granular reachability walk over a
coherent snapshot of the user, system, collection, and protected roots. The
snapshot basis includes the commit and root IDs, leaf-generation state version,
retained value-log set, and protected root/path sets. Dynamic protected-root
snapshots are captured on both sides of backend state and must match; cached
providers also contribute their monotonic root-domain version so same-ID ABA is
observable. For unversioned callbacks, equal canonical root sets with unchanged
backend commit/root state are the same audit-visible basis because retiring or
reusing a protected page changes that backend state. Value-log reference counts
retain every logical pointer projection, while live-byte accounting counts a
grouped physical record once. Audit results are published to the incremental
reference tracker only after the protected basis brackets two exact backend-state
checks; one invalidation is retried and a second returns
`ErrCompactStorageAuditStale`. Later settle/final audits perform zero new walks
when they reuse structural reachability on an exact complete-basis match. File
sizes, pins, and zero-byte debt are recomputed on every audit.

Value-log deletion in this lifecycle remains reachability-based. Zero-byte
cleanup is limited to untracked `value_vlog` segment files and must honor
cached-mode protected paths.

## 7. Read Integrity Options

Value-log read integrity mode:

- `IntegrityVerify` (default): verify value-log checksums.
- `IntegritySkipChecksums`: disable checksum verification (unsafe).

Template and dictionary lookup failures for encoded/compressed records are treated as read/recovery errors.

## 8. Operational Guidance

- Use `ValueLogGC` regularly to reclaim fully unreachable segments.
- Prefer `DB.CompactStorage` for explicit online "make this DB compact"
  maintenance across value logs, split leaf logs, and `index.db`.
- Use `ValueLogRewriteOffline` only when an exclusive offline rewrite is
  intentionally required.
- Monitor retained bytes and optional guardrails:
  - `ValueLog.MaxRetainedBytes` (warning threshold),
  - `ValueLog.MaxRetainedBytesHard` (pointer admission cap).

## 9. Lifecycle Invariants

1. Pointers remain readable after reopen/checkpoint.
2. Unreferenced segments may be deleted; referenced segments must remain.
3. Segment deletion is reachability-based, not age-based.
4. Rewrite must preserve key/value visibility across reopen.

## 10. Ordinary Compressed Frame Read Cost

An owned point read verifies the complete stored frame CRC and, on a decoded
group-cache miss, decodes the complete compressed payload before selecting one
value. Ordinary cached user-value block batches under auto/balanced compression
therefore bound newly ingested frames by 32 KiB of actual decoded value bytes,
with the selector's K as an upper bound and oversized values as singletons.
RID/offset metadata is not part of that decoded payload. Compression rejection
keeps the same bounded grouping when writing the frame raw.

The bound reduces cold point-read amplification without changing frame version,
read verification, owned-output lifetime, cache budget, publication/durability
boundaries, pointer reachability or GC rules. Smaller groups can cost compression
ratio, frame overhead and ingestion/checkpoint throughput; assess those costs
together with read throughput and tail latency on representative workloads.
Eligible grouping counters report successful emitted frame sizes, including
compression-rejected frames, rather than the selector's pre-cap estimate.

Recognized retained JSON/template/semantic streams and template-enabled lanes,
dedicated leaf-log lanes, explicit block/dictionary compression, size/throughput
policies and selected raw-mode batches keep their existing grouping policies.
The bound applies to eligible fresh cached ingestion, not every persisted frame:
previously written files remain readable and maintenance rewrite independently
groups up to 4 MiB. Rewriting can consequently restore larger read amplification;
an ingestion improvement alone is not a global post-maintenance size guarantee.

Command-WAL collection side roots use the same stable outer-leaf producer as
ordinary COW rewrite through the replay leaf-log wrapper. Installation forwards
the DB-scoped identity registry and stable dictionary provider resolver before
opening any leaf segment. Stable single and batch appends preserve the replay
appender shared RID reservation and register every produced segment. Producer
capture owns dictionary/template and raw-file frontiers; registration failure
releases capture authority, while publication transfers or releases the set
through the existing success, conflict, and abort paths. An absent stable provider
continues to reject dictionary-dependent publication, including scanner fallback.

### Apply-scoped caching leaf token handoff

The concrete cached value-log writer can retain one physical producer family
per selected lane within that same Apply owner. The first capture uses the full
stable producer constructor. Repeated captures flush the actual same writer,
validate its exact handle identity and retained namespace, and read its current
file size to issue a new immutable frontier certificate. Ordinary pinned-view
cloning alone cannot certify frontier growth. Each certificate acquires its own
registry pin and shared-handle/namespace reference; it inherits no arbitrary
caller callback or dictionary snapshot owner. A changed writer handle, file ID,
or complete registration constructs a fresh family before releasing the old
attempt reference. Rotation's closed/current authority remains independently
captured, and final candidate membership still recaptures registered resources.

The private view explicitly implements `LeafPageLogApplyResourceOwner`. Group
and lane views share its bounded family slots, and record-length hint adapters
forward that ownership. After all appenders join, freeze or abandon releases
each attempt reference. Output certificates retain the exact physical handle
independently until their existing builder/candidate ownership ends. Append
failure closes the private owner; retry creates a new attempt. A terminal private
view rejects further appends. Per-lane certificate operations remain independent,
and release synchronizes with in-flight family operations. Public stable APIs
still construct full owned resources on every call; unknown writer implementations
use that original validated path. Dictionary/template capture, generation fences,
durability barriers and final closure validation are unchanged. This reduces
handle and namespace construction by current writer generations per attempt,
while frontier validation and token ownership remain per append.

The concrete caching outer-leaf producer explicitly implements
`LeafPageLogApplyTokenProvider`. Its attempt-bound regular appenders, including
prepared batches, ChildRefs and group lanes, deliver each append's raw tokens to
the existing Apply builder. Record-length hints and dependency-append hooks are
forwarded by the hint wrapper. Wrapper interface presence alone does not grant
this capability: an unsupported inner producer uses the validated public stable
append path. Public stable APIs on an attempt-bound view still return complete
owned resource sets.

Each append retains the same writer preflight, namespace-creation certification,
rotation capture and the actual writer flush. This is a token ownership
handoff. Each token's captured segment frontier remains immutable. The receiver
checks all pointer generations and overflow-safe record ends, complete frontier
coverage, raw resource kinds/reachability, and exact retained namespace bindings
before adding tokens. Rotation certificates for the same generation combine
their maximum immutable frontier. Unreferenced rotation tokens are released. The
raw callback consumes its entire token inventory on success or failure;
dependency set Merge transfers ownership only on success. Partial Add failures
require the Apply owner to abort and abandon the already accepted inventory.

Concurrent lanes share the attempt's synchronized builder. The caller joins all
appenders before freeze or abort. One final Apply Freeze retains namespace and
closure checks; exact current logical membership, publication, WAL/replay and
recovery authority are unchanged. Dictionary/template child sets retain their
existing validation and ownership; dictionary generation fences remain separate
from token-local snapshot readers. The optimization removes raw-only child
Freeze, physical-descriptor copies and child-set Merge topology. It adds no
persistent cache, generation lease union, durability shortcut or format change.
