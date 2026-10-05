# TreeDB Behavioral Contracts

This document specifies externally-observable behavior expected by callers.

Status:

- Current shipped contract: sections that describe existing key/value, cached,
  read-only, and collection write-domain behavior are normative for current
  code.
- Target conditional raw-KV behavior is tracked by issue
  https://github.com/snissn/gomap/issues/3420 and its child stack. It is not
  current behavior until the named implementation and verification gates land.
- Supported collection commands follow the selected production profile's
  acknowledgement boundary in `write-path-and-durability.md` and
  `collections-write-domain.md`. In `command_wal_durable`, the complete command
  is recoverable before acknowledged write-domain visibility; root publication
  and `AppliedLSN` may follow later. See `command-wal-support-matrix.json` for
  supported entry points; planned commands in `user-command-wal.md` are not
  supported merely because they appear in that design.

## 1. Key Model

- Raw KV keys and values are byte strings.
- The empty point key is a valid raw KV key and sorts before every non-empty key.
- A nil point key passed to raw KV point APIs is equivalent to the empty key.
- A nil raw KV value passed to `Set`/batch `Set` or an `Update` set intent is stored as a zero-length value.
- Key order is lexicographic (`bytes.Compare` semantics).
- The tree is ordered and range-addressable.
- Collection/document APIs may keep separate non-empty logical ID contracts.

### 1.1 Target external-timestamp MVCC key contract

The opt-in codec in `TreeDB/internal/mvcckey` establishes the physical ordering
contract for the external-version MVCC work tracked by
https://github.com/snissn/gomap/issues/3668. It is not a read/write MVCC engine
and does not change current raw KV behavior.

- Caller-assigned versions are nonzero `uint64` timestamps. Timestamp zero is
  rejected rather than remapped.
- Logical keys are arbitrary bytes, including empty and embedded zero bytes.
- Encoded order is logical key ascending and timestamp descending. Equal-key
  scans therefore encounter the newest version first.
- `EntryRevision` remains TreeDB's native current-entry validation token. It is
  not an external timestamp, historical version, or MVCC visibility boundary.
- Namespace, logical-prefix, and exact-logical-key/all-version bounds are
  half-open byte ranges suitable for TreeDB iterators. Bound construction is
  available in append-to-caller-buffer form.
- Exact-key bounds reject logical keys whose eventual timestamp-bearing key
  cannot fit the codec envelope. Logical-prefix bounds are prefix-sized and may
  therefore be valid even when a same-sized exact key is not encodable.
- The version-1 namespace is owned only by an MVCC layer that explicitly opts
  in. Existing raw APIs neither encode keys nor reject that range; a mixed-use
  owner must keep unrelated raw physical writes out of the reserved range.
- The codec format and internal Go API are pre-alpha. A format change may
  require rebuilding experimental DB directories rather than migration.

The opt-in `TreeDB/mvcc` package owns the first read/write use of this codec:

- `CommitAt(timestamp, mutations, mode)` rejects timestamp zero, invalid commit
  modes, oversized logical keys, and duplicate logical keys before creating a
  TreeDB batch. Nil and empty logical keys have the same duplicate identity.
- After timestamp/mode and non-nil Store validation, an empty mutation list is a
  no-op that does not access TreeDB or probe its durability/open state.
- Duplicate logical keys within one batch are rejected; they are not resolved
  by order-dependent last-write-wins behavior. Separate successful commits to
  the same logical key and timestamp address one physical version, so the later
  commit replaces the earlier version.
- All puts and tombstones in one call are stored through one TreeDB atomic
  batch. A storage error may be commit-ambiguous, but the batch is wholly
  visible or wholly absent, never partially visible.
- `CommitRelaxed` uses `Batch.Write` and promises atomic visibility without an
  fsync boundary. `CommitDurable` is accepted only when TreeDB is configured in
  `DurabilityDurable` mode and uses `Batch.WriteSync`.
- `GetAt(key, readTimestamp)` creates a physical lower bound at
  `(key, readTimestamp)` and seeks directly into the exact-key range. It returns
  the newest retained version whose timestamp is at most the read timestamp;
  it does not materialize the key's history.
- Read results distinguish absent, present, and tombstoned states. Malformed
  physical keys/value envelopes and underlying iterator/storage errors are
  returned explicitly. Storage failures wrap `ErrStorage` while retaining the
  underlying error for `errors.Is`/`errors.As`. Present values are caller-owned
  copies.

`Store.GetAt` prefers the optional `SeekGEVersionRange` capability. Its caller
contract is one Store owning the reserved namespace, with no raw writes there;
the codec alone does not authorize shared access. Cached reads use the shared
writer gate only for one canonical version range with no retained range spans.
Ordinary `SeekGE`, noncanonical bounds and range spans retain the exclusive
gate. Any observed physical node tombstone releases the shared attempt's view
and backend leases before a fresh exclusive attempt. MVCC tombstones are value
records and do not require that fallback. Results own their bytes.

For this capability, multi-record commits hold the existing Store floor lock
exclusively through batch publication; reads, single-record commits and version
snapshot acquisition share that lock. This prevents a reader from observing a
cached batch between shard applications. Batch-only adapters retain their
existing admission. A durable multi-record commit holds this fence during its
WAL acknowledgement, so independent multi-record commits through one Store
cannot form a concurrent durability group. Staging and validation precede the
fence. Qualified pruning holds the same Store lock exclusively through physical
pruning and lease cleanup.

The shared source-selection contract depends on these existing invariants:

- All physical versions of one logical key share a mutable shard. Writers
  select the current table while holding that shard's mutex; rotation freezes
  and replaces it under the same mutex. A previously admitted WAL/vlog writer
  cannot insert into a frozen old generation after a newer generation commits.
- A retained view keeps the captured mutable and immutable tables, their
  borrowed arenas and published-root leases alive through materialization.
  Frozen tables remain probed even after their queued units retire. Updates
  allocate fresh value bytes or replacement nodes; they do not overwrite a
  previously selected value. Canonical physical keys are at least 19 bytes,
  excluding append-only's inline 8-byte keys in reusable entry backing.
- Flush collection takes a global queue prefix, respecting lane and range
  barriers. Within a canonical run, physical keys publish in sorted order;
  queued sources retire only after all chunks succeed. Therefore a later
  backend snapshot cannot expose a new lower-priority eligible version while
  omitting its previously committed, earlier-sorting predecessor. Native
  grouped-root publication supplies a stronger all-at-once boundary.
- A selected old same-timestamp value can linearize before its concurrent
  replacement. An eligible successor newly observed in the backend can
  linearize when it became visible, because the source and publication order
  above covers its earlier eligible predecessors. Versions above the read
  timestamp fall outside the range.
- Pruning cannot recreate keys at/below its floor: commits reject them. It
  retains a live value anchor and deletes a tombstone anchor only after all
  older versions. Qualified pruning also holds the Store lock exclusively:
  ordering alone cannot exclude a retained old queue value combined with a
  later backend snapshot after its winning tombstone anchor has been deleted.
  Physical tombstone observations still take the exclusive retry.

The shared gate remains held until owned-byte materialization and lease cleanup
finish. Close, checkpoint, conditional/range writes and value-log maintenance
keep their existing exclusive fences and lock order.

Retained-version iteration and discard/pruning extend that opt-in owner:

- Exactly one `Store` owns one open TreeDB handle. Creating multiple `Store`
  values over the same handle is unsupported because floor caching and
  maintenance serialization are owner-local.
- `IterateVersions` pins one TreeDB snapshot. Forward order is logical key
  ascending then timestamp descending; reverse order is logical key descending
  then timestamp ascending. Prefix, inclusive logical lower bound, exclusive
  logical upper bound, and optional read-timestamp ceiling are combined without
  full-database materialization. Options are copied at open; `Entry` returns
  caller-owned keys and values, and tombstones remain explicit records. `EntryView`
  returns read-only borrowed key/payload slices valid until the next `Next`,
  `Seek`, or `Close`. It reuses the current decoded version without copying or
  reading the payload again; advancing still materializes pointer values and
  reports their errors eagerly. Retained or mutable output must use `Entry`.
  `GetAt` transfers validated successor-owned payloads and copies borrowed
  iterator-fallback payloads before Close. Empty Present results retain nil Value.
  Transferred payloads retain their complete record backing allocation, including
  the envelope byte and allocator rounding; reducing slice capacity cannot
  reduce this GC retention. Iterator
  accounting reports physical versions visited, filtered/skipped, and returned.
- `VersionIteratorOptions.ExactKey` intersects the other logical filters with
  one logical key. Nil disables this filter; a nonnil empty slice selects the
  empty key. Canonical codec bounds cover all its versions, including binary
  keys and codec-maximum keys, without a synthetic `key+NUL` upper bound. The
  codec limit does not guarantee a physical key fits a published TreeDB page.
  Oversized exact keys fail before copying that option. Valid options and owned
  results retain the existing floor, error and Close contracts.
- Canonical exact-version snapshot bounds select the existing frozen point-shard
  queue and published root when complete shard metadata is available. Snapshot
  cuts still rotate all populated mutable shards and retain the full snapshot
  owner. Span presence is captured during the existing immutable-view clone,
  avoiding a full-queue absence scan at iterator open. Retained range spans,
  missing queue/point metadata and a singleton
  published global root during upgrade use the generic iterator root. An initial
  absent published set keeps the existing backend snapshot fallback. Full-queue
  positional range spans are never attached to a filtered queue; newest-source
  duplicate precedence and physical delete filtering are unchanged.
- A nonzero global discard floor is the greatest timestamp that may be
  discarded, matching Badger managed-mode `SetDiscardTs` boundary semantics.
  Reads and read-timestamp scans at or below it are rejected; commits must be
  strictly above it. The floor advances monotonically and survives reopen.
- Pruning retains every version above the floor. At or below the floor it
  retains the newest value anchor required by later allowed reads and deletes
  older versions. When that anchor is a tombstone, the tombstone is deleted
  only after all older versions, so a crash cannot expose an older value;
  allowed reads then observe absence until a newer value exists.
- Pruning is a bounded reverse scan plus bounded delete batches. Durable mode
  re-syncs the floor before its first delete; interrupted runs are safe and
  idempotent. TreeDB snapshots acquired before floor advancement keep their
  pinned physical view until close.
- Floor advancement and pruning are serialized by one owner-local maintenance
  lock. With the qualified successor capability, pruning holds the Store
  floor lock exclusively through its scan, physical delete batches (including
  durable acknowledgement), and iterator/snapshot cleanup. Foreground reads,
  commits and snapshot acquisition wait; snapshots already pinned remain
  readable. This full-prune foreground fence is a correctness-driven ceiling,
  pending measurement before any narrower capture protocol. Adapters without
  the capability retain the earlier release after floor/snapshot capture and
  allow foreground operations during pruning. The maintenance lock prevents
  the captured floor from advancing underneath either prune path.
- Successful prune accounting satisfies `Visited = Retained + Pruned`.
  `Skipped` is the subset of `Retained` with timestamps above the captured
  floor, not a disjoint outcome counter. Partial-error statistics report only
  deletes whose batches committed, so the success equality need not yet hold.

Dgraph-specific metadata, TTL, subscriptions, and conditional transactions
remain separate contracts. `TreeDB/mvcc` does not reinterpret `EntryRevision`
and does not perform conflict detection.

## 2. Read Contracts

### 2.1 `Get`

- `Get(key)` returns `(value, nil)` when key exists.
- `Get(nil)` is equivalent to `Get([]byte{})` for raw KV point reads.
- A present zero-length value returns a non-nil zero-length byte slice.
- `Get(key)` returns `(nil, nil)` when key is absent.
- Returned bytes are safe copies.

Backend point reads search the leaf once, then decode value/pointer flags and
entry revision without materializing the matched key on columnar-v2 and
columnar+prefix-v2 leaves. The search validates key bounds and prefix metadata;
the value decoder validates value/pointer and revision bounds. Other encodings
retain their full entry decoder. This preserves owned results, present-empty and
tombstone behavior, checksum policy, and snapshot/leaf-view lease lifetimes.
`GetEntry` and iterators still materialize keys when their caller needs them.

### 2.2 `GetMany` and `GetManyView`

- `GetMany(keys)` returns one entry per input key. Missing keys are returned as
  `nil` entries with no error. Returned value bytes are safe copies.
- `GetManyView(keys, fn)` calls `fn(index, key, value, found)` once for each
  input key unless an error stops the call. The callback order is unspecified,
  and DB-level implementations may invoke callbacks concurrently for large
  batches; callers that mutate shared state in the callback must synchronize it.
  `index` identifies the input key.
- `GetManyView` reports missing or tombstoned keys with `found=false` and
  `value=nil`. Present empty values have `found=true` and `len(value)==0`.
- `GetManyView` value slices are read-only views valid only until the callback
  returns. Callers must copy a value before retaining it, mutating it, passing it
  to another goroutine, or using it after the owning DB/snapshot/view operation
  advances or closes.
- `GetManyView` must not weaken `GetMany`: callers that need stable ownership
  must continue to use `GetMany` or copy inside the callback.

### 2.3 `GetUnsafe`

- Public `DB.GetUnsafe` currently aliases `Get` behavior (safe copy).
- Zero-copy reads are available through snapshot/iterator internals and the
  callback-scoped `GetManyView` API.

### 2.4 `Has`

- `Has(key)` returns `(true, nil)` only for a visible non-deleted key.
- Deleted/missing keys return `(false, nil)`.

### 2.5 Cached-mode visibility

When the cached layer is enabled (default `treedb.Open` behavior):

- Point reads (`Get`, `GetMany`, `Has`, `GetAppend`) MUST reflect writes buffered in memtables (mutable + queued), even if they have not been flushed to the backend B+Tree yet.
- Newer memtable entries MUST shadow older backend state ("newest wins"), including tombstones.

### 2.5.1 Optional negative point lookup coverage

`Options.NegativeLookupFilterBytes` enables an in-memory, fixed-size monotonic
membership filter for the main backend. Zero disables it. Enabled budgets are
8 bytes through 64 MiB, rounded down to whole 64-bit words. Dictionary/template
side stores do not inherit this main-store budget. No on-disk format changes.

- Cached point reads inspect visible mutable and queued buffers before backend
  selection. Close, poison, admission, snapshot registration and lifetime checks
  still precede any negative answer.
- Only the raw tree attached to a coherent captured main-root view may reject a
  point key. Existing index identity and base/candidate root and commit-sequence
  coordinates prove publication coverage. Arbitrary roots, range/prefix scans
  and successor searches retain exact lookup. Raw tree `Reset` and `SetRoot`
  clear prior coverage; attaching coverage again requires an explicit complete
  filter for the selected root.
- Every normalized point mutation key, including deletes and every intermediate
  grouped batch, enters the filter before its root becomes visible. Abandoned
  writes may leave bits set. Bits never clear, resize or rebuild per commit.
- Old snapshots share monotonic bits safely: later inserts may add false
  positives but cannot invalidate prior coverage. Tombstones are included in
  bootstrap so versioned reads preserve stored tombstone revisions.
- Startup performs one bounded keys-only scan after recovery and replay
  finalizers. An incomplete/error scan authorizes no negative answers. At most
  `floor(budget*8/10)` entries and `budget*64` key bytes are admitted; keys longer
  than 1024 bytes always use exact lookup, including valid larger keys.
- Unknown root/index replacement removes coverage from the new view and its
  descendants. Later point deltas cannot restore it; reopening may bootstrap
  complete coverage again. Metadata-only publication may preserve coverage when
  main-root identity and commit sequence are unchanged. Explicit command-WAL
  current-root metadata publication (including checkpoint fallback refresh) may
  advance the sequence while carrying the existing exact base/candidate token
  under the durable publication gate. Supplied root candidates remain uncovered,
  even when their numeric IDs happen to match the current roots.
- Saturation safely degrades to exact lookups. Ordinary missing-value, empty
  value, callback ownership and revision contracts remain unchanged.

### 2.6 Target versioned entry reads

Target versioned entry APIs return the visible value together with an
`EntryRevision` suitable for cache validation and stale-read detection.

- `EntryRevision` is a monotonic token for a live raw-KV key. It advances when
  that key is overwritten or deleted and reinserted. Revision `0` is reserved
  for legacy/no-revision entries and must not be assigned to new raw mutations
  once versioned reads are advertised.
- `EntryRevision` is assigned from one persisted raw-KV revision domain for the
  directory. The active write authority may derive the mutation revision from the
  ordering source that makes the mutation visible: command-WAL LSN for
  command-WAL raw writes and replay, backend commit sequence for backend-only
  WAL-off raw writes, cached mutation sequence for cached WAL-off writes, and
  future Raft apply identity for consensus-applied raw writes. Those authorities
  are valid only when their allocator is seeded above the durable
  `MaxEntryRevision`/revision floor selected with the current roots, or when the
  accepted command carries an explicit effective mutation revision from that same
  domain. Cached mutation sequence is allocated before the memtable entry
  becomes visible and is later carried into backend publication. The revision is
  stored with the entry rather than recomputed by readers.
- Opening, replaying, restoring, or changing write profiles must not allow a
  later overwrite of a live key to receive a lower revision than an earlier
  visible value. If the implementation cannot prove the active authority is
  seeded above the persisted revision domain, versioned mutation support must
  fail closed before visibility.
- A snapshot reports the revision visible at the time that snapshot was
  acquired. Later writes do not mutate the revision observed through an older
  snapshot.
- Missing/tombstoned keys report the same not-found behavior as the matching
  existing point-read API and do not invent a live revision.
- Public safe-copy APIs return caller-owned key/value bytes. View APIs may
  return callback-scoped or iterator-scoped views, but revision metadata must
  not change the existing lifetime rules.
- Cached mode must include buffered memtable writes in versioned reads before
  the API is advertised as supported. Until then, public cached versioned reads
  must fail closed with a documented unsupported error rather than read only the
  backend root.
- Versioned reads must be a native read-path operation. Implementations must not
  require a second ordered-root lookup, a system-root sidecar lookup, or adapter
  private metadata storage to obtain the revision for the visible entry.

## 3. Write Contracts

### 3.1 Point ops

- `Set`, `Delete` are non-sync writes.
- `SetSync`, `DeleteSync` request sync durability boundary subject to durability mode.
- Point `Set`/`SetSync`/`Get`/`Has`/`Delete`/`DeleteSync`/`Update`/`UpdateSync` accept the empty key; nil point keys are canonicalized to the empty key.
- Point `Set`/`SetSync` and `Update`/`UpdateSync` set intents accept nil values and store them as zero-length values.
- `Update`, `UpdateSync` are single-key read-modify-write helpers. The callback
  receives the current value as a safe copy, or `nil` when the key is absent, and
  returns `Set`, `Delete`, or `Noop` intent through `UpdateResult`.
- Concurrent `Update`/`UpdateSync` calls for the same key on the same cached or
  backend `DB` handle are serialized around the read-modify-write sequence. This
  prevents lost updates for logical single-key mutations such as set-membership
  updates when competing writers use the update primitive.
- Point `Set`/`SetSync`/`Delete`/`DeleteSync` calls participate in the same
  single-key serialization on the same handle, but they remain unconditional
  writes. Batch writes and multi-key atomicity remain outside this contract.

### 3.2 Batches

- `NewBatch` accumulates operations.
- Batch `Set`, `Delete`, and `DeleteRange(start,end)` are applied in submission
  order as one atomic write unit.
- Batch point `Set`/`Delete` accept empty/nil point keys with the same point-key
  canonicalization as public raw KV point operations, and batch `Set` stores nil
  values as zero-length values.
- Batch `Replay` preserves empty keys and zero-length values.
- `DeleteRange` bounds are half-open `[start,end)`, with nil bounds unbounded.
  Empty concrete bounds remain concrete byte-string bounds; for example,
  `[nil, []byte{})` is empty because the end is exclusive and the empty key is
  the minimum key, while `[[]byte{}, end)` includes the empty key when `end` is
  greater than empty. Empty/reversed bounded ranges are no-ops.
- Cached batch `DeleteRange` uses a serialized materialization fallback and
  fails closed with `ErrBatchDeleteRangeTooLarge` if the bounded fallback cap is
  exceeded; the backend TreeDB path applies range deletes natively.
- `Write` follows the profile's ordinary ACK class: `command_wal_durable`
  covers a stable recoverable command-WAL prefix; `command_wal_relaxed` drains
  command-WAL buffers to the kernel and publishes without forcing fsync.
- `WriteSync` forces the profile's explicit durability boundary, including
  durable opt-up on a relaxed command-WAL profile. Neither command-WAL ACK
  class requires a per-call backend flush or checkpoint.

For WAL replay, commit-log batches are treated atomically at replay boundaries.

### 3.3 Target conditional raw-KV transactions

Target conditional transactions provide optimistic raw-KV commits with explicit
read/precondition validation.

- A transaction is created from a snapshot token that includes the commit
  sequence and root identity needed to validate its reads.
- `Get`, target versioned reads, and `Has` record point-read preconditions.
  Reading an absent key records an absent-key precondition.
- `Set` and `Delete` stage unconditional point mutations inside the
  transaction. Commit applies the staged mutations atomically only if all
  recorded preconditions still hold.
- If another committed write touches a key read by the transaction, commit
  returns `ErrConcurrentModification`. This includes overwrite, delete,
  absent-read insert, and insert/delete cycles that occur while the transaction
  is active.
- Disjoint concurrent transactions may both commit. The implementation may
  serialize the final root publish through the existing commit machinery, but it
  must not serialize the whole transaction body behind a coarse global
  transaction lock as the production design.
- Recent committed write fingerprints are retained only while active
  transactions can conflict with them. They are in-memory conflict-detection
  state, not persistent tombstones.
- Range reads and `DeleteRange` must either participate in documented range
  guards or fail closed with a stable unsupported/conflict error. They must not
  silently under-detect conflicts.
- Command-WAL-backed transactions must append deterministic logical command
  frames before visibility. Recovery replay must produce the same value and
  revision contract as live apply by using the accepted command LSN as the
  mutation revision only when the command LSN stream is seeded above the durable
  raw-KV revision floor; otherwise the accepted command input must carry the
  effective mutation revision from the same persisted revision domain.

## 4. Range and Iterator Contracts

### 4.1 Ordering

- `Iterator(start, end)` yields ascending lexicographic keys.
- `ReverseIterator(start, end)` yields descending order over the same bound domain.

### 4.2 Bounds

- Range domain is half-open: `[start, end)`.
- `nil` start means unbounded lower bound.
- `nil` end means unbounded upper bound.
- Non-nil empty bounds are concrete empty byte-string bounds, not unbounded sentinels.
- If both bounds are non-nil and `start >= end`, iterator is immediately invalid.

### 4.3 Iterator lifetime

- Iterators are point-in-time views.
- `Key()`/`Value()` data is a read-only view valid only until the next
  `Next`, `Seek`, iterator `Close`, or owning snapshot `Close`.
- Callers must not retain or mutate `Key()`/`Value()` views across iterator movement.
- `KeyCopy`/`ValueCopy` provide caller-owned stable copies that may be retained after movement/close.
- Iterator must be closed.

### 4.4 Seek

- `Seek` never changes the iterator's original half-open `[start,end)` domain.
- On a forward iterator, `Seek(target)` selects the least visible key greater
  than or equal to `target`.
- On a reverse iterator, `Seek(target)` selects the greatest visible key less
  than or equal to `target`; a nil target selects the greatest key in the
  domain.
- Seeking before or after the domain clamps to the first eligible key or makes
  the iterator invalid, respectively. Tombstones and shadowed source versions
  are never exposed.

### 4.5 Cached-mode iterator semantics

When the cached layer is enabled:

- Iterators MUST include buffered memtable writes (queued + rotated mutable state) and be snapshot-isolated.
- Iterators merge multiple sorted sources: immutable memtables (newest first) + a backend snapshot.
- When the same key exists in multiple sources, the newest entry wins; tombstones suppress older versions of the key.
- `ReverseIterator` follows the same visibility rules but yields keys in descending order.

## 5. Snapshot Contracts

- Exported snapshot handles are single-use and MUST NOT be recycled: a closed
  alias stays closed after any later snapshot acquisition.
- Backend `DB.Get`, `GetAppend`, `GetVersioned`, and `GetVersionedAppend` use a
  private pooled one-shot guard. It runs the same coherent capture and release
  as an exported snapshot: publication/root-reuse locking, acquisition epochs,
  commit-sequence registration, value-log set and leaf-generation pins, and
  close/poison rejection all apply. Retry-on-missing-file reads release the first
  capture before refresh and recapture. Its pointer MUST NOT reach callers,
  callbacks, root readers, or iterators; only owned results or caller-buffer
  appends may escape. Release clears captured state, reader interfaces, and the
  tree pager and negative-filter reference before reuse. Public snapshots, batched callback reads, and
  iterators continue to acquire fresh exported handles.

- Snapshots are point-in-time readers and MUST be closed to release retention pressure.
- `Snapshot.Iterator` and `Snapshot.ReverseIterator` bind iterators to that
  snapshot's pinned view. Closing the snapshot immediately invalidates every
  outstanding bound iterator. After snapshot closure, iterator movement and
  seek are no-ops, accessors return no view, `Valid` is false, and `Error`
  returns `ErrClosed`; iterator `Close` remains idempotent. Physical snapshot
  reclamation is deferred until those invalidated iterators are closed, so a
  close racing `Next` or `Seek` cannot recycle their backing snapshot.
- Snapshot iterator creation racing snapshot closure either returns a fully
  registered iterator or fails with `ErrClosed`; it must not return an iterator
  backed by a recycled snapshot object.
- In cached mode, snapshots MUST include buffered memtable writes and MUST be snapshot-isolated (writes after snapshot acquisition are not visible through the snapshot).
- `Snapshot.Get` / `Snapshot.GetAppend` return `ErrKeyNotFound` for missing/tombstoned keys (unlike `DB.Get`, which returns `(nil, nil)` on miss).
- `Snapshot.Get` returns caller-owned bytes that remain valid after subsequent
  reads and snapshot closure. Present empty values return a non-nil empty slice.
  Cached `Get` shares the `GetAppend` visibility and published/backend append
  path, then detaches its result from pooled scratch. `GetAppend` is the
  recommended point-read API when callers can reuse their own destination;
  owned `Get` still allocates storage for positive results. Entry-only published
  readers and active range spans retain their existing entry lookup fallback.
  Diagnostic hit attribution follows `GetAppend`: with active range spans,
  published entries found by its cached lookup are counted as cached hits.
- Under the planned user-command WAL contract, collection scans and snapshots
  that can read pending collection-local state use a `CollectionReadView`. The
  view pins backend snapshot state, pending mutable/queued/publishing
  collection units, derived index views, and reachable external refs until the
  read closes.

## 6. Concurrency and Locking

The internal immutable memtable foundation has separate writer-private and
published read headers, owned immutable payloads and finite generation/view
admission. Old views survive replacement, physical deletion and budget Close.
Budget Close refuses fresh root Retain and external allocation leases. Caller
wrappers/scratch allocated before Prepare require an external lease first;
its full lifetime shares bounded in-flight admission and ends after cleanup.
Estimate/refusal and immutable lookup/seek allocate no key conversions in either
build. `treedb_safe` uses byte comparisons over rank search at O(log N * height),
preserving admission and ownership without unsafe byte/string borrowing.
Private cancellation has no visibility; resource callbacks run only after final
ownership release and outside publication/admission locks. The full internal
contract is [immutable memtable ownership](cow-memtable-ownership.md). This
foundation adds no independently usable public DB mode or stronger public
durability/snapshot semantics before the coherent cached installer is integrated.

### 6.1 Process-level locking

- Read-write open acquires exclusive directory lock.
- Concurrent read-write opens on same DB directory are not allowed.

### 6.2 In-process concurrency

- Effective model is single-writer / multi-reader.
- Concurrent writers are serialized.
- Readers can run concurrently with writer via snapshot semantics.

## 7. Durability Contract Summary

Durability mode controls guarantees for sync calls:

- `DurabilityDurable`:
  - `*Sync` methods use fsync durability boundaries.
- `DurabilityWALOnRelaxed`:
  - In legacy/raw compatibility mode, WAL remains on, but `*Sync` is relaxed
    (no fsync boundary). In the current command-WAL relaxed profile, ordinary
    command frames remain relaxed while explicit sync APIs opt up to a durable
    V2 prefix.
- `DurabilityWALOffRelaxed`:
  - Production `no_wal_fast` supports authoritative data with volatile ordinary
    acknowledgement and durable explicit sync/checkpoint/clean-close boundaries.

Detailed semantics are in `TreeDB/docs/spec/write-path-and-durability.md`.

Collection APIs currently have a separate write-domain durability boundary:
acknowledged writes that remain in mutable, queued, or publishing
collection-local state are visible in-process but not durable-at-ack. That
current behavior is owned by `collections-write-domain.md`. The active target
for WAL-on durable-at-ack collection behavior is the user-command WAL contract
in `user-command-wal.md`; the older collection root-delta WAL plan is historical
context only.

### 7.1 Collection Write Acknowledgement Contract

Terms:

- `visible`: observable by collection reads, scans, secondary-index lookups,
  unique-index checks, update planners, delete planners, schema/index barriers,
  or pending-state merges in the serving process.
- `recoverable`: after process crash and read-write reopen, TreeDB can make the
  write visible without relying on pre-crash memory.
- `published`: root descriptors for the collection state have been committed to
  backend roots.
- `applied`: the backend durability boundary includes the published roots,
  required value-log/leaf-log/external-ref reachability, and `AppliedLSN` for
  the command LSN prefix.
- `fsynced`: the selected durable-mode fsync boundary has completed. Non-sync
  collection APIs do not imply fsync.

Current collection data mutators:

| Method | Current shipped contract |
|---|---|
| `Collection.Insert` / `InsertBatch` | Successful return means process-local visibility according to the path that executed it. Crash recovery is promised only after `Flush`, `FlushAll`, `Checkpoint`, `Close`, or a synchronous publish path covers the mutation. |
| `Collection.PrepareInsertBatchOwned` / `PreparedInsertBatch.Commit` | Preparation owns complete caller ID/document backings but has no visibility, LSN, asset, or durable acknowledgment. The caller keeps them unchanged until commit or abandon. `ErrPreparedInsertResourceLimit` is a fail-closed size/structural rejection; unsupported configurations return the distinct `ErrPreparedInsertIneligible`. `Commit` consumes the object once and uses the same ordered command-WAL publication and acknowledgment as `InsertBatch`; `Abandon` releases only private memory. The on-disk format, replay, and GC authority are unchanged. |
| `Collection.Update` / `UpdateBatch` | Successful return is process-local visibility until a public persistence boundary. Callback code is not durable replay input. |
| `Collection.Delete` / `DeleteBatch` | Successful return is process-local visibility until a public persistence boundary. |

Target user-command WAL mutators:

| Method family | Target command-WAL durable-at-ack contract | Benchmark/compatibility WAL-off relaxed contract |
|---|---|---|
| Insert by explicit ID | `CollectionInsertBatchByID` is one command frame with one LSN and all-or-nothing replay. Success/visibility waits for recoverable WAL plus normal-executor apply; checkpoint/cleanup later publishes roots plus `AppliedLSN`. | Process-local visibility until an explicit persistence boundary covers the batch. |
| Delete by explicit ID | `CollectionDeleteBatchByID` is one command frame with one LSN and all-or-nothing replay. Secondary-index deletes and tombstones are derived by the normal executor; checkpoint/cleanup later publishes roots plus `AppliedLSN`. | Process-local visibility until an explicit persistence boundary. |
| Declarative update by explicit ID | `CollectionUpdateByIDOps` logs canonical operators over resolved literal values. Resolver helpers run before WAL append and are never invoked during recovery. | Process-local visibility until an explicit persistence boundary. |
| Callback update by explicit ID | Callback identity is discarded before WAL append; the WAL logs final accepted replacements/no-ops or another stable replayable result. No Go callback is replayed. | Process-local visibility until an explicit persistence boundary. |
| Query-wide update/delete | Command-WAL durable-at-ack modes reject until a future command kind defines deterministic target ordering, preconditions, and result assertions. | May remain allowed only under benchmark/compatibility durability profiles that make no durable-at-ack claim. |

A target WAL-supported command may not return success or become owner-visible
from the WAL frame alone. V1 has no durable pending overlay: success/visibility
requires a recoverable command frame plus normal-executor apply. Checkpoint,
flush, close, synced ack, and recovery boundaries later publish roots, required
reachability metadata, and `AppliedLSN` in one backend durability boundary before
WAL cleanup. Unsupported command kinds must fail before staging, uniqueness
reservation, visible mutation, or external ref protection that would require
recovery.

Batch mutators are all-or-nothing at the command boundary. An ordinary
pre-commit error means no item from that batch became visible or recoverable. A
post-recoverable-frame failure must be reported as commit-ambiguous or
recovery-required for the whole command. Oversized WAL-on batches must be
rejected before LSN assignment unless the caller explicitly submits smaller
independent batches; implicit atomic splitting requires a future `CommandGroup`
protocol.

Collection metadata mutators:

`Collection.UpdateTypedMetadataByID` changes explicit existing IDs only, skipping
missing IDs and rejecting duplicates. It accepts non-overlapping `meta.*` paths,
requires non-null strings for declared scalar columns, and rejects content,
vector, text-indexed metadata and chunk-linkage changes. `expectedGeneration` is
the schema/index generation, not a document revision. Matched and modified counts
are separate; an unchanged admitted request creates no own WAL/publication. The
metadata batch is atomic with its scalar-index deltas. It retains canonical
content/vector authority without serializing or re-admitting unchanged vectors;
pinned readers see their original metadata and data. HTTP/native/Python use the
same operation without a vector parameter or automatic retry after ambiguous
commit. See the normalized-vector and storage-format contracts for cold reads
and fold costs.

| Method | Contract |
|---|---|
| `CollectionManager.CreateCollection` | WAL-off behavior publishes the catalog entry under the selected non-sync durability mode. Command-WAL behavior uses a `CatalogCreateCollection` frame and publishes the catalog root plus `AppliedCommandLSN` in one backend tuple. Retrying creation with identical metadata is idempotent; retrying with incompatible metadata fails without changing the existing collection. |
| `Collection.CreateIndex` and index-drop methods | These methods establish an index/schema barrier. They drain collection writes admitted before the barrier before taking the backfill/planning snapshot, or target a `CatalogMutation` command frame that carries descriptor changes atomically. A unique-index conflict is an ordinary pre-commit error and must not expose partial schema/index state. |

Barrier methods:

| Method | Contract |
|---|---|
| `Collection.Flush` | Publishes all pending collection write-domain state for that collection admitted before the flush cut and waits for in-flight async indexed publishing for that collection. It does not imply fsync unless composed with a sync/checkpoint boundary that requires fsync. Under command WAL, it may also serve as a publish boundary for commands admitted before the cut only if it advances `AppliedLSN` atomically with the roots. |
| `CollectionManager.FlushAll` | Applies the `Flush` contract to every collection write domain known to the manager at the flush cut. Future backend-owned command WAL services must use a global domain registry when the contract needs to cover all collection handles, not only one manager instance. |
| `DB.Checkpoint` | In WAL-free mode, drains acknowledged local writes and dirty native vector state from registered collection managers before sealing a backend root. Command WAL cleanup boundary: successful return may report clean only when every pre-cut command frame required for recovery is covered by durable roots plus `AppliedLSN`, and when cleanup metadata for omitted WAL ranges is durable. If publication/`AppliedLSN`/checkpoint coverage cannot be completed, `Checkpoint` must return an error unless a future result type explicitly exposes nonzero command WAL debt. |
| Document service `OptimizeIndex` after deferred inserts | Drains collection work, crosses `DB.Checkpoint`, then performs any debt-qualified vacuum and publishes query-ready vector assets. Once the checkpoint succeeds, accepted documents survive process crash even if later optimization fails; only a successful Optimize response promises a clean query-ready vector generation. |
| `DB.Close` | Establishes a close admission cut. Every collection write racing with the cut either fails before visible install with `ErrClosed` or is included in the close drain. If `Close` returns `nil`, every included successful collection mutation is visible after read-write reopen. Physical cleanup may remain a safe leak, but WAL/external refs needed for recovery must not be removed. |

Public durability errors:

- `ErrDurabilityUnavailable`: a requested durability boundary cannot be
  satisfied before a complete command frame becomes recoverable.
- `CommitAmbiguousError` / `ErrCommitAmbiguous`: the mutation may already be
  recoverable or committed. Callers must not blindly retry non-idempotent
  operations.
- `ErrRecoveryRequired`: read-only open found complete command WAL with
  `LSN > AppliedLSN` that requires mutating recovery before collection APIs can
  be served.

Retry guidance:

- TreeDB does not provide exactly-once mutation semantics without a durable
  idempotency key.
- After timeout, disconnect, or commit-ambiguous error, duplicate document ID or
  unique-index conflict can mean the prior attempt committed.
- Updates are safe to retry only when the update operation is idempotent,
  guarded by a version/compare predicate, or protected by a durable idempotency
  key.

Mongo gateway:

- Standalone absent/default, `{w: 1}`, `j: false`, and `wtimeout: 0`
  acknowledgements use the ordinary boundary of the configured durability
  profile. In particular, `command_wal_relaxed` and `no_wal_fast` do not gain a
  stronger promise from an ordinary acknowledgement.
- A successful standalone `j: true` acknowledgement first drains collection
  publishing, then completes `SyncCommandWALAppliedPrefix` when command WAL is
  enabled or `DB.Checkpoint` when it is disabled. The command-WAL boundary
  orders persistent value-log dependencies, syncs a relaxed suffix, and
  publishes its durable-prefix barrier as a root-neutral contiguous applied
  command; an already-durable prefix is reused without another barrier or
  file sync. Because existing multi-item and DDL handlers can return a command
  error after a partial mutation, an accepted `j: true` request closes this
  boundary even when the handler response is `ok: 0`; the original command
  error is preserved. This is a local crash/reopen boundary only; it does not
  imply replica commit, retryability, transactions, or a stronger read concern.
- Unsupported or malformed standalone concerns reject before mutation. A
  post-mutation failure to close the requested sync boundary reports an
  explicit uncertain `writeConcernError`; clients must apply the ambiguous
  retry guidance above.
- Standalone OP_MSG writes with `moreToCome` reject before dispatch with no
  response and no mutation, including crafted absent/default or `{w: 1}`
  commands as well as the normal `{w: 0}` shape. The connection remains usable.
- Ordered update/delete commands must document whether earlier items can remain
  applied after a later item error. If partial ordered semantics are intentional,
  responses must report the applied prefix; otherwise validation must complete
  before applying any item.
- Gateway collection auto-create and `createIndexes` inherit the collection
  metadata mutator contracts above.

## 8. Read-only Open Contract

When opened read-only:

- write operations must fail,
- no mutating recovery steps run,
- no background maintenance mutates on-disk state.
- if complete command WAL frames exist with `LSN > AppliedLSN`, read-only open
  must fail with `ErrRecoveryRequired` unless an explicit stale read-only mode is
  selected.
- stale read-only mode, if added, must be explicitly named stale, must report
  command WAL debt, and must be rejected by backup and offline maintenance entry
  points.

## 9. Collections Native Fast Path

Collections runtime code uses the native ordered-root publish path as the
default execution path. The historical oracle branch is an external comparison
artifact only; it is not a runtime selector or dependency in the collections
package.

Collection root physical policy is explicit per root:

- document/data roots are the production-mainline roots and benchmark defaults
  prioritize value-log-backed outer leaves,
- collection index-state roots follow the collection data-root policy,
- secondary index roots support both pager-backed fast mode and value-log-backed
  compressed mode,
- benchmark artifacts must label the storage-policy cell being measured.

Secondary indexes are typed. Every index definition must declare one of
`string`, `bool`, `int64`, or `double`; missing or unknown value types are
schema errors. Ordered secondary keys store the typed value component followed by
the document ID, so old untyped secondary-index metadata and key layouts are
intentionally incompatible with this format.

Native collection writes must publish primary, index-state, secondary, and root
descriptor updates through grouped ordered-root publish primitives. They must not
route the steady-state runtime path through oracle selectors, detached replay,
overlay state, or other translation-only hooks.

Indexed collection writes use collection-local write memtables by default.
Pending indexed writes are visible through the owning collection manager before
they are published to persisted roots. Primary reads, secondary index lookups,
unique checks, and update/delete planning must merge write-domain state with
explicit newest-to-oldest precedence: current mutable runs, queued immutable
flush units, in-flight async publishing units, then persisted roots.

`BufferedIndexedAsyncFlush` is enabled by default for indexed schemas. It is a
throughput feature, not a durable-at-ack mutation log. The current contract is
durability-boundary durable: callers may treat `Collection.Flush`,
`CollectionManager.FlushAll`, or backend `DB.Close` as collection write-domain
durability boundaries when those operations return successfully. Foreground or
background threshold publish may also move a subset of staged writes into
persisted roots, but completing such a publish must not be described as a
per-update durability guarantee for earlier acknowledgements. Acknowledged writes
that remain only in mutable, queued, or publishing write-domain state must not be
advertised as crash-durable.

Operations that need persisted roots as planning input, including schema/index
changes, must drain pending indexed write-domain state and wait for in-flight
async publish units before taking their planning snapshot.

Under the planned user-command WAL contract, command-WAL collection visibility
implies recoverability. No read, uniqueness check, update/delete planner, or
pending merge may observe a mutation before its typed command WAL frame is
committed/recoverable.

Detailed indexed collection write-domain semantics are in
`TreeDB/docs/spec/collections-write-domain.md`.

### Split-source insert first checkpoint

The authenticated fixed-peer insert-only split checkpoint binds an exact
canonical source operation to one distinct mutable ANN target. A successful
response proves durable source publication, target graph receipt and source
intent retirement, and returns an owned bounded visibility token. Strict
search carrying that token requires fresh current-DB/ACTIVE and real target
receipt quorum/applied fences before search and final response; unsupported
generic/fast/pinned paths refuse the token. Ambiguous completion preserves the
durable pending intent for retry rather than reporting an unproved success.
This fixed identity accepts one pending and at most 64 completed operations,
with no completed-outcome eviction. This is a bounded checkpoint, not general
indefinite write capacity. See [the split insert contract](vector-partition-split-source-insert-v1.md).

### Mixed colocated qualification diagnostics

Optional version1 `ColocatedAudit` on existing fixed-peer diagnostics is bounded
to six original outcomes and final known IDs, with an optional complete current
source-vector `Population` expectation (524288 encoded plan bytes). A population-only
initial attachment requires empty outcome/final ledgers, zero highest new commit
and a positive applied floor; partial ledgers cannot downgrade. Its
current-FSM/ACTIVE/root/applied/physical-WAL/summary fences and prepared-owner
source/live proof are mandatory; stale or torn evidence is refused. Ordinary
serving/ordinary diagnostics and mutation formats are unchanged. The optional
population observation hashes every current primary/overlay ID and raw FP32
vector against independently retained full-oracle rows/count/digest; it proves
source vectors only, not full-document or reverse live-graph equality or a
distributed quiescence mechanism. All four RF4 voters must remain
live through audit acquisition; root owns stop/join/catchup and clean shutdown
verification. The mixed-window README and schema specify retained attempts,
causal prefixes, original retries, observation limits and failure consumption.
The [runtime specification](fixed-peer-tcp-runtime-v1.md) defines the supported
bounded projection, exact encoding, mandatory limits, source-record accounting,
and untimed admitted-scan cost. Qualification remains owned by #4250.
