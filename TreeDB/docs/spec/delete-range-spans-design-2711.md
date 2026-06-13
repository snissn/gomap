# TreeDB DeleteRange Range-Span Design (#2711)

Status: design proposal / implementation gate. This document does **not** make
TreeDB public-testnet-ready, does not make TreeDB the geth default, and does not
weaken checksum-verified value-log reads.

Tracker: <https://github.com/snissn/gomap/issues/2711>

Parent context: #2676, #2704, PR #2712. PR #2712 added counters proving that the
geth-hot-KV shape still materializes point tombstones in cached command-WAL mode:
for the representative 1M/400k run, `delete_range_visited_keys=1,000,000`,
`delete_range_tombstone_keys=1,000,000`, and
`delete_range_materialized_keys=500,000`.

## 1. Design gate verdict

Broad on-disk range-tombstone implementation is **not** safe to start before this
design is reviewed. A safe first implementation slice is a cached-layer
range-span overlay plus tests/counters, because it can avoid changing B-tree page
encoding and can flush spans through the existing backend `Batch.DeleteRange` /
zipper range path.

R1 SHOULD target the geth-hot-KV cached command-WAL path and preserve existing
backend/index formats:

1. keep durable `RawKVBatch` command-WAL `DeleteRange` frames as the recovery
   record;
2. store active range deletes as immutable cached range-span layers instead of
   per-key point tombstones;
3. apply those spans to backend roots at flush/checkpoint through existing
   backend range-delete apply (`batch.OpDeleteRange` / `zipper.Apply` ranges);
4. defer persistent on-disk range tombstone/page-format work until benchmarks
   show checkpoint-time backend range application is the next bottleneck.

The design intentionally treats range spans as cached visibility overlays, not
as long-term persistent index records in R1. The command WAL remains the durable
replay source until spans are checkpointed into backend roots.

## 2. Current behavior summary

Relevant code paths at `9166c6ca`:

- Batch range model: `TreeDB/batch/batch.go`
  - `DeleteRange` records `OpDeleteRange` with half-open `[start,end)` bounds.
  - `BuildApplyPlanFromEntries` removes point ops shadowed by later ranges and
    returns final point ops plus exact adjacent/overlap coalesced ranges.
  - `MergeDeleteRanges` coalesces only overlapping/touching ranges.
- Cached DB-level range delete: `TreeDB/caching/db.go:22122+`
  - command-WAL cached mode appends a raw KV `DeleteRange` frame before memory
    visibility via `DeleteRangeAfterCommandWALAppend`.
  - current cached fallback scans keys and writes point tombstones.
- Cached batch range delete: `TreeDB/caching/db.go:30211+`,
  `writeRangeBatch` at `30617+`
  - current range batches scan `db.Iterator` for each effective range and append
    materialized `OpDelete` point tombstones before writing to the mutable
    memtable.
- Backend range application: `TreeDB/db/batch.go` and `TreeDB/zipper/zipper.go`
  - backend `Batch` preserves `OpDeleteRange` in the apply plan;
  - zipper streams affected spans and drops covered leaf entries without storing
    persistent tombstones.
- Public command WAL: `TreeDB/command_wal_public_cached.go` and
  `TreeDB/db/command_wal_raw.go`
  - raw KV command payloads encode `RawKVOpDeleteRange`;
  - recovery replays frames through backend `Batch.DeleteRange`, so unapplied
    frames already avoid cached point materialization during open-time replay.

## 3. Bound semantics

Range spans MUST use the existing public DeleteRange semantics:

- Range is half-open `[start,end)`.
- `nil` start means unbounded lower.
- `nil` end means unbounded upper.
- concrete empty key `[]byte{}` is the minimum valid key and is distinct from
  `nil`.
- API/overlay no-op ranges:
  - non-nil `start >= end`;
  - `start == nil && end != nil && len(end) == 0`.
- Cached overlay and public batch paths MUST normalize/drop those no-op ranges
  before command-WAL encoding. A command-WAL frame containing
  `start == nil && end == []byte{}` or a concrete reversed/equal bounded range is
  non-canonical and MUST fail closed as corrupt during command-frame validation
  and replay.
- `DeleteRange([]byte{}, []byte("a"))` includes the empty concrete key.
- `DeleteRange(nil, nil)` covers the full keyspace.
- Adjacent and overlapping ranges MAY be merged exactly; ranges separated by a
  keyspace gap MUST NOT be merged unless every possible key in the gap is known
  absent for the covered visibility epoch.
- Duplicate ranges are equivalent to one range, subject to ordered interaction
  with point writes in the same batch.

## 4. Visibility model

### 4.1 Layered state

R1 should introduce a cached `rangeLayer` or equivalent immutable overlay:

```text
range layer:
  epoch / priority
  point entries from the same canonicalized batch, partitioned by shard when useful
  sorted non-overlapping spans from the same canonicalized batch
  min/max key coverage for cheap disjoint checks
  WAL/value-log path retention metadata
```

Each layer is immutable after publication. Newer layers have higher precedence.
Range-only layers count as in-memory state even when they contain zero point
entries. The existing mutable memtables, queued immutable memtables, and
published backend roots remain lower-level data sources.

For correctness without per-op sequence numbers, R1 should apply range batches by
publishing range spans at a layer boundary:

1. serialize with writers/flushers;
2. rotate or freeze the current mutable point state so existing points are older
   than the range layer;
3. canonicalize the incoming mixed batch with `BuildApplyPlanFromEntries`;
4. publish one immutable layer containing the merged spans and surviving point
   ops from that batch;
5. start a fresh mutable point state for later writes.

Within one canonicalized layer, point entries win over spans in that same layer.
This preserves ordered batch semantics because `BuildApplyPlanFromEntries`
removes only points shadowed by later ranges; surviving points are either after
all covering ranges or outside them.

### 4.2 Point `Get` and `Has`

For `Get`, `GetAppend`, `GetMany`, `GetManyView`, `Has`, `HasMany`, and
snapshot equivalents, the lookup rule is:

1. scan active cached layers from newest to oldest;
2. if a point entry for the key exists in a layer, return it (`Put` visible,
   point tombstone missing);
3. otherwise, if that layer contains a range span covering the key, return
   missing and do not consult older layers or backend;
4. if no cached layer resolves the key, consult the published/backend root.

Fast-path bypasses such as `canBypassMemtableRead` MUST also prove there are no
active range spans covering the key (or no active spans at all) before skipping
cached state. `AcquireBackendSnapshotFastPath` and any raw backend snapshot/read
shortcut MUST be disabled when cached range spans are present, unless the
shortcut can prove the spans are disjoint from the query. `Has`, `HasMany`, and
`HasPrefixes` MUST not route directly to backend when an active span could hide a
backend key.

### 4.3 Forward and reverse iteration

Iterators must remain point-in-time views over `[start,end)` bounds.

The merging iterator must become range-aware. For each candidate winner key from
source priority `p`:

1. consume lower-priority duplicate point entries as today;
2. skip point tombstone winners as today;
3. check whether any higher-priority range layer covers the key;
4. if covered, advance the winner and continue;
5. if not covered, yield the key/value.

For a candidate from the same layer as a span, the same-layer point wins; only
higher-priority spans hide it. For a candidate from backend or older queue
layers, all newer spans can hide it.

Reverse iteration uses the same rule while preserving reverse bounds: `start` is
inclusive lower bound, `end` is exclusive upper bound, and keys `>= end` are
skipped/advanced.

The implementation MUST NOT raise traversal/depth guardrails to hide missed
visibility cases.

### 4.4 Snapshots and concurrent readers

Snapshots and iterators currently retain an immutable `memtableView`. Range-span
state must be part of the same RCU-published view, or retained by an equivalent
view object, so a snapshot sees exactly the span layers present at snapshot
creation.

Requirements:

- `AcquireSnapshot`/`Iterator` rotation captures active mutable range layers
  before returning.
- A range delete after snapshot creation is invisible to that snapshot.
- A point put after snapshot creation is invisible to that snapshot even if it
  would override a range in the live DB.
- Retired span layers are not freed until all views that can reference them are
  released.
- Existing memtable-view deferred-retirement telemetry should either include
  span-layer bytes/counts or expose parallel counters.

## 5. WAL, command-WAL, and recovery

### 5.1 Cached command-WAL mode

R1 target path:

- DB-level `DeleteRange` first drops API no-op ranges, then appends
  `RawKVBatch(DeleteRange)` before publishing a range layer.
- Batch `DeleteRange` first canonicalizes/drops API no-op ranges, then appends
  the original canonical command payload before publishing the corresponding
  range layer and point survivors.
- If command WAL append succeeds but layer publication/apply fails, the handle
  MUST be poisoned with recovery-required state, matching current
  `MarkCommandWALRecoveryRequired` behavior.
- Checkpoint publication (`AppliedCommandLSN`) MUST occur only in the backend
  commit that also contains all range-layer effects covered by that LSN.

Open-time replay of unapplied command-WAL `DeleteRange` frames can continue to
use backend `Batch.DeleteRange`, because recovery replays into backend roots
before serving the DB.

### 5.2 Legacy cached redo WAL

If range spans are enabled for non-command cached WAL modes, the redo journal
must gain a durable range-delete record (`logOpDeleteRange`) with the same bound
encoding and no-op validation. Replaying that record should apply backend
`Batch.DeleteRange` or rebuild a cached span layer before serving.

Until that is implemented and crash-tested, R1 may scope span overlays to
command-WAL cached mode and leave legacy WAL `DeleteRange` on the current
point-tombstone fallback.

### 5.3 WAL-off relaxed mode

WAL-off cached mode may use span layers for performance, but they are only
flush/checkpoint durable. Value-log bytes remain persistent storage. A crash may
lose uncheckpointed WAL-off span layers exactly as it can lose uncheckpointed
WAL-off point writes.

## 6. Flush, checkpoint, and backend roots

Range layers must flush as logical ranges, not as materialized point deletes.

Flush requirements:

- Queue units must be able to carry range spans even when their point memtable is
  empty. A range-only unit MUST count as pending flush work and MUST NOT be
  dropped by the current `totalLen == 0` fast path.
- Backend batches should receive operations in layer order. For one canonical
  layer, emit range ops first and surviving point ops after them so same-layer
  points override earlier ranges.
- Combining multiple queued units is allowed only when layer order is preserved.
  A later range may delete earlier flushed/backend keys; a later point may
  override an earlier range.
- Chunked backend commits must not split one range layer in a way that publishes
  `AppliedCommandLSN` before the layer's range effect is durable.
- Checkpoint must rotate/freeze active spans, flush all queued span layers, and
  then publish any command-WAL coverage atomically with the backend root that
  contains those spans.

Because the backend zipper already accepts `batch.DeleteRange`, R1 does not need
an index-page tombstone format. Backend roots after flush/checkpoint contain the
post-delete key set, not persistent tombstone records.

## 7. Persistent format and migration implications

R1 cached spans do not add a new durable index/page format. The durable record is
already the command-WAL `RawKVBatch(DeleteRange)` frame until flush/checkpoint,
and the post-checkpoint backend root contains ordinary point entries with covered
keys omitted. Therefore R1 should not need a format-config bump for index page
encoding.

Future persistent span options, ranked from lower to higher format risk:

1. system-root or dedicated ordered-root span records keyed by span start/end and
   epoch;
2. sidecar span manifest/file with explicit atomic root/meta coupling;
3. reserved synthetic records in user keyspace;
4. native leaf/internal page range-tombstone records.

Any future option that persists spans beyond cached overlays MUST update
`storage-format.md`, recovery, backup/restore, value-log lifecycle, and
verification docs in the same PR. Unknown required span formats must fail closed
on open; TreeDB is pre-alpha, so no complex migration scaffolding is required.

## 8. Value-log reachability, GC, rewrite, and compaction

Value-log pointers are persistent. Range spans MUST NOT cause GC/rewrite to
remove a segment that can still be read by any live root, snapshot, iterator, or
uncheckpointed recovery path.

R1 safety rule:

- Pending cached range spans are visibility overlays only.
- Covered backend/index pointers remain conservatively reachable for value-log
  GC/rewrite until the span layer is flushed/checkpointed into backend roots.
- Strict `ValueLogGC`, `ValueLogRewriteOnline`, and `CompactStorage` already
  checkpoint cached state first; after that, normal backend reachability scans
  see only surviving pointers.
- Online GC without checkpoint must either treat pending span layers as blockers
  or fail closed/dry-run; it must not delete covered backend pointers solely
  because a pending cached span hides them from live reads.
- Rewrite must not patch or drop value-log records protected only by pending
  command WAL or live cached span layers.

After range layers are checkpointed, hidden old keys are no longer present in the
backend root. Existing reachability scans and incremental ref-count deltas can
then mark their value-log segments unreferenced.

Implementation follow-up: if backend range apply uses
`buildValueLogRefDelta`, the delta path already scans the affected backend
range to decrement old pointer counts. That scan is checkpoint-time backend work,
not DeleteRange-call materialization. Benchmark evidence should decide whether a
later range-aware ref-count optimization is needed.

## 9. Storage reclaim implications

R1 reduces DeleteRange call-time CPU/allocation and point-tombstone memory
pressure. It does not promise immediate file shrinkage at the DeleteRange return
boundary.

Expected reclaim sequence:

1. DeleteRange publishes cached spans quickly.
2. Reads/iterators hide covered keys immediately.
3. Close/checkpoint/flush applies spans to backend roots.
4. `ValueLogGC` or `CompactStorage` reclaims value-log segments once no live root
   or retained view references them.
5. `CompactIndex` / full `CompactStorage` may still be required to reduce
   `index.db` high-watermark after large destructive workloads.

Benchmark reports should therefore include both `post_delete_size_bytes` and
whether close/checkpoint/GC/CompactStorage ran before measuring it.

## 10. Observability and counters

Existing #2712 counters remain required and should show the improvement:

- `treedb.cache.delete_range.input_ranges_total`
- `treedb.cache.delete_range.effective_ranges_total`
- `treedb.cache.delete_range.coalesced_ranges_total`
- `treedb.cache.delete_range.iterators_total`
- `treedb.cache.delete_range.visited_keys_total`
- `treedb.cache.delete_range.tombstone_keys_total`
- `treedb.cache.delete_range.materialized_keys_total`
- `treedb.cache.delete_range.materialized_key_bytes_total`

R1 should add or derive:

- `range_span_layers_total` / current active layers;
- `range_span_input_total` and `range_span_effective_total`;
- `range_span_keys_materialized_total` (should remain zero on the span path);
- `range_span_point_overrides_total` for same-layer point survivors;
- point read span probes / hits;
- iterator candidate span probes / skipped keys;
- range-only queued units flushed;
- range spans flushed to backend;
- checkpoint-time backend range-apply keys/pointers scanned, if cheaply
  available from zipper/ref-delta metrics.

For geth-hot-KV, the acceptance signal is: same submitted ranges, but
`delete_range_materialized_keys_total` and call-time per-range iterator counters
fall toward zero on the DeleteRange phase, while correctness/reopen tests pass.

## 11. Required tests before implementation mergeability

Minimum test matrix:

1. Bound semantics:
   - no-op reversed/equal ranges;
   - API/cache overlay treats `DeleteRange(nil, []byte{})` as no-op and never
     emits that non-canonical command-WAL frame;
   - command-WAL validation/replay rejects `nil` start plus concrete empty end
     if such a frame is encountered;
   - `DeleteRange([]byte{}, []byte("a"))` deletes the empty concrete key;
   - `DeleteRange(nil, nil)` full keyspace;
   - adjacent/overlap/duplicate ranges.
2. Interleaved point operations:
   - puts/deletes before ranges are hidden;
   - puts/deletes after ranges survive according to point semantics;
   - points between multiple ranges are hidden only by later covering ranges.
3. DB-level and batch `DeleteRange` in cached command-WAL mode.
4. Reopen/crash replay with unapplied raw KV `DeleteRange` frames.
5. Value-log pointer keys deleted by ranges:
   - close/reopen reads;
   - `ValueLogGC` after checkpoint deletes only unreachable segments;
   - rewrite/CompactStorage retains reachable pointers.
6. Forward and reverse iterators with active range spans, including keys from
   backend, queued layers, and newer mutable points.
7. Snapshot isolation:
   - snapshot before range still sees old keys;
   - live DB after range hides them;
   - snapshot after range sees range effects;
   - concurrent iterator retains span-layer lifetime.
8. Checkpoint publication:
   - range-only queued units are not dropped;
   - `AppliedCommandLSN` is advanced only with backend roots containing the
     range effects.
9. Regression/parity tests for existing backend `Batch.DeleteRange` and raw KV
   command-WAL decode/replay paths.

Suggested packages:

- `TreeDB/caching` for span-layer read/iterator/snapshot tests;
- `TreeDB` public command-WAL tests for DB/batch/reopen behavior;
- `TreeDB/db` for backend range/ref-count/recovery interactions;
- `benchmarks/geth_hot_kv` script/harness tests for counter columns.

## 12. Benchmark evidence gate

Implementation PRs must run the issue-prescribed shape from the latest head and
compare against an identical baseline:

```sh
# Set GETH_REPO to a local go-ethereum checkout.
GETH_REPO=/path/to/go-ethereum \
ENGINES=treedb \
KEYS=30000 READS=12000 \
KEY_SHAPES=geth-mixed VALUE_SHAPES=geth-mixed VALUE_SIZES=128 \
BATCH_TARGET_BYTES=102400 \
TREEDB_READ_INTEGRITIES=verify \
ITERATION_MODES=value \
  scripts/treedb_geth_hot_kv_matrix.sh
```

Repeat at `KEYS=1000000 READS=400000` if practical.

Report:

- DeleteRange keys/sec;
- phase allocation/malloc deltas if available;
- `phase_counters.tsv` DeleteRange counters;
- post-delete bytes and what durability/reclaim boundary produced them;
- write/read/iterate correctness and performance regression assessment;
- checksum read-integrity mode (`verify`, not unsafe skip-checksum).

## 13. Non-goals

- No checksum-default weakening.
- No TreeDB default-engine promotion for geth.
- No public-testnet readiness claim.
- No traversal/depth guardrail increase.
- No broad B-tree page-format rewrite in R1.
- No persistent verified-forever value-log checksum cache.
- No #2709 write-copy-pressure work or #2721/#2722 locality/verification work in
  this lane.
