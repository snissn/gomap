# codex resume 019c00be-d055-7923-9519-53e8eff550e0

• # Plan: TreeDB Keyspace Sharding via Automatic Range Partitions

  ## Goals / Success Criteria

  - Transparently shard TreeDB’s user keyspace into range partitions (ordered, non-overlapping) so each partition is
    stored in its own independent B+Tree root.
  - Achieve near-linear reduction in per-write / per-read B+Tree work as partitions increase (smaller trees), and
    enable parallel apply across partitions for batches/flushes.
  - Make partition count automatic (db-managed), with minimal configuration, using write contention as the primary
    signal (per your selection).
  - Support both scale up (split) and scale down (merge) with gating/cooldowns to avoid thrashing.
  - Preserve existing TreeDB semantics:
      - Batch.Write() remains atomic across the whole keyspace.
      - Iteration order remains lexicographic over full keyspace.
      - Reopen/recovery remains correct.

  Non-goals (explicit):

  - No HashDB changes.
  - No public API changes required to read/write keys (feature is transparent).
  - No attempt to do zero-copy split; split/merge are rebuild operations.

  ———

  ## Architecture Overview

  ### Core Concept: Partitioned User Keyspace

  Represent the user keyspace as N ordered partitions. Each partition has:

  - Start (inclusive; nil for -inf)
  - Root (page id of that partition’s B+Tree)

  Partition i covers:

  - [Start[i], Start[i+1]) for i < N-1
  - [Start[i], +inf) for last

  Invariant: Each partition’s B+Tree contains only keys within its range.

  ### Metadata Storage (Persistent)

  Persist the partition layout in the System Tree (rooted at Meta.SystemRootPageID) under a single fixed internal
  key.

  - File: TreeDB/db/keyspace_meta.go (new)
  - Key (exact bytes): []byte{0x00, 'k','s','p','a','c','e', 0x01} (0x00| "kspace" | 0x01)
  - Value encoding: versioned binary (see “Encoding” below)

  Rationale:

  - No MetaPage format change
  - Atomic update alongside commits via existing finalizeCommit(newUserRoot, newSysRoot, ...)

  ### In-Memory Publication (RCU)

  Extend DBState (atomic pointer) to include the decoded keyspace layout, so snapshots/reads never consult disk
  metadata.

  - File: TreeDB/db/db.go
  - Modify:
      - type DBState struct { ... Keyspace *keyspace.Layout }
      - RootPageID remains for compatibility/diagnostics and is set to Keyspace.Parts[0].Root when sharded.

  ———

  ## Data Structures (Decision-Complete)

  ### New Internal Package: TreeDB/internal/keyspace

  Files:

  - TreeDB/internal/keyspace/layout.go
  - TreeDB/internal/keyspace/codec.go

  Types:

  package keyspace

  type Part struct {
      Start []byte // inclusive; nil means -inf (encoded as len=0)
      Root  uint64 // partition B+Tree root page id
  }

  type Layout struct {
      Version uint8  // currently 1
      Epoch   uint64 // increments only when boundaries/count change (split/merge), not on root updates
      Parts   []Part // sorted by Start; Parts[0].Start must be nil; len>=1
  }

  func Single(root uint64) *Layout
  func (l *Layout) Clone() *Layout // deep copy (starts copied)
  func (l *Layout) N() int
  func (l *Layout) Start(i int) []byte
  func (l *Layout) End(i int) []byte // derived from next Start or nil
  func (l *Layout) PartitionForKey(key []byte) int // binary search, rightmost Start <= key
  func (l *Layout) PartitionsForRange(start, end []byte) (first, last int) // inclusive indices
  func (l *Layout) Validate() error

  Encoding (binary, little endian):

  - u8 version (=1)
  - u64 epoch
  - u16 nParts
  - For each part:
      - u16 startLen
      - []byte start
      - u64 root

  Validation rules:

  - nParts >= 1
  - Parts[0].Start == nil
  - Starts strictly increasing (bytes.Compare(prev, cur) < 0)
  - Roots are non-zero and rootPageValid(...) on load

  Codec functions:

  func Encode(l *Layout) []byte
  func Decode(b []byte) (*Layout, error)

  ———

  ## Snapshot / Read Path Changes

  ### Snapshot becomes Partition-Aware

  File: TreeDB/db/db.go

  Modify:

  - type Snapshot struct adds:
      - keyspace *keyspace.Layout
      - trees []tree.Tree (one per partition; reused via pool)
      - router partitionRouter (small struct with methods)

  On AcquireSnapshot():

  - Load state := db.state.Load()
  - If state.Keyspace == nil => behave like today (single snap.tree)
  - Else:
      - snap.keyspace = state.Keyspace
      - Ensure len(snap.trees) == keyspace.N()
      - For each partition i, snap.trees[i].Reset(idx.pager, valueReader{...}, keyspace.Parts[i].Root)

  ### Router API (internal)

  File: TreeDB/db/keyspace_router.go (new)

  type partitionRouter struct {
      layout *keyspace.Layout
      trees  []tree.Tree
  }

  func (r *partitionRouter) Get(key []byte) ([]byte, error)
  func (r *partitionRouter) GetEntry(key []byte) (node.LeafEntry, error)
  func (r *partitionRouter) Has(key []byte) (bool, error)
  func (r *partitionRouter) Iterator(start, end []byte) iterator.UnsafeIterator
  func (r *partitionRouter) ReverseIterator(start, end []byte) iterator.UnsafeIterator

  Iterator strategy (range partitioning advantage):

  - For [start,end) spanning multiple partitions: create a sequential concatenation iterator over partition
    iterators in order (no heap merge needed).
  - Must implement correct Seek() across partitions.

  ### Iterator Implementation

  File: TreeDB/db/keyspace_iter.go (new)

  Implement type keyspaceIterator struct that satisfies internal/iterator.UnsafeIterator:

  - Holds:
      - layout *keyspace.Layout
      - trees []tree.Tree
      - start,end []byte
      - part int current partition index
      - cur iterator.UnsafeIterator underlying iterator
      - reverse bool
  - Seek(k):
      - Find correct partition via PartitionForKey(k)
      - Close current underlying iterator (if any)
      - Open a new underlying iterator for that partition with start=k (or end=k for reverse), respecting overall
        [start,end)
      - If k is beyond the overall domain, iterator becomes invalid
  - Next():
      - Advance underlying; if it becomes invalid, advance to next partition (or prev for reverse), opening the next
        underlying iterator at the partition boundary and continuing
  - Domain() returns original (start,end)

  ———

  ## Write Path Changes (Atomic, Partitioned, Parallel Apply)

  ### Batch Write must become Multi-Root Aware

  File: TreeDB/db/batch.go

  Add a sharding-aware write path while keeping a fast path for non-sharded DBs.

  #### Key idea: Disjoint-partition commits shouldn’t conflict

  Today optimistic writes conflict if meta.UserRootPageID changes. With partitions:

  - A writer that touches partition p should only fail if partition p’s root changed since it started (or the
    partition layout epoch changed).
  - Other partitions may change without causing retry.

  #### Implementation Outline

  Add:

  - func (b *Batch) writeOptimisticPartitioned(sync bool) (bool, error)
  - func (b *Batch) writeSerializedPartitioned(sync bool) error

  Common helper (new file TreeDB/db/keyspace_write.go):

  type touchedPartition struct {
      idx      int
      baseRoot uint64
      ops      []batch.Entry // ops for this partition
  }

  func partitionOps(layout *keyspace.Layout, entries []batch.Entry) ([]touchedPartition, error)

  Partitioning logic:

  - Use layout.PartitionForKey(entry.Key)
  - Group ops per partition, preserving sorted order within each group (entries are already sorted globally; stable
    append keeps per-partition sorted).

  Parallel apply (per commit attempt):

  - For each touched partition:
      - Clone zipper (idx.zipper.CloneWithAllocator(tracker))
      - Apply ops to that partition root
  - Concurrency: min(len(touched), runtime.GOMAXPROCS(0)) via a bounded worker pool (no config required)

  Allocator tracking:

  - Use one allocTracker per partition apply (or a shared tracker if simpler); on abandon, free newly allocated
    pages.

  #### Optimistic Partitioned Commit Algorithm (exact)

  1. db.writeMu.RLock()
  2. Load:
      - idx := db.idx.Load()
      - state := db.state.Load()
      - layout := state.Keyspace; if nil, treat as Single(meta.UserRootPageID) for partition routing
  3. Partition ops from b.batch.SortedEntries() -> touched[] with baseRoot from layout.Parts[idx].Root
  4. Apply per partition in parallel -> newRoots map[partIdx]uint64, retired []uint64 (concat), metrics aggregated
     (optional)
  5. db.commitMu.Lock()
  6. Re-load current layout from db.state.Load() (not from meta):
      - If currentLayout.Epoch != layout.Epoch => abandon attempt (free allocs, unlock, return not committed)
      - For each touched partition: if currentLayout.Parts[p].Root != baseRoot => abandon attempt
  7. Create nextLayout := currentLayout.Clone() and update touched roots to newRoots[p]
  8. Persist nextLayout into System Tree:
      - Build a tiny internal batch sysBatch with Set(sysLayoutKey, Encode(nextLayout)) (force inline by using a
        huge inline threshold)
      - newSysRoot, sysRetired, _, err := idx.zipper.Apply(currentSysRoot, sysBatch)
  9. Compute newUserRootForMeta := nextLayout.Parts[0].Root (compat)
  10. Call db.finalizeCommit(newUserRootForMeta, newSysRoot, append(retired, sysRetired...), sync, metrics)
  11. Inside finalizeCommit, update db.state.Store(&DBState{..., RootPageID: newUserRootForMeta, SystemRootPageID:
     newSysRoot, Keyspace: nextLayout, ...})
  12. Unlock commitMu, then writeMu.RUnlock()

  Serialized partitioned commit:

  - Same as above but under db.writeMu.Lock() and no retry loop.

  ### Bootstrapping Layout Metadata

  When sharding is enabled but the system layout key is missing:

  - First successful commit persists a single-partition layout (epoch=0, Parts=[{Start:nil, Root:newRoot}]) into
    system tree.
  - Reads continue to work even before the first persistence (layout inferred from meta root).

  File: TreeDB/db/keyspace_meta.go

  - func (db *DB) loadKeyspaceLayout(...) (*keyspace.Layout, error):
      - If key missing => nil (meaning “legacy single root”)
      - If present but invalid => error on open

  Update open paths:

  - TreeDB/db/db.go:openWithLock and TreeDB/db/open_readonly.go:
      - After recover() (and WAL replay for RW), read system key and set initialState.Keyspace accordingly.

  ———

  ## Split / Merge Operations (Rebuild-Based)

  Per your choice, merge is rebuild, not stitch.

  ### Internal Maintenance API

  File: TreeDB/db/keyspace_rebalance.go (new)

  func (db *DB) SplitPartition(ctx context.Context, partIdx int, splitKey []byte) error
  func (db *DB) MergeAdjacent(ctx context.Context, leftIdx int) error

  Both run under db.writeMu.Lock() to exclude concurrent writers.

  #### SplitPartition (exact steps)

  Inputs:

  - partIdx
  - splitKey (must satisfy Start < splitKey < End)

  Steps:

  1. Lock writeMu
  2. Load idx and current state + layout; require layout != nil && layout.N() > 0
  3. Validate splitKey is inside partition bounds
  4. Build:
      - oldTree := tree.New(idx.pager, valueReader{...}, layout.Parts[partIdx].Root)
      - leftIter := oldTree.Iterator(nil, splitKey) (within partition tree)
      - rightIter := oldTree.Iterator(splitKey, nil)
  5. Collect retired pages: oldTree.CollectPageIDs()
  6. Bulk-build two new roots (can be parallel):
      - leftRoot := bulk.BuildWithOptions(leftIter, alloc, idx.pager, bulkOptsFromDB(db))
      - rightRoot := bulk.BuildWithOptions(rightIter, alloc, idx.pager, bulkOptsFromDB(db))
  7. Construct nextLayout := layout.Clone(), nextLayout.Epoch++, replace partition partIdx with two parts:
      - left part keeps Start=oldStart, Root=leftRoot
      - right part uses Start=splitKeyCopy, Root=rightRoot
  8. Persist layout to system tree via zipper.Apply (as in writes)
  9. finalizeCommit(nextLayout.Parts[0].Root, newSysRoot, retiredAll, true, adaptive.Metrics{})

  #### MergeAdjacent (exact steps)

  Inputs:

  - leftIdx merges leftIdx and leftIdx+1

  Steps:

  1. Lock writeMu
  2. Load current layout; validate indices
  3. Create iterators:
      - leftTree := tree.New(..., layout.Parts[leftIdx].Root)
      - rightTree := tree.New(..., layout.Parts[leftIdx+1].Root)
      - mergedIter := concatIter(leftTree.Iterator(nil,nil), rightTree.Iterator(nil,nil)) (new internal iterator)
  4. Collect retired pages from both trees (CollectPageIDs)
  5. mergedRoot := bulk.BuildWithOptions(mergedIter, alloc, idx.pager, bulkOptsFromDB(db))
  6. nextLayout := layout.Clone(), nextLayout.Epoch++, replace two parts with one:
      - Start = layout.Parts[leftIdx].Start
      - Root = mergedRoot
  7. Persist layout, finalizeCommit as above

  Concat iterator:

  - File: TreeDB/db/concat_iter.go (new)
  - Implements iterator.UnsafeIterator by chaining iterators in order; Seek can be implemented minimally for bulk-
    build usage (bulk builder never calls Seek), but implement it fully anyway for reuse.

  ———

  ## Automatic Scaling (Write Contention Driven)

  ### Minimal Config Surface

  File: TreeDB/db/db.go (type Options struct)

  Add:

  type KeyspaceShardingOptions struct {
      Mode string // "off"(default), "auto"
      MinPartitions int // default 1
      MaxPartitions int // default 16
      CheckInterval time.Duration // default 30s
  }

  Add field:

  KeyspaceSharding KeyspaceShardingOptions

  Defaults:

  - Mode="" treated as "off"
  - MinPartitions<=0 -> 1
  - MaxPartitions<=0 -> 16
  - CheckInterval==0 -> 30s

  ### Contention Metrics Collection

  File: TreeDB/db/keyspace_stats.go (new)

  Maintain rolling counters per partition:

  - attempts (# optimistic commit attempts touching partition)
  - conflicts (# optimistic abandons caused by touched partition root mismatch)
  - samples (reservoir of keys, for split key selection)

  Update points:

  - In optimistic write path:
      - After partitioning ops, increment attempts for touched partitions
      - If abandon due to partition root mismatch, increment conflicts for the partitions that mismatched (or all
        touched, but record mismatched specifically)

  Reservoir sampling policy (per your choice: sample median):

  - Maintain SampleSize = 256 keys per partition
  - Only consider keys from Set/Delete ops
  - Sampling gate: xxhash(key) & 0x0F == 0 (1/16) to cap overhead
  - Store copied keys (immutable)

  ### Autoscaler Worker

  File: TreeDB/db/keyspace_autoscaler.go (new)

  Start/stop:

  - Started in openWithLock if opts.KeyspaceSharding.Mode == "auto" and not read-only
  - Stored on db as a goroutine with stop chan struct{}

  Decision policy (exact, decision-complete):

  Constants (internal, not config):

  - minOpsToConsider = 10_000 (per partition, per window)
  - conflictRateSplit = 0.02 (2% conflicts/attempts over window)
  - cooldown = 10 * time.Minute (per partition for split; global for merges)
  - minDistinctSamples = 64 (must have enough variety to choose split key)
  - mergeIdleWindows = 6 (no meaningful ops for 6 intervals)
  - mergeMaxCombinedAttempts = 2_000 (combined attempts over window)

  Windowing:

  - Use a ring buffer of the last W=4 intervals per partition for attempts/conflicts (so decisions use ~2 minutes at
    30s interval by default).

  Split selection:

  1. For each partition p:
      - attempts_p >= minOpsToConsider
      - conflicts_p/attempts_p >= conflictRateSplit
  2. Pick p with the highest conflicts_p/attempts_p
  3. Ensure layout.N() < MaxPartitions
  4. Ensure cooldown since last split affecting p
  5. Choose splitKey:
      - Sort that partition’s sample keys
      - Take median key
      - Validate Start < splitKey < End
      - If invalid or not enough samples => skip split this cycle

  Merge selection:

  1. Only if layout.N() > MinPartitions
  2. Identify adjacent pairs (i, i+1) where both have been “idle” for mergeIdleWindows:
      - attempts in each window below a tiny threshold (e.g. <100/window)
  3. Among candidates, pick pair with lowest combined attempts
  4. Ensure global cooldown since last merge/split
  5. Execute MergeAdjacent(ctx, i)

  Execution:

  - Worker calls exactly one operation per tick (at most one split or one merge), then sleeps until next tick.
  - If db.vacuumInProgress or other heavy maintenance is active, skip tick (gate).

  ———

  ## System-Wide Maintenance Updates (Correctness)

  Any code that assumes a single user tree must be updated to iterate over all partitions.

  ### ValueLogGC

  File: TreeDB/db/vlog_gc.go

  Replace:

  - userIter := snap.tree.Iterator(nil,nil)

  With:

  - userIter := snap.Iterator(nil,nil) (after Snapshot iterator becomes partition-aware), or explicitly iterate each
    partition iterator.
  - Ensure it still scans system tree as today.

  ### Vacuum / Compact / Full-scan utilities

  Files to update:

  - TreeDB/db/db.go (CompactIndex)
  - TreeDB/db/vacuum_offline.go
  - TreeDB/db/vacuum_online.go
  - TreeDB/db/vlog_rewrite.go (if it rebuilds user tree)

  Strategy:

  - Operate per partition (recommended to preserve isolation):
      - Vacuum/compact each partition tree independently (may run sequentially to keep IO sane)
      - Update layout roots accordingly and commit via system layout update
  - If an operation truly needs “entire keyspace sequential rebuild”, implement via concatenated iterator over
    partitions, but still write back into the same partitioning (not a single root).

  Decision (to avoid new design choices later): implement per-partition versions for:

  - CompactIndex: CompactIndex() becomes CompactIndexPartitions() internally and retains the public name; it
    rewrites each partition B+Tree with bulk.Build and retires old pages.

  ———

  ## Testing Plan (TDD, Decision-Complete)

  Add tests in TreeDB/db (and minimal in TreeDB if needed).

  ### 1) Layout Persistence / Reopen

  File: TreeDB/db/keyspace_reopen_test.go (new)

  - Open DB with KeyspaceSharding.Mode="auto"
  - Write a few keys
  - Close, reopen
  - Assert:
      - AcquireSnapshot().State().Keyspace != nil after first commit
      - Layout validates
      - Reads return expected values

  ### 2) Split + Isolation

  File: TreeDB/db/keyspace_split_test.go (new)

  - Open DB, write keys across a wide range
  - Manually call db.SplitPartition(ctx, 0, splitKey) (choose splitKey from inserted set)
  - Assert:
      - All keys still readable
      - Iteration over full range is sorted and complete
      - For each partition:
          - Iterate that partition only and assert every key is within [Start,End)

  ### 3) Merge + Correctness

  File: TreeDB/db/keyspace_merge_test.go (new)

  - After split, call db.MergeAdjacent(ctx, 0)
  - Assert:
      - All keys readable
      - Layout has 1 fewer partition
      - Full iterator matches baseline

  ### 4) Atomic Batch Across Partitions

  File: TreeDB/db/keyspace_atomic_batch_test.go (new)

  - Ensure there are 2 partitions
  - Create a single Batch containing keys in both partitions
  - Write()
  - Assert commit seq increments once and both keys exist

  ### 5) Iterator Seek Across Partitions

  File: TreeDB/db/keyspace_iter_seek_test.go (new)

  - With 2 partitions, create iterator over full domain
  - Seek() into second partition and iterate forward
  - Assert correct first key and ordered traversal continues into later partitions

  ### 6) Autoscaler Decision Unit Tests

  File: TreeDB/db/keyspace_autoscaler_test.go (new)

  - Test pure decision function:
      - Given attempts/conflicts samples, choose correct split candidate
      - Given idle neighbors, choose merge candidate
  - Keep the actual split/merge execution in integration tests above (avoid flakiness)

  ———

  ## Rollout / Safety

  - Feature default: KeyspaceSharding.Mode="off"
  - Enabling requires explicit opt-in.
  - If layout key exists but cannot be decoded/validated: fail open with a clear error (avoid silent data loss).
  - Cooldowns and “one op per tick” prevent runaway rebuild loops.
  - Stats-only overhead is bounded (sampling 1/16 keys, 256 sample cap).

  ———

  ## Files to Change / Add (Exact)

  Add:

  - TreeDB/internal/keyspace/layout.go
  - TreeDB/internal/keyspace/codec.go
  - TreeDB/db/keyspace_meta.go
  - TreeDB/db/keyspace_router.go
  - TreeDB/db/keyspace_iter.go
  - TreeDB/db/concat_iter.go
  - TreeDB/db/keyspace_write.go
  - TreeDB/db/keyspace_rebalance.go
  - TreeDB/db/keyspace_stats.go
  - TreeDB/db/keyspace_autoscaler.go
  - Tests:
      - TreeDB/db/keyspace_reopen_test.go
      - TreeDB/db/keyspace_split_test.go
      - TreeDB/db/keyspace_merge_test.go
      - TreeDB/db/keyspace_atomic_batch_test.go
      - TreeDB/db/keyspace_iter_seek_test.go
      - TreeDB/db/keyspace_autoscaler_test.go

  Modify:

  - TreeDB/db/db.go (Options, DBState, Snapshot acquisition, open load layout, autoscaler lifecycle hooks)
  - TreeDB/db/api.go (Iterator/ReverseIterator route through partition-aware snapshot)
  - TreeDB/db/batch.go (partitioned write paths; optimistic + serialized)
  - TreeDB/db/vlog_gc.go (scan partition-aware user iter)
  - TreeDB/db/vacuum_offline.go, TreeDB/db/vacuum_online.go, TreeDB/db/vlog_rewrite.go (replace single-root
    assumptions)
  - TreeDB/public.go only if you want to surface stats fields (optional; no config needed beyond Options already re-
    exported)

  ———

  ## Acceptance Benchmarks (What to Measure)

  - Flush/apply time vs partition count (1,2,4,8,16) with large batches (cached mode).
  - Contended write workload (many goroutines writing disjoint key ranges):
      - optimistic conflicts drop as partitions increase
      - throughput rises
  - Iterator overhead for full scans (should remain near-linear, small constant factor per partition boundary).
