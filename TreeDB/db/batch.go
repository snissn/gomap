package db

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/page"
)

// Batch implements the cosmos-db Batch interface.
type Batch struct {
	db    *DB
	batch *batch.Batch
}

const optimisticWriteMaxAttempts = 3

func (db *DB) NewBatch() batch.Interface {
	return db.NewBatchWithSize(0)
}

func (db *DB) NewBatchWithSize(size int) batch.Interface {
	threshold := db.policy.InlineThreshold
	if db.adaptive != nil {
		threshold = db.adaptive.GetThreshold()
	}
	if size < 0 {
		size = 0
	}
	internal := batch.New(db.valueLogManager, threshold)
	internal.Reserve(size)
	return &Batch{
		db:    db,
		batch: internal,
	}
}

func (b *Batch) Set(key, value []byte) error {
	return b.batch.Set(key, value)
}

// SetView records a Put without copying key/value bytes. Callers must treat
// key/value as immutable until the batch is written or closed.
//
// This is intentionally not part of the public batch.Interface; it is a
// best-effort optimization used by higher-level layers (e.g. cached streaming).
func (b *Batch) SetView(key, value []byte) error {
	return b.batch.SetView(key, value)
}

func (b *Batch) Delete(key []byte) error {
	return b.batch.Delete(key)
}

// DeleteView records a Delete without copying the key bytes. Callers must treat
// key as immutable until the batch is written or closed.
func (b *Batch) DeleteView(key []byte) error {
	return b.batch.DeleteView(key)
}

// SetPointer records a pointer without copying the value bytes.
func (b *Batch) SetPointer(key []byte, ptr page.ValuePtr) error {
	return b.batch.SetPointer(key, ptr)
}

// SetPointerView records a pointer without copying the key bytes.
func (b *Batch) SetPointerView(key []byte, ptr page.ValuePtr) error {
	return b.batch.SetPointerView(key, ptr)
}

func (b *Batch) SetOps(ops []batch.Entry) error {
	return b.batch.SetOps(ops)
}

func (b *Batch) Write() error {
	return b.write(false)
}

func (b *Batch) WriteSync() error {
	return b.write(true)
}

func (b *Batch) write(sync bool) error {
	if b == nil || b.db == nil {
		return fmt.Errorf("missing db")
	}
	if b.db.readOnly {
		return ErrReadOnly
	}
	for attempt := 0; attempt < optimisticWriteMaxAttempts; attempt++ {
		committed, err := b.writeOptimistic(sync)
		if err != nil {
			return err
		}
		if committed {
			return nil
		}
	}
	return b.writeSerialized(sync)
}

func (b *Batch) writeOptimistic(sync bool) (bool, error) {
	b.db.writeMu.RLock()
	idx := b.db.idx.Load()
	if idx == nil {
		b.db.writeMu.RUnlock()
		return false, fmt.Errorf("missing index")
	}

	b.db.mu.RLock()
	rootID := b.db.meta.UserRootPageID
	baseSeq := b.db.meta.CommitSeq
	b.db.mu.RUnlock()

	// Track whether Apply was forced to append new pages due to freelist starvation.
	// We use this to trigger a bounded catch-up prune right after Apply returns,
	// so subsequent commits can reuse freed pages instead of growing the file.
	_, appendBefore := idx.allocator.AllocCounters()

	// Opportunistic prune pass to convert eligible retired pages into freelist
	// entries before Apply starts allocating new pages.
	//
	// In cached mode, each backend commit can retire tens of thousands of pages.
	// If we only prune a small fixed amount here, the allocator can still starve
	// mid-Apply and force file growth via pager.Alloc, even though there are
	// plenty of eligible pages in the graveyard. Use the same bounded budgets as
	// the (possibly disabled) background pruner, so large batches can replenish
	// the freelist fast enough.
	if !b.db.preferAppendAlloc && b.db.keepRecent > 0 && b.db.keepRecent <= 1 {
		_, maxPages, maxDuration, _, _, _, _ := b.db.pruner.Stats()
		if maxPages > 0 && maxDuration > 0 {
			// Cached flush commits can retire huge batches; give prune a bit more
			// budget than a single background tick so we don't hit freelist
			// starvation mid-Apply.
			maxPages *= 4
			maxDuration *= 4
			_, _ = b.db.pruneSomeWithCurrentSeq(nil, maxPages, maxDuration, baseSeq+1)
		}
	}

	// If the freelist is empty, proactively prune (bounded) before Apply starts
	// allocating pages. This avoids needless file growth and improves throughput
	// under churn, while still respecting KeepRecent and pinned readers.
	if !b.db.preferAppendAlloc && b.db.keepRecent > 0 && b.db.keepRecent <= 1 && idx.allocator.Head() == 0 {
		_, maxPages, maxDuration, _, _, _, _ := b.db.pruner.Stats()
		// When the freelist is empty, we need to catch up quickly or we'll be
		// forced to grow the file. Use a larger bounded budget than the steady-
		// state pruner tick.
		maxPages *= 8
		maxDuration *= 8
		// Use the *next* commit sequence so pages retired at baseSeq become
		// eligible (subject to KeepRecent) before Apply allocates new pages.
		_, _ = b.db.pruneSomeWithCurrentSeq(nil, maxPages, maxDuration, baseSeq+1)
	}

	// Register this writer as a "reader" of the base state to prevent the
	// pruner from reclaiming pages we are about to read during z.Apply.
	regID := idx.registry.Register(baseSeq)

	// We only need to pin the base sequence while Apply is reading old pages.
	// Unpin as soon as Apply returns so pruning can reclaim pages promptly.
	unregister := func() {
		if regID != 0 {
			idx.registry.Unregister(regID)
			regID = 0
		}
	}
	defer unregister()

	tracker := newAllocTracker(idx.allocator)
	z := idx.zipper.CloneWithAllocator(tracker)
	newRoot, retired, metrics, err := z.Apply(rootID, b.batch)
	if err != nil {
		freeErr := tracker.FreeAll()
		b.db.writeMu.RUnlock()
		if freeErr != nil {
			return false, freeErr
		}
		return false, err
	}

	// Apply is done reading the old tree; allow pruning while we finalize commit.
	unregister()

	// If Apply was forced to append pages (freelist starvation), proactively
	// reclaim eligible retired pages *before* committing the new state. We use
	// baseSeq+1 so pages retired at baseSeq become eligible (KeepRecent) as soon
	// as possible.
	//
	// This is bounded and only runs when we observed append allocations during
	// Apply, so steady-state overwrite workloads do not pay extra cost.
	if !b.db.preferAppendAlloc && b.db.keepRecent > 0 && b.db.keepRecent <= 1 {
		_, appendAfter := idx.allocator.AllocCounters()
		if appendAfter > appendBefore {
			_, maxPages, maxDuration, _, _, _, _ := b.db.pruner.Stats()
			maxPages *= 8
			maxDuration *= 8
			_, _ = b.db.pruneSomeWithCurrentSeq(nil, maxPages, maxDuration, baseSeq+1)
		}
	}

	b.db.commitMu.Lock()
	b.db.mu.RLock()
	currentRoot := b.db.meta.UserRootPageID
	sysRoot := b.db.meta.SystemRootPageID
	b.db.mu.RUnlock()
	if currentRoot != rootID {
		b.db.commitMu.Unlock()
		freeErr := tracker.FreeAll()
		b.db.writeMu.RUnlock()
		if freeErr != nil {
			return false, freeErr
		}
		return false, nil
	}

	err = b.db.finalizeCommit(newRoot, sysRoot, retired, sync, metrics)
	b.db.commitMu.Unlock()
	if err != nil {
		b.db.writeMu.RUnlock()
		return false, err
	}
	if b.db.vacuum.Active() {
		b.db.vacuum.RecordOps(b.batch.Ops())
	}
	b.db.writeMu.RUnlock()
	return true, nil
}

func (b *Batch) writeSerialized(sync bool) error {
	b.db.writeMu.Lock()
	defer b.db.writeMu.Unlock()

	idx := b.db.idx.Load()
	if idx == nil {
		return fmt.Errorf("missing index")
	}

	b.db.mu.RLock()
	rootID := b.db.meta.UserRootPageID
	baseSeq := b.db.meta.CommitSeq
	b.db.mu.RUnlock()

	_, appendBefore := idx.allocator.AllocCounters()

	if !b.db.preferAppendAlloc && b.db.keepRecent > 0 && b.db.keepRecent <= 1 {
		_, maxPages, maxDuration, _, _, _, _ := b.db.pruner.Stats()
		if maxPages > 0 && maxDuration > 0 {
			maxPages *= 4
			maxDuration *= 4
			_, _ = b.db.pruneSomeWithCurrentSeq(nil, maxPages, maxDuration, baseSeq+1)
		}
	}

	if !b.db.preferAppendAlloc && b.db.keepRecent > 0 && b.db.keepRecent <= 1 && idx.allocator.Head() == 0 {
		_, maxPages, maxDuration, _, _, _, _ := b.db.pruner.Stats()
		maxPages *= 8
		maxDuration *= 8
		_, _ = b.db.pruneSomeWithCurrentSeq(nil, maxPages, maxDuration, baseSeq+1)
	}

	regID := idx.registry.Register(baseSeq)
	unregister := func() {
		if regID != 0 {
			idx.registry.Unregister(regID)
			regID = 0
		}
	}
	defer unregister()

	newRoot, retired, metrics, err := idx.zipper.Apply(rootID, b.batch)
	if err != nil {
		return err
	}

	// Apply is done reading the old tree; allow pruning while we finalize commit.
	unregister()

	if !b.db.preferAppendAlloc && b.db.keepRecent > 0 && b.db.keepRecent <= 1 {
		_, appendAfter := idx.allocator.AllocCounters()
		if appendAfter > appendBefore {
			_, maxPages, maxDuration, _, _, _, _ := b.db.pruner.Stats()
			maxPages *= 8
			maxDuration *= 8
			_, _ = b.db.pruneSomeWithCurrentSeq(nil, maxPages, maxDuration, baseSeq+1)
		}
	}

	b.db.mu.Lock()
	if b.db.meta.UserRootPageID != rootID {
		// This should not happen if writeMu is held and we are the only writer.
		b.db.mu.Unlock()
		return fmt.Errorf("concurrent modification detected during batch write")
	}
	sysRoot := b.db.meta.SystemRootPageID
	b.db.mu.Unlock()

	if err := b.db.finalizeCommit(newRoot, sysRoot, retired, sync, metrics); err != nil {
		return err
	}
	if b.db.vacuum.Active() {
		b.db.vacuum.RecordOps(b.batch.Ops())
	}
	return nil
}

func (b *Batch) Close() error {
	if b.batch != nil {
		err := b.batch.Close()
		b.batch = nil
		return err
	}
	b.batch = nil
	return nil
}

// Reset clears the batch for reuse.
func (b *Batch) Reset() {
	if b == nil || b.batch == nil {
		return
	}
	b.batch.Reset()
}

func (b *Batch) Replay(fn func(batch.Entry) error) error {
	if b == nil || b.batch == nil {
		return nil
	}
	entries := b.batch.SortedEntries()
	for _, entry := range entries {
		if entry.IsPtr && entry.Value == nil {
			ptr := entry.ValuePtr
			if !page.IsValueLogFileID(ptr.FileID) {
				return fmt.Errorf("expected value-log pointer, got file=%d", ptr.FileID)
			}
			if b.db == nil || b.db.valueLogManager == nil {
				return fmt.Errorf("missing value log manager")
			}
			val, err := b.db.valueLogManager.Read(ptr)
			if err != nil {
				return err
			}
			entry.Value = val
		}
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) GetByteSize() (int, error) {
	return b.batch.ByteSize(), nil
}
