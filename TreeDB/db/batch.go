package db

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/page"
)

// Batch implements the cosmos-db Batch interface.
type Batch struct {
	db          *DB
	batch       *batch.Batch
	targetRoot  batchRootTarget
	usedAutoPtr bool
}

const optimisticWriteMaxAttempts = 3

type batchRootTarget uint8

const (
	batchRootUser batchRootTarget = iota
	batchRootSystem
)

const errConcurrentBatchWrite = "concurrent modification detected during batch write"

var errConcurrentBatchWriteInternal = fmt.Errorf("treedb: " + errConcurrentBatchWrite)

func (db *DB) NewBatch() batch.Interface {
	return db.NewBatchWithSize(0)
}

func (db *DB) NewBatchWithSize(size int) batch.Interface {
	internal := db.newInternalBatch(size)
	return &Batch{
		db:         db,
		batch:      internal,
		targetRoot: batchRootUser,
	}
}

func (db *DB) newInternalBatch(size int) *batch.Batch {
	threshold := db.InlineThreshold()
	if size < 0 {
		size = 0
	}
	internal := batch.New(db.valueLogManager, threshold)
	if db != nil && db.inlineThresholdResolver != nil {
		internal.SetInlineThresholdResolver(db.inlineThresholdResolver)
	}
	internal.Reserve(size)
	return internal
}

// NewSystemBatch targets writes into the system catalog root.
func (db *DB) NewSystemBatch() batch.Interface {
	if db == nil {
		return nil
	}
	internal := db.NewBatch()
	internalBatch, ok := internal.(*Batch)
	if !ok {
		return internal
	}
	internalBatch.targetRoot = batchRootSystem
	return internalBatch
}

func (b *Batch) targetRoots(db *DB) (targetRootID uint64, peerRootID uint64, err error) {
	if b == nil || db == nil {
		return 0, 0, fmt.Errorf("missing db")
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	switch b.targetRoot {
	case batchRootSystem:
		return db.meta.SystemRootPageID, db.meta.UserRootPageID, nil
	case batchRootUser:
		fallthrough
	default:
		return db.meta.UserRootPageID, db.meta.SystemRootPageID, nil
	}
}

func (b *Batch) Set(key, value []byte) error {
	return b.batch.Set(key, value)
}

// SetAuto records a Put and transparently falls back to value-log pointer
// storage when inline thresholds are exceeded.
func (b *Batch) SetAuto(key, value []byte) error {
	if b == nil || b.db == nil {
		return fmt.Errorf("missing db")
	}
	usedPointer, err := b.db.batchSetWithPointerFallback(b, key, value, false)
	if usedPointer {
		b.usedAutoPtr = true
	}
	return err
}

// SetAutoView records a Put without copying key/value bytes and transparently
// falls back to value-log pointer storage when inline thresholds are exceeded.
// Callers must treat key/value as immutable until the batch is written or
// closed.
func (b *Batch) SetAutoView(key, value []byte) error {
	if b == nil || b.db == nil {
		return fmt.Errorf("missing db")
	}
	usedPointer, err := b.db.batchSetWithPointerFallback(b, key, value, true)
	if usedPointer {
		b.usedAutoPtr = true
	}
	return err
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

// Reserve forwards best-effort preallocation hints to the internal batch.
func (b *Batch) Reserve(n int) {
	if b == nil || b.batch == nil || n <= 0 {
		return
	}
	b.batch.Reserve(n)
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
	if b.usedAutoPtr {
		if err := b.db.flushInlineAppender(sync); err != nil {
			return false, err
		}
	}
	touchedValueLogSegments := b.batch.TouchedValueLogSegments()

	b.db.writeMu.RLock()
	idx := b.db.idx.Load()
	if idx == nil {
		b.db.writeMu.RUnlock()
		return false, fmt.Errorf("missing index")
	}

	targetRootID, peerRootID, err := b.targetRoots(b.db)
	if err != nil {
		b.db.writeMu.RUnlock()
		return false, err
	}
	b.db.mu.RLock()
	baseSeq := b.db.meta.CommitSeq
	// Register this writer as a "reader" of the base state to prevent the
	// pruner from reclaiming pages we are about to read during z.Apply.
	regID := idx.registry.Register(baseSeq)
	b.db.mu.RUnlock()

	defer idx.registry.Unregister(regID)

	tracker := newAllocTracker(idx.allocator)
	z := idx.zipper.CloneWithAllocator(tracker)
	newRoot, retired, metrics, err := z.Apply(targetRootID, b.batch)
	if err != nil {
		freeErr := tracker.FreeAll()
		b.db.writeMu.RUnlock()
		if freeErr != nil {
			return false, freeErr
		}
		return false, err
	}
	entries := b.batch.SortedEntries()
	vlogRefDelta, err := b.db.buildValueLogRefDelta(idx.pager, targetRootID, baseSeq, entries)
	if err != nil {
		freeErr := tracker.FreeAll()
		b.db.writeMu.RUnlock()
		if freeErr != nil {
			return false, freeErr
		}
		return false, err
	}
	b.db.commitMu.Lock()
	b.db.mu.RLock()
	currentTargetRoot, currentPeerRoot := b.db.meta.UserRootPageID, b.db.meta.SystemRootPageID
	if b.targetRoot == batchRootSystem {
		currentTargetRoot, currentPeerRoot = currentPeerRoot, currentTargetRoot
	}
	b.db.mu.RUnlock()
	if currentTargetRoot != targetRootID || currentPeerRoot != peerRootID {
		b.db.commitMu.Unlock()
		freeErr := tracker.FreeAll()
		b.db.writeMu.RUnlock()
		if freeErr != nil {
			return false, freeErr
		}
		return false, nil
	}
	var userRootID, systemRootID uint64
	if b.targetRoot == batchRootSystem {
		userRootID = currentTargetRoot
		systemRootID = newRoot
	} else {
		userRootID = newRoot
		systemRootID = currentPeerRoot
	}

	post, err := b.db.finalizeCommitLocked(userRootID, systemRootID, retired, sync, metrics, touchedValueLogSegments, b.db.indexOuterLeavesInValueLog, vlogRefDelta)
	b.db.commitMu.Unlock()
	if err != nil {
		b.db.writeMu.RUnlock()
		return false, err
	}
	b.db.finalizeCommitPostWork(post)
	if b.db.vacuum.Active() {
		b.db.vacuum.RecordOps(b.batch.Ops())
	}
	b.db.writeMu.RUnlock()
	return true, nil
}

func (b *Batch) writeSerialized(sync bool) error {
	if b.usedAutoPtr {
		if err := b.db.flushInlineAppender(sync); err != nil {
			return err
		}
	}
	touchedValueLogSegments := b.batch.TouchedValueLogSegments()

	b.db.writeMu.Lock()
	defer b.db.writeMu.Unlock()

	idx := b.db.idx.Load()
	if idx == nil {
		return fmt.Errorf("missing index")
	}

	b.db.mu.RLock()
	targetRootID, peerRootID, err := b.targetRoots(b.db)
	if err != nil {
		b.db.writeMu.Unlock()
		return err
	}
	baseSeq := b.db.meta.CommitSeq
	regID := idx.registry.Register(baseSeq)
	b.db.mu.RUnlock()

	defer idx.registry.Unregister(regID)

	newRoot, retired, metrics, err := idx.zipper.Apply(targetRootID, b.batch)
	if err != nil {
		return err
	}
	entries := b.batch.SortedEntries()
	vlogRefDelta, err := b.db.buildValueLogRefDelta(idx.pager, targetRootID, baseSeq, entries)
	if err != nil {
		return err
	}

	b.db.mu.Lock()
	currentTargetRoot := b.db.meta.UserRootPageID
	currentPeerRoot := b.db.meta.SystemRootPageID
	if b.targetRoot == batchRootSystem {
		currentTargetRoot, currentPeerRoot = currentPeerRoot, currentTargetRoot
	}
	if currentTargetRoot != targetRootID || currentPeerRoot != peerRootID {
		// This should not happen if writeMu is held and we are the only writer.
		b.db.mu.Unlock()
		return errConcurrentBatchWriteInternal
	}
	var userRootID, systemRootID uint64
	if b.targetRoot == batchRootSystem {
		userRootID = currentPeerRoot
		systemRootID = newRoot
	} else {
		userRootID = newRoot
		systemRootID = currentPeerRoot
	}
	b.db.mu.Unlock()

	if err := b.db.finalizeCommit(userRootID, systemRootID, retired, sync, metrics, touchedValueLogSegments, b.db.indexOuterLeavesInValueLog, vlogRefDelta); err != nil {
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
		b.usedAutoPtr = false
		return err
	}
	b.batch = nil
	b.usedAutoPtr = false
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
