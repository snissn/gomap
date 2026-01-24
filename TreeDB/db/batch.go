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
	internal := batch.New(db.slabManager, threshold)
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
	// Register this writer as a "reader" of the base state to prevent the
	// pruner from reclaiming pages we are about to read during z.Apply.
	regID := idx.registry.Register(baseSeq)
	b.db.mu.RUnlock()

	defer idx.registry.Unregister(regID)

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
	metrics.SlabWriteBytes += b.batch.SlabWriteBytes()
	if byFile := b.batch.SlabWriteBytesByFile(); len(byFile) > 0 {
		if metrics.SlabWriteBytesByFile == nil {
			metrics.SlabWriteBytesByFile = byFile
		} else {
			for id, n := range byFile {
				metrics.SlabWriteBytesByFile[id] += n
			}
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
	regID := idx.registry.Register(baseSeq)
	b.db.mu.RUnlock()

	defer idx.registry.Unregister(regID)

	newRoot, retired, metrics, err := idx.zipper.Apply(rootID, b.batch)
	if err != nil {
		return err
	}
	metrics.SlabWriteBytes += b.batch.SlabWriteBytes()
	if byFile := b.batch.SlabWriteBytesByFile(); len(byFile) > 0 {
		if metrics.SlabWriteBytesByFile == nil {
			metrics.SlabWriteBytesByFile = byFile
		} else {
			for id, n := range byFile {
				metrics.SlabWriteBytesByFile[id] += n
			}
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
			var (
				val []byte
				err error
			)
			if page.IsValueLogFileID(ptr.FileID) {
				if b.db == nil || b.db.valueLogManager == nil {
					return fmt.Errorf("missing value log manager")
				}
				val, err = b.db.valueLogManager.Read(ptr)
			} else {
				if b.db == nil || b.db.slabManager == nil {
					return fmt.Errorf("missing slab manager")
				}
				val, err = b.db.slabManager.Read(ptr)
			}
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
