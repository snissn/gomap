package db

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/batch"
)

// Batch implements the cosmos-db Batch interface.
type Batch struct {
	db    *DB
	batch *batch.Batch
}

func (db *DB) NewBatch() batch.Interface {
	return db.NewBatchWithSize(0)
}

func (db *DB) NewBatchWithSize(size int) batch.Interface {
	threshold := db.policy.InlineThreshold
	if db.adaptive != nil {
		threshold = db.adaptive.GetThreshold()
	}
	return &Batch{
		db:    db,
		batch: batch.New(db.slabManager, threshold),
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

func (b *Batch) SetOps(ops []batch.Entry) error {
	return b.batch.SetOps(ops)
}

func (b *Batch) Write() error {
	// Serialize writers
	b.db.writeMu.Lock()
	defer b.db.writeMu.Unlock()

	if b.db.vacuum.Active() {
		b.db.vacuum.RecordOps(b.batch.Ops())
	}

	// Get current root (Read Lock)
	b.db.mu.RLock()
	rootID := b.db.meta.UserRootPageID
	b.db.mu.RUnlock()

	// Zipper Apply (No DB Lock, runs concurrently with Readers)
	newRoot, retired, metrics, err := b.db.zipper.Apply(rootID, b.batch)
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

	// Commit (Write Lock)
	b.db.mu.Lock()
	if b.db.meta.UserRootPageID != rootID {
		// This should not happen if writeMu is held and we are the only writer
		b.db.mu.Unlock()
		return fmt.Errorf("concurrent modification detected during batch write")
	}
	sysRoot := b.db.meta.SystemRootPageID
	b.db.mu.Unlock()

	// Commit (System Root is unchanged for now)
	return b.db.finalizeCommit(newRoot, sysRoot, retired, false, metrics)
}

func (b *Batch) WriteSync() error {
	b.db.writeMu.Lock()
	defer b.db.writeMu.Unlock()

	if b.db.vacuum.Active() {
		b.db.vacuum.RecordOps(b.batch.Ops())
	}

	b.db.mu.RLock()
	rootID := b.db.meta.UserRootPageID
	b.db.mu.RUnlock()

	newRoot, retired, metrics, err := b.db.zipper.Apply(rootID, b.batch)
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
		b.db.mu.Unlock()
		return fmt.Errorf("concurrent modification detected during batch write")
	}
	sysRoot := b.db.meta.SystemRootPageID
	b.db.mu.Unlock()

	return b.db.finalizeCommit(newRoot, sysRoot, retired, true, metrics)
}

func (b *Batch) Close() error {
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
	return b.batch.Replay(fn)
}

func (b *Batch) GetByteSize() (int, error) {
	return b.batch.ByteSize(), nil
}
