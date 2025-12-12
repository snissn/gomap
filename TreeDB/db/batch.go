package db

import (
	"github.com/snissn/gomap-gemini/TreeDB/batch"
)

// Batch implements the cosmos-db Batch interface.
type Batch struct {
	db    *DB
	batch *batch.Batch
}

func (db *DB) NewBatch() *Batch {
	return &Batch{
		db:    db,
		batch: batch.New(db.slabManager),
	}
}

func (b *Batch) Set(key, value []byte) error {
	return b.batch.Set(key, value)
}

func (b *Batch) Delete(key []byte) error {
	return b.batch.Delete(key)
}

func (b *Batch) Write() error {
	// 1. Apply via Zipper
	// We need a write lock on the DB for the Commit phase?
	// The Zipper itself (Phase 2) reads old pages and allocates new ones.
	// It relies on COW.
	// But concurrent writers?
	// Spec says: "Single-Writer / Multi-Reader (SWMR)".
	// So we need a global write lock.
	
	b.db.mu.Lock()
	defer b.db.mu.Unlock()
	
	// Get current root
	// We need latest committed root.
	rootID := b.db.meta.UserRootPageID
	
	// Zipper Apply
	// Note: Zipper reads from pager. Pager is thread-safe.
	// But we are modifying Pager (Alloc). Pager.Alloc is locked.
	newRoot, err := b.db.zipper.Apply(rootID, b.batch)
	if err != nil {
		return err
	}
	
	// Commit
	// We are already holding Lock?
	// DB.Commit also takes Lock.
	// We should split DB.Commit or have `commitLocked`.
	// Or `Write` takes lock, then calls `commitLocked`.
	
	return b.db.commitLocked(newRoot, false)
}

func (b *Batch) WriteSync() error {
	b.db.mu.Lock()
	defer b.db.mu.Unlock()
	
	rootID := b.db.meta.UserRootPageID
	newRoot, err := b.db.zipper.Apply(rootID, b.batch)
	if err != nil {
		return err
	}
	
	return b.db.commitLocked(newRoot, true)
}

func (b *Batch) Close() error {
	b.batch = nil
	return nil
}

func (b *Batch) GetByteSize() (int, error) {
	// batch package didn't expose byteSize?
	// I added `byteSize` field but maybe not getter?
	// Let's check batch.go.
	return 0, nil // Placeholder
}
