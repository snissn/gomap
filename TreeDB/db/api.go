package db

import (
	"fmt"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// --- Public API ---

// Get returns the value for a key.
//
// Semantics: Returns a safe copy of the value.
func (db *DB) Get(key []byte) ([]byte, error) {
	snap := db.AcquireSnapshot()
	defer snap.Close()
	val, err := snap.Get(key)
	if err == tree.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

// GetUnsafe returns the value for a key.
//
// Semantics: Returns a safe copy of the value. For zero-copy views tied to a
// snapshot lifetime, use Snapshot.GetUnsafe.
func (db *DB) GetUnsafe(key []byte) ([]byte, error) {
	return db.Get(key)
}

// GetAppend appends the value for the key to dst and returns the new slice.
// If the key is not found, it returns dst and ErrKeyNotFound.
func (db *DB) GetAppend(key, dst []byte) ([]byte, error) {
	snap := db.AcquireSnapshot()
	defer snap.Close()
	val, err := snap.GetUnsafe(key)
	if err == tree.ErrKeyNotFound {
		return dst, err
	}
	if err != nil {
		return dst, err
	}
	return append(dst, val...), nil
}

// Has checks if a key exists.
func (db *DB) Has(key []byte) (bool, error) {
	snap := db.AcquireSnapshot()
	defer snap.Close()
	return snap.Has(key)
}

// Set sets the value for a key.
func (db *DB) Set(key, value []byte) error {
	b := db.NewBatch().(*Batch)
	if err := b.batch.Set(key, value); err != nil {
		return err
	}
	return b.Write() // Using Write for better throughput (async)
}

// SetSync sets the value and syncs to disk.
func (db *DB) SetSync(key, value []byte) error {
	b := db.NewBatch().(*Batch)
	if err := b.batch.Set(key, value); err != nil {
		return err
	}
	return b.WriteSync()
}

// Delete removes a key.
func (db *DB) Delete(key []byte) error {
	b := db.NewBatch().(*Batch)
	if err := b.batch.Delete(key); err != nil {
		return err
	}
	return b.Write() // Using Write for better throughput (async)
}

// DeleteSync removes a key and syncs.
func (db *DB) DeleteSync(key []byte) error {
	b := db.NewBatch().(*Batch)
	if err := b.batch.Delete(key); err != nil {
		return err
	}
	return b.WriteSync()
}

// DBIterator wraps tree.Iterator and holds a Snapshot.
type DBIterator struct {
	snap *Snapshot
	iter iterator.UnsafeIterator
	err  error
}

func (it *DBIterator) DebugStats() (queueLen int, sourcesUsed int) {
	return 0, 1
}

func (it *DBIterator) Next() {
	if !it.Valid() {
		return
	}
	it.iter.Next()
}

func (it *DBIterator) Valid() bool {
	return it.iter.Valid() && it.err == nil
}

func (it *DBIterator) Key() []byte {
	return it.iter.Key()
}

func (it *DBIterator) Value() []byte {
	val := it.UnsafeValue()
	if val == nil {
		return nil
	}
	// Copy
	dst := make([]byte, len(val))
	copy(dst, val)
	return dst
}

func (it *DBIterator) KeyCopy(dst []byte) []byte {
	return it.iter.KeyCopy(dst)
}

func (it *DBIterator) ValueCopy(dst []byte) []byte {
	val := it.UnsafeValue()
	return append(dst, val...)
}

func (it *DBIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.iter.Error()
}

func (it *DBIterator) Close() error {
	err := it.iter.Close()
	if e := it.snap.Close(); e != nil {
		if err == nil {
			err = e
		}
	}
	return err
}

// UnsafeIterator methods
func (it *DBIterator) Seek(key []byte) {
	it.iter.Seek(key)
}

func (it *DBIterator) UnsafeKey() []byte {
	return it.iter.UnsafeKey()
}

func (it *DBIterator) UnsafeValue() []byte {
	if it.err != nil {
		return nil
	}
	val := it.iter.UnsafeValue()
	_, _, flags := it.iter.UnsafeEntry()
	if flags&node.FlagValueID != 0 {
		resolved, err := it.snap.resolveValueID(val, true)
		if err != nil {
			it.err = err
			return nil
		}
		return resolved
	}
	return val
}

func (it *DBIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	return it.iter.UnsafeEntry()
}

func (it *DBIterator) IsDeleted() bool {
	return it.iter.IsDeleted()
}

func (it *DBIterator) Domain() (start, end []byte) {
	return it.iter.Domain()
}

// Iterator returns an iterator.
func (db *DB) Iterator(start, end []byte) (iterator.UnsafeIterator, error) {
	snap := db.AcquireSnapshot()
	it := snap.tree.Iterator(start, end)
	return &DBIterator{snap: snap, iter: it}, nil
}

// ReverseIterator returns a reverse iterator.
func (db *DB) ReverseIterator(start, end []byte) (iterator.UnsafeIterator, error) {
	snap := db.AcquireSnapshot()
	it := snap.tree.ReverseIterator(start, end)
	return &DBIterator{snap: snap, iter: it}, nil
}

// Stats returns database statistics.
func (db *DB) Stats() map[string]string {
	stats := make(map[string]string)
	stats["cosmos.db.type"] = "treedb"

	snap := db.AcquireSnapshot()
	if snap == nil || snap.idx == nil || snap.state == nil {
		if snap != nil {
			_ = snap.Close()
		}
		return stats
	}
	defer func() { _ = snap.Close() }()

	state := snap.state
	idx := snap.idx

	stats["treedb.commit_seq"] = fmt.Sprintf("%d", state.CommitSeq)
	stats["treedb.root_page"] = fmt.Sprintf("%d", state.RootPageID)
	stats["treedb.system_root_page"] = fmt.Sprintf("%d", state.SystemRootPageID)

	stats["treedb.pages.total"] = fmt.Sprintf("%d", idx.pager.PageCount())

	stats["treedb.slabs.active_id"] = fmt.Sprintf("%d", db.slabManager.ActiveSlabID())
	stats["treedb.slabs.zombies"] = fmt.Sprintf("%d", db.slabManager.ZombieCount())
	slabRemaps, slabDeadMappings := db.slabManager.RemapStats()
	stats["treedb.slabs.mmap_remaps"] = fmt.Sprintf("%d", slabRemaps)
	stats["treedb.slabs.mmap_dead_mappings"] = fmt.Sprintf("%d", slabDeadMappings)
	if db.valueLogManager != nil {
		vlogRemaps, vlogDeadMappings := db.valueLogManager.RemapStats()
		stats["treedb.vlog.mmap_remaps"] = fmt.Sprintf("%d", vlogRemaps)
		stats["treedb.vlog.mmap_dead_mappings"] = fmt.Sprintf("%d", vlogDeadMappings)
	}

	pruneStatsInto(stats, &db.pruner)

	// Best-effort slab fragmentation stats (derived from persisted System tree).
	var total, dead uint64
	sysTree := tree.New(idx.pager, valueReader{slabs: state.SlabSet, vlogs: state.ValueLogSet}, state.SystemRootPageID)
	it := sysTree.Iterator(slabStatsKeyPrefix, slabStatsPrefixEnd())
	for it.Valid() {
		_, vPtr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer != 0 {
			// System keys should be inline; ignore if not.
			_ = vPtr
		} else {
			val := it.UnsafeValue()
			if d, t, err := decodeSlabStatsValue(val); err == nil {
				dead += d
				total += t
			}
		}
		it.Next()
	}
	_ = it.Close()

	stats["treedb.slabs.total_bytes"] = fmt.Sprintf("%d", total)
	stats["treedb.slabs.dead_bytes"] = fmt.Sprintf("%d", dead)
	if total > 0 {
		stats["treedb.slabs.dead_ratio_ppm"] = fmt.Sprintf("%d", (dead*1_000_000)/total)
	}

	if gcStats := db.lastGCStats.Load(); gcStats != nil {
		stats["treedb.gc.last_run_time"] = gcStats.LastRunTime.Format(time.RFC3339)
		stats["treedb.gc.last_run_duration"] = gcStats.LastRunDuration.String()
		stats["treedb.gc.reclaimed_bytes"] = fmt.Sprintf("%d", gcStats.ReclaimedBytes)
		if gcStats.LastError != nil {
			stats["treedb.gc.last_error"] = gcStats.LastError.Error()
		}
	}

	return stats
}

// Print debugs the tree (simple dump).
func (db *DB) Print() error {
	// Not implemented fully
	return nil
}
