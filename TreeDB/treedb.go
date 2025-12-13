package treedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"treedb/caching"
	"treedb/internal/adaptive"
	"treedb/internal/iterator" // Import for iterator.UnsafeIterator
	"treedb/internal/mvcc"
	"treedb/internal/page"
	"treedb/internal/pager"
	"treedb/internal/slab"
)

// Options configures a TreeDB instance.
type Options struct {
	Dir             string
	ChunkSize       int64
	InlineThreshold int
	KeepRecent      uint64
	AdaptiveEnabled bool
	EnableCaching   bool
	FlushThreshold  int64
}

// OpenCached opens a TreeDB with write-back caching enabled.
func OpenCached(opts Options) (*caching.DB, error) {
	db, err := Open(opts)
	if err != nil {
		return nil, err
	}
	return caching.Open(opts.Dir, db, opts.FlushThreshold)
}

// DB is the public database handle.
// Phase 7 wires batch/commit and single-op helpers.
type DB struct {
	opts Options

	pager  *pager.Pager
	slabs  *slab.SlabManager
	state  *mvcc.StateHolder
	grave  *mvcc.Graveyard
	pruner *mvcc.Pruner

	adaptive *adaptive.Controller

	writerMu sync.Mutex
	closed   atomic.Bool

	// test hooks (nil in production)
	hooks *dbHooks
}

type dbHooks struct {
	panicAfterOps     int
	opApplied         atomic.Int32
	slabSynced        func()
	indexSynced       func()
	thresholdObserved func(th int)
}

// Open opens or creates a TreeDB instance at opts.Dir.
func Open(opts Options) (*DB, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("treedb: Dir required")
	}
	if opts.InlineThreshold <= 0 {
		opts.InlineThreshold = page.InlineThresholdDefault
	}
	if opts.InlineThreshold < page.InlineHardMin {
		opts.InlineThreshold = page.InlineHardMin
	}
	if opts.InlineThreshold > page.InlineHardMax {
		opts.InlineThreshold = page.InlineHardMax
	}

	p, err := pager.Open(opts.Dir, opts.ChunkSize)
	if err != nil {
		return nil, err
	}
	meta := p.ReadActiveMeta()

	smgr, slabSet, err := slab.Load(opts.Dir, meta.ActiveSlabID, meta.ActiveSlabTail)
	if err != nil {
		_ = p.Close()
		return nil, err
	}

	holder := mvcc.NewStateHolder(&mvcc.DBState{
		CommitSeq:        meta.CommitSeq,
		UserRootPageID:   meta.UserRootPageID,
		SystemRootPageID: meta.SystemRootPageID,
		SlabSet:          slabSet,
	})

	grave := mvcc.NewGraveyard()
	pruner := mvcc.NewPruner(p, grave, holder.Registry(), opts.KeepRecent)

	var ctrl *adaptive.Controller
	if opts.AdaptiveEnabled {
		cfg := adaptive.DefaultConfig()
		cfg.Enabled = true
		ctrl = adaptive.New(cfg, opts.InlineThreshold)
	}

	db := &DB{
		opts:     opts,
		pager:    p,
		slabs:    smgr,
		state:    holder,
		grave:    grave,
		pruner:   pruner,
		adaptive: ctrl,
	}
	return db, nil
}

// Close closes the database and flushes pending state.
func (db *DB) Close() error {
	if db == nil {
		return nil
	}
	if db.closed.Swap(true) {
		return nil
	}
	var first error
	if db.slabs != nil {
		if err := db.slabs.Close(); err != nil && first == nil {
			first = err
		}
	}
	if db.pager != nil {
		if err := db.pager.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Get fetches the value for key or nil if absent.
func (db *DB) Get(key []byte) ([]byte, error) {
        if key == nil || len(key) == 0 {
                return nil, ErrKeyEmpty
        }
        snap, err := db.state.AcquireSnapshot()
        	if err != nil {
        		return nil, err
        	}
        	defer snap.Close()
        
        	it := newIterator(db, snap, key, nil, false) // Seek to key
	it.Seek(key)
	if it.err != nil {
            return nil, it.err
        }
        if !it.Valid() || bytes.Compare(it.UnsafeKey(), key) != 0 {
            return nil, nil // Key not found or exhausted
        }

        if it.IsDeleted() {
            return nil, nil // Tombstone
        }
        return it.UnsafeValue(), nil
}

// Has reports whether key exists.
func (db *DB) Has(key []byte) (bool, error) {
        v, err := db.Get(key)
        if err != nil {
                return false, err
        }
        return v != nil, nil
}

// Set sets key/value without durability guarantee.
func (db *DB) Set(key, value []byte) error {
        b := db.NewBatch()
        if err := b.Set(key, value); err != nil {
                return err
        }
        return b.Write()
}

// SetSync sets key/value and flushes durability boundary.
func (db *DB) SetSync(key, value []byte) error {
        b := db.NewBatch()
        if err := b.Set(key, value); err != nil {
                return err
        }
        return b.WriteSync()
}

// Delete removes key without durability guarantee.
func (db *DB) Delete(key []byte) error {
        b := db.NewBatch()
        if err := b.Delete(key); err != nil {
                return err
        }
        return b.Write()
}

// DeleteSync removes key and flushes durability boundary.
func (db *DB) DeleteSync(key []byte) error {
        b := db.NewBatch()
        if err := b.Delete(key); err != nil {
                return err
        }
        return b.WriteSync()
}

// NewBatch returns an empty batch.
func (db *DB) NewBatch() caching.BatchInterface { return db.NewBatchWithSize(0) }

// NewBatchWithSize returns a batch with a size hint.
func (db *DB) NewBatchWithSize(size int) caching.BatchInterface {
        return newBatch(db, size)
}

func (db *DB) Iterator(start, end []byte) (iterator.UnsafeIterator, error) {
        if db == nil {
                return nil, fmt.Errorf("treedb: nil db")
        }
        if start != nil && len(start) == 0 {
                return nil, ErrKeyEmpty
        }
        if end != nil && len(end) == 0 {
                return nil, ErrKeyEmpty
        }
        if start != nil && end != nil && bytes.Compare(start, end) >= 0 {
                snap, err := db.state.AcquireSnapshot()
                if err != nil {
                        return nil, err
                }
                it := newIterator(db, snap, start, end, false)
                it.valid.Store(false)
                return it, nil
        }
        snap, err := db.state.AcquireSnapshot()
        if err != nil {
                return nil, err
        }
        	it := newIterator(db, snap, start, end, false)
        	it.Seek(start) // Initial seek
        	return it, nil
        }
        
        func (db *DB) ReverseIterator(start, end []byte) (iterator.UnsafeIterator, error) {
        	if db == nil {
        		return nil, fmt.Errorf("treedb: nil db")
        	}
        	if start != nil && len(start) == 0 {
        		return nil, ErrKeyEmpty
        	}
        	if end != nil && len(end) == 0 {
        		return nil, ErrKeyEmpty
        	}
        	// It's the same error logic, but newIterator sets it.
	snap, err := db.state.AcquireSnapshot()
        	if err != nil {
        		return nil, err
        	}
        	it := newIterator(db, snap, start, end, true)
        	it.Seek(end) // Initial seek for ReverseIterator
        	return it, nil
        }// Print is a debug helper that dumps all user keys and values.
// It is best-effort and intended only for development.
func (db *DB) Print() error {
	if db == nil {
		return fmt.Errorf("treedb: nil db")
	}
	it, err := db.Iterator(nil, nil)
	if err != nil {
		return err
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		fmt.Printf("%x = %x\n", it.Key(), it.Value())
	}
	return it.Error()
}

// Stats returns runtime statistics and mandatory TreeDB keys.
func (db *DB) Stats() map[string]string {
	out := map[string]string{
		"cosmos.db.type": "treedb",
	}
	if db == nil {
		return out
	}

	var meta pager.Meta
	if db.pager != nil {
		meta = db.pager.ReadActiveMeta()
		out["treedb.pages.total"] = fmt.Sprintf("%d", meta.TotalPages)
	}

	st := db.state.Load()
	seq := uint64(0)
	if st != nil {
		seq = st.CommitSeq
	} else {
		seq = meta.CommitSeq
	}
	out["treedb.commit_seq"] = fmt.Sprintf("%d", seq)
	// Preserve legacy keys for older tests/tools.
	out["commit_seq"] = out["treedb.commit_seq"]
	out["total_pages"] = out["treedb.pages.total"]

	if db.slabs != nil {
		out["treedb.slabs.active_id"] = fmt.Sprintf("%d", db.slabs.ActiveID())
		zombies := 0
		set := db.slabs.SlabSet()
		if set != nil {
			for _, id := range set.IDs() {
				f, ok := set.Get(id)
				if !ok || f == nil {
					continue
				}
				if f.IsZombie.Load() {
					zombies++
				}
			}
		}
		out["treedb.slabs.zombies"] = fmt.Sprintf("%d", zombies)
	} else {
		out["treedb.slabs.active_id"] = "0"
		out["treedb.slabs.zombies"] = "0"
	}

	if db.adaptive != nil && db.adaptive.Enabled() {
		for k, v := range db.adaptive.StatsMap() {
			out[k] = v
		}
	}
	return out
}

// readPtr reads a slab record referenced by ptr and returns the value bytes.
func (db *DB) readPtr(ptr page.ValuePtr, set *slab.SlabSet) ([]byte, error) {
	if set == nil {
		return nil, fmt.Errorf("treedb: missing slab set")
	}
	f, ok := set.Get(ptr.FileID)
	if !ok || f == nil || f.Handle == nil {
		return nil, fmt.Errorf("treedb: slab %d missing", ptr.FileID)
	}
	start := int64(ptr.Offset - 4)
	want := int(ptr.Length) + 4
	buf := make([]byte, want)
	if _, err := f.Handle.ReadAt(buf, start); err != nil {
		return nil, err
	}
	// DecodeRecord expects a buffer starting at offset 0 for the slab.
	// We read a single record slice starting at recordStart, so adjust ptr.
	localPtr := page.ValuePtr{Offset: 4, Length: ptr.Length, FileID: ptr.FileID}
	_, val, err := slab.DecodeRecord(buf, localPtr)
	return val, err
}

// encodeMetaPage builds a meta page buffer for pid and meta.
func encodeMetaPage(pid page.PageID, m pager.Meta) ([]byte, error) {
	buf := make([]byte, page.PageSize)
	h, body, err := page.SplitPage(buf)
	if err != nil {
		return nil, err
	}
	h.PageID = pid
	h.Flags = page.PageTypeMeta
	h.Count = 0
	if len(body) < 60 {
		return nil, fmt.Errorf("treedb: meta body too small")
	}
	binary.LittleEndian.PutUint64(body[0:8], m.CommitSeq)
	binary.LittleEndian.PutUint64(body[8:16], uint64(m.UserRootPageID))
	binary.LittleEndian.PutUint64(body[16:24], uint64(m.SystemRootPageID))
	binary.LittleEndian.PutUint64(body[24:32], uint64(m.FreelistHeadID))
	binary.LittleEndian.PutUint64(body[32:40], m.TotalPages)
	binary.LittleEndian.PutUint32(body[40:44], m.ActiveSlabID)
	binary.LittleEndian.PutUint64(body[44:52], m.ActiveSlabTail)
	binary.LittleEndian.PutUint64(body[52:60], m.LastCommitHeight)
	for i := 60; i < len(body); i++ {
		body[i] = 0
	}
	h.SetBodyCRC(body)
	return buf, nil
}

// compareKeys orders byte-slice keys lexicographically.
func compareKeys(a, b []byte) int { return bytes.Compare(a, b) }