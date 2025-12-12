package db

import (
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap-gemini/TreeDB/batch"
	"github.com/snissn/gomap-gemini/TreeDB/freelist"
	"github.com/snissn/gomap-gemini/TreeDB/lifecycle"
	"github.com/snissn/gomap-gemini/TreeDB/node"
	"github.com/snissn/gomap-gemini/TreeDB/page"
	"github.com/snissn/gomap-gemini/TreeDB/pager"
	"github.com/snissn/gomap-gemini/TreeDB/slab"
	"github.com/snissn/gomap-gemini/TreeDB/tree"
	"github.com/snissn/gomap-gemini/TreeDB/zipper"
)

const (
	MetaPage0ID = 0
	MetaPage1ID = 1
	KeepRecent  = 10000
)

type DBState struct {
	CommitSeq        uint64
	RootPageID       uint64
	SystemRootPageID uint64
	SlabSet          *slab.SlabSet
}

type DB struct {
	pager       *pager.Pager
	slabManager *slab.SlabManager
	zipper      *zipper.Zipper
	allocator   *freelist.Allocator
	graveyard   *lifecycle.Graveyard
	registry    *lifecycle.ReaderRegistry
	
	inlineThreshold int
	
	mu          sync.RWMutex
	meta        page.MetaPageBody
	metaPageID  uint64 
	
	state       atomic.Pointer[DBState]
}

type Options struct {
	Dir       string
	ChunkSize int64 // Default 256MB
}

type Snapshot struct {
	db         *DB
	state      *DBState
	tree       *tree.Tree
	registryID int64
}

// Open opens the database.
func Open(opts Options) (*DB, error) {
	if opts.ChunkSize == 0 {
		opts.ChunkSize = 256 * 1024 * 1024
	}

	idxPath := filepath.Join(opts.Dir, "index.db")
	p, err := pager.Open(idxPath, opts.ChunkSize)
	if err != nil {
		return nil, err
	}

	sm, err := slab.NewSlabManager(opts.Dir)
	if err != nil {
		p.Close()
		return nil, err
	}

		// Allocator initialized after recovery (needs Meta)

		// But Zipper needs it.

		// We'll init with 0 and update after recovery.

		alloc := freelist.New(p, 0)

		

		db := &DB{

			pager:           p,

			slabManager:     sm,

			zipper:          zipper.New(p, alloc),

			allocator:       alloc,

			graveyard:       lifecycle.NewGraveyard(),

			registry:        lifecycle.NewReaderRegistry(),

			inlineThreshold: page.DefaultInlineThreshold,

		}

	

		if err := db.recover(); err != nil {
		db.Close()
		return nil, err
	}

	// Initialize State
	initialState := &DBState{
		CommitSeq:        db.meta.CommitSeq,
		RootPageID:       db.meta.UserRootPageID,
		SystemRootPageID: db.meta.SystemRootPageID,
		SlabSet:          sm.CurrentSlabSet(),
	}
	db.state.Store(initialState)
	
	return db, nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	
	if err := db.pager.Close(); err != nil {
		return err
	}
	return db.slabManager.Close()
}

// recover reads meta pages and restores state.
func (db *DB) recover() error {
	if db.pager.PageCount() < 2 {
		if _, err := db.pager.Alloc(2); err != nil {
			return err
		}
		db.meta = page.MetaPageBody{}
		db.metaPageID = MetaPage1ID 
		
		rootID, err := db.pager.Alloc(1)
		if err != nil {
			return err
		}
		data, err := db.pager.Get(rootID)
		if err != nil {
			return err
		}
		n := node.NewNode(data)
		n.SetPageID(rootID)
		n.SetType(page.PageTypeLeaf)
		n.SetCount(0)
		n.UpdateChecksum()
		
		db.meta.UserRootPageID = rootID
		
		// Init System Root
		sysRootID, err := db.pager.Alloc(1)
		if err != nil {
			return err
		}
		dataSys, err := db.pager.Get(sysRootID)
		if err != nil {
			return err
		}
		nSys := node.NewNode(dataSys)
		nSys.SetPageID(sysRootID)
		nSys.SetType(page.PageTypeLeaf)
		nSys.SetCount(0)
		nSys.UpdateChecksum()
		
		db.meta.SystemRootPageID = sysRootID
		db.meta.CommitSeq = 0
		
		if err := db.writeMeta(MetaPage0ID, db.meta); err != nil {
			return err
		}
		if err := db.writeMeta(MetaPage1ID, db.meta); err != nil {
			return err
		}
		db.metaPageID = MetaPage0ID
		return nil
	}

	m0, valid0 := db.readMeta(MetaPage0ID)
	m1, valid1 := db.readMeta(MetaPage1ID)

	var activeMeta page.MetaPageBody
	var activeID uint64

	if !valid0 && !valid1 {
		return errors.New("both meta pages corrupted")
	} else if valid0 && !valid1 {
		activeMeta = m0
		activeID = MetaPage0ID
	} else if !valid0 && valid1 {
		activeMeta = m1
		activeID = MetaPage1ID
	} else {
		if m0.CommitSeq >= m1.CommitSeq {
			activeMeta = m0
			activeID = MetaPage0ID
		} else {
			activeMeta = m1
			activeID = MetaPage1ID
		}
	}

	db.meta = activeMeta
	db.metaPageID = activeID

	if err := db.slabManager.SetActiveSlab(activeMeta.ActiveSlabID); err != nil {
		return err
	}
	if err := db.slabManager.TruncateActiveSlab(activeMeta.ActiveSlabTail); err != nil {
		return err
	}
	if err := db.slabManager.PruneSlabs(activeMeta.ActiveSlabID); err != nil {
		return err
	}
	
	if activeMeta.TotalPages > 0 {
		db.pager.SetPageCount(activeMeta.TotalPages)
	}
	
	// Update Allocator Head
	db.allocator.SetHead(activeMeta.FreelistHeadID)
	
	return nil
}

func (db *DB) readMeta(pageID uint64) (page.MetaPageBody, bool) {
	data, err := db.pager.ReadPage(pageID)
	if err != nil {
		return page.MetaPageBody{}, false
	}
	n := node.NewNode(data) 
	if !n.VerifyChecksum() {
		return page.MetaPageBody{}, false
	}
	if n.Type() != page.PageTypeMeta {
		return page.MetaPageBody{}, false
	}
	return page.DecodeMetaBody(data[page.PageHeaderSize:]), true
}

func (db *DB) writeMeta(pageID uint64, meta page.MetaPageBody) error {
	data, err := db.pager.Get(pageID)
	if err != nil {
		return err
	}
	meta.Encode(data[page.PageHeaderSize:])
	n := node.NewNode(data)
	n.SetPageID(pageID)
	n.SetType(page.PageTypeMeta)
	n.SetCount(0)
	n.UpdateChecksum()
	return nil
}

// commitLocked persists the new root.
// Caller must hold db.mu.
func (db *DB) commitLocked(newRootID uint64, sysRootID uint64, retired []uint64, sync bool) error {
	if sync {
		if err := db.slabManager.Sync(); err != nil {
			return err
		}
		if err := db.pager.Sync(); err != nil {
			return err
		}
	}
	
	nextMeta := db.meta
	nextMeta.CommitSeq++
	nextMeta.UserRootPageID = newRootID
	nextMeta.SystemRootPageID = sysRootID
	nextMeta.FreelistHeadID = db.allocator.Head()
	nextMeta.ActiveSlabID = db.slabManager.ActiveSlabID()
	nextMeta.ActiveSlabTail = db.slabManager.ActiveSlabTail()
	nextMeta.TotalPages = db.pager.PageCount()
	
	targetPageID := uint64(0)
	if db.metaPageID == 0 {
		targetPageID = 1
	}
	
	if err := db.writeMeta(targetPageID, nextMeta); err != nil {
		return err
	}
	
	if sync {
		if err := db.pager.Sync(); err != nil {
			return err
		}
	}
	
	db.meta = nextMeta
	db.metaPageID = targetPageID
	
	// Add retired pages to Graveyard
	// Note: We use nextMeta.CommitSeq (the seq we just committed).
	// These pages were replaced by this commit.
	db.graveyard.Add(nextMeta.CommitSeq, retired)
	
	// Prune
	db.Prune()
	
	// Update State
	newState := &DBState{
		CommitSeq:        nextMeta.CommitSeq,
		RootPageID:       nextMeta.UserRootPageID,
		SystemRootPageID: nextMeta.SystemRootPageID,
		SlabSet:          db.slabManager.CurrentSlabSet(),
	}
	db.state.Store(newState)
	
	return nil
}

// Commit persists the new root (Sync=true by default).
// Note: This is usually called internally by Batch.Write or externally if manual root management.
// If manual, retired pages are unknown? `Commit` signature assumes manual root.
// If external user calls Commit, they might not know retired pages.
// We'll accept nil for retired if manual.
func (db *DB) Commit(newRootID uint64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.commitLocked(newRootID, db.meta.SystemRootPageID, nil, true)
}

// AcquireSnapshot returns a new snapshot.
func (db *DB) AcquireSnapshot() *Snapshot {
	db.mu.RLock()
	defer db.mu.RUnlock()

	state := db.state.Load()
	db.slabManager.AcquireSlabs(state.SlabSet)
	
	// Register Reader
	id := db.registry.Register(state.CommitSeq)
	
	return &Snapshot{
		db:         db,
		state:      state,
		tree:       tree.New(db.pager, db.slabManager, state.RootPageID),
		registryID: id,
	}
}

// Close releases the snapshot.
func (s *Snapshot) Close() error {
	s.db.registry.Unregister(s.registryID)
	return s.db.slabManager.ReleaseSlabs(s.state.SlabSet)
}

// Prune reclaims pages from the graveyard.
func (db *DB) Prune() {
	min := db.registry.MinPinnedSeq()
	current := db.meta.CommitSeq
	
	freed := db.graveyard.Extract(min, current, KeepRecent)
	
	if len(freed) > 0 {
		for _, id := range freed {
			_ = db.allocator.Free(id) // Ignore error?
		}
	}
}

// Get returns value from snapshot.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	return s.tree.Get(key)
}

// Getters
func (db *DB) Pager() *pager.Pager {
	return db.pager
}
func (db *DB) SlabManager() *slab.SlabManager {
	return db.slabManager
}
func (db *DB) Zipper() *zipper.Zipper {
	return db.zipper
}
func (db *DB) State() *DBState {
	return db.state.Load()
}

type CompactionOp struct {
	Key    []byte
	OldPtr page.ValuePtr
	NewPtr page.ValuePtr
}

// ApplyCompaction applies pointer updates atomically.
func (db *DB) ApplyCompaction(ops []CompactionOp) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	
	b := batch.New(db.slabManager, db.inlineThreshold)
	
	// Create temporary tree for verification (using current root)
	tr := tree.New(db.pager, db.slabManager, db.meta.UserRootPageID)
	
	for _, op := range ops {
		entry, err := tr.GetEntry(op.Key)
		if err != nil {
			continue // Skip if not found
		}
		
		if entry.Flags & node.FlagPointer != 0 {
			if entry.ValuePtr == op.OldPtr {
				if err := b.SetPointer(op.Key, op.NewPtr); err != nil {
					return err
				}
			}
		}
	}
	
	if len(b.Ops()) == 0 {
		return nil
	}
	
	rootID := db.meta.UserRootPageID
	newRoot, retired, err := db.zipper.Apply(rootID, b)
	if err != nil {
		return err
	}
	
	// Commit with sync=true (Compaction should be durable)
	return db.commitLocked(newRoot, db.meta.SystemRootPageID, retired, true)
}