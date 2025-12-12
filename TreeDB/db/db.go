package db

import (
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"

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
)

type DBState struct {
	CommitSeq  uint64
	RootPageID uint64
	SlabSet    *slab.SlabSet
}

type DB struct {
	pager       *pager.Pager
	slabManager *slab.SlabManager
	zipper      *zipper.Zipper
	// tree        *tree.Tree // Removed: Tree is created per-snapshot or operation
	
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
	db    *DB
	state *DBState
	tree  *tree.Tree
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

	db := &DB{
		pager:       p,
		slabManager: sm,
		zipper:      zipper.New(p),
	}

	if err := db.recover(); err != nil {
		db.Close()
		return nil, err
	}

	// Initialize State
	initialState := &DBState{
		CommitSeq:  db.meta.CommitSeq,
		RootPageID: db.meta.UserRootPageID,
		SlabSet:    sm.CurrentSlabSet(),
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
	
	return nil
}

func (db *DB) readMeta(pageID uint64) (page.MetaPageBody, bool) {
	data, err := db.pager.Get(pageID)
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
	// Note: MetaBody struct size is 60. Header is 16.
	// Ensure we don't panic if page is somehow smaller? Pager guarantees 4KB.
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
func (db *DB) commitLocked(newRootID uint64, sync bool) error {
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
	
	// Update State
	newState := &DBState{
		CommitSeq:  nextMeta.CommitSeq,
		RootPageID: nextMeta.UserRootPageID,
		SlabSet:    db.slabManager.CurrentSlabSet(),
	}
	db.state.Store(newState)
	
	return nil
}

// Commit persists the new root (Sync=true by default for public API convenience if used directly).
func (db *DB) Commit(newRootID uint64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.commitLocked(newRootID, true)
}

// AcquireSnapshot returns a new snapshot.
func (db *DB) AcquireSnapshot() *Snapshot {
	state := db.state.Load()
	db.slabManager.AcquireSlabs(state.SlabSet)
	return &Snapshot{
		db:    db,
		state: state,
		tree:  tree.New(db.pager, db.slabManager, state.RootPageID),
	}
}

// Close releases the snapshot.
func (s *Snapshot) Close() error {
	return s.db.slabManager.ReleaseSlabs(s.state.SlabSet)
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