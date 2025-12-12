package db

import (
	"errors"
	"path/filepath"
	"sync"

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

type DB struct {
	pager       *pager.Pager
	slabManager *slab.SlabManager
	zipper      *zipper.Zipper
	tree        *tree.Tree
	
	mu          sync.RWMutex
	meta        page.MetaPageBody
	metaPageID  uint64 // Which page (0 or 1) holds the current valid meta
}

type Options struct {
	Dir       string
	ChunkSize int64 // Default 256MB
}

// Open opens the database, performing recovery if necessary.
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

	// Initialize Tree with recovering root
	db.tree = tree.New(p, sm, db.meta.UserRootPageID)
	
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
	// 1. Ensure Meta Pages exist (Alloc if new file)
	if db.pager.PageCount() < 2 {
		// New DB? Alloc 0 and 1.
		if _, err := db.pager.Alloc(2); err != nil {
			return err
		}
		// Init empty meta
		db.meta = page.MetaPageBody{}
		db.metaPageID = MetaPage1ID // So next write goes to 0? Or start with 0 valid?
		// Write initial meta to 0 and 1?
		// Let's assume initialized to 0s is fine?
		// We should write a valid empty meta.
		// "New DB" state: CommitSeq=0, Roots=0 (invalid? or empty root?).
		// We need an Empty Root Node.
		
		// Create Empty Root
		rootID, err := db.pager.Alloc(1)
		if err != nil {
			return err
		}
		data, err := db.pager.Get(rootID)
		if err != nil {
			return err
		}
		n := page.UnsafeCastHeader(data)
		n.PageID = rootID
		n.Flags = uint16(page.PageTypeLeaf)
		n.Count = 0
		n.Checksum = page.Checksum(data) // Update checksum
		
		db.meta.UserRootPageID = rootID
		db.meta.CommitSeq = 0
		
		// Write to Meta 0
		if err := db.writeMeta(MetaPage0ID, db.meta); err != nil {
			return err
		}
		// Write to Meta 1
		if err := db.writeMeta(MetaPage1ID, db.meta); err != nil {
			return err
		}
		db.metaPageID = MetaPage0ID
		return nil
	}

	// 2. Read Meta 0 and 1
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
		// Both valid, pick highest Seq
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

	// 3. Slab Repair
	// "Open ActiveSlabID. Truncate it to ActiveSlabTail."
	if err := db.slabManager.SetActiveSlab(activeMeta.ActiveSlabID); err != nil {
		return err
	}
	if err := db.slabManager.TruncateActiveSlab(activeMeta.ActiveSlabTail); err != nil {
		return err
	}

	// 4. Orphan Cleanup
	// "Delete slabs > ActiveSlabID"
	if err := db.slabManager.PruneSlabs(activeMeta.ActiveSlabID); err != nil {
		return err
	}
	
	// Update Pager PageCount
	// We trust Meta.TotalPages. We do NOT truncate the physical file (shrinking forbidden).
	// We just reset the logical count so new allocations overwrite the "dead" tail.
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
	// Verify checksum
	// PageHeader.Checksum covers the BODY.
	// Let's manually verify.
	n := node.NewNode(data) // Wrapper
	if !n.VerifyChecksum() {
		return page.MetaPageBody{}, false
	}
	
	// Check Type
	if n.Type() != page.PageTypeMeta {
		return page.MetaPageBody{}, false
	}
	
	// Decode Body
	// Body starts at PageHeaderSize
	bodyData := data[page.PageHeaderSize:]
	return page.DecodeMetaBody(bodyData), true
}

func (db *DB) writeMeta(pageID uint64, meta page.MetaPageBody) error {
	data, err := db.pager.Get(pageID)
	if err != nil {
		return err
	}
	
	// Write Body
	meta.Encode(data[page.PageHeaderSize:])
	
	// Update Header
	n := node.NewNode(data)
	n.SetPageID(pageID)
	n.SetType(page.PageTypeMeta)
	n.SetCount(0) // Unused for Meta
	n.UpdateChecksum()
	
	return nil
}

// Commit persists the new root.
func (db *DB) Commit(newRootID uint64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 1. Sync Active Slab
	// "fdatasync the active slab file MUST complete before any meta page update"
	if err := db.slabManager.Sync(); err != nil {
		return err
	}
	
	// 2. Sync Index (Pages)
	// Ensure new root and children are durable.
	if err := db.pager.Sync(); err != nil {
		return err
	}
	
	// 3. Update Meta
	nextMeta := db.meta
	nextMeta.CommitSeq++
	nextMeta.UserRootPageID = newRootID
	nextMeta.ActiveSlabID = db.slabManager.ActiveSlabID()
	nextMeta.ActiveSlabTail = db.slabManager.ActiveSlabTail()
	nextMeta.TotalPages = db.pager.PageCount()
	
	// Write to inactive meta page
	targetPageID := uint64(0)
	if db.metaPageID == 0 {
		targetPageID = 1
	}
	
	if err := db.writeMeta(targetPageID, nextMeta); err != nil {
		return err
	}
	
	// 4. Sync Index (Meta)
	if err := db.pager.Sync(); err != nil {
		return err
	}
	
	// 5. Update In-Memory State
	db.meta = nextMeta
	db.metaPageID = targetPageID
	db.tree.SetRoot(newRootID)
	
	return nil
}

func (db *DB) Pager() *pager.Pager {
	return db.pager
}

func (db *DB) SlabManager() *slab.SlabManager {
	return db.slabManager
}

func (db *DB) Zipper() *zipper.Zipper {
	return db.zipper
}

func (db *DB) Tree() *tree.Tree {
	return db.tree
}
