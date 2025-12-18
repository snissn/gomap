package db

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/lifecycle"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/slab"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/TreeDB/zipper"
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
	lock        *lockfile.Lock
	adaptive    *adaptive.Controller

	keepRecent uint64
	policy     WritePolicy

	mu         sync.RWMutex
	writeMu    sync.Mutex
	meta       page.MetaPageBody
	metaPageID uint64

	state atomic.Pointer[DBState]
}

type Mode uint8

const (
	ModeCached Mode = iota
	ModeBackend
)

type Options struct {
	Dir            string
	ChunkSize      int64  // Default 256MB
	KeepRecent     uint64 // Default 10000
	Mode           Mode   // Default ModeCached
	FlushThreshold int64
	// PreferAppendAlloc makes the page allocator ignore the freelist and append
	// new pages instead. This can improve scan locality under churn at the cost
	// of file growth (space is reclaimed later via vacuum).
	PreferAppendAlloc bool
	// MaxQueuedMemtables controls how much immutable-memtable backlog the cached
	// layer will allow before applying backpressure (i.e. forcing flush work on
	// writers). A negative value disables backpressure entirely (higher short-term
	// ingest, but potentially unbounded flush debt). Zero uses the default.
	MaxQueuedMemtables int

	// SlowdownBacklogSeconds begins applying writer backpressure when queued flush
	// backlog exceeds this many seconds of estimated flush work (0 disables).
	SlowdownBacklogSeconds float64
	// StopBacklogSeconds blocks writers when queued flush backlog exceeds this many
	// seconds of estimated flush work (0 disables).
	StopBacklogSeconds float64
	// MaxBacklogBytes is an absolute cap on queued flush backlog bytes (0 disables).
	MaxBacklogBytes int64

	// WriterFlushMaxMemtables bounds how much queued work a writer will help flush
	// per write when backpressure is active (0 uses a default).
	WriterFlushMaxMemtables int
	// WriterFlushMaxDuration bounds how long a writer will spend helping flush per
	// write when backpressure is active (0 disables the time bound).
	WriterFlushMaxDuration time.Duration
}

type Snapshot struct {
	db         *DB
	state      *DBState
	tree       *tree.Tree
	registryID int64
}

// AcquireSnapshot returns a new snapshot.
func (db *DB) AcquireSnapshot() *Snapshot {
	db.mu.RLock()
	defer db.mu.RUnlock()

	state := db.state.Load()
	if state.SlabSet != nil {
		db.slabManager.AcquireSlabs(state.SlabSet) // This now pins the Set, not files
	}

	// Register Reader
	id := db.registry.Register(state.CommitSeq)

	return &Snapshot{
		db:         db,
		state:      state,
		tree:       tree.New(db.pager, state.SlabSet, state.RootPageID),
		registryID: id,
	}
}

// Close releases the snapshot.
func (s *Snapshot) Close() error {
	var err error
	if s.state != nil && s.state.SlabSet != nil {
		err = s.db.slabManager.ReleaseSlabs(s.state.SlabSet)
	}
	s.db.registry.Unregister(s.registryID)
	return err
}

// Open opens the database.
func Open(opts Options) (*DB, error) {
	if opts.Dir == "" {
		return nil, errors.New("db dir required")
	}
	if opts.ChunkSize == 0 {
		opts.ChunkSize = 256 * 1024 * 1024
	}
	if opts.KeepRecent == 0 {
		opts.KeepRecent = 10000
	}

	lock, err := lockfile.Acquire(filepath.Join(opts.Dir, "LOCK"))
	if err != nil {
		return nil, err
	}

	idxPath := filepath.Join(opts.Dir, "index.db")
	p, err := pager.Open(idxPath, opts.ChunkSize)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}

	sm, err := slab.NewSlabManager(opts.Dir)
	if err != nil {
		p.Close()
		_ = lock.Close()
		return nil, err
	}

	// Allocator initialized after recovery (needs Meta)

	// But Zipper needs it.

	// We'll init with 0 and update after recovery.

	alloc := freelist.New(p, 0)
	alloc.SetPreferAppend(opts.PreferAppendAlloc)

	db := &DB{
		pager:       p,
		slabManager: sm,
		zipper:      zipper.New(p, alloc),
		allocator:   alloc,
		graveyard:   lifecycle.NewGraveyard(),
		registry:    lifecycle.NewReaderRegistry(),
		lock:        lock,
		adaptive:    adaptive.New(),
		keepRecent:  opts.KeepRecent,
		policy: WritePolicy{
			InlineThreshold: page.DefaultInlineThreshold,
			FlushThreshold:  opts.FlushThreshold,
		},
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

	segments, err := listWALSegments(opts.Dir)
	if err != nil {
		db.Close()
		return nil, err
	}
	if err := replayWALIntoBackend(db, segments); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	var errs []error

	if db.pager != nil {
		if err := db.pager.Close(); err != nil {
			errs = append(errs, err)
		}
		db.pager = nil
	}

	if db.slabManager != nil {
		if err := db.slabManager.Close(); err != nil {
			errs = append(errs, err)
		}
		db.slabManager = nil
	}

	if db.lock != nil {
		if err := db.lock.Close(); err != nil {
			errs = append(errs, err)
		}
		db.lock = nil
	}

	return errors.Join(errs...)
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
		data, err := db.pager.GetForWrite(rootID)
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
		dataSys, err := db.pager.GetForWrite(sysRootID)
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
		// Debug print
		fmt.Printf("DEBUG db.recover: Setting pager page count to %d (from activeMeta.TotalPages)\n", activeMeta.TotalPages)
		db.pager.SetPageCount(activeMeta.TotalPages)
	}

	// Update Allocator Head
	db.allocator.SetHead(activeMeta.FreelistHeadID)

	return nil
}

func (db *DB) readMeta(pageID uint64) (page.MetaPageBody, bool) {
	data, err := db.pager.Get(pageID)
	if err != nil {
		return page.MetaPageBody{}, false
	}
	n := node.NewNode(data)

	if !db.pager.IsVerified(pageID) {
		if !n.VerifyChecksum() {
			return page.MetaPageBody{}, false
		}
		db.pager.MarkVerified(pageID)
	}

	if n.Type() != page.PageTypeMeta {
		return page.MetaPageBody{}, false
	}
	return page.DecodeMetaBody(data[page.PageHeaderSize:]), true
}

func (db *DB) writeMeta(pageID uint64, meta page.MetaPageBody) error {
	data, err := db.pager.GetForWrite(pageID)
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

// finalizeCommit handles durability and state updates with minimal lock contention.
func (db *DB) finalizeCommit(newRootID uint64, sysRootID uint64, retired []uint64, sync bool, metrics adaptive.Metrics) error {
	// 0. Update System metadata tree (slab stats, etc) before sync/meta.
	//
	// This mutates index pages, so it must run before any Sync() durability
	// boundary.
	if nextSysRoot, sysRetired, err := db.applySystemStatsUpdates(sysRootID, metrics); err != nil {
		return err
	} else if nextSysRoot != sysRootID || len(sysRetired) > 0 {
		sysRootID = nextSysRoot
		if len(sysRetired) > 0 {
			retired = append(retired, sysRetired...)
		}
	}

	// 1. Sync Data (Slabs + Index Pages) - No DB Lock
	if sync {
		if err := db.slabManager.Sync(); err != nil {
			return err
		}
		if err := db.pager.Sync(); err != nil {
			return err
		}
	}

	// 2. Prepare Meta - Short Lock
	db.mu.Lock()
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
	db.mu.Unlock()

	// 3. Write Meta - No DB Lock
	if err := db.writeMeta(targetPageID, nextMeta); err != nil {
		return err
	}

	// 4. Sync Meta - No DB Lock
	if sync {
		if err := db.pager.Sync(); err != nil {
			return err
		}
	}

	// 5. Update State (Visible) - Short Lock
	db.mu.Lock()
	defer db.mu.Unlock()

	db.meta = nextMeta
	db.metaPageID = targetPageID

	// Add retired pages to Graveyard
	db.graveyard.Add(nextMeta.CommitSeq, retired)

	// Prune
	db.Prune()

	// Update State
	oldState := db.state.Load()
	newState := &DBState{
		CommitSeq:        nextMeta.CommitSeq,
		RootPageID:       nextMeta.UserRootPageID,
		SystemRootPageID: nextMeta.SystemRootPageID,
		SlabSet:          db.slabManager.CurrentSlabSet(),
	}
	db.state.Store(newState)

	if oldState != nil {
		db.slabManager.ReleaseSlabs(oldState.SlabSet)
	}

	if db.adaptive != nil {
		db.adaptive.RecordCommit(metrics)
	}

	return nil
}

// Commit persists the new root (Sync=true by default).
// Note: This is usually called internally by Batch.Write or externally if manual root management.
// If manual, retired pages are unknown? `Commit` signature assumes manual root.
// If external user calls Commit, they might not know retired pages.
// We'll accept nil for retired if manual.
func (db *DB) Commit(newRootID uint64) error {
	// Public Commit assumes the caller has built a new tree.
	// We need to serialize with other writers.
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	// Since we are committing a root provided by caller, we assume they based it on current state?
	// If caller is external, they might have read old state.
	// But Commit(newRoot) implies "Force Set Root".
	// We just commit it.

	// Need sysRootID.
	db.mu.RLock()
	sysRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()

	return db.finalizeCommit(newRootID, sysRoot, nil, true, adaptive.Metrics{})
}

// Prune reclaims pages from the graveyard.
func (db *DB) Prune() {
	min := db.registry.MinPinnedSeq()
	current := db.meta.CommitSeq

	freed := db.graveyard.Extract(min, current, db.keepRecent)

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

// GetEntry returns the raw entry from snapshot.
func (s *Snapshot) GetEntry(key []byte) (node.LeafEntry, error) {
	return s.tree.GetEntry(key)
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

// RefreshSlabSet publishes a new DBState with the current SlabSet (excluding
// zombies) without creating a new commit. This is used by background compaction
// so that future snapshots stop pinning compacted slabs immediately.
func (db *DB) RefreshSlabSet() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	oldState := db.state.Load()
	if oldState == nil {
		return nil
	}

	newState := &DBState{
		CommitSeq:        oldState.CommitSeq,
		RootPageID:       oldState.RootPageID,
		SystemRootPageID: oldState.SystemRootPageID,
		SlabSet:          db.slabManager.CurrentSlabSet(),
	}
	db.state.Store(newState)

	return db.slabManager.ReleaseSlabs(oldState.SlabSet)
}

type CompactionOp struct {
	Key    []byte
	OldPtr page.ValuePtr
	NewPtr page.ValuePtr
}

const defaultCompactionMicroBatchSize = 256

// ApplyCompaction applies pointer updates to the current tree. It uses
// micro-batching to bound time under the writer lock.
func (db *DB) ApplyCompaction(ops []CompactionOp) error {
	return db.ApplyCompactionMicroBatches(ops, defaultCompactionMicroBatchSize)
}

// ApplyCompactionMicroBatches applies compaction pointer updates in chunks of at
// most maxOps per commit. This bounds writer pauses and keeps the system
// responsive under large compactions.
func (db *DB) ApplyCompactionMicroBatches(ops []CompactionOp, maxOps int) error {
	if len(ops) == 0 {
		return nil
	}
	if maxOps <= 0 {
		maxOps = defaultCompactionMicroBatchSize
	}

	for start := 0; start < len(ops); start += maxOps {
		end := start + maxOps
		if end > len(ops) {
			end = len(ops)
		}
		chunk := ops[start:end]

		db.writeMu.Lock()

		// Snapshot roots under DB lock.
		db.mu.RLock()
		rootID := db.meta.UserRootPageID
		sysRoot := db.meta.SystemRootPageID
		db.mu.RUnlock()

		// Build a micro-batch of still-live pointer updates.
		tr := tree.New(db.pager, db.slabManager, rootID)
		b := batch.New(db.slabManager, db.policy.InlineThreshold)
		var slabWritesByFile map[uint32]int64

		for _, op := range chunk {
			entry, err := tr.GetEntry(op.Key)
			if err != nil {
				continue
			}
			if entry.Flags&node.FlagPointer == 0 {
				continue
			}
			if entry.ValuePtr != op.OldPtr {
				continue
			}
			if err := b.SetPointer(op.Key, op.NewPtr); err != nil {
				db.writeMu.Unlock()
				return err
			}
			if slabWritesByFile == nil {
				slabWritesByFile = make(map[uint32]int64, 4)
			}
			slabWritesByFile[op.NewPtr.FileID] += int64(op.NewPtr.Length)
		}

		if len(b.Ops()) == 0 {
			db.writeMu.Unlock()
			continue
		}

		newRoot, retired, metrics, err := db.zipper.Apply(rootID, b)
		if err != nil {
			db.writeMu.Unlock()
			return err
		}
		if len(slabWritesByFile) > 0 {
			if metrics.SlabWriteBytesByFile == nil {
				metrics.SlabWriteBytesByFile = slabWritesByFile
			} else {
				for id, n := range slabWritesByFile {
					metrics.SlabWriteBytesByFile[id] += n
				}
			}
		}

		// Commit without forcing Sync; compaction can be lazily durable.
		if err := db.finalizeCommit(newRoot, sysRoot, retired, false, metrics); err != nil {
			db.writeMu.Unlock()
			return err
		}

		db.writeMu.Unlock()
	}

	return nil
}

// CompactIndex rewrites the entire B-Tree sequentially to the end of the file.
// This improves Full Scan performance by restoring physical locality.
// Note: This operation causes file growth as old pages are not immediately reclaimed
// (they are leaked to the freelist but not reused during this append-only build).
func (db *DB) CompactIndex() error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	// Acquire Snapshot
	db.mu.RLock()
	state := db.state.Load()
	tr := tree.New(db.pager, state.SlabSet, state.RootPageID)
	rootID := state.RootPageID
	db.mu.RUnlock()

	// Collect pages in the old tree so they can be retired after the swap.
	retired, err := tr.CollectPageIDs()
	if err != nil {
		return err
	}

	// Create Iterator (Full Scan)
	iter := tr.Iterator(nil, nil)
	defer iter.Close()

	// Build new tree sequentially
	alloc := &pagerAllocator{p: db.pager}
	newRoot, err := bulk.Build(iter, alloc, db.pager)
	if err != nil {
		return err
	}

	db.mu.Lock()
	if db.meta.UserRootPageID != rootID {
		db.mu.Unlock()
		return fmt.Errorf("concurrent modification detected during compaction")
	}
	sysRoot := db.meta.SystemRootPageID
	db.mu.Unlock()

	// Commit new root and retire the old tree pages.
	return db.finalizeCommit(newRoot, sysRoot, retired, true, adaptive.Metrics{})
}

type pagerAllocator struct {
	p *pager.Pager
}

func (a *pagerAllocator) Alloc() (uint64, error) {
	return a.p.Alloc(1)
}
