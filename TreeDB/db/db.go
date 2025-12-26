package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/vlog"
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
	ValueLogSet      *vlog.Set
}

type DB struct {
	slabManager     *slab.SlabManager
	valueLogManager *vlog.Manager
	lock            *lockfile.Lock
	adaptive        *adaptive.Controller
	pruner          pruneWorker

	// idx is the current index generation (pager + MVCC lifecycle state).
	// It may be swapped during online vacuum; old generations remain alive until
	// pinned readers drain.
	idx atomic.Pointer[indexGen]

	idxMu   sync.Mutex
	idxAll  map[uint64]*indexGen
	idxNext uint64

	snapPool     *SnapshotPool
	ghostManager *indexGhostManager

	dir                  string
	chunkSize            int64
	preferAppendAlloc    bool
	freelistRegionPages  uint64
	freelistRegionRadius int

	keepRecent            uint64
	policy                WritePolicy
	leafFillTargetPPM     uint32
	internalFillTargetPPM uint32
	piggybackCompaction   bool
	// repairSlabTailOnOpen enables tail-record repair during recovery. This
	// protects against torn/partial slab record tails after crashes.
	repairSlabTailOnOpen bool

	mu               sync.RWMutex
	writeMu          sync.RWMutex
	commitMu         sync.Mutex
	vacuumInProgress atomic.Bool
	vacuum           vacuumRecorder
	meta             page.MetaPageBody
	metaPageID       uint64

	state atomic.Pointer[DBState]

	notifyError func(error)
	bgErrMu     sync.Mutex
	bgErr       error
}

type Mode uint8

const (
	ModeCached Mode = iota
	ModeBackend
)

type Options struct {
	Dir        string
	ChunkSize  int64  // Default 256MB
	KeepRecent uint64 // Default 10000
	// DisableBackgroundPrune keeps pruning on the commit critical path (legacy
	// behavior). When false (default), a bounded background pruner frees pages
	// asynchronously to reduce commit latency under churn.
	DisableBackgroundPrune bool
	// PruneInterval controls how often the background pruner wakes up (0 uses a
	// default).
	PruneInterval time.Duration
	// PruneMaxPages bounds how many pages are freed per pruner tick (0 uses a
	// default; <0 means unlimited).
	PruneMaxPages int
	// PruneMaxDuration bounds how long a pruner tick may run (0 uses a default;
	// <0 means unlimited).
	PruneMaxDuration time.Duration

	// BackgroundCompactionInterval enables background slab compaction when > 0.
	// Background compaction is managed by the public wrapper (TreeDB/Open) so it
	// can coordinate with the caching layer.
	BackgroundCompactionInterval          time.Duration
	BackgroundCompactionMaxSlabs          int
	BackgroundCompactionDeadRatio         float64
	BackgroundCompactionMinBytes          uint64
	BackgroundCompactionMicroBatch        int
	BackgroundCompactionCopyBytesPerSec   int64
	BackgroundCompactionCopyBurstBytes    int64
	BackgroundCompactionRotateBeforeWrite bool
	// BackgroundIndexVacuumInterval enables background index vacuum when > 0.
	// The worker uses FragmentationReport span ratio to decide if a rebuild is
	// warranted; see BackgroundIndexVacuumSpanRatioPPM.
	BackgroundIndexVacuumInterval time.Duration
	// BackgroundIndexVacuumSpanRatioPPM is the span ratio threshold (ppm) that
	// triggers a background index vacuum. Zero uses a default.
	BackgroundIndexVacuumSpanRatioPPM uint32

	Mode           Mode // Default ModeCached
	FlushThreshold int64
	// MemtableMode selects the cached-mode memtable implementation.
	// Supported values: "skiplist", "hash_sorted", "btree", "adaptive".
	MemtableMode string
	// MemtableShards controls the number of mutable memtable shards in cached
	// mode. Values <= 0 use a runtime-dependent default.
	MemtableShards int
	// PreferAppendAlloc makes the page allocator ignore the freelist and append
	// new pages instead. This can improve scan locality under churn at the cost
	// of file growth (space is reclaimed later via vacuum).
	PreferAppendAlloc bool
	// FreelistRegionPages and FreelistRegionRadius bias freelist reuse toward
	// nearby page regions to improve locality. Leave both at 0 to disable the
	// bias (default). If either is set, missing values will use defaults.
	// Set FreelistRegionRadius < 0 to force-disable the bias.
	FreelistRegionPages  uint64
	FreelistRegionRadius int

	// LeafFillTargetPPM and InternalFillTargetPPM control how full newly-written
	// B+Tree pages are allowed to become before forcing a split (soft-full).
	// Lower values reduce split churn and slow re-fragmentation under updates, at
	// the cost of higher page count (more index bytes).
	//
	// Values are in parts-per-million where 1_000_000 means "allow full pages"
	// (current behavior). Zero uses the default (1_000_000).
	LeafFillTargetPPM     uint32
	InternalFillTargetPPM uint32
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
	// FlushBuildConcurrency controls how many goroutines may be used to build a
	// combined flush batch from multiple immutable memtables in cached mode.
	// Values <= 1 disable parallelism.
	FlushBuildConcurrency int

	// DisableSlabTailRepairOnOpen disables best-effort recovery that truncates
	// partial/corrupt tail records on the active slab. Disabling may reduce open
	// latency for very large slabs but risks starting up with committed pointers
	// that decode to checksum errors after a crash.
	DisableSlabTailRepairOnOpen bool
	// AllowUnsafe acknowledges unsafe durability/integrity options.
	// When false, Open will reject options that disable WAL, fsync, checksums,
	// or slab tail repair.
	AllowUnsafe bool

	// DisableWAL disables the Write-Ahead Log in cached mode.
	// This improves performance but sacrifices durability: a crash will revert
	// the database to the last Checkpoint (backend flush).
	DisableWAL bool
	// DisableValueLog forces cached-mode WAL to remain in legacy mode (no value-log pointers).
	DisableValueLog bool
	// MaxValueLogRetainedBytes emits a warning when retained value-log bytes exceed
	// this threshold (0 disables warnings). Cached mode only.
	MaxValueLogRetainedBytes int64
	// MaxValueLogRetainedBytesHard disables value-log pointers for new large
	// values once retained bytes exceed this threshold (0 disables the cap).
	MaxValueLogRetainedBytesHard int64

	// RelaxedSync disables fsync on CommitSync and SetSync operations.
	// This improves performance for synchronous workloads but provides only
	// crash consistency (OS buffer cache), not true durability.
	RelaxedSync bool

	// NotifyError is an optional hook for background maintenance failures.
	NotifyError func(error)

	// DisableReadChecksum skips CRC verification on slab reads.
	// This improves read performance (especially for large values) but risks
	// returning silent data corruption if the disk/memory is compromised.
	DisableReadChecksum bool
	// VerifyOnRead forces checksum verification on every index page read,
	// bypassing the verified-page cache.
	VerifyOnRead bool

	// DisablePiggybackCompaction disables opportunistic defragmentation during writes.
	// When false (default), nodes are rewritten if their siblings are physically
	// distant, keeping the tree clustered. Set to true to maximize write speed.
	DisablePiggybackCompaction bool

	// BackgroundCheckpointInterval enables periodic durable checkpoints in cached
	// mode. A checkpoint creates a backend sync boundary and trims
	// cached-mode WAL segments to keep `wal/` growth bounded.
	//
	// Semantics:
	// - `0` uses a default.
	// - `<0` disables the periodic interval trigger.
	BackgroundCheckpointInterval time.Duration
	// BackgroundCheckpointIdleDuration triggers an opportunistic checkpoint after
	// a period of write-idleness in cached mode.
	//
	// Semantics:
	// - `0` uses a default.
	// - `<0` disables the idle trigger.
	BackgroundCheckpointIdleDuration time.Duration
	// MaxWALBytes triggers an immediate checkpoint in cached mode when the sum of
	// WAL segment sizes exceeds this many bytes (0 uses a default; <0 disables the
	// size trigger). This is an operational safety cap; it does not make each
	// individual write durable (use *Sync APIs for that).
	MaxWALBytes int64
}

type Snapshot struct {
	db         *DB
	idx        *indexGen
	state      *DBState
	tree       tree.Tree
	registryID int64
}

func (s *Snapshot) Pager() *pager.Pager {
	if s == nil || s.idx == nil {
		return nil
	}
	return s.idx.pager
}

func (s *Snapshot) State() *DBState {
	if s == nil {
		return nil
	}
	return s.state
}

// AcquireSnapshot returns a new snapshot.
func (db *DB) AcquireSnapshot() *Snapshot {
	db.mu.RLock()
	defer db.mu.RUnlock()

	idx := db.idx.Load()
	state := db.state.Load()
	if state.SlabSet != nil {
		db.slabManager.AcquireSlabs(state.SlabSet) // This now pins the Set, not files
	}
	if state.ValueLogSet != nil {
		db.valueLogManager.Acquire(state.ValueLogSet)
	}

	// Register Reader
	if idx != nil {
		idx.acquire()
	}
	id := int64(0)
	if idx != nil {
		id = idx.registry.Register(state.CommitSeq)
	}

	snap := db.snapPool.Get()
	snap.db = db
	snap.idx = idx
	snap.state = state
	if idx != nil {
		snap.tree.Reset(idx.pager, valueReader{slabs: state.SlabSet, vlogs: state.ValueLogSet}, state.RootPageID)
	}
	snap.registryID = id
	return snap
}

// Close releases the snapshot.
func (s *Snapshot) Close() error {
	var err error
	if s.state != nil && s.state.SlabSet != nil {
		err = s.db.slabManager.ReleaseSlabs(s.state.SlabSet)
	}
	if s.state != nil && s.state.ValueLogSet != nil {
		err = errors.Join(err, s.db.valueLogManager.Release(s.state.ValueLogSet))
	}
	if s.idx != nil {
		s.idx.registry.Unregister(s.registryID)
		s.db.releaseIndex(s.idx)
	}
	if s.db != nil {
		s.db.snapPool.Put(s)
	}
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
	if opts.LeafFillTargetPPM == 0 {
		opts.LeafFillTargetPPM = 1_000_000
	}
	if opts.InternalFillTargetPPM == 0 {
		opts.InternalFillTargetPPM = 1_000_000
	}
	if opts.PruneInterval == 0 {
		opts.PruneInterval = 250 * time.Millisecond
	}
	if opts.PruneMaxPages == 0 {
		opts.PruneMaxPages = 4096
	}
	if opts.PruneMaxDuration == 0 {
		opts.PruneMaxDuration = 25 * time.Millisecond
	}
	if opts.FreelistRegionRadius < 0 {
		opts.FreelistRegionPages = 0
		opts.FreelistRegionRadius = 0
	} else if opts.FreelistRegionPages > 0 || opts.FreelistRegionRadius > 0 {
		if opts.FreelistRegionPages == 0 {
			opts.FreelistRegionPages = 8192
		}
		if opts.FreelistRegionRadius == 0 {
			opts.FreelistRegionRadius = 1
		}
	} else if !opts.PreferAppendAlloc {
		opts.FreelistRegionPages = 8192
		opts.FreelistRegionRadius = 1
	}

	if err := validateUnsafeOptions(opts); err != nil {
		return nil, err
	}
	warnInsecureDir(opts.Dir, opts.NotifyError)

	lock, err := lockfile.Acquire(filepath.Join(opts.Dir, "LOCK"))
	if err != nil {
		return nil, err
	}
	db, err := openWithLock(opts, lock)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	return db, nil
}

func validateUnsafeOptions(opts Options) error {
	if opts.AllowUnsafe {
		return nil
	}
	if opts.DisableWAL || opts.RelaxedSync || opts.DisableReadChecksum || opts.DisableSlabTailRepairOnOpen {
		return ErrUnsafeOptions
	}
	return nil
}

func openWithLock(opts Options, lock *lockfile.Lock) (*DB, error) {
	if err := recoverIndexSwap(opts.Dir); err != nil {
		return nil, err
	}

	idxPath := filepath.Join(opts.Dir, "index.db")
	p, err := pager.Open(idxPath, opts.ChunkSize)
	if err != nil {
		return nil, err
	}
	p.SetVerifyOnRead(opts.VerifyOnRead)

	sm, err := slab.NewSlabManager(opts.Dir)
	if err != nil {
		p.Close()
		return nil, err
	}
	sm.SetDisableReadChecksum(opts.DisableReadChecksum)

	vlogDir := filepath.Join(opts.Dir, "wal")
	vm, err := vlog.NewManager(vlogDir)
	if err != nil {
		p.Close()
		_ = sm.Close()
		return nil, err
	}
	vm.SetDisableReadChecksum(opts.DisableReadChecksum)

	alloc := freelist.New(p, 0)
	alloc.SetPreferAppend(opts.PreferAppendAlloc)
	alloc.SetFreelistRegion(opts.FreelistRegionPages, opts.FreelistRegionRadius)

	z := zipper.New(p, alloc)

	gen := newIndexGen(1, p, alloc, z)

	db := &DB{
		slabManager:           sm,
		valueLogManager:       vm,
		lock:                  lock,
		adaptive:              adaptive.New(),
		keepRecent:            opts.KeepRecent,
		leafFillTargetPPM:     opts.LeafFillTargetPPM,
		internalFillTargetPPM: opts.InternalFillTargetPPM,
		piggybackCompaction:   !opts.DisablePiggybackCompaction,
		repairSlabTailOnOpen:  !opts.DisableSlabTailRepairOnOpen,
		dir:                   opts.Dir,
		chunkSize:             opts.ChunkSize,
		preferAppendAlloc:     opts.PreferAppendAlloc,
		freelistRegionPages:   opts.FreelistRegionPages,
		freelistRegionRadius:  opts.FreelistRegionRadius,
		policy: WritePolicy{
			InlineThreshold: page.DefaultInlineThreshold,
			FlushThreshold:  opts.FlushThreshold,
		},

		idxAll:  map[uint64]*indexGen{gen.id: gen},
		idxNext: gen.id + 1,

		snapPool:     NewSnapshotPool(),
		ghostManager: &indexGhostManager{},
		notifyError:  opts.NotifyError,
	}
	db.ghostManager.start()
	db.idx.Store(gen)

	gen.zipper.SetFillTargets(opts.LeafFillTargetPPM, opts.InternalFillTargetPPM)
	gen.zipper.SetPiggybackCompaction(!opts.DisablePiggybackCompaction)

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
		ValueLogSet:      vm.CurrentSet(),
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

	db.pruner.Start(db, pruneWorkerOptions{
		enabled:     !opts.DisableBackgroundPrune,
		interval:    opts.PruneInterval,
		maxPages:    opts.PruneMaxPages,
		maxDuration: opts.PruneMaxDuration,
	})

	return db, nil
}

func (db *DB) Close() error {
	db.pruner.Stop()
	if db.ghostManager != nil {
		db.ghostManager.stop()
	}

	db.mu.Lock()
	sm := db.slabManager
	vm := db.valueLogManager
	db.slabManager = nil
	db.valueLogManager = nil
	lock := db.lock
	db.lock = nil
	db.mu.Unlock()

	var errs []error
	if err := db.closeAllIndexes(); err != nil {
		errs = append(errs, err)
	}
	if sm != nil {
		if err := sm.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if vm != nil {
		if err := vm.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if lock != nil {
		if err := lock.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if bgErr := db.backgroundError(); bgErr != nil {
		errs = append(errs, bgErr)
	}
	return errors.Join(errs...)
}

func (db *DB) reportError(err error) {
	if err == nil {
		return
	}
	if db.notifyError != nil {
		db.notifyError(err)
	}
	db.bgErrMu.Lock()
	if db.bgErr == nil {
		db.bgErr = err
	}
	db.bgErrMu.Unlock()
}

func (db *DB) backgroundError() error {
	db.bgErrMu.Lock()
	defer db.bgErrMu.Unlock()
	return db.bgErr
}

// recover reads meta pages and restores state.
func (db *DB) recover() error {
	idx := db.idx.Load()
	if idx == nil || idx.pager == nil {
		return errors.New("missing pager")
	}
	p := idx.pager

	if p.PageCount() < 2 {
		if _, err := p.Alloc(2); err != nil {
			return err
		}
		db.meta = page.MetaPageBody{}
		db.metaPageID = MetaPage1ID

		rootID, err := p.Alloc(1)
		if err != nil {
			return err
		}
		data, err := p.GetForWrite(rootID)
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
		sysRootID, err := p.Alloc(1)
		if err != nil {
			return err
		}
		dataSys, err := p.GetForWrite(sysRootID)
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

	if valid0 && !db.metaSlabTailValid(m0) {
		valid0 = false
	}
	if valid1 && !db.metaSlabTailValid(m1) {
		valid1 = false
	}

	type metaCandidate struct {
		id   uint64
		meta page.MetaPageBody
	}
	var candidates []metaCandidate
	if valid0 {
		candidates = append(candidates, metaCandidate{id: MetaPage0ID, meta: m0})
	}
	if valid1 {
		candidates = append(candidates, metaCandidate{id: MetaPage1ID, meta: m1})
	}
	if len(candidates) == 0 {
		return errors.New("both meta pages corrupted")
	}
	// Prefer the highest CommitSeq, but fall back if the active slab tail is not
	// actually readable (e.g. torn last record).
	if len(candidates) == 2 && candidates[0].meta.CommitSeq < candidates[1].meta.CommitSeq {
		candidates[0], candidates[1] = candidates[1], candidates[0]
	}

	var chosen *metaCandidate
	for i := range candidates {
		c := &candidates[i]

		// Re-check slab tail against current size (note: prior iterations may have
		// truncated the slab).
		path := db.slabManager.GetSlabPath(c.meta.ActiveSlabID)
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		if c.meta.ActiveSlabTail > uint64(info.Size()) {
			continue
		}

		if err := db.slabManager.SetActiveSlab(c.meta.ActiveSlabID); err != nil {
			continue
		}
		if err := db.slabManager.TruncateActiveSlab(c.meta.ActiveSlabTail); err != nil {
			return err
		}

		if db.repairSlabTailOnOpen {
			repairedTail, err := db.slabManager.RepairActiveSlabTail()
			if err != nil {
				return err
			}
			if repairedTail < c.meta.ActiveSlabTail {
				// This commit's meta tail was beyond the last checksum-valid record;
				// reject it and fall back to an older meta page.
				continue
			}
		}

		if !db.rootPageValid(p, c.meta.UserRootPageID) || !db.rootPageValid(p, c.meta.SystemRootPageID) {
			continue
		}

		chosen = c
		break
	}
	if chosen == nil {
		return errors.New("no meta page with durable slab tail")
	}

	db.meta = chosen.meta
	db.metaPageID = chosen.id

	if err := db.slabManager.PruneSlabs(chosen.meta.ActiveSlabID); err != nil {
		return err
	}

	if chosen.meta.TotalPages > 0 {
		p.SetPageCount(chosen.meta.TotalPages)
	}

	// Update Allocator Head
	idx.allocator.SetHead(chosen.meta.FreelistHeadID)

	return nil
}

func (db *DB) metaSlabTailValid(m page.MetaPageBody) bool {
	// ActiveSlabTail must not exceed the on-disk slab size; otherwise we'd end up
	// pointing reads at bytes that were never durable (possible after a crash on
	// async writes where index meta reached disk but slab didn't).
	path := db.slabManager.GetSlabPath(m.ActiveSlabID)
	info, err := os.Stat(path)
	if err != nil {
		// For empty/new DBs, active slab should exist; treat missing as invalid.
		return false
	}
	if m.ActiveSlabTail > uint64(info.Size()) {
		return false
	}
	return true
}

func (db *DB) rootPageValid(p *pager.Pager, pageID uint64) bool {
	if pageID == 0 || p == nil {
		return false
	}
	data, err := p.Get(pageID)
	if err != nil {
		return false
	}
	n := node.NewNode(data)
	verifyAlways := p.VerifyOnRead()
	if verifyAlways || !p.IsVerified(pageID) {
		if !n.VerifyChecksum() {
			return false
		}
		if !verifyAlways {
			p.MarkVerified(pageID)
		}
	}
	switch n.Type() {
	case page.PageTypeLeaf, page.PageTypeInternal:
		return true
	default:
		return false
	}
}

func (db *DB) readMeta(pageID uint64) (page.MetaPageBody, bool) {
	idx := db.idx.Load()
	if idx == nil || idx.pager == nil {
		return page.MetaPageBody{}, false
	}

	data, err := idx.pager.Get(pageID)
	if err != nil {
		return page.MetaPageBody{}, false
	}
	n := node.NewNode(data)

	verifyAlways := idx.pager.VerifyOnRead()
	if verifyAlways || !idx.pager.IsVerified(pageID) {
		if !n.VerifyChecksum() {
			return page.MetaPageBody{}, false
		}
		if !verifyAlways {
			idx.pager.MarkVerified(pageID)
		}
	}

	if n.Type() != page.PageTypeMeta {
		return page.MetaPageBody{}, false
	}
	return page.DecodeMetaBody(data[page.PageHeaderSize:]), true
}

func (db *DB) writeMeta(pageID uint64, meta page.MetaPageBody) error {
	idx := db.idx.Load()
	if idx == nil || idx.pager == nil {
		return errors.New("missing pager")
	}

	data, err := idx.pager.GetForWrite(pageID)
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
	idx := db.idx.Load()
	if idx == nil {
		return errors.New("missing index")
	}

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
		if err := idx.pager.Sync(); err != nil {
			return err
		}
	}

	// 2. Prepare Meta - Short Lock
	db.mu.Lock()
	nextMeta := db.meta
	nextMeta.CommitSeq++
	nextMeta.UserRootPageID = newRootID
	nextMeta.SystemRootPageID = sysRootID
	nextMeta.FreelistHeadID = idx.allocator.Head()
	nextMeta.ActiveSlabID = db.slabManager.ActiveSlabID()
	nextMeta.ActiveSlabTail = db.slabManager.ActiveSlabTail()
	nextMeta.TotalPages = idx.pager.PageCount()

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
		if err := idx.pager.Sync(); err != nil {
			return err
		}
	}

	// 5. Update State (Visible) - Short Lock
	db.mu.Lock()
	defer db.mu.Unlock()

	db.meta = nextMeta
	db.metaPageID = targetPageID

	// Add retired pages to Graveyard
	idx.graveyard.Add(nextMeta.CommitSeq, retired)

	// Prune asynchronously to keep commit latency stable under churn.
	// If background pruning is disabled, fall back to legacy on-commit pruning.
	if db.pruner.Enabled() {
		db.pruner.Kick()
	} else {
		db.Prune()
	}

	// Update State
	oldState := db.state.Load()
	newState := &DBState{
		CommitSeq:        nextMeta.CommitSeq,
		RootPageID:       nextMeta.UserRootPageID,
		SystemRootPageID: nextMeta.SystemRootPageID,
		SlabSet:          db.slabManager.CurrentSlabSet(),
		ValueLogSet:      db.valueLogManager.CurrentSet(),
	}
	db.state.Store(newState)

	if oldState != nil {
		db.slabManager.ReleaseSlabs(oldState.SlabSet)
		_ = db.valueLogManager.Release(oldState.ValueLogSet)
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
	idx := db.idx.Load()
	if idx == nil {
		return
	}
	idx.acquire()
	defer db.releaseIndex(idx)

	min := idx.registry.MinPinnedSeq()
	current := db.meta.CommitSeq

	freed := idx.graveyard.Extract(min, current, db.keepRecent)

	if len(freed) > 0 {
		for _, id := range freed {
			_ = idx.allocator.Free(id) // Ignore error?
		}
	}
}

// Get returns value from snapshot.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	return s.tree.Get(key)
}

// GetUnsafe returns a zero-copy view of the value from the snapshot.
// The slice is valid until the snapshot is closed.
func (s *Snapshot) GetUnsafe(key []byte) ([]byte, error) {
	return s.tree.GetUnsafe(key)
}

func (s *Snapshot) Has(key []byte) (bool, error) {
	return s.tree.Has(key)
}

// GetEntry returns the raw entry from snapshot.
func (s *Snapshot) GetEntry(key []byte) (node.LeafEntry, error) {
	return s.tree.GetEntry(key)
}

// Getters
func (db *DB) Pager() *pager.Pager {
	idx := db.idx.Load()
	if idx == nil {
		return nil
	}
	return idx.pager
}
func (db *DB) SlabManager() *slab.SlabManager {
	return db.slabManager
}
func (db *DB) Zipper() *zipper.Zipper {
	idx := db.idx.Load()
	if idx == nil {
		return nil
	}
	return idx.zipper
}
func (db *DB) InlineThreshold() int {
	if db == nil {
		return page.DefaultInlineThreshold
	}
	if db.adaptive != nil {
		return db.adaptive.GetThreshold()
	}
	if db.policy.InlineThreshold > 0 {
		return db.policy.InlineThreshold
	}
	return page.DefaultInlineThreshold
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
		ValueLogSet:      db.valueLogManager.CurrentSet(),
	}
	db.state.Store(newState)

	err := db.slabManager.ReleaseSlabs(oldState.SlabSet)
	return errors.Join(err, db.valueLogManager.Release(oldState.ValueLogSet))
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
		idx := db.idx.Load()
		if idx == nil {
			db.writeMu.Unlock()
			return errors.New("missing index")
		}

		// Snapshot roots under DB lock.
		db.mu.RLock()
		rootID := db.meta.UserRootPageID
		sysRoot := db.meta.SystemRootPageID
		db.mu.RUnlock()

		// Build a micro-batch of still-live pointer updates.
		tr := tree.New(idx.pager, valueReader{slabs: db.slabManager, vlogs: db.valueLogManager}, rootID)
		b := batch.New(db.slabManager, db.policy.InlineThreshold)
		closeBatch := func() { _ = b.Close() }
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
				closeBatch()
				db.writeMu.Unlock()
				return err
			}
			if slabWritesByFile == nil {
				slabWritesByFile = make(map[uint32]int64, 4)
			}
			slabWritesByFile[op.NewPtr.FileID] += int64(op.NewPtr.Length)
		}

		if len(b.Ops()) == 0 {
			closeBatch()
			db.writeMu.Unlock()
			continue
		}

		newRoot, retired, metrics, err := idx.zipper.Apply(rootID, b)
		if err != nil {
			closeBatch()
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
			closeBatch()
			db.writeMu.Unlock()
			return err
		}

		db.writeMu.Unlock()
		closeBatch()
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

	idx := db.idx.Load()
	if idx == nil {
		return errors.New("missing index")
	}

	// Acquire Snapshot
	db.mu.RLock()
	state := db.state.Load()
	tr := tree.New(idx.pager, valueReader{slabs: state.SlabSet, vlogs: state.ValueLogSet}, state.RootPageID)
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
	alloc := &pagerAllocator{p: idx.pager}
	newRoot, err := bulk.Build(iter, alloc, idx.pager)
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

func (a *pagerAllocator) Alloc(hint uint64) (uint64, error) {
	return a.p.Alloc(1)
}
