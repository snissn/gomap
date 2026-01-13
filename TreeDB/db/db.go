package db

import (
	"encoding/binary"
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
	LastSeq          uint64
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

	readOnly bool

	keepRecent            uint64
	policy                WritePolicy
	leafFillTargetPPM     uint32
	internalFillTargetPPM uint32
	leafPrefixCompression bool
	piggybackCompaction   bool
	enableValueIndex      bool
	forceValuePointers    bool
	omitSlabKeys          bool
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

	nextValueID atomic.Uint64

	state atomic.Pointer[DBState]

	notifyError func(error)
	bgErrMu     sync.Mutex
	bgErr       error

	lastGCStats atomic.Pointer[GCStats]
}

type Mode uint8

const (
	ModeCached Mode = iota
	ModeBackend
)

type Options struct {
	Dir string
	// ReadOnly opens the database without acquiring an exclusive lock and without
	// modifying on-disk state (no recovery truncation, no WAL replay, no background
	// maintenance). Only read operations are supported.
	ReadOnly   bool
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
	// BackgroundCompactionIndexSwap compacts slabs by rebuilding the index into a
	// new file and swapping it in once per pass (two-index-file approach). This
	// can drastically reduce index churn during large slab pointer rewrites.
	BackgroundCompactionIndexSwap bool
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
	// IteratorMutableMaxBytes allows iterators to read from mutable memtables
	// without forcing a rotation when the mutable size is small. This preserves
	// snapshot isolation but can block writers while iterators are open.
	// A value <= 0 disables the optimization.
	IteratorMutableMaxBytes int64
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
	// LeafPrefixCompression enables prefix-compressed leaf nodes for new pages.
	LeafPrefixCompression bool
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
	// WALCompression enables compression for the metadata WAL.
	WALCompression bool
	// DisableValueLog forces cached-mode WAL to remain in legacy mode (no value-log pointers).
	DisableValueLog bool
	// SplitValueLog stores WAL records in wal/ while large values go to vlog/
	// segments, and WAL entries reference them via pointers.
	SplitValueLog bool
	// WALMaxSegmentBytes caps the size of a single WAL segment payload.
	// 0 uses the default limit.
	WALMaxSegmentBytes int64
	// MemtableValueLogPointers avoids storing large values in the memtable and
	// serves them by pointer from the value log (WAL/vlog). Requires WAL/value-log.
	MemtableValueLogPointers bool
	// ValueLogPointerThreshold controls when WAL/vlog pointers are used.
	// Values <= 0 use the default inline threshold (256 bytes).
	ValueLogPointerThreshold int
	// ForceValuePointers stores all values out-of-line in slabs (no inline values).
	ForceValuePointers bool
	// EnableValueIndex enables the Value Index for large values (pointer-backed).
	// Writes generate a ValueID and store the mapping in the System Tree.
	// Reads resolve the ValueID via the System Tree.
	EnableValueIndex bool
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
	// SlabCompression configures compression for slab-stored values.
	SlabCompression slab.CompressionOptions
	// SlabCompressionMetrics logs rolling compression ratios for slabs.
	SlabCompressionMetrics bool
	// SlabCompressionMetricsWindowBytes controls log window size for compression ratios.
	SlabCompressionMetricsWindowBytes int
	// SlabCompressionAdaptiveRatio enables adaptive compression pausing when ratios degrade (>= threshold).
	SlabCompressionAdaptiveRatio float64
	// SlabCompressionAdaptivePauseBytes controls how many raw bytes to skip after a degradation trigger.
	SlabCompressionAdaptivePauseBytes int
	// SlabCompressionAdaptiveMinRecords controls the minimum records per window before triggering pause.
	SlabCompressionAdaptiveMinRecords int
	// SlabCompressionAdaptiveTrainBytes controls how many raw bytes to sample for training (0 disables).
	SlabCompressionAdaptiveTrainBytes int
	// SlabCompressionAdaptiveTrainDictBytes controls the trained dictionary size.
	SlabCompressionAdaptiveTrainDictBytes int
	// SlabCompressionAdaptiveTrainMinRecords controls the minimum records before training.
	SlabCompressionAdaptiveTrainMinRecords int
	// SlabCompressionAdaptiveTrainMaxRecordBytes caps per-record sample size for training.
	SlabCompressionAdaptiveTrainMaxRecordBytes int
	// SlabCompressionAdaptiveTrainSampleStride samples every Nth record for training.
	SlabCompressionAdaptiveTrainSampleStride int
	// SlabCompressionAdaptiveTrainDedupWindow controls the exact-hash dedup window size.
	SlabCompressionAdaptiveTrainDedupWindow int
	// OmitSlabKeys avoids storing the key in the slab record. This saves space
	// for small records but requires IndexSwap compaction (the default compactor
	// will skip these records as dead).
	OmitSlabKeys bool
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
		opts.KeepRecent = 20
	}
	if opts.LeafFillTargetPPM == 0 {
		opts.LeafFillTargetPPM = 1_000_000
	}
	if opts.InternalFillTargetPPM == 0 {
		opts.InternalFillTargetPPM = 1_000_000
	}
	if opts.PruneInterval == 0 {
		opts.PruneInterval = 100 * time.Millisecond
	}
	if opts.PruneMaxPages == 0 {
		opts.PruneMaxPages = 40960
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

	if opts.ReadOnly {
		return openReadOnly(opts)
	}

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
	if opts.ReadOnly {
		// Read-only opens never mutate on-disk state, so "unsafe" write options do
		// not apply.
		return nil
	}
	if opts.AllowUnsafe {
		return nil
	}
	if opts.DisableWAL || opts.RelaxedSync || opts.DisableReadChecksum || opts.DisableSlabTailRepairOnOpen || opts.MemtableValueLogPointers {
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

	sm, err := slab.NewSlabManagerWithOptions(opts.Dir, slab.Options{
		Compression:                            opts.SlabCompression,
		OmitSlabKeys:                           opts.OmitSlabKeys,
		CompressionMetrics:                     opts.SlabCompressionMetrics,
		CompressionMetricsWindowBytes:          opts.SlabCompressionMetricsWindowBytes,
		CompressionAdaptiveRatio:               opts.SlabCompressionAdaptiveRatio,
		CompressionAdaptivePauseBytes:          opts.SlabCompressionAdaptivePauseBytes,
		CompressionAdaptiveMinRecords:          opts.SlabCompressionAdaptiveMinRecords,
		CompressionAdaptiveTrainBytes:          opts.SlabCompressionAdaptiveTrainBytes,
		CompressionAdaptiveTrainDictBytes:      opts.SlabCompressionAdaptiveTrainDictBytes,
		CompressionAdaptiveTrainMinRecords:     opts.SlabCompressionAdaptiveTrainMinRecords,
		CompressionAdaptiveTrainMaxRecordBytes: opts.SlabCompressionAdaptiveTrainMaxRecordBytes,
		CompressionAdaptiveTrainSampleStride:   opts.SlabCompressionAdaptiveTrainSampleStride,
		CompressionAdaptiveTrainDedupWindow:    opts.SlabCompressionAdaptiveTrainDedupWindow,
	})
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

	adaptiveCtrl := adaptive.New()
	inlineThreshold := page.DefaultInlineThreshold
	if opts.ForceValuePointers {
		inlineThreshold = 0
		adaptiveCtrl = nil
	}

	db := &DB{
		slabManager:           sm,
		valueLogManager:       vm,
		lock:                  lock,
		adaptive:              adaptiveCtrl,
		keepRecent:            opts.KeepRecent,
		leafFillTargetPPM:     opts.LeafFillTargetPPM,
		internalFillTargetPPM: opts.InternalFillTargetPPM,
		leafPrefixCompression: opts.LeafPrefixCompression,
		piggybackCompaction:   !opts.DisablePiggybackCompaction,
		repairSlabTailOnOpen:  !opts.DisableSlabTailRepairOnOpen,
		enableValueIndex:      opts.EnableValueIndex,
		forceValuePointers:    opts.ForceValuePointers,
		omitSlabKeys:          opts.OmitSlabKeys,
		dir:                   opts.Dir,
		chunkSize:             opts.ChunkSize,
		preferAppendAlloc:     opts.PreferAppendAlloc,
		freelistRegionPages:   opts.FreelistRegionPages,
		freelistRegionRadius:  opts.FreelistRegionRadius,
		policy: WritePolicy{
			InlineThreshold: inlineThreshold,
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
	gen.zipper.SetLeafPrefixCompression(opts.LeafPrefixCompression)

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

	includeValueLog := !opts.DisableValueLog && !opts.SplitValueLog
	segments, err := listWALSegments(opts.Dir, includeValueLog)
	if err != nil {
		db.Close()
		return nil, err
	}
	if err := replayWALIntoBackend(db, segments, opts.WALMaxSegmentBytes); err != nil {
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
		if db.readOnly {
			return errors.New("read-only open requires an existing index with meta pages")
		}
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
		b := node.NewBuilderWithOptions(data, page.PageTypeLeaf, node.BuilderOptions{LeafPrefixCompression: db.leafPrefixCompression})
		b.SetPageID(rootID)
		b.Finish()

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
		bSys := node.NewBuilderWithOptions(dataSys, page.PageTypeLeaf, node.BuilderOptions{LeafPrefixCompression: db.leafPrefixCompression})
		bSys.SetPageID(sysRootID)
		bSys.Finish()

		db.meta.SystemRootPageID = sysRootID
		db.meta.CommitSeq = 0
		db.meta.NextValueID = 1

		if err := db.writeMeta(MetaPage0ID, db.meta); err != nil {
			return err
		}
		if err := db.writeMeta(MetaPage1ID, db.meta); err != nil {
			return err
		}
		db.metaPageID = MetaPage0ID
		db.nextValueID.Store(db.meta.NextValueID)
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
		if !db.readOnly {
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
		}

		if !db.rootPageValid(p, c.meta.UserRootPageID) || !db.rootPageValid(p, c.meta.SystemRootPageID) {
			continue
		}
		if !db.freelistHeadValid(p, c.meta.FreelistHeadID) {
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
	db.nextValueID.Store(db.meta.NextValueID)

	if !db.readOnly {
		if err := db.slabManager.PruneSlabs(chosen.meta.ActiveSlabID); err != nil {
			return err
		}
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

func (db *DB) freelistHeadValid(p *pager.Pager, head uint64) bool {
	if head == 0 || p == nil {
		return true
	}
	data, err := p.Get(head)
	if err != nil {
		return false
	}
	n := node.NewNode(data)
	verifyAlways := p.VerifyOnRead()
	if verifyAlways || !p.IsVerified(head) {
		if !n.VerifyChecksum() {
			return false
		}
		if !verifyAlways {
			p.MarkVerified(head)
		}
	}
	return n.Type() == page.PageTypeFreelist
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
func (db *DB) finalizeCommit(newRootID uint64, sysRootID uint64, retired []uint64, sync bool, sysOps []batch.Entry, metrics adaptive.Metrics, lastSeq uint64) error {
	if db.readOnly {
		return ErrReadOnly
	}
	idx := db.idx.Load()
	if idx == nil {
		return errors.New("missing index")
	}

	// 0. Update System metadata tree (slab stats, etc) before sync/meta.
	//
	// This mutates index pages, so it must run before any Sync() durability
	// boundary.
	if nextSysRoot, sysRetired, err := db.applySystemUpdates(sysRootID, sysOps, metrics); err != nil {
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
	nextMeta.NextValueID = db.nextValueID.Load()
	if lastSeq > nextMeta.LastSeq {
		nextMeta.LastSeq = lastSeq
	}

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
		LastSeq:          nextMeta.LastSeq,
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
	if db.readOnly {
		return ErrReadOnly
	}
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

	return db.finalizeCommit(newRootID, sysRoot, nil, true, nil, adaptive.Metrics{}, 0)
}

// Prune reclaims pages from the graveyard.
func (db *DB) Prune() {
	if db.readOnly {
		return
	}
	idx := db.idx.Load()
	if idx == nil {
		return
	}
	idx.acquire()
	defer db.releaseIndex(idx)

	min := idx.registry.MinPinnedSeq()
	state := db.state.Load()
	if state == nil {
		return
	}
	current := state.CommitSeq

	freed := idx.graveyard.Extract(min, current, db.keepRecent)

	if len(freed) > 0 {
		for _, id := range freed {
			_ = idx.allocator.Free(id) // Ignore error?
		}
	}
}

// Get returns value from snapshot.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	entry, err := s.tree.GetEntry(key)
	if err != nil {
		return nil, err
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return nil, tree.ErrKeyNotFound
	}

	if entry.Flags&node.FlagValueID != 0 {
		if len(entry.Value) != 8 {
			return nil, fmt.Errorf("invalid value id length in Get: %d", len(entry.Value))
		}
		return s.resolveValueID(entry.Value, false)
	}

	if entry.Flags&node.FlagPointer != 0 {
		// Use tree's configured reader (which is ValueReaderForState logic internally)
		// But s.tree.slabReader is private.
		// Reconstruct reader.
		vr := ValueReaderForState(s.state)
		return vr.Read(entry.ValuePtr)
	}

	// Inline value
	// Return copy
	val := make([]byte, len(entry.Value))
	copy(val, entry.Value)
	return val, nil
}

// GetUnsafe returns a zero-copy view of the value from the snapshot.
// The slice is valid until the snapshot is closed.
func (s *Snapshot) GetUnsafe(key []byte) ([]byte, error) {
	entry, err := s.tree.GetEntry(key)
	if err != nil {
		return nil, err
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return nil, tree.ErrKeyNotFound
	}

	if entry.Flags&node.FlagValueID != 0 {
		return s.resolveValueID(entry.Value, true)
	}

	if entry.Flags&node.FlagPointer != 0 {
		vr := ValueReaderForState(s.state)
		return vr.ReadUnsafe(entry.ValuePtr)
	}

	return entry.Value, nil
}

func (s *Snapshot) resolveValueID(idBytes []byte, unsafe bool) ([]byte, error) {
	ptr, err := s.ResolveValueIDToPtr(idBytes)
	if err != nil {
		return nil, err
	}

	vr := ValueReaderForState(s.state)
	if unsafe {
		return vr.ReadUnsafe(ptr)
	}
	return vr.Read(ptr)
}

func (s *Snapshot) ResolveValueIDToPtr(idBytes []byte) (page.ValuePtr, error) {
	if len(idBytes) != 8 {
		return page.ValuePtr{}, fmt.Errorf("invalid value id length: %d", len(idBytes))
	}
	id := ValueID(binary.BigEndian.Uint64(idBytes))

	// System Tree Lookup
	sysTree := tree.New(s.idx.pager, ValueReaderForState(s.state), s.state.SystemRootPageID)
	vi := valueIndexHelper{}
	return vi.Get(sysTree, id)
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
	if db.policy.InlineThreshold >= 0 {
		return db.policy.InlineThreshold
	}
	return page.DefaultInlineThreshold
}
func (db *DB) State() *DBState {
	return db.state.Load()
}

// LastSeq returns the highest sequence number persisted in the database.
func (db *DB) LastSeq() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.meta.LastSeq
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
		LastSeq:          oldState.LastSeq,
	}
	db.state.Store(newState)

	err := db.slabManager.ReleaseSlabs(oldState.SlabSet)
	return errors.Join(err, db.valueLogManager.Release(oldState.ValueLogSet))
}

// MarkValueLogZombie marks a value-log segment as zombie so it can be removed
// once all snapshots release it.
func (db *DB) MarkValueLogZombie(id uint32) error {
	if db == nil || db.valueLogManager == nil {
		return fmt.Errorf("value log manager unavailable")
	}
	return db.valueLogManager.MarkZombie(id)
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

		// Fast path: if no readers are pinned to the current index generation,
		// update leaf pointer values in-place to avoid COW page churn.
		db.mu.Lock()
		rootID := db.meta.UserRootPageID
		sysRoot := db.meta.SystemRootPageID
		noPinnedReaders := idx.registry.MinPinnedSeq() == ^uint64(0)
		if noPinnedReaders {
			tr := tree.New(idx.pager, nil, rootID)
			sysTr := tree.New(idx.pager, nil, sysRoot)
			var (
				metrics       adaptive.Metrics
				modifiedPages map[uint64]struct{}
			)
			for _, op := range chunk {
				updated, leafID, err := tr.UpdateValuePtrInPlace(op.Key, op.OldPtr, op.NewPtr)
				if err != nil {
					db.mu.Unlock()
					db.writeMu.Unlock()
					return err
				}

				if !updated {
					// Key might be using a ValueID. If so, update the pointer in the
					// system tree in-place.
					entry, err := tr.GetEntry(op.Key)
					if err == nil && entry.Flags&node.FlagValueID != 0 && len(entry.Value) == 8 {
						vid := ValueID(binary.BigEndian.Uint64(entry.Value))
						sysKey := encodeValueIndexKey(vid)
						sysUpdated, sysLeafID, err := sysTr.UpdateValuePtrInPlace(sysKey, op.OldPtr, op.NewPtr)
						if err != nil {
							db.mu.Unlock()
							db.writeMu.Unlock()
							return err
						}
						if sysUpdated {
							updated = true
							leafID = sysLeafID
						}
					}
				}

				if !updated {
					continue
				}

				if modifiedPages == nil {
					modifiedPages = make(map[uint64]struct{}, 8)
				}
				modifiedPages[leafID] = struct{}{}

				if metrics.SlabWriteBytesByFile == nil {
					metrics.SlabWriteBytesByFile = make(map[uint32]int64, 4)
				}
				metrics.SlabWriteBytesByFile[op.NewPtr.FileID] += int64(page.ValuePtrRecordLength(op.NewPtr))

				if metrics.SlabDeadBytesByFile == nil {
					metrics.SlabDeadBytesByFile = make(map[uint32]int64, 4)
				}
				metrics.SlabDeadBytesByFile[op.OldPtr.FileID] += int64(page.ValuePtrRecordLength(op.OldPtr))
				metrics.SlabDeadBytes += int(page.ValuePtrRecordLength(op.OldPtr))
			}
			if len(modifiedPages) > 0 {
				metrics.IndexWriteBytes += len(modifiedPages) * page.PageSize
			}
			db.mu.Unlock()

			if len(metrics.SlabWriteBytesByFile) == 0 && len(metrics.SlabDeadBytesByFile) == 0 {
				db.writeMu.Unlock()
				continue
			}

			// Commit without forcing Sync; compaction can be lazily durable.
			if err := db.finalizeCommit(rootID, sysRoot, nil, false, nil, metrics, 0); err != nil {
				db.writeMu.Unlock()
				return err
			}

			db.writeMu.Unlock()
			continue
		}
		db.mu.Unlock()

		// Build a micro-batch of still-live pointer updates.
		tr := tree.New(idx.pager, valueReader{slabs: db.slabManager, vlogs: db.valueLogManager}, rootID)
		sysTr := tree.New(idx.pager, valueReader{slabs: db.slabManager, vlogs: db.valueLogManager}, sysRoot)
		b := batch.New(db.slabManager, db.policy.InlineThreshold)
		closeBatch := func() { _ = b.Close() }
		var slabWritesByFile map[uint32]int64
		var sysOps []batch.Entry
		var metrics adaptive.Metrics

		for _, op := range chunk {
			entry, err := tr.GetEntry(op.Key)
			if err != nil {
				continue
			}

			// Value Index Path
			if entry.Flags&node.FlagValueID != 0 {
				if len(entry.Value) != 8 {
					continue
				}
				vid := ValueID(binary.BigEndian.Uint64(entry.Value))
				sysKey := encodeValueIndexKey(vid)
				sysVal, err := sysTr.Get(sysKey)
				if err != nil {
					continue
				}
				if len(sysVal) != page.ValuePtrSize {
					continue
				}
				currentPtr := page.DecodeValuePtr(sysVal)
				if currentPtr.FileID != op.OldPtr.FileID || currentPtr.Offset != op.OldPtr.Offset || page.ValuePtrRecordLength(currentPtr) != page.ValuePtrRecordLength(op.OldPtr) {
					continue
				}

				// Queue update for System Tree
				var newPtrBuf [page.ValuePtrSize]byte
				op.NewPtr.Encode(newPtrBuf[:])
				sysOps = append(sysOps, batch.Entry{
					Type:  batch.OpPut,
					Key:   append([]byte(nil), sysKey...),
					Value: append([]byte(nil), newPtrBuf[:]...),
				})

				if slabWritesByFile == nil {
					slabWritesByFile = make(map[uint32]int64, 4)
				}
				slabWritesByFile[op.NewPtr.FileID] += int64(page.ValuePtrRecordLength(op.NewPtr))

				if metrics.SlabDeadBytesByFile == nil {
					metrics.SlabDeadBytesByFile = make(map[uint32]int64, 4)
				}
				metrics.SlabDeadBytesByFile[op.OldPtr.FileID] += int64(page.ValuePtrRecordLength(op.OldPtr))
				metrics.SlabDeadBytes += int(page.ValuePtrRecordLength(op.OldPtr))
				continue
			}

			// Legacy Pointer Path
			if entry.Flags&node.FlagPointer == 0 {
				continue
			}
			if entry.ValuePtr.FileID != op.OldPtr.FileID || entry.ValuePtr.Offset != op.OldPtr.Offset || page.ValuePtrRecordLength(entry.ValuePtr) != page.ValuePtrRecordLength(op.OldPtr) {
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
			slabWritesByFile[op.NewPtr.FileID] += int64(page.ValuePtrRecordLength(op.NewPtr))
		}

		opsMap := b.Ops()
		if len(opsMap) == 0 && len(sysOps) == 0 {
			closeBatch()
			db.writeMu.Unlock()
			continue
		}

		var newRoot uint64
		var retired []uint64
		var err error

		if len(opsMap) > 0 {
			var r uint64
			r, retired, metrics, err = idx.zipper.Apply(rootID, b)
			if err != nil {
				closeBatch()
				db.writeMu.Unlock()
				return err
			}
			newRoot = r
		} else {
			newRoot = rootID
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
		if err := db.finalizeCommit(newRoot, sysRoot, retired, false, sysOps, metrics, 0); err != nil {
			closeBatch()
			db.writeMu.Unlock()
			return err
		}
		if db.vacuum.Active() {
			db.vacuum.RecordOps(opsMap)
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
	newRoot, err := bulk.BuildWithOptions(iter, alloc, idx.pager, bulk.BuildOptions{
		LeafPrefixCompression: db.leafPrefixCompression,
	})
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
	return db.finalizeCommit(newRoot, sysRoot, retired, true, nil, adaptive.Metrics{}, 0)
}

type pagerAllocator struct {
	p *pager.Pager
}

func (a *pagerAllocator) Alloc(hint uint64) (uint64, error) {
	return a.p.Alloc(1)
}
