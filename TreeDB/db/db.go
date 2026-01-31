package db

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
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
	ValueLogSet      *valuelog.Set
}

type DB struct {
	valueLogManager *valuelog.Manager
	lock            *lockfile.Lock
	adaptive        *adaptive.Controller
	pruner          pruneWorker

	// idx is the current index generation (pager + MVCC lifecycle state).
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

	keepRecent                uint64
	policy                    WritePolicy
	leafFillTargetPPM         uint32
	internalFillTargetPPM     uint32
	leafPrefixCompression     bool
	indexColumnarLeaves       bool
	indexInternalBaseDelta    bool
	piggybackCompaction       bool
	maintenanceOpsPerCoalesce int

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

const (
	defaultChunkSize                 = 4 * 1024 * 1024
	defaultMaintenanceOpsPerCoalesce = 400_000
)

// DurabilityMode configures cached-mode durability semantics.
//
// These modes are explicit and intentionally replace the previous boolean
// combination of DisableWAL + RelaxedSync + AllowUnsafe.
type DurabilityMode uint8

const (
	// DurabilityDurable enables WAL (journal) and uses fsync for sync operations.
	DurabilityDurable DurabilityMode = iota
	// DurabilityWALOnRelaxed keeps WAL enabled but disables fsync (crash-consistent).
	DurabilityWALOnRelaxed
	// DurabilityWALOffRelaxed disables WAL and fsync (unsafe; recent writes may be lost).
	DurabilityWALOffRelaxed
)

// IntegrityMode configures value-log read integrity checks.
//
// It intentionally replaces the previous DisableReadChecksum boolean.
type IntegrityMode uint8

const (
	// IntegrityVerify enables checksum verification on value-log reads.
	IntegrityVerify IntegrityMode = iota
	// IntegritySkipChecksums disables checksum verification on value-log reads (unsafe).
	IntegritySkipChecksums
)

// ValueLogOptions configures value-log pointer behavior and optional compression/dict tuning.
type ValueLogOptions struct {
	// PointerThreshold controls when value-log pointers are used.
	// Values <= 0 use the default inline threshold (256 bytes).
	PointerThreshold int
	// ForcePointers stores all values out-of-line in the value log (no inline values).
	ForcePointers bool

	// ReadIntegrity configures checksum verification on value-log reads.
	ReadIntegrity IntegrityMode

	// MaxRetainedBytes emits a warning when retained value-log bytes exceed this
	// threshold (0 disables warnings). Cached mode only.
	MaxRetainedBytes int64
	// MaxRetainedBytesHard disables value-log pointers for new large values once
	// retained bytes exceed this threshold (0 disables the cap).
	MaxRetainedBytesHard int64

	// DictLookup provides dictionary bytes for value-log decoding.
	DictLookup valuelog.DictLookup

	// DictTrain configures background dictionary training for value-log frame
	// compression in cached mode.
	DictTrain compression.TrainConfig
	// DictAdaptiveRatio enables best-effort adaptive disable/pause of value-log
	// dictionary compression when payload compression ratios degrade (0 disables).
	DictAdaptiveRatio float64
	// DictMetricsWindowBytes controls the rolling window size for ratio tracking (0=default).
	DictMetricsWindowBytes int
	// DictMetricsMinRecords controls how many records must be observed in a window
	// before adaptive pause triggers (0=default).
	DictMetricsMinRecords int
	// DictMetricsPauseBytes controls how long to pause dict compression after a degraded
	// window is detected (0=default).
	DictMetricsPauseBytes int
	// DictMinPayloadSavingsRatio rejects newly trained dictionaries whose payload
	// ratio does not improve by at least this fraction (0 uses default ~0.5%).
	DictMinPayloadSavingsRatio float64

	// CompressionAutotune configures the wall-time value-log compression autotuner.
	CompressionAutotune valuelog.AutotuneOptions
}

type Options struct {
	Dir string
	// ReadOnly opens the database without acquiring an exclusive lock and without
	// modifying on-disk state (no recovery truncation, no WAL replay, no background
	// maintenance). Only read operations are supported.
	ReadOnly   bool
	ChunkSize  int64  // Default 4MiB
	KeepRecent uint64 // Default 10000
	// PagerSyncConcurrency controls how many goroutines may msync dirty chunks
	// in parallel during Sync. Values <= 0 use the default (1).
	PagerSyncConcurrency int

	// Durability configures cached-mode durability semantics.
	//
	// The default (zero) is DurabilityDurable.
	Durability DurabilityMode
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
	// MaintenanceOpsPerCoalesce controls the maintenance budget during zipper
	// merge. It bounds coalesce work to roughly len(ops)/K operations per batch.
	// 0 uses the default; negative disables the budget (full maintenance).
	MaintenanceOpsPerCoalesce int
	// LeafPrefixCompression enables prefix-compressed leaf nodes for new pages.
	LeafPrefixCompression bool
	// IndexColumnarLeaves enables the experimental columnar leaf encoding for new pages.
	IndexColumnarLeaves bool
	// IndexInternalBaseDelta enables the experimental internal-node base-delta encoding.
	IndexInternalBaseDelta bool
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
	// FlushBuildMinEntries gates the parallel build path by total entries.
	// Values <= 0 use a default of 16k.
	FlushBuildMinEntries int
	// FlushBuildMinUnits gates the parallel build path by number of queued units.
	// Values <= 0 use a default of 2.
	FlushBuildMinUnits int
	// FlushBuildChunkCap controls the maximum entries per build chunk.
	// A value of 0 enables adaptive chunk sizing, values < 0 use the fixed default of 8192,
	// and values > 0 set an explicit cap.
	FlushBuildChunkCap int
	// FlushBuildChunkTargetBytes controls adaptive chunk sizing (bytes per chunk).
	// Values <= 0 use a default of 2MiB.
	FlushBuildChunkTargetBytes int
	// FlushBuildChunkMinBytes clamps adaptive chunk sizes (minimum bytes).
	// Values <= 0 use a default of 1MiB.
	FlushBuildChunkMinBytes int
	// FlushBuildChunkMaxBytes clamps adaptive chunk sizes (maximum bytes).
	// Values <= 0 use a default of 4MiB.
	FlushBuildChunkMaxBytes int
	// FlushBuildPrefetchUnits controls how many memtables to start building ahead
	// of the consumer. Values <= 0 use FlushBuildConcurrency.
	FlushBuildPrefetchUnits int

	// JournalLanes controls the number of active commit/value log lanes (0=default).
	// Max supported lanes is 255; value-log segment sequence per lane is capped at 8,388,607.
	JournalLanes int
	// WALMaxSegmentBytes caps the size of a single WAL segment payload.
	// 0 uses the default limit.
	WALMaxSegmentBytes int64
	// JournalCompression enables best-effort zstd compression for cached-mode
	// journal/commitlog segments (metadata only).
	//
	// The redo log will only keep compressed bytes when they are smaller than the
	// raw payload, so compression never causes size amplification.
	JournalCompression bool

	// ValueLog configures value-log pointer behavior and read integrity.
	ValueLog ValueLogOptions

	// NotifyError is an optional hook for background maintenance failures.
	NotifyError func(error)

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
	// BackgroundIndexVacuumInterval enables periodic online index vacuum passes.
	// `0` uses a default; `<0` disables.
	BackgroundIndexVacuumInterval time.Duration
	// BackgroundIndexVacuumSpanRatioPPM sets the span ratio threshold that
	// triggers a vacuum pass (0 uses a default).
	BackgroundIndexVacuumSpanRatioPPM uint32
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
		snap.tree.Reset(idx.pager, valueReader{vlogs: state.ValueLogSet}, state.RootPageID)
	}
	snap.registryID = id
	return snap
}

// Close releases the snapshot.
func (s *Snapshot) Close() error {
	var err error
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
		opts.ChunkSize = defaultChunkSize
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
	if opts.MaintenanceOpsPerCoalesce == 0 {
		opts.MaintenanceOpsPerCoalesce = defaultMaintenanceOpsPerCoalesce
	} else if opts.MaintenanceOpsPerCoalesce < 0 {
		opts.MaintenanceOpsPerCoalesce = 0
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

	if err := validateOptions(opts); err != nil {
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

func validateOptions(opts Options) error {
	if opts.ReadOnly {
		// Read-only opens never mutate on-disk state, so "unsafe" write options do
		// not apply.
		return nil
	}
	switch opts.Durability {
	case DurabilityDurable, DurabilityWALOnRelaxed, DurabilityWALOffRelaxed:
	default:
		return fmt.Errorf("treedb: invalid durability mode %d", opts.Durability)
	}
	switch opts.ValueLog.ReadIntegrity {
	case IntegrityVerify, IntegritySkipChecksums:
	default:
		return fmt.Errorf("treedb: invalid value-log integrity mode %d", opts.ValueLog.ReadIntegrity)
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
	if opts.PagerSyncConcurrency > 0 {
		p.SetSyncConcurrency(opts.PagerSyncConcurrency)
	}
	p.SetVerifyOnRead(opts.VerifyOnRead)

	valueLogDir := filepath.Join(opts.Dir, "wal")
	vm, err := valuelog.NewManager(valueLogDir)
	if err != nil {
		p.Close()
		return nil, err
	}
	vm.SetDisableReadChecksum(opts.ValueLog.ReadIntegrity == IntegritySkipChecksums)
	vm.SetDictLookup(opts.ValueLog.DictLookup)

	alloc := freelist.New(p, 0)
	alloc.SetPreferAppend(opts.PreferAppendAlloc)
	alloc.SetFreelistRegion(opts.FreelistRegionPages, opts.FreelistRegionRadius)

	z := zipper.New(p, alloc)

	gen := newIndexGen(1, p, alloc, z)

	adaptiveCtrl := adaptive.New()
	inlineThreshold := page.DefaultInlineThreshold
	if opts.ValueLog.ForcePointers {
		inlineThreshold = 0
		adaptiveCtrl = nil
	}

	db := &DB{
		valueLogManager:           vm,
		lock:                      lock,
		adaptive:                  adaptiveCtrl,
		keepRecent:                opts.KeepRecent,
		leafFillTargetPPM:         opts.LeafFillTargetPPM,
		internalFillTargetPPM:     opts.InternalFillTargetPPM,
		leafPrefixCompression:     opts.LeafPrefixCompression,
		indexColumnarLeaves:       opts.IndexColumnarLeaves,
		indexInternalBaseDelta:    opts.IndexInternalBaseDelta,
		piggybackCompaction:       !opts.DisablePiggybackCompaction,
		maintenanceOpsPerCoalesce: opts.MaintenanceOpsPerCoalesce,
		dir:                       opts.Dir,
		chunkSize:                 opts.ChunkSize,
		preferAppendAlloc:         opts.PreferAppendAlloc,
		freelistRegionPages:       opts.FreelistRegionPages,
		freelistRegionRadius:      opts.FreelistRegionRadius,
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
	gen.zipper.SetIndexColumnarLeaves(opts.IndexColumnarLeaves)
	gen.zipper.SetIndexInternalBaseDelta(opts.IndexInternalBaseDelta)
	gen.zipper.SetMaintenanceOpsPerCoalesce(opts.MaintenanceOpsPerCoalesce)

	if err := db.recover(); err != nil {
		db.Close()
		return nil, err
	}

	if opts.Durability != DurabilityWALOffRelaxed {
		segments, err := listWALSegments(opts.Dir)
		if err != nil {
			db.Close()
			return nil, err
		}
		if err := replayWALIntoBackend(db, segments, opts.WALMaxSegmentBytes, opts.ValueLog.DictLookup); err != nil {
			db.Close()
			return nil, err
		}
	}

	// Initialize State after recovery so log cleanup can proceed without pinning.
	initialState := &DBState{
		CommitSeq:        db.meta.CommitSeq,
		RootPageID:       db.meta.UserRootPageID,
		SystemRootPageID: db.meta.SystemRootPageID,
		ValueLogSet:      vm.CurrentSet(),
	}
	db.state.Store(initialState)

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
	vm := db.valueLogManager
	db.valueLogManager = nil
	lock := db.lock
	db.lock = nil
	db.mu.Unlock()

	var errs []error
	if err := db.persistFreelistHeadOnClose(); err != nil {
		errs = append(errs, err)
	}
	if err := db.closeAllIndexes(); err != nil {
		errs = append(errs, err)
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
		b := node.NewBuilderWithOptions(data, page.PageTypeLeaf, node.BuilderOptions{
			LeafPrefixCompression: db.leafPrefixCompression,
			LeafColumnar:          db.indexColumnarLeaves,
			InternalBaseDelta:     db.indexInternalBaseDelta,
		})
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
		bSys := node.NewBuilderWithOptions(dataSys, page.PageTypeLeaf, node.BuilderOptions{
			LeafPrefixCompression: db.leafPrefixCompression,
			LeafColumnar:          db.indexColumnarLeaves,
			InternalBaseDelta:     db.indexInternalBaseDelta,
		})
		bSys.SetPageID(sysRootID)
		bSys.Finish()

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
	if len(candidates) == 2 && candidates[0].meta.CommitSeq < candidates[1].meta.CommitSeq {
		candidates[0], candidates[1] = candidates[1], candidates[0]
	}

	var chosen *metaCandidate
	for i := range candidates {
		c := &candidates[i]

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
		return errors.New("no valid meta page")
	}

	db.meta = chosen.meta
	db.metaPageID = chosen.id

	if chosen.meta.TotalPages > 0 {
		p.SetPageCount(chosen.meta.TotalPages)
	}

	// Update Allocator Head
	idx.allocator.SetHead(chosen.meta.FreelistHeadID)

	return nil
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

func (db *DB) persistFreelistHeadOnClose() error {
	if db.readOnly {
		return nil
	}
	idx := db.idx.Load()
	if idx == nil || idx.pager == nil || idx.allocator == nil {
		return nil
	}
	head := idx.allocator.Head()
	totalPages := idx.pager.PageCount()

	db.mu.Lock()
	nextMeta := db.meta
	if nextMeta.FreelistHeadID == head && nextMeta.TotalPages == totalPages {
		db.mu.Unlock()
		return nil
	}
	nextMeta.FreelistHeadID = head
	nextMeta.TotalPages = totalPages
	db.mu.Unlock()

	// Write both meta pages to avoid commit sequence tie-break ambiguity.
	if err := db.writeMeta(MetaPage0ID, nextMeta); err != nil {
		return err
	}
	if err := db.writeMeta(MetaPage1ID, nextMeta); err != nil {
		return err
	}

	db.mu.Lock()
	db.meta = nextMeta
	db.mu.Unlock()
	return nil
}

// finalizeCommit handles durability and state updates with minimal lock contention.
func (db *DB) finalizeCommit(newRootID uint64, sysRootID uint64, retired []uint64, sync bool, metrics adaptive.Metrics) error {
	if db.readOnly {
		return ErrReadOnly
	}
	idx := db.idx.Load()
	if idx == nil {
		return errors.New("missing index")
	}
	debugTiming := commitTimingEnabled()
	var (
		start    time.Time
		durSync1 time.Duration
		durMeta  time.Duration
		durSync2 time.Duration
		durPrune time.Duration
	)
	if debugTiming {
		start = time.Now()
	}

	// 1. Sync Data (Index Pages) - No DB Lock
	if sync {
		t0 := time.Now()
		if err := idx.pager.Sync(); err != nil {
			return err
		}
		if debugTiming {
			durSync1 = time.Since(t0)
		}
	}

	// 2. Prepare Meta - Short Lock
	db.mu.Lock()
	nextMeta := db.meta
	nextMeta.CommitSeq++
	nextMeta.UserRootPageID = newRootID
	nextMeta.SystemRootPageID = sysRootID
	nextMeta.FreelistHeadID = idx.allocator.Head()
	nextMeta.TotalPages = idx.pager.PageCount()

	targetPageID := uint64(0)
	if db.metaPageID == 0 {
		targetPageID = 1
	}
	db.mu.Unlock()

	// 3. Write Meta - No DB Lock
	t0 := time.Now()
	if err := db.writeMeta(targetPageID, nextMeta); err != nil {
		return err
	}
	if debugTiming {
		durMeta = time.Since(t0)
	}

	// 4. Sync Meta - No DB Lock
	if sync {
		t1 := time.Now()
		if err := idx.pager.Sync(); err != nil {
			return err
		}
		if debugTiming {
			durSync2 = time.Since(t1)
		}
	}

	// 5. Update State (Visible) - Short Lock
	db.mu.Lock()
	defer db.mu.Unlock()

	db.meta = nextMeta
	db.metaPageID = targetPageID

	// Add retired pages to Graveyard
	idx.graveyard.Add(nextMeta.CommitSeq, retired)

	// Update State
	oldState := db.state.Load()
	newState := &DBState{
		CommitSeq:        nextMeta.CommitSeq,
		RootPageID:       nextMeta.UserRootPageID,
		SystemRootPageID: nextMeta.SystemRootPageID,
		ValueLogSet:      db.valueLogManager.CurrentSet(),
	}
	db.state.Store(newState)

	// Prune asynchronously to keep commit latency stable under churn.
	// IMPORTANT: kick/Prune only after publishing the new state so the pruner sees
	// the updated commit sequence and can reclaim pages promptly.
	if db.pruner.Enabled() {
		// When KeepRecent is extremely small (used in churn/bloat tests),
		// opportunistically prune on-commit so freed pages become available for
		// reuse immediately. This helps avoid file growth under rapid churn.
		if db.keepRecent <= 1 && !db.preferAppendAlloc {
			tp := time.Now()
			_, err := db.pruneSome(make(chan struct{}), db.pruner.maxPages, db.pruner.maxDuration*4)
			if err != nil {
				db.reportError(err)
			}
			if debugTiming {
				durPrune = time.Since(tp)
			}
		} else {
			db.pruner.Kick()
		}
	} else {
		tp := time.Now()
		db.Prune()
		if debugTiming {
			durPrune = time.Since(tp)
		}
	}

	if oldState != nil {
		_ = db.valueLogManager.Release(oldState.ValueLogSet)
	}

	if db.adaptive != nil {
		db.adaptive.RecordCommit(metrics)
	}
	if debugTiming {
		commitTimingPrintf(
			"treedb: commit_timing sync=%t sync1=%s meta=%s sync2=%s prune=%s total=%s\n",
			sync,
			durSync1,
			durMeta,
			durSync2,
			durPrune,
			time.Since(start),
		)
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

	return db.finalizeCommit(newRootID, sysRoot, nil, true, adaptive.Metrics{})
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
	// Prefer the published DBState commit sequence because it is updated on every
	// successful commit and accessed lock-free elsewhere (e.g. prune worker).
	// Falling back to meta avoids a nil panic during early open/recover.
	current := uint64(0)
	if st := db.state.Load(); st != nil {
		current = st.CommitSeq
	} else {
		// Do not take db.mu here: Prune() is called from finalizeCommit while
		// holding db.mu exclusively, including during WAL replay before state is
		// initialized. Reading meta without the lock is best-effort and avoids a
		// self-deadlock.
		current = db.meta.CommitSeq
	}

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
	entry, err := s.tree.GetEntry(key)
	if err != nil {
		return nil, err
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return nil, tree.ErrKeyNotFound
	}

	if entry.Flags&node.FlagPointer != 0 {
		vr := ValueReaderForState(s.state)
		return vr.ReadUnsafe(entry.ValuePtr)
	}

	return entry.Value, nil
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

// RefreshValueLogSet publishes a new DBState with the current value-log set
// (excluding zombies) without creating a new commit.
func (db *DB) RefreshValueLogSet() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.valueLogManager == nil {
		return nil
	}
	oldState := db.state.Load()
	if oldState == nil {
		return nil
	}

	newState := &DBState{
		CommitSeq:        oldState.CommitSeq,
		RootPageID:       oldState.RootPageID,
		SystemRootPageID: oldState.SystemRootPageID,
		ValueLogSet:      db.valueLogManager.CurrentSet(),
	}
	db.state.Store(newState)

	if oldState.ValueLogSet != nil {
		return db.valueLogManager.Release(oldState.ValueLogSet)
	}
	return nil
}

// MarkValueLogZombie marks a value-log segment as zombie so it can be removed
// once all snapshots release it.
func (db *DB) MarkValueLogZombie(id uint32) error {
	if db == nil || db.valueLogManager == nil {
		return fmt.Errorf("value log manager unavailable")
	}
	return db.valueLogManager.MarkZombie(id)
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
	tr := tree.New(idx.pager, valueReader{vlogs: state.ValueLogSet}, state.RootPageID)
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
		LeafColumnar:          db.indexColumnarLeaves,
		InternalBaseDelta:     db.indexInternalBaseDelta,
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
	return db.finalizeCommit(newRoot, sysRoot, retired, true, adaptive.Metrics{})
}

type pagerAllocator struct {
	p *pager.Pager
}

func (a *pagerAllocator) Alloc(hint uint64) (uint64, error) {
	return a.p.Alloc(1)
}
