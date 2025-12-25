package main

import (
	"flag"
	"math"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbcaching "github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/kvstore"
	treedbadapter "github.com/snissn/gomap/kvstore/adapters/treedb"
)

var (
	treedbFlushThreshold           = flag.Int64("treedb-flush-threshold", 64*1024*1024, "TreeDB (cached): flush threshold in bytes")
	treedbKeepRecent               = flag.Uint64("treedb-keep-recent", 0, "TreeDB: KeepRecent commit versions to retain before page reuse (0=default; cached defaults to 1)")
	treedbMaxQueuedMems            = flag.Int("treedb-max-queued-memtables", 0, "TreeDB (cached): max queued immutable memtables before backpressure flush (0=default, <0=disable)")
	treedbSlowdownBacklogSeconds   = flag.Float64("treedb-slowdown-backlog-seconds", 1, "TreeDB (cached): begin writer backpressure when queued flush backlog exceeds this many seconds (0=disabled)")
	treedbStopBacklogSeconds       = flag.Float64("treedb-stop-backlog-seconds", 2, "TreeDB (cached): block writers when queued flush backlog exceeds this many seconds (0=disabled)")
	treedbMaxBacklogBytes          = flag.Int64("treedb-max-backlog-bytes", 2<<30, "TreeDB (cached): absolute cap on queued flush backlog bytes (0=disabled)")
	treedbWriterFlushMaxMems       = flag.Int("treedb-writer-flush-max-memtables", 0, "TreeDB (cached): max memtables a writer will help flush per op when backpressure triggers (0=default)")
	treedbWriterFlushMaxMs         = flag.Int("treedb-writer-flush-max-ms", 0, "TreeDB (cached): max milliseconds a writer will help flush per op when backpressure triggers (0=disabled)")
	treedbPreferAppendAlloc        = flag.Bool("treedb-prefer-append-alloc", false, "TreeDB: allocate new index pages by appending instead of freelist reuse (improves scan locality under churn; grows index.db)")
	treedbLeafFillPPM              = flag.Int("treedb-leaf-fill-ppm", 0, "TreeDB: leaf fill target (ppm). Lower reduces split churn at cost of more pages (0=default=1_000_000)")
	treedbInternalFillPPM          = flag.Int("treedb-internal-fill-ppm", 0, "TreeDB: internal fill target (ppm). Lower reduces split churn at cost of more pages (0=default=1_000_000)")
	treedbIterDebug                = flag.Bool("treedb-iter-debug", false, "TreeDB: print prefix_scan iterator build/iterate timing and debug stats (queueLen, sourcesUsed)")
	treedbIterDebugLimit           = flag.Int("treedb-iter-debug-limit", 20, "TreeDB: maximum prefix_scan queries to print per DB run when -treedb-iter-debug is set")
	treedbCompactBeforeScans       = flag.Bool("treedb-compact-before-scans", false, "TreeDB: run slab compaction before scan tests (typically used with -settle-before-scans)")
	treedbCompactDeadRatio         = flag.Float64("treedb-compact-dead-ratio", 0.50, "TreeDB: slab compaction candidate dead ratio threshold")
	treedbCompactMinBytes          = flag.Uint64("treedb-compact-min-bytes", 1*1024*1024, "TreeDB: minimum slab total bytes to consider for compaction")
	treedbCompactMaxSlabs          = flag.Int("treedb-compact-max-slabs", 1, "TreeDB: maximum slabs to compact per run (0=unlimited)")
	treedbCompactMicroBatch        = flag.Int("treedb-compact-microbatch", 256, "TreeDB: compaction apply micro-batch size (keys per commit)")
	treedbCompactRotateBeforeWrite = flag.Bool("treedb-compact-rotate-before-write", false, "TreeDB: rotate to a fresh active slab before copying live records")
	treedbCompactCopyBps           = flag.Int64("treedb-compact-copy-bps", 0, "TreeDB: compaction copy throttling (bytes/sec), 0=disabled")
	treedbCompactCopyBurst         = flag.Int64("treedb-compact-copy-burst", 0, "TreeDB: compaction copy throttling burst (bytes), 0=default")
	treedbVacuumBeforeScans        = flag.Bool("treedb-vacuum-before-scans", false, "TreeDB: vacuum (rebuild) the user index before scan tests (typically used with -settle-before-scans)")

	treedbDisableWAL           = flag.Bool("treedb-disable-wal", false, "TreeDB: disable WAL (unsafe)")
	treedbRelaxedSync          = flag.Bool("treedb-relaxed-sync", false, "TreeDB: relaxed sync (unsafe)")
	treedbDisableReadChecksum  = flag.Bool("treedb-disable-read-checksum", false, "TreeDB: disable read checksum (unsafe)")
	treedbBgCompactionInterval = flag.Duration("treedb-bg-compaction-interval", 0, "TreeDB: background compaction interval (0=disabled)")
	treedbDisablePiggyback     = flag.Bool("treedb-disable-piggyback-compaction", false, "TreeDB: disable piggyback compaction")
	treedbBgVacuumInterval     = flag.Duration("treedb-bg-vacuum-interval", 0, "TreeDB: background index vacuum interval (0=disabled)")
	treedbBgVacuumSpanPPM      = flag.Uint64("treedb-bg-vacuum-span-ppm", 0, "TreeDB: background index vacuum span ratio threshold (ppm), 0=default")
)

func init() {
	RegisterDB("treedb", NewTreeDB)
	RegisterAlias("treedbcached", "treedb")
	RegisterDB("treedbbackend", NewTreeDBBackend)
	RegisterAlias("treedbraw", "treedbbackend")
}

func clampPPM(v int) int {
	if v < 0 {
		return 0
	}
	if v > 1_000_000 {
		return 1_000_000
	}
	return v
}

func clampUint32(v uint64) uint32 {
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}

func NewTreeDB(dir string) (kvstore.DB, error) {
	treedbcaching.SetIteratorDebug(*treedbIterDebug)
	opts := treedb.Options{
		Dir:                               dir,
		ChunkSize:                         64 * 1024 * 1024,
		KeepRecent:                        *treedbKeepRecent,
		PreferAppendAlloc:                 *treedbPreferAppendAlloc,
		LeafFillTargetPPM:                 uint32(clampPPM(*treedbLeafFillPPM)),
		InternalFillTargetPPM:             uint32(clampPPM(*treedbInternalFillPPM)),
		FlushThreshold:                    *treedbFlushThreshold,
		MaxQueuedMemtables:                *treedbMaxQueuedMems,
		SlowdownBacklogSeconds:            *treedbSlowdownBacklogSeconds,
		StopBacklogSeconds:                *treedbStopBacklogSeconds,
		MaxBacklogBytes:                   *treedbMaxBacklogBytes,
		WriterFlushMaxMemtables:           *treedbWriterFlushMaxMems,
		DisableWAL:                        *treedbDisableWAL,
		RelaxedSync:                       *treedbRelaxedSync,
		DisableReadChecksum:               *treedbDisableReadChecksum,
		BackgroundCompactionInterval:      *treedbBgCompactionInterval,
		BackgroundIndexVacuumInterval:     *treedbBgVacuumInterval,
		BackgroundIndexVacuumSpanRatioPPM: clampUint32(*treedbBgVacuumSpanPPM),
		DisablePiggybackCompaction:        *treedbDisablePiggyback,
	}
	if *treedbWriterFlushMaxMs > 0 {
		opts.WriterFlushMaxDuration = time.Duration(*treedbWriterFlushMaxMs) * time.Millisecond
	}
	db, err := treedb.Open(opts)
	if err != nil {
		return nil, err
	}
	return treedbadapter.WrapNamed(db, "TreeDB"), nil
}

func NewTreeDBBackend(dir string) (kvstore.DB, error) {
	opts := treedb.Options{
		Dir:                               dir,
		ChunkSize:                         64 * 1024 * 1024,
		KeepRecent:                        *treedbKeepRecent,
		PreferAppendAlloc:                 *treedbPreferAppendAlloc,
		LeafFillTargetPPM:                 uint32(clampPPM(*treedbLeafFillPPM)),
		InternalFillTargetPPM:             uint32(clampPPM(*treedbInternalFillPPM)),
		BackgroundIndexVacuumInterval:     *treedbBgVacuumInterval,
		BackgroundIndexVacuumSpanRatioPPM: clampUint32(*treedbBgVacuumSpanPPM),
	}
	db, err := treedb.OpenBackend(opts)
	if err != nil {
		return nil, err
	}
	return treedbadapter.WrapNamed(db, "TreeDBBackend"), nil
}
