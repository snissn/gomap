package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	hashdb "github.com/snissn/gomap/HashDB"
	btreeonhashdb "github.com/snissn/gomap/HashDB/BTreeOnHashDB"
	treedb "github.com/snissn/gomap/TreeDB"
	treedbcaching "github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/compaction"
	"github.com/snissn/gomap/kvstore"
	btreeadapter "github.com/snissn/gomap/kvstore/adapters/btreeonhashdb"
	hashdbadapter "github.com/snissn/gomap/kvstore/adapters/hashdb"
	treedbadapter "github.com/snissn/gomap/kvstore/adapters/treedb"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// --- Engine Openers (kvstore) ---

var (
	hashdbLockControls       = flag.Bool("hashdb-lock-controls", true, "HashDB: best-effort lock (mlock/VirtualLock) the SwissHash control bytes")
	hashdbLockControlsStrict = flag.Bool("hashdb-lock-controls-strict", false, "HashDB: require control-bytes locking to succeed (may require memlock/ulimit changes)")
	hashdbAdviseKeysWillNeed = flag.Bool("hashdb-advise-keys-willneed", true, "HashDB: madvise WILLNEED for hash key map (best-effort)")
	hashdbAdviseKeysRandom   = flag.Bool("hashdb-advise-keys-random", true, "HashDB: madvise RANDOM for hash key map (best-effort)")
)

func openHashDBForBench(dir string) (*hashdb.HashDB, error) {
	opts := hashdb.HashDBOptions{
		IndexMemoryPolicySet: true,
		IndexMemoryPolicy: hashdb.IndexMemoryPolicy{
			LockControls:       *hashdbLockControls,
			LockControlsStrict: *hashdbLockControlsStrict,
			AdviseKeysWillNeed: *hashdbAdviseKeysWillNeed,
			AdviseKeysRandom:   *hashdbAdviseKeysRandom,
		},
	}
	return hashdb.OpenWithOptions(dir, opts)
}

func NewHashDB(dir string) (kvstore.DB, error) {
	m, err := openHashDBForBench(dir)
	if err != nil {
		return nil, err
	}
	return hashdbadapter.WrapNamed(m, "HashDB"), nil
}

func NewBTree(dir string) (kvstore.DB, error) {
	m, err := openHashDBForBench(dir)
	if err != nil {
		return nil, err
	}
	t, err := btreeonhashdb.NewTreeOnHashDB(m, "bench", &btreeonhashdb.Options{CacheSize: 4096})
	if err != nil {
		return nil, err
	}
	return btreeadapter.WrapNamed(m, t, "BTree"), nil
}

func NewTreeDB(dir string) (kvstore.DB, error) {
	treedbcaching.SetIteratorDebug(*treedbIterDebug)
	opts := treedb.Options{
		Dir:                     dir,
		ChunkSize:               64 * 1024 * 1024,
		KeepRecent:              *treedbKeepRecent,
		PreferAppendAlloc:       *treedbPreferAppendAlloc,
		LeafFillTargetPPM:       uint32(clampPPM(*treedbLeafFillPPM)),
		InternalFillTargetPPM:   uint32(clampPPM(*treedbInternalFillPPM)),
		FlushThreshold:          *treedbFlushThreshold,
		MaxQueuedMemtables:      *treedbMaxQueuedMems,
		SlowdownBacklogSeconds:  *treedbSlowdownBacklogSeconds,
		StopBacklogSeconds:      *treedbStopBacklogSeconds,
		MaxBacklogBytes:         *treedbMaxBacklogBytes,
		WriterFlushMaxMemtables: *treedbWriterFlushMaxMems,
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
		Dir:                   dir,
		ChunkSize:             64 * 1024 * 1024,
		KeepRecent:            *treedbKeepRecent,
		PreferAppendAlloc:     *treedbPreferAppendAlloc,
		LeafFillTargetPPM:     uint32(clampPPM(*treedbLeafFillPPM)),
		InternalFillTargetPPM: uint32(clampPPM(*treedbInternalFillPPM)),
	}
	db, err := treedb.OpenBackend(opts)
	if err != nil {
		return nil, err
	}
	return treedbadapter.WrapNamed(db, "TreeDBBackend"), nil
}

// 5. Badger Wrapper
type BadgerBatch struct {
	wb *badger.WriteBatch
}

func (b *BadgerBatch) Set(key, value []byte) error { return b.wb.Set(key, value) }
func (b *BadgerBatch) Delete(key []byte) error     { return b.wb.Delete(key) }
func (b *BadgerBatch) Commit() error               { return b.wb.Flush() }
func (b *BadgerBatch) CommitSync() error           { return b.wb.Flush() }
func (b *BadgerBatch) Close() error                { b.wb.Cancel(); return nil }

type BadgerIterator struct {
	txn     *badger.Txn
	it      *badger.Iterator
	end     []byte
	keyBuf  []byte
	valBuf  []byte
	lastErr error
	reverse bool
}

func (i *BadgerIterator) Valid() bool {
	if !i.it.Valid() {
		return false
	}
	if i.end == nil {
		return true
	}
	if i.reverse {
		return bytes.Compare(i.it.Item().Key(), i.end) >= 0
	}
	return bytes.Compare(i.it.Item().Key(), i.end) < 0
}
func (i *BadgerIterator) Next() { i.it.Next() }
func (i *BadgerIterator) Key() []byte {
	i.keyBuf = i.it.Item().KeyCopy(i.keyBuf[:0])
	return i.keyBuf
}
func (i *BadgerIterator) Value() []byte {
	var err error
	i.valBuf, err = i.it.Item().ValueCopy(i.valBuf[:0])
	if err != nil && i.lastErr == nil {
		i.lastErr = err
	}
	return i.valBuf
}

func (i *BadgerIterator) KeyCopy(dst []byte) []byte   { return append(dst, i.Key()...) }
func (i *BadgerIterator) ValueCopy(dst []byte) []byte { return append(dst, i.Value()...) }

func (i *BadgerIterator) Close() error {
	i.it.Close()
	i.txn.Discard()
	return nil
}
func (i *BadgerIterator) Error() error { return i.lastErr }

type BadgerWrapper struct {
	db *badger.DB
}

func NewBadger(dir string) (kvstore.DB, error) {
	opts := badger.DefaultOptions(dir).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &BadgerWrapper{db: db}, nil
}

func (b *BadgerWrapper) Name() string { return "Badger" }
func (b *BadgerWrapper) Set(k, v []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(k, v)
	})
}
func (b *BadgerWrapper) Get(k []byte) ([]byte, error) {
	var out []byte
	err := b.db.View(func(txn *badger.Txn) error {
		it, err := txn.Get(k)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		out, err = it.ValueCopy(nil)
		return err
	})
	return out, err
}
func (b *BadgerWrapper) Delete(k []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(k)
	})
}
func (b *BadgerWrapper) Close() error { return b.db.Close() }

func (b *BadgerWrapper) Iterator(start, end []byte) (kvstore.Iterator, error) {
	txn := b.db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	it := txn.NewIterator(opts)
	if start != nil {
		it.Seek(start)
	} else {
		it.Rewind()
	}

	var endCopy []byte
	if end != nil {
		endCopy = append([]byte(nil), end...)
	}
	return &BadgerIterator{txn: txn, it: it, end: endCopy}, nil
}

func (b *BadgerWrapper) ReverseIterator(start, end []byte) (kvstore.Iterator, error) {
	txn := b.db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	opts.Reverse = true
	it := txn.NewIterator(opts)

	if end == nil {
		it.Rewind()
	} else {
		it.Seek(end)
		if it.Valid() {
			item := it.Item()
			if bytes.Compare(item.Key(), end) >= 0 {
				it.Next()
			}
		} else {
			it.Rewind()
		}
	}

	var startCopy []byte
	if start != nil {
		startCopy = append([]byte(nil), start...)
	}
	return &BadgerIterator{txn: txn, it: it, end: startCopy, reverse: true}, nil
}

func (b *BadgerWrapper) NewBatch() (kvstore.Batch, error) {
	return &BadgerBatch{wb: b.db.NewWriteBatch()}, nil
}

// 6. LevelDB Wrapper
type LevelDBBatch struct {
	batch *leveldb.Batch
	db    *leveldb.DB
}

func (b *LevelDBBatch) Set(key, value []byte) error {
	b.batch.Put(key, value)
	return nil
}
func (b *LevelDBBatch) Delete(key []byte) error {
	b.batch.Delete(key)
	return nil
}
func (b *LevelDBBatch) Commit() error {
	return b.db.Write(b.batch, nil)
}
func (b *LevelDBBatch) CommitSync() error {
	return b.db.Write(b.batch, nil)
}
func (b *LevelDBBatch) Close() error {
	b.batch.Reset()
	return nil
}

type LevelDBIterator struct {
	it      iterator.Iterator
	reverse bool
}

func (i *LevelDBIterator) Valid() bool { return i.it.Valid() }
func (i *LevelDBIterator) Next() {
	if i.reverse {
		i.it.Prev()
		return
	}
	i.it.Next()
}
func (i *LevelDBIterator) Key() []byte   { return i.it.Key() }
func (i *LevelDBIterator) Value() []byte { return i.it.Value() }
func (i *LevelDBIterator) KeyCopy(dst []byte) []byte {
	return append(dst, i.Key()...)
}
func (i *LevelDBIterator) ValueCopy(dst []byte) []byte {
	return append(dst, i.Value()...)
}
func (i *LevelDBIterator) Close() error { i.it.Release(); return nil }
func (i *LevelDBIterator) Error() error { return i.it.Error() }

type LevelDBWrapper struct {
	db *leveldb.DB
}

var (
	verifyRangePrefix = []byte{0x00, 'u', 'n', 'i', 'b', 'e', 'n', 'c', 'h', '-', 'v', 'e', 'r', 'i', 'f', 'y', 0x00}
	verifyRangeCount  = 32
)

func NewLevelDB(dir string) (kvstore.DB, error) {
	db, err := leveldb.OpenFile(dir, nil)
	if err != nil {
		return nil, err
	}
	return &LevelDBWrapper{db: db}, nil
}

func (l *LevelDBWrapper) Name() string                 { return "LevelDB" }
func (l *LevelDBWrapper) Set(k, v []byte) error        { return l.db.Put(k, v, nil) }
func (l *LevelDBWrapper) Get(k []byte) ([]byte, error) { return l.db.Get(k, nil) }
func (l *LevelDBWrapper) Delete(k []byte) error        { return l.db.Delete(k, nil) }
func (l *LevelDBWrapper) Close() error                 { return l.db.Close() }
func (l *LevelDBWrapper) Iterator(start, end []byte) (kvstore.Iterator, error) {
	var slice *util.Range
	if start != nil || end != nil {
		slice = &util.Range{Start: start, Limit: end}
	}
	it := l.db.NewIterator(slice, nil)
	it.First()
	return &LevelDBIterator{it: it, reverse: false}, nil
}
func (l *LevelDBWrapper) ReverseIterator(start, end []byte) (kvstore.Iterator, error) {
	var slice *util.Range
	if start != nil || end != nil {
		slice = &util.Range{Start: start, Limit: end}
	}
	it := l.db.NewIterator(slice, nil)
	if end == nil {
		it.Last()
	} else {
		it.Seek(end)
		if it.Valid() {
			if bytes.Compare(it.Key(), end) >= 0 {
				it.Prev()
			}
		} else {
			it.Last()
		}
	}
	return &LevelDBIterator{it: it, reverse: true}, nil
}
func (l *LevelDBWrapper) NewBatch() (kvstore.Batch, error) {
	return &LevelDBBatch{batch: new(leveldb.Batch), db: l.db}, nil
}

func verifyRangeIteration(db kvstore.DB, rs kvstore.RangeScanner, prefix []byte, n int) (retErr error) {
	keys := make([][]byte, n)
	val := []byte("v")
	for i := 0; i < n; i++ {
		k := make([]byte, len(prefix)+8)
		copy(k, prefix)
		binary.BigEndian.PutUint64(k[len(prefix):], uint64(i))
		keys[i] = k
		if err := db.Set(k, val); err != nil {
			return fmt.Errorf("verify: set %d: %w", i, err)
		}
	}

	cleanup := func() error {
		var cerr error
		for _, k := range keys {
			if err := db.Delete(k); err != nil && cerr == nil {
				cerr = err
			}
		}
		return cerr
	}
	defer func() {
		if err := cleanup(); err != nil && retErr == nil {
			retErr = fmt.Errorf("verify: cleanup: %w", err)
		}
	}()

	start := append([]byte(nil), prefix...)
	end := append(append([]byte(nil), prefix...), 0xFF)

	checkIter := func(iter kvstore.Iterator, descending bool) (int, error) {
		count := 0
		var prev []byte
		for iter.Valid() {
			k := append([]byte(nil), iter.Key()...)
			if prev != nil {
				cmp := bytes.Compare(prev, k)
				if descending && cmp <= 0 {
					return count, fmt.Errorf("order broke: %x then %x (expected descending)", prev, k)
				}
				if !descending && cmp >= 0 {
					return count, fmt.Errorf("order broke: %x then %x (expected ascending)", prev, k)
				}
			}
			prev = k
			_ = iter.Value()
			iter.Next()
			count++
		}
		if err := iter.Error(); err != nil {
			return count, err
		}
		return count, nil
	}

	fwd, err := rs.Iterator(start, end)
	if err != nil {
		return fmt.Errorf("verify: forward iterator: %w", err)
	}
	if count, err := checkIter(fwd, false); err != nil {
		_ = fwd.Close()
		return fmt.Errorf("verify: forward iterator: %w", err)
	} else if count != n {
		_ = fwd.Close()
		return fmt.Errorf("verify: forward iterator count=%d want %d", count, n)
	}
	if err := fwd.Close(); err != nil {
		return fmt.Errorf("verify: forward close: %w", err)
	}

	rev, err := rs.ReverseIterator(start, end)
	if err != nil {
		return fmt.Errorf("verify: reverse iterator: %w", err)
	}
	if count, err := checkIter(rev, true); err != nil {
		_ = rev.Close()
		return fmt.Errorf("verify: reverse iterator: %w", err)
	} else if count != n {
		_ = rev.Close()
		return fmt.Errorf("verify: reverse iterator count=%d want %d", count, n)
	}
	if err := rev.Close(); err != nil {
		return fmt.Errorf("verify: reverse close: %w", err)
	}

	return nil
}

// --- Benchmark Runner ---

var (
	numKeys      = flag.Int("keys", 100000, "Number of keys")
	valSize      = flag.Int("valsize", 128, "Value size in bytes")
	batchSize    = flag.Int("batchsize", 1000, "Size of batches")
	rangeQueries = flag.Int("range-queries", 200, "number of range queries")
	rangeSpan    = flag.Int("range-span", 100, "number of keys per range")
	keyCountsArg = flag.String("keycounts", "", "Comma-separated key counts to sweep over (overrides -keys)")
	keyScaleArg  = flag.String("keyscale", "", "Generate keycounts by scale: log10 or doubling (uses -keys-min/-keys-max)")
	keysMin      = flag.Int("keys-min", 1000, "Minimum key count for -keyscale")
	keysMax      = flag.Int("keys-max", 10000000, "Maximum key count for -keyscale")
	dbsArg       = flag.String("dbs", "all", "Comma-separated list of DBs to run (hashdb,btree,treedb,treedbbackend,badger,leveldb); aliases: gomap->hashdb, treedbcached->treedb, treedbraw->treedbbackend")
	testArg      = flag.String("test", "all", "Comma-separated list of tests (sequential_write,random_read,random_write,random_delete,full_scan,prefix_scan,batch_write,batch_random); aliases: write_seq->sequential_write, write_rand->random_write, read_rand->random_read, delete_rand->random_delete, scan->full_scan, range_scan->prefix_scan")
	formatArg    = flag.String("format", "table", "Output format: table or markdown")
	suiteArg     = flag.String("suite", "", "Named benchmark suite (e.g. readme)")
	outDirArg    = flag.String("outdir", "", "Write plots/results to this directory (used by -suite readme)")
	keepDir      = flag.Bool("keep", false, "Keep data directories after run")
	progress     = flag.Bool("progress", true, "Live-update the results table on stderr (cell-by-cell) while running; final table prints once to stdout")
	seed         = flag.Int64("seed", 1, "PRNG seed for randomized tests (0 = time-based)")
	cpuProfile   = flag.String("cpuprofile", "", "write cpu profile to file")

	blockProfile = flag.String("blockprofile", "", "write goroutine blocking profile (pprof) to file")
	blockRate    = flag.Int("blockprofilerate", 1, "runtime.SetBlockProfileRate sampling rate (1 = sample all)")
	mutexProfile = flag.String("mutexprofile", "", "write mutex contention profile (pprof) to file")
	mutexFrac    = flag.Int("mutexprofilefraction", 1, "runtime.SetMutexProfileFraction sampling fraction (1 = sample all)")
	traceProfile = flag.String("trace", "", "write runtime execution trace to file")

	treedbFlushThreshold           = flag.Int64("treedb-flush-threshold", 64*1024*1024, "TreeDB (cached): flush threshold in bytes")
	treedbKeepRecent               = flag.Uint64("treedb-keep-recent", 0, "TreeDB: KeepRecent commit versions to retain before page reuse (0=default; cached defaults to 1)")
	treedbMaxQueuedMems            = flag.Int("treedb-max-queued-memtables", 0, "TreeDB (cached): max queued immutable memtables before backpressure flush (0=default, <0=disable)")
	treedbSlowdownBacklogSeconds   = flag.Float64("treedb-slowdown-backlog-seconds", 0, "TreeDB (cached): begin writer backpressure when queued flush backlog exceeds this many seconds (0=disabled)")
	treedbStopBacklogSeconds       = flag.Float64("treedb-stop-backlog-seconds", 0, "TreeDB (cached): block writers when queued flush backlog exceeds this many seconds (0=disabled)")
	treedbMaxBacklogBytes          = flag.Int64("treedb-max-backlog-bytes", 0, "TreeDB (cached): absolute cap on queued flush backlog bytes (0=disabled)")
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
	settleBeforeScans              = flag.Bool("settle-before-scans", false, "Close+reopen DBs before scan tests to measure settled scan performance (flushes caches/WAL)")
)

func clampPPM(v int) int {
	if v < 0 {
		return 0
	}
	if v > 1_000_000 {
		return 1_000_000
	}
	return v
}

type DBInstance struct {
	Name    string
	Wrapper kvstore.DB
	Dir     string
}

type BenchConfig struct {
	Keys         int
	ValueSize    int
	BatchSize    int
	RangeQueries int
	RangeSpan    int

	DBsArg   string
	TestsArg string

	KeepDir  bool
	Progress bool
	SeedUsed int64

	CPUProfile string

	BlockProfile         string
	BlockProfileRate     int
	MutexProfile         string
	MutexProfileFraction int
	TraceProfile         string

	SettleBeforeScans bool

	TreeDBIterDebug      bool
	TreeDBIterDebugLimit int

	TreeDBCompactBeforeScans       bool
	TreeDBCompactDeadRatio         float64
	TreeDBCompactMinBytes          uint64
	TreeDBCompactMaxSlabs          int
	TreeDBCompactMicroBatch        int
	TreeDBCompactRotateBeforeWrite bool
	TreeDBCompactCopyBps           int64
	TreeDBCompactCopyBurst         int64
	TreeDBVacuumBeforeScans        bool
}

type BenchRun struct {
	Config       BenchConfig
	Instances    []*DBInstance
	TestOrder    []string
	DisplayNames map[string]string
	Results      map[string]map[string]float64
}

type scanDiag struct {
	dbName              string
	queueBacklogBytes   string
	queueLen            string
	flushThresholdBytes string
	maxQueuedMemtables  string
	backpressureMode    string
	flushBpsEWMA        string
}

func main() {
	flag.Parse()

	seedUsed := *seed
	if seedUsed == 0 {
		seedUsed = time.Now().UnixNano()
	}
	fmt.Fprintf(os.Stderr, "Throughput Benchmark (Operations per second)\n")
	fmt.Fprintf(os.Stderr, "seed=%d\n", seedUsed)

	baseCfg := BenchConfig{
		Keys:                           *numKeys,
		ValueSize:                      *valSize,
		BatchSize:                      *batchSize,
		RangeQueries:                   *rangeQueries,
		RangeSpan:                      *rangeSpan,
		DBsArg:                         *dbsArg,
		TestsArg:                       *testArg,
		KeepDir:                        *keepDir,
		Progress:                       *progress,
		SeedUsed:                       seedUsed,
		CPUProfile:                     *cpuProfile,
		BlockProfile:                   *blockProfile,
		BlockProfileRate:               *blockRate,
		MutexProfile:                   *mutexProfile,
		MutexProfileFraction:           *mutexFrac,
		TraceProfile:                   *traceProfile,
		SettleBeforeScans:              *settleBeforeScans,
		TreeDBIterDebug:                *treedbIterDebug,
		TreeDBIterDebugLimit:           *treedbIterDebugLimit,
		TreeDBCompactBeforeScans:       *treedbCompactBeforeScans,
		TreeDBCompactDeadRatio:         *treedbCompactDeadRatio,
		TreeDBCompactMinBytes:          *treedbCompactMinBytes,
		TreeDBCompactMaxSlabs:          *treedbCompactMaxSlabs,
		TreeDBCompactMicroBatch:        *treedbCompactMicroBatch,
		TreeDBCompactRotateBeforeWrite: *treedbCompactRotateBeforeWrite,
		TreeDBCompactCopyBps:           *treedbCompactCopyBps,
		TreeDBCompactCopyBurst:         *treedbCompactCopyBurst,
		TreeDBVacuumBeforeScans:        *treedbVacuumBeforeScans,
	}

	suite := strings.ToLower(strings.TrimSpace(*suiteArg))
	if suite != "" {
		switch suite {
		case "readme":
			out, err := runReadmeSuite(baseCfg)
			if err != nil {
				log.Fatalf("readme suite: %v", err)
			}
			fmt.Print(out)
		case "churn":
			out, err := runChurnSuite(baseCfg)
			if err != nil {
				log.Fatalf("churn suite: %v", err)
			}
			fmt.Print(out)
		case "churnvacuum":
			out, err := runChurnVacuumSuite(baseCfg)
			if err != nil {
				log.Fatalf("churnvacuum suite: %v", err)
			}
			fmt.Print(out)
		case "churnmaint":
			out, err := runChurnMaintSuite(baseCfg)
			if err != nil {
				log.Fatalf("churnmaint suite: %v", err)
			}
			fmt.Print(out)
		default:
			log.Fatalf("unknown suite: %q", suite)
		}
		return
	}

	format := strings.ToLower(strings.TrimSpace(*formatArg))
	keyCounts, err := resolveKeyCounts(*numKeys, *keyCountsArg, *keyScaleArg, *keysMin, *keysMax)
	if err != nil {
		log.Fatalf("keycounts: %v", err)
	}

	hasAnyProfiling := baseCfg.CPUProfile != "" || baseCfg.BlockProfile != "" || baseCfg.MutexProfile != "" || baseCfg.TraceProfile != ""
	if hasAnyProfiling && len(keyCounts) > 1 {
		log.Fatalf("profiling flags require a single key count (got %d): disable sweep keycounts or omit -cpuprofile/-blockprofile/-mutexprofile/-trace", len(keyCounts))
	}

	if len(keyCounts) == 1 {
		cfg := baseCfg
		cfg.Keys = keyCounts[0]
		if format == "markdown" {
			cfg.Progress = false
			run, err := runBenchmark(cfg)
			if err != nil {
				log.Fatalf("benchmark: %v", err)
			}
			fmt.Print(renderMarkdownSingle(run))
			return
		}

		run, err := runBenchmark(cfg)
		if err != nil {
			log.Fatalf("benchmark: %v", err)
		}
		printResultsTable(run.Instances, run.TestOrder, run.DisplayNames, run.Results)
		return
	}

	cfg := baseCfg
	cfg.Progress = false
	runs, err := runSweep(cfg, keyCounts)
	if err != nil {
		log.Fatalf("benchmark sweep: %v", err)
	}

	switch format {
	case "table":
		for _, run := range runs {
			fmt.Printf("\nkeys=%s valsize=%d batchsize=%d range-queries=%d range-span=%d\n\n",
				formatInt(run.Config.Keys), run.Config.ValueSize, run.Config.BatchSize, run.Config.RangeQueries, run.Config.RangeSpan)
			printResultsTable(run.Instances, run.TestOrder, run.DisplayNames, run.Results)
		}
	case "markdown":
		fmt.Print(renderMarkdownSweep(runs))
	default:
		log.Fatalf("unknown -format: %q", format)
	}
}

func resolveKeyCounts(keys int, keyCountsArg, keyScaleArg string, keysMin, keysMax int) ([]int, error) {
	if strings.TrimSpace(keyCountsArg) != "" {
		parts := strings.Split(keyCountsArg, ",")
		out := make([]int, 0, len(parts))
		for _, p := range parts {
			v, err := parseKeyCount(p)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return sortUniquePositive(out), nil
	}

	if strings.TrimSpace(keyScaleArg) != "" {
		scale := strings.ToLower(strings.TrimSpace(keyScaleArg))
		if keysMin <= 0 || keysMax <= 0 || keysMin > keysMax {
			return nil, fmt.Errorf("invalid -keys-min/-keys-max: %d..%d", keysMin, keysMax)
		}

		var out []int
		switch scale {
		case "log10":
			for v := keysMin; v <= keysMax; {
				out = append(out, v)
				if v > keysMax/10 {
					break
				}
				v *= 10
			}
		case "doubling":
			for v := keysMin; v <= keysMax; {
				out = append(out, v)
				if v > keysMax/2 {
					break
				}
				v *= 2
			}
		default:
			return nil, fmt.Errorf("unknown -keyscale: %q (supported: log10, doubling)", scale)
		}
		return sortUniquePositive(out), nil
	}

	if keys <= 0 {
		return nil, fmt.Errorf("invalid -keys: %d", keys)
	}
	return []int{keys}, nil
}

func parseKeyCount(s string) (int, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return 0, fmt.Errorf("empty keycount")
	}

	multiplier := 1.0
	switch s[len(s)-1] {
	case 'k':
		multiplier = 1e3
		s = strings.TrimSpace(s[:len(s)-1])
	case 'm':
		multiplier = 1e6
		s = strings.TrimSpace(s[:len(s)-1])
	case 'g':
		multiplier = 1e9
		s = strings.TrimSpace(s[:len(s)-1])
	}

	s = strings.ReplaceAll(s, "_", "")
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("parse keycount %q: %w", s, err)
	}
	v := int(f * multiplier)
	if v <= 0 {
		return 0, fmt.Errorf("invalid keycount %q", s)
	}
	return v, nil
}

func sortUniquePositive(vals []int) []int {
	if len(vals) == 0 {
		return nil
	}
	sort.Ints(vals)
	out := vals[:0]
	for _, v := range vals {
		if v <= 0 {
			continue
		}
		if len(out) == 0 || v != out[len(out)-1] {
			out = append(out, v)
		}
	}
	return out
}

func runSweep(baseCfg BenchConfig, keyCounts []int) ([]BenchRun, error) {
	runs := make([]BenchRun, 0, len(keyCounts))
	for _, kc := range keyCounts {
		cfg := baseCfg
		cfg.Keys = kc
		run, err := runBenchmark(cfg)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, nil
}

func runBenchmark(cfg BenchConfig) (BenchRun, error) {
	if cfg.Keys <= 0 {
		return BenchRun{}, fmt.Errorf("invalid keys: %d", cfg.Keys)
	}
	if cfg.ValueSize < 0 {
		return BenchRun{}, fmt.Errorf("invalid valsize: %d", cfg.ValueSize)
	}
	if cfg.BatchSize <= 0 {
		return BenchRun{}, fmt.Errorf("invalid batchsize: %d", cfg.BatchSize)
	}
	if cfg.RangeQueries < 0 || cfg.RangeSpan < 0 {
		return BenchRun{}, fmt.Errorf("invalid range settings: queries=%d span=%d", cfg.RangeQueries, cfg.RangeSpan)
	}

	dbsToRun := parseList(cfg.DBsArg)
	testsToRun := normalizeTests(parseList(cfg.TestsArg))

	factories := map[string]func(string) (kvstore.DB, error){
		"hashdb":        func(d string) (kvstore.DB, error) { return NewHashDB(d) },
		"gomap":         func(d string) (kvstore.DB, error) { return NewHashDB(d) }, // legacy alias
		"btree":         func(d string) (kvstore.DB, error) { return NewBTree(d) },
		"treedb":        func(d string) (kvstore.DB, error) { return NewTreeDB(d) },        // cached (default)
		"treedbcached":  func(d string) (kvstore.DB, error) { return NewTreeDB(d) },        // legacy alias
		"treedbbackend": func(d string) (kvstore.DB, error) { return NewTreeDBBackend(d) }, // uncached
		"treedbraw":     func(d string) (kvstore.DB, error) { return NewTreeDBBackend(d) }, // alias
		"badger":        func(d string) (kvstore.DB, error) { return NewBadger(d) },
		"leveldb":       func(d string) (kvstore.DB, error) { return NewLevelDB(d) },
	}

	// Initialize DBs
	instances := make([]*DBInstance, 0)
	// Order matching dbsArg or default hardcoded order if "all"
	orderedDBs := []string{"hashdb", "btree", "treedb", "treedbbackend", "badger", "leveldb"}
	if strings.TrimSpace(cfg.DBsArg) != "all" {
		orderedDBs = dbsToRun
	}

	for _, name := range orderedDBs {
		if !contains(dbsToRun, name) && !contains(dbsToRun, "all") {
			continue
		}
		factory, ok := factories[name]
		if !ok {
			return BenchRun{}, fmt.Errorf("unknown DB: %q", name)
		}

		dir, err := os.MkdirTemp("", "bench-"+name+"*")
		if err != nil {
			return BenchRun{}, fmt.Errorf("temp dir: %w", err)
		}

		db, err := factory(dir)
		if err != nil {
			_ = os.RemoveAll(dir)
			return BenchRun{}, fmt.Errorf("init %s: %w", name, err)
		}

		instances = append(instances, &DBInstance{Name: name, Wrapper: db, Dir: dir})
	}
	if len(instances) == 0 {
		return BenchRun{}, fmt.Errorf("no DBs selected")
	}

	// Define Tests
	type TestFunc func(db kvstore.DB, rng *rand.Rand) (float64, error)

	prefixScanBase := 0
	expectedFullScanCount := -1
	checkPrefixCounts := false
	testFuncs := map[string]TestFunc{
		"vacuum_index": func(db kvstore.DB, _ *rand.Rand) (float64, error) {
			td, ok := db.(*treedbadapter.DB)
			if !ok || td == nil || td.DB == nil {
				return math.NaN(), nil
			}
			if err := td.DB.CompactIndex(); err != nil {
				return 0, fmt.Errorf("vacuum_index: %w", err)
			}
			return math.NaN(), nil
		},
		"compact_slabs": func(db kvstore.DB, _ *rand.Rand) (float64, error) {
			td, ok := db.(*treedbadapter.DB)
			if !ok || td == nil || td.DB == nil {
				return math.NaN(), nil
			}
			if err := td.DB.CompactCandidates(compaction.Options{
				DeadRatioThreshold: cfg.TreeDBCompactDeadRatio,
				MinTotalBytes:      cfg.TreeDBCompactMinBytes,
				MaxSlabs:           cfg.TreeDBCompactMaxSlabs,
				MicroBatchSize:     cfg.TreeDBCompactMicroBatch,
				RotateBeforeWrite:  cfg.TreeDBCompactRotateBeforeWrite,
				CopyBytesPerSec:    cfg.TreeDBCompactCopyBps,
				CopyBurstBytes:     cfg.TreeDBCompactCopyBurst,
			}); err != nil {
				return 0, fmt.Errorf("compact_slabs: %w", err)
			}
			return math.NaN(), nil
		},
		"sequential_write": func(db kvstore.DB, _ *rand.Rand) (float64, error) {
			start := time.Now()
			val := make([]byte, cfg.ValueSize)
			var k [8]byte
			for i := 0; i < cfg.Keys; i++ {
				binary.BigEndian.PutUint64(k[:], uint64(i))
				if err := db.Set(k[:], val); err != nil {
					return 0, fmt.Errorf("sequential_write: %w", err)
				}
			}
			return float64(cfg.Keys) / time.Since(start).Seconds(), nil
		},
		"random_write": func(db kvstore.DB, rng *rand.Rand) (float64, error) {
			start := time.Now()
			val := make([]byte, cfg.ValueSize)
			var k [8]byte
			for i := 0; i < cfg.Keys; i++ {
				binary.BigEndian.PutUint64(k[:], uint64(rng.Intn(cfg.Keys*10))) // Use a larger range for randomness
				if err := db.Set(k[:], val); err != nil {
					return 0, fmt.Errorf("random_write: %w", err)
				}
			}
			return float64(cfg.Keys) / time.Since(start).Seconds(), nil
		},
		"batch_write": func(db kvstore.DB, _ *rand.Rand) (float64, error) {
			batcher, ok := db.(kvstore.Batcher)
			if !ok {
				return math.NaN(), nil
			}
			start := time.Now()
			val := make([]byte, cfg.ValueSize)
			total := cfg.Keys
			var k [8]byte
			for i := 0; i < total; i += cfg.BatchSize {
				batch, err := batcher.NewBatch()
				if err != nil {
					return 0, fmt.Errorf("batch_write: new batch: %w", err)
				}

				end := i + cfg.BatchSize
				if end > total {
					end = total
				}
				for j := i; j < end; j++ {
					binary.BigEndian.PutUint64(k[:], uint64(j+cfg.Keys))
					if err := batch.Set(k[:], val); err != nil {
						_ = batch.Close()
						return 0, fmt.Errorf("batch_write: set: %w", err)
					}
				}
				if err := batch.Commit(); err != nil {
					_ = batch.Close()
					return 0, fmt.Errorf("batch_write: commit: %w", err)
				}
				if err := batch.Close(); err != nil {
					return 0, fmt.Errorf("batch_write: close: %w", err)
				}
			}
			return float64(total) / time.Since(start).Seconds(), nil
		},
		"batch_random": func(db kvstore.DB, rng *rand.Rand) (float64, error) {
			batcher, ok := db.(kvstore.Batcher)
			if !ok {
				return math.NaN(), nil
			}
			start := time.Now()
			val := make([]byte, cfg.ValueSize)
			total := cfg.Keys
			batchSize := 1000 // Using typical batch size
			var k [8]byte

			keys := make([][]byte, total)
			for i := 0; i < total; i++ {
				binary.BigEndian.PutUint64(k[:], uint64(rng.Intn(total*10))) // Spread out to cause random I/O
				keys[i] = append([]byte(nil), k[:]...)
			}

			// Reset timer to exclude setup
			start = time.Now()

			for i := 0; i < total; i += batchSize {
				batch, err := batcher.NewBatch()
				if err != nil {
					return 0, fmt.Errorf("batch_random: new batch: %w", err)
				}

				end := i + batchSize
				if end > total {
					end = total
				}
				for j := i; j < end; j++ {
					if err := batch.Set(keys[j], val); err != nil {
						_ = batch.Close()
						return 0, fmt.Errorf("batch_random: set: %w", err)
					}
				}
				if err := batch.Commit(); err != nil {
					_ = batch.Close()
					return 0, fmt.Errorf("batch_random: commit: %w", err)
				}
				if err := batch.Close(); err != nil {
					return 0, fmt.Errorf("batch_random: close: %w", err)
				}
			}
			return float64(total) / time.Since(start).Seconds(), nil
		},
		"batch_small_seq": func(db kvstore.DB, rng *rand.Rand) (float64, error) {
			batcher, ok := db.(kvstore.Batcher)
			if !ok {
				return math.NaN(), nil
			}
			start := time.Now()
			val := make([]byte, cfg.ValueSize)
			total := cfg.Keys
			batchSize := 100 // Pathological: small enough to hurt if not buffered, sequential to trigger streaming
			var k [8]byte

			for i := 0; i < total; i += batchSize {
				batch, err := batcher.NewBatch()
				if err != nil {
					return 0, fmt.Errorf("batch_small_seq: new batch: %w", err)
				}

				end := i + batchSize
				if end > total {
					end = total
				}
				for j := i; j < end; j++ {
					binary.BigEndian.PutUint64(k[:], uint64(j)) // Sequential
					if err := batch.Set(k[:], val); err != nil {
						_ = batch.Close()
						return 0, fmt.Errorf("batch_small_seq: set: %w", err)
					}
				}
				if err := batch.Commit(); err != nil {
					_ = batch.Close()
					return 0, fmt.Errorf("batch_small_seq: commit: %w", err)
				}
				if err := batch.Close(); err != nil {
					return 0, fmt.Errorf("batch_small_seq: close: %w", err)
				}
			}
			return float64(total) / time.Since(start).Seconds(), nil
		},
		"random_delete": func(db kvstore.DB, rng *rand.Rand) (float64, error) {
			start := time.Now()
			var k [8]byte
			for i := 0; i < cfg.Keys; i++ {
				binary.BigEndian.PutUint64(k[:], uint64(rng.Intn(cfg.Keys)))
				_ = db.Delete(k[:])
			}
			return float64(cfg.Keys) / time.Since(start).Seconds(), nil
		},
		"random_read": func(db kvstore.DB, rng *rand.Rand) (float64, error) {
			start := time.Now()
			var k [8]byte
			for i := 0; i < cfg.Keys; i++ {
				binary.BigEndian.PutUint64(k[:], uint64(rng.Intn(cfg.Keys)))
				_, _ = db.Get(k[:])
			}
			return float64(cfg.Keys) / time.Since(start).Seconds(), nil
		},
		"full_scan": func(db kvstore.DB, _ *rand.Rand) (float64, error) {
			start := time.Now()
			if rs, ok := db.(kvstore.RangeScanner); ok {
				iter, err := rs.Iterator(nil, nil)
				if err != nil {
					return 0, fmt.Errorf("full_scan: iterator: %w", err)
				}
				defer func() { _ = iter.Close() }()

				count := 0
				for iter.Valid() {
					_ = iter.Key()
					_ = iter.Value()
					iter.Next()
					count++
				}
				if err := iter.Error(); err != nil {
					return 0, fmt.Errorf("full_scan: %w", err)
				}
				if expectedFullScanCount >= 0 && count != expectedFullScanCount {
					return 0, fmt.Errorf("full_scan: expected %d items, got %d", expectedFullScanCount, count)
				}
				return float64(count) / time.Since(start).Seconds(), nil
			}

			if fe, ok := db.(kvstore.ForEacher); ok {
				count := 0
				if err := fe.ForEach(func(_ []byte, _ []byte) error {
					count++
					return nil
				}); err != nil {
					return 0, fmt.Errorf("full_scan: foreach: %w", err)
				}
				return float64(count) / time.Since(start).Seconds(), nil
			}

			return math.NaN(), nil
		},
		"full_scan2": func(db kvstore.DB, _ *rand.Rand) (float64, error) {
			start := time.Now()
			if rs, ok := db.(kvstore.RangeScanner); ok {
				iter, err := rs.Iterator(nil, nil)
				if err != nil {
					return 0, fmt.Errorf("full_scan2: iterator: %w", err)
				}
				defer func() { _ = iter.Close() }()

				count := 0
				for iter.Valid() {
					_ = iter.Key()
					_ = iter.Value()
					iter.Next()
					count++
				}
				if err := iter.Error(); err != nil {
					return 0, fmt.Errorf("full_scan2: %w", err)
				}
				return float64(count) / time.Since(start).Seconds(), nil
			}

			if fe, ok := db.(kvstore.ForEacher); ok {
				count := 0
				if err := fe.ForEach(func(_ []byte, _ []byte) error {
					count++
					return nil
				}); err != nil {
					return 0, fmt.Errorf("full_scan2: foreach: %w", err)
				}
				return float64(count) / time.Since(start).Seconds(), nil
			}

			return math.NaN(), nil
		},
		"prefix_scan": func(db kvstore.DB, rng *rand.Rand) (float64, error) {
			rs, ok := db.(kvstore.RangeScanner)
			if !ok {
				return math.NaN(), nil
			}
			start := time.Now()
			var totalBuild time.Duration
			var totalIter time.Duration
			var debugQueueSum int
			var debugSourcesSum int
			var debugStatsCount int
			totalItems := 0
			maxKey := prefixScanBase + cfg.Keys
			for i := 0; i < cfg.RangeQueries; i++ {
				startIdx := prefixScanBase + rng.Intn(cfg.Keys)
				endIdx := startIdx + cfg.RangeSpan
				if endIdx > maxKey {
					endIdx = maxKey
				}

				var startKeyBuf [8]byte
				binary.BigEndian.PutUint64(startKeyBuf[:], uint64(startIdx))
				startKey := startKeyBuf[:]

				var endKeyBuf [8]byte
				binary.BigEndian.PutUint64(endKeyBuf[:], uint64(endIdx))
				endKey := endKeyBuf[:]

				buildStart := time.Now()
				iter, err := rs.Iterator(startKey, endKey)
				if err != nil {
					return 0, fmt.Errorf("prefix_scan: iterator: %w", err)
				}
				buildDur := time.Since(buildStart)
				totalBuild += buildDur

				expected := endIdx - startIdx
				itemsThisQuery := 0
				iterStart := time.Now()
				for iter.Valid() {
					_ = iter.Key()
					iter.Next()
					itemsThisQuery++
				}
				iterDur := time.Since(iterStart)
				totalIter += iterDur
				if err := iter.Error(); err != nil {
					_ = iter.Close()
					return 0, fmt.Errorf("prefix_scan: %w", err)
				}
				if checkPrefixCounts && itemsThisQuery != expected {
					_ = iter.Close()
					return 0, fmt.Errorf("prefix_scan: expected %d items in query %d, got %d", expected, i, itemsThisQuery)
				}
				totalItems += itemsThisQuery

				type debugStats interface {
					DebugStats() (queueLen int, sourcesUsed int)
				}
				if ds, ok := iter.(debugStats); ok {
					q, s := ds.DebugStats()
					debugQueueSum += q
					debugSourcesSum += s
					debugStatsCount++
					if cfg.TreeDBIterDebug && i < cfg.TreeDBIterDebugLimit {
						fmt.Fprintf(os.Stderr, "prefix_scan/%s query=%d items=%d build=%s iter=%s queue=%d sources=%d\n",
							db.Name(), i, itemsThisQuery, buildDur, iterDur, q, s)
					}
				} else if cfg.TreeDBIterDebug && i < cfg.TreeDBIterDebugLimit {
					fmt.Fprintf(os.Stderr, "prefix_scan/%s query=%d items=%d build=%s iter=%s\n",
						db.Name(), i, itemsThisQuery, buildDur, iterDur)
				}

				_ = iter.Close()
			}
			if cfg.TreeDBIterDebug && cfg.RangeQueries > 0 {
				avgBuild := totalBuild / time.Duration(cfg.RangeQueries)
				avgIter := totalIter / time.Duration(cfg.RangeQueries)
				if debugStatsCount > 0 {
					fmt.Fprintf(os.Stderr, "prefix_scan/%s summary queries=%d span=%d items=%d build_avg=%s iter_avg=%s queue_avg=%.2f sources_avg=%.2f\n",
						db.Name(), cfg.RangeQueries, cfg.RangeSpan, totalItems, avgBuild, avgIter,
						float64(debugQueueSum)/float64(debugStatsCount),
						float64(debugSourcesSum)/float64(debugStatsCount))
				} else {
					fmt.Fprintf(os.Stderr, "prefix_scan/%s summary queries=%d span=%d items=%d build_avg=%s iter_avg=%s\n",
						db.Name(), cfg.RangeQueries, cfg.RangeSpan, totalItems, avgBuild, avgIter)
				}
			}
			return float64(totalItems) / time.Since(start).Seconds(), nil
		},
		"prefix_scan2": func(db kvstore.DB, rng *rand.Rand) (float64, error) {
			rs, ok := db.(kvstore.RangeScanner)
			if !ok {
				return math.NaN(), nil
			}
			start := time.Now()
			totalItems := 0
			maxKey := prefixScanBase + cfg.Keys
			for i := 0; i < cfg.RangeQueries; i++ {
				startIdx := prefixScanBase + rng.Intn(cfg.Keys)
				endIdx := startIdx + cfg.RangeSpan
				if endIdx > maxKey {
					endIdx = maxKey
				}

				var startKeyBuf [8]byte
				binary.BigEndian.PutUint64(startKeyBuf[:], uint64(startIdx))
				startKey := startKeyBuf[:]

				var endKeyBuf [8]byte
				binary.BigEndian.PutUint64(endKeyBuf[:], uint64(endIdx))
				endKey := endKeyBuf[:]

				iter, err := rs.Iterator(startKey, endKey)
				if err != nil {
					return 0, fmt.Errorf("prefix_scan2: iterator: %w", err)
				}

				itemsThisQuery := 0
				for iter.Valid() {
					_ = iter.Key()
					_ = iter.Value()
					iter.Next()
					itemsThisQuery++
				}
				if err := iter.Error(); err != nil {
					_ = iter.Close()
					return 0, fmt.Errorf("prefix_scan2: %w", err)
				}
				if err := iter.Close(); err != nil {
					return 0, fmt.Errorf("prefix_scan2: close: %w", err)
				}

				totalItems += itemsThisQuery
			}
			return float64(totalItems) / time.Since(start).Seconds(), nil
		},
	}

	allTestOrder := []string{"sequential_write", "random_write", "batch_write", "batch_random", "batch_small_seq", "random_delete", "random_read", "full_scan", "prefix_scan"}
	displayNames := map[string]string{
		"vacuum_index":     "VACUUM (Index)",
		"compact_slabs":    "COMPACT (Slabs)",
		"sequential_write": "Sequential Write",
		"random_write":     "Random Write",
		"random_read":      "Random Read",
		"full_scan":        "Full Scan",
		"full_scan2":       "Full Scan (After VACUUM)",
		"prefix_scan":      "Prefix Scan",
		"prefix_scan2":     "Prefix Scan (After VACUUM)",
		"batch_write":      "Batch Write",
		"batch_random":     "Batch Random",
		"batch_small_seq":  "Batch Small Seq",
		"random_delete":    "Random Delete",
	}

	finalTestOrder := make([]string, 0)
	if contains(testsToRun, "all") {
		finalTestOrder = allTestOrder
	} else {
		for _, t := range testsToRun {
			if t == "" {
				continue
			}
			if _, ok := testFuncs[t]; !ok {
				return BenchRun{}, fmt.Errorf("unknown test: %q", t)
			}
			finalTestOrder = append(finalTestOrder, t)
		}
	}
	if len(finalTestOrder) == 0 {
		return BenchRun{}, fmt.Errorf("no tests selected")
	}

	// If the user selects only read/scan/delete tests, the DBs are empty unless we
	// preload a baseline dataset. We intentionally keep this setup out of the
	// per-test timings so that read/scan numbers reflect a populated DB.
	hasMeasuredWrites := containsAny(finalTestOrder,
		"sequential_write",
		"random_write",
		"batch_write",
		"batch_random",
		"batch_small_seq",
	)
	needsExistingData := containsAny(finalTestOrder,
		"random_read",
		"random_delete",
		"full_scan",
		"prefix_scan",
	)
	preloadedOnly := needsExistingData && !hasMeasuredWrites
	if preloadedOnly {
		val := make([]byte, cfg.ValueSize)
		var k [8]byte
		for _, inst := range instances {
			for i := 0; i < cfg.Keys; i++ {
				binary.BigEndian.PutUint64(k[:], uint64(i))
				if err := inst.Wrapper.Set(k[:], val); err != nil {
					return BenchRun{}, fmt.Errorf("preload/%s: %w", inst.Wrapper.Name(), err)
				}
			}
		}
	}

	if preloadedOnly {
		expectedFullScanCount = cfg.Keys
		checkPrefixCounts = true
	}

	for _, inst := range instances {
		rs, ok := inst.Wrapper.(kvstore.RangeScanner)
		if !ok {
			continue
		}
		if err := verifyRangeIteration(inst.Wrapper, rs, verifyRangePrefix, verifyRangeCount); err != nil {
			return BenchRun{}, fmt.Errorf("verify/%s: %w", inst.Wrapper.Name(), err)
		}
	}

	if contains(finalTestOrder, "batch_write") && !contains(finalTestOrder, "sequential_write") && !contains(finalTestOrder, "random_write") {
		prefixScanBase = cfg.Keys
	}

	if cfg.BlockProfile != "" {
		rate := cfg.BlockProfileRate
		if rate <= 0 {
			rate = 1
		}
		runtime.SetBlockProfileRate(rate)
		defer runtime.SetBlockProfileRate(0)

		f, err := os.Create(cfg.BlockProfile)
		if err != nil {
			return BenchRun{}, fmt.Errorf("blockprofile: %w", err)
		}
		defer func() {
			_ = pprof.Lookup("block").WriteTo(f, 0)
			_ = f.Close()
		}()
	}

	if cfg.MutexProfile != "" {
		frac := cfg.MutexProfileFraction
		if frac <= 0 {
			frac = 1
		}
		runtime.SetMutexProfileFraction(frac)
		defer runtime.SetMutexProfileFraction(0)

		f, err := os.Create(cfg.MutexProfile)
		if err != nil {
			return BenchRun{}, fmt.Errorf("mutexprofile: %w", err)
		}
		defer func() {
			_ = pprof.Lookup("mutex").WriteTo(f, 0)
			_ = f.Close()
		}()
	}

	if cfg.TraceProfile != "" {
		f, err := os.Create(cfg.TraceProfile)
		if err != nil {
			return BenchRun{}, fmt.Errorf("trace: %w", err)
		}
		if err := trace.Start(f); err != nil {
			_ = f.Close()
			return BenchRun{}, fmt.Errorf("trace start: %w", err)
		}
		defer func() {
			trace.Stop()
			_ = f.Close()
		}()
	}

	var cpuProfFile *os.File
	if cfg.CPUProfile != "" {
		f, err := os.Create(cfg.CPUProfile)
		if err != nil {
			return BenchRun{}, fmt.Errorf("cpuprofile: %w", err)
		}
		cpuProfFile = f
		if err := pprof.StartCPUProfile(cpuProfFile); err != nil {
			_ = cpuProfFile.Close()
			return BenchRun{}, fmt.Errorf("cpuprofile start: %w", err)
		}
		defer func() {
			pprof.StopCPUProfile()
			_ = cpuProfFile.Close()
		}()
	}

	// Run Tests
	results := make(map[string]map[string]float64)
	for _, testName := range finalTestOrder {
		results[testName] = make(map[string]float64, len(instances))
		for _, inst := range instances {
			results[testName][inst.Wrapper.Name()] = math.NaN()
		}
	}

	var live *liveTable
	if cfg.Progress {
		live = newLiveTable(os.Stderr, instances, finalTestOrder, displayNames)
		_ = live.Render(results)
	}

	// Capture cache backlog stats at scan start so scan results can be interpreted.
	var scanDiagnostics []scanDiag
	scanDiagnosticsCaptured := false
	captureScanDiagnostics := func() {
		scanDiagnosticsCaptured = true
		for _, inst := range instances {
			sp, ok := inst.Wrapper.(kvstore.StatsProvider)
			if !ok {
				continue
			}
			stats := sp.Stats()
			if stats == nil {
				continue
			}
			backlog := stats["treedb.cache.queue_backlog_bytes"]
			if backlog == "" {
				continue
			}
			scanDiagnostics = append(scanDiagnostics, scanDiag{
				dbName:              inst.Wrapper.Name(),
				queueBacklogBytes:   backlog,
				queueLen:            stats["treedb.cache.queue_len"],
				flushThresholdBytes: stats["treedb.cache.flush_threshold_bytes"],
				maxQueuedMemtables:  stats["treedb.cache.max_queued_memtables"],
				backpressureMode:    stats["treedb.cache.backpressure_mode"],
				flushBpsEWMA:        stats["treedb.cache.flush_bps_ewma"],
			})
		}
	}

	settled := false
	compactTreeDBs := func() error {
		if !cfg.TreeDBCompactBeforeScans {
			return nil
		}
		for _, inst := range instances {
			td, ok := inst.Wrapper.(*treedbadapter.DB)
			if !ok || td == nil || td.DB == nil {
				continue
			}
			if err := td.DB.CompactCandidates(compaction.Options{
				DeadRatioThreshold: cfg.TreeDBCompactDeadRatio,
				MinTotalBytes:      cfg.TreeDBCompactMinBytes,
				MaxSlabs:           cfg.TreeDBCompactMaxSlabs,
				MicroBatchSize:     cfg.TreeDBCompactMicroBatch,
				RotateBeforeWrite:  cfg.TreeDBCompactRotateBeforeWrite,
				CopyBytesPerSec:    cfg.TreeDBCompactCopyBps,
				CopyBurstBytes:     cfg.TreeDBCompactCopyBurst,
			}); err != nil {
				return fmt.Errorf("compact %s: %w", inst.Wrapper.Name(), err)
			}
		}
		return nil
	}

	vacuumTreeDBs := func() error {
		if !cfg.TreeDBVacuumBeforeScans {
			return nil
		}
		for _, inst := range instances {
			td, ok := inst.Wrapper.(*treedbadapter.DB)
			if !ok || td == nil || td.DB == nil {
				continue
			}
			if err := td.DB.CompactIndex(); err != nil {
				return fmt.Errorf("vacuum %s: %w", inst.Wrapper.Name(), err)
			}
		}
		return nil
	}

	settle := func() error {
		for _, inst := range instances {
			_ = inst.Wrapper.Close()
			factory, ok := factories[inst.Name]
			if !ok {
				return fmt.Errorf("unknown DB factory: %q", inst.Name)
			}
			db, err := factory(inst.Dir)
			if err != nil {
				return fmt.Errorf("reopen %s: %w", inst.Name, err)
			}
			inst.Wrapper = db
		}
		if err := vacuumTreeDBs(); err != nil {
			return err
		}
		if err := compactTreeDBs(); err != nil {
			return err
		}
		return nil
	}

	for _, testName := range finalTestOrder {
		fn := testFuncs[testName]
		if fn == nil {
			return BenchRun{}, fmt.Errorf("unknown test: %q", testName)
		}

		if cfg.SettleBeforeScans && !settled && (testName == "full_scan" || testName == "prefix_scan") {
			if err := settle(); err != nil {
				return BenchRun{}, err
			}
			settled = true
			if live != nil {
				_ = live.Render(results)
			}
		} else if (cfg.TreeDBCompactBeforeScans || cfg.TreeDBVacuumBeforeScans) && !settled && (testName == "full_scan" || testName == "prefix_scan") {
			// If the caller didn't ask to settle, still allow an optional compaction
			// pass before scans so scan regressions after churn can be studied in a
			// "compacted values" state.
			if err := vacuumTreeDBs(); err != nil {
				return BenchRun{}, err
			}
			if err := compactTreeDBs(); err != nil {
				return BenchRun{}, err
			}
			settled = true
		}

		if !scanDiagnosticsCaptured && (testName == "full_scan" || testName == "prefix_scan") {
			captureScanDiagnostics()
		}

		for _, inst := range instances {
			rng := rand.New(rand.NewSource(testSeed(cfg.SeedUsed, testName)))
			res, err := fn(inst.Wrapper, rng)
			if err != nil {
				return BenchRun{}, fmt.Errorf("%s/%s: %w", testName, inst.Wrapper.Name(), err)
			}
			results[testName][inst.Wrapper.Name()] = res
			if live != nil {
				_ = live.UpdateCell(testName, inst.Wrapper.Name(), res)
			}
		}
	}

	// Cleanup
	for _, inst := range instances {
		_ = inst.Wrapper.Close()
		if !cfg.KeepDir {
			_ = os.RemoveAll(inst.Dir)
		}
	}
	if live != nil {
		_ = live.Clear()
	}

	if len(scanDiagnostics) > 0 {
		anyBacklog := false
		for _, diag := range scanDiagnostics {
			if n, err := strconv.ParseInt(diag.queueBacklogBytes, 10, 64); err == nil && n > 0 {
				anyBacklog = true
				break
			}
		}
		if anyBacklog && !cfg.SettleBeforeScans {
			fmt.Fprintln(os.Stderr)
			fmt.Fprintln(os.Stderr, "scan note: TreeDB scan tests ran with cached write-back backlog present; use -settle-before-scans to measure settled scan performance")
			for _, diag := range scanDiagnostics {
				fmt.Fprintf(os.Stderr, "  %s: queue_backlog_bytes=%s queue_len=%s flush_threshold_bytes=%s max_queued_memtables=%s backpressure_mode=%s flush_bps_ewma=%s\n",
					diag.dbName,
					diag.queueBacklogBytes,
					diag.queueLen,
					diag.flushThresholdBytes,
					diag.maxQueuedMemtables,
					diag.backpressureMode,
					diag.flushBpsEWMA,
				)
			}
		}
	}

	return BenchRun{
		Config:       cfg,
		Instances:    instances,
		TestOrder:    finalTestOrder,
		DisplayNames: displayNames,
		Results:      results,
	}, nil
}

func renderMarkdownSingle(run BenchRun) string {
	var sb strings.Builder
	sb.WriteString("# unified_bench\n\n")
	sb.WriteString(fmt.Sprintf("- keys: %s\n", formatInt(run.Config.Keys)))
	sb.WriteString(fmt.Sprintf("- valsize: %d\n", run.Config.ValueSize))
	sb.WriteString(fmt.Sprintf("- batchsize: %d\n", run.Config.BatchSize))
	sb.WriteString(fmt.Sprintf("- range-queries: %d\n", run.Config.RangeQueries))
	sb.WriteString(fmt.Sprintf("- range-span: %d\n", run.Config.RangeSpan))
	sb.WriteString("\n")

	sb.WriteString("```text\n")
	table, _, _, _ := renderResultsTableStringWithLayout(run.Instances, run.TestOrder, run.DisplayNames, run.Results)
	sb.WriteString(table)
	sb.WriteString("```\n")
	return sb.String()
}

func renderMarkdownSweep(runs []BenchRun) string {
	if len(runs) == 0 {
		return ""
	}

	dbNames := make([]string, 0, len(runs[0].Instances))
	for _, inst := range runs[0].Instances {
		dbNames = append(dbNames, inst.Wrapper.Name())
	}

	var sb strings.Builder
	sb.WriteString("# unified_bench sweep\n\n")
	sb.WriteString(fmt.Sprintf("- keys: %s\n", formatKeyCounts(runs)))
	sb.WriteString(fmt.Sprintf("- valsize: %d\n", runs[0].Config.ValueSize))
	sb.WriteString(fmt.Sprintf("- batchsize: %d\n", runs[0].Config.BatchSize))
	sb.WriteString(fmt.Sprintf("- range-queries: %d\n", runs[0].Config.RangeQueries))
	sb.WriteString(fmt.Sprintf("- range-span: %d\n", runs[0].Config.RangeSpan))
	sb.WriteString("\n")

	for _, testName := range runs[0].TestOrder {
		sb.WriteString("## ")
		sb.WriteString(runs[0].DisplayNames[testName])
		sb.WriteString("\n\n")
		sb.WriteString(renderMarkdownTestSweep(testName, runs, dbNames))
		sb.WriteString("\n")
	}

	return sb.String()
}

func formatKeyCounts(runs []BenchRun) string {
	parts := make([]string, 0, len(runs))
	for _, r := range runs {
		parts = append(parts, formatInt(r.Config.Keys))
	}
	return strings.Join(parts, ", ")
}

func renderMarkdownTestSweep(testName string, runs []BenchRun, dbNames []string) string {
	var sb strings.Builder

	sb.WriteString("| keys |")
	for _, db := range dbNames {
		sb.WriteString(" ")
		sb.WriteString(db)
		sb.WriteString(" |")
	}
	sb.WriteString("\n")

	sb.WriteString("|---:|")
	for range dbNames {
		sb.WriteString("---:|")
	}
	sb.WriteString("\n")

	for _, run := range runs {
		sb.WriteString("| ")
		sb.WriteString(formatInt(run.Config.Keys))
		sb.WriteString(" |")

		maxVal := math.NaN()
		for _, db := range dbNames {
			v := run.Results[testName][db]
			if math.IsNaN(v) {
				continue
			}
			if math.IsNaN(maxVal) || v > maxVal {
				maxVal = v
			}
		}

		for _, db := range dbNames {
			v := run.Results[testName][db]
			cell := formatMarkdownValue(v)
			if !math.IsNaN(maxVal) && !math.IsNaN(v) && v == maxVal {
				cell = "**" + cell + "**"
			}
			sb.WriteString(" ")
			sb.WriteString(cell)
			sb.WriteString(" |")
		}
		sb.WriteString("\n")
	}

	return sb.String()
}

func formatMarkdownValue(f float64) string {
	if math.IsNaN(f) {
		return "—"
	}
	return formatFloat(f)
}

func formatInt(v int) string { return formatFloat(float64(v)) }

func runReadmeSuite(baseCfg BenchConfig) (string, error) {
	outDir := strings.TrimSpace(*outDirArg)

	keyCounts := []int{100000, 1000000}
	if strings.TrimSpace(*keyCountsArg) != "" || strings.TrimSpace(*keyScaleArg) != "" {
		var err error
		keyCounts, err = resolveKeyCounts(baseCfg.Keys, *keyCountsArg, *keyScaleArg, *keysMin, *keysMax)
		if err != nil {
			return "", err
		}
	}

	pointCfg := baseCfg
	pointCfg.DBsArg = "hashdb,treedb,badger,leveldb"
	pointCfg.TestsArg = "sequential_write,random_write,random_read"
	pointCfg.Progress = false

	scanCfg := baseCfg
	scanCfg.DBsArg = "treedb,treedbbackend,badger,leveldb"
	scanCfg.TestsArg = "batch_write,full_scan,prefix_scan"
	scanCfg.Progress = false

	// TreeDBBackend is a useful baseline for point ops, but too slow to include
	// in larger sweeps. Run it once at a moderately sized keycount so the numbers
	// are representative without dominating runtime.
	backendBaselineKeys := 10_000
	if len(keyCounts) > 0 && keyCounts[len(keyCounts)-1] < backendBaselineKeys {
		backendBaselineKeys = keyCounts[len(keyCounts)-1]
	}
	if backendBaselineKeys <= 0 {
		backendBaselineKeys = 1
	}

	backendBaselineCfg := baseCfg
	backendBaselineCfg.Keys = backendBaselineKeys
	backendBaselineCfg.DBsArg = "treedbbackend"
	backendBaselineCfg.TestsArg = "sequential_write,random_write,random_read"
	backendBaselineCfg.Progress = false

	pointRuns, err := runSweep(pointCfg, keyCounts)
	if err != nil {
		return "", err
	}
	scanRuns, err := runSweep(scanCfg, keyCounts)
	if err != nil {
		return "", err
	}
	backendBaseline, err := runBenchmark(backendBaselineCfg)
	if err != nil {
		return "", err
	}

	generatedAt := time.Now().UTC()
	env := getHostInfo()

	var (
		pointOpsPlotPath  string
		batchScanPlotPath string
	)
	if outDir != "" {
		pointOpsPlotPath, batchScanPlotPath, err = writeReadmePlots(outDir, pointRuns, scanRuns)
		if err != nil {
			return "", err
		}
	}

	var sb strings.Builder
	sb.WriteString("_Generated by `go run ./cmd/unified_bench -suite readme -format markdown`._\n\n")
	sb.WriteString(fmt.Sprintf("_Generated at:_ %s\n", generatedAt.Format(time.RFC3339)))
	sb.WriteString(fmt.Sprintf("_Environment:_ %s\n", env.MarkdownSummary()))
	sb.WriteString(fmt.Sprintf("_Seed:_ %d\n\n", baseCfg.SeedUsed))
	sb.WriteString(fmt.Sprintf("_Key counts:_ %s (valsize=%d, batchsize=%d, range-queries=%d, range-span=%d)\n\n",
		formatInt(keyCounts[0])+"…"+formatInt(keyCounts[len(keyCounts)-1]),
		baseCfg.ValueSize, baseCfg.BatchSize, baseCfg.RangeQueries, baseCfg.RangeSpan))

	sb.WriteString("Notes:\n")
	sb.WriteString("- Results depend on hardware and OS.\n")
	sb.WriteString(fmt.Sprintf("- `TreeDBBackend` (uncached) point ops are shown only at %s keys (baseline) and excluded from larger sweeps.\n", formatInt(backendBaseline.Config.Keys)))
	sb.WriteString("- `HashDB` does not support ordered scans.\n\n")

	if pointOpsPlotPath != "" && batchScanPlotPath != "" {
		sb.WriteString("### Graphs\n\n")
		sb.WriteString(fmt.Sprintf("![Unified bench: point ops scaling](%s)\n\n", pointOpsPlotPath))
		sb.WriteString(fmt.Sprintf("![Unified bench: batch + scans scaling](%s)\n\n", batchScanPlotPath))
	}

	sb.WriteString("### Point Ops (writes + gets)\n\n")
	sb.WriteString(renderMarkdownSuiteSection(pointRuns))

	sb.WriteString("\n### TreeDBBackend baseline (point ops)\n\n")
	sb.WriteString(renderMarkdownBaseline(backendBaseline))

	sb.WriteString("\n### Batch + Scans\n\n")
	sb.WriteString(renderMarkdownSuiteSection(scanRuns))

	sb.WriteString("\n### Quick takeaways\n\n")
	sb.WriteString("- `HashDB`: great for high-throughput point reads/writes; no ordered scan API yet.\n")
	sb.WriteString("- `TreeDB` (cached): strong default for random-write-heavy workloads; scans include merge overhead.\n")
	sb.WriteString("- `TreeDBBackend` (uncached): best when you batch writes yourself; slow for per-key writes.\n")
	sb.WriteString("- `Badger`/`LevelDB`: useful baselines with different storage tradeoffs.\n")

	return sb.String(), nil
}

func runChurnSuite(baseCfg BenchConfig) (string, error) {
	// Default churn suite parameters (override via regular flags like -keys, -valsize).
	cfg := baseCfg
	cfg.Progress = false
	cfg.DBsArg = "treedb,leveldb"
	cfg.TestsArg = "random_write,random_delete,random_write,full_scan,prefix_scan"
	cfg.SettleBeforeScans = true

	run, err := runBenchmark(cfg)
	if err != nil {
		return "", err
	}
	return renderMarkdownSingle(run), nil
}

func runChurnVacuumSuite(baseCfg BenchConfig) (string, error) {
	// Churn + settled scans, then VACUUM and scan again on the same dataset.
	cfg := baseCfg
	cfg.Progress = false
	cfg.DBsArg = "treedb,leveldb"
	cfg.TestsArg = "random_write,random_delete,random_write,full_scan,prefix_scan,vacuum_index,full_scan2,prefix_scan2"
	cfg.SettleBeforeScans = true

	run, err := runBenchmark(cfg)
	if err != nil {
		return "", err
	}
	return renderMarkdownSingle(run), nil
}

func runChurnMaintSuite(baseCfg BenchConfig) (string, error) {
	// Churn + settled scans, then value-log compaction + VACUUM and scan again.
	cfg := baseCfg
	cfg.Progress = false
	cfg.DBsArg = "treedb,leveldb"
	cfg.TestsArg = "random_write,random_delete,random_write,full_scan,prefix_scan,compact_slabs,vacuum_index,full_scan2,prefix_scan2"
	cfg.SettleBeforeScans = true

	// Ensure compaction has permissive defaults so the suite is effective even if
	// the caller didn't pass compaction flags.
	if cfg.TreeDBCompactDeadRatio == 0 {
		cfg.TreeDBCompactDeadRatio = 0.10
	}
	if cfg.TreeDBCompactMinBytes == 0 {
		cfg.TreeDBCompactMinBytes = 1
	}
	if cfg.TreeDBCompactMicroBatch == 0 {
		cfg.TreeDBCompactMicroBatch = 256
	}

	run, err := runBenchmark(cfg)
	if err != nil {
		return "", err
	}
	return renderMarkdownSingle(run), nil
}

func renderMarkdownSuiteSection(runs []BenchRun) string {
	if len(runs) == 0 {
		return ""
	}

	dbNames := make([]string, 0, len(runs[0].Instances))
	for _, inst := range runs[0].Instances {
		dbNames = append(dbNames, inst.Wrapper.Name())
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("_Key counts:_ %s\n\n", formatKeyCounts(runs)))

	for _, testName := range runs[0].TestOrder {
		sb.WriteString("#### ")
		sb.WriteString(runs[0].DisplayNames[testName])
		sb.WriteString("\n\n")
		sb.WriteString(renderMarkdownTestSweep(testName, runs, dbNames))
		sb.WriteString("\n")
	}
	return sb.String()
}

func renderMarkdownBaseline(run BenchRun) string {
	if len(run.Instances) == 0 {
		return ""
	}
	dbName := run.Instances[0].Wrapper.Name()

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("_Key count:_ %s\n\n", formatInt(run.Config.Keys)))
	sb.WriteString("| Test | ")
	sb.WriteString(dbName)
	sb.WriteString(" |\n")
	sb.WriteString("|---|---:|\n")
	for _, testName := range run.TestOrder {
		sb.WriteString("| ")
		sb.WriteString(run.DisplayNames[testName])
		sb.WriteString(" | ")
		sb.WriteString(formatMarkdownValue(run.Results[testName][dbName]))
		sb.WriteString(" |\n")
	}
	return sb.String()
}

func testSeed(seed int64, testName string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(testName))
	return seed ^ int64(h.Sum64())
}

func printResultsTable(instances []*DBInstance, finalTestOrder []string, displayNames map[string]string, results map[string]map[string]float64) {
	// Dynamically determine column widths based on content
	colNames := []string{"Test"}
	for _, inst := range instances {
		colNames = append(colNames, inst.Wrapper.Name())
	}

	colWidths := make(map[string]int)
	for _, colName := range colNames {
		colWidths[colName] = len(colName) // Start with header length
	}

	// Update widths based on test names (using display names)
	for _, testName := range finalTestOrder {
		dispName := displayNames[testName]
		if len(dispName) > colWidths["Test"] {
			colWidths["Test"] = len(dispName)
		}
	}

	// Update widths based on results (or "-" for not-yet-run)
	for _, testName := range finalTestOrder {
		for _, inst := range instances {
			dbName := inst.Wrapper.Name()
			valStr := formatFloat(results[testName][dbName])
			if len(valStr) > colWidths[dbName] {
				colWidths[dbName] = len(valStr)
			}
		}
	}

	// Print Header
	headerRow := fmt.Sprintf("%*s", colWidths["Test"], "Test")
	for _, inst := range instances {
		dbName := inst.Wrapper.Name()
		headerRow += fmt.Sprintf("  %*s", colWidths[dbName], dbName) // Right-align DB names for consistency with data
	}
	fmt.Println(headerRow)

	// Print Separator
	separatorRow := fmt.Sprintf("%*s", colWidths["Test"], strings.Repeat("-", colWidths["Test"]))
	for _, inst := range instances {
		dbName := inst.Wrapper.Name()
		separatorRow += fmt.Sprintf("  %*s", colWidths[dbName], strings.Repeat("-", colWidths[dbName]))
	}
	fmt.Println(separatorRow)

	// Print Data Rows
	for _, testName := range finalTestOrder {
		dataRow := fmt.Sprintf("%*s", colWidths["Test"], displayNames[testName])
		for _, inst := range instances {
			dbName := inst.Wrapper.Name()
			val := results[testName][dbName]
			dataRow += fmt.Sprintf("  %*s", colWidths[dbName], formatFloat(val)) // Right-align values
		}
		fmt.Println(dataRow)
	}
}

func parseList(s string) []string {
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.ToLower(strings.TrimSpace(parts[i]))
	}
	return parts
}

func normalizeTests(list []string) []string {
	if contains(list, "all") {
		return list
	}
	seen := make(map[string]struct{}, len(list))
	out := make([]string, 0, len(list))
	for _, t := range list {
		switch t {
		case "full_scan":
			// keep
		case "scan":
			t = "full_scan"
		case "range_scan":
			t = "prefix_scan"
		case "prefix_scan":
			// keep
		case "write_seq":
			t = "sequential_write"
		case "write_rand":
			t = "random_write"
		case "read_rand":
			t = "random_read"
		case "delete_rand":
			t = "random_delete"
		case "batch_write_random":
			t = "batch_random"
		case "batch_write_small_seq":
			t = "batch_small_seq"
		}
		if t == "" {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}

func contains(list []string, item string) bool {
	for _, s := range list {
		if s == item {
			return true
		}
	}
	return false
}

func containsAny(list []string, items ...string) bool {
	for _, item := range items {
		if contains(list, item) {
			return true
		}
	}
	return false
}

type liveTable struct {
	w              io.Writer
	instances      []*DBInstance
	finalTestOrder []string
	displayNames   map[string]string
	printedLines   int
	enabledVT100   bool
	colWidths      map[string]int
	dbColStart     map[string]int
	testRowIndex   map[string]int
}

func newLiveTable(w io.Writer, instances []*DBInstance, finalTestOrder []string, displayNames map[string]string) *liveTable {
	enabledVT100 := false
	if w == os.Stderr {
		if fi, err := os.Stderr.Stat(); err == nil {
			enabledVT100 = (fi.Mode() & os.ModeCharDevice) != 0
		}
	}

	return &liveTable{
		w:              w,
		instances:      instances,
		finalTestOrder: finalTestOrder,
		displayNames:   displayNames,
		enabledVT100:   enabledVT100,
	}
}

func (t *liveTable) Render(results map[string]map[string]float64) error {
	table, colWidths, dbColStart, testRowIndex := renderResultsTableStringWithLayout(t.instances, t.finalTestOrder, t.displayNames, results)
	lines := 2 + len(t.finalTestOrder) // header + separator + rows

	if t.printedLines == 0 {
		_, err := fmt.Fprint(t.w, table)
		if err != nil {
			return err
		}
		t.printedLines = lines
		t.colWidths = colWidths
		t.dbColStart = dbColStart
		t.testRowIndex = testRowIndex
		return nil
	}

	// Once printed, we only do cell updates; re-render is a no-op.
	return nil
}

func (t *liveTable) Clear() error {
	if t.printedLines == 0 {
		return nil
	}
	if !t.enabledVT100 {
		return nil
	}
	// Move cursor back up over the previously printed table and clear to end of screen.
	_, err := fmt.Fprintf(t.w, "\r\x1b[%dA\x1b[J", t.printedLines)
	return err
}

func (t *liveTable) UpdateCell(testName, dbName string, val float64) error {
	if t.printedLines == 0 {
		return nil
	}
	rowIdx, ok := t.testRowIndex[testName]
	if !ok {
		return nil
	}
	colStart, ok := t.dbColStart[dbName]
	if !ok {
		return nil
	}
	colWidth, ok := t.colWidths[dbName]
	if !ok {
		return nil
	}

	cell := fmt.Sprintf("%*s", colWidth, formatFloat(val))

	if !t.enabledVT100 {
		// Fallback: emit a simple progress line rather than trying to "page" in non-TTY output.
		_, err := fmt.Fprintf(t.w, "%s / %s = %s\n", t.displayNames[testName], dbName, strings.TrimSpace(cell))
		return err
	}

	// Table layout:
	// line 1: header
	// line 2: separator
	// line 3..: rows (in finalTestOrder order)
	targetLineFromTop := 3 + rowIdx

	// Save cursor, jump to target cell, write, restore cursor.
	// Cursor is currently below the table after initial Render().
	_, err := fmt.Fprintf(t.w, "\x1b7\r\x1b[%dA\x1b[%dB\x1b[%dC%s\x1b8",
		t.printedLines,      // up to top
		targetLineFromTop-1, // down to row
		colStart-1,          // right to col
		cell,
	)
	return err
}

func renderResultsTableStringWithLayout(instances []*DBInstance, finalTestOrder []string, displayNames map[string]string, results map[string]map[string]float64) (table string, colWidths map[string]int, dbColStart map[string]int, testRowIndex map[string]int) {
	// Dynamically determine column widths based on content
	colNames := []string{"Test"}
	for _, inst := range instances {
		colNames = append(colNames, inst.Wrapper.Name())
	}

	colWidths = make(map[string]int)
	for _, colName := range colNames {
		colWidths[colName] = len(colName)
	}

	for _, testName := range finalTestOrder {
		dispName := displayNames[testName]
		if len(dispName) > colWidths["Test"] {
			colWidths["Test"] = len(dispName)
		}
	}

	// Minimum width so early placeholder "-" columns don't shrink.
	const minValWidth = 13
	for _, inst := range instances {
		dbName := inst.Wrapper.Name()
		if colWidths[dbName] < minValWidth {
			colWidths[dbName] = minValWidth
		}
	}

	// Update widths based on known results so far.
	for _, testName := range finalTestOrder {
		for _, inst := range instances {
			dbName := inst.Wrapper.Name()
			valStr := formatFloat(results[testName][dbName])
			if len(valStr) > colWidths[dbName] {
				colWidths[dbName] = len(valStr)
			}
		}
	}

	var sb strings.Builder

	dbColStart = make(map[string]int, len(instances))
	// First DB column starts after the Test column plus two spaces.
	pos := colWidths["Test"] + 3 // 1-based index
	for _, inst := range instances {
		dbName := inst.Wrapper.Name()
		dbColStart[dbName] = pos
		pos += colWidths[dbName] + 2
	}

	testRowIndex = make(map[string]int, len(finalTestOrder))
	for i, tn := range finalTestOrder {
		testRowIndex[tn] = i
	}

	// Header
	headerRow := fmt.Sprintf("%*s", colWidths["Test"], "Test")
	for _, inst := range instances {
		dbName := inst.Wrapper.Name()
		headerRow += fmt.Sprintf("  %*s", colWidths[dbName], dbName)
	}
	sb.WriteString(headerRow)
	sb.WriteString("\n")

	// Separator
	separatorRow := fmt.Sprintf("%*s", colWidths["Test"], strings.Repeat("-", colWidths["Test"]))
	for _, inst := range instances {
		dbName := inst.Wrapper.Name()
		separatorRow += fmt.Sprintf("  %*s", colWidths[dbName], strings.Repeat("-", colWidths[dbName]))
	}
	sb.WriteString(separatorRow)
	sb.WriteString("\n")

	// Rows
	for _, testName := range finalTestOrder {
		dataRow := fmt.Sprintf("%*s", colWidths["Test"], displayNames[testName])
		for _, inst := range instances {
			dbName := inst.Wrapper.Name()
			val := results[testName][dbName]
			dataRow += fmt.Sprintf("  %*s", colWidths[dbName], formatFloat(val))
		}
		sb.WriteString(dataRow)
		sb.WriteString("\n")
	}

	return sb.String(), colWidths, dbColStart, testRowIndex
}

// formatFloat formats a float with commas (e.g. 1,234,567)
func formatFloat(f float64) string {
	if math.IsNaN(f) {
		return "-"
	}
	s := fmt.Sprintf("%.0f", f)
	if f == 0 {
		return "0"
	}
	n := len(s)
	if n <= 3 {
		return s
	}
	var sb strings.Builder
	remainder := n % 3
	if remainder == 0 {
		remainder = 3
	}
	sb.WriteString(s[:remainder])
	for i := remainder; i < n; i += 3 {
		sb.WriteString(",")
		sb.WriteString(s[i : i+3])
	}
	return sb.String()
}
