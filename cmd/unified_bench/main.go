package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/snissn/gomap/HashDB"
	btreeonhashdb "github.com/snissn/gomap/HashDB/BTreeOnHashDB"
	treedb "github.com/snissn/gomap/TreeDB"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// --- Interfaces ---

// GenericIterator defines a common interface for iterators
type GenericIterator interface {
	Valid() bool
	Next()
	Key() []byte
	Value() []byte
	Close() error
	Error() error
}

// BatchInterface defines a common interface for batches
type BatchInterface interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	Commit() error
	Close() error
}

// DBInterface defines the common benchmark interface
type DBInterface interface {
	Name() string
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Close() error
	SupportsScan() bool
	Iterator(start, end []byte) (GenericIterator, error)
	SupportsBatch() bool
	NewBatch() (BatchInterface, error)
}

// --- Wrappers ---

// 1. HashDB Wrapper
type HashDBBatch struct {
	bw *hashdb.BatchWriter
}

func (b *HashDBBatch) Set(key, value []byte) error {
	return b.bw.Add(key, value)
}
func (b *HashDBBatch) Delete(key []byte) error {
	return errors.New("HashDB batch delete not supported")
}
func (b *HashDBBatch) Commit() error {
	return b.bw.Flush()
}
func (b *HashDBBatch) Close() error {
	// BatchWriter doesn't need explicit close, but we ensure flush in Commit
	return nil
}

type HashDBWrapper struct {
	m *hashdb.HashDB
}

func NewHashDB(dir string) (*HashDBWrapper, error) {
	m, err := hashdb.Open(dir)
	if err != nil {
		return nil, err
	}
	return &HashDBWrapper{m: m}, nil
}

func (g *HashDBWrapper) Name() string                 { return "HashDB" }
func (g *HashDBWrapper) Set(k, v []byte) error        { return g.m.Put(k, v) }
func (g *HashDBWrapper) Get(k []byte) ([]byte, error) { return g.m.Get(k) }
func (g *HashDBWrapper) Delete(k []byte) error        { return g.m.Delete(k) }
func (g *HashDBWrapper) Close() error                 { return g.m.Close() }
func (g *HashDBWrapper) SupportsScan() bool           { return false }
func (g *HashDBWrapper) Iterator(start, end []byte) (GenericIterator, error) {
	return nil, errors.New("HashDB does not support scan")
}
func (g *HashDBWrapper) SupportsBatch() bool { return true }
func (g *HashDBWrapper) NewBatch() (BatchInterface, error) {
	// Use global batchSize flag if possible, or default
	bs := 1000
	if batchSize != nil {
		bs = *batchSize
	}
	return &HashDBBatch{bw: hashdb.NewBatchWriter(g.m, bs)}, nil
}

// 2. BTree Wrapper
type BTreeWrapper struct {
	t *btreeonhashdb.Tree
	m *hashdb.HashDB
}

func NewBTree(dir string) (*BTreeWrapper, error) {
	m, err := hashdb.Open(dir)
	if err != nil {
		return nil, err
	}
	t, err := btreeonhashdb.NewTreeOnHashDB(m, "bench")
	if err != nil {
		return nil, err
	}
	return &BTreeWrapper{t: t, m: m}, nil
}

func (b *BTreeWrapper) Name() string                 { return "BTree" }
func (b *BTreeWrapper) Set(k, v []byte) error        { return b.t.Put(k, v) }
func (b *BTreeWrapper) Get(k []byte) ([]byte, error) { return b.t.Get(k) }
func (b *BTreeWrapper) Delete(k []byte) error        { return b.t.Delete(k) }
func (b *BTreeWrapper) Close() error                 { return b.m.Close() }
func (b *BTreeWrapper) SupportsScan() bool           { return false }
func (b *BTreeWrapper) Iterator(start, end []byte) (GenericIterator, error) {
	return nil, errors.New("BTree does not support scan via public API")
}
func (b *BTreeWrapper) SupportsBatch() bool { return false }
func (b *BTreeWrapper) NewBatch() (BatchInterface, error) {
	return nil, errors.New("BTree does not support batch")
}

// 3. TreeDB Wrapper (cached; recommended)
type treeDBBatch interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	Write() error
	Close() error
}

type TreeDBBatchWrapper struct {
	b treeDBBatch
}

func (w *TreeDBBatchWrapper) Set(k, v []byte) error { return w.b.Set(k, v) }
func (w *TreeDBBatchWrapper) Delete(k []byte) error { return w.b.Delete(k) }
func (w *TreeDBBatchWrapper) Commit() error         { return w.b.Write() }
func (w *TreeDBBatchWrapper) Close() error          { return w.b.Close() }

type TreeDBWrapper struct {
	db *treedb.DB
}

func NewTreeDB(dir string) (*TreeDBWrapper, error) {
	opts := treedb.Options{Dir: dir, ChunkSize: 64 * 1024 * 1024, FlushThreshold: 4 * 1024 * 1024}
	db, err := treedb.Open(opts)
	if err != nil {
		return nil, err
	}
	return &TreeDBWrapper{db: db}, nil
}

func (t *TreeDBWrapper) Name() string                 { return "TreeDB" }
func (t *TreeDBWrapper) Set(k, v []byte) error        { return t.db.Set(k, v) }
func (t *TreeDBWrapper) Get(k []byte) ([]byte, error) { return t.db.Get(k) }
func (t *TreeDBWrapper) Delete(k []byte) error        { return t.db.Delete(k) }
func (t *TreeDBWrapper) Close() error                 { return t.db.Close() }
func (t *TreeDBWrapper) SupportsScan() bool           { return true }
func (t *TreeDBWrapper) Iterator(start, end []byte) (GenericIterator, error) {
	return t.db.Iterator(start, end)
}
func (t *TreeDBWrapper) SupportsBatch() bool { return true }
func (t *TreeDBWrapper) NewBatch() (BatchInterface, error) {
	return &TreeDBBatchWrapper{b: t.db.NewBatch()}, nil
}

// 4. TreeDB Backend Wrapper (uncached; for comparison)
type TreeDBBackendWrapper struct {
	db *treedb.DB
}

func NewTreeDBBackend(dir string) (*TreeDBBackendWrapper, error) {
	opts := treedb.Options{Dir: dir, ChunkSize: 64 * 1024 * 1024}
	db, err := treedb.OpenBackend(opts)
	if err != nil {
		return nil, err
	}
	return &TreeDBBackendWrapper{db: db}, nil
}

func (t *TreeDBBackendWrapper) Name() string                 { return "TreeDBBackend" }
func (t *TreeDBBackendWrapper) Set(k, v []byte) error        { return t.db.Set(k, v) }
func (t *TreeDBBackendWrapper) Get(k []byte) ([]byte, error) { return t.db.Get(k) }
func (t *TreeDBBackendWrapper) Delete(k []byte) error        { return t.db.Delete(k) }
func (t *TreeDBBackendWrapper) Close() error                 { return t.db.Close() }
func (t *TreeDBBackendWrapper) SupportsScan() bool           { return true }
func (t *TreeDBBackendWrapper) Iterator(start, end []byte) (GenericIterator, error) {
	return t.db.Iterator(start, end)
}
func (t *TreeDBBackendWrapper) SupportsBatch() bool { return true }
func (t *TreeDBBackendWrapper) NewBatch() (BatchInterface, error) {
	return &TreeDBBatchWrapper{b: t.db.NewBatch()}, nil
}

// 5. Badger Wrapper
type BadgerBatch struct {
	wb *badger.WriteBatch
}

func (b *BadgerBatch) Set(key, value []byte) error { return b.wb.Set(key, value) }
func (b *BadgerBatch) Delete(key []byte) error     { return b.wb.Delete(key) }
func (b *BadgerBatch) Commit() error               { return b.wb.Flush() }
func (b *BadgerBatch) Close() error                { b.wb.Cancel(); return nil }

type BadgerIterator struct {
	txn     *badger.Txn
	it      *badger.Iterator
	end     []byte
	keyBuf  []byte
	valBuf  []byte
	lastErr error
}

func (i *BadgerIterator) Valid() bool {
	if !i.it.Valid() {
		return false
	}
	if i.end == nil {
		return true
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
func (i *BadgerIterator) Close() error {
	i.it.Close()
	i.txn.Discard()
	return nil
}
func (i *BadgerIterator) Error() error { return i.lastErr }

type BadgerWrapper struct {
	db *badger.DB
}

func NewBadger(dir string) (*BadgerWrapper, error) {
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
func (b *BadgerWrapper) Close() error       { return b.db.Close() }
func (b *BadgerWrapper) SupportsScan() bool { return true }
func (b *BadgerWrapper) Iterator(start, end []byte) (GenericIterator, error) {
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
func (b *BadgerWrapper) SupportsBatch() bool { return true }
func (b *BadgerWrapper) NewBatch() (BatchInterface, error) {
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
func (b *LevelDBBatch) Close() error {
	b.batch.Reset()
	return nil
}

type LevelDBIterator struct {
	it iterator.Iterator
}

func (i *LevelDBIterator) Valid() bool   { return i.it.Valid() }
func (i *LevelDBIterator) Next()         { i.it.Next() }
func (i *LevelDBIterator) Key() []byte   { return i.it.Key() }
func (i *LevelDBIterator) Value() []byte { return i.it.Value() }
func (i *LevelDBIterator) Close() error  { i.it.Release(); return nil }
func (i *LevelDBIterator) Error() error  { return i.it.Error() }

type LevelDBWrapper struct {
	db *leveldb.DB
}

func NewLevelDB(dir string) (*LevelDBWrapper, error) {
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
func (l *LevelDBWrapper) SupportsScan() bool           { return true }
func (l *LevelDBWrapper) Iterator(start, end []byte) (GenericIterator, error) {
	var slice *util.Range
	if start != nil || end != nil {
		slice = &util.Range{Start: start, Limit: end}
	}
	it := l.db.NewIterator(slice, nil)
	it.First()
	return &LevelDBIterator{it: it}, nil
}
func (l *LevelDBWrapper) SupportsBatch() bool { return true }
func (l *LevelDBWrapper) NewBatch() (BatchInterface, error) {
	return &LevelDBBatch{batch: new(leveldb.Batch), db: l.db}, nil
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
	testsArg     = flag.String("tests", "all", "Comma-separated list of tests (write_seq,read_rand,write_rand,delete_rand,full_scan,prefix_scan,batch_write); aliases: scan->full_scan, range_scan->prefix_scan")
	formatArg    = flag.String("format", "table", "Output format: table or markdown")
	suiteArg     = flag.String("suite", "", "Named benchmark suite (e.g. readme)")
	outDirArg    = flag.String("outdir", "", "Write plots/results to this directory (used by -suite readme)")
	keepDir      = flag.Bool("keep", false, "Keep data directories after run")
	progress     = flag.Bool("progress", true, "Live-update the results table on stderr (cell-by-cell) while running; final table prints once to stdout")
	seed         = flag.Int64("seed", 1, "PRNG seed for randomized tests (0 = time-based)")
)

type DBInstance struct {
	Wrapper DBInterface
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
}

type BenchRun struct {
	Config       BenchConfig
	Instances    []*DBInstance
	TestOrder    []string
	DisplayNames map[string]string
	Results      map[string]map[string]float64
}

func main() {
	flag.Parse()

	seedUsed := *seed
	if seedUsed == 0 {
		seedUsed = time.Now().UnixNano()
	}
	fmt.Fprintf(os.Stderr, "seed=%d\n", seedUsed)

	baseCfg := BenchConfig{
		Keys:         *numKeys,
		ValueSize:    *valSize,
		BatchSize:    *batchSize,
		RangeQueries: *rangeQueries,
		RangeSpan:    *rangeSpan,
		DBsArg:       *dbsArg,
		TestsArg:     *testsArg,
		KeepDir:      *keepDir,
		Progress:     *progress,
		SeedUsed:     seedUsed,
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
		fmt.Println()
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

	factories := map[string]func(string) (DBInterface, error){
		"hashdb":        func(d string) (DBInterface, error) { return NewHashDB(d) },
		"gomap":         func(d string) (DBInterface, error) { return NewHashDB(d) }, // legacy alias
		"btree":         func(d string) (DBInterface, error) { return NewBTree(d) },
		"treedb":        func(d string) (DBInterface, error) { return NewTreeDB(d) },        // cached (default)
		"treedbcached":  func(d string) (DBInterface, error) { return NewTreeDB(d) },        // legacy alias
		"treedbbackend": func(d string) (DBInterface, error) { return NewTreeDBBackend(d) }, // uncached
		"treedbraw":     func(d string) (DBInterface, error) { return NewTreeDBBackend(d) }, // alias
		"badger":        func(d string) (DBInterface, error) { return NewBadger(d) },
		"leveldb":       func(d string) (DBInterface, error) { return NewLevelDB(d) },
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

		instances = append(instances, &DBInstance{Wrapper: db, Dir: dir})
	}
	if len(instances) == 0 {
		return BenchRun{}, fmt.Errorf("no DBs selected")
	}

	// Define Tests
	type TestFunc func(db DBInterface, rng *rand.Rand) (float64, error)

	prefixScanBase := 0
	testFuncs := map[string]TestFunc{
		"write_seq": func(db DBInterface, _ *rand.Rand) (float64, error) {
			start := time.Now()
			val := make([]byte, cfg.ValueSize)
			var k [8]byte
			for i := 0; i < cfg.Keys; i++ {
				binary.BigEndian.PutUint64(k[:], uint64(i))
				if err := db.Set(k[:], val); err != nil {
					return 0, fmt.Errorf("write_seq: %w", err)
				}
			}
			return float64(cfg.Keys) / time.Since(start).Seconds(), nil
		},
		"write_rand": func(db DBInterface, rng *rand.Rand) (float64, error) {
			start := time.Now()
			val := make([]byte, cfg.ValueSize)
			var k [8]byte
			for i := 0; i < cfg.Keys; i++ {
				binary.BigEndian.PutUint64(k[:], uint64(rng.Intn(cfg.Keys)))
				if err := db.Set(k[:], val); err != nil {
					return 0, fmt.Errorf("write_rand: %w", err)
				}
			}
			return float64(cfg.Keys) / time.Since(start).Seconds(), nil
		},
		"batch_write": func(db DBInterface, _ *rand.Rand) (float64, error) {
			if !db.SupportsBatch() {
				return math.NaN(), nil
			}
			start := time.Now()
			val := make([]byte, cfg.ValueSize)
			total := cfg.Keys
			var k [8]byte
			for i := 0; i < total; i += cfg.BatchSize {
				batch, err := db.NewBatch()
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
		"delete_rand": func(db DBInterface, rng *rand.Rand) (float64, error) {
			start := time.Now()
			var k [8]byte
			for i := 0; i < cfg.Keys; i++ {
				binary.BigEndian.PutUint64(k[:], uint64(rng.Intn(cfg.Keys)))
				_ = db.Delete(k[:])
			}
			return float64(cfg.Keys) / time.Since(start).Seconds(), nil
		},
		"read_rand": func(db DBInterface, rng *rand.Rand) (float64, error) {
			start := time.Now()
			var k [8]byte
			for i := 0; i < cfg.Keys; i++ {
				binary.BigEndian.PutUint64(k[:], uint64(rng.Intn(cfg.Keys)))
				_, _ = db.Get(k[:])
			}
			return float64(cfg.Keys) / time.Since(start).Seconds(), nil
		},
		"full_scan": func(db DBInterface, _ *rand.Rand) (float64, error) {
			if !db.SupportsScan() {
				return math.NaN(), nil
			}
			start := time.Now()
			iter, err := db.Iterator(nil, nil)
			if err != nil {
				return 0, fmt.Errorf("full_scan: iterator: %w", err)
			}
			defer iter.Close()

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
			return float64(count) / time.Since(start).Seconds(), nil
		},
		"prefix_scan": func(db DBInterface, rng *rand.Rand) (float64, error) {
			if !db.SupportsScan() {
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

				iter, err := db.Iterator(startKey, endKey)
				if err != nil {
					return 0, fmt.Errorf("prefix_scan: iterator: %w", err)
				}

				for iter.Valid() {
					_ = iter.Key()
					iter.Next()
					totalItems++
				}
				if err := iter.Error(); err != nil {
					_ = iter.Close()
					return 0, fmt.Errorf("prefix_scan: %w", err)
				}
				_ = iter.Close()
			}
			return float64(totalItems) / time.Since(start).Seconds(), nil
		},
	}

	allTestOrder := []string{"write_seq", "write_rand", "batch_write", "delete_rand", "read_rand", "full_scan", "prefix_scan"}
	displayNames := map[string]string{
		"write_seq":   "Sequential Write",
		"write_rand":  "Random Write",
		"read_rand":   "Random Read",
		"full_scan":   "Full Scan",
		"prefix_scan": "Prefix Scan",
		"batch_write": "Batch Write",
		"delete_rand": "Random Delete",
	}

	finalTestOrder := make([]string, 0)
	if contains(testsToRun, "all") {
		finalTestOrder = allTestOrder
	} else {
		for _, t := range allTestOrder {
			if contains(testsToRun, t) {
				finalTestOrder = append(finalTestOrder, t)
			}
		}
	}
	if len(finalTestOrder) == 0 {
		return BenchRun{}, fmt.Errorf("no tests selected")
	}

	if contains(finalTestOrder, "batch_write") && !contains(finalTestOrder, "write_seq") && !contains(finalTestOrder, "write_rand") {
		prefixScanBase = cfg.Keys
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

	for _, testName := range finalTestOrder {
		fn := testFuncs[testName]
		if fn == nil {
			return BenchRun{}, fmt.Errorf("unknown test: %q", testName)
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
	pointCfg.TestsArg = "write_seq,write_rand,read_rand"
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
	backendBaselineCfg.TestsArg = "write_seq,write_rand,read_rand"
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
