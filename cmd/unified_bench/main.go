package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/snissn/gomap/HashDB"
	btreeonhashdb "github.com/snissn/gomap/HashDB/BTreeOnHashDB"
	treedbcaching "github.com/snissn/gomap/TreeDB/caching"
	treedbdb "github.com/snissn/gomap/TreeDB/db"

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

// 3. TreeDB Wrapper
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
	db *treedbdb.DB
}

func NewTreeDB(dir string) (*TreeDBWrapper, error) {
	opts := treedbdb.Options{
		Dir:       dir,
		ChunkSize: 64 * 1024 * 1024,
	}
	db, err := treedbdb.Open(opts)
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

// 4. TreeDB Cached Wrapper
type TreeDBCachedWrapper struct {
	db *treedbcaching.DB
}

func NewTreeDBCached(dir string) (*TreeDBCachedWrapper, error) {
	opts := treedbdb.Options{
		Dir:       dir,
		ChunkSize: 64 * 1024 * 1024,
	}
	backendDB, err := treedbdb.Open(opts)
	if err != nil {
		return nil, err
	}

	db, err := treedbcaching.Open(dir, backendDB, 4*1024*1024) // 4MB flush threshold
	if err != nil {
		_ = backendDB.Close()
		return nil, err
	}
	return &TreeDBCachedWrapper{db: db}, nil
}

func (t *TreeDBCachedWrapper) Name() string                 { return "TreeDBCached" }
func (t *TreeDBCachedWrapper) Set(k, v []byte) error        { return t.db.Set(k, v) }
func (t *TreeDBCachedWrapper) Get(k []byte) ([]byte, error) { return t.db.Get(k) }
func (t *TreeDBCachedWrapper) Delete(k []byte) error        { return t.db.Delete(k) }
func (t *TreeDBCachedWrapper) Close() error                 { return t.db.Close() }
func (t *TreeDBCachedWrapper) SupportsScan() bool           { return true }
func (t *TreeDBCachedWrapper) Iterator(start, end []byte) (GenericIterator, error) {
	return t.db.Iterator(start, end)
}
func (t *TreeDBCachedWrapper) SupportsBatch() bool { return true }
func (t *TreeDBCachedWrapper) NewBatch() (BatchInterface, error) {
	return &TreeDBBatchWrapper{b: t.db.NewBatch()}, nil
}

// 5. LevelDB Wrapper
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
	dbsArg       = flag.String("dbs", "all", "Comma-separated list of DBs to run (hashdb,btree,treedb,treedbcached,leveldb); 'gomap' is accepted as an alias for 'hashdb'")
	testsArg     = flag.String("tests", "all", "Comma-separated list of tests (write_seq,read_rand,write_rand,delete_rand,scan,batch_write,range_scan)")
	keepDir      = flag.Bool("keep", false, "Keep data directories after run")
	progress     = flag.Bool("progress", true, "Live-update the results table on stderr (cell-by-cell) while running; final table prints once to stdout")
)

type DBInstance struct {
	Wrapper DBInterface
	Dir     string
}

func main() {
	flag.Parse()

	dbsToRun := parseList(*dbsArg)
	testsToRun := parseList(*testsArg)

	// Define Factory Map
	factories := map[string]func(string) (DBInterface, error){
		"hashdb":       func(d string) (DBInterface, error) { return NewHashDB(d) },
		"gomap":        func(d string) (DBInterface, error) { return NewHashDB(d) }, // legacy alias
		"btree":        func(d string) (DBInterface, error) { return NewBTree(d) },
		"treedb":       func(d string) (DBInterface, error) { return NewTreeDB(d) },
		"treedbcached": func(d string) (DBInterface, error) { return NewTreeDBCached(d) },
		"leveldb":      func(d string) (DBInterface, error) { return NewLevelDB(d) },
	}

	// 1. Initialize DBs
	instances := make([]*DBInstance, 0)
	// Order matching dbsArg or default hardcoded order if "all"
	orderedDBs := []string{"hashdb", "btree", "treedb", "treedbcached", "leveldb"}
	if *dbsArg != "all" {
		orderedDBs = dbsToRun
	}

	for _, name := range orderedDBs {
		if !contains(dbsToRun, name) && !contains(dbsToRun, "all") {
			continue
		}
		factory, ok := factories[name]
		if !ok {
			log.Printf("Unknown DB: %s", name)
			continue
		}

		dir, err := os.MkdirTemp("", "bench-"+name+"*")
		if err != nil {
			log.Fatalf("Temp dir failed: %v", err)
		}

		db, err := factory(dir)
		if err != nil {
			log.Printf("Failed to init %s: %v", name, err)
			os.RemoveAll(dir)
			continue
		}

		instances = append(instances, &DBInstance{Wrapper: db, Dir: dir})
	}

	// 2. Define Tests
	// Map of TestName -> Function
	type TestFunc func(db DBInterface) float64

	testFuncs := map[string]TestFunc{
		"write_seq": func(db DBInterface) float64 {
			start := time.Now()
			val := make([]byte, *valSize)
			var k [8]byte // Stack allocation
			for i := 0; i < *numKeys; i++ {
				binary.BigEndian.PutUint64(k[:], uint64(i))
				if err := db.Set(k[:], val); err != nil {
					log.Fatalf("write_seq error: %v", err)
				}
			}
			return float64(*numKeys) / time.Since(start).Seconds()
		},
		"read_rand": func(db DBInterface) float64 {
			start := time.Now()
			var k [8]byte
			for i := 0; i < *numKeys; i++ {
				binary.BigEndian.PutUint64(k[:], uint64(rand.Intn(*numKeys)))
				if _, err := db.Get(k[:]); err != nil {
					// ignore
				}
			}
			return float64(*numKeys) / time.Since(start).Seconds()
		},
		"scan": func(db DBInterface) float64 {
			if !db.SupportsScan() {
				return 0
			}
			start := time.Now()
			iter, err := db.Iterator(nil, nil)
			if err != nil {
				log.Fatalf("scan iterator error: %v", err)
			}
			defer iter.Close()
			count := 0
			for iter.Valid() {
				_ = iter.Key()
				_ = iter.Value()
				iter.Next()
				count++
			}
			if iter.Error() != nil {
				log.Fatalf("scan iter error: %v", iter.Error())
			}
			return float64(count) / time.Since(start).Seconds()
		},
		"range_scan": func(db DBInterface) float64 {
			if !db.SupportsScan() {
				return 0
			}
			start := time.Now()
			for i := 0; i < *rangeQueries; i++ {
				startIdx := rand.Intn(*numKeys)
				endIdx := startIdx + *rangeSpan
				if endIdx > *numKeys {
					endIdx = *numKeys
				}

				var startKeyBuf [8]byte
				binary.BigEndian.PutUint64(startKeyBuf[:], uint64(startIdx))
				startKey := startKeyBuf[:]

				var endKeyBuf [8]byte
				binary.BigEndian.PutUint64(endKeyBuf[:], uint64(endIdx))
				endKey := endKeyBuf[:]

				iter, err := db.Iterator(startKey, endKey)
				if err != nil {
					log.Fatalf("range_scan iterator error: %v", err)
				}

				count := 0
				for iter.Valid() {
					iter.Next()
					count++
				}
				iter.Close()
			}
			return float64(*rangeQueries) / time.Since(start).Seconds()
		},
		"write_rand": func(db DBInterface) float64 {
			start := time.Now()
			val := make([]byte, *valSize)
			var k [8]byte
			for i := 0; i < *numKeys; i++ {
				binary.BigEndian.PutUint64(k[:], uint64(rand.Intn(*numKeys)))
				if err := db.Set(k[:], val); err != nil {
					log.Fatalf("write_rand error: %v", err)
				}
			}
			return float64(*numKeys) / time.Since(start).Seconds()
		},
		"batch_write": func(db DBInterface) float64 {
			if !db.SupportsBatch() {
				return 0
			}
			start := time.Now()
			val := make([]byte, *valSize)
			total := *numKeys
			var k [8]byte
			for i := 0; i < total; i += *batchSize {
				batch, err := db.NewBatch()
				if err != nil {
					log.Fatalf("new batch error: %v", err)
				}
				end := i + *batchSize
				if end > total {
					end = total
				}
				for j := i; j < end; j++ {
					// Use new keys for batch to stress growth/append?
					// Or overwrite?
					// Let's use high range keys to append.
					binary.BigEndian.PutUint64(k[:], uint64(j+*numKeys))
					batch.Set(k[:], val)
				}
				if err := batch.Commit(); err != nil {
					log.Fatalf("batch commit error: %v", err)
				}
				batch.Close()
			}
			return float64(total) / time.Since(start).Seconds()
		},
		"delete_rand": func(db DBInterface) float64 {
			start := time.Now()
			var k [8]byte
			for i := 0; i < *numKeys; i++ {
				binary.BigEndian.PutUint64(k[:], uint64(rand.Intn(*numKeys)))
				if err := db.Delete(k[:]); err != nil {
					// ignore
				}
			}
			return float64(*numKeys) / time.Since(start).Seconds()
		},
	}

	// Ordered list of tests to run
	allTestOrder := []string{"write_seq", "write_rand", "batch_write", "delete_rand", "read_rand", "scan", "range_scan"}
	finalTestOrder := make([]string, 0)

	// Display names for polished output
	displayNames := map[string]string{
		"write_seq":   "Sequential Write",
		"write_rand":  "Random Write",
		"read_rand":   "Random Read",
		"scan":        "Scan",
		"range_scan":  "Range Scan",
		"batch_write": "Batch Write",
		"delete_rand": "Random Delete",
	}

	if contains(testsToRun, "all") {
		finalTestOrder = allTestOrder
	} else {
		// Filter based on input but keep logical order if possible?
		// Or strictly follow input order?
		// User said "loop over tests and run one test after another".
		// We'll follow input order if explicit, otherwise default order filtered.
		if len(testsToRun) > 0 && testsToRun[0] != "all" {
			// Actually, let's respect the standard order for consistency in dependencies
			// (e.g. read after write), unless user explicitly asks for subset.
			// If user asks for "read_rand,write_seq", we might have issues if read runs first on empty DB.
			// But we initialized DBs once. They persist.
			// So if user asks "read_rand" first, it will read 0 keys (or not founds).
			// We trust user or enforce standard order.
			// Let's enforce standard order intersection.
			for _, t := range allTestOrder {
				if contains(testsToRun, t) {
					finalTestOrder = append(finalTestOrder, t)
				}
			}
		} else {
			finalTestOrder = allTestOrder
		}
	}

	// 3. Run Tests
	// map[TestName][DBName]Result
	results := make(map[string]map[string]float64)
	for _, t := range finalTestOrder {
		results[t] = make(map[string]float64)
	}

	var live *liveTable
	if *progress {
		live = newLiveTable(os.Stderr, instances, finalTestOrder, displayNames)
		_ = live.Render(results) // placeholder table
	}

	for _, testName := range finalTestOrder {
		fn := testFuncs[testName]

		for _, inst := range instances {
			// Run Test
			// Reset? No, we persist state.
			res := fn(inst.Wrapper)
			results[testName][inst.Wrapper.Name()] = res
			if live != nil {
				_ = live.UpdateCell(testName, inst.Wrapper.Name(), res)
			}
		}
	}

	// 4. Cleanup
	for _, inst := range instances {
		inst.Wrapper.Close()
		if !*keepDir {
			os.RemoveAll(inst.Dir)
		}
	}

	// 5. Print final transposed table once to stdout
	if live != nil {
		_ = live.Clear()
	}
	fmt.Println()
	printResultsTable(instances, finalTestOrder, displayNames, results)
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
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
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
	s := fmt.Sprintf("%.0f", f)
	if f == 0 { // Explicitly handle 0 for non-supported tests
		return "-"
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
