package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/kvstore"
)

type errorBatchReaderDB struct {
	readBatchCalls int
}

type fixedNameDB struct {
	name string
}

type statsOnlyDB struct {
	fixedNameDB
	stats map[string]string
}

func (d *fixedNameDB) Name() string {
	return d.name
}

func (d *statsOnlyDB) Stats() map[string]string {
	out := make(map[string]string, len(d.stats))
	for k, v := range d.stats {
		out[k] = v
	}
	return out
}

func (d *fixedNameDB) Close() error {
	return nil
}

func (d *fixedNameDB) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (d *fixedNameDB) Set(key, value []byte) error {
	return nil
}

func (d *fixedNameDB) Delete(key []byte) error {
	return nil
}

func (d *errorBatchReaderDB) Name() string {
	return "ReadBatchSentinel"
}

func (d *errorBatchReaderDB) Close() error {
	return nil
}

func (d *errorBatchReaderDB) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (d *errorBatchReaderDB) Set(key, value []byte) error {
	return nil
}

func (d *errorBatchReaderDB) Delete(key []byte) error {
	return nil
}

func (d *errorBatchReaderDB) ReadBatch(keys [][]byte) error {
	d.readBatchCalls++
	return errors.New("readbatch should not be called for random_read_batch")
}

func (d *errorBatchReaderDB) GetMany(keys [][]byte) ([][]byte, error) {
	return make([][]byte, len(keys)), nil
}

type errorGetManyDB struct {
	err error
}

type errorGetManyViewDB struct {
	err error
}

func (d *errorGetManyDB) Name() string {
	return "ErrGetMany"
}

func (d *errorGetManyDB) Close() error {
	return nil
}

func (d *errorGetManyDB) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (d *errorGetManyDB) Set(key, value []byte) error {
	return nil
}

func (d *errorGetManyDB) Delete(key []byte) error {
	return nil
}

func (d *errorGetManyDB) GetMany(keys [][]byte) ([][]byte, error) {
	return nil, d.err
}

func (d *errorGetManyViewDB) Name() string {
	return "ErrGetManyView"
}

func (d *errorGetManyViewDB) Close() error {
	return nil
}

func (d *errorGetManyViewDB) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (d *errorGetManyViewDB) Set(key, value []byte) error {
	return nil
}

func (d *errorGetManyViewDB) Delete(key []byte) error {
	return nil
}

func (d *errorGetManyViewDB) GetManyView(keys [][]byte, fn kvstore.MultiGetViewFunc) error {
	return d.err
}

type errorGetDB struct {
	err error
}

func (d *errorGetDB) Name() string {
	return "ErrGet"
}

func (d *errorGetDB) Close() error {
	return nil
}

func (d *errorGetDB) Get(key []byte) ([]byte, error) {
	return nil, d.err
}

func (d *errorGetDB) Set(key, value []byte) error {
	return nil
}

func (d *errorGetDB) Delete(key []byte) error {
	return nil
}

type checkpointCountingDB struct {
	checkpointCalls int
}

func (d *checkpointCountingDB) Name() string {
	return "CheckpointCountingDB"
}

func (d *checkpointCountingDB) Close() error {
	return nil
}

func (d *checkpointCountingDB) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (d *checkpointCountingDB) Set(key, value []byte) error {
	return nil
}

func (d *checkpointCountingDB) Delete(key []byte) error {
	return nil
}

func (d *checkpointCountingDB) Checkpoint() error {
	d.checkpointCalls++
	return nil
}

type closeStatsDB struct {
	name          string
	checkpointErr error
	closeErr      error
	checkpointed  atomic.Bool
	closed        atomic.Bool
}

type checkpointStatsDB struct {
	fixedNameDB
	checkpointCalls atomic.Int64
}

type activeFlushStatsDB struct {
	fixedNameDB
	statsCalls atomic.Int64
}

func (d *closeStatsDB) Name() string { return d.name }

func (d *closeStatsDB) Close() error {
	d.closed.Store(true)
	return d.closeErr
}

func (d *closeStatsDB) Checkpoint() error {
	if d.checkpointErr != nil {
		return d.checkpointErr
	}
	d.checkpointed.Store(true)
	return nil
}

func (d *closeStatsDB) Get(key []byte) ([]byte, error) { return nil, nil }

func (d *closeStatsDB) Set(key, value []byte) error { return nil }

func (d *closeStatsDB) Delete(key []byte) error { return nil }

func (d *closeStatsDB) Stats() map[string]string {
	stats := map[string]string{
		"treedb.test.final_checkpoint_total": "0",
	}
	if d.checkpointed.Load() {
		stats["treedb.test.final_checkpoint_total"] = "1"
	}
	if d.closed.Load() {
		stats["treedb.publish.ordered_root_delta_group.root_apply_calls_total"] = "1"
		return stats
	}
	stats["treedb.publish.ordered_root_delta_group.root_apply_calls_total"] = "0"
	return stats
}

func (d *checkpointStatsDB) Checkpoint() error {
	d.checkpointCalls.Add(1)
	return nil
}

func (d *checkpointStatsDB) Stats() map[string]string {
	calls := d.checkpointCalls.Load()
	return map[string]string{
		"treedb.cache.queue_len":                                                       "0",
		"treedb.cache.queue_backlog_bytes":                                             "0",
		"treedb.cache.flush_apply.coordinator.active":                                  "0",
		"treedb.cache.flush_apply.coordinator.active_workers":                          "0",
		"treedb.cache.flush_apply.coordinator.in_flight_bytes":                         "0",
		"treedb.cache.checkpoint.active_background_flush_wait_ns_last":                 fmt.Sprintf("%d", calls*10),
		"treedb.cache.checkpoint.wait.frontier_units_at_request_last":                  fmt.Sprintf("%d", calls),
		"treedb.cache.checkpoint.stage.flush_all.total_ns":                             fmt.Sprintf("%d", calls*100),
		"treedb.flush_apply.span_native.fallback.reason.close_or_checkpoint.ops_total": fmt.Sprintf("%d", calls),
	}
}

func (d *activeFlushStatsDB) Stats() map[string]string {
	calls := d.statsCalls.Add(1)
	active := "0"
	activeWorkers := "0"
	inFlightBytes := "0"
	if calls == 1 {
		active = "1"
		activeWorkers = "1"
		inFlightBytes = "1024"
	}
	return map[string]string{
		"treedb.cache.queue_len":                               "0",
		"treedb.cache.queue_backlog_bytes":                     "0",
		"treedb.cache.flush_apply.coordinator.active":          active,
		"treedb.cache.flush_apply.coordinator.active_workers":  activeWorkers,
		"treedb.cache.flush_apply.coordinator.in_flight_bytes": inFlightBytes,
	}
}

type settleProbeDB struct {
	id       int
	probe    *settleProbeState
	setCalls atomic.Int64
}

type settleProbeState struct {
	nextID                  atomic.Int64
	writerClosedBeforeRead  atomic.Bool
	readAfterWriterCloseCnt atomic.Int64
}

func (d *settleProbeDB) Name() string { return "SettleProbe" }

func (d *settleProbeDB) Close() error {
	if d.setCalls.Load() > 0 {
		d.probe.writerClosedBeforeRead.Store(true)
	}
	return nil
}

func (d *settleProbeDB) Get(key []byte) ([]byte, error) {
	if d.probe.writerClosedBeforeRead.Load() {
		d.probe.readAfterWriterCloseCnt.Add(1)
	}
	return nil, nil
}

func (d *settleProbeDB) Set(key, value []byte) error {
	d.setCalls.Add(1)
	return nil
}

func (d *settleProbeDB) Delete(key []byte) error { return nil }

type preferGetManyDB struct {
	getCalls     int
	getManyCalls int
}

type preferGetManyViewDB struct {
	getCalls         int
	getManyCalls     int
	getManyViewCalls int
}

type scanViewOnlyDB struct {
	fixedNameDB
	entries        []scanViewEntry
	keyCalls       int
	valueCalls     int
	keyCopyCalls   int
	valueCopyCalls int
}

type scanViewEntry struct {
	key   []byte
	value []byte
}

type scanViewOnlyIterator struct {
	db      *scanViewOnlyDB
	entries []scanViewEntry
	idx     int
}

func (d *scanViewOnlyDB) Set(key, value []byte) error {
	d.entries = append(d.entries, scanViewEntry{
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
	})
	return nil
}

func (d *scanViewOnlyDB) Iterator(start, end []byte) (kvstore.Iterator, error) {
	sorted := append([]scanViewEntry(nil), d.entries...)
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i].key, sorted[j].key) < 0
	})
	filtered := sorted[:0]
	for _, entry := range sorted {
		if start != nil && bytes.Compare(entry.key, start) < 0 {
			continue
		}
		if end != nil && bytes.Compare(entry.key, end) >= 0 {
			continue
		}
		filtered = append(filtered, entry)
	}
	return &scanViewOnlyIterator{db: d, entries: filtered}, nil
}

func (d *scanViewOnlyDB) ReverseIterator(start, end []byte) (kvstore.Iterator, error) {
	return nil, kvstore.ErrUnsupported
}

func (it *scanViewOnlyIterator) Valid() bool { return it.idx < len(it.entries) }
func (it *scanViewOnlyIterator) Next()       { it.idx++ }
func (it *scanViewOnlyIterator) Key() []byte {
	it.db.keyCalls++
	return it.entries[it.idx].key
}
func (it *scanViewOnlyIterator) Value() []byte {
	it.db.valueCalls++
	return it.entries[it.idx].value
}
func (it *scanViewOnlyIterator) KeyCopy(dst []byte) []byte {
	it.db.keyCopyCalls++
	return append(dst[:0], it.entries[it.idx].key...)
}
func (it *scanViewOnlyIterator) ValueCopy(dst []byte) []byte {
	it.db.valueCopyCalls++
	return append(dst[:0], it.entries[it.idx].value...)
}
func (it *scanViewOnlyIterator) Error() error { return nil }
func (it *scanViewOnlyIterator) Close() error { return nil }

type batchDeleteRangeMemoryDB struct {
	fixedNameDB
	entries          map[string]scanViewEntry
	deleteRangeCalls int
	noopDeleteRange  bool
	rangeDeleteMode  string
	events           *[]string
}

type batchDeleteRangeMemoryBatch struct {
	db      *batchDeleteRangeMemoryDB
	sets    []scanViewEntry
	deletes [][]byte
	ranges  []batchDeleteRangeMemoryRange
}

type batchDeleteRangeMemoryRange struct {
	start []byte
	end   []byte
}

type batchDeleteRangeMemoryIterator struct {
	entries []scanViewEntry
	idx     int
}

type batchWithoutRangeDB struct {
	fixedNameDB
	newBatchCalls int
	setCalls      int
}

type batchWithoutRangeBatch struct {
	db *batchWithoutRangeDB
}

func newBatchDeleteRangeMemoryDB(name string) *batchDeleteRangeMemoryDB {
	return &batchDeleteRangeMemoryDB{
		fixedNameDB: fixedNameDB{name: name},
		entries:     make(map[string]scanViewEntry),
	}
}

func (d *batchDeleteRangeMemoryDB) Set(key, value []byte) error {
	d.entries[string(key)] = scanViewEntry{key: append([]byte(nil), key...), value: append([]byte(nil), value...)}
	return nil
}

func (d *batchDeleteRangeMemoryDB) Get(key []byte) ([]byte, error) {
	entry, ok := d.entries[string(key)]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), entry.value...), nil
}

func (d *batchDeleteRangeMemoryDB) Delete(key []byte) error {
	delete(d.entries, string(key))
	return nil
}

func (d *batchDeleteRangeMemoryDB) NewBatch() (kvstore.Batch, error) {
	return &batchDeleteRangeMemoryBatch{db: d}, nil
}

func (d *batchDeleteRangeMemoryDB) RangeDeleteMode() string {
	if d.rangeDeleteMode != "" {
		return d.rangeDeleteMode
	}
	return kvstore.RangeDeleteModeNative
}

func (d *batchDeleteRangeMemoryDB) Iterator(start, end []byte) (kvstore.Iterator, error) {
	entries := make([]scanViewEntry, 0, len(d.entries))
	for _, entry := range d.entries {
		if start != nil && bytes.Compare(entry.key, start) < 0 {
			continue
		}
		if end != nil && bytes.Compare(entry.key, end) >= 0 {
			continue
		}
		entries = append(entries, scanViewEntry{key: append([]byte(nil), entry.key...), value: append([]byte(nil), entry.value...)})
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})
	return &batchDeleteRangeMemoryIterator{entries: entries}, nil
}

func (d *batchDeleteRangeMemoryDB) ReverseIterator(start, end []byte) (kvstore.Iterator, error) {
	return nil, kvstore.ErrUnsupported
}

func (b *batchDeleteRangeMemoryBatch) Set(key, value []byte) error {
	if b.db.events != nil {
		*b.db.events = append(*b.db.events, "set")
	}
	b.sets = append(b.sets, scanViewEntry{key: append([]byte(nil), key...), value: append([]byte(nil), value...)})
	return nil
}

func (b *batchDeleteRangeMemoryBatch) Delete(key []byte) error {
	b.deletes = append(b.deletes, append([]byte(nil), key...))
	return nil
}

func (b *batchDeleteRangeMemoryBatch) DeleteRange(start, end []byte) error {
	b.db.deleteRangeCalls++
	if b.db.events != nil {
		*b.db.events = append(*b.db.events, "delete_range")
	}
	if b.db.noopDeleteRange {
		return nil
	}
	b.ranges = append(b.ranges, batchDeleteRangeMemoryRange{
		start: append([]byte(nil), start...),
		end:   append([]byte(nil), end...),
	})
	return nil
}

func (b *batchDeleteRangeMemoryBatch) Commit() error {
	for _, entry := range b.sets {
		b.db.entries[string(entry.key)] = scanViewEntry{key: append([]byte(nil), entry.key...), value: append([]byte(nil), entry.value...)}
	}
	for _, key := range b.deletes {
		delete(b.db.entries, string(key))
	}
	for _, r := range b.ranges {
		for key, entry := range b.db.entries {
			if r.start != nil && bytes.Compare(entry.key, r.start) < 0 {
				continue
			}
			if r.end != nil && bytes.Compare(entry.key, r.end) >= 0 {
				continue
			}
			delete(b.db.entries, key)
		}
	}
	return nil
}

func (b *batchDeleteRangeMemoryBatch) CommitSync() error { return b.Commit() }
func (b *batchDeleteRangeMemoryBatch) Close() error      { return nil }

func (it *batchDeleteRangeMemoryIterator) Valid() bool { return it.idx < len(it.entries) }
func (it *batchDeleteRangeMemoryIterator) Next()       { it.idx++ }
func (it *batchDeleteRangeMemoryIterator) Key() []byte { return it.entries[it.idx].key }
func (it *batchDeleteRangeMemoryIterator) Value() []byte {
	return it.entries[it.idx].value
}
func (it *batchDeleteRangeMemoryIterator) KeyCopy(dst []byte) []byte {
	return append(dst[:0], it.Key()...)
}
func (it *batchDeleteRangeMemoryIterator) ValueCopy(dst []byte) []byte {
	return append(dst[:0], it.Value()...)
}
func (it *batchDeleteRangeMemoryIterator) Error() error { return nil }
func (it *batchDeleteRangeMemoryIterator) Close() error { return nil }

func (d *batchWithoutRangeDB) NewBatch() (kvstore.Batch, error) {
	d.newBatchCalls++
	return &batchWithoutRangeBatch{db: d}, nil
}

func (b *batchWithoutRangeBatch) Set(key, value []byte) error {
	b.db.setCalls++
	return nil
}

func (b *batchWithoutRangeBatch) Delete(key []byte) error { return nil }
func (b *batchWithoutRangeBatch) Commit() error           { return nil }
func (b *batchWithoutRangeBatch) CommitSync() error       { return nil }
func (b *batchWithoutRangeBatch) Close() error            { return nil }

type countingReadSnapshotDB struct {
	getCalls               atomic.Int64
	setCalls               atomic.Int64
	acquireSnapshotCalls   atomic.Int64
	snapshotGetCalls       atomic.Int64
	snapshotGetAppendCalls atomic.Int64
	snapshotCloseCalls     atomic.Int64
}

type countingReadSnapshot struct {
	parent *countingReadSnapshotDB
}

func (d *countingReadSnapshotDB) Name() string {
	return "SnapshotCounter"
}

func (d *countingReadSnapshotDB) Close() error {
	return nil
}

func (d *countingReadSnapshotDB) Get(key []byte) ([]byte, error) {
	d.getCalls.Add(1)
	return nil, nil
}

func (d *countingReadSnapshotDB) Set(key, value []byte) error {
	d.setCalls.Add(1)
	return nil
}

func (d *countingReadSnapshotDB) Delete(key []byte) error {
	return nil
}

func (d *countingReadSnapshotDB) AcquireReadSnapshot() (kvstore.ReadSnapshot, error) {
	d.acquireSnapshotCalls.Add(1)
	return &countingReadSnapshot{parent: d}, nil
}

func (s *countingReadSnapshot) Get(key []byte) ([]byte, error) {
	s.parent.snapshotGetCalls.Add(1)
	return nil, nil
}

func (s *countingReadSnapshot) GetAppend(key, dst []byte) ([]byte, error) {
	s.parent.snapshotGetAppendCalls.Add(1)
	return dst, nil
}

func (s *countingReadSnapshot) Close() error {
	s.parent.snapshotCloseCalls.Add(1)
	return nil
}

func (d *preferGetManyDB) Name() string {
	return "PreferGetMany"
}

func (d *preferGetManyDB) Close() error {
	return nil
}

func (d *preferGetManyDB) Get(key []byte) ([]byte, error) {
	d.getCalls++
	return nil, errors.New("get should not be called when GetMany is available")
}

func (d *preferGetManyDB) Set(key, value []byte) error {
	return nil
}

func (d *preferGetManyDB) Delete(key []byte) error {
	return nil
}

func (d *preferGetManyDB) GetMany(keys [][]byte) ([][]byte, error) {
	d.getManyCalls++
	return make([][]byte, len(keys)), nil
}

func (d *preferGetManyViewDB) Name() string {
	return "PreferGetManyView"
}

func (d *preferGetManyViewDB) Close() error {
	return nil
}

func (d *preferGetManyViewDB) Get(key []byte) ([]byte, error) {
	d.getCalls++
	return nil, errors.New("get should not be called when GetManyView is available")
}

func (d *preferGetManyViewDB) Set(key, value []byte) error {
	return nil
}

func (d *preferGetManyViewDB) Delete(key []byte) error {
	return nil
}

func (d *preferGetManyViewDB) GetMany(keys [][]byte) ([][]byte, error) {
	d.getManyCalls++
	return nil, errors.New("GetMany should not be called when GetManyView is available")
}

func (d *preferGetManyViewDB) GetManyView(keys [][]byte, fn kvstore.MultiGetViewFunc) error {
	d.getManyViewCalls++
	for i, key := range keys {
		if err := fn(i, key, nil, false); err != nil {
			return err
		}
	}
	return nil
}

func runRandomReadBatchErrorCase(t *testing.T, dbName string, factory DBFactory, want error) {
	t.Helper()
	RegisterHiddenDB(dbName, factory)

	_, err := runBenchmark(BenchConfig{
		Keys:         256,
		ValueSize:    16,
		BatchSize:    64,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       dbName,
		TestsArg:     "random_read_batch",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err == nil {
		t.Fatalf("expected random_read_batch error wrapping %v, got nil", want)
	}
	if !errors.Is(err, want) {
		t.Fatalf("expected random_read_batch error wrapping %v, got %v", want, err)
	}
	if !strings.Contains(err.Error(), "random_read_batch") {
		t.Fatalf("expected random_read_batch context in error, got %v", err)
	}
}

func TestRunBenchmark_PreloadsForReadAndScanOnly(t *testing.T) {
	run, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "read_rand,full_scan,prefix_scan",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}

	full := run.Results["full_scan"]["TreeDB"]
	prefix := run.Results["prefix_scan"]["TreeDB"]
	if math.IsNaN(full) || full <= 0 {
		t.Fatalf("expected full_scan > 0, got %v", full)
	}
	if math.IsNaN(prefix) || prefix <= 0 {
		t.Fatalf("expected prefix_scan > 0, got %v", prefix)
	}
}

func TestRunBenchmark_ScansUseIteratorViewsNotCopies(t *testing.T) {
	var db *scanViewOnlyDB
	const dbName = "scan_view_only"
	RegisterHiddenDB(dbName, func(_ string) (kvstore.DB, error) {
		db = &scanViewOnlyDB{fixedNameDB: fixedNameDB{name: "ScanViewOnly"}}
		return db, nil
	})

	run, err := runBenchmark(BenchConfig{
		Keys:         64,
		ValueSize:    16,
		BatchSize:    16,
		RangeQueries: 8,
		RangeSpan:    4,
		DBsArg:       dbName,
		TestsArg:     "full_scan,prefix_scan",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if db == nil {
		t.Fatalf("expected test DB instance")
	}
	if db.keyCopyCalls != 0 || db.valueCopyCalls != 0 {
		t.Fatalf("scan benchmarks should use Key()/Value() views, got KeyCopy=%d ValueCopy=%d", db.keyCopyCalls, db.valueCopyCalls)
	}
	if db.keyCalls == 0 || db.valueCalls == 0 {
		t.Fatalf("expected Key()/Value() view calls, got Key=%d Value=%d", db.keyCalls, db.valueCalls)
	}
	if got := run.Results["full_scan"][db.Name()]; math.IsNaN(got) || got <= 0 {
		t.Fatalf("expected full_scan > 0, got %v", got)
	}
	if got := run.Results["prefix_scan"][db.Name()]; math.IsNaN(got) || got <= 0 {
		t.Fatalf("expected prefix_scan > 0, got %v", got)
	}
}

func TestRunBenchmark_RandomReadBatch_Smoke(t *testing.T) {
	run, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    128,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb,leveldb",
		TestsArg:     "sequential_write,random_read_batch",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}

	for _, dbName := range []string{"TreeDB", "LevelDB"} {
		got := run.Results["random_read_batch"][dbName]
		if math.IsNaN(got) || got <= 0 {
			t.Fatalf("expected random_read_batch > 0 for %s, got %v", dbName, got)
		}
	}
}

func TestRunBenchmark_BatchWriteSteady_Smoke(t *testing.T) {
	run, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    128,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "batch_write_steady",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	got, ok := run.Results["batch_write_steady"]["TreeDB"]
	if !ok {
		t.Fatalf("expected batch_write_steady result for TreeDB")
	}
	if math.IsNaN(got) || got <= 0 {
		t.Fatalf("expected batch_write_steady > 0 for TreeDB, got %v", got)
	}
}

func TestRunBenchmarkBatchWriteSteadyCPUProfileMatchesTimedSpan(t *testing.T) {
	var db *batchDeleteRangeMemoryDB
	var events []string
	const dbName = "batch_write_steady_profile_timed_span"
	RegisterHiddenDB(dbName, func(_ string) (kvstore.DB, error) {
		db = newBatchDeleteRangeMemoryDB("BatchWriteSteadyProfileTimedSpan")
		db.events = &events
		return db, nil
	})

	profileTmpDir := t.TempDir()
	newProfilePath := func(prefix string) (string, error) {
		f, err := os.CreateTemp(profileTmpDir, prefix+"_*.pprof")
		if err != nil {
			return "", err
		}
		path := f.Name()
		if err := f.Close(); err != nil {
			_ = os.Remove(path)
			return "", err
		}
		return path, nil
	}
	profileHooks := &benchmarkProfileHooks{
		startCPUProfile: func(_ io.Writer) error {
			events = append(events, "cpu_start")
			return nil
		},
		stopCPUProfile: func() {
			events = append(events, "cpu_stop")
		},
		writeAllocsSnapshotTemp: func(prefix string) (string, error) {
			events = append(events, prefix)
			return newProfilePath(prefix)
		},
		writeAllocsDeltaProfile: func(_, _, _ string) error {
			events = append(events, "alloc_delta")
			return nil
		},
	}

	_, err := runBenchmark(BenchConfig{
		Keys:          64,
		ValueSize:     8,
		BatchSize:     16,
		DBsArg:        dbName,
		TestsArg:      "batch_write_steady",
		KeepDir:       false,
		Progress:      false,
		SeedUsed:      1,
		CPUProfile:    filepath.Join(t.TempDir(), "cpu"),
		AllocsProfile: filepath.Join(t.TempDir(), "allocs"),
		profileHooks:  profileHooks,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if db == nil {
		t.Fatal("expected batch-write test DB")
	}

	firstIndex := func(target string) int {
		for i, event := range events {
			if event == target {
				return i
			}
		}
		return -1
	}
	lastIndex := func(target string) int {
		for i := len(events) - 1; i >= 0; i-- {
			if events[i] == target {
				return i
			}
		}
		return -1
	}
	allocBase := firstIndex("unified_bench_allocs_base")
	cpuStart := firstIndex("cpu_start")
	firstSet := firstIndex("set")
	lastSet := lastIndex("set")
	cpuStop := firstIndex("cpu_stop")
	allocAfter := firstIndex("unified_bench_allocs_after")
	if allocBase < 0 || cpuStart < 0 || firstSet < 0 || lastSet < 0 || cpuStop < 0 || allocAfter < 0 {
		t.Fatalf("missing expected profile/write events: %v", events)
	}
	if !(allocBase < cpuStart && cpuStart < firstSet && lastSet < cpuStop && cpuStop < allocAfter) {
		t.Fatalf("CPU profile must wrap only the timed batch-write span: %v", events)
	}
}

func TestNormalizeTests_BatchDeleteRangeAliases(t *testing.T) {
	got := normalizeTests(parseList("delete_range,batch_range_delete,batch_delete_range"))
	want := []string{"batch_delete_range"}
	if len(got) != len(want) {
		t.Fatalf("unexpected len: got=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected normalize result: got=%v want=%v", got, want)
		}
	}
}

func TestNormalizeTests_AllWithBatchDeleteRangeAlias(t *testing.T) {
	got := normalizeTests(parseList("all,delete_range,batch_delete_range"))
	want := []string{"all", "batch_delete_range"}
	if len(got) != len(want) {
		t.Fatalf("unexpected len: got=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected normalize result: got=%v want=%v", got, want)
		}
	}
}

func TestRunBenchmark_BatchDeleteRange_UsesBatchCapabilityAndReports(t *testing.T) {
	var db *batchDeleteRangeMemoryDB
	const dbName = "batch_delete_range_memory"
	RegisterHiddenDB(dbName, func(_ string) (kvstore.DB, error) {
		db = newBatchDeleteRangeMemoryDB("BatchDeleteRangeMemory")
		return db, nil
	})

	run, err := runBenchmark(BenchConfig{
		Keys:                      64,
		ValueSize:                 8,
		BatchSize:                 16,
		BatchDeleteRangeWidth:     8,
		BatchDeleteRangesPerBatch: 2,
		BatchDeleteRangeValidate:  true,
		RangeQueries:              0,
		RangeSpan:                 0,
		DBsArg:                    dbName,
		TestsArg:                  "batch_delete_range",
		KeepDir:                   false,
		Progress:                  false,
		SeedUsed:                  1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if db == nil {
		t.Fatalf("expected test DB instance")
	}
	if !contains(run.TestOrder, "batch_delete_range") {
		t.Fatalf("expected batch_delete_range in test order, got %v", run.TestOrder)
	}
	if got, want := db.deleteRangeCalls, 8; got != want {
		t.Fatalf("DeleteRange calls=%d want %d", got, want)
	}
	gotResult := run.Results["batch_delete_range"][db.Name()]
	if math.IsNaN(gotResult) || gotResult <= 0 {
		t.Fatalf("expected batch_delete_range result > 0, got %v", gotResult)
	}
	report := run.BatchDeleteRange["batch_delete_range"][db.Name()]
	if report.Mode != kvstore.RangeDeleteModeNative || report.LoadedKeys != 64 || report.RangeWidth != 8 || report.RangesPerBatch != 2 || report.RangeCount != 8 || report.AffectedKeys != 64 || report.ValueSize != 8 || report.Validation != "passed" {
		t.Fatalf("unexpected batch_delete_range report: %+v", report)
	}
	if report.RangeOpsPerSec != gotResult || report.AffectedKeysPerSec <= report.RangeOpsPerSec {
		t.Fatalf("unexpected throughput metrics: result=%v report=%+v", gotResult, report)
	}
}

func TestRunBenchmark_BatchDeleteRangeDoesNotSuppressPreloadForEarlierReads(t *testing.T) {
	var db *batchDeleteRangeMemoryDB
	const dbName = "batch_delete_range_after_read"
	RegisterHiddenDB(dbName, func(_ string) (kvstore.DB, error) {
		db = newBatchDeleteRangeMemoryDB("BatchDeleteRangeAfterRead")
		return db, nil
	})

	run, err := runBenchmark(BenchConfig{
		Keys:                      64,
		ValueSize:                 8,
		BatchSize:                 16,
		BatchDeleteRangeWidth:     8,
		BatchDeleteRangesPerBatch: 2,
		BatchDeleteRangeValidate:  true,
		ReadRequireHit:            true,
		RangeQueries:              0,
		RangeSpan:                 0,
		DBsArg:                    dbName,
		TestsArg:                  "random_read,batch_delete_range",
		KeepDir:                   false,
		Progress:                  false,
		SeedUsed:                  1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if db == nil || db.deleteRangeCalls == 0 {
		t.Fatalf("expected explicit batch_delete_range to run after preloaded read, db=%v", db)
	}
	got := run.Results["random_read"][db.Name()]
	if math.IsNaN(got) || got <= 0 {
		t.Fatalf("expected preloaded random_read > 0 before batch_delete_range, got %v", got)
	}
}

func TestRunBenchmark_BatchDeleteRange_SkipsUnsupportedBeforeLoad(t *testing.T) {
	var db *batchWithoutRangeDB
	const dbName = "batch_without_range"
	RegisterHiddenDB(dbName, func(_ string) (kvstore.DB, error) {
		db = &batchWithoutRangeDB{fixedNameDB: fixedNameDB{name: "BatchWithoutRange"}}
		return db, nil
	})

	run, err := runBenchmark(BenchConfig{
		Keys:                      64,
		ValueSize:                 8,
		BatchSize:                 16,
		BatchDeleteRangeWidth:     8,
		BatchDeleteRangesPerBatch: 2,
		RangeQueries:              0,
		RangeSpan:                 0,
		DBsArg:                    dbName,
		TestsArg:                  "batch_delete_range",
		KeepDir:                   false,
		Progress:                  false,
		SeedUsed:                  1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if db == nil {
		t.Fatalf("expected test DB instance")
	}
	if db.newBatchCalls != 1 {
		t.Fatalf("expected only capability probe batch, got %d NewBatch calls", db.newBatchCalls)
	}
	if db.setCalls != 0 {
		t.Fatalf("unsupported DeleteRange batch should not preload keys, got %d Set calls", db.setCalls)
	}
	if got := run.Results["batch_delete_range"][db.Name()]; !math.IsNaN(got) {
		t.Fatalf("unsupported DeleteRange batch result=%v want NaN", got)
	}
	if report := run.BatchDeleteRange["batch_delete_range"][db.Name()]; report.Mode != "" {
		t.Fatalf("unsupported DeleteRange batch should not report metrics, got %+v", report)
	}
}

func TestRunBenchmark_BatchDeleteRange_WrappersSmoke(t *testing.T) {
	run, err := runBenchmark(BenchConfig{
		Keys:                      256,
		ValueSize:                 16,
		BatchSize:                 32,
		BatchDeleteRangeWidth:     16,
		BatchDeleteRangesPerBatch: 4,
		BatchDeleteRangeValidate:  true,
		RangeQueries:              0,
		RangeSpan:                 0,
		DBsArg:                    "treedb,pebble,leveldb",
		TestsArg:                  "batch_delete_range",
		KeepDir:                   false,
		Progress:                  false,
		SeedUsed:                  1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	wantModes := map[string]string{
		"TreeDB":  kvstore.RangeDeleteModeNative,
		"Pebble":  kvstore.RangeDeleteModeNative,
		"LevelDB": kvstore.RangeDeleteModeFallbackIteratorDelete,
	}
	for dbName, wantMode := range wantModes {
		got := run.Results["batch_delete_range"][dbName]
		if math.IsNaN(got) || got <= 0 {
			t.Fatalf("expected batch_delete_range > 0 for %s, got %v", dbName, got)
		}
		report := run.BatchDeleteRange["batch_delete_range"][dbName]
		if report.Mode != wantMode {
			t.Fatalf("%s mode=%q want %q (report=%+v)", dbName, report.Mode, wantMode, report)
		}
		if report.AffectedKeysPerSec <= 0 || report.RangeOpsPerSec <= 0 || report.Validation != "passed" {
			t.Fatalf("%s missing throughput/validation report: %+v", dbName, report)
		}
	}
}

func TestRunBenchmark_BatchDeleteRangeValidationCatchesFailedDeletes(t *testing.T) {
	var db *batchDeleteRangeMemoryDB
	const dbName = "batch_delete_range_noop"
	RegisterHiddenDB(dbName, func(_ string) (kvstore.DB, error) {
		db = newBatchDeleteRangeMemoryDB("BatchDeleteRangeNoop")
		db.noopDeleteRange = true
		return db, nil
	})

	_, err := runBenchmark(BenchConfig{
		Keys:                      32,
		ValueSize:                 8,
		BatchSize:                 16,
		BatchDeleteRangeWidth:     8,
		BatchDeleteRangesPerBatch: 2,
		BatchDeleteRangeValidate:  true,
		RangeQueries:              0,
		RangeSpan:                 0,
		DBsArg:                    dbName,
		TestsArg:                  "batch_delete_range",
		KeepDir:                   false,
		Progress:                  false,
		SeedUsed:                  1,
	})
	if err == nil || !strings.Contains(err.Error(), "batch_delete_range validation") || !strings.Contains(err.Error(), "live key") {
		t.Fatalf("expected validation failure, got %v", err)
	}
	if db == nil || db.deleteRangeCalls == 0 {
		t.Fatalf("expected DeleteRange to be attempted, db=%v", db)
	}
}

func TestBatchDeleteRangeReportingVisibleInMarkdownAndJSON(t *testing.T) {
	var db *batchDeleteRangeMemoryDB
	const dbName = "batch_delete_range_reporting"
	RegisterHiddenDB(dbName, func(_ string) (kvstore.DB, error) {
		db = newBatchDeleteRangeMemoryDB("BatchDeleteRangeReporting")
		db.rangeDeleteMode = kvstore.RangeDeleteModeFallbackIteratorDelete
		return db, nil
	})

	run, err := runBenchmark(BenchConfig{
		Keys:                      64,
		ValueSize:                 8,
		BatchSize:                 16,
		BatchDeleteRangeWidth:     8,
		BatchDeleteRangesPerBatch: 2,
		BatchDeleteRangeValidate:  true,
		RangeQueries:              0,
		RangeSpan:                 0,
		DBsArg:                    dbName,
		TestsArg:                  "batch_delete_range",
		KeepDir:                   false,
		Progress:                  false,
		SeedUsed:                  1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}

	md := renderMarkdownSingle(run)
	for _, want := range []string{"Batch DeleteRange Metrics", "range_ops/s", "affected_keys/s", "fallback_iterator_delete", "batch-delete-range-width"} {
		if !strings.Contains(md, want) {
			t.Fatalf("markdown missing %q:\n%s", want, md)
		}
	}

	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "benchprof_results.json")
	mdPath := filepath.Join(dir, "benchprof_results.md")
	if err := writeBenchprofArtifactsToPaths(jsonPath, mdPath, "native-fastpath", []BenchRun{run}); err != nil {
		t.Fatalf("writeBenchprofArtifactsToPaths: %v", err)
	}
	data, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("read json: %v", err)
	}
	for _, want := range []string{"\"batch_delete_range\"", "\"affected_keys_per_sec\"", "\"ranges_per_batch\"", "fallback_iterator_delete"} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("json missing %q:\n%s", want, data)
		}
	}
}

func TestRenderGethHotKVSummary(t *testing.T) {
	const treeName = "TreeDB (public cached command_wal_v1)"
	run := BenchRun{
		Instances: []*DBInstance{
			{Wrapper: &fixedNameDB{name: treeName}},
			{Wrapper: &fixedNameDB{name: "Pebble"}},
		},
		Results: map[string]map[string]float64{
			"sequential_write": {treeName: 1000, "Pebble": 2000},
			"random_read":      {treeName: 3000, "Pebble": 4000},
			"full_scan":        {treeName: 5000, "Pebble": 6000},
		},
		BatchDeleteRange: map[string]map[string]batchDeleteRangeReport{
			"batch_delete_range": {
				treeName: {AffectedKeysPerSec: 7000},
				"Pebble": {AffectedKeysPerSec: 8000},
			},
		},
		DiskUsage: map[string]dirDiskUsage{
			treeName: {TotalBytes: 9000},
			"Pebble": {TotalBytes: 10000},
		},
	}

	out := renderGethHotKVSummary(run)
	for _, want := range []string{
		"| engine | write ops/sec | read ops/sec | iterate keys/sec | DeleteRange keys/sec | size bytes |",
		"| TreeDB | 1,000 | 3,000 | 5,000 | 7,000 | 9,000 |",
		"| Pebble | 2,000 | 4,000 | 6,000 | 8,000 | 10,000 |",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("summary missing %q:\n%s", want, out)
		}
	}
}

func TestRunGethHotKVSuiteWritesBenchprofArtifacts(t *testing.T) {
	const dbName = "geth_hot_kv_artifacts_memory"
	RegisterHiddenDB(dbName, func(_ string) (kvstore.DB, error) {
		return newBatchDeleteRangeMemoryDB("GethHotKVArtifactsMemory"), nil
	})

	oldProfileDir := *profileDir
	oldPathLabel := *pathLabel
	oldExplicit := explicitFlags
	defer func() {
		*profileDir = oldProfileDir
		*pathLabel = oldPathLabel
		explicitFlags = oldExplicit
	}()

	dir := t.TempDir()
	*profileDir = dir
	*pathLabel = "native-fastpath"
	explicitFlags = map[string]bool{
		"keys":                          true,
		"dbs":                           true,
		"test":                          true,
		"val-pattern":                   true,
		"batch-delete-range-width":      true,
		"batch-delete-ranges-per-batch": true,
		"batch-delete-range-validate":   true,
		"read-require-hit":              true,
	}

	out, err := runGethHotKVSuite(BenchConfig{
		Keys:                      16,
		ValueSize:                 8,
		BatchSize:                 8,
		ValuePattern:              "random",
		DBsArg:                    dbName,
		TestsArg:                  "sequential_write,random_read,full_scan,batch_delete_range",
		BatchDeleteRangeWidth:     4,
		BatchDeleteRangesPerBatch: 2,
		BatchDeleteRangeValidate:  true,
		ReadRequireHit:            true,
		SeedUsed:                  1,
	})
	if err != nil {
		t.Fatalf("runGethHotKVSuite: %v", err)
	}
	if !strings.Contains(out, "# unified_bench suite: geth_hot_kv") {
		t.Fatalf("suite output missing title:\n%s", out)
	}
	for _, name := range []string{"benchprof_results.json", "benchprof_results.md"} {
		path := filepath.Join(dir, name)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected %s: %v", path, err)
		}
	}
}

func TestRunBenchmark_BatchDeleteRangeProfilesExcludePreload(t *testing.T) {
	var db *batchDeleteRangeMemoryDB
	var events []string
	const dbName = "batch_delete_range_profile_excludes_preload"
	RegisterHiddenDB(dbName, func(_ string) (kvstore.DB, error) {
		db = newBatchDeleteRangeMemoryDB("BatchDeleteRangeProfileExcludesPreload")
		db.events = &events
		return db, nil
	})

	profileTmpDir := t.TempDir()
	newProfilePath := func(prefix string) (string, error) {
		f, err := os.CreateTemp(profileTmpDir, prefix+"_*.pprof")
		if err != nil {
			return "", err
		}
		path := f.Name()
		if err := f.Close(); err != nil {
			_ = os.Remove(path)
			return "", err
		}
		return path, nil
	}
	profileHooks := &benchmarkProfileHooks{
		startCPUProfile: func(_ io.Writer) error {
			events = append(events, "cpu_start")
			return nil
		},
		stopCPUProfile: func() {
			events = append(events, "cpu_stop")
		},
		writeAllocsSnapshotTemp: func(prefix string) (string, error) {
			events = append(events, prefix)
			return newProfilePath(prefix)
		},
		writeAllocsDeltaProfile: func(basePath, afterPath, outPath string) error {
			events = append(events, "alloc_delta")
			return nil
		},
	}

	_, err := runBenchmark(BenchConfig{
		Keys:                      64,
		ValueSize:                 8,
		BatchSize:                 16,
		BatchDeleteRangeWidth:     8,
		BatchDeleteRangesPerBatch: 2,
		BatchDeleteRangeValidate:  true,
		RangeQueries:              0,
		RangeSpan:                 0,
		DBsArg:                    dbName,
		TestsArg:                  "batch_delete_range",
		KeepDir:                   false,
		Progress:                  false,
		SeedUsed:                  1,
		CPUProfile:                filepath.Join(t.TempDir(), "cpu"),
		AllocsProfile:             filepath.Join(t.TempDir(), "allocs"),
		profileHooks:              profileHooks,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if db == nil || db.deleteRangeCalls == 0 {
		t.Fatalf("expected DeleteRange calls, db=%v events=%v", db, events)
	}

	firstIndex := func(target string) int {
		for i, event := range events {
			if event == target {
				return i
			}
		}
		return -1
	}
	lastIndex := func(target string) int {
		for i := len(events) - 1; i >= 0; i-- {
			if events[i] == target {
				return i
			}
		}
		return -1
	}
	lastSet := lastIndex("set")
	cpuStart := firstIndex("cpu_start")
	allocBase := firstIndex("unified_bench_allocs_base")
	firstDeleteRange := firstIndex("delete_range")
	if lastSet < 0 || cpuStart < 0 || allocBase < 0 || firstDeleteRange < 0 {
		t.Fatalf("missing expected events: %v", events)
	}
	if lastSet > cpuStart || lastSet > allocBase {
		t.Fatalf("preload Set events should finish before profile starts/baselines, events=%v", events)
	}
	if firstDeleteRange < cpuStart || firstDeleteRange < allocBase {
		t.Fatalf("DeleteRange should run after profile starts/baselines, events=%v", events)
	}
}

func TestRunBenchmark_RandomReadBatch_DoesNotCallReadBatch(t *testing.T) {
	var db *errorBatchReaderDB
	const dbName = "random_read_batch_ignore_readbatch"
	RegisterHiddenDB(dbName, func(_ string) (kvstore.DB, error) {
		db = &errorBatchReaderDB{}
		return db, nil
	})

	run, err := runBenchmark(BenchConfig{
		Keys:         256,
		ValueSize:    16,
		BatchSize:    64,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       dbName,
		TestsArg:     "random_read_batch",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if db == nil {
		t.Fatalf("expected test DB instance")
	}
	if db.readBatchCalls != 0 {
		t.Fatalf("expected ReadBatch to be unused, got %d calls", db.readBatchCalls)
	}
	got := run.Results["random_read_batch"][db.Name()]
	if math.IsNaN(got) || got <= 0 {
		t.Fatalf("expected random_read_batch > 0 for %s, got %v", db.Name(), got)
	}
}

func TestRunBenchmark_RandomReadBatch_PrefersGetManyOverGet(t *testing.T) {
	var db *preferGetManyDB
	const dbName = "random_read_batch_prefer_getmany"
	RegisterHiddenDB(dbName, func(_ string) (kvstore.DB, error) {
		db = &preferGetManyDB{}
		return db, nil
	})

	run, err := runBenchmark(BenchConfig{
		Keys:         257,
		ValueSize:    16,
		BatchSize:    64,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       dbName,
		TestsArg:     "random_read_batch",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if db == nil {
		t.Fatalf("expected test DB instance")
	}
	if db.getCalls != 0 {
		t.Fatalf("expected Get to be unused when GetMany is implemented, got %d calls", db.getCalls)
	}
	if db.getManyCalls == 0 {
		t.Fatalf("expected GetMany to be called at least once")
	}
	got := run.Results["random_read_batch"][db.Name()]
	if math.IsNaN(got) || got <= 0 {
		t.Fatalf("expected random_read_batch > 0 for %s, got %v", db.Name(), got)
	}
}

func TestRunBenchmark_RandomReadBatch_PrefersGetManyViewOverGetMany(t *testing.T) {
	var db *preferGetManyViewDB
	const dbName = "random_read_batch_prefer_getmanyview"
	RegisterHiddenDB(dbName, func(_ string) (kvstore.DB, error) {
		db = &preferGetManyViewDB{}
		return db, nil
	})

	run, err := runBenchmark(BenchConfig{
		Keys:         257,
		ValueSize:    16,
		BatchSize:    64,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       dbName,
		TestsArg:     "random_read_batch",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if db == nil {
		t.Fatalf("expected test DB instance")
	}
	if db.getCalls != 0 || db.getManyCalls != 0 {
		t.Fatalf("expected Get/GetMany to be unused when GetManyView is implemented, get=%d getmany=%d", db.getCalls, db.getManyCalls)
	}
	if db.getManyViewCalls == 0 {
		t.Fatalf("expected GetManyView to be called at least once")
	}
	got := run.Results["random_read_batch"][db.Name()]
	if math.IsNaN(got) || got <= 0 {
		t.Fatalf("expected random_read_batch > 0 for %s, got %v", db.Name(), got)
	}
}

func TestRunBenchmark_RandomReadBatch_PropagatesGetManyViewError(t *testing.T) {
	want := errors.New("getmanyview forced failure")
	runRandomReadBatchErrorCase(t, "random_read_batch_error_db_getmanyview", func(_ string) (kvstore.DB, error) {
		return &errorGetManyViewDB{err: want}, nil
	}, want)
}

func TestRunBenchmark_RandomReadBatch_PropagatesGetManyError(t *testing.T) {
	want := errors.New("getmany forced failure")
	runRandomReadBatchErrorCase(t, "random_read_batch_error_db_getmany", func(_ string) (kvstore.DB, error) {
		return &errorGetManyDB{err: want}, nil
	}, want)
}

func TestRunBenchmark_RandomReadBatch_PropagatesGetError(t *testing.T) {
	want := errors.New("get forced failure")
	runRandomReadBatchErrorCase(t, "random_read_batch_error_db_get", func(_ string) (kvstore.DB, error) {
		return &errorGetDB{err: want}, nil
	}, want)
}

func TestRunBenchmark_RandomReadParallel_Smoke(t *testing.T) {
	run, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    128,
		ReadWorkers:  4,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb,leveldb",
		TestsArg:     "sequential_write,random_read_parallel",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}

	for _, dbName := range []string{"TreeDB", "LevelDB"} {
		got := run.Results["random_read_parallel"][dbName]
		if math.IsNaN(got) || got <= 0 {
			t.Fatalf("expected random_read_parallel > 0 for %s, got %v", dbName, got)
		}
	}
}

func TestRunBenchmark_RandomReadParallelAcquireSnapshot_Smoke(t *testing.T) {
	run, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    128,
		ReadWorkers:  4,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "sequential_write,random_read_parallel_acquire_snapshot",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}

	got := run.Results["random_read_parallel_acquire_snapshot"]["TreeDB"]
	if math.IsNaN(got) || got <= 0 {
		t.Fatalf("expected random_read_parallel_acquire_snapshot > 0 for TreeDB, got %v", got)
	}
}

func TestRunBenchmark_RandomReadParallelAcquireSnapshot_UsesSnapshots(t *testing.T) {
	var db *countingReadSnapshotDB
	const dbName = "random_read_parallel_snapshot_counter"
	RegisterHiddenDB(dbName, func(_ string) (kvstore.DB, error) {
		db = &countingReadSnapshotDB{}
		return db, nil
	})

	cfg := BenchConfig{
		Keys:         512,
		ValueSize:    16,
		BatchSize:    64,
		ReadWorkers:  4,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       dbName,
		TestsArg:     "sequential_write,random_read_parallel_acquire_snapshot",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	}
	run, err := runBenchmark(cfg)
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if db == nil {
		t.Fatalf("expected test DB instance")
	}
	workers := cfg.ReadWorkers
	if workers <= 0 {
		workers = 1
	}
	expectedAcquires := int64(1 + cfg.Keys*workers) // one probe + one per read op
	if got := db.acquireSnapshotCalls.Load(); got != expectedAcquires {
		t.Fatalf("expected AcquireReadSnapshot calls=%d, got=%d", expectedAcquires, got)
	}
	if got := db.snapshotGetAppendCalls.Load(); got == 0 {
		t.Fatalf("expected snapshot GetAppend to be called, got=%d", got)
	}
	if gotGet := db.getCalls.Load(); gotGet != 0 {
		t.Fatalf("expected DB Get to be unused in snapshot benchmark path, got=%d calls", gotGet)
	}
	if gotClose := db.snapshotCloseCalls.Load(); gotClose != expectedAcquires {
		t.Fatalf("expected snapshot closes=%d, got=%d", expectedAcquires, gotClose)
	}

	got := run.Results["random_read_parallel_acquire_snapshot"][db.Name()]
	if math.IsNaN(got) || got <= 0 {
		t.Fatalf("expected random_read_parallel_acquire_snapshot > 0 for %s, got %v", db.Name(), got)
	}
}

func TestRunBenchmark_TreeDBPerfCapturesSnapshotMetrics(t *testing.T) {
	run, err := runBenchmark(BenchConfig{
		Keys:         256,
		ValueSize:    16,
		BatchSize:    64,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       "treedb",
		TestsArg:     "sequential_write,random_read_parallel_acquire_snapshot",
		KeepDir:      false,
		Progress:     false,
		ReadWorkers:  4,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	perTest, ok := run.TreeDBPerf["random_read_parallel_acquire_snapshot"]
	if !ok {
		t.Fatalf("expected TreeDB perf entry for snapshot benchmark")
	}
	perf, ok := perTest["TreeDB"]
	if !ok {
		t.Fatalf("expected TreeDB perf metrics for snapshot benchmark")
	}
	if perf.Snapshot.AcquireCalls == 0 {
		t.Fatalf("expected snapshot acquire calls > 0, got 0")
	}
	if perf.Snapshot.CloseCalls == 0 {
		t.Fatalf("expected snapshot close calls > 0, got 0")
	}
	if perf.Snapshot.AcquireTotalNanos == 0 {
		t.Fatalf("expected snapshot acquire time > 0")
	}
	if perf.Snapshot.CloseTotalNanos == 0 {
		t.Fatalf("expected snapshot close time > 0")
	}
}

func TestNormalizeTests_ReadRandomBatchAliases(t *testing.T) {
	got := normalizeTests(parseList("read_rand_batch,read_random_batch,random_read_batch"))
	want := []string{"random_read_batch"}
	if len(got) != len(want) {
		t.Fatalf("unexpected len: got=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected normalize result: got=%v want=%v", got, want)
		}
	}
}

func TestNormalizeTests_ReadRandomParallelAlias(t *testing.T) {
	got := normalizeTests(parseList("read_rand_parallel,random_read_parallel"))
	want := []string{"random_read_parallel"}
	if len(got) != len(want) {
		t.Fatalf("unexpected len: got=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected normalize result: got=%v want=%v", got, want)
		}
	}
}

func TestRunBenchmark_AllExcludesBatchDeleteRangeAndIncludesRandomReadParallel(t *testing.T) {
	run, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    100,
		ReadWorkers:  2,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb,leveldb",
		TestsArg:     "all",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}

	if contains(run.TestOrder, "batch_delete_range") {
		t.Fatalf("default all suite must not include destructive batch_delete_range before read/scan tests: %v", run.TestOrder)
	}
	for _, want := range []string{"random_read", "random_read_parallel", "random_read_parallel_acquire_snapshot", "random_read_batch", "full_scan", "prefix_scan"} {
		if !contains(run.TestOrder, want) {
			t.Fatalf("default all suite missing %s: %v", want, run.TestOrder)
		}
	}

	for _, dbName := range []string{"TreeDB", "LevelDB"} {
		got, ok := run.Results["random_read_parallel"][dbName]
		if !ok {
			t.Fatalf("expected random_read_parallel result for %s", dbName)
		}
		if math.IsNaN(got) || got <= 0 {
			t.Fatalf("expected random_read_parallel > 0 for %s, got %v", dbName, got)
		}

		gotSnap, ok := run.Results["random_read_parallel_acquire_snapshot"][dbName]
		if !ok {
			t.Fatalf("expected random_read_parallel_acquire_snapshot result for %s", dbName)
		}
		if math.IsNaN(gotSnap) || gotSnap <= 0 {
			t.Fatalf("expected random_read_parallel_acquire_snapshot > 0 for %s, got %v", dbName, gotSnap)
		}
	}
}

func TestRunBenchmark_AllWithExplicitBatchDeleteRangeRunsAfterReadScans(t *testing.T) {
	var db *batchDeleteRangeMemoryDB
	const dbName = "all_plus_batch_delete_range"
	RegisterHiddenDB(dbName, func(_ string) (kvstore.DB, error) {
		db = newBatchDeleteRangeMemoryDB("AllPlusBatchDeleteRange")
		return db, nil
	})

	run, err := runBenchmark(BenchConfig{
		Keys:                      64,
		ValueSize:                 8,
		BatchSize:                 16,
		ReadWorkers:               2,
		RangeQueries:              8,
		RangeSpan:                 4,
		BatchDeleteRangeWidth:     8,
		BatchDeleteRangesPerBatch: 2,
		BatchDeleteRangeValidate:  true,
		DBsArg:                    dbName,
		TestsArg:                  "all,batch_delete_range",
		KeepDir:                   false,
		Progress:                  false,
		SeedUsed:                  1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if db == nil || db.deleteRangeCalls == 0 {
		t.Fatalf("expected explicit batch_delete_range to run, db=%v", db)
	}

	deleteIdx := -1
	for i, testName := range run.TestOrder {
		if testName == "batch_delete_range" {
			deleteIdx = i
			break
		}
	}
	if deleteIdx < 0 {
		t.Fatalf("expected explicit batch_delete_range in order: %v", run.TestOrder)
	}
	for _, readScan := range []string{"random_read", "random_read_parallel", "random_read_parallel_acquire_snapshot", "random_read_batch", "full_scan", "prefix_scan"} {
		idx := -1
		for i, testName := range run.TestOrder {
			if testName == readScan {
				idx = i
				break
			}
		}
		if idx < 0 {
			t.Fatalf("missing %s in all suite: %v", readScan, run.TestOrder)
		}
		if idx > deleteIdx {
			t.Fatalf("%s runs after destructive batch_delete_range: order=%v", readScan, run.TestOrder)
		}
	}
}

func TestRunBenchmark_PrefixScanMatchesBatchWriteKeyRange(t *testing.T) {
	run, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "batch_write,full_scan,prefix_scan",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}

	full := run.Results["full_scan"]["TreeDB"]
	prefix := run.Results["prefix_scan"]["TreeDB"]
	if math.IsNaN(full) || full <= 0 {
		t.Fatalf("expected full_scan > 0, got %v", full)
	}
	if math.IsNaN(prefix) || prefix <= 0 {
		t.Fatalf("expected prefix_scan > 0, got %v", prefix)
	}
}

func TestRunChurnSuite_Smoke(t *testing.T) {
	out, err := runChurnSuite(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "all",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runChurnSuite: %v", err)
	}
	if out == "" {
		t.Fatalf("expected non-empty output")
	}
}

func TestRunChurnVacuumSuite_Smoke(t *testing.T) {
	out, err := runChurnVacuumSuite(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "all",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runChurnVacuumSuite: %v", err)
	}
	if out == "" {
		t.Fatalf("expected non-empty output")
	}
}

func TestRunFlushThrashSuite_Smoke(t *testing.T) {
	out, err := runFlushThrashSuite(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "all",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runFlushThrashSuite: %v", err)
	}
	if out == "" {
		t.Fatalf("expected non-empty output")
	}
}

func TestRunLongMixSuite_Smoke(t *testing.T) {
	out, err := runLongMixSuite(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "all",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runLongMixSuite: %v", err)
	}
	if out == "" {
		t.Fatalf("expected non-empty output")
	}
}

func TestRunBigKeysGuardSuite_Smoke(t *testing.T) {
	out, err := runBigKeysGuardSuite(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "all",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,

		MaxWall: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("runBigKeysGuardSuite: %v", err)
	}
	if out == "" {
		t.Fatalf("expected non-empty output")
	}
}

func TestRunBenchmark_CheckpointBetweenTests_Smoke(t *testing.T) {
	run, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "sequential_write,random_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,

		CheckpointBetweenTests: true,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}

	seq := run.Results["sequential_write"]["TreeDB"]
	randWrite := run.Results["random_write"]["TreeDB"]
	if math.IsNaN(seq) || seq <= 0 {
		t.Fatalf("expected sequential_write > 0, got %v", seq)
	}
	if math.IsNaN(randWrite) || randWrite <= 0 {
		t.Fatalf("expected random_write > 0, got %v", randWrite)
	}
}

func TestRunBenchmark_CheckpointBetweenTests_RunsFinalCheckpoint(t *testing.T) {
	const dbName = "checkpoint_final_mock"
	var db *checkpointCountingDB
	RegisterHiddenDB(dbName, func(dir string) (kvstore.DB, error) {
		db = &checkpointCountingDB{}
		return db, nil
	})

	run, err := runBenchmark(BenchConfig{
		Keys:         1,
		ValueSize:    0,
		BatchSize:    1,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       dbName,
		TestsArg:     "sequential_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,

		CheckpointBetweenTests: true,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if db == nil {
		t.Fatalf("expected db to be initialized")
	}
	if got, want := db.checkpointCalls, 2; got != want {
		t.Fatalf("checkpoint calls=%d, want %d", got, want)
	}
	if _, ok := run.CheckpointDurations[checkpointPostRunLabel]; !ok {
		t.Fatalf("expected post-run checkpoint durations under %q", checkpointPostRunLabel)
	}
}

func TestRunBenchmark_CheckpointSettleAndStatsSnapshots(t *testing.T) {
	const dbName = "treedb_checkpoint_stats_mock"
	RegisterHiddenDB(dbName, func(dir string) (kvstore.DB, error) {
		return &checkpointStatsDB{fixedNameDB: fixedNameDB{name: "TreeDB"}}, nil
	})

	run, err := runBenchmark(BenchConfig{
		Keys:           1,
		ValueSize:      1,
		BatchSize:      1,
		DBsArg:         dbName,
		TestsArg:       "sequential_write,random_read",
		KeepDir:        false,
		Progress:       false,
		SeedUsed:       1,
		ReadRequireHit: false,

		CheckpointBetweenTests:      true,
		CheckpointSettleBeforeTests: map[string]struct{}{"random_read": {}},
		CheckpointSettleTimeout:     100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if _, ok := run.CheckpointSettleDurations["random_read"]["TreeDB"]; !ok {
		t.Fatalf("missing checkpoint settle duration: %#v", run.CheckpointSettleDurations)
	}
	randomReadStats := run.CheckpointTreeDBStats["random_read"]["TreeDB"]
	if got := randomReadStats["treedb.cache.checkpoint.wait.frontier_units_at_request_last"]; got == "" || got == "0" {
		t.Fatalf("missing checkpoint snapshot stats: %#v", randomReadStats)
	}
	if got := randomReadStats["treedb.cache.checkpoint.active_background_flush_wait_ns_last"]; got == "" || got == "0" {
		t.Fatalf("missing checkpoint wait last stat: %#v", randomReadStats)
	}

	md := renderMarkdownSingle(run)
	if !strings.Contains(md, "## Checkpoint Settle Time (Before Selected Checkpoints)") {
		t.Fatalf("missing checkpoint settle markdown section:\n%s", md)
	}
	if !strings.Contains(md, "## TreeDB Selected Stats (Checkpoint Snapshots)") {
		t.Fatalf("missing checkpoint stats markdown section:\n%s", md)
	}

	dir := t.TempDir()
	if err := writeBenchprofArtifacts(dir, "native-fastpath", []BenchRun{run}); err != nil {
		t.Fatalf("writeBenchprofArtifacts: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "benchprof_results.json"))
	if err != nil {
		t.Fatalf("read benchprof_results.json: %v", err)
	}
	var parsed benchprofExport
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("parse benchprof_results.json: %v", err)
	}
	if len(parsed.Runs) != 1 {
		t.Fatalf("runs=%d want 1", len(parsed.Runs))
	}
	if _, ok := parsed.Runs[0].CheckpointSettleSeconds["random_read"]["TreeDB"]; !ok {
		t.Fatalf("missing exported settle seconds: %#v", parsed.Runs[0].CheckpointSettleSeconds)
	}
	if got := parsed.Runs[0].CheckpointTreeDBStats["random_read"]["TreeDB"]["treedb.cache.checkpoint.wait.frontier_units_at_request_last"]; got == "" || got == "0" {
		t.Fatalf("missing exported checkpoint stats: %#v", parsed.Runs[0].CheckpointTreeDBStats)
	}
}

func TestWaitForTreeDBQueueDrainInstanceWaitsForActiveFlush(t *testing.T) {
	db := &activeFlushStatsDB{fixedNameDB: fixedNameDB{name: "TreeDB"}}
	_, settled, err := waitForTreeDBQueueDrainInstance(&DBInstance{Name: "treedb", Wrapper: db}, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("waitForTreeDBQueueDrainInstance: %v", err)
	}
	if !settled {
		t.Fatalf("settled=false want true")
	}
	if got := db.statsCalls.Load(); got < 2 {
		t.Fatalf("stats calls=%d want at least 2", got)
	}
}

func TestRunBenchmark_CheckpointSettleRejectsUnknownLabel(t *testing.T) {
	const dbName = "treedb_checkpoint_bad_settle_label_mock"
	RegisterHiddenDB(dbName, func(dir string) (kvstore.DB, error) {
		return &checkpointStatsDB{fixedNameDB: fixedNameDB{name: "TreeDB"}}, nil
	})

	_, err := runBenchmark(BenchConfig{
		Keys:           1,
		ValueSize:      1,
		BatchSize:      1,
		DBsArg:         dbName,
		TestsArg:       "sequential_write",
		KeepDir:        false,
		Progress:       false,
		SeedUsed:       1,
		ReadRequireHit: false,

		CheckpointBetweenTests:      true,
		CheckpointSettleBeforeTests: map[string]struct{}{"typo_label": {}},
	})
	if err == nil || !strings.Contains(err.Error(), "unknown checkpoint settle label") {
		t.Fatalf("runBenchmark error=%v, want unknown checkpoint settle label", err)
	}
}

func TestRunBenchmark_CapturesTreeDBStatsAfterClose(t *testing.T) {
	const dbName = "treedb_close_stats_mock"
	RegisterHiddenDB(dbName, func(dir string) (kvstore.DB, error) {
		return &closeStatsDB{name: dbName}, nil
	})

	run, err := runBenchmark(BenchConfig{
		Keys:         1,
		ValueSize:    1,
		BatchSize:    1,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       dbName,
		TestsArg:     "sequential_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	stats := run.TreeDBStats[dbName]
	if got, want := stats["treedb.publish.ordered_root_delta_group.root_apply_calls_total"], "1"; got != want {
		t.Fatalf("post-close TreeDB stat=%q want %q in %#v", got, want, stats)
	}
	if got, want := stats["treedb.test.final_checkpoint_total"], "1"; got != want {
		t.Fatalf("final-checkpoint TreeDB stat=%q want %q in %#v", got, want, stats)
	}
}

func TestRunBenchmark_TreeDBVlogRewriteAfterRunRunsOfflineVacuumByDefault(t *testing.T) {
	oldVacuumFlag := *treedbVacuumAfterVlogRewriteRun
	t.Cleanup(func() {
		*treedbVacuumAfterVlogRewriteRun = oldVacuumFlag
	})
	if !*treedbVacuumAfterVlogRewriteRun {
		t.Fatalf("expected offline vacuum default to be enabled")
	}

	report, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    100,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       "treedb",
		TestsArg:     "sequential_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,

		TreeDBVlogRewriteAfterRun: true,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	rep, ok := report.TreeDBVlogRewrite["TreeDB"]
	if !ok {
		t.Fatalf("expected TreeDB rewrite report in run result")
	}
	if !rep.VacuumRan {
		t.Fatalf("expected offline vacuum to run by default")
	}
}

func TestRunBenchmark_TreeDBVlogRewriteAfterRunSkipsOfflineVacuumWhenDisabled(t *testing.T) {
	oldVacuumFlag := *treedbVacuumAfterVlogRewriteRun
	t.Cleanup(func() {
		*treedbVacuumAfterVlogRewriteRun = oldVacuumFlag
	})
	*treedbVacuumAfterVlogRewriteRun = false

	report, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    100,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       "treedb",
		TestsArg:     "sequential_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,

		TreeDBVlogRewriteAfterRun: true,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	rep, ok := report.TreeDBVlogRewrite["TreeDB"]
	if !ok {
		t.Fatalf("expected TreeDB rewrite report in run result")
	}
	if rep.VacuumRan {
		t.Fatalf("expected offline vacuum to be skipped when disabled")
	}
	if rep.AfterVacuum.TotalBytes != 0 || rep.AfterVacuum.TotalFiles != 0 {
		t.Fatalf("expected no post-vacuum usage report when vacuum is disabled, got=%#v", rep.AfterVacuum)
	}
}

func TestRunBenchmark_TreeDBVlogRewriteAfterRun_OfflineVacuumNoCatastrophicIndexGrowthE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e vacuum regression in short mode")
	}

	oldVacuumFlag := *treedbVacuumAfterVlogRewriteRun
	t.Cleanup(func() {
		*treedbVacuumAfterVlogRewriteRun = oldVacuumFlag
	})
	*treedbVacuumAfterVlogRewriteRun = true

	report, err := runBenchmark(BenchConfig{
		// Keep this e2e guard large enough to exercise multi-level trees while
		// staying practical for standard CI runs.
		Keys:         500_000,
		ValueSize:    128,
		BatchSize:    4_000,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       "treedb",
		TestsArg:     "batch_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
		Profile:      "fast",

		TreeDBVlogRewriteAfterRun: true,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	rep, ok := report.TreeDBVlogRewrite["TreeDB"]
	if !ok {
		t.Fatalf("expected TreeDB rewrite report in run result")
	}
	if !rep.VacuumRan {
		t.Fatalf("expected offline vacuum to run")
	}

	beforeIndex := rep.AfterTree.MainIndexBytes
	afterIndex := rep.AfterVacuumTree.MainIndexBytes
	if beforeIndex <= 0 || afterIndex <= 0 {
		t.Fatalf("expected non-zero index sizes, before=%d after=%d", beforeIndex, afterIndex)
	}

	// Regression guard for issue #1467: offline vacuum must not explode index
	// size by materializing outer leaf-log data into index.db.
	const maxIndexGrowthMultiple uint64 = 8
	if afterIndex > beforeIndex*maxIndexGrowthMultiple {
		t.Fatalf(
			"catastrophic post-vacuum index growth: before=%d after=%d multiple=%.2f (> %dx)",
			beforeIndex,
			afterIndex,
			float64(afterIndex)/float64(beforeIndex),
			maxIndexGrowthMultiple,
		)
	}
}

func TestRunBenchmark_PropagatesCloseError(t *testing.T) {
	const dbName = "close_error_mock"
	closeErr := errors.New("close failed")
	RegisterHiddenDB(dbName, func(dir string) (kvstore.DB, error) {
		return &closeStatsDB{name: dbName, closeErr: closeErr}, nil
	})

	_, err := runBenchmark(BenchConfig{
		Keys:         1,
		ValueSize:    1,
		BatchSize:    1,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       dbName,
		TestsArg:     "sequential_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if !errors.Is(err, closeErr) {
		t.Fatalf("runBenchmark err=%v want close error", err)
	}
}

func TestRunBenchmark_PropagatesFinalStatsCheckpointError(t *testing.T) {
	const dbName = "final_stats_checkpoint_error_mock"
	checkpointErr := errors.New("checkpoint failed")
	RegisterHiddenDB(dbName, func(dir string) (kvstore.DB, error) {
		return &closeStatsDB{name: dbName, checkpointErr: checkpointErr}, nil
	})

	_, err := runBenchmark(BenchConfig{
		Keys:         1,
		ValueSize:    1,
		BatchSize:    1,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       dbName,
		TestsArg:     "sequential_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if !errors.Is(err, checkpointErr) {
		t.Fatalf("runBenchmark err=%v want checkpoint error", err)
	}
}

func TestRunBenchmark_SettleBeforeScansRunsAfterMeasuredWrites(t *testing.T) {
	const dbName = "settle_probe"
	probe := &settleProbeState{}
	RegisterHiddenDB(dbName, func(dir string) (kvstore.DB, error) {
		id := int(probe.nextID.Add(1))
		return &settleProbeDB{id: id, probe: probe}, nil
	})

	_, err := runBenchmark(BenchConfig{
		Keys:              16,
		ValueSize:         1,
		BatchSize:         4,
		RangeQueries:      0,
		RangeSpan:         0,
		DBsArg:            dbName,
		TestsArg:          "sequential_write,dataset_read_random",
		KeepDir:           false,
		Progress:          false,
		SeedUsed:          1,
		SettleBeforeScans: true,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if got := probe.readAfterWriterCloseCnt.Load(); got == 0 {
		t.Fatalf("dataset_read_random did not run after closing the measured write instance")
	}
}

func TestRunBenchmark_VacuumBetweenTests_Smoke(t *testing.T) {
	t.Skip("deferred to #3681: successful online vacuum requires RecoverableRootSet convergence")

	run, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "sequential_write,random_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,

		CheckpointBetweenTests: true,
		VacuumBetweenTests:     true,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}

	if len(run.VacuumDurations) == 0 {
		t.Fatalf("expected non-empty vacuum durations")
	}
}

func TestRunBenchmark_CompressionVariantsMatrix_Smoke(t *testing.T) {
	prevTreeDB := *treedbVlogDictMode
	prevTreeDBCompressionVariant := *treedbVlogCompressionVariant
	prevLevelDB := *leveldbBlockCompressionMode
	defer func() {
		*treedbVlogDictMode = prevTreeDB
		*treedbVlogCompressionVariant = prevTreeDBCompressionVariant
		*leveldbBlockCompressionMode = prevLevelDB
	}()
	*treedbVlogDictMode = "both"
	*treedbVlogCompressionVariant = "default"
	*leveldbBlockCompressionMode = "both"

	run, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		ValueSize:    128,
		BatchSize:    100,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       "treedb,leveldb",
		TestsArg:     "batch_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if len(run.Instances) != 4 {
		t.Fatalf("expected 4 instances, got %d", len(run.Instances))
	}

	got := run.Results["batch_write"]
	wantCols := []string{
		"TreeDB (vlog_dict=off)",
		"TreeDB (vlog_dict=on)",
		"LevelDB (block=off)",
		"LevelDB (block=on)",
	}
	for _, col := range wantCols {
		if _, ok := got[col]; !ok {
			t.Fatalf("missing result column %q (have: %v)", col, mapsKeysSorted(got))
		}
	}
}

func TestRunBenchmark_CompressionVariantsAutoMatrix_Smoke(t *testing.T) {
	prevTreeDB := *treedbVlogDictMode
	prevTreeDBCompressionVariant := *treedbVlogCompressionVariant
	prevLevelDB := *leveldbBlockCompressionMode
	defer func() {
		*treedbVlogDictMode = prevTreeDB
		*treedbVlogCompressionVariant = prevTreeDBCompressionVariant
		*leveldbBlockCompressionMode = prevLevelDB
	}()
	*treedbVlogDictMode = "default"
	*treedbVlogCompressionVariant = "all"
	*leveldbBlockCompressionMode = "both"

	run, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		ValueSize:    128,
		BatchSize:    100,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       "treedb,leveldb",
		TestsArg:     "batch_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if len(run.Instances) != 8 {
		t.Fatalf("expected 8 instances, got %d", len(run.Instances))
	}

	got := run.Results["batch_write"]
	wantCols := []string{
		"TreeDB (vlog=off)",
		"TreeDB (vlog=dict)",
		"TreeDB (vlog=block/snappy)",
		"TreeDB (vlog=block/lz4)",
		"TreeDB (vlog=block/zstd)",
		"TreeDB (vlog=auto)",
		"LevelDB (block=off)",
		"LevelDB (block=on)",
	}
	for _, col := range wantCols {
		if _, ok := got[col]; !ok {
			t.Fatalf("missing result column %q (have: %v)", col, mapsKeysSorted(got))
		}
	}
}

func TestRunBenchmark_KeyShapePrefix4(t *testing.T) {
	run, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		KeyShape:     "be8_prefix4",
		ValueSize:    16,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "batch_write,full_scan,prefix_scan",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	full := run.Results["full_scan"]["TreeDB"]
	prefix := run.Results["prefix_scan"]["TreeDB"]
	if math.IsNaN(full) || full <= 0 {
		t.Fatalf("expected full_scan > 0, got %v", full)
	}
	if math.IsNaN(prefix) || prefix <= 0 {
		t.Fatalf("expected prefix_scan > 0, got %v", prefix)
	}
}

func TestRunBenchmark_InvalidKeyShape(t *testing.T) {
	_, err := runBenchmark(BenchConfig{
		Keys:         100,
		KeyShape:     "nope",
		ValueSize:    16,
		BatchSize:    10,
		RangeQueries: 5,
		RangeSpan:    2,
		DBsArg:       "treedb",
		TestsArg:     "sequential_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported -key-shape") {
		t.Fatalf("expected unsupported key-shape error, got %v", err)
	}
}

func TestRunBenchmark_KeyShapePrefix4RejectsOverflow(t *testing.T) {
	_, err := runBenchmark(BenchConfig{
		Keys:         (math.MaxUint32 / 10) + 1,
		KeyShape:     "be8_prefix4",
		ValueSize:    16,
		BatchSize:    10,
		RangeQueries: 5,
		RangeSpan:    2,
		DBsArg:       "treedb",
		TestsArg:     "random_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err == nil || !strings.Contains(err.Error(), "exceeds uint32 range") {
		t.Fatalf("expected be8_prefix4 overflow error, got %v", err)
	}
}

func TestClampWarmupKeyCount(t *testing.T) {
	if got := clampWarmupKeyCount(benchKeyShapeBE8Prefix4, uint64(math.MaxUint32)-3, 10); got != 4 {
		t.Fatalf("prefix4 clamp mismatch: got %d want 4", got)
	}
	if got := clampWarmupKeyCount(benchKeyShapeBE8Prefix4, uint64(math.MaxUint32)+1, 10); got != 0 {
		t.Fatalf("prefix4 out-of-range base should clamp to 0, got %d", got)
	}
	if got := clampWarmupKeyCount(benchKeyShapeBE8, math.MaxUint64-8, 16); got != 9 {
		t.Fatalf("be8 clamp mismatch near max uint64: got %d want 9", got)
	}
	if got := clampWarmupKeyCount(benchKeyShapeBE8, 100, 32); got != 32 {
		t.Fatalf("be8 in-range should preserve warmup count, got %d", got)
	}
}

func TestMakeWriteValuePool_RepeatNotAllIdentical(t *testing.T) {
	values, err := makeWriteValuePool(1, "repeat", 128, 32)
	if err != nil {
		t.Fatalf("makeWriteValuePool: %v", err)
	}
	if len(values) < 2 {
		t.Fatalf("expected pool size >= 2, got %d", len(values))
	}
	allSame := true
	for i := 1; i < len(values); i++ {
		if !bytes.Equal(values[0], values[i]) {
			allSame = false
			break
		}
	}
	if allSame {
		t.Fatalf("expected repeat value pool to contain non-identical values")
	}
}

func TestMakeWriteValuePool_CelestiaHeightPrefixFill(t *testing.T) {
	values, err := makeWriteValuePool(1, "celestia_height_prefix_fill", 128, 8)
	if err != nil {
		t.Fatalf("makeWriteValuePool: %v", err)
	}
	if len(values) != 8 {
		t.Fatalf("expected 8 values, got %d", len(values))
	}
	wantPrefix := []byte("celestia/height/")
	if !bytes.HasPrefix(values[0], wantPrefix) {
		t.Fatalf("expected prefix %q", wantPrefix)
	}
	if bytes.Equal(values[0], values[1]) {
		t.Fatalf("expected distinct values across indices")
	}
}

func mapsKeysSorted(m map[string]float64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func TestMakeWriteValuePool_UnknownPatternErrors(t *testing.T) {
	if _, err := makeWriteValuePool(1, "not_a_real_pattern", 16, 0); err == nil {
		t.Fatalf("expected error")
	}
}

func TestParseTreeDBVlogCompressionVariant(t *testing.T) {
	cases := []struct {
		in      string
		wantErr bool
	}{
		{in: "default"},
		{in: "off"},
		{in: "dict"},
		{in: "block_snappy"},
		{in: "block_lz4"},
		{in: "block_zstd"},
		{in: "auto"},
		{in: "all"},
		{in: "nope", wantErr: true},
	}
	for _, tc := range cases {
		_, err := parseTreeDBVlogCompressionVariant("treedb-vlog-compression-variant", tc.in)
		if tc.wantErr && err == nil {
			t.Fatalf("expected error for %q", tc.in)
		}
		if !tc.wantErr && err != nil {
			t.Fatalf("unexpected error for %q: %v", tc.in, err)
		}
	}
}

func TestSelectedTreeDBVlogCompressionMode_DefaultTreeDBCountsAuto(t *testing.T) {
	prev := *treedbVlogCompression
	defer func() { *treedbVlogCompression = prev }()
	*treedbVlogCompression = "default"

	mode, ok := selectedTreeDBVlogCompressionMode("treedb")
	if !ok || formatTreeDBVlogCompression(mode) != "auto" {
		t.Fatalf("selected mode ok=%t mode=%s, want auto", ok, formatTreeDBVlogCompression(mode))
	}
	if !treeDBInstanceCountsAutoVlogCandidates(&DBInstance{TreeDBVlogCompressionMode: mode, TreeDBVlogCompressionModeSet: ok}) {
		t.Fatalf("default treedb instance should count auto candidates")
	}
}

func TestSelectedTreeDBVlogCompressionMode_VariantBlockDoesNotCountAuto(t *testing.T) {
	mode, ok := selectedTreeDBVlogCompressionMode("treedb_vlog_block_zstd")
	if !ok || formatTreeDBVlogCompression(mode) != "block" {
		t.Fatalf("selected mode ok=%t mode=%s, want block", ok, formatTreeDBVlogCompression(mode))
	}
	if treeDBInstanceCountsAutoVlogCandidates(&DBInstance{TreeDBVlogCompressionMode: mode, TreeDBVlogCompressionModeSet: ok}) {
		t.Fatalf("block variant should not count auto candidates")
	}
}

func TestRenderKeptDirsString_SortsAndUsesWrapperNames(t *testing.T) {
	got := renderKeptDirsString([]*DBInstance{
		{Name: "zeta", Wrapper: &fixedNameDB{name: "B DB"}, Dir: "/tmp/b"},
		{Name: "alpha", Wrapper: &fixedNameDB{name: "A DB"}, Dir: "/tmp/a"},
		{Name: "missing_dir", Wrapper: &fixedNameDB{name: "Skip DB"}, Dir: ""},
		{Name: "fallback_only", Wrapper: nil, Dir: "/tmp/fallback"},
		nil,
	})
	want := "" +
		"A DB: /tmp/a\n" +
		"B DB: /tmp/b\n" +
		"fallback_only: /tmp/fallback\n"
	if got != want {
		t.Fatalf("renderKeptDirsString mismatch\n--- got ---\n%s--- want ---\n%s", got, want)
	}
}

func TestRenderMarkdownSingle_KeepDirIncludesKeptSection(t *testing.T) {
	run := BenchRun{
		Config: BenchConfig{
			Keys:         100,
			ValueSize:    16,
			BatchSize:    10,
			RangeQueries: 0,
			RangeSpan:    0,
			KeepDir:      true,
		},
		Instances: []*DBInstance{
			{Name: "fake", Wrapper: &fixedNameDB{name: "FakeDB"}, Dir: "/tmp/bench-fake"},
		},
		TestOrder: []string{"batch_write"},
		DisplayNames: map[string]string{
			"batch_write": "Batch Write",
		},
		Results: map[string]map[string]float64{
			"batch_write": {
				"FakeDB": 1234,
			},
		},
	}

	md := renderMarkdownSingle(run)
	if !strings.Contains(md, "## Kept Data Directories") {
		t.Fatalf("expected kept directories section, got:\n%s", md)
	}
	if !strings.Contains(md, "FakeDB: /tmp/bench-fake") {
		t.Fatalf("expected kept directory line in markdown, got:\n%s", md)
	}
}

func TestRenderMarkdownSingle_IncludesTreeDBPerfSections(t *testing.T) {
	run := BenchRun{
		Config: BenchConfig{
			Keys:      100,
			ValueSize: 16,
			BatchSize: 10,
		},
		Instances: []*DBInstance{
			{Name: "treedb", Wrapper: &fixedNameDB{name: "TreeDB"}, Dir: "/tmp/bench-treedb"},
		},
		TestOrder: []string{"random_read_parallel_acquire_snapshot"},
		DisplayNames: map[string]string{
			"random_read_parallel_acquire_snapshot": "Random Read (Parallel, Snapshot Per Key)",
		},
		Results: map[string]map[string]float64{
			"random_read_parallel_acquire_snapshot": {
				"TreeDB": 1234,
			},
		},
		TreeDBPerf: map[string]map[string]treeDBPerfMetrics{
			"random_read_parallel_acquire_snapshot": {
				"TreeDB": {
					Mmap: treeDBMmapPerfMetrics{
						Hits:           7,
						MissNoMapping:  2,
						FallbackReadAt: 3,
						HitRatio:       0.7,
					},
					Snapshot: treeDBSnapshotPerfMetrics{
						AcquireCalls:      4,
						AcquireTotalNanos: 8_000,
						AcquireAvgMicros:  2,
						CloseCalls:        4,
						CloseTotalNanos:   12_000,
						CloseAvgMicros:    3,
					},
					LeafGenerationsPinnedAfter: 1,
					LeafPinsTotalAfter:         4,
				},
			},
		},
		TreeDBStats: map[string]map[string]string{
			"TreeDB": {
				"treedb.cache.vlog_mmap.read.hits":                                                                            "7",
				"treedb.cache.vlog_mmap.read.hit_ratio":                                                                       "0.700000",
				"treedb.applied_command_lsn":                                                                                  "3",
				"treedb.command_wal.enabled":                                                                                  "true",
				"treedb.command_wal.required_feature":                                                                         "true",
				"treedb.command_wal.live_accepted_frames":                                                                     "3",
				"treedb.command_wal.live_accepted_max_lsn":                                                                    "3",
				"treedb.command_wal.live_covered_frames":                                                                      "3",
				"treedb.command_wal.live_covered_max_lsn":                                                                     "3",
				"treedb.command_wal.frames":                                                                                   "3",
				"treedb.command_wal.typed_segments":                                                                           "1",
				"treedb.command_wal.max_lsn":                                                                                  "3",
				"treedb.leaf_generation.generations.pinned":                                                                   "1",
				"treedb.leaf_generation.pins.total":                                                                           "4",
				"treedb.publish.ordered_root_delta_group.calls_total":                                                         "9",
				"treedb.publish.ordered_root_delta_group.root_apply_calls_total":                                              "11",
				"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_node_bytes_read_total":                           "2048",
				"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_page_bytes_written_total":                        "4096",
				"treedb.publish.ordered_root_delta_group.publish_prepare_ns_total":                                            "66",
				"treedb.publish.ordered_root_delta_group.write_lock_hold_ns_total":                                            "12345",
				"treedb.publish.ordered_root_delta_group.span_native.candidate_ops_total":                                     "12",
				"treedb.publish.ordered_root_delta_group.span_native.fallbacks_total":                                         "3",
				"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.span_native_not_implemented.count_total": "3",
				"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.span_native_not_implemented.ops_total":   "12",
				"treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish.observations_total":            "3",
				"treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish.candidate_ops_total":           "12",
				"treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish.fallbacks_total":               "3",
				"treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish.fallback.reason.span_native_not_implemented.ops_total": "12",
				"treedb.publish.ordered_root_delta_group.span_native.triage.route.delta_batch_publish.status":                                         "fallback",
				"treedb.publish.ordered_root_delta_group.span_native.triage.route.delta_batch_publish.fallback_reason":                                "span_native_not_implemented",
				"treedb.cache.vlog_auto.frames.block_lz4":                 "7",
				"treedb.cache.vlog_auto.bytes.block_lz4":                  "3500",
				"treedb.cache.vlog_auto.frames_frac.block_lz4":            "1.000000",
				"treedb.cache.vlog_write_mode.frames.block":               "7",
				"treedb.cache.vlog_write_mode.raw_bytes.block":            "7000",
				"treedb.cache.vlog_write_mode.stored_bytes.block":         "3500",
				"treedb.cache.vlog_write_mode.stored_ratio.block":         "0.500000",
				"treedb.cache.vlog_payload_kind.frames.outer_leaf":        "7",
				"treedb.cache.vlog_payload_kind.raw_bytes.outer_leaf":     "7000",
				"treedb.cache.vlog_payload_kind.stored_bytes.outer_leaf":  "3500",
				"treedb.cache.vlog_payload_kind.stored_ratio.outer_leaf":  "0.500000",
				"treedb.cache.vlog_payload_split.records.outer_leaf":      "7",
				"treedb.cache.vlog_payload_split.raw_bytes.outer_leaf":    "7000",
				"treedb.cache.vlog_payload_split.stored_bytes.outer_leaf": "3500",
				"treedb.cache.vlog_payload_split.stored_ratio.outer_leaf": "0.500000",
				"treedb.cache.vlog_outer_leaf_codec.frames.lz4":           "7",
				"treedb.cache.vlog_outer_leaf_codec.raw_bytes.lz4":        "7000",
				"treedb.cache.vlog_outer_leaf_codec.stored_bytes.lz4":     "3500",
				"treedb.cache.vlog_outer_leaf_codec.stored_ratio.lz4":     "0.500000",
				"treedb.cache.vlog_block.k.count.lz4":                     "7",
				"treedb.cache.vlog_block.k.avg.lz4":                       "1.000",
				"treedb.cache.vlog_block.k.max.lz4":                       "1",
				"treedb.cache.vlog_block.ratio.lz4":                       "0.500000",
				"treedb.cache.vlog_block.k.bucket.lz4.le_1":               "7",
			},
		},
	}

	md := renderMarkdownSingle(run)
	if !strings.Contains(md, "## TreeDB Perf Instrumentation") {
		t.Fatalf("expected TreeDB perf section, got:\n%s", md)
	}
	if !strings.Contains(md, "snapshot.acquire.calls=4") {
		t.Fatalf("expected snapshot metrics in markdown, got:\n%s", md)
	}
	if !strings.Contains(md, "vlog_mmap.read.hits.delta=7") {
		t.Fatalf("expected mmap metrics in markdown, got:\n%s", md)
	}
	if !strings.Contains(md, "## TreeDB Value-Log Codec Summary (End of Run)") {
		t.Fatalf("expected TreeDB codec summary section, got:\n%s", md)
	}
	for _, want := range []string{
		"vlog_auto.frames: block_lz4=7",
		"vlog_auto.bytes: block_lz4=3500",
		"vlog_write_mode.block: frames=7 raw_bytes=7000 stored_bytes=3500 stored_ratio=0.500000",
		"vlog_payload_kind.outer_leaf: frames=7 raw_bytes=7000 stored_bytes=3500 stored_ratio=0.500000",
		"vlog_payload_split.outer_leaf: records=7 raw_bytes=7000 stored_bytes=3500 stored_ratio=0.500000",
		"vlog_outer_leaf_codec.lz4: frames=7 raw_bytes=7000 stored_bytes=3500 stored_ratio=0.500000",
		"vlog_block.k.lz4: count=7 avg=1.000 max=1 ratio=0.500000",
		"vlog_block.k.bucket.lz4: le_1=7",
	} {
		if !strings.Contains(md, want) {
			t.Fatalf("expected codec summary %q in markdown, got:\n%s", want, md)
		}
	}
	if !strings.Contains(md, "## TreeDB Selected Stats (End of Run)") {
		t.Fatalf("expected TreeDB selected stats section, got:\n%s", md)
	}
	if !strings.Contains(md, "leaf_generation.pins.total: 4") {
		t.Fatalf("expected selected stats in markdown, got:\n%s", md)
	}
	for _, want := range []string{
		"publish.ordered_root_delta_group.calls_total: 9",
		"publish.ordered_root_delta_group.root_apply_calls_total: 11",
		"publish.ordered_root_delta_group.root_apply_leaf_log_node_bytes_read_total: 2048",
		"publish.ordered_root_delta_group.root_apply_leaf_log_page_bytes_written_total: 4096",
		"publish.ordered_root_delta_group.publish_prepare_ns_total: 66",
		"publish.ordered_root_delta_group.write_lock_hold_ns_total: 12345",
		"publish.ordered_root_delta_group.span_native.candidate_ops_total: 12",
		"publish.ordered_root_delta_group.span_native.fallbacks_total: 3",
		"publish.ordered_root_delta_group.span_native.fallback.not_implemented_count_total: 3",
		"publish.ordered_root_delta_group.span_native.fallback.not_implemented_ops_total: 12",
		"publish.ordered_root_delta_group.span_native.route.delta_batch_publish.observations_total: 3",
		"publish.ordered_root_delta_group.span_native.route.delta_batch_publish.candidate_ops_total: 12",
		"publish.ordered_root_delta_group.span_native.route.delta_batch_publish.fallbacks_total: 3",
		"publish.ordered_root_delta_group.span_native.route.delta_batch_publish.fallback.reason.span_native_not_implemented.ops_total: 12",
		"publish.ordered_root_delta_group.span_native.triage.delta_batch_publish.status: fallback",
		"publish.ordered_root_delta_group.span_native.triage.delta_batch_publish.fallback_reason: span_native_not_implemented",
		"applied_command_lsn: 3",
		"command_wal.enabled: true",
		"command_wal.required_feature: true",
		"command_wal.live_accepted_frames: 3",
		"command_wal.live_accepted_max_lsn: 3",
		"command_wal.live_covered_frames: 3",
		"command_wal.live_covered_max_lsn: 3",
		"command_wal.frames: 3",
		"command_wal.typed_segments: 1",
		"command_wal.max_lsn: 3",
	} {
		if !strings.Contains(md, want) {
			t.Fatalf("expected selected ordered-root stat %q in markdown, got:\n%s", want, md)
		}
	}
}

func TestScanTreeDBLeafVLogCodecStats_ParsesGroupedFrameCodecs(t *testing.T) {
	dir := t.TempDir()
	leafDir := filepath.Join(dir, "maindb", "leaf_vlog")
	if err := os.MkdirAll(leafDir, 0o755); err != nil {
		t.Fatalf("MkdirAll leaf_vlog: %v", err)
	}

	var file []byte
	file = append(file, testTreeDBVLogFrame(t, 3, treeDBVlogScanBlockCodecZSTD, true, 300, 120)...)
	file = append(file, testTreeDBVLogFrame(t, 1, 0, false, 50, 50)...)
	file = append(file, testTreeDBVLogUngroupedRecord(t, 4096)...)
	if err := os.WriteFile(filepath.Join(leafDir, "value-l255-000001.log"), file, 0o644); err != nil {
		t.Fatalf("WriteFile leaf log: %v", err)
	}

	stats, err := scanTreeDBLeafVLogCodecStats(dir, true)
	if err != nil {
		t.Fatalf("scanTreeDBLeafVLogCodecStats: %v", err)
	}
	want := map[string]string{
		"treedb.cache.vlog_write_mode.frames.block":              "1",
		"treedb.cache.vlog_write_mode.raw_bytes.block":           "300",
		"treedb.cache.vlog_write_mode.stored_bytes.block":        "120",
		"treedb.cache.vlog_write_mode.stored_ratio.block":        "0.400000",
		"treedb.cache.vlog_write_mode.frames.off":                "1",
		"treedb.cache.vlog_payload_kind.frames.outer_leaf":       "2",
		"treedb.cache.vlog_payload_kind.raw_bytes.outer_leaf":    "350",
		"treedb.cache.vlog_payload_kind.stored_bytes.outer_leaf": "170",
		"treedb.cache.vlog_payload_split.records.outer_leaf":     "4",
		"treedb.cache.vlog_outer_leaf_codec.frames.unknown":      "1",
		"treedb.cache.vlog_outer_leaf_codec.frames.legacy_page":  "1",
		"treedb.cache.vlog_block.k.count.zstd":                   "1",
		"treedb.cache.vlog_block.k.avg.zstd":                     "3.000",
		"treedb.cache.vlog_block.k.max.zstd":                     "3",
		"treedb.cache.vlog_block.k.bucket.zstd.le_4":             "1",
		"treedb.cache.vlog_block.ratio.zstd":                     "0.400000",
		"treedb.cache.vlog_auto.frames.block_zstd":               "1",
		"treedb.cache.vlog_auto.bytes.block_zstd":                "300",
		"treedb.cache.vlog_auto.frames.off":                      "1",
		"treedb.cache.vlog_auto.bytes.off":                       "50",
		"treedb.cache.vlog_auto.frames_frac.block_zstd":          "0.500000",
		"treedb.cache.vlog_auto.frames_frac.off":                 "0.500000",
	}
	for key, wantValue := range want {
		if got := stats[key]; got != wantValue {
			t.Fatalf("%s=%q want %q (stats=%#v)", key, got, wantValue, stats)
		}
	}
}

func TestScanTreeDBLeafVLogCodecStats_UncompressedDictFrameUsesDictMode(t *testing.T) {
	dir := t.TempDir()
	leafDir := filepath.Join(dir, "maindb", "leaf_vlog")
	if err := os.MkdirAll(leafDir, 0o755); err != nil {
		t.Fatalf("MkdirAll leaf_vlog: %v", err)
	}
	record := testTreeDBVLogFrame(t, 1, 0, false, 50, 50)
	binary.LittleEndian.PutUint64(record[treeDBVlogScanHeaderSize+4:treeDBVlogScanHeaderSize+12], 123)
	if err := os.WriteFile(filepath.Join(leafDir, "value-l255-000001.log"), record, 0o644); err != nil {
		t.Fatalf("WriteFile leaf log: %v", err)
	}

	stats, err := scanTreeDBLeafVLogCodecStats(dir, true)
	if err != nil {
		t.Fatalf("scanTreeDBLeafVLogCodecStats: %v", err)
	}
	if got := stats["treedb.cache.vlog_write_mode.frames.dict"]; got != "1" {
		t.Fatalf("dict frames=%q want 1 (stats=%#v)", got, stats)
	}
	if got := stats["treedb.cache.vlog_auto.frames.dict"]; got != "1" {
		t.Fatalf("auto dict frames=%q want 1 (stats=%#v)", got, stats)
	}
	if got := stats["treedb.cache.vlog_write_mode.frames.off"]; got != "" {
		t.Fatalf("uncompressed dict frame misclassified as off=%q (stats=%#v)", got, stats)
	}
}

func TestScanTreeDBLeafVLogCodecStats_PreservesPartialStatsOnSegmentError(t *testing.T) {
	dir := t.TempDir()
	leafDir := filepath.Join(dir, "maindb", "leaf_vlog")
	if err := os.MkdirAll(leafDir, 0o755); err != nil {
		t.Fatalf("MkdirAll leaf_vlog: %v", err)
	}
	if err := os.WriteFile(filepath.Join(leafDir, "value-l255-000001.log"), testTreeDBVLogFrame(t, 1, 0, false, 50, 50), 0o644); err != nil {
		t.Fatalf("WriteFile good leaf log: %v", err)
	}
	bad := make([]byte, treeDBVlogScanHeaderSize)
	bad[4] = treeDBVlogScanVersion
	bad[5] = treeDBVlogScanRecordFlagGrouped
	binary.LittleEndian.PutUint32(bad[16:20], treeDBVlogScanMaxBodyLen+1)
	if err := os.WriteFile(filepath.Join(leafDir, "value-l255-000002.log"), bad, 0o644); err != nil {
		t.Fatalf("WriteFile bad leaf log: %v", err)
	}

	stats, err := scanTreeDBLeafVLogCodecStats(dir, true)
	if err == nil || !strings.Contains(err.Error(), "value-log body too large") {
		t.Fatalf("expected oversized body error, got %v", err)
	}
	if got := stats["treedb.cache.vlog_write_mode.frames.off"]; got != "1" {
		t.Fatalf("partial stats off frames=%q want 1 (stats=%#v)", got, stats)
	}
}

func TestScanTreeDBLeafVLogCodecStats_UnknownBlockCodecNotAttributedToSnappy(t *testing.T) {
	dir := t.TempDir()
	leafDir := filepath.Join(dir, "maindb", "leaf_vlog")
	if err := os.MkdirAll(leafDir, 0o755); err != nil {
		t.Fatalf("MkdirAll leaf_vlog: %v", err)
	}
	file := testTreeDBVLogFrame(t, 2, 99, true, 200, 120)
	if err := os.WriteFile(filepath.Join(leafDir, "value-l255-000001.log"), file, 0o644); err != nil {
		t.Fatalf("WriteFile leaf log: %v", err)
	}

	stats, err := scanTreeDBLeafVLogCodecStats(dir, true)
	if err != nil {
		t.Fatalf("scanTreeDBLeafVLogCodecStats: %v", err)
	}
	if got := stats["treedb.cache.vlog_outer_leaf_codec.frames.unknown"]; got != "1" {
		t.Fatalf("unknown codec frames=%q want 1 (stats=%#v)", got, stats)
	}
	if got := stats["treedb.cache.vlog_auto.frames.block_snappy"]; got != "" {
		t.Fatalf("unknown codec attributed to block_snappy=%q (stats=%#v)", got, stats)
	}
}

func TestScanTreeDBVLogCodecStatsFile_RejectsInvalidOffsets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "value-l255-000001.log")
	record := testTreeDBVLogFrame(t, 2, 0, false, 50, 50)
	offsetStart := treeDBVlogScanHeaderSize + treeDBVlogScanFrameHeaderSize + (2 * 8)
	binary.LittleEndian.PutUint32(record[offsetStart:offsetStart+4], 1)
	if err := os.WriteFile(path, record, 0o644); err != nil {
		t.Fatalf("WriteFile leaf log: %v", err)
	}

	err := scanTreeDBVLogCodecStatsFile(path, newTreeDBVlogCodecScanStats(), true)
	if err == nil || !strings.Contains(err.Error(), "frame first offset") {
		t.Fatalf("expected invalid offset error, got %v", err)
	}
}

func TestScanTreeDBLeafVLogCodecStatsFile_RejectsOversizedBodyLength(t *testing.T) {
	path := filepath.Join(t.TempDir(), "value-l255-000001.log")
	record := make([]byte, treeDBVlogScanHeaderSize)
	record[4] = treeDBVlogScanVersion
	record[5] = treeDBVlogScanRecordFlagGrouped
	binary.LittleEndian.PutUint32(record[16:20], treeDBVlogScanMaxBodyLen+1)
	if err := os.WriteFile(path, record, 0o644); err != nil {
		t.Fatalf("WriteFile leaf log: %v", err)
	}

	err := scanTreeDBVLogCodecStatsFile(path, newTreeDBVlogCodecScanStats(), true)
	if err == nil || !strings.Contains(err.Error(), "value-log body too large") {
		t.Fatalf("expected oversized body error, got %v", err)
	}
}

func testTreeDBVLogFrame(t *testing.T, k int, codec int, compressed bool, rawBytes int, storedBytes int) []byte {
	t.Helper()
	if k <= 0 {
		t.Fatalf("invalid k=%d", k)
	}
	prefixLen := treeDBVlogScanFrameHeaderSize + (k * 8) + ((k + 1) * 4)
	bodyLen := prefixLen + storedBytes
	body := make([]byte, bodyLen)
	body[0] = treeDBVlogScanFrameVersion
	if compressed {
		body[1] = treeDBVlogScanFrameFlagCompressed
	}
	body[2] = byte(k)
	body[3] = byte(codec)
	for i := 0; i < k; i++ {
		binary.LittleEndian.PutUint64(body[treeDBVlogScanFrameHeaderSize+(i*8):], uint64(i+1))
	}
	offsetStart := treeDBVlogScanFrameHeaderSize + (k * 8)
	for i := 0; i <= k; i++ {
		binary.LittleEndian.PutUint32(body[offsetStart+(i*4):], uint32((rawBytes*i)/k))
	}

	out := make([]byte, treeDBVlogScanHeaderSize+bodyLen)
	out[4] = treeDBVlogScanVersion
	out[5] = treeDBVlogScanRecordFlagGrouped
	binary.LittleEndian.PutUint32(out[16:20], uint32(bodyLen))
	copy(out[treeDBVlogScanHeaderSize:], body)
	return out
}

func testTreeDBVLogUngroupedRecord(t *testing.T, bodyLen int) []byte {
	t.Helper()
	if bodyLen <= 0 {
		t.Fatalf("invalid bodyLen=%d", bodyLen)
	}
	out := make([]byte, treeDBVlogScanHeaderSize+bodyLen)
	out[4] = treeDBVlogScanVersion
	binary.LittleEndian.PutUint32(out[16:20], uint32(bodyLen))
	for i := treeDBVlogScanHeaderSize; i < len(out); i++ {
		out[i] = byte(i)
	}
	return out
}

func TestMergeTreeDBLeafVLogCodecStats_PreservesAggregateStats(t *testing.T) {
	dst := map[string]string{
		"treedb.cache.vlog_write_mode.frames.block":       "9",
		"treedb.cache.vlog_write_mode.raw_bytes.block":    "900",
		"treedb.cache.vlog_write_mode.stored_bytes.block": "450",
	}
	leaf := map[string]string{
		"treedb.cache.vlog_write_mode.frames.block":       "2",
		"treedb.cache.vlog_write_mode.raw_bytes.block":    "200",
		"treedb.cache.vlog_write_mode.stored_bytes.block": "100",
	}
	mergeTreeDBLeafVLogCodecStats(dst, leaf)
	if got := dst["treedb.cache.vlog_write_mode.frames.block"]; got != "9" {
		t.Fatalf("aggregate frames overwritten: %q", got)
	}
	if got := dst["treedb.cache.vlog_leaf_scan.write_mode.frames.block"]; got != "2" {
		t.Fatalf("leaf-only frames missing: %q in %#v", got, dst)
	}
}

func TestAppendTreeDBLeafScanVlogSummaryLines_IncludesNamespacedStats(t *testing.T) {
	stats := map[string]string{
		"treedb.cache.vlog_leaf_scan.write_mode.frames.block":             "2",
		"treedb.cache.vlog_leaf_scan.write_mode.stored_ratio.block":       "0.500000",
		"treedb.cache.vlog_leaf_scan.outer_leaf_codec.frames.snappy":      "2",
		"treedb.cache.vlog_leaf_scan.outer_leaf_codec.frames.legacy_page": "1",
		"treedb.cache.vlog_leaf_scan.block.k.count.snappy":                "2",
		"treedb.cache.vlog_leaf_scan.block.k.max.snappy":                  "8",
	}
	var sb strings.Builder
	appendTreeDBLeafScanVlogSummaryLines(&sb, stats)
	got := sb.String()
	for _, want := range []string{
		"vlog_leaf_scan.write_mode.block: frames=2 stored_ratio=0.500000",
		"vlog_leaf_scan.outer_leaf_codec.snappy: frames=2",
		"vlog_leaf_scan.outer_leaf_codec.legacy_page: frames=1",
		"vlog_leaf_scan.block.k.snappy: count=2 max=8",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("summary missing %q in:\n%s", want, got)
		}
	}
}

func TestMergeTreeDBLeafVLogCodecStats_FillsZeroAggregateStats(t *testing.T) {
	dst := map[string]string{
		"treedb.cache.vlog_write_mode.frames.block": "0",
	}
	leaf := map[string]string{
		"treedb.cache.vlog_write_mode.frames.block": "2",
	}
	mergeTreeDBLeafVLogCodecStats(dst, leaf)
	if got := dst["treedb.cache.vlog_write_mode.frames.block"]; got != "2" {
		t.Fatalf("zero aggregate not filled: %q", got)
	}
	if _, ok := dst["treedb.cache.vlog_leaf_scan.write_mode.frames.block"]; ok {
		t.Fatalf("unexpected leaf-only key when aggregate was zero: %#v", dst)
	}
}

func TestRenderMarkdownSweep_KeepDirIncludesKeptSection(t *testing.T) {
	makeRun := func(keys int, dir string) BenchRun {
		return BenchRun{
			Config: BenchConfig{
				Keys:         keys,
				ValueSize:    16,
				BatchSize:    10,
				RangeQueries: 0,
				RangeSpan:    0,
				KeepDir:      true,
			},
			Instances: []*DBInstance{
				{Name: "fake", Wrapper: &fixedNameDB{name: "FakeDB"}, Dir: dir},
			},
			TestOrder: []string{"batch_write"},
			DisplayNames: map[string]string{
				"batch_write": "Batch Write",
			},
			Results: map[string]map[string]float64{
				"batch_write": {
					"FakeDB": 1234,
				},
			},
		}
	}

	md := renderMarkdownSweep([]BenchRun{
		makeRun(100, "/tmp/bench-fake-100"),
		makeRun(200, "/tmp/bench-fake-200"),
	})
	if !strings.Contains(md, "## Kept Data Directories") {
		t.Fatalf("expected kept directories section, got:\n%s", md)
	}
	if !strings.Contains(md, "keys=100") || !strings.Contains(md, "keys=200") {
		t.Fatalf("expected keyed kept-directory subsections, got:\n%s", md)
	}
	if !strings.Contains(md, "FakeDB: /tmp/bench-fake-100") || !strings.Contains(md, "FakeDB: /tmp/bench-fake-200") {
		t.Fatalf("expected kept directory rows, got:\n%s", md)
	}
}

func TestTreeDBOptionsReportFlushBacklogCoalescingEffectiveDefaults(t *testing.T) {
	rep := treeDBOptionsReport{opts: treedb.Options{FlushBacklogCoalescing: true}}
	text := rep.formatText("")
	for _, want := range []string{
		"flush_backlog_coalescing=true",
		"flush_backlog_coalescing_max_memtables=default (effective=64)",
		"flush_backlog_coalescing_max_bytes=default (effective=536870912)",
		"flush_backlog_coalescing_max_ops=default (effective=2097152)",
		"flush_backlog_coalescing_single_op_ratio=default (effective=0.500000)",
		"flush_backlog_coalescing_max_ops_per_span=default (effective=4.000000)",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected %q in resolved options, got:\n%s", want, text)
		}
	}
}

func TestTreeDBOptionsReportFlushBacklogCoalescingEffectiveCaps(t *testing.T) {
	rep := treeDBOptionsReport{opts: treedb.Options{
		FlushBacklogCoalescing:                  true,
		FlushBacklogCoalescingMaxMemtables:      256,
		FlushBacklogCoalescingSingleOpSpanRatio: 2,
	}}
	text := rep.formatText("")
	for _, want := range []string{
		"flush_backlog_coalescing_max_memtables=256 (effective=128 cap)",
		"flush_backlog_coalescing_single_op_ratio=2.000000 (effective=1.000000 cap)",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected %q in resolved options, got:\n%s", want, text)
		}
	}
}

func TestRenderTreeDBSelectedStatsString_SkipsDBsWithoutSelectedKeys(t *testing.T) {
	instances := []*DBInstance{
		{Name: "tree", Wrapper: &fixedNameDB{name: "TreeDB"}},
		{Name: "other", Wrapper: &fixedNameDB{name: "OtherDB"}},
	}
	stats := map[string]map[string]string{
		"TreeDB": {
			"treedb.cache.vlog_mmap.read.hits": "7",
		},
		"OtherDB": {
			"unrelated.stat": "1",
		},
	}

	got := renderTreeDBSelectedStatsString(instances, stats)
	if !strings.Contains(got, "TreeDB:\n") {
		t.Fatalf("expected TreeDB stats, got:\n%s", got)
	}
	if strings.Contains(got, "OtherDB:\n") {
		t.Fatalf("expected OtherDB to be omitted, got:\n%s", got)
	}
}

func TestRenderTreeDBSelectedStatsString_IncludesSpanRunProofCounters(t *testing.T) {
	instances := []*DBInstance{{Name: "tree", Wrapper: &fixedNameDB{name: "TreeDB"}}}
	stats := map[string]map[string]string{
		"TreeDB": {
			"treedb.flush_admission.policy":                                                                                           "auto",
			"treedb.flush_admission.admitted":                                                                                         "true",
			"treedb.flush_admission.reason":                                                                                           "auto_admitted_hardware_aware",
			"treedb.flush_admission.flush_apply_concurrency_configured":                                                               "0",
			"treedb.flush_admission.flush_apply_concurrency":                                                                          "6",
			"treedb.flush_admission.flush_apply_concurrency_cap_reason":                                                               "default_physical_cores",
			"treedb.flush_admission.flush_apply_concurrency_defaulted":                                                                "true",
			"treedb.flush_admission.gomaxprocs":                                                                                       "16",
			"treedb.flush_admission.physical_cores":                                                                                   "6",
			"treedb.flush_admission.flush_apply_span_native":                                                                          "true",
			"treedb.flush_admission.flush_backlog_coalescing":                                                                         "true",
			"treedb.flush_admission.leaf_page_read_cache_write_admission":                                                             "adaptive",
			"treedb.cache.memtable_shards":                                                                                            "16",
			"treedb.cache.journal_lanes.configured":                                                                                   "0",
			"treedb.cache.journal_lanes.defaulted":                                                                                    "true",
			"treedb.cache.journal_lanes.effective":                                                                                    "3",
			"treedb.cache.journal_lanes.hot":                                                                                          "1",
			"treedb.cache.journal_lanes.warm":                                                                                         "1",
			"treedb.cache.journal_lanes.cold":                                                                                         "1",
			"treedb.cache.flush_span_run.source_point_ops_total":                                                                      "11",
			"treedb.cache.flush_span_run.planned_point_ops_total":                                                                     "10",
			"treedb.cache.flush_span_run.backend_chunks_total":                                                                        "3",
			"treedb.cache.leaf_log_lanes.configured":                                                                                  "4",
			"treedb.cache.leaf_log_lanes.active":                                                                                      "4",
			"treedb.cache.leaf_log_lanes.append_lanes_used":                                                                           "3",
			"treedb.cache.leaf_log_lanes.append_calls_total":                                                                          "12",
			"treedb.cache.leaf_log_lanes.append_pages_total":                                                                          "21",
			"treedb.cache.leaf_log_lanes.append_bytes_total":                                                                          "4096",
			"treedb.cache.leaf_log_lanes.append_lock_wait_ns_total":                                                                   "700",
			"treedb.cache.leaf_log_lanes.append_lock_hold_ns_total":                                                                   "900",
			"treedb.cache.leaf_log_lanes.append_errors_total":                                                                         "1",
			"treedb.cache.leaf_log_lanes.segment_rotations_total":                                                                     "2",
			"treedb.cache.flush_span_run.target_leaf_spans_total":                                                                     "4",
			"treedb.cache.flush_span_run.target_leaves_split_across_chunks_total":                                                     "1",
			"treedb.cache.flush_span_run.single_op_span_ratio":                                                                        "0.250000",
			"treedb.cache.flush_backlog_coalescing.admitted_runs_total":                                                               "2",
			"treedb.cache.flush_backlog_coalescing.selected_memtables_max":                                                            "8",
			"treedb.cache.flush_backlog_coalescing.skip.reason.memory_budget_total":                                                   "1",
			"treedb.cache.checkpoint.flushmu_wait_total_ms":                                                                           "1.250",
			"treedb.cache.checkpoint.stage.value_log_flush.total_ns":                                                                  "22",
			"treedb.cache.checkpoint.stage.flush_all.total_ns":                                                                        "33",
			"treedb.cache.checkpoint.stage.reducer_publish.total_ns":                                                                  "44",
			"treedb.flush_apply.old_leaf_read_decode.bytes_per_op":                                                                    "2.200000",
			"treedb.flush_apply.leaf_log_output.append_wait_ns_total":                                                                 "1234",
			"treedb.flush_apply.leaf_log_output.append_calls_total":                                                                   "5",
			"treedb.flush_apply.leaf_log_output.append_pages_total":                                                                   "9",
			"treedb.flush_apply.leaf_log_output.lane.tasks_total":                                                                     "8",
			"treedb.flush_apply.leaf_log_output.lane.tasks_lanes_used":                                                                "4",
			"treedb.flush_apply.leaf_log_output.lane.tasks_max":                                                                       "3",
			"treedb.flush_apply.leaf_log_output.lane.tasks_overflow_total":                                                            "0",
			"treedb.flush_apply.span_native.scheduler.worker_busy_ns_total":                                                           "7000",
			"treedb.flush_apply.span_native.scheduler.worker_idle_ns_total":                                                           "3000",
			"treedb.flush_apply.span_native.scheduler.worker_wait_ns_total":                                                           "2500",
			"treedb.flush_apply.span_native.scheduler.ready_tasks_total":                                                              "8",
			"treedb.flush_apply.span_native.scheduler.dispatched_tasks_total":                                                         "8",
			"treedb.flush_apply.span_native.scheduler.completed_tasks_total":                                                          "8",
			"treedb.flush_apply.span_native.scheduler.queue_depth_max":                                                                "4",
			"treedb.flush_apply.span_native.scheduler.scheduled_workers_total":                                                        "4",
			"treedb.flush_apply.span_native.scheduler.scheduled_workers_max":                                                          "4",
			"treedb.flush_apply.span_native.scheduler.task_spans_per_task":                                                            "6.000000",
			"treedb.flush_apply.span_native.scheduler.task_spans_max":                                                                 "9",
			"treedb.flush_apply.span_native.scheduler.task_ops_per_task":                                                              "7.000000",
			"treedb.flush_apply.span_native.scheduler.task_ops_max":                                                                   "11",
			"treedb.flush_apply.span_native.scheduler.task_bytes_per_task":                                                            "512.000000",
			"treedb.flush_apply.span_native.scheduler.task_bytes_max":                                                                 "1024",
			"treedb.flush_apply.span_native.scheduler.single_span_tasks_total":                                                        "1",
			"treedb.flush_apply.publish_prepare.ns_total":                                                                             "44",
			"treedb.flush_apply.publish_final_install.ns_total":                                                                       "55",
			"treedb.flush_apply.publish_total.ns_total":                                                                               "99",
			"treedb.flush_apply.reducer_publish.ns_total":                                                                             "99",
			"treedb.flush_apply.span_native.fallback.reason.span_native_not_implemented.ops_total":                                    "10",
			"treedb.flush_apply.span_native.fallback.reason.root_mismatch.ops_total":                                                  "1",
			"treedb.flush_apply.span_native.fallback.reason.close_or_checkpoint.ops_total":                                            "2",
			"treedb.flush_apply.span_native.fallback.reason.output_ownership_failure.ops_total":                                       "3",
			"treedb.flush_apply.span_native.fallback.reason.reducer_validation_failed.ops_total":                                      "4",
			"treedb.publish.ordered_root_delta_group.span_native.candidate_ops_total":                                                 "12",
			"treedb.publish.ordered_root_delta_group.span_native.eligible_ops_total":                                                  "0",
			"treedb.publish.ordered_root_delta_group.span_native.used_ops_total":                                                      "0",
			"treedb.publish.ordered_root_delta_group.span_native.ineligible_ops_total":                                                "12",
			"treedb.publish.ordered_root_delta_group.span_native.fallbacks_total":                                                     "3",
			"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.span_native_not_implemented.count_total":             "3",
			"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.span_native_not_implemented.ops_total":               "12",
			"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.prepare_error.count_total":                           "1",
			"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.prepare_error.ops_total":                             "2",
			"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.unknown.count_total":                                 "0",
			"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.unknown.ops_total":                                   "0",
			"treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish.observations_total":                        "3",
			"treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish.candidate_ops_total":                       "12",
			"treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish.fallbacks_total":                           "3",
			"treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish.fallback.reason.prepare_error.count_total": "1",
			"treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish.fallback.reason.prepare_error.ops_total":   "2",
			"treedb.publish.ordered_root_delta_group.span_native.route.command_wal_publish.observations_total":                        "1",
			"treedb.publish.ordered_root_delta_group.span_native.route.command_wal_publish.candidate_ops_total":                       "4",
			"treedb.publish.ordered_root_delta_group.span_native.triage.route.delta_batch_publish.status":                             "fallback",
			"treedb.publish.ordered_root_delta_group.span_native.triage.route.delta_batch_publish.fallback_reason":                    "span_native_not_implemented",
			"treedb.publish.ordered_root_delta_group.span_native.triage.route.delta_batch_publish.admission_reason":                   "auto_admitted_hardware_aware",
			"treedb.publish.ordered_root_delta_group.span_native.triage.route.command_wal_publish.status":                             "fallback",
			"treedb.publish.ordered_root_delta_group.span_native.triage.route.command_wal_publish.fallback_reason":                    "span_native_not_implemented",
			"treedb.publish.ordered_root_delta_group.span_native.triage.route.overlay_cold_build.status":                              "ineligible",
			"treedb.publish.ordered_root_delta_group.span_native.triage.route.overlay_cold_build.fallback_reason":                     "cold_build",
		},
	}

	got := renderTreeDBSelectedStatsString(instances, stats)
	for _, want := range []string{
		"flush_admission.policy: auto",
		"flush_admission.admitted: true",
		"flush_admission.reason: auto_admitted_hardware_aware",
		"flush_admission.flush_apply_concurrency_configured: 0",
		"flush_admission.flush_apply_concurrency: 6",
		"flush_admission.flush_apply_concurrency_cap_reason: default_physical_cores",
		"flush_admission.flush_apply_concurrency_defaulted: true",
		"flush_admission.gomaxprocs: 16",
		"flush_admission.physical_cores: 6",
		"flush_admission.flush_apply_span_native: true",
		"flush_admission.flush_backlog_coalescing: true",
		"flush_admission.leaf_page_read_cache_write_admission: adaptive",
		"memtable_shards: 16",
		"journal_lanes.configured: 0",
		"journal_lanes.defaulted: true",
		"journal_lanes.effective: 3",
		"journal_lanes.hot: 1",
		"journal_lanes.warm: 1",
		"journal_lanes.cold: 1",
		"flush_span_run.source_point_ops_total: 11",
		"flush_span_run.planned_point_ops_total: 10",
		"flush_span_run.backend_chunks_total: 3",
		"leaf_log_lanes.configured: 4",
		"leaf_log_lanes.active: 4",
		"leaf_log_lanes.append_lanes_used: 3",
		"leaf_log_lanes.append_calls_total: 12",
		"leaf_log_lanes.append_pages_total: 21",
		"leaf_log_lanes.append_bytes_total: 4096",
		"leaf_log_lanes.append_lock_wait_ns_total: 700",
		"leaf_log_lanes.append_lock_hold_ns_total: 900",
		"leaf_log_lanes.append_errors_total: 1",
		"leaf_log_lanes.segment_rotations_total: 2",
		"flush_span_run.target_leaf_spans_total: 4",
		"flush_span_run.target_leaves_split_across_chunks_total: 1",
		"flush_span_run.single_op_span_ratio: 0.250000",
		"flush_backlog_coalescing.admitted_runs_total: 2",
		"flush_backlog_coalescing.selected_memtables_max: 8",
		"flush_backlog_coalescing.skip.memory_budget_total: 1",
		"checkpoint.flushmu_wait_total_ms: 1.250",
		"checkpoint.stage.value_log_flush.total_ns: 22",
		"checkpoint.stage.flush_all.total_ns: 33",
		"checkpoint.stage.reducer_publish.total_ns: 44",
		"flush_apply.old_leaf_read_decode.bytes_per_op: 2.200000",
		"flush_apply.leaf_log_output.append_wait_ns_total: 1234",
		"flush_apply.leaf_log_output.append_calls_total: 5",
		"flush_apply.leaf_log_output.append_pages_total: 9",
		"flush_apply.leaf_log_output.lane.tasks_total: 8",
		"flush_apply.leaf_log_output.lane.tasks_lanes_used: 4",
		"flush_apply.leaf_log_output.lane.tasks_max: 3",
		"flush_apply.leaf_log_output.lane.tasks_overflow_total: 0",
		"flush_apply.span_native.scheduler.worker_busy_ns_total: 7000",
		"flush_apply.span_native.scheduler.worker_idle_ns_total: 3000",
		"flush_apply.span_native.scheduler.worker_wait_ns_total: 2500",
		"flush_apply.span_native.scheduler.ready_tasks_total: 8",
		"flush_apply.span_native.scheduler.dispatched_tasks_total: 8",
		"flush_apply.span_native.scheduler.completed_tasks_total: 8",
		"flush_apply.span_native.scheduler.queue_depth_max: 4",
		"flush_apply.span_native.scheduler.scheduled_workers_total: 4",
		"flush_apply.span_native.scheduler.scheduled_workers_max: 4",
		"flush_apply.span_native.scheduler.task_spans_per_task: 6.000000",
		"flush_apply.span_native.scheduler.task_spans_max: 9",
		"flush_apply.span_native.scheduler.task_ops_per_task: 7.000000",
		"flush_apply.span_native.scheduler.task_ops_max: 11",
		"flush_apply.span_native.scheduler.task_bytes_per_task: 512.000000",
		"flush_apply.span_native.scheduler.task_bytes_max: 1024",
		"flush_apply.span_native.scheduler.single_span_tasks_total: 1",
		"flush_apply.publish_prepare.ns_total: 44",
		"flush_apply.publish_final_install.ns_total: 55",
		"flush_apply.publish_total.ns_total: 99",
		"flush_apply.reducer_publish.ns_total: 99",
		"flush_apply.span_native.fallback.not_implemented_ops_total: 10",
		"flush_apply.span_native.fallback.root_mismatch_ops_total: 1",
		"flush_apply.span_native.fallback.close_or_checkpoint_ops_total: 2",
		"flush_apply.span_native.fallback.output_ownership_failure_ops_total: 3",
		"flush_apply.span_native.fallback.reducer_validation_failed_ops_total: 4",
		"publish.ordered_root_delta_group.span_native.candidate_ops_total: 12",
		"publish.ordered_root_delta_group.span_native.eligible_ops_total: 0",
		"publish.ordered_root_delta_group.span_native.used_ops_total: 0",
		"publish.ordered_root_delta_group.span_native.ineligible_ops_total: 12",
		"publish.ordered_root_delta_group.span_native.fallbacks_total: 3",
		"publish.ordered_root_delta_group.span_native.fallback.not_implemented_count_total: 3",
		"publish.ordered_root_delta_group.span_native.fallback.not_implemented_ops_total: 12",
		"publish.ordered_root_delta_group.span_native.fallback.prepare_error_count_total: 1",
		"publish.ordered_root_delta_group.span_native.fallback.prepare_error_ops_total: 2",
		"publish.ordered_root_delta_group.span_native.fallback.unknown_count_total: 0",
		"publish.ordered_root_delta_group.span_native.fallback.unknown_ops_total: 0",
		"publish.ordered_root_delta_group.span_native.route.delta_batch_publish.observations_total: 3",
		"publish.ordered_root_delta_group.span_native.route.delta_batch_publish.candidate_ops_total: 12",
		"publish.ordered_root_delta_group.span_native.route.delta_batch_publish.fallbacks_total: 3",
		"publish.ordered_root_delta_group.span_native.route.delta_batch_publish.fallback.reason.prepare_error.count_total: 1",
		"publish.ordered_root_delta_group.span_native.route.delta_batch_publish.fallback.reason.prepare_error.ops_total: 2",
		"publish.ordered_root_delta_group.span_native.route.command_wal_publish.observations_total: 1",
		"publish.ordered_root_delta_group.span_native.route.command_wal_publish.candidate_ops_total: 4",
		"publish.ordered_root_delta_group.span_native.triage.delta_batch_publish.status: fallback",
		"publish.ordered_root_delta_group.span_native.triage.delta_batch_publish.fallback_reason: span_native_not_implemented",
		"publish.ordered_root_delta_group.span_native.triage.delta_batch_publish.admission_reason: auto_admitted_hardware_aware",
		"publish.ordered_root_delta_group.span_native.triage.command_wal_publish.status: fallback",
		"publish.ordered_root_delta_group.span_native.triage.command_wal_publish.fallback_reason: span_native_not_implemented",
		"publish.ordered_root_delta_group.span_native.triage.overlay_cold_build.status: ineligible",
		"publish.ordered_root_delta_group.span_native.triage.overlay_cold_build.fallback_reason: cold_build",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected %q in selected stats, got:\n%s", want, got)
		}
	}
}

func TestRenderTreeDBSelectedStatsString_PrefersBackendMmapReadFamily(t *testing.T) {
	instances := []*DBInstance{
		{Name: "tree", Wrapper: &fixedNameDB{name: "TreeDB"}},
	}
	stats := map[string]map[string]string{
		"TreeDB": {
			"treedb.cache.vlog_mmap.read.hits":              "0",
			"treedb.cache.vlog_mmap.read.fallback_readat":   "0",
			"treedb.vlog.mmap_read.hits":                    "10626606",
			"treedb.vlog.mmap_read.miss_no_mapping":         "10",
			"treedb.vlog.mmap_read.fallback_readat":         "0",
			"treedb.vlog.mmap_read.hit_ratio":               "1.000000",
			"treedb.vlog.mmap_max_mapped_leaf_sealed_bytes": "2147483648",
		},
	}

	got := renderTreeDBSelectedStatsString(instances, stats)
	for _, want := range []string{
		"vlog_mmap.read.hits: 10626606",
		"vlog_mmap.read.miss_no_mapping: 10",
		"vlog_mmap.read.hit_ratio: 1.000000",
		"vlog_mmap.max_mapped_leaf_sealed_bytes: 2147483648",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected %q in selected stats, got:\n%s", want, got)
		}
	}
	if strings.Contains(got, "vlog_mmap.read.hits: 0") {
		t.Fatalf("selected stats used stale cache alias instead of backend family:\n%s", got)
	}
}

func TestSnapshotSelectedTreeDBStats_PrefersBackendMmapReadFamilyAndKeepsValidZero(t *testing.T) {
	db := &statsOnlyDB{
		fixedNameDB: fixedNameDB{name: "TreeDB"},
		stats: map[string]string{
			"treedb.cache.vlog_mmap.read.hits":      "99",
			"treedb.vlog.mmap_read.hits":            "0",
			"treedb.vlog.mmap_read.fallback_readat": "0",
		},
	}

	got := snapshotSelectedTreeDBStats(db)
	if got.mmapHits != 0 || got.mmapFallbackReadAt != 0 {
		t.Fatalf("expected valid backend zero values to win, got %+v", got)
	}
}

func TestComputeTreeDBPerfMetrics_PopulatesMmapDeltaFromBackendStats(t *testing.T) {
	before := snapshotSelectedTreeDBStats(&statsOnlyDB{
		fixedNameDB: fixedNameDB{name: "TreeDB"},
		stats: map[string]string{
			"treedb.cache.vlog_mmap.read.hits":            "0",
			"treedb.cache.vlog_mmap.read.fallback_readat": "0",
			"treedb.vlog.mmap_read.hits":                  "10",
			"treedb.vlog.mmap_read.fallback_readat":       "4",
		},
	})
	after := snapshotSelectedTreeDBStats(&statsOnlyDB{
		fixedNameDB: fixedNameDB{name: "TreeDB"},
		stats: map[string]string{
			"treedb.cache.vlog_mmap.read.hits":            "0",
			"treedb.cache.vlog_mmap.read.fallback_readat": "0",
			"treedb.vlog.mmap_read.hits":                  "35",
			"treedb.vlog.mmap_read.fallback_readat":       "9",
		},
	})

	got := computeTreeDBPerfMetrics(before, after, treeDBSnapshotPerfMetrics{})
	if got.Mmap.Hits != 25 || got.Mmap.FallbackReadAt != 5 {
		t.Fatalf("unexpected backend mmap deltas: %+v", got.Mmap)
	}
	if got.Mmap.HitRatio != 0.8333333333333334 {
		t.Fatalf("unexpected hit ratio: %.12f", got.Mmap.HitRatio)
	}
}

func TestRenderMarkdownSweep_IncludesTreeDBPerfAndStatsSections(t *testing.T) {
	makeRun := func(keys int, hits uint64, pinned int64) BenchRun {
		return BenchRun{
			Config: BenchConfig{
				Keys:         keys,
				ValueSize:    16,
				BatchSize:    10,
				RangeQueries: 0,
				RangeSpan:    0,
			},
			Instances: []*DBInstance{
				{Name: "tree", Wrapper: &fixedNameDB{name: "TreeDB"}},
			},
			TestOrder: []string{"random_read_parallel"},
			DisplayNames: map[string]string{
				"random_read_parallel": "Random Read (Parallel)",
			},
			Results: map[string]map[string]float64{
				"random_read_parallel": {
					"TreeDB": 1234,
				},
			},
			TreeDBPerf: map[string]map[string]treeDBPerfMetrics{
				"random_read_parallel": {
					"TreeDB": {
						Mmap: treeDBMmapPerfMetrics{
							Hits:           hits,
							FallbackReadAt: 1,
							HitRatio:       0.5,
						},
					},
				},
			},
			TreeDBStats: map[string]map[string]string{
				"TreeDB": {
					"treedb.cache.vlog_mmap.read.hits":           "7",
					"treedb.cache.flush_apply.planning_ns_total": formatInt(100 + int(pinned)),
					"treedb.leaf_generation.generations.pinned":  formatInt(int(pinned)),
				},
			},
		}
	}

	md := renderMarkdownSweep([]BenchRun{
		makeRun(100, 7, 1),
		makeRun(200, 9, 2),
	})
	if !strings.Contains(md, "## TreeDB Perf Instrumentation") {
		t.Fatalf("expected TreeDB perf section, got:\n%s", md)
	}
	if !strings.Contains(md, "keys=100") || !strings.Contains(md, "keys=200") {
		t.Fatalf("expected keyed perf/stats subsections, got:\n%s", md)
	}
	if !strings.Contains(md, "vlog_mmap.read.hits.delta=7") || !strings.Contains(md, "vlog_mmap.read.hits.delta=9") {
		t.Fatalf("expected sweep perf details, got:\n%s", md)
	}
	if !strings.Contains(md, "## TreeDB Selected Stats (End of Run)") {
		t.Fatalf("expected TreeDB selected stats section, got:\n%s", md)
	}
	if !strings.Contains(md, "leaf_generation.generations.pinned: 1") || !strings.Contains(md, "leaf_generation.generations.pinned: 2") {
		t.Fatalf("expected sweep selected stats details, got:\n%s", md)
	}
	if !strings.Contains(md, "flush_apply.cache.planning_ns_total: 101") || !strings.Contains(md, "flush_apply.cache.planning_ns_total: 102") {
		t.Fatalf("expected flush/apply selected stats details, got:\n%s", md)
	}
}

func TestRunFlushDrainSuite_ShortKeys(t *testing.T) {
	cfg := BenchConfig{
		Keys:                   1,
		ValueSize:              128,
		BatchSize:              1000,
		DBsArg:                 "treedb",
		TestsArg:               "all",
		KeepDir:                false,
		Progress:               false,
		SeedUsed:               1,
		CheckpointBetweenTests: true,
		MaxWall:                10 * time.Second,
	}
	if _, err := runFlushDrainSuite(cfg); err != nil {
		t.Fatalf("runFlushDrainSuite failed: %v", err)
	}
}

func TestWriteBenchprofArtifacts_WritesJSONAndMarkdown(t *testing.T) {
	dir := t.TempDir()
	runs := []BenchRun{
		{
			Config: BenchConfig{
				Keys:    123,
				Profile: "fast",
			},
			Results: map[string]map[string]float64{
				"full_scan": {
					"TreeDB": 1000,
				},
				"prefix_scan": {
					"TreeDB": 1200,
				},
			},
			TreeDBPerf: map[string]map[string]treeDBPerfMetrics{
				"full_scan": {
					"TreeDB": {
						Mmap: treeDBMmapPerfMetrics{Hits: 10, FallbackReadAt: 2, HitRatio: 0.833333},
					},
				},
			},
			TreeDBStats: map[string]map[string]string{
				"TreeDB": {
					"treedb.cache.vlog_mmap.read.hits":                                                                                        "10",
					"treedb.cache.vlog_mmap.max_mapped_leaf_sealed_bytes":                                                                     "2147483648",
					"treedb.vlog.mmap_max_mapped_leaf_sealed_segments":                                                                        "512",
					"treedb.cache.flush_apply.planning_ns_total":                                                                              "11",
					"treedb.cache.flush_span_run.source_point_ops_total":                                                                      "11",
					"treedb.cache.flush_span_run.planned_point_ops_total":                                                                     "10",
					"treedb.cache.flush_span_run.backend_chunks_total":                                                                        "3",
					"treedb.cache.checkpoint.stage.reducer_publish.total_ns":                                                                  "44",
					"treedb.flush_apply.old_leaf_read_decode.bytes_total":                                                                     "22",
					"treedb.flush_apply.old_leaf_read_decode.bytes_per_op":                                                                    "2.200000",
					"treedb.flush_apply.leaf_log_output.append_wait_ns_total":                                                                 "1234",
					"treedb.flush_apply.span_native.scheduler.worker_busy_ns_total":                                                           "7000",
					"treedb.flush_apply.span_native.scheduler.worker_idle_ns_total":                                                           "3000",
					"treedb.flush_apply.span_native.scheduler.ready_tasks_total":                                                              "8",
					"treedb.flush_apply.span_native.scheduler.task_spans_per_task":                                                            "6.000000",
					"treedb.flush_apply.publish_prepare.ns_total":                                                                             "44",
					"treedb.flush_apply.publish_final_install.ns_total":                                                                       "55",
					"treedb.flush_apply.publish_total.ns_total":                                                                               "99",
					"treedb.flush_apply.reducer_publish.ns_total":                                                                             "99",
					"treedb.flush_apply.span_native.fallback.reason.span_native_not_implemented.ops_total":                                    "10",
					"treedb.publish.ordered_root_delta_group.root_apply_calls_total":                                                          "4",
					"treedb.publish.ordered_root_delta_group.publish_prepare_ns_total":                                                        "66",
					"treedb.publish.ordered_root_delta_group.span_native.candidate_ops_total":                                                 "12",
					"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.span_native_not_implemented.count_total":             "3",
					"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.span_native_not_implemented.ops_total":               "12",
					"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.prepare_error.count_total":                           "1",
					"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.prepare_error.ops_total":                             "2",
					"treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish.fallback.reason.prepare_error.count_total": "1",
					"treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish.fallback.reason.prepare_error.ops_total":   "2",
					"treedb.publish.ordered_root_delta_group.span_native.triage.route.delta_batch_publish.fallback_reason":                    "span_native_not_implemented",
					"treedb.cache.vlog_auto.frames.block_lz4":                                                                                 "5",
					"treedb.cache.vlog_block.k.bucket.lz4.le_1":                                                                               "5",
					"treedb.cache.vlog_outer_leaf_codec.frames.lz4":                                                                           "5",
					"treedb.unselected": "drop",
				},
			},
		},
	}

	if err := writeBenchprofArtifacts(dir, "native-fastpath", runs); err != nil {
		t.Fatalf("writeBenchprofArtifacts: %v", err)
	}

	jsonPath := filepath.Join(dir, "benchprof_results.json")
	mdPath := filepath.Join(dir, "benchprof_results.md")
	if _, err := os.Stat(jsonPath); err != nil {
		t.Fatalf("expected json output: %v", err)
	}
	if _, err := os.Stat(mdPath); err != nil {
		t.Fatalf("expected markdown output: %v", err)
	}
	mdData, err := os.ReadFile(mdPath)
	if err != nil {
		t.Fatalf("read markdown: %v", err)
	}
	if !strings.Contains(string(mdData), "- execution path: `native-fastpath`") {
		t.Fatalf("markdown missing execution path label:\n%s", string(mdData))
	}
	var parsed benchprofExport
	data, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("read json: %v", err)
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal json: %v", err)
	}
	if len(parsed.Runs) != 1 {
		t.Fatalf("expected 1 run in json, got %d", len(parsed.Runs))
	}
	if got, want := parsed.Runs[0].ExecutionPath, "native-fastpath"; got != want {
		t.Fatalf("unexpected execution path: got %q want %q", got, want)
	}
	if got := parsed.Runs[0].Results["full_scan"]["TreeDB"]; got != 1000 {
		t.Fatalf("unexpected full_scan value: %v", got)
	}
	if got := parsed.Runs[0].TreeDBPerf["full_scan"]["TreeDB"].Mmap.Hits; got != 10 {
		t.Fatalf("unexpected TreeDB perf mmap hits: %v", got)
	}
	if got := parsed.Runs[0].TreeDBStats["TreeDB"]["treedb.publish.ordered_root_delta_group.root_apply_calls_total"]; got != "4" {
		t.Fatalf("unexpected TreeDB selected stat root_apply_calls_total=%q", got)
	}
	if got := parsed.Runs[0].TreeDBStats["TreeDB"]["treedb.publish.ordered_root_delta_group.publish_prepare_ns_total"]; got != "66" {
		t.Fatalf("unexpected TreeDB selected stat publish_prepare_ns_total=%q", got)
	}
	if got := parsed.Runs[0].TreeDBStats["TreeDB"]["treedb.cache.vlog_mmap.max_mapped_leaf_sealed_bytes"]; got != "2147483648" {
		t.Fatalf("unexpected cache leaf mmap budget stat=%q", got)
	}
	if got := parsed.Runs[0].TreeDBStats["TreeDB"]["treedb.vlog.mmap_max_mapped_leaf_sealed_segments"]; got != "512" {
		t.Fatalf("unexpected backend leaf mmap budget stat=%q", got)
	}
	if got := parsed.Runs[0].TreeDBStats["TreeDB"]["treedb.cache.flush_apply.planning_ns_total"]; got != "11" {
		t.Fatalf("unexpected cache flush planning stat=%q", got)
	}
	if got := parsed.Runs[0].TreeDBStats["TreeDB"]["treedb.cache.flush_span_run.source_point_ops_total"]; got != "11" {
		t.Fatalf("unexpected source point ops stat=%q", got)
	}
	if got := parsed.Runs[0].TreeDBStats["TreeDB"]["treedb.cache.flush_span_run.planned_point_ops_total"]; got != "10" {
		t.Fatalf("unexpected planned point ops stat=%q", got)
	}
	if got := parsed.Runs[0].TreeDBStats["TreeDB"]["treedb.cache.flush_span_run.backend_chunks_total"]; got != "3" {
		t.Fatalf("unexpected span-run chunk stat=%q", got)
	}
	if got := parsed.Runs[0].TreeDBStats["TreeDB"]["treedb.cache.checkpoint.stage.reducer_publish.total_ns"]; got != "44" {
		t.Fatalf("unexpected checkpoint reducer/publish stat=%q", got)
	}
	if got := parsed.Runs[0].TreeDBStats["TreeDB"]["treedb.flush_apply.old_leaf_read_decode.bytes_total"]; got != "22" {
		t.Fatalf("unexpected backend flush old-leaf stat=%q", got)
	}
	if got := parsed.Runs[0].TreeDBStats["TreeDB"]["treedb.flush_apply.old_leaf_read_decode.bytes_per_op"]; got != "2.200000" {
		t.Fatalf("unexpected old-leaf bytes/op stat=%q", got)
	}
	for key, want := range map[string]string{
		"treedb.flush_apply.leaf_log_output.append_wait_ns_total":       "1234",
		"treedb.flush_apply.span_native.scheduler.worker_busy_ns_total": "7000",
		"treedb.flush_apply.span_native.scheduler.worker_idle_ns_total": "3000",
		"treedb.flush_apply.span_native.scheduler.ready_tasks_total":    "8",
		"treedb.flush_apply.span_native.scheduler.task_spans_per_task":  "6.000000",
		"treedb.flush_apply.publish_prepare.ns_total":                   "44",
		"treedb.flush_apply.publish_final_install.ns_total":             "55",
		"treedb.flush_apply.publish_total.ns_total":                     "99",
		"treedb.flush_apply.reducer_publish.ns_total":                   "99",
	} {
		if got := parsed.Runs[0].TreeDBStats["TreeDB"][key]; got != want {
			t.Fatalf("unexpected TreeDB scheduler/wait stat %s=%q want %q", key, got, want)
		}
	}
	if got := parsed.Runs[0].TreeDBStats["TreeDB"]["treedb.flush_apply.span_native.fallback.reason.span_native_not_implemented.ops_total"]; got != "10" {
		t.Fatalf("unexpected span-native fallback stat=%q", got)
	}
	for key, want := range map[string]string{
		"treedb.publish.ordered_root_delta_group.span_native.candidate_ops_total":                                                 "12",
		"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.span_native_not_implemented.count_total":             "3",
		"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.span_native_not_implemented.ops_total":               "12",
		"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.prepare_error.count_total":                           "1",
		"treedb.publish.ordered_root_delta_group.span_native.fallback.reason.prepare_error.ops_total":                             "2",
		"treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish.fallback.reason.prepare_error.count_total": "1",
		"treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish.fallback.reason.prepare_error.ops_total":   "2",
		"treedb.publish.ordered_root_delta_group.span_native.triage.route.delta_batch_publish.fallback_reason":                    "span_native_not_implemented",
	} {
		if got := parsed.Runs[0].TreeDBStats["TreeDB"][key]; got != want {
			t.Fatalf("unexpected ordered-root span-native stat %s=%q want %q", key, got, want)
		}
	}
	for key, want := range map[string]string{
		"treedb.cache.vlog_auto.frames.block_lz4":       "5",
		"treedb.cache.vlog_block.k.bucket.lz4.le_1":     "5",
		"treedb.cache.vlog_outer_leaf_codec.frames.lz4": "5",
	} {
		if got := parsed.Runs[0].TreeDBStats["TreeDB"][key]; got != want {
			t.Fatalf("unexpected TreeDB codec stat %s=%q want %q", key, got, want)
		}
	}
	if _, ok := parsed.Runs[0].TreeDBStats["TreeDB"]["treedb.unselected"]; ok {
		t.Fatalf("unselected TreeDB stat was exported: %#v", parsed.Runs[0].TreeDBStats["TreeDB"])
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal raw json: %v", err)
	}
	runsValue, ok := raw["runs"].([]any)
	if !ok || len(runsValue) != 1 {
		t.Fatalf("expected 1 raw run in json, got %#v", raw["runs"])
	}
	runValue, ok := runsValue[0].(map[string]any)
	if !ok {
		t.Fatalf("expected raw run object, got %#v", runsValue[0])
	}
	if _, ok := runValue["treedb_stats"]; !ok {
		t.Fatalf("expected treedb_stats in benchprof json, got: %#v", runValue)
	}
}

func TestWriteBenchprofArtifacts_OmitsNaNResultsForJSON(t *testing.T) {
	dir := t.TempDir()
	run := BenchRun{
		Config: BenchConfig{Keys: 10, Profile: "fast"},
		Results: map[string]map[string]float64{
			"batch_delete_range": {
				"LevelDB":     500,
				"Unsupported": math.NaN(),
			},
			"unsupported_only": {
				"Unsupported": math.NaN(),
			},
		},
	}
	jsonPath := filepath.Join(dir, "benchprof_results.json")
	mdPath := filepath.Join(dir, "benchprof_results.md")
	if err := writeBenchprofArtifactsToPaths(jsonPath, mdPath, "native-fastpath", []BenchRun{run}); err != nil {
		t.Fatalf("writeBenchprofArtifactsToPaths: %v", err)
	}

	data, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("read json: %v", err)
	}
	var parsed benchprofExport
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal json: %v", err)
	}
	results := parsed.Runs[0].Results["batch_delete_range"]
	if got := results["LevelDB"]; got != 500 {
		t.Fatalf("LevelDB result=%v want 500", got)
	}
	if _, ok := results["Unsupported"]; ok {
		t.Fatalf("NaN unsupported result should be omitted from JSON: %s", data)
	}
	unsupportedOnly, ok := parsed.Runs[0].Results["unsupported_only"]
	if !ok {
		t.Fatalf("all-NaN test key should be preserved for profile parsing: %s", data)
	}
	if len(unsupportedOnly) != 0 {
		t.Fatalf("all-NaN test should export an empty result map, got %#v", unsupportedOnly)
	}
}

func TestComputeTreeDBPerfMetrics_SaturatesCounterRegression(t *testing.T) {
	metrics := computeTreeDBPerfMetrics(
		treeDBSelectedStats{
			mmapHits:           11,
			mmapMissOutOfRange: 13,
			mmapMissNoMapping:  17,
			mmapMissDeadCap:    19,
			mmapFallbackReadAt: 23,
			leafGenerationsPin: 5,
			leafPinsTotal:      7,
		},
		treeDBSelectedStats{
			mmapHits:           3,
			mmapMissOutOfRange: 2,
			mmapMissNoMapping:  1,
			mmapMissDeadCap:    18,
			mmapFallbackReadAt: 4,
			leafGenerationsPin: 2,
			leafPinsTotal:      9,
		},
		treeDBSnapshotPerfMetrics{},
	)
	if metrics.Mmap.Hits != 0 || metrics.Mmap.MissOutOfRange != 0 || metrics.Mmap.MissNoMapping != 0 || metrics.Mmap.FallbackReadAt != 0 {
		t.Fatalf("expected saturating mmap deltas, got %+v", metrics.Mmap)
	}
	if metrics.Mmap.MissDeadMapCap != 0 {
		t.Fatalf("expected saturating dead-map delta, got %d", metrics.Mmap.MissDeadMapCap)
	}
	if metrics.LeafGenerationsPinnedAfter != 2 {
		t.Fatalf("expected after-value leaf generations pinned, got %d", metrics.LeafGenerationsPinnedAfter)
	}
	if metrics.LeafPinsTotalAfter != 9 {
		t.Fatalf("expected after-value leaf pins total, got %d", metrics.LeafPinsTotalAfter)
	}
}

func TestWriteBenchprofArtifacts_InvalidExecutionPath(t *testing.T) {
	dir := t.TempDir()
	runs := []BenchRun{{
		Config: BenchConfig{Keys: 1, Profile: "fast"},
		Results: map[string]map[string]float64{
			"full_scan": {"TreeDB": 1},
		},
	}}

	for _, path := range []string{"oracle,native-fastpath", "native-fastpath+legacy"} {
		if err := writeBenchprofArtifacts(dir, path, runs); err == nil {
			t.Fatalf("expected invalid execution path %q to fail", path)
		} else if !strings.Contains(err.Error(), "mixed-path labels are forbidden") {
			t.Fatalf("unexpected error for %q: %v", path, err)
		}
	}
}

func TestWriteBenchprofArtifacts_InvalidExecutionPathListsAllowedValues(t *testing.T) {
	dir := t.TempDir()
	runs := []BenchRun{{
		Config: BenchConfig{Keys: 1, Profile: "fast"},
		Results: map[string]map[string]float64{
			"full_scan": {"TreeDB": 1},
		},
	}}

	err := writeBenchprofArtifacts(dir, "foo", runs)
	if err == nil {
		t.Fatal("expected invalid execution path to fail")
	}
	if !strings.Contains(err.Error(), "expected one of oracle|native-fastpath|m8-m14-10mm-gate|span-native-default-gate|span-native-read-scan-guardrail") {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(err.Error(), "mixed-path labels are forbidden") {
		t.Fatalf("non-mixed invalid label reported as mixed: %v", err)
	}
}

func TestWriteBenchprofArtifacts_AcceptsM8M14GateExecutionPath(t *testing.T) {
	dir := t.TempDir()
	runs := []BenchRun{{
		Config: BenchConfig{Keys: 1, Profile: "fast"},
		Results: map[string]map[string]float64{
			"full_scan": {"TreeDB": 1},
		},
	}}
	if err := writeBenchprofArtifacts(dir, "m8-m14-10mm-gate", runs); err != nil {
		t.Fatalf("m8-m14 gate execution path should be accepted: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "benchprof_results.json"))
	if err != nil {
		t.Fatalf("read benchprof results: %v", err)
	}
	var parsed benchprofExport
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("parse benchprof results: %v", err)
	}
	if len(parsed.Runs) != 1 {
		t.Fatalf("benchprof runs=%d want 1", len(parsed.Runs))
	}
	if got := parsed.Runs[0].ExecutionPath; got != "m8-m14-10mm-gate" {
		t.Fatalf("execution_path=%q want m8-m14-10mm-gate", got)
	}
}

func TestWriteBenchprofArtifacts_AcceptsSpanNativeCloseoutExecutionPaths(t *testing.T) {
	for _, path := range []string{"span-native-default-gate", "span-native-read-scan-guardrail"} {
		t.Run(path, func(t *testing.T) {
			dir := t.TempDir()
			runs := []BenchRun{{
				Config: BenchConfig{Keys: 1, Profile: "fast"},
				Results: map[string]map[string]float64{
					"full_scan": {"TreeDB": 1},
				},
			}}
			if err := writeBenchprofArtifacts(dir, path, runs); err != nil {
				t.Fatalf("%s execution path should be accepted: %v", path, err)
			}
			data, err := os.ReadFile(filepath.Join(dir, "benchprof_results.json"))
			if err != nil {
				t.Fatalf("read benchprof results: %v", err)
			}
			var parsed benchprofExport
			if err := json.Unmarshal(data, &parsed); err != nil {
				t.Fatalf("parse benchprof results: %v", err)
			}
			if len(parsed.Runs) != 1 {
				t.Fatalf("benchprof runs=%d want 1", len(parsed.Runs))
			}
			if got := parsed.Runs[0].ExecutionPath; got != path {
				t.Fatalf("execution_path=%q want %q", got, path)
			}
		})
	}
}

func TestWriteBenchprofArtifacts_DefaultsExecutionPath(t *testing.T) {
	dir := t.TempDir()
	runs := []BenchRun{{
		Config: BenchConfig{Keys: 1, Profile: "fast"},
		Results: map[string]map[string]float64{
			"full_scan": {"TreeDB": 1},
		},
	}}

	if err := writeBenchprofArtifacts(dir, "", runs); err != nil {
		t.Fatalf("missing execution path should default to native-fastpath: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "benchprof_results.json"))
	if err != nil {
		t.Fatalf("read benchprof results: %v", err)
	}
	var parsed benchprofExport
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("parse benchprof results: %v", err)
	}
	if len(parsed.Runs) != 1 {
		t.Fatalf("expected one run, got %d", len(parsed.Runs))
	}
	if got, want := parsed.Runs[0].ExecutionPath, "native-fastpath"; got != want {
		t.Fatalf("unexpected execution path: got %q want %q", got, want)
	}
}

func TestWriteRuntimeProfileDeltaProfile_EmptyOutputSkipsFile(t *testing.T) {
	runner := func(basePath, afterPath string) ([]byte, string, error) {
		return nil, "", nil
	}

	outPath := filepath.Join(t.TempDir(), "block_random_read_treedb.pprof")
	if err := os.WriteFile(outPath, []byte("stale"), 0o644); err != nil {
		t.Fatalf("seed stale output: %v", err)
	}

	wrote, err := writeRuntimeProfileDeltaProfileWithRunner("base.pprof", "after.pprof", outPath, runner)
	if err != nil {
		t.Fatalf("writeRuntimeProfileDeltaProfile: %v", err)
	}
	if wrote {
		t.Fatalf("expected empty delta to skip write")
	}
	if _, err := os.Stat(outPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected no delta file, got stat err=%v", err)
	}
}

func TestGoToolExecutableFallsBackToRuntimeGOROOTWhenPATHMissing(t *testing.T) {
	goroot := runtime.GOROOT()
	if goroot == "" {
		t.Skip("runtime GOROOT unavailable")
	}
	name := "go"
	if runtime.GOOS == "windows" {
		name = "go.exe"
	}
	candidate := filepath.Join(goroot, "bin", name)
	if info, err := os.Stat(candidate); err != nil || info.IsDir() {
		t.Skipf("runtime GOROOT go tool unavailable at %s: stat=(%v, %v)", candidate, info, err)
	}
	t.Setenv("PATH", "")

	path := goToolExecutable()
	if path == "go" || path == "" {
		t.Fatalf("goToolExecutable()=%q, want runtime GOROOT fallback", path)
	}
	if !filepath.IsAbs(path) {
		t.Fatalf("goToolExecutable()=%q, want absolute fallback path", path)
	}
	if info, err := os.Stat(path); err != nil || info.IsDir() {
		t.Fatalf("goToolExecutable()=%q stat=(%v, %v), want executable file", path, info, err)
	}
}

func TestRunBenchmark_ContentionAfterSnapshotsBeforeAllocsPostProcessing(t *testing.T) {
	var events []string
	profileTmpDir := t.TempDir()
	newProfilePath := func(prefix string) (string, error) {
		f, err := os.CreateTemp(profileTmpDir, prefix+"_*.pprof")
		if err != nil {
			return "", err
		}
		path := f.Name()
		if err := f.Close(); err != nil {
			_ = os.Remove(path)
			return "", err
		}
		return path, nil
	}

	profileHooks := &benchmarkProfileHooks{
		startCPUProfile: func(_ io.Writer) error {
			return nil
		},
		stopCPUProfile: func() {
			events = append(events, "cpu_stop")
		},
		writeAllocsSnapshotTemp: func(prefix string) (string, error) {
			events = append(events, prefix)
			return newProfilePath(prefix)
		},
		writeRuntimeProfileSnapshotTemp: func(prefix, profileName string) (string, error) {
			events = append(events, prefix)
			return newProfilePath(prefix)
		},
		writeAllocsDeltaProfile: func(basePath, afterPath, outPath string) error {
			events = append(events, "alloc_delta")
			return nil
		},
		writeRuntimeProfileDeltaProfile: func(basePath, afterPath, outPath string) (bool, error) {
			events = append(events, filepath.Base(outPath))
			return false, nil
		},
	}

	outDir := t.TempDir()
	_, err := runBenchmark(BenchConfig{
		Keys:         64,
		ValueSize:    16,
		BatchSize:    16,
		RangeQueries: 4,
		RangeSpan:    4,
		DBsArg:       "treedb",
		TestsArg:     "sequential_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,

		CPUProfile:    filepath.Join(outDir, "cpu"),
		AllocsProfile: filepath.Join(outDir, "allocs"),
		BlockProfile:  filepath.Join(outDir, "block.pprof"),
		MutexProfile:  filepath.Join(outDir, "mutex.pprof"),
		profileHooks:  profileHooks,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}

	indexOf := func(target string) int {
		for i, evt := range events {
			if evt == target {
				return i
			}
		}
		return -1
	}

	cpuStopIdx := indexOf("cpu_stop")
	blockAfterIdx := indexOf("unified_bench_block_after")
	mutexAfterIdx := indexOf("unified_bench_mutex_after")
	allocAfterIdx := indexOf("unified_bench_allocs_after")

	if cpuStopIdx < 0 || blockAfterIdx < 0 || mutexAfterIdx < 0 || allocAfterIdx < 0 {
		t.Fatalf("missing expected profiling events: %v", events)
	}
	if cpuStopIdx > blockAfterIdx {
		t.Fatalf("expected cpu_stop before block_after, events=%v", events)
	}
	if cpuStopIdx > mutexAfterIdx {
		t.Fatalf("expected cpu_stop before mutex_after, events=%v", events)
	}
	if blockAfterIdx > allocAfterIdx {
		t.Fatalf("expected block_after before allocs_after, events=%v", events)
	}
	if mutexAfterIdx > allocAfterIdx {
		t.Fatalf("expected mutex_after before allocs_after, events=%v", events)
	}
}

func TestRunBenchmarkDatasetWriteAllocsProfilesExcludeFixtureSetup(t *testing.T) {
	for _, testName := range []string{"dataset_write_random", "dataset_write_sorted"} {
		t.Run(testName, func(t *testing.T) {
			outDir := t.TempDir()
			allocPrefix := filepath.Join(outDir, "allocs")

			_, err := runBenchmark(BenchConfig{
				Keys:              2048,
				ValueSize:         256,
				ValuePattern:      "random",
				BatchSize:         256,
				RangeQueries:      0,
				RangeSpan:         0,
				DBsArg:            "treedb",
				TestsArg:          testName,
				KeepDir:           false,
				Progress:          false,
				SeedUsed:          1,
				AllocsProfile:     allocPrefix,
				AllocsProfileRate: 1,
			})
			if err != nil {
				t.Fatalf("runBenchmark: %v", err)
			}

			profilePath := fmt.Sprintf("%s_%s_treedb.pprof", allocPrefix, testName)
			if _, err := os.Stat(profilePath); err != nil {
				t.Fatalf("expected allocs profile %q: %v", profilePath, err)
			}

			cmd := exec.Command(goToolExecutable(), "tool", "pprof", "-top", "-nodecount=0", "-sample_index=alloc_objects", profilePath)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("pprof top: %v\n%s", err, out)
			}
			if strings.Contains(string(out), "main.makeValuePool") {
				t.Fatalf("dataset fixture generation leaked into %s allocs profile:\n%s", testName, out)
			}
		})
	}
}

func TestInstallAllocsProfileRateIgnoresWhitespaceOnlyPrefixM11A(t *testing.T) {
	prevRate := runtime.MemProfileRate
	t.Cleanup(func() {
		runtime.MemProfileRate = prevRate
	})
	runtime.MemProfileRate = 4096

	restore := installAllocsProfileRate(BenchConfig{
		AllocsProfile:     " \t ",
		AllocsProfileRate: 1,
	})
	if got := runtime.MemProfileRate; got != 4096 {
		restore()
		t.Fatalf("MemProfileRate changed for whitespace-only allocs profile: got %d", got)
	}
	restore()
	if got := runtime.MemProfileRate; got != 4096 {
		t.Fatalf("MemProfileRate restore changed whitespace-only allocs profile: got %d", got)
	}
}

func TestBenchConfigHasAnyProfileOutputIncludesCheckpointCPUM11A(t *testing.T) {
	if benchConfigHasAnyProfileOutput(BenchConfig{}) {
		t.Fatal("empty config should not report profile output")
	}
	if !benchConfigHasAnyProfileOutput(BenchConfig{CheckpointCPUProfile: " checkpoint_cpu "}) {
		t.Fatal("checkpoint CPU profile should count as profile output")
	}
}

func TestInstallAllocsProfileRateIgnoresFilteredTestsM11A(t *testing.T) {
	prevRate := runtime.MemProfileRate
	t.Cleanup(func() {
		runtime.MemProfileRate = prevRate
	})
	runtime.MemProfileRate = 4096

	restore := installAllocsProfileRate(BenchConfig{
		AllocsProfile:      filepath.Join(t.TempDir(), "allocs"),
		AllocsProfileRate:  1,
		TestsArg:           "sequential_write",
		AllocsProfileTests: map[string]struct{}{"random_read": {}},
	})
	if got := runtime.MemProfileRate; got != 4096 {
		restore()
		t.Fatalf("MemProfileRate changed for filtered allocs profile: got %d", got)
	}
	restore()
	if got := runtime.MemProfileRate; got != 4096 {
		t.Fatalf("MemProfileRate restore changed filtered allocs profile: got %d", got)
	}
}

func TestInstallAllocsProfileRateTreatsEmptyTestsAsFullSelectionM11A(t *testing.T) {
	prevRate := runtime.MemProfileRate
	t.Cleanup(func() {
		runtime.MemProfileRate = prevRate
	})
	runtime.MemProfileRate = 4096

	restore := installAllocsProfileRate(BenchConfig{
		AllocsProfile:      filepath.Join(t.TempDir(), "allocs"),
		AllocsProfileRate:  1,
		TestsArg:           " \t ",
		AllocsProfileTests: map[string]struct{}{"random_read": {}},
	})
	if got := runtime.MemProfileRate; got != 1 {
		restore()
		t.Fatalf("MemProfileRate was not set for default full test selection: got %d", got)
	}
	restore()
	if got := runtime.MemProfileRate; got != 4096 {
		t.Fatalf("MemProfileRate was not restored for default full test selection: got %d", got)
	}
}

func TestInstallAllocsProfileRateAppliesMatchingTestFilterM11A(t *testing.T) {
	prevRate := runtime.MemProfileRate
	t.Cleanup(func() {
		runtime.MemProfileRate = prevRate
	})
	runtime.MemProfileRate = 4096

	restore := installAllocsProfileRate(BenchConfig{
		AllocsProfile:      filepath.Join(t.TempDir(), "allocs"),
		AllocsProfileRate:  1,
		TestsArg:           "sequential_write",
		AllocsProfileTests: map[string]struct{}{"sequential_write": {}},
	})
	if got := runtime.MemProfileRate; got != 1 {
		restore()
		t.Fatalf("MemProfileRate was not set for matching allocs profile filter: got %d", got)
	}
	restore()
	if got := runtime.MemProfileRate; got != 4096 {
		t.Fatalf("MemProfileRate was not restored for matching allocs profile filter: got %d", got)
	}
}

func TestInstallAllocsProfileRateRestoresEnabledPrefixM11A(t *testing.T) {
	prevRate := runtime.MemProfileRate
	t.Cleanup(func() {
		runtime.MemProfileRate = prevRate
	})
	runtime.MemProfileRate = 4096

	restore := installAllocsProfileRate(BenchConfig{
		AllocsProfile:     filepath.Join(t.TempDir(), "allocs"),
		AllocsProfileRate: 1,
	})
	if got := runtime.MemProfileRate; got != 1 {
		restore()
		t.Fatalf("MemProfileRate was not set for enabled allocs profile: got %d", got)
	}
	restore()
	if got := runtime.MemProfileRate; got != 4096 {
		t.Fatalf("MemProfileRate was not restored for enabled allocs profile: got %d", got)
	}
}

func TestRunBenchmarkCPUProfileStartFailureRemovesArtifactM11A(t *testing.T) {
	for _, testName := range []string{"sequential_write", "batch_write_steady"} {
		t.Run(testName, func(t *testing.T) {
			profileHooks := &benchmarkProfileHooks{
				startCPUProfile: func(_ io.Writer) error {
					return errors.New("start failed")
				},
				stopCPUProfile: func() {},
			}
			prefix := filepath.Join(t.TempDir(), "cpu")

			_, err := runBenchmark(BenchConfig{
				Keys:         8,
				ValueSize:    16,
				BatchSize:    4,
				RangeQueries: 0,
				RangeSpan:    0,
				DBsArg:       "treedb",
				TestsArg:     testName,
				KeepDir:      false,
				Progress:     false,
				SeedUsed:     1,
				CPUProfile:   prefix,
				profileHooks: profileHooks,
			})
			if err == nil {
				t.Fatal("expected CPU profile start failure")
			}
			path := fmt.Sprintf("%s_%s_treedb.pprof", prefix, testName)
			if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("expected failed CPU profile artifact to be removed, stat err=%v", statErr)
			}
		})
	}
}

func TestRunBenchmark_EmptyContentionDeltaOmitsArtifactM11A(t *testing.T) {
	var deltas []string
	profileTmpDir := t.TempDir()
	newProfilePath := func(prefix string) (string, error) {
		f, err := os.CreateTemp(profileTmpDir, prefix+"_*.pprof")
		if err != nil {
			return "", err
		}
		path := f.Name()
		if err := f.Close(); err != nil {
			_ = os.Remove(path)
			return "", err
		}
		return path, nil
	}

	profileHooks := &benchmarkProfileHooks{
		writeRuntimeProfileSnapshotTemp: func(prefix, profileName string) (string, error) {
			return newProfilePath(prefix)
		},
		writeRuntimeProfileDeltaProfile: func(basePath, afterPath, outPath string) (bool, error) {
			deltas = append(deltas, filepath.Base(outPath))
			if err := os.WriteFile(outPath, []byte("stale"), 0o644); err != nil {
				return false, err
			}
			return false, nil
		},
	}

	outDir := t.TempDir()
	_, err := runBenchmark(BenchConfig{
		Keys:         64,
		ValueSize:    16,
		BatchSize:    16,
		RangeQueries: 4,
		RangeSpan:    4,
		DBsArg:       "treedb",
		TestsArg:     "sequential_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,

		BlockProfile: filepath.Join(outDir, "block.pprof"),
		MutexProfile: filepath.Join(outDir, "mutex.pprof"),
		profileHooks: profileHooks,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	for _, name := range []string{
		"block_sequential_write_treedb.pprof",
		"mutex_sequential_write_treedb.pprof",
	} {
		if _, statErr := os.Stat(filepath.Join(outDir, name)); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("expected omitted empty delta artifact %s, stat err=%v", name, statErr)
		}
	}
	if got, want := strings.Join(deltas, ","), "block_sequential_write_treedb.pprof,mutex_sequential_write_treedb.pprof"; got != want {
		t.Fatalf("contention deltas = %s, want %s", got, want)
	}
}

func TestRunBenchmarkRestoresPreviousMutexProfileFractionM11A(t *testing.T) {
	originalFraction := runtime.SetMutexProfileFraction(0)
	t.Cleanup(func() {
		runtime.SetMutexProfileFraction(originalFraction)
	})
	runtime.SetMutexProfileFraction(3)

	_, err := runBenchmark(BenchConfig{
		Keys:                 16,
		ValueSize:            16,
		BatchSize:            8,
		RangeQueries:         1,
		RangeSpan:            4,
		DBsArg:               "treedb",
		TestsArg:             "sequential_write",
		KeepDir:              false,
		Progress:             false,
		SeedUsed:             1,
		MutexProfile:         filepath.Join(t.TempDir(), "mutex.pprof"),
		MutexProfileFraction: 1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if gotPrev := runtime.SetMutexProfileFraction(0); gotPrev != 3 {
		t.Fatalf("mutex profile fraction was not restored: previous=%d want 3", gotPrev)
	}
}

func TestRunBenchmark_IgnoresWhitespaceOnlyRuntimeProfilesM11A(t *testing.T) {
	whitespacePath := " \t "
	_ = os.Remove(whitespacePath)
	t.Cleanup(func() { _ = os.Remove(whitespacePath) })

	var runtimeSnapshots int
	profileHooks := &benchmarkProfileHooks{
		writeRuntimeProfileSnapshotTemp: func(prefix, profileName string) (string, error) {
			runtimeSnapshots++
			return "", errors.New("runtime profile snapshot should be disabled for whitespace-only profile")
		},
	}

	_, err := runBenchmark(BenchConfig{
		Keys:         64,
		ValueSize:    16,
		BatchSize:    16,
		RangeQueries: 4,
		RangeSpan:    4,
		DBsArg:       "treedb",
		TestsArg:     "sequential_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,

		BlockProfile: whitespacePath,
		MutexProfile: whitespacePath,
		TraceProfile: whitespacePath,
		profileHooks: profileHooks,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if runtimeSnapshots != 0 {
		t.Fatalf("runtime profile snapshots = %d, want 0", runtimeSnapshots)
	}
	if _, statErr := os.Stat(whitespacePath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("whitespace trace/profile path was created, stat err=%v", statErr)
	}
}

func TestRenderTreeDBDiskUsageString_EmitsValueLogWithoutWAL(t *testing.T) {
	out := renderTreeDBDiskUsageString(map[string]treeDBDiskUsage{
		"treedb": {
			MainValueLog: walDiskUsage{
				TotalFiles: 1,
				TotalBytes: 2048,
				ValueBytes: 1536,
			},
		},
	})
	if !strings.Contains(out, "maindb/value_vlog:") {
		t.Fatalf("expected value_vlog line, got:\n%s", out)
	}
	if strings.Contains(out, "maindb/wal:") {
		t.Fatalf("did not expect wal line when wal usage is empty, got:\n%s", out)
	}
}

func TestComputeTreeDBDiskUsage_IncludesLeafVLog(t *testing.T) {
	dir := t.TempDir()
	mainDir := filepath.Join(dir, "maindb")
	if err := os.MkdirAll(filepath.Join(mainDir, "leaf_vlog"), 0o755); err != nil {
		t.Fatalf("MkdirAll(leaf_vlog): %v", err)
	}
	if err := os.WriteFile(filepath.Join(mainDir, "leaf_vlog", "value-l255-000001.log"), bytes.Repeat([]byte("x"), 321), 0o644); err != nil {
		t.Fatalf("WriteFile(leaf_vlog): %v", err)
	}

	usage, err := computeTreeDBDiskUsage(dir)
	if err != nil {
		t.Fatalf("computeTreeDBDiskUsage: %v", err)
	}
	if usage.MainLeafLog.TotalFiles != 1 {
		t.Fatalf("MainLeafLog.TotalFiles=%d want 1", usage.MainLeafLog.TotalFiles)
	}
	if usage.MainLeafLog.TotalBytes != 321 {
		t.Fatalf("MainLeafLog.TotalBytes=%d want 321", usage.MainLeafLog.TotalBytes)
	}
}

func TestRenderTreeDBDiskUsageString_EmitsLeafLog(t *testing.T) {
	out := renderTreeDBDiskUsageString(map[string]treeDBDiskUsage{
		"treedb": {
			MainLeafLog: walDiskUsage{
				TotalFiles: 1,
				TotalBytes: 3072,
				ValueBytes: 3072,
			},
		},
	})
	if !strings.Contains(out, "maindb/leaf_vlog:") {
		t.Fatalf("expected leaf_vlog line, got:\n%s", out)
	}
}

func TestRenderTreeDBVlogRewriteString_EmitsValueLogWithoutWAL(t *testing.T) {
	out := renderTreeDBVlogRewriteString(map[string]treeDBVlogRewriteReport{
		"treedb": {
			BeforeUsage: dirDiskUsage{TotalBytes: 4096},
			AfterUsage:  dirDiskUsage{TotalBytes: 2048},
			BeforeTree: treeDBDiskUsage{
				MainValueLog: walDiskUsage{TotalBytes: 4096},
			},
			AfterTree: treeDBDiskUsage{
				MainValueLog: walDiskUsage{TotalBytes: 2048},
			},
			SegmentsBefore: 2,
			SegmentsAfter:  1,
			BytesBefore:    4096,
			BytesAfter:     2048,
			RecordsCopied:  7,
		},
	})
	if !strings.Contains(out, "maindb/value_vlog: 4 KiB -> 2 KiB") {
		t.Fatalf("expected value_vlog rewrite line, got:\n%s", out)
	}
	if strings.Contains(out, "maindb/wal:") {
		t.Fatalf("did not expect wal line when wal usage is empty, got:\n%s", out)
	}
}

func TestRenderTreeDBVlogRewriteString_EmitsLeafLog(t *testing.T) {
	out := renderTreeDBVlogRewriteString(map[string]treeDBVlogRewriteReport{
		"treedb": {
			BeforeUsage: dirDiskUsage{TotalBytes: 8192},
			AfterUsage:  dirDiskUsage{TotalBytes: 4096},
			BeforeTree: treeDBDiskUsage{
				MainLeafLog: walDiskUsage{TotalBytes: 6144},
			},
			AfterTree: treeDBDiskUsage{
				MainLeafLog: walDiskUsage{TotalBytes: 2048},
			},
			SegmentsBefore: 3,
			SegmentsAfter:  1,
			BytesBefore:    6144,
			BytesAfter:     2048,
			RecordsCopied:  5,
		},
	})
	if !strings.Contains(out, "maindb/leaf_vlog: 6 KiB -> 2 KiB") {
		t.Fatalf("expected leaf_vlog rewrite line, got:\n%s", out)
	}
}

func TestRenderTreeDBVlogRewriteString_EmitsPostVacuumBytes(t *testing.T) {
	out := renderTreeDBVlogRewriteString(map[string]treeDBVlogRewriteReport{
		"treedb": {
			BeforeUsage: dirDiskUsage{TotalBytes: 8192},
			AfterUsage:  dirDiskUsage{TotalBytes: 6144},
			AfterVacuum: dirDiskUsage{TotalBytes: 4096},
			VacuumRan:   true,
			BeforeTree: treeDBDiskUsage{
				MainIndexBytes: 4096,
			},
			AfterTree: treeDBDiskUsage{
				MainIndexBytes: 4096,
			},
			AfterVacuumTree: treeDBDiskUsage{
				MainIndexBytes: 2048,
			},
			SegmentsBefore: 2,
			SegmentsAfter:  1,
			BytesBefore:    4096,
			BytesAfter:     2048,
			RecordsCopied:  3,
		},
	})
	if !strings.Contains(out, "bytes: 8 KiB -> 6 KiB -> 4 KiB after index vacuum") {
		t.Fatalf("expected post-vacuum total bytes line, got:\n%s", out)
	}
	if !strings.Contains(out, "maindb/index.db after vacuum: 2 KiB") {
		t.Fatalf("expected post-vacuum index line, got:\n%s", out)
	}
}

func TestRenderTreeDBVlogRewriteString_OmitsPostVacuumBytesWhenDisabled(t *testing.T) {
	out := renderTreeDBVlogRewriteString(map[string]treeDBVlogRewriteReport{
		"treedb": {
			BeforeUsage: dirDiskUsage{TotalBytes: 8192},
			AfterUsage:  dirDiskUsage{TotalBytes: 6144},
			AfterVacuum: dirDiskUsage{TotalBytes: 6144},
			BeforeTree: treeDBDiskUsage{
				MainIndexBytes: 4096,
			},
			AfterTree: treeDBDiskUsage{
				MainIndexBytes: 4096,
			},
			AfterVacuumTree: treeDBDiskUsage{
				MainIndexBytes: 2048,
			},
			SegmentsBefore: 2,
			SegmentsAfter:  1,
			BytesBefore:    4096,
			BytesAfter:     2048,
			RecordsCopied:  3,
		},
	})
	if strings.Contains(out, "after index vacuum") {
		t.Fatalf("did not expect post-vacuum total bytes line, got:\n%s", out)
	}
	if strings.Contains(out, "maindb/index.db after vacuum") {
		t.Fatalf("did not expect post-vacuum index line, got:\n%s", out)
	}
}
