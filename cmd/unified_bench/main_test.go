package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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

func TestRunBenchmark_AllIncludesRandomReadParallel(t *testing.T) {
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
	if len(run.Instances) != 7 {
		t.Fatalf("expected 7 instances, got %d", len(run.Instances))
	}

	got := run.Results["batch_write"]
	wantCols := []string{
		"TreeDB (vlog=off)",
		"TreeDB (vlog=dict)",
		"TreeDB (vlog=block/snappy)",
		"TreeDB (vlog=block/lz4)",
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
				"treedb.cache.vlog_mmap.read.hits":                                                     "7",
				"treedb.cache.vlog_mmap.read.hit_ratio":                                                "0.700000",
				"treedb.applied_command_lsn":                                                           "3",
				"treedb.command_wal.enabled":                                                           "true",
				"treedb.command_wal.required_feature":                                                  "true",
				"treedb.command_wal.live_accepted_frames":                                              "3",
				"treedb.command_wal.live_accepted_max_lsn":                                             "3",
				"treedb.command_wal.live_covered_frames":                                               "3",
				"treedb.command_wal.live_covered_max_lsn":                                              "3",
				"treedb.command_wal.frames":                                                            "3",
				"treedb.command_wal.typed_segments":                                                    "1",
				"treedb.command_wal.max_lsn":                                                           "3",
				"treedb.leaf_generation.generations.pinned":                                            "1",
				"treedb.leaf_generation.pins.total":                                                    "4",
				"treedb.publish.ordered_root_delta_group.calls_total":                                  "9",
				"treedb.publish.ordered_root_delta_group.root_apply_calls_total":                       "11",
				"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_node_bytes_read_total":    "2048",
				"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_page_bytes_written_total": "4096",
				"treedb.publish.ordered_root_delta_group.write_lock_hold_ns_total":                     "12345",
				"treedb.cache.vlog_auto.frames.block_lz4":                                              "7",
				"treedb.cache.vlog_auto.bytes.block_lz4":                                               "3500",
				"treedb.cache.vlog_auto.frames_frac.block_lz4":                                         "1.000000",
				"treedb.cache.vlog_write_mode.frames.block":                                            "7",
				"treedb.cache.vlog_write_mode.raw_bytes.block":                                         "7000",
				"treedb.cache.vlog_write_mode.stored_bytes.block":                                      "3500",
				"treedb.cache.vlog_write_mode.stored_ratio.block":                                      "0.500000",
				"treedb.cache.vlog_payload_kind.frames.outer_leaf":                                     "7",
				"treedb.cache.vlog_payload_kind.raw_bytes.outer_leaf":                                  "7000",
				"treedb.cache.vlog_payload_kind.stored_bytes.outer_leaf":                               "3500",
				"treedb.cache.vlog_payload_kind.stored_ratio.outer_leaf":                               "0.500000",
				"treedb.cache.vlog_payload_split.records.outer_leaf":                                   "7",
				"treedb.cache.vlog_payload_split.raw_bytes.outer_leaf":                                 "7000",
				"treedb.cache.vlog_payload_split.stored_bytes.outer_leaf":                              "3500",
				"treedb.cache.vlog_payload_split.stored_ratio.outer_leaf":                              "0.500000",
				"treedb.cache.vlog_outer_leaf_codec.frames.lz4":                                        "7",
				"treedb.cache.vlog_outer_leaf_codec.raw_bytes.lz4":                                     "7000",
				"treedb.cache.vlog_outer_leaf_codec.stored_bytes.lz4":                                  "3500",
				"treedb.cache.vlog_outer_leaf_codec.stored_ratio.lz4":                                  "0.500000",
				"treedb.cache.vlog_block.k.count.lz4":                                                  "7",
				"treedb.cache.vlog_block.k.avg.lz4":                                                    "1.000",
				"treedb.cache.vlog_block.k.max.lz4":                                                    "1",
				"treedb.cache.vlog_block.ratio.lz4":                                                    "0.500000",
				"treedb.cache.vlog_block.k.bucket.lz4.le_1":                                            "7",
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
		"publish.ordered_root_delta_group.write_lock_hold_ns_total: 12345",
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
	file = append(file, testTreeDBVLogFrame(t, 3, treeDBVlogScanBlockCodecLZ4, true, 300, 120)...)
	file = append(file, testTreeDBVLogFrame(t, 1, 0, false, 50, 50)...)
	if err := os.WriteFile(filepath.Join(leafDir, "value-l255-000001.log"), file, 0o644); err != nil {
		t.Fatalf("WriteFile leaf log: %v", err)
	}

	stats, err := scanTreeDBLeafVLogCodecStats(dir, "TreeDB (vlog=auto)")
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
		"treedb.cache.vlog_outer_leaf_codec.frames.lz4":          "1",
		"treedb.cache.vlog_outer_leaf_codec.frames.none":         "1",
		"treedb.cache.vlog_block.k.count.lz4":                    "1",
		"treedb.cache.vlog_block.k.avg.lz4":                      "3.000",
		"treedb.cache.vlog_block.k.max.lz4":                      "3",
		"treedb.cache.vlog_block.k.bucket.lz4.le_4":              "1",
		"treedb.cache.vlog_block.ratio.lz4":                      "0.400000",
		"treedb.cache.vlog_auto.frames.block_lz4":                "1",
		"treedb.cache.vlog_auto.frames.off":                      "1",
		"treedb.cache.vlog_auto.frames_frac.block_lz4":           "0.500000",
		"treedb.cache.vlog_auto.frames_frac.off":                 "0.500000",
	}
	for key, wantValue := range want {
		if got := stats[key]; got != wantValue {
			t.Fatalf("%s=%q want %q (stats=%#v)", key, got, wantValue, stats)
		}
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
	binary.LittleEndian.PutUint32(out[16:20], uint32(bodyLen))
	copy(out[treeDBVlogScanHeaderSize:], body)
	return out
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
			"treedb.vlog.mmap_max_mapped_leaf_sealed_bytes": "8589934592",
		},
	}

	got := renderTreeDBSelectedStatsString(instances, stats)
	for _, want := range []string{
		"vlog_mmap.read.hits: 10626606",
		"vlog_mmap.read.miss_no_mapping: 10",
		"vlog_mmap.read.hit_ratio: 1.000000",
		"vlog_mmap.max_mapped_leaf_sealed_bytes: 8589934592",
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
					"treedb.cache.vlog_mmap.read.hits":          "7",
					"treedb.leaf_generation.generations.pinned": formatInt(int(pinned)),
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
					"treedb.cache.vlog_mmap.read.hits":                               "10",
					"treedb.cache.vlog_mmap.max_mapped_leaf_sealed_bytes":            "8589934592",
					"treedb.vlog.mmap_max_mapped_leaf_sealed_segments":               "512",
					"treedb.publish.ordered_root_delta_group.root_apply_calls_total": "4",
					"treedb.cache.vlog_auto.frames.block_lz4":                        "5",
					"treedb.cache.vlog_block.k.bucket.lz4.le_1":                      "5",
					"treedb.cache.vlog_outer_leaf_codec.frames.lz4":                  "5",
					"treedb.unselected":                                              "drop",
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
	if got := parsed.Runs[0].TreeDBStats["TreeDB"]["treedb.cache.vlog_mmap.max_mapped_leaf_sealed_bytes"]; got != "8589934592" {
		t.Fatalf("unexpected cache leaf mmap budget stat=%q", got)
	}
	if got := parsed.Runs[0].TreeDBStats["TreeDB"]["treedb.vlog.mmap_max_mapped_leaf_sealed_segments"]; got != "512" {
		t.Fatalf("unexpected backend leaf mmap budget stat=%q", got)
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
	if !strings.Contains(err.Error(), "expected one of oracle|native-fastpath") {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(err.Error(), "mixed-path labels are forbidden") {
		t.Fatalf("non-mixed invalid label reported as mixed: %v", err)
	}
}

func TestWriteBenchprofArtifacts_RequiresExecutionPath(t *testing.T) {
	dir := t.TempDir()
	runs := []BenchRun{{
		Config: BenchConfig{Keys: 1, Profile: "fast"},
		Results: map[string]map[string]float64{
			"full_scan": {"TreeDB": 1},
		},
	}}

	if err := writeBenchprofArtifacts(dir, "", runs); err == nil {
		t.Fatal("expected missing execution path to fail")
	} else if !strings.Contains(err.Error(), "execution path is required") {
		t.Fatalf("unexpected error: %v", err)
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
	profileHooks := &benchmarkProfileHooks{
		startCPUProfile: func(_ io.Writer) error {
			return errors.New("start failed")
		},
		stopCPUProfile: func() {},
	}
	outDir := t.TempDir()
	prefix := filepath.Join(outDir, "cpu")

	_, err := runBenchmark(BenchConfig{
		Keys:         8,
		ValueSize:    16,
		BatchSize:    4,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       "treedb",
		TestsArg:     "sequential_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
		CPUProfile:   prefix,
		profileHooks: profileHooks,
	})
	if err == nil {
		t.Fatal("expected CPU profile start failure")
	}
	path := prefix + "_sequential_write_treedb.pprof"
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected failed CPU profile artifact to be removed, stat err=%v", statErr)
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
