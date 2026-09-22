package dgraphdurability

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/fs"
	"math"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/mvcc"
)

const benchmarkWarmupCommits = 16

var (
	fixedMixedOperationCounts      = []int{50_000, 100_000, 250_000, 500_000}
	concurrentDurableConcurrencies = []int{1, 2, 4, 8, 16}
	concurrentDurableBatchSizes    = []int{1, 16}
	concurrentDurableValueSizes    = []int{128, 4096}
)

type mutation struct {
	key    []byte
	value  []byte
	delete bool
}

type versionedStore interface {
	commitAt(uint64, []mutation) error
	getAt([]byte, uint64) ([]byte, bool, error)
	stats() map[string]string
	close() error
}

type benchmarkProfile struct {
	name    string
	backend string
	class   string
	open    func(testing.TB, string) versionedStore
}

var benchmarkProfiles = []benchmarkProfile{
	{name: "Badger", backend: "badger", class: "relaxed", open: openBadgerRelaxed},
	{name: "TreeDB-command-WAL", backend: "treedb", class: "relaxed", open: openTreeDBRelaxed},
	{name: "Badger", backend: "badger", class: "durable", open: openBadgerDurable},
	{name: "TreeDB-command-WAL", backend: "treedb", class: "durable", open: openTreeDBDurable},
}

// BenchmarkDgraphShapedCommit isolates externally timestamped, independently
// acknowledged commits. The durable rows intentionally include the storage
// engine's power-loss boundary in the timed operation.
func BenchmarkDgraphShapedCommit(b *testing.B) {
	for _, profile := range benchmarkProfiles {
		profile := profile
		for _, batchSize := range []int{1, 16} {
			batchSize := batchSize
			for _, valueSize := range []int{128, 4096} {
				valueSize := valueSize
				name := fmt.Sprintf("%s/%s/batch=%d/value=%d", profile.class, profile.name, batchSize, valueSize)
				b.Run(name, func(b *testing.B) {
					benchmarkCommit(b, profile, batchSize, valueSize)
				})
			}
		}
	}
}

// BenchmarkDgraphShapedMixed uses the Dgraph benchmark's 60/20/20
// read/write/delete proportions while excluding database open and seed work.
func BenchmarkDgraphShapedMixed(b *testing.B) {
	for _, profile := range benchmarkProfiles {
		profile := profile
		b.Run(profile.class+"/"+profile.name, func(b *testing.B) {
			benchmarkMixed(b, profile)
		})
	}
}

// BenchmarkDgraphShapedMixedFixed makes duration-dependent source growth
// visible without relying on the benchmark driver's adaptive operation count.
// Run with -benchtime=1x so each row is one fresh, fixed-size trial.
func BenchmarkDgraphShapedMixedFixed(b *testing.B) {
	for _, operations := range fixedMixedOperationCounts {
		operations := operations
		for _, profile := range benchmarkProfiles {
			if profile.class != "relaxed" {
				continue
			}
			profile := profile
			b.Run(fmt.Sprintf("relaxed/%s/operations=%d/seed=1", profile.name, operations), func(b *testing.B) {
				benchmarkMixedFixed(b, profile, operations)
			})
		}
	}
}

// BenchmarkDgraphShapedConcurrentDurable measures independently acknowledged
// durable commits with per-acknowledgement latency and matching sync counters.
func BenchmarkDgraphShapedConcurrentDurable(b *testing.B) {
	for _, profile := range benchmarkProfiles {
		if profile.class != "durable" {
			continue
		}
		profile := profile
		for _, concurrency := range concurrentDurableConcurrencies {
			for _, batchSize := range concurrentDurableBatchSizes {
				for _, valueSize := range concurrentDurableValueSizes {
					concurrency, batchSize, valueSize := concurrency, batchSize, valueSize
					name := fmt.Sprintf("durable/%s/concurrency=%d/batch=%d/value=%d", profile.name, concurrency, batchSize, valueSize)
					b.Run(name, func(b *testing.B) {
						benchmarkConcurrentAcknowledgement(b, profile, concurrency, batchSize, valueSize)
					})
				}
			}
		}
	}
}

// BenchmarkDgraphShapedConcurrentAcknowledgement reports matched ordinary-ACK
// latency for relaxed profiles and explicit-boundary latency for durable
// profiles. The profile class is part of every row so unlike contracts are not
// used as hard regression denominators.
func BenchmarkDgraphShapedConcurrentAcknowledgement(b *testing.B) {
	for _, profile := range benchmarkProfiles {
		profile := profile
		for _, concurrency := range concurrentDurableConcurrencies {
			for _, batchSize := range concurrentDurableBatchSizes {
				for _, valueSize := range concurrentDurableValueSizes {
					concurrency, batchSize, valueSize := concurrency, batchSize, valueSize
					name := fmt.Sprintf("%s/%s/concurrency=%d/batch=%d/value=%d", profile.class, profile.name, concurrency, batchSize, valueSize)
					b.Run(name, func(b *testing.B) {
						benchmarkConcurrentAcknowledgement(b, profile, concurrency, batchSize, valueSize)
					})
				}
			}
		}
	}
}

func benchmarkCommit(b *testing.B, profile benchmarkProfile, batchSize, valueSize int) {
	store := profile.open(b, b.TempDir())
	mutations := benchmarkMutations(batchSize, valueSize)
	for i := 0; i < benchmarkWarmupCommits; i++ {
		if err := store.commitAt(uint64(i+1), mutations); err != nil {
			b.Fatalf("warmup commit %d: %v", i+1, err)
		}
	}
	before := store.stats()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := store.commitAt(uint64(benchmarkWarmupCommits+i+1), mutations); err != nil {
			b.Fatalf("commit %d: %v", i+1, err)
		}
	}
	b.StopTimer()
	reportRates(b, batchSize)
	reportStoreCounters(b, before, store.stats(), b.N, 0)
	if err := store.close(); err != nil {
		b.Fatalf("close: %v", err)
	}
}

func benchmarkMixed(b *testing.B, profile benchmarkProfile) {
	store := profile.open(b, b.TempDir())
	const keyCount = 256
	keys := make([][]byte, keyCount)
	seed := make([]mutation, keyCount)
	for i := range keys {
		keys[i] = benchmarkKey(i)
		seed[i] = mutation{key: keys[i], value: benchmarkValue(128)}
	}
	if err := store.commitAt(1, seed); err != nil {
		b.Fatalf("seed: %v", err)
	}
	before := store.stats()
	value := benchmarkValue(128)
	timestamp := uint64(1)
	writeCommits := 0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[(i*131)%len(keys)]
		switch i % 10 {
		case 0, 1, 2, 3, 4, 5:
			got, present, err := store.getAt(key, timestamp)
			if err != nil {
				b.Fatalf("read %d: %v", i, err)
			}
			if present && len(got) == 0 {
				b.Fatalf("read %d returned an empty present value", i)
			}
		case 6, 7:
			timestamp++
			if err := store.commitAt(timestamp, []mutation{{key: key, value: value}}); err != nil {
				b.Fatalf("write %d: %v", i, err)
			}
			writeCommits++
		case 8, 9:
			timestamp++
			if err := store.commitAt(timestamp, []mutation{{key: key, delete: true}}); err != nil {
				b.Fatalf("delete %d: %v", i, err)
			}
			writeCommits++
		}
	}
	b.StopTimer()
	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "operations/s")
		b.ReportMetric(float64(writeCommits)/elapsed.Seconds(), "write_commits/s")
	}
	reportStoreCounters(b, before, store.stats(), writeCommits, b.N-writeCommits)
	if err := store.close(); err != nil {
		b.Fatalf("close: %v", err)
	}
}

func benchmarkMixedFixed(b *testing.B, profile benchmarkProfile, operations int) {
	var totalWrites, totalReads int
	var totalAllocatedBytes, totalMallocs uint64
	b.ReportAllocs()
	b.ResetTimer()
	for trial := 0; trial < b.N; trial++ {
		b.StopTimer()
		store, keys, value := prepareMixedStore(b, profile)
		before := store.stats()
		timestamp := uint64(1)
		var memoryBefore, memoryAfter runtime.MemStats
		runtime.ReadMemStats(&memoryBefore)
		b.StartTimer()
		writes, reads := runMixedOperations(b, store, keys, value, &timestamp, operations)
		b.StopTimer()
		runtime.ReadMemStats(&memoryAfter)
		totalAllocatedBytes += memoryAfter.TotalAlloc - memoryBefore.TotalAlloc
		totalMallocs += memoryAfter.Mallocs - memoryBefore.Mallocs
		totalWrites += writes
		totalReads += reads
		reportStoreCounters(b, before, store.stats(), writes, reads)
		if err := store.close(); err != nil {
			b.Fatalf("close: %v", err)
		}
	}
	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(float64(b.N*operations)/elapsed.Seconds(), "operations/s")
		b.ReportMetric(float64(totalWrites)/elapsed.Seconds(), "write_commits/s")
	}
	b.ReportMetric(float64(operations), "operations/trial")
	b.ReportMetric(1, "fixture_seed")
	totalOperations := uint64(b.N * operations)
	if totalOperations > 0 {
		b.ReportMetric(float64(totalAllocatedBytes)/float64(totalOperations), "workload_B/op")
		b.ReportMetric(float64(totalMallocs)/float64(totalOperations), "workload_allocs/op")
	}
}

func prepareMixedStore(tb testing.TB, profile benchmarkProfile) (versionedStore, [][]byte, []byte) {
	store := profile.open(tb, tb.TempDir())
	const keyCount = 256
	keys := make([][]byte, keyCount)
	seed := make([]mutation, keyCount)
	for i := range keys {
		keys[i] = benchmarkKey(i)
		seed[i] = mutation{key: keys[i], value: benchmarkValue(128)}
	}
	if err := store.commitAt(1, seed); err != nil {
		tb.Fatalf("seed: %v", err)
	}
	return store, keys, benchmarkValue(128)
}

func runMixedOperations(tb testing.TB, store versionedStore, keys [][]byte, value []byte, timestamp *uint64, operations int) (writes, reads int) {
	for i := 0; i < operations; i++ {
		key := keys[(i*131)%len(keys)]
		switch i % 10 {
		case 0, 1, 2, 3, 4, 5:
			got, present, err := store.getAt(key, *timestamp)
			if err != nil {
				tb.Fatalf("read %d: %v", i, err)
			}
			if present && len(got) == 0 {
				tb.Fatalf("read %d returned an empty present value", i)
			}
			reads++
		case 6, 7:
			(*timestamp)++
			if err := store.commitAt(*timestamp, []mutation{{key: key, value: value}}); err != nil {
				tb.Fatalf("write %d: %v", i, err)
			}
			writes++
		case 8, 9:
			(*timestamp)++
			if err := store.commitAt(*timestamp, []mutation{{key: key, delete: true}}); err != nil {
				tb.Fatalf("delete %d: %v", i, err)
			}
			writes++
		}
	}
	return writes, reads
}

func benchmarkConcurrentAcknowledgement(b *testing.B, profile benchmarkProfile, concurrency, batchSize, valueSize int) {
	store := profile.open(b, b.TempDir())
	mutations := benchmarkMutations(batchSize, valueSize)
	for i := 0; i < benchmarkWarmupCommits; i++ {
		if err := store.commitAt(uint64(i+1), mutations); err != nil {
			b.Fatalf("warmup commit %d: %v", i+1, err)
		}
	}
	before := store.stats()
	latencies := make([]time.Duration, b.N)
	jobs := make(chan int)
	var nextTimestamp atomic.Uint64
	nextTimestamp.Store(benchmarkWarmupCommits)
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(concurrency)
	b.ReportAllocs()
	b.StopTimer()
	for worker := 0; worker < concurrency; worker++ {
		go func() {
			defer wg.Done()
			<-start
			for index := range jobs {
				started := time.Now()
				timestamp := nextTimestamp.Add(1)
				if err := commitConcurrent(store, timestamp, mutations); err != nil {
					b.Errorf("commit %d: %v", index, err)
				}
				latencies[index] = time.Since(started)
			}
		}()
	}
	b.ResetTimer()
	b.StartTimer()
	close(start)
	for i := 0; i < b.N; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	b.StopTimer()
	reportRates(b, batchSize)
	reportLatencyPercentiles(b, profile.class, latencies)
	reportStoreCounters(b, before, store.stats(), b.N, 0)
	b.ReportMetric(float64(concurrency), "workers")
	if tree, ok := store.(*treeDBStore); ok {
		totalWriteCommits := benchmarkWarmupCommits + b.N
		reportTreeDBLifecycleAndStorage(b, tree, uint64(totalWriteCommits), mutations[0].key, totalWriteCommits)
		return
	}
	if err := store.close(); err != nil {
		b.Fatalf("close: %v", err)
	}
}

func commitConcurrent(store versionedStore, timestamp uint64, mutations []mutation) error {
	if tree, ok := store.(*treeDBStore); ok {
		converted := make([]mvcc.Mutation, len(mutations))
		for i, mutation := range mutations {
			converted[i] = mvcc.Mutation{Key: mutation.key, Value: mutation.value, Delete: mutation.delete}
		}
		return tree.mvcc.CommitAt(timestamp, converted, tree.mode)
	}
	return store.commitAt(timestamp, mutations)
}

func reportLatencyPercentiles(b *testing.B, class string, latencies []time.Duration) {
	if len(latencies) == 0 {
		return
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	for _, percentile := range []struct {
		name string
		p    float64
	}{{"p50_ack_ms", .50}, {"p95_ack_ms", .95}, {"p99_ack_ms", .99}} {
		index := int(math.Ceil(float64(len(latencies))*percentile.p)) - 1
		if index < 0 {
			index = 0
		}
		value := float64(latencies[index]) / float64(time.Millisecond)
		b.ReportMetric(value, percentile.name)
		boundaryName := strings.Replace(percentile.name, "_ack_", "_ordinary_ack_", 1)
		if class == "durable" {
			boundaryName = strings.Replace(percentile.name, "_ack_", "_explicit_boundary_", 1)
		}
		b.ReportMetric(value, boundaryName)
	}
}

func reportRates(b *testing.B, mutationsPerCommit int) {
	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "commits/s")
		b.ReportMetric(float64(b.N*mutationsPerCommit)/elapsed.Seconds(), "mutations/s")
	}
	b.ReportMetric(float64(mutationsPerCommit), "mutations/commit")
}

func benchmarkMutations(batchSize, valueSize int) []mutation {
	value := benchmarkValue(valueSize)
	mutations := make([]mutation, batchSize)
	for i := range mutations {
		mutations[i] = mutation{key: benchmarkKey(i), value: value}
	}
	return mutations
}

func benchmarkKey(index int) []byte {
	key := make([]byte, 24)
	copy(key, "dgraph-posting-")
	binary.BigEndian.PutUint64(key[len(key)-8:], uint64(index))
	return key
}

func benchmarkValue(size int) []byte {
	value := make([]byte, size)
	for i := range value {
		value[i] = byte(i*31 + 17)
	}
	return value
}

type badgerStore struct {
	db         *badger.DB
	syncWrites bool
}

func openBadgerRelaxed(tb testing.TB, dir string) versionedStore {
	return openBadger(tb, dir, false)
}

func openBadgerDurable(tb testing.TB, dir string) versionedStore {
	return openBadger(tb, dir, true)
}

func openBadger(tb testing.TB, dir string, syncWrites bool) versionedStore {
	tb.Helper()
	opts := badger.DefaultOptions(dir).WithLogger(nil).WithSyncWrites(syncWrites)
	opts.NumVersionsToKeep = math.MaxInt32
	opts.DetectConflicts = false
	db, err := badger.OpenManaged(opts)
	if err != nil {
		tb.Fatalf("open Badger syncwrites=%t: %v", syncWrites, err)
	}
	return &badgerStore{db: db, syncWrites: syncWrites}
}

func (s *badgerStore) commitAt(timestamp uint64, mutations []mutation) error {
	txn := s.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()
	for _, mutation := range mutations {
		var err error
		if mutation.delete {
			err = txn.Delete(mutation.key)
		} else {
			err = txn.Set(mutation.key, mutation.value)
		}
		if err != nil {
			return err
		}
	}
	return txn.CommitAt(timestamp, nil)
}

func (s *badgerStore) getAt(key []byte, timestamp uint64) ([]byte, bool, error) {
	txn := s.db.NewTransactionAt(timestamp, false)
	defer txn.Discard()
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	value, err := item.ValueCopy(nil)
	return value, err == nil, err
}

func (s *badgerStore) stats() map[string]string { return nil }
func (s *badgerStore) close() error             { return s.db.Close() }

type treeDBStore struct {
	db      *treedb.DB
	mvcc    *mvcc.Store
	mode    mvcc.CommitMode
	profile treedb.Profile
	dir     string
	scratch []mvcc.Mutation
}

func openTreeDBRelaxed(tb testing.TB, dir string) versionedStore {
	return openTreeDB(tb, dir, treedb.ProfileCommandWALRelaxed, mvcc.CommitRelaxed)
}

func openTreeDBDurable(tb testing.TB, dir string) versionedStore {
	return openTreeDB(tb, dir, treedb.ProfileCommandWALDurable, mvcc.CommitDurable)
}

func openTreeDB(tb testing.TB, dir string, profile treedb.Profile, mode mvcc.CommitMode) versionedStore {
	tb.Helper()
	caching.SetIteratorDebug(true)
	tb.Cleanup(func() { caching.SetIteratorDebug(false) })
	opts := treedb.OptionsFor(profile, dir)
	db, err := treedb.Open(opts)
	if err != nil {
		tb.Fatalf("open TreeDB profile=%s: %v", profile, err)
	}
	return &treeDBStore{db: db, mvcc: mvcc.New(db), mode: mode, profile: profile, dir: dir}
}

func (s *treeDBStore) commitAt(timestamp uint64, mutations []mutation) error {
	if cap(s.scratch) < len(mutations) {
		s.scratch = make([]mvcc.Mutation, len(mutations))
	} else {
		s.scratch = s.scratch[:len(mutations)]
	}
	for i, mutation := range mutations {
		s.scratch[i] = mvcc.Mutation{Key: mutation.key, Value: mutation.value, Delete: mutation.delete}
	}
	return s.mvcc.CommitAt(timestamp, s.scratch, s.mode)
}

func (s *treeDBStore) getAt(key []byte, timestamp uint64) ([]byte, bool, error) {
	result, err := s.mvcc.GetAt(key, timestamp)
	if err != nil {
		return nil, false, err
	}
	switch result.State {
	case mvcc.Absent, mvcc.Tombstone:
		return nil, false, nil
	case mvcc.Present:
		return result.Value, true, nil
	default:
		return nil, false, fmt.Errorf("unexpected MVCC state %d", result.State)
	}
}

func (s *treeDBStore) stats() map[string]string { return s.db.Stats() }
func (s *treeDBStore) close() error             { return s.db.Close() }

type treeDBStorageBytes struct {
	total      uint64
	commandWAL uint64
	valueLog   uint64
}

func reportTreeDBLifecycleAndStorage(b *testing.B, store *treeDBStore, timestamp uint64, key []byte, writeCommits int) {
	b.Helper()
	before := measureTreeDBStorageBytes(b, store.dir)
	reportTreeDBStorageBytes(b, "pre_checkpoint", before, writeCommits)

	started := time.Now()
	if err := store.db.Checkpoint(); err != nil {
		b.Fatalf("checkpoint: %v", err)
	}
	b.ReportMetric(float64(time.Since(started).Nanoseconds()), "checkpoint_ns")
	after := measureTreeDBStorageBytes(b, store.dir)
	reportTreeDBStorageBytes(b, "post_checkpoint", after, writeCommits)

	started = time.Now()
	if err := store.close(); err != nil {
		b.Fatalf("close: %v", err)
	}
	b.ReportMetric(float64(time.Since(started).Nanoseconds()), "close_ns")

	opts := treedb.OptionsFor(store.profile, store.dir)
	started = time.Now()
	reopened, err := treedb.Open(opts)
	if err != nil {
		b.Fatalf("reopen: %v", err)
	}
	b.ReportMetric(float64(time.Since(started).Nanoseconds()), "reopen_ns")
	result, err := mvcc.New(reopened).GetAt(key, timestamp)
	if err != nil || result.State != mvcc.Present {
		_ = reopened.Close()
		b.Fatalf("reopen GetAt state=%d err=%v", result.State, err)
	}
	if err := reopened.Close(); err != nil {
		b.Fatalf("reopened close: %v", err)
	}
}

func measureTreeDBStorageBytes(b *testing.B, root string) treeDBStorageBytes {
	b.Helper()
	var measured treeDBStorageBytes
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		size := uint64(info.Size())
		measured.total += size
		name := filepath.Base(path)
		switch {
		case strings.HasPrefix(name, "commit-l"):
			measured.commandWAL += size
		case strings.HasPrefix(name, "value-l"):
			measured.valueLog += size
		}
		return nil
	})
	if err != nil {
		b.Fatalf("measure TreeDB storage: %v", err)
	}
	return measured
}

func reportTreeDBStorageBytes(b *testing.B, phase string, measured treeDBStorageBytes, writeCommits int) {
	b.Helper()
	denominator := float64(max(writeCommits, 1))
	b.ReportMetric(float64(measured.total)/denominator, phase+"_storage_B/write_commit")
	b.ReportMetric(float64(measured.commandWAL)/denominator, phase+"_command_wal_B/write_commit")
	b.ReportMetric(float64(measured.valueLog)/denominator, phase+"_value_log_B/write_commit")
}

type storeCounterMetric struct {
	key         string
	name        string
	denominator string
}

var storeCounterMetrics = []storeCounterMetric{
	{key: "treedb.command_wal.write.bytes_total", name: "command_wal_write_B/write_commit", denominator: "writes"},
	{key: "treedb.command_wal.sync.count_total", name: "command_wal_syncs/write_commit", denominator: "writes"},
	{key: "treedb.command_wal.flush.count_total", name: "command_wal_flushes/write_commit", denominator: "writes"},
	{key: "treedb.command_wal.file_sync.calls_total", name: "command_wal_file_syncs/write_commit", denominator: "writes"},
	{key: "treedb.command_wal.directory_sync.calls_total", name: "command_wal_directory_syncs/write_commit", denominator: "writes"},
	{key: "treedb.command_wal.group_commit.groups_total", name: "command_wal_group_commits/write_commit", denominator: "writes"},
	{key: "treedb.command_wal.group_commit.commits_total", name: "command_wal_group_acks/write_commit", denominator: "writes"},
	{key: "treedb.command_wal.group_commit.syncs_total", name: "command_wal_group_syncs/write_commit", denominator: "writes"},
	{key: "treedb.command_wal.group_commit.fallbacks_total", name: "command_wal_group_fallbacks/write_commit", denominator: "writes"},
	{key: "treedb.cache.value_log.sync.calls_total", name: "value_log_syncs/write_commit", denominator: "writes"},
	{key: "treedb.cache.value_log.file_sync.calls_total", name: "value_log_file_syncs/write_commit", denominator: "writes"},
	{key: "treedb.cache.value_log.directory_sync.calls_total", name: "value_log_directory_syncs/write_commit", denominator: "writes"},
	{key: "treedb.cache.vlog_writev.bytes", name: "value_log_writev_B/write_commit", denominator: "writes"},
	{key: "treedb.cache.vlog_write.bytes", name: "value_log_write_B/write_commit", denominator: "writes"},
	{key: "treedb.cache.checkpoint.runs", name: "checkpoints/write_commit", denominator: "writes"},
	{key: "treedb.publish.ordered_root_delta_group.calls_total", name: "root_publications/write_commit", denominator: "writes"},
	{key: "treedb.cache.write.wait_for_checkpoint.count_total", name: "checkpoint_caller_waits/write_commit", denominator: "writes"},
	{key: "treedb.cache.flush_apply.coordinator.progress_waits_total", name: "publication_progress_waits/write_commit", denominator: "writes"},
	{key: "treedb.cache.flush_apply.coordinator.stall_waits_total", name: "publication_stall_waits/write_commit", denominator: "writes"},
	{key: "treedb.cache.iterator.snapshot_rotations_total", name: "iterator_snapshot_rotations/lookup", denominator: "lookups"},
	{key: "treedb.cache.iterator.sources_total", name: "iterator_sources/lookup", denominator: "lookups"},
	{key: "treedb.cache.iterator.calls_total", name: "iterator_calls/lookup", denominator: "lookups"},
	{key: "treedb.cache.point_successor.calls_total", name: "point_successor_calls/lookup", denominator: "lookups"},
	{key: "treedb.cache.point_successor.hits_total", name: "point_successor_hits/lookup", denominator: "lookups"},
	{key: "treedb.cache.point_successor.sources_total", name: "point_successor_sources/lookup", denominator: "lookups"},
	{key: "treedb.cache.point_successor.general_merge_iterators_total", name: "point_successor_general_merges/lookup", denominator: "lookups"},
}

func reportStoreCounters(b *testing.B, before, after map[string]string, writeCommits, lookups int) {
	b.Helper()
	for _, metric := range storeCounterMetrics {
		denominator := writeCommits
		if metric.denominator == "lookups" {
			denominator = lookups
		}
		if denominator == 0 {
			continue
		}
		start, startOK := parseCounter(before, metric.key)
		end, endOK := parseCounter(after, metric.key)
		if startOK && endOK && end >= start {
			b.ReportMetric(float64(end-start)/float64(denominator), metric.name)
		}
	}
	groupCommitsStart, commitsStartOK := parseCounter(before, "treedb.command_wal.group_commit.commits_total")
	groupCommitsEnd, commitsEndOK := parseCounter(after, "treedb.command_wal.group_commit.commits_total")
	groupSyncsStart, syncsStartOK := parseCounter(before, "treedb.command_wal.group_commit.syncs_total")
	groupSyncsEnd, syncsEndOK := parseCounter(after, "treedb.command_wal.group_commit.syncs_total")
	if commitsStartOK && commitsEndOK && syncsStartOK && syncsEndOK &&
		groupCommitsEnd >= groupCommitsStart && groupSyncsEnd >= groupSyncsStart {
		commits := groupCommitsEnd - groupCommitsStart
		syncs := groupSyncsEnd - groupSyncsStart
		if syncs > 0 {
			b.ReportMetric(float64(commits)/float64(syncs), "command_wal_group_commits/sync")
		}
		if commits > 0 {
			b.ReportMetric(float64(syncs)/float64(commits), "command_wal_group_syncs/ack")
		}
	}
	if groupSizeMax, ok := parseCounter(after, "treedb.command_wal.group_commit.group_size_max"); ok {
		b.ReportMetric(float64(groupSizeMax), "command_wal_group_size_max")
	}
	groupDependenciesStart, dependenciesStartOK := parseCounter(before, "treedb.command_wal.group_commit.dependency_entries_covered_total")
	groupDependenciesEnd, dependenciesEndOK := parseCounter(after, "treedb.command_wal.group_commit.dependency_entries_covered_total")
	groupCountStart, groupsStartOK := parseCounter(before, "treedb.command_wal.group_commit.groups_total")
	groupCountEnd, groupsEndOK := parseCounter(after, "treedb.command_wal.group_commit.groups_total")
	if dependenciesStartOK && dependenciesEndOK && groupsStartOK && groupsEndOK &&
		groupDependenciesEnd >= groupDependenciesStart && groupCountEnd > groupCountStart {
		b.ReportMetric(
			float64(groupDependenciesEnd-groupDependenciesStart)/float64(groupCountEnd-groupCountStart),
			"command_wal_group_dependencies/group",
		)
	}
	publicationCallsStart, callsStartOK := parseCounter(before, "treedb.publish.ordered_root_delta_group.calls_total")
	publicationCallsEnd, callsEndOK := parseCounter(after, "treedb.publish.ordered_root_delta_group.calls_total")
	publicationRootsStart, rootsStartOK := parseCounter(before, "treedb.publish.ordered_root_delta_group.roots_total")
	publicationRootsEnd, rootsEndOK := parseCounter(after, "treedb.publish.ordered_root_delta_group.roots_total")
	if callsStartOK && callsEndOK && publicationCallsEnd >= publicationCallsStart {
		calls := publicationCallsEnd - publicationCallsStart
		if elapsed := b.Elapsed(); elapsed > 0 {
			b.ReportMetric(float64(calls)/elapsed.Seconds(), "root_publications/s")
		}
		if calls > 0 && rootsStartOK && rootsEndOK && publicationRootsEnd >= publicationRootsStart {
			b.ReportMetric(float64(publicationRootsEnd-publicationRootsStart)/float64(calls), "roots/publication")
		}
	}
	for _, metric := range []struct{ key, name string }{
		{"treedb.cache.iterator.sources_max", "iterator_sources_max"},
		{"treedb.cache.iterator.queue_len_max", "iterator_queue_len_max"},
		{"treedb.cache.queue_len", "iterator_queue_len_end"},
		{"treedb.cache.point_successor.sources_max", "point_successor_sources_max"},
		{"treedb.command_wal.dependency_debt.pending_bytes", "dependency_debt_pending_bytes_end"},
		{"treedb.command_wal.dependency_debt.max_age_ns", "dependency_debt_max_age_ns_end"},
		{"treedb.command_wal.durable_wal_lsn", "durable_wal_lsn_end"},
		{"treedb.applied_command_lsn", "durable_root_applied_lsn_end"},
	} {
		if value, ok := parseCounter(after, metric.key); ok {
			b.ReportMetric(float64(value), metric.name)
		}
	}
}

func parseCounter(stats map[string]string, key string) (uint64, bool) {
	if stats == nil {
		return 0, false
	}
	var value uint64
	_, err := fmt.Sscanf(stats[key], "%d", &value)
	return value, err == nil
}

func TestDgraphShapedProfilesRoundTrip(t *testing.T) {
	for _, profile := range benchmarkProfiles {
		profile := profile
		t.Run(profile.class+"/"+profile.name, func(t *testing.T) {
			store := profile.open(t, t.TempDir())
			key := benchmarkKey(7)
			value := benchmarkValue(256)
			if err := store.commitAt(11, []mutation{{key: key, value: value}}); err != nil {
				t.Fatalf("commit value: %v", err)
			}
			got, present, err := store.getAt(key, 11)
			if err != nil || !present || !bytes.Equal(got, value) {
				t.Fatalf("get value present=%t got=%x err=%v", present, got, err)
			}
			if err := store.commitAt(12, []mutation{{key: key, delete: true}}); err != nil {
				t.Fatalf("commit delete: %v", err)
			}
			if got, present, err := store.getAt(key, 12); err != nil || present || got != nil {
				t.Fatalf("get tombstone present=%t got=%x err=%v", present, got, err)
			}
			if profile.backend == "treedb" {
				stats := store.stats()
				for _, key := range []string{
					"treedb.cache.iterator.calls_total",
					"treedb.cache.iterator.snapshot_rotations_total",
					"treedb.cache.iterator.sources_total",
					"treedb.cache.iterator.sources_max",
					"treedb.cache.iterator.queue_len_max",
					"treedb.cache.point_successor.calls_total",
					"treedb.cache.point_successor.hits_total",
					"treedb.cache.point_successor.sources_total",
					"treedb.cache.point_successor.sources_max",
					"treedb.cache.point_successor.general_merge_iterators_total",
				} {
					if _, ok := stats[key]; !ok {
						t.Errorf("missing TreeDB attribution stat %q", key)
					}
				}
				for _, want := range []struct {
					key   string
					value uint64
				}{
					{key: "treedb.cache.iterator.calls_total", value: 1},
					{key: "treedb.cache.iterator.snapshot_rotations_total", value: 0},
					{key: "treedb.cache.point_successor.calls_total", value: 2},
					{key: "treedb.cache.point_successor.hits_total", value: 2},
					{key: "treedb.cache.point_successor.sources_total", value: 2},
					{key: "treedb.cache.point_successor.sources_max", value: 1},
					{key: "treedb.cache.point_successor.general_merge_iterators_total", value: 0},
				} {
					if got, ok := parseCounter(stats, want.key); !ok || got != want.value {
						t.Errorf("%s=%d ok=%t want %d", want.key, got, ok, want.value)
					}
				}
			}
			if err := store.close(); err != nil {
				t.Fatalf("close: %v", err)
			}
		})
	}
}

func TestDgraphShapedProfileContracts(t *testing.T) {
	for _, profile := range benchmarkProfiles {
		profile := profile
		t.Run(profile.class+"/"+profile.name, func(t *testing.T) {
			store := profile.open(t, t.TempDir())
			if profile.backend == "badger" {
				got := store.(*badgerStore).syncWrites
				want := profile.class == "durable"
				if got != want {
					t.Fatalf("Badger SyncWrites=%t want %t", got, want)
				}
			}
			if profile.backend == "treedb" {
				stats := store.stats()
				if got := stats["treedb.write_path.mode"]; got != "command_wal_cached" {
					t.Fatalf("TreeDB write path=%q want command_wal_cached", got)
				}
				wantDurability := "wal_on_relaxed_sync"
				if profile.class == "durable" {
					wantDurability = "wal_on_sync"
				}
				if got := stats["treedb.durability_mode"]; got != wantDurability {
					t.Fatalf("TreeDB durability=%q want %q", got, wantDurability)
				}
			}
			if err := store.close(); err != nil {
				t.Fatalf("close: %v", err)
			}
		})
	}
}
