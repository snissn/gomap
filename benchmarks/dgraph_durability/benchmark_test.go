package dgraphdurability

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/dgraph-io/badger/v4"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/mvcc"
)

const benchmarkWarmupCommits = 16

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
	reportStoreCounters(b, before, store.stats(), b.N)
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
	reportStoreCounters(b, before, store.stats(), writeCommits)
	if err := store.close(); err != nil {
		b.Fatalf("close: %v", err)
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
	opts := treedb.OptionsFor(profile, dir)
	db, err := treedb.Open(opts)
	if err != nil {
		tb.Fatalf("open TreeDB profile=%s: %v", profile, err)
	}
	return &treeDBStore{db: db, mvcc: mvcc.New(db), mode: mode}
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

func reportStoreCounters(b *testing.B, before, after map[string]string, writeCommits int) {
	b.Helper()
	if writeCommits == 0 {
		return
	}
	for _, metric := range []struct {
		key  string
		name string
	}{
		{key: "treedb.command_wal.sync.count_total", name: "command_wal_syncs/write_commit"},
		{key: "treedb.command_wal.flush.count_total", name: "command_wal_flushes/write_commit"},
		{key: "treedb.cache.checkpoint.runs", name: "checkpoints/write_commit"},
	} {
		start, startOK := parseCounter(before, metric.key)
		end, endOK := parseCounter(after, metric.key)
		if startOK && endOK && end >= start {
			b.ReportMetric(float64(end-start)/float64(writeCommits), metric.name)
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
				wantDurability := "wal_on_relaxed_sync+no_read_checksum"
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
