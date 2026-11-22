package main

import (
	"bytes"
	"errors"
	"flag"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/snissn/gomap/kvstore"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

func TestComparatorWriteDurability(t *testing.T) {
	const childEnv = "GOMAP_TEST_COMPARATOR_DURABILITY_CHILD"
	forceSync := []string{"-badger-nosync=false", "-leveldb-nosync=false", "-pebble-nosync=false"}
	cases := []struct {
		name, profile string
		flags         []string
		sync          bool
		pebblePoint   bool
	}{
		{name: "durable", profile: "durable", sync: true, pebblePoint: true},
		{name: "wal_on_fast", profile: "wal_on_fast"},
		{name: "fast", profile: "fast"},
		{name: "bench_unsafe", profile: "bench_unsafe"},
		{name: "unsafe", profile: "unsafe"},
		{name: "balanced", profile: "balanced", pebblePoint: true},
		{name: "omitted", pebblePoint: true},
		{name: "durable_override", profile: "durable", flags: []string{"-badger-nosync", "-leveldb-nosync", "-pebble-nosync"}},
		{name: "fast_override", profile: "fast", flags: forceSync, sync: true, pebblePoint: true},
		{name: "omitted_override", flags: forceSync, sync: true, pebblePoint: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if os.Getenv(childEnv) != "1" {
				// Use fresh CLI flags for each case. Presets also set flags for
				// engines outside this test.
				args := []string{"-test.run=^TestComparatorWriteDurability$/^" + tc.name + "$",
					"-leveldb-cache-mb=16", "-pebble-cache-mb=16"}
				if tc.profile != "" {
					args = append(args, "-profile="+tc.profile)
				}
				cmd := exec.Command(os.Args[0], append(args, tc.flags...)...)
				cmd.Env = append(os.Environ(), childEnv+"=1")
				if out, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("profile subprocess: %v\n%s", err, out)
				}
				return
			}

			explicitFlags = make(map[string]bool)
			flag.Visit(func(f *flag.Flag) { explicitFlags[f.Name] = true })
			requireComparatorOK(t, "resolve profile", applyProfile(*profileArg, explicitFlags))

			for _, name := range []string{"badger", "leveldb", "leveldb_block_comp_on", "leveldb_block_comp_off", "pebble"} {
				t.Run(name, func(t *testing.T) {
					factory, err := GetDBFactory(name)
					requireComparatorOK(t, "select adapter", err)
					if tc.name == "durable" {
						t.Run("reopen", func(t *testing.T) {
							verifyComparatorReopen(t, factory)
						})
					}
					db, err := factory(t.TempDir())
					requireComparatorOK(t, "open adapter", err)
					pointSync := tc.sync
					var journal *levelDBObservedStorage
					var syncs *atomic.Uint64
					switch wrapper := db.(type) {
					case *BadgerWrapper:
						if got := wrapper.db.Opts().SyncWrites; got != tc.sync {
							t.Errorf("Badger SyncWrites=%t, want %t", got, tc.sync)
						}
					case *LevelDBWrapper:
						// Preserve the registered wrapper's policy and observe the
						// native write boundary on a test storage implementation.
						requireComparatorOK(t, "close original LevelDB handle", wrapper.Close())
						journal = &levelDBObservedStorage{Storage: storage.NewMemStorage()}
						t.Cleanup(func() { requireComparatorOK(t, "close test storage", journal.Close()) })
						wrapper.db, err = leveldb.Open(journal, leveldbBenchOptions(wrapper.compression))
						requireComparatorOK(t, "open observed LevelDB", err)
						syncs = &journal.syncs
					case *PebbleWrapper:
						pointSync = tc.pebblePoint
						requireComparatorOK(t, "close original Pebble handle", wrapper.Close())
						fs := &pebbleObservedFS{FS: vfs.NewMem()}
						wrapper.db, err = pebble.Open("db", &pebble.Options{FS: fs})
						requireComparatorOK(t, "open observed Pebble", err)
						syncs = &fs.syncs
					}
					t.Cleanup(func() { requireComparatorOK(t, "close adapter", db.Close()) })

					checkWrite := func(op string, sync bool, write func() error) {
						t.Helper()
						var before uint64
						if syncs != nil {
							before = syncs.Load()
						}
						requireComparatorOK(t, op, write())
						if syncs != nil {
							if got := syncs.Load() > before; got != sync {
								t.Errorf("%s issued native log sync=%t, want %t", op, got, sync)
							}
						}
					}
					key, value := []byte("point"), []byte("value")
					checkWrite("Set", pointSync, func() error { return db.Set(key, value) })
					checkWrite("Delete", pointSync, func() error { return db.Delete(key) })

					newBatch := db.(kvstore.Batcher).NewBatch
					batch, err := newBatch()
					requireComparatorOK(t, "new point batch", err)
					t.Cleanup(func() { requireComparatorOK(t, "close point batch", batch.Close()) })
					requireComparatorOK(t, "batch Set", batch.Set([]byte("range-key"), value))
					checkWrite("Batch.Commit", tc.sync, batch.Commit)
					if syncs != nil {
						ranges, err := newBatch()
						requireComparatorOK(t, "new range batch", err)
						t.Cleanup(func() { requireComparatorOK(t, "close range batch", ranges.Close()) })
						requireComparatorOK(t, "batch DeleteRange", ranges.(kvstore.BatchRangeDeleter).DeleteRange([]byte("range-"), []byte("range.")))
						checkWrite("range Batch.Commit", tc.sync, ranges.Commit)

						explicit, err := newBatch()
						requireComparatorOK(t, "new explicit sync batch", err)
						t.Cleanup(func() { requireComparatorOK(t, "close explicit sync batch", explicit.Close()) })
						requireComparatorOK(t, "explicit batch Set", explicit.Set(key, value))
						checkWrite("Batch.CommitSync", true, explicit.CommitSync)
					}
					if tc.sync && journal != nil {
						journal.failSync.Store(true)
						if err := db.Set(key, value); !errors.Is(err, errComparatorJournalSync) {
							t.Errorf("Set sync error = %v, want %v", err, errComparatorJournalSync)
						}
					}
				})
			}
		})
	}
}

func verifyComparatorReopen(t *testing.T, factory DBFactory) {
	dir := t.TempDir()
	db, err := factory(dir)
	requireComparatorOK(t, "open for persistence check", err)
	t.Cleanup(func() {
		if db != nil {
			requireComparatorOK(t, "close persistence fixture", db.Close())
		}
	})
	point, batched, deleted := []byte("point"), []byte("batched"), []byte("deleted")
	value, updated := []byte("value"), []byte("updated")
	requireComparatorOK(t, "set point value", db.Set(point, value))
	batch, err := db.(kvstore.Batcher).NewBatch()
	requireComparatorOK(t, "new persistence batch", err)
	t.Run("commit", func(t *testing.T) {
		t.Cleanup(func() { requireComparatorOK(t, "close persistence batch", batch.Close()) })
		requireComparatorOK(t, "set batched value", batch.Set(batched, value))
		requireComparatorOK(t, "set temporary value", batch.Set(deleted, value))
		requireComparatorOK(t, "commit persistence batch", batch.Commit())
	})
	requireComparatorOK(t, "delete temporary value", db.Delete(deleted))

	checkValue := func(key, want []byte) {
		t.Helper()
		got, err := db.Get(key)
		if want != nil || !errors.Is(err, leveldb.ErrNotFound) {
			requireComparatorOK(t, "read value", err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("Get(%q) = %q, want %q", key, got, want)
		}
	}
	checkValue(point, value)
	checkValue(batched, value)
	checkValue(deleted, nil)
	requireComparatorOK(t, "checkpoint acknowledged writes", db.(checkpointer).Checkpoint())
	requireComparatorOK(t, "write after checkpoint", db.Set(point, updated))
	err = db.Close()
	db = nil
	requireComparatorOK(t, "close before reopen", err)
	db, err = factory(dir)
	requireComparatorOK(t, "fresh reopen", err)
	checkValue(point, updated)
	checkValue(batched, value)
	checkValue(deleted, nil)
}

func requireComparatorOK(t *testing.T, op string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
}

var errComparatorJournalSync = errors.New("injected journal sync error")

type levelDBObservedStorage struct {
	storage.Storage
	syncs    atomic.Uint64
	failSync atomic.Bool
}

func (s *levelDBObservedStorage) Create(fd storage.FileDesc) (storage.Writer, error) {
	w, err := s.Storage.Create(fd)
	if err != nil || fd.Type != storage.TypeJournal {
		return w, err
	}
	return &levelDBObservedJournal{Writer: w, storage: s}, nil
}

type levelDBObservedJournal struct {
	storage.Writer
	storage *levelDBObservedStorage
}

func (w *levelDBObservedJournal) Sync() error {
	w.storage.syncs.Add(1)
	if w.storage.failSync.Load() {
		return errComparatorJournalSync
	}
	return w.Writer.Sync()
}

type pebbleObservedFS struct {
	vfs.FS
	syncs atomic.Uint64
}

func (fs *pebbleObservedFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	if err != nil || !strings.HasSuffix(name, ".log") {
		return f, err
	}
	return &pebbleObservedWAL{File: f, syncs: &fs.syncs}, nil
}

type pebbleObservedWAL struct {
	vfs.File
	syncs *atomic.Uint64
}

func (f *pebbleObservedWAL) SyncData() error {
	f.syncs.Add(1)
	return f.File.SyncData()
}
