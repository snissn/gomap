package caching

import (
	"encoding/binary"
	"math/rand"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestAdaptiveMemtableMode_EarlyRotationKeepsSampling(t *testing.T) {
	for _, rotation := range []string{"snapshot", "iterator", "ordinary"} {
		for _, workload := range []string{"mixed", "sequential"} {
			t.Run(rotation+"/"+workload, func(t *testing.T) {
				dir := t.TempDir()
				backend, err := backenddb.Open(backenddb.Options{Dir: dir})
				if err != nil {
					t.Fatal(err)
				}
				defer backend.Close()
				db, err := Open(dir, backend, Options{AllowUnsafe: true, DisableWAL: true,
					FlushThreshold: 1 << 30, MemtableMode: "adaptive", MemtableShards: 1})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				rotate := func() {
					t.Helper()
					switch rotation {
					case "snapshot":
						snap := db.AcquireSnapshot()
						if snap == nil {
							t.Fatal("AcquireSnapshot returned nil")
						}
						if err := snap.Close(); err != nil {
							t.Fatal(err)
						}
					case "iterator":
						it, err := db.Iterator(nil, nil)
						if err != nil {
							t.Fatal(err)
						}
						if err := it.Close(); err != nil {
							t.Fatal(err)
						}
					case "ordinary":
						db.mu.Lock()
						err := db.rotateMemtableLocked(false)
						db.mu.Unlock()
						if err != nil {
							t.Fatal(err)
						}
					}
				}
				rng := rand.New(rand.NewSource(1))
				write := func(first, count int) {
					t.Helper()
					for i := first; i < first+count; i++ {
						var key [8]byte
						k := uint64(i)
						if workload == "mixed" {
							k = rng.Uint64()
						}
						binary.BigEndian.PutUint64(key[:], k)
						if err := db.Set(key[:], []byte("value")); err != nil {
							t.Fatal(err)
						}
					}
				}
				write(0, 112)
				rotate()
				if got := db.Stats()["treedb.cache.memtable_adaptive.last_reason"]; got != "low_data" {
					t.Fatalf("early rotation reason=%q, want low_data", got)
				}
				if !db.memtableAdaptiveObserve.Load() {
					t.Fatal("early rotation stopped adaptive sampling below minimum")
				}
				write(112, adaptiveMinWrites-112)
				rotate()
				stats := db.Stats()
				wantMode, wantReason := "hash_sorted", "hash_mixed"
				if workload == "sequential" {
					wantMode, wantReason = "append_only", "append_sequential"
				}
				if stats["treedb.cache.memtable_mode"] != wantMode || stats["treedb.cache.memtable_adaptive.last_reason"] != wantReason {
					t.Fatalf("mode=%q reason=%q, want %s/%s", stats["treedb.cache.memtable_mode"], stats["treedb.cache.memtable_adaptive.last_reason"], wantMode, wantReason)
				}
				if workload == "sequential" {
					if db.memtableAdaptiveObserve.Load() {
						t.Fatal("sufficient append-only selection kept sampling")
					}
					write(adaptiveMinWrites, adaptiveMinWrites*4)
					rotate()
					if got := db.memtableStats.writes.Load(); got != adaptiveMinWrites {
						t.Fatalf("stopped sample grew to %d writes", got)
					}
					if err := db.DeleteRange(nil, nil); err != nil {
						t.Fatal(err)
					}
					if db.memtableAdaptiveObserve.Load() || db.memtableStats.writes.Load() != 0 {
						t.Fatal("clearing live counters restarted selected append-only sampling")
					}
				}
			})
		}
	}
}

func TestAdaptiveMemtableMode_ExplicitModesDoNotObserve(t *testing.T) {
	for _, mode := range []string{"append_only", "skiplist", "hash_sorted", "btree"} {
		t.Run(mode, func(t *testing.T) {
			db, err := Open(t.TempDir(), NewMockBackend(), Options{AllowUnsafe: true,
				DisableWAL: true, FlushThreshold: 1 << 30, MemtableMode: mode, MemtableShards: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.noteWriteKey([]byte("key"))
			db.noteWriteSortedRun([]byte("a"), []byte("z"), adaptiveMinWrites)
			db.noteIterator(nil, nil)
			db.mu.Lock()
			err = db.rotateMemtableLocked(false)
			db.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			if db.currentMemtableMode().String() != mode || db.memtableAdaptiveObserve.Load() || db.memtableStats.writes.Load() != 0 {
				t.Fatal("explicit mode changed or enabled adaptive sampling")
			}
		})
	}
}

func TestAdaptiveMemtableMode_SwitchesAfterWarmupRotation(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	// Use a very large flush threshold so we only ever rotate once (at the warmup
	// threshold). This reproduces the historical case where adaptive mode needed
	// two rotations to switch and therefore never took effect.
	db, err := Open(dir, backend, Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		FlushThreshold:           1 << 30,
		MemtableMode:             "adaptive:skiplist",
		MemtableShards:           1,
		ValueLogPointerThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	b := db.NewBatch()
	defer b.Close()
	rng := rand.New(rand.NewSource(1))
	value := make([]byte, 8<<10) // 8KiB
	for i := 0; i < adaptiveMinWrites*2; i++ {
		var key [16]byte
		binary.BigEndian.PutUint64(key[0:8], rng.Uint64())
		binary.BigEndian.PutUint64(key[8:16], uint64(i))
		if err := b.Set(key[:], value); err != nil {
			t.Fatalf("Batch.Set: %v", err)
		}
	}
	// Add enough payload to exceed the 16MiB warmup threshold and trigger a single
	// rotation.
	for i := 0; i < 600; i++ {
		var key [16]byte
		binary.BigEndian.PutUint64(key[0:8], rng.Uint64())
		binary.BigEndian.PutUint64(key[8:16], uint64(adaptiveMinWrites*2+i))
		if err := b.Set(key[:], value); err != nil {
			t.Fatalf("Batch.Set: %v", err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Batch.Write: %v", err)
	}

	stats := db.Stats()
	if got := stats["treedb.cache.memtable_mode_config"]; got != "adaptive" {
		t.Fatalf("expected adaptive memtable config, got %q", got)
	}
	if got := stats["treedb.cache.memtable_warmup_active"]; got != "false" {
		t.Fatalf("expected warmup to be finished after rotation, got %q", got)
	}
	if got := stats["treedb.cache.memtable_mode"]; got != "hash_sorted" {
		t.Fatalf("expected memtable mode to switch to hash_sorted after warmup rotation, got %q", got)
	}
}

func TestAdaptiveMemtableMode_SequentialWritesSwitchToAppendOnly(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		FlushThreshold:           1 << 30,
		MemtableMode:             "adaptive",
		MemtableShards:           1,
		ValueLogPointerThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	b := db.NewBatch()
	value := make([]byte, 8<<10)
	for i := 0; i < adaptiveMinWrites*2+600; i++ {
		// Break "strictly increasing" so Batch never switches to backend streaming
		// bypass (which would skip memtable stats/rotation entirely).
		k := uint64(i)
		if i == 1 {
			k = 0
		}
		var key [16]byte
		binary.BigEndian.PutUint64(key[8:16], k)
		if err := b.Set(key[:], value); err != nil {
			t.Fatalf("Batch.Set: %v", err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Batch.Write: %v", err)
	}

	stats := db.Stats()
	if got := stats["treedb.cache.memtable_warmup_active"]; got != "false" {
		t.Fatalf("expected warmup to be finished after rotation, got %q", got)
	}
	if got := stats["treedb.cache.memtable_mode"]; got != "append_only" {
		t.Fatalf("expected sequential workload to switch to append_only, got %q", got)
	}
}

func TestAdaptiveMemtableMode_DefaultAdaptiveStartsWarmup(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		FlushThreshold:           1 << 30,
		MemtableMode:             "adaptive",
		MemtableShards:           1,
		ValueLogPointerThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	stats := db.Stats()
	if got := stats["treedb.cache.memtable_mode_config"]; got != "adaptive" {
		t.Fatalf("expected adaptive memtable config, got %q", got)
	}
	if got := stats["treedb.cache.memtable_warmup_active"]; got != "true" {
		t.Fatalf("expected default adaptive mode to start with warmup active, got %q", got)
	}
}

func TestAdaptiveMemtableMode_BTreeMinIteratorSamplesEnvExplicitZero(t *testing.T) {
	t.Setenv(envAdaptiveBTreeMinIteratorSamples, "0")

	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		FlushThreshold:           1 << 30,
		MemtableMode:             "adaptive",
		MemtableShards:           1,
		ValueLogPointerThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	stats := db.Stats()
	if got := stats["treedb.cache.memtable_adaptive.btree_min_iterator_samples_effective"]; got != "0" {
		t.Fatalf("expected explicit env zero to keep legacy BTree guard, got %q", got)
	}
}

func TestAdaptiveMemtableMode_DefaultAdaptiveOverwriteHeavySwitchesToHashSorted(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		FlushThreshold:           1 << 30,
		MemtableMode:             "adaptive",
		MemtableShards:           1,
		ValueLogPointerThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	b := db.NewBatch()
	defer b.Close()
	rng := rand.New(rand.NewSource(1))
	const uniqueKeys = adaptiveMinWrites + 1200
	value := make([]byte, 8<<10) // 8KiB
	keys := make([][16]byte, uniqueKeys)

	for i := 0; i < uniqueKeys; i++ {
		binary.BigEndian.PutUint64(keys[i][0:8], rng.Uint64())
		binary.BigEndian.PutUint64(keys[i][8:16], uint64(i))
		if err := b.Set(keys[i][:], value); err != nil {
			t.Fatalf("Batch.Set(first %d): %v", i, err)
		}
	}
	for i := range keys {
		j := rng.Intn(uniqueKeys)
		keys[i], keys[j] = keys[j], keys[i]
	}
	for i := 0; i < uniqueKeys; i++ {
		if err := b.Set(keys[i][:], value); err != nil {
			t.Fatalf("Batch.Set(overwrite %d): %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Batch.Write: %v", err)
	}

	stats := db.Stats()
	if got := stats["treedb.cache.memtable_warmup_active"]; got != "false" {
		t.Fatalf("expected warmup to be finished after rotation, got %q", got)
	}
	if got := stats["treedb.cache.memtable_mode"]; got != "hash_sorted" {
		t.Fatalf("expected overwrite-heavy adaptive workload to switch to hash_sorted, got %q", got)
	}
}
