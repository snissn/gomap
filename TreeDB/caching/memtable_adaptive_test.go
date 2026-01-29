package caching

import (
	"encoding/binary"
	"math/rand"
	"testing"
)

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
		MemtableMode:             "adaptive",
		MemtableShards:           1,
		ValueLogPointerThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	b := db.NewBatch()
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

func TestAdaptiveMemtableMode_SequentialWritesSwitchToHashSorted(t *testing.T) {
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
	if got := stats["treedb.cache.memtable_mode"]; got != "hash_sorted" {
		t.Fatalf("expected sequential workload to switch to hash_sorted, got %q", got)
	}
}
