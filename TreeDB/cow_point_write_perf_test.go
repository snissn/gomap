package treedb

import (
	"bytes"
	"encoding/binary"
	"runtime"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func openCOWPointPerfDB(tb testing.TB) *DB {
	tb.Helper()
	opts := OptionsFor(ProfileNoWALFast, tb.TempDir())
	opts.MemtableMode = "cow_btree"
	db, err := Open(opts)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := db.Close(); err != nil {
			tb.Error(err)
		}
	})
	return db
}

func BenchmarkCOWPointSet(b *testing.B) {
	b.Run("sequential", func(b *testing.B) {
		db := openCOWPointPerfDB(b)
		var key [8]byte
		val := make([]byte, 128)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			binary.BigEndian.PutUint64(key[:], uint64(i))
			if err := db.Set(key[:], val); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("random", func(b *testing.B) {
		db := openCOWPointPerfDB(b)
		var key [8]byte
		val := make([]byte, 128)
		x := uint64(0x9e3779b97f4a7c15)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			x = x*6364136223846793005 + 1442695040888963407
			binary.BigEndian.PutUint64(key[:], x)
			if err := db.Set(key[:], val); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkCOWPointDelete(b *testing.B) {
	db := openCOWPointPerfDB(b)
	const preload = 8192
	var key [8]byte
	val := make([]byte, 128)
	batch := db.NewBatch()
	for i := 0; i < preload; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(i))
		if err := batch.Set(key[:], val); err != nil {
			b.Fatal(err)
		}
	}
	if err := batch.Write(); err != nil {
		b.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(i))
		if err := db.Delete(key[:]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCOWBatchSet(b *testing.B) {
	db := openCOWPointPerfDB(b)
	const batchSize = 1000
	var key [8]byte
	val := make([]byte, 128)
	b.ReportAllocs()
	b.ResetTimer()
	seq := uint64(0)
	for i := 0; i < b.N; i++ {
		batch := db.NewBatch()
		for j := 0; j < batchSize; j++ {
			binary.BigEndian.PutUint64(key[:], seq)
			seq++
			if err := batch.Set(key[:], val); err != nil {
				_ = batch.Close()
				b.Fatal(err)
			}
		}
		if err := batch.Write(); err != nil {
			_ = batch.Close()
			b.Fatal(err)
		}
		if err := batch.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// TestCOWPointWriteHistoryAndAllocs verifies that 10k single-op inline writes
// avoid premature generation rollovers, cut per-op allocations, preserve
// pointer-threshold fallback for large values, and reopen cleanly.
func TestCOWPointWriteHistoryAndAllocs(t *testing.T) {
	dir := t.TempDir()
	opts := OptionsFor(ProfileNoWALFast, dir)
	opts.MemtableMode = "cow_btree"
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	var key [8]byte
	val := bytes.Repeat([]byte("a"), 128)
	const count = 10000
	for i := 0; i < count; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(i))
		if err := db.Set(key[:], val); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	stats := db.Stats()
	rollovers, _ := strconv.ParseUint(stats["treedb.cache.cow.rollovers"], 10, 64)
	inlinePubs, _ := strconv.ParseUint(stats["treedb.cache.cow.inline_point_publications_total"], 10, 64)
	t.Logf("10k point writes: rollovers=%d inline_point_publications=%d commit_seq=%s history_bytes=%s",
		rollovers, inlinePubs, stats["treedb.commit_seq"], stats["treedb.cache.cow.history_bytes"])
	if inlinePubs < count {
		_ = db.Close()
		t.Fatalf("inline_point_publications=%d want >= %d", inlinePubs, count)
	}
	// Large value (>512B pointer threshold) must cleanly take value-log pointer fallback.
	largeKey := []byte("large-value-key")
	largeVal := bytes.Repeat([]byte("L"), 2048)
	if err := db.Set(largeKey, largeVal); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	// Point delete must take inline point path and remove key.
	binary.BigEndian.PutUint64(key[:], 42)
	if err := db.Delete(key[:]); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	gotLarge, err := reopened.Get(largeKey)
	if err != nil || !bytes.Equal(gotLarge, largeVal) {
		t.Fatalf("reopened large value mismatch: len=%d err=%v", len(gotLarge), err)
	}
	binary.BigEndian.PutUint64(key[:], 42)
	gotDel, err := reopened.Get(key[:])
	if err != nil || gotDel != nil {
		t.Fatalf("reopened deleted key: got=%v err=%v", gotDel, err)
	}
	binary.BigEndian.PutUint64(key[:], count-1)
	gotLast, err := reopened.Get(key[:])
	if err != nil || !bytes.Equal(gotLast, val) {
		t.Fatalf("reopened last key mismatch: len=%d err=%v", len(gotLast), err)
	}
	// Assert per-op allocation budget on steady-state single-op inline Set.
	var benchKey [8]byte
	nextID := uint64(count + 100)
	allocs := testing.AllocsPerRun(200, func() {
		binary.BigEndian.PutUint64(benchKey[:], nextID)
		nextID++
		if err := reopened.Set(benchKey[:], val); err != nil {
			panic(err)
		}
	})
	t.Logf("steady-state Set allocs/op = %.1f", allocs)
	if rollovers > 0 {
		t.Fatalf("10k 128B single-op writes triggered %d premature generation rollovers (want 0)", rollovers)
	}
	if allocs > 18 {
		t.Fatalf("steady-state COW inline Set allocs/op = %.1f, want <= 18", allocs)
	}
}

func TestCOWPointWriteObserverUnwindReleasesGates(t *testing.T) {
	points := []durabilitycut.Point{
		durabilitycut.BeforeCOWPreparation,
		durabilitycut.AfterCOWPreparation,
		durabilitycut.AfterCOWCanonicalPreparation,
		durabilitycut.BeforeCOWCutSwap,
	}
	for _, point := range points {
		for _, mode := range []string{"panic", "goexit"} {
			t.Run(string(point)+"/"+mode, func(t *testing.T) {
				db := openCOWPointPerfDB(t)
				if err := db.Set([]byte("seed"), []byte("ok")); err != nil {
					t.Fatal(err)
				}
				triggered := false
				restore := durabilitycut.Install(func(ev durabilitycut.Event) error {
					if ev.Point == point && !triggered {
						triggered = true
						if mode == "panic" {
							panic("injected observer panic")
						}
						runtime.Goexit()
					}
					return nil
				})
				done := make(chan any, 1)
				go func() {
					defer func() {
						done <- recover()
					}()
					_ = db.Set([]byte("aborted"), []byte("never-visible"))
				}()
				recovered := <-done
				restore()
				if !triggered {
					t.Fatalf("observer point %s was not reached", point)
				}
				if mode == "panic" && recovered == nil {
					t.Fatalf("expected panic at %s", point)
				}
				// Gates must be released and aborted mutation must not be visible.
				got, err := db.Get([]byte("aborted"))
				if err != nil || got != nil {
					t.Fatalf("aborted key visible after %s %s: got=%q err=%v", point, mode, got, err)
				}
				if err := db.Set([]byte("after"), []byte("healthy")); err != nil {
					t.Fatalf("subsequent Set failed after %s %s: %v", point, mode, err)
				}
				gotAfter, err := db.Get([]byte("after"))
				if err != nil || string(gotAfter) != "healthy" {
					t.Fatalf("subsequent Get mismatch: got=%q err=%v", gotAfter, err)
				}
			})
		}
	}
}
