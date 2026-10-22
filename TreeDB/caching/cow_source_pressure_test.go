package caching

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

func cowSourcePressureFixture(t *testing.T, mode string, shards int, limits memtable.COWLimits) (*DB, *backenddb.DB) {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(dir, "backend")})
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, MemtableMode: mode, MemtableShards: shards, COWMemtableLimits: limits, FlushThreshold: 1 << 30})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !db.closing.Load() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	return db, backend
}

func cowSourcePressureKeys(t *testing.T, db *DB) [][]byte {
	t.Helper()
	keys := make([][]byte, len(db.cow.writers))
	found := 0
	for n := 0; n < 10000 && found < len(keys); n++ {
		key := cowOrderedLifecycleKey(t, fmt.Sprintf("source-%04d", n), 9)
		i := db.shardIndex(key)
		if keys[i] == nil {
			keys[i] = key
			found++
		}
	}
	if found != len(keys) {
		t.Fatal("could not populate all shards")
	}
	return keys
}

// This uses the production default source frontier and no held read view. A
// flush fence makes the pre-producer checkpoint wait observable without changing
// source ownership, the workload, or the finite memory limits.
func TestCOWDefaultSourcePressureJoinsBeforeProducer(t *testing.T) {
	for _, mode := range []string{"cow_btree", "cow_ordered"} {
		t.Run(mode, func(t *testing.T) {
			limits := memtable.DefaultCOWLimits()
			db, backend := cowSourcePressureFixture(t, mode, 4, limits)
			db.flushMu.Lock()
			locked := true
			defer func() {
				if locked {
					db.flushMu.Unlock()
				}
			}()
			keys := cowSourcePressureKeys(t, db)
			value := bytes.Repeat([]byte("v"), 128)
			for round := 0; round < limits.MaxSources/len(keys); round++ {
				b := db.NewBatchWithSize(len(keys))
				for _, key := range keys {
					if err := b.Set(key, value); err != nil {
						t.Fatal(err)
					}
				}
				if err := b.Write(); err != nil {
					t.Fatalf("round %d: %v", round, err)
				}
				_ = b.Close()
				if err := db.rolloverCOWForFlush(); err != nil {
					t.Fatal(err)
				}
			}
			stats := db.COWMemoryStats()
			if stats.Sources != limits.MaxSources || stats.Views != 0 || len(db.cow.cut.frozen) != limits.MaxSources {
				t.Fatalf("not an unpinned full source frontier: %+v", stats)
			}
			b := db.NewBatchWithSize(1)
			defer b.Close()
			if err := b.Set(keys[0], []byte("new")); err != nil {
				t.Fatal(err)
			}
			seq, lsn, runs := db.cow.cut.logicalSeq, backend.CommandWALNextLSN(), db.checkpointRuns.Load()
			done := make(chan error, 1)
			go func() { done <- b.Write() }()
			select {
			case err := <-done:
				t.Fatalf("write escaped before draining reclaimable sources: %v receipt=%+v", err, b.COWReceipt())
			case <-time.After(50 * time.Millisecond):
			}
			// Checkpoint cannot complete while flushMu is held, and this phase must not
			// assign or install anything. The flush fence orders these observations
			// before any cut publication; batch receipt is read only after Write exits.
			// The same command continues once the fence opens.
			db.cow.writerMu.Lock()
			observedSeq := db.cow.cut.logicalSeq
			db.cow.writerMu.Unlock()
			if observedSeq != seq || backend.CommandWALNextLSN() != lsn {
				t.Fatal("producer advanced before source drain")
			}
			db.flushMu.Unlock()
			locked = false
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("source pressure join stalled")
			}
			if db.checkpointRuns.Load() != runs+1 || db.cow.cut.logicalSeq != seq+1 || db.cow.budget.Limits() != limits {
				t.Fatal("expected one checkpoint and one publication with unchanged limits")
			}
			if got, err := db.Get(keys[0]); err != nil || string(got) != "new" {
				t.Fatalf("same-call progress: %q %v", got, err)
			}
		})
	}
}

func TestCOWSourceGenerationAggregateDemandAndParkedSuccessors(t *testing.T) {
	for _, mode := range []string{"cow_btree", "cow_ordered"} {
		t.Run(mode, func(t *testing.T) {
			for _, dimension := range []string{"source", "generation"} {
				t.Run(dimension, func(t *testing.T) {
					for _, parked := range []bool{false, true} {
						t.Run(fmt.Sprintf("parked=%t", parked), func(t *testing.T) {
							limits := memtable.DefaultCOWLimits()
							limits.MaxSources, limits.MaxGenerations = 3, 8
							fillers := 2
							if dimension == "generation" {
								limits.MaxSources, limits.MaxGenerations = 6, 6
								fillers = 3
							}
							db, _ := cowSourcePressureFixture(t, mode, 2, limits)
							keys := cowSourcePressureKeys(t, db)
							if parked {
								b := db.NewBatchWithSize(2)
								for _, key := range keys {
									if err := b.Set(key, []byte("old")); err != nil {
										t.Fatal(err)
									}
								}
								if err := b.Write(); err != nil {
									t.Fatal(err)
								}
								_ = b.Close()
								fillers--
							}
							for n := 0; n < fillers; n++ {
								w, err := db.cow.newWriter(false)
								if err != nil {
									t.Fatal(err)
								}
								t.Cleanup(func() { retired := w.Close(); retired.Drain() })
								if err := w.ReserveRawRolloverSource(); err != nil {
									t.Fatal(err)
								}
							}
							stats := db.COWMemoryStats()
							remaining := limits.MaxSources - stats.Sources
							if dimension == "generation" {
								remaining = limits.MaxGenerations - stats.Generations
							}
							if (!parked && remaining != 1) || (parked && remaining != 0) {
								t.Fatalf("not aggregate partial headroom/full promised frontier: parked=%t stats=%+v remaining=%d", parked, stats, remaining)
							}
							b := db.NewBatchWithSize(2)
							defer b.Close()
							groups := make([][]memtable.COWMutation, 2)
							for i, key := range keys {
								if err := b.Set(key, []byte("new")); err != nil {
									t.Fatal(err)
								}
								groups[i] = []memtable.COWMutation{{Key: key, Value: []byte("new"), MVCC: true, Timestamp: 9, Flags: node.FlagInline}}
							}
							if err := b.cowPrepareWriteStorage(); err != nil {
								t.Fatal(err)
							}
							db.cow.writerMu.Lock()
							loose := b.cowPredictionCanSkipEstimate(groups)
							rollover, pressure, _, err := b.cowGenerationRollover(true)
							db.cow.writerMu.Unlock()
							if err != nil || rollover != nil || pressure == parked {
								t.Fatalf("aggregate source/generation demand: parked=%t pressure=%t err=%v", parked, pressure, err)
							}
							if mode == "cow_ordered" && loose != parked {
								t.Fatalf("qualified fast proof bypassed or overcounted successors: parked=%t loose=%t", parked, loose)
							}
						})
					}
				})
			}
		})
	}
}

func TestCOWPinnedSourceGenerationPressureRefusesOnceBeforeProducer(t *testing.T) {
	for _, mode := range []string{"cow_btree", "cow_ordered"} {
		t.Run(mode, func(t *testing.T) {
			for _, dimension := range []string{"source", "generation"} {
				t.Run(dimension, func(t *testing.T) {
					limits := memtable.DefaultCOWLimits()
					limits.MaxSources, limits.MaxGenerations = 1, 4
					if dimension == "generation" {
						limits.MaxSources, limits.MaxGenerations = 2, 2
					}
					db, backend := cowSourcePressureFixture(t, mode, 1, limits)
					key := []byte("retained")
					if err := db.Set(key, []byte("old")); err != nil {
						t.Fatal(err)
					}
					old, err := db.acquireCOWSnapshotWithError()
					if err != nil {
						t.Fatal(err)
					}
					defer old.Close()
					if err := db.Checkpoint(); err != nil {
						t.Fatal(err)
					}
					before := db.COWMemoryStats()
					if before.Views != 1 || (dimension == "source" && before.Sources != limits.MaxSources) || (dimension == "generation" && before.Generations != limits.MaxGenerations) {
						t.Fatalf("not a pinned frontier: %+v", before)
					}
					b := db.NewBatchWithSize(1)
					defer b.Close()
					if err := b.Set(key, []byte("new")); err != nil {
						t.Fatal(err)
					}
					seq, lsn, runs := db.cow.cut.logicalSeq, backend.CommandWALNextLSN(), db.checkpointRuns.Load()
					err = b.Write()
					if !errors.Is(err, memtable.ErrCOWCapacity) || b.COWReceipt().Assigned || b.COWReceipt().LogicalInstalled || db.cow.cut.logicalSeq != seq || backend.CommandWALNextLSN() != lsn {
						t.Fatalf("finite pinned refusal changed acceptance: %v receipt=%+v", err, b.COWReceipt())
					}
					if db.checkpointRuns.Load() != runs+1 {
						t.Fatal("pinned frontier did not stop after one checkpoint")
					}
					if got, err := old.Get(key); err != nil || string(got) != "old" {
						t.Fatalf("retained cut invalid: %q %v", got, err)
					}
					if got, err := db.Get(key); err != nil || string(got) != "old" {
						t.Fatalf("refused new value visible: %q %v", got, err)
					}
					old.Close()
					if err := b.Write(); err != nil {
						t.Fatalf("caller submission after owner release: %v", err)
					}
				})
			}
		})
	}
}
