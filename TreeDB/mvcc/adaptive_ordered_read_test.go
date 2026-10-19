package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/node"
)

func TestAdaptiveMVCCOrderedFirstQualifiedRead(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileNoWALFast, treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		t.Run(string(profile), func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.DisableSideStores = true
			opts.BackgroundCheckpointInterval = -1
			opts.ValueLog.PointerThreshold = 1
			db, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if db != nil {
					if err := db.Close(); err != nil {
						t.Error(err)
					}
				}
			})
			before := db.Stats()
			if before["treedb.cache.memtable_mode_config"] != "adaptive" || before["treedb.cache.memtable_mode"] != "append_only" {
				t.Fatalf("default adaptive mode=%s/%s", before["treedb.cache.memtable_mode_config"], before["treedb.cache.memtable_mode"])
			}
			store := New(db)
			after := db.Stats()
			if after["treedb.cache.memtable_mode_config"] != "adaptive" || after["treedb.cache.memtable_mode"] != "append_only" {
				t.Fatalf("write-only MVCC attachment changed mode=%s/%s", after["treedb.cache.memtable_mode_config"], after["treedb.cache.memtable_mode"])
			}
			for _, key := range []string{"treedb.cache.queue_len", "treedb.cache.memtable_stats.writes", "treedb.cache.memtable_stats.iterators", "treedb.cache.memtable_stats.range_iterators", "treedb.cache.iterator.calls_total", "treedb.cache.snapshot.rotations_total"} {
				if after[key] != before[key] {
					t.Fatalf("attachment changed %s: %s -> %s", key, before[key], after[key])
				}
			}
			logical := []byte("versioned")
			oldValue := bytes.Repeat([]byte("old!"), 1024)
			if err := store.CommitAt(10, []Mutation{{Key: logical, Value: oldValue}}, CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			held := db.AcquireSnapshot()
			if held == nil {
				t.Fatal("old snapshot unavailable")
			}
			defer func() {
				if held != nil {
					if err := held.Close(); err != nil {
						t.Error(err)
					}
				}
			}()
			physical, err := mvcckey.Encode(logical, 10)
			if err != nil {
				t.Fatal(err)
			}
			entry, err := held.GetEntry(physical)
			if err != nil || entry.Flags&node.FlagPointer == 0 || entry.ValuePtr.FileID == 0 {
				t.Fatalf("expected persisted pointer: %+v, %v", entry, err)
			}
			owned, err := held.Get(physical)
			if err != nil || !bytes.Equal(owned, append([]byte{recordValueV1}, oldValue...)) {
				t.Fatalf("held record len=%d err=%v", len(owned), err)
			}
			if err := store.CommitAt(20, []Mutation{{Key: logical, Delete: true}}, CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			if err := store.CommitAt(30, []Mutation{{Key: logical, Value: []byte("new")}}, CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			beforeRead := db.Stats()
			if beforeRead["treedb.cache.memtable_mode"] != "append_only" {
				t.Fatalf("write-only commits changed mode=%s", beforeRead["treedb.cache.memtable_mode"])
			}
			// This canonical successor demand, rather than Store construction or
			// a write, chooses the ordered future mutable and preserves all sources.
			requireResult(t, store, logical, 9, Absent, 0, nil)
			afterRead := db.Stats()
			if afterRead["treedb.cache.memtable_mode"] != "btree" || afterRead["treedb.cache.memtable_adaptive.last_reason"] != "mvcc_ordered" {
				t.Fatalf("qualified read mode=%s reason=%s", afterRead["treedb.cache.memtable_mode"], afterRead["treedb.cache.memtable_adaptive.last_reason"])
			}
			for _, key := range []string{"treedb.cache.iterator.calls_total", "treedb.cache.snapshot.rotations_total"} {
				if afterRead[key] != beforeRead[key] {
					t.Fatalf("qualified read fabricated %s: %s -> %s", key, beforeRead[key], afterRead[key])
				}
			}
			for _, checkpoint := range []bool{false, true} {
				if checkpoint {
					if err := db.Checkpoint(); err != nil {
						t.Fatal(err)
					}
				}
				requireResult(t, store, logical, 9, Absent, 0, nil)
				requireResult(t, store, logical, 10, Present, 10, oldValue)
				requireResult(t, store, logical, 20, Tombstone, 20, nil)
				requireResult(t, store, logical, 30, Present, 30, []byte("new"))
				got, err := held.Get(physical)
				if err != nil || !bytes.Equal(got, owned) {
					t.Fatalf("held pointer changed across checkpoint: %v", err)
				}
				future, err := mvcckey.Encode(logical, 30)
				if err != nil {
					t.Fatal(err)
				}
				if got, err := held.Get(future); !errors.Is(err, treedb.ErrKeyNotFound) || got != nil {
					t.Fatalf("held snapshot saw future record %x err=%v", got, err)
				}
			}
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			held = nil
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = nil
			if !bytes.Equal(owned, append([]byte{recordValueV1}, oldValue...)) {
				t.Fatal("owned snapshot output changed after Close")
			}
			db, err = treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			reopened := New(db)
			requireResult(t, reopened, logical, 10, Present, 10, oldValue)
			requireResult(t, reopened, logical, 20, Tombstone, 20, nil)
			requireResult(t, reopened, logical, 30, Present, 30, []byte("new"))
		})
	}
}

func TestAdaptiveMVCCOrderedLateQualifiedRead(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.ValueLog.PointerThreshold = 1
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	raw := []byte("ordinary-raw")
	old := bytes.Repeat([]byte("old"), 1024)
	if err := db.Set(raw, old); err != nil {
		t.Fatal(err)
	}
	held := db.AcquireSnapshot()
	if held == nil {
		t.Fatal("old snapshot unavailable")
	}
	defer func() {
		if held != nil {
			if err := held.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	// Populate the successor too: attachment must preserve both the retained
	// snapshot's frozen source and the current mutable, regardless of flushing.
	if err := db.Set(raw, []byte("current")); err != nil {
		t.Fatal(err)
	}
	before := db.Stats()
	store := New(db)
	after := db.Stats()
	if before["treedb.cache.memtable_mode"] != "append_only" || after["treedb.cache.memtable_mode"] != before["treedb.cache.memtable_mode"] {
		t.Fatalf("late attachment replaced populated mode %s -> %s", before["treedb.cache.memtable_mode"], after["treedb.cache.memtable_mode"])
	}
	for _, key := range []string{"treedb.cache.memtable_stats.writes", "treedb.cache.memtable_stats.iterators", "treedb.cache.memtable_stats.range_iterators"} {
		if after[key] != before[key] {
			t.Fatalf("late attachment reset %s: %s -> %s", key, before[key], after[key])
		}
	}
	if got, err := held.Get(raw); err != nil || !bytes.Equal(got, old) {
		t.Fatalf("held raw pointer changed: %v", err)
	}
	if got, err := db.Get(raw); err != nil || string(got) != "current" {
		t.Fatalf("current raw=%q err=%v", got, err)
	}
	if err := store.CommitAt(10, []Mutation{{Key: []byte("managed"), Value: []byte("one")}}, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	if stats := db.Stats(); stats["treedb.cache.memtable_mode"] != "append_only" {
		t.Fatalf("write-only late Store changed mode=%s", stats["treedb.cache.memtable_mode"])
	}
	// The first real read rotates through the maintained queue/publication path,
	// even with populated mutables and an already retained old source.
	requireResult(t, store, []byte("managed"), 10, Present, 10, []byte("one"))
	stats := db.Stats()
	if stats["treedb.cache.memtable_mode"] != "btree" || stats["treedb.cache.memtable_mode_config"] != "adaptive" {
		t.Fatalf("first late read mode=%s/%s", stats["treedb.cache.memtable_mode_config"], stats["treedb.cache.memtable_mode"])
	}
	if got, err := held.Get(raw); err != nil || !bytes.Equal(got, old) {
		t.Fatalf("held source after ordered rotation: %v", err)
	}
	if got, err := db.Get(raw); err != nil || string(got) != "current" {
		t.Fatalf("current raw after rotation=%q err=%v", got, err)
	}
	requireResult(t, store, []byte("managed"), 10, Present, 10, []byte("one"))
	if err := store.CommitAt(20, []Mutation{{Key: []byte("managed"), Value: []byte("two")}}, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	requireResult(t, store, []byte("managed"), 10, Present, 10, []byte("one"))
	requireResult(t, store, []byte("managed"), 20, Present, 20, []byte("two"))
	// A later write-heavy observation must not switch this qualified-successor
	// workload back to an unordered table at the following natural rotation.
	mutations := make([]Mutation, 1100)
	for i := range mutations {
		mutations[i] = Mutation{Key: []byte{byte(i >> 8), byte(i)}, Value: []byte("batch")}
	}
	if err := store.CommitAt(30, mutations, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if stats := db.Stats(); stats["treedb.cache.memtable_mode"] != "btree" || stats["treedb.cache.memtable_adaptive.last_reason"] != "mvcc_ordered" {
		t.Fatalf("write-heavy rotation lost ordered workload: mode=%s reason=%s", stats["treedb.cache.memtable_mode"], stats["treedb.cache.memtable_adaptive.last_reason"])
	}
	requireResult(t, store, mutations[0].Key, 30, Present, 30, []byte("batch"))
	requireResult(t, store, mutations[len(mutations)-1].Key, 30, Present, 30, []byte("batch"))
	if got, err := held.Get(raw); err != nil || !bytes.Equal(got, old) {
		t.Fatalf("held source after write-heavy rotation: %v", err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	held = nil
}

func TestMVCCOrderedReadPreservesExplicitMode(t *testing.T) {
	for _, mode := range []string{"append_only", "hash_sorted", "btree", "skiplist", "cow_btree"} {
		t.Run(mode, func(t *testing.T) {
			opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
			opts.DisableSideStores = true
			opts.BackgroundCheckpointInterval = -1
			opts.MemtableMode = mode
			db, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			}()
			before := db.Stats()
			store := New(db)
			after := db.Stats()
			if after["treedb.cache.memtable_mode"] != before["treedb.cache.memtable_mode"] || after["treedb.cache.memtable_mode_config"] != "fixed" {
				t.Fatalf("explicit %s became %s/%s", mode, after["treedb.cache.memtable_mode_config"], after["treedb.cache.memtable_mode"])
			}
			if err := store.CommitAt(1, []Mutation{{Key: []byte("key"), Value: []byte("value")}}, CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			requireResult(t, store, []byte("key"), 1, Present, 1, []byte("value"))
			if stats := db.Stats(); stats["treedb.cache.memtable_mode"] != before["treedb.cache.memtable_mode"] || stats["treedb.cache.memtable_mode_config"] != "fixed" {
				t.Fatalf("qualified read changed explicit %s to %s/%s", mode, stats["treedb.cache.memtable_mode_config"], stats["treedb.cache.memtable_mode"])
			}
		})
	}
}

// Generic raw successors and malformed qualified ranges retain their existing
// nonrotating read route; only canonical Store demand selects adaptive ordering.
func TestAdaptiveMVCCOrderedReadExcludesUnqualifiedRanges(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	store := New(db)
	key := []byte("qualified")
	if err := store.CommitAt(10, []Mutation{{Key: key, Value: []byte("old")}}, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	lower, err := mvcckey.Encode(key, 20)
	if err != nil {
		t.Fatal(err)
	}
	upper, err := mvcckey.AppendKeyVersionsUpper(nil, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, found, err := db.SeekGE(lower, upper); err != nil || !found {
		t.Fatalf("raw successor found=%t err=%v", found, err)
	}
	if _, _, _, err := db.SeekGEVersionRange([]byte("a"), []byte("z")); err != nil {
		t.Fatal(err)
	}
	if stats := db.Stats(); stats["treedb.cache.memtable_mode"] != "append_only" {
		t.Fatalf("unqualified range selected %s", stats["treedb.cache.memtable_mode"])
	}
	requireResult(t, store, key, 20, Present, 10, []byte("old"))
	if stats := db.Stats(); stats["treedb.cache.memtable_mode"] != "btree" {
		t.Fatalf("canonical demand selected %s", stats["treedb.cache.memtable_mode"])
	}
}

func TestAdaptiveMVCCOrderedConcurrentFirstRead(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.ValueLog.PointerThreshold = 1
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	store := New(db)
	key := []byte("contended")
	old := bytes.Repeat([]byte("old"), 1024)
	if err := store.CommitAt(10, []Mutation{{Key: key, Value: old}}, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	held := db.AcquireSnapshot()
	if held == nil {
		t.Fatal("old snapshot unavailable")
	}
	defer func() {
		if held != nil {
			if err := held.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	physical, err := mvcckey.Encode(key, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CommitAt(20, []Mutation{{Key: key, Value: []byte("twenty")}}, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	if stats := db.Stats(); stats["treedb.cache.memtable_mode"] != "append_only" {
		t.Fatalf("write-only mode=%s", stats["treedb.cache.memtable_mode"])
	}
	start := make(chan struct{})
	results := make(chan error, 9)
	for range 8 {
		go func() {
			<-start
			for range 16 {
				result, err := store.GetAt(key, 10)
				if err != nil || result.State != Present || result.Timestamp != 10 || !bytes.Equal(result.Value, old) {
					results <- fmt.Errorf("old read=%+v err=%v", result, err)
					return
				}
			}
			results <- nil
		}()
	}
	go func() {
		<-start
		for ts := uint64(30); ts <= 60; ts += 10 {
			if err := store.CommitAt(ts, []Mutation{{Key: key, Value: []byte("newer")}}, CommitRelaxed); err != nil {
				results <- err
				return
			}
		}
		results <- nil
	}()
	close(start)
	var firstErr error
	for range 9 {
		if err := <-results; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	if stats := db.Stats(); stats["treedb.cache.memtable_mode"] != "btree" || stats["treedb.cache.memtable_adaptive.last_reason"] != "mvcc_ordered" {
		t.Fatalf("concurrent first read mode=%s reason=%s", stats["treedb.cache.memtable_mode"], stats["treedb.cache.memtable_adaptive.last_reason"])
	}
	requireResult(t, store, key, 60, Present, 60, []byte("newer"))
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got, err := held.Get(physical); err != nil || !bytes.Equal(got, append([]byte{recordValueV1}, old...)) {
		t.Fatalf("held pointer across concurrent transition len=%d err=%v", len(got), err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	held = nil
}
