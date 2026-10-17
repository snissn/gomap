package mvcc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/caching"
)

// TestManagedHistoryMeasurement is an opt-in, one-cell public API measurement.
// Run each cell in a fresh test process. The source is also copied unchanged to
// the baseline: unsupported baseline prune is recorded, never scored as faster.
func TestManagedHistoryMeasurement(t *testing.T) {
	out := os.Getenv("TREEDB_HISTORY_MEASURE_OUT")
	if out == "" {
		t.Skip("opt-in measurement; requires exclusive heavy-job slot")
	}
	profile := treedb.Profile(os.Getenv("TREEDB_HISTORY_MEASURE_PROFILE"))
	pointers, err := strconv.ParseBool(os.Getenv("TREEDB_HISTORY_MEASURE_POINTERS"))
	if err != nil {
		t.Fatal(err)
	}
	dir := os.Getenv("TREEDB_HISTORY_MEASURE_DIR")
	if dir == "" {
		t.Fatal("explicit fresh DB directory required")
	}
	opts := treedb.OptionsFor(profile, dir)
	opts.MemtableMode = "cow_btree"
	opts.MemtableShards = 4
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.ValueLog.ForcePointers = pointers
	opts.ValueLog.PointerThreshold = 1 << 30
	if pointers {
		opts.ValueLog.PointerThreshold = 1
	}
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()
	store := New(db)
	mode := CommitRelaxed
	if profile == treedb.ProfileCommandWALDurable {
		mode = CommitDurable
	}
	type phase struct {
		Name                        string
		Operations                  int
		Nanoseconds                 int64
		AllocatedBytes, Allocations uint64
		HeapAlloc                   uint64
	}
	type retained struct {
		Name                                    string
		HeapAlloc, HeapObjects                  uint64
		DirectoryBytes, AllocatedDirectoryBytes int64
	}
	packet := struct {
		Profile        treedb.Profile
		Pointers       bool
		GoVersion      string
		Phases         []phase
		Retained       []retained
		PruneSupported bool
		PruneError     string
		PruneStats     PruneStats
	}{Profile: profile, Pointers: pointers, GoVersion: runtime.Version()}
	measure := func(name string, ops int, f func() error) error {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		start := time.Now()
		e := f()
		elapsed := time.Since(start)
		runtime.ReadMemStats(&after)
		packet.Phases = append(packet.Phases, phase{Name: name, Operations: ops, Nanoseconds: elapsed.Nanoseconds(), AllocatedBytes: after.TotalAlloc - before.TotalAlloc, Allocations: after.Mallocs - before.Mallocs, HeapAlloc: after.HeapAlloc})
		return e
	}
	capture := func(name string) {
		runtime.GC()
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		r := retained{Name: name, HeapAlloc: mem.HeapAlloc, HeapObjects: mem.HeapObjects}
		e := filepath.WalkDir(dir, func(path string, d fs.DirEntry, e error) error {
			if e != nil {
				return e
			}
			if d.IsDir() {
				return nil
			}
			info, e := d.Info()
			if e != nil {
				return e
			}
			r.DirectoryBytes += info.Size()
			if st, ok := info.Sys().(*syscall.Stat_t); ok {
				r.AllocatedDirectoryBytes += st.Blocks * 512
			}
			return nil
		})
		if e != nil {
			t.Fatal(e)
		}
		packet.Retained = append(packet.Retained, r)
	}
	keys := make([][]byte, 32)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("k%02d", i))
	}
	value := bytes.Repeat([]byte("v"), 256)
	// Arrival order differs from application timestamp without duplicate writes.
	order := []uint64{16, 1, 9, 2, 10, 3, 11, 4, 12, 5, 13, 6, 14, 7, 15, 8}
	mutations := make([]Mutation, len(keys))
	for i, key := range keys {
		mutations[i] = Mutation{Key: key, Value: value}
	}
	if err = measure("CommitAt_ACK", len(order), func() error {
		for _, ts := range order {
			if e := store.CommitAt(ts, mutations, mode); e != nil {
				return e
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = measure("GetAt", len(keys)*16, func() error {
		for _, key := range keys {
			for ts := uint64(1); ts <= 16; ts++ {
				got, e := store.GetAt(key, ts)
				if e != nil {
					return e
				}
				if got.State != Present || !bytes.Equal(got.Value, value) {
					return fmt.Errorf("bad visible value %s@%d", key, ts)
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	old, err := store.IterateVersions(VersionIteratorOptions{ReadTimestamp: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if err = measure("AdvanceDiscardFloor_ACK", 1, func() error { return store.AdvanceDiscardFloor(12, mode) }); err != nil {
		t.Fatal(err)
	}
	err = measure("PruneVersions_ACK", 1, func() error {
		var e error
		packet.PruneStats, e = store.PruneVersions(PruneOptions{BatchSize: 32, Mode: mode})
		return e
	})
	packet.PruneSupported = err == nil
	if err != nil {
		if !errors.Is(err, caching.ErrCOWUnsupported) {
			t.Fatal(err)
		}
		packet.PruneError = err.Error()
	}
	// A separate drain exposes asynchronous work excluded from foreground ACK.
	if err = measure("Checkpoint_pinned_cut", 1, db.Checkpoint); err != nil {
		t.Fatal(err)
	}
	capture("old_cut_pinned")
	oldCount := 0
	for old.Valid() {
		oldCount++
		old.Next()
	}
	if err = old.Error(); err != nil {
		t.Fatal(err)
	}
	if oldCount != 32*8 {
		t.Fatalf("retained old cut versions=%d", oldCount)
	}
	if err = old.Close(); err != nil {
		t.Fatal(err)
	}
	if err = measure("Checkpoint_released_cut", 1, db.Checkpoint); err != nil {
		t.Fatal(err)
	}
	capture("old_cut_released")
	// Ordinary transaction control uses disjoint keys and the same resolved mode.
	if err = measure("ConditionalTxn_ACK", 32, func() error {
		for i := 0; i < 32; i++ {
			tx, e := db.NewConditionalTxnWithSnapshot()
			if e != nil {
				return e
			}
			key := []byte(fmt.Sprintf("ordinary%02d", i))
			if e = tx.Set(key, value); e != nil {
				_ = tx.Close()
				return e
			}
			if mode == CommitDurable {
				e = tx.CommitSync()
			} else {
				e = tx.Commit()
			}
			if e != nil {
				_ = tx.Close()
				return e
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = measure("Checkpoint_full_drain", 1, db.Checkpoint); err != nil {
		t.Fatal(err)
	}
	if err = measure("Close_full_drain", 1, db.Close); err != nil {
		t.Fatal(err)
	}
	closed = true
	capture("closed_full_drain")
	data, err := json.MarshalIndent(packet, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(out, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
