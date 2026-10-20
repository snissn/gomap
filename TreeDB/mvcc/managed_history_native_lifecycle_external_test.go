package mvcc_test

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/snissn/gomap/TreeDB/node"
)

// Fixed eligible history must not make pruning inspect a growing retained tail.
// The original cut still owns the pruned pointer values through checkpoint and
// shutdown; the new view must retain every above-floor version after reopen.
func TestManagedHistoryPruneSkipsRetainedTail(t *testing.T) {
	for _, depth := range []int{16, 64} {
		t.Run(fmt.Sprintf("depth%d", depth), func(t *testing.T) {
			opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
			opts.MemtableMode, opts.MemtableShards = "cow_btree", 4
			opts.FlushThreshold = 1 << 30
			opts.DisableSideStores = true
			opts.BackgroundCheckpointInterval, opts.BackgroundCheckpointIdleDuration = -1, -1
			opts.MaxWALBytes = -1
			opts.ValueLog.PointerThreshold = 1
			opts.ValueLog.Compression = treedb.ValueLogCompressionOff
			opts.ValueLog.CompressionAutotune = treedb.AutotuneOptions{Mode: treedb.AutotuneOff}
			database, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if database != nil {
					if err := database.Close(); err != nil {
						t.Error(err)
					}
				}
			})
			handle, err := database.NativeSemanticBackend()
			if err != nil || !handle.HasNativeSemanticBridge() || database.ResolvedProfile() != treedb.ProfileNoWALFast || database.Stats()["treedb.cache.memtable_mode"] != "cow_btree" {
				t.Fatalf("native COW route: %v", err)
			}
			registry := handle.ValueLogIdentityPinRegistry()
			capture, err := database.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			parent, parentOK := capture.NativeAllocator().(*memtable.COWBudget)
			maintenance, maintenanceOK := capture.NativeMaintenanceAllocator().(*memtable.COWBudget)
			if err := capture.Close(); err != nil {
				t.Fatal(err)
			}
			capture = nil // Observe the original capabilities without holding a cut.
			if !parentOK || !maintenanceOK || parent == maintenance {
				t.Fatal("missing distinct original producer and maintenance budgets")
			}
			keys := [][]byte{{}, {0}, {0, 0}, {0, 0xff}, {1}, {0xff}}
			for i := len(keys); i < 32; i++ {
				keys = append(keys, []byte(fmt.Sprintf("k%02d", i)))
			}
			sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
			value := func(key []byte, ts uint64) []byte {
				return bytes.Repeat([]byte(fmt.Sprintf("history-%x@%d/", key, ts)), 32)
			}
			store := mvcc.New(database)
			// Timestamp arrival is deliberately unrelated to physical order.
			for offset := 0; offset < depth; offset += 16 {
				for _, ts := range []uint64{16, 1, 9, 2, 10, 3, 11, 4, 12, 5, 13, 6, 14, 7, 15, 8} {
					ts += uint64(offset)
					mutations := make([]mvcc.Mutation, len(keys))
					for i, key := range keys {
						mutations[i] = mvcc.Mutation{Key: key, Value: value(key, ts)}
						if i == 0 && ts == 14 {
							mutations[i] = mvcc.Mutation{Key: key, Delete: true}
						}
					}
					if err := store.CommitAt(ts, mutations, mvcc.CommitRelaxed); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			// Public exact-entry metadata proves that an old payload is persisted
			// as a pointer without duplicating the reserved MVCC key encoding.
			func() {
				proof := database.AcquireSnapshot()
				if proof == nil {
					t.Fatal("nil pointer-proof snapshot")
				}
				defer func() {
					if err := proof.Close(); err != nil {
						t.Error(err)
					}
				}()
				it, err := proof.Iterator(nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := it.Close(); err != nil {
						t.Error(err)
					}
				}()
				found := false
				for ; it.Valid(); it.Next() {
					if !bytes.HasSuffix(it.ValueCopy(nil), value(keys[0], 1)) {
						continue
					}
					entry, err := proof.GetEntryExact(it.KeyCopy(nil))
					if err != nil || entry.Flags&node.FlagPointer == 0 || entry.ValuePtr.FileID == 0 {
						t.Fatalf("old pointer=%+v: %v", entry, err)
					}
					found = true
				}
				if err := it.Error(); err != nil || !found {
					t.Fatalf("old pointer found=%t: %v", found, err)
				}
			}()
			// Overlay the first above-floor version in COW so the prune cut
			// includes both immutable COW roots and the published backend basis.
			overlay := make([]mvcc.Mutation, len(keys))
			for i, key := range keys {
				overlay[i] = mvcc.Mutation{Key: key, Value: value(key, 13)}
			}
			if err := store.CommitAt(13, overlay, mvcc.CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			old, err := store.IterateVersions(mvcc.VersionIteratorOptions{ReadTimestamp: 8})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := old.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := store.AdvanceDiscardFloor(12, mvcc.CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			stats, err := store.PruneVersions(mvcc.PruneOptions{BatchSize: 32, Mode: mvcc.CommitRelaxed})
			if err != nil {
				t.Fatal(err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			for _, key := range keys {
				for ts := uint64(8); ts > 0; ts-- {
					got := old.Entry()
					if !old.Valid() || !bytes.Equal(got.Key, key) || got.Timestamp != ts || got.State != mvcc.Present || !bytes.Equal(got.Value, value(key, ts)) {
						t.Fatalf("held %x@%d=%+v: %v", key, ts, got, old.Error())
					}
					old.Next()
				}
			}
			if old.Valid() || old.Error() != nil {
				t.Fatalf("held history tail: %v", old.Error())
			}
			old.Seek(keys[0], 8)
			if !old.Valid() || old.Error() != nil {
				t.Fatalf("held history rewind: %v", old.Error())
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			database = nil
			if parent.Stats().TotalBytes == 0 || maintenance.Stats().TotalBytes == 0 || registry.Stats().ActivePins == 0 {
				t.Fatal("shutdown lost original held-cut custody")
			}
			if err := old.Close(); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				p, m, pins := parent.Stats(), maintenance.Stats(), registry.Stats()
				p.PeakBytes, m.PeakBytes = 0, 0
				if err := registry.CleanupError(); err != nil {
					t.Fatal(err)
				}
				if p == (memtable.COWStats{}) && m == (memtable.COWStats{}) && pins.ActivePins == 0 && pins.ActiveIdentities == 0 && pins.ActiveStableNamespaceLinks == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("original custody did not drain: parent=%+v maintenance=%+v registry=%+v", p, m, pins)
				}
				time.Sleep(time.Millisecond)
			}
			database, err = treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			store = mvcc.New(database)
			if floor, err := store.DiscardFloor(); err != nil || floor != 12 {
				t.Fatalf("reopened floor=%d: %v", floor, err)
			}
			if _, err := store.GetAt(keys[0], 12); !errors.Is(err, mvcc.ErrReadBeforeDiscardFloor) {
				t.Fatalf("reopened discarded read: %v", err)
			}
			survivors, err := store.IterateVersions(mvcc.VersionIteratorOptions{ReadTimestamp: uint64(depth)})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := survivors.Close(); err != nil {
					t.Error(err)
				}
			}()
			for i, key := range keys {
				for ts := uint64(depth); ts >= 12; ts-- {
					got := survivors.Entry()
					wantState := mvcc.Present
					if i == 0 && ts == 14 {
						wantState = mvcc.Tombstone
					}
					if !survivors.Valid() || !bytes.Equal(got.Key, key) || got.Timestamp != ts || got.State != wantState || (wantState == mvcc.Present && !bytes.Equal(got.Value, value(key, ts))) {
						t.Fatalf("reopened survivor %x@%d=%+v: %v", key, ts, got, survivors.Error())
					}
					survivors.Next()
				}
			}
			if survivors.Valid() || survivors.Error() != nil {
				t.Fatalf("reopened survivor tail: %v", survivors.Error())
			}
			// Check the algorithmic bound after all public correctness and
			// original-owner release checks, including on the expected RED.
			if stats.Visited != 416 || stats.Retained != 64 || stats.Skipped != 32 || stats.Pruned != 352 || stats.Batches != 11 {
				t.Fatalf("inspected prune work must be independent of retained depth: %+v", stats)
			}
		})
	}
}

// Storage operations use the exported Store, native composite and DB APIs.
// Existing allocator capabilities observe the SAME original owners through
// shutdown; retaining those capabilities does not retain an engine or cut.
func TestManagedHistoryNativeDeferredCloseCustody(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.MemtableShards = 4
			opts.FlushThreshold = 1 << 30
			opts.DisableSideStores = true
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			opts.MaxWALBytes = -1
			opts.ValueLog.ForcePointers = false
			opts.ValueLog.PointerThreshold = 64
			opts.ValueLog.Compression = treedb.ValueLogCompressionOff
			opts.ValueLog.CompressionAutotune = treedb.AutotuneOptions{Mode: treedb.AutotuneOff}
			database, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if database != nil {
					if err := database.Close(); err != nil {
						t.Error(err)
					}
				}
			})
			handle, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			registry := handle.ValueLogIdentityPinRegistry()
			capture, err := database.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			parent, parentOK := capture.NativeAllocator().(*memtable.COWBudget)
			maintenance, maintenanceOK := capture.NativeMaintenanceAllocator().(*memtable.COWBudget)
			if err := capture.Close(); err != nil {
				t.Fatal(err)
			}
			capture = nil // Diagnostic capture ends before the workload begins.
			if !parentOK || !maintenanceOK || parent == maintenance {
				t.Fatal("missing distinct original producer and maintenance budgets")
			}
			manager, err := collections.NewNativeJSONManager(handle)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"left", "right"} {
				if _, err := manager.CreateCollection(collections.CollectionMeta{Name: name}); err != nil {
					t.Fatal(err)
				}
			}
			value := func(key string, ts uint64) []byte {
				return bytes.Repeat([]byte(fmt.Sprintf("history-%s@%d/", key, ts)), 128)
			}
			mutation := func(key string, ts uint64) mvcc.Mutation {
				return mvcc.Mutation{Key: []byte(key), Value: value(key, ts)}
			}
			store := mvcc.New(database)
			for _, commit := range []struct {
				ts uint64
				ms []mvcc.Mutation
			}{
				{30, []mvcc.Mutation{mutation("a", 30), {Key: []byte("c"), Delete: true}}},
				{10, []mvcc.Mutation{mutation("a", 10), mutation("b", 10), mutation("c", 10)}},
				{20, []mvcc.Mutation{mutation("a", 20), {Key: []byte("b"), Delete: true}, mutation("c", 20)}},
			} {
				if err := store.CommitAt(commit.ts, commit.ms, mvcc.CommitRelaxed); err != nil {
					t.Fatal(err)
				}
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			// Prove the three old payloads are actual persisted pointers without
			// duplicating Store's reserved physical-key encoding.
			func() {
				proof := database.AcquireSnapshot()
				if proof == nil {
					t.Fatal("nil pointer-proof snapshot")
				}
				defer func() {
					if err := proof.Close(); err != nil {
						t.Error(err)
					}
				}()
				it, err := proof.Iterator(nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := it.Close(); err != nil {
						t.Error(err)
					}
				}()
				seen := make(map[string]bool)
				for ; it.Valid(); it.Next() {
					for _, key := range []string{"a", "b", "c"} {
						if !bytes.HasSuffix(it.ValueCopy(nil), value(key, 10)) {
							continue
						}
						entry, err := proof.GetEntryExact(it.KeyCopy(nil))
						if err != nil || entry.Flags&node.FlagPointer == 0 || entry.ValuePtr.FileID == 0 {
							t.Fatalf("old %s pointer=%+v flags=%x: %v", key, entry.ValuePtr, entry.Flags, err)
						}
						seen[key] = true
					}
				}
				if err := it.Error(); err != nil || len(seen) != 3 {
					t.Fatalf("old pointer proof=%v: %v", seen, err)
				}
			}()
			old, err := store.IterateVersions(mvcc.VersionIteratorOptions{ReadTimestamp: 15})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := old.Close(); err != nil {
					t.Error(err)
				}
			})
			tx, err := manager.Begin("left", "right")
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Close()
			for _, name := range []string{"left", "right"} {
				if err := tx.InsertBatchInto(name, [][]byte{[]byte("row")}, [][]byte{[]byte(`{"owner":"` + name + `"}`)}); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.SetRaw([]byte("history-lifecycle/raw"), []byte("native")); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if err := tx.Close(); err != nil {
				t.Fatal(err)
			}
			if err := store.AdvanceDiscardFloor(25, mvcc.CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			stats, err := store.PruneVersions(mvcc.PruneOptions{BatchSize: 1, Mode: mvcc.CommitDurable})
			if err != nil || stats.Visited != 8 || stats.Pruned != 4 || stats.Retained != 4 {
				t.Fatalf("prune=%+v: %v", stats, err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"a", "b", "c"} {
				got := old.Entry()
				if !old.Valid() || string(got.Key) != key || got.Timestamp != 10 || got.State != mvcc.Present || !bytes.Equal(got.Value, value(key, 10)) {
					t.Fatalf("held history %s=%+v: %v", key, got, old.Error())
				}
				old.Next()
			}
			if old.Valid() || old.Error() != nil {
				t.Fatalf("held history tail: %v", old.Error())
			}
			old.Seek([]byte("a"), 15)
			if !old.Valid() || old.Error() != nil {
				t.Fatalf("held history rewind: %v", old.Error())
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			database = nil
			if parent.Stats().TotalBytes == 0 || maintenance.Stats().TotalBytes == 0 || registry.Stats().ActivePins == 0 {
				t.Fatalf("Close lost held original custody: parent=%+v maintenance=%+v registry=%+v", parent.Stats(), maintenance.Stats(), registry.Stats())
			}
			// DB shutdown ends new storage reads, while retaining the cut's
			// cleanup custody until the user releases that original owner.
			old.Next()
			old.Seek([]byte("a"), 15)
			if old.Valid() || !errors.Is(old.Error(), treedb.ErrClosed) {
				t.Fatalf("read after DB.Close: valid=%t error=%v", old.Valid(), old.Error())
			}
			if parent.Stats().TotalBytes == 0 || maintenance.Stats().TotalBytes == 0 || registry.Stats().ActivePins == 0 {
				t.Fatal("refused read consumed the old iterator's original cleanup custody")
			}
			t.Logf("after owner Close, before iterator Close: parent=%+v maintenance=%+v registry=%+v", parent.Stats(), maintenance.Stats(), registry.Stats())
			if err := old.Close(); err != nil {
				t.Fatal(err)
			}
			// The final cut queues original semantic cleanup for the existing
			// worker. Close relinquishes custody before that worker refunds it.
			deadline := time.Now().Add(5 * time.Second)
			for {
				remainingParent, remainingMaintenance := parent.Stats(), maintenance.Stats()
				remainingParent.PeakBytes, remainingMaintenance.PeakBytes = 0, 0
				pins := registry.Stats()
				if err := registry.CleanupError(); err != nil {
					t.Fatalf("original cleanup failed: %v; parent=%+v maintenance=%+v registry=%+v", err, remainingParent, remainingMaintenance, pins)
				}
				if remainingParent == (memtable.COWStats{}) && remainingMaintenance == (memtable.COWStats{}) && pins.ActivePins == 0 && pins.ActiveIdentities == 0 && pins.ActiveStableNamespaceLinks == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("original owners did not drain after final cut release: parent=%+v maintenance=%+v registry=%+v", remainingParent, remainingMaintenance, pins)
				}
				time.Sleep(time.Millisecond)
			}
			t.Logf("after final iterator Close: parent=%+v maintenance=%+v registry=%+v", parent.Stats(), maintenance.Stats(), registry.Stats())
			database, err = treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			store = mvcc.New(database)
			if floor, err := store.DiscardFloor(); err != nil || floor != 25 {
				t.Fatalf("reopened floor=%d: %v", floor, err)
			}
			if _, err := store.GetAt([]byte("a"), 25); !errors.Is(err, mvcc.ErrReadBeforeDiscardFloor) {
				t.Fatalf("reopened discarded read: %v", err)
			}
			for _, want := range []struct {
				key      string
				read, ts uint64
				state    mvcc.ReadState
			}{{"a", 26, 20, mvcc.Present}, {"a", 30, 30, mvcc.Present}, {"b", 26, 0, mvcc.Absent}, {"c", 26, 20, mvcc.Present}, {"c", 30, 30, mvcc.Tombstone}} {
				got, err := store.GetAt([]byte(want.key), want.read)
				if err != nil || got.State != want.state || got.Timestamp != want.ts || (want.state == mvcc.Present && !bytes.Equal(got.Value, value(want.key, want.ts))) {
					t.Fatalf("reopened %s@%d=%+v: %v", want.key, want.read, got, err)
				}
			}
			handle, err = database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager, err = collections.NewNativeJSONManager(handle)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"left", "right"} {
				if got, err := manager.Get(name, []byte("row")); err != nil || string(got) != `{"owner":"`+name+`"}` {
					t.Fatalf("reopened native %s=%q: %v", name, got, err)
				}
			}
			if got, err := database.Get([]byte("history-lifecycle/raw")); err != nil || string(got) != "native" {
				t.Fatalf("reopened composite raw=%q: %v", got, err)
			}
		})
	}
}
