package treedb_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/snissn/gomap/TreeDB/node"
)

// Full documents and external-timestamp history share the public native owner.
// Equivalent physical maintenance must preserve both admitted readers without
// consuming another logical command or changing the retained manifest generation.
func TestNativeFullStoreValueLogMaintenanceHeldReopen(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode, opts.MemtableShards = "cow_btree", 4
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval, opts.BackgroundCheckpointIdleDuration = -1, -1
	opts.BackgroundIndexVacuumInterval, opts.MaxWALBytes = -1, -1
	opts.DisableBackgroundPrune, opts.DisableSideStores = true, true
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.Compression = treedb.ValueLogCompressionOff
	opts.ValueLog.CompressionAutotune = treedb.AutotuneOptions{Mode: treedb.AutotuneOff}
	opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationOff
	database, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	var manager *collections.CollectionManager
	var heldFull *collections.CollectionReadView
	var heldHistory *mvcc.VersionIterator
	t.Cleanup(func() {
		if heldFull != nil {
			if err := heldFull.Close(); err != nil {
				t.Error(err)
			}
		}
		if heldHistory != nil {
			if err := heldHistory.Close(); err != nil {
				t.Error(err)
			}
		}
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
		if database != nil {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	store := mvcc.New(database)
	value := func(key string, ts uint64) []byte {
		return bytes.Repeat([]byte(fmt.Sprintf("full-store-history/%s@%d/", key, ts)), 32)
	}
	mutation := func(key string, ts uint64) mvcc.Mutation {
		return mvcc.Mutation{Key: []byte(key), Value: value(key, ts)}
	}
	commit := func(ts uint64, mutations ...mvcc.Mutation) {
		t.Helper()
		if err := store.CommitAt(ts, mutations, mvcc.CommitRelaxed); err != nil {
			t.Fatalf("Store.CommitAt(%d): %v", ts, err)
		}
	}
	commit(10, mutation("a", 10), mutation("b", 10), mutation("c", 10), mutation("d", 10))
	commit(30, mutation("a", 30), mutation("b", 30), mvcc.Mutation{Key: []byte("c"), Delete: true}, mutation("d", 30))
	commit(20, mutation("a", 20), mvcc.Mutation{Key: []byte("b"), Delete: true}, mutation("c", 20))
	if err := database.Checkpoint(); err != nil {
		t.Fatalf("physical Store seed before activation: %v", err)
	}
	// Discover real physical keys by payload; do not duplicate the reserved
	// history key codec or infer pointer activation from the configured threshold.
	func() {
		proof := database.AcquireSnapshot()
		if proof == nil {
			t.Fatal("nil history pointer-proof snapshot")
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
			for _, key := range []string{"a", "b", "c", "d"} {
				if bytes.HasSuffix(it.ValueCopy(nil), value(key, 10)) {
					entry, err := proof.GetEntryExact(it.KeyCopy(nil))
					if err != nil || entry.Flags&node.FlagPointer == 0 || entry.ValuePtr.FileID == 0 {
						t.Fatalf("physical history %s: entry=%+v error=%v", key, entry, err)
					}
					seen[key] = true
				}
			}
		}
		if err := it.Error(); err != nil || len(seen) != 4 {
			t.Fatalf("physical history pointers=%v error=%v", seen, err)
		}
	}()
	handle, err := database.NativeSemanticBackend()
	if err != nil || !handle.HasNativeSemanticBridge() || database.ResolvedProfile() != treedb.ProfileCommandWALDurable || database.Stats()["treedb.cache.memtable_mode"] != "cow_btree" {
		t.Fatalf("actual durable native COW activation: %v", err)
	}
	captureBudgets := func() (*memtable.COWBudget, *memtable.COWBudget) {
		t.Helper()
		capture, err := database.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		parent, parentOK := capture.NativeAllocator().(*memtable.COWBudget)
		maintenance, maintenanceOK := capture.NativeMaintenanceAllocator().(*memtable.COWBudget)
		if err := capture.Close(); err != nil {
			t.Fatal(err)
		}
		if !parentOK || !maintenanceOK || parent == maintenance {
			t.Fatal("missing distinct original producer/maintenance budgets")
		}
		return parent, maintenance // Capabilities only; the original cut is closed.
	}
	parent, maintenance := captureBudgets()
	manager = collections.NewCollectionManager(handle)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "full", Options: collections.CollectionOptions{ColumnStore: &collections.ColumnStoreConfig{
		Enabled: true, RetainedPayload: collections.ColumnRetainedPayloadFull,
		RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingJSON,
		Columns: []collections.ColumnStoreColumn{
			{Name: "row_id", Path: "row_id", ValueType: collections.ColumnStoreValueInt64, Owner: collections.TypedStorageOwnerRowAsset},
			{Name: "kind", Path: "kind", ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerColumnPart},
		},
	}}}); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("full")
	if err != nil {
		t.Fatal(err)
	}
	before := [][]byte{[]byte(`{"row_id":1,"kind":"alpha","payload":{"complete":[true,1,"seed"]}}`), []byte(`{"row_id":2,"kind":"beta","payload":{"complete":[false,2,"later"]}}`)}
	after := [][]byte{[]byte(`{"row_id":11,"kind":"changed-alpha","payload":{"complete":[false,11,"updated-a"],"extra":{"a":true}}}`), []byte(`{"row_id":22,"kind":"changed-beta","payload":{"complete":[true,22,"updated-b"],"extra":{"b":false}}}`)}
	ids := [][]byte{[]byte("a"), []byte("b")}
	if inserted, err := col.InsertBatch(ids, before); err != nil || !reflect.DeepEqual(inserted, ids) {
		t.Fatalf("Full InsertBatch: ids=%q error=%v", inserted, err)
	}
	stat := func(key string) uint64 {
		t.Helper()
		raw := database.Stats()[key]
		n, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			t.Fatalf("public stat %s=%q: %v", key, raw, err)
		}
		return n
	}
	checkpoint := func(phase string) {
		t.Helper()
		next, sequence := handle.CommandWALNextLSN(), stat("treedb.cache.cow.txn.logical_seq")
		if err := database.Checkpoint(); err != nil {
			t.Fatalf("%s checkpoint: %v", phase, err)
		}
		state, ok := handle.StateToken()
		if !ok || state.AppliedCommandLSN != next-1 || handle.CommandWALNextLSN() != next || stat("treedb.cache.cow.txn.logical_seq") != sequence {
			t.Fatalf("%s checkpoint frontier: state=%+v valid=%v next=%d want next=%d sequence=%d", phase, state, ok, handle.CommandWALNextLSN(), next, sequence)
		}
	}
	checkpoint("Full physical seed")
	heldFull, err = col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	heldHistory, err = store.IterateVersions(mvcc.VersionIteratorOptions{ReadTimestamp: 15})
	if err != nil {
		t.Fatal(err)
	}
	checkJSON := func(got, want []byte) {
		t.Helper()
		var decoded, expected any
		if err := json.Unmarshal(got, &decoded); err != nil {
			t.Fatalf("Full document %q: %v", got, err)
		}
		if err := json.Unmarshal(want, &expected); err != nil || !reflect.DeepEqual(decoded, expected) {
			t.Fatalf("Full document=%s want=%s error=%v", got, want, err)
		}
	}
	checkHeld := func() {
		t.Helper()
		heldHistory.Seek([]byte("a"), 15)
		for _, key := range []string{"a", "b", "c", "d"} {
			entry := heldHistory.Entry()
			if !heldHistory.Valid() || string(entry.Key) != key || entry.Timestamp != 10 || entry.State != mvcc.Present || !bytes.Equal(entry.Value, value(key, 10)) {
				t.Fatalf("held history %s: entry=%+v error=%v", key, entry, heldHistory.Error())
			}
			heldHistory.Next()
		}
		if heldHistory.Valid() || heldHistory.Error() != nil {
			t.Fatalf("held history tail: valid=%v error=%v", heldHistory.Valid(), heldHistory.Error())
		}
		got, err := heldFull.FetchDocumentsByID(ids, collections.DocumentFetchOptions{})
		if err != nil || len(got.Results) != len(ids) {
			t.Fatalf("held Full fetch: %+v error=%v", got.Results, err)
		}
		for i, result := range got.Results {
			if !result.Found {
				t.Fatalf("held Full document %s absent", ids[i])
			}
			checkJSON(result.Document, before[i])
		}
	}
	checkHeld()
	items := make([]collections.UpdateBatchItem, len(ids))
	for i := range ids {
		items[i] = collections.UpdateBatchItem{DocumentID: ids[i], Update: func(document []byte) ([]byte, bool, error) {
			checkJSON(document, before[i])
			return bytes.Clone(after[i]), true, nil
		}}
	}
	next, sequence := handle.CommandWALNextLSN(), stat("treedb.cache.cow.txn.logical_seq")
	results, err := col.UpdateBatch(items)
	if err != nil || len(results) != 2 || !results[0].Matched || !results[0].Modified || !results[1].Matched || !results[1].Modified || handle.CommandWALNextLSN() != next+1 || stat("treedb.cache.cow.txn.logical_seq") != sequence+1 {
		t.Fatalf("one Full UPDATE: results=%+v error=%v next=%d sequence=%d", results, err, handle.CommandWALNextLSN(), stat("treedb.cache.cow.txn.logical_seq"))
	}
	commit(40, mutation("a", 40), mutation("b", 40))
	if err := store.AdvanceDiscardFloor(25, mvcc.CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	pruned, err := store.PruneVersions(mvcc.PruneOptions{BatchSize: 1, Mode: mvcc.CommitDurable})
	if err != nil || pruned.Visited != 11 || pruned.Pruned != 4 || pruned.Retained != 7 || pruned.Batches == 0 {
		t.Fatalf("bounded original history prune=%+v error=%v", pruned, err)
	}
	checkCurrent := func() {
		t.Helper()
		for i, id := range ids {
			got, err := col.Get(id)
			if err != nil {
				t.Fatalf("current Full %s: %v", id, err)
			}
			checkJSON(got, after[i])
		}
		if floor, err := store.DiscardFloor(); err != nil || floor != 25 {
			t.Fatalf("current history floor=%d error=%v", floor, err)
		}
		for _, want := range []struct {
			key      string
			read, ts uint64
			state    mvcc.ReadState
		}{{"a", 26, 20, mvcc.Present}, {"b", 26, 0, mvcc.Absent}, {"c", 26, 20, mvcc.Present}, {"d", 26, 10, mvcc.Present}, {"a", 45, 40, mvcc.Present}, {"b", 45, 40, mvcc.Present}, {"c", 45, 30, mvcc.Tombstone}} {
			got, err := store.GetAt([]byte(want.key), want.read)
			if err != nil || got.Timestamp != want.ts || got.State != want.state || (want.state == mvcc.Present && !bytes.Equal(got.Value, value(want.key, want.ts))) {
				t.Fatalf("current history %s@%d=%+v error=%v want timestamp=%d state=%d", want.key, want.read, got, err, want.ts, want.state)
			}
		}
		if _, err := store.GetAt([]byte("a"), 15); !errors.Is(err, mvcc.ErrReadBeforeDiscardFloor) {
			t.Fatalf("new discarded history read: %v", err)
		}
	}
	checkpoint("replacements and prune with both readers held")
	checkCurrent()
	checkHeld()
	cfg := col.Meta().Options.ColumnStore
	if cfg == nil || cfg.ActiveManifest == nil || cfg.ActiveManifest.Generation == 0 {
		t.Fatal("missing actual Full physical manifest")
	}
	generation, parts := cfg.ActiveManifest.Generation, cfg.PhysicalMutationParts
	next, sequence = handle.CommandWALNextLSN(), stat("treedb.cache.cow.txn.logical_seq")
	basis, valid := handle.StateToken()
	if !valid {
		t.Fatal("missing physical basis before rewrite")
	}
	rewritten, err := database.ValueLogRewriteOnline(context.Background(), treedb.ValueLogRewriteOnlineOptions{BatchSize: 1, SyncEachBatch: true})
	if err != nil || rewritten.ValueRecordsCopied == 0 || rewritten.ValueBytesCopied == 0 {
		t.Fatalf("actual online rewrite: %+v error=%v", rewritten, err)
	}
	checkCurrent()
	checkHeld()
	gc, err := database.ValueLogGC(context.Background(), treedb.ValueLogGCOptions{Mode: treedb.ValueLogGCModeStrict})
	if err != nil || gc.FailClosedToDryRun {
		t.Fatalf("actual strict GC: %+v error=%v", gc, err)
	}
	state, valid := handle.StateToken()
	cfg = col.Meta().Options.ColumnStore
	if !valid || state.CommitSeq <= basis.CommitSeq || state.AppliedCommandLSN != basis.AppliedCommandLSN || handle.CommandWALNextLSN() != next || stat("treedb.cache.cow.txn.logical_seq") != sequence || cfg == nil || cfg.ActiveManifest == nil || cfg.ActiveManifest.Generation != generation || cfg.PhysicalMutationParts != parts {
		t.Fatalf("equivalent maintenance changed logical frontier/manifest: before=%+v after=%+v valid=%v next=%d sequence=%d config=%+v", basis, state, valid, handle.CommandWALNextLSN(), stat("treedb.cache.cow.txn.logical_seq"), cfg)
	}
	checkCurrent()
	checkHeld()
	if err := heldFull.Close(); err != nil {
		t.Fatal(err)
	}
	heldFull = nil
	if err := heldHistory.Close(); err != nil {
		t.Fatal(err)
	}
	heldHistory = nil
	checkpoint("old readers released, first drain")
	checkpoint("old readers released, second drain")
	if _, err := database.ValueLogGC(context.Background(), treedb.ValueLogGCOptions{Mode: treedb.ValueLogGCModeStrict}); err != nil {
		t.Fatalf("post-release strict GC: %v", err)
	}
	checkCurrent()
	closeOriginal := func() {
		t.Helper()
		if err := manager.Close(); err != nil {
			t.Fatal(err)
		}
		manager = nil
		registry := handle.ValueLogIdentityPinRegistry()
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
		database = nil
		p, m, pins := parent.Stats(), maintenance.Stats(), registry.Stats()
		p.PeakBytes, m.PeakBytes = 0, 0
		if p != (memtable.COWStats{}) || m != (memtable.COWStats{}) || pins.ActivePins != 0 || pins.ActiveIdentities != 0 || pins.ActiveStableNamespaceLinks != 0 || registry.CleanupError() != nil {
			t.Fatalf("original owners after Close: parent=%+v maintenance=%+v registry=%+v cleanup=%v", p, m, pins, registry.CleanupError())
		}
	}
	closeOriginal()
	database, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	store = mvcc.New(database)
	handle, err = database.NativeSemanticBackend()
	if err != nil || !handle.HasNativeSemanticBridge() {
		t.Fatalf("native reopen activation: %v", err)
	}
	parent, maintenance = captureBudgets()
	manager = collections.NewCollectionManager(handle)
	col, err = manager.OpenCollection("full")
	if err != nil {
		t.Fatal(err)
	}
	state, valid = handle.StateToken()
	cfg = col.Meta().Options.ColumnStore
	if !valid || state.AppliedCommandLSN != next-1 || handle.CommandWALNextLSN() != next || cfg == nil || cfg.ActiveManifest == nil || cfg.ActiveManifest.Generation != generation || cfg.PhysicalMutationParts != parts {
		t.Fatalf("reopen command frontier/manifest: %+v valid=%v next=%d want=%d config=%+v", state, valid, handle.CommandWALNextLSN(), next, cfg)
	}
	checkCurrent()
	closeOriginal()
	t.Logf("PUBLIC durable Full/history held readers survived replacement/prune/rewrite/GC; generation=%d parts=%d nextLSN=%d rewrite=%+v gc=%+v prune=%+v; released, checkpointed, reopened, original owners zero", generation, parts, next, rewritten, gc, pruned)
}
