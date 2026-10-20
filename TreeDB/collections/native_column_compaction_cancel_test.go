package collections

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Caller cancellation is a refusal before adoption, but cannot abandon the
// original accepted build after its replacement assets have been constructed.
func TestNativeColumnStoreCompactCancellationBoundary(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	for _, afterAssets := range []bool{false, true} {
		name := "pre-canceled"
		if afterAssets {
			name = "cancel-after-assets"
		}
		t.Run(name, func(t *testing.T) {
			opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.FlushThreshold = 1 << 30
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			opts.MaxWALBytes = -1
			database, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			handle, err := database.NativeSemanticBackend()
			if err != nil {
				_ = database.Close()
				t.Fatal(err)
			}
			manager := NewCollectionManager(handle)
			var held *CollectionReadView
			t.Cleanup(func() {
				if held != nil {
					if err := held.Close(); err != nil {
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
			if _, err := manager.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
				Enabled: true, Columns: []ColumnStoreColumn{{Name: "label", Path: "label", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerRowAsset}},
				RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
			}}}); err != nil {
				t.Fatal(err)
			}
			col, err := manager.OpenCollection("events")
			if err != nil {
				t.Fatal(err)
			}
			ids := [][]byte{[]byte("a"), []byte("b")}
			retained := [][]byte{[]byte(`{"payload":"seed-a"}`), []byte(`{"payload":"seed-b"}`)}
			if _, _, err := col.InsertTypedBatchWithStats(ids, retained, []TypedColumnBatch{{Name: "label", Strings: []string{"alpha", "beta"}}}); err != nil {
				t.Fatal(err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			held, err = col.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			checkDocument := func(got []byte, label, payload string) {
				t.Helper()
				var decoded map[string]string
				if err := json.Unmarshal(got, &decoded); err != nil || len(decoded) != 2 || decoded["label"] != label || decoded["payload"] != payload {
					t.Fatalf("document=%s want label=%q payload=%q: %v", got, label, payload, err)
				}
			}
			checkHeld := func() {
				t.Helper()
				got, err := held.FetchDocumentsByID(ids, DocumentFetchOptions{})
				if err != nil || len(got.Results) != 2 || !got.Results[0].Found || !got.Results[1].Found {
					t.Fatalf("held seed rows=%+v: %v", got.Results, err)
				}
				checkDocument(got.Results[0].Document, "alpha", "seed-a")
				checkDocument(got.Results[1].Document, "beta", "seed-b")
			}
			checkHeld()
			replace := func(id, label, payload string) {
				t.Helper()
				doc, err := json.Marshal(map[string]string{"payload": payload})
				if err != nil {
					t.Fatal(err)
				}
				results, err := col.ReplaceTypedBatch([][]byte{[]byte(id)}, [][]byte{doc}, []TypedColumnBatch{{Name: "label", Strings: []string{label}}})
				if err != nil || len(results) != 1 || !results[0].Matched || !results[0].Modified {
					t.Fatalf("replace %s=%+v: %v", id, results, err)
				}
			}
			replace("a", "physical", "physical-a")
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if cfg := col.Meta().Options.ColumnStore; cfg.ActiveManifest == nil || cfg.PhysicalMutationParts == 0 {
				t.Fatalf("fixture lacks physical mutation history: %+v", cfg)
			}
			replace("a", "latest", "pending-a")
			// Retain only the original bare accounting authorities for final-zero
			// inspection; no engine/capture alias survives this opening read.
			read, err := manager.nativeJSON.Capture("events")
			if err != nil {
				t.Fatal(err)
			}
			engine := read.capture.State().(*nativeEngineVersion)
			type memoryStats interface{ Stats() memtable.COWStats }
			budget := engine.allocator.(memoryStats)
			maintenance := engine.maintenance.(memoryStats)
			engine = nil
			if err := read.Close(); err != nil {
				t.Fatal(err)
			}
			read = nil
			before, ok := handle.StateToken()
			if !ok {
				t.Fatal("missing pre-compaction state token")
			}
			next := handle.CommandWALNextLSN()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			called := false
			restore := setColumnStoreCompactionAfterPrepareTestHook(func(prepared ColumnPublishPreparedAssets) error {
				called = true
				if !afterAssets || len(prepared.Assets) == 0 {
					t.Fatalf("unexpected asset-prepared hook: afterAssets=%v assets=%d", afterAssets, len(prepared.Assets))
				}
				cancel()
				return nil
			})
			defer restore()
			if !afterAssets {
				cancel()
			}
			stats, err := col.ColumnStoreCompact(ctx, ColumnStoreCompactOptions{})
			restore()
			if afterAssets {
				if err != nil || !called || !stats.Compacted || stats.AssetsPublished == 0 || stats.SystemRoot == 0 || !errors.Is(ctx.Err(), context.Canceled) {
					t.Fatalf("adopted compaction must finish: hook=%v stats=%+v ctx=%v err=%v", called, stats, ctx.Err(), err)
				}
			} else {
				after, ok := handle.StateToken()
				if !errors.Is(err, context.Canceled) || called || !ok || after != before || stats.Compacted || stats.SystemRoot != 0 {
					t.Fatalf("pre-canceled compaction published: hook=%v stats=%+v before=%+v after=%+v err=%v", called, stats, before, after, err)
				}
			}
			if handle.CommandWALNextLSN() != next {
				t.Fatal("compaction invented a canonical command")
			}
			checkHeld()
			replace("b", "final", "final-b") // Cancellation must not poison future writes.
			if err := database.Checkpoint(); err != nil {
				t.Fatalf("checkpoint after cancellation: %v", err)
			}
			checkCurrent := func() {
				t.Helper()
				for _, row := range []struct{ id, label, payload string }{{"a", "latest", "pending-a"}, {"b", "final", "final-b"}} {
					got, err := col.Get([]byte(row.id))
					if err != nil || len(got) == 0 {
						t.Fatalf("Get %s=%s: %v", row.id, got, err)
					}
					checkDocument(got, row.label, row.payload)
				}
			}
			checkCurrent()
			checkHeld()
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			held = nil
			closeOwner := func() {
				t.Helper()
				pins := handle.ValueLogIdentityPinRegistry()
				if err := manager.Close(); err != nil {
					t.Fatal(err)
				}
				manager = nil
				if err := database.Close(); err != nil {
					t.Fatal(err)
				}
				database = nil
				for _, authority := range []memoryStats{budget, maintenance} {
					if s := authority.Stats(); s.TotalBytes != 0 || s.Sources != 0 || s.ExternalLeases != 0 || s.Views != 0 || s.Generations != 0 {
						t.Fatalf("closed accounting authority=%+v", s)
					}
				}
				if s := pins.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 || s.ActiveStableNamespaceLinks != 0 || pins.CleanupError() != nil {
					t.Fatalf("closed stable resources=%+v cleanup=%v", s, pins.CleanupError())
				}
			}
			closeOwner()
			database, err = treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			handle, err = database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager = NewCollectionManager(handle)
			col, err = manager.OpenCollection("events")
			if err != nil {
				t.Fatal(err)
			}
			read, err = manager.nativeJSON.Capture("events")
			if err != nil {
				t.Fatal(err)
			}
			engine = read.capture.State().(*nativeEngineVersion)
			budget, maintenance = engine.allocator.(memoryStats), engine.maintenance.(memoryStats)
			engine = nil
			if err := read.Close(); err != nil {
				t.Fatal(err)
			}
			read = nil
			checkCurrent()
			closeOwner()
		})
	}
}
