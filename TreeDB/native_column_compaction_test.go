package treedb

import (
	"context"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

// Use the maintained retained-JSON string/row-asset layout: Int64 semantic
// streams currently have no public native replacement producer. Compaction must
// preserve the captured old assets while replacing both physical and pending
// mutation history with the latest visible rows.
func TestNativeColumnStoreCompactLatestVisibleHeldReopen(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := indexedSliceOptions(ProfileCommandWALDurable, t.TempDir())
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(handle)
	var held *collections.CollectionReadView
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

	const family = "string"
	fixture := nativeColumnPendingCaseFor(t, family)
	fixture.column.Owner = collections.TypedStorageOwnerRowAsset
	fixture.typedSeed = &collections.TypedColumnBatch{} // Maintained retained-JSON contract.
	if _, err := manager.CreateCollection(nativeColumnPendingMeta(fixture)); err != nil {
		t.Fatalf("native typed Create: %v", err)
	}
	col, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	a, b, c := nativeTypedOpsRows(family)
	ids, retained, columns := nativeTypedOpsInput(family, a, b, c)
	inserted, insertStats, err := col.InsertTypedBatchWithStats(ids, retained, columns)
	if err != nil || len(inserted) != 3 || insertStats.Documents != 3 {
		t.Fatalf("typed seed ids=%q stats=%+v: %v", inserted, insertStats, err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	held, err = col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	checkHeld := func() {
		t.Helper()
		got, err := held.FetchDocumentsByID(ids, collections.DocumentFetchOptions{})
		if err != nil || len(got.Results) != 3 {
			t.Fatalf("held seed rows=%+v: %v", got.Results, err)
		}
		for i, row := range []nativeTypedOpsRow{a, b, c} {
			if !got.Results[i].Found {
				t.Fatalf("held seed row %s missing", row.id)
			}
			checkNativeTypedOpsDocument(t, family, got.Results[i].Document, row)
		}
	}
	checkHeld() // Materialize original assets before any replacement.

	physicalA := nativeTypedOpsChanged(a)
	replaceIDs, replacement, replacementColumns := nativeTypedOpsInput(family, physicalA)
	results, err := col.ReplaceTypedBatch(replaceIDs, replacement, replacementColumns)
	if err != nil || len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("checkpointed typed replacement=%+v: %v", results, err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	physicalConfig := col.Meta().Options.ColumnStore
	if physicalConfig == nil || physicalConfig.ActiveManifest == nil || physicalConfig.PhysicalMutationParts == 0 {
		t.Fatalf("checkpointed replacement lacks a mutation-bearing manifest: %+v", physicalConfig)
	}
	physicalGeneration := physicalConfig.ActiveManifest.Generation
	latestA := nativeTypedOpsChanged(physicalA)
	replaceIDs, replacement, replacementColumns = nativeTypedOpsInput(family, latestA)
	results, err = col.ReplaceTypedBatch(replaceIDs, replacement, replacementColumns)
	if err != nil || len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("pending typed replacement=%+v: %v", results, err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte(b.id)}); err != nil || deleted != 1 {
		t.Fatalf("pending typed deletion=%d: %v", deleted, err)
	}
	if applied, next := indexedCRUDAppliedLSN(t, handle), handle.CommandWALNextLSN(); applied >= next-1 {
		t.Fatalf("compaction input must include pending mutations: applied=%d next=%d", applied, next)
	}
	checkNativeTypedOpsCurrent(t, col, family, []nativeTypedOpsRow{latestA, c}, []string{b.id})
	checkHeld()

	stats, err := col.ColumnStoreCompact(context.Background(), collections.ColumnStoreCompactOptions{})
	if err != nil {
		t.Fatalf("native ColumnStoreCompact: %v", err)
	}
	if !stats.Compacted || stats.PreviousGeneration < physicalGeneration || stats.NewGeneration <= stats.PreviousGeneration || stats.RowsCompacted != 2 || stats.MutationPartsBefore == 0 || stats.MutationPartsAfter != 0 || stats.AssetsPublished == 0 || len(stats.PublishedRefs) == 0 || len(stats.SupersededRefs) == 0 {
		t.Fatalf("latest-visible native compaction stats=%+v physical generation=%d", stats, physicalGeneration)
	}
	checkNativeTypedOpsCurrent(t, col, family, []nativeTypedOpsRow{latestA, c}, []string{b.id})
	checkHeld()
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	cfg := col.Meta().Options.ColumnStore
	if cfg == nil || cfg.ActiveManifest == nil || cfg.ActiveManifest.Generation != stats.NewGeneration || cfg.PhysicalMutationParts != 0 {
		t.Fatalf("checkpoint lost compacted insert-only manifest: %+v", cfg)
	}
	checkHeld()

	// A later producer must advance from the compacted manifest, rather than
	// resurrect the superseded physical or pending mutation lineage.
	finalC := nativeTypedOpsChanged(c)
	replaceIDs, replacement, replacementColumns = nativeTypedOpsInput(family, finalC)
	results, err = col.ReplaceTypedBatch(replaceIDs, replacement, replacementColumns)
	if err != nil || len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("post-compaction typed replacement=%+v: %v", results, err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkNativeTypedOpsCurrent(t, col, family, []nativeTypedOpsRow{latestA, finalC}, []string{b.id})
	checkHeld()
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	held = nil
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager = nil
	cache, pins := database.cached, handle.ValueLogIdentityPinRegistry()
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	requirePublicCOWFinalZero(t, cache)
	if s := pins.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 || s.ActiveStableNamespaceLinks != 0 || pins.CleanupError() != nil {
		t.Fatalf("closed compaction resources=%+v", s)
	}

	database, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	handle, err = database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(handle)
	col, err = manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	checkNativeTypedOpsCurrent(t, col, family, []nativeTypedOpsRow{latestA, finalC}, []string{b.id})
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager = nil
	cache, pins = database.cached, handle.ValueLogIdentityPinRegistry()
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	requirePublicCOWFinalZero(t, cache)
	if s := pins.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 || s.ActiveStableNamespaceLinks != 0 || pins.CleanupError() != nil {
		t.Fatalf("reopened compaction resources=%+v", s)
	}
}

func TestNativeColumnStoreCompactEmptyAndInsertOnly(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	database, err := Open(indexedSliceOptions(ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(handle)
	var held *collections.CollectionReadView
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

	const family = "string"
	fixture := nativeColumnPendingCaseFor(t, family)
	fixture.column.Owner = collections.TypedStorageOwnerRowAsset
	fixture.typedSeed = &collections.TypedColumnBatch{}
	if _, err := manager.CreateCollection(nativeColumnPendingMeta(fixture)); err != nil {
		t.Fatalf("native typed Create: %v", err)
	}
	col, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	a, _, _ := nativeTypedOpsRows(family)
	ids, retained, columns := nativeTypedOpsInput(family, a)
	held, err = col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	checkHeld := func(found bool) {
		t.Helper()
		got, err := held.FetchDocumentsByID(ids, collections.DocumentFetchOptions{})
		if err != nil || len(got.Results) != 1 || got.Results[0].Found != found {
			t.Fatalf("held row found=%t results=%+v: %v", found, got.Results, err)
		}
		if found {
			checkNativeTypedOpsDocument(t, family, got.Results[0].Document, a)
		}
	}
	checkNoRewrite := func(stats collections.ColumnStoreCompactStats, next uint64) {
		t.Helper()
		if stats.Compacted || stats.NewGeneration != 0 || stats.ManifestRoot != 0 || stats.ManifestRecordsAfter != 0 || stats.MutationPartsBefore != 0 || stats.MutationPartsAfter != 0 || stats.RowsCompacted != 0 || stats.AssetsPublished != 0 || stats.AggregateMetadataPublished != 0 || len(stats.PublishedRefs) != 0 || len(stats.SupersededRefs) != 0 {
			t.Fatalf("no-op compaction reported replacement outputs: %+v", stats)
		}
		snapshot := handle.AcquireSnapshot()
		if snapshot == nil {
			t.Fatal("missing accepted physical snapshot")
		}
		token, ok := snapshot.StateToken()
		snapshot.Close()
		if !ok || stats.SystemRoot == 0 || stats.SystemRoot != token.SystemRootPageID || next == 0 || token.AppliedCommandLSN != next-1 || handle.CommandWALNextLSN() != next {
			t.Fatalf("no-op accepted boundary stats=%+v token=%+v next=%d want=%d", stats, token, handle.CommandWALNextLSN(), next)
		}
	}
	checkHeld(false)
	next := handle.CommandWALNextLSN()
	stats, err := col.ColumnStoreCompact(context.Background(), collections.ColumnStoreCompactOptions{})
	if err != nil {
		t.Fatalf("empty native compaction: %v", err)
	}
	checkNoRewrite(stats, next)
	if stats.PreviousGeneration != 0 || stats.ManifestRecordsBefore != 0 || stats.RowsScanned != 0 || stats.DeletedRows != 0 || stats.PhysicalBytesRead != 0 {
		t.Fatalf("empty compaction reported physical input: %+v", stats)
	}
	checkNativeTypedOpsCurrent(t, col, family, nil, []string{a.id})
	checkHeld(false)
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if cfg := col.Meta().Options.ColumnStore; cfg == nil || cfg.ActiveManifest != nil || cfg.RecoveryAuthoritativeManifest != nil || cfg.PhysicalMutationParts != 0 {
		t.Fatalf("empty compaction invented a manifest: %+v", cfg)
	}
	if applied := indexedCRUDAppliedLSN(t, handle); applied != next-1 || handle.CommandWALNextLSN() != next {
		t.Fatalf("empty checkpoint frontier applied=%d next=%d want=%d", applied, handle.CommandWALNextLSN(), next)
	}

	inserted, insertStats, err := col.InsertTypedBatchWithStats(ids, retained, columns)
	if err != nil || len(inserted) != 1 || insertStats.Documents != 1 {
		t.Fatalf("typed seed ids=%q stats=%+v: %v", inserted, insertStats, err)
	}
	if handle.CommandWALNextLSN() != next+1 {
		t.Fatalf("seed command frontier=%d want=%d", handle.CommandWALNextLSN(), next+1)
	}
	next++
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkNativeTypedOpsCurrent(t, col, family, []nativeTypedOpsRow{a}, nil)
	checkHeld(false) // The original empty view survives the first physical asset.
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	held = nil
	cfg := col.Meta().Options.ColumnStore
	if cfg == nil || cfg.ActiveManifest == nil || cfg.PhysicalMutationParts != 0 {
		t.Fatalf("seed is not a physical insert-only generation: %+v", cfg)
	}
	identity := *cfg.ActiveManifest
	held, err = col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	checkHeld(true)
	stats, err = col.ColumnStoreCompact(context.Background(), collections.ColumnStoreCompactOptions{})
	if err != nil {
		t.Fatalf("insert-only native compaction: %v", err)
	}
	checkNoRewrite(stats, next)
	if stats.PreviousGeneration != identity.Generation || stats.ManifestRecordsBefore == 0 || stats.RowsScanned != 1 || stats.DeletedRows != 0 || stats.PhysicalBytesRead <= 0 {
		t.Fatalf("insert-only compaction input stats=%+v identity=%+v", stats, identity)
	}
	checkNativeTypedOpsCurrent(t, col, family, []nativeTypedOpsRow{a}, nil)
	checkHeld(true)
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	cfg = col.Meta().Options.ColumnStore
	if cfg == nil || cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeManifest == nil || *cfg.ActiveManifest != identity || *cfg.RecoveryAuthoritativeManifest != identity || cfg.PhysicalMutationParts != 0 {
		t.Fatalf("insert-only compaction changed manifest identity: %+v", cfg)
	}
	if applied := indexedCRUDAppliedLSN(t, handle); applied != next-1 || handle.CommandWALNextLSN() != next {
		t.Fatalf("insert-only checkpoint frontier applied=%d next=%d want=%d", applied, handle.CommandWALNextLSN(), next)
	}
	checkNativeTypedOpsCurrent(t, col, family, []nativeTypedOpsRow{a}, nil)
	checkHeld(true)
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	held = nil
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager = nil
	cache, pins := database.cached, handle.ValueLogIdentityPinRegistry()
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	requirePublicCOWFinalZero(t, cache)
	if s := pins.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 || s.ActiveStableNamespaceLinks != 0 || pins.CleanupError() != nil {
		t.Fatalf("closed empty/insert-only compaction resources=%+v", s)
	}
}
