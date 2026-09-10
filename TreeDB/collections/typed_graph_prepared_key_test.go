package collections

import (
	"context"
	"errors"
	"math"
	"reflect"
	"slices"
	"testing"
)

func TestTypedGraphServingMetadataChargesSegmentOwnership(t *testing.T) {
	b := &typedGraphServingBaseMetadata{}
	base, err := typedGraphServingMetadataBytes(b, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	b.view.SegmentOwnership = make([]columnManifestSegmentOwnership, 1, 3)
	b.materializerView.SegmentOwnership = make([]columnManifestSegmentOwnership, 1, 5)
	b.view.SegmentOwnership[0].Ref = ColumnAssetRef{Kind: "row-kind", Namespace: "row-namespace"}
	b.materializerView.SegmentOwnership[0].Ref = ColumnAssetRef{Kind: "typed-kind", Namespace: "typed-namespace"}
	want := base + 8*int64(reflect.TypeFor[columnManifestSegmentOwnership]().Size()) + int64(len("row-kindrow-namespacetyped-kindtyped-namespace"))
	charged, err := typedGraphServingMetadataBytes(b, math.MaxInt64)
	if err != nil || charged != want {
		t.Fatalf("ownership charge=%d want %d: %v", charged, want, err)
	}
	if _, err := typedGraphServingMetadataBytes(b, want-1); !errors.Is(err, errTypedGraphOwnerBudget) {
		t.Fatalf("accepted insufficient ownership budget: %v", err)
	}
	if _, err := typedGraphServingMetadataBytes(b, want); err != nil {
		t.Fatal(err)
	}
}

func TestTypedGraphServingRetainsExactPreparedKey(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
	index := base.indexName
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	opts := typedGraphPublicTestOptions()
	if err := col.EnsureColumnGraphServing(context.Background(), index, opts); err != nil {
		t.Fatal(err)
	}
	b := col.collectionSchemaCoordinator().typedPublication.Load().servingBase
	owner, err := col.openTypedGraphReadOwner(opts.Owners)
	if err != nil {
		t.Fatal(err)
	}
	assertTypedGraphMaterializerReuse(t, owner.overlay.current, true)
	read := owner.overlay.current
	shared := read.columnSnapshotView
	full, err := read.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if read.columnSnapshotView != shared {
		t.Fatal("first asset cache creation discarded admitted metadata")
	}
	// A cache-integrity change keeps the existing full-loader invalidation path.
	fallback, err := read.FetchDocumentsByID(ids, DocumentFetchOptions{ColumnAssetReadIntegrity: ColumnAssetReadIntegritySkipChecksums})
	if err != nil || !reflect.DeepEqual(full.Results, fallback.Results) {
		t.Fatalf("full FP32/content/meta differs from root loader: %v", err)
	}
	if read.columnSnapshotView == shared {
		t.Fatal("integrity change retained derived metadata")
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if read.columnSnapshotView != nil {
		t.Fatal("closed owner retained derived metadata")
	}
	if _, err := read.FetchDocumentsByID(ids, DocumentFetchOptions{}); err == nil {
		t.Fatal("closed owner fetch succeeded")
	}
	want, err := columnVectorGraphSharedPreparedSearchCacheKey(b.view.Catalog.meta.Name, b.view.AssetNamespace, b.view.Catalog.meta.VectorIndexes[0], b.graph, b.view.VectorIndexState)
	if err != nil {
		t.Fatal(err)
	}
	if b.preparedKey != want {
		t.Fatal("serving metadata does not retain exact existing prepared key")
	}
	charged, err := typedGraphServingMetadataBytes(b, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	without := *b
	without.preparedKey = ""
	if _, err := without.openPhysicalReader(col, nil, columnVectorGraphPhysicalRowReaderOptions{}); !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
		t.Fatalf("missing bound key accepted: %v", err)
	}
	unkeyed, err := typedGraphServingMetadataBytes(&without, math.MaxInt64)
	if err != nil || charged-unkeyed != int64(len(want)) {
		t.Fatalf("key charge=%d want%d err=%v", charged-unkeyed, len(want), err)
	}
	if _, err := typedGraphServingMetadataBytes(b, charged-1); err == nil {
		t.Fatal("accepted insufficient retained key budget")
	}
	if _, err := typedGraphServingMetadataBytes(b, charged); err != nil {
		t.Fatal(err)
	}
	// Admission must use real full records, including the adapter identity header.
	snap := col.db.AcquireSnapshot()
	records, err := loadColumnManifestRecordsFromRoot(snap, b.view.Catalog.rootID(collectionColumnManifestRootName(col.Name())))
	if err != nil {
		t.Fatal(err)
	}
	realView := b.view
	realView.graphOwnerRecords = records
	if _, err := prepareTypedGraphServingBaseMetadata(b.graph, b.view, opts.Owners.Cold); err == nil {
		t.Fatal("accepted missing real records")
	}
	cold := opts.Owners.Cold
	cold.ManifestRecords = len(records)
	if _, err := prepareTypedGraphMaterializerMetadata(realView, cold); !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
		t.Fatalf("unadmitted identity entry: %v", err)
	}
	cold.ManifestRecords++
	cold.ManifestBytes = columnManifestRecordsBytes(records) - 1
	if _, err := prepareTypedGraphMaterializerMetadata(realView, cold); !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
		t.Fatalf("unadmitted raw bytes: %v", err)
	}
	cold.ManifestBytes++
	cold.DecodedTermBytes = int64(len(records)+1)*int64(reflect.TypeFor[systemTargetEntry]().Size()) + int64(len(columnManifestIdentityRecordKey)+columnManifestIdentityRecordSize)
	if _, err := prepareTypedGraphMaterializerMetadata(realView, cold); err != nil {
		t.Fatal(err)
	}
	cold.DecodedTermBytes--
	if _, err := prepareTypedGraphMaterializerMetadata(realView, cold); !errors.Is(err, errTypedGraphOwnerBudget) {
		t.Fatalf("unadmitted adapter headers: %v", err)
	}
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	// A retained nested SortKey owns both slice capacity and decoded strings.
	withSort := *b
	withSort.materializerView.TypedColumnPartRefs = make([]columnManifestAssetRefForScan, len(b.materializerView.TypedColumnPartRefs), cap(b.materializerView.TypedColumnPartRefs))
	copy(withSort.materializerView.TypedColumnPartRefs, b.materializerView.TypedColumnPartRefs)
	withSort.materializerView.TypedColumnPartRefs[0].SortKey = []ColumnSortKey{{Column: "embedding", Direction: "asc"}}
	sortCharge, err := typedGraphServingMetadataBytes(&withSort, math.MaxInt64)
	wantSortCharge := int64(reflect.TypeFor[ColumnSortKey]().Size()) + int64(len("embeddingasc"))
	if err != nil || sortCharge-charged != wantSortCharge {
		t.Fatalf("nested SortKey charge=%d want%d err=%v", sortCharge-charged, wantSortCharge, err)
	}
	for _, mutate := range []func(*columnVectorGraphManifestSnapshot, *columnPhysicalScanSnapshotView){
		func(_ *columnVectorGraphManifestSnapshot, v *columnPhysicalScanSnapshotView) {
			v.AssetNamespace += "-other"
			for i := range v.VectorIndexState.Assets {
				v.VectorIndexState.Assets[i].Ref.Namespace = v.AssetNamespace
			}
		},
		func(_ *columnVectorGraphManifestSnapshot, v *columnPhysicalScanSnapshotView) {
			v.Catalog.meta.VectorIndexes[0].EfSearch++
		},
		func(g *columnVectorGraphManifestSnapshot, _ *columnPhysicalScanSnapshotView) {
			g.BaseManifestGeneration++
		},
		func(g *columnVectorGraphManifestSnapshot, _ *columnPhysicalScanSnapshotView) { g.GraphSchemaHash++ },
		func(_ *columnVectorGraphManifestSnapshot, v *columnPhysicalScanSnapshotView) {
			v.VectorIndexState.BaseManifestGeneration++
			v.Config.ActiveManifest.Generation++
			for i := range v.VectorIndexState.Assets {
				v.VectorIndexState.Assets[i].Ref.Generation++
			}
		},
		func(_ *columnVectorGraphManifestSnapshot, v *columnPhysicalScanSnapshotView) {
			v.VectorIndexState.Assets[0].Ref.Checksum++
		},
	} {
		graph, view := b.graph, b.view
		view.Catalog = view.Catalog.copy()
		view.Config = view.Config.copy()
		view.Catalog.meta.VectorIndexes = slices.Clone(view.Catalog.meta.VectorIndexes)
		view.VectorIndexState.Assets = slices.Clone(view.VectorIndexState.Assets)
		mutate(&graph, &view)
		exact, err := columnVectorGraphSharedPreparedSearchCacheKey(view.Catalog.meta.Name, view.AssetNamespace, view.Catalog.meta.VectorIndexes[0], graph, view.VectorIndexState)
		if err != nil || exact == want {
			t.Fatalf("changed identity not rebound err=%v", err)
		}
	}
	for _, mutate := range []func(*columnVectorGraphManifestSnapshot, *columnPhysicalScanSnapshotView){
		func(g *columnVectorGraphManifestSnapshot, _ *columnPhysicalScanSnapshotView) { g.RowCount = 0 },
		func(_ *columnVectorGraphManifestSnapshot, v *columnPhysicalScanSnapshotView) { v.AssetNamespace = "" },
		func(_ *columnVectorGraphManifestSnapshot, v *columnPhysicalScanSnapshotView) {
			v.VectorIndexStateFound = false
		},
	} {
		graph, view := b.graph, b.view
		mutate(&graph, &view)
		if columnVectorGraphSharedPreparedEligible(graph, view) {
			t.Fatal("changed eligibility")
		}
	}
	held, err := col.openTypedGraphReadOwner(opts.Owners)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	assertTypedGraphMaterializerReuse(t, held.overlay.current, true)
	changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"changed"}}, {Name: "user", Strings: []string{"changed"}}, {Name: "path", Strings: []string{"changed"}}}
	if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
		t.Fatal(err)
	}
	if col.collectionSchemaCoordinator().typedPublication.Load().servingBase != b {
		t.Fatal("same-base suffix replaced immutable metadata")
	}
	if err := col.FoldColumnGraphServing(context.Background(), index); err != nil {
		t.Fatal(err)
	}
	after := col.collectionSchemaCoordinator().typedPublication.Load().servingBase
	if after == b || after.preparedKey == b.preparedKey {
		t.Fatal("fold did not replace base identity")
	}
	old, err := held.overlay.current.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil || !reflect.DeepEqual(full.Results, old.Results) {
		t.Fatalf("prepared old owner lost full values across mutation/Fold: %v", err)
	}
}

func assertTypedGraphMaterializerReuse(t *testing.T, read *CollectionReadView, reused bool) {
	t.Helper()
	if (read.columnSnapshotView != nil) != reused {
		t.Fatalf("full materializer reuse=%v want%v", read.columnSnapshotView != nil, reused)
	}
	if !reused {
		return
	}
	b := read.collection.collectionSchemaCoordinator().typedPublication.Load().servingBase
	view := read.columnSnapshotView
	if view.Catalog != read.catalog || view.snapshot != read.snapshot || view.CommitSeq == 0 || view.SystemRoot == 0 {
		t.Fatal("materializer header is not bound to admitted current owner")
	}
	root := read.catalog.rootID(collectionColumnManifestRootName(read.catalog.meta.Name))
	if view.Diagnostics.ManifestRoot != root || root == b.view.Catalog.rootID(collectionColumnManifestRootName(read.catalog.meta.Name)) {
		t.Fatal("reuse requires current diagnostics across independently copied roots")
	}
	if !reflect.DeepEqual(view.FullConfig, *read.catalog.meta.Options.ColumnStore) || !reflect.DeepEqual(view.Config, columnStoreRowAssetConfig(view.FullConfig)) {
		t.Fatal("materializer reused partial config")
	}
	if len(view.TypedColumnPartRefs) == 0 || &view.TypedColumnPartRefs[0] != &b.materializerView.TypedColumnPartRefs[0] {
		t.Fatal("materializer did not share complete typed refs")
	}
	if b.materializerView.snapshot != nil || b.materializerView.CommitSeq != 0 || b.materializerView.SystemRoot != 0 {
		t.Fatal("shared metadata retained a query snapshot")
	}
}

func TestTypedGraphCurrentMaterializerAcrossMutations(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
	index := base.indexName
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	opts := typedGraphPublicTestOptions()
	if err := col.EnsureColumnGraphServing(context.Background(), index, opts); err != nil {
		t.Fatal(err)
	}
	held, err := col.openTypedGraphReadOwner(opts.Owners)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	original, err := held.overlay.current.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	newIDs := [][]byte{[]byte("new-row")}
	allIDs := append(slices.Clone(ids), newIDs...)
	changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"changed content"}}, {Name: "user", Strings: []string{"changed user"}}, {Name: "path", Strings: []string{"changed path"}}}
	for _, step := range []struct {
		name string
		run  func() error
	}{
		{"replace", func() error { _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); return err }},
		{"insert", func() error {
			_, _, err := col.InsertTypedBatchWithStats(newIDs, [][]byte{[]byte(`{"id":"new-row"}`)}, changed)
			return err
		}},
		{"delete", func() error { _, err := col.DeleteBatch(ids[1:2]); return err }},
		{"fold", func() error { return col.FoldColumnGraphServing(context.Background(), index) }},
	} {
		t.Run(step.name, func(t *testing.T) {
			if err := step.run(); err != nil {
				t.Fatal(err)
			}
			owner, err := col.openTypedGraphReadOwner(opts.Owners)
			if err != nil {
				t.Fatal(err)
			}
			defer owner.Close()
			read := owner.overlay.current
			if read.columnSnapshotView == nil {
				t.Fatal("current full fetch must reuse validated publication metadata after mutation")
			}
			got, err := read.FetchDocumentsByID(allIDs, DocumentFetchOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if read.pointRowRefs != nil || read.typedColumnReconstructionCache == nil || read.typedColumnReconstructionCache.Refs != nil {
				t.Fatal("prepared full fetch rebuilt per-request reference maps")
			}
			// Independent ordinary loader, bound to exactly the same snapshot.
			plain := &CollectionReadView{collection: col, snapshot: read.snapshot, catalog: read.catalog}
			defer plain.Close()
			want, err := plain.FetchDocumentsByID(allIDs, DocumentFetchOptions{})
			if err != nil || !reflect.DeepEqual(got.Results, want.Results) {
				t.Fatalf("current full payload differs from root loader: %v", err)
			}
			old, err := held.overlay.current.FetchDocumentsByID(ids, DocumentFetchOptions{})
			if err != nil || !reflect.DeepEqual(old.Results, original.Results) {
				t.Fatalf("old owner lost original full payload: %v", err)
			}
		})
	}
}
