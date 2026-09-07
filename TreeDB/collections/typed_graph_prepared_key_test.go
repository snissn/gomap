package collections

import (
	"context"
	"errors"
	"math"
	"slices"
	"testing"
)

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
		view.Catalog.meta.VectorIndexes = slices.Clone(view.Catalog.meta.VectorIndexes)
		view.VectorIndexState.Assets = slices.Clone(view.VectorIndexState.Assets)
		mutate(&graph, &view)
		next, err := prepareTypedGraphServingBaseMetadata(graph, view, opts.Owners.Cold)
		if err != nil {
			t.Fatal(err)
		}
		exact, err := columnVectorGraphSharedPreparedSearchCacheKey(view.Catalog.meta.Name, view.AssetNamespace, view.Catalog.meta.VectorIndexes[0], graph, view.VectorIndexState)
		if err != nil || next.preparedKey != exact || next.preparedKey == want {
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
}
