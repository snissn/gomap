package collections

import (
	"errors"
	"slices"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestTypedGraphOverlaySuffixLineageAndBounds(t *testing.T) {
	part := func(generation uint64, kind ColumnAssetKind, reason ColumnPublishOperation, role ColumnManifestPartRole) columnManifestAssetRefForScan {
		return columnManifestAssetRefForScan{Ref: ColumnAssetRef{Kind: kind, Namespace: "test", Generation: generation, PartID: generation, Length: 10, Checksum: uint32(generation)}, Rows: 1, Reason: reason, Role: role}
	}
	base := columnPhysicalScanSnapshotView{CollectionName: "test", AssetNamespace: "test", CommitSeq: 1, FullConfig: ColumnStoreConfig{SchemaHash: 7, ActiveManifest: &ColumnManifestIdentity{Generation: 1}}}
	base.AssetRefs = []columnManifestAssetRefForScan{part(1, ColumnAssetKindTCS1PartImage, ColumnPublishOperationInsert, ColumnManifestPartRoleBase)}
	base.TypedColumnPartRefs = []columnManifestAssetRefForScan{part(1, ColumnAssetKindTCS1TypedColumnPart, ColumnPublishOperationInsert, ColumnManifestPartRoleBase)}
	current := base
	current.CommitSeq = 3
	current.FullConfig.ActiveManifest = &ColumnManifestIdentity{Generation: 3}
	current.AssetRefs = append(slices.Clone(base.AssetRefs), part(2, ColumnAssetKindTCS1PartImage, ColumnPublishOperationInsert, ColumnManifestPartRoleBase), part(3, ColumnAssetKindTCS1PartImage, ColumnPublishOperationDelete, ColumnManifestPartRoleTombstone))
	current.TypedColumnPartRefs = append(slices.Clone(base.TypedColumnPartRefs), part(2, ColumnAssetKindTCS1TypedColumnPart, ColumnPublishOperationInsert, ColumnManifestPartRoleBase))
	limits := typedGraphOverlayLimits{Rows: 2, Tombstones: 1, Bytes: 30}
	suffix, err := checkedTypedGraphOverlaySuffix(base, current, limits)
	if err != nil || suffix.rows != 2 || suffix.tombstones != 1 || suffix.bytes != 30 || len(suffix.view.TypedColumnPartRefs) != 1 {
		t.Fatalf("append-only plus tombstone suffix: %+v, %v", suffix, err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*columnPhysicalScanSnapshotView, *typedGraphOverlayLimits)
		want   error
	}{
		{"rows", func(_ *columnPhysicalScanSnapshotView, l *typedGraphOverlayLimits) { l.Rows-- }, errTypedGraphOverlayFoldNeeded},
		{"tombstones", func(_ *columnPhysicalScanSnapshotView, l *typedGraphOverlayLimits) { l.Tombstones-- }, errTypedGraphOverlayFoldNeeded},
		{"bytes", func(_ *columnPhysicalScanSnapshotView, l *typedGraphOverlayLimits) { l.Bytes-- }, errTypedGraphOverlayFoldNeeded},
		{"missing_base", func(v *columnPhysicalScanSnapshotView, _ *typedGraphOverlayLimits) { v.AssetRefs = v.AssetRefs[1:] }, ErrVectorIndexSnapshotMismatch},
		{"missing_vector_base", func(v *columnPhysicalScanSnapshotView, _ *typedGraphOverlayLimits) {
			v.TypedColumnPartRefs = v.TypedColumnPartRefs[1:]
		}, ErrVectorIndexSnapshotMismatch},
		{"changed_base", func(v *columnPhysicalScanSnapshotView, _ *typedGraphOverlayLimits) { v.AssetRefs[0].Ref.Checksum++ }, ErrVectorIndexSnapshotMismatch},
		{"namespace", func(v *columnPhysicalScanSnapshotView, _ *typedGraphOverlayLimits) { v.AssetNamespace = "recreated" }, ErrVectorIndexSnapshotMismatch},
		{"schema", func(v *columnPhysicalScanSnapshotView, _ *typedGraphOverlayLimits) { v.FullConfig.SchemaHash++ }, ErrVectorIndexSnapshotMismatch},
		{"future_part", func(v *columnPhysicalScanSnapshotView, _ *typedGraphOverlayLimits) { v.AssetRefs[2].Ref.Generation++ }, ErrVectorIndexSnapshotMismatch},
	} {
		t.Run(test.name, func(t *testing.T) {
			view, bound := current, limits
			view.AssetRefs = slices.Clone(current.AssetRefs)
			view.TypedColumnPartRefs = slices.Clone(current.TypedColumnPartRefs)
			test.mutate(&view, &bound)
			if _, err := checkedTypedGraphOverlaySuffix(base, view, bound); !errors.Is(err, test.want) {
				t.Fatalf("got %v, want %v", err, test.want)
			}
		})
	}
}

func TestTypedGraphOverlayExistingBaseMutation(t *testing.T) {
	for _, operation := range []string{"insert", "replace", "delete"} {
		t.Run(operation, func(t *testing.T) {
			_, db, col := openTypedMinimaCollection(t)
			defer db.Close()
			ids, retained := [][]byte{[]byte("base")}, [][]byte{[]byte(`{"id":"base"}`)}
			columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"alpha"}}, {Name: "user", Strings: []string{"tenant"}}, {Name: "path", Strings: []string{"source"}}}
			if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
				t.Fatal(err)
			}
			if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
				t.Fatal(err)
			}
			base, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: "embedding_graph"})
			if err != nil {
				t.Fatal(err)
			}
			defer base.Close()
			wantID := "base"
			switch operation {
			case "insert":
				ids[0], retained[0], wantID = []byte("new"), []byte(`{"id":"new"}`), "new"
				columns[0].Float32Vectors[0] = []float32{0, 1, 0, 0, 0, 0, 0, 0}
				_, _, err = col.InsertTypedBatchWithStats(ids, retained, columns)
			case "replace":
				columns[0].Float32Vectors[0] = []float32{0, 1, 0, 0, 0, 0, 0, 0}
				_, err = col.ReplaceTypedBatch(ids, retained, columns)
			case "delete":
				wantID = ""
				err = col.Delete(ids[0])
			}
			if err != nil {
				t.Fatalf("existing-base %s admission: %v", operation, err)
			}
			_, err = col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: "embedding_graph", Query: []float32{0, 1, 0, 0, 0, 0, 0, 0}, TopK: 1, EfSearch: 8})
			if !errors.Is(err, ErrVectorIndexSearchUnavailable) {
				t.Fatalf("public mutable graph route must remain gated until durable lifecycle integration: %v", err)
			}
			current, err := col.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			defer current.Close()
			suffix, err := prepareTypedGraphOverlaySuffix(base, current, typedGraphOverlayLimits{Rows: 8, Tombstones: 8, Bytes: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			// The actual current manifest rejects a different immutable graph
			// identity even when collection/schema/index definitions are equal.
			staleBase := *base
			staleReader := *base.reader
			staleReader.graph.BaseManifestChecksum++
			staleBase.reader = &staleReader
			if _, err := prepareTypedGraphOverlaySuffix(&staleBase, current, typedGraphOverlayLimits{Rows: 8, Tombstones: 8, Bytes: 1 << 20}); !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
				t.Fatalf("different graph identity accepted: %v", err)
			}
			rows, err := suffix.prepareRows(current, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 || string(rows[0].ID) != string(ids[0]) || rows[0].Deleted != (wantID == "") {
				t.Fatalf("wrong authoritative suffix: %+v", rows)
			}
			if wantID != "" && (len(rows[0].Values) != 4 || len(rows[0].Values[0].Float32Vector) != 8 || rows[0].Values[0].Float32Vector[1] != 1 || rows[0].Values[1].String != "alpha") {
				t.Fatalf("typed suffix lost vector/string authority: %+v", rows[0].Values)
			}
			if _, err := prepareTypedGraphOverlaySuffix(base, current, typedGraphOverlayLimits{Rows: 8, Tombstones: 8, Bytes: 1}); !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
				t.Fatalf("byte bound: %v", err)
			}
		})
	}
}

func TestTypedGraphOverlayRepeatedMutationAndSnapshot(t *testing.T) {
	_, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	ids, retained := [][]byte{[]byte("base")}, [][]byte{[]byte(`{"id":"base"}`)}
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"original"}}, {Name: "user", Strings: []string{"tenant"}}, {Name: "path", Strings: []string{"source"}}}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	base, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: "embedding_graph"})
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	limits := typedGraphOverlayLimits{Rows: 3, Tombstones: 1, Bytes: 1 << 20}
	var pinned *CollectionReadView
	for version := 1; version <= 2; version++ {
		columns[0].Float32Vectors[0] = make([]float32, 8)
		columns[0].Float32Vectors[0][version] = 1
		columns[1].Strings[0] = []string{"", "first", "second"}[version]
		if _, err := col.ReplaceTypedBatch(ids, retained, columns); err != nil {
			t.Fatal(err)
		}
		current, err := col.OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
		defer current.Close()
		if version == 1 {
			pinned = current
		}
		suffix, err := prepareTypedGraphOverlaySuffix(base, current, limits)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := suffix.prepareRows(current, limits.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		if suffix.rows != version || len(rows) != 1 || rows[0].Values[0].Float32Vector[version] != 1 || rows[0].Values[1].String != columns[1].Strings[0] {
			t.Fatalf("version %d suffix=%d rows=%+v", version, suffix.rows, rows)
		}
	}
	// A later deletion must not alter the logical query view already pinned.
	if err := col.Delete(ids[0]); err != nil {
		t.Fatal(err)
	}
	oldSuffix, err := prepareTypedGraphOverlaySuffix(base, pinned, limits)
	if err != nil {
		t.Fatal(err)
	}
	oldRows, err := oldSuffix.prepareRows(pinned, limits.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(oldRows) != 1 || oldRows[0].Values[0].Float32Vector[1] != 1 || oldRows[0].Values[1].String != "first" {
		t.Fatalf("pinned authority changed: %+v", oldRows)
	}
	current, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	suffix, err := prepareTypedGraphOverlaySuffix(base, current, limits)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := suffix.prepareRows(current, limits.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if suffix.rows != 3 || suffix.tombstones != 1 || len(rows) != 1 || !rows[0].Deleted {
		t.Fatalf("cumulative deletion lost: suffix=%+v rows=%+v", suffix, rows)
	}
	limits.Rows = 2
	if _, err := prepareTypedGraphOverlaySuffix(base, current, limits); !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
		t.Fatalf("surviving row hid cumulative work: %v", err)
	}
	// This profile does not support index drop/recreate: preserve its existing
	// root-publication barrier rather than claiming a new lifecycle guarantee.
	def := base.reader.def
	if _, err := col.DropVectorIndex(def.Name); !errors.Is(err, backenddb.ErrCommandWALUnsupported) {
		t.Fatalf("index drop crossed existing command-WAL lifecycle barrier: %v", err)
	}
	unchanged, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer unchanged.Close()
	limits.Rows = 3
	if _, err := prepareTypedGraphOverlaySuffix(base, unchanged, limits); err != nil {
		t.Fatalf("rejected drop changed the pinned graph lineage: %v", err)
	}
}
