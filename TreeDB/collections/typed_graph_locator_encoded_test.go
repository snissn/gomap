package collections

import (
	"strings"
	"testing"
)

func TestTypedGraphGeneratedLocatorEncodedBound(t *testing.T) {
	meta := typedMinimaCollectionMeta()
	cfg, err := normalizeColumnStoreConfig(meta.Name, meta.Options.ColumnStore)
	if err != nil {
		t.Fatal(err)
	}
	meta.Options.ColumnStore = cfg
	values := []columnDeclaredValue{
		{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: []float32{1, 0, 0, 0, 0, 0, 0, 0}},
		{Type: ColumnStoreValueString, Present: true, String: "content"},
		{Type: ColumnStoreValueString, Present: true, String: "user"},
		{Type: ColumnStoreValueString, Present: true, String: "path"},
	}
	for _, operation := range []ColumnPublishOperation{ColumnPublishOperationInsert, ColumnPublishOperationUpdate, ColumnPublishOperationDelete} {
		t.Run(string(operation), func(t *testing.T) {
			input := columnWritePublishInput{meta: meta, operation: operation, documents: []columnWriteDocument{{ID: []byte("x"), declaredValues: values, declaredValuesReady: true}}}
			short, err := typedGraphColumnEncodedBound(input)
			if err != nil {
				t.Fatal(err)
			}
			input.documents[0].ID = []byte(strings.Repeat("x", 513))
			long, err := typedGraphColumnEncodedBound(input)
			if err != nil {
				t.Fatal(err)
			}
			// The ID occurs in both the row image and generated locator key.
			// No codec or value content changes between these inputs.
			if got, want := long-short, int64(2*512); got != want {
				t.Fatalf("ID growth charged %d bytes, want %d for row plus locator", got, want)
			}
			// Source removal reserves its row image and the actual locator
			// tombstone, even when the inserted ID overlaps the removed ID.
			input.sourceDeleteDocuments = []columnWriteDocument{{ID: input.documents[0].ID}}
			mixed, err := typedGraphColumnEncodedBound(input)
			if err != nil {
				t.Fatal(err)
			}
			row, err := columnPhysicalAssetEncodedUpperBound(columnPhysicalAssetEncodeInput{Collection: meta.Name, Namespace: cfg.AssetManager.Namespace, Operation: ColumnPublishOperationDelete, Columns: columnStoreRowAssetColumns(*cfg), Rows: []columnDeclaredRow{{ID: input.documents[0].ID, Deleted: true}}})
			if err != nil {
				t.Fatal(err)
			}
			table, err := buildColumnPrimaryRowLocatorTable(ColumnPublishPlan{Rows: 1, Operation: ColumnPublishOperationDelete}, input.sourceDeleteDocuments)
			if err != nil {
				t.Fatal(err)
			}
			locator, err := typedGraphTableEncodedBound(table)
			if err != nil || mixed-long != row+locator {
				t.Fatalf("source removal charged %d, row=%d locator=%d err=%v", mixed-long, row, locator, err)
			}
		})
	}
	if got, err := typedGraphColumnEncodedBound(columnWritePublishInput{meta: meta}); err != nil || got != 0 {
		t.Fatalf("empty bound=%d err=%v", got, err)
	}
	state := &typedGraphPublicationState{catalog: &collectionCatalog{meta: meta}, limits: typedGraphPublicationLimits{EncodedOutputBytes: 1 << 20}}
	if err := state.prepareEncodedBounds(); err != nil {
		t.Fatal(err)
	}
	input := columnWritePublishInput{meta: meta, operation: ColumnPublishOperationInsert, documents: []columnWriteDocument{{ID: []byte("x"), declaredValues: values, declaredValuesReady: true}}}
	baseAllocs := testing.AllocsPerRun(10, func() {
		if _, err := typedGraphColumnEncodedBound(input); err != nil {
			t.Fatal(err)
		}
	})
	fullAllocs := testing.AllocsPerRun(10, func() {
		if _, err := typedGraphWriteEncodedBound(input, state); err != nil {
			t.Fatal(err)
		}
	})
	if fullAllocs != baseAllocs {
		t.Fatalf("generated metadata adds per-write allocations: assets=%v complete=%v", baseAllocs, fullAllocs)
	}
	if got, err := typedGraphWriteEncodedBound(columnWritePublishInput{meta: meta}, state); err != nil || got != 0 {
		t.Fatalf("empty write bound=%d err=%v", got, err)
	}
}
