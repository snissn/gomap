package collections

import (
	"encoding/json"
	"reflect"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestTypedColumnAdjacencyOffsetsListReopen1915(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open DB: %v", err)
	}
	cfg := &ColumnStoreConfig{
		Enabled:         true,
		RetainedPayload: ColumnRetainedPayloadNonColumn,
		Reconstruction:  ColumnReconstructionRetainedPayloadAndColumns,
		Columns: []ColumnStoreColumn{{
			Name:            "neighbors",
			Path:            "neighbors",
			Owner:           TypedStorageOwnerColumnPart,
			ValueType:       ColumnStoreValueAdjacencyList,
			AdjacencyLayout: ColumnAdjacencyListLayoutUint32OffsetsList,
		}},
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "graphs", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("graphs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	ids := [][]byte{[]byte("r0"), []byte("r1"), []byte("r2"), []byte("r3")}
	docs := [][]byte{
		[]byte(`{"neighbors":[],"label":"zero"}`),
		[]byte(`{"neighbors":[7,8],"label":"two"}`),
		[]byte(`{"neighbors":[],"label":"empty"}`),
		[]byte(`{"neighbors":[9,10,11],"label":"three"}`),
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open reopened DB: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("graphs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	want := map[string][]any{
		"r0": []any{},
		"r1": []any{float64(7), float64(8)},
		"r2": []any{},
		"r3": []any{float64(9), float64(10), float64(11)},
	}
	for _, id := range ids {
		gotRaw, err := reopenedCol.Get(id)
		if err != nil {
			t.Fatalf("Get %s: %v", id, err)
		}
		var got map[string]any
		if err := json.Unmarshal(gotRaw, &got); err != nil {
			t.Fatalf("json %s: %v raw=%s", id, err, gotRaw)
		}
		neighbors, ok := got["neighbors"].([]any)
		if !ok {
			t.Fatalf("neighbors for %s = %#v raw=%s", id, got["neighbors"], gotRaw)
		}
		if !reflect.DeepEqual(neighbors, want[string(id)]) {
			t.Fatalf("neighbors for %s = %#v want %#v raw=%s", id, neighbors, want[string(id)], gotRaw)
		}
	}
}
