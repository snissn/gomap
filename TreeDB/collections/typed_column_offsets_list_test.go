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
	t.Cleanup(func() {
		if d != nil {
			_ = d.Close()
		}
	})
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
	ids := [][]byte{[]byte("r0"), []byte("r1"), []byte("r2"), []byte("r3"), []byte("r4")}
	docs := [][]byte{
		[]byte(`{"neighbors":[],"label":"zero"}`),
		[]byte(`{"neighbors":[7,8],"label":"two"}`),
		[]byte(`{"neighbors":[],"label":"empty"}`),
		[]byte(`{"neighbors":[9,10,11],"label":"three"}`),
		[]byte(`{"neighbors":[42],"label":"one"}`),
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
	d = nil

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
		"r4": []any{float64(42)},
	}
	for _, id := range ids {
		assertTypedColumnOffsetsListNeighbors(t, reopenedCol, id, want[string(id)])
	}
}

func TestTypedColumnUint32ListReopen1985(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open DB: %v", err)
	}
	t.Cleanup(func() {
		if d != nil {
			_ = d.Close()
		}
	})
	cfg := &ColumnStoreConfig{
		Enabled:         true,
		RetainedPayload: ColumnRetainedPayloadNonColumn,
		Reconstruction:  ColumnReconstructionRetainedPayloadAndColumns,
		Columns: []ColumnStoreColumn{{
			Name:      "tags",
			Path:      "tags",
			Owner:     TypedStorageOwnerColumnPart,
			ValueType: ColumnStoreValueUint32List,
		}},
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "lists", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("lists")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	ids := [][]byte{[]byte("r0"), []byte("r1"), []byte("r2"), []byte("r3")}
	docs := [][]byte{
		[]byte(`{"tags":[],"label":"zero"}`),
		[]byte(`{"tags":[7,8],"label":"two"}`),
		[]byte(`{"tags":[],"label":"empty"}`),
		[]byte(`{"tags":[9,10,11],"label":"three"}`),
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
	d = nil

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open reopened DB: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("lists")
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
		assertTypedColumnOffsetsListField(t, reopenedCol, id, "tags", want[string(id)])
	}
}

func TestTypedColumnAdjacencyOffsetsListUpdateDeleteVisibility1917(t *testing.T) {
	d, col := newTypedColumnAdjacencyOffsetsListTestCollection(t)
	defer func() { _ = d.Close() }()
	ids := [][]byte{[]byte("r0"), []byte("r1"), []byte("r2")}
	docs := [][]byte{
		[]byte(`{"neighbors":[],"label":"zero"}`),
		[]byte(`{"neighbors":[1],"label":"one"}`),
		[]byte(`{"neighbors":[2,3],"label":"two"}`),
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	matched, modified, err := col.Update([]byte("r1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"neighbors":[7,8,9],"label":"updated"}`), true, nil
	})
	if err != nil || !matched || !modified {
		t.Fatalf("Update matched=%v modified=%v err=%v", matched, modified, err)
	}
	if err := col.Delete([]byte("r2")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	assertTypedColumnOffsetsListNeighbors(t, col, []byte("r0"), []any{})
	assertTypedColumnOffsetsListNeighbors(t, col, []byte("r1"), []any{float64(7), float64(8), float64(9)})
	if got, err := col.Get([]byte("r2")); err != nil || got != nil {
		t.Fatalf("deleted r2 Get=%s err=%v want missing", got, err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	assertTypedColumnOffsetsListNeighbors(t, col, []byte("r0"), []any{})
	assertTypedColumnOffsetsListNeighbors(t, col, []byte("r1"), []any{float64(7), float64(8), float64(9)})
	if got, err := col.Get([]byte("r2")); err != nil || got != nil {
		t.Fatalf("post-checkpoint deleted r2 Get=%s err=%v want missing", got, err)
	}
}

func newTypedColumnAdjacencyOffsetsListTestCollection(t *testing.T) (*backenddb.DB, *Collection) {
	t.Helper()
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
		_ = d.Close()
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("graphs")
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection: %v", err)
	}
	return d, col
}

func assertTypedColumnOffsetsListNeighbors(t *testing.T, col *Collection, id []byte, want []any) {
	t.Helper()
	assertTypedColumnOffsetsListField(t, col, id, "neighbors", want)
}

func assertTypedColumnOffsetsListField(t *testing.T, col *Collection, id []byte, field string, want []any) {
	t.Helper()
	gotRaw, err := col.Get(id)
	if err != nil {
		t.Fatalf("Get %s: %v", id, err)
	}
	var got map[string]any
	if err := json.Unmarshal(gotRaw, &got); err != nil {
		t.Fatalf("json %s: %v raw=%s", id, err, gotRaw)
	}
	values, ok := got[field].([]any)
	if !ok {
		t.Fatalf("%s for %s = %#v raw=%s", field, id, got[field], gotRaw)
	}
	if !reflect.DeepEqual(values, want) {
		t.Fatalf("%s for %s = %#v want %#v raw=%s", field, id, values, want, gotRaw)
	}
}
