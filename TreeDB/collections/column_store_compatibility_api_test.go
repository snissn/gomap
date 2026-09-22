package collections_test

import (
	"encoding/json"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestColumnStoreCompatibilityAPINamesStillCompile(t *testing.T) {
	var valueType collections.ColumnStoreValueType = collections.ColumnStoreValueString
	cfg := &collections.ColumnStoreConfig{
		Enabled: true,
		Columns: []collections.ColumnStoreColumn{{
			Name:      "kind",
			Path:      "kind",
			ValueType: valueType,
		}},
	}
	options := collections.CollectionOptions{ColumnStore: cfg}
	encoded, err := json.Marshal(options)
	if err != nil {
		t.Fatalf("marshal public compatibility options: %v", err)
	}
	if !json.Valid(encoded) {
		t.Fatalf("marshal produced invalid JSON: %q", encoded)
	}
	var decoded collections.CollectionOptions
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal public compatibility options: %v", err)
	}
	if decoded.ColumnStore == nil || len(decoded.ColumnStore.Columns) != 1 {
		t.Fatalf("decoded public compatibility options lost declared column: %+v", decoded)
	}
	got := decoded.ColumnStore.Columns[0]
	if got.Name != "kind" || got.Path != "kind" || got.ValueType != valueType {
		t.Fatalf("decoded public compatibility column = %+v, want name/path kind and value type %q", got, valueType)
	}
}
