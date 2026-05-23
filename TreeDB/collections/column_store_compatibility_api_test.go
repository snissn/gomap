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
	if options.ColumnStore == nil || len(options.ColumnStore.Columns) != 1 {
		t.Fatalf("public compatibility options lost declared column: %+v", options)
	}
	if !json.Valid(encoded) {
		t.Fatalf("marshal produced invalid JSON: %q", encoded)
	}
}
