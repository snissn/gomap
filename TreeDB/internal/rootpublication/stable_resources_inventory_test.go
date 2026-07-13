package rootpublication

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestStableResourceInventoryCoverage is intentionally executable: adding a
// new authoritative kind requires a complete reviewed row, and each current
// row must continue to name real producer, validator, and deleter sources.
func TestStableResourceInventoryCoverage(t *testing.T) {
	treedb := filepath.Clean(filepath.Join("..", ".."))
	type row struct {
		kind                         StableResourceKind
		producer, validator, deleter string
	}
	rows := []row{
		{StableResourceValueLog, "db/value_log_appender.go", "db/value_log_appender.go", "db/vlog_gc.go"},
		{StableResourceOuterLeaf, "db/leaf_page_log.go", "db/leaf_generation_manifest.go", "db/leaf_generation_gc.go"},
		{StableResourceDictionary, "collections/template_v1.go", "collections/template_v1.go", "db/vacuum_collection_roots.go"},
		{StableResourceTemplate, "collections/template_v1.go", "collections/template_v1.go", "db/vacuum_collection_roots.go"},
		{StableResourceColumn, "collections/column_store.go", "collections/column_store.go", "collections/column_store_compaction.go"},
		{StableResourceTypedColumn, "internal/typedcolumn/part.go", "internal/typedcolumn/part_image_decode.go", "collections/column_store_compaction.go"},
		{StableResourceVector, "collections/column_vector_graph_manifest.go", "collections/column_vector_graph_manifest.go", "collections/column_store_compaction.go"},
		{StableResourceText, "collections/text_v2_storage.go", "collections/text_v2_storage.go", "collections/text_v2_rewrite.go"},
		{StableResourceCommandWAL, "db/command_wal_raw.go", "db/command_wal_v2_recovery.go", "db/command_wal_raw.go"},
		{StableResourceQueryReady, "internal/typedcolumn/query_ready_delta.go", "internal/typedcolumn/query_ready_delta.go", "collections/column_store_compaction.go"},
	}
	for _, row := range rows {
		if row.kind == "" || row.producer == "" || row.validator == "" || row.deleter == "" {
			t.Fatalf("unknown inventory cell: %+v", row)
		}
		for _, path := range []string{row.producer, row.validator, row.deleter} {
			if _, err := os.Stat(filepath.Join(treedb, path)); err != nil {
				t.Fatalf("%s %q: %v", row.kind, path, err)
			}
		}
	}
	contents, err := os.ReadFile("STABLE_RESOURCE_INVENTORY.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if !strings.Contains(string(contents), string(row.kind)) {
			t.Fatalf("inventory document missing kind %q", row.kind)
		}
	}
}
