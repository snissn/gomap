package collections

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func readRepoText(t *testing.T, rel string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(typedStorageRepoRoot(t), filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return string(data)
}

func requireTextContains(t *testing.T, label, text string, needles ...string) {
	t.Helper()
	foldedText := strings.Join(strings.Fields(text), " ")
	for _, needle := range needles {
		foldedNeedle := strings.Join(strings.Fields(needle), " ")
		if !strings.Contains(foldedText, foldedNeedle) {
			t.Fatalf("%s missing %q", label, needle)
		}
	}
}

func TestColumnGraphAdjacencySourceQuarantineAuditDoc(t *testing.T) {
	doc := readRepoText(t, "TreeDB/docs/spec/typed-column-uint32-list-adjacency-quarantine.md")

	requireTextContains(t, "adjacency quarantine audit doc", doc,
		"Status: audit/quarantine contract for the #1982 typed-column integer-list stack.",
		"## Target model",
		"ClickHouse `Array(T)` separation",
		"`raw_uint32_offsets_list` is a physical encoding for generic `uint32_list`",
		"## Inventory buckets",
		"### Keep as low-level mechanics",
		"### Generalize/rename in child issues",
		"### Quarantine/remove as graph-specific storage architecture",
		"### Current transitional behavior",
		"## Remediation ownership",
		"## Guardrails for new code",
	)

	requireTextContains(t, "adjacency quarantine audit doc", doc,
		"`TreeDB/internal/typedcolumn/raw_uint32_offsets_list.go`",
		"`TreeDB/internal/typeddecode/plan.go`",
		"`TreeDB/collections/column_vector_graph_adjacency_source.go`",
		"`TreeDB/collections/column_vector_graph_adjacency_direct_source.go`",
		"`TreeDB/collections/column_vector_graph_manifest.go`",
		"`column_graph_layer0_adjacency/raw_uint32_offsets_list/v1`",
		"`column_graph_adjacency_layer/raw_uint32_offsets_list/v1`",
		"`adjacency_layout`",
		"`uint32_list` / `uint32[]` / conceptual `Array(UInt32)`",
		"`offsets []uint64`, little-endian, length `rows+1`, `offsets[0] == 0`",
		"`values []uint32`, little-endian, length `offsets[rows]`",
	)

	requireTextContains(t, "adjacency quarantine audit doc", doc,
		"#1984", "#1985", "#1986", "#1987", "#1988", "#1989", "#1990", "#1992",
		"Do not add new storage features to `"+"Column"+"StoreValueAdjacencyList`",
		"Do reuse the offsets/value split, validation, alignment, mappedresource",
		"Do keep graph-specific validation",
		"Do fail closed or ask callers to rebuild pre-alpha assets",
	)
}

func TestColumnGraphAdjacencySourceQuarantineMarkers(t *testing.T) {
	markers := map[string][]string{
		"TreeDB/collections/column_vector_graph_adjacency_source.go": {
			"Quarantine: graph-specific adjacency-source storage is transitional",
			"#1985 should salvage the raw_uint32_offsets_list",
		},
		"TreeDB/collections/column_vector_graph_adjacency_direct_source.go": {
			"Quarantine: graph-specific adjacency-source storage direct readers",
			"not the target\n// uint32_list datastore primitive",
		},
		"TreeDB/collections/column_vector_graph_manifest.go": {
			"Quarantine: graph-specific adjacency-source storage refs embedded",
			"#1986 owns moving\n// vector-index state refs",
		},
		"TreeDB/collections/column_store.go": {
			"must not become the target variable-list datastore primitive",
			"future generic uint32_list API",
		},
		"TreeDB/collections/typed_column_adapter.go": {
			"quarantined adjacency_list selector",
			"generic uint32_list adapter path",
		},
		"TreeDB/internal/typeddecode/plan.go": {
			"quarantined compatibility; #1985 owns the generic uint32_list direct-view plan",
			"Graph-specific\n// naming is quarantined by #1983",
		},
		"TreeDB/internal/typedcolumn/raw_uint32_offsets_list.go": {
			"consumer-neutral storage\n// machinery",
			"graph-specific semantics belong\n// above this layer",
		},
	}

	for rel, want := range markers {
		t.Run(rel, func(t *testing.T) {
			text := readRepoText(t, rel)
			requireTextContains(t, rel, text, want...)
		})
	}
}

func TestColumnGraphAdjacencySourceDocsPointToQuarantine(t *testing.T) {
	docs := map[string][]string{
		"TreeDB/docs/spec/README.md": {
			"typed-column-uint32-list-adjacency-quarantine.md",
			"separating reusable\n    `raw_uint32_offsets_list` mechanics from graph-specific adjacency-source",
		},
		"TreeDB/docs/spec/typed-column-direct-view-alignment.md": {
			"quarantined by #1983",
			"See `typed-column-uint32-list-adjacency-quarantine.md`",
			"future generic `uint32_list` primitive",
		},
		"TreeDB/docs/spec/typed-column-adapter.md": {
			"#1983 quarantines the graph-specific storage integration",
			"generic `uint32_list` primitive",
			"New storage work must route through\ngeneric `uint32_list` typed-column assets and vector-index state",
		},
		"TreeDB/docs/spec/typed-column-layout-capabilities.md": {
			"#1983 quarantines\n  that graph-specific integration",
			"generic\n  `uint32_list` primitive",
		},
		"TreeDB/docs/spec/typed-column-semantics.md": {
			"#1983 quarantines that graph-specific logical integration",
			"first-class `uint32_list` semantics",
		},
		"TreeDB/docs/spec/storage-format.md": {
			"Issue #1983 quarantines the\nconsumer-specific storage integration",
			"Do not add new storage\nfeatures to this `TCGA`/`TCGL` path",
			"Those adjacency-source refs are current #1983-quarantined\ncompatibility",
		},
		"TreeDB/docs/guides/vector-search-typed-column.md": {
			"current quarantined `column_graph` adjacency direct sources",
			"generic `uint32_list` assets owned by vector-index state",
		},
	}

	for rel, want := range docs {
		t.Run(rel, func(t *testing.T) {
			text := readRepoText(t, rel)
			requireTextContains(t, rel, text, want...)
		})
	}
}
