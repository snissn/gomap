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
		"Status: #1989 quarantine/removal contract for the #1982 typed-column integer-list stack.",
		"New `column_graph` rebuilds publish HNSW adjacency as vector-index state `uint32_list` typed-column assets",
		"## Target model",
		"ClickHouse `Array(T)` separation",
		"`raw_uint32_offsets_list` is a physical encoding for generic `uint32_list`",
		"## Inventory buckets",
		"### Keep as low-level mechanics",
		"### Generalized primary path",
		"### Quarantine/remove as graph-specific storage architecture",
		"### Current #1989 behavior",
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
		"Do not publish graph-specific adjacency-source assets from primary rebuild",
		"Do reuse the offsets/value split, validation, alignment, mappedresource",
		"Do keep graph-specific validation",
		"Do fail closed or ask callers to rebuild pre-alpha assets",
	)
}

func TestColumnGraphAdjacencySourceQuarantineMarkers(t *testing.T) {
	markers := map[string][]string{
		"TreeDB/collections/column_vector_graph_adjacency_source.go": {
			"Quarantine: graph-specific adjacency-source storage is legacy compatibility",
			"New graph builds must publish vector-index state uint32_list assets instead",
		},
		"TreeDB/collections/column_vector_graph_adjacency_direct_source.go": {
			"Quarantine: graph-specific adjacency-source storage direct readers keep old",
			"New graph builds/search use\n// vector-index state uint32_list assets",
		},
		"TreeDB/collections/column_vector_graph_manifest.go": {
			"Quarantine: graph-specific adjacency-source storage refs embedded",
			"New graph builds leave\n// these fields empty",
		},
		"TreeDB/collections/column_store.go": {
			"must not become the target variable-list datastore primitive",
			"not the generic uint32_list API",
		},
		"TreeDB/collections/typed_column_adapter.go": {
			"quarantined adjacency_list selector",
			"generic uint32_list adapter path",
		},
		"TreeDB/internal/typeddecode/plan.go": {
			"quarantined\n// compatibility; generic uint32_list direct-view planning owns the reusable",
			"Graph-specific\n// naming is quarantined by #1989",
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
			"issue #1989 quarantine/removal contract",
			"vector-index state\n    `uint32_list` adjacency on the primary path",
		},
		"TreeDB/docs/spec/typed-column-direct-view-alignment.md": {
			"quarantined by #1989",
			"See `typed-column-uint32-list-adjacency-quarantine.md`",
			"primary path is the physical split offsets/value\nmechanics behind generic `uint32_list` vector-index state",
		},
		"TreeDB/docs/spec/typed-column-adapter.md": {
			"#1989 quarantines graph-specific storage integration",
			"primary list storage is generic `uint32_list`",
			"New\nstorage work must route through generic `uint32_list` typed-column assets and\nvector-index state",
		},
		"TreeDB/docs/spec/typed-column-layout-capabilities.md": {
			"#1989 quarantines that\n  graph-specific integration",
			"primary variable-list storage is generic\n  `uint32_list`",
		},
		"TreeDB/docs/spec/typed-column-semantics.md": {
			"#1989 quarantines that graph-specific logical integration",
			"first-class `uint32_list` semantics",
		},
		"TreeDB/docs/spec/storage-format.md": {
			"Issue #1989 quarantines that\nconsumer-specific selector",
			"New graph builds leave\nthese `TCGA`/`TCGL` fields empty",
			"Old adjacency-source refs are\n`#1989-quarantined` compatibility",
		},
		"TreeDB/docs/guides/vector-search-typed-column.md": {
			"typed-column `uint32_list` assets owned by vector-index state",
			"new graph builds should not publish those graph-specific source assets",
		},
	}

	for rel, want := range docs {
		t.Run(rel, func(t *testing.T) {
			text := readRepoText(t, rel)
			requireTextContains(t, rel, text, want...)
		})
	}
}
