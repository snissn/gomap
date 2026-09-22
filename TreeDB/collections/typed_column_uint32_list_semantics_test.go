package collections

import "testing"

func TestUint32ListSemanticContractDoc(t *testing.T) {
	doc := readRepoText(t, "TreeDB/docs/spec/typed-column-uint32-list-semantics.md")

	requireTextContains(t, "uint32_list semantic contract", doc,
		"Status: semantic contract for the #1982 typed-column integer-list stack.",
		"`uint32_list` | Canonical TreeDB logical typed-column value type",
		"`uint32[]` | Conceptual user-facing alias for `uint32_list`",
		"`Array(UInt32)` | ClickHouse-style reference model",
		"`raw_uint32_offsets_list` | TreeDB v1 physical encoding for `uint32_list`; it is not a logical graph or adjacency type.",
		"`adjacency_list` | Legacy/consumer-specific compatibility type for current graph adjacency data; not the generic primitive.",
	)

	requireTextContains(t, "uint32_list semantic contract", doc,
		"Every row has exactly one list value. The v1 contract is non-null",
		"Empty lists are valid. They are represented by equal adjacent offsets",
		"The primitive does not assign graph meaning, sortedness, uniqueness, or row/ordinal validity to elements.",
		"v1 does not define `int32_list`, `uint64_list`, nested lists, shared-offset parallel arrays, compressed list sections, or nullable list sections.",
	)

	requireTextContains(t, "uint32_list semantic contract", doc,
		"The TreeDB v1 physical convention is explicit sentinel offsets",
		"offsets = []uint64, little-endian, length rows+1",
		"values = []uint32, little-endian, flattened row values",
		"row i = values[offsets[i]:offsets[i+1]]",
		"The offsets section and values section are separate typed-column image sections",
	)

	requireTextContains(t, "uint32_list semantic contract", doc,
		"The offsets section byte length is exactly `(rows+1)*8`.",
		"The values section byte length is a multiple of 4.",
		"`offsets[0] == 0`.",
		"Offsets are monotonic non-decreasing.",
		"Every offset, including the final offset, fits the host Go `int`.",
		"The final offset equals the flattened value count",
		"Full values reads must validate the values section identity, byte length, checksum/read-integrity policy, and little-endian `uint32` element decoding",
	)

	requireTextContains(t, "uint32_list semantic contract", doc,
		"The offsets substream is first-class metadata and can be validated/read without opening or decoding the flattened values bytes",
		"Offset-only validation can prove the row count, offset byte shape, `offsets[0]`, monotonicity, host-int bounds, row lengths, and the required final flattened value count.",
		"If trusted values-section metadata is available, an offset-only path may also check `offsets[rows] == values_section_bytes/4` without decoding values.",
		"It must not claim value element integrity, value checksum verification, or value direct view eligibility until the values substream is validated",
	)
}

func TestUint32ListCompatibilityNamingAndLegacyClassification(t *testing.T) {
	doc := readRepoText(t, "TreeDB/docs/spec/typed-column-uint32-list-semantics.md")
	nameDoc := readRepoText(t, "TreeDB/docs/spec/typed-storage-naming.md")
	adapterDoc := readRepoText(t, "TreeDB/docs/spec/typed-column-adapter.md")
	quarantineDoc := readRepoText(t, "TreeDB/docs/spec/typed-column-uint32-list-adjacency-quarantine.md")

	preferredSymbol := "Column" + "StoreValueUint32List"
	legacySymbol := "Column" + "StoreValueAdjacencyList"

	requireTextContains(t, "uint32_list semantic naming", doc,
		"The public compatibility name for the generic logical type is `"+preferredSymbol+"` with documented string `uint32_list`.",
		"Issue #1985 adds that Go constant, adapter admission, the code vocabulary row, and",
		"round-trip/direct-view/fallback validation without requiring\n`"+legacySymbol+"` semantics.",
		"`"+legacySymbol+"`, `adjacency_layout`, and existing `column_graph` adjacency-source schema strings remain legacy/consumer-specific compatibility.",
	)

	requireTextContains(t, "typed-storage naming", nameDoc,
		"## `uint32_list` Compatibility Naming Strategy (#1984)",
		"The preferred public compatibility symbol is `"+preferredSymbol+"` with\ndocumented string `uint32_list`.",
		"Issue #1985 adds the runtime\nwriter/reader/direct-view implementation and updates the code vocabulary table,",
		"`"+legacySymbol+"` remains the legacy graph-adjacency compatibility name.",
		"`adjacency_list` must remain classified as consumer-specific/legacy rather than a\nfirst-class datastore list type.",
	)

	requireTextContains(t, "typed-column adapter", adapterDoc,
		"## `uint32_list` adapter naming boundary (#1984)",
		"The preferred public compatibility\nconstant is `"+preferredSymbol+"` with string `uint32_list`; #1985 adds",
		"that code vocabulary, adapter mapping, conformance tests, writer/fallback\nreader/direct-view paths, and naming regression updates.",
		"The current `"+legacySymbol+"` and\n`adjacency_layout` selector remain legacy/consumer-specific compatibility, not\nthe generic primitive.",
	)

	requireTextContains(t, "adjacency quarantine doc", quarantineDoc,
		"validation invariants, length-only offsets behavior, and compatibility naming strategy are defined in `typed-column-uint32-list-semantics.md`.",
		"HNSW adjacency is a consumer that reads row `i` as `values[offsets[i]:offsets[i+1]]` from vector-index state.",
	)
}

func TestUint32ListSemanticContractLinkedFromCoreSpecs(t *testing.T) {
	docs := map[string][]string{
		"TreeDB/docs/spec/README.md": {
			"typed-column-uint32-list-semantics.md",
			"issue #1984 first-class `uint32_list` semantic contract",
			"`uint32[]` / conceptual `Array(UInt32)` aliases",
			"length-only offset-substream behavior",
		},
		"TreeDB/docs/spec/typed-column-semantics.md": {
			"## First-class `uint32_list` semantic contract (#1984)",
			"`TreeDB/docs/spec/typed-column-uint32-list-semantics.md` is the canonical semantic contract",
			"Offset/length metadata may be validated independently for length-only APIs.",
			"HNSW adjacency is a consumer above this primitive",
		},
		"TreeDB/docs/spec/typed-column-layout-capabilities.md": {
			"## Target `uint32_list` layout contract (#1984)",
			"separate declared-column offsets and values sections",
			"Length-only APIs may certify the offsets substream independently from values bytes.",
			"Graph traversal capability is not a layout capability of `uint32_list`",
		},
		"TreeDB/docs/spec/typed-column-direct-view-alignment.md": {
			"See `typed-column-uint32-list-semantics.md` for the #1984 logical `uint32_list` semantics",
			"length-only offsets behavior",
		},
		"TreeDB/docs/spec/storage-format.md": {
			"#1984 defines `uint32_list` semantics in `typed-column-uint32-list-semantics.md`",
			"values []uint32 // flattened uint32 values, little-endian",
		},
	}

	for rel, want := range docs {
		t.Run(rel, func(t *testing.T) {
			text := readRepoText(t, rel)
			requireTextContains(t, rel, text, want...)
		})
	}
}
