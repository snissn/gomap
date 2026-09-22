package docs_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestDocs_TypedColumnOptimizedConsumerCapabilityMatrix(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	matrixPath := filepath.Join(treeRoot, "docs", "spec", "typed-column-optimized-consumer-capabilities.md")
	data, err := os.ReadFile(matrixPath)
	if err != nil {
		t.Fatalf("read capability matrix: %v", err)
	}
	text := string(data)
	normalized := strings.Join(strings.Fields(text), " ")

	for _, tier := range []string{"mmap_direct", "heap_typed_view", "scratch_decode", "predicate_only", "generic_only", "unsupported/experimental"} {
		if !strings.Contains(text, "`"+tier+"`") {
			t.Fatalf("capability matrix missing tier %q", tier)
		}
	}
	for _, want := range []string{
		"Graph-search healthy-path use requires `mmap_direct`",
		"#2044",
		"#2046",
		"Value-log pointers must not be described as transient or WAL-like storage",
		"Adding a new collection logical value type, `typedcolumn.ColumnType`, or `typedcolumn.Encoding` must update this document",
	} {
		if !strings.Contains(normalized, strings.Join(strings.Fields(want), " ")) {
			t.Fatalf("capability matrix missing required contract text %q", want)
		}
	}

	requiredRows := []struct {
		logical      string
		physical     string
		encoding     string
		tier         string
		graphLinked  bool
		directLinked bool
	}{
		{logical: "bool", physical: "bool", encoding: "bool_bitpack_rle", tier: "predicate_only"},
		{logical: "int64", physical: "int64", encoding: "delta_varint", tier: "predicate_only"},
		{logical: "int64", physical: "int64", encoding: "double_delta_varint", tier: "generic_only"},
		{logical: "int64", physical: "int64", encoding: "raw_int64", tier: "mmap_direct", graphLinked: true, directLinked: true},
		{logical: "nullable scalar wrappers", physical: "int64", encoding: "nullable_int64", tier: "predicate_only"},
		{logical: "float32", physical: "int64", encoding: "raw_int64", tier: "generic_only"},
		{logical: "double", physical: "int64", encoding: "raw_int64", tier: "generic_only"},
		{logical: "float32", physical: "float32", encoding: "raw_float32", tier: "mmap_direct", graphLinked: true, directLinked: true},
		{logical: "double", physical: "float64", encoding: "raw_float64", tier: "mmap_direct"},
		{logical: "string", physical: "low_cardinality_code", encoding: "low_cardinality_uint32", tier: "predicate_only"},
		{logical: "float32_vector", physical: "float32_vector", encoding: "raw_float32_vector", tier: "mmap_direct", graphLinked: true, directLinked: true},
		{logical: "uint32_list", physical: "uint32_list", encoding: "raw_uint32_offsets_list", tier: "mmap_direct", graphLinked: true, directLinked: true},
		{logical: "bytes", physical: "bytes", encoding: "raw_bytes_offsets", tier: "mmap_direct", graphLinked: true, directLinked: true},
		{logical: "adjacency_list", physical: "adjacency_list", encoding: "raw_uint32_dense", tier: "scratch_decode"},
		{logical: "adjacency_list", physical: "adjacency_list", encoding: "raw_uint32_offsets_list", tier: "unsupported/experimental", graphLinked: true},
	}
	for _, row := range requiredRows {
		line := findCapabilityMatrixRow(text, row.logical, row.physical, row.encoding, row.tier)
		if line == "" {
			t.Fatalf("capability matrix missing row logical=%s physical=%s encoding=%s tier=%s", row.logical, row.physical, row.encoding, row.tier)
		}
		if row.graphLinked && !strings.Contains(line, "#2044") {
			t.Fatalf("graph-search-relevant row lacks #2044 admission link: %s", line)
		}
		if row.directLinked && !strings.Contains(line, "#2046") {
			t.Fatalf("direct-capable row lacks #2046 certifier link: %s", line)
		}
	}

	collectionValueConstPrefix := "Column" + "StoreValue"
	collectionValueTypeName := collectionValueConstPrefix + "Type"
	for _, valueType := range extractQuotedConstValues(t, filepath.Join(treeRoot, "collections", "column_store.go"), collectionValueConstPrefix+`[A-Za-z0-9_]+\s+`+collectionValueTypeName+`\s*=\s*"([^"]+)"`) {
		if !strings.Contains(text, "`"+valueType+"`") {
			t.Fatalf("capability matrix missing collection logical value type %q", valueType)
		}
	}
	for _, columnType := range extractQuotedConstValues(t, filepath.Join(treeRoot, "internal", "typedcolumn", "part.go"), `ColumnType[A-Za-z0-9_]+\s+ColumnType\s*=\s*"([^"]+)"`) {
		if !strings.Contains(text, "`"+columnType+"`") {
			t.Fatalf("capability matrix missing typedcolumn.ColumnType %q", columnType)
		}
	}
	for _, encoding := range extractTypedColumnEncodingNames(t, filepath.Join(treeRoot, "internal", "typedcolumn", "granule.go")) {
		if !strings.Contains(text, "`"+encoding+"`") {
			t.Fatalf("capability matrix missing typedcolumn.Encoding %q", encoding)
		}
	}
}

func findCapabilityMatrixRow(doc, logical, physical, encoding, tier string) string {
	for _, line := range strings.Split(doc, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "|") || strings.Contains(line, "---") {
			continue
		}
		cells := markdownTableCells(line)
		if len(cells) < 4 {
			continue
		}
		if capabilityCellContains(cells[0], logical) && capabilityCellContains(cells[1], physical) && capabilityCellContains(cells[2], encoding) && cells[3] == "`"+tier+"`" {
			return line
		}
	}
	return ""
}

func capabilityCellContains(cell, token string) bool {
	return strings.Contains(cell, "`"+token+"`") || strings.Contains(cell, token)
}

func markdownTableCells(line string) []string {
	parts := strings.Split(line, "|")
	cells := make([]string, 0, len(parts))
	for _, part := range parts {
		cell := strings.TrimSpace(part)
		if cell == "" {
			continue
		}
		cells = append(cells, cell)
	}
	return cells
}

func extractQuotedConstValues(t *testing.T, path, expr string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	re := regexp.MustCompile(expr)
	matches := re.FindAllStringSubmatch(string(data), -1)
	if len(matches) == 0 {
		t.Fatalf("no constants matched %s in %s", expr, path)
	}
	out := make([]string, 0, len(matches))
	seen := make(map[string]bool)
	for _, match := range matches {
		if !seen[match[1]] {
			seen[match[1]] = true
			out = append(out, match[1])
		}
	}
	return out
}

func extractTypedColumnEncodingNames(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	text := strings.ReplaceAll(string(data), "\r\n", "\n")
	start := strings.Index(text, "func (e Encoding) String() string")
	if start < 0 {
		t.Fatalf("Encoding.String not found in %s", path)
	}
	body := text[start:]
	if end := strings.Index(body, "\n}\n\n"); end >= 0 {
		body = body[:end]
	}
	re := regexp.MustCompile(`return "([^"]+)"`)
	matches := re.FindAllStringSubmatch(body, -1)
	if len(matches) == 0 {
		t.Fatalf("no encoding strings found in %s", path)
	}
	out := make([]string, 0, len(matches))
	seen := make(map[string]bool)
	for _, match := range matches {
		if !seen[match[1]] {
			seen[match[1]] = true
			out = append(out, match[1])
		}
	}
	return out
}
