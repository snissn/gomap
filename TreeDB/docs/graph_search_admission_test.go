package docs_test

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

func TestDocs_GraphSearchTypedColumnAdmissionGate(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	path := filepath.Join(treeRoot, "docs", "spec", "typed-column-graph-search-admission.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read graph-search admission spec: %v", err)
	}
	doc := string(data)
	normalized := strings.Join(strings.Fields(doc), " ")

	for _, want := range []string{
		"Every healthy current-format graph-search role must have exactly one row",
		"Current hot/state roles require #2047 `mmap_direct`",
		"A weaker tier such as `heap_typed_view` may be admitted only by changing this table",
		"Rows marked `deferred`, `experimental`, or `fallback-only` are unadmitted for healthy search",
		"Healthy evidence must show no silent graph-row fallback",
		"Evidence required to promote a row to `admitted`",
		"`ns/op`, `ops/sec`, `B/op`, `allocs/op`",
		"#2043 final type readiness summary",
		"not an unconditional claim that #2035 beat the legacy control",
		"not apples-to-apples storage-path evidence",
		"prepared CSR adjacency access is zero-allocation",
	} {
		if !strings.Contains(normalized, strings.Join(strings.Fields(want), " ")) {
			t.Fatalf("graph-search admission spec missing required policy text %q", want)
		}
	}
	for _, status := range []string{"pending", "admitted", "deferred", "experimental", "fallback-only"} {
		if !strings.Contains(doc, "`"+status+"`") {
			t.Fatalf("graph-search admission spec missing status %q", status)
		}
	}

	rows := graphSearchAdmissionRows(doc)
	required := []struct {
		role      string
		owner     string
		logical   string
		encoding  string
		tier      string
		status    string
		shape     string
		boundary  string
		fallback  string
		counter   string
		test      string
		benchmark string
	}{
		{role: "Base vectors", owner: "typed_column_part", logical: "float32_vector", encoding: "raw_float32_vector", tier: "mmap_direct", status: "admitted", shape: "[]float32", boundary: "Candidate scoring", fallback: "MUST NOT fetch candidate vectors", counter: "vector_prepared_direct/search", test: "dimension mismatch", benchmark: "BenchmarkColumnVectorGraphNativeSearchCosineV3"},
		{role: "HNSW adjacency", owner: "adjacency", logical: "uint32_list", encoding: "raw_uint32_offsets_list", tier: "mmap_direct", status: "admitted", shape: "CSR", boundary: "Edge traversal", fallback: "MUST NOT read legacy graph-row adjacency", counter: "adjacency_prepared_csr_mmap_direct/search", test: "missing layer", benchmark: "edges/search"},
		{role: "Inverse norms", owner: "inverse_norm", logical: "float32", encoding: "raw_float32", tier: "mmap_direct", status: "admitted", shape: "[]float32", boundary: "Candidate scoring", fallback: "MUST NOT read inverse norms from legacy graph rows", counter: "norm_prepared_direct/search", test: "finite-value", benchmark: "graph-only search"},
		{role: "Row refs", owner: "row_refs", logical: "int64", encoding: "raw_int64", tier: "mmap_direct", status: "admitted", shape: "row-ref arrays", boundary: "Result and document-fetch", fallback: "MUST NOT scan legacy graph row IDs", counter: "row_ref_state_prepared_views/search", test: "coordinate bounds", benchmark: "doc-fetch timing"},
		{role: "Document IDs", owner: "document_ids", logical: "bytes", encoding: "raw_bytes_offsets", tier: "mmap_direct", status: "admitted", shape: "Offsets []uint64", boundary: "Final top-k", fallback: "MUST NOT return IDs from legacy graph rows", counter: "result_id_prepared_bytes_views/search", test: "arbitrary binary IDs", benchmark: "BenchmarkSearchVectorIndexColumnGraphNativeReaderWithDocumentsV4"},
		{role: "Optional normalized vectors", owner: "normalized_vectors", logical: "float32_vector", encoding: "raw_float32_vector", tier: "mmap_direct", status: "deferred", shape: "[]float32", boundary: "Not in the healthy", fallback: "MUST NOT silently replace base-vector evidence", counter: "normalized_vector_mmap_direct/search", test: "zero healthy fallback", benchmark: "memory residency"},
		{role: "Legacy graph-row vector, norm, row-ref, and result-ID payloads", owner: "graph-row", logical: "Legacy graph-row physical asset", encoding: "not typed-column", tier: "not a #2047 typed-column tier", status: "fallback-only", shape: "No prepared shape", boundary: "Legacy compatibility", fallback: "MUST NOT silently fall back", counter: "result_id_graph_fallbacks", test: "compatibility fixtures", benchmark: "labeled legacy"},
		{role: "Legacy graph-specific adjacency sources", owner: "TCGA", logical: "adjacency_list", encoding: "raw_uint32_offsets_list", tier: "unsupported/experimental", status: "fallback-only", shape: "No prepared shape", boundary: "Legacy compatibility", fallback: "MUST NOT read graph-specific adjacency-source payloads", counter: "adjacency_source_fallbacks/search", test: "quarantine tests", benchmark: "labeled legacy"},
		{role: "Legacy dense adjacency or row-image adjacency", owner: "row-image adjacency", logical: "adjacency_list", encoding: "raw_uint32_dense", tier: "scratch_decode", status: "fallback-only", shape: "No prepared shape", boundary: "Legacy compatibility", fallback: "MUST NOT use dense row-image adjacency", counter: "adjacency_legacy_fallbacks/search", test: "dense adjacency compatibility tests", benchmark: "diagnostic"},
	}
	for _, want := range required {
		cells := findGraphSearchAdmissionRow(t, rows, want.role)
		assertAdmissionCellContains(t, want.role, "owner", cells[1], want.owner)
		assertAdmissionCellContains(t, want.role, "format", cells[2], want.logical)
		assertAdmissionCellContains(t, want.role, "format", cells[2], want.encoding)
		assertAdmissionCellContains(t, want.role, "tier", cells[3], want.tier)
		assertAdmissionCellContains(t, want.role, "status", cells[4], want.status)
		assertAdmissionCellContains(t, want.role, "shape", cells[5], want.shape)
		assertAdmissionCellContains(t, want.role, "boundary", cells[6], want.boundary)
		assertAdmissionCellContains(t, want.role, "fallback", cells[7], want.fallback)
		assertAdmissionCellContains(t, want.role, "counters/tests", cells[8], want.counter)
		assertAdmissionCellContains(t, want.role, "counters/tests", cells[8], want.test)
		assertAdmissionCellContains(t, want.role, "benchmark/evidence", cells[9], want.benchmark)
		for _, requiredEvidence := range []string{"ns/op", "ops/sec", "B/op", "allocs/op"} {
			assertAdmissionCellContains(t, want.role, "benchmark/evidence", cells[9], requiredEvidence)
		}
	}
}

func TestDocs_GraphSearchAdmissionCoversVectorIndexStateRoles(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	admissionData, err := os.ReadFile(filepath.Join(treeRoot, "docs", "spec", "typed-column-graph-search-admission.md"))
	if err != nil {
		t.Fatalf("read graph-search admission spec: %v", err)
	}
	rows := graphSearchAdmissionRows(string(admissionData))

	sourcePath := filepath.Join(treeRoot, "collections", "column_vector_index_state_manifest.go")
	sourceData, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatalf("read vector-index state manifest source: %v", err)
	}
	source := string(sourceData)
	constValues := graphSearchAdmissionSourceConstants(source)
	roles := graphSearchAdmissionVectorIndexStateRoles(t, constValues)
	for _, role := range roles {
		findGraphSearchAdmissionRowByOwner(t, rows, role)
	}

	contracts := graphSearchAdmissionStateContracts(t, source, constValues)
	if len(contracts) == 0 {
		t.Fatalf("no vector-index state role contracts found in %s", sourcePath)
	}

	for _, contract := range contracts {
		cells := findGraphSearchAdmissionRowByOwner(t, rows, contract.role)
		assertAdmissionCellContains(t, contract.role, "format", cells[2], contract.logical)
		assertAdmissionCellContains(t, contract.role, "format", cells[2], contract.encoding)
		assertAdmissionCellContains(t, contract.role, "tier", cells[3], "mmap_direct")
		switch contract.role {
		case "normalized_vectors":
			if !graphSearchAdmissionCellHasStatus(cells[4], "deferred") && !graphSearchAdmissionCellHasStatus(cells[4], "experimental") {
				t.Fatalf("%s admission status=%q, want deferred or experimental", contract.role, cells[4])
			}
		case "hnsw_search_pack":
			if !graphSearchAdmissionCellHasStatus(cells[4], "admitted") {
				t.Fatalf("%s admission status=%q, want admitted existing eligible base-only route", contract.role, cells[4])
			}
			assertAdmissionCellContains(t, contract.role, "boundary", cells[6], "hnswSearchPackSearchWithBufferRoute")
			assertAdmissionCellContains(t, contract.role, "status", cells[4], "base-only")
		default:
			if !graphSearchAdmissionCellHasStatus(cells[4], "pending") && !graphSearchAdmissionCellHasStatus(cells[4], "admitted") {
				t.Fatalf("state role %q admission status=%q, want pending or admitted", contract.role, cells[4])
			}
		}
	}
}

func TestDocs_GraphSearchTypedMutationSeamRemainsInternal(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	data, err := os.ReadFile(filepath.Join(treeRoot, "docs", "spec", "typed-column-graph-search-admission.md"))
	if err != nil {
		t.Fatal(err)
	}
	cells := findGraphSearchAdmissionRow(t, graphSearchAdmissionRows(string(data)), "Typed mutation suffix")
	assertAdmissionCellContains(t, "Typed mutation suffix", "tier", cells[3], "heap_typed_view")
	assertAdmissionCellContains(t, "Typed mutation suffix", "status", cells[4], "experimental")
	assertAdmissionCellContains(t, "Typed mutation suffix", "status", cells[4], "internal-only")
	for _, name := range []string{"typed_graph_overlay.go", "typed_graph_base_filter.go", "typed_graph_filter_search.go"} {
		if _, err := os.Stat(filepath.Join(treeRoot, "collections", name)); err != nil {
			t.Fatal(err)
		}
	}
	for _, boundary := range []string{"no ordinary mutable graph dispatch", "incremental current-pin publication", "fold policy", "no public filtered route"} {
		if !strings.Contains(string(data), boundary) {
			t.Fatalf("missing internal admission boundary %q", boundary)
		}
	}
}

func TestDocs_GraphSearchAdmissionHealthyRowsRequireMmapDirect(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	data, err := os.ReadFile(filepath.Join(treeRoot, "docs", "spec", "typed-column-graph-search-admission.md"))
	if err != nil {
		t.Fatalf("read graph-search admission spec: %v", err)
	}
	rows := graphSearchAdmissionRows(string(data))
	healthyRoles := []string{"Base vectors", "HNSW adjacency", "Inverse norms", "Row refs", "Document IDs"}
	for _, role := range healthyRoles {
		cells := findGraphSearchAdmissionRow(t, rows, role)
		if got := graphSearchAdmissionNormalizeCell(cells[3]); got != "mmap_direct" {
			t.Fatalf("healthy graph-search row %q tier=%q, want mmap_direct unless explicitly admitted weaker tier", role, cells[3])
		}
		for _, idx := range []int{5, 6, 7, 8, 9} {
			cell := strings.TrimSpace(cells[idx])
			if cell == "" || strings.EqualFold(cell, "TBD") || strings.Contains(strings.ToLower(cell), "to be filled") {
				t.Fatalf("healthy graph-search row %q has incomplete admission field %d: %q", role, idx, cell)
			}
		}
	}

	for _, cells := range rows {
		status := cells[4]
		if !graphSearchAdmissionCellHasStatus(status, "deferred") && !graphSearchAdmissionCellHasStatus(status, "experimental") && !graphSearchAdmissionCellHasStatus(status, "fallback-only") {
			continue
		}
		fallback := strings.ToLower(cells[7])
		if !strings.Contains(fallback, "must not") {
			t.Fatalf("unadmitted/fallback row %q lacks MUST NOT fallback guard: %s", cells[0], cells[7])
		}
		if !strings.Contains(fallback, "fail closed") && !strings.Contains(fallback, "compatibility") && !strings.Contains(fallback, "ignore") {
			t.Fatalf("unadmitted/fallback row %q lacks fail-closed or compatibility-only rule: %s", cells[0], cells[7])
		}
	}
}

func TestDocs_GraphSearchAdmissionLinkedFromOwners(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	links := map[string][]string{
		filepath.Join(treeRoot, "docs", "spec", "README.md"): {
			"typed-column-graph-search-admission.md",
		},
		filepath.Join(treeRoot, "docs", "spec", "typed-column-graph-search-prepared-views.md"): {
			"typed-column-graph-search-admission.md",
		},
		filepath.Join(treeRoot, "docs", "spec", "typed-column-optimized-consumer-capabilities.md"): {
			"typed-column-graph-search-admission.md",
		},
		filepath.Join(treeRoot, "docs", "spec", "vector-index-state-manifest.md"): {
			"typed-column-graph-search-admission.md",
		},
		filepath.Join(treeRoot, "docs", "guides", "vector-search-typed-column.md"): {
			"typed-column-graph-search-admission.md",
		},
		filepath.Join(treeRoot, "docs", "spec", "verification.md"): {
			"typed-column-graph-search-admission.md",
			"TestDocs_GraphSearchTypedColumnAdmissionGate",
		},
	}
	for path, wants := range links {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		text := string(data)
		for _, want := range wants {
			if !strings.Contains(text, want) {
				t.Fatalf("%s missing graph-search admission link %q", path, want)
			}
		}
	}
}

type graphSearchAdmissionStateContract struct {
	role     string
	logical  string
	encoding string
}

func graphSearchAdmissionRows(doc string) [][]string {
	var rows [][]string
	for _, line := range strings.Split(doc, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "|") || strings.Contains(line, "---") {
			continue
		}
		cells := markdownTableCells(line)
		if len(cells) < 10 || cells[0] == "Graph-search role" {
			continue
		}
		rows = append(rows, cells)
	}
	return rows
}

func findGraphSearchAdmissionRow(t *testing.T, rows [][]string, role string) []string {
	t.Helper()
	var matches [][]string
	for _, cells := range rows {
		if graphSearchAdmissionNormalizeCell(cells[0]) == graphSearchAdmissionNormalizeCell(role) {
			matches = append(matches, cells)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("graph-search admission table row %q matches=%d, want exactly one", role, len(matches))
	}
	return matches[0]
}

func findGraphSearchAdmissionRowByOwner(t *testing.T, rows [][]string, owner string) []string {
	t.Helper()
	roleNeedle := "role `" + owner + "`"
	var matches [][]string
	for _, cells := range rows {
		if strings.Contains(cells[1], roleNeedle) {
			matches = append(matches, cells)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("graph-search admission table owner role %q matches=%d, want exactly one", owner, len(matches))
	}
	return matches[0]
}

func assertAdmissionCellContains(t *testing.T, role, field, cell, want string) {
	t.Helper()
	if !strings.Contains(strings.ToLower(cell), strings.ToLower(want)) {
		t.Fatalf("graph-search admission row %q %s missing %q: %s", role, field, want, cell)
	}
}

func graphSearchAdmissionNormalizeCell(cell string) string {
	cell = strings.TrimSpace(cell)
	cell = strings.Trim(cell, "`")
	cell = strings.TrimSuffix(cell, ".")
	return strings.Join(strings.Fields(cell), " ")
}

func graphSearchAdmissionCellHasStatus(cell, status string) bool {
	status = strings.ToLower(status)
	for _, token := range graphSearchAdmissionStatusTokens(cell) {
		if token == status {
			return true
		}
	}
	return false
}

func graphSearchAdmissionStatusTokens(cell string) []string {
	cell = strings.ToLower(cell)
	backtick := regexp.MustCompile("`([^`]+)`")
	matches := backtick.FindAllStringSubmatch(cell, -1)
	if len(matches) > 0 {
		out := make([]string, 0, len(matches))
		for _, match := range matches {
			out = append(out, strings.TrimSpace(match[1]))
		}
		return out
	}
	fields := strings.FieldsFunc(cell, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-')
	})
	return fields
}

func graphSearchAdmissionVectorIndexStateRoles(t *testing.T, constValues map[string]string) []string {
	t.Helper()
	var roles []string
	seen := make(map[string]bool)
	for name, value := range constValues {
		if !strings.HasPrefix(name, "columnVectorIndexStateAssetRole") || seen[value] {
			continue
		}
		seen[value] = true
		roles = append(roles, value)
	}
	if len(roles) == 0 {
		t.Fatalf("no columnVectorIndexStateAssetRole constants found")
	}
	sort.Strings(roles)
	return roles
}

func graphSearchAdmissionSourceConstants(source string) map[string]string {
	re := regexp.MustCompile(`(?m)(columnVectorIndexState(?:AssetRole|LogicalType|Encoding)[A-Za-z0-9]+)\s*=\s*"([^"]+)"`)
	matches := re.FindAllStringSubmatch(source, -1)
	out := make(map[string]string, len(matches))
	for _, match := range matches {
		out[match[1]] = match[2]
	}
	return out
}

func graphSearchAdmissionStateContracts(t *testing.T, source string, constValues map[string]string) []graphSearchAdmissionStateContract {
	t.Helper()
	re := regexp.MustCompile(`(?s)case\s+(columnVectorIndexStateAssetRole[A-Za-z0-9]+):\s*return\s+(columnVectorIndexStateLogicalType[A-Za-z0-9]+),\s*(columnVectorIndexStateEncoding[A-Za-z0-9]+),\s*true`)
	matches := re.FindAllStringSubmatch(source, -1)
	out := make([]graphSearchAdmissionStateContract, 0, len(matches))
	seen := make(map[string]bool)
	for _, match := range matches {
		role, ok := constValues[match[1]]
		if !ok {
			t.Fatalf("missing string value for %s", match[1])
		}
		logical, ok := constValues[match[2]]
		if !ok {
			t.Fatalf("missing string value for %s", match[2])
		}
		encoding, ok := constValues[match[3]]
		if !ok {
			t.Fatalf("missing string value for %s", match[3])
		}
		if seen[role] {
			continue
		}
		seen[role] = true
		out = append(out, graphSearchAdmissionStateContract{role: role, logical: logical, encoding: encoding})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].role < out[j].role })
	return out
}
