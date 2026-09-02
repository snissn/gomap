package docs_test

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"
)

func repoRoots(t *testing.T) (treeRoot, repoRoot string) {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("runtime.Caller failed")
	}
	treeRoot = filepath.Clean(filepath.Join(filepath.Dir(thisFile), ".."))
	repoRoot = filepath.Clean(filepath.Join(treeRoot, ".."))
	return treeRoot, repoRoot
}

func markdownDocs(t *testing.T) []string {
	t.Helper()
	treeRoot, repoRoot := repoRoots(t)
	roots := []string{
		filepath.Join(treeRoot, "README.md"),
		filepath.Join(treeRoot, "AGENTS.md"),
		filepath.Join(treeRoot, "AUDIT_TRACKING.md"),
		filepath.Join(treeRoot, "docs", "spec"),
		filepath.Join(treeRoot, "docs", "guides"),
		filepath.Join(repoRoot, "docs"),
	}

	seen := make(map[string]bool)
	var paths []string
	for _, root := range roots {
		info, err := os.Stat(root)
		if err != nil {
			t.Fatalf("stat %s: %v", root, err)
		}
		if !info.IsDir() {
			if strings.HasSuffix(root, ".md") && !seen[root] {
				seen[root] = true
				paths = append(paths, root)
			}
			continue
		}
		err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == "benchmarks" {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(path, ".md") && !seen[path] {
				seen[path] = true
				paths = append(paths, path)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
	sort.Strings(paths)
	return paths
}

func TestDocs_NoTreeDBSlabTerminology(t *testing.T) {
	treeRoot, _ := repoRoots(t)

	paths := []string{
		filepath.Join(treeRoot, "README.md"),
		filepath.Join(treeRoot, "AGENTS.md"),
		filepath.Join(treeRoot, "AUDIT_TRACKING.md"),
	}
	specPaths, err := filepath.Glob(filepath.Join(treeRoot, "docs", "spec", "*.md"))
	if err != nil {
		t.Fatalf("glob spec docs: %v", err)
	}
	paths = append(paths, specPaths...)
	guidePaths, err := filepath.Glob(filepath.Join(treeRoot, "docs", "guides", "*.md"))
	if err != nil {
		t.Fatalf("glob guide docs: %v", err)
	}
	paths = append(paths, guidePaths...)
	allowedLegacyFields := regexp.MustCompile(`\b(activeslabid|activeslabtail)\b`)

	for _, p := range paths {
		content, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		text := strings.ToLower(string(content))
		// Preserve on-disk identifier accuracy where code still uses legacy
		// field names in MetaPageBody.
		text = allowedLegacyFields.ReplaceAllString(text, "")
		if strings.Contains(text, "slab") {
			t.Fatalf("legacy slab terminology found in %s; use persistent value-log wording", p)
		}
	}
}

func TestDocs_CanonicalStoragePaths(t *testing.T) {
	staleValueLogPath := regexp.MustCompile(`wal/value-l(?:\*|<|\d|-)`)
	for _, p := range markdownDocs(t) {
		content, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		for i, line := range strings.Split(string(content), "\n") {
			lower := strings.ToLower(line)
			if strings.Contains(lower, "legacy") {
				continue
			}
			if staleValueLogPath.MatchString(lower) || strings.Contains(lower, "maindb/wal/value") {
				t.Fatalf("%s:%d uses stale value-log path; canonical value-log path is maindb/value_vlog/value-l*.log", p, i+1)
			}
			mentionsValueLog := strings.Contains(lower, "value-log") || strings.Contains(lower, "value log") || strings.Contains(lower, "large values")
			if mentionsValueLog && (strings.Contains(lower, "dir/maindb/wal") || strings.Contains(lower, "options.dir/maindb/wal") || strings.Contains(lower, "maindb/wal/")) {
				t.Fatalf("%s:%d places value-log data under wal; canonical value-log path is maindb/value_vlog/", p, i+1)
			}
		}
	}
}

func TestDocs_VectorIndexStateWordingDoesNotRecanonicalizeGraphRows(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	stalePhrases := []string{
		"row-asset graph remains the canonical searchable index",
		"row graph asset ref as the canonical graph asset",
		"canonical graph row asset plus vector-index state",
		"future row/document refs",
		"gating tied to the canonical row-asset",
	}
	roots := []string{
		filepath.Join(treeRoot, "collections"),
		filepath.Join(treeRoot, "docs", "spec"),
		filepath.Join(treeRoot, "docs", "guides"),
	}
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || (!strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, ".md")) {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			text := strings.ToLower(string(data))
			for _, phrase := range stalePhrases {
				if strings.Contains(text, strings.ToLower(phrase)) {
					t.Fatalf("%s still contains stale vector-index wording %q", path, phrase)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}

	guide, err := os.ReadFile(filepath.Join(treeRoot, "docs", "guides", "vector-search-typed-column.md"))
	if err != nil {
		t.Fatalf("read vector search guide: %v", err)
	}
	guideText := string(guide)
	for _, want := range []string{"vector-index state `row_refs` assets", "Returned opaque document IDs", "vector-index state `document_ids` asset"} {
		if !strings.Contains(guideText, want) {
			t.Fatalf("vector search guide missing current row-ref wording %q", want)
		}
	}
}

func TestDocs_VectorProjectionFetchGuidance(t *testing.T) {
	treeRoot, repoRoot := repoRoots(t)
	checks := map[string][]string{
		filepath.Join(treeRoot, "docs", "guides", "vector-search-typed-column.md"): {
			"ProjectionOrientedVectorDocumentFetchPreset",
			"ColumnRetainedPayloadNonColumn",
			"ColumnRetainedPayloadFull` is supported for latency-oriented compatibility",
			"full-document comparison row",
		},
		filepath.Join(treeRoot, "docs", "spec", "column-graph-native-vector-search.md"): {
			"ProjectionOrientedVectorDocumentFetchPresetForField(\"embedding\")",
			"explicit full-document/embedding-echo",
			"Keep these document-fetch rows separate from graph-search",
		},
		filepath.Join(repoRoot, "cmd", "unified_bench", "README.md"): {
			"projection_without_embedding",
			"ProjectionOrientedVectorDocumentFetchPreset",
			"-collection-storage-vector-full-documents",
		},
		filepath.Join(repoRoot, "cmd", "treedb_vector_search_demo", "README.md"): {
			"no-document ANN/search-throughput boundary",
			"do not time final",
			"ProjectionOrientedVectorDocumentFetchPreset",
		},
		filepath.Join(repoRoot, "cmd", "treedb_vector_demo", "README.md"): {
			"explicit full-document/embedding-echo",
			"ProjectionOrientedVectorDocumentFetchPreset",
		},
	}
	for path, needles := range checks {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		text := string(data)
		for _, needle := range needles {
			if !strings.Contains(text, needle) {
				t.Fatalf("%s missing vector projection guidance %q", path, needle)
			}
		}
	}
}

func TestDocs_VectorHighQPSBenchmarkWorkflow2410(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	path := filepath.Join(treeRoot, "docs", "guides", "vector-search-benchmark-workflow.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	doc := string(data)
	normalizedDoc := strings.Join(strings.Fields(doc), " ")

	for _, want := range []string{
		"# TreeDB vs USearch vector benchmark workflow",
		"## Current performance snapshot: Tier S exact no-document comparison",
		"2feb1f0e35459d1b3d044008203d0c8afcf5630f",
		"Apple M3 (`darwin/arm64`)",
		"Fixture: 10k documents, 64 dimensions, `M=16`, `efConstruction=128`",
		"USearch is a pure in-memory external ANN baseline, not TreeDB persistent storage",
		"| `TreeDB_CollectionSearchVectorIndexWithBuffer` | 1 | 43,049 | 0 | 0 | collection buffered, caller-owned results |",
		"| `TreeDB_CollectionSearchVectorIndexNoDocsOneShot` | 8 | 44,679 | 816 | 2 | public one-shot convenience, response-owned results |",
		"| `TreeDB_SearchWithBufferParallel` | 8 | 8,610 | 0 | 0 | reusable searcher, one buffer/searcher per worker |",
		"| `USearch_SearchParallel` | 8 | 6,906 | 139 | 3 | pure in-memory USearch baseline |",
		"search_route_hnsw_search_pack/search=1",
		"hnsw_search_pack_active/search=1",
		"docs_fetched/search=0",
		"open_searcher_calls/op=0",
		"open_setup_in_timed_loop=0",
		"graph_row_fallbacks/search=0",
		"typed_column_vector_fallbacks/search=0",
		"vector_scratch_decodes/search=0",
		"`816 B/op`, `2 allocs/op`, and `response_owned_result_alloc/op=1`",
		"`TreeDB_CollectionSearchVectorIndexWithDocumentsOneShot` | 0 | 1.000 | 10.00",
		"libusearch_c.dylib",
		"libusearch_c.so",
		"DYLD_LIBRARY_PATH",
		"LD_LIBRARY_PATH",
		"$RUN_DIR/README.md",
		"$RUN_DIR/bench.txt",
		"RUN_DIR=/tmp/gomap_vector_search_compare_tier_s_$(date +%Y%m%d_%H%M%S)",
		"TREEDB_VECTOR_BENCH_DOCS=10000 TREEDB_VECTOR_BENCH_DIMS=64",
		"TREEDB_VECTOR_BENCH_DOCS=100000 TREEDB_VECTOR_BENCH_DIMS=128",
		"CPU_LIST=1,8 BENCHTIME=1000x COUNT=3",
		"BENCH_REGEX='BenchmarkCollectionVectorUSearchProductionCompare$'",
		"focused regex that excludes the with-documents/materialization row",
		"BENCH_REGEX='BenchmarkCollectionVectorUSearchProductionCompare/(TreeDB_SearchWithBuffer|TreeDB_SearchWithBufferParallel|TreeDB_CollectionSearchVectorIndexWithBuffer|TreeDB_CollectionSearchVectorIndexNoDocsOneShot|USearch_Search|USearch_SearchParallel)$'",
		"/tmp/gomap_2366_final_20260605_030355/closeout_summary.md",
		"/tmp/gomap_2366_final_20260605_030355/tier_s_bench.log",
		"/tmp/gomap_2366_final_20260605_030355/with_buffer_alloc_proof.log",
		"The exact FP32 `hnsw_search_pack_v1` counters above are distinct from future quantized route counters",
		"`TreeDB_SearchWithBufferParallel` for the actual c=8 concurrent profile",
		"only raises `GOMAXPROCS`; it does not create concurrent benchmark workers",
	} {
		normalizedWant := strings.Join(strings.Fields(want), " ")
		if !strings.Contains(doc, want) && !strings.Contains(normalizedDoc, normalizedWant) {
			t.Fatalf("%s missing #2410 vector benchmark contract text %q", path, want)
		}
	}

	readmePath := filepath.Join(treeRoot, "README.md")
	readme, err := os.ReadFile(readmePath)
	if err != nil {
		t.Fatalf("read %s: %v", readmePath, err)
	}
	if !strings.Contains(string(readme), "TreeDB/docs/guides/vector-search-benchmark-workflow.md") {
		t.Fatalf("%s missing link to #2410 vector benchmark workflow guide", readmePath)
	}

	guidesReadmePath := filepath.Join(treeRoot, "docs", "guides", "README.md")
	guidesReadme, err := os.ReadFile(guidesReadmePath)
	if err != nil {
		t.Fatalf("read %s: %v", guidesReadmePath, err)
	}
	guidesReadmeText := string(guidesReadme)
	for _, want := range []string{
		"TreeDB vs USearch vector benchmark workflow",
		"vector-search-benchmark-workflow.md",
		"required fast-path",
		"artifact directories",
		"profile capture",
	} {
		if !strings.Contains(guidesReadmeText, want) {
			t.Fatalf("%s missing #2410 guide index wording %q", guidesReadmePath, want)
		}
	}
}

func TestTypedStorageStorageFormatDocsMentionCompatibilityDirectory(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	path := filepath.Join(treeRoot, "docs", "spec", "storage-format.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read storage-format doc: %v", err)
	}
	doc := string(data)
	pathNeedle := "`column_assets/<namespace>/assets/segments/segment-*.tca`"
	if !strings.Contains(doc, pathNeedle) {
		t.Fatalf("storage-format doc missing exact typed asset manager path %q", pathNeedle)
	}

	normalizedDoc := strings.Join(strings.Fields(doc), " ")
	for _, want := range []string{
		"- typed asset manager segments under `column_assets/<namespace>/assets/segments/segment-*.tca` for production typed-storage physical assets",
		"`column_assets` remains the compatibility directory name",
		"Production typed-storage physical data is stored in typed asset manager segments under the compatibility `column_assets` directory",
		"typed-row payloads, typed-column part payloads, and derived accelerator payloads",
	} {
		if !strings.Contains(normalizedDoc, want) {
			t.Fatalf("storage-format doc missing typed-storage compatibility wording %q", want)
		}
	}
}

func TestDocs_NullableTypedColumnSemantics(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	storagePath := filepath.Join(treeRoot, "docs", "spec", "storage-format.md")
	storageData, err := os.ReadFile(storagePath)
	if err != nil {
		t.Fatalf("read storage-format doc: %v", err)
	}
	storageDoc := strings.Join(strings.Fields(string(storageData)), " ")
	for _, want := range []string{
		"Nullable scalar typed-column support uses nullable int64 carrier granules for bool, int64, float32, double/float64, and low-cardinality string fields",
		"nullable scalar column uses the `nullable_int64` encoding",
		"the null bitmap marks rows whose JSON path was present with an explicit `null`",
		"the default/missing bitmap marks rows whose declared path was omitted",
		"metadata, when present, covers only stored present/non-null carrier values",
		"positive optimization expectation, not only a no-regression gate",
		"actively remove existing avoidable allocations and obvious local overhead in the same touched path",
		"target 0 allocs/op after setup when benchmarking the core typed-column loop separately from document materialization",
		"Touched inner loops must be measurably no worse, and preferably better, on `B/op` and `allocs/op`",
		"Checksum, lifetime, schema, null/missing, and fail-closed validation must not be weakened",
		"Production `float32_vector`, `uint32_list`, and `adjacency_list` nullable/missing support remains staged and fail-closed",
	} {
		if !strings.Contains(storageDoc, want) {
			t.Fatalf("storage-format doc missing nullable typed-column wording %q", want)
		}
	}

	adapterPath := filepath.Join(treeRoot, "docs", "spec", "typed-column-adapter.md")
	adapterData, err := os.ReadFile(adapterPath)
	if err != nil {
		t.Fatalf("read typed-column adapter doc: %v", err)
	}
	adapterDoc := strings.Join(strings.Fields(string(adapterData)), " ")
	for _, want := range []string{
		"Nullable scalar adapter support uses `nullable_int64` as the carrier encoding for bool, int64, float32, double, and low-cardinality string fields",
		"present/non-null rows write the declared path and value, explicit-null rows write the declared path with JSON null, and missing/default rows leave the declared path absent",
		"the scan fails closed with `ErrColumnQueryPlanUnsupported`; it must not fall back to full-document reconstruction/materialization",
		"Direct typed-column predicate paths must preserve hot-path allocation discipline and should actively remove existing avoidable allocations",
		"Touched inner loops must be measurably no worse, and preferably better, on `B/op` and `allocs/op`",
		"baseline-versus-final `B/op`/`allocs/op` evidence and an allocation profile/top",
	} {
		if !strings.Contains(adapterDoc, want) {
			t.Fatalf("typed-column adapter doc missing nullable query/reconstruction wording %q", want)
		}
	}
}

func TestDocs_DurabilityMatrixSingleOwner(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	owner := filepath.Join(treeRoot, "docs", "spec", "write-path-and-durability.md")
	heading := regexp.MustCompile(`(?im)^#{1,6}\s+.*durability matrix`)
	for _, p := range markdownDocs(t) {
		content, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		if p != owner && heading.Match(content) {
			t.Fatalf("%s defines a durability matrix; link to %s instead", p, owner)
		}
	}
}

func TestDocs_CollectionWALCurrentTargetLabels(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	ownerDocs := map[string]bool{
		filepath.Join(treeRoot, "docs", "spec", "backup-restore.md"):                 true,
		filepath.Join(treeRoot, "docs", "spec", "collection-wal-durability-plan.md"): true,
		filepath.Join(treeRoot, "docs", "spec", "collections-write-domain.md"):       true,
		filepath.Join(treeRoot, "docs", "spec", "contracts.md"):                      true,
		filepath.Join(treeRoot, "docs", "spec", "native-query-raft-roadmap.md"):      true,
		filepath.Join(treeRoot, "docs", "spec", "native-wire-protocol.md"):           true,
		filepath.Join(treeRoot, "docs", "spec", "recovery.md"):                       true,
		filepath.Join(treeRoot, "docs", "spec", "storage-format.md"):                 true,
		filepath.Join(treeRoot, "docs", "spec", "value-log-lifecycle.md"):            true,
		filepath.Join(treeRoot, "docs", "spec", "verification.md"):                   true,
		filepath.Join(treeRoot, "docs", "spec", "write-path-and-durability.md"):      true,
		filepath.Join(treeRoot, "docs", "spec", "GOMAP_TREEDB_COLUMN_STORE_RFC.md"):  true,
		filepath.Join(treeRoot, "docs", "spec", "COMPRESSION_TECHNOLOGY_SPEC.md"):    true,
	}
	terms := []string{"collection wal", "durable-at-ack", "applied watermark", "side ref", "root group"}
	phaseTerms := []string{"current behavior", "target behavior", "target contract", "planned", "until collection wal lands", "after the collection wal gate", "before collection wal lands", "once collection wal", "target collection", "current shipped", "future collection wal"}

	for _, p := range markdownDocs(t) {
		if ownerDocs[p] {
			continue
		}
		content, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		text := strings.ToLower(string(content))
		hasTerm := false
		for _, term := range terms {
			if strings.Contains(text, term) {
				hasTerm = true
				break
			}
		}
		if !hasTerm {
			continue
		}
		hasPhase := false
		for _, term := range phaseTerms {
			if strings.Contains(text, term) {
				hasPhase = true
				break
			}
		}
		if !hasPhase {
			t.Fatalf("%s mentions collection WAL terms without current/target phase language", p)
		}
	}
}

func TestDocs_SpecManifestFilesExist(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	readmePath := filepath.Join(treeRoot, "docs", "spec", "README.md")
	content, err := os.ReadFile(readmePath)
	if err != nil {
		t.Fatalf("read %s: %v", readmePath, err)
	}
	re := regexp.MustCompile("TreeDB/docs/spec/([^`\\s]+\\.md)")
	matches := re.FindAllSubmatch(content, -1)
	if len(matches) == 0 {
		t.Fatalf("no spec manifest links found in %s", readmePath)
	}
	for _, match := range matches {
		name := string(match[1])
		path := filepath.Join(treeRoot, "docs", "spec", name)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("spec manifest references missing file %s: %v", path, err)
		}
	}
}

func TestDocs_HybridSearchContract2502(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	path := filepath.Join(treeRoot, "docs", "spec", "hybrid-search-contract.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	doc := string(data)
	normalizedDoc := strings.Join(strings.Fields(doc), " ")
	for _, want := range []string{
		"Collection.SearchHybrid(HybridSearchOptions)",
		"HybridSearchCandidate",
		"HybridSearchStats",
		"ErrHybridSearchUnsupported",
		"ErrHybridSearchIndexUnavailable",
		"ErrHybridSearchStaleIndex",
		"higher-is-better",
		"reciprocal-rank fusion",
		"fused_score_best_rank_source_order_id",
		"prefilter",
		"postfilter",
		"text_first",
		"vector_first",
		"union_fusion",
		"current_snapshot",
		"bound_snapshot",
		"full_document_scan_fallbacks",
		"documents_fetched",
		"#1764",
		"#2503",
		"#2504",
		"#2505",
	} {
		normalizedWant := strings.Join(strings.Fields(want), " ")
		if !strings.Contains(doc, want) && !strings.Contains(normalizedDoc, normalizedWant) {
			t.Fatalf("%s missing hybrid contract wording %q", path, want)
		}
	}
}

func TestDocs_NativeWireRaftLocalWALSeparation(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	paths := []string{
		filepath.Join(treeRoot, "docs", "spec", "collection-wal-durability-plan.md"),
		filepath.Join(treeRoot, "docs", "spec", "native-query-raft-roadmap.md"),
		filepath.Join(treeRoot, "docs", "spec", "native-wire-protocol.md"),
	}
	for _, p := range paths {
		content, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		text := strings.ToLower(string(content))
		if strings.Contains(text, "raft") && strings.Contains(text, "collection wal") {
			hasLocalPhysical := strings.Contains(text, "local physical")
			hasNotRaftLog := strings.Contains(text, "not a raft log")
			if !hasLocalPhysical || !hasNotRaftLog {
				t.Fatalf("%s mentions Raft and collection WAL without stating that collection WAL is local physical state and not a Raft log entry", p)
			}
		}
	}
}
