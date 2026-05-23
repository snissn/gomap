package collections

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"
	"unicode/utf8"
)

func typedStorageRepoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
}

func readTypedStorageNamingDoc(t *testing.T) string {
	t.Helper()
	path := filepath.Join(typedStorageRepoRoot(t), "TreeDB", "docs", "spec", "typed-storage-naming.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read typed-storage naming doc: %v", err)
	}
	return string(data)
}

func readTypedStorageSpecREADME(t *testing.T) string {
	t.Helper()
	path := filepath.Join(typedStorageRepoRoot(t), "TreeDB", "docs", "spec", "README.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read typed-storage spec README: %v", err)
	}
	return string(data)
}

func requireDocContains(t *testing.T, doc string, needles ...string) {
	t.Helper()
	for _, needle := range needles {
		if !strings.Contains(doc, needle) {
			t.Fatalf("typed-storage naming doc missing %q", needle)
		}
	}
}

func TestTypedStorageNamingVocabulary(t *testing.T) {
	doc := readTypedStorageNamingDoc(t)

	requireDocContains(t, doc,
		"`typed storage` is the umbrella name",
		"`typed storage`",
		"`typed-row storage` / `typed_row_asset`",
		"`typed-column storage` / `typed_column_part`",
		"`retained document` / `document_payload`",
		"`derived accelerator`",
		"Do not use \"column "+"store\" as the umbrella name",
	)
}

func TestTypedStorageFieldOwnerVocabulary(t *testing.T) {
	doc := readTypedStorageNamingDoc(t)

	requireDocContains(t, doc,
		"Authoritative field owners are only:",
		"`retained_document` / `document_payload`",
		"`typed_row_asset`",
		"`typed_column_part`",
	)

	if strings.Contains(doc, "`derived_accelerator` is an authoritative") {
		t.Fatalf("typed-storage naming doc must not describe derived_accelerator as authoritative")
	}
}

func TestTypedStorageVocabularyMatchesConstants(t *testing.T) {
	doc := readTypedStorageNamingDoc(t)
	rows := []string{
		fmt.Sprintf("| `%s` | `%s` |", "TypedStorageOwnerRetainedDocument", TypedStorageOwnerRetainedDocument),
		"| `document_payload` alias | `document_payload` |",
		fmt.Sprintf("| `%s` | `%s` |", "TypedStorageOwnerRowAsset", TypedStorageOwnerRowAsset),
		fmt.Sprintf("| `%s` | `%s` |", "TypedStorageOwnerColumnPart", TypedStorageOwnerColumnPart),
		fmt.Sprintf("| `%s` | `%s` |", "TypedStorageAssetClassDerivedAccelerator", TypedStorageAssetClassDerivedAccelerator),
		fmt.Sprintf("| `%s` | `%s` |", "Column"+"StoreValueBool", ColumnStoreValueBool),
		fmt.Sprintf("| `%s` | `%s` |", "Column"+"StoreValueInt64", ColumnStoreValueInt64),
		fmt.Sprintf("| `%s` | `%s` |", "Column"+"StoreValueFloat32", ColumnStoreValueFloat32),
		fmt.Sprintf("| `%s` | `%s` |", "Column"+"StoreValueDouble", ColumnStoreValueDouble),
		fmt.Sprintf("| `%s` | `%s` |", "Column"+"StoreValueString", ColumnStoreValueString),
		fmt.Sprintf("| `%s` | `%s` |", "Column"+"StoreValueFloat32Vector", ColumnStoreValueFloat32Vector),
		fmt.Sprintf("| `%s` | `%s` |", "Column"+"StoreValueAdjacencyList", ColumnStoreValueAdjacencyList),
	}
	for _, want := range rows {
		if !strings.Contains(doc, want) {
			t.Fatalf("typed-storage naming doc missing code vocabulary row %q", want)
		}
	}
}

func TestTypedStorageDerivedAcceleratorsAreNotAuthoritative(t *testing.T) {
	doc := readTypedStorageNamingDoc(t)

	requireDocContains(t, doc,
		"`derived_accelerator` is a classification only",
		"must not silently become a second source of truth",
		"dictionary-code assets",
		"int64-values assets",
		"aggregate metadata",
		"vector graph assets",
		"read caches and decoded metadata caches",
	)

	for _, row := range []string{
		"| dictionary-code assets | `derived_accelerator` |",
		"| int64-values assets | `derived_accelerator` |",
		"| aggregate metadata | `derived_accelerator` |",
		"| vector graph assets | `derived_accelerator` |",
		"| read caches and decoded metadata caches | `derived_accelerator` |",
	} {
		if !strings.Contains(doc, row) {
			t.Fatalf("typed-storage naming doc missing derived accelerator row %q", row)
		}
	}
}

func TestTypedStorageLegacyNameInventoryTable(t *testing.T) {
	doc := readTypedStorageNamingDoc(t)

	requireDocContains(t, doc,
		"## Legacy Name Inventory",
		"rg -n \""+typedStorageLegacyNamePatternText()+"\" TreeDB docs experiments",
		"| Path | Symbol/text | Current meaning | Classification | Action | Deferral reason |",
		"`TreeDB/collections/api.go`",
		"`Column"+"StoreConfig`",
		"`experiments/colgranule/**`",
		"true typed-column terminology",
		"compatibility-retained",
		"deferred",
		"TestTypedStorageLegacyNameAllowlistIsComplete",
	)
}

func TestTypedStorageLegacyNamesRemainClassified(t *testing.T) {
	doc := readTypedStorageNamingDoc(t)

	requireDocContains(t, doc,
		"## PR 2 Umbrella Rename Cleanup",
		"Issue #1752 is a naming cleanup only",
		"Public/exported `Column"+"Store*` names remain compatibility-retained",
		"Remaining legacy names must fall into one of these classes:",
		"| compatibility-retained |",
		"| true typed-column terminology |",
		"| deferred |",
		"Public API compatibility; do not remove in PR 2",
	)

	for _, class := range []string{"compatibility-retained", "true typed-column terminology", "deferred"} {
		if !strings.Contains(doc, class) {
			t.Fatalf("typed-storage naming doc missing legacy class %q", class)
		}
	}
}

func TestTypedStorageLegacyNameAllowlistIsComplete(t *testing.T) {
	actual := scanTypedStorageLegacyNameUsage(t)
	expected := make(map[string]typedStorageLegacyNameAllowlistEntry, len(typedStorageLegacyNameAllowlist))
	var problems []string
	for _, entry := range typedStorageLegacyNameAllowlist {
		if entry.path == "" || entry.classification == "" {
			problems = append(problems, fmt.Sprintf("invalid empty allowlist entry: %+v", entry))
			continue
		}
		if _, ok := typedStorageLegacyNameClassifications[entry.classification]; !ok {
			problems = append(problems, fmt.Sprintf("%s uses unknown classification %q", entry.path, entry.classification))
		}
		if prior, ok := expected[entry.path]; ok {
			problems = append(problems, fmt.Sprintf("duplicate allowlist entry for %s: %+v and %+v", entry.path, prior, entry))
			continue
		}
		expected[entry.path] = entry
	}

	for path, usage := range actual {
		entry, ok := expected[path]
		if !ok {
			problems = append(problems, fmt.Sprintf("unclassified legacy-name usage in %s: %d matching lines, %d occurrences", path, usage.matchingLines, usage.occurrences))
			continue
		}
		if usage.matchingLines != entry.matchingLines || usage.occurrences != entry.occurrences {
			problems = append(problems, fmt.Sprintf("legacy-name usage drift in %s (%s): got %d matching lines/%d occurrences, want %d/%d", path, entry.classification, usage.matchingLines, usage.occurrences, entry.matchingLines, entry.occurrences))
		}
	}
	for path, entry := range expected {
		if _, ok := actual[path]; !ok {
			problems = append(problems, fmt.Sprintf("allowlist entry %s (%s) has no current matches", path, entry.classification))
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		t.Fatalf("typed-storage legacy-name allowlist is stale:\n%s\nRun `rg -n %q TreeDB docs experiments`, then classify each changed match before updating this allowlist.", strings.Join(problems, "\n"), typedStorageLegacyNamePatternText())
	}
}

func TestTypedStorageSpecLinksResolve(t *testing.T) {
	repoRoot := typedStorageRepoRoot(t)
	specDir := filepath.Join(repoRoot, "TreeDB", "docs", "spec")
	docs := []string{
		filepath.Join(specDir, "typed-storage-naming.md"),
		filepath.Join(specDir, "README.md"),
	}
	var problems []string
	for _, docPath := range docs {
		data, err := os.ReadFile(docPath)
		if err != nil {
			t.Fatalf("read %s: %v", docPath, err)
		}
		refs := typedStorageMarkdownReferences(string(data))
		if len(refs) == 0 {
			problems = append(problems, fmt.Sprintf("%s has no markdown/spec references to validate", filepath.ToSlash(docPath)))
			continue
		}
		for _, ref := range refs {
			if ok := typedStorageMarkdownReferenceResolves(repoRoot, filepath.Dir(docPath), ref); !ok {
				problems = append(problems, fmt.Sprintf("%s references missing markdown path %q", filepath.ToSlash(docPath), ref))
			}
		}
	}
	readme := readTypedStorageSpecREADME(t)
	requireDocContains(t, readme,
		"`TreeDB/docs/spec/typed-storage-naming.md`",
		"Typed physical storage vocabulary",
		"`typed-storage-naming.md`; the #1753 typed-column transplant scope is recorded",
	)
	if len(problems) > 0 {
		sort.Strings(problems)
		t.Fatalf("typed-storage spec link validation failed:\n%s", strings.Join(problems, "\n"))
	}
}

func TestTypedStoragePR0RuntimeBoundary(t *testing.T) {
	doc := readTypedStorageNamingDoc(t)

	requireDocContains(t, doc,
		"## PR 0 Runtime Boundary",
		"no durable typed-column part format",
		"no `Column"+"StoreConfig` removal",
		"no public API break",
		"no query planner change",
		"no production data-path behavior change",
		"no #1736 resource-manager behavior change",
	)
}

func TestTypedStorageNamingRuntimeBoundary(t *testing.T) {
	doc := readTypedStorageNamingDoc(t)

	requireDocContains(t, doc,
		"## Naming Regression Test Boundary",
		"Issue #1773 adds regression coverage for this naming contract only",
		"limited to docs, tests, and process evidence",
		"repo-scan allowlist updates",
		"spec link checks",
		"code-vocabulary alignment checks",
		"must move to a separate implementation tracker",
	)
}

type typedStorageLegacyNameClassification string

const (
	typedStorageLegacyCompatibility typedStorageLegacyNameClassification = "compatibility-retained"
	typedStorageLegacyDerived       typedStorageLegacyNameClassification = "derived accelerator"
	typedStorageLegacyDeferred      typedStorageLegacyNameClassification = "deferred"
	typedStorageLegacyTrueColumn    typedStorageLegacyNameClassification = "true typed-column terminology"
)

var typedStorageLegacyNameClassifications = map[typedStorageLegacyNameClassification]struct{}{
	typedStorageLegacyCompatibility: {},
	typedStorageLegacyDerived:       {},
	typedStorageLegacyDeferred:      {},
	typedStorageLegacyTrueColumn:    {},
}

type typedStorageLegacyNameAllowlistEntry struct {
	path           string
	classification typedStorageLegacyNameClassification
	matchingLines  int
	occurrences    int
}

var typedStorageLegacyNameAllowlist = []typedStorageLegacyNameAllowlistEntry{
	{path: "TreeDB/collections/api.go", classification: typedStorageLegacyCompatibility, matchingLines: 45, occurrences: 51},
	{path: "TreeDB/collections/column_aggregate_metadata_asset.go", classification: typedStorageLegacyDerived, matchingLines: 7, occurrences: 8},
	{path: "TreeDB/collections/column_asset_gc_test.go", classification: typedStorageLegacyCompatibility, matchingLines: 32, occurrences: 34},
	{path: "TreeDB/collections/column_asset_manager.go", classification: typedStorageLegacyCompatibility, matchingLines: 11, occurrences: 11},
	{path: "TreeDB/collections/column_asset_mappedresource_test.go", classification: typedStorageLegacyCompatibility, matchingLines: 12, occurrences: 12},
	{path: "TreeDB/collections/column_asset_reachability_test.go", classification: typedStorageLegacyCompatibility, matchingLines: 22, occurrences: 23},
	{path: "TreeDB/collections/column_asset_rewrite.go", classification: typedStorageLegacyCompatibility, matchingLines: 8, occurrences: 10},
	{path: "TreeDB/collections/column_asset_rewrite_test.go", classification: typedStorageLegacyCompatibility, matchingLines: 18, occurrences: 18},
	{path: "TreeDB/collections/column_dict_int64_query.go", classification: typedStorageLegacyDerived, matchingLines: 2, occurrences: 2},
	{path: "TreeDB/collections/column_dictionary_codes_asset.go", classification: typedStorageLegacyDerived, matchingLines: 10, occurrences: 10},
	{path: "TreeDB/collections/column_dictionary_query.go", classification: typedStorageLegacyDerived, matchingLines: 6, occurrences: 6},
	{path: "TreeDB/collections/column_int64_query.go", classification: typedStorageLegacyDerived, matchingLines: 2, occurrences: 2},
	{path: "TreeDB/collections/column_int64_values_asset.go", classification: typedStorageLegacyDerived, matchingLines: 8, occurrences: 8},
	{path: "TreeDB/collections/column_manifest_format.go", classification: typedStorageLegacyCompatibility, matchingLines: 2, occurrences: 2},
	{path: "TreeDB/collections/column_physical_asset.go", classification: typedStorageLegacyDeferred, matchingLines: 35, occurrences: 35},
	{path: "TreeDB/collections/column_physical_asset_test.go", classification: typedStorageLegacyDeferred, matchingLines: 156, occurrences: 160},
	{path: "TreeDB/collections/column_physical_query.go", classification: typedStorageLegacyDeferred, matchingLines: 36, occurrences: 38},
	{path: "TreeDB/collections/column_physical_query_test.go", classification: typedStorageLegacyDeferred, matchingLines: 128, occurrences: 139},
	{path: "TreeDB/collections/column_physical_row_reader.go", classification: typedStorageLegacyDeferred, matchingLines: 21, occurrences: 21},
	{path: "TreeDB/collections/column_physical_row_reader_test.go", classification: typedStorageLegacyDeferred, matchingLines: 19, occurrences: 20},
	{path: "TreeDB/collections/column_physical_scan.go", classification: typedStorageLegacyDeferred, matchingLines: 34, occurrences: 34},
	{path: "TreeDB/collections/column_physical_scan_test.go", classification: typedStorageLegacyDeferred, matchingLines: 36, occurrences: 43},
	{path: "TreeDB/collections/column_physical_visibility.go", classification: typedStorageLegacyDeferred, matchingLines: 8, occurrences: 8},
	{path: "TreeDB/collections/column_publish_plan.go", classification: typedStorageLegacyCompatibility, matchingLines: 46, occurrences: 59},
	{path: "TreeDB/collections/column_publish_plan_test.go", classification: typedStorageLegacyCompatibility, matchingLines: 69, occurrences: 90},
	{path: "TreeDB/collections/column_publish_write.go", classification: typedStorageLegacyCompatibility, matchingLines: 56, occurrences: 60},
	{path: "TreeDB/collections/column_publish_write_bench_test.go", classification: typedStorageLegacyCompatibility, matchingLines: 35, occurrences: 37},
	{path: "TreeDB/collections/column_publish_write_path_test.go", classification: typedStorageLegacyCompatibility, matchingLines: 163, occurrences: 183},
	{path: "TreeDB/collections/column_query_plan.go", classification: typedStorageLegacyCompatibility, matchingLines: 43, occurrences: 43},
	{path: "TreeDB/collections/column_query_plan_test.go", classification: typedStorageLegacyCompatibility, matchingLines: 56, occurrences: 68},
	{path: "TreeDB/collections/column_reconstruction.go", classification: typedStorageLegacyCompatibility, matchingLines: 19, occurrences: 20},
	{path: "TreeDB/collections/column_store.go", classification: typedStorageLegacyCompatibility, matchingLines: 72, occurrences: 113},
	{path: "TreeDB/collections/column_store_test.go", classification: typedStorageLegacyCompatibility, matchingLines: 167, occurrences: 213},
	{path: "TreeDB/collections/column_vector_graph_block_view.go", classification: typedStorageLegacyDerived, matchingLines: 4, occurrences: 4},
	{path: "TreeDB/collections/column_vector_graph_manifest.go", classification: typedStorageLegacyDerived, matchingLines: 26, occurrences: 31},
	{path: "TreeDB/collections/column_vector_graph_manifest_test.go", classification: typedStorageLegacyDerived, matchingLines: 56, occurrences: 76},
	{path: "TreeDB/collections/column_vector_graph_row_reader.go", classification: typedStorageLegacyDerived, matchingLines: 8, occurrences: 10},
	{path: "TreeDB/collections/column_vector_graph_row_reader_test.go", classification: typedStorageLegacyDerived, matchingLines: 15, occurrences: 16},
	{path: "TreeDB/collections/column_vector_graph_search_test.go", classification: typedStorageLegacyDerived, matchingLines: 1, occurrences: 1},
	{path: "TreeDB/collections/command_wal_test.go", classification: typedStorageLegacyCompatibility, matchingLines: 4, occurrences: 6},
	{path: "TreeDB/collections/typed_column_adapter.go", classification: typedStorageLegacyCompatibility, matchingLines: 26, occurrences: 26},
	{path: "TreeDB/collections/typed_column_adapter_test.go", classification: typedStorageLegacyCompatibility, matchingLines: 59, occurrences: 61},
	{path: "TreeDB/collections/typed_column_publication.go", classification: typedStorageLegacyCompatibility, matchingLines: 16, occurrences: 18},
	{path: "TreeDB/collections/typed_column_publication_test.go", classification: typedStorageLegacyCompatibility, matchingLines: 31, occurrences: 33},
	{path: "TreeDB/collections/typed_storage_layout.go", classification: typedStorageLegacyCompatibility, matchingLines: 14, occurrences: 20},
	{path: "TreeDB/collections/typed_storage_layout_test.go", classification: typedStorageLegacyCompatibility, matchingLines: 30, occurrences: 30},
	{path: "TreeDB/collections/typed_storage_naming_test.go", classification: typedStorageLegacyCompatibility, matchingLines: 7, occurrences: 7},
	{path: "TreeDB/collections/vector_index.go", classification: typedStorageLegacyDerived, matchingLines: 1, occurrences: 1},
	{path: "TreeDB/collections/vector_index_metadata_test.go", classification: typedStorageLegacyDerived, matchingLines: 9, occurrences: 11},
	{path: "TreeDB/collections/vector_index_rebuild.go", classification: typedStorageLegacyDerived, matchingLines: 16, occurrences: 18},
	{path: "TreeDB/collections/vector_index_rebuild_test.go", classification: typedStorageLegacyDerived, matchingLines: 32, occurrences: 38},
	{path: "TreeDB/docs/spec/COMPRESSION_TECHNOLOGY_SPEC.md", classification: typedStorageLegacyDeferred, matchingLines: 5, occurrences: 5},
	{path: "TreeDB/docs/spec/GOMAP_TREEDB_COLUMN_STORE_RFC.md", classification: typedStorageLegacyDeferred, matchingLines: 108, occurrences: 112},
	{path: "TreeDB/docs/spec/backup-restore.md", classification: typedStorageLegacyDeferred, matchingLines: 1, occurrences: 1},
	{path: "TreeDB/docs/spec/collection-wal-durability-plan.md", classification: typedStorageLegacyDeferred, matchingLines: 42, occurrences: 42},
	{path: "TreeDB/docs/spec/column-graph-native-block-planner.md", classification: typedStorageLegacyDeferred, matchingLines: 1, occurrences: 1},
	{path: "TreeDB/docs/spec/column-graph-native-reconstruction-inventory.md", classification: typedStorageLegacyDeferred, matchingLines: 14, occurrences: 14},
	{path: "TreeDB/docs/spec/column-graph-native-vector-search.md", classification: typedStorageLegacyDeferred, matchingLines: 7, occurrences: 8},
	{path: "TreeDB/docs/spec/typed-column-adapter.md", classification: typedStorageLegacyCompatibility, matchingLines: 2, occurrences: 2},
	{path: "TreeDB/docs/spec/typed-column-transplant.md", classification: typedStorageLegacyCompatibility, matchingLines: 4, occurrences: 5},
	{path: "TreeDB/docs/spec/typed-storage-naming.md", classification: typedStorageLegacyCompatibility, matchingLines: 23, occurrences: 28},
	{path: "TreeDB/docs/spec/user-command-wal.md", classification: typedStorageLegacyDeferred, matchingLines: 1, occurrences: 1},
	{path: "TreeDB/docs/spec/verification.md", classification: typedStorageLegacyDeferred, matchingLines: 2, occurrences: 2},
	{path: "experiments/colgranule/DEFERRED_ISSUES.md", classification: typedStorageLegacyTrueColumn, matchingLines: 1, occurrences: 1},
	{path: "experiments/colgranule/README.md", classification: typedStorageLegacyTrueColumn, matchingLines: 1, occurrences: 1},
	{path: "experiments/colgranule/cmd/jsonbench_compare/main.go", classification: typedStorageLegacyTrueColumn, matchingLines: 1, occurrences: 1},
	{path: "experiments/colgranule/collection_manifest.go", classification: typedStorageLegacyTrueColumn, matchingLines: 4, occurrences: 5},
	{path: "experiments/colgranule/collection_manifest_test.go", classification: typedStorageLegacyTrueColumn, matchingLines: 1, occurrences: 1},
	{path: "experiments/colgranule/granule_bench_test.go", classification: typedStorageLegacyTrueColumn, matchingLines: 2, occurrences: 2},
	{path: "experiments/colgranule/jsonbench_bench_test.go", classification: typedStorageLegacyTrueColumn, matchingLines: 1, occurrences: 1},
	{path: "experiments/colgranule/jsonbench_part_build_report.go", classification: typedStorageLegacyTrueColumn, matchingLines: 2, occurrences: 3},
	{path: "experiments/colgranule/jsonbench_part_queries.go", classification: typedStorageLegacyTrueColumn, matchingLines: 9, occurrences: 9},
	{path: "experiments/colgranule/jsonbench_test.go", classification: typedStorageLegacyTrueColumn, matchingLines: 2, occurrences: 4},
	{path: "experiments/colgranule/mutation_adapter.go", classification: typedStorageLegacyTrueColumn, matchingLines: 3, occurrences: 3},
	{path: "experiments/colgranule/mutation_adapter_test.go", classification: typedStorageLegacyTrueColumn, matchingLines: 5, occurrences: 5},
	{path: "experiments/colgranule/part.go", classification: typedStorageLegacyTrueColumn, matchingLines: 24, occurrences: 26},
	{path: "experiments/colgranule/part_accounting.go", classification: typedStorageLegacyTrueColumn, matchingLines: 4, occurrences: 4},
	{path: "experiments/colgranule/part_accounting_test.go", classification: typedStorageLegacyTrueColumn, matchingLines: 1, occurrences: 1},
	{path: "experiments/colgranule/part_image_decode.go", classification: typedStorageLegacyTrueColumn, matchingLines: 1, occurrences: 1},
	{path: "experiments/colgranule/part_image_test.go", classification: typedStorageLegacyTrueColumn, matchingLines: 1, occurrences: 1},
	{path: "experiments/colgranule/part_set.go", classification: typedStorageLegacyTrueColumn, matchingLines: 3, occurrences: 3},
	{path: "experiments/colgranule/part_set_test.go", classification: typedStorageLegacyTrueColumn, matchingLines: 5, occurrences: 5},
	{path: "experiments/colgranule/part_test.go", classification: typedStorageLegacyTrueColumn, matchingLines: 2, occurrences: 2},
}

type typedStorageLegacyNameUsage struct {
	matchingLines int
	occurrences   int
}

func typedStorageLegacyNamePatternText() string {
	return "Column" + "Store|column " + "store|column-" + "store"
}

var typedStorageLegacyNamePattern = regexp.MustCompile(typedStorageLegacyNamePatternText())

func scanTypedStorageLegacyNameUsage(t *testing.T) map[string]typedStorageLegacyNameUsage {
	t.Helper()
	repoRoot := typedStorageRepoRoot(t)
	roots := []string{"TreeDB", "docs", "experiments"}
	usage := make(map[string]typedStorageLegacyNameUsage)
	for _, root := range roots {
		rootPath := filepath.Join(repoRoot, root)
		if _, err := os.Stat(rootPath); err != nil {
			t.Fatalf("stat legacy-name scan root %s: %v", rootPath, err)
		}
		err := filepath.WalkDir(rootPath, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				switch d.Name() {
				case ".git", "node_modules", "vendor":
					return filepath.SkipDir
				}
				return nil
			}
			rel, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			if typedStorageLegacyNameScanSkipsGeneratedArtifact(rel) {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if !utf8.Valid(data) {
				return nil
			}
			var current typedStorageLegacyNameUsage
			for _, line := range strings.Split(string(data), "\n") {
				matches := typedStorageLegacyNamePattern.FindAllStringIndex(line, -1)
				if len(matches) == 0 {
					continue
				}
				current.matchingLines++
				current.occurrences += len(matches)
			}
			if current.matchingLines > 0 {
				usage[rel] = current
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk legacy-name scan root %s: %v", rootPath, err)
		}
	}
	return usage
}

func typedStorageLegacyNameScanSkipsGeneratedArtifact(rel string) bool {
	// CI test/race jobs write transient JSONL output under TreeDB before package
	// tests run. The contract is over repository sources matched by the audit
	// command in a clean checkout, not generated run logs.
	return strings.HasPrefix(rel, "TreeDB/treedb-") && strings.HasSuffix(rel, ".jsonl")
}

var typedStorageMarkdownReferencePatterns = []*regexp.Regexp{
	regexp.MustCompile(`\[[^\]]+\]\(([^)\s#?]+\.md)(?:#[^)]*)?\)`),
	regexp.MustCompile("`([^`\\s]+\\.md)`"),
}

func typedStorageMarkdownReferences(content string) []string {
	seen := make(map[string]struct{})
	var refs []string
	for _, pattern := range typedStorageMarkdownReferencePatterns {
		for _, match := range pattern.FindAllStringSubmatch(content, -1) {
			if len(match) < 2 {
				continue
			}
			ref := strings.TrimSpace(match[1])
			if ref == "" {
				continue
			}
			if _, ok := seen[ref]; ok {
				continue
			}
			seen[ref] = struct{}{}
			refs = append(refs, ref)
		}
	}
	sort.Strings(refs)
	return refs
}

func typedStorageMarkdownReferenceResolves(repoRoot, baseDir, ref string) bool {
	ref = strings.TrimSpace(ref)
	ref = strings.TrimSuffix(ref, ".")
	ref = strings.TrimSuffix(ref, ",")
	ref = strings.TrimSuffix(ref, ";")
	ref = strings.TrimPrefix(ref, "./")
	if idx := strings.IndexByte(ref, '#'); idx >= 0 {
		ref = ref[:idx]
	}
	var candidate string
	slashRef := filepath.FromSlash(ref)
	if filepath.IsAbs(slashRef) {
		candidate = slashRef
	} else if strings.HasPrefix(ref, "TreeDB/") || strings.HasPrefix(ref, "docs/") || strings.HasPrefix(ref, "experiments/") {
		candidate = filepath.Join(repoRoot, slashRef)
	} else {
		candidate = filepath.Join(baseDir, slashRef)
	}
	if strings.ContainsAny(candidate, "*?[") {
		matches, err := filepath.Glob(candidate)
		return err == nil && len(matches) > 0
	}
	info, err := os.Stat(candidate)
	return err == nil && !info.IsDir()
}
