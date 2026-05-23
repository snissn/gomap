package collections

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func readTypedStorageNamingDoc(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "docs", "spec", "typed-storage-naming.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read typed-storage naming doc: %v", err)
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
		"Do not use \"column store\" as the umbrella name",
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
		"rg -n \"ColumnStore|column store|column-store\" TreeDB docs experiments",
		"| Path | Symbol/text | Current meaning | Classification | Action | Deferral reason |",
		"`TreeDB/collections/api.go`",
		"`ColumnStoreConfig`",
		"`experiments/colgranule/**`",
		"true typed-column terminology",
		"compatibility-retained",
		"deferred",
	)
}

func TestTypedStorageLegacyColumnStoreNamesRemainClassified(t *testing.T) {
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

func TestTypedStoragePR0RuntimeBoundary(t *testing.T) {
	doc := readTypedStorageNamingDoc(t)

	requireDocContains(t, doc,
		"## PR 0 Runtime Boundary",
		"no durable typed-column part format",
		"no `ColumnStoreConfig` removal",
		"no public API break",
		"no query planner change",
		"no production data-path behavior change",
		"no #1736 resource-manager behavior change",
	)
}
