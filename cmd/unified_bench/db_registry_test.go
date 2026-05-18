package main

import (
	"reflect"
	"testing"
)

func withDBAliasesForTest(t *testing.T, aliases map[string]string) {
	t.Helper()
	saved := make(map[string]string, len(dbAliases))
	for name, target := range dbAliases {
		saved[name] = target
	}
	dbAliases = aliases
	t.Cleanup(func() {
		dbAliases = saved
	})
}

func TestCanonicalDBNameResolvesTransitiveAliasesM11A(t *testing.T) {
	withDBAliasesForTest(t, map[string]string{
		"treedb_indirect": "treedbcached",
		"treedbcached":    "treedb",
	})

	if got := canonicalDBName("treedb_indirect"); got != "treedb" {
		t.Fatalf("canonicalDBName(treedb_indirect)=%q, want treedb", got)
	}

	got := resolveDBs("treedb_indirect", "")
	if want := []string{"treedb"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("resolveDBs transitive alias=%v, want %v", got, want)
	}
}

func TestResolveDBsDedupesCanonicalAliasesM11A(t *testing.T) {
	withDBAliasesForTest(t, map[string]string{
		"treedb_alias":    "treedb",
		"treedb_indirect": "treedb_alias",
	})

	got := resolveDBs("treedb,treedb_alias,treedb_indirect", "")
	if want := []string{"treedb"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("resolveDBs duplicate aliases=%v, want %v", got, want)
	}

	got = resolveDBs("treedb,treedb_alias,treedb_indirect", "treedb_alias")
	if len(got) != 0 {
		t.Fatalf("resolveDBs excluded canonical alias=%v, want empty", got)
	}
}

func TestCanonicalDBNameNormalizesNonCLIInputsM11A(t *testing.T) {
	withDBAliasesForTest(t, map[string]string{
		"treedb_alias": "treedb",
	})

	if got := canonicalDBName(" TREEDB_ALIAS "); got != "treedb" {
		t.Fatalf("canonicalDBName normalized alias=%q, want treedb", got)
	}
	if got := canonicalDBName(" TREEDB "); got != "treedb" {
		t.Fatalf("canonicalDBName normalized name=%q, want treedb", got)
	}
}

func TestCanonicalDBNameStopsAliasCyclesM11A(t *testing.T) {
	withDBAliasesForTest(t, map[string]string{
		"alias_a": "alias_b",
		"alias_b": "alias_a",
	})

	if got := canonicalDBName("alias_a"); got != "alias_a" {
		t.Fatalf("canonicalDBName(alias_a)=%q, want alias_a", got)
	}
}

func TestCanonicalDBNameStopsTransitiveAliasCyclesAtOriginalM11A(t *testing.T) {
	withDBAliasesForTest(t, map[string]string{
		"alias_a": "alias_b",
		"alias_b": "alias_c",
		"alias_c": "alias_b",
	})

	if got := canonicalDBName("alias_a"); got != "alias_a" {
		t.Fatalf("canonicalDBName(alias_a)=%q, want alias_a", got)
	}
}
