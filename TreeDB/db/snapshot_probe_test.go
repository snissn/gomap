package db

import (
	"reflect"
	"testing"
)

func TestSnapshot_HasManyAndHasPrefixes(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.SetSync([]byte("acct/alice/doc-1"), []byte("v1")); err != nil {
		t.Fatalf("set alice: %v", err)
	}
	if err := db.SetSync([]byte("acct/bob/doc-1"), []byte("v2")); err != nil {
		t.Fatalf("set bob: %v", err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()

	if err := db.DeleteSync([]byte("acct/alice/doc-1")); err != nil {
		t.Fatalf("delete alice: %v", err)
	}
	if err := db.SetSync([]byte("acct/carol/doc-1"), []byte("v3")); err != nil {
		t.Fatalf("set carol: %v", err)
	}

	hasMany, err := snap.HasMany([][]byte{
		[]byte("acct/alice/doc-1"),
		[]byte("acct/bob/doc-1"),
		[]byte("acct/carol/doc-1"),
	})
	if err != nil {
		t.Fatalf("HasMany: %v", err)
	}
	if want := []bool{true, true, false}; !reflect.DeepEqual(hasMany, want) {
		t.Fatalf("HasMany mismatch: got=%v want=%v", hasMany, want)
	}

	hasPrefixes, err := snap.HasPrefixes([][]byte{
		[]byte("acct/alice/"),
		[]byte("acct/bob/"),
		[]byte("acct/carol/"),
	})
	if err != nil {
		t.Fatalf("HasPrefixes: %v", err)
	}
	if want := []bool{true, true, false}; !reflect.DeepEqual(hasPrefixes, want) {
		t.Fatalf("HasPrefixes mismatch: got=%v want=%v", hasPrefixes, want)
	}
}

func TestSnapshot_HasManyAtRootAndHasPrefixesAtRoot(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	_, rootIDs, err := db.PublishOrderedRootGroup(nil, []OrderedRootPublishInput{{
		Iter: mustFrozenSystemMemtable(t,
			"acct/alice/doc-1", "v1",
			"acct/bob/doc-1", "v2",
		).NewIterator(nil, nil),
	}})
	if err != nil {
		t.Fatalf("publish root: %v", err)
	}
	if len(rootIDs) != 1 {
		t.Fatalf("root IDs len=%d want 1", len(rootIDs))
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()

	hasMany, err := snap.HasManyAtRoot(rootIDs[0], [][]byte{
		[]byte("acct/alice/doc-1"),
		[]byte("acct/bob/doc-1"),
		[]byte("acct/carol/doc-1"),
	})
	if err != nil {
		t.Fatalf("HasManyAtRoot: %v", err)
	}
	if want := []bool{true, true, false}; !reflect.DeepEqual(hasMany, want) {
		t.Fatalf("HasManyAtRoot mismatch: got=%v want=%v", hasMany, want)
	}

	hasAnySorted, err := snap.HasAnySortedAtRoot(rootIDs[0], [][]byte{
		[]byte("acct/alice/doc-0"),
		[]byte("acct/bob/doc-1"),
		[]byte("acct/carol/doc-1"),
	})
	if err != nil {
		t.Fatalf("HasAnySortedAtRoot: %v", err)
	}
	if !hasAnySorted {
		t.Fatalf("HasAnySortedAtRoot got false want true")
	}

	hasAnySorted, err = snap.HasAnySortedAtRoot(rootIDs[0], [][]byte{
		[]byte("acct/alice/doc-0"),
		[]byte("acct/carol/doc-1"),
	})
	if err != nil {
		t.Fatalf("HasAnySortedAtRoot miss: %v", err)
	}
	if hasAnySorted {
		t.Fatalf("HasAnySortedAtRoot miss got true want false")
	}

	if _, err := snap.HasAnySortedAtRoot(rootIDs[0], [][]byte{
		[]byte("acct/bob/doc-1"),
		[]byte("acct/alice/doc-1"),
	}); err == nil {
		t.Fatalf("HasAnySortedAtRoot unsorted keys got nil error")
	}

	hasPrefixes, err := snap.HasPrefixesAtRoot(rootIDs[0], [][]byte{
		[]byte("acct/alice/"),
		[]byte("acct/bob/"),
		[]byte("acct/carol/"),
	})
	if err != nil {
		t.Fatalf("HasPrefixesAtRoot: %v", err)
	}
	if want := []bool{true, true, false}; !reflect.DeepEqual(hasPrefixes, want) {
		t.Fatalf("HasPrefixesAtRoot mismatch: got=%v want=%v", hasPrefixes, want)
	}

	hasPrefixes, err = snap.HasPrefixesAtRoot(rootIDs[0], [][]byte{
		[]byte("acct/bob/"),
		[]byte("acct/alice/doc-1"),
		[]byte("acct/alice/"),
		[]byte("acct/alice/"),
		[]byte("acct/"),
		[]byte("acct/zoe/"),
	})
	if err != nil {
		t.Fatalf("HasPrefixesAtRoot unordered/duplicate: %v", err)
	}
	if want := []bool{true, true, true, true, true, false}; !reflect.DeepEqual(hasPrefixes, want) {
		t.Fatalf("HasPrefixesAtRoot unordered/duplicate mismatch: got=%v want=%v", hasPrefixes, want)
	}

	missingRootHasMany, err := snap.HasManyAtRoot(0, [][]byte{[]byte("acct/alice/doc-1")})
	if err != nil {
		t.Fatalf("HasManyAtRoot missing root: %v", err)
	}
	if want := []bool{false}; !reflect.DeepEqual(missingRootHasMany, want) {
		t.Fatalf("HasManyAtRoot missing root mismatch: got=%v want=%v", missingRootHasMany, want)
	}

	missingRootHasAnySorted, err := snap.HasAnySortedAtRoot(0, [][]byte{[]byte("acct/alice/doc-1")})
	if err != nil {
		t.Fatalf("HasAnySortedAtRoot missing root: %v", err)
	}
	if missingRootHasAnySorted {
		t.Fatalf("HasAnySortedAtRoot missing root got true want false")
	}

	missingRootHasPrefixes, err := snap.HasPrefixesAtRoot(0, [][]byte{[]byte("acct/alice/")})
	if err != nil {
		t.Fatalf("HasPrefixesAtRoot missing root: %v", err)
	}
	if want := []bool{false}; !reflect.DeepEqual(missingRootHasPrefixes, want) {
		t.Fatalf("HasPrefixesAtRoot missing root mismatch: got=%v want=%v", missingRootHasPrefixes, want)
	}
}

func TestSnapshot_HasAnySortedAtRootRecordsPerItemFallback(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	_, rootIDs, err := db.PublishOrderedRootGroup(nil, []OrderedRootPublishInput{{
		Iter: mustFrozenSystemMemtable(t, systemRangeKVs(1100, nil)...).NewIterator(nil, nil),
	}})
	if err != nil {
		t.Fatalf("publish root: %v", err)
	}
	if len(rootIDs) != 1 {
		t.Fatalf("root IDs len=%d want 1", len(rootIDs))
	}

	before := db.rootProbeStatsSnapshot()
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()

	got, err := snap.HasAnySortedAtRoot(rootIDs[0], [][]byte{
		[]byte("sys/0000/missing"),
		[]byte("sys/1050"),
		[]byte("sys/9999"),
	})
	if err != nil {
		t.Fatalf("HasAnySortedAtRoot: %v", err)
	}
	if !got {
		t.Fatalf("HasAnySortedAtRoot got false want true")
	}

	after := db.rootProbeStatsSnapshot()
	if delta := after.keyFallbackCalls - before.keyFallbackCalls; delta != 1 {
		t.Fatalf("key fallback calls delta=%d want 1", delta)
	}
	if delta := after.keyFallbackItems - before.keyFallbackItems; delta != 1 {
		t.Fatalf("key fallback items delta=%d want 1", delta)
	}
	if delta := after.prefixFallbackItems - before.prefixFallbackItems; delta != 0 {
		t.Fatalf("prefix fallback items delta=%d want 0", delta)
	}

	stats := db.Stats()
	if got := stats["treedb.native_fastpath.per_item_key_probe_fallback_count"]; got != "1" {
		t.Fatalf("per_item_key_probe_fallback_count=%q want 1", got)
	}
	if got := stats["treedb.native_fastpath.per_item_prefix_probe_fallback_count"]; got != "0" {
		t.Fatalf("per_item_prefix_probe_fallback_count=%q want 0", got)
	}
}
