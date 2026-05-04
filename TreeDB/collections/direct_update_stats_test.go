package collections

import (
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionDirectUpdatePopulatesLastUpdateStats(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	mgr.SetUpdateBatchDetailedStatsEnabled(true)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DisableIndexedWriteMemtables: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
			{Name: "active", Field: "active", ValueType: IndexValueBool},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com","city":"hnl","active":true,"seen":false}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	matched, modified, err := col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"email":"ada@example.com","city":"sea","active":true,"seen":false}`), true, nil
	})
	if err != nil {
		t.Fatalf("direct update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("direct update matched/modified=%v/%v want true/true", matched, modified)
	}
	stats := col.LastUpdateStats()
	if got, want := stats.Items, 1; got != want {
		t.Fatalf("items=%d want %d", got, want)
	}
	if got, want := stats.Matched, 1; got != want {
		t.Fatalf("matched=%d want %d", got, want)
	}
	if got, want := stats.Modified, 1; got != want {
		t.Fatalf("modified=%d want %d", got, want)
	}
	if got, want := stats.Indexes, 3; got != want {
		t.Fatalf("indexes=%d want %d", got, want)
	}
	if got, want := stats.Runs, 3; got != want {
		t.Fatalf("runs=%d want primary/index-state/city roots", got)
	}
	if got, want := stats.IndexValueChanges, 1; got != want {
		t.Fatalf("index changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 2; got != want {
		t.Fatalf("index unchanged=%d want %d", got, want)
	}
	if got, want := stats.UniqueIndexChecks, 0; got != want {
		t.Fatalf("unique checks=%d want %d", got, want)
	}
	if got, want := stats.UniqueIndexCheckSkips, 1; got != want {
		t.Fatalf("unique skips=%d want %d", got, want)
	}
	if got, want := stats.SecondaryDeleteEntries, 1; got != want {
		t.Fatalf("secondary deletes=%d want %d", got, want)
	}
	if got, want := stats.SecondarySetEntries, 1; got != want {
		t.Fatalf("secondary sets=%d want %d", got, want)
	}
	if got := stats.SecondaryKeyBytes; got == 0 {
		t.Fatal("secondary key bytes=0 want positive")
	}
	if got, want := len(stats.SecondaryRuns), 1; got != want {
		t.Fatalf("secondary runs=%d want %d: %+v", got, want, stats.SecondaryRuns)
	}
	if run := stats.SecondaryRuns[0]; run.IndexName != "city" || run.Deletes != 1 || run.Sets != 1 || run.KeyBytes == 0 {
		t.Fatalf("city secondary run stats=%+v want delete+set with key bytes", run)
	}
	indexStats := stats.IndexStats[:stats.IndexStatsCount]
	if got, want := len(indexStats), 3; got != want {
		t.Fatalf("index stats=%d want %d: %+v", got, want, stats.IndexStats)
	}
	directUpdateStatsRequireIndex(t, indexStats, "email", true, 0, 1, 0, 1, 0, 0, 0)
	directUpdateStatsRequireIndex(t, indexStats, "city", false, 1, 0, 0, 0, 1, 1, 1)
	directUpdateStatsRequireIndex(t, indexStats, "active", false, 0, 1, 0, 0, 0, 0, 0)

	stats.SecondaryRuns[0].IndexName = "mutated"
	if got := col.LastUpdateStats().SecondaryRuns[0].IndexName; got != "city" {
		t.Fatalf("LastUpdateStats did not return owned secondary-run stats, got %q", got)
	}
}

func TestCollectionDirectUpdateStatsForNoopAndMissingDocument(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com","seen":false}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	matched, modified, err := col.updateDirect([]byte("u1"), func(current []byte) ([]byte, bool, error) {
		return current, false, nil
	})
	if err != nil {
		t.Fatalf("noop update: %v", err)
	}
	if !matched || modified {
		t.Fatalf("noop update matched/modified=%v/%v want true/false", matched, modified)
	}
	stats := col.LastUpdateStats()
	if stats.Items != 1 || stats.Matched != 1 || stats.Modified != 0 || stats.Runs != 0 || stats.IndexValueChanges != 0 || stats.IndexValueUnchanged != 0 {
		t.Fatalf("noop stats=%+v want one matched unmodified item and no root/index work", stats)
	}

	matched, modified, err = col.updateDirect([]byte("missing"), func(current []byte) ([]byte, bool, error) {
		t.Fatalf("callback should not run for missing document: %q", current)
		return nil, false, nil
	})
	if err != nil {
		t.Fatalf("missing update: %v", err)
	}
	if matched || modified {
		t.Fatalf("missing update matched/modified=%v/%v want false/false", matched, modified)
	}
	stats = col.LastUpdateStats()
	if stats.Items != 1 || stats.Matched != 0 || stats.Modified != 0 || stats.Runs != 0 || stats.IndexValueChanges != 0 || stats.IndexValueUnchanged != 0 {
		t.Fatalf("missing stats=%+v want one unmatched item and no root/index work", stats)
	}
}

func TestCollectionDirectUpdateDetailedStatsCapsAtInlineIndexLimit(t *testing.T) {
	const indexCount = maxCollectionUpdateInlineIndexStats + 2

	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	indexes := make([]IndexDefinition, indexCount)
	for i := range indexes {
		indexes[i] = IndexDefinition{
			Name:      fmt.Sprintf("idx%02d", i),
			Field:     fmt.Sprintf("f%02d", i),
			ValueType: IndexValueString,
		}
	}
	mgr := NewCollectionManager(d)
	mgr.SetUpdateBatchDetailedStatsEnabled(true)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DisableIndexedWriteMemtables: true,
		},
		Indexes: indexes,
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{directUpdateStatsWideDocument(indexCount, -1, "")},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	matched, modified, err := col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return directUpdateStatsWideDocument(indexCount, 1, "changed"), true, nil
	})
	if err != nil {
		t.Fatalf("direct update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("direct update matched/modified=%v/%v want true/true", matched, modified)
	}
	stats := col.LastUpdateStats()
	if got, want := stats.IndexStatsCount, maxCollectionUpdateInlineIndexStats; got != want {
		t.Fatalf("index stats count=%d want inline cap %d", got, want)
	}
	if got, want := stats.IndexStats[maxCollectionUpdateInlineIndexStats-1].IndexName, fmt.Sprintf("idx%02d", maxCollectionUpdateInlineIndexStats-1); got != want {
		t.Fatalf("last inline index stat name=%q want %q", got, want)
	}
	if got, want := stats.IndexValueChanges, 1; got != want {
		t.Fatalf("aggregate index changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, indexCount-1; got != want {
		t.Fatalf("aggregate index unchanged=%d want %d", got, want)
	}
	directUpdateStatsRequireIndex(t, stats.IndexStats[:stats.IndexStatsCount], "idx01", false, 1, 0, 0, 0, 1, 1, 1)
}

func directUpdateStatsWideDocument(indexCount, changedOrdinal int, changedValue string) []byte {
	out := []byte{'{'}
	for i := 0; i < indexCount; i++ {
		if i > 0 {
			out = append(out, ',')
		}
		value := fmt.Sprintf("v%02d", i)
		if i == changedOrdinal {
			value = changedValue
		}
		out = fmt.Appendf(out, "%q:%q", fmt.Sprintf("f%02d", i), value)
	}
	out = append(out, '}')
	return out
}

func directUpdateStatsRequireIndex(
	t *testing.T,
	stats []CollectionUpdateIndexStats,
	name string,
	unique bool,
	changed, unchanged, uniqueChecks, uniqueSkips, secondaryRuns, secondaryDeletes, secondarySets int,
) {
	t.Helper()
	for _, stat := range stats {
		if stat.IndexName != name {
			continue
		}
		if stat.Unique != unique ||
			stat.Changed != changed ||
			stat.Unchanged != unchanged ||
			stat.UniqueChecks != uniqueChecks ||
			stat.UniqueCheckSkips != uniqueSkips ||
			stat.SecondaryRuns != secondaryRuns ||
			stat.SecondaryDeletes != secondaryDeletes ||
			stat.SecondarySets != secondarySets {
			t.Fatalf("index %s stats=%+v want unique=%v changed=%d unchanged=%d uniqueChecks=%d uniqueSkips=%d secondaryRuns=%d deletes=%d sets=%d",
				name, stat, unique, changed, unchanged, uniqueChecks, uniqueSkips, secondaryRuns, secondaryDeletes, secondarySets)
		}
		return
	}
	t.Fatalf("missing index stats for %q in %+v", name, stats)
}
