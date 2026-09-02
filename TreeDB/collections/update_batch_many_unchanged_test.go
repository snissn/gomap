package collections

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionUpdateBatchManyIndexesSkipsAllSecondaryWorkForNonIndexedUpdate(t *testing.T) {
	const indexCount = 32

	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DisableIndexedWriteMemtables: true,
		},
		Indexes: batchManyUnchangedIndexesForTest(indexCount),
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{batchManyUnchangedDocumentForTest(indexCount, nil)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rootNames := []string{
		collectionPrimaryRootName("users"),
		collectionIndexStateRootName("users"),
	}
	for i := 0; i < indexCount; i++ {
		rootNames = append(rootNames, collectionSecondaryRootName("users", fmt.Sprintf("idx%02d", i)))
	}
	before := batchManyUnchangedRootIDsForTest(t, d, "users", rootNames)
	replacement := batchManyUnchangedDocumentForTest(indexCount, map[string]string{"note": "primary changed"})
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return replacement, true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("results=%+v want one matched modified row", results)
	}
	stats := col.LastUpdateStats()
	if got, want := stats.IndexValueChanges, 0; got != want {
		t.Fatalf("changed indexes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, indexCount; got != want {
		t.Fatalf("unchanged indexes=%d want %d", got, want)
	}
	if stats.SecondaryDeleteEntries != 0 || stats.SecondarySetEntries != 0 || stats.SecondaryKeyBytes != 0 || len(stats.SecondaryRuns) != 0 {
		t.Fatalf("secondary work deletes=%d sets=%d bytes=%d runs=%+v, want none",
			stats.SecondaryDeleteEntries, stats.SecondarySetEntries, stats.SecondaryKeyBytes, stats.SecondaryRuns)
	}

	after := batchManyUnchangedRootIDsForTest(t, d, "users", rootNames)
	if after[collectionPrimaryRootName("users")] == before[collectionPrimaryRootName("users")] {
		t.Fatal("primary root did not change for document replacement")
	}
	if rootName := collectionIndexStateRootName("users"); after[rootName] != before[rootName] {
		t.Fatalf("index-state root %q changed from %d to %d for non-indexed update", rootName, before[rootName], after[rootName])
	}
	for i := 0; i < indexCount; i++ {
		rootName := collectionSecondaryRootName("users", fmt.Sprintf("idx%02d", i))
		if after[rootName] != before[rootName] {
			t.Fatalf("secondary root %q changed from %d to %d for non-indexed update", rootName, before[rootName], after[rootName])
		}
	}
}

func batchManyUnchangedIndexesForTest(n int) []IndexDefinition {
	indexes := make([]IndexDefinition, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("idx%02d", i)
		indexes[i] = IndexDefinition{
			Name:      name,
			Field:     fmt.Sprintf("f%02d", i),
			ValueType: IndexValueString,
		}
	}
	return indexes
}

func batchManyUnchangedDocumentForTest(indexCount int, extra map[string]string) []byte {
	var builder strings.Builder
	builder.WriteByte('{')
	first := true
	writeField := func(name, value string) {
		if !first {
			builder.WriteByte(',')
		}
		first = false
		fmt.Fprintf(&builder, "%q:%q", name, value)
	}
	for i := 0; i < indexCount; i++ {
		writeField(fmt.Sprintf("f%02d", i), fmt.Sprintf("v%02d", i))
	}
	extraNames := make([]string, 0, len(extra))
	for name := range extra {
		extraNames = append(extraNames, name)
	}
	sort.Strings(extraNames)
	for _, name := range extraNames {
		writeField(name, extra[name])
	}
	builder.WriteByte('}')
	return []byte(builder.String())
}

func batchManyUnchangedRootIDsForTest(t *testing.T, d *backenddb.DB, collectionName string, rootNames []string) map[string]uint64 {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, collectionName)
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		t.Fatal("missing catalog")
	}
	out := make(map[string]uint64, len(rootNames))
	for _, rootName := range rootNames {
		rootID := catalog.rootID(rootName)
		if rootID == 0 {
			t.Fatalf("root %q was not persisted", rootName)
		}
		out[rootName] = rootID
	}
	return out
}
