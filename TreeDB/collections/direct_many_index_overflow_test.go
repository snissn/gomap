package collections

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionDirectUpdateMinimizesIndexOrdinalBeyondMaskWidth(t *testing.T) {
	const indexCount = 65
	const targetOrdinal = 64

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
		Indexes: directOverflowIndexes(indexCount),
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{directOverflowDocument(indexCount, -1, "", "")},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rootNames := []string{
		collectionPrimaryRootName("users"),
		collectionIndexStateRootName("users"),
	}
	for i := 0; i < indexCount; i++ {
		rootNames = append(rootNames, collectionSecondaryRootName("users", directOverflowIndexName(i)))
	}
	before := directOverflowRootIDs(t, d, rootNames)

	matched, modified, err := col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return directOverflowDocument(indexCount, targetOrdinal, "target-new", "primary changed"), true, nil
	})
	if err != nil {
		t.Fatalf("direct update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("direct update matched/modified=%v/%v want true/true", matched, modified)
	}
	stats := col.LastUpdateStats()
	if got, want := stats.MaskFallbacks, 1; got != want {
		t.Fatalf("direct update fast-mask fallbacks=%d want %d", got, want)
	}
	managerStats := mgr.StatsSnapshot()
	if got, want := managerStats.UpdateBatchMaskFallbacks, uint64(1); got != want {
		t.Fatalf("manager fast-mask fallbacks=%d want %d", got, want)
	}

	after := directOverflowRootIDs(t, d, rootNames)
	for _, rootName := range []string{
		collectionPrimaryRootName("users"),
		collectionIndexStateRootName("users"),
		collectionSecondaryRootName("users", directOverflowIndexName(targetOrdinal)),
	} {
		if after[rootName] == before[rootName] {
			t.Fatalf("root %q did not change for direct update", rootName)
		}
	}
	for i := 0; i < indexCount; i++ {
		if i == targetOrdinal {
			continue
		}
		rootName := collectionSecondaryRootName("users", directOverflowIndexName(i))
		if after[rootName] != before[rootName] {
			t.Fatalf("secondary root %q changed from %d to %d for unrelated index", rootName, before[rootName], after[rootName])
		}
	}

	directOverflowRequireIndexIDs(t, col, directOverflowIndexName(targetOrdinal), fmt.Sprintf("v%02d", targetOrdinal))
	directOverflowRequireIndexIDs(t, col, directOverflowIndexName(targetOrdinal), "target-new", "u1")
}

func directOverflowIndexes(n int) []IndexDefinition {
	indexes := make([]IndexDefinition, n)
	for i := 0; i < n; i++ {
		indexes[i] = IndexDefinition{
			Name:      directOverflowIndexName(i),
			Field:     fmt.Sprintf("f%02d", i),
			ValueType: IndexValueString,
		}
	}
	return indexes
}

func directOverflowIndexName(ordinal int) string {
	return fmt.Sprintf("idx%02d", ordinal)
}

func directOverflowDocument(indexCount, overrideOrdinal int, overrideValue, note string) []byte {
	var builder strings.Builder
	builder.WriteByte('{')
	for i := 0; i < indexCount; i++ {
		if i > 0 {
			builder.WriteByte(',')
		}
		value := fmt.Sprintf("v%02d", i)
		if i == overrideOrdinal {
			value = overrideValue
		}
		fmt.Fprintf(&builder, "%q:%q", fmt.Sprintf("f%02d", i), value)
	}
	if note != "" {
		fmt.Fprintf(&builder, ",%q:%q", "note", note)
	}
	builder.WriteByte('}')
	return []byte(builder.String())
}

func directOverflowRootIDs(t *testing.T, d *backenddb.DB, rootNames []string) map[string]uint64 {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, "users")
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		t.Fatal("missing catalog")
	}
	roots := make(map[string]uint64, len(rootNames))
	for _, rootName := range rootNames {
		rootID := catalog.rootID(rootName)
		if rootID == 0 {
			t.Fatalf("root %q was not persisted", rootName)
		}
		roots[rootName] = rootID
	}
	return roots
}

func directOverflowRequireIndexIDs(t *testing.T, col *Collection, indexName string, value any, want ...string) {
	t.Helper()
	ids, err := col.FindByIndexValue(indexName, value)
	if err != nil {
		t.Fatalf("find index %s=%v: %v", indexName, value, err)
	}
	if len(ids) != len(want) {
		t.Fatalf("index %s=%v ids=%q want %q", indexName, value, ids, want)
	}
	for i := range want {
		if !bytes.Equal(ids[i], []byte(want[i])) {
			t.Fatalf("index %s=%v ids=%q want %q", indexName, value, ids, want)
		}
	}
}
