package collections

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionDirectUpdateManyIndexesSkipsUnchangedUniqueRoot(t *testing.T) {
	const indexCount = 12
	const targetOrdinal = 3
	const uniqueOrdinal = 7

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
		Indexes: directManyUniqueSkipIndexes(indexCount, uniqueOrdinal),
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{directManyUniqueSkipDocument(indexCount, map[int]string{
			uniqueOrdinal: "stable-unique",
		}, nil)},
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
	before := directManyUniqueSkipRootIDs(t, d, "users", rootNames)

	matched, modified, err := col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return directManyUniqueSkipDocument(indexCount, map[int]string{
			targetOrdinal: "target-new",
			uniqueOrdinal: "stable-unique",
		}, map[string]string{"note": "primary changed"}), true, nil
	})
	if err != nil {
		t.Fatalf("direct update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("direct update matched/modified=%v/%v want true/true", matched, modified)
	}

	after := directManyUniqueSkipRootIDs(t, d, "users", rootNames)
	for _, rootName := range []string{
		collectionPrimaryRootName("users"),
		collectionIndexStateRootName("users"),
		collectionSecondaryRootName("users", fmt.Sprintf("idx%02d", targetOrdinal)),
	} {
		if after[rootName] == before[rootName] {
			t.Fatalf("root %q did not change for direct update", rootName)
		}
	}
	for i := 0; i < indexCount; i++ {
		if i == targetOrdinal {
			continue
		}
		rootName := collectionSecondaryRootName("users", fmt.Sprintf("idx%02d", i))
		if after[rootName] != before[rootName] {
			t.Fatalf("secondary root %q changed from %d to %d for unrelated/unchanged index", rootName, before[rootName], after[rootName])
		}
	}

	directManyUniqueSkipRequireIndexIDs(t, col, fmt.Sprintf("idx%02d", uniqueOrdinal), "stable-unique", "u1")
	directManyUniqueSkipRequireIndexIDs(t, col, fmt.Sprintf("idx%02d", targetOrdinal), fmt.Sprintf("v%02d", targetOrdinal))
	directManyUniqueSkipRequireIndexIDs(t, col, fmt.Sprintf("idx%02d", targetOrdinal), "target-new", "u1")
}

func directManyUniqueSkipIndexes(n, uniqueOrdinal int) []IndexDefinition {
	indexes := make([]IndexDefinition, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("idx%02d", i)
		indexes[i] = IndexDefinition{
			Name:      name,
			Field:     fmt.Sprintf("f%02d", i),
			ValueType: IndexValueString,
			Unique:    i == uniqueOrdinal,
		}
	}
	return indexes
}

func directManyUniqueSkipDocument(indexCount int, overrides map[int]string, extra map[string]string) []byte {
	var builder strings.Builder
	builder.WriteByte('{')
	first := true
	writeStringField := func(name, value string) {
		if !first {
			builder.WriteByte(',')
		}
		first = false
		fmt.Fprintf(&builder, "%q:%q", name, value)
	}
	for i := 0; i < indexCount; i++ {
		value := fmt.Sprintf("v%02d", i)
		if override, ok := overrides[i]; ok {
			value = override
		}
		writeStringField(fmt.Sprintf("f%02d", i), value)
	}
	extraNames := make([]string, 0, len(extra))
	for name := range extra {
		extraNames = append(extraNames, name)
	}
	sort.Strings(extraNames)
	for _, name := range extraNames {
		writeStringField(name, extra[name])
	}
	builder.WriteByte('}')
	return []byte(builder.String())
}

func directManyUniqueSkipRootIDs(t *testing.T, d *backenddb.DB, collectionName string, rootNames []string) map[string]uint64 {
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

func directManyUniqueSkipRequireIndexIDs(t *testing.T, col *Collection, indexName string, value any, want ...string) {
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
