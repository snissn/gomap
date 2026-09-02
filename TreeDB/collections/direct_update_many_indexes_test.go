package collections

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionDirectUpdateManyIndexesTouchesOnlyChangedSecondaryRoot(t *testing.T) {
	const indexCount = 16
	const targetOrdinal = 9

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
		Indexes: directManyIndexesForTest(indexCount),
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{directManyIndexDocumentForTest(indexCount, nil, nil)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rootNames := []string{collectionPrimaryRootName("users")}
	for i := 0; i < indexCount; i++ {
		rootNames = append(rootNames, collectionSecondaryRootName("users", fmt.Sprintf("idx%02d", i)))
	}
	before := directManyRootIDsForTest(t, d, "users", rootNames)
	replacement := directManyIndexDocumentForTest(indexCount, map[int]string{
		targetOrdinal: "target-new",
	}, map[string]string{"note": "primary changed"})

	matched, modified, err := col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return replacement, true, nil
	})
	if err != nil {
		t.Fatalf("direct update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("direct update matched/modified=%v/%v want true/true", matched, modified)
	}
	after := directManyRootIDsForTest(t, d, "users", rootNames)
	if after[collectionPrimaryRootName("users")] == before[collectionPrimaryRootName("users")] {
		t.Fatalf("primary root did not change for direct document replacement")
	}
	for i := 0; i < indexCount; i++ {
		rootName := collectionSecondaryRootName("users", fmt.Sprintf("idx%02d", i))
		changed := after[rootName] != before[rootName]
		if i == targetOrdinal {
			if !changed {
				t.Fatalf("target secondary root %q did not change", rootName)
			}
			continue
		}
		if changed {
			t.Fatalf("unrelated secondary root %q changed from %d to %d", rootName, before[rootName], after[rootName])
		}
	}
	newIDs, err := col.FindByIndexValue(fmt.Sprintf("idx%02d", targetOrdinal), "target-new")
	if err != nil {
		t.Fatalf("find new target value: %v", err)
	}
	if len(newIDs) != 1 || !bytes.Equal(newIDs[0], []byte("u1")) {
		t.Fatalf("new target ids=%q want [u1]", newIDs)
	}
	oldIDs, err := col.FindByIndexValue(fmt.Sprintf("idx%02d", targetOrdinal), fmt.Sprintf("v%02d", targetOrdinal))
	if err != nil {
		t.Fatalf("find old target value: %v", err)
	}
	if len(oldIDs) != 0 {
		t.Fatalf("old target ids=%q want none", oldIDs)
	}
}

func TestCollectionDirectUpdateManyIndexesSkipsAllSecondaryRootsWhenEffectiveValuesUnchanged(t *testing.T) {
	const indexCount = 16

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
		Indexes: directManyIndexesForTest(indexCount),
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{directManyIndexDocumentForTest(indexCount, nil, nil)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rootNames := []string{collectionPrimaryRootName("users")}
	for i := 0; i < indexCount; i++ {
		rootNames = append(rootNames, collectionSecondaryRootName("users", fmt.Sprintf("idx%02d", i)))
	}
	before := directManyRootIDsForTest(t, d, "users", rootNames)
	replacement := directManyIndexDocumentForTest(indexCount, nil, map[string]string{"note": "primary changed"})

	matched, modified, err := col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return replacement, true, nil
	})
	if err != nil {
		t.Fatalf("direct update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("direct update matched/modified=%v/%v want true/true", matched, modified)
	}
	after := directManyRootIDsForTest(t, d, "users", rootNames)
	if after[collectionPrimaryRootName("users")] == before[collectionPrimaryRootName("users")] {
		t.Fatalf("primary root did not change for direct document replacement")
	}
	for i := 0; i < indexCount; i++ {
		rootName := collectionSecondaryRootName("users", fmt.Sprintf("idx%02d", i))
		if after[rootName] != before[rootName] {
			t.Fatalf("secondary root %q changed from %d to %d for same-effective indexed values", rootName, before[rootName], after[rootName])
		}
	}
}

func directManyIndexesForTest(n int) []IndexDefinition {
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

func directManyIndexDocumentForTest(indexCount int, overrides map[int]string, extra map[string]string) []byte {
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
		value := fmt.Sprintf("v%02d", i)
		if override, ok := overrides[i]; ok {
			value = override
		}
		writeField(fmt.Sprintf("f%02d", i), value)
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

func directManyRootIDsForTest(t *testing.T, d *backenddb.DB, collectionName string, rootNames []string) map[string]uint64 {
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
