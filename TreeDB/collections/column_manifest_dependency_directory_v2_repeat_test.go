package collections

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// This is the production repeated-manifest path, not a synthetic resource-set
// merge. V2 is present before Open, and a new part must reuse a physical asset.
func TestColumnManifestDependencyDirectoryV2RepeatedPublicAppend(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1, backenddb.RequiredFeatureDependencyDirectoryV2}}); err != nil {
		t.Fatal(err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() {
		if d != nil {
			if err := d.Close(); err != nil {
				t.Errorf("cleanup Close: %v", err)
			}
		}
	}()
	mgr := NewCollectionManager(d)
	_, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: testColumnStoreConfig(nil)}})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	col := openColumnStoreCollectionM10B(t, d, mgr)
	insert := func(i int) {
		t.Helper()
		id := fmt.Sprintf("e%d", i)
		doc := fmt.Sprintf(`{"time_us":%d,"kind":"like","did":"d%d"}`, i+1, i)
		if _, err := col.InsertBatch([][]byte{[]byte(id)}, [][]byte{[]byte(doc)}); err != nil {
			t.Fatalf("InsertBatch %d: %v", i, err)
		}
		if err := d.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint %d: %v", i, err)
		}
		work := col.LastInsertStats().ColumnPublishFinalizeCandidateResourceWork
		if work.AppendOnlyFastPath == 0 || work.NewlyAdmittedObligations == 0 {
			t.Fatalf("round %d did not use production append-only admission: %+v", i, work)
		}
	}
	insert(0)
	first, err := col.loadColumnAssetRewriteManifestState()
	if err != nil {
		t.Fatal(err)
	}
	firstRefs := make(map[ColumnAssetRef]bool)
	firstFiles := make(map[string]os.FileInfo)
	for _, part := range first.manifest.Parts {
		firstRefs[part.AssetRef] = true
		path, err := columnAssetSegmentPath(backenddb.ColumnAssetRootDirPath(dir), part.AssetRef)
		if err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		firstFiles[path] = info
		t.Logf("first physical column asset: ref=%+v path=%s bytes=%d", part.AssetRef, path, info.Size())
	}
	if len(firstFiles) == 0 {
		t.Fatal("first publication has no physical column asset")
	}
	old := d.AcquireSnapshot()
	if old == nil {
		t.Fatal("nil first Snapshot")
	}
	defer func() {
		if old != nil {
			if err := old.Close(); err != nil {
				t.Errorf("cleanup old Snapshot: %v", err)
			}
		}
	}()
	oldCatalog, err := col.catalogForSnapshot(old)
	if err != nil {
		t.Fatal(err)
	}
	oldValue, found, err := collectionGetAppendAtCatalogRoot(old, oldCatalog, collectionPrimaryRootName("events"), []byte("e0"), nil)
	if err != nil || !found {
		t.Fatalf("old e0 missing: %v", err)
	}
	oldValue = bytes.Clone(oldValue)
	insert(1)
	second, err := col.loadColumnAssetRewriteManifestState()
	if err != nil {
		t.Fatal(err)
	}
	sharedNewPart := false
	for _, part := range second.manifest.Parts {
		if firstRefs[part.AssetRef] {
			continue
		}
		path, err := columnAssetSegmentPath(backenddb.ColumnAssetRootDirPath(dir), part.AssetRef)
		if err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if before := firstFiles[path]; before != nil && os.SameFile(before, info) && info.Size() > before.Size() {
			sharedNewPart = true
			t.Logf("new logical part appended to original physical asset: ref=%+v path=%s bytes=%d->%d", part.AssetRef, path, before.Size(), info.Size())
		}
	}
	if !sharedNewPart {
		t.Fatal("same-physical bound-plus-delta premise not reached: new parts did not append to an original asset")
	}
	got, found, err := collectionGetAppendAtCatalogRoot(old, oldCatalog, collectionPrimaryRootName("events"), []byte("e0"), nil)
	if err != nil || !found || !bytes.Equal(got, oldValue) {
		t.Fatalf("old e0 changed: found=%t err=%v", found, err)
	}
	_, found, err = collectionGetAppendAtCatalogRoot(old, oldCatalog, collectionPrimaryRootName("events"), []byte("e1"), nil)
	if err != nil || found {
		t.Fatalf("old Snapshot saw second row: found=%t err=%v", found, err)
	}
	oldRecords, err := loadColumnManifestRecordsFromRoot(old, oldCatalog.rootID(collectionColumnManifestRootName("events")))
	if err != nil {
		t.Fatal(err)
	}
	if !equalColumnManifestRecords(oldRecords, first.records) {
		t.Fatal("old manifest changed after second publication")
	}
	lastLSN := d.State().AppliedCommandLSN
	assertColumnManifestStateM10B(t, col, 2, lastLSN)
	for i := 0; i < 2; i++ {
		assertCollectionDocument(t, col, fmt.Sprintf("e%d", i), fmt.Sprintf(`{"time_us":%d,"kind":"like","did":"d%d"}`, i+1, i))
	}
	oldCatalog = nil
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	old = nil
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
	d = openCollectionCommandWALDB(t, dir)
	col = openColumnStoreCollectionM10B(t, d)
	assertColumnManifestStateM10B(t, col, 2, lastLSN)
	for i := 0; i < 2; i++ {
		assertCollectionDocument(t, col, fmt.Sprintf("e%d", i), fmt.Sprintf(`{"time_us":%d,"kind":"like","did":"d%d"}`, i+1, i))
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
}
