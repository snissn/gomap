//go:build windows

package collections

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestColumnStoreCommandWALFirstCentralPublishFailsWithoutNamespacePersistenceM10B(t *testing.T) {
	if rootpublication.StableRelativeNamespaceSupported() {
		t.Fatal("Windows unexpectedly reports exact retained-parent namespace persistence support")
	}

	dir, baseLSN := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	col := openColumnStoreCollectionM10B(t, d, mgr)
	assertColumnStoreManifestUnpublishedOnWindows(t, col)

	insertedIDs, err := col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1"}`),
	})
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("InsertBatch error=%v, want ErrNamespacePersistenceUnsupported", err)
	}
	if len(insertedIDs) != 0 {
		t.Fatalf("InsertBatch inserted=%d, want 0 on unsupported central publish", len(insertedIDs))
	}

	assertDBAppliedCommandLSNM10B(t, d, baseLSN)
	assertColumnStoreDocumentMissingM10B(t, col, "e1")
	assertColumnStorePersistedDocumentMissingM10B(t, d, "events", "e1")
	assertColumnStoreWriteDomainEmptyM10B(t, col)
	reopened := openColumnStoreCollectionM10B(t, d, mgr)
	assertColumnStoreManifestUnpublishedOnWindows(t, reopened)
	if got := d.ColumnAssetIdentityPinRegistry().ActivePins(); got != 0 {
		t.Fatalf("unsupported central publish retained %d column asset pins", got)
	}
	if got := d.ColumnAssetIdentityPinRegistry().ActiveIdentities(); got != 0 {
		t.Fatalf("unsupported central publish retained %d column asset identities", got)
	}
}

func assertColumnStoreManifestUnpublishedOnWindows(t testing.TB, col *Collection) {
	t.Helper()
	cfg := col.Meta().Options.ColumnStore
	if cfg == nil || cfg.ActiveManifest != nil || cfg.RecoveryAuthoritativeManifest != nil || cfg.RecoveryAuthoritativeAppliedCommandLSN != 0 {
		t.Fatalf("central publish leaked column manifest metadata: %+v", cfg)
	}
	id, ok := col.ColumnStoreCacheIdentity()
	if !ok || id.ManifestRoot != 0 || id.ManifestGeneration != 0 || id.ManifestChecksum != 0 || id.RecoveryAuthoritativeGeneration != 0 || id.RecoveryAuthoritativeChecksum != 0 || id.RecoveryAuthoritativeAppliedCommandLSN != 0 {
		t.Fatalf("central publish leaked manifest/root visibility: %+v ok=%v", id, ok)
	}
}
