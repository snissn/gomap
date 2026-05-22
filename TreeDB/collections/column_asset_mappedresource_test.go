package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

func TestColumnAssetReadCacheReportsMappedResourceHandlesGateA1736(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	payload := []byte("gate-a-typed-row-asset-resource")
	ref, err := writeColumnPhysicalAssetToManager(root, *normalized, payload, 7, 3)
	if err != nil {
		t.Fatalf("writeColumnPhysicalAssetToManager: %v", err)
	}

	manager := mappedresource.NewManager()
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(root, normalized.AssetManager.Namespace, ColumnAssetReadIntegritySkipChecksums)
	if err != nil {
		t.Fatalf("new read cache: %v", err)
	}
	scope := mappedresource.Scope{
		Kind:       mappedresource.ScopeSnapshot,
		ID:         "snapshot-7",
		Namespace:  normalized.AssetManager.Namespace,
		Collection: "events",
		Generation: 7,
		Reason:     "gate-a-test",
	}
	if err := readCache.useMappedResourceManager(manager, scope, "typed-row-asset-read"); err != nil {
		t.Fatalf("useMappedResourceManager: %v", err)
	}

	raw, err := readCache.read(ref, nil)
	if err != nil {
		_ = readCache.close()
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(raw, payload) {
		_ = readCache.close()
		t.Fatalf("raw=%q want %q", raw, payload)
	}
	stats := readCache.mappedResourceStats()
	if stats.ActiveHandles != 1 || stats.ActiveHeapCopyBytes != int64(len(payload)) || stats.TotalAcquires != 1 {
		_ = readCache.close()
		t.Fatalf("mapped resource stats after read: %+v", stats)
	}
	if got := stats.ValidationModeReads[mappedresource.ValidationSkipChecksum]; got != 1 {
		_ = readCache.close()
		t.Fatalf("validation mode count=%d want 1 stats=%+v", got, stats)
	}
	pins := readCache.mappedResourcePins()
	if len(pins) != 1 {
		_ = readCache.close()
		t.Fatalf("pins=%d want 1", len(pins))
	}
	pin := pins[0]
	if pin.Key.Class != mappedresource.ClassTypedRowAsset || pin.Key.Namespace != ref.Namespace || pin.Key.FileID != ref.FileID || pin.Key.Offset != ref.Offset || pin.Key.Length != ref.Length || pin.Key.Checksum != uint64(ref.Checksum) {
		_ = readCache.close()
		t.Fatalf("unexpected pin key: %+v ref=%+v", pin.Key, ref)
	}
	if pin.Scope.Kind != mappedresource.ScopeSnapshot || pin.Scope.ID != "snapshot-7" || pin.Reason != "typed-row-asset-read" {
		_ = readCache.close()
		t.Fatalf("unexpected pin scope/reason: %+v", pin)
	}

	if err := readCache.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	stats = manager.Stats()
	if stats.ActiveHandles != 0 || stats.ActiveHeapCopyBytes != 0 || stats.TotalReleases != 1 {
		t.Fatalf("mapped resource stats after close: %+v", stats)
	}
	if pins := manager.PinSummary(); len(pins) != 0 {
		t.Fatalf("pins after close=%d want 0", len(pins))
	}
}

func TestColumnAssetReadCacheMappedResourceHeapHandlesOwnStableBytesGateA1736(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	firstPayload := []byte("aaaaaaaaaaaaaaaa")
	secondPayload := []byte("bbbbbbbbbbbbbbbb")
	first, err := writeColumnPhysicalAssetToManager(root, *normalized, firstPayload, 7, 3)
	if err != nil {
		t.Fatalf("write first asset: %v", err)
	}
	second, err := writeColumnPhysicalAssetToManager(root, *normalized, secondPayload, 7, 4)
	if err != nil {
		t.Fatalf("write second asset: %v", err)
	}

	manager := mappedresource.NewManager()
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(root, normalized.AssetManager.Namespace, ColumnAssetReadIntegritySkipChecksums)
	if err != nil {
		t.Fatalf("new read cache: %v", err)
	}
	defer readCache.close()
	if err := readCache.useMappedResourceManager(manager, mappedresource.Scope{Kind: mappedresource.ScopeSnapshot, ID: "snapshot-7", Namespace: normalized.AssetManager.Namespace}, "typed-row-asset-read"); err != nil {
		t.Fatalf("useMappedResourceManager: %v", err)
	}
	if _, err := readCache.read(first, nil); err != nil {
		t.Fatalf("read first: %v", err)
	}
	if len(readCache.resourceHandles) != 1 {
		t.Fatalf("resource handles after first read=%d want 1", len(readCache.resourceHandles))
	}
	firstHandle := readCache.resourceHandles[0]
	if _, err := readCache.read(second, nil); err != nil {
		t.Fatalf("read second: %v", err)
	}
	if got := firstHandle.Bytes(); !bytes.Equal(got, firstPayload) {
		t.Fatalf("first handle bytes after second read=%q want %q", got, firstPayload)
	}
}

func TestColumnAssetReadCacheMappedResourceScopeValidationGateA1736(t *testing.T) {
	readCache := columnPhysicalAssetReadCache{namespace: "events"}
	manager := mappedresource.NewManager()
	if err := readCache.useMappedResourceManager(manager, mappedresource.Scope{Kind: mappedresource.ScopeSnapshot}, "bad"); err == nil {
		t.Fatal("useMappedResourceManager accepted lifetime-less scope")
	}
}
