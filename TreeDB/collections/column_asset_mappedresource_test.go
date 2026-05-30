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
	if got := firstHandle.Bytes(); !bytes.Equal(got, firstPayload) {
		t.Fatalf("first handle bytes before scratch reuse=%q want %q", got, firstPayload)
	}
	if _, err := readCache.read(second, nil); err != nil {
		t.Fatalf("read second: %v", err)
	}
	if !firstHandle.Released() {
		t.Fatal("first heap handle is still active after scratch-backed read reuse")
	}
	if len(readCache.resourceHandles) != 1 || readCache.resourceHandles[0] == firstHandle {
		t.Fatalf("resource handles after second read=%d first retained=%v", len(readCache.resourceHandles), len(readCache.resourceHandles) == 1 && readCache.resourceHandles[0] == firstHandle)
	}
	stats := manager.Stats()
	if stats.ActiveHandles != 1 || stats.ActiveHeapCopyBytes != int64(len(secondPayload)) || stats.TotalReleases != 1 {
		t.Fatalf("stats after second scratch-backed read: %+v", stats)
	}
}

func TestColumnAssetReadCacheMappedResourceViewHandlesDoNotAccumulateGateA1736(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	payload := []byte("mapped-view-handle-payload")
	ref, err := writeColumnPhysicalAssetToManager(root, *normalized, payload, 7, 3)
	if err != nil {
		t.Fatalf("write asset: %v", err)
	}

	manager := mappedresource.NewManager()
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(root, normalized.AssetManager.Namespace, ColumnAssetReadIntegritySkipChecksums)
	if err != nil {
		t.Fatalf("new read cache: %v", err)
	}
	defer readCache.close()
	readCache.returnViews = true
	if err := readCache.useMappedResourceManager(manager, mappedresource.Scope{Kind: mappedresource.ScopeSnapshot, ID: "snapshot-7", Namespace: normalized.AssetManager.Namespace}, "typed-row-asset-read"); err != nil {
		t.Fatalf("useMappedResourceManager: %v", err)
	}
	if _, err := readCache.read(ref, nil); err != nil {
		t.Fatalf("read first: %v", err)
	}
	if len(readCache.resourceHandles) != 1 {
		t.Fatalf("resource handles after first view read=%d want 1", len(readCache.resourceHandles))
	}
	firstHandle := readCache.resourceHandles[0]
	if firstHandle.Source() != mappedresource.SourceMapped {
		t.Skip("mmap views are unavailable on this platform")
	}
	if _, err := readCache.read(ref, nil); err != nil {
		t.Fatalf("read second: %v", err)
	}
	if firstHandle.Released() {
		t.Fatal("first mapped view handle was released while its mmap-backed bytes remain readable")
	}
	if len(readCache.resourceHandles) != 1 || readCache.resourceHandles[0] != firstHandle {
		t.Fatalf("resource handles after duplicate view read=%d first retained=%v", len(readCache.resourceHandles), len(readCache.resourceHandles) == 1 && readCache.resourceHandles[0] == firstHandle)
	}
	stats := manager.Stats()
	if stats.ActiveHandles != 1 || stats.ActiveMappedBytes != int64(len(payload)) || stats.TotalAcquires != 1 || stats.TotalReleases != 0 {
		t.Fatalf("stats after repeated view read: %+v", stats)
	}
}

func TestColumnAssetReadCacheIntegrityModesReportMappedResourceValidation(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	payload := []byte("mapped-resource-integrity-mode-payload")
	ref, err := writeColumnPhysicalAssetToManager(root, *normalized, payload, 7, 3)
	if err != nil {
		t.Fatalf("writeColumnPhysicalAssetToManager: %v", err)
	}

	tests := []struct {
		name      string
		integrity ColumnAssetReadIntegrity
		wantMode  mappedresource.ValidationMode
	}{
		{name: "verify", integrity: ColumnAssetReadIntegrityVerify, wantMode: mappedresource.ValidationVerify},
		{name: "cached_verify", integrity: ColumnAssetReadIntegrityCachedVerify, wantMode: mappedresource.ValidationCachedVerify},
		{name: "skip_checksums", integrity: ColumnAssetReadIntegritySkipChecksums, wantMode: mappedresource.ValidationSkipChecksum},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := mappedresource.NewManager()
			readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(root, normalized.AssetManager.Namespace, tt.integrity)
			if err != nil {
				t.Fatalf("new read cache: %v", err)
			}
			if err := readCache.useMappedResourceManager(manager, mappedresource.Scope{Kind: mappedresource.ScopeSnapshot, ID: "snapshot-" + tt.name, Namespace: normalized.AssetManager.Namespace}, "integrity-mode-read"); err != nil {
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
			if got := stats.ValidationModeReads[tt.wantMode]; got != 1 {
				_ = readCache.close()
				t.Fatalf("validation mode %q count=%d want 1 stats=%+v", tt.wantMode, got, stats)
			}
			if len(stats.ValidationModeReads) != 1 {
				_ = readCache.close()
				t.Fatalf("validation mode map=%+v want only %q", stats.ValidationModeReads, tt.wantMode)
			}
			if got := columnAssetReadIntegrityLabel(tt.integrity); got != string(tt.wantMode) {
				_ = readCache.close()
				t.Fatalf("integrity label=%q want mappedresource validation label %q", got, tt.wantMode)
			}
			if err := readCache.close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			if stats := manager.Stats(); stats.ActiveHandles != 0 || stats.ActiveHeapCopyBytes != 0 || stats.TotalReleases != 1 {
				t.Fatalf("mapped resource stats after close: %+v", stats)
			}
		})
	}
}

func TestColumnAssetReadCacheMappedResourceScopeValidationGateA1736(t *testing.T) {
	readCache := columnPhysicalAssetReadCache{namespace: "events"}
	manager := mappedresource.NewManager()
	if err := readCache.useMappedResourceManager(manager, mappedresource.Scope{Kind: mappedresource.ScopeSnapshot}, "bad"); err == nil {
		t.Fatal("useMappedResourceManager accepted lifetime-less scope")
	}
}
