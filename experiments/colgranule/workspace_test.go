package colgranule

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestColumnWorkspacePublishesReopensAndRunsJSONBenchQueries(t *testing.T) {
	ds := syntheticJSONBenchDataset(256)
	part, err := BuildJSONBenchColumnPart(ds, 32)
	if err != nil {
		t.Fatalf("BuildJSONBenchColumnPart: %v", err)
	}
	dir := t.TempDir()
	workspace, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace: %v", err)
	}
	entry, err := workspace.PublishPart(part, ds.Dictionaries)
	if err != nil {
		t.Fatalf("PublishPart: %v", err)
	}
	if entry.PartID != part.Descriptor.PartID || entry.Rows != ds.Rows || entry.AssetBytes == 0 {
		t.Fatalf("bad manifest entry=%+v part rows=%d", entry, ds.Rows)
	}
	if err := workspace.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"})
	if err != nil {
		t.Fatalf("reopen workspace: %v", err)
	}
	defer reopened.Close()
	manifest := reopened.Manifest()
	if manifest.Collection != "jsonbench" || len(manifest.Parts) != 1 || manifest.Parts[0].PartID != part.Descriptor.PartID {
		t.Fatalf("bad reopened manifest=%+v", manifest)
	}

	first, err := reopened.LoadPartWithOptions(part.Descriptor.PartID, ColumnPartImageReadOptions{})
	if err != nil {
		t.Fatalf("LoadPart cold: %v", err)
	}
	if first.CacheState != "cold" || first.CacheStats.MarkCache.Misses == 0 || first.CacheStats.DecodedCache.Misses == 0 {
		t.Fatalf("cold cache stats=%+v state=%s", first.CacheStats, first.CacheState)
	}
	second, err := reopened.LoadPartWithOptions(part.Descriptor.PartID, ColumnPartImageReadOptions{})
	if err != nil {
		t.Fatalf("LoadPart warm: %v", err)
	}
	if second.CacheState != "warm" || second.CacheStats.MarkCache.Hits == 0 || second.CacheStats.DecodedCache.Hits == 0 {
		t.Fatalf("warm cache stats=%+v state=%s", second.CacheStats, second.CacheState)
	}

	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		t.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	queries := []struct {
		name string
		raw  func(JSONBenchDataset, queryCodeSet) (int, uint64)
		part jsonBenchPartQueryRunner
	}{
		{"Q1", runJSONBenchQ1, runJSONBenchPartQ1},
		{"Q2", runJSONBenchQ2, runJSONBenchPartQ2},
		{"Q3", runJSONBenchQ3, runJSONBenchPartQ3},
		{"Q4", runJSONBenchQ4, runJSONBenchPartQ4},
		{"Q5", runJSONBenchQ5, runJSONBenchPartQ5},
	}
	scratch := &jsonBenchPartQueryScratch{
		scanner:   first.Part.NewScanner(),
		projected: make(map[string][]int64, 6),
	}
	for _, query := range queries {
		rawRows, rawDigest := query.raw(ds, codes)
		partRows, partDigest, diagnostics, err := query.part(first.Part, codes, scratch)
		if err != nil {
			t.Fatalf("%s workspace query: %v", query.name, err)
		}
		if partRows != rawRows || partDigest != rawDigest {
			t.Fatalf("%s rows/digest=(%d,%d) raw=(%d,%d)", query.name, partRows, partDigest, rawRows, rawDigest)
		}
		if diagnostics.RowsScanned == 0 || diagnostics.AggregateKernel == "" {
			t.Fatalf("%s bad diagnostics=%+v", query.name, diagnostics)
		}
	}
}

func TestColumnWorkspaceCreatesIsolatedNamespace(t *testing.T) {
	dir := t.TempDir()
	workspace, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace: %v", err)
	}
	namespace := workspace.Namespace()
	if err := workspace.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, path := range []string{
		namespace.ManifestDir,
		namespace.AssetDir,
		namespace.SegmentDir,
		namespace.IndexDir,
		namespace.PreparedDir,
		namespace.QuarantineDir,
		namespace.TempDir,
	} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("Stat(%s): %v", path, err)
		}
		if !info.IsDir() {
			t.Fatalf("%s is not a directory", path)
		}
	}
	if _, err := os.Stat(filepath.Join(namespace.ManifestDir, columnWorkspaceManifestFile)); err != nil {
		t.Fatalf("workspace manifest in isolated manifest dir: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(namespace.ManifestDir, columnWorkspaceManifestFile))
	if err != nil {
		t.Fatalf("ReadFile workspace manifest: %v", err)
	}
	if !bytes.HasPrefix(data, []byte(columnWorkspaceManifestBinaryMagic)) {
		t.Fatalf("workspace manifest magic=%q want binary %q", data[:min(len(data), 4)], columnWorkspaceManifestBinaryMagic)
	}
	if _, err := os.Stat(filepath.Join(namespace.SegmentDir, "column-assets-000001.seg")); err != nil {
		t.Fatalf("asset segment in isolated segment dir: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, columnWorkspaceManifestFile)); !os.IsNotExist(err) {
		t.Fatalf("root workspace manifest err=%v want not exist", err)
	}
}

func TestColumnWorkspaceManifestSyncModeControlsFsyncM9D(t *testing.T) {
	syncs := 0
	syncHook := func(file *os.File) error {
		syncs++
		return nil
	}

	var nilWorkspace *ColumnWorkspace
	if got := nilWorkspace.ManifestSyncMode(); got != "" {
		t.Fatalf("nil ManifestSyncMode=%q want empty", got)
	}

	manifest, err := NewColumnCollectionManifest("jsonbench", partTestOptions([]SortKeyColumn{{Column: "id"}}), nil, nil, nil)
	if err != nil {
		t.Fatalf("NewColumnCollectionManifest: %v", err)
	}

	durable, err := OpenColumnWorkspace(t.TempDir(), ColumnWorkspaceOptions{
		Collection:   "jsonbench",
		syncTempFile: syncHook,
	})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace durable: %v", err)
	}
	if syncs != 1 {
		t.Fatalf("durable open syncs=%d want 1", syncs)
	}
	syncs = 0
	if err := durable.SaveCollectionManifest(manifest); err != nil {
		t.Fatalf("SaveCollectionManifest durable: %v", err)
	}
	if syncs != 1 {
		t.Fatalf("durable collection manifest syncs=%d want 1", syncs)
	}
	if err := durable.Close(); err != nil {
		t.Fatalf("Close durable: %v", err)
	}

	syncs = 0
	relaxed, err := OpenColumnWorkspace(t.TempDir(), ColumnWorkspaceOptions{
		Collection:       "jsonbench",
		ManifestSyncMode: ColumnWorkspaceManifestSyncDisabledForBenchmark,
		syncTempFile:     syncHook,
	})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace relaxed: %v", err)
	}
	if syncs != 0 {
		t.Fatalf("relaxed open syncs=%d want 0", syncs)
	}
	if got := relaxed.ManifestSyncMode(); got != ColumnWorkspaceManifestSyncDisabledForBenchmark {
		t.Fatalf("ManifestSyncMode=%q want %q", got, ColumnWorkspaceManifestSyncDisabledForBenchmark)
	}
	if err := relaxed.SaveCollectionManifest(manifest); err != nil {
		t.Fatalf("SaveCollectionManifest relaxed: %v", err)
	}
	if syncs != 0 {
		t.Fatalf("relaxed collection manifest syncs=%d want 0", syncs)
	}
	if err := relaxed.SavePreparedAssetRegistry(1, 1, nil); err != nil {
		t.Fatalf("SavePreparedAssetRegistry relaxed: %v", err)
	}
	if syncs != 0 {
		t.Fatalf("relaxed prepared asset registry syncs=%d want 0", syncs)
	}
	ds := syntheticJSONBenchDataset(8)
	opts, err := JSONBenchColumnPartOptions(ds, 4)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	_ = publishJSONBenchPartRows(t, relaxed, opts, ds, 91, 0, ds.Rows)
	if syncs != 0 {
		t.Fatalf("relaxed workspace manifest syncs=%d want 0", syncs)
	}
	if err := relaxed.Close(); err != nil {
		t.Fatalf("Close relaxed: %v", err)
	}
}

func TestColumnWorkspacePublishPartSyncsAssetsBeforeDurableManifestM9D(t *testing.T) {
	ds := syntheticJSONBenchDataset(16)
	opts, err := JSONBenchColumnPartOptions(ds, 8)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	part, err := BuildColumnPart(111, opts, ColumnBatch{Rows: ds.Rows, Columns: sliceJSONBenchColumns(ds, 0, ds.Rows)})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}

	var assetStore *syncProbeAssetStore
	manifestSyncsAfterAssetSwap := 0
	syncHook := func(file *os.File) error {
		if assetStore != nil {
			manifestSyncsAfterAssetSwap++
			if got := assetStore.syncCalls.Load(); got == 0 {
				t.Errorf("asset sync count=%d before durable manifest sync", got)
			}
		}
		return nil
	}
	workspace, err := OpenColumnWorkspace(t.TempDir(), ColumnWorkspaceOptions{
		Collection:   "jsonbench",
		syncTempFile: syncHook,
	})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace: %v", err)
	}
	defer workspace.Close()
	if err := workspace.assets.Close(); err != nil {
		t.Fatalf("Close original asset manager: %v", err)
	}
	assetStore = &syncProbeAssetStore{MemoryColumnAssetStore: NewMemoryColumnAssetStore()}
	manager, err := NewColumnAssetManager(assetStore)
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	workspace.assets = manager

	if _, err := workspace.PublishPart(part, ds.Dictionaries); err != nil {
		t.Fatalf("PublishPart durable: %v", err)
	}
	if got := assetStore.syncCalls.Load(); got != 1 {
		t.Fatalf("asset sync calls=%d want 1", got)
	}
	if manifestSyncsAfterAssetSwap != 1 {
		t.Fatalf("durable manifest syncs after asset swap=%d want 1", manifestSyncsAfterAssetSwap)
	}
	if state := workspace.assets.DebugState(); state.SyncedAttempts != 0 || state.SyncedRefs != 0 || len(state.Quarantine) != 0 || len(state.PublishFailed) != 0 {
		t.Fatalf("asset manager state after durable publish=%+v want clean synced publish closure", state)
	}
}

func TestColumnWorkspacePublishPartBenchmarkSyncModeSkipsAssetSyncM9D(t *testing.T) {
	ds := syntheticJSONBenchDataset(16)
	opts, err := JSONBenchColumnPartOptions(ds, 8)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	part, err := BuildColumnPart(112, opts, ColumnBatch{Rows: ds.Rows, Columns: sliceJSONBenchColumns(ds, 0, ds.Rows)})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	workspace, err := OpenColumnWorkspace(t.TempDir(), ColumnWorkspaceOptions{
		Collection:       "jsonbench",
		ManifestSyncMode: ColumnWorkspaceManifestSyncDisabledForBenchmark,
	})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace: %v", err)
	}
	defer workspace.Close()
	if err := workspace.assets.Close(); err != nil {
		t.Fatalf("Close original asset manager: %v", err)
	}
	assetStore := &syncProbeAssetStore{MemoryColumnAssetStore: NewMemoryColumnAssetStore()}
	manager, err := NewColumnAssetManager(assetStore)
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	workspace.assets = manager

	if _, err := workspace.PublishPart(part, ds.Dictionaries); err != nil {
		t.Fatalf("PublishPart benchmark sync mode: %v", err)
	}
	if got := assetStore.syncCalls.Load(); got != 0 {
		t.Fatalf("asset sync calls=%d want 0 in benchmark sync mode", got)
	}
}

func TestColumnWorkspacePublishPartMarksAssetFailedWhenDurableManifestFailsM9D(t *testing.T) {
	ds := syntheticJSONBenchDataset(16)
	opts, err := JSONBenchColumnPartOptions(ds, 8)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	part, err := BuildColumnPart(113, opts, ColumnBatch{Rows: ds.Rows, Columns: sliceJSONBenchColumns(ds, 0, ds.Rows)})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}

	manifestErr := fmt.Errorf("injected durable manifest sync failure")
	var assetStore *syncProbeAssetStore
	manifestFailures := 1
	syncHook := func(file *os.File) error {
		if assetStore != nil && manifestFailures > 0 {
			manifestFailures--
			return manifestErr
		}
		return nil
	}
	workspace, err := OpenColumnWorkspace(t.TempDir(), ColumnWorkspaceOptions{
		Collection:   "jsonbench",
		syncTempFile: syncHook,
	})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace: %v", err)
	}
	defer workspace.Close()
	if err := workspace.assets.Close(); err != nil {
		t.Fatalf("Close original asset manager: %v", err)
	}
	assetStore = &syncProbeAssetStore{MemoryColumnAssetStore: NewMemoryColumnAssetStore()}
	manager, err := NewColumnAssetManager(assetStore)
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	workspace.assets = manager

	if _, err := workspace.PublishPart(part, ds.Dictionaries); err == nil || !strings.Contains(err.Error(), manifestErr.Error()) {
		t.Fatalf("PublishPart err=%v want injected manifest failure", err)
	}
	if got := assetStore.syncCalls.Load(); got != 1 {
		t.Fatalf("asset sync calls=%d want 1 before failed durable manifest publish", got)
	}
	if manifest := workspace.Manifest(); len(manifest.Parts) != 0 || manifest.Generation != 0 || manifest.PublishID != 0 {
		t.Fatalf("workspace manifest after failed publish=%+v want rollback to empty manifest", manifest)
	}
	state := workspace.assets.DebugState()
	if len(state.Quarantine) != 1 || len(state.PublishFailed) != 1 || state.SyncedAttempts != 0 || state.SyncedRefs != 0 {
		t.Fatalf("asset manager state after failed durable publish=%+v want quarantined publish failure", state)
	}
	if _, err := workspace.PublishPart(part, ds.Dictionaries); err != nil {
		t.Fatalf("PublishPart retry after failed durable manifest publish: %v", err)
	}
	if got := assetStore.syncCalls.Load(); got != 2 {
		t.Fatalf("asset sync calls after retry=%d want 2", got)
	}
	if manifest := workspace.Manifest(); len(manifest.Parts) != 1 || manifest.Generation != 1 || manifest.PublishID != 1 {
		t.Fatalf("workspace manifest after retry=%+v want one published part", manifest)
	}
}

func TestColumnWorkspacePreparedRegistryAndInventory(t *testing.T) {
	ds := syntheticJSONBenchDataset(64)
	opts, err := JSONBenchColumnPartOptions(ds, 16)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	dir := t.TempDir()
	workspace, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace: %v", err)
	}
	entry := publishJSONBenchPartRows(t, workspace, opts, ds, 401, 0, ds.Rows)
	preparedRef, err := workspace.assets.Put(ColumnAssetKindTCS1PartImage, make([]byte, tcs1HeaderBytes+8))
	if err != nil {
		t.Fatalf("put prepared asset: %v", err)
	}
	prepared := ColumnPreparedAsset{
		Ref:          preparedRef,
		PublishID:    9,
		GenerationID: 10,
		Reason:       "prepare publish",
	}
	if err := workspace.SavePreparedAssetRegistry(9, 10, []ColumnPreparedAsset{prepared}); err != nil {
		t.Fatalf("SavePreparedAssetRegistry: %v", err)
	}
	registryData, err := os.ReadFile(filepath.Join(ColumnWorkspaceNamespaceForDir(dir).PreparedDir, columnWorkspacePreparedFile))
	if err != nil {
		t.Fatalf("ReadFile prepared registry: %v", err)
	}
	if !bytes.HasPrefix(registryData, []byte(columnWorkspacePreparedBinaryMagic)) {
		t.Fatalf("prepared registry magic=%q want binary %q", registryData[:min(len(registryData), 4)], columnWorkspacePreparedBinaryMagic)
	}
	inventory, err := workspace.InventoryNamespace()
	if err != nil {
		t.Fatalf("InventoryNamespace: %v", err)
	}
	if !inventory.PreparedRegistryPresent || len(inventory.PreparedAssets) != 1 || len(inventory.OrphanPreparedAssets) != 1 {
		t.Fatalf("inventory prepared=%+v", inventory)
	}
	if inventory.OrphanPreparedAssets[0].Ref != preparedRef {
		t.Fatalf("orphan prepared ref=%+v want %+v", inventory.OrphanPreparedAssets[0].Ref, preparedRef)
	}
	if len(inventory.SegmentFiles) != 1 || inventory.SegmentFiles[0].FileID != preparedRef.FileID || inventory.SegmentFiles[0].Bytes == 0 {
		t.Fatalf("segment inventory=%+v", inventory.SegmentFiles)
	}
	manifest, err := NewColumnCollectionManifest("jsonbench", opts, []ColumnManifestPartRef{
		NewColumnManifestPartRef(ColumnPartRoleBase, 1, entry),
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewColumnCollectionManifest: %v", err)
	}
	plan, err := PlanColumnAssetReachability(ColumnAssetReachabilityInput{
		ActiveManifest: &manifest,
		PreparedAssets: []ColumnPreparedAsset{prepared},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	if plan.PreparedBytes != int(preparedRef.Length) || plan.ReclaimableBytes != 0 {
		t.Fatalf("prepared reachability=%+v want protected prepared bytes=%d", plan, preparedRef.Length)
	}
	if err := workspace.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"})
	if err != nil {
		t.Fatalf("reopen workspace: %v", err)
	}
	defer reopened.Close()
	registry, err := reopened.LoadPreparedAssetRegistry()
	if err != nil {
		t.Fatalf("LoadPreparedAssetRegistry: %v", err)
	}
	if registry.PublishID != 9 || registry.GenerationID != 10 || len(registry.Assets) != 1 || registry.Assets[0].Ref != preparedRef {
		t.Fatalf("bad prepared registry=%+v", registry)
	}
	if err := reopened.ClearPreparedAssetRegistry(); err != nil {
		t.Fatalf("ClearPreparedAssetRegistry: %v", err)
	}
	cleared, err := reopened.InventoryNamespace()
	if err != nil {
		t.Fatalf("InventoryNamespace after clear: %v", err)
	}
	if cleared.PreparedRegistryPresent || len(cleared.PreparedAssets) != 0 {
		t.Fatalf("cleared inventory=%+v want no prepared registry", cleared)
	}
}

func TestColumnWorkspaceRejectsUnknownManifestVersion(t *testing.T) {
	dir, _ := testColumnWorkspaceWithPart(t)
	path := filepath.Join(ColumnWorkspaceNamespaceForDir(dir).ManifestDir, columnWorkspaceManifestFile)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	corrupt := append([]byte(nil), data...)
	binary.LittleEndian.PutUint16(corrupt[4:], columnWorkspaceManifestBinaryVersion+1)
	if err := os.WriteFile(path, corrupt, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"}); err == nil || !strings.Contains(err.Error(), "unsupported workspace manifest binary version") {
		t.Fatalf("OpenColumnWorkspace err=%v want unsupported manifest version", err)
	}
}

func TestColumnWorkspaceRejectsManifestChecksumMismatch(t *testing.T) {
	dir, _ := testColumnWorkspaceWithPart(t)
	path := filepath.Join(ColumnWorkspaceNamespaceForDir(dir).ManifestDir, columnWorkspaceManifestFile)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	corrupt := append([]byte(nil), data...)
	corrupt[len(corrupt)-1] ^= 0xff
	if err := os.WriteFile(path, corrupt, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"}); err == nil || !strings.Contains(err.Error(), "workspace manifest binary checksum") {
		t.Fatalf("OpenColumnWorkspace err=%v want checksum mismatch", err)
	}
}

func TestColumnWorkspaceRejectsCorruptTCS1AssetOnReopen(t *testing.T) {
	dir, entry := testColumnWorkspaceWithPart(t)
	path := filepath.Join(ColumnWorkspaceNamespaceForDir(dir).SegmentDir, "column-assets-000001.seg")
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := file.WriteAt([]byte{0}, entry.AssetRef.Offset); err != nil {
		_ = file.Close()
		t.Fatalf("WriteAt: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close corrupt file: %v", err)
	}
	if _, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"}); err == nil || !strings.Contains(err.Error(), "validate workspace part") {
		t.Fatalf("OpenColumnWorkspace err=%v want corrupt asset validation", err)
	}
}

func TestColumnWorkspaceCanValidateTCS1HeaderWithoutPayload(t *testing.T) {
	dir, entry := testColumnWorkspaceWithPart(t)
	path := filepath.Join(ColumnWorkspaceNamespaceForDir(dir).SegmentDir, "column-assets-000001.seg")
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := file.WriteAt([]byte{0}, entry.AssetRef.Offset+tcs1PayloadOffset+1); err != nil {
		_ = file.Close()
		t.Fatalf("WriteAt: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close corrupt file: %v", err)
	}
	reopened, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench", ValidationMode: ColumnWorkspaceValidateTCS1Header})
	if err != nil {
		t.Fatalf("metadata-only reopen should not read full payload: %v", err)
	}
	defer reopened.Close()
	if _, err := reopened.LoadPart(entry.PartID); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("LoadPart err=%v want payload checksum failure", err)
	}
}

func BenchmarkColumnWorkspaceReopenValidationModes(b *testing.B) {
	dir, parts, assetBytes := benchmarkColumnWorkspaceDir(b, 128, 64)
	modes := []struct {
		name string
		mode ColumnWorkspaceValidationMode
	}{
		{"full_image", ColumnWorkspaceValidateFullImage},
		{"tcs1_header", ColumnWorkspaceValidateTCS1Header},
	}
	for _, mode := range modes {
		b.Run(mode.name, func(b *testing.B) {
			b.ReportMetric(float64(parts), "parts")
			b.ReportMetric(float64(assetBytes), "asset_bytes")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				workspace, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench", ValidationMode: mode.mode})
				if err != nil {
					b.Fatalf("OpenColumnWorkspace: %v", err)
				}
				if err := workspace.Close(); err != nil {
					b.Fatalf("Close: %v", err)
				}
			}
		})
	}
}

func testColumnWorkspaceWithPart(t *testing.T) (string, ColumnWorkspacePartManifest) {
	t.Helper()
	ds := syntheticJSONBenchDataset(64)
	part, err := BuildJSONBenchColumnPart(ds, 16)
	if err != nil {
		t.Fatalf("BuildJSONBenchColumnPart: %v", err)
	}
	dir := t.TempDir()
	workspace, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace: %v", err)
	}
	entry, err := workspace.PublishPart(part, ds.Dictionaries)
	if err != nil {
		t.Fatalf("PublishPart: %v", err)
	}
	if err := workspace.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return dir, entry
}

func benchmarkColumnWorkspaceDir(b *testing.B, parts int, rowsPerPart int) (string, int, int) {
	b.Helper()
	ds := syntheticJSONBenchDataset(parts * rowsPerPart)
	opts, err := JSONBenchColumnPartOptions(ds, rowsPerPart)
	if err != nil {
		b.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	dir := b.TempDir()
	workspace, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"})
	if err != nil {
		b.Fatalf("OpenColumnWorkspace: %v", err)
	}
	assetBytes := 0
	for part := 0; part < parts; part++ {
		entry := publishJSONBenchPartRows(b, workspace, opts, ds, uint64(10_000+part), part*rowsPerPart, (part+1)*rowsPerPart)
		assetBytes += entry.AssetBytes
	}
	if err := workspace.Close(); err != nil {
		b.Fatalf("Close: %v", err)
	}
	return dir, parts, assetBytes
}
