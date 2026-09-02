package collections

import (
	"encoding/binary"
	"reflect"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestColumnDictionaryCodesAssetLittleEndianPayloadDirectView1934(t *testing.T) {
	cfg := columnDictionaryCodesAssetTestConfig1934(t)
	asset := columnDictionaryCodesAssetTestAsset1934(cfg)
	raw, err := encodeColumnDictionaryCodesAsset(asset)
	if err != nil {
		t.Fatalf("encodeColumnDictionaryCodesAsset: %v", err)
	}
	if size := columnDictionaryCodesEncodedSize(asset); size != len(raw) {
		t.Fatalf("encoded dictionary codes size=%d want len=%d", size, len(raw))
	}
	ref := columnDictionaryCodesAssetTestRef1934(cfg, asset, raw)
	ref.Offset = int64(columnDictionaryCodesPayloadAlignment)

	cur, cardinality, rowCount, err := decodeColumnDictionaryCodesAssetHeader(raw, ref, cfg, "events", "kind", true)
	if err != nil {
		t.Fatalf("decode header: %v", err)
	}
	if cardinality != len(asset.Dictionary) || rowCount != len(asset.Codes) {
		t.Fatalf("header cardinality/rows=%d/%d want %d/%d", cardinality, rowCount, len(asset.Dictionary), len(asset.Codes))
	}
	for i := 0; i < cardinality; i++ {
		_ = cur.stringBytes()
	}
	payload, err := columnDictionaryCodesPayloadAfterDictionary(raw, ref, &cur, rowCount)
	if err != nil {
		t.Fatalf("payload after dictionary: %v", err)
	}
	if payload.alignment != columnDictionaryCodesPayloadAlignment || payload.byteLen != len(asset.Codes)*4 || payload.offset%columnDictionaryCodesPayloadAlignment != 0 {
		t.Fatalf("payload=%+v want 4-byte aligned little-endian row-code payload", payload)
	}
	payloadBytes, err := columnDictionaryCodesPayloadBytes(raw, payload)
	if err != nil {
		t.Fatalf("payload bytes: %v", err)
	}
	for i, want := range asset.Codes {
		if got := binary.LittleEndian.Uint32(payloadBytes[i*4:]); got != want {
			t.Fatalf("payload code[%d] little-endian=%d want %d bytes=%x", i, got, want, payloadBytes[i*4:i*4+4])
		}
	}
	codes, direct, err := viewColumnDictionaryCodesPayload(raw, payload)
	if err != nil {
		t.Fatalf("view payload: %v", err)
	}
	if !reflect.DeepEqual(codes, asset.Codes) {
		t.Fatalf("codes=%v want %v", codes, asset.Codes)
	}
	if columnPhysicalNativeLittleEndian && !direct {
		t.Fatalf("direct view=false on native little-endian aligned payload=%+v", payload)
	}

	decoded, err := decodeColumnDictionaryCodesAsset(raw, ref, cfg, "events", "kind", true)
	if err != nil {
		t.Fatalf("decode full asset: %v", err)
	}
	if !reflect.DeepEqual(decoded.Codes, asset.Codes) || !reflect.DeepEqual(decoded.Dictionary, asset.Dictionary) {
		t.Fatalf("decoded dictionary/codes=%v/%v want %v/%v", decoded.Dictionary, decoded.Codes, asset.Dictionary, asset.Codes)
	}
}

func TestColumnDictionaryCodesAssetRejectsLegacyV1Payload1934(t *testing.T) {
	cfg := columnDictionaryCodesAssetTestConfig1934(t)
	asset := columnDictionaryCodesAssetTestAsset1934(cfg)
	raw, err := encodeColumnDictionaryCodesAsset(asset)
	if err != nil {
		t.Fatalf("encodeColumnDictionaryCodesAsset: %v", err)
	}
	legacy := append([]byte(nil), raw...)
	binary.BigEndian.PutUint16(legacy[4:6], columnDictionaryCodesAssetVersionV1)
	ref := columnDictionaryCodesAssetTestRef1934(cfg, asset, legacy)
	if _, err := decodeColumnDictionaryCodesAsset(legacy, ref, cfg, "events", "kind", true); err == nil || !strings.Contains(err.Error(), "unsupported dictionary codes asset version=1") {
		t.Fatalf("legacy v1 decode err=%v want unsupported version=1", err)
	}
}

func TestColumnDictionaryCodesAssetRejectsPayloadPaddingCorruption1934(t *testing.T) {
	cfg := columnDictionaryCodesAssetTestConfig1934(t)
	asset := columnDictionaryCodesAssetTestAsset1934(cfg)
	var raw []byte
	var paddingStart, padding int
	for suffix := ""; suffix != "xxxxxxxx"; suffix += "x" {
		asset.Dictionary[0] = "alpha" + suffix
		encoded, err := encodeColumnDictionaryCodesAsset(asset)
		if err != nil {
			t.Fatalf("encode suffix=%q: %v", suffix, err)
		}
		ref := columnDictionaryCodesAssetTestRef1934(cfg, asset, encoded)
		cur, cardinality, _, err := decodeColumnDictionaryCodesAssetHeader(encoded, ref, cfg, "events", "kind", true)
		if err != nil {
			t.Fatalf("decode header suffix=%q: %v", suffix, err)
		}
		for i := 0; i < cardinality; i++ {
			_ = cur.stringBytes()
		}
		paddingStart = cur.pos
		padding = columnSidecarPayloadPadding(cur.pos, columnDictionaryCodesPayloadAlignment)
		if padding > 0 {
			raw = encoded
			break
		}
	}
	if padding == 0 {
		t.Fatal("test asset did not produce dictionary payload padding")
	}
	corrupt := append([]byte(nil), raw...)
	corrupt[paddingStart] = 0x7f
	ref := columnDictionaryCodesAssetTestRef1934(cfg, asset, corrupt)
	if _, err := decodeColumnDictionaryCodesAsset(corrupt, ref, cfg, "events", "kind", true); err == nil || !strings.Contains(err.Error(), "payload padding") {
		t.Fatalf("padding corruption err=%v want payload padding failure", err)
	}
}

func TestColumnDictionaryCodesAssetManagerAlignsPayloadForMmapDirectView1934(t *testing.T) {
	cfg := columnDictionaryCodesAssetTestConfig1934(t)
	asset := columnDictionaryCodesAssetTestAsset1934(cfg)
	raw, err := encodeColumnDictionaryCodesAsset(asset)
	if err != nil {
		t.Fatalf("encodeColumnDictionaryCodesAsset: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	seedRef, err := writeColumnAssetToManagerSegment(root, cfg, []byte{0xab}, ColumnAssetKindTCS1PartImage, asset.Generation, 99, columnAssetM12ASegmentFileID)
	if err != nil {
		t.Fatalf("seed write: %v", err)
	}
	dictRef, err := writeColumnDictionaryCodesAssetToManager(root, cfg, raw, asset.Generation, asset.PartID)
	if err != nil {
		t.Fatalf("write dictionary codes: %v", err)
	}
	if dictRef.Offset%int64(dictionaryCodesDirectViewAssetAlignment) != 0 {
		t.Fatalf("dictionary ref offset=%d want %d-byte aligned", dictRef.Offset, dictionaryCodesDirectViewAssetAlignment)
	}
	segment := readColumnAssetSegmentFileForTest(t, root, dictRef)
	assertZeroBytesForTest(t, segment[seedRef.Offset+seedRef.Length:dictRef.Offset], "dictionary direct-view prefix padding")

	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(root, cfg.AssetManager.Namespace, ColumnAssetReadIntegritySkipChecksums)
	if err != nil {
		t.Fatalf("new read cache: %v", err)
	}
	readCache.returnViews = true
	defer func() { _ = readCache.close() }()
	viewRaw, err := readCache.read(dictRef, nil)
	if err != nil {
		t.Fatalf("read dictionary codes: %v", err)
	}
	cur, cardinality, rowCount, err := decodeColumnDictionaryCodesAssetHeader(viewRaw, dictRef, cfg, "events", "kind", false)
	if err != nil {
		t.Fatalf("decode mmap header: %v", err)
	}
	for i := 0; i < cardinality; i++ {
		_ = cur.stringBytes()
	}
	payload, err := columnDictionaryCodesPayloadAfterDictionary(viewRaw, dictRef, &cur, rowCount)
	if err != nil {
		t.Fatalf("mmap payload: %v", err)
	}
	codes, direct, err := viewColumnDictionaryCodesPayload(viewRaw, payload)
	if err != nil {
		t.Fatalf("mmap payload view: %v", err)
	}
	if columnPhysicalNativeLittleEndian && !direct {
		t.Fatalf("mmap dictionary payload did not use direct []uint32 view: payload=%+v ref=%+v", payload, dictRef)
	}
	if !reflect.DeepEqual(codes, asset.Codes) {
		t.Fatalf("mmap codes=%v want %v", codes, asset.Codes)
	}
}

func TestColumnDictionaryCodesAssetLittleEndianPersistsAfterReopen1934(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(128)
	collection, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()
	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind", ColumnAssetReadIntegrity: ColumnAssetReadIntegritySkipChecksums}
	result, err := collection.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery: %v", err)
	}
	if result.Diagnostics.DictionaryCodeHits == 0 || result.Diagnostics.ReduceRows != len(events) || len(result.Groups) != 4 {
		t.Fatalf("reopened q1 result groups=%+v diagnostics=%+v", result.Groups, result.Diagnostics)
	}

	view, closeView, err := collection.prepareColumnPhysicalScanSnapshotViewWithSidecars(columnManifestScanSidecarsForPhysicalQuery(req))
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		t.Fatalf("prepare scan view: %v", err)
	}
	byPart := columnDictionaryCodeSnapshotsByPart(view, "kind")
	if len(byPart) != 1 {
		t.Fatalf("kind dictionary snapshots=%d want 1", len(byPart))
	}
	var snapshot columnManifestDictionaryCodesSnapshot
	for _, candidate := range byPart {
		snapshot = candidate
	}
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, ColumnAssetReadIntegritySkipChecksums)
	if err != nil {
		t.Fatalf("new read cache: %v", err)
	}
	readCache.returnViews = true
	defer func() { _ = readCache.close() }()
	raw, err := readCache.read(snapshot.AssetRef, nil)
	if err != nil {
		t.Fatalf("read reopened dictionary asset: %v", err)
	}
	cur, cardinality, rowCount, err := decodeColumnDictionaryCodesAssetHeader(raw, snapshot.AssetRef, view.Config, view.CollectionName, "kind", false)
	if err != nil {
		t.Fatalf("decode reopened header: %v", err)
	}
	for i := 0; i < cardinality; i++ {
		_ = cur.stringBytes()
	}
	payload, err := columnDictionaryCodesPayloadAfterDictionary(raw, snapshot.AssetRef, &cur, rowCount)
	if err != nil {
		t.Fatalf("decode reopened payload: %v", err)
	}
	codes, direct, err := viewColumnDictionaryCodesPayload(raw, payload)
	if err != nil {
		t.Fatalf("view reopened payload: %v", err)
	}
	if len(codes) != len(events) {
		t.Fatalf("reopened payload codes=%d want rows=%d", len(codes), len(events))
	}
	if columnPhysicalNativeLittleEndian && !direct {
		t.Fatalf("reopened dictionary payload not direct-view eligible: payload=%+v ref=%+v", payload, snapshot.AssetRef)
	}
}

func TestBuildColumnDictionaryCodesAssetsMultiColumnSinglePassM3110(t *testing.T) {
	cfg := columnDictionaryCodesAssetTestConfig1934(t)
	rows := columnDictionaryCodesAssetTestRowsM3110()

	assets, err := buildColumnDictionaryCodesAssets(cfg, rows, "events", cfg.AssetManager.Namespace, 7, 3, 42)
	if err != nil {
		t.Fatalf("buildColumnDictionaryCodesAssets: %v", err)
	}
	if len(assets) != 2 {
		t.Fatalf("assets=%d want kind+did", len(assets))
	}
	if assets[0].ColumnName != "kind" || assets[1].ColumnName != "did" {
		t.Fatalf("asset order=%s,%s want kind,did", assets[0].ColumnName, assets[1].ColumnName)
	}
	byName := make(map[string]columnDictionaryCodesAsset, len(assets))
	for _, asset := range assets {
		byName[asset.ColumnName] = asset
	}
	assertDictionaryCodesAssetM3110(t, byName["kind"], 1, []string{"post", "like", "repost"}, []uint32{0, 1, 0, 2})
	assertDictionaryCodesAssetM3110(t, byName["did"], 2, []string{"did:plc:2", "did:plc:1", "did:plc:3"}, []uint32{0, 1, 0, 2})

	for _, colIdx := range []int{1, 2} {
		asset, ok, err := buildColumnDictionaryCodesAssetForColumn(cfg, rows, "events", cfg.AssetManager.Namespace, 7, 3, 42, colIdx)
		if err != nil {
			t.Fatalf("buildColumnDictionaryCodesAssetForColumn col=%d: %v", colIdx, err)
		}
		if !ok {
			t.Fatalf("buildColumnDictionaryCodesAssetForColumn col=%d ok=false", colIdx)
		}
		if !reflect.DeepEqual(asset, byName[asset.ColumnName]) {
			t.Fatalf("column %d direct asset=%+v batched=%+v", colIdx, asset, byName[asset.ColumnName])
		}
	}
}

func TestBuildColumnDictionaryCodesAssetsFailClosedM3110(t *testing.T) {
	cfg := columnDictionaryCodesAssetTestConfig1934(t)

	rows := columnDictionaryCodesAssetTestRowsM3110()
	rows[1].Values[1] = columnDeclaredValue{Type: ColumnStoreValueString, Present: true, Null: true}
	assets, err := buildColumnDictionaryCodesAssets(cfg, rows, "events", cfg.AssetManager.Namespace, 7, 3, 42)
	if err != nil {
		t.Fatalf("nullable buildColumnDictionaryCodesAssets: %v", err)
	}
	if len(assets) != 1 || assets[0].ColumnName != "did" {
		t.Fatalf("nullable kind assets=%+v want only did", assets)
	}

	rows = columnDictionaryCodesAssetTestRowsM3110()
	rows[0].Deleted = true
	assets, err = buildColumnDictionaryCodesAssets(cfg, rows, "events", cfg.AssetManager.Namespace, 7, 3, 42)
	if err != nil {
		t.Fatalf("deleted buildColumnDictionaryCodesAssets: %v", err)
	}
	if len(assets) != 0 {
		t.Fatalf("deleted row assets=%+v want none", assets)
	}

	rows = columnDictionaryCodesAssetTestRowsM3110()
	rows[0].Values = rows[0].Values[:2]
	if _, err := buildColumnDictionaryCodesAssets(cfg, rows, "events", cfg.AssetManager.Namespace, 7, 3, 42); err == nil || !strings.Contains(err.Error(), "values=2 columns=3") {
		t.Fatalf("row-width err=%v want values=2 columns=3", err)
	}

	rows = columnDictionaryCodesAssetTestRowsM3110()
	rows[0].Values[2] = columnDeclaredValue{Type: ColumnStoreValueInt64, Present: true, Int64: 10}
	if _, err := buildColumnDictionaryCodesAssets(cfg, rows, "events", cfg.AssetManager.Namespace, 7, 3, 42); err == nil || !strings.Contains(err.Error(), "column[2] type=\"int64\" want string") {
		t.Fatalf("type mismatch err=%v want did string type error", err)
	}
}

func columnDictionaryCodesAssetTestConfig1934(t *testing.T) ColumnStoreConfig {
	t.Helper()
	normalized, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	normalized.RecoveryAuthoritativeAppliedCommandLSN = 42
	return *normalized
}

func columnDictionaryCodesAssetTestAsset1934(cfg ColumnStoreConfig) columnDictionaryCodesAsset {
	return columnDictionaryCodesAsset{
		Collection:        "events",
		Namespace:         cfg.AssetManager.Namespace,
		Generation:        7,
		PartID:            3,
		AppliedCommandLSN: 42,
		SchemaHash:        cfg.SchemaHash,
		ColumnName:        "kind",
		ColumnIndex:       1,
		Dictionary:        []string{"alpha", "beta", "gamma"},
		Codes:             []uint32{0, 1, 2, 1, 0, 2},
	}
}

func columnDictionaryCodesAssetTestRowsM3110() []columnDeclaredRow {
	return []columnDeclaredRow{
		columnDictionaryCodesAssetTestRowM3110(10, "post", "did:plc:2"),
		columnDictionaryCodesAssetTestRowM3110(11, "like", "did:plc:1"),
		columnDictionaryCodesAssetTestRowM3110(12, "post", "did:plc:2"),
		columnDictionaryCodesAssetTestRowM3110(13, "repost", "did:plc:3"),
	}
}

func columnDictionaryCodesAssetTestRowM3110(timeUS int64, kind, did string) columnDeclaredRow {
	return columnDeclaredRow{
		ID: []byte(kind + ":" + did),
		Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: timeUS},
			{Type: ColumnStoreValueString, Present: true, String: kind},
			{Type: ColumnStoreValueString, Present: true, String: did},
		},
	}
}

func assertDictionaryCodesAssetM3110(t *testing.T, asset columnDictionaryCodesAsset, columnIndex int, dictionary []string, codes []uint32) {
	t.Helper()
	if asset.ColumnIndex != columnIndex {
		t.Fatalf("%s column index=%d want %d", asset.ColumnName, asset.ColumnIndex, columnIndex)
	}
	if !reflect.DeepEqual(asset.Dictionary, dictionary) || !reflect.DeepEqual(asset.Codes, codes) {
		t.Fatalf("%s dictionary/codes=%v/%v want %v/%v", asset.ColumnName, asset.Dictionary, asset.Codes, dictionary, codes)
	}
}

func columnDictionaryCodesAssetTestRef1934(cfg ColumnStoreConfig, asset columnDictionaryCodesAsset, raw []byte) ColumnAssetRef {
	return ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1DictionaryCodes,
		Namespace:  cfg.AssetManager.Namespace,
		Generation: asset.Generation,
		PartID:     asset.PartID,
		Length:     int64(len(raw)),
		Checksum:   page.Checksum(raw),
	}
}
