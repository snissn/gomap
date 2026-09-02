package collections

import (
	"encoding/binary"
	"reflect"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestColumnInt64ValuesAssetLittleEndianPayloadDirectView1935(t *testing.T) {
	cfg := columnDictionaryCodesAssetTestConfig1934(t)
	asset := columnInt64ValuesAssetTestAsset1935(cfg.AssetManager.Namespace, cfg.SchemaHash)
	raw, err := encodeColumnInt64ValuesAsset(asset)
	if err != nil {
		t.Fatalf("encodeColumnInt64ValuesAsset: %v", err)
	}
	if size := columnInt64ValuesEncodedSize(asset); size != len(raw) {
		t.Fatalf("encoded int64 values size=%d want len=%d", size, len(raw))
	}
	ref := columnInt64ValuesAssetTestRef1935(cfg.AssetManager.Namespace, asset, raw)
	ref.Offset = int64(columnInt64ValuesPayloadAlignment)

	_, payload, err := decodeColumnInt64ValuesAssetPayload(raw, ref, cfg, asset.Collection, "time_us", true)
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload.alignment != columnInt64ValuesPayloadAlignment || payload.byteLen != len(asset.Values)*columnInt64ValuesPayloadElemBytes || payload.offset%columnInt64ValuesPayloadAlignment != 0 {
		t.Fatalf("payload=%+v want 8-byte aligned little-endian int64 payload", payload)
	}
	payloadBytes, err := columnInt64ValuesPayloadBytes(raw, payload)
	if err != nil {
		t.Fatalf("payload bytes: %v", err)
	}
	for i, want := range asset.Values {
		if got := int64(binary.LittleEndian.Uint64(payloadBytes[i*columnInt64ValuesPayloadElemBytes:])); got != want {
			t.Fatalf("payload value[%d] little-endian=%d want %d bytes=%x", i, got, want, payloadBytes[i*columnInt64ValuesPayloadElemBytes:i*columnInt64ValuesPayloadElemBytes+columnInt64ValuesPayloadElemBytes])
		}
	}
	values, direct, err := viewColumnInt64ValuesPayload(raw, payload)
	if err != nil {
		t.Fatalf("view payload: %v", err)
	}
	if !reflect.DeepEqual(values, asset.Values) {
		t.Fatalf("values=%v want %v", values, asset.Values)
	}
	if columnPhysicalNativeLittleEndian && !direct {
		t.Fatalf("direct view=false on native little-endian aligned payload=%+v", payload)
	}

	decoded, err := decodeColumnInt64ValuesAsset(raw, ref, cfg, asset.Collection, "time_us", true)
	if err != nil {
		t.Fatalf("decode full asset: %v", err)
	}
	if !reflect.DeepEqual(decoded.Values, asset.Values) {
		t.Fatalf("decoded values=%v want %v", decoded.Values, asset.Values)
	}
}

func TestColumnInt64ValuesAssetRejectsLegacyV1Payload1935(t *testing.T) {
	cfg := columnDictionaryCodesAssetTestConfig1934(t)
	asset := columnInt64ValuesAssetTestAsset1935(cfg.AssetManager.Namespace, cfg.SchemaHash)
	raw, err := encodeColumnInt64ValuesAsset(asset)
	if err != nil {
		t.Fatalf("encodeColumnInt64ValuesAsset: %v", err)
	}
	legacy := append([]byte(nil), raw...)
	binary.BigEndian.PutUint16(legacy[4:6], columnInt64ValuesAssetVersionV1)
	ref := columnInt64ValuesAssetTestRef1935(cfg.AssetManager.Namespace, asset, legacy)
	if _, err := decodeColumnInt64ValuesAsset(legacy, ref, cfg, asset.Collection, "time_us", true); err == nil || !strings.Contains(err.Error(), "unsupported int64 values asset version=1") {
		t.Fatalf("legacy v1 decode err=%v want unsupported version=1", err)
	}
}

func TestColumnInt64ValuesAssetRejectsPayloadPaddingCorruption1935(t *testing.T) {
	cfg := columnDictionaryCodesAssetTestConfig1934(t)
	asset := columnInt64ValuesAssetTestAsset1935(cfg.AssetManager.Namespace, cfg.SchemaHash)
	var raw []byte
	var paddingStart, padding int
	for suffix := ""; suffix != "xxxxxxxx"; suffix += "x" {
		asset.Collection = "events" + suffix
		encoded, err := encodeColumnInt64ValuesAsset(asset)
		if err != nil {
			t.Fatalf("encode suffix=%q: %v", suffix, err)
		}
		cur, _, err := columnInt64ValuesAssetHeaderCursorBeforePayload1935(encoded)
		if err != nil {
			t.Fatalf("header cursor suffix=%q: %v", suffix, err)
		}
		paddingStart = cur.pos
		padding = columnSidecarPayloadPadding(cur.pos, columnInt64ValuesPayloadAlignment)
		if padding > 0 {
			raw = encoded
			break
		}
	}
	if padding == 0 {
		t.Fatal("test asset did not produce int64 payload padding")
	}
	corrupt := append([]byte(nil), raw...)
	corrupt[paddingStart] = 0x7f
	ref := columnInt64ValuesAssetTestRef1935(cfg.AssetManager.Namespace, asset, corrupt)
	if _, err := decodeColumnInt64ValuesAsset(corrupt, ref, cfg, asset.Collection, "time_us", true); err == nil || !strings.Contains(err.Error(), "payload padding") {
		t.Fatalf("padding corruption err=%v want payload padding failure", err)
	}
}

func TestColumnInt64ValuesAssetManagerAlignsPayloadForMmapDirectView1935(t *testing.T) {
	cfg := columnDictionaryCodesAssetTestConfig1934(t)
	asset := columnInt64ValuesAssetTestAsset1935(cfg.AssetManager.Namespace, cfg.SchemaHash)
	raw, err := encodeColumnInt64ValuesAsset(asset)
	if err != nil {
		t.Fatalf("encodeColumnInt64ValuesAsset: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	seedRef, err := writeColumnAssetToManagerSegment(root, cfg, []byte{0xab}, ColumnAssetKindTCS1PartImage, asset.Generation, 99, columnAssetM12ASegmentFileID)
	if err != nil {
		t.Fatalf("seed write: %v", err)
	}
	valuesRef, err := writeColumnInt64ValuesAssetToManager(root, cfg, raw, asset.Generation, asset.PartID)
	if err != nil {
		t.Fatalf("write int64 values: %v", err)
	}
	if valuesRef.Offset%int64(int64ValuesDirectViewAssetAlignment) != 0 {
		t.Fatalf("int64 values ref offset=%d want %d-byte aligned", valuesRef.Offset, int64ValuesDirectViewAssetAlignment)
	}
	segment := readColumnAssetSegmentFileForTest(t, root, valuesRef)
	assertZeroBytesForTest(t, segment[seedRef.Offset+seedRef.Length:valuesRef.Offset], "int64 values direct-view prefix padding")

	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(root, cfg.AssetManager.Namespace, ColumnAssetReadIntegritySkipChecksums)
	if err != nil {
		t.Fatalf("new read cache: %v", err)
	}
	readCache.returnViews = true
	defer func() { _ = readCache.close() }()
	viewRaw, err := readCache.read(valuesRef, nil)
	if err != nil {
		t.Fatalf("read int64 values: %v", err)
	}
	_, payload, err := decodeColumnInt64ValuesAssetPayload(viewRaw, valuesRef, cfg, asset.Collection, "time_us", false)
	if err != nil {
		t.Fatalf("decode mmap payload: %v", err)
	}
	values, direct, err := viewColumnInt64ValuesPayload(viewRaw, payload)
	if err != nil {
		t.Fatalf("mmap payload view: %v", err)
	}
	if columnPhysicalNativeLittleEndian && !direct {
		t.Fatalf("mmap int64 payload did not use direct []int64 view: payload=%+v ref=%+v", payload, valuesRef)
	}
	if !reflect.DeepEqual(values, asset.Values) {
		t.Fatalf("mmap values=%v want %v", values, asset.Values)
	}
}

func TestColumnInt64ValuesAssetLittleEndianPersistsAfterReopen1935(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(128)
	collection, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()
	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryHourCount, ValueColumn: "time_us", ColumnAssetReadIntegrity: ColumnAssetReadIntegritySkipChecksums}
	result, err := collection.RunColumnPhysicalQuery(req)
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery: %v", err)
	}
	if result.Diagnostics.Int64ValueHits == 0 || result.Diagnostics.ReduceRows != len(events) || len(result.Groups) == 0 {
		t.Fatalf("reopened q3 result groups=%+v diagnostics=%+v", result.Groups, result.Diagnostics)
	}

	view, closeView, err := collection.prepareColumnPhysicalScanSnapshotViewWithSidecars(columnManifestScanSidecarsForPhysicalQuery(req))
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		t.Fatalf("prepare scan view: %v", err)
	}
	byPart := columnInt64ValueSnapshotsByPart(view, "time_us")
	if len(byPart) != 1 {
		t.Fatalf("time_us int64 snapshots=%d want 1", len(byPart))
	}
	var snapshot columnManifestInt64ValuesSnapshot
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
		t.Fatalf("read reopened int64 asset: %v", err)
	}
	_, payload, err := decodeColumnInt64ValuesAssetPayload(raw, snapshot.AssetRef, view.Config, view.CollectionName, "time_us", false)
	if err != nil {
		t.Fatalf("decode reopened payload: %v", err)
	}
	values, direct, err := viewColumnInt64ValuesPayload(raw, payload)
	if err != nil {
		t.Fatalf("view reopened payload: %v", err)
	}
	if len(values) != len(events) {
		t.Fatalf("reopened payload values=%d want rows=%d", len(values), len(events))
	}
	if columnPhysicalNativeLittleEndian && !direct {
		t.Fatalf("reopened int64 payload not direct-view eligible: payload=%+v ref=%+v", payload, snapshot.AssetRef)
	}
}

func columnInt64ValuesAssetTestAsset1935(namespace string, schemaHash uint64) columnInt64ValuesAsset {
	const minInt64 = -1 << 63
	const maxInt64 = 1<<63 - 1
	return columnInt64ValuesAsset{
		Collection:        "events",
		Namespace:         namespace,
		Generation:        7,
		PartID:            3,
		AppliedCommandLSN: 42,
		SchemaHash:        schemaHash,
		ColumnName:        "time_us",
		ColumnIndex:       0,
		Values:            []int64{0, -1, 1, minInt64, maxInt64, -123456789012345, 123456789012345},
	}
}

func columnInt64ValuesAssetTestRef1935(namespace string, asset columnInt64ValuesAsset, raw []byte) ColumnAssetRef {
	return ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1Int64Values,
		Namespace:  namespace,
		Generation: asset.Generation,
		PartID:     asset.PartID,
		Length:     int64(len(raw)),
		Checksum:   page.Checksum(raw),
	}
}

func columnInt64ValuesAssetHeaderCursorBeforePayload1935(raw []byte) (manifestCursor, int, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnInt64ValuesAssetMagic {
		return manifestCursor{}, 0, nil
	}
	if version := cur.u16(); version != columnInt64ValuesAssetVersion {
		return manifestCursor{}, 0, nil
	}
	_ = cur.stringBytes()
	_ = cur.stringBytes()
	_ = cur.u64()
	_ = cur.u64()
	_ = cur.u64()
	_ = cur.u64()
	_ = cur.stringBytes()
	_ = cur.u64()
	rowCount := cur.u64()
	if err := cur.err; err != nil {
		return manifestCursor{}, 0, err
	}
	if rowCount > uint64(maxCollectionInt) {
		return manifestCursor{}, 0, nil
	}
	return cur, int(rowCount), nil
}
