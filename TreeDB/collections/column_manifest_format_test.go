package collections

import (
	"bytes"
	"testing"
)

func TestColumnManifestRecordDecodeAcceptsV1CompatibilityM1634(t *testing.T) {
	header := mustEncodeColumnManifestHeaderRecordVersionM1634(t, 1)
	decodedHeader, err := decodeColumnManifestHeaderRecord(header)
	if err != nil {
		t.Fatalf("decodeColumnManifestHeaderRecord v1: %v", err)
	}
	if decodedHeader.Generation != 7 || decodedHeader.ExpectedParts != 1 {
		t.Fatalf("unexpected v1 header: %+v", decodedHeader)
	}

	asset := ColumnPreparedAsset{
		Ref: ColumnAssetRef{
			Kind:       ColumnAssetKindTCS1PartImage,
			Namespace:  "events_column_assets",
			Generation: 7,
			PartID:     3,
			FileID:     1,
			Offset:     16,
			Length:     128,
			Checksum:   99,
		},
		Rows:         42,
		Bytes:        128,
		PublishID:    11,
		GenerationID: 7,
		Reason:       string(ColumnPublishOperationInsert),
	}
	part := mustEncodeColumnManifestPartRecordVersionM1634(t, asset, 1)
	decodedPart, err := decodeColumnManifestPartRecord(part)
	if err != nil {
		t.Fatalf("decodeColumnManifestPartRecord v1: %v", err)
	}
	if decodedPart.AssetRef != asset.Ref || decodedPart.Rows != 0 || decodedPart.Bytes != asset.Bytes || decodedPart.Reason != asset.Reason {
		t.Fatalf("unexpected v1 part decode: %+v", decodedPart)
	}

	records := []columnManifestRecord{
		{key: []byte(columnManifestHeaderRecordKey), value: header},
		{key: columnManifestPartRecordKey(asset.Ref.Generation, asset.Ref.PartID), value: part},
	}
	snapshot, err := decodeColumnManifestRecords(records)
	if err != nil {
		t.Fatalf("decodeColumnManifestRecords v1: %v", err)
	}
	if len(snapshot.Parts) != 1 || snapshot.Parts[0].Rows != 0 {
		t.Fatalf("unexpected v1 snapshot parts: %+v", snapshot.Parts)
	}

	scanHeader, err := decodeColumnManifestHeaderRecordForScan(header)
	if err != nil {
		t.Fatalf("decodeColumnManifestHeaderRecordForScan v1: %v", err)
	}
	if scanHeader.generation != 7 || scanHeader.expectedParts != 1 {
		t.Fatalf("unexpected v1 scan header: %+v", scanHeader)
	}
	scanRef, reason, err := decodeColumnManifestPartRefForScan(part, asset.Ref.Namespace)
	if err != nil {
		t.Fatalf("decodeColumnManifestPartRefForScan v1: %v", err)
	}
	if scanRef != asset.Ref || string(reason) != asset.Reason {
		t.Fatalf("unexpected v1 scan part: ref=%+v reason=%q", scanRef, string(reason))
	}
	rewriteRef, _, err := columnAssetRewriteManifestPartRefForPatch(part, asset.Ref.Namespace)
	if err != nil {
		t.Fatalf("columnAssetRewriteManifestPartRefForPatch v1: %v", err)
	}
	if rewriteRef != asset.Ref {
		t.Fatalf("unexpected v1 rewrite ref: %+v", rewriteRef)
	}
}

func mustEncodeColumnManifestHeaderRecordVersionM1634(t *testing.T, version uint16) []byte {
	t.Helper()
	var b bytes.Buffer
	writeManifestUint32(&b, columnManifestHeaderMagic)
	writeManifestUint16(&b, version)
	writeManifestString(&b, "events")
	writeManifestString(&b, string(ColumnPublishOperationInsert))
	writeManifestUint64(&b, 7)
	writeManifestUint64(&b, 123)
	writeManifestUint64(&b, 456)
	writeManifestUint64(&b, 42)
	writeManifestUint64(&b, 100)
	writeManifestUint64(&b, 20)
	writeManifestUint64(&b, 80)
	writeManifestUint64(&b, 1)
	return b.Bytes()
}

func mustEncodeColumnManifestPartRecordVersionM1634(t *testing.T, asset ColumnPreparedAsset, version uint16) []byte {
	t.Helper()
	var b bytes.Buffer
	writeManifestUint32(&b, columnManifestPartMagic)
	writeManifestUint16(&b, version)
	writeManifestString(&b, string(asset.Ref.Kind))
	writeManifestString(&b, asset.Ref.Namespace)
	writeManifestUint64(&b, asset.Ref.Generation)
	writeManifestUint64(&b, asset.Ref.PartID)
	writeManifestUint64(&b, uint64(asset.Ref.FileID))
	writeManifestUint64(&b, uint64(asset.Ref.Offset))
	writeManifestUint64(&b, uint64(asset.Ref.Length))
	writeManifestUint64(&b, uint64(asset.Ref.Checksum))
	if version >= columnManifestRecordVersionV2 {
		writeManifestUint64(&b, uint64(asset.Rows))
	}
	writeManifestUint64(&b, uint64(asset.Bytes))
	writeManifestUint64(&b, asset.PublishID)
	writeManifestUint64(&b, asset.GenerationID)
	writeManifestString(&b, asset.Reason)
	if version >= columnManifestRecordVersion {
		writeManifestString(&b, string(asset.PartRole))
	}
	return b.Bytes()
}
