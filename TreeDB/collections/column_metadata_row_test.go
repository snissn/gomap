package collections

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

func TestMetadataRowLocator4769(t *testing.T) {
	id := []byte("doc")
	old := DocumentRowRef{DocumentID: id, Generation: 2, PartID: 1, RowIndex: 3, AppliedCommandLSN: 5}
	latest := DocumentRowRef{DocumentID: id, Generation: 7, PartID: 1, RowIndex: 0, AppliedCommandLSN: 9}
	for _, encoded := range [][]byte{encodeColumnPrimaryRowLocator(old), encodeColumnMetadataRowLocator(latest, columnCoordinates(old))} {
		scoring, err := decodeColumnScoringRowLocatorBorrowedID(id, encoded)
		if err != nil || !reflect.DeepEqual(scoring, old) {
			t.Fatalf("scoring=%+v err=%v", scoring, err)
		}
	}
	encoded := encodeColumnMetadataRowLocator(latest, columnCoordinates(old))
	got, err := decodeColumnPrimaryRowLocator(id, encoded)
	if err != nil || !reflect.DeepEqual(got, latest) {
		t.Fatalf("latest=%+v err=%v", got, err)
	}
	for n := range len(encoded) {
		if _, err := decodeColumnPrimaryRowLocator(id, encoded[:n]); err == nil {
			t.Fatalf("accepted prefix %d", n)
		}
	}
	for _, offset := range []int{4, 12, 28, 36, 44, 60} {
		bad := bytes.Clone(encoded)
		clear(bad[offset : offset+8])
		if _, err := decodeColumnPrimaryRowLocator(id, bad); err == nil {
			t.Fatalf("accepted zero coordinate at %d", offset)
		}
	}
	bad := encodeColumnMetadataRowLocator(latest, columnCoordinates(latest))
	if _, err := decodeColumnPrimaryRowLocator(id, bad); err == nil {
		t.Fatal("accepted self reference")
	}
}

func TestMetadataPhysicalRowCodec4769(t *testing.T) {
	columns := []ColumnStoreColumn{
		{Name: "content", Path: "content", ValueType: ColumnStoreValueString},
		{Name: "tenant", Path: "meta.tenant", ValueType: ColumnStoreValueString},
	}
	prior := columnRowCoordinates{Generation: 2, PartID: 1, RowIndex: 3, AppliedCommandLSN: 5}
	input := columnPhysicalAssetEncodeInput{
		Collection: "docs", Namespace: "docs", Generation: 7, PartID: 1,
		AppliedCommandLSN: 9, Operation: ColumnPublishOperationUpdate, SchemaHash: 13, Columns: columns,
		Rows: []columnDeclaredRow{{ID: []byte("doc"), Preserved: &prior, Values: []columnDeclaredValue{{}, {Type: ColumnStoreValueString, Present: true, String: "new-tenant"}}}},
	}
	raw, _, err := encodeColumnPhysicalAsset(input)
	if err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint16(raw[4:]) != columnPhysicalAssetVersionV9 {
		t.Fatal("metadata row did not select v9")
	}
	decoded, err := decodeColumnPhysicalAsset(raw)
	if err != nil || !reflect.DeepEqual(decoded.Rows, input.Rows) {
		t.Fatalf("decoded=%+v err=%v", decoded.Rows, err)
	}
	for n := range len(raw) {
		if _, err := decodeColumnPhysicalAsset(raw[:n]); err == nil {
			t.Fatalf("accepted prefix %d", n)
		}
	}
	if _, err := decodeColumnPhysicalAsset(append(bytes.Clone(raw), 0)); err == nil {
		t.Fatal("accepted trailing bytes")
	}
	input.Rows[0].Preserved = &columnRowCoordinates{Generation: 7, PartID: 1, AppliedCommandLSN: 9}
	if _, _, err := encodeColumnPhysicalAsset(input); err == nil {
		t.Fatal("accepted self reference")
	}
}
