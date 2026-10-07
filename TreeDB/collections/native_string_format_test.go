package collections

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestColumnFieldRowLocatorFlattenedCanonical(t *testing.T) {
	old := columnRowCoordinates{1, columnPhysicalRowAssetPartID, 7, 1}
	latest := DocumentRowRef{DocumentID: []byte("id"), Generation: 3, PartID: columnPhysicalRowAssetPartID, RowIndex: 2, AppliedCommandLSN: 3}
	raw, err := encodeColumnFieldRowLocator(latest, []columnRowCoordinates{old, {}, old, {}})
	if err != nil {
		t.Fatal(err)
	}
	sources, err := decodeColumnFieldSources(latest.DocumentID, raw, 4)
	if err != nil {
		t.Fatal(err)
	}
	if sources[0] != old || sources[2] != old || sources[1] != columnCoordinates(latest) {
		t.Fatalf("sources %v", sources)
	}
	next := latest
	next.Generation = 4
	next.AppliedCommandLSN = 4
	sources[2] = columnRowCoordinates{}
	second, err := encodeColumnFieldRowLocator(next, sources)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeColumnFieldSources(latest.DocumentID, second, 4)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != old || got[1] != columnCoordinates(latest) || got[2] != columnCoordinates(next) {
		t.Fatalf("flattening lost exact sources: %v", got)
	}
	if len(second) != 44+3*32+4*4 {
		t.Fatal("unexpected chained representation")
	}
	for _, mutate := range []func([]byte){
		func(b []byte) { binary.BigEndian.PutUint32(b[36:], ^uint32(0)) },
		func(b []byte) { binary.BigEndian.PutUint32(b[40:], 5) },
		func(b []byte) { binary.BigEndian.PutUint32(b[len(b)-4:], 3) },
		func(b []byte) { copy(b[44+32:44+64], b[44:44+32]) },
	} {
		bad := bytes.Clone(second)
		mutate(bad)
		if _, err := decodeColumnFieldSources(latest.DocumentID, bad, 4); err == nil {
			t.Fatalf("admitted malformed %x", bad)
		}
	}
	if _, err := decodeColumnFieldSources(latest.DocumentID, raw, 3); err == nil {
		t.Fatal("schema mismatch admitted")
	}
}

func TestColumnSparseAssetStoredMissingAndNull(t *testing.T) {
	cols := []ColumnStoreColumn{{Name: "a", Path: "a", ValueType: ColumnStoreValueString, Nullable: true}, {Name: "b", Path: "b", ValueType: ColumnStoreValueString, Nullable: true}, {Name: "c", Path: "c", ValueType: ColumnStoreValueString, Nullable: true}}
	rows := []columnDeclaredRow{{ID: []byte("id"), Stored: []bool{true, false, true}, Values: []columnDeclaredValue{{Type: ColumnStoreValueString, Present: true, String: ""}, {}, {Type: ColumnStoreValueString, Present: true, String: "last"}}}}
	raw, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{Collection: "test", Namespace: "test/assets", Generation: 2, PartID: columnPhysicalRowAssetPartID, AppliedCommandLSN: 2, Operation: ColumnPublishOperationUpdate, SchemaHash: 1, Columns: cols, Rows: rows})
	if err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint16(raw[4:]) != columnPhysicalAssetVersionV10 {
		t.Fatal("not sparse version")
	}
	asset, err := decodeColumnPhysicalAsset(raw)
	if err != nil {
		t.Fatal(err)
	}
	row := asset.Rows[0]
	if !row.Stored[0] || row.Stored[1] || !row.Stored[2] || !row.Values[0].Present || row.Values[0].Null || row.Values[2].Null || row.Values[2].String != "last" {
		t.Fatalf("sparse slot semantics %+v", row)
	}
	rows[0].Values[2].Null = true
	if _, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{Collection: "test", Namespace: "test/assets", Generation: 2, PartID: 1, AppliedCommandLSN: 2, Operation: ColumnPublishOperationUpdate, SchemaHash: 1, Columns: cols, Rows: rows}); err == nil {
		t.Fatal("owned sparse null admitted")
	}
	rows[0].Values[2].Null = false
	rows[0].Stored = []bool{true}
	if _, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{Collection: "test", Namespace: "test/assets", Generation: 2, PartID: 1, AppliedCommandLSN: 2, Operation: ColumnPublishOperationUpdate, SchemaHash: 1, Columns: cols, Rows: rows}); err == nil {
		t.Fatal("invalid bitmap admitted")
	}
}
