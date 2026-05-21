package collections

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
)

var (
	columnPhysicalAssetBenchBytes []byte
	columnPhysicalAssetBenchRows  []columnDeclaredRow
	columnPhysicalAssetBenchAsset columnPhysicalAsset
	columnPhysicalAssetBenchRef   ColumnAssetRef
	columnPhysicalScanBenchRows   int64
	columnPhysicalScanBenchSum    int64
)

func encodeColumnPhysicalAssetV1ForTest(t *testing.T, input columnPhysicalAssetEncodeInput) []byte {
	t.Helper()
	var b bytes.Buffer
	writeManifestUint32(&b, columnPhysicalAssetMagic)
	writeManifestUint16(&b, columnPhysicalAssetVersionV1)
	writeManifestString(&b, input.Collection)
	writeManifestString(&b, input.Namespace)
	writeManifestUint64(&b, input.Generation)
	writeManifestUint64(&b, input.PartID)
	writeManifestUint64(&b, input.AppliedCommandLSN)
	writeManifestString(&b, string(input.Operation))
	writeManifestUint64(&b, input.SchemaHash)
	writeManifestUint64(&b, uint64(len(input.Columns)))
	writeManifestUint64(&b, uint64(len(input.Rows)))
	for _, col := range input.Columns {
		writeManifestString(&b, col.Name)
		writeManifestString(&b, col.Path)
		writeManifestString(&b, string(col.ValueType))
		writeManifestBool(&b, col.Nullable)
		writeManifestBool(&b, col.Dictionary)
	}
	for rowIdx, row := range input.Rows {
		if row.Deleted {
			t.Fatalf("v1 compatibility asset row[%d] is deleted", rowIdx)
		}
		if len(row.Values) != len(input.Columns) {
			t.Fatalf("v1 compatibility asset row[%d] values=%d columns=%d", rowIdx, len(row.Values), len(input.Columns))
		}
		writeManifestBytes(&b, row.ID)
		for _, value := range row.Values {
			writeManifestString(&b, string(value.Type))
			writeManifestBool(&b, value.Null)
			if value.Null {
				continue
			}
			switch value.Type {
			case ColumnStoreValueBool:
				writeManifestBool(&b, value.Bool)
			case ColumnStoreValueInt64:
				writeManifestUint64(&b, uint64(value.Int64))
			case ColumnStoreValueDouble:
				writeManifestUint64(&b, math.Float64bits(value.Double))
			case ColumnStoreValueString:
				writeManifestString(&b, value.String)
			default:
				t.Fatalf("unsupported v1 compatibility value type %q", value.Type)
			}
		}
	}
	return b.Bytes()
}

func TestColumnPhysicalAssetCodecRoundTripM12A(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	rows, err := extractColumnDeclaredRowsFromJSONDocuments(*normalized, []columnWriteDocument{
		{ID: []byte("e1"), Document: []byte(`{"time_us":1,"kind":"like","did":"d1","extra":"ignored"}`)},
		{ID: []byte("e2"), Document: []byte(`{"time_us":2,"kind":"post","did":"d2"}`)},
	})
	if err != nil {
		t.Fatalf("extractColumnDeclaredRowsFromJSONDocuments: %v", err)
	}
	if len(rows) != 2 || len(rows[0].Values) != len(normalized.Columns) {
		t.Fatalf("unexpected extracted rows: %+v", rows)
	}

	encoded, summary, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        "events",
		Namespace:         normalized.AssetManager.Namespace,
		Generation:        7,
		PartID:            3,
		AppliedCommandLSN: 101,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        normalized.SchemaHash,
		Columns:           normalized.Columns,
		Rows:              rows,
	})
	if err != nil {
		t.Fatalf("encodeColumnPhysicalAsset: %v", err)
	}
	if len(encoded) == 0 || summary.RowCount != 2 || summary.ColumnCount != 3 || summary.PayloadBytes != int64(len(encoded)) {
		t.Fatalf("unexpected asset summary=%+v len=%d", summary, len(encoded))
	}

	decoded, err := decodeColumnPhysicalAsset(encoded)
	if err != nil {
		t.Fatalf("decodeColumnPhysicalAsset: %v", err)
	}
	if decoded.Header.Collection != "events" || decoded.Header.Namespace != normalized.AssetManager.Namespace || decoded.Header.Generation != 7 || decoded.Header.PartID != 3 {
		t.Fatalf("unexpected decoded header: %+v", decoded.Header)
	}
	if got := decoded.Rows[0].Values[0].Int64; got != 1 {
		t.Fatalf("time_us row0=%d want 1", got)
	}
	if got := decoded.Rows[1].Values[1].String; got != "post" {
		t.Fatalf("kind row1=%q want post", got)
	}

	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  normalized.AssetManager.Namespace,
		Generation: 7,
		PartID:     3,
		FileID:     9,
		Offset:     4096,
		Length:     int64(len(encoded)),
		Checksum:   page.Checksum(encoded),
	}
	if err := validateColumnPhysicalAssetForManifest(encoded, ref, *normalized); err != nil {
		t.Fatalf("validateColumnPhysicalAssetForManifest: %v", err)
	}
	wrongSchema := *normalized
	wrongSchema.Columns = append([]ColumnStoreColumn(nil), normalized.Columns...)
	wrongSchema.Columns[0].Path = "wrong_time_us"
	if err := validateColumnPhysicalAssetForManifest(encoded, ref, wrongSchema); err == nil || !strings.Contains(err.Error(), "column[0]") {
		t.Fatalf("validate wrong schema err=%v want column mismatch failure", err)
	}
	ref.Namespace = "other/column-assets"
	if err := validateColumnPhysicalAssetForManifest(encoded, ref, *normalized); err == nil || !strings.Contains(err.Error(), "namespace") {
		t.Fatalf("validate wrong namespace err=%v want namespace failure", err)
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-1] ^= 0x7f
	ref.Namespace = normalized.AssetManager.Namespace
	if err := validateColumnPhysicalAssetForManifest(corrupt, ref, *normalized); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("validate corrupt asset err=%v want checksum failure", err)
	}
}

func TestColumnPhysicalAssetVectorValueTypesRoundTrip(t *testing.T) {
	cfg := &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 3},
			{Name: "embedding_inv_norm", Path: "embedding_inv_norm", ValueType: ColumnStoreValueFloat32},
			{Name: "embedding_neighbors", Path: "embedding_neighbors", ValueType: ColumnStoreValueAdjacencyList},
		},
	}
	normalized, err := normalizeColumnStoreConfig("vectors", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	rows, err := extractColumnDeclaredRowsFromJSONDocuments(*normalized, []columnWriteDocument{{
		ID:       []byte("v1"),
		Document: []byte(`{"embedding":[1.0,0.5,-0.25],"embedding_inv_norm":0.87287156,"embedding_neighbors":[7,11,13,17]}`),
	}})
	if err != nil {
		t.Fatalf("extractColumnDeclaredRowsFromJSONDocuments: %v", err)
	}
	if got := rows[0].Values[0].Float32Vector; len(got) != 3 || got[0] != 1 || got[1] != 0.5 || got[2] != -0.25 {
		t.Fatalf("extracted vector=%v", got)
	}
	if got := rows[0].Values[1].Float32; math.Abs(float64(got-0.87287156)) > 1e-7 {
		t.Fatalf("extracted inv_norm=%g", got)
	}
	if got := rows[0].Values[2].AdjacencyList; len(got) != 4 || got[0] != 7 || got[3] != 17 {
		t.Fatalf("extracted adjacency=%v", got)
	}

	encoded, summary, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        "vectors",
		Namespace:         normalized.AssetManager.Namespace,
		Generation:        2,
		PartID:            1,
		AppliedCommandLSN: 9,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        normalized.SchemaHash,
		Columns:           normalized.Columns,
		Rows:              rows,
	})
	if err != nil {
		t.Fatalf("encodeColumnPhysicalAsset: %v", err)
	}
	if summary.RowCount != 1 || summary.ColumnCount != 3 || summary.PayloadBytes != int64(len(encoded)) {
		t.Fatalf("unexpected summary=%+v len=%d", summary, len(encoded))
	}
	decoded, err := decodeColumnPhysicalAsset(encoded)
	if err != nil {
		t.Fatalf("decodeColumnPhysicalAsset: %v", err)
	}
	if got := decoded.Columns[0].VectorDims; got != 3 {
		t.Fatalf("decoded vector_dims=%d want 3", got)
	}
	if got := decoded.Rows[0].Values[0].Float32Vector; len(got) != 3 || got[2] != -0.25 {
		t.Fatalf("decoded vector=%v", got)
	}
	if got := decoded.Rows[0].Values[2].AdjacencyList; len(got) != 4 || got[1] != 11 {
		t.Fatalf("decoded adjacency=%v", got)
	}
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  normalized.AssetManager.Namespace,
		Generation: 2,
		PartID:     1,
		FileID:     columnAssetM12ASegmentFileID,
		Length:     int64(len(encoded)),
		Checksum:   page.Checksum(encoded),
	}
	if err := validateColumnPhysicalAssetForManifest(encoded, ref, *normalized); err != nil {
		t.Fatalf("validateColumnPhysicalAssetForManifest: %v", err)
	}

	projection, err := newColumnPhysicalScanProjection(*normalized, []string{"embedding", "embedding_inv_norm", "embedding_neighbors"})
	if err != nil {
		t.Fatalf("newColumnPhysicalScanProjection: %v", err)
	}
	visited := 0
	_, err = scanColumnPhysicalAssetRows(encoded, ref, "vectors", normalized, projection, func(row columnPhysicalScanRowView) error {
		visited++
		if got := row.Values[0].Float32Vector; len(got) != 3 || got[0] != 1 || got[2] != -0.25 {
			t.Fatalf("scanned vector=%v", got)
		}
		if got := row.Values[1].Float32; math.Abs(float64(got-0.87287156)) > 1e-7 {
			t.Fatalf("scanned inv_norm=%g", got)
		}
		if got := row.Values[2].AdjacencyList; len(got) != 4 || got[3] != 17 {
			t.Fatalf("scanned adjacency=%v", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scanColumnPhysicalAssetRows: %v", err)
	}
	if visited != 1 {
		t.Fatalf("visited=%d want 1", visited)
	}
}

func TestColumnPhysicalAssetFloat32VectorLittleEndianRoundTrip(t *testing.T) {
	cfg := &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 3, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian},
			{Name: "embedding_inv_norm", Path: "embedding_inv_norm", ValueType: ColumnStoreValueFloat32},
			{Name: "embedding_neighbors", Path: "embedding_neighbors", ValueType: ColumnStoreValueAdjacencyList},
		},
	}
	normalized, err := normalizeColumnStoreConfig("vectors", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	rows := []columnDeclaredRow{{
		ID: []byte("v1"),
		Values: []columnDeclaredValue{
			{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: []float32{
				math.Float32frombits(0x3f800000),
				math.Float32frombits(0xc0200000),
				math.Float32frombits(0x7fc12345),
			}},
			{Type: ColumnStoreValueFloat32, Present: true, Float32: math.Float32frombits(0x3e800000)},
			{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{0x01020304, 0xa0b0c0d0}},
		},
	}}
	encoded, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        "vectors",
		Namespace:         normalized.AssetManager.Namespace,
		Generation:        2,
		PartID:            1,
		AppliedCommandLSN: 9,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        normalized.SchemaHash,
		Columns:           normalized.Columns,
		Rows:              rows,
	})
	if err != nil {
		t.Fatalf("encodeColumnPhysicalAsset: %v", err)
	}
	assertColumnPhysicalAssetFloat32VectorLittleEndianFixture(t, encoded, normalized)

	decoded, err := decodeColumnPhysicalAsset(encoded)
	if err != nil {
		t.Fatalf("decodeColumnPhysicalAsset: %v", err)
	}
	if got := decoded.Columns[0].FixedWidthEncoding; got != ColumnFixedWidthEncodingLittleEndian {
		t.Fatalf("decoded fixed_width_encoding=%q want little_endian", got)
	}
	if got := math.Float32bits(decoded.Rows[0].Values[0].Float32Vector[2]); got != 0x7fc12345 {
		t.Fatalf("decoded vector nan bits=0x%08x want 0x7fc12345", got)
	}
	if got := math.Float32bits(decoded.Rows[0].Values[1].Float32); got != 0x3e800000 {
		t.Fatalf("decoded scalar bits=0x%08x want 0x3e800000", got)
	}
	if got := decoded.Rows[0].Values[2].AdjacencyList; len(got) != 2 || got[0] != 0x01020304 || got[1] != 0xa0b0c0d0 {
		t.Fatalf("decoded adjacency=%x", got)
	}

	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  normalized.AssetManager.Namespace,
		Generation: 2,
		PartID:     1,
		FileID:     columnAssetM12ASegmentFileID,
		Length:     int64(len(encoded)),
		Checksum:   page.Checksum(encoded),
	}
	if err := validateColumnPhysicalAssetForManifest(encoded, ref, *normalized); err != nil {
		t.Fatalf("validateColumnPhysicalAssetForManifest: %v", err)
	}
	projection, err := newColumnPhysicalScanProjection(*normalized, []string{"embedding", "embedding_inv_norm", "embedding_neighbors"})
	if err != nil {
		t.Fatalf("newColumnPhysicalScanProjection: %v", err)
	}
	visited := 0
	_, err = scanColumnPhysicalAssetRows(encoded, ref, "vectors", normalized, projection, func(row columnPhysicalScanRowView) error {
		visited++
		if got := math.Float32bits(row.Values[0].Float32Vector[2]); got != 0x7fc12345 {
			t.Fatalf("scanned vector nan bits=0x%08x want 0x7fc12345", got)
		}
		if got := math.Float32bits(row.Values[1].Float32); got != 0x3e800000 {
			t.Fatalf("scanned scalar bits=0x%08x want 0x3e800000", got)
		}
		if got := row.Values[2].AdjacencyList; len(got) != 2 || got[1] != 0xa0b0c0d0 {
			t.Fatalf("scanned adjacency=%x", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scanColumnPhysicalAssetRows: %v", err)
	}
	if visited != 1 {
		t.Fatalf("visited=%d want 1", visited)
	}

	mismatch := *normalized
	mismatch.Columns = append([]ColumnStoreColumn(nil), normalized.Columns...)
	mismatch.Columns[0].FixedWidthEncoding = ColumnFixedWidthEncodingDefault
	mismatch.SchemaHash = normalized.SchemaHash
	if err := validateColumnPhysicalAssetForManifest(encoded, ref, mismatch); err == nil || !strings.Contains(err.Error(), "column[0]") {
		t.Fatalf("validate metadata mismatch err=%v want column mismatch", err)
	}
}

func assertColumnPhysicalAssetFloat32VectorLittleEndianFixture(t *testing.T, encoded []byte, cfg *ColumnStoreConfig) {
	t.Helper()
	cur := manifestCursor{raw: encoded}
	if magic := cur.u32(); magic != columnPhysicalAssetMagic {
		t.Fatalf("magic=0x%08x want 0x%08x", magic, columnPhysicalAssetMagic)
	}
	if version := cur.u16(); version != columnPhysicalAssetVersionV5 {
		t.Fatalf("version=%d want %d", version, columnPhysicalAssetVersionV5)
	}
	_ = cur.stringBytes()
	_ = cur.stringBytes()
	_ = cur.u64()
	_ = cur.u64()
	_ = cur.u64()
	_ = cur.stringBytes()
	_ = cur.u64()
	columnCount := cur.u64()
	rowCount := cur.u64()
	if cur.err != nil {
		t.Fatalf("header cursor err=%v", cur.err)
	}
	if columnCount != uint64(len(cfg.Columns)) || rowCount != 1 {
		t.Fatalf("column_count=%d row_count=%d", columnCount, rowCount)
	}
	for i, col := range cfg.Columns {
		_ = cur.stringBytes()
		_ = cur.stringBytes()
		_ = cur.stringBytes()
		_ = cur.bool()
		_ = cur.bool()
		_ = cur.u64()
		if got := ColumnFixedWidthEncoding(cur.string()); got != col.FixedWidthEncoding {
			t.Fatalf("column[%d] fixed_width_encoding=%q want %q", i, got, col.FixedWidthEncoding)
		}
	}
	_ = cur.bytesView()
	_ = cur.bool()
	_ = cur.stringBytes()
	_ = cur.bool()
	_ = cur.bool()
	if n := cur.u64(); n != 3 {
		t.Fatalf("vector length=%d want 3", n)
	}
	if got, want := encoded[cur.pos:cur.pos+12], []byte{0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x20, 0xc0, 0x45, 0x23, 0xc1, 0x7f}; !bytes.Equal(got, want) {
		t.Fatalf("vector payload bytes=% x want % x", got, want)
	}
	cur.pos += 12
	_ = cur.stringBytes()
	_ = cur.bool()
	_ = cur.bool()
	if got, want := encoded[cur.pos:cur.pos+4], []byte{0x3e, 0x80, 0x00, 0x00}; !bytes.Equal(got, want) {
		t.Fatalf("float32 payload bytes=% x want % x", got, want)
	}
	cur.pos += 4
	_ = cur.stringBytes()
	_ = cur.bool()
	_ = cur.bool()
	if n := cur.u64(); n != 2 {
		t.Fatalf("adjacency length=%d want 2", n)
	}
	if got, want := encoded[cur.pos:cur.pos+8], []byte{0x01, 0x02, 0x03, 0x04, 0xa0, 0xb0, 0xc0, 0xd0}; !bytes.Equal(got, want) {
		t.Fatalf("adjacency payload bytes=% x want % x", got, want)
	}
	cur.pos += 8
	if cur.err != nil {
		t.Fatalf("cursor err=%v", cur.err)
	}
	if cur.pos != len(encoded) {
		t.Fatalf("cursor pos=%d len=%d", cur.pos, len(encoded))
	}
}

func TestEncodeColumnPhysicalAssetRejectsMismatchedVectorLength(t *testing.T) {
	cfg := &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 3},
			{Name: "embedding_inv_norm", Path: "embedding_inv_norm", ValueType: ColumnStoreValueFloat32},
			{Name: "embedding_neighbors", Path: "embedding_neighbors", ValueType: ColumnStoreValueAdjacencyList},
		},
	}
	normalized, err := normalizeColumnStoreConfig("vectors", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	_, _, err = encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        "vectors",
		Namespace:         normalized.AssetManager.Namespace,
		Generation:        2,
		PartID:            1,
		AppliedCommandLSN: 9,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        normalized.SchemaHash,
		Columns:           normalized.Columns,
		Rows: []columnDeclaredRow{{
			ID: []byte("v1"),
			Values: []columnDeclaredValue{
				{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: []float32{1, 2}},
				{Type: ColumnStoreValueFloat32, Present: true, Float32: 0.5},
				{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{7, 11}},
			},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "row[0] column[0]: float32_vector length=2 want vector_dims=3") {
		t.Fatalf("encodeColumnPhysicalAsset err=%v want vector length mismatch", err)
	}
}

func TestDecodeColumnPhysicalAssetRejectsMismatchedVectorLength(t *testing.T) {
	var raw bytes.Buffer
	writeManifestUint32(&raw, columnPhysicalAssetMagic)
	writeManifestUint16(&raw, columnPhysicalAssetVersion)
	writeManifestString(&raw, "vectors")
	writeManifestString(&raw, "vectors/column-assets")
	writeManifestUint64(&raw, 2)
	writeManifestUint64(&raw, 1)
	writeManifestUint64(&raw, 9)
	writeManifestString(&raw, string(ColumnPublishOperationInsert))
	writeManifestUint64(&raw, 123)
	writeManifestUint64(&raw, 1)
	writeManifestUint64(&raw, 1)
	writeManifestString(&raw, "embedding")
	writeManifestString(&raw, "embedding")
	writeManifestString(&raw, string(ColumnStoreValueFloat32Vector))
	writeManifestBool(&raw, false)
	writeManifestBool(&raw, false)
	writeManifestUint64(&raw, 3)
	writeManifestBytes(&raw, []byte("v1"))
	writeManifestBool(&raw, false)
	writeManifestString(&raw, string(ColumnStoreValueFloat32Vector))
	writeManifestBool(&raw, false)
	writeManifestBool(&raw, true)
	writeManifestFloat32Slice(&raw, []float32{1, 2})

	_, err := decodeColumnPhysicalAsset(raw.Bytes())
	if err == nil || !strings.Contains(err.Error(), "float32_vector length=2 want vector_dims=3") {
		t.Fatalf("decodeColumnPhysicalAsset err=%v want vector length mismatch", err)
	}
}

func TestColumnPhysicalAssetScannerRejectsMismatchedVectorLength(t *testing.T) {
	tests := []struct {
		name      string
		projected []string
		want      string
	}{
		{name: "unprojected", projected: []string{"score"}, want: "float32_vector length=2 want vector_dims=3"},
		{name: "projected", projected: []string{"embedding"}, want: "float32_vector length=2 want vector_dims=3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{
					{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 3},
					{Name: "score", Path: "score", ValueType: ColumnStoreValueFloat32},
				},
			}
			normalized, err := normalizeColumnStoreConfig("vectors", cfg)
			if err != nil {
				t.Fatalf("normalizeColumnStoreConfig: %v", err)
			}

			var raw bytes.Buffer
			writeManifestUint32(&raw, columnPhysicalAssetMagic)
			writeManifestUint16(&raw, columnPhysicalAssetVersion)
			writeManifestString(&raw, "vectors")
			writeManifestString(&raw, normalized.AssetManager.Namespace)
			writeManifestUint64(&raw, 2)
			writeManifestUint64(&raw, 1)
			writeManifestUint64(&raw, 9)
			writeManifestString(&raw, string(ColumnPublishOperationInsert))
			writeManifestUint64(&raw, normalized.SchemaHash)
			writeManifestUint64(&raw, uint64(len(normalized.Columns)))
			writeManifestUint64(&raw, 1)
			for _, col := range normalized.Columns {
				writeManifestString(&raw, col.Name)
				writeManifestString(&raw, col.Path)
				writeManifestString(&raw, string(col.ValueType))
				writeManifestBool(&raw, col.Nullable)
				writeManifestBool(&raw, col.Dictionary)
				writeManifestUint64(&raw, uint64(col.VectorDims))
			}
			writeManifestBytes(&raw, []byte("v1"))
			writeManifestBool(&raw, false)
			writeManifestString(&raw, string(ColumnStoreValueFloat32Vector))
			writeManifestBool(&raw, false)
			writeManifestBool(&raw, true)
			writeManifestFloat32Slice(&raw, []float32{1, 2})
			writeManifestString(&raw, string(ColumnStoreValueFloat32))
			writeManifestBool(&raw, false)
			writeManifestBool(&raw, true)
			writeManifestUint32(&raw, math.Float32bits(0.5))

			encoded := raw.Bytes()
			ref := ColumnAssetRef{
				Kind:       ColumnAssetKindTCS1PartImage,
				Namespace:  normalized.AssetManager.Namespace,
				Generation: 2,
				PartID:     1,
				FileID:     columnAssetM12ASegmentFileID,
				Length:     int64(len(encoded)),
				Checksum:   page.Checksum(encoded),
			}
			projection, err := newColumnPhysicalScanProjection(*normalized, tt.projected)
			if err != nil {
				t.Fatalf("newColumnPhysicalScanProjection: %v", err)
			}
			_, err = scanColumnPhysicalAssetRows(encoded, ref, "vectors", normalized, projection, func(row columnPhysicalScanRowView) error {
				t.Fatalf("visitor should not run for mismatched vector length: %+v", row)
				return nil
			})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("scanColumnPhysicalAssetRows err=%v want %q", err, tt.want)
			}
		})
	}
}

func TestColumnPhysicalAssetDecodeV1CompatibilityM12C(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	rows, err := extractColumnDeclaredRowsFromJSONDocuments(*normalized, []columnWriteDocument{
		{ID: []byte("e1"), Document: []byte(`{"time_us":1,"kind":"like","did":"d1"}`)},
		{ID: []byte("e2"), Document: []byte(`{"time_us":2,"kind":"post","did":"d2"}`)},
	})
	if err != nil {
		t.Fatalf("extractColumnDeclaredRowsFromJSONDocuments: %v", err)
	}
	encoded := encodeColumnPhysicalAssetV1ForTest(t, columnPhysicalAssetEncodeInput{
		Collection:        "events",
		Namespace:         normalized.AssetManager.Namespace,
		Generation:        7,
		PartID:            3,
		AppliedCommandLSN: 101,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        normalized.SchemaHash,
		Columns:           normalized.Columns,
		Rows:              rows,
	})

	decoded, err := decodeColumnPhysicalAsset(encoded)
	if err != nil {
		t.Fatalf("decodeColumnPhysicalAsset v1: %v", err)
	}
	if decoded.Header.Operation != ColumnPublishOperationInsert || len(decoded.Rows) != 2 {
		t.Fatalf("unexpected v1 decoded asset: header=%+v rows=%+v", decoded.Header, decoded.Rows)
	}
	for i, row := range decoded.Rows {
		if row.Deleted {
			t.Fatalf("v1 decoded row[%d] marked deleted", i)
		}
		if len(row.Values) != len(normalized.Columns) {
			t.Fatalf("v1 decoded row[%d] values=%d want %d", i, len(row.Values), len(normalized.Columns))
		}
	}
	if got := decoded.Rows[1].Values[1].String; got != "post" {
		t.Fatalf("v1 decoded kind row1=%q want post", got)
	}
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  normalized.AssetManager.Namespace,
		Generation: 7,
		PartID:     3,
		FileID:     1,
		Offset:     0,
		Length:     int64(len(encoded)),
		Checksum:   page.Checksum(encoded),
	}
	if err := validateColumnPhysicalAssetForManifest(encoded, ref, *normalized); err != nil {
		t.Fatalf("validateColumnPhysicalAssetForManifest v1: %v", err)
	}
}

func TestColumnPhysicalAssetRejectsUnsupportedOperationEmptyRowsM12C(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	badOperation := ColumnPublishOperation("rewrite")
	if _, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        "events",
		Namespace:         normalized.AssetManager.Namespace,
		Generation:        10,
		PartID:            1,
		AppliedCommandLSN: 104,
		Operation:         badOperation,
		SchemaHash:        normalized.SchemaHash,
		Columns:           normalized.Columns,
	}); err == nil || !strings.Contains(err.Error(), "unsupported column physical asset operation") {
		t.Fatalf("encode unsupported operation err=%v want unsupported operation failure", err)
	}

	var raw bytes.Buffer
	writeManifestUint32(&raw, columnPhysicalAssetMagic)
	writeManifestUint16(&raw, columnPhysicalAssetVersion)
	writeManifestString(&raw, "events")
	writeManifestString(&raw, normalized.AssetManager.Namespace)
	writeManifestUint64(&raw, 10)
	writeManifestUint64(&raw, 1)
	writeManifestUint64(&raw, 104)
	writeManifestString(&raw, string(badOperation))
	writeManifestUint64(&raw, normalized.SchemaHash)
	writeManifestUint64(&raw, uint64(len(normalized.Columns)))
	writeManifestUint64(&raw, 0)
	for _, col := range normalized.Columns {
		writeManifestString(&raw, col.Name)
		writeManifestString(&raw, col.Path)
		writeManifestString(&raw, string(col.ValueType))
		writeManifestBool(&raw, col.Nullable)
		writeManifestBool(&raw, col.Dictionary)
		writeManifestUint64(&raw, uint64(col.VectorDims))
	}
	encoded := raw.Bytes()
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  normalized.AssetManager.Namespace,
		Generation: 10,
		PartID:     1,
		FileID:     1,
		Offset:     0,
		Length:     int64(len(encoded)),
		Checksum:   page.Checksum(encoded),
	}
	if err := validateColumnPhysicalAssetForManifest(encoded, ref, *normalized); err == nil || !strings.Contains(err.Error(), "unsupported column physical asset operation") {
		t.Fatalf("validate unsupported operation err=%v want unsupported operation failure", err)
	}
}

func TestColumnPhysicalAssetDeleteRowsM12C(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	rows := []columnDeclaredRow{
		{ID: []byte("e1"), Deleted: true},
		{ID: []byte("e2"), Deleted: true},
	}
	encoded, summary, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        "events",
		Namespace:         normalized.AssetManager.Namespace,
		Generation:        8,
		PartID:            1,
		AppliedCommandLSN: 102,
		Operation:         ColumnPublishOperationDelete,
		SchemaHash:        normalized.SchemaHash,
		Columns:           normalized.Columns,
		Rows:              rows,
	})
	if err != nil {
		t.Fatalf("encodeColumnPhysicalAsset delete: %v", err)
	}
	if summary.RowCount != 2 || summary.ColumnCount != len(normalized.Columns) || summary.PayloadBytes != int64(len(encoded)) {
		t.Fatalf("unexpected delete asset summary=%+v len=%d", summary, len(encoded))
	}
	decoded, err := decodeColumnPhysicalAsset(encoded)
	if err != nil {
		t.Fatalf("decodeColumnPhysicalAsset delete: %v", err)
	}
	if decoded.Header.Operation != ColumnPublishOperationDelete || len(decoded.Rows) != 2 {
		t.Fatalf("unexpected decoded delete asset: header=%+v rows=%+v", decoded.Header, decoded.Rows)
	}
	for i, row := range decoded.Rows {
		if !row.Deleted || len(row.Values) != 0 {
			t.Fatalf("decoded delete row[%d]=%+v, want deleted row without values", i, row)
		}
	}
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  normalized.AssetManager.Namespace,
		Generation: 8,
		PartID:     1,
		FileID:     1,
		Offset:     0,
		Length:     int64(len(encoded)),
		Checksum:   page.Checksum(encoded),
	}
	if err := validateColumnPhysicalAssetForManifest(encoded, ref, *normalized); err != nil {
		t.Fatalf("validateColumnPhysicalAssetForManifest delete: %v", err)
	}

	insertInput := columnPhysicalAssetEncodeInput{
		Collection:        "events",
		Namespace:         normalized.AssetManager.Namespace,
		Generation:        9,
		PartID:            1,
		AppliedCommandLSN: 103,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        normalized.SchemaHash,
		Columns:           normalized.Columns,
		Rows:              rows[:1],
	}
	if _, _, err := encodeColumnPhysicalAsset(insertInput); err == nil || !strings.Contains(err.Error(), "marked deleted") {
		t.Fatalf("encode insert with deleted row err=%v want marked deleted failure", err)
	}
	deleteWithValues := rows[:1]
	deleteWithValues[0].Values = []columnDeclaredValue{{Type: ColumnStoreValueInt64, Int64: 1}}
	deleteInput := insertInput
	deleteInput.Operation = ColumnPublishOperationDelete
	deleteInput.Rows = deleteWithValues
	if _, _, err := encodeColumnPhysicalAsset(deleteInput); err == nil || !strings.Contains(err.Error(), "values=1 want 0") {
		t.Fatalf("encode delete with values err=%v want values failure", err)
	}
}

func TestColumnPhysicalAssetRejectsNonNullableNullM12A(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	rows, err := extractColumnDeclaredRowsFromJSONDocuments(*normalized, []columnWriteDocument{
		{ID: []byte("e1"), Document: []byte(`{"time_us":1,"kind":"like","did":"d1"}`)},
	})
	if err != nil {
		t.Fatalf("extractColumnDeclaredRowsFromJSONDocuments: %v", err)
	}
	rows[0].Values[0].Null = true
	rows[0].Values[0].Int64 = 0
	if _, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        "events",
		Namespace:         normalized.AssetManager.Namespace,
		Generation:        7,
		PartID:            3,
		AppliedCommandLSN: 101,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        normalized.SchemaHash,
		Columns:           normalized.Columns,
		Rows:              rows,
	}); err == nil || !strings.Contains(err.Error(), "not nullable") {
		t.Fatalf("encode nullable violation err=%v want not nullable failure", err)
	}
}

func TestColumnPhysicalAssetRejectsInvalidAbsentValueM13C(t *testing.T) {
	cfg := &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Nullable: true},
		},
		SortKey: []ColumnSortKey{{Column: "kind"}},
	}
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	rows := []columnDeclaredRow{{
		ID: []byte("e1"),
		Values: []columnDeclaredValue{{
			Type:    ColumnStoreValueString,
			Present: false,
			Null:    false,
			String:  "invalid",
		}},
	}}
	if _, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        "events",
		Namespace:         normalized.AssetManager.Namespace,
		Generation:        1,
		PartID:            1,
		AppliedCommandLSN: 1,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        normalized.SchemaHash,
		Columns:           normalized.Columns,
		Rows:              rows,
	}); err == nil || !strings.Contains(err.Error(), "absent value is not null") {
		t.Fatalf("encode invalid absent value err=%v want absent value failure", err)
	}
}

func TestColumnAssetManagerWritesIsolatedSegmentAndValidatesM12A(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	rows, err := extractColumnDeclaredRowsFromJSONDocuments(*normalized, []columnWriteDocument{
		{ID: []byte("e1"), Document: []byte(`{"time_us":1,"kind":"like","did":"d1","extra":"ignored"}`)},
	})
	if err != nil {
		t.Fatalf("extractColumnDeclaredRowsFromJSONDocuments: %v", err)
	}
	encoded, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        "events",
		Namespace:         normalized.AssetManager.Namespace,
		Generation:        7,
		PartID:            3,
		AppliedCommandLSN: 101,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        normalized.SchemaHash,
		Columns:           normalized.Columns,
		Rows:              rows,
	})
	if err != nil {
		t.Fatalf("encodeColumnPhysicalAsset: %v", err)
	}

	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	ref, err := writeColumnPhysicalAssetToManager(root, *normalized, encoded, 7, 3)
	if err != nil {
		t.Fatalf("writeColumnPhysicalAssetToManager: %v", err)
	}
	if ref.Namespace != normalized.AssetManager.Namespace || ref.Generation != 7 || ref.PartID != 3 || ref.FileID == 0 || ref.Length != int64(len(encoded)) {
		t.Fatalf("unexpected ref: %+v", ref)
	}
	if checksum := page.Checksum(encoded); checksum != ref.Checksum {
		t.Fatalf("asset ref checksum=%d want encoded checksum=%d", ref.Checksum, checksum)
	}
	assetPath, err := columnAssetSegmentPath(root, ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	wantDir := filepath.Join(root, "events", "column-assets", "assets", "segments")
	if filepath.Dir(assetPath) != wantDir {
		t.Fatalf("asset path=%q want dir %q", assetPath, wantDir)
	}
	if strings.Contains(assetPath, "value_vlog") || strings.Contains(assetPath, "leaf_vlog") {
		t.Fatalf("column asset path must be isolated from value/leaf logs: %q", assetPath)
	}

	raw, err := readColumnPhysicalAssetFromManager(root, ref)
	if err != nil {
		t.Fatalf("readColumnPhysicalAssetFromManager: %v", err)
	}
	if err := validateColumnPhysicalAssetForManifest(raw, ref, *normalized); err != nil {
		t.Fatalf("validateColumnPhysicalAssetForManifest: %v", err)
	}

	file, err := os.OpenFile(assetPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile corrupt target: %v", err)
	}
	if _, err := file.WriteAt([]byte{raw[0] ^ 0x7f}, ref.Offset); err != nil {
		_ = file.Close()
		t.Fatalf("WriteAt corrupt target: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close corrupt target: %v", err)
	}
	if _, err := readColumnPhysicalAssetFromManager(root, ref); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("read corrupt asset err=%v want checksum failure", err)
	}
	relaxedRaw, err := readColumnPhysicalAssetFromManagerIntoWithIntegrity(root, ref, nil, ColumnAssetReadIntegritySkipChecksums)
	if err != nil {
		t.Fatalf("relaxed read corrupt asset: %v", err)
	}
	if bytes.Equal(relaxedRaw, raw) {
		t.Fatalf("relaxed read returned uncorrupted payload")
	}
	if err := validateColumnPhysicalAssetForManifest(relaxedRaw, ref, *normalized); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("manifest validation err=%v want checksum failure", err)
	}
}

func TestColumnAssetReadIntegrityLabelPreservesUnsupportedM1634(t *testing.T) {
	if got := columnAssetReadIntegrityLabel(""); got != string(ColumnAssetReadIntegrityVerify) {
		t.Fatalf("empty integrity label=%q want %q", got, ColumnAssetReadIntegrityVerify)
	}
	if got := columnAssetReadIntegrityLabel(ColumnAssetReadIntegrityCachedVerify); got != string(ColumnAssetReadIntegrityCachedVerify) {
		t.Fatalf("cached integrity label=%q want %q", got, ColumnAssetReadIntegrityCachedVerify)
	}
	if got := columnAssetReadIntegrityLabel(ColumnAssetReadIntegrity("bad-mode")); got != "bad-mode" {
		t.Fatalf("unsupported integrity label=%q want raw value", got)
	}
}

func TestColumnAssetReadIntegrityCachedVerifyFirstReadRejectsCorruptionM1634(t *testing.T) {
	resetColumnAssetVerifiedChecksumCacheForTest(t)
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	payload := []byte("cached-verify-first-read-payload")
	ref, err := writeColumnPhysicalAssetToManager(root, *normalized, payload, 7, 3)
	if err != nil {
		t.Fatalf("writeColumnPhysicalAssetToManager: %v", err)
	}
	corruptColumnAssetPayloadByte(t, root, ref)

	if _, err := readColumnPhysicalAssetFromManagerIntoWithIntegrity(root, ref, nil, ColumnAssetReadIntegrityCachedVerify); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("cached first corrupt read err=%v want checksum failure", err)
	}
	if _, err := readColumnPhysicalAssetFromManagerIntoWithIntegrity(root, ref, nil, ColumnAssetReadIntegrityCachedVerify); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("cached repeated corrupt read err=%v want checksum failure after rejected first read", err)
	}
}

func TestColumnAssetReadIntegrityCachedVerifyReusesVerifiedRefM1634(t *testing.T) {
	resetColumnAssetVerifiedChecksumCacheForTest(t)
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	payload := []byte("cached-verify-reuse-payload")
	ref, err := writeColumnPhysicalAssetToManager(root, *normalized, payload, 7, 3)
	if err != nil {
		t.Fatalf("writeColumnPhysicalAssetToManager: %v", err)
	}

	raw, err := readColumnPhysicalAssetFromManagerIntoWithIntegrity(root, ref, nil, ColumnAssetReadIntegrityCachedVerify)
	if err != nil {
		t.Fatalf("cached first read: %v", err)
	}
	if !bytes.Equal(raw, payload) {
		t.Fatalf("cached first read raw=%q want %q", raw, payload)
	}
	corruptColumnAssetPayloadByte(t, root, ref)

	if _, err := readColumnPhysicalAssetFromManager(root, ref); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("strict corrupt read err=%v want checksum failure", err)
	}
	cachedRaw, err := readColumnPhysicalAssetFromManagerIntoWithIntegrity(root, ref, nil, ColumnAssetReadIntegrityCachedVerify)
	if err != nil {
		t.Fatalf("cached repeated read after corruption: %v", err)
	}
	if bytes.Equal(cachedRaw, payload) {
		t.Fatalf("cached repeated read returned uncorrupted payload; expected corrupted bytes after file mutation")
	}

	badRef := ref
	badRef.Checksum++
	if _, err := readColumnPhysicalAssetFromManagerIntoWithIntegrity(root, badRef, nil, ColumnAssetReadIntegrityCachedVerify); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("cached read with changed checksum err=%v want checksum failure", err)
	}
}

func resetColumnAssetVerifiedChecksumCacheForTest(t *testing.T) {
	t.Helper()
	columnAssetVerifiedChecksumCache.Lock()
	columnAssetVerifiedChecksumCache.entries = [columnAssetVerifiedChecksumCacheSlots]columnAssetVerifiedChecksumEntry{}
	columnAssetVerifiedChecksumCache.Unlock()
	t.Cleanup(func() {
		columnAssetVerifiedChecksumCache.Lock()
		columnAssetVerifiedChecksumCache.entries = [columnAssetVerifiedChecksumCacheSlots]columnAssetVerifiedChecksumEntry{}
		columnAssetVerifiedChecksumCache.Unlock()
	})
}

func corruptColumnAssetPayloadByte(t *testing.T, root string, ref ColumnAssetRef) {
	t.Helper()
	assetPath, err := columnAssetSegmentPath(root, ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	file, err := os.OpenFile(assetPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile corrupt target: %v", err)
	}
	if _, err := file.WriteAt([]byte{0xff}, ref.Offset); err != nil {
		_ = file.Close()
		t.Fatalf("WriteAt corrupt target: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close corrupt target: %v", err)
	}
}

func TestColumnAssetReadCacheViewsRequireExplicitOptInM1634(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	payload := []byte("column-asset-reader-view-payload")
	ref, err := writeColumnPhysicalAssetToManager(root, *normalized, payload, 7, 3)
	if err != nil {
		t.Fatalf("writeColumnPhysicalAssetToManager: %v", err)
	}

	copyCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(root, normalized.AssetManager.Namespace, ColumnAssetReadIntegritySkipChecksums)
	if err != nil {
		t.Fatalf("new copy read cache: %v", err)
	}
	copyRaw, err := copyCache.read(ref, nil)
	if err != nil {
		_ = copyCache.close()
		t.Fatalf("copy cache read: %v", err)
	}
	if !bytes.Equal(copyRaw, payload) {
		_ = copyCache.close()
		t.Fatalf("copy cache raw=%q want %q", copyRaw, payload)
	}
	if copyCache.lastView {
		_ = copyCache.close()
		t.Fatalf("copy cache returned an mmap view without opt-in")
	}
	if copyCache.file != nil && len(copyCache.file.mmap) != 0 {
		_ = copyCache.close()
		t.Fatalf("copy cache mapped segment without opt-in")
	}
	if err := copyCache.close(); err != nil {
		t.Fatalf("copy cache close: %v", err)
	}

	viewCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(root, normalized.AssetManager.Namespace, ColumnAssetReadIntegritySkipChecksums)
	if err != nil {
		t.Fatalf("new view read cache: %v", err)
	}
	viewCache.returnViews = true
	viewRaw, err := viewCache.read(ref, nil)
	if err != nil {
		_ = viewCache.close()
		t.Fatalf("view cache read: %v", err)
	}
	if !bytes.Equal(viewRaw, payload) {
		_ = viewCache.close()
		t.Fatalf("view cache raw=%q want %q", viewRaw, payload)
	}
	if viewCache.file != nil && len(viewCache.file.mmap) != 0 && !viewCache.lastView {
		_ = viewCache.close()
		t.Fatalf("view cache mapped segment but did not report view return")
	}
	if err := viewCache.close(); err != nil {
		t.Fatalf("view cache close: %v", err)
	}
}

func TestColumnAssetManagerWriteAllowsZeroChecksumM12A(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	payload := []byte{0xab, 0x9b, 0xe0, 0x9b}
	if checksum := page.Checksum(payload); checksum != 0 {
		t.Fatalf("test payload checksum=%d, want 0", checksum)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	ref, err := writeColumnPhysicalAssetToManager(root, *normalized, payload, 7, 3)
	if err != nil {
		t.Fatalf("writeColumnPhysicalAssetToManager: %v", err)
	}
	if ref.Checksum != 0 || ref.Length != int64(len(payload)) {
		t.Fatalf("unexpected zero-checksum ref: %+v", ref)
	}
	raw, err := readColumnPhysicalAssetFromManager(root, ref)
	if err != nil {
		t.Fatalf("readColumnPhysicalAssetFromManager: %v", err)
	}
	if !bytes.Equal(raw, payload) {
		t.Fatalf("raw payload mismatch: got %x want %x", raw, payload)
	}
}

func TestColumnAssetManagerReadRejectsLengthOverflowM13C(t *testing.T) {
	maxInt := int64(maxCollectionInt)
	if maxInt == math.MaxInt64 {
		t.Skip("int64 builds cannot represent a ColumnAssetRef length larger than max int")
	}
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  "events/column-assets",
		Generation: 1,
		PartID:     1,
		FileID:     1,
		Length:     maxInt + 1,
	}
	if _, err := readColumnPhysicalAssetFromManagerInto(t.TempDir(), ref, nil); err == nil || !strings.Contains(err.Error(), "overflows int") {
		t.Fatalf("readColumnPhysicalAssetFromManagerInto err=%v want length overflow", err)
	}
}

func TestColumnAssetManagerEnsuresNamespaceParentsM12A(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	cfg.AssetManager = &ColumnAssetManagerConfig{Namespace: "events/nested/column-assets"}
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	namespace, err := columnAssetManagerNamespaceForRoot(root, normalized.AssetManager.Namespace)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	dirs, err := columnAssetManagerNamespaceDirs(namespace)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceDirs: %v", err)
	}
	wantDirs := []string{
		root,
		filepath.Join(root, "events"),
		filepath.Join(root, "events", "nested"),
		filepath.Join(root, "events", "nested", "column-assets"),
		filepath.Join(root, "events", "nested", "column-assets", "assets"),
		filepath.Join(root, "events", "nested", "column-assets", "assets", "segments"),
		filepath.Join(root, "events", "nested", "column-assets", "assets", "indexes"),
		filepath.Join(root, "events", "nested", "column-assets", "prepared"),
		filepath.Join(root, "events", "nested", "column-assets", "quarantine"),
		filepath.Join(root, "events", "nested", "column-assets", "tmp"),
	}
	if fmt.Sprint(dirs) != fmt.Sprint(wantDirs) {
		t.Fatalf("namespace dirs=%v want %v", dirs, wantDirs)
	}
	if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
		t.Fatalf("ensureColumnAssetManagerNamespace: %v", err)
	}
	for _, dir := range wantDirs {
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("Stat(%q): %v", dir, err)
		}
		if !info.IsDir() {
			t.Fatalf("%q is not a directory", dir)
		}
	}
}

func TestColumnAssetManagerConcurrentWritesKeepOffsetsStableM12A(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	const writers = 16
	refs := make([]ColumnAssetRef, writers)
	payloads := make([][]byte, writers)
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			rows, err := extractColumnDeclaredRowsFromJSONDocuments(*normalized, []columnWriteDocument{
				{ID: []byte(fmt.Sprintf("e%d", i)), Document: []byte(fmt.Sprintf(`{"time_us":%d,"kind":"like","did":"d%d"}`, i, i))},
			})
			if err != nil {
				errs <- err
				return
			}
			encoded, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
				Collection:        "events",
				Namespace:         normalized.AssetManager.Namespace,
				Generation:        uint64(i + 1),
				PartID:            1,
				AppliedCommandLSN: uint64(i + 1),
				Operation:         ColumnPublishOperationInsert,
				SchemaHash:        normalized.SchemaHash,
				Columns:           normalized.Columns,
				Rows:              rows,
			})
			if err != nil {
				errs <- err
				return
			}
			ref, err := writeColumnPhysicalAssetToManager(root, *normalized, encoded, uint64(i+1), 1)
			if err != nil {
				errs <- err
				return
			}
			payloads[i] = encoded
			refs[i] = ref
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent write: %v", err)
		}
	}
	seenOffsets := make(map[int64]bool, writers)
	for i, ref := range refs {
		if seenOffsets[ref.Offset] {
			t.Fatalf("duplicate offset for ref[%d]=%+v", i, ref)
		}
		seenOffsets[ref.Offset] = true
		raw, err := readColumnPhysicalAssetFromManager(root, ref)
		if err != nil {
			t.Fatalf("read ref[%d]=%+v: %v", i, ref, err)
		}
		if string(raw) != string(payloads[i]) {
			t.Fatalf("payload[%d] read from offset=%d does not match original", i, ref.Offset)
		}
	}
}

func TestColumnAssetManagerAllocatesDistinctNewSegmentsConcurrentlyM15C(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	const writers = 8
	refs := make([]ColumnAssetRef, writers)
	payloads := make([][]byte, writers)
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			appender, err := newNextColumnPhysicalAssetSegmentAppender(root, *normalized)
			if err != nil {
				errs <- err
				return
			}
			payload := []byte(fmt.Sprintf("payload-%02d", i))
			ref, err := appender.append(payload, uint64(i+1), 1)
			if err != nil {
				_ = cleanupColumnAssetRewriteOpenAppender(appender)
				errs <- err
				return
			}
			if err := appender.close(); err != nil {
				errs <- err
				return
			}
			payloads[i] = payload
			refs[i] = ref
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent segment allocation: %v", err)
		}
	}
	seenFileIDs := make(map[uint32]bool, writers)
	for i, ref := range refs {
		if ref.FileID == 0 {
			t.Fatalf("ref[%d]=%+v has zero file_id", i, ref)
		}
		if ref.FileID == columnAssetM12ASegmentFileID {
			t.Fatalf("ref[%d]=%+v used reserved file_id %d", i, ref, columnAssetM12ASegmentFileID)
		}
		if seenFileIDs[ref.FileID] {
			t.Fatalf("duplicate file_id for ref[%d]=%+v refs=%+v", i, ref, refs)
		}
		seenFileIDs[ref.FileID] = true
		raw, err := readColumnPhysicalAssetFromManager(root, ref)
		if err != nil {
			t.Fatalf("read ref[%d]=%+v: %v", i, ref, err)
		}
		if !bytes.Equal(raw, payloads[i]) {
			t.Fatalf("payload[%d]=%q want %q", i, raw, payloads[i])
		}
	}
}

func TestColumnAssetSegmentAppenderAbortRemovesSegmentM15C(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	appender, err := newNextColumnPhysicalAssetSegmentAppender(root, *normalized)
	if err != nil {
		t.Fatalf("newNextColumnPhysicalAssetSegmentAppender: %v", err)
	}
	assetPath := appender.assetPath
	if _, err := os.Stat(assetPath); err != nil {
		t.Fatalf("segment before abort stat: %v", err)
	}
	if err := appender.abort(); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if _, err := os.Stat(assetPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("segment after abort stat=%v want not exist", err)
	}
}

func TestColumnAssetSegmentAllocationCacheRescansOnConflictM15C(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	first, err := newNextColumnPhysicalAssetSegmentAppender(root, *normalized)
	if err != nil {
		t.Fatalf("first newNextColumnPhysicalAssetSegmentAppender: %v", err)
	}
	firstFileID := first.fileID
	if err := first.close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	conflictFileID := firstFileID + 1
	conflictPath, err := columnAssetSegmentPath(root, ColumnAssetRef{
		Namespace: normalized.AssetManager.Namespace,
		FileID:    conflictFileID,
	})
	if err != nil {
		t.Fatalf("conflict segment path: %v", err)
	}
	if err := os.WriteFile(conflictPath, []byte("reserved"), 0o600); err != nil {
		t.Fatalf("write conflict segment: %v", err)
	}
	second, err := newNextColumnPhysicalAssetSegmentAppender(root, *normalized)
	if err != nil {
		t.Fatalf("second newNextColumnPhysicalAssetSegmentAppender: %v", err)
	}
	defer func() { _ = second.abort() }()
	if second.fileID <= conflictFileID {
		t.Fatalf("second file_id=%d want > conflict file_id %d", second.fileID, conflictFileID)
	}
}

func TestColumnAssetSegmentAppenderFailedCloseRemovesSegmentM15C(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	appender, err := newNextColumnPhysicalAssetSegmentAppender(root, *normalized)
	if err != nil {
		t.Fatalf("newNextColumnPhysicalAssetSegmentAppender: %v", err)
	}
	assetPath := appender.assetPath
	appender.failed = true
	if err := appender.close(); err == nil || !strings.Contains(err.Error(), "appender is failed") {
		t.Fatalf("failed close err=%v want appender failed", err)
	}
	if _, err := os.Stat(assetPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("segment after failed close stat=%v want not exist", err)
	}
}

func TestColumnAssetSegmentAppenderRejectsUnsupportedConfigBeforeNamespaceM15C(t *testing.T) {
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	tests := []struct {
		name string
		cfg  ColumnStoreConfig
	}{
		{
			name: "unsupported_kind",
			cfg: ColumnStoreConfig{
				AssetManager: &ColumnAssetManagerConfig{
					Kind:              ColumnAssetManagerKind("unsupported"),
					IsolatedNamespace: true,
					Namespace:         "events/column-assets",
				},
			},
		},
		{
			name: "non_isolated",
			cfg: ColumnStoreConfig{
				AssetManager: &ColumnAssetManagerConfig{
					Kind:              ColumnAssetManagerValueLogShaped,
					IsolatedNamespace: false,
					Namespace:         "events/column-assets",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if appender, err := newNextColumnPhysicalAssetSegmentAppender(root, tt.cfg); err == nil {
				_ = appender.abort()
				t.Fatalf("newNextColumnPhysicalAssetSegmentAppender succeeded; want unsupported config error")
			}
			if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("column asset root stat=%v want not created for invalid config", err)
			}
			if appender, err := newColumnPhysicalAssetSegmentAppender(root, tt.cfg, columnAssetM12ASegmentFileID); err == nil {
				_ = appender.abort()
				t.Fatalf("newColumnPhysicalAssetSegmentAppender succeeded; want unsupported config error")
			}
			if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("column asset root stat=%v want not created for invalid config", err)
			}
		})
	}
}

func TestColumnAssetSegmentAppenderRemoveOnCloseErrorsM15C(t *testing.T) {
	ioErr := errors.New("close-time io")
	tests := []struct {
		name         string
		failed       bool
		fileSyncErr  error
		fileCloseErr error
		dirSyncErr   error
		want         bool
	}{
		{name: "clean", want: false},
		{name: "failed", failed: true, want: true},
		{name: "sync_error", fileSyncErr: ioErr, want: true},
		{name: "close_error", fileCloseErr: ioErr, want: true},
		{name: "dir_sync_error", dirSyncErr: ioErr, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := columnPhysicalAssetSegmentAppenderRemoveOnClose(tt.failed, tt.fileSyncErr, tt.fileCloseErr, tt.dirSyncErr)
			if got != tt.want {
				t.Fatalf("columnPhysicalAssetSegmentAppenderRemoveOnClose=%v want %v", got, tt.want)
			}
		})
	}
}

func TestColumnAssetSegmentAppenderDirSyncErrorRemovesSegmentM15C(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory fsync is intentionally a no-op on windows")
	}
	root := t.TempDir()
	assetPath := filepath.Join(root, "segment-1.tca")
	if err := os.WriteFile(assetPath, []byte("unpublished"), 0o600); err != nil {
		t.Fatalf("write asset: %v", err)
	}
	appender := &columnPhysicalAssetSegmentAppender{
		namespace: columnAssetManagerNamespace{
			SegmentDir: filepath.Join(root, "missing-segment-dir"),
		},
		assetPath: assetPath,
	}
	if err := appender.close(); err == nil {
		t.Fatalf("close err=nil want dir sync error")
	}
	if _, err := os.Stat(assetPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("segment after dir sync failed close stat=%v want not exist", err)
	}
}

type chunkedColumnAssetWriter struct {
	chunks []int
	buf    bytes.Buffer
}

func (w *chunkedColumnAssetWriter) Write(p []byte) (int, error) {
	if len(w.chunks) == 0 {
		return w.buf.Write(p)
	}
	n := w.chunks[0]
	w.chunks = w.chunks[1:]
	if n > len(p) {
		n = len(p)
	}
	if n > 0 {
		_, _ = w.buf.Write(p[:n])
	}
	return n, nil
}

func TestColumnAssetSegmentPayloadWriteRetriesShortWritesM15C(t *testing.T) {
	writer := &chunkedColumnAssetWriter{chunks: []int{2, 1, 99}}
	payload := []byte("column-payload")
	written, err := writeColumnAssetSegmentPayload(writer, payload)
	if err != nil {
		t.Fatalf("writeColumnAssetSegmentPayload: %v", err)
	}
	if written != len(payload) {
		t.Fatalf("written=%d want %d", written, len(payload))
	}
	if !bytes.Equal(writer.buf.Bytes(), payload) {
		t.Fatalf("payload=%q want %q", writer.buf.Bytes(), payload)
	}
}

func TestColumnAssetSegmentPayloadWriteRejectsNoProgressM15C(t *testing.T) {
	writer := &chunkedColumnAssetWriter{chunks: []int{0}}
	written, err := writeColumnAssetSegmentPayload(writer, []byte("payload"))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writeColumnAssetSegmentPayload err=%v want io.ErrShortWrite", err)
	}
	if written != 0 {
		t.Fatalf("written=%d want 0", written)
	}
}

func TestColumnDeclaredExtractionJSONBenchShapeM12A(t *testing.T) {
	cfg := &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Dictionary: true},
			{Name: "did", Path: "did", ValueType: ColumnStoreValueString, Dictionary: true},
			{Name: "repo_id", Path: "commit.repo_id", ValueType: ColumnStoreValueInt64},
			{Name: "author_time_us", Path: "commit.author.time_us", ValueType: ColumnStoreValueInt64},
		},
		SortKey: []ColumnSortKey{{Column: "time_us"}},
	}
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	rows, err := extractColumnDeclaredRowsFromJSONDocuments(*normalized, []columnWriteDocument{{
		ID:       []byte("evt-1"),
		Document: []byte(`{"time_us":11,"kind":"commit","did":"did:plc:1","commit.repo_id":99,"commit":{"repo_id":42,"author":{"time_us":9}},"payload":{"ignored":true}}`),
	}})
	if err != nil {
		t.Fatalf("extractColumnDeclaredRowsFromJSONDocuments: %v", err)
	}
	if len(rows) != 1 || len(rows[0].Values) != len(normalized.Columns) {
		t.Fatalf("unexpected rows: %+v", rows)
	}
	if got := rows[0].Values[0].Int64; got != 11 {
		t.Fatalf("time_us=%d want 11", got)
	}
	if got := rows[0].Values[3].Int64; got != 42 {
		t.Fatalf("commit.repo_id=%d want 42", got)
	}
	if got := rows[0].Values[4].Int64; got != 9 {
		t.Fatalf("commit.author.time_us=%d want 9", got)
	}
}

func TestColumnManifestBinaryRecordsAndGCEnumerableAssetRefsM12A(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  normalized.AssetManager.Namespace,
		Generation: 5,
		PartID:     1,
		FileID:     4,
		Offset:     8192,
		Length:     1024,
		Checksum:   0xdecafbad,
	}
	prepared := ColumnPublishPreparedAssets{
		Assets: []ColumnPreparedAsset{{
			Ref:          ref,
			Bytes:        ref.Length,
			PublishID:    1,
			GenerationID: ref.Generation,
			Reason:       "insert",
		}},
		RowCount:           12,
		CommandBytes:       4096,
		RowRemainderBytes:  2048,
		ColumnPayloadBytes: ref.Length,
	}
	manifest, err := encodeColumnManifestForWrite(ColumnPublishManifestEncodeInput{
		Collection:        "events",
		ColumnStore:       *normalized,
		Operation:         ColumnPublishOperationInsert,
		AppliedCommandLSN: 99,
		Prepared:          prepared,
	})
	if err != nil {
		t.Fatalf("encodeColumnManifestForWrite: %v", err)
	}
	if manifest.ManifestBytes <= columnManifestIdentityRecordSize {
		t.Fatalf("ManifestBytes=%d want binary manifest beyond identity record", manifest.ManifestBytes)
	}
	wantManifestBytes := int64(len(columnManifestIdentityRecordKey) + columnManifestIdentityRecordSize)
	for _, record := range manifest.Records {
		wantManifestBytes += int64(len(record.key) + len(record.value))
	}
	if manifest.ManifestBytes != wantManifestBytes {
		t.Fatalf("ManifestBytes=%d want consistent key+value accounting %d", manifest.ManifestBytes, wantManifestBytes)
	}
	if len(manifest.Records) < 2 {
		t.Fatalf("manifest records=%d want header + part records", len(manifest.Records))
	}
	snapshot, err := decodeColumnManifestRecords(manifest.Records)
	if err != nil {
		t.Fatalf("decodeColumnManifestRecords: %v", err)
	}
	if snapshot.Generation != 5 || snapshot.AppliedCommandLSN != 99 || snapshot.RowCount != 12 || len(snapshot.Parts) != 1 {
		t.Fatalf("unexpected manifest snapshot: %+v", snapshot)
	}
	if snapshot.Parts[0].AssetRef != ref {
		t.Fatalf("manifest asset ref=%+v want %+v", snapshot.Parts[0].AssetRef, ref)
	}

	delta := ColumnManifestRootDelta{
		RootName:       collectionColumnManifestRootName("events"),
		BaseRootID:     44,
		StoragePolicy:  RootStorageFast,
		Identity:       manifest.Identity,
		IdentityRecord: encodeColumnManifestIdentityRecordArray(manifest.Identity),
		Records:        manifest.Records,
	}
	ordered, err := delta.OrderedRootDeltaPublishInput()
	if err != nil {
		t.Fatalf("OrderedRootDeltaPublishInput: %v", err)
	}
	defer func() { _ = ordered.Iter.Close() }()
	assetRefs, err := enumerateColumnManifestAssetRefs(ordered.Iter)
	if err != nil {
		t.Fatalf("enumerateColumnManifestAssetRefs: %v", err)
	}
	if len(assetRefs) != 1 || assetRefs[0] != ref {
		t.Fatalf("enumerated refs=%+v want %+v", assetRefs, ref)
	}
	ordered.Iter.Seek(columnManifestPartRecordKey(ref.Generation, ref.PartID))
	if !ordered.Iter.Valid() {
		t.Fatal("missing asset part record")
	}
	value, ptr, flags := ordered.Iter.UnsafeEntry()
	if flags != 0 {
		t.Fatalf("asset part record flags=%#x want inline", flags)
	}
	if ptr.FileID != 0 || ptr.Offset != 0 || ptr.Length != 0 {
		t.Fatalf("asset part record has pointer=%+v, want typed inline ColumnAssetRef only", ptr)
	}
	if len(value) == 0 {
		t.Fatal("asset part record is empty")
	}

	batched, cleanup, err := delta.OrderedRootDeltaBatchPublishInput()
	if err != nil {
		t.Fatalf("OrderedRootDeltaBatchPublishInput: %v", err)
	}
	defer cleanup()
	var sawPart bool
	for _, entry := range batched.Delta.SortedEntries() {
		if string(entry.Key) == string(columnManifestPartRecordKey(ref.Generation, ref.PartID)) {
			sawPart = !entry.IsPtr && len(entry.Value) != 0
		}
	}
	if !sawPart {
		t.Fatalf("delta batch did not preserve inline manifest asset ref record")
	}

	corruptRecords := cloneColumnManifestRecords(manifest.Records)
	for i := range corruptRecords {
		if string(corruptRecords[i].key) == columnManifestHeaderRecordKey {
			corruptRecords[i].value = append(corruptRecords[i].value, 0)
		}
	}
	if _, err := decodeColumnManifestRecords(corruptRecords); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("decode corrupt manifest records err=%v want trailing-bytes failure", err)
	}

	badIdentity := manifest.Identity
	badIdentity.Checksum++
	badDelta := delta
	badDelta.Identity = badIdentity
	badDelta.IdentityRecord = encodeColumnManifestIdentityRecordArray(badIdentity)
	badDelta.StoragePolicy = normalized.ManifestRoot.StoragePolicy
	if err := validateColumnManifestRootDeltaForPlan(badDelta, delta.BaseRootID, *normalized, badIdentity); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("validate bad manifest checksum err=%v want checksum failure", err)
	}
}

func TestColumnManifestAggregateMetadataRequiresLivePartM1634(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	part := testColumnPublishPreparedAssetM10A()
	part.Ref.Generation = 5
	part.Ref.PartID = 3
	part.GenerationID = part.Ref.Generation
	part.Reason = string(ColumnPublishOperationInsert)
	metadata := part
	metadata.Ref.Kind = ColumnAssetKindTCS1AggregateMetadata
	metadata.Ref.Offset += metadata.Ref.Length
	metadata.Ref.Length = 256
	metadata.Bytes = metadata.Ref.Length
	metadata.Reason = "min_time_us"

	manifest, err := encodeColumnManifestForWrite(ColumnPublishManifestEncodeInput{
		Collection:        "events",
		ColumnStore:       *cfg,
		Operation:         ColumnPublishOperationInsert,
		AppliedCommandLSN: 99,
		Prepared: ColumnPublishPreparedAssets{
			Assets:             []ColumnPreparedAsset{part, metadata},
			RowCount:           10,
			ColumnPayloadBytes: part.Bytes,
		},
	})
	if err != nil {
		t.Fatalf("encodeColumnManifestForWrite: %v", err)
	}
	snapshot, err := decodeColumnManifestRecords(manifest.Records)
	if err != nil {
		t.Fatalf("decodeColumnManifestRecords valid metadata: %v", err)
	}
	if got, want := len(snapshot.AggregateMetadata), 1; got != want {
		t.Fatalf("aggregate metadata refs=%d want %d", got, want)
	}

	withoutPart := make([]columnManifestRecord, 0, len(manifest.Records)-1)
	for _, record := range manifest.Records {
		if bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes) {
			continue
		}
		withoutPart = append(withoutPart, record)
	}
	if _, err := decodeColumnManifestRecords(withoutPart); err == nil || !strings.Contains(err.Error(), "matching live part") {
		t.Fatalf("decode metadata without part err=%v want matching live part failure", err)
	}

	namespaceMismatch := cloneColumnManifestRecords(manifest.Records)
	badMetadata := metadata
	badMetadata.Ref.Namespace = "wrong/column-assets"
	badValue, err := encodeColumnManifestPartRecord(badMetadata)
	if err != nil {
		t.Fatalf("encode bad metadata: %v", err)
	}
	for i := range namespaceMismatch {
		if bytes.HasPrefix(namespaceMismatch[i].key, columnManifestAggregateMetadataRecordPrefixBytes) {
			namespaceMismatch[i].value = badValue
		}
	}
	if _, err := decodeColumnManifestRecords(namespaceMismatch); err == nil || !strings.Contains(err.Error(), "does not match part namespace") {
		t.Fatalf("decode metadata namespace mismatch err=%v want namespace failure", err)
	}
}

func TestColumnAggregateMetadataDecodeRejectsNilAssetManagerM1634(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	cfg.RecoveryAuthoritativeAppliedCommandLSN = 99
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1AggregateMetadata,
		Namespace:  cfg.AssetManager.Namespace,
		Generation: 5,
		PartID:     3,
		FileID:     4,
		Offset:     4096,
		Length:     256,
		Checksum:   0xdecafbad,
	}
	raw, err := encodeColumnAggregateMetadataAsset(columnAggregateMetadataAsset{
		Collection:        "events",
		Namespace:         cfg.AssetManager.Namespace,
		Generation:        ref.Generation,
		PartID:            ref.PartID,
		AppliedCommandLSN: cfg.RecoveryAuthoritativeAppliedCommandLSN,
		SchemaHash:        cfg.SchemaHash,
		AggregateName:     "min_time_us",
		GroupColumn:       "did",
		ValueColumn:       "time_us",
		Rows:              1,
		Entries: []columnAggregateMetadataEntry{
			{Group: "d1", Count: 1, Min: 10, Max: 10},
		},
	})
	if err != nil {
		t.Fatalf("encodeColumnAggregateMetadataAsset: %v", err)
	}
	ref.Length = int64(len(raw))
	ref.Checksum = page.Checksum(raw)
	cfg.AssetManager = nil
	_, err = decodeColumnAggregateMetadataAsset(raw, ref, *cfg, "events", "min_time_us")
	if err == nil || !strings.Contains(err.Error(), "requires column asset manager") {
		t.Fatalf("decodeColumnAggregateMetadataAsset err=%v want asset-manager failure", err)
	}
}

func TestColumnAssetRefRequiresNamespaceGenerationAndSegmentIDM12A(t *testing.T) {
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  "events/column-assets",
		Generation: 1,
		PartID:     1,
		FileID:     1,
		Offset:     128,
		Length:     64,
		Checksum:   1,
	}
	if err := validateColumnAssetRefForPlan(ref); err != nil {
		t.Fatalf("validateColumnAssetRefForPlan valid ref: %v", err)
	}
	for name, mutate := range map[string]func(*ColumnAssetRef){
		"namespace":  func(r *ColumnAssetRef) { r.Namespace = "" },
		"generation": func(r *ColumnAssetRef) { r.Generation = 0 },
		"part_id":    func(r *ColumnAssetRef) { r.PartID = 0 },
		"segment_id": func(r *ColumnAssetRef) { r.FileID = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			bad := ref
			mutate(&bad)
			if err := validateColumnAssetRefForPlan(bad); err == nil {
				t.Fatalf("validateColumnAssetRefForPlan accepted bad %s ref: %+v", name, bad)
			}
		})
	}
}

func TestColumnDeclaredExtractionFailsClosedOnUnsupportedShapeM12A(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	_, err = extractColumnDeclaredRowsFromJSONDocuments(*normalized, []columnWriteDocument{
		{ID: []byte("e1"), Document: []byte(`{"time_us":"not-int","kind":"like","did":"d1"}`)},
	})
	if err == nil {
		t.Fatal("extractColumnDeclaredRowsFromJSONDocuments accepted invalid declared int64")
	}
	if !errors.Is(err, ErrColumnDeclaredValueUnsupported) {
		t.Fatalf("extract error=%v want ErrColumnDeclaredValueUnsupported", err)
	}
	_, err = extractColumnDeclaredRowsFromJSONDocuments(*normalized, []columnWriteDocument{
		{ID: []byte("e1"), Document: []byte(`{"time_us":1,"kind":"like","did":"d1"}{}`)},
	})
	if err == nil {
		t.Fatal("extractColumnDeclaredRowsFromJSONDocuments accepted trailing JSON value")
	}
	if !errors.Is(err, ErrColumnDeclaredValueUnsupported) {
		t.Fatalf("extract trailing JSON error=%v want ErrColumnDeclaredValueUnsupported", err)
	}
}

func BenchmarkColumnDeclaredExtractionJSONM12A(b *testing.B) {
	normalized, docs, docBytes := makeColumnPhysicalAssetBenchmarkDocs(b, 1024)
	b.ReportAllocs()
	b.SetBytes(docBytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := extractColumnDeclaredRowsFromJSONDocuments(*normalized, docs)
		if err != nil {
			b.Fatal(err)
		}
		columnPhysicalAssetBenchRows = rows
	}
}

func BenchmarkColumnPhysicalAssetEncodeM12A(b *testing.B) {
	normalized, rows := makeColumnPhysicalAssetBenchmarkRows(b, 1024)
	preview := makeColumnPhysicalAssetBenchmarkPayload(b, 1024)
	b.ReportAllocs()
	b.SetBytes(int64(len(preview)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoded, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
			Collection:        "events",
			Namespace:         normalized.AssetManager.Namespace,
			Generation:        uint64(i + 1),
			PartID:            1,
			AppliedCommandLSN: uint64(i + 1),
			Operation:         ColumnPublishOperationInsert,
			SchemaHash:        normalized.SchemaHash,
			Columns:           normalized.Columns,
			Rows:              rows,
		})
		if err != nil {
			b.Fatal(err)
		}
		columnPhysicalAssetBenchBytes = encoded
	}
}

func BenchmarkColumnPhysicalAssetDecodeM12A(b *testing.B) {
	encoded := makeColumnPhysicalAssetBenchmarkPayload(b, 1024)
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		asset, err := decodeColumnPhysicalAsset(encoded)
		if err != nil {
			b.Fatal(err)
		}
		columnPhysicalAssetBenchAsset = asset
	}
}

func BenchmarkColumnPhysicalAssetSerialScanM13A(b *testing.B) {
	for _, rows := range []int{1024, 8192} {
		b.Run(fmt.Sprintf("rows_%d", rows), func(b *testing.B) {
			normalized, extracted := makeColumnPhysicalAssetBenchmarkRows(b, rows)
			encoded, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
				Collection:        "events",
				Namespace:         normalized.AssetManager.Namespace,
				Generation:        1,
				PartID:            1,
				AppliedCommandLSN: 1,
				Operation:         ColumnPublishOperationInsert,
				SchemaHash:        normalized.SchemaHash,
				Columns:           normalized.Columns,
				Rows:              extracted,
			})
			if err != nil {
				b.Fatal(err)
			}
			ref := ColumnAssetRef{
				Kind:       ColumnAssetKindTCS1PartImage,
				Namespace:  normalized.AssetManager.Namespace,
				Generation: 1,
				PartID:     1,
				FileID:     columnAssetM12ASegmentFileID,
				Length:     int64(len(encoded)),
				Checksum:   page.Checksum(encoded),
			}
			projection, err := newColumnPhysicalScanProjection(*normalized, []string{"time_us"})
			if err != nil {
				b.Fatal(err)
			}
			var scanned int64
			var sum int64
			b.ReportAllocs()
			b.SetBytes(int64(len(encoded)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				summary, err := scanColumnPhysicalAssetRows(encoded, ref, "events", normalized, projection, func(row columnPhysicalScanRowView) error {
					if len(row.Values) != 1 {
						return fmt.Errorf("values=%d want one projected value", len(row.Values))
					}
					sum += row.Values[0].Int64
					return nil
				})
				if err != nil {
					b.Fatal(err)
				}
				scanned += int64(summary.rows)
			}
			columnPhysicalScanBenchRows = scanned
			columnPhysicalScanBenchSum = sum
		})
	}
}

func BenchmarkColumnPhysicalAssetVectorScanV1(b *testing.B) {
	for _, rows := range []int{1024, 8192} {
		normalized, extracted := makeColumnPhysicalAssetVectorBenchmarkRows(b, rows, 128, 16)
		encoded, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
			Collection:        "vectors",
			Namespace:         normalized.AssetManager.Namespace,
			Generation:        1,
			PartID:            1,
			AppliedCommandLSN: 1,
			Operation:         ColumnPublishOperationInsert,
			SchemaHash:        normalized.SchemaHash,
			Columns:           normalized.Columns,
			Rows:              extracted,
		})
		if err != nil {
			b.Fatal(err)
		}
		ref := ColumnAssetRef{
			Kind:       ColumnAssetKindTCS1PartImage,
			Namespace:  normalized.AssetManager.Namespace,
			Generation: 1,
			PartID:     1,
			FileID:     columnAssetM12ASegmentFileID,
			Length:     int64(len(encoded)),
			Checksum:   page.Checksum(encoded),
		}
		cases := []struct {
			name      string
			projected []string
		}{
			{name: "score_only_skip_vector_adj", projected: []string{"embedding_inv_norm"}},
			{name: "vector_and_adjacency", projected: []string{"embedding", "embedding_neighbors"}},
		}
		for _, tc := range cases {
			b.Run(fmt.Sprintf("rows_%d/%s", rows, tc.name), func(b *testing.B) {
				projection, err := newColumnPhysicalScanProjection(*normalized, tc.projected)
				if err != nil {
					b.Fatal(err)
				}
				var scanned int64
				var sum int64
				b.ReportAllocs()
				b.SetBytes(int64(len(encoded)))
				b.ReportMetric(float64(rows), "rows/op")
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					summary, err := scanColumnPhysicalAssetRows(encoded, ref, "vectors", normalized, projection, func(row columnPhysicalScanRowView) error {
						switch tc.name {
						case "score_only_skip_vector_adj":
							sum += int64(row.Values[0].Float32 * 1000)
						case "vector_and_adjacency":
							sum += int64(len(row.Values[0].Float32Vector) + len(row.Values[1].AdjacencyList))
						}
						return nil
					})
					if err != nil {
						b.Fatal(err)
					}
					scanned += int64(summary.rows)
				}
				columnPhysicalScanBenchRows = scanned
				columnPhysicalScanBenchSum = sum
			})
		}
	}
}

func BenchmarkColumnPhysicalCollectionSerialScanM13A(b *testing.B) {
	dir := b.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		b.Fatal(err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		b.Fatal(err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: testColumnStoreConfig(nil)},
	}); err != nil {
		b.Fatal(err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		b.Fatal(err)
	}
	_, docs, _ := makeColumnPhysicalAssetBenchmarkDocs(b, 1024)
	ids := make([][]byte, len(docs))
	values := make([][]byte, len(docs))
	for i := range docs {
		ids[i] = docs[i].ID
		values[i] = docs[i].Document
	}
	if _, err := col.InsertBatch(ids, values); err != nil {
		b.Fatal(err)
	}
	if err := d.Close(); err != nil {
		b.Fatal(err)
	}
	reopen, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = reopen.Close() })
	reopened, err := NewCollectionManager(reopen).OpenCollection("events")
	if err != nil {
		b.Fatal(err)
	}
	var sum int64
	req := columnPhysicalScanRequest{
		ProjectedColumns: []string{"time_us"},
		Visitor: func(row columnPhysicalScanRowView) error {
			sum += row.Values[0].Int64
			return nil
		},
	}
	preview, err := reopened.scanColumnPhysicalRows(req)
	if err != nil {
		b.Fatal(err)
	}
	// The preview scan provides the byte count; discard its visitor sum before timing.
	sum = 0
	b.ReportAllocs()
	b.SetBytes(preview.PhysicalBytesScanned)
	b.ResetTimer()
	var rows int64
	for i := 0; i < b.N; i++ {
		diag, err := reopened.scanColumnPhysicalRows(req)
		if err != nil {
			b.Fatal(err)
		}
		if diag.PhysicalBytesScanned != preview.PhysicalBytesScanned {
			b.Fatalf("physical bytes scanned=%d want preview %d", diag.PhysicalBytesScanned, preview.PhysicalBytesScanned)
		}
		rows += int64(diag.RowsScanned)
	}
	columnPhysicalScanBenchRows = rows
	columnPhysicalScanBenchSum = sum
}

func BenchmarkColumnAssetManagerWriteM12A(b *testing.B) {
	normalized, rows := makeColumnPhysicalAssetBenchmarkRows(b, 256)
	encoded, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        "events",
		Namespace:         normalized.AssetManager.Namespace,
		Generation:        1,
		PartID:            1,
		AppliedCommandLSN: 1,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        normalized.SchemaHash,
		Columns:           normalized.Columns,
		Rows:              rows,
	})
	if err != nil {
		b.Fatal(err)
	}
	root := backenddb.ColumnAssetRootDirPath(b.TempDir())
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ref, err := writeColumnPhysicalAssetToManager(root, *normalized, encoded, uint64(i+1), 1)
		if err != nil {
			b.Fatal(err)
		}
		columnPhysicalAssetBenchRef = ref
	}
}

func makeColumnPhysicalAssetBenchmarkDocs(tb testing.TB, rows int) (*ColumnStoreConfig, []columnWriteDocument, int64) {
	tb.Helper()
	normalized, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		tb.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	docs := make([]columnWriteDocument, rows)
	var docBytes int64
	for i := range docs {
		id := []byte(fmt.Sprintf("e%09d", i))
		doc := []byte(fmt.Sprintf(`{"time_us":%d,"kind":"kind_%d","did":"d%06d","payload":"ignored_%d"}`, i, i%8, i%1024, i))
		docs[i] = columnWriteDocument{ID: id, Document: doc}
		docBytes += int64(len(id) + len(doc))
	}
	return normalized, docs, docBytes
}

func makeColumnPhysicalAssetBenchmarkRows(tb testing.TB, rows int) (*ColumnStoreConfig, []columnDeclaredRow) {
	tb.Helper()
	normalized, docs, _ := makeColumnPhysicalAssetBenchmarkDocs(tb, rows)
	extracted, err := extractColumnDeclaredRowsFromJSONDocuments(*normalized, docs)
	if err != nil {
		tb.Fatalf("extractColumnDeclaredRowsFromJSONDocuments: %v", err)
	}
	return normalized, extracted
}

func makeColumnPhysicalAssetVectorBenchmarkRows(tb testing.TB, rows, dims, degree int) (*ColumnStoreConfig, []columnDeclaredRow) {
	tb.Helper()
	cfg := &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: dims},
			{Name: "embedding_inv_norm", Path: "embedding_inv_norm", ValueType: ColumnStoreValueFloat32},
			{Name: "embedding_neighbors", Path: "embedding_neighbors", ValueType: ColumnStoreValueAdjacencyList},
		},
	}
	normalized, err := normalizeColumnStoreConfig("vectors", cfg)
	if err != nil {
		tb.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	out := make([]columnDeclaredRow, rows)
	for row := range out {
		vector := make([]float32, dims)
		for dim := range vector {
			vector[dim] = float32((row+1)*(dim+3)%1024) / 1024
		}
		neighbors := make([]uint32, degree)
		for edge := range neighbors {
			neighbors[edge] = uint32((row + edge + 1) % rows)
		}
		out[row] = columnDeclaredRow{
			ID: []byte(fmt.Sprintf("v%09d", row)),
			Values: []columnDeclaredValue{
				{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: vector},
				{Type: ColumnStoreValueFloat32, Present: true, Float32: 1},
				{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: neighbors},
			},
		}
	}
	return normalized, out
}

func makeColumnPhysicalAssetBenchmarkPayload(tb testing.TB, rows int) []byte {
	tb.Helper()
	normalized, extracted := makeColumnPhysicalAssetBenchmarkRows(tb, rows)
	encoded, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        "events",
		Namespace:         normalized.AssetManager.Namespace,
		Generation:        1,
		PartID:            1,
		AppliedCommandLSN: 1,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        normalized.SchemaHash,
		Columns:           normalized.Columns,
		Rows:              extracted,
	})
	if err != nil {
		tb.Fatalf("encodeColumnPhysicalAsset: %v", err)
	}
	return encoded
}

func TestManifestCursorRejectsWrappedFixedWidthSliceLengths(t *testing.T) {
	wrappedElementCount := uint64(maxCollectionInt)/2 + 2
	var b bytes.Buffer
	writeManifestUint64(&b, wrappedElementCount)
	raw := b.Bytes()

	t.Run("float32_vector", func(t *testing.T) {
		cur := manifestCursor{raw: raw}
		if got := cur.float32Slice(); got != nil {
			t.Fatalf("float32Slice returned %d values for wrapped length", len(got))
		}
		if cur.err == nil || !strings.Contains(cur.err.Error(), "short column binary float32_vector") {
			t.Fatalf("float32Slice err=%v want short binary error", cur.err)
		}
	})

	t.Run("uint32_slice", func(t *testing.T) {
		cur := manifestCursor{raw: raw}
		if got := cur.uint32Slice(); got != nil {
			t.Fatalf("uint32Slice returned %d values for wrapped length", len(got))
		}
		if cur.err == nil || !strings.Contains(cur.err.Error(), "short column binary uint32 slice") {
			t.Fatalf("uint32Slice err=%v want short binary error", cur.err)
		}
	})

	t.Run("skip_uint32_slice", func(t *testing.T) {
		cur := manifestCursor{raw: raw}
		if got := cur.skipUint32Slice(); got != 0 {
			t.Fatalf("skipUint32Slice returned length=%d for wrapped length", got)
		}
		if cur.err == nil || !strings.Contains(cur.err.Error(), "short column binary uint32 slice") {
			t.Fatalf("skipUint32Slice err=%v want short binary error", cur.err)
		}
		if cur.pos != len(raw) {
			t.Fatalf("skipUint32Slice pos=%d want %d after reading length only", cur.pos, len(raw))
		}
	})
}
