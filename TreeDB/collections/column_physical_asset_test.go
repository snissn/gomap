package collections

import (
	"bytes"
	"encoding/binary"
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
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
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

func TestColumnPhysicalAssetExtractsTypedColumnBytesFromJSON2010(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{{Name: "opaque", Path: "opaque", ValueType: ColumnStoreValueBytes, Owner: TypedStorageOwnerColumnPart}}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	rows, err := extractColumnDeclaredRowsFromJSONDocuments(*normalized, []columnWriteDocument{
		{ID: []byte("e1"), Document: []byte(`{"opaque":[0,65,255]}`)},
		{ID: []byte("e2"), Document: []byte(`{"opaque":[]}`)},
	})
	if err != nil {
		t.Fatalf("extractColumnDeclaredRowsFromJSONDocuments: %v", err)
	}
	if got := rows[0].Values[0].Bytes; !bytes.Equal(got, []byte{0x00, 'A', 0xff}) {
		t.Fatalf("extracted row0 bytes=%v", got)
	}
	image, rowCount, err := buildTypedColumnPartImageForDeclaredRows(*normalized, 7, typedColumnPartAssetPartID, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnPartImageForDeclaredRows: %v", err)
	}
	if rowCount != 2 || len(image) == 0 {
		t.Fatalf("typed-column bytes image rows=%d len=%d", rowCount, len(image))
	}
	part, err := typedColumnAdapterPartFromBytesForReconstruction(typedColumnAdapterOptions{Fields: columnStoreTypedColumnPartFields(*normalized), SchemaVersion: uint32(normalized.SchemaHash)}, image)
	if err != nil {
		t.Fatalf("typedColumnAdapterPartFromBytesForReconstruction: %v", err)
	}
	bytesColumn, err := part.Part.BytesColumn("opaque", nil, nil)
	if err != nil {
		t.Fatalf("BytesColumn: %v", err)
	}
	got0, err := bytesColumn.Row(0)
	if err != nil {
		t.Fatalf("BytesColumn.Row(0): %v", err)
	}
	got1, err := bytesColumn.Row(1)
	if err != nil {
		t.Fatalf("BytesColumn.Row(1): %v", err)
	}
	if !bytes.Equal(got0, []byte{0x00, 'A', 0xff}) || len(got1) != 0 {
		t.Fatalf("decoded typed-column bytes rows=%v/%v", got0, got1)
	}
	if _, err := extractColumnDeclaredRowsFromJSONDocuments(*normalized, []columnWriteDocument{{ID: []byte("bad"), Document: []byte(`{"opaque":"not bytes"}`)}}); err == nil || !strings.Contains(err.Error(), "expected bytes array") {
		t.Fatalf("extract string bytes err=%v want array rejection", err)
	}
	if _, err := extractColumnDeclaredRowsFromJSONDocuments(*normalized, []columnWriteDocument{{ID: []byte("bad"), Document: []byte(`{"opaque":[256]}`)}}); err == nil || !strings.Contains(err.Error(), "outside byte range") {
		t.Fatalf("extract out-of-range bytes err=%v want range rejection", err)
	}
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
	explicitRowOwner := *normalized
	explicitRowOwner.Columns = append([]ColumnStoreColumn(nil), normalized.Columns...)
	for i := range explicitRowOwner.Columns {
		explicitRowOwner.Columns[i].Owner = TypedStorageOwnerRowAsset
	}
	if err := validateColumnPhysicalAssetForManifest(encoded, ref, explicitRowOwner); err != nil {
		t.Fatalf("validateColumnPhysicalAssetForManifest explicit typed_row_asset owner: %v", err)
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

func TestColumnPhysicalAssetDenseIDRangeEncoding2663(t *testing.T) {
	cfg := testColumnPhysicalRowReaderFixedIDConfigV7(t)
	rows := []columnDeclaredRow{
		{ID: columnPhysicalAssetBigEndianUint64ID(42)},
		{ID: columnPhysicalAssetBigEndianUint64ID(43)},
		{ID: columnPhysicalAssetBigEndianUint64ID(44)},
	}
	encoded, summary, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        "events",
		Namespace:         cfg.AssetManager.Namespace,
		Generation:        7,
		PartID:            3,
		AppliedCommandLSN: 101,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        cfg.SchemaHash,
		Columns:           cfg.Columns,
		Rows:              rows,
	})
	if err != nil {
		t.Fatalf("encodeColumnPhysicalAsset dense ids: %v", err)
	}
	if len(encoded) == 0 || summary.RowCount != len(rows) || summary.ColumnCount != 0 || summary.PayloadBytes != int64(len(encoded)) {
		t.Fatalf("unexpected dense asset summary=%+v len=%d", summary, len(encoded))
	}
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  cfg.AssetManager.Namespace,
		Generation: 7,
		PartID:     3,
		FileID:     1,
		Length:     int64(len(encoded)),
		Checksum:   page.Checksum(encoded),
	}
	header, version, _, err := parseColumnPhysicalAssetScanHeader(encoded, ref, "events", cfg, ColumnPublishOperationInsert)
	if err != nil {
		t.Fatalf("parseColumnPhysicalAssetScanHeader dense ids: %v", err)
	}
	if version != columnPhysicalAssetVersionV8 || header.RowCount != len(rows) || header.ColumnCount != 0 {
		t.Fatalf("header=%+v version=%d want V8 zero-column dense rows", header, version)
	}

	decoded, err := decodeColumnPhysicalAsset(encoded)
	if err != nil {
		t.Fatalf("decodeColumnPhysicalAsset dense ids: %v", err)
	}
	for i, row := range decoded.Rows {
		if got, want := binary.BigEndian.Uint64(row.ID), uint64(42+i); got != want || row.Deleted || len(row.Values) != 0 {
			t.Fatalf("decoded row[%d]=%+v id=%d want dense id %d without values", i, row, got, want)
		}
	}

	projection, err := newColumnPhysicalScanProjection(*cfg, nil)
	if err != nil {
		t.Fatalf("newColumnPhysicalScanProjection: %v", err)
	}
	var scanned []uint64
	scanSummary, err := scanColumnPhysicalAssetRows(encoded, ref, "events", cfg, projection, func(row columnPhysicalScanRowView) error {
		scanned = append(scanned, binary.BigEndian.Uint64(row.ID))
		if row.Deleted || len(row.Values) != 0 {
			return fmt.Errorf("row=%+v want non-deleted dense-id row without values", row)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scanColumnPhysicalAssetRows dense ids: %v", err)
	}
	if scanSummary.rows != len(rows) || scanSummary.deleted != 0 || fmt.Sprint(scanned) != "[42 43 44]" {
		t.Fatalf("scan summary=%+v scanned=%v", scanSummary, scanned)
	}

	accounting, err := columnStoreRowAssetPayloadAccounting(encoded, ref, "events", *cfg, ColumnPublishOperationInsert, false)
	if err != nil {
		t.Fatalf("columnStoreRowAssetPayloadAccounting dense ids: %v", err)
	}
	if accounting.RowIDStoredBytes != 0 || accounting.RowIDValueBytes != int64(len(rows))*8 || accounting.RowEncodingHeaderBytes == 0 {
		t.Fatalf("dense row-id accounting=%+v", accounting)
	}
	categoryBytes := accounting.FormatHeaderBytes +
		accounting.ColumnMetadataBytes +
		accounting.RowEncodingHeaderBytes +
		accounting.RowIDStoredBytes +
		accounting.RowDeletedFlagBytes +
		accounting.RowValueHeaderBytes +
		accounting.RowValuePayloadBytes
	if categoryBytes != accounting.TotalStoredBytes || accounting.TotalStoredBytes != int64(len(encoded)) {
		t.Fatalf("dense category bytes=%d accounting=%+v len=%d", categoryBytes, accounting, len(encoded))
	}

	gappedRows := []columnDeclaredRow{
		{ID: columnPhysicalAssetBigEndianUint64ID(42)},
		{ID: columnPhysicalAssetBigEndianUint64ID(44)},
		{ID: columnPhysicalAssetBigEndianUint64ID(45)},
	}
	gapped, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        "events",
		Namespace:         cfg.AssetManager.Namespace,
		Generation:        7,
		PartID:            4,
		AppliedCommandLSN: 102,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        cfg.SchemaHash,
		Columns:           cfg.Columns,
		Rows:              gappedRows,
	})
	if err != nil {
		t.Fatalf("encodeColumnPhysicalAsset gapped ids: %v", err)
	}
	gappedRef := ref
	gappedRef.PartID = 4
	gappedRef.Length = int64(len(gapped))
	gappedRef.Checksum = page.Checksum(gapped)
	_, gappedVersion, _, err := parseColumnPhysicalAssetScanHeader(gapped, gappedRef, "events", cfg, ColumnPublishOperationInsert)
	if err != nil {
		t.Fatalf("parseColumnPhysicalAssetScanHeader gapped ids: %v", err)
	}
	if gappedVersion != columnPhysicalAssetVersionV7 || len(gapped) <= len(encoded) {
		t.Fatalf("gapped version=%d len=%d dense len=%d want V7 fallback larger than dense V8", gappedVersion, len(gapped), len(encoded))
	}
}

func TestColumnPhysicalAssetZeroColumnProjectionAliasesIDs3067(t *testing.T) {
	allColumns := []ColumnStoreColumn{
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
	}
	rows := []columnDeclaredRow{
		{
			ID: columnPhysicalAssetBigEndianUint64ID(42),
			Values: []columnDeclaredValue{{
				Type:    ColumnStoreValueString,
				Present: true,
				String:  "like",
			}},
		},
		{
			ID: columnPhysicalAssetBigEndianUint64ID(43),
			Values: []columnDeclaredValue{{
				Type:    ColumnStoreValueString,
				Present: true,
				String:  "post",
			}},
		},
	}
	projected, err := projectColumnDeclaredRowsForColumns(allColumns, nil, rows)
	if err != nil {
		t.Fatalf("projectColumnDeclaredRowsForColumns zero columns: %v", err)
	}
	if len(projected) != len(rows) {
		t.Fatalf("projected rows=%d want %d", len(projected), len(rows))
	}
	for i := range projected {
		if len(projected[i].Values) != 0 {
			t.Fatalf("projected row[%d] values=%d want 0", i, len(projected[i].Values))
		}
		if len(projected[i].ID) == 0 || &projected[i].ID[0] != &rows[i].ID[0] {
			t.Fatalf("projected row[%d] did not alias source id", i)
		}
	}
	if _, err := projectColumnDeclaredRowsForColumns(allColumns, nil, []columnDeclaredRow{{ID: []byte("bad")}}); err == nil || !strings.Contains(err.Error(), "values=0 columns=1") {
		t.Fatalf("zero-column projection malformed row err=%v want values/columns error", err)
	}

	cfg := testColumnPhysicalRowReaderFixedIDConfigV7(t)
	encoded, summary, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        "events",
		Namespace:         cfg.AssetManager.Namespace,
		Generation:        7,
		PartID:            3,
		AppliedCommandLSN: 101,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        cfg.SchemaHash,
		Columns:           cfg.Columns,
		Rows:              projected,
	})
	if err != nil {
		t.Fatalf("encodeColumnPhysicalAsset zero-column projected rows: %v", err)
	}
	if summary.RowCount != len(rows) || summary.ColumnCount != 0 || summary.PayloadBytes != int64(len(encoded)) {
		t.Fatalf("summary=%+v len=%d want zero-column row asset", summary, len(encoded))
	}
	decoded, err := decodeColumnPhysicalAsset(encoded)
	if err != nil {
		t.Fatalf("decodeColumnPhysicalAsset zero-column projected rows: %v", err)
	}
	for i, row := range decoded.Rows {
		if row.Deleted || len(row.Values) != 0 {
			t.Fatalf("decoded row[%d]=%+v want no projected values", i, row)
		}
		if !bytes.Equal(row.ID, rows[i].ID) {
			t.Fatalf("decoded row[%d] id=%x want %x", i, row.ID, rows[i].ID)
		}
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

	aliasConfig := *normalized
	aliasConfig.Columns = append([]ColumnStoreColumn(nil), normalized.Columns...)
	aliasConfig.Columns[0].ElementsPerRow = aliasConfig.Columns[0].VectorDims
	aliasConfig.SchemaHash = hashColumnStoreSchema(&aliasConfig)
	if aliasConfig.SchemaHash != normalized.SchemaHash {
		t.Fatalf("alias schema hash=%x want %x", aliasConfig.SchemaHash, normalized.SchemaHash)
	}
	aliasProjection, err := newColumnPhysicalScanProjection(aliasConfig, []string{"embedding"})
	if err != nil {
		t.Fatalf("newColumnPhysicalScanProjection alias: %v", err)
	}
	_, err = scanColumnPhysicalAssetRows(encoded, ref, "vectors", &aliasConfig, aliasProjection, func(row columnPhysicalScanRowView) error {
		if got := row.Values[0].Float32Vector; len(got) != 3 || got[2] != -0.25 {
			t.Fatalf("alias scanned vector=%v", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scanColumnPhysicalAssetRows alias cfg: %v", err)
	}
}

func TestColumnPhysicalAssetDenseNumericVectorRoundTrip1930(t *testing.T) {
	cfg := &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{{
			Name:           "codes",
			Path:           "codes",
			Owner:          TypedStorageOwnerColumnPart,
			ValueType:      ColumnStoreValueUint16Vector,
			ElementsPerRow: 3,
		}},
	}
	normalized, err := normalizeColumnStoreConfig("vectors", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	rows, err := extractColumnDeclaredRowsFromJSONDocuments(*normalized, []columnWriteDocument{{
		ID:       []byte("v1"),
		Document: []byte(`{"codes":[1,513,65535]}`),
	}})
	if err != nil {
		t.Fatalf("extractColumnDeclaredRowsFromJSONDocuments: %v", err)
	}
	wantRaw := []byte{1, 0, 1, 2, 0xff, 0xff}
	if got := rows[0].Values[0].DenseNumericVector; !bytes.Equal(got, wantRaw) {
		t.Fatalf("extracted dense vector=%x want %x", got, wantRaw)
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
	if summary.RowCount != 1 || summary.ColumnCount != 1 || summary.PayloadBytes != int64(len(encoded)) {
		t.Fatalf("unexpected summary=%+v len=%d", summary, len(encoded))
	}
	decoded, err := decodeColumnPhysicalAsset(encoded)
	if err != nil {
		t.Fatalf("decodeColumnPhysicalAsset: %v", err)
	}
	if got := decoded.Columns[0].ElementsPerRow; got != 3 {
		t.Fatalf("decoded elements_per_row=%d want 3", got)
	}
	if got := decoded.Rows[0].Values[0].DenseNumericVector; !bytes.Equal(got, wantRaw) {
		t.Fatalf("decoded dense vector=%x want %x", got, wantRaw)
	}

	ref := ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: normalized.AssetManager.Namespace, Generation: 2, PartID: 1, FileID: columnAssetM12ASegmentFileID, Length: int64(len(encoded)), Checksum: page.Checksum(encoded)}
	projection, err := newColumnPhysicalScanProjection(*normalized, []string{"codes"})
	if err != nil {
		t.Fatalf("newColumnPhysicalScanProjection: %v", err)
	}
	visited := 0
	_, err = scanColumnPhysicalAssetRows(encoded, ref, "vectors", normalized, projection, func(row columnPhysicalScanRowView) error {
		visited++
		if got := row.Values[0].DenseNumericVector; !bytes.Equal(got, wantRaw) {
			t.Fatalf("scanned dense vector=%x want %x", got, wantRaw)
		}
		jsonValue, err := columnDeclaredValueToJSON(row.Values[0])
		if err != nil {
			t.Fatalf("columnDeclaredValueToJSON: %v", err)
		}
		gotJSON, ok := jsonValue.([]uint16)
		if !ok || len(gotJSON) != 3 || gotJSON[1] != 513 || gotJSON[2] != 65535 {
			t.Fatalf("json value=%T %[1]v want []uint16", jsonValue)
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

func TestColumnPhysicalAssetVersionSelectionValidatesAllFixedWidthEncodings(t *testing.T) {
	columns := []ColumnStoreColumn{
		{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 3, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian},
		{Name: "embedding_inv_norm", Path: "embedding_inv_norm", ValueType: ColumnStoreValueFloat32, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian},
	}
	_, err := columnPhysicalAssetVersionForColumns(columns)
	if err == nil || !strings.Contains(err.Error(), "column[1]") || !strings.Contains(err.Error(), "typed_column_part-only") {
		t.Fatalf("columnPhysicalAssetVersionForColumns err=%v want typed-column-only scalar fixed-width validation failure", err)
	}
}

func TestColumnPhysicalAssetFloat32SliceRejectsUnsupportedFixedWidthEncoding(t *testing.T) {
	var b bytes.Buffer
	writeManifestFloat32Slice(&b, []float32{1})
	cur := manifestCursor{raw: b.Bytes()}
	if got := cur.float32SliceWithExpectedLengthAndEncoding(1, ColumnFixedWidthEncoding("future")); got != nil {
		t.Fatalf("float32SliceWithExpectedLengthAndEncoding got=%v want nil", got)
	}
	if cur.err == nil || !strings.Contains(cur.err.Error(), "unsupported fixed_width_encoding") {
		t.Fatalf("cursor err=%v want unsupported fixed_width_encoding", cur.err)
	}
}

func TestColumnPhysicalAssetFloat32SliceWriteRejectsUnsupportedFixedWidthEncoding(t *testing.T) {
	var b bytes.Buffer
	err := writeManifestFloat32SliceWithEncoding(&b, []float32{1}, ColumnFixedWidthEncoding("future"))
	if err == nil || !strings.Contains(err.Error(), "unsupported fixed_width_encoding") {
		t.Fatalf("writeManifestFloat32SliceWithEncoding err=%v want unsupported fixed_width_encoding", err)
	}
	if b.Len() != 0 {
		t.Fatalf("buffer len=%d want 0 after rejected write", b.Len())
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

func TestColumnPhysicalAssetScannerUnprojectedVectorTruncationNamesVectorType(t *testing.T) {
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
	writeManifestUint64(&raw, 3)
	writeManifestUint32(&raw, math.Float32bits(1))

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
	projection, err := newColumnPhysicalScanProjection(*normalized, []string{"score"})
	if err != nil {
		t.Fatalf("newColumnPhysicalScanProjection: %v", err)
	}
	_, err = scanColumnPhysicalAssetRows(encoded, ref, "vectors", normalized, projection, func(row columnPhysicalScanRowView) error {
		t.Fatalf("visitor should not run for truncated vector: %+v", row)
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "short column binary float32_vector") {
		t.Fatalf("scanColumnPhysicalAssetRows err=%v want float32_vector truncation label", err)
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
	assetPath, err := columnAssetSegmentPath(root, ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	verifiedFile, err := os.Open(assetPath)
	if err != nil {
		t.Fatalf("Open verified asset: %v", err)
	}
	identity := columnAssetVerifiedChecksumFileIdentityFromFile(verifiedFile)
	if err := verifiedFile.Close(); err != nil {
		t.Fatalf("Close verified asset: %v", err)
	}
	if !identity.valid {
		t.Skip("cached verify reuse requires stable column asset file identity")
	}
	verifiedInfo, err := os.Stat(assetPath)
	if err != nil {
		t.Fatalf("Stat verified asset: %v", err)
	}
	corruptColumnAssetPayloadByte(t, root, ref)
	if err := os.Chtimes(assetPath, verifiedInfo.ModTime(), verifiedInfo.ModTime()); err != nil {
		t.Fatalf("restore verified asset modtime: %v", err)
	}

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

func TestColumnAssetReadCacheCachedVerifyRefreshesOpenFileIdentityM1634(t *testing.T) {
	resetColumnAssetVerifiedChecksumCacheForTest(t)
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	payload := []byte("cached-verify-open-reader-payload")
	ref, err := writeColumnPhysicalAssetToManager(root, *normalized, payload, 7, 3)
	if err != nil {
		t.Fatalf("writeColumnPhysicalAssetToManager: %v", err)
	}

	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(root, normalized.AssetManager.Namespace, ColumnAssetReadIntegrityCachedVerify)
	if err != nil {
		t.Fatalf("new cached read cache: %v", err)
	}
	defer func() {
		if err := readCache.close(); err != nil {
			t.Fatalf("read cache close: %v", err)
		}
	}()
	raw, err := readCache.read(ref, nil)
	if err != nil {
		t.Fatalf("cached first read: %v", err)
	}
	if !bytes.Equal(raw, payload) {
		t.Fatalf("cached first read raw=%q want %q", raw, payload)
	}

	corruptColumnAssetPayloadByte(t, root, ref)
	assetPath, err := columnAssetSegmentPath(root, ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	changedModTime := time.Now().Add(2 * time.Hour).Round(0)
	if err := os.Chtimes(assetPath, changedModTime, changedModTime); err != nil {
		t.Fatalf("Chtimes corrupt asset: %v", err)
	}

	if _, err := readCache.read(ref, nil); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("cached same-reader read after corruption err=%v want checksum failure", err)
	}
}

func TestColumnAssetReadIntegrityCachedVerifyRejectsRecreatedSegmentM1634(t *testing.T) {
	resetColumnAssetVerifiedChecksumCacheForTest(t)
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	payload := []byte("cached-verify-recreated-payload")
	ref, err := writeColumnPhysicalAssetToManager(root, *normalized, payload, 7, 3)
	if err != nil {
		t.Fatalf("writeColumnPhysicalAssetToManager: %v", err)
	}
	if _, err := readColumnPhysicalAssetFromManagerIntoWithIntegrity(root, ref, nil, ColumnAssetReadIntegrityCachedVerify); err != nil {
		t.Fatalf("cached first read: %v", err)
	}

	assetPath, err := columnAssetSegmentPath(root, ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	corrupt := bytes.Clone(payload)
	corrupt[0] ^= 0xff
	if err := os.Remove(assetPath); err != nil {
		t.Fatalf("Remove recreated asset target: %v", err)
	}
	if err := os.WriteFile(assetPath, corrupt, 0o600); err != nil {
		t.Fatalf("WriteFile recreated asset: %v", err)
	}
	recreatedModTime := time.Now().Add(2 * time.Hour).Round(0)
	if err := os.Chtimes(assetPath, recreatedModTime, recreatedModTime); err != nil {
		t.Fatalf("Chtimes recreated asset: %v", err)
	}

	if _, err := readColumnPhysicalAssetFromManagerIntoWithIntegrity(root, ref, nil, ColumnAssetReadIntegrityCachedVerify); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("cached read after recreated segment err=%v want checksum failure", err)
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
	payload := []byte{0x9d, 0x0a, 0xd9, 0x6d}
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

func TestColumnAssetSegmentAppenderAbortRetainsUnreachableOrphanM15C(t *testing.T) {
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
	file := appender.file
	failedPayload := []byte("abandoned-prefix")
	if _, err := appender.append(failedPayload, 7, 1); err != nil {
		t.Fatalf("append abandoned prefix: %v", err)
	}
	if _, err := os.Stat(assetPath); err != nil {
		t.Fatalf("segment before abort stat: %v", err)
	}
	if err := appender.abort(); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if !fileHandleClosedForTest(file) {
		t.Fatal("abort leaked exact file handle")
	}
	if got, err := os.ReadFile(assetPath); err != nil || !bytes.Equal(got, failedPayload) {
		t.Fatalf("segment after abort payload=%q err=%v want retained orphan %q", got, err, failedPayload)
	}
	if columnAssetSegmentDirSyncKnown(assetPath) {
		t.Fatal("abort marked orphan pathname directory-sync cache known")
	}
	retry, err := newNextColumnPhysicalAssetSegmentAppender(root, *normalized)
	if err != nil {
		t.Fatalf("retry newNextColumnPhysicalAssetSegmentAppender: %v", err)
	}
	if retry.fileID <= appender.fileID {
		_ = retry.abort()
		t.Fatalf("retry file_id=%d want later than orphan file_id=%d", retry.fileID, appender.fileID)
	}
	retryPayload := []byte("published-retry")
	retryRef, err := retry.append(retryPayload, 8, 1)
	if err != nil {
		_ = retry.abort()
		t.Fatalf("retry append: %v", err)
	}
	if err := retry.close(); err != nil {
		t.Fatalf("retry close: %v", err)
	}
	got, err := readColumnPhysicalAssetFromManager(root, retryRef)
	if err != nil || !bytes.Equal(got, retryPayload) {
		t.Fatalf("retry ref payload=%q err=%v want %q", got, err, retryPayload)
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

func TestColumnAssetSegmentAllocationSkipsDirectViewReservedBandM1849(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	namespace, err := columnAssetManagerNamespaceForRoot(root, normalized.AssetManager.Namespace)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
		t.Fatalf("ensureColumnAssetManagerNamespace: %v", err)
	}
	directViewFileID, err := directViewTypedColumnSegmentFileID(7)
	if err != nil {
		t.Fatalf("directViewTypedColumnSegmentFileID: %v", err)
	}
	directViewPath, err := columnAssetSegmentPath(root, ColumnAssetRef{Namespace: normalized.AssetManager.Namespace, FileID: directViewFileID})
	if err != nil {
		t.Fatalf("columnAssetSegmentPath direct-view: %v", err)
	}
	if err := os.WriteFile(directViewPath, []byte("reserved-direct-view"), 0o600); err != nil {
		t.Fatalf("write direct-view segment: %v", err)
	}
	appender, err := newNextColumnPhysicalAssetSegmentAppender(root, *normalized)
	if err != nil {
		t.Fatalf("newNextColumnPhysicalAssetSegmentAppender: %v", err)
	}
	defer func() { _ = appender.abort() }()
	if appender.fileID >= columnAssetDirectViewSegmentFileIDBase || appender.fileID == directViewFileID {
		t.Fatalf("appender file_id=%d collided with direct-view reserved file_id=%d base=%d", appender.fileID, directViewFileID, columnAssetDirectViewSegmentFileIDBase)
	}
}

func TestColumnAssetSegmentAllocationCacheStopsBeforeDirectViewReservedBandM1849(t *testing.T) {
	cleanSegmentDir := filepath.Clean(t.TempDir())
	cache := &columnAssetSegmentAllocationCache{segmentDir: cleanSegmentDir, nextFileID: columnAssetDirectViewSegmentFileIDBase - 1, valid: true}
	namespace := columnAssetManagerNamespace{SegmentDir: cleanSegmentDir}
	fileID, err := nextColumnAssetSegmentFileIDCached(namespace, cleanSegmentDir, cache)
	if err != nil || fileID != columnAssetDirectViewSegmentFileIDBase-1 {
		t.Fatalf("cached file_id=%d err=%v want last regular id", fileID, err)
	}
	advanceColumnAssetSegmentFileIDCache(cleanSegmentDir, cache, fileID)
	if cache.nextFileID != 0 {
		t.Fatalf("advanced cached next_file_id=%d want exhausted before direct-view band", cache.nextFileID)
	}
	if _, err := nextColumnAssetSegmentFileIDCached(namespace, cleanSegmentDir, cache); err == nil {
		t.Fatal("nextColumnAssetSegmentFileIDCached err=nil want exhausted before direct-view band")
	}

	cache.nextFileID = columnAssetDirectViewSegmentFileIDBase
	cache.valid = true
	if _, err := nextColumnAssetSegmentFileIDCached(namespace, cleanSegmentDir, cache); err == nil {
		t.Fatal("nextColumnAssetSegmentFileIDCached accepted reserved direct-view file id")
	}
}

func TestColumnAssetTypedColumnPartDirectViewSegmentTriggerAlignment(t *testing.T) {
	cases := []struct {
		name       string
		field      TypedStorageField
		value      columnDeclaredValue
		wantDirect bool
	}{
		{
			name:       "raw_int64",
			field:      TypedStorageField{Name: "count", Path: "count", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueInt64, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian},
			value:      columnDeclaredValue{Type: ColumnStoreValueInt64, Present: true, Int64: 7},
			wantDirect: true,
		},
		{
			name:       "native_float32",
			field:      TypedStorageField{Name: "score32", Path: "score32", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueFloat32, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian},
			value:      columnDeclaredValue{Type: ColumnStoreValueFloat32, Present: true, Float32: 1.25},
			wantDirect: true,
		},
		{
			name:       "native_float64",
			field:      TypedStorageField{Name: "score64", Path: "score64", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueDouble, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian},
			value:      columnDeclaredValue{Type: ColumnStoreValueDouble, Present: true, Double: -2.5},
			wantDirect: true,
		},
		{
			name:       "float32_vector",
			field:      TypedStorageField{Name: "embedding", Path: "embedding", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueFloat32Vector, VectorDims: 2},
			value:      columnDeclaredValue{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: []float32{1, 2}},
			wantDirect: true,
		},
		{
			name:       "bytes",
			field:      TypedStorageField{Name: "opaque", Path: "opaque", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueBytes},
			value:      columnDeclaredValue{Type: ColumnStoreValueBytes, Present: true, Bytes: []byte{0, 'A', 255}},
			wantDirect: true,
		},
		{
			name:       "adjacency_deferred",
			field:      TypedStorageField{Name: "neighbors", Path: "neighbors", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueAdjacencyList, AdjacencyDegree: 2},
			value:      columnDeclaredValue{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{3, 4}},
			wantDirect: false,
		},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := columnAssetTypedColumnPartDirectViewTestConfig(t, "events_"+tc.name, tc.field)
			root := backenddb.ColumnAssetRootDirPath(t.TempDir())
			payload := columnAssetTypedColumnPartDirectViewTestImage(t, tc.field, []columnDeclaredValue{tc.value})
			ref, err := writeTypedColumnPartAssetToManager(root, cfg, payload, uint64(i+1), 1)
			if err != nil {
				t.Fatalf("writeTypedColumnPartAssetToManager: %v", err)
			}
			directFileID, err := directViewTypedColumnSegmentFileID(uint64(i + 1))
			if err != nil {
				t.Fatalf("directViewTypedColumnSegmentFileID: %v", err)
			}
			if tc.wantDirect {
				if ref.FileID != directFileID || ref.Offset%typedColumnPartDirectViewAssetAlignment != 0 {
					t.Fatalf("ref=%+v want direct-view file_id=%d and %d-byte aligned offset", ref, directFileID, typedColumnPartDirectViewAssetAlignment)
				}
			} else if ref.FileID != columnAssetM12ASegmentFileID {
				t.Fatalf("deferred ref=%+v want regular segment file_id=%d", ref, columnAssetM12ASegmentFileID)
			}
		})
	}
}

func TestColumnAssetTypedColumnPartDirectViewSegmentPaddingAlignment(t *testing.T) {
	cfg, payloads := columnAssetDirectViewAlignmentTestPayloads(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	const generation = uint64(17)
	directFileID, err := directViewTypedColumnSegmentFileID(generation)
	if err != nil {
		t.Fatalf("directViewTypedColumnSegmentFileID: %v", err)
	}
	seedRef, err := writeColumnAssetToManagerSegment(root, cfg, []byte{0xab}, ColumnAssetKindTCS1DictionaryCodes, generation, 90, directFileID)
	if err != nil {
		t.Fatalf("seed writeColumnAssetToManagerSegment: %v", err)
	}
	firstRef, err := writeTypedColumnPartAssetToManager(root, cfg, payloads[0], generation, 1)
	if err != nil {
		t.Fatalf("first writeTypedColumnPartAssetToManager: %v", err)
	}
	secondRef, err := writeTypedColumnPartAssetToManager(root, cfg, payloads[1], generation, 2)
	if err != nil {
		t.Fatalf("second writeTypedColumnPartAssetToManager: %v", err)
	}
	assertColumnAssetDirectViewAlignedTypedPartRef(t, firstRef, payloads[0], "count")
	assertColumnAssetDirectViewAlignedTypedPartRef(t, secondRef, payloads[1], "count")
	if firstRef.Offset != int64(typedColumnPartDirectViewAssetAlignment) {
		t.Fatalf("first offset=%d want seeded prefix padded to %d", firstRef.Offset, typedColumnPartDirectViewAssetAlignment)
	}
	if secondRef.Offset <= firstRef.Offset || secondRef.Offset%typedColumnPartDirectViewAssetAlignment != 0 {
		t.Fatalf("second offset=%d first=%d want later aligned typed-column part in same segment", secondRef.Offset, firstRef.Offset)
	}
	segment := readColumnAssetSegmentFileForTest(t, root, firstRef)
	assertZeroBytesForTest(t, segment[seedRef.Offset+seedRef.Length:firstRef.Offset], "seed-to-first padding")
	assertZeroBytesForTest(t, segment[firstRef.Offset+firstRef.Length:secondRef.Offset], "first-to-second padding")
	if got, want := int64(len(segment)), secondRef.Offset+secondRef.Length; got != want {
		t.Fatalf("segment size=%d want second ref end=%d including padding", got, want)
	}
}

func TestColumnAssetTypedColumnPartDirectViewAppenderPaddingAlignment(t *testing.T) {
	cfg, payloads := columnAssetDirectViewAlignmentTestPayloads(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	const generation = uint64(23)
	directFileID, err := directViewTypedColumnSegmentFileID(generation)
	if err != nil {
		t.Fatalf("directViewTypedColumnSegmentFileID: %v", err)
	}
	appender, err := newColumnPhysicalAssetSegmentAppender(root, cfg, directFileID)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetSegmentAppender: %v", err)
	}
	seedRef, err := appender.appendKind([]byte{0xcd}, ColumnAssetKindTCS1DictionaryCodes, generation, 90)
	if err != nil {
		_ = appender.abort()
		t.Fatalf("seed appendKind: %v", err)
	}
	firstRef, err := appender.appendKind(payloads[0], ColumnAssetKindTCS1TypedColumnPart, generation, 1)
	if err != nil {
		_ = appender.abort()
		t.Fatalf("first appendKind: %v", err)
	}
	secondRef, err := appender.appendKind(payloads[1], ColumnAssetKindTCS1TypedColumnPart, generation, 2)
	if err != nil {
		_ = appender.abort()
		t.Fatalf("second appendKind: %v", err)
	}
	if appender.offset != secondRef.Offset+secondRef.Length {
		_ = appender.abort()
		t.Fatalf("appender offset=%d want second ref end=%d", appender.offset, secondRef.Offset+secondRef.Length)
	}
	if err := appender.close(); err != nil {
		t.Fatalf("appender close: %v", err)
	}
	assertColumnAssetDirectViewAlignedTypedPartRef(t, firstRef, payloads[0], "count")
	assertColumnAssetDirectViewAlignedTypedPartRef(t, secondRef, payloads[1], "count")
	if firstRef.Offset != int64(typedColumnPartDirectViewAssetAlignment) || secondRef.Offset <= firstRef.Offset || secondRef.Offset%typedColumnPartDirectViewAssetAlignment != 0 {
		t.Fatalf("refs first=%+v second=%+v want aligned typed-column parts in same appender segment", firstRef, secondRef)
	}
	segment := readColumnAssetSegmentFileForTest(t, root, firstRef)
	assertZeroBytesForTest(t, segment[seedRef.Offset+seedRef.Length:firstRef.Offset], "appender seed-to-first padding")
	assertZeroBytesForTest(t, segment[firstRef.Offset+firstRef.Length:secondRef.Offset], "appender first-to-second padding")
	if got, want := int64(len(segment)), secondRef.Offset+secondRef.Length; got != want {
		t.Fatalf("segment size=%d want second ref end=%d including appender padding", got, want)
	}
}

func TestColumnPhysicalAssetSegmentAppendWriterBatchesExistingSegment3142(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	seedPayload := []byte("seed")
	seedRef, err := writeColumnAssetToManagerSegment(root, *normalized, seedPayload, ColumnAssetKindTCS1PartImage, 7, 1, columnAssetM12ASegmentFileID)
	if err != nil {
		t.Fatalf("seed writeColumnAssetToManagerSegment: %v", err)
	}
	appender, err := newColumnPhysicalAssetSegmentAppendWriter(root, *normalized, columnAssetM12ASegmentFileID)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetSegmentAppendWriter: %v", err)
	}
	dictPayload := []byte("dictionary-codes")
	dictRef, err := appender.appendKind(dictPayload, ColumnAssetKindTCS1DictionaryCodes, 7, 2)
	if err != nil {
		_ = appender.abort()
		t.Fatalf("append dictionary: %v", err)
	}
	intPayload := []byte("int64-values")
	intRef, err := appender.appendKind(intPayload, ColumnAssetKindTCS1Int64Values, 7, 3)
	if err != nil {
		_ = appender.abort()
		t.Fatalf("append int64: %v", err)
	}
	if err := appender.close(); err != nil {
		t.Fatalf("appender close: %v", err)
	}
	if dictRef.FileID != columnAssetM12ASegmentFileID || intRef.FileID != columnAssetM12ASegmentFileID {
		t.Fatalf("batched refs dict=%+v int=%+v want shared regular segment", dictRef, intRef)
	}
	if dictRef.Offset <= seedRef.Offset || intRef.Offset <= dictRef.Offset {
		t.Fatalf("offsets seed=%+v dict=%+v int=%+v want append order", seedRef, dictRef, intRef)
	}
	if dictRef.Offset%int64(dictionaryCodesDirectViewAssetAlignment) != 0 {
		t.Fatalf("dictionary offset=%d want %d-byte alignment", dictRef.Offset, dictionaryCodesDirectViewAssetAlignment)
	}
	if intRef.Offset%int64(int64ValuesDirectViewAssetAlignment) != 0 {
		t.Fatalf("int64 offset=%d want %d-byte alignment", intRef.Offset, int64ValuesDirectViewAssetAlignment)
	}
	segment := readColumnAssetSegmentFileForTest(t, root, seedRef)
	if got := segment[seedRef.Offset : seedRef.Offset+seedRef.Length]; !bytes.Equal(got, seedPayload) {
		t.Fatalf("seed payload=%q want %q", got, seedPayload)
	}
	if got := segment[dictRef.Offset : dictRef.Offset+dictRef.Length]; !bytes.Equal(got, dictPayload) {
		t.Fatalf("dictionary payload=%q want %q", got, dictPayload)
	}
	if got := segment[intRef.Offset : intRef.Offset+intRef.Length]; !bytes.Equal(got, intPayload) {
		t.Fatalf("int64 payload=%q want %q", got, intPayload)
	}
}

func TestColumnPhysicalAssetAppendSessionBatchesMixedTargets3151(t *testing.T) {
	cfg, payloads := columnAssetDirectViewAlignmentTestPayloads(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	const generation = uint64(31)
	directFileID, err := directViewTypedColumnSegmentFileID(generation)
	if err != nil {
		t.Fatalf("directViewTypedColumnSegmentFileID: %v", err)
	}
	session := newColumnPhysicalAssetAppendSession(root, cfg)
	closed := false
	defer func() {
		if !closed {
			_ = session.abort()
		}
	}()
	sharedRefs, err := session.appendKinds(columnAssetM12ASegmentFileID, []columnPhysicalAssetAppendItem{
		{payload: []byte("row-asset"), kind: ColumnAssetKindTCS1PartImage, generation: generation, partID: columnPhysicalRowAssetPartID},
		{payload: []byte("int64-values"), kind: ColumnAssetKindTCS1Int64Values, generation: generation, partID: 2},
	})
	if err != nil {
		t.Fatalf("shared appendKinds: %v", err)
	}
	directRefs, err := session.appendKinds(directFileID, []columnPhysicalAssetAppendItem{
		{payload: payloads[0], kind: ColumnAssetKindTCS1TypedColumnPart, generation: generation, partID: typedColumnPartAssetPartID},
	})
	if err != nil {
		t.Fatalf("direct appendKinds: %v", err)
	}
	closeStats, err := session.close()
	closed = true
	if err != nil {
		t.Fatalf("session close: %v", err)
	}
	if len(sharedRefs) != 2 || len(directRefs) != 1 {
		t.Fatalf("refs shared=%d direct=%d want 2/1", len(sharedRefs), len(directRefs))
	}
	if sharedRefs[0].FileID != columnAssetM12ASegmentFileID || sharedRefs[1].FileID != columnAssetM12ASegmentFileID {
		t.Fatalf("shared refs=%+v want shared segment file_id=%d", sharedRefs, columnAssetM12ASegmentFileID)
	}
	directRef := directRefs[0]
	if directRef.FileID != directFileID {
		t.Fatalf("direct ref=%+v want direct file_id=%d", directRef, directFileID)
	}
	if directRef.Offset%typedColumnPartDirectViewAssetAlignment != 0 {
		t.Fatalf("direct ref offset=%d want %d-byte alignment", directRef.Offset, typedColumnPartDirectViewAssetAlignment)
	}
	if closeStats.CloseCount != 2 || closeStats.FileSyncCount != 2 || closeStats.SyncEpochCount != 2 {
		t.Fatalf("close stats=%+v want two closed synced segments", closeStats)
	}
	if closeStats.Total.CloseCount != 2 || closeStats.Total.FileSyncCount != 2 || closeStats.Total.SyncEpochCount != 2 {
		t.Fatalf("total close stats=%+v want two closed synced segments", closeStats.Total)
	}
	if closeStats.SharedSegment.CloseCount != 1 || closeStats.SharedSegment.FileSyncCount != 1 || closeStats.SharedSegment.SyncEpochCount != 1 {
		t.Fatalf("shared close stats=%+v want one closed synced shared segment", closeStats.SharedSegment)
	}
	if closeStats.DirectViewSegment.CloseCount != 1 || closeStats.DirectViewSegment.FileSyncCount != 1 || closeStats.DirectViewSegment.SyncEpochCount != 1 {
		t.Fatalf("direct-view close stats=%+v want one closed synced direct-view segment", closeStats.DirectViewSegment)
	}
	sharedSegment := readColumnAssetSegmentFileForTest(t, root, sharedRefs[0])
	if got := sharedSegment[sharedRefs[0].Offset : sharedRefs[0].Offset+sharedRefs[0].Length]; !bytes.Equal(got, []byte("row-asset")) {
		t.Fatalf("shared row payload=%q want row-asset", got)
	}
	if got := sharedSegment[sharedRefs[1].Offset : sharedRefs[1].Offset+sharedRefs[1].Length]; !bytes.Equal(got, []byte("int64-values")) {
		t.Fatalf("shared int64 payload=%q want int64-values", got)
	}
	directSegment := readColumnAssetSegmentFileForTest(t, root, directRef)
	if got := directSegment[directRef.Offset : directRef.Offset+directRef.Length]; !bytes.Equal(got, payloads[0]) {
		t.Fatalf("direct payload=%q want %q", got, payloads[0])
	}
}

func TestColumnPhysicalAssetAppendSessionAbortRetainsNewUnreachableTargets3151(t *testing.T) {
	cfg, _ := columnAssetDirectViewAlignmentTestPayloads(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	const generation = uint64(32)
	sharedRef, err := writeColumnAssetToManagerSegment(root, cfg, []byte("existing-shared"), ColumnAssetKindTCS1PartImage, generation, columnPhysicalRowAssetPartID, columnAssetM12ASegmentFileID)
	if err != nil {
		t.Fatalf("writeColumnAssetToManagerSegment: %v", err)
	}
	sharedPath, err := columnAssetSegmentPath(root, sharedRef)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath shared: %v", err)
	}
	directFileID, err := directViewTypedColumnSegmentFileID(generation)
	if err != nil {
		t.Fatalf("directViewTypedColumnSegmentFileID: %v", err)
	}
	directPath, err := columnAssetSegmentPath(root, ColumnAssetRef{
		Namespace: cfg.AssetManager.Namespace,
		FileID:    directFileID,
	})
	if err != nil {
		t.Fatalf("columnAssetSegmentPath direct: %v", err)
	}
	session := newColumnPhysicalAssetAppendSession(root, cfg)
	if _, err := session.appender(columnAssetM12ASegmentFileID); err != nil {
		t.Fatalf("shared session appender: %v", err)
	}
	if _, err := session.appender(directFileID); err != nil {
		t.Fatalf("direct session appender: %v", err)
	}
	if _, err := os.Stat(directPath); err != nil {
		t.Fatalf("direct path before abort stat=%v want exists", err)
	}
	if err := session.abort(); err != nil {
		t.Fatalf("session abort: %v", err)
	}
	if _, err := os.Stat(sharedPath); err != nil {
		t.Fatalf("shared path after abort stat=%v want existing shared segment preserved", err)
	}
	if _, err := os.Stat(directPath); err != nil {
		t.Fatalf("direct path after abort stat=%v want retained unreachable segment", err)
	}
	if columnAssetSegmentDirSyncKnown(directPath) {
		t.Fatal("session abort marked new direct segment directory-sync cache known")
	}
}

func TestColumnPhysicalAssetSegmentAppendWriterSyncsDirectoryOnlyForCreate3150(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	first, err := newColumnPhysicalAssetSegmentAppendWriter(root, *normalized, columnAssetM12ASegmentFileID)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetSegmentAppendWriter first: %v", err)
	}
	if !first.syncDirOnClose {
		_ = first.abort()
		t.Fatalf("new segment append writer should sync segment directory on close")
	}
	if _, err := first.appendKind([]byte("first"), ColumnAssetKindTCS1PartImage, 7, 1); err != nil {
		_ = first.abort()
		t.Fatalf("first appendKind: %v", err)
	}
	if err := first.close(); err != nil {
		t.Fatalf("first close: %v", err)
	}

	second, err := newColumnPhysicalAssetSegmentAppendWriter(root, *normalized, columnAssetM12ASegmentFileID)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetSegmentAppendWriter second: %v", err)
	}
	if second.syncDirOnClose {
		_ = second.abort()
		t.Fatalf("existing segment append writer should not sync unchanged segment directory on close")
	}
	if _, err := second.appendKind([]byte("second"), ColumnAssetKindTCS1PartImage, 7, 2); err != nil {
		_ = second.abort()
		t.Fatalf("second appendKind: %v", err)
	}
	if err := second.close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestColumnPhysicalAssetSegmentAppendWriterSyncsUnknownExistingSegment3152(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	namespace, err := columnAssetManagerNamespaceForRoot(root, normalized.AssetManager.Namespace)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
		t.Fatalf("ensureColumnAssetManagerNamespace: %v", err)
	}
	seedRef := ColumnAssetRef{
		Kind:      ColumnAssetKindTCS1PartImage,
		Namespace: normalized.AssetManager.Namespace,
		FileID:    columnAssetM12ASegmentFileID,
		Length:    1,
	}
	assetPath, err := columnAssetSegmentPath(root, seedRef)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	clearColumnAssetSegmentDirSyncKnown(assetPath)
	if err := os.WriteFile(assetPath, []byte("left-by-prior-dir-sync-failure"), 0o600); err != nil {
		t.Fatalf("write existing segment: %v", err)
	}
	if columnAssetSegmentDirSyncKnown(assetPath) {
		t.Fatalf("manually-created segment should not start as dir-sync known")
	}
	if _, err := writeColumnAssetToManagerSegment(root, *normalized, []byte("direct"), ColumnAssetKindTCS1PartImage, 7, 1, columnAssetM12ASegmentFileID); err != nil {
		t.Fatalf("writeColumnAssetToManagerSegment: %v", err)
	}
	if !columnAssetSegmentDirSyncKnown(assetPath) {
		t.Fatalf("direct segment write should mark unknown existing segment dir-sync known")
	}

	clearColumnAssetSegmentDirSyncKnown(assetPath)
	appender, err := newColumnPhysicalAssetSegmentAppendWriter(root, *normalized, columnAssetM12ASegmentFileID)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetSegmentAppendWriter: %v", err)
	}
	if !appender.syncDirOnClose {
		_ = appender.abort()
		t.Fatalf("unknown existing segment append writer should sync segment directory on close")
	}
	if _, err := appender.appendKind([]byte("batched"), ColumnAssetKindTCS1PartImage, 7, 2); err != nil {
		_ = appender.abort()
		t.Fatalf("appendKind: %v", err)
	}
	if err := appender.close(); err != nil {
		t.Fatalf("appender close: %v", err)
	}
	if !columnAssetSegmentDirSyncKnown(assetPath) {
		t.Fatalf("successful append close should mark segment dir-sync known")
	}

	next, err := newColumnPhysicalAssetSegmentAppendWriter(root, *normalized, columnAssetM12ASegmentFileID)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetSegmentAppendWriter next: %v", err)
	}
	if next.syncDirOnClose {
		_ = next.abort()
		t.Fatalf("known durable existing segment append writer should skip unchanged segment directory sync")
	}
	if err := next.abort(); err != nil {
		t.Fatalf("next abort: %v", err)
	}
}

func withColumnAssetSegmentFileSyncForTest(t *testing.T, fn func(*os.File) error) {
	t.Helper()
	old := syncColumnAssetSegmentFileForPublish
	syncColumnAssetSegmentFileForPublish = fn
	t.Cleanup(func() {
		syncColumnAssetSegmentFileForPublish = old
	})
}

func TestColumnAssetSegmentDirectWriteRequiresFileSyncBeforeRef3151(t *testing.T) {
	cfg, _ := columnAssetDirectViewAlignmentTestPayloads(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	refForPath := ColumnAssetRef{
		Kind:      ColumnAssetKindTCS1PartImage,
		Namespace: cfg.AssetManager.Namespace,
		FileID:    columnAssetM12ASegmentFileID,
		Length:    1,
	}
	assetPath, err := columnAssetSegmentPath(root, refForPath)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	syncErr := errors.New("forced segment data sync failure")
	calls := 0
	withColumnAssetSegmentFileSyncForTest(t, func(file *os.File) error {
		if file == nil {
			t.Fatalf("syncColumnAssetSegmentFileForPublish called with nil file")
		}
		calls++
		return syncErr
	})
	ref, err := writeColumnAssetToManagerSegment(root, cfg, []byte("direct"), ColumnAssetKindTCS1PartImage, 7, 1, columnAssetM12ASegmentFileID)
	if !errors.Is(err, syncErr) {
		t.Fatalf("writeColumnAssetToManagerSegment err=%v want %v", err, syncErr)
	}
	if ref != (ColumnAssetRef{}) {
		t.Fatalf("writeColumnAssetToManagerSegment ref=%+v want zero ref after sync failure", ref)
	}
	if calls != 1 {
		t.Fatalf("sync calls=%d want 1", calls)
	}
	if columnAssetSegmentDirSyncKnown(assetPath) {
		t.Fatalf("failed direct segment write should not mark segment dir-sync known")
	}
}

func TestColumnAssetSegmentAppenderCloseRequiresFileSyncBeforeRef3151(t *testing.T) {
	cfg, _ := columnAssetDirectViewAlignmentTestPayloads(t)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	syncErr := errors.New("forced appender data sync failure")
	calls := 0
	withColumnAssetSegmentFileSyncForTest(t, func(file *os.File) error {
		if file == nil {
			t.Fatalf("syncColumnAssetSegmentFileForPublish called with nil file")
		}
		calls++
		time.Sleep(time.Millisecond)
		return syncErr
	})
	appender, err := newColumnPhysicalAssetSegmentAppender(root, cfg, columnAssetM12ASegmentFileID)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetSegmentAppender: %v", err)
	}
	ref, err := appender.appendKind([]byte("batched"), ColumnAssetKindTCS1PartImage, 7, 1)
	if err != nil {
		_ = appender.abort()
		t.Fatalf("appendKind: %v", err)
	}
	assetPath, err := columnAssetSegmentPath(root, ref)
	if err != nil {
		_ = appender.abort()
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	if err := appender.close(); !errors.Is(err, syncErr) {
		t.Fatalf("appender close err=%v want %v", err, syncErr)
	}
	closeStats := appender.closeStats
	if closeStats.FileSync <= 0 {
		t.Fatalf("file sync close stats=%+v want positive file sync duration", closeStats)
	}
	if closeStats.CloseCount != 1 {
		t.Fatalf("close count=%d want 1", closeStats.CloseCount)
	}
	if closeStats.FileSyncCount != 1 {
		t.Fatalf("file sync count=%d want 1", closeStats.FileSyncCount)
	}
	if closeStats.SyncEpochCount != 0 {
		t.Fatalf("sync epoch count=%d want 0 after failed sync", closeStats.SyncEpochCount)
	}
	if calls != 1 {
		t.Fatalf("sync calls=%d want 1", calls)
	}
	if _, err := os.Stat(assetPath); err != nil {
		t.Fatalf("asset path after failed close err=%v want retained unreachable orphan", err)
	}
	if closeStats.Remove != 0 || closeStats.RemoveDirSync != 0 || closeStats.CleanupDuration() != 0 {
		t.Fatalf("failed close removal accounting=%+v want zero", closeStats)
	}
	if columnAssetSegmentDirSyncKnown(assetPath) {
		t.Fatalf("failed appender close should not mark segment dir-sync known")
	}
}

func TestColumnPhysicalAssetSegmentAppendWriterBatchesMixedRegularAssets3145(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	seedPayload := []byte("seed")
	seedRef, err := writeColumnAssetToManagerSegment(root, *normalized, seedPayload, ColumnAssetKindTCS1PartImage, 11, 1, columnAssetM12ASegmentFileID)
	if err != nil {
		t.Fatalf("seed writeColumnAssetToManagerSegment: %v", err)
	}
	appender, err := newColumnPhysicalAssetSegmentAppendWriter(root, *normalized, columnAssetM12ASegmentFileID)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetSegmentAppendWriter: %v", err)
	}
	rowPayload := []byte("row-image")
	rowRef, err := appender.appendKind(rowPayload, ColumnAssetKindTCS1PartImage, 11, 2)
	if err != nil {
		_ = appender.abort()
		t.Fatalf("append row image: %v", err)
	}
	typedPayload := []byte("typed-column-part")
	typedRef, err := appender.appendKind(typedPayload, ColumnAssetKindTCS1TypedColumnPart, 11, 3)
	if err != nil {
		_ = appender.abort()
		t.Fatalf("append typed-column part: %v", err)
	}
	dictPayload := []byte("dictionary-codes")
	dictRef, err := appender.appendKind(dictPayload, ColumnAssetKindTCS1DictionaryCodes, 11, 2)
	if err != nil {
		_ = appender.abort()
		t.Fatalf("append dictionary: %v", err)
	}
	intPayload := []byte("int64-values")
	intRef, err := appender.appendKind(intPayload, ColumnAssetKindTCS1Int64Values, 11, 2)
	if err != nil {
		_ = appender.abort()
		t.Fatalf("append int64: %v", err)
	}
	if err := appender.close(); err != nil {
		t.Fatalf("appender close: %v", err)
	}
	refs := []ColumnAssetRef{rowRef, typedRef, dictRef, intRef}
	for _, ref := range refs {
		if ref.FileID != columnAssetM12ASegmentFileID {
			t.Fatalf("ref=%+v want shared regular segment file_id=%d", ref, columnAssetM12ASegmentFileID)
		}
	}
	if rowRef.Offset <= seedRef.Offset || typedRef.Offset <= rowRef.Offset || dictRef.Offset <= typedRef.Offset || intRef.Offset <= dictRef.Offset {
		t.Fatalf("offsets seed=%+v row=%+v typed=%+v dict=%+v int=%+v want append order", seedRef, rowRef, typedRef, dictRef, intRef)
	}
	if dictRef.Offset%int64(dictionaryCodesDirectViewAssetAlignment) != 0 {
		t.Fatalf("dictionary offset=%d want %d-byte alignment", dictRef.Offset, dictionaryCodesDirectViewAssetAlignment)
	}
	if intRef.Offset%int64(int64ValuesDirectViewAssetAlignment) != 0 {
		t.Fatalf("int64 offset=%d want %d-byte alignment", intRef.Offset, int64ValuesDirectViewAssetAlignment)
	}
	segment := readColumnAssetSegmentFileForTest(t, root, seedRef)
	expectedPayloads := []struct {
		ref     ColumnAssetRef
		payload []byte
		name    string
	}{
		{seedRef, seedPayload, "seed"},
		{rowRef, rowPayload, "row"},
		{typedRef, typedPayload, "typed"},
		{dictRef, dictPayload, "dictionary"},
		{intRef, intPayload, "int64"},
	}
	for _, expected := range expectedPayloads {
		got := segment[expected.ref.Offset : expected.ref.Offset+expected.ref.Length]
		if !bytes.Equal(got, expected.payload) {
			t.Fatalf("%s payload=%q want %q", expected.name, got, expected.payload)
		}
	}
}

func TestColumnPhysicalAssetSegmentAppendWriterBatchMatchesSequential3150(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	type appendCase struct {
		payload []byte
		kind    ColumnAssetKind
		partID  uint64
	}
	cases := []appendCase{
		{[]byte("row-image"), ColumnAssetKindTCS1PartImage, 2},
		{[]byte("typed-column-part"), ColumnAssetKindTCS1TypedColumnPart, 3},
		{[]byte("dictionary-codes"), ColumnAssetKindTCS1DictionaryCodes, 4},
		{[]byte("int64-values"), ColumnAssetKindTCS1Int64Values, 5},
		{[]byte("aggregate-metadata"), ColumnAssetKindTCS1AggregateMetadata, 6},
	}
	writeSequential := func(t *testing.T, root string) ([]byte, []ColumnAssetRef) {
		t.Helper()
		seedRef, err := writeColumnAssetToManagerSegment(root, *normalized, []byte("seed"), ColumnAssetKindTCS1PartImage, 13, 1, columnAssetM12ASegmentFileID)
		if err != nil {
			t.Fatalf("seed writeColumnAssetToManagerSegment: %v", err)
		}
		appender, err := newColumnPhysicalAssetSegmentAppendWriter(root, *normalized, columnAssetM12ASegmentFileID)
		if err != nil {
			t.Fatalf("newColumnPhysicalAssetSegmentAppendWriter: %v", err)
		}
		refs := make([]ColumnAssetRef, 0, len(cases))
		for _, tc := range cases {
			ref, err := appender.appendKind(tc.payload, tc.kind, 13, tc.partID)
			if err != nil {
				_ = appender.abort()
				t.Fatalf("appendKind %s: %v", tc.kind, err)
			}
			refs = append(refs, ref)
		}
		if err := appender.close(); err != nil {
			t.Fatalf("appender close: %v", err)
		}
		return readColumnAssetSegmentFileForTest(t, root, seedRef), refs
	}
	writeBatch := func(t *testing.T, root string) ([]byte, []ColumnAssetRef) {
		t.Helper()
		seedRef, err := writeColumnAssetToManagerSegment(root, *normalized, []byte("seed"), ColumnAssetKindTCS1PartImage, 13, 1, columnAssetM12ASegmentFileID)
		if err != nil {
			t.Fatalf("seed writeColumnAssetToManagerSegment: %v", err)
		}
		appender, err := newColumnPhysicalAssetSegmentAppendWriter(root, *normalized, columnAssetM12ASegmentFileID)
		if err != nil {
			t.Fatalf("newColumnPhysicalAssetSegmentAppendWriter: %v", err)
		}
		items := make([]columnPhysicalAssetAppendItem, 0, len(cases))
		for _, tc := range cases {
			items = append(items, columnPhysicalAssetAppendItem{
				payload:    tc.payload,
				kind:       tc.kind,
				generation: 13,
				partID:     tc.partID,
			})
		}
		refs, err := appender.appendKinds(items)
		if err != nil {
			_ = appender.abort()
			t.Fatalf("appendKinds: %v", err)
		}
		if err := appender.close(); err != nil {
			t.Fatalf("appender close: %v", err)
		}
		return readColumnAssetSegmentFileForTest(t, root, seedRef), refs
	}
	sequentialSegment, sequentialRefs := writeSequential(t, backenddb.ColumnAssetRootDirPath(t.TempDir()))
	batchSegment, batchRefs := writeBatch(t, backenddb.ColumnAssetRootDirPath(t.TempDir()))
	if !bytes.Equal(batchSegment, sequentialSegment) {
		t.Fatalf("batch segment bytes differ from sequential append")
	}
	if len(batchRefs) != len(sequentialRefs) {
		t.Fatalf("batch refs=%d want sequential refs=%d", len(batchRefs), len(sequentialRefs))
	}
	for i := range sequentialRefs {
		if batchRefs[i] != sequentialRefs[i] {
			t.Fatalf("ref[%d]=%+v want sequential %+v", i, batchRefs[i], sequentialRefs[i])
		}
		if got, want := batchRefs[i].Checksum, page.Checksum(cases[i].payload); got != want {
			t.Fatalf("ref[%d] checksum=%d want %d", i, got, want)
		}
		if got := batchSegment[batchRefs[i].Offset : batchRefs[i].Offset+batchRefs[i].Length]; !bytes.Equal(got, cases[i].payload) {
			t.Fatalf("ref[%d] payload=%q want %q", i, got, cases[i].payload)
		}
	}
}

func TestColumnPhysicalAssetSegmentAppendWriterAbortKeepsExistingSegment3142(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("events", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	seedPayload := []byte("seed")
	seedRef, err := writeColumnAssetToManagerSegment(root, *normalized, seedPayload, ColumnAssetKindTCS1PartImage, 9, 1, columnAssetM12ASegmentFileID)
	if err != nil {
		t.Fatalf("seed writeColumnAssetToManagerSegment: %v", err)
	}
	assetPath, err := columnAssetSegmentPath(root, seedRef)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	appender, err := newColumnPhysicalAssetSegmentAppendWriter(root, *normalized, columnAssetM12ASegmentFileID)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetSegmentAppendWriter: %v", err)
	}
	if _, err := appender.appendKind([]byte("orphan"), ColumnAssetKindTCS1DictionaryCodes, 9, 2); err != nil {
		_ = appender.abort()
		t.Fatalf("append orphan: %v", err)
	}
	if err := appender.abort(); err != nil {
		t.Fatalf("appender abort: %v", err)
	}
	if _, err := os.Stat(assetPath); err != nil {
		t.Fatalf("stat existing segment after abort: %v", err)
	}
	segment := readColumnAssetSegmentFileForTest(t, root, seedRef)
	if got := segment[seedRef.Offset : seedRef.Offset+seedRef.Length]; !bytes.Equal(got, seedPayload) {
		t.Fatalf("seed payload after abort=%q want %q", got, seedPayload)
	}
}

func TestColumnAssetTypedColumnPartFallbackSegmentsDoNotCertifyInternalPrimaryID(t *testing.T) {
	field := TypedStorageField{Name: "flag", Path: "flag", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueBool}
	cfg := columnAssetTypedColumnPartDirectViewTestConfig(t, "events_bool_fallback", field)
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	payloads := [][]byte{
		columnAssetTypedColumnPartDirectViewTestImage(t, field, []columnDeclaredValue{{Type: ColumnStoreValueBool, Present: true, Bool: true}, {Type: ColumnStoreValueBool, Present: true, Bool: false}, {Type: ColumnStoreValueBool, Present: true, Bool: true}}),
		columnAssetTypedColumnPartDirectViewTestImage(t, field, []columnDeclaredValue{{Type: ColumnStoreValueBool, Present: true, Bool: false}, {Type: ColumnStoreValueBool, Present: true, Bool: false}}),
	}
	firstRef, err := writeTypedColumnPartAssetToManager(root, cfg, payloads[0], 31, 1)
	if err != nil {
		t.Fatalf("first writeTypedColumnPartAssetToManager: %v", err)
	}
	secondRef, err := writeTypedColumnPartAssetToManager(root, cfg, payloads[1], 32, 2)
	if err != nil {
		t.Fatalf("second writeTypedColumnPartAssetToManager: %v", err)
	}
	if firstRef.FileID != columnAssetM12ASegmentFileID || secondRef.FileID != columnAssetM12ASegmentFileID {
		t.Fatalf("fallback refs first=%+v second=%+v want regular shared segment", firstRef, secondRef)
	}
	if secondRef.Offset != firstRef.Offset+firstRef.Length {
		t.Fatalf("fallback second offset=%d want immediately after first end=%d", secondRef.Offset, firstRef.Offset+firstRef.Length)
	}
	assertColumnAssetNoDirectViewCertificationForTest(t, payloads[0], "flag", typedColumnAdapterPrimaryIDColumn)
	assertColumnAssetNoDirectViewCertificationForTest(t, payloads[1], "flag", typedColumnAdapterPrimaryIDColumn)
}

func columnAssetDirectViewAlignmentTestPayloads(t *testing.T) (ColumnStoreConfig, [][]byte) {
	t.Helper()
	field := TypedStorageField{Name: "count", Path: "count", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueInt64, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian}
	cfg := columnAssetTypedColumnPartDirectViewTestConfig(t, "events_alignment", field)
	payloads := [][]byte{
		columnAssetTypedColumnPartDirectViewTestImage(t, field, []columnDeclaredValue{{Type: ColumnStoreValueInt64, Present: true, Int64: 1}, {Type: ColumnStoreValueInt64, Present: true, Int64: 2}}),
		columnAssetTypedColumnPartDirectViewTestImage(t, field, []columnDeclaredValue{{Type: ColumnStoreValueInt64, Present: true, Int64: 3}, {Type: ColumnStoreValueInt64, Present: true, Int64: 4}, {Type: ColumnStoreValueInt64, Present: true, Int64: 5}}),
	}
	return cfg, payloads
}

func columnAssetTypedColumnPartDirectViewTestConfig(t *testing.T, collection string, field TypedStorageField) ColumnStoreConfig {
	t.Helper()
	cfg := &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{{Name: field.Name, Path: field.Path, Owner: field.Owner, ValueType: field.ValueType, Nullable: field.Nullable, FixedWidthEncoding: field.FixedWidthEncoding, VectorDims: field.VectorDims, AdjacencyDegree: field.AdjacencyDegree, AdjacencyLayout: field.AdjacencyLayout}}}
	normalized, err := normalizeColumnStoreConfig(collection, cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	return *normalized
}

func columnAssetTypedColumnPartDirectViewTestImage(t *testing.T, field TypedStorageField, values []columnDeclaredValue) []byte {
	t.Helper()
	part := typedColumnAdapterBuildPart(t, field, values)
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	return image.Bytes
}

func assertColumnAssetDirectViewAlignedTypedPartRef(t *testing.T, ref ColumnAssetRef, payload []byte, column string) {
	t.Helper()
	if ref.Kind != ColumnAssetKindTCS1TypedColumnPart || ref.Offset%typedColumnPartDirectViewAssetAlignment != 0 || ref.Length != int64(len(payload)) {
		t.Fatalf("ref=%+v payload_len=%d want typed-column %d-byte aligned ref", ref, len(payload), typedColumnPartDirectViewAssetAlignment)
	}
	image, err := typedcolumn.ParseColumnPartImage(payload)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		t.Fatalf("CertifyColumnPartLayoutContractFromImage: %v", err)
	}
	cert, ok := certification.Column(column)
	if !ok || !cert.DirectViewCertified {
		t.Fatalf("column %q certification=%+v ok=%v want direct-view certified", column, cert, ok)
	}
	if (ref.Offset+int64(cert.Section.Offset))%int64(cert.Alignment) != 0 {
		t.Fatalf("section absolute offset ref=%d section=%d alignment=%d", ref.Offset, cert.Section.Offset, cert.Alignment)
	}
	for i, block := range cert.Blocks {
		if (ref.Offset+int64(block.PayloadOffset))%int64(cert.Alignment) != 0 {
			t.Fatalf("block[%d] absolute payload offset ref=%d payload=%d alignment=%d", i, ref.Offset, block.PayloadOffset, cert.Alignment)
		}
	}
}

func assertColumnAssetNoDirectViewCertificationForTest(t *testing.T, payload []byte, columns ...string) {
	t.Helper()
	image, err := typedcolumn.ParseColumnPartImage(payload)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		t.Fatalf("CertifyColumnPartLayoutContractFromImage: %v", err)
	}
	if certification.DirectViewCertified != 0 {
		t.Fatalf("certification direct-view columns=%d want none for fallback-only typed-column part", certification.DirectViewCertified)
	}
	for _, column := range columns {
		cert, ok := certification.Column(column)
		if !ok {
			t.Fatalf("missing contract column %q", column)
		}
		if cert.DirectViewCertified {
			t.Fatalf("column %q contract=%+v want no direct-view certification", column, cert)
		}
	}
}

func readColumnAssetSegmentFileForTest(t *testing.T, root string, ref ColumnAssetRef) []byte {
	t.Helper()
	assetPath, err := columnAssetSegmentPath(root, ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	segment, err := os.ReadFile(assetPath)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", assetPath, err)
	}
	return segment
}

func assertZeroBytesForTest(t *testing.T, raw []byte, label string) {
	t.Helper()
	for i, b := range raw {
		if b != 0 {
			t.Fatalf("%s byte[%d]=0x%02x want zero padding", label, i, b)
		}
	}
}

func TestColumnAssetSegmentAppenderFailedCloseRetainsUnreachableSegmentM15C(t *testing.T) {
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
	if _, err := os.Stat(assetPath); err != nil {
		t.Fatalf("segment after failed close stat=%v want retained orphan", err)
	}
	if appender.closeStats.Remove != 0 || appender.closeStats.RemoveDirSync != 0 || appender.closeStats.CleanupDuration() != 0 {
		t.Fatalf("failed close removal accounting=%+v want zero", appender.closeStats)
	}
	if columnAssetSegmentDirSyncKnown(assetPath) {
		t.Fatal("failed close marked orphan pathname directory-sync cache known")
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

func TestColumnAssetSegmentAppenderDirSyncErrorRetainsSegmentM15C(t *testing.T) {
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
		assetPath:      assetPath,
		syncDirOnClose: true,
		created:        true,
	}
	if err := appender.close(); err == nil {
		t.Fatalf("close err=nil want dir sync error")
	}
	if _, err := os.Stat(assetPath); err != nil {
		t.Fatalf("segment after dir sync failed close stat=%v want retained orphan", err)
	}
	if appender.closeStats.Remove != 0 || appender.closeStats.RemoveDirSync != 0 || appender.closeStats.CleanupDuration() != 0 {
		t.Fatalf("dir-sync failed close removal accounting=%+v want zero", appender.closeStats)
	}
	if columnAssetSegmentDirSyncKnown(assetPath) {
		t.Fatal("dir-sync failed close marked pathname directory-sync cache known")
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

func TestColumnAssetChecksumWriterRejectsOverflow4427(t *testing.T) {
	var dst bytes.Buffer
	w := &columnAssetChecksumWriter{dst: &dst, limit: 3}
	if n, err := w.Write([]byte("four")); !errors.Is(err, io.ErrShortWrite) || n != 0 {
		t.Fatalf("overflow write n=%d err=%v want 0/io.ErrShortWrite", n, err)
	}
	if dst.Len() != 0 || w.written != 0 {
		t.Fatalf("overflow wrote dst=%d tracked=%d", dst.Len(), w.written)
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

func TestColumnDeclaredExtractionJSONRootFastPathAndNestedFallbackM2589(t *testing.T) {
	rootCfg := &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Dictionary: true},
			{Name: "maybe", Path: "maybe", ValueType: ColumnStoreValueString, Nullable: true},
			{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 3},
		},
		SortKey: []ColumnSortKey{{Column: "time_us"}},
	}
	rootNormalized, err := normalizeColumnStoreConfig("events", rootCfg)
	if err != nil {
		t.Fatalf("normalize root column store: %v", err)
	}
	rows, err := extractColumnDeclaredRowsFromJSONDocuments(*rootNormalized, []columnWriteDocument{
		{ID: []byte("e1"), Document: []byte(`{"time_us":7,"kind":"li\u006be","maybe":null,"embedding":[1,2.5,-3]}`)},
		{ID: []byte("e2"), Document: []byte(`{"time_us":8,"kind":"post","embedding":[0,0,1]}`)},
	})
	if err != nil {
		t.Fatalf("extract root fast path: %v", err)
	}
	if len(rows) != 2 || len(rows[0].Values) != len(rootNormalized.Columns) {
		t.Fatalf("unexpected root fast-path rows: %+v", rows)
	}
	values := rows[0].Values
	if values[0].Int64 != 7 || values[1].String != "like" {
		t.Fatalf("row0 scalar values=%+v want decoded int64/string", values[:2])
	}
	if !values[2].Present || !values[2].Null {
		t.Fatalf("row0 nullable null=%+v want present null", values[2])
	}
	if got := values[3].Float32Vector; len(got) != 3 || got[0] != 1 || got[1] != 2.5 || got[2] != -3 {
		t.Fatalf("row0 vector=%v want [1 2.5 -3]", got)
	}
	if rows[1].Values[2].Present || !rows[1].Values[2].Null {
		t.Fatalf("row1 nullable missing=%+v want absent null", rows[1].Values[2])
	}
	if _, err := extractColumnDeclaredRowsFromJSONDocuments(*rootNormalized, []columnWriteDocument{{ID: []byte("bad"), Document: []byte(`{"time_us":1,"kind":"bad","embedding":[1,2,3]}{}`)}}); err == nil || !errors.Is(err, ErrColumnDeclaredValueUnsupported) {
		t.Fatalf("extract root trailing JSON err=%v want ErrColumnDeclaredValueUnsupported", err)
	}

	nestedCfg := &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
			{Name: "repo_id", Path: "commit.repo_id", ValueType: ColumnStoreValueInt64},
		},
		SortKey: []ColumnSortKey{{Column: "time_us"}},
	}
	nestedNormalized, err := normalizeColumnStoreConfig("events", nestedCfg)
	if err != nil {
		t.Fatalf("normalize nested column store: %v", err)
	}
	nestedRows, err := extractColumnDeclaredRowsFromJSONDocuments(*nestedNormalized, []columnWriteDocument{{
		ID:       []byte("evt-1"),
		Document: []byte(`{"time_us":11,"commit.repo_id":99,"commit":{"repo_id":42}}`),
	}})
	if err != nil {
		t.Fatalf("extract nested fallback: %v", err)
	}
	if got := nestedRows[0].Values[1].Int64; got != 42 {
		t.Fatalf("nested commit.repo_id=%d want 42 from nested object, not literal dotted root", got)
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

	badKeyRecords := cloneColumnManifestRecords(manifest.Records)
	for i := range badKeyRecords {
		if bytes.Equal(badKeyRecords[i].key, columnManifestPartRecordKey(ref.Generation, ref.PartID)) {
			badKeyRecords[i].key = columnManifestPartRecordKey(ref.Generation+1, ref.PartID)
			break
		}
	}
	if _, err := decodeColumnManifestRecords(badKeyRecords); err == nil || !strings.Contains(err.Error(), "part key") {
		t.Fatalf("decode manifest records with mismatched part key err=%v want part-key failure", err)
	}

	badIdentity := manifest.Identity
	badIdentity.Checksum++
	badDelta := delta
	badDelta.Identity = badIdentity
	badDelta.IdentityRecord = encodeColumnManifestIdentityRecordArray(badIdentity)
	badDelta.StoragePolicy = normalized.ManifestRoot.StoragePolicy
	if err := validateColumnManifestRootDeltaForPlan(badDelta, nil, delta.BaseRootID, *normalized, badIdentity); err == nil || !strings.Contains(err.Error(), "checksum") {
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
			vector    bool
		}{
			{name: "score_only_skip_vector_adj", projected: []string{"embedding_inv_norm"}},
			{name: "vector_and_adjacency", projected: []string{"embedding", "embedding_neighbors"}, vector: true},
		}
		for _, tc := range cases {
			b.Run(fmt.Sprintf("rows_%d/%s", rows, tc.name), func(b *testing.B) {
				projection, err := newColumnPhysicalScanProjection(*normalized, tc.projected)
				if err != nil {
					b.Fatal(err)
				}
				var scanned int64
				var sum int64
				consumeRow := func(row columnPhysicalScanRowView) error {
					sum += int64(row.Values[0].Float32 * 1000)
					return nil
				}
				if tc.vector {
					consumeRow = func(row columnPhysicalScanRowView) error {
						sum += int64(len(row.Values[0].Float32Vector) + len(row.Values[1].AdjacencyList))
						return nil
					}
				}
				b.ReportAllocs()
				b.SetBytes(int64(len(encoded)))
				b.ReportMetric(float64(rows), "rows/op")
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					summary, err := scanColumnPhysicalAssetRows(encoded, ref, "vectors", normalized, projection, consumeRow)
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
