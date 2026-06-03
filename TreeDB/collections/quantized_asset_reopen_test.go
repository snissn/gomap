package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/quantizedasset"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestQuantizedAssetPreparedAccessReopenTypedColumnPart1932(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	col := createQuantizedAssetCollection1932(t, d)
	ids := [][]byte{[]byte("q0"), []byte("q1"), []byte("q2"), []byte("q3")}
	docs := [][]byte{
		[]byte(`{"codes":[0,1,2,3],"norm":1.25,"code_count":4}`),
		[]byte(`{"codes":[10,11,12,13],"norm":2.5,"code_count":5}`),
		[]byte(`{"codes":[20,21,22,23],"norm":3.75,"code_count":6}`),
		[]byte(`{"codes":[30,31,32,33],"norm":4.5,"code_count":7}`),
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	d, err = backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("reopen Open: %v", err)
	}
	defer d.Close()
	mgr := NewCollectionManager(d)
	col, err = mgr.OpenCollection("quantized_assets")
	if err != nil {
		t.Fatalf("reopen OpenCollection: %v", err)
	}
	refs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(refs) != 1 {
		t.Fatalf("typed-column refs=%+v want 1", refs)
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), refs[0])
	if err != nil {
		t.Fatalf("read typed-column asset: %v", err)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	ref := quantizedAssetRefIdentityFromColumnRef1932(refs[0])
	base := quantizedasset.BaseGraphIdentity{IndexName: "vec_idx", Field: "embedding", Metric: "cosine", Dimensions: 16, RowCount: 4, BaseManifestGeneration: refs[0].Generation, BaseManifestChecksum: 0x1932, BaseSchemaHash: 0x1933, GraphSchemaHash: 0x1934}
	schema := quantizedasset.SchemaDescriptor{
		Name:             "persisted-fixed",
		Metric:           "cosine",
		VectorDimensions: 16,
		CodeDimensions:   4,
		CodeWidthBits:    8,
		RowCount:         4,
		OrdinalOrder:     quantizedasset.GraphOrdinalOrderVectorOrdinal,
		Codec:            quantizedasset.CodecDescriptor{Name: "sq8", Version: 1, ConfigHash: 0x1932},
		BaseGraph:        base,
		Columns: []quantizedasset.ColumnDescriptor{
			{Role: quantizedasset.RoleCodes, Column: "codes", Required: true, LogicalType: string(columnsemantics.LogicalByteVector), Type: typedcolumn.ColumnTypeFixedBytes, Encoding: typedcolumn.EncodingRawFixedBytes, BytesPerRow: 4, Ref: ref, AssetBytes: refs[0].Length},
			{Role: quantizedasset.RoleNorm, Column: "norm", Required: true, LogicalType: string(columnsemantics.LogicalFloat32), Type: typedcolumn.ColumnTypeFloat32, Encoding: typedcolumn.EncodingRawFloat32, Ref: ref, AssetBytes: refs[0].Length},
			{Role: quantizedasset.RoleCodeCount, Column: "code_count", Required: true, LogicalType: string(columnsemantics.LogicalUint32), Type: typedcolumn.ColumnTypeUint32, Encoding: typedcolumn.EncodingRawUint32, Ref: ref, AssetBytes: refs[0].Length},
		},
	}
	prepared, err := quantizedasset.Prepare(quantizedasset.PrepareRequest{
		Schema: schema,
		Expected: quantizedasset.ExpectedSchema{
			Metric:           schema.Metric,
			VectorDimensions: schema.VectorDimensions,
			CodeDimensions:   schema.CodeDimensions,
			CodeWidthBits:    schema.CodeWidthBits,
			RowCount:         schema.RowCount,
			OrdinalOrder:     schema.OrdinalOrder,
			Codec:            schema.Codec,
			BaseGraph:        schema.BaseGraph,
			RequiredRoles:    []quantizedasset.Role{quantizedasset.RoleCodes, quantizedasset.RoleNorm, quantizedasset.RoleCodeCount},
		},
		Parts: []quantizedasset.PartImageSource{{Image: image, Ref: ref, AssetBytes: refs[0].Length}},
	})
	if err != nil {
		t.Fatalf("Prepare reopened typed-column asset: %v", err)
	}
	row, ok := prepared.CodeRowBytes(quantizedasset.RoleCodes, 2)
	if !ok || !bytes.Equal(row, []byte{20, 21, 22, 23}) {
		t.Fatalf("reopened codes row=%v ok=%v", row, ok)
	}
	norm, ok := prepared.Float32(quantizedasset.RoleNorm, 2)
	if !ok || norm != 3.75 {
		t.Fatalf("reopened norm=%v ok=%v", norm, ok)
	}
	count, ok := prepared.Uint32(quantizedasset.RoleCodeCount, 2)
	if !ok || count != 6 {
		t.Fatalf("reopened code_count=%v ok=%v", count, ok)
	}
	if fp := prepared.Footprint(); fp.AssetBytes != refs[0].Length || len(fp.Columns) != 3 || fp.BytesPerVector == 0 {
		t.Fatalf("footprint=%+v ref.Length=%d", fp, refs[0].Length)
	}
}

func createQuantizedAssetCollection1932(t testing.TB, d *backenddb.DB) *Collection {
	t.Helper()
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{
		{Name: "codes", Path: "codes", ValueType: ColumnStoreValueByteVector, Owner: TypedStorageOwnerColumnPart, BytesPerRow: 4},
		{Name: "norm", Path: "norm", ValueType: ColumnStoreValueFloat32, Owner: TypedStorageOwnerColumnPart, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian},
		{Name: "code_count", Path: "code_count", ValueType: ColumnStoreValueUint32, Owner: TypedStorageOwnerColumnPart, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian},
	}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "quantized_assets", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("quantized_assets")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	return col
}

func quantizedAssetRefIdentityFromColumnRef1932(ref ColumnAssetRef) quantizedasset.AssetRefIdentity {
	return quantizedasset.AssetRefIdentity{Present: true, Kind: string(ref.Kind), Namespace: ref.Namespace, Generation: ref.Generation, PartID: ref.PartID, FileID: ref.FileID, Offset: ref.Offset, Length: ref.Length, Checksum: ref.Checksum}
}
