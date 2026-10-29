package collections

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
)

func columnGraphVectorProjectionFixture(t *testing.T) (ColumnStoreConfig, typedcolumn.ColumnPartImage, []typedColumnAdapterRow) {
	t.Helper()
	cfg, err := normalizeColumnStoreConfig("docs", &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "embedding", Path: "embedding", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueFloat32Vector, VectorDims: 3},
			{Name: "kind", Path: "kind", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueString, Dictionary: true},
			{Name: "count", Path: "count", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]typedColumnAdapterRow, 512)
	for i := range rows {
		rows[i] = typedColumnAdapterRow{PrimaryID: int64(i + 1), Values: map[string]columnDeclaredValue{
			"embedding": {Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: []float32{float32(i + 1), 2, 3}},
			"kind":      {Type: ColumnStoreValueString, Present: true, String: fmt.Sprintf("%s%03d", strings.Repeat("shared-dictionary-prefix/", 8), i%64)},
			"count":     {Type: ColumnStoreValueInt64, Present: true, Int64: int64(i % 4)},
		}}
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{
		PartID: typedColumnPartAssetPartID, SchemaVersion: uint32(cfg.SchemaHash),
		Fields: columnStoreTypedColumnPartFields(*cfg), RowsPerGranule: 128,
		DefaultCompression: typedcolumn.CompressionLZ4, DefaultCompressionSet: true,
		DefaultCompressionOnlySupported: true,
		SectionCompression:              typedcolumn.CompressionZSTD, SectionCompressionSet: true,
	}, rows)
	if err != nil {
		t.Fatal(err)
	}
	image, err := part.buildImage()
	if err != nil {
		t.Fatal(err)
	}
	return *cfg, image, rows
}

func TestColumnGraphVectorProjectionParityWithCompressedUnusedSections(t *testing.T) {
	cfg, image, rows := columnGraphVectorProjectionFixture(t)
	full, err := typedColumnAdapterPartFromImage(typedColumnAdapterOptions{Fields: columnStoreTypedColumnPartFields(cfg)}, image)
	if err != nil {
		t.Fatal(err)
	}
	projected, err := columnVectorGraphTypedColumnProjectionPart(cfg, image)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(projected.Columns, full.Part.Columns) || !reflect.DeepEqual(projected.Descriptor, full.Part.Descriptor) {
		t.Fatal("vector projection changed stored column definitions, blocks, payloads or descriptor")
	}
	if projected.Locators != nil || len(projected.AggregateMetadata) != 0 || !projected.ColumnStats.Empty() || !projected.PruningMetadata.Empty() {
		t.Fatal("projection materialized unused image metadata")
	}
	var dictionaryZSTD, unusedDataLZ4 bool
	for _, section := range image.Sections {
		if section.Kind == typedcolumn.ColumnPartImageSectionDictionaries && section.Compression == typedcolumn.CompressionZSTD {
			dictionaryZSTD = true
		}
		if section.Kind == typedcolumn.ColumnPartImageSectionColumnData && section.Column != "embedding" && section.Compression == typedcolumn.CompressionLZ4 {
			unusedDataLZ4 = true
		}
	}
	if !dictionaryZSTD || !unusedDataLZ4 {
		t.Fatalf("fixture did not exercise compressed metadata and unused data: dictionaryZSTD=%v unusedDataLZ4=%v", dictionaryZSTD, unusedDataLZ4)
	}
	cert, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		t.Fatal(err)
	}
	certColumn, ok := cert.Column("embedding")
	if !ok {
		t.Fatal("missing vector certification")
	}
	section, err := columnVectorGraphTypedColumnVectorSection(image, "embedding")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateColumnVectorGraphTypedColumnDenseVectorSection(projected, section, certColumn, "embedding", len(rows), 3); err != nil {
		t.Fatal(err)
	}
	row := 0
	for _, block := range projected.Columns["embedding"].Blocks {
		values, err := typedcolumn.DecodeRawFloat32VectorPayload(nil, block.Granule.Payload, block.Descriptor.RowCount, 3)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < block.Descriptor.RowCount; i++ {
			if !reflect.DeepEqual(values[i*3:(i+1)*3], rows[row].Values["embedding"].Float32Vector) {
				t.Fatalf("vector mismatch at row %d", row)
			}
			row++
		}
	}
	if row != len(rows) {
		t.Fatalf("decoded rows=%d want=%d", row, len(rows))
	}
}

func TestColumnGraphVectorProjectionRetainsSchemaAndCodecChecks(t *testing.T) {
	cfg, image, _ := columnGraphVectorProjectionFixture(t)
	t.Run("schema-version", func(t *testing.T) {
		bad := cfg
		bad.SchemaHash++
		if _, err := columnVectorGraphTypedColumnProjectionPart(bad, image); err == nil {
			t.Fatal("wrong schema version accepted")
		}
	})
	t.Run("sibling-declaration", func(t *testing.T) {
		bad := cfg
		bad.Columns = append([]ColumnStoreColumn(nil), cfg.Columns[:2]...)
		if _, err := columnVectorGraphTypedColumnProjectionPart(bad, image); err == nil {
			t.Fatal("undeclared sibling column accepted")
		}
	})
	t.Run("stored-codec", func(t *testing.T) {
		bad := image
		bad.Sections = append([]typedcolumn.ColumnPartImageSection(nil), image.Sections...)
		for i := range bad.Sections {
			if bad.Sections[i].Kind == typedcolumn.ColumnPartImageSectionColumnData && bad.Sections[i].Column == "count" {
				bad.Sections[i].Compression = typedcolumn.CompressionZSTDDict
			}
		}
		if _, err := columnVectorGraphTypedColumnProjectionPart(cfg, bad); err == nil {
			t.Fatal("unsupported sibling stored codec accepted")
		}
	})
	t.Run("layout-payload-checksum", func(t *testing.T) {
		bad := image
		bad.Bytes = bytes.Clone(image.Bytes)
		section, err := columnVectorGraphTypedColumnVectorSection(bad, "embedding")
		if err != nil {
			t.Fatal(err)
		}
		bad.Bytes[section.Offset] ^= 1
		if _, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(bad); err == nil {
			t.Fatal("changed vector payload accepted by layout certification")
		}
	})
}

func TestColumnGraphVectorProjectionUnusedMetadataBoundary(t *testing.T) {
	cfg, image, _ := columnGraphVectorProjectionFixture(t)
	for _, kind := range []typedcolumn.ColumnPartImageSectionKind{typedcolumn.ColumnPartImageSectionColumnStats, typedcolumn.ColumnPartImageSectionRowLocators} {
		t.Run(string(kind), func(t *testing.T) {
			bad := image
			bad.Bytes = bytes.Clone(image.Bytes)
			section, ok := typedColumnImageSectionByKind(bad, kind)
			if !ok || section.Length == 0 {
				t.Fatalf("missing %s metadata for corruption boundary control", kind)
			}
			// Full reconstruction consumes internal image locators and checks
			// them against the primary-key payload. Graph projection instead uses
			// the captured latest collection locator; it never reads this metadata.
			// Real opens still verify the complete physical checksum first.
			bad.Bytes[section.Offset] ^= 0xff
			if _, err := typedcolumn.ColumnPartFromImage(bad); err == nil {
				t.Fatalf("full reconstruction accepted corrupted %s metadata", kind)
			}
			if _, err := columnVectorGraphTypedColumnProjectionPart(cfg, bad); err != nil {
				t.Fatalf("projection decoded unused %s metadata: %v", kind, err)
			}
			if _, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(bad); err != nil {
				t.Fatalf("unused %s metadata changed vector layout certification: %v", kind, err)
			}
		})
	}
}

func TestColumnGraphVectorProjectionPhysicalIntegrityAndLifetime(t *testing.T) {
	cfg, image, rows := columnGraphVectorProjectionFixture(t)
	rootDir := t.TempDir()
	ref, err := writeTypedColumnPartAssetToManager(rootDir, cfg, image.Bytes, 1, image.PartID)
	if err != nil {
		t.Fatal(err)
	}
	typedRef := columnManifestAssetRefForScan{Ref: ref, Rows: image.Rows, Reason: ColumnPublishOperationInsert}
	field, column, ok, err := columnVectorGraphTypedColumnVectorField(cfg, "embedding", 3)
	if err != nil || !ok {
		t.Fatalf("field found=%v err=%v", ok, err)
	}
	manager := mappedresource.NewManager()
	part, _, err := loadColumnVectorGraphTypedColumnVectorPartAtRoot(rootDir, "docs", cfg, typedRef, image.Rows, field, column, manager, nil)
	if err != nil {
		t.Fatal(err)
	}
	source := &columnVectorGraphTypedColumnVectorSource{dims: 3, manager: manager, parts: []*columnVectorGraphTypedColumnVectorPart{part}, locations: []columnVectorGraphTypedColumnVectorLocation{{part: part, rowIndex: 0}}}
	defer source.Close()
	got, _, _, ok := source.vectorForOrdinal(0)
	if !ok || !reflect.DeepEqual(got, rows[0].Values["embedding"].Float32Vector) {
		t.Fatalf("first vector=%v found=%v", got, ok)
	}
	// Projection descriptor/payload borrows ended with setup. Its retained
	// vector mapping must survive independently of the input image allocation.
	clear(image.Bytes)
	got, _, _, ok = source.vectorForOrdinal(0)
	if !ok || !reflect.DeepEqual(got, rows[0].Values["embedding"].Float32Vector) {
		t.Fatal("retained vectors alias temporary image metadata")
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := manager.Stats(); stats.ActiveHandles != 0 || stats.ActiveMappedBytes != 0 || stats.ActiveHeapCopyBytes != 0 {
		t.Fatalf("source close retained resources: %+v", stats)
	}
	if values, outcome, reason, ok := source.vectorForOrdinal(0); ok || values != nil || outcome != columnVectorGraphTypedColumnVectorOutcomeUnknown || reason != typeddecode.Reason("") {
		t.Fatal("closed source exposes vectors")
	}
	assetPath, err := columnAssetSegmentPath(rootDir, ref)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(assetPath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.WriteAt([]byte{0}, int64(ref.Offset))
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatalf("corrupt asset write=%v close=%v", writeErr, closeErr)
	}
	if part, _, err := loadColumnVectorGraphTypedColumnVectorPartAtRoot(rootDir, "docs", cfg, typedRef, typedRef.Rows, field, column, manager, nil); err == nil || part != nil {
		t.Fatalf("physical corruption accepted: part=%v err=%v", part, err)
	}
	if stats := manager.Stats(); stats.ActiveHandles != 0 {
		t.Fatalf("failed open retained handles: %+v", stats)
	}
}
