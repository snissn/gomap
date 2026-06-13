package collections

import (
	"errors"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestTypedColumnProductionCapabilityMapsBaselineFields(t *testing.T) {
	cases := []struct {
		name      string
		field     TypedStorageField
		wantType  typedcolumn.ColumnType
		wantEnc   typedcolumn.Encoding
		wantStats bool
	}{
		{name: "bool", field: typedColumnAdapterField("flag", ColumnStoreValueBool), wantType: typedcolumn.ColumnTypeBool, wantEnc: typedcolumn.EncodingBoolBitpackRLE, wantStats: true},
		{name: "int64-delta", field: typedColumnAdapterField("count", ColumnStoreValueInt64), wantType: typedcolumn.ColumnTypeInt64, wantEnc: typedcolumn.EncodingDeltaVarint},
		{name: "nullable-int64", field: typedColumnAdapterNullableField("maybe_count", ColumnStoreValueInt64), wantType: typedcolumn.ColumnTypeInt64, wantEnc: typedcolumn.EncodingNullableInt64},
		{name: "string-low-cardinality", field: typedColumnAdapterField("kind", ColumnStoreValueString), wantType: typedcolumn.ColumnTypeLowCardinalityCode, wantEnc: typedcolumn.EncodingLowCardinalityUint32, wantStats: true},
		{name: "nullable-string", field: typedColumnAdapterNullableField("maybe_kind", ColumnStoreValueString), wantType: typedcolumn.ColumnTypeLowCardinalityCode, wantEnc: typedcolumn.EncodingNullableInt64, wantStats: true},
		{name: "nullable-bool", field: typedColumnAdapterNullableField("maybe_flag", ColumnStoreValueBool), wantType: typedcolumn.ColumnTypeBool, wantEnc: typedcolumn.EncodingNullableInt64, wantStats: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			def, err := typedColumnProductionDefinitionForField(tc.field)
			if err != nil {
				t.Fatalf("typedColumnProductionDefinitionForField: %v", err)
			}
			if def.Type != tc.wantType || def.Encoding != tc.wantEnc || def.Compression != typedcolumn.CompressionNone || !def.CompressionSet || def.StatsDisabled != tc.wantStats {
				t.Fatalf("definition=%+v want type=%s encoding=%s compression=none stats_disabled=%v", def, tc.wantType, tc.wantEnc, tc.wantStats)
			}
			if err := validateTypedColumnProductionDefinition(tc.field, def); err != nil {
				t.Fatalf("validateTypedColumnProductionDefinition: %v", err)
			}
		})
	}

	raw := typedColumnAdapterField("raw_count", ColumnStoreValueInt64)
	raw.FixedWidthEncoding = ColumnFixedWidthEncodingLittleEndian
	rawDef, err := typedColumnProductionDefinitionForField(raw)
	if err != nil {
		t.Fatalf("raw int64 definition: %v", err)
	}
	if rawDef.Type != typedcolumn.ColumnTypeInt64 || rawDef.Encoding != typedcolumn.EncodingRawInt64 || rawDef.Compression != typedcolumn.CompressionNone {
		t.Fatalf("raw int64 definition=%+v", rawDef)
	}

	for _, enc := range []typedcolumn.Encoding{typedcolumn.EncodingRawInt64, typedcolumn.EncodingDeltaVarint, typedcolumn.EncodingDoubleDeltaVarint} {
		field := typedColumnAdapterField("count", ColumnStoreValueInt64)
		def, err := typedColumnProductionDefinitionForField(field)
		if err != nil {
			t.Fatalf("int64 definition for %s: %v", enc, err)
		}
		def.Encoding = enc
		if err := validateTypedColumnProductionDefinition(field, def); err != nil {
			t.Fatalf("validate int64 encoding %s: %v", enc, err)
		}
	}
}

func TestTypedColumnProductionCapabilityRejectsUnsupportedCodecCompression(t *testing.T) {
	field := typedColumnAdapterField("count", ColumnStoreValueInt64)
	base, err := typedColumnProductionDefinitionForField(field)
	if err != nil {
		t.Fatalf("base definition: %v", err)
	}

	for _, compression := range []typedcolumn.Compression{typedcolumn.CompressionSnappy, typedcolumn.CompressionLZ4, typedcolumn.CompressionZSTD} {
		t.Run("scalar-supported-"+compression.String(), func(t *testing.T) {
			def := base
			def.Compression = compression
			if err := validateTypedColumnProductionDefinition(field, def); err != nil {
				t.Fatalf("validate %s: %v", compression, err)
			}
		})
	}

	vectorField := TypedStorageField{Name: "embedding", Path: "embedding", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueFloat32Vector, VectorDims: 3}
	vectorDef, err := typedColumnProductionDefinitionForField(vectorField)
	if err != nil {
		t.Fatalf("vector definition: %v", err)
	}
	vectorDef.Compression = typedcolumn.CompressionSnappy
	if err := validateTypedColumnProductionDefinition(vectorField, vectorDef); !errors.Is(err, errTypedColumnProductionLayoutUnsupported) || !strings.Contains(err.Error(), "compression snappy is unsupported") {
		t.Fatalf("vector compression err=%v want unsupported", err)
	}

	compressionCases := []struct {
		name        string
		compression typedcolumn.Compression
		want        string
	}{
		{name: "zstd-dict-unsupported", compression: typedcolumn.CompressionZSTDDict, want: "unsupported compression zstd_dict"},
		{name: "unknown", compression: typedcolumn.Compression(250), want: "unknown compression compression_250"},
	}
	for _, tc := range compressionCases {
		t.Run(tc.name, func(t *testing.T) {
			def := base
			def.Compression = tc.compression
			err := validateTypedColumnProductionDefinition(field, def)
			if !errors.Is(err, errTypedColumnProductionLayoutUnsupported) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want %q and errTypedColumnProductionLayoutUnsupported", err, tc.want)
			}
		})
	}

	t.Run("unknown encoding", func(t *testing.T) {
		def := base
		def.Encoding = typedcolumn.Encoding(250)
		err := validateTypedColumnProductionDefinition(field, def)
		if !errors.Is(err, errTypedColumnProductionLayoutUnsupported) || !strings.Contains(err.Error(), "unsupported encoding encoding_250") {
			t.Fatalf("err=%v want unsupported encoding", err)
		}
	})

	t.Run("bool wrong encoding", func(t *testing.T) {
		boolField := typedColumnAdapterField("flag", ColumnStoreValueBool)
		def, err := typedColumnProductionDefinitionForField(boolField)
		if err != nil {
			t.Fatalf("bool definition: %v", err)
		}
		def.Encoding = typedcolumn.EncodingRawInt64
		err = validateTypedColumnProductionDefinition(boolField, def)
		if !errors.Is(err, errTypedColumnProductionLayoutUnsupported) || !strings.Contains(err.Error(), "encoding=raw_int64 want bool_bitpack_rle") {
			t.Fatalf("err=%v want bool encoding rejection", err)
		}
	})

	t.Run("fixed-width int64 rejects delta", func(t *testing.T) {
		rawField := typedColumnAdapterField("raw_count", ColumnStoreValueInt64)
		rawField.FixedWidthEncoding = ColumnFixedWidthEncodingLittleEndian
		def, err := typedColumnProductionDefinitionForField(rawField)
		if err != nil {
			t.Fatalf("raw definition: %v", err)
		}
		def.Encoding = typedcolumn.EncodingDeltaVarint
		err = validateTypedColumnProductionDefinition(rawField, def)
		if !errors.Is(err, errTypedColumnProductionLayoutUnsupported) || !strings.Contains(err.Error(), "encoding=delta_varint want raw_int64") {
			t.Fatalf("err=%v want fixed-width raw encoding rejection", err)
		}
	})
}

func TestTypedColumnProductionCapabilityRejectsNullableUnsupportedTypes(t *testing.T) {
	cases := []struct {
		name  string
		field TypedStorageField
		want  string
	}{
		{name: "vector", field: TypedStorageField{Name: "embedding", Path: "embedding", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueFloat32Vector, Nullable: true, VectorDims: 3}, want: "nullable float32_vector typed-column fields are not supported"},
		{name: "uint32_list", field: typedColumnAdapterNullableField("tags", ColumnStoreValueUint32List), want: "nullable uint32_list typed-column fields are not supported"},
		{name: "bytes", field: typedColumnAdapterNullableField("blob", ColumnStoreValueBytes), want: "nullable bytes typed-column fields are not supported"},
		{name: "adjacency", field: TypedStorageField{Name: "neighbors", Path: "neighbors", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueAdjacencyList, Nullable: true, AdjacencyDegree: 2}, want: "nullable adjacency_list typed-column fields are not supported"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := typedColumnProductionDefinitionForField(tc.field)
			if !errors.Is(err, errTypedColumnAdapterUnsupportedType) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want %q", err, tc.want)
			}
		})
	}
}

func TestTypedColumnProductionCapabilityValidatesCompressedReadLayouts(t *testing.T) {
	field := typedColumnAdapterField("count", ColumnStoreValueInt64)
	newPart := func(t *testing.T) *typedColumnAdapterPart {
		t.Helper()
		return typedColumnAdapterBuildPart(t, field, []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: 1},
			{Type: ColumnStoreValueInt64, Present: true, Int64: 2},
		})
	}

	t.Run("declared block compression", func(t *testing.T) {
		part := newPart(t)
		col := part.Part.Columns[field.Name]
		col.Blocks[0].Descriptor.Compression = typedcolumn.CompressionSnappy
		part.Part.Columns[field.Name] = col
		err := validateTypedColumnAdapterImageSchema(part.Part, part.Columns, uint32(part.Part.Descriptor.SchemaVersion))
		if !errors.Is(err, errTypedColumnProductionLayoutUnsupported) || !strings.Contains(err.Error(), "compression=snappy not admitted by requested compression=none") {
			t.Fatalf("err=%v want compressed declared block rejection", err)
		}
	})

	t.Run("declared granule compression", func(t *testing.T) {
		part := newPart(t)
		col := part.Part.Columns[field.Name]
		col.Blocks[0].Granule.Compression = typedcolumn.CompressionLZ4
		part.Part.Columns[field.Name] = col
		err := validateTypedColumnAdapterImageSchema(part.Part, part.Columns, uint32(part.Part.Descriptor.SchemaVersion))
		if !errors.Is(err, errTypedColumnProductionLayoutUnsupported) || !strings.Contains(err.Error(), "descriptor/granule compression mismatch") {
			t.Fatalf("err=%v want compressed granule rejection", err)
		}
	})

	t.Run("declared requested compressed blocks", func(t *testing.T) {
		rows := make([]typedColumnAdapterRow, 256)
		for i := range rows {
			rows[i] = typedColumnAdapterRow{PrimaryID: int64(i + 1), Values: map[string]columnDeclaredValue{field.Path: {Type: ColumnStoreValueInt64, Present: true, Int64: 7}}}
		}
		part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 52, RowsPerGranule: 256, Fields: []TypedStorageField{field}, DefaultCompression: typedcolumn.CompressionSnappy, DefaultCompressionSet: true}, rows)
		if err != nil {
			t.Fatalf("build compressed part: %v", err)
		}
		if err := validateTypedColumnAdapterImageSchema(part.Part, part.Columns, uint32(part.Part.Descriptor.SchemaVersion)); err != nil {
			t.Fatalf("validate compressed layout: %v", err)
		}
	})

	t.Run("primary compression", func(t *testing.T) {
		part := newPart(t)
		primary := part.Part.Columns[typedColumnAdapterPrimaryIDColumn]
		primary.Definition.Compression = typedcolumn.CompressionSnappy
		part.Part.Columns[typedColumnAdapterPrimaryIDColumn] = primary
		err := validateTypedColumnAdapterImageSchema(part.Part, part.Columns, uint32(part.Part.Descriptor.SchemaVersion))
		if err != nil {
			t.Fatalf("validate compressed primary layout: %v", err)
		}
	})
}
