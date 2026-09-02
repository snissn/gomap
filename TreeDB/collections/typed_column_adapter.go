package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
	"github.com/snissn/gomap/TreeDB/internal/typedkernel"
)

var errTypedColumnAdapterUnsupportedType = errors.New("collections: typed-column adapter unsupported type")

const (
	typedColumnDenseParallelMinRows    = 1 << 16
	typedColumnDenseParallelMinBlocks  = 16
	typedColumnDenseParallelMaxWorkers = 8
)

func columnStoreValueTypeIsPackedUintVector(valueType ColumnStoreValueType) bool {
	_, ok := columnStorePackedUintVectorBits(valueType)
	return ok
}

func columnStorePackedUintVectorBits(valueType ColumnStoreValueType) (int, bool) {
	switch valueType {
	case ColumnStoreValuePackedBitVector:
		return 1, true
	case ColumnStoreValuePackedUint2Vector:
		return 2, true
	case ColumnStoreValuePackedUint4Vector:
		return 4, true
	default:
		return 0, false
	}
}

func typedColumnPackedUintVectorMapping(valueType ColumnStoreValueType) (typedcolumn.ColumnType, typedcolumn.Encoding, int, bool) {
	bitsPerElement, ok := columnStorePackedUintVectorBits(valueType)
	if !ok {
		return "", 0, 0, false
	}
	columnType, _ := typedcolumn.PackedUintVectorTypeForBits(bitsPerElement)
	encoding, _ := typedcolumn.PackedUintVectorEncodingForBits(bitsPerElement)
	return columnType, encoding, bitsPerElement, true
}

func convertJSONByteVector(raw any, col ColumnStoreColumn) ([]byte, error) {
	if col.BytesPerRow <= 0 {
		return nil, fmt.Errorf("invalid byte_vector bytes_per_row %d", col.BytesPerRow)
	}
	values, err := convertJSONBytes(raw)
	if err != nil {
		return nil, err
	}
	if len(values) != col.BytesPerRow {
		return nil, fmt.Errorf("byte_vector length=%d want bytes_per_row=%d", len(values), col.BytesPerRow)
	}
	return values, nil
}

func convertJSONPackedUintVector(raw any, col ColumnStoreColumn) ([]byte, error) {
	bitsPerElement, ok := columnStorePackedUintVectorBits(col.ValueType)
	if !ok {
		return nil, fmt.Errorf("unsupported packed uint vector value_type=%s", col.ValueType)
	}
	if col.BitsPerElement != 0 && col.BitsPerElement != bitsPerElement {
		return nil, fmt.Errorf("%s bits_per_element=%d want %d", col.ValueType, col.BitsPerElement, bitsPerElement)
	}
	if col.ElementsPerRow <= 0 {
		return nil, fmt.Errorf("invalid %s elements_per_row %d", col.ValueType, col.ElementsPerRow)
	}
	values, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("expected %s array got %T", col.ValueType, raw)
	}
	if len(values) != col.ElementsPerRow {
		return nil, fmt.Errorf("%s length=%d want elements_per_row=%d", col.ValueType, len(values), col.ElementsPerRow)
	}
	maxValue := uint64((1 << uint(bitsPerElement)) - 1)
	unpacked := make([]uint8, len(values))
	for i, rawValue := range values {
		n, ok := rawValue.(json.Number)
		if !ok {
			return nil, fmt.Errorf("%s[%d] expected integer got %T", col.ValueType, i, rawValue)
		}
		v, err := convertJSONUnsignedScalar(n, string(col.ValueType), maxValue)
		if err != nil {
			return nil, fmt.Errorf("%s[%d]: %w", col.ValueType, i, err)
		}
		unpacked[i] = uint8(v)
	}
	return typedcolumn.EncodePackedUintRows(nil, 1, col.ElementsPerRow, bitsPerElement, unpacked)
}

func columnDeclaredPackedUintVectorBytesPerRow(valueType ColumnStoreValueType, elementsPerRow int) (int, error) {
	bitsPerElement, ok := columnStorePackedUintVectorBits(valueType)
	if !ok {
		return 0, fmt.Errorf("unsupported packed uint vector value_type=%s", valueType)
	}
	return typedcolumn.PackedUintRowBytes(elementsPerRow, bitsPerElement)
}

func typedColumnDefaultRowsPerGranule() int {
	return typedcolumn.DefaultRowsPerGranule
}

const (
	typedColumnAdapterPrimaryIDColumn                 = "__treedb_primary_id"
	typedColumnAdapterMetadataDictionary              = "__treedb_adapter_metadata"
	typedColumnAdapterMetadataValueTypeMark           = "value_type"
	typedColumnAdapterMetadataDictionaryIdentityMark  = "dictionary_identity"
	typedColumnAdapterMetadataDictionaryOrderMark     = "dictionary_order"
	typedColumnAdapterMetadataDictionaryCollationMark = "dictionary_collation"

	typedColumnAdapterStringDictionaryIdentity        = "part_local_string_dictionary_v1"
	typedColumnAdapterStringDictionaryOrder           = "logical_bytewise_ascending"
	typedColumnAdapterStringDictionaryCollation       = "utf8_bytewise"
	typedColumnAdapterStringDictionaryLegacyOrder     = "none"
	typedColumnAdapterStringDictionaryLegacyCollation = "none"
)

type typedColumnAdapterTypeStatus string

const (
	typedColumnAdapterRepresented typedColumnAdapterTypeStatus = "represented"
	typedColumnAdapterFailClosed  typedColumnAdapterTypeStatus = "fail_closed"
)

type typedColumnAdapterTypeMapping struct {
	ValueType  ColumnStoreValueType
	Status     typedColumnAdapterTypeStatus
	Reason     string
	ColumnType typedcolumn.ColumnType
	Encoding   typedcolumn.Encoding
}

type typedColumnAdapterColumn struct {
	Field                  TypedStorageField
	Definition             typedcolumn.ColumnDefinition
	Dictionary             map[string]int64
	ReverseDictionary      map[int64]string
	DictionaryValuesByCode []string
	FixedWidthEncoding     ColumnFixedWidthEncoding
}

type typedColumnAdapterDictionaryMode struct {
	Forward      bool
	Reverse      bool
	ValuesByCode bool
}

type typedColumnAdapterOptions struct {
	Collection                      string
	Namespace                       string
	SchemaVersion                   uint32
	PartID                          uint64
	RowsPerGranule                  int
	DefaultCompression              typedcolumn.Compression
	DefaultCompressionSet           bool
	DefaultCompressionOnlySupported bool
	SectionCompression              typedcolumn.Compression
	SectionCompressionSet           bool
	LocatorSectionCompression       typedcolumn.Compression
	LocatorSectionCompressionSet    bool
	DictionarySectionCompression    typedcolumn.Compression
	DictionarySectionCompressionSet bool
	DictionaryModes                 map[string]typedColumnAdapterDictionaryMode
	PruningSectionCompression       typedcolumn.Compression
	PruningSectionCompressionSet    bool
	Int64Encoding                   typedcolumn.Encoding
	Int64EncodingSet                bool
	AdaptiveMarkSizing              typedcolumn.ColumnAdaptiveMarkSizing
	Fields                          []TypedStorageField
	SortKey                         []ColumnSortKey
}

type typedColumnAdapterRow struct {
	PrimaryID int64
	Values    map[string]columnDeclaredValue
}

type typedColumnAdapterRowSource interface {
	Len() int
	PrimaryID(rowIdx int) int64
	Value(rowIdx int, column typedColumnAdapterColumn) (columnDeclaredValue, bool, error)
}

type typedColumnAdapterIndexedRowSource interface {
	typedColumnAdapterRowSource
	ValueIndex(column typedColumnAdapterColumn) (int, error)
	ValueAt(rowIdx, valueIdx int) (columnDeclaredValue, bool, error)
}

type typedColumnAdapterRowsSource []typedColumnAdapterRow

func (s typedColumnAdapterRowsSource) Len() int {
	return len(s)
}

func (s typedColumnAdapterRowsSource) PrimaryID(rowIdx int) int64 {
	return s[rowIdx].PrimaryID
}

func (s typedColumnAdapterRowsSource) Value(rowIdx int, column typedColumnAdapterColumn) (columnDeclaredValue, bool, error) {
	return typedColumnAdapterRowValue(s[rowIdx], column)
}

type typedColumnDeclaredRowSource struct {
	allColumns  []ColumnStoreColumn
	rows        []columnDeclaredRow
	indexByPath map[string]int
}

func newTypedColumnDeclaredRowSource(allColumns []ColumnStoreColumn, rows []columnDeclaredRow) typedColumnDeclaredRowSource {
	indexByPath := make(map[string]int, len(allColumns))
	for i, col := range allColumns {
		indexByPath[col.Path] = i
	}
	return typedColumnDeclaredRowSource{allColumns: allColumns, rows: rows, indexByPath: indexByPath}
}

func (s typedColumnDeclaredRowSource) Len() int {
	return len(s.rows)
}

func (s typedColumnDeclaredRowSource) PrimaryID(rowIdx int) int64 {
	return int64(rowIdx)
}

func (s typedColumnDeclaredRowSource) Value(rowIdx int, column typedColumnAdapterColumn) (columnDeclaredValue, bool, error) {
	if rowIdx < 0 || rowIdx >= len(s.rows) {
		return columnDeclaredValue{}, false, fmt.Errorf("row index=%d outside rows=%d", rowIdx, len(s.rows))
	}
	row := s.rows[rowIdx]
	if row.Deleted {
		return columnDeclaredValue{}, false, fmt.Errorf("row[%d] is deleted", rowIdx)
	}
	if len(row.Values) != len(s.allColumns) {
		return columnDeclaredValue{}, false, fmt.Errorf("row[%d] values=%d columns=%d", rowIdx, len(row.Values), len(s.allColumns))
	}
	allIdx, ok := s.indexByPath[column.Field.Path]
	if !ok {
		return columnDeclaredValue{}, false, fmt.Errorf("typed-column part field %q not found", column.Field.Path)
	}
	return row.Values[allIdx], true, nil
}

func (s typedColumnDeclaredRowSource) ValueIndex(column typedColumnAdapterColumn) (int, error) {
	allIdx, ok := s.indexByPath[column.Field.Path]
	if !ok {
		return -1, fmt.Errorf("typed-column part field %q not found", column.Field.Path)
	}
	return allIdx, nil
}

func (s typedColumnDeclaredRowSource) ValueAt(rowIdx, valueIdx int) (columnDeclaredValue, bool, error) {
	if rowIdx < 0 || rowIdx >= len(s.rows) {
		return columnDeclaredValue{}, false, fmt.Errorf("row index=%d outside rows=%d", rowIdx, len(s.rows))
	}
	if valueIdx < 0 || valueIdx >= len(s.allColumns) {
		return columnDeclaredValue{}, false, fmt.Errorf("value index=%d outside columns=%d", valueIdx, len(s.allColumns))
	}
	row := s.rows[rowIdx]
	if row.Deleted {
		return columnDeclaredValue{}, false, fmt.Errorf("row[%d] is deleted", rowIdx)
	}
	if len(row.Values) != len(s.allColumns) {
		return columnDeclaredValue{}, false, fmt.Errorf("row[%d] values=%d columns=%d", rowIdx, len(row.Values), len(s.allColumns))
	}
	return row.Values[valueIdx], true, nil
}

type typedColumnAdapterPart struct {
	Options    typedColumnAdapterOptions
	Columns    []typedColumnAdapterColumn
	Part       *typedcolumn.ColumnPart
	Dictionary map[string]map[string]int64
	Metrics    typedColumnAdapterBuildMetrics
}

type typedColumnAdapterBuildMetrics struct {
	DictionaryBuild time.Duration
	BatchAllocation time.Duration
	BatchFill       time.Duration
	PartBuild       time.Duration
}

type typedColumnAdapterStringDictionaryBuildState struct {
	codeByValue           map[string]int64
	valuesByTemporaryCode []string
}

type typedColumnPartDecodedValues struct {
	PrimaryIDs     []int64
	RowByPrimaryID []int
	Values         [][]columnDeclaredValue
}

type typedColumnAdapterResourceReader struct {
	Manager       *mappedresource.Manager
	Image         typedcolumn.ColumnPartImage
	Path          string
	Namespace     string
	Generation    uint64
	PartID        uint64
	FileID        uint32
	Scope         mappedresource.Scope
	PreferMapped  bool
	AllowHeapCopy bool
}

type typedColumnAdapterImageSummary struct {
	PartID       uint64
	Rows         int
	Sections     int
	SectionBytes uint64
	SortKey      []ColumnSortKey
}

func typedColumnAdapterTypeMatrix() []typedColumnAdapterTypeMapping {
	return []typedColumnAdapterTypeMapping{
		{ValueType: ColumnStoreValueBool, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeBool, Encoding: typedcolumn.EncodingBoolBitpackRLE},
		{ValueType: ColumnStoreValueInt64, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingDeltaVarint},
		{ValueType: ColumnStoreValueFloat32, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Reason: "default compatibility carrier stores raw int64 float32 bit patterns; fixed_width_encoding little_endian selects native raw_float32"},
		{ValueType: ColumnStoreValueDouble, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Reason: "default compatibility carrier stores raw int64 float64 bit patterns; fixed_width_encoding little_endian selects native raw_float64"},
		{ValueType: ColumnStoreValueString, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeLowCardinalityCode, Encoding: typedcolumn.EncodingLowCardinalityUint32, Reason: "stored as dictionary codes with dictionary section metadata"},
		{ValueType: ColumnStoreValueInt8, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeInt8, Encoding: typedcolumn.EncodingRawInt8, Reason: "stored as raw fixed-width little-endian int8 sections"},
		{ValueType: ColumnStoreValueUint8, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeUint8, Encoding: typedcolumn.EncodingRawUint8, Reason: "stored as raw fixed-width little-endian uint8 sections"},
		{ValueType: ColumnStoreValueInt16, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeInt16, Encoding: typedcolumn.EncodingRawInt16, Reason: "stored as raw fixed-width little-endian int16 sections"},
		{ValueType: ColumnStoreValueUint16, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeUint16, Encoding: typedcolumn.EncodingRawUint16, Reason: "stored as raw fixed-width little-endian uint16 sections"},
		{ValueType: ColumnStoreValueInt32, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeInt32, Encoding: typedcolumn.EncodingRawInt32, Reason: "stored as raw fixed-width little-endian int32 sections"},
		{ValueType: ColumnStoreValueUint32, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeUint32, Encoding: typedcolumn.EncodingRawUint32, Reason: "stored as raw fixed-width little-endian uint32 sections"},
		{ValueType: ColumnStoreValueUint64, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeUint64, Encoding: typedcolumn.EncodingRawUint64, Reason: "stored as raw fixed-width little-endian uint64 sections"},
		{ValueType: ColumnStoreValueFloat16, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeFloat16, Encoding: typedcolumn.EncodingRawFloat16, Reason: "stored as storage-only raw IEEE binary16 bit sections"},
		{ValueType: ColumnStoreValueBFloat16, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeBFloat16, Encoding: typedcolumn.EncodingRawBFloat16, Reason: "stored as storage-only raw bfloat16 bit sections"},
		{ValueType: ColumnStoreValueUint8Vector, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeUint8Vector, Encoding: typedcolumn.EncodingRawUint8Vector, Reason: "stored as row-major dense raw uint8 vector sections"},
		{ValueType: ColumnStoreValueInt8Vector, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeInt8Vector, Encoding: typedcolumn.EncodingRawInt8Vector, Reason: "stored as row-major dense raw int8 vector sections"},
		{ValueType: ColumnStoreValueUint16Vector, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeUint16Vector, Encoding: typedcolumn.EncodingRawUint16Vector, Reason: "stored as row-major dense little-endian uint16 vector sections"},
		{ValueType: ColumnStoreValueInt16Vector, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeInt16Vector, Encoding: typedcolumn.EncodingRawInt16Vector, Reason: "stored as row-major dense little-endian int16 vector sections"},
		{ValueType: ColumnStoreValueUint32Vector, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeUint32Vector, Encoding: typedcolumn.EncodingRawUint32Vector, Reason: "stored as generic row-major dense little-endian uint32 vector sections, separate from adjacency_list semantics"},
		{ValueType: ColumnStoreValueInt32Vector, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeInt32Vector, Encoding: typedcolumn.EncodingRawInt32Vector, Reason: "stored as row-major dense little-endian int32 vector sections"},
		{ValueType: ColumnStoreValueUint64Vector, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeUint64Vector, Encoding: typedcolumn.EncodingRawUint64Vector, Reason: "stored as row-major dense little-endian uint64 vector sections"},
		{ValueType: ColumnStoreValueInt64Vector, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeInt64Vector, Encoding: typedcolumn.EncodingRawInt64Vector, Reason: "stored as row-major dense little-endian int64 vector sections"},
		{ValueType: ColumnStoreValueFloat16Vector, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeFloat16Vector, Encoding: typedcolumn.EncodingRawFloat16Vector, Reason: "stored as row-major dense little-endian raw float16-bit vector sections"},
		{ValueType: ColumnStoreValueBFloat16Vector, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeBFloat16Vector, Encoding: typedcolumn.EncodingRawBFloat16Vector, Reason: "stored as row-major dense little-endian raw bfloat16-bit vector sections"},
		{ValueType: ColumnStoreValueFloat32Vector, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeFloat32Vector, Encoding: typedcolumn.EncodingRawFloat32Vector, Reason: "stored as fixed-dim dense little-endian float32 sections"},
		{ValueType: ColumnStoreValueFloat64Vector, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeFloat64Vector, Encoding: typedcolumn.EncodingRawFloat64Vector, Reason: "stored as row-major dense little-endian float64 vector sections"},
		{ValueType: ColumnStoreValueByteVector, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeFixedBytes, Encoding: typedcolumn.EncodingRawFixedBytes, Reason: "stored as fixed-width byte_vector/fixed_bytes sections"},
		{ValueType: ColumnStoreValuePackedBitVector, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypePackedBitVector, Encoding: typedcolumn.EncodingRawPackedBitVector, Reason: "stored as row-major packed 1-bit unsigned code vector sections with zero padding"},
		{ValueType: ColumnStoreValuePackedUint2Vector, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypePackedUint2Vector, Encoding: typedcolumn.EncodingRawPackedUint2Vector, Reason: "stored as row-major packed 2-bit unsigned code vector sections with zero padding"},
		{ValueType: ColumnStoreValuePackedUint4Vector, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypePackedUint4Vector, Encoding: typedcolumn.EncodingRawPackedUint4Vector, Reason: "stored as row-major packed 4-bit unsigned code vector sections with zero padding"},
		{ValueType: ColumnStoreValueUint32List, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeUint32List, Encoding: typedcolumn.EncodingRawUint32OffsetsList, Reason: "stored as generic non-null uint32_list sections with uint64 offsets plus flattened uint32 values"},
		{ValueType: ColumnStoreValueBytes, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeBytes, Encoding: typedcolumn.EncodingRawBytesOffsets, Reason: "stored as generic non-null bytes sections with uint64 offsets plus exact opaque payload bytes"},
		{ValueType: ColumnStoreValueAdjacencyList, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeAdjacencyList, Encoding: typedcolumn.EncodingRawUint32Dense, Reason: "stored as fixed-degree dense little-endian uint32 sections by default; explicit adjacency_layout uint32_offsets_list selects the variable offsets-list writer/fallback/direct reader"},
	}
}

func typedColumnAdapterMappingForValueType(valueType ColumnStoreValueType) (typedColumnAdapterTypeMapping, error) {
	for _, mapping := range typedColumnAdapterTypeMatrix() {
		if mapping.ValueType == valueType {
			if mapping.Status != typedColumnAdapterRepresented {
				return mapping, fmt.Errorf("%w: %s: %s", errTypedColumnAdapterUnsupportedType, valueType, mapping.Reason)
			}
			return mapping, nil
		}
	}
	return typedColumnAdapterTypeMapping{ValueType: valueType, Status: typedColumnAdapterFailClosed, Reason: "unknown declared value type"}, fmt.Errorf("%w: %s", errTypedColumnAdapterUnsupportedType, valueType)
}

func typedColumnAdapterUint32ListDirectPayloadSupported(column typedColumnAdapterColumn) bool {
	return column.Field.ValueType == ColumnStoreValueUint32List &&
		!column.Field.Nullable &&
		column.Definition.Type == typedcolumn.ColumnTypeUint32List &&
		column.Definition.Encoding == typedcolumn.EncodingRawUint32OffsetsList &&
		column.Definition.Compression == typedcolumn.CompressionNone &&
		column.Definition.FixedWidthElements == 0
}

func typedColumnAdapterBytesDirectPayloadSupported(column typedColumnAdapterColumn) bool {
	return column.Field.ValueType == ColumnStoreValueBytes &&
		!column.Field.Nullable &&
		column.Definition.Type == typedcolumn.ColumnTypeBytes &&
		column.Definition.Encoding == typedcolumn.EncodingRawBytesOffsets &&
		column.Definition.Compression == typedcolumn.CompressionNone &&
		column.Definition.FixedWidthElements == 0
}

func typedColumnAdapterFixedBytesDirectPayloadSupported(column typedColumnAdapterColumn) bool {
	return column.Field.ValueType == ColumnStoreValueByteVector &&
		!column.Field.Nullable &&
		column.Field.BytesPerRow > 0 &&
		column.Definition.Type == typedcolumn.ColumnTypeFixedBytes &&
		column.Definition.Encoding == typedcolumn.EncodingRawFixedBytes &&
		column.Definition.Compression == typedcolumn.CompressionNone &&
		column.Definition.FixedWidthElements == column.Field.BytesPerRow &&
		column.Definition.BitsPerElement == 0
}

func typedColumnAdapterPackedUintDirectPayloadSupported(column typedColumnAdapterColumn) bool {
	wantType, wantEncoding, wantBits, ok := typedColumnPackedUintVectorMapping(column.Field.ValueType)
	return ok &&
		!column.Field.Nullable &&
		column.Definition.Type == wantType &&
		column.Field.ElementsPerRow > 0 &&
		column.Definition.Encoding == wantEncoding &&
		column.Definition.Compression == typedcolumn.CompressionNone &&
		column.Definition.FixedWidthElements == column.Field.ElementsPerRow &&
		column.Definition.BitsPerElement == wantBits
}

// typedColumnAdapterOffsetsListAdjacencyDirectPayloadSupported gates the current
// quarantined adjacency_list selector. The generic uint32_list adapter path is
// typedColumnAdapterUint32ListDirectPayloadSupported.
func typedColumnAdapterOffsetsListAdjacencyDirectPayloadSupported(column typedColumnAdapterColumn) bool {
	return column.Field.ValueType == ColumnStoreValueAdjacencyList &&
		!column.Field.Nullable &&
		column.Definition.Encoding == typedcolumn.EncodingRawUint32OffsetsList &&
		column.Definition.Compression == typedcolumn.CompressionNone &&
		column.Definition.FixedWidthElements == 0
}

func typedColumnAdapterMapField(field TypedStorageField) (typedColumnAdapterColumn, error) {
	if field.Owner != TypedStorageOwnerColumnPart {
		return typedColumnAdapterColumn{}, fmt.Errorf("collections: typed-column adapter field %q owner=%q want %q", field.Path, field.Owner, TypedStorageOwnerColumnPart)
	}
	def, err := typedColumnProductionDefinitionForField(field)
	if err != nil {
		return typedColumnAdapterColumn{}, err
	}
	return typedColumnAdapterColumn{Field: field, Definition: def, FixedWidthEncoding: field.FixedWidthEncoding}, nil
}

func typedColumnAdapterMapFieldWithOptions(field TypedStorageField, opts typedColumnAdapterOptions) (typedColumnAdapterColumn, error) {
	column, err := typedColumnAdapterMapField(field)
	if err != nil {
		return typedColumnAdapterColumn{}, err
	}
	if !typedColumnAdapterDefinitionOptionsActive(opts) {
		return column, nil
	}
	def := column.Definition
	if err := applyTypedColumnAdapterDefinitionOptions(field, &def, opts); err != nil {
		return typedColumnAdapterColumn{}, err
	}
	column.Definition = def
	return column, nil
}

func typedColumnAdapterDefinitionOptionsActive(opts typedColumnAdapterOptions) bool {
	return opts.Int64EncodingSet || opts.DefaultCompressionSet
}

func applyTypedColumnAdapterDefinitionOptions(field TypedStorageField, def *typedcolumn.ColumnDefinition, opts typedColumnAdapterOptions) error {
	if def == nil {
		return errors.New("collections: typed-column adapter nil definition")
	}
	if opts.Int64EncodingSet && field.ValueType == ColumnStoreValueInt64 {
		if field.Nullable || field.FixedWidthEncoding != ColumnFixedWidthEncodingDefault {
			return fmt.Errorf("%w: int64 encoding override %s is only supported for non-null default-encoding int64 fields, got field %q value_type=%s nullable=%v fixed_width_encoding=%q", errTypedColumnProductionLayoutUnsupported, opts.Int64Encoding, field.Path, field.ValueType, field.Nullable, field.FixedWidthEncoding)
		}
		switch opts.Int64Encoding {
		case typedcolumn.EncodingRawInt64, typedcolumn.EncodingDeltaVarint, typedcolumn.EncodingDoubleDeltaVarint:
			def.Encoding = opts.Int64Encoding
		default:
			return fmt.Errorf("%w: unsupported int64 encoding override %s", errTypedColumnProductionLayoutUnsupported, opts.Int64Encoding)
		}
	}
	if opts.DefaultCompressionSet {
		if opts.DefaultCompressionOnlySupported {
			trial := *def
			trial.Compression = opts.DefaultCompression
			trial.CompressionSet = true
			if validateTypedColumnProductionDefinition(field, trial) == nil {
				*def = trial
			}
		} else {
			def.Compression = opts.DefaultCompression
			def.CompressionSet = true
		}
	}
	return validateTypedColumnProductionDefinition(field, *def)
}

func typedColumnAdapterPrimaryIDDefinition(opts typedColumnAdapterOptions) typedcolumn.ColumnDefinition {
	compression := typedcolumn.CompressionNone
	if opts.DefaultCompressionSet {
		compression = opts.DefaultCompression
	}
	return typedcolumn.ColumnDefinition{
		Name:           typedColumnAdapterPrimaryIDColumn,
		Type:           typedcolumn.ColumnTypeInt64,
		Encoding:       typedcolumn.EncodingDeltaVarint,
		Compression:    compression,
		CompressionSet: true,
		StatsDisabled:  true,
	}
}

func typedColumnAdapterColumnsForFields(fields []TypedStorageField) ([]typedColumnAdapterColumn, error) {
	return typedColumnAdapterColumnsForFieldsMapped(fields, nil)
}

func typedColumnAdapterColumnsForFieldsWithOptions(fields []TypedStorageField, opts typedColumnAdapterOptions) ([]typedColumnAdapterColumn, error) {
	if !typedColumnAdapterDefinitionOptionsActive(opts) {
		return typedColumnAdapterColumnsForFields(fields)
	}
	return typedColumnAdapterColumnsForFieldsMapped(fields, &opts)
}

func typedColumnAdapterColumnsForFieldsMapped(fields []TypedStorageField, opts *typedColumnAdapterOptions) ([]typedColumnAdapterColumn, error) {
	columns := make([]typedColumnAdapterColumn, 0, len(fields))
	seenColumns := map[string]struct{}{typedColumnAdapterPrimaryIDColumn: {}}
	seenNames := make(map[string]string, len(fields))
	seenPaths := make(map[string]string, len(fields))
	for _, field := range fields {
		var column typedColumnAdapterColumn
		var err error
		if opts != nil {
			column, err = typedColumnAdapterMapFieldWithOptions(field, *opts)
		} else {
			column, err = typedColumnAdapterMapField(field)
		}
		if err != nil {
			return nil, err
		}
		if _, exists := seenColumns[column.Definition.Name]; exists {
			return nil, fmt.Errorf("collections: typed-column adapter duplicate column %q", column.Definition.Name)
		}
		if field.Path != "" {
			if owner, exists := seenPaths[field.Path]; exists {
				return nil, fmt.Errorf("collections: typed-column adapter duplicate field path %q for columns %q and %q", field.Path, owner, column.Definition.Name)
			}
			if owner, exists := seenNames[field.Path]; exists {
				return nil, fmt.Errorf("collections: typed-column adapter ambiguous field path %q collides with field name %q", field.Path, owner)
			}
		}
		if field.Name != "" {
			if owner, exists := seenPaths[field.Name]; exists {
				return nil, fmt.Errorf("collections: typed-column adapter ambiguous field name %q collides with field path %q", field.Name, owner)
			}
			seenNames[field.Name] = column.Definition.Name
		}
		seenColumns[column.Definition.Name] = struct{}{}
		if field.Path != "" {
			seenPaths[field.Path] = column.Definition.Name
		}
		columns = append(columns, column)
	}
	return columns, nil
}

func typedColumnAdapterSortKey(opts typedColumnAdapterOptions, columns []typedColumnAdapterColumn) ([]typedcolumn.SortKeyColumn, error) {
	if len(opts.SortKey) == 0 {
		return []typedcolumn.SortKeyColumn{{Column: typedColumnAdapterPrimaryIDColumn}}, nil
	}
	if len(opts.SortKey) > typedColumnPartSortKeyMaxColumns {
		return nil, fmt.Errorf("collections: typed-column adapter sort key columns=%d exceeds cap %d", len(opts.SortKey), typedColumnPartSortKeyMaxColumns)
	}
	byName := make(map[string]typedColumnAdapterColumn, len(columns))
	for _, column := range columns {
		if column.Field.Name != "" {
			byName[column.Field.Name] = column
		}
	}
	out := make([]typedcolumn.SortKeyColumn, 0, len(opts.SortKey))
	seen := make(map[string]struct{}, len(opts.SortKey))
	for _, sortKey := range opts.SortKey {
		column, ok := byName[sortKey.Column]
		if !ok {
			return nil, fmt.Errorf("collections: typed-column adapter sort key column %q is not owned by typed_column_part by field name; sort keys must reference column names, not paths", sortKey.Column)
		}
		if _, exists := seen[column.Definition.Name]; exists {
			return nil, fmt.Errorf("collections: typed-column adapter duplicate sort key column %q", sortKey.Column)
		}
		seen[column.Definition.Name] = struct{}{}
		if sortKey.Direction != ColumnSortAscending {
			return nil, fmt.Errorf("collections: typed-column adapter sort key column %q direction %q is unsupported; only ascending is supported", sortKey.Column, sortKey.Direction)
		}
		if column.Field.Nullable {
			return nil, fmt.Errorf("collections: typed-column adapter sort key column %q is nullable; null/default ordering is not defined", sortKey.Column)
		}
		if !columnStoreValueTypeSupportsTypedColumnPartSort(column.Field.ValueType) {
			return nil, fmt.Errorf("collections: typed-column adapter sort key column %q value_type %q is unsupported", sortKey.Column, column.Field.ValueType)
		}
		if column.Field.ValueType == ColumnStoreValueString {
			if err := validateTypedColumnAdapterStringDictionaryLogicalOrder(column); err != nil {
				return nil, fmt.Errorf("collections: typed-column adapter sort key column %q: %w", sortKey.Column, err)
			}
		}
		out = append(out, typedcolumn.SortKeyColumn{Column: column.Definition.Name, Direction: typedcolumn.SortKeyAsc})
	}
	return out, nil
}

func columnSortKeysFromTypedColumnSortKeys(sortKeys []typedcolumn.SortKeyColumn) []ColumnSortKey {
	if len(sortKeys) == 0 {
		return nil
	}
	count := 0
	for _, sortKey := range sortKeys {
		if sortKey.Column != typedColumnAdapterPrimaryIDColumn {
			count++
		}
	}
	if count == 0 {
		return nil
	}
	out := make([]ColumnSortKey, 0, count)
	for _, sortKey := range sortKeys {
		if sortKey.Column == typedColumnAdapterPrimaryIDColumn {
			continue
		}
		direction := ColumnSortAscending
		if sortKey.Direction == typedcolumn.SortKeyDesc {
			direction = ColumnSortDescending
		}
		out = append(out, ColumnSortKey{Column: sortKey.Column, Direction: direction})
	}
	return out
}

func typedColumnPartDescriptorHasLogicalSortKey(desc typedcolumn.ColumnPartDescriptor) bool {
	return len(desc.SortKey) != 0 && !typedColumnSortKeyIsSyntheticPrimaryID(desc.SortKey)
}

func typedColumnAdapterPartHasLogicalSortKey(part *typedColumnAdapterPart) bool {
	return part != nil && part.Part != nil && typedColumnPartDescriptorHasLogicalSortKey(part.Part.Descriptor)
}

func typedColumnPreparedPartHasLogicalSortKey(part *typedColumnPreparedPartState) bool {
	return part != nil && typedColumnPartDescriptorHasLogicalSortKey(part.Descriptor)
}

func typedColumnSortKeyIsSyntheticPrimaryID(sortKey []typedcolumn.SortKeyColumn) bool {
	if len(sortKey) != 1 {
		return false
	}
	column := sortKey[0]
	return column.Column == typedColumnAdapterPrimaryIDColumn &&
		(column.Direction == "" || column.Direction == typedcolumn.SortKeyAsc) &&
		column.Nulls == typedcolumn.SortKeyNullsDefault
}

func validateTypedColumnAdapterStringDictionaryLogicalOrder(column typedColumnAdapterColumn) error {
	if len(column.Dictionary) == 0 {
		if column.Field.Nullable {
			return nil
		}
		return fmt.Errorf("string dictionary is empty")
	}
	valuesByCode := make([]string, len(column.Dictionary))
	seenCode := make([]bool, len(column.Dictionary))
	for value, code := range column.Dictionary {
		if code < 0 || int(code) >= len(valuesByCode) {
			return fmt.Errorf("dictionary code %d outside cardinality %d", code, len(valuesByCode))
		}
		if seenCode[code] {
			return fmt.Errorf("duplicate dictionary code %d", code)
		}
		seenCode[code] = true
		valuesByCode[code] = value
	}
	for i := 1; i < len(valuesByCode); i++ {
		if valuesByCode[i-1] > valuesByCode[i] {
			return fmt.Errorf("dictionary code order is not logical bytewise ascending at code %d (%q > %q)", i, valuesByCode[i-1], valuesByCode[i])
		}
	}
	return nil
}

func buildTypedColumnAdapterPart(opts typedColumnAdapterOptions, rows []typedColumnAdapterRow) (*typedColumnAdapterPart, error) {
	return buildTypedColumnAdapterPartFromSource(opts, typedColumnAdapterRowsSource(rows))
}

func buildTypedColumnAdapterPartFromDeclaredRows(opts typedColumnAdapterOptions, allColumns []ColumnStoreColumn, rows []columnDeclaredRow) (*typedColumnAdapterPart, error) {
	return buildTypedColumnAdapterPartFromSource(opts, newTypedColumnDeclaredRowSource(allColumns, rows))
}

func buildTypedColumnAdapterPartFromSource(opts typedColumnAdapterOptions, rowSource typedColumnAdapterRowSource) (*typedColumnAdapterPart, error) {
	if opts.PartID == 0 {
		opts.PartID = 1
	}
	var metrics typedColumnAdapterBuildMetrics
	columns, err := typedColumnAdapterColumnsForFieldsWithOptions(opts.Fields, opts)
	if err != nil {
		return nil, err
	}
	indexedSource, valueIndexes, err := typedColumnAdapterIndexedSourceColumns(rowSource, columns)
	if err != nil {
		return nil, err
	}
	stringDictionaryStates := make([]*typedColumnAdapterStringDictionaryBuildState, len(columns))
	for i := range columns {
		if typedColumnAdapterFusedStringDictionaryColumn(columns[i]) {
			stringDictionaryStates[i] = newTypedColumnAdapterStringDictionaryBuildState()
		}
	}

	rowCount := rowSource.Len()
	batchAllocStart := time.Now()
	batch := typedcolumn.Batch{Rows: rowCount, Columns: make(map[string][]int64, len(columns)+1), Nulls: make(map[string][]bool), Defaults: make(map[string][]bool)}
	batch.Columns[typedColumnAdapterPrimaryIDColumn] = make([]int64, rowCount)
	for _, column := range columns {
		switch column.Definition.Type {
		case typedcolumn.ColumnTypeFloat32:
			if batch.Float32Columns == nil {
				batch.Float32Columns = make(map[string][]float32)
			}
			batch.Float32Columns[column.Definition.Name] = make([]float32, rowCount)
		case typedcolumn.ColumnTypeFloat64:
			if batch.Float64Columns == nil {
				batch.Float64Columns = make(map[string][]float64)
			}
			batch.Float64Columns[column.Definition.Name] = make([]float64, rowCount)
		case typedcolumn.ColumnTypeFloat32Vector:
			if batch.Float32Vectors == nil {
				batch.Float32Vectors = make(map[string][]float32)
			}
			elements, err := typedColumnAdapterDenseElements(rowCount, column.Definition.FixedWidthElements)
			if err != nil {
				return nil, err
			}
			batch.Float32Vectors[column.Definition.Name] = make([]float32, elements)
		case typedcolumn.ColumnTypeUint8Vector, typedcolumn.ColumnTypeInt8Vector,
			typedcolumn.ColumnTypeUint16Vector, typedcolumn.ColumnTypeInt16Vector,
			typedcolumn.ColumnTypeUint32Vector, typedcolumn.ColumnTypeInt32Vector,
			typedcolumn.ColumnTypeUint64Vector, typedcolumn.ColumnTypeInt64Vector,
			typedcolumn.ColumnTypeFloat16Vector, typedcolumn.ColumnTypeBFloat16Vector,
			typedcolumn.ColumnTypeFloat64Vector:
			_, _, width, ok := typedColumnDenseNumericVectorMapping(column.Field.ValueType)
			if !ok {
				return nil, fmt.Errorf("%w: unsupported dense numeric vector value_type=%s", errTypedColumnAdapterUnsupportedType, column.Field.ValueType)
			}
			bytes, err := typedColumnAdapterDenseBytes(rowCount, column.Definition.FixedWidthElements, width)
			if err != nil {
				return nil, err
			}
			if batch.DenseFixedWidthVectors == nil {
				batch.DenseFixedWidthVectors = make(map[string]typedcolumn.RawDenseFixedWidth)
			}
			batch.DenseFixedWidthVectors[column.Definition.Name] = typedcolumn.RawDenseFixedWidth{Rows: rowCount, ElementsPerRow: column.Definition.FixedWidthElements, ElementWidthBytes: width, Values: make([]byte, bytes)}
		case typedcolumn.ColumnTypeFixedBytes:
			bytes, err := typedColumnAdapterDenseBytes(rowCount, column.Definition.FixedWidthElements, 1)
			if err != nil {
				return nil, err
			}
			if batch.FixedBytesColumns == nil {
				batch.FixedBytesColumns = make(map[string]typedcolumn.FixedBytesRows)
			}
			batch.FixedBytesColumns[column.Definition.Name] = typedcolumn.FixedBytesRows{Rows: rowCount, BytesPerRow: column.Definition.FixedWidthElements, Values: make([]byte, bytes)}
		case typedcolumn.ColumnTypePackedBitVector, typedcolumn.ColumnTypePackedUint2Vector, typedcolumn.ColumnTypePackedUint4Vector:
			bitsPerElement, ok := typedcolumn.PackedUintVectorBits(column.Definition.Type)
			if !ok {
				return nil, fmt.Errorf("%w: unsupported packed uint column type %s", errTypedColumnAdapterUnsupportedType, column.Definition.Type)
			}
			rowBytes, err := typedcolumn.PackedUintRowBytes(column.Definition.FixedWidthElements, bitsPerElement)
			if err != nil {
				return nil, err
			}
			bytes, err := typedColumnAdapterDenseBytes(rowCount, rowBytes, 1)
			if err != nil {
				return nil, err
			}
			if batch.PackedUintColumns == nil {
				batch.PackedUintColumns = make(map[string]typedcolumn.PackedUintRows)
			}
			batch.PackedUintColumns[column.Definition.Name] = typedcolumn.PackedUintRows{Rows: rowCount, ElementsPerRow: column.Definition.FixedWidthElements, BitsPerElement: bitsPerElement, BytesPerRow: rowBytes, Values: make([]byte, bytes)}
		case typedcolumn.ColumnTypeUint32List:
			if batch.Uint32OffsetsLists == nil {
				batch.Uint32OffsetsLists = make(map[string]typedcolumn.RawUint32OffsetsList)
			}
			batch.Uint32OffsetsLists[column.Definition.Name] = typedcolumn.Uint32List{Rows: rowCount, Offsets: make([]uint64, rowCount+1)}
		case typedcolumn.ColumnTypeBytes:
			if batch.BytesColumns == nil {
				batch.BytesColumns = make(map[string]typedcolumn.RawBytesOffsets)
			}
			batch.BytesColumns[column.Definition.Name] = typedcolumn.BytesColumn{Rows: rowCount, Offsets: make([]uint64, rowCount+1)}
		case typedcolumn.ColumnTypeInt8,
			typedcolumn.ColumnTypeUint8,
			typedcolumn.ColumnTypeInt16,
			typedcolumn.ColumnTypeUint16,
			typedcolumn.ColumnTypeInt32,
			typedcolumn.ColumnTypeUint32,
			typedcolumn.ColumnTypeUint64,
			typedcolumn.ColumnTypeFloat16,
			typedcolumn.ColumnTypeBFloat16:
			if !typedColumnAdapterInitPrimitiveScalarBatchColumn(&batch, column, rowCount) {
				return nil, fmt.Errorf("%w: unsupported primitive scalar column type %s", errTypedColumnAdapterUnsupportedType, column.Definition.Type)
			}
		case typedcolumn.ColumnTypeAdjacencyList:
			if column.Definition.Encoding == typedcolumn.EncodingRawUint32OffsetsList {
				if batch.Uint32OffsetsLists == nil {
					batch.Uint32OffsetsLists = make(map[string]typedcolumn.RawUint32OffsetsList)
				}
				batch.Uint32OffsetsLists[column.Definition.Name] = typedcolumn.RawUint32OffsetsList{Rows: rowCount, Offsets: make([]uint64, rowCount+1)}
			} else {
				if batch.Uint32Vectors == nil {
					batch.Uint32Vectors = make(map[string][]uint32)
				}
				elements, err := typedColumnAdapterDenseElements(rowCount, column.Definition.FixedWidthElements)
				if err != nil {
					return nil, err
				}
				batch.Uint32Vectors[column.Definition.Name] = make([]uint32, elements)
			}
		default:
			batch.Columns[column.Definition.Name] = make([]int64, rowCount)
			if column.Field.Nullable {
				batch.Nulls[column.Definition.Name] = make([]bool, rowCount)
				batch.Defaults[column.Definition.Name] = make([]bool, rowCount)
			}
		}
	}
	metrics.BatchAllocation += time.Since(batchAllocStart)
	batchFillStart := time.Now()
	for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
		batch.Columns[typedColumnAdapterPrimaryIDColumn][rowIdx] = rowSource.PrimaryID(rowIdx)
		for columnIdx, column := range columns {
			var value columnDeclaredValue
			var ok bool
			var err error
			if indexedSource != nil {
				value, ok, err = indexedSource.ValueAt(rowIdx, valueIndexes[columnIdx])
			} else {
				value, ok, err = rowSource.Value(rowIdx, column)
			}
			if err != nil {
				return nil, fmt.Errorf("collections: typed-column adapter row %d field %q: %w", rowIdx, column.Field.Path, err)
			}
			if !ok {
				if !column.Field.Nullable {
					return nil, fmt.Errorf("collections: typed-column adapter row %d missing field %q", rowIdx, column.Field.Path)
				}
				value = columnDeclaredValue{Type: column.Field.ValueType, Present: false, Null: true}
			}
			switch column.Definition.Type {
			case typedcolumn.ColumnTypeFloat32:
				encoded, err := encodeTypedColumnAdapterNativeFloat32Value(column, value)
				if err != nil {
					return nil, fmt.Errorf("collections: typed-column adapter row %d field %q: %w", rowIdx, column.Field.Path, err)
				}
				batch.Float32Columns[column.Definition.Name][rowIdx] = encoded
			case typedcolumn.ColumnTypeFloat64:
				encoded, err := encodeTypedColumnAdapterNativeFloat64Value(column, value)
				if err != nil {
					return nil, fmt.Errorf("collections: typed-column adapter row %d field %q: %w", rowIdx, column.Field.Path, err)
				}
				batch.Float64Columns[column.Definition.Name][rowIdx] = encoded
			case typedcolumn.ColumnTypeFloat32Vector:
				if err := encodeTypedColumnAdapterFloat32VectorValue(batch.Float32Vectors[column.Definition.Name], rowIdx, column, value); err != nil {
					return nil, fmt.Errorf("collections: typed-column adapter row %d field %q: %w", rowIdx, column.Field.Path, err)
				}
			case typedcolumn.ColumnTypeUint8Vector, typedcolumn.ColumnTypeInt8Vector,
				typedcolumn.ColumnTypeUint16Vector, typedcolumn.ColumnTypeInt16Vector,
				typedcolumn.ColumnTypeUint32Vector, typedcolumn.ColumnTypeInt32Vector,
				typedcolumn.ColumnTypeUint64Vector, typedcolumn.ColumnTypeInt64Vector,
				typedcolumn.ColumnTypeFloat16Vector, typedcolumn.ColumnTypeBFloat16Vector,
				typedcolumn.ColumnTypeFloat64Vector:
				dense := batch.DenseFixedWidthVectors[column.Definition.Name]
				if err := encodeTypedColumnAdapterDenseNumericVectorValue(dense, rowIdx, column, value); err != nil {
					return nil, fmt.Errorf("collections: typed-column adapter row %d field %q: %w", rowIdx, column.Field.Path, err)
				}
				batch.DenseFixedWidthVectors[column.Definition.Name] = dense
			case typedcolumn.ColumnTypeFixedBytes:
				fixed := batch.FixedBytesColumns[column.Definition.Name]
				if err := encodeTypedColumnAdapterFixedBytesValue(fixed, rowIdx, column, value); err != nil {
					return nil, fmt.Errorf("collections: typed-column adapter row %d field %q: %w", rowIdx, column.Field.Path, err)
				}
				batch.FixedBytesColumns[column.Definition.Name] = fixed
			case typedcolumn.ColumnTypePackedBitVector, typedcolumn.ColumnTypePackedUint2Vector, typedcolumn.ColumnTypePackedUint4Vector:
				packed := batch.PackedUintColumns[column.Definition.Name]
				if err := encodeTypedColumnAdapterPackedUintVectorValue(packed, rowIdx, column, value); err != nil {
					return nil, fmt.Errorf("collections: typed-column adapter row %d field %q: %w", rowIdx, column.Field.Path, err)
				}
				batch.PackedUintColumns[column.Definition.Name] = packed
			case typedcolumn.ColumnTypeUint32List:
				list := batch.Uint32OffsetsLists[column.Definition.Name]
				updated, err := encodeTypedColumnAdapterUint32ListValue(list, rowIdx, column, value)
				if err != nil {
					return nil, fmt.Errorf("collections: typed-column adapter row %d field %q: %w", rowIdx, column.Field.Path, err)
				}
				batch.Uint32OffsetsLists[column.Definition.Name] = updated
			case typedcolumn.ColumnTypeBytes:
				bytesColumn := batch.BytesColumns[column.Definition.Name]
				updated, err := encodeTypedColumnAdapterBytesValue(bytesColumn, rowIdx, column, value)
				if err != nil {
					return nil, fmt.Errorf("collections: typed-column adapter row %d field %q: %w", rowIdx, column.Field.Path, err)
				}
				batch.BytesColumns[column.Definition.Name] = updated
			case typedcolumn.ColumnTypeInt8,
				typedcolumn.ColumnTypeUint8,
				typedcolumn.ColumnTypeInt16,
				typedcolumn.ColumnTypeUint16,
				typedcolumn.ColumnTypeInt32,
				typedcolumn.ColumnTypeUint32,
				typedcolumn.ColumnTypeUint64,
				typedcolumn.ColumnTypeFloat16,
				typedcolumn.ColumnTypeBFloat16:
				if err := encodeTypedColumnAdapterPrimitiveScalarValue(&batch, rowIdx, column, value); err != nil {
					return nil, fmt.Errorf("collections: typed-column adapter row %d field %q: %w", rowIdx, column.Field.Path, err)
				}
			case typedcolumn.ColumnTypeAdjacencyList:
				if column.Definition.Encoding == typedcolumn.EncodingRawUint32OffsetsList {
					list := batch.Uint32OffsetsLists[column.Definition.Name]
					updated, err := encodeTypedColumnAdapterAdjacencyOffsetsListValue(list, rowIdx, column, value)
					if err != nil {
						return nil, fmt.Errorf("collections: typed-column adapter row %d field %q: %w", rowIdx, column.Field.Path, err)
					}
					batch.Uint32OffsetsLists[column.Definition.Name] = updated
				} else if err := encodeTypedColumnAdapterAdjacencyListValue(batch.Uint32Vectors[column.Definition.Name], rowIdx, column, value); err != nil {
					return nil, fmt.Errorf("collections: typed-column adapter row %d field %q: %w", rowIdx, column.Field.Path, err)
				}
			default:
				var encoded int64
				var isNull, isDefault bool
				if stringDictionaryStates[columnIdx] != nil {
					encoded, isNull, isDefault, err = encodeTypedColumnAdapterStringDictionaryValue(column, value, stringDictionaryStates[columnIdx])
				} else {
					encoded, isNull, isDefault, err = encodeTypedColumnAdapterScalarValue(column, value)
				}
				if err != nil {
					return nil, fmt.Errorf("collections: typed-column adapter row %d field %q: %w", rowIdx, column.Field.Path, err)
				}
				batch.Columns[column.Definition.Name][rowIdx] = encoded
				if column.Field.Nullable {
					batch.Nulls[column.Definition.Name][rowIdx] = isNull
					batch.Defaults[column.Definition.Name][rowIdx] = isDefault
				}
			}
		}
	}
	metrics.BatchFill += time.Since(batchFillStart)
	dictionaryStart := time.Now()
	if err := finalizeTypedColumnAdapterStringDictionaries(opts, columns, stringDictionaryStates, batch); err != nil {
		return nil, err
	}
	metrics.DictionaryBuild += time.Since(dictionaryStart)
	defs := make([]typedcolumn.ColumnDefinition, 0, len(columns)+1)
	defs = append(defs, typedColumnAdapterPrimaryIDDefinition(opts))
	for _, column := range columns {
		defs = append(defs, column.Definition)
	}
	partBuildStart := time.Now()
	rowsPerGranule := opts.RowsPerGranule
	if rowsPerGranule == 0 {
		rowsPerGranule = typedcolumn.DefaultRowsPerGranule
	}
	sortKey, err := typedColumnAdapterSortKey(opts, columns)
	if err != nil {
		return nil, err
	}
	partOpts := typedcolumn.Options{
		SchemaVersion: opts.SchemaVersion,
		SchemaMode:    typedcolumn.ColumnSchemaFixed,
		Columns:       defs,
		LogicalPrimaryKey: typedcolumn.LogicalPrimaryKey{
			Columns: []string{typedColumnAdapterPrimaryIDColumn},
		},
		SortKey:     typedcolumn.SortKey{Columns: sortKey},
		PartPolicy:  typedcolumn.ColumnPartPolicy{RowsPerGranule: rowsPerGranule, AdaptiveMarkSizing: opts.AdaptiveMarkSizing},
		Compression: typedcolumn.ColumnCompressionPolicy{Default: typedcolumn.CompressionNone},
	}
	part, err := typedcolumn.BuildColumnPart(opts.PartID, partOpts, batch)
	if err != nil {
		return nil, err
	}
	metrics.PartBuild += time.Since(partBuildStart)
	return &typedColumnAdapterPart{Options: opts, Columns: columns, Part: part, Dictionary: typedColumnAdapterDictionaries(columns), Metrics: metrics}, nil
}

func typedColumnAdapterPartFromBytes(opts typedColumnAdapterOptions, raw []byte) (*typedColumnAdapterPart, error) {
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return nil, err
	}
	return typedColumnAdapterPartFromImage(opts, image)
}

func typedColumnAdapterPartFromBytesForReconstruction(opts typedColumnAdapterOptions, raw []byte) (*typedColumnAdapterPart, error) {
	part, _, err := typedColumnAdapterPartFromBytesForReconstructionWithSummary(opts, raw)
	return part, err
}

func typedColumnAdapterPartFromBytesForReconstructionWithSummary(opts typedColumnAdapterOptions, raw []byte) (*typedColumnAdapterPart, typedColumnAdapterImageSummary, error) {
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return nil, typedColumnAdapterImageSummary{}, err
	}
	part, err := typedColumnAdapterPartFromImageWithoutRowLocators(opts, image)
	if err != nil {
		return nil, typedColumnAdapterImageSummary{}, err
	}
	sectionBytes := uint64(0)
	for _, section := range image.Sections {
		if section.Length > 0 {
			sectionBytes += uint64(section.Length)
		}
	}
	return part, typedColumnAdapterImageSummary{PartID: image.PartID, Rows: image.Rows, Sections: len(image.Sections), SectionBytes: sectionBytes, SortKey: columnSortKeysFromTypedColumnSortKeys(part.Part.Descriptor.SortKey)}, nil
}

func typedColumnAdapterPartFromImage(opts typedColumnAdapterOptions, image typedcolumn.ColumnPartImage) (*typedColumnAdapterPart, error) {
	part, err := typedcolumn.ColumnPartFromImage(image)
	if err != nil {
		return nil, err
	}
	return typedColumnAdapterPartFromDecodedImage(opts, image, part)
}

func typedColumnAdapterPartFromImageWithoutRowLocators(opts typedColumnAdapterOptions, image typedcolumn.ColumnPartImage) (*typedColumnAdapterPart, error) {
	return typedColumnAdapterPartFromImageWithoutRowLocatorsWithDiagnostics(opts, image, nil)
}

func typedColumnAdapterPartFromImageWithoutRowLocatorsWithDiagnostics(opts typedColumnAdapterOptions, image typedcolumn.ColumnPartImage, prepareDiagnostics *columnTypedColumnPhysicalQueryPrepareDiagnostics) (*typedColumnAdapterPart, error) {
	var phaseStart time.Time
	if prepareDiagnostics != nil {
		phaseStart = time.Now()
	}
	part, err := typedcolumn.ColumnPartFromImageWithOptions(image, typedcolumn.ColumnPartImageReadOptions{
		IncludeRowLocators:       false,
		ValidateRowLocators:      false,
		IncludeAggregateMetadata: false,
	})
	if prepareDiagnostics != nil {
		prepareDiagnostics.StateBuildNanos += time.Since(phaseStart).Nanoseconds()
	}
	if err != nil {
		return nil, err
	}
	return typedColumnAdapterPartFromDecodedImageWithDiagnostics(opts, image, part, prepareDiagnostics)
}

func typedColumnAdapterPartFromImageForInt64PredicateScan(opts typedColumnAdapterOptions, image typedcolumn.ColumnPartImage) (*typedColumnAdapterPart, error) {
	return typedColumnAdapterPartFromImageWithoutRowLocators(opts, image)
}

func typedColumnAdapterPartFromImageForStringPredicateScan(opts typedColumnAdapterOptions, image typedcolumn.ColumnPartImage) (*typedColumnAdapterPart, error) {
	return typedColumnAdapterPartFromImageWithoutRowLocators(opts, image)
}

func typedColumnAdapterInt64AggregatePartFromImage(opts typedColumnAdapterOptions, image typedcolumn.ColumnPartImage, adapterColumn typedColumnAdapterColumn) (*typedColumnAdapterPart, error) {
	part, err := typedcolumn.ColumnPartFromImageWithOptions(image, typedcolumn.ColumnPartImageReadOptions{
		IncludeRowLocators:       false,
		ValidateRowLocators:      false,
		IncludeAggregateMetadata: false,
	})
	if err != nil {
		return nil, err
	}
	if err := validateTypedColumnAdapterInt64AggregateImage(part, adapterColumn, opts.SchemaVersion); err != nil {
		return nil, err
	}
	if got, ok := part.Columns[adapterColumn.Definition.Name]; ok {
		adapterColumn.Definition = got.Definition
	}
	return &typedColumnAdapterPart{Options: opts, Columns: []typedColumnAdapterColumn{adapterColumn}, Part: part}, nil
}

func validateTypedColumnAdapterInt64AggregateImage(part *typedcolumn.ColumnPart, adapterColumn typedColumnAdapterColumn, schemaVersion uint32) error {
	if part == nil {
		return errors.New("collections: typed-column int64 aggregate nil image part")
	}
	if schemaVersion != 0 && part.Descriptor.SchemaVersion != schemaVersion {
		return fmt.Errorf("collections: typed-column adapter image schema_version=%d want %d", part.Descriptor.SchemaVersion, schemaVersion)
	}
	primary, ok := part.Columns[typedColumnAdapterPrimaryIDColumn]
	if !ok {
		return fmt.Errorf("collections: typed-column int64 aggregate image missing primary-id column %q", typedColumnAdapterPrimaryIDColumn)
	}
	if err := validateTypedColumnProductionPrimaryColumnLayout(primary); err != nil {
		return fmt.Errorf("collections: typed-column int64 aggregate image primary-id validation failed: %w", err)
	}
	got, ok := part.Columns[adapterColumn.Definition.Name]
	if !ok {
		return fmt.Errorf("collections: typed-column int64 aggregate image missing column %q", adapterColumn.Definition.Name)
	}
	if err := typedColumnAdapterValidateStoredDefinition(adapterColumn.Field, adapterColumn.Definition, got.Definition, "typed-column int64 aggregate image column"); err != nil {
		return err
	}
	if err := validateTypedColumnProductionPartColumnLayout(adapterColumn.Field, got); err != nil {
		return fmt.Errorf("collections: typed-column int64 aggregate image column %q layout validation failed: %w", adapterColumn.Definition.Name, err)
	}
	return nil
}

func typedColumnAdapterPartFromDecodedImage(opts typedColumnAdapterOptions, image typedcolumn.ColumnPartImage, part *typedcolumn.ColumnPart) (*typedColumnAdapterPart, error) {
	return typedColumnAdapterPartFromDecodedImageWithDiagnostics(opts, image, part, nil)
}

func typedColumnAdapterPartFromDecodedImageWithDiagnostics(opts typedColumnAdapterOptions, image typedcolumn.ColumnPartImage, part *typedcolumn.ColumnPart, prepareDiagnostics *columnTypedColumnPhysicalQueryPrepareDiagnostics) (*typedColumnAdapterPart, error) {
	var phaseStart time.Time
	if prepareDiagnostics != nil {
		phaseStart = time.Now()
	}
	columns, err := typedColumnAdapterColumnsForFieldsWithOptions(opts.Fields, opts)
	if err != nil {
		if prepareDiagnostics != nil {
			prepareDiagnostics.AdapterNanos += time.Since(phaseStart).Nanoseconds()
		}
		return nil, err
	}
	if err := validateTypedColumnAdapterImageSchema(part, columns, opts.SchemaVersion); err != nil {
		if prepareDiagnostics != nil {
			prepareDiagnostics.AdapterNanos += time.Since(phaseStart).Nanoseconds()
		}
		return nil, err
	}
	applyTypedColumnAdapterStoredDefinitions(columns, part)
	if prepareDiagnostics != nil {
		prepareDiagnostics.AdapterNanos += time.Since(phaseStart).Nanoseconds()
		phaseStart = time.Now()
	}
	dictionaries, err := image.Dictionaries()
	if err != nil {
		if prepareDiagnostics != nil {
			prepareDiagnostics.DictionaryNanos += time.Since(phaseStart).Nanoseconds()
		}
		return nil, err
	}
	if err := validateTypedColumnAdapterMetadata(dictionaries, columns); err != nil {
		if prepareDiagnostics != nil {
			prepareDiagnostics.DictionaryNanos += time.Since(phaseStart).Nanoseconds()
		}
		return nil, err
	}
	for i := range columns {
		if columns[i].Field.ValueType == ColumnStoreValueString {
			partColumn := part.Columns[columns[i].Definition.Name]
			dict := dictionaries[columns[i].Definition.Name]
			if len(dict) == 0 && !(columns[i].Field.Nullable && partColumn.Definition.Cardinality == 0) {
				return nil, fmt.Errorf("collections: typed-column adapter image missing dictionary for %q", columns[i].Definition.Name)
			}
			if err := validateTypedColumnAdapterStringDictionary(columns[i], partColumn.Definition.Cardinality, dict); err != nil {
				if prepareDiagnostics != nil {
					prepareDiagnostics.DictionaryNanos += time.Since(phaseStart).Nanoseconds()
				}
				return nil, err
			}
			columns[i].Definition.Cardinality = partColumn.Definition.Cardinality
			mode := typedColumnAdapterDictionaryModeForColumn(opts, columns[i].Definition.Name)
			if mode.Forward {
				columns[i].Dictionary = dict
			}
			if mode.Reverse {
				columns[i].ReverseDictionary = reverseTypedColumnAdapterDictionary(dict)
			}
			if mode.ValuesByCode {
				valuesByCode, err := typedColumnAdapterDictionaryValuesByCodeFromForward(dict, int(partColumn.Definition.Cardinality))
				if err != nil {
					if prepareDiagnostics != nil {
						prepareDiagnostics.DictionaryNanos += time.Since(phaseStart).Nanoseconds()
					}
					return nil, err
				}
				columns[i].DictionaryValuesByCode = valuesByCode
			}
		}
	}
	if prepareDiagnostics != nil {
		prepareDiagnostics.DictionaryNanos += time.Since(phaseStart).Nanoseconds()
	}
	return &typedColumnAdapterPart{Options: opts, Columns: columns, Part: part, Dictionary: dictionaries}, nil
}

func applyTypedColumnAdapterStoredDefinitions(columns []typedColumnAdapterColumn, part *typedcolumn.ColumnPart) {
	if part == nil {
		return
	}
	for i := range columns {
		got, ok := part.Columns[columns[i].Definition.Name]
		if !ok {
			continue
		}
		columns[i].Definition = got.Definition
	}
}

func typedColumnAdapterValidateStoredDefinition(field TypedStorageField, want typedcolumn.ColumnDefinition, got typedcolumn.ColumnDefinition, context string) error {
	if got.Name != want.Name || got.Type != want.Type || got.FixedWidthElements != want.FixedWidthElements || got.BitsPerElement != want.BitsPerElement || (typedColumnAdapterRequiresExactStoredEncoding(field) && got.Encoding != want.Encoding) {
		return fmt.Errorf("collections: %s %q schema mismatch: got {type=%s encoding=%s compression=%s fixed_width_elements=%d bits_per_element=%d}; want {type=%s encoding=%s compression=%s fixed_width_elements=%d bits_per_element=%d}; details: fixed_width_elements=%d want %d bits_per_element=%d want %d", context, want.Name, got.Type, got.Encoding, got.Compression, got.FixedWidthElements, got.BitsPerElement, want.Type, want.Encoding, want.Compression, want.FixedWidthElements, want.BitsPerElement, got.FixedWidthElements, want.FixedWidthElements, got.BitsPerElement, want.BitsPerElement)
	}
	return nil
}

func typedColumnAdapterRequiresExactStoredEncoding(field TypedStorageField) bool {
	if field.ValueType == ColumnStoreValueInt64 && !field.Nullable && field.FixedWidthEncoding == ColumnFixedWidthEncodingDefault {
		return false
	}
	return true
}

func validateTypedColumnAdapterImageSchema(part *typedcolumn.ColumnPart, columns []typedColumnAdapterColumn, schemaVersion uint32) error {
	if part == nil {
		return errors.New("collections: typed-column adapter nil image part")
	}
	if schemaVersion != 0 && part.Descriptor.SchemaVersion != schemaVersion {
		return fmt.Errorf("collections: typed-column adapter image schema_version=%d want %d", part.Descriptor.SchemaVersion, schemaVersion)
	}
	primary, ok := part.Columns[typedColumnAdapterPrimaryIDColumn]
	if !ok {
		return fmt.Errorf("collections: typed-column adapter image missing primary-id column %q", typedColumnAdapterPrimaryIDColumn)
	}
	if err := validateTypedColumnProductionPrimaryColumnLayout(primary); err != nil {
		return fmt.Errorf("collections: typed-column adapter image primary-id validation failed: %w", err)
	}
	expected := make(map[string]struct{}, len(columns)+1)
	expected[typedColumnAdapterPrimaryIDColumn] = struct{}{}
	for _, column := range columns {
		expected[column.Definition.Name] = struct{}{}
	}
	for name := range part.Columns {
		if _, ok := expected[name]; !ok {
			return fmt.Errorf("collections: typed-column adapter image unexpected column %q", name)
		}
	}
	if len(part.Columns) != len(expected) {
		return fmt.Errorf("collections: typed-column adapter image column count=%d want %d", len(part.Columns), len(expected))
	}
	for _, column := range columns {
		got, ok := part.Columns[column.Definition.Name]
		if !ok {
			return fmt.Errorf("collections: typed-column adapter image missing column %q", column.Definition.Name)
		}
		if err := typedColumnAdapterValidateStoredDefinition(column.Field, column.Definition, got.Definition, "typed-column adapter image column"); err != nil {
			return err
		}
		if err := validateTypedColumnProductionPartColumnLayout(column.Field, got); err != nil {
			return fmt.Errorf("collections: typed-column adapter image column %q layout validation failed: %w", column.Definition.Name, err)
		}
	}
	return nil
}

func (p *typedColumnAdapterPart) buildImage() (typedcolumn.ColumnPartImage, error) {
	if p == nil || p.Part == nil {
		return typedcolumn.ColumnPartImage{}, errors.New("collections: nil typed-column adapter part")
	}
	// The adapter primary-id column is an internal row locator, not a declared
	// ColumnStoreValueInt64 field. Leave it out of direct-view certification so
	// fallback-only declared typed-column parts do not publish an internal
	// direct-view claim that the segment writer intentionally does not align for.
	logicalTypes := make(map[string]string, len(p.Columns))
	for _, column := range p.Columns {
		if logical, ok := columnStoreSemanticLogicalType(column.Field.ValueType); ok {
			logicalTypes[column.Definition.Name] = string(logical)
		}
	}
	imageOpts := typedcolumn.ColumnPartImageOptions{Dictionaries: p.Dictionary, LayoutLogicalTypes: logicalTypes}
	if p.Options.SectionCompressionSet {
		imageOpts.SectionCompression = p.Options.SectionCompression
	}
	if p.Options.LocatorSectionCompressionSet {
		imageOpts.RowLocatorSectionCompression = p.Options.LocatorSectionCompression
		imageOpts.RowLocatorSectionCompressionSet = true
	}
	if p.Options.DictionarySectionCompressionSet {
		imageOpts.DictionarySectionCompression = p.Options.DictionarySectionCompression
		imageOpts.DictionarySectionCompressionSet = true
	}
	if p.Options.PruningSectionCompressionSet {
		imageOpts.PruningMetadataSectionCompression = p.Options.PruningSectionCompression
		imageOpts.PruningMetadataSectionCompressionSet = true
	}
	return typedcolumn.BuildColumnPartImage(p.Part, imageOpts)
}

func decodeTypedColumnPhysicalQuerySortedGroupedDistinctPart(plan columnTypedColumnPhysicalQueryPlan, schemaHash uint64, typedRef, physical columnManifestAssetRefForScan, raw []byte, includePhysicalRows bool) (columnTypedColumnPhysicalQueryPart, error) {
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	adapterPart, err := typedColumnAdapterPartFromImageWithoutRowLocators(typedColumnAdapterOptions{Fields: plan.Fields, SchemaVersion: uint32(schemaHash)}, image)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	summary := typedColumnAdapterImageSummary{PartID: image.PartID, Rows: image.Rows, Sections: len(image.Sections), SectionBytes: typedColumnPhysicalQueryImageSectionBytes(image), SortKey: columnSortKeysFromTypedColumnSortKeys(adapterPart.Part.Descriptor.SortKey)}
	if summary.PartID != typedRef.Ref.PartID || summary.Rows != typedRef.Rows {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("typed_column_part image/ref mismatch image_part=%d ref_part=%d image_rows=%d manifest_rows=%d", summary.PartID, typedRef.Ref.PartID, summary.Rows, typedRef.Rows)
	}
	if physical.Rows != 0 && summary.Rows != physical.Rows {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("typed_column_part rows=%d do not match physical rows=%d", summary.Rows, physical.Rows)
	}
	if err := validateTypedColumnPhysicalQuerySortMetadata(plan.SortKey, typedRef.SortKey, summary.SortKey); err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	pruned, err := plan.SortKeyPrefix.prunePartRows(adapterPart)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	selectedRows := pruned.Rows
	if pruned.AllRows {
		selectedRows = nil
	} else if selectedRows == nil {
		selectedRows = []int{}
	}
	selectedRowCount := len(selectedRows)
	if selectedRows == nil {
		selectedRowCount = summary.Rows
	}

	projection, err := typedColumnSortedGroupedDistinctProjection(adapterPart, plan)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	scan, err := typedColumnScanSortedGroupedDistinctColumns(adapterPart, projection, selectedRows)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}

	group, err := typedColumnSortedGroupedDistinctCodeColumn(adapterPart, scan, plan.GroupColumn, selectedRowCount)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	distinct, err := typedColumnSortedGroupedDistinctCodeColumn(adapterPart, scan, plan.DistinctColumn, selectedRowCount)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	predicates := make([]columnTypedColumnSortedGroupedDistinctPredicate, len(plan.PredicateSpecs))
	for i, spec := range plan.PredicateSpecs {
		predicate, err := typedColumnSortedGroupedDistinctPredicate(adapterPart, scan, spec, selectedRowCount)
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, err
		}
		predicates[i] = predicate
	}

	physicalRowIndexes := []int(nil)
	if includePhysicalRows {
		var primaryDiag columnTypedColumnPhysicalRowIndexDiagnostics
		physicalRowIndexes, primaryDiag, err = typedColumnPhysicalQueryPhysicalRows(adapterPart, selectedRows, selectedRowCount)
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, err
		}
		if primaryDiag.GranulesDecoded > scan.Diagnostics.GranulesDecoded {
			scan.Diagnostics.GranulesDecoded = primaryDiag.GranulesDecoded
		}
		scan.Diagnostics.BlocksDecoded += primaryDiag.BlocksDecoded
		scan.Diagnostics.BytesDecoded += primaryDiag.BytesDecoded
	}

	return columnTypedColumnPhysicalQueryPart{
		Ref:                       typedRef,
		PhysicalRef:               physical,
		RowIndexes:                selectedRows,
		PhysicalRowIndexes:        physicalRowIndexes,
		Rows:                      summary.Rows,
		Bytes:                     int64(len(raw)),
		Sections:                  summary.Sections,
		SectionBytes:              summary.SectionBytes,
		GranulesConsidered:        pruned.Considered,
		GranulesDecoded:           scan.Diagnostics.GranulesDecoded,
		GranulesSkipped:           pruned.Skips,
		DecodedBlocks:             scan.Diagnostics.BlocksDecoded,
		DecodedPayloadBytes:       uint64(scan.Diagnostics.BytesDecoded),
		SortKeyMarkChecks:         pruned.Checks,
		SortKeyMarkMatches:        pruned.Matches,
		SortKeyMarkSkips:          pruned.Skips,
		SortKeyMarkFallbackReason: pruned.FallbackReason,
		SortedGroupedDistinct: &columnTypedColumnSortedGroupedDistinctPart{
			Rows:         selectedRowCount,
			RowIndexes:   selectedRows,
			PhysicalRows: physicalRowIndexes,
			Group:        group,
			Distinct:     distinct,
			Predicates:   predicates,
		},
	}, nil
}

func typedColumnSortedGroupedDistinctProjection(part *typedColumnAdapterPart, plan columnTypedColumnPhysicalQueryPlan) ([]string, error) {
	seen := make(map[string]struct{}, 2+len(plan.PredicateSpecs))
	projection := make([]string, 0, 2+len(plan.PredicateSpecs))
	add := func(name string) error {
		column, ok := part.columnByName(name)
		if !ok {
			return fmt.Errorf("collections: sorted grouped-distinct missing typed-column column %q", name)
		}
		if _, exists := seen[column.Definition.Name]; exists {
			return nil
		}
		seen[column.Definition.Name] = struct{}{}
		projection = append(projection, column.Definition.Name)
		return nil
	}
	if err := add(plan.GroupColumn); err != nil {
		return nil, err
	}
	if err := add(plan.DistinctColumn); err != nil {
		return nil, err
	}
	for _, spec := range plan.PredicateSpecs {
		if err := add(spec.column); err != nil {
			return nil, err
		}
	}
	return projection, nil
}

func typedColumnScanSortedGroupedDistinctColumns(part *typedColumnAdapterPart, projection []string, selectedRows []int) (typedcolumn.ProjectedScanResult, error) {
	if selectedRows != nil && len(selectedRows) == 0 {
		columns := make(map[string][]int64, len(projection))
		for _, name := range projection {
			columns[name] = []int64{}
		}
		return typedcolumn.ProjectedScanResult{Columns: columns, Diagnostics: typedcolumn.PartScanDiagnostics{RowsScanned: 0, ColumnsProjected: len(projection), GranulesConsidered: len(part.Part.Descriptor.Granules)}}, nil
	}
	if selectedRows == nil {
		return part.Part.NewScanner().ScanProjected(projection)
	}
	return part.Part.NewScanner().ScanProjectedRows(projection, selectedRows)
}

func typedColumnSortedGroupedDistinctCodeColumn(part *typedColumnAdapterPart, scan typedcolumn.ProjectedScanResult, columnName string, rows int) (columnTypedColumnSortedGroupedDistinctCodeColumn, error) {
	column, valuesByCode, codes, err := typedColumnSortedGroupedDistinctScannedColumn(part, scan, columnName, rows)
	if err != nil {
		return columnTypedColumnSortedGroupedDistinctCodeColumn{}, err
	}
	if column.Field.ValueType != ColumnStoreValueString || column.Definition.Type != typedcolumn.ColumnTypeLowCardinalityCode || column.Definition.Encoding != typedcolumn.EncodingLowCardinalityUint32 {
		return columnTypedColumnSortedGroupedDistinctCodeColumn{}, fmt.Errorf("%w: sorted grouped-distinct column %q is not a non-null dictionary string", ErrColumnQueryPlanUnsupported, columnName)
	}
	return columnTypedColumnSortedGroupedDistinctCodeColumn{Codes: codes, Dictionary: valuesByCode}, nil
}

func typedColumnSortedGroupedDistinctPredicate(part *typedColumnAdapterPart, scan typedcolumn.ProjectedScanResult, spec columnPhysicalQueryPredicateSpec, rows int) (columnTypedColumnSortedGroupedDistinctPredicate, error) {
	column, valuesByCode, codes, err := typedColumnSortedGroupedDistinctScannedColumn(part, scan, spec.column, rows)
	if err != nil {
		return columnTypedColumnSortedGroupedDistinctPredicate{}, err
	}
	allowed := make([]uint64, (len(valuesByCode)+63)/64)
	matched := 0
	for _, value := range spec.values {
		code, ok := column.Dictionary[value]
		if !ok {
			continue
		}
		word := int(code / 64)
		bit := uint(code % 64)
		mask := uint64(1) << bit
		if allowed[word]&mask == 0 {
			allowed[word] |= mask
			matched++
		}
	}
	return columnTypedColumnSortedGroupedDistinctPredicate{Codes: codes, Allowed: allowed, RejectsAll: matched == 0}, nil
}

func typedColumnSortedGroupedDistinctScannedColumn(part *typedColumnAdapterPart, scan typedcolumn.ProjectedScanResult, columnName string, rows int) (typedColumnAdapterColumn, []string, []int64, error) {
	column, ok := part.columnByName(columnName)
	if !ok {
		return typedColumnAdapterColumn{}, nil, nil, fmt.Errorf("collections: sorted grouped-distinct missing typed-column column %q", columnName)
	}
	if column.Field.Nullable {
		return typedColumnAdapterColumn{}, nil, nil, fmt.Errorf("%w: sorted grouped-distinct column %q is nullable", ErrColumnQueryPlanUnsupported, columnName)
	}
	if column.Field.ValueType != ColumnStoreValueString || column.Definition.Type != typedcolumn.ColumnTypeLowCardinalityCode || column.Definition.Encoding != typedcolumn.EncodingLowCardinalityUint32 {
		return typedColumnAdapterColumn{}, nil, nil, fmt.Errorf("%w: sorted grouped-distinct column %q is not a dictionary string", ErrColumnQueryPlanUnsupported, columnName)
	}
	valuesByCode, err := typedColumnSortedGroupedDistinctDictionaryValuesByCode(column)
	if err != nil {
		return typedColumnAdapterColumn{}, nil, nil, err
	}
	codes, ok := scan.Columns[column.Definition.Name]
	if !ok {
		return typedColumnAdapterColumn{}, nil, nil, fmt.Errorf("collections: sorted grouped-distinct scan missing column %q", column.Definition.Name)
	}
	if len(codes) != rows {
		return typedColumnAdapterColumn{}, nil, nil, fmt.Errorf("collections: sorted grouped-distinct column %q rows=%d want %d", columnName, len(codes), rows)
	}
	for row, code := range codes {
		if code < 0 || code >= int64(len(valuesByCode)) {
			return typedColumnAdapterColumn{}, nil, nil, fmt.Errorf("collections: sorted grouped-distinct column %q row=%d code=%d outside cardinality=%d", columnName, row, code, len(valuesByCode))
		}
	}
	return column, valuesByCode, codes, nil
}

func typedColumnSortedGroupedDistinctDictionaryValuesByCode(column typedColumnAdapterColumn) ([]string, error) {
	if err := validateTypedColumnAdapterStringDictionary(column, column.Definition.Cardinality, column.Dictionary); err != nil {
		return nil, err
	}
	if uint64(int(column.Definition.Cardinality)) != uint64(column.Definition.Cardinality) {
		return nil, fmt.Errorf("collections: sorted grouped-distinct dictionary cardinality=%d exceeds host int for column %q", column.Definition.Cardinality, column.Definition.Name)
	}
	valuesByCode := make([]string, int(column.Definition.Cardinality))
	for value, code := range column.Dictionary {
		valuesByCode[code] = value
	}
	return valuesByCode, nil
}

func columnTypedColumnPhysicalQueryAdapterDictionaryModes(plan columnTypedColumnPhysicalQueryPlan, valuesByCodeColumns []string, predicateSpecs []columnPhysicalQueryPredicateSpec) map[string]typedColumnAdapterDictionaryMode {
	modes := make(map[string]typedColumnAdapterDictionaryMode, len(valuesByCodeColumns)+len(predicateSpecs))
	add := func(column string, mode typedColumnAdapterDictionaryMode) {
		adapterColumn, ok, err := typedColumnStringPredicateAdapterColumn(plan.Fields, column)
		if err != nil || !ok {
			return
		}
		name := adapterColumn.Definition.Name
		current := modes[name]
		current.Forward = current.Forward || mode.Forward
		current.Reverse = current.Reverse || mode.Reverse
		current.ValuesByCode = current.ValuesByCode || mode.ValuesByCode
		modes[name] = current
	}
	for _, column := range valuesByCodeColumns {
		add(column, typedColumnAdapterDictionaryMode{ValuesByCode: true})
	}
	for _, spec := range predicateSpecs {
		add(spec.column, typedColumnAdapterDictionaryMode{Forward: true})
	}
	return modes
}

// decodeTypedColumnPhysicalQueryDenseGroupCountPart prepares the q1 typed-column
// section fast path from the adapter seam so production query routing does not
// import the typedcolumn data plane directly.
func decodeTypedColumnPhysicalQueryDenseGroupCountPart(plan columnTypedColumnPhysicalQueryPlan, schemaHash uint64, typedRef, physical columnManifestAssetRefForScan, raw []byte, decodeRows bool, prepareDiagnostics *columnTypedColumnPhysicalQueryPrepareDiagnostics) (columnTypedColumnPhysicalQueryPart, error) {
	var phaseStart time.Time
	if prepareDiagnostics != nil {
		phaseStart = time.Now()
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if prepareDiagnostics != nil {
		prepareDiagnostics.ReadImageNanos += time.Since(phaseStart).Nanoseconds()
	}
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	adapterPart, err := typedColumnAdapterPartFromImageWithoutRowLocatorsWithDiagnostics(typedColumnAdapterOptions{
		Fields:          plan.Fields,
		SchemaVersion:   uint32(schemaHash),
		DictionaryModes: columnTypedColumnPhysicalQueryAdapterDictionaryModes(plan, []string{plan.GroupColumn}, nil),
	}, image, prepareDiagnostics)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	summary := typedColumnAdapterImageSummary{PartID: image.PartID, Rows: image.Rows, Sections: len(image.Sections), SectionBytes: typedColumnPhysicalQueryImageSectionBytes(image), SortKey: columnSortKeysFromTypedColumnSortKeys(adapterPart.Part.Descriptor.SortKey)}
	if summary.PartID != typedRef.Ref.PartID || summary.Rows != typedRef.Rows {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("typed_column_part image/ref mismatch image_part=%d ref_part=%d image_rows=%d manifest_rows=%d", summary.PartID, typedRef.Ref.PartID, summary.Rows, typedRef.Rows)
	}
	if physical.Rows != 0 && summary.Rows != physical.Rows {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("typed_column_part rows=%d do not match physical rows=%d", summary.Rows, physical.Rows)
	}
	if err := validateTypedColumnPhysicalQuerySortMetadata(plan.SortKey, typedRef.SortKey, summary.SortKey); err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	if plan.SortKeyPrefix.Planned {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("%w: dense typed-column group-count does not support sort-key row pruning", ErrColumnQueryPlanUnsupported)
	}
	if prepareDiagnostics != nil {
		phaseStart = time.Now()
	}
	var (
		group        columnTypedColumnDenseGroupCountPart
		decodedBytes uint64
		blocks       int
	)
	if decodeRows {
		codes, codeDecodedBytes, codeBlocks, err := typedColumnDenseGroupCountDistinctCodeColumn(adapterPart, plan.Fields, plan.GroupColumn, summary.Rows, "group-count group")
		if err != nil {
			if prepareDiagnostics != nil {
				prepareDiagnostics.DenseGroupNanos += time.Since(phaseStart).Nanoseconds()
			}
			return columnTypedColumnPhysicalQueryPart{}, err
		}
		decodedBytes = codeDecodedBytes
		blocks = codeBlocks
		group = columnTypedColumnDenseGroupCountPart{Cardinality: len(codes.Dictionary), Dictionary: codes.Dictionary, Codes: codes.Codes, Valid: codes.Valid, Rows: summary.Rows}
	} else {
		var err error
		group, decodedBytes, blocks, err = typedColumnDenseGroupCountCodeColumn(adapterPart, plan.Fields, plan.GroupColumn, summary.Rows, "group-count group")
		if err != nil {
			if prepareDiagnostics != nil {
				prepareDiagnostics.DenseGroupNanos += time.Since(phaseStart).Nanoseconds()
			}
			return columnTypedColumnPhysicalQueryPart{}, err
		}
	}
	if prepareDiagnostics != nil {
		prepareDiagnostics.DenseGroupNanos += time.Since(phaseStart).Nanoseconds()
	}
	return columnTypedColumnPhysicalQueryPart{
		Ref:                 typedRef,
		PhysicalRef:         physical,
		Rows:                summary.Rows,
		Bytes:               int64(len(raw)),
		Sections:            summary.Sections,
		SectionBytes:        summary.SectionBytes,
		GranulesConsidered:  blocks,
		GranulesDecoded:     blocks,
		DecodedBlocks:       blocks,
		DecodedPayloadBytes: decodedBytes,
		DenseGroupCount:     &group,
	}, nil
}

// decodeTypedColumnPhysicalQueryDenseGroupHourCountPart prepares the q3
// typed-column section fast path from the adapter seam so production query
// routing does not import the typedcolumn data plane directly.
func decodeTypedColumnPhysicalQueryDenseGroupHourCountPart(plan columnTypedColumnPhysicalQueryPlan, schemaHash uint64, typedRef, physical columnManifestAssetRefForScan, raw []byte) (columnTypedColumnPhysicalQueryPart, error) {
	spanPlan, groupPredicate, hasGroupPredicate := columnTypedColumnPhysicalQueryDenseGroupHourSpanPlan(plan)
	part, err := decodeTypedColumnPhysicalQueryDenseInt64SpanPart(spanPlan, schemaHash, typedRef, physical, raw, false)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	if part.DenseInt64Span == nil {
		return columnTypedColumnPhysicalQueryPart{}, errors.New("collections: dense typed-column group-hour missing decoded string/int64 payload")
	}
	predicates := part.DenseInt64Span.Predicates
	if hasGroupPredicate {
		predicate, err := densePredicateFromDictionaryCodes(part.DenseInt64Span.GroupCodes, part.DenseInt64Span.GroupValid, part.DenseInt64Span.Dictionary, part.DenseInt64Span.DictionaryByCode, part.DenseInt64Span.Cardinality, groupPredicate)
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, err
		}
		predicates = append(predicates, predicate)
	}
	part.DenseGroupHourCount = &columnTypedColumnDenseGroupHourCountPart{
		Cardinality:      part.DenseInt64Span.Cardinality,
		Dictionary:       part.DenseInt64Span.Dictionary,
		DictionaryByCode: part.DenseInt64Span.DictionaryByCode,
		GroupCodes:       part.DenseInt64Span.GroupCodes,
		GroupValid:       part.DenseInt64Span.GroupValid,
		Values:           part.DenseInt64Span.Values,
		Predicates:       predicates,
	}
	part.DenseInt64Span = nil
	return part, nil
}

func decodeTypedColumnPhysicalQueryDensePartFromRanges(plan columnTypedColumnPhysicalQueryPlan, req ColumnPhysicalQueryRequest, schemaHash uint64, typedRef, physical columnManifestAssetRefForScan, readCache *columnPhysicalAssetReadCache, allowDenseGroupCountDistinct bool, includePhysicalRows bool, opts columnTypedColumnPhysicalQueryPartDecodeOptions, prepareDiagnostics *columnTypedColumnPhysicalQueryPrepareDiagnostics) (columnTypedColumnPhysicalQueryPart, bool, error) {
	if !typedColumnStringUseTargetedRanges(req.ColumnAssetReadIntegrity) {
		return columnTypedColumnPhysicalQueryPart{}, false, nil
	}
	if includePhysicalRows && columnTypedColumnPhysicalQueryUseTimeOrderTopK(plan, req) {
		return columnTypedColumnPhysicalQueryPart{}, false, nil
	}
	if opts.decodeDenseGroupCountRows && columnTypedColumnPhysicalQueryUseDenseGroupCount(plan, req) {
		return columnTypedColumnPhysicalQueryPart{}, false, nil
	}
	requests, ok, err := columnTypedColumnPhysicalQueryDensePreparedRequests(plan, req, allowDenseGroupCountDistinct)
	if err != nil || !ok {
		return columnTypedColumnPhysicalQueryPart{}, ok, err
	}
	adapterDictionaryModes, err := typedColumnPreparedAdapterDictionaryModesFromRequests(requests)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, true, err
	}
	reader := &columnTypedColumnPhysicalRangePartReader{
		readCache:          readCache,
		ref:                typedRef.Ref,
		integrity:          req.ColumnAssetReadIntegrity,
		measureDiagnostics: prepareDiagnostics != nil,
	}
	if err := reader.ensureFullAssetValidated(); err != nil {
		return columnTypedColumnPhysicalQueryPart{}, true, err
	}
	prepared, diag, err := typedColumnPreparePartStateFromRangesWithOptions(typedRef.Ref, physical.Ref, typedRef.Rows, physical.Rows, plan.Fields, schemaHash, requests, reader.readRange, nil, typedColumnPreparePartStateOptions{
		CoalescePreparedMetadataReads: true,
	})
	if prepareDiagnostics != nil {
		prepareDiagnostics.ReadImageNanos += diag.ReadImageNanos
		prepareDiagnostics.StateBuildNanos += diag.StateBuildNanos
		prepareDiagnostics.DictionaryNanos += diag.DictionaryNanos
		prepareDiagnostics.PruningNanos += diag.PruningNanos
		prepareDiagnostics.SortKeyNanos += diag.SortKeyNanos
		prepareDiagnostics.StatsNanos += diag.StatsNanos
	}
	if err != nil {
		if prepareDiagnostics != nil {
			prepareDiagnostics.RangeReadNanos += reader.readNanos
			prepareDiagnostics.RangeReadBytes += reader.bytesRead
		}
		return columnTypedColumnPhysicalQueryPart{}, true, err
	}
	if prepared == nil {
		if prepareDiagnostics != nil {
			prepareDiagnostics.RangeReadNanos += reader.readNanos
			prepareDiagnostics.RangeReadBytes += reader.bytesRead
		}
		return columnTypedColumnPhysicalQueryPart{}, false, nil
	}
	defer prepared.close()
	if diag.Fallback {
		if prepareDiagnostics != nil {
			prepareDiagnostics.RangeReadNanos += reader.readNanos
			prepareDiagnostics.RangeReadBytes += reader.bytesRead
		}
		return columnTypedColumnPhysicalQueryPart{}, false, nil
	}
	summary, err := typedColumnPhysicalQueryPreparedSummary(prepared, plan, typedRef, physical)
	if err != nil {
		if prepareDiagnostics != nil {
			prepareDiagnostics.RangeReadNanos += reader.readNanos
			prepareDiagnostics.RangeReadBytes += reader.bytesRead
		}
		return columnTypedColumnPhysicalQueryPart{}, true, err
	}
	if columnTypedColumnPhysicalQueryUseTimeOrderTopK(plan, req) {
		var adapterStart time.Time
		if prepareDiagnostics != nil {
			adapterStart = time.Now()
		}
		adapterPart, err := typedColumnPhysicalQueryPreparedAdapterPartWithOptions(prepared, plan.Fields, reader.readRange, typedColumnPreparedAdapterPartOptions{
			LazyPayloads:    !opts.eagerTimeOrderTopKPayloads,
			DictionaryModes: adapterDictionaryModes,
		})
		if prepareDiagnostics != nil {
			prepareDiagnostics.AdapterNanos += time.Since(adapterStart).Nanoseconds()
		}
		if err != nil {
			if prepareDiagnostics != nil {
				prepareDiagnostics.RangeReadNanos += reader.readNanos
				prepareDiagnostics.RangeReadBytes += reader.bytesRead
			}
			return columnTypedColumnPhysicalQueryPart{}, true, err
		}
		var payloadLoader *columnTypedColumnTimeOrderTopKPayloadLoader
		if !opts.eagerTimeOrderTopKPayloads {
			payloadLoader = newColumnTypedColumnTimeOrderTopKPayloadLoader(reader.readRangeOwned, prepared)
		}
		part, err := decodeTypedColumnPhysicalQueryTimeOrderTopKPreparedPart(plan, summary, typedRef, physical, adapterPart, reader.bytesRead, payloadLoader)
		if prepareDiagnostics != nil {
			prepareDiagnostics.RangeReadNanos += reader.readNanos
			prepareDiagnostics.RangeReadBytes += reader.bytesRead
		}
		return part, true, err
	}
	if opts.compactDenseInt64SpanPredicateRows && !includePhysicalRows && columnTypedColumnPhysicalQueryUseDenseInt64Span(plan, req) && len(plan.PredicateSpecs) == 3 {
		var adapterStart time.Time
		if prepareDiagnostics != nil {
			adapterStart = time.Now()
		}
		adapterOpts := typedColumnPreparedAdapterPartOptions{
			LazyPayloads:    true,
			DictionaryModes: adapterDictionaryModes,
		}
		adapterPart, minimalAdapterOK, err := typedColumnPhysicalQueryDenseGroupValuePredicateMinimalPreparedAdapterPart(prepared, plan.Fields, plan, adapterOpts)
		if err == nil && !minimalAdapterOK {
			adapterPart, err = typedColumnPhysicalQueryPreparedAdapterPartWithOptions(prepared, plan.Fields, reader.readRange, adapterOpts)
		}
		if prepareDiagnostics != nil {
			prepareDiagnostics.AdapterNanos += time.Since(adapterStart).Nanoseconds()
		}
		if err != nil {
			if prepareDiagnostics != nil {
				prepareDiagnostics.RangeReadNanos += reader.readNanos
				prepareDiagnostics.RangeReadBytes += reader.bytesRead
			}
			return columnTypedColumnPhysicalQueryPart{}, true, err
		}
		part, ok, err := decodeTypedColumnPhysicalQueryDenseInt64SpanPredicateFirstPreparedPart(plan, summary, typedRef, physical, prepared, adapterPart, reader.readRange, func() int64 { return reader.bytesRead }, prepareDiagnostics)
		if err != nil || ok {
			if prepareDiagnostics != nil {
				prepareDiagnostics.RangeReadNanos += reader.readNanos
				prepareDiagnostics.RangeReadBytes += reader.bytesRead
			}
			return part, true, err
		}
	}
	if opts.compactDenseGroupHourPredicateRows && !includePhysicalRows && columnTypedColumnPhysicalQueryUseDenseGroupHourCount(plan, req) {
		var adapterStart time.Time
		if prepareDiagnostics != nil {
			adapterStart = time.Now()
		}
		adapterOpts := typedColumnPreparedAdapterPartOptions{
			LazyPayloads:    true,
			DictionaryModes: adapterDictionaryModes,
		}
		adapterPart, minimalAdapterOK, err := typedColumnPhysicalQueryDenseGroupValuePredicateMinimalPreparedAdapterPart(prepared, plan.Fields, plan, adapterOpts)
		if err == nil && !minimalAdapterOK {
			adapterPart, err = typedColumnPhysicalQueryPreparedAdapterPartWithOptions(prepared, plan.Fields, reader.readRange, adapterOpts)
		}
		if prepareDiagnostics != nil {
			prepareDiagnostics.AdapterNanos += time.Since(adapterStart).Nanoseconds()
		}
		if err != nil {
			if prepareDiagnostics != nil {
				prepareDiagnostics.RangeReadNanos += reader.readNanos
				prepareDiagnostics.RangeReadBytes += reader.bytesRead
			}
			return columnTypedColumnPhysicalQueryPart{}, true, err
		}
		part, ok, err := decodeTypedColumnPhysicalQueryDenseGroupHourCountPredicateFirstPreparedPart(plan, summary, typedRef, physical, prepared, adapterPart, reader.readRange, func() int64 { return reader.bytesRead }, prepareDiagnostics)
		if err != nil || ok {
			if prepareDiagnostics != nil {
				prepareDiagnostics.RangeReadNanos += reader.readNanos
				prepareDiagnostics.RangeReadBytes += reader.bytesRead
			}
			return part, true, err
		}
	}
	var adapterStart time.Time
	if prepareDiagnostics != nil {
		adapterStart = time.Now()
	}
	adapterPart, err := typedColumnPhysicalQueryPreparedAdapterPartWithOptions(prepared, plan.Fields, reader.readRange, typedColumnPreparedAdapterPartOptions{DictionaryModes: adapterDictionaryModes})
	if prepareDiagnostics != nil {
		prepareDiagnostics.AdapterNanos += time.Since(adapterStart).Nanoseconds()
	}
	if err != nil {
		if prepareDiagnostics != nil {
			prepareDiagnostics.RangeReadNanos += reader.readNanos
			prepareDiagnostics.RangeReadBytes += reader.bytesRead
		}
		return columnTypedColumnPhysicalQueryPart{}, true, err
	}
	switch {
	case columnTypedColumnPhysicalQueryUseDenseGroupCount(plan, req):
		part, err := decodeTypedColumnPhysicalQueryDenseGroupCountPreparedPart(plan, summary, typedRef, physical, adapterPart, reader.bytesRead, prepareDiagnostics)
		if prepareDiagnostics != nil {
			prepareDiagnostics.RangeReadNanos += reader.readNanos
			prepareDiagnostics.RangeReadBytes += reader.bytesRead
		}
		return part, true, err
	case columnTypedColumnPhysicalQueryUseDenseGroupCountDistinct(plan, req):
		part, err := decodeTypedColumnPhysicalQueryDenseGroupCountDistinctPreparedPart(plan, summary, typedRef, physical, adapterPart, reader.bytesRead)
		if prepareDiagnostics != nil {
			prepareDiagnostics.RangeReadNanos += reader.readNanos
			prepareDiagnostics.RangeReadBytes += reader.bytesRead
		}
		return part, true, err
	case columnTypedColumnPhysicalQueryUseDenseGroupHourCount(plan, req):
		part, err := decodeTypedColumnPhysicalQueryDenseGroupHourCountPreparedPart(plan, summary, typedRef, physical, adapterPart, reader.bytesRead, prepareDiagnostics)
		if prepareDiagnostics != nil {
			prepareDiagnostics.RangeReadNanos += reader.readNanos
			prepareDiagnostics.RangeReadBytes += reader.bytesRead
		}
		return part, true, err
	case columnTypedColumnPhysicalQueryUseDenseInt64Span(plan, req):
		part, err := decodeTypedColumnPhysicalQueryDenseInt64SpanPreparedPart(plan, summary, typedRef, physical, adapterPart, reader.bytesRead, true, prepareDiagnostics)
		if prepareDiagnostics != nil {
			prepareDiagnostics.RangeReadNanos += reader.readNanos
			prepareDiagnostics.RangeReadBytes += reader.bytesRead
		}
		return part, true, err
	default:
		if prepareDiagnostics != nil {
			prepareDiagnostics.RangeReadNanos += reader.readNanos
			prepareDiagnostics.RangeReadBytes += reader.bytesRead
		}
		return columnTypedColumnPhysicalQueryPart{}, false, nil
	}
}

type columnTypedColumnPhysicalRangePartReader struct {
	readCache          *columnPhysicalAssetReadCache
	ref                ColumnAssetRef
	integrity          ColumnAssetReadIntegrity
	bytesRead          int64
	readNanos          int64
	measureDiagnostics bool
}

func (r *columnTypedColumnPhysicalRangePartReader) ensureFullAssetValidated() error {
	if r == nil || r.readCache == nil {
		return errors.New("collections: typed-column physical range reader missing read cache")
	}
	if r.integrity != ColumnAssetReadIntegrityCachedVerify {
		return nil
	}
	n, err := r.readCache.validateFullRef(r.ref)
	if err != nil {
		return err
	}
	r.bytesRead += int64(n)
	return nil
}

func (r *columnTypedColumnPhysicalRangePartReader) readRange(offset int, length int, _ bool) ([]byte, error) {
	if r == nil || r.readCache == nil {
		return nil, errors.New("collections: typed-column physical range reader missing read cache")
	}
	if offset < 0 || length <= 0 {
		return nil, fmt.Errorf("collections: typed-column physical range offset=%d length=%d is invalid", offset, length)
	}
	var readStart time.Time
	if r.measureDiagnostics {
		readStart = time.Now()
	}
	raw, err := r.readCache.readRange(r.ref, int64(offset), int64(length))
	if r.measureDiagnostics {
		r.readNanos += time.Since(readStart).Nanoseconds()
	}
	if err != nil {
		return nil, err
	}
	r.bytesRead += int64(len(raw))
	return raw, nil
}

func (r *columnTypedColumnPhysicalRangePartReader) readRangeOwned(offset int, length int, section bool) ([]byte, error) {
	if r == nil || r.readCache == nil {
		return nil, errors.New("collections: typed-column physical range reader missing read cache")
	}
	savedReturnViews := r.readCache.returnViews
	r.readCache.returnViews = false
	raw, err := r.readRange(offset, length, section)
	r.readCache.returnViews = savedReturnViews
	return raw, err
}

func columnTypedColumnPhysicalQueryDensePreparedRequests(plan columnTypedColumnPhysicalQueryPlan, req ColumnPhysicalQueryRequest, allowDenseGroupCountDistinct bool) ([]typedColumnPreparedColumnRequest, bool, error) {
	if columnTypedColumnPhysicalQueryUseSortedGroupedDistinct(plan, req) {
		return nil, false, nil
	}
	requests := make([]typedColumnPreparedColumnRequest, 0, 4+len(plan.PredicateSpecs))
	addString := func(column string, role typedcolumn.ColumnExecutionRole, op columnsemantics.Operation, valuesByCode bool) error {
		adapterColumn, ok, err := typedColumnStringPredicateAdapterColumn(plan.Fields, column)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("collections: dense typed-column prepared range column %q is not owned by typed_column_part", column)
		}
		if adapterColumn.Field.Nullable {
			op = columnsemantics.OpCountRows
		}
		requests = append(requests, typedColumnPreparedColumnRequest{
			Field:                  adapterColumn.Field,
			Role:                   role,
			Operation:              op,
			IncludeDictionaries:    true,
			DictionaryValuesByCode: valuesByCode,
		})
		return nil
	}
	addInt64 := func(column string) error {
		adapterColumn, ok, err := typedColumnInt64PredicateAdapterColumn(plan.Fields, column)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("collections: dense typed-column prepared range int64 column %q is not owned by typed_column_part", column)
		}
		requests = append(requests, typedColumnPreparedColumnRequest{
			Field:     adapterColumn.Field,
			Role:      typedcolumn.ColumnRoleMeasure,
			Operation: columnsemantics.OpMin,
		})
		return nil
	}
	addPredicates := func(specs []columnPhysicalQueryPredicateSpec) error {
		for _, spec := range specs {
			if err := addString(spec.column, typedcolumn.ColumnRolePredicate, columnTypedColumnPhysicalPredicatePreparedOperation(spec), false); err != nil {
				return err
			}
		}
		return nil
	}
	switch {
	case columnTypedColumnPhysicalQueryUseDenseGroupCount(plan, req):
		if err := addString(plan.GroupColumn, typedcolumn.ColumnRoleProjection, columnsemantics.OpDictionaryCount, true); err != nil {
			return nil, true, err
		}
	case columnTypedColumnPhysicalQueryUseDenseGroupCountDistinct(plan, req):
		if !allowDenseGroupCountDistinct {
			return nil, false, nil
		}
		if err := addString(plan.GroupColumn, typedcolumn.ColumnRoleProjection, columnsemantics.OpDictionaryGroupBy, true); err != nil {
			return nil, true, err
		}
		if err := addString(plan.DistinctColumn, typedcolumn.ColumnRoleMeasure, columnsemantics.OpDictionaryGroupBy, true); err != nil {
			return nil, true, err
		}
		if err := addPredicates(plan.PredicateSpecs); err != nil {
			return nil, true, err
		}
	case columnTypedColumnPhysicalQueryUseDenseGroupHourCount(plan, req):
		spanPlan, _, _ := columnTypedColumnPhysicalQueryDenseGroupHourSpanPlan(plan)
		if err := addString(plan.GroupColumn, typedcolumn.ColumnRoleProjection, columnsemantics.OpDictionaryGroupBy, true); err != nil {
			return nil, true, err
		}
		if err := addInt64(plan.ValueColumn); err != nil {
			return nil, true, err
		}
		if err := addPredicates(spanPlan.PredicateSpecs); err != nil {
			return nil, true, err
		}
	case columnTypedColumnPhysicalQueryUseDenseInt64Span(plan, req):
		if err := addString(plan.GroupColumn, typedcolumn.ColumnRoleProjection, columnsemantics.OpDictionaryGroupBy, true); err != nil {
			return nil, true, err
		}
		if err := addInt64(plan.ValueColumn); err != nil {
			return nil, true, err
		}
		if err := addPredicates(plan.PredicateSpecs); err != nil {
			return nil, true, err
		}
	case columnTypedColumnPhysicalQueryUseTimeOrderTopK(plan, req):
		if err := addString(plan.GroupColumn, typedcolumn.ColumnRoleProjection, columnsemantics.OpDictionaryGroupBy, false); err != nil {
			return nil, true, err
		}
		if err := addInt64(plan.ValueColumn); err != nil {
			return nil, true, err
		}
		if err := addPredicates(plan.PredicateSpecs); err != nil {
			return nil, true, err
		}
		if len(requests) != 0 {
			requests[0].IncludeSortKeyMetadata = true
			requests[0].IncludeSortKeyMarks = true
		}
	default:
		return nil, false, nil
	}
	return requests, true, nil
}

func columnTypedColumnPhysicalPredicatePreparedOperation(spec columnPhysicalQueryPredicateSpec) columnsemantics.Operation {
	switch columnPhysicalQueryPredicateKindOrDefault(spec.kind) {
	case ColumnPhysicalQueryPredicateInList:
		return columnsemantics.OpDictionaryInList
	default:
		return columnsemantics.OpDictionaryEquality
	}
}

func columnTypedColumnPhysicalQueryDenseGroupHourSpanPlan(plan columnTypedColumnPhysicalQueryPlan) (columnTypedColumnPhysicalQueryPlan, columnPhysicalQueryPredicateSpec, bool) {
	spanPlan := plan
	spanPlan.PredicateSpecs = make([]columnPhysicalQueryPredicateSpec, 0, len(plan.PredicateSpecs))
	var groupPredicate columnPhysicalQueryPredicateSpec
	hasGroupPredicate := false
	for _, spec := range plan.PredicateSpecs {
		if spec.column == plan.GroupColumn {
			groupPredicate = spec
			hasGroupPredicate = true
			continue
		}
		spanPlan.PredicateSpecs = append(spanPlan.PredicateSpecs, spec)
	}
	return spanPlan, groupPredicate, hasGroupPredicate
}

type typedColumnPreparedAdapterPartOptions struct {
	LazyPayloads    bool
	DictionaryModes map[string]typedColumnAdapterDictionaryMode
}

func typedColumnPreparedAdapterDictionaryModesFromRequests(requests []typedColumnPreparedColumnRequest) (map[string]typedColumnAdapterDictionaryMode, error) {
	requestModes, err := typedColumnPreparedDictionaryRequestModes(requests)
	if err != nil {
		return nil, err
	}
	if len(requestModes) == 0 {
		return nil, nil
	}
	modes := make(map[string]typedColumnAdapterDictionaryMode, len(requestModes))
	for name, mode := range requestModes {
		if name == typedColumnAdapterMetadataDictionary {
			continue
		}
		modes[name] = typedColumnAdapterDictionaryMode{
			Forward:      mode.Forward,
			Reverse:      mode.Reverse,
			ValuesByCode: mode.ValuesByCode,
		}
	}
	return modes, nil
}

func typedColumnPhysicalQueryPreparedAdapterPart(prepared *typedColumnPreparedPartState, fields []TypedStorageField, readRange typedColumnPreparedRangeReader) (*typedColumnAdapterPart, error) {
	return typedColumnPhysicalQueryPreparedAdapterPartWithOptions(prepared, fields, readRange, typedColumnPreparedAdapterPartOptions{})
}

func typedColumnPhysicalQueryPreparedAdapterPartWithOptions(prepared *typedColumnPreparedPartState, fields []TypedStorageField, readRange typedColumnPreparedRangeReader, opts typedColumnPreparedAdapterPartOptions) (*typedColumnAdapterPart, error) {
	if prepared == nil {
		return nil, errors.New("collections: dense typed-column prepared range missing part")
	}
	if readRange == nil && !opts.LazyPayloads {
		return nil, errors.New("collections: dense typed-column prepared range missing payload reader")
	}
	columns := make([]typedColumnAdapterColumn, 0, len(prepared.Columns))
	partColumns := make(map[string]typedcolumn.ColumnPartColumn, len(prepared.Columns))
	dictionaries := make(map[string]map[string]int64, len(prepared.Columns))
	for _, field := range fields {
		if field.Owner != TypedStorageOwnerColumnPart {
			continue
		}
		adapterColumn, err := typedColumnAdapterMapField(field)
		if err != nil {
			return nil, err
		}
		preparedColumn := prepared.Columns[adapterColumn.Definition.Name]
		if preparedColumn == nil {
			continue
		}
		adapterColumn.Definition = preparedColumn.Column.Definition
		if preparedColumn.Dictionaries != nil {
			adapterColumn.Dictionary = preparedColumn.Dictionaries
			dictionaries[adapterColumn.Definition.Name] = preparedColumn.Dictionaries
		}
		if preparedColumn.ReverseDictionaries != nil {
			adapterColumn.ReverseDictionary = preparedColumn.ReverseDictionaries
		}
		mode := typedColumnPreparedAdapterDictionaryModeForColumn(opts, adapterColumn.Definition.Name)
		if mode.ValuesByCode {
			if preparedColumn.DictionaryValuesByCode != nil {
				adapterColumn.DictionaryValuesByCode = preparedColumn.DictionaryValuesByCode
			} else if preparedColumn.Dictionaries != nil {
				valuesByCode, err := typedColumnAdapterDictionaryValuesByCodeFromForward(preparedColumn.Dictionaries, int(adapterColumn.Definition.Cardinality))
				if err != nil {
					return nil, err
				}
				adapterColumn.DictionaryValuesByCode = valuesByCode
			}
		}
		partColumn, err := typedColumnPreparedColumnWithOptionalPayloads(preparedColumn, readRange, !opts.LazyPayloads)
		if err != nil {
			return nil, err
		}
		columns = append(columns, adapterColumn)
		partColumns[adapterColumn.Definition.Name] = partColumn
	}
	descriptor := prepared.Descriptor
	if len(descriptor.SortKey) != 0 {
		descriptor.SortKey = append([]typedcolumn.SortKeyColumn(nil), descriptor.SortKey...)
	}
	marks := append([]typedcolumn.SortKeyMark(nil), prepared.Marks...)
	return &typedColumnAdapterPart{
		Options:    typedColumnAdapterOptions{Fields: fields, SchemaVersion: prepared.Descriptor.SchemaVersion},
		Columns:    columns,
		Part:       &typedcolumn.ColumnPart{Descriptor: descriptor, Columns: partColumns, Marks: marks},
		Dictionary: dictionaries,
	}, nil
}

func typedColumnPhysicalQueryDenseGroupValuePredicateMinimalPreparedAdapterPart(prepared *typedColumnPreparedPartState, fields []TypedStorageField, plan columnTypedColumnPhysicalQueryPlan, opts typedColumnPreparedAdapterPartOptions) (*typedColumnAdapterPart, bool, error) {
	if prepared == nil || len(plan.PredicateSpecs) != 3 {
		return nil, false, nil
	}
	adapterColumns, err := typedColumnAdapterColumnsForFields(fields)
	if err != nil {
		return nil, false, err
	}
	findColumn := func(column string) (typedColumnAdapterColumn, bool) {
		for _, adapterColumn := range adapterColumns {
			if adapterColumn.Field.Name == column || adapterColumn.Field.Path == column || adapterColumn.Definition.Name == column {
				return adapterColumn, true
			}
		}
		return typedColumnAdapterColumn{}, false
	}
	addColumn := func(adapterColumn typedColumnAdapterColumn, columns *[]typedColumnAdapterColumn, partColumns map[string]typedcolumn.ColumnPartColumn, dictionaries map[string]map[string]int64, seen map[string]struct{}) error {
		name := adapterColumn.Definition.Name
		if _, ok := seen[name]; ok {
			return nil
		}
		preparedColumn := prepared.Columns[name]
		if preparedColumn == nil {
			return fmt.Errorf("collections: dense typed-column minimal prepared adapter missing column %q", name)
		}
		adapterColumn.Definition = preparedColumn.Column.Definition
		if preparedColumn.Dictionaries != nil {
			adapterColumn.Dictionary = preparedColumn.Dictionaries
			dictionaries[adapterColumn.Definition.Name] = preparedColumn.Dictionaries
		}
		if preparedColumn.ReverseDictionaries != nil {
			adapterColumn.ReverseDictionary = preparedColumn.ReverseDictionaries
		}
		mode := typedColumnPreparedAdapterDictionaryModeForColumn(opts, adapterColumn.Definition.Name)
		if mode.ValuesByCode {
			if preparedColumn.DictionaryValuesByCode != nil {
				adapterColumn.DictionaryValuesByCode = preparedColumn.DictionaryValuesByCode
			} else if preparedColumn.Dictionaries != nil {
				valuesByCode, err := typedColumnAdapterDictionaryValuesByCodeFromForward(preparedColumn.Dictionaries, int(adapterColumn.Definition.Cardinality))
				if err != nil {
					return err
				}
				adapterColumn.DictionaryValuesByCode = valuesByCode
			}
		}
		partColumn := preparedColumn.Column
		if len(partColumn.Blocks) != 0 {
			partColumn.Blocks = append([]typedcolumn.ColumnBlock(nil), partColumn.Blocks...)
		}
		*columns = append(*columns, adapterColumn)
		partColumns[adapterColumn.Definition.Name] = partColumn
		seen[name] = struct{}{}
		return nil
	}

	columns := make([]typedColumnAdapterColumn, 0, 2+len(plan.PredicateSpecs))
	partColumns := make(map[string]typedcolumn.ColumnPartColumn, 2+len(plan.PredicateSpecs))
	dictionaries := make(map[string]map[string]int64, 1+len(plan.PredicateSpecs))
	seen := make(map[string]struct{}, 2+len(plan.PredicateSpecs))
	groupColumn, ok := findColumn(plan.GroupColumn)
	if !ok {
		return nil, false, fmt.Errorf("collections: dense typed-column group column %q is not owned by typed_column_part", plan.GroupColumn)
	}
	if err := addColumn(groupColumn, &columns, partColumns, dictionaries, seen); err != nil {
		return nil, false, err
	}
	valueColumn, ok := findColumn(plan.ValueColumn)
	if !ok {
		return nil, false, fmt.Errorf("collections: dense typed-column value column %q is not owned by typed_column_part", plan.ValueColumn)
	}
	if err := addColumn(valueColumn, &columns, partColumns, dictionaries, seen); err != nil {
		return nil, false, err
	}
	for _, spec := range plan.PredicateSpecs {
		predicateColumn, ok := findColumn(spec.column)
		if !ok {
			return nil, false, fmt.Errorf("collections: dense typed-column predicate column %q is not owned by typed_column_part", spec.column)
		}
		if err := addColumn(predicateColumn, &columns, partColumns, dictionaries, seen); err != nil {
			return nil, false, err
		}
	}
	descriptor := prepared.Descriptor
	if len(descriptor.SortKey) != 0 {
		descriptor.SortKey = append([]typedcolumn.SortKeyColumn(nil), descriptor.SortKey...)
	}
	marks := append([]typedcolumn.SortKeyMark(nil), prepared.Marks...)
	return &typedColumnAdapterPart{
		Options:    typedColumnAdapterOptions{Fields: fields, SchemaVersion: prepared.Descriptor.SchemaVersion},
		Columns:    columns,
		Part:       &typedcolumn.ColumnPart{Descriptor: descriptor, Columns: partColumns, Marks: marks},
		Dictionary: dictionaries,
	}, true, nil
}

func typedColumnPreparedColumnWithPayloads(preparedColumn *typedColumnPreparedColumnState, readRange typedColumnPreparedRangeReader) (typedcolumn.ColumnPartColumn, error) {
	return typedColumnPreparedColumnWithOptionalPayloads(preparedColumn, readRange, true)
}

func typedColumnPreparedColumnWithOptionalPayloads(preparedColumn *typedColumnPreparedColumnState, readRange typedColumnPreparedRangeReader, includePayloads bool) (typedcolumn.ColumnPartColumn, error) {
	if preparedColumn == nil {
		return typedcolumn.ColumnPartColumn{}, errors.New("collections: dense typed-column prepared range missing column")
	}
	column := preparedColumn.Column
	if len(column.Blocks) != 0 {
		column.Blocks = append([]typedcolumn.ColumnBlock(nil), column.Blocks...)
	}
	if !includePayloads {
		return column, nil
	}
	if len(preparedColumn.BlockPlans) != len(column.Blocks) {
		return typedcolumn.ColumnPartColumn{}, fmt.Errorf("collections: dense typed-column prepared range column %q block plans=%d want blocks=%d", preparedColumn.Column.Definition.Name, len(preparedColumn.BlockPlans), len(column.Blocks))
	}
	for _, blockPlan := range preparedColumn.BlockPlans {
		if blockPlan.Index < 0 || blockPlan.Index >= len(column.Blocks) {
			return typedcolumn.ColumnPartColumn{}, fmt.Errorf("collections: dense typed-column prepared range column %q block index=%d out of bounds", preparedColumn.Column.Definition.Name, blockPlan.Index)
		}
		if blockPlan.PayloadLength == 0 {
			continue
		}
		payload, err := readRange(blockPlan.PayloadOffset, blockPlan.PayloadLength, false)
		if err != nil {
			return typedcolumn.ColumnPartColumn{}, fmt.Errorf("collections: dense typed-column prepared range read column %q block %d payload: %w", preparedColumn.Column.Definition.Name, blockPlan.Index, err)
		}
		if len(payload) != blockPlan.PayloadLength {
			return typedcolumn.ColumnPartColumn{}, fmt.Errorf("collections: dense typed-column prepared range column %q block %d payload bytes=%d want %d", preparedColumn.Column.Definition.Name, blockPlan.Index, len(payload), blockPlan.PayloadLength)
		}
		block := &column.Blocks[blockPlan.Index]
		block.Granule.Payload = payload
		block.Granule.PayloadRef = typedcolumn.PayloadRef{Kind: typedcolumn.PayloadRefInline, Length: len(payload)}
	}
	return column, nil
}

func typedColumnPhysicalQueryPreparedSummary(prepared *typedColumnPreparedPartState, plan columnTypedColumnPhysicalQueryPlan, typedRef, physical columnManifestAssetRefForScan) (typedColumnAdapterImageSummary, error) {
	if prepared == nil {
		return typedColumnAdapterImageSummary{}, errors.New("collections: dense typed-column prepared range missing part")
	}
	summary := typedColumnAdapterImageSummary{
		PartID:       prepared.Image.PartID,
		Rows:         prepared.Image.Rows,
		Sections:     len(prepared.Image.Sections),
		SectionBytes: typedColumnPhysicalQueryImageSectionBytes(prepared.Image),
		SortKey:      typedRef.SortKey,
	}
	if summary.PartID != typedRef.Ref.PartID || summary.Rows != typedRef.Rows {
		return typedColumnAdapterImageSummary{}, fmt.Errorf("typed_column_part prepared image/ref mismatch image_part=%d ref_part=%d image_rows=%d manifest_rows=%d", summary.PartID, typedRef.Ref.PartID, summary.Rows, typedRef.Rows)
	}
	if physical.Rows != 0 && summary.Rows != physical.Rows {
		return typedColumnAdapterImageSummary{}, fmt.Errorf("typed_column_part prepared rows=%d do not match physical rows=%d", summary.Rows, physical.Rows)
	}
	if err := validateTypedColumnPhysicalQuerySortMetadata(plan.SortKey, typedRef.SortKey, summary.SortKey); err != nil {
		return typedColumnAdapterImageSummary{}, err
	}
	return summary, nil
}

func decodeTypedColumnPhysicalQueryDenseGroupCountPreparedPart(plan columnTypedColumnPhysicalQueryPlan, summary typedColumnAdapterImageSummary, typedRef, physical columnManifestAssetRefForScan, adapterPart *typedColumnAdapterPart, bytesRead int64, prepareDiagnostics *columnTypedColumnPhysicalQueryPrepareDiagnostics) (columnTypedColumnPhysicalQueryPart, error) {
	if plan.SortKeyPrefix.Planned {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("%w: dense typed-column group-count does not support sort-key row pruning", ErrColumnQueryPlanUnsupported)
	}
	var phaseStart time.Time
	if prepareDiagnostics != nil {
		phaseStart = time.Now()
	}
	group, decodedBytes, blocks, err := typedColumnDenseGroupCountCodeColumn(adapterPart, plan.Fields, plan.GroupColumn, summary.Rows, "group-count group")
	if prepareDiagnostics != nil {
		prepareDiagnostics.DenseGroupNanos += time.Since(phaseStart).Nanoseconds()
	}
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	return columnTypedColumnPhysicalQueryPart{
		Ref:                 typedRef,
		PhysicalRef:         physical,
		Rows:                summary.Rows,
		Bytes:               bytesRead,
		Sections:            summary.Sections,
		SectionBytes:        summary.SectionBytes,
		GranulesConsidered:  blocks,
		GranulesDecoded:     blocks,
		DecodedBlocks:       blocks,
		DecodedPayloadBytes: decodedBytes,
		DenseGroupCount:     &group,
	}, nil
}

func decodeTypedColumnPhysicalQueryDenseGroupCountDistinctPreparedPart(plan columnTypedColumnPhysicalQueryPlan, summary typedColumnAdapterImageSummary, typedRef, physical columnManifestAssetRefForScan, adapterPart *typedColumnAdapterPart, bytesRead int64) (columnTypedColumnPhysicalQueryPart, error) {
	group, groupDecodedBytes, groupBlocks, err := typedColumnDenseGroupCountDistinctCodeColumn(adapterPart, plan.Fields, plan.GroupColumn, summary.Rows, "grouped count-distinct group")
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	distinct, distinctDecodedBytes, distinctBlocks, err := typedColumnDenseGroupCountDistinctCodeColumn(adapterPart, plan.Fields, plan.DistinctColumn, summary.Rows, "grouped count-distinct distinct")
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	if len(group.Codes) != len(distinct.Codes) {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("collections: dense typed-column grouped count-distinct group/distinct rows=%d/%d", len(group.Codes), len(distinct.Codes))
	}

	predicates := make([]columnTypedColumnDensePredicatePart, 0, len(plan.PredicateSpecs))
	predicateDecodedBytes := uint64(0)
	predicateBlocks := 0
	for _, spec := range plan.PredicateSpecs {
		predicate, decodedBytes, blocks, err := decodeTypedColumnDensePredicatePart(adapterPart, plan.Fields, spec, summary.Rows)
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, err
		}
		predicates = append(predicates, predicate)
		predicateDecodedBytes += decodedBytes
		predicateBlocks += blocks
	}

	decodedBlocks := groupBlocks + distinctBlocks + predicateBlocks
	return columnTypedColumnPhysicalQueryPart{
		Ref:                 typedRef,
		PhysicalRef:         physical,
		Rows:                summary.Rows,
		Bytes:               bytesRead,
		Sections:            summary.Sections,
		SectionBytes:        summary.SectionBytes,
		GranulesConsidered:  decodedBlocks,
		GranulesDecoded:     decodedBlocks,
		DecodedBlocks:       decodedBlocks,
		DecodedPayloadBytes: groupDecodedBytes + distinctDecodedBytes + predicateDecodedBytes,
		DenseGroupCountDistinct: &columnTypedColumnDenseGroupCountDistinctPart{
			Rows:       summary.Rows,
			Group:      group,
			Distinct:   distinct,
			Predicates: predicates,
		},
	}, nil
}

func decodeTypedColumnPhysicalQueryDenseGroupHourCountPreparedPart(plan columnTypedColumnPhysicalQueryPlan, summary typedColumnAdapterImageSummary, typedRef, physical columnManifestAssetRefForScan, adapterPart *typedColumnAdapterPart, bytesRead int64, prepareDiagnostics *columnTypedColumnPhysicalQueryPrepareDiagnostics) (columnTypedColumnPhysicalQueryPart, error) {
	spanPlan, groupPredicate, hasGroupPredicate := columnTypedColumnPhysicalQueryDenseGroupHourSpanPlan(plan)
	part, err := decodeTypedColumnPhysicalQueryDenseInt64SpanPreparedPart(spanPlan, summary, typedRef, physical, adapterPart, bytesRead, false, prepareDiagnostics)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	if part.DenseInt64Span == nil {
		return columnTypedColumnPhysicalQueryPart{}, errors.New("collections: dense typed-column group-hour missing decoded string/int64 payload")
	}
	predicates := part.DenseInt64Span.Predicates
	if hasGroupPredicate {
		predicate, err := densePredicateFromDictionaryCodes(part.DenseInt64Span.GroupCodes, part.DenseInt64Span.GroupValid, part.DenseInt64Span.Dictionary, part.DenseInt64Span.DictionaryByCode, part.DenseInt64Span.Cardinality, groupPredicate)
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, err
		}
		predicates = append(predicates, predicate)
	}
	part.DenseGroupHourCount = &columnTypedColumnDenseGroupHourCountPart{
		Cardinality:      part.DenseInt64Span.Cardinality,
		Dictionary:       part.DenseInt64Span.Dictionary,
		DictionaryByCode: part.DenseInt64Span.DictionaryByCode,
		GroupCodes:       part.DenseInt64Span.GroupCodes,
		GroupValid:       part.DenseInt64Span.GroupValid,
		Values:           part.DenseInt64Span.Values,
		Predicates:       predicates,
	}
	part.DenseInt64Span = nil
	return part, nil
}

func decodeTypedColumnPhysicalQueryDenseGroupHourCountPredicateFirstPreparedPart(plan columnTypedColumnPhysicalQueryPlan, summary typedColumnAdapterImageSummary, typedRef, physical columnManifestAssetRefForScan, prepared *typedColumnPreparedPartState, adapterPart *typedColumnAdapterPart, readRange typedColumnPreparedRangeReader, bytesRead func() int64, prepareDiagnostics *columnTypedColumnPhysicalQueryPrepareDiagnostics) (columnTypedColumnPhysicalQueryPart, bool, error) {
	if prepared == nil || adapterPart == nil || adapterPart.Part == nil || readRange == nil {
		return columnTypedColumnPhysicalQueryPart{}, false, nil
	}
	spanPlan, groupPredicateSpec, hasGroupPredicate := columnTypedColumnPhysicalQueryDenseGroupHourSpanPlan(plan)
	if !hasGroupPredicate || len(spanPlan.PredicateSpecs) != 2 {
		return columnTypedColumnPhysicalQueryPart{}, false, nil
	}
	groupColumn, groupPartColumn, cardinality, err := typedColumnDenseStringCodeColumn(adapterPart, plan.Fields, plan.GroupColumn, "group-hour group")
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, false, err
	}
	valueColumn, ok, err := typedColumnInt64PredicateAdapterColumn(plan.Fields, plan.ValueColumn)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, false, err
	}
	if !ok {
		return columnTypedColumnPhysicalQueryPart{}, false, fmt.Errorf("collections: dense typed-column group-hour value column %q is not owned by typed_column_part", plan.ValueColumn)
	}
	for _, candidate := range adapterPart.Columns {
		if candidate.Definition.Name == valueColumn.Definition.Name {
			valueColumn = candidate
			break
		}
	}
	if valueColumn.Field.Nullable || valueColumn.Definition.Type != typedcolumn.ColumnTypeInt64 {
		return columnTypedColumnPhysicalQueryPart{}, false, fmt.Errorf("%w: dense typed-column group-hour value column %q is not a non-null int64", ErrColumnQueryPlanUnsupported, plan.ValueColumn)
	}
	valuePartColumn, ok := adapterPart.Part.Columns[valueColumn.Definition.Name]
	if !ok {
		return columnTypedColumnPhysicalQueryPart{}, false, fmt.Errorf("collections: dense typed-column group-hour missing value column %q", valueColumn.Definition.Name)
	}
	if valuePartColumn.Definition.Type != typedcolumn.ColumnTypeInt64 {
		return columnTypedColumnPhysicalQueryPart{}, false, fmt.Errorf("%w: dense typed-column group-hour value column %q type=%s", ErrColumnQueryPlanUnsupported, plan.ValueColumn, valuePartColumn.Definition.Type)
	}
	groupPredicate, err := densePredicateFromDictionaryCodes(nil, nil, groupColumn.DictionaryValuesByCode, groupColumn.ReverseDictionary, cardinality, groupPredicateSpec)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, false, err
	}
	var phaseStart time.Time
	if prepareDiagnostics != nil {
		phaseStart = time.Now()
	}
	candidates, predicates, ok, err := typedColumnDenseSingleCodePairPredicateCandidates(adapterPart, plan.Fields, spanPlan.PredicateSpecs, summary.Rows)
	if err == nil && ok {
		var predicateBlocksMask []bool
		var predicateRowBlocksSkipped int
		predicateBlocksMask, predicateRowBlocksSkipped, err = typedColumnDenseSingleCodePredicateBlockMask(candidates, summary.Rows)
		selectedPredicateBlocks := predicateBlocksMask
		if predicateRowBlocksSkipped == 0 {
			selectedPredicateBlocks = nil
		}
		if err == nil {
			_, err = typedColumnPhysicalQueryAttachPredicatePayloads(prepared, adapterPart, plan.Fields, spanPlan.PredicateSpecs, selectedPredicateBlocks, readRange)
		}
		if err == nil {
			err = refreshTypedColumnDenseSingleCodePredicateCandidatePayloads(adapterPart, candidates)
		}
		var selectedRows []uint32
		var predicateDecodedBytes uint64
		var predicateBlocks int
		if err == nil {
			predicates, selectedRows, predicateDecodedBytes, predicateBlocks, ok, err = decodeTypedColumnDenseSingleCodePairPredicateRowsFromCandidates(candidates, predicates, summary.Rows, selectedPredicateBlocks)
		}
		if prepareDiagnostics != nil {
			prepareDiagnostics.DensePredicateNanos += time.Since(phaseStart).Nanoseconds()
		}
		if err != nil || !ok {
			return columnTypedColumnPhysicalQueryPart{}, ok, err
		}
		return decodeTypedColumnPhysicalQueryDenseGroupHourCountPredicateFirstSelectedRows(plan, summary, typedRef, physical, prepared, adapterPart, readRange, bytesRead, prepareDiagnostics, groupColumn, groupPartColumn, valueColumn, valuePartColumn, cardinality, groupPredicate, predicates, selectedRows, predicateDecodedBytes, predicateBlocks)
	}
	if prepareDiagnostics != nil {
		prepareDiagnostics.DensePredicateNanos += time.Since(phaseStart).Nanoseconds()
	}
	return columnTypedColumnPhysicalQueryPart{}, ok, err
}

func decodeTypedColumnPhysicalQueryDenseGroupHourCountPredicateFirstSelectedRows(plan columnTypedColumnPhysicalQueryPlan, summary typedColumnAdapterImageSummary, typedRef, physical columnManifestAssetRefForScan, prepared *typedColumnPreparedPartState, adapterPart *typedColumnAdapterPart, readRange typedColumnPreparedRangeReader, bytesRead func() int64, prepareDiagnostics *columnTypedColumnPhysicalQueryPrepareDiagnostics, groupColumn typedColumnAdapterColumn, groupPartColumn typedcolumn.ColumnPartColumn, valueColumn typedColumnAdapterColumn, valuePartColumn typedcolumn.ColumnPartColumn, cardinality int, groupPredicate columnTypedColumnDensePredicatePart, predicates []columnTypedColumnDensePredicatePart, selectedRows []uint32, predicateDecodedBytes uint64, predicateBlocks int) (columnTypedColumnPhysicalQueryPart, bool, error) {
	var phaseStart time.Time
	predicates = append(predicates, groupPredicate)
	var groupCodes []uint32
	var groupValid []bool
	var filteredRows []uint32
	groupDecodedBytes := uint64(0)
	groupBlocks := 0
	if len(selectedRows) != 0 && !groupPredicate.RejectsAll {
		groupBlocksMask, err := typedColumnDenseSelectedPredicateBlockMask(groupPartColumn, selectedRows, summary.Rows, &groupPredicate, cardinality, "group-hour group")
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, true, err
		}
		if prepareDiagnostics != nil {
			phaseStart = time.Now()
		}
		if _, _, err := typedColumnPhysicalQueryAttachPreparedPayloads(prepared, adapterPart, groupColumn.Definition.Name, "group-hour group", groupBlocksMask, readRange); err != nil {
			return columnTypedColumnPhysicalQueryPart{}, true, err
		}
		groupPartColumn = adapterPart.Part.Columns[groupColumn.Definition.Name]
		if groupColumn.Field.Nullable {
			groupCodes, groupValid, filteredRows, groupDecodedBytes, groupBlocks, err = decodeTypedColumnDenseNullableUint32CodesSelectedRowsMatchingPredicate(groupPartColumn, cardinality, selectedRows, summary.Rows, &groupPredicate, groupBlocksMask, "group-hour group")
		} else {
			groupCodes, filteredRows, groupDecodedBytes, groupBlocks, err = decodeTypedColumnDenseUint32CodesSelectedRowsMatchingPredicate(groupPartColumn, cardinality, selectedRows, summary.Rows, &groupPredicate, groupBlocksMask, "group-hour group")
		}
		if prepareDiagnostics != nil {
			prepareDiagnostics.DenseGroupNanos += time.Since(phaseStart).Nanoseconds()
		}
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, true, err
		}
	}
	values := make([]int64, 0, len(filteredRows))
	valueDecodedBytes := uint64(0)
	valueBlocks := 0
	if len(filteredRows) != 0 {
		valueBlocksMask, err := typedColumnDenseSelectedBlockMask(valuePartColumn, filteredRows, summary.Rows, "group-hour value")
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, true, err
		}
		if prepareDiagnostics != nil {
			phaseStart = time.Now()
		}
		if _, _, err := typedColumnPhysicalQueryAttachPreparedPayloads(prepared, adapterPart, valueColumn.Definition.Name, "group-hour value", valueBlocksMask, readRange); err != nil {
			return columnTypedColumnPhysicalQueryPart{}, true, err
		}
		valuePartColumn = adapterPart.Part.Columns[valueColumn.Definition.Name]
		values, valueDecodedBytes, valueBlocks, err = decodeTypedColumnDenseInt64ValuesSelectedRows(valuePartColumn, filteredRows, summary.Rows, "group-hour value")
		if prepareDiagnostics != nil {
			prepareDiagnostics.DenseValueNanos += time.Since(phaseStart).Nanoseconds()
		}
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, true, err
		}
	}
	if len(groupCodes) != len(values) {
		return columnTypedColumnPhysicalQueryPart{}, true, fmt.Errorf("collections: dense typed-column group-hour selected rows group/value=%d/%d", len(groupCodes), len(values))
	}
	decodedBlocks := predicateBlocks + groupBlocks + valueBlocks
	partBytes := int64(0)
	if bytesRead != nil {
		partBytes = bytesRead()
	}
	return columnTypedColumnPhysicalQueryPart{
		Ref:                 typedRef,
		PhysicalRef:         physical,
		Rows:                summary.Rows,
		Bytes:               partBytes,
		Sections:            summary.Sections,
		SectionBytes:        summary.SectionBytes,
		GranulesConsidered:  decodedBlocks,
		GranulesDecoded:     decodedBlocks,
		DecodedBlocks:       decodedBlocks,
		DecodedPayloadBytes: predicateDecodedBytes + groupDecodedBytes + valueDecodedBytes,
		DenseGroupHourCount: &columnTypedColumnDenseGroupHourCountPart{
			Cardinality:           cardinality,
			Dictionary:            groupColumn.DictionaryValuesByCode,
			DictionaryByCode:      groupColumn.ReverseDictionary,
			GroupCodes:            groupCodes,
			GroupValid:            groupValid,
			Values:                values,
			Predicates:            predicates,
			PredicatesPreApplied:  true,
			PreAppliedRowsScanned: summary.Rows,
		},
	}, true, nil
}

func decodeTypedColumnPhysicalQueryDenseInt64SpanPreparedPart(plan columnTypedColumnPhysicalQueryPlan, summary typedColumnAdapterImageSummary, typedRef, physical columnManifestAssetRefForScan, adapterPart *typedColumnAdapterPart, bytesRead int64, preapplyPredicates bool, prepareDiagnostics *columnTypedColumnPhysicalQueryPrepareDiagnostics) (columnTypedColumnPhysicalQueryPart, error) {
	groupColumn, groupPartColumn, cardinality, err := typedColumnDenseStringCodeColumn(adapterPart, plan.Fields, plan.GroupColumn, "int64-span group")
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	valueColumn, ok, err := typedColumnInt64PredicateAdapterColumn(plan.Fields, plan.ValueColumn)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	if !ok {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("collections: dense typed-column int64-span value column %q is not owned by typed_column_part", plan.ValueColumn)
	}
	for _, candidate := range adapterPart.Columns {
		if candidate.Definition.Name == valueColumn.Definition.Name {
			valueColumn = candidate
			break
		}
	}
	if valueColumn.Field.Nullable || valueColumn.Definition.Type != typedcolumn.ColumnTypeInt64 {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("%w: dense typed-column int64-span value column %q is not a non-null int64", ErrColumnQueryPlanUnsupported, plan.ValueColumn)
	}
	valuePartColumn, ok := adapterPart.Part.Columns[valueColumn.Definition.Name]
	if !ok {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("collections: dense typed-column int64-span missing value column %q", valueColumn.Definition.Name)
	}
	if valuePartColumn.Definition.Type != typedcolumn.ColumnTypeInt64 {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("%w: dense typed-column int64-span value column %q type=%s", ErrColumnQueryPlanUnsupported, plan.ValueColumn, valuePartColumn.Definition.Type)
	}

	var groupCodes []uint32
	var groupValid []bool
	var groupDecodedBytes uint64
	var groupBlocks int
	var phaseStart time.Time
	if prepareDiagnostics != nil {
		phaseStart = time.Now()
	}
	if groupColumn.Field.Nullable {
		groupCodes, groupValid, groupDecodedBytes, groupBlocks, err = decodeTypedColumnDenseNullableUint32Codes(groupPartColumn, cardinality, summary.Rows, "int64-span group")
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, err
		}
	} else {
		groupCodes, groupDecodedBytes, groupBlocks, err = decodeTypedColumnDenseUint32Codes(groupPartColumn, cardinality, summary.Rows, "int64-span group")
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, err
		}
	}
	if prepareDiagnostics != nil {
		prepareDiagnostics.DenseGroupNanos += time.Since(phaseStart).Nanoseconds()
	}
	if prepareDiagnostics != nil {
		phaseStart = time.Now()
	}
	values, valueDecodedBytes, valueBlocks, err := decodeTypedColumnDenseInt64Values(valuePartColumn, summary.Rows, "int64-span value")
	if prepareDiagnostics != nil {
		prepareDiagnostics.DenseValueNanos += time.Since(phaseStart).Nanoseconds()
	}
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	if len(groupCodes) != len(values) {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("collections: dense typed-column int64-span group/value rows=%d/%d", len(groupCodes), len(values))
	}

	predicateDecodedBytes := uint64(0)
	predicateBlocks := 0
	predicateRowsPreApplied := false
	var predicateRows []uint32
	var predicates []columnTypedColumnDensePredicatePart
	if prepareDiagnostics != nil {
		phaseStart = time.Now()
	}
	if preapplyPredicates {
		var ok bool
		predicates, predicateRows, predicateDecodedBytes, predicateBlocks, ok, err = decodeTypedColumnDenseInt64SpanSingleCodeTriplePredicateRows(adapterPart, plan.Fields, plan.PredicateSpecs, summary.Rows)
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, err
		}
		predicateRowsPreApplied = ok
	}
	if !predicateRowsPreApplied {
		predicates = make([]columnTypedColumnDensePredicatePart, 0, len(plan.PredicateSpecs))
		predicateDecodedBytes = 0
		predicateBlocks = 0
		for _, spec := range plan.PredicateSpecs {
			predicate, decodedBytes, blocks, err := decodeTypedColumnDensePredicatePart(adapterPart, plan.Fields, spec, summary.Rows)
			if err != nil {
				return columnTypedColumnPhysicalQueryPart{}, err
			}
			predicates = append(predicates, predicate)
			predicateDecodedBytes += decodedBytes
			predicateBlocks += blocks
		}
	}
	if prepareDiagnostics != nil {
		prepareDiagnostics.DensePredicateNanos += time.Since(phaseStart).Nanoseconds()
	}

	denseSpan := &columnTypedColumnDenseInt64SpanPart{
		Cardinality:      cardinality,
		Dictionary:       groupColumn.DictionaryValuesByCode,
		DictionaryByCode: groupColumn.ReverseDictionary,
		GroupCodes:       groupCodes,
		GroupValid:       groupValid,
		Values:           values,
		Predicates:       predicates,
	}
	if predicateRowsPreApplied {
		denseSpan.PredicatesPreApplied = true
		denseSpan.PredicateRows = predicateRows
		denseSpan.PreAppliedRowsScanned = summary.Rows
	} else if preapplyPredicates {
		if prepareDiagnostics != nil {
			phaseStart = time.Now()
		}
		preapplyColumnTypedColumnDenseInt64SpanPredicates(denseSpan, summary.Rows)
		if prepareDiagnostics != nil {
			prepareDiagnostics.DensePreapplyNanos += time.Since(phaseStart).Nanoseconds()
		}
	}
	decodedBlocks := groupBlocks + valueBlocks + predicateBlocks
	return columnTypedColumnPhysicalQueryPart{
		Ref:                 typedRef,
		PhysicalRef:         physical,
		Rows:                summary.Rows,
		Bytes:               bytesRead,
		Sections:            summary.Sections,
		SectionBytes:        summary.SectionBytes,
		GranulesConsidered:  decodedBlocks,
		GranulesDecoded:     decodedBlocks,
		DecodedBlocks:       decodedBlocks,
		DecodedPayloadBytes: groupDecodedBytes + valueDecodedBytes + predicateDecodedBytes,
		DenseInt64Span:      denseSpan,
	}, nil
}

func decodeTypedColumnPhysicalQueryDenseInt64SpanPredicateFirstPreparedPart(plan columnTypedColumnPhysicalQueryPlan, summary typedColumnAdapterImageSummary, typedRef, physical columnManifestAssetRefForScan, prepared *typedColumnPreparedPartState, adapterPart *typedColumnAdapterPart, readRange typedColumnPreparedRangeReader, bytesRead func() int64, prepareDiagnostics *columnTypedColumnPhysicalQueryPrepareDiagnostics) (columnTypedColumnPhysicalQueryPart, bool, error) {
	if prepared == nil || adapterPart == nil || adapterPart.Part == nil || readRange == nil || len(plan.PredicateSpecs) != 3 {
		return columnTypedColumnPhysicalQueryPart{}, false, nil
	}
	groupColumn, groupPartColumn, cardinality, err := typedColumnDenseStringCodeColumn(adapterPart, plan.Fields, plan.GroupColumn, "int64-span group")
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, false, err
	}
	valueColumn, ok, err := typedColumnInt64PredicateAdapterColumn(plan.Fields, plan.ValueColumn)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, false, err
	}
	if !ok {
		return columnTypedColumnPhysicalQueryPart{}, false, fmt.Errorf("collections: dense typed-column int64-span value column %q is not owned by typed_column_part", plan.ValueColumn)
	}
	for _, candidate := range adapterPart.Columns {
		if candidate.Definition.Name == valueColumn.Definition.Name {
			valueColumn = candidate
			break
		}
	}
	if valueColumn.Field.Nullable || valueColumn.Definition.Type != typedcolumn.ColumnTypeInt64 {
		return columnTypedColumnPhysicalQueryPart{}, false, fmt.Errorf("%w: dense typed-column int64-span value column %q is not a non-null int64", ErrColumnQueryPlanUnsupported, plan.ValueColumn)
	}
	valuePartColumn, ok := adapterPart.Part.Columns[valueColumn.Definition.Name]
	if !ok {
		return columnTypedColumnPhysicalQueryPart{}, false, fmt.Errorf("collections: dense typed-column int64-span missing value column %q", valueColumn.Definition.Name)
	}
	if valuePartColumn.Definition.Type != typedcolumn.ColumnTypeInt64 {
		return columnTypedColumnPhysicalQueryPart{}, false, fmt.Errorf("%w: dense typed-column int64-span value column %q type=%s", ErrColumnQueryPlanUnsupported, plan.ValueColumn, valuePartColumn.Definition.Type)
	}

	var phaseStart time.Time
	if prepareDiagnostics != nil {
		phaseStart = time.Now()
	}
	candidates, predicates, ok, err := typedColumnDenseInt64SpanSingleCodeTriplePredicateCandidates(adapterPart, plan.Fields, plan.PredicateSpecs, summary.Rows)
	if err != nil || !ok {
		return columnTypedColumnPhysicalQueryPart{}, ok, err
	}
	predicateBlocksMask, predicateRowBlocksSkipped, err := typedColumnDenseSingleCodeTriplePredicateBlockMask(candidates, summary.Rows)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, false, err
	}
	if prepareDiagnostics != nil {
		prepareDiagnostics.DensePredicateNanos += time.Since(phaseStart).Nanoseconds()
	}
	selectedPredicateBlocks := predicateBlocksMask
	if predicateRowBlocksSkipped == 0 {
		selectedPredicateBlocks = nil
	}
	predicatePayloadColumns, err := typedColumnPhysicalQueryAttachPredicatePayloads(prepared, adapterPart, plan.Fields, plan.PredicateSpecs, selectedPredicateBlocks, readRange)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, false, err
	}
	predicateBlocksSkipped := predicateRowBlocksSkipped * predicatePayloadColumns
	if err := refreshTypedColumnDenseSingleCodePredicateCandidatePayloads(adapterPart, candidates); err != nil {
		return columnTypedColumnPhysicalQueryPart{}, false, err
	}
	if prepareDiagnostics != nil {
		phaseStart = time.Now()
	}
	predicates, selectedRows, predicateDecodedBytes, predicateBlocks, ok, err := decodeTypedColumnDenseInt64SpanSingleCodeTriplePredicateRowsFromCandidates(candidates, predicates, summary.Rows, selectedPredicateBlocks)
	if prepareDiagnostics != nil {
		prepareDiagnostics.DensePredicateNanos += time.Since(phaseStart).Nanoseconds()
	}
	if err != nil || !ok {
		return columnTypedColumnPhysicalQueryPart{}, ok, err
	}

	groupCodes := make([]uint32, 0, len(selectedRows))
	var groupValid []bool
	values := make([]int64, 0, len(selectedRows))
	groupDecodedBytes := uint64(0)
	valueDecodedBytes := uint64(0)
	groupBlocks := 0
	valueBlocks := 0
	if len(selectedRows) != 0 {
		groupBlocksMask, err := typedColumnDenseSelectedBlockMask(groupPartColumn, selectedRows, summary.Rows, "int64-span group")
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, true, err
		}
		if prepareDiagnostics != nil {
			phaseStart = time.Now()
		}
		if _, _, err := typedColumnPhysicalQueryAttachPreparedPayloads(prepared, adapterPart, groupColumn.Definition.Name, "int64-span group", groupBlocksMask, readRange); err != nil {
			return columnTypedColumnPhysicalQueryPart{}, true, err
		}
		groupPartColumn = adapterPart.Part.Columns[groupColumn.Definition.Name]
		if groupColumn.Field.Nullable {
			groupCodes, groupValid, groupDecodedBytes, groupBlocks, err = decodeTypedColumnDenseNullableUint32CodesSelectedRows(groupPartColumn, cardinality, selectedRows, summary.Rows, "int64-span group")
		} else {
			groupCodes, groupDecodedBytes, groupBlocks, err = decodeTypedColumnDenseUint32CodesSelectedRows(groupPartColumn, cardinality, selectedRows, summary.Rows, "int64-span group")
		}
		if prepareDiagnostics != nil {
			prepareDiagnostics.DenseGroupNanos += time.Since(phaseStart).Nanoseconds()
		}
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, true, err
		}

		valueBlocksMask, err := typedColumnDenseSelectedBlockMask(valuePartColumn, selectedRows, summary.Rows, "int64-span value")
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, true, err
		}
		if prepareDiagnostics != nil {
			phaseStart = time.Now()
		}
		if _, _, err := typedColumnPhysicalQueryAttachPreparedPayloads(prepared, adapterPart, valueColumn.Definition.Name, "int64-span value", valueBlocksMask, readRange); err != nil {
			return columnTypedColumnPhysicalQueryPart{}, true, err
		}
		valuePartColumn = adapterPart.Part.Columns[valueColumn.Definition.Name]
		values, valueDecodedBytes, valueBlocks, err = decodeTypedColumnDenseInt64ValuesSelectedRows(valuePartColumn, selectedRows, summary.Rows, "int64-span value")
		if prepareDiagnostics != nil {
			prepareDiagnostics.DenseValueNanos += time.Since(phaseStart).Nanoseconds()
		}
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, true, err
		}
	}
	if len(groupCodes) != len(values) || len(groupCodes) != len(selectedRows) {
		return columnTypedColumnPhysicalQueryPart{}, true, fmt.Errorf("collections: dense typed-column int64-span selected rows group/value/selected=%d/%d/%d", len(groupCodes), len(values), len(selectedRows))
	}

	predicateRows := make([]uint32, len(selectedRows))
	for idx := range selectedRows {
		predicateRows[idx] = uint32(idx)
	}
	denseSpan := &columnTypedColumnDenseInt64SpanPart{
		Cardinality:           cardinality,
		Dictionary:            groupColumn.DictionaryValuesByCode,
		DictionaryByCode:      groupColumn.ReverseDictionary,
		GroupCodes:            groupCodes,
		GroupValid:            groupValid,
		Values:                values,
		Predicates:            predicates,
		PredicatesPreApplied:  true,
		PredicateRows:         predicateRows,
		PreAppliedRowsScanned: summary.Rows,
	}
	decodedBlocks := groupBlocks + valueBlocks + predicateBlocks
	partBytes := int64(0)
	if bytesRead != nil {
		partBytes = bytesRead()
	}
	return columnTypedColumnPhysicalQueryPart{
		Ref:                                  typedRef,
		PhysicalRef:                          physical,
		Rows:                                 summary.Rows,
		Bytes:                                partBytes,
		Sections:                             summary.Sections,
		SectionBytes:                         summary.SectionBytes,
		GranulesConsidered:                   decodedBlocks,
		GranulesDecoded:                      decodedBlocks,
		DecodedBlocks:                        decodedBlocks,
		DecodedPayloadBytes:                  groupDecodedBytes + valueDecodedBytes + predicateDecodedBytes,
		DenseInt64SpanPredicateBlocksSkipped: predicateBlocksSkipped,
		DenseInt64Span:                       denseSpan,
	}, true, nil
}

func typedColumnPhysicalQueryAttachPredicatePayloads(prepared *typedColumnPreparedPartState, adapterPart *typedColumnAdapterPart, fields []TypedStorageField, specs []columnPhysicalQueryPredicateSpec, selectedBlocks []bool, readRange typedColumnPreparedRangeReader) (int, error) {
	seen := make(map[string]struct{}, len(specs))
	attachedColumns := 0
	for _, spec := range specs {
		adapterColumn, ok, err := typedColumnStringPredicateAdapterColumn(fields, spec.column)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, fmt.Errorf("collections: dense typed-column predicate column %q is not owned by typed_column_part", spec.column)
		}
		name := adapterColumn.Definition.Name
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		if _, _, err := typedColumnPhysicalQueryAttachPreparedPayloads(prepared, adapterPart, name, "predicate", selectedBlocks, readRange); err != nil {
			return 0, err
		}
		attachedColumns++
	}
	return attachedColumns, nil
}

func typedColumnPhysicalQueryAttachPreparedPayloads(prepared *typedColumnPreparedPartState, adapterPart *typedColumnAdapterPart, columnName, role string, selectedBlocks []bool, readRange typedColumnPreparedRangeReader) (uint64, int, error) {
	if prepared == nil || adapterPart == nil || adapterPart.Part == nil {
		return 0, 0, errors.New("collections: dense typed-column prepared range missing part")
	}
	if readRange == nil {
		return 0, 0, errors.New("collections: dense typed-column prepared range missing payload reader")
	}
	preparedColumn := prepared.Columns[columnName]
	if preparedColumn == nil {
		return 0, 0, fmt.Errorf("collections: dense typed-column prepared range missing %s column %q", role, columnName)
	}
	partColumn, ok := adapterPart.Part.Columns[columnName]
	if !ok {
		return 0, 0, fmt.Errorf("collections: dense typed-column prepared range missing %s part column %q", role, columnName)
	}
	if len(preparedColumn.BlockPlans) != len(partColumn.Blocks) {
		return 0, 0, fmt.Errorf("collections: dense typed-column prepared range %s column %q block plans=%d want blocks=%d", role, columnName, len(preparedColumn.BlockPlans), len(partColumn.Blocks))
	}
	if selectedBlocks != nil && len(selectedBlocks) != len(partColumn.Blocks) {
		return 0, 0, fmt.Errorf("collections: dense typed-column prepared range %s column %q selected blocks=%d want %d", role, columnName, len(selectedBlocks), len(partColumn.Blocks))
	}
	decodedBytes := uint64(0)
	decodedBlocks := 0
	for blockIdx := 0; blockIdx < len(partColumn.Blocks); {
		if selectedBlocks != nil && !selectedBlocks[blockIdx] {
			blockIdx++
			continue
		}
		blockPlan := preparedColumn.BlockPlans[blockIdx]
		if blockPlan.Index != blockIdx {
			return 0, 0, fmt.Errorf("collections: dense typed-column prepared range %s column %q block plan index=%d want %d", role, columnName, blockPlan.Index, blockIdx)
		}
		block := &partColumn.Blocks[blockIdx]
		if blockPlan.PayloadLength > 0 && len(block.Granule.Payload) == 0 {
			runEnd := blockIdx + 1
			runOffset := blockPlan.PayloadOffset
			runLength := blockPlan.PayloadLength
			for runEnd < len(partColumn.Blocks) {
				runEndOffset := runOffset + runLength
				if runEndOffset < runOffset {
					return 0, 0, fmt.Errorf("collections: dense typed-column prepared range %s column %q payload run end overflows at block %d", role, columnName, runEnd)
				}
				if selectedBlocks != nil && !selectedBlocks[runEnd] {
					break
				}
				nextPlan := preparedColumn.BlockPlans[runEnd]
				if nextPlan.Index != runEnd {
					return 0, 0, fmt.Errorf("collections: dense typed-column prepared range %s column %q block plan index=%d want %d", role, columnName, nextPlan.Index, runEnd)
				}
				nextBlock := &partColumn.Blocks[runEnd]
				if nextPlan.PayloadLength <= 0 || len(nextBlock.Granule.Payload) != 0 {
					break
				}
				if nextPlan.PayloadOffset != runEndOffset {
					break
				}
				if nextPlan.PayloadLength > maxCollectionInt-runLength {
					return 0, 0, fmt.Errorf("collections: dense typed-column prepared range %s column %q payload run length overflows at block %d", role, columnName, runEnd)
				}
				runLength += nextPlan.PayloadLength
				runEnd++
			}
			payload, err := readRange(runOffset, runLength, false)
			if err != nil {
				return 0, 0, fmt.Errorf("collections: dense typed-column prepared range read %s column %q blocks %d-%d payload: %w", role, columnName, blockIdx, runEnd-1, err)
			}
			if len(payload) != runLength {
				return 0, 0, fmt.Errorf("collections: dense typed-column prepared range %s column %q blocks %d-%d payload bytes=%d want %d", role, columnName, blockIdx, runEnd-1, len(payload), runLength)
			}
			for runBlockIdx := blockIdx; runBlockIdx < runEnd; runBlockIdx++ {
				runBlockPlan := preparedColumn.BlockPlans[runBlockIdx]
				payloadStart := runBlockPlan.PayloadOffset - runOffset
				payloadEnd := payloadStart + runBlockPlan.PayloadLength
				if payloadStart < 0 || payloadEnd < payloadStart || payloadEnd > len(payload) {
					return 0, 0, fmt.Errorf("collections: dense typed-column prepared range %s column %q block %d payload slice [%d,%d) outside run bytes=%d", role, columnName, runBlockIdx, payloadStart, payloadEnd, len(payload))
				}
				runBlock := &partColumn.Blocks[runBlockIdx]
				runBlock.Granule.Payload = payload[payloadStart:payloadEnd]
				runBlock.Granule.PayloadRef = typedcolumn.PayloadRef{Kind: typedcolumn.PayloadRefInline, Length: runBlockPlan.PayloadLength}
				decodedBytes += uint64(runBlock.Granule.RawBytes)
				decodedBlocks++
			}
			blockIdx = runEnd
			continue
		}
		decodedBytes += uint64(block.Granule.RawBytes)
		decodedBlocks++
		blockIdx++
	}
	adapterPart.Part.Columns[columnName] = partColumn
	return decodedBytes, decodedBlocks, nil
}

func typedColumnDenseSelectedBlockMask(partColumn typedcolumn.ColumnPartColumn, selectedRows []uint32, rows int, role string) ([]bool, error) {
	mask := make([]bool, len(partColumn.Blocks))
	if len(selectedRows) == 0 {
		return mask, nil
	}
	selectedIdx := 0
	rowOffset := 0
	previous := -1
	for blockIdx, block := range partColumn.Blocks {
		blockRows := block.Descriptor.RowCount
		if blockRows < 0 || blockRows > rows-rowOffset {
			return nil, fmt.Errorf("collections: dense typed-column %s block %d rows=%d outside remaining rows=%d", role, blockIdx, blockRows, rows-rowOffset)
		}
		blockLimit := rowOffset + blockRows
		for selectedIdx < len(selectedRows) {
			row := int(selectedRows[selectedIdx])
			if row <= previous {
				return nil, fmt.Errorf("collections: dense typed-column %s selected rows are not strictly ascending at index %d", role, selectedIdx)
			}
			if row < rowOffset {
				return nil, fmt.Errorf("collections: dense typed-column %s selected row %d precedes block offset %d", role, row, rowOffset)
			}
			if row >= blockLimit {
				break
			}
			mask[blockIdx] = true
			previous = row
			selectedIdx++
		}
		rowOffset = blockLimit
	}
	if rowOffset != rows {
		return nil, fmt.Errorf("collections: dense typed-column %s block rows=%d want part rows=%d", role, rowOffset, rows)
	}
	if selectedIdx != len(selectedRows) {
		return nil, fmt.Errorf("collections: dense typed-column %s selected row %d outside part rows=%d", role, selectedRows[selectedIdx], rows)
	}
	return mask, nil
}

func typedColumnDenseSelectedPredicateBlockMask(partColumn typedcolumn.ColumnPartColumn, selectedRows []uint32, rows int, predicate *columnTypedColumnDensePredicatePart, cardinality int, role string) ([]bool, error) {
	mask := make([]bool, len(partColumn.Blocks))
	if len(selectedRows) == 0 {
		return mask, nil
	}
	selectedIdx := 0
	rowOffset := 0
	previous := -1
	for blockIdx, block := range partColumn.Blocks {
		blockRows := block.Descriptor.RowCount
		if blockRows < 0 || blockRows > rows-rowOffset {
			return nil, fmt.Errorf("collections: dense typed-column %s block %d rows=%d outside remaining rows=%d", role, blockIdx, blockRows, rows-rowOffset)
		}
		blockLimit := rowOffset + blockRows
		selectedInBlock := false
		for selectedIdx < len(selectedRows) {
			row := int(selectedRows[selectedIdx])
			if row <= previous {
				return nil, fmt.Errorf("collections: dense typed-column %s selected rows are not strictly ascending at index %d", role, selectedIdx)
			}
			if row < rowOffset {
				return nil, fmt.Errorf("collections: dense typed-column %s selected row %d precedes block offset %d", role, row, rowOffset)
			}
			if row >= blockLimit {
				break
			}
			selectedInBlock = true
			previous = row
			selectedIdx++
		}
		if selectedInBlock {
			mask[blockIdx] = typedColumnDensePredicateGranuleMayMatch(predicate, block.Granule, cardinality)
		}
		rowOffset = blockLimit
	}
	if rowOffset != rows {
		return nil, fmt.Errorf("collections: dense typed-column %s block rows=%d want part rows=%d", role, rowOffset, rows)
	}
	if selectedIdx != len(selectedRows) {
		return nil, fmt.Errorf("collections: dense typed-column %s selected row %d outside part rows=%d", role, selectedRows[selectedIdx], rows)
	}
	return mask, nil
}

func typedColumnDensePredicateGranuleMayMatch(predicate *columnTypedColumnDensePredicatePart, granule typedcolumn.EncodedGranule, cardinality int) bool {
	if predicate == nil {
		return true
	}
	if predicate.RejectsAll {
		return false
	}
	if predicate.MissingMatchesEmpty {
		return true
	}
	if !granule.HasMinMax {
		return true
	}
	minCode := granule.Min
	maxCode := granule.Max
	if minCode < 0 {
		minCode = 0
	}
	cardinalityMax := int64(cardinality) - 1
	if maxCode > cardinalityMax {
		maxCode = cardinalityMax
	}
	if maxCode < minCode || len(predicate.Allowed) == 0 {
		return false
	}
	startWord := int(minCode / 64)
	endWord := int(maxCode / 64)
	if startWord >= len(predicate.Allowed) {
		return false
	}
	if endWord >= len(predicate.Allowed) {
		endWord = len(predicate.Allowed) - 1
	}
	for word := startWord; word <= endWord; word++ {
		mask := ^uint64(0)
		if word == startWord {
			mask &= ^uint64(0) << uint(minCode%64)
		}
		if word == endWord {
			mask &= ^uint64(0) >> uint(63-maxCode%64)
		}
		if predicate.Allowed[word]&mask != 0 {
			return true
		}
	}
	return false
}

func decodeTypedColumnDenseUint32CodesSelectedRows(partColumn typedcolumn.ColumnPartColumn, cardinality int, selectedRows []uint32, rows int, role string) ([]uint32, uint64, int, error) {
	codes := make([]uint32, 0, len(selectedRows))
	var scratch []uint32
	var reader typedcolumn.GranuleReader
	decodedBytes := uint64(0)
	decodedBlocks := 0
	selectedIdx := 0
	rowOffset := 0
	previous := -1
	for blockIdx, block := range partColumn.Blocks {
		blockRows := block.Descriptor.RowCount
		if blockRows < 0 || blockRows > rows-rowOffset {
			return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d rows=%d outside remaining rows=%d", role, blockIdx, blockRows, rows-rowOffset)
		}
		blockLimit := rowOffset + blockRows
		if selectedIdx >= len(selectedRows) || int(selectedRows[selectedIdx]) >= blockLimit {
			rowOffset = blockLimit
			continue
		}
		g := block.Granule
		if g.HasMinMax {
			if g.Min < 0 || g.Max < 0 || g.Min > g.Max || uint64(g.Max) >= uint64(cardinality) {
				return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d min/max [%d,%d] outside cardinality %d", role, blockIdx, g.Min, g.Max, cardinality)
			}
		}
		decoded, err := reader.DecodeUint32CodesInto(scratch[:0], g)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d: %w", role, blockIdx, err)
		}
		if len(decoded) != blockRows {
			return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d decoded rows=%d want %d", role, blockIdx, len(decoded), blockRows)
		}
		for selectedIdx < len(selectedRows) {
			row := int(selectedRows[selectedIdx])
			if row <= previous {
				return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s selected rows are not strictly ascending at index %d", role, selectedIdx)
			}
			if row < rowOffset {
				return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s selected row %d precedes block offset %d", role, row, rowOffset)
			}
			if row >= blockLimit {
				break
			}
			code := decoded[row-rowOffset]
			if uint64(code) >= uint64(cardinality) {
				return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s code[%d]=%d outside cardinality=%d", role, row, code, cardinality)
			}
			codes = append(codes, code)
			previous = row
			selectedIdx++
		}
		scratch = decoded
		decodedBytes += uint64(g.RawBytes)
		decodedBlocks++
		rowOffset = blockLimit
	}
	if rowOffset != rows {
		return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block rows=%d want part rows=%d", role, rowOffset, rows)
	}
	if len(codes) != len(selectedRows) {
		return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s decoded selected rows=%d want %d", role, len(codes), len(selectedRows))
	}
	return codes, decodedBytes, decodedBlocks, nil
}

func decodeTypedColumnDenseUint32CodesSelectedRowsMatchingPredicate(partColumn typedcolumn.ColumnPartColumn, cardinality int, selectedRows []uint32, rows int, predicate *columnTypedColumnDensePredicatePart, selectedBlocks []bool, role string) ([]uint32, []uint32, uint64, int, error) {
	codes := make([]uint32, 0, len(selectedRows))
	filteredRows := make([]uint32, 0, len(selectedRows))
	var scratch []uint32
	var reader typedcolumn.GranuleReader
	decodedBytes := uint64(0)
	decodedBlocks := 0
	selectedIdx := 0
	rowOffset := 0
	previous := -1
	for blockIdx, block := range partColumn.Blocks {
		blockRows := block.Descriptor.RowCount
		if blockRows < 0 || blockRows > rows-rowOffset {
			return nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d rows=%d outside remaining rows=%d", role, blockIdx, blockRows, rows-rowOffset)
		}
		blockLimit := rowOffset + blockRows
		if selectedIdx >= len(selectedRows) || int(selectedRows[selectedIdx]) >= blockLimit {
			rowOffset = blockLimit
			continue
		}
		blockSelectedStart := selectedIdx
		for selectedIdx < len(selectedRows) {
			row := int(selectedRows[selectedIdx])
			if row <= previous {
				return nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s selected rows are not strictly ascending at index %d", role, selectedIdx)
			}
			if row < rowOffset {
				return nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s selected row %d precedes block offset %d", role, row, rowOffset)
			}
			if row >= blockLimit {
				break
			}
			previous = row
			selectedIdx++
		}
		if selectedBlocks != nil {
			if len(selectedBlocks) != len(partColumn.Blocks) {
				return nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s selected blocks=%d want %d", role, len(selectedBlocks), len(partColumn.Blocks))
			}
			if !selectedBlocks[blockIdx] {
				rowOffset = blockLimit
				continue
			}
		}
		g := block.Granule
		if g.HasMinMax {
			if g.Min < 0 || g.Max < 0 || g.Min > g.Max || uint64(g.Max) >= uint64(cardinality) {
				return nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d min/max [%d,%d] outside cardinality %d", role, blockIdx, g.Min, g.Max, cardinality)
			}
		}
		decoded, err := reader.DecodeUint32CodesInto(scratch[:0], g)
		if err != nil {
			return nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d: %w", role, blockIdx, err)
		}
		if len(decoded) != blockRows {
			return nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d decoded rows=%d want %d", role, blockIdx, len(decoded), blockRows)
		}
		for idx := blockSelectedStart; idx < selectedIdx; idx++ {
			row := int(selectedRows[idx])
			code := decoded[row-rowOffset]
			if uint64(code) >= uint64(cardinality) {
				return nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s code[%d]=%d outside cardinality=%d", role, row, code, cardinality)
			}
			if !columnTypedColumnDensePredicateAllowsCode(predicate, code, true) {
				continue
			}
			codes = append(codes, code)
			filteredRows = append(filteredRows, selectedRows[idx])
		}
		scratch = decoded
		decodedBytes += uint64(g.RawBytes)
		decodedBlocks++
		rowOffset = blockLimit
	}
	if rowOffset != rows {
		return nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block rows=%d want part rows=%d", role, rowOffset, rows)
	}
	if selectedIdx != len(selectedRows) {
		return nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s selected row %d outside part rows=%d", role, selectedRows[selectedIdx], rows)
	}
	return codes, filteredRows, decodedBytes, decodedBlocks, nil
}

func decodeTypedColumnDenseNullableUint32CodesSelectedRowsMatchingPredicate(partColumn typedcolumn.ColumnPartColumn, cardinality int, selectedRows []uint32, rows int, predicate *columnTypedColumnDensePredicatePart, selectedBlocks []bool, role string) ([]uint32, []bool, []uint32, uint64, int, error) {
	codes := make([]uint32, 0, len(selectedRows))
	var valid []bool
	if predicate != nil && predicate.MissingMatchesEmpty {
		valid = make([]bool, 0, len(selectedRows))
	}
	filteredRows := make([]uint32, 0, len(selectedRows))
	var valueScratch []int64
	var nullScratch []bool
	var defaultScratch []bool
	var reader typedcolumn.GranuleReader
	decodedBytes := uint64(0)
	decodedBlocks := 0
	selectedIdx := 0
	rowOffset := 0
	previous := -1
	for blockIdx, block := range partColumn.Blocks {
		blockRows := block.Descriptor.RowCount
		if blockRows < 0 || blockRows > rows-rowOffset {
			return nil, nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d rows=%d outside remaining rows=%d", role, blockIdx, blockRows, rows-rowOffset)
		}
		blockLimit := rowOffset + blockRows
		if selectedIdx >= len(selectedRows) || int(selectedRows[selectedIdx]) >= blockLimit {
			rowOffset = blockLimit
			continue
		}
		blockSelectedStart := selectedIdx
		for selectedIdx < len(selectedRows) {
			row := int(selectedRows[selectedIdx])
			if row <= previous {
				return nil, nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s selected rows are not strictly ascending at index %d", role, selectedIdx)
			}
			if row < rowOffset {
				return nil, nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s selected row %d precedes block offset %d", role, row, rowOffset)
			}
			if row >= blockLimit {
				break
			}
			previous = row
			selectedIdx++
		}
		if selectedBlocks != nil {
			if len(selectedBlocks) != len(partColumn.Blocks) {
				return nil, nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s selected blocks=%d want %d", role, len(selectedBlocks), len(partColumn.Blocks))
			}
			if !selectedBlocks[blockIdx] {
				rowOffset = blockLimit
				continue
			}
		}
		g := block.Granule
		if g.HasMinMax {
			if g.Min < 0 || g.Max < 0 || g.Min > g.Max || uint64(g.Max) >= uint64(cardinality) {
				return nil, nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d min/max [%d,%d] outside cardinality %d", role, blockIdx, g.Min, g.Max, cardinality)
			}
		}
		values, nulls, defaults, err := reader.DecodeNullableInt64Into(valueScratch[:0], nullScratch[:0], defaultScratch[:0], g)
		if err != nil {
			return nil, nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d: %w", role, blockIdx, err)
		}
		if len(values) != blockRows || len(nulls) != blockRows || len(defaults) != blockRows {
			return nil, nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d decoded rows values/nulls/defaults=%d/%d/%d want %d", role, blockIdx, len(values), len(nulls), len(defaults), blockRows)
		}
		for idx := blockSelectedStart; idx < selectedIdx; idx++ {
			row := int(selectedRows[idx])
			local := row - rowOffset
			value := values[local]
			if nulls[local] || defaults[local] {
				if !columnTypedColumnDensePredicateAllowsCode(predicate, 0, false) {
					continue
				}
				codes = append(codes, 0)
				filteredRows = append(filteredRows, selectedRows[idx])
				if valid != nil {
					valid = append(valid, false)
				}
				continue
			}
			if value < 0 || uint64(value) >= uint64(cardinality) || value > int64(^uint32(0)) {
				return nil, nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s code[%d]=%d outside cardinality=%d", role, row, value, cardinality)
			}
			code := uint32(value)
			if !columnTypedColumnDensePredicateAllowsCode(predicate, code, true) {
				continue
			}
			codes = append(codes, code)
			filteredRows = append(filteredRows, selectedRows[idx])
			if valid != nil {
				valid = append(valid, true)
			}
		}
		valueScratch = values
		nullScratch = nulls
		defaultScratch = defaults
		decodedBytes += uint64(g.RawBytes)
		decodedBlocks++
		rowOffset = blockLimit
	}
	if rowOffset != rows {
		return nil, nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block rows=%d want part rows=%d", role, rowOffset, rows)
	}
	if selectedIdx != len(selectedRows) {
		return nil, nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s selected row %d outside part rows=%d", role, selectedRows[selectedIdx], rows)
	}
	if valid != nil && len(valid) != len(codes) {
		return nil, nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s decoded filtered rows codes/valid=%d/%d", role, len(codes), len(valid))
	}
	return codes, valid, filteredRows, decodedBytes, decodedBlocks, nil
}

func decodeTypedColumnDenseNullableUint32CodesSelectedRows(partColumn typedcolumn.ColumnPartColumn, cardinality int, selectedRows []uint32, rows int, role string) ([]uint32, []bool, uint64, int, error) {
	codes := make([]uint32, 0, len(selectedRows))
	valid := make([]bool, 0, len(selectedRows))
	var valueScratch []int64
	var nullScratch []bool
	var defaultScratch []bool
	var reader typedcolumn.GranuleReader
	decodedBytes := uint64(0)
	decodedBlocks := 0
	selectedIdx := 0
	rowOffset := 0
	previous := -1
	for blockIdx, block := range partColumn.Blocks {
		blockRows := block.Descriptor.RowCount
		if blockRows < 0 || blockRows > rows-rowOffset {
			return nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d rows=%d outside remaining rows=%d", role, blockIdx, blockRows, rows-rowOffset)
		}
		blockLimit := rowOffset + blockRows
		if selectedIdx >= len(selectedRows) || int(selectedRows[selectedIdx]) >= blockLimit {
			rowOffset = blockLimit
			continue
		}
		g := block.Granule
		if g.HasMinMax {
			if g.Min < 0 || g.Max < 0 || g.Min > g.Max || uint64(g.Max) >= uint64(cardinality) {
				return nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d min/max [%d,%d] outside cardinality %d", role, blockIdx, g.Min, g.Max, cardinality)
			}
		}
		values, nulls, defaults, err := reader.DecodeNullableInt64Into(valueScratch[:0], nullScratch[:0], defaultScratch[:0], g)
		if err != nil {
			return nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d: %w", role, blockIdx, err)
		}
		if len(values) != blockRows || len(nulls) != blockRows || len(defaults) != blockRows {
			return nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d decoded rows values/nulls/defaults=%d/%d/%d want %d", role, blockIdx, len(values), len(nulls), len(defaults), blockRows)
		}
		for selectedIdx < len(selectedRows) {
			row := int(selectedRows[selectedIdx])
			if row <= previous {
				return nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s selected rows are not strictly ascending at index %d", role, selectedIdx)
			}
			if row < rowOffset {
				return nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s selected row %d precedes block offset %d", role, row, rowOffset)
			}
			if row >= blockLimit {
				break
			}
			local := row - rowOffset
			value := values[local]
			if nulls[local] || defaults[local] {
				codes = append(codes, 0)
				valid = append(valid, false)
			} else {
				if value < 0 || uint64(value) >= uint64(cardinality) || value > int64(^uint32(0)) {
					return nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s code[%d]=%d outside cardinality=%d", role, row, value, cardinality)
				}
				codes = append(codes, uint32(value))
				valid = append(valid, true)
			}
			previous = row
			selectedIdx++
		}
		valueScratch = values
		nullScratch = nulls
		defaultScratch = defaults
		decodedBytes += uint64(g.RawBytes)
		decodedBlocks++
		rowOffset = blockLimit
	}
	if rowOffset != rows {
		return nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block rows=%d want part rows=%d", role, rowOffset, rows)
	}
	if len(codes) != len(selectedRows) || len(valid) != len(selectedRows) {
		return nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s decoded selected rows codes/valid=%d/%d want %d", role, len(codes), len(valid), len(selectedRows))
	}
	return codes, valid, decodedBytes, decodedBlocks, nil
}

func decodeTypedColumnDenseInt64ValuesSelectedRows(partColumn typedcolumn.ColumnPartColumn, selectedRows []uint32, rows int, role string) ([]int64, uint64, int, error) {
	values := make([]int64, 0, len(selectedRows))
	var scratch []int64
	var reader typedcolumn.GranuleReader
	decodedBytes := uint64(0)
	decodedBlocks := 0
	selectedIdx := 0
	rowOffset := 0
	previous := -1
	for blockIdx, block := range partColumn.Blocks {
		blockRows := block.Descriptor.RowCount
		if blockRows < 0 || blockRows > rows-rowOffset {
			return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d rows=%d outside remaining rows=%d", role, blockIdx, blockRows, rows-rowOffset)
		}
		blockLimit := rowOffset + blockRows
		if selectedIdx >= len(selectedRows) || int(selectedRows[selectedIdx]) >= blockLimit {
			rowOffset = blockLimit
			continue
		}
		decoded, err := reader.DecodeInt64Into(scratch[:0], block.Granule)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d: %w", role, blockIdx, err)
		}
		if len(decoded) != blockRows {
			return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d decoded rows=%d want %d", role, blockIdx, len(decoded), blockRows)
		}
		for selectedIdx < len(selectedRows) {
			row := int(selectedRows[selectedIdx])
			if row <= previous {
				return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s selected rows are not strictly ascending at index %d", role, selectedIdx)
			}
			if row < rowOffset {
				return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s selected row %d precedes block offset %d", role, row, rowOffset)
			}
			if row >= blockLimit {
				break
			}
			values = append(values, decoded[row-rowOffset])
			previous = row
			selectedIdx++
		}
		scratch = decoded
		decodedBytes += uint64(block.Granule.RawBytes)
		decodedBlocks++
		rowOffset = blockLimit
	}
	if rowOffset != rows {
		return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block rows=%d want part rows=%d", role, rowOffset, rows)
	}
	if len(values) != len(selectedRows) {
		return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s decoded selected rows=%d want %d", role, len(values), len(selectedRows))
	}
	return values, decodedBytes, decodedBlocks, nil
}

func preapplyColumnTypedColumnDenseInt64SpanPredicates(dense *columnTypedColumnDenseInt64SpanPart, rows int) {
	if dense == nil || len(dense.Predicates) == 0 || rows <= 0 {
		return
	}
	if uint64(rows) > uint64(^uint32(0)) {
		return
	}
	dense.PredicatesPreApplied = true
	dense.PreAppliedRowsScanned = rows
	if columnTypedColumnDensePredicatesRejectAll(dense.Predicates) {
		return
	}
	if preapplyColumnTypedColumnDenseInt64SpanSingleCodeTriplePredicates(dense, rows) {
		return
	}
	selected := make([]uint32, 0, min(rows, 4096))
	for rowIdx := 0; rowIdx < rows; rowIdx++ {
		if columnTypedColumnDensePredicatesMatch(dense.Predicates, rowIdx) {
			selected = append(selected, uint32(rowIdx))
		}
	}
	dense.PredicateRows = selected
}

func preapplyColumnTypedColumnDenseInt64SpanSingleCodeTriplePredicates(dense *columnTypedColumnDenseInt64SpanPart, rows int) bool {
	if dense == nil || len(dense.Predicates) != 3 {
		return false
	}
	left := &dense.Predicates[0]
	mid := &dense.Predicates[1]
	right := &dense.Predicates[2]
	if !columnTypedColumnDensePredicateCanUseSingleCodePreapply(left, rows) ||
		!columnTypedColumnDensePredicateCanUseSingleCodePreapply(mid, rows) ||
		!columnTypedColumnDensePredicateCanUseSingleCodePreapply(right, rows) {
		return false
	}
	leftCode := left.SingleCode
	midCode := mid.SingleCode
	rightCode := right.SingleCode
	leftValid := left.Valid
	midValid := mid.Valid
	rightValid := right.Valid
	selected := make([]uint32, 0, min(rows, 4096))
	for rowIdx := 0; rowIdx < rows; rowIdx++ {
		if (leftValid == nil || leftValid[rowIdx]) &&
			(midValid == nil || midValid[rowIdx]) &&
			(rightValid == nil || rightValid[rowIdx]) &&
			left.Codes[rowIdx] == leftCode &&
			mid.Codes[rowIdx] == midCode &&
			right.Codes[rowIdx] == rightCode {
			selected = append(selected, uint32(rowIdx))
		}
	}
	dense.PredicateRows = selected
	return true
}

func columnTypedColumnDensePredicateCanUseSingleCodePreapply(predicate *columnTypedColumnDensePredicatePart, rows int) bool {
	if predicate == nil || predicate.RejectsAll || !predicate.SingleCodeAllowed || predicate.MissingMatchesEmpty {
		return false
	}
	return rows >= 0 && len(predicate.Codes) >= rows && (predicate.Valid == nil || len(predicate.Valid) >= rows)
}

type columnTypedColumnDenseSingleCodePredicateCandidate struct {
	partColumn  typedcolumn.ColumnPartColumn
	cardinality int
	nullable    bool
	predicate   columnTypedColumnDensePredicatePart
}

func decodeTypedColumnDenseInt64SpanSingleCodeTriplePredicateRows(adapterPart *typedColumnAdapterPart, fields []TypedStorageField, specs []columnPhysicalQueryPredicateSpec, rows int) ([]columnTypedColumnDensePredicatePart, []uint32, uint64, int, bool, error) {
	candidates, predicates, ok, err := typedColumnDenseInt64SpanSingleCodeTriplePredicateCandidates(adapterPart, fields, specs, rows)
	if err != nil || !ok {
		return nil, nil, 0, 0, ok, err
	}
	return decodeTypedColumnDenseInt64SpanSingleCodeTriplePredicateRowsFromCandidates(candidates, predicates, rows, nil)
}

func typedColumnDenseInt64SpanSingleCodeTriplePredicateCandidates(adapterPart *typedColumnAdapterPart, fields []TypedStorageField, specs []columnPhysicalQueryPredicateSpec, rows int) ([]columnTypedColumnDenseSingleCodePredicateCandidate, []columnTypedColumnDensePredicatePart, bool, error) {
	if rows <= 0 || uint64(rows) > uint64(^uint32(0)) || len(specs) != 3 {
		return nil, nil, false, nil
	}
	candidates := make([]columnTypedColumnDenseSingleCodePredicateCandidate, len(specs))
	predicates := make([]columnTypedColumnDensePredicatePart, len(specs))
	for idx, spec := range specs {
		candidate, ok, err := columnTypedColumnDenseSingleCodePredicateCandidateForSpec(adapterPart, fields, spec)
		if err != nil {
			return nil, nil, false, err
		}
		if !ok {
			return nil, nil, false, nil
		}
		candidates[idx] = candidate
		predicates[idx] = candidate.predicate
	}
	if !columnTypedColumnDenseSingleCodePredicateBlocksAligned(candidates, rows) {
		return nil, nil, false, nil
	}
	return candidates, predicates, true, nil
}

func typedColumnDenseSingleCodePairPredicateCandidates(adapterPart *typedColumnAdapterPart, fields []TypedStorageField, specs []columnPhysicalQueryPredicateSpec, rows int) ([]columnTypedColumnDenseSingleCodePredicateCandidate, []columnTypedColumnDensePredicatePart, bool, error) {
	if rows <= 0 || uint64(rows) > uint64(^uint32(0)) || len(specs) != 2 {
		return nil, nil, false, nil
	}
	candidates := make([]columnTypedColumnDenseSingleCodePredicateCandidate, len(specs))
	predicates := make([]columnTypedColumnDensePredicatePart, len(specs))
	for idx, spec := range specs {
		candidate, ok, err := columnTypedColumnDenseSingleCodePredicateCandidateForSpec(adapterPart, fields, spec)
		if err != nil {
			return nil, nil, false, err
		}
		if !ok {
			return nil, nil, false, nil
		}
		candidates[idx] = candidate
		predicates[idx] = candidate.predicate
	}
	if !columnTypedColumnDenseSingleCodePredicateBlocksAligned(candidates, rows) {
		return nil, nil, false, nil
	}
	return candidates, predicates, true, nil
}

func decodeTypedColumnDenseInt64SpanSingleCodeTriplePredicateRowsFromCandidates(candidates []columnTypedColumnDenseSingleCodePredicateCandidate, predicates []columnTypedColumnDensePredicatePart, rows int, selectedBlocks []bool) ([]columnTypedColumnDensePredicatePart, []uint32, uint64, int, bool, error) {
	if rows <= 0 || uint64(rows) > uint64(^uint32(0)) || len(candidates) != 3 || len(predicates) != len(candidates) {
		return nil, nil, 0, 0, false, nil
	}
	if !columnTypedColumnDenseSingleCodePredicateBlocksAligned(candidates, rows) {
		return nil, nil, 0, 0, false, nil
	}
	if selectedBlocks != nil && len(selectedBlocks) != len(candidates[0].partColumn.Blocks) {
		return nil, nil, 0, 0, false, fmt.Errorf("collections: dense typed-column predicate selected blocks=%d want %d", len(selectedBlocks), len(candidates[0].partColumn.Blocks))
	}
	selected := make([]uint32, 0, min(rows, 4096))
	var readers [3]typedcolumn.GranuleReader
	var codeScratch [3][]uint32
	var valueScratch [3][]int64
	var nullScratch [3][]bool
	var defaultScratch [3][]bool
	decodedBytes := uint64(0)
	decodedBlocks := 0
	rowOffset := 0
	for blockIdx := range candidates[0].partColumn.Blocks {
		blockRows := candidates[0].partColumn.Blocks[blockIdx].Descriptor.RowCount
		if selectedBlocks != nil && !selectedBlocks[blockIdx] {
			rowOffset += blockRows
			continue
		}
		for predicateIdx := range candidates {
			block := candidates[predicateIdx].partColumn.Blocks[blockIdx]
			g := block.Granule
			if err := validateTypedColumnDenseSingleCodePredicateGranuleBounds(g, candidates[predicateIdx].cardinality, candidates[predicateIdx].partColumn.Definition.Name, blockIdx); err != nil {
				return nil, nil, 0, 0, false, err
			}
			if candidates[predicateIdx].nullable {
				values, nulls, defaults, err := readers[predicateIdx].DecodeNullableInt64Into(valueScratch[predicateIdx][:0], nullScratch[predicateIdx][:0], defaultScratch[predicateIdx][:0], g)
				if err != nil {
					return nil, nil, 0, 0, false, fmt.Errorf("collections: dense typed-column predicate column %q block %d: %w", candidates[predicateIdx].partColumn.Definition.Name, blockIdx, err)
				}
				if len(values) != blockRows || len(nulls) != blockRows || len(defaults) != blockRows {
					return nil, nil, 0, 0, false, fmt.Errorf("collections: dense typed-column predicate column %q block %d decoded rows values/nulls/defaults=%d/%d/%d want %d", candidates[predicateIdx].partColumn.Definition.Name, blockIdx, len(values), len(nulls), len(defaults), blockRows)
				}
				valueScratch[predicateIdx] = values
				nullScratch[predicateIdx] = nulls
				defaultScratch[predicateIdx] = defaults
			} else {
				decoded, err := readers[predicateIdx].DecodeUint32CodesInto(codeScratch[predicateIdx][:0], g)
				if err != nil {
					return nil, nil, 0, 0, false, fmt.Errorf("collections: dense typed-column predicate column %q block %d: %w", candidates[predicateIdx].partColumn.Definition.Name, blockIdx, err)
				}
				if len(decoded) != blockRows {
					return nil, nil, 0, 0, false, fmt.Errorf("collections: dense typed-column predicate column %q block %d decoded rows=%d want %d", candidates[predicateIdx].partColumn.Definition.Name, blockIdx, len(decoded), blockRows)
				}
				codeScratch[predicateIdx] = decoded
			}
			decodedBytes += uint64(g.RawBytes)
			decodedBlocks++
		}
		var err error
		selected, err = appendTypedColumnDenseSingleCodeTriplePredicateRows(selected, candidates, codeScratch, valueScratch, nullScratch, defaultScratch, rowOffset, blockRows)
		if err != nil {
			return nil, nil, 0, 0, false, err
		}
		rowOffset += blockRows
	}
	if rowOffset != rows {
		return nil, nil, 0, 0, false, fmt.Errorf("collections: dense typed-column predicate decoded rows=%d want part rows=%d", rowOffset, rows)
	}
	return predicates, selected, decodedBytes, decodedBlocks, true, nil
}

func decodeTypedColumnDenseSingleCodePairPredicateRowsFromCandidates(candidates []columnTypedColumnDenseSingleCodePredicateCandidate, predicates []columnTypedColumnDensePredicatePart, rows int, selectedBlocks []bool) ([]columnTypedColumnDensePredicatePart, []uint32, uint64, int, bool, error) {
	if rows <= 0 || uint64(rows) > uint64(^uint32(0)) || len(candidates) != 2 || len(predicates) != len(candidates) {
		return nil, nil, 0, 0, false, nil
	}
	if !columnTypedColumnDenseSingleCodePredicateBlocksAligned(candidates, rows) {
		return nil, nil, 0, 0, false, nil
	}
	if selectedBlocks != nil && len(selectedBlocks) != len(candidates[0].partColumn.Blocks) {
		return nil, nil, 0, 0, false, fmt.Errorf("collections: dense typed-column predicate selected blocks=%d want %d", len(selectedBlocks), len(candidates[0].partColumn.Blocks))
	}
	selected := make([]uint32, 0, min(rows, 4096))
	var readers [2]typedcolumn.GranuleReader
	var codeScratch [2][]uint32
	var valueScratch [2][]int64
	var nullScratch [2][]bool
	var defaultScratch [2][]bool
	decodedBytes := uint64(0)
	decodedBlocks := 0
	rowOffset := 0
	for blockIdx := range candidates[0].partColumn.Blocks {
		blockRows := candidates[0].partColumn.Blocks[blockIdx].Descriptor.RowCount
		if selectedBlocks != nil && !selectedBlocks[blockIdx] {
			rowOffset += blockRows
			continue
		}
		for predicateIdx := range candidates {
			block := candidates[predicateIdx].partColumn.Blocks[blockIdx]
			g := block.Granule
			if err := validateTypedColumnDenseSingleCodePredicateGranuleBounds(g, candidates[predicateIdx].cardinality, candidates[predicateIdx].partColumn.Definition.Name, blockIdx); err != nil {
				return nil, nil, 0, 0, false, err
			}
			if candidates[predicateIdx].nullable {
				values, nulls, defaults, err := readers[predicateIdx].DecodeNullableInt64Into(valueScratch[predicateIdx][:0], nullScratch[predicateIdx][:0], defaultScratch[predicateIdx][:0], g)
				if err != nil {
					return nil, nil, 0, 0, false, fmt.Errorf("collections: dense typed-column predicate column %q block %d: %w", candidates[predicateIdx].partColumn.Definition.Name, blockIdx, err)
				}
				if len(values) != blockRows || len(nulls) != blockRows || len(defaults) != blockRows {
					return nil, nil, 0, 0, false, fmt.Errorf("collections: dense typed-column predicate column %q block %d decoded rows values/nulls/defaults=%d/%d/%d want %d", candidates[predicateIdx].partColumn.Definition.Name, blockIdx, len(values), len(nulls), len(defaults), blockRows)
				}
				valueScratch[predicateIdx] = values
				nullScratch[predicateIdx] = nulls
				defaultScratch[predicateIdx] = defaults
			} else {
				decoded, err := readers[predicateIdx].DecodeUint32CodesInto(codeScratch[predicateIdx][:0], g)
				if err != nil {
					return nil, nil, 0, 0, false, fmt.Errorf("collections: dense typed-column predicate column %q block %d: %w", candidates[predicateIdx].partColumn.Definition.Name, blockIdx, err)
				}
				if len(decoded) != blockRows {
					return nil, nil, 0, 0, false, fmt.Errorf("collections: dense typed-column predicate column %q block %d decoded rows=%d want %d", candidates[predicateIdx].partColumn.Definition.Name, blockIdx, len(decoded), blockRows)
				}
				codeScratch[predicateIdx] = decoded
			}
			decodedBytes += uint64(g.RawBytes)
			decodedBlocks++
		}
		var err error
		selected, err = appendTypedColumnDenseSingleCodePairPredicateRows(selected, candidates, codeScratch, valueScratch, nullScratch, defaultScratch, rowOffset, blockRows)
		if err != nil {
			return nil, nil, 0, 0, false, err
		}
		rowOffset += blockRows
	}
	if rowOffset != rows {
		return nil, nil, 0, 0, false, fmt.Errorf("collections: dense typed-column predicate decoded rows=%d want part rows=%d", rowOffset, rows)
	}
	return predicates, selected, decodedBytes, decodedBlocks, true, nil
}

func appendTypedColumnDenseSingleCodeTriplePredicateRows(selected []uint32, candidates []columnTypedColumnDenseSingleCodePredicateCandidate, codeScratch [3][]uint32, valueScratch [3][]int64, nullScratch [3][]bool, defaultScratch [3][]bool, rowOffset, blockRows int) ([]uint32, error) {
	leftCode := candidates[0].predicate.SingleCode
	midCode := candidates[1].predicate.SingleCode
	rightCode := candidates[2].predicate.SingleCode
	if candidates[0].nullable && candidates[1].nullable && candidates[2].nullable {
		leftValues := valueScratch[0]
		midValues := valueScratch[1]
		rightValues := valueScratch[2]
		leftNulls := nullScratch[0]
		midNulls := nullScratch[1]
		rightNulls := nullScratch[2]
		leftDefaults := defaultScratch[0]
		midDefaults := defaultScratch[1]
		rightDefaults := defaultScratch[2]
		leftCardinality := candidates[0].cardinality
		midCardinality := candidates[1].cardinality
		rightCardinality := candidates[2].cardinality
		for row := 0; row < blockRows; row++ {
			leftValue := leftValues[row]
			midValue := midValues[row]
			rightValue := rightValues[row]
			if !leftNulls[row] && !leftDefaults[row] && (leftValue < 0 || uint64(leftValue) >= uint64(leftCardinality) || leftValue > int64(^uint32(0))) {
				return nil, fmt.Errorf("collections: dense typed-column predicate column %q code[%d]=%d outside cardinality=%d", candidates[0].partColumn.Definition.Name, rowOffset+row, leftValue, leftCardinality)
			}
			if !midNulls[row] && !midDefaults[row] && (midValue < 0 || uint64(midValue) >= uint64(midCardinality) || midValue > int64(^uint32(0))) {
				return nil, fmt.Errorf("collections: dense typed-column predicate column %q code[%d]=%d outside cardinality=%d", candidates[1].partColumn.Definition.Name, rowOffset+row, midValue, midCardinality)
			}
			if !rightNulls[row] && !rightDefaults[row] && (rightValue < 0 || uint64(rightValue) >= uint64(rightCardinality) || rightValue > int64(^uint32(0))) {
				return nil, fmt.Errorf("collections: dense typed-column predicate column %q code[%d]=%d outside cardinality=%d", candidates[2].partColumn.Definition.Name, rowOffset+row, rightValue, rightCardinality)
			}
			if !leftNulls[row] && !leftDefaults[row] && uint32(leftValue) == leftCode &&
				!midNulls[row] && !midDefaults[row] && uint32(midValue) == midCode &&
				!rightNulls[row] && !rightDefaults[row] && uint32(rightValue) == rightCode {
				selected = append(selected, uint32(rowOffset+row))
			}
		}
		return selected, nil
	}
	if !candidates[0].nullable && !candidates[1].nullable && !candidates[2].nullable {
		left := codeScratch[0]
		mid := codeScratch[1]
		right := codeScratch[2]
		leftCardinality := candidates[0].cardinality
		midCardinality := candidates[1].cardinality
		rightCardinality := candidates[2].cardinality
		for row := 0; row < blockRows; row++ {
			leftValue := left[row]
			midValue := mid[row]
			rightValue := right[row]
			if uint64(leftValue) >= uint64(leftCardinality) {
				return nil, fmt.Errorf("collections: dense typed-column predicate column %q code[%d]=%d outside cardinality=%d", candidates[0].partColumn.Definition.Name, rowOffset+row, leftValue, leftCardinality)
			}
			if uint64(midValue) >= uint64(midCardinality) {
				return nil, fmt.Errorf("collections: dense typed-column predicate column %q code[%d]=%d outside cardinality=%d", candidates[1].partColumn.Definition.Name, rowOffset+row, midValue, midCardinality)
			}
			if uint64(rightValue) >= uint64(rightCardinality) {
				return nil, fmt.Errorf("collections: dense typed-column predicate column %q code[%d]=%d outside cardinality=%d", candidates[2].partColumn.Definition.Name, rowOffset+row, rightValue, rightCardinality)
			}
			if leftValue == leftCode && midValue == midCode && rightValue == rightCode {
				selected = append(selected, uint32(rowOffset+row))
			}
		}
		return selected, nil
	}
	for row := 0; row < blockRows; row++ {
		leftMatch, err := columnTypedColumnDenseSingleCodePredicateCandidateRowMatches(candidates[0], row, rowOffset, leftCode, codeScratch[0], valueScratch[0], nullScratch[0], defaultScratch[0])
		if err != nil {
			return nil, err
		}
		midMatch, err := columnTypedColumnDenseSingleCodePredicateCandidateRowMatches(candidates[1], row, rowOffset, midCode, codeScratch[1], valueScratch[1], nullScratch[1], defaultScratch[1])
		if err != nil {
			return nil, err
		}
		rightMatch, err := columnTypedColumnDenseSingleCodePredicateCandidateRowMatches(candidates[2], row, rowOffset, rightCode, codeScratch[2], valueScratch[2], nullScratch[2], defaultScratch[2])
		if err != nil {
			return nil, err
		}
		if leftMatch && midMatch && rightMatch {
			selected = append(selected, uint32(rowOffset+row))
		}
	}
	return selected, nil
}

func appendTypedColumnDenseSingleCodePairPredicateRows(selected []uint32, candidates []columnTypedColumnDenseSingleCodePredicateCandidate, codeScratch [2][]uint32, valueScratch [2][]int64, nullScratch [2][]bool, defaultScratch [2][]bool, rowOffset, blockRows int) ([]uint32, error) {
	leftCode := candidates[0].predicate.SingleCode
	rightCode := candidates[1].predicate.SingleCode
	if candidates[0].nullable && candidates[1].nullable {
		leftValues := valueScratch[0]
		rightValues := valueScratch[1]
		leftNulls := nullScratch[0]
		rightNulls := nullScratch[1]
		leftDefaults := defaultScratch[0]
		rightDefaults := defaultScratch[1]
		leftCardinality := candidates[0].cardinality
		rightCardinality := candidates[1].cardinality
		for row := 0; row < blockRows; row++ {
			leftValue := leftValues[row]
			rightValue := rightValues[row]
			if !leftNulls[row] && !leftDefaults[row] && (leftValue < 0 || uint64(leftValue) >= uint64(leftCardinality) || leftValue > int64(^uint32(0))) {
				return nil, fmt.Errorf("collections: dense typed-column predicate column %q code[%d]=%d outside cardinality=%d", candidates[0].partColumn.Definition.Name, rowOffset+row, leftValue, leftCardinality)
			}
			if !rightNulls[row] && !rightDefaults[row] && (rightValue < 0 || uint64(rightValue) >= uint64(rightCardinality) || rightValue > int64(^uint32(0))) {
				return nil, fmt.Errorf("collections: dense typed-column predicate column %q code[%d]=%d outside cardinality=%d", candidates[1].partColumn.Definition.Name, rowOffset+row, rightValue, rightCardinality)
			}
			if !leftNulls[row] && !leftDefaults[row] && uint32(leftValue) == leftCode &&
				!rightNulls[row] && !rightDefaults[row] && uint32(rightValue) == rightCode {
				selected = append(selected, uint32(rowOffset+row))
			}
		}
		return selected, nil
	}
	if !candidates[0].nullable && !candidates[1].nullable {
		left := codeScratch[0]
		right := codeScratch[1]
		leftCardinality := candidates[0].cardinality
		rightCardinality := candidates[1].cardinality
		for row := 0; row < blockRows; row++ {
			leftValue := left[row]
			rightValue := right[row]
			if uint64(leftValue) >= uint64(leftCardinality) {
				return nil, fmt.Errorf("collections: dense typed-column predicate column %q code[%d]=%d outside cardinality=%d", candidates[0].partColumn.Definition.Name, rowOffset+row, leftValue, leftCardinality)
			}
			if uint64(rightValue) >= uint64(rightCardinality) {
				return nil, fmt.Errorf("collections: dense typed-column predicate column %q code[%d]=%d outside cardinality=%d", candidates[1].partColumn.Definition.Name, rowOffset+row, rightValue, rightCardinality)
			}
			if leftValue == leftCode && rightValue == rightCode {
				selected = append(selected, uint32(rowOffset+row))
			}
		}
		return selected, nil
	}
	for row := 0; row < blockRows; row++ {
		leftMatch, err := columnTypedColumnDenseSingleCodePredicateCandidateRowMatches(candidates[0], row, rowOffset, leftCode, codeScratch[0], valueScratch[0], nullScratch[0], defaultScratch[0])
		if err != nil {
			return nil, err
		}
		rightMatch, err := columnTypedColumnDenseSingleCodePredicateCandidateRowMatches(candidates[1], row, rowOffset, rightCode, codeScratch[1], valueScratch[1], nullScratch[1], defaultScratch[1])
		if err != nil {
			return nil, err
		}
		if leftMatch && rightMatch {
			selected = append(selected, uint32(rowOffset+row))
		}
	}
	return selected, nil
}

func columnTypedColumnDenseSingleCodePredicateCandidateRowMatches(candidate columnTypedColumnDenseSingleCodePredicateCandidate, row, rowOffset int, want uint32, codes []uint32, values []int64, nulls []bool, defaults []bool) (bool, error) {
	if candidate.nullable {
		if nulls[row] || defaults[row] {
			return false, nil
		}
		value := values[row]
		if value < 0 || uint64(value) >= uint64(candidate.cardinality) || value > int64(^uint32(0)) {
			return false, fmt.Errorf("collections: dense typed-column predicate column %q code[%d]=%d outside cardinality=%d", candidate.partColumn.Definition.Name, rowOffset+row, value, candidate.cardinality)
		}
		return uint32(value) == want, nil
	}
	value := codes[row]
	if uint64(value) >= uint64(candidate.cardinality) {
		return false, fmt.Errorf("collections: dense typed-column predicate column %q code[%d]=%d outside cardinality=%d", candidate.partColumn.Definition.Name, rowOffset+row, value, candidate.cardinality)
	}
	return value == want, nil
}

func columnTypedColumnDenseSingleCodePredicateCandidateForSpec(adapterPart *typedColumnAdapterPart, fields []TypedStorageField, spec columnPhysicalQueryPredicateSpec) (columnTypedColumnDenseSingleCodePredicateCandidate, bool, error) {
	adapterColumn, partColumn, cardinality, err := typedColumnDenseStringCodeColumn(adapterPart, fields, spec.column, "predicate")
	if err != nil {
		return columnTypedColumnDenseSingleCodePredicateCandidate{}, false, err
	}
	allowed := make([]uint64, (cardinality+63)/64)
	matchedLiterals := 0
	singleCode := uint32(0)
	singleCodeAllowed := false
	multipleCodesAllowed := false
	missingMatchesEmpty := false
	for _, value := range spec.values {
		if adapterColumn.Field.Nullable && value == "" {
			missingMatchesEmpty = true
		}
		code, ok := adapterColumn.Dictionary[value]
		if !ok {
			continue
		}
		if code < 0 || uint64(code) >= uint64(cardinality) {
			return columnTypedColumnDenseSingleCodePredicateCandidate{}, false, fmt.Errorf("collections: dense typed-column predicate dictionary code %d outside cardinality %d for column %q", code, cardinality, adapterColumn.Definition.Name)
		}
		idx := int(code)
		mask := uint64(1) << uint(idx%64)
		if allowed[idx/64]&mask == 0 {
			if singleCodeAllowed && singleCode != uint32(idx) {
				multipleCodesAllowed = true
			} else {
				singleCode = uint32(idx)
				singleCodeAllowed = true
			}
		}
		allowed[idx/64] |= mask
		matchedLiterals++
	}
	if matchedLiterals == 0 || missingMatchesEmpty {
		return columnTypedColumnDenseSingleCodePredicateCandidate{}, false, nil
	}
	singleCodeAllowed = singleCodeAllowed && !multipleCodesAllowed
	if !singleCodeAllowed {
		return columnTypedColumnDenseSingleCodePredicateCandidate{}, false, nil
	}
	predicate := columnTypedColumnDensePredicatePart{
		Allowed:           allowed,
		SingleCode:        singleCode,
		SingleCodeAllowed: true,
	}
	return columnTypedColumnDenseSingleCodePredicateCandidate{
		partColumn:  partColumn,
		cardinality: cardinality,
		nullable:    adapterColumn.Field.Nullable,
		predicate:   predicate,
	}, true, nil
}

func columnTypedColumnDenseSingleCodePredicateBlocksAligned(candidates []columnTypedColumnDenseSingleCodePredicateCandidate, rows int) bool {
	if len(candidates) == 0 {
		return false
	}
	blocks := len(candidates[0].partColumn.Blocks)
	for idx := 1; idx < len(candidates); idx++ {
		if len(candidates[idx].partColumn.Blocks) != blocks {
			return false
		}
	}
	rowOffset := 0
	for blockIdx := 0; blockIdx < blocks; blockIdx++ {
		blockRows := candidates[0].partColumn.Blocks[blockIdx].Descriptor.RowCount
		if blockRows < 0 || blockRows > rows-rowOffset {
			return false
		}
		for idx := 1; idx < len(candidates); idx++ {
			if candidates[idx].partColumn.Blocks[blockIdx].Descriptor.RowCount != blockRows {
				return false
			}
		}
		rowOffset += blockRows
	}
	return rowOffset == rows
}

func typedColumnDenseSingleCodeTriplePredicateBlockMask(candidates []columnTypedColumnDenseSingleCodePredicateCandidate, rows int) ([]bool, int, error) {
	if len(candidates) != 3 {
		return nil, 0, fmt.Errorf("collections: dense typed-column predicate block mask candidates=%d want 3", len(candidates))
	}
	return typedColumnDenseSingleCodePredicateBlockMask(candidates, rows)
}

func typedColumnDenseSingleCodePredicateBlockMask(candidates []columnTypedColumnDenseSingleCodePredicateCandidate, rows int) ([]bool, int, error) {
	if !columnTypedColumnDenseSingleCodePredicateBlocksAligned(candidates, rows) {
		return nil, 0, fmt.Errorf("collections: dense typed-column predicate block mask candidates are not aligned")
	}
	mask := make([]bool, len(candidates[0].partColumn.Blocks))
	skipped := 0
	rowOffset := 0
	for blockIdx := range candidates[0].partColumn.Blocks {
		blockRows := candidates[0].partColumn.Blocks[blockIdx].Descriptor.RowCount
		selected := true
		for predicateIdx := range candidates {
			block := candidates[predicateIdx].partColumn.Blocks[blockIdx]
			g := block.Granule
			if err := validateTypedColumnDenseSingleCodePredicateGranuleBounds(g, candidates[predicateIdx].cardinality, candidates[predicateIdx].partColumn.Definition.Name, blockIdx); err != nil {
				return nil, 0, err
			}
			if !g.HasMinMax {
				continue
			}
			code := int64(candidates[predicateIdx].predicate.SingleCode)
			if code < g.Min || code > g.Max {
				selected = false
			}
		}
		mask[blockIdx] = selected
		if !selected {
			skipped++
		}
		rowOffset += blockRows
	}
	if rowOffset != rows {
		return nil, 0, fmt.Errorf("collections: dense typed-column predicate block mask rows=%d want part rows=%d", rowOffset, rows)
	}
	return mask, skipped, nil
}

func refreshTypedColumnDenseSingleCodePredicateCandidatePayloads(adapterPart *typedColumnAdapterPart, candidates []columnTypedColumnDenseSingleCodePredicateCandidate) error {
	if adapterPart == nil || adapterPart.Part == nil {
		return errors.New("collections: dense typed-column predicate refresh missing part")
	}
	for idx := range candidates {
		name := candidates[idx].partColumn.Definition.Name
		partColumn, ok := adapterPart.Part.Columns[name]
		if !ok {
			return fmt.Errorf("collections: dense typed-column predicate refresh missing column %q", name)
		}
		candidates[idx].partColumn = partColumn
	}
	return nil
}

func validateTypedColumnDenseSingleCodePredicateGranuleBounds(g typedcolumn.EncodedGranule, cardinality int, column string, blockIdx int) error {
	if g.HasMinMax {
		if g.Min < 0 || g.Max < 0 || g.Min > g.Max || uint64(g.Max) >= uint64(cardinality) {
			return fmt.Errorf("collections: dense typed-column predicate column %q block %d min/max [%d,%d] outside cardinality %d", column, blockIdx, g.Min, g.Max, cardinality)
		}
	}
	return nil
}

func densePredicateFromDictionaryCodes(codes []uint32, valid []bool, dictionary []string, dictionaryByCode map[int64]string, cardinality int, spec columnPhysicalQueryPredicateSpec) (columnTypedColumnDensePredicatePart, error) {
	allowed := make([]uint64, (cardinality+63)/64)
	matchedLiterals := 0
	missingMatchesEmpty := false
	for code := 0; code < cardinality; code++ {
		value, ok := denseDictionaryValue(dictionary, dictionaryByCode, code)
		if !ok {
			return columnTypedColumnDensePredicatePart{}, fmt.Errorf("collections: dense typed-column predicate dictionary missing local code %d for column %q", code, spec.column)
		}
		for _, target := range spec.values {
			if value != target {
				continue
			}
			allowed[code/64] |= uint64(1) << uint(code%64)
			matchedLiterals++
			break
		}
	}
	for _, target := range spec.values {
		if target == "" {
			missingMatchesEmpty = true
			break
		}
	}
	if matchedLiterals == 0 && !missingMatchesEmpty {
		return columnTypedColumnDensePredicatePart{RejectsAll: true}, nil
	}
	return columnTypedColumnDensePredicatePart{Codes: codes, Valid: valid, Allowed: allowed, MissingMatchesEmpty: missingMatchesEmpty}, nil
}

func denseDictionaryValue(dictionary []string, dictionaryByCode map[int64]string, code int) (string, bool) {
	if code >= 0 && code < len(dictionary) {
		return dictionary[code], true
	}
	if dictionaryByCode != nil {
		value, ok := dictionaryByCode[int64(code)]
		return value, ok
	}
	return "", false
}

type columnTypedColumnTimeOrderCodeColumn struct {
	PartColumn       typedcolumn.ColumnPartColumn
	Cardinality      int
	DictionaryByCode map[int64]string
	Nullable         bool
}

type columnTypedColumnTimeOrderPredicateColumn struct {
	CodeColumn          columnTypedColumnTimeOrderCodeColumn
	Allowed             []uint64
	MissingMatchesEmpty bool
	RejectsAll          bool
	UsesGroupCode       bool
}

type columnTypedColumnTimeOrderTopKPart struct {
	Rows           int
	Granules       []typedcolumn.GranuleDescriptor
	Marks          []typedcolumn.SortKeyMark
	PhysicalRows   []int
	PayloadLoader  *columnTypedColumnTimeOrderTopKPayloadLoader
	ValueColumn    typedcolumn.ColumnPartColumn
	Group          columnTypedColumnTimeOrderCodeColumn
	Predicates     []columnTypedColumnTimeOrderPredicateColumn
	decodedGranule int
	timeValues     []int64
	groupCodes     []uint32
	groupValid     []bool
	predicateCodes [][]uint32
	predicateValid [][]bool
}

type columnTypedColumnTimeOrderTopKPayloadLoader struct {
	readRange typedColumnPreparedRangeReader
	plans     map[string][]typedColumnPreparedBlockPlan
	bytesRead int64
}

func newColumnTypedColumnTimeOrderTopKPayloadLoader(readRange typedColumnPreparedRangeReader, prepared *typedColumnPreparedPartState) *columnTypedColumnTimeOrderTopKPayloadLoader {
	if readRange == nil || prepared == nil || len(prepared.Columns) == 0 {
		return nil
	}
	plans := make(map[string][]typedColumnPreparedBlockPlan, len(prepared.Columns))
	for name, column := range prepared.Columns {
		if column == nil {
			continue
		}
		plans[name] = append([]typedColumnPreparedBlockPlan(nil), column.BlockPlans...)
	}
	return &columnTypedColumnTimeOrderTopKPayloadLoader{readRange: readRange, plans: plans}
}

func (l *columnTypedColumnTimeOrderTopKPayloadLoader) ensureColumnPayloadsForRowRange(column *typedcolumn.ColumnPartColumn, firstRow, rowCount int, role string) error {
	if l == nil {
		return nil
	}
	if l.readRange == nil {
		return errors.New("collections: time-order topK payload loader missing range reader")
	}
	if column == nil {
		return fmt.Errorf("collections: time-order topK payload loader missing %s column", role)
	}
	plans, ok := l.plans[column.Definition.Name]
	if !ok {
		return fmt.Errorf("collections: time-order topK payload loader missing block plans for %s column %q", role, column.Definition.Name)
	}
	if len(plans) != len(column.Blocks) {
		return fmt.Errorf("collections: time-order topK payload loader column %q block plans=%d want blocks=%d", column.Definition.Name, len(plans), len(column.Blocks))
	}
	limit := firstRow + rowCount
	for blockIdx := range column.Blocks {
		block := &column.Blocks[blockIdx]
		blockFirst := block.Descriptor.FirstRow
		blockLimit := blockFirst + block.Descriptor.RowCount
		if blockLimit <= firstRow {
			continue
		}
		if blockFirst >= limit {
			break
		}
		if len(block.Granule.Payload) != 0 {
			continue
		}
		plan := plans[blockIdx]
		if plan.Index != blockIdx {
			return fmt.Errorf("collections: time-order topK payload loader column %q plan index=%d want %d", column.Definition.Name, plan.Index, blockIdx)
		}
		if plan.PayloadLength == 0 {
			continue
		}
		payload, err := l.readRange(plan.PayloadOffset, plan.PayloadLength, false)
		if err != nil {
			return fmt.Errorf("collections: time-order topK payload loader read %s column %q block %d: %w", role, column.Definition.Name, blockIdx, err)
		}
		if len(payload) != plan.PayloadLength {
			return fmt.Errorf("collections: time-order topK payload loader column %q block %d payload bytes=%d want %d", column.Definition.Name, blockIdx, len(payload), plan.PayloadLength)
		}
		block.Granule.Payload = payload
		block.Granule.PayloadRef = typedcolumn.PayloadRef{Kind: typedcolumn.PayloadRefInline, Length: len(payload)}
		l.bytesRead += int64(len(payload))
	}
	return nil
}

// decodeTypedColumnPhysicalQueryTimeOrderTopKPart prepares the q4a
// typed-column section fast path. It keeps the encoded per-granule payloads and
// validates time_us sort-key marks, then the query runner decodes group and
// predicate code granules only until the Top-K time threshold is closed.
func decodeTypedColumnPhysicalQueryTimeOrderTopKPart(plan columnTypedColumnPhysicalQueryPlan, schemaHash uint64, typedRef, physical columnManifestAssetRefForScan, raw []byte, includePhysicalRows bool) (columnTypedColumnPhysicalQueryPart, error) {
	// Unlike the dense q1/q3/q5 decoders, the time-order runner keeps encoded
	// granule payloads and decodes only the prefix needed by Top-K. The physical
	// query prepare loop reuses its read scratch between parts, so take ownership
	// before parsing to keep prepared/direct payload references stable.
	raw = append([]byte(nil), raw...)
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	adapterPart, err := typedColumnAdapterPartFromImageWithoutRowLocators(typedColumnAdapterOptions{Fields: plan.Fields, SchemaVersion: uint32(schemaHash)}, image)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	summary := typedColumnAdapterImageSummary{PartID: image.PartID, Rows: image.Rows, Sections: len(image.Sections), SectionBytes: typedColumnPhysicalQueryImageSectionBytes(image), SortKey: columnSortKeysFromTypedColumnSortKeys(adapterPart.Part.Descriptor.SortKey)}
	if summary.PartID != typedRef.Ref.PartID || summary.Rows != typedRef.Rows {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("typed_column_part image/ref mismatch image_part=%d ref_part=%d image_rows=%d manifest_rows=%d", summary.PartID, typedRef.Ref.PartID, summary.Rows, typedRef.Rows)
	}
	if physical.Rows != 0 && summary.Rows != physical.Rows {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("typed_column_part rows=%d do not match physical rows=%d", summary.Rows, physical.Rows)
	}
	if err := validateTypedColumnPhysicalQuerySortMetadata(plan.SortKey, typedRef.SortKey, summary.SortKey); err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}

	groupColumn, groupPartColumn, cardinality, err := typedColumnDenseStringCodeColumn(adapterPart, plan.Fields, plan.GroupColumn, "time-order topK group")
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	valueColumn, ok, err := typedColumnInt64PredicateAdapterColumn(plan.Fields, plan.ValueColumn)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	if !ok {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("collections: time-order topK value column %q is not owned by typed_column_part", plan.ValueColumn)
	}
	for _, candidate := range adapterPart.Columns {
		if candidate.Definition.Name == valueColumn.Definition.Name {
			valueColumn = candidate
			break
		}
	}
	if valueColumn.Field.Nullable || valueColumn.Definition.Type != typedcolumn.ColumnTypeInt64 {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("%w: time-order topK value column %q is not a non-null int64", ErrColumnQueryPlanUnsupported, plan.ValueColumn)
	}
	valuePartColumn, ok := adapterPart.Part.Columns[valueColumn.Definition.Name]
	if !ok {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("collections: time-order topK missing value column %q", valueColumn.Definition.Name)
	}
	if valuePartColumn.Definition.Type != typedcolumn.ColumnTypeInt64 {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("%w: time-order topK value column %q type=%s", ErrColumnQueryPlanUnsupported, plan.ValueColumn, valuePartColumn.Definition.Type)
	}
	if err := validateTypedColumnTimeOrderTopKMarks(adapterPart.Part, valueColumn.Definition.Name); err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}

	group := columnTypedColumnTimeOrderCodeColumn{PartColumn: groupPartColumn, Cardinality: cardinality, DictionaryByCode: groupColumn.ReverseDictionary, Nullable: groupColumn.Field.Nullable}
	predicates := make([]columnTypedColumnTimeOrderPredicateColumn, 0, len(plan.PredicateSpecs))
	for _, spec := range plan.PredicateSpecs {
		if spec.column == plan.GroupColumn {
			allowed, missingMatchesEmpty, rejectsAll, err := timeOrderTopKAllowedCodes(groupColumn, cardinality, spec)
			if err != nil {
				return columnTypedColumnPhysicalQueryPart{}, err
			}
			predicates = append(predicates, columnTypedColumnTimeOrderPredicateColumn{CodeColumn: group, Allowed: allowed, MissingMatchesEmpty: missingMatchesEmpty, RejectsAll: rejectsAll, UsesGroupCode: true})
			continue
		}
		predicateColumn, predicatePartColumn, predicateCardinality, err := typedColumnDenseStringCodeColumn(adapterPart, plan.Fields, spec.column, "time-order topK predicate")
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, err
		}
		allowed, missingMatchesEmpty, rejectsAll, err := timeOrderTopKAllowedCodes(predicateColumn, predicateCardinality, spec)
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, err
		}
		predicates = append(predicates, columnTypedColumnTimeOrderPredicateColumn{
			CodeColumn:          columnTypedColumnTimeOrderCodeColumn{PartColumn: predicatePartColumn, Cardinality: predicateCardinality, DictionaryByCode: predicateColumn.ReverseDictionary, Nullable: predicateColumn.Field.Nullable},
			Allowed:             allowed,
			MissingMatchesEmpty: missingMatchesEmpty,
			RejectsAll:          rejectsAll,
		})
	}

	var physicalRows []int
	var primaryDiag columnTypedColumnPhysicalRowIndexDiagnostics
	if includePhysicalRows {
		physicalRows, primaryDiag, err = typedColumnPhysicalQueryPhysicalRows(adapterPart, nil, summary.Rows)
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, err
		}
	}

	return columnTypedColumnPhysicalQueryPart{
		Ref:                 typedRef,
		PhysicalRef:         physical,
		Rows:                summary.Rows,
		Bytes:               int64(len(raw)),
		Sections:            summary.Sections,
		SectionBytes:        summary.SectionBytes,
		GranulesConsidered:  len(adapterPart.Part.Descriptor.Granules),
		GranulesDecoded:     primaryDiag.GranulesDecoded,
		DecodedBlocks:       primaryDiag.BlocksDecoded,
		SortKeyMarkChecks:   len(adapterPart.Part.Marks),
		DecodedPayloadBytes: uint64(primaryDiag.BytesDecoded),
		TimeOrderTopK: &columnTypedColumnTimeOrderTopKPart{
			Rows:           summary.Rows,
			Granules:       append([]typedcolumn.GranuleDescriptor(nil), adapterPart.Part.Descriptor.Granules...),
			Marks:          append([]typedcolumn.SortKeyMark(nil), adapterPart.Part.Marks...),
			PhysicalRows:   physicalRows,
			ValueColumn:    valuePartColumn,
			Group:          group,
			Predicates:     predicates,
			decodedGranule: -1,
		},
	}, nil
}

func decodeTypedColumnPhysicalQueryTimeOrderTopKPreparedPart(plan columnTypedColumnPhysicalQueryPlan, summary typedColumnAdapterImageSummary, typedRef, physical columnManifestAssetRefForScan, adapterPart *typedColumnAdapterPart, bytesRead int64, payloadLoader *columnTypedColumnTimeOrderTopKPayloadLoader) (columnTypedColumnPhysicalQueryPart, error) {
	groupColumn, groupPartColumn, cardinality, err := typedColumnDenseStringCodeColumn(adapterPart, plan.Fields, plan.GroupColumn, "time-order topK group")
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	valueColumn, ok, err := typedColumnInt64PredicateAdapterColumn(plan.Fields, plan.ValueColumn)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	if !ok {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("collections: time-order topK value column %q is not owned by typed_column_part", plan.ValueColumn)
	}
	for _, candidate := range adapterPart.Columns {
		if candidate.Definition.Name == valueColumn.Definition.Name {
			valueColumn = candidate
			break
		}
	}
	if valueColumn.Field.Nullable || valueColumn.Definition.Type != typedcolumn.ColumnTypeInt64 {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("%w: time-order topK value column %q is not a non-null int64", ErrColumnQueryPlanUnsupported, plan.ValueColumn)
	}
	valuePartColumn, ok := adapterPart.Part.Columns[valueColumn.Definition.Name]
	if !ok {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("collections: time-order topK missing value column %q", valueColumn.Definition.Name)
	}
	if valuePartColumn.Definition.Type != typedcolumn.ColumnTypeInt64 {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("%w: time-order topK value column %q type=%s", ErrColumnQueryPlanUnsupported, plan.ValueColumn, valuePartColumn.Definition.Type)
	}
	if err := validateTypedColumnTimeOrderTopKMarks(adapterPart.Part, valueColumn.Definition.Name); err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}

	group := columnTypedColumnTimeOrderCodeColumn{PartColumn: groupPartColumn, Cardinality: cardinality, DictionaryByCode: groupColumn.ReverseDictionary, Nullable: groupColumn.Field.Nullable}
	predicates := make([]columnTypedColumnTimeOrderPredicateColumn, 0, len(plan.PredicateSpecs))
	for _, spec := range plan.PredicateSpecs {
		if spec.column == plan.GroupColumn {
			allowed, missingMatchesEmpty, rejectsAll, err := timeOrderTopKAllowedCodes(groupColumn, cardinality, spec)
			if err != nil {
				return columnTypedColumnPhysicalQueryPart{}, err
			}
			predicates = append(predicates, columnTypedColumnTimeOrderPredicateColumn{CodeColumn: group, Allowed: allowed, MissingMatchesEmpty: missingMatchesEmpty, RejectsAll: rejectsAll, UsesGroupCode: true})
			continue
		}
		predicateColumn, predicatePartColumn, predicateCardinality, err := typedColumnDenseStringCodeColumn(adapterPart, plan.Fields, spec.column, "time-order topK predicate")
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, err
		}
		allowed, missingMatchesEmpty, rejectsAll, err := timeOrderTopKAllowedCodes(predicateColumn, predicateCardinality, spec)
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, err
		}
		predicates = append(predicates, columnTypedColumnTimeOrderPredicateColumn{
			CodeColumn:          columnTypedColumnTimeOrderCodeColumn{PartColumn: predicatePartColumn, Cardinality: predicateCardinality, DictionaryByCode: predicateColumn.ReverseDictionary, Nullable: predicateColumn.Field.Nullable},
			Allowed:             allowed,
			MissingMatchesEmpty: missingMatchesEmpty,
			RejectsAll:          rejectsAll,
		})
	}

	return columnTypedColumnPhysicalQueryPart{
		Ref:                typedRef,
		PhysicalRef:        physical,
		Rows:               summary.Rows,
		Bytes:              bytesRead,
		Sections:           summary.Sections,
		SectionBytes:       summary.SectionBytes,
		GranulesConsidered: len(adapterPart.Part.Descriptor.Granules),
		SortKeyMarkChecks:  len(adapterPart.Part.Marks),
		TimeOrderTopK: &columnTypedColumnTimeOrderTopKPart{
			Rows:           summary.Rows,
			Granules:       append([]typedcolumn.GranuleDescriptor(nil), adapterPart.Part.Descriptor.Granules...),
			Marks:          append([]typedcolumn.SortKeyMark(nil), adapterPart.Part.Marks...),
			PayloadLoader:  payloadLoader,
			ValueColumn:    valuePartColumn,
			Group:          group,
			Predicates:     predicates,
			decodedGranule: -1,
		},
	}, nil
}

func validateTypedColumnTimeOrderTopKMarks(part *typedcolumn.ColumnPart, valueColumn string) error {
	if part == nil {
		return errors.New("collections: time-order topK nil typed-column part")
	}
	if part.Descriptor.RowCount == 0 {
		if len(part.Descriptor.Granules) != 0 || len(part.Marks) != 0 {
			return fmt.Errorf("collections: time-order topK empty part has granules=%d marks=%d", len(part.Descriptor.Granules), len(part.Marks))
		}
		return nil
	}
	if len(part.Marks) == 0 {
		return fmt.Errorf("%w: time-order topK requires sort-key marks", ErrColumnQueryPlanUnsupported)
	}
	if len(part.Marks) != len(part.Descriptor.Granules) {
		return fmt.Errorf("collections: time-order topK marks=%d granules=%d", len(part.Marks), len(part.Descriptor.Granules))
	}
	seenRows := 0
	prevSet := false
	prev := int64(0)
	for idx, granule := range part.Descriptor.Granules {
		mark := part.Marks[idx]
		if granule.Ordinal != idx || granule.MarkOrdinal != idx || mark.Rows != granule.RowCount {
			return fmt.Errorf("collections: time-order topK stale mark at granule %d", idx)
		}
		if granule.FirstRow != seenRows {
			return fmt.Errorf("collections: time-order topK non-contiguous granule %d first_row=%d want %d", idx, granule.FirstRow, seenRows)
		}
		if len(mark.Columns) == 0 || mark.Columns[0] != valueColumn {
			return fmt.Errorf("collections: time-order topK mark %d first column=%v want %q", idx, mark.Columns, valueColumn)
		}
		if len(mark.Prefixes) == 0 || len(mark.Prefixes[0].Lower.Values) == 0 {
			return fmt.Errorf("collections: time-order topK mark %d missing lower time bound", idx)
		}
		value := mark.Prefixes[0].Lower.Values[0]
		if prevSet && value < prev {
			return fmt.Errorf("collections: time-order topK sort key out of order at granule %d", idx)
		}
		prev = value
		prevSet = true
		seenRows += granule.RowCount
	}
	if seenRows != part.Descriptor.RowCount {
		return fmt.Errorf("collections: time-order topK granule rows=%d want part rows=%d", seenRows, part.Descriptor.RowCount)
	}
	return nil
}

func timeOrderTopKAllowedCodes(column typedColumnAdapterColumn, cardinality int, spec columnPhysicalQueryPredicateSpec) ([]uint64, bool, bool, error) {
	needsDictionary := false
	for _, value := range spec.values {
		if column.Field.Nullable && value == "" {
			continue
		}
		needsDictionary = true
		break
	}
	if column.Dictionary == nil && needsDictionary {
		return nil, false, false, fmt.Errorf("collections: time-order topK predicate column %q missing forward dictionary", column.Definition.Name)
	}
	allowed := make([]uint64, (cardinality+63)/64)
	matchedLiterals := 0
	missingMatchesEmpty := false
	for _, value := range spec.values {
		if column.Field.Nullable && value == "" {
			missingMatchesEmpty = true
		}
		code, ok := column.Dictionary[value]
		if !ok {
			continue
		}
		if code < 0 || uint64(code) >= uint64(cardinality) {
			return nil, false, false, fmt.Errorf("collections: time-order topK predicate dictionary code %d outside cardinality %d for column %q", code, cardinality, column.Definition.Name)
		}
		idx := int(code)
		allowed[idx/64] |= uint64(1) << uint(idx%64)
		matchedLiterals++
	}
	return allowed, missingMatchesEmpty, matchedLiterals == 0 && !missingMatchesEmpty, nil
}

func (p *columnTypedColumnTimeOrderTopKPart) resetTimeOrderTopKScan() {
	p.decodedGranule = -1
	p.timeValues = p.timeValues[:0]
	p.groupCodes = p.groupCodes[:0]
	p.groupValid = p.groupValid[:0]
	if cap(p.predicateCodes) < len(p.Predicates) {
		p.predicateCodes = make([][]uint32, len(p.Predicates))
	} else {
		p.predicateCodes = p.predicateCodes[:len(p.Predicates)]
		for idx := range p.predicateCodes {
			p.predicateCodes[idx] = p.predicateCodes[idx][:0]
		}
	}
	if cap(p.predicateValid) < len(p.Predicates) {
		p.predicateValid = make([][]bool, len(p.Predicates))
	} else {
		p.predicateValid = p.predicateValid[:len(p.Predicates)]
		for idx := range p.predicateValid {
			p.predicateValid[idx] = p.predicateValid[idx][:0]
		}
	}
}

func (p *columnTypedColumnTimeOrderTopKPart) firstTimeOrderTopKTime() (int64, bool, error) {
	if p == nil {
		return 0, false, errors.New("collections: nil time-order topK part")
	}
	if p.Rows == 0 {
		return 0, false, nil
	}
	timeValue, err := p.timeOrderTopKTimeAt(0, 0)
	return timeValue, true, err
}

func (p *columnTypedColumnTimeOrderTopKPart) nextTimeOrderTopKPosition(granuleIdx, rowInGranule int) (int, int, int64, bool, error) {
	if granuleIdx < 0 || granuleIdx >= len(p.Granules) {
		return 0, 0, 0, true, fmt.Errorf("collections: time-order topK granule %d outside %d", granuleIdx, len(p.Granules))
	}
	rowInGranule++
	for granuleIdx < len(p.Granules) && rowInGranule >= p.Granules[granuleIdx].RowCount {
		granuleIdx++
		rowInGranule = 0
	}
	if granuleIdx >= len(p.Granules) {
		return 0, 0, 0, true, nil
	}
	timeValue, err := p.timeOrderTopKTimeAt(granuleIdx, rowInGranule)
	return granuleIdx, rowInGranule, timeValue, false, err
}

func (p *columnTypedColumnTimeOrderTopKPart) timeOrderTopKTimeAt(granuleIdx, rowInGranule int) (int64, error) {
	if p.decodedGranule == granuleIdx {
		if rowInGranule < 0 || rowInGranule >= len(p.timeValues) {
			return 0, fmt.Errorf("collections: time-order topK row_in_granule=%d outside decoded time rows=%d", rowInGranule, len(p.timeValues))
		}
		return p.timeValues[rowInGranule], nil
	}
	if rowInGranule != 0 {
		return 0, fmt.Errorf("collections: time-order topK cannot read row %d in undecoded granule %d", rowInGranule, granuleIdx)
	}
	if granuleIdx < 0 || granuleIdx >= len(p.Marks) {
		return 0, fmt.Errorf("collections: time-order topK mark %d outside %d", granuleIdx, len(p.Marks))
	}
	mark := p.Marks[granuleIdx]
	if len(mark.Prefixes) == 0 || len(mark.Prefixes[0].Lower.Values) == 0 {
		return 0, fmt.Errorf("collections: time-order topK mark %d missing lower time bound", granuleIdx)
	}
	return mark.Prefixes[0].Lower.Values[0], nil
}

func (p *columnTypedColumnTimeOrderTopKPart) evaluateTimeOrderTopKRow(granuleIdx, rowInGranule int) (string, bool, bool, int, uint64, error) {
	decoded, blocks, decodedBytes, err := p.ensureTimeOrderTopKGranuleDecoded(granuleIdx)
	if err != nil {
		return "", false, decoded, blocks, decodedBytes, err
	}
	for predIdx, predicate := range p.Predicates {
		if predicate.RejectsAll {
			return "", false, decoded, blocks, decodedBytes, nil
		}
		codes := p.predicateCodes[predIdx]
		if rowInGranule < 0 || rowInGranule >= len(codes) {
			return "", false, decoded, blocks, decodedBytes, fmt.Errorf("collections: time-order topK predicate row=%d outside decoded rows=%d", rowInGranule, len(codes))
		}
		if valid := p.predicateValid[predIdx]; valid != nil && !columnTypedColumnDenseCodeValid(valid, rowInGranule) {
			if !predicate.MissingMatchesEmpty {
				return "", false, decoded, blocks, decodedBytes, nil
			}
			continue
		}
		if !columnTypedColumnCodeAllowed(predicate.Allowed, codes[rowInGranule]) {
			return "", false, decoded, blocks, decodedBytes, nil
		}
	}
	if rowInGranule < 0 || rowInGranule >= len(p.groupCodes) {
		return "", false, decoded, blocks, decodedBytes, fmt.Errorf("collections: time-order topK group row=%d outside decoded rows=%d", rowInGranule, len(p.groupCodes))
	}
	if !columnTypedColumnDenseCodeValid(p.groupValid, rowInGranule) {
		return "", true, decoded, blocks, decodedBytes, nil
	}
	groupCode := p.groupCodes[rowInGranule]
	if uint64(groupCode) >= uint64(p.Group.Cardinality) {
		return "", false, decoded, blocks, decodedBytes, fmt.Errorf("collections: time-order topK group code=%d outside cardinality=%d", groupCode, p.Group.Cardinality)
	}
	group, ok := p.Group.DictionaryByCode[int64(groupCode)]
	if !ok {
		return "", false, decoded, blocks, decodedBytes, fmt.Errorf("collections: time-order topK dictionary missing group code %d", groupCode)
	}
	return group, true, decoded, blocks, decodedBytes, nil
}

func (p *columnTypedColumnTimeOrderTopKPart) ensureTimeOrderTopKGranuleDecoded(granuleIdx int) (bool, int, uint64, error) {
	if p.decodedGranule == granuleIdx {
		return false, 0, 0, nil
	}
	if granuleIdx < 0 || granuleIdx >= len(p.Granules) {
		return false, 0, 0, fmt.Errorf("collections: time-order topK granule %d outside %d", granuleIdx, len(p.Granules))
	}
	granule := p.Granules[granuleIdx]
	if err := p.ensureTimeOrderTopKGranulePayloads(granuleIdx, granule); err != nil {
		return false, 0, 0, err
	}
	blocks := 0
	decodedBytes := uint64(0)
	var err error
	p.timeValues, decodedBytes, blocks, err = decodeTypedColumnInt64ValuesForRowRange(p.ValueColumn, granule.FirstRow, granule.RowCount, "time-order topK value", p.timeValues)
	if err != nil {
		return true, blocks, decodedBytes, err
	}
	if len(p.timeValues) != 0 {
		markLower, err := p.timeOrderTopKTimeAt(granuleIdx, 0)
		if err != nil {
			return true, blocks, decodedBytes, err
		}
		if p.timeValues[0] != markLower {
			return true, blocks, decodedBytes, fmt.Errorf("collections: time-order topK granule %d first time=%d mark lower=%d", granuleIdx, p.timeValues[0], markLower)
		}
		for row := 1; row < len(p.timeValues); row++ {
			if p.timeValues[row] < p.timeValues[row-1] {
				return true, blocks, decodedBytes, fmt.Errorf("collections: time-order topK granule %d time values out of order at row %d", granuleIdx, row)
			}
		}
	}
	var groupBytes uint64
	var groupBlocks int
	if p.Group.Nullable {
		p.groupCodes, p.groupValid, groupBytes, groupBlocks, err = decodeTypedColumnNullableUint32CodesForRowRange(p.Group.PartColumn, p.Group.Cardinality, granule.FirstRow, granule.RowCount, "time-order topK group", p.groupCodes, p.groupValid)
	} else {
		p.groupCodes, groupBytes, groupBlocks, err = decodeTypedColumnUint32CodesForRowRange(p.Group.PartColumn, p.Group.Cardinality, granule.FirstRow, granule.RowCount, "time-order topK group", p.groupCodes)
		p.groupValid = nil
	}
	decodedBytes += groupBytes
	blocks += groupBlocks
	if err != nil {
		return true, blocks, decodedBytes, err
	}
	for idx, predicate := range p.Predicates {
		if predicate.UsesGroupCode {
			p.predicateCodes[idx] = p.groupCodes
			p.predicateValid[idx] = p.groupValid
			continue
		}
		var predicateBytes uint64
		var predicateBlocks int
		if predicate.CodeColumn.Nullable {
			p.predicateCodes[idx], p.predicateValid[idx], predicateBytes, predicateBlocks, err = decodeTypedColumnNullableUint32CodesForRowRange(predicate.CodeColumn.PartColumn, predicate.CodeColumn.Cardinality, granule.FirstRow, granule.RowCount, "time-order topK predicate", p.predicateCodes[idx], p.predicateValid[idx])
		} else {
			p.predicateCodes[idx], predicateBytes, predicateBlocks, err = decodeTypedColumnUint32CodesForRowRange(predicate.CodeColumn.PartColumn, predicate.CodeColumn.Cardinality, granule.FirstRow, granule.RowCount, "time-order topK predicate", p.predicateCodes[idx])
			p.predicateValid[idx] = nil
		}
		decodedBytes += predicateBytes
		blocks += predicateBlocks
		if err != nil {
			return true, blocks, decodedBytes, err
		}
	}
	p.decodedGranule = granuleIdx
	return true, blocks, decodedBytes, nil
}

func (p *columnTypedColumnTimeOrderTopKPart) ensureTimeOrderTopKGranulePayloads(granuleIdx int, granule typedcolumn.GranuleDescriptor) error {
	if p == nil || p.PayloadLoader == nil {
		return nil
	}
	if err := p.PayloadLoader.ensureColumnPayloadsForRowRange(&p.ValueColumn, granule.FirstRow, granule.RowCount, "value"); err != nil {
		return fmt.Errorf("collections: time-order topK granule %d: %w", granuleIdx, err)
	}
	if err := p.PayloadLoader.ensureColumnPayloadsForRowRange(&p.Group.PartColumn, granule.FirstRow, granule.RowCount, "group"); err != nil {
		return fmt.Errorf("collections: time-order topK granule %d: %w", granuleIdx, err)
	}
	for idx := range p.Predicates {
		predicate := &p.Predicates[idx]
		if predicate.UsesGroupCode {
			continue
		}
		if err := p.PayloadLoader.ensureColumnPayloadsForRowRange(&predicate.CodeColumn.PartColumn, granule.FirstRow, granule.RowCount, "predicate"); err != nil {
			return fmt.Errorf("collections: time-order topK granule %d: %w", granuleIdx, err)
		}
	}
	return nil
}

func decodeTypedColumnInt64ValuesForRowRange(partColumn typedcolumn.ColumnPartColumn, firstRow, rowCount int, role string, dst []int64) ([]int64, uint64, int, error) {
	if rowCount < 0 || firstRow < 0 {
		return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s invalid row range first=%d rows=%d", role, firstRow, rowCount)
	}
	if rowCount == 0 {
		return dst[:0], 0, 0, nil
	}
	limit := firstRow + rowCount
	out := dst[:0]
	if cap(out) < rowCount {
		out = make([]int64, 0, rowCount)
	}
	var scratch []int64
	var reader typedcolumn.GranuleReader
	decodedBytes := uint64(0)
	blocks := 0
	for blockIdx, block := range partColumn.Blocks {
		blockFirst := block.Descriptor.FirstRow
		blockLimit := blockFirst + block.Descriptor.RowCount
		if blockLimit <= firstRow {
			continue
		}
		if blockFirst >= limit {
			break
		}
		g := block.Granule
		decoded, err := reader.DecodeInt64Into(scratch[:0], g)
		if err != nil {
			return nil, decodedBytes, blocks, fmt.Errorf("collections: dense typed-column %s block %d: %w", role, blockIdx, err)
		}
		if len(decoded) != block.Descriptor.RowCount {
			return nil, decodedBytes, blocks, fmt.Errorf("collections: dense typed-column %s block %d decoded rows=%d want %d", role, blockIdx, len(decoded), block.Descriptor.RowCount)
		}
		start := max(firstRow, blockFirst)
		end := min(limit, blockLimit)
		out = append(out, decoded[start-blockFirst:end-blockFirst]...)
		scratch = decoded
		decodedBytes += uint64(g.RawBytes)
		blocks++
	}
	if len(out) != rowCount {
		return nil, decodedBytes, blocks, fmt.Errorf("collections: dense typed-column %s decoded rows=%d want range rows=%d", role, len(out), rowCount)
	}
	return out, decodedBytes, blocks, nil
}

func decodeTypedColumnUint32CodesForRowRange(partColumn typedcolumn.ColumnPartColumn, cardinality, firstRow, rowCount int, role string, dst []uint32) ([]uint32, uint64, int, error) {
	if rowCount < 0 || firstRow < 0 {
		return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s invalid row range first=%d rows=%d", role, firstRow, rowCount)
	}
	if rowCount == 0 {
		return dst[:0], 0, 0, nil
	}
	limit := firstRow + rowCount
	out := dst[:0]
	if cap(out) < rowCount {
		out = make([]uint32, 0, rowCount)
	}
	var scratch []uint32
	var reader typedcolumn.GranuleReader
	decodedBytes := uint64(0)
	blocks := 0
	for blockIdx, block := range partColumn.Blocks {
		blockFirst := block.Descriptor.FirstRow
		blockLimit := blockFirst + block.Descriptor.RowCount
		if blockLimit <= firstRow {
			continue
		}
		if blockFirst >= limit {
			break
		}
		g := block.Granule
		if g.HasMinMax {
			if g.Min < 0 || g.Max < 0 || g.Min > g.Max || uint64(g.Max) >= uint64(cardinality) {
				return nil, decodedBytes, blocks, fmt.Errorf("collections: dense typed-column %s block %d min/max [%d,%d] outside cardinality %d", role, blockIdx, g.Min, g.Max, cardinality)
			}
		}
		decoded, err := reader.DecodeUint32CodesInto(scratch[:0], g)
		if err != nil {
			return nil, decodedBytes, blocks, fmt.Errorf("collections: dense typed-column %s block %d: %w", role, blockIdx, err)
		}
		if len(decoded) != block.Descriptor.RowCount {
			return nil, decodedBytes, blocks, fmt.Errorf("collections: dense typed-column %s decoded rows=%d want %d", role, len(decoded), block.Descriptor.RowCount)
		}
		start := max(firstRow, blockFirst)
		end := min(limit, blockLimit)
		for _, code := range decoded[start-blockFirst : end-blockFirst] {
			if uint64(code) >= uint64(cardinality) {
				return nil, decodedBytes, blocks, fmt.Errorf("collections: dense typed-column %s code=%d outside cardinality=%d", role, code, cardinality)
			}
			out = append(out, code)
		}
		scratch = decoded
		decodedBytes += uint64(g.RawBytes)
		blocks++
	}
	if len(out) != rowCount {
		return nil, decodedBytes, blocks, fmt.Errorf("collections: dense typed-column %s decoded rows=%d want range rows=%d", role, len(out), rowCount)
	}
	return out, decodedBytes, blocks, nil
}

func decodeTypedColumnNullableUint32CodesForRowRange(partColumn typedcolumn.ColumnPartColumn, cardinality, firstRow, rowCount int, role string, dst []uint32, validDst []bool) ([]uint32, []bool, uint64, int, error) {
	if rowCount < 0 || firstRow < 0 {
		return nil, nil, 0, 0, fmt.Errorf("collections: dense typed-column %s invalid row range first=%d rows=%d", role, firstRow, rowCount)
	}
	if rowCount == 0 {
		return dst[:0], validDst[:0], 0, 0, nil
	}
	limit := firstRow + rowCount
	out := dst[:0]
	if cap(out) < rowCount {
		out = make([]uint32, 0, rowCount)
	}
	valid := validDst[:0]
	if cap(valid) < rowCount {
		valid = make([]bool, 0, rowCount)
	}
	var valueScratch []int64
	var nullScratch []bool
	var defaultScratch []bool
	var reader typedcolumn.GranuleReader
	decodedBytes := uint64(0)
	blocks := 0
	for blockIdx, block := range partColumn.Blocks {
		blockFirst := block.Descriptor.FirstRow
		blockLimit := blockFirst + block.Descriptor.RowCount
		if blockLimit <= firstRow {
			continue
		}
		if blockFirst >= limit {
			break
		}
		g := block.Granule
		if g.HasMinMax {
			if g.Min < 0 || g.Max < 0 || g.Min > g.Max || uint64(g.Max) >= uint64(cardinality) {
				return nil, nil, decodedBytes, blocks, fmt.Errorf("collections: dense typed-column %s block %d min/max [%d,%d] outside cardinality %d", role, blockIdx, g.Min, g.Max, cardinality)
			}
		}
		values, nulls, defaults, err := reader.DecodeNullableInt64Into(valueScratch[:0], nullScratch[:0], defaultScratch[:0], g)
		if err != nil {
			return nil, nil, decodedBytes, blocks, fmt.Errorf("collections: dense typed-column %s block %d: %w", role, blockIdx, err)
		}
		if len(values) != block.Descriptor.RowCount || len(nulls) != block.Descriptor.RowCount || len(defaults) != block.Descriptor.RowCount {
			return nil, nil, decodedBytes, blocks, fmt.Errorf("collections: dense typed-column %s block %d decoded rows values/nulls/defaults=%d/%d/%d want %d", role, blockIdx, len(values), len(nulls), len(defaults), block.Descriptor.RowCount)
		}
		start := max(firstRow, blockFirst)
		end := min(limit, blockLimit)
		for row := start - blockFirst; row < end-blockFirst; row++ {
			if nulls[row] || defaults[row] {
				out = append(out, 0)
				valid = append(valid, false)
				continue
			}
			code := values[row]
			if code < 0 || uint64(code) >= uint64(cardinality) || code > int64(^uint32(0)) {
				return nil, nil, decodedBytes, blocks, fmt.Errorf("collections: dense typed-column %s code=%d outside cardinality=%d", role, code, cardinality)
			}
			out = append(out, uint32(code))
			valid = append(valid, true)
		}
		valueScratch = values
		nullScratch = nulls
		defaultScratch = defaults
		decodedBytes += uint64(g.RawBytes)
		blocks++
	}
	if len(out) != rowCount || len(valid) != rowCount {
		return nil, nil, decodedBytes, blocks, fmt.Errorf("collections: dense typed-column %s decoded rows codes/valid=%d/%d want range rows=%d", role, len(out), len(valid), rowCount)
	}
	return out, valid, decodedBytes, blocks, nil
}

// decodeTypedColumnPhysicalQueryDenseInt64SpanPart prepares the q5 typed-column
// section fast path from the adapter seam so production query routing does not
// import the typedcolumn data plane directly.
func decodeTypedColumnPhysicalQueryDenseInt64SpanPart(plan columnTypedColumnPhysicalQueryPlan, schemaHash uint64, typedRef, physical columnManifestAssetRefForScan, raw []byte, preapplyPredicates bool) (columnTypedColumnPhysicalQueryPart, error) {
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	adapterPart, err := typedColumnAdapterPartFromImageWithoutRowLocators(typedColumnAdapterOptions{
		Fields:          plan.Fields,
		SchemaVersion:   uint32(schemaHash),
		DictionaryModes: columnTypedColumnPhysicalQueryAdapterDictionaryModes(plan, []string{plan.GroupColumn}, plan.PredicateSpecs),
	}, image)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	summary := typedColumnAdapterImageSummary{PartID: image.PartID, Rows: image.Rows, Sections: len(image.Sections), SectionBytes: typedColumnPhysicalQueryImageSectionBytes(image), SortKey: columnSortKeysFromTypedColumnSortKeys(adapterPart.Part.Descriptor.SortKey)}
	if summary.PartID != typedRef.Ref.PartID || summary.Rows != typedRef.Rows {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("typed_column_part image/ref mismatch image_part=%d ref_part=%d image_rows=%d manifest_rows=%d", summary.PartID, typedRef.Ref.PartID, summary.Rows, typedRef.Rows)
	}
	if physical.Rows != 0 && summary.Rows != physical.Rows {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("typed_column_part rows=%d do not match physical rows=%d", summary.Rows, physical.Rows)
	}
	if err := validateTypedColumnPhysicalQuerySortMetadata(plan.SortKey, typedRef.SortKey, summary.SortKey); err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}

	groupColumn, groupPartColumn, cardinality, err := typedColumnDenseStringCodeColumn(adapterPart, plan.Fields, plan.GroupColumn, "int64-span group")
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	valueColumn, ok, err := typedColumnInt64PredicateAdapterColumn(plan.Fields, plan.ValueColumn)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	if !ok {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("collections: dense typed-column int64-span value column %q is not owned by typed_column_part", plan.ValueColumn)
	}
	for _, candidate := range adapterPart.Columns {
		if candidate.Definition.Name == valueColumn.Definition.Name {
			valueColumn = candidate
			break
		}
	}
	if valueColumn.Field.Nullable || valueColumn.Definition.Type != typedcolumn.ColumnTypeInt64 {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("%w: dense typed-column int64-span value column %q is not a non-null int64", ErrColumnQueryPlanUnsupported, plan.ValueColumn)
	}
	valuePartColumn, ok := adapterPart.Part.Columns[valueColumn.Definition.Name]
	if !ok {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("collections: dense typed-column int64-span missing value column %q", valueColumn.Definition.Name)
	}
	if valuePartColumn.Definition.Type != typedcolumn.ColumnTypeInt64 {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("%w: dense typed-column int64-span value column %q type=%s", ErrColumnQueryPlanUnsupported, plan.ValueColumn, valuePartColumn.Definition.Type)
	}

	var groupCodes []uint32
	var groupValid []bool
	var groupDecodedBytes uint64
	var groupBlocks int
	if groupColumn.Field.Nullable {
		groupCodes, groupValid, groupDecodedBytes, groupBlocks, err = decodeTypedColumnDenseNullableUint32Codes(groupPartColumn, cardinality, summary.Rows, "int64-span group")
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, err
		}
	} else {
		groupCodes, groupDecodedBytes, groupBlocks, err = decodeTypedColumnDenseUint32Codes(groupPartColumn, cardinality, summary.Rows, "int64-span group")
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, err
		}
	}
	values, valueDecodedBytes, valueBlocks, err := decodeTypedColumnDenseInt64Values(valuePartColumn, summary.Rows, "int64-span value")
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	if len(groupCodes) != len(values) {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("collections: dense typed-column int64-span group/value rows=%d/%d", len(groupCodes), len(values))
	}

	predicateDecodedBytes := uint64(0)
	predicateBlocks := 0
	predicateRowsPreApplied := false
	var predicateRows []uint32
	var predicates []columnTypedColumnDensePredicatePart
	if preapplyPredicates {
		var ok bool
		predicates, predicateRows, predicateDecodedBytes, predicateBlocks, ok, err = decodeTypedColumnDenseInt64SpanSingleCodeTriplePredicateRows(adapterPart, plan.Fields, plan.PredicateSpecs, summary.Rows)
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, err
		}
		predicateRowsPreApplied = ok
	}
	if !predicateRowsPreApplied {
		predicates = make([]columnTypedColumnDensePredicatePart, 0, len(plan.PredicateSpecs))
		predicateDecodedBytes = 0
		predicateBlocks = 0
		for _, spec := range plan.PredicateSpecs {
			predicate, decodedBytes, blocks, err := decodeTypedColumnDensePredicatePart(adapterPart, plan.Fields, spec, summary.Rows)
			if err != nil {
				return columnTypedColumnPhysicalQueryPart{}, err
			}
			predicates = append(predicates, predicate)
			predicateDecodedBytes += decodedBytes
			predicateBlocks += blocks
		}
	}

	denseSpan := &columnTypedColumnDenseInt64SpanPart{
		Cardinality:      cardinality,
		Dictionary:       groupColumn.DictionaryValuesByCode,
		DictionaryByCode: groupColumn.ReverseDictionary,
		GroupCodes:       groupCodes,
		GroupValid:       groupValid,
		Values:           values,
		Predicates:       predicates,
	}
	if predicateRowsPreApplied {
		denseSpan.PredicatesPreApplied = true
		denseSpan.PredicateRows = predicateRows
		denseSpan.PreAppliedRowsScanned = summary.Rows
	} else if preapplyPredicates {
		preapplyColumnTypedColumnDenseInt64SpanPredicates(denseSpan, summary.Rows)
	}

	decodedBlocks := groupBlocks + valueBlocks + predicateBlocks
	return columnTypedColumnPhysicalQueryPart{
		Ref:                 typedRef,
		PhysicalRef:         physical,
		Rows:                summary.Rows,
		Bytes:               int64(len(raw)),
		Sections:            summary.Sections,
		SectionBytes:        summary.SectionBytes,
		GranulesConsidered:  decodedBlocks,
		GranulesDecoded:     decodedBlocks,
		DecodedBlocks:       decodedBlocks,
		DecodedPayloadBytes: groupDecodedBytes + valueDecodedBytes + predicateDecodedBytes,
		DenseInt64Span:      denseSpan,
	}, nil
}

// decodeTypedColumnPhysicalQueryDenseGroupCountDistinctPart prepares the q2
// typed-column section fast path. It decodes dictionary codes for the group,
// distinct, and predicate columns once, then the runner reduces by integer code.
func decodeTypedColumnPhysicalQueryDenseGroupCountDistinctPart(plan columnTypedColumnPhysicalQueryPlan, schemaHash uint64, typedRef, physical columnManifestAssetRefForScan, raw []byte) (columnTypedColumnPhysicalQueryPart, error) {
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	adapterPart, err := typedColumnAdapterPartFromImageWithoutRowLocators(typedColumnAdapterOptions{
		Fields:          plan.Fields,
		SchemaVersion:   uint32(schemaHash),
		DictionaryModes: columnTypedColumnPhysicalQueryAdapterDictionaryModes(plan, []string{plan.GroupColumn, plan.DistinctColumn}, plan.PredicateSpecs),
	}, image)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	summary := typedColumnAdapterImageSummary{PartID: image.PartID, Rows: image.Rows, Sections: len(image.Sections), SectionBytes: typedColumnPhysicalQueryImageSectionBytes(image), SortKey: columnSortKeysFromTypedColumnSortKeys(adapterPart.Part.Descriptor.SortKey)}
	if summary.PartID != typedRef.Ref.PartID || summary.Rows != typedRef.Rows {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("typed_column_part image/ref mismatch image_part=%d ref_part=%d image_rows=%d manifest_rows=%d", summary.PartID, typedRef.Ref.PartID, summary.Rows, typedRef.Rows)
	}
	if physical.Rows != 0 && summary.Rows != physical.Rows {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("typed_column_part rows=%d do not match physical rows=%d", summary.Rows, physical.Rows)
	}
	if err := validateTypedColumnPhysicalQuerySortMetadata(plan.SortKey, typedRef.SortKey, summary.SortKey); err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}

	group, groupDecodedBytes, groupBlocks, err := typedColumnDenseGroupCountDistinctCodeColumn(adapterPart, plan.Fields, plan.GroupColumn, summary.Rows, "grouped count-distinct group")
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	distinct, distinctDecodedBytes, distinctBlocks, err := typedColumnDenseGroupCountDistinctCodeColumn(adapterPart, plan.Fields, plan.DistinctColumn, summary.Rows, "grouped count-distinct distinct")
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	if len(group.Codes) != len(distinct.Codes) {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("collections: dense typed-column grouped count-distinct group/distinct rows=%d/%d", len(group.Codes), len(distinct.Codes))
	}

	predicates := make([]columnTypedColumnDensePredicatePart, 0, len(plan.PredicateSpecs))
	predicateDecodedBytes := uint64(0)
	predicateBlocks := 0
	for _, spec := range plan.PredicateSpecs {
		predicate, decodedBytes, blocks, err := decodeTypedColumnDensePredicatePart(adapterPart, plan.Fields, spec, summary.Rows)
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, err
		}
		predicates = append(predicates, predicate)
		predicateDecodedBytes += decodedBytes
		predicateBlocks += blocks
	}

	decodedBlocks := groupBlocks + distinctBlocks + predicateBlocks
	return columnTypedColumnPhysicalQueryPart{
		Ref:                 typedRef,
		PhysicalRef:         physical,
		Rows:                summary.Rows,
		Bytes:               int64(len(raw)),
		Sections:            summary.Sections,
		SectionBytes:        summary.SectionBytes,
		GranulesConsidered:  decodedBlocks,
		GranulesDecoded:     decodedBlocks,
		DecodedBlocks:       decodedBlocks,
		DecodedPayloadBytes: groupDecodedBytes + distinctDecodedBytes + predicateDecodedBytes,
		DenseGroupCountDistinct: &columnTypedColumnDenseGroupCountDistinctPart{
			Rows:       summary.Rows,
			Group:      group,
			Distinct:   distinct,
			Predicates: predicates,
		},
	}, nil
}

func typedColumnDenseGroupCountDistinctCodeColumn(adapterPart *typedColumnAdapterPart, fields []TypedStorageField, column string, rows int, role string) (columnTypedColumnDenseStringCodeColumn, uint64, int, error) {
	adapterColumn, partColumn, cardinality, err := typedColumnDenseStringCodeColumn(adapterPart, fields, column, role)
	if err != nil {
		return columnTypedColumnDenseStringCodeColumn{}, 0, 0, err
	}
	dictionary, err := typedColumnDenseStringValuesByCode(adapterColumn, cardinality, role)
	if err != nil {
		return columnTypedColumnDenseStringCodeColumn{}, 0, 0, err
	}
	if adapterColumn.Field.Nullable {
		codes, valid, decodedBytes, blocks, err := decodeTypedColumnDenseNullableUint32Codes(partColumn, cardinality, rows, role)
		if err != nil {
			return columnTypedColumnDenseStringCodeColumn{}, 0, 0, err
		}
		return columnTypedColumnDenseStringCodeColumn{Codes: codes, Valid: valid, HasMissing: columnTypedColumnDenseValidityHasMissing(valid), HasMissingKnown: true, Dictionary: dictionary}, decodedBytes, blocks, nil
	}
	codes, decodedBytes, blocks, err := decodeTypedColumnDenseUint32Codes(partColumn, cardinality, rows, role)
	if err != nil {
		return columnTypedColumnDenseStringCodeColumn{}, 0, 0, err
	}
	return columnTypedColumnDenseStringCodeColumn{Codes: codes, HasMissingKnown: true, Dictionary: dictionary}, decodedBytes, blocks, nil
}

func typedColumnDenseGroupCountCodeColumn(adapterPart *typedColumnAdapterPart, fields []TypedStorageField, column string, rows int, role string) (columnTypedColumnDenseGroupCountPart, uint64, int, error) {
	adapterColumn, partColumn, cardinality, err := typedColumnDenseStringCodeColumn(adapterPart, fields, column, role)
	if err != nil {
		return columnTypedColumnDenseGroupCountPart{}, 0, 0, err
	}
	return typedColumnDenseGroupCountFromStringColumn(adapterColumn, partColumn, cardinality, rows, role)
}

func typedColumnDenseGroupCountFromStringColumn(adapterColumn typedColumnAdapterColumn, partColumn typedcolumn.ColumnPartColumn, cardinality int, rows int, role string) (columnTypedColumnDenseGroupCountPart, uint64, int, error) {
	dictionary, err := typedColumnDenseStringValuesByCode(adapterColumn, cardinality, role)
	if err != nil {
		return columnTypedColumnDenseGroupCountPart{}, 0, 0, err
	}
	if adapterColumn.Field.Nullable {
		counts, missing, decodedBytes, blocks, err := decodeTypedColumnDenseNullableUint32CodeCounts(partColumn, cardinality, rows, role)
		if err != nil {
			return columnTypedColumnDenseGroupCountPart{}, 0, 0, err
		}
		return columnTypedColumnDenseGroupCountPart{Cardinality: cardinality, Dictionary: dictionary, Counts: counts, Missing: missing, Rows: rows}, decodedBytes, blocks, nil
	}
	counts, decodedBytes, blocks, err := decodeTypedColumnDenseUint32CodeCounts(partColumn, cardinality, rows, role)
	if err != nil {
		return columnTypedColumnDenseGroupCountPart{}, 0, 0, err
	}
	return columnTypedColumnDenseGroupCountPart{Cardinality: cardinality, Dictionary: dictionary, Counts: counts, Rows: rows}, decodedBytes, blocks, nil
}

func typedColumnDenseStringCodeColumn(adapterPart *typedColumnAdapterPart, fields []TypedStorageField, column, role string) (typedColumnAdapterColumn, typedcolumn.ColumnPartColumn, int, error) {
	adapterColumn, ok, err := typedColumnStringPredicateAdapterColumn(fields, column)
	if err != nil {
		return typedColumnAdapterColumn{}, typedcolumn.ColumnPartColumn{}, 0, err
	}
	if !ok {
		return typedColumnAdapterColumn{}, typedcolumn.ColumnPartColumn{}, 0, fmt.Errorf("collections: dense typed-column %s column %q is not owned by typed_column_part", role, column)
	}
	for _, candidate := range adapterPart.Columns {
		if candidate.Definition.Name == adapterColumn.Definition.Name {
			adapterColumn = candidate
			break
		}
	}
	wantEncoding := typedcolumn.EncodingLowCardinalityUint32
	if adapterColumn.Field.Nullable {
		wantEncoding = typedcolumn.EncodingNullableInt64
	}
	if adapterColumn.Definition.Type != typedcolumn.ColumnTypeLowCardinalityCode || adapterColumn.Definition.Encoding != wantEncoding {
		return typedColumnAdapterColumn{}, typedcolumn.ColumnPartColumn{}, 0, fmt.Errorf("%w: dense typed-column %s column %q is not a compatible low-cardinality string", ErrColumnQueryPlanUnsupported, role, column)
	}
	partColumn, ok := adapterPart.Part.Columns[adapterColumn.Definition.Name]
	if !ok {
		return typedColumnAdapterColumn{}, typedcolumn.ColumnPartColumn{}, 0, fmt.Errorf("collections: dense typed-column %s missing column %q", role, adapterColumn.Definition.Name)
	}
	if partColumn.Definition.Type != typedcolumn.ColumnTypeLowCardinalityCode || partColumn.Definition.Encoding != wantEncoding || (!adapterColumn.Field.Nullable && partColumn.Definition.Cardinality == 0) {
		return typedColumnAdapterColumn{}, typedcolumn.ColumnPartColumn{}, 0, fmt.Errorf("%w: dense typed-column %s column %q type=%s encoding=%s cardinality=%d", ErrColumnQueryPlanUnsupported, role, column, partColumn.Definition.Type, partColumn.Definition.Encoding, partColumn.Definition.Cardinality)
	}
	if uint64(int(partColumn.Definition.Cardinality)) != uint64(partColumn.Definition.Cardinality) {
		return typedColumnAdapterColumn{}, typedcolumn.ColumnPartColumn{}, 0, fmt.Errorf("collections: dense typed-column %s cardinality=%d exceeds host int", role, partColumn.Definition.Cardinality)
	}
	cardinality := int(partColumn.Definition.Cardinality)
	if typedColumnDenseStringCodeColumnNeedsForwardDictionary(role) {
		if err := validateTypedColumnAdapterStringDictionary(adapterColumn, partColumn.Definition.Cardinality, adapterColumn.Dictionary); err != nil {
			return typedColumnAdapterColumn{}, typedcolumn.ColumnPartColumn{}, 0, fmt.Errorf("collections: dense typed-column %s forward dictionary: %w", role, err)
		}
	}
	if typedColumnDenseStringCodeColumnNeedsReverseDictionary(role) {
		if len(adapterColumn.DictionaryValuesByCode) != cardinality && len(adapterColumn.ReverseDictionary) != cardinality {
			return typedColumnAdapterColumn{}, typedcolumn.ColumnPartColumn{}, 0, fmt.Errorf("collections: dense typed-column %s dictionary cardinality values_by_code=%d reverse=%d want %d for column %q", role, len(adapterColumn.DictionaryValuesByCode), len(adapterColumn.ReverseDictionary), cardinality, adapterColumn.Definition.Name)
		}
	}
	return adapterColumn, partColumn, cardinality, nil
}

func typedColumnDenseStringValuesByCode(adapterColumn typedColumnAdapterColumn, cardinality int, role string) ([]string, error) {
	if len(adapterColumn.DictionaryValuesByCode) == cardinality {
		return adapterColumn.DictionaryValuesByCode, nil
	}
	if len(adapterColumn.ReverseDictionary) != cardinality {
		return nil, fmt.Errorf("collections: dense typed-column %s dictionary cardinality values_by_code=%d reverse=%d want %d for column %q", role, len(adapterColumn.DictionaryValuesByCode), len(adapterColumn.ReverseDictionary), cardinality, adapterColumn.Definition.Name)
	}
	dictionary := make([]string, cardinality)
	for code := 0; code < cardinality; code++ {
		value, ok := adapterColumn.ReverseDictionary[int64(code)]
		if !ok {
			return nil, fmt.Errorf("collections: dense typed-column %s dictionary missing local code %d for column %q", role, code, adapterColumn.Definition.Name)
		}
		dictionary[code] = value
	}
	return dictionary, nil
}

func typedColumnDenseStringCodeColumnNeedsForwardDictionary(role string) bool {
	return strings.Contains(role, "predicate")
}

func typedColumnDenseStringCodeColumnNeedsReverseDictionary(role string) bool {
	return !typedColumnDenseStringCodeColumnNeedsForwardDictionary(role)
}

func decodeTypedColumnDensePredicatePart(adapterPart *typedColumnAdapterPart, fields []TypedStorageField, spec columnPhysicalQueryPredicateSpec, rows int) (columnTypedColumnDensePredicatePart, uint64, int, error) {
	adapterColumn, partColumn, cardinality, err := typedColumnDenseStringCodeColumn(adapterPart, fields, spec.column, "predicate")
	if err != nil {
		return columnTypedColumnDensePredicatePart{}, 0, 0, err
	}
	allowed := make([]uint64, (cardinality+63)/64)
	matchedLiterals := 0
	singleCode := uint32(0)
	singleCodeAllowed := false
	multipleCodesAllowed := false
	missingMatchesEmpty := false
	for _, value := range spec.values {
		if adapterColumn.Field.Nullable && value == "" {
			missingMatchesEmpty = true
		}
		code, ok := adapterColumn.Dictionary[value]
		if !ok {
			continue
		}
		if code < 0 || uint64(code) >= uint64(cardinality) {
			return columnTypedColumnDensePredicatePart{}, 0, 0, fmt.Errorf("collections: dense typed-column predicate dictionary code %d outside cardinality %d for column %q", code, cardinality, adapterColumn.Definition.Name)
		}
		idx := int(code)
		mask := uint64(1) << uint(idx%64)
		if allowed[idx/64]&mask == 0 {
			if singleCodeAllowed && singleCode != uint32(idx) {
				multipleCodesAllowed = true
			} else {
				singleCode = uint32(idx)
				singleCodeAllowed = true
			}
		}
		allowed[idx/64] |= mask
		matchedLiterals++
	}
	if matchedLiterals == 0 && !missingMatchesEmpty {
		return columnTypedColumnDensePredicatePart{RejectsAll: true}, 0, 0, nil
	}
	singleCodeAllowed = singleCodeAllowed && !multipleCodesAllowed && !missingMatchesEmpty
	if adapterColumn.Field.Nullable {
		codes, valid, decodedBytes, blocks, err := decodeTypedColumnDenseNullableUint32Codes(partColumn, cardinality, rows, "predicate")
		if err != nil {
			return columnTypedColumnDensePredicatePart{}, 0, 0, err
		}
		return columnTypedColumnDensePredicatePart{Codes: codes, Valid: valid, Allowed: allowed, SingleCode: singleCode, SingleCodeAllowed: singleCodeAllowed, MissingMatchesEmpty: missingMatchesEmpty}, decodedBytes, blocks, nil
	}
	codes, decodedBytes, blocks, err := decodeTypedColumnDenseUint32Codes(partColumn, cardinality, rows, "predicate")
	if err != nil {
		return columnTypedColumnDensePredicatePart{}, 0, 0, err
	}
	return columnTypedColumnDensePredicatePart{Codes: codes, Allowed: allowed, SingleCode: singleCode, SingleCodeAllowed: singleCodeAllowed}, decodedBytes, blocks, nil
}

func typedColumnDenseParallelWorkers(blocks, rows int) int {
	if rows < typedColumnDenseParallelMinRows || blocks < typedColumnDenseParallelMinBlocks {
		return 0
	}
	procs := runtime.GOMAXPROCS(0)
	if procs <= 1 {
		return 0
	}
	workers := min(procs, blocks)
	workers = min(workers, typedColumnDenseParallelMaxWorkers)
	if workers < 2 {
		return 0
	}
	return workers
}

func typedColumnDenseDecodeLayout(partColumn typedcolumn.ColumnPartColumn, rows int, role string) ([]int, uint64, error) {
	if rows < 0 {
		return nil, 0, fmt.Errorf("collections: dense typed-column %s invalid rows=%d", role, rows)
	}
	offsets := make([]int, len(partColumn.Blocks))
	offset := 0
	decodedBytes := uint64(0)
	for blockIdx, block := range partColumn.Blocks {
		if block.Descriptor.RowCount < 0 {
			return nil, 0, fmt.Errorf("collections: dense typed-column %s block %d invalid rows=%d", role, blockIdx, block.Descriptor.RowCount)
		}
		if offset > rows || block.Descriptor.RowCount > rows-offset {
			return nil, 0, fmt.Errorf("collections: dense typed-column %s blocks exceed part rows=%d at block %d", role, rows, blockIdx)
		}
		offsets[blockIdx] = offset
		offset += block.Descriptor.RowCount
		decodedBytes += uint64(block.Granule.RawBytes)
	}
	if offset != rows {
		return nil, 0, fmt.Errorf("collections: dense typed-column %s decoded rows=%d want part rows=%d", role, offset, rows)
	}
	return offsets, decodedBytes, nil
}

func typedColumnDenseRunParallel(blocks, workers int, runBlock func(worker, blockIdx int) error) error {
	jobs := make(chan int)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for blockIdx := range jobs {
				if err := runBlock(worker, blockIdx); err != nil {
					select {
					case errs <- err:
					default:
					}
				}
			}
		}()
	}
	for blockIdx := 0; blockIdx < blocks; blockIdx++ {
		jobs <- blockIdx
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}

func decodeTypedColumnDenseUint32Codes(partColumn typedcolumn.ColumnPartColumn, cardinality int, rows int, role string) ([]uint32, uint64, int, error) {
	if workers := typedColumnDenseParallelWorkers(len(partColumn.Blocks), rows); workers > 0 {
		codes, decodedBytes, err := decodeTypedColumnDenseUint32CodesParallel(partColumn, cardinality, rows, role, workers)
		if err != nil {
			return nil, 0, 0, err
		}
		return codes, decodedBytes, len(partColumn.Blocks), nil
	}
	codes := make([]uint32, 0, rows)
	var scratch []uint32
	var reader typedcolumn.GranuleReader
	decodedBytes := uint64(0)
	for blockIdx, block := range partColumn.Blocks {
		g := block.Granule
		if g.HasMinMax {
			if g.Min < 0 || g.Max < 0 || g.Min > g.Max || uint64(g.Max) >= uint64(cardinality) {
				return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d min/max [%d,%d] outside cardinality %d", role, blockIdx, g.Min, g.Max, cardinality)
			}
		}
		decoded, err := reader.DecodeUint32CodesInto(scratch[:0], g)
		if err != nil {
			return nil, 0, 0, err
		}
		if len(decoded) != block.Descriptor.RowCount {
			return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s decoded rows=%d want %d", role, len(decoded), block.Descriptor.RowCount)
		}
		for i, code := range decoded {
			if uint64(code) >= uint64(cardinality) {
				return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s code[%d]=%d outside cardinality=%d", role, i, code, cardinality)
			}
		}
		codes = append(codes, decoded...)
		scratch = decoded
		decodedBytes += uint64(g.RawBytes)
	}
	if len(codes) != rows {
		return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s decoded rows=%d want part rows=%d", role, len(codes), rows)
	}
	return codes, decodedBytes, len(partColumn.Blocks), nil
}

func decodeTypedColumnDenseUint32CodeCounts(partColumn typedcolumn.ColumnPartColumn, cardinality int, rows int, role string) ([]int, uint64, int, error) {
	if rows < 0 {
		return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s invalid rows=%d", role, rows)
	}
	if cardinality < 0 {
		return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s invalid cardinality=%d", role, cardinality)
	}
	counts := make([]int, cardinality)
	var blockCounts []int
	var reader typedcolumn.GranuleReader
	decodedBytes := uint64(0)
	decodedRows := 0
	for blockIdx, block := range partColumn.Blocks {
		g := block.Granule
		if g.HasMinMax {
			if g.Min < 0 || g.Max < 0 || g.Min > g.Max || uint64(g.Max) >= uint64(cardinality) {
				return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d min/max [%d,%d] outside cardinality %d", role, blockIdx, g.Min, g.Max, cardinality)
			}
		}
		blockCounts, err := reader.CountUint32Codes(g, blockCounts)
		if err != nil {
			return nil, 0, 0, err
		}
		if len(blockCounts) > cardinality {
			return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d cardinality=%d exceeds part cardinality=%d", role, blockIdx, len(blockCounts), cardinality)
		}
		total := 0
		for code, count := range blockCounts {
			if count < 0 {
				return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d count[%d]=%d is negative", role, blockIdx, code, count)
			}
			if count == 0 {
				continue
			}
			counts[code] += count
			total += count
		}
		if total != block.Descriptor.RowCount {
			return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d counted rows=%d want %d", role, blockIdx, total, block.Descriptor.RowCount)
		}
		decodedRows += total
		decodedBytes += uint64(g.RawBytes)
	}
	if decodedRows != rows {
		return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s decoded rows=%d want part rows=%d", role, decodedRows, rows)
	}
	return counts, decodedBytes, len(partColumn.Blocks), nil
}

func decodeTypedColumnDenseUint32CodesParallel(partColumn typedcolumn.ColumnPartColumn, cardinality int, rows int, role string, workers int) ([]uint32, uint64, error) {
	offsets, decodedBytes, err := typedColumnDenseDecodeLayout(partColumn, rows, role)
	if err != nil {
		return nil, 0, err
	}
	codes := make([]uint32, rows)
	scratchByWorker := make([][]uint32, workers)
	readerByWorker := make([]typedcolumn.GranuleReader, workers)
	err = typedColumnDenseRunParallel(len(partColumn.Blocks), workers, func(worker, blockIdx int) error {
		block := partColumn.Blocks[blockIdx]
		g := block.Granule
		if g.HasMinMax {
			if g.Min < 0 || g.Max < 0 || g.Min > g.Max || uint64(g.Max) >= uint64(cardinality) {
				return fmt.Errorf("collections: dense typed-column %s block %d min/max [%d,%d] outside cardinality %d", role, blockIdx, g.Min, g.Max, cardinality)
			}
		}
		decoded, err := readerByWorker[worker].DecodeUint32CodesInto(scratchByWorker[worker][:0], g)
		if err != nil {
			return fmt.Errorf("collections: dense typed-column %s block %d: %w", role, blockIdx, err)
		}
		if len(decoded) != block.Descriptor.RowCount {
			return fmt.Errorf("collections: dense typed-column %s decoded rows=%d want %d", role, len(decoded), block.Descriptor.RowCount)
		}
		for i, code := range decoded {
			if uint64(code) >= uint64(cardinality) {
				return fmt.Errorf("collections: dense typed-column %s code[%d]=%d outside cardinality=%d", role, i, code, cardinality)
			}
		}
		copy(codes[offsets[blockIdx]:offsets[blockIdx]+len(decoded)], decoded)
		scratchByWorker[worker] = decoded
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return codes, decodedBytes, nil
}

func decodeTypedColumnDenseNullableUint32Codes(partColumn typedcolumn.ColumnPartColumn, cardinality int, rows int, role string) ([]uint32, []bool, uint64, int, error) {
	if workers := typedColumnDenseParallelWorkers(len(partColumn.Blocks), rows); workers > 0 {
		codes, valid, decodedBytes, err := decodeTypedColumnDenseNullableUint32CodesParallel(partColumn, cardinality, rows, role, workers)
		if err != nil {
			return nil, nil, 0, 0, err
		}
		return codes, valid, decodedBytes, len(partColumn.Blocks), nil
	}
	return decodeTypedColumnNullableUint32CodesForRowRange(partColumn, cardinality, 0, rows, role, make([]uint32, 0, rows), make([]bool, 0, rows))
}

func decodeTypedColumnDenseNullableUint32CodeCounts(partColumn typedcolumn.ColumnPartColumn, cardinality int, rows int, role string) ([]int, int, uint64, int, error) {
	if rows < 0 {
		return nil, 0, 0, 0, fmt.Errorf("collections: dense typed-column %s invalid rows=%d", role, rows)
	}
	if cardinality < 0 {
		return nil, 0, 0, 0, fmt.Errorf("collections: dense typed-column %s invalid cardinality=%d", role, cardinality)
	}
	counts := make([]int, cardinality)
	missing := 0
	var valueScratch []int64
	var nullScratch []bool
	var defaultScratch []bool
	var reader typedcolumn.GranuleReader
	decodedBytes := uint64(0)
	decodedRows := 0
	for blockIdx, block := range partColumn.Blocks {
		g := block.Granule
		if g.HasMinMax {
			if g.Min < 0 || g.Max < 0 || g.Min > g.Max || uint64(g.Max) >= uint64(cardinality) {
				return nil, 0, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d min/max [%d,%d] outside cardinality %d", role, blockIdx, g.Min, g.Max, cardinality)
			}
		}
		values, nulls, defaults, err := reader.DecodeNullableInt64Into(valueScratch[:0], nullScratch[:0], defaultScratch[:0], g)
		if err != nil {
			return nil, 0, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d: %w", role, blockIdx, err)
		}
		if len(values) != block.Descriptor.RowCount || len(nulls) != block.Descriptor.RowCount || len(defaults) != block.Descriptor.RowCount {
			return nil, 0, 0, 0, fmt.Errorf("collections: dense typed-column %s block %d decoded rows values/nulls/defaults=%d/%d/%d want %d", role, blockIdx, len(values), len(nulls), len(defaults), block.Descriptor.RowCount)
		}
		for row, code := range values {
			if nulls[row] || defaults[row] {
				missing++
				continue
			}
			if code < 0 || uint64(code) >= uint64(cardinality) || code > int64(^uint32(0)) {
				return nil, 0, 0, 0, fmt.Errorf("collections: dense typed-column %s code=%d outside cardinality=%d", role, code, cardinality)
			}
			counts[int(code)]++
		}
		decodedRows += len(values)
		valueScratch = values
		nullScratch = nulls
		defaultScratch = defaults
		decodedBytes += uint64(g.RawBytes)
	}
	if decodedRows != rows {
		return nil, 0, 0, 0, fmt.Errorf("collections: dense typed-column %s decoded rows=%d want part rows=%d", role, decodedRows, rows)
	}
	return counts, missing, decodedBytes, len(partColumn.Blocks), nil
}

func decodeTypedColumnDenseNullableUint32CodesParallel(partColumn typedcolumn.ColumnPartColumn, cardinality int, rows int, role string, workers int) ([]uint32, []bool, uint64, error) {
	offsets, decodedBytes, err := typedColumnDenseDecodeLayout(partColumn, rows, role)
	if err != nil {
		return nil, nil, 0, err
	}
	codes := make([]uint32, rows)
	valid := make([]bool, rows)
	valueScratchByWorker := make([][]int64, workers)
	nullScratchByWorker := make([][]bool, workers)
	defaultScratchByWorker := make([][]bool, workers)
	readerByWorker := make([]typedcolumn.GranuleReader, workers)
	err = typedColumnDenseRunParallel(len(partColumn.Blocks), workers, func(worker, blockIdx int) error {
		block := partColumn.Blocks[blockIdx]
		g := block.Granule
		if g.HasMinMax {
			if g.Min < 0 || g.Max < 0 || g.Min > g.Max || uint64(g.Max) >= uint64(cardinality) {
				return fmt.Errorf("collections: dense typed-column %s block %d min/max [%d,%d] outside cardinality %d", role, blockIdx, g.Min, g.Max, cardinality)
			}
		}
		values, nulls, defaults, err := readerByWorker[worker].DecodeNullableInt64Into(valueScratchByWorker[worker][:0], nullScratchByWorker[worker][:0], defaultScratchByWorker[worker][:0], g)
		if err != nil {
			return fmt.Errorf("collections: dense typed-column %s block %d: %w", role, blockIdx, err)
		}
		if len(values) != block.Descriptor.RowCount || len(nulls) != block.Descriptor.RowCount || len(defaults) != block.Descriptor.RowCount {
			return fmt.Errorf("collections: dense typed-column %s block %d decoded rows values/nulls/defaults=%d/%d/%d want %d", role, blockIdx, len(values), len(nulls), len(defaults), block.Descriptor.RowCount)
		}
		offset := offsets[blockIdx]
		for row, code := range values {
			if nulls[row] || defaults[row] {
				codes[offset+row] = 0
				valid[offset+row] = false
				continue
			}
			if code < 0 || uint64(code) >= uint64(cardinality) || code > int64(^uint32(0)) {
				return fmt.Errorf("collections: dense typed-column %s code=%d outside cardinality=%d", role, code, cardinality)
			}
			codes[offset+row] = uint32(code)
			valid[offset+row] = true
		}
		valueScratchByWorker[worker] = values
		nullScratchByWorker[worker] = nulls
		defaultScratchByWorker[worker] = defaults
		return nil
	})
	if err != nil {
		return nil, nil, 0, err
	}
	return codes, valid, decodedBytes, nil
}

func decodeTypedColumnDenseInt64Values(partColumn typedcolumn.ColumnPartColumn, rows int, role string) ([]int64, uint64, int, error) {
	if workers := typedColumnDenseParallelWorkers(len(partColumn.Blocks), rows); workers > 0 {
		values, decodedBytes, err := decodeTypedColumnDenseInt64ValuesParallel(partColumn, rows, role, workers)
		if err != nil {
			return nil, 0, 0, err
		}
		return values, decodedBytes, len(partColumn.Blocks), nil
	}
	values := make([]int64, 0, rows)
	var scratch []int64
	var reader typedcolumn.GranuleReader
	decodedBytes := uint64(0)
	for _, block := range partColumn.Blocks {
		g := block.Granule
		decoded, err := reader.DecodeInt64Into(scratch[:0], g)
		if err != nil {
			return nil, 0, 0, err
		}
		if len(decoded) != block.Descriptor.RowCount {
			return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s decoded rows=%d want %d", role, len(decoded), block.Descriptor.RowCount)
		}
		values = append(values, decoded...)
		scratch = decoded
		decodedBytes += uint64(g.RawBytes)
	}
	if len(values) != rows {
		return nil, 0, 0, fmt.Errorf("collections: dense typed-column %s decoded rows=%d want part rows=%d", role, len(values), rows)
	}
	return values, decodedBytes, len(partColumn.Blocks), nil
}

func decodeTypedColumnDenseInt64ValuesParallel(partColumn typedcolumn.ColumnPartColumn, rows int, role string, workers int) ([]int64, uint64, error) {
	offsets, decodedBytes, err := typedColumnDenseDecodeLayout(partColumn, rows, role)
	if err != nil {
		return nil, 0, err
	}
	values := make([]int64, rows)
	scratchByWorker := make([][]int64, workers)
	readerByWorker := make([]typedcolumn.GranuleReader, workers)
	err = typedColumnDenseRunParallel(len(partColumn.Blocks), workers, func(worker, blockIdx int) error {
		block := partColumn.Blocks[blockIdx]
		decoded, err := readerByWorker[worker].DecodeInt64Into(scratchByWorker[worker][:0], block.Granule)
		if err != nil {
			return fmt.Errorf("collections: dense typed-column %s block %d: %w", role, blockIdx, err)
		}
		if len(decoded) != block.Descriptor.RowCount {
			return fmt.Errorf("collections: dense typed-column %s decoded rows=%d want %d", role, len(decoded), block.Descriptor.RowCount)
		}
		copy(values[offsets[blockIdx]:offsets[blockIdx]+len(decoded)], decoded)
		scratchByWorker[worker] = decoded
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return values, decodedBytes, nil
}

func typedColumnPhysicalQueryImageSectionBytes(image typedcolumn.ColumnPartImage) uint64 {
	var out uint64
	for _, section := range image.Sections {
		if section.Length > 0 {
			out += uint64(section.Length)
		}
	}
	return out
}

func (p *typedColumnAdapterPart) scanDecodedValues() (typedColumnPartDecodedValues, error) {
	return p.scanDecodedValuesSelected(nil)
}

func (p *typedColumnAdapterPart) scanDecodedValuesSelected(selected []bool) (typedColumnPartDecodedValues, error) {
	return p.scanDecodedValuesSelectedWithPrimaryLocator(selected, false)
}

func (p *typedColumnAdapterPart) scanDecodedValuesSelectedForReconstruction(selected []bool) (typedColumnPartDecodedValues, error) {
	return p.scanDecodedValuesSelectedWithPrimaryLocator(selected, true)
}

func (p *typedColumnAdapterPart) scanDecodedValuesSelectedRows(selected []bool, rows []int) (typedColumnPartDecodedValues, typedcolumn.PartScanDiagnostics, error) {
	if p == nil || p.Part == nil {
		return typedColumnPartDecodedValues{}, typedcolumn.PartScanDiagnostics{}, errors.New("collections: nil typed-column adapter part")
	}
	if selected != nil && len(selected) != len(p.Columns) {
		return typedColumnPartDecodedValues{}, typedcolumn.PartScanDiagnostics{}, fmt.Errorf("collections: typed-column adapter projection columns=%d want %d", len(selected), len(p.Columns))
	}
	selectedRowCount := len(rows)
	if rows == nil {
		selectedRowCount = p.Part.Descriptor.RowCount
	}
	values := make([][]columnDeclaredValue, len(p.Columns))
	var outDiag typedcolumn.PartScanDiagnostics
	for i, column := range p.Columns {
		if selected != nil && !selected[i] {
			continue
		}
		columnValues, diag, err := p.scanColumnValuesRows(column.Definition.Name, rows)
		if err != nil {
			return typedColumnPartDecodedValues{}, outDiag, err
		}
		if len(columnValues) != selectedRowCount {
			return typedColumnPartDecodedValues{}, outDiag, fmt.Errorf("collections: typed-column adapter column %q rows=%d want selected rows=%d", column.Definition.Name, len(columnValues), selectedRowCount)
		}
		values[i] = columnValues
		if diag.GranulesDecoded > outDiag.GranulesDecoded {
			outDiag.GranulesDecoded = diag.GranulesDecoded
		}
		outDiag.BlocksDecoded += diag.BlocksDecoded
		outDiag.BytesDecoded += diag.BytesDecoded
	}
	outDiag.RowsScanned = selectedRowCount
	outDiag.ColumnsProjected = 0
	for _, selectedColumn := range selected {
		if selectedColumn {
			outDiag.ColumnsProjected++
		}
	}
	if selected == nil {
		outDiag.ColumnsProjected = len(p.Columns)
	}
	if p.Part != nil {
		outDiag.GranulesConsidered = len(p.Part.Descriptor.Granules)
	}
	return typedColumnPartDecodedValues{Values: values}, outDiag, nil
}

func (p *typedColumnAdapterPart) scanDecodedValuesSelectedWithPrimaryLocator(selected []bool, includePrimaryLocator bool) (typedColumnPartDecodedValues, error) {
	if p == nil || p.Part == nil {
		return typedColumnPartDecodedValues{}, errors.New("collections: nil typed-column adapter part")
	}
	if selected != nil && len(selected) != len(p.Columns) {
		return typedColumnPartDecodedValues{}, fmt.Errorf("collections: typed-column adapter projection columns=%d want %d", len(selected), len(p.Columns))
	}
	ids, err := p.scanInt64ColumnValues(typedColumnAdapterPrimaryIDColumn)
	if err != nil {
		return typedColumnPartDecodedValues{}, err
	}
	values := make([][]columnDeclaredValue, len(p.Columns))
	for i, column := range p.Columns {
		if selected != nil && !selected[i] {
			continue
		}
		columnValues, err := p.scanColumnValues(column.Definition.Name)
		if err != nil {
			return typedColumnPartDecodedValues{}, err
		}
		if len(columnValues) != len(ids) {
			return typedColumnPartDecodedValues{}, fmt.Errorf("collections: typed-column adapter column %q rows=%d want %d", column.Definition.Name, len(columnValues), len(ids))
		}
		values[i] = columnValues
	}
	var rowByPrimaryID []int
	if includePrimaryLocator {
		var err error
		rowByPrimaryID, err = typedColumnAdapterRowsByPrimaryID(ids)
		if err != nil {
			return typedColumnPartDecodedValues{}, err
		}
	}
	return typedColumnPartDecodedValues{PrimaryIDs: ids, RowByPrimaryID: rowByPrimaryID, Values: values}, nil
}

func typedColumnAdapterRowsByPrimaryID(ids []int64) ([]int, error) {
	rowByPrimaryID := make([]int, len(ids))
	for i := range rowByPrimaryID {
		rowByPrimaryID[i] = -1
	}
	for partRow, primaryID := range ids {
		if primaryID < 0 || primaryID >= int64(len(ids)) {
			return nil, fmt.Errorf("collections: typed-column reconstruction primary_id=%d outside rows=%d", primaryID, len(ids))
		}
		idx := int(primaryID)
		if rowByPrimaryID[idx] >= 0 {
			return nil, fmt.Errorf("collections: typed-column reconstruction duplicate primary_id=%d", primaryID)
		}
		rowByPrimaryID[idx] = partRow
	}
	for primaryID, partRow := range rowByPrimaryID {
		if partRow < 0 {
			return nil, fmt.Errorf("collections: typed-column reconstruction missing primary_id=%d", primaryID)
		}
	}
	return rowByPrimaryID, nil
}

func (d typedColumnPartDecodedValues) valuesForRowInto(rowIdx int, dst []columnDeclaredValue) ([]columnDeclaredValue, error) {
	if rowIdx < 0 || rowIdx >= len(d.PrimaryIDs) {
		return nil, fmt.Errorf("collections: typed-column reconstruction row_index=%d outside typed_column_part rows=%d", rowIdx, len(d.PrimaryIDs))
	}
	partRow := rowIdx
	if len(d.RowByPrimaryID) != 0 {
		if rowIdx >= len(d.RowByPrimaryID) || d.RowByPrimaryID[rowIdx] < 0 {
			return nil, fmt.Errorf("collections: typed-column reconstruction missing locator for row_index=%d", rowIdx)
		}
		partRow = d.RowByPrimaryID[rowIdx]
	} else if d.PrimaryIDs[rowIdx] != int64(rowIdx) {
		return nil, fmt.Errorf("collections: typed-column reconstruction locator=%d want row_index=%d", d.PrimaryIDs[rowIdx], rowIdx)
	}
	if cap(dst) < len(d.Values) {
		dst = make([]columnDeclaredValue, len(d.Values))
	} else {
		dst = dst[:len(d.Values)]
	}
	for i := range d.Values {
		if d.Values[i] == nil {
			dst[i] = columnDeclaredValue{}
			continue
		}
		if partRow >= len(d.Values[i]) {
			return nil, fmt.Errorf("collections: typed-column reconstruction part_row=%d outside field[%d] rows=%d", partRow, i, len(d.Values[i]))
		}
		dst[i] = d.Values[i][partRow]
	}
	return dst, nil
}

func (p *typedColumnAdapterPart) scanInt64ColumnValues(columnName string) ([]int64, error) {
	scan, err := p.Part.NewScanner().ScanProjected([]string{columnName})
	if err != nil {
		return nil, err
	}
	return scan.Columns[columnName], nil
}

func (p *typedColumnAdapterPart) scanColumnValues(columnName string) ([]columnDeclaredValue, error) {
	if p == nil || p.Part == nil {
		return nil, errors.New("collections: nil typed-column adapter part")
	}
	column, ok := p.columnByName(columnName)
	if !ok {
		return nil, fmt.Errorf("collections: typed-column adapter missing column %q", columnName)
	}
	if column.Field.Nullable {
		return p.scanNullableColumnValues(column)
	}
	if column.Field.ValueType == ColumnStoreValueFloat32 && column.Definition.Type == typedcolumn.ColumnTypeFloat32 {
		return p.scanNativeFloat32ColumnValues(column)
	}
	if column.Field.ValueType == ColumnStoreValueDouble && column.Definition.Type == typedcolumn.ColumnTypeFloat64 {
		return p.scanNativeFloat64ColumnValues(column)
	}
	if typedColumnAdapterPrimitiveScalarColumnType(column.Definition.Type) {
		return p.scanPrimitiveScalarColumnValues(column)
	}
	if column.Field.ValueType == ColumnStoreValueFloat32Vector {
		matrix, err := p.Part.DenseFloat32VectorColumn(column.Definition.Name, nil)
		if err != nil {
			return nil, err
		}
		out := make([]columnDeclaredValue, matrix.Rows)
		for i := 0; i < matrix.Rows; i++ {
			start := i * matrix.ElementsPerRow
			out[i] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, Float32Vector: append([]float32(nil), matrix.Values[start:start+matrix.ElementsPerRow]...)}
		}
		return out, nil
	}
	if columnStoreValueTypeIsDenseNumericVector(column.Field.ValueType) {
		matrix, err := p.Part.DenseFixedWidthColumn(column.Definition.Name, nil)
		if err != nil {
			return nil, err
		}
		out := make([]columnDeclaredValue, matrix.Rows)
		for i := 0; i < matrix.Rows; i++ {
			row, err := matrix.RowBytes(i)
			if err != nil {
				return nil, err
			}
			out[i] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, DenseNumericVector: append([]byte(nil), row...)}
		}
		return out, nil
	}
	if column.Field.ValueType == ColumnStoreValueByteVector {
		matrix, err := p.Part.FixedBytesColumn(column.Definition.Name, nil)
		if err != nil {
			return nil, err
		}
		out := make([]columnDeclaredValue, matrix.Rows)
		for i := 0; i < matrix.Rows; i++ {
			row, err := matrix.Row(i)
			if err != nil {
				return nil, err
			}
			out[i] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, Bytes: append([]byte(nil), row...), BytesPerRow: matrix.BytesPerRow}
		}
		return out, nil
	}
	if columnStoreValueTypeIsPackedUintVector(column.Field.ValueType) {
		matrix, err := p.Part.PackedUintColumn(column.Definition.Name, nil)
		if err != nil {
			return nil, err
		}
		out := make([]columnDeclaredValue, matrix.Rows)
		for i := 0; i < matrix.Rows; i++ {
			row, err := matrix.RowBytes(i)
			if err != nil {
				return nil, err
			}
			out[i] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, Bytes: append([]byte(nil), row...), ElementsPerRow: matrix.ElementsPerRow, BitsPerElement: matrix.BitsPerElement}
		}
		return out, nil
	}
	if column.Field.ValueType == ColumnStoreValueUint32List {
		list, err := p.Part.Uint32ListColumn(column.Definition.Name, nil, nil)
		if err != nil {
			return nil, err
		}
		out := make([]columnDeclaredValue, list.Rows)
		for i := 0; i < list.Rows; i++ {
			row, err := list.Row(i)
			if err != nil {
				return nil, err
			}
			out[i] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, Uint32List: append([]uint32{}, row...)}
		}
		return out, nil
	}
	if column.Field.ValueType == ColumnStoreValueBytes {
		bytesColumn, err := p.Part.BytesColumn(column.Definition.Name, nil, nil)
		if err != nil {
			return nil, err
		}
		out := make([]columnDeclaredValue, bytesColumn.Rows)
		for i := 0; i < bytesColumn.Rows; i++ {
			row, err := bytesColumn.Row(i)
			if err != nil {
				return nil, err
			}
			out[i] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, Bytes: append([]byte{}, row...)}
		}
		return out, nil
	}
	if column.Field.ValueType == ColumnStoreValueAdjacencyList {
		if column.Definition.Encoding == typedcolumn.EncodingRawUint32OffsetsList {
			list, err := p.Part.Uint32OffsetsListColumn(column.Definition.Name, nil, nil)
			if err != nil {
				return nil, err
			}
			out := make([]columnDeclaredValue, list.Rows)
			for i := 0; i < list.Rows; i++ {
				start := int(list.Offsets[i])
				end := int(list.Offsets[i+1])
				out[i] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, AdjacencyList: append([]uint32{}, list.Values[start:end]...)}
			}
			return out, nil
		}
		matrix, err := p.Part.DenseUint32Column(column.Definition.Name, nil)
		if err != nil {
			return nil, err
		}
		out := make([]columnDeclaredValue, matrix.Rows)
		for i := 0; i < matrix.Rows; i++ {
			start := i * matrix.ElementsPerRow
			out[i] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, AdjacencyList: append([]uint32(nil), matrix.Values[start:start+matrix.ElementsPerRow]...)}
		}
		return out, nil
	}
	scan, err := p.Part.NewScanner().ScanProjected([]string{column.Definition.Name})
	if err != nil {
		return nil, err
	}
	encoded := scan.Columns[column.Definition.Name]
	out := make([]columnDeclaredValue, len(encoded))
	for i, raw := range encoded {
		value, err := decodeTypedColumnAdapterValue(column, raw)
		if err != nil {
			return nil, fmt.Errorf("collections: typed-column adapter row %d column %q: %w", i, columnName, err)
		}
		out[i] = value
	}
	return out, nil
}

func (p *typedColumnAdapterPart) scanColumnValuesRows(columnName string, rows []int) ([]columnDeclaredValue, typedcolumn.PartScanDiagnostics, error) {
	if p == nil || p.Part == nil {
		return nil, typedcolumn.PartScanDiagnostics{}, errors.New("collections: nil typed-column adapter part")
	}
	column, ok := p.columnByName(columnName)
	if !ok {
		return nil, typedcolumn.PartScanDiagnostics{}, fmt.Errorf("collections: typed-column adapter missing column %q", columnName)
	}
	if rows != nil && len(rows) == 0 {
		return []columnDeclaredValue{}, typedcolumn.PartScanDiagnostics{RowsScanned: 0, ColumnsProjected: 1, GranulesConsidered: len(p.Part.Descriptor.Granules)}, nil
	}
	if column.Field.Nullable {
		if rows != nil {
			return nil, typedcolumn.PartScanDiagnostics{}, fmt.Errorf("%w: typed-column adapter selected-row scan does not support nullable column %q", ErrColumnQueryPlanUnsupported, columnName)
		}
		values, err := p.scanNullableColumnValues(column)
		if err != nil {
			return nil, typedcolumn.PartScanDiagnostics{}, err
		}
		diag, err := p.scanColumnValuesRowsFullDiagnostics(column)
		if err != nil {
			return nil, typedcolumn.PartScanDiagnostics{}, err
		}
		return values, diag, nil
	}
	if typedColumnAdapterPrimitiveScalarColumnType(column.Definition.Type) {
		return p.scanPrimitiveScalarColumnValuesRows(column, rows)
	}
	if columnStoreValueTypeIsDenseNumericVector(column.Field.ValueType) {
		return p.scanDenseNumericVectorColumnValuesRows(column, rows)
	}
	if column.Field.ValueType == ColumnStoreValueByteVector {
		return p.scanFixedBytesColumnValuesRows(column, rows)
	}
	if columnStoreValueTypeIsPackedUintVector(column.Field.ValueType) {
		return p.scanPackedUintVectorColumnValuesRows(column, rows)
	}
	switch column.Field.ValueType {
	case ColumnStoreValueString, ColumnStoreValueInt64, ColumnStoreValueBool:
	default:
		return nil, typedcolumn.PartScanDiagnostics{}, fmt.Errorf("%w: typed-column adapter selected-row scan does not support value_type=%q column %q", ErrColumnQueryPlanUnsupported, column.Field.ValueType, columnName)
	}
	var scan typedcolumn.ProjectedScanResult
	var err error
	if rows == nil {
		scan, err = p.Part.NewScanner().ScanProjected([]string{column.Definition.Name})
	} else {
		scan, err = p.Part.NewScanner().ScanProjectedRows([]string{column.Definition.Name}, rows)
	}
	if err != nil {
		return nil, typedcolumn.PartScanDiagnostics{}, err
	}
	encoded := scan.Columns[column.Definition.Name]
	out := make([]columnDeclaredValue, len(encoded))
	for i, raw := range encoded {
		value, err := decodeTypedColumnAdapterValue(column, raw)
		if err != nil {
			return nil, scan.Diagnostics, fmt.Errorf("collections: typed-column adapter selected row %d column %q: %w", i, columnName, err)
		}
		out[i] = value
	}
	return out, scan.Diagnostics, nil
}

func (p *typedColumnAdapterPart) scanDenseNumericVectorColumnValuesRows(column typedColumnAdapterColumn, rows []int) ([]columnDeclaredValue, typedcolumn.PartScanDiagnostics, error) {
	if rows != nil {
		return p.scanDenseNumericVectorColumnSelectedRows(column, rows)
	}
	diag, err := p.scanColumnValuesRowsFullDiagnostics(column)
	if err != nil {
		return nil, typedcolumn.PartScanDiagnostics{}, err
	}
	matrix, err := p.Part.DenseFixedWidthColumn(column.Definition.Name, nil)
	if err != nil {
		return nil, diag, err
	}
	out := make([]columnDeclaredValue, matrix.Rows)
	for i := 0; i < matrix.Rows; i++ {
		row, err := matrix.RowBytes(i)
		if err != nil {
			return nil, diag, err
		}
		out[i] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, DenseNumericVector: append([]byte(nil), row...)}
	}
	return out, diag, nil
}

func (p *typedColumnAdapterPart) scanDenseNumericVectorColumnSelectedRows(column typedColumnAdapterColumn, rows []int) ([]columnDeclaredValue, typedcolumn.PartScanDiagnostics, error) {
	diag := typedcolumn.PartScanDiagnostics{RowsScanned: len(rows), ColumnsProjected: 1, GranulesConsidered: len(p.Part.Descriptor.Granules)}
	if err := typedColumnAdapterValidateSelectedRows(rows, p.Part.Descriptor.RowCount); err != nil {
		return nil, typedcolumn.PartScanDiagnostics{}, err
	}
	partColumn, ok := p.Part.Columns[column.Definition.Name]
	if !ok {
		return nil, typedcolumn.PartScanDiagnostics{}, fmt.Errorf("collections: typed-column adapter missing column %q", column.Definition.Name)
	}
	width, ok := typedcolumn.DenseFixedWidthVectorElementWidth(column.Definition.Type)
	if !ok || column.Definition.Type == typedcolumn.ColumnTypeFloat32Vector {
		return nil, typedcolumn.PartScanDiagnostics{}, fmt.Errorf("collections: typed-column adapter column %q type=%s is not dense numeric vector", column.Definition.Name, column.Definition.Type)
	}
	rowBytes, err := typedColumnAdapterDenseBytes(1, column.Definition.FixedWidthElements, width)
	if err != nil {
		return nil, typedcolumn.PartScanDiagnostics{}, err
	}
	out := make([]columnDeclaredValue, 0, len(rows))
	var reader typedcolumn.GranuleReader
	var scratch []byte
	coveredStart := -1
	coveredEnd := -1
	prevFirstGranule := -1
	rowIndex := 0
	for blockIdx, block := range partColumn.Blocks {
		first := block.Descriptor.FirstRow
		limit := first + block.Descriptor.RowCount
		if first < 0 || block.Descriptor.RowCount < 0 || first > p.Part.Descriptor.RowCount-block.Descriptor.RowCount {
			return nil, diag, fmt.Errorf("collections: typed-column adapter column %q block %d rows %d..%d outside part rows=%d", column.Definition.Name, blockIdx, first, limit, p.Part.Descriptor.RowCount)
		}
		for rowIndex < len(rows) && rows[rowIndex] < first {
			return nil, diag, fmt.Errorf("collections: typed-column adapter selected row %d before block %d first row %d", rows[rowIndex], blockIdx, first)
		}
		start := rowIndex
		for rowIndex < len(rows) && rows[rowIndex] < limit {
			rowIndex++
		}
		if start == rowIndex {
			continue
		}
		values, err := reader.DecodeDenseFixedWidthInto(scratch[:0], block.Granule, column.Definition.FixedWidthElements, width)
		if err != nil {
			return nil, diag, fmt.Errorf("collections: typed-column adapter dense vector column %q block %d: %w", column.Definition.Name, blockIdx, err)
		}
		scratch = values
		wantBytes, err := typedColumnAdapterDenseBytes(block.Descriptor.RowCount, column.Definition.FixedWidthElements, width)
		if err != nil {
			return nil, diag, err
		}
		if len(values) != wantBytes {
			return nil, diag, fmt.Errorf("collections: typed-column adapter dense vector column %q block %d decoded bytes=%d want %d", column.Definition.Name, blockIdx, len(values), wantBytes)
		}
		for _, selectedRow := range rows[start:rowIndex] {
			offset := (selectedRow - first) * rowBytes
			out = append(out, columnDeclaredValue{Type: column.Field.ValueType, Present: true, DenseNumericVector: append([]byte(nil), values[offset:offset+rowBytes]...)})
		}
		diag.BlocksDecoded++
		diag.BytesDecoded += block.Granule.RawBytes
		if block.Descriptor.FirstGranule < 0 || block.Descriptor.LastGranule < block.Descriptor.FirstGranule {
			return nil, diag, fmt.Errorf("collections: typed-column adapter invalid granule range %d..%d for column %q", block.Descriptor.FirstGranule, block.Descriptor.LastGranule, column.Definition.Name)
		}
		if prevFirstGranule >= 0 && block.Descriptor.FirstGranule < prevFirstGranule {
			return nil, diag, fmt.Errorf("collections: typed-column adapter granule ranges out of order for column %q: %d after %d", column.Definition.Name, block.Descriptor.FirstGranule, prevFirstGranule)
		}
		prevFirstGranule = block.Descriptor.FirstGranule
		coveredStart, coveredEnd = typedColumnAdapterExtendGranuleCoverage(coveredStart, coveredEnd, block.Descriptor.FirstGranule, block.Descriptor.LastGranule, &diag.GranulesDecoded)
	}
	if coveredStart >= 0 {
		diag.GranulesDecoded += coveredEnd - coveredStart + 1
	}
	if rowIndex != len(rows) {
		return nil, diag, fmt.Errorf("collections: typed-column adapter %d selected rows outside column %q blocks", len(rows)-rowIndex, column.Definition.Name)
	}
	return out, diag, nil
}

func (p *typedColumnAdapterPart) scanFixedBytesColumnValuesRows(column typedColumnAdapterColumn, rows []int) ([]columnDeclaredValue, typedcolumn.PartScanDiagnostics, error) {
	if rows != nil {
		return p.scanFixedRowByteColumnSelectedRows(column, rows, column.Definition.FixedWidthElements, "byte_vector", func(reader *typedcolumn.GranuleReader, scratch []byte, granule typedcolumn.EncodedGranule) ([]byte, error) {
			return reader.DecodeFixedBytesInto(scratch, granule, column.Definition.FixedWidthElements)
		})
	}
	diag, err := p.scanColumnValuesRowsFullDiagnostics(column)
	if err != nil {
		return nil, typedcolumn.PartScanDiagnostics{}, err
	}
	matrix, err := p.Part.FixedBytesColumn(column.Definition.Name, nil)
	if err != nil {
		return nil, diag, err
	}
	out := make([]columnDeclaredValue, matrix.Rows)
	for i := 0; i < matrix.Rows; i++ {
		row, err := matrix.Row(i)
		if err != nil {
			return nil, diag, err
		}
		out[i] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, Bytes: append([]byte(nil), row...), BytesPerRow: matrix.BytesPerRow}
	}
	return out, diag, nil
}

func (p *typedColumnAdapterPart) scanPackedUintVectorColumnValuesRows(column typedColumnAdapterColumn, rows []int) ([]columnDeclaredValue, typedcolumn.PartScanDiagnostics, error) {
	_, _, bitsPerElement, ok := typedColumnPackedUintVectorMapping(column.Field.ValueType)
	if !ok {
		return nil, typedcolumn.PartScanDiagnostics{}, fmt.Errorf("%w: unsupported packed uint vector value_type=%s", errTypedColumnAdapterUnsupportedType, column.Field.ValueType)
	}
	rowBytes, err := typedcolumn.PackedUintRowBytes(column.Definition.FixedWidthElements, bitsPerElement)
	if err != nil {
		return nil, typedcolumn.PartScanDiagnostics{}, err
	}
	if rows != nil {
		return p.scanFixedRowByteColumnSelectedRows(column, rows, rowBytes, string(column.Field.ValueType), func(reader *typedcolumn.GranuleReader, scratch []byte, granule typedcolumn.EncodedGranule) ([]byte, error) {
			return reader.DecodePackedUintInto(scratch, granule, column.Definition.FixedWidthElements, bitsPerElement)
		})
	}
	diag, err := p.scanColumnValuesRowsFullDiagnostics(column)
	if err != nil {
		return nil, typedcolumn.PartScanDiagnostics{}, err
	}
	matrix, err := p.Part.PackedUintColumn(column.Definition.Name, nil)
	if err != nil {
		return nil, diag, err
	}
	out := make([]columnDeclaredValue, matrix.Rows)
	for i := 0; i < matrix.Rows; i++ {
		row, err := matrix.RowBytes(i)
		if err != nil {
			return nil, diag, err
		}
		out[i] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, Bytes: append([]byte(nil), row...), ElementsPerRow: matrix.ElementsPerRow, BitsPerElement: matrix.BitsPerElement}
	}
	return out, diag, nil
}

func (p *typedColumnAdapterPart) scanFixedRowByteColumnSelectedRows(column typedColumnAdapterColumn, rows []int, rowBytes int, label string, decode func(*typedcolumn.GranuleReader, []byte, typedcolumn.EncodedGranule) ([]byte, error)) ([]columnDeclaredValue, typedcolumn.PartScanDiagnostics, error) {
	diag := typedcolumn.PartScanDiagnostics{RowsScanned: len(rows), ColumnsProjected: 1, GranulesConsidered: len(p.Part.Descriptor.Granules)}
	if err := typedColumnAdapterValidateSelectedRows(rows, p.Part.Descriptor.RowCount); err != nil {
		return nil, typedcolumn.PartScanDiagnostics{}, err
	}
	partColumn, ok := p.Part.Columns[column.Definition.Name]
	if !ok {
		return nil, typedcolumn.PartScanDiagnostics{}, fmt.Errorf("collections: typed-column adapter missing column %q", column.Definition.Name)
	}
	if rowBytes <= 0 {
		return nil, typedcolumn.PartScanDiagnostics{}, fmt.Errorf("collections: typed-column adapter column %q invalid row bytes=%d", column.Definition.Name, rowBytes)
	}
	out := make([]columnDeclaredValue, 0, len(rows))
	var reader typedcolumn.GranuleReader
	var scratch []byte
	coveredStart := -1
	coveredEnd := -1
	prevFirstGranule := -1
	rowIndex := 0
	for blockIdx, block := range partColumn.Blocks {
		first := block.Descriptor.FirstRow
		limit := first + block.Descriptor.RowCount
		if first < 0 || block.Descriptor.RowCount < 0 || first > p.Part.Descriptor.RowCount-block.Descriptor.RowCount {
			return nil, diag, fmt.Errorf("collections: typed-column adapter column %q block %d rows %d..%d outside part rows=%d", column.Definition.Name, blockIdx, first, limit, p.Part.Descriptor.RowCount)
		}
		for rowIndex < len(rows) && rows[rowIndex] < first {
			return nil, diag, fmt.Errorf("collections: typed-column adapter selected row %d before block %d first row %d", rows[rowIndex], blockIdx, first)
		}
		start := rowIndex
		for rowIndex < len(rows) && rows[rowIndex] < limit {
			rowIndex++
		}
		if start == rowIndex {
			continue
		}
		values, err := decode(&reader, scratch[:0], block.Granule)
		if err != nil {
			return nil, diag, fmt.Errorf("collections: typed-column adapter %s column %q block %d: %w", label, column.Definition.Name, blockIdx, err)
		}
		scratch = values
		wantBytes, err := typedColumnAdapterDenseBytes(block.Descriptor.RowCount, rowBytes, 1)
		if err != nil {
			return nil, diag, err
		}
		if len(values) != wantBytes {
			return nil, diag, fmt.Errorf("collections: typed-column adapter %s column %q block %d decoded bytes=%d want %d", label, column.Definition.Name, blockIdx, len(values), wantBytes)
		}
		for _, selectedRow := range rows[start:rowIndex] {
			offset := (selectedRow - first) * rowBytes
			value := columnDeclaredValue{Type: column.Field.ValueType, Present: true, Bytes: append([]byte(nil), values[offset:offset+rowBytes]...)}
			if column.Field.ValueType == ColumnStoreValueByteVector {
				value.BytesPerRow = rowBytes
			} else if bitsPerElement, ok := columnStorePackedUintVectorBits(column.Field.ValueType); ok {
				value.ElementsPerRow = column.Definition.FixedWidthElements
				value.BitsPerElement = bitsPerElement
			}
			out = append(out, value)
		}
		diag.BlocksDecoded++
		diag.BytesDecoded += block.Granule.RawBytes
		if block.Descriptor.FirstGranule < 0 || block.Descriptor.LastGranule < block.Descriptor.FirstGranule {
			return nil, diag, fmt.Errorf("collections: typed-column adapter invalid granule range %d..%d for column %q", block.Descriptor.FirstGranule, block.Descriptor.LastGranule, column.Definition.Name)
		}
		if prevFirstGranule >= 0 && block.Descriptor.FirstGranule < prevFirstGranule {
			return nil, diag, fmt.Errorf("collections: typed-column adapter granule ranges out of order for column %q: %d after %d", column.Definition.Name, block.Descriptor.FirstGranule, prevFirstGranule)
		}
		prevFirstGranule = block.Descriptor.FirstGranule
		coveredStart, coveredEnd = typedColumnAdapterExtendGranuleCoverage(coveredStart, coveredEnd, block.Descriptor.FirstGranule, block.Descriptor.LastGranule, &diag.GranulesDecoded)
	}
	if coveredStart >= 0 {
		diag.GranulesDecoded += coveredEnd - coveredStart + 1
	}
	if rowIndex != len(rows) {
		return nil, diag, fmt.Errorf("collections: typed-column adapter %d selected rows outside column %q blocks", len(rows)-rowIndex, column.Definition.Name)
	}
	return out, diag, nil
}

func (p *typedColumnAdapterPart) scanColumnValuesRowsFullDiagnostics(column typedColumnAdapterColumn) (typedcolumn.PartScanDiagnostics, error) {
	if p == nil || p.Part == nil {
		return typedcolumn.PartScanDiagnostics{}, errors.New("collections: nil typed-column adapter part")
	}
	partColumn, ok := p.Part.Columns[column.Definition.Name]
	if !ok {
		return typedcolumn.PartScanDiagnostics{}, fmt.Errorf("collections: typed-column adapter missing column %q", column.Definition.Name)
	}
	diag := typedcolumn.PartScanDiagnostics{
		RowsScanned:        p.Part.Descriptor.RowCount,
		ColumnsProjected:   1,
		GranulesConsidered: len(p.Part.Descriptor.Granules),
	}
	coveredStart := -1
	coveredEnd := -1
	prevFirstGranule := -1
	for _, block := range partColumn.Blocks {
		if block.Descriptor.FirstGranule < 0 || block.Descriptor.LastGranule < block.Descriptor.FirstGranule {
			return typedcolumn.PartScanDiagnostics{}, fmt.Errorf("collections: typed-column adapter invalid granule range %d..%d for column %q", block.Descriptor.FirstGranule, block.Descriptor.LastGranule, column.Definition.Name)
		}
		if prevFirstGranule >= 0 && block.Descriptor.FirstGranule < prevFirstGranule {
			return typedcolumn.PartScanDiagnostics{}, fmt.Errorf("collections: typed-column adapter granule ranges out of order for column %q: %d after %d", column.Definition.Name, block.Descriptor.FirstGranule, prevFirstGranule)
		}
		prevFirstGranule = block.Descriptor.FirstGranule
		coveredStart, coveredEnd = typedColumnAdapterExtendGranuleCoverage(coveredStart, coveredEnd, block.Descriptor.FirstGranule, block.Descriptor.LastGranule, &diag.GranulesDecoded)
		diag.BlocksDecoded++
		diag.BytesDecoded += block.Granule.RawBytes
	}
	if coveredStart >= 0 {
		diag.GranulesDecoded += coveredEnd - coveredStart + 1
	}
	return diag, nil
}

func typedColumnAdapterExtendGranuleCoverage(coveredStart, coveredEnd, first, last int, total *int) (int, int) {
	if coveredStart < 0 {
		return first, last
	}
	if first <= coveredEnd+1 {
		if last > coveredEnd {
			coveredEnd = last
		}
		return coveredStart, coveredEnd
	}
	*total += coveredEnd - coveredStart + 1
	return first, last
}

func (p *typedColumnAdapterPart) scanNativeFloat32ColumnValues(column typedColumnAdapterColumn) ([]columnDeclaredValue, error) {
	partColumn, ok := p.Part.Columns[column.Definition.Name]
	if !ok {
		return nil, fmt.Errorf("collections: typed-column adapter missing column %q", column.Definition.Name)
	}
	if partColumn.Definition.Type != typedcolumn.ColumnTypeFloat32 || partColumn.Definition.Encoding != typedcolumn.EncodingRawFloat32 {
		return nil, fmt.Errorf("collections: typed-column adapter native float32 column %q type=%s encoding=%s", column.Definition.Name, partColumn.Definition.Type, partColumn.Definition.Encoding)
	}
	out := make([]columnDeclaredValue, p.Part.Descriptor.RowCount)
	var reader typedcolumn.GranuleReader
	var scratch []float32
	for blockIdx, block := range partColumn.Blocks {
		values, err := reader.DecodeFloat32Into(scratch[:0], block.Granule)
		if err != nil {
			return nil, fmt.Errorf("collections: typed-column adapter native float32 column %q block %d: %w", column.Definition.Name, blockIdx, err)
		}
		scratch = values
		if len(values) != block.Descriptor.RowCount {
			return nil, fmt.Errorf("collections: typed-column adapter native float32 column %q block %d decoded rows=%d want %d", column.Definition.Name, blockIdx, len(values), block.Descriptor.RowCount)
		}
		for i, value := range values {
			row := block.Descriptor.FirstRow + i
			if row < 0 || row >= len(out) {
				return nil, fmt.Errorf("collections: typed-column adapter native float32 column %q block %d row %d outside rows=%d", column.Definition.Name, blockIdx, row, len(out))
			}
			out[row] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, Float32: value}
		}
	}
	return out, nil
}

func (p *typedColumnAdapterPart) scanNativeFloat64ColumnValues(column typedColumnAdapterColumn) ([]columnDeclaredValue, error) {
	partColumn, ok := p.Part.Columns[column.Definition.Name]
	if !ok {
		return nil, fmt.Errorf("collections: typed-column adapter missing column %q", column.Definition.Name)
	}
	if partColumn.Definition.Type != typedcolumn.ColumnTypeFloat64 || partColumn.Definition.Encoding != typedcolumn.EncodingRawFloat64 {
		return nil, fmt.Errorf("collections: typed-column adapter native float64 column %q type=%s encoding=%s", column.Definition.Name, partColumn.Definition.Type, partColumn.Definition.Encoding)
	}
	out := make([]columnDeclaredValue, p.Part.Descriptor.RowCount)
	var reader typedcolumn.GranuleReader
	var scratch []float64
	for blockIdx, block := range partColumn.Blocks {
		values, err := reader.DecodeFloat64Into(scratch[:0], block.Granule)
		if err != nil {
			return nil, fmt.Errorf("collections: typed-column adapter native float64 column %q block %d: %w", column.Definition.Name, blockIdx, err)
		}
		scratch = values
		if len(values) != block.Descriptor.RowCount {
			return nil, fmt.Errorf("collections: typed-column adapter native float64 column %q block %d decoded rows=%d want %d", column.Definition.Name, blockIdx, len(values), block.Descriptor.RowCount)
		}
		for i, value := range values {
			row := block.Descriptor.FirstRow + i
			if row < 0 || row >= len(out) {
				return nil, fmt.Errorf("collections: typed-column adapter native float64 column %q block %d row %d outside rows=%d", column.Definition.Name, blockIdx, row, len(out))
			}
			out[row] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, Double: value}
		}
	}
	return out, nil
}

type typedColumnAdapterNullableScanScratch struct {
	reader   typedcolumn.GranuleReader
	values   []int64
	nulls    []bool
	defaults []bool
}

func (p *typedColumnAdapterPart) scanNullableColumnValues(column typedColumnAdapterColumn) ([]columnDeclaredValue, error) {
	return p.scanNullableColumnValuesInto(column, nil, nil)
}

func (p *typedColumnAdapterPart) scanNullableColumnValuesInto(column typedColumnAdapterColumn, dst []columnDeclaredValue, scratch *typedColumnAdapterNullableScanScratch) ([]columnDeclaredValue, error) {
	partColumn, ok := p.Part.Columns[column.Definition.Name]
	if !ok {
		return nil, fmt.Errorf("collections: typed-column adapter missing column %q", column.Definition.Name)
	}
	if partColumn.Definition.Encoding != typedcolumn.EncodingNullableInt64 {
		return nil, fmt.Errorf("collections: typed-column adapter nullable column %q encoding=%s want %s", column.Definition.Name, partColumn.Definition.Encoding, typedcolumn.EncodingNullableInt64)
	}
	rows := p.Part.Descriptor.RowCount
	out := dst[:0]
	if cap(out) < rows {
		out = make([]columnDeclaredValue, rows)
	} else {
		out = out[:rows]
	}
	if scratch == nil {
		var local typedColumnAdapterNullableScanScratch
		scratch = &local
	}
	for blockIdx, block := range partColumn.Blocks {
		decodedValues, decodedNulls, decodedDefaults, err := scratch.reader.DecodeNullableInt64Into(scratch.values[:0], scratch.nulls[:0], scratch.defaults[:0], block.Granule)
		if err != nil {
			return nil, fmt.Errorf("collections: typed-column adapter nullable column %q block %d: %w", column.Definition.Name, blockIdx, err)
		}
		scratch.values, scratch.nulls, scratch.defaults = decodedValues, decodedNulls, decodedDefaults
		if len(scratch.values) != block.Descriptor.RowCount || len(scratch.nulls) != block.Descriptor.RowCount || len(scratch.defaults) != block.Descriptor.RowCount {
			return nil, fmt.Errorf("collections: typed-column adapter nullable column %q block %d decoded rows mismatch", column.Definition.Name, blockIdx)
		}
		for i, raw := range scratch.values {
			row := block.Descriptor.FirstRow + i
			if row < 0 || row >= len(out) {
				return nil, fmt.Errorf("collections: typed-column adapter nullable column %q block %d row %d outside rows=%d", column.Definition.Name, blockIdx, row, len(out))
			}
			switch {
			case scratch.nulls[i]:
				out[row] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, Null: true}
			case scratch.defaults[i]:
				out[row] = columnDeclaredValue{Type: column.Field.ValueType, Present: false, Null: true}
			default:
				value, err := decodeTypedColumnAdapterValue(column, raw)
				if err != nil {
					return nil, fmt.Errorf("collections: typed-column adapter row %d column %q: %w", row, column.Definition.Name, err)
				}
				out[row] = value
			}
		}
	}
	return out, nil
}

func (p *typedColumnAdapterPart) scanRows() ([]typedColumnAdapterRow, error) {
	if p == nil || p.Part == nil {
		return nil, errors.New("collections: nil typed-column adapter part")
	}
	scan, err := p.Part.NewScanner().ScanProjected([]string{typedColumnAdapterPrimaryIDColumn})
	if err != nil {
		return nil, err
	}
	ids := scan.Columns[typedColumnAdapterPrimaryIDColumn]
	columnValues := make(map[string][]columnDeclaredValue, len(p.Columns))
	for _, column := range p.Columns {
		values, err := p.scanColumnValues(column.Definition.Name)
		if err != nil {
			return nil, err
		}
		if len(values) != len(ids) {
			return nil, fmt.Errorf("collections: typed-column adapter column %q rows=%d want %d", column.Definition.Name, len(values), len(ids))
		}
		columnValues[column.Definition.Name] = values
	}
	rows := make([]typedColumnAdapterRow, len(ids))
	for rowIdx, id := range ids {
		values := make(map[string]columnDeclaredValue, len(p.Columns))
		for _, column := range p.Columns {
			values[column.Field.Path] = columnValues[column.Definition.Name][rowIdx]
		}
		rows[rowIdx] = typedColumnAdapterRow{PrimaryID: id, Values: values}
	}
	return rows, nil
}

func (p *typedColumnAdapterPart) columnByName(name string) (typedColumnAdapterColumn, bool) {
	for _, column := range p.Columns {
		if column.Field.Name == name || column.Field.Path == name || column.Definition.Name == name {
			return column, true
		}
	}
	return typedColumnAdapterColumn{}, false
}

func encodeTypedColumnAdapterScalarValue(column typedColumnAdapterColumn, value columnDeclaredValue) (int64, bool, bool, error) {
	if err := validateTypedColumnAdapterDeclaredValue(column, value); err != nil {
		return 0, false, false, err
	}
	if !value.Present {
		return 0, false, true, nil
	}
	if value.Null {
		return 0, true, false, nil
	}
	encoded, err := encodeTypedColumnAdapterValue(column, value)
	return encoded, false, false, err
}

func encodeTypedColumnAdapterStringDictionaryValue(column typedColumnAdapterColumn, value columnDeclaredValue, state *typedColumnAdapterStringDictionaryBuildState) (int64, bool, bool, error) {
	if state == nil {
		return encodeTypedColumnAdapterScalarValue(column, value)
	}
	if !typedColumnAdapterFusedStringDictionaryColumn(column) {
		return 0, false, false, fmt.Errorf("%w: %s is not a string dictionary column", errTypedColumnAdapterUnsupportedType, column.Field.ValueType)
	}
	if err := validateTypedColumnAdapterDeclaredValue(column, value); err != nil {
		return 0, false, false, err
	}
	if !value.Present {
		return 0, false, true, nil
	}
	if value.Null {
		return 0, true, false, nil
	}
	return state.temporaryCode(value.String), false, false, nil
}

func typedColumnAdapterFusedStringDictionaryColumn(column typedColumnAdapterColumn) bool {
	return column.Field.ValueType == ColumnStoreValueString &&
		column.Definition.Type == typedcolumn.ColumnTypeLowCardinalityCode
}

func encodeTypedColumnAdapterValue(column typedColumnAdapterColumn, value columnDeclaredValue) (int64, error) {
	if err := validateTypedColumnAdapterDeclaredValue(column, value); err != nil {
		return 0, err
	}
	switch column.Field.ValueType {
	case ColumnStoreValueBool:
		if value.Bool {
			return 1, nil
		}
		return 0, nil
	case ColumnStoreValueInt64:
		return value.Int64, nil
	case ColumnStoreValueFloat32:
		return int64(math.Float32bits(value.Float32)), nil
	case ColumnStoreValueDouble:
		return int64(math.Float64bits(value.Double)), nil
	case ColumnStoreValueString:
		code, ok := column.Dictionary[value.String]
		if !ok {
			return 0, fmt.Errorf("string value %q missing dictionary code", value.String)
		}
		return code, nil
	default:
		return 0, fmt.Errorf("%w: %s", errTypedColumnAdapterUnsupportedType, column.Field.ValueType)
	}
}

func encodeTypedColumnAdapterNativeFloat32Value(column typedColumnAdapterColumn, value columnDeclaredValue) (float32, error) {
	if column.Field.ValueType != ColumnStoreValueFloat32 || column.Definition.Type != typedcolumn.ColumnTypeFloat32 || column.Definition.Encoding != typedcolumn.EncodingRawFloat32 {
		return 0, fmt.Errorf("%w: %s is not native raw float32", errTypedColumnAdapterUnsupportedType, column.Field.ValueType)
	}
	if err := validateTypedColumnAdapterDeclaredValue(column, value); err != nil {
		return 0, err
	}
	return value.Float32, nil
}

func encodeTypedColumnAdapterNativeFloat64Value(column typedColumnAdapterColumn, value columnDeclaredValue) (float64, error) {
	if column.Field.ValueType != ColumnStoreValueDouble || column.Definition.Type != typedcolumn.ColumnTypeFloat64 || column.Definition.Encoding != typedcolumn.EncodingRawFloat64 {
		return 0, fmt.Errorf("%w: %s is not native raw float64", errTypedColumnAdapterUnsupportedType, column.Field.ValueType)
	}
	if err := validateTypedColumnAdapterDeclaredValue(column, value); err != nil {
		return 0, err
	}
	return value.Double, nil
}

func encodeTypedColumnAdapterFloat32VectorValue(dst []float32, rowIdx int, column typedColumnAdapterColumn, value columnDeclaredValue) error {
	if err := validateTypedColumnAdapterDeclaredValue(column, value); err != nil {
		return err
	}
	dims := column.Definition.FixedWidthElements
	if dims <= 0 {
		return fmt.Errorf("float32_vector fixed-width elements=%d", dims)
	}
	if len(value.Float32Vector) != dims {
		return fmt.Errorf("float32_vector length=%d want vector_dims=%d", len(value.Float32Vector), dims)
	}
	start, err := typedColumnAdapterDenseElements(rowIdx, dims)
	if err != nil {
		return err
	}
	if start > len(dst)-dims {
		return fmt.Errorf("float32_vector row %d outside destination", rowIdx)
	}
	copy(dst[start:start+dims], value.Float32Vector)
	return nil
}

func encodeTypedColumnAdapterDenseNumericVectorValue(dst typedcolumn.RawDenseFixedWidth, rowIdx int, column typedColumnAdapterColumn, value columnDeclaredValue) error {
	if err := validateTypedColumnAdapterDeclaredValue(column, value); err != nil {
		return err
	}
	_, _, width, ok := typedColumnDenseNumericVectorMapping(column.Field.ValueType)
	if !ok {
		return fmt.Errorf("%w: unsupported dense numeric vector value_type=%s", errTypedColumnAdapterUnsupportedType, column.Field.ValueType)
	}
	if dst.ElementsPerRow != column.Definition.FixedWidthElements || dst.ElementWidthBytes != width {
		return fmt.Errorf("%s metadata elements_per_row=%d width=%d want elements_per_row=%d width=%d", column.Field.ValueType, dst.ElementsPerRow, dst.ElementWidthBytes, column.Definition.FixedWidthElements, width)
	}
	rowBytes, err := typedColumnAdapterDenseBytes(1, column.Definition.FixedWidthElements, width)
	if err != nil {
		return err
	}
	if len(value.DenseNumericVector) != rowBytes {
		return fmt.Errorf("%s bytes=%d want elements_per_row=%d width=%d bytes=%d", column.Field.ValueType, len(value.DenseNumericVector), column.Definition.FixedWidthElements, width, rowBytes)
	}
	start, err := typedColumnAdapterDenseBytes(rowIdx, column.Definition.FixedWidthElements, width)
	if err != nil {
		return err
	}
	if start > len(dst.Values)-rowBytes {
		return fmt.Errorf("%s row %d outside destination", column.Field.ValueType, rowIdx)
	}
	copy(dst.Values[start:start+rowBytes], value.DenseNumericVector)
	return nil
}

func encodeTypedColumnAdapterFixedBytesValue(dst typedcolumn.FixedBytesRows, rowIdx int, column typedColumnAdapterColumn, value columnDeclaredValue) error {
	if err := validateTypedColumnAdapterDeclaredValue(column, value); err != nil {
		return err
	}
	if column.Definition.Type != typedcolumn.ColumnTypeFixedBytes || column.Definition.Encoding != typedcolumn.EncodingRawFixedBytes {
		return fmt.Errorf("byte_vector column type=%s encoding=%s", column.Definition.Type, column.Definition.Encoding)
	}
	if dst.BytesPerRow != column.Definition.FixedWidthElements {
		return fmt.Errorf("byte_vector bytes_per_row=%d want fixed_width_elements=%d", dst.BytesPerRow, column.Definition.FixedWidthElements)
	}
	if len(value.Bytes) != dst.BytesPerRow {
		return fmt.Errorf("byte_vector bytes=%d want bytes_per_row=%d", len(value.Bytes), dst.BytesPerRow)
	}
	start, err := typedColumnAdapterDenseBytes(rowIdx, dst.BytesPerRow, 1)
	if err != nil {
		return err
	}
	if start > len(dst.Values)-dst.BytesPerRow {
		return fmt.Errorf("byte_vector row %d outside destination", rowIdx)
	}
	copy(dst.Values[start:start+dst.BytesPerRow], value.Bytes)
	return nil
}

func encodeTypedColumnAdapterPackedUintVectorValue(dst typedcolumn.PackedUintRows, rowIdx int, column typedColumnAdapterColumn, value columnDeclaredValue) error {
	if err := validateTypedColumnAdapterDeclaredValue(column, value); err != nil {
		return err
	}
	wantType, wantEncoding, wantBits, ok := typedColumnPackedUintVectorMapping(column.Field.ValueType)
	if !ok {
		return fmt.Errorf("%w: unsupported packed uint vector value_type=%s", errTypedColumnAdapterUnsupportedType, column.Field.ValueType)
	}
	if column.Definition.Type != wantType || column.Definition.Encoding != wantEncoding {
		return fmt.Errorf("%s column type=%s encoding=%s want type=%s encoding=%s", column.Field.ValueType, column.Definition.Type, column.Definition.Encoding, wantType, wantEncoding)
	}
	if dst.ElementsPerRow != column.Definition.FixedWidthElements || dst.BitsPerElement != wantBits {
		return fmt.Errorf("%s metadata elements_per_row=%d bits_per_element=%d want elements_per_row=%d bits_per_element=%d", column.Field.ValueType, dst.ElementsPerRow, dst.BitsPerElement, column.Definition.FixedWidthElements, wantBits)
	}
	rowBytes, err := typedcolumn.PackedUintRowBytes(dst.ElementsPerRow, dst.BitsPerElement)
	if err != nil {
		return err
	}
	if dst.BytesPerRow != rowBytes {
		return fmt.Errorf("%s bytes_per_row=%d want %d", column.Field.ValueType, dst.BytesPerRow, rowBytes)
	}
	if len(value.Bytes) != rowBytes {
		return fmt.Errorf("%s bytes=%d want row_bytes=%d", column.Field.ValueType, len(value.Bytes), rowBytes)
	}
	if err := typedcolumn.ValidatePackedUintRowPadding(value.Bytes, dst.ElementsPerRow, dst.BitsPerElement); err != nil {
		return err
	}
	start, err := typedColumnAdapterDenseBytes(rowIdx, rowBytes, 1)
	if err != nil {
		return err
	}
	if start > len(dst.Values)-rowBytes {
		return fmt.Errorf("%s row %d outside destination", column.Field.ValueType, rowIdx)
	}
	copy(dst.Values[start:start+rowBytes], value.Bytes)
	return nil
}

func encodeTypedColumnAdapterAdjacencyListValue(dst []uint32, rowIdx int, column typedColumnAdapterColumn, value columnDeclaredValue) error {
	if err := validateTypedColumnAdapterDeclaredValue(column, value); err != nil {
		return err
	}
	degree := column.Definition.FixedWidthElements
	if degree <= 0 {
		return fmt.Errorf("adjacency_list fixed-width elements=%d", degree)
	}
	if len(value.AdjacencyList) != degree {
		return fmt.Errorf("adjacency_list length=%d want adjacency_degree=%d", len(value.AdjacencyList), degree)
	}
	start, err := typedColumnAdapterDenseElements(rowIdx, degree)
	if err != nil {
		return err
	}
	if start > len(dst)-degree {
		return fmt.Errorf("adjacency_list row %d outside destination", rowIdx)
	}
	copy(dst[start:start+degree], value.AdjacencyList)
	return nil
}

func encodeTypedColumnAdapterUint32ListValue(list typedcolumn.Uint32List, rowIdx int, column typedColumnAdapterColumn, value columnDeclaredValue) (typedcolumn.Uint32List, error) {
	if err := validateTypedColumnAdapterDeclaredValue(column, value); err != nil {
		return typedcolumn.Uint32List{}, err
	}
	if column.Definition.Type != typedcolumn.ColumnTypeUint32List || column.Definition.Encoding != typedcolumn.EncodingRawUint32OffsetsList || column.Definition.FixedWidthElements != 0 {
		return typedcolumn.Uint32List{}, fmt.Errorf("uint32_list column type=%s encoding=%s fixed_width_elements=%d", column.Definition.Type, column.Definition.Encoding, column.Definition.FixedWidthElements)
	}
	return appendTypedColumnAdapterUint32ListRow(list, rowIdx, value.Uint32List, "uint32_list")
}

func encodeTypedColumnAdapterAdjacencyOffsetsListValue(list typedcolumn.RawUint32OffsetsList, rowIdx int, column typedColumnAdapterColumn, value columnDeclaredValue) (typedcolumn.RawUint32OffsetsList, error) {
	if err := validateTypedColumnAdapterDeclaredValue(column, value); err != nil {
		return typedcolumn.RawUint32OffsetsList{}, err
	}
	if column.Definition.Encoding != typedcolumn.EncodingRawUint32OffsetsList || column.Definition.FixedWidthElements != 0 {
		return typedcolumn.RawUint32OffsetsList{}, fmt.Errorf("adjacency_list offsets-list column encoding=%s fixed_width_elements=%d", column.Definition.Encoding, column.Definition.FixedWidthElements)
	}
	return appendTypedColumnAdapterUint32ListRow(list, rowIdx, value.AdjacencyList, "adjacency_list offsets-list")
}

func encodeTypedColumnAdapterBytesValue(bytesColumn typedcolumn.RawBytesOffsets, rowIdx int, column typedColumnAdapterColumn, value columnDeclaredValue) (typedcolumn.RawBytesOffsets, error) {
	if err := validateTypedColumnAdapterDeclaredValue(column, value); err != nil {
		return typedcolumn.RawBytesOffsets{}, err
	}
	if column.Definition.Type != typedcolumn.ColumnTypeBytes || column.Definition.Encoding != typedcolumn.EncodingRawBytesOffsets || column.Definition.FixedWidthElements != 0 {
		return typedcolumn.RawBytesOffsets{}, fmt.Errorf("bytes column type=%s encoding=%s fixed_width_elements=%d", column.Definition.Type, column.Definition.Encoding, column.Definition.FixedWidthElements)
	}
	return appendTypedColumnAdapterBytesRow(bytesColumn, rowIdx, value.Bytes)
}

func appendTypedColumnAdapterUint32ListRow(list typedcolumn.Uint32List, rowIdx int, values []uint32, label string) (typedcolumn.Uint32List, error) {
	if rowIdx < 0 || rowIdx >= list.Rows || len(list.Offsets) != list.Rows+1 {
		return typedcolumn.Uint32List{}, fmt.Errorf("%s row %d outside rows=%d offsets=%d", label, rowIdx, list.Rows, len(list.Offsets))
	}
	if uint64(len(list.Values)) > uint64(int(^uint(0)>>1))-uint64(len(values)) {
		return typedcolumn.Uint32List{}, fmt.Errorf("%s values overflow", label)
	}
	list.Values = append(list.Values, values...)
	list.Offsets[rowIdx+1] = uint64(len(list.Values))
	return list, nil
}

func appendTypedColumnAdapterBytesRow(bytesColumn typedcolumn.BytesColumn, rowIdx int, value []byte) (typedcolumn.BytesColumn, error) {
	if rowIdx < 0 || rowIdx >= bytesColumn.Rows || len(bytesColumn.Offsets) != bytesColumn.Rows+1 {
		return typedcolumn.BytesColumn{}, fmt.Errorf("bytes row %d outside rows=%d offsets=%d", rowIdx, bytesColumn.Rows, len(bytesColumn.Offsets))
	}
	if uint64(len(bytesColumn.Values)) > uint64(int(^uint(0)>>1))-uint64(len(value)) {
		return typedcolumn.BytesColumn{}, fmt.Errorf("bytes values overflow")
	}
	bytesColumn.Values = append(bytesColumn.Values, value...)
	bytesColumn.Offsets[rowIdx+1] = uint64(len(bytesColumn.Values))
	return bytesColumn, nil
}

func typedColumnAdapterDenseElements(rows int, elementsPerRow int) (int, error) {
	if rows < 0 || elementsPerRow < 0 {
		return 0, fmt.Errorf("dense elements negative operands rows=%d elements_per_row=%d", rows, elementsPerRow)
	}
	if elementsPerRow != 0 && rows > int(^uint(0)>>1)/elementsPerRow {
		return 0, fmt.Errorf("dense elements overflow rows=%d elements_per_row=%d", rows, elementsPerRow)
	}
	return rows * elementsPerRow, nil
}

func typedColumnAdapterDenseBytes(rows int, elementsPerRow int, elementWidth int) (int, error) {
	if elementsPerRow <= 0 {
		return 0, fmt.Errorf("dense elements_per_row=%d", elementsPerRow)
	}
	elements, err := typedColumnAdapterDenseElements(rows, elementsPerRow)
	if err != nil {
		return 0, err
	}
	if elementWidth <= 0 {
		return 0, fmt.Errorf("dense element width=%d", elementWidth)
	}
	if elements > int(^uint(0)>>1)/elementWidth {
		return 0, fmt.Errorf("dense bytes overflow rows=%d elements_per_row=%d width=%d", rows, elementsPerRow, elementWidth)
	}
	return elements * elementWidth, nil
}

func validateTypedColumnAdapterDeclaredValue(column typedColumnAdapterColumn, value columnDeclaredValue) error {
	if value.Type == "" {
		return errors.New("declared type required")
	}
	if value.Type != column.Field.ValueType {
		return fmt.Errorf("value type=%q want %q", value.Type, column.Field.ValueType)
	}
	if !value.Present || value.Null {
		if column.Field.Nullable {
			if !value.Present && !value.Null {
				return fmt.Errorf("absent nullable value is not marked null")
			}
			return nil
		}
		return fmt.Errorf("null or missing values are not represented by the typed-column adapter")
	}
	return nil
}

func decodeTypedColumnAdapterValue(column typedColumnAdapterColumn, raw int64) (columnDeclaredValue, error) {
	value := columnDeclaredValue{Type: column.Field.ValueType, Present: true}
	switch column.Field.ValueType {
	case ColumnStoreValueBool:
		if raw != 0 && raw != 1 {
			return columnDeclaredValue{}, fmt.Errorf("bool encoded value %d outside 0/1", raw)
		}
		value.Bool = raw != 0
	case ColumnStoreValueInt64:
		value.Int64 = raw
	case ColumnStoreValueFloat32:
		if raw < 0 || raw > int64(^uint32(0)) {
			return columnDeclaredValue{}, fmt.Errorf("float32 encoded bits %d outside uint32", raw)
		}
		value.Float32 = math.Float32frombits(uint32(raw))
	case ColumnStoreValueDouble:
		value.Double = math.Float64frombits(uint64(raw))
	case ColumnStoreValueString:
		text, ok := column.ReverseDictionary[raw]
		if !ok {
			return columnDeclaredValue{}, fmt.Errorf("missing dictionary value for code %d", raw)
		}
		value.String = text
	default:
		return columnDeclaredValue{}, fmt.Errorf("%w: %s", errTypedColumnAdapterUnsupportedType, column.Field.ValueType)
	}
	return value, nil
}

func typedColumnAdapterRowValue(row typedColumnAdapterRow, column typedColumnAdapterColumn) (columnDeclaredValue, bool, error) {
	pathKey := column.Field.Path
	if pathKey == "" {
		pathKey = column.Field.Name
	}
	pathValue, pathOK := row.Values[pathKey]
	if column.Field.Name == "" || column.Field.Name == pathKey {
		return pathValue, pathOK, nil
	}
	_, nameOK := row.Values[column.Field.Name]
	if pathOK && nameOK {
		return columnDeclaredValue{}, false, fmt.Errorf("ambiguous field keys %q and %q", pathKey, column.Field.Name)
	}
	return pathValue, pathOK, nil
}

func newTypedColumnAdapterStringDictionaryBuildState() *typedColumnAdapterStringDictionaryBuildState {
	return &typedColumnAdapterStringDictionaryBuildState{codeByValue: make(map[string]int64)}
}

func (s *typedColumnAdapterStringDictionaryBuildState) temporaryCode(value string) int64 {
	if code, ok := s.codeByValue[value]; ok {
		return code
	}
	code := int64(len(s.valuesByTemporaryCode))
	s.codeByValue[value] = code
	s.valuesByTemporaryCode = append(s.valuesByTemporaryCode, value)
	return code
}

func (s *typedColumnAdapterStringDictionaryBuildState) sortedDictionaryAndRecode() (map[string]int64, []int64) {
	values := append([]string(nil), s.valuesByTemporaryCode...)
	sort.Strings(values)
	dict := make(map[string]int64, len(values))
	temporaryToFinal := make([]int64, len(values))
	for finalCode, value := range values {
		final := int64(finalCode)
		dict[value] = final
		temporaryToFinal[s.codeByValue[value]] = final
	}
	return dict, temporaryToFinal
}

func finalizeTypedColumnAdapterStringDictionaries(opts typedColumnAdapterOptions, columns []typedColumnAdapterColumn, states []*typedColumnAdapterStringDictionaryBuildState, batch typedcolumn.Batch) error {
	for i := range columns {
		if i >= len(states) || states[i] == nil {
			continue
		}
		dict, temporaryToFinal := states[i].sortedDictionaryAndRecode()
		if err := recodeTypedColumnAdapterStringBatchColumn(columns[i], batch, temporaryToFinal); err != nil {
			return err
		}
		columns[i].Dictionary = dict
		mode := typedColumnAdapterBuildDictionaryModeForColumn(opts, columns[i].Definition.Name)
		if mode.Reverse {
			columns[i].ReverseDictionary = reverseTypedColumnAdapterDictionary(dict)
		}
		if mode.ValuesByCode {
			valuesByCode, err := typedColumnAdapterDictionaryValuesByCodeFromForward(dict, len(dict))
			if err != nil {
				return err
			}
			columns[i].DictionaryValuesByCode = valuesByCode
		}
		columns[i].Definition.Cardinality = uint32(len(dict))
	}
	return nil
}

func recodeTypedColumnAdapterStringBatchColumn(column typedColumnAdapterColumn, batch typedcolumn.Batch, temporaryToFinal []int64) error {
	values, ok := batch.Columns[column.Definition.Name]
	if !ok {
		return fmt.Errorf("collections: typed-column adapter missing string batch column %q", column.Definition.Name)
	}
	nulls := batch.Nulls[column.Definition.Name]
	defaults := batch.Defaults[column.Definition.Name]
	if column.Field.Nullable {
		var err error
		nulls, err = typedColumnAdapterRequiredBitmapForRows(batch.Nulls, column.Definition.Name, "null", len(values))
		if err != nil {
			return err
		}
		defaults, err = typedColumnAdapterRequiredBitmapForRows(batch.Defaults, column.Definition.Name, "default", len(values))
		if err != nil {
			return err
		}
	} else if len(nulls) != 0 || len(defaults) != 0 {
		return fmt.Errorf("collections: typed-column adapter non-nullable column %q has null/default bitmap lengths %d/%d", column.Definition.Name, len(nulls), len(defaults))
	}
	for rowIdx, temporary := range values {
		if column.Field.Nullable && (nulls[rowIdx] || defaults[rowIdx]) {
			continue
		}
		if temporary < 0 || int64(len(temporaryToFinal)) <= temporary {
			return fmt.Errorf("collections: typed-column adapter row %d column %q temporary dictionary code %d outside cardinality %d", rowIdx, column.Definition.Name, temporary, len(temporaryToFinal))
		}
		values[rowIdx] = temporaryToFinal[temporary]
	}
	return nil
}

func typedColumnAdapterRequiredBitmapForRows(bitmaps map[string][]bool, columnName, bitmapName string, rows int) ([]bool, error) {
	values, ok := bitmaps[columnName]
	if !ok {
		return nil, fmt.Errorf("collections: typed-column adapter missing %s bitmap for nullable column %q", bitmapName, columnName)
	}
	if len(values) != rows {
		return nil, fmt.Errorf("collections: typed-column adapter %s bitmap for column %q has %d rows, want %d", bitmapName, columnName, len(values), rows)
	}
	return values, nil
}

func typedColumnAdapterIndexedSourceColumns(rowSource typedColumnAdapterRowSource, columns []typedColumnAdapterColumn) (typedColumnAdapterIndexedRowSource, []int, error) {
	indexed, ok := rowSource.(typedColumnAdapterIndexedRowSource)
	if !ok {
		return nil, nil, nil
	}
	indexes := make([]int, len(columns))
	for i, column := range columns {
		valueIdx, err := indexed.ValueIndex(column)
		if err != nil {
			return nil, nil, err
		}
		indexes[i] = valueIdx
	}
	return indexed, indexes, nil
}

func reverseTypedColumnAdapterDictionary(dict map[string]int64) map[int64]string {
	reverse := make(map[int64]string, len(dict))
	for value, code := range dict {
		reverse[code] = value
	}
	return reverse
}

func typedColumnAdapterDictionaryModeForColumn(opts typedColumnAdapterOptions, column string) typedColumnAdapterDictionaryMode {
	if opts.DictionaryModes != nil {
		return opts.DictionaryModes[column]
	}
	return typedColumnAdapterDictionaryMode{Forward: true, Reverse: true, ValuesByCode: true}
}

func typedColumnAdapterBuildDictionaryModeForColumn(opts typedColumnAdapterOptions, column string) typedColumnAdapterDictionaryMode {
	mode := typedColumnAdapterDictionaryMode{Forward: true, Reverse: true, ValuesByCode: true}
	if opts.DictionaryModes != nil {
		if requested, ok := opts.DictionaryModes[column]; ok {
			mode = requested
		} else {
			mode = typedColumnAdapterDictionaryMode{}
		}
	}
	mode.Forward = true
	return mode
}

func typedColumnPreparedAdapterDictionaryModeForColumn(opts typedColumnPreparedAdapterPartOptions, column string) typedColumnAdapterDictionaryMode {
	if opts.DictionaryModes != nil {
		return opts.DictionaryModes[column]
	}
	return typedColumnAdapterDictionaryMode{Forward: true, Reverse: true, ValuesByCode: true}
}

func typedColumnAdapterDictionaryValuesByCodeFromForward(dict map[string]int64, cardinality int) ([]string, error) {
	if cardinality == 0 && len(dict) == 0 {
		return nil, nil
	}
	if cardinality <= 0 {
		return nil, fmt.Errorf("collections: typed-column adapter dictionary cardinality=%d is invalid", cardinality)
	}
	valuesByCode := make([]string, cardinality)
	seen := make([]bool, cardinality)
	for value, code := range dict {
		if code < 0 || int64(cardinality) <= code {
			return nil, fmt.Errorf("collections: typed-column adapter dictionary code %d outside cardinality %d", code, cardinality)
		}
		codeIdx := int(code)
		if seen[codeIdx] {
			return nil, fmt.Errorf("collections: typed-column adapter dictionary duplicate code %d", code)
		}
		seen[codeIdx] = true
		valuesByCode[codeIdx] = value
	}
	for code, ok := range seen {
		if !ok {
			return nil, fmt.Errorf("collections: typed-column adapter dictionary missing code %d", code)
		}
	}
	return valuesByCode, nil
}

func typedColumnAdapterDictionaryValueByCode(column typedColumnAdapterColumn, code int) (string, bool) {
	if code >= 0 && code < len(column.DictionaryValuesByCode) {
		return column.DictionaryValuesByCode[code], true
	}
	if column.ReverseDictionary != nil {
		value, ok := column.ReverseDictionary[int64(code)]
		return value, ok
	}
	return "", false
}

func validateTypedColumnAdapterStringDictionary(column typedColumnAdapterColumn, cardinality uint32, dict map[string]int64) error {
	if cardinality == 0 {
		if column.Field.Nullable && len(dict) == 0 {
			return nil
		}
		return fmt.Errorf("collections: typed-column adapter image dictionary for %q has zero cardinality", column.Definition.Name)
	}
	if uint64(len(dict)) != uint64(cardinality) {
		return fmt.Errorf("collections: typed-column adapter image dictionary cardinality mismatch for %q: got %d want %d", column.Definition.Name, len(dict), cardinality)
	}
	seen := make(map[int64]string, len(dict))
	for value, code := range dict {
		if code < 0 || uint64(code) >= uint64(cardinality) {
			return fmt.Errorf("collections: typed-column adapter image dictionary code %d for %q outside cardinality %d", code, column.Definition.Name, cardinality)
		}
		if previous, ok := seen[code]; ok {
			return fmt.Errorf("collections: typed-column adapter image dictionary duplicate code %d for %q values %q and %q", code, column.Definition.Name, previous, value)
		}
		seen[code] = value
	}
	var previous string
	for code := int64(0); code < int64(cardinality); code++ {
		value, ok := seen[code]
		if !ok {
			return fmt.Errorf("collections: typed-column adapter image dictionary missing code %d for %q", code, column.Definition.Name)
		}
		if code > 0 && previous > value {
			return fmt.Errorf("collections: typed-column adapter image dictionary for %q is not logical bytewise ascending at code %d (%q > %q)", column.Definition.Name, code, previous, value)
		}
		previous = value
	}
	return nil
}

func typedColumnAdapterMetadataKey(column typedColumnAdapterColumn) string {
	return typedColumnAdapterMetadataEntryKey(column, typedColumnAdapterMetadataValueTypeMark, string(column.Field.ValueType))
}

func typedColumnAdapterMetadataEntryKey(column typedColumnAdapterColumn, mark, value string) string {
	return column.Definition.Name + "\x00" + mark + "\x00" + value
}

func validateTypedColumnAdapterMetadata(dictionaries map[string]map[string]int64, columns []typedColumnAdapterColumn) error {
	if len(columns) == 0 {
		return nil
	}
	metadata := dictionaries[typedColumnAdapterMetadataDictionary]
	if len(metadata) == 0 {
		return fmt.Errorf("collections: typed-column adapter image missing metadata dictionary %q", typedColumnAdapterMetadataDictionary)
	}
	for _, column := range columns {
		if _, ok := metadata[typedColumnAdapterMetadataKey(column)]; !ok {
			return fmt.Errorf("collections: typed-column adapter image value type metadata mismatch for column %q", column.Definition.Name)
		}
		if column.Field.ValueType == ColumnStoreValueString {
			if !typedColumnAdapterMetadataEntryExists(metadata, column, typedColumnAdapterMetadataDictionaryIdentityMark, typedColumnAdapterStringDictionaryIdentity) {
				return fmt.Errorf("collections: typed-column adapter image dictionary identity metadata mismatch for column %q", column.Definition.Name)
			}
			if !typedColumnAdapterMetadataEntryExists(metadata, column, typedColumnAdapterMetadataDictionaryOrderMark, typedColumnAdapterStringDictionaryOrder, typedColumnAdapterStringDictionaryLegacyOrder) {
				return fmt.Errorf("collections: typed-column adapter image dictionary order metadata mismatch for column %q", column.Definition.Name)
			}
			if !typedColumnAdapterMetadataEntryExists(metadata, column, typedColumnAdapterMetadataDictionaryCollationMark, typedColumnAdapterStringDictionaryCollation, typedColumnAdapterStringDictionaryLegacyCollation) {
				return fmt.Errorf("collections: typed-column adapter image dictionary collation metadata mismatch for column %q", column.Definition.Name)
			}
		}
	}
	return nil
}

func typedColumnAdapterMetadataEntryExists(metadata map[string]int64, column typedColumnAdapterColumn, mark string, values ...string) bool {
	for _, value := range values {
		if _, ok := metadata[typedColumnAdapterMetadataEntryKey(column, mark, value)]; ok {
			return true
		}
	}
	return false
}

func typedColumnAdapterDictionaries(columns []typedColumnAdapterColumn) map[string]map[string]int64 {
	out := make(map[string]map[string]int64)
	metadata := make(map[string]int64, len(columns))
	metadataCode := int64(1)
	for _, column := range columns {
		metadata[typedColumnAdapterMetadataKey(column)] = metadataCode
		metadataCode++
		if column.Field.ValueType == ColumnStoreValueString {
			metadata[typedColumnAdapterMetadataEntryKey(column, typedColumnAdapterMetadataDictionaryIdentityMark, typedColumnAdapterStringDictionaryIdentity)] = metadataCode
			metadataCode++
			metadata[typedColumnAdapterMetadataEntryKey(column, typedColumnAdapterMetadataDictionaryOrderMark, typedColumnAdapterStringDictionaryOrder)] = metadataCode
			metadataCode++
			metadata[typedColumnAdapterMetadataEntryKey(column, typedColumnAdapterMetadataDictionaryCollationMark, typedColumnAdapterStringDictionaryCollation)] = metadataCode
			metadataCode++
		}
		if len(column.Dictionary) != 0 {
			out[column.Definition.Name] = column.Dictionary
		}
	}
	if len(metadata) != 0 {
		out[typedColumnAdapterMetadataDictionary] = metadata
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func typedColumnAdapterRetainedPayloadSplitRestore(cfg ColumnStoreConfig, document []byte, values []columnDeclaredValue) ([]byte, []byte, error) {
	retained, err := columnRetainedPayloadFromJSONDocument(cfg, document)
	if err != nil {
		return nil, nil, err
	}
	restored, err := reconstructColumnJSONDocument(cfg, retained, values)
	if err != nil {
		return nil, nil, err
	}
	return retained, restored, nil
}

func (r typedColumnAdapterResourceReader) ReadSection(section typedcolumn.ColumnPartImageSection) ([]byte, error) {
	h, err := r.AcquireSection(section)
	if err != nil {
		return nil, err
	}
	defer h.Release()
	return bytes.Clone(h.Bytes()), nil
}

type typedColumnAdapterFloat32ScalarResourceView struct {
	Rows   int
	Values []float32
	Handle *mappedresource.Handle
	Direct bool
}

type typedColumnAdapterFloat64ScalarResourceView struct {
	Rows   int
	Values []float64
	Handle *mappedresource.Handle
	Direct bool
}

type typedColumnAdapterDenseUint32ResourceView struct {
	Rows           int
	ElementsPerRow int
	Values         []uint32
	Handle         *mappedresource.Handle
	Direct         bool
}

type typedColumnAdapterUint32OffsetsListViewClass string

const (
	typedColumnAdapterUint32OffsetsListViewMmapDirect              typedColumnAdapterUint32OffsetsListViewClass = "mmap_direct"
	typedColumnAdapterUint32OffsetsListViewHeapCopyTyped           typedColumnAdapterUint32OffsetsListViewClass = "heap_copy_typed_view"
	typedColumnAdapterUint32OffsetsListViewScratchDecode           typedColumnAdapterUint32OffsetsListViewClass = "scratch_decode"
	typedColumnAdapterUint32OffsetsListViewSourceUnsupported       typedColumnAdapterUint32OffsetsListViewClass = "source_unsupported"
	typedColumnAdapterUint32OffsetsListViewStaleHandle             typedColumnAdapterUint32OffsetsListViewClass = "stale_handle"
	typedColumnAdapterUint32OffsetsListViewActualPointerUnaligned  typedColumnAdapterUint32OffsetsListViewClass = "actual_pointer_unaligned"
	typedColumnAdapterUint32OffsetsListViewAbsoluteOffsetUnaligned typedColumnAdapterUint32OffsetsListViewClass = "absolute_offset_unaligned"
	typedColumnAdapterUint32OffsetsListViewCertificationFailure    typedColumnAdapterUint32OffsetsListViewClass = "certification_failure"
	typedColumnAdapterUint32OffsetsListViewValidationFailure       typedColumnAdapterUint32OffsetsListViewClass = "validation_failure"
)

type typedColumnAdapterUint32OffsetsListClassification struct {
	Class    typedColumnAdapterUint32OffsetsListViewClass
	Counter  typeddecode.Counter
	Counters []typeddecode.Counter
	Status   typeddecode.Status
}

type typedColumnAdapterUint32OffsetsListResourceView struct {
	Rows          int
	Offsets       []uint64
	Values        []uint32
	OffsetsHandle *mappedresource.Handle
	ValuesHandle  *mappedresource.Handle
	Direct        bool
	HeapCopy      bool
	Scratch       bool
	Class         typedColumnAdapterUint32OffsetsListClassification
}

func (v *typedColumnAdapterUint32OffsetsListResourceView) Close() error {
	if v == nil {
		return nil
	}
	offsets := v.OffsetsHandle
	values := v.ValuesHandle
	v.OffsetsHandle = nil
	v.ValuesHandle = nil
	v.Offsets = nil
	v.Values = nil
	return errors.Join(releaseMappedResourceHandle(offsets), releaseMappedResourceHandle(values))
}

func releaseMappedResourceHandle(h *mappedresource.Handle) error {
	if h == nil {
		return nil
	}
	return h.Release()
}

func typedColumnAdapterAcquireFloat32ScalarColumnView(reader typedColumnAdapterResourceReader, column typedColumnAdapterColumn, rows int) (typedColumnAdapterFloat32ScalarResourceView, error) {
	if column.Field.ValueType != ColumnStoreValueFloat32 || column.Definition.Type != typedcolumn.ColumnTypeFloat32 || column.Definition.Encoding != typedcolumn.EncodingRawFloat32 {
		return typedColumnAdapterFloat32ScalarResourceView{}, fmt.Errorf("collections: typed-column adapter column %q is not native raw float32", column.Definition.Name)
	}
	if rows == 0 {
		rows = reader.Image.Rows
	}
	section, found := typedColumnAdapterColumnDataSection(reader.Image, column.Definition.Name)
	if !found {
		return typedColumnAdapterFloat32ScalarResourceView{}, fmt.Errorf("collections: typed-column adapter image missing column data section %q", column.Definition.Name)
	}
	if section.Encoding != typedcolumn.EncodingRawFloat32 || section.Compression != typedcolumn.CompressionNone {
		return typedColumnAdapterFloat32ScalarResourceView{}, fmt.Errorf("collections: typed-column adapter column %q section encoding/compression mismatch", column.Definition.Name)
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(reader.Image)
	if err != nil {
		return typedColumnAdapterFloat32ScalarResourceView{}, fmt.Errorf("collections: typed-column adapter column %q layout certification: %w", column.Definition.Name, err)
	}
	certColumn, ok := certification.Column(column.Definition.Name)
	if !ok {
		return typedColumnAdapterFloat32ScalarResourceView{}, fmt.Errorf("collections: typed-column adapter image missing layout certification for column %q", column.Definition.Name)
	}
	plan := typeddecode.Float32ScalarPlan(certColumn)
	directReq := typeddecode.DirectViewColumnRequest{Plan: plan, Certification: certColumn, Rows: rows, PayloadBytes: section.Length, AssetOffset: 0, HasAssetOffset: true}
	certStatus := typeddecode.ValidateDirectViewColumn(directReq)
	h, err := reader.AcquireSection(section)
	if err != nil {
		return typedColumnAdapterFloat32ScalarResourceView{}, err
	}
	viewStatus := certStatus
	if certStatus.Direct() {
		values, status := typeddecode.Float32ScalarView(reader.Manager, h, directReq, typeddecode.ResourceViewOptions{ExpectedElements: rows, RequireMapped: true})
		viewStatus = status
		if viewStatus.Direct() {
			return typedColumnAdapterFloat32ScalarResourceView{Rows: rows, Values: values, Handle: h, Direct: true}, nil
		}
	}
	viewErr := fmt.Errorf("collections: typed-column adapter column %q float32 direct-view validation: %s", column.Definition.Name, viewStatus.String())
	if !typedColumnDenseDecodeFallbackAllowed(viewStatus) {
		_ = h.Release()
		return typedColumnAdapterFloat32ScalarResourceView{}, viewErr
	}
	decoded, decodeErr := typedcolumn.DecodeRawFloat32Payload(nil, h.Bytes(), rows)
	releaseErr := h.Release()
	if decodeErr != nil {
		return typedColumnAdapterFloat32ScalarResourceView{}, errors.Join(viewErr, decodeErr, releaseErr)
	}
	if releaseErr != nil {
		return typedColumnAdapterFloat32ScalarResourceView{}, errors.Join(viewErr, releaseErr)
	}
	return typedColumnAdapterFloat32ScalarResourceView{Rows: rows, Values: decoded, Direct: false}, nil
}

func typedColumnAdapterAcquireFloat64ScalarColumnView(reader typedColumnAdapterResourceReader, column typedColumnAdapterColumn, rows int) (typedColumnAdapterFloat64ScalarResourceView, error) {
	if column.Field.ValueType != ColumnStoreValueDouble || column.Definition.Type != typedcolumn.ColumnTypeFloat64 || column.Definition.Encoding != typedcolumn.EncodingRawFloat64 {
		return typedColumnAdapterFloat64ScalarResourceView{}, fmt.Errorf("collections: typed-column adapter column %q is not native raw float64", column.Definition.Name)
	}
	if rows == 0 {
		rows = reader.Image.Rows
	}
	section, found := typedColumnAdapterColumnDataSection(reader.Image, column.Definition.Name)
	if !found {
		return typedColumnAdapterFloat64ScalarResourceView{}, fmt.Errorf("collections: typed-column adapter image missing column data section %q", column.Definition.Name)
	}
	if section.Encoding != typedcolumn.EncodingRawFloat64 || section.Compression != typedcolumn.CompressionNone {
		return typedColumnAdapterFloat64ScalarResourceView{}, fmt.Errorf("collections: typed-column adapter column %q section encoding/compression mismatch", column.Definition.Name)
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(reader.Image)
	if err != nil {
		return typedColumnAdapterFloat64ScalarResourceView{}, fmt.Errorf("collections: typed-column adapter column %q layout certification: %w", column.Definition.Name, err)
	}
	certColumn, ok := certification.Column(column.Definition.Name)
	if !ok {
		return typedColumnAdapterFloat64ScalarResourceView{}, fmt.Errorf("collections: typed-column adapter image missing layout certification for column %q", column.Definition.Name)
	}
	plan := typeddecode.Float64ScalarPlan(certColumn)
	directReq := typeddecode.DirectViewColumnRequest{Plan: plan, Certification: certColumn, Rows: rows, PayloadBytes: section.Length, AssetOffset: 0, HasAssetOffset: true}
	certStatus := typeddecode.ValidateDirectViewColumn(directReq)
	h, err := reader.AcquireSection(section)
	if err != nil {
		return typedColumnAdapterFloat64ScalarResourceView{}, err
	}
	viewStatus := certStatus
	if certStatus.Direct() {
		values, status := typeddecode.Float64ScalarView(reader.Manager, h, directReq, typeddecode.ResourceViewOptions{ExpectedElements: rows, RequireMapped: true})
		viewStatus = status
		if viewStatus.Direct() {
			return typedColumnAdapterFloat64ScalarResourceView{Rows: rows, Values: values, Handle: h, Direct: true}, nil
		}
	}
	viewErr := fmt.Errorf("collections: typed-column adapter column %q float64 direct-view validation: %s", column.Definition.Name, viewStatus.String())
	if !typedColumnDenseDecodeFallbackAllowed(viewStatus) {
		_ = h.Release()
		return typedColumnAdapterFloat64ScalarResourceView{}, viewErr
	}
	decoded, decodeErr := typedcolumn.DecodeRawFloat64Payload(nil, h.Bytes(), rows)
	releaseErr := h.Release()
	if decodeErr != nil {
		return typedColumnAdapterFloat64ScalarResourceView{}, errors.Join(viewErr, decodeErr, releaseErr)
	}
	if releaseErr != nil {
		return typedColumnAdapterFloat64ScalarResourceView{}, errors.Join(viewErr, releaseErr)
	}
	return typedColumnAdapterFloat64ScalarResourceView{Rows: rows, Values: decoded, Direct: false}, nil
}

func typedColumnAdapterAcquireDenseUint32ColumnView(reader typedColumnAdapterResourceReader, column typedColumnAdapterColumn, rows int) (typedColumnAdapterDenseUint32ResourceView, error) {
	if column.Field.ValueType != ColumnStoreValueAdjacencyList || column.Definition.Type != typedcolumn.ColumnTypeAdjacencyList || column.Definition.Encoding != typedcolumn.EncodingRawUint32Dense {
		return typedColumnAdapterDenseUint32ResourceView{}, fmt.Errorf("collections: typed-column adapter column %q is not dense adjacency_list", column.Definition.Name)
	}
	degree := column.Definition.FixedWidthElements
	if degree <= 0 {
		return typedColumnAdapterDenseUint32ResourceView{}, fmt.Errorf("collections: typed-column adapter column %q adjacency_degree=%d", column.Definition.Name, degree)
	}
	if rows == 0 {
		rows = reader.Image.Rows
	}
	expected, err := typedColumnAdapterDenseElements(rows, degree)
	if err != nil {
		return typedColumnAdapterDenseUint32ResourceView{}, err
	}
	var section typedcolumn.ColumnPartImageSection
	found := false
	for _, candidate := range reader.Image.Sections {
		if candidate.Kind == typedcolumn.ColumnPartImageSectionColumnData && candidate.Column == column.Definition.Name {
			section = candidate
			found = true
			break
		}
	}
	if !found {
		return typedColumnAdapterDenseUint32ResourceView{}, fmt.Errorf("collections: typed-column adapter image missing column data section %q", column.Definition.Name)
	}
	if section.Encoding != typedcolumn.EncodingRawUint32Dense || section.Compression != typedcolumn.CompressionNone {
		return typedColumnAdapterDenseUint32ResourceView{}, fmt.Errorf("collections: typed-column adapter column %q section encoding/compression mismatch", column.Definition.Name)
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(reader.Image)
	if err != nil {
		return typedColumnAdapterDenseUint32ResourceView{}, fmt.Errorf("collections: typed-column adapter column %q layout certification: %w", column.Definition.Name, err)
	}
	certColumn, ok := certification.Column(column.Definition.Name)
	if !ok {
		return typedColumnAdapterDenseUint32ResourceView{}, fmt.Errorf("collections: typed-column adapter image missing layout certification for column %q", column.Definition.Name)
	}
	plan := typeddecode.AdjacencyListPlan(certColumn, degree)
	directReq := typeddecode.DirectViewColumnRequest{Plan: plan, Certification: certColumn, Rows: rows, PayloadBytes: section.Length, AssetOffset: 0, HasAssetOffset: true}
	certStatus := typeddecode.ValidateDirectViewColumn(directReq)
	h, err := reader.AcquireSection(section)
	if err != nil {
		return typedColumnAdapterDenseUint32ResourceView{}, err
	}
	viewStatus := certStatus
	if certStatus.Direct() {
		values, status := typeddecode.AdjacencyListView(reader.Manager, h, directReq, typeddecode.ResourceViewOptions{ExpectedElements: expected, RequireMapped: true})
		viewStatus = status
		if viewStatus.Direct() {
			return typedColumnAdapterDenseUint32ResourceView{Rows: rows, ElementsPerRow: degree, Values: values, Handle: h, Direct: true}, nil
		}
	}
	viewErr := fmt.Errorf("collections: typed-column adapter column %q adjacency direct-view validation: %s", column.Definition.Name, viewStatus.String())
	if !typedColumnDenseDecodeFallbackAllowed(viewStatus) {
		_ = h.Release()
		return typedColumnAdapterDenseUint32ResourceView{}, viewErr
	}
	decoded, decodeErr := typedcolumn.DecodeRawUint32DensePayload(nil, h.Bytes(), rows, degree)
	releaseErr := h.Release()
	if decodeErr != nil {
		decodeErr = errors.Join(viewErr, decodeErr, releaseErr)
		return typedColumnAdapterDenseUint32ResourceView{}, decodeErr
	}
	if releaseErr != nil {
		return typedColumnAdapterDenseUint32ResourceView{}, errors.Join(viewErr, releaseErr)
	}
	return typedColumnAdapterDenseUint32ResourceView{Rows: rows, ElementsPerRow: degree, Values: decoded, Direct: false}, nil
}

func typedColumnAdapterAcquireUint32OffsetsListColumnView(reader typedColumnAdapterResourceReader, column typedColumnAdapterColumn, rows int) (typedColumnAdapterUint32OffsetsListResourceView, error) {
	generic := column.Field.ValueType == ColumnStoreValueUint32List && column.Definition.Type == typedcolumn.ColumnTypeUint32List
	legacyAdjacency := column.Field.ValueType == ColumnStoreValueAdjacencyList && column.Definition.Type == typedcolumn.ColumnTypeAdjacencyList
	if (!generic && !legacyAdjacency) || column.Definition.Encoding != typedcolumn.EncodingRawUint32OffsetsList {
		return typedColumnAdapterUint32OffsetsListResourceView{}, fmt.Errorf("collections: typed-column adapter column %q is not raw_uint32_offsets_list uint32_list/adjacency_list", column.Definition.Name)
	}
	if rows == 0 {
		rows = reader.Image.Rows
	}
	offsetsSection, valuesSection, found := typedColumnAdapterColumnOffsetsListSections(reader.Image, column.Definition.Name)
	if !found {
		return typedColumnAdapterUint32OffsetsListResourceView{}, fmt.Errorf("collections: typed-column adapter image missing offsets-list sections %q", column.Definition.Name)
	}
	if offsetsSection.Encoding != typedcolumn.EncodingRawUint32OffsetsList || valuesSection.Encoding != typedcolumn.EncodingRawUint32OffsetsList || offsetsSection.Compression != typedcolumn.CompressionNone || valuesSection.Compression != typedcolumn.CompressionNone {
		return typedColumnAdapterUint32OffsetsListResourceView{}, fmt.Errorf("collections: typed-column adapter column %q offsets-list section encoding/compression mismatch", column.Definition.Name)
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(reader.Image)
	if err != nil {
		return typedColumnAdapterUint32OffsetsListResourceView{}, fmt.Errorf("collections: typed-column adapter column %q layout certification: %w", column.Definition.Name, err)
	}
	certColumn, ok := certification.Column(column.Definition.Name)
	if !ok {
		return typedColumnAdapterUint32OffsetsListResourceView{}, fmt.Errorf("collections: typed-column adapter image missing layout certification for column %q", column.Definition.Name)
	}
	offsetsHandle, err := reader.AcquireSection(offsetsSection)
	if err != nil {
		return typedColumnAdapterUint32OffsetsListResourceView{}, err
	}
	valuesHandle, err := reader.AcquireSection(valuesSection)
	if err != nil {
		releaseErr := offsetsHandle.Release()
		return typedColumnAdapterUint32OffsetsListResourceView{}, errors.Join(err, releaseErr)
	}
	view, err := typedColumnAdapterOpenUint32OffsetsListColumnViewFromHandles(reader.Manager, certColumn, rows, offsetsSection.Length, valuesSection.Length, offsetsHandle, valuesHandle)
	if err != nil {
		releaseErr := errors.Join(offsetsHandle.Release(), valuesHandle.Release())
		return typedColumnAdapterUint32OffsetsListResourceView{}, errors.Join(err, releaseErr)
	}
	if view.OffsetsHandle == nil && view.ValuesHandle == nil {
		releaseErr := errors.Join(offsetsHandle.Release(), valuesHandle.Release())
		if releaseErr != nil {
			return typedColumnAdapterUint32OffsetsListResourceView{}, releaseErr
		}
	}
	return view, nil
}

func typedColumnAdapterOpenUint32OffsetsListColumnViewFromHandles(mgr *mappedresource.Manager, certColumn typedcolumn.ColumnPartLayoutContractColumn, rows int, offsetsBytes int, valuesBytes int, offsetsHandle *mappedresource.Handle, valuesHandle *mappedresource.Handle) (typedColumnAdapterUint32OffsetsListResourceView, error) {
	if offsetsHandle == nil || valuesHandle == nil {
		status := typeddecode.StreamingStatus(typeddecode.ReasonNilHandle, "nil offsets-list handle")
		class := typedColumnAdapterClassifyUint32OffsetsListStatus(status)
		return typedColumnAdapterUint32OffsetsListResourceView{Rows: rows, Class: class}, typedColumnAdapterUint32OffsetsListError(certColumn.Name, class)
	}
	if offsetsHandle.Released() || valuesHandle.Released() {
		status := typeddecode.UnsupportedStatus(typeddecode.ReasonStaleHandle, "released offsets-list handle")
		class := typedColumnAdapterClassifyUint32OffsetsListStatus(status)
		return typedColumnAdapterUint32OffsetsListResourceView{Rows: rows, Class: class}, typedColumnAdapterUint32OffsetsListError(certColumn.Name, class)
	}
	plan := typeddecode.AdjacencyOffsetsListPlan(certColumn)
	if certColumn.LogicalType == string(columnsemantics.LogicalUint32List) || certColumn.Type == typedcolumn.ColumnTypeUint32List {
		plan = typeddecode.Uint32ListPlan(certColumn)
	}
	directReq := typeddecode.Uint32OffsetsListDirectViewRequest{Plan: plan, Certification: certColumn, Rows: rows, OffsetsBytes: offsetsBytes, ValuesBytes: valuesBytes, AssetOffset: 0, HasAssetOffset: true}
	status := typeddecode.ValidateUint32OffsetsListDirectViewSections(directReq)
	if !status.Direct() {
		if typedColumnUint32OffsetsListScratchFallbackAllowed(status) {
			return typedColumnAdapterDecodeUint32OffsetsListScratch(certColumn.Name, rows, offsetsHandle, valuesHandle, typedColumnAdapterClassifyUint32OffsetsListStatus(status))
		}
		class := typedColumnAdapterClassifyUint32OffsetsListStatus(status)
		return typedColumnAdapterUint32OffsetsListResourceView{Rows: rows, Class: class}, typedColumnAdapterUint32OffsetsListError(certColumn.Name, class)
	}
	if offsetsHandle.Source() == mappedresource.SourceMapped && valuesHandle.Source() == mappedresource.SourceMapped {
		offsets, values, viewStatus := typeddecode.Uint32OffsetsListView(mgr, offsetsHandle, valuesHandle, directReq, typeddecode.ResourceViewOptions{RequireMapped: true})
		if viewStatus.Direct() {
			class := typedColumnAdapterUint32OffsetsListClassification{Class: typedColumnAdapterUint32OffsetsListViewMmapDirect, Counter: typeddecode.CounterMmapDirectView, Counters: []typeddecode.Counter{typeddecode.CounterOffsetsMmapDirectView, typeddecode.CounterValuesMmapDirectView}, Status: viewStatus}
			return typedColumnAdapterUint32OffsetsListResourceView{Rows: rows, Offsets: offsets, Values: values, OffsetsHandle: offsetsHandle, ValuesHandle: valuesHandle, Direct: true, Class: class}, nil
		}
		if typedColumnUint32OffsetsListScratchFallbackAllowed(viewStatus) {
			return typedColumnAdapterDecodeUint32OffsetsListScratch(certColumn.Name, rows, offsetsHandle, valuesHandle, typedColumnAdapterClassifyUint32OffsetsListStatus(viewStatus))
		}
		class := typedColumnAdapterClassifyUint32OffsetsListStatus(viewStatus)
		return typedColumnAdapterUint32OffsetsListResourceView{Rows: rows, Class: class}, typedColumnAdapterUint32OffsetsListError(certColumn.Name, class)
	}
	if offsetsHandle.Source() == mappedresource.SourceHeapCopy && valuesHandle.Source() == mappedresource.SourceHeapCopy {
		offsets, values, viewStatus := typeddecode.Uint32OffsetsListView(nil, offsetsHandle, valuesHandle, directReq, typeddecode.ResourceViewOptions{RequireMapped: false})
		if viewStatus.Direct() {
			class := typedColumnAdapterUint32OffsetsListClassification{Class: typedColumnAdapterUint32OffsetsListViewHeapCopyTyped, Counter: typeddecode.CounterHeapCopyTypedView, Counters: []typeddecode.Counter{typeddecode.CounterOffsetsHeapCopyTypedView, typeddecode.CounterValuesHeapCopyTypedView}, Status: viewStatus}
			return typedColumnAdapterUint32OffsetsListResourceView{Rows: rows, Offsets: offsets, Values: values, OffsetsHandle: offsetsHandle, ValuesHandle: valuesHandle, HeapCopy: true, Class: class}, nil
		}
		if typedColumnUint32OffsetsListScratchFallbackAllowed(viewStatus) {
			return typedColumnAdapterDecodeUint32OffsetsListScratch(certColumn.Name, rows, offsetsHandle, valuesHandle, typedColumnAdapterClassifyUint32OffsetsListStatus(viewStatus))
		}
		class := typedColumnAdapterClassifyUint32OffsetsListStatus(viewStatus)
		return typedColumnAdapterUint32OffsetsListResourceView{Rows: rows, Class: class}, typedColumnAdapterUint32OffsetsListError(certColumn.Name, class)
	}
	status = typeddecode.StreamingStatus(typeddecode.ReasonHandleSourceUnsupported, fmt.Sprintf("offsets_source=%s values_source=%s", offsetsHandle.Source(), valuesHandle.Source()))
	class := typedColumnAdapterClassifyUint32OffsetsListStatus(status)
	if typedColumnAdapterUint32OffsetsListMixedMmapHeapCopySources(offsetsHandle.Source(), valuesHandle.Source()) {
		return typedColumnAdapterDecodeUint32OffsetsListScratch(certColumn.Name, rows, offsetsHandle, valuesHandle, class)
	}
	return typedColumnAdapterUint32OffsetsListResourceView{Rows: rows, Class: class}, typedColumnAdapterUint32OffsetsListError(certColumn.Name, class)
}

func typedColumnAdapterUint32OffsetsListMixedMmapHeapCopySources(offsetsSource, valuesSource mappedresource.Source) bool {
	return (offsetsSource == mappedresource.SourceMapped && valuesSource == mappedresource.SourceHeapCopy) ||
		(offsetsSource == mappedresource.SourceHeapCopy && valuesSource == mappedresource.SourceMapped)
}

func typedColumnAdapterDecodeUint32OffsetsListScratch(column string, rows int, offsetsHandle *mappedresource.Handle, valuesHandle *mappedresource.Handle, class typedColumnAdapterUint32OffsetsListClassification) (typedColumnAdapterUint32OffsetsListResourceView, error) {
	decoded, err := typedcolumn.DecodeRawUint32OffsetsListFallback(nil, nil, offsetsHandle.Bytes(), valuesHandle.Bytes(), rows)
	if err != nil {
		class = typedColumnAdapterClassifyUint32OffsetsListStatus(typeddecode.UnsupportedStatus(typeddecode.ReasonValidationFailed, err.Error()))
		return typedColumnAdapterUint32OffsetsListResourceView{Rows: rows, Class: class}, typedColumnAdapterUint32OffsetsListError(typedColumnAdapterUint32OffsetsListScratchColumn(column, offsetsHandle), class)
	}
	if class.Class == "" {
		class.Class = typedColumnAdapterUint32OffsetsListViewScratchDecode
	}
	if class.Counter == "" {
		class.Counter = typeddecode.CounterScratchDecode
	} else if class.Counter != typeddecode.CounterScratchDecode && !slices.Contains(class.Counters, typeddecode.CounterScratchDecode) {
		class.Counters = append(class.Counters, typeddecode.CounterScratchDecode)
	}
	return typedColumnAdapterUint32OffsetsListResourceView{Rows: rows, Offsets: decoded.Offsets, Values: decoded.Values, Scratch: true, Class: class}, nil
}

func typedColumnAdapterUint32OffsetsListScratchColumn(column string, offsetsHandle *mappedresource.Handle) string {
	if column != "" {
		return column
	}
	if offsetsHandle != nil {
		return offsetsHandle.Key().Section.Column
	}
	return ""
}

func typedColumnAdapterUint32OffsetsListError(column string, class typedColumnAdapterUint32OffsetsListClassification) error {
	if column == "" {
		column = "<unknown>"
	}
	return fmt.Errorf("collections: typed-column adapter column %q offsets-list direct-view classification=%s reason=%s: %s", column, class.Class, class.Status.Reason, class.Status.String())
}

func typedColumnAdapterClassifyUint32OffsetsListStatus(status typeddecode.Status) typedColumnAdapterUint32OffsetsListClassification {
	class := typedColumnAdapterUint32OffsetsListClassification{Status: status}
	switch status.Reason {
	case typeddecode.ReasonHandleSourceUnsupported:
		class.Class = typedColumnAdapterUint32OffsetsListViewSourceUnsupported
		class.Counter = typeddecode.CounterSourceUnsupported
	case typeddecode.ReasonNilHandle, typeddecode.ReasonStaleHandle:
		class.Class = typedColumnAdapterUint32OffsetsListViewStaleHandle
		class.Counter = typeddecode.CounterStaleHandle
	case typeddecode.ReasonActualPointerUnaligned:
		class.Class = typedColumnAdapterUint32OffsetsListViewActualPointerUnaligned
		class.Counter = typeddecode.CounterActualPointerUnaligned
	case typeddecode.ReasonAbsoluteOffsetUnaligned:
		class.Class = typedColumnAdapterUint32OffsetsListViewAbsoluteOffsetUnaligned
		class.Counter = typeddecode.CounterAbsoluteOffsetUnaligned
	case typeddecode.ReasonNotWriterCertified, typeddecode.ReasonWrongEndian, typeddecode.ReasonCompressed, typeddecode.ReasonNullableWrapper:
		class.Class = typedColumnAdapterUint32OffsetsListViewCertificationFailure
		class.Counter = typeddecode.CounterCertificationFailure
	case typeddecode.ReasonOffsetsCountMismatch, typeddecode.ReasonOffsetsStartMismatch, typeddecode.ReasonOffsetsNonMonotonic, typeddecode.ReasonOffsetsGoIntRange, typeddecode.ReasonValuesLengthMismatch:
		class.Class = typedColumnAdapterUint32OffsetsListViewValidationFailure
		class.Counter = typeddecode.CounterOffsetsListValidation
	case typeddecode.ReasonDirectViewDeferred:
		class.Class = typedColumnAdapterUint32OffsetsListViewScratchDecode
		class.Counter = typeddecode.CounterScratchDecode
	default:
		class.Class = typedColumnAdapterUint32OffsetsListViewValidationFailure
		class.Counter = typeddecode.CounterOffsetsListValidation
	}
	return class
}

func typedColumnUint32OffsetsListScratchFallbackAllowed(status typeddecode.Status) bool {
	switch status.Reason {
	case typeddecode.ReasonAbsoluteOffsetUnaligned, typeddecode.ReasonActualPointerUnaligned, typeddecode.ReasonWrongEndian, typeddecode.ReasonNotWriterCertified, typeddecode.ReasonDirectViewDeferred:
		return true
	default:
		return false
	}
}

func typedColumnDenseDecodeFallbackAllowed(status typeddecode.Status) bool {
	switch status.Reason {
	case typeddecode.ReasonAbsoluteOffsetUnaligned, typeddecode.ReasonActualPointerUnaligned, typeddecode.ReasonUnaligned, typeddecode.ReasonWrongEndian, typeddecode.ReasonHandleSourceUnsupported, typeddecode.ReasonNotWriterCertified, typeddecode.ReasonDirectViewDeferred:
		return true
	default:
		return false
	}
}

func (r typedColumnAdapterResourceReader) AcquireSection(section typedcolumn.ColumnPartImageSection) (*mappedresource.Handle, error) {
	mgr := r.Manager
	if mgr == nil {
		return nil, errors.New("collections: typed-column adapter resource reader requires manager")
	}
	namespace := r.Namespace
	if namespace == "" {
		namespace = "typed-column-adapter"
	}
	fileID := r.FileID
	if fileID == 0 {
		fileID = 1
	}
	partID := r.PartID
	if partID == 0 {
		partID = r.Image.PartID
	}
	key := mappedresource.Key{
		Class:      mappedresource.ClassTypedColumnAsset,
		Namespace:  namespace,
		Kind:       string(section.Kind),
		Generation: r.Generation,
		PartID:     partID,
		FileID:     fileID,
		Offset:     int64(section.Offset),
		Length:     int64(section.Length),
		Version:    r.Image.Version,
		Encoding:   section.Encoding.String(),
		Section: mappedresource.Section{
			Kind:     string(section.Kind),
			Category: string(section.Category),
			Name:     section.Name,
			Column:   section.Column,
		},
	}
	scope := r.Scope
	if scope == (mappedresource.Scope{}) {
		scope = mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: "typed-column-adapter", Namespace: namespace, Generation: r.Generation, Reason: "typed-column adapter section read"}
	}
	if r.Path != "" {
		return mgr.AcquireFileRange(key, scope, r.Path, mappedresource.AcquireOptions{
			Reason:         "typed-column adapter section read",
			ValidationMode: mappedresource.ValidationVerify,
			PreferMapped:   r.PreferMapped,
			AllowHeapCopy:  r.AllowHeapCopy,
		})
	}
	data, err := r.Image.SectionBytes(section)
	if err != nil {
		return nil, err
	}
	return mgr.AcquireBytes(key, scope, mappedresource.SourceHeapCopy, data, mappedresource.AcquireOptions{Reason: "typed-column adapter heap section read", ValidationMode: mappedresource.ValidationVerify})
}

func typedColumnAdapterHasInt64PredicateColumn(fields []TypedStorageField, column string) (bool, error) {
	adapterColumn, ok, err := typedColumnInt64PredicateAdapterColumn(fields, column)
	if err != nil || !ok {
		return ok, err
	}
	if adapterColumn.Field.ValueType != ColumnStoreValueInt64 || adapterColumn.Field.Nullable || adapterColumn.Definition.Type != typedcolumn.ColumnTypeInt64 {
		if err := requireTypedColumnAdapterCapability(adapterColumn, columnsemantics.OpOrderedRange, fmt.Sprintf("typed-column int64 predicate column %q", column)); err != nil {
			return false, err
		}
		if adapterColumn.Field.Nullable {
			return false, fmt.Errorf("%w: typed-column int64 predicate column %q nullable=true is unsupported", ErrColumnQueryPlanUnsupported, column)
		}
		return false, fmt.Errorf("%w: typed-column int64 predicate column %q is not encoded as int64", ErrColumnQueryPlanUnsupported, column)
	}
	if err := requireTypedColumnAdapterCapability(adapterColumn, columnsemantics.OpOrderedRange, fmt.Sprintf("typed-column int64 predicate column %q", column)); err != nil {
		return false, err
	}
	if err := requireTypedColumnLayoutCapability(adapterColumn, columnsemantics.OpOrderedRange, fmt.Sprintf("typed-column int64 predicate column %q", column)); err != nil {
		return false, err
	}
	return true, nil
}

func typedColumnAdapterHasStringPredicateColumn(fields []TypedStorageField, column string) (bool, error) {
	adapterColumn, ok, err := typedColumnStringPredicateAdapterColumn(fields, column)
	if err != nil || !ok {
		return ok, err
	}
	if adapterColumn.Field.ValueType != ColumnStoreValueString || adapterColumn.Definition.Type != typedcolumn.ColumnTypeLowCardinalityCode || adapterColumn.Definition.Encoding != typedcolumn.EncodingLowCardinalityUint32 {
		return false, fmt.Errorf("%w: typed-column string predicate column %q is not encoded as low-cardinality uint32 codes", ErrColumnQueryPlanUnsupported, column)
	}
	if err := requireTypedColumnAdapterCapability(adapterColumn, columnsemantics.OpDictionaryEquality, fmt.Sprintf("typed-column string predicate column %q", column)); err != nil {
		return false, err
	}
	return true, nil
}

func typedColumnInt64PredicateSemanticOperation(kind TypedColumnInt64PredicateScanKind) columnsemantics.Operation {
	switch kind {
	case TypedColumnInt64PredicateAll:
		return columnsemantics.OpAllRows
	case TypedColumnInt64PredicateEqual:
		return columnsemantics.OpEquality
	case TypedColumnInt64PredicateRange:
		return columnsemantics.OpOrderedRange
	default:
		return columnsemantics.OpUnknownPredicateKind
	}
}

type typedColumnAdapterPartImageDecoder func(typedColumnAdapterOptions, typedcolumn.ColumnPartImage) (*typedColumnAdapterPart, error)

type typedColumnStringPredicatePreparedPart struct {
	AdapterPart     *typedColumnAdapterPart
	Column          typedColumnAdapterColumn
	QueryCode       uint32
	QueryCodeFound  bool
	ManifestBytes   int
	DictionaryBytes int
}

func typedColumnAdapterPrepareInt64PredicateScanPart(fields []TypedStorageField, raw []byte, refPartID uint64, typedRows int, physicalRows int, schemaHash uint64, column string) (*typedColumnAdapterPart, typedColumnAdapterColumn, int, error) {
	return typedColumnAdapterPrepareInt64PredicatePart(fields, raw, refPartID, typedRows, physicalRows, schemaHash, column, "scan", typedColumnAdapterPartFromImageForInt64PredicateScan)
}

func typedColumnAdapterPrepareInt64PredicateAggregatePart(fields []TypedStorageField, raw []byte, refPartID uint64, typedRows int, physicalRows int, schemaHash uint64, column string) (*typedColumnAdapterPart, typedColumnAdapterColumn, int, error) {
	adapterColumn, ok, err := typedColumnInt64PredicateAdapterColumn(fields, column)
	if err != nil {
		return nil, typedColumnAdapterColumn{}, 0, err
	}
	if !ok {
		return nil, typedColumnAdapterColumn{}, 0, fmt.Errorf("collections: typed-column int64 predicate aggregate column %q is not owned by typed_column_part", column)
	}
	if adapterColumn.Field.ValueType != ColumnStoreValueInt64 || adapterColumn.Field.Nullable || adapterColumn.Definition.Type != typedcolumn.ColumnTypeInt64 {
		if err := requireTypedColumnAdapterCapability(adapterColumn, columnsemantics.OpSum, fmt.Sprintf("typed-column int64 predicate aggregate column %q", column)); err != nil {
			return nil, typedColumnAdapterColumn{}, 0, err
		}
		return nil, typedColumnAdapterColumn{}, 0, fmt.Errorf("%w: typed-column int64 predicate aggregate column %q is not a non-null scalar int64 typed-column", ErrColumnQueryPlanUnsupported, column)
	}
	if err := requireTypedColumnAdapterCapability(adapterColumn, columnsemantics.OpSum, fmt.Sprintf("typed-column int64 predicate aggregate column %q", column)); err != nil {
		return nil, typedColumnAdapterColumn{}, 0, err
	}
	if err := requireTypedColumnLayoutCapability(adapterColumn, columnsemantics.OpSum, fmt.Sprintf("typed-column int64 predicate aggregate column %q", column)); err != nil {
		return nil, typedColumnAdapterColumn{}, 0, err
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return nil, typedColumnAdapterColumn{}, 0, err
	}
	if image.PartID != refPartID || image.Rows != typedRows || image.Rows != physicalRows {
		return nil, typedColumnAdapterColumn{}, 0, fmt.Errorf("collections: typed_column_part aggregate image/ref mismatch image_part=%d ref_part=%d image_rows=%d typed_manifest_rows=%d physical_rows=%d", image.PartID, refPartID, image.Rows, typedRows, physicalRows)
	}
	adapterPart, err := typedColumnAdapterInt64AggregatePartFromImage(typedColumnAdapterOptions{Fields: []TypedStorageField{adapterColumn.Field}, SchemaVersion: uint32(schemaHash)}, image, adapterColumn)
	if err != nil {
		return nil, typedColumnAdapterColumn{}, 0, err
	}
	if adapterPart.Part.Descriptor.SchemaVersion != uint32(schemaHash) {
		return nil, typedColumnAdapterColumn{}, 0, fmt.Errorf("collections: typed_column_part schema_version=%d want %d", adapterPart.Part.Descriptor.SchemaVersion, uint32(schemaHash))
	}
	if len(adapterPart.Columns) == 1 {
		adapterColumn = adapterPart.Columns[0]
	}
	return adapterPart, adapterColumn, image.ManifestBytes, nil
}

type typedColumnInt64AggregateRangeReader func(offset int, length int, section bool) ([]byte, error)
type typedColumnInt64AggregatePayloadReader func(offset int, length int) ([]byte, error)

type typedColumnInt64AggregateBlockRange struct {
	index  int
	offset int
	length int
}

type typedColumnInt64AggregateTargetedPart struct {
	adapterPart   *typedColumnAdapterPart
	adapterColumn typedColumnAdapterColumn
	manifestBytes int
	blockRanges   []typedColumnInt64AggregateBlockRange
}

func typedColumnAdapterPrepareInt64PredicateAggregatePartFromRanges(fields []TypedStorageField, refLength int64, refPartID uint64, typedRows int, physicalRows int, schemaHash uint64, column string, req TypedColumnInt64PredicateScanRequest, readRange typedColumnInt64AggregateRangeReader) (*typedColumnAdapterPart, typedColumnAdapterColumn, int, error) {
	targeted, err := typedColumnAdapterPrepareInt64PredicateAggregateTargetedPartFromRanges(fields, refLength, refPartID, typedRows, physicalRows, schemaHash, column, req, readRange)
	if err != nil {
		return nil, typedColumnAdapterColumn{}, 0, err
	}
	readPayload := func(offset int, length int) ([]byte, error) {
		return readRange(offset, length, false)
	}
	adapterPart, adapterColumn, err := targeted.instantiate(readPayload)
	if err != nil {
		return nil, typedColumnAdapterColumn{}, 0, err
	}
	return adapterPart, adapterColumn, targeted.manifestBytes, nil
}

func typedColumnAdapterPrepareInt64PredicateAggregateTargetedPartFromRanges(fields []TypedStorageField, refLength int64, refPartID uint64, typedRows int, physicalRows int, schemaHash uint64, column string, req TypedColumnInt64PredicateScanRequest, readRange typedColumnInt64AggregateRangeReader) (*typedColumnInt64AggregateTargetedPart, error) {
	if readRange == nil {
		return nil, errors.New("collections: typed-column int64 aggregate targeted prepare requires range reader")
	}
	if refLength > int64(maxCollectionInt) {
		return nil, fmt.Errorf("collections: typed-column part length=%d overflows int", refLength)
	}
	header, err := readRange(0, typedcolumn.ColumnPartImageManifestHeaderBytes, true)
	if err != nil {
		return nil, err
	}
	manifestBytes, err := typedcolumn.ColumnPartImageManifestLength(header)
	if err != nil {
		return nil, err
	}
	if manifestBytes > int(refLength) {
		return nil, fmt.Errorf("collections: typed-column part manifest bytes=%d exceed ref length=%d", manifestBytes, refLength)
	}
	manifest := make([]byte, manifestBytes)
	copy(manifest, header)
	if manifestBytes > len(header) {
		tail, err := readRange(len(header), manifestBytes-len(header), true)
		if err != nil {
			return nil, err
		}
		copy(manifest[len(header):], tail)
	}
	image, err := typedcolumn.ParseColumnPartImageManifest(manifest, int(refLength))
	if err != nil {
		return nil, err
	}
	descriptorSection, err := typedColumnAdapterImageSingleSection(image, typedcolumn.ColumnPartImageSectionDescriptor)
	if err != nil {
		return nil, err
	}
	descriptorRaw, err := readRange(descriptorSection.Offset, descriptorSection.Length, true)
	if err != nil {
		return nil, err
	}
	return typedColumnAdapterPrepareInt64PredicateAggregateTargetedPartFromSections(fields, image, descriptorRaw, refPartID, typedRows, physicalRows, schemaHash, column, req)
}

func typedColumnAdapterPrepareInt64PredicateAggregatePartFromSections(fields []TypedStorageField, image typedcolumn.ColumnPartImage, descriptorRaw []byte, refPartID uint64, typedRows int, physicalRows int, schemaHash uint64, column string, req TypedColumnInt64PredicateScanRequest, readPayload typedColumnInt64AggregatePayloadReader) (*typedColumnAdapterPart, typedColumnAdapterColumn, error) {
	targeted, err := typedColumnAdapterPrepareInt64PredicateAggregateTargetedPartFromSections(fields, image, descriptorRaw, refPartID, typedRows, physicalRows, schemaHash, column, req)
	if err != nil {
		return nil, typedColumnAdapterColumn{}, err
	}
	return targeted.instantiate(readPayload)
}

func typedColumnAdapterPrepareInt64PredicateAggregateTargetedPartFromSections(fields []TypedStorageField, image typedcolumn.ColumnPartImage, descriptorRaw []byte, refPartID uint64, typedRows int, physicalRows int, schemaHash uint64, column string, req TypedColumnInt64PredicateScanRequest) (*typedColumnInt64AggregateTargetedPart, error) {
	adapterColumn, ok, err := typedColumnInt64PredicateAdapterColumn(fields, column)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("collections: typed-column int64 predicate aggregate column %q is not owned by typed_column_part", column)
	}
	if adapterColumn.Field.ValueType != ColumnStoreValueInt64 || adapterColumn.Field.Nullable || adapterColumn.Definition.Type != typedcolumn.ColumnTypeInt64 {
		if err := requireTypedColumnAdapterCapability(adapterColumn, typedColumnInt64PredicateSemanticOperation(req.Kind), fmt.Sprintf("typed-column int64 predicate aggregate column %q", column)); err != nil {
			return nil, err
		}
		if err := requireTypedColumnAdapterCapability(adapterColumn, columnsemantics.OpSum, fmt.Sprintf("typed-column int64 predicate aggregate column %q", column)); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%w: typed-column int64 predicate aggregate column %q is not a non-null scalar int64 typed-column", ErrColumnQueryPlanUnsupported, column)
	}
	if err := requireTypedColumnAdapterCapability(adapterColumn, typedColumnInt64PredicateSemanticOperation(req.Kind), fmt.Sprintf("typed-column int64 predicate aggregate column %q", column)); err != nil {
		return nil, err
	}
	if err := requireTypedColumnAdapterCapability(adapterColumn, columnsemantics.OpSum, fmt.Sprintf("typed-column int64 predicate aggregate column %q", column)); err != nil {
		return nil, err
	}
	if err := requireTypedColumnLayoutCapability(adapterColumn, typedColumnInt64PredicateSemanticOperation(req.Kind), fmt.Sprintf("typed-column int64 predicate aggregate column %q", column)); err != nil {
		return nil, err
	}
	if err := requireTypedColumnLayoutCapability(adapterColumn, columnsemantics.OpSum, fmt.Sprintf("typed-column int64 predicate aggregate column %q", column)); err != nil {
		return nil, err
	}
	if image.PartID != refPartID || image.Rows != typedRows || image.Rows != physicalRows {
		return nil, fmt.Errorf("collections: typed_column_part aggregate image/ref mismatch image_part=%d ref_part=%d image_rows=%d typed_manifest_rows=%d physical_rows=%d", image.PartID, refPartID, image.Rows, typedRows, physicalRows)
	}
	desc, columns, err := typedcolumn.DecodeColumnPartDescriptorSection(descriptorRaw)
	if err != nil {
		return nil, err
	}
	columns, err = typedColumnAdapterColumnsWithSectionCompression(image, columns)
	if err != nil {
		return nil, err
	}
	part := &typedcolumn.ColumnPart{Descriptor: desc, Columns: columns}
	if err := validateTypedColumnAdapterInt64AggregateImage(part, adapterColumn, uint32(schemaHash)); err != nil {
		return nil, err
	}
	if part.Descriptor.PartID != image.PartID || part.Descriptor.RowCount != image.Rows {
		return nil, fmt.Errorf("collections: typed_column_part aggregate descriptor/image mismatch descriptor_part=%d image_part=%d descriptor_rows=%d image_rows=%d", part.Descriptor.PartID, image.PartID, part.Descriptor.RowCount, image.Rows)
	}
	if part.Descriptor.SchemaVersion != uint32(schemaHash) {
		return nil, fmt.Errorf("collections: typed_column_part schema_version=%d want %d", part.Descriptor.SchemaVersion, uint32(schemaHash))
	}
	if got, ok := part.Columns[adapterColumn.Definition.Name]; ok {
		adapterColumn.Definition = got.Definition
	}
	if err := validateTypedColumnAdapterInt64AggregateTargetedSections(image, part); err != nil {
		return nil, err
	}
	section, ok := typedColumnAdapterColumnDataSection(image, adapterColumn.Definition.Name)
	if !ok {
		return nil, fmt.Errorf("collections: typed-column int64 aggregate image missing column data section %q", adapterColumn.Definition.Name)
	}
	if section.Encoding != adapterColumn.Definition.Encoding || section.Compression != adapterColumn.Definition.Compression {
		if section.Encoding != adapterColumn.Definition.Encoding {
			return nil, fmt.Errorf("collections: typed-column int64 aggregate column %q section encoding=%s want %s", adapterColumn.Definition.Name, section.Encoding, adapterColumn.Definition.Encoding)
		}
		if err := validateTypedColumnProductionCompression(section.Compression); err != nil {
			return nil, fmt.Errorf("collections: typed-column int64 aggregate column %q section compression=%s unsupported: %w", adapterColumn.Definition.Name, section.Compression, err)
		}
		adapterColumn.Definition.Compression = section.Compression
	}
	valueCol := part.Columns[adapterColumn.Definition.Name]
	valueCol.Definition = adapterColumn.Definition
	if err := validateTypedColumnProductionPartColumnLayout(adapterColumn.Field, valueCol); err != nil {
		return nil, fmt.Errorf("collections: typed-column int64 aggregate column %q layout validation failed: %w", adapterColumn.Definition.Name, err)
	}
	if section.Blocks != 0 && section.Blocks != len(valueCol.Blocks) {
		return nil, fmt.Errorf("collections: typed-column int64 aggregate column %q section blocks=%d want %d", adapterColumn.Definition.Name, section.Blocks, len(valueCol.Blocks))
	}
	if section.Rows != 0 && section.Rows != image.Rows {
		return nil, fmt.Errorf("collections: typed-column int64 aggregate column %q section rows=%d want %d", adapterColumn.Definition.Name, section.Rows, image.Rows)
	}
	blockRanges := make([]typedColumnInt64AggregateBlockRange, 0, len(valueCol.Blocks))
	offset := section.Offset
	sectionEnd := section.Offset + section.Length
	for i := range valueCol.Blocks {
		block := &valueCol.Blocks[i]
		length := block.Descriptor.StoredBytes
		if length <= 0 || offset > sectionEnd || length > sectionEnd-offset {
			return nil, fmt.Errorf("collections: typed-column int64 aggregate column %q block %d length=%d outside section", adapterColumn.Definition.Name, i, length)
		}
		blockOffset := offset
		offset += length
		if block.Granule.HasMinMax && !typedColumnInt64PredicateMayMatch(req, block.Granule.Min, block.Granule.Max) {
			continue
		}
		blockRanges = append(blockRanges, typedColumnInt64AggregateBlockRange{index: i, offset: blockOffset, length: length})
	}
	if offset != sectionEnd {
		return nil, fmt.Errorf("collections: typed-column int64 aggregate column %q consumed=%d section=%d", adapterColumn.Definition.Name, offset-section.Offset, section.Length)
	}
	part.Columns[adapterColumn.Definition.Name] = valueCol
	adapterPart := &typedColumnAdapterPart{Options: typedColumnAdapterOptions{Fields: []TypedStorageField{adapterColumn.Field}, SchemaVersion: uint32(schemaHash)}, Columns: []typedColumnAdapterColumn{adapterColumn}, Part: part}
	return &typedColumnInt64AggregateTargetedPart{adapterPart: adapterPart, adapterColumn: adapterColumn, manifestBytes: image.ManifestBytes, blockRanges: blockRanges}, nil
}

func typedColumnAdapterColumnsWithSectionCompression(image typedcolumn.ColumnPartImage, columns map[string]typedcolumn.ColumnPartColumn) (map[string]typedcolumn.ColumnPartColumn, error) {
	out := make(map[string]typedcolumn.ColumnPartColumn, len(columns))
	for name, column := range columns {
		out[name] = column
	}
	for _, section := range image.Sections {
		if section.Kind != typedcolumn.ColumnPartImageSectionColumnData {
			continue
		}
		column, ok := out[section.Column]
		if !ok {
			return nil, fmt.Errorf("collections: typed-column adapter image unexpected column data section %q", section.Column)
		}
		if section.Encoding != column.Definition.Encoding {
			return nil, fmt.Errorf("collections: typed-column adapter image column %q section encoding=%s want %s", section.Column, section.Encoding, column.Definition.Encoding)
		}
		if err := validateTypedColumnProductionCompression(section.Compression); err != nil {
			return nil, fmt.Errorf("collections: typed-column adapter image column %q section compression=%s unsupported: %w", section.Column, section.Compression, err)
		}
		column.Definition.Compression = section.Compression
		out[section.Column] = column
	}
	return out, nil
}

func validateTypedColumnAdapterInt64AggregateTargetedSections(image typedcolumn.ColumnPartImage, part *typedcolumn.ColumnPart) error {
	if part == nil {
		return errors.New("collections: typed-column int64 aggregate targeted nil image part")
	}
	sectionsByColumn := make(map[string]typedcolumn.ColumnPartImageSection, len(part.Columns))
	for _, section := range image.Sections {
		if section.Kind != typedcolumn.ColumnPartImageSectionColumnData {
			continue
		}
		column, ok := part.Columns[section.Column]
		if !ok {
			return fmt.Errorf("collections: typed-column int64 aggregate image unexpected column data section %q", section.Column)
		}
		if _, exists := sectionsByColumn[section.Column]; exists {
			return fmt.Errorf("collections: typed-column int64 aggregate image duplicate column data section %q", section.Column)
		}
		if section.Encoding != column.Definition.Encoding {
			return fmt.Errorf("collections: typed-column int64 aggregate image column %q section encoding=%s want %s", section.Column, section.Encoding, column.Definition.Encoding)
		}
		if err := validateTypedColumnProductionCompression(section.Compression); err != nil {
			return fmt.Errorf("collections: typed-column int64 aggregate image column %q section compression=%s unsupported: %w", section.Column, section.Compression, err)
		}
		if section.Rows != 0 && section.Rows != part.Descriptor.RowCount {
			return fmt.Errorf("collections: typed-column int64 aggregate image column %q section rows=%d want %d", section.Column, section.Rows, part.Descriptor.RowCount)
		}
		if section.Blocks != 0 && section.Blocks != len(column.Blocks) {
			return fmt.Errorf("collections: typed-column int64 aggregate image column %q section blocks=%d want %d", section.Column, section.Blocks, len(column.Blocks))
		}
		expectedBytes := 0
		for i, block := range column.Blocks {
			if block.Descriptor.StoredBytes < 0 {
				return fmt.Errorf("collections: typed-column int64 aggregate image column %q block %d stored_bytes=%d", section.Column, i, block.Descriptor.StoredBytes)
			}
			if expectedBytes > maxCollectionInt-block.Descriptor.StoredBytes {
				return fmt.Errorf("collections: typed-column int64 aggregate image column %q stored bytes overflow", section.Column)
			}
			expectedBytes += block.Descriptor.StoredBytes
		}
		if err := validateTypedColumnProductionBlocks(section.Column, section.Encoding, section.Compression, column.Blocks); err != nil {
			return fmt.Errorf("collections: typed-column int64 aggregate image column %q blocks validation failed: %w", section.Column, err)
		}
		if section.Length != expectedBytes {
			return fmt.Errorf("collections: typed-column int64 aggregate image column %q section length=%d want %d", section.Column, section.Length, expectedBytes)
		}
		sectionsByColumn[section.Column] = section
	}
	for name := range part.Columns {
		if _, ok := sectionsByColumn[name]; !ok {
			return fmt.Errorf("collections: typed-column int64 aggregate image missing column data section %q", name)
		}
	}
	return nil
}

func (p *typedColumnInt64AggregateTargetedPart) instantiate(readPayload typedColumnInt64AggregatePayloadReader) (*typedColumnAdapterPart, typedColumnAdapterColumn, error) {
	if p == nil || p.adapterPart == nil || p.adapterPart.Part == nil {
		return nil, typedColumnAdapterColumn{}, errors.New("collections: typed-column int64 aggregate targeted metadata is missing")
	}
	if len(p.blockRanges) != 0 && readPayload == nil {
		return nil, typedColumnAdapterColumn{}, errors.New("collections: typed-column int64 aggregate targeted prepare requires payload reader")
	}
	basePart := p.adapterPart.Part
	partCopy := *basePart
	columns := make(map[string]typedcolumn.ColumnPartColumn, len(basePart.Columns))
	for name, column := range basePart.Columns {
		columnCopy := column
		if len(column.Blocks) != 0 {
			columnCopy.Blocks = append([]typedcolumn.ColumnBlock(nil), column.Blocks...)
		}
		columns[name] = columnCopy
	}
	partCopy.Columns = columns
	adapterPartCopy := *p.adapterPart
	if len(p.adapterPart.Columns) != 0 {
		adapterPartCopy.Columns = append([]typedColumnAdapterColumn(nil), p.adapterPart.Columns...)
	}
	adapterPartCopy.Part = &partCopy
	valueCol, ok := partCopy.Columns[p.adapterColumn.Definition.Name]
	if !ok {
		return nil, typedColumnAdapterColumn{}, fmt.Errorf("collections: typed-column int64 aggregate cached metadata missing column %q", p.adapterColumn.Definition.Name)
	}
	for _, blockRange := range p.blockRanges {
		if blockRange.index < 0 || blockRange.index >= len(valueCol.Blocks) {
			return nil, typedColumnAdapterColumn{}, fmt.Errorf("collections: typed-column int64 aggregate cached block index=%d out of bounds", blockRange.index)
		}
		payload, err := readPayload(blockRange.offset, blockRange.length)
		if err != nil {
			return nil, typedColumnAdapterColumn{}, fmt.Errorf("collections: typed-column int64 aggregate read column %q block %d payload: %w", p.adapterColumn.Definition.Name, blockRange.index, err)
		}
		if len(payload) != blockRange.length {
			return nil, typedColumnAdapterColumn{}, fmt.Errorf("collections: typed-column int64 aggregate column %q block %d payload bytes=%d want %d", p.adapterColumn.Definition.Name, blockRange.index, len(payload), blockRange.length)
		}
		block := &valueCol.Blocks[blockRange.index]
		block.Granule.Payload = payload
		block.Granule.PayloadRef = typedcolumn.PayloadRef{Kind: typedcolumn.PayloadRefInline, Length: blockRange.length}
	}
	partCopy.Columns[p.adapterColumn.Definition.Name] = valueCol
	return &adapterPartCopy, p.adapterColumn, nil
}

func typedColumnAdapterImageSingleSection(image typedcolumn.ColumnPartImage, kind typedcolumn.ColumnPartImageSectionKind) (typedcolumn.ColumnPartImageSection, error) {
	var out typedcolumn.ColumnPartImageSection
	found := false
	for _, section := range image.Sections {
		if section.Kind != kind {
			continue
		}
		if found {
			return typedcolumn.ColumnPartImageSection{}, fmt.Errorf("duplicate %s section", kind)
		}
		out = section
		found = true
	}
	if !found {
		return typedcolumn.ColumnPartImageSection{}, fmt.Errorf("missing %s section", kind)
	}
	return out, nil
}

func typedColumnAdapterColumnDataSection(image typedcolumn.ColumnPartImage, column string) (typedcolumn.ColumnPartImageSection, bool) {
	for _, section := range image.Sections {
		if section.Kind == typedcolumn.ColumnPartImageSectionColumnData && section.Column == column {
			return section, true
		}
	}
	return typedcolumn.ColumnPartImageSection{}, false
}

func typedColumnAdapterColumnOffsetsListSections(image typedcolumn.ColumnPartImage, column string) (typedcolumn.ColumnPartImageSection, typedcolumn.ColumnPartImageSection, bool) {
	return image.ColumnOffsetsListSections(column)
}

func typedColumnAdapterPrepareInt64PredicatePart(fields []TypedStorageField, raw []byte, refPartID uint64, typedRows int, physicalRows int, schemaHash uint64, column string, operation string, decode typedColumnAdapterPartImageDecoder) (*typedColumnAdapterPart, typedColumnAdapterColumn, int, error) {
	adapterColumn, ok, err := typedColumnInt64PredicateAdapterColumn(fields, column)
	if err != nil {
		return nil, typedColumnAdapterColumn{}, 0, err
	}
	if !ok {
		return nil, typedColumnAdapterColumn{}, 0, fmt.Errorf("collections: typed-column int64 predicate %s column %q is not owned by typed_column_part", operation, column)
	}
	if adapterColumn.Field.ValueType != ColumnStoreValueInt64 || adapterColumn.Field.Nullable || adapterColumn.Definition.Type != typedcolumn.ColumnTypeInt64 {
		if err := requireTypedColumnAdapterCapability(adapterColumn, columnsemantics.OpOrderedRange, fmt.Sprintf("typed-column int64 predicate %s column %q", operation, column)); err != nil {
			return nil, typedColumnAdapterColumn{}, 0, err
		}
		if adapterColumn.Field.Nullable {
			return nil, typedColumnAdapterColumn{}, 0, fmt.Errorf("%w: typed-column int64 predicate %s column %q nullable=true is unsupported", ErrColumnQueryPlanUnsupported, operation, column)
		}
		return nil, typedColumnAdapterColumn{}, 0, fmt.Errorf("%w: typed-column int64 predicate %s column %q is not encoded as int64", ErrColumnQueryPlanUnsupported, operation, column)
	}
	if err := requireTypedColumnAdapterCapability(adapterColumn, columnsemantics.OpOrderedRange, fmt.Sprintf("typed-column int64 predicate %s column %q", operation, column)); err != nil {
		return nil, typedColumnAdapterColumn{}, 0, err
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return nil, typedColumnAdapterColumn{}, 0, err
	}
	if image.PartID != refPartID || image.Rows != typedRows || image.Rows != physicalRows {
		return nil, typedColumnAdapterColumn{}, 0, fmt.Errorf("collections: typed_column_part %s image/ref mismatch image_part=%d ref_part=%d image_rows=%d typed_manifest_rows=%d physical_rows=%d", operation, image.PartID, refPartID, image.Rows, typedRows, physicalRows)
	}
	adapterPart, err := decode(typedColumnAdapterOptions{Fields: fields, SchemaVersion: uint32(schemaHash)}, image)
	if err != nil {
		return nil, typedColumnAdapterColumn{}, 0, err
	}
	if adapterPart.Part.Descriptor.SchemaVersion != uint32(schemaHash) {
		return nil, typedColumnAdapterColumn{}, 0, fmt.Errorf("collections: typed_column_part schema_version=%d want %d", adapterPart.Part.Descriptor.SchemaVersion, uint32(schemaHash))
	}
	for _, candidate := range adapterPart.Columns {
		if candidate.Definition.Name == adapterColumn.Definition.Name {
			adapterColumn = candidate
			break
		}
	}
	return adapterPart, adapterColumn, image.ManifestBytes, nil
}

// typedColumnAdapterPrepareStringPredicateScanPart decodes and validates durable
// typed-column dictionary metadata before any string predicate row loop can run.
func typedColumnAdapterPrepareStringPredicateScanPart(fields []TypedStorageField, raw []byte, refPartID uint64, typedRows int, physicalRows int, schemaHash uint64, column string, value string) (typedColumnStringPredicatePreparedPart, error) {
	adapterColumn, ok, err := typedColumnStringPredicateAdapterColumn(fields, column)
	if err != nil {
		return typedColumnStringPredicatePreparedPart{}, err
	}
	if !ok {
		return typedColumnStringPredicatePreparedPart{}, fmt.Errorf("collections: typed-column string predicate scan column %q is not owned by typed_column_part", column)
	}
	if adapterColumn.Field.ValueType != ColumnStoreValueString || adapterColumn.Definition.Type != typedcolumn.ColumnTypeLowCardinalityCode {
		return typedColumnStringPredicatePreparedPart{}, fmt.Errorf("%w: typed-column string predicate scan column %q is not encoded as low-cardinality string", ErrColumnQueryPlanUnsupported, column)
	}
	if adapterColumn.Field.Nullable {
		return typedColumnStringPredicatePreparedPart{}, fmt.Errorf("%w: typed-column string predicate scan column %q nullable=true is unsupported", ErrColumnQueryPlanUnsupported, column)
	}
	if err := requireTypedColumnAdapterCapability(adapterColumn, columnsemantics.OpDictionaryEquality, fmt.Sprintf("typed-column string predicate scan column %q", column)); err != nil {
		return typedColumnStringPredicatePreparedPart{}, err
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return typedColumnStringPredicatePreparedPart{}, err
	}
	if _, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image); err != nil {
		return typedColumnStringPredicatePreparedPart{}, fmt.Errorf("collections: typed-column string predicate scan dictionary/layout identity validation: %w", err)
	}
	if image.PartID != refPartID || image.Rows != typedRows || image.Rows != physicalRows {
		return typedColumnStringPredicatePreparedPart{}, fmt.Errorf("collections: typed_column_part string predicate scan image/ref mismatch image_part=%d ref_part=%d image_rows=%d typed_manifest_rows=%d physical_rows=%d", image.PartID, refPartID, image.Rows, typedRows, physicalRows)
	}
	adapterPart, err := typedColumnAdapterPartFromImageForStringPredicateScan(typedColumnAdapterOptions{Fields: fields, SchemaVersion: uint32(schemaHash)}, image)
	if err != nil {
		return typedColumnStringPredicatePreparedPart{}, err
	}
	if adapterPart.Part.Descriptor.SchemaVersion != uint32(schemaHash) {
		return typedColumnStringPredicatePreparedPart{}, fmt.Errorf("collections: typed_column_part schema_version=%d want %d", adapterPart.Part.Descriptor.SchemaVersion, uint32(schemaHash))
	}
	for _, candidate := range adapterPart.Columns {
		if candidate.Definition.Name == adapterColumn.Definition.Name {
			adapterColumn = candidate
			break
		}
	}
	partColumn, ok := adapterPart.Part.Columns[adapterColumn.Definition.Name]
	if !ok {
		return typedColumnStringPredicatePreparedPart{}, fmt.Errorf("collections: typed-column string predicate scan missing column %q", adapterColumn.Definition.Name)
	}
	if partColumn.Definition.Type != typedcolumn.ColumnTypeLowCardinalityCode || partColumn.Definition.Encoding != typedcolumn.EncodingLowCardinalityUint32 {
		return typedColumnStringPredicatePreparedPart{}, fmt.Errorf("%w: typed-column string predicate scan column %q type=%s encoding=%s", ErrColumnQueryPlanUnsupported, column, partColumn.Definition.Type, partColumn.Definition.Encoding)
	}
	dict := adapterPart.Dictionary[adapterColumn.Definition.Name]
	code, found := dict[value]
	prepared := typedColumnStringPredicatePreparedPart{AdapterPart: adapterPart, Column: adapterColumn, QueryCodeFound: found, ManifestBytes: image.ManifestBytes, DictionaryBytes: image.CategoryBytes(typedcolumn.ColumnPartImageCategoryDictionaries)}
	if !found {
		return prepared, nil
	}
	if code < 0 || uint64(code) >= uint64(partColumn.Definition.Cardinality) || uint64(code) > uint64(^uint32(0)) {
		return typedColumnStringPredicatePreparedPart{}, fmt.Errorf("collections: typed-column string predicate scan dictionary code %d for column %q outside cardinality %d", code, adapterColumn.Definition.Name, partColumn.Definition.Cardinality)
	}
	prepared.QueryCode = uint32(code)
	return prepared, nil
}

func typedColumnStringPredicateAdapterColumn(fields []TypedStorageField, column string) (typedColumnAdapterColumn, bool, error) {
	columns, err := typedColumnAdapterColumnsForFields(fields)
	if err != nil {
		return typedColumnAdapterColumn{}, false, err
	}
	for _, adapterColumn := range columns {
		if adapterColumn.Field.Name == column || adapterColumn.Field.Path == column || adapterColumn.Definition.Name == column {
			return adapterColumn, true, nil
		}
	}
	return typedColumnAdapterColumn{}, false, nil
}

func typedColumnInt64PredicateAdapterColumn(fields []TypedStorageField, column string) (typedColumnAdapterColumn, bool, error) {
	columns, err := typedColumnAdapterColumnsForFields(fields)
	if err != nil {
		return typedColumnAdapterColumn{}, false, err
	}
	for _, adapterColumn := range columns {
		if adapterColumn.Field.Name == column || adapterColumn.Field.Path == column || adapterColumn.Definition.Name == column {
			return adapterColumn, true, nil
		}
	}
	return typedColumnAdapterColumn{}, false, nil
}

func scanTypedColumnStringEqualityPredicateCodes(part *typedcolumn.ColumnPart, valueColumn string, queryCode uint32, queryCodeFound bool, visit func(rowIndex int, primaryID int64) error) (bool, int, int, error) {
	if part == nil {
		return false, 0, 0, errors.New("nil typed-column part")
	}
	if !queryCodeFound {
		return true, 0, 0, nil
	}
	valueCol, ok := part.Columns[valueColumn]
	if !ok {
		return false, 0, 0, fmt.Errorf("missing value column %q", valueColumn)
	}
	idCol, ok := part.Columns[typedColumnAdapterPrimaryIDColumn]
	if !ok {
		return false, 0, 0, fmt.Errorf("missing primary id column %q", typedColumnAdapterPrimaryIDColumn)
	}
	if valueCol.Definition.Type != typedcolumn.ColumnTypeLowCardinalityCode || valueCol.Definition.Encoding != typedcolumn.EncodingLowCardinalityUint32 {
		return false, 0, 0, fmt.Errorf("value column %q is not low-cardinality uint32", valueColumn)
	}
	if idCol.Definition.Type != typedcolumn.ColumnTypeInt64 {
		return false, 0, 0, fmt.Errorf("primary id column %q is not int64", typedColumnAdapterPrimaryIDColumn)
	}
	cardinality := valueCol.Definition.Cardinality
	if cardinality == 0 || queryCode >= cardinality {
		return false, 0, 0, fmt.Errorf("query code %d outside cardinality %d", queryCode, cardinality)
	}
	var reader typedcolumn.GranuleReader
	var codeScratch []uint32
	var idScratch []int64
	idBlockIndex := 0
	decodedAny := false
	rowsScanned := 0
	rowsMatched := 0
	for _, block := range valueCol.Blocks {
		g := block.Granule
		if g.HasMinMax {
			if g.Min < 0 || g.Max < 0 || g.Min > g.Max || uint64(g.Max) >= uint64(cardinality) {
				return false, rowsScanned, rowsMatched, fmt.Errorf("value column %q block first_row=%d min/max [%d,%d] outside cardinality %d", valueColumn, block.Descriptor.FirstRow, g.Min, g.Max, cardinality)
			}
			if int64(queryCode) < g.Min || int64(queryCode) > g.Max {
				continue
			}
		}
		idBlock, ok := typedColumnAlignedBlock(idCol.Blocks, &idBlockIndex, block.Descriptor.FirstRow, block.Descriptor.RowCount)
		if !ok {
			return false, rowsScanned, rowsMatched, fmt.Errorf("missing aligned primary-id block first_row=%d rows=%d", block.Descriptor.FirstRow, block.Descriptor.RowCount)
		}
		codes, err := reader.DecodeUint32CodesInto(codeScratch[:0], g)
		if err != nil {
			return false, rowsScanned, rowsMatched, err
		}
		codeScratch = codes
		ids, err := reader.DecodeInt64Into(idScratch[:0], idBlock.Granule)
		if err != nil {
			return false, rowsScanned, rowsMatched, err
		}
		idScratch = ids
		if len(codes) != block.Descriptor.RowCount || len(ids) != block.Descriptor.RowCount {
			return false, rowsScanned, rowsMatched, fmt.Errorf("decoded rows codes=%d ids=%d want %d", len(codes), len(ids), block.Descriptor.RowCount)
		}
		decodedAny = true
		rowsScanned += len(codes)
		for i, code := range codes {
			if code >= cardinality {
				return false, rowsScanned, rowsMatched, fmt.Errorf("typed-column string predicate code %d outside cardinality %d", code, cardinality)
			}
			if code != queryCode {
				continue
			}
			rowsMatched++
			if visit != nil {
				if err := visit(block.Descriptor.FirstRow+i, ids[i]); err != nil {
					return false, rowsScanned, rowsMatched, err
				}
			}
		}
	}
	return !decodedAny && len(valueCol.Blocks) != 0, rowsScanned, rowsMatched, nil
}

func scanTypedColumnInt64PredicatePart(part *typedcolumn.ColumnPart, valueColumn string, req TypedColumnInt64PredicateScanRequest, generation uint64, partID uint64, result *TypedColumnInt64PredicateScanResult) (bool, error) {
	return scanTypedColumnInt64PredicatePartWithVisibility(part, valueColumn, req, generation, partID, result, nil)
}

func scanTypedColumnInt64PredicatePartWithVisibility(part *typedcolumn.ColumnPart, valueColumn string, req TypedColumnInt64PredicateScanRequest, generation uint64, partID uint64, result *TypedColumnInt64PredicateScanResult, visibility *typedColumnLatestPhysicalPart) (bool, error) {
	if part == nil {
		return false, errors.New("nil typed-column part")
	}
	valueCol, ok := part.Columns[valueColumn]
	if !ok {
		return false, fmt.Errorf("missing value column %q", valueColumn)
	}
	idCol, ok := part.Columns[typedColumnAdapterPrimaryIDColumn]
	if !ok {
		return false, fmt.Errorf("missing primary id column %q", typedColumnAdapterPrimaryIDColumn)
	}
	if valueCol.Definition.Type != typedcolumn.ColumnTypeInt64 || idCol.Definition.Type != typedcolumn.ColumnTypeInt64 {
		return false, fmt.Errorf("value or primary id column is not int64")
	}
	var reader typedcolumn.GranuleReader
	var valueScratch []int64
	var idScratch []int64
	idBlockIndex := 0
	decodedAny := false
	for _, block := range valueCol.Blocks {
		result.Diagnostics.BlocksConsidered++
		g := block.Granule
		if g.HasMinMax && !typedColumnInt64PredicateMayMatch(req, g.Min, g.Max) {
			result.Diagnostics.BlocksPruned++
			continue
		}
		idBlock, ok := typedColumnAlignedBlock(idCol.Blocks, &idBlockIndex, block.Descriptor.FirstRow, block.Descriptor.RowCount)
		if !ok {
			return false, fmt.Errorf("missing aligned primary-id block first_row=%d rows=%d", block.Descriptor.FirstRow, block.Descriptor.RowCount)
		}
		values, err := reader.DecodeInt64Into(valueScratch[:0], g)
		if err != nil {
			return false, err
		}
		valueScratch = values
		ids, err := reader.DecodeInt64Into(idScratch[:0], idBlock.Granule)
		if err != nil {
			return false, err
		}
		idScratch = ids
		if len(values) != block.Descriptor.RowCount || len(ids) != block.Descriptor.RowCount {
			return false, fmt.Errorf("decoded rows value=%d ids=%d want %d", len(values), len(ids), block.Descriptor.RowCount)
		}
		decodedAny = true
		result.Diagnostics.BlocksDecoded++
		result.Diagnostics.FastDecodeScratchDecodes++
		result.Diagnostics.DecodedHeapCopyBytes += uint64(g.RawBytes + idBlock.Granule.RawBytes)
		result.Diagnostics.RowsScanned += len(values)
		for i, v := range values {
			primaryID := ids[i]
			physicalRowIndex, err := typedColumnPhysicalRowIndexFromPrimaryID(primaryID, part.Descriptor.RowCount)
			if err != nil {
				return false, err
			}
			if visibility != nil && !visibility.rowVisible(physicalRowIndex) {
				continue
			}
			if !typedColumnInt64PredicateMatches(req, v) {
				continue
			}
			row := TypedColumnInt64PredicateScanRow{Generation: generation, PartID: partID, RowIndex: physicalRowIndex, PrimaryID: primaryID, Value: v}
			if visibility != nil {
				row.Generation = visibility.Ref.Generation
				row.PartID = visibility.Ref.PartID
				row.DocumentID = visibility.documentID(physicalRowIndex)
			}
			result.Rows = append(result.Rows, row)
			result.Diagnostics.RowsMatched++
		}
	}
	return !decodedAny && len(valueCol.Blocks) != 0, nil
}

func scanTypedColumnInt64PredicateAggregatePart(part *typedcolumn.ColumnPart, valueColumn string, req TypedColumnInt64PredicateScanRequest, result *TypedColumnInt64PredicateAggregateResult) (bool, error) {
	return scanTypedColumnInt64PredicateAggregatePartWithVisibility(part, valueColumn, req, result, nil)
}

func scanTypedColumnInt64PredicateAggregatePartWithVisibility(part *typedcolumn.ColumnPart, valueColumn string, req TypedColumnInt64PredicateScanRequest, result *TypedColumnInt64PredicateAggregateResult, visibility *typedColumnLatestPhysicalPart) (bool, error) {
	return scanTypedColumnInt64PredicateAggregatePartWithVisibilityAndScratch(part, valueColumn, req, result, visibility, nil)
}

type typedColumnInt64PredicateAggregateScanScratch struct {
	// GranuleReader only retains decode/decompression scratch and is safe to
	// reuse across immutable typed-column parts within the session lifetime.
	reader          typedcolumn.GranuleReader
	values          []int64
	predicateRows   []int
	predicateRanges []typedcolumn.RowRange
	predicateBitmap []uint64
	visibilityRows  []int
	selection       typedcolumn.RowSelectionScratch
	boolSelection   typedcolumn.BoolSelectionScratch
	kernel          typedkernel.Scratch
}

func scanTypedColumnInt64PredicateAggregatePartWithVisibilityAndScratch(part *typedcolumn.ColumnPart, valueColumn string, req TypedColumnInt64PredicateScanRequest, result *TypedColumnInt64PredicateAggregateResult, visibility *typedColumnLatestPhysicalPart, scratch *typedColumnInt64PredicateAggregateScanScratch) (bool, error) {
	return scanTypedColumnInt64PredicateAggregatePartWithExpressionAndScratch(part, valueColumn, req, TypedColumnInt64AggregateIdentity, result, visibility, scratch)
}

func scanTypedColumnInt64PredicateAggregatePartWithExpressionAndScratch(part *typedcolumn.ColumnPart, valueColumn string, req TypedColumnInt64PredicateScanRequest, expression TypedColumnInt64AggregateExpression, result *TypedColumnInt64PredicateAggregateResult, visibility *typedColumnLatestPhysicalPart, scratch *typedColumnInt64PredicateAggregateScanScratch) (bool, error) {
	if part == nil {
		return false, errors.New("nil typed-column part")
	}
	valueCol, ok := part.Columns[valueColumn]
	if !ok {
		return false, fmt.Errorf("missing value column %q", valueColumn)
	}
	if valueCol.Definition.Type != typedcolumn.ColumnTypeInt64 {
		return false, fmt.Errorf("value column is not int64")
	}
	if scratch == nil {
		var localScratch typedColumnInt64PredicateAggregateScanScratch
		scratch = &localScratch
	}
	decodedAny := false
	for _, block := range valueCol.Blocks {
		result.Diagnostics.BlocksConsidered++
		g := block.Granule
		if g.HasMinMax && !typedColumnInt64PredicateMayMatch(req, g.Min, g.Max) {
			result.Diagnostics.BlocksPruned++
			selection, err := typedcolumn.NewEmptyRowSelection(block.Descriptor.RowCount)
			if err != nil {
				return false, err
			}
			recordTypedColumnSelectionDiagnostics(&result.Diagnostics, selection)
			continue
		}
		values, err := scratch.reader.DecodeInt64Into(scratch.values[:0], g)
		if err != nil {
			return false, err
		}
		scratch.values = values
		if len(values) != block.Descriptor.RowCount {
			return false, fmt.Errorf("decoded rows value=%d want %d", len(values), block.Descriptor.RowCount)
		}
		decodedAny = true
		result.Diagnostics.BlocksDecoded++
		result.Diagnostics.FastDecodeScratchDecodes++
		result.Diagnostics.DecodedHeapCopyBytes += uint64(g.RawBytes)
		result.Diagnostics.RowsScanned += len(values)

		selection, err := typedColumnInt64PredicateAggregateBlockSelection(req, g, values, scratch)
		if err != nil {
			return false, err
		}
		if visibility != nil && !selection.IsEmpty() {
			visibilitySelection, err := typedColumnInt64VisibilitySelectionForBlock(visibility, block.Descriptor.FirstRow, block.Descriptor.RowCount, scratch)
			if err != nil {
				return false, err
			}
			selection, err = typedcolumn.ComposeRowSelectionsInto(block.Descriptor.RowCount, typedcolumn.RowSelectionComponents{Predicate: &selection, Visibility: &visibilitySelection}, &scratch.selection)
			if err != nil {
				return false, err
			}
			result.Diagnostics.SelectionCompositions++
		}
		recordTypedColumnSelectionDiagnostics(&result.Diagnostics, selection)
		if selection.IsEmpty() {
			continue
		}
		if err := addTypedColumnInt64AggregateSelectedValues(result, values, selection, expression); err != nil {
			return false, err
		}
	}
	return !decodedAny && len(valueCol.Blocks) != 0, nil
}

func typedColumnInt64PredicateAggregateBlockSelection(req TypedColumnInt64PredicateScanRequest, g typedcolumn.EncodedGranule, values []int64, scratch *typedColumnInt64PredicateAggregateScanScratch) (typedcolumn.RowSelection, error) {
	rows := len(values)
	if rows == 0 {
		return typedcolumn.NewEmptyRowSelection(0)
	}
	if typedColumnInt64PredicateCoversGranule(req, g) {
		return typedcolumn.NewAllRowSelection(rows)
	}
	scratch.predicateRows = scratch.predicateRows[:0]
	for i, v := range values {
		if typedColumnInt64PredicateMatches(req, v) {
			scratch.predicateRows = append(scratch.predicateRows, i)
		}
	}
	return typedColumnInt64PredicateRowsSelection(rows, scratch)
}

const typedColumnInt64PredicateSelectionRangeLimit = 8

func typedColumnInt64PredicateRowsSelection(rows int, scratch *typedColumnInt64PredicateAggregateScanScratch) (typedcolumn.RowSelection, error) {
	if scratch == nil {
		return typedcolumn.NewEmptyRowSelection(rows)
	}
	selected := scratch.predicateRows
	if len(selected) == 0 {
		return typedcolumn.NewEmptyRowSelection(rows)
	}
	if len(selected) == rows {
		return typedcolumn.NewAllRowSelection(rows)
	}
	if isContiguousIntRows(selected) {
		return typedcolumn.NewRangeRowSelection(rows, selected[0], selected[len(selected)-1]+1)
	}

	scratch.predicateRanges = scratch.predicateRanges[:0]
	start, prev := selected[0], selected[0]
	for _, row := range selected[1:] {
		if row == prev+1 {
			prev = row
			continue
		}
		scratch.predicateRanges = append(scratch.predicateRanges, typedcolumn.RowRange{Start: start, End: prev + 1})
		start, prev = row, row
	}
	scratch.predicateRanges = append(scratch.predicateRanges, typedcolumn.RowRange{Start: start, End: prev + 1})
	if len(scratch.predicateRanges) <= typedColumnInt64PredicateSelectionRangeLimit {
		return typedcolumn.NewRangesRowSelectionNoCopy(rows, scratch.predicateRanges)
	}
	if len(selected) >= 64 && len(selected)*4 >= rows*3 {
		words := (rows + 63) / 64
		if cap(scratch.predicateBitmap) < words {
			scratch.predicateBitmap = make([]uint64, words)
		} else {
			scratch.predicateBitmap = scratch.predicateBitmap[:words]
			for i := range scratch.predicateBitmap {
				scratch.predicateBitmap[i] = 0
			}
		}
		for _, row := range selected {
			scratch.predicateBitmap[row/64] |= uint64(1) << uint(row%64)
		}
		return typedcolumn.NewBitmapRowSelectionNoCopy(rows, scratch.predicateBitmap)
	}
	return typedcolumn.NewSparseRowSelectionNoCopy(rows, selected)
}

func isContiguousIntRows(rows []int) bool {
	for i := 1; i < len(rows); i++ {
		if rows[i] != rows[i-1]+1 {
			return false
		}
	}
	return len(rows) != 0
}

func typedColumnInt64PredicateCoversGranule(req TypedColumnInt64PredicateScanRequest, g typedcolumn.EncodedGranule) bool {
	switch req.Kind {
	case TypedColumnInt64PredicateAll:
		return true
	case TypedColumnInt64PredicateEqual:
		return g.HasMinMax && g.Min == req.Value && g.Max == req.Value
	case TypedColumnInt64PredicateRange:
		return g.HasMinMax && req.Low <= g.Min && req.High >= g.Max
	default:
		return false
	}
}

func typedColumnInt64VisibilitySelectionForBlock(visibility *typedColumnLatestPhysicalPart, firstRow int, rowCount int, scratch *typedColumnInt64PredicateAggregateScanScratch) (typedcolumn.RowSelection, error) {
	if visibility == nil {
		return typedcolumn.NewAllRowSelection(rowCount)
	}
	scratch.visibilityRows = scratch.visibilityRows[:0]
	for offset := 0; offset < rowCount; offset++ {
		if visibility.rowVisible(firstRow + offset) {
			scratch.visibilityRows = append(scratch.visibilityRows, offset)
		}
	}
	return typedcolumn.NewSparseRowSelectionNoCopy(rowCount, scratch.visibilityRows)
}

func addTypedColumnInt64AggregateSelectedValues(result *TypedColumnInt64PredicateAggregateResult, values []int64, selection typedcolumn.RowSelection, expression TypedColumnInt64AggregateExpression) error {
	if expression == TypedColumnInt64AggregateSecondOfDaySquare {
		return addTypedColumnInt64AggregateSecondOfDaySquareSelectedValues(result, values, selection)
	}
	switch selection.Kind() {
	case typedcolumn.RowSelectionEmpty:
		return nil
	case typedcolumn.RowSelectionAll:
		for _, v := range values {
			if err := addTypedColumnInt64PredicateAggregateExpressionValue(result, expression, v); err != nil {
				return err
			}
			result.Diagnostics.RowsMatched++
		}
		return nil
	case typedcolumn.RowSelectionRange:
		start, end, ok := selection.SingleRange()
		if !ok || start < 0 || end > len(values) {
			return fmt.Errorf("typed-column int64 aggregate invalid range selection [%d,%d) values=%d", start, end, len(values))
		}
		for _, v := range values[start:end] {
			if err := addTypedColumnInt64PredicateAggregateExpressionValue(result, expression, v); err != nil {
				return err
			}
			result.Diagnostics.RowsMatched++
		}
		return nil
	case typedcolumn.RowSelectionRanges:
		for _, r := range selection.Ranges() {
			if r.Start < 0 || r.End < r.Start || r.End > len(values) {
				return fmt.Errorf("typed-column int64 aggregate invalid ranges selection [%d,%d) values=%d", r.Start, r.End, len(values))
			}
			for _, v := range values[r.Start:r.End] {
				if err := addTypedColumnInt64PredicateAggregateExpressionValue(result, expression, v); err != nil {
					return err
				}
				result.Diagnostics.RowsMatched++
			}
		}
		return nil
	case typedcolumn.RowSelectionSparse:
		for _, row := range selection.SparseRows() {
			if row < 0 || row >= len(values) {
				return fmt.Errorf("typed-column int64 aggregate sparse row=%d values=%d", row, len(values))
			}
			if err := addTypedColumnInt64PredicateAggregateExpressionValue(result, expression, values[row]); err != nil {
				return err
			}
			result.Diagnostics.RowsMatched++
		}
		return nil
	case typedcolumn.RowSelectionBitmap:
		for wordIndex, word := range selection.BitmapWords() {
			for word != 0 {
				bit := bits.TrailingZeros64(word)
				row := wordIndex*64 + bit
				if row >= len(values) {
					break
				}
				if err := addTypedColumnInt64PredicateAggregateExpressionValue(result, expression, values[row]); err != nil {
					return err
				}
				result.Diagnostics.RowsMatched++
				word &^= uint64(1) << uint(bit)
			}
		}
		return nil
	default:
		return fmt.Errorf("typed-column int64 aggregate unsupported selection shape %s", selection.Shape().Kind)
	}
}

func addTypedColumnInt64AggregateSecondOfDaySquareSelectedValues(result *TypedColumnInt64PredicateAggregateResult, values []int64, selection typedcolumn.RowSelection) error {
	switch selection.Kind() {
	case typedcolumn.RowSelectionEmpty:
		return nil
	case typedcolumn.RowSelectionAll:
		return addTypedColumnInt64AggregateSecondOfDaySquareValueRange(result, values, 0, len(values))
	case typedcolumn.RowSelectionRange:
		start, end, ok := selection.SingleRange()
		if !ok || start < 0 || end > len(values) {
			return fmt.Errorf("typed-column int64 aggregate invalid range selection [%d,%d) values=%d", start, end, len(values))
		}
		return addTypedColumnInt64AggregateSecondOfDaySquareValueRange(result, values, start, end)
	case typedcolumn.RowSelectionRanges:
		for _, r := range selection.Ranges() {
			if r.Start < 0 || r.End < r.Start || r.End > len(values) {
				return fmt.Errorf("typed-column int64 aggregate invalid ranges selection [%d,%d) values=%d", r.Start, r.End, len(values))
			}
			if err := addTypedColumnInt64AggregateSecondOfDaySquareValueRange(result, values, r.Start, r.End); err != nil {
				return err
			}
		}
		return nil
	case typedcolumn.RowSelectionSparse:
		return addTypedColumnInt64AggregateSecondOfDaySquareSparseValues(result, values, selection.SparseRows())
	case typedcolumn.RowSelectionBitmap:
		return addTypedColumnInt64AggregateSecondOfDaySquareBitmapValues(result, values, selection.BitmapWords())
	default:
		return fmt.Errorf("typed-column int64 aggregate unsupported selection shape %s", selection.Shape().Kind)
	}
}

func addTypedColumnInt64AggregateSecondOfDaySquareValueRange(result *TypedColumnInt64PredicateAggregateResult, values []int64, start int, end int) error {
	if start < 0 || end < start || end > len(values) {
		return fmt.Errorf("typed-column int64 aggregate invalid second-of-day-square range [%d,%d) values=%d", start, end, len(values))
	}
	count := end - start
	if count == 0 {
		return nil
	}
	checkOverflow := typedColumnInt64AggregateSecondOfDaySquareCountCanOverflow(count)
	var sum int64
	for _, value := range values[start:end] {
		if err := addTypedColumnInt64AggregateSecondOfDaySquareLocal(&sum, value, checkOverflow); err != nil {
			return err
		}
	}
	return addTypedColumnInt64AggregateSecondOfDaySquareBatch(result, count, sum)
}

func addTypedColumnInt64AggregateSecondOfDaySquareSparseValues(result *TypedColumnInt64PredicateAggregateResult, values []int64, rows []int) error {
	if len(rows) == 0 {
		return nil
	}
	checkOverflow := typedColumnInt64AggregateSecondOfDaySquareCountCanOverflow(len(rows))
	var sum int64
	for _, row := range rows {
		if row < 0 || row >= len(values) {
			return fmt.Errorf("typed-column int64 aggregate sparse row=%d values=%d", row, len(values))
		}
		if err := addTypedColumnInt64AggregateSecondOfDaySquareLocal(&sum, values[row], checkOverflow); err != nil {
			return err
		}
	}
	return addTypedColumnInt64AggregateSecondOfDaySquareBatch(result, len(rows), sum)
}

func addTypedColumnInt64AggregateSecondOfDaySquareBitmapValues(result *TypedColumnInt64PredicateAggregateResult, values []int64, bitmap []uint64) error {
	count := 0
	for _, word := range bitmap {
		count += bits.OnesCount64(word)
	}
	if count == 0 {
		return nil
	}
	checkOverflow := typedColumnInt64AggregateSecondOfDaySquareCountCanOverflow(count)
	var sum int64
	for wordIndex, word := range bitmap {
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			row := wordIndex*64 + bit
			if row >= len(values) {
				break
			}
			if err := addTypedColumnInt64AggregateSecondOfDaySquareLocal(&sum, values[row], checkOverflow); err != nil {
				return err
			}
			word &^= uint64(1) << uint(bit)
		}
	}
	return addTypedColumnInt64AggregateSecondOfDaySquareBatch(result, count, sum)
}

func addTypedColumnInt64AggregateSecondOfDaySquareBatch(result *TypedColumnInt64PredicateAggregateResult, count int, sum int64) error {
	if count == 0 {
		return nil
	}
	return addTypedColumnInt64AggregateKernelResult(result, typedkernel.AggregateResult{NonNulls: int64(count), Sum: sum, HasValue: true})
}

func typedColumnInt64AggregateSecondOfDaySquareCountCanOverflow(count int) bool {
	return int64(count) > typedColumnInt64PredicateAggregateMaxSum/typedColumnInt64AggregateSecondOfDaySquareMaxValue
}

func addTypedColumnInt64AggregateSecondOfDaySquareLocal(sum *int64, value int64, checkOverflow bool) error {
	transformed := typedColumnInt64AggregateSecondOfDaySquareValue(value)
	if checkOverflow && transformed > 0 && *sum > typedColumnInt64PredicateAggregateMaxSum-transformed {
		return fmt.Errorf("collections: typed-column int64 predicate aggregate sum overflow current=%d value=%d", *sum, transformed)
	}
	*sum += transformed
	return nil
}

func recordTypedColumnSelectionDiagnostics(diag *TypedColumnInt64PredicateScanDiagnostics, selection typedcolumn.RowSelection) {
	if diag == nil {
		return
	}
	switch selection.Kind() {
	case typedcolumn.RowSelectionEmpty:
		diag.SelectionEmptyBlocks++
	case typedcolumn.RowSelectionAll:
		diag.SelectionAllBlocks++
	case typedcolumn.RowSelectionRange:
		diag.SelectionRangeBlocks++
	case typedcolumn.RowSelectionRanges:
		diag.SelectionRangesBlocks++
	case typedcolumn.RowSelectionBitmap:
		diag.SelectionBitmapBlocks++
	case typedcolumn.RowSelectionSparse:
		diag.SelectionSparseBlocks++
	}
}

type typedColumnStringPredicateScanScratch struct {
	reader         typedcolumn.GranuleReader
	codes          []uint32
	ids            []int64
	visibilityRows []int
	selection      typedcolumn.RowSelectionScratch
	kernel         typedkernel.Scratch
}

func typedColumnStringPreparedColumnRequests(adapterColumn typedColumnAdapterColumn, op columnsemantics.Operation) []typedColumnPreparedColumnRequest {
	return []typedColumnPreparedColumnRequest{{Field: adapterColumn.Field, Role: typedcolumn.ColumnRolePredicate, Operation: op, IncludeDictionaries: true}}
}

func typedColumnPreparedPartDictionaryBytes(preparedPart *typedColumnPreparedPartState) int {
	if preparedPart == nil {
		return 0
	}
	return preparedPart.Image.CategoryBytes(typedcolumn.ColumnPartImageCategoryDictionaries)
}

func scanTypedColumnStringPreparedPartWithVisibility(preparedPart *typedColumnPreparedPartState, valueColumn string, codes []uint32, valueByCode map[uint32]string, generation uint64, partID uint64, result *TypedColumnStringPredicateScanResult, visibility *typedColumnLatestPhysicalPart, readRange typedColumnPreparedRangeReader, scratch *typedColumnStringPredicateScanScratch) (bool, error) {
	if preparedPart == nil {
		return false, errors.New("nil typed-column prepared part")
	}
	preparedColumn := preparedPart.Columns[valueColumn]
	if preparedColumn == nil {
		return false, fmt.Errorf("missing prepared value column %q", valueColumn)
	}
	idCol, ok := preparedPart.PhysicalColumns[typedColumnAdapterPrimaryIDColumn]
	if !ok {
		return false, fmt.Errorf("missing primary id column %q", typedColumnAdapterPrimaryIDColumn)
	}
	idSection, ok := typedColumnAdapterColumnDataSection(preparedPart.Image, typedColumnAdapterPrimaryIDColumn)
	if !ok {
		return false, fmt.Errorf("missing primary id section %q", typedColumnAdapterPrimaryIDColumn)
	}
	if idCol.Definition.Type != typedcolumn.ColumnTypeInt64 {
		return false, fmt.Errorf("primary id column %q type=%s", typedColumnAdapterPrimaryIDColumn, idCol.Definition.Type)
	}
	switch idCol.Definition.Encoding {
	case typedcolumn.EncodingRawInt64, typedcolumn.EncodingDeltaVarint, typedcolumn.EncodingDoubleDeltaVarint:
	default:
		return false, fmt.Errorf("primary id column %q unsupported encoding=%s", typedColumnAdapterPrimaryIDColumn, idCol.Definition.Encoding)
	}
	cardinality := preparedColumn.Column.Definition.Cardinality
	if cardinality == 0 {
		return false, fmt.Errorf("typed-column string predicate column %q has zero cardinality", valueColumn)
	}
	if scratch == nil {
		scratch = &typedColumnStringPredicateScanScratch{}
	}
	idBlockIndex := 0
	decodedAny := false
	for blockIdx := range preparedColumn.BlockPlans {
		block := &preparedColumn.BlockPlans[blockIdx]
		result.Diagnostics.BlocksConsidered++
		if block.CandidateSelection.IsEmpty() {
			result.Diagnostics.BlocksPruned++
			recordTypedColumnSelectionDiagnostics(&result.Diagnostics, block.CandidateSelection)
			continue
		}
		g := block.Granule
		if g.HasMinMax {
			if g.Min < 0 || g.Max < 0 || g.Min > g.Max || uint64(g.Max) >= uint64(cardinality) {
				return false, fmt.Errorf("value column %q block first_row=%d min/max [%d,%d] outside cardinality %d", valueColumn, block.Descriptor.FirstRow, g.Min, g.Max, cardinality)
			}
			if !typedColumnStringCodesIntersectMinMax(codes, uint32(g.Min), uint32(g.Max)) {
				result.Diagnostics.BlocksPruned++
				continue
			}
		}
		payload, err := readRange(block.PayloadOffset, block.PayloadLength, false)
		if err != nil {
			return false, fmt.Errorf("read column %q block %d payload: %w", valueColumn, block.Index, err)
		}
		if len(payload) != block.PayloadLength {
			return false, fmt.Errorf("typed-column string predicate column %q block %d payload bytes=%d want %d", valueColumn, block.Index, len(payload), block.PayloadLength)
		}
		granule := g
		granule.Payload = payload
		granule.PayloadRef = typedcolumn.PayloadRef{Kind: typedcolumn.PayloadRefInline, Length: block.PayloadLength}
		if err := typedColumnPreparedGranuleLayout(preparedColumn.Plan, granule).ValidateGranulePayload(granule, payload); err != nil {
			return false, err
		}
		selection := block.CandidateSelection
		if len(codes) == 1 {
			selection, err = typedkernel.SelectDictionaryCode(typedkernel.DictionaryPredicateRequest{Rows: block.Descriptor.RowCount, Selection: selection, Granule: granule, HasGranule: true, Reader: &scratch.reader, Code: codes[0]}, &scratch.kernel)
		} else {
			selection, err = typedkernel.SelectDictionaryCodesIn(typedkernel.DictionaryPredicateRequest{Rows: block.Descriptor.RowCount, Selection: selection, Granule: granule, HasGranule: true, Reader: &scratch.reader, Codes: codes}, &scratch.kernel)
		}
		if err != nil {
			return false, err
		}
		result.Diagnostics.KernelBlocks++
		result.Diagnostics.KernelSelectedBlocks++
		if selection.IsEmpty() {
			recordTypedColumnSelectionDiagnostics(&result.Diagnostics, selection)
			continue
		}
		idBlock, ok := typedColumnAlignedBlock(idCol.Blocks, &idBlockIndex, block.Descriptor.FirstRow, block.Descriptor.RowCount)
		if !ok {
			return false, fmt.Errorf("missing aligned primary-id block first_row=%d rows=%d", block.Descriptor.FirstRow, block.Descriptor.RowCount)
		}
		idPayloadOffset := idSection.Offset
		for i := 0; i < idBlockIndex; i++ {
			idPayloadOffset += idCol.Blocks[i].Descriptor.StoredBytes
		}
		idPayload, err := readRange(idPayloadOffset, idBlock.Descriptor.StoredBytes, false)
		if err != nil {
			return false, fmt.Errorf("read primary-id block first_row=%d payload: %w", idBlock.Descriptor.FirstRow, err)
		}
		idGranule := idBlock.Granule
		idGranule.Payload = idPayload
		idGranule.PayloadRef = typedcolumn.PayloadRef{Kind: typedcolumn.PayloadRefInline, Length: idBlock.Descriptor.StoredBytes}
		ids, err := scratch.reader.DecodeInt64Into(scratch.ids[:0], idGranule)
		if err != nil {
			return false, err
		}
		scratch.ids = ids
		if len(ids) != block.Descriptor.RowCount {
			return false, fmt.Errorf("decoded primary ids=%d want %d", len(ids), block.Descriptor.RowCount)
		}
		if visibility != nil {
			visibilitySelection, err := typedColumnStringVisibilitySelectionForPrimaryIDs(visibility, ids, preparedPart.Descriptor.RowCount, scratch)
			if err != nil {
				return false, err
			}
			selection, err = typedcolumn.ComposeRowSelectionsInto(block.Descriptor.RowCount, typedcolumn.RowSelectionComponents{Predicate: &selection, Visibility: &visibilitySelection}, &scratch.selection)
			if err != nil {
				return false, err
			}
			result.Diagnostics.SelectionCompositions++
		}
		recordTypedColumnSelectionDiagnostics(&result.Diagnostics, selection)
		if selection.IsEmpty() {
			continue
		}
		var codesForRows []uint32
		if len(codes) != 1 {
			codesForRows, err = scratch.reader.DecodeUint32CodesInto(scratch.codes[:0], granule)
			if err != nil {
				return false, err
			}
			scratch.codes = codesForRows
			if len(codesForRows) != block.Descriptor.RowCount {
				return false, fmt.Errorf("decoded codes=%d want %d", len(codesForRows), block.Descriptor.RowCount)
			}
		}
		decodedAny = true
		result.Diagnostics.BlocksDecoded++
		result.Diagnostics.DecodedHeapCopyBytes += uint64(block.PayloadLength + idBlock.Descriptor.StoredBytes)
		result.Diagnostics.RowsScanned += block.Descriptor.RowCount
		var forEachErr error
		selection.ForEach(func(row int) {
			if forEachErr != nil {
				return
			}
			if row < 0 || row >= len(ids) {
				forEachErr = fmt.Errorf("typed-column string predicate selection row=%d outside ids=%d first_row=%d rows=%d", row, len(ids), block.Descriptor.FirstRow, block.Descriptor.RowCount)
				return
			}
			matchedValue := valueByCode[codes[0]]
			if len(codes) != 1 {
				matchedValue = valueByCode[codesForRows[row]]
			}
			primaryID := ids[row]
			physicalRowIndex, err := typedColumnPhysicalRowIndexFromPrimaryID(primaryID, preparedPart.Descriptor.RowCount)
			if err != nil {
				forEachErr = err
				return
			}
			out := TypedColumnStringPredicateScanRow{Generation: generation, PartID: partID, RowIndex: physicalRowIndex, PrimaryID: primaryID, Value: matchedValue}
			if visibility != nil {
				out.Generation = visibility.Ref.Generation
				out.PartID = visibility.Ref.PartID
				out.DocumentID = visibility.documentID(physicalRowIndex)
			}
			result.Rows = append(result.Rows, out)
			result.Diagnostics.RowsMatched++
			result.Diagnostics.CodesMatched++
		})
		if forEachErr != nil {
			return false, forEachErr
		}
	}
	return !decodedAny && len(preparedColumn.BlockPlans) != 0, nil
}

func typedColumnStringCodesIntersectMinMax(codes []uint32, minCode uint32, maxCode uint32) bool {
	for _, code := range codes {
		if code >= minCode && code <= maxCode {
			return true
		}
	}
	return false
}

func typedColumnStringVisibilitySelectionForPrimaryIDs(visibility *typedColumnLatestPhysicalPart, ids []int64, physicalRows int, scratch *typedColumnStringPredicateScanScratch) (typedcolumn.RowSelection, error) {
	if visibility == nil {
		return typedcolumn.NewAllRowSelection(len(ids))
	}
	scratch.visibilityRows = scratch.visibilityRows[:0]
	for offset, primaryID := range ids {
		physicalRowIndex, err := typedColumnPhysicalRowIndexFromPrimaryID(primaryID, physicalRows)
		if err != nil {
			return typedcolumn.RowSelection{}, err
		}
		if visibility.rowVisible(physicalRowIndex) {
			scratch.visibilityRows = append(scratch.visibilityRows, offset)
		}
	}
	return typedcolumn.NewSparseRowSelectionNoCopy(len(ids), scratch.visibilityRows)
}

func typedColumnStringResolvePreparedCodes(column *typedColumnPreparedColumnState, values []string) ([]uint32, map[uint32]string, bool, error) {
	if column == nil {
		return nil, nil, false, errors.New("collections: typed-column string prepared state missing column")
	}
	if column.Column.Definition.Type != typedcolumn.ColumnTypeLowCardinalityCode || column.Column.Definition.Encoding != typedcolumn.EncodingLowCardinalityUint32 {
		return nil, nil, false, fmt.Errorf("collections: typed-column string prepared column %q type=%s encoding=%s", column.Plan.Definition.Name, column.Column.Definition.Type, column.Column.Definition.Encoding)
	}
	if len(values) == 0 {
		return nil, nil, false, nil
	}
	valueByCode := make(map[uint32]string, len(values))
	seenValues := make(map[string]struct{}, len(values))
	codes := make([]uint32, 0, len(values))
	for _, value := range values {
		if _, dup := seenValues[value]; dup {
			continue
		}
		seenValues[value] = struct{}{}
		code, ok := column.Dictionaries[value]
		if !ok {
			continue
		}
		if code < 0 || uint64(code) >= uint64(column.Column.Definition.Cardinality) || uint64(code) > uint64(^uint32(0)) {
			return nil, nil, false, fmt.Errorf("collections: typed-column string dictionary code %d for column %q outside cardinality %d", code, column.Plan.Definition.Name, column.Column.Definition.Cardinality)
		}
		u32 := uint32(code)
		if _, exists := valueByCode[u32]; exists {
			continue
		}
		valueByCode[u32] = value
		codes = append(codes, u32)
	}
	return codes, valueByCode, len(codes) != 0, nil
}

func scanTypedColumnStringPredicatePartWithVisibility(part *typedcolumn.ColumnPart, valueColumn string, code uint32, value string, generation uint64, partID uint64, result *TypedColumnStringPredicateScanResult, visibility *typedColumnLatestPhysicalPart, scratch *typedColumnStringPredicateScanScratch) (bool, error) {
	if part == nil {
		return false, errors.New("nil typed-column part")
	}
	valueCol, ok := part.Columns[valueColumn]
	if !ok {
		return false, fmt.Errorf("missing value column %q", valueColumn)
	}
	idCol, ok := part.Columns[typedColumnAdapterPrimaryIDColumn]
	if !ok {
		return false, fmt.Errorf("missing primary id column %q", typedColumnAdapterPrimaryIDColumn)
	}
	if valueCol.Definition.Type != typedcolumn.ColumnTypeLowCardinalityCode || valueCol.Definition.Encoding != typedcolumn.EncodingLowCardinalityUint32 || idCol.Definition.Type != typedcolumn.ColumnTypeInt64 {
		return false, fmt.Errorf("value column is not low-cardinality uint32 or primary id column is not int64")
	}
	cardinality := valueCol.Definition.Cardinality
	if cardinality == 0 || code >= cardinality {
		return false, fmt.Errorf("typed-column string predicate query code %d outside cardinality %d", code, cardinality)
	}
	if scratch == nil {
		scratch = &typedColumnStringPredicateScanScratch{}
	}
	idBlockIndex := 0
	decodedAny := false
	for _, block := range valueCol.Blocks {
		result.Diagnostics.BlocksConsidered++
		g := block.Granule
		if g.HasMinMax {
			if g.Min < 0 || g.Max < 0 || g.Min > g.Max || uint64(g.Max) >= uint64(cardinality) {
				return false, fmt.Errorf("value column %q block first_row=%d min/max [%d,%d] outside cardinality %d", valueColumn, block.Descriptor.FirstRow, g.Min, g.Max, cardinality)
			}
			if int64(code) < g.Min || int64(code) > g.Max {
				result.Diagnostics.BlocksPruned++
				continue
			}
		}
		idBlock, ok := typedColumnAlignedBlock(idCol.Blocks, &idBlockIndex, block.Descriptor.FirstRow, block.Descriptor.RowCount)
		if !ok {
			return false, fmt.Errorf("missing aligned primary-id block first_row=%d rows=%d", block.Descriptor.FirstRow, block.Descriptor.RowCount)
		}
		codes, err := scratch.reader.DecodeUint32CodesInto(scratch.codes[:0], g)
		if err != nil {
			return false, err
		}
		scratch.codes = codes
		ids, err := scratch.reader.DecodeInt64Into(scratch.ids[:0], idBlock.Granule)
		if err != nil {
			return false, err
		}
		scratch.ids = ids
		if len(codes) != block.Descriptor.RowCount || len(ids) != block.Descriptor.RowCount {
			return false, fmt.Errorf("decoded rows codes=%d ids=%d want %d", len(codes), len(ids), block.Descriptor.RowCount)
		}
		decodedAny = true
		result.Diagnostics.BlocksDecoded++
		result.Diagnostics.DecodedHeapCopyBytes += uint64(g.RawBytes + idBlock.Granule.RawBytes)
		result.Diagnostics.RowsScanned += len(codes)
		for i, got := range codes {
			if got >= cardinality {
				return false, fmt.Errorf("typed-column string predicate code %d outside cardinality %d", got, cardinality)
			}
			rowIndex := block.Descriptor.FirstRow + i
			if visibility != nil && !visibility.rowVisible(rowIndex) {
				continue
			}
			if got != code {
				continue
			}
			row := TypedColumnStringPredicateScanRow{Generation: generation, PartID: partID, RowIndex: rowIndex, PrimaryID: ids[i], Value: value}
			if visibility != nil {
				row.Generation = visibility.Ref.Generation
				row.PartID = visibility.Ref.PartID
				row.DocumentID = visibility.documentID(rowIndex)
			}
			result.Rows = append(result.Rows, row)
			result.Diagnostics.RowsMatched++
			result.Diagnostics.CodesMatched++
		}
	}
	return !decodedAny && len(valueCol.Blocks) != 0, nil
}

func typedColumnAlignedBlock(blocks []typedcolumn.ColumnBlock, cursor *int, firstRow, rowCount int) (typedcolumn.ColumnBlock, bool) {
	idx := 0
	if cursor != nil {
		idx = *cursor
	}
	for idx < len(blocks) && blocks[idx].Descriptor.FirstRow < firstRow {
		idx++
	}
	if cursor != nil {
		*cursor = idx
	}
	if idx >= len(blocks) {
		return typedcolumn.ColumnBlock{}, false
	}
	block := blocks[idx]
	if block.Descriptor.FirstRow != firstRow || block.Descriptor.RowCount != rowCount {
		return typedcolumn.ColumnBlock{}, false
	}
	return block, true
}

func typedColumnAdapterInt64View(mgr *mappedresource.Manager, h *mappedresource.Handle) ([]int64, error) {
	return mgr.Int64View(h)
}

func typedColumnAdapterFloat32View(mgr *mappedresource.Manager, h *mappedresource.Handle) ([]float32, error) {
	return mgr.Float32View(h)
}

func typedColumnAdapterFloat64View(mgr *mappedresource.Manager, h *mappedresource.Handle) ([]float64, error) {
	return mgr.Float64View(h)
}

func typedColumnAdapterUint32View(mgr *mappedresource.Manager, h *mappedresource.Handle) ([]uint32, error) {
	return mgr.Uint32View(h)
}
