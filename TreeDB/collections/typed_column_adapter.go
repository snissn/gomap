package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"slices"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
	"github.com/snissn/gomap/TreeDB/internal/typedkernel"
)

var errTypedColumnAdapterUnsupportedType = errors.New("collections: typed-column adapter unsupported type")

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
	Field              TypedStorageField
	Definition         typedcolumn.ColumnDefinition
	Dictionary         map[string]int64
	ReverseDictionary  map[int64]string
	FixedWidthEncoding ColumnFixedWidthEncoding
}

type typedColumnAdapterOptions struct {
	Collection     string
	Namespace      string
	SchemaVersion  uint32
	PartID         uint64
	RowsPerGranule int
	Fields         []TypedStorageField
	SortKey        []ColumnSortKey
}

type typedColumnAdapterRow struct {
	PrimaryID int64
	Values    map[string]columnDeclaredValue
}

type typedColumnAdapterPart struct {
	Options    typedColumnAdapterOptions
	Columns    []typedColumnAdapterColumn
	Part       *typedcolumn.ColumnPart
	Dictionary map[string]map[string]int64
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
		{ValueType: ColumnStoreValueFloat32Vector, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeFloat32Vector, Encoding: typedcolumn.EncodingRawFloat32Vector, Reason: "stored as fixed-dim dense little-endian float32 sections"},
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
	mapping, err := typedColumnAdapterMappingForValueType(field.ValueType)
	if err != nil {
		return typedColumnAdapterColumn{}, err
	}
	if field.FixedWidthEncoding != "" {
		switch field.ValueType {
		case ColumnStoreValueInt64:
			if field.FixedWidthEncoding != ColumnFixedWidthEncodingLittleEndian {
				return typedColumnAdapterColumn{}, fmt.Errorf("%w: unsupported int64 fixed_width_encoding=%q", errTypedColumnAdapterUnsupportedType, field.FixedWidthEncoding)
			}
			if field.Nullable {
				return typedColumnAdapterColumn{}, fmt.Errorf("%w: nullable int64 raw fixed-width encoding is unsupported", errTypedColumnAdapterUnsupportedType)
			}
			mapping.Encoding = typedcolumn.EncodingRawInt64
		case ColumnStoreValueFloat32:
			if field.FixedWidthEncoding != ColumnFixedWidthEncodingLittleEndian {
				return typedColumnAdapterColumn{}, fmt.Errorf("%w: unsupported float32 fixed_width_encoding=%q", errTypedColumnAdapterUnsupportedType, field.FixedWidthEncoding)
			}
			if field.Nullable {
				return typedColumnAdapterColumn{}, fmt.Errorf("%w: nullable float32 raw fixed-width encoding is unsupported", errTypedColumnAdapterUnsupportedType)
			}
			mapping.ColumnType = typedcolumn.ColumnTypeFloat32
			mapping.Encoding = typedcolumn.EncodingRawFloat32
		case ColumnStoreValueDouble:
			if field.FixedWidthEncoding != ColumnFixedWidthEncodingLittleEndian {
				return typedColumnAdapterColumn{}, fmt.Errorf("%w: unsupported double fixed_width_encoding=%q", errTypedColumnAdapterUnsupportedType, field.FixedWidthEncoding)
			}
			if field.Nullable {
				return typedColumnAdapterColumn{}, fmt.Errorf("%w: nullable double raw fixed-width encoding is unsupported", errTypedColumnAdapterUnsupportedType)
			}
			mapping.ColumnType = typedcolumn.ColumnTypeFloat64
			mapping.Encoding = typedcolumn.EncodingRawFloat64
		case ColumnStoreValueFloat32Vector, ColumnStoreValueAdjacencyList:
			if field.FixedWidthEncoding != ColumnFixedWidthEncodingLittleEndian {
				return typedColumnAdapterColumn{}, fmt.Errorf("%w: unsupported %s fixed_width_encoding=%q", errTypedColumnAdapterUnsupportedType, field.ValueType, field.FixedWidthEncoding)
			}
		default:
			return typedColumnAdapterColumn{}, fmt.Errorf("%w: fixed_width_encoding is unsupported for value_type=%s", errTypedColumnAdapterUnsupportedType, field.ValueType)
		}
	}
	if field.Nullable {
		switch field.ValueType {
		case ColumnStoreValueBool, ColumnStoreValueInt64, ColumnStoreValueFloat32, ColumnStoreValueDouble, ColumnStoreValueString:
			mapping.Encoding = typedcolumn.EncodingNullableInt64
		case ColumnStoreValueFloat32Vector, ColumnStoreValueUint32List, ColumnStoreValueBytes, ColumnStoreValueAdjacencyList:
			return typedColumnAdapterColumn{}, fmt.Errorf("%w: nullable %s typed-column fields are not supported", errTypedColumnAdapterUnsupportedType, field.ValueType)
		default:
			return typedColumnAdapterColumn{}, fmt.Errorf("%w: nullable %s", errTypedColumnAdapterUnsupportedType, field.ValueType)
		}
	}
	name := field.Name
	if name == "" {
		name = field.Path
	}
	if name == "" {
		return typedColumnAdapterColumn{}, errors.New("collections: typed-column adapter field requires name or path")
	}
	if name == typedColumnAdapterPrimaryIDColumn || field.Path == typedColumnAdapterPrimaryIDColumn {
		return typedColumnAdapterColumn{}, fmt.Errorf("collections: typed-column adapter field %q uses reserved primary-id column %q", field.Path, typedColumnAdapterPrimaryIDColumn)
	}
	if name == typedColumnAdapterMetadataDictionary || field.Path == typedColumnAdapterMetadataDictionary {
		return typedColumnAdapterColumn{}, fmt.Errorf("collections: typed-column adapter field %q uses reserved metadata dictionary %q", field.Path, typedColumnAdapterMetadataDictionary)
	}
	def := typedcolumn.ColumnDefinition{
		Name:           name,
		Type:           mapping.ColumnType,
		Encoding:       mapping.Encoding,
		Compression:    typedcolumn.CompressionNone,
		CompressionSet: true,
		StatsDisabled:  field.ValueType != ColumnStoreValueInt64,
	}
	switch field.ValueType {
	case ColumnStoreValueFloat32Vector:
		if field.VectorDims <= 0 {
			return typedColumnAdapterColumn{}, fmt.Errorf("collections: typed-column adapter field %q float32_vector requires positive vector_dims", field.Path)
		}
		def.FixedWidthElements = field.VectorDims
	case ColumnStoreValueUint32List:
		if field.AdjacencyDegree != 0 {
			return typedColumnAdapterColumn{}, fmt.Errorf("%w: uint32_list adjacency_degree=%d must be zero", errTypedColumnAdapterUnsupportedType, field.AdjacencyDegree)
		}
		if field.AdjacencyLayout != "" {
			return typedColumnAdapterColumn{}, fmt.Errorf("%w: uint32_list must not set adjacency_layout %q", errTypedColumnAdapterUnsupportedType, field.AdjacencyLayout)
		}
		if field.FixedWidthEncoding != ColumnFixedWidthEncodingDefault {
			return typedColumnAdapterColumn{}, fmt.Errorf("%w: uint32_list fixed_width_encoding is unsupported", errTypedColumnAdapterUnsupportedType)
		}
		def.Encoding = typedcolumn.EncodingRawUint32OffsetsList
		def.FixedWidthElements = 0
	case ColumnStoreValueBytes:
		if field.AdjacencyDegree != 0 {
			return typedColumnAdapterColumn{}, fmt.Errorf("%w: bytes adjacency_degree=%d must be zero", errTypedColumnAdapterUnsupportedType, field.AdjacencyDegree)
		}
		if field.AdjacencyLayout != "" {
			return typedColumnAdapterColumn{}, fmt.Errorf("%w: bytes must not set adjacency_layout %q", errTypedColumnAdapterUnsupportedType, field.AdjacencyLayout)
		}
		if field.FixedWidthEncoding != ColumnFixedWidthEncodingDefault {
			return typedColumnAdapterColumn{}, fmt.Errorf("%w: bytes fixed_width_encoding is unsupported", errTypedColumnAdapterUnsupportedType)
		}
		def.Encoding = typedcolumn.EncodingRawBytesOffsets
		def.FixedWidthElements = 0
	case ColumnStoreValueAdjacencyList:
		switch field.AdjacencyLayout {
		case ColumnAdjacencyListLayoutUint32OffsetsList:
			if field.AdjacencyDegree != 0 {
				return typedColumnAdapterColumn{}, fmt.Errorf("%w: adjacency_list adjacency_degree=%d must be zero for adjacency_layout %q", errTypedColumnAdapterUnsupportedType, field.AdjacencyDegree, field.AdjacencyLayout)
			}
			if field.FixedWidthEncoding != ColumnFixedWidthEncodingDefault {
				return typedColumnAdapterColumn{}, fmt.Errorf("%w: adjacency_list fixed_width_encoding is unsupported for adjacency_layout %q", errTypedColumnAdapterUnsupportedType, field.AdjacencyLayout)
			}
			def.Encoding = typedcolumn.EncodingRawUint32OffsetsList
			def.FixedWidthElements = 0
		case ColumnAdjacencyListLayoutFixedDense:
			if field.AdjacencyDegree <= 0 {
				return typedColumnAdapterColumn{}, fmt.Errorf("collections: typed-column adapter field %q adjacency_list requires positive adjacency_degree", field.Path)
			}
			def.FixedWidthElements = field.AdjacencyDegree
		default:
			return typedColumnAdapterColumn{}, fmt.Errorf("%w: unsupported adjacency_list layout %q", errTypedColumnAdapterUnsupportedType, field.AdjacencyLayout)
		}
	}
	return typedColumnAdapterColumn{Field: field, Definition: def, FixedWidthEncoding: field.FixedWidthEncoding}, nil
}

func typedColumnAdapterColumnsForFields(fields []TypedStorageField) ([]typedColumnAdapterColumn, error) {
	columns := make([]typedColumnAdapterColumn, 0, len(fields))
	seenColumns := map[string]struct{}{typedColumnAdapterPrimaryIDColumn: {}}
	seenNames := make(map[string]string, len(fields))
	seenPaths := make(map[string]string, len(fields))
	for _, field := range fields {
		column, err := typedColumnAdapterMapField(field)
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
	if opts.PartID == 0 {
		opts.PartID = 1
	}
	columns, err := typedColumnAdapterColumnsForFields(opts.Fields)
	if err != nil {
		return nil, err
	}
	for i := range columns {
		if columns[i].Field.ValueType == ColumnStoreValueString {
			dict, err := buildTypedColumnAdapterStringDictionary(columns[i], rows)
			if err != nil {
				return nil, err
			}
			columns[i].Dictionary = dict
			columns[i].ReverseDictionary = reverseTypedColumnAdapterDictionary(dict)
			columns[i].Definition.Cardinality = uint32(len(dict))
		}
	}

	defs := make([]typedcolumn.ColumnDefinition, 0, len(columns)+1)
	defs = append(defs, typedcolumn.ColumnDefinition{
		Name:           typedColumnAdapterPrimaryIDColumn,
		Type:           typedcolumn.ColumnTypeInt64,
		Encoding:       typedcolumn.EncodingRawInt64,
		Compression:    typedcolumn.CompressionNone,
		CompressionSet: true,
		StatsDisabled:  true,
	})
	for _, column := range columns {
		defs = append(defs, column.Definition)
	}
	batch := typedcolumn.Batch{Rows: len(rows), Columns: make(map[string][]int64, len(defs)), Nulls: make(map[string][]bool), Defaults: make(map[string][]bool)}
	batch.Columns[typedColumnAdapterPrimaryIDColumn] = make([]int64, len(rows))
	for _, column := range columns {
		switch column.Definition.Type {
		case typedcolumn.ColumnTypeFloat32:
			if batch.Float32Columns == nil {
				batch.Float32Columns = make(map[string][]float32)
			}
			batch.Float32Columns[column.Definition.Name] = make([]float32, len(rows))
		case typedcolumn.ColumnTypeFloat64:
			if batch.Float64Columns == nil {
				batch.Float64Columns = make(map[string][]float64)
			}
			batch.Float64Columns[column.Definition.Name] = make([]float64, len(rows))
		case typedcolumn.ColumnTypeFloat32Vector:
			if batch.Float32Vectors == nil {
				batch.Float32Vectors = make(map[string][]float32)
			}
			elements, err := typedColumnAdapterDenseElements(len(rows), column.Definition.FixedWidthElements)
			if err != nil {
				return nil, err
			}
			batch.Float32Vectors[column.Definition.Name] = make([]float32, elements)
		case typedcolumn.ColumnTypeUint32List:
			if batch.Uint32OffsetsLists == nil {
				batch.Uint32OffsetsLists = make(map[string]typedcolumn.RawUint32OffsetsList)
			}
			batch.Uint32OffsetsLists[column.Definition.Name] = typedcolumn.Uint32List{Rows: len(rows), Offsets: make([]uint64, len(rows)+1)}
		case typedcolumn.ColumnTypeBytes:
			if batch.BytesColumns == nil {
				batch.BytesColumns = make(map[string]typedcolumn.RawBytesOffsets)
			}
			batch.BytesColumns[column.Definition.Name] = typedcolumn.BytesColumn{Rows: len(rows), Offsets: make([]uint64, len(rows)+1)}
		case typedcolumn.ColumnTypeAdjacencyList:
			if column.Definition.Encoding == typedcolumn.EncodingRawUint32OffsetsList {
				if batch.Uint32OffsetsLists == nil {
					batch.Uint32OffsetsLists = make(map[string]typedcolumn.RawUint32OffsetsList)
				}
				batch.Uint32OffsetsLists[column.Definition.Name] = typedcolumn.RawUint32OffsetsList{Rows: len(rows), Offsets: make([]uint64, len(rows)+1)}
			} else {
				if batch.Uint32Vectors == nil {
					batch.Uint32Vectors = make(map[string][]uint32)
				}
				elements, err := typedColumnAdapterDenseElements(len(rows), column.Definition.FixedWidthElements)
				if err != nil {
					return nil, err
				}
				batch.Uint32Vectors[column.Definition.Name] = make([]uint32, elements)
			}
		default:
			batch.Columns[column.Definition.Name] = make([]int64, len(rows))
			if column.Field.Nullable {
				batch.Nulls[column.Definition.Name] = make([]bool, len(rows))
				batch.Defaults[column.Definition.Name] = make([]bool, len(rows))
			}
		}
	}
	for rowIdx, row := range rows {
		batch.Columns[typedColumnAdapterPrimaryIDColumn][rowIdx] = row.PrimaryID
		for _, column := range columns {
			value, ok, err := typedColumnAdapterRowValue(row, column)
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
				encoded, isNull, isDefault, err := encodeTypedColumnAdapterScalarValue(column, value)
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
		PartPolicy:  typedcolumn.ColumnPartPolicy{RowsPerGranule: rowsPerGranule},
		Compression: typedcolumn.ColumnCompressionPolicy{Default: typedcolumn.CompressionNone},
	}
	part, err := typedcolumn.BuildColumnPart(opts.PartID, partOpts, batch)
	if err != nil {
		return nil, err
	}
	return &typedColumnAdapterPart{Options: opts, Columns: columns, Part: part, Dictionary: typedColumnAdapterDictionaries(columns)}, nil
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
	part, err := typedcolumn.ColumnPartFromImageWithOptions(image, typedcolumn.ColumnPartImageReadOptions{
		IncludeRowLocators:       false,
		ValidateRowLocators:      false,
		IncludeAggregateMetadata: false,
	})
	if err != nil {
		return nil, err
	}
	return typedColumnAdapterPartFromDecodedImage(opts, image, part)
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
	if primary.Definition.Type != typedcolumn.ColumnTypeInt64 || primary.Definition.Encoding != typedcolumn.EncodingRawInt64 || primary.Definition.Compression != typedcolumn.CompressionNone {
		return fmt.Errorf("collections: typed-column int64 aggregate image primary-id column %q type=%s encoding=%s compression=%s want type=%s encoding=%s compression=%s", typedColumnAdapterPrimaryIDColumn, primary.Definition.Type, primary.Definition.Encoding, primary.Definition.Compression, typedcolumn.ColumnTypeInt64, typedcolumn.EncodingRawInt64, typedcolumn.CompressionNone)
	}
	got, ok := part.Columns[adapterColumn.Definition.Name]
	if !ok {
		return fmt.Errorf("collections: typed-column int64 aggregate image missing column %q", adapterColumn.Definition.Name)
	}
	if got.Definition.Type != adapterColumn.Definition.Type || got.Definition.Encoding != adapterColumn.Definition.Encoding || got.Definition.Compression != adapterColumn.Definition.Compression || got.Definition.FixedWidthElements != adapterColumn.Definition.FixedWidthElements {
		return fmt.Errorf("collections: typed-column int64 aggregate image column %q schema mismatch: got type=%s encoding=%s compression=%s fixed_width_elements=%d want type=%s encoding=%s compression=%s fixed_width_elements=%d", adapterColumn.Definition.Name, got.Definition.Type, got.Definition.Encoding, got.Definition.Compression, got.Definition.FixedWidthElements, adapterColumn.Definition.Type, adapterColumn.Definition.Encoding, adapterColumn.Definition.Compression, adapterColumn.Definition.FixedWidthElements)
	}
	return nil
}

func typedColumnAdapterPartFromDecodedImage(opts typedColumnAdapterOptions, image typedcolumn.ColumnPartImage, part *typedcolumn.ColumnPart) (*typedColumnAdapterPart, error) {
	columns, err := typedColumnAdapterColumnsForFields(opts.Fields)
	if err != nil {
		return nil, err
	}
	if err := validateTypedColumnAdapterImageSchema(part, columns, opts.SchemaVersion); err != nil {
		return nil, err
	}
	dictionaries, err := image.Dictionaries()
	if err != nil {
		return nil, err
	}
	if err := validateTypedColumnAdapterMetadata(dictionaries, columns); err != nil {
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
				return nil, err
			}
			columns[i].Definition.Cardinality = partColumn.Definition.Cardinality
			columns[i].Dictionary = dict
			columns[i].ReverseDictionary = reverseTypedColumnAdapterDictionary(dict)
		}
	}
	return &typedColumnAdapterPart{Options: opts, Columns: columns, Part: part, Dictionary: dictionaries}, nil
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
	if primary.Definition.Type != typedcolumn.ColumnTypeInt64 || primary.Definition.Encoding != typedcolumn.EncodingRawInt64 {
		return fmt.Errorf("collections: typed-column adapter image primary-id column %q type/encoding mismatch", typedColumnAdapterPrimaryIDColumn)
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
		if got.Definition.Type != column.Definition.Type || got.Definition.Encoding != column.Definition.Encoding || got.Definition.Compression != column.Definition.Compression || got.Definition.FixedWidthElements != column.Definition.FixedWidthElements {
			return fmt.Errorf("collections: typed-column adapter image column %q schema mismatch: got type=%s encoding=%s compression=%s fixed_width_elements=%d want type=%s encoding=%s compression=%s fixed_width_elements=%d", column.Definition.Name, got.Definition.Type, got.Definition.Encoding, got.Definition.Compression, got.Definition.FixedWidthElements, column.Definition.Type, column.Definition.Encoding, column.Definition.Compression, column.Definition.FixedWidthElements)
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
	return typedcolumn.BuildColumnPartImage(p.Part, typedcolumn.ColumnPartImageOptions{Dictionaries: p.Dictionary, LayoutLogicalTypes: logicalTypes})
}

// decodeTypedColumnPhysicalQueryDenseGroupCountPart prepares the q1 typed-column
// section fast path from the adapter seam so production query routing does not
// import the typedcolumn data plane directly.
func decodeTypedColumnPhysicalQueryDenseGroupCountPart(plan columnTypedColumnPhysicalQueryPlan, schemaHash uint64, typedRef, physical columnManifestAssetRefForScan, raw []byte) (columnTypedColumnPhysicalQueryPart, error) {
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
	if plan.SortKeyPrefix.Planned {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("%w: dense typed-column group-count does not support sort-key row pruning", ErrColumnQueryPlanUnsupported)
	}
	adapterColumn, ok, err := typedColumnStringPredicateAdapterColumn(plan.Fields, plan.GroupColumn)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	if !ok {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("collections: dense typed-column group-count column %q is not owned by typed_column_part", plan.GroupColumn)
	}
	for _, candidate := range adapterPart.Columns {
		if candidate.Definition.Name == adapterColumn.Definition.Name {
			adapterColumn = candidate
			break
		}
	}
	if adapterColumn.Field.Nullable || adapterColumn.Definition.Type != typedcolumn.ColumnTypeLowCardinalityCode || adapterColumn.Definition.Encoding != typedcolumn.EncodingLowCardinalityUint32 {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("%w: dense typed-column group-count column %q is not a non-null low-cardinality string", ErrColumnQueryPlanUnsupported, plan.GroupColumn)
	}
	partColumn, ok := adapterPart.Part.Columns[adapterColumn.Definition.Name]
	if !ok {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("collections: dense typed-column group-count missing column %q", adapterColumn.Definition.Name)
	}
	if partColumn.Definition.Type != typedcolumn.ColumnTypeLowCardinalityCode || partColumn.Definition.Encoding != typedcolumn.EncodingLowCardinalityUint32 || partColumn.Definition.Cardinality == 0 {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("%w: dense typed-column group-count column %q type=%s encoding=%s cardinality=%d", ErrColumnQueryPlanUnsupported, plan.GroupColumn, partColumn.Definition.Type, partColumn.Definition.Encoding, partColumn.Definition.Cardinality)
	}
	if uint64(int(partColumn.Definition.Cardinality)) != uint64(partColumn.Definition.Cardinality) {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("collections: dense typed-column group-count cardinality=%d exceeds host int", partColumn.Definition.Cardinality)
	}
	cardinality := int(partColumn.Definition.Cardinality)
	if len(adapterColumn.ReverseDictionary) != cardinality {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("collections: dense typed-column group-count dictionary cardinality=%d want %d for column %q", len(adapterColumn.ReverseDictionary), cardinality, adapterColumn.Definition.Name)
	}
	for code := 0; code < cardinality; code++ {
		if _, ok := adapterColumn.ReverseDictionary[int64(code)]; !ok {
			return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("collections: dense typed-column group-count dictionary missing local code %d for column %q", code, adapterColumn.Definition.Name)
		}
	}
	codes := make([]uint32, 0, summary.Rows)
	var scratch []uint32
	var reader typedcolumn.GranuleReader
	decodedBytes := uint64(0)
	for blockIdx, block := range partColumn.Blocks {
		g := block.Granule
		if g.HasMinMax {
			if g.Min < 0 || g.Max < 0 || g.Min > g.Max || uint64(g.Max) >= uint64(cardinality) {
				return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("collections: dense typed-column group-count block %d min/max [%d,%d] outside cardinality %d", blockIdx, g.Min, g.Max, cardinality)
			}
		}
		decoded, err := reader.DecodeUint32CodesInto(scratch[:0], g)
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, err
		}
		if len(decoded) != block.Descriptor.RowCount {
			return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("collections: dense typed-column group-count decoded rows=%d want %d", len(decoded), block.Descriptor.RowCount)
		}
		for i, code := range decoded {
			if uint64(code) >= uint64(cardinality) {
				return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("collections: dense typed-column group-count code[%d]=%d outside cardinality=%d", i, code, cardinality)
			}
		}
		codes = append(codes, decoded...)
		scratch = decoded
		decodedBytes += uint64(g.RawBytes)
	}
	if len(codes) != summary.Rows {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("collections: dense typed-column group-count decoded rows=%d want part rows=%d", len(codes), summary.Rows)
	}
	return columnTypedColumnPhysicalQueryPart{
		Ref:                 typedRef,
		PhysicalRef:         physical,
		Rows:                summary.Rows,
		Bytes:               int64(len(raw)),
		Sections:            summary.Sections,
		SectionBytes:        summary.SectionBytes,
		GranulesConsidered:  len(partColumn.Blocks),
		GranulesDecoded:     len(partColumn.Blocks),
		DecodedBlocks:       len(partColumn.Blocks),
		DecodedPayloadBytes: decodedBytes,
		DenseGroupCount:     &columnTypedColumnDenseGroupCountPart{Cardinality: cardinality, DictionaryByCode: adapterColumn.ReverseDictionary, Codes: codes},
	}, nil
}

// decodeTypedColumnPhysicalQueryDenseGroupHourCountPart prepares the q3
// typed-column section fast path from the adapter seam so production query
// routing does not import the typedcolumn data plane directly.
func decodeTypedColumnPhysicalQueryDenseGroupHourCountPart(plan columnTypedColumnPhysicalQueryPlan, schemaHash uint64, typedRef, physical columnManifestAssetRefForScan, raw []byte) (columnTypedColumnPhysicalQueryPart, error) {
	spanPlan := plan
	spanPlan.PredicateSpecs = make([]columnPhysicalQueryPredicateSpec, 0, len(plan.PredicateSpecs))
	var groupPredicate *columnPhysicalQueryPredicateSpec
	for idx := range plan.PredicateSpecs {
		spec := plan.PredicateSpecs[idx]
		if spec.column == plan.GroupColumn {
			groupPredicate = &plan.PredicateSpecs[idx]
			continue
		}
		spanPlan.PredicateSpecs = append(spanPlan.PredicateSpecs, spec)
	}
	part, err := decodeTypedColumnPhysicalQueryDenseInt64SpanPart(spanPlan, schemaHash, typedRef, physical, raw)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	if part.DenseInt64Span == nil {
		return columnTypedColumnPhysicalQueryPart{}, errors.New("collections: dense typed-column group-hour missing decoded string/int64 payload")
	}
	predicates := part.DenseInt64Span.Predicates
	if groupPredicate != nil {
		predicate, err := densePredicateFromDictionaryCodes(part.DenseInt64Span.GroupCodes, part.DenseInt64Span.DictionaryByCode, part.DenseInt64Span.Cardinality, *groupPredicate)
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, err
		}
		predicates = append(predicates, predicate)
	}
	part.DenseGroupHourCount = &columnTypedColumnDenseGroupHourCountPart{
		Cardinality:      part.DenseInt64Span.Cardinality,
		DictionaryByCode: part.DenseInt64Span.DictionaryByCode,
		GroupCodes:       part.DenseInt64Span.GroupCodes,
		Values:           part.DenseInt64Span.Values,
		Predicates:       predicates,
	}
	part.DenseInt64Span = nil
	return part, nil
}

func densePredicateFromDictionaryCodes(codes []uint32, dictionaryByCode map[int64]string, cardinality int, spec columnPhysicalQueryPredicateSpec) (columnTypedColumnDensePredicatePart, error) {
	allowed := make([]uint64, (cardinality+63)/64)
	matchedLiterals := 0
	for code := 0; code < cardinality; code++ {
		value, ok := dictionaryByCode[int64(code)]
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
	if matchedLiterals == 0 {
		return columnTypedColumnDensePredicatePart{RejectsAll: true}, nil
	}
	return columnTypedColumnDensePredicatePart{Codes: codes, Allowed: allowed}, nil
}

// decodeTypedColumnPhysicalQueryDenseInt64SpanPart prepares the q5 typed-column
// section fast path from the adapter seam so production query routing does not
// import the typedcolumn data plane directly.
func decodeTypedColumnPhysicalQueryDenseInt64SpanPart(plan columnTypedColumnPhysicalQueryPlan, schemaHash uint64, typedRef, physical columnManifestAssetRefForScan, raw []byte) (columnTypedColumnPhysicalQueryPart, error) {
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

	groupCodes, groupDecodedBytes, groupBlocks, err := decodeTypedColumnDenseUint32Codes(groupPartColumn, cardinality, summary.Rows, "int64-span group")
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	values, valueDecodedBytes, valueBlocks, err := decodeTypedColumnDenseInt64Values(valuePartColumn, summary.Rows, "int64-span value")
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	if len(groupCodes) != len(values) {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("collections: dense typed-column int64-span group/value rows=%d/%d", len(groupCodes), len(values))
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
		DenseInt64Span: &columnTypedColumnDenseInt64SpanPart{
			Cardinality:      cardinality,
			DictionaryByCode: groupColumn.ReverseDictionary,
			GroupCodes:       groupCodes,
			Values:           values,
			Predicates:       predicates,
		},
	}, nil
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
	if adapterColumn.Field.Nullable || adapterColumn.Definition.Type != typedcolumn.ColumnTypeLowCardinalityCode || adapterColumn.Definition.Encoding != typedcolumn.EncodingLowCardinalityUint32 {
		return typedColumnAdapterColumn{}, typedcolumn.ColumnPartColumn{}, 0, fmt.Errorf("%w: dense typed-column %s column %q is not a non-null low-cardinality string", ErrColumnQueryPlanUnsupported, role, column)
	}
	partColumn, ok := adapterPart.Part.Columns[adapterColumn.Definition.Name]
	if !ok {
		return typedColumnAdapterColumn{}, typedcolumn.ColumnPartColumn{}, 0, fmt.Errorf("collections: dense typed-column %s missing column %q", role, adapterColumn.Definition.Name)
	}
	if partColumn.Definition.Type != typedcolumn.ColumnTypeLowCardinalityCode || partColumn.Definition.Encoding != typedcolumn.EncodingLowCardinalityUint32 || partColumn.Definition.Cardinality == 0 {
		return typedColumnAdapterColumn{}, typedcolumn.ColumnPartColumn{}, 0, fmt.Errorf("%w: dense typed-column %s column %q type=%s encoding=%s cardinality=%d", ErrColumnQueryPlanUnsupported, role, column, partColumn.Definition.Type, partColumn.Definition.Encoding, partColumn.Definition.Cardinality)
	}
	if uint64(int(partColumn.Definition.Cardinality)) != uint64(partColumn.Definition.Cardinality) {
		return typedColumnAdapterColumn{}, typedcolumn.ColumnPartColumn{}, 0, fmt.Errorf("collections: dense typed-column %s cardinality=%d exceeds host int", role, partColumn.Definition.Cardinality)
	}
	cardinality := int(partColumn.Definition.Cardinality)
	if len(adapterColumn.ReverseDictionary) != cardinality {
		return typedColumnAdapterColumn{}, typedcolumn.ColumnPartColumn{}, 0, fmt.Errorf("collections: dense typed-column %s dictionary cardinality=%d want %d for column %q", role, len(adapterColumn.ReverseDictionary), cardinality, adapterColumn.Definition.Name)
	}
	for code := 0; code < cardinality; code++ {
		if _, ok := adapterColumn.ReverseDictionary[int64(code)]; !ok {
			return typedColumnAdapterColumn{}, typedcolumn.ColumnPartColumn{}, 0, fmt.Errorf("collections: dense typed-column %s dictionary missing local code %d for column %q", role, code, adapterColumn.Definition.Name)
		}
	}
	return adapterColumn, partColumn, cardinality, nil
}

func decodeTypedColumnDensePredicatePart(adapterPart *typedColumnAdapterPart, fields []TypedStorageField, spec columnPhysicalQueryPredicateSpec, rows int) (columnTypedColumnDensePredicatePart, uint64, int, error) {
	adapterColumn, partColumn, cardinality, err := typedColumnDenseStringCodeColumn(adapterPart, fields, spec.column, "predicate")
	if err != nil {
		return columnTypedColumnDensePredicatePart{}, 0, 0, err
	}
	allowed := make([]uint64, (cardinality+63)/64)
	matchedLiterals := 0
	for _, value := range spec.values {
		code, ok := adapterColumn.Dictionary[value]
		if !ok {
			continue
		}
		if code < 0 || uint64(code) >= uint64(cardinality) {
			return columnTypedColumnDensePredicatePart{}, 0, 0, fmt.Errorf("collections: dense typed-column predicate dictionary code %d outside cardinality %d for column %q", code, cardinality, adapterColumn.Definition.Name)
		}
		idx := int(code)
		allowed[idx/64] |= uint64(1) << uint(idx%64)
		matchedLiterals++
	}
	if matchedLiterals == 0 {
		return columnTypedColumnDensePredicatePart{RejectsAll: true}, 0, 0, nil
	}
	codes, decodedBytes, blocks, err := decodeTypedColumnDenseUint32Codes(partColumn, cardinality, rows, "predicate")
	if err != nil {
		return columnTypedColumnDensePredicatePart{}, 0, 0, err
	}
	return columnTypedColumnDensePredicatePart{Codes: codes, Allowed: allowed}, decodedBytes, blocks, nil
}

func decodeTypedColumnDenseUint32Codes(partColumn typedcolumn.ColumnPartColumn, cardinality int, rows int, role string) ([]uint32, uint64, int, error) {
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

func decodeTypedColumnDenseInt64Values(partColumn typedcolumn.ColumnPartColumn, rows int, role string) ([]int64, uint64, int, error) {
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
		return nil, typedcolumn.PartScanDiagnostics{}, fmt.Errorf("%w: typed-column adapter selected-row scan does not support nullable column %q", ErrColumnQueryPlanUnsupported, columnName)
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

func buildTypedColumnAdapterStringDictionary(column typedColumnAdapterColumn, rows []typedColumnAdapterRow) (map[string]int64, error) {
	seen := make(map[string]struct{})
	for rowIdx, row := range rows {
		value, ok, err := typedColumnAdapterRowValue(row, column)
		if err != nil {
			return nil, fmt.Errorf("collections: typed-column adapter row %d field %q: %w", rowIdx, column.Field.Path, err)
		}
		if ok && value.Present && !value.Null {
			seen[value.String] = struct{}{}
		}
	}
	values := make([]string, 0, len(seen))
	for value := range seen {
		values = append(values, value)
	}
	sort.Strings(values)
	dict := make(map[string]int64, len(values))
	for i, value := range values {
		dict[value] = int64(i)
	}
	return dict, nil
}

func reverseTypedColumnAdapterDictionary(dict map[string]int64) map[int64]string {
	reverse := make(map[int64]string, len(dict))
	for value, code := range dict {
		reverse[code] = value
	}
	return reverse
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
	if err := validateTypedColumnAdapterInt64AggregateTargetedSections(image, part); err != nil {
		return nil, err
	}
	section, ok := typedColumnAdapterColumnDataSection(image, adapterColumn.Definition.Name)
	if !ok {
		return nil, fmt.Errorf("collections: typed-column int64 aggregate image missing column data section %q", adapterColumn.Definition.Name)
	}
	if section.Encoding != adapterColumn.Definition.Encoding || section.Compression != adapterColumn.Definition.Compression {
		return nil, fmt.Errorf("collections: typed-column int64 aggregate column %q section encoding=%s compression=%s want encoding=%s compression=%s", adapterColumn.Definition.Name, section.Encoding, section.Compression, adapterColumn.Definition.Encoding, adapterColumn.Definition.Compression)
	}
	valueCol := part.Columns[adapterColumn.Definition.Name]
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
		if section.Encoding != column.Definition.Encoding || section.Compression != column.Definition.Compression {
			return fmt.Errorf("collections: typed-column int64 aggregate image column %q section encoding=%s compression=%s want encoding=%s compression=%s", section.Column, section.Encoding, section.Compression, column.Definition.Encoding, column.Definition.Compression)
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
		if err := addTypedColumnInt64AggregateSelectedValues(result, values, selection); err != nil {
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

func addTypedColumnInt64AggregateSelectedValues(result *TypedColumnInt64PredicateAggregateResult, values []int64, selection typedcolumn.RowSelection) error {
	switch selection.Kind() {
	case typedcolumn.RowSelectionEmpty:
		return nil
	case typedcolumn.RowSelectionAll:
		for _, v := range values {
			if err := addTypedColumnInt64PredicateAggregateValue(result, v); err != nil {
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
			if err := addTypedColumnInt64PredicateAggregateValue(result, v); err != nil {
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
				if err := addTypedColumnInt64PredicateAggregateValue(result, v); err != nil {
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
			if err := addTypedColumnInt64PredicateAggregateValue(result, values[row]); err != nil {
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
				if err := addTypedColumnInt64PredicateAggregateValue(result, values[row]); err != nil {
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
	if idCol.Definition.Type != typedcolumn.ColumnTypeInt64 || idCol.Definition.Encoding != typedcolumn.EncodingRawInt64 {
		return false, fmt.Errorf("primary id column %q type=%s encoding=%s", typedColumnAdapterPrimaryIDColumn, idCol.Definition.Type, idCol.Definition.Encoding)
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
		if err := preparedColumn.Plan.Layout.ValidateGranulePayload(granule, payload); err != nil {
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
