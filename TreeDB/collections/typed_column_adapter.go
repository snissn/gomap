package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

var errTypedColumnAdapterUnsupportedType = errors.New("collections: typed-column adapter unsupported type")

const (
	typedColumnAdapterPrimaryIDColumn       = "__treedb_primary_id"
	typedColumnAdapterMetadataDictionary    = "__treedb_adapter_metadata"
	typedColumnAdapterMetadataValueTypeMark = "value_type"
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
	PrimaryIDs []int64
	Values     [][]columnDeclaredValue
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

func typedColumnAdapterTypeMatrix() []typedColumnAdapterTypeMapping {
	return []typedColumnAdapterTypeMapping{
		{ValueType: ColumnStoreValueBool, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeBool, Encoding: typedcolumn.EncodingBoolBitpackRLE},
		{ValueType: ColumnStoreValueInt64, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingDeltaVarint},
		{ValueType: ColumnStoreValueFloat32, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Reason: "stored as raw int64 float32 bit patterns until native float sections land"},
		{ValueType: ColumnStoreValueDouble, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Reason: "stored as raw int64 float64 bit patterns until native float sections land"},
		{ValueType: ColumnStoreValueString, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeLowCardinalityCode, Encoding: typedcolumn.EncodingLowCardinalityUint32, Reason: "stored as dictionary codes with dictionary section metadata"},
		{ValueType: ColumnStoreValueFloat32Vector, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeFloat32Vector, Encoding: typedcolumn.EncodingRawFloat32Vector, Reason: "stored as fixed-dim dense little-endian float32 sections"},
		{ValueType: ColumnStoreValueAdjacencyList, Status: typedColumnAdapterRepresented, ColumnType: typedcolumn.ColumnTypeAdjacencyList, Encoding: typedcolumn.EncodingRawUint32Dense, Reason: "stored as fixed-degree dense little-endian uint32 sections"},
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
		case ColumnStoreValueFloat32Vector, ColumnStoreValueAdjacencyList:
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
	}
	switch field.ValueType {
	case ColumnStoreValueFloat32Vector:
		if field.VectorDims <= 0 {
			return typedColumnAdapterColumn{}, fmt.Errorf("collections: typed-column adapter field %q float32_vector requires positive vector_dims", field.Path)
		}
		def.FixedWidthElements = field.VectorDims
	case ColumnStoreValueAdjacencyList:
		if field.AdjacencyDegree <= 0 {
			return typedColumnAdapterColumn{}, fmt.Errorf("collections: typed-column adapter field %q adjacency_list requires positive adjacency_degree", field.Path)
		}
		def.FixedWidthElements = field.AdjacencyDegree
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
	})
	for _, column := range columns {
		defs = append(defs, column.Definition)
	}
	batch := typedcolumn.Batch{Rows: len(rows), Columns: make(map[string][]int64, len(defs)), Nulls: make(map[string][]bool), Defaults: make(map[string][]bool)}
	batch.Columns[typedColumnAdapterPrimaryIDColumn] = make([]int64, len(rows))
	for _, column := range columns {
		switch column.Field.ValueType {
		case ColumnStoreValueFloat32Vector:
			if batch.Float32Vectors == nil {
				batch.Float32Vectors = make(map[string][]float32)
			}
			elements, err := typedColumnAdapterDenseElements(len(rows), column.Definition.FixedWidthElements)
			if err != nil {
				return nil, err
			}
			batch.Float32Vectors[column.Definition.Name] = make([]float32, elements)
		case ColumnStoreValueAdjacencyList:
			if batch.Uint32Vectors == nil {
				batch.Uint32Vectors = make(map[string][]uint32)
			}
			elements, err := typedColumnAdapterDenseElements(len(rows), column.Definition.FixedWidthElements)
			if err != nil {
				return nil, err
			}
			batch.Uint32Vectors[column.Definition.Name] = make([]uint32, elements)
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
			switch column.Field.ValueType {
			case ColumnStoreValueFloat32Vector:
				if err := encodeTypedColumnAdapterFloat32VectorValue(batch.Float32Vectors[column.Definition.Name], rowIdx, column, value); err != nil {
					return nil, fmt.Errorf("collections: typed-column adapter row %d field %q: %w", rowIdx, column.Field.Path, err)
				}
			case ColumnStoreValueAdjacencyList:
				if err := encodeTypedColumnAdapterAdjacencyListValue(batch.Uint32Vectors[column.Definition.Name], rowIdx, column, value); err != nil {
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
	partOpts := typedcolumn.Options{
		SchemaVersion: opts.SchemaVersion,
		SchemaMode:    typedcolumn.ColumnSchemaFixed,
		Columns:       defs,
		LogicalPrimaryKey: typedcolumn.LogicalPrimaryKey{
			Columns: []string{typedColumnAdapterPrimaryIDColumn},
		},
		SortKey:     typedcolumn.SortKey{Columns: []typedcolumn.SortKeyColumn{{Column: typedColumnAdapterPrimaryIDColumn}}},
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
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return nil, err
	}
	return typedColumnAdapterPartFromImageWithoutRowLocators(opts, image)
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
	logicalTypes := map[string]string{typedColumnAdapterPrimaryIDColumn: string(columnsemantics.LogicalInt64)}
	for _, column := range p.Columns {
		if logical, ok := columnStoreSemanticLogicalType(column.Field.ValueType); ok {
			logicalTypes[column.Definition.Name] = string(logical)
		}
	}
	return typedcolumn.BuildColumnPartImage(p.Part, typedcolumn.ColumnPartImageOptions{Dictionaries: p.Dictionary, LayoutLogicalTypes: logicalTypes})
}

func (p *typedColumnAdapterPart) scanDecodedValues() (typedColumnPartDecodedValues, error) {
	if p == nil || p.Part == nil {
		return typedColumnPartDecodedValues{}, errors.New("collections: nil typed-column adapter part")
	}
	ids, err := p.scanInt64ColumnValues(typedColumnAdapterPrimaryIDColumn)
	if err != nil {
		return typedColumnPartDecodedValues{}, err
	}
	values := make([][]columnDeclaredValue, len(p.Columns))
	for i, column := range p.Columns {
		columnValues, err := p.scanColumnValues(column.Definition.Name)
		if err != nil {
			return typedColumnPartDecodedValues{}, err
		}
		if len(columnValues) != len(ids) {
			return typedColumnPartDecodedValues{}, fmt.Errorf("collections: typed-column adapter column %q rows=%d want %d", column.Definition.Name, len(columnValues), len(ids))
		}
		values[i] = columnValues
	}
	return typedColumnPartDecodedValues{PrimaryIDs: ids, Values: values}, nil
}

func (d typedColumnPartDecodedValues) valuesForRowInto(rowIdx int, dst []columnDeclaredValue) ([]columnDeclaredValue, error) {
	if rowIdx < 0 || rowIdx >= len(d.PrimaryIDs) {
		return nil, fmt.Errorf("collections: typed-column reconstruction row_index=%d outside typed_column_part rows=%d", rowIdx, len(d.PrimaryIDs))
	}
	if d.PrimaryIDs[rowIdx] != int64(rowIdx) {
		return nil, fmt.Errorf("collections: typed-column reconstruction locator=%d want row_index=%d", d.PrimaryIDs[rowIdx], rowIdx)
	}
	if cap(dst) < len(d.Values) {
		dst = make([]columnDeclaredValue, len(d.Values))
	} else {
		dst = dst[:len(d.Values)]
	}
	for i := range d.Values {
		if rowIdx >= len(d.Values[i]) {
			return nil, fmt.Errorf("collections: typed-column reconstruction row_index=%d outside field[%d] rows=%d", rowIdx, i, len(d.Values[i]))
		}
		dst[i] = d.Values[i][rowIdx]
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
	if column.Field.ValueType == ColumnStoreValueAdjacencyList {
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
	for code := int64(0); code < int64(cardinality); code++ {
		if _, ok := seen[code]; !ok {
			return fmt.Errorf("collections: typed-column adapter image dictionary missing code %d for %q", code, column.Definition.Name)
		}
	}
	return nil
}

func typedColumnAdapterMetadataKey(column typedColumnAdapterColumn) string {
	return column.Definition.Name + "\x00" + typedColumnAdapterMetadataValueTypeMark + "\x00" + string(column.Field.ValueType)
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
	}
	return nil
}

func typedColumnAdapterDictionaries(columns []typedColumnAdapterColumn) map[string]map[string]int64 {
	out := make(map[string]map[string]int64)
	metadata := make(map[string]int64, len(columns))
	for i, column := range columns {
		metadata[typedColumnAdapterMetadataKey(column)] = int64(i + 1)
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

type typedColumnAdapterDenseUint32ResourceView struct {
	Rows           int
	ElementsPerRow int
	Values         []uint32
	Handle         *mappedresource.Handle
	Direct         bool
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
	h, err := reader.AcquireSection(section)
	if err != nil {
		return typedColumnAdapterDenseUint32ResourceView{}, err
	}
	values, viewErr := typedColumnAdapterUint32View(reader.Manager, h)
	if viewErr == nil {
		if len(values) != expected {
			_ = h.Release()
			return typedColumnAdapterDenseUint32ResourceView{}, fmt.Errorf("collections: typed-column adapter column %q dense uint32 values=%d want %d", column.Definition.Name, len(values), expected)
		}
		return typedColumnAdapterDenseUint32ResourceView{Rows: rows, ElementsPerRow: degree, Values: values, Handle: h, Direct: true}, nil
	}
	decoded, decodeErr := typedcolumn.DecodeRawUint32DensePayload(nil, h.Bytes(), rows, degree)
	releaseErr := h.Release()
	if decodeErr != nil {
		if releaseErr != nil {
			decodeErr = errors.Join(decodeErr, releaseErr)
		}
		return typedColumnAdapterDenseUint32ResourceView{}, decodeErr
	}
	if releaseErr != nil {
		return typedColumnAdapterDenseUint32ResourceView{}, releaseErr
	}
	return typedColumnAdapterDenseUint32ResourceView{Rows: rows, ElementsPerRow: degree, Values: decoded, Direct: false}, nil
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
	if adapterColumn.Definition.Type == typedcolumn.ColumnTypeInt64 && adapterColumn.Field.ValueType != ColumnStoreValueInt64 {
		if err := requireTypedColumnAdapterCapability(adapterColumn, columnsemantics.OpOrderedRange, fmt.Sprintf("typed-column int64 predicate column %q", column)); err != nil {
			return false, err
		}
	}
	if adapterColumn.Field.ValueType != ColumnStoreValueInt64 || adapterColumn.Definition.Type != typedcolumn.ColumnTypeInt64 {
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
	if adapterColumn.Definition.Type == typedcolumn.ColumnTypeInt64 && adapterColumn.Field.ValueType != ColumnStoreValueInt64 {
		if err := requireTypedColumnAdapterCapability(adapterColumn, columnsemantics.OpSum, fmt.Sprintf("typed-column int64 predicate aggregate column %q", column)); err != nil {
			return nil, typedColumnAdapterColumn{}, 0, err
		}
	}
	if adapterColumn.Field.ValueType != ColumnStoreValueInt64 || adapterColumn.Field.Nullable || adapterColumn.Definition.Type != typedcolumn.ColumnTypeInt64 {
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
	if adapterColumn.Definition.Type == typedcolumn.ColumnTypeInt64 && adapterColumn.Field.ValueType != ColumnStoreValueInt64 {
		if err := requireTypedColumnAdapterCapability(adapterColumn, typedColumnInt64PredicateSemanticOperation(req.Kind), fmt.Sprintf("typed-column int64 predicate aggregate column %q", column)); err != nil {
			return nil, err
		}
	}
	if adapterColumn.Field.ValueType != ColumnStoreValueInt64 || adapterColumn.Field.Nullable || adapterColumn.Definition.Type != typedcolumn.ColumnTypeInt64 {
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

func typedColumnAdapterPrepareInt64PredicatePart(fields []TypedStorageField, raw []byte, refPartID uint64, typedRows int, physicalRows int, schemaHash uint64, column string, operation string, decode typedColumnAdapterPartImageDecoder) (*typedColumnAdapterPart, typedColumnAdapterColumn, int, error) {
	adapterColumn, ok, err := typedColumnInt64PredicateAdapterColumn(fields, column)
	if err != nil {
		return nil, typedColumnAdapterColumn{}, 0, err
	}
	if !ok {
		return nil, typedColumnAdapterColumn{}, 0, fmt.Errorf("collections: typed-column int64 predicate %s column %q is not owned by typed_column_part", operation, column)
	}
	if adapterColumn.Definition.Type == typedcolumn.ColumnTypeInt64 && adapterColumn.Field.ValueType != ColumnStoreValueInt64 {
		if err := requireTypedColumnAdapterCapability(adapterColumn, columnsemantics.OpOrderedRange, fmt.Sprintf("typed-column int64 predicate %s column %q", operation, column)); err != nil {
			return nil, typedColumnAdapterColumn{}, 0, err
		}
	}
	if adapterColumn.Field.ValueType != ColumnStoreValueInt64 || adapterColumn.Definition.Type != typedcolumn.ColumnTypeInt64 {
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
		result.Diagnostics.DecodedHeapCopyBytes += uint64(g.RawBytes + idBlock.Granule.RawBytes)
		result.Diagnostics.RowsScanned += len(values)
		for i, v := range values {
			rowIndex := block.Descriptor.FirstRow + i
			if visibility != nil && !visibility.rowVisible(rowIndex) {
				continue
			}
			if !typedColumnInt64PredicateMatches(req, v) {
				continue
			}
			row := TypedColumnInt64PredicateScanRow{Generation: generation, PartID: partID, RowIndex: rowIndex, PrimaryID: ids[i], Value: v}
			if visibility != nil {
				row.Generation = visibility.Ref.Generation
				row.PartID = visibility.Ref.PartID
				row.DocumentID = visibility.documentID(rowIndex)
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
	reader typedcolumn.GranuleReader
	codes  []uint32
	ids    []int64
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
