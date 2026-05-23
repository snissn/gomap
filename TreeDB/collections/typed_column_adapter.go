package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sort"

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
		{ValueType: ColumnStoreValueFloat32Vector, Status: typedColumnAdapterFailClosed, Reason: "dense vector typed-column sections are staged to #1756"},
		{ValueType: ColumnStoreValueAdjacencyList, Status: typedColumnAdapterFailClosed, Reason: "adjacency typed-column sections are staged to #1756"},
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
	batch := typedcolumn.Batch{Rows: len(rows), Columns: make(map[string][]int64, len(defs))}
	batch.Columns[typedColumnAdapterPrimaryIDColumn] = make([]int64, len(rows))
	for _, column := range columns {
		batch.Columns[column.Definition.Name] = make([]int64, len(rows))
	}
	for rowIdx, row := range rows {
		batch.Columns[typedColumnAdapterPrimaryIDColumn][rowIdx] = row.PrimaryID
		for _, column := range columns {
			value, ok, err := typedColumnAdapterRowValue(row, column)
			if err != nil {
				return nil, fmt.Errorf("collections: typed-column adapter row %d field %q: %w", rowIdx, column.Field.Path, err)
			}
			if !ok {
				return nil, fmt.Errorf("collections: typed-column adapter row %d missing field %q", rowIdx, column.Field.Path)
			}
			encoded, err := encodeTypedColumnAdapterValue(column, value)
			if err != nil {
				return nil, fmt.Errorf("collections: typed-column adapter row %d field %q: %w", rowIdx, column.Field.Path, err)
			}
			batch.Columns[column.Definition.Name][rowIdx] = encoded
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

func typedColumnAdapterPartFromImage(opts typedColumnAdapterOptions, image typedcolumn.ColumnPartImage) (*typedColumnAdapterPart, error) {
	part, err := typedcolumn.ColumnPartFromImage(image)
	if err != nil {
		return nil, err
	}
	columns, err := typedColumnAdapterColumnsForFields(opts.Fields)
	if err != nil {
		return nil, err
	}
	if err := validateTypedColumnAdapterImageSchema(part, columns); err != nil {
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
			dict := dictionaries[columns[i].Definition.Name]
			if len(dict) == 0 {
				return nil, fmt.Errorf("collections: typed-column adapter image missing dictionary for %q", columns[i].Definition.Name)
			}
			columns[i].Dictionary = dict
			columns[i].ReverseDictionary = reverseTypedColumnAdapterDictionary(dict)
		}
	}
	return &typedColumnAdapterPart{Options: opts, Columns: columns, Part: part, Dictionary: dictionaries}, nil
}

func validateTypedColumnAdapterImageSchema(part *typedcolumn.ColumnPart, columns []typedColumnAdapterColumn) error {
	if part == nil {
		return errors.New("collections: typed-column adapter nil image part")
	}
	primary, ok := part.Columns[typedColumnAdapterPrimaryIDColumn]
	if !ok {
		return fmt.Errorf("collections: typed-column adapter image missing primary-id column %q", typedColumnAdapterPrimaryIDColumn)
	}
	if primary.Definition.Type != typedcolumn.ColumnTypeInt64 || primary.Definition.Encoding != typedcolumn.EncodingRawInt64 {
		return fmt.Errorf("collections: typed-column adapter image primary-id column %q type/encoding mismatch", typedColumnAdapterPrimaryIDColumn)
	}
	for _, column := range columns {
		got, ok := part.Columns[column.Definition.Name]
		if !ok {
			return fmt.Errorf("collections: typed-column adapter image missing column %q", column.Definition.Name)
		}
		if got.Definition.Type != column.Definition.Type || got.Definition.Encoding != column.Definition.Encoding || got.Definition.Compression != column.Definition.Compression {
			return fmt.Errorf("collections: typed-column adapter image column %q schema mismatch: got type=%s encoding=%s compression=%s want type=%s encoding=%s compression=%s", column.Definition.Name, got.Definition.Type, got.Definition.Encoding, got.Definition.Compression, column.Definition.Type, column.Definition.Encoding, column.Definition.Compression)
		}
	}
	return nil
}

func (p *typedColumnAdapterPart) buildImage() (typedcolumn.ColumnPartImage, error) {
	if p == nil || p.Part == nil {
		return typedcolumn.ColumnPartImage{}, errors.New("collections: nil typed-column adapter part")
	}
	return typedcolumn.BuildColumnPartImage(p.Part, typedcolumn.ColumnPartImageOptions{Dictionaries: p.Dictionary})
}

func (p *typedColumnAdapterPart) scanColumnValues(columnName string) ([]columnDeclaredValue, error) {
	if p == nil || p.Part == nil {
		return nil, errors.New("collections: nil typed-column adapter part")
	}
	column, ok := p.columnByName(columnName)
	if !ok {
		return nil, fmt.Errorf("collections: typed-column adapter missing column %q", columnName)
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

func (p *typedColumnAdapterPart) columnByName(name string) (typedColumnAdapterColumn, bool) {
	for _, column := range p.Columns {
		if column.Field.Name == name || column.Field.Path == name || column.Definition.Name == name {
			return column, true
		}
	}
	return typedColumnAdapterColumn{}, false
}

func encodeTypedColumnAdapterValue(column typedColumnAdapterColumn, value columnDeclaredValue) (int64, error) {
	if value.Type == "" {
		return 0, errors.New("declared type required")
	}
	if value.Type != column.Field.ValueType {
		return 0, fmt.Errorf("value type=%q want %q", value.Type, column.Field.ValueType)
	}
	if !value.Present || value.Null {
		return 0, fmt.Errorf("null or missing values are not represented by the #1754 typed-column adapter")
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

func decodeTypedColumnAdapterValue(column typedColumnAdapterColumn, raw int64) (columnDeclaredValue, error) {
	value := columnDeclaredValue{Type: column.Field.ValueType, Present: true}
	switch column.Field.ValueType {
	case ColumnStoreValueBool:
		value.Bool = raw != 0
	case ColumnStoreValueInt64:
		value.Int64 = raw
	case ColumnStoreValueFloat32:
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
	pathValue, pathOK := row.Values[column.Field.Path]
	if column.Field.Name == "" || column.Field.Name == column.Field.Path {
		return pathValue, pathOK, nil
	}
	nameValue, nameOK := row.Values[column.Field.Name]
	if pathOK && nameOK {
		return columnDeclaredValue{}, false, fmt.Errorf("ambiguous field keys %q and %q", column.Field.Path, column.Field.Name)
	}
	if pathOK {
		return pathValue, true, nil
	}
	return nameValue, nameOK, nil
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
