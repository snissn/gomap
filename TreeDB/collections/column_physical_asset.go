package collections

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/buger/jsonparser"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/tidwall/gjson"
)

const (
	columnPhysicalAssetMagic     = uint32(0x54435041) // TCPA
	columnPhysicalAssetVersionV1 = uint16(1)
	columnPhysicalAssetVersionV2 = uint16(2)
	columnPhysicalAssetVersionV3 = uint16(3)
	columnPhysicalAssetVersionV4 = uint16(4)
	columnPhysicalAssetVersionV5 = uint16(5)
	columnPhysicalAssetVersionV6 = uint16(6)
	columnPhysicalAssetVersionV7 = uint16(7)
	columnPhysicalAssetVersionV8 = uint16(8)
	columnPhysicalAssetVersion   = columnPhysicalAssetVersionV4
)

const (
	columnPhysicalAssetRowEncodingFixedID      = "fixed_id"
	columnPhysicalAssetRowEncodingDenseIDRange = "dense_id_range"
)

var ErrColumnDeclaredValueUnsupported = errors.New("collections: unsupported column declared value")

type columnWriteDocument struct {
	ID                  []byte
	Document            []byte
	declaredValues      []columnDeclaredValue
	declaredValuesReady bool
}

type trustedFloat32Projection struct {
	column  string
	metric  VectorMetric
	vectors [][]float32
}

func newTrustedFloat32Projection(ids, documents [][]byte, column string, metric VectorMetric, vectors [][]float32) (*trustedFloat32Projection, error) {
	if column == "" {
		return nil, errors.New("collections: validated float32 projection column is required")
	}
	metric, err := normalizeVectorMetric(metric)
	if err != nil {
		return nil, err
	}
	if len(ids) != len(documents) || len(vectors) != len(documents) {
		return nil, fmt.Errorf("collections: validated float32 projection ids=%d documents=%d vectors=%d", len(ids), len(documents), len(vectors))
	}
	owned := make([][]float32, len(vectors))
	for row := range vectors {
		if len(vectors[row]) == 0 {
			return nil, fmt.Errorf("collections: validated float32 projection row %d is empty", row)
		}
		owned[row] = append([]float32(nil), vectors[row]...)
		if err := validateFloat32Vector(owned[row]); err != nil {
			return nil, fmt.Errorf("collections: validated float32 projection row %d: %w", row, err)
		}
		if metric == VectorMetricCosine && vectorNormSquared(owned[row]) == 0 {
			return nil, fmt.Errorf("collections: validated float32 projection row %d has zero magnitude for cosine metric", row)
		}
	}
	return &trustedFloat32Projection{column: column, metric: metric, vectors: owned}, nil
}

func validateTrustedFloat32ProjectionMeta(meta CollectionMeta, projection *trustedFloat32Projection) error {
	if projection == nil {
		return nil
	}
	if normalizedDocumentFormat(meta.Options.DocumentFormat) != DocumentFormatJSON {
		return fmt.Errorf("collections: validated float32 projection requires JSON document format, got %q", meta.Options.DocumentFormat)
	}
	cfg := meta.Options.ColumnStore
	if !columnStoreWriteEnabled(meta) || len(cfg.Columns) != 1 {
		return errors.New("collections: validated float32 projection requires exactly one enabled column")
	}
	if cfg.RetainedPayload != ColumnRetainedPayloadFull || columnRetainedPayloadEffectiveEncoding(cfg) != ColumnRetainedPayloadEncodingJSON {
		return errors.New("collections: validated float32 projection requires full JSON retained payload")
	}
	column := cfg.Columns[0]
	owner, err := columnStoreColumnOwner(column)
	if err != nil {
		return err
	}
	if column.Name != projection.column || column.Path != projection.column || column.ValueType != ColumnStoreValueFloat32Vector || owner != TypedStorageOwnerColumnPart || column.Nullable || column.VectorDims <= 0 {
		return fmt.Errorf("collections: validated float32 projection does not match configured column %q", projection.column)
	}
	if len(meta.VectorIndexes) != 1 {
		return fmt.Errorf("collections: validated float32 projection requires exactly one vector index, got %d", len(meta.VectorIndexes))
	}
	def, err := normalizeVectorIndexDefinition(meta.VectorIndexes[0])
	if err != nil {
		return fmt.Errorf("collections: validated float32 projection vector index: %w", err)
	}
	if def.Strategy != VectorIndexStrategyColumnGraph || def.Field != projection.column || def.Dimensions != column.VectorDims || def.Metric != projection.metric {
		return fmt.Errorf("collections: validated float32 projection column %q has no matching column_graph index", projection.column)
	}
	for row := range projection.vectors {
		if len(projection.vectors[row]) != column.VectorDims {
			return fmt.Errorf("collections: validated float32 projection row %d dimensions=%d want %d", row, len(projection.vectors[row]), column.VectorDims)
		}
	}
	return nil
}

func applyTrustedFloat32Projection(ids [][]byte, documents []columnWriteDocument, projection *trustedFloat32Projection) error {
	if projection == nil {
		return nil
	}
	if len(ids) != len(documents) || len(projection.vectors) != len(documents) {
		return fmt.Errorf("collections: validated float32 projection ids=%d documents=%d vectors=%d", len(ids), len(documents), len(projection.vectors))
	}
	vectorsByID := make(map[string][]float32, len(ids))
	for row := range ids {
		key := string(ids[row])
		if _, exists := vectorsByID[key]; exists {
			return fmt.Errorf("collections: validated float32 projection duplicate id at row %d", row)
		}
		vectorsByID[key] = projection.vectors[row]
	}
	for row := range documents {
		vector, ok := vectorsByID[string(documents[row].ID)]
		if !ok {
			return fmt.Errorf("collections: validated float32 projection document row %d id mismatch", row)
		}
		documents[row].declaredValues = []columnDeclaredValue{{
			Type:          ColumnStoreValueFloat32Vector,
			Present:       true,
			Float32Vector: vector,
		}}
		documents[row].declaredValuesReady = true
	}
	return nil
}

type columnDeclaredValue struct {
	Type ColumnStoreValueType
	// Present distinguishes an omitted nullable JSON path from an explicit null.
	Present  bool
	Null     bool
	Bool     bool
	Int64    int64
	Float32  float32
	Double   float64
	String   string
	Int8     int8
	Uint8    uint8
	Int16    int16
	Uint16   uint16
	Int32    int32
	Uint32   uint32
	Uint64   uint64
	Float16  uint16
	BFloat16 uint16
	// Float32Vector stores a decoded vector column value. It is used for first-
	// class vector physical assets and is not part of scalar query hot paths.
	Float32Vector []float32
	// DenseNumericVector stores raw row-major fixed-width vector bytes for
	// quantized dense vector typed-column values. Multi-byte elements are already
	// little-endian encoded for the declared ColumnStoreValue*Vector type.
	DenseNumericVector []byte
	// Byte/packed-code row-shape metadata accompanies Bytes when Type is
	// byte_vector or packed_*_vector so reconstruction can fail closed and, for
	// packed values, expand logical elements without consulting mutable schema.
	ElementsPerRow int
	BitsPerElement int
	BytesPerRow    int
	Uint32List     []uint32
	AdjacencyList  []uint32
	Bytes          []byte
	// StringBytes is used by physical scan/query hot paths as an asset-buffer
	// view valid only for the current scan callback / pinned prepared runner
	// view. Copy before retaining beyond that lifetime. String remains the
	// owned representation for full asset decoding.
	StringBytes []byte
}

type columnDeclaredRow struct {
	ID      []byte
	Deleted bool
	Values  []columnDeclaredValue
}

type columnPhysicalAssetEncodeInput struct {
	Collection        string
	Namespace         string
	Generation        uint64
	PartID            uint64
	AppliedCommandLSN uint64
	Operation         ColumnPublishOperation
	SchemaHash        uint64
	Columns           []ColumnStoreColumn
	Rows              []columnDeclaredRow
}

type columnPhysicalAssetSummary struct {
	RowCount     int
	ColumnCount  int
	PayloadBytes int64
}

type columnPhysicalAssetHeader struct {
	Collection        string
	Namespace         string
	Generation        uint64
	PartID            uint64
	AppliedCommandLSN uint64
	Operation         ColumnPublishOperation
	SchemaHash        uint64
	RowCount          int
	ColumnCount       int
}

type columnPhysicalAsset struct {
	Header  columnPhysicalAssetHeader
	Columns []ColumnStoreColumn
	Rows    []columnDeclaredRow
}

func extractColumnDeclaredRowsFromJSONDocuments(cfg ColumnStoreConfig, docs []columnWriteDocument) ([]columnDeclaredRow, error) {
	if rows, ok, err := extractColumnDeclaredRowsFromRootJSONDocumentsFastPath(cfg, docs); ok || err != nil {
		return rows, err
	}
	rows := make([]columnDeclaredRow, 0, len(docs))
	for docIdx, doc := range docs {
		decoder := json.NewDecoder(bytes.NewReader(doc.Document))
		decoder.UseNumber()
		var root any
		if err := decoder.Decode(&root); err != nil {
			return nil, fmt.Errorf("%w: document[%d] invalid JSON: %v", ErrColumnDeclaredValueUnsupported, docIdx, err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			if err == nil {
				err = errors.New("trailing JSON value")
			}
			return nil, fmt.Errorf("%w: document[%d] invalid JSON: %v", ErrColumnDeclaredValueUnsupported, docIdx, err)
		}
		obj, ok := root.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: document[%d] root is not object", ErrColumnDeclaredValueUnsupported, docIdx)
		}
		values := make([]columnDeclaredValue, len(cfg.Columns))
		for colIdx, col := range cfg.Columns {
			raw, exists := lookupColumnJSONPath(obj, col.Path)
			value, err := convertColumnDeclaredValue(col, raw, exists)
			if err != nil {
				return nil, fmt.Errorf("%w: document[%d] column[%d] %q: %v", ErrColumnDeclaredValueUnsupported, docIdx, colIdx, col.Name, err)
			}
			values[colIdx] = value
		}
		rows = append(rows, columnDeclaredRow{
			ID:     bytes.Clone(doc.ID),
			Values: values,
		})
	}
	return rows, nil
}

func extractColumnDeclaredRowsFromRootJSONDocumentsFastPath(cfg ColumnStoreConfig, docs []columnWriteDocument) ([]columnDeclaredRow, bool, error) {
	for _, col := range cfg.Columns {
		if col.Path == "" || strings.Contains(col.Path, ".") || !columnDeclaredJSONParserValueSupported(col.ValueType) {
			return nil, false, nil
		}
	}
	rows := make([]columnDeclaredRow, 0, len(docs))
	for docIdx, doc := range docs {
		if !gjson.ValidBytes(doc.Document) {
			return nil, true, fmt.Errorf("%w: document[%d] invalid JSON: invalid JSON", ErrColumnDeclaredValueUnsupported, docIdx)
		}
		if !jsonDocumentLooksObject(doc.Document) {
			return nil, true, fmt.Errorf("%w: document[%d] root is not object", ErrColumnDeclaredValueUnsupported, docIdx)
		}
		var stackValues [8]jsonParserIndexValue
		valuesRaw := stackValues[:]
		if len(cfg.Columns) > len(stackValues) {
			valuesRaw = make([]jsonParserIndexValue, len(cfg.Columns))
		} else {
			valuesRaw = valuesRaw[:len(cfg.Columns)]
		}
		if err := jsonparser.ObjectEach(doc.Document, func(key, value []byte, dataType jsonparser.ValueType, _ int) error {
			for colIdx, col := range cfg.Columns {
				if string(key) == col.Path {
					valuesRaw[colIdx] = jsonParserIndexValue{raw: value, valueType: dataType}
				}
			}
			return nil
		}); err != nil {
			return nil, true, fmt.Errorf("%w: document[%d] invalid JSON: %v", ErrColumnDeclaredValueUnsupported, docIdx, err)
		}
		values := make([]columnDeclaredValue, len(cfg.Columns))
		var scratch []byte
		for colIdx, col := range cfg.Columns {
			value, err := convertColumnDeclaredJSONParserValue(col, valuesRaw[colIdx], &scratch)
			if err != nil {
				return nil, true, fmt.Errorf("%w: document[%d] column[%d] %q: %v", ErrColumnDeclaredValueUnsupported, docIdx, colIdx, col.Name, err)
			}
			values[colIdx] = value
		}
		rows = append(rows, columnDeclaredRow{
			ID:     bytes.Clone(doc.ID),
			Values: values,
		})
	}
	return rows, true, nil
}

func columnDeclaredJSONParserValueSupported(valueType ColumnStoreValueType) bool {
	switch valueType {
	case ColumnStoreValueInt64, ColumnStoreValueString, ColumnStoreValueFloat32Vector:
		return true
	default:
		return false
	}
}

type columnDeclaredStringInterner struct {
	values []string
	lookup map[string]string
}

const columnDeclaredStringInternerLinearLimit = 16

func (i *columnDeclaredStringInterner) intern(value []byte) string {
	if i == nil {
		return string(value)
	}
	if len(value) == 0 {
		return ""
	}
	if i.lookup != nil {
		if interned, ok := i.lookup[string(value)]; ok {
			return interned
		}
		interned := string(value)
		i.lookup[interned] = interned
		return interned
	}
	for _, interned := range i.values {
		if interned == string(value) {
			return interned
		}
	}
	interned := string(value)
	i.values = append(i.values, interned)
	if len(i.values) > columnDeclaredStringInternerLinearLimit {
		i.lookup = make(map[string]string, len(i.values)*2)
		for _, value := range i.values {
			i.lookup[value] = value
		}
		i.values = nil
	}
	return interned
}

func convertColumnDeclaredJSONParserValue(col ColumnStoreColumn, raw jsonParserIndexValue, scratch *[]byte) (columnDeclaredValue, error) {
	return convertColumnDeclaredJSONParserValueWithStringInterner(col, raw, scratch, nil)
}

func convertColumnDeclaredJSONParserValueWithStringInterner(col ColumnStoreColumn, raw jsonParserIndexValue, scratch *[]byte, stringInterner *columnDeclaredStringInterner) (columnDeclaredValue, error) {
	exists := raw.valueType != jsonparser.NotExist
	value := columnDeclaredValue{Type: col.ValueType, Present: exists}
	if !exists || raw.valueType == jsonparser.Null {
		if col.Nullable {
			value.Null = true
			return value, nil
		}
		return columnDeclaredValue{}, errors.New("missing non-null declared value")
	}
	switch col.ValueType {
	case ColumnStoreValueInt64:
		v, err := parseColumnJSONParserInt64(raw)
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.Int64 = v
	case ColumnStoreValueString:
		if raw.valueType != jsonparser.String {
			return columnDeclaredValue{}, fmt.Errorf("expected string got %s", raw.valueType)
		}
		unescaped, err := columnJSONParserStringBytes(raw.raw, scratch)
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.String = stringInterner.intern(unescaped)
	case ColumnStoreValueFloat32Vector:
		values, err := convertColumnJSONParserFloat32Vector(raw, columnStoreFloat32VectorElementsPerRow(col))
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.Float32Vector = values
	default:
		return columnDeclaredValue{}, fmt.Errorf("unsupported declared value type %q", col.ValueType)
	}
	return value, nil
}

func columnJSONParserStringBytes(raw []byte, scratch *[]byte) ([]byte, error) {
	if bytes.IndexByte(raw, '\\') == -1 {
		return raw, nil
	}
	unescaped, err := jsonparser.Unescape(raw, (*scratch)[:0])
	if err != nil {
		return nil, err
	}
	*scratch = unescaped[:0]
	return unescaped, nil
}

func parseColumnJSONParserInt64(raw jsonParserIndexValue) (int64, error) {
	if raw.valueType != jsonparser.Number {
		return 0, fmt.Errorf("expected int64 number got %s", raw.valueType)
	}
	return strconv.ParseInt(string(raw.raw), 10, 64)
}

func convertColumnJSONParserFloat32Vector(raw jsonParserIndexValue, dims int) ([]float32, error) {
	if raw.valueType != jsonparser.Array {
		return nil, fmt.Errorf("expected float32_vector array got %s", raw.valueType)
	}
	if dims <= 0 {
		return nil, fmt.Errorf("invalid float32_vector dims %d", dims)
	}
	out := make([]float32, 0, dims)
	var parseErr error
	_, err := jsonparser.ArrayEach(raw.raw, func(elem []byte, dataType jsonparser.ValueType, idx int, elemErr error) {
		if parseErr != nil {
			return
		}
		if elemErr != nil {
			parseErr = fmt.Errorf("float32_vector[%d]: %w", idx, elemErr)
			return
		}
		if dataType != jsonparser.Number {
			parseErr = fmt.Errorf("float32_vector[%d] expected number got %s", idx, dataType)
			return
		}
		v, err := strconv.ParseFloat(string(elem), 32)
		if err != nil {
			parseErr = fmt.Errorf("float32_vector[%d]: %w", idx, err)
			return
		}
		out = append(out, float32(v))
	})
	if err != nil {
		return nil, err
	}
	if parseErr != nil {
		return nil, parseErr
	}
	if len(out) != dims {
		return nil, fmt.Errorf("float32_vector length=%d want dims=%d", len(out), dims)
	}
	return out, nil
}

func lookupColumnJSONPath(obj map[string]any, path string) (any, bool) {
	if path == "" {
		return nil, false
	}
	// Dotted column paths mirror collection/index path semantics: they traverse
	// nested objects and do not address literal top-level keys containing dots.
	if !strings.Contains(path, ".") {
		value, ok := obj[path]
		return value, ok
	}
	parts := strings.Split(path, ".")
	var current any = obj
	for _, part := range parts {
		next, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		value, ok := next[part]
		if !ok {
			return nil, false
		}
		current = value
	}
	return current, true
}

func convertColumnDeclaredValue(col ColumnStoreColumn, raw any, exists bool) (columnDeclaredValue, error) {
	value := columnDeclaredValue{Type: col.ValueType, Present: exists}
	if !exists || raw == nil {
		if col.Nullable {
			value.Null = true
			return value, nil
		}
		return columnDeclaredValue{}, errors.New("missing non-null declared value")
	}
	switch col.ValueType {
	case ColumnStoreValueBool:
		v, ok := raw.(bool)
		if !ok {
			return columnDeclaredValue{}, fmt.Errorf("expected bool got %T", raw)
		}
		value.Bool = v
	case ColumnStoreValueInt64:
		n, ok := raw.(json.Number)
		if !ok {
			return columnDeclaredValue{}, fmt.Errorf("expected int64 number got %T", raw)
		}
		v, err := n.Int64()
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.Int64 = v
	case ColumnStoreValueFloat32:
		n, ok := raw.(json.Number)
		if !ok {
			return columnDeclaredValue{}, fmt.Errorf("expected float32 number got %T", raw)
		}
		v, err := parseJSONFloat32(n)
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.Float32 = v
	case ColumnStoreValueDouble:
		n, ok := raw.(json.Number)
		if !ok {
			return columnDeclaredValue{}, fmt.Errorf("expected double number got %T", raw)
		}
		v, err := n.Float64()
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.Double = v
	case ColumnStoreValueString:
		v, ok := raw.(string)
		if !ok {
			return columnDeclaredValue{}, fmt.Errorf("expected string got %T", raw)
		}
		value.String = v
	case ColumnStoreValueInt8:
		v, err := convertJSONSignedScalar(raw, "int8", -128, 127)
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.Int8 = int8(v)
	case ColumnStoreValueUint8:
		v, err := convertJSONUnsignedScalar(raw, "uint8", 1<<8-1)
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.Uint8 = uint8(v)
	case ColumnStoreValueInt16:
		v, err := convertJSONSignedScalar(raw, "int16", -1<<15, 1<<15-1)
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.Int16 = int16(v)
	case ColumnStoreValueUint16:
		v, err := convertJSONUnsignedScalar(raw, "uint16", 1<<16-1)
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.Uint16 = uint16(v)
	case ColumnStoreValueInt32:
		v, err := convertJSONSignedScalar(raw, "int32", -1<<31, 1<<31-1)
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.Int32 = int32(v)
	case ColumnStoreValueUint32:
		v, err := convertJSONUnsignedScalar(raw, "uint32", 1<<32-1)
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.Uint32 = uint32(v)
	case ColumnStoreValueUint64:
		v, err := convertJSONUnsignedScalar(raw, "uint64", ^uint64(0))
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.Uint64 = v
	case ColumnStoreValueFloat16:
		v, err := convertJSONUnsignedScalar(raw, "float16", 1<<16-1)
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.Float16 = uint16(v)
	case ColumnStoreValueBFloat16:
		v, err := convertJSONUnsignedScalar(raw, "bfloat16", 1<<16-1)
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.BFloat16 = uint16(v)
	case ColumnStoreValueFloat32Vector:
		values, err := convertJSONFloat32Vector(raw, columnStoreFloat32VectorElementsPerRow(col))
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.Float32Vector = values
	case ColumnStoreValueUint8Vector, ColumnStoreValueInt8Vector, ColumnStoreValueUint16Vector, ColumnStoreValueInt16Vector, ColumnStoreValueUint32Vector, ColumnStoreValueInt32Vector, ColumnStoreValueUint64Vector, ColumnStoreValueInt64Vector, ColumnStoreValueFloat16Vector, ColumnStoreValueBFloat16Vector, ColumnStoreValueFloat64Vector:
		values, err := convertJSONDenseNumericVector(raw, col)
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.DenseNumericVector = values
	case ColumnStoreValueByteVector:
		values, err := convertJSONByteVector(raw, col)
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.Bytes = values
		value.BytesPerRow = col.BytesPerRow
	case ColumnStoreValuePackedBitVector, ColumnStoreValuePackedUint2Vector, ColumnStoreValuePackedUint4Vector:
		bitsPerElement, _ := columnStorePackedUintVectorBits(col.ValueType)
		values, err := convertJSONPackedUintVector(raw, col)
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.Bytes = values
		value.ElementsPerRow = col.ElementsPerRow
		value.BitsPerElement = bitsPerElement
	case ColumnStoreValueUint32List:
		values, err := convertJSONUint32List(raw, "uint32_list")
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.Uint32List = values
	case ColumnStoreValueBytes:
		values, err := convertJSONBytes(raw)
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.Bytes = values
	case ColumnStoreValueAdjacencyList:
		values, err := convertJSONUint32List(raw, "adjacency_list")
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.AdjacencyList = values
	default:
		return columnDeclaredValue{}, fmt.Errorf("unsupported declared value type %q", col.ValueType)
	}
	return value, nil
}

func parseJSONFloat32(n json.Number) (float32, error) {
	v, err := strconv.ParseFloat(n.String(), 32)
	if err != nil {
		return 0, err
	}
	return float32(v), nil
}

func convertJSONFloat32Vector(raw any, dims int) ([]float32, error) {
	values, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("expected float32_vector array got %T", raw)
	}
	if dims <= 0 {
		return nil, fmt.Errorf("invalid float32_vector dims %d", dims)
	}
	if len(values) != dims {
		return nil, fmt.Errorf("float32_vector length=%d want dims=%d", len(values), dims)
	}
	out := make([]float32, len(values))
	for i, rawValue := range values {
		n, ok := rawValue.(json.Number)
		if !ok {
			return nil, fmt.Errorf("float32_vector[%d] expected number got %T", i, rawValue)
		}
		v, err := parseJSONFloat32(n)
		if err != nil {
			return nil, fmt.Errorf("float32_vector[%d]: %w", i, err)
		}
		out[i] = v
	}
	return out, nil
}

func convertJSONUint32List(raw any, label string) ([]uint32, error) {
	values, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("expected %s array got %T", label, raw)
	}
	out := make([]uint32, len(values))
	for i, rawValue := range values {
		n, ok := rawValue.(json.Number)
		if !ok {
			return nil, fmt.Errorf("%s[%d] expected integer got %T", label, i, rawValue)
		}
		v, err := n.Int64()
		if err != nil {
			return nil, fmt.Errorf("%s[%d]: %w", label, i, err)
		}
		if v < 0 || v > int64(1<<32-1) {
			return nil, fmt.Errorf("%s[%d]=%d outside uint32 range", label, i, v)
		}
		out[i] = uint32(v)
	}
	return out, nil
}

func convertJSONBytes(raw any) ([]byte, error) {
	values, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("expected bytes array got %T", raw)
	}
	out := make([]byte, len(values))
	for i, rawValue := range values {
		n, ok := rawValue.(json.Number)
		if !ok {
			return nil, fmt.Errorf("bytes[%d] expected integer got %T", i, rawValue)
		}
		v, err := n.Int64()
		if err != nil {
			return nil, fmt.Errorf("bytes[%d]: %w", i, err)
		}
		if v < 0 || v > 255 {
			return nil, fmt.Errorf("bytes[%d]=%d outside byte range", i, v)
		}
		out[i] = byte(v)
	}
	return out, nil
}

func encodeColumnPhysicalAsset(input columnPhysicalAssetEncodeInput) ([]byte, columnPhysicalAssetSummary, error) {
	if input.Collection == "" || input.Namespace == "" || input.Generation == 0 || input.PartID == 0 {
		return nil, columnPhysicalAssetSummary{}, errors.New("collections: column physical asset missing collection, namespace, generation, or part_id")
	}
	if !isSupportedColumnPhysicalAssetOperation(input.Operation) {
		return nil, columnPhysicalAssetSummary{}, fmt.Errorf("collections: unsupported column physical asset operation %q", input.Operation)
	}
	for rowIdx, row := range input.Rows {
		switch input.Operation {
		case ColumnPublishOperationInsert, ColumnPublishOperationUpdate:
			if row.Deleted {
				return nil, columnPhysicalAssetSummary{}, fmt.Errorf("collections: column physical asset %s row[%d] is marked deleted", input.Operation, rowIdx)
			}
			if len(row.Values) != len(input.Columns) {
				return nil, columnPhysicalAssetSummary{}, fmt.Errorf("collections: column physical asset row[%d] values=%d columns=%d", rowIdx, len(row.Values), len(input.Columns))
			}
			for colIdx, value := range row.Values {
				if !value.Present && !value.Null {
					return nil, columnPhysicalAssetSummary{}, fmt.Errorf("collections: column physical asset row[%d] column[%d] absent value is not null", rowIdx, colIdx)
				}
				if value.Type != input.Columns[colIdx].ValueType {
					return nil, columnPhysicalAssetSummary{}, fmt.Errorf("collections: column physical asset row[%d] column[%d] type=%q want %q", rowIdx, colIdx, value.Type, input.Columns[colIdx].ValueType)
				}
				if !value.Present && !input.Columns[colIdx].Nullable {
					return nil, columnPhysicalAssetSummary{}, fmt.Errorf("collections: column physical asset row[%d] column[%d] is absent but column is not nullable", rowIdx, colIdx)
				}
				if value.Null && !input.Columns[colIdx].Nullable {
					return nil, columnPhysicalAssetSummary{}, fmt.Errorf("collections: column physical asset row[%d] column[%d] is null but column is not nullable", rowIdx, colIdx)
				}
				if !value.Null && value.Present {
					if err := validateColumnDeclaredPhysicalValueShape(input.Columns[colIdx], value); err != nil {
						return nil, columnPhysicalAssetSummary{}, fmt.Errorf("collections: column physical asset row[%d] column[%d]: %w", rowIdx, colIdx, err)
					}
				}
			}
		case ColumnPublishOperationDelete:
			if !row.Deleted {
				return nil, columnPhysicalAssetSummary{}, fmt.Errorf("collections: column physical asset delete row[%d] is not marked deleted", rowIdx)
			}
			if len(row.Values) != 0 {
				return nil, columnPhysicalAssetSummary{}, fmt.Errorf("collections: column physical asset delete row[%d] values=%d want 0", rowIdx, len(row.Values))
			}
		default:
			return nil, columnPhysicalAssetSummary{}, fmt.Errorf("collections: unsupported column physical asset operation %q", input.Operation)
		}
	}
	var b bytes.Buffer
	denseIDBase, useDenseIDRows := columnPhysicalAssetDenseBigEndianUint64RangeBase(input)
	fixedIDWidth, useFixedIDRows := columnPhysicalAssetFixedIDRowEncodingWidth(input)
	version, err := columnPhysicalAssetVersionForColumns(input.Columns)
	if err != nil {
		return nil, columnPhysicalAssetSummary{}, err
	}
	if useDenseIDRows {
		version = columnPhysicalAssetVersionV8
	} else if useFixedIDRows {
		version = columnPhysicalAssetVersionV7
	}
	writeManifestUint32(&b, columnPhysicalAssetMagic)
	writeManifestUint16(&b, version)
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
		writeManifestUint64(&b, uint64(col.VectorDims))
		if version >= columnPhysicalAssetVersionV6 {
			writeManifestUint64(&b, uint64(col.ElementsPerRow))
		}
		if version >= columnPhysicalAssetVersionV5 {
			writeManifestString(&b, string(col.FixedWidthEncoding))
		}
	}
	if useDenseIDRows {
		writeManifestString(&b, columnPhysicalAssetRowEncodingDenseIDRange)
		writeManifestUint64(&b, denseIDBase)
		payload := b.Bytes()
		return payload, columnPhysicalAssetSummary{
			RowCount:     len(input.Rows),
			ColumnCount:  len(input.Columns),
			PayloadBytes: int64(len(payload)),
		}, nil
	}
	if useFixedIDRows {
		writeManifestString(&b, columnPhysicalAssetRowEncodingFixedID)
		writeManifestUint64(&b, uint64(fixedIDWidth))
		for _, row := range input.Rows {
			_, _ = b.Write(row.ID)
		}
		payload := b.Bytes()
		return payload, columnPhysicalAssetSummary{
			RowCount:     len(input.Rows),
			ColumnCount:  len(input.Columns),
			PayloadBytes: int64(len(payload)),
		}, nil
	}
	for _, row := range input.Rows {
		writeManifestBytes(&b, row.ID)
		writeManifestBool(&b, row.Deleted)
		if row.Deleted {
			continue
		}
		for colIdx, value := range row.Values {
			col := input.Columns[colIdx]
			writeManifestString(&b, string(value.Type))
			writeManifestBool(&b, value.Null)
			writeManifestBool(&b, columnDeclaredValuePresentForEncode(value))
			if !columnDeclaredValuePresentForEncode(value) {
				continue
			}
			if value.Null {
				continue
			}
			switch value.Type {
			case ColumnStoreValueBool:
				writeManifestBool(&b, value.Bool)
			case ColumnStoreValueInt64:
				writeManifestUint64(&b, uint64(value.Int64))
			case ColumnStoreValueFloat32:
				writeManifestUint32(&b, math.Float32bits(value.Float32))
			case ColumnStoreValueDouble:
				writeManifestUint64(&b, math.Float64bits(value.Double))
			case ColumnStoreValueString:
				writeManifestString(&b, value.String)
			case ColumnStoreValueInt8:
				writeManifestUint8(&b, uint8(value.Int8))
			case ColumnStoreValueUint8:
				writeManifestUint8(&b, value.Uint8)
			case ColumnStoreValueInt16:
				writeManifestUint16(&b, uint16(value.Int16))
			case ColumnStoreValueUint16:
				writeManifestUint16(&b, value.Uint16)
			case ColumnStoreValueInt32:
				writeManifestUint32(&b, uint32(value.Int32))
			case ColumnStoreValueUint32:
				writeManifestUint32(&b, value.Uint32)
			case ColumnStoreValueUint64:
				writeManifestUint64(&b, value.Uint64)
			case ColumnStoreValueFloat16:
				writeManifestUint16(&b, value.Float16)
			case ColumnStoreValueBFloat16:
				writeManifestUint16(&b, value.BFloat16)
			case ColumnStoreValueFloat32Vector:
				if err := writeManifestFloat32SliceWithEncoding(&b, value.Float32Vector, col.FixedWidthEncoding); err != nil {
					return nil, columnPhysicalAssetSummary{}, fmt.Errorf("collections: column physical asset row value column[%d] float32_vector: %w", colIdx, err)
				}
			case ColumnStoreValueUint8Vector, ColumnStoreValueInt8Vector, ColumnStoreValueUint16Vector, ColumnStoreValueInt16Vector, ColumnStoreValueUint32Vector, ColumnStoreValueInt32Vector, ColumnStoreValueUint64Vector, ColumnStoreValueInt64Vector, ColumnStoreValueFloat16Vector, ColumnStoreValueBFloat16Vector, ColumnStoreValueFloat64Vector:
				writeManifestBytes(&b, value.DenseNumericVector)
			case ColumnStoreValueUint32List:
				if err := writeManifestUint32SliceWithEncoding(&b, value.Uint32List, col.FixedWidthEncoding); err != nil {
					return nil, columnPhysicalAssetSummary{}, fmt.Errorf("collections: column physical asset row value column[%d] uint32_list: %w", colIdx, err)
				}
			case ColumnStoreValueAdjacencyList:
				if err := writeManifestUint32SliceWithEncoding(&b, value.AdjacencyList, col.FixedWidthEncoding); err != nil {
					return nil, columnPhysicalAssetSummary{}, fmt.Errorf("collections: column physical asset row value column[%d] adjacency_list: %w", colIdx, err)
				}
			default:
				return nil, columnPhysicalAssetSummary{}, fmt.Errorf("collections: unsupported column physical value type %q", value.Type)
			}
		}
	}
	payload := b.Bytes()
	return payload, columnPhysicalAssetSummary{
		RowCount:     len(input.Rows),
		ColumnCount:  len(input.Columns),
		PayloadBytes: int64(len(payload)),
	}, nil
}

func decodeColumnPhysicalAsset(raw []byte) (columnPhysicalAsset, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnPhysicalAssetMagic {
		return columnPhysicalAsset{}, fmt.Errorf("collections: bad column physical asset magic=0x%08x", magic)
	}
	version := cur.u16()
	if !isSupportedColumnPhysicalAssetVersion(version) {
		return columnPhysicalAsset{}, fmt.Errorf("collections: unsupported column physical asset version=%d", version)
	}
	header := columnPhysicalAssetHeader{
		Collection:        cur.string(),
		Namespace:         cur.string(),
		Generation:        cur.u64(),
		PartID:            cur.u64(),
		AppliedCommandLSN: cur.u64(),
		Operation:         ColumnPublishOperation(cur.string()),
		SchemaHash:        cur.u64(),
	}
	columnCount := cur.u64()
	rowCount := cur.u64()
	if err := cur.err; err != nil {
		return columnPhysicalAsset{}, err
	}
	if columnCount > uint64(maxCollectionInt) || rowCount > uint64(maxCollectionInt) {
		return columnPhysicalAsset{}, errors.New("collections: column physical asset dimensions overflow int")
	}
	header.ColumnCount = int(columnCount)
	header.RowCount = int(rowCount)
	asset := columnPhysicalAsset{
		Header:  header,
		Columns: make([]ColumnStoreColumn, int(columnCount)),
		Rows:    make([]columnDeclaredRow, 0, int(rowCount)),
	}
	for i := range asset.Columns {
		asset.Columns[i] = ColumnStoreColumn{
			Name:       cur.string(),
			Path:       cur.string(),
			ValueType:  ColumnStoreValueType(cur.string()),
			Nullable:   cur.bool(),
			Dictionary: cur.bool(),
		}
		if version >= columnPhysicalAssetVersionV4 {
			vectorDims := cur.u64()
			if vectorDims > uint64(maxCollectionInt) {
				return columnPhysicalAsset{}, errors.New("collections: column physical asset vector_dims overflow int")
			}
			asset.Columns[i].VectorDims = int(vectorDims)
		}
		if version >= columnPhysicalAssetVersionV6 {
			elementsPerRow := cur.u64()
			if elementsPerRow > uint64(maxCollectionInt) {
				return columnPhysicalAsset{}, errors.New("collections: column physical asset elements_per_row overflow int")
			}
			asset.Columns[i].ElementsPerRow = int(elementsPerRow)
		}
		if version >= columnPhysicalAssetVersionV5 {
			asset.Columns[i].FixedWidthEncoding = ColumnFixedWidthEncoding(cur.string())
			if _, err := normalizeColumnFixedWidthEncoding(asset.Columns[i].FixedWidthEncoding); err != nil {
				return columnPhysicalAsset{}, fmt.Errorf("collections: column physical asset column[%d] fixed_width_encoding: %w", i, err)
			}
			if asset.Columns[i].FixedWidthEncoding != ColumnFixedWidthEncodingDefault && !columnStoreValueTypeSupportsFixedWidthEncoding(asset.Columns[i].ValueType) {
				return columnPhysicalAsset{}, fmt.Errorf("collections: column physical asset column[%d] fixed_width_encoding unsupported for value_type %q", i, asset.Columns[i].ValueType)
			}
			if asset.Columns[i].FixedWidthEncoding != ColumnFixedWidthEncodingDefault && columnStoreValueTypeHasScalarFixedWidthPayload(asset.Columns[i].ValueType) {
				return columnPhysicalAsset{}, fmt.Errorf("collections: column physical asset column[%d] scalar fixed_width_encoding for value_type %q is typed_column_part-only", i, asset.Columns[i].ValueType)
			}
		}
	}
	if version >= columnPhysicalAssetVersionV7 {
		rowEncoding := cur.string()
		if rowEncoding != columnPhysicalAssetRowEncodingFixedID && rowEncoding != columnPhysicalAssetRowEncodingDenseIDRange {
			return columnPhysicalAsset{}, fmt.Errorf("collections: unsupported column physical asset row encoding %q", rowEncoding)
		}
		if len(asset.Columns) != 0 {
			return columnPhysicalAsset{}, fmt.Errorf("collections: column physical asset row encoding %q requires zero columns", rowEncoding)
		}
		deleted := asset.Header.Operation == ColumnPublishOperationDelete
		if asset.Header.Operation != ColumnPublishOperationInsert && asset.Header.Operation != ColumnPublishOperationUpdate && asset.Header.Operation != ColumnPublishOperationDelete {
			return columnPhysicalAsset{}, fmt.Errorf("collections: unsupported column physical asset operation %q", asset.Header.Operation)
		}
		switch rowEncoding {
		case columnPhysicalAssetRowEncodingFixedID:
			idWidth := cur.u64()
			if idWidth == 0 || idWidth > uint64(maxCollectionInt) {
				return columnPhysicalAsset{}, fmt.Errorf("collections: column physical asset fixed id width=%d invalid", idWidth)
			}
			for rowIdx := 0; rowIdx < int(rowCount); rowIdx++ {
				if uint64(len(raw)-cur.pos) < idWidth {
					return columnPhysicalAsset{}, errors.New("collections: short column physical asset fixed row id block")
				}
				row := columnDeclaredRow{
					ID:      bytes.Clone(raw[cur.pos : cur.pos+int(idWidth)]),
					Deleted: deleted,
				}
				cur.pos += int(idWidth)
				asset.Rows = append(asset.Rows, row)
			}
		case columnPhysicalAssetRowEncodingDenseIDRange:
			if version < columnPhysicalAssetVersionV8 {
				return columnPhysicalAsset{}, fmt.Errorf("collections: column physical asset row encoding %q requires version >= %d", rowEncoding, columnPhysicalAssetVersionV8)
			}
			baseID := cur.u64()
			if rowCount > 0 && baseID > ^uint64(0)-uint64(rowCount-1) {
				return columnPhysicalAsset{}, errors.New("collections: column physical asset dense id range overflows uint64")
			}
			for rowIdx := 0; rowIdx < int(rowCount); rowIdx++ {
				row := columnDeclaredRow{
					ID:      columnPhysicalAssetBigEndianUint64ID(baseID + uint64(rowIdx)),
					Deleted: deleted,
				}
				asset.Rows = append(asset.Rows, row)
			}
		}
		if cur.err != nil {
			return columnPhysicalAsset{}, cur.err
		}
		if cur.pos != len(raw) {
			return columnPhysicalAsset{}, errors.New("collections: trailing bytes in column physical asset")
		}
		return asset, nil
	}
	for rowIdx := 0; rowIdx < int(rowCount); rowIdx++ {
		row := columnDeclaredRow{
			ID: cur.bytes(),
		}
		if version >= columnPhysicalAssetVersionV2 {
			row.Deleted = cur.bool()
		}
		if !row.Deleted {
			row.Values = make([]columnDeclaredValue, int(columnCount))
			for colIdx := 0; colIdx < int(columnCount); colIdx++ {
				value := columnDeclaredValue{
					Type: ColumnStoreValueType(cur.string()),
					Null: cur.bool(),
				}
				if version >= columnPhysicalAssetVersionV3 {
					value.Present = cur.bool()
				} else {
					value.Present = true
				}
				if !value.Present {
					if !value.Null {
						return columnPhysicalAsset{}, errors.New("collections: column physical asset absent value must be null")
					}
					row.Values[colIdx] = value
					continue
				}
				if !value.Null {
					switch value.Type {
					case ColumnStoreValueBool:
						value.Bool = cur.bool()
					case ColumnStoreValueInt64:
						value.Int64 = int64(cur.u64())
					case ColumnStoreValueFloat32:
						value.Float32 = math.Float32frombits(cur.u32())
					case ColumnStoreValueDouble:
						value.Double = math.Float64frombits(cur.u64())
					case ColumnStoreValueString:
						value.String = cur.string()
					case ColumnStoreValueInt8:
						value.Int8 = int8(cur.u8())
					case ColumnStoreValueUint8:
						value.Uint8 = cur.u8()
					case ColumnStoreValueInt16:
						value.Int16 = int16(cur.u16())
					case ColumnStoreValueUint16:
						value.Uint16 = cur.u16()
					case ColumnStoreValueInt32:
						value.Int32 = int32(cur.u32())
					case ColumnStoreValueUint32:
						value.Uint32 = cur.u32()
					case ColumnStoreValueUint64:
						value.Uint64 = cur.u64()
					case ColumnStoreValueFloat16:
						value.Float16 = cur.u16()
					case ColumnStoreValueBFloat16:
						value.BFloat16 = cur.u16()
					case ColumnStoreValueFloat32Vector:
						if version >= columnPhysicalAssetVersionV4 {
							value.Float32Vector = cur.float32SliceWithExpectedLengthAndEncoding(columnStoreFloat32VectorElementsPerRow(asset.Columns[colIdx]), asset.Columns[colIdx].FixedWidthEncoding)
						} else {
							value.Float32Vector = cur.float32Slice()
						}
					case ColumnStoreValueUint8Vector, ColumnStoreValueInt8Vector, ColumnStoreValueUint16Vector, ColumnStoreValueInt16Vector, ColumnStoreValueUint32Vector, ColumnStoreValueInt32Vector, ColumnStoreValueUint64Vector, ColumnStoreValueInt64Vector, ColumnStoreValueFloat16Vector, ColumnStoreValueBFloat16Vector, ColumnStoreValueFloat64Vector:
						value.DenseNumericVector = cur.denseNumericVectorBytesWithExpectedLength(asset.Columns[colIdx])
					case ColumnStoreValueUint32List:
						value.Uint32List = cur.uint32SliceWithEncoding(asset.Columns[colIdx].FixedWidthEncoding)
					case ColumnStoreValueAdjacencyList:
						value.AdjacencyList = cur.uint32SliceWithEncoding(asset.Columns[colIdx].FixedWidthEncoding)
					default:
						return columnPhysicalAsset{}, fmt.Errorf("collections: unsupported column physical value type %q", value.Type)
					}
				}
				row.Values[colIdx] = value
			}
		}
		asset.Rows = append(asset.Rows, row)
	}
	if cur.err != nil {
		return columnPhysicalAsset{}, cur.err
	}
	if cur.pos != len(raw) {
		return columnPhysicalAsset{}, errors.New("collections: trailing bytes in column physical asset")
	}
	return asset, nil
}

func columnPhysicalAssetColumnMatchesManifestConfig(got, want ColumnStoreColumn) (bool, error) {
	gotOwner, err := columnStoreColumnOwner(got)
	if err != nil {
		return false, fmt.Errorf("decoded owner: %w", err)
	}
	wantOwner, err := columnStoreColumnOwner(want)
	if err != nil {
		return false, fmt.Errorf("manifest owner: %w", err)
	}
	if gotOwner != TypedStorageOwnerRowAsset || wantOwner != TypedStorageOwnerRowAsset {
		return false, nil
	}
	// TCPA row assets predate typed-storage owner metadata and do not encode an
	// owner field. Treat the legacy zero owner and explicit typed_row_asset owner
	// as the same physical schema while still requiring all encoded fields to
	// match exactly.
	got.Owner = ""
	want.Owner = ""
	return got == want, nil
}

func validateColumnPhysicalAssetForManifest(raw []byte, ref ColumnAssetRef, cfg ColumnStoreConfig) error {
	cfg = columnStoreRowAssetConfig(cfg)
	if err := validateColumnAssetRefForPlan(ref); err != nil {
		return err
	}
	if int64(len(raw)) != ref.Length {
		return fmt.Errorf("collections: column physical asset length=%d does not match ref length=%d", len(raw), ref.Length)
	}
	if checksum := page.Checksum(raw); checksum != ref.Checksum {
		return fmt.Errorf("collections: column physical asset checksum=%d does not match ref checksum=%d", checksum, ref.Checksum)
	}
	asset, err := decodeColumnPhysicalAsset(raw)
	if err != nil {
		return err
	}
	if cfg.AssetManager == nil {
		return errors.New("collections: column physical asset validation requires asset manager")
	}
	if asset.Header.Namespace != cfg.AssetManager.Namespace || ref.Namespace != cfg.AssetManager.Namespace {
		return fmt.Errorf("collections: column physical asset namespace=%q ref_namespace=%q want %q", asset.Header.Namespace, ref.Namespace, cfg.AssetManager.Namespace)
	}
	if asset.Header.Generation != ref.Generation {
		return fmt.Errorf("collections: column physical asset generation=%d does not match ref generation=%d", asset.Header.Generation, ref.Generation)
	}
	if asset.Header.PartID != ref.PartID {
		return fmt.Errorf("collections: column physical asset part_id=%d does not match ref part_id=%d", asset.Header.PartID, ref.PartID)
	}
	if asset.Header.SchemaHash != cfg.SchemaHash {
		return fmt.Errorf("collections: column physical asset schema_hash=%d want %d", asset.Header.SchemaHash, cfg.SchemaHash)
	}
	if len(asset.Columns) != len(cfg.Columns) {
		return fmt.Errorf("collections: column physical asset columns=%d want %d", len(asset.Columns), len(cfg.Columns))
	}
	for i := range cfg.Columns {
		match, err := columnPhysicalAssetColumnMatchesManifestConfig(asset.Columns[i], cfg.Columns[i])
		if err != nil {
			return fmt.Errorf("collections: column physical asset column[%d]: %w", i, err)
		}
		if !match {
			return fmt.Errorf("collections: column physical asset column[%d]=%+v want %+v", i, asset.Columns[i], cfg.Columns[i])
		}
	}
	if asset.Header.RowCount != len(asset.Rows) {
		return fmt.Errorf("collections: column physical asset row_count=%d rows=%d", asset.Header.RowCount, len(asset.Rows))
	}
	if !isSupportedColumnPhysicalAssetOperation(asset.Header.Operation) {
		return fmt.Errorf("collections: unsupported column physical asset operation %q", asset.Header.Operation)
	}
	for rowIdx, row := range asset.Rows {
		switch asset.Header.Operation {
		case ColumnPublishOperationInsert, ColumnPublishOperationUpdate:
			if row.Deleted {
				return fmt.Errorf("collections: column physical asset %s row[%d] is marked deleted", asset.Header.Operation, rowIdx)
			}
		case ColumnPublishOperationDelete:
			if !row.Deleted {
				return fmt.Errorf("collections: column physical asset delete row[%d] is not marked deleted", rowIdx)
			}
			if len(row.Values) != 0 {
				return fmt.Errorf("collections: column physical asset delete row[%d] values=%d want 0", rowIdx, len(row.Values))
			}
			continue
		default:
			return fmt.Errorf("collections: unsupported column physical asset operation %q", asset.Header.Operation)
		}
		if len(row.Values) != len(cfg.Columns) {
			return fmt.Errorf("collections: column physical asset row[%d] values=%d want %d", rowIdx, len(row.Values), len(cfg.Columns))
		}
		for colIdx, value := range row.Values {
			if value.Type != cfg.Columns[colIdx].ValueType {
				return fmt.Errorf("collections: column physical asset row[%d] column[%d] type=%q want %q", rowIdx, colIdx, value.Type, cfg.Columns[colIdx].ValueType)
			}
			if !value.Present && !value.Null {
				return fmt.Errorf("collections: column physical asset row[%d] column[%d] absent value is not null", rowIdx, colIdx)
			}
			if !value.Present && !cfg.Columns[colIdx].Nullable {
				return fmt.Errorf("collections: column physical asset row[%d] column[%d] is absent but column is not nullable", rowIdx, colIdx)
			}
			if value.Null && !cfg.Columns[colIdx].Nullable {
				return fmt.Errorf("collections: column physical asset row[%d] column[%d] is null but column is not nullable", rowIdx, colIdx)
			}
			if !value.Null && value.Present {
				if err := validateColumnDeclaredPhysicalValueShape(cfg.Columns[colIdx], value); err != nil {
					return fmt.Errorf("collections: column physical asset row[%d] column[%d]: %w", rowIdx, colIdx, err)
				}
			}
		}
	}
	return nil
}

func isSupportedColumnPhysicalAssetVersion(version uint16) bool {
	switch version {
	case columnPhysicalAssetVersionV1, columnPhysicalAssetVersionV2, columnPhysicalAssetVersionV3, columnPhysicalAssetVersionV4, columnPhysicalAssetVersionV5, columnPhysicalAssetVersionV6, columnPhysicalAssetVersionV7, columnPhysicalAssetVersionV8:
		return true
	default:
		return false
	}
}

func columnPhysicalAssetFixedIDRowEncodingWidth(input columnPhysicalAssetEncodeInput) (int, bool) {
	if len(input.Columns) != 0 || len(input.Rows) == 0 {
		return 0, false
	}
	if !isSupportedColumnPhysicalAssetOperation(input.Operation) {
		return 0, false
	}
	deleted := input.Operation == ColumnPublishOperationDelete
	width := len(input.Rows[0].ID)
	if width == 0 || input.Rows[0].Deleted != deleted {
		return 0, false
	}
	for _, row := range input.Rows[1:] {
		if len(row.ID) != width || row.Deleted != deleted {
			return 0, false
		}
	}
	return width, true
}

func columnPhysicalAssetDenseBigEndianUint64RangeBase(input columnPhysicalAssetEncodeInput) (uint64, bool) {
	if len(input.Columns) != 0 || len(input.Rows) == 0 {
		return 0, false
	}
	if !isSupportedColumnPhysicalAssetOperation(input.Operation) {
		return 0, false
	}
	deleted := input.Operation == ColumnPublishOperationDelete
	base, ok := columnPhysicalAssetParseBigEndianUint64ID(input.Rows[0].ID)
	if !ok || input.Rows[0].Deleted != deleted {
		return 0, false
	}
	if len(input.Rows) > 1 && base > ^uint64(0)-uint64(len(input.Rows)-1) {
		return 0, false
	}
	for i, row := range input.Rows[1:] {
		value, ok := columnPhysicalAssetParseBigEndianUint64ID(row.ID)
		if !ok || row.Deleted != deleted || value != base+uint64(i+1) {
			return 0, false
		}
	}
	return base, true
}

func columnPhysicalAssetParseBigEndianUint64ID(id []byte) (uint64, bool) {
	if len(id) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(id), true
}

func columnPhysicalAssetBigEndianUint64ID(value uint64) []byte {
	id := make([]byte, 8)
	binary.BigEndian.PutUint64(id, value)
	return id
}

func columnPhysicalAssetVersionForColumns(columns []ColumnStoreColumn) (uint16, error) {
	requiresV5 := false
	requiresV6 := false
	for i, col := range columns {
		encoding, err := normalizeColumnFixedWidthEncoding(col.FixedWidthEncoding)
		if err != nil {
			return 0, fmt.Errorf("collections: column physical asset column[%d] fixed_width_encoding: %w", i, err)
		}
		if col.ElementsPerRow != 0 || columnStoreValueTypeIsDenseNumericVector(col.ValueType) {
			requiresV6 = true
		}
		if encoding != ColumnFixedWidthEncodingDefault {
			if !columnStoreValueTypeSupportsFixedWidthEncoding(col.ValueType) {
				return 0, fmt.Errorf("collections: column physical asset column[%d] fixed_width_encoding unsupported for value_type %q", i, col.ValueType)
			}
			if columnStoreValueTypeHasScalarFixedWidthPayload(col.ValueType) {
				return 0, fmt.Errorf("collections: column physical asset column[%d] scalar fixed_width_encoding for value_type %q is typed_column_part-only", i, col.ValueType)
			}
			requiresV5 = true
		}
	}
	if requiresV6 {
		return columnPhysicalAssetVersionV6, nil
	}
	if requiresV5 {
		return columnPhysicalAssetVersionV5, nil
	}
	return columnPhysicalAssetVersion, nil
}

func validateColumnDeclaredPhysicalValueShape(col ColumnStoreColumn, value columnDeclaredValue) error {
	switch col.ValueType {
	case ColumnStoreValueFloat32Vector:
		dims := columnStoreFloat32VectorElementsPerRow(col)
		if dims <= 0 {
			return fmt.Errorf("float32_vector column has invalid vector_dims/elements_per_row=%d/%d", col.VectorDims, col.ElementsPerRow)
		}
		if len(value.Float32Vector) != dims {
			return fmt.Errorf("float32_vector length=%d want vector_dims=%d", len(value.Float32Vector), dims)
		}
	case ColumnStoreValueUint8Vector, ColumnStoreValueInt8Vector, ColumnStoreValueUint16Vector, ColumnStoreValueInt16Vector, ColumnStoreValueUint32Vector, ColumnStoreValueInt32Vector, ColumnStoreValueUint64Vector, ColumnStoreValueInt64Vector, ColumnStoreValueFloat16Vector, ColumnStoreValueBFloat16Vector, ColumnStoreValueFloat64Vector:
		want, err := columnStoreDenseNumericVectorRowBytes(col)
		if err != nil {
			return err
		}
		if len(value.DenseNumericVector) != want {
			return fmt.Errorf("%s bytes=%d want elements_per_row=%d bytes=%d", col.ValueType, len(value.DenseNumericVector), col.ElementsPerRow, want)
		}
	case ColumnStoreValueUint32List:
		if !value.Present || value.Null {
			return fmt.Errorf("uint32_list is non-null in v1")
		}
	case ColumnStoreValueBytes:
		if !value.Present || value.Null {
			return fmt.Errorf("bytes is non-null in v1")
		}
	case ColumnStoreValueAdjacencyList:
		if col.AdjacencyDegree > 0 && len(value.AdjacencyList) != col.AdjacencyDegree {
			return fmt.Errorf("adjacency_list length=%d want adjacency_degree=%d", len(value.AdjacencyList), col.AdjacencyDegree)
		}
	}
	return nil
}

func columnDeclaredValuePresentForEncode(value columnDeclaredValue) bool {
	return value.Present
}

func isSupportedColumnPhysicalAssetOperation(operation ColumnPublishOperation) bool {
	switch operation {
	case ColumnPublishOperationInsert, ColumnPublishOperationUpdate, ColumnPublishOperationDelete:
		return true
	default:
		return false
	}
}

func writeManifestBool(b *bytes.Buffer, value bool) {
	if value {
		_ = b.WriteByte(1)
		return
	}
	_ = b.WriteByte(0)
}

func writeManifestUint8(b *bytes.Buffer, value uint8) {
	_ = b.WriteByte(value)
}

func writeManifestBytes(b *bytes.Buffer, value []byte) {
	writeManifestUint64(b, uint64(len(value)))
	_, _ = b.Write(value)
}

func writeManifestFloat32Slice(b *bytes.Buffer, values []float32) {
	_ = writeManifestFloat32SliceWithEncoding(b, values, ColumnFixedWidthEncodingDefault)
}

func writeManifestFloat32SliceWithEncoding(b *bytes.Buffer, values []float32, encoding ColumnFixedWidthEncoding) error {
	littleEndian, err := columnFixedWidthEncodingIsLittleEndian(encoding)
	if err != nil {
		return err
	}
	writeManifestUint64(b, uint64(len(values)))
	var buf [4]byte
	if littleEndian {
		for _, value := range values {
			binary.LittleEndian.PutUint32(buf[:], math.Float32bits(value))
			_, _ = b.Write(buf[:])
		}
		return nil
	}
	for _, value := range values {
		binary.BigEndian.PutUint32(buf[:], math.Float32bits(value))
		_, _ = b.Write(buf[:])
	}
	return nil
}

func writeManifestUint32Slice(b *bytes.Buffer, values []uint32) {
	_ = writeManifestUint32SliceWithEncoding(b, values, ColumnFixedWidthEncodingDefault)
}

func writeManifestUint32SliceWithEncoding(b *bytes.Buffer, values []uint32, encoding ColumnFixedWidthEncoding) error {
	littleEndian, err := columnFixedWidthEncodingIsLittleEndian(encoding)
	if err != nil {
		return err
	}
	writeManifestUint64(b, uint64(len(values)))
	var buf [4]byte
	if littleEndian {
		for _, value := range values {
			binary.LittleEndian.PutUint32(buf[:], value)
			_, _ = b.Write(buf[:])
		}
		return nil
	}
	for _, value := range values {
		binary.BigEndian.PutUint32(buf[:], value)
		_, _ = b.Write(buf[:])
	}
	return nil
}

func (c *manifestCursor) bool() bool {
	if c.err != nil {
		return false
	}
	if len(c.raw)-c.pos < 1 {
		c.err = errors.New("collections: short column binary bool")
		return false
	}
	value := c.raw[c.pos] != 0
	c.pos++
	return value
}

func (c *manifestCursor) u8() uint8 {
	if c.err != nil {
		return 0
	}
	if len(c.raw)-c.pos < 1 {
		c.err = errors.New("collections: short column binary uint8")
		return 0
	}
	value := c.raw[c.pos]
	c.pos++
	return value
}

func (c *manifestCursor) bytes() []byte {
	value := c.bytesView()
	if c.err != nil {
		return nil
	}
	return bytes.Clone(value)
}

func (c *manifestCursor) bytesView() []byte {
	if c.err != nil {
		return nil
	}
	n := c.u64()
	if c.err != nil {
		return nil
	}
	if n > uint64(len(c.raw)-c.pos) {
		c.err = errors.New("collections: short column binary bytes")
		return nil
	}
	value := c.raw[c.pos : c.pos+int(n)]
	c.pos += int(n)
	return value
}

func (c *manifestCursor) float32Slice() []float32 {
	n := c.u64()
	if c.err != nil {
		return nil
	}
	return c.float32SliceAfterLength(n)
}

func (c *manifestCursor) float32SliceWithExpectedLength(expected int) []float32 {
	return c.float32SliceWithExpectedLengthAndEncoding(expected, ColumnFixedWidthEncodingDefault)
}

func (c *manifestCursor) float32SliceWithExpectedLengthAndEncoding(expected int, encoding ColumnFixedWidthEncoding) []float32 {
	n := c.u64()
	if c.err != nil {
		return nil
	}
	if expected < 0 || n != uint64(expected) {
		c.err = fmt.Errorf("collections: float32_vector length=%d want vector_dims=%d", n, expected)
		return nil
	}
	return c.float32SliceAfterLengthWithEncoding(n, encoding)
}

func (c *manifestCursor) skipFloat32SliceWithExpectedLength(expected int) uint64 {
	n := c.u64()
	if c.err != nil {
		return 0
	}
	if expected < 0 || n != uint64(expected) {
		c.err = fmt.Errorf("collections: float32_vector length=%d want vector_dims=%d", n, expected)
		return 0
	}
	byteLen, ok := c.fixedWidthSliceByteLen(n, 4, "float32_vector")
	if !ok {
		return 0
	}
	c.pos += int(byteLen)
	return n
}

func (c *manifestCursor) float32SliceAfterLength(n uint64) []float32 {
	return c.float32SliceAfterLengthWithEncoding(n, ColumnFixedWidthEncodingDefault)
}

func (c *manifestCursor) float32SliceAfterLengthWithEncoding(n uint64, encoding ColumnFixedWidthEncoding) []float32 {
	if c.err != nil {
		return nil
	}
	_, ok := c.fixedWidthSliceByteLen(n, 4, "float32_vector")
	if !ok {
		return nil
	}
	littleEndian, err := columnFixedWidthEncodingIsLittleEndian(encoding)
	if err != nil {
		c.err = fmt.Errorf("collections: unsupported fixed_width_encoding %q", encoding)
		return nil
	}
	out := make([]float32, int(n))
	for i := range out {
		var bits uint32
		if littleEndian {
			bits = binary.LittleEndian.Uint32(c.raw[c.pos:])
		} else {
			bits = binary.BigEndian.Uint32(c.raw[c.pos:])
		}
		out[i] = math.Float32frombits(bits)
		c.pos += 4
	}
	return out
}

func (c *manifestCursor) denseNumericVectorBytesWithExpectedLength(col ColumnStoreColumn) []byte {
	value := c.denseNumericVectorBytesViewWithExpectedLength(col)
	if c.err != nil {
		return nil
	}
	return bytes.Clone(value)
}

func (c *manifestCursor) denseNumericVectorBytesViewWithExpectedLength(col ColumnStoreColumn) []byte {
	value := c.bytesView()
	if c.err != nil {
		return nil
	}
	want, err := columnStoreDenseNumericVectorRowBytes(col)
	if err != nil {
		c.err = err
		return nil
	}
	if len(value) != want {
		c.err = fmt.Errorf("collections: %s bytes=%d want elements_per_row=%d bytes=%d", col.ValueType, len(value), col.ElementsPerRow, want)
		return nil
	}
	return value
}

func (c *manifestCursor) skipDenseNumericVectorBytesWithExpectedLength(col ColumnStoreColumn) {
	_ = c.denseNumericVectorBytesViewWithExpectedLength(col)
}

func columnStoreDenseNumericVectorRowBytes(col ColumnStoreColumn) (int, error) {
	width, ok := columnStoreDenseNumericVectorWidth(col.ValueType)
	if !ok {
		return 0, fmt.Errorf("collections: unsupported dense numeric vector value_type=%s", col.ValueType)
	}
	if col.ElementsPerRow <= 0 {
		return 0, fmt.Errorf("collections: %s column has invalid elements_per_row=%d", col.ValueType, col.ElementsPerRow)
	}
	if col.ElementsPerRow > maxCollectionInt/width {
		return 0, fmt.Errorf("collections: %s bytes overflow elements_per_row=%d width=%d", col.ValueType, col.ElementsPerRow, width)
	}
	return col.ElementsPerRow * width, nil
}

func (c *manifestCursor) uint32Slice() []uint32 {
	return c.uint32SliceWithEncoding(ColumnFixedWidthEncodingDefault)
}

func (c *manifestCursor) uint32SliceWithEncoding(encoding ColumnFixedWidthEncoding) []uint32 {
	n := c.u64()
	if c.err != nil {
		return nil
	}
	_, ok := c.fixedWidthSliceByteLen(n, 4, "uint32 slice")
	if !ok {
		return nil
	}
	littleEndian, err := columnFixedWidthEncodingIsLittleEndian(encoding)
	if err != nil {
		c.err = fmt.Errorf("collections: unsupported fixed_width_encoding %q", encoding)
		return nil
	}
	out := make([]uint32, int(n))
	for i := range out {
		if littleEndian {
			out[i] = binary.LittleEndian.Uint32(c.raw[c.pos:])
		} else {
			out[i] = binary.BigEndian.Uint32(c.raw[c.pos:])
		}
		c.pos += 4
	}
	return out
}

func (c *manifestCursor) skipUint32Slice() uint64 {
	n := c.u64()
	if c.err != nil {
		return 0
	}
	byteLen, ok := c.fixedWidthSliceByteLen(n, 4, "uint32 slice")
	if !ok {
		return 0
	}
	c.pos += int(byteLen)
	return n
}

func (c *manifestCursor) fixedWidthSliceByteLen(n uint64, elemBytes uint64, label string) (uint64, bool) {
	if n > uint64(maxCollectionInt) {
		c.err = fmt.Errorf("collections: %s length overflows int", label)
		return 0, false
	}
	if elemBytes == 0 || n > uint64(len(c.raw)-c.pos)/elemBytes {
		c.err = fmt.Errorf("collections: short column binary %s", label)
		return 0, false
	}
	return n * elemBytes, true
}
