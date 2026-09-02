package collections

import (
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// errTypedColumnProductionLayoutUnsupported is returned when a typed_column_part
// field asks the production adapter to publish or read a codec/layout that is
// not enabled for the durable production path. The typedcolumn package may know
// about more enum values than production currently allows; this guard keeps that
// boundary explicit and fail-closed.
var errTypedColumnProductionLayoutUnsupported = errors.New("collections: typed-column production layout unsupported")

func typedColumnProductionDefinitionForField(field TypedStorageField) (typedcolumn.ColumnDefinition, error) {
	mapping, err := typedColumnAdapterMappingForValueType(field.ValueType)
	if err != nil {
		return typedcolumn.ColumnDefinition{}, err
	}
	if field.FixedWidthEncoding != "" {
		switch field.ValueType {
		case ColumnStoreValueInt8, ColumnStoreValueUint8, ColumnStoreValueInt16, ColumnStoreValueUint16, ColumnStoreValueInt32, ColumnStoreValueUint32, ColumnStoreValueUint64, ColumnStoreValueFloat16, ColumnStoreValueBFloat16:
			if field.FixedWidthEncoding != ColumnFixedWidthEncodingLittleEndian {
				return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: unsupported %s fixed_width_encoding=%q", errTypedColumnAdapterUnsupportedType, field.ValueType, field.FixedWidthEncoding)
			}
			if field.Nullable {
				return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: nullable %s raw fixed-width encoding is unsupported", errTypedColumnAdapterUnsupportedType, field.ValueType)
			}
		case ColumnStoreValueInt64:
			if field.FixedWidthEncoding != ColumnFixedWidthEncodingLittleEndian {
				return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: unsupported int64 fixed_width_encoding=%q", errTypedColumnAdapterUnsupportedType, field.FixedWidthEncoding)
			}
			if field.Nullable {
				return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: nullable int64 raw fixed-width encoding is unsupported", errTypedColumnAdapterUnsupportedType)
			}
			mapping.Encoding = typedcolumn.EncodingRawInt64
		case ColumnStoreValueFloat32:
			if field.FixedWidthEncoding != ColumnFixedWidthEncodingLittleEndian {
				return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: unsupported float32 fixed_width_encoding=%q", errTypedColumnAdapterUnsupportedType, field.FixedWidthEncoding)
			}
			if field.Nullable {
				return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: nullable float32 raw fixed-width encoding is unsupported", errTypedColumnAdapterUnsupportedType)
			}
			mapping.ColumnType = typedcolumn.ColumnTypeFloat32
			mapping.Encoding = typedcolumn.EncodingRawFloat32
		case ColumnStoreValueDouble:
			if field.FixedWidthEncoding != ColumnFixedWidthEncodingLittleEndian {
				return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: unsupported double fixed_width_encoding=%q", errTypedColumnAdapterUnsupportedType, field.FixedWidthEncoding)
			}
			if field.Nullable {
				return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: nullable double raw fixed-width encoding is unsupported", errTypedColumnAdapterUnsupportedType)
			}
			mapping.ColumnType = typedcolumn.ColumnTypeFloat64
			mapping.Encoding = typedcolumn.EncodingRawFloat64
		case ColumnStoreValueFloat32Vector, ColumnStoreValueAdjacencyList,
			ColumnStoreValueUint8Vector, ColumnStoreValueInt8Vector,
			ColumnStoreValueUint16Vector, ColumnStoreValueInt16Vector,
			ColumnStoreValueUint32Vector, ColumnStoreValueInt32Vector,
			ColumnStoreValueUint64Vector, ColumnStoreValueInt64Vector,
			ColumnStoreValueFloat16Vector, ColumnStoreValueBFloat16Vector,
			ColumnStoreValueFloat64Vector:
			if field.FixedWidthEncoding != ColumnFixedWidthEncodingLittleEndian {
				return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: unsupported %s fixed_width_encoding=%q", errTypedColumnAdapterUnsupportedType, field.ValueType, field.FixedWidthEncoding)
			}
		default:
			return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: fixed_width_encoding is unsupported for value_type=%s", errTypedColumnAdapterUnsupportedType, field.ValueType)
		}
	}
	if field.Nullable {
		switch field.ValueType {
		case ColumnStoreValueBool, ColumnStoreValueInt64, ColumnStoreValueFloat32, ColumnStoreValueDouble, ColumnStoreValueString:
			mapping.Encoding = typedcolumn.EncodingNullableInt64
		case ColumnStoreValueFloat32Vector, ColumnStoreValueUint32List, ColumnStoreValueBytes, ColumnStoreValueAdjacencyList,
			ColumnStoreValueByteVector, ColumnStoreValuePackedBitVector, ColumnStoreValuePackedUint2Vector, ColumnStoreValuePackedUint4Vector,
			ColumnStoreValueInt8, ColumnStoreValueUint8, ColumnStoreValueInt16, ColumnStoreValueUint16, ColumnStoreValueInt32, ColumnStoreValueUint32, ColumnStoreValueUint64, ColumnStoreValueFloat16, ColumnStoreValueBFloat16,
			ColumnStoreValueUint8Vector, ColumnStoreValueInt8Vector, ColumnStoreValueUint16Vector, ColumnStoreValueInt16Vector, ColumnStoreValueUint32Vector, ColumnStoreValueInt32Vector, ColumnStoreValueUint64Vector, ColumnStoreValueInt64Vector, ColumnStoreValueFloat16Vector, ColumnStoreValueBFloat16Vector, ColumnStoreValueFloat64Vector:
			return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: nullable %s typed-column fields are not supported", errTypedColumnAdapterUnsupportedType, field.ValueType)
		default:
			return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: nullable %s", errTypedColumnAdapterUnsupportedType, field.ValueType)
		}
	}
	name, err := typedColumnProductionFieldName(field)
	if err != nil {
		return typedcolumn.ColumnDefinition{}, err
	}
	def := typedcolumn.ColumnDefinition{
		Name:           name,
		Type:           mapping.ColumnType,
		Encoding:       mapping.Encoding,
		Compression:    typedcolumn.CompressionNone,
		CompressionSet: true,
		StatsDisabled:  !columnStoreValueTypeHasTypedColumnIntegerStats(field.ValueType),
	}
	switch field.ValueType {
	case ColumnStoreValueFloat32Vector:
		if field.VectorDims < 0 {
			return typedcolumn.ColumnDefinition{}, fmt.Errorf("collections: typed-column adapter field %q float32_vector vector_dims=%d must be non-negative", field.Path, field.VectorDims)
		}
		if field.ElementsPerRow < 0 {
			return typedcolumn.ColumnDefinition{}, fmt.Errorf("collections: typed-column adapter field %q float32_vector elements_per_row=%d must be non-negative", field.Path, field.ElementsPerRow)
		}
		if field.VectorDims > 0 && field.ElementsPerRow > 0 && field.VectorDims != field.ElementsPerRow {
			return typedcolumn.ColumnDefinition{}, fmt.Errorf("collections: typed-column adapter field %q float32_vector elements_per_row=%d must match vector_dims=%d", field.Path, field.ElementsPerRow, field.VectorDims)
		}
		dims := field.VectorDims
		if dims <= 0 {
			dims = field.ElementsPerRow
		}
		if dims <= 0 {
			return typedcolumn.ColumnDefinition{}, fmt.Errorf("collections: typed-column adapter field %q float32_vector requires positive vector_dims/elements_per_row", field.Path)
		}
		def.FixedWidthElements = dims
	case ColumnStoreValueUint8Vector, ColumnStoreValueInt8Vector, ColumnStoreValueUint16Vector, ColumnStoreValueInt16Vector, ColumnStoreValueUint32Vector, ColumnStoreValueInt32Vector, ColumnStoreValueUint64Vector, ColumnStoreValueInt64Vector, ColumnStoreValueFloat16Vector, ColumnStoreValueBFloat16Vector, ColumnStoreValueFloat64Vector:
		if field.ElementsPerRow <= 0 {
			return typedcolumn.ColumnDefinition{}, fmt.Errorf("collections: typed-column adapter field %q %s requires positive elements_per_row", field.Path, field.ValueType)
		}
		def.FixedWidthElements = field.ElementsPerRow
	case ColumnStoreValueByteVector:
		if field.BytesPerRow <= 0 {
			return typedcolumn.ColumnDefinition{}, fmt.Errorf("collections: typed-column adapter field %q byte_vector requires positive bytes_per_row", field.Path)
		}
		def.FixedWidthElements = field.BytesPerRow
		def.BitsPerElement = 0
	case ColumnStoreValuePackedBitVector, ColumnStoreValuePackedUint2Vector, ColumnStoreValuePackedUint4Vector:
		_, encoding, bitsPerElement, ok := typedColumnPackedUintVectorMapping(field.ValueType)
		if !ok {
			return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: unsupported packed uint value_type=%s", errTypedColumnAdapterUnsupportedType, field.ValueType)
		}
		if field.ElementsPerRow <= 0 {
			return typedcolumn.ColumnDefinition{}, fmt.Errorf("collections: typed-column adapter field %q %s requires positive elements_per_row", field.Path, field.ValueType)
		}
		if field.BitsPerElement != 0 && field.BitsPerElement != bitsPerElement {
			return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: %s bits_per_element=%d want %d", errTypedColumnAdapterUnsupportedType, field.ValueType, field.BitsPerElement, bitsPerElement)
		}
		def.Encoding = encoding
		def.FixedWidthElements = field.ElementsPerRow
		def.BitsPerElement = bitsPerElement
	case ColumnStoreValueUint32List:
		if field.AdjacencyDegree != 0 {
			return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: uint32_list adjacency_degree=%d must be zero", errTypedColumnAdapterUnsupportedType, field.AdjacencyDegree)
		}
		if field.AdjacencyLayout != "" {
			return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: uint32_list must not set adjacency_layout %q", errTypedColumnAdapterUnsupportedType, field.AdjacencyLayout)
		}
		if field.FixedWidthEncoding != ColumnFixedWidthEncodingDefault {
			return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: uint32_list fixed_width_encoding is unsupported", errTypedColumnAdapterUnsupportedType)
		}
		def.Encoding = typedcolumn.EncodingRawUint32OffsetsList
		def.FixedWidthElements = 0
	case ColumnStoreValueBytes:
		if field.AdjacencyDegree != 0 {
			return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: bytes adjacency_degree=%d must be zero", errTypedColumnAdapterUnsupportedType, field.AdjacencyDegree)
		}
		if field.AdjacencyLayout != "" {
			return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: bytes must not set adjacency_layout %q", errTypedColumnAdapterUnsupportedType, field.AdjacencyLayout)
		}
		if field.FixedWidthEncoding != ColumnFixedWidthEncodingDefault {
			return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: bytes fixed_width_encoding is unsupported", errTypedColumnAdapterUnsupportedType)
		}
		def.Encoding = typedcolumn.EncodingRawBytesOffsets
		def.FixedWidthElements = 0
	case ColumnStoreValueAdjacencyList:
		switch field.AdjacencyLayout {
		case ColumnAdjacencyListLayoutUint32OffsetsList:
			if field.AdjacencyDegree != 0 {
				return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: adjacency_list adjacency_degree=%d must be zero for adjacency_layout %q", errTypedColumnAdapterUnsupportedType, field.AdjacencyDegree, field.AdjacencyLayout)
			}
			if field.FixedWidthEncoding != ColumnFixedWidthEncodingDefault {
				return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: adjacency_list fixed_width_encoding is unsupported for adjacency_layout %q", errTypedColumnAdapterUnsupportedType, field.AdjacencyLayout)
			}
			def.Encoding = typedcolumn.EncodingRawUint32OffsetsList
			def.FixedWidthElements = 0
		case ColumnAdjacencyListLayoutFixedDense:
			if field.AdjacencyDegree <= 0 {
				return typedcolumn.ColumnDefinition{}, fmt.Errorf("collections: typed-column adapter field %q adjacency_list requires positive adjacency_degree", field.Path)
			}
			def.FixedWidthElements = field.AdjacencyDegree
		default:
			return typedcolumn.ColumnDefinition{}, fmt.Errorf("%w: unsupported adjacency_list layout %q", errTypedColumnAdapterUnsupportedType, field.AdjacencyLayout)
		}
	}
	if err := validateTypedColumnProductionDefinition(field, def); err != nil {
		return typedcolumn.ColumnDefinition{}, err
	}
	return def, nil
}

func typedColumnProductionFieldName(field TypedStorageField) (string, error) {
	name := field.Name
	if name == "" {
		name = field.Path
	}
	if name == "" {
		return "", errors.New("collections: typed-column adapter field requires name or path")
	}
	if name == typedColumnAdapterPrimaryIDColumn || field.Path == typedColumnAdapterPrimaryIDColumn {
		return "", fmt.Errorf("collections: typed-column adapter field %q uses reserved primary-id column %q", field.Path, typedColumnAdapterPrimaryIDColumn)
	}
	if name == typedColumnAdapterMetadataDictionary || field.Path == typedColumnAdapterMetadataDictionary {
		return "", fmt.Errorf("collections: typed-column adapter field %q uses reserved metadata dictionary %q", field.Path, typedColumnAdapterMetadataDictionary)
	}
	return name, nil
}

func validateTypedColumnProductionDefinition(field TypedStorageField, def typedcolumn.ColumnDefinition) error {
	if def.Name == "" {
		return fmt.Errorf("%w: empty column name for field %q", errTypedColumnProductionLayoutUnsupported, field.Path)
	}
	if err := validateTypedColumnProductionCompression(def.Compression); err != nil {
		return err
	}
	if !typedColumnProductionEncodingKnown(def.Encoding) {
		return fmt.Errorf("%w: unsupported encoding %s for field %q", errTypedColumnProductionLayoutUnsupported, def.Encoding, field.Path)
	}
	if def.CodecBlockRows < 0 {
		return fmt.Errorf("%w: invalid codec block rows %d for field %q", errTypedColumnProductionLayoutUnsupported, def.CodecBlockRows, field.Path)
	}
	var err error
	switch field.ValueType {
	case ColumnStoreValueBool:
		err = validateTypedColumnProductionBoolDefinition(field, def)
	case ColumnStoreValueInt64:
		err = validateTypedColumnProductionInt64Definition(field, def)
	case ColumnStoreValueFloat32:
		if field.FixedWidthEncoding == ColumnFixedWidthEncodingLittleEndian && !field.Nullable {
			err = requireTypedColumnProductionTypeEncoding(field, def, typedcolumn.ColumnTypeFloat32, typedcolumn.EncodingRawFloat32)
		} else {
			err = validateTypedColumnProductionInt64CarrierDefinition(field, def, typedcolumn.ColumnTypeInt64)
		}
	case ColumnStoreValueDouble:
		if field.FixedWidthEncoding == ColumnFixedWidthEncodingLittleEndian && !field.Nullable {
			err = requireTypedColumnProductionTypeEncoding(field, def, typedcolumn.ColumnTypeFloat64, typedcolumn.EncodingRawFloat64)
		} else {
			err = validateTypedColumnProductionInt64CarrierDefinition(field, def, typedcolumn.ColumnTypeInt64)
		}
	case ColumnStoreValueString:
		err = validateTypedColumnProductionStringDefinition(field, def)
	case ColumnStoreValueInt8, ColumnStoreValueUint8, ColumnStoreValueInt16, ColumnStoreValueUint16, ColumnStoreValueInt32, ColumnStoreValueUint32, ColumnStoreValueUint64, ColumnStoreValueFloat16, ColumnStoreValueBFloat16:
		err = validateTypedColumnProductionPrimitiveScalarDefinition(field, def)
	case ColumnStoreValueFloat32Vector:
		if field.Nullable {
			err = fmt.Errorf("%w: nullable float32_vector field %q is unsupported", errTypedColumnProductionLayoutUnsupported, field.Path)
		} else if e := requireTypedColumnProductionTypeEncoding(field, def, typedcolumn.ColumnTypeFloat32Vector, typedcolumn.EncodingRawFloat32Vector); e != nil {
			err = e
		} else if def.FixedWidthElements <= 0 {
			err = fmt.Errorf("%w: float32_vector field %q requires positive fixed_width_elements", errTypedColumnProductionLayoutUnsupported, field.Path)
		}
	case ColumnStoreValueUint8Vector, ColumnStoreValueInt8Vector, ColumnStoreValueUint16Vector, ColumnStoreValueInt16Vector, ColumnStoreValueUint32Vector, ColumnStoreValueInt32Vector, ColumnStoreValueUint64Vector, ColumnStoreValueInt64Vector, ColumnStoreValueFloat16Vector, ColumnStoreValueBFloat16Vector, ColumnStoreValueFloat64Vector:
		err = validateTypedColumnProductionDenseNumericVectorDefinition(field, def)
	case ColumnStoreValueByteVector:
		err = validateTypedColumnProductionFixedBytesDefinition(field, def)
	case ColumnStoreValuePackedBitVector, ColumnStoreValuePackedUint2Vector, ColumnStoreValuePackedUint4Vector:
		err = validateTypedColumnProductionPackedUintVectorDefinition(field, def)
	case ColumnStoreValueUint32List:
		if field.Nullable {
			err = fmt.Errorf("%w: nullable uint32_list field %q is unsupported", errTypedColumnProductionLayoutUnsupported, field.Path)
		} else if e := requireTypedColumnProductionTypeEncoding(field, def, typedcolumn.ColumnTypeUint32List, typedcolumn.EncodingRawUint32OffsetsList); e != nil {
			err = e
		} else if def.FixedWidthElements != 0 {
			err = fmt.Errorf("%w: uint32_list field %q requires fixed_width_elements=0", errTypedColumnProductionLayoutUnsupported, field.Path)
		}
	case ColumnStoreValueBytes:
		if field.Nullable {
			err = fmt.Errorf("%w: nullable bytes field %q is unsupported", errTypedColumnProductionLayoutUnsupported, field.Path)
		} else if e := requireTypedColumnProductionTypeEncoding(field, def, typedcolumn.ColumnTypeBytes, typedcolumn.EncodingRawBytesOffsets); e != nil {
			err = e
		} else if def.FixedWidthElements != 0 {
			err = fmt.Errorf("%w: bytes field %q requires fixed_width_elements=0", errTypedColumnProductionLayoutUnsupported, field.Path)
		}
	case ColumnStoreValueAdjacencyList:
		if field.Nullable {
			err = fmt.Errorf("%w: nullable adjacency_list field %q is unsupported", errTypedColumnProductionLayoutUnsupported, field.Path)
		} else {
			wantEncoding := typedcolumn.EncodingRawUint32Dense
			wantElementsPositive := true
			if field.AdjacencyLayout == ColumnAdjacencyListLayoutUint32OffsetsList {
				wantEncoding = typedcolumn.EncodingRawUint32OffsetsList
				wantElementsPositive = false
			}
			if e := requireTypedColumnProductionTypeEncoding(field, def, typedcolumn.ColumnTypeAdjacencyList, wantEncoding); e != nil {
				err = e
			} else if wantElementsPositive {
				if def.FixedWidthElements <= 0 {
					err = fmt.Errorf("%w: adjacency_list field %q requires positive fixed_width_elements", errTypedColumnProductionLayoutUnsupported, field.Path)
				}
			} else if def.FixedWidthElements != 0 {
				err = fmt.Errorf("%w: adjacency_list field %q offsets-list requires fixed_width_elements=0", errTypedColumnProductionLayoutUnsupported, field.Path)
			}
		}
	default:
		err = fmt.Errorf("%w: unsupported value_type %q for field %q", errTypedColumnProductionLayoutUnsupported, field.ValueType, field.Path)
	}
	if err != nil {
		return err
	}
	return validateTypedColumnProductionCompressionForField(field, def)
}

func validateTypedColumnProductionBoolDefinition(field TypedStorageField, def typedcolumn.ColumnDefinition) error {
	if field.Nullable {
		return requireTypedColumnProductionTypeEncoding(field, def, typedcolumn.ColumnTypeBool, typedcolumn.EncodingNullableInt64)
	}
	return requireTypedColumnProductionTypeEncoding(field, def, typedcolumn.ColumnTypeBool, typedcolumn.EncodingBoolBitpackRLE)
}

func validateTypedColumnProductionInt64Definition(field TypedStorageField, def typedcolumn.ColumnDefinition) error {
	if def.Type != typedcolumn.ColumnTypeInt64 {
		return typedColumnProductionTypeMismatch(field, def, typedcolumn.ColumnTypeInt64)
	}
	if field.Nullable {
		if field.FixedWidthEncoding != ColumnFixedWidthEncodingDefault {
			return fmt.Errorf("%w: nullable int64 field %q cannot use raw fixed-width encoding", errTypedColumnProductionLayoutUnsupported, field.Path)
		}
		if def.Encoding != typedcolumn.EncodingNullableInt64 {
			return typedColumnProductionEncodingMismatch(field, def, typedcolumn.EncodingNullableInt64)
		}
		return nil
	}
	if field.FixedWidthEncoding == ColumnFixedWidthEncodingLittleEndian {
		if def.Encoding != typedcolumn.EncodingRawInt64 {
			return typedColumnProductionEncodingMismatch(field, def, typedcolumn.EncodingRawInt64)
		}
		return nil
	}
	switch def.Encoding {
	case typedcolumn.EncodingRawInt64, typedcolumn.EncodingDeltaVarint, typedcolumn.EncodingDoubleDeltaVarint:
		return nil
	default:
		return typedColumnProductionEncodingMismatch(field, def, typedcolumn.EncodingDeltaVarint)
	}
}

func validateTypedColumnProductionStringDefinition(field TypedStorageField, def typedcolumn.ColumnDefinition) error {
	if def.Type != typedcolumn.ColumnTypeLowCardinalityCode {
		return typedColumnProductionTypeMismatch(field, def, typedcolumn.ColumnTypeLowCardinalityCode)
	}
	if field.Nullable {
		if def.Encoding != typedcolumn.EncodingNullableInt64 {
			return typedColumnProductionEncodingMismatch(field, def, typedcolumn.EncodingNullableInt64)
		}
		return nil
	}
	if def.Encoding != typedcolumn.EncodingLowCardinalityUint32 {
		return typedColumnProductionEncodingMismatch(field, def, typedcolumn.EncodingLowCardinalityUint32)
	}
	return nil
}

func typedColumnPrimitiveScalarMapping(valueType ColumnStoreValueType) (typedcolumn.ColumnType, typedcolumn.Encoding, int, bool) {
	switch valueType {
	case ColumnStoreValueInt8:
		return typedcolumn.ColumnTypeInt8, typedcolumn.EncodingRawInt8, 1, true
	case ColumnStoreValueUint8:
		return typedcolumn.ColumnTypeUint8, typedcolumn.EncodingRawUint8, 1, true
	case ColumnStoreValueInt16:
		return typedcolumn.ColumnTypeInt16, typedcolumn.EncodingRawInt16, 2, true
	case ColumnStoreValueUint16:
		return typedcolumn.ColumnTypeUint16, typedcolumn.EncodingRawUint16, 2, true
	case ColumnStoreValueInt32:
		return typedcolumn.ColumnTypeInt32, typedcolumn.EncodingRawInt32, 4, true
	case ColumnStoreValueUint32:
		return typedcolumn.ColumnTypeUint32, typedcolumn.EncodingRawUint32, 4, true
	case ColumnStoreValueUint64:
		return typedcolumn.ColumnTypeUint64, typedcolumn.EncodingRawUint64, 8, true
	case ColumnStoreValueFloat16:
		return typedcolumn.ColumnTypeFloat16, typedcolumn.EncodingRawFloat16, 2, true
	case ColumnStoreValueBFloat16:
		return typedcolumn.ColumnTypeBFloat16, typedcolumn.EncodingRawBFloat16, 2, true
	default:
		return "", 0, 0, false
	}
}

func typedColumnDenseNumericVectorMapping(valueType ColumnStoreValueType) (typedcolumn.ColumnType, typedcolumn.Encoding, int, bool) {
	switch valueType {
	case ColumnStoreValueUint8Vector:
		return typedcolumn.ColumnTypeUint8Vector, typedcolumn.EncodingRawUint8Vector, 1, true
	case ColumnStoreValueInt8Vector:
		return typedcolumn.ColumnTypeInt8Vector, typedcolumn.EncodingRawInt8Vector, 1, true
	case ColumnStoreValueUint16Vector:
		return typedcolumn.ColumnTypeUint16Vector, typedcolumn.EncodingRawUint16Vector, 2, true
	case ColumnStoreValueInt16Vector:
		return typedcolumn.ColumnTypeInt16Vector, typedcolumn.EncodingRawInt16Vector, 2, true
	case ColumnStoreValueUint32Vector:
		return typedcolumn.ColumnTypeUint32Vector, typedcolumn.EncodingRawUint32Vector, 4, true
	case ColumnStoreValueInt32Vector:
		return typedcolumn.ColumnTypeInt32Vector, typedcolumn.EncodingRawInt32Vector, 4, true
	case ColumnStoreValueUint64Vector:
		return typedcolumn.ColumnTypeUint64Vector, typedcolumn.EncodingRawUint64Vector, 8, true
	case ColumnStoreValueInt64Vector:
		return typedcolumn.ColumnTypeInt64Vector, typedcolumn.EncodingRawInt64Vector, 8, true
	case ColumnStoreValueFloat16Vector:
		return typedcolumn.ColumnTypeFloat16Vector, typedcolumn.EncodingRawFloat16Vector, 2, true
	case ColumnStoreValueBFloat16Vector:
		return typedcolumn.ColumnTypeBFloat16Vector, typedcolumn.EncodingRawBFloat16Vector, 2, true
	case ColumnStoreValueFloat64Vector:
		return typedcolumn.ColumnTypeFloat64Vector, typedcolumn.EncodingRawFloat64Vector, 8, true
	default:
		return "", 0, 0, false
	}
}

func validateTypedColumnProductionDenseNumericVectorDefinition(field TypedStorageField, def typedcolumn.ColumnDefinition) error {
	wantType, wantEncoding, _, ok := typedColumnDenseNumericVectorMapping(field.ValueType)
	if !ok {
		return fmt.Errorf("%w: unsupported dense numeric vector value_type=%s", errTypedColumnProductionLayoutUnsupported, field.ValueType)
	}
	if field.Nullable {
		return fmt.Errorf("%w: nullable %s field %q is unsupported", errTypedColumnProductionLayoutUnsupported, field.ValueType, field.Path)
	}
	if field.ElementsPerRow <= 0 {
		return fmt.Errorf("%w: %s field %q requires positive elements_per_row", errTypedColumnProductionLayoutUnsupported, field.ValueType, field.Path)
	}
	if err := requireTypedColumnProductionTypeEncoding(field, def, wantType, wantEncoding); err != nil {
		return err
	}
	if def.FixedWidthElements != field.ElementsPerRow {
		return fmt.Errorf("%w: %s field %q fixed_width_elements=%d want elements_per_row=%d", errTypedColumnProductionLayoutUnsupported, field.ValueType, field.Path, def.FixedWidthElements, field.ElementsPerRow)
	}
	if def.Compression != typedcolumn.CompressionNone {
		return fmt.Errorf("%w: compression %s is unsupported for field %q value_type=%s", errTypedColumnProductionLayoutUnsupported, def.Compression, field.Path, field.ValueType)
	}
	return nil
}

func validateTypedColumnProductionFixedBytesDefinition(field TypedStorageField, def typedcolumn.ColumnDefinition) error {
	if field.Nullable {
		return fmt.Errorf("%w: nullable byte_vector field %q is unsupported", errTypedColumnProductionLayoutUnsupported, field.Path)
	}
	if field.BytesPerRow <= 0 {
		return fmt.Errorf("%w: byte_vector field %q requires positive bytes_per_row", errTypedColumnProductionLayoutUnsupported, field.Path)
	}
	if err := requireTypedColumnProductionTypeEncoding(field, def, typedcolumn.ColumnTypeFixedBytes, typedcolumn.EncodingRawFixedBytes); err != nil {
		return err
	}
	if def.FixedWidthElements != field.BytesPerRow {
		return fmt.Errorf("%w: byte_vector field %q fixed_width_elements=%d want bytes_per_row=%d", errTypedColumnProductionLayoutUnsupported, field.Path, def.FixedWidthElements, field.BytesPerRow)
	}
	if def.BitsPerElement != 0 {
		return fmt.Errorf("%w: byte_vector field %q bits_per_element=%d want 0", errTypedColumnProductionLayoutUnsupported, field.Path, def.BitsPerElement)
	}
	if def.Compression != typedcolumn.CompressionNone {
		return fmt.Errorf("%w: compression %s is unsupported for field %q value_type=%s", errTypedColumnProductionLayoutUnsupported, def.Compression, field.Path, field.ValueType)
	}
	return nil
}

func validateTypedColumnProductionPackedUintVectorDefinition(field TypedStorageField, def typedcolumn.ColumnDefinition) error {
	wantType, wantEncoding, wantBits, ok := typedColumnPackedUintVectorMapping(field.ValueType)
	if !ok {
		return fmt.Errorf("%w: unsupported packed uint vector value_type=%s", errTypedColumnProductionLayoutUnsupported, field.ValueType)
	}
	if field.Nullable {
		return fmt.Errorf("%w: nullable %s field %q is unsupported", errTypedColumnProductionLayoutUnsupported, field.ValueType, field.Path)
	}
	if field.ElementsPerRow <= 0 {
		return fmt.Errorf("%w: %s field %q requires positive elements_per_row", errTypedColumnProductionLayoutUnsupported, field.ValueType, field.Path)
	}
	if field.BitsPerElement != 0 && field.BitsPerElement != wantBits {
		return fmt.Errorf("%w: %s field %q bits_per_element=%d want %d", errTypedColumnProductionLayoutUnsupported, field.ValueType, field.Path, field.BitsPerElement, wantBits)
	}
	if err := requireTypedColumnProductionTypeEncoding(field, def, wantType, wantEncoding); err != nil {
		return err
	}
	if def.FixedWidthElements != field.ElementsPerRow {
		return fmt.Errorf("%w: %s field %q fixed_width_elements=%d want elements_per_row=%d", errTypedColumnProductionLayoutUnsupported, field.ValueType, field.Path, def.FixedWidthElements, field.ElementsPerRow)
	}
	if def.BitsPerElement != wantBits {
		return fmt.Errorf("%w: %s field %q bits_per_element=%d want %d", errTypedColumnProductionLayoutUnsupported, field.ValueType, field.Path, def.BitsPerElement, wantBits)
	}
	if def.Compression != typedcolumn.CompressionNone {
		return fmt.Errorf("%w: compression %s is unsupported for field %q value_type=%s", errTypedColumnProductionLayoutUnsupported, def.Compression, field.Path, field.ValueType)
	}
	return nil
}

func validateTypedColumnProductionPrimitiveScalarDefinition(field TypedStorageField, def typedcolumn.ColumnDefinition) error {
	wantType, wantEncoding, _, ok := typedColumnPrimitiveScalarMapping(field.ValueType)
	if !ok {
		return fmt.Errorf("%w: unsupported primitive scalar value_type=%s", errTypedColumnProductionLayoutUnsupported, field.ValueType)
	}
	if field.Nullable {
		return fmt.Errorf("%w: nullable %s field %q is unsupported", errTypedColumnProductionLayoutUnsupported, field.ValueType, field.Path)
	}
	if err := requireTypedColumnProductionTypeEncoding(field, def, wantType, wantEncoding); err != nil {
		return err
	}
	if def.FixedWidthElements != 0 {
		return fmt.Errorf("%w: %s field %q requires fixed_width_elements=0", errTypedColumnProductionLayoutUnsupported, field.ValueType, field.Path)
	}
	if def.Compression != typedcolumn.CompressionNone {
		return fmt.Errorf("%w: compression %s is unsupported for field %q value_type=%s", errTypedColumnProductionLayoutUnsupported, def.Compression, field.Path, field.ValueType)
	}
	return nil
}

func validateTypedColumnProductionInt64CarrierDefinition(field TypedStorageField, def typedcolumn.ColumnDefinition, wantType typedcolumn.ColumnType) error {
	if def.Type != wantType {
		return typedColumnProductionTypeMismatch(field, def, wantType)
	}
	wantEncoding := typedcolumn.EncodingRawInt64
	if field.Nullable {
		wantEncoding = typedcolumn.EncodingNullableInt64
	}
	if def.Encoding != wantEncoding {
		return typedColumnProductionEncodingMismatch(field, def, wantEncoding)
	}
	return nil
}

func requireTypedColumnProductionTypeEncoding(field TypedStorageField, def typedcolumn.ColumnDefinition, wantType typedcolumn.ColumnType, wantEncoding typedcolumn.Encoding) error {
	if def.Type != wantType {
		return typedColumnProductionTypeMismatch(field, def, wantType)
	}
	if def.Encoding != wantEncoding {
		return typedColumnProductionEncodingMismatch(field, def, wantEncoding)
	}
	return nil
}

func typedColumnProductionTypeMismatch(field TypedStorageField, def typedcolumn.ColumnDefinition, want typedcolumn.ColumnType) error {
	return fmt.Errorf("%w: field %q value_type=%s physical_type=%s want %s", errTypedColumnProductionLayoutUnsupported, field.Path, field.ValueType, def.Type, want)
}

func typedColumnProductionEncodingMismatch(field TypedStorageField, def typedcolumn.ColumnDefinition, want typedcolumn.Encoding) error {
	return fmt.Errorf("%w: field %q value_type=%s encoding=%s want %s", errTypedColumnProductionLayoutUnsupported, field.Path, field.ValueType, def.Encoding, want)
}

func validateTypedColumnProductionCompression(compression typedcolumn.Compression) error {
	switch compression {
	case typedcolumn.CompressionNone, typedcolumn.CompressionSnappy, typedcolumn.CompressionLZ4, typedcolumn.CompressionZSTD:
		return nil
	case typedcolumn.CompressionZSTDDict:
		return fmt.Errorf("%w: unsupported compression %s", errTypedColumnProductionLayoutUnsupported, compression)
	default:
		return fmt.Errorf("%w: unknown compression %s", errTypedColumnProductionLayoutUnsupported, compression)
	}
}

func validateTypedColumnProductionCompressionForField(field TypedStorageField, def typedcolumn.ColumnDefinition) error {
	if def.Compression == typedcolumn.CompressionNone {
		return nil
	}
	if err := validateTypedColumnProductionCompression(def.Compression); err != nil {
		return err
	}
	switch field.ValueType {
	case ColumnStoreValueBool, ColumnStoreValueString:
		return nil
	case ColumnStoreValueInt64:
		if field.FixedWidthEncoding == ColumnFixedWidthEncodingLittleEndian && !field.Nullable {
			return fmt.Errorf("%w: compression %s is unsupported for fixed-width field %q value_type=%s", errTypedColumnProductionLayoutUnsupported, def.Compression, field.Path, field.ValueType)
		}
		return nil
	default:
		return fmt.Errorf("%w: compression %s is unsupported for field %q value_type=%s", errTypedColumnProductionLayoutUnsupported, def.Compression, field.Path, field.ValueType)
	}
}

func typedColumnProductionEncodingKnown(encoding typedcolumn.Encoding) bool {
	switch encoding {
	case typedcolumn.EncodingRawInt64,
		typedcolumn.EncodingDeltaVarint,
		typedcolumn.EncodingDoubleDeltaVarint,
		typedcolumn.EncodingNullableInt64,
		typedcolumn.EncodingBoolBitpackRLE,
		typedcolumn.EncodingLowCardinalityUint32,
		typedcolumn.EncodingRawFloat32Vector,
		typedcolumn.EncodingRawUint32Dense,
		typedcolumn.EncodingRawFloat32,
		typedcolumn.EncodingRawFloat64,
		typedcolumn.EncodingRawUint32OffsetsList,
		typedcolumn.EncodingRawBytesOffsets,
		typedcolumn.EncodingRawInt8,
		typedcolumn.EncodingRawUint8,
		typedcolumn.EncodingRawInt16,
		typedcolumn.EncodingRawUint16,
		typedcolumn.EncodingRawInt32,
		typedcolumn.EncodingRawUint32,
		typedcolumn.EncodingRawUint64,
		typedcolumn.EncodingRawFloat16,
		typedcolumn.EncodingRawBFloat16,
		typedcolumn.EncodingRawUint8Vector,
		typedcolumn.EncodingRawInt8Vector,
		typedcolumn.EncodingRawUint16Vector,
		typedcolumn.EncodingRawInt16Vector,
		typedcolumn.EncodingRawUint32Vector,
		typedcolumn.EncodingRawInt32Vector,
		typedcolumn.EncodingRawUint64Vector,
		typedcolumn.EncodingRawInt64Vector,
		typedcolumn.EncodingRawFloat16Vector,
		typedcolumn.EncodingRawBFloat16Vector,
		typedcolumn.EncodingRawFloat64Vector,
		typedcolumn.EncodingRawFixedBytes,
		typedcolumn.EncodingRawPackedBitVector,
		typedcolumn.EncodingRawPackedUint2Vector,
		typedcolumn.EncodingRawPackedUint4Vector:
		return true
	default:
		return false
	}
}

func validateTypedColumnProductionPartColumnLayout(field TypedStorageField, column typedcolumn.ColumnPartColumn) error {
	if err := validateTypedColumnProductionDefinition(field, column.Definition); err != nil {
		return err
	}
	return validateTypedColumnProductionBlocks(field.Path, column.Definition.Encoding, column.Definition.Compression, column.Blocks)
}

func validateTypedColumnProductionPrimaryColumnLayout(column typedcolumn.ColumnPartColumn) error {
	if column.Definition.Name != typedColumnAdapterPrimaryIDColumn || column.Definition.Type != typedcolumn.ColumnTypeInt64 {
		return fmt.Errorf("%w: primary-id column %q type=%s want type=%s", errTypedColumnProductionLayoutUnsupported, column.Definition.Name, column.Definition.Type, typedcolumn.ColumnTypeInt64)
	}
	switch column.Definition.Encoding {
	case typedcolumn.EncodingRawInt64, typedcolumn.EncodingDeltaVarint, typedcolumn.EncodingDoubleDeltaVarint:
	default:
		return fmt.Errorf("%w: primary-id column %q encoding=%s is unsupported", errTypedColumnProductionLayoutUnsupported, column.Definition.Name, column.Definition.Encoding)
	}
	return validateTypedColumnProductionBlocks(typedColumnAdapterPrimaryIDColumn, column.Definition.Encoding, column.Definition.Compression, column.Blocks)
}

func validateTypedColumnProductionBlocks(label string, wantEncoding typedcolumn.Encoding, requestedCompression typedcolumn.Compression, blocks []typedcolumn.ColumnBlock) error {
	if err := validateTypedColumnProductionCompression(requestedCompression); err != nil {
		return err
	}
	for i, block := range blocks {
		if block.Descriptor.Encoding != wantEncoding {
			return fmt.Errorf("%w: field %q block %d descriptor encoding=%s want %s", errTypedColumnProductionLayoutUnsupported, label, i, block.Descriptor.Encoding, wantEncoding)
		}
		if block.Granule.Encoding != wantEncoding {
			return fmt.Errorf("%w: field %q block %d granule encoding=%s want %s", errTypedColumnProductionLayoutUnsupported, label, i, block.Granule.Encoding, wantEncoding)
		}
		if err := validateTypedColumnProductionActualCompression(label, i, requestedCompression, block.Descriptor.Compression, block.Descriptor.StoredBytes, block.Descriptor.RawBytes, "descriptor"); err != nil {
			return err
		}
		if block.Descriptor.Compression != block.Granule.Compression {
			return fmt.Errorf("%w: field %q block %d descriptor/granule compression mismatch %s/%s", errTypedColumnProductionLayoutUnsupported, label, i, block.Descriptor.Compression, block.Granule.Compression)
		}
		if err := validateTypedColumnProductionActualCompression(label, i, requestedCompression, block.Granule.Compression, block.Granule.StoredBytes, block.Granule.RawBytes, "granule"); err != nil {
			return err
		}
	}
	return nil
}

func validateTypedColumnProductionActualCompression(label string, blockIndex int, requestedCompression, actualCompression typedcolumn.Compression, storedBytes, rawBytes int, source string) error {
	if err := validateTypedColumnProductionCompression(actualCompression); err != nil {
		return err
	}
	if actualCompression != typedcolumn.CompressionNone && actualCompression != requestedCompression {
		return fmt.Errorf("%w: field %q block %d %s compression=%s not admitted by requested compression=%s", errTypedColumnProductionLayoutUnsupported, label, blockIndex, source, actualCompression, requestedCompression)
	}
	if actualCompression == typedcolumn.CompressionNone {
		if storedBytes != rawBytes {
			return fmt.Errorf("%w: field %q block %d uncompressed %s stored_bytes=%d raw_bytes=%d", errTypedColumnProductionLayoutUnsupported, label, blockIndex, source, storedBytes, rawBytes)
		}
		return nil
	}
	if storedBytes <= 0 || rawBytes <= 0 || storedBytes >= rawBytes {
		return fmt.Errorf("%w: field %q block %d compressed %s stored_bytes=%d raw_bytes=%d did not satisfy keep-if-smaller", errTypedColumnProductionLayoutUnsupported, label, blockIndex, source, storedBytes, rawBytes)
	}
	return nil
}
