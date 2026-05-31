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
		case ColumnStoreValueFloat32Vector, ColumnStoreValueAdjacencyList:
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
		case ColumnStoreValueFloat32Vector, ColumnStoreValueUint32List, ColumnStoreValueBytes, ColumnStoreValueAdjacencyList:
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
		StatsDisabled:  field.ValueType != ColumnStoreValueInt64,
	}
	switch field.ValueType {
	case ColumnStoreValueFloat32Vector:
		if field.VectorDims <= 0 {
			return typedcolumn.ColumnDefinition{}, fmt.Errorf("collections: typed-column adapter field %q float32_vector requires positive vector_dims", field.Path)
		}
		def.FixedWidthElements = field.VectorDims
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
	switch field.ValueType {
	case ColumnStoreValueBool:
		if err := validateTypedColumnProductionBoolDefinition(field, def); err != nil {
			return err
		}
	case ColumnStoreValueInt64:
		if err := validateTypedColumnProductionInt64Definition(field, def); err != nil {
			return err
		}
	case ColumnStoreValueFloat32:
		if field.FixedWidthEncoding == ColumnFixedWidthEncodingLittleEndian && !field.Nullable {
			return requireTypedColumnProductionTypeEncoding(field, def, typedcolumn.ColumnTypeFloat32, typedcolumn.EncodingRawFloat32)
		}
		return validateTypedColumnProductionInt64CarrierDefinition(field, def, typedcolumn.ColumnTypeInt64)
	case ColumnStoreValueDouble:
		if field.FixedWidthEncoding == ColumnFixedWidthEncodingLittleEndian && !field.Nullable {
			return requireTypedColumnProductionTypeEncoding(field, def, typedcolumn.ColumnTypeFloat64, typedcolumn.EncodingRawFloat64)
		}
		return validateTypedColumnProductionInt64CarrierDefinition(field, def, typedcolumn.ColumnTypeInt64)
	case ColumnStoreValueString:
		return validateTypedColumnProductionStringDefinition(field, def)
	case ColumnStoreValueFloat32Vector:
		if field.Nullable {
			return fmt.Errorf("%w: nullable float32_vector field %q is unsupported", errTypedColumnProductionLayoutUnsupported, field.Path)
		}
		if err := requireTypedColumnProductionTypeEncoding(field, def, typedcolumn.ColumnTypeFloat32Vector, typedcolumn.EncodingRawFloat32Vector); err != nil {
			return err
		}
		if def.FixedWidthElements <= 0 {
			return fmt.Errorf("%w: float32_vector field %q requires positive fixed_width_elements", errTypedColumnProductionLayoutUnsupported, field.Path)
		}
	case ColumnStoreValueUint32List:
		if field.Nullable {
			return fmt.Errorf("%w: nullable uint32_list field %q is unsupported", errTypedColumnProductionLayoutUnsupported, field.Path)
		}
		if err := requireTypedColumnProductionTypeEncoding(field, def, typedcolumn.ColumnTypeUint32List, typedcolumn.EncodingRawUint32OffsetsList); err != nil {
			return err
		}
		if def.FixedWidthElements != 0 {
			return fmt.Errorf("%w: uint32_list field %q requires fixed_width_elements=0", errTypedColumnProductionLayoutUnsupported, field.Path)
		}
	case ColumnStoreValueBytes:
		if field.Nullable {
			return fmt.Errorf("%w: nullable bytes field %q is unsupported", errTypedColumnProductionLayoutUnsupported, field.Path)
		}
		if err := requireTypedColumnProductionTypeEncoding(field, def, typedcolumn.ColumnTypeBytes, typedcolumn.EncodingRawBytesOffsets); err != nil {
			return err
		}
		if def.FixedWidthElements != 0 {
			return fmt.Errorf("%w: bytes field %q requires fixed_width_elements=0", errTypedColumnProductionLayoutUnsupported, field.Path)
		}
	case ColumnStoreValueAdjacencyList:
		if field.Nullable {
			return fmt.Errorf("%w: nullable adjacency_list field %q is unsupported", errTypedColumnProductionLayoutUnsupported, field.Path)
		}
		wantEncoding := typedcolumn.EncodingRawUint32Dense
		wantElementsPositive := true
		if field.AdjacencyLayout == ColumnAdjacencyListLayoutUint32OffsetsList {
			wantEncoding = typedcolumn.EncodingRawUint32OffsetsList
			wantElementsPositive = false
		}
		if err := requireTypedColumnProductionTypeEncoding(field, def, typedcolumn.ColumnTypeAdjacencyList, wantEncoding); err != nil {
			return err
		}
		if wantElementsPositive {
			if def.FixedWidthElements <= 0 {
				return fmt.Errorf("%w: adjacency_list field %q requires positive fixed_width_elements", errTypedColumnProductionLayoutUnsupported, field.Path)
			}
		} else if def.FixedWidthElements != 0 {
			return fmt.Errorf("%w: adjacency_list field %q offsets-list requires fixed_width_elements=0", errTypedColumnProductionLayoutUnsupported, field.Path)
		}
	default:
		return fmt.Errorf("%w: unsupported value_type %q for field %q", errTypedColumnProductionLayoutUnsupported, field.ValueType, field.Path)
	}
	return nil
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
	case typedcolumn.CompressionNone:
		return nil
	case typedcolumn.CompressionSnappy, typedcolumn.CompressionLZ4:
		return fmt.Errorf("%w: compression %s is not enabled for production typed-column layouts", errTypedColumnProductionLayoutUnsupported, compression)
	case typedcolumn.CompressionZSTD, typedcolumn.CompressionZSTDDict:
		return fmt.Errorf("%w: unsupported compression %s", errTypedColumnProductionLayoutUnsupported, compression)
	default:
		return fmt.Errorf("%w: unknown compression %s", errTypedColumnProductionLayoutUnsupported, compression)
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
		typedcolumn.EncodingRawBytesOffsets:
		return true
	default:
		return false
	}
}

func validateTypedColumnProductionPartColumnLayout(field TypedStorageField, column typedcolumn.ColumnPartColumn) error {
	if err := validateTypedColumnProductionDefinition(field, column.Definition); err != nil {
		return err
	}
	return validateTypedColumnProductionBlocks(field.Path, column.Definition.Encoding, column.Blocks)
}

func validateTypedColumnProductionPrimaryColumnLayout(column typedcolumn.ColumnPartColumn) error {
	if column.Definition.Name != typedColumnAdapterPrimaryIDColumn || column.Definition.Type != typedcolumn.ColumnTypeInt64 || column.Definition.Encoding != typedcolumn.EncodingRawInt64 || column.Definition.Compression != typedcolumn.CompressionNone {
		return fmt.Errorf("%w: primary-id column %q type=%s encoding=%s compression=%s want type=%s encoding=%s compression=%s", errTypedColumnProductionLayoutUnsupported, column.Definition.Name, column.Definition.Type, column.Definition.Encoding, column.Definition.Compression, typedcolumn.ColumnTypeInt64, typedcolumn.EncodingRawInt64, typedcolumn.CompressionNone)
	}
	return validateTypedColumnProductionBlocks(typedColumnAdapterPrimaryIDColumn, typedcolumn.EncodingRawInt64, column.Blocks)
}

func validateTypedColumnProductionBlocks(label string, wantEncoding typedcolumn.Encoding, blocks []typedcolumn.ColumnBlock) error {
	for i, block := range blocks {
		if block.Descriptor.Encoding != wantEncoding {
			return fmt.Errorf("%w: field %q block %d descriptor encoding=%s want %s", errTypedColumnProductionLayoutUnsupported, label, i, block.Descriptor.Encoding, wantEncoding)
		}
		if block.Granule.Encoding != wantEncoding {
			return fmt.Errorf("%w: field %q block %d granule encoding=%s want %s", errTypedColumnProductionLayoutUnsupported, label, i, block.Granule.Encoding, wantEncoding)
		}
		if block.Descriptor.Compression != typedcolumn.CompressionNone {
			return fmt.Errorf("%w: field %q block %d descriptor compression=%s want %s", errTypedColumnProductionLayoutUnsupported, label, i, block.Descriptor.Compression, typedcolumn.CompressionNone)
		}
		if block.Granule.Compression != typedcolumn.CompressionNone {
			return fmt.Errorf("%w: field %q block %d granule compression=%s want %s", errTypedColumnProductionLayoutUnsupported, label, i, block.Granule.Compression, typedcolumn.CompressionNone)
		}
		if block.Descriptor.StoredBytes != block.Descriptor.RawBytes {
			return fmt.Errorf("%w: field %q block %d uncompressed descriptor stored_bytes=%d raw_bytes=%d", errTypedColumnProductionLayoutUnsupported, label, i, block.Descriptor.StoredBytes, block.Descriptor.RawBytes)
		}
		if block.Granule.StoredBytes != block.Granule.RawBytes {
			return fmt.Errorf("%w: field %q block %d uncompressed granule stored_bytes=%d raw_bytes=%d", errTypedColumnProductionLayoutUnsupported, label, i, block.Granule.StoredBytes, block.Granule.RawBytes)
		}
	}
	return nil
}
