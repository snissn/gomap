package collections

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
)

func columnStoreDenseNumericVectorValueTypes() []ColumnStoreValueType {
	return []ColumnStoreValueType{
		ColumnStoreValueUint8Vector,
		ColumnStoreValueInt8Vector,
		ColumnStoreValueUint16Vector,
		ColumnStoreValueInt16Vector,
		ColumnStoreValueUint32Vector,
		ColumnStoreValueInt32Vector,
		ColumnStoreValueUint64Vector,
		ColumnStoreValueInt64Vector,
		ColumnStoreValueFloat16Vector,
		ColumnStoreValueBFloat16Vector,
		ColumnStoreValueFloat64Vector,
	}
}

func columnStoreValueTypeIsDenseNumericVector(valueType ColumnStoreValueType) bool {
	_, ok := columnStoreDenseNumericVectorWidth(valueType)
	return ok
}

func columnStoreDenseNumericVectorWidth(valueType ColumnStoreValueType) (int, bool) {
	switch valueType {
	case ColumnStoreValueUint8Vector, ColumnStoreValueInt8Vector:
		return 1, true
	case ColumnStoreValueUint16Vector, ColumnStoreValueInt16Vector, ColumnStoreValueFloat16Vector, ColumnStoreValueBFloat16Vector:
		return 2, true
	case ColumnStoreValueUint32Vector, ColumnStoreValueInt32Vector:
		return 4, true
	case ColumnStoreValueUint64Vector, ColumnStoreValueInt64Vector, ColumnStoreValueFloat64Vector:
		return 8, true
	default:
		return 0, false
	}
}

func columnStoreDenseNumericVectorElementsPerRow(col ColumnStoreColumn) int {
	return col.ElementsPerRow
}

func columnStoreFloat32VectorElementsPerRow(col ColumnStoreColumn) int {
	if col.VectorDims > 0 {
		return col.VectorDims
	}
	return col.ElementsPerRow
}

func typedStorageDenseNumericVectorElementsPerRow(field TypedStorageField) int {
	return field.ElementsPerRow
}

func convertJSONDenseNumericVector(raw any, col ColumnStoreColumn) ([]byte, error) {
	values, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("expected %s array got %T", col.ValueType, raw)
	}
	elementsPerRow := columnStoreDenseNumericVectorElementsPerRow(col)
	if elementsPerRow <= 0 {
		return nil, fmt.Errorf("invalid %s elements_per_row %d", col.ValueType, elementsPerRow)
	}
	if len(values) != elementsPerRow {
		return nil, fmt.Errorf("%s length=%d want elements_per_row=%d", col.ValueType, len(values), elementsPerRow)
	}
	width, ok := columnStoreDenseNumericVectorWidth(col.ValueType)
	if !ok {
		return nil, fmt.Errorf("unsupported dense numeric vector value_type=%s", col.ValueType)
	}
	out := make([]byte, len(values)*width)
	for i, rawValue := range values {
		if err := encodeJSONDenseNumericVectorElement(out[i*width:], col.ValueType, rawValue); err != nil {
			return nil, fmt.Errorf("%s[%d]: %w", col.ValueType, i, err)
		}
	}
	return out, nil
}

func encodeJSONDenseNumericVectorElement(dst []byte, valueType ColumnStoreValueType, raw any) error {
	switch valueType {
	case ColumnStoreValueUint8Vector:
		v, err := convertJSONUnsignedScalar(raw, "uint8_vector", 1<<8-1)
		if err != nil {
			return err
		}
		dst[0] = byte(v)
	case ColumnStoreValueInt8Vector:
		v, err := convertJSONSignedScalar(raw, "int8_vector", -128, 127)
		if err != nil {
			return err
		}
		dst[0] = byte(int8(v))
	case ColumnStoreValueUint16Vector, ColumnStoreValueFloat16Vector, ColumnStoreValueBFloat16Vector:
		v, err := convertJSONUnsignedScalar(raw, string(valueType), 1<<16-1)
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint16(dst, uint16(v))
	case ColumnStoreValueInt16Vector:
		v, err := convertJSONSignedScalar(raw, "int16_vector", -1<<15, 1<<15-1)
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint16(dst, uint16(int16(v)))
	case ColumnStoreValueUint32Vector:
		v, err := convertJSONUnsignedScalar(raw, "uint32_vector", 1<<32-1)
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint32(dst, uint32(v))
	case ColumnStoreValueInt32Vector:
		v, err := convertJSONSignedScalar(raw, "int32_vector", -1<<31, 1<<31-1)
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint32(dst, uint32(int32(v)))
	case ColumnStoreValueUint64Vector:
		v, err := convertJSONUnsignedScalar(raw, "uint64_vector", ^uint64(0))
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint64(dst, v)
	case ColumnStoreValueInt64Vector:
		v, err := convertJSONSignedScalar(raw, "int64_vector", math.MinInt64, math.MaxInt64)
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint64(dst, uint64(v))
	case ColumnStoreValueFloat64Vector:
		n, ok := raw.(json.Number)
		if !ok {
			return fmt.Errorf("expected float64_vector number got %T", raw)
		}
		v, err := strconv.ParseFloat(n.String(), 64)
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint64(dst, math.Float64bits(v))
	default:
		return fmt.Errorf("unsupported dense numeric vector value_type=%s", valueType)
	}
	return nil
}
