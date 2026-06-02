package collections

import (
	"encoding/json"
	"fmt"
	"strconv"
)

func columnStorePrimitiveScalarValueTypes() []ColumnStoreValueType {
	return []ColumnStoreValueType{
		ColumnStoreValueInt8,
		ColumnStoreValueUint8,
		ColumnStoreValueInt16,
		ColumnStoreValueUint16,
		ColumnStoreValueInt32,
		ColumnStoreValueUint32,
		ColumnStoreValueUint64,
		ColumnStoreValueFloat16,
		ColumnStoreValueBFloat16,
	}
}

func columnStoreValueTypeIsPrimitiveScalar(valueType ColumnStoreValueType) bool {
	switch valueType {
	case ColumnStoreValueInt8,
		ColumnStoreValueUint8,
		ColumnStoreValueInt16,
		ColumnStoreValueUint16,
		ColumnStoreValueInt32,
		ColumnStoreValueUint32,
		ColumnStoreValueUint64,
		ColumnStoreValueFloat16,
		ColumnStoreValueBFloat16:
		return true
	default:
		return false
	}
}

func columnStoreValueTypeHasTypedColumnIntegerStats(valueType ColumnStoreValueType) bool {
	switch valueType {
	case ColumnStoreValueInt64,
		ColumnStoreValueInt8,
		ColumnStoreValueUint8,
		ColumnStoreValueInt16,
		ColumnStoreValueUint16,
		ColumnStoreValueInt32,
		ColumnStoreValueUint32:
		return true
	default:
		return false
	}
}

func columnStoreValueTypeIsStorageOnlyFloatBits(valueType ColumnStoreValueType) bool {
	switch valueType {
	case ColumnStoreValueFloat16, ColumnStoreValueBFloat16:
		return true
	default:
		return false
	}
}

func columnStorePrimitiveScalarWidth(valueType ColumnStoreValueType) (int, bool) {
	switch valueType {
	case ColumnStoreValueInt8, ColumnStoreValueUint8:
		return 1, true
	case ColumnStoreValueInt16, ColumnStoreValueUint16, ColumnStoreValueFloat16, ColumnStoreValueBFloat16:
		return 2, true
	case ColumnStoreValueInt32, ColumnStoreValueUint32:
		return 4, true
	case ColumnStoreValueUint64:
		return 8, true
	default:
		return 0, false
	}
}

func convertJSONSignedScalar(raw any, label string, minValue, maxValue int64) (int64, error) {
	n, ok := raw.(json.Number)
	if !ok {
		return 0, fmt.Errorf("expected %s integer got %T", label, raw)
	}
	v, err := strconv.ParseInt(n.String(), 10, 64)
	if err != nil {
		return 0, err
	}
	if v < minValue || v > maxValue {
		return 0, fmt.Errorf("%s=%d outside range [%d,%d]", label, v, minValue, maxValue)
	}
	return v, nil
}

func convertJSONUnsignedScalar(raw any, label string, maxValue uint64) (uint64, error) {
	n, ok := raw.(json.Number)
	if !ok {
		return 0, fmt.Errorf("expected %s integer got %T", label, raw)
	}
	v, err := strconv.ParseUint(n.String(), 10, 64)
	if err != nil {
		return 0, err
	}
	if v > maxValue {
		return 0, fmt.Errorf("%s=%d outside range [0,%d]", label, v, maxValue)
	}
	return v, nil
}
