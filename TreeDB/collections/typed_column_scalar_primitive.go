package collections

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func typedColumnAdapterPrimitiveScalarColumnType(t typedcolumn.ColumnType) bool {
	switch t {
	case typedcolumn.ColumnTypeInt8,
		typedcolumn.ColumnTypeUint8,
		typedcolumn.ColumnTypeInt16,
		typedcolumn.ColumnTypeUint16,
		typedcolumn.ColumnTypeInt32,
		typedcolumn.ColumnTypeUint32,
		typedcolumn.ColumnTypeUint64,
		typedcolumn.ColumnTypeFloat16,
		typedcolumn.ColumnTypeBFloat16:
		return true
	default:
		return false
	}
}

func typedColumnAdapterInitPrimitiveScalarBatchColumn(batch *typedcolumn.Batch, column typedColumnAdapterColumn, rows int) bool {
	switch column.Definition.Type {
	case typedcolumn.ColumnTypeInt8:
		if batch.Int8Columns == nil {
			batch.Int8Columns = make(map[string][]int8)
		}
		batch.Int8Columns[column.Definition.Name] = make([]int8, rows)
	case typedcolumn.ColumnTypeUint8:
		if batch.Uint8Columns == nil {
			batch.Uint8Columns = make(map[string][]uint8)
		}
		batch.Uint8Columns[column.Definition.Name] = make([]uint8, rows)
	case typedcolumn.ColumnTypeInt16:
		if batch.Int16Columns == nil {
			batch.Int16Columns = make(map[string][]int16)
		}
		batch.Int16Columns[column.Definition.Name] = make([]int16, rows)
	case typedcolumn.ColumnTypeUint16:
		if batch.Uint16Columns == nil {
			batch.Uint16Columns = make(map[string][]uint16)
		}
		batch.Uint16Columns[column.Definition.Name] = make([]uint16, rows)
	case typedcolumn.ColumnTypeInt32:
		if batch.Int32Columns == nil {
			batch.Int32Columns = make(map[string][]int32)
		}
		batch.Int32Columns[column.Definition.Name] = make([]int32, rows)
	case typedcolumn.ColumnTypeUint32:
		if batch.Uint32Columns == nil {
			batch.Uint32Columns = make(map[string][]uint32)
		}
		batch.Uint32Columns[column.Definition.Name] = make([]uint32, rows)
	case typedcolumn.ColumnTypeUint64:
		if batch.Uint64Columns == nil {
			batch.Uint64Columns = make(map[string][]uint64)
		}
		batch.Uint64Columns[column.Definition.Name] = make([]uint64, rows)
	case typedcolumn.ColumnTypeFloat16:
		if batch.Float16Columns == nil {
			batch.Float16Columns = make(map[string][]uint16)
		}
		batch.Float16Columns[column.Definition.Name] = make([]uint16, rows)
	case typedcolumn.ColumnTypeBFloat16:
		if batch.BFloat16Columns == nil {
			batch.BFloat16Columns = make(map[string][]uint16)
		}
		batch.BFloat16Columns[column.Definition.Name] = make([]uint16, rows)
	default:
		return false
	}
	return true
}

func encodeTypedColumnAdapterPrimitiveScalarValue(batch *typedcolumn.Batch, rowIdx int, column typedColumnAdapterColumn, value columnDeclaredValue) error {
	if err := validateTypedColumnAdapterDeclaredValue(column, value); err != nil {
		return err
	}
	switch column.Field.ValueType {
	case ColumnStoreValueInt8:
		batch.Int8Columns[column.Definition.Name][rowIdx] = value.Int8
	case ColumnStoreValueUint8:
		batch.Uint8Columns[column.Definition.Name][rowIdx] = value.Uint8
	case ColumnStoreValueInt16:
		batch.Int16Columns[column.Definition.Name][rowIdx] = value.Int16
	case ColumnStoreValueUint16:
		batch.Uint16Columns[column.Definition.Name][rowIdx] = value.Uint16
	case ColumnStoreValueInt32:
		batch.Int32Columns[column.Definition.Name][rowIdx] = value.Int32
	case ColumnStoreValueUint32:
		batch.Uint32Columns[column.Definition.Name][rowIdx] = value.Uint32
	case ColumnStoreValueUint64:
		batch.Uint64Columns[column.Definition.Name][rowIdx] = value.Uint64
	case ColumnStoreValueFloat16:
		batch.Float16Columns[column.Definition.Name][rowIdx] = value.Float16
	case ColumnStoreValueBFloat16:
		batch.BFloat16Columns[column.Definition.Name][rowIdx] = value.BFloat16
	default:
		return fmt.Errorf("%w: %s is not a primitive scalar", errTypedColumnAdapterUnsupportedType, column.Field.ValueType)
	}
	return nil
}

func (p *typedColumnAdapterPart) scanPrimitiveScalarColumnValues(column typedColumnAdapterColumn) ([]columnDeclaredValue, error) {
	if !typedColumnAdapterPrimitiveScalarColumnType(column.Definition.Type) {
		return nil, fmt.Errorf("%w: %s is not a primitive scalar", errTypedColumnAdapterUnsupportedType, column.Definition.Type)
	}
	partColumn, ok := p.Part.Columns[column.Definition.Name]
	if !ok {
		return nil, fmt.Errorf("collections: typed-column adapter missing column %q", column.Definition.Name)
	}
	if partColumn.Definition.Type != column.Definition.Type || partColumn.Definition.Encoding != column.Definition.Encoding {
		return nil, fmt.Errorf("collections: typed-column adapter column %q type/encoding=(%s,%s) want (%s,%s)", column.Definition.Name, partColumn.Definition.Type, partColumn.Definition.Encoding, column.Definition.Type, column.Definition.Encoding)
	}
	out := make([]columnDeclaredValue, p.Part.Descriptor.RowCount)
	var reader typedcolumn.GranuleReader
	for blockIdx, block := range partColumn.Blocks {
		if block.Descriptor.FirstRow < 0 || block.Descriptor.RowCount < 0 || block.Descriptor.FirstRow > len(out)-block.Descriptor.RowCount {
			return nil, fmt.Errorf("collections: typed-column adapter column %q block %d rows %d..%d outside part rows=%d", column.Definition.Name, blockIdx, block.Descriptor.FirstRow, block.Descriptor.FirstRow+block.Descriptor.RowCount, len(out))
		}
		switch column.Field.ValueType {
		case ColumnStoreValueInt8:
			values, err := reader.DecodeInt8(block.Granule)
			if err != nil {
				return nil, fmt.Errorf("collections: typed-column adapter column %q block %d: %w", column.Definition.Name, blockIdx, err)
			}
			if len(values) != block.Descriptor.RowCount {
				return nil, fmt.Errorf("collections: typed-column adapter column %q block %d decoded rows=%d want %d", column.Definition.Name, blockIdx, len(values), block.Descriptor.RowCount)
			}
			for i, v := range values {
				out[block.Descriptor.FirstRow+i] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, Int8: v}
			}
		case ColumnStoreValueUint8:
			values, err := reader.DecodeUint8(block.Granule)
			if err != nil {
				return nil, fmt.Errorf("collections: typed-column adapter column %q block %d: %w", column.Definition.Name, blockIdx, err)
			}
			if len(values) != block.Descriptor.RowCount {
				return nil, fmt.Errorf("collections: typed-column adapter column %q block %d decoded rows=%d want %d", column.Definition.Name, blockIdx, len(values), block.Descriptor.RowCount)
			}
			for i, v := range values {
				out[block.Descriptor.FirstRow+i] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, Uint8: v}
			}
		case ColumnStoreValueInt16:
			values, err := reader.DecodeInt16(block.Granule)
			if err != nil {
				return nil, fmt.Errorf("collections: typed-column adapter column %q block %d: %w", column.Definition.Name, blockIdx, err)
			}
			if len(values) != block.Descriptor.RowCount {
				return nil, fmt.Errorf("collections: typed-column adapter column %q block %d decoded rows=%d want %d", column.Definition.Name, blockIdx, len(values), block.Descriptor.RowCount)
			}
			for i, v := range values {
				out[block.Descriptor.FirstRow+i] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, Int16: v}
			}
		case ColumnStoreValueUint16:
			values, err := reader.DecodeUint16(block.Granule)
			if err != nil {
				return nil, fmt.Errorf("collections: typed-column adapter column %q block %d: %w", column.Definition.Name, blockIdx, err)
			}
			if len(values) != block.Descriptor.RowCount {
				return nil, fmt.Errorf("collections: typed-column adapter column %q block %d decoded rows=%d want %d", column.Definition.Name, blockIdx, len(values), block.Descriptor.RowCount)
			}
			for i, v := range values {
				out[block.Descriptor.FirstRow+i] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, Uint16: v}
			}
		case ColumnStoreValueInt32:
			values, err := reader.DecodeInt32(block.Granule)
			if err != nil {
				return nil, fmt.Errorf("collections: typed-column adapter column %q block %d: %w", column.Definition.Name, blockIdx, err)
			}
			if len(values) != block.Descriptor.RowCount {
				return nil, fmt.Errorf("collections: typed-column adapter column %q block %d decoded rows=%d want %d", column.Definition.Name, blockIdx, len(values), block.Descriptor.RowCount)
			}
			for i, v := range values {
				out[block.Descriptor.FirstRow+i] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, Int32: v}
			}
		case ColumnStoreValueUint32:
			values, err := reader.DecodeUint32(block.Granule)
			if err != nil {
				return nil, fmt.Errorf("collections: typed-column adapter column %q block %d: %w", column.Definition.Name, blockIdx, err)
			}
			if len(values) != block.Descriptor.RowCount {
				return nil, fmt.Errorf("collections: typed-column adapter column %q block %d decoded rows=%d want %d", column.Definition.Name, blockIdx, len(values), block.Descriptor.RowCount)
			}
			for i, v := range values {
				out[block.Descriptor.FirstRow+i] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, Uint32: v}
			}
		case ColumnStoreValueUint64:
			values, err := reader.DecodeUint64(block.Granule)
			if err != nil {
				return nil, fmt.Errorf("collections: typed-column adapter column %q block %d: %w", column.Definition.Name, blockIdx, err)
			}
			if len(values) != block.Descriptor.RowCount {
				return nil, fmt.Errorf("collections: typed-column adapter column %q block %d decoded rows=%d want %d", column.Definition.Name, blockIdx, len(values), block.Descriptor.RowCount)
			}
			for i, v := range values {
				out[block.Descriptor.FirstRow+i] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, Uint64: v}
			}
		case ColumnStoreValueFloat16:
			values, err := reader.DecodeFloat16Bits(block.Granule)
			if err != nil {
				return nil, fmt.Errorf("collections: typed-column adapter column %q block %d: %w", column.Definition.Name, blockIdx, err)
			}
			if len(values) != block.Descriptor.RowCount {
				return nil, fmt.Errorf("collections: typed-column adapter column %q block %d decoded rows=%d want %d", column.Definition.Name, blockIdx, len(values), block.Descriptor.RowCount)
			}
			for i, v := range values {
				out[block.Descriptor.FirstRow+i] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, Float16: v}
			}
		case ColumnStoreValueBFloat16:
			values, err := reader.DecodeBFloat16Bits(block.Granule)
			if err != nil {
				return nil, fmt.Errorf("collections: typed-column adapter column %q block %d: %w", column.Definition.Name, blockIdx, err)
			}
			if len(values) != block.Descriptor.RowCount {
				return nil, fmt.Errorf("collections: typed-column adapter column %q block %d decoded rows=%d want %d", column.Definition.Name, blockIdx, len(values), block.Descriptor.RowCount)
			}
			for i, v := range values {
				out[block.Descriptor.FirstRow+i] = columnDeclaredValue{Type: column.Field.ValueType, Present: true, BFloat16: v}
			}
		default:
			return nil, fmt.Errorf("%w: %s is not a primitive scalar", errTypedColumnAdapterUnsupportedType, column.Field.ValueType)
		}
	}
	return out, nil
}
