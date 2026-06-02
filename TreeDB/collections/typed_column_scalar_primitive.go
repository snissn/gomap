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

func (p *typedColumnAdapterPart) scanPrimitiveScalarColumnValuesRows(column typedColumnAdapterColumn, rows []int) ([]columnDeclaredValue, typedcolumn.PartScanDiagnostics, error) {
	if rows == nil {
		values, err := p.scanPrimitiveScalarColumnValues(column)
		if err != nil {
			return nil, typedcolumn.PartScanDiagnostics{}, err
		}
		diag, err := p.scanColumnValuesRowsFullDiagnostics(column)
		if err != nil {
			return nil, typedcolumn.PartScanDiagnostics{}, err
		}
		return values, diag, nil
	}
	if !typedColumnAdapterPrimitiveScalarColumnType(column.Definition.Type) {
		return nil, typedcolumn.PartScanDiagnostics{}, fmt.Errorf("%w: %s is not a primitive scalar", errTypedColumnAdapterUnsupportedType, column.Definition.Type)
	}
	partColumn, ok := p.Part.Columns[column.Definition.Name]
	if !ok {
		return nil, typedcolumn.PartScanDiagnostics{}, fmt.Errorf("collections: typed-column adapter missing column %q", column.Definition.Name)
	}
	if partColumn.Definition.Type != column.Definition.Type || partColumn.Definition.Encoding != column.Definition.Encoding {
		return nil, typedcolumn.PartScanDiagnostics{}, fmt.Errorf("collections: typed-column adapter column %q type/encoding=(%s,%s) want (%s,%s)", column.Definition.Name, partColumn.Definition.Type, partColumn.Definition.Encoding, column.Definition.Type, column.Definition.Encoding)
	}
	diag := typedcolumn.PartScanDiagnostics{RowsScanned: len(rows), ColumnsProjected: 1, GranulesConsidered: len(p.Part.Descriptor.Granules)}
	if len(rows) == 0 {
		return []columnDeclaredValue{}, diag, nil
	}
	if err := typedColumnAdapterValidateSelectedRows(rows, p.Part.Descriptor.RowCount); err != nil {
		return nil, typedcolumn.PartScanDiagnostics{}, err
	}
	out := make([]columnDeclaredValue, 0, len(rows))
	var reader typedcolumn.GranuleReader
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
		if err := appendPrimitiveScalarSelectedBlockValues(&out, &reader, column, block, blockIdx, rows[start:rowIndex]); err != nil {
			return nil, diag, err
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

func typedColumnAdapterValidateSelectedRows(rows []int, totalRows int) error {
	previous := -1
	for i, row := range rows {
		if row < 0 || row >= totalRows {
			return fmt.Errorf("collections: typed-column adapter selected row[%d]=%d outside part rows=%d", i, row, totalRows)
		}
		if row <= previous {
			return fmt.Errorf("collections: typed-column adapter selected rows must be strictly increasing at index %d (%d after %d)", i, row, previous)
		}
		previous = row
	}
	return nil
}

func appendPrimitiveScalarSelectedBlockValues(out *[]columnDeclaredValue, reader *typedcolumn.GranuleReader, column typedColumnAdapterColumn, block typedcolumn.ColumnBlock, blockIdx int, rows []int) error {
	first := block.Descriptor.FirstRow
	switch column.Field.ValueType {
	case ColumnStoreValueInt8:
		values, err := reader.DecodeInt8(block.Granule)
		if err != nil {
			return fmt.Errorf("collections: typed-column adapter column %q block %d: %w", column.Definition.Name, blockIdx, err)
		}
		if len(values) != block.Descriptor.RowCount {
			return fmt.Errorf("collections: typed-column adapter column %q block %d decoded rows=%d want %d", column.Definition.Name, blockIdx, len(values), block.Descriptor.RowCount)
		}
		for _, row := range rows {
			*out = append(*out, columnDeclaredValue{Type: column.Field.ValueType, Present: true, Int8: values[row-first]})
		}
	case ColumnStoreValueUint8:
		values, err := reader.DecodeUint8(block.Granule)
		if err != nil {
			return fmt.Errorf("collections: typed-column adapter column %q block %d: %w", column.Definition.Name, blockIdx, err)
		}
		if len(values) != block.Descriptor.RowCount {
			return fmt.Errorf("collections: typed-column adapter column %q block %d decoded rows=%d want %d", column.Definition.Name, blockIdx, len(values), block.Descriptor.RowCount)
		}
		for _, row := range rows {
			*out = append(*out, columnDeclaredValue{Type: column.Field.ValueType, Present: true, Uint8: values[row-first]})
		}
	case ColumnStoreValueInt16:
		values, err := reader.DecodeInt16(block.Granule)
		if err != nil {
			return fmt.Errorf("collections: typed-column adapter column %q block %d: %w", column.Definition.Name, blockIdx, err)
		}
		if len(values) != block.Descriptor.RowCount {
			return fmt.Errorf("collections: typed-column adapter column %q block %d decoded rows=%d want %d", column.Definition.Name, blockIdx, len(values), block.Descriptor.RowCount)
		}
		for _, row := range rows {
			*out = append(*out, columnDeclaredValue{Type: column.Field.ValueType, Present: true, Int16: values[row-first]})
		}
	case ColumnStoreValueUint16:
		values, err := reader.DecodeUint16(block.Granule)
		if err != nil {
			return fmt.Errorf("collections: typed-column adapter column %q block %d: %w", column.Definition.Name, blockIdx, err)
		}
		if len(values) != block.Descriptor.RowCount {
			return fmt.Errorf("collections: typed-column adapter column %q block %d decoded rows=%d want %d", column.Definition.Name, blockIdx, len(values), block.Descriptor.RowCount)
		}
		for _, row := range rows {
			*out = append(*out, columnDeclaredValue{Type: column.Field.ValueType, Present: true, Uint16: values[row-first]})
		}
	case ColumnStoreValueInt32:
		values, err := reader.DecodeInt32(block.Granule)
		if err != nil {
			return fmt.Errorf("collections: typed-column adapter column %q block %d: %w", column.Definition.Name, blockIdx, err)
		}
		if len(values) != block.Descriptor.RowCount {
			return fmt.Errorf("collections: typed-column adapter column %q block %d decoded rows=%d want %d", column.Definition.Name, blockIdx, len(values), block.Descriptor.RowCount)
		}
		for _, row := range rows {
			*out = append(*out, columnDeclaredValue{Type: column.Field.ValueType, Present: true, Int32: values[row-first]})
		}
	case ColumnStoreValueUint32:
		values, err := reader.DecodeUint32(block.Granule)
		if err != nil {
			return fmt.Errorf("collections: typed-column adapter column %q block %d: %w", column.Definition.Name, blockIdx, err)
		}
		if len(values) != block.Descriptor.RowCount {
			return fmt.Errorf("collections: typed-column adapter column %q block %d decoded rows=%d want %d", column.Definition.Name, blockIdx, len(values), block.Descriptor.RowCount)
		}
		for _, row := range rows {
			*out = append(*out, columnDeclaredValue{Type: column.Field.ValueType, Present: true, Uint32: values[row-first]})
		}
	case ColumnStoreValueUint64:
		values, err := reader.DecodeUint64(block.Granule)
		if err != nil {
			return fmt.Errorf("collections: typed-column adapter column %q block %d: %w", column.Definition.Name, blockIdx, err)
		}
		if len(values) != block.Descriptor.RowCount {
			return fmt.Errorf("collections: typed-column adapter column %q block %d decoded rows=%d want %d", column.Definition.Name, blockIdx, len(values), block.Descriptor.RowCount)
		}
		for _, row := range rows {
			*out = append(*out, columnDeclaredValue{Type: column.Field.ValueType, Present: true, Uint64: values[row-first]})
		}
	case ColumnStoreValueFloat16:
		values, err := reader.DecodeFloat16Bits(block.Granule)
		if err != nil {
			return fmt.Errorf("collections: typed-column adapter column %q block %d: %w", column.Definition.Name, blockIdx, err)
		}
		if len(values) != block.Descriptor.RowCount {
			return fmt.Errorf("collections: typed-column adapter column %q block %d decoded rows=%d want %d", column.Definition.Name, blockIdx, len(values), block.Descriptor.RowCount)
		}
		for _, row := range rows {
			*out = append(*out, columnDeclaredValue{Type: column.Field.ValueType, Present: true, Float16: values[row-first]})
		}
	case ColumnStoreValueBFloat16:
		values, err := reader.DecodeBFloat16Bits(block.Granule)
		if err != nil {
			return fmt.Errorf("collections: typed-column adapter column %q block %d: %w", column.Definition.Name, blockIdx, err)
		}
		if len(values) != block.Descriptor.RowCount {
			return fmt.Errorf("collections: typed-column adapter column %q block %d decoded rows=%d want %d", column.Definition.Name, blockIdx, len(values), block.Descriptor.RowCount)
		}
		for _, row := range rows {
			*out = append(*out, columnDeclaredValue{Type: column.Field.ValueType, Present: true, BFloat16: values[row-first]})
		}
	default:
		return fmt.Errorf("%w: %s is not a primitive scalar", errTypedColumnAdapterUnsupportedType, column.Field.ValueType)
	}
	return nil
}
