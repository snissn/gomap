package typedcolumn

import "fmt"

const (
	DefaultAdaptiveMarkTargetBytes = 1 << 20
	DefaultAdaptiveMarkMinRows     = 512
)

type ColumnAdaptiveMarkSizing struct {
	Enabled     bool `json:"enabled,omitempty"`
	TargetBytes int  `json:"target_bytes,omitempty"`
	MinRows     int  `json:"min_rows,omitempty"`
	MaxRows     int  `json:"max_rows,omitempty"`
}

type ColumnAdaptiveMarkEstimate struct {
	Rows           int     `json:"rows"`
	RawBytes       int     `json:"raw_bytes"`
	RawBytesPerRow float64 `json:"raw_bytes_per_row"`
	TargetBytes    int     `json:"target_bytes"`
	MinRows        int     `json:"min_rows"`
	MaxRows        int     `json:"max_rows"`
	RowsPerMark    int     `json:"rows_per_mark"`
	Marks          int     `json:"marks"`
	ClampedByMin   bool    `json:"clamped_by_min,omitempty"`
	ClampedByMax   bool    `json:"clamped_by_max,omitempty"`
}

func NormalizeColumnAdaptiveMarkSizing(cfg ColumnAdaptiveMarkSizing, rowsPerGranule int) (ColumnAdaptiveMarkSizing, error) {
	if !cfg.Enabled {
		return cfg, nil
	}
	if cfg.TargetBytes == 0 {
		cfg.TargetBytes = DefaultAdaptiveMarkTargetBytes
	}
	if cfg.TargetBytes < 0 {
		return ColumnAdaptiveMarkSizing{}, fmt.Errorf("typedcolumn: invalid adaptive mark target bytes %d", cfg.TargetBytes)
	}
	if cfg.MinRows == 0 {
		cfg.MinRows = DefaultAdaptiveMarkMinRows
	}
	if cfg.MinRows <= 0 {
		return ColumnAdaptiveMarkSizing{}, fmt.Errorf("typedcolumn: invalid adaptive mark min rows %d", cfg.MinRows)
	}
	if cfg.MaxRows == 0 {
		if rowsPerGranule > 0 {
			cfg.MaxRows = rowsPerGranule
		} else {
			cfg.MaxRows = DefaultRowsPerGranule
		}
	}
	if cfg.MaxRows <= 0 {
		return ColumnAdaptiveMarkSizing{}, fmt.Errorf("typedcolumn: invalid adaptive mark max rows %d", cfg.MaxRows)
	}
	if cfg.MinRows > cfg.MaxRows {
		return ColumnAdaptiveMarkSizing{}, fmt.Errorf("typedcolumn: adaptive mark min rows %d exceeds max rows %d", cfg.MinRows, cfg.MaxRows)
	}
	return cfg, nil
}

func EstimateAdaptiveRowsPerMark(rows int, rawBytes int, cfg ColumnAdaptiveMarkSizing) (ColumnAdaptiveMarkEstimate, error) {
	cfg, err := NormalizeColumnAdaptiveMarkSizing(cfg, 0)
	if err != nil {
		return ColumnAdaptiveMarkEstimate{}, err
	}
	if !cfg.Enabled {
		return ColumnAdaptiveMarkEstimate{}, fmt.Errorf("typedcolumn: adaptive mark sizing is disabled")
	}
	if rows <= 0 {
		return ColumnAdaptiveMarkEstimate{}, fmt.Errorf("typedcolumn: invalid adaptive mark rows %d", rows)
	}
	if rawBytes < 0 {
		return ColumnAdaptiveMarkEstimate{}, fmt.Errorf("typedcolumn: invalid adaptive mark raw bytes %d", rawBytes)
	}
	bytesPerRow := 1
	if rawBytes > 0 {
		bytesPerRow = (rawBytes + rows - 1) / rows
	}
	rowsPerMark := cfg.TargetBytes / bytesPerRow
	if rowsPerMark == 0 {
		rowsPerMark = 1
	}
	estimate := ColumnAdaptiveMarkEstimate{
		Rows:           rows,
		RawBytes:       rawBytes,
		RawBytesPerRow: float64(rawBytes) / float64(rows),
		TargetBytes:    cfg.TargetBytes,
		MinRows:        cfg.MinRows,
		MaxRows:        cfg.MaxRows,
		RowsPerMark:    rowsPerMark,
	}
	if estimate.RowsPerMark < cfg.MinRows {
		estimate.RowsPerMark = cfg.MinRows
		estimate.ClampedByMin = true
	}
	if estimate.RowsPerMark > cfg.MaxRows {
		estimate.RowsPerMark = cfg.MaxRows
		estimate.ClampedByMax = true
	}
	estimate.Marks = (rows + estimate.RowsPerMark - 1) / estimate.RowsPerMark
	return estimate, nil
}

func EstimateBatchUncompressedBytes(batch Batch, defs []ColumnDefinition) (int, error) {
	rows, err := validateBatch(batch, defs)
	if err != nil {
		return 0, err
	}
	total := 0
	for _, def := range defs {
		switch def.Type {
		case ColumnTypeInt64:
			total += rows * 8
		case ColumnTypeLowCardinalityCode:
			total += rows * 4
		case ColumnTypeBool:
			total += rows
		case ColumnTypeFloat32Vector:
			rowBytes, err := checkedMulInt(def.FixedWidthElements, 4, "dense column row bytes")
			if err != nil {
				return 0, err
			}
			bytes, err := checkedMulInt(rows, rowBytes, "dense column uncompressed bytes")
			if err != nil {
				return 0, err
			}
			total += bytes
		case ColumnTypeUint8Vector, ColumnTypeInt8Vector, ColumnTypeUint16Vector, ColumnTypeInt16Vector, ColumnTypeUint32Vector, ColumnTypeInt32Vector, ColumnTypeUint64Vector, ColumnTypeInt64Vector, ColumnTypeFloat16Vector, ColumnTypeBFloat16Vector, ColumnTypeFloat64Vector:
			width, ok := DenseFixedWidthVectorElementWidth(def.Type)
			if !ok {
				return 0, fmt.Errorf("typedcolumn: unsupported dense vector type %s for adaptive mark sizing", def.Type)
			}
			rowBytes, err := checkedMulInt(def.FixedWidthElements, width, "dense column row bytes")
			if err != nil {
				return 0, err
			}
			bytes, err := checkedMulInt(rows, rowBytes, "dense column uncompressed bytes")
			if err != nil {
				return 0, err
			}
			total += bytes
		case ColumnTypeFixedBytes:
			bytes, err := FixedBytesPayloadBytes(rows, def.FixedWidthElements)
			if err != nil {
				return 0, err
			}
			total += bytes
		case ColumnTypePackedBitVector, ColumnTypePackedUint2Vector, ColumnTypePackedUint4Vector:
			bitsPerElement, ok := PackedUintVectorBits(def.Type)
			if !ok {
				return 0, fmt.Errorf("typedcolumn: unsupported packed_uint type %s for adaptive mark sizing", def.Type)
			}
			bytes, err := PackedUintPayloadBytes(rows, def.FixedWidthElements, bitsPerElement)
			if err != nil {
				return 0, err
			}
			total += bytes
		case ColumnTypeUint32List:
			list := batch.Uint32OffsetsLists[def.Name]
			offsetsBytes, err := checkedMulInt(list.Rows+1, 8, "offsets-list offsets bytes")
			if err != nil {
				return 0, err
			}
			valuesBytes, err := checkedMulInt(len(list.Values), 4, "offsets-list values bytes")
			if err != nil {
				return 0, err
			}
			bytes, err := checkedAddInt(offsetsBytes, valuesBytes, "offsets-list bytes")
			if err != nil {
				return 0, err
			}
			total += bytes
		case ColumnTypeAdjacencyList:
			if def.Encoding == EncodingRawUint32OffsetsList {
				list := batch.Uint32OffsetsLists[def.Name]
				offsetsBytes, err := checkedMulInt(list.Rows+1, 8, "offsets-list offsets bytes")
				if err != nil {
					return 0, err
				}
				valuesBytes, err := checkedMulInt(len(list.Values), 4, "offsets-list values bytes")
				if err != nil {
					return 0, err
				}
				bytes, err := checkedAddInt(offsetsBytes, valuesBytes, "offsets-list bytes")
				if err != nil {
					return 0, err
				}
				total += bytes
				continue
			}
			rowBytes, err := checkedMulInt(def.FixedWidthElements, 4, "dense column row bytes")
			if err != nil {
				return 0, err
			}
			bytes, err := checkedMulInt(rows, rowBytes, "dense column uncompressed bytes")
			if err != nil {
				return 0, err
			}
			total += bytes
		default:
			return 0, fmt.Errorf("typedcolumn: unsupported column type %s for adaptive mark sizing", def.Type)
		}
	}
	return total, nil
}
