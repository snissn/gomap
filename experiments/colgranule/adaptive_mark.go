package colgranule

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
		return ColumnAdaptiveMarkSizing{}, fmt.Errorf("colgranule: invalid adaptive mark target bytes %d", cfg.TargetBytes)
	}
	if cfg.MinRows == 0 {
		cfg.MinRows = DefaultAdaptiveMarkMinRows
	}
	if cfg.MinRows <= 0 {
		return ColumnAdaptiveMarkSizing{}, fmt.Errorf("colgranule: invalid adaptive mark min rows %d", cfg.MinRows)
	}
	if cfg.MaxRows == 0 {
		if rowsPerGranule > 0 {
			cfg.MaxRows = rowsPerGranule
		} else {
			cfg.MaxRows = DefaultRowsPerGranule
		}
	}
	if cfg.MaxRows <= 0 {
		return ColumnAdaptiveMarkSizing{}, fmt.Errorf("colgranule: invalid adaptive mark max rows %d", cfg.MaxRows)
	}
	if cfg.MinRows > cfg.MaxRows {
		return ColumnAdaptiveMarkSizing{}, fmt.Errorf("colgranule: adaptive mark min rows %d exceeds max rows %d", cfg.MinRows, cfg.MaxRows)
	}
	return cfg, nil
}

func EstimateAdaptiveRowsPerMark(rows int, rawBytes int, cfg ColumnAdaptiveMarkSizing) (ColumnAdaptiveMarkEstimate, error) {
	cfg, err := NormalizeColumnAdaptiveMarkSizing(cfg, 0)
	if err != nil {
		return ColumnAdaptiveMarkEstimate{}, err
	}
	if !cfg.Enabled {
		return ColumnAdaptiveMarkEstimate{}, fmt.Errorf("colgranule: adaptive mark sizing is disabled")
	}
	if rows <= 0 {
		return ColumnAdaptiveMarkEstimate{}, fmt.Errorf("colgranule: invalid adaptive mark rows %d", rows)
	}
	if rawBytes < 0 {
		return ColumnAdaptiveMarkEstimate{}, fmt.Errorf("colgranule: invalid adaptive mark raw bytes %d", rawBytes)
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

func EstimateColumnBatchUncompressedBytes(batch ColumnBatch, defs []ColumnDefinition) (int, error) {
	rows, err := validateColumnBatch(batch, defs)
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
		default:
			return 0, fmt.Errorf("colgranule: unsupported column type %s for adaptive mark sizing", def.Type)
		}
	}
	return total, nil
}
