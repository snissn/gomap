package colgranule

import "testing"

func TestEstimateAdaptiveRowsPerMark(t *testing.T) {
	tests := []struct {
		name      string
		rows      int
		rawBytes  int
		cfg       ColumnAdaptiveMarkSizing
		wantRows  int
		wantMin   bool
		wantMax   bool
		wantMarks int
	}{
		{
			name:      "narrow rows clamp to max",
			rows:      100_000,
			rawBytes:  100_000 * 8,
			cfg:       ColumnAdaptiveMarkSizing{Enabled: true, TargetBytes: 1 << 20, MinRows: 128, MaxRows: 8192},
			wantRows:  8192,
			wantMax:   true,
			wantMarks: 13,
		},
		{
			name:      "wide rows target bytes",
			rows:      100_000,
			rawBytes:  100_000 * 4096,
			cfg:       ColumnAdaptiveMarkSizing{Enabled: true, TargetBytes: 1 << 20, MinRows: 128, MaxRows: 8192},
			wantRows:  256,
			wantMarks: 391,
		},
		{
			name:      "very wide rows clamp to min",
			rows:      100_000,
			rawBytes:  100_000 * 65536,
			cfg:       ColumnAdaptiveMarkSizing{Enabled: true, TargetBytes: 1 << 20, MinRows: 128, MaxRows: 8192},
			wantRows:  128,
			wantMin:   true,
			wantMarks: 782,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EstimateAdaptiveRowsPerMark(tt.rows, tt.rawBytes, tt.cfg)
			if err != nil {
				t.Fatalf("EstimateAdaptiveRowsPerMark: %v", err)
			}
			if got.RowsPerMark != tt.wantRows || got.ClampedByMin != tt.wantMin || got.ClampedByMax != tt.wantMax || got.Marks != tt.wantMarks {
				t.Fatalf("estimate=%+v want rows=%d min=%v max=%v marks=%d", got, tt.wantRows, tt.wantMin, tt.wantMax, tt.wantMarks)
			}
			if got.RawBytesPerRow <= 0 {
				t.Fatalf("raw bytes per row=%f want >0", got.RawBytesPerRow)
			}
		})
	}
}

func TestBuildColumnPartUsesAdaptiveMarkSizing(t *testing.T) {
	rows := 20
	batch := ColumnBatch{Rows: rows, Columns: map[string][]int64{
		"id":        make([]int64, rows),
		"time_us":   make([]int64, rows),
		"value":     make([]int64, rows),
		"kind_code": make([]int64, rows),
		"has_reply": make([]int64, rows),
	}}
	for i := 0; i < rows; i++ {
		batch.Columns["id"][i] = int64(i)
		batch.Columns["time_us"][i] = int64(1000 + i)
		batch.Columns["value"][i] = int64(i * 10)
		batch.Columns["kind_code"][i] = int64(i % 3)
		batch.Columns["has_reply"][i] = int64(i % 2)
	}
	opts := partTestOptions([]SortKeyColumn{{Column: "id"}})
	opts.PartPolicy.RowsPerGranule = 0
	opts.PartPolicy.DefaultCodecBlockRows = 0
	opts.PartPolicy.AdaptiveMarkSizing = ColumnAdaptiveMarkSizing{
		Enabled:     true,
		TargetBytes: 100,
		MinRows:     2,
		MaxRows:     10,
	}

	part, err := BuildColumnPart(77, opts, batch)
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	if got := part.Options.PartPolicy.RowsPerGranule; got != 3 {
		t.Fatalf("adaptive rows per granule=%d want 3", got)
	}
	if got, want := len(part.Descriptor.Granules), 7; got != want {
		t.Fatalf("granules=%d want %d", got, want)
	}
	for i, granule := range part.Descriptor.Granules[:len(part.Descriptor.Granules)-1] {
		if granule.RowCount != 3 {
			t.Fatalf("granule %d row count=%d want 3", i, granule.RowCount)
		}
	}
}

func TestNormalizeColumnAdaptiveMarkSizingRejectsInvalidClamp(t *testing.T) {
	if _, err := NormalizeColumnAdaptiveMarkSizing(ColumnAdaptiveMarkSizing{Enabled: true, MinRows: 9, MaxRows: 8}, 0); err == nil {
		t.Fatal("NormalizeColumnAdaptiveMarkSizing accepted min > max")
	}
}

func BenchmarkAdaptiveMarkSizingWideRows(b *testing.B) {
	const rows = 100_000
	const rawBytes = rows * 4096
	cfg := ColumnAdaptiveMarkSizing{
		Enabled:     true,
		TargetBytes: 1 << 20,
		MinRows:     128,
		MaxRows:     8192,
	}
	estimate, err := EstimateAdaptiveRowsPerMark(rows, rawBytes, cfg)
	if err != nil {
		b.Fatalf("EstimateAdaptiveRowsPerMark: %v", err)
	}
	fixedRowsPerMark := 8192
	fixedMarks := (rows + fixedRowsPerMark - 1) / fixedRowsPerMark
	b.ReportMetric(float64(fixedRowsPerMark), "fixed_rows_per_mark")
	b.ReportMetric(float64(fixedMarks), "fixed_marks")
	b.ReportMetric(float64(rawBytes/fixedMarks), "fixed_raw_bytes_per_mark")
	b.ReportMetric(float64(estimate.RowsPerMark), "adaptive_rows_per_mark")
	b.ReportMetric(float64(estimate.Marks), "adaptive_marks")
	b.ReportMetric(float64(rawBytes/estimate.Marks), "adaptive_raw_bytes_per_mark")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := EstimateAdaptiveRowsPerMark(rows, rawBytes, cfg)
		if err != nil {
			b.Fatal(err)
		}
		benchSink += int64(got.RowsPerMark + got.Marks)
	}
}
