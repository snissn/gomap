package colgranule

import (
	"fmt"
	"runtime"
	"time"
)

type JSONBenchPartBuildReport struct {
	Layout               JSONBenchColumnPartLayout       `json:"layout"`
	Rows                 int                             `json:"rows"`
	RowsPerGranule       int                             `json:"rows_per_granule"`
	Columns              int                             `json:"columns"`
	Attempts             []JSONBenchPartBuildAttempt     `json:"attempts"`
	Best                 JSONBenchPartBuildAttempt       `json:"best"`
	Accounting           ColumnPartByteAccounting        `json:"accounting"`
	CompressionRows      []JSONBenchCompressionReportRow `json:"compression_rows"`
	RawJSONBytes         int64                           `json:"raw_json_bytes"`
	DictionaryBytes      int                             `json:"dictionary_bytes"`
	RowsPerSecond        float64                         `json:"rows_per_second"`
	EncodedMiBPerSecond  float64                         `json:"encoded_mib_per_second"`
	StoredMiBPerSecond   float64                         `json:"stored_mib_per_second"`
	NanosPerRow          float64                         `json:"nanos_per_row"`
	AllocatedBytesPerOp  uint64                          `json:"allocated_bytes_per_op"`
	AllocsPerOp          uint64                          `json:"allocs_per_op"`
	AllocatedBytesPerRow float64                         `json:"allocated_bytes_per_row"`
	TemporaryBytes       uint64                          `json:"temporary_bytes_estimate"`
	TemporaryBytesPerRow float64                         `json:"temporary_bytes_per_row"`
}

type JSONBenchPartBuildAttempt struct {
	Duration                  time.Duration `json:"duration"`
	AllocatedBytes            uint64        `json:"allocated_bytes"`
	Mallocs                   uint64        `json:"mallocs"`
	TemporaryBytes            uint64        `json:"temporary_bytes_estimate"`
	TotalBytes                int           `json:"total_bytes"`
	EncodedRawBytes           int           `json:"encoded_raw_bytes"`
	DeclaredColumnStoredBytes int           `json:"declared_column_stored_bytes"`
}

func RunJSONBenchPartBuildReports(ds JSONBenchDataset, rowsPerGranule int, attempts int) ([]JSONBenchPartBuildReport, error) {
	if attempts <= 0 {
		attempts = 1
	}
	if rowsPerGranule <= 0 {
		rowsPerGranule = DefaultRowsPerGranule
	}
	layouts := []JSONBenchColumnPartLayout{
		JSONBenchColumnPartLayoutTimeUS,
		JSONBenchColumnPartLayoutClickHouseFilterUserTime,
	}
	out := make([]JSONBenchPartBuildReport, 0, len(layouts))
	for _, layout := range layouts {
		report, err := runJSONBenchPartBuildReport(ds, rowsPerGranule, attempts, layout)
		if err != nil {
			return nil, err
		}
		out = append(out, report)
	}
	return out, nil
}

func runJSONBenchPartBuildReport(ds JSONBenchDataset, rowsPerGranule int, attempts int, layout JSONBenchColumnPartLayout) (JSONBenchPartBuildReport, error) {
	var report JSONBenchPartBuildReport
	report.Layout = layout
	report.Rows = ds.Rows
	report.RowsPerGranule = rowsPerGranule
	report.Columns = len(ds.Columns)
	report.RawJSONBytes = JSONBenchRawDocumentBytes(ds)
	for i := 0; i < attempts; i++ {
		part, attempt, accounting, err := measureJSONBenchPartBuild(ds, rowsPerGranule, layout)
		if err != nil {
			return JSONBenchPartBuildReport{}, fmt.Errorf("build layout=%s attempt=%d: %w", layout, i, err)
		}
		if part.Descriptor.RowCount != ds.Rows {
			return JSONBenchPartBuildReport{}, fmt.Errorf("build layout=%s rows=%d want=%d", layout, part.Descriptor.RowCount, ds.Rows)
		}
		report.Attempts = append(report.Attempts, attempt)
		if i == 0 || attempt.Duration < report.Best.Duration {
			report.Best = attempt
			report.Accounting = accounting
		}
	}
	report.DictionaryBytes = report.Accounting.DictionaryBytes
	report.fillDerivedMetrics()
	report.CompressionRows = jsonBenchPartBuildCompressionRows(report)
	return report, nil
}

func measureJSONBenchPartBuild(ds JSONBenchDataset, rowsPerGranule int, layout JSONBenchColumnPartLayout) (*ColumnPart, JSONBenchPartBuildAttempt, ColumnPartByteAccounting, error) {
	var before runtime.MemStats
	var after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	part, err := BuildJSONBenchColumnPartWithAggregateMetadataForLayout(ds, rowsPerGranule, layout)
	if err != nil {
		return nil, JSONBenchPartBuildAttempt{}, ColumnPartByteAccounting{}, err
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{Dictionaries: ds.Dictionaries})
	if err != nil {
		return nil, JSONBenchPartBuildAttempt{}, ColumnPartByteAccounting{}, err
	}
	duration := time.Since(start)
	runtime.ReadMemStats(&after)
	parsedImage, err := ParseColumnPartImage(image.Bytes)
	if err != nil {
		return nil, JSONBenchPartBuildAttempt{}, ColumnPartByteAccounting{}, err
	}
	if _, err := ColumnPartFromImage(parsedImage); err != nil {
		return nil, JSONBenchPartBuildAttempt{}, ColumnPartByteAccounting{}, err
	}
	accounting := part.ByteAccountingFromImage(parsedImage)
	allocated := after.TotalAlloc - before.TotalAlloc
	temporary := uint64(0)
	if allocated > uint64(accounting.TotalStoredBytes) {
		temporary = allocated - uint64(accounting.TotalStoredBytes)
	}
	attempt := JSONBenchPartBuildAttempt{
		Duration:                  duration,
		AllocatedBytes:            allocated,
		Mallocs:                   after.Mallocs - before.Mallocs,
		TemporaryBytes:            temporary,
		TotalBytes:                accounting.TotalStoredBytes,
		EncodedRawBytes:           accounting.EncodedRawBytes,
		DeclaredColumnStoredBytes: accounting.DeclaredColumnStoredBytes,
	}
	return part, attempt, accounting, nil
}

func (r *JSONBenchPartBuildReport) fillDerivedMetrics() {
	seconds := r.Best.Duration.Seconds()
	if seconds > 0 {
		r.RowsPerSecond = float64(r.Rows) / seconds
		r.EncodedMiBPerSecond = float64(r.Accounting.EncodedRawBytes) / seconds / 1024 / 1024
		r.StoredMiBPerSecond = float64(r.Accounting.TotalStoredBytes) / seconds / 1024 / 1024
		if r.Rows > 0 {
			r.NanosPerRow = float64(r.Best.Duration.Nanoseconds()) / float64(r.Rows)
		}
	}
	r.AllocatedBytesPerOp = r.Best.AllocatedBytes
	r.AllocsPerOp = r.Best.Mallocs
	r.TemporaryBytes = r.Best.TemporaryBytes
	if r.Rows > 0 {
		r.AllocatedBytesPerRow = float64(r.Best.AllocatedBytes) / float64(r.Rows)
		r.TemporaryBytesPerRow = float64(r.Best.TemporaryBytes) / float64(r.Rows)
	}
}

func jsonBenchPartBuildCompressionRows(report JSONBenchPartBuildReport) []JSONBenchCompressionReportRow {
	rows := make([]JSONBenchCompressionReportRow, 0, len(report.Accounting.CompressionDetail))
	for _, detail := range report.Accounting.CompressionDetail {
		rows = append(rows, JSONBenchCompressionReportRow{
			CodecLayoutLabel:            string(report.Layout) + "/" + detail.Column + "/" + detail.Encoding.String(),
			CompressionPolicyLabel:      jsonBenchCompressionPolicyLabel(detail.RequestedCompression),
			RequestedCompression:        detail.RequestedCompression.String(),
			ActualCompression:           detail.ActualCompression.String(),
			SupportState:                "supported",
			CompressedBytes:             detail.StoredBytes,
			CompressedBytesSource:       "column_part_byte_accounting.compression_detail.stored_bytes",
			RawBytes:                    detail.EncodedRawBytes,
			RawBytesSource:              "column_part_byte_accounting.compression_detail.encoded_raw_bytes",
			DecompressedBytes:           detail.EncodedRawBytes,
			DecompressedBytesSource:     "column_part_byte_accounting.compression_detail.encoded_raw_bytes",
			CompressionRatio:            jsonBenchCompressionRatio(detail.StoredBytes, detail.EncodedRawBytes),
			CompressionRatioSource:      "compressed_bytes/decompressed_bytes",
			CompressionDuration:         time.Duration(detail.CompressionNanos),
			CompressionDurationSource:   "typedcolumn_codec_report_compression_nanos",
			DecompressionDuration:       0,
			DecompressionDurationSource: "not_measured_by_part_build_report",
			BenchmarkBPerOp:             report.AllocatedBytesPerOp,
			BenchmarkAllocsPerOp:        report.AllocsPerOp,
			BenchmarkAllocationSource:   "runtime_memstats_best_part_build_attempt_whole_operation",
		})
	}
	return rows
}

func JSONBenchRawDocumentBytes(ds JSONBenchDataset) int64 {
	var total int64
	for _, bytes := range ds.Columns["line_bytes"] {
		total += bytes
	}
	return total
}
