package colgranule

import (
	"fmt"
	"math"
	"strings"
	"time"
)

type JSONBenchCompressionReportRow struct {
	CodecLayoutLabel            string        `json:"codec_layout_label"`
	CompressionPolicyLabel      string        `json:"compression_policy_label"`
	RequestedCompression        string        `json:"requested_compression"`
	ActualCompression           string        `json:"actual_compression"`
	SupportState                string        `json:"support_state"`
	SupportReason               string        `json:"support_reason,omitempty"`
	CompressedBytes             int           `json:"compressed_bytes"`
	CompressedBytesSource       string        `json:"compressed_bytes_source"`
	RawBytes                    int           `json:"raw_bytes"`
	RawBytesSource              string        `json:"raw_bytes_source"`
	DecompressedBytes           int           `json:"decompressed_bytes"`
	DecompressedBytesSource     string        `json:"decompressed_bytes_source"`
	CompressionRatio            float64       `json:"compression_ratio"`
	CompressionRatioSource      string        `json:"compression_ratio_source"`
	CompressionDuration         time.Duration `json:"compression_duration"`
	CompressionDurationSource   string        `json:"compression_duration_source"`
	DecompressionDuration       time.Duration `json:"decompression_duration"`
	DecompressionDurationSource string        `json:"decompression_duration_source"`
	BenchmarkBPerOp             uint64        `json:"benchmark_b_per_op"`
	BenchmarkAllocsPerOp        uint64        `json:"benchmark_allocs_per_op"`
	BenchmarkAllocationSource   string        `json:"benchmark_allocation_source"`
}

type ColumnCodecSummary struct {
	Column               string
	Rows                 int
	Granules             int
	Encoding             Encoding
	RequestedCompression Compression
	ActualCompressionMix map[Compression]int
	ValueBytes           int
	EncodedRawBytes      int
	StoredBytes          int
	CompressionRow       JSONBenchCompressionReportRow
	EncodeDuration       time.Duration
	DecodeDuration       time.Duration
	RangeScanDuration    time.Duration
	RangeScanMatches     int
}

func SummarizeJSONBenchDataset(ds JSONBenchDataset, rowsPerGranule int, configs []Config) ([]ColumnCodecSummary, error) {
	if rowsPerGranule <= 0 {
		rowsPerGranule = DefaultRowsPerGranule
	}
	var out []ColumnCodecSummary
	for _, name := range ds.ColumnNames() {
		values := ds.Columns[name]
		low, high := middleRange(values)
		for _, cfg := range configs {
			summary := ColumnCodecSummary{
				Column:               name,
				Rows:                 len(values),
				Encoding:             cfg.Encoding,
				RequestedCompression: cfg.Compression,
				ActualCompressionMix: make(map[Compression]int),
				ValueBytes:           len(values) * 8,
			}
			encoded := make([]EncodedGranule, 0, (len(values)+rowsPerGranule-1)/rowsPerGranule)
			var compressionNanos int64
			start := time.Now()
			for startRow := 0; startRow < len(values); startRow += rowsPerGranule {
				end := startRow + rowsPerGranule
				if end > len(values) {
					end = len(values)
				}
				g, err := EncodeInt64(nil, values[startRow:end], cfg)
				if err != nil {
					return nil, fmt.Errorf("encode column=%s config=%s/%s rows=%d:%d: %w", name, cfg.Encoding, cfg.Compression, startRow, end, err)
				}
				encoded = append(encoded, g)
				summary.EncodedRawBytes += g.RawBytes
				summary.StoredBytes += g.StoredBytes
				summary.ActualCompressionMix[g.Compression]++
				compressionNanos += g.CodecReport.CompressionNanos
			}
			summary.EncodeDuration = time.Since(start)
			summary.Granules = len(encoded)

			start = time.Now()
			var reader GranuleReader
			for _, g := range encoded {
				if _, err := reader.DecodeInt64(g); err != nil {
					return nil, fmt.Errorf("decode column=%s config=%s/%s: %w", name, cfg.Encoding, cfg.Compression, err)
				}
			}
			summary.DecodeDuration = time.Since(start)

			start = time.Now()
			var scanReader GranuleReader
			for _, g := range encoded {
				count, err := scanReader.RangeScanCountInt64(g, low, high)
				if err != nil {
					return nil, fmt.Errorf("range scan column=%s config=%s/%s: %w", name, cfg.Encoding, cfg.Compression, err)
				}
				summary.RangeScanMatches += count
			}
			summary.RangeScanDuration = time.Since(start)
			summary.CompressionRow = jsonBenchCompressionReportRowFromSummary(summary, time.Duration(compressionNanos))
			out = append(out, summary)
		}
	}
	return out, nil
}

func jsonBenchCompressionReportRowFromSummary(summary ColumnCodecSummary, compressionDuration time.Duration) JSONBenchCompressionReportRow {
	return JSONBenchCompressionReportRow{
		CodecLayoutLabel:            jsonBenchCodecLayoutLabel(summary.Encoding),
		CompressionPolicyLabel:      jsonBenchCompressionPolicyLabel(summary.RequestedCompression),
		RequestedCompression:        summary.RequestedCompression.String(),
		ActualCompression:           formatCompressionMix(summary.ActualCompressionMix),
		SupportState:                "supported",
		CompressedBytes:             summary.StoredBytes,
		CompressedBytesSource:       "encoded_granule_stored_bytes",
		RawBytes:                    summary.EncodedRawBytes,
		RawBytesSource:              "encoded_granule_raw_bytes",
		DecompressedBytes:           summary.EncodedRawBytes,
		DecompressedBytesSource:     "encoded_granule_raw_bytes",
		CompressionRatio:            jsonBenchCompressionRatio(summary.StoredBytes, summary.EncodedRawBytes),
		CompressionRatioSource:      "compressed_bytes/decompressed_bytes",
		CompressionDuration:         compressionDuration,
		CompressionDurationSource:   "typedcolumn_codec_report_compression_nanos",
		DecompressionDuration:       summary.DecodeDuration,
		DecompressionDurationSource: "summarize_jsonbench_dataset_decode_wall_clock",
		BenchmarkBPerOp:             0,
		BenchmarkAllocsPerOp:        0,
		BenchmarkAllocationSource:   "not_measured_by_summary; use go test -bench BenchmarkJSONBenchLocalColumns -benchmem for B/op and allocs/op",
	}
}

func jsonBenchCodecLayoutLabel(encoding Encoding) string {
	return "int64_granule/" + encoding.String()
}

func jsonBenchCompressionPolicyLabel(compression Compression) string {
	if compression == CompressionNone {
		return "compression_off"
	}
	return "requested_" + compression.String()
}

func jsonBenchCompressionRatio(compressedBytes, rawBytes int) float64 {
	if rawBytes <= 0 {
		return 0
	}
	return float64(compressedBytes) / float64(rawBytes)
}

func DefaultJSONBenchConfigs() []Config {
	return []Config{
		{Encoding: EncodingRawInt64, Compression: CompressionNone},
		{Encoding: EncodingRawInt64, Compression: CompressionSnappy},
		{Encoding: EncodingRawInt64, Compression: CompressionLZ4},
		{Encoding: EncodingDeltaVarint, Compression: CompressionNone},
		{Encoding: EncodingDeltaVarint, Compression: CompressionSnappy},
		{Encoding: EncodingDeltaVarint, Compression: CompressionLZ4},
		{Encoding: EncodingDoubleDeltaVarint, Compression: CompressionNone},
		{Encoding: EncodingDoubleDeltaVarint, Compression: CompressionSnappy},
		{Encoding: EncodingDoubleDeltaVarint, Compression: CompressionLZ4},
	}
}

func FormatColumnCodecSummary(s ColumnCodecSummary) string {
	ratioValues := 0.0
	if s.ValueBytes > 0 {
		ratioValues = float64(s.StoredBytes) / float64(s.ValueBytes)
	}
	ratioEncoded := 0.0
	if s.EncodedRawBytes > 0 {
		ratioEncoded = float64(s.StoredBytes) / float64(s.EncodedRawBytes)
	}
	return fmt.Sprintf("%s\trows=%d\tgranules=%d\tcodec_layout=%s\tcompression_policy=%s\tencoding=%s\trequested=%s\tactual=%s\tcompressed_bytes=%d\tdecompressed_bytes=%d\traw_bytes=%d\tvalue_bytes=%d\tencoded_raw_bytes=%d\tstored_bytes=%d\tratio=%.6f\tratio_vs_values=%.6f\tratio_vs_encoded=%.6f\tcompression_duration=%s\tcompression_duration_source=%s\tdecompression_duration=%s\tdecompression_duration_source=%s\tB/op=%d\tallocs/op=%d\tallocation_source=%s\tencode=%s\tdecode=%s\trange_scan=%s\trange_matches=%d",
		s.Column,
		s.Rows,
		s.Granules,
		s.CompressionRow.CodecLayoutLabel,
		s.CompressionRow.CompressionPolicyLabel,
		s.Encoding,
		s.RequestedCompression,
		formatCompressionMix(s.ActualCompressionMix),
		s.CompressionRow.CompressedBytes,
		s.CompressionRow.DecompressedBytes,
		s.CompressionRow.RawBytes,
		s.ValueBytes,
		s.EncodedRawBytes,
		s.StoredBytes,
		s.CompressionRow.CompressionRatio,
		ratioValues,
		ratioEncoded,
		s.CompressionRow.CompressionDuration,
		s.CompressionRow.CompressionDurationSource,
		s.CompressionRow.DecompressionDuration,
		s.CompressionRow.DecompressionDurationSource,
		s.CompressionRow.BenchmarkBPerOp,
		s.CompressionRow.BenchmarkAllocsPerOp,
		s.CompressionRow.BenchmarkAllocationSource,
		s.EncodeDuration,
		s.DecodeDuration,
		s.RangeScanDuration,
		s.RangeScanMatches)
}

func formatCompressionMix(m map[Compression]int) string {
	var parts []string
	for _, c := range []Compression{CompressionNone, CompressionSnappy, CompressionLZ4} {
		if n := m[c]; n > 0 {
			parts = append(parts, fmt.Sprintf("%s:%d", c, n))
		}
	}
	return strings.Join(parts, ",")
}

func middleRange(values []int64) (int64, int64) {
	if len(values) == 0 {
		return 1, 0
	}
	min, max := minMax(values)
	if min == max {
		return min, max
	}
	if min < 0 && max > math.MaxInt64+min {
		return min, max
	}
	span := max - min
	low := min + span/2
	high := low + span/64
	if high < low {
		high = low
	}
	return low, high
}
