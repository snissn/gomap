package colgranule

import (
	"fmt"
	"math"
	"strings"
	"time"
)

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
				summary.StoredBytes += len(g.Payload)
				summary.ActualCompressionMix[g.Compression]++
			}
			summary.EncodeDuration = time.Since(start)
			summary.Granules = len(encoded)

			start = time.Now()
			for _, g := range encoded {
				if _, err := DecodeInt64(nil, g); err != nil {
					return nil, fmt.Errorf("decode column=%s config=%s/%s: %w", name, cfg.Encoding, cfg.Compression, err)
				}
			}
			summary.DecodeDuration = time.Since(start)

			start = time.Now()
			var scratch []int64
			for _, g := range encoded {
				count, out, err := RangeScanCount(g, low, high, scratch)
				if err != nil {
					return nil, fmt.Errorf("range scan column=%s config=%s/%s: %w", name, cfg.Encoding, cfg.Compression, err)
				}
				summary.RangeScanMatches += count
				scratch = out
			}
			summary.RangeScanDuration = time.Since(start)
			out = append(out, summary)
		}
	}
	return out, nil
}

func DefaultJSONBenchConfigs() []Config {
	return []Config{
		{Encoding: EncodingRawInt64, Compression: CompressionNone},
		{Encoding: EncodingRawInt64, Compression: CompressionSnappy},
		{Encoding: EncodingRawInt64, Compression: CompressionLZ4},
		{Encoding: EncodingDeltaVarint, Compression: CompressionNone},
		{Encoding: EncodingDeltaVarint, Compression: CompressionSnappy},
		{Encoding: EncodingDeltaVarint, Compression: CompressionLZ4},
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
	return fmt.Sprintf("%s\trows=%d\tgranules=%d\tencoding=%s\trequested=%s\tactual=%s\tvalue_bytes=%d\tencoded_raw_bytes=%d\tstored_bytes=%d\tratio_vs_values=%.6f\tratio_vs_encoded=%.6f\tencode=%s\tdecode=%s\trange_scan=%s\trange_matches=%d",
		s.Column,
		s.Rows,
		s.Granules,
		s.Encoding,
		s.RequestedCompression,
		formatCompressionMix(s.ActualCompressionMix),
		s.ValueBytes,
		s.EncodedRawBytes,
		s.StoredBytes,
		ratioValues,
		ratioEncoded,
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
