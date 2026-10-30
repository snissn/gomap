package typedcolumn

import (
	"fmt"
	"testing"
)

// The ordinal remains an already-supported policy so this isolates admission
// of each Snappy section selector from admission of Snappy ordinal blocks.
func TestFP32ImageEncodedUpperBoundSnappySections(t *testing.T) {
	for _, field := range []string{"section", "locator", "dictionary", "pruning"} {
		t.Run(field, func(t *testing.T) {
			opts := fp32BoundOptions()
			var imageOpts ColumnPartImageOptions
			switch field {
			case "section":
				imageOpts.SectionCompression = CompressionSnappy
			case "locator":
				imageOpts.RowLocatorSectionCompression = CompressionSnappy
			case "dictionary":
				imageOpts.DictionarySectionCompression = CompressionSnappy
			case "pruning":
				imageOpts.PruningMetadataSectionCompression = CompressionSnappy
			}
			bound, err := FP32ImageEncodedUpperBound(opts, 257, imageOpts)
			if err != nil || bound <= 0 {
				t.Fatalf("Snappy %s section rejected: bound=%d err=%v", field, bound, err)
			}
		})
	}
}

// The same benchmark source runs against baseline and candidate. Rejections
// and admissions are separate dispositions, not equivalent units of work.
func BenchmarkFP32ImageEncodedUpperBoundPolicies(b *testing.B) {
	for _, policy := range []struct {
		name  string
		codec Compression
	}{{"none", CompressionNone}, {"lz4", CompressionLZ4}, {"snappy", CompressionSnappy}} {
		for _, rows := range []int{1, 256, 257, 4097} {
			b.Run(fmt.Sprintf("%s/rows=%d", policy.name, rows), func(b *testing.B) {
				opts := fp32BoundOptions()
				opts.PartPolicy.RowsPerGranule = 256
				opts.Columns[0].Compression = policy.codec
				imageOpts := ColumnPartImageOptions{SectionCompression: policy.codec}
				var admitted, rejected int
				var last int64
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					bound, err := FP32ImageEncodedUpperBound(opts, rows, imageOpts)
					if err != nil {
						rejected++
					} else {
						admitted++
						last = bound
					}
				}
				b.ReportMetric(float64(rows), "rows/op")
				b.ReportMetric(float64(admitted)/float64(b.N), "admit/op")
				b.ReportMetric(float64(rejected)/float64(b.N), "reject/op")
				b.ReportMetric(float64(last), "bound-bytes/op")
			})
		}
	}
}
