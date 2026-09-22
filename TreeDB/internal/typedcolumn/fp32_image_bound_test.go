package typedcolumn

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

func fp32BoundOptions() Options {
	return Options{SchemaMode: ColumnSchemaFixed, Columns: []ColumnDefinition{
		{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingDeltaVarint, CompressionSet: true, StatsDisabled: true},
		{Name: "embedding", Type: ColumnTypeFloat32Vector, Encoding: EncodingRawFloat32Vector, CompressionSet: true, FixedWidthElements: 8},
	}, LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}}, SortKey: SortKey{Columns: []SortKeyColumn{{Column: "id"}}}, PartPolicy: ColumnPartPolicy{RowsPerGranule: 16}}
}

func TestFP32ImageEncodedUpperBound(t *testing.T) {
	for _, alignment := range []int{8, 64} {
		for _, n := range []int{0, 1, 15, 16, 17, 257} {
			t.Run(fmt.Sprintf("%d/%d", alignment, n), func(t *testing.T) {
				opts := fp32BoundOptions()
				opts.Columns[0].Compression = CompressionLZ4
				opts.Columns[1].Name = strings.Repeat("vector", 100)
				imageOpts := ColumnPartImageOptions{SectionAlignment: alignment, SectionCompression: CompressionZSTD, LayoutLogicalTypes: map[string]string{opts.Columns[1].Name: "float32_vector"}}
				ids := make([]int64, n)
				values := make([]float32, n*8)
				for i := range ids {
					ids[i] = int64(i) * 100003
					values[i*8] = float32(i) + 0.25
				}
				part, err := BuildColumnPart(math.MaxUint64, opts, Batch{Rows: n, Columns: map[string][]int64{"id": ids}, Float32Vectors: map[string][]float32{opts.Columns[1].Name: values}})
				if n == 0 {
					if err == nil {
						t.Fatal("empty part unexpectedly encoded")
					}
					if _, boundErr := FP32ImageEncodedUpperBound(opts, n, imageOpts); boundErr == nil {
						t.Fatal("empty TCIM admitted")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				image, err := BuildColumnPartImage(part, imageOpts)
				if err != nil {
					t.Fatal(err)
				}
				bound, err := FP32ImageEncodedUpperBound(opts, n, imageOpts)
				if err != nil {
					t.Fatal(err)
				}
				if int64(len(image.Bytes)) > bound {
					t.Fatalf("encoded=%d bound=%d", len(image.Bytes), bound)
				}
				if n > 0 {
					one, err := FP32ImageEncodedUpperBound(opts, 1, imageOpts)
					if err != nil {
						t.Fatal(err)
					}
					if bound > int64(n)*one {
						t.Fatalf("batch bound %d exceeds singleton sum %d", bound, int64(n)*one)
					}
				}
			})
		}
	}
}

func TestFP32ImageEncodedUpperBoundAdmission(t *testing.T) {
	for _, mutate := range []func(*Options){
		func(o *Options) { o.Columns[0].StatsDisabled = false },
		func(o *Options) { o.Columns[1].Compression = CompressionSnappy },
		func(o *Options) { o.PartPolicy.AdaptiveMarkSizing.Enabled = true },
		func(o *Options) { o.Columns[0].Encoding = EncodingRawInt64 },
	} {
		o := fp32BoundOptions()
		mutate(&o)
		if _, err := FP32ImageEncodedUpperBound(o, 1, ColumnPartImageOptions{}); err == nil {
			t.Fatal("unsupported layout accepted")
		}
	}
	opts := fp32BoundOptions()
	if _, err := FP32ImageEncodedUpperBound(opts, math.MaxInt, ColumnPartImageOptions{}); err == nil {
		t.Fatal("overflow accepted")
	}
	opts.Columns[0].StatsDisabled = false
	if _, err := FP32ImageEncodedUpperBound(opts, 1, ColumnPartImageOptions{}); err == nil {
		t.Fatal("unsupported stats accepted")
	}
	opts = fp32BoundOptions()
	opts.Columns[1].FixedWidthElements = math.MaxInt32
	if _, err := FP32ImageEncodedUpperBound(opts, math.MaxInt32, ColumnPartImageOptions{}); err == nil {
		t.Fatal("payload arithmetic overflow accepted")
	}
	opts = fp32BoundOptions()
	if a := testing.AllocsPerRun(100, func() {
		if _, err := FP32ImageEncodedUpperBound(opts, 257, ColumnPartImageOptions{}); err != nil {
			panic(err)
		}
	}); a != 0 {
		t.Fatalf("allocs=%g", a)
	}
}

func BenchmarkFP32ImageEncodedUpperBound(b *testing.B) {
	opts := fp32BoundOptions()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := FP32ImageEncodedUpperBound(opts, 4097, ColumnPartImageOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
