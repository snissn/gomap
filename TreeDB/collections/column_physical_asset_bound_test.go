package collections

import (
	"math"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func BenchmarkColumnPhysicalAssetEncodedUpperBound(b *testing.B) {
	in := columnPhysicalAssetEncodeInput{Collection: "minima", Namespace: "minima", Operation: ColumnPublishOperationInsert, Columns: []ColumnStoreColumn{{Name: "content", Path: "content", ValueType: ColumnStoreValueString}}, Rows: make([]columnDeclaredRow, 64)}
	for i := range in.Rows {
		in.Rows[i] = columnDeclaredRow{ID: []byte("id"), Values: []columnDeclaredValue{{Type: ColumnStoreValueString, Present: true, String: "content"}}}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := columnPhysicalAssetEncodedUpperBound(in); err != nil {
			b.Fatal(err)
		}
	}
}

func TestColumnPhysicalAssetEncodedUpperBoundSelectedProducer(t *testing.T) {
	meta := typedMinimaCollectionMeta()
	cfg, err := normalizeColumnStoreConfig(meta.Name, meta.Options.ColumnStore)
	if err != nil {
		t.Fatal(err)
	}
	for _, count := range []int{1, 17, typedcolumn.DefaultRowsPerGranule + 1} {
		rows := make([]columnDeclaredRow, count)
		for i := range rows {
			rows[i] = columnDeclaredRow{ID: []byte(strings.Repeat("id", 1+i%101)), Values: []columnDeclaredValue{
				{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: []float32{1, 2, 3, 4, 5, 6, 7, 8}},
				{Type: ColumnStoreValueString, Present: true, String: "content"}, {Type: ColumnStoreValueString, Present: true, String: "user"}, {Type: ColumnStoreValueString, Present: true, String: "path"},
			}}
		}
		fields := columnStoreTypedColumnPartFields(*cfg)
		sortKey, err := typedColumnPartPublicationSortKey(*cfg, fields)
		if err != nil {
			t.Fatal(err)
		}
		opts, err := typedColumnPublicationAdapterOptionsFromConfig(*cfg, math.MaxUint64, fields, sortKey)
		if err != nil {
			t.Fatal(err)
		}
		part, err := buildTypedColumnAdapterPartFromDeclaredRows(opts, cfg.Columns, rows)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := buildTypedColumnPartImageForDeclaredRowsWithResult(*cfg, math.MaxUint64, math.MaxUint64, rows)
		if err != nil {
			t.Fatal(err)
		}
		imageOpts := part.imageOptions()
		bound, err := typedcolumn.FP32ImageEncodedUpperBound(part.Part.Options, count, imageOpts)
		if err != nil {
			t.Fatalf("actual selected options %+v image %+v: %v", part.Part.Options, imageOpts, err)
		}
		if int64(len(actual.Bytes)) > bound {
			image, parseErr := typedcolumn.ParseColumnPartImage(actual.Bytes)
			t.Logf("actual image manifest=%d sections=%+v parse=%v", image.ManifestBytes, image.Sections, parseErr)
			t.Fatalf("selected rows=%d actual=%d bound=%d", count, len(actual.Bytes), bound)
		}
	}
}

func TestColumnPhysicalAssetEncodedUpperBound(t *testing.T) {
	for _, count := range []int{0, 1, 17, 257} {
		in := columnPhysicalAssetEncodeInput{Collection: strings.Repeat("c", 200), Namespace: strings.Repeat("n", 200), Generation: 1, PartID: 1, Operation: ColumnPublishOperationInsert, Columns: []ColumnStoreColumn{{Name: "content", Path: "content", ValueType: ColumnStoreValueString}}}
		for i := 0; i < count; i++ {
			in.Rows = append(in.Rows, columnDeclaredRow{ID: []byte(strings.Repeat("id", i+1)), Values: []columnDeclaredValue{{Type: ColumnStoreValueString, Present: true, String: strings.Repeat("x", i)}}})
		}
		encoded, _, err := encodeColumnPhysicalAsset(in)
		if err != nil {
			t.Fatal(err)
		}
		bound, err := columnPhysicalAssetEncodedUpperBound(in)
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(encoded)) > bound {
			t.Fatalf("rows=%d encoded=%d bound=%d", count, len(encoded), bound)
		}
		if count > 0 {
			var sum int64
			for i := range in.Rows {
				single := in
				single.Rows = in.Rows[i : i+1]
				one, err := columnPhysicalAssetEncodedUpperBound(single)
				if err != nil {
					t.Fatal(err)
				}
				sum += one
			}
			if bound > sum {
				t.Fatalf("bound=%d singleton sum=%d", bound, sum)
			}
		}
		if a := testing.AllocsPerRun(10, func() {
			if _, err := columnPhysicalAssetEncodedUpperBound(in); err != nil {
				panic(err)
			}
		}); a != 0 {
			t.Fatalf("allocs=%g", a)
		}
		in.Operation = ColumnPublishOperationDelete
		for i := range in.Rows {
			in.Rows[i].Deleted = true
			in.Rows[i].Values = nil
		}
		encoded, _, err = encodeColumnPhysicalAsset(in)
		if err != nil {
			t.Fatal(err)
		}
		bound, err = columnPhysicalAssetEncodedUpperBound(in)
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(encoded)) > bound {
			t.Fatal("delete bound")
		}
	}
}
