package collections

import (
	"fmt"
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
		meta.Options.ColumnStore = cfg
		documents := make([]columnWriteDocument, len(rows))
		for i, row := range rows {
			documents[i] = columnWriteDocument{ID: row.ID, declaredValues: row.Values, declaredValuesReady: true}
		}
		combined, err := typedGraphColumnEncodedBound(columnWritePublishInput{meta: meta, operation: ColumnPublishOperationInsert, documents: documents})
		if err != nil {
			t.Fatal(err)
		}
		rowCfg := columnStoreRowAssetConfig(*cfg)
		rowRows, err := projectColumnDeclaredRowsForColumns(cfg.Columns, rowCfg.Columns, rows)
		if err != nil {
			t.Fatal(err)
		}
		rowImage, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{Collection: meta.Name, Namespace: cfg.AssetManager.Namespace, Generation: math.MaxUint64, PartID: math.MaxUint64, AppliedCommandLSN: math.MaxUint64, Operation: ColumnPublishOperationInsert, SchemaHash: cfg.SchemaHash, Columns: rowCfg.Columns, Rows: rowRows})
		if err != nil {
			t.Fatal(err)
		}
		locatorTable, err := buildColumnPrimaryRowLocatorTable(ColumnPublishPlan{Rows: count, Operation: ColumnPublishOperationInsert, UpdatedActiveManifest: ColumnManifestIdentity{Generation: math.MaxUint64}, AppliedCommandLSN: math.MaxUint64}, documents)
		if err != nil {
			t.Fatal(err)
		}
		locatorBytes, err := typedGraphTableEncodedBound(locatorTable)
		if err != nil || combined < int64(len(actual.Bytes)+len(rowImage))+locatorBytes {
			t.Fatalf("combined actual producer=%d locator=%d bound=%d err=%v", len(actual.Bytes)+len(rowImage), locatorBytes, combined, err)
		}
		bound, err := typedcolumn.FP32ImageEncodedUpperBound(part.Part.Options, count, imageOpts)
		if err != nil {
			t.Fatalf("actual selected options %+v image %+v: %v", part.Part.Options, imageOpts, err)
		}
		if int64(len(actual.Bytes)) > bound {
			image, parseErr := typedcolumn.ParseColumnPartImage(actual.Bytes)
			t.Logf("actual image manifest=%d sections=%+v parse=%v", image.ManifestBytes, image.Sections, parseErr)
			t.Fatalf("selected rows=%d actual=%d bound=%d", count, len(actual.Bytes), bound)
		}
		// The graph source census separately sums every section's larger raw
		// or stored length. Exercise that potential use of the image bound
		// against the real selected producer, including its metadata sections.
		// This example alone is not a proof for other producer options/codecs.
		image, err := typedcolumn.ParseColumnPartImage(actual.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		var sectionBytes int64
		for _, section := range image.Sections {
			sectionBytes += int64(max(section.RawBytes, section.Length))
		}
		if sectionBytes > bound {
			t.Fatalf("selected rows=%d raw-or-stored sections=%d bound=%d", count, sectionBytes, bound)
		}
	}
}

// A row-only handoff may combine several members into one physical part. This
// checks representative granule boundaries for the narrow durable ordinary
// layout; it is not an admission proof for other options or producer shapes.
func TestOrdinaryFP32ImageBoundCoalescedSamples(t *testing.T) {
	clearNativeOrdinaryPendingGraphCodecEnv(t)
	meta, err := normalizeCollectionMeta(nativeOrdinaryPendingGraphMeta())
	if err != nil {
		t.Fatal(err)
	}
	cfg := meta.Options.ColumnStore
	if cfg == nil || cfg.ProfileSupport != ColumnStoreProfileDurableOnly {
		t.Fatal("ordinary fixture must use the durable-only column profile")
	}
	counts := []int{1, 17, typedcolumn.DefaultRowsPerGranule, typedcolumn.DefaultRowsPerGranule + 1}
	for _, a := range counts {
		for _, b := range counts {
			left, err := typedColumnPublicationFP32EncodedBound(*cfg, a)
			if err != nil {
				t.Fatal(err)
			}
			right, err := typedColumnPublicationFP32EncodedBound(*cfg, b)
			if err != nil {
				t.Fatal(err)
			}
			joined, err := typedColumnPublicationFP32EncodedBound(*cfg, a+b)
			if err != nil {
				t.Fatal(err)
			}
			if joined > left+right {
				t.Fatalf("coalesced rows=%d+%d bound=%d exceeds member sum=%d", a, b, joined, left+right)
			}
		}
	}
}

func TestOrdinaryFP32ImageBoundProductionSnappy(t *testing.T) {
	clearNativeOrdinaryPendingGraphCodecEnv(t)
	meta := nativeOrdinaryPendingGraphMeta()
	meta.Options.ColumnStore.TypedColumnCompression = ColumnStoreTypedColumnCompressionSnappy
	meta, err := normalizeCollectionMeta(meta)
	if err != nil {
		t.Fatal(err)
	}
	cfg := *meta.Options.ColumnStore
	if cfg.ProfileSupport != ColumnStoreProfileDurableOnly || cfg.TypedColumnSectionCompression != ColumnStoreTypedColumnCompressionSnappy {
		t.Fatalf("unexpected production profile: %+v", cfg)
	}
	for _, count := range []int{1, typedcolumn.DefaultRowsPerGranule, typedcolumn.DefaultRowsPerGranule + 1} {
		rows := make([]columnDeclaredRow, count)
		for i := range rows {
			rows[i] = columnDeclaredRow{ID: []byte(fmt.Sprintf("id-%d", i)), Values: []columnDeclaredValue{{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: []float32{float32(i), 1}}}}
		}
		actual, err := buildTypedColumnPartImageForDeclaredRowsWithResult(cfg, math.MaxUint64, math.MaxUint64, rows)
		if err != nil {
			t.Fatal(err)
		}
		bound, err := typedColumnPublicationFP32EncodedBound(cfg, count)
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(actual.Bytes)) > bound {
			t.Fatalf("rows=%d encoded=%d bound=%d", count, len(actual.Bytes), bound)
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
