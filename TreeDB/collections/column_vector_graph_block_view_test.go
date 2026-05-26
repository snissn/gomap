package collections

import (
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"strings"
	"testing"
	"unsafe"
)

func TestColumnVectorGraphSearchPlanOrdinalRefV1(t *testing.T) {
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetV2B(t, []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{1}},
		{ID: []byte("doc-b"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{0}},
	})
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	plan, err := newColumnVectorGraphSearchPlan(reader)
	if err != nil {
		t.Fatalf("newColumnVectorGraphSearchPlan: %v", err)
	}
	ref, err := plan.ordinalRef(1)
	if err != nil {
		t.Fatalf("ordinalRef: %v", err)
	}
	if ref.assetOrdinal != 0 || ref.rowIndex != 1 {
		t.Fatalf("ordinalRef={asset:%d row:%d} want {asset:0 row:1}", ref.assetOrdinal, ref.rowIndex)
	}
	_, err = plan.ordinalRef(2)
	if !errors.Is(err, errColumnPhysicalRowOrdinalOutOfBounds) {
		t.Fatalf("ordinalRef out-of-bounds err=%v want row bounds", err)
	}
}

func TestColumnVectorGraphBlockViewAccessorsV1(t *testing.T) {
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetV2B(t, []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{1}},
		{ID: []byte("doc-b"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{0}},
	})
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	plan, err := newColumnVectorGraphSearchPlan(reader)
	if err != nil {
		t.Fatalf("newColumnVectorGraphSearchPlan: %v", err)
	}
	view, ref, err := plan.blockViewForOrdinal(1)
	if err != nil {
		t.Fatalf("blockViewForOrdinal: %v", err)
	}
	if ref.rowIndex != 1 {
		t.Fatalf("rowIndex=%d want 1", ref.rowIndex)
	}
	id, err := view.id(ref.rowIndex)
	if err != nil {
		t.Fatalf("id: %v", err)
	}
	if string(id) != "doc-b" {
		t.Fatalf("id=%q want doc-b", string(id))
	}
	vector, vectorScratch, err := view.vector(ref.rowIndex, nil)
	if err != nil {
		t.Fatalf("vector: %v", err)
	}
	if !slices.Equal(vector, []float32{0, 1, 0}) {
		t.Fatalf("vector=%v want [0 1 0]", vector)
	}
	if len(vectorScratch) > 0 && !slices.Equal(vector, vectorScratch) {
		t.Fatalf("vectorScratch=%v want it to mirror vector=%v when scratch is used", vectorScratch, vector)
	}
	invNorm, err := view.invNorm(ref.rowIndex)
	if err != nil {
		t.Fatalf("invNorm: %v", err)
	}
	if math.Abs(float64(invNorm-1)) > 1e-6 {
		t.Fatalf("invNorm=%v want 1", invNorm)
	}
	adjacency, adjacencyScratch, direct, err := view.adjacency(ref.rowIndex, nil)
	if err != nil {
		t.Fatalf("adjacency: %v", err)
	}
	if !slices.Equal(adjacency, []uint32{0}) {
		t.Fatalf("adjacency=%v want [0]", adjacency)
	}
	if columnPhysicalNativeLittleEndian {
		if direct && len(adjacencyScratch) != 0 {
			t.Fatalf("direct adjacency view used scratch=%v", adjacencyScratch)
		}
		if !direct && !slices.Equal(adjacencyScratch, []uint32{0}) {
			t.Fatalf("scratch adjacency=%v want [0] when direct view is unaligned", adjacencyScratch)
		}
	} else if direct || !slices.Equal(adjacencyScratch, []uint32{0}) {
		t.Fatalf("direct=%t scratch=%v want scratch adjacency decode", direct, adjacencyScratch)
	}
	stats := reader.Stats()
	if stats.RowFetches != 0 || stats.RowsFetched != 0 {
		t.Fatalf("stats after block-view access=%+v want no generic row fetch/materialization", stats)
	}
}

func TestColumnVectorGraphBlockViewRejectsClosedReaderV1(t *testing.T) {
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetV2B(t, []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{0}},
	})
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	plan, err := newColumnVectorGraphSearchPlan(reader)
	if err != nil {
		t.Fatalf("newColumnVectorGraphSearchPlan: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, err := plan.blockViewForOrdinal(0); err == nil || !strings.Contains(err.Error(), "physical column row reader is closed") {
		t.Fatalf("blockViewForOrdinal after close err=%v want closed reader", err)
	}
}

func TestColumnVectorGraphBlockViewRejectsClosedReaderWithCachedViewV1(t *testing.T) {
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetV2B(t, []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{0}},
	})
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	plan, err := newColumnVectorGraphSearchPlan(reader)
	if err != nil {
		t.Fatalf("newColumnVectorGraphSearchPlan: %v", err)
	}
	if _, _, err := plan.blockViewForOrdinal(0); err != nil {
		t.Fatalf("prime cached blockViewForOrdinal: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, err := plan.blockViewForOrdinal(0); err == nil || !strings.Contains(err.Error(), "physical column row reader is closed") {
		t.Fatalf("cached blockViewForOrdinal after close err=%v want closed reader", err)
	}
}

func TestColumnVectorGraphBlockViewRejectsMalformedRowsV1(t *testing.T) {
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetV2B(t, []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 0, Adjacency: []uint32{0}},
	})
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	plan, err := newColumnVectorGraphSearchPlan(reader)
	if err != nil {
		t.Fatalf("newColumnVectorGraphSearchPlan: %v", err)
	}
	_, _, err = plan.blockViewForOrdinal(0)
	if err == nil || !strings.Contains(err.Error(), "invalid inv_norm") {
		t.Fatalf("blockViewForOrdinal err=%v want invalid inv_norm", err)
	}
}

func TestColumnVectorGraphAdjacencyDirectViewProbeAlignedLittleEndianV1(t *testing.T) {
	if !columnPhysicalNativeLittleEndian {
		t.Skip("direct uint32 byte views require little-endian host order")
	}
	raw := columnVectorGraphAlignedBytesForTest(t, 8)
	columnVectorGraphPutLittleEndianUint32sForTest(raw, []uint32{7, 11})
	adjacency, ok := columnVectorGraphLittleEndianUint32DirectView(raw, 2)
	if !ok {
		t.Fatal("direct view probe returned ok=false for aligned little-endian adjacency")
	}
	if !slices.Equal(adjacency, []uint32{7, 11}) {
		t.Fatalf("adjacency=%v want [7 11]", adjacency)
	}
}

func TestColumnVectorGraphBlockViewAdjacencyAlignedDirectNoScratchV1(t *testing.T) {
	if !columnPhysicalNativeLittleEndian {
		t.Skip("direct uint32 byte views require little-endian host order")
	}
	raw := columnVectorGraphAlignedBytesForTest(t, 8)
	columnVectorGraphPutLittleEndianUint32sForTest(raw, []uint32{3, 5})
	view := columnVectorGraphAdjacencyBlockViewForTest(nil, raw, columnVectorGraphAdjacencySpan{start: 0, end: len(raw), count: 2})
	adjacency, scratch, direct, err := view.adjacency(0, nil)
	if err != nil {
		t.Fatalf("adjacency: %v", err)
	}
	if !direct {
		t.Fatal("adjacency direct=false want direct view")
	}
	if len(scratch) != 0 {
		t.Fatalf("scratch len=%d want 0 for direct view", len(scratch))
	}
	if !slices.Equal(adjacency, []uint32{3, 5}) {
		t.Fatalf("adjacency=%v want [3 5]", adjacency)
	}
}

func TestColumnVectorGraphAdjacencyDirectViewProbeUnalignedFallsBackV1(t *testing.T) {
	raw, start := columnVectorGraphUnalignedAdjacencyRawForTest(t, 8)
	columnVectorGraphPutLittleEndianUint32sForTest(raw[start:start+8], []uint32{13, 17})
	if adjacency, ok := columnVectorGraphLittleEndianUint32DirectView(raw[start:start+8], 2); ok || adjacency != nil {
		t.Fatalf("direct view probe=(%v,%t) want ineligible unaligned span", adjacency, ok)
	}
	reader, cleanup := columnVectorGraphAdjacencyLittleEndianReaderForTest(t)
	defer cleanup()
	view := columnVectorGraphAdjacencyBlockViewForTest(reader, raw, columnVectorGraphAdjacencySpan{start: start, end: start + 8, count: 2})
	adjacency, scratch, direct, err := view.adjacency(0, nil)
	if err != nil {
		t.Fatalf("adjacency: %v", err)
	}
	if direct {
		t.Fatal("adjacency direct=true want scratch fallback for unaligned span")
	}
	if !slices.Equal(adjacency, []uint32{13, 17}) || !slices.Equal(scratch, []uint32{13, 17}) {
		t.Fatalf("adjacency=%v scratch=%v want scratch decoded [13 17]", adjacency, scratch)
	}
}

func TestColumnVectorGraphAdjacencyDirectViewProbeZeroLengthDirectV1(t *testing.T) {
	adjacency, ok := columnVectorGraphLittleEndianUint32DirectView(nil, 0)
	if !ok || adjacency != nil {
		t.Fatalf("direct view probe=(%v,%t) want nil,true for zero-length adjacency", adjacency, ok)
	}
	view := columnVectorGraphAdjacencyBlockViewForTest(nil, nil, columnVectorGraphAdjacencySpan{})
	adjacency, scratch, direct, err := view.adjacency(0, nil)
	if err != nil {
		t.Fatalf("adjacency: %v", err)
	}
	if !direct || adjacency != nil || scratch != nil {
		t.Fatalf("adjacency=%v scratch=%v direct=%t want nil nil true", adjacency, scratch, direct)
	}
}

func TestColumnVectorGraphAdjacencyDirectViewProbeMalformedLengthV1(t *testing.T) {
	raw := columnVectorGraphAlignedBytesForTest(t, 7)
	if adjacency, ok := columnVectorGraphLittleEndianUint32DirectView(raw, 2); ok || adjacency != nil {
		t.Fatalf("direct view probe=(%v,%t) want ineligible malformed byte length", adjacency, ok)
	}
	raw = columnVectorGraphAlignedBytesForTest(t, 8)
	if adjacency, ok := columnVectorGraphLittleEndianUint32DirectView(raw, 3); ok || adjacency != nil {
		t.Fatalf("direct view probe=(%v,%t) want ineligible mismatched count", adjacency, ok)
	}
}

func columnVectorGraphAdjacencyBlockViewForTest(reader *columnVectorGraphPhysicalRowReader, raw []byte, span columnVectorGraphAdjacencySpan) *columnVectorGraphBlockView {
	return &columnVectorGraphBlockView{
		reader:              reader,
		block:               &columnPhysicalRowReaderBlock{raw: raw},
		adjSpans:            []columnVectorGraphAdjacencySpan{span},
		rowValidated:        []bool{true},
		adjacencyDirectView: true,
	}
}

func columnVectorGraphAdjacencyLittleEndianReaderForTest(tb testing.TB) (*columnVectorGraphPhysicalRowReader, func()) {
	tb.Helper()
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetV2B(tb, []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{0}},
	})
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		_ = d.Close()
		tb.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	return reader, func() {
		_ = reader.Close()
		_ = d.Close()
	}
}

func columnVectorGraphAlignedBytesForTest(tb testing.TB, n int) []byte {
	tb.Helper()
	raw := make([]byte, n+3)
	for off := 0; off < 4; off++ {
		candidate := raw[off : off+n]
		if n == 0 || uintptr(unsafe.Pointer(&candidate[0]))%4 == 0 {
			return candidate
		}
	}
	tb.Fatal("could not find aligned byte window")
	return nil
}

func columnVectorGraphUnalignedAdjacencyRawForTest(tb testing.TB, n int) ([]byte, int) {
	tb.Helper()
	raw := make([]byte, n+4)
	for off := 0; off < 4; off++ {
		candidate := raw[off : off+1+n]
		if uintptr(unsafe.Pointer(&candidate[1]))%4 != 0 {
			return candidate, 1
		}
	}
	tb.Fatal("could not find unaligned byte window")
	return nil, 0
}

func columnVectorGraphPutLittleEndianUint32sForTest(raw []byte, values []uint32) {
	for i, value := range values {
		binary.LittleEndian.PutUint32(raw[i*4:], value)
	}
}
