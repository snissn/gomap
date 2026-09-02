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

func TestColumnVectorGraphSearchPlanOrdinalRefRejectsMalformedRanges1968(t *testing.T) {
	cases := []struct {
		name   string
		reader *columnPhysicalRowReader
	}{
		{
			name:   "single_range_not_identity",
			reader: &columnPhysicalRowReader{totalRows: 2, ranges: []columnPhysicalRowReaderRange{{assetOrdinal: 0, startOrdinal: 1, rowCount: 1}}},
		},
		{
			name:   "overlap",
			reader: &columnPhysicalRowReader{totalRows: 3, ranges: []columnPhysicalRowReaderRange{{assetOrdinal: 0, startOrdinal: 0, rowCount: 2}, {assetOrdinal: 1, startOrdinal: 1, rowCount: 2}}},
		},
		{
			name:   "gap",
			reader: &columnPhysicalRowReader{totalRows: 3, ranges: []columnPhysicalRowReaderRange{{assetOrdinal: 0, startOrdinal: 0, rowCount: 1}, {assetOrdinal: 1, startOrdinal: 2, rowCount: 1}}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := &columnVectorGraphSearchPlan{reader: &columnVectorGraphPhysicalRowReader{}, physicalReader: tc.reader}
			err := plan.prepareOrdinalRefs()
			if !errors.Is(err, errColumnPhysicalRowOrdinalOutOfBounds) {
				t.Fatalf("prepareOrdinalRefs err=%v want ordinal bounds sentinel", err)
			}
			if plan.ordinalRefsReady {
				t.Fatalf("ordinalRefsReady=true after malformed range")
			}
		})
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
	if direct || !slices.Equal(adjacencyScratch, []uint32{0}) {
		t.Fatalf("direct=%t scratch=%v want fallback-only scratch adjacency decode", direct, adjacencyScratch)
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
	if reader.invNormSource != nil {
		_ = reader.invNormSource.Close()
		reader.invNormSource = nil
		reader.invNormStateUnavailable = true
	}
	plan, err := newColumnVectorGraphSearchPlan(reader)
	if err != nil {
		t.Fatalf("newColumnVectorGraphSearchPlan: %v", err)
	}
	_, _, err = plan.blockViewForOrdinal(0)
	if err == nil || !strings.Contains(err.Error(), "invalid inv_norm") {
		t.Fatalf("blockViewForOrdinal err=%v want invalid inv_norm", err)
	}
}

func TestColumnVectorGraphBlockViewInvNormStateSourceAndLegacyFallbackV1(t *testing.T) {
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{3, 4, 0}, InvNorm: 0.2, Adjacency: []uint32{1}},
		{ID: []byte("doc-b"), Vector: []float32{0, 2, 0}, InvNorm: 0.5, Adjacency: []uint32{0}},
	}
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetV2B(t, rows)
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if !reader.usesInvNormStateSource() {
		t.Fatal("reader did not bind inverse-norm state source")
	}
	plan, err := newColumnVectorGraphSearchPlan(reader)
	if err != nil {
		t.Fatalf("newColumnVectorGraphSearchPlan: %v", err)
	}
	targetOrdinal := len(rows) - 1
	view, ref, err := plan.blockViewForOrdinal(targetOrdinal)
	if err != nil {
		t.Fatalf("blockViewForOrdinal: %v", err)
	}
	want, err := columnVectorGraphInvNorm(rows[targetOrdinal].Vector)
	if err != nil {
		t.Fatalf("columnVectorGraphInvNorm: %v", err)
	}
	if _, _, _, ok := reader.invNormForOrdinal(targetOrdinal); !ok {
		t.Fatalf("reader.invNormForOrdinal(%d)=!ok want typed-state source hit before fallback", targetOrdinal)
	}
	if got, err := view.invNorm(ref.rowIndex); err != nil || math.Abs(float64(got-want)) > 1e-6 {
		t.Fatalf("invNorm state got=%v err=%v want=%v", got, err, want)
	}
	if reader.invNormSource == nil {
		t.Fatal("reader.invNormSource=nil want bound source")
	}
	if err := reader.invNormSource.Close(); err != nil {
		t.Fatalf("reader.invNormSource.Close: %v", err)
	}
	if _, _, _, ok := reader.invNormForOrdinal(targetOrdinal); ok {
		t.Fatalf("reader.invNormForOrdinal(%d)=ok want typed-state miss after source close", targetOrdinal)
	}
	if got, err := view.invNorm(ref.rowIndex); err != nil || math.Abs(float64(got-want)) > 1e-6 {
		t.Fatalf("invNorm legacy fallback got=%v err=%v want=%v", got, err, want)
	}
}

func TestColumnVectorGraphAdjacencyLittleEndianPayloadFixtureV1(t *testing.T) {
	raw := columnVectorGraphAlignedBytesForTest(t, 8)
	columnVectorGraphPutLittleEndianUint32sForTest(raw, []uint32{7, 11})
	if got := []uint32{binary.LittleEndian.Uint32(raw[0:4]), binary.LittleEndian.Uint32(raw[4:8])}; !slices.Equal(got, []uint32{7, 11}) {
		t.Fatalf("little-endian adjacency payload=%v want [7 11]", got)
	}
}

func TestColumnVectorGraphBlockViewAdjacencyAlignedUsesScratchFallbackV1(t *testing.T) {
	raw := columnVectorGraphAlignedBytesForTest(t, 8)
	columnVectorGraphPutLittleEndianUint32sForTest(raw, []uint32{3, 5})
	reader, cleanup := columnVectorGraphAdjacencyLittleEndianReaderForTest(t)
	defer cleanup()
	view := columnVectorGraphAdjacencyBlockViewForTest(reader, raw, columnVectorGraphAdjacencySpan{start: 0, end: len(raw), count: 2})
	adjacency, scratch, direct, err := view.adjacency(0, nil)
	if err != nil {
		t.Fatalf("adjacency: %v", err)
	}
	if direct {
		t.Fatal("adjacency direct=true want fallback-only row-asset adjacency")
	}
	if !slices.Equal(adjacency, []uint32{3, 5}) || !slices.Equal(scratch, []uint32{3, 5}) {
		t.Fatalf("adjacency=%v scratch=%v want scratch decoded [3 5]", adjacency, scratch)
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

func TestColumnVectorGraphAdjacencyZeroLengthFallbackV1(t *testing.T) {
	view := columnVectorGraphAdjacencyBlockViewForTest(nil, nil, columnVectorGraphAdjacencySpan{})
	adjacency, scratch, direct, err := view.adjacency(0, nil)
	if err != nil {
		t.Fatalf("adjacency: %v", err)
	}
	if direct || adjacency != nil || scratch != nil {
		t.Fatalf("adjacency=%v scratch=%v direct=%t want nil nil false", adjacency, scratch, direct)
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
		adjacencyDirectView: false,
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
