package collections

import (
	"errors"
	"math"
	"slices"
	"strings"
	"testing"
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
	if columnPhysicalNativeLittleEndian && len(vector) > 0 && len(vectorScratch) == 0 {
		t.Fatalf("little-endian vector used no scratch; want aligned scratch copy")
	}
	invNorm, err := view.invNorm(ref.rowIndex)
	if err != nil {
		t.Fatalf("invNorm: %v", err)
	}
	if math.Abs(float64(invNorm-1)) > 1e-6 {
		t.Fatalf("invNorm=%v want 1", invNorm)
	}
	adjacency, adjacencyScratch, err := view.adjacency(ref.rowIndex, nil)
	if err != nil {
		t.Fatalf("adjacency: %v", err)
	}
	if !slices.Equal(adjacency, []uint32{0}) || !slices.Equal(adjacencyScratch, []uint32{0}) {
		t.Fatalf("adjacency=%v scratch=%v want [0]", adjacency, adjacencyScratch)
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
