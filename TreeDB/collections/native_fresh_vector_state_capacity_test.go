package collections

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Exercise the maintained asset emitter and codec, rather than eleven invented
// placeholder descriptors. Three real adjacency layers plus the emitted norm,
// five row-ref columns, document IDs and search pack give eleven actual assets.
func TestNativeFreshVectorStateActualAssets(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("docs", columnGraphRebuildColumnStoreConfigV2A(3))
	if err != nil {
		t.Fatal(err)
	}
	def := columnGraphRebuildVectorIndexDefinitionV2A(3, 2)
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{columnVectorGraphLayeredAdjacencyMagic, 2, 1, 1, 1, 1, 1, 1}, BaseRowRef: DocumentRowRef{Generation: 1, PartID: 1, RowIndex: 0, AppliedCommandLSN: 1}},
		{ID: []byte("b"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{columnVectorGraphLayeredAdjacencyMagic, 2, 1, 0, 1, 0, 1, 0}, BaseRowRef: DocumentRowRef{Generation: 1, PartID: 1, RowIndex: 1, AppliedCommandLSN: 1}},
	}
	_, _, state, _, err := prepareColumnVectorGraphRebuildAssets("docs", *cfg, def, 1, 17, 1, nil, rows, t.TempDir(), nil, nil, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	layers := columnVectorGraphExpectedAdjacencyLayerCountFromAssetRows1989(t, rows)
	wantAssets := layers + len(columnVectorGraphRowRefStateFields) + 3 // norm, document IDs, pack
	if layers != 3 || len(state.Assets) != wantAssets || len(state.Assets) != 11 {
		t.Fatalf("actual generated assets=%d layers=%d want=%d", len(state.Assets), layers, wantAssets)
	}
	shape, err := nativeFreshVectorStateShapeFromSnapshot(state)
	if err != nil {
		t.Fatal(err)
	}
	if shape.assetCount != uint64(len(state.Assets)) {
		t.Fatal("lost real asset census")
	}
	// The preaccept caller can perform this accumulation on its existing census
	// without constructing a descriptor slice. Actual strings, including layer
	// IDs and namespaces, determine the result; no fixed asset/byte constant.
	census, err := newNativeFreshVectorStateShape(def.Name, def.Field, def.Metric.String(), def.Encoding.String())
	if err != nil {
		t.Fatal(err)
	}
	for _, asset := range state.Assets {
		census, err = census.withAsset(asset.Role, asset.AssetID, asset.LogicalType, asset.PhysicalEncoding, string(asset.Ref.Kind), asset.Ref.Namespace)
		if err != nil {
			t.Fatal(err)
		}
	}
	if census != shape {
		t.Fatalf("scalar census=%+v actual=%+v", census, shape)
	}
	rawBytes, err := shape.rawBytes()
	if err != nil {
		t.Fatal(err)
	}
	raw := encodeColumnVectorIndexStateRecordV2(state)
	if rawBytes != uint64(len(raw)) {
		t.Fatalf("framing=%d encoder=%d", rawBytes, len(raw))
	}
	stored, err := encodeColumnVectorIndexStateRecord(state)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, raw) {
		t.Fatal("expected actual fresh eleven-asset shape to fit uncompressed")
	}
	slot, err := nativeFreshVectorStateCapacityV2(uint64(len(stored)), rawBytes, shape.assetCount)
	if err != nil {
		t.Fatal(err)
	}
	floor := memtable.COWAllocationCharge(uint64(len(stored))) + 3*memtable.COWAllocationCharge(rawBytes) + memtable.COWAllocationCharge(uint64(len(state.Assets))*uint64(unsafe.Sizeof(columnVectorIndexStateAssetSnapshot{}))) + memtable.COWAllocationCharge(uint64(len(state.Assets))*128)
	if slot != floor {
		t.Fatalf("fresh slot=%d actual descriptor/raw cohorts=%d", slot, floor)
	}
	if slot >= nativeSourceRequirementStateCapacityV2(columnVectorIndexStateMaxDecodedBytes) {
		t.Fatal("fresh shape unexpectedly uses maximum untrusted-state slot")
	}
	decoded, err := decodeColumnVectorIndexStateRecord(stored)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Assets) != len(state.Assets) {
		t.Fatal("maintained decoder census differs")
	}
	// Infer-layer normalization and asset order cannot change fixed-width size.
	state.AdjacencyLayerCount = 0
	for i, j := 0, len(state.Assets)-1; i < j; i, j = i+1, j-1 {
		state.Assets[i], state.Assets[j] = state.Assets[j], state.Assets[i]
	}
	normalized, err := encodeColumnVectorIndexStateRecord(state)
	if err != nil {
		t.Fatal(err)
	}
	if uint64(len(normalized)) != rawBytes {
		t.Fatal("normalization/order changed framing length")
	}
}

func TestNativeFreshVectorStateFramingAndOverflow(t *testing.T) {
	for _, names := range [][2]string{{}, {"index", "vector"}, {"索引", "nested.字段"}} {
		state := columnVectorIndexStateSnapshot{IndexName: names[0], Field: names[1], Metric: VectorMetricCosine, Encoding: VectorIndexEncodingFloat32}
		shape, err := nativeFreshVectorStateShapeFromSnapshot(state)
		if err != nil {
			t.Fatal(err)
		}
		size, err := shape.rawBytes()
		if err != nil || size != uint64(len(encodeColumnVectorIndexStateRecordV2(state))) {
			t.Fatalf("raw framing=%d err=%v", size, err)
		}
	}
	for _, shape := range []nativeFreshVectorStateShape{{assetCount: ^uint64(0)}, {stringBytes: ^uint64(0)}, {assetCount: (^uint64(0)) / 120}} {
		if _, err := shape.rawBytes(); !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatalf("overflow shape %+v: %v", shape, err)
		}
	}
	for _, shape := range []nativeFreshVectorStateShape{{assetCount: ^uint64(0)}, {stringBytes: ^uint64(0)}} {
		if _, err := shape.withAsset("r", "i", "l", "e", "k", "n"); !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatalf("asset accumulation overflow: %v", err)
		}
	}
	const fixed = uint64(4 + 2 + 4*8 + 10*8)
	cases := []struct{ stored, raw, assets uint64 }{
		{0, fixed, 0}, {columnVectorIndexStateMaxInlineRecordBytes + 1, fixed, 0},
		{1, fixed - 1, 0}, {1, columnVectorIndexStateMaxDecodedBytes + 1, 0},
		{1, fixed, 1}, {1, fixed + 119, 1}, {1, fixed, ^uint64(0)},
	}
	for _, c := range cases {
		if _, err := nativeFreshVectorStateCapacityV2(c.stored, c.raw, c.assets); !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatalf("invalid fresh shape %+v: %v", c, err)
		}
	}
	// Bounds on stored and decoded are independent. This is only a capacity
	// calculation with a supplied proven stored upper, not compression success.
	for _, c := range []struct{ stored, raw, assets uint64 }{{fixed, fixed, 0}, {fixed + 120, fixed + 120, 1}, {columnVectorIndexStateMaxInlineRecordBytes, columnVectorIndexStateMaxDecodedBytes, 1}} {
		if _, err := nativeFreshVectorStateCapacityV2(c.stored, c.raw, c.assets); err != nil {
			t.Fatalf("boundary %+v: %v", c, err)
		}
	}
}

func TestNativeFreshVectorStateCompressedShapeIsNotStoredProof(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("docs", testColumnGraphBaseColumnStoreConfigV2A())
	if err != nil {
		t.Fatal(err)
	}
	def := testColumnGraphVectorIndexDefinitionV2A()
	state := testColumnVectorIndexStateSnapshot1986(*cfg, def, 11, 17)
	layer0 := state.Assets[0]
	for layer := 1; layer < 32; layer++ {
		asset := layer0
		asset.AssetID = fmt.Sprintf("hnsw/layer/%d", layer)
		asset.Ref.PartID = uint64(100 + layer)
		state.Assets = append(state.Assets, asset)
	}
	state.AdjacencyLayerCount = 32
	shape, err := nativeFreshVectorStateShapeFromSnapshot(state)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := shape.rawBytes()
	if err != nil {
		t.Fatal(err)
	}
	if raw != uint64(len(encodeColumnVectorIndexStateRecordV2(state))) || raw <= columnVectorIndexStateMaxInlineRecordBytes {
		t.Fatal("expected actual oversized raw framing")
	}
	if _, err = nativeFreshVectorStateCapacityV2(raw, raw, shape.assetCount); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("raw length cannot masquerade as inline stored proof: %v", err)
	}
	stored, err := encodeColumnVectorIndexStateRecord(state)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = nativeFreshVectorStateCapacityV2(uint64(len(stored)), raw, shape.assetCount); err != nil {
		t.Fatal(err)
	}
	// Existing envelope preflight still supplies the raw length, and full decode
	// still validates compressed content. The new helper never parses this input.
	got, _, err := columnVectorIndexStateRecordDecodedLength(stored)
	if err != nil || uint64(got) != raw {
		t.Fatalf("maintained envelope decoded=%d want=%d err=%v", got, raw, err)
	}
}
