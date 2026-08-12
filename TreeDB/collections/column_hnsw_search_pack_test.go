package collections

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestColumnHNSWSearchPackRoundTrip2312(t *testing.T) {
	input := testColumnHNSWSearchPackInput2312()
	raw, err := encodeColumnHNSWSearchPack(input)
	if err != nil {
		t.Fatalf("encodeColumnHNSWSearchPack: %v", err)
	}
	pack, err := decodeColumnHNSWSearchPack(raw, columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: input.BaseIdentity})
	if err != nil {
		t.Fatalf("decodeColumnHNSWSearchPack: %v", err)
	}
	if pack.Header.Rows != input.Rows || pack.Header.Dimensions != input.Dimensions || pack.Header.VectorStride != input.VectorStride || pack.Header.M != input.M || pack.Header.EfConstruction != input.EfConstruction || pack.Header.EfSearch != input.EfSearch || pack.Header.EntryOrdinal != input.EntryOrdinal || pack.Header.MaxLayer != input.MaxLayer || pack.Header.AdjacencyLayerCount != len(input.AdjacencyLayers) {
		t.Fatalf("decoded header=%+v does not match input", pack.Header)
	}
	if pack.Header.BaseManifestGeneration != input.BaseIdentity.ManifestGeneration || pack.Header.BaseManifestChecksum != input.BaseIdentity.ManifestChecksum || pack.Header.BaseSchemaHash != input.BaseIdentity.SchemaHash {
		t.Fatalf("decoded base identity header=%+v input=%+v", pack.Header, input.BaseIdentity)
	}
	if !reflect.DeepEqual(pack.NormalizedVectors, input.NormalizedVectors) || !reflect.DeepEqual(pack.Levels, input.Levels) || !reflect.DeepEqual(pack.RowRefGenerations, input.RowRefGenerations) || !reflect.DeepEqual(pack.RowRefPartIDs, input.RowRefPartIDs) || !reflect.DeepEqual(pack.RowRefRowIndexes, input.RowRefRowIndexes) || !reflect.DeepEqual(pack.RowRefAppliedLSNs, input.RowRefAppliedCommandLSN) || !reflect.DeepEqual(pack.DocumentIDOffsets, input.DocumentIDOffsets) || !reflect.DeepEqual(pack.DocumentIDBytes, input.DocumentIDBytes) {
		t.Fatalf("decoded payload does not match input")
	}
	if len(pack.AdjacencyLayers) != len(input.AdjacencyLayers) {
		t.Fatalf("decoded adjacency layers=%d want %d", len(pack.AdjacencyLayers), len(input.AdjacencyLayers))
	}
	for layer := range input.AdjacencyLayers {
		if !reflect.DeepEqual(pack.AdjacencyLayers[layer].Offsets, input.AdjacencyLayers[layer].Offsets) || !reflect.DeepEqual(pack.AdjacencyLayers[layer].Neighbors, input.AdjacencyLayers[layer].Neighbors) {
			t.Fatalf("decoded adjacency layer %d=%+v want %+v", layer, pack.AdjacencyLayers[layer], input.AdjacencyLayers[layer])
		}
	}
	for _, section := range pack.Sections {
		payload := raw[section.Offset : section.Offset+section.Length]
		if got := page.Checksum(payload); got != section.Checksum {
			t.Fatalf("section %s[%d] checksum=%08x want %08x", section.Kind, section.Index, got, section.Checksum)
		}
	}
}

func TestColumnHNSWSearchPackAuxiliaryNavigationV3(t *testing.T) {
	v2 := testColumnHNSWSearchPackInput2312()
	v2.MembershipDigest[0] = 1
	v2Raw, err := encodeColumnHNSWSearchPack(v2)
	if err != nil || hnswPackU16(v2Raw, columnHNSWSearchPackHeaderVersionOffset) != columnHNSWSearchPackVersionV2 {
		t.Fatalf("digest-only v2 encode err=%v version=%d", err, hnswPackU16(v2Raw, columnHNSWSearchPackHeaderVersionOffset))
	}
	v2Pack, err := decodeColumnHNSWSearchPack(v2Raw, columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: v2.BaseIdentity, ExpectedMembershipDigest: v2.MembershipDigest})
	if err != nil || v2Pack.Header.HasAuxiliaryNavigation || len(v2Pack.AuxiliaryNavigation.Offsets) != 0 || len(v2Pack.AuxiliaryNavigation.Neighbors) != 0 {
		t.Fatalf("digest-only v2 decode err=%v auxiliary=%+v", err, v2Pack.AuxiliaryNavigation)
	}
	v2View, _ := testColumnHNSWSearchPackPreparedViewFromBytes2314(t, v2Raw, mappedresource.SourceHeapCopy, v2.BaseIdentity)
	var v2Scratch columnVectorGraphNativeSearchScratch
	if _, v2Stats, err := v2View.searchCosine([]float32{1, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: 1, StatsMode: columnVectorGraphNativeSearchStatsModeFullDiagnostics}, &v2Scratch); err != nil || v2Stats.AuxiliaryEdges != 0 || v2Stats.AuxiliaryCandidates != 0 || v2Stats.AuxiliaryAdmissions != 0 {
		_ = v2View.Close()
		t.Fatalf("v2 auxiliary stats=%+v err=%v", v2Stats, err)
	}
	if err := v2View.Close(); err != nil {
		t.Fatalf("close v2 view: %v", err)
	}

	connected := testColumnHNSWSearchPackInput2312()
	connected.MembershipDigest[0] = 1
	connected.HasAuxiliaryNavigation = true
	connected.AuxiliaryNavigation.Offsets = []uint64{0, 0, 0, 0}
	connectedRaw, err := encodeColumnHNSWSearchPack(connected)
	if err != nil || hnswPackU16(connectedRaw, columnHNSWSearchPackHeaderVersionOffset) != columnHNSWSearchPackVersionV3 {
		t.Fatalf("connected v3 encode err=%v version=%d", err, hnswPackU16(connectedRaw, columnHNSWSearchPackHeaderVersionOffset))
	}
	connectedPack, err := decodeColumnHNSWSearchPack(connectedRaw, columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: connected.BaseIdentity, ExpectedMembershipDigest: connected.MembershipDigest})
	if err != nil || !connectedPack.Header.HasAuxiliaryNavigation || !reflect.DeepEqual(connectedPack.AuxiliaryNavigation.Offsets, []uint64{0, 0, 0, 0}) || len(connectedPack.AuxiliaryNavigation.Neighbors) != 0 {
		t.Fatalf("connected v3 decode err=%v auxiliary=%+v", err, connectedPack.AuxiliaryNavigation)
	}
	input := testColumnHNSWSearchPackAuxiliaryInput4106()
	raw, err := encodeColumnHNSWSearchPack(input)
	if err != nil {
		t.Fatalf("encode v3: %v", err)
	}
	if version := hnswPackU16(raw, columnHNSWSearchPackHeaderVersionOffset); version != columnHNSWSearchPackVersionV3 {
		t.Fatalf("version=%d want %d", version, columnHNSWSearchPackVersionV3)
	}
	pack, err := decodeColumnHNSWSearchPack(raw, columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: input.BaseIdentity, ExpectedMembershipDigest: input.MembershipDigest})
	if err != nil {
		t.Fatalf("decode v3: %v", err)
	}
	if !pack.Header.HasAuxiliaryNavigation || !reflect.DeepEqual(pack.AuxiliaryNavigation, columnHNSWSearchPackLayer{Offsets: input.AuxiliaryNavigation.Offsets, Neighbors: input.AuxiliaryNavigation.Neighbors}) {
		t.Fatalf("decoded auxiliary=%+v header=%+v", pack.AuxiliaryNavigation, pack.Header)
	}
	view, _ := testColumnHNSWSearchPackPreparedViewFromBytes2314(t, raw, mappedresource.SourceHeapCopy, input.BaseIdentity)
	if !reflect.DeepEqual(view.AuxiliaryNavigation, columnHNSWSearchPackPreparedLayer{Offsets: input.AuxiliaryNavigation.Offsets, Neighbors: input.AuxiliaryNavigation.Neighbors}) {
		_ = view.Close()
		t.Fatalf("prepared auxiliary=%+v", view.AuxiliaryNavigation)
	}
	var scratch columnVectorGraphNativeSearchScratch
	results, stats, err := view.searchCosine([]float32{0, 0, 1}, columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: 1, StatsMode: columnVectorGraphNativeSearchStatsModeFullDiagnostics}, &scratch)
	if err != nil || len(results) != 1 || results[0].Ordinal != 2 || stats.AuxiliaryEdges == 0 || stats.AuxiliaryCandidates == 0 || stats.AuxiliaryAdmissions == 0 || stats.AuxiliaryAdmissions > stats.AuxiliaryCandidates {
		_ = view.Close()
		t.Fatalf("auxiliary traversal results=%+v stats=%+v err=%v", results, stats, err)
	}
	if err := view.Close(); err != nil {
		t.Fatalf("close v3 view: %v", err)
	}
	path := t.TempDir() + "/v3-pack.tca"
	fileRaw := append(make([]byte, int(columnHNSWSearchPackVectorSectionAlignment)), raw...)
	if err := os.WriteFile(path, fileRaw, 0o600); err != nil {
		t.Fatalf("WriteFile v3: %v", err)
	}
	manager := mappedresource.NewManager()
	key := testColumnHNSWSearchPackMappedResourceKey2314(int64(columnHNSWSearchPackVectorSectionAlignment), int64(len(raw)), page.Checksum(raw))
	handle, err := manager.AcquireFileRange(key, testColumnHNSWSearchPackScope2314(), path, mappedresource.AcquireOptions{Reason: "mapped v3 test", PreferMapped: true, AllowHeapCopy: true, ValidationMode: mappedresource.ValidationVerify, ResourcePath: path})
	if err != nil {
		t.Fatalf("AcquireFileRange v3: %v", err)
	}
	mappedView, err := newColumnHNSWSearchPackPreparedViewFromHandle(manager, handle, columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: input.BaseIdentity, ExpectedMembershipDigest: input.MembershipDigest})
	if err != nil {
		_ = handle.Release()
		t.Fatalf("new mapped v3 prepared view: %v", err)
	}
	if !reflect.DeepEqual(mappedView.AuxiliaryNavigation, columnHNSWSearchPackPreparedLayer{Offsets: input.AuxiliaryNavigation.Offsets, Neighbors: input.AuxiliaryNavigation.Neighbors}) {
		_ = mappedView.Close()
		t.Fatalf("mapped v3 prepared auxiliary=%+v", mappedView.AuxiliaryNavigation)
	}
	if err := mappedView.Close(); err != nil {
		t.Fatalf("close mapped v3 view: %v", err)
	}
	if manager.Stats().ActiveHandles != 0 {
		t.Fatalf("mapped v3 manager stats after close=%+v", manager.Stats())
	}
	for _, raw := range [][]byte{
		testColumnHNSWSearchPackMutateSectionPayload2312(raw, columnHNSWSearchPackSectionAuxiliaryOffsets, 0, func(payload []byte) { binary.LittleEndian.PutUint64(payload[8:], 0) }),
		testColumnHNSWSearchPackMutateSectionPayload2312(raw, columnHNSWSearchPackSectionAuxiliaryNeighbors, 0, func(payload []byte) { binary.LittleEndian.PutUint32(payload, 1) }),
	} {
		if _, err := decodeColumnHNSWSearchPack(raw, columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: input.BaseIdentity, ExpectedMembershipDigest: input.MembershipDigest}); err == nil {
			t.Fatal("malformed v3 auxiliary pack was accepted")
		}
		if _, _, err := testColumnHNSWSearchPackPreparedViewFromBytesAllowErr2314(raw, mappedresource.SourceHeapCopy, input.BaseIdentity); err == nil {
			t.Fatal("malformed v3 auxiliary prepared view was accepted")
		}
	}
}

func TestColumnHNSWSearchPackAuxiliaryNavigationUpperSeedAnchorV3(t *testing.T) {
	input := testColumnHNSWSearchPackUpperSeedAnchorInput4114()
	raw, err := encodeColumnHNSWSearchPack(input)
	if err != nil {
		t.Fatal(err)
	}
	view, _ := testColumnHNSWSearchPackPreparedViewFromBytes2314(t, raw, mappedresource.SourceHeapCopy, input.BaseIdentity)
	defer view.Close()
	var scratch columnVectorGraphNativeSearchScratch
	results, _, err := view.searchCosine([]float32{0, 1, 0}, columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: 2}, &scratch)
	if err != nil || len(results) != 1 || results[0].Ordinal != 3 {
		t.Fatalf("anchored results=%+v err=%v", results, err)
	}
	searcher := &VectorPartitionLocalSearcherV1{asset: VectorPartitionSearchAssetV1{Dimensions: 3}, prepared: view, opened: 1, searchRoute: VectorPartitionSearchRouteHNSWSearchPackV1}
	ordinary, ordinaryMetrics, err := searcher.SearchWithOptionsV1(t.Context(), []float32{0, 1, 0}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 2})
	attributed, attributedMetrics, _, attributedErr := searcher.SearchWithAttributionV1(t.Context(), []float32{0, 1, 0}, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 2})
	if err != nil || attributedErr != nil || !reflect.DeepEqual(ordinary, attributed) || ordinaryMetrics != attributedMetrics || ordinaryMetrics.Route != VectorPartitionSearchRouteHNSWSearchPackV1 {
		t.Fatalf("ordinary=%+v/%+v err=%v attributed=%+v/%+v err=%v", ordinary, ordinaryMetrics, err, attributed, attributedMetrics, attributedErr)
	}
	seed, err := view.greedyNearestAtLayer([]float32{0, 1, 0, 0}, 0, 1, columnVectorGraphScoreBatchModeScalar, &columnVectorGraphNativeSearchScratch{}, &columnVectorGraphNativeSearchStats{}, false, nil)
	if err != nil || seed != 1 {
		t.Fatalf("upper descent seed=%d err=%v", seed, err)
	}
	if got := view.AuxiliaryNavigation.Neighbors[view.AuxiliaryNavigation.Offsets[1]:view.AuxiliaryNavigation.Offsets[2]]; !reflect.DeepEqual(got, []uint32{0}) {
		t.Fatalf("upper seed anchor=%v want [0]", got)
	}
	levelDrift := testColumnHNSWSearchPackMutateSectionPayload2312(raw, columnHNSWSearchPackSectionLevels, 0, func(payload []byte) {
		binary.LittleEndian.PutUint16(payload[2:], 0)
	})
	if _, err := decodeColumnHNSWSearchPack(levelDrift, columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: input.BaseIdentity, ExpectedMembershipDigest: input.MembershipDigest}); err == nil {
		t.Fatal("v3 upper-seed level drift was accepted")
	}
	if _, _, err := testColumnHNSWSearchPackPreparedViewFromBytesAllowErr2314(levelDrift, mappedresource.SourceHeapCopy, input.BaseIdentity); err == nil {
		t.Fatal("v3 prepared upper-seed level drift was accepted")
	}
	withoutAuxiliary := input
	withoutAuxiliary.HasAuxiliaryNavigation = false
	withoutAuxiliary.AuxiliaryNavigation = columnHNSWSearchPackLayerInput{}
	withoutRaw, err := encodeColumnHNSWSearchPack(withoutAuxiliary)
	if err != nil {
		t.Fatal(err)
	}
	withoutView, _ := testColumnHNSWSearchPackPreparedViewFromBytes2314(t, withoutRaw, mappedresource.SourceHeapCopy, input.BaseIdentity)
	defer withoutView.Close()
	results, _, err = withoutView.searchCosine([]float32{0, 1, 0}, columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: 2}, &columnVectorGraphNativeSearchScratch{})
	if err != nil || len(results) != 1 || results[0].Ordinal == 3 {
		t.Fatalf("unanchored results=%+v err=%v", results, err)
	}
}

func TestColumnHNSWSearchPackDecodeRejectsCorruptEnvelope2312(t *testing.T) {
	raw := testColumnHNSWSearchPackRaw2312(t)
	cases := []struct {
		name string
		raw  []byte
		want string
	}{
		{
			name: "truncated_header",
			raw:  raw[:columnHNSWSearchPackHeaderSize-1],
			want: "truncated",
		},
		{
			name: "bad_magic",
			raw:  testColumnHNSWSearchPackPatchByte2312(raw, 0, 'X'),
			want: "bad hnsw_search_pack_v1 magic",
		},
		{
			name: "bad_version",
			raw:  testColumnHNSWSearchPackPatchU16Header2312(raw, columnHNSWSearchPackHeaderVersionOffset, columnHNSWSearchPackVersionV3+1),
			want: "unsupported hnsw_search_pack_v1 version",
		},
		{
			name: "corrupt_section_directory",
			raw:  testColumnHNSWSearchPackPatchByte2312(raw, columnHNSWSearchPackDirectoryOffset, raw[columnHNSWSearchPackDirectoryOffset]^0xff),
			want: "section directory checksum",
		},
		{
			name: "invalid_total_length",
			raw:  testColumnHNSWSearchPackPatchU64Header2312(raw, columnHNSWSearchPackHeaderTotalLengthOffset, uint64(len(raw)+1)),
			want: "length",
		},
		{
			name: "invalid_section_length",
			raw: testColumnHNSWSearchPackMutateSectionEntry2312(raw, columnHNSWSearchPackSectionNormalizedVectors, 0, func(section *columnHNSWSearchPackSection) {
				section.Length -= 4
				section.Count--
			}),
			want: "normalized_vectors",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeColumnHNSWSearchPack(tc.raw, columnHNSWSearchPackDecodeOptions{}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("decode err=%v want containing %q", err, tc.want)
			}
		})
	}
}

func TestColumnHNSWSearchPackDecodeRejectsShapeMismatches2312(t *testing.T) {
	raw := testColumnHNSWSearchPackRaw2312(t)
	cases := []struct {
		name string
		raw  []byte
		opts columnHNSWSearchPackDecodeOptions
		want string
	}{
		{
			name: "bad_row_count_cap",
			raw:  testColumnHNSWSearchPackPatchU64Header2312(raw, columnHNSWSearchPackHeaderRowsOffset, 4),
			opts: columnHNSWSearchPackDecodeOptions{MaxRows: 3},
			want: "row count",
		},
		{
			name: "bad_dimensions",
			raw:  testColumnHNSWSearchPackPatchU32Header2312(raw, columnHNSWSearchPackHeaderDimensionsOffset, 0),
			want: "dimensions",
		},
		{
			name: "bad_stride",
			raw:  testColumnHNSWSearchPackPatchU32Header2312(raw, columnHNSWSearchPackHeaderVectorStrideOffset, 3),
			want: "vector stride",
		},
		{
			name: "bad_alignment",
			raw: testColumnHNSWSearchPackMutateSectionEntry2312(raw, columnHNSWSearchPackSectionNormalizedVectors, 0, func(section *columnHNSWSearchPackSection) {
				section.Alignment = columnHNSWSearchPackAlignment
			}),
			want: "alignment",
		},
		{
			name: "bad_offsets",
			raw: testColumnHNSWSearchPackMutateSectionPayload2312(raw, columnHNSWSearchPackSectionAdjacencyOffsets, 0, func(payload []byte) {
				binary.LittleEndian.PutUint64(payload[2*8:], 1)
			}),
			want: "offsets are not monotonic",
		},
		{
			name: "bad_adjacency_ordinal",
			raw: testColumnHNSWSearchPackMutateSectionPayload2312(raw, columnHNSWSearchPackSectionAdjacencyNeighbors, 0, func(payload []byte) {
				binary.LittleEndian.PutUint32(payload, 3)
			}),
			want: "neighbor ordinal",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeColumnHNSWSearchPack(tc.raw, tc.opts); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("decode err=%v want containing %q", err, tc.want)
			}
		})
	}
}

func TestColumnHNSWSearchPackDecodeRejectsBaseManifestMismatch2312(t *testing.T) {
	input := testColumnHNSWSearchPackInput2312()
	raw, err := encodeColumnHNSWSearchPack(input)
	if err != nil {
		t.Fatalf("encodeColumnHNSWSearchPack: %v", err)
	}
	cases := []struct {
		name string
		want columnHNSWSearchPackBaseIdentity
		msg  string
	}{
		{name: "generation", want: columnHNSWSearchPackBaseIdentity{ManifestGeneration: input.BaseIdentity.ManifestGeneration + 1}, msg: "base manifest generation"},
		{name: "checksum", want: columnHNSWSearchPackBaseIdentity{ManifestChecksum: input.BaseIdentity.ManifestChecksum + 1}, msg: "base manifest checksum"},
		{name: "schema", want: columnHNSWSearchPackBaseIdentity{SchemaHash: input.BaseIdentity.SchemaHash + 1}, msg: "base schema hash"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeColumnHNSWSearchPack(raw, columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: tc.want}); err == nil || !strings.Contains(err.Error(), tc.msg) {
				t.Fatalf("decode err=%v want containing %q", err, tc.msg)
			}
		})
	}
}

func TestColumnHNSWSearchPackVectorIndexStateAssetContract2312(t *testing.T) {
	input := testColumnHNSWSearchPackInput2312()
	raw, err := encodeColumnHNSWSearchPack(input)
	if err != nil {
		t.Fatalf("encodeColumnHNSWSearchPack: %v", err)
	}
	ref := ColumnAssetRef{Kind: ColumnAssetKindTCS1HNSWSearchPack, Namespace: "docs_column_assets", Generation: input.BaseIdentity.ManifestGeneration, PartID: 77, FileID: 3, Offset: 16, Length: int64(len(raw)), Checksum: page.Checksum(raw)}
	state := columnVectorIndexStateSnapshot{
		IndexName:              "embedding_graph",
		Field:                  "embedding",
		Metric:                 VectorMetricCosine,
		Encoding:               VectorIndexEncodingFloat32,
		Dimensions:             input.Dimensions,
		M:                      input.M,
		EfConstruction:         input.EfConstruction,
		EfSearch:               input.EfSearch,
		RowCount:               input.Rows,
		BaseManifestGeneration: input.BaseIdentity.ManifestGeneration,
		BaseManifestChecksum:   input.BaseIdentity.ManifestChecksum,
		BaseSchemaHash:         input.BaseIdentity.SchemaHash,
		Assets: []columnVectorIndexStateAssetSnapshot{{
			Role:             columnVectorIndexStateAssetRoleHNSWSearchPack,
			AssetID:          columnVectorIndexStateHNSWSearchPackAssetID,
			LogicalType:      columnVectorIndexStateLogicalTypeSearchPack,
			PhysicalEncoding: columnVectorIndexStateEncodingHNSWSearchPackV1,
			RowCount:         input.Rows,
			SourceSchemaHash: input.BaseIdentity.SchemaHash,
			Ref:              ref,
			AssetBytes:       ref.Length,
		}},
	}
	encoded, err := encodeColumnVectorIndexStateRecord(state)
	if err != nil {
		t.Fatalf("encodeColumnVectorIndexStateRecord: %v", err)
	}
	decoded, err := decodeColumnVectorIndexStateRecord(encoded)
	if err != nil {
		t.Fatalf("decodeColumnVectorIndexStateRecord: %v", err)
	}
	if !reflect.DeepEqual(decoded, state) {
		t.Fatalf("decoded state=%+v want %+v", decoded, state)
	}
	refs, err := columnVectorIndexStateManifestAssetRefsForScan(decoded, input.BaseIdentity.ManifestGeneration, ref.Namespace)
	if err != nil {
		t.Fatalf("columnVectorIndexStateManifestAssetRefsForScan: %v", err)
	}
	if len(refs) != 1 || refs[0] != ref {
		t.Fatalf("scan refs=%+v want pack ref %+v", refs, ref)
	}

	wrongAssetID := state
	wrongAssetID.Assets = append([]columnVectorIndexStateAssetSnapshot(nil), state.Assets...)
	wrongAssetID.Assets[0].AssetID = "hnsw_search_pack_v0"
	if _, err := encodeColumnVectorIndexStateRecord(wrongAssetID); err == nil || !strings.Contains(err.Error(), "asset id") {
		t.Fatalf("wrong asset id encode err=%v want asset id failure", err)
	}
	wrongKind := state
	wrongKind.Assets = append([]columnVectorIndexStateAssetSnapshot(nil), state.Assets...)
	wrongKind.Assets[0].Ref.Kind = ColumnAssetKindTCS1TypedColumnPart
	if _, err := encodeColumnVectorIndexStateRecord(wrongKind); err == nil || !strings.Contains(err.Error(), "ref kind") {
		t.Fatalf("wrong kind encode err=%v want ref kind failure", err)
	}
	wrongEncoding := state
	wrongEncoding.Assets = append([]columnVectorIndexStateAssetSnapshot(nil), state.Assets...)
	wrongEncoding.Assets[0].PhysicalEncoding = columnVectorIndexStateEncodingRawBytesOffsets
	if _, err := encodeColumnVectorIndexStateRecord(wrongEncoding); err == nil || !strings.Contains(err.Error(), "type/encoding") {
		t.Fatalf("wrong encoding encode err=%v want type/encoding failure", err)
	}
}

func TestColumnHNSWSearchPackRebuildPublishesPack2313(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{3, 0, 0}},
		{id: "doc-b", vector: []float32{0, 4, 0}},
		{id: "doc-c", vector: []float32{0, 0, 5}},
		{id: "doc-d", vector: []float32{1, 1, 0}},
		{id: "doc-e", vector: []float32{0, 1, 1}},
	}
	dir, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	graph, scanned := loadAndScanColumnGraphRebuildRowsV2A(t, d, "docs", def)
	records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	state := columnVectorIndexStateFromRecords1987(t, records, def)
	asset, raw, pack := loadColumnHNSWSearchPackForTest2313(t, d, def, graph, state)
	assertColumnHNSWSearchPackMatchesRebuild2313(t, pack, raw, def, graph, scanned)
	assertColumnAssetReachabilityProtectsGraphRefV2A(t, col, asset.Ref)
	if got, want := status.Stats.BytesDisk, columnVectorGraphStorageBytesWithState(graph, state); got != want {
		t.Fatalf("status bytes_disk=%d want graph+state+pack=%d", got, want)
	}
	if asset.AssetBytes != int64(len(raw)) || asset.AssetBytes <= 0 {
		t.Fatalf("pack asset bytes=%d raw=%d", asset.AssetBytes, len(raw))
	}
	if cfg.ActiveManifest == nil || pack.Header.BaseManifestGeneration != cfg.ActiveManifest.Generation {
		t.Fatalf("pack base generation=%d active=%+v", pack.Header.BaseManifestGeneration, cfg.ActiveManifest)
	}
	if dir == "" {
		t.Fatal("test fixture returned empty dir")
	}
}

func TestColumnHNSWSearchPackReopenPreservesManifestIdentity2313(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	dir, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		_ = d.Close()
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	status, err := reopenedCol.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("VectorIndexStatus reopen: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	graph, scanned := loadAndScanColumnGraphRebuildRowsV2A(t, reopened, "docs", def)
	records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, reopened, "docs")
	state := columnVectorIndexStateFromRecords1987(t, records, def)
	_, raw, pack := loadColumnHNSWSearchPackForTest2313(t, reopened, def, graph, state)
	assertColumnHNSWSearchPackMatchesRebuild2313(t, pack, raw, def, graph, scanned)
	searcher, err := reopenedCol.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher reopen: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	var buf VectorIndexSearchBuffer
	response, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: []float32{1, 0, 0}, TopK: 2, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeProduction}, &buf)
	if err != nil {
		t.Fatalf("SearchWithBuffer reopen: %v", err)
	}
	stats := response.Stats
	if stats.SearchRouteHNSWSearchPack != 1 || stats.SearchRouteColumnGraphPrepared+stats.SearchRouteColumnGraphFallback != 0 || stats.HNSWSearchPackActive != 1 || stats.HNSWSearchPackFallbacks != 0 || stats.HNSWSearchPackMmapDirect+stats.HNSWSearchPackHeapCopy != 1 || stats.HNSWSearchPackOpenNanos == 0 || stats.HNSWSearchPackMappedBytes+stats.HNSWSearchPackHeapCopyBytes == 0 {
		t.Fatalf("reopen search stats=%+v want hnsw_search_pack_v1 route", stats)
	}
	fullOpts := VectorIndexSearcherSearchOptions{Query: []float32{1, 0, 0}, TopK: 2, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeFullDiagnostics}
	fullResponse, err := searcher.SearchWithBuffer(fullOpts, &buf)
	if err != nil {
		t.Fatalf("SearchWithBuffer reopen full stats: %v", err)
	}
	assertColumnHNSWSearchPackMatchesColumnGraphRoute2315(t, searcher, fullOpts, fullResponse)
}

func TestColumnHNSWSearchPackWorkAccountingStats(t *testing.T) {
	const dims = 128
	rows := make([]columnGraphRebuildInputRowV2A, 32)
	for i := range rows {
		vec := make([]float32, dims)
		for j := range vec {
			vec[j] = float32(((i+1)*(j+3))%17) + 1
		}
		rows[i] = columnGraphRebuildInputRowV2A{id: fmt.Sprintf("doc-%02d", i), vector: vec}
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, dims, 4, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	query := make([]float32, dims)
	for i := range query {
		query[i] = float32(i%5) + 1
	}
	var buf VectorIndexSearchBuffer
	response, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, TopK: 5, EfSearch: 16, StatsMode: VectorIndexSearchStatsModeWorkAccounting}, &buf)
	if err != nil {
		t.Fatalf("SearchWithBuffer work accounting: %v", err)
	}
	stats := response.Stats
	if stats.RouteKind() != VectorIndexSearchRouteExactHNSWSearchPackV1 || stats.WorkAccountingSearches != 1 {
		t.Fatalf("work-accounting route/stats=%+v", stats)
	}
	if stats.VisitedNodes == 0 || stats.VisitedEdges == 0 || stats.PreparedScoreCalls == 0 || stats.FP32ScoreCalls != stats.PreparedScoreCalls {
		t.Fatalf("work-accounting score/visit stats=%+v", stats)
	}
	if stats.QuantizedScoreCalls != 0 || stats.ExactRerankScoreCalls != 0 {
		t.Fatalf("exact route reported quantized/rerank work stats=%+v", stats)
	}
	if stats.FrontierPushes == 0 || stats.FrontierPops == 0 || stats.HeapPushes != stats.FrontierPushes || stats.HeapPops != stats.FrontierPops {
		t.Fatalf("work-accounting heap stats=%+v", stats)
	}
	if stats.DistanceKernelNanos == 0 || stats.GraphTraversalNanos == 0 {
		t.Fatalf("work-accounting timers missing stats=%+v", stats)
	}
	assertColumnVectorGraphPreparedIndexedBackendCounters2125(t, stats.ScoreBatchOptimizedCalls, stats.ScoreBatchScalarFallbackCalls, int(stats.ScoreBatchMaxTileSize), dims)
}

func TestColumnHNSWSearchPackEmptyAndSingleRowFixtures2313(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows []columnGraphRebuildInputRowV2A
	}{
		{name: "empty"},
		{name: "single", rows: []columnGraphRebuildInputRowV2A{{id: "solo", vector: []float32{1, 0, 0}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, tc.rows)
			defer func() { _ = d.Close() }()
			if _, err := col.RebuildVectorIndex(def.Name); err != nil {
				t.Fatalf("RebuildVectorIndex: %v", err)
			}
			graph, scanned := loadAndScanColumnGraphRebuildRowsV2A(t, d, "docs", def)
			records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
			state := columnVectorIndexStateFromRecords1987(t, records, def)
			_, raw, pack := loadColumnHNSWSearchPackForTest2313(t, d, def, graph, state)
			assertColumnHNSWSearchPackMatchesRebuild2313(t, pack, raw, def, graph, scanned)
			if len(tc.rows) == 0 && (pack.Header.EntryOrdinal != -1 || pack.Header.MaxLayer != -1 || pack.Header.AdjacencyLayerCount != 0) {
				t.Fatalf("empty pack header=%+v want no-entry/no-layer", pack.Header)
			}
			if len(tc.rows) == 1 && (pack.Header.EntryOrdinal != 0 || pack.Header.MaxLayer != 0 || pack.Header.AdjacencyLayerCount != 1 || len(pack.AdjacencyLayers[0].Neighbors) != 0) {
				t.Fatalf("single-row pack header/layers=%+v %+v", pack.Header, pack.AdjacencyLayers)
			}
		})
	}
}

func TestColumnHNSWSearchPackValidationRejectsMismatchedRefs2313(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	graph, _ := loadAndScanColumnGraphRebuildRowsV2A(t, d, "docs", def)
	records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	state := columnVectorIndexStateFromRecords1987(t, records, def)
	asset, _, _ := loadColumnHNSWSearchPackForTest2313(t, d, def, graph, state)

	badChecksum := state
	badChecksum.Assets = append([]columnVectorIndexStateAssetSnapshot(nil), state.Assets...)
	for i := range badChecksum.Assets {
		if badChecksum.Assets[i].Role == columnVectorIndexStateAssetRoleHNSWSearchPack {
			badChecksum.Assets[i].Ref.Checksum++
			break
		}
	}
	if err := validateColumnHNSWSearchPackStateAssetIfPresent(d.ColumnAssetRootDir(), *cfg, def, graph, badChecksum); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("bad checksum validation err=%v want checksum failure", err)
	}
	if _, err := decodeColumnHNSWSearchPack(readColumnHNSWSearchPackRawForTest2313(t, d, asset.Ref), columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: columnHNSWSearchPackBaseIdentity{ManifestGeneration: graph.BaseManifestGeneration + 1}}); err == nil || !strings.Contains(err.Error(), "base manifest generation") {
		t.Fatalf("base mismatch decode err=%v want generation failure", err)
	}
}

func TestColumnHNSWSearchPackSearchPathServesSearchWithBuffer2313(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	var buf VectorIndexSearchBuffer
	response, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: []float32{1, 0, 0}, TopK: 2, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeProduction}, &buf)
	if err != nil {
		t.Fatalf("SearchWithBuffer: %v", err)
	}
	if len(response.Results) != 2 {
		t.Fatalf("results=%d want 2", len(response.Results))
	}
	stats := response.Stats
	if stats.SearchRouteHNSWSearchPack != 1 || stats.SearchRouteColumnGraphPrepared+stats.SearchRouteColumnGraphFallback != 0 {
		t.Fatalf("route stats=%+v want hnsw_search_pack_v1 route", stats)
	}
	if stats.HNSWSearchPackActive != 1 || stats.HNSWSearchPackMissing != 0 || stats.HNSWSearchPackInvalid != 0 || stats.HNSWSearchPackFallbacks != 0 || stats.HNSWSearchPackMmapDirect+stats.HNSWSearchPackHeapCopy != 1 || stats.HNSWSearchPackOpenNanos == 0 || stats.HNSWSearchPackMappedBytes+stats.HNSWSearchPackHeapCopyBytes == 0 || stats.GraphRowFallbacks != 0 || stats.VectorScratchDecodes != 0 || stats.DocumentsFetched != 0 {
		t.Fatalf("pack stats=%+v want healthy hnsw_search_pack_v1 search routing", stats)
	}
}

func TestColumnHNSWSearchPackPreparedViewContracts2314(t *testing.T) {
	raw := testColumnHNSWSearchPackRaw2312(t)
	base := testColumnHNSWSearchPackInput2312().BaseIdentity
	view, handle := testColumnHNSWSearchPackPreparedViewFromBytes2314(t, raw, mappedresource.SourceHeapCopy, base)
	if view.status != columnHNSWSearchPackPreparedStatusHeap || len(view.NormalizedVectors) != len(testColumnHNSWSearchPackInput2312().NormalizedVectors) || len(view.Levels) != 3 || len(view.AdjacencyLayers) != 2 || len(view.RowRefGenerations) != 3 || len(view.DocumentIDOffsets) != 4 || string(view.DocumentIDBytes) != "doc-adoc-bdoc-c" {
		t.Fatalf("prepared view=%+v status=%s", view.Header, view.status)
	}
	stats := view.routeStats(columnHNSWSearchPackPreparedStatusMissing, 123)
	if stats.HNSWSearchPackActive != 1 || stats.HNSWSearchPackHeapCopy != 1 || stats.HNSWSearchPackMappedBytes != 0 || stats.HNSWSearchPackHeapCopyBytes != uint64(len(raw)) || stats.HNSWSearchPackOpenNanos != 123 || stats.HNSWSearchPackActiveHandles != 1 {
		t.Fatalf("heap route stats=%+v", stats)
	}
	if err := handle.Release(); err != nil {
		t.Fatalf("release stale handle: %v", err)
	}
	if err := view.validateLive(); err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("validate stale err=%v want stale", err)
	}
	staleStats := view.routeStats(columnHNSWSearchPackPreparedStatusMissing, 0)
	if staleStats.HNSWSearchPackStale != 1 || staleStats.HNSWSearchPackFallbacks != 1 {
		t.Fatalf("stale stats=%+v", staleStats)
	}

	closedView, _ := testColumnHNSWSearchPackPreparedViewFromBytes2314(t, raw, mappedresource.SourceHeapCopy, base)
	if err := closedView.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := closedView.validateLive(); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("validate closed err=%v want closed", err)
	}
	closedStats := closedView.routeStats(columnHNSWSearchPackPreparedStatusMissing, 0)
	if closedStats.HNSWSearchPackClosed != 1 || closedStats.HNSWSearchPackFallbacks != 1 || closedView.mappedResourceStats().ActiveHandles != 0 {
		t.Fatalf("closed stats=%+v manager=%+v", closedStats, closedView.mappedResourceStats())
	}

	bad := testColumnHNSWSearchPackMutateSectionPayload2312(raw, columnHNSWSearchPackSectionAdjacencyNeighbors, 0, func(payload []byte) {
		binary.LittleEndian.PutUint32(payload, 99)
	})
	if _, _, err := testColumnHNSWSearchPackPreparedViewFromBytesAllowErr2314(bad, mappedresource.SourceHeapCopy, base); err == nil || !strings.Contains(err.Error(), "neighbor ordinal") {
		t.Fatalf("bad prepared view err=%v want neighbor ordinal", err)
	}
}

func TestColumnHNSWSearchPackPreparedValidationObservesContext2314(t *testing.T) {
	const rows = 8192
	offsets := make([]uint64, rows+1)
	ctx := &vectorPartitionRouterDeadlineAfterErrContextV1{
		Context: context.Background(), deadlineAfter: 3,
	}
	err := validateColumnHNSWSearchPackAdjacencyWithContext(ctx, 0, rows, offsets, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("adjacency validation err=%v want deadline exceeded", err)
	}
	if ctx.calls != ctx.deadlineAfter {
		t.Fatalf("context calls=%d want %d", ctx.calls, ctx.deadlineAfter)
	}
}

func TestColumnHNSWSearchPackPreparedViewMappedFile2314(t *testing.T) {
	raw := testColumnHNSWSearchPackRaw2312(t)
	base := testColumnHNSWSearchPackInput2312().BaseIdentity
	path := t.TempDir() + "/pack.tca"
	fileRaw := append(make([]byte, int(columnHNSWSearchPackVectorSectionAlignment)), raw...)
	if err := os.WriteFile(path, fileRaw, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	manager := mappedresource.NewManager()
	key := testColumnHNSWSearchPackMappedResourceKey2314(int64(columnHNSWSearchPackVectorSectionAlignment), int64(len(raw)), page.Checksum(raw))
	handle, err := manager.AcquireFileRange(key, testColumnHNSWSearchPackScope2314(), path, mappedresource.AcquireOptions{Reason: "mapped test", PreferMapped: true, AllowHeapCopy: true, ValidationMode: mappedresource.ValidationVerify, ResourcePath: path})
	if err != nil {
		t.Fatalf("AcquireFileRange: %v", err)
	}
	view, err := newColumnHNSWSearchPackPreparedViewFromHandle(manager, handle, columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: base})
	if err != nil {
		_ = handle.Release()
		t.Fatalf("new prepared view: %v", err)
	}
	if handle.Source() == mappedresource.SourceMapped && view.status != columnHNSWSearchPackPreparedStatusDirect {
		t.Fatalf("mapped source status=%s want direct", view.status)
	}
	if handle.Source() == mappedresource.SourceHeapCopy && view.status != columnHNSWSearchPackPreparedStatusHeap {
		t.Fatalf("heap source status=%s want heap", view.status)
	}
	stats := view.routeStats(columnHNSWSearchPackPreparedStatusMissing, 0)
	if stats.HNSWSearchPackActive != 1 || stats.HNSWSearchPackActiveHandles != 1 || stats.HNSWSearchPackMappedBytes+stats.HNSWSearchPackHeapCopyBytes != uint64(len(raw)) {
		t.Fatalf("mapped file stats=%+v source=%s", stats, handle.Source())
	}
	if err := view.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if manager.Stats().ActiveHandles != 0 {
		t.Fatalf("manager stats after close=%+v", manager.Stats())
	}
}

func TestColumnHNSWSearchPackPreparedViewSharedReadersRelease2314(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
		{id: "doc-d", vector: []float32{0.5, 0.5, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	first, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher first: %v", err)
	}
	second, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		_ = first.Close()
		t.Fatalf("OpenVectorIndexSearcher second: %v", err)
	}
	if first.reader == nil || second.reader == nil || first.reader.hnswSearchPack == nil || second.reader.hnswSearchPack == nil {
		_ = second.Close()
		_ = first.Close()
		t.Fatalf("searchers missing prepared pack first=%+v second=%+v", first.reader, second.reader)
	}
	if first.reader.hnswSearchPack != second.reader.hnswSearchPack {
		_ = second.Close()
		_ = first.Close()
		t.Skip("current platform did not admit shared prepared search; pack release remains covered by per-reader close")
	}
	view := first.reader.hnswSearchPack
	if stats := view.mappedResourceStats(); stats.ActiveHandles != 1 {
		_ = second.Close()
		_ = first.Close()
		t.Fatalf("shared pack stats before close=%+v want one handle", stats)
	}
	if err := first.Close(); err != nil {
		_ = second.Close()
		t.Fatalf("first Close: %v", err)
	}
	if stats := view.mappedResourceStats(); stats.ActiveHandles != 1 || view.closed.Load() {
		_ = second.Close()
		t.Fatalf("shared pack stats after first close=%+v closed=%v", stats, view.closed.Load())
	}
	if err := second.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if stats := view.mappedResourceStats(); stats.ActiveHandles != 0 || !view.closed.Load() {
		t.Fatalf("shared pack stats after final close=%+v closed=%v", stats, view.closed.Load())
	}
}

func TestColumnHNSWSearchPackRouteMatchesColumnGraph2315(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1}},
		{id: "doc-e", vector: []float32{0.5, 0.5, 0}},
		{id: "doc-f", vector: []float32{0.2, 0.8, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	opts := VectorIndexSearcherSearchOptions{Query: []float32{1, 0.05, 0}, TopK: 4, EfSearch: len(rows), StatsMode: VectorIndexSearchStatsModeFullDiagnostics}
	var buffer VectorIndexSearchBuffer
	packResponse, err := searcher.SearchWithBuffer(opts, &buffer)
	if err != nil {
		t.Fatalf("SearchWithBuffer pack route: %v", err)
	}
	if packResponse.Stats.SearchRouteHNSWSearchPack != 1 || packResponse.Stats.HNSWSearchPackActive != 1 || packResponse.Stats.HNSWSearchPackFallbacks != 0 || packResponse.Stats.GraphRowFallbacks != 0 || packResponse.Stats.VectorScratchDecodes != 0 || packResponse.Stats.DocumentsFetched != 0 {
		t.Fatalf("pack route stats=%+v", packResponse.Stats)
	}
	assertColumnHNSWSearchPackMatchesColumnGraphRoute2315(t, searcher, opts, packResponse)
}

func TestColumnHNSWPreparedTraversalScorePlaneSeam2585(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1}},
		{id: "doc-e", vector: []float32{0.4, 0.6, 0}},
	}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	if searcher.reader == nil || searcher.reader.hnswSearchPack == nil {
		t.Fatal("searcher missing hnsw_search_pack_v1 prepared view")
	}
	pack := searcher.reader.hnswSearchPack
	query := []float32{1, 0.05, 0}
	nativeOpts := columnVectorGraphNativeSearchOptions{TopK: 3, EfSearch: len(rows), StatsMode: columnVectorGraphNativeSearchStatsModeFullDiagnostics, QueryMode: columnVectorGraphNativeSearchQueryModeExact}
	var packScratch columnVectorGraphNativeSearchScratch
	packResults, packStats, err := pack.searchCosine(query, nativeOpts, &packScratch)
	if err != nil {
		t.Fatalf("pack searchCosine exact: %v", err)
	}
	var tracedScratch columnVectorGraphNativeSearchScratch
	trace := columnHNSWSearchPackAttributionTrace{Termination: "stale"}
	tracedResults, _, err := pack.searchCosineWithContextTrace(t.Context(), query, nativeOpts, &tracedScratch, &trace)
	if err != nil {
		t.Fatalf("traced pack search: %v", err)
	}
	assertColumnHNSWPreparedTraversalResultsMatch2585(t, tracedResults, packResults)
	switch trace.Termination {
	case "candidate_limit", "frontier_empty_retained_full", "frontier_empty_no_seed", "distance_bound":
	default:
		t.Fatalf("trace termination=%q", trace.Termination)
	}
	var exactScratch columnVectorGraphNativeSearchScratch
	exactPlane := &columnHNSWPreparedExactFP32ScorePlane{}
	exactResults, exactStats, err := pack.searchCosinePreparedScorePlane(query, columnHNSWPreparedTraversalOptions{TopK: 3, EfSearch: len(rows), StatsMode: columnVectorGraphNativeSearchStatsModeFullDiagnostics}, &exactScratch, exactPlane)
	if err != nil {
		t.Fatalf("prepared traversal exact score plane: %v", err)
	}
	assertColumnHNSWPreparedTraversalResultsMatch2585(t, exactResults, packResults)
	if exactPlane.kind() != columnHNSWPreparedTraversalScorePlaneKindExactFP32 || exactStats.PreparedScoreCalls == 0 || exactStats.QuantizedScoreCalls != 0 || exactStats.Candidates != packStats.Candidates || exactStats.VisitedEdges != packStats.VisitedEdges {
		t.Fatalf("exact seam stats=%+v pack=%+v kind=%s", exactStats, packStats, exactPlane.kind())
	}

	var quantizedScratch columnVectorGraphNativeSearchScratch
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		t.Fatalf("columnVectorGraphInvNorm query: %v", err)
	}
	scorer, err := searcher.reader.prepareQuantizedScorer(columnVectorGraphNativeSearchQueryModeQuantizedOnly, def.QuantizedIndexes[0].Name, query, queryInvNorm, &quantizedScratch)
	if err != nil {
		t.Fatalf("prepareQuantizedScorer: %v", err)
	}
	quantizedPlane := &columnHNSWPreparedQuantizedScorePlane{scorer: &scorer}
	quantizedResults, quantizedStats, err := pack.searchCosinePreparedScorePlane(query, columnHNSWPreparedTraversalOptions{TopK: 3, EfSearch: len(rows), StatsMode: columnVectorGraphNativeSearchStatsModeFullDiagnostics}, &quantizedScratch, quantizedPlane)
	if err != nil {
		t.Fatalf("prepared traversal quantized score plane: %v", err)
	}
	if len(quantizedResults) != 3 {
		t.Fatalf("quantized seam results=%d want 3: %+v", len(quantizedResults), quantizedResults)
	}
	if quantizedPlane.kind() != columnHNSWPreparedTraversalScorePlaneKindQuantized || quantizedStats.QuantizedScoreCalls == 0 || quantizedStats.PreparedScoreCalls != 0 || quantizedStats.VectorBytesRead != 0 || quantizedStats.NormBytesRead != 0 || quantizedStats.GraphRowFallbacks != 0 {
		t.Fatalf("quantized seam stats=%+v kind=%s want explicit quantized scoring without exact fallback", quantizedStats, quantizedPlane.kind())
	}
	if _, _, err := pack.searchCosinePreparedScorePlane(query, columnHNSWPreparedTraversalOptions{TopK: 1, EfSearch: len(rows)}, &columnVectorGraphNativeSearchScratch{}, nil); !errors.Is(err, errColumnHNSWPreparedTraversalScorePlaneUnavailable) {
		t.Fatalf("nil score plane err=%v want explicit score-plane failure", err)
	}
}

type testColumnHNSWPreparedTraversalRowIDScorePlane2653 struct {
	rows           int
	singleCalls    int
	genericBatches int
	rowIDBatches   int
	fusedBatches   int
}

func (p *testColumnHNSWPreparedTraversalRowIDScorePlane2653) kind() columnHNSWPreparedTraversalScorePlaneKind {
	return columnHNSWPreparedTraversalScorePlaneKindQuantized
}

func (p *testColumnHNSWPreparedTraversalRowIDScorePlane2653) prepareForHNSWPreparedTraversal(pack *columnHNSWSearchPackPreparedView, query []float32, opts columnHNSWPreparedTraversalOptions, scratch *columnVectorGraphNativeSearchScratch) error {
	_ = query
	_ = opts
	_ = scratch
	if pack == nil {
		return errColumnHNSWPreparedTraversalScorePlaneUnavailable
	}
	p.rows = pack.Header.Rows
	return nil
}

func (p *testColumnHNSWPreparedTraversalRowIDScorePlane2653) scoreOrdinal(ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (float64, error) {
	_ = scratch
	_ = stats
	p.singleCalls++
	return p.score(uint32(ordinal)), nil
}

func (p *testColumnHNSWPreparedTraversalRowIDScorePlane2653) scoreOrdinals(ordinals []int, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	_ = scratch
	_ = stats
	p.genericBatches++
	if cap(dst) < len(ordinals) {
		dst = make([]float64, len(ordinals))
	} else {
		dst = dst[:len(ordinals)]
	}
	for i, ordinal := range ordinals {
		dst[i] = p.score(uint32(ordinal))
	}
	return dst, nil
}

func (p *testColumnHNSWPreparedTraversalRowIDScorePlane2653) scoreRowIDsPrevalidated(rowIDs []uint32, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	_ = scratch
	_ = stats
	p.rowIDBatches++
	if cap(dst) < len(rowIDs) {
		dst = make([]float64, len(rowIDs))
	} else {
		dst = dst[:len(rowIDs)]
	}
	for i, rowID := range rowIDs {
		dst[i] = p.score(rowID)
	}
	return dst, nil
}

func (p *testColumnHNSWPreparedTraversalRowIDScorePlane2653) scoreAndPushFrontierVisitedRowIDsPrevalidated(rowIDs []uint32, topK int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (int, error) {
	_ = stats
	p.fusedBatches++
	for _, rowID := range rowIDs {
		candidate := columnVectorGraphSearchCandidate{ordinal: int(rowID), score: p.score(rowID)}
		if scratch.insertTop(topK, candidate) {
			scratch.pushFrontier(candidate)
		}
	}
	return len(rowIDs), nil
}

func (p *testColumnHNSWPreparedTraversalRowIDScorePlane2653) score(rowID uint32) float64 {
	return float64(p.rows - int(rowID))
}

func TestColumnHNSWPreparedTraversalUsesRowIDScorePlane2653(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1}},
		{id: "doc-e", vector: []float32{0.4, 0.6, 0}},
	}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	if searcher.reader == nil || searcher.reader.hnswSearchPack == nil {
		t.Fatal("searcher missing hnsw_search_pack_v1 prepared view")
	}
	plane := &testColumnHNSWPreparedTraversalRowIDScorePlane2653{}
	var scratch columnVectorGraphNativeSearchScratch
	results, _, err := searcher.reader.hnswSearchPack.searchCosinePreparedScorePlane(
		[]float32{1, 0.05, 0},
		columnHNSWPreparedTraversalOptions{TopK: 3, EfSearch: len(rows), StatsMode: columnVectorGraphNativeSearchStatsModeFullDiagnostics},
		&scratch,
		plane,
	)
	if err != nil {
		t.Fatalf("prepared traversal rowID score plane: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("results=%d want 3: %+v", len(results), results)
	}
	if plane.rowIDBatches+plane.fusedBatches == 0 || plane.fusedBatches == 0 {
		t.Fatalf("rowID score plane was not used: rowIDBatches=%d fusedBatches=%d singleCalls=%d", plane.rowIDBatches, plane.fusedBatches, plane.singleCalls)
	}
	if plane.genericBatches != 0 {
		t.Fatalf("generic ordinal score plane batches=%d want 0 when rowID seam is available", plane.genericBatches)
	}
}

func TestColumnHNSWPreparedScalarU8RouteUsesTypedRowIDScorePlane2653(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1}},
		{id: "doc-e", vector: []float32{0.4, 0.6, 0}},
	}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	if searcher.reader == nil || searcher.reader.hnswSearchPack == nil {
		t.Fatal("searcher missing hnsw_search_pack_v1 prepared view")
	}

	query := []float32{0.8, 0.2, 0}
	qName := def.QuantizedIndexes[0].Name
	opts := columnVectorGraphNativeSearchOptions{
		TopK:               3,
		EfSearch:           len(rows),
		QueryMode:          columnVectorGraphNativeSearchQueryModeQuantizedOnly,
		QuantizedIndexName: qName,
		StatsMode:          columnVectorGraphNativeSearchStatsModeFullDiagnostics,
	}
	var routeScratch columnVectorGraphNativeSearchScratch
	results, stats, err := searcher.reader.SearchCosineScalarU8PreparedTraversal(searcher.reader.hnswSearchPack, query, opts, &routeScratch)
	if err != nil {
		t.Fatalf("SearchCosineScalarU8PreparedTraversal: %v", err)
	}
	want := scalarU8QuantizedTopKForTest1926(t, rows, query, opts.TopK)
	if len(results) != len(want) {
		t.Fatalf("scalar_u8 route results=%d want %d: %+v", len(results), len(want), results)
	}
	for i := range want {
		if !bytes.Equal(results[i].ID, want[i].ID) || math.Abs(results[i].Score-want[i].Score) > 1e-6 {
			t.Fatalf("scalar_u8 route result[%d]=%+v want id=%q ordinal=%d score=%v", i, results[i], want[i].ID, want[i].Ordinal, want[i].Score)
		}
	}
	if stats.SearchRouteQuantizedOnly != 1 || stats.SearchRouteQuantizedRerank != 0 || stats.QuantizedScorerActive != 1 || stats.PreparedScoreCalls != 0 || stats.QuantizedScoreCalls == 0 {
		t.Fatalf("scalar_u8 route stats=%+v want typed quantized-only scoring without exact scoring", stats)
	}
	if !routeScratch.preparedScalarU8Plane.ready {
		t.Fatalf("scalar_u8 route did not arm typed prepared score plane")
	}
	rowIDPlane, ok := any(&routeScratch.preparedScalarU8Plane).(columnHNSWPreparedTraversalRowIDScorePlane)
	if !ok {
		t.Fatalf("scalar_u8 prepared score plane does not implement direct rowID seam")
	}
	if routeScratch.preparedQuantizedPlane.scorer != nil || routeScratch.searchPlan.quantizedScorer.kind != columnVectorGraphQuantizedScorerKindNone {
		t.Fatalf("scalar_u8 route populated generic quantized scorer scratch: prepared=%p kind=%d", routeScratch.preparedQuantizedPlane.scorer, routeScratch.searchPlan.quantizedScorer.kind)
	}

	rowIDs := []uint32{0, 1, 4}
	ordinals := []int{0, 1, 4}
	var rowIDScratch, ordinalScratch columnVectorGraphNativeSearchScratch
	rowScores, err := rowIDPlane.scoreRowIDsPrevalidated(rowIDs, nil, &rowIDScratch, nil)
	if err != nil {
		t.Fatalf("scoreRowIDsPrevalidated: %v", err)
	}
	ordinalScores, err := routeScratch.preparedScalarU8Plane.scoreOrdinals(ordinals, nil, &ordinalScratch, nil)
	if err != nil {
		t.Fatalf("scoreOrdinals: %v", err)
	}
	if len(rowScores) != len(ordinalScores) {
		t.Fatalf("rowID scores=%d ordinal scores=%d", len(rowScores), len(ordinalScores))
	}
	for i := range rowScores {
		if rowScores[i] != ordinalScores[i] {
			t.Fatalf("score[%d] rowID=%v ordinal=%v", i, rowScores[i], ordinalScores[i])
		}
	}
}

func TestColumnHNSWPreparedScalarU8RerankUsesPackNativeRowIDExactScorer2657(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-exact", vector: append([]float32{0.40633525, -0.06700023, -0.027197814}, make([]float32, 61)...)},
		{id: "doc-quantized", vector: append([]float32{-0.22174846, 0.8332732, 0.28568664}, make([]float32, 61)...)},
	}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	if searcher.reader == nil || searcher.reader.hnswSearchPack == nil {
		t.Fatal("searcher missing hnsw_search_pack_v1 prepared view")
	}

	query := append([]float32{-0.23968919, -0.60389674, 0.9352316}, make([]float32, 61)...)
	quantizedTop := scalarU8QuantizedTopKForTest1926(t, rows, query, 1)
	exactTop := exactColumnGraphTopKForTest(t, rows, query, 1)
	if string(quantizedTop[0].ID) != "doc-quantized" || string(exactTop[0].ID) != "doc-exact" {
		t.Fatalf("fixture quantized=%q exact=%q want differing top candidates", quantizedTop[0].ID, exactTop[0].ID)
	}

	opts := columnVectorGraphNativeSearchOptions{
		TopK:                      1,
		EfSearch:                  len(rows),
		QueryMode:                 columnVectorGraphNativeSearchQueryModeQuantizedRerank,
		QuantizedIndexName:        def.QuantizedIndexes[0].Name,
		QuantizedRerankCandidates: len(rows),
		StatsMode:                 columnVectorGraphNativeSearchStatsModeFullDiagnostics,
	}
	var routeScratch columnVectorGraphNativeSearchScratch
	results, stats, err := searcher.reader.SearchCosineScalarU8PreparedTraversal(searcher.reader.hnswSearchPack, query, opts, &routeScratch)
	if err != nil {
		t.Fatalf("SearchCosineScalarU8PreparedTraversal quantized_rerank: %v", err)
	}
	assertColumnHNSWPreparedTraversalResultsMatch2585(t, results, exactTop)
	if stats.QuantizedRerankCandidates != uint64(len(rows)) || stats.QuantizedRerankExactScoreCalls != uint64(len(rows)) {
		t.Fatalf("scalar_u8 prepared rerank stats=%+v want shortlist=%d", stats, len(rows))
	}
	if stats.PreparedScoreCalls != uint64(len(rows)) || stats.VectorBytesRead != uint64(len(rows)*def.Dimensions*4) || stats.NormBytesRead != uint64(len(rows)*4) {
		t.Fatalf("scalar_u8 prepared rerank stats=%+v want pack-native exact row-ID vector reads and logical norm bytes", stats)
	}
	assertColumnVectorGraphPreparedIndexedBackendCounters2125(t, stats.ScoreBatchOptimizedCalls, stats.ScoreBatchScalarFallbackCalls, int(stats.ScoreBatchMaxTileSize), def.Dimensions)
	if routeScratch.searchPlan.reader != nil || routeScratch.searchPlan.physicalReader != nil || routeScratch.searchPlan.preparedSearch != nil || routeScratch.searchPlan.quantizedScorer.kind != columnVectorGraphQuantizedScorerKindNone {
		t.Fatalf("scalar_u8 prepared rerank populated generic native search plan: %+v", routeScratch.searchPlan)
	}
	if routeScratch.preparedQuantizedPlane.scorer != nil {
		t.Fatalf("scalar_u8 prepared rerank populated generic prepared quantized plane: %p", routeScratch.preparedQuantizedPlane.scorer)
	}
}

func TestColumnHNSWPreparedTraversalOmitReturnsRetainedShortlist2585(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0.7, 0.3, 0}},
		{id: "doc-d", vector: []float32{0.4, 0.6, 0}},
		{id: "doc-e", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	if searcher.reader == nil || searcher.reader.hnswSearchPack == nil {
		t.Fatal("searcher missing hnsw_search_pack_v1 prepared view")
	}
	pack := searcher.reader.hnswSearchPack
	query := []float32{1, 0.05, 0}
	const (
		topK     = 2
		retained = 4
	)
	opts := columnHNSWPreparedTraversalOptions{
		TopK:                      topK,
		EfSearch:                  len(rows),
		RetainedCandidateLimit:    retained,
		StatsMode:                 columnVectorGraphNativeSearchStatsModeFullDiagnostics,
		OmitResultMaterialization: true,
	}
	var scratch columnVectorGraphNativeSearchScratch
	shortlist, _, err := pack.searchCosinePreparedScorePlane(query, opts, &scratch, &columnHNSWPreparedExactFP32ScorePlane{})
	if err != nil {
		t.Fatalf("prepared traversal omitted shortlist: %v", err)
	}
	if len(shortlist) != retained {
		t.Fatalf("omitted shortlist results=%d want retained=%d: %+v", len(shortlist), retained, shortlist)
	}
	for i, result := range shortlist {
		if result.ID != nil || result.HasRowRef || len(result.RowRef.DocumentID) != 0 {
			t.Fatalf("omitted shortlist result[%d]=%+v unexpectedly materialized ID/row ref", i, result)
		}
	}

	var materializedScratch columnVectorGraphNativeSearchScratch
	materializedOpts := opts
	materializedOpts.OmitResultMaterialization = false
	materialized, _, err := pack.searchCosinePreparedScorePlane(query, materializedOpts, &materializedScratch, &columnHNSWPreparedExactFP32ScorePlane{})
	if err != nil {
		t.Fatalf("prepared traversal materialized results: %v", err)
	}
	if len(materialized) != topK {
		t.Fatalf("materialized results=%d want top_k=%d: %+v", len(materialized), topK, materialized)
	}
	for i, result := range materialized {
		if len(result.ID) == 0 {
			t.Fatalf("materialized result[%d]=%+v missing ID", i, result)
		}
	}
}

func TestColumnHNSWSearchPackExactRouteAndQuantizedFailClosed2585(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	query := []float32{1, 0, 0}
	var buffer VectorIndexSearchBuffer
	exact, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeExact, TopK: 2, EfSearch: len(rows), StatsMode: VectorIndexSearchStatsModeProduction}, &buffer)
	if err != nil {
		t.Fatalf("exact SearchWithBuffer: %v", err)
	}
	if exact.Stats.RouteKind() != VectorIndexSearchRouteExactHNSWSearchPackV1 || exact.Stats.SearchRouteHNSWSearchPack != 1 || exact.Stats.HNSWSearchPackActive != 1 || exact.Stats.SearchRouteQuantizedOnly+exact.Stats.SearchRouteQuantizedRerank != 0 || exact.Stats.QuantizedScorerActive != 0 {
		t.Fatalf("exact route stats=%+v want unchanged hnsw_search_pack_v1 route", exact.Stats)
	}
	quantizedOpts := VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: def.QuantizedIndexes[0].Name, TopK: 2, EfSearch: len(rows), StatsMode: VectorIndexSearchStatsModeProduction}
	quantized, err := searcher.SearchWithBuffer(quantizedOpts, &buffer)
	if err != nil {
		t.Fatalf("quantized SearchWithBuffer: %v", err)
	}
	assertQuantizedOnlyGuardrailStats2416(t, quantized.Stats, def.Dimensions)

	if searcher.reader == nil || searcher.reader.quantizedAssetStatus == nil {
		t.Fatal("searcher missing quantized asset status")
	}
	qName := def.QuantizedIndexes[0].Name
	originalStatus := searcher.reader.quantizedAssetStatus[qName]
	delete(searcher.reader.quantizedAssetStatus, qName)
	missingOnly, onlyErr := searcher.SearchWithBuffer(quantizedOpts, &buffer)
	rerankOpts := VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: qName, QuantizedRerankCandidates: 2, TopK: 2, EfSearch: len(rows), StatsMode: VectorIndexSearchStatsModeProduction}
	missingRerank, rerankErr := searcher.SearchWithBuffer(rerankOpts, &buffer)
	searcher.reader.quantizedAssetStatus[qName] = originalStatus
	if !errors.Is(onlyErr, ErrVectorIndexSearchUnavailable) || len(missingOnly.Results) != 0 {
		t.Fatalf("missing quantized_only asset response=%+v err=%v want fail-closed unavailable", missingOnly, onlyErr)
	}
	assertQuantizedUnavailableGuardrailStats2416(t, missingOnly.Stats, columnVectorGraphNativeSearchQueryModeQuantizedOnly, columnVectorGraphQuantizedAssetHealthMissing)
	if !errors.Is(rerankErr, ErrVectorIndexSearchUnavailable) || len(missingRerank.Results) != 0 {
		t.Fatalf("missing quantized_rerank asset response=%+v err=%v want fail-closed unavailable", missingRerank, rerankErr)
	}
	assertQuantizedUnavailableGuardrailStats2416(t, missingRerank.Stats, columnVectorGraphNativeSearchQueryModeQuantizedRerank, columnVectorGraphQuantizedAssetHealthMissing)
}

func assertColumnHNSWPreparedTraversalResultsMatch2585(tb testing.TB, got, want []columnVectorGraphNativeSearchResult) {
	tb.Helper()
	if len(got) != len(want) {
		tb.Fatalf("results=%d want %d got=%+v want=%+v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i].Ordinal != want[i].Ordinal || !bytes.Equal(got[i].ID, want[i].ID) || math.Abs(got[i].Score-want[i].Score) > 1e-6 {
			tb.Fatalf("result[%d]=%+v want %+v", i, got[i], want[i])
		}
	}
}

func TestColumnHNSWSearchPackMissingAndStaleFallbackSearchWithBuffer2315(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	opts := VectorIndexSearcherSearchOptions{Query: []float32{1, 0, 0}, TopK: 2, EfSearch: len(rows), StatsMode: VectorIndexSearchStatsModeProduction}

	missingSearcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher missing: %v", err)
	}
	missingSearcher.reader.hnswSearchPack = nil
	missingSearcher.reader.hnswSearchPackStatus = columnHNSWSearchPackPreparedStatusMissing
	missingSearcher.reader.hnswSearchPackOpenNanos = 0
	var missingBuffer VectorIndexSearchBuffer
	missingResponse, err := missingSearcher.SearchWithBuffer(opts, &missingBuffer)
	if closeErr := missingSearcher.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("SearchWithBuffer missing pack fallback: %v", err)
	}
	if len(missingResponse.Results) != opts.TopK || missingResponse.Stats.SearchRouteHNSWSearchPack != 0 || missingResponse.Stats.SearchRouteColumnGraphPrepared+missingResponse.Stats.SearchRouteColumnGraphFallback != 1 || missingResponse.Stats.HNSWSearchPackMissing != 1 || missingResponse.Stats.HNSWSearchPackFallbacks != 1 {
		t.Fatalf("missing fallback response=%+v stats=%+v", missingResponse.Results, missingResponse.Stats)
	}

	staleSearcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher stale: %v", err)
	}
	if staleSearcher.reader == nil || staleSearcher.reader.hnswSearchPack == nil || staleSearcher.reader.hnswSearchPack.handle == nil {
		_ = staleSearcher.Close()
		t.Fatal("stale test opened searcher without hnsw search pack handle")
	}
	if err := staleSearcher.reader.hnswSearchPack.handle.Release(); err != nil {
		_ = staleSearcher.Close()
		t.Fatalf("release pack handle for stale simulation: %v", err)
	}
	var staleBuffer VectorIndexSearchBuffer
	staleResponse, err := staleSearcher.SearchWithBuffer(opts, &staleBuffer)
	if closeErr := staleSearcher.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("SearchWithBuffer stale pack fallback: %v", err)
	}
	if len(staleResponse.Results) != opts.TopK || staleResponse.Stats.SearchRouteHNSWSearchPack != 0 || staleResponse.Stats.SearchRouteColumnGraphPrepared+staleResponse.Stats.SearchRouteColumnGraphFallback != 1 || staleResponse.Stats.HNSWSearchPackStale != 1 || staleResponse.Stats.HNSWSearchPackFallbacks != 1 {
		t.Fatalf("stale fallback response=%+v stats=%+v", staleResponse.Results, staleResponse.Stats)
	}
}

func assertColumnHNSWSearchPackMatchesColumnGraphRoute2315(tb testing.TB, searcher *VectorIndexSearcher, opts VectorIndexSearcherSearchOptions, packResponse VectorIndexSearchResponse) {
	tb.Helper()
	if searcher == nil || searcher.reader == nil {
		tb.Fatal("nil searcher/reader for column_graph parity")
	}
	var scratch columnVectorGraphNativeSearchScratch
	reference, referenceStats, err := searcher.reader.SearchCosine(opts.Query, columnVectorGraphNativeSearchOptions{TopK: opts.TopK, EfSearch: opts.EfSearch, StatsMode: columnVectorGraphNativeSearchStatsModeFullDiagnostics}, &scratch)
	if err != nil {
		tb.Fatalf("reference SearchCosine: %v", err)
	}
	if len(packResponse.Results) != len(reference) {
		tb.Fatalf("pack results=%d reference=%d", len(packResponse.Results), len(reference))
	}
	for i := range reference {
		if packResponse.Results[i].Ordinal != reference[i].Ordinal || string(packResponse.Results[i].ID) != string(reference[i].ID) || math.Abs(packResponse.Results[i].Score-reference[i].Score) > 1e-5 {
			tb.Fatalf("result[%d]=%+v reference ordinal=%d id=%q score=%v", i, packResponse.Results[i], reference[i].Ordinal, reference[i].ID, reference[i].Score)
		}
	}
	if packResponse.Stats.Candidates != referenceStats.Candidates || packResponse.Stats.VisitedEdges != referenceStats.VisitedEdges {
		tb.Fatalf("pack stats candidates/edges=(%d,%d) reference=(%d,%d) pack=%+v reference=%+v", packResponse.Stats.Candidates, packResponse.Stats.VisitedEdges, referenceStats.Candidates, referenceStats.VisitedEdges, packResponse.Stats, referenceStats)
	}
}

func TestColumnHNSWSearchPackInvalidCandidateFallsBackToCurrentRoute2314(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	graph, _ := loadAndScanColumnGraphRebuildRowsV2A(t, d, "docs", def)
	records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	state := columnVectorIndexStateFromRecords1987(t, records, def)
	asset, _, _ := loadColumnHNSWSearchPackForTest2313(t, d, def, graph, state)
	path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), asset.Ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile pack: %v", err)
	}
	if _, err := file.WriteAt([]byte{'X'}, asset.Ref.Offset); err != nil {
		_ = file.Close()
		t.Fatalf("corrupt pack WriteAt: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close corrupt pack file: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher with invalid pack: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	var buf VectorIndexSearchBuffer
	response, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: []float32{1, 0, 0}, TopK: 2, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeProduction}, &buf)
	if err != nil {
		t.Fatalf("SearchWithBuffer with invalid pack: %v", err)
	}
	if len(response.Results) != 2 {
		t.Fatalf("results=%d want 2", len(response.Results))
	}
	stats := response.Stats
	if stats.SearchRouteHNSWSearchPack != 0 || stats.SearchRouteColumnGraphPrepared+stats.SearchRouteColumnGraphFallback != 1 || stats.HNSWSearchPackInvalid != 1 || stats.HNSWSearchPackFallbacks != 1 || stats.HNSWSearchPackActive != 0 {
		t.Fatalf("stats=%+v want invalid pack counted while current route searches", stats)
	}
}

func loadColumnHNSWSearchPackForTest2313(tb testing.TB, d *backenddb.DB, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot) (columnVectorIndexStateAssetSnapshot, []byte, columnHNSWSearchPack) {
	tb.Helper()
	asset, found, err := findColumnHNSWSearchPackStateAsset(state)
	if err != nil {
		tb.Fatalf("findColumnHNSWSearchPackStateAsset: %v", err)
	}
	if !found {
		tb.Fatalf("state missing hnsw search pack asset: %+v", state.Assets)
	}
	raw := readColumnHNSWSearchPackRawForTest2313(tb, d, asset.Ref)
	pack, err := decodeColumnHNSWSearchPack(raw, columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: columnHNSWSearchPackBaseIdentity{ManifestGeneration: graph.BaseManifestGeneration, ManifestChecksum: graph.BaseManifestChecksum, SchemaHash: graph.BaseSchemaHash}})
	if err != nil {
		tb.Fatalf("decodeColumnHNSWSearchPack: %v", err)
	}
	return asset, raw, pack
}

func readColumnHNSWSearchPackRawForTest2313(tb testing.TB, d *backenddb.DB, ref ColumnAssetRef) []byte {
	tb.Helper()
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), ref)
	if err != nil {
		tb.Fatalf("read hnsw search pack asset: %v", err)
	}
	return raw
}

func assertColumnHNSWSearchPackMatchesRebuild2313(tb testing.TB, pack columnHNSWSearchPack, raw []byte, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, rows []columnGraphRebuildScannedRowV2A) {
	tb.Helper()
	stride, err := columnHNSWSearchPackVectorStrideForDimensions(def.Dimensions)
	if err != nil {
		tb.Fatalf("columnHNSWSearchPackVectorStrideForDimensions: %v", err)
	}
	if pack.Header.Rows != len(rows) || pack.Header.Rows != graph.RowCount || pack.Header.Dimensions != def.Dimensions || pack.Header.VectorStride != stride || pack.Header.M != def.M || pack.Header.EfConstruction != def.EfConstruction || pack.Header.EfSearch != def.EfSearch {
		tb.Fatalf("pack header=%+v graph rows=%d def=%+v stride=%d", pack.Header, graph.RowCount, def, stride)
	}
	if pack.Header.BaseManifestGeneration != graph.BaseManifestGeneration || pack.Header.BaseManifestChecksum != graph.BaseManifestChecksum || pack.Header.BaseSchemaHash != graph.BaseSchemaHash {
		tb.Fatalf("pack base identity header=%+v graph=%+v", pack.Header, graph)
	}
	wantSections := 8 + 2*pack.Header.AdjacencyLayerCount
	if len(pack.Sections) != wantSections {
		tb.Fatalf("pack sections=%d want %d", len(pack.Sections), wantSections)
	}
	if len(pack.NormalizedVectors) != len(rows)*stride || len(pack.Levels) != len(rows) || len(pack.RowRefGenerations) != len(rows) || len(pack.RowRefPartIDs) != len(rows) || len(pack.RowRefRowIndexes) != len(rows) || len(pack.RowRefAppliedLSNs) != len(rows) || len(pack.DocumentIDOffsets) != len(rows)+1 {
		tb.Fatalf("pack section lengths vectors=%d levels=%d rowrefs=(%d,%d,%d,%d) doc_offsets=%d rows=%d stride=%d", len(pack.NormalizedVectors), len(pack.Levels), len(pack.RowRefGenerations), len(pack.RowRefPartIDs), len(pack.RowRefRowIndexes), len(pack.RowRefAppliedLSNs), len(pack.DocumentIDOffsets), len(rows), stride)
	}
	assetRows := make([]columnVectorGraphAssetRow, len(rows))
	for ordinal, row := range rows {
		assetRows[ordinal] = columnVectorGraphAssetRow{ID: []byte(row.id), Vector: row.vector, InvNorm: row.invNorm, Adjacency: row.adjacency}
		base := ordinal * stride
		for dim := 0; dim < def.Dimensions; dim++ {
			want := row.vector[dim] * row.invNorm
			if math.Abs(float64(pack.NormalizedVectors[base+dim]-want)) > 1e-6 {
				tb.Fatalf("normalized row=%d dim=%d got=%v want=%v", ordinal, dim, pack.NormalizedVectors[base+dim], want)
			}
		}
		for pad := def.Dimensions; pad < stride; pad++ {
			if pack.NormalizedVectors[base+pad] != 0 {
				tb.Fatalf("normalized row=%d padding dim=%d got=%v want 0", ordinal, pad, pack.NormalizedVectors[base+pad])
			}
		}
		if pack.RowRefGenerations[ordinal] <= 0 || pack.RowRefPartIDs[ordinal] <= 0 || pack.RowRefAppliedLSNs[ordinal] <= 0 || pack.RowRefRowIndexes[ordinal] < 0 {
			tb.Fatalf("row-ref ordinal=%d generations/part/row/lsn=(%d,%d,%d,%d)", ordinal, pack.RowRefGenerations[ordinal], pack.RowRefPartIDs[ordinal], pack.RowRefRowIndexes[ordinal], pack.RowRefAppliedLSNs[ordinal])
		}
		start, end := pack.DocumentIDOffsets[ordinal], pack.DocumentIDOffsets[ordinal+1]
		if string(pack.DocumentIDBytes[start:end]) != row.id {
			tb.Fatalf("document id ordinal=%d got=%q want %q", ordinal, string(pack.DocumentIDBytes[start:end]), row.id)
		}
	}
	lists, err := buildColumnVectorIndexStateAdjacencyLists(assetRows)
	if err != nil {
		tb.Fatalf("buildColumnVectorIndexStateAdjacencyLists: %v", err)
	}
	if len(rows) == 0 {
		lists = nil
	}
	if len(pack.AdjacencyLayers) != len(lists) {
		tb.Fatalf("pack adjacency layers=%d want %d", len(pack.AdjacencyLayers), len(lists))
	}
	for layer, list := range lists {
		if !uint64SlicesEqual2313(pack.AdjacencyLayers[layer].Offsets, list.Offsets) || !uint32SlicesEqual(pack.AdjacencyLayers[layer].Neighbors, list.Values) {
			tb.Fatalf("pack adjacency layer %d=%+v want offsets=%v values=%v", layer, pack.AdjacencyLayers[layer], list.Offsets, list.Values)
		}
	}
	for _, section := range pack.Sections {
		if section.Offset%uint64(section.Alignment) != 0 || section.Offset+section.Length > uint64(len(raw)) {
			tb.Fatalf("bad section bounds/alignment %+v raw=%d", section, len(raw))
		}
	}
}

func uint64SlicesEqual2313(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func BenchmarkColumnHNSWSearchPackRebuildStorage2313(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	params := columnHNSWSearchPackRebuildBenchParamsFromEnv2313(b)
	d, col, def := openColumnHNSWSearchPackRebuildBenchCollection2313(b, docs, dims, params)
	defer func() { _ = d.Close() }()
	b.ReportAllocs()
	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(params.m), "hnsw_m")
	b.ReportMetric(float64(params.efConstruction), "ef_construction")
	b.ReportMetric(float64(params.efSearch), "ef_search")
	var status VectorIndexStatus
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		status, err = col.RebuildVectorIndex(def.Name)
		if err != nil {
			b.Fatalf("RebuildVectorIndex: %v", err)
		}
		if !status.Loaded || status.RebuildNeeded {
			b.Fatalf("status=%+v, want loaded", status)
		}
		columnGraphRebuildBenchSinkV2A = status
	}
	b.StopTimer()
	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/sec")
	}
	b.ReportMetric(float64(status.Stats.BytesDisk), "index_bytes_disk")
	if status.Duration > 0 {
		b.ReportMetric(float64(status.Duration.Nanoseconds()), "rebuild_ns")
	}
	records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(b, d, "docs")
	state := columnVectorIndexStateFromRecords1987(b, records, def)
	packAsset := reportColumnHNSWSearchPackStorageMetrics2313(b, d, state)
	if status.Stats.BytesDisk >= packAsset.AssetBytes {
		b.ReportMetric(float64(status.Stats.BytesDisk-packAsset.AssetBytes), "index_without_hnsw_search_pack_B/op")
	}
}

type columnHNSWSearchPackRebuildBenchParams2313 struct {
	m              int
	efConstruction int
	efSearch       int
}

func columnHNSWSearchPackRebuildBenchParamsFromEnv2313(tb testing.TB) columnHNSWSearchPackRebuildBenchParams2313 {
	tb.Helper()
	params := columnHNSWSearchPackRebuildBenchParams2313{
		m:              vectorBenchmarkPositiveEnvInt(tb, "TREEDB_VECTOR_BENCH_M", 16),
		efConstruction: vectorBenchmarkPositiveEnvInt(tb, "TREEDB_VECTOR_BENCH_EF_CONSTRUCTION", 128),
		efSearch:       vectorBenchmarkPositiveEnvInt(tb, "TREEDB_VECTOR_BENCH_EF_SEARCH", 128),
	}
	if params.efConstruction < params.m {
		tb.Fatalf("TREEDB_VECTOR_BENCH_EF_CONSTRUCTION=%d must be >= TREEDB_VECTOR_BENCH_M=%d", params.efConstruction, params.m)
	}
	return params
}

func openColumnHNSWSearchPackRebuildBenchCollection2313(tb testing.TB, docs, dims int, params columnHNSWSearchPackRebuildBenchParams2313) (*backenddb.DB, *Collection, VectorIndexDefinition) {
	tb.Helper()
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(tb, dir)
	def, err := normalizeVectorIndexDefinition(VectorIndexDefinition{
		Name:           "embedding_graph_production",
		Field:          "embedding",
		Metric:         VectorMetricCosine,
		Dimensions:     dims,
		M:              params.m,
		EfConstruction: params.efConstruction,
		EfSearch:       params.efSearch,
		Strategy:       VectorIndexStrategyColumnGraph,
	})
	if err != nil {
		_ = d.Close()
		tb.Fatalf("normalize vector index definition: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    columnGraphRebuildColumnStoreConfigV2A(dims),
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection: %v", err)
	}
	insertColumnHNSWSearchPackRebuildBenchRows2313(tb, col, docs, dims, 512)
	return d, col, def
}

func insertColumnHNSWSearchPackRebuildBenchRows2313(tb testing.TB, col *Collection, docs, dims, batchSize int) {
	tb.Helper()
	ids := make([][]byte, 0, batchSize)
	documents := make([][]byte, 0, batchSize)
	flush := func() {
		if len(ids) == 0 {
			return
		}
		if _, err := col.InsertBatch(ids, documents); err != nil {
			tb.Fatalf("InsertBatch: %v", err)
		}
		ids = ids[:0]
		documents = documents[:0]
	}
	for i := 0; i < docs; i++ {
		docID := fmt.Sprintf("doc-%06d", i)
		raw, err := json.Marshal(map[string]any{
			"time_us":   int64(i + 1),
			"kind":      "vector",
			"did":       docID,
			"embedding": vectorBenchmarkEmbedding(i, dims),
		})
		if err != nil {
			tb.Fatalf("json.Marshal row %q: %v", docID, err)
		}
		ids = append(ids, []byte(docID))
		documents = append(documents, raw)
		if len(ids) == batchSize {
			flush()
		}
	}
	flush()
	if err := col.Flush(); err != nil {
		tb.Fatalf("Flush: %v", err)
	}
}

func reportColumnHNSWSearchPackStorageMetrics2313(b *testing.B, d *backenddb.DB, state columnVectorIndexStateSnapshot) columnVectorIndexStateAssetSnapshot {
	b.Helper()
	asset, found, err := findColumnHNSWSearchPackStateAsset(state)
	if err != nil {
		b.Fatalf("findColumnHNSWSearchPackStateAsset: %v", err)
	}
	if !found {
		b.Fatalf("state missing hnsw search pack asset: %+v", state.Assets)
	}
	raw := readColumnHNSWSearchPackRawForTest2313(b, d, asset.Ref)
	pack, err := decodeColumnHNSWSearchPack(raw, columnHNSWSearchPackDecodeOptions{})
	if err != nil {
		b.Fatalf("decodeColumnHNSWSearchPack: %v", err)
	}
	var payloadBytes, normalizedBytes, levelsBytes, adjacencyOffsetsBytes, adjacencyNeighborsBytes, rowRefBytes, documentIDOffsetsBytes, documentIDBytes uint64
	for _, section := range pack.Sections {
		payloadBytes += section.Length
		switch section.Kind {
		case columnHNSWSearchPackSectionNormalizedVectors:
			normalizedBytes += section.Length
		case columnHNSWSearchPackSectionLevels:
			levelsBytes += section.Length
		case columnHNSWSearchPackSectionAdjacencyOffsets:
			adjacencyOffsetsBytes += section.Length
		case columnHNSWSearchPackSectionAdjacencyNeighbors:
			adjacencyNeighborsBytes += section.Length
		case columnHNSWSearchPackSectionRowRefGeneration, columnHNSWSearchPackSectionRowRefPartID, columnHNSWSearchPackSectionRowRefRowIndex, columnHNSWSearchPackSectionRowRefAppliedLSN:
			rowRefBytes += section.Length
		case columnHNSWSearchPackSectionDocumentIDOffsets:
			documentIDOffsetsBytes += section.Length
		case columnHNSWSearchPackSectionDocumentIDBytes:
			documentIDBytes += section.Length
		}
	}
	metadataBytes := int64(columnHNSWSearchPackHeaderSize + len(pack.Sections)*columnHNSWSearchPackSectionEntrySize)
	paddingBytes := int64(len(raw)) - metadataBytes - int64(payloadBytes)
	if paddingBytes < 0 {
		paddingBytes = 0
	}
	b.ReportMetric(float64(asset.AssetBytes), "hnsw_search_pack_B/op")
	b.ReportMetric(float64(metadataBytes), "hnsw_search_pack_metadata_B/op")
	b.ReportMetric(float64(paddingBytes), "hnsw_search_pack_padding_B/op")
	b.ReportMetric(float64(normalizedBytes), "hnsw_search_pack_normalized_vectors_B/op")
	b.ReportMetric(float64(levelsBytes), "hnsw_search_pack_levels_B/op")
	b.ReportMetric(float64(adjacencyOffsetsBytes), "hnsw_search_pack_adjacency_offsets_B/op")
	b.ReportMetric(float64(adjacencyNeighborsBytes), "hnsw_search_pack_adjacency_neighbors_B/op")
	b.ReportMetric(float64(adjacencyOffsetsBytes+adjacencyNeighborsBytes), "hnsw_search_pack_adjacency_B/op")
	b.ReportMetric(float64(rowRefBytes), "hnsw_search_pack_row_refs_B/op")
	b.ReportMetric(float64(documentIDOffsetsBytes), "hnsw_search_pack_document_id_offsets_B/op")
	b.ReportMetric(float64(documentIDBytes), "hnsw_search_pack_document_id_bytes_B/op")
	b.ReportMetric(float64(rowRefBytes+documentIDOffsetsBytes+documentIDBytes), "hnsw_search_pack_identity_B/op")
	b.ReportMetric(float64(len(pack.Sections)), "hnsw_search_pack_sections")
	b.ReportMetric(float64(pack.Header.AdjacencyLayerCount), "hnsw_search_pack_adjacency_layers")
	b.ReportMetric(float64(pack.Header.VectorStride), "hnsw_search_pack_vector_stride")
	if asset.RowCount > 0 {
		b.ReportMetric(float64(asset.AssetBytes)/float64(asset.RowCount), "hnsw_search_pack_B/row")
		b.ReportMetric(float64(normalizedBytes)/float64(asset.RowCount), "hnsw_search_pack_normalized_vectors_B/row")
		b.ReportMetric(float64(adjacencyOffsetsBytes+adjacencyNeighborsBytes)/float64(asset.RowCount), "hnsw_search_pack_adjacency_B/row")
		b.ReportMetric(float64(rowRefBytes+documentIDOffsetsBytes+documentIDBytes)/float64(asset.RowCount), "hnsw_search_pack_identity_B/row")
	}
	return asset
}

func BenchmarkColumnHNSWSearchPackPreparedOpen2314(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	params := columnHNSWSearchPackRebuildBenchParamsFromEnv2313(b)
	d, col, def := openColumnHNSWSearchPackRebuildBenchCollection2313(b, docs, dims, params)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	graph, _ := loadAndScanColumnGraphRebuildRowsV2A(b, d, "docs", def)
	records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(b, d, "docs")
	state := columnVectorIndexStateFromRecords1987(b, records, def)
	b.ReportAllocs()
	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	var last vectorIndexSearchRouteStats
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		view, status, openNanos, err := col.openColumnHNSWSearchPackPreparedViewForReader("docs", *cfg, def, graph, state)
		if err != nil {
			b.Fatalf("openColumnHNSWSearchPackPreparedViewForReader: %v", err)
		}
		if status != columnHNSWSearchPackPreparedStatusDirect && status != columnHNSWSearchPackPreparedStatusHeap {
			b.Fatalf("prepared status=%s want direct or heap", status)
		}
		last = view.routeStats(status, openNanos)
		if err := view.Close(); err != nil {
			b.Fatalf("Close prepared view: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(last.HNSWSearchPackOpenNanos), "hnsw_search_pack_open_ns")
	b.ReportMetric(float64(last.HNSWSearchPackMappedBytes), "hnsw_search_pack_mapped_B")
	b.ReportMetric(float64(last.HNSWSearchPackHeapCopyBytes), "hnsw_search_pack_heap_copy_B")
	b.ReportMetric(float64(last.HNSWSearchPackMmapDirect), "hnsw_search_pack_mmap_direct/open")
	b.ReportMetric(float64(last.HNSWSearchPackHeapCopy), "hnsw_search_pack_heap_copy/open")
}

func BenchmarkColumnHNSWSearchPackDecodeValidate2312(b *testing.B) {
	raw := testColumnHNSWSearchPackRaw2312(b)
	base := testColumnHNSWSearchPackInput2312().BaseIdentity
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := decodeColumnHNSWSearchPack(raw, columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: base}); err != nil {
			b.Fatalf("decodeColumnHNSWSearchPack: %v", err)
		}
	}
}

func testColumnHNSWSearchPackPreparedViewFromBytes2314(tb testing.TB, raw []byte, source mappedresource.Source, base columnHNSWSearchPackBaseIdentity) (*columnHNSWSearchPackPreparedView, *mappedresource.Handle) {
	tb.Helper()
	view, handle, err := testColumnHNSWSearchPackPreparedViewFromBytesAllowErr2314(raw, source, base)
	if err != nil {
		tb.Fatalf("newColumnHNSWSearchPackPreparedViewFromHandle: %v", err)
	}
	return view, handle
}

func testColumnHNSWSearchPackPreparedViewFromBytesAllowErr2314(raw []byte, source mappedresource.Source, base columnHNSWSearchPackBaseIdentity) (*columnHNSWSearchPackPreparedView, *mappedresource.Handle, error) {
	manager := mappedresource.NewManager()
	key := testColumnHNSWSearchPackMappedResourceKey2314(0, int64(len(raw)), page.Checksum(raw))
	handle, err := manager.AcquireBytes(key, testColumnHNSWSearchPackScope2314(), source, raw, mappedresource.AcquireOptions{Reason: "prepared test", ValidationMode: mappedresource.ValidationVerify})
	if err != nil {
		return nil, nil, err
	}
	view, err := newColumnHNSWSearchPackPreparedViewFromHandle(manager, handle, columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: base})
	if err != nil {
		_ = handle.Release()
		return nil, nil, err
	}
	return view, handle, nil
}

func testColumnHNSWSearchPackMappedResourceKey2314(offset, length int64, checksum uint32) mappedresource.Key {
	return mappedresource.Key{
		Class:      mappedresource.ClassTypedColumnAsset,
		Namespace:  "docs_column_assets",
		Kind:       string(ColumnAssetKindTCS1HNSWSearchPack),
		Generation: 11,
		PartID:     77,
		FileID:     3,
		Offset:     offset,
		Length:     length,
		Checksum:   uint64(checksum),
		Version:    columnHNSWSearchPackVersionV1,
		Encoding:   columnVectorIndexStateEncodingHNSWSearchPackV1,
		Section: mappedresource.Section{
			Kind:     columnVectorIndexStateAssetRoleHNSWSearchPack,
			Category: string(ColumnAssetKindTCS1HNSWSearchPack),
			Name:     columnVectorIndexStateHNSWSearchPackAssetID,
		},
	}
}

func testColumnHNSWSearchPackScope2314() mappedresource.Scope {
	return mappedresource.Scope{Kind: mappedresource.ScopePreparedSearch, ID: "hnsw-search-pack-test", Namespace: "docs_column_assets", Collection: "docs", Generation: 11, Reason: "test"}
}

func testColumnHNSWSearchPackInput2312() columnHNSWSearchPackBuildInput {
	return columnHNSWSearchPackBuildInput{
		Rows:           3,
		Dimensions:     3,
		VectorStride:   4,
		M:              16,
		EfConstruction: 128,
		EfSearch:       64,
		EntryOrdinal:   0,
		MaxLayer:       1,
		BaseIdentity:   columnHNSWSearchPackBaseIdentity{ManifestGeneration: 11, ManifestChecksum: 0x123456789abcdef0, SchemaHash: 0x0fedcba987654321},
		NormalizedVectors: []float32{
			1, 0, 0, 0,
			0, 1, 0, 0,
			0, 0, 1, 0,
		},
		Levels: []uint16{1, 0, 0},
		AdjacencyLayers: []columnHNSWSearchPackLayerInput{
			{Offsets: []uint64{0, 2, 3, 4}, Neighbors: []uint32{1, 2, 0, 1}},
			{Offsets: []uint64{0, 1, 1, 1}, Neighbors: []uint32{1}},
		},
		RowRefGenerations:       []int64{11, 11, 10},
		RowRefPartIDs:           []int64{1, 1, 2},
		RowRefRowIndexes:        []int64{0, 1, 0},
		RowRefAppliedCommandLSN: []int64{101, 102, 103},
		DocumentIDOffsets:       []uint64{0, 5, 10, 15},
		DocumentIDBytes:         []byte("doc-adoc-bdoc-c"),
	}
}

func testColumnHNSWSearchPackAuxiliaryInput4106() columnHNSWSearchPackBuildInput {
	input := testColumnHNSWSearchPackInput2312()
	input.MembershipDigest[0] = 1
	input.AdjacencyLayers[0] = columnHNSWSearchPackLayerInput{Offsets: []uint64{0, 1, 2, 2}, Neighbors: []uint32{1, 0}}
	input.HasAuxiliaryNavigation = true
	input.AuxiliaryNavigation = columnHNSWSearchPackLayerInput{Offsets: []uint64{0, 1, 1, 2}, Neighbors: []uint32{2, 0}}
	return input
}

func testColumnHNSWSearchPackUpperSeedAnchorInput4114() columnHNSWSearchPackBuildInput {
	input := testColumnHNSWSearchPackInput2312()
	input.Rows = 4
	input.EntryOrdinal = 0
	input.MaxLayer = 1
	input.M = 2
	input.MembershipDigest[0] = 1
	input.NormalizedVectors = []float32{
		1, 0, 0, 0,
		0.9, 0.1, 0, 0,
		0.1, 0.9, 0, 0,
		0, 1, 0, 0,
	}
	input.Levels = []uint16{1, 1, 0, 0}
	input.AdjacencyLayers = []columnHNSWSearchPackLayerInput{
		{Offsets: []uint64{0, 1, 1, 2, 2}, Neighbors: []uint32{1, 3}},
		{Offsets: []uint64{0, 1, 1, 1, 1}, Neighbors: []uint32{1}},
	}
	input.HasAuxiliaryNavigation = true
	input.AuxiliaryNavigation = columnHNSWSearchPackLayerInput{Offsets: []uint64{0, 1, 2, 3, 3}, Neighbors: []uint32{2, 0, 0}}
	input.RowRefGenerations = []int64{11, 11, 11, 11}
	input.RowRefPartIDs = []int64{1, 1, 1, 1}
	input.RowRefRowIndexes = []int64{0, 1, 2, 3}
	input.RowRefAppliedCommandLSN = []int64{101, 102, 103, 104}
	input.DocumentIDOffsets = []uint64{0, 1, 2, 3, 4}
	input.DocumentIDBytes = []byte("abcd")
	return input
}

func testColumnHNSWSearchPackRaw2312(tb testing.TB) []byte {
	tb.Helper()
	raw, err := encodeColumnHNSWSearchPack(testColumnHNSWSearchPackInput2312())
	if err != nil {
		tb.Fatalf("encodeColumnHNSWSearchPack: %v", err)
	}
	return raw
}

func testColumnHNSWSearchPackPatchByte2312(raw []byte, off int, value byte) []byte {
	out := append([]byte(nil), raw...)
	out[off] = value
	return out
}

func testColumnHNSWSearchPackPatchU16Header2312(raw []byte, off int, value uint16) []byte {
	out := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint16(out[off:], value)
	return out
}

func testColumnHNSWSearchPackPatchU32Header2312(raw []byte, off int, value uint32) []byte {
	out := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint32(out[off:], value)
	return out
}

func testColumnHNSWSearchPackPatchU64Header2312(raw []byte, off int, value uint64) []byte {
	out := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint64(out[off:], value)
	return out
}

func testColumnHNSWSearchPackMutateSectionEntry2312(raw []byte, kind columnHNSWSearchPackSectionKind, index uint16, mutate func(*columnHNSWSearchPackSection)) []byte {
	out := append([]byte(nil), raw...)
	entryOff, section := testColumnHNSWSearchPackFindSectionEntry2312(out, kind, index)
	mutate(&section)
	encodeColumnHNSWSearchPackSectionEntry(out[entryOff:], section)
	testColumnHNSWSearchPackRechecksumDirectory2312(out)
	return out
}

func testColumnHNSWSearchPackMutateSectionPayload2312(raw []byte, kind columnHNSWSearchPackSectionKind, index uint16, mutate func([]byte)) []byte {
	out := append([]byte(nil), raw...)
	entryOff, section := testColumnHNSWSearchPackFindSectionEntry2312(out, kind, index)
	payload := out[section.Offset : section.Offset+section.Length]
	mutate(payload)
	section.Checksum = page.Checksum(payload)
	encodeColumnHNSWSearchPackSectionEntry(out[entryOff:], section)
	testColumnHNSWSearchPackRechecksumDirectory2312(out)
	return out
}

func testColumnHNSWSearchPackFindSectionEntry2312(raw []byte, kind columnHNSWSearchPackSectionKind, index uint16) (int, columnHNSWSearchPackSection) {
	dirOff := int(binary.LittleEndian.Uint64(raw[columnHNSWSearchPackHeaderDirectoryOffsetOffset:]))
	sectionCount := int(binary.LittleEndian.Uint32(raw[columnHNSWSearchPackHeaderSectionCountOffset:]))
	for i := 0; i < sectionCount; i++ {
		entryOff := dirOff + i*columnHNSWSearchPackSectionEntrySize
		section, err := decodeColumnHNSWSearchPackSectionEntry(raw[entryOff:])
		if err != nil {
			panic(err)
		}
		if section.Kind == kind && section.Index == index {
			return entryOff, section
		}
	}
	panic("section not found")
}

func testColumnHNSWSearchPackRechecksumDirectory2312(raw []byte) {
	dirOff := int(binary.LittleEndian.Uint64(raw[columnHNSWSearchPackHeaderDirectoryOffsetOffset:]))
	dirLen := int(binary.LittleEndian.Uint64(raw[columnHNSWSearchPackHeaderDirectoryLengthOffset:]))
	binary.LittleEndian.PutUint32(raw[columnHNSWSearchPackHeaderDirectoryChecksumOffset:], page.Checksum(raw[dirOff:dirOff+dirLen]))
}
