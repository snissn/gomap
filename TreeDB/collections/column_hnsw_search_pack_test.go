package collections

import (
	"encoding/binary"
	"reflect"
	"strings"
	"testing"

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
			raw:  testColumnHNSWSearchPackPatchU16Header2312(raw, columnHNSWSearchPackHeaderVersionOffset, columnHNSWSearchPackVersionV1+1),
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
