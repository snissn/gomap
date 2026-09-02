package collections

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/page"
)

// hnsw_search_pack_v1 is the durable, vector-index-owned serving artifact for
// the future no-document HNSW fast path. The pack is derived state: raw typed
// collection vectors remain authoritative, while the pack stores normalized
// float32 vectors plus HNSW graph/identity sections in one mmap-able object.
//
// Binary contract:
//   - fixed little-endian header with magic/version, graph counts, row/dim/
//     stride counts, cosine-normalized-dot metric, and base manifest identity;
//   - version 2 extends that header with a required membership digest for
//     partition-local packs; ordinary column-index packs remain version 1;
//   - fixed-width section directory immediately after the header;
//   - section payloads use absolute offsets from the start of the pack;
//   - every section declares alignment, element count, byte length, and CRC32;
//   - all allocation-sized counts are capped before slices are allocated;
//   - corrupt, truncated, mismatched, overlapping, or stale packs fail closed.
//
// Rebuild publication is handled by the writer path. Mmap resource ownership
// and production SearchWithBuffer routing remain deferred to later reader/search
// promotion issues in the #2309 stack.
const (
	columnHNSWSearchPackVersionV1 = uint16(1)
	columnHNSWSearchPackVersionV2 = uint16(2)
	columnHNSWSearchPackVersionV3 = uint16(3)

	columnHNSWSearchPackHeaderSize       = 144
	columnHNSWSearchPackHeaderSizeV2     = 176
	columnHNSWSearchPackSectionEntrySize = 48
	columnHNSWSearchPackDirectoryOffset  = columnHNSWSearchPackHeaderSize

	columnHNSWSearchPackHeaderVersionOffset           = 8
	columnHNSWSearchPackHeaderHeaderSizeOffset        = 10
	columnHNSWSearchPackHeaderSectionEntrySizeOffset  = 12
	columnHNSWSearchPackHeaderFlagsOffset             = 14
	columnHNSWSearchPackHeaderTotalLengthOffset       = 16
	columnHNSWSearchPackHeaderSectionCountOffset      = 24
	columnHNSWSearchPackHeaderRowsOffset              = 32
	columnHNSWSearchPackHeaderDimensionsOffset        = 40
	columnHNSWSearchPackHeaderVectorStrideOffset      = 44
	columnHNSWSearchPackHeaderMetricOffset            = 48
	columnHNSWSearchPackHeaderEncodingOffset          = 50
	columnHNSWSearchPackHeaderMaxLayerOffset          = 52
	columnHNSWSearchPackHeaderAdjacencyLayerCount     = 56
	columnHNSWSearchPackHeaderMOffset                 = 60
	columnHNSWSearchPackHeaderEfConstructionOffset    = 64
	columnHNSWSearchPackHeaderEfSearchOffset          = 68
	columnHNSWSearchPackHeaderEntryOrdinalOffset      = 72
	columnHNSWSearchPackHeaderBaseGenerationOffset    = 80
	columnHNSWSearchPackHeaderBaseChecksumOffset      = 88
	columnHNSWSearchPackHeaderBaseSchemaHashOffset    = 96
	columnHNSWSearchPackHeaderDirectoryOffsetOffset   = 104
	columnHNSWSearchPackHeaderDirectoryLengthOffset   = 112
	columnHNSWSearchPackHeaderDataOffsetOffset        = 120
	columnHNSWSearchPackHeaderDataLengthOffset        = 128
	columnHNSWSearchPackHeaderDirectoryChecksumOffset = 136
	columnHNSWSearchPackHeaderMembershipDigestOffset  = 144

	columnHNSWSearchPackEntryKindOffset      = 0
	columnHNSWSearchPackEntryIndexOffset     = 2
	columnHNSWSearchPackEntryAlignmentOffset = 4
	columnHNSWSearchPackEntryFlagsOffset     = 6
	columnHNSWSearchPackEntrySectionOffset   = 8
	columnHNSWSearchPackEntryLengthOffset    = 16
	columnHNSWSearchPackEntryCountOffset     = 24
	columnHNSWSearchPackEntryChecksumOffset  = 32

	columnHNSWSearchPackMetricCosineNormalizedDot = uint16(1)
	columnHNSWSearchPackEncodingFloat32           = uint16(1)

	columnHNSWSearchPackNoEntryOrdinal = ^uint64(0)
	columnHNSWSearchPackNoMaxLayer     = ^uint32(0)

	columnHNSWSearchPackMaxRowsDefault          = uint64(1 << 26)
	columnHNSWSearchPackMaxDimensionsDefault    = uint32(1 << 15)
	columnHNSWSearchPackMaxVectorStrideDefault  = uint32(1 << 15)
	columnHNSWSearchPackMaxLayersDefault        = uint32(64)
	columnHNSWSearchPackMaxNeighborsDefault     = uint64(1 << 30)
	columnHNSWSearchPackMaxDocumentBytesDefault = uint64(1 << 30)

	columnHNSWSearchPackAlignment              = uint32(8)
	columnHNSWSearchPackVectorSectionAlignment = uint32(16)
)

var columnHNSWSearchPackMagic = [...]byte{'T', 'H', 'S', 'P', 'A', 'C', 'K', '1'}

type columnHNSWSearchPackSectionKind uint16

const (
	columnHNSWSearchPackSectionNormalizedVectors  columnHNSWSearchPackSectionKind = 1
	columnHNSWSearchPackSectionLevels             columnHNSWSearchPackSectionKind = 2
	columnHNSWSearchPackSectionAdjacencyOffsets   columnHNSWSearchPackSectionKind = 3
	columnHNSWSearchPackSectionAdjacencyNeighbors columnHNSWSearchPackSectionKind = 4
	columnHNSWSearchPackSectionRowRefGeneration   columnHNSWSearchPackSectionKind = 5
	columnHNSWSearchPackSectionRowRefPartID       columnHNSWSearchPackSectionKind = 6
	columnHNSWSearchPackSectionRowRefRowIndex     columnHNSWSearchPackSectionKind = 7
	columnHNSWSearchPackSectionRowRefAppliedLSN   columnHNSWSearchPackSectionKind = 8
	columnHNSWSearchPackSectionDocumentIDOffsets  columnHNSWSearchPackSectionKind = 9
	columnHNSWSearchPackSectionDocumentIDBytes    columnHNSWSearchPackSectionKind = 10
	columnHNSWSearchPackSectionAuxiliaryOffsets   columnHNSWSearchPackSectionKind = 11
	columnHNSWSearchPackSectionAuxiliaryNeighbors columnHNSWSearchPackSectionKind = 12
)

func (k columnHNSWSearchPackSectionKind) String() string {
	switch k {
	case columnHNSWSearchPackSectionNormalizedVectors:
		return "normalized_vectors"
	case columnHNSWSearchPackSectionLevels:
		return "levels"
	case columnHNSWSearchPackSectionAdjacencyOffsets:
		return "adjacency_offsets"
	case columnHNSWSearchPackSectionAdjacencyNeighbors:
		return "adjacency_neighbors"
	case columnHNSWSearchPackSectionRowRefGeneration:
		return "row_ref_generation"
	case columnHNSWSearchPackSectionRowRefPartID:
		return "row_ref_part_id"
	case columnHNSWSearchPackSectionRowRefRowIndex:
		return "row_ref_row_index"
	case columnHNSWSearchPackSectionRowRefAppliedLSN:
		return "row_ref_applied_command_lsn"
	case columnHNSWSearchPackSectionDocumentIDOffsets:
		return "document_id_offsets"
	case columnHNSWSearchPackSectionDocumentIDBytes:
		return "document_id_bytes"
	case columnHNSWSearchPackSectionAuxiliaryOffsets:
		return "auxiliary_offsets"
	case columnHNSWSearchPackSectionAuxiliaryNeighbors:
		return "auxiliary_neighbors"
	default:
		return fmt.Sprintf("unknown(%d)", uint16(k))
	}
}

type columnHNSWSearchPackBaseIdentity struct {
	ManifestGeneration uint64
	ManifestChecksum   uint64
	SchemaHash         uint64
}

type columnHNSWSearchPackBuildInput struct {
	Rows           int
	Dimensions     int
	VectorStride   int
	M              int
	EfConstruction int
	EfSearch       int
	EntryOrdinal   int
	MaxLayer       int
	BaseIdentity   columnHNSWSearchPackBaseIdentity
	// MembershipDigest is optional for general column-index packs. Partition
	// packs require it and are emitted as wire version 2 so the exact canonical
	// home/overlap set is part of the persisted header identity.
	MembershipDigest [sha256.Size]byte
	// HasAuxiliaryNavigation requires wire version 3 even when its CSR is
	// empty, so connected partition packs cannot silently lose the channel.
	HasAuxiliaryNavigation bool
	AuxiliaryNavigation    columnHNSWSearchPackLayerInput

	NormalizedVectors       []float32
	Levels                  []uint16
	AdjacencyLayers         []columnHNSWSearchPackLayerInput
	RowRefGenerations       []int64
	RowRefPartIDs           []int64
	RowRefRowIndexes        []int64
	RowRefAppliedCommandLSN []int64
	DocumentIDOffsets       []uint64
	DocumentIDBytes         []byte
}

type columnHNSWSearchPackLayerInput struct {
	Offsets   []uint64
	Neighbors []uint32
}

type columnHNSWSearchPackDecodeOptions struct {
	ExpectedBaseIdentity     columnHNSWSearchPackBaseIdentity
	ExpectedMembershipDigest [sha256.Size]byte
	MaxRows                  uint64
	MaxDimensions            uint32
	MaxVectorStride          uint32
	MaxLayers                uint32
	MaxNeighbors             uint64
	MaxDocumentIDBytes       uint64
}

type columnHNSWSearchPack struct {
	Header              columnHNSWSearchPackHeader
	Sections            []columnHNSWSearchPackSection
	NormalizedVectors   []float32
	Levels              []uint16
	AdjacencyLayers     []columnHNSWSearchPackLayer
	AuxiliaryNavigation columnHNSWSearchPackLayer
	RowRefGenerations   []int64
	RowRefPartIDs       []int64
	RowRefRowIndexes    []int64
	RowRefAppliedLSNs   []int64
	DocumentIDOffsets   []uint64
	DocumentIDBytes     []byte
}

type columnHNSWSearchPackHeader struct {
	Rows                   int
	Dimensions             int
	VectorStride           int
	M                      int
	EfConstruction         int
	EfSearch               int
	EntryOrdinal           int
	MaxLayer               int
	AdjacencyLayerCount    int
	BaseManifestGeneration uint64
	BaseManifestChecksum   uint64
	BaseSchemaHash         uint64
	MembershipDigest       [sha256.Size]byte
	HasAuxiliaryNavigation bool
	TotalLength            uint64
	DataOffset             uint64
	DataLength             uint64
}

type columnHNSWSearchPackLayer struct {
	Offsets   []uint64
	Neighbors []uint32
}

type columnHNSWSearchPackSection struct {
	Kind      columnHNSWSearchPackSectionKind
	Index     uint16
	Alignment uint32
	Offset    uint64
	Length    uint64
	Count     uint64
	Checksum  uint32
}

func encodeColumnHNSWSearchPack(input columnHNSWSearchPackBuildInput) ([]byte, error) {
	if err := validateColumnHNSWSearchPackBuildInput(input); err != nil {
		return nil, err
	}
	sectionCount := 8 + 2*len(input.AdjacencyLayers)
	if input.HasAuxiliaryNavigation {
		sectionCount += 2
	}
	directoryLength := sectionCount * columnHNSWSearchPackSectionEntrySize
	version := columnHNSWSearchPackVersionV1
	headerSize := columnHNSWSearchPackHeaderSize
	if input.MembershipDigest != ([sha256.Size]byte{}) {
		version = columnHNSWSearchPackVersionV2
		headerSize = columnHNSWSearchPackHeaderSizeV2
	}
	if input.HasAuxiliaryNavigation {
		version = columnHNSWSearchPackVersionV3
	}
	directoryOffset := headerSize
	dataOffset, ok := alignColumnHNSWSearchPackUint64(uint64(headerSize+directoryLength), uint64(columnHNSWSearchPackAlignment))
	if !ok || dataOffset > uint64(math.MaxInt) {
		return nil, errors.New("collections: hnsw search pack directory length overflow")
	}
	raw := make([]byte, int(dataOffset))
	sections := make([]columnHNSWSearchPackSection, 0, sectionCount)
	appendSection := func(kind columnHNSWSearchPackSectionKind, index uint16, alignment uint32, payload []byte, count uint64) error {
		offset, ok := alignColumnHNSWSearchPackUint64(uint64(len(raw)), uint64(alignment))
		if !ok || offset > uint64(math.MaxInt) {
			return fmt.Errorf("collections: hnsw search pack section %s offset overflow", kind)
		}
		if pad := int(offset) - len(raw); pad > 0 {
			raw = append(raw, make([]byte, pad)...)
		}
		if len(payload) > math.MaxInt-len(raw) {
			return fmt.Errorf("collections: hnsw search pack section %s length overflow", kind)
		}
		section := columnHNSWSearchPackSection{
			Kind:      kind,
			Index:     index,
			Alignment: alignment,
			Offset:    offset,
			Length:    uint64(len(payload)),
			Count:     count,
			Checksum:  page.Checksum(payload),
		}
		raw = append(raw, payload...)
		sections = append(sections, section)
		return nil
	}
	if err := appendSection(columnHNSWSearchPackSectionNormalizedVectors, 0, columnHNSWSearchPackVectorSectionAlignment, encodeFloat32SliceLE(input.NormalizedVectors), uint64(len(input.NormalizedVectors))); err != nil {
		return nil, err
	}
	if err := appendSection(columnHNSWSearchPackSectionLevels, 0, columnHNSWSearchPackAlignment, encodeUint16SliceLE(input.Levels), uint64(len(input.Levels))); err != nil {
		return nil, err
	}
	for layer, adjacency := range input.AdjacencyLayers {
		if err := appendSection(columnHNSWSearchPackSectionAdjacencyOffsets, uint16(layer), columnHNSWSearchPackAlignment, encodeUint64SliceLE(adjacency.Offsets), uint64(len(adjacency.Offsets))); err != nil {
			return nil, err
		}
		if err := appendSection(columnHNSWSearchPackSectionAdjacencyNeighbors, uint16(layer), columnHNSWSearchPackAlignment, encodeUint32SliceLE(adjacency.Neighbors), uint64(len(adjacency.Neighbors))); err != nil {
			return nil, err
		}
	}
	if input.HasAuxiliaryNavigation {
		if err := appendSection(columnHNSWSearchPackSectionAuxiliaryOffsets, 0, columnHNSWSearchPackAlignment, encodeUint64SliceLE(input.AuxiliaryNavigation.Offsets), uint64(len(input.AuxiliaryNavigation.Offsets))); err != nil {
			return nil, err
		}
		if err := appendSection(columnHNSWSearchPackSectionAuxiliaryNeighbors, 0, columnHNSWSearchPackAlignment, encodeUint32SliceLE(input.AuxiliaryNavigation.Neighbors), uint64(len(input.AuxiliaryNavigation.Neighbors))); err != nil {
			return nil, err
		}
	}
	if err := appendSection(columnHNSWSearchPackSectionRowRefGeneration, 0, columnHNSWSearchPackAlignment, encodeInt64SliceLE(input.RowRefGenerations), uint64(len(input.RowRefGenerations))); err != nil {
		return nil, err
	}
	if err := appendSection(columnHNSWSearchPackSectionRowRefPartID, 0, columnHNSWSearchPackAlignment, encodeInt64SliceLE(input.RowRefPartIDs), uint64(len(input.RowRefPartIDs))); err != nil {
		return nil, err
	}
	if err := appendSection(columnHNSWSearchPackSectionRowRefRowIndex, 0, columnHNSWSearchPackAlignment, encodeInt64SliceLE(input.RowRefRowIndexes), uint64(len(input.RowRefRowIndexes))); err != nil {
		return nil, err
	}
	if err := appendSection(columnHNSWSearchPackSectionRowRefAppliedLSN, 0, columnHNSWSearchPackAlignment, encodeInt64SliceLE(input.RowRefAppliedCommandLSN), uint64(len(input.RowRefAppliedCommandLSN))); err != nil {
		return nil, err
	}
	if err := appendSection(columnHNSWSearchPackSectionDocumentIDOffsets, 0, columnHNSWSearchPackAlignment, encodeUint64SliceLE(input.DocumentIDOffsets), uint64(len(input.DocumentIDOffsets))); err != nil {
		return nil, err
	}
	if err := appendSection(columnHNSWSearchPackSectionDocumentIDBytes, 0, columnHNSWSearchPackAlignment, input.DocumentIDBytes, uint64(len(input.DocumentIDBytes))); err != nil {
		return nil, err
	}

	return finishColumnHNSWSearchPack(raw, input, sections, version, headerSize, directoryOffset, directoryLength, dataOffset), nil
}

func finishColumnHNSWSearchPack(raw []byte, input columnHNSWSearchPackBuildInput, sections []columnHNSWSearchPackSection, version uint16, headerSize, directoryOffset, directoryLength int, dataOffset uint64) []byte {
	directory := raw[directoryOffset : directoryOffset+directoryLength]
	for i, section := range sections {
		encodeColumnHNSWSearchPackSectionEntry(directory[i*columnHNSWSearchPackSectionEntrySize:], section)
	}
	copy(raw[:8], columnHNSWSearchPackMagic[:])
	putHNSWPackU16(raw, columnHNSWSearchPackHeaderVersionOffset, version)
	putHNSWPackU16(raw, columnHNSWSearchPackHeaderHeaderSizeOffset, uint16(headerSize))
	putHNSWPackU16(raw, columnHNSWSearchPackHeaderSectionEntrySizeOffset, columnHNSWSearchPackSectionEntrySize)
	putHNSWPackU16(raw, columnHNSWSearchPackHeaderFlagsOffset, 0)
	putHNSWPackU64(raw, columnHNSWSearchPackHeaderTotalLengthOffset, uint64(len(raw)))
	putHNSWPackU32(raw, columnHNSWSearchPackHeaderSectionCountOffset, uint32(len(sections)))
	putHNSWPackU64(raw, columnHNSWSearchPackHeaderRowsOffset, uint64(input.Rows))
	putHNSWPackU32(raw, columnHNSWSearchPackHeaderDimensionsOffset, uint32(input.Dimensions))
	putHNSWPackU32(raw, columnHNSWSearchPackHeaderVectorStrideOffset, uint32(input.VectorStride))
	putHNSWPackU16(raw, columnHNSWSearchPackHeaderMetricOffset, columnHNSWSearchPackMetricCosineNormalizedDot)
	putHNSWPackU16(raw, columnHNSWSearchPackHeaderEncodingOffset, columnHNSWSearchPackEncodingFloat32)
	maxLayer := uint32(columnHNSWSearchPackNoMaxLayer)
	if input.MaxLayer >= 0 {
		maxLayer = uint32(input.MaxLayer)
	}
	putHNSWPackU32(raw, columnHNSWSearchPackHeaderMaxLayerOffset, maxLayer)
	putHNSWPackU32(raw, columnHNSWSearchPackHeaderAdjacencyLayerCount, uint32(len(input.AdjacencyLayers)))
	putHNSWPackU32(raw, columnHNSWSearchPackHeaderMOffset, uint32(input.M))
	putHNSWPackU32(raw, columnHNSWSearchPackHeaderEfConstructionOffset, uint32(input.EfConstruction))
	putHNSWPackU32(raw, columnHNSWSearchPackHeaderEfSearchOffset, uint32(input.EfSearch))
	entryOrdinal := uint64(input.EntryOrdinal)
	if input.EntryOrdinal < 0 {
		entryOrdinal = columnHNSWSearchPackNoEntryOrdinal
	}
	putHNSWPackU64(raw, columnHNSWSearchPackHeaderEntryOrdinalOffset, entryOrdinal)
	putHNSWPackU64(raw, columnHNSWSearchPackHeaderBaseGenerationOffset, input.BaseIdentity.ManifestGeneration)
	putHNSWPackU64(raw, columnHNSWSearchPackHeaderBaseChecksumOffset, input.BaseIdentity.ManifestChecksum)
	putHNSWPackU64(raw, columnHNSWSearchPackHeaderBaseSchemaHashOffset, input.BaseIdentity.SchemaHash)
	putHNSWPackU64(raw, columnHNSWSearchPackHeaderDirectoryOffsetOffset, uint64(directoryOffset))
	putHNSWPackU64(raw, columnHNSWSearchPackHeaderDirectoryLengthOffset, uint64(directoryLength))
	putHNSWPackU64(raw, columnHNSWSearchPackHeaderDataOffsetOffset, dataOffset)
	putHNSWPackU64(raw, columnHNSWSearchPackHeaderDataLengthOffset, uint64(len(raw))-dataOffset)
	putHNSWPackU32(raw, columnHNSWSearchPackHeaderDirectoryChecksumOffset, page.Checksum(directory))
	if version == columnHNSWSearchPackVersionV2 || version == columnHNSWSearchPackVersionV3 {
		copy(raw[columnHNSWSearchPackHeaderMembershipDigestOffset:], input.MembershipDigest[:])
	}
	return raw
}

func encodeColumnHNSWSearchPackRows(input columnHNSWSearchPackBuildInput, rows []columnVectorGraphAssetRow) ([]byte, error) {
	if len(rows) != input.Rows {
		return nil, fmt.Errorf("collections: hnsw search pack vector rows=%d want %d", len(rows), input.Rows)
	}
	if len(input.NormalizedVectors) != 0 {
		return nil, errors.New("collections: hnsw search pack row encoder requires no materialized vectors")
	}
	if err := validateColumnHNSWSearchPackBuildInputWithoutVectors(input); err != nil {
		return nil, err
	}

	type plannedSection struct {
		section columnHNSWSearchPackSection
		fill    func([]byte) error
	}
	sectionCount := 8 + 2*len(input.AdjacencyLayers)
	if input.HasAuxiliaryNavigation {
		sectionCount += 2
	}
	directoryLength := sectionCount * columnHNSWSearchPackSectionEntrySize
	version := columnHNSWSearchPackVersionV1
	headerSize := columnHNSWSearchPackHeaderSize
	if input.MembershipDigest != ([sha256.Size]byte{}) {
		version = columnHNSWSearchPackVersionV2
		headerSize = columnHNSWSearchPackHeaderSizeV2
	}
	if input.HasAuxiliaryNavigation {
		version = columnHNSWSearchPackVersionV3
	}
	directoryOffset := headerSize
	dataOffset, ok := alignColumnHNSWSearchPackUint64(uint64(headerSize+directoryLength), uint64(columnHNSWSearchPackAlignment))
	if !ok || dataOffset > uint64(math.MaxInt) {
		return nil, errors.New("collections: hnsw search pack directory length overflow")
	}
	cursor := dataOffset
	plans := make([]plannedSection, 0, sectionCount)
	plan := func(kind columnHNSWSearchPackSectionKind, index uint16, alignment uint32, count, width int, fill func([]byte) error) error {
		if count < 0 || (count != 0 && width > math.MaxInt/count) {
			return fmt.Errorf("collections: hnsw search pack section %s length overflow", kind)
		}
		offset, ok := alignColumnHNSWSearchPackUint64(cursor, uint64(alignment))
		length := uint64(count * width)
		if !ok || offset > uint64(math.MaxInt) || length > uint64(math.MaxInt)-offset {
			return fmt.Errorf("collections: hnsw search pack section %s length overflow", kind)
		}
		plans = append(plans, plannedSection{section: columnHNSWSearchPackSection{Kind: kind, Index: index, Alignment: alignment, Offset: offset, Length: length, Count: uint64(count)}, fill: fill})
		cursor = offset + length
		return nil
	}
	vectorCount := input.Rows * input.VectorStride
	if err := plan(columnHNSWSearchPackSectionNormalizedVectors, 0, columnHNSWSearchPackVectorSectionAlignment, vectorCount, 4, func(dst []byte) error {
		for ordinal, row := range rows {
			if len(row.Vector) != input.Dimensions {
				return fmt.Errorf("collections: hnsw search pack row[%d] vector dims=%d want %d", ordinal, len(row.Vector), input.Dimensions)
			}
			invNorm := row.InvNorm
			if invNorm <= 0 || math.IsNaN(float64(invNorm)) || math.IsInf(float64(invNorm), 0) {
				var err error
				invNorm, err = columnVectorGraphInvNorm(row.Vector)
				if err != nil {
					return fmt.Errorf("collections: hnsw search pack row[%d] inverse norm: %w", ordinal, err)
				}
			}
			base := ordinal * input.VectorStride * 4
			for dim, value := range row.Vector {
				normalized := value * invNorm
				if math.IsNaN(float64(normalized)) || math.IsInf(float64(normalized), 0) {
					return fmt.Errorf("collections: hnsw search pack row[%d] normalized vector[%d] is not finite", ordinal, dim)
				}
				binary.LittleEndian.PutUint32(dst[base+dim*4:], math.Float32bits(normalized))
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	planUint16 := func(kind columnHNSWSearchPackSectionKind, index uint16, values []uint16) error {
		return plan(kind, index, columnHNSWSearchPackAlignment, len(values), 2, func(dst []byte) error {
			for i, value := range values {
				binary.LittleEndian.PutUint16(dst[i*2:], value)
			}
			return nil
		})
	}
	planUint32 := func(kind columnHNSWSearchPackSectionKind, index uint16, values []uint32) error {
		return plan(kind, index, columnHNSWSearchPackAlignment, len(values), 4, func(dst []byte) error {
			for i, value := range values {
				binary.LittleEndian.PutUint32(dst[i*4:], value)
			}
			return nil
		})
	}
	planUint64 := func(kind columnHNSWSearchPackSectionKind, index uint16, values []uint64) error {
		return plan(kind, index, columnHNSWSearchPackAlignment, len(values), 8, func(dst []byte) error {
			for i, value := range values {
				binary.LittleEndian.PutUint64(dst[i*8:], value)
			}
			return nil
		})
	}
	planInt64 := func(kind columnHNSWSearchPackSectionKind, values []int64) error {
		return plan(kind, 0, columnHNSWSearchPackAlignment, len(values), 8, func(dst []byte) error {
			for i, value := range values {
				binary.LittleEndian.PutUint64(dst[i*8:], uint64(value))
			}
			return nil
		})
	}
	if err := planUint16(columnHNSWSearchPackSectionLevels, 0, input.Levels); err != nil {
		return nil, err
	}
	for layer, adjacency := range input.AdjacencyLayers {
		if err := planUint64(columnHNSWSearchPackSectionAdjacencyOffsets, uint16(layer), adjacency.Offsets); err != nil {
			return nil, err
		}
		if err := planUint32(columnHNSWSearchPackSectionAdjacencyNeighbors, uint16(layer), adjacency.Neighbors); err != nil {
			return nil, err
		}
	}
	if input.HasAuxiliaryNavigation {
		if err := planUint64(columnHNSWSearchPackSectionAuxiliaryOffsets, 0, input.AuxiliaryNavigation.Offsets); err != nil {
			return nil, err
		}
		if err := planUint32(columnHNSWSearchPackSectionAuxiliaryNeighbors, 0, input.AuxiliaryNavigation.Neighbors); err != nil {
			return nil, err
		}
	}
	if err := planInt64(columnHNSWSearchPackSectionRowRefGeneration, input.RowRefGenerations); err != nil {
		return nil, err
	}
	if err := planInt64(columnHNSWSearchPackSectionRowRefPartID, input.RowRefPartIDs); err != nil {
		return nil, err
	}
	if err := planInt64(columnHNSWSearchPackSectionRowRefRowIndex, input.RowRefRowIndexes); err != nil {
		return nil, err
	}
	if err := planInt64(columnHNSWSearchPackSectionRowRefAppliedLSN, input.RowRefAppliedCommandLSN); err != nil {
		return nil, err
	}
	if err := planUint64(columnHNSWSearchPackSectionDocumentIDOffsets, 0, input.DocumentIDOffsets); err != nil {
		return nil, err
	}
	if err := plan(columnHNSWSearchPackSectionDocumentIDBytes, 0, columnHNSWSearchPackAlignment, len(input.DocumentIDBytes), 1, func(dst []byte) error { copy(dst, input.DocumentIDBytes); return nil }); err != nil {
		return nil, err
	}

	raw := make([]byte, int(cursor))
	sections := make([]columnHNSWSearchPackSection, len(plans))
	for i := range plans {
		payload := raw[plans[i].section.Offset : plans[i].section.Offset+plans[i].section.Length]
		if err := plans[i].fill(payload); err != nil {
			return nil, err
		}
		plans[i].section.Checksum = page.Checksum(payload)
		sections[i] = plans[i].section
	}
	return finishColumnHNSWSearchPack(raw, input, sections, version, headerSize, directoryOffset, directoryLength, dataOffset), nil
}

func writeColumnHNSWSearchPackRowsWithBackpatch(w io.Writer, backpatch func([]byte) error, input columnHNSWSearchPackBuildInput, rows []columnVectorGraphAssetRow) (int64, error) {
	if len(rows) != input.Rows || len(input.NormalizedVectors) != 0 {
		return 0, errors.New("collections: hnsw search pack streamed row input is invalid")
	}
	plan, err := planColumnHNSWSearchPackStream(input)
	if err != nil {
		return 0, err
	}
	if backpatch == nil {
		return 0, errors.New("collections: hnsw search pack streamed writer requires backpatch")
	}
	sections := plan.sections
	dataOffset := plan.dataOffset
	cursor := plan.totalLength
	version := plan.version
	headerSize := plan.headerSize
	directoryLength := plan.directoryLength
	scratch := make([]byte, 1<<20)
	emitSection := func(dst io.Writer, s columnHNSWSearchPackSection) error {
		return writeColumnHNSWSearchPackStreamSection(dst, scratch, s, input, rows)
	}
	if _, err := writeColumnAssetSegmentZeroPadding(w, int(dataOffset)); err != nil {
		return 0, err
	}
	written := int64(dataOffset)
	for i := range sections {
		s := &sections[i]
		if gap := int64(s.Offset) - written; gap > 0 {
			if _, err := writeColumnAssetSegmentZeroPadding(w, int(gap)); err != nil {
				return written, err
			}
			written += gap
		}
		sum := columnHNSWSearchPackStreamChecksum{}
		if err := emitSection(io.MultiWriter(w, &sum), *s); err != nil {
			return written, err
		}
		s.Checksum = sum.sum
		written += int64(s.Length)
	}
	prefix := make([]byte, int(dataOffset))
	finishColumnHNSWSearchPack(prefix, input, sections, version, headerSize, headerSize, directoryLength, dataOffset)
	putHNSWPackU64(prefix, columnHNSWSearchPackHeaderTotalLengthOffset, cursor)
	putHNSWPackU64(prefix, columnHNSWSearchPackHeaderDataLengthOffset, cursor-dataOffset)
	if err := backpatch(prefix); err != nil {
		return written, err
	}
	return written, nil
}

type columnHNSWSearchPackStreamPlan struct {
	sections        []columnHNSWSearchPackSection
	version         uint16
	headerSize      int
	directoryLength int
	dataOffset      uint64
	totalLength     uint64
}

func planColumnHNSWSearchPackStream(input columnHNSWSearchPackBuildInput) (columnHNSWSearchPackStreamPlan, error) {
	if err := validateColumnHNSWSearchPackBuildInputWithoutVectors(input); err != nil {
		return columnHNSWSearchPackStreamPlan{}, err
	}
	sectionCount := 8 + 2*len(input.AdjacencyLayers)
	if input.HasAuxiliaryNavigation {
		sectionCount += 2
	}
	headerSize, version := columnHNSWSearchPackHeaderSize, columnHNSWSearchPackVersionV1
	if input.MembershipDigest != ([sha256.Size]byte{}) {
		headerSize, version = columnHNSWSearchPackHeaderSizeV2, columnHNSWSearchPackVersionV2
	}
	if input.HasAuxiliaryNavigation {
		version = columnHNSWSearchPackVersionV3
	}
	directoryLength := sectionCount * columnHNSWSearchPackSectionEntrySize
	dataOffset, ok := alignColumnHNSWSearchPackUint64(uint64(headerSize+directoryLength), uint64(columnHNSWSearchPackAlignment))
	if !ok || dataOffset > uint64(math.MaxInt) {
		return columnHNSWSearchPackStreamPlan{}, errors.New("collections: hnsw search pack directory length overflow")
	}
	sections := make([]columnHNSWSearchPackSection, 0, sectionCount)
	cursor := dataOffset
	add := func(kind columnHNSWSearchPackSectionKind, index uint16, alignment uint32, count, width int) error {
		if count < 0 || (count != 0 && width > math.MaxInt/count) {
			return fmt.Errorf("collections: hnsw search pack section %s length overflow", kind)
		}
		offset, ok := alignColumnHNSWSearchPackUint64(cursor, uint64(alignment))
		length := uint64(count * width)
		if !ok || offset > uint64(math.MaxInt) || length > uint64(math.MaxInt)-offset {
			return fmt.Errorf("collections: hnsw search pack section %s length overflow", kind)
		}
		sections = append(sections, columnHNSWSearchPackSection{Kind: kind, Index: index, Alignment: alignment, Offset: offset, Length: length, Count: uint64(count)})
		cursor = offset + length
		return nil
	}
	if err := add(columnHNSWSearchPackSectionNormalizedVectors, 0, columnHNSWSearchPackVectorSectionAlignment, input.Rows*input.VectorStride, 4); err != nil {
		return columnHNSWSearchPackStreamPlan{}, err
	}
	if err := add(columnHNSWSearchPackSectionLevels, 0, columnHNSWSearchPackAlignment, len(input.Levels), 2); err != nil {
		return columnHNSWSearchPackStreamPlan{}, err
	}
	for i, layer := range input.AdjacencyLayers {
		if err := add(columnHNSWSearchPackSectionAdjacencyOffsets, uint16(i), columnHNSWSearchPackAlignment, len(layer.Offsets), 8); err != nil {
			return columnHNSWSearchPackStreamPlan{}, err
		}
		if err := add(columnHNSWSearchPackSectionAdjacencyNeighbors, uint16(i), columnHNSWSearchPackAlignment, len(layer.Neighbors), 4); err != nil {
			return columnHNSWSearchPackStreamPlan{}, err
		}
	}
	if input.HasAuxiliaryNavigation {
		if err := add(columnHNSWSearchPackSectionAuxiliaryOffsets, 0, columnHNSWSearchPackAlignment, len(input.AuxiliaryNavigation.Offsets), 8); err != nil {
			return columnHNSWSearchPackStreamPlan{}, err
		}
		if err := add(columnHNSWSearchPackSectionAuxiliaryNeighbors, 0, columnHNSWSearchPackAlignment, len(input.AuxiliaryNavigation.Neighbors), 4); err != nil {
			return columnHNSWSearchPackStreamPlan{}, err
		}
	}
	if err := add(columnHNSWSearchPackSectionRowRefGeneration, 0, columnHNSWSearchPackAlignment, len(input.RowRefGenerations), 8); err != nil {
		return columnHNSWSearchPackStreamPlan{}, err
	}
	if err := add(columnHNSWSearchPackSectionRowRefPartID, 0, columnHNSWSearchPackAlignment, len(input.RowRefPartIDs), 8); err != nil {
		return columnHNSWSearchPackStreamPlan{}, err
	}
	if err := add(columnHNSWSearchPackSectionRowRefRowIndex, 0, columnHNSWSearchPackAlignment, len(input.RowRefRowIndexes), 8); err != nil {
		return columnHNSWSearchPackStreamPlan{}, err
	}
	if err := add(columnHNSWSearchPackSectionRowRefAppliedLSN, 0, columnHNSWSearchPackAlignment, len(input.RowRefAppliedCommandLSN), 8); err != nil {
		return columnHNSWSearchPackStreamPlan{}, err
	}
	if err := add(columnHNSWSearchPackSectionDocumentIDOffsets, 0, columnHNSWSearchPackAlignment, len(input.DocumentIDOffsets), 8); err != nil {
		return columnHNSWSearchPackStreamPlan{}, err
	}
	if err := add(columnHNSWSearchPackSectionDocumentIDBytes, 0, columnHNSWSearchPackAlignment, len(input.DocumentIDBytes), 1); err != nil {
		return columnHNSWSearchPackStreamPlan{}, err
	}
	return columnHNSWSearchPackStreamPlan{sections: sections, version: version, headerSize: headerSize, directoryLength: directoryLength, dataOffset: dataOffset, totalLength: cursor}, nil
}

type columnHNSWSearchPackStreamChecksum struct{ sum uint32 }

func (w *columnHNSWSearchPackStreamChecksum) Write(p []byte) (int, error) {
	w.sum = crc.Update(w.sum, p)
	return len(p), nil
}

func writeColumnHNSWSearchPackStreamAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if n > 0 {
			p = p[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func writeColumnHNSWSearchPackStreamSection(w io.Writer, scratch []byte, s columnHNSWSearchPackSection, input columnHNSWSearchPackBuildInput, rows []columnVectorGraphAssetRow) error {
	if s.Kind == columnHNSWSearchPackSectionNormalizedVectors {
		if len(rows) == 0 {
			return nil
		}
		bytesPerRow := input.VectorStride * 4
		chunkRows := max(1, len(scratch)/bytesPerRow)
		buf := scratch[:chunkRows*bytesPerRow]
		for start := 0; start < len(rows); start += chunkRows {
			end := min(start+chunkRows, len(rows))
			part := buf[:(end-start)*input.VectorStride*4]
			clear(part)
			for ordinal := start; ordinal < end; ordinal++ {
				row := rows[ordinal]
				if len(row.Vector) != input.Dimensions {
					return fmt.Errorf("collections: hnsw search pack row[%d] vector dims=%d want %d", ordinal, len(row.Vector), input.Dimensions)
				}
				inv := row.InvNorm
				if inv <= 0 || math.IsNaN(float64(inv)) || math.IsInf(float64(inv), 0) {
					var err error
					inv, err = columnVectorGraphInvNorm(row.Vector)
					if err != nil {
						return fmt.Errorf("collections: hnsw search pack row[%d] inverse norm: %w", ordinal, err)
					}
				}
				base := (ordinal - start) * input.VectorStride * 4
				for dim, value := range row.Vector {
					normalized := value * inv
					if math.IsNaN(float64(normalized)) || math.IsInf(float64(normalized), 0) {
						return fmt.Errorf("collections: hnsw search pack row[%d] normalized vector[%d] is not finite", ordinal, dim)
					}
					binary.LittleEndian.PutUint32(part[base+dim*4:], math.Float32bits(normalized))
				}
			}
			if err := writeColumnHNSWSearchPackStreamAll(w, part); err != nil {
				return err
			}
		}
		return nil
	}
	buf := scratch
	writeUint16 := func(values []uint16) error {
		for start := 0; start < len(values); {
			count := min(len(values)-start, len(buf)/2)
			part := buf[:count*2]
			for i, v := range values[start : start+count] {
				binary.LittleEndian.PutUint16(part[i*2:], v)
			}
			if err := writeColumnHNSWSearchPackStreamAll(w, part); err != nil {
				return err
			}
			start += count
		}
		return nil
	}
	writeUint32 := func(values []uint32) error {
		for start := 0; start < len(values); {
			count := min(len(values)-start, len(buf)/4)
			part := buf[:count*4]
			for i, v := range values[start : start+count] {
				binary.LittleEndian.PutUint32(part[i*4:], v)
			}
			if err := writeColumnHNSWSearchPackStreamAll(w, part); err != nil {
				return err
			}
			start += count
		}
		return nil
	}
	writeUint64 := func(values []uint64) error {
		for start := 0; start < len(values); {
			count := min(len(values)-start, len(buf)/8)
			part := buf[:count*8]
			for i, v := range values[start : start+count] {
				binary.LittleEndian.PutUint64(part[i*8:], v)
			}
			if err := writeColumnHNSWSearchPackStreamAll(w, part); err != nil {
				return err
			}
			start += count
		}
		return nil
	}
	writeInt64 := func(values []int64) error {
		for start := 0; start < len(values); {
			count := min(len(values)-start, len(buf)/8)
			part := buf[:count*8]
			for i, v := range values[start : start+count] {
				binary.LittleEndian.PutUint64(part[i*8:], uint64(v))
			}
			if err := writeColumnHNSWSearchPackStreamAll(w, part); err != nil {
				return err
			}
			start += count
		}
		return nil
	}
	switch s.Kind {
	case columnHNSWSearchPackSectionLevels:
		return writeUint16(input.Levels)
	case columnHNSWSearchPackSectionAdjacencyOffsets:
		return writeUint64(input.AdjacencyLayers[s.Index].Offsets)
	case columnHNSWSearchPackSectionAdjacencyNeighbors:
		return writeUint32(input.AdjacencyLayers[s.Index].Neighbors)
	case columnHNSWSearchPackSectionAuxiliaryOffsets:
		return writeUint64(input.AuxiliaryNavigation.Offsets)
	case columnHNSWSearchPackSectionAuxiliaryNeighbors:
		return writeUint32(input.AuxiliaryNavigation.Neighbors)
	case columnHNSWSearchPackSectionRowRefGeneration, columnHNSWSearchPackSectionRowRefPartID, columnHNSWSearchPackSectionRowRefRowIndex, columnHNSWSearchPackSectionRowRefAppliedLSN:
		var values []int64
		switch s.Kind {
		case columnHNSWSearchPackSectionRowRefGeneration:
			values = input.RowRefGenerations
		case columnHNSWSearchPackSectionRowRefPartID:
			values = input.RowRefPartIDs
		case columnHNSWSearchPackSectionRowRefRowIndex:
			values = input.RowRefRowIndexes
		default:
			values = input.RowRefAppliedCommandLSN
		}
		return writeInt64(values)
	case columnHNSWSearchPackSectionDocumentIDOffsets:
		return writeUint64(input.DocumentIDOffsets)
	case columnHNSWSearchPackSectionDocumentIDBytes:
		for start := 0; start < len(input.DocumentIDBytes); start += len(buf) {
			if err := writeColumnHNSWSearchPackStreamAll(w, input.DocumentIDBytes[start:min(start+len(buf), len(input.DocumentIDBytes))]); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("collections: unsupported hnsw search pack streamed section %s", s.Kind)
	}
}

func decodeColumnHNSWSearchPack(raw []byte, opts columnHNSWSearchPackDecodeOptions) (columnHNSWSearchPack, error) {
	opts = opts.withDefaults()
	if len(raw) < columnHNSWSearchPackHeaderSize {
		return columnHNSWSearchPack{}, fmt.Errorf("collections: truncated hnsw_search_pack_v1 header bytes=%d want at least %d", len(raw), columnHNSWSearchPackHeaderSize)
	}
	if !bytes.Equal(raw[:8], columnHNSWSearchPackMagic[:]) {
		return columnHNSWSearchPack{}, fmt.Errorf("collections: bad hnsw_search_pack_v1 magic=%q", string(raw[:8]))
	}
	version := hnswPackU16(raw, columnHNSWSearchPackHeaderVersionOffset)
	headerSize := columnHNSWSearchPackHeaderSize
	switch version {
	case columnHNSWSearchPackVersionV1:
	case columnHNSWSearchPackVersionV2, columnHNSWSearchPackVersionV3:
		headerSize = columnHNSWSearchPackHeaderSizeV2
	default:
		return columnHNSWSearchPack{}, fmt.Errorf("collections: unsupported hnsw_search_pack_v1 version=%d", version)
	}
	if len(raw) < headerSize {
		return columnHNSWSearchPack{}, fmt.Errorf("collections: truncated hnsw_search_pack_v1 header bytes=%d want at least %d", len(raw), headerSize)
	}
	if got := hnswPackU16(raw, columnHNSWSearchPackHeaderHeaderSizeOffset); got != uint16(headerSize) {
		return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 header_size=%d want %d", got, headerSize)
	}
	if got := hnswPackU16(raw, columnHNSWSearchPackHeaderSectionEntrySizeOffset); got != columnHNSWSearchPackSectionEntrySize {
		return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 section_entry_size=%d want %d", got, columnHNSWSearchPackSectionEntrySize)
	}
	if flags := hnswPackU16(raw, columnHNSWSearchPackHeaderFlagsOffset); flags != 0 {
		return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 unsupported flags=0x%x", flags)
	}
	totalLength := hnswPackU64(raw, columnHNSWSearchPackHeaderTotalLengthOffset)
	if totalLength != uint64(len(raw)) {
		return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 length=%d want header total_length=%d", len(raw), totalLength)
	}
	rows64 := hnswPackU64(raw, columnHNSWSearchPackHeaderRowsOffset)
	dims32 := hnswPackU32(raw, columnHNSWSearchPackHeaderDimensionsOffset)
	stride32 := hnswPackU32(raw, columnHNSWSearchPackHeaderVectorStrideOffset)
	maxLayer32 := hnswPackU32(raw, columnHNSWSearchPackHeaderMaxLayerOffset)
	layerCount32 := hnswPackU32(raw, columnHNSWSearchPackHeaderAdjacencyLayerCount)
	if rows64 > opts.MaxRows || rows64 > uint64(math.MaxInt) {
		return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 row count=%d exceeds cap=%d", rows64, opts.MaxRows)
	}
	if dims32 == 0 || dims32 > opts.MaxDimensions || uint64(dims32) > uint64(math.MaxInt) {
		return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 dimensions=%d outside cap=%d", dims32, opts.MaxDimensions)
	}
	if stride32 < dims32 || stride32 > opts.MaxVectorStride || uint64(stride32) > uint64(math.MaxInt) || (uint64(stride32)*4)%uint64(columnHNSWSearchPackVectorSectionAlignment) != 0 {
		return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 vector stride=%d invalid for dimensions=%d", stride32, dims32)
	}
	if layerCount32 > opts.MaxLayers || uint64(layerCount32) > uint64(math.MaxInt) {
		return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer count=%d exceeds cap=%d", layerCount32, opts.MaxLayers)
	}
	if metric := hnswPackU16(raw, columnHNSWSearchPackHeaderMetricOffset); metric != columnHNSWSearchPackMetricCosineNormalizedDot {
		return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 metric=%d want cosine_normalized_dot", metric)
	}
	if encoding := hnswPackU16(raw, columnHNSWSearchPackHeaderEncodingOffset); encoding != columnHNSWSearchPackEncodingFloat32 {
		return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 encoding=%d want float32", encoding)
	}
	if m := hnswPackU32(raw, columnHNSWSearchPackHeaderMOffset); m == 0 || uint64(m) > uint64(math.MaxInt) {
		return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 M=%d must be positive", m)
	}
	if ef := hnswPackU32(raw, columnHNSWSearchPackHeaderEfConstructionOffset); ef == 0 || uint64(ef) > uint64(math.MaxInt) {
		return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 ef_construction=%d must be positive", ef)
	}
	if ef := hnswPackU32(raw, columnHNSWSearchPackHeaderEfSearchOffset); ef == 0 || uint64(ef) > uint64(math.MaxInt) {
		return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 ef_search=%d must be positive", ef)
	}
	if rows64 == 0 {
		if layerCount32 != 0 || maxLayer32 != columnHNSWSearchPackNoMaxLayer || hnswPackU64(raw, columnHNSWSearchPackHeaderEntryOrdinalOffset) != columnHNSWSearchPackNoEntryOrdinal {
			return columnHNSWSearchPack{}, errors.New("collections: hnsw_search_pack_v1 empty pack must use no-entry/no-layer sentinels")
		}
	} else {
		entryOrdinal := hnswPackU64(raw, columnHNSWSearchPackHeaderEntryOrdinalOffset)
		if entryOrdinal >= rows64 {
			return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 entry ordinal=%d outside rows=%d", entryOrdinal, rows64)
		}
		if maxLayer32 == columnHNSWSearchPackNoMaxLayer || maxLayer32+1 != layerCount32 {
			return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 max_layer=%d layer_count=%d mismatch", maxLayer32, layerCount32)
		}
	}
	baseIdentity := columnHNSWSearchPackBaseIdentity{
		ManifestGeneration: hnswPackU64(raw, columnHNSWSearchPackHeaderBaseGenerationOffset),
		ManifestChecksum:   hnswPackU64(raw, columnHNSWSearchPackHeaderBaseChecksumOffset),
		SchemaHash:         hnswPackU64(raw, columnHNSWSearchPackHeaderBaseSchemaHashOffset),
	}
	if baseIdentity.ManifestGeneration == 0 || baseIdentity.ManifestChecksum == 0 || baseIdentity.SchemaHash == 0 {
		return columnHNSWSearchPack{}, errors.New("collections: hnsw_search_pack_v1 missing base manifest identity")
	}
	if err := validateColumnHNSWSearchPackExpectedBaseIdentity(baseIdentity, opts.ExpectedBaseIdentity); err != nil {
		return columnHNSWSearchPack{}, err
	}
	var membershipDigest [sha256.Size]byte
	if version == columnHNSWSearchPackVersionV2 || version == columnHNSWSearchPackVersionV3 {
		copy(membershipDigest[:], raw[columnHNSWSearchPackHeaderMembershipDigestOffset:columnHNSWSearchPackHeaderSizeV2])
		if membershipDigest == ([sha256.Size]byte{}) {
			return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 version %d missing membership digest", version)
		}
	}
	if opts.ExpectedMembershipDigest != ([sha256.Size]byte{}) && membershipDigest != opts.ExpectedMembershipDigest {
		return columnHNSWSearchPack{}, errors.New("collections: hnsw_search_pack_v1 membership digest mismatch")
	}
	directoryOffset := hnswPackU64(raw, columnHNSWSearchPackHeaderDirectoryOffsetOffset)
	directoryLength := hnswPackU64(raw, columnHNSWSearchPackHeaderDirectoryLengthOffset)
	dataOffset := hnswPackU64(raw, columnHNSWSearchPackHeaderDataOffsetOffset)
	dataLength := hnswPackU64(raw, columnHNSWSearchPackHeaderDataLengthOffset)
	sectionCount32 := hnswPackU32(raw, columnHNSWSearchPackHeaderSectionCountOffset)
	expectedSectionCount := uint32(8 + 2*layerCount32)
	if version == columnHNSWSearchPackVersionV3 {
		expectedSectionCount += 2
	}
	if sectionCount32 != expectedSectionCount {
		return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 section_count=%d want %d", sectionCount32, expectedSectionCount)
	}
	maxSectionCount := uint32(8 + 2*opts.MaxLayers)
	if version == columnHNSWSearchPackVersionV3 {
		maxSectionCount += 2
	}
	if sectionCount32 > maxSectionCount {
		return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 section_count=%d exceeds cap", sectionCount32)
	}
	if directoryOffset != uint64(headerSize) || directoryLength != uint64(sectionCount32)*columnHNSWSearchPackSectionEntrySize {
		return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 corrupt section directory offset=%d length=%d count=%d", directoryOffset, directoryLength, sectionCount32)
	}
	if dataOffset < directoryOffset+directoryLength || dataOffset > totalLength || dataLength != totalLength-dataOffset {
		return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 corrupt data region offset=%d length=%d total=%d", dataOffset, dataLength, totalLength)
	}
	if directoryOffset+directoryLength > uint64(len(raw)) {
		return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 section directory exceeds pack length")
	}
	directory := raw[directoryOffset : directoryOffset+directoryLength]
	if got, want := page.Checksum(directory), hnswPackU32(raw, columnHNSWSearchPackHeaderDirectoryChecksumOffset); got != want {
		return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 section directory checksum=%08x want %08x", got, want)
	}
	sections := make([]columnHNSWSearchPackSection, int(sectionCount32))
	for i := range sections {
		section, err := decodeColumnHNSWSearchPackSectionEntry(directory[i*columnHNSWSearchPackSectionEntrySize:])
		if err != nil {
			return columnHNSWSearchPack{}, fmt.Errorf("collections: hnsw_search_pack_v1 section[%d]: %w", i, err)
		}
		sections[i] = section
	}
	if err := validateColumnHNSWSearchPackSectionDirectory(raw, sections, dataOffset); err != nil {
		return columnHNSWSearchPack{}, err
	}
	pack := columnHNSWSearchPack{
		Header: columnHNSWSearchPackHeader{
			Rows:                   int(rows64),
			Dimensions:             int(dims32),
			VectorStride:           int(stride32),
			M:                      int(hnswPackU32(raw, columnHNSWSearchPackHeaderMOffset)),
			EfConstruction:         int(hnswPackU32(raw, columnHNSWSearchPackHeaderEfConstructionOffset)),
			EfSearch:               int(hnswPackU32(raw, columnHNSWSearchPackHeaderEfSearchOffset)),
			EntryOrdinal:           int(hnswPackU64(raw, columnHNSWSearchPackHeaderEntryOrdinalOffset)),
			MaxLayer:               int(maxLayer32),
			AdjacencyLayerCount:    int(layerCount32),
			BaseManifestGeneration: baseIdentity.ManifestGeneration,
			BaseManifestChecksum:   baseIdentity.ManifestChecksum,
			BaseSchemaHash:         baseIdentity.SchemaHash,
			MembershipDigest:       membershipDigest,
			HasAuxiliaryNavigation: version == columnHNSWSearchPackVersionV3,
			TotalLength:            totalLength,
			DataOffset:             dataOffset,
			DataLength:             dataLength,
		},
		Sections: sections,
	}
	if rows64 == 0 {
		pack.Header.EntryOrdinal = -1
		pack.Header.MaxLayer = -1
	}
	if err := decodeColumnHNSWSearchPackSections(raw, opts, &pack); err != nil {
		return columnHNSWSearchPack{}, err
	}
	return pack, nil
}

func (opts columnHNSWSearchPackDecodeOptions) withDefaults() columnHNSWSearchPackDecodeOptions {
	if opts.MaxRows == 0 {
		opts.MaxRows = columnHNSWSearchPackMaxRowsDefault
	}
	if opts.MaxDimensions == 0 {
		opts.MaxDimensions = columnHNSWSearchPackMaxDimensionsDefault
	}
	if opts.MaxVectorStride == 0 {
		opts.MaxVectorStride = columnHNSWSearchPackMaxVectorStrideDefault
	}
	if opts.MaxLayers == 0 {
		opts.MaxLayers = columnHNSWSearchPackMaxLayersDefault
	}
	if opts.MaxNeighbors == 0 {
		opts.MaxNeighbors = columnHNSWSearchPackMaxNeighborsDefault
	}
	if opts.MaxDocumentIDBytes == 0 {
		opts.MaxDocumentIDBytes = columnHNSWSearchPackMaxDocumentBytesDefault
	}
	return opts
}

func validateColumnHNSWSearchPackBuildInput(input columnHNSWSearchPackBuildInput) error {
	return validateColumnHNSWSearchPackBuildInputMode(input, true)
}

func validateColumnHNSWSearchPackBuildInputWithoutVectors(input columnHNSWSearchPackBuildInput) error {
	return validateColumnHNSWSearchPackBuildInputMode(input, false)
}

func validateColumnHNSWSearchPackBuildInputMode(input columnHNSWSearchPackBuildInput, requireVectors bool) error {
	if input.Rows < 0 || input.Dimensions <= 0 || input.VectorStride < input.Dimensions || (input.VectorStride*4)%int(columnHNSWSearchPackVectorSectionAlignment) != 0 {
		return fmt.Errorf("collections: hnsw search pack invalid rows/dimensions/stride=(%d,%d,%d)", input.Rows, input.Dimensions, input.VectorStride)
	}
	if input.Rows != 0 && input.VectorStride > math.MaxInt/input.Rows {
		return errors.New("collections: hnsw search pack normalized vector count overflows int")
	}
	if input.M <= 0 || input.EfConstruction <= 0 || input.EfSearch <= 0 {
		return errors.New("collections: hnsw search pack graph parameters must be positive")
	}
	if input.BaseIdentity.ManifestGeneration == 0 || input.BaseIdentity.ManifestChecksum == 0 || input.BaseIdentity.SchemaHash == 0 {
		return errors.New("collections: hnsw search pack requires base manifest identity")
	}
	if input.Rows == 0 {
		if input.EntryOrdinal != -1 || input.MaxLayer != -1 || len(input.AdjacencyLayers) != 0 {
			return errors.New("collections: empty hnsw search pack requires no-entry/no-layer sentinels")
		}
	} else {
		if len(input.AdjacencyLayers) > math.MaxUint16 {
			return errors.New("collections: hnsw search pack adjacency layer count overflows section index")
		}
		if input.EntryOrdinal < 0 || input.EntryOrdinal >= input.Rows {
			return fmt.Errorf("collections: hnsw search pack entry ordinal=%d outside rows=%d", input.EntryOrdinal, input.Rows)
		}
		if input.MaxLayer < 0 || input.MaxLayer+1 != len(input.AdjacencyLayers) {
			return fmt.Errorf("collections: hnsw search pack max_layer=%d adjacency layers=%d mismatch", input.MaxLayer, len(input.AdjacencyLayers))
		}
	}
	if requireVectors {
		if len(input.NormalizedVectors) != input.Rows*input.VectorStride {
			return fmt.Errorf("collections: hnsw search pack normalized vector values=%d want rows*stride=%d", len(input.NormalizedVectors), input.Rows*input.VectorStride)
		}
		for i, v := range input.NormalizedVectors {
			if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
				return fmt.Errorf("collections: hnsw search pack normalized vector[%d] is not finite", i)
			}
		}
	}
	if len(input.Levels) != input.Rows {
		return fmt.Errorf("collections: hnsw search pack levels=%d want rows=%d", len(input.Levels), input.Rows)
	}
	for ordinal, level := range input.Levels {
		if int(level) > input.MaxLayer {
			return fmt.Errorf("collections: hnsw search pack level[%d]=%d exceeds max_layer=%d", ordinal, level, input.MaxLayer)
		}
	}
	for layer, adjacency := range input.AdjacencyLayers {
		if len(adjacency.Offsets) != input.Rows+1 {
			return fmt.Errorf("collections: hnsw search pack layer=%d offsets=%d want rows+1=%d", layer, len(adjacency.Offsets), input.Rows+1)
		}
		if err := validateColumnHNSWSearchPackAdjacency(layer, uint64(input.Rows), adjacency.Offsets, adjacency.Neighbors); err != nil {
			return err
		}
	}
	if input.HasAuxiliaryNavigation {
		if input.MembershipDigest == ([sha256.Size]byte{}) {
			return errors.New("collections: hnsw search pack auxiliary navigation requires membership digest")
		}
		maxAuxiliaryNeighbors := uint64(0)
		if input.Rows > 1 {
			maxAuxiliaryNeighbors = uint64(input.Rows-1) * 2
		}
		if len(input.AuxiliaryNavigation.Offsets) != input.Rows+1 || uint64(len(input.AuxiliaryNavigation.Neighbors)) > maxAuxiliaryNeighbors || uint64(len(input.AuxiliaryNavigation.Neighbors)) > columnHNSWSearchPackMaxNeighborsDefault {
			return errors.New("collections: hnsw search pack auxiliary navigation shape")
		}
		if input.Rows > 0 && len(input.AdjacencyLayers) == 0 {
			return errors.New("collections: hnsw search pack auxiliary navigation requires layer 0")
		}
		var native columnHNSWSearchPackLayerInput
		if len(input.AdjacencyLayers) != 0 {
			native = input.AdjacencyLayers[0]
		}
		if err := validateVectorPartitionLocalAuxiliaryNavigationFromNativeLayer0V1(input.Rows, input.EntryOrdinal, input.Levels, native.Offsets, native.Neighbors, vectorPartitionLocalAuxiliaryNavigationV1{Offsets: input.AuxiliaryNavigation.Offsets, Neighbors: input.AuxiliaryNavigation.Neighbors}); err != nil {
			return fmt.Errorf("collections: hnsw search pack auxiliary navigation: %w", err)
		}
	} else if len(input.AuxiliaryNavigation.Offsets) != 0 || len(input.AuxiliaryNavigation.Neighbors) != 0 {
		return errors.New("collections: hnsw search pack auxiliary navigation without version 3")
	}
	if len(input.RowRefGenerations) != input.Rows || len(input.RowRefPartIDs) != input.Rows || len(input.RowRefRowIndexes) != input.Rows || len(input.RowRefAppliedCommandLSN) != input.Rows {
		return errors.New("collections: hnsw search pack row-ref section row counts must match")
	}
	for ordinal := 0; ordinal < input.Rows; ordinal++ {
		if input.RowRefGenerations[ordinal] <= 0 || input.RowRefPartIDs[ordinal] <= 0 || input.RowRefAppliedCommandLSN[ordinal] <= 0 || input.RowRefRowIndexes[ordinal] < 0 {
			return fmt.Errorf("collections: hnsw search pack invalid row-ref ordinal=%d", ordinal)
		}
		if uint64(input.RowRefGenerations[ordinal]) > input.BaseIdentity.ManifestGeneration {
			return fmt.Errorf("collections: hnsw search pack row-ref generation=%d exceeds base generation=%d", input.RowRefGenerations[ordinal], input.BaseIdentity.ManifestGeneration)
		}
	}
	if len(input.DocumentIDOffsets) != input.Rows+1 {
		return fmt.Errorf("collections: hnsw search pack document id offsets=%d want rows+1=%d", len(input.DocumentIDOffsets), input.Rows+1)
	}
	return validateColumnHNSWSearchPackDocumentIDs(uint64(input.Rows), input.DocumentIDOffsets, input.DocumentIDBytes)
}

func validateColumnHNSWSearchPackExpectedBaseIdentity(got, want columnHNSWSearchPackBaseIdentity) error {
	if want.ManifestGeneration != 0 && got.ManifestGeneration != want.ManifestGeneration {
		return fmt.Errorf("collections: hnsw_search_pack_v1 base manifest generation=%d want %d", got.ManifestGeneration, want.ManifestGeneration)
	}
	if want.ManifestChecksum != 0 && got.ManifestChecksum != want.ManifestChecksum {
		return fmt.Errorf("collections: hnsw_search_pack_v1 base manifest checksum=%d want %d", got.ManifestChecksum, want.ManifestChecksum)
	}
	if want.SchemaHash != 0 && got.SchemaHash != want.SchemaHash {
		return fmt.Errorf("collections: hnsw_search_pack_v1 base schema hash=%d want %d", got.SchemaHash, want.SchemaHash)
	}
	return nil
}

func validateColumnHNSWSearchPackSectionDirectory(raw []byte, sections []columnHNSWSearchPackSection, dataOffset uint64) error {
	seen := make(map[columnHNSWSearchPackSectionKey]struct{}, len(sections))
	ranges := make([]columnHNSWSearchPackSectionRange, 0, len(sections))
	for _, section := range sections {
		if section.Kind == 0 || section.Alignment == 0 {
			return fmt.Errorf("collections: hnsw_search_pack_v1 section %s has missing kind/alignment", section.Kind)
		}
		if !columnHNSWSearchPackKnownSection(section.Kind) {
			return fmt.Errorf("collections: hnsw_search_pack_v1 unknown section kind=%d", section.Kind)
		}
		if section.Offset < dataOffset || section.Offset > uint64(len(raw)) || section.Length > uint64(len(raw))-section.Offset {
			return fmt.Errorf("collections: hnsw_search_pack_v1 section %s[%d] bounds offset=%d length=%d total=%d", section.Kind, section.Index, section.Offset, section.Length, len(raw))
		}
		if section.Offset%uint64(section.Alignment) != 0 {
			return fmt.Errorf("collections: hnsw_search_pack_v1 section %s[%d] offset=%d is not aligned to %d", section.Kind, section.Index, section.Offset, section.Alignment)
		}
		if err := validateColumnHNSWSearchPackSectionAlignment(section.Kind, section.Alignment); err != nil {
			return err
		}
		key := columnHNSWSearchPackSectionKey{kind: section.Kind, index: section.Index}
		if _, ok := seen[key]; ok {
			return fmt.Errorf("collections: hnsw_search_pack_v1 duplicate section %s[%d]", section.Kind, section.Index)
		}
		seen[key] = struct{}{}
		payload := raw[section.Offset : section.Offset+section.Length]
		if got := page.Checksum(payload); got != section.Checksum {
			return fmt.Errorf("collections: hnsw_search_pack_v1 section %s[%d] checksum=%08x want %08x", section.Kind, section.Index, got, section.Checksum)
		}
		ranges = append(ranges, columnHNSWSearchPackSectionRange{start: section.Offset, end: section.Offset + section.Length, section: section})
	}
	slices.SortFunc(ranges, func(a, b columnHNSWSearchPackSectionRange) int {
		if a.start < b.start {
			return -1
		}
		if a.start > b.start {
			return 1
		}
		return 0
	})
	for i := 1; i < len(ranges); i++ {
		if ranges[i].start < ranges[i-1].end {
			return fmt.Errorf("collections: hnsw_search_pack_v1 overlapping sections %s[%d] and %s[%d]", ranges[i-1].section.Kind, ranges[i-1].section.Index, ranges[i].section.Kind, ranges[i].section.Index)
		}
	}
	return nil
}

func decodeColumnHNSWSearchPackSections(raw []byte, opts columnHNSWSearchPackDecodeOptions, pack *columnHNSWSearchPack) error {
	rows := uint64(pack.Header.Rows)
	stride := uint64(pack.Header.VectorStride)
	vectorCount, ok := checkedHNSWPackMulOK(rows, stride)
	if !ok {
		return errors.New("collections: hnsw_search_pack_v1 normalized vector count overflows uint64")
	}
	vectors, err := columnHNSWSearchPackRequireSection(pack.Sections, columnHNSWSearchPackSectionNormalizedVectors, 0, vectorCount, 4)
	if err != nil {
		return err
	}
	pack.NormalizedVectors = decodeFloat32SliceLE(raw[vectors.Offset : vectors.Offset+vectors.Length])
	for i, v := range pack.NormalizedVectors {
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return fmt.Errorf("collections: hnsw_search_pack_v1 normalized vector[%d] is not finite", i)
		}
	}
	levels, err := columnHNSWSearchPackRequireSection(pack.Sections, columnHNSWSearchPackSectionLevels, 0, rows, 2)
	if err != nil {
		return err
	}
	pack.Levels = decodeUint16SliceLE(raw[levels.Offset : levels.Offset+levels.Length])
	for ordinal, level := range pack.Levels {
		if int(level) > pack.Header.MaxLayer {
			return fmt.Errorf("collections: hnsw_search_pack_v1 level[%d]=%d exceeds max_layer=%d", ordinal, level, pack.Header.MaxLayer)
		}
	}
	pack.AdjacencyLayers = make([]columnHNSWSearchPackLayer, pack.Header.AdjacencyLayerCount)
	for layer := 0; layer < pack.Header.AdjacencyLayerCount; layer++ {
		offsetsSection, err := columnHNSWSearchPackRequireSection(pack.Sections, columnHNSWSearchPackSectionAdjacencyOffsets, uint16(layer), rows+1, 8)
		if err != nil {
			return err
		}
		offsets := decodeUint64SliceLE(raw[offsetsSection.Offset : offsetsSection.Offset+offsetsSection.Length])
		neighborsSection, err := columnHNSWSearchPackFindSection(pack.Sections, columnHNSWSearchPackSectionAdjacencyNeighbors, uint16(layer))
		if err != nil {
			return err
		}
		if neighborsSection.Length%4 != 0 || neighborsSection.Count != neighborsSection.Length/4 {
			return fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer=%d neighbors length/count mismatch", layer)
		}
		if neighborsSection.Count > opts.MaxNeighbors || neighborsSection.Count > uint64(math.MaxInt) {
			return fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer=%d neighbors=%d exceeds cap=%d", layer, neighborsSection.Count, opts.MaxNeighbors)
		}
		neighbors := decodeUint32SliceLE(raw[neighborsSection.Offset : neighborsSection.Offset+neighborsSection.Length])
		if err := validateColumnHNSWSearchPackAdjacency(layer, rows, offsets, neighbors); err != nil {
			return err
		}
		pack.AdjacencyLayers[layer] = columnHNSWSearchPackLayer{Offsets: offsets, Neighbors: neighbors}
	}
	if pack.Header.HasAuxiliaryNavigation {
		offsetsSection, err := columnHNSWSearchPackRequireSection(pack.Sections, columnHNSWSearchPackSectionAuxiliaryOffsets, 0, rows+1, 8)
		if err != nil {
			return err
		}
		offsets := decodeUint64SliceLE(raw[offsetsSection.Offset : offsetsSection.Offset+offsetsSection.Length])
		neighborsSection, err := columnHNSWSearchPackFindSection(pack.Sections, columnHNSWSearchPackSectionAuxiliaryNeighbors, 0)
		if err != nil {
			return err
		}
		maxAuxiliaryNeighbors := uint64(0)
		if pack.Header.Rows > 1 {
			maxAuxiliaryNeighbors = uint64(pack.Header.Rows-1) * 2
		}
		if neighborsSection.Length%4 != 0 || neighborsSection.Count != neighborsSection.Length/4 || neighborsSection.Count > maxAuxiliaryNeighbors || neighborsSection.Count > opts.MaxNeighbors || neighborsSection.Count > uint64(math.MaxInt) {
			return errors.New("collections: hnsw_search_pack_v1 auxiliary neighbors shape")
		}
		neighbors := decodeUint32SliceLE(raw[neighborsSection.Offset : neighborsSection.Offset+neighborsSection.Length])
		if pack.Header.Rows > 0 && len(pack.AdjacencyLayers) == 0 {
			return errors.New("collections: hnsw_search_pack_v1 auxiliary navigation requires layer 0")
		}
		var native columnHNSWSearchPackLayer
		if len(pack.AdjacencyLayers) != 0 {
			native = pack.AdjacencyLayers[0]
		}
		if err := validateVectorPartitionLocalAuxiliaryNavigationFromNativeLayer0V1(pack.Header.Rows, pack.Header.EntryOrdinal, pack.Levels, native.Offsets, native.Neighbors, vectorPartitionLocalAuxiliaryNavigationV1{Offsets: offsets, Neighbors: neighbors}); err != nil {
			return fmt.Errorf("collections: hnsw_search_pack_v1 auxiliary navigation: %w", err)
		}
		pack.AuxiliaryNavigation = columnHNSWSearchPackLayer{Offsets: offsets, Neighbors: neighbors}
	}
	if pack.RowRefGenerations, err = decodeColumnHNSWSearchPackInt64Section(raw, pack.Sections, columnHNSWSearchPackSectionRowRefGeneration, rows); err != nil {
		return err
	}
	if pack.RowRefPartIDs, err = decodeColumnHNSWSearchPackInt64Section(raw, pack.Sections, columnHNSWSearchPackSectionRowRefPartID, rows); err != nil {
		return err
	}
	if pack.RowRefRowIndexes, err = decodeColumnHNSWSearchPackInt64Section(raw, pack.Sections, columnHNSWSearchPackSectionRowRefRowIndex, rows); err != nil {
		return err
	}
	if pack.RowRefAppliedLSNs, err = decodeColumnHNSWSearchPackInt64Section(raw, pack.Sections, columnHNSWSearchPackSectionRowRefAppliedLSN, rows); err != nil {
		return err
	}
	for ordinal := 0; ordinal < pack.Header.Rows; ordinal++ {
		if pack.RowRefGenerations[ordinal] <= 0 || pack.RowRefPartIDs[ordinal] <= 0 || pack.RowRefAppliedLSNs[ordinal] <= 0 || pack.RowRefRowIndexes[ordinal] < 0 {
			return fmt.Errorf("collections: hnsw_search_pack_v1 invalid row-ref ordinal=%d", ordinal)
		}
		if uint64(pack.RowRefGenerations[ordinal]) > pack.Header.BaseManifestGeneration {
			return fmt.Errorf("collections: hnsw_search_pack_v1 row-ref generation=%d exceeds base generation=%d", pack.RowRefGenerations[ordinal], pack.Header.BaseManifestGeneration)
		}
	}
	docOffsetsSection, err := columnHNSWSearchPackRequireSection(pack.Sections, columnHNSWSearchPackSectionDocumentIDOffsets, 0, rows+1, 8)
	if err != nil {
		return err
	}
	pack.DocumentIDOffsets = decodeUint64SliceLE(raw[docOffsetsSection.Offset : docOffsetsSection.Offset+docOffsetsSection.Length])
	docBytesSection, err := columnHNSWSearchPackFindSection(pack.Sections, columnHNSWSearchPackSectionDocumentIDBytes, 0)
	if err != nil {
		return err
	}
	if docBytesSection.Count != docBytesSection.Length {
		return fmt.Errorf("collections: hnsw_search_pack_v1 document_id_bytes length/count mismatch")
	}
	if docBytesSection.Length > opts.MaxDocumentIDBytes || docBytesSection.Length > uint64(math.MaxInt) {
		return fmt.Errorf("collections: hnsw_search_pack_v1 document_id_bytes=%d exceeds cap=%d", docBytesSection.Length, opts.MaxDocumentIDBytes)
	}
	pack.DocumentIDBytes = append([]byte(nil), raw[docBytesSection.Offset:docBytesSection.Offset+docBytesSection.Length]...)
	return validateColumnHNSWSearchPackDocumentIDs(rows, pack.DocumentIDOffsets, pack.DocumentIDBytes)
}

func decodeColumnHNSWSearchPackInt64Section(raw []byte, sections []columnHNSWSearchPackSection, kind columnHNSWSearchPackSectionKind, rows uint64) ([]int64, error) {
	section, err := columnHNSWSearchPackRequireSection(sections, kind, 0, rows, 8)
	if err != nil {
		return nil, err
	}
	return decodeInt64SliceLE(raw[section.Offset : section.Offset+section.Length]), nil
}

func columnHNSWSearchPackRequireSection(sections []columnHNSWSearchPackSection, kind columnHNSWSearchPackSectionKind, index uint16, count uint64, elemBytes uint64) (columnHNSWSearchPackSection, error) {
	section, err := columnHNSWSearchPackFindSection(sections, kind, index)
	if err != nil {
		return columnHNSWSearchPackSection{}, err
	}
	length, ok := checkedHNSWPackMulOK(count, elemBytes)
	if !ok {
		return columnHNSWSearchPackSection{}, fmt.Errorf("collections: hnsw_search_pack_v1 section %s[%d] expected length overflow", kind, index)
	}
	if section.Count != count || section.Length != length {
		return columnHNSWSearchPackSection{}, fmt.Errorf("collections: hnsw_search_pack_v1 section %s[%d] length/count=(%d,%d) want (%d,%d)", kind, index, section.Length, section.Count, length, count)
	}
	return section, nil
}

func columnHNSWSearchPackFindSection(sections []columnHNSWSearchPackSection, kind columnHNSWSearchPackSectionKind, index uint16) (columnHNSWSearchPackSection, error) {
	for _, section := range sections {
		if section.Kind == kind && section.Index == index {
			return section, nil
		}
	}
	return columnHNSWSearchPackSection{}, fmt.Errorf("collections: hnsw_search_pack_v1 missing section %s[%d]", kind, index)
}

func validateColumnHNSWSearchPackAdjacency(layer int, rows uint64, offsets []uint64, neighbors []uint32) error {
	if uint64(len(offsets)) != rows+1 {
		return fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer=%d offsets=%d want rows+1=%d", layer, len(offsets), rows+1)
	}
	if offsets[0] != 0 {
		return fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer=%d first offset=%d want 0", layer, offsets[0])
	}
	for i := 1; i < len(offsets); i++ {
		if offsets[i] < offsets[i-1] {
			return fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer=%d offsets are not monotonic at row=%d", layer, i-1)
		}
	}
	if offsets[len(offsets)-1] != uint64(len(neighbors)) {
		return fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer=%d last offset=%d want neighbors=%d", layer, offsets[len(offsets)-1], len(neighbors))
	}
	for i, neighbor := range neighbors {
		if uint64(neighbor) >= rows {
			return fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer=%d neighbor ordinal[%d]=%d outside rows=%d", layer, i, neighbor, rows)
		}
	}
	return nil
}

func validateColumnHNSWSearchPackDocumentIDs(rows uint64, offsets []uint64, values []byte) error {
	if uint64(len(offsets)) != rows+1 {
		return fmt.Errorf("collections: hnsw_search_pack_v1 document id offsets=%d want rows+1=%d", len(offsets), rows+1)
	}
	if offsets[0] != 0 {
		return fmt.Errorf("collections: hnsw_search_pack_v1 document id first offset=%d want 0", offsets[0])
	}
	for i := uint64(0); i < rows; i++ {
		if offsets[i+1] < offsets[i] {
			return fmt.Errorf("collections: hnsw_search_pack_v1 document id offsets are not monotonic at row=%d", i)
		}
		if offsets[i+1] == offsets[i] {
			return fmt.Errorf("collections: hnsw_search_pack_v1 document id row=%d is empty", i)
		}
	}
	if offsets[len(offsets)-1] != uint64(len(values)) {
		return fmt.Errorf("collections: hnsw_search_pack_v1 document id last offset=%d want bytes=%d", offsets[len(offsets)-1], len(values))
	}
	return nil
}

func validateColumnHNSWSearchPackSectionAlignment(kind columnHNSWSearchPackSectionKind, alignment uint32) error {
	want := columnHNSWSearchPackAlignment
	if kind == columnHNSWSearchPackSectionNormalizedVectors {
		want = columnHNSWSearchPackVectorSectionAlignment
	}
	if alignment != want {
		return fmt.Errorf("collections: hnsw_search_pack_v1 section %s alignment=%d want %d", kind, alignment, want)
	}
	return nil
}

func columnHNSWSearchPackKnownSection(kind columnHNSWSearchPackSectionKind) bool {
	switch kind {
	case columnHNSWSearchPackSectionNormalizedVectors,
		columnHNSWSearchPackSectionLevels,
		columnHNSWSearchPackSectionAdjacencyOffsets,
		columnHNSWSearchPackSectionAdjacencyNeighbors,
		columnHNSWSearchPackSectionRowRefGeneration,
		columnHNSWSearchPackSectionRowRefPartID,
		columnHNSWSearchPackSectionRowRefRowIndex,
		columnHNSWSearchPackSectionRowRefAppliedLSN,
		columnHNSWSearchPackSectionDocumentIDOffsets,
		columnHNSWSearchPackSectionDocumentIDBytes,
		columnHNSWSearchPackSectionAuxiliaryOffsets,
		columnHNSWSearchPackSectionAuxiliaryNeighbors:
		return true
	default:
		return false
	}
}

type columnHNSWSearchPackSectionKey struct {
	kind  columnHNSWSearchPackSectionKind
	index uint16
}

type columnHNSWSearchPackSectionRange struct {
	start   uint64
	end     uint64
	section columnHNSWSearchPackSection
}

func decodeColumnHNSWSearchPackSectionEntry(raw []byte) (columnHNSWSearchPackSection, error) {
	if len(raw) < columnHNSWSearchPackSectionEntrySize {
		return columnHNSWSearchPackSection{}, errors.New("truncated section entry")
	}
	flags := hnswPackU16(raw, columnHNSWSearchPackEntryFlagsOffset)
	if flags != 0 {
		return columnHNSWSearchPackSection{}, fmt.Errorf("unsupported section flags=0x%x", flags)
	}
	return columnHNSWSearchPackSection{
		Kind:      columnHNSWSearchPackSectionKind(hnswPackU16(raw, columnHNSWSearchPackEntryKindOffset)),
		Index:     hnswPackU16(raw, columnHNSWSearchPackEntryIndexOffset),
		Alignment: uint32(hnswPackU16(raw, columnHNSWSearchPackEntryAlignmentOffset)),
		Offset:    hnswPackU64(raw, columnHNSWSearchPackEntrySectionOffset),
		Length:    hnswPackU64(raw, columnHNSWSearchPackEntryLengthOffset),
		Count:     hnswPackU64(raw, columnHNSWSearchPackEntryCountOffset),
		Checksum:  hnswPackU32(raw, columnHNSWSearchPackEntryChecksumOffset),
	}, nil
}

func encodeColumnHNSWSearchPackSectionEntry(raw []byte, section columnHNSWSearchPackSection) {
	putHNSWPackU16(raw, columnHNSWSearchPackEntryKindOffset, uint16(section.Kind))
	putHNSWPackU16(raw, columnHNSWSearchPackEntryIndexOffset, section.Index)
	putHNSWPackU16(raw, columnHNSWSearchPackEntryAlignmentOffset, uint16(section.Alignment))
	putHNSWPackU16(raw, columnHNSWSearchPackEntryFlagsOffset, 0)
	putHNSWPackU64(raw, columnHNSWSearchPackEntrySectionOffset, section.Offset)
	putHNSWPackU64(raw, columnHNSWSearchPackEntryLengthOffset, section.Length)
	putHNSWPackU64(raw, columnHNSWSearchPackEntryCountOffset, section.Count)
	putHNSWPackU32(raw, columnHNSWSearchPackEntryChecksumOffset, section.Checksum)
}

func checkedHNSWPackMulOK(left, right uint64) (uint64, bool) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, false
	}
	return left * right, true
}

func alignColumnHNSWSearchPackUint64(value, alignment uint64) (uint64, bool) {
	if alignment <= 1 {
		return value, true
	}
	rem := value % alignment
	if rem == 0 {
		return value, true
	}
	add := alignment - rem
	if value > math.MaxUint64-add {
		return 0, false
	}
	return value + add, true
}

func encodeFloat32SliceLE(values []float32) []byte {
	raw := make([]byte, len(values)*4)
	for i, value := range values {
		binary.LittleEndian.PutUint32(raw[i*4:], math.Float32bits(value))
	}
	return raw
}

func decodeFloat32SliceLE(raw []byte) []float32 {
	values := make([]float32, len(raw)/4)
	for i := range values {
		values[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[i*4:]))
	}
	return values
}

func encodeUint16SliceLE(values []uint16) []byte {
	raw := make([]byte, len(values)*2)
	for i, value := range values {
		binary.LittleEndian.PutUint16(raw[i*2:], value)
	}
	return raw
}

func decodeUint16SliceLE(raw []byte) []uint16 {
	values := make([]uint16, len(raw)/2)
	for i := range values {
		values[i] = binary.LittleEndian.Uint16(raw[i*2:])
	}
	return values
}

func encodeUint32SliceLE(values []uint32) []byte {
	raw := make([]byte, len(values)*4)
	for i, value := range values {
		binary.LittleEndian.PutUint32(raw[i*4:], value)
	}
	return raw
}

func decodeUint32SliceLE(raw []byte) []uint32 {
	values := make([]uint32, len(raw)/4)
	for i := range values {
		values[i] = binary.LittleEndian.Uint32(raw[i*4:])
	}
	return values
}

func encodeUint64SliceLE(values []uint64) []byte {
	raw := make([]byte, len(values)*8)
	for i, value := range values {
		binary.LittleEndian.PutUint64(raw[i*8:], value)
	}
	return raw
}

func decodeUint64SliceLE(raw []byte) []uint64 {
	values := make([]uint64, len(raw)/8)
	for i := range values {
		values[i] = binary.LittleEndian.Uint64(raw[i*8:])
	}
	return values
}

func encodeInt64SliceLE(values []int64) []byte {
	raw := make([]byte, len(values)*8)
	for i, value := range values {
		binary.LittleEndian.PutUint64(raw[i*8:], uint64(value))
	}
	return raw
}

func decodeInt64SliceLE(raw []byte) []int64 {
	values := make([]int64, len(raw)/8)
	for i := range values {
		values[i] = int64(binary.LittleEndian.Uint64(raw[i*8:]))
	}
	return values
}

func hnswPackU16(raw []byte, off int) uint16 { return binary.LittleEndian.Uint16(raw[off:]) }
func hnswPackU32(raw []byte, off int) uint32 { return binary.LittleEndian.Uint32(raw[off:]) }
func hnswPackU64(raw []byte, off int) uint64 { return binary.LittleEndian.Uint64(raw[off:]) }

func putHNSWPackU16(raw []byte, off int, value uint16) {
	binary.LittleEndian.PutUint16(raw[off:], value)
}
func putHNSWPackU32(raw []byte, off int, value uint32) {
	binary.LittleEndian.PutUint32(raw[off:], value)
}
func putHNSWPackU64(raw []byte, off int, value uint64) {
	binary.LittleEndian.PutUint64(raw[off:], value)
}
