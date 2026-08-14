package collections

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	internalcrc "github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

type columnHNSWSearchPackPreparedStatus string

const (
	columnHNSWSearchPackPreparedStatusMissing columnHNSWSearchPackPreparedStatus = "missing"
	columnHNSWSearchPackPreparedStatusDirect  columnHNSWSearchPackPreparedStatus = "direct"
	columnHNSWSearchPackPreparedStatusHeap    columnHNSWSearchPackPreparedStatus = "heap_copy"
	columnHNSWSearchPackPreparedStatusInvalid columnHNSWSearchPackPreparedStatus = "invalid"
	columnHNSWSearchPackPreparedStatusStale   columnHNSWSearchPackPreparedStatus = "stale"
	columnHNSWSearchPackPreparedStatusClosed  columnHNSWSearchPackPreparedStatus = "closed"
)

var (
	errColumnHNSWSearchPackPreparedViewClosed = errors.New("collections: hnsw_search_pack_v1 prepared view is closed")
	errColumnHNSWSearchPackPreparedViewStale  = errors.New("collections: hnsw_search_pack_v1 prepared view handle is stale")
)

type columnHNSWSearchPackPreparedLayer struct {
	Offsets   []uint64
	Neighbors []uint32
}

type columnHNSWSearchPackPreparedView struct {
	Header   columnHNSWSearchPackHeader
	Sections []columnHNSWSearchPackSection

	NormalizedVectors   []float32
	Levels              []uint16
	AdjacencyLayers     []columnHNSWSearchPackPreparedLayer
	AuxiliaryNavigation columnHNSWSearchPackPreparedLayer
	RowRefGenerations   []int64
	RowRefPartIDs       []int64
	RowRefRowIndexes    []int64
	RowRefAppliedLSNs   []int64
	DocumentIDOffsets   []uint64
	DocumentIDBytes     []byte

	manager *mappedresource.Manager
	handle  *mappedresource.Handle
	source  mappedresource.Source
	status  columnHNSWSearchPackPreparedStatus

	openNanos     uint64
	mappedBytes   uint64
	heapCopyBytes uint64
	activeHandles int64
	closeOnce     sync.Once
	closeErr      error
	closed        atomic.Bool
}

func (c *Collection) openColumnHNSWSearchPackPreparedViewForReader(collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot) (*columnHNSWSearchPackPreparedView, columnHNSWSearchPackPreparedStatus, uint64, error) {
	start := time.Now()
	view, status, err := c.openColumnHNSWSearchPackPreparedViewForReaderNoTimer(collection, cfg, def, graph, state)
	elapsedNanos := time.Since(start).Nanoseconds()
	if elapsedNanos < 0 {
		elapsedNanos = 0
	}
	openNanos := uint64(elapsedNanos)
	if view != nil && openNanos == 0 {
		// Windows timer granularity can report zero for tiny heap-copy test
		// fixtures. Keep the status counter observable for any opened view.
		openNanos = 1
	}
	if view != nil {
		view.openNanos = openNanos
	}
	return view, status, openNanos, err
}

func (c *Collection) openColumnHNSWSearchPackPreparedViewForReaderNoTimer(collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot) (*columnHNSWSearchPackPreparedView, columnHNSWSearchPackPreparedStatus, error) {
	asset, found, err := findColumnHNSWSearchPackStateAsset(state)
	if err != nil {
		return nil, columnHNSWSearchPackPreparedStatusInvalid, err
	}
	if !found {
		return nil, columnHNSWSearchPackPreparedStatusMissing, nil
	}
	if c == nil || c.db == nil {
		return nil, columnHNSWSearchPackPreparedStatusInvalid, errCollectionDBNil
	}
	if err := validateColumnHNSWSearchPackStateAssetIfPresentWithMode(c.db.ColumnAssetRootDir(), cfg, def, graph, state, false); err != nil {
		return nil, columnHNSWSearchPackPreparedStatusInvalid, err
	}
	path, err := columnAssetSegmentPath(c.db.ColumnAssetRootDir(), asset.Ref)
	if err != nil {
		return nil, columnHNSWSearchPackPreparedStatusInvalid, err
	}
	manager := mappedresource.NewManager()
	key := columnHNSWSearchPackMappedResourceKey(asset)
	scope := mappedresource.Scope{
		Kind:       mappedresource.ScopePreparedSearch,
		ID:         "hnsw_search_pack_v1/" + def.Name + "/" + strconv.FormatUint(graph.BaseManifestGeneration, 10),
		Collection: collection,
		Namespace:  asset.Ref.Namespace,
		Generation: graph.BaseManifestGeneration,
		Reason:     "hnsw_search_pack_v1 prepared view",
	}
	handle, err := manager.AcquireFileRange(key, scope, path, mappedresource.AcquireOptions{
		Reason:         "hnsw_search_pack_v1 prepared view",
		ValidationMode: mappedresource.ValidationVerify,
		PreferMapped:   true,
		AllowHeapCopy:  true,
		ResourceRoot:   c.db.ColumnAssetRootDir(),
		ResourcePath:   path,
	})
	if err != nil {
		return nil, columnHNSWSearchPackPreparedStatusInvalid, err
	}
	view, err := newColumnHNSWSearchPackPreparedViewFromHandle(manager, handle, columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: columnHNSWSearchPackBaseIdentity{
		ManifestGeneration: graph.BaseManifestGeneration,
		ManifestChecksum:   graph.BaseManifestChecksum,
		SchemaHash:         graph.BaseSchemaHash,
	}})
	if err != nil {
		releaseErr := handle.Release()
		return nil, columnHNSWSearchPackPreparedStatusInvalid, errors.Join(err, releaseErr)
	}
	if view.Header.Rows != graph.RowCount || view.Header.Dimensions != def.Dimensions || view.Header.M != def.M || view.Header.EfConstruction != def.EfConstruction || view.Header.EfSearch != def.EfSearch {
		releaseErr := view.Close()
		return nil, columnHNSWSearchPackPreparedStatusInvalid, errors.Join(fmt.Errorf("collections: hnsw_search_pack_v1 header rows/dims/M/ef=(%d,%d,%d,%d,%d) want (%d,%d,%d,%d,%d)", view.Header.Rows, view.Header.Dimensions, view.Header.M, view.Header.EfConstruction, view.Header.EfSearch, graph.RowCount, def.Dimensions, def.M, def.EfConstruction, def.EfSearch), releaseErr)
	}
	return view, view.status, nil
}

func columnHNSWSearchPackMappedResourceKey(asset columnVectorIndexStateAssetSnapshot) mappedresource.Key {
	return mappedresource.Key{
		Class:      mappedresource.ClassTypedColumnAsset,
		Namespace:  asset.Ref.Namespace,
		Kind:       string(asset.Ref.Kind),
		Generation: asset.Ref.Generation,
		PartID:     asset.Ref.PartID,
		FileID:     asset.Ref.FileID,
		Offset:     asset.Ref.Offset,
		Length:     asset.Ref.Length,
		Checksum:   uint64(asset.Ref.Checksum),
		Version:    columnHNSWSearchPackVersionV1,
		Encoding:   columnVectorIndexStateEncodingHNSWSearchPackV1,
		Section: mappedresource.Section{
			Kind:     string(columnVectorIndexStateAssetRoleHNSWSearchPack),
			Category: string(ColumnAssetKindTCS1HNSWSearchPack),
			Name:     asset.AssetID,
		},
	}
}

func newColumnHNSWSearchPackPreparedViewFromHandle(manager *mappedresource.Manager, handle *mappedresource.Handle, opts columnHNSWSearchPackDecodeOptions) (*columnHNSWSearchPackPreparedView, error) {
	return newColumnHNSWSearchPackPreparedViewFromHandleWithContext(context.Background(), manager, handle, opts)
}

func newColumnHNSWSearchPackPreparedViewFromHandleWithContext(ctx context.Context, manager *mappedresource.Manager, handle *mappedresource.Handle, opts columnHNSWSearchPackDecodeOptions) (*columnHNSWSearchPackPreparedView, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if manager == nil {
		return nil, errors.New("collections: hnsw_search_pack_v1 prepared view requires mapped resource manager")
	}
	if handle == nil || handle.Released() {
		return nil, errColumnHNSWSearchPackPreparedViewStale
	}
	raw := handle.Bytes()
	if len(raw) == 0 {
		return nil, errors.New("collections: hnsw_search_pack_v1 prepared view has empty bytes")
	}
	if key := handle.Key(); key.Checksum != 0 {
		got, err := columnHNSWSearchPackChecksumWithContext(ctx, raw)
		if err != nil {
			return nil, err
		}
		if got != uint32(key.Checksum) {
			return nil, fmt.Errorf("collections: hnsw_search_pack_v1 checksum=%08x want %08x", got, uint32(key.Checksum))
		}
	}
	pack, opts, err := decodeColumnHNSWSearchPackEnvelopeWithContext(ctx, raw, opts)
	if err != nil {
		return nil, err
	}
	view := &columnHNSWSearchPackPreparedView{
		Header:   pack.Header,
		Sections: append([]columnHNSWSearchPackSection(nil), pack.Sections...),
		manager:  manager,
		handle:   handle,
		source:   handle.Source(),
	}
	switch view.source {
	case mappedresource.SourceMapped:
		view.status = columnHNSWSearchPackPreparedStatusDirect
		view.mappedBytes = uint64(len(raw))
		view.activeHandles = 1
	case mappedresource.SourceHeapCopy:
		view.status = columnHNSWSearchPackPreparedStatusHeap
		view.heapCopyBytes = uint64(len(raw))
		view.activeHandles = 1
	default:
		return nil, fmt.Errorf("collections: hnsw_search_pack_v1 unsupported mapped resource source %q", view.source)
	}
	if err := view.prepareSectionViewsWithContext(ctx, raw, opts); err != nil {
		return nil, err
	}
	if err := view.validateLive(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return view, nil
}

func decodeColumnHNSWSearchPackEnvelope(raw []byte, opts columnHNSWSearchPackDecodeOptions) (columnHNSWSearchPack, columnHNSWSearchPackDecodeOptions, error) {
	return decodeColumnHNSWSearchPackEnvelopeWithContext(context.Background(), raw, opts)
}

func decodeColumnHNSWSearchPackEnvelopeWithContext(ctx context.Context, raw []byte, opts columnHNSWSearchPackDecodeOptions) (columnHNSWSearchPack, columnHNSWSearchPackDecodeOptions, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	opts = opts.withDefaults()
	if err := ctx.Err(); err != nil {
		return columnHNSWSearchPack{}, opts, err
	}
	if len(raw) < columnHNSWSearchPackHeaderSize {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: truncated hnsw_search_pack_v1 header bytes=%d want at least %d", len(raw), columnHNSWSearchPackHeaderSize)
	}
	if !bytes.Equal(raw[:8], columnHNSWSearchPackMagic[:]) {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: bad hnsw_search_pack_v1 magic=%q", string(raw[:8]))
	}
	version := hnswPackU16(raw, columnHNSWSearchPackHeaderVersionOffset)
	headerSize := columnHNSWSearchPackHeaderSize
	switch version {
	case columnHNSWSearchPackVersionV1:
	case columnHNSWSearchPackVersionV2, columnHNSWSearchPackVersionV3:
		headerSize = columnHNSWSearchPackHeaderSizeV2
	default:
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: unsupported hnsw_search_pack_v1 version=%d", version)
	}
	if len(raw) < headerSize {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: truncated hnsw_search_pack_v1 header bytes=%d want at least %d", len(raw), headerSize)
	}
	if got := hnswPackU16(raw, columnHNSWSearchPackHeaderHeaderSizeOffset); got != uint16(headerSize) {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 header_size=%d want %d", got, headerSize)
	}
	if got := hnswPackU16(raw, columnHNSWSearchPackHeaderSectionEntrySizeOffset); got != columnHNSWSearchPackSectionEntrySize {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 section_entry_size=%d want %d", got, columnHNSWSearchPackSectionEntrySize)
	}
	flags := hnswPackU16(raw, columnHNSWSearchPackHeaderFlagsOffset)
	if flags&^columnHNSWSearchPackFlagPreservedAuxiliary != 0 || flags != 0 && version != columnHNSWSearchPackVersionV3 {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 unsupported flags=0x%x", flags)
	}
	totalLength := hnswPackU64(raw, columnHNSWSearchPackHeaderTotalLengthOffset)
	if totalLength != uint64(len(raw)) {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 length=%d want header total_length=%d", len(raw), totalLength)
	}
	rows64 := hnswPackU64(raw, columnHNSWSearchPackHeaderRowsOffset)
	dims32 := hnswPackU32(raw, columnHNSWSearchPackHeaderDimensionsOffset)
	stride32 := hnswPackU32(raw, columnHNSWSearchPackHeaderVectorStrideOffset)
	maxLayer32 := hnswPackU32(raw, columnHNSWSearchPackHeaderMaxLayerOffset)
	layerCount32 := hnswPackU32(raw, columnHNSWSearchPackHeaderAdjacencyLayerCount)
	if rows64 > opts.MaxRows || rows64 > uint64(math.MaxInt) {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 row count=%d exceeds cap=%d", rows64, opts.MaxRows)
	}
	if dims32 == 0 || dims32 > opts.MaxDimensions || uint64(dims32) > uint64(math.MaxInt) {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 dimensions=%d outside cap=%d", dims32, opts.MaxDimensions)
	}
	if stride32 < dims32 || stride32 > opts.MaxVectorStride || uint64(stride32) > uint64(math.MaxInt) || (uint64(stride32)*4)%uint64(columnHNSWSearchPackVectorSectionAlignment) != 0 {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 vector stride=%d invalid for dimensions=%d", stride32, dims32)
	}
	if layerCount32 > opts.MaxLayers || uint64(layerCount32) > uint64(math.MaxInt) {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer count=%d exceeds cap=%d", layerCount32, opts.MaxLayers)
	}
	if metric := hnswPackU16(raw, columnHNSWSearchPackHeaderMetricOffset); metric != columnHNSWSearchPackMetricCosineNormalizedDot {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 metric=%d want cosine_normalized_dot", metric)
	}
	if encoding := hnswPackU16(raw, columnHNSWSearchPackHeaderEncodingOffset); encoding != columnHNSWSearchPackEncodingFloat32 {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 encoding=%d want float32", encoding)
	}
	if m := hnswPackU32(raw, columnHNSWSearchPackHeaderMOffset); m == 0 || uint64(m) > uint64(math.MaxInt) {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 M=%d must be positive", m)
	}
	if ef := hnswPackU32(raw, columnHNSWSearchPackHeaderEfConstructionOffset); ef == 0 || uint64(ef) > uint64(math.MaxInt) {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 ef_construction=%d must be positive", ef)
	}
	if ef := hnswPackU32(raw, columnHNSWSearchPackHeaderEfSearchOffset); ef == 0 || uint64(ef) > uint64(math.MaxInt) {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 ef_search=%d must be positive", ef)
	}
	if rows64 == 0 {
		if layerCount32 != 0 || maxLayer32 != columnHNSWSearchPackNoMaxLayer || hnswPackU64(raw, columnHNSWSearchPackHeaderEntryOrdinalOffset) != columnHNSWSearchPackNoEntryOrdinal {
			return columnHNSWSearchPack{}, opts, errors.New("collections: hnsw_search_pack_v1 empty pack must use no-entry/no-layer sentinels")
		}
	} else {
		entryOrdinal := hnswPackU64(raw, columnHNSWSearchPackHeaderEntryOrdinalOffset)
		if entryOrdinal >= rows64 {
			return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 entry ordinal=%d outside rows=%d", entryOrdinal, rows64)
		}
		if maxLayer32 == columnHNSWSearchPackNoMaxLayer || maxLayer32+1 != layerCount32 {
			return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 max_layer=%d layer_count=%d mismatch", maxLayer32, layerCount32)
		}
	}
	baseIdentity := columnHNSWSearchPackBaseIdentity{
		ManifestGeneration: hnswPackU64(raw, columnHNSWSearchPackHeaderBaseGenerationOffset),
		ManifestChecksum:   hnswPackU64(raw, columnHNSWSearchPackHeaderBaseChecksumOffset),
		SchemaHash:         hnswPackU64(raw, columnHNSWSearchPackHeaderBaseSchemaHashOffset),
	}
	if baseIdentity.ManifestGeneration == 0 || baseIdentity.ManifestChecksum == 0 || baseIdentity.SchemaHash == 0 {
		return columnHNSWSearchPack{}, opts, errors.New("collections: hnsw_search_pack_v1 missing base manifest identity")
	}
	if err := validateColumnHNSWSearchPackExpectedBaseIdentity(baseIdentity, opts.ExpectedBaseIdentity); err != nil {
		return columnHNSWSearchPack{}, opts, err
	}
	var membershipDigest [sha256.Size]byte
	if version == columnHNSWSearchPackVersionV2 || version == columnHNSWSearchPackVersionV3 {
		copy(membershipDigest[:], raw[columnHNSWSearchPackHeaderMembershipDigestOffset:columnHNSWSearchPackHeaderSizeV2])
		if membershipDigest == ([sha256.Size]byte{}) {
			return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 version %d missing membership digest", version)
		}
	}
	if opts.ExpectedMembershipDigest != ([sha256.Size]byte{}) && membershipDigest != opts.ExpectedMembershipDigest {
		return columnHNSWSearchPack{}, opts, errors.New("collections: hnsw_search_pack_v1 membership digest mismatch")
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
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 section_count=%d want %d", sectionCount32, expectedSectionCount)
	}
	maxSectionCount := uint32(8 + 2*opts.MaxLayers)
	if version == columnHNSWSearchPackVersionV3 {
		maxSectionCount += 2
	}
	if sectionCount32 > maxSectionCount {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 section_count=%d exceeds cap", sectionCount32)
	}
	if directoryOffset != uint64(headerSize) || directoryLength != uint64(sectionCount32)*columnHNSWSearchPackSectionEntrySize {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 corrupt section directory offset=%d length=%d count=%d", directoryOffset, directoryLength, sectionCount32)
	}
	if dataOffset < directoryOffset+directoryLength || dataOffset > totalLength || dataLength != totalLength-dataOffset {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 corrupt data region offset=%d length=%d total=%d", dataOffset, dataLength, totalLength)
	}
	if directoryOffset+directoryLength > uint64(len(raw)) {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 section directory exceeds pack length")
	}
	directory := raw[directoryOffset : directoryOffset+directoryLength]
	directoryChecksum, err := columnHNSWSearchPackChecksumWithContext(ctx, directory)
	if err != nil {
		return columnHNSWSearchPack{}, opts, err
	}
	if want := hnswPackU32(raw, columnHNSWSearchPackHeaderDirectoryChecksumOffset); directoryChecksum != want {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 section directory checksum=%08x want %08x", directoryChecksum, want)
	}
	sections := make([]columnHNSWSearchPackSection, int(sectionCount32))
	for i := range sections {
		if i&63 == 0 {
			if err := ctx.Err(); err != nil {
				return columnHNSWSearchPack{}, opts, err
			}
		}
		section, err := decodeColumnHNSWSearchPackSectionEntry(directory[i*columnHNSWSearchPackSectionEntrySize:])
		if err != nil {
			return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 section[%d]: %w", i, err)
		}
		sections[i] = section
	}
	if err := validateColumnHNSWSearchPackSectionDirectoryWithContext(ctx, raw, sections, dataOffset); err != nil {
		return columnHNSWSearchPack{}, opts, err
	}
	pack := columnHNSWSearchPack{
		Header: columnHNSWSearchPackHeader{
			Rows:                         int(rows64),
			Dimensions:                   int(dims32),
			VectorStride:                 int(stride32),
			M:                            int(hnswPackU32(raw, columnHNSWSearchPackHeaderMOffset)),
			EfConstruction:               int(hnswPackU32(raw, columnHNSWSearchPackHeaderEfConstructionOffset)),
			EfSearch:                     int(hnswPackU32(raw, columnHNSWSearchPackHeaderEfSearchOffset)),
			EntryOrdinal:                 int(hnswPackU64(raw, columnHNSWSearchPackHeaderEntryOrdinalOffset)),
			MaxLayer:                     int(maxLayer32),
			AdjacencyLayerCount:          int(layerCount32),
			BaseManifestGeneration:       baseIdentity.ManifestGeneration,
			BaseManifestChecksum:         baseIdentity.ManifestChecksum,
			BaseSchemaHash:               baseIdentity.SchemaHash,
			MembershipDigest:             membershipDigest,
			HasAuxiliaryNavigation:       version == columnHNSWSearchPackVersionV3,
			PreservedAuxiliaryNavigation: flags&columnHNSWSearchPackFlagPreservedAuxiliary != 0,
			TotalLength:                  totalLength,
			DataOffset:                   dataOffset,
			DataLength:                   dataLength,
		},
		Sections: sections,
	}
	if rows64 == 0 {
		pack.Header.EntryOrdinal = -1
		pack.Header.MaxLayer = -1
	}
	if err := ctx.Err(); err != nil {
		return columnHNSWSearchPack{}, opts, err
	}
	return pack, opts, nil
}

func columnHNSWSearchPackChecksumWithContext(ctx context.Context, raw []byte) (uint32, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var checksum uint32
	const chunkBytes = 1 << 20
	for start := 0; start < len(raw); start += chunkBytes {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		end := min(start+chunkBytes, len(raw))
		checksum = internalcrc.Update(checksum, raw[start:end])
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return checksum, nil
}

func validateColumnHNSWSearchPackSectionDirectoryWithContext(ctx context.Context, raw []byte, sections []columnHNSWSearchPackSection, dataOffset uint64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	seen := make(map[columnHNSWSearchPackSectionKey]struct{}, len(sections))
	ranges := make([]columnHNSWSearchPackSectionRange, 0, len(sections))
	for ordinal, section := range sections {
		if ordinal&63 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
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
		checksum, err := columnHNSWSearchPackChecksumWithContext(ctx, payload)
		if err != nil {
			return err
		}
		if checksum != section.Checksum {
			return fmt.Errorf("collections: hnsw_search_pack_v1 section %s[%d] checksum=%08x want %08x", section.Kind, section.Index, checksum, section.Checksum)
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
	return ctx.Err()
}

func (v *columnHNSWSearchPackPreparedView) prepareSectionViews(raw []byte, opts columnHNSWSearchPackDecodeOptions) error {
	return v.prepareSectionViewsWithContext(context.Background(), raw, opts)
}

func (v *columnHNSWSearchPackPreparedView) prepareSectionViewsWithContext(ctx context.Context, raw []byte, opts columnHNSWSearchPackDecodeOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	rows := uint64(v.Header.Rows)
	stride := uint64(v.Header.VectorStride)
	vectorCount, ok := checkedHNSWPackMulOK(rows, stride)
	if !ok {
		return errors.New("collections: hnsw_search_pack_v1 normalized vector count overflows uint64")
	}
	vectors, err := columnHNSWSearchPackRequireSection(v.Sections, columnHNSWSearchPackSectionNormalizedVectors, 0, vectorCount, 4)
	if err != nil {
		return err
	}
	vectorBytes, err := v.sectionDirectBytes(raw, vectors, 4, "normalized_vectors")
	if err != nil {
		return err
	}
	if v.NormalizedVectors, err = mappedresource.Float32View(vectorBytes); err != nil {
		return fmt.Errorf("collections: hnsw_search_pack_v1 normalized_vectors direct view: %w", err)
	}
	for i, value := range v.NormalizedVectors {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return fmt.Errorf("collections: hnsw_search_pack_v1 normalized vector[%d] is not finite", i)
		}
	}
	levels, err := columnHNSWSearchPackRequireSection(v.Sections, columnHNSWSearchPackSectionLevels, 0, rows, 2)
	if err != nil {
		return err
	}
	levelBytes, err := v.sectionDirectBytes(raw, levels, 2, "levels")
	if err != nil {
		return err
	}
	if v.Levels, err = mappedresource.Uint16View(levelBytes); err != nil {
		return fmt.Errorf("collections: hnsw_search_pack_v1 levels direct view: %w", err)
	}
	for ordinal, level := range v.Levels {
		if ordinal&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if int(level) > v.Header.MaxLayer {
			return fmt.Errorf("collections: hnsw_search_pack_v1 level[%d]=%d exceeds max_layer=%d", ordinal, level, v.Header.MaxLayer)
		}
	}
	v.AdjacencyLayers = make([]columnHNSWSearchPackPreparedLayer, v.Header.AdjacencyLayerCount)
	for layer := 0; layer < v.Header.AdjacencyLayerCount; layer++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		offsetsSection, err := columnHNSWSearchPackRequireSection(v.Sections, columnHNSWSearchPackSectionAdjacencyOffsets, uint16(layer), rows+1, 8)
		if err != nil {
			return err
		}
		offsetBytes, err := v.sectionDirectBytes(raw, offsetsSection, 8, fmt.Sprintf("adjacency_offsets[%d]", layer))
		if err != nil {
			return err
		}
		offsets, err := mappedresource.Uint64View(offsetBytes)
		if err != nil {
			return fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer=%d offsets direct view: %w", layer, err)
		}
		neighborsSection, err := columnHNSWSearchPackFindSection(v.Sections, columnHNSWSearchPackSectionAdjacencyNeighbors, uint16(layer))
		if err != nil {
			return err
		}
		if neighborsSection.Length%4 != 0 || neighborsSection.Count != neighborsSection.Length/4 {
			return fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer=%d neighbors length/count mismatch", layer)
		}
		if neighborsSection.Count > opts.MaxNeighbors || neighborsSection.Count > uint64(math.MaxInt) {
			return fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer=%d neighbors=%d exceeds cap=%d", layer, neighborsSection.Count, opts.MaxNeighbors)
		}
		neighborBytes, err := v.sectionDirectBytes(raw, neighborsSection, 4, fmt.Sprintf("adjacency_neighbors[%d]", layer))
		if err != nil {
			return err
		}
		neighbors, err := mappedresource.Uint32View(neighborBytes)
		if err != nil {
			return fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer=%d neighbors direct view: %w", layer, err)
		}
		if err := validateColumnHNSWSearchPackAdjacencyWithContext(ctx, layer, rows, offsets, neighbors); err != nil {
			return err
		}
		v.AdjacencyLayers[layer] = columnHNSWSearchPackPreparedLayer{Offsets: offsets, Neighbors: neighbors}
	}
	if v.Header.HasAuxiliaryNavigation {
		offsetsSection, err := columnHNSWSearchPackRequireSection(v.Sections, columnHNSWSearchPackSectionAuxiliaryOffsets, 0, rows+1, 8)
		if err != nil {
			return err
		}
		offsetBytes, err := v.sectionDirectBytes(raw, offsetsSection, 8, "auxiliary_offsets")
		if err != nil {
			return err
		}
		offsets, err := mappedresource.Uint64View(offsetBytes)
		if err != nil {
			return fmt.Errorf("collections: hnsw_search_pack_v1 auxiliary offsets direct view: %w", err)
		}
		neighborsSection, err := columnHNSWSearchPackFindSection(v.Sections, columnHNSWSearchPackSectionAuxiliaryNeighbors, 0)
		if err != nil {
			return err
		}
		maxAuxiliaryNeighbors := uint64(0)
		if v.Header.Rows > 1 {
			maxAuxiliaryNeighbors = uint64(v.Header.Rows-1) * 2
		}
		if neighborsSection.Length%4 != 0 || neighborsSection.Count != neighborsSection.Length/4 || neighborsSection.Count > maxAuxiliaryNeighbors || neighborsSection.Count > opts.MaxNeighbors || neighborsSection.Count > uint64(math.MaxInt) {
			return errors.New("collections: hnsw_search_pack_v1 auxiliary neighbors shape")
		}
		neighborBytes, err := v.sectionDirectBytes(raw, neighborsSection, 4, "auxiliary_neighbors")
		if err != nil {
			return err
		}
		neighbors, err := mappedresource.Uint32View(neighborBytes)
		if err != nil {
			return fmt.Errorf("collections: hnsw_search_pack_v1 auxiliary neighbors direct view: %w", err)
		}
		if v.Header.Rows > 0 && len(v.AdjacencyLayers) == 0 {
			return errors.New("collections: hnsw_search_pack_v1 auxiliary navigation requires layer 0")
		}
		var native columnHNSWSearchPackPreparedLayer
		if len(v.AdjacencyLayers) != 0 {
			native = v.AdjacencyLayers[0]
		}
		auxiliary := vectorPartitionLocalAuxiliaryNavigationV1{Offsets: offsets, Neighbors: neighbors}
		var validationErr error
		if v.Header.PreservedAuxiliaryNavigation {
			validationErr = validateVectorPartitionPreservedAuxiliaryNavigationV1(v.Header.Rows, v.Header.EntryOrdinal, native.Offsets, native.Neighbors, auxiliary)
		} else {
			validationErr = validateVectorPartitionLocalAuxiliaryNavigationFromNativeLayer0WithContextV1(ctx, v.Header.Rows, v.Header.EntryOrdinal, v.Levels, native.Offsets, native.Neighbors, auxiliary)
		}
		if validationErr != nil {
			return fmt.Errorf("collections: hnsw_search_pack_v1 auxiliary navigation: %w", validationErr)
		}
		v.AuxiliaryNavigation = columnHNSWSearchPackPreparedLayer{Offsets: offsets, Neighbors: neighbors}
	}
	if v.RowRefGenerations, err = v.int64DirectView(raw, columnHNSWSearchPackSectionRowRefGeneration, rows); err != nil {
		return err
	}
	if v.RowRefPartIDs, err = v.int64DirectView(raw, columnHNSWSearchPackSectionRowRefPartID, rows); err != nil {
		return err
	}
	if v.RowRefRowIndexes, err = v.int64DirectView(raw, columnHNSWSearchPackSectionRowRefRowIndex, rows); err != nil {
		return err
	}
	if v.RowRefAppliedLSNs, err = v.int64DirectView(raw, columnHNSWSearchPackSectionRowRefAppliedLSN, rows); err != nil {
		return err
	}
	for ordinal := 0; ordinal < v.Header.Rows; ordinal++ {
		if ordinal&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if v.RowRefGenerations[ordinal] <= 0 || v.RowRefPartIDs[ordinal] <= 0 || v.RowRefAppliedLSNs[ordinal] <= 0 || v.RowRefRowIndexes[ordinal] < 0 {
			return fmt.Errorf("collections: hnsw_search_pack_v1 invalid row-ref ordinal=%d", ordinal)
		}
		if uint64(v.RowRefGenerations[ordinal]) > v.Header.BaseManifestGeneration {
			return fmt.Errorf("collections: hnsw_search_pack_v1 row-ref generation=%d exceeds base generation=%d", v.RowRefGenerations[ordinal], v.Header.BaseManifestGeneration)
		}
	}
	docOffsetsSection, err := columnHNSWSearchPackRequireSection(v.Sections, columnHNSWSearchPackSectionDocumentIDOffsets, 0, rows+1, 8)
	if err != nil {
		return err
	}
	docOffsetBytes, err := v.sectionDirectBytes(raw, docOffsetsSection, 8, "document_id_offsets")
	if err != nil {
		return err
	}
	if v.DocumentIDOffsets, err = mappedresource.Uint64View(docOffsetBytes); err != nil {
		return fmt.Errorf("collections: hnsw_search_pack_v1 document_id_offsets direct view: %w", err)
	}
	docBytesSection, err := columnHNSWSearchPackFindSection(v.Sections, columnHNSWSearchPackSectionDocumentIDBytes, 0)
	if err != nil {
		return err
	}
	if docBytesSection.Count != docBytesSection.Length {
		return fmt.Errorf("collections: hnsw_search_pack_v1 document_id_bytes length/count mismatch")
	}
	if docBytesSection.Length > opts.MaxDocumentIDBytes || docBytesSection.Length > uint64(math.MaxInt) {
		return fmt.Errorf("collections: hnsw_search_pack_v1 document_id_bytes=%d exceeds cap=%d", docBytesSection.Length, opts.MaxDocumentIDBytes)
	}
	if v.DocumentIDBytes, err = v.sectionDirectBytes(raw, docBytesSection, 1, "document_id_bytes"); err != nil {
		return err
	}
	return validateColumnHNSWSearchPackDocumentIDsWithContext(ctx, rows, v.DocumentIDOffsets, v.DocumentIDBytes)
}

func validateColumnHNSWSearchPackAdjacencyWithContext(ctx context.Context, layer int, rows uint64, offsets []uint64, neighbors []uint32) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if uint64(len(offsets)) != rows+1 {
		return fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer=%d offsets=%d want rows+1=%d", layer, len(offsets), rows+1)
	}
	if offsets[0] != 0 {
		return fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer=%d first offset=%d want 0", layer, offsets[0])
	}
	for i := 1; i < len(offsets); i++ {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if offsets[i] < offsets[i-1] {
			return fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer=%d offsets are not monotonic at row=%d", layer, i-1)
		}
	}
	if offsets[len(offsets)-1] != uint64(len(neighbors)) {
		return fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer=%d last offset=%d want neighbors=%d", layer, offsets[len(offsets)-1], len(neighbors))
	}
	for i, neighbor := range neighbors {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if uint64(neighbor) >= rows {
			return fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer=%d neighbor ordinal[%d]=%d outside rows=%d", layer, i, neighbor, rows)
		}
	}
	return ctx.Err()
}

func validateColumnHNSWSearchPackDocumentIDsWithContext(ctx context.Context, rows uint64, offsets []uint64, values []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if uint64(len(offsets)) != rows+1 {
		return fmt.Errorf("collections: hnsw_search_pack_v1 document id offsets=%d want rows+1=%d", len(offsets), rows+1)
	}
	if offsets[0] != 0 {
		return fmt.Errorf("collections: hnsw_search_pack_v1 document id first offset=%d want 0", offsets[0])
	}
	for i := uint64(0); i < rows; i++ {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
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
	return ctx.Err()
}

func (v *columnHNSWSearchPackPreparedView) int64DirectView(raw []byte, kind columnHNSWSearchPackSectionKind, rows uint64) ([]int64, error) {
	section, err := columnHNSWSearchPackRequireSection(v.Sections, kind, 0, rows, 8)
	if err != nil {
		return nil, err
	}
	sectionBytes, err := v.sectionDirectBytes(raw, section, 8, kind.String())
	if err != nil {
		return nil, err
	}
	view, err := mappedresource.Int64View(sectionBytes)
	if err != nil {
		return nil, fmt.Errorf("collections: hnsw_search_pack_v1 %s direct view: %w", kind, err)
	}
	return view, nil
}

func (v *columnHNSWSearchPackPreparedView) sectionDirectBytes(raw []byte, section columnHNSWSearchPackSection, elemBytes uintptr, typeName string) ([]byte, error) {
	sectionBytes := columnHNSWSearchPackSectionBytes(raw, section)
	alignment := elemBytes
	if v != nil && v.source == mappedresource.SourceMapped {
		alignment = uintptr(section.Alignment)
	}
	if _, err := mappedresource.ValidateDirectView(sectionBytes, mappedresource.DirectViewOptions{ElementSize: elemBytes, Alignment: alignment, TypeName: "hnsw_search_pack_v1 " + typeName, RequireLittleEndian: elemBytes > 1}); err != nil {
		return nil, err
	}
	return sectionBytes, nil
}

func columnHNSWSearchPackSectionBytes(raw []byte, section columnHNSWSearchPackSection) []byte {
	return raw[int(section.Offset):int(section.Offset+section.Length)]
}

func (v *columnHNSWSearchPackPreparedView) fastStatus(defaultStatus columnHNSWSearchPackPreparedStatus) columnHNSWSearchPackPreparedStatus {
	if v == nil {
		if defaultStatus == "" {
			return columnHNSWSearchPackPreparedStatusMissing
		}
		return defaultStatus
	}
	if v.closed.Load() {
		return columnHNSWSearchPackPreparedStatusClosed
	}
	if v.handle == nil || v.handle.Released() {
		return columnHNSWSearchPackPreparedStatusStale
	}
	switch v.status {
	case columnHNSWSearchPackPreparedStatusDirect, columnHNSWSearchPackPreparedStatusHeap, columnHNSWSearchPackPreparedStatusInvalid, columnHNSWSearchPackPreparedStatusStale, columnHNSWSearchPackPreparedStatusClosed, columnHNSWSearchPackPreparedStatusMissing:
		return v.status
	default:
		return columnHNSWSearchPackPreparedStatusInvalid
	}
}

func (v *columnHNSWSearchPackPreparedView) validateLive() error {
	if v == nil {
		return columnHNSWSearchPackStatusError(columnHNSWSearchPackPreparedStatusMissing)
	}
	if v.closed.Load() {
		return errColumnHNSWSearchPackPreparedViewClosed
	}
	if v.handle == nil || v.handle.Released() || v.handle.Bytes() == nil {
		return errColumnHNSWSearchPackPreparedViewStale
	}
	if v.Header.Rows < 0 || v.Header.Dimensions <= 0 || v.Header.VectorStride < v.Header.Dimensions {
		return fmt.Errorf("collections: hnsw_search_pack_v1 prepared view invalid header rows/dims/stride=(%d,%d,%d)", v.Header.Rows, v.Header.Dimensions, v.Header.VectorStride)
	}
	wantVectors := v.Header.Rows * v.Header.VectorStride
	if len(v.NormalizedVectors) != wantVectors || len(v.Levels) != v.Header.Rows || len(v.RowRefGenerations) != v.Header.Rows || len(v.RowRefPartIDs) != v.Header.Rows || len(v.RowRefRowIndexes) != v.Header.Rows || len(v.RowRefAppliedLSNs) != v.Header.Rows || len(v.DocumentIDOffsets) != v.Header.Rows+1 {
		return fmt.Errorf("collections: hnsw_search_pack_v1 prepared view section lengths vectors=%d levels=%d rowrefs=(%d,%d,%d,%d) doc_offsets=%d rows=%d stride=%d", len(v.NormalizedVectors), len(v.Levels), len(v.RowRefGenerations), len(v.RowRefPartIDs), len(v.RowRefRowIndexes), len(v.RowRefAppliedLSNs), len(v.DocumentIDOffsets), v.Header.Rows, v.Header.VectorStride)
	}
	if len(v.AdjacencyLayers) != v.Header.AdjacencyLayerCount {
		return fmt.Errorf("collections: hnsw_search_pack_v1 prepared adjacency layers=%d want %d", len(v.AdjacencyLayers), v.Header.AdjacencyLayerCount)
	}
	if v.Header.HasAuxiliaryNavigation && (len(v.AuxiliaryNavigation.Offsets) != v.Header.Rows+1 || len(v.AdjacencyLayers) == 0) {
		return errors.New("collections: hnsw_search_pack_v1 prepared auxiliary navigation unavailable")
	}
	return nil
}

func (v *columnHNSWSearchPackPreparedView) Close() error {
	if v == nil {
		return nil
	}
	v.closeOnce.Do(func() {
		v.closed.Store(true)
		if v.handle != nil {
			v.closeErr = v.handle.Release()
			v.handle = nil
		}
		v.activeHandles = 0
	})
	return v.closeErr
}

func (v *columnHNSWSearchPackPreparedView) mappedResourceStats() mappedresource.Stats {
	if v == nil || v.manager == nil {
		return mappedresource.Stats{}
	}
	return v.manager.Stats()
}

func (v *columnHNSWSearchPackPreparedView) routeStats(defaultStatus columnHNSWSearchPackPreparedStatus, openNanos uint64) vectorIndexSearchRouteStats {
	stats := vectorIndexSearchRouteStats{HNSWSearchPackOpenNanos: openNanos}
	if openNanos == 0 && v != nil {
		stats.HNSWSearchPackOpenNanos = v.openNanos
	}
	if v == nil {
		stats.applyHNSWSearchPackStatus(defaultStatus)
		return stats
	}
	if err := v.validateLive(); err != nil {
		if errors.Is(err, errColumnHNSWSearchPackPreparedViewClosed) {
			stats.applyHNSWSearchPackStatus(columnHNSWSearchPackPreparedStatusClosed)
			return stats
		}
		if errors.Is(err, errColumnHNSWSearchPackPreparedViewStale) {
			stats.applyHNSWSearchPackStatus(columnHNSWSearchPackPreparedStatusStale)
			return stats
		}
		stats.applyHNSWSearchPackStatus(columnHNSWSearchPackPreparedStatusInvalid)
		return stats
	}
	stats.applyHNSWSearchPackStatus(v.status)
	stats.HNSWSearchPackMappedBytes = v.mappedBytes
	stats.HNSWSearchPackHeapCopyBytes = v.heapCopyBytes
	stats.HNSWSearchPackActiveHandles = v.activeHandles
	return stats
}

func (r *vectorIndexSearchRouteStats) add(other vectorIndexSearchRouteStats) {
	if r == nil {
		return
	}
	r.SearchRouteColumnGraphPrepared += other.SearchRouteColumnGraphPrepared
	r.SearchRouteColumnGraphFallback += other.SearchRouteColumnGraphFallback
	r.SearchRouteHNSWSearchPack += other.SearchRouteHNSWSearchPack
	r.HNSWSearchPackActive += other.HNSWSearchPackActive
	r.HNSWSearchPackMissing += other.HNSWSearchPackMissing
	r.HNSWSearchPackInvalid += other.HNSWSearchPackInvalid
	r.HNSWSearchPackStale += other.HNSWSearchPackStale
	r.HNSWSearchPackClosed += other.HNSWSearchPackClosed
	r.HNSWSearchPackFallbacks += other.HNSWSearchPackFallbacks
	r.HNSWSearchPackMmapDirect += other.HNSWSearchPackMmapDirect
	r.HNSWSearchPackHeapCopy += other.HNSWSearchPackHeapCopy
	if r.HNSWSearchPackOpenNanos == 0 {
		r.HNSWSearchPackOpenNanos = other.HNSWSearchPackOpenNanos
	}
	r.HNSWSearchPackMappedBytes += other.HNSWSearchPackMappedBytes
	r.HNSWSearchPackHeapCopyBytes += other.HNSWSearchPackHeapCopyBytes
	r.HNSWSearchPackActiveHandles += other.HNSWSearchPackActiveHandles
}

func (r *vectorIndexSearchRouteStats) applyHNSWSearchPackStatus(status columnHNSWSearchPackPreparedStatus) {
	if r == nil {
		return
	}
	switch status {
	case columnHNSWSearchPackPreparedStatusDirect:
		r.HNSWSearchPackActive = 1
		r.HNSWSearchPackMmapDirect = 1
	case columnHNSWSearchPackPreparedStatusHeap:
		r.HNSWSearchPackActive = 1
		r.HNSWSearchPackHeapCopy = 1
	case columnHNSWSearchPackPreparedStatusInvalid:
		r.HNSWSearchPackInvalid = 1
		r.HNSWSearchPackFallbacks = 1
	case columnHNSWSearchPackPreparedStatusStale:
		r.HNSWSearchPackStale = 1
		r.HNSWSearchPackFallbacks = 1
	case columnHNSWSearchPackPreparedStatusClosed:
		r.HNSWSearchPackClosed = 1
		r.HNSWSearchPackFallbacks = 1
	case columnHNSWSearchPackPreparedStatusMissing, "":
		r.HNSWSearchPackMissing = 1
	default:
		r.HNSWSearchPackInvalid = 1
		r.HNSWSearchPackFallbacks = 1
	}
}

type columnHNSWSearchPackStatusError columnHNSWSearchPackPreparedStatus

func (e columnHNSWSearchPackStatusError) Error() string {
	return "collections: hnsw_search_pack_v1 prepared status " + string(e)
}
