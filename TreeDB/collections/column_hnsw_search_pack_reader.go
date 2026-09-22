package collections

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

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

type columnHNSWSearchPackPreparedChunks[T any] struct {
	values [][]T
	starts []uint64
	length uint64
}

func newColumnHNSWSearchPackPreparedChunks[T any](values [][]T) columnHNSWSearchPackPreparedChunks[T] {
	out := columnHNSWSearchPackPreparedChunks[T]{values: values, starts: make([]uint64, len(values)+1)}
	for i := range values {
		out.length += uint64(len(values[i]))
		out.starts[i+1] = out.length
	}
	return out
}

func columnHNSWSearchPackPreparedChunkMetadataBytes[T any](chunks columnHNSWSearchPackPreparedChunks[T]) uint64 {
	return uint64(cap(chunks.values))*uint64(unsafe.Sizeof([]T(nil))) +
		uint64(cap(chunks.starts))*uint64(unsafe.Sizeof(uint64(0)))
}

func (c columnHNSWSearchPackPreparedChunks[T]) at(index uint64) (T, bool) {
	var zero T
	if index >= c.length || len(c.values) == 0 {
		return zero, false
	}
	chunk := sort.Search(len(c.values), func(i int) bool { return c.starts[i+1] > index })
	if chunk >= len(c.values) {
		return zero, false
	}
	return c.values[chunk][index-c.starts[chunk]], true
}

func (c columnHNSWSearchPackPreparedChunks[T]) span(start, end uint64) ([]T, bool) {
	if end < start || end > c.length {
		return nil, false
	}
	if start == end {
		return nil, true
	}
	chunk := sort.Search(len(c.values), func(i int) bool { return c.starts[i+1] > start })
	if chunk >= len(c.values) || end > c.starts[chunk+1] {
		return nil, false
	}
	return c.values[chunk][start-c.starts[chunk] : end-c.starts[chunk]], true
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

	normalizedVectorChunks  columnHNSWSearchPackPreparedChunks[float32]
	levelChunks             columnHNSWSearchPackPreparedChunks[uint16]
	adjacencyOffsetChunks   []columnHNSWSearchPackPreparedChunks[uint64]
	adjacencyNeighborChunks []columnHNSWSearchPackPreparedChunks[uint32]
	rowRefGenerationChunks  columnHNSWSearchPackPreparedChunks[int64]
	rowRefPartIDChunks      columnHNSWSearchPackPreparedChunks[int64]
	rowRefRowIndexChunks    columnHNSWSearchPackPreparedChunks[int64]
	rowRefAppliedLSNChunks  columnHNSWSearchPackPreparedChunks[int64]
	documentIDOffsetChunks  columnHNSWSearchPackPreparedChunks[uint64]
	documentIDByteChunks    columnHNSWSearchPackPreparedChunks[byte]

	manager      *mappedresource.Manager
	handle       *mappedresource.Handle
	handles      []*mappedresource.Handle
	sectionBytes map[columnHNSWSearchPackSectionKey][]byte
	source       mappedresource.Source
	status       columnHNSWSearchPackPreparedStatus
	// Process-local derived adjacency views borrow vectors from another pinned
	// pack, so they intentionally have no mapped-resource handle.
	ephemeralHeap bool

	openNanos      uint64
	mappedBytes    uint64
	heapCopyBytes  uint64
	chunkMetaBytes uint64
	activeHandles  int64
	closeOnce      sync.Once
	closeErr       error
	closed         atomic.Bool
}

func (c *Collection) openColumnHNSWSearchPackPreparedViewForReader(collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot) (*columnHNSWSearchPackPreparedView, columnHNSWSearchPackPreparedStatus, uint64, error) {
	return c.openColumnHNSWSearchPackPreparedViewForReaderWithSourceAccess(collection, cfg, def, graph, state, nil)
}

func (c *Collection) openColumnHNSWSearchPackPreparedViewForReaderWithSourceAccess(collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot, access *columnVectorGraphSourceAccess) (*columnHNSWSearchPackPreparedView, columnHNSWSearchPackPreparedStatus, uint64, error) {
	start := time.Now()
	view, status, err := c.openColumnHNSWSearchPackPreparedViewForReaderNoTimerWithSourceAccess(collection, cfg, def, graph, state, access)
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
	return c.openColumnHNSWSearchPackPreparedViewForReaderNoTimerWithSourceAccess(collection, cfg, def, graph, state, nil)
}

func (c *Collection) openColumnHNSWSearchPackPreparedViewForReaderNoTimerWithSourceAccess(collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot, access *columnVectorGraphSourceAccess) (*columnHNSWSearchPackPreparedView, columnHNSWSearchPackPreparedStatus, error) {
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
	opts := mappedresource.AcquireOptions{
		Reason:         "hnsw_search_pack_v1 prepared view",
		ValidationMode: mappedresource.ValidationVerify,
		PreferMapped:   true,
		AllowHeapCopy:  true,
		ResourceRoot:   c.db.ColumnAssetRootDir(),
	}
	var handle *mappedresource.Handle
	if access != nil {
		handle, err = access.acquireRange(nil, c.db.ColumnAssetRootDir(), asset.Ref, manager, key, scope, opts)
	} else {
		path, pathErr := columnAssetSegmentPath(c.db.ColumnAssetRootDir(), asset.Ref)
		if pathErr != nil {
			return nil, columnHNSWSearchPackPreparedStatusInvalid, pathErr
		}
		opts.ResourcePath = path
		handle, err = manager.AcquireFileRange(key, scope, path, opts)
	}
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
	version := columnHNSWSearchPackVersionV1
	encoding := asset.PhysicalEncoding
	if asset.AssetID == columnVectorIndexStateHNSWTopologyPackAssetID || encoding == columnVectorIndexStateEncodingHNSWSearchPackV2 {
		version = columnHNSWSearchPackVersionV4
		encoding = columnVectorIndexStateEncodingHNSWSearchPackV2
	}
	if encoding == "" {
		encoding = columnVectorIndexStateEncodingHNSWSearchPackV1
	}
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
		Version:    version,
		Encoding:   encoding,
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
		view.mappedBytes = uint64(handle.AccountedBytes())
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
	view.bindFlatChunkAccessors()
	if err := view.validateTopologyLive(); err != nil {
		return nil, err
	}
	if !view.Header.ExternalNormalizedVectors {
		if err := view.validateLive(); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return view, nil
}

func newColumnHNSWSearchPackPreparedViewFromSectionHandlesWithContext(ctx context.Context, manager *mappedresource.Manager, root *mappedresource.Handle, sections map[columnHNSWSearchPackSectionKey][]*mappedresource.Handle, opts columnHNSWSearchPackDecodeOptions) (*columnHNSWSearchPackPreparedView, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if manager == nil || root == nil || root.Released() {
		return nil, errColumnHNSWSearchPackPreparedViewStale
	}
	raw := root.Bytes()
	if len(raw) < columnHNSWSearchPackHeaderSizeV2 {
		return nil, errors.New("collections: chunked hnsw_search_pack_v1 root is truncated")
	}
	totalLength := hnswPackU64(raw, columnHNSWSearchPackHeaderTotalLengthOffset)
	pack, opts, err := decodeColumnHNSWSearchPackEnvelopeMetadataWithContext(ctx, raw, totalLength, opts, false)
	if err != nil {
		return nil, err
	}
	if pack.Header.Version != columnHNSWSearchPackVersionV6 {
		return nil, errors.New("collections: chunked hnsw_search_pack_v1 version")
	}
	if err := validateColumnHNSWSearchPackChunkedGeometry(raw, pack); err != nil {
		return nil, err
	}
	allHandles := make([]*mappedresource.Handle, 0)
	view := &columnHNSWSearchPackPreparedView{
		Header: pack.Header, Sections: append([]columnHNSWSearchPackSection(nil), pack.Sections...),
		manager: manager, handle: root, source: root.Source(),
	}
	allMapped := view.source == mappedresource.SourceMapped
	sectionChunks := make(map[columnHNSWSearchPackSectionKey][][]byte, len(pack.Sections))
	expectedSections := make(map[columnHNSWSearchPackSectionKey]struct{}, len(pack.Sections))
	for _, section := range pack.Sections {
		key := columnHNSWSearchPackSectionKey{kind: section.Kind, index: section.Index}
		expectedSections[key] = struct{}{}
		handles := sections[key]
		var length uint64
		var checksum uint32
		for _, h := range handles {
			if h == nil || h.Released() || len(h.Bytes()) == 0 || uint64(len(h.Bytes())) > section.Length-length {
				return nil, fmt.Errorf("collections: chunked hnsw_search_pack_v1 section %s[%d] extent", section.Kind, section.Index)
			}
			bytes := h.Bytes()
			length += uint64(len(bytes))
			checksum = internalcrc.Update(checksum, bytes)
			sectionChunks[key] = append(sectionChunks[key], bytes)
			allHandles = append(allHandles, h)
			if h.Source() == mappedresource.SourceMapped {
				view.mappedBytes += uint64(h.AccountedBytes())
			} else if h.Source() == mappedresource.SourceHeapCopy {
				view.heapCopyBytes += uint64(len(bytes))
				allMapped = false
			} else {
				return nil, fmt.Errorf("collections: chunked hnsw_search_pack_v1 unsupported source %q", h.Source())
			}
		}
		if length != section.Length || checksum != section.Checksum {
			return nil, fmt.Errorf("collections: chunked hnsw_search_pack_v1 section %s[%d] exact cover", section.Kind, section.Index)
		}
	}
	for key, handles := range sections {
		if _, ok := expectedSections[key]; !ok && len(handles) != 0 {
			return nil, fmt.Errorf("collections: chunked hnsw_search_pack_v1 foreign section %s[%d]", key.kind, key.index)
		}
	}
	view.handles = allHandles
	if view.source == mappedresource.SourceMapped {
		view.mappedBytes += uint64(root.AccountedBytes())
	} else if view.source == mappedresource.SourceHeapCopy {
		view.heapCopyBytes += uint64(len(raw))
		allMapped = false
	} else {
		return nil, fmt.Errorf("collections: chunked hnsw_search_pack_v1 unsupported root source %q", view.source)
	}
	view.activeHandles = int64(len(allHandles) + 1)
	if allMapped {
		view.status = columnHNSWSearchPackPreparedStatusDirect
	} else {
		view.status = columnHNSWSearchPackPreparedStatusHeap
		view.source = mappedresource.SourceHeapCopy
	}
	if err := view.prepareChunkedV6SectionViewsWithContext(ctx, sectionChunks, opts); err != nil {
		return nil, err
	}
	view.chunkMetaBytes = view.retainedChunkMetadataBytes()
	if err := view.validateLive(); err != nil {
		return nil, err
	}
	return view, ctx.Err()
}

func (v *columnHNSWSearchPackPreparedView) retainedChunkMetadataBytes() uint64 {
	if v == nil {
		return 0
	}
	bytes := uint64(cap(v.Sections))*uint64(unsafe.Sizeof(columnHNSWSearchPackSection{})) +
		uint64(cap(v.handles))*uint64(unsafe.Sizeof((*mappedresource.Handle)(nil))) +
		uint64(len(v.handles)+1)*mappedresource.ConservativeHandleMetadataBytes() +
		uint64(cap(v.AdjacencyLayers))*uint64(unsafe.Sizeof(columnHNSWSearchPackPreparedLayer{})) +
		uint64(cap(v.adjacencyOffsetChunks))*uint64(unsafe.Sizeof(columnHNSWSearchPackPreparedChunks[uint64]{})) +
		uint64(cap(v.adjacencyNeighborChunks))*uint64(unsafe.Sizeof(columnHNSWSearchPackPreparedChunks[uint32]{}))
	bytes += columnHNSWSearchPackPreparedChunkMetadataBytes(v.normalizedVectorChunks)
	bytes += columnHNSWSearchPackPreparedChunkMetadataBytes(v.levelChunks)
	bytes += columnHNSWSearchPackPreparedChunkMetadataBytes(v.rowRefGenerationChunks)
	bytes += columnHNSWSearchPackPreparedChunkMetadataBytes(v.rowRefPartIDChunks)
	bytes += columnHNSWSearchPackPreparedChunkMetadataBytes(v.rowRefRowIndexChunks)
	bytes += columnHNSWSearchPackPreparedChunkMetadataBytes(v.rowRefAppliedLSNChunks)
	bytes += columnHNSWSearchPackPreparedChunkMetadataBytes(v.documentIDOffsetChunks)
	bytes += columnHNSWSearchPackPreparedChunkMetadataBytes(v.documentIDByteChunks)
	for i := range v.adjacencyOffsetChunks {
		bytes += columnHNSWSearchPackPreparedChunkMetadataBytes(v.adjacencyOffsetChunks[i])
		bytes += columnHNSWSearchPackPreparedChunkMetadataBytes(v.adjacencyNeighborChunks[i])
	}
	return bytes
}

func (v *columnHNSWSearchPackPreparedView) bindFlatChunkAccessors() {
	if v == nil {
		return
	}
	v.normalizedVectorChunks = newColumnHNSWSearchPackPreparedChunks([][]float32{v.NormalizedVectors})
	v.levelChunks = newColumnHNSWSearchPackPreparedChunks([][]uint16{v.Levels})
	v.adjacencyOffsetChunks = make([]columnHNSWSearchPackPreparedChunks[uint64], len(v.AdjacencyLayers))
	v.adjacencyNeighborChunks = make([]columnHNSWSearchPackPreparedChunks[uint32], len(v.AdjacencyLayers))
	for i, layer := range v.AdjacencyLayers {
		v.adjacencyOffsetChunks[i] = newColumnHNSWSearchPackPreparedChunks([][]uint64{layer.Offsets})
		v.adjacencyNeighborChunks[i] = newColumnHNSWSearchPackPreparedChunks([][]uint32{layer.Neighbors})
	}
	v.rowRefGenerationChunks = newColumnHNSWSearchPackPreparedChunks([][]int64{v.RowRefGenerations})
	v.rowRefPartIDChunks = newColumnHNSWSearchPackPreparedChunks([][]int64{v.RowRefPartIDs})
	v.rowRefRowIndexChunks = newColumnHNSWSearchPackPreparedChunks([][]int64{v.RowRefRowIndexes})
	v.rowRefAppliedLSNChunks = newColumnHNSWSearchPackPreparedChunks([][]int64{v.RowRefAppliedLSNs})
	v.documentIDOffsetChunks = newColumnHNSWSearchPackPreparedChunks([][]uint64{v.DocumentIDOffsets})
	v.documentIDByteChunks = newColumnHNSWSearchPackPreparedChunks([][]byte{v.DocumentIDBytes})
}

func columnHNSWSearchPackTypedChunks[T any](chunks [][]byte, name string, view func([]byte) ([]T, error)) ([][]T, error) {
	values := make([][]T, len(chunks))
	for i, chunk := range chunks {
		if len(chunk) == 0 {
			return nil, fmt.Errorf("collections: chunked hnsw_search_pack_v1 %s chunk=%d is empty", name, i)
		}
		var err error
		values[i], err = view(chunk)
		if err != nil {
			return nil, fmt.Errorf("collections: chunked hnsw_search_pack_v1 %s chunk=%d direct view: %w", name, i, err)
		}
	}
	return values, nil
}

func (v *columnHNSWSearchPackPreparedView) prepareChunkedV6SectionViewsWithContext(ctx context.Context, chunks map[columnHNSWSearchPackSectionKey][][]byte, opts columnHNSWSearchPackDecodeOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if v == nil || v.Header.Version != columnHNSWSearchPackVersionV6 || v.Header.ExternalNormalizedVectors || v.Header.HasAuxiliaryNavigation {
		return errors.New("collections: chunked hnsw_search_pack_v1 requires an embedded-vector V6 pack")
	}
	rows := uint64(v.Header.Rows)
	vectorCount, ok := checkedHNSWPackMulOK(rows, uint64(v.Header.VectorStride))
	if !ok {
		return errors.New("collections: chunked hnsw_search_pack_v1 normalized vector count overflows uint64")
	}
	sectionChunks := func(kind columnHNSWSearchPackSectionKind, index uint16) [][]byte {
		return chunks[columnHNSWSearchPackSectionKey{kind: kind, index: index}]
	}

	if _, err := columnHNSWSearchPackRequireSection(v.Sections, columnHNSWSearchPackSectionNormalizedVectors, 0, vectorCount, 4); err != nil {
		return err
	}
	vectors, err := columnHNSWSearchPackTypedChunks(sectionChunks(columnHNSWSearchPackSectionNormalizedVectors, 0), "normalized_vectors", mappedresource.Float32View)
	if err != nil {
		return err
	}
	v.normalizedVectorChunks = newColumnHNSWSearchPackPreparedChunks(vectors)
	if v.normalizedVectorChunks.length != vectorCount {
		return errors.New("collections: chunked hnsw_search_pack_v1 normalized_vectors count mismatch")
	}
	if len(vectors) == 1 {
		v.NormalizedVectors = vectors[0]
	}
	for _, chunk := range vectors {
		if len(chunk)%v.Header.VectorStride != 0 {
			return errors.New("collections: chunked hnsw_search_pack_v1 normalized vector row crosses a chunk")
		}
		for i, value := range chunk {
			if i&1023 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
				return errors.New("collections: chunked hnsw_search_pack_v1 normalized vector is not finite")
			}
		}
	}

	if _, err := columnHNSWSearchPackRequireSection(v.Sections, columnHNSWSearchPackSectionLevels, 0, rows, 2); err != nil {
		return err
	}
	levels, err := columnHNSWSearchPackTypedChunks(sectionChunks(columnHNSWSearchPackSectionLevels, 0), "levels", mappedresource.Uint16View)
	if err != nil {
		return err
	}
	v.levelChunks = newColumnHNSWSearchPackPreparedChunks(levels)
	if v.levelChunks.length != rows {
		return errors.New("collections: chunked hnsw_search_pack_v1 levels count mismatch")
	}
	if len(levels) == 1 {
		v.Levels = levels[0]
	}
	for ordinal := uint64(0); ordinal < rows; ordinal++ {
		level, ok := v.levelChunks.at(ordinal)
		if !ok || int(level) > v.Header.MaxLayer {
			return fmt.Errorf("collections: chunked hnsw_search_pack_v1 invalid level ordinal=%d", ordinal)
		}
	}

	v.AdjacencyLayers = make([]columnHNSWSearchPackPreparedLayer, v.Header.AdjacencyLayerCount)
	v.adjacencyOffsetChunks = make([]columnHNSWSearchPackPreparedChunks[uint64], v.Header.AdjacencyLayerCount)
	v.adjacencyNeighborChunks = make([]columnHNSWSearchPackPreparedChunks[uint32], v.Header.AdjacencyLayerCount)
	for layer := 0; layer < v.Header.AdjacencyLayerCount; layer++ {
		if _, err := columnHNSWSearchPackRequireSection(v.Sections, columnHNSWSearchPackSectionAdjacencyOffsets, uint16(layer), rows+1, 8); err != nil {
			return err
		}
		neighborSection, err := columnHNSWSearchPackFindSection(v.Sections, columnHNSWSearchPackSectionAdjacencyNeighbors, uint16(layer))
		if err != nil {
			return err
		}
		if neighborSection.Length%4 != 0 || neighborSection.Count != neighborSection.Length/4 || neighborSection.Count > opts.MaxNeighbors {
			return fmt.Errorf("collections: chunked hnsw_search_pack_v1 adjacency layer=%d neighbors shape", layer)
		}
		offsets, err := columnHNSWSearchPackTypedChunks(sectionChunks(columnHNSWSearchPackSectionAdjacencyOffsets, uint16(layer)), fmt.Sprintf("adjacency_offsets[%d]", layer), mappedresource.Uint64View)
		if err != nil {
			return err
		}
		neighbors, err := columnHNSWSearchPackTypedChunks(sectionChunks(columnHNSWSearchPackSectionAdjacencyNeighbors, uint16(layer)), fmt.Sprintf("adjacency_neighbors[%d]", layer), mappedresource.Uint32View)
		if err != nil {
			return err
		}
		v.adjacencyOffsetChunks[layer] = newColumnHNSWSearchPackPreparedChunks(offsets)
		v.adjacencyNeighborChunks[layer] = newColumnHNSWSearchPackPreparedChunks(neighbors)
		if v.adjacencyOffsetChunks[layer].length != rows+1 || v.adjacencyNeighborChunks[layer].length != neighborSection.Count {
			return fmt.Errorf("collections: chunked hnsw_search_pack_v1 adjacency layer=%d count mismatch", layer)
		}
		if len(offsets) == 1 {
			v.AdjacencyLayers[layer].Offsets = offsets[0]
		}
		if len(neighbors) == 1 {
			v.AdjacencyLayers[layer].Neighbors = neighbors[0]
		}
		if err := v.validateChunkedAdjacencyWithContext(ctx, layer); err != nil {
			return err
		}
	}

	loadInt64 := func(kind columnHNSWSearchPackSectionKind, name string) (columnHNSWSearchPackPreparedChunks[int64], []int64, error) {
		if _, err := columnHNSWSearchPackRequireSection(v.Sections, kind, 0, rows, 8); err != nil {
			return columnHNSWSearchPackPreparedChunks[int64]{}, nil, err
		}
		values, err := columnHNSWSearchPackTypedChunks(sectionChunks(kind, 0), name, mappedresource.Int64View)
		if err != nil {
			return columnHNSWSearchPackPreparedChunks[int64]{}, nil, err
		}
		prepared := newColumnHNSWSearchPackPreparedChunks(values)
		if prepared.length != rows {
			return columnHNSWSearchPackPreparedChunks[int64]{}, nil, fmt.Errorf("collections: chunked hnsw_search_pack_v1 %s count mismatch", name)
		}
		if len(values) == 1 {
			return prepared, values[0], nil
		}
		return prepared, nil, nil
	}
	if v.rowRefGenerationChunks, v.RowRefGenerations, err = loadInt64(columnHNSWSearchPackSectionRowRefGeneration, "row_ref_generation"); err != nil {
		return err
	}
	if v.rowRefPartIDChunks, v.RowRefPartIDs, err = loadInt64(columnHNSWSearchPackSectionRowRefPartID, "row_ref_part_id"); err != nil {
		return err
	}
	if v.rowRefRowIndexChunks, v.RowRefRowIndexes, err = loadInt64(columnHNSWSearchPackSectionRowRefRowIndex, "row_ref_row_index"); err != nil {
		return err
	}
	if v.rowRefAppliedLSNChunks, v.RowRefAppliedLSNs, err = loadInt64(columnHNSWSearchPackSectionRowRefAppliedLSN, "row_ref_applied_lsn"); err != nil {
		return err
	}
	for ordinal := uint64(0); ordinal < rows; ordinal++ {
		generation, gok := v.rowRefGenerationChunks.at(ordinal)
		partID, pok := v.rowRefPartIDChunks.at(ordinal)
		rowIndex, rok := v.rowRefRowIndexChunks.at(ordinal)
		appliedLSN, aok := v.rowRefAppliedLSNChunks.at(ordinal)
		if !gok || !pok || !rok || !aok || generation <= 0 || partID <= 0 || rowIndex < 0 || appliedLSN <= 0 || uint64(generation) > v.Header.BaseManifestGeneration {
			return fmt.Errorf("collections: chunked hnsw_search_pack_v1 invalid row-ref ordinal=%d", ordinal)
		}
	}

	if _, err := columnHNSWSearchPackRequireSection(v.Sections, columnHNSWSearchPackSectionDocumentIDOffsets, 0, rows+1, 8); err != nil {
		return err
	}
	docOffsets, err := columnHNSWSearchPackTypedChunks(sectionChunks(columnHNSWSearchPackSectionDocumentIDOffsets, 0), "document_id_offsets", mappedresource.Uint64View)
	if err != nil {
		return err
	}
	v.documentIDOffsetChunks = newColumnHNSWSearchPackPreparedChunks(docOffsets)
	if v.documentIDOffsetChunks.length != rows+1 {
		return errors.New("collections: chunked hnsw_search_pack_v1 document_id_offsets count mismatch")
	}
	if len(docOffsets) == 1 {
		v.DocumentIDOffsets = docOffsets[0]
	}
	docBytesSection, err := columnHNSWSearchPackFindSection(v.Sections, columnHNSWSearchPackSectionDocumentIDBytes, 0)
	if err != nil {
		return err
	}
	if docBytesSection.Count != docBytesSection.Length || docBytesSection.Length > opts.MaxDocumentIDBytes {
		return errors.New("collections: chunked hnsw_search_pack_v1 document_id_bytes shape")
	}
	docBytes := sectionChunks(columnHNSWSearchPackSectionDocumentIDBytes, 0)
	v.documentIDByteChunks = newColumnHNSWSearchPackPreparedChunks(docBytes)
	if v.documentIDByteChunks.length != docBytesSection.Length {
		return errors.New("collections: chunked hnsw_search_pack_v1 document_id_bytes count mismatch")
	}
	if len(docBytes) == 1 {
		v.DocumentIDBytes = docBytes[0]
	}
	return v.validateChunkedDocumentIDsWithContext(ctx, rows)
}

func (v *columnHNSWSearchPackPreparedView) validateChunkedDocumentIDsWithContext(ctx context.Context, rows uint64) error {
	first, ok := v.documentIDOffsetChunks.at(0)
	if !ok || first != 0 {
		return errors.New("collections: chunked hnsw_search_pack_v1 document id first offset is not zero")
	}
	for ordinal := uint64(0); ordinal < rows; ordinal++ {
		start, sok := v.documentIDOffsetChunks.at(ordinal)
		end, eok := v.documentIDOffsetChunks.at(ordinal + 1)
		if !sok || !eok || end <= start || end > v.documentIDByteChunks.length {
			return fmt.Errorf("collections: chunked hnsw_search_pack_v1 document id row=%d bounds", ordinal)
		}
		if _, ok := v.documentIDByteChunks.span(start, end); !ok {
			return fmt.Errorf("collections: chunked hnsw_search_pack_v1 document id row=%d crosses a chunk", ordinal)
		}
	}
	last, ok := v.documentIDOffsetChunks.at(rows)
	if !ok || last != v.documentIDByteChunks.length {
		return errors.New("collections: chunked hnsw_search_pack_v1 document id trailing bytes")
	}
	return ctx.Err()
}

func (v *columnHNSWSearchPackPreparedView) validateChunkedAdjacencyWithContext(ctx context.Context, layer int) error {
	if layer < 0 || layer >= len(v.adjacencyOffsetChunks) || layer >= len(v.adjacencyNeighborChunks) {
		return errors.New("collections: chunked hnsw_search_pack_v1 adjacency layer unavailable")
	}
	offsets := v.adjacencyOffsetChunks[layer]
	neighbors := v.adjacencyNeighborChunks[layer]
	first, ok := offsets.at(0)
	if !ok || first != 0 {
		return fmt.Errorf("collections: chunked hnsw_search_pack_v1 adjacency layer=%d first offset", layer)
	}
	for row := 0; row < v.Header.Rows; row++ {
		if row&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		start, sok := offsets.at(uint64(row))
		end, eok := offsets.at(uint64(row + 1))
		if !sok || !eok || end < start || end > neighbors.length {
			return fmt.Errorf("collections: chunked hnsw_search_pack_v1 adjacency layer=%d row=%d bounds", layer, row)
		}
		current, ok := neighbors.span(start, end)
		if !ok {
			return fmt.Errorf("collections: chunked hnsw_search_pack_v1 adjacency layer=%d row=%d crosses a chunk", layer, row)
		}
		if v.Header.Version == columnHNSWSearchPackVersionV6 && layer == 0 && len(current) > vectorPartitionVamanaDegreeV1 {
			return fmt.Errorf("collections: connectivity-preserving partition Vamana row=%d exceeds degree=%d", row, vectorPartitionVamanaDegreeV1)
		}
		for i, neighbor := range current {
			if int(neighbor) >= v.Header.Rows {
				return fmt.Errorf("collections: chunked hnsw_search_pack_v1 adjacency layer=%d row=%d neighbor=%d outside rows", layer, row, neighbor)
			}
			if v.Header.Version == columnHNSWSearchPackVersionV6 && layer == 0 {
				if int(neighbor) == row {
					return fmt.Errorf("collections: connectivity-preserving partition Vamana row=%d has self edge", row)
				}
				for _, earlier := range current[:i] {
					if earlier == neighbor {
						return fmt.Errorf("collections: connectivity-preserving partition Vamana row=%d has duplicate neighbor=%d", row, neighbor)
					}
				}
			}
		}
	}
	last, ok := offsets.at(uint64(v.Header.Rows))
	if !ok || last != neighbors.length {
		return fmt.Errorf("collections: chunked hnsw_search_pack_v1 adjacency layer=%d final offset", layer)
	}
	if v.Header.Version != columnHNSWSearchPackVersionV6 || layer != 0 || v.Header.Rows == 0 {
		return nil
	}
	seen := make([]bool, v.Header.Rows)
	queue := make([]uint32, 1, v.Header.Rows)
	seen[0] = true
	for head := 0; head < len(queue); head++ {
		current, err := v.adjacencyLayerForOrdinal(int(queue[head]), layer, nil)
		if err != nil {
			return err
		}
		for _, neighbor := range current {
			if !seen[neighbor] {
				seen[neighbor] = true
				queue = append(queue, neighbor)
			}
		}
	}
	if len(queue) != v.Header.Rows {
		return fmt.Errorf("collections: connectivity-preserving partition Vamana entry reaches %d of %d rows", len(queue), v.Header.Rows)
	}
	return nil
}

func decodeColumnHNSWSearchPackEnvelope(raw []byte, opts columnHNSWSearchPackDecodeOptions) (columnHNSWSearchPack, columnHNSWSearchPackDecodeOptions, error) {
	return decodeColumnHNSWSearchPackEnvelopeWithContext(context.Background(), raw, opts)
}

func decodeColumnHNSWSearchPackEnvelopeWithContext(ctx context.Context, raw []byte, opts columnHNSWSearchPackDecodeOptions) (columnHNSWSearchPack, columnHNSWSearchPackDecodeOptions, error) {
	return decodeColumnHNSWSearchPackEnvelopeMetadataWithContext(ctx, raw, uint64(len(raw)), opts, true)
}

// decodeColumnHNSWSearchPackEnvelopeMetadataWithContext decodes the bounded
// header/directory prefix used by the direct-file validator. Full decodes keep
// validating section payload checksums here; the direct-file path streams them
// from disk instead.
func decodeColumnHNSWSearchPackEnvelopeMetadataWithContext(ctx context.Context, raw []byte, totalLength uint64, opts columnHNSWSearchPackDecodeOptions, validatePayloadChecksums bool) (columnHNSWSearchPack, columnHNSWSearchPackDecodeOptions, error) {
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
	case columnHNSWSearchPackVersionV2, columnHNSWSearchPackVersionV3, columnHNSWSearchPackVersionV4, columnHNSWSearchPackVersionV5, columnHNSWSearchPackVersionV6:
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
	externalVectors := version == columnHNSWSearchPackVersionV4
	wantFlags := uint16(0)
	if externalVectors {
		wantFlags = columnHNSWSearchPackFlagExternalNormalizedVectors
	}
	if flags != wantFlags {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 unsupported flags=0x%x", flags)
	}
	if got := hnswPackU64(raw, columnHNSWSearchPackHeaderTotalLengthOffset); got != totalLength {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 length=%d want header total_length=%d", totalLength, got)
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
	if stride32 < dims32 || stride32 > opts.MaxVectorStride || uint64(stride32) > uint64(math.MaxInt) || (externalVectors && stride32 != dims32) || (!externalVectors && (uint64(stride32)*4)%uint64(columnHNSWSearchPackVectorSectionAlignment) != 0) {
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
	if version == columnHNSWSearchPackVersionV5 &&
		(hnswPackU32(raw, columnHNSWSearchPackHeaderMOffset) != columnHNSWCanonicalPartitionM ||
			hnswPackU32(raw, columnHNSWSearchPackHeaderEfConstructionOffset) != columnHNSWCanonicalPartitionEfConstruction) {
		return columnHNSWSearchPack{}, opts, errors.New("collections: canonical partition hnsw graph parameters mismatch")
	}
	if version == columnHNSWSearchPackVersionV6 &&
		(hnswPackU32(raw, columnHNSWSearchPackHeaderMOffset) != columnVamanaConnectivityPreservingPartitionM ||
			hnswPackU32(raw, columnHNSWSearchPackHeaderEfConstructionOffset) != columnVamanaConnectivityPreservingPartitionL ||
			hnswPackU64(raw, columnHNSWSearchPackHeaderEntryOrdinalOffset) != 0 ||
			hnswPackU32(raw, columnHNSWSearchPackHeaderMaxLayerOffset) != 0 ||
			hnswPackU32(raw, columnHNSWSearchPackHeaderAdjacencyLayerCount) != 1) {
		return columnHNSWSearchPack{}, opts, errors.New("collections: connectivity-preserving partition Vamana graph parameters mismatch")
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
	var externalVectorDigest [sha256.Size]byte
	if version == columnHNSWSearchPackVersionV2 || version == columnHNSWSearchPackVersionV3 || version == columnHNSWSearchPackVersionV5 || version == columnHNSWSearchPackVersionV6 {
		copy(membershipDigest[:], raw[columnHNSWSearchPackHeaderMembershipDigestOffset:columnHNSWSearchPackHeaderSizeV2])
		if membershipDigest == ([sha256.Size]byte{}) {
			return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 version %d missing membership digest", version)
		}
	} else if externalVectors {
		copy(externalVectorDigest[:], raw[columnHNSWSearchPackHeaderExternalVectorDigestOffset:columnHNSWSearchPackHeaderSizeV2])
		if externalVectorDigest == ([sha256.Size]byte{}) {
			return columnHNSWSearchPack{}, opts, errors.New("collections: hnsw_search_pack_v1 version 4 missing external vector digest")
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
	if externalVectors {
		expectedSectionCount--
	}
	if version == columnHNSWSearchPackVersionV3 {
		expectedSectionCount += 2
	}
	if sectionCount32 != expectedSectionCount {
		return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 section_count=%d want %d", sectionCount32, expectedSectionCount)
	}
	maxSectionCount := uint32(8 + 2*opts.MaxLayers)
	if externalVectors {
		maxSectionCount--
	}
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
	if err := validateColumnHNSWSearchPackSectionDirectoryLayoutWithContext(ctx, totalLength, sections, dataOffset); err != nil {
		return columnHNSWSearchPack{}, opts, err
	}
	if validatePayloadChecksums {
		for _, section := range sections {
			payload := raw[section.Offset : section.Offset+section.Length]
			checksum, err := columnHNSWSearchPackChecksumWithContext(ctx, payload)
			if err != nil {
				return columnHNSWSearchPack{}, opts, err
			}
			if checksum != section.Checksum {
				return columnHNSWSearchPack{}, opts, fmt.Errorf("collections: hnsw_search_pack_v1 section %s[%d] checksum=%08x want %08x", section.Kind, section.Index, checksum, section.Checksum)
			}
		}
	}
	pack := columnHNSWSearchPack{
		Header: columnHNSWSearchPackHeader{
			Version:                   version,
			Rows:                      int(rows64),
			Dimensions:                int(dims32),
			VectorStride:              int(stride32),
			M:                         int(hnswPackU32(raw, columnHNSWSearchPackHeaderMOffset)),
			EfConstruction:            int(hnswPackU32(raw, columnHNSWSearchPackHeaderEfConstructionOffset)),
			EfSearch:                  int(hnswPackU32(raw, columnHNSWSearchPackHeaderEfSearchOffset)),
			EntryOrdinal:              int(hnswPackU64(raw, columnHNSWSearchPackHeaderEntryOrdinalOffset)),
			MaxLayer:                  int(maxLayer32),
			AdjacencyLayerCount:       int(layerCount32),
			BaseManifestGeneration:    baseIdentity.ManifestGeneration,
			BaseManifestChecksum:      baseIdentity.ManifestChecksum,
			BaseSchemaHash:            baseIdentity.SchemaHash,
			MembershipDigest:          membershipDigest,
			HasAuxiliaryNavigation:    version == columnHNSWSearchPackVersionV3,
			ExternalNormalizedVectors: externalVectors,
			ExternalVectorDigest:      externalVectorDigest,
			TotalLength:               totalLength,
			DataOffset:                dataOffset,
			DataLength:                dataLength,
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
	if err := validateColumnHNSWSearchPackSectionDirectoryLayoutWithContext(ctx, uint64(len(raw)), sections, dataOffset); err != nil {
		return err
	}
	for _, section := range sections {
		payload := raw[section.Offset : section.Offset+section.Length]
		checksum, err := columnHNSWSearchPackChecksumWithContext(ctx, payload)
		if err != nil {
			return err
		}
		if checksum != section.Checksum {
			return fmt.Errorf("collections: hnsw_search_pack_v1 section %s[%d] checksum=%08x want %08x", section.Kind, section.Index, checksum, section.Checksum)
		}
	}
	return nil
}

func validateColumnHNSWSearchPackSectionDirectoryLayoutWithContext(ctx context.Context, totalLength uint64, sections []columnHNSWSearchPackSection, dataOffset uint64) error {
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
		if section.Offset < dataOffset || section.Offset > totalLength || section.Length > totalLength-section.Offset {
			return fmt.Errorf("collections: hnsw_search_pack_v1 section %s[%d] bounds offset=%d length=%d total=%d", section.Kind, section.Index, section.Offset, section.Length, totalLength)
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
	if !v.Header.ExternalNormalizedVectors {
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
		if v.Header.Version == columnHNSWSearchPackVersionV6 && layer == 0 {
			if err := validateColumnVamanaPartitionGraphV1(ctx, offsets, neighbors); err != nil {
				return err
			}
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
		if err := validateVectorPartitionLocalAuxiliaryNavigationFromNativeLayer0WithContextV1(ctx, v.Header.Rows, v.Header.EntryOrdinal, v.Levels, native.Offsets, native.Neighbors, vectorPartitionLocalAuxiliaryNavigationV1{Offsets: offsets, Neighbors: neighbors}); err != nil {
			return fmt.Errorf("collections: hnsw_search_pack_v1 auxiliary navigation: %w", err)
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

func validateColumnHNSWSearchPackChunkedGeometry(root []byte, pack columnHNSWSearchPack) error {
	wantRoot := columnHNSWSearchPackHeaderSizeV2 + len(pack.Sections)*columnHNSWSearchPackSectionEntrySize
	if len(root) != wantRoot {
		return errors.New("collections: chunked hnsw_search_pack_v1 root geometry")
	}
	cursor := pack.Header.DataOffset
	for _, section := range pack.Sections {
		offset, ok := alignColumnHNSWSearchPackUint64(cursor, uint64(section.Alignment))
		if !ok || section.Offset != offset || section.Length > ^uint64(0)-section.Offset {
			return fmt.Errorf("collections: chunked hnsw_search_pack_v1 section %s[%d] geometry", section.Kind, section.Index)
		}
		cursor = section.Offset + section.Length
	}
	if cursor != pack.Header.TotalLength {
		return errors.New("collections: chunked hnsw_search_pack_v1 trailing geometry")
	}
	return nil
}

func (v *columnHNSWSearchPackPreparedView) sectionDirectBytes(raw []byte, section columnHNSWSearchPackSection, elemBytes uintptr, typeName string) ([]byte, error) {
	var sectionBytes []byte
	if v != nil && v.sectionBytes != nil {
		var ok bool
		sectionBytes, ok = v.sectionBytes[columnHNSWSearchPackSectionKey{kind: section.Kind, index: section.Index}]
		if !ok || uint64(len(sectionBytes)) != section.Length {
			return nil, fmt.Errorf("collections: hnsw_search_pack_v1 missing chunk %s[%d]", section.Kind, section.Index)
		}
	} else {
		sectionBytes = columnHNSWSearchPackSectionBytes(raw, section)
	}
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
	if v.ephemeralHeap {
		return columnHNSWSearchPackPreparedStatusHeap
	}
	if v.handle == nil || v.handle.Released() {
		return columnHNSWSearchPackPreparedStatusStale
	}
	for _, handle := range v.handles {
		if handle == nil || handle.Released() {
			return columnHNSWSearchPackPreparedStatusStale
		}
	}
	switch v.status {
	case columnHNSWSearchPackPreparedStatusDirect, columnHNSWSearchPackPreparedStatusHeap, columnHNSWSearchPackPreparedStatusInvalid, columnHNSWSearchPackPreparedStatusStale, columnHNSWSearchPackPreparedStatusClosed, columnHNSWSearchPackPreparedStatusMissing:
		return v.status
	default:
		return columnHNSWSearchPackPreparedStatusInvalid
	}
}

func (v *columnHNSWSearchPackPreparedView) validateLive() error {
	if err := v.validateTopologyLive(); err != nil {
		return err
	}
	wantVectors := uint64(v.Header.Rows) * uint64(v.Header.VectorStride)
	if v.normalizedVectorChunks.length != wantVectors {
		return fmt.Errorf("collections: hnsw_search_pack_v1 prepared vector length=%d want rows*stride=%d", v.normalizedVectorChunks.length, wantVectors)
	}
	return nil
}

func (v *columnHNSWSearchPackPreparedView) bindExternalNormalizedVectors(state columnVectorIndexStateSnapshot, records []columnManifestRecord, cfg ColumnStoreConfig, source *columnVectorGraphTypedColumnVectorSource) error {
	if v == nil || !v.Header.ExternalNormalizedVectors {
		return errors.New("collections: hnsw topology pack does not request external normalized vectors")
	}
	if v.Header.Rows == 0 {
		if v.Header.ExternalVectorDigest != columnHNSWSearchPackEmptyExternalVectorDigest() {
			return errors.New("collections: empty hnsw topology pack canonical vector sentinel mismatch")
		}
		for _, asset := range state.Assets {
			if asset.Role == columnVectorIndexStateAssetRoleNormalizedVectors || asset.Role == columnVectorIndexStateAssetRoleInverseNorm {
				return errors.New("collections: empty hnsw topology pack retained vector row state")
			}
		}
		v.NormalizedVectors = nil
		v.normalizedVectorChunks = newColumnHNSWSearchPackPreparedChunks[float32](nil)
		return v.validateLive()
	}
	var normalized columnVectorIndexStateAssetSnapshot
	found := false
	for _, asset := range state.Assets {
		if asset.Role != columnVectorIndexStateAssetRoleNormalizedVectors {
			continue
		}
		if found {
			return errors.New("collections: duplicate canonical normalized vector state asset")
		}
		normalized, found = asset, true
	}
	if !found || normalized.AssetID != columnVectorIndexStateNormalizedVectorsAssetID || normalized.PhysicalEncoding != columnVectorIndexStateEncodingRawFloat32Vector || normalized.RowCount != v.Header.Rows || normalized.SourceSchemaHash != cfg.SchemaHash {
		return errors.New("collections: hnsw topology pack canonical normalized vector state asset mismatch")
	}
	if got := columnHNSWSearchPackExternalVectorRefDigest(normalized.Ref); got != v.Header.ExternalVectorDigest {
		return errors.New("collections: hnsw topology pack canonical vector identity mismatch")
	}
	if cfg.AssetManager == nil {
		return errors.New("collections: hnsw topology pack canonical vectors require asset manager")
	}
	typedRefs, err := typedColumnPartRefsByGenerationFromManifestRecords(records, cfg.AssetManager.Namespace)
	if err != nil {
		return err
	}
	typed, ok := typedRefs[state.BaseManifestGeneration]
	if !ok || typed.Ref != normalized.Ref {
		return errors.New("collections: hnsw topology pack canonical vector ref is not the base typed-column owner")
	}
	prepared, err := columnHNSWSearchPackCanonicalVectorView(source, v.Header.Rows, v.Header.Dimensions)
	if err != nil {
		return err
	}
	v.NormalizedVectors = prepared.values
	v.normalizedVectorChunks = newColumnHNSWSearchPackPreparedChunks([][]float32{v.NormalizedVectors})
	return v.validateLive()
}

func columnHNSWSearchPackCanonicalVectorView(source *columnVectorGraphTypedColumnVectorSource, rows, dims int) (columnVectorGraphPreparedVectorView, error) {
	if source == nil {
		return columnVectorGraphPreparedVectorView{}, errors.New("collections: hnsw topology pack canonical vector source is not a single graph-order direct view")
	}
	prepared := source.prepared
	if !prepared.identityMapping() || prepared.rows != rows || prepared.dims != dims || len(prepared.values) != rows*dims {
		// A platform without mmap still has one canonical FP32 plane: the
		// checksummed, handle-owned heap-copy typed view. Admit that retained view
		// for topology binding without relaxing the generic prepared-search mmap
		// prerequisite or allocating another vector representation.
		fallback, reason, description, ok := prepareColumnVectorGraphPreparedVectorViewWithHolderFallback(source, rows, dims, true)
		if !ok {
			return columnVectorGraphPreparedVectorView{}, fmt.Errorf("collections: hnsw topology pack canonical vector source is not a single graph-order direct view: reason=%s description=%s", reason, description)
		}
		prepared = fallback
	}
	if !prepared.identityMapping() || prepared.rows != rows || prepared.dims != dims || len(prepared.values) != rows*dims {
		return columnVectorGraphPreparedVectorView{}, errors.New("collections: hnsw topology pack canonical vector source is not a single graph-order direct view")
	}
	return prepared, nil
}

func (v *columnHNSWSearchPackPreparedView) validateTopologyLive() error {
	if v == nil {
		return columnHNSWSearchPackStatusError(columnHNSWSearchPackPreparedStatusMissing)
	}
	if v.closed.Load() {
		return errColumnHNSWSearchPackPreparedViewClosed
	}
	if v.handle == nil || v.handle.Released() || v.handle.Bytes() == nil {
		return errColumnHNSWSearchPackPreparedViewStale
	}
	for _, handle := range v.handles {
		if handle == nil || handle.Released() || handle.Bytes() == nil {
			return errColumnHNSWSearchPackPreparedViewStale
		}
	}
	if v.Header.Rows < 0 || v.Header.Dimensions <= 0 || v.Header.VectorStride < v.Header.Dimensions {
		return fmt.Errorf("collections: hnsw_search_pack_v1 prepared view invalid header rows/dims/stride=(%d,%d,%d)", v.Header.Rows, v.Header.Dimensions, v.Header.VectorStride)
	}
	rows := uint64(v.Header.Rows)
	if v.levelChunks.length != rows || v.rowRefGenerationChunks.length != rows || v.rowRefPartIDChunks.length != rows || v.rowRefRowIndexChunks.length != rows || v.rowRefAppliedLSNChunks.length != rows || v.documentIDOffsetChunks.length != rows+1 {
		return fmt.Errorf("collections: hnsw_search_pack_v1 prepared view section lengths vectors=%d levels=%d rowrefs=(%d,%d,%d,%d) doc_offsets=%d rows=%d stride=%d", v.normalizedVectorChunks.length, v.levelChunks.length, v.rowRefGenerationChunks.length, v.rowRefPartIDChunks.length, v.rowRefRowIndexChunks.length, v.rowRefAppliedLSNChunks.length, v.documentIDOffsetChunks.length, v.Header.Rows, v.Header.VectorStride)
	}
	if len(v.AdjacencyLayers) != v.Header.AdjacencyLayerCount {
		return fmt.Errorf("collections: hnsw_search_pack_v1 prepared adjacency layers=%d want %d", len(v.AdjacencyLayers), v.Header.AdjacencyLayerCount)
	}
	if v.Header.HasAuxiliaryNavigation && (len(v.AuxiliaryNavigation.Offsets) != v.Header.Rows+1 || len(v.AdjacencyLayers) == 0) {
		return errors.New("collections: hnsw_search_pack_v1 prepared auxiliary navigation unavailable")
	}
	return nil
}

// Metadata providers borrow the same validated mapping; they never own or
// certify its slices as separate TCIM views.
func (v *columnHNSWSearchPackPreparedView) metadataAlive() bool {
	if v == nil || v.closed.Load() || v.handle == nil || v.handle.Released() {
		return false
	}
	for _, handle := range v.handles {
		if handle == nil || handle.Released() {
			return false
		}
	}
	return true
}

func (v *columnHNSWSearchPackPreparedView) Close() error {
	if v == nil {
		return nil
	}
	v.closeOnce.Do(func() {
		v.closed.Store(true)
		errs := []error{v.closeErr}
		for _, handle := range v.handles {
			if handle != nil {
				errs = append(errs, handle.Release())
			}
		}
		v.handles = nil
		if v.handle != nil {
			errs = append(errs, v.handle.Release())
			v.handle = nil
		}
		v.closeErr = errors.Join(errs...)
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
