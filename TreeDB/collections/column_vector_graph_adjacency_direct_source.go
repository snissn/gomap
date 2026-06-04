package collections

import (
	"errors"
	"fmt"
	"math"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
	"github.com/snissn/gomap/TreeDB/page"
)

// Quarantine: graph-specific adjacency-source storage direct readers keep old
// column_graph compatibility paths safe/readable. New graph builds/search use
// vector-index state uint32_list assets; do not extend legacy graph-source
// opening beyond compatibility, validation, and safe fallback.
const columnVectorGraphLayer0AdjacencySourceScopeID = "column-vector-graph-adjacency-source"

type columnVectorGraphLayer0AdjacencySourceOutcome uint8

const (
	columnVectorGraphLayer0AdjacencySourceOutcomeUnknown columnVectorGraphLayer0AdjacencySourceOutcome = iota
	columnVectorGraphLayer0AdjacencySourceOutcomeMmapDirect
	columnVectorGraphLayer0AdjacencySourceOutcomeHeapCopyTypedView
	columnVectorGraphLayer0AdjacencySourceOutcomeScratchDecode
	columnVectorGraphLayer0AdjacencySourceOutcomePreparedCSRMmapDirect
	columnVectorGraphLayer0AdjacencySourceOutcomeTypedListMmapDirect
	columnVectorGraphLayer0AdjacencySourceOutcomeTypedListHeapCopyTypedView
	columnVectorGraphLayer0AdjacencySourceOutcomeTypedListScratchDecode
)

func (o columnVectorGraphLayer0AdjacencySourceOutcome) String() string {
	switch o {
	case columnVectorGraphLayer0AdjacencySourceOutcomeMmapDirect:
		return "mmap_direct"
	case columnVectorGraphLayer0AdjacencySourceOutcomeHeapCopyTypedView:
		return "heap_copy_typed_view"
	case columnVectorGraphLayer0AdjacencySourceOutcomeScratchDecode:
		return "scratch_decode"
	case columnVectorGraphLayer0AdjacencySourceOutcomePreparedCSRMmapDirect:
		return "prepared_csr_mmap_direct"
	case columnVectorGraphLayer0AdjacencySourceOutcomeTypedListMmapDirect:
		return "typed_list_mmap_direct"
	case columnVectorGraphLayer0AdjacencySourceOutcomeTypedListHeapCopyTypedView:
		return "typed_list_heap_copy_typed_view"
	case columnVectorGraphLayer0AdjacencySourceOutcomeTypedListScratchDecode:
		return "typed_list_scratch_decode"
	default:
		return "unknown"
	}
}

type columnVectorGraphLayer0AdjacencyDirectSource struct {
	layer   int
	rows    int
	offsets []uint64
	values  []uint32
	outcome columnVectorGraphLayer0AdjacencySourceOutcome

	manager       *mappedresource.Manager
	offsetsHandle *mappedresource.Handle
	valuesHandle  *mappedresource.Handle

	mappedBytes     uint64
	heapCopyBytes   uint64
	decodedBytes    uint64
	activeHandles   int64
	deniedResources uint64
	owned           bool
	closed          bool
}

type columnVectorGraphAdjacencyDirectSources struct {
	sources   []*columnVectorGraphLayer0AdjacencyDirectSource
	allLayers bool
	closed    bool
}

func (c *Collection) openColumnVectorGraphAdjacencyDirectSourcesForReader(collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot) (*columnVectorGraphAdjacencyDirectSources, typeddecode.Reason, error) {
	if len(graph.AdjacencyLayerSources) == 0 {
		if !graph.Layer0AdjacencySource.Present {
			return nil, "", nil
		}
		source, reason, err := c.newColumnVectorGraphLayer0AdjacencyDirectSource(collection, cfg, def, graph)
		if err != nil {
			if reason == "" {
				reason = typeddecode.ReasonValidationFailed
			}
			return nil, reason, err
		}
		return &columnVectorGraphAdjacencyDirectSources{sources: []*columnVectorGraphLayer0AdjacencyDirectSource{source}}, "", nil
	}
	if err := validateColumnVectorGraphAdjacencyLayerSourcesSnapshot(graph.AdjacencyLayerCount, graph.AdjacencyLayerSources); err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	if !graph.Layer0AdjacencySource.Present || graph.Layer0AdjacencySource != graph.AdjacencyLayerSources[0] {
		return nil, typeddecode.ReasonValidationFailed, errors.New("collections: column_graph adjacency direct sources layer-0 alias mismatch")
	}
	group := &columnVectorGraphAdjacencyDirectSources{sources: make([]*columnVectorGraphLayer0AdjacencyDirectSource, 0, len(graph.AdjacencyLayerSources)), allLayers: true}
	for layer, sourceMeta := range graph.AdjacencyLayerSources {
		if !sourceMeta.Present || sourceMeta.Layer != layer {
			_ = group.Close()
			return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph adjacency direct source[%d] present/layer=(%t,%d)", layer, sourceMeta.Present, sourceMeta.Layer)
		}
		source, reason, err := c.newColumnVectorGraphAdjacencyDirectSource(collection, cfg, def, graph, sourceMeta)
		if err != nil {
			_ = group.Close()
			if reason == "" {
				reason = typeddecode.ReasonValidationFailed
			}
			return nil, reason, err
		}
		group.sources = append(group.sources, source)
	}
	return group, "", nil
}

func (c *Collection) openColumnVectorGraphLayer0AdjacencyDirectSourceForReader(collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot) (*columnVectorGraphLayer0AdjacencyDirectSource, typeddecode.Reason, error) {
	if !graph.Layer0AdjacencySource.Present {
		return nil, "", nil
	}
	source, reason, err := c.newColumnVectorGraphLayer0AdjacencyDirectSource(collection, cfg, def, graph)
	if err != nil {
		if reason == "" {
			reason = typeddecode.ReasonValidationFailed
		}
		return nil, reason, err
	}
	return source, "", nil
}

func (c *Collection) newColumnVectorGraphLayer0AdjacencyDirectSource(collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot) (*columnVectorGraphLayer0AdjacencyDirectSource, typeddecode.Reason, error) {
	return c.newColumnVectorGraphAdjacencyDirectSource(collection, cfg, def, graph, graph.Layer0AdjacencySource)
}

func (c *Collection) newColumnVectorGraphAdjacencyDirectSource(collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, source columnVectorGraphLayer0AdjacencySourceSnapshot) (*columnVectorGraphLayer0AdjacencyDirectSource, typeddecode.Reason, error) {
	if c == nil {
		return nil, typeddecode.ReasonValidationFailed, errCollectionNil
	}
	if c.db == nil {
		return nil, typeddecode.ReasonValidationFailed, errCollectionDBNil
	}
	if !source.Present {
		return nil, "", nil
	}
	sourceCfg, adapterColumn, err := columnVectorGraphAdjacencySourceColumnStoreConfig(collection, cfg, def, source.Layer)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	if err := validateColumnVectorGraphAdjacencySourceMatchesGraph(graph, source, sourceCfg); err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	if err := validateColumnVectorGraphAssetRefAvailable(c.db.ColumnAssetRootDir(), source.Ref); err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	raw, err := readColumnPhysicalAssetFromManager(c.db.ColumnAssetRootDir(), source.Ref)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	if int64(len(raw)) != source.AssetBytes || int64(len(raw)) != source.Ref.Length {
		return nil, typeddecode.ReasonPayloadLengthMismatch, fmt.Errorf("collections: column_graph adjacency layer %d source bytes=%d manifest=%d ref=%d", source.Layer, len(raw), source.AssetBytes, source.Ref.Length)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	if image.PartID != source.Ref.PartID || image.Rows != source.RowCount {
		return nil, typeddecode.ReasonRowCountMismatch, fmt.Errorf("collections: column_graph adjacency layer %d source image part/rows=(%d,%d) want (%d,%d)", source.Layer, image.PartID, image.Rows, source.Ref.PartID, source.RowCount)
	}
	fields := columnStoreTypedColumnPartFields(sourceCfg)
	if _, err := typedColumnAdapterPartFromImageWithoutRowLocators(typedColumnAdapterOptions{Fields: fields, SchemaVersion: uint32(sourceCfg.SchemaHash)}, image); err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph adjacency layer %d source layout certification: %w", source.Layer, err)
	}
	certColumn, ok := certification.Column(adapterColumn.Definition.Name)
	if !ok {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph adjacency layer %d source missing layout certification for column %q", source.Layer, adapterColumn.Definition.Name)
	}
	offsetsSection, valuesSection, ok := image.ColumnOffsetsListSections(adapterColumn.Definition.Name)
	if !ok {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph adjacency layer %d source missing offsets-list sections for column %q", source.Layer, adapterColumn.Definition.Name)
	}
	if int64(offsetsSection.Length) != source.OffsetsBytes || int64(valuesSection.Length) != source.ValuesBytes {
		return nil, typeddecode.ReasonPayloadLengthMismatch, fmt.Errorf("collections: column_graph adjacency layer %d source section bytes offsets=%d/%d values=%d/%d", source.Layer, offsetsSection.Length, source.OffsetsBytes, valuesSection.Length, source.ValuesBytes)
	}
	plan := typeddecode.AdjacencyOffsetsListPlan(certColumn)
	directReq := typeddecode.Uint32OffsetsListDirectViewRequest{
		Plan:           plan,
		Certification:  certColumn,
		Rows:           source.RowCount,
		OffsetsBytes:   offsetsSection.Length,
		ValuesBytes:    valuesSection.Length,
		AssetOffset:    source.Ref.Offset,
		HasAssetOffset: true,
	}
	if status := typeddecode.ValidateUint32OffsetsListDirectViewSections(directReq); !status.Direct() {
		return nil, status.Reason, fmt.Errorf("collections: column_graph adjacency layer %d source direct-view section validation failed: %s", source.Layer, status.String())
	}
	offsetsRaw, err := image.SectionBytes(offsetsSection)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	valuesRaw, err := image.SectionBytes(valuesSection)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	manager := mappedresource.NewManager()
	offsetsHandle, err := c.acquireColumnVectorGraphLayer0AdjacencySourceSection(collection, source.Ref, image.Version, offsetsSection, page.Checksum(offsetsRaw), manager, fmt.Sprintf("layer %d offsets", source.Layer))
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	valuesHandle, err := c.acquireColumnVectorGraphLayer0AdjacencySourceSection(collection, source.Ref, image.Version, valuesSection, page.Checksum(valuesRaw), manager, fmt.Sprintf("layer %d values", source.Layer))
	if err != nil {
		releaseErr := offsetsHandle.Release()
		return nil, typeddecode.ReasonValidationFailed, errors.Join(err, releaseErr)
	}
	adjacencySource, reason, err := columnVectorGraphLayer0AdjacencyDirectSourceFromHandles(manager, source.Layer, source.RowCount, source.ValuesCount, directReq, offsetsHandle, valuesHandle)
	if err != nil {
		releaseErr := errors.Join(offsetsHandle.Release(), valuesHandle.Release())
		return nil, reason, errors.Join(err, releaseErr)
	}
	adjacencySource.captureResourceStats()
	return adjacencySource, "", nil
}

func (c *Collection) acquireColumnVectorGraphLayer0AdjacencySourceSection(collection string, ref ColumnAssetRef, imageVersion uint16, section typedcolumn.ColumnPartImageSection, checksum uint32, manager *mappedresource.Manager, label string) (*mappedresource.Handle, error) {
	if manager == nil {
		return nil, errors.New("collections: column_graph adjacency source requires mappedresource manager")
	}
	path, err := columnAssetSegmentPath(c.db.ColumnAssetRootDir(), ref)
	if err != nil {
		return nil, err
	}
	sectionOffset, err := columnVectorGraphTypedColumnSectionOffset(ref, section)
	if err != nil {
		return nil, err
	}
	key := mappedresource.Key{
		Class:      mappedresource.ClassTypedColumnAsset,
		Namespace:  ref.Namespace,
		Kind:       string(ref.Kind),
		Generation: ref.Generation,
		PartID:     ref.PartID,
		FileID:     ref.FileID,
		Offset:     sectionOffset,
		Length:     int64(section.Length),
		Checksum:   uint64(checksum),
		Version:    imageVersion,
		Encoding:   section.Encoding.String(),
		Section: mappedresource.Section{
			Kind:     string(section.Kind),
			Category: string(section.Category),
			Name:     section.Name,
			Column:   section.Column,
		},
	}
	scope := mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: columnVectorGraphLayer0AdjacencySourceScopeID, Collection: collection, Namespace: ref.Namespace, Generation: ref.Generation, Reason: "column_graph adjacency source"}
	handle, err := manager.AcquireFileRange(key, scope, path, mappedresource.AcquireOptions{
		Reason:         "column_graph adjacency source " + label,
		ValidationMode: mappedresource.ValidationVerify,
		PreferMapped:   true,
		AllowHeapCopy:  true,
		ResourceRoot:   c.db.ColumnAssetRootDir(),
		ResourcePath:   path,
	})
	if err != nil {
		return nil, err
	}
	if got := page.Checksum(handle.Bytes()); got != checksum {
		releaseErr := handle.Release()
		return nil, errors.Join(fmt.Errorf("collections: column_graph adjacency source %s checksum=%d want %d", label, got, checksum), releaseErr)
	}
	return handle, nil
}

func columnVectorGraphLayer0AdjacencyDirectSourceFromHandles(manager *mappedresource.Manager, layer int, rows int, valuesCount int, directReq typeddecode.Uint32OffsetsListDirectViewRequest, offsetsHandle *mappedresource.Handle, valuesHandle *mappedresource.Handle) (*columnVectorGraphLayer0AdjacencyDirectSource, typeddecode.Reason, error) {
	return columnVectorGraphAdjacencyDirectSourceFromHandles(manager, layer, rows, valuesCount, directReq, offsetsHandle, valuesHandle, columnVectorGraphLayer0AdjacencySourceOutcomeMmapDirect, columnVectorGraphLayer0AdjacencySourceOutcomeHeapCopyTypedView, columnVectorGraphLayer0AdjacencySourceOutcomeScratchDecode, false, "column_graph adjacency source")
}

func columnVectorGraphTypedListAdjacencyDirectSourceFromHandles(manager *mappedresource.Manager, layer int, rows int, valuesCount int, directReq typeddecode.Uint32OffsetsListDirectViewRequest, offsetsHandle *mappedresource.Handle, valuesHandle *mappedresource.Handle) (*columnVectorGraphLayer0AdjacencyDirectSource, typeddecode.Reason, error) {
	return columnVectorGraphAdjacencyDirectSourceFromHandles(manager, layer, rows, valuesCount, directReq, offsetsHandle, valuesHandle, columnVectorGraphLayer0AdjacencySourceOutcomeTypedListMmapDirect, columnVectorGraphLayer0AdjacencySourceOutcomeTypedListHeapCopyTypedView, columnVectorGraphLayer0AdjacencySourceOutcomeTypedListScratchDecode, true, "vector-index state adjacency")
}

func columnVectorGraphPreparedCSRAdjacencyDirectSourceFromHandles(manager *mappedresource.Manager, layer int, rows int, valuesCount int, req typeddecode.GraphUint32ListDirectViewRequest, fallbackReq typeddecode.Uint32OffsetsListDirectViewRequest, offsetsHandle *mappedresource.Handle, valuesHandle *mappedresource.Handle) (*columnVectorGraphLayer0AdjacencyDirectSource, typeddecode.Reason, error) {
	view, status := typeddecode.CertifyGraphUint32ListDirectView(req)
	if status.Direct() {
		if len(view.Values) != valuesCount {
			releaseErr := view.Close()
			return nil, typeddecode.ReasonValuesLengthMismatch, errors.Join(fmt.Errorf("collections: vector-index state prepared CSR adjacency layer %d values=%d want %d", layer, len(view.Values), valuesCount), releaseErr)
		}
		return &columnVectorGraphLayer0AdjacencyDirectSource{layer: layer, rows: rows, offsets: view.Offsets, values: view.Values, outcome: columnVectorGraphLayer0AdjacencySourceOutcomePreparedCSRMmapDirect, manager: manager, offsetsHandle: view.OffsetsHandle, valuesHandle: view.ValuesHandle}, "", nil
	}
	if columnVectorGraphPreparedCSRAdjacencyFallbackAllowed(status) {
		return columnVectorGraphTypedListAdjacencyDirectSourceFromHandles(manager, layer, rows, valuesCount, fallbackReq, offsetsHandle, valuesHandle)
	}
	return nil, status.Reason, fmt.Errorf("collections: vector-index state prepared CSR adjacency layer %d certification: %s", layer, status.String())
}

func columnVectorGraphPreparedCSRAdjacencyFallbackAllowed(status typeddecode.Status) bool {
	return status.Reason == typeddecode.ReasonHandleSourceUnsupported
}

func columnVectorGraphAdjacencyDirectSourceFromHandles(manager *mappedresource.Manager, layer int, rows int, valuesCount int, directReq typeddecode.Uint32OffsetsListDirectViewRequest, offsetsHandle *mappedresource.Handle, valuesHandle *mappedresource.Handle, mmapOutcome, heapOutcome, scratchOutcome columnVectorGraphLayer0AdjacencySourceOutcome, allowScratch bool, label string) (*columnVectorGraphLayer0AdjacencyDirectSource, typeddecode.Reason, error) {
	if offsetsHandle == nil || valuesHandle == nil {
		status := typeddecode.StreamingStatus(typeddecode.ReasonNilHandle, "nil adjacency source handle")
		return nil, status.Reason, fmt.Errorf("collections: %s handle validation: %s", label, status.String())
	}
	if offsetsHandle.Released() || valuesHandle.Released() {
		status := typeddecode.UnsupportedStatus(typeddecode.ReasonStaleHandle, "released adjacency source handle")
		return nil, status.Reason, fmt.Errorf("collections: %s handle validation: %s", label, status.String())
	}
	decodeScratch := func(firstStatus typeddecode.Status, fallbackReason typeddecode.Reason) (*columnVectorGraphLayer0AdjacencyDirectSource, typeddecode.Reason, error) {
		decoded, decodeErr := typedcolumn.DecodeRawUint32OffsetsListFallback(nil, nil, offsetsHandle.Bytes(), valuesHandle.Bytes(), rows)
		releaseErr := errors.Join(offsetsHandle.Release(), valuesHandle.Release())
		if decodeErr != nil {
			return nil, typeddecode.ReasonValidationFailed, errors.Join(fmt.Errorf("collections: %s direct-view validation: %s", label, firstStatus.String()), decodeErr, releaseErr)
		}
		if releaseErr != nil {
			return nil, fallbackReason, errors.Join(fmt.Errorf("collections: %s direct-view validation: %s", label, firstStatus.String()), releaseErr)
		}
		if len(decoded.Values) != valuesCount {
			return nil, typeddecode.ReasonValuesLengthMismatch, fmt.Errorf("collections: %s layer %d decoded values=%d want %d", label, layer, len(decoded.Values), valuesCount)
		}
		return &columnVectorGraphLayer0AdjacencyDirectSource{layer: layer, rows: rows, offsets: decoded.Offsets, values: decoded.Values, outcome: scratchOutcome, manager: manager, decodedBytes: uint64(len(decoded.Offsets))*8 + uint64(len(decoded.Values))*4, owned: true}, fallbackReason, nil
	}
	if status := typeddecode.ValidateUint32OffsetsListDirectViewSections(directReq); !status.Direct() {
		if allowScratch && typedColumnUint32OffsetsListScratchFallbackAllowed(status) {
			return decodeScratch(status, status.Reason)
		}
		return nil, status.Reason, fmt.Errorf("collections: %s section validation: %s", label, status.String())
	}
	offsets, values, status := typeddecode.Uint32OffsetsListView(manager, offsetsHandle, valuesHandle, directReq, typeddecode.ResourceViewOptions{RequireMapped: true})
	if status.Direct() {
		if len(values) != valuesCount {
			return nil, typeddecode.ReasonValuesLengthMismatch, fmt.Errorf("collections: %s layer %d values=%d want %d", label, layer, len(values), valuesCount)
		}
		return &columnVectorGraphLayer0AdjacencyDirectSource{layer: layer, rows: rows, offsets: offsets, values: values, outcome: mmapOutcome, manager: manager, offsetsHandle: offsetsHandle, valuesHandle: valuesHandle}, "", nil
	}
	firstStatus := status
	fallbackReason := status.Reason
	if status.Reason == typeddecode.ReasonHandleSourceUnsupported || status.Reason == typeddecode.ReasonActualPointerUnaligned {
		offsets, values, status = typeddecode.Uint32OffsetsListView(manager, offsetsHandle, valuesHandle, directReq, typeddecode.ResourceViewOptions{RequireMapped: false})
		if status.Direct() {
			if len(values) != valuesCount {
				return nil, typeddecode.ReasonValuesLengthMismatch, fmt.Errorf("collections: %s layer %d values=%d want %d", label, layer, len(values), valuesCount)
			}
			return &columnVectorGraphLayer0AdjacencyDirectSource{layer: layer, rows: rows, offsets: offsets, values: values, outcome: heapOutcome, manager: manager, offsetsHandle: offsetsHandle, valuesHandle: valuesHandle}, "", nil
		}
		fallbackReason = status.Reason
	}
	if allowScratch && typedColumnUint32OffsetsListScratchFallbackAllowed(status) {
		return decodeScratch(firstStatus, fallbackReason)
	}
	if status.Reason == "" {
		status = firstStatus
	}
	return nil, status.Reason, fmt.Errorf("collections: %s direct-view validation: %s", label, status.String())
}

func (g *columnVectorGraphAdjacencyDirectSources) Neighbors(layer, ordinal int) ([]uint32, columnVectorGraphLayer0AdjacencySourceOutcome, typeddecode.Reason, bool) {
	if g == nil || g.closed || layer < 0 || layer >= len(g.sources) || g.sources[layer] == nil {
		return nil, columnVectorGraphLayer0AdjacencySourceOutcomeUnknown, "", false
	}
	return g.sources[layer].Neighbors(ordinal)
}

func (g *columnVectorGraphAdjacencyDirectSources) preparedCSRNeighbors(layer, ordinal int) ([]uint32, typeddecode.Reason, bool) {
	if g == nil {
		return nil, "", false
	}
	if g.closed {
		return nil, typeddecode.ReasonStaleHandle, false
	}
	if layer < 0 || layer >= len(g.sources) || g.sources[layer] == nil {
		return nil, typeddecode.ReasonRowCountMismatch, false
	}
	return g.sources[layer].preparedCSRNeighbors(ordinal)
}

func (g *columnVectorGraphAdjacencyDirectSources) MaxLayerForOrdinal(ordinal int) (int, []uint32, columnVectorGraphAdjacencySourceCounterSnapshot, typeddecode.Reason, bool) {
	if g == nil || g.closed || len(g.sources) == 0 {
		return 0, nil, columnVectorGraphAdjacencySourceCounterSnapshot{}, "", false
	}
	var counters columnVectorGraphAdjacencySourceCounterSnapshot
	for layer := len(g.sources) - 1; layer >= 0; layer-- {
		neighbors, outcome, reason, ok := g.Neighbors(layer, ordinal)
		if !ok {
			return 0, nil, counters, reason, false
		}
		counters.addOutcome(len(neighbors), outcome)
		if len(neighbors) > 0 {
			return layer, neighbors, counters, "", true
		}
	}
	return 0, nil, counters, "", true
}

func (g *columnVectorGraphAdjacencyDirectSources) Close() error {
	if g == nil || g.closed {
		return nil
	}
	g.closed = true
	var closeErr error
	for i, source := range g.sources {
		if err := source.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
		g.sources[i] = nil
	}
	return closeErr
}

func (g *columnVectorGraphAdjacencyDirectSources) ActiveHandles() int64 {
	if g == nil || g.closed {
		return 0
	}
	var handles int64
	for _, source := range g.sources {
		if source != nil && source.manager != nil {
			handles += source.manager.Stats().ActiveHandles
		}
	}
	return handles
}

func (s *columnVectorGraphLayer0AdjacencyDirectSource) Neighbors(ordinal int) ([]uint32, columnVectorGraphLayer0AdjacencySourceOutcome, typeddecode.Reason, bool) {
	if s == nil {
		return nil, columnVectorGraphLayer0AdjacencySourceOutcomeUnknown, "", false
	}
	if s.closed {
		return nil, columnVectorGraphLayer0AdjacencySourceOutcomeUnknown, typeddecode.ReasonStaleHandle, false
	}
	if !s.owned && (s.offsetsHandle == nil || s.valuesHandle == nil || s.offsetsHandle.Released() || s.valuesHandle.Released()) {
		return nil, columnVectorGraphLayer0AdjacencySourceOutcomeUnknown, typeddecode.ReasonStaleHandle, false
	}
	if ordinal < 0 || ordinal >= s.rows || ordinal+1 >= len(s.offsets) {
		return nil, columnVectorGraphLayer0AdjacencySourceOutcomeUnknown, typeddecode.ReasonRowCountMismatch, false
	}
	start64 := s.offsets[ordinal]
	end64 := s.offsets[ordinal+1]
	if end64 < start64 || end64 > uint64(len(s.values)) || end64 > uint64(math.MaxInt) {
		return nil, columnVectorGraphLayer0AdjacencySourceOutcomeUnknown, typeddecode.ReasonValuesLengthMismatch, false
	}
	return s.values[int(start64):int(end64)], s.outcome, "", true
}

func (s *columnVectorGraphLayer0AdjacencyDirectSource) preparedCSRNeighbors(ordinal int) ([]uint32, typeddecode.Reason, bool) {
	if s == nil {
		return nil, "", false
	}
	if s.closed {
		return nil, typeddecode.ReasonStaleHandle, false
	}
	if s.outcome != columnVectorGraphLayer0AdjacencySourceOutcomePreparedCSRMmapDirect {
		return nil, typeddecode.ReasonHandleSourceUnsupported, false
	}
	if !s.owned && (s.offsetsHandle == nil || s.valuesHandle == nil || s.offsetsHandle.Released() || s.valuesHandle.Released()) {
		return nil, typeddecode.ReasonStaleHandle, false
	}
	return s.neighborsUncheckedHandle(ordinal)
}

func (s *columnVectorGraphLayer0AdjacencyDirectSource) neighborsUncheckedHandle(ordinal int) ([]uint32, typeddecode.Reason, bool) {
	if ordinal < 0 || ordinal >= s.rows || ordinal+1 >= len(s.offsets) {
		return nil, typeddecode.ReasonRowCountMismatch, false
	}
	start64 := s.offsets[ordinal]
	end64 := s.offsets[ordinal+1]
	if end64 < start64 || end64 > uint64(len(s.values)) || end64 > uint64(math.MaxInt) {
		return nil, typeddecode.ReasonValuesLengthMismatch, false
	}
	return s.values[int(start64):int(end64)], "", true
}

func (s *columnVectorGraphLayer0AdjacencyDirectSource) captureResourceStats() {
	if s == nil || s.manager == nil {
		return
	}
	stats := s.manager.Stats()
	if stats.ActiveMappedBytes > 0 {
		s.mappedBytes = uint64(stats.ActiveMappedBytes)
	}
	if stats.ActiveHeapCopyBytes > 0 {
		s.heapCopyBytes = uint64(stats.ActiveHeapCopyBytes)
	}
	s.activeHandles = stats.ActiveHandles
	for _, count := range stats.DeniedByReason {
		s.deniedResources += count
	}
}

func (s *columnVectorGraphLayer0AdjacencyDirectSource) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	closeErr := errors.Join(releaseMappedResourceHandle(s.offsetsHandle), releaseMappedResourceHandle(s.valuesHandle))
	s.offsetsHandle = nil
	s.valuesHandle = nil
	s.offsets = nil
	s.values = nil
	s.outcome = columnVectorGraphLayer0AdjacencySourceOutcomeUnknown
	s.rows = 0
	s.decodedBytes = 0
	s.activeHandles = 0
	s.owned = false
	return closeErr
}
