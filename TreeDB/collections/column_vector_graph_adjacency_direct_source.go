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

const columnVectorGraphLayer0AdjacencySourceScopeID = "column-vector-graph-layer0-adjacency-source"

type columnVectorGraphLayer0AdjacencySourceOutcome uint8

const (
	columnVectorGraphLayer0AdjacencySourceOutcomeUnknown columnVectorGraphLayer0AdjacencySourceOutcome = iota
	columnVectorGraphLayer0AdjacencySourceOutcomeMmapDirect
	columnVectorGraphLayer0AdjacencySourceOutcomeHeapCopyTypedView
)

func (o columnVectorGraphLayer0AdjacencySourceOutcome) String() string {
	switch o {
	case columnVectorGraphLayer0AdjacencySourceOutcomeMmapDirect:
		return "mmap_direct"
	case columnVectorGraphLayer0AdjacencySourceOutcomeHeapCopyTypedView:
		return "heap_copy_typed_view"
	default:
		return "unknown"
	}
}

type columnVectorGraphLayer0AdjacencyDirectSource struct {
	rows    int
	offsets []uint64
	values  []uint32
	outcome columnVectorGraphLayer0AdjacencySourceOutcome

	manager       *mappedresource.Manager
	offsetsHandle *mappedresource.Handle
	valuesHandle  *mappedresource.Handle

	mappedBytes     uint64
	heapCopyBytes   uint64
	activeHandles   int64
	deniedResources uint64
	closed          bool
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
	if c == nil {
		return nil, typeddecode.ReasonValidationFailed, errCollectionNil
	}
	if c.db == nil {
		return nil, typeddecode.ReasonValidationFailed, errCollectionDBNil
	}
	source := graph.Layer0AdjacencySource
	if !source.Present {
		return nil, "", nil
	}
	sourceCfg, adapterColumn, err := columnVectorGraphLayer0AdjacencySourceColumnStoreConfig(collection, cfg, def)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	if err := validateColumnVectorGraphLayer0AdjacencySourceMatchesGraph(graph, sourceCfg); err != nil {
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
		return nil, typeddecode.ReasonPayloadLengthMismatch, fmt.Errorf("collections: column_graph layer-0 adjacency source bytes=%d manifest=%d ref=%d", len(raw), source.AssetBytes, source.Ref.Length)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	if image.PartID != source.Ref.PartID || image.Rows != source.RowCount {
		return nil, typeddecode.ReasonRowCountMismatch, fmt.Errorf("collections: column_graph layer-0 adjacency source image part/rows=(%d,%d) want (%d,%d)", image.PartID, image.Rows, source.Ref.PartID, source.RowCount)
	}
	fields := columnStoreTypedColumnPartFields(sourceCfg)
	if _, err := typedColumnAdapterPartFromImageWithoutRowLocators(typedColumnAdapterOptions{Fields: fields, SchemaVersion: uint32(sourceCfg.SchemaHash)}, image); err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph layer-0 adjacency source layout certification: %w", err)
	}
	certColumn, ok := certification.Column(adapterColumn.Definition.Name)
	if !ok {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph layer-0 adjacency source missing layout certification for column %q", adapterColumn.Definition.Name)
	}
	offsetsSection, valuesSection, ok := image.ColumnOffsetsListSections(adapterColumn.Definition.Name)
	if !ok {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph layer-0 adjacency source missing offsets-list sections for column %q", adapterColumn.Definition.Name)
	}
	if int64(offsetsSection.Length) != source.OffsetsBytes || int64(valuesSection.Length) != source.ValuesBytes {
		return nil, typeddecode.ReasonPayloadLengthMismatch, fmt.Errorf("collections: column_graph layer-0 adjacency source section bytes offsets=%d/%d values=%d/%d", offsetsSection.Length, source.OffsetsBytes, valuesSection.Length, source.ValuesBytes)
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
		return nil, status.Reason, fmt.Errorf("collections: column_graph layer-0 adjacency source direct-view section validation failed: %s", status.String())
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
	offsetsHandle, err := c.acquireColumnVectorGraphLayer0AdjacencySourceSection(collection, source.Ref, image.Version, offsetsSection, page.Checksum(offsetsRaw), manager, "offsets")
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	valuesHandle, err := c.acquireColumnVectorGraphLayer0AdjacencySourceSection(collection, source.Ref, image.Version, valuesSection, page.Checksum(valuesRaw), manager, "values")
	if err != nil {
		releaseErr := offsetsHandle.Release()
		return nil, typeddecode.ReasonValidationFailed, errors.Join(err, releaseErr)
	}
	adjacencySource, reason, err := columnVectorGraphLayer0AdjacencyDirectSourceFromHandles(manager, source.RowCount, source.ValuesCount, directReq, offsetsHandle, valuesHandle)
	if err != nil {
		releaseErr := errors.Join(offsetsHandle.Release(), valuesHandle.Release())
		return nil, reason, errors.Join(err, releaseErr)
	}
	adjacencySource.captureResourceStats()
	return adjacencySource, "", nil
}

func (c *Collection) acquireColumnVectorGraphLayer0AdjacencySourceSection(collection string, ref ColumnAssetRef, imageVersion uint16, section typedcolumn.ColumnPartImageSection, checksum uint32, manager *mappedresource.Manager, label string) (*mappedresource.Handle, error) {
	if manager == nil {
		return nil, errors.New("collections: column_graph layer-0 adjacency source requires mappedresource manager")
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
	scope := mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: columnVectorGraphLayer0AdjacencySourceScopeID, Collection: collection, Namespace: ref.Namespace, Generation: ref.Generation, Reason: "column_graph layer-0 adjacency source"}
	handle, err := manager.AcquireFileRange(key, scope, path, mappedresource.AcquireOptions{
		Reason:         "column_graph layer-0 adjacency source " + label,
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
		return nil, errors.Join(fmt.Errorf("collections: column_graph layer-0 adjacency source %s checksum=%d want %d", label, got, checksum), releaseErr)
	}
	return handle, nil
}

func columnVectorGraphLayer0AdjacencyDirectSourceFromHandles(manager *mappedresource.Manager, rows int, valuesCount int, directReq typeddecode.Uint32OffsetsListDirectViewRequest, offsetsHandle *mappedresource.Handle, valuesHandle *mappedresource.Handle) (*columnVectorGraphLayer0AdjacencyDirectSource, typeddecode.Reason, error) {
	if offsetsHandle == nil || valuesHandle == nil {
		status := typeddecode.StreamingStatus(typeddecode.ReasonNilHandle, "nil layer-0 adjacency source handle")
		return nil, status.Reason, fmt.Errorf("collections: column_graph layer-0 adjacency source handle validation: %s", status.String())
	}
	if offsetsHandle.Released() || valuesHandle.Released() {
		status := typeddecode.UnsupportedStatus(typeddecode.ReasonStaleHandle, "released layer-0 adjacency source handle")
		return nil, status.Reason, fmt.Errorf("collections: column_graph layer-0 adjacency source handle validation: %s", status.String())
	}
	if status := typeddecode.ValidateUint32OffsetsListDirectViewSections(directReq); !status.Direct() {
		return nil, status.Reason, fmt.Errorf("collections: column_graph layer-0 adjacency source section validation: %s", status.String())
	}
	offsets, values, status := typeddecode.Uint32OffsetsListView(manager, offsetsHandle, valuesHandle, directReq, typeddecode.ResourceViewOptions{RequireMapped: true})
	if status.Direct() {
		if len(values) != valuesCount {
			return nil, typeddecode.ReasonValuesLengthMismatch, fmt.Errorf("collections: column_graph layer-0 adjacency source values=%d want %d", len(values), valuesCount)
		}
		return &columnVectorGraphLayer0AdjacencyDirectSource{rows: rows, offsets: offsets, values: values, outcome: columnVectorGraphLayer0AdjacencySourceOutcomeMmapDirect, manager: manager, offsetsHandle: offsetsHandle, valuesHandle: valuesHandle}, "", nil
	}
	firstStatus := status
	if status.Reason == typeddecode.ReasonHandleSourceUnsupported {
		offsets, values, status = typeddecode.Uint32OffsetsListView(manager, offsetsHandle, valuesHandle, directReq, typeddecode.ResourceViewOptions{RequireMapped: false})
		if status.Direct() {
			if len(values) != valuesCount {
				return nil, typeddecode.ReasonValuesLengthMismatch, fmt.Errorf("collections: column_graph layer-0 adjacency source values=%d want %d", len(values), valuesCount)
			}
			return &columnVectorGraphLayer0AdjacencyDirectSource{rows: rows, offsets: offsets, values: values, outcome: columnVectorGraphLayer0AdjacencySourceOutcomeHeapCopyTypedView, manager: manager, offsetsHandle: offsetsHandle, valuesHandle: valuesHandle}, "", nil
		}
	}
	if status.Reason == "" {
		status = firstStatus
	}
	return nil, status.Reason, fmt.Errorf("collections: column_graph layer-0 adjacency source direct-view validation: %s", status.String())
}

func (s *columnVectorGraphLayer0AdjacencyDirectSource) Neighbors(ordinal int) ([]uint32, columnVectorGraphLayer0AdjacencySourceOutcome, typeddecode.Reason, bool) {
	if s == nil {
		return nil, columnVectorGraphLayer0AdjacencySourceOutcomeUnknown, "", false
	}
	if s.closed {
		return nil, columnVectorGraphLayer0AdjacencySourceOutcomeUnknown, typeddecode.ReasonStaleHandle, false
	}
	if s.offsetsHandle == nil || s.valuesHandle == nil || s.offsetsHandle.Released() || s.valuesHandle.Released() {
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
	s.activeHandles = 0
	return closeErr
}
