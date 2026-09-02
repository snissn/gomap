package collections

import (
	"errors"
	"fmt"
	"math"
	"unsafe"
)

var errColumnVectorGraphBlockViewRowOutOfBounds = errors.New("column_graph block view row outside block")

type columnVectorGraphOrdinalRef struct {
	assetOrdinal int
	rowIndex     int
}

type columnVectorGraphByteSpan struct {
	start int
	end   int
}

type columnVectorGraphVectorSpan struct {
	start int
	end   int
	dims  int
}

type columnVectorGraphAdjacencySpan struct {
	start int
	end   int
	count int
}

type columnVectorGraphSearchPlan struct {
	reader                *columnVectorGraphPhysicalRowReader
	physicalReader        *columnPhysicalRowReader
	blockViews            map[int]*columnVectorGraphBlockView
	singleBlockView       *columnVectorGraphBlockView
	ordinalRefs           []columnVectorGraphOrdinalRef
	ordinalAssigned       []bool
	ordinalRefsReady      bool
	singleOrdinalRange    bool
	scoreSource           columnVectorGraphSearchSource
	preparedSearch        *columnVectorGraphPreparedSearchView
	quantizedScorer       columnVectorGraphQuantizedScorer
	quantizedScorerActive bool
	scoreBatchMode        columnVectorGraphScoreBatchMode
	hits                  uint64
	misses                uint64
	builds                uint64
}

func (s *columnVectorGraphNativeSearchScratch) prepareSearchPlan(reader *columnVectorGraphPhysicalRowReader) (*columnVectorGraphSearchPlan, error) {
	return s.prepareSearchPlanInternal(reader, false)
}

func (s *columnVectorGraphNativeSearchScratch) prepareSearchPlanForNativeSearch(reader *columnVectorGraphPhysicalRowReader) (*columnVectorGraphSearchPlan, error) {
	return s.prepareSearchPlanInternal(reader, true)
}

func (s *columnVectorGraphNativeSearchScratch) prepareSearchPlanInternal(reader *columnVectorGraphPhysicalRowReader, useCombinedPrepared bool) (*columnVectorGraphSearchPlan, error) {
	if s == nil {
		return nil, errColumnVectorGraphNativeSearchScratchRequired
	}
	if reader == nil {
		return nil, errNilColumnVectorGraphPhysicalRowReader
	}
	physicalReader, err := reader.rowReader()
	if err != nil && columnVectorGraphManifestHasPhysicalAsset(reader.graph) {
		return nil, err
	}
	plan := &s.searchPlan
	if plan.reader != reader {
		for k := range plan.blockViews {
			delete(plan.blockViews, k)
		}
		plan.reader = reader
		plan.singleBlockView = nil
		plan.ordinalRefs = plan.ordinalRefs[:0]
		plan.ordinalAssigned = plan.ordinalAssigned[:0]
		plan.ordinalRefsReady = false
		plan.singleOrdinalRange = false
	}
	plan.physicalReader = physicalReader
	plan.preparedSearch = nil
	plan.quantizedScorer = columnVectorGraphQuantizedScorer{}
	plan.quantizedScorerActive = false
	plan.hits = 0
	plan.misses = 0
	plan.builds = 0
	if physicalReader != nil {
		if err := plan.prepareOrdinalRefs(); err != nil {
			return nil, err
		}
	} else {
		plan.ordinalRefsReady = true
		plan.singleOrdinalRange = true
	}
	if useCombinedPrepared && reader.preparedSearch != nil {
		if err := reader.preparedSearch.validateLive(); err != nil {
			return nil, fmt.Errorf("collections: column_graph %q combined prepared graph-search view is stale: %w", reader.def.Name, err)
		}
		plan.preparedSearch = reader.preparedSearch
		plan.scoreSource.reset()
		return plan, nil
	}
	// The source route is also the Windows compatibility route when prepared
	// mmap views are unavailable. Keep its already-validated bindings across
	// warm searches; rebuilding them for every query can make the hot path
	// allocate on that route. A closed or replaced source is prepared again so
	// stale state keeps the existing fail-closed behavior.
	if !plan.scoreSource.liveFor(plan) {
		if err := plan.scoreSource.prepare(plan); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

type columnVectorGraphBlockView struct {
	reader              *columnVectorGraphPhysicalRowReader
	block               *columnPhysicalRowReaderBlock
	idSpans             []columnVectorGraphByteSpan
	vectorSpans         []columnVectorGraphVectorSpan
	invNorms            []float32
	adjSpans            []columnVectorGraphAdjacencySpan
	rowValidated        []bool
	adjacencyDirectView bool
	invNormStateSource  bool
}

func newColumnVectorGraphSearchPlan(reader *columnVectorGraphPhysicalRowReader) (*columnVectorGraphSearchPlan, error) {
	if reader == nil {
		return nil, errNilColumnVectorGraphPhysicalRowReader
	}
	physicalReader, err := reader.rowReader()
	if err != nil && columnVectorGraphManifestHasPhysicalAsset(reader.graph) {
		return nil, err
	}
	return &columnVectorGraphSearchPlan{reader: reader, physicalReader: physicalReader}, nil
}

func (p *columnVectorGraphSearchPlan) prepareOrdinalRefs() error {
	if p == nil || p.reader == nil {
		return errNilColumnVectorGraphPhysicalRowReader
	}
	reader := p.physicalReader
	if reader == nil {
		var err error
		reader, err = p.reader.rowReader()
		if err != nil {
			return err
		}
		p.physicalReader = reader
	}
	if p.ordinalRefsReady {
		return nil
	}
	p.singleOrdinalRange = len(reader.ranges) == 1
	rowCount := reader.totalRows
	if p.singleOrdinalRange {
		rowRange := reader.ranges[0]
		if rowRange.assetOrdinal != 0 || rowRange.startOrdinal != 0 || rowRange.rowCount != rowCount {
			return fmt.Errorf("collections: column_graph single ordinal range asset=%d start=%d rows=%d row_count=%d: %w", rowRange.assetOrdinal, rowRange.startOrdinal, rowRange.rowCount, rowCount, errColumnPhysicalRowOrdinalOutOfBounds)
		}
		p.ordinalRefs = p.ordinalRefs[:0]
		p.ordinalAssigned = p.ordinalAssigned[:0]
		p.ordinalRefsReady = true
		return nil
	}
	if cap(p.ordinalRefs) < rowCount {
		p.ordinalRefs = make([]columnVectorGraphOrdinalRef, rowCount)
	} else {
		p.ordinalRefs = p.ordinalRefs[:rowCount]
	}
	if cap(p.ordinalAssigned) < rowCount {
		p.ordinalAssigned = make([]bool, rowCount)
	} else {
		p.ordinalAssigned = p.ordinalAssigned[:rowCount]
		clear(p.ordinalAssigned)
	}
	for _, rowRange := range reader.ranges {
		end := rowRange.startOrdinal + rowRange.rowCount
		if rowRange.startOrdinal < 0 || rowRange.rowCount < 0 || end < rowRange.startOrdinal || end > rowCount {
			return fmt.Errorf("collections: column_graph ordinal range asset=%d start=%d rows=%d outside row_count=%d: %w", rowRange.assetOrdinal, rowRange.startOrdinal, rowRange.rowCount, rowCount, errColumnPhysicalRowOrdinalOutOfBounds)
		}
		for ordinal := rowRange.startOrdinal; ordinal < end; ordinal++ {
			if p.ordinalAssigned[ordinal] {
				return fmt.Errorf("collections: column_graph ordinal=%d assigned by overlapping physical ranges: %w", ordinal, errColumnPhysicalRowOrdinalOutOfBounds)
			}
			p.ordinalAssigned[ordinal] = true
			p.ordinalRefs[ordinal] = columnVectorGraphOrdinalRef{assetOrdinal: rowRange.assetOrdinal, rowIndex: ordinal - rowRange.startOrdinal}
		}
	}
	for ordinal, assigned := range p.ordinalAssigned {
		if !assigned {
			return fmt.Errorf("collections: column_graph ordinal=%d not covered by physical ranges: %w", ordinal, errColumnPhysicalRowOrdinalOutOfBounds)
		}
	}
	p.ordinalRefsReady = true
	return nil
}

func (p *columnVectorGraphSearchPlan) ordinalRef(ordinal int) (columnVectorGraphOrdinalRef, error) {
	if p == nil || p.reader == nil {
		return columnVectorGraphOrdinalRef{}, errNilColumnVectorGraphPhysicalRowReader
	}
	reader := p.physicalReader
	if reader == nil {
		var err error
		reader, err = p.reader.rowReader()
		if err != nil {
			return columnVectorGraphOrdinalRef{}, err
		}
		p.physicalReader = reader
	}
	if !p.ordinalRefsReady {
		if err := p.prepareOrdinalRefs(); err != nil {
			return columnVectorGraphOrdinalRef{}, err
		}
	}
	if uint64(ordinal) >= uint64(reader.totalRows) {
		return columnVectorGraphOrdinalRef{}, fmt.Errorf("collections: physical column row ordinal=%d outside row_count=%d: %w", ordinal, reader.totalRows, errColumnPhysicalRowOrdinalOutOfBounds)
	}
	if p.singleOrdinalRange {
		return columnVectorGraphOrdinalRef{assetOrdinal: 0, rowIndex: ordinal}, nil
	}
	if ordinal >= len(p.ordinalRefs) {
		return columnVectorGraphOrdinalRef{}, fmt.Errorf("collections: physical column row ordinal=%d outside row_count=%d: %w", ordinal, reader.totalRows, errColumnPhysicalRowOrdinalOutOfBounds)
	}
	return p.ordinalRefs[ordinal], nil
}

func (p *columnVectorGraphSearchPlan) blockViewForOrdinal(ordinal int) (*columnVectorGraphBlockView, columnVectorGraphOrdinalRef, error) {
	if p != nil && p.physicalReader != nil && len(p.physicalReader.ranges) == 1 {
		if uint64(ordinal) >= uint64(p.physicalReader.totalRows) {
			return nil, columnVectorGraphOrdinalRef{}, fmt.Errorf("collections: physical column row ordinal=%d outside row_count=%d: %w", ordinal, p.physicalReader.totalRows, errColumnPhysicalRowOrdinalOutOfBounds)
		}
		view, err := p.blockViewForAssetOrdinal(0)
		if err != nil {
			return nil, columnVectorGraphOrdinalRef{}, err
		}
		return view, columnVectorGraphOrdinalRef{assetOrdinal: 0, rowIndex: ordinal}, nil
	}
	ref, err := p.ordinalRef(ordinal)
	if err != nil {
		return nil, columnVectorGraphOrdinalRef{}, err
	}
	view, err := p.blockViewForAssetOrdinal(ref.assetOrdinal)
	if err != nil {
		return nil, columnVectorGraphOrdinalRef{}, err
	}
	return view, ref, nil
}

func (p *columnVectorGraphSearchPlan) blockViewForAssetOrdinal(assetOrdinal int) (*columnVectorGraphBlockView, error) {
	if p == nil || p.reader == nil {
		return nil, errNilColumnVectorGraphPhysicalRowReader
	}
	reader := p.physicalReader
	if reader == nil {
		var err error
		reader, err = p.reader.rowReader()
		if err != nil {
			return nil, err
		}
		p.physicalReader = reader
	}
	if reader.closed {
		return nil, errors.New("collections: physical column row reader is closed")
	}
	if len(reader.ranges) == 1 && assetOrdinal == 0 {
		if p.singleBlockView != nil {
			p.hits++
			return p.singleBlockView, nil
		}
	}
	if assetOrdinal < 0 || assetOrdinal >= len(reader.ranges) {
		return nil, fmt.Errorf("collections: column_graph block view asset ordinal=%d outside ranges=%d", assetOrdinal, len(reader.ranges))
	}
	if p.blockViews != nil {
		if view := p.blockViews[assetOrdinal]; view != nil {
			p.hits++
			return view, nil
		}
	}
	p.misses++
	block, err := reader.loadBlock(reader.ranges[assetOrdinal])
	if err != nil {
		return nil, err
	}
	view, err := newColumnVectorGraphBlockView(p.reader, block)
	if err != nil {
		return nil, err
	}
	p.builds++
	if len(reader.ranges) == 1 && assetOrdinal == 0 {
		p.singleBlockView = view
	} else {
		if p.blockViews == nil {
			p.blockViews = make(map[int]*columnVectorGraphBlockView, reader.maxBlocks)
		}
		if reader.maxBlocks > 0 && len(p.blockViews) >= reader.maxBlocks {
			for existing := range p.blockViews {
				delete(p.blockViews, existing)
				break
			}
		}
		p.blockViews[assetOrdinal] = view
	}
	return view, nil
}

func newColumnVectorGraphBlockView(reader *columnVectorGraphPhysicalRowReader, block *columnPhysicalRowReaderBlock) (*columnVectorGraphBlockView, error) {
	if reader == nil {
		return nil, errNilColumnVectorGraphPhysicalRowReader
	}
	if block == nil {
		return nil, errors.New("collections: column_graph block view requires block")
	}
	rows := len(block.rowOffsets)
	view := &columnVectorGraphBlockView{
		reader:              reader,
		block:               block,
		idSpans:             make([]columnVectorGraphByteSpan, rows),
		vectorSpans:         make([]columnVectorGraphVectorSpan, rows),
		adjSpans:            make([]columnVectorGraphAdjacencySpan, rows),
		rowValidated:        make([]bool, rows),
		adjacencyDirectView: columnVectorGraphBlockViewAdjacencyDirectView(reader),
		invNormStateSource:  reader.usesInvNormStateSource(),
	}
	if !view.invNormStateSource {
		view.invNorms = make([]float32, rows)
	}
	for rowIndex := 0; rowIndex < rows; rowIndex++ {
		if err := view.indexRow(rowIndex); err != nil {
			return nil, err
		}
	}
	return view, nil
}

func (v *columnVectorGraphBlockView) indexRow(rowIndex int) error {
	cur := manifestCursor{raw: v.block.raw, pos: v.block.rowOffsets[rowIndex]}
	idStart, idEnd, err := columnVectorGraphReadBytesSpan(&cur)
	if err != nil {
		return err
	}
	deleted := false
	if v.block.version >= columnPhysicalAssetVersionV2 {
		deleted = cur.bool()
	}
	if cur.err != nil {
		return cur.err
	}
	ordinal := v.ordinalForRowIndex(rowIndex)
	if len(v.block.raw[idStart:idEnd]) == 0 {
		return fmt.Errorf("collections: column_graph %q ordinal=%d missing document id", v.reader.def.Name, ordinal)
	}
	if deleted {
		if v.block.header.Operation != ColumnPublishOperationDelete {
			return fmt.Errorf("column physical asset %s row[%d] is marked deleted", v.block.header.Operation, rowIndex)
		}
		return fmt.Errorf("collections: column_graph %q ordinal=%d row is deleted", v.reader.def.Name, ordinal)
	}
	if v.block.header.Operation == ColumnPublishOperationDelete {
		return fmt.Errorf("column physical asset delete row[%d] is not marked deleted", rowIndex)
	}
	v.idSpans[rowIndex] = columnVectorGraphByteSpan{start: idStart, end: idEnd}

	vectorSpan, err := v.indexVector(&cur, ordinal)
	if err != nil {
		return fmt.Errorf("row[%d]: %w", rowIndex, err)
	}
	var invNorm float32
	if v.invNormStateSource {
		if err := v.skipInvNorm(&cur, ordinal); err != nil {
			return fmt.Errorf("row[%d]: %w", rowIndex, err)
		}
	} else {
		var err error
		invNorm, err = v.indexInvNorm(&cur, ordinal)
		if err != nil {
			return fmt.Errorf("row[%d]: %w", rowIndex, err)
		}
	}
	adjSpan, err := v.indexAdjacency(&cur, ordinal)
	if err != nil {
		return fmt.Errorf("row[%d]: %w", rowIndex, err)
	}
	if cur.err != nil {
		return cur.err
	}
	v.vectorSpans[rowIndex] = vectorSpan
	if !v.invNormStateSource {
		v.invNorms[rowIndex] = invNorm
	}
	v.adjSpans[rowIndex] = adjSpan
	v.rowValidated[rowIndex] = true
	return nil
}

func (v *columnVectorGraphBlockView) ordinalForRowIndex(rowIndex int) int {
	if v == nil || v.reader == nil || v.reader.reader == nil || v.block == nil {
		return rowIndex
	}
	assetOrdinal := v.block.assetOrdinal
	if assetOrdinal < 0 || assetOrdinal >= len(v.reader.reader.ranges) {
		return rowIndex
	}
	return v.reader.reader.ranges[assetOrdinal].startOrdinal + rowIndex
}

func (v *columnVectorGraphBlockView) indexVector(cur *manifestCursor, ordinal int) (columnVectorGraphVectorSpan, error) {
	if err := v.readGraphHeader(cur, v.block.version, ordinal, 0, ColumnStoreValueFloat32Vector); err != nil {
		return columnVectorGraphVectorSpan{}, err
	}
	n := cur.u64()
	if cur.err != nil {
		return columnVectorGraphVectorSpan{}, cur.err
	}
	if n != uint64(v.reader.def.Dimensions) {
		return columnVectorGraphVectorSpan{}, fmt.Errorf("collections: column_graph %q ordinal=%d vector dims=%d want %d", v.reader.def.Name, ordinal, n, v.reader.def.Dimensions)
	}
	byteLen, ok := cur.fixedWidthSliceByteLen(n, 4, "float32_vector")
	if !ok {
		return columnVectorGraphVectorSpan{}, cur.err
	}
	start := cur.pos
	cur.pos += int(byteLen)
	return columnVectorGraphVectorSpan{start: start, end: cur.pos, dims: int(n)}, nil
}

func (v *columnVectorGraphBlockView) indexInvNorm(cur *manifestCursor, ordinal int) (float32, error) {
	if err := v.readGraphHeader(cur, v.block.version, ordinal, 1, ColumnStoreValueFloat32); err != nil {
		return 0, err
	}
	invNorm := math.Float32frombits(cur.u32())
	if cur.err != nil {
		return 0, cur.err
	}
	if invNorm <= 0 || math.IsNaN(float64(invNorm)) || math.IsInf(float64(invNorm), 0) {
		return 0, fmt.Errorf("collections: column_graph %q ordinal=%d invalid inv_norm=%v", v.reader.def.Name, ordinal, invNorm)
	}
	return invNorm, nil
}

func (v *columnVectorGraphBlockView) skipInvNorm(cur *manifestCursor, ordinal int) error {
	if err := v.readGraphHeader(cur, v.block.version, ordinal, 1, ColumnStoreValueFloat32); err != nil {
		return err
	}
	_ = cur.u32()
	return cur.err
}

func (v *columnVectorGraphBlockView) indexAdjacency(cur *manifestCursor, ordinal int) (columnVectorGraphAdjacencySpan, error) {
	if err := v.readGraphHeader(cur, v.block.version, ordinal, 2, ColumnStoreValueAdjacencyList); err != nil {
		return columnVectorGraphAdjacencySpan{}, err
	}
	n := cur.u64()
	if cur.err != nil {
		return columnVectorGraphAdjacencySpan{}, cur.err
	}
	byteLen, ok := cur.fixedWidthSliceByteLen(n, 4, "uint32 slice")
	if !ok {
		return columnVectorGraphAdjacencySpan{}, cur.err
	}
	start := cur.pos
	cur.pos += int(byteLen)
	return columnVectorGraphAdjacencySpan{start: start, end: cur.pos, count: int(n)}, nil
}

func (v *columnVectorGraphBlockView) readGraphHeader(cur *manifestCursor, version uint16, ordinal, colIdx int, want ColumnStoreValueType) error {
	typeBytes := cur.stringBytes()
	if cur.err != nil {
		return cur.err
	}
	if !columnPhysicalBytesEqualString(typeBytes, string(want)) {
		return fmt.Errorf("column[%d] type=%q want %q", colIdx, string(typeBytes), want)
	}
	null := cur.bool()
	if cur.err != nil {
		return cur.err
	}
	present := true
	if version >= columnPhysicalAssetVersionV3 {
		present = cur.bool()
		if cur.err != nil {
			return cur.err
		}
	}
	if !present {
		return fmt.Errorf("collections: column_graph %q ordinal=%d missing graph value", v.reader.def.Name, ordinal)
	}
	if null {
		return fmt.Errorf("collections: column_graph %q ordinal=%d contains null graph value", v.reader.def.Name, ordinal)
	}
	return nil
}

func (v *columnVectorGraphBlockView) checkRowIndex(rowIndex int) error {
	if v == nil || v.block == nil || rowIndex < 0 || rowIndex >= len(v.rowValidated) || !v.rowValidated[rowIndex] {
		return errColumnVectorGraphBlockViewRowOutOfBounds
	}
	return nil
}

func (v *columnVectorGraphBlockView) id(rowIndex int) ([]byte, error) {
	if err := v.checkRowIndex(rowIndex); err != nil {
		return nil, err
	}
	span := v.idSpans[rowIndex]
	return v.block.raw[span.start:span.end], nil
}

func (v *columnVectorGraphBlockView) vector(rowIndex int, scratch []float32) ([]float32, []float32, error) {
	if err := v.checkRowIndex(rowIndex); err != nil {
		return nil, scratch, err
	}
	vector, scratch := v.vectorUnchecked(rowIndex, scratch)
	return vector, scratch, nil
}

func (v *columnVectorGraphBlockView) vectorUnchecked(rowIndex int, scratch []float32) ([]float32, []float32) {
	span := v.vectorSpans[rowIndex]
	if span.dims == 0 {
		return nil, scratch
	}
	base := len(scratch)
	need := base + span.dims
	if cap(scratch) < need {
		next := make([]float32, need)
		copy(next, scratch)
		scratch = next
	} else {
		scratch = scratch[:need]
	}
	if columnPhysicalNativeLittleEndian {
		columnPhysicalCopyLittleEndianFloat32Bytes(scratch[base:need], v.block.raw[span.start:span.end])
		return scratch[base:need], scratch
	}
	pos := span.start
	for i := base; i < need; i++ {
		scratch[i] = math.Float32frombits(uint32(v.block.raw[pos]) | uint32(v.block.raw[pos+1])<<8 | uint32(v.block.raw[pos+2])<<16 | uint32(v.block.raw[pos+3])<<24)
		pos += 4
	}
	return scratch[base:], scratch
}

func (v *columnVectorGraphBlockView) invNorm(rowIndex int) (float32, error) {
	if err := v.checkRowIndex(rowIndex); err != nil {
		return 0, err
	}
	if v.invNormStateSource {
		ordinal := v.ordinalForRowIndex(rowIndex)
		if invNorm, _, _, ok := v.reader.invNormForOrdinal(ordinal); ok {
			return invNorm, nil
		}
		return v.legacyInvNorm(rowIndex)
	}
	return v.invNormUnchecked(rowIndex), nil
}

func (v *columnVectorGraphBlockView) legacyInvNorm(rowIndex int) (float32, error) {
	if err := v.checkRowIndex(rowIndex); err != nil {
		return 0, err
	}
	if rowIndex < len(v.invNorms) {
		return v.invNorms[rowIndex], nil
	}
	ordinal := v.ordinalForRowIndex(rowIndex)
	cur := manifestCursor{raw: v.block.raw, pos: v.block.rowOffsets[rowIndex]}
	if _, _, err := columnVectorGraphReadBytesSpan(&cur); err != nil {
		return 0, err
	}
	if v.block.version >= columnPhysicalAssetVersionV2 {
		_ = cur.bool()
		if cur.err != nil {
			return 0, cur.err
		}
	}
	if _, err := v.indexVector(&cur, ordinal); err != nil {
		return 0, err
	}
	return v.indexInvNorm(&cur, ordinal)
}

func (v *columnVectorGraphBlockView) invNormUnchecked(rowIndex int) float32 {
	if rowIndex < 0 || rowIndex >= len(v.invNorms) {
		return 0
	}
	return v.invNorms[rowIndex]
}

func (v *columnVectorGraphBlockView) adjacency(rowIndex int, scratch []uint32) ([]uint32, []uint32, bool, error) {
	if err := v.checkRowIndex(rowIndex); err != nil {
		return nil, scratch, false, err
	}
	span := v.adjSpans[rowIndex]
	if span.count == 0 {
		return nil, scratch, false, nil
	}
	if v.adjacencyDirectView {
		adjacency, ok := columnVectorGraphLittleEndianUint32DirectView(v.block.raw[span.start:span.end], span.count)
		if ok {
			return adjacency, scratch, true, nil
		}
	}
	base := len(scratch)
	need := base + span.count
	if cap(scratch) < need {
		next := make([]uint32, need)
		copy(next, scratch)
		scratch = next
	} else {
		scratch = scratch[:need]
	}
	// Adjacency uses the current big-endian row-record payload encoding; dense
	// little-endian adjacency blocks are a later format-level optimization.
	pos := span.start
	if v.adjacencyLittleEndian() {
		for i := base; i < need; i++ {
			scratch[i] = uint32(v.block.raw[pos]) | uint32(v.block.raw[pos+1])<<8 | uint32(v.block.raw[pos+2])<<16 | uint32(v.block.raw[pos+3])<<24
			pos += 4
		}
		return scratch[base:], scratch, false, nil
	}
	for i := base; i < need; i++ {
		scratch[i] = uint32(v.block.raw[pos])<<24 | uint32(v.block.raw[pos+1])<<16 | uint32(v.block.raw[pos+2])<<8 | uint32(v.block.raw[pos+3])
		pos += 4
	}
	return scratch[base:], scratch, false, nil
}

func columnVectorGraphBlockViewAdjacencyDirectView(reader *columnVectorGraphPhysicalRowReader) bool {
	// Physical row-asset direct views are deferred to #1897 and adjacency direct
	// views are deferred to #1901. Keep little-endian payload decode
	// compatibility, but do not classify row-asset adjacency spans as
	// current-stack mmap direct views.
	return false
}

func columnVectorGraphLittleEndianUint32DirectView(raw []byte, count int) ([]uint32, bool) {
	if count < 0 || len(raw)%4 != 0 || len(raw)/4 != count {
		return nil, false
	}
	if count == 0 {
		return nil, true
	}
	ptr := unsafe.Pointer(unsafe.SliceData(raw))
	if !columnPhysicalNativeLittleEndian || uintptr(ptr)%unsafe.Alignof(uint32(0)) != 0 {
		return nil, false
	}
	return unsafe.Slice((*uint32)(ptr), count), true
}

func (v *columnVectorGraphBlockView) adjacencyLittleEndian() bool {
	if v == nil || v.reader == nil || v.reader.reader == nil {
		return false
	}
	cols := v.reader.reader.view.Config.Columns
	if len(cols) <= columnVectorGraphPhysicalRowValueAdjacency {
		return false
	}
	return cols[columnVectorGraphPhysicalRowValueAdjacency].FixedWidthEncoding == ColumnFixedWidthEncodingLittleEndian
}

func columnVectorGraphReadBytesSpan(cur *manifestCursor) (int, int, error) {
	if cur.err != nil {
		return 0, 0, cur.err
	}
	n := cur.u64()
	if cur.err != nil {
		return 0, 0, cur.err
	}
	if n > uint64(len(cur.raw)-cur.pos) {
		cur.err = errors.New("collections: short column binary bytes")
		return 0, 0, cur.err
	}
	start := cur.pos
	cur.pos += int(n)
	return start, cur.pos, nil
}
