package collections

import (
	"errors"
	"fmt"
	"math"
	"strconv"
)

const defaultColumnPhysicalRowReaderMaxDecodedBlocks = 4

var errColumnPhysicalRowOrdinalOutOfBounds = errors.New("physical column row ordinal outside row_count")

type columnPhysicalRowReaderOptions struct {
	ProjectedColumns  []string
	MaxDecodedBlocks  int
	RequireInsertOnly bool
}

type columnPhysicalRowReaderStats struct {
	Rows                   int
	Granules               int
	OpenGranulesRead       int
	OpenPhysicalBytesRead  int64
	OpenSegmentCacheHits   uint64
	OpenSegmentCacheMisses uint64

	RowFetches             uint64
	BatchFetches           uint64
	RowsFetched            uint64
	CacheHits              uint64
	CacheMisses            uint64
	DecodedBlocks          uint64
	BlockEvictions         uint64
	GranulesTouched        uint64
	PhysicalBytesRead      int64
	ResidentBytes          int64
	MaxResidentBytes       int64
	SegmentFileCacheHits   uint64
	SegmentFileCacheMisses uint64
}

type columnPhysicalRowReaderScratch struct {
	Values        []columnDeclaredValue
	Float32Values []float32
	Uint32Values  []uint32
}

// columnPhysicalRowReader is a bounded, row-oriented physical column reader.
// It is intentionally generic column-store machinery: callers fetch projected
// rows by ordinal and provide worker-local scratch. Returned IDs alias cached
// asset bytes; returned values and vector/adjacency slices alias scratch and are
// invalid after the next fetch using the same scratch or after reader close.
//
// The reader is not concurrency-safe. Parallel consumers must use independent
// readers or add synchronization above this internal contract.
type columnPhysicalRowReader struct {
	view       columnPhysicalScanSnapshotView
	projection columnPhysicalScanProjection
	readCache  columnPhysicalAssetReadCache
	closeView  func()
	ranges     []columnPhysicalRowReaderRange
	totalRows  int
	maxBlocks  int
	blocks     map[int]*columnPhysicalRowReaderBlock
	lru        []int
	// lastRange/lastBlock are a one-entry hot path over the existing bounded
	// block cache. They avoid repeating binary range lookup and map/LRU work
	// when graph/native readers fetch many rows from the same physical granule.
	lastRange int
	lastBlock *columnPhysicalRowReaderBlock
	closed    bool
	stats     columnPhysicalRowReaderStats
}

type columnPhysicalRowReaderRange struct {
	assetOrdinal int
	ref          ColumnAssetRef
	startOrdinal int
	rowCount     int
	version      uint16
	header       columnPhysicalAssetScanHeader
	rowsOffset   int
}

type columnPhysicalRowReaderBlock struct {
	assetOrdinal  int
	raw           []byte
	version       uint16
	header        columnPhysicalAssetScanHeader
	rowOffsets    []int
	residentBytes int64
}

type columnPhysicalRowReaderRow struct {
	Generation        uint64
	PartID            uint64
	AppliedCommandLSN uint64
	Operation         ColumnPublishOperation
	Ordinal           int
	RowIndex          int
	ID                []byte
	Values            []columnDeclaredValue
	Deleted           bool
}

func (c *Collection) openColumnPhysicalRowReader(opts columnPhysicalRowReaderOptions) (*columnPhysicalRowReader, error) {
	view, closeView, err := c.prepareColumnPhysicalScanSnapshotView()
	if err != nil {
		if closeView != nil {
			closeView()
		}
		return nil, err
	}
	return newColumnPhysicalRowReaderFromSnapshotViewWithClose(view, opts, closeView)
}

func newColumnPhysicalRowReaderFromSnapshotView(view columnPhysicalScanSnapshotView, opts columnPhysicalRowReaderOptions) (*columnPhysicalRowReader, error) {
	return newColumnPhysicalRowReaderFromSnapshotViewWithClose(view, opts, nil)
}

func newColumnPhysicalRowReaderFromSnapshotViewWithClose(view columnPhysicalScanSnapshotView, opts columnPhysicalRowReaderOptions, closeView func()) (_ *columnPhysicalRowReader, err error) {
	cleanupCloseView := closeView
	if cleanupCloseView != nil {
		defer func() {
			if err != nil && cleanupCloseView != nil {
				cleanupCloseView()
			}
		}()
	}
	cfg := view.Config
	if !view.ColumnStoreEnabled || !cfg.Enabled {
		return nil, errors.New("collections: physical column row reader requires enabled column_store")
	}
	if cfg.ActiveManifest == nil {
		return nil, errors.New("collections: physical column row reader requires active column manifest")
	}
	if opts.RequireInsertOnly && view.MutationParts != 0 {
		return nil, errColumnPhysicalQueryNeedsVisibility
	}
	if opts.MaxDecodedBlocks < 0 {
		return nil, errors.New("collections: physical column row reader max decoded blocks cannot be negative")
	}
	maxBlocks := opts.MaxDecodedBlocks
	if maxBlocks == 0 {
		maxBlocks = defaultColumnPhysicalRowReaderMaxDecodedBlocks
	}
	projection, err := newColumnPhysicalScanProjection(cfg, opts.ProjectedColumns)
	if err != nil {
		return nil, err
	}
	readCache, err := newColumnPhysicalAssetReadCache(view.ColumnAssetRootDir, view.AssetNamespace)
	if err != nil {
		return nil, err
	}
	reader := &columnPhysicalRowReader{
		view:       view,
		projection: projection,
		readCache:  readCache,
		closeView:  closeView,
		maxBlocks:  maxBlocks,
		blocks:     make(map[int]*columnPhysicalRowReaderBlock, maxBlocks),
		lastRange:  -1,
		stats: columnPhysicalRowReaderStats{
			Granules: len(view.AssetRefs),
		},
	}
	cleanupCloseView = nil
	if err := reader.buildOrdinalRanges(); err != nil {
		_ = reader.Close()
		return nil, err
	}
	return reader, nil
}

func (r *columnPhysicalRowReader) Close() error {
	if r == nil || r.closed {
		return nil
	}
	r.closed = true
	var closeErr error
	if err := r.readCache.close(); err != nil {
		closeErr = err
	}
	for key := range r.blocks {
		delete(r.blocks, key)
	}
	r.lru = r.lru[:0]
	r.lastRange = -1
	r.lastBlock = nil
	r.stats.ResidentBytes = 0
	if r.closeView != nil {
		r.closeView()
		r.closeView = nil
	}
	return closeErr
}

func (r *columnPhysicalRowReader) RowCount() int {
	if r == nil {
		return 0
	}
	return r.totalRows
}

func (r *columnPhysicalRowReader) Stats() columnPhysicalRowReaderStats {
	if r == nil {
		return columnPhysicalRowReaderStats{}
	}
	stats := r.stats
	stats.SegmentFileCacheHits = r.readCache.hits
	stats.SegmentFileCacheMisses = r.readCache.misses
	return stats
}

func (r *columnPhysicalRowReader) FetchRow(ordinal int, scratch *columnPhysicalRowReaderScratch) (columnPhysicalRowReaderRow, error) {
	if r == nil {
		return columnPhysicalRowReaderRow{}, errors.New("collections: nil physical column row reader")
	}
	if r.closed {
		return columnPhysicalRowReaderRow{}, errors.New("collections: physical column row reader is closed")
	}
	if scratch == nil {
		return columnPhysicalRowReaderRow{}, errors.New("collections: physical column row reader requires caller-owned scratch")
	}
	r.stats.RowFetches++
	row, err := r.fetchRow(ordinal, scratch)
	if err != nil {
		return columnPhysicalRowReaderRow{}, err
	}
	r.stats.RowsFetched++
	return row, nil
}

func (r *columnPhysicalRowReader) FetchBatch(ordinals []int, scratch *columnPhysicalRowReaderScratch, visitor func(columnPhysicalRowReaderRow) error) error {
	if r == nil {
		return errors.New("collections: nil physical column row reader")
	}
	if r.closed {
		return errors.New("collections: physical column row reader is closed")
	}
	if scratch == nil {
		return errors.New("collections: physical column row reader requires caller-owned scratch")
	}
	if visitor == nil {
		return errors.New("collections: physical column row reader batch visitor is nil")
	}
	r.stats.BatchFetches++
	for _, ordinal := range ordinals {
		row, err := r.fetchRow(ordinal, scratch)
		if err != nil {
			return err
		}
		r.stats.RowsFetched++
		if err := visitor(row); err != nil {
			return err
		}
	}
	return nil
}

func (r *columnPhysicalRowReader) buildOrdinalRanges() error {
	if len(r.view.AssetRefs) == 0 {
		r.stats.Rows = 0
		return nil
	}
	ranges := make([]columnPhysicalRowReaderRange, 0, len(r.view.AssetRefs))
	var rawScratch []byte
	totalRows := 0
	for assetOrdinal, assetRef := range r.view.AssetRefs {
		ref := assetRef.Ref
		raw, err := r.readCache.read(ref, rawScratch)
		r.stats.OpenSegmentCacheHits = r.readCache.hits
		r.stats.OpenSegmentCacheMisses = r.readCache.misses
		if err != nil {
			return fmt.Errorf("collections: physical column row reader open read generation=%d part_id=%d: %w", ref.Generation, ref.PartID, err)
		}
		rawScratch = raw
		r.stats.OpenGranulesRead++
		r.stats.OpenPhysicalBytesRead += int64(len(raw))
		header, version, rowsOffset, err := parseColumnPhysicalAssetScanHeader(raw, ref, r.view.CollectionName, &r.view.Config, assetRef.Reason)
		if err != nil {
			return fmt.Errorf("collections: physical column row reader open decode generation=%d part_id=%d: %w", ref.Generation, ref.PartID, err)
		}
		if header.RowCount > maxCollectionInt-totalRows {
			return errors.New("collections: physical column row reader row count overflows int")
		}
		ranges = append(ranges, columnPhysicalRowReaderRange{
			assetOrdinal: assetOrdinal,
			ref:          ref,
			startOrdinal: totalRows,
			rowCount:     header.RowCount,
			version:      version,
			header:       header,
			rowsOffset:   rowsOffset,
		})
		totalRows += header.RowCount
	}
	r.ranges = ranges
	r.totalRows = totalRows
	r.stats.Rows = totalRows
	return nil
}

func (r *columnPhysicalRowReader) fetchRow(ordinal int, scratch *columnPhysicalRowReaderScratch) (columnPhysicalRowReaderRow, error) {
	rangeIdx := r.rangeIndexForOrdinal(ordinal)
	if rangeIdx < 0 {
		return columnPhysicalRowReaderRow{}, fmt.Errorf("collections: physical column row ordinal=%d outside row_count=%d: %w", ordinal, r.totalRows, errColumnPhysicalRowOrdinalOutOfBounds)
	}
	rowRange := r.ranges[rangeIdx]
	block, err := r.loadBlock(rowRange)
	if err != nil {
		return columnPhysicalRowReaderRow{}, err
	}
	rowIndex := ordinal - rowRange.startOrdinal
	if rowIndex < 0 || rowIndex >= len(block.rowOffsets) {
		return columnPhysicalRowReaderRow{}, fmt.Errorf("collections: physical column row index=%d outside block rows=%d", rowIndex, len(block.rowOffsets))
	}
	return r.decodeRowFromBlock(block, ordinal, rowIndex, scratch)
}

func (r *columnPhysicalRowReader) rangeIndexForOrdinal(ordinal int) int {
	if ordinal < 0 || ordinal >= r.totalRows {
		return -1
	}
	if r.lastRange >= 0 && r.lastRange < len(r.ranges) {
		rowRange := r.ranges[r.lastRange]
		if ordinal >= rowRange.startOrdinal && ordinal < rowRange.startOrdinal+rowRange.rowCount {
			return r.lastRange
		}
	}
	if len(r.ranges) == 1 {
		// Single-range readers are common for column_graph assets; skip binary
		// search entirely after the bounds check above.
		r.lastRange = 0
		return 0
	}
	lo, hi := 0, len(r.ranges)
	for lo < hi {
		mid := lo + (hi-lo)/2
		rowRange := r.ranges[mid]
		if ordinal < rowRange.startOrdinal {
			hi = mid
			continue
		}
		if ordinal >= rowRange.startOrdinal+rowRange.rowCount {
			lo = mid + 1
			continue
		}
		r.lastRange = mid
		return mid
	}
	return -1
}

func (r *columnPhysicalRowReader) loadBlock(rowRange columnPhysicalRowReaderRange) (*columnPhysicalRowReaderBlock, error) {
	assetOrdinal := rowRange.assetOrdinal
	if block := r.lastBlock; block != nil && block.assetOrdinal == assetOrdinal {
		r.stats.CacheHits++
		// lastBlock matches only when this block was the previous access, so it
		// is already at the LRU tail. Interleaved access changes lastBlock and
		// falls through to the map path, which refreshes LRU with touchBlock.
		return block, nil
	}
	if block := r.blocks[assetOrdinal]; block != nil {
		r.stats.CacheHits++
		r.touchBlock(assetOrdinal)
		r.lastBlock = block
		return block, nil
	}
	r.stats.CacheMisses++
	if assetOrdinal < 0 || assetOrdinal >= len(r.view.AssetRefs) {
		return nil, fmt.Errorf("collections: physical column row reader asset ordinal=%d outside refs=%d", assetOrdinal, len(r.view.AssetRefs))
	}
	var dst []byte
	if rowRange.ref.Length >= 0 && rowRange.ref.Length <= int64(maxCollectionInt) {
		dst = make([]byte, int(rowRange.ref.Length))
	}
	// Cached blocks must own stable bytes. The asset read cache reuses scratch
	// for syscall reads, so storing that scratch would let later cache misses
	// corrupt already-indexed row offsets. Malformed lengths skip the owned
	// destination and are rejected by the read/index checks below.
	raw, err := r.readCache.read(rowRange.ref, dst)
	if err != nil {
		return nil, fmt.Errorf("collections: physical column row reader read generation=%d part_id=%d: %w", rowRange.ref.Generation, rowRange.ref.PartID, err)
	}
	if dst != nil {
		if len(raw) != len(dst) {
			return nil, fmt.Errorf("collections: physical column row reader read generation=%d part_id=%d length=%d want %d", rowRange.ref.Generation, rowRange.ref.PartID, len(raw), len(dst))
		}
		if len(raw) > 0 && &raw[0] != &dst[0] {
			copy(dst, raw)
			raw = dst
		}
	}
	rowOffsets, err := indexColumnPhysicalAssetReaderRows(raw, rowRange.version, rowRange.rowsOffset, rowRange.header, &r.view.Config)
	if err != nil {
		return nil, fmt.Errorf("collections: physical column row reader index generation=%d part_id=%d: %w", rowRange.ref.Generation, rowRange.ref.PartID, err)
	}
	block := &columnPhysicalRowReaderBlock{
		assetOrdinal: assetOrdinal,
		raw:          raw,
		version:      rowRange.version,
		header:       rowRange.header,
		rowOffsets:   rowOffsets,
	}
	block.residentBytes = int64(len(block.raw)) + int64(len(block.rowOffsets))*(strconv.IntSize/8)
	r.evictBlocksForInsert()
	r.blocks[assetOrdinal] = block
	r.lru = append(r.lru, assetOrdinal)
	r.lastBlock = block
	r.stats.DecodedBlocks++
	r.stats.GranulesTouched++
	r.stats.PhysicalBytesRead += int64(len(raw))
	r.stats.ResidentBytes += block.residentBytes
	if r.stats.ResidentBytes > r.stats.MaxResidentBytes {
		r.stats.MaxResidentBytes = r.stats.ResidentBytes
	}
	return block, nil
}

func (r *columnPhysicalRowReader) touchBlock(assetOrdinal int) {
	for i, existing := range r.lru {
		if existing != assetOrdinal {
			continue
		}
		copy(r.lru[i:], r.lru[i+1:])
		r.lru[len(r.lru)-1] = assetOrdinal
		return
	}
	panic(fmt.Sprintf("collections: physical column row reader LRU missing cached asset ordinal=%d", assetOrdinal))
}

func (r *columnPhysicalRowReader) evictBlocksForInsert() {
	for len(r.blocks) >= r.maxBlocks && len(r.lru) > 0 {
		evict := r.lru[0]
		copy(r.lru, r.lru[1:])
		r.lru = r.lru[:len(r.lru)-1]
		block := r.blocks[evict]
		delete(r.blocks, evict)
		if block != nil {
			if r.lastBlock == block {
				r.lastBlock = nil
			}
			r.stats.ResidentBytes -= block.residentBytes
			if r.stats.ResidentBytes < 0 {
				r.stats.ResidentBytes = 0
			}
		}
		r.stats.BlockEvictions++
	}
}

func (r *columnPhysicalRowReader) decodeRowFromBlock(block *columnPhysicalRowReaderBlock, ordinal, rowIndex int, scratch *columnPhysicalRowReaderScratch) (columnPhysicalRowReaderRow, error) {
	cur := manifestCursor{raw: block.raw, pos: block.rowOffsets[rowIndex]}
	id := cur.bytesView()
	deleted := false
	if block.version >= columnPhysicalAssetVersionV2 {
		deleted = cur.bool()
	}
	if cur.err != nil {
		return columnPhysicalRowReaderRow{}, cur.err
	}
	scratch.Values = scratch.Values[:0]
	scratch.Float32Values = scratch.Float32Values[:0]
	scratch.Uint32Values = scratch.Uint32Values[:0]
	if deleted {
		if block.header.Operation != ColumnPublishOperationDelete {
			return columnPhysicalRowReaderRow{}, fmt.Errorf("column physical asset %s row[%d] is marked deleted", block.header.Operation, rowIndex)
		}
	} else {
		if block.header.Operation == ColumnPublishOperationDelete {
			return columnPhysicalRowReaderRow{}, fmt.Errorf("column physical asset delete row[%d] is not marked deleted", rowIndex)
		}
		if cap(scratch.Values) < r.projection.count {
			scratch.Values = make([]columnDeclaredValue, r.projection.count)
		} else {
			scratch.Values = scratch.Values[:r.projection.count]
			clear(scratch.Values)
		}
		if err := readColumnPhysicalRowValuesIntoScratch(&cur, block.version, &r.view.Config, r.projection, scratch); err != nil {
			return columnPhysicalRowReaderRow{}, fmt.Errorf("row[%d]: %w", rowIndex, err)
		}
	}
	if cur.err != nil {
		return columnPhysicalRowReaderRow{}, cur.err
	}
	return columnPhysicalRowReaderRow{
		Generation:        block.header.Generation,
		PartID:            block.header.PartID,
		AppliedCommandLSN: block.header.AppliedCommandLSN,
		Operation:         block.header.Operation,
		Ordinal:           ordinal,
		RowIndex:          rowIndex,
		ID:                id,
		Values:            scratch.Values,
		Deleted:           deleted,
	}, nil
}

func indexColumnPhysicalAssetReaderRows(raw []byte, version uint16, rowsOffset int, header columnPhysicalAssetScanHeader, cfg *ColumnStoreConfig) ([]int, error) {
	if rowsOffset < 0 || rowsOffset > len(raw) {
		return nil, fmt.Errorf("column physical asset invalid rows offset=%d len=%d", rowsOffset, len(raw))
	}
	offsets := make([]int, header.RowCount)
	cur := manifestCursor{raw: raw, pos: rowsOffset}
	for rowIdx := 0; rowIdx < header.RowCount; rowIdx++ {
		offsets[rowIdx] = cur.pos
		_ = cur.bytesView()
		deleted := false
		if version >= columnPhysicalAssetVersionV2 {
			deleted = cur.bool()
		}
		if cur.err != nil {
			return nil, cur.err
		}
		if deleted {
			if header.Operation != ColumnPublishOperationDelete {
				return nil, fmt.Errorf("column physical asset %s row[%d] is marked deleted", header.Operation, rowIdx)
			}
			continue
		}
		if header.Operation == ColumnPublishOperationDelete {
			return nil, fmt.Errorf("column physical asset delete row[%d] is not marked deleted", rowIdx)
		}
		if err := skipColumnPhysicalRowValues(&cur, version, cfg); err != nil {
			return nil, fmt.Errorf("row[%d]: %w", rowIdx, err)
		}
	}
	if cur.err != nil {
		return nil, cur.err
	}
	if cur.pos != len(raw) {
		return nil, errors.New("trailing bytes in column physical asset")
	}
	return offsets, nil
}

func skipColumnPhysicalRowValues(cur *manifestCursor, version uint16, cfg *ColumnStoreConfig) error {
	for colIdx, col := range cfg.Columns {
		typeBytes := cur.stringBytes()
		if cur.err != nil {
			return cur.err
		}
		if !columnPhysicalBytesEqualString(typeBytes, string(col.ValueType)) {
			return fmt.Errorf("column[%d] type=%q want %q", colIdx, string(typeBytes), col.ValueType)
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
			if !null {
				return fmt.Errorf("column[%d] absent value is not null", colIdx)
			}
			if !col.Nullable {
				return fmt.Errorf("column[%d] is absent but column is not nullable", colIdx)
			}
			continue
		}
		if null {
			if !col.Nullable {
				return fmt.Errorf("column[%d] is null but column is not nullable", colIdx)
			}
			continue
		}
		if err := skipColumnPhysicalValue(cur, col); err != nil {
			return fmt.Errorf("column[%d]: %w", colIdx, err)
		}
	}
	return nil
}

func skipColumnPhysicalValue(cur *manifestCursor, col ColumnStoreColumn) error {
	switch col.ValueType {
	case ColumnStoreValueBool:
		_ = cur.bool()
	case ColumnStoreValueInt64:
		_ = cur.u64()
	case ColumnStoreValueFloat32:
		_ = cur.u32()
	case ColumnStoreValueDouble:
		_ = cur.u64()
	case ColumnStoreValueString:
		_ = cur.stringBytes()
	case ColumnStoreValueFloat32Vector:
		n := cur.skipUint32Slice()
		if cur.err != nil {
			return cur.err
		}
		if n != uint64(col.VectorDims) {
			return fmt.Errorf("float32_vector length=%d want vector_dims=%d", n, col.VectorDims)
		}
	case ColumnStoreValueAdjacencyList:
		_ = cur.skipUint32Slice()
	default:
		return fmt.Errorf("unsupported column physical value type %q", col.ValueType)
	}
	return cur.err
}

func readColumnPhysicalRowValuesIntoScratch(cur *manifestCursor, version uint16, cfg *ColumnStoreConfig, projection columnPhysicalScanProjection, scratch *columnPhysicalRowReaderScratch) error {
	rowValues := scratch.Values
	for colIdx, col := range cfg.Columns {
		typeBytes := cur.stringBytes()
		if cur.err != nil {
			return cur.err
		}
		if !columnPhysicalBytesEqualString(typeBytes, string(col.ValueType)) {
			return fmt.Errorf("column[%d] type=%q want %q", colIdx, string(typeBytes), col.ValueType)
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
		outputIdx := projection.outputByColumn[colIdx]
		selected := outputIdx >= 0
		if selected {
			rowValues[outputIdx] = columnDeclaredValue{
				Type:    col.ValueType,
				Present: present,
				Null:    null,
			}
		}
		if !present {
			if !null {
				return fmt.Errorf("column[%d] absent value is not null", colIdx)
			}
			if !col.Nullable {
				return fmt.Errorf("column[%d] is absent but column is not nullable", colIdx)
			}
			continue
		}
		if null {
			if !col.Nullable {
				return fmt.Errorf("column[%d] is null but column is not nullable", colIdx)
			}
			continue
		}
		if !selected {
			if err := skipColumnPhysicalValue(cur, col); err != nil {
				return fmt.Errorf("column[%d]: %w", colIdx, err)
			}
			continue
		}
		if err := readSelectedColumnPhysicalValueIntoScratch(cur, col, &rowValues[outputIdx], scratch); err != nil {
			return fmt.Errorf("column[%d]: %w", colIdx, err)
		}
	}
	return nil
}

func readSelectedColumnPhysicalValueIntoScratch(cur *manifestCursor, col ColumnStoreColumn, value *columnDeclaredValue, scratch *columnPhysicalRowReaderScratch) error {
	switch col.ValueType {
	case ColumnStoreValueBool:
		value.Bool = cur.bool()
	case ColumnStoreValueInt64:
		value.Int64 = int64(cur.u64())
	case ColumnStoreValueFloat32:
		value.Float32 = math.Float32frombits(cur.u32())
	case ColumnStoreValueDouble:
		value.Double = math.Float64frombits(cur.u64())
	case ColumnStoreValueString:
		value.StringBytes = cur.stringBytes()
	case ColumnStoreValueFloat32Vector:
		start := len(scratch.Float32Values)
		var err error
		scratch.Float32Values, err = cur.appendFloat32SliceWithExpectedLength(scratch.Float32Values, col.VectorDims)
		if err != nil {
			return err
		}
		value.Float32Vector = scratch.Float32Values[start:]
	case ColumnStoreValueAdjacencyList:
		start := len(scratch.Uint32Values)
		var err error
		scratch.Uint32Values, err = cur.appendUint32Slice(scratch.Uint32Values)
		if err != nil {
			return err
		}
		value.AdjacencyList = scratch.Uint32Values[start:]
	default:
		return fmt.Errorf("unsupported column physical value type %q", col.ValueType)
	}
	return cur.err
}

func (c *manifestCursor) appendFloat32SliceWithExpectedLength(dst []float32, expected int) ([]float32, error) {
	n := c.u64()
	if c.err != nil {
		return dst, c.err
	}
	if expected < 0 || n != uint64(expected) {
		c.err = fmt.Errorf("collections: float32_vector length=%d want vector_dims=%d", n, expected)
		return dst, c.err
	}
	byteLen, ok := c.fixedWidthSliceByteLen(n, 4, "float32_vector")
	if !ok {
		return dst, c.err
	}
	base := len(dst)
	need := base + int(n)
	if cap(dst) < need {
		next := make([]float32, need)
		copy(next, dst)
		dst = next
	} else {
		dst = dst[:need]
	}
	// Keep cursor state out of the per-element loop; vector search decodes
	// these fixed-width slices for every candidate row fetch.
	pos := c.pos
	end := pos + int(byteLen)
	raw := c.raw
	if pos < end {
		_ = raw[end-1] // BCE: prove the full [pos, end) range before the loop.
	}
	for i := base; i < need; i++ {
		dst[i] = math.Float32frombits(uint32(raw[pos])<<24 | uint32(raw[pos+1])<<16 | uint32(raw[pos+2])<<8 | uint32(raw[pos+3]))
		pos += 4
	}
	c.pos = end
	return dst, nil
}

func (c *manifestCursor) appendUint32Slice(dst []uint32) ([]uint32, error) {
	n := c.u64()
	if c.err != nil {
		return dst, c.err
	}
	byteLen, ok := c.fixedWidthSliceByteLen(n, 4, "uint32 slice")
	if !ok {
		return dst, c.err
	}
	base := len(dst)
	need := base + int(n)
	if cap(dst) < need {
		next := make([]uint32, need)
		copy(next, dst)
		dst = next
	} else {
		dst = dst[:need]
	}
	// Keep cursor state out of the per-element loop; vector search decodes
	// these fixed-width slices for every candidate row fetch.
	pos := c.pos
	end := pos + int(byteLen)
	raw := c.raw
	if pos < end {
		_ = raw[end-1] // BCE: prove the full [pos, end) range before the loop.
	}
	for i := base; i < need; i++ {
		dst[i] = uint32(raw[pos])<<24 | uint32(raw[pos+1])<<16 | uint32(raw[pos+2])<<8 | uint32(raw[pos+3])
		pos += 4
	}
	c.pos = end
	return dst, nil
}
