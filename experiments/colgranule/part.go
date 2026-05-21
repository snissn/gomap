package colgranule

import (
	"errors"
	"fmt"
	"math"
	"sort"
)

const columnPartDescriptorVersion = 2

type ColumnSchemaMode string

const (
	ColumnSchemaFixed ColumnSchemaMode = "fixed"
)

type ColumnType string

const (
	ColumnTypeInt64              ColumnType = "int64"
	ColumnTypeLowCardinalityCode ColumnType = "low_cardinality_code"
	ColumnTypeBool               ColumnType = "bool"
	ColumnTypeFloat32Vector      ColumnType = "float32_vector"
	ColumnTypeAdjacencyList      ColumnType = "adjacency_list"
)

type SortKeyDirection string

const (
	SortKeyAsc  SortKeyDirection = "asc"
	SortKeyDesc SortKeyDirection = "desc"
)

type SortKeyNullOrder string

const (
	SortKeyNullsDefault SortKeyNullOrder = ""
	SortKeyNullsFirst   SortKeyNullOrder = "first"
	SortKeyNullsLast    SortKeyNullOrder = "last"
)

type LogicalPrimaryKey struct {
	Columns []string
}

type SortKey struct {
	Columns []SortKeyColumn
}

type SortKeyColumn struct {
	Column    string
	Direction SortKeyDirection
	Nulls     SortKeyNullOrder
}

type ColumnStoreOptions struct {
	SchemaVersion     uint32
	SchemaMode        ColumnSchemaMode
	Columns           []ColumnDefinition
	LogicalPrimaryKey LogicalPrimaryKey
	SortKey           SortKey
	PartPolicy        ColumnPartPolicy
	Compression       ColumnCompressionPolicy
	AggregateMetadata []AggregateMetadataDefinition
}

type ColumnPartPolicy struct {
	RowsPerGranule        int
	DefaultCodecBlockRows int
	AdaptiveMarkSizing    ColumnAdaptiveMarkSizing
}

type ColumnCompressionPolicy struct {
	Default Compression
}

type ColumnDefinition struct {
	Name           string
	Type           ColumnType
	Encoding       Encoding
	Compression    Compression
	CompressionSet bool
	Cardinality    uint32
	VectorDims     int
	CodecBlockRows int
}

type ColumnBatch struct {
	Rows           int
	Columns        map[string][]int64
	Float32Vectors map[string]Float32VectorColumn
	AdjacencyLists map[string]AdjacencyListColumn
}

type ColumnPart struct {
	Options           ColumnStoreOptions
	Descriptor        ColumnPartDescriptor
	Columns           map[string]ColumnPartColumn
	Marks             []SortKeyMark
	Locators          map[int64]RowLocator
	AggregateMetadata map[string]AggregateMetadata
}

type ColumnPartDescriptor struct {
	Version           uint8
	PartID            uint64
	SchemaVersion     uint32
	RowCount          int
	VisibleRowCount   int
	LogicalPrimaryKey []string
	SortKey           []SortKeyColumn
	Granules          []GranuleDescriptor
	Columns           []ColumnPartColumnDescriptor
}

type GranuleDescriptor struct {
	Ordinal          int
	FirstRow         int
	RowCount         int
	VisibleRows      int
	DeletedRows      int
	IDLower          int64
	IDUpperExclusive int64
	MarkOrdinal      int
}

type ColumnPartColumnDescriptor struct {
	Name       string
	Type       ColumnType
	VectorDims int
	Blocks     []ColumnBlockDescriptor
}

type ColumnBlockDescriptor struct {
	FirstRow          int
	RowCount          int
	FirstGranule      int
	LastGranule       int
	Encoding          Encoding
	Compression       Compression
	RawBytes          int
	StoredBytes       int
	CodecBlockOrdinal int
}

type ColumnPartColumn struct {
	Definition ColumnDefinition
	Blocks     []ColumnBlock
}

type ColumnBlock struct {
	Descriptor ColumnBlockDescriptor
	Granule    EncodedGranule
}

type RowLocator struct {
	PrimaryID      int64
	PartID         uint64
	PartRow        int
	GranuleOrdinal int
	RowInGranule   int
}

type ColumnPartBuilder struct {
	opts        ColumnStoreOptions
	order       []int
	values64    []int64
	codes32     []uint32
	bools       []bool
	vectors32   []float32
	listOffsets []uint32
	listValues  []int64
	builder     *GranuleBuilder
}

func NewColumnPartBuilder(opts ColumnStoreOptions) (*ColumnPartBuilder, error) {
	normalized, err := normalizeColumnStoreOptions(opts)
	if err != nil {
		return nil, err
	}
	return &ColumnPartBuilder{opts: normalized, builder: NewGranuleBuilder(Config{})}, nil
}

func BuildColumnPart(partID uint64, opts ColumnStoreOptions, batch ColumnBatch) (*ColumnPart, error) {
	builder, err := NewColumnPartBuilder(opts)
	if err != nil {
		return nil, err
	}
	return builder.Build(partID, batch)
}

func (b *ColumnPartBuilder) Build(partID uint64, batch ColumnBatch) (*ColumnPart, error) {
	rows, err := validateColumnBatch(batch, b.opts.Columns)
	if err != nil {
		return nil, err
	}
	if b.opts.PartPolicy.AdaptiveMarkSizing.Enabled {
		rawBytes, err := EstimateColumnBatchUncompressedBytes(batch, b.opts.Columns)
		if err != nil {
			return nil, err
		}
		estimate, err := EstimateAdaptiveRowsPerMark(rows, rawBytes, b.opts.PartPolicy.AdaptiveMarkSizing)
		if err != nil {
			return nil, err
		}
		b.opts.PartPolicy.RowsPerGranule = estimate.RowsPerMark
	}
	pkColumn := b.opts.LogicalPrimaryKey.Columns[0]
	order, err := b.sortedOrder(batch, rows, pkColumn)
	if err != nil {
		return nil, err
	}
	b.order = order

	part := &ColumnPart{
		Options: b.opts,
		Descriptor: ColumnPartDescriptor{
			Version:           columnPartDescriptorVersion,
			PartID:            partID,
			SchemaVersion:     b.opts.SchemaVersion,
			RowCount:          rows,
			VisibleRowCount:   rows,
			LogicalPrimaryKey: append([]string(nil), b.opts.LogicalPrimaryKey.Columns...),
			SortKey:           append([]SortKeyColumn(nil), b.opts.SortKey.Columns...),
		},
		Columns:  make(map[string]ColumnPartColumn, len(b.opts.Columns)),
		Locators: make(map[int64]RowLocator, rows),
	}

	if err := b.buildGranulesAndLocators(part, batch, pkColumn); err != nil {
		return nil, err
	}
	for _, def := range b.opts.Columns {
		column, descriptor, err := b.buildColumn(batch, def)
		if err != nil {
			return nil, err
		}
		part.Columns[def.Name] = column
		part.Descriptor.Columns = append(part.Descriptor.Columns, descriptor)
	}
	if err := b.buildAggregateMetadata(part, batch); err != nil {
		return nil, err
	}
	return part, nil
}

func (p *ColumnPart) LocatePrimaryID(primaryID int64) (RowLocator, bool) {
	locator, ok := p.Locators[primaryID]
	return locator, ok
}

func (p *ColumnPart) EncodedColumnBlocks(name string) ([]EncodedGranule, error) {
	column, ok := p.Columns[name]
	if !ok {
		return nil, fmt.Errorf("colgranule: missing column %s", name)
	}
	out := make([]EncodedGranule, len(column.Blocks))
	for i, block := range column.Blocks {
		out[i] = block.Granule
	}
	return out, nil
}

func (p *ColumnPart) NewScanner() *ColumnPartScanner {
	return &ColumnPartScanner{part: p}
}

type ColumnPartScanner struct {
	part        *ColumnPart
	reader      GranuleReader
	values      []int64
	codes       []uint32
	bools       []bool
	scratch     []int64
	vectors32   []float32
	listOffsets []uint32
	listValues  []int64
	listScratch []int64
}

type ProjectedScanResult struct {
	Rows        int
	Columns     map[string][]int64
	Diagnostics PartScanDiagnostics
}

type Float32VectorScanResult struct {
	Rows        int
	Dims        int
	Values      []float32
	Diagnostics PartScanDiagnostics
}

type AdjacencyListScanResult struct {
	Rows        int
	Offsets     []uint32
	Values      []int64
	Diagnostics PartScanDiagnostics
}

type PartScanDiagnostics struct {
	RowsScanned        int
	ColumnsProjected   int
	GranulesConsidered int
	GranulesDecoded    int
	BlocksDecoded      int
	BytesDecoded       int
}

func (s *ColumnPartScanner) ScanProjected(columns []string) (ProjectedScanResult, error) {
	return s.ScanProjectedInto(make(map[string][]int64, len(columns)), columns)
}

func (s *ColumnPartScanner) ScanProjectedInto(dst map[string][]int64, columns []string) (ProjectedScanResult, error) {
	if s.part == nil {
		return ProjectedScanResult{}, errors.New("colgranule: nil part scanner")
	}
	projection, err := s.validateProjection(columns)
	if err != nil {
		return ProjectedScanResult{}, err
	}
	if dst == nil {
		dst = make(map[string][]int64, len(projection))
	}
	out := ProjectedScanResult{
		Rows:    s.part.Descriptor.RowCount,
		Columns: dst,
		Diagnostics: PartScanDiagnostics{
			RowsScanned:        s.part.Descriptor.RowCount,
			ColumnsProjected:   len(projection),
			GranulesConsidered: len(s.part.Descriptor.Granules),
		},
	}
	next := make(map[string][]int64, len(projection))
	for _, name := range projection {
		values, diagnostics, err := s.scanColumnInto(name, dst[name])
		if err != nil {
			return ProjectedScanResult{}, err
		}
		next[name] = values
		if diagnostics.GranulesDecoded > out.Diagnostics.GranulesDecoded {
			out.Diagnostics.GranulesDecoded = diagnostics.GranulesDecoded
		}
		out.Diagnostics.BlocksDecoded += diagnostics.BlocksDecoded
		out.Diagnostics.BytesDecoded += diagnostics.BytesDecoded
	}
	for name := range dst {
		if _, ok := next[name]; !ok {
			delete(dst, name)
		}
	}
	for name, values := range next {
		dst[name] = values
	}
	return out, nil
}

func (s *ColumnPartScanner) validateProjection(columns []string) ([]string, error) {
	if len(columns) == 0 {
		return nil, errors.New("colgranule: empty projection")
	}
	seen := make(map[string]struct{}, len(columns))
	for _, name := range columns {
		if name == "" {
			return nil, errors.New("colgranule: empty projection column")
		}
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("colgranule: duplicate projection column %s", name)
		}
		if _, ok := s.part.Columns[name]; !ok {
			return nil, fmt.Errorf("colgranule: missing column %s", name)
		}
		seen[name] = struct{}{}
	}
	return columns, nil
}

func (s *ColumnPartScanner) ValueAt(locator RowLocator, columnName string) (int64, error) {
	if s.part == nil {
		return 0, errors.New("colgranule: nil part scanner")
	}
	if locator.PartID != s.part.Descriptor.PartID {
		return 0, fmt.Errorf("colgranule: locator part=%d want %d", locator.PartID, s.part.Descriptor.PartID)
	}
	column, ok := s.part.Columns[columnName]
	if !ok {
		return 0, fmt.Errorf("colgranule: missing column %s", columnName)
	}
	for _, block := range column.Blocks {
		if locator.PartRow < block.Descriptor.FirstRow || locator.PartRow >= block.Descriptor.FirstRow+block.Descriptor.RowCount {
			continue
		}
		values, err := s.decodeBlock(column.Definition.Type, block.Granule)
		if err != nil {
			return 0, err
		}
		return values[locator.PartRow-block.Descriptor.FirstRow], nil
	}
	return 0, fmt.Errorf("colgranule: locator row %d outside column %s", locator.PartRow, columnName)
}

func (s *ColumnPartScanner) Float32VectorAt(locator RowLocator, columnName string, dst []float32) ([]float32, error) {
	if s.part == nil {
		return nil, errors.New("colgranule: nil part scanner")
	}
	if locator.PartID != s.part.Descriptor.PartID {
		return nil, fmt.Errorf("colgranule: locator part=%d want %d", locator.PartID, s.part.Descriptor.PartID)
	}
	column, ok := s.part.Columns[columnName]
	if !ok {
		return nil, fmt.Errorf("colgranule: missing column %s", columnName)
	}
	if column.Definition.Type != ColumnTypeFloat32Vector {
		return nil, fmt.Errorf("colgranule: column %s type=%s is not %s", columnName, column.Definition.Type, ColumnTypeFloat32Vector)
	}
	dims := column.Definition.VectorDims
	for _, block := range column.Blocks {
		if locator.PartRow < block.Descriptor.FirstRow || locator.PartRow >= block.Descriptor.FirstRow+block.Descriptor.RowCount {
			continue
		}
		row := locator.PartRow - block.Descriptor.FirstRow
		if block.Granule.Compression == CompressionNone {
			raw, err := s.reader.decompressPayload(block.Granule)
			if err != nil {
				return nil, err
			}
			return decodeFloat32VectorRowInto(dst[:0], raw, block.Descriptor.RowCount, dims, row)
		}
		values, err := s.reader.DecodeFloat32VectorsInto(s.vectors32[:0], block.Granule, dims)
		if err != nil {
			return nil, err
		}
		s.vectors32 = values
		start := row * dims
		out := ensureFloat32Len(dst[:0], dims)
		copy(out, values[start:start+dims])
		return out, nil
	}
	return nil, fmt.Errorf("colgranule: locator row %d outside column %s", locator.PartRow, columnName)
}

func (s *ColumnPartScanner) AdjacencyListAt(locator RowLocator, columnName string, dst []int64) ([]int64, error) {
	if s.part == nil {
		return nil, errors.New("colgranule: nil part scanner")
	}
	if locator.PartID != s.part.Descriptor.PartID {
		return nil, fmt.Errorf("colgranule: locator part=%d want %d", locator.PartID, s.part.Descriptor.PartID)
	}
	column, ok := s.part.Columns[columnName]
	if !ok {
		return nil, fmt.Errorf("colgranule: missing column %s", columnName)
	}
	if column.Definition.Type != ColumnTypeAdjacencyList {
		return nil, fmt.Errorf("colgranule: column %s type=%s is not %s", columnName, column.Definition.Type, ColumnTypeAdjacencyList)
	}
	for _, block := range column.Blocks {
		if locator.PartRow < block.Descriptor.FirstRow || locator.PartRow >= block.Descriptor.FirstRow+block.Descriptor.RowCount {
			continue
		}
		row := locator.PartRow - block.Descriptor.FirstRow
		if block.Granule.Compression == CompressionNone {
			raw, err := s.reader.decompressPayload(block.Granule)
			if err != nil {
				return nil, err
			}
			return decodeInt64AdjacencyListRowInto(dst[:0], raw, block.Descriptor.RowCount, row)
		}
		offsets, values, err := s.reader.DecodeInt64AdjacencyListsInto(s.listOffsets[:0], s.listValues[:0], block.Granule)
		if err != nil {
			return nil, err
		}
		s.listOffsets = offsets
		s.listValues = values
		start := int(offsets[row])
		end := int(offsets[row+1])
		out := ensureInt64Len(dst[:0], end-start)
		copy(out, values[start:end])
		return out, nil
	}
	return nil, fmt.Errorf("colgranule: locator row %d outside column %s", locator.PartRow, columnName)
}

func (s *ColumnPartScanner) ScanFloat32VectorsInto(columnName string, dst []float32) (Float32VectorScanResult, error) {
	if s.part == nil {
		return Float32VectorScanResult{}, errors.New("colgranule: nil part scanner")
	}
	values, dims, diagnostics, err := s.scanFloat32VectorColumnInto(columnName, dst)
	if err != nil {
		return Float32VectorScanResult{}, err
	}
	return Float32VectorScanResult{
		Rows:        s.part.Descriptor.RowCount,
		Dims:        dims,
		Values:      values,
		Diagnostics: diagnostics,
	}, nil
}

func (s *ColumnPartScanner) ScanAdjacencyListsInto(columnName string, offsets []uint32, values []int64) (AdjacencyListScanResult, error) {
	if s.part == nil {
		return AdjacencyListScanResult{}, errors.New("colgranule: nil part scanner")
	}
	outOffsets, outValues, diagnostics, err := s.scanAdjacencyListColumnInto(columnName, offsets, values)
	if err != nil {
		return AdjacencyListScanResult{}, err
	}
	return AdjacencyListScanResult{
		Rows:        s.part.Descriptor.RowCount,
		Offsets:     outOffsets,
		Values:      outValues,
		Diagnostics: diagnostics,
	}, nil
}

func (s *ColumnPartScanner) scanColumn(name string) ([]int64, PartScanDiagnostics, error) {
	return s.scanColumnInto(name, nil)
}

func (s *ColumnPartScanner) scanColumnInto(name string, dst []int64) ([]int64, PartScanDiagnostics, error) {
	column, ok := s.part.Columns[name]
	if !ok {
		return nil, PartScanDiagnostics{}, fmt.Errorf("colgranule: missing column %s", name)
	}
	out := ensureInt64Len(dst[:0], s.part.Descriptor.RowCount)
	var diagnostics PartScanDiagnostics
	granulesDecoded, err := countGranulesCoveredByBlocks(column.Blocks)
	if err != nil {
		return nil, diagnostics, fmt.Errorf("colgranule: column %s: %w", name, err)
	}
	diagnostics.GranulesDecoded = granulesDecoded
	for _, block := range column.Blocks {
		values, err := s.decodeBlock(column.Definition.Type, block.Granule)
		if err != nil {
			return nil, diagnostics, err
		}
		if len(values) != block.Descriptor.RowCount {
			return nil, diagnostics, fmt.Errorf("colgranule: block rows=%d decoded=%d", block.Descriptor.RowCount, len(values))
		}
		copy(out[block.Descriptor.FirstRow:block.Descriptor.FirstRow+block.Descriptor.RowCount], values)
		diagnostics.BlocksDecoded++
		diagnostics.BytesDecoded += block.Granule.RawBytes
	}
	return out, diagnostics, nil
}

func (s *ColumnPartScanner) scanFloat32VectorColumnInto(name string, dst []float32) ([]float32, int, PartScanDiagnostics, error) {
	column, ok := s.part.Columns[name]
	if !ok {
		return nil, 0, PartScanDiagnostics{}, fmt.Errorf("colgranule: missing column %s", name)
	}
	if column.Definition.Type != ColumnTypeFloat32Vector {
		return nil, 0, PartScanDiagnostics{}, fmt.Errorf("colgranule: column %s type=%s is not %s", name, column.Definition.Type, ColumnTypeFloat32Vector)
	}
	dims := column.Definition.VectorDims
	valueCount, err := checkedMulInt(s.part.Descriptor.RowCount, dims, "float32 vector scan values")
	if err != nil {
		return nil, 0, PartScanDiagnostics{}, err
	}
	out := ensureFloat32Len(dst[:0], valueCount)
	var diagnostics PartScanDiagnostics
	for _, block := range column.Blocks {
		values, err := s.reader.DecodeFloat32VectorsInto(s.vectors32[:0], block.Granule, dims)
		if err != nil {
			return nil, 0, diagnostics, err
		}
		s.vectors32 = values
		blockValueCount, err := checkedMulInt(block.Descriptor.RowCount, dims, "float32 vector block values")
		if err != nil {
			return nil, 0, diagnostics, err
		}
		if len(values) != blockValueCount {
			return nil, 0, diagnostics, fmt.Errorf("colgranule: block vector values=%d want=%d", len(values), blockValueCount)
		}
		start := block.Descriptor.FirstRow * dims
		copy(out[start:start+blockValueCount], values)
		diagnostics.BlocksDecoded++
		diagnostics.BytesDecoded += block.Granule.RawBytes
	}
	diagnostics.RowsScanned = s.part.Descriptor.RowCount
	diagnostics.ColumnsProjected = 1
	diagnostics.GranulesConsidered = len(s.part.Descriptor.Granules)
	return out, dims, diagnostics, nil
}

func (s *ColumnPartScanner) scanAdjacencyListColumnInto(name string, offsetDst []uint32, valueDst []int64) ([]uint32, []int64, PartScanDiagnostics, error) {
	column, ok := s.part.Columns[name]
	if !ok {
		return nil, nil, PartScanDiagnostics{}, fmt.Errorf("colgranule: missing column %s", name)
	}
	if column.Definition.Type != ColumnTypeAdjacencyList {
		return nil, nil, PartScanDiagnostics{}, fmt.Errorf("colgranule: column %s type=%s is not %s", name, column.Definition.Type, ColumnTypeAdjacencyList)
	}
	outOffsets := ensureUint32Len(offsetDst[:0], s.part.Descriptor.RowCount+1)
	clear(outOffsets)
	outValues := valueDst[:0]
	var diagnostics PartScanDiagnostics
	for _, block := range column.Blocks {
		offsets, values, err := s.reader.DecodeInt64AdjacencyListsInto(s.listOffsets[:0], s.listValues[:0], block.Granule)
		if err != nil {
			return nil, nil, diagnostics, err
		}
		s.listOffsets = offsets
		s.listValues = values
		if len(offsets) != block.Descriptor.RowCount+1 {
			return nil, nil, diagnostics, fmt.Errorf("colgranule: block adjacency offsets=%d want=%d", len(offsets), block.Descriptor.RowCount+1)
		}
		for row := 0; row < block.Descriptor.RowCount; row++ {
			start := int(offsets[row])
			end := int(offsets[row+1])
			outValues = append(outValues, values[start:end]...)
			if len(outValues) > math.MaxUint32 {
				return nil, nil, diagnostics, fmt.Errorf("colgranule: adjacency scan values=%d exceed uint32 offsets", len(outValues))
			}
			outOffsets[block.Descriptor.FirstRow+row+1] = uint32(len(outValues))
		}
		diagnostics.BlocksDecoded++
		diagnostics.BytesDecoded += block.Granule.RawBytes
	}
	diagnostics.RowsScanned = s.part.Descriptor.RowCount
	diagnostics.ColumnsProjected = 1
	diagnostics.GranulesConsidered = len(s.part.Descriptor.Granules)
	return outOffsets, outValues, diagnostics, nil
}

func (s *ColumnPartScanner) scanColumnRowsInto(name string, dst []int64, rows []int) ([]int64, PartScanDiagnostics, error) {
	column, ok := s.part.Columns[name]
	if !ok {
		return nil, PartScanDiagnostics{}, fmt.Errorf("colgranule: missing column %s", name)
	}
	if len(rows) == 0 {
		return dst[:0], PartScanDiagnostics{}, nil
	}
	if rows[0] < 0 || rows[len(rows)-1] >= s.part.Descriptor.RowCount {
		return nil, PartScanDiagnostics{}, fmt.Errorf("colgranule: visible row range [%d,%d] outside part rows=%d", rows[0], rows[len(rows)-1], s.part.Descriptor.RowCount)
	}
	out := dst
	var diagnostics PartScanDiagnostics
	rowIndex := 0
	for _, block := range column.Blocks {
		first := block.Descriptor.FirstRow
		limit := first + block.Descriptor.RowCount
		for rowIndex < len(rows) && rows[rowIndex] < first {
			return nil, diagnostics, fmt.Errorf("colgranule: visible row %d before block %d first row %d", rows[rowIndex], block.Descriptor.CodecBlockOrdinal, first)
		}
		start := rowIndex
		for rowIndex < len(rows) && rows[rowIndex] < limit {
			rowIndex++
		}
		if start == rowIndex {
			continue
		}
		values, err := s.decodeBlock(column.Definition.Type, block.Granule)
		if err != nil {
			return nil, diagnostics, err
		}
		if len(values) != block.Descriptor.RowCount {
			return nil, diagnostics, fmt.Errorf("colgranule: block rows=%d decoded=%d", block.Descriptor.RowCount, len(values))
		}
		for _, row := range rows[start:rowIndex] {
			out = append(out, values[row-first])
		}
		diagnostics.BlocksDecoded++
		diagnostics.BytesDecoded += block.Granule.RawBytes
	}
	if rowIndex != len(rows) {
		return nil, diagnostics, fmt.Errorf("colgranule: %d visible rows outside column %s blocks", len(rows)-rowIndex, name)
	}
	diagnostics.RowsScanned = len(rows)
	return out, diagnostics, nil
}

func countGranulesCoveredByBlocks(blocks []ColumnBlock) (int, error) {
	total := 0
	coveredStart := -1
	coveredEnd := -1
	prevFirst := -1
	for _, block := range blocks {
		first := block.Descriptor.FirstGranule
		last := block.Descriptor.LastGranule
		if first < 0 || last < first {
			return 0, fmt.Errorf("invalid granule range %d..%d", first, last)
		}
		if prevFirst >= 0 && first < prevFirst {
			return 0, fmt.Errorf("granule ranges out of order: %d after %d", first, prevFirst)
		}
		prevFirst = first
		if coveredStart < 0 {
			coveredStart = first
			coveredEnd = last
			continue
		}
		if first <= coveredEnd+1 {
			if last > coveredEnd {
				coveredEnd = last
			}
			continue
		}
		total += coveredEnd - coveredStart + 1
		coveredStart = first
		coveredEnd = last
	}
	if coveredStart >= 0 {
		total += coveredEnd - coveredStart + 1
	}
	return total, nil
}

func (s *ColumnPartScanner) decodeBlock(columnType ColumnType, g EncodedGranule) ([]int64, error) {
	switch columnType {
	case ColumnTypeInt64:
		values, err := s.reader.DecodeInt64Into(s.values[:0], g)
		if err != nil {
			return nil, err
		}
		s.values = values
		return values, nil
	case ColumnTypeLowCardinalityCode:
		codes, err := s.reader.DecodeUint32CodesInto(s.codes[:0], g)
		if err != nil {
			return nil, err
		}
		s.codes = codes
		s.scratch = ensureInt64Len(s.scratch[:0], len(codes))
		for i, code := range codes {
			s.scratch[i] = int64(code)
		}
		return s.scratch, nil
	case ColumnTypeBool:
		values, err := s.reader.DecodeBoolInto(s.bools[:0], g)
		if err != nil {
			return nil, err
		}
		s.bools = values
		s.scratch = ensureInt64Len(s.scratch[:0], len(values))
		for i, v := range values {
			if v {
				s.scratch[i] = 1
			} else {
				s.scratch[i] = 0
			}
		}
		return s.scratch, nil
	default:
		return nil, fmt.Errorf("colgranule: unsupported column type %s", columnType)
	}
}

func normalizeColumnStoreOptions(opts ColumnStoreOptions) (ColumnStoreOptions, error) {
	opts.Columns = append([]ColumnDefinition(nil), opts.Columns...)
	opts.LogicalPrimaryKey.Columns = append([]string(nil), opts.LogicalPrimaryKey.Columns...)
	opts.SortKey.Columns = append([]SortKeyColumn(nil), opts.SortKey.Columns...)
	if opts.SchemaVersion == 0 {
		opts.SchemaVersion = 1
	}
	if opts.SchemaMode == "" {
		opts.SchemaMode = ColumnSchemaFixed
	}
	if opts.SchemaMode != ColumnSchemaFixed {
		return ColumnStoreOptions{}, fmt.Errorf("colgranule: unsupported schema mode %s", opts.SchemaMode)
	}
	var err error
	if opts.PartPolicy.AdaptiveMarkSizing.Enabled {
		opts.PartPolicy.AdaptiveMarkSizing, err = NormalizeColumnAdaptiveMarkSizing(opts.PartPolicy.AdaptiveMarkSizing, opts.PartPolicy.RowsPerGranule)
		if err != nil {
			return ColumnStoreOptions{}, err
		}
		opts.PartPolicy.RowsPerGranule = opts.PartPolicy.AdaptiveMarkSizing.MaxRows
	} else {
		if opts.PartPolicy.RowsPerGranule == 0 {
			opts.PartPolicy.RowsPerGranule = DefaultRowsPerGranule
		}
		if opts.PartPolicy.RowsPerGranule <= 0 {
			return ColumnStoreOptions{}, fmt.Errorf("colgranule: invalid rows per granule %d", opts.PartPolicy.RowsPerGranule)
		}
	}
	if opts.PartPolicy.DefaultCodecBlockRows < 0 {
		return ColumnStoreOptions{}, fmt.Errorf("colgranule: invalid default codec block rows %d", opts.PartPolicy.DefaultCodecBlockRows)
	}
	if err := validateCompression(opts.Compression.Default); err != nil {
		return ColumnStoreOptions{}, fmt.Errorf("colgranule: unsupported default compression %s", opts.Compression.Default)
	}
	if len(opts.LogicalPrimaryKey.Columns) != 1 {
		return ColumnStoreOptions{}, fmt.Errorf("colgranule: experiment requires exactly one logical primary key column, got %d", len(opts.LogicalPrimaryKey.Columns))
	}
	if len(opts.SortKey.Columns) == 0 {
		opts.SortKey.Columns = []SortKeyColumn{{Column: opts.LogicalPrimaryKey.Columns[0]}}
	}
	if len(opts.Columns) == 0 {
		return ColumnStoreOptions{}, errors.New("colgranule: no declared columns")
	}
	seen := make(map[string]struct{}, len(opts.Columns))
	columnsByName := make(map[string]ColumnDefinition, len(opts.Columns))
	for i := range opts.Columns {
		def, err := normalizeColumnDefinition(opts.Columns[i], opts.Compression.Default)
		if err != nil {
			return ColumnStoreOptions{}, err
		}
		if _, ok := seen[def.Name]; ok {
			return ColumnStoreOptions{}, fmt.Errorf("colgranule: duplicate column %s", def.Name)
		}
		seen[def.Name] = struct{}{}
		columnsByName[def.Name] = def
		opts.Columns[i] = def
	}
	if _, ok := seen[opts.LogicalPrimaryKey.Columns[0]]; !ok {
		return ColumnStoreOptions{}, fmt.Errorf("colgranule: logical primary key column %s is not declared", opts.LogicalPrimaryKey.Columns[0])
	}
	if columnsByName[opts.LogicalPrimaryKey.Columns[0]].Type != ColumnTypeInt64 {
		return ColumnStoreOptions{}, fmt.Errorf("colgranule: logical primary key column %s type=%s want %s", opts.LogicalPrimaryKey.Columns[0], columnsByName[opts.LogicalPrimaryKey.Columns[0]].Type, ColumnTypeInt64)
	}
	for i := range opts.SortKey.Columns {
		c := &opts.SortKey.Columns[i]
		if c.Column == "" {
			return ColumnStoreOptions{}, fmt.Errorf("colgranule: empty sort key column at %d", i)
		}
		if _, ok := seen[c.Column]; !ok {
			return ColumnStoreOptions{}, fmt.Errorf("colgranule: sort key column %s is not declared", c.Column)
		}
		if !columnTypeSupportsOrdering(columnsByName[c.Column].Type) {
			return ColumnStoreOptions{}, fmt.Errorf("colgranule: sort key column %s type=%s is not orderable", c.Column, columnsByName[c.Column].Type)
		}
		if c.Direction == "" {
			c.Direction = SortKeyAsc
		}
		if c.Direction != SortKeyAsc && c.Direction != SortKeyDesc {
			return ColumnStoreOptions{}, fmt.Errorf("colgranule: unsupported sort direction %s", c.Direction)
		}
		if c.Direction == SortKeyDesc {
			return ColumnStoreOptions{}, errors.New("colgranule: descending sort keys are not supported by experiment marks yet")
		}
		if c.Nulls != SortKeyNullsDefault && c.Nulls != SortKeyNullsFirst && c.Nulls != SortKeyNullsLast {
			return ColumnStoreOptions{}, fmt.Errorf("colgranule: unsupported null order %s", c.Nulls)
		}
	}
	seenMetadata := make(map[string]struct{}, len(opts.AggregateMetadata))
	for i := range opts.AggregateMetadata {
		def, err := normalizeAggregateMetadataDefinition(opts.AggregateMetadata[i], columnsByName)
		if err != nil {
			return ColumnStoreOptions{}, err
		}
		if _, ok := seenMetadata[def.Name]; ok {
			return ColumnStoreOptions{}, fmt.Errorf("colgranule: duplicate aggregate metadata %s", def.Name)
		}
		seenMetadata[def.Name] = struct{}{}
		opts.AggregateMetadata[i] = def
	}
	return opts, nil
}

func normalizeColumnDefinition(def ColumnDefinition, defaultCompression Compression) (ColumnDefinition, error) {
	if def.Name == "" {
		return ColumnDefinition{}, errors.New("colgranule: empty column name")
	}
	if def.Type == "" {
		def.Type = ColumnTypeInt64
	}
	if def.CodecBlockRows < 0 {
		return ColumnDefinition{}, fmt.Errorf("colgranule: invalid codec block rows %d for %s", def.CodecBlockRows, def.Name)
	}
	if !def.CompressionSet && def.Compression == CompressionNone && defaultCompression != CompressionNone {
		def.Compression = defaultCompression
	}
	if err := validateCompression(def.Compression); err != nil {
		return ColumnDefinition{}, fmt.Errorf("colgranule: unsupported compression %s for %s", def.Compression, def.Name)
	}
	switch def.Type {
	case ColumnTypeInt64:
		if def.Encoding == 0 {
			def.Encoding = EncodingDeltaVarint
		}
		if def.Encoding != EncodingRawInt64 && def.Encoding != EncodingDeltaVarint && def.Encoding != EncodingDoubleDeltaVarint {
			return ColumnDefinition{}, fmt.Errorf("colgranule: unsupported int64 encoding %s for %s", def.Encoding, def.Name)
		}
	case ColumnTypeLowCardinalityCode:
		def.Encoding = EncodingLowCardinalityUint32
	case ColumnTypeBool:
		def.Encoding = EncodingBoolBitpackRLE
	case ColumnTypeFloat32Vector:
		if def.VectorDims <= 0 {
			return ColumnDefinition{}, fmt.Errorf("colgranule: invalid vector dims %d for %s", def.VectorDims, def.Name)
		}
		def.Encoding = EncodingRawFloat32Vector
		def.Cardinality = 0
	case ColumnTypeAdjacencyList:
		if def.VectorDims != 0 {
			return ColumnDefinition{}, fmt.Errorf("colgranule: adjacency column %s has vector dims %d", def.Name, def.VectorDims)
		}
		def.Encoding = EncodingRawInt64AdjacencyList
		def.Cardinality = 0
	default:
		return ColumnDefinition{}, fmt.Errorf("colgranule: unsupported column type %s for %s", def.Type, def.Name)
	}
	return def, nil
}

func validateCompression(compression Compression) error {
	switch compression {
	case CompressionNone, CompressionSnappy, CompressionLZ4:
		return nil
	default:
		return fmt.Errorf("colgranule: unsupported compression %s", compression)
	}
}

func validateColumnBatch(batch ColumnBatch, defs []ColumnDefinition) (int, error) {
	if batch.Columns == nil && batch.Float32Vectors == nil && batch.AdjacencyLists == nil {
		return 0, errors.New("colgranule: nil column batch")
	}
	rows := batch.Rows
	for _, def := range defs {
		columnRows, err := columnBatchRows(batch, def)
		if err != nil {
			return 0, err
		}
		if rows == 0 {
			rows = columnRows
		}
		if columnRows != rows {
			return 0, fmt.Errorf("colgranule: column %s rows=%d want=%d", def.Name, columnRows, rows)
		}
	}
	if rows <= 0 {
		return 0, fmt.Errorf("colgranule: invalid part rows %d", rows)
	}
	return rows, nil
}

func columnBatchRows(batch ColumnBatch, def ColumnDefinition) (int, error) {
	switch def.Type {
	case ColumnTypeInt64, ColumnTypeLowCardinalityCode, ColumnTypeBool:
		values, ok := batch.Columns[def.Name]
		if !ok {
			return 0, fmt.Errorf("colgranule: missing column %s", def.Name)
		}
		return len(values), nil
	case ColumnTypeFloat32Vector:
		column, ok := batch.Float32Vectors[def.Name]
		if !ok {
			return 0, fmt.Errorf("colgranule: missing float32 vector column %s", def.Name)
		}
		if column.Dims != def.VectorDims {
			return 0, fmt.Errorf("colgranule: vector column %s dims=%d want=%d", def.Name, column.Dims, def.VectorDims)
		}
		rows, err := validateFloat32VectorValues(column.Values, column.Dims)
		if err != nil {
			return 0, fmt.Errorf("colgranule: vector column %s: %w", def.Name, err)
		}
		return rows, nil
	case ColumnTypeAdjacencyList:
		column, ok := batch.AdjacencyLists[def.Name]
		if !ok {
			return 0, fmt.Errorf("colgranule: missing adjacency-list column %s", def.Name)
		}
		rows, err := validateAdjacencyListValues(column.Offsets, column.Values)
		if err != nil {
			return 0, fmt.Errorf("colgranule: adjacency-list column %s: %w", def.Name, err)
		}
		return rows, nil
	default:
		return 0, fmt.Errorf("colgranule: unsupported column type %s for %s", def.Type, def.Name)
	}
}

func columnTypeSupportsOrdering(columnType ColumnType) bool {
	switch columnType {
	case ColumnTypeInt64, ColumnTypeLowCardinalityCode, ColumnTypeBool:
		return true
	default:
		return false
	}
}

func (b *ColumnPartBuilder) sortedOrder(batch ColumnBatch, rows int, pkColumn string) ([]int, error) {
	if cap(b.order) < rows {
		b.order = make([]int, rows)
	} else {
		b.order = b.order[:rows]
	}
	for i := range b.order {
		b.order[i] = i
	}
	sort.Slice(b.order, func(i, j int) bool {
		left := b.order[i]
		right := b.order[j]
		for _, c := range b.opts.SortKey.Columns {
			values := batch.Columns[c.Column]
			if values[left] == values[right] {
				continue
			}
			if c.Direction == SortKeyDesc {
				return values[left] > values[right]
			}
			return values[left] < values[right]
		}
		pk := batch.Columns[pkColumn]
		if pk[left] != pk[right] {
			return pk[left] < pk[right]
		}
		return left < right
	})
	return b.order, nil
}

func (b *ColumnPartBuilder) buildGranulesAndLocators(part *ColumnPart, batch ColumnBatch, pkColumn string) error {
	rowsPerGranule := b.opts.PartPolicy.RowsPerGranule
	pkValues := batch.Columns[pkColumn]
	for start := 0; start < len(b.order); start += rowsPerGranule {
		end := min(start+rowsPerGranule, len(b.order))
		ordinal := len(part.Descriptor.Granules)
		idLower, idUpper := int64(math.MaxInt64), int64(math.MinInt64)
		for partRow := start; partRow < end; partRow++ {
			primaryID := pkValues[b.order[partRow]]
			if primaryID == math.MaxInt64 {
				return fmt.Errorf("colgranule: primary id %d cannot form exclusive upper bound", primaryID)
			}
			if _, exists := part.Locators[primaryID]; exists {
				return fmt.Errorf("colgranule: duplicate primary id %d", primaryID)
			}
			if primaryID < idLower {
				idLower = primaryID
			}
			if primaryID > idUpper {
				idUpper = primaryID
			}
			part.Locators[primaryID] = RowLocator{
				PrimaryID:      primaryID,
				PartID:         part.Descriptor.PartID,
				PartRow:        partRow,
				GranuleOrdinal: ordinal,
				RowInGranule:   partRow - start,
			}
		}
		markColumns := make([]SortKeyColumnValues, len(b.opts.SortKey.Columns))
		for i, sortColumn := range b.opts.SortKey.Columns {
			values := make([]int64, end-start)
			sourceValues := batch.Columns[sortColumn.Column]
			for row := start; row < end; row++ {
				values[row-start] = sourceValues[b.order[row]]
			}
			markColumns[i] = SortKeyColumnValues{Name: sortColumn.Column, Values: values}
		}
		mark, err := buildOwnedSortKeyMark(markColumns)
		if err != nil {
			return err
		}
		part.Marks = append(part.Marks, mark)
		part.Descriptor.Granules = append(part.Descriptor.Granules, GranuleDescriptor{
			Ordinal:          ordinal,
			FirstRow:         start,
			RowCount:         end - start,
			VisibleRows:      end - start,
			IDLower:          idLower,
			IDUpperExclusive: exclusiveInt64Upper(idUpper),
			MarkOrdinal:      len(part.Marks) - 1,
		})
	}
	return nil
}

func (b *ColumnPartBuilder) buildColumn(batch ColumnBatch, def ColumnDefinition) (ColumnPartColumn, ColumnPartColumnDescriptor, error) {
	blockRows := def.CodecBlockRows
	if blockRows == 0 {
		blockRows = b.opts.PartPolicy.DefaultCodecBlockRows
	}
	if blockRows == 0 {
		blockRows = b.opts.PartPolicy.RowsPerGranule
	}
	column := ColumnPartColumn{Definition: def}
	descriptor := ColumnPartColumnDescriptor{Name: def.Name, Type: def.Type, VectorDims: def.VectorDims}
	for start := 0; start < len(b.order); start += blockRows {
		end := min(start+blockRows, len(b.order))
		g, err := b.buildColumnBlockGranule(batch, def, start, end)
		if err != nil {
			return ColumnPartColumn{}, ColumnPartColumnDescriptor{}, err
		}
		owned := g
		owned.Payload = append([]byte(nil), g.Payload...)
		desc := ColumnBlockDescriptor{
			FirstRow:          start,
			RowCount:          end - start,
			FirstGranule:      start / b.opts.PartPolicy.RowsPerGranule,
			LastGranule:       (end - 1) / b.opts.PartPolicy.RowsPerGranule,
			Encoding:          owned.Encoding,
			Compression:       owned.Compression,
			RawBytes:          owned.RawBytes,
			StoredBytes:       owned.StoredBytes,
			CodecBlockOrdinal: len(column.Blocks),
		}
		column.Blocks = append(column.Blocks, ColumnBlock{Descriptor: desc, Granule: owned})
		descriptor.Blocks = append(descriptor.Blocks, desc)
	}
	return column, descriptor, nil
}

func (b *ColumnPartBuilder) buildColumnBlockGranule(batch ColumnBatch, def ColumnDefinition, start int, end int) (EncodedGranule, error) {
	cfg := Config{Encoding: def.Encoding, Compression: def.Compression}
	b.builder.Reset(cfg)
	switch def.Type {
	case ColumnTypeInt64:
		sourceValues := batch.Columns[def.Name]
		b.values64 = ensureInt64Len(b.values64[:0], end-start)
		for row := start; row < end; row++ {
			b.values64[row-start] = sourceValues[b.order[row]]
		}
		return b.builder.BuildInt64(b.values64)
	case ColumnTypeLowCardinalityCode:
		sourceValues := batch.Columns[def.Name]
		b.values64 = ensureInt64Len(b.values64[:0], end-start)
		for row := start; row < end; row++ {
			b.values64[row-start] = sourceValues[b.order[row]]
		}
		b.codes32 = ensureUint32Len(b.codes32[:0], len(b.values64))
		for i, v := range b.values64 {
			if v < 0 || v > math.MaxUint32 {
				return EncodedGranule{}, fmt.Errorf("colgranule: code value %d outside uint32 for %s", v, def.Name)
			}
			b.codes32[i] = uint32(v)
		}
		return b.builder.BuildUint32Codes(b.codes32, def.Cardinality)
	case ColumnTypeBool:
		sourceValues := batch.Columns[def.Name]
		b.values64 = ensureInt64Len(b.values64[:0], end-start)
		for row := start; row < end; row++ {
			b.values64[row-start] = sourceValues[b.order[row]]
		}
		b.bools = ensureBoolLen(b.bools[:0], len(b.values64))
		for i, v := range b.values64 {
			if v != 0 && v != 1 {
				return EncodedGranule{}, fmt.Errorf("colgranule: bool value %d outside 0/1 for %s", v, def.Name)
			}
			b.bools[i] = v == 1
		}
		return b.builder.BuildBool(b.bools)
	case ColumnTypeFloat32Vector:
		return b.buildFloat32VectorBlockGranule(batch.Float32Vectors[def.Name], def, start, end)
	case ColumnTypeAdjacencyList:
		return b.buildAdjacencyListBlockGranule(batch.AdjacencyLists[def.Name], start, end)
	default:
		return EncodedGranule{}, fmt.Errorf("colgranule: unsupported column type %s", def.Type)
	}
}

func (b *ColumnPartBuilder) buildFloat32VectorBlockGranule(source Float32VectorColumn, def ColumnDefinition, start int, end int) (EncodedGranule, error) {
	rows := end - start
	valueCount, err := checkedMulInt(rows, def.VectorDims, "float32 vector block values")
	if err != nil {
		return EncodedGranule{}, err
	}
	b.vectors32 = ensureFloat32Len(b.vectors32[:0], valueCount)
	for row := start; row < end; row++ {
		sourceRow := b.order[row]
		sourceStart := sourceRow * def.VectorDims
		dstStart := (row - start) * def.VectorDims
		copy(b.vectors32[dstStart:dstStart+def.VectorDims], source.Values[sourceStart:sourceStart+def.VectorDims])
	}
	return b.builder.BuildFloat32Vectors(b.vectors32, def.VectorDims)
}

func (b *ColumnPartBuilder) buildAdjacencyListBlockGranule(source AdjacencyListColumn, start int, end int) (EncodedGranule, error) {
	rows := end - start
	b.listOffsets = ensureUint32Len(b.listOffsets[:0], rows+1)
	b.listOffsets[0] = 0
	b.listValues = b.listValues[:0]
	for row := start; row < end; row++ {
		sourceRow := b.order[row]
		sourceStart := int(source.Offsets[sourceRow])
		sourceEnd := int(source.Offsets[sourceRow+1])
		b.listValues = append(b.listValues, source.Values[sourceStart:sourceEnd]...)
		if len(b.listValues) > math.MaxUint32 {
			return EncodedGranule{}, fmt.Errorf("colgranule: adjacency block values=%d exceed uint32 offsets", len(b.listValues))
		}
		b.listOffsets[row-start+1] = uint32(len(b.listValues))
	}
	return b.builder.BuildInt64AdjacencyLists(b.listOffsets, b.listValues)
}

func exclusiveInt64Upper(v int64) int64 {
	if v == math.MaxInt64 {
		return math.MaxInt64
	}
	return v + 1
}
