package typedcolumn

import (
	"errors"
	"fmt"
	"math"
	"sort"
)

const columnPartDescriptorVersion = 3

type ColumnSchemaMode string

const (
	ColumnSchemaFixed ColumnSchemaMode = "fixed"
)

type ColumnType string

const (
	ColumnTypeInt64              ColumnType = "int64"
	ColumnTypeLowCardinalityCode ColumnType = "low_cardinality_code"
	ColumnTypeBool               ColumnType = "bool"
	ColumnTypeFloat32            ColumnType = "float32"
	ColumnTypeFloat64            ColumnType = "float64"
	ColumnTypeInt8               ColumnType = "int8"
	ColumnTypeUint8              ColumnType = "uint8"
	ColumnTypeInt16              ColumnType = "int16"
	ColumnTypeUint16             ColumnType = "uint16"
	ColumnTypeInt32              ColumnType = "int32"
	ColumnTypeUint32             ColumnType = "uint32"
	ColumnTypeUint64             ColumnType = "uint64"
	// Float16 and BFloat16 are storage-only raw 16-bit bit payloads.
	ColumnTypeFloat16           ColumnType = "float16"
	ColumnTypeBFloat16          ColumnType = "bfloat16"
	ColumnTypeUint8Vector       ColumnType = "uint8_vector"
	ColumnTypeInt8Vector        ColumnType = "int8_vector"
	ColumnTypeUint16Vector      ColumnType = "uint16_vector"
	ColumnTypeInt16Vector       ColumnType = "int16_vector"
	ColumnTypeUint32Vector      ColumnType = "uint32_vector"
	ColumnTypeInt32Vector       ColumnType = "int32_vector"
	ColumnTypeUint64Vector      ColumnType = "uint64_vector"
	ColumnTypeInt64Vector       ColumnType = "int64_vector"
	ColumnTypeFloat16Vector     ColumnType = "float16_vector"
	ColumnTypeBFloat16Vector    ColumnType = "bfloat16_vector"
	ColumnTypeFloat32Vector     ColumnType = "float32_vector"
	ColumnTypeFloat64Vector     ColumnType = "float64_vector"
	ColumnTypeFixedBytes        ColumnType = "fixed_bytes"
	ColumnTypePackedBitVector   ColumnType = "packed_bit_vector"
	ColumnTypePackedUint2Vector ColumnType = "packed_uint2_vector"
	ColumnTypePackedUint4Vector ColumnType = "packed_uint4_vector"
	ColumnTypeUint32List        ColumnType = "uint32_list"
	ColumnTypeBytes             ColumnType = "bytes"
	ColumnTypeAdjacencyList     ColumnType = "adjacency_list"
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

type Options struct {
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
	Name               string
	Type               ColumnType
	Encoding           Encoding
	Compression        Compression
	CompressionSet     bool
	Cardinality        uint32
	FixedWidthElements int
	BitsPerElement     int
	CodecBlockRows     int
	StatsDisabled      bool
}

type Batch struct {
	Rows                   int
	Columns                map[string][]int64
	Nulls                  map[string][]bool
	Defaults               map[string][]bool
	DefaultValues          map[string]int64
	Float32Columns         map[string][]float32
	Float64Columns         map[string][]float64
	Int8Columns            map[string][]int8
	Uint8Columns           map[string][]uint8
	Int16Columns           map[string][]int16
	Uint16Columns          map[string][]uint16
	Int32Columns           map[string][]int32
	Uint32Columns          map[string][]uint32
	Uint64Columns          map[string][]uint64
	Float16Columns         map[string][]uint16
	BFloat16Columns        map[string][]uint16
	Float32Vectors         map[string][]float32
	DenseFixedWidthVectors map[string]RawDenseFixedWidth
	FixedBytesColumns      map[string]FixedBytesRows
	PackedUintColumns      map[string]PackedUintRows
	Uint32Vectors          map[string][]uint32
	Uint32OffsetsLists     map[string]RawUint32OffsetsList
	BytesColumns           map[string]RawBytesOffsets
}

type ColumnPart struct {
	Options           Options
	Descriptor        ColumnPartDescriptor
	Columns           map[string]ColumnPartColumn
	Marks             []SortKeyMark
	Locators          map[int64]RowLocator
	AggregateMetadata map[string]AggregateMetadata
	ColumnStats       ColumnPartStats
	PruningMetadata   ColumnPartPruning
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
	Name               string
	Type               ColumnType
	FixedWidthElements int
	BitsPerElement     int
	Blocks             []ColumnBlockDescriptor
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
	opts      Options
	order     []int
	values64  []int64
	codes32   []uint32
	bools     []bool
	nulls     []bool
	defaults  []bool
	float32s  []float32
	float64s  []float64
	int8s     []int8
	u8s       []uint8
	int16s    []int16
	u16s      []uint16
	int32s    []int32
	u32s      []uint32
	u64s      []uint64
	float16s  []uint16
	bfloat16s []uint16
	denseRaw  []byte
	packedRaw []byte
	u32dense  []uint32
	u32offset []uint64
	bytesData []byte
	bytesOff  []uint64
	builder   *GranuleBuilder
}

func NewColumnPartBuilder(opts Options) (*ColumnPartBuilder, error) {
	normalized, err := normalizeOptions(opts)
	if err != nil {
		return nil, err
	}
	return &ColumnPartBuilder{opts: normalized, builder: NewGranuleBuilder(Config{})}, nil
}

func BuildColumnPart(partID uint64, opts Options, batch Batch) (*ColumnPart, error) {
	builder, err := NewColumnPartBuilder(opts)
	if err != nil {
		return nil, err
	}
	return builder.Build(partID, batch)
}

func (b *ColumnPartBuilder) Build(partID uint64, batch Batch) (*ColumnPart, error) {
	rows, err := validateBatch(batch, b.opts.Columns)
	if err != nil {
		return nil, err
	}
	if b.opts.PartPolicy.AdaptiveMarkSizing.Enabled {
		rawBytes, err := EstimateBatchUncompressedBytes(batch, b.opts.Columns)
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
	stats, err := buildColumnPartStats(part)
	if err != nil {
		return nil, err
	}
	part.ColumnStats = stats
	pruning, err := buildColumnPartPruning(part)
	if err != nil {
		return nil, err
	}
	part.PruningMetadata = pruning
	return part, nil
}

func (p *ColumnPart) LocatePrimaryID(primaryID int64) (RowLocator, bool) {
	locator, ok := p.Locators[primaryID]
	return locator, ok
}

func (p *ColumnPart) EncodedColumnBlocks(name string) ([]EncodedGranule, error) {
	column, ok := p.Columns[name]
	if !ok {
		return nil, fmt.Errorf("typedcolumn: missing column %s", name)
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
	part     *ColumnPart
	reader   GranuleReader
	values   []int64
	codes    []uint32
	bools    []bool
	nulls    []bool
	defaults []bool
	scratch  []int64
}

type ProjectedScanResult struct {
	Rows        int
	Columns     map[string][]int64
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
		return ProjectedScanResult{}, errors.New("typedcolumn: nil part scanner")
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

func (s *ColumnPartScanner) ScanProjectedRows(columns []string, rows []int) (ProjectedScanResult, error) {
	return s.ScanProjectedRowsInto(make(map[string][]int64, len(columns)), columns, rows)
}

func (s *ColumnPartScanner) ScanProjectedRowsInto(dst map[string][]int64, columns []string, rows []int) (ProjectedScanResult, error) {
	if s.part == nil {
		return ProjectedScanResult{}, errors.New("typedcolumn: nil part scanner")
	}
	if rows == nil {
		return s.ScanProjectedInto(dst, columns)
	}
	projection, err := s.validateProjection(columns)
	if err != nil {
		return ProjectedScanResult{}, err
	}
	if err := validateProjectedScanRows(rows, s.part.Descriptor.RowCount); err != nil {
		return ProjectedScanResult{}, err
	}
	if dst == nil {
		dst = make(map[string][]int64, len(projection))
	}
	out := ProjectedScanResult{
		Rows:    len(rows),
		Columns: dst,
		Diagnostics: PartScanDiagnostics{
			RowsScanned:        len(rows),
			ColumnsProjected:   len(projection),
			GranulesConsidered: len(s.part.Descriptor.Granules),
		},
	}
	next := make(map[string][]int64, len(projection))
	for _, name := range projection {
		values, diagnostics, err := s.scanColumnRowsInto(name, dst[name], rows)
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

func validateProjectedScanRows(rows []int, totalRows int) error {
	previous := -1
	for i, row := range rows {
		if row < 0 || row >= totalRows {
			return fmt.Errorf("typedcolumn: projected row[%d]=%d outside part rows=%d", i, row, totalRows)
		}
		if row <= previous {
			return fmt.Errorf("typedcolumn: projected rows must be strictly increasing at index %d (%d after %d)", i, row, previous)
		}
		previous = row
	}
	return nil
}

func (s *ColumnPartScanner) validateProjection(columns []string) ([]string, error) {
	if len(columns) == 0 {
		return nil, errors.New("typedcolumn: empty projection")
	}
	seen := make(map[string]struct{}, len(columns))
	for _, name := range columns {
		if name == "" {
			return nil, errors.New("typedcolumn: empty projection column")
		}
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("typedcolumn: duplicate projection column %s", name)
		}
		if _, ok := s.part.Columns[name]; !ok {
			return nil, fmt.Errorf("typedcolumn: missing column %s", name)
		}
		seen[name] = struct{}{}
	}
	return columns, nil
}

func (s *ColumnPartScanner) ValueAt(locator RowLocator, columnName string) (int64, error) {
	if s.part == nil {
		return 0, errors.New("typedcolumn: nil part scanner")
	}
	if locator.PartID != s.part.Descriptor.PartID {
		return 0, fmt.Errorf("typedcolumn: locator part=%d want %d", locator.PartID, s.part.Descriptor.PartID)
	}
	column, ok := s.part.Columns[columnName]
	if !ok {
		return 0, fmt.Errorf("typedcolumn: missing column %s", columnName)
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
	return 0, fmt.Errorf("typedcolumn: locator row %d outside column %s", locator.PartRow, columnName)
}

func (s *ColumnPartScanner) scanColumn(name string) ([]int64, PartScanDiagnostics, error) {
	return s.scanColumnInto(name, nil)
}

func (s *ColumnPartScanner) scanColumnInto(name string, dst []int64) ([]int64, PartScanDiagnostics, error) {
	column, ok := s.part.Columns[name]
	if !ok {
		return nil, PartScanDiagnostics{}, fmt.Errorf("typedcolumn: missing column %s", name)
	}
	out := ensureInt64Len(dst[:0], s.part.Descriptor.RowCount)
	var diagnostics PartScanDiagnostics
	granulesDecoded, err := countGranulesCoveredByBlocks(column.Blocks)
	if err != nil {
		return nil, diagnostics, fmt.Errorf("typedcolumn: column %s: %w", name, err)
	}
	diagnostics.GranulesDecoded = granulesDecoded
	for _, block := range column.Blocks {
		values, err := s.decodeBlock(column.Definition.Type, block.Granule)
		if err != nil {
			return nil, diagnostics, err
		}
		if len(values) != block.Descriptor.RowCount {
			return nil, diagnostics, fmt.Errorf("typedcolumn: block rows=%d decoded=%d", block.Descriptor.RowCount, len(values))
		}
		copy(out[block.Descriptor.FirstRow:block.Descriptor.FirstRow+block.Descriptor.RowCount], values)
		diagnostics.BlocksDecoded++
		diagnostics.BytesDecoded += block.Granule.RawBytes
	}
	return out, diagnostics, nil
}

func (s *ColumnPartScanner) scanColumnRowsInto(name string, dst []int64, rows []int) ([]int64, PartScanDiagnostics, error) {
	column, ok := s.part.Columns[name]
	if !ok {
		return nil, PartScanDiagnostics{}, fmt.Errorf("typedcolumn: missing column %s", name)
	}
	if len(rows) == 0 {
		return dst[:0], PartScanDiagnostics{}, nil
	}
	if rows[0] < 0 || rows[len(rows)-1] >= s.part.Descriptor.RowCount {
		return nil, PartScanDiagnostics{}, fmt.Errorf("typedcolumn: visible row range [%d,%d] outside part rows=%d", rows[0], rows[len(rows)-1], s.part.Descriptor.RowCount)
	}
	out := dst[:0]
	if cap(out) < len(rows) {
		out = make([]int64, 0, len(rows))
	}
	var diagnostics PartScanDiagnostics
	coveredStart := -1
	coveredEnd := -1
	prevFirstGranule := -1
	rowIndex := 0
	for _, block := range column.Blocks {
		first := block.Descriptor.FirstRow
		limit := first + block.Descriptor.RowCount
		for rowIndex < len(rows) && rows[rowIndex] < first {
			return nil, diagnostics, fmt.Errorf("typedcolumn: visible row %d before block %d first row %d", rows[rowIndex], block.Descriptor.CodecBlockOrdinal, first)
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
			return nil, diagnostics, fmt.Errorf("typedcolumn: block rows=%d decoded=%d", block.Descriptor.RowCount, len(values))
		}
		for _, row := range rows[start:rowIndex] {
			out = append(out, values[row-first])
		}
		diagnostics.BlocksDecoded++
		diagnostics.BytesDecoded += block.Granule.RawBytes
		if block.Descriptor.FirstGranule < 0 || block.Descriptor.LastGranule < block.Descriptor.FirstGranule {
			return nil, diagnostics, fmt.Errorf("typedcolumn: invalid granule range %d..%d", block.Descriptor.FirstGranule, block.Descriptor.LastGranule)
		}
		if prevFirstGranule >= 0 && block.Descriptor.FirstGranule < prevFirstGranule {
			return nil, diagnostics, fmt.Errorf("typedcolumn: granule ranges out of order: %d after %d", block.Descriptor.FirstGranule, prevFirstGranule)
		}
		prevFirstGranule = block.Descriptor.FirstGranule
		coveredStart, coveredEnd = extendGranuleCoverage(coveredStart, coveredEnd, block.Descriptor.FirstGranule, block.Descriptor.LastGranule, &diagnostics.GranulesDecoded)
	}
	if coveredStart >= 0 {
		diagnostics.GranulesDecoded += coveredEnd - coveredStart + 1
	}
	if rowIndex != len(rows) {
		return nil, diagnostics, fmt.Errorf("typedcolumn: %d visible rows outside column %s blocks", len(rows)-rowIndex, name)
	}
	diagnostics.RowsScanned = len(rows)
	return out, diagnostics, nil
}

func extendGranuleCoverage(coveredStart, coveredEnd, first, last int, total *int) (int, int) {
	if coveredStart < 0 {
		return first, last
	}
	if first <= coveredEnd+1 {
		if last > coveredEnd {
			coveredEnd = last
		}
		return coveredStart, coveredEnd
	}
	*total += coveredEnd - coveredStart + 1
	return first, last
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
	if g.Encoding == EncodingNullableInt64 {
		values, nulls, defaults, err := s.reader.DecodeNullableInt64Into(s.values[:0], s.nulls[:0], s.defaults[:0], g)
		if err != nil {
			return nil, err
		}
		s.values, s.nulls, s.defaults = values, nulls, defaults
		return values, validateNullableDecodedCarrierValues(columnType, values)
	}
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
	case ColumnTypeInt8, ColumnTypeUint8, ColumnTypeInt16, ColumnTypeUint16, ColumnTypeInt32, ColumnTypeUint32:
		values, err := s.reader.DecodeIntegerAsInt64Into(s.scratch[:0], columnType, g)
		if err != nil {
			return nil, err
		}
		s.scratch = values
		return values, nil
	default:
		return nil, fmt.Errorf("typedcolumn: unsupported column type %s", columnType)
	}
}

func normalizeOptions(opts Options) (Options, error) {
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
		return Options{}, fmt.Errorf("typedcolumn: unsupported schema mode %s", opts.SchemaMode)
	}
	var err error
	if opts.PartPolicy.AdaptiveMarkSizing.Enabled {
		opts.PartPolicy.AdaptiveMarkSizing, err = NormalizeColumnAdaptiveMarkSizing(opts.PartPolicy.AdaptiveMarkSizing, opts.PartPolicy.RowsPerGranule)
		if err != nil {
			return Options{}, err
		}
		opts.PartPolicy.RowsPerGranule = opts.PartPolicy.AdaptiveMarkSizing.MaxRows
	} else {
		if opts.PartPolicy.RowsPerGranule == 0 {
			opts.PartPolicy.RowsPerGranule = DefaultRowsPerGranule
		}
		if opts.PartPolicy.RowsPerGranule <= 0 {
			return Options{}, fmt.Errorf("typedcolumn: invalid rows per granule %d", opts.PartPolicy.RowsPerGranule)
		}
	}
	if opts.PartPolicy.DefaultCodecBlockRows < 0 {
		return Options{}, fmt.Errorf("typedcolumn: invalid default codec block rows %d", opts.PartPolicy.DefaultCodecBlockRows)
	}
	if err := validateCompression(opts.Compression.Default); err != nil {
		return Options{}, fmt.Errorf("typedcolumn: unsupported default compression %s", opts.Compression.Default)
	}
	if len(opts.LogicalPrimaryKey.Columns) != 1 {
		return Options{}, fmt.Errorf("typedcolumn: engine requires exactly one logical primary key column, got %d", len(opts.LogicalPrimaryKey.Columns))
	}
	if len(opts.SortKey.Columns) == 0 {
		opts.SortKey.Columns = []SortKeyColumn{{Column: opts.LogicalPrimaryKey.Columns[0]}}
	}
	if len(opts.Columns) == 0 {
		return Options{}, errors.New("typedcolumn: no declared columns")
	}
	seen := make(map[string]struct{}, len(opts.Columns))
	columnsByName := make(map[string]ColumnDefinition, len(opts.Columns))
	for i := range opts.Columns {
		def, err := normalizeColumnDefinition(opts.Columns[i], opts.Compression.Default)
		if err != nil {
			return Options{}, err
		}
		if _, ok := seen[def.Name]; ok {
			return Options{}, fmt.Errorf("typedcolumn: duplicate column %s", def.Name)
		}
		seen[def.Name] = struct{}{}
		columnsByName[def.Name] = def
		opts.Columns[i] = def
	}
	primaryKeyDefinition, ok := columnsByName[opts.LogicalPrimaryKey.Columns[0]]
	if !ok {
		return Options{}, fmt.Errorf("typedcolumn: logical primary key column %s is not declared", opts.LogicalPrimaryKey.Columns[0])
	}
	if !isInt64SortCarrier(primaryKeyDefinition.Type) {
		return Options{}, fmt.Errorf("typedcolumn: logical primary key column %s type=%s is not scalar/int64 sort carrier", opts.LogicalPrimaryKey.Columns[0], primaryKeyDefinition.Type)
	}
	for i := range opts.SortKey.Columns {
		c := &opts.SortKey.Columns[i]
		if c.Column == "" {
			return Options{}, fmt.Errorf("typedcolumn: empty sort key column at %d", i)
		}
		def, ok := columnsByName[c.Column]
		if !ok {
			return Options{}, fmt.Errorf("typedcolumn: sort key column %s is not declared", c.Column)
		}
		if !isInt64SortCarrier(def.Type) {
			return Options{}, fmt.Errorf("typedcolumn: sort key column %s type=%s is not scalar/int64 sort carrier", c.Column, def.Type)
		}
		if c.Direction == "" {
			c.Direction = SortKeyAsc
		}
		if c.Direction != SortKeyAsc && c.Direction != SortKeyDesc {
			return Options{}, fmt.Errorf("typedcolumn: unsupported sort direction %s", c.Direction)
		}
		if c.Direction == SortKeyDesc {
			return Options{}, errors.New("typedcolumn: descending sort keys are not supported by transplant marks yet")
		}
		if c.Nulls != SortKeyNullsDefault && c.Nulls != SortKeyNullsFirst && c.Nulls != SortKeyNullsLast {
			return Options{}, fmt.Errorf("typedcolumn: unsupported null order %s", c.Nulls)
		}
	}
	seenMetadata := make(map[string]struct{}, len(opts.AggregateMetadata))
	for i := range opts.AggregateMetadata {
		def, err := normalizeAggregateMetadataDefinition(opts.AggregateMetadata[i], columnsByName)
		if err != nil {
			return Options{}, err
		}
		if _, ok := seenMetadata[def.Name]; ok {
			return Options{}, fmt.Errorf("typedcolumn: duplicate aggregate metadata %s", def.Name)
		}
		seenMetadata[def.Name] = struct{}{}
		opts.AggregateMetadata[i] = def
	}
	return opts, nil
}

func normalizeColumnDefinition(def ColumnDefinition, defaultCompression Compression) (ColumnDefinition, error) {
	if def.Name == "" {
		return ColumnDefinition{}, errors.New("typedcolumn: empty column name")
	}
	if def.Type == "" {
		def.Type = ColumnTypeInt64
	}
	if def.CodecBlockRows < 0 {
		return ColumnDefinition{}, fmt.Errorf("typedcolumn: invalid codec block rows %d for %s", def.CodecBlockRows, def.Name)
	}
	if !def.CompressionSet && def.Compression == CompressionNone && defaultCompression != CompressionNone {
		def.Compression = defaultCompression
	}
	if err := validateCompression(def.Compression); err != nil {
		return ColumnDefinition{}, fmt.Errorf("typedcolumn: unsupported compression %s for %s", def.Compression, def.Name)
	}
	switch def.Type {
	case ColumnTypeInt64:
		if def.Encoding == 0 {
			def.Encoding = EncodingDeltaVarint
		}
		if def.Encoding != EncodingRawInt64 && def.Encoding != EncodingDeltaVarint && def.Encoding != EncodingDoubleDeltaVarint && def.Encoding != EncodingNullableInt64 {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: unsupported int64 encoding %s for %s", def.Encoding, def.Name)
		}
	case ColumnTypeLowCardinalityCode:
		if def.Encoding == 0 {
			def.Encoding = EncodingLowCardinalityUint32
		}
		if def.Encoding != EncodingLowCardinalityUint32 && def.Encoding != EncodingNullableInt64 {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: unsupported low-cardinality encoding %s for %s", def.Encoding, def.Name)
		}
	case ColumnTypeBool:
		if def.Encoding == 0 {
			def.Encoding = EncodingBoolBitpackRLE
		}
		if def.Encoding != EncodingBoolBitpackRLE && def.Encoding != EncodingNullableInt64 {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: unsupported bool encoding %s for %s", def.Encoding, def.Name)
		}
	case ColumnTypeFloat32:
		if def.Encoding == 0 {
			def.Encoding = EncodingRawFloat32
		}
		if def.Encoding != EncodingRawFloat32 {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: unsupported float32 encoding %s for %s", def.Encoding, def.Name)
		}
	case ColumnTypeFloat64:
		if def.Encoding == 0 {
			def.Encoding = EncodingRawFloat64
		}
		if def.Encoding != EncodingRawFloat64 {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: unsupported float64 encoding %s for %s", def.Encoding, def.Name)
		}
	case ColumnTypeInt8, ColumnTypeUint8, ColumnTypeInt16, ColumnTypeUint16, ColumnTypeInt32, ColumnTypeUint32, ColumnTypeUint64, ColumnTypeFloat16, ColumnTypeBFloat16:
		want := rawScalarEncodingForColumnType(def.Type)
		if want == 0 {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: unsupported scalar column type %s for %s", def.Type, def.Name)
		}
		if def.Encoding == 0 {
			def.Encoding = want
		}
		if def.Encoding != want {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: unsupported %s encoding %s for %s", def.Type, def.Encoding, def.Name)
		}
		if def.FixedWidthElements != 0 {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: %s column %s requires fixed_width_elements=0", def.Type, def.Name)
		}
	case ColumnTypeFloat32Vector:
		if def.FixedWidthElements <= 0 {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: float32_vector column %s requires positive fixed-width elements", def.Name)
		}
		if def.BitsPerElement != 0 {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: float32_vector column %s requires bits_per_element=0", def.Name)
		}
		if def.Encoding == 0 {
			def.Encoding = EncodingRawFloat32Vector
		}
		if def.Encoding != EncodingRawFloat32Vector {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: unsupported float32_vector encoding %s for %s", def.Encoding, def.Name)
		}
		if def.Compression != CompressionNone {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: float32_vector column %s requires uncompressed dense sections", def.Name)
		}
	case ColumnTypeUint8Vector, ColumnTypeInt8Vector, ColumnTypeUint16Vector, ColumnTypeInt16Vector, ColumnTypeUint32Vector, ColumnTypeInt32Vector, ColumnTypeUint64Vector, ColumnTypeInt64Vector, ColumnTypeFloat16Vector, ColumnTypeBFloat16Vector, ColumnTypeFloat64Vector:
		if def.FixedWidthElements <= 0 {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: dense vector column %s type=%s requires positive fixed-width elements", def.Name, def.Type)
		}
		if def.BitsPerElement != 0 {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: dense vector column %s type=%s requires bits_per_element=0", def.Name, def.Type)
		}
		wantEncoding, ok := DenseFixedWidthVectorEncoding(def.Type)
		if !ok || wantEncoding == EncodingRawFloat32Vector {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: unsupported dense vector type %s for %s", def.Type, def.Name)
		}
		if def.Encoding == 0 {
			def.Encoding = wantEncoding
		}
		if def.Encoding != wantEncoding {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: unsupported dense vector encoding %s for %s type=%s want %s", def.Encoding, def.Name, def.Type, wantEncoding)
		}
		if def.Compression != CompressionNone {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: dense vector column %s type=%s requires uncompressed dense sections", def.Name, def.Type)
		}
	case ColumnTypeFixedBytes:
		if def.FixedWidthElements <= 0 {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: fixed_bytes column %s requires positive bytes_per_row/fixed-width elements", def.Name)
		}
		if def.BitsPerElement != 0 {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: fixed_bytes column %s requires bits_per_element=0", def.Name)
		}
		if def.Encoding == 0 {
			def.Encoding = EncodingRawFixedBytes
		}
		if def.Encoding != EncodingRawFixedBytes {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: unsupported fixed_bytes encoding %s for %s", def.Encoding, def.Name)
		}
		if def.Compression != CompressionNone {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: fixed_bytes column %s requires uncompressed raw sections", def.Name)
		}
	case ColumnTypePackedBitVector, ColumnTypePackedUint2Vector, ColumnTypePackedUint4Vector:
		bitsPerElement, ok := PackedUintVectorBits(def.Type)
		if !ok {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: unsupported packed_uint vector type %s for %s", def.Type, def.Name)
		}
		if def.FixedWidthElements <= 0 {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: packed_uint vector column %s type=%s requires positive fixed-width elements", def.Name, def.Type)
		}
		if def.BitsPerElement == 0 {
			def.BitsPerElement = bitsPerElement
		}
		if def.BitsPerElement != bitsPerElement {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: packed_uint vector column %s type=%s bits_per_element=%d want %d", def.Name, def.Type, def.BitsPerElement, bitsPerElement)
		}
		wantEncoding, _ := PackedUintVectorEncoding(def.Type)
		if def.Encoding == 0 {
			def.Encoding = wantEncoding
		}
		if def.Encoding != wantEncoding {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: unsupported packed_uint vector encoding %s for %s type=%s want %s", def.Encoding, def.Name, def.Type, wantEncoding)
		}
		if def.Compression != CompressionNone {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: packed_uint vector column %s type=%s requires uncompressed raw sections", def.Name, def.Type)
		}
	case ColumnTypeUint32List:
		if def.Encoding == 0 {
			def.Encoding = EncodingRawUint32OffsetsList
		}
		if def.Encoding != EncodingRawUint32OffsetsList {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: unsupported uint32_list encoding %s for %s", def.Encoding, def.Name)
		}
		if def.FixedWidthElements != 0 {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: uint32_list column %s requires fixed-width elements=0", def.Name)
		}
		if def.BitsPerElement != 0 {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: uint32_list column %s requires bits_per_element=0", def.Name)
		}
		if def.Compression != CompressionNone {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: uint32_list column %s requires uncompressed offsets-list sections", def.Name)
		}
	case ColumnTypeBytes:
		if def.Encoding == 0 {
			def.Encoding = EncodingRawBytesOffsets
		}
		if def.Encoding != EncodingRawBytesOffsets {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: unsupported bytes encoding %s for %s", def.Encoding, def.Name)
		}
		if def.FixedWidthElements != 0 {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: bytes column %s requires fixed-width elements=0", def.Name)
		}
		if def.BitsPerElement != 0 {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: bytes column %s requires bits_per_element=0", def.Name)
		}
		if def.Compression != CompressionNone {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: bytes column %s requires uncompressed offsets sections", def.Name)
		}
	case ColumnTypeAdjacencyList:
		if def.BitsPerElement != 0 {
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: adjacency_list column %s requires bits_per_element=0", def.Name)
		}
		if def.Encoding == 0 {
			def.Encoding = EncodingRawUint32Dense
		}
		switch def.Encoding {
		case EncodingRawUint32Dense:
			if def.FixedWidthElements <= 0 {
				return ColumnDefinition{}, fmt.Errorf("typedcolumn: adjacency_list column %s requires positive fixed-width elements", def.Name)
			}
			if def.Compression != CompressionNone {
				return ColumnDefinition{}, fmt.Errorf("typedcolumn: adjacency_list column %s requires uncompressed dense sections", def.Name)
			}
		case EncodingRawUint32OffsetsList:
			if def.FixedWidthElements != 0 {
				return ColumnDefinition{}, fmt.Errorf("typedcolumn: adjacency_list column %s raw_uint32_offsets_list requires fixed-width elements=0", def.Name)
			}
			if def.Compression != CompressionNone {
				return ColumnDefinition{}, fmt.Errorf("typedcolumn: adjacency_list column %s requires uncompressed offsets-list sections", def.Name)
			}
		default:
			return ColumnDefinition{}, fmt.Errorf("typedcolumn: unsupported adjacency_list encoding %s for %s", def.Encoding, def.Name)
		}
	default:
		return ColumnDefinition{}, fmt.Errorf("typedcolumn: unsupported column type %s for %s", def.Type, def.Name)
	}
	if !IsDenseFixedWidthVectorColumnType(def.Type) && !IsPackedUintVectorColumnType(def.Type) && def.Type != ColumnTypeFixedBytes && def.Type != ColumnTypeAdjacencyList && def.Type != ColumnTypeUint32List && def.Type != ColumnTypeBytes && def.FixedWidthElements != 0 {
		return ColumnDefinition{}, fmt.Errorf("typedcolumn: scalar column %s has fixed-width elements=%d", def.Name, def.FixedWidthElements)
	}
	if !IsPackedUintVectorColumnType(def.Type) && def.BitsPerElement != 0 {
		return ColumnDefinition{}, fmt.Errorf("typedcolumn: column %s type=%s has bits_per_element=%d", def.Name, def.Type, def.BitsPerElement)
	}
	return def, nil
}

func validateCompression(compression Compression) error {
	switch compression {
	case CompressionNone, CompressionSnappy, CompressionLZ4, CompressionZSTD:
		return nil
	default:
		return fmt.Errorf("typedcolumn: unsupported compression %s", compression)
	}
}

func validateBatch(batch Batch, defs []ColumnDefinition) (int, error) {
	declared := make(map[string]ColumnDefinition, len(defs))
	nullableDeclared := make(map[string]struct{}, len(defs))
	rows := batch.Rows
	for _, def := range defs {
		declared[def.Name] = def
		switch def.Type {
		case ColumnTypeFloat32:
			values, ok := batch.Float32Columns[def.Name]
			if !ok {
				return 0, fmt.Errorf("typedcolumn: missing float32 column %s", def.Name)
			}
			if rows == 0 {
				rows = len(values)
			}
			if len(values) != rows {
				return 0, fmt.Errorf("typedcolumn: column %s rows=%d want=%d", def.Name, len(values), rows)
			}
		case ColumnTypeFloat64:
			values, ok := batch.Float64Columns[def.Name]
			if !ok {
				return 0, fmt.Errorf("typedcolumn: missing float64 column %s", def.Name)
			}
			if rows == 0 {
				rows = len(values)
			}
			if len(values) != rows {
				return 0, fmt.Errorf("typedcolumn: column %s rows=%d want=%d", def.Name, len(values), rows)
			}
		case ColumnTypeInt8:
			values, ok := batch.Int8Columns[def.Name]
			if !ok {
				return 0, fmt.Errorf("typedcolumn: missing int8 column %s", def.Name)
			}
			if rows == 0 {
				rows = len(values)
			}
			if len(values) != rows {
				return 0, fmt.Errorf("typedcolumn: column %s rows=%d want=%d", def.Name, len(values), rows)
			}
		case ColumnTypeUint8:
			values, ok := batch.Uint8Columns[def.Name]
			if !ok {
				return 0, fmt.Errorf("typedcolumn: missing uint8 column %s", def.Name)
			}
			if rows == 0 {
				rows = len(values)
			}
			if len(values) != rows {
				return 0, fmt.Errorf("typedcolumn: column %s rows=%d want=%d", def.Name, len(values), rows)
			}
		case ColumnTypeInt16:
			values, ok := batch.Int16Columns[def.Name]
			if !ok {
				return 0, fmt.Errorf("typedcolumn: missing int16 column %s", def.Name)
			}
			if rows == 0 {
				rows = len(values)
			}
			if len(values) != rows {
				return 0, fmt.Errorf("typedcolumn: column %s rows=%d want=%d", def.Name, len(values), rows)
			}
		case ColumnTypeUint16:
			values, ok := batch.Uint16Columns[def.Name]
			if !ok {
				return 0, fmt.Errorf("typedcolumn: missing uint16 column %s", def.Name)
			}
			if rows == 0 {
				rows = len(values)
			}
			if len(values) != rows {
				return 0, fmt.Errorf("typedcolumn: column %s rows=%d want=%d", def.Name, len(values), rows)
			}
		case ColumnTypeInt32:
			values, ok := batch.Int32Columns[def.Name]
			if !ok {
				return 0, fmt.Errorf("typedcolumn: missing int32 column %s", def.Name)
			}
			if rows == 0 {
				rows = len(values)
			}
			if len(values) != rows {
				return 0, fmt.Errorf("typedcolumn: column %s rows=%d want=%d", def.Name, len(values), rows)
			}
		case ColumnTypeUint32:
			values, ok := batch.Uint32Columns[def.Name]
			if !ok {
				return 0, fmt.Errorf("typedcolumn: missing uint32 column %s", def.Name)
			}
			if rows == 0 {
				rows = len(values)
			}
			if len(values) != rows {
				return 0, fmt.Errorf("typedcolumn: column %s rows=%d want=%d", def.Name, len(values), rows)
			}
		case ColumnTypeUint64:
			values, ok := batch.Uint64Columns[def.Name]
			if !ok {
				return 0, fmt.Errorf("typedcolumn: missing uint64 column %s", def.Name)
			}
			if rows == 0 {
				rows = len(values)
			}
			if len(values) != rows {
				return 0, fmt.Errorf("typedcolumn: column %s rows=%d want=%d", def.Name, len(values), rows)
			}
		case ColumnTypeFloat16:
			values, ok := batch.Float16Columns[def.Name]
			if !ok {
				return 0, fmt.Errorf("typedcolumn: missing float16 column %s", def.Name)
			}
			if rows == 0 {
				rows = len(values)
			}
			if len(values) != rows {
				return 0, fmt.Errorf("typedcolumn: column %s rows=%d want=%d", def.Name, len(values), rows)
			}
		case ColumnTypeBFloat16:
			values, ok := batch.BFloat16Columns[def.Name]
			if !ok {
				return 0, fmt.Errorf("typedcolumn: missing bfloat16 column %s", def.Name)
			}
			if rows == 0 {
				rows = len(values)
			}
			if len(values) != rows {
				return 0, fmt.Errorf("typedcolumn: column %s rows=%d want=%d", def.Name, len(values), rows)
			}
		case ColumnTypeFloat32Vector:
			values, ok := batch.Float32Vectors[def.Name]
			if !ok {
				return 0, fmt.Errorf("typedcolumn: missing float32_vector column %s", def.Name)
			}
			columnRows, err := denseRowsForValues(len(values), def.FixedWidthElements, def.Name)
			if err != nil {
				return 0, err
			}
			if rows == 0 {
				rows = columnRows
			}
			if columnRows != rows {
				return 0, fmt.Errorf("typedcolumn: column %s rows=%d want=%d", def.Name, columnRows, rows)
			}
		case ColumnTypeUint8Vector, ColumnTypeInt8Vector, ColumnTypeUint16Vector, ColumnTypeInt16Vector, ColumnTypeUint32Vector, ColumnTypeInt32Vector, ColumnTypeUint64Vector, ColumnTypeInt64Vector, ColumnTypeFloat16Vector, ColumnTypeBFloat16Vector, ColumnTypeFloat64Vector:
			values, ok := batch.DenseFixedWidthVectors[def.Name]
			if !ok {
				return 0, fmt.Errorf("typedcolumn: missing dense vector column %s", def.Name)
			}
			elementWidth, ok := DenseFixedWidthVectorElementWidth(def.Type)
			if !ok || elementWidth == 0 {
				return 0, fmt.Errorf("typedcolumn: unsupported dense vector type %s", def.Type)
			}
			if values.ElementsPerRow != def.FixedWidthElements || values.ElementWidthBytes != elementWidth {
				return 0, fmt.Errorf("typedcolumn: dense vector column %s metadata elements_per_row=%d width=%d want elements_per_row=%d width=%d", def.Name, values.ElementsPerRow, values.ElementWidthBytes, def.FixedWidthElements, elementWidth)
			}
			if err := validateRawDenseFixedWidth(values, def.Name); err != nil {
				return 0, err
			}
			if rows == 0 {
				rows = values.Rows
			}
			if values.Rows != rows {
				return 0, fmt.Errorf("typedcolumn: column %s rows=%d want=%d", def.Name, values.Rows, rows)
			}
		case ColumnTypeFixedBytes:
			values, ok := batch.FixedBytesColumns[def.Name]
			if !ok {
				return 0, fmt.Errorf("typedcolumn: missing fixed_bytes column %s", def.Name)
			}
			if values.BytesPerRow != def.FixedWidthElements {
				return 0, fmt.Errorf("typedcolumn: fixed_bytes column %s bytes_per_row=%d want %d", def.Name, values.BytesPerRow, def.FixedWidthElements)
			}
			if err := values.Validate(); err != nil {
				return 0, fmt.Errorf("typedcolumn: column %s: %w", def.Name, err)
			}
			if rows == 0 {
				rows = values.Rows
			}
			if values.Rows != rows {
				return 0, fmt.Errorf("typedcolumn: column %s rows=%d want=%d", def.Name, values.Rows, rows)
			}
		case ColumnTypePackedBitVector, ColumnTypePackedUint2Vector, ColumnTypePackedUint4Vector:
			values, ok := batch.PackedUintColumns[def.Name]
			if !ok {
				return 0, fmt.Errorf("typedcolumn: missing packed_uint column %s", def.Name)
			}
			bitsPerElement, _ := PackedUintVectorBits(def.Type)
			if values.ElementsPerRow != def.FixedWidthElements || values.BitsPerElement != bitsPerElement {
				return 0, fmt.Errorf("typedcolumn: packed_uint column %s metadata elements_per_row=%d bits=%d want elements_per_row=%d bits=%d", def.Name, values.ElementsPerRow, values.BitsPerElement, def.FixedWidthElements, bitsPerElement)
			}
			if err := values.Validate(); err != nil {
				return 0, fmt.Errorf("typedcolumn: column %s: %w", def.Name, err)
			}
			if rows == 0 {
				rows = values.Rows
			}
			if values.Rows != rows {
				return 0, fmt.Errorf("typedcolumn: column %s rows=%d want=%d", def.Name, values.Rows, rows)
			}
		case ColumnTypeUint32List:
			list, ok := batch.Uint32OffsetsLists[def.Name]
			if !ok {
				return 0, fmt.Errorf("typedcolumn: missing uint32_list offsets-list column %s", def.Name)
			}
			if err := ValidateRawUint32OffsetsListShape(list.Rows, list.Offsets, uint64(len(list.Values))); err != nil {
				return 0, fmt.Errorf("typedcolumn: column %s: %w", def.Name, err)
			}
			columnRows := list.Rows
			if rows == 0 {
				rows = columnRows
			}
			if columnRows != rows {
				return 0, fmt.Errorf("typedcolumn: column %s rows=%d want=%d", def.Name, columnRows, rows)
			}
		case ColumnTypeBytes:
			bytesColumn, ok := batch.BytesColumns[def.Name]
			if !ok {
				return 0, fmt.Errorf("typedcolumn: missing bytes offsets column %s", def.Name)
			}
			if err := ValidateRawBytesOffsetsShape(bytesColumn.Rows, bytesColumn.Offsets, uint64(len(bytesColumn.Values))); err != nil {
				return 0, fmt.Errorf("typedcolumn: column %s: %w", def.Name, err)
			}
			columnRows := bytesColumn.Rows
			if rows == 0 {
				rows = columnRows
			}
			if columnRows != rows {
				return 0, fmt.Errorf("typedcolumn: column %s rows=%d want=%d", def.Name, columnRows, rows)
			}
		case ColumnTypeAdjacencyList:
			var columnRows int
			switch def.Encoding {
			case EncodingRawUint32OffsetsList:
				list, ok := batch.Uint32OffsetsLists[def.Name]
				if !ok {
					return 0, fmt.Errorf("typedcolumn: missing adjacency_list offsets-list column %s", def.Name)
				}
				if err := ValidateRawUint32OffsetsListShape(list.Rows, list.Offsets, uint64(len(list.Values))); err != nil {
					return 0, fmt.Errorf("typedcolumn: column %s: %w", def.Name, err)
				}
				columnRows = list.Rows
			default:
				values, ok := batch.Uint32Vectors[def.Name]
				if !ok {
					return 0, fmt.Errorf("typedcolumn: missing adjacency_list column %s", def.Name)
				}
				var err error
				columnRows, err = denseRowsForValues(len(values), def.FixedWidthElements, def.Name)
				if err != nil {
					return 0, err
				}
			}
			if rows == 0 {
				rows = columnRows
			}
			if columnRows != rows {
				return 0, fmt.Errorf("typedcolumn: column %s rows=%d want=%d", def.Name, columnRows, rows)
			}
		default:
			values, ok := batch.Columns[def.Name]
			if !ok {
				return 0, fmt.Errorf("typedcolumn: missing column %s", def.Name)
			}
			if rows == 0 {
				rows = len(values)
			}
			if len(values) != rows {
				return 0, fmt.Errorf("typedcolumn: column %s rows=%d want=%d", def.Name, len(values), rows)
			}
			if def.Encoding != EncodingNullableInt64 {
				if _, ok := batch.Nulls[def.Name]; ok {
					return 0, fmt.Errorf("typedcolumn: nullable metadata supplied for non-nullable column %s", def.Name)
				}
				if _, ok := batch.Defaults[def.Name]; ok {
					return 0, fmt.Errorf("typedcolumn: nullable metadata supplied for non-nullable column %s", def.Name)
				}
				if _, ok := batch.DefaultValues[def.Name]; ok {
					return 0, fmt.Errorf("typedcolumn: nullable metadata supplied for non-nullable column %s", def.Name)
				}
				continue
			}
			nullableDeclared[def.Name] = struct{}{}
			if err := validateOptionalBoolRows("nulls for "+def.Name, rows, batch.Nulls[def.Name]); err != nil {
				return 0, err
			}
			if err := validateOptionalBoolRows("defaults for "+def.Name, rows, batch.Defaults[def.Name]); err != nil {
				return 0, err
			}
		}
	}
	for name, values := range batch.Columns {
		def, ok := declared[name]
		if !ok {
			return 0, fmt.Errorf("typedcolumn: undeclared column %s", name)
		}
		if !columnTypeUsesInt64BatchCarrier(def.Type) {
			return 0, fmt.Errorf("typedcolumn: column %s supplied in int64 carrier but declared type %s", name, def.Type)
		}
		if rows > 0 && len(values) != rows {
			return 0, fmt.Errorf("typedcolumn: column %s rows=%d want=%d", name, len(values), rows)
		}
	}
	for name := range batch.Nulls {
		if _, ok := declared[name]; !ok {
			return 0, fmt.Errorf("typedcolumn: undeclared nullable nulls column %s", name)
		}
		if _, ok := nullableDeclared[name]; !ok {
			return 0, fmt.Errorf("typedcolumn: nullable metadata supplied for non-nullable column %s", name)
		}
	}
	for name := range batch.Defaults {
		if _, ok := declared[name]; !ok {
			return 0, fmt.Errorf("typedcolumn: undeclared nullable defaults column %s", name)
		}
		if _, ok := nullableDeclared[name]; !ok {
			return 0, fmt.Errorf("typedcolumn: nullable metadata supplied for non-nullable column %s", name)
		}
	}
	for name := range batch.DefaultValues {
		if _, ok := declared[name]; !ok {
			return 0, fmt.Errorf("typedcolumn: undeclared nullable default value column %s", name)
		}
		if _, ok := nullableDeclared[name]; !ok {
			return 0, fmt.Errorf("typedcolumn: nullable metadata supplied for non-nullable column %s", name)
		}
	}
	for name, values := range batch.Float32Columns {
		if err := validateTypedBatchCarrier(declared, name, ColumnTypeFloat32, "float32", len(values), rows); err != nil {
			return 0, err
		}
	}
	for name, values := range batch.Float64Columns {
		if err := validateTypedBatchCarrier(declared, name, ColumnTypeFloat64, "float64", len(values), rows); err != nil {
			return 0, err
		}
	}
	for name, values := range batch.Int8Columns {
		if err := validateTypedBatchCarrier(declared, name, ColumnTypeInt8, "int8", len(values), rows); err != nil {
			return 0, err
		}
	}
	for name, values := range batch.Uint8Columns {
		if err := validateTypedBatchCarrier(declared, name, ColumnTypeUint8, "uint8", len(values), rows); err != nil {
			return 0, err
		}
	}
	for name, values := range batch.Int16Columns {
		if err := validateTypedBatchCarrier(declared, name, ColumnTypeInt16, "int16", len(values), rows); err != nil {
			return 0, err
		}
	}
	for name, values := range batch.Uint16Columns {
		if err := validateTypedBatchCarrier(declared, name, ColumnTypeUint16, "uint16", len(values), rows); err != nil {
			return 0, err
		}
	}
	for name, values := range batch.Int32Columns {
		if err := validateTypedBatchCarrier(declared, name, ColumnTypeInt32, "int32", len(values), rows); err != nil {
			return 0, err
		}
	}
	for name, values := range batch.Uint32Columns {
		if err := validateTypedBatchCarrier(declared, name, ColumnTypeUint32, "uint32", len(values), rows); err != nil {
			return 0, err
		}
	}
	for name, values := range batch.Uint64Columns {
		if err := validateTypedBatchCarrier(declared, name, ColumnTypeUint64, "uint64", len(values), rows); err != nil {
			return 0, err
		}
	}
	for name, values := range batch.Float16Columns {
		if err := validateTypedBatchCarrier(declared, name, ColumnTypeFloat16, "float16", len(values), rows); err != nil {
			return 0, err
		}
	}
	for name, values := range batch.BFloat16Columns {
		if err := validateTypedBatchCarrier(declared, name, ColumnTypeBFloat16, "bfloat16", len(values), rows); err != nil {
			return 0, err
		}
	}
	for name := range batch.Float32Vectors {
		if _, ok := declared[name]; !ok {
			return 0, fmt.Errorf("typedcolumn: undeclared float32_vector column %s", name)
		}
	}
	for name := range batch.DenseFixedWidthVectors {
		if _, ok := declared[name]; !ok {
			return 0, fmt.Errorf("typedcolumn: undeclared dense vector column %s", name)
		}
	}
	for name := range batch.FixedBytesColumns {
		def, ok := declared[name]
		if !ok {
			return 0, fmt.Errorf("typedcolumn: undeclared fixed_bytes column %s", name)
		}
		if def.Type != ColumnTypeFixedBytes {
			return 0, fmt.Errorf("typedcolumn: column %s supplied in fixed_bytes carrier but declared type %s", name, def.Type)
		}
	}
	for name := range batch.PackedUintColumns {
		def, ok := declared[name]
		if !ok {
			return 0, fmt.Errorf("typedcolumn: undeclared packed_uint column %s", name)
		}
		if !IsPackedUintVectorColumnType(def.Type) {
			return 0, fmt.Errorf("typedcolumn: column %s supplied in packed_uint carrier but declared type %s", name, def.Type)
		}
	}
	for name := range batch.Uint32Vectors {
		if _, ok := declared[name]; !ok {
			return 0, fmt.Errorf("typedcolumn: undeclared adjacency_list column %s", name)
		}
	}
	for name := range batch.Uint32OffsetsLists {
		if _, ok := declared[name]; !ok {
			return 0, fmt.Errorf("typedcolumn: undeclared uint32 offsets-list column %s", name)
		}
	}
	for name := range batch.BytesColumns {
		if _, ok := declared[name]; !ok {
			return 0, fmt.Errorf("typedcolumn: undeclared bytes offsets column %s", name)
		}
	}
	if rows <= 0 {
		if rows == 0 && batchAllowsZeroRowsForOffsetsList(batch, defs) {
			return 0, nil
		}
		return 0, fmt.Errorf("typedcolumn: invalid part rows %d", rows)
	}
	return rows, nil
}

func columnTypeUsesInt64BatchCarrier(t ColumnType) bool {
	switch t {
	case ColumnTypeInt64, ColumnTypeBool, ColumnTypeLowCardinalityCode:
		return true
	default:
		return false
	}
}

func validateTypedBatchCarrier(declared map[string]ColumnDefinition, name string, want ColumnType, carrier string, actualRows, rows int) error {
	def, ok := declared[name]
	if !ok {
		return fmt.Errorf("typedcolumn: undeclared %s column %s", carrier, name)
	}
	if def.Type != want {
		return fmt.Errorf("typedcolumn: column %s supplied in %s carrier but declared type %s", name, carrier, def.Type)
	}
	if rows > 0 && actualRows != rows {
		return fmt.Errorf("typedcolumn: column %s rows=%d want=%d", name, actualRows, rows)
	}
	return nil
}

func batchAllowsZeroRowsForOffsetsList(batch Batch, defs []ColumnDefinition) bool {
	hasOffsetsList := false
	for _, def := range defs {
		switch def.Type {
		case ColumnTypeUint32List, ColumnTypeAdjacencyList:
			if def.Encoding != EncodingRawUint32OffsetsList {
				return false
			}
			list, ok := batch.Uint32OffsetsLists[def.Name]
			if !ok || list.Rows != 0 || len(list.Offsets) != 1 || len(list.Values) != 0 || list.Offsets[0] != 0 {
				return false
			}
			hasOffsetsList = true
		case ColumnTypeBytes:
			if def.Encoding != EncodingRawBytesOffsets {
				return false
			}
			bytesColumn, ok := batch.BytesColumns[def.Name]
			if !ok || bytesColumn.Rows != 0 || len(bytesColumn.Offsets) != 1 || len(bytesColumn.Values) != 0 || bytesColumn.Offsets[0] != 0 {
				return false
			}
			hasOffsetsList = true
		case ColumnTypeFixedBytes:
			values, ok := batch.FixedBytesColumns[def.Name]
			if !ok || values.Rows != 0 || values.BytesPerRow != def.FixedWidthElements || len(values.Values) != 0 {
				return false
			}
			hasOffsetsList = true
		case ColumnTypePackedBitVector, ColumnTypePackedUint2Vector, ColumnTypePackedUint4Vector:
			bitsPerElement, _ := PackedUintVectorBits(def.Type)
			rowBytes, err := PackedUintRowBytes(def.FixedWidthElements, bitsPerElement)
			if err != nil {
				return false
			}
			values, ok := batch.PackedUintColumns[def.Name]
			if !ok || values.Rows != 0 || values.ElementsPerRow != def.FixedWidthElements || values.BitsPerElement != bitsPerElement || values.BytesPerRow != rowBytes || len(values.Values) != 0 {
				return false
			}
			hasOffsetsList = true
		case ColumnTypeInt64:
			if def.Encoding != EncodingRawInt64 || len(batch.Columns[def.Name]) != 0 {
				return false
			}
		default:
			return false
		}
	}
	return hasOffsetsList
}

func (b *ColumnPartBuilder) sortedOrder(batch Batch, rows int, pkColumn string) ([]int, error) {
	if cap(b.order) < rows {
		b.order = make([]int, rows)
	} else {
		b.order = b.order[:rows]
	}
	if b.canUseIdentitySortedOrder(batch, rows, pkColumn) {
		for i := range b.order {
			b.order[i] = i
		}
		return b.order, nil
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

func (b *ColumnPartBuilder) canUseIdentitySortedOrder(batch Batch, rows int, pkColumn string) bool {
	if len(b.opts.SortKey.Columns) != 1 {
		return false
	}
	sortColumn := b.opts.SortKey.Columns[0]
	if sortColumn.Column != pkColumn || sortColumn.Direction != SortKeyAsc || sortColumn.Nulls != SortKeyNullsDefault {
		return false
	}
	pkValues := batch.Columns[pkColumn]
	if len(pkValues) != rows {
		return false
	}
	if rows <= 1 {
		return true
	}
	base := pkValues[0]
	if base > math.MaxInt64-int64(rows-1) {
		return false
	}
	for row := 1; row < rows; row++ {
		if pkValues[row] != base+int64(row) {
			return false
		}
	}
	return true
}

func (b *ColumnPartBuilder) buildGranulesAndLocators(part *ColumnPart, batch Batch, pkColumn string) error {
	rowsPerGranule := b.opts.PartPolicy.RowsPerGranule
	pkValues := batch.Columns[pkColumn]
	for start := 0; start < len(b.order); start += rowsPerGranule {
		end := min(start+rowsPerGranule, len(b.order))
		ordinal := len(part.Descriptor.Granules)
		idLower, idUpper := int64(math.MaxInt64), int64(math.MinInt64)
		for partRow := start; partRow < end; partRow++ {
			primaryID := pkValues[b.order[partRow]]
			if primaryID == math.MaxInt64 {
				return fmt.Errorf("typedcolumn: primary id %d cannot form exclusive upper bound", primaryID)
			}
			if _, exists := part.Locators[primaryID]; exists {
				return fmt.Errorf("typedcolumn: duplicate primary id %d", primaryID)
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

func (b *ColumnPartBuilder) buildColumn(batch Batch, def ColumnDefinition) (ColumnPartColumn, ColumnPartColumnDescriptor, error) {
	blockRows := def.CodecBlockRows
	if blockRows == 0 {
		blockRows = b.opts.PartPolicy.DefaultCodecBlockRows
	}
	if blockRows == 0 {
		blockRows = b.opts.PartPolicy.RowsPerGranule
	}
	column := ColumnPartColumn{Definition: def}
	descriptor := ColumnPartColumnDescriptor{Name: def.Name, Type: def.Type, FixedWidthElements: def.FixedWidthElements, BitsPerElement: def.BitsPerElement}
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

func (b *ColumnPartBuilder) buildColumnBlockGranule(batch Batch, def ColumnDefinition, start int, end int) (EncodedGranule, error) {
	cfg := Config{Encoding: def.Encoding, Compression: def.Compression}
	b.builder.Reset(cfg)
	switch def.Type {
	case ColumnTypeFloat32:
		sourceValues := batch.Float32Columns[def.Name]
		b.float32s = ensureFloat32Len(b.float32s[:0], end-start)
		for row := start; row < end; row++ {
			b.float32s[row-start] = sourceValues[b.order[row]]
		}
		return b.builder.BuildFloat32(b.float32s)
	case ColumnTypeFloat64:
		sourceValues := batch.Float64Columns[def.Name]
		b.float64s = ensureFloat64Len(b.float64s[:0], end-start)
		for row := start; row < end; row++ {
			b.float64s[row-start] = sourceValues[b.order[row]]
		}
		return b.builder.BuildFloat64(b.float64s)
	case ColumnTypeInt8:
		sourceValues := batch.Int8Columns[def.Name]
		b.int8s = ensureInt8Len(b.int8s[:0], end-start)
		for row := start; row < end; row++ {
			b.int8s[row-start] = sourceValues[b.order[row]]
		}
		return b.builder.BuildInt8(b.int8s)
	case ColumnTypeUint8:
		sourceValues := batch.Uint8Columns[def.Name]
		b.u8s = ensureUint8Len(b.u8s[:0], end-start)
		for row := start; row < end; row++ {
			b.u8s[row-start] = sourceValues[b.order[row]]
		}
		return b.builder.BuildUint8(b.u8s)
	case ColumnTypeInt16:
		sourceValues := batch.Int16Columns[def.Name]
		b.int16s = ensureInt16Len(b.int16s[:0], end-start)
		for row := start; row < end; row++ {
			b.int16s[row-start] = sourceValues[b.order[row]]
		}
		return b.builder.BuildInt16(b.int16s)
	case ColumnTypeUint16:
		sourceValues := batch.Uint16Columns[def.Name]
		b.u16s = ensureUint16Len(b.u16s[:0], end-start)
		for row := start; row < end; row++ {
			b.u16s[row-start] = sourceValues[b.order[row]]
		}
		return b.builder.BuildUint16(b.u16s)
	case ColumnTypeInt32:
		sourceValues := batch.Int32Columns[def.Name]
		b.int32s = ensureInt32Len(b.int32s[:0], end-start)
		for row := start; row < end; row++ {
			b.int32s[row-start] = sourceValues[b.order[row]]
		}
		return b.builder.BuildInt32(b.int32s)
	case ColumnTypeUint32:
		sourceValues := batch.Uint32Columns[def.Name]
		b.u32s = ensureUint32Len(b.u32s[:0], end-start)
		for row := start; row < end; row++ {
			b.u32s[row-start] = sourceValues[b.order[row]]
		}
		return b.builder.BuildUint32(b.u32s)
	case ColumnTypeUint64:
		sourceValues := batch.Uint64Columns[def.Name]
		b.u64s = ensureUint64Len(b.u64s[:0], end-start)
		for row := start; row < end; row++ {
			b.u64s[row-start] = sourceValues[b.order[row]]
		}
		return b.builder.BuildUint64(b.u64s)
	case ColumnTypeFloat16:
		sourceValues := batch.Float16Columns[def.Name]
		b.float16s = ensureUint16Len(b.float16s[:0], end-start)
		for row := start; row < end; row++ {
			b.float16s[row-start] = sourceValues[b.order[row]]
		}
		return b.builder.BuildFloat16Bits(b.float16s)
	case ColumnTypeBFloat16:
		sourceValues := batch.BFloat16Columns[def.Name]
		b.bfloat16s = ensureUint16Len(b.bfloat16s[:0], end-start)
		for row := start; row < end; row++ {
			b.bfloat16s[row-start] = sourceValues[b.order[row]]
		}
		return b.builder.BuildBFloat16Bits(b.bfloat16s)
	case ColumnTypeFloat32Vector:
		sourceValues := batch.Float32Vectors[def.Name]
		values, err := b.gatherFloat32Dense(sourceValues, def.FixedWidthElements, start, end)
		if err != nil {
			return EncodedGranule{}, err
		}
		return b.builder.BuildFloat32Vector(values, end-start, def.FixedWidthElements)
	case ColumnTypeUint8Vector, ColumnTypeInt8Vector, ColumnTypeUint16Vector, ColumnTypeInt16Vector, ColumnTypeUint32Vector, ColumnTypeInt32Vector, ColumnTypeUint64Vector, ColumnTypeInt64Vector, ColumnTypeFloat16Vector, ColumnTypeBFloat16Vector, ColumnTypeFloat64Vector:
		elementWidth, ok := DenseFixedWidthVectorElementWidth(def.Type)
		if !ok {
			return EncodedGranule{}, fmt.Errorf("typedcolumn: unsupported dense vector type %s", def.Type)
		}
		values, err := b.gatherDenseFixedWidth(batch.DenseFixedWidthVectors[def.Name], def, start, end)
		if err != nil {
			return EncodedGranule{}, err
		}
		return b.builder.BuildDenseFixedWidth(values, end-start, def.FixedWidthElements, elementWidth)
	case ColumnTypeFixedBytes:
		values, err := b.gatherFixedBytes(batch.FixedBytesColumns[def.Name], def, start, end)
		if err != nil {
			return EncodedGranule{}, err
		}
		return b.builder.BuildFixedBytes(values, end-start, def.FixedWidthElements)
	case ColumnTypePackedBitVector, ColumnTypePackedUint2Vector, ColumnTypePackedUint4Vector:
		values, err := b.gatherPackedUint(batch.PackedUintColumns[def.Name], def, start, end)
		if err != nil {
			return EncodedGranule{}, err
		}
		return b.builder.BuildPackedUint(values, end-start, def.FixedWidthElements, def.BitsPerElement)
	case ColumnTypeUint32List:
		list, err := b.gatherUint32OffsetsList(batch.Uint32OffsetsLists[def.Name], start, end)
		if err != nil {
			return EncodedGranule{}, err
		}
		return b.builder.BuildUint32OffsetsList(list.Rows, list.Offsets, list.Values)
	case ColumnTypeBytes:
		bytesColumn, err := b.gatherBytesOffsets(batch.BytesColumns[def.Name], start, end)
		if err != nil {
			return EncodedGranule{}, err
		}
		return b.builder.BuildBytes(bytesColumn.Rows, bytesColumn.Offsets, bytesColumn.Values)
	case ColumnTypeAdjacencyList:
		if def.Encoding == EncodingRawUint32OffsetsList {
			list, err := b.gatherUint32OffsetsList(batch.Uint32OffsetsLists[def.Name], start, end)
			if err != nil {
				return EncodedGranule{}, err
			}
			return b.builder.BuildUint32OffsetsList(list.Rows, list.Offsets, list.Values)
		}
		sourceValues := batch.Uint32Vectors[def.Name]
		values, err := b.gatherUint32Dense(sourceValues, def.FixedWidthElements, start, end)
		if err != nil {
			return EncodedGranule{}, err
		}
		return b.builder.BuildUint32Dense(values, end-start, def.FixedWidthElements)
	}

	sourceValues := batch.Columns[def.Name]
	b.values64 = ensureInt64Len(b.values64[:0], end-start)
	for row := start; row < end; row++ {
		b.values64[row-start] = sourceValues[b.order[row]]
	}
	if def.Encoding == EncodingNullableInt64 {
		b.nulls = gatherOptionalBools(b.nulls[:0], batch.Nulls[def.Name], b.order, start, end)
		b.defaults = gatherOptionalBools(b.defaults[:0], batch.Defaults[def.Name], b.order, start, end)
		defaultValue, hasDefaultValue := batch.DefaultValues[def.Name]
		if err := validateNullableInt64DefaultValue(def, defaultValue, hasDefaultValue); err != nil {
			return EncodedGranule{}, err
		}
		for i, v := range b.values64 {
			if boolAt(b.nulls, i) || boolAt(b.defaults, i) {
				continue
			}
			if err := validateNullableCarrierValue(def.Type, def.Name, v); err != nil {
				return EncodedGranule{}, err
			}
		}
		return b.builder.BuildNullableInt64(b.values64, b.nulls, b.defaults, defaultValue)
	}
	switch def.Type {
	case ColumnTypeInt64:
		return b.builder.BuildInt64(b.values64)
	case ColumnTypeLowCardinalityCode:
		b.codes32 = ensureUint32Len(b.codes32[:0], len(b.values64))
		for i, v := range b.values64 {
			if v < 0 || v > math.MaxUint32 {
				return EncodedGranule{}, fmt.Errorf("typedcolumn: code value %d outside uint32 for %s", v, def.Name)
			}
			b.codes32[i] = uint32(v)
		}
		return b.builder.BuildUint32Codes(b.codes32, def.Cardinality)
	case ColumnTypeBool:
		b.bools = ensureBoolLen(b.bools[:0], len(b.values64))
		for i, v := range b.values64 {
			if v != 0 && v != 1 {
				return EncodedGranule{}, fmt.Errorf("typedcolumn: bool value %d outside 0/1 for %s", v, def.Name)
			}
			b.bools[i] = v == 1
		}
		return b.builder.BuildBool(b.bools)
	default:
		return EncodedGranule{}, fmt.Errorf("typedcolumn: unsupported column type %s", def.Type)
	}
}

func isInt64SortCarrier(t ColumnType) bool {
	switch t {
	case ColumnTypeInt64, ColumnTypeLowCardinalityCode, ColumnTypeBool:
		return true
	default:
		return false
	}
}

func gatherOptionalBools(dst []bool, source []bool, order []int, start int, end int) []bool {
	if len(source) == 0 {
		return nil
	}
	out := ensureBoolLen(dst, end-start)
	for row := start; row < end; row++ {
		out[row-start] = source[order[row]]
	}
	return out
}

func validateNullableInt64DefaultValue(def ColumnDefinition, defaultValue int64, ok bool) error {
	if !ok {
		return nil
	}
	return validateNullableCarrierValue(def.Type, def.Name, defaultValue)
}

func validateNullableDecodedCarrierValues(columnType ColumnType, values []int64) error {
	for _, value := range values {
		if err := validateNullableCarrierValue(columnType, "scan", value); err != nil {
			return err
		}
	}
	return nil
}

func validateNullableCarrierValue(columnType ColumnType, name string, value int64) error {
	switch columnType {
	case ColumnTypeLowCardinalityCode:
		if value < 0 || value > math.MaxUint32 {
			return fmt.Errorf("typedcolumn: code value %d outside uint32 for %s", value, name)
		}
	case ColumnTypeBool:
		if value != 0 && value != 1 {
			return fmt.Errorf("typedcolumn: bool value %d outside 0/1 for %s", value, name)
		}
	}
	return nil
}

func exclusiveInt64Upper(v int64) int64 {
	if v == math.MaxInt64 {
		return math.MaxInt64
	}
	return v + 1
}
