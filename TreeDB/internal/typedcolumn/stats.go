package typedcolumn

import (
	"fmt"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/crc"
)

const (
	columnPartStatsSectionMagic   = uint32(0x54435354) // TCST
	columnPartStatsSectionVersion = uint16(1)
	ColumnStatsEnvelopeVersion    = uint16(1)
	Int64StatsPayloadVersion      = uint16(1)
)

// ColumnStatsOperation is stored as the semantic operation string advertised by
// a stats envelope. The values intentionally match columnsemantics.Operation
// without importing that package into typedcolumn.
type ColumnStatsOperation string

const (
	ColumnStatsOpCountRows    ColumnStatsOperation = "aggregate.count_rows"
	ColumnStatsOpCountNonNull ColumnStatsOperation = "aggregate.count_non_null"
	ColumnStatsOpSum          ColumnStatsOperation = "aggregate.sum"
	ColumnStatsOpAvg          ColumnStatsOperation = "aggregate.avg"
	ColumnStatsOpMin          ColumnStatsOperation = "aggregate.min"
	ColumnStatsOpMax          ColumnStatsOperation = "aggregate.max"
	ColumnStatsOpStatsMinMax  ColumnStatsOperation = "stats.min_max"
	ColumnStatsOpStatsSum     ColumnStatsOperation = "stats.sum"
)

// ColumnStatsSelectionShape records the row-selection domains an envelope can
// answer without decoding values. Narrower/random selections must not use a
// payload unless a future type-specific stats format advertises that shape.
type ColumnStatsSelectionShape string

const (
	ColumnStatsSelectionAllRows   ColumnStatsSelectionShape = "all_rows"
	ColumnStatsSelectionFullBlock ColumnStatsSelectionShape = "full_block"
)

type ColumnStatsPayloadKind string

const (
	ColumnStatsPayloadInt64V1      ColumnStatsPayloadKind = "int64_count_sum_min_max_v1"
	ColumnStatsPayloadUnsupported  ColumnStatsPayloadKind = "unsupported"
	ColumnStatsPayloadFuturePrefix ColumnStatsPayloadKind = "future"
)

const (
	ColumnStatsReasonSupported            = "supported"
	ColumnStatsReasonUnsupportedPayload   = "stats_payload_unsupported"
	ColumnStatsReasonOperationUnsupported = "operation_unsupported"
	ColumnStatsReasonSelectionUnsupported = "selection_shape_unsupported"
	ColumnStatsReasonChecksumMismatch     = "checksum_mismatch"
	ColumnStatsReasonIdentityMismatch     = "identity_mismatch"
	ColumnStatsReasonRowCountMismatch     = "row_count_mismatch"
	ColumnStatsReasonNullDefaultMismatch  = "null_default_count_mismatch"
	ColumnStatsReasonVisibilityMismatch   = "visibility_count_mismatch"
	ColumnStatsReasonMinMaxMismatch       = "min_max_mismatch"
	ColumnStatsReasonSumOverflow          = "sum_overflow"
)

type ColumnStatsEnvelope struct {
	Version         uint16
	PartID          uint64
	ColumnName      string
	ColumnType      ColumnType
	Encoding        Encoding
	Compression     Compression
	Rows            int
	Blocks          int
	NullCount       int
	DefaultCount    int
	VisibleCount    int
	ValueCount      int
	PayloadKind     ColumnStatsPayloadKind
	Operations      []ColumnStatsOperation
	SelectionShapes []ColumnStatsSelectionShape
	PayloadLength   int
	PayloadChecksum uint32
}

type Int64BlockStats struct {
	Index        int
	FirstRow     int
	RowCount     int
	NullCount    int
	DefaultCount int
	VisibleCount int
	ValueCount   int
	Sum          int64
	SumValid     bool
	HasMinMax    bool
	Min          int64
	Max          int64
}

type Int64ColumnStats struct {
	Envelope     ColumnStatsEnvelope
	Count        int
	NullCount    int
	DefaultCount int
	VisibleCount int
	ValueCount   int
	Sum          int64
	SumValid     bool
	HasMinMax    bool
	Min          int64
	Max          int64
	Blocks       []Int64BlockStats
}

type ColumnPartStats struct {
	Version uint16
	PartID  uint64
	Rows    int
	Int64   map[string]Int64ColumnStats
}

func (s ColumnPartStats) Empty() bool {
	return len(s.Int64) == 0
}

func (s ColumnPartStats) Int64Column(name string) (Int64ColumnStats, bool) {
	if s.Int64 == nil {
		return Int64ColumnStats{}, false
	}
	stats, ok := s.Int64[name]
	return stats, ok
}

func (s Int64ColumnStats) Block(index int) (Int64BlockStats, bool) {
	if index < 0 || index >= len(s.Blocks) {
		return Int64BlockStats{}, false
	}
	block := s.Blocks[index]
	if block.Index != index {
		return Int64BlockStats{}, false
	}
	return block, true
}

func (e ColumnStatsEnvelope) SupportsOperation(op ColumnStatsOperation) bool {
	for _, advertised := range e.Operations {
		if advertised == op {
			return true
		}
	}
	return false
}

func (e ColumnStatsEnvelope) SupportsSelectionShape(shape ColumnStatsSelectionShape) bool {
	for _, advertised := range e.SelectionShapes {
		if advertised == shape {
			return true
		}
	}
	return false
}

func (s Int64ColumnStats) CanAnswer(op ColumnStatsOperation, shape ColumnStatsSelectionShape) (bool, string) {
	if s.Envelope.PayloadKind != ColumnStatsPayloadInt64V1 {
		return false, ColumnStatsReasonUnsupportedPayload
	}
	if !s.Envelope.SupportsOperation(op) {
		return false, ColumnStatsReasonOperationUnsupported
	}
	if !s.Envelope.SupportsSelectionShape(shape) {
		return false, ColumnStatsReasonSelectionUnsupported
	}
	if (op == ColumnStatsOpSum || op == ColumnStatsOpAvg || op == ColumnStatsOpStatsSum) && shape == ColumnStatsSelectionAllRows && !s.SumValid {
		return false, ColumnStatsReasonSumOverflow
	}
	return true, ColumnStatsReasonSupported
}

func (b Int64BlockStats) CanAnswer(op ColumnStatsOperation) (bool, string) {
	switch op {
	case ColumnStatsOpCountRows, ColumnStatsOpCountNonNull, ColumnStatsOpMin, ColumnStatsOpMax, ColumnStatsOpStatsMinMax:
		return true, ColumnStatsReasonSupported
	case ColumnStatsOpSum, ColumnStatsOpAvg, ColumnStatsOpStatsSum:
		if !b.SumValid {
			return false, ColumnStatsReasonSumOverflow
		}
		return true, ColumnStatsReasonSupported
	default:
		return false, ColumnStatsReasonOperationUnsupported
	}
}

func buildColumnPartStats(part *ColumnPart) (ColumnPartStats, error) {
	if part == nil {
		return ColumnPartStats{}, fmt.Errorf("typedcolumn: nil part")
	}
	out := ColumnPartStats{Version: columnPartStatsSectionVersion, PartID: part.Descriptor.PartID, Rows: part.Descriptor.RowCount}
	for _, columnDesc := range part.Descriptor.Columns {
		if !integerStatsPayloadColumnType(columnDesc.Type) {
			continue
		}
		column, ok := part.Columns[columnDesc.Name]
		if !ok {
			return ColumnPartStats{}, fmt.Errorf("typedcolumn: stats missing column %s", columnDesc.Name)
		}
		stats, ok, err := buildInt64ColumnStats(part.Descriptor, columnDesc, column)
		if err != nil {
			return ColumnPartStats{}, err
		}
		if !ok {
			continue
		}
		if out.Int64 == nil {
			out.Int64 = make(map[string]Int64ColumnStats)
		}
		out.Int64[columnDesc.Name] = stats
	}
	return out, nil
}

func buildInt64ColumnStats(desc ColumnPartDescriptor, columnDesc ColumnPartColumnDescriptor, column ColumnPartColumn) (Int64ColumnStats, bool, error) {
	if !columnStatsPartFullyVisible(desc) {
		return Int64ColumnStats{}, false, nil
	}
	if column.Definition.StatsDisabled || !integerStatsPayloadColumnType(column.Definition.Type) || column.Definition.Encoding == EncodingNullableInt64 {
		return Int64ColumnStats{}, false, nil
	}
	stats := Int64ColumnStats{
		Envelope: ColumnStatsEnvelope{
			Version:     ColumnStatsEnvelopeVersion,
			PartID:      desc.PartID,
			ColumnName:  columnDesc.Name,
			ColumnType:  columnDesc.Type,
			Encoding:    column.Definition.Encoding,
			Compression: column.Definition.Compression,
			Rows:        desc.RowCount,
			Blocks:      len(column.Blocks),
			PayloadKind: ColumnStatsPayloadInt64V1,
			Operations:  []ColumnStatsOperation{ColumnStatsOpCountRows, ColumnStatsOpCountNonNull, ColumnStatsOpMin, ColumnStatsOpMax, ColumnStatsOpStatsMinMax},
			SelectionShapes: []ColumnStatsSelectionShape{
				ColumnStatsSelectionFullBlock,
			},
		},
		Blocks:   make([]Int64BlockStats, 0, len(column.Blocks)),
		SumValid: true,
	}
	partHasMinMax := false
	partMin, partMax := int64(0), int64(0)
	var reader GranuleReader
	for i, block := range column.Blocks {
		if i >= len(columnDesc.Blocks) {
			return Int64ColumnStats{}, false, fmt.Errorf("typedcolumn: stats column %s block %d missing descriptor", columnDesc.Name, i)
		}
		if block.Descriptor != columnDesc.Blocks[i] {
			return Int64ColumnStats{}, false, fmt.Errorf("typedcolumn: stats column %s block %d descriptor mismatch", columnDesc.Name, i)
		}
		blockStats, err := buildInt64BlockStats(&reader, column.Definition.Type, i, block)
		if err != nil {
			return Int64ColumnStats{}, false, fmt.Errorf("typedcolumn: stats column %s block %d: %w", columnDesc.Name, i, err)
		}
		stats.Blocks = append(stats.Blocks, blockStats)
		stats.Count += blockStats.RowCount
		stats.NullCount += blockStats.NullCount
		stats.DefaultCount += blockStats.DefaultCount
		stats.VisibleCount += blockStats.VisibleCount
		stats.ValueCount += blockStats.ValueCount
		if blockStats.HasMinMax {
			partMin, partMax, partHasMinMax = updateOptionalMinMax(partMin, partMax, partHasMinMax, blockStats.Min)
			partMin, partMax, partHasMinMax = updateOptionalMinMax(partMin, partMax, partHasMinMax, blockStats.Max)
		}
		if stats.SumValid && blockStats.SumValid {
			updated, err := checkedInt64Add(stats.Sum, blockStats.Sum)
			if err != nil {
				stats.SumValid = false
				stats.Sum = 0
			} else {
				stats.Sum = updated
			}
		} else {
			stats.SumValid = false
			stats.Sum = 0
		}
	}
	if stats.Count != desc.RowCount {
		return Int64ColumnStats{}, false, fmt.Errorf("typedcolumn: stats column %s row count=%d want part rows=%d", columnDesc.Name, stats.Count, desc.RowCount)
	}
	stats.HasMinMax = partHasMinMax
	stats.Min = partMin
	stats.Max = partMax
	stats.Envelope.NullCount = stats.NullCount
	stats.Envelope.DefaultCount = stats.DefaultCount
	stats.Envelope.VisibleCount = stats.VisibleCount
	stats.Envelope.ValueCount = stats.ValueCount
	if anyInt64BlockSumValid(stats.Blocks) {
		stats.Envelope.Operations = append(stats.Envelope.Operations, ColumnStatsOpSum, ColumnStatsOpAvg, ColumnStatsOpStatsSum)
	}
	stats.Envelope.SelectionShapes = append(stats.Envelope.SelectionShapes, ColumnStatsSelectionAllRows)
	return stats, true, nil
}

func columnStatsPartFullyVisible(desc ColumnPartDescriptor) bool {
	if desc.VisibleRowCount != desc.RowCount {
		return false
	}
	for _, granule := range desc.Granules {
		if granule.VisibleRows != granule.RowCount || granule.DeletedRows != 0 {
			return false
		}
	}
	return true
}

func buildInt64BlockStats(reader *GranuleReader, columnType ColumnType, index int, block ColumnBlock) (Int64BlockStats, error) {
	g := block.Granule
	if g.Rows != block.Descriptor.RowCount {
		return Int64BlockStats{}, fmt.Errorf("granule rows=%d descriptor rows=%d", g.Rows, block.Descriptor.RowCount)
	}
	stats := Int64BlockStats{
		Index:        index,
		FirstRow:     block.Descriptor.FirstRow,
		RowCount:     block.Descriptor.RowCount,
		NullCount:    g.NullCount,
		DefaultCount: g.DefaultCount,
		VisibleCount: block.Descriptor.RowCount,
		ValueCount:   block.Descriptor.RowCount - g.NullCount - g.DefaultCount,
		SumValid:     true,
		HasMinMax:    g.HasMinMax,
		Min:          g.Min,
		Max:          g.Max,
	}
	if stats.ValueCount < 0 {
		return Int64BlockStats{}, fmt.Errorf("value count=%d from rows=%d nulls=%d defaults=%d", stats.ValueCount, stats.RowCount, g.NullCount, g.DefaultCount)
	}
	if g.NullCount != 0 || g.DefaultCount != 0 {
		return Int64BlockStats{}, fmt.Errorf("non-null int64-compatible integer stats for %s got null/default counts %d/%d", columnType, g.NullCount, g.DefaultCount)
	}
	if columnType == ColumnTypeInt64 {
		cursor, err := reader.Int64Cursor(g)
		if err != nil {
			return Int64BlockStats{}, err
		}
		for row := 0; row < g.Rows; row++ {
			value, err := cursor.Next()
			if err != nil {
				return Int64BlockStats{}, err
			}
			stats.addValueToSum(value)
		}
		if err := cursor.Finish(); err != nil {
			return Int64BlockStats{}, err
		}
		return stats, nil
	}

	values, err := reader.DecodeIntegerAsInt64Into(reader.values[:0], columnType, g)
	if err != nil {
		return Int64BlockStats{}, err
	}
	reader.values = values
	if len(values) != g.Rows {
		return Int64BlockStats{}, fmt.Errorf("decoded values=%d want rows=%d", len(values), g.Rows)
	}
	for _, value := range values {
		stats.addValueToSum(value)
	}
	return stats, nil
}

func (stats *Int64BlockStats) addValueToSum(value int64) {
	if !stats.SumValid {
		return
	}
	updated, err := checkedInt64Add(stats.Sum, value)
	if err != nil {
		stats.SumValid = false
		stats.Sum = 0
		return
	}
	stats.Sum = updated
}

func anyInt64BlockSumValid(blocks []Int64BlockStats) bool {
	for _, block := range blocks {
		if block.SumValid {
			return true
		}
	}
	return false
}

func encodeColumnPartStatsSection(stats ColumnPartStats) ([]byte, error) {
	if stats.Empty() {
		return nil, nil
	}
	var enc columnPartImageEncoder
	enc.u32(columnPartStatsSectionMagic)
	enc.u16(columnPartStatsSectionVersion)
	enc.u16(0)
	enc.u64(stats.PartID)
	enc.i64(int64(stats.Rows))
	names := make([]string, 0, len(stats.Int64))
	for name := range stats.Int64 {
		names = append(names, name)
	}
	sort.Strings(names)
	enc.u32(uint32(len(names)))
	for _, name := range names {
		columnStats := stats.Int64[name]
		payload, err := encodeInt64StatsPayload(columnStats)
		if err != nil {
			return nil, err
		}
		envelope := columnStats.Envelope
		envelope.PayloadLength = len(payload)
		envelope.PayloadChecksum = crc.Checksum(payload)
		if err := encodeColumnStatsEnvelope(&enc, envelope); err != nil {
			return nil, err
		}
		enc.buf = append(enc.buf, payload...)
	}
	return enc.bytes(), nil
}

func (i ColumnPartImage) ColumnStatsSection() (ColumnPartImageSection, bool, error) {
	sections := i.sectionsByKind(ColumnPartImageSectionColumnStats)
	if len(sections) == 0 {
		return ColumnPartImageSection{}, false, nil
	}
	if len(sections) != 1 {
		return ColumnPartImageSection{}, false, fmt.Errorf("typedcolumn: image has %d %s sections, want at most 1", len(sections), ColumnPartImageSectionColumnStats)
	}
	return sections[0], true, nil
}

func decodeColumnStatsSectionFromImage(image ColumnPartImage, desc ColumnPartDescriptor, columns map[string]ColumnPartColumn) (ColumnPartStats, error) {
	section, ok, err := image.ColumnStatsSection()
	if err != nil || !ok {
		return ColumnPartStats{}, err
	}
	stats, err := DecodeColumnPartStatsSection(image.sectionBytes(section))
	if err != nil {
		return ColumnPartStats{}, err
	}
	if err := ValidateColumnPartStats(stats, desc, columns); err != nil {
		return ColumnPartStats{}, err
	}
	return stats, nil
}

func DecodeColumnPartStatsSection(data []byte) (ColumnPartStats, error) {
	dec := columnPartImageDecoder{data: data}
	magic, err := dec.u32()
	if err != nil {
		return ColumnPartStats{}, err
	}
	if magic != columnPartStatsSectionMagic {
		return ColumnPartStats{}, fmt.Errorf("typedcolumn: bad column stats section magic=0x%08x", magic)
	}
	version, err := dec.u16()
	if err != nil {
		return ColumnPartStats{}, err
	}
	if version != columnPartStatsSectionVersion {
		return ColumnPartStats{}, fmt.Errorf("typedcolumn: unsupported column stats section version=%d", version)
	}
	reserved, err := dec.u16()
	if err != nil {
		return ColumnPartStats{}, err
	}
	if reserved != 0 {
		return ColumnPartStats{}, fmt.Errorf("typedcolumn: column stats section reserved=%d want 0", reserved)
	}
	partID, err := dec.u64()
	if err != nil {
		return ColumnPartStats{}, err
	}
	rows64, err := dec.i64()
	if err != nil {
		return ColumnPartStats{}, err
	}
	rows, err := nonNegativeInt64ToInt(rows64, "column stats rows")
	if err != nil {
		return ColumnPartStats{}, err
	}
	columnCount, err := dec.u32()
	if err != nil {
		return ColumnPartStats{}, err
	}
	columns, err := dec.boundedCount(columnCount, 96, "column stats entries")
	if err != nil {
		return ColumnPartStats{}, err
	}
	stats := ColumnPartStats{Version: version, PartID: partID, Rows: rows, Int64: make(map[string]Int64ColumnStats, columns)}
	for i := 0; i < columns; i++ {
		envelope, err := decodeColumnStatsEnvelope(&dec)
		if err != nil {
			return ColumnPartStats{}, err
		}
		payload, err := dec.bytes(envelope.PayloadLength)
		if err != nil {
			return ColumnPartStats{}, err
		}
		if got := crc.Checksum(payload); got != envelope.PayloadChecksum {
			return ColumnPartStats{}, fmt.Errorf("typedcolumn: column stats %s payload checksum=%08x want=%08x: %s", envelope.ColumnName, got, envelope.PayloadChecksum, ColumnStatsReasonChecksumMismatch)
		}
		switch envelope.PayloadKind {
		case ColumnStatsPayloadInt64V1:
			columnStats, err := decodeInt64StatsPayload(envelope, payload)
			if err != nil {
				return ColumnPartStats{}, err
			}
			if _, exists := stats.Int64[envelope.ColumnName]; exists {
				return ColumnPartStats{}, fmt.Errorf("typedcolumn: duplicate int64 stats for column %s", envelope.ColumnName)
			}
			stats.Int64[envelope.ColumnName] = columnStats
		default:
			return ColumnPartStats{}, fmt.Errorf("typedcolumn: unsupported column stats payload %q for column %s: %s", envelope.PayloadKind, envelope.ColumnName, ColumnStatsReasonUnsupportedPayload)
		}
	}
	if err := dec.finish(); err != nil {
		return ColumnPartStats{}, err
	}
	return stats, nil
}

func encodeColumnStatsEnvelope(enc *columnPartImageEncoder, envelope ColumnStatsEnvelope) error {
	if envelope.Version == 0 {
		envelope.Version = ColumnStatsEnvelopeVersion
	}
	if envelope.Version != ColumnStatsEnvelopeVersion {
		return fmt.Errorf("typedcolumn: unsupported column stats envelope version=%d", envelope.Version)
	}
	if envelope.ColumnName == "" {
		return fmt.Errorf("typedcolumn: column stats envelope requires column name")
	}
	columnType, err := columnTypeCode(envelope.ColumnType)
	if err != nil {
		return err
	}
	enc.u16(envelope.Version)
	enc.u16(0)
	enc.u64(envelope.PartID)
	enc.str(envelope.ColumnName)
	enc.u16(columnType)
	enc.u16(uint16(envelope.Encoding))
	enc.u16(uint16(envelope.Compression))
	enc.u16(0)
	enc.i64(int64(envelope.Rows))
	enc.i64(int64(envelope.Blocks))
	enc.i64(int64(envelope.NullCount))
	enc.i64(int64(envelope.DefaultCount))
	enc.i64(int64(envelope.VisibleCount))
	enc.i64(int64(envelope.ValueCount))
	enc.str(string(envelope.PayloadKind))
	operations := make([]string, len(envelope.Operations))
	for i, op := range envelope.Operations {
		operations[i] = string(op)
	}
	enc.stringSlice(operations)
	shapes := make([]string, len(envelope.SelectionShapes))
	for i, shape := range envelope.SelectionShapes {
		shapes[i] = string(shape)
	}
	enc.stringSlice(shapes)
	enc.i64(int64(envelope.PayloadLength))
	enc.u32(envelope.PayloadChecksum)
	enc.u32(0)
	return nil
}

func decodeColumnStatsEnvelope(dec *columnPartImageDecoder) (ColumnStatsEnvelope, error) {
	version, err := dec.u16()
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	if version != ColumnStatsEnvelopeVersion {
		return ColumnStatsEnvelope{}, fmt.Errorf("typedcolumn: unsupported column stats envelope version=%d", version)
	}
	reserved, err := dec.u16()
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	if reserved != 0 {
		return ColumnStatsEnvelope{}, fmt.Errorf("typedcolumn: column stats envelope reserved=%d want 0", reserved)
	}
	partID, err := dec.u64()
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	columnName, err := dec.str()
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	columnTypeCode, err := dec.u16()
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	columnType, err := columnTypeFromCode(columnTypeCode)
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	encodingCode, err := dec.u16()
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	encoding, err := decodeStatsEncoding(encodingCode)
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	compressionCode, err := dec.u16()
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	compression, err := decodeStatsCompression(compressionCode)
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	reserved, err = dec.u16()
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	if reserved != 0 {
		return ColumnStatsEnvelope{}, fmt.Errorf("typedcolumn: column stats envelope encoding reserved=%d want 0", reserved)
	}
	rows, err := decodeStatsInt(dec, "column stats rows")
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	blocks, err := decodeStatsInt(dec, "column stats blocks")
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	nullCount, err := decodeStatsInt(dec, "column stats null count")
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	defaultCount, err := decodeStatsInt(dec, "column stats default count")
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	visibleCount, err := decodeStatsInt(dec, "column stats visible count")
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	valueCount, err := decodeStatsInt(dec, "column stats value count")
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	payloadKind, err := dec.str()
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	operationStrings, err := dec.stringSlice()
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	operations := make([]ColumnStatsOperation, len(operationStrings))
	for i, op := range operationStrings {
		operations[i] = ColumnStatsOperation(op)
	}
	shapeStrings, err := dec.stringSlice()
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	shapes := make([]ColumnStatsSelectionShape, len(shapeStrings))
	for i, shape := range shapeStrings {
		shapes[i] = ColumnStatsSelectionShape(shape)
	}
	payloadLength, err := decodeStatsInt(dec, "column stats payload length")
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	payloadChecksum, err := dec.u32()
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	reserved32, err := dec.u32()
	if err != nil {
		return ColumnStatsEnvelope{}, err
	}
	if reserved32 != 0 {
		return ColumnStatsEnvelope{}, fmt.Errorf("typedcolumn: column stats envelope trailer reserved=%d want 0", reserved32)
	}
	return ColumnStatsEnvelope{
		Version:         version,
		PartID:          partID,
		ColumnName:      columnName,
		ColumnType:      columnType,
		Encoding:        encoding,
		Compression:     compression,
		Rows:            rows,
		Blocks:          blocks,
		NullCount:       nullCount,
		DefaultCount:    defaultCount,
		VisibleCount:    visibleCount,
		ValueCount:      valueCount,
		PayloadKind:     ColumnStatsPayloadKind(payloadKind),
		Operations:      operations,
		SelectionShapes: shapes,
		PayloadLength:   payloadLength,
		PayloadChecksum: payloadChecksum,
	}, nil
}

func decodeStatsEncoding(code uint16) (Encoding, error) {
	if code > 0xff {
		return 0, fmt.Errorf("typedcolumn: column stats envelope encoding=%d exceeds uint8", code)
	}
	encoding := Encoding(code)
	switch encoding {
	case EncodingRawInt64, EncodingDeltaVarint, EncodingDoubleDeltaVarint, EncodingNullableInt64, EncodingBoolBitpackRLE, EncodingLowCardinalityUint32, EncodingRawFloat32Vector, EncodingRawUint32Dense, EncodingRawFloat32, EncodingRawFloat64, EncodingRawUint32OffsetsList, EncodingRawBytesOffsets, EncodingRawInt8, EncodingRawUint8, EncodingRawInt16, EncodingRawUint16, EncodingRawInt32, EncodingRawUint32, EncodingRawUint64, EncodingRawFloat16, EncodingRawBFloat16, EncodingRawUint8Vector, EncodingRawInt8Vector, EncodingRawUint16Vector, EncodingRawInt16Vector, EncodingRawUint32Vector, EncodingRawInt32Vector, EncodingRawUint64Vector, EncodingRawInt64Vector, EncodingRawFloat16Vector, EncodingRawBFloat16Vector, EncodingRawFloat64Vector:
		return encoding, nil
	default:
		return 0, fmt.Errorf("typedcolumn: unknown column stats envelope encoding=%d", code)
	}
}

func decodeStatsCompression(code uint16) (Compression, error) {
	if code > 0xff {
		return 0, fmt.Errorf("typedcolumn: column stats envelope compression=%d exceeds uint8", code)
	}
	compression := Compression(code)
	switch compression {
	case CompressionNone, CompressionSnappy, CompressionLZ4, CompressionZSTD, CompressionZSTDDict:
		return compression, nil
	default:
		return 0, fmt.Errorf("typedcolumn: unknown column stats envelope compression=%d", code)
	}
}

func encodeInt64StatsPayload(stats Int64ColumnStats) ([]byte, error) {
	var enc columnPartImageEncoder
	enc.u16(Int64StatsPayloadVersion)
	enc.u16(0)
	enc.i64(int64(stats.Count))
	enc.i64(int64(stats.NullCount))
	enc.i64(int64(stats.DefaultCount))
	enc.i64(int64(stats.VisibleCount))
	enc.i64(int64(stats.ValueCount))
	enc.boolean(stats.SumValid)
	enc.i64(stats.Sum)
	enc.boolean(stats.HasMinMax)
	enc.i64(stats.Min)
	enc.i64(stats.Max)
	enc.u32(uint32(len(stats.Blocks)))
	for _, block := range stats.Blocks {
		enc.i64(int64(block.Index))
		enc.i64(int64(block.FirstRow))
		enc.i64(int64(block.RowCount))
		enc.i64(int64(block.NullCount))
		enc.i64(int64(block.DefaultCount))
		enc.i64(int64(block.VisibleCount))
		enc.i64(int64(block.ValueCount))
		enc.boolean(block.SumValid)
		enc.i64(block.Sum)
		enc.boolean(block.HasMinMax)
		enc.i64(block.Min)
		enc.i64(block.Max)
	}
	return enc.bytes(), nil
}

func decodeInt64StatsPayload(envelope ColumnStatsEnvelope, payload []byte) (Int64ColumnStats, error) {
	dec := columnPartImageDecoder{data: payload}
	version, err := dec.u16()
	if err != nil {
		return Int64ColumnStats{}, err
	}
	if version != Int64StatsPayloadVersion {
		return Int64ColumnStats{}, fmt.Errorf("typedcolumn: unsupported int64 stats payload version=%d", version)
	}
	reserved, err := dec.u16()
	if err != nil {
		return Int64ColumnStats{}, err
	}
	if reserved != 0 {
		return Int64ColumnStats{}, fmt.Errorf("typedcolumn: int64 stats reserved=%d want 0", reserved)
	}
	count, err := decodeStatsInt(&dec, "int64 stats count")
	if err != nil {
		return Int64ColumnStats{}, err
	}
	nullCount, err := decodeStatsInt(&dec, "int64 stats null count")
	if err != nil {
		return Int64ColumnStats{}, err
	}
	defaultCount, err := decodeStatsInt(&dec, "int64 stats default count")
	if err != nil {
		return Int64ColumnStats{}, err
	}
	visibleCount, err := decodeStatsInt(&dec, "int64 stats visible count")
	if err != nil {
		return Int64ColumnStats{}, err
	}
	valueCount, err := decodeStatsInt(&dec, "int64 stats value count")
	if err != nil {
		return Int64ColumnStats{}, err
	}
	sumValid, err := dec.boolean()
	if err != nil {
		return Int64ColumnStats{}, err
	}
	sum, err := dec.i64()
	if err != nil {
		return Int64ColumnStats{}, err
	}
	hasMinMax, err := dec.boolean()
	if err != nil {
		return Int64ColumnStats{}, err
	}
	minValue, err := dec.i64()
	if err != nil {
		return Int64ColumnStats{}, err
	}
	maxValue, err := dec.i64()
	if err != nil {
		return Int64ColumnStats{}, err
	}
	blockCount, err := dec.u32()
	if err != nil {
		return Int64ColumnStats{}, err
	}
	blocks, err := dec.boundedCount(blockCount, 84, "int64 stats blocks")
	if err != nil {
		return Int64ColumnStats{}, err
	}
	stats := Int64ColumnStats{
		Envelope:     envelope,
		Count:        count,
		NullCount:    nullCount,
		DefaultCount: defaultCount,
		VisibleCount: visibleCount,
		ValueCount:   valueCount,
		Sum:          sum,
		SumValid:     sumValid,
		HasMinMax:    hasMinMax,
		Min:          minValue,
		Max:          maxValue,
		Blocks:       make([]Int64BlockStats, 0, blocks),
	}
	for i := 0; i < blocks; i++ {
		block, err := decodeInt64BlockStats(&dec)
		if err != nil {
			return Int64ColumnStats{}, err
		}
		if block.Index != i {
			return Int64ColumnStats{}, fmt.Errorf("typedcolumn: int64 stats block index=%d want %d", block.Index, i)
		}
		stats.Blocks = append(stats.Blocks, block)
	}
	if err := dec.finish(); err != nil {
		return Int64ColumnStats{}, err
	}
	return stats, nil
}

func decodeInt64BlockStats(dec *columnPartImageDecoder) (Int64BlockStats, error) {
	index, err := decodeStatsInt(dec, "int64 stats block index")
	if err != nil {
		return Int64BlockStats{}, err
	}
	firstRow, err := decodeStatsInt(dec, "int64 stats block first row")
	if err != nil {
		return Int64BlockStats{}, err
	}
	rowCount, err := decodeStatsInt(dec, "int64 stats block row count")
	if err != nil {
		return Int64BlockStats{}, err
	}
	nullCount, err := decodeStatsInt(dec, "int64 stats block null count")
	if err != nil {
		return Int64BlockStats{}, err
	}
	defaultCount, err := decodeStatsInt(dec, "int64 stats block default count")
	if err != nil {
		return Int64BlockStats{}, err
	}
	visibleCount, err := decodeStatsInt(dec, "int64 stats block visible count")
	if err != nil {
		return Int64BlockStats{}, err
	}
	valueCount, err := decodeStatsInt(dec, "int64 stats block value count")
	if err != nil {
		return Int64BlockStats{}, err
	}
	sumValid, err := dec.boolean()
	if err != nil {
		return Int64BlockStats{}, err
	}
	sum, err := dec.i64()
	if err != nil {
		return Int64BlockStats{}, err
	}
	hasMinMax, err := dec.boolean()
	if err != nil {
		return Int64BlockStats{}, err
	}
	minValue, err := dec.i64()
	if err != nil {
		return Int64BlockStats{}, err
	}
	maxValue, err := dec.i64()
	if err != nil {
		return Int64BlockStats{}, err
	}
	return Int64BlockStats{Index: index, FirstRow: firstRow, RowCount: rowCount, NullCount: nullCount, DefaultCount: defaultCount, VisibleCount: visibleCount, ValueCount: valueCount, Sum: sum, SumValid: sumValid, HasMinMax: hasMinMax, Min: minValue, Max: maxValue}, nil
}

func (d *columnPartImageDecoder) bytes(n int) ([]byte, error) {
	if err := d.require(n); err != nil {
		return nil, err
	}
	out := d.data[d.offset : d.offset+n]
	d.offset += n
	return out, nil
}

func decodeStatsInt(dec *columnPartImageDecoder, field string) (int, error) {
	v, err := dec.i64()
	if err != nil {
		return 0, err
	}
	return nonNegativeInt64ToInt(v, field)
}

func ValidateColumnPartStats(stats ColumnPartStats, desc ColumnPartDescriptor, columns map[string]ColumnPartColumn) error {
	if stats.Version != columnPartStatsSectionVersion {
		return fmt.Errorf("typedcolumn: column stats version=%d want %d", stats.Version, columnPartStatsSectionVersion)
	}
	if stats.PartID != desc.PartID {
		return fmt.Errorf("typedcolumn: column stats part_id=%d want %d: %s", stats.PartID, desc.PartID, ColumnStatsReasonIdentityMismatch)
	}
	if stats.Rows != desc.RowCount {
		return fmt.Errorf("typedcolumn: column stats rows=%d want %d: %s", stats.Rows, desc.RowCount, ColumnStatsReasonRowCountMismatch)
	}
	columnDescs := make(map[string]ColumnPartColumnDescriptor, len(desc.Columns))
	for _, columnDesc := range desc.Columns {
		columnDescs[columnDesc.Name] = columnDesc
	}
	for name, int64Stats := range stats.Int64 {
		columnDesc, ok := columnDescs[name]
		if !ok {
			return fmt.Errorf("typedcolumn: column stats %s missing descriptor: %s", name, ColumnStatsReasonIdentityMismatch)
		}
		column, ok := columns[name]
		if !ok {
			return fmt.Errorf("typedcolumn: column stats %s missing column: %s", name, ColumnStatsReasonIdentityMismatch)
		}
		if err := ValidateInt64ColumnStats(int64Stats, desc, columnDesc, column); err != nil {
			return err
		}
	}
	return nil
}

func ValidateInt64ColumnStats(stats Int64ColumnStats, desc ColumnPartDescriptor, columnDesc ColumnPartColumnDescriptor, column ColumnPartColumn) error {
	envelope := stats.Envelope
	if envelope.Version != ColumnStatsEnvelopeVersion {
		return fmt.Errorf("typedcolumn: column stats %s envelope version=%d want %d", envelope.ColumnName, envelope.Version, ColumnStatsEnvelopeVersion)
	}
	if envelope.PartID != desc.PartID || envelope.PartID != stats.Envelope.PartID {
		return fmt.Errorf("typedcolumn: column stats %s part_id=%d want %d: %s", envelope.ColumnName, envelope.PartID, desc.PartID, ColumnStatsReasonIdentityMismatch)
	}
	if envelope.ColumnName != columnDesc.Name || envelope.ColumnName != column.Definition.Name {
		return fmt.Errorf("typedcolumn: column stats name=%q descriptor=%q definition=%q: %s", envelope.ColumnName, columnDesc.Name, column.Definition.Name, ColumnStatsReasonIdentityMismatch)
	}
	if !integerStatsPayloadColumnType(envelope.ColumnType) || !integerStatsPayloadColumnType(columnDesc.Type) || !integerStatsPayloadColumnType(column.Definition.Type) || envelope.PayloadKind != ColumnStatsPayloadInt64V1 {
		return fmt.Errorf("typedcolumn: column stats %s int64-compatible payload cannot apply to type envelope=%s descriptor=%s definition=%s payload=%s: %s", envelope.ColumnName, envelope.ColumnType, columnDesc.Type, column.Definition.Type, envelope.PayloadKind, ColumnStatsReasonUnsupportedPayload)
	}
	if envelope.ColumnType != columnDesc.Type || envelope.ColumnType != column.Definition.Type {
		return fmt.Errorf("typedcolumn: column stats %s type identity envelope=%s descriptor=%s definition=%s: %s", envelope.ColumnName, envelope.ColumnType, columnDesc.Type, column.Definition.Type, ColumnStatsReasonIdentityMismatch)
	}
	if envelope.Encoding != column.Definition.Encoding || envelope.Compression != column.Definition.Compression {
		return fmt.Errorf("typedcolumn: column stats %s encoding/compression=%s/%s want %s/%s: %s", envelope.ColumnName, envelope.Encoding, envelope.Compression, column.Definition.Encoding, column.Definition.Compression, ColumnStatsReasonIdentityMismatch)
	}
	if envelope.Rows != desc.RowCount || stats.Count != desc.RowCount {
		return fmt.Errorf("typedcolumn: column stats %s rows envelope=%d count=%d want %d: %s", envelope.ColumnName, envelope.Rows, stats.Count, desc.RowCount, ColumnStatsReasonRowCountMismatch)
	}
	if envelope.Blocks != len(column.Blocks) || len(stats.Blocks) != len(column.Blocks) || len(columnDesc.Blocks) != len(column.Blocks) {
		return fmt.Errorf("typedcolumn: column stats %s blocks envelope=%d payload=%d descriptor=%d column=%d: %s", envelope.ColumnName, envelope.Blocks, len(stats.Blocks), len(columnDesc.Blocks), len(column.Blocks), ColumnStatsReasonRowCountMismatch)
	}
	if envelope.NullCount != stats.NullCount || envelope.DefaultCount != stats.DefaultCount || envelope.ValueCount != stats.ValueCount || envelope.VisibleCount != stats.VisibleCount {
		return fmt.Errorf("typedcolumn: column stats %s envelope count mismatch null/default/visible/value: %s", envelope.ColumnName, ColumnStatsReasonNullDefaultMismatch)
	}
	if stats.NullCount != 0 || stats.DefaultCount != 0 {
		return fmt.Errorf("typedcolumn: column stats %s null/default counts=%d/%d want 0/0: %s", envelope.ColumnName, stats.NullCount, stats.DefaultCount, ColumnStatsReasonNullDefaultMismatch)
	}
	if stats.VisibleCount != desc.VisibleRowCount || stats.ValueCount != desc.RowCount {
		return fmt.Errorf("typedcolumn: column stats %s visible/value=%d/%d want %d/%d: %s", envelope.ColumnName, stats.VisibleCount, stats.ValueCount, desc.VisibleRowCount, desc.RowCount, ColumnStatsReasonVisibilityMismatch)
	}
	if stats.HasMinMax && stats.Min > stats.Max {
		return fmt.Errorf("typedcolumn: column stats %s min=%d max=%d: %s", envelope.ColumnName, stats.Min, stats.Max, ColumnStatsReasonMinMaxMismatch)
	}
	rowTotal := 0
	nullTotal := 0
	defaultTotal := 0
	visibleTotal := 0
	valueTotal := 0
	partMin, partMax, partHasMinMax := int64(0), int64(0), false
	partSum, partSumValid := int64(0), true
	for i, blockStats := range stats.Blocks {
		block := column.Blocks[i]
		if blockStats.Index != i || blockStats.FirstRow != block.Descriptor.FirstRow || blockStats.RowCount != block.Descriptor.RowCount {
			return fmt.Errorf("typedcolumn: column stats %s block %d row identity mismatch stats=(%d,%d,%d) descriptor=(%d,%d): %s", envelope.ColumnName, i, blockStats.Index, blockStats.FirstRow, blockStats.RowCount, block.Descriptor.FirstRow, block.Descriptor.RowCount, ColumnStatsReasonRowCountMismatch)
		}
		if blockStats.NullCount != block.Granule.NullCount || blockStats.DefaultCount != block.Granule.DefaultCount {
			return fmt.Errorf("typedcolumn: column stats %s block %d null/default=%d/%d want %d/%d: %s", envelope.ColumnName, i, blockStats.NullCount, blockStats.DefaultCount, block.Granule.NullCount, block.Granule.DefaultCount, ColumnStatsReasonNullDefaultMismatch)
		}
		if blockStats.VisibleCount != blockStats.RowCount {
			return fmt.Errorf("typedcolumn: column stats %s block %d visible=%d want rows=%d: %s", envelope.ColumnName, i, blockStats.VisibleCount, blockStats.RowCount, ColumnStatsReasonVisibilityMismatch)
		}
		if blockStats.ValueCount != blockStats.RowCount-blockStats.NullCount-blockStats.DefaultCount {
			return fmt.Errorf("typedcolumn: column stats %s block %d value_count=%d rows/null/default=%d/%d/%d: %s", envelope.ColumnName, i, blockStats.ValueCount, blockStats.RowCount, blockStats.NullCount, blockStats.DefaultCount, ColumnStatsReasonNullDefaultMismatch)
		}
		if blockStats.HasMinMax != block.Granule.HasMinMax || (blockStats.HasMinMax && (blockStats.Min != block.Granule.Min || blockStats.Max != block.Granule.Max || blockStats.Min > blockStats.Max)) {
			return fmt.Errorf("typedcolumn: column stats %s block %d min/max mismatch stats=(has=%v min=%d max=%d) granule=(has=%v min=%d max=%d): %s", envelope.ColumnName, i, blockStats.HasMinMax, blockStats.Min, blockStats.Max, block.Granule.HasMinMax, block.Granule.Min, block.Granule.Max, ColumnStatsReasonMinMaxMismatch)
		}
		rowTotal += blockStats.RowCount
		nullTotal += blockStats.NullCount
		defaultTotal += blockStats.DefaultCount
		visibleTotal += blockStats.VisibleCount
		valueTotal += blockStats.ValueCount
		if blockStats.HasMinMax {
			partMin, partMax, partHasMinMax = updateOptionalMinMax(partMin, partMax, partHasMinMax, blockStats.Min)
			partMin, partMax, partHasMinMax = updateOptionalMinMax(partMin, partMax, partHasMinMax, blockStats.Max)
		}
		if partSumValid && blockStats.SumValid {
			updated, err := checkedInt64Add(partSum, blockStats.Sum)
			if err != nil {
				partSumValid = false
				partSum = 0
			} else {
				partSum = updated
			}
		} else {
			partSumValid = false
			partSum = 0
		}
	}
	if rowTotal != desc.RowCount || nullTotal != stats.NullCount || defaultTotal != stats.DefaultCount || visibleTotal != stats.VisibleCount || valueTotal != stats.ValueCount {
		return fmt.Errorf("typedcolumn: column stats %s totals rows/null/default/visible/value=%d/%d/%d/%d/%d want %d/%d/%d/%d/%d: %s", envelope.ColumnName, rowTotal, nullTotal, defaultTotal, visibleTotal, valueTotal, desc.RowCount, stats.NullCount, stats.DefaultCount, stats.VisibleCount, stats.ValueCount, ColumnStatsReasonRowCountMismatch)
	}
	if stats.HasMinMax != partHasMinMax || (stats.HasMinMax && (stats.Min != partMin || stats.Max != partMax)) {
		return fmt.Errorf("typedcolumn: column stats %s part min/max mismatch: %s", envelope.ColumnName, ColumnStatsReasonMinMaxMismatch)
	}
	if stats.SumValid != partSumValid || (stats.SumValid && stats.Sum != partSum) {
		return fmt.Errorf("typedcolumn: column stats %s part sum mismatch: %s", envelope.ColumnName, ColumnStatsReasonSumOverflow)
	}
	return nil
}

func cloneColumnPartStats(stats ColumnPartStats) ColumnPartStats {
	out := stats
	if stats.Int64 != nil {
		out.Int64 = make(map[string]Int64ColumnStats, len(stats.Int64))
		for name, columnStats := range stats.Int64 {
			out.Int64[name] = cloneInt64ColumnStats(columnStats)
		}
	}
	return out
}

func cloneInt64ColumnStats(stats Int64ColumnStats) Int64ColumnStats {
	out := stats
	out.Envelope.Operations = append([]ColumnStatsOperation(nil), stats.Envelope.Operations...)
	out.Envelope.SelectionShapes = append([]ColumnStatsSelectionShape(nil), stats.Envelope.SelectionShapes...)
	out.Blocks = append([]Int64BlockStats(nil), stats.Blocks...)
	return out
}
