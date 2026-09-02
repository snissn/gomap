package typedcolumn

import (
	"fmt"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/crc"
)

const (
	columnPartPruningSectionMagic   = uint32(0x54435052) // TCPR
	columnPartPruningSectionVersion = uint16(1)
	ColumnPruningEnvelopeVersion    = uint16(1)
	Int64PruningPayloadVersion      = uint16(1)
)

// ColumnPruningOperation is stored as the semantic predicate operation string
// advertised by a pruning/index envelope. The values intentionally match
// columnsemantics.Operation without importing that package into typedcolumn.
type ColumnPruningOperation string

const (
	ColumnPruningOpEquality     ColumnPruningOperation = "pruning.equality"
	ColumnPruningOpOrderedRange ColumnPruningOperation = "pruning.ordered_range"
)

type ColumnPruningPayloadKind string

const (
	ColumnPruningPayloadInt64ValueRowsV1 ColumnPruningPayloadKind = "int64_value_rows_v1"
	ColumnPruningPayloadUnsupported      ColumnPruningPayloadKind = "unsupported"
)

const (
	ColumnPruningReasonSupported            = "supported"
	ColumnPruningReasonMissingMetadata      = "pruning_metadata_missing"
	ColumnPruningReasonUnsupportedPayload   = "pruning_payload_unsupported"
	ColumnPruningReasonOperationUnsupported = "pruning_operation_unsupported"
	ColumnPruningReasonChecksumMismatch     = "pruning_checksum_mismatch"
	ColumnPruningReasonIdentityMismatch     = "pruning_identity_mismatch"
	ColumnPruningReasonRowCountMismatch     = "pruning_row_count_mismatch"
	ColumnPruningReasonNullDefaultMismatch  = "pruning_null_default_mismatch"
	ColumnPruningReasonEntryOrderMismatch   = "pruning_entry_order_mismatch"
	ColumnPruningReasonEntryRowMismatch     = "pruning_entry_row_mismatch"
	ColumnPruningReasonMinMaxMismatch       = "pruning_min_max_mismatch"
	ColumnPruningReasonSumOverflow          = "pruning_sum_overflow"
)

type ColumnPruningEnvelope struct {
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
	PayloadKind     ColumnPruningPayloadKind
	Operations      []ColumnPruningOperation
	PayloadLength   int
	PayloadChecksum uint32
}

type Int64PruningBlock struct {
	Index     int
	FirstRow  int
	RowCount  int
	HasMinMax bool
	Min       int64
	Max       int64
}

type Int64PruningEntry struct {
	Value int64
	Row   int
}

type Int64ValueRowIndex struct {
	Envelope     ColumnPruningEnvelope
	Rows         int
	NullCount    int
	DefaultCount int
	Blocks       []Int64PruningBlock
	Entries      []Int64PruningEntry
}

type ColumnPartPruning struct {
	Version uint16
	PartID  uint64
	Rows    int
	Int64   map[string]Int64ValueRowIndex
}

func (p ColumnPartPruning) Empty() bool { return len(p.Int64) == 0 }

func (p ColumnPartPruning) Int64Column(name string) (Int64ValueRowIndex, bool) {
	if p.Int64 == nil {
		return Int64ValueRowIndex{}, false
	}
	index, ok := p.Int64[name]
	return index, ok
}

func (e ColumnPruningEnvelope) SupportsOperation(op ColumnPruningOperation) bool {
	for _, advertised := range e.Operations {
		if advertised == op {
			return true
		}
	}
	return false
}

func (idx Int64ValueRowIndex) CanPlan(op ColumnPruningOperation) (bool, string) {
	if idx.Envelope.PayloadKind != ColumnPruningPayloadInt64ValueRowsV1 {
		return false, ColumnPruningReasonUnsupportedPayload
	}
	if !idx.Envelope.SupportsOperation(op) {
		return false, ColumnPruningReasonOperationUnsupported
	}
	return true, ColumnPruningReasonSupported
}

type Int64PruningPredicateKind string

const (
	Int64PruningPredicateAll   Int64PruningPredicateKind = "all"
	Int64PruningPredicateEqual Int64PruningPredicateKind = "equal"
	Int64PruningPredicateRange Int64PruningPredicateKind = "range"
)

type Int64PruningPredicate struct {
	Kind  Int64PruningPredicateKind
	Value int64
	Low   int64
	High  int64
}

type ColumnPruningBlockCandidate struct {
	BlockIndex     int
	FirstRow       int
	RowCount       int
	Selection      RowSelection
	NeedsPredicate bool
	Exact          bool
	ExactCount     int64
	ExactSum       int64
}

type ColumnPruningCandidatePlan struct {
	Rows              int
	Blocks            []ColumnPruningBlockCandidate
	CandidateRows     int
	PrunedRows        int
	CandidateBlocks   int
	PrunedBlocks      int
	FalsePositiveRows int
	Exact             bool
	ExactCount        int64
	ExactSum          int64
	Reason            string
}

// PlanInt64Predicate turns a durable int64 value-row index into block-local
// RowSelection candidates. The returned selections are immutable and own their
// slice storage; callers may retain them for a prepared/session lifetime.
func (idx Int64ValueRowIndex) PlanInt64Predicate(pred Int64PruningPredicate) (ColumnPruningCandidatePlan, error) {
	if err := validateInt64PruningPredicate(pred); err != nil {
		return ColumnPruningCandidatePlan{}, err
	}
	op := ColumnPruningOpOrderedRange
	if pred.Kind == Int64PruningPredicateEqual {
		op = ColumnPruningOpEquality
	}
	if pred.Kind != Int64PruningPredicateAll {
		if ok, reason := idx.CanPlan(op); !ok {
			return ColumnPruningCandidatePlan{Rows: idx.Rows, Reason: reason}, nil
		}
	} else {
		return idx.planAllInt64Predicate()
	}
	plan := ColumnPruningCandidatePlan{Rows: idx.Rows, Blocks: make([]ColumnPruningBlockCandidate, len(idx.Blocks)), Exact: true}
	rowsByBlock := make([][]int, len(idx.Blocks))
	exactCountByBlock := make([]int64, len(idx.Blocks))
	exactSumByBlock := make([]int64, len(idx.Blocks))
	exactSumOK := true
	exactSumOKByBlock := make([]bool, len(idx.Blocks))
	for i := range exactSumOKByBlock {
		exactSumOKByBlock[i] = true
	}
	entries := idx.entriesForPredicate(pred)
	for _, entry := range entries {
		blockIndex := idx.blockIndexForRow(entry.Row)
		if blockIndex < 0 {
			return ColumnPruningCandidatePlan{}, fmt.Errorf("typedcolumn: pruning entry row=%d outside block map: %s", entry.Row, ColumnPruningReasonEntryRowMismatch)
		}
		local := entry.Row - idx.Blocks[blockIndex].FirstRow
		rowsByBlock[blockIndex] = append(rowsByBlock[blockIndex], local)
		exactCountByBlock[blockIndex]++
		if exactSumOKByBlock[blockIndex] {
			updatedBlock, err := checkedInt64Add(exactSumByBlock[blockIndex], entry.Value)
			if err != nil {
				exactSumOKByBlock[blockIndex] = false
				exactSumByBlock[blockIndex] = 0
				exactSumOK = false
				plan.Exact = false
				plan.ExactSum = 0
			} else {
				exactSumByBlock[blockIndex] = updatedBlock
			}
		}
		if exactSumOK {
			updated, err := checkedInt64Add(plan.ExactSum, entry.Value)
			if err != nil {
				exactSumOK = false
				plan.Exact = false
				plan.ExactSum = 0
			} else {
				plan.ExactSum = updated
			}
		}
		plan.ExactCount++
	}
	for i, block := range idx.Blocks {
		rows := rowsByBlock[i]
		sort.Ints(rows)
		selection, err := pruningSelectionFromSortedRows(block.RowCount, rows)
		if err != nil {
			return ColumnPruningCandidatePlan{}, fmt.Errorf("typedcolumn: pruning block %d selection: %w", i, err)
		}
		candidate := ColumnPruningBlockCandidate{BlockIndex: block.Index, FirstRow: block.FirstRow, RowCount: block.RowCount, Selection: selection, Exact: exactSumOKByBlock[i], ExactCount: exactCountByBlock[i], ExactSum: exactSumByBlock[i]}
		plan.Blocks[i] = candidate
		if selection.IsEmpty() {
			plan.PrunedBlocks++
			plan.PrunedRows += block.RowCount
			continue
		}
		plan.CandidateBlocks++
		plan.CandidateRows += selection.Count()
	}
	return plan, nil
}

func (idx Int64ValueRowIndex) planAllInt64Predicate() (ColumnPruningCandidatePlan, error) {
	plan := ColumnPruningCandidatePlan{Rows: idx.Rows, Blocks: make([]ColumnPruningBlockCandidate, len(idx.Blocks)), Exact: false}
	for i, block := range idx.Blocks {
		selection, err := NewAllRowSelection(block.RowCount)
		if err != nil {
			return ColumnPruningCandidatePlan{}, err
		}
		candidate := ColumnPruningBlockCandidate{BlockIndex: block.Index, FirstRow: block.FirstRow, RowCount: block.RowCount, Selection: selection, Exact: false}
		plan.Blocks[i] = candidate
		if selection.IsEmpty() {
			plan.PrunedBlocks++
			plan.PrunedRows += block.RowCount
			continue
		}
		plan.CandidateBlocks++
		plan.CandidateRows += selection.Count()
	}
	return plan, nil
}

func validateInt64PruningPredicate(pred Int64PruningPredicate) error {
	switch pred.Kind {
	case Int64PruningPredicateAll, Int64PruningPredicateEqual:
		return nil
	case Int64PruningPredicateRange:
		if pred.Low > pred.High {
			return fmt.Errorf("typedcolumn: int64 pruning range low=%d high=%d", pred.Low, pred.High)
		}
		return nil
	default:
		return fmt.Errorf("typedcolumn: unsupported int64 pruning predicate %q", pred.Kind)
	}
}

func (idx Int64ValueRowIndex) entriesForPredicate(pred Int64PruningPredicate) []Int64PruningEntry {
	switch pred.Kind {
	case Int64PruningPredicateAll:
		return idx.Entries
	case Int64PruningPredicateEqual:
		start := sort.Search(len(idx.Entries), func(i int) bool { return idx.Entries[i].Value >= pred.Value })
		end := sort.Search(len(idx.Entries), func(i int) bool { return idx.Entries[i].Value > pred.Value })
		return idx.Entries[start:end]
	case Int64PruningPredicateRange:
		start := sort.Search(len(idx.Entries), func(i int) bool { return idx.Entries[i].Value >= pred.Low })
		end := sort.Search(len(idx.Entries), func(i int) bool { return idx.Entries[i].Value > pred.High })
		return idx.Entries[start:end]
	default:
		return nil
	}
}

func (idx Int64ValueRowIndex) blockIndexForRow(row int) int {
	if row < 0 || row >= idx.Rows || len(idx.Blocks) == 0 {
		return -1
	}
	pos := sort.Search(len(idx.Blocks), func(i int) bool {
		return idx.Blocks[i].FirstRow+idx.Blocks[i].RowCount > row
	})
	if pos >= len(idx.Blocks) {
		return -1
	}
	block := idx.Blocks[pos]
	if row < block.FirstRow || row >= block.FirstRow+block.RowCount {
		return -1
	}
	return pos
}

func pruningSelectionFromSortedRows(rows int, selected []int) (RowSelection, error) {
	if len(selected) == 0 {
		return NewEmptyRowSelection(rows)
	}
	start, prev := selected[0], selected[0]
	if start < 0 || start >= rows {
		return RowSelection{}, fmt.Errorf("typedcolumn: selected row %d outside [0,%d)", start, rows)
	}
	ranges := make([]RowRange, 0, 4)
	for _, row := range selected[1:] {
		if row <= prev {
			return RowSelection{}, fmt.Errorf("typedcolumn: selected rows not strictly increasing at %d", row)
		}
		if row >= rows {
			return RowSelection{}, fmt.Errorf("typedcolumn: selected row %d outside [0,%d)", row, rows)
		}
		if row == prev+1 {
			prev = row
			continue
		}
		ranges = append(ranges, RowRange{Start: start, End: prev + 1})
		start, prev = row, row
	}
	ranges = append(ranges, RowRange{Start: start, End: prev + 1})
	if len(ranges) <= typedColumnPruningSelectionRangeLimit {
		return NewRangesRowSelection(rows, ranges)
	}
	return NewSparseRowSelection(rows, selected)
}

const typedColumnPruningSelectionRangeLimit = 8

func buildColumnPartPruning(part *ColumnPart) (ColumnPartPruning, error) {
	if part == nil {
		return ColumnPartPruning{}, fmt.Errorf("typedcolumn: nil part")
	}
	out := ColumnPartPruning{Version: columnPartPruningSectionVersion, PartID: part.Descriptor.PartID, Rows: part.Descriptor.RowCount}
	for _, columnDesc := range part.Descriptor.Columns {
		if !integerStatsPayloadColumnType(columnDesc.Type) {
			continue
		}
		column, ok := part.Columns[columnDesc.Name]
		if !ok {
			return ColumnPartPruning{}, fmt.Errorf("typedcolumn: pruning missing column %s", columnDesc.Name)
		}
		index, ok, err := buildInt64ValueRowIndex(part.Descriptor, columnDesc, column)
		if err != nil {
			return ColumnPartPruning{}, err
		}
		if !ok {
			continue
		}
		if out.Int64 == nil {
			out.Int64 = make(map[string]Int64ValueRowIndex)
		}
		out.Int64[columnDesc.Name] = index
	}
	return out, nil
}

func buildInt64ValueRowIndex(desc ColumnPartDescriptor, columnDesc ColumnPartColumnDescriptor, column ColumnPartColumn) (Int64ValueRowIndex, bool, error) {
	if column.Definition.StatsDisabled || !integerStatsPayloadColumnType(column.Definition.Type) || column.Definition.Encoding == EncodingNullableInt64 {
		return Int64ValueRowIndex{}, false, nil
	}
	index := Int64ValueRowIndex{
		Envelope: ColumnPruningEnvelope{
			Version:     ColumnPruningEnvelopeVersion,
			PartID:      desc.PartID,
			ColumnName:  columnDesc.Name,
			ColumnType:  columnDesc.Type,
			Encoding:    column.Definition.Encoding,
			Compression: column.Definition.Compression,
			Rows:        desc.RowCount,
			Blocks:      len(column.Blocks),
			PayloadKind: ColumnPruningPayloadInt64ValueRowsV1,
			Operations:  []ColumnPruningOperation{ColumnPruningOpEquality, ColumnPruningOpOrderedRange},
		},
		Rows:    desc.RowCount,
		Blocks:  make([]Int64PruningBlock, 0, len(column.Blocks)),
		Entries: make([]Int64PruningEntry, 0, desc.RowCount),
	}
	var reader GranuleReader
	for i, block := range column.Blocks {
		if i >= len(columnDesc.Blocks) {
			return Int64ValueRowIndex{}, false, fmt.Errorf("typedcolumn: pruning column %s block %d missing descriptor", columnDesc.Name, i)
		}
		if block.Descriptor != columnDesc.Blocks[i] {
			return Int64ValueRowIndex{}, false, fmt.Errorf("typedcolumn: pruning column %s block %d descriptor mismatch", columnDesc.Name, i)
		}
		g := block.Granule
		if g.NullCount != 0 || g.DefaultCount != 0 {
			return Int64ValueRowIndex{}, false, nil
		}
		values, err := reader.DecodeIntegerAsInt64Into(reader.values[:0], column.Definition.Type, g)
		if err != nil {
			return Int64ValueRowIndex{}, false, fmt.Errorf("typedcolumn: pruning column %s block %d decode: %w", columnDesc.Name, i, err)
		}
		reader.values = values
		if len(values) != block.Descriptor.RowCount {
			return Int64ValueRowIndex{}, false, fmt.Errorf("typedcolumn: pruning column %s block %d values=%d want rows=%d", columnDesc.Name, i, len(values), block.Descriptor.RowCount)
		}
		index.Blocks = append(index.Blocks, Int64PruningBlock{Index: i, FirstRow: block.Descriptor.FirstRow, RowCount: block.Descriptor.RowCount, HasMinMax: g.HasMinMax, Min: g.Min, Max: g.Max})
		for row, value := range values {
			index.Entries = append(index.Entries, Int64PruningEntry{Value: value, Row: block.Descriptor.FirstRow + row})
		}
	}
	sort.Slice(index.Entries, func(i, j int) bool {
		if index.Entries[i].Value != index.Entries[j].Value {
			return index.Entries[i].Value < index.Entries[j].Value
		}
		return index.Entries[i].Row < index.Entries[j].Row
	})
	if err := ValidateInt64ValueRowIndex(index, desc, columnDesc, column); err != nil {
		return Int64ValueRowIndex{}, false, err
	}
	return index, true, nil
}

func encodeColumnPartPruningSection(pruning ColumnPartPruning) ([]byte, error) {
	if pruning.Empty() {
		return nil, nil
	}
	var enc columnPartImageEncoder
	enc.u32(columnPartPruningSectionMagic)
	enc.u16(columnPartPruningSectionVersion)
	enc.u16(0)
	enc.u64(pruning.PartID)
	enc.i64(int64(pruning.Rows))
	names := make([]string, 0, len(pruning.Int64))
	for name := range pruning.Int64 {
		names = append(names, name)
	}
	sort.Strings(names)
	enc.u32(uint32(len(names)))
	for _, name := range names {
		index := pruning.Int64[name]
		payload, err := encodeInt64PruningPayload(index)
		if err != nil {
			return nil, err
		}
		envelope := index.Envelope
		envelope.PayloadLength = len(payload)
		envelope.PayloadChecksum = crc.Checksum(payload)
		if err := encodeColumnPruningEnvelope(&enc, envelope); err != nil {
			return nil, err
		}
		enc.buf = append(enc.buf, payload...)
	}
	return enc.bytes(), nil
}

func (i ColumnPartImage) PruningMetadataSection() (ColumnPartImageSection, bool, error) {
	sections := i.sectionsByKind(ColumnPartImageSectionPruningMetadata)
	if len(sections) == 0 {
		return ColumnPartImageSection{}, false, nil
	}
	if len(sections) != 1 {
		return ColumnPartImageSection{}, false, fmt.Errorf("typedcolumn: image has %d %s sections, want at most 1", len(sections), ColumnPartImageSectionPruningMetadata)
	}
	return sections[0], true, nil
}

func decodeColumnPruningSectionFromImage(image ColumnPartImage, desc ColumnPartDescriptor, columns map[string]ColumnPartColumn) (ColumnPartPruning, error) {
	section, ok, err := image.PruningMetadataSection()
	if err != nil || !ok {
		return ColumnPartPruning{}, err
	}
	data, err := image.pruningMetadataSectionBytes(section)
	if err != nil {
		return ColumnPartPruning{}, err
	}
	pruning, err := DecodeColumnPartPruningSection(data)
	if err != nil {
		return ColumnPartPruning{}, err
	}
	if err := ValidateColumnPartPruning(pruning, desc, columns); err != nil {
		return ColumnPartPruning{}, err
	}
	return pruning, nil
}

func DecodeColumnPartPruningImageSection(section ColumnPartImageSection, payload []byte) (ColumnPartPruning, error) {
	rawBytes := section.RawBytes
	if rawBytes == 0 && section.Compression == CompressionNone {
		rawBytes = section.Length
	}
	data, err := sectionPayloadBytesWithKnownRawLength(section, payload, rawBytes, maxCompressedPruningMetadataSectionRawBytes, "pruning metadata")
	if err != nil {
		return ColumnPartPruning{}, err
	}
	return DecodeColumnPartPruningSection(data)
}

func DecodeColumnPartPruningSection(data []byte) (ColumnPartPruning, error) {
	dec := columnPartImageDecoder{data: data}
	magic, err := dec.u32()
	if err != nil {
		return ColumnPartPruning{}, err
	}
	if magic != columnPartPruningSectionMagic {
		return ColumnPartPruning{}, fmt.Errorf("typedcolumn: bad column pruning section magic=0x%08x", magic)
	}
	version, err := dec.u16()
	if err != nil {
		return ColumnPartPruning{}, err
	}
	if version != columnPartPruningSectionVersion {
		return ColumnPartPruning{}, fmt.Errorf("typedcolumn: unsupported column pruning section version=%d", version)
	}
	reserved, err := dec.u16()
	if err != nil {
		return ColumnPartPruning{}, err
	}
	if reserved != 0 {
		return ColumnPartPruning{}, fmt.Errorf("typedcolumn: column pruning section reserved=%d want 0", reserved)
	}
	partID, err := dec.u64()
	if err != nil {
		return ColumnPartPruning{}, err
	}
	rows64, err := dec.i64()
	if err != nil {
		return ColumnPartPruning{}, err
	}
	rows, err := nonNegativeInt64ToInt(rows64, "column pruning rows")
	if err != nil {
		return ColumnPartPruning{}, err
	}
	columnCount, err := dec.u32()
	if err != nil {
		return ColumnPartPruning{}, err
	}
	columns, err := dec.boundedCount(columnCount, 64, "column pruning entries")
	if err != nil {
		return ColumnPartPruning{}, err
	}
	pruning := ColumnPartPruning{Version: version, PartID: partID, Rows: rows, Int64: make(map[string]Int64ValueRowIndex, columns)}
	for i := 0; i < columns; i++ {
		envelope, err := decodeColumnPruningEnvelope(&dec)
		if err != nil {
			return ColumnPartPruning{}, err
		}
		payload, err := dec.bytes(envelope.PayloadLength)
		if err != nil {
			return ColumnPartPruning{}, err
		}
		if got := crc.Checksum(payload); got != envelope.PayloadChecksum {
			return ColumnPartPruning{}, fmt.Errorf("typedcolumn: column pruning %s payload checksum=%08x want=%08x: %s", envelope.ColumnName, got, envelope.PayloadChecksum, ColumnPruningReasonChecksumMismatch)
		}
		switch envelope.PayloadKind {
		case ColumnPruningPayloadInt64ValueRowsV1:
			index, err := decodeInt64PruningPayload(envelope, payload)
			if err != nil {
				return ColumnPartPruning{}, err
			}
			if _, exists := pruning.Int64[envelope.ColumnName]; exists {
				return ColumnPartPruning{}, fmt.Errorf("typedcolumn: duplicate int64 pruning metadata for column %s", envelope.ColumnName)
			}
			pruning.Int64[envelope.ColumnName] = index
		default:
			return ColumnPartPruning{}, fmt.Errorf("typedcolumn: unsupported column pruning payload %q for column %s: %s", envelope.PayloadKind, envelope.ColumnName, ColumnPruningReasonUnsupportedPayload)
		}
	}
	if err := dec.finish(); err != nil {
		return ColumnPartPruning{}, err
	}
	return pruning, nil
}

func encodeColumnPruningEnvelope(enc *columnPartImageEncoder, envelope ColumnPruningEnvelope) error {
	if envelope.Version == 0 {
		envelope.Version = ColumnPruningEnvelopeVersion
	}
	if envelope.Version != ColumnPruningEnvelopeVersion {
		return fmt.Errorf("typedcolumn: unsupported column pruning envelope version=%d", envelope.Version)
	}
	if envelope.ColumnName == "" {
		return fmt.Errorf("typedcolumn: column pruning envelope requires column name")
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
	enc.str(string(envelope.PayloadKind))
	operations := make([]string, len(envelope.Operations))
	for i, op := range envelope.Operations {
		operations[i] = string(op)
	}
	enc.stringSlice(operations)
	enc.i64(int64(envelope.PayloadLength))
	enc.u32(envelope.PayloadChecksum)
	enc.u32(0)
	return nil
}

func decodeColumnPruningEnvelope(dec *columnPartImageDecoder) (ColumnPruningEnvelope, error) {
	version, err := dec.u16()
	if err != nil {
		return ColumnPruningEnvelope{}, err
	}
	if version != ColumnPruningEnvelopeVersion {
		return ColumnPruningEnvelope{}, fmt.Errorf("typedcolumn: unsupported column pruning envelope version=%d", version)
	}
	reserved, err := dec.u16()
	if err != nil {
		return ColumnPruningEnvelope{}, err
	}
	if reserved != 0 {
		return ColumnPruningEnvelope{}, fmt.Errorf("typedcolumn: column pruning envelope reserved=%d want 0", reserved)
	}
	partID, err := dec.u64()
	if err != nil {
		return ColumnPruningEnvelope{}, err
	}
	columnName, err := dec.str()
	if err != nil {
		return ColumnPruningEnvelope{}, err
	}
	columnTypeCode, err := dec.u16()
	if err != nil {
		return ColumnPruningEnvelope{}, err
	}
	columnType, err := columnTypeFromCode(columnTypeCode)
	if err != nil {
		return ColumnPruningEnvelope{}, err
	}
	encodingCode, err := dec.u16()
	if err != nil {
		return ColumnPruningEnvelope{}, err
	}
	encoding, err := decodeStatsEncoding(encodingCode)
	if err != nil {
		return ColumnPruningEnvelope{}, err
	}
	compressionCode, err := dec.u16()
	if err != nil {
		return ColumnPruningEnvelope{}, err
	}
	compression, err := decodeStatsCompression(compressionCode)
	if err != nil {
		return ColumnPruningEnvelope{}, err
	}
	reserved, err = dec.u16()
	if err != nil {
		return ColumnPruningEnvelope{}, err
	}
	if reserved != 0 {
		return ColumnPruningEnvelope{}, fmt.Errorf("typedcolumn: column pruning envelope encoding reserved=%d want 0", reserved)
	}
	rows, err := decodeStatsInt(dec, "column pruning rows")
	if err != nil {
		return ColumnPruningEnvelope{}, err
	}
	blocks, err := decodeStatsInt(dec, "column pruning blocks")
	if err != nil {
		return ColumnPruningEnvelope{}, err
	}
	nullCount, err := decodeStatsInt(dec, "column pruning null count")
	if err != nil {
		return ColumnPruningEnvelope{}, err
	}
	defaultCount, err := decodeStatsInt(dec, "column pruning default count")
	if err != nil {
		return ColumnPruningEnvelope{}, err
	}
	payloadKind, err := dec.str()
	if err != nil {
		return ColumnPruningEnvelope{}, err
	}
	operationStrings, err := dec.stringSlice()
	if err != nil {
		return ColumnPruningEnvelope{}, err
	}
	operations := make([]ColumnPruningOperation, len(operationStrings))
	for i, op := range operationStrings {
		operations[i] = ColumnPruningOperation(op)
	}
	payloadLength, err := decodeStatsInt(dec, "column pruning payload length")
	if err != nil {
		return ColumnPruningEnvelope{}, err
	}
	payloadChecksum, err := dec.u32()
	if err != nil {
		return ColumnPruningEnvelope{}, err
	}
	reserved32, err := dec.u32()
	if err != nil {
		return ColumnPruningEnvelope{}, err
	}
	if reserved32 != 0 {
		return ColumnPruningEnvelope{}, fmt.Errorf("typedcolumn: column pruning envelope trailer reserved=%d want 0", reserved32)
	}
	return ColumnPruningEnvelope{Version: version, PartID: partID, ColumnName: columnName, ColumnType: columnType, Encoding: encoding, Compression: compression, Rows: rows, Blocks: blocks, NullCount: nullCount, DefaultCount: defaultCount, PayloadKind: ColumnPruningPayloadKind(payloadKind), Operations: operations, PayloadLength: payloadLength, PayloadChecksum: payloadChecksum}, nil
}

func encodeInt64PruningPayload(index Int64ValueRowIndex) ([]byte, error) {
	blocksBytes, err := checkedMulInt(len(index.Blocks), int64PruningBlockEncodedBytes, "int64 pruning payload block bytes")
	if err != nil {
		return nil, err
	}
	entriesBytes, err := checkedMulInt(len(index.Entries), 16, "int64 pruning payload entry bytes")
	if err != nil {
		return nil, err
	}
	payloadBytes, err := checkedAddInt(36, blocksBytes, "int64 pruning payload bytes")
	if err != nil {
		return nil, err
	}
	payloadBytes, err = checkedAddInt(payloadBytes, entriesBytes, "int64 pruning payload bytes")
	if err != nil {
		return nil, err
	}
	enc := columnPartImageEncoder{buf: make([]byte, 0, payloadBytes)}
	enc.u16(Int64PruningPayloadVersion)
	enc.u16(0)
	enc.i64(int64(index.Rows))
	enc.i64(int64(index.NullCount))
	enc.i64(int64(index.DefaultCount))
	enc.u32(uint32(len(index.Blocks)))
	for _, block := range index.Blocks {
		enc.i64(int64(block.Index))
		enc.i64(int64(block.FirstRow))
		enc.i64(int64(block.RowCount))
		enc.boolean(block.HasMinMax)
		enc.i64(block.Min)
		enc.i64(block.Max)
	}
	enc.u32(uint32(len(index.Entries)))
	for _, entry := range index.Entries {
		enc.i64(entry.Value)
		enc.i64(int64(entry.Row))
	}
	return enc.bytes(), nil
}

func decodeInt64PruningPayload(envelope ColumnPruningEnvelope, payload []byte) (Int64ValueRowIndex, error) {
	dec := columnPartImageDecoder{data: payload}
	version, err := dec.u16()
	if err != nil {
		return Int64ValueRowIndex{}, err
	}
	if version != Int64PruningPayloadVersion {
		return Int64ValueRowIndex{}, fmt.Errorf("typedcolumn: unsupported int64 pruning payload version=%d", version)
	}
	reserved, err := dec.u16()
	if err != nil {
		return Int64ValueRowIndex{}, err
	}
	if reserved != 0 {
		return Int64ValueRowIndex{}, fmt.Errorf("typedcolumn: int64 pruning reserved=%d want 0", reserved)
	}
	rows, err := decodeStatsInt(&dec, "int64 pruning rows")
	if err != nil {
		return Int64ValueRowIndex{}, err
	}
	nullCount, err := decodeStatsInt(&dec, "int64 pruning null count")
	if err != nil {
		return Int64ValueRowIndex{}, err
	}
	defaultCount, err := decodeStatsInt(&dec, "int64 pruning default count")
	if err != nil {
		return Int64ValueRowIndex{}, err
	}
	blockCount, err := dec.u32()
	if err != nil {
		return Int64ValueRowIndex{}, err
	}
	blocksTotal, err := dec.boundedCount(blockCount, int64PruningBlockEncodedBytes, "int64 pruning blocks")
	if err != nil {
		return Int64ValueRowIndex{}, err
	}
	index := Int64ValueRowIndex{Envelope: envelope, Rows: rows, NullCount: nullCount, DefaultCount: defaultCount, Blocks: make([]Int64PruningBlock, 0, blocksTotal)}
	for i := 0; i < blocksTotal; i++ {
		block, err := decodeInt64PruningBlock(&dec)
		if err != nil {
			return Int64ValueRowIndex{}, err
		}
		if block.Index != i {
			return Int64ValueRowIndex{}, fmt.Errorf("typedcolumn: int64 pruning block index=%d want %d", block.Index, i)
		}
		index.Blocks = append(index.Blocks, block)
	}
	entryCount, err := dec.u32()
	if err != nil {
		return Int64ValueRowIndex{}, err
	}
	entriesTotal, err := dec.boundedCount(entryCount, 16, "int64 pruning entries")
	if err != nil {
		return Int64ValueRowIndex{}, err
	}
	index.Entries = make([]Int64PruningEntry, 0, entriesTotal)
	for i := 0; i < entriesTotal; i++ {
		value, err := dec.i64()
		if err != nil {
			return Int64ValueRowIndex{}, err
		}
		row64, err := dec.i64()
		if err != nil {
			return Int64ValueRowIndex{}, err
		}
		row, err := nonNegativeInt64ToInt(row64, "int64 pruning entry row")
		if err != nil {
			return Int64ValueRowIndex{}, err
		}
		index.Entries = append(index.Entries, Int64PruningEntry{Value: value, Row: row})
	}
	if err := dec.finish(); err != nil {
		return Int64ValueRowIndex{}, err
	}
	return index, nil
}

const int64PruningBlockEncodedBytes = 42

func decodeInt64PruningBlock(dec *columnPartImageDecoder) (Int64PruningBlock, error) {
	index, err := decodeStatsInt(dec, "int64 pruning block index")
	if err != nil {
		return Int64PruningBlock{}, err
	}
	firstRow, err := decodeStatsInt(dec, "int64 pruning block first row")
	if err != nil {
		return Int64PruningBlock{}, err
	}
	rowCount, err := decodeStatsInt(dec, "int64 pruning block row count")
	if err != nil {
		return Int64PruningBlock{}, err
	}
	hasMinMax, err := dec.boolean()
	if err != nil {
		return Int64PruningBlock{}, err
	}
	minValue, err := dec.i64()
	if err != nil {
		return Int64PruningBlock{}, err
	}
	maxValue, err := dec.i64()
	if err != nil {
		return Int64PruningBlock{}, err
	}
	return Int64PruningBlock{Index: index, FirstRow: firstRow, RowCount: rowCount, HasMinMax: hasMinMax, Min: minValue, Max: maxValue}, nil
}

func ValidateColumnPartPruning(pruning ColumnPartPruning, desc ColumnPartDescriptor, columns map[string]ColumnPartColumn) error {
	if pruning.Version != columnPartPruningSectionVersion {
		return fmt.Errorf("typedcolumn: column pruning version=%d want %d", pruning.Version, columnPartPruningSectionVersion)
	}
	if pruning.PartID != desc.PartID {
		return fmt.Errorf("typedcolumn: column pruning part_id=%d want %d: %s", pruning.PartID, desc.PartID, ColumnPruningReasonIdentityMismatch)
	}
	if pruning.Rows != desc.RowCount {
		return fmt.Errorf("typedcolumn: column pruning rows=%d want %d: %s", pruning.Rows, desc.RowCount, ColumnPruningReasonRowCountMismatch)
	}
	columnDescs := make(map[string]ColumnPartColumnDescriptor, len(desc.Columns))
	for _, columnDesc := range desc.Columns {
		columnDescs[columnDesc.Name] = columnDesc
	}
	for name, index := range pruning.Int64 {
		columnDesc, ok := columnDescs[name]
		if !ok {
			return fmt.Errorf("typedcolumn: column pruning %s missing descriptor: %s", name, ColumnPruningReasonIdentityMismatch)
		}
		column, ok := columns[name]
		if !ok {
			return fmt.Errorf("typedcolumn: column pruning %s missing column: %s", name, ColumnPruningReasonIdentityMismatch)
		}
		if err := ValidateInt64ValueRowIndex(index, desc, columnDesc, column); err != nil {
			return err
		}
	}
	return nil
}

func ValidateInt64ValueRowIndex(index Int64ValueRowIndex, desc ColumnPartDescriptor, columnDesc ColumnPartColumnDescriptor, column ColumnPartColumn) error {
	envelope := index.Envelope
	if envelope.Version != ColumnPruningEnvelopeVersion {
		return fmt.Errorf("typedcolumn: column pruning %s envelope version=%d want %d", envelope.ColumnName, envelope.Version, ColumnPruningEnvelopeVersion)
	}
	if envelope.PartID != desc.PartID {
		return fmt.Errorf("typedcolumn: column pruning %s part_id=%d want %d: %s", envelope.ColumnName, envelope.PartID, desc.PartID, ColumnPruningReasonIdentityMismatch)
	}
	if envelope.ColumnName != columnDesc.Name || envelope.ColumnName != column.Definition.Name {
		return fmt.Errorf("typedcolumn: column pruning name=%q descriptor=%q definition=%q: %s", envelope.ColumnName, columnDesc.Name, column.Definition.Name, ColumnPruningReasonIdentityMismatch)
	}
	if column.Definition.StatsDisabled {
		return fmt.Errorf("typedcolumn: column pruning %s disabled by definition: %s", envelope.ColumnName, ColumnPruningReasonUnsupportedPayload)
	}
	if !integerStatsPayloadColumnType(envelope.ColumnType) || !integerStatsPayloadColumnType(columnDesc.Type) || !integerStatsPayloadColumnType(column.Definition.Type) || envelope.PayloadKind != ColumnPruningPayloadInt64ValueRowsV1 {
		return fmt.Errorf("typedcolumn: column pruning %s int64-compatible payload cannot apply to type envelope=%s descriptor=%s definition=%s payload=%s: %s", envelope.ColumnName, envelope.ColumnType, columnDesc.Type, column.Definition.Type, envelope.PayloadKind, ColumnPruningReasonUnsupportedPayload)
	}
	if envelope.ColumnType != columnDesc.Type || envelope.ColumnType != column.Definition.Type {
		return fmt.Errorf("typedcolumn: column pruning %s type identity envelope=%s descriptor=%s definition=%s: %s", envelope.ColumnName, envelope.ColumnType, columnDesc.Type, column.Definition.Type, ColumnPruningReasonIdentityMismatch)
	}
	if envelope.Encoding != column.Definition.Encoding || envelope.Compression != column.Definition.Compression {
		return fmt.Errorf("typedcolumn: column pruning %s encoding/compression=%s/%s want %s/%s: %s", envelope.ColumnName, envelope.Encoding, envelope.Compression, column.Definition.Encoding, column.Definition.Compression, ColumnPruningReasonIdentityMismatch)
	}
	if envelope.Rows != desc.RowCount || index.Rows != desc.RowCount {
		return fmt.Errorf("typedcolumn: column pruning %s rows envelope=%d payload=%d want %d: %s", envelope.ColumnName, envelope.Rows, index.Rows, desc.RowCount, ColumnPruningReasonRowCountMismatch)
	}
	if envelope.Blocks != len(column.Blocks) || len(index.Blocks) != len(column.Blocks) || len(columnDesc.Blocks) != len(column.Blocks) {
		return fmt.Errorf("typedcolumn: column pruning %s blocks envelope=%d payload=%d descriptor=%d column=%d: %s", envelope.ColumnName, envelope.Blocks, len(index.Blocks), len(columnDesc.Blocks), len(column.Blocks), ColumnPruningReasonRowCountMismatch)
	}
	if envelope.NullCount != index.NullCount || envelope.DefaultCount != index.DefaultCount || index.NullCount != 0 || index.DefaultCount != 0 {
		return fmt.Errorf("typedcolumn: column pruning %s null/default counts envelope=%d/%d payload=%d/%d want 0/0: %s", envelope.ColumnName, envelope.NullCount, envelope.DefaultCount, index.NullCount, index.DefaultCount, ColumnPruningReasonNullDefaultMismatch)
	}
	if !envelope.SupportsOperation(ColumnPruningOpEquality) || !envelope.SupportsOperation(ColumnPruningOpOrderedRange) {
		return fmt.Errorf("typedcolumn: column pruning %s does not advertise equality and ordered range: %s", envelope.ColumnName, ColumnPruningReasonOperationUnsupported)
	}
	rowTotal := 0
	for i, blockIndex := range index.Blocks {
		block := column.Blocks[i]
		if blockIndex.Index != i || blockIndex.FirstRow != block.Descriptor.FirstRow || blockIndex.RowCount != block.Descriptor.RowCount {
			return fmt.Errorf("typedcolumn: column pruning %s block %d row identity mismatch: %s", envelope.ColumnName, i, ColumnPruningReasonRowCountMismatch)
		}
		if blockIndex.HasMinMax != block.Granule.HasMinMax || (blockIndex.HasMinMax && (blockIndex.Min != block.Granule.Min || blockIndex.Max != block.Granule.Max || blockIndex.Min > blockIndex.Max)) {
			return fmt.Errorf("typedcolumn: column pruning %s block %d min/max mismatch: %s", envelope.ColumnName, i, ColumnPruningReasonMinMaxMismatch)
		}
		rowTotal += blockIndex.RowCount
	}
	if rowTotal != desc.RowCount {
		return fmt.Errorf("typedcolumn: column pruning %s block rows=%d want %d: %s", envelope.ColumnName, rowTotal, desc.RowCount, ColumnPruningReasonRowCountMismatch)
	}
	if len(index.Entries) != desc.RowCount {
		return fmt.Errorf("typedcolumn: column pruning %s entries=%d want rows=%d: %s", envelope.ColumnName, len(index.Entries), desc.RowCount, ColumnPruningReasonRowCountMismatch)
	}
	seen := make([]bool, desc.RowCount)
	for i, entry := range index.Entries {
		if i > 0 {
			prev := index.Entries[i-1]
			if entry.Value < prev.Value || (entry.Value == prev.Value && entry.Row <= prev.Row) {
				return fmt.Errorf("typedcolumn: column pruning %s entry %d order mismatch: %s", envelope.ColumnName, i, ColumnPruningReasonEntryOrderMismatch)
			}
		}
		if entry.Row < 0 || entry.Row >= desc.RowCount {
			return fmt.Errorf("typedcolumn: column pruning %s entry %d row=%d outside rows=%d: %s", envelope.ColumnName, i, entry.Row, desc.RowCount, ColumnPruningReasonEntryRowMismatch)
		}
		if seen[entry.Row] {
			return fmt.Errorf("typedcolumn: column pruning %s duplicate row=%d: %s", envelope.ColumnName, entry.Row, ColumnPruningReasonEntryRowMismatch)
		}
		seen[entry.Row] = true
		blockIndex := index.blockIndexForRow(entry.Row)
		if blockIndex < 0 {
			return fmt.Errorf("typedcolumn: column pruning %s entry %d row=%d outside blocks: %s", envelope.ColumnName, i, entry.Row, ColumnPruningReasonEntryRowMismatch)
		}
		block := index.Blocks[blockIndex]
		if block.HasMinMax && (entry.Value < block.Min || entry.Value > block.Max) {
			return fmt.Errorf("typedcolumn: column pruning %s entry %d value=%d outside block min/max [%d,%d]: %s", envelope.ColumnName, i, entry.Value, block.Min, block.Max, ColumnPruningReasonMinMaxMismatch)
		}
	}
	return nil
}

func cloneColumnPartPruning(pruning ColumnPartPruning) ColumnPartPruning {
	out := pruning
	if pruning.Int64 != nil {
		out.Int64 = make(map[string]Int64ValueRowIndex, len(pruning.Int64))
		for name, index := range pruning.Int64 {
			out.Int64[name] = cloneInt64ValueRowIndex(index)
		}
	}
	return out
}

func cloneInt64ValueRowIndex(index Int64ValueRowIndex) Int64ValueRowIndex {
	out := index
	out.Envelope.Operations = append([]ColumnPruningOperation(nil), index.Envelope.Operations...)
	out.Blocks = append([]Int64PruningBlock(nil), index.Blocks...)
	out.Entries = append([]Int64PruningEntry(nil), index.Entries...)
	return out
}
