package typedcolumn

import (
	"encoding/binary"
	"strings"
	"testing"
)

func TestColumnStatsInt64RoundTripAndValidation(t *testing.T) {
	part := mustStatsTestPart(t, []int64{1, 2, 3, 4, 5}, EncodingDeltaVarint)
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{"id": "int64", "value": "int64"}})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	decoded, err := ColumnPartFromImage(image)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	stats, ok := decoded.ColumnStats.Int64Column("value")
	if !ok {
		t.Fatalf("missing int64 stats: %+v", decoded.ColumnStats)
	}
	if stats.Count != 5 || stats.ValueCount != 5 || stats.NullCount != 0 || stats.DefaultCount != 0 || !stats.SumValid || stats.Sum != 15 || !stats.HasMinMax || stats.Min != 1 || stats.Max != 5 {
		t.Fatalf("stats=%+v want count/sum/min/max", stats)
	}
	if ok, reason := stats.CanAnswer(ColumnStatsOpSum, ColumnStatsSelectionFullBlock); !ok || reason != ColumnStatsReasonSupported {
		t.Fatalf("CanAnswer sum/full_block ok=%v reason=%s", ok, reason)
	}
	if ok, reason := stats.CanAnswer(ColumnStatsOpSum, ColumnStatsSelectionShape("sparse")); ok || reason != ColumnStatsReasonSelectionUnsupported {
		t.Fatalf("CanAnswer sparse ok=%v reason=%s", ok, reason)
	}
	if ok, reason := stats.CanAnswer(ColumnStatsOperation("predicate.equality"), ColumnStatsSelectionFullBlock); ok || reason != ColumnStatsReasonOperationUnsupported {
		t.Fatalf("CanAnswer predicate ok=%v reason=%s", ok, reason)
	}
}

func TestBuildInt64BlockStatsUsesCursorWithoutBlockAllocation(t *testing.T) {
	const rows = 4096
	raw := make([]byte, rows*8)
	var wantSum int64
	for i := 0; i < rows; i++ {
		value := int64(i + 1)
		wantSum += value
		binary.LittleEndian.PutUint64(raw[i*8:], uint64(value))
	}
	block := ColumnBlock{
		Descriptor: ColumnBlockDescriptor{FirstRow: 0, RowCount: rows, FirstGranule: 0, LastGranule: 0, Encoding: EncodingRawInt64, Compression: CompressionNone, RawBytes: len(raw), StoredBytes: len(raw)},
		Granule:    EncodedGranule{Rows: rows, Encoding: EncodingRawInt64, Compression: CompressionNone, RawBytes: len(raw), StoredBytes: len(raw), HasMinMax: true, Min: 1, Max: rows, PayloadRef: PayloadRef{Kind: PayloadRefInline, Length: len(raw)}, Payload: raw},
	}
	var reader GranuleReader
	var got Int64BlockStats
	allocs := testing.AllocsPerRun(100, func() {
		stats, err := buildInt64BlockStats(&reader, ColumnTypeInt64, 0, block)
		if err != nil {
			panic(err)
		}
		if stats.Sum != wantSum || !stats.SumValid || stats.RowCount != rows {
			panic("unexpected int64 block stats")
		}
		got = stats
	})
	if allocs != 0 {
		t.Fatalf("buildInt64BlockStats int64 allocations/run=%v want 0; stats=%+v", allocs, got)
	}
}

func TestBuildInt64BlockStatsReusesPrimitiveScratch(t *testing.T) {
	const rows = 4096
	raw := make([]byte, rows*4)
	var wantSum int64
	for i := 0; i < rows; i++ {
		value := uint32(i + 1)
		wantSum += int64(value)
		binary.LittleEndian.PutUint32(raw[i*4:], value)
	}
	block := ColumnBlock{
		Descriptor: ColumnBlockDescriptor{FirstRow: 0, RowCount: rows, FirstGranule: 0, LastGranule: 0, Encoding: EncodingRawUint32, Compression: CompressionNone, RawBytes: len(raw), StoredBytes: len(raw)},
		Granule:    EncodedGranule{Rows: rows, Encoding: EncodingRawUint32, Compression: CompressionNone, RawBytes: len(raw), StoredBytes: len(raw), HasMinMax: true, Min: 1, Max: rows, PayloadRef: PayloadRef{Kind: PayloadRefInline, Length: len(raw)}, Payload: raw},
	}
	var reader GranuleReader
	var got Int64BlockStats
	allocs := testing.AllocsPerRun(100, func() {
		stats, err := buildInt64BlockStats(&reader, ColumnTypeUint32, 0, block)
		if err != nil {
			panic(err)
		}
		if stats.Sum != wantSum || !stats.SumValid || stats.RowCount != rows {
			panic("unexpected uint32 block stats")
		}
		got = stats
	})
	if allocs != 0 {
		t.Fatalf("buildInt64BlockStats uint32 allocations/run=%v want 0 after scratch warmup; stats=%+v", allocs, got)
	}
}

func TestColumnStatsEnvelopeRejectsCorruptAndMismatchedMetadata(t *testing.T) {
	part := mustStatsTestPart(t, []int64{10, 20, 30}, EncodingRawInt64)
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{"id": "int64", "value": "int64"}})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	section, ok, err := image.ColumnStatsSection()
	if err != nil || !ok {
		t.Fatalf("ColumnStatsSection ok=%v err=%v", ok, err)
	}
	raw := append([]byte(nil), image.sectionBytes(section)...)
	if len(raw) < 6 {
		t.Fatalf("stats raw too short: %d", len(raw))
	}
	badVersion := append([]byte(nil), raw...)
	badVersion[4]++
	if _, err := DecodeColumnPartStatsSection(badVersion); err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("bad version err=%v", err)
	}
	truncated := raw[:len(raw)-1]
	if _, err := DecodeColumnPartStatsSection(truncated); err == nil || !strings.Contains(err.Error(), "truncated") {
		t.Fatalf("truncated err=%v", err)
	}
	badChecksum := append([]byte(nil), raw...)
	badChecksum[len(badChecksum)-1] ^= 0x55
	if _, err := DecodeColumnPartStatsSection(badChecksum); err == nil || !strings.Contains(err.Error(), ColumnStatsReasonChecksumMismatch) {
		t.Fatalf("checksum err=%v", err)
	}
	encodingOffset, compressionOffset := firstStatsEnvelopeEncodingOffsets(t, raw)
	badEncodingOverflow := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint16(badEncodingOverflow[encodingOffset:], 0x0101)
	if _, err := DecodeColumnPartStatsSection(badEncodingOverflow); err == nil || !strings.Contains(err.Error(), "encoding") || !strings.Contains(err.Error(), "exceeds uint8") {
		t.Fatalf("encoding overflow err=%v", err)
	}
	badEncodingUnknown := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint16(badEncodingUnknown[encodingOffset:], 200)
	if _, err := DecodeColumnPartStatsSection(badEncodingUnknown); err == nil || !strings.Contains(err.Error(), "unknown column stats envelope encoding") {
		t.Fatalf("encoding unknown err=%v", err)
	}
	badCompressionOverflow := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint16(badCompressionOverflow[compressionOffset:], 0x0101)
	if _, err := DecodeColumnPartStatsSection(badCompressionOverflow); err == nil || !strings.Contains(err.Error(), "compression") || !strings.Contains(err.Error(), "exceeds uint8") {
		t.Fatalf("compression overflow err=%v", err)
	}
	badCompressionUnknown := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint16(badCompressionUnknown[compressionOffset:], 200)
	if _, err := DecodeColumnPartStatsSection(badCompressionUnknown); err == nil || !strings.Contains(err.Error(), "unknown column stats envelope compression") {
		t.Fatalf("compression unknown err=%v", err)
	}

	stats, err := DecodeColumnPartStatsSection(raw)
	if err != nil {
		t.Fatalf("DecodeColumnPartStatsSection: %v", err)
	}
	if err := ValidateColumnPartStats(stats, part.Descriptor, part.Columns); err != nil {
		t.Fatalf("ValidateColumnPartStats: %v", err)
	}
	wrongPart := stats
	wrongPart.PartID++
	if err := ValidateColumnPartStats(wrongPart, part.Descriptor, part.Columns); err == nil || !strings.Contains(err.Error(), ColumnStatsReasonIdentityMismatch) {
		t.Fatalf("wrong part err=%v", err)
	}
	valueStats := stats.Int64["value"]
	wrongName := cloneInt64ColumnStats(valueStats)
	wrongName.Envelope.ColumnName = "other"
	if err := ValidateInt64ColumnStats(wrongName, part.Descriptor, part.Descriptor.Columns[1], part.Columns["value"]); err == nil || !strings.Contains(err.Error(), ColumnStatsReasonIdentityMismatch) {
		t.Fatalf("wrong name err=%v", err)
	}
	wrongRows := cloneInt64ColumnStats(valueStats)
	wrongRows.Blocks[0].RowCount++
	if err := ValidateInt64ColumnStats(wrongRows, part.Descriptor, part.Descriptor.Columns[1], part.Columns["value"]); err == nil || !strings.Contains(err.Error(), ColumnStatsReasonRowCountMismatch) {
		t.Fatalf("wrong rows err=%v", err)
	}
	wrongMinMax := cloneInt64ColumnStats(valueStats)
	wrongMinMax.Blocks[0].Min--
	if err := ValidateInt64ColumnStats(wrongMinMax, part.Descriptor, part.Descriptor.Columns[1], part.Columns["value"]); err == nil || !strings.Contains(err.Error(), ColumnStatsReasonMinMaxMismatch) {
		t.Fatalf("wrong min/max err=%v", err)
	}
	wrongCounts := cloneInt64ColumnStats(valueStats)
	wrongCounts.Blocks[0].DefaultCount = 1
	if err := ValidateInt64ColumnStats(wrongCounts, part.Descriptor, part.Descriptor.Columns[1], part.Columns["value"]); err == nil || !strings.Contains(err.Error(), ColumnStatsReasonNullDefaultMismatch) {
		t.Fatalf("wrong null/default err=%v", err)
	}
	wrongType := cloneInt64ColumnStats(valueStats)
	wrongType.Envelope.ColumnType = ColumnTypeBool
	if err := ValidateInt64ColumnStats(wrongType, part.Descriptor, part.Descriptor.Columns[1], part.Columns["value"]); err == nil || !strings.Contains(err.Error(), ColumnStatsReasonUnsupportedPayload) {
		t.Fatalf("wrong type err=%v", err)
	}
}

func firstStatsEnvelopeEncodingOffsets(t *testing.T, data []byte) (int, int) {
	t.Helper()
	dec := columnPartImageDecoder{data: data}
	if _, err := dec.u32(); err != nil {
		t.Fatalf("stats magic: %v", err)
	}
	if _, err := dec.u16(); err != nil {
		t.Fatalf("stats version: %v", err)
	}
	if _, err := dec.u16(); err != nil {
		t.Fatalf("stats reserved: %v", err)
	}
	if _, err := dec.u64(); err != nil {
		t.Fatalf("stats part id: %v", err)
	}
	if _, err := dec.i64(); err != nil {
		t.Fatalf("stats rows: %v", err)
	}
	if _, err := dec.u32(); err != nil {
		t.Fatalf("stats column count: %v", err)
	}
	if _, err := dec.u16(); err != nil {
		t.Fatalf("envelope version: %v", err)
	}
	if _, err := dec.u16(); err != nil {
		t.Fatalf("envelope reserved: %v", err)
	}
	if _, err := dec.u64(); err != nil {
		t.Fatalf("envelope part id: %v", err)
	}
	if _, err := dec.str(); err != nil {
		t.Fatalf("envelope column name: %v", err)
	}
	if _, err := dec.u16(); err != nil {
		t.Fatalf("envelope column type: %v", err)
	}
	encodingOffset := dec.offset
	if _, err := dec.u16(); err != nil {
		t.Fatalf("envelope encoding: %v", err)
	}
	compressionOffset := dec.offset
	if _, err := dec.u16(); err != nil {
		t.Fatalf("envelope compression: %v", err)
	}
	return encodingOffset, compressionOffset
}

func TestColumnStatsOverflowAndNullableSemantics(t *testing.T) {
	overflowPart := mustStatsTestPart(t, []int64{1<<63 - 1, 1}, EncodingDeltaVarint)
	stats, ok := overflowPart.ColumnStats.Int64Column("value")
	if !ok {
		t.Fatalf("missing overflow stats")
	}
	block, ok := stats.Block(0)
	if !ok {
		t.Fatalf("missing block stats")
	}
	if block.SumValid {
		t.Fatalf("overflow block stats=%+v should not advertise valid sum", block)
	}
	if ok, reason := block.CanAnswer(ColumnStatsOpSum); ok || reason != ColumnStatsReasonSumOverflow {
		t.Fatalf("overflow CanAnswer ok=%v reason=%s", ok, reason)
	}
	if stats.Envelope.SupportsOperation(ColumnStatsOpSum) {
		t.Fatalf("single overflowing block stats should not advertise sum op: %+v", stats.Envelope.Operations)
	}

	partOverflowOnly := mustStatsTestPartWithBlockRows(t, []int64{1<<63 - 1, 1}, EncodingRawInt64, 1)
	partOverflowStats, ok := partOverflowOnly.ColumnStats.Int64Column("value")
	if !ok {
		t.Fatalf("missing part-overflow stats")
	}
	if partOverflowStats.SumValid {
		t.Fatalf("part-overflow stats should not have a valid part sum: %+v", partOverflowStats)
	}
	if !partOverflowStats.Envelope.SupportsSelectionShape(ColumnStatsSelectionAllRows) {
		t.Fatalf("part-overflow stats should still advertise all-rows shape for count operations: %+v", partOverflowStats.Envelope.SelectionShapes)
	}
	if ok, reason := partOverflowStats.CanAnswer(ColumnStatsOpSum, ColumnStatsSelectionAllRows); ok || reason != ColumnStatsReasonSumOverflow {
		t.Fatalf("part-overflow all-rows sum CanAnswer ok=%v reason=%s", ok, reason)
	}
	if ok, reason := partOverflowStats.CanAnswer(ColumnStatsOpCountRows, ColumnStatsSelectionAllRows); !ok || reason != ColumnStatsReasonSupported {
		t.Fatalf("part-overflow all-rows count CanAnswer ok=%v reason=%s", ok, reason)
	}
	if !partOverflowStats.Envelope.SupportsOperation(ColumnStatsOpSum) {
		t.Fatalf("part-overflow stats should still advertise block sum op: %+v", partOverflowStats.Envelope.Operations)
	}
	for i := range partOverflowStats.Blocks {
		if ok, reason := partOverflowStats.Blocks[i].CanAnswer(ColumnStatsOpSum); !ok || reason != ColumnStatsReasonSupported {
			t.Fatalf("block %d CanAnswer sum ok=%v reason=%s stats=%+v", i, ok, reason, partOverflowStats.Blocks[i])
		}
	}

	part, err := BuildColumnPart(1, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64},
			{Name: "maybe", Type: ColumnTypeInt64, Encoding: EncodingNullableInt64},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 4, DefaultCodecBlockRows: 4},
	}, Batch{
		Rows:          3,
		Columns:       map[string][]int64{"id": []int64{1, 2, 3}, "maybe": []int64{10, 20, 30}},
		Nulls:         map[string][]bool{"maybe": []bool{false, true, false}},
		Defaults:      map[string][]bool{"maybe": []bool{false, false, true}},
		DefaultValues: map[string]int64{"maybe": 7},
	})
	if err != nil {
		t.Fatalf("Build nullable part: %v", err)
	}
	if _, ok := part.ColumnStats.Int64Column("maybe"); ok {
		t.Fatalf("nullable/default int64 carrier must not emit value sum stats: %+v", part.ColumnStats)
	}
}

func TestColumnStatsSkipsNonFullyVisiblePart(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *ColumnPart)
	}{
		{
			name: "part visible row count",
			mutate: func(t *testing.T, part *ColumnPart) {
				part.Descriptor.VisibleRowCount = part.Descriptor.RowCount - 1
			},
		},
		{
			name: "granule visibility",
			mutate: func(t *testing.T, part *ColumnPart) {
				if len(part.Descriptor.Granules) == 0 {
					t.Fatalf("test part has no granules")
				}
				part.Descriptor.Granules[0].VisibleRows = part.Descriptor.Granules[0].RowCount - 1
				part.Descriptor.Granules[0].DeletedRows = 1
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			part := mustStatsTestPart(t, []int64{1, 2, 3}, EncodingDeltaVarint)
			tt.mutate(t, part)
			stats, err := buildColumnPartStats(part)
			if err != nil {
				t.Fatalf("buildColumnPartStats non-visible: %v", err)
			}
			if !stats.Empty() {
				t.Fatalf("non-fully-visible part emitted stats: %+v", stats)
			}
		})
	}
}

func TestColumnStatsDisabledDefinitionSkipsInt64Payload(t *testing.T) {
	part, err := BuildColumnPart(1, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, StatsDisabled: true},
			{Name: "raw_float_bits", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, StatsDisabled: true},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 4, DefaultCodecBlockRows: 4},
	}, Batch{Rows: 3, Columns: map[string][]int64{"id": []int64{1, 2, 3}, "raw_float_bits": []int64{0, 1, 2}}})
	if err != nil {
		t.Fatalf("Build disabled stats part: %v", err)
	}
	if !part.ColumnStats.Empty() {
		t.Fatalf("disabled int64 carriers emitted stats: %+v", part.ColumnStats)
	}
}

func TestColumnPartImageWithoutStatsRemainsReadable(t *testing.T) {
	part := mustStatsTestPart(t, []int64{1, 2, 3}, EncodingDeltaVarint)
	part.ColumnStats = ColumnPartStats{}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{"id": "int64", "value": "int64"}})
	if err != nil {
		t.Fatalf("BuildColumnPartImage without stats: %v", err)
	}
	if _, ok, err := image.ColumnStatsSection(); err != nil || ok {
		t.Fatalf("ColumnStatsSection ok=%v err=%v want absent", ok, err)
	}
	decoded, err := ColumnPartFromImage(image)
	if err != nil {
		t.Fatalf("ColumnPartFromImage without stats: %v", err)
	}
	if !decoded.ColumnStats.Empty() {
		t.Fatalf("decoded stats=%+v want empty", decoded.ColumnStats)
	}
}

func mustStatsTestPart(t *testing.T, values []int64, encoding Encoding) *ColumnPart {
	t.Helper()
	return mustStatsTestPartWithBlockRows(t, values, encoding, len(values))
}

func mustStatsTestPartWithBlockRows(t *testing.T, values []int64, encoding Encoding, blockRows int) *ColumnPart {
	t.Helper()
	ids := make([]int64, len(values))
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	part, err := BuildColumnPart(1, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64},
			{Name: "value", Type: ColumnTypeInt64, Encoding: encoding},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: blockRows, DefaultCodecBlockRows: blockRows},
	}, Batch{Rows: len(values), Columns: map[string][]int64{"id": ids, "value": values}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	return part
}
