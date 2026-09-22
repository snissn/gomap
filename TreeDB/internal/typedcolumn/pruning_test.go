package typedcolumn

import (
	"reflect"
	"strings"
	"testing"
)

func TestEncodeInt64PruningPayloadSizeAndRoundTrip(t *testing.T) {
	index := Int64ValueRowIndex{
		Rows:         4,
		NullCount:    1,
		DefaultCount: 2,
		Blocks: []Int64PruningBlock{
			{Index: 0, FirstRow: 0, RowCount: 2, HasMinMax: true, Min: -2, Max: 5},
			{Index: 1, FirstRow: 2, RowCount: 2, HasMinMax: true, Min: 3, Max: 8},
		},
		Entries: []Int64PruningEntry{{Value: -2, Row: 0}, {Value: 3, Row: 2}, {Value: 5, Row: 1}, {Value: 8, Row: 3}},
	}
	payload, err := encodeInt64PruningPayload(index)
	if err != nil {
		t.Fatalf("encodeInt64PruningPayload: %v", err)
	}
	if got, want := len(payload), 36+len(index.Blocks)*int64PruningBlockEncodedBytes+len(index.Entries)*16; got != want {
		t.Fatalf("payload bytes=%d want %d", got, want)
	}
	if cap(payload) != len(payload) {
		t.Fatalf("payload capacity=%d want exact size=%d", cap(payload), len(payload))
	}
	decoded, err := decodeInt64PruningPayload(ColumnPruningEnvelope{}, payload)
	if err != nil {
		t.Fatalf("decodeInt64PruningPayload: %v", err)
	}
	if decoded.Rows != index.Rows || decoded.NullCount != index.NullCount || decoded.DefaultCount != index.DefaultCount || !reflect.DeepEqual(decoded.Blocks, index.Blocks) || !reflect.DeepEqual(decoded.Entries, index.Entries) {
		t.Fatalf("decoded=%+v want=%+v", decoded, index)
	}
}

func TestColumnPruningInt64ValueRowsRoundTrip(t *testing.T) {
	part := mustStatsTestPartWithBlockRows(t, []int64{5, 1, 5, 9, 5, 2}, EncodingDeltaVarint, 3)
	index, ok := part.PruningMetadata.Int64Column("value")
	if !ok {
		t.Fatalf("missing int64 pruning metadata for value")
	}
	if !index.Envelope.SupportsOperation(ColumnPruningOpEquality) || !index.Envelope.SupportsOperation(ColumnPruningOpOrderedRange) {
		t.Fatalf("operations=%v", index.Envelope.Operations)
	}
	plan, err := index.PlanInt64Predicate(Int64PruningPredicate{Kind: Int64PruningPredicateEqual, Value: 5})
	if err != nil {
		t.Fatalf("PlanInt64Predicate: %v", err)
	}
	if got, want := plan.CandidateRows, 3; got != want {
		t.Fatalf("candidate rows=%d want %d", got, want)
	}
	if got, want := plan.ExactCount, int64(3); got != want || plan.ExactSum != 15 {
		t.Fatalf("exact count/sum=%d/%d want %d/%d", got, plan.ExactSum, want, int64(15))
	}
	assertPruningSelectionRows(t, plan.Blocks[0].Selection, []int{0, 2})
	assertPruningSelectionRows(t, plan.Blocks[1].Selection, []int{1})

	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{"id": "int64", "value": "int64"}})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	section, ok, err := image.PruningMetadataSection()
	if err != nil || !ok {
		t.Fatalf("PruningMetadataSection ok=%v err=%v", ok, err)
	}
	decoded, err := DecodeColumnPartPruningSection(image.sectionBytes(section))
	if err != nil {
		t.Fatalf("DecodeColumnPartPruningSection: %v", err)
	}
	if err := ValidateColumnPartPruning(decoded, part.Descriptor, part.Columns); err != nil {
		t.Fatalf("ValidateColumnPartPruning: %v", err)
	}
	decodedIndex, ok := decoded.Int64Column("value")
	if !ok {
		t.Fatalf("decoded missing value index")
	}
	decodedPlan, err := decodedIndex.PlanInt64Predicate(Int64PruningPredicate{Kind: Int64PruningPredicateRange, Low: 2, High: 5})
	if err != nil {
		t.Fatalf("decoded range plan: %v", err)
	}
	if got, want := decodedPlan.CandidateRows, 4; got != want {
		t.Fatalf("decoded candidate rows=%d want %d", got, want)
	}
}

func TestColumnPruningCompressedImageSectionRoundTrip(t *testing.T) {
	values := make([]int64, 8192)
	for i := range values {
		values[i] = int64(i % 8)
	}
	part := mustStatsTestPartWithBlockRows(t, values, EncodingDeltaVarint, 512)
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{
		LayoutLogicalTypes: map[string]string{"id": "int64", "value": "int64"},
		SectionCompression: CompressionLZ4,
	})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	section, ok, err := image.PruningMetadataSection()
	if err != nil || !ok {
		t.Fatalf("PruningMetadataSection ok=%v err=%v", ok, err)
	}
	if section.Compression != CompressionLZ4 || section.RawBytes <= section.Length {
		t.Fatalf("pruning metadata section=%+v want kept lz4 compression", section)
	}
	parsed, err := ParseColumnPartImage(image.Bytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	reopened, err := ColumnPartFromImage(parsed)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	index, ok := reopened.PruningMetadata.Int64Column("value")
	if !ok {
		t.Fatalf("reopened pruning metadata missing value column")
	}
	plan, err := index.PlanInt64Predicate(Int64PruningPredicate{Kind: Int64PruningPredicateEqual, Value: 3})
	if err != nil {
		t.Fatalf("PlanInt64Predicate: %v", err)
	}
	if got, want := plan.CandidateRows, len(values)/8; got != want {
		t.Fatalf("candidate rows=%d want %d", got, want)
	}

	corrupt := parsed
	corrupt.Sections = append([]ColumnPartImageSection(nil), parsed.Sections...)
	for i := range corrupt.Sections {
		if corrupt.Sections[i].Kind == ColumnPartImageSectionPruningMetadata {
			corrupt.Sections[i].RawBytes--
			break
		}
	}
	if _, err := ColumnPartFromImage(corrupt); err == nil || !strings.Contains(err.Error(), "pruning metadata") {
		t.Fatalf("ColumnPartFromImage corrupt pruning metadata err=%v want pruning metadata raw-length failure", err)
	}
}

func TestColumnPruningAllPredicateDoesNotScanValueRows(t *testing.T) {
	part := mustStatsTestPartWithBlockRows(t, []int64{3, 4, 5, 6}, EncodingRawInt64, 2)
	index, ok := part.PruningMetadata.Int64Column("value")
	if !ok {
		t.Fatalf("missing int64 pruning metadata for value")
	}
	index.Entries = nil
	plan, err := index.PlanInt64Predicate(Int64PruningPredicate{Kind: Int64PruningPredicateAll})
	if err != nil {
		t.Fatalf("PlanInt64Predicate all: %v", err)
	}
	if plan.Exact || plan.CandidateRows != 4 || plan.CandidateBlocks != 2 || plan.PrunedBlocks != 0 {
		t.Fatalf("all plan=%+v want all rows/blocks without exact value scan", plan)
	}
	for _, block := range plan.Blocks {
		if !block.Selection.IsAll() || block.NeedsPredicate || block.Exact {
			t.Fatalf("all block candidate=%+v want all non-exact selection", block)
		}
	}
}

func TestColumnPruningDisabledDefinitionSkipsInt64Payload(t *testing.T) {
	part, err := BuildColumnPart(1, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, StatsDisabled: true},
			{Name: "value", Type: ColumnTypeInt64, Encoding: EncodingDeltaVarint, StatsDisabled: true},
			{Name: "raw_float_bits", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, StatsDisabled: true},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 4, DefaultCodecBlockRows: 4},
	}, Batch{Rows: 3, Columns: map[string][]int64{"id": {1, 2, 3}, "value": {10, 20, 30}, "raw_float_bits": {0x3f800000, 0x40000000, 0x40400000}}})
	if err != nil {
		t.Fatalf("Build disabled pruning part: %v", err)
	}
	if !part.PruningMetadata.Empty() {
		t.Fatalf("disabled int64 carriers emitted pruning metadata: %+v", part.PruningMetadata)
	}

	present := mustStatsTestPartWithBlockRows(t, []int64{1, 2, 3}, EncodingDeltaVarint, 3)
	index, ok := present.PruningMetadata.Int64Column("value")
	if !ok {
		t.Fatalf("missing value pruning metadata")
	}
	column := present.Columns["value"]
	column.Definition.StatsDisabled = true
	columnDesc := ColumnPartColumnDescriptor{}
	for _, desc := range present.Descriptor.Columns {
		if desc.Name == "value" {
			columnDesc = desc
			break
		}
	}
	if columnDesc.Name == "" {
		t.Fatalf("missing value descriptor")
	}
	if err := ValidateInt64ValueRowIndex(index, present.Descriptor, columnDesc, column); err == nil || !strings.Contains(err.Error(), ColumnPruningReasonUnsupportedPayload) {
		t.Fatalf("Validate disabled pruning err=%v want unsupported-payload fail-closed", err)
	}
}

func TestColumnPruningPayloadChecksumFailsClosed(t *testing.T) {
	part := mustStatsTestPartWithBlockRows(t, []int64{3, 4, 5, 6}, EncodingRawInt64, 2)
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{"id": "int64", "value": "int64"}})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	section, ok, err := image.PruningMetadataSection()
	if err != nil || !ok {
		t.Fatalf("PruningMetadataSection ok=%v err=%v", ok, err)
	}
	raw := append([]byte(nil), image.sectionBytes(section)...)
	raw[len(raw)-1] ^= 0x80
	_, err = DecodeColumnPartPruningSection(raw)
	if err == nil || !strings.Contains(err.Error(), ColumnPruningReasonChecksumMismatch) {
		t.Fatalf("DecodeColumnPartPruningSection err=%v want checksum mismatch", err)
	}
}

func assertPruningSelectionRows(t testing.TB, selection RowSelection, want []int) {
	t.Helper()
	got := make([]int, 0, selection.Count())
	for row := 0; row < selection.Rows(); row++ {
		if selection.Contains(row) {
			got = append(got, row)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("selection rows=%v want %v shape=%+v", got, want, selection.Shape())
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("selection rows=%v want %v shape=%+v", got, want, selection.Shape())
		}
	}
}
