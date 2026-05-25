package typedcolumn

import (
	"strings"
	"testing"
)

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
