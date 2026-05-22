package collections

import (
	"testing"
)

// Tests for columnDictionaryInt64GroupNextScratchCap (utility function in column_dict_int64_query.go).

func TestColumnDictionaryInt64GroupNextScratchCapDoublesBelowMaxM1634(t *testing.T) {
	cases := []struct {
		current  int
		required int
		wantMin  int // returned value must be >= required
	}{
		{current: 0, required: 1, wantMin: 1},
		{current: 0, required: 4, wantMin: 4},
		{current: 2, required: 3, wantMin: 3},
		{current: 4, required: 5, wantMin: 5},
		{current: 8, required: 9, wantMin: 9},
		{current: 16, required: 16, wantMin: 16},
	}
	for _, tc := range cases {
		got := columnDictionaryInt64GroupNextScratchCap(tc.current, tc.required)
		if got < tc.wantMin {
			t.Errorf("current=%d required=%d: got=%d want>=%d", tc.current, tc.required, got, tc.wantMin)
		}
		if got > columnDictionaryInt64GroupMaxGroups {
			t.Errorf("current=%d required=%d: got=%d exceeds max=%d", tc.current, tc.required, got, columnDictionaryInt64GroupMaxGroups)
		}
	}
}

func TestColumnDictionaryInt64GroupNextScratchCapCapsAtMaxM1634(t *testing.T) {
	// When current is near or at the max, result should be capped.
	got := columnDictionaryInt64GroupNextScratchCap(columnDictionaryInt64GroupMaxGroups, columnDictionaryInt64GroupMaxGroups)
	if got != columnDictionaryInt64GroupMaxGroups {
		t.Fatalf("expected cap=%d, got %d", columnDictionaryInt64GroupMaxGroups, got)
	}
}

func TestColumnDictionaryInt64GroupNextScratchCapAlwaysAtLeast4M1634(t *testing.T) {
	// Even with zero current and zero required, cap should be at least 4.
	got := columnDictionaryInt64GroupNextScratchCap(0, 0)
	if got < 4 {
		t.Fatalf("expected at least 4, got %d", got)
	}
}

func TestColumnDictionaryInt64GroupNextScratchCapRequiredDominatesM1634(t *testing.T) {
	// If required > 2*current, required should dominate.
	got := columnDictionaryInt64GroupNextScratchCap(4, 100)
	if got < 100 {
		t.Fatalf("required=100 but got=%d", got)
	}
}

func TestColumnDictionaryInt64GroupNextScratchCapLargeCurrentM1634(t *testing.T) {
	// Large current just below max: result should be max.
	largeBelow := columnDictionaryInt64GroupMaxGroups / 2
	got := columnDictionaryInt64GroupNextScratchCap(largeBelow, largeBelow+1)
	if got > columnDictionaryInt64GroupMaxGroups {
		t.Fatalf("result=%d exceeds max=%d", got, columnDictionaryInt64GroupMaxGroups)
	}
}

// Tests for columnDictionaryInt64GroupRunner.reduceValue (tested indirectly
// via exported types; tests the core aggregation logic).

func TestColumnDictionaryInt64GroupRunnerSeenCodeM1634(t *testing.T) {
	runner := &columnDictionaryInt64GroupRunner{
		kind: ColumnPhysicalQueryGroupMinInt64,
	}
	runner.groupDict = []string{"a", "b", "c"}
	runner.initScratch()

	// Initially nothing seen.
	if runner.seenCode(0) {
		t.Fatal("code 0 should not be seen initially")
	}
	// Reduce a value for code 0.
	runner.reduceValue(0, 42)
	if !runner.seenCode(0) {
		t.Fatal("code 0 should be seen after reduceValue")
	}
	if !runner.seenCode(0) || runner.minValues[0] != 42 {
		t.Fatalf("expected minValues[0]=42, got %d", runner.minValues[0])
	}
	// A smaller value should update min.
	runner.reduceValue(0, 10)
	if runner.minValues[0] != 10 {
		t.Fatalf("expected minValues[0]=10 after smaller value, got %d", runner.minValues[0])
	}
	// A larger value should not update min.
	runner.reduceValue(0, 999)
	if runner.minValues[0] != 10 {
		t.Fatalf("expected minValues[0]=10 unchanged, got %d", runner.minValues[0])
	}
}

func TestColumnDictionaryInt64GroupRunnerMaxKindM1634(t *testing.T) {
	runner := &columnDictionaryInt64GroupRunner{
		kind: ColumnPhysicalQueryGroupMaxInt64,
	}
	runner.groupDict = []string{"x"}
	runner.initScratch()

	runner.reduceValue(0, 5)
	if runner.minValues[0] != 5 {
		t.Fatalf("expected initial max=5, got %d", runner.minValues[0])
	}
	// Smaller should not update max.
	runner.reduceValue(0, 3)
	if runner.minValues[0] != 5 {
		t.Fatalf("expected max unchanged=5, got %d", runner.minValues[0])
	}
	// Larger should update max.
	runner.reduceValue(0, 100)
	if runner.minValues[0] != 100 {
		t.Fatalf("expected max=100, got %d", runner.minValues[0])
	}
}

func TestColumnDictionaryInt64GroupRunnerSpanKindM1634(t *testing.T) {
	runner := &columnDictionaryInt64GroupRunner{
		kind: ColumnPhysicalQueryGroupInt64Span,
	}
	runner.groupDict = []string{"y"}
	runner.initScratch()

	runner.reduceValue(0, 50)
	if runner.minValues[0] != 50 || runner.maxValues[0] != 50 {
		t.Fatalf("expected min=max=50, got min=%d max=%d", runner.minValues[0], runner.maxValues[0])
	}
	runner.reduceValue(0, 10)
	if runner.minValues[0] != 10 {
		t.Fatalf("expected min=10, got %d", runner.minValues[0])
	}
	runner.reduceValue(0, 200)
	if runner.maxValues[0] != 200 {
		t.Fatalf("expected max=200, got %d", runner.maxValues[0])
	}
}