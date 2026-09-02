package typedcolumn

import "testing"

func TestSortKeyMarkRangeBoundaries1949(t *testing.T) {
	mark, err := BuildSortKeyMark([]SortKeyColumnValues{
		{Name: "kind", Values: []int64{1, 1, 1, 1}},
		{Name: "operation", Values: []int64{2, 2, 3, 3}},
		{Name: "collection", Values: []int64{10, 20, 20, 30}},
	})
	if err != nil {
		t.Fatalf("BuildSortKeyMark: %v", err)
	}
	cases := []struct {
		name        string
		ranges      []Int64RangePredicate
		mayContain  bool
		constrained bool
	}{
		{name: "empty range", ranges: []Int64RangePredicate{{Column: "kind", Low: 2, High: 1}}, mayContain: false, constrained: true},
		{name: "lower boundary equality", ranges: []Int64RangePredicate{{Column: "kind", Low: 1, High: 1}, {Column: "operation", Low: 2, High: 2}, {Column: "collection", Low: 10, High: 10}}, mayContain: true, constrained: true},
		{name: "upper boundary equality", ranges: []Int64RangePredicate{{Column: "kind", Low: 1, High: 1}, {Column: "operation", Low: 3, High: 3}, {Column: "collection", Low: 30, High: 30}}, mayContain: true, constrained: true},
		{name: "above exclusive upper", ranges: []Int64RangePredicate{{Column: "kind", Low: 1, High: 1}, {Column: "operation", Low: 3, High: 3}, {Column: "collection", Low: 31, High: 31}}, mayContain: false, constrained: true},
		{name: "multi-column prefix miss before lower", ranges: []Int64RangePredicate{{Column: "kind", Low: 1, High: 1}, {Column: "operation", Low: 1, High: 1}}, mayContain: false, constrained: true},
		{name: "unconstrained non-prefix", ranges: []Int64RangePredicate{{Column: "collection", Low: 20, High: 20}}, mayContain: true, constrained: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mayContain, constrained, err := mark.MayContainRanges(tc.ranges)
			if err != nil {
				t.Fatalf("MayContainRanges: %v", err)
			}
			if mayContain != tc.mayContain || constrained != tc.constrained {
				t.Fatalf("MayContainRanges=(%v,%v) want (%v,%v)", mayContain, constrained, tc.mayContain, tc.constrained)
			}
		})
	}
}
