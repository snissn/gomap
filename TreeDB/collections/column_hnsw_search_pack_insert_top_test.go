package collections

import (
	"reflect"
	"testing"
)

func TestColumnHNSWSearchPackInsertTopMatchesGenericOrder2445(t *testing.T) {
	candidates := []columnVectorGraphSearchCandidate{
		{ordinal: 5, score: 0.80},
		{ordinal: 3, score: 0.90},
		{ordinal: 7, score: 0.90},
		{ordinal: 2, score: 0.90},
		{ordinal: 9, score: 0.10},
		{ordinal: 1, score: 0.95},
		{ordinal: 8, score: 0.90},
		{ordinal: 4, score: 0.85},
		{ordinal: 6, score: 0.80},
		{ordinal: 0, score: 0.95},
		{ordinal: 10, score: 0.70},
		{ordinal: 11, score: 0.85},
	}

	for _, limit := range []int{0, 1, 3, 5, 8, len(candidates)} {
		var fast columnVectorGraphNativeSearchScratch
		var generic columnVectorGraphNativeSearchScratch
		for step, candidate := range candidates {
			fastInserted := columnHNSWSearchPackInsertTop(&fast, limit, candidate)
			genericInserted := generic.insertTop(limit, candidate)
			if fastInserted != genericInserted {
				t.Fatalf("limit=%d step=%d inserted mismatch: fast=%v generic=%v", limit, step, fastInserted, genericInserted)
			}
			if len(fast.top) > limit {
				t.Fatalf("limit=%d step=%d fast top len=%d exceeds limit", limit, step, len(fast.top))
			}
			sortedFast := append([]columnVectorGraphSearchCandidate(nil), fast.top...)
			columnHNSWSearchPackSortTop(sortedFast)
			if !reflect.DeepEqual(sortedFast, generic.top) {
				t.Fatalf("limit=%d step=%d top order mismatch:\nfast(sorted)=%+v\ngeneric=%+v\nfast(heap)=%+v", limit, step, sortedFast, generic.top, fast.top)
			}
			if limit > 0 && len(fast.top) >= limit && !reflect.DeepEqual(fast.top[0], generic.top[len(generic.top)-1]) {
				t.Fatalf("limit=%d step=%d heap root mismatch: root=%+v worst=%+v", limit, step, fast.top[0], generic.top[len(generic.top)-1])
			}
			for _, probe := range []columnVectorGraphSearchCandidate{
				{ordinal: 99, score: -1.00},
				{ordinal: 98, score: 0.80},
				{ordinal: 97, score: 0.90},
				{ordinal: 96, score: 0.95},
				{ordinal: -1, score: 2.00},
			} {
				fastStop := columnHNSWSearchPackLayer0SearchShouldStop(probe, fast.top, limit)
				genericStop := columnVectorGraphLayer0SearchShouldStop(probe, generic.top, limit)
				if fastStop != genericStop {
					t.Fatalf("limit=%d step=%d stop mismatch for probe=%+v: fast=%v generic=%v heap=%+v sorted=%+v", limit, step, probe, fastStop, genericStop, fast.top, generic.top)
				}
			}
		}
		for _, topK := range []int{1, 2, 5, limit, limit + 1} {
			if topK <= 0 {
				continue
			}
			leftoverFrontier := []columnVectorGraphSearchCandidate{
				{ordinal: 101, score: 101},
				{ordinal: 102, score: 102},
			}
			selected := columnVectorGraphNativeSearchScratch{
				top:      append([]columnVectorGraphSearchCandidate(nil), fast.top...),
				frontier: append(make([]columnVectorGraphSearchCandidate, 0, topK+len(leftoverFrontier)), leftoverFrontier...),
			}
			columnHNSWSearchPackSelectBestTop(&selected, topK)
			expected := append([]columnVectorGraphSearchCandidate(nil), generic.top...)
			if topK > 0 && len(expected) > topK {
				expected = expected[:topK]
			}
			if !reflect.DeepEqual(selected.top, expected) {
				t.Fatalf("limit=%d topK=%d selected top mismatch with leftover frontier:\nselected=%+v\nexpected=%+v", limit, topK, selected.top, expected)
			}
			if len(fast.top) > topK && !reflect.DeepEqual(selected.frontier, expected) {
				t.Fatalf("limit=%d topK=%d selected frontier scratch mismatch:\nfrontier=%+v\nexpected=%+v", limit, topK, selected.frontier, expected)
			}

			fallbackSelected := columnVectorGraphNativeSearchScratch{top: append([]columnVectorGraphSearchCandidate(nil), fast.top...)}
			columnHNSWSearchPackSelectBestTop(&fallbackSelected, topK)
			if !reflect.DeepEqual(fallbackSelected.top, expected) {
				t.Fatalf("limit=%d topK=%d fallback selected top mismatch:\nselected=%+v\nexpected=%+v", limit, topK, fallbackSelected.top, expected)
			}
		}
	}
}
