package collections

import (
	"errors"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

func TestColumnHNSWSearchPackWavefront4158(t *testing.T) {
	input := testColumnHNSWSearchPackInput2312()
	raw, err := encodeColumnHNSWSearchPack(input)
	if err != nil {
		t.Fatal(err)
	}
	view, _ := testColumnHNSWSearchPackPreparedViewFromBytes2314(t, raw, mappedresource.SourceHeapCopy, input.BaseIdentity)
	defer view.Close()
	opts := columnVectorGraphNativeSearchOptions{TopK: 2, EfSearch: 2, TraversalMode: columnVectorGraphNativeSearchTraversalModeWavefront, WavefrontWidth: 2}
	first, firstStats, err := view.searchCosine([]float32{1, 0, 0}, opts, &columnVectorGraphNativeSearchScratch{})
	if err != nil {
		t.Fatal(err)
	}
	second, secondStats, err := view.searchCosine([]float32{1, 0, 0}, opts, &columnVectorGraphNativeSearchScratch{})
	if err != nil || !reflect.DeepEqual(first, second) || firstStats.Candidates != secondStats.Candidates || firstStats.Edges != secondStats.Edges {
		t.Fatalf("first=%+v/%+v second=%+v/%+v err=%v", first, firstStats, second, secondStats, err)
	}
	if firstStats.WavefrontSearches != 1 || firstStats.WavefrontWidth != 2 || firstStats.WavefrontRounds == 0 || firstStats.Candidates > 2 {
		t.Fatalf("wavefront stats=%+v", firstStats)
	}
	bad := opts
	bad.WavefrontWidth = 3
	if _, _, err := view.searchCosine([]float32{1, 0, 0}, bad, &columnVectorGraphNativeSearchScratch{}); !errors.Is(err, errColumnVectorGraphNativeSearchWavefrontWidthInvalid) {
		t.Fatalf("width above ef err=%v", err)
	}
}
