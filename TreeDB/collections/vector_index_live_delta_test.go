package collections

import (
	"testing"
)

func TestMergeVectorIndexViewResultsDeduplicatesExhaustedPlanes(t *testing.T) {
	base := []VectorIndexSearchResult{{ID: []byte("a"), Score: 0.9}, {ID: []byte("b"), Score: 0.7}}
	delta := []VectorIndexSearchResult{{ID: []byte("a"), Score: 0.8}}
	var buffer VectorIndexSearchBuffer
	got, err := mergeVectorIndexViewResults(base, delta, 4, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || string(got[0].ID) != "a" || string(got[1].ID) != "b" {
		t.Fatalf("merged results=%+v want unique a,b", got)
	}
}
