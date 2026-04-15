package db

import "testing"

func TestLeafGenerationRecordLengthIndex_LookupWithHintLocality(t *testing.T) {
	idx := &leafGenerationRecordLengthIndex{
		offsets: []uint32{10, 20, 30, 40, 50, 60},
		lengths: []uint32{100, 200, 300, 400, 500, 600},
	}

	if got, hint, ok := idx.lookupWithHint(30, 2); !ok || got != 300 || hint != 2 {
		t.Fatalf("exact hint lookup = (%d,%d,%v), want (300,2,true)", got, hint, ok)
	}
	if got, hint, ok := idx.lookupWithHint(40, 2); !ok || got != 400 || hint != 3 {
		t.Fatalf("forward local lookup = (%d,%d,%v), want (400,3,true)", got, hint, ok)
	}
	if got, hint, ok := idx.lookupWithHint(20, 3); !ok || got != 200 || hint != 1 {
		t.Fatalf("backward local lookup = (%d,%d,%v), want (200,1,true)", got, hint, ok)
	}
	if got, hint, ok := idx.lookupWithHint(35, 2); ok || got != 0 || hint != 3 {
		t.Fatalf("midpoint miss = (%d,%d,%v), want (0,3,false)", got, hint, ok)
	}
	if got, hint, ok := idx.lookupWithHint(5, 2); ok || got != 0 || hint != 0 {
		t.Fatalf("low miss = (%d,%d,%v), want (0,0,false)", got, hint, ok)
	}
}
