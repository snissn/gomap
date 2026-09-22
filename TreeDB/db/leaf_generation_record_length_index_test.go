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

func TestLeafGenerationRecordLengthIndex_NoteRawIsCopyOnWrite(t *testing.T) {
	db := &DB{}
	seed := &leafGenerationRecordLengthIndex{
		offsets: []uint32{4},
		lengths: []uint32{96},
	}
	db.storeLeafGenerationRecordLengthIndex(7, seed)

	before, ok := db.loadLeafGenerationRecordLengthIndex(7)
	if !ok || before == nil {
		t.Fatal("expected cached record-length index")
	}
	db.noteLeafGenerationRecordLengthRaw(7, 128, 144)

	after, ok := db.loadLeafGenerationRecordLengthIndex(7)
	if !ok || after == nil {
		t.Fatal("expected updated cached record-length index")
	}
	if got, ok := before.lookup(128); ok || got != 0 {
		t.Fatalf("stale reader lookup(128)=(%d,%v), want (0,false)", got, ok)
	}
	if got, ok := after.lookup(128); !ok || got != 144 {
		t.Fatalf("updated reader lookup(128)=(%d,%v), want (144,true)", got, ok)
	}
}

func TestLeafGenerationRecordLengthIndex_NoteRawAllowsOffsetZero(t *testing.T) {
	db := &DB{}
	db.noteLeafGenerationRecordLengthRaw(9, 0, 64)
	idx, ok := db.loadLeafGenerationRecordLengthIndex(9)
	if !ok || idx == nil {
		t.Fatal("expected record-length index for offset-zero entry")
	}
	if got, ok := idx.lookup(0); !ok || got != 64 {
		t.Fatalf("lookup(0)=(%d,%v), want (64,true)", got, ok)
	}
}
