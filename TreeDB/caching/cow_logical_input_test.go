package caching

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

func TestCOWOrderedLogicalStageOwnsCanonicalInput(t *testing.T) {
	db, owner := cowOrderedLifecycleFixture(t)
	b := db.NewBatchWithSize(3)
	defer b.Close()
	logical := []byte{'k', 0, 0xff}
	value := []byte("first-value")
	mutations := []mvccadmission.LogicalMutation{
		{Key: logical, Value: value, Timestamp: 3},
		{Key: logical, Delete: true, Timestamp: 7},
		{Key: nil, Timestamp: 11},
	}
	n, err := b.StageLogicalMVCC(mutations, owner.LogicalInput(mutations, 0))
	if err != nil || b.hasViewOps || len(b.entries) != 3 || n != b.size {
		t.Fatalf("native owned stage: bytes=%d size=%d entries=%d callerViews=%t err=%v", n, b.size, len(b.entries), b.hasViewOps, err)
	}
	for _, entry := range b.entries {
		if _, qualified := entry.MVCCInput.LogicalTimestamp(entry.Key); !qualified {
			t.Fatal("input lost qualification on its owned physical key")
		}
	}
	// The staging borrow ends before Write, so mutate all original operands
	// while the original Store authority still has to consume the owned input.
	logical[0], value[0] = 'X', 'X'
	mutations[1].Timestamp = 99
	if err := b.Write(); err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		key    []byte
		ts     uint64
		record []byte
	}{
		{[]byte{'k', 0, 0xff}, 3, []byte("\x01first-value")},
		{[]byte{'k', 0, 0xff}, 7, []byte{0x02}},
		{nil, 11, []byte{0x01}},
	} {
		physical, err := mvcckey.Encode(want.key, want.ts)
		if err != nil {
			t.Fatal(err)
		}
		got, err := db.Get(physical)
		if err != nil || !bytes.Equal(got, want.record) {
			t.Fatalf("canonical record ts%d: got=%x want=%x err=%v", want.ts, got, want.record, err)
		}
	}
	// Reset must clear the old summary and permit a fresh input on this owner.
	b.Reset()
	mutations = []mvccadmission.LogicalMutation{{Key: []byte("reuse"), Value: []byte("new"), Timestamp: 19}}
	if _, err := b.StageLogicalMVCC(mutations, owner.LogicalInput(mutations, 0)); err != nil {
		t.Fatal(err)
	}
	if err := b.Write(); err != nil {
		t.Fatal(err)
	}
}
