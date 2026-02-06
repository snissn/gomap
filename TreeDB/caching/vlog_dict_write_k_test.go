package caching

import "testing"

func TestChooseValueLogDictWriteK_SmallValuesBoostK(t *testing.T) {
	db := &DB{}
	got := db.chooseValueLogDictWriteK(8, 100, 12800) // avg=128
	if got != 16 {
		t.Fatalf("chooseValueLogDictWriteK small values: got=%d want=16", got)
	}
}

func TestChooseValueLogDictWriteK_WALOffUsesAggressiveK(t *testing.T) {
	db := &DB{disableJournal: true}
	got := db.chooseValueLogDictWriteK(8, 100, 12800) // avg=128
	if got != 96 {
		t.Fatalf("chooseValueLogDictWriteK wal-off: got=%d want=96", got)
	}
}

func TestChooseValueLogDictWriteK_RespectsConfiguredMax(t *testing.T) {
	db := &DB{valueLogDictMaxK: 64}
	got := db.chooseValueLogDictWriteK(8, 100, 12800) // avg=128 -> wants 96, clamps to 64
	if got != 16 {
		t.Fatalf("chooseValueLogDictWriteK clamp: got=%d want=16", got)
	}
}

func TestChooseValueLogDictWriteK_WALOffRespectsConfiguredMax(t *testing.T) {
	db := &DB{disableJournal: true, valueLogDictMaxK: 64}
	got := db.chooseValueLogDictWriteK(8, 100, 12800) // avg=128 -> wants 96, clamps to 64
	if got != 64 {
		t.Fatalf("chooseValueLogDictWriteK wal-off clamp: got=%d want=64", got)
	}
}

func TestChooseValueLogDictWriteK_LeavesLargeValuesAlone(t *testing.T) {
	db := &DB{}
	got := db.chooseValueLogDictWriteK(16, 10, 64<<10) // avg ~6.4KiB
	if got != 16 {
		t.Fatalf("chooseValueLogDictWriteK large values: got=%d want=16", got)
	}
}
