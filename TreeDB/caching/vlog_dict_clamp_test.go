package caching

import "testing"

func TestClampValueLogDictK_RespectsConfiguredMax(t *testing.T) {
	db := &DB{valueLogDictMaxK: 32}
	if got := db.clampValueLogDictK(128); got != 32 {
		t.Fatalf("clampValueLogDictK: got=%d want=%d", got, 32)
	}
	if got := db.clampValueLogDictK(16); got != 16 {
		t.Fatalf("clampValueLogDictK: got=%d want=%d", got, 16)
	}
	if got := db.clampValueLogDictK(0); got != 1 {
		t.Fatalf("clampValueLogDictK: got=%d want=%d", got, 1)
	}
}
