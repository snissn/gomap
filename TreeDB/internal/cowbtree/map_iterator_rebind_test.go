package cowbtree

import "testing"

func TestMapIteratorResetTreeReusesAndClearsBacking(t *testing.T) {
	first := NewMap[uint64, int](32)
	for i := 0; i < 500; i++ {
		first.Set(uint64(i), i)
	}
	iter := first.ImmutableCopy().Iter()
	if !iter.Last() {
		t.Fatal("initial position")
	}
	capacity := cap(iter.stack)
	iter.ResetTree(nil)
	if cap(iter.stack) != capacity || len(iter.stack) != 0 || iter.First() {
		t.Fatal("nil rebind state")
	}
	for _, frame := range iter.stack[:cap(iter.stack)] {
		if frame.n != nil {
			t.Fatal("prior tree edge retained")
		}
	}
	second := NewMap[uint64, int](32)
	second.Set(99, 42)
	second = second.ImmutableCopy()
	for i := 0; i < 100; i++ {
		iter.ResetTree(second)
		if !iter.First() || iter.Key() != 99 || iter.Value() != 42 || cap(iter.stack) != capacity {
			t.Fatal("rebound cursor")
		}
		iter.ResetTree(nil)
	}
}
