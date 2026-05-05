package keyupdate

import "testing"

func TestLocksLockDoesNotAllocate(t *testing.T) {
	var locks Locks
	key := []byte("alloc-key")

	allocs := testing.AllocsPerRun(1000, func() {
		unlock := locks.Lock(key)
		unlock.Unlock()
	})
	if allocs != 0 {
		t.Fatalf("Lock allocations = %v, want 0", allocs)
	}
}

func TestUnlockerZeroValueIsNoop(t *testing.T) {
	var unlock Unlocker
	unlock.Unlock()
}

func BenchmarkLocksLockUnlock(b *testing.B) {
	var locks Locks
	key := []byte("bench-key")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		unlock := locks.Lock(key)
		unlock.Unlock()
	}
}
