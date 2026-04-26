package keyupdate

import "testing"

func TestLockMutexDoesNotAllocate(t *testing.T) {
	var locks Locks
	key := []byte("hot-key")

	allocs := testing.AllocsPerRun(1000, func() {
		mu := locks.LockMutex(key)
		mu.Unlock()
	})
	if allocs != 0 {
		t.Fatalf("LockMutex allocs/run=%v want 0", allocs)
	}
}
