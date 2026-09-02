package keyupdate

import (
	"testing"

	"github.com/cespare/xxhash/v2"
)

func TestGuardUnlockZeroValueNoop(t *testing.T) {
	var guard Guard
	guard.Unlock()
}

func TestLockSerializesSameKey(t *testing.T) {
	var locks Locks
	key := []byte("same-key")

	first := locks.Lock(key)
	idx := xxhash.Sum64(key) & (stripes - 1)
	if locks.stripes[idx].TryLock() {
		locks.stripes[idx].Unlock()
		first.Unlock()
		t.Fatal("stripe for same key was not locked")
	}
	first.Unlock()

	second := locks.Lock(key)
	second.Unlock()
}

func TestLockUnlockNoAlloc(t *testing.T) {
	var locks Locks
	key := []byte("hot-key")
	allocs := testing.AllocsPerRun(1000, func() {
		guard := locks.Lock(key)
		guard.Unlock()
	})
	if allocs != 0 {
		t.Fatalf("Lock/Unlock allocs = %v, want 0", allocs)
	}
}

func TestLockUnlockWithDeferNoAlloc(t *testing.T) {
	var locks Locks
	key := []byte("hot-key")
	allocs := testing.AllocsPerRun(1000, func() {
		lockUnlockWithDefer(&locks, key)
	})
	if allocs != 0 {
		t.Fatalf("deferred Lock/Unlock allocs = %v, want 0", allocs)
	}
}

func lockUnlockWithDefer(locks *Locks, key []byte) {
	guard := locks.Lock(key)
	defer guard.Unlock()
}

func BenchmarkLockUnlock(b *testing.B) {
	var locks Locks
	key := []byte("hot-key")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		guard := locks.Lock(key)
		guard.Unlock()
	}
}

func BenchmarkLockUnlockParallel(b *testing.B) {
	var locks Locks
	key := []byte("hot-key")
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			guard := locks.Lock(key)
			guard.Unlock()
		}
	})
}
