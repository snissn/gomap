package db

import "testing"

func TestLockUpdateKeyNoAlloc(t *testing.T) {
	var d DB
	key := []byte("hot-key")
	allocs := testing.AllocsPerRun(1000, func() {
		guard := d.lockUpdateKey(key)
		guard.Unlock()
	})
	if allocs != 0 {
		t.Fatalf("lockUpdateKey allocs = %v, want 0", allocs)
	}
}

func TestLockUpdateKeyWithDeferNoAlloc(t *testing.T) {
	var d DB
	key := []byte("hot-key")
	allocs := testing.AllocsPerRun(1000, func() {
		lockUpdateKeyWithDefer(&d, key)
	})
	if allocs != 0 {
		t.Fatalf("deferred lockUpdateKey allocs = %v, want 0", allocs)
	}
}

func lockUpdateKeyWithDefer(d *DB, key []byte) {
	guard := d.lockUpdateKey(key)
	defer guard.Unlock()
}

func BenchmarkLockUpdateKey(b *testing.B) {
	var d DB
	key := []byte("hot-key")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		guard := d.lockUpdateKey(key)
		guard.Unlock()
	}
}

func BenchmarkLockUpdateKeyWithDefer(b *testing.B) {
	var d DB
	key := []byte("hot-key")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		lockUpdateKeyWithDefer(&d, key)
	}
}
