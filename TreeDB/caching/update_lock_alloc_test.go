package caching

import "testing"

func TestLockUpdateKeyNoAlloc(t *testing.T) {
	var db DB
	key := []byte("hot-key")
	allocs := testing.AllocsPerRun(1000, func() {
		guard := db.lockUpdateKey(key)
		guard.Unlock()
	})
	if allocs != 0 {
		t.Fatalf("lockUpdateKey allocs = %v, want 0", allocs)
	}
}

func TestLockUpdateKeyWithDeferNoAlloc(t *testing.T) {
	var db DB
	key := []byte("hot-key")
	allocs := testing.AllocsPerRun(1000, func() {
		lockUpdateKeyWithDefer(&db, key)
	})
	if allocs != 0 {
		t.Fatalf("deferred lockUpdateKey allocs = %v, want 0", allocs)
	}
}

func lockUpdateKeyWithDefer(db *DB, key []byte) {
	guard := db.lockUpdateKey(key)
	defer guard.Unlock()
}

func BenchmarkLockUpdateKey(b *testing.B) {
	var db DB
	key := []byte("hot-key")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		guard := db.lockUpdateKey(key)
		guard.Unlock()
	}
}

func BenchmarkLockUpdateKeyWithDefer(b *testing.B) {
	var db DB
	key := []byte("hot-key")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		lockUpdateKeyWithDefer(&db, key)
	}
}
