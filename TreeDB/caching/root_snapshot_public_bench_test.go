package caching

import (
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func BenchmarkCachedSnapshotReuse(b *testing.B) {
	dir := b.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		b.Fatal(err)
	}
	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close(); _ = backend.Close() })
	if err := db.Set([]byte("snapshot-key"), []byte("snapshot-value")); err != nil {
		b.Fatal(err)
	}
	s := db.AcquireSnapshot()
	if s == nil || s.view == nil {
		b.Fatal("cached snapshot path inactive")
	}
	if err := s.Close(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			snap := db.AcquireSnapshot()
			if snap == nil || snap.view == nil {
				panic("cached snapshot path inactive")
			}
			if err := snap.Close(); err != nil {
				panic(err)
			}
		}
	})
}

// Each measured acquisition follows an untimed write, and therefore includes
// freezing that write and the first snapshot of its resulting view.
func BenchmarkCachedSnapshotAfterWrite(b *testing.B) {
	dir := b.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		b.Fatal(err)
	}
	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close(); _ = backend.Close() })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		if err := db.Set([]byte("snapshot-key"), []byte("snapshot-value")); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		snap := db.AcquireSnapshot()
		if snap == nil || snap.view == nil {
			b.Fatal("cached snapshot path inactive")
		}
		if err := snap.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
