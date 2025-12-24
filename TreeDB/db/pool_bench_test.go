package db

import (
	"testing"
)

func BenchmarkSnapshotPool(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s := db.AcquireSnapshot()
			s.Close()
		}
	})
}
