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
	// The historical name is retained for benchmark continuity. Snapshot
	// handles now have unique identities and intentionally allocate once per
	// acquisition; SnapshotPool still owns their cleanup contract.
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s := db.AcquireSnapshot()
			s.Close()
		}
	})
}
