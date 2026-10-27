package mvcc

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

// Includes the exhaustion Next used by posting.readFromDisk, not just construction.
func BenchmarkExactHistoryLifecycle(b *testing.B) {
	for _, depth := range []int{0, 1, 2, 16} {
		b.Run(fmt.Sprint(depth), func(b *testing.B) {
			db := c3ReadDB(b, treedb.ProfileCommandWALRelaxed, false)
			store := New(db)
			for ts := 1; ts <= max(depth, 1); ts++ {
				mutations := make([]Mutation, 256)
				for k := range mutations {
					mutations[k] = Mutation{Key: []byte(fmt.Sprintf("key%04d", k)), Value: bytes.Repeat([]byte{byte(ts)}, 128)}
				}
				if err := store.CommitAt(uint64(ts), mutations, CommitRelaxed); err != nil {
					b.Fatal(err)
				}
			}
			if err := db.Checkpoint(); err != nil {
				b.Fatal(err)
			}
			key := []byte("key0128")
			if depth == 0 {
				key = []byte("absent")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				it, err := store.IterateVersions(VersionIteratorOptions{ExactKey: key, ReadTimestamp: math.MaxUint64})
				if err != nil {
					b.Fatal(err)
				}
				n := 0
				for it.Valid() {
					n++
					it.Next()
				}
				if err := it.Error(); err != nil {
					b.Fatal(err)
				}
				if err := it.Close(); err != nil {
					b.Fatal(err)
				}
				if n != depth {
					b.Fatalf("rows=%d want%d", n, depth)
				}
			}
		})
	}
}
