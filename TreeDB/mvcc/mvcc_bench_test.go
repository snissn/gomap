package mvcc

import (
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

func openBenchDB(b *testing.B) *treedb.DB {
	b.Helper()
	db := openTestDB(b, b.TempDir(), treedb.DurabilityWALOffRelaxed)
	b.Cleanup(func() { _ = db.Close() })
	return db
}

func BenchmarkCommitAt(b *testing.B) {
	for _, batchSize := range []int{1, 32} {
		b.Run(fmt.Sprintf("DirectTreeDB/%d", batchSize), func(b *testing.B) {
			db := openBenchDB(b)
			value := []byte("value")
			logicalKeys := make([][]byte, batchSize)
			for i := range logicalKeys {
				logicalKeys[i] = []byte(fmt.Sprintf("key-%03d", i))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				write := db.NewBatchWithSize(batchSize)
				for keyIndex := 0; keyIndex < batchSize; keyIndex++ {
					key, err := mvcckey.Encode(logicalKeys[keyIndex], uint64(i+1))
					if err != nil {
						b.Fatal(err)
					}
					if err := write.Set(key, append([]byte{recordValueV1}, value...)); err != nil {
						b.Fatal(err)
					}
				}
				if err := write.Write(); err != nil {
					b.Fatal(err)
				}
				if err := write.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("MVCC/%d", batchSize), func(b *testing.B) {
			db := openBenchDB(b)
			store := New(db)
			mutations := make([]Mutation, batchSize)
			for i := range mutations {
				mutations[i] = Mutation{Key: []byte(fmt.Sprintf("key-%03d", i)), Value: []byte("value")}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := store.CommitAt(uint64(i+1), mutations, CommitRelaxed); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkGetAt(b *testing.B) {
	for _, depth := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("DirectSeek/%d", depth), func(b *testing.B) {
			db, key := prepareGetAtBench(b, depth)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				value, err := directSeek(db, key, uint64(depth+1))
				if err != nil || len(value) == 0 {
					b.Fatalf("directSeek value=%q err=%v", value, err)
				}
			}
		})
		b.Run(fmt.Sprintf("MVCC/%d", depth), func(b *testing.B) {
			db, key := prepareGetAtBench(b, depth)
			store := New(db)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := store.GetAt(key, uint64(depth+1))
				if err != nil || result.State != Present || len(result.Value) == 0 {
					b.Fatalf("GetAt result=%+v err=%v", result, err)
				}
			}
		})
	}
}

func prepareGetAtBench(b *testing.B, depth int) (*treedb.DB, []byte) {
	b.Helper()
	db := openBenchDB(b)
	store := New(db)
	key := []byte("benchmark-key")
	for timestamp := 1; timestamp <= depth; timestamp++ {
		if err := store.CommitAt(uint64(timestamp), []Mutation{{Key: key, Value: []byte("value")}}, CommitRelaxed); err != nil {
			b.Fatalf("prepare CommitAt: %v", err)
		}
	}
	return db, key
}

func directSeek(db *treedb.DB, logical []byte, timestamp uint64) ([]byte, error) {
	lower, err := mvcckey.Encode(logical, timestamp)
	if err != nil {
		return nil, err
	}
	upper, err := mvcckey.AppendKeyVersionsUpper(nil, logical)
	if err != nil {
		return nil, err
	}
	it, err := db.Iterator(lower, upper)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	if !it.Valid() {
		return nil, it.Error()
	}
	return it.ValueCopy(nil), nil
}
