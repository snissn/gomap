package db_test

import (
	"fmt"
	"math/rand"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func BenchmarkLargeRandomRead(b *testing.B) {
	dir := b.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	const count = 1000
	const valSize = 1024 * 1024 // 1MB bodies
	val := make([]byte, valSize)
	rand.Read(val)

	keys := make([][]byte, count)
	for i := 0; i < count; i++ {
		keys[i] = []byte(fmt.Sprintf("k%04d", i))
		db.Set(keys[i], val)
	}
	db.Checkpoint()

	b.ResetTimer()
	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = db.Get(keys[i%count])
		}
	})

	b.Run("GetUnsafe", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = db.GetUnsafe(keys[i%count])
		}
	})
}
