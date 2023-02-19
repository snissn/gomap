package gomap

import (
	"os"
	"strconv"

	"testing"

	"github.com/syndtr/goleveldb/leveldb"
)

func BenchmarkLeveldb(b *testing.B) {
	folder, _ := os.MkdirTemp("", "hash")
	db, _ := leveldb.OpenFile(folder, nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i)
		_ = db.Put([]byte(key), []byte(key), nil)
	}
}
