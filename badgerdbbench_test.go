package gomap

import (
	"os"
	"strconv"
	"testing"

	badger "github.com/dgraph-io/badger/v3"
)

func BenchmarkBadger(b *testing.B) {
	folder, _ := os.MkdirTemp("", "hash")
	opts := badger.DefaultOptions(folder)
	db, _ := badger.Open(opts)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i)
		err := db.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte(key), []byte(key))
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	db.Close()

}
