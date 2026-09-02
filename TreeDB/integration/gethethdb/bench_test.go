package gethethdb

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

var benchValue = []byte("0123456789abcdef0123456789abcdef")

func BenchmarkAdapterVsDirect(b *testing.B) {
	b.Run("Put", func(b *testing.B) {
		b.Run("adapter", benchAdapterPut)
		b.Run("direct", benchDirectPut)
	})
	b.Run("Get", func(b *testing.B) {
		b.Run("adapter", benchAdapterGet)
		b.Run("direct", benchDirectGet)
	})
	b.Run("BatchWrite", func(b *testing.B) {
		b.Run("adapter", benchAdapterBatchWrite)
		b.Run("direct", benchDirectBatchWrite)
	})
	b.Run("BatchWriteReset", func(b *testing.B) {
		b.Run("adapter", benchAdapterBatchWriteReset)
		b.Run("direct", benchDirectBatchWriteReset)
	})
	b.Run("Iterator", func(b *testing.B) {
		b.Run("adapter", benchAdapterIterator)
		b.Run("direct", benchDirectIterator)
	})
	b.Run("DeleteRange", func(b *testing.B) {
		b.Run("adapter", benchAdapterDeleteRange)
		b.Run("direct", benchDirectDeleteRange)
	})
	b.Run("BatchDeleteRange", func(b *testing.B) {
		b.Run("adapter", benchAdapterBatchDeleteRange)
		b.Run("direct", benchDirectBatchDeleteRange)
	})
}

func benchAdapterPut(b *testing.B) {
	db := openBenchAdapter(b)
	defer db.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(benchKey(i), benchValue); err != nil {
			b.Fatal(err)
		}
	}
}

func benchDirectPut(b *testing.B) {
	db := openBenchTreeDB(b)
	defer db.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Set(benchKey(i), benchValue); err != nil {
			b.Fatal(err)
		}
	}
}

func benchAdapterGet(b *testing.B) {
	db := openBenchAdapter(b)
	defer db.Close()
	preloadAdapter(b, db, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get(benchKey(i & 1023)); err != nil {
			b.Fatal(err)
		}
	}
}

func benchDirectGet(b *testing.B) {
	db := openBenchTreeDB(b)
	defer db.Close()
	preloadDirect(b, db, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get(benchKey(i & 1023)); err != nil {
			b.Fatal(err)
		}
	}
}

func benchAdapterBatchWrite(b *testing.B) {
	db := openBenchAdapter(b)
	defer db.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := db.NewBatchWithSize(16)
		base := i * 16
		for j := 0; j < 16; j++ {
			if err := batch.Put(benchKey(base+j), benchValue); err != nil {
				b.Fatal(err)
			}
		}
		if err := batch.Write(); err != nil {
			b.Fatal(err)
		}
	}
}

func benchDirectBatchWrite(b *testing.B) {
	db := openBenchTreeDB(b)
	defer db.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := db.NewBatchWithSize(16)
		base := i * 16
		for j := 0; j < 16; j++ {
			if err := batch.Set(benchKey(base+j), benchValue); err != nil {
				b.Fatal(err)
			}
		}
		if err := batch.Write(); err != nil {
			b.Fatal(err)
		}
	}
}

func benchAdapterBatchWriteReset(b *testing.B) {
	db := openBenchAdapter(b)
	defer db.Close()
	batch := db.NewBatchWithSize(16)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base := i * 16
		for j := 0; j < 16; j++ {
			if err := batch.Put(benchKey(base+j), benchValue); err != nil {
				b.Fatal(err)
			}
		}
		if err := batch.Write(); err != nil {
			b.Fatal(err)
		}
		batch.Reset()
	}
}

func benchDirectBatchWriteReset(b *testing.B) {
	db := openBenchTreeDB(b)
	defer db.Close()
	batch := db.NewBatchWithSize(16)
	resetter, ok := batch.(interface{ Reset() })
	if !ok {
		b.Fatal("direct TreeDB batch does not support Reset")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base := i * 16
		for j := 0; j < 16; j++ {
			if err := batch.Set(benchKey(base+j), benchValue); err != nil {
				b.Fatal(err)
			}
		}
		if err := batch.Write(); err != nil {
			b.Fatal(err)
		}
		resetter.Reset()
	}
}

func benchAdapterIterator(b *testing.B) {
	db := openBenchAdapter(b)
	defer db.Close()
	preloadAdapter(b, db, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := db.NewIterator(nil, nil)
		for it.Next() {
		}
		if err := it.Error(); err != nil {
			b.Fatal(err)
		}
		it.Release()
	}
}

func benchDirectIterator(b *testing.B) {
	db := openBenchTreeDB(b)
	defer db.Close()
	preloadDirect(b, db, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it, err := db.Iterator(nil, nil)
		if err != nil {
			b.Fatal(err)
		}
		for it.Valid() {
			it.Next()
		}
		if err := it.Error(); err != nil {
			b.Fatal(err)
		}
		if err := it.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func benchAdapterDeleteRange(b *testing.B) {
	db := openBenchAdapter(b)
	defer db.Close()
	benchDeleteRangeLoop(b, func(prefix string) error {
		return preloadRangeAdapter(db, prefix, 16)
	}, func(start, end []byte) error {
		return db.DeleteRange(start, end)
	})
}

func benchDirectDeleteRange(b *testing.B) {
	db := openBenchTreeDB(b)
	defer db.Close()
	benchDeleteRangeLoop(b, func(prefix string) error {
		return preloadRangeDirect(db, prefix, 16)
	}, func(start, end []byte) error {
		return db.DeleteRange(start, end)
	})
}

func benchAdapterBatchDeleteRange(b *testing.B) {
	db := openBenchAdapter(b)
	defer db.Close()
	benchDeleteRangeLoop(b, func(prefix string) error {
		return preloadRangeAdapter(db, prefix, 16)
	}, func(start, end []byte) error {
		batch := db.NewBatch()
		if err := batch.DeleteRange(start, end); err != nil {
			return err
		}
		return batch.Write()
	})
}

func benchDirectBatchDeleteRange(b *testing.B) {
	db := openBenchTreeDB(b)
	defer db.Close()
	benchDeleteRangeLoop(b, func(prefix string) error {
		return preloadRangeDirect(db, prefix, 16)
	}, func(start, end []byte) error {
		batch := db.NewBatch()
		if err := batch.DeleteRange(start, end); err != nil {
			return err
		}
		return batch.Write()
	})
}

func benchDeleteRangeLoop(b *testing.B, preload func(prefix string) error, deleteRange func(start, end []byte) error) {
	b.ReportAllocs()
	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prefix := fmt.Sprintf("dr/%08d/", i)
		if err := preload(prefix); err != nil {
			b.Fatal(err)
		}
		start := []byte(prefix)
		end := append([]byte(prefix), 0xff)
		b.StartTimer()
		err := deleteRange(start, end)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func openBenchAdapter(b testing.TB) *Database {
	b.Helper()
	db, err := Open(filepath.Join(b.TempDir(), "treedb"), &OpenOptions{Profile: treedb.ProfileCommandWALRelaxed})
	if err != nil {
		b.Fatalf("Open adapter: %v", err)
	}
	return db
}

func openBenchTreeDB(b testing.TB) *treedb.DB {
	b.Helper()
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, filepath.Join(b.TempDir(), "treedb")))
	if err != nil {
		b.Fatalf("Open TreeDB: %v", err)
	}
	return db
}

func preloadAdapter(b testing.TB, db *Database, n int) {
	b.Helper()
	for i := 0; i < n; i++ {
		if err := db.Put(benchKey(i), benchValue); err != nil {
			b.Fatal(err)
		}
	}
}

func preloadDirect(b testing.TB, db *treedb.DB, n int) {
	b.Helper()
	for i := 0; i < n; i++ {
		if err := db.Set(benchKey(i), benchValue); err != nil {
			b.Fatal(err)
		}
	}
}

func preloadRangeAdapter(db *Database, prefix string, n int) error {
	for i := 0; i < n; i++ {
		if err := db.Put([]byte(fmt.Sprintf("%s%02d", prefix, i)), benchValue); err != nil {
			return err
		}
	}
	return nil
}

func preloadRangeDirect(db *treedb.DB, prefix string, n int) error {
	for i := 0; i < n; i++ {
		if err := db.Set([]byte(fmt.Sprintf("%s%02d", prefix, i)), benchValue); err != nil {
			return err
		}
	}
	return nil
}

func benchKey(i int) []byte {
	var key [16]byte
	copy(key[:], "bench-key/")
	binary.BigEndian.PutUint64(key[8:], uint64(i))
	return key[:]
}
