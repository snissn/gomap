package treedb

import (
	"fmt"
	"os"
	"testing"
)

func BenchmarkPublicConditionalTxnCachedReadSet1(b *testing.B) {
	benchmarkPublicConditionalTxnCachedReadSet(b, 1)
}

func BenchmarkPublicConditionalTxnCachedReadSet10(b *testing.B) {
	benchmarkPublicConditionalTxnCachedReadSet(b, 10)
}

func BenchmarkPublicConditionalTxnCachedReusableReadSet1(b *testing.B) {
	benchmarkPublicConditionalTxnCachedReusableReadSet(b, 1)
}

func BenchmarkPublicConditionalTxnCachedReusableReadSet10(b *testing.B) {
	benchmarkPublicConditionalTxnCachedReusableReadSet(b, 10)
}

func BenchmarkPublicConditionalTxnCachedBaselineGet1BatchWrite(b *testing.B) {
	benchmarkPublicConditionalTxnCachedBaselineGetBatchWrite(b, 1)
}

func BenchmarkPublicConditionalTxnCachedBaselineGet10BatchWrite(b *testing.B) {
	benchmarkPublicConditionalTxnCachedBaselineGetBatchWrite(b, 10)
}

func benchmarkPublicConditionalTxnCachedReadSet(b *testing.B, readSet int) {
	tdb, keys := setupPublicConditionalTxnCachedBenchDB(b, readSet)
	value := []byte("updated")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := tdb.NewConditionalTxn()
		if err != nil {
			b.Fatalf("NewConditionalTxn: %v", err)
		}
		tx.ReserveReadSet(readSet)
		for j := 0; j < readSet; j++ {
			if _, _, err := tx.GetVersioned(keys[(i+j)%len(keys)]); err != nil {
				_ = tx.Close()
				b.Fatalf("GetVersioned: %v", err)
			}
		}
		if err := tx.Set(keys[i%len(keys)], value); err != nil {
			_ = tx.Close()
			b.Fatalf("Set: %v", err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatalf("Commit: %v", err)
		}
	}
}

func benchmarkPublicConditionalTxnCachedReusableReadSet(b *testing.B, readSet int) {
	tdb, keys := setupPublicConditionalTxnCachedBenchDB(b, readSet)
	value := []byte("updated")
	var tx ConditionalTxn
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := tdb.InitConditionalTxn(&tx); err != nil {
			b.Fatalf("InitConditionalTxn: %v", err)
		}
		tx.ReserveReadSet(readSet)
		for j := 0; j < readSet; j++ {
			if _, _, err := tx.GetVersioned(keys[(i+j)%len(keys)]); err != nil {
				_ = tx.Close()
				b.Fatalf("GetVersioned: %v", err)
			}
		}
		if err := tx.Set(keys[i%len(keys)], value); err != nil {
			_ = tx.Close()
			b.Fatalf("Set: %v", err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatalf("Commit: %v", err)
		}
	}
}

func benchmarkPublicConditionalTxnCachedBaselineGetBatchWrite(b *testing.B, readSet int) {
	tdb, keys := setupPublicConditionalTxnCachedBenchDB(b, readSet)
	value := []byte("updated")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < readSet; j++ {
			if _, _, err := tdb.GetVersioned(keys[(i+j)%len(keys)]); err != nil {
				b.Fatalf("GetVersioned: %v", err)
			}
		}
		wb := tdb.NewBatch()
		if wb == nil {
			b.Fatal("NewBatch returned nil")
		}
		if err := wb.Set(keys[i%len(keys)], value); err != nil {
			_ = wb.Close()
			b.Fatalf("Set: %v", err)
		}
		if err := wb.Write(); err != nil {
			_ = wb.Close()
			b.Fatalf("Write: %v", err)
		}
		if err := wb.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
	}
}

func setupPublicConditionalTxnCachedBenchDB(b *testing.B, readSet int) (*DB, [][]byte) {
	b.Helper()
	if readSet <= 0 {
		readSet = 1
	}
	dir, err := os.MkdirTemp("", "treedb-public-conditional-cached-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = os.RemoveAll(dir) })
	tdb, err := Open(Options{Dir: dir})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = tdb.Close() })
	keyCount := 1024
	if keyCount < readSet*2 {
		keyCount = readSet * 2
	}
	keys := make([][]byte, keyCount)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%08d", i))
		if err := tdb.Set(keys[i], []byte("value")); err != nil {
			b.Fatalf("seed Set(%d): %v", i, err)
		}
	}
	return tdb, keys
}
