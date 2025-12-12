package treedb

import (
	"encoding/binary"
	"testing"
)

// makeBenchValue returns a deterministic value of size n.
func makeBenchValue(n int) []byte {
	v := make([]byte, n)
	for i := range v {
		v[i] = byte(i)
	}
	return v
}

func openBenchDB(b *testing.B, inlineThreshold int) *DB {
	b.Helper()
	dir := b.TempDir()
	db, err := Open(Options{Dir: dir, InlineThreshold: inlineThreshold})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return db
}

func prefillBenchDB(b *testing.B, db *DB, n int, val []byte) {
	b.Helper()
	const batchSize = 1000
	batch := db.NewBatch()
	for i := 0; i < n; i++ {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(i))
		if err := batch.Set(key[:], val); err != nil {
			b.Fatalf("prefill set: %v", err)
		}
		if (i+1)%batchSize == 0 {
			if err := batch.Write(); err != nil {
				b.Fatalf("prefill write: %v", err)
			}
			batch = db.NewBatch()
		}
	}
	if err := batch.Write(); err != nil {
		b.Fatalf("prefill write final: %v", err)
	}
}

// BenchmarkSet150B measures single-op Set() throughput for IAVL-like values.
func BenchmarkSet150B(b *testing.B) {
	val := makeBenchValue(150)
	b.SetBytes(int64(len(val)))

	for _, th := range []int{64, 256} {
		th := th
		b.Run("InlineThreshold="+itoa(th), func(b *testing.B) {
			db := openBenchDB(b, th)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var key [8]byte
				binary.BigEndian.PutUint64(key[:], uint64(i))
				if err := db.Set(key[:], val); err != nil {
					b.Fatalf("set: %v", err)
				}
			}
			b.StopTimer()
			reportOpsPerSec(b)
		})
	}
}

// BenchmarkBatchSet150B measures batched writes (1000 ops/commit).
func BenchmarkBatchSet150B(b *testing.B) {
	val := makeBenchValue(150)
	b.SetBytes(int64(len(val)))
	const batchSize = 1000

	for _, th := range []int{64, 256} {
		th := th
		b.Run("InlineThreshold="+itoa(th), func(b *testing.B) {
			db := openBenchDB(b, th)
			b.ResetTimer()
			i := 0
			for i < b.N {
				batch := db.NewBatchWithSize(batchSize)
				for j := 0; j < batchSize && i < b.N; j++ {
					var key [8]byte
					binary.BigEndian.PutUint64(key[:], uint64(i))
					if err := batch.Set(key[:], val); err != nil {
						b.Fatalf("batch set: %v", err)
					}
					i++
				}
				if err := batch.Write(); err != nil {
					b.Fatalf("batch write: %v", err)
				}
			}
			b.StopTimer()
			reportOpsPerSec(b)
		})
	}
}

// BenchmarkGet150B measures point lookups on a prefilled tree.
func BenchmarkGet150B(b *testing.B) {
	val := makeBenchValue(150)
	const prefill = 50000

	for _, th := range []int{64, 256} {
		th := th
		b.Run("InlineThreshold="+itoa(th), func(b *testing.B) {
			db := openBenchDB(b, th)
			prefillBenchDB(b, db, prefill, val)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var key [8]byte
				binary.BigEndian.PutUint64(key[:], uint64(i%prefill))
				if _, err := db.Get(key[:]); err != nil {
					b.Fatalf("get: %v", err)
				}
			}
			b.StopTimer()
			reportOpsPerSec(b)
		})
	}
}

// BenchmarkIterScan measures full-range forward scans over a prefilled tree.
func BenchmarkIterScan(b *testing.B) {
	val := makeBenchValue(150)
	const prefill = 50000

	for _, th := range []int{64, 256} {
		th := th
		b.Run("InlineThreshold="+itoa(th), func(b *testing.B) {
			db := openBenchDB(b, th)
			prefillBenchDB(b, db, prefill, val)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				it, err := db.Iterator(nil, nil)
				if err != nil {
					b.Fatalf("iterator: %v", err)
				}
				for ; it.Valid(); it.Next() {
				}
				if err := it.Close(); err != nil {
					b.Fatalf("iterator close: %v", err)
				}
			}
			b.StopTimer()
			reportOpsPerSec(b)
		})
	}
}

func reportOpsPerSec(b *testing.B) {
	b.Helper()
	elapsed := b.Elapsed()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/s")
	}
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	n := v
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
