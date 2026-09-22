package db

import (
	"fmt"
	"testing"
)

func benchmarkDeleteRangeKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("k%09d", i))
	}
	return keys
}

func benchmarkSeedKeys(b *testing.B, d *DB, keys [][]byte, value []byte) {
	b.Helper()
	batch := d.NewBatch()
	for _, key := range keys {
		if err := batch.Set(key, value); err != nil {
			_ = batch.Close()
			b.Fatalf("seed Set: %v", err)
		}
	}
	if err := batch.Write(); err != nil {
		_ = batch.Close()
		b.Fatalf("seed Write: %v", err)
	}
	_ = batch.Close()
}

func BenchmarkBatchDeleteRangeDense(b *testing.B) {
	const count = 4096
	keys := benchmarkDeleteRangeKeys(count)
	value := []byte("value")
	rangeStart := keys[0]
	rangeEnd := []byte("k999999999")

	b.Run("range_delete_4096", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(count), "affected_keys/op")
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			d, err := Open(Options{Dir: b.TempDir(), DisableBackgroundPrune: true})
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			benchmarkSeedKeys(b, d, keys, value)
			batch := d.NewBatch()
			b.StartTimer()
			if err := batch.DeleteRange(rangeStart, rangeEnd); err != nil {
				b.Fatalf("DeleteRange: %v", err)
			}
			if err := batch.Write(); err != nil {
				b.Fatalf("Write: %v", err)
			}
			b.StopTimer()
			_ = batch.Close()
			_ = d.Close()
		}
	})

	b.Run("point_delete_4096", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(count), "affected_keys/op")
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			d, err := Open(Options{Dir: b.TempDir(), DisableBackgroundPrune: true})
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			benchmarkSeedKeys(b, d, keys, value)
			batch := d.NewBatch()
			b.StartTimer()
			for _, key := range keys {
				if err := batch.Delete(key); err != nil {
					b.Fatalf("Delete: %v", err)
				}
			}
			if err := batch.Write(); err != nil {
				b.Fatalf("Write: %v", err)
			}
			b.StopTimer()
			_ = batch.Close()
			_ = d.Close()
		}
	})

	b.Run("batch_write_4096", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(count), "affected_keys/op")
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			d, err := Open(Options{Dir: b.TempDir(), DisableBackgroundPrune: true})
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			batch := d.NewBatch()
			b.StartTimer()
			for _, key := range keys {
				if err := batch.Set(key, value); err != nil {
					b.Fatalf("Set: %v", err)
				}
			}
			if err := batch.Write(); err != nil {
				b.Fatalf("Write: %v", err)
			}
			b.StopTimer()
			_ = batch.Close()
			_ = d.Close()
		}
	})
}
