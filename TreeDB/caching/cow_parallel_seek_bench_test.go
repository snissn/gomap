//go:build !windows

package caching

import (
	"bytes"
	"errors"
	"testing"
)

func BenchmarkCOWParallelWarmSeek(b *testing.B) {
	db, backend, want, _ := cowLeafMissFixture(b, 128, 64)
	start, end := []byte("a000"), []byte("b")
	seek := func() error {
		s, err := db.AcquireMVCCReadCut()
		if err != nil {
			return err
		}
		k, v, found, readErr := s.SeekGEVersionRange(start, end)
		if err := errors.Join(readErr, s.Close()); err != nil {
			return err
		}
		if !found || !bytes.Equal(k, start) || !bytes.Equal(v, want) {
			return errors.New("incorrect public seek result")
		}
		return nil
	}
	for range 128 {
		if err := seek(); err != nil {
			b.Fatal(err)
		}
	}
	warmHits := cowLeafCacheCounter(b, backend, "hits")
	warmMapped := cowLeafMappedHits(b, backend)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := seek(); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	hits := cowLeafCacheCounter(b, backend, "hits")
	mapped := cowLeafMappedHits(b, backend)
	b.ReportMetric(float64(hits-warmHits)/float64(b.N), "cache-hits/op")
	b.ReportMetric(float64(mapped-warmMapped)/float64(b.N), "mmap-hits/op")
}
