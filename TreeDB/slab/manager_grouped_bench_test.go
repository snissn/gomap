package slab

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/page"
)

// BenchmarkSlabGrouped writes N values with K>1 profile and verifies grouped pointers.
// It reports write/read timings and counts grouped vs legacy pointers.
func BenchmarkSlabGrouped(b *testing.B) {
	const (
		N           = 200000
		readSamples = 10000
		k           = 3
		valLen      = 169
	)
	dir := b.TempDir()
	opts := Options{
		Compression: CompressionOptions{
			Kind: CompressionZSTD,
		},
		OmitSlabKeys: true,
	}
	sm, err := NewSlabManagerWithOptions(dir, opts)
	if err != nil {
		b.Fatalf("NewSlabManagerWithOptions: %v", err)
	}
	// Force active profile K>1 for benchmark.
	trainer := &compressionTrainer{}
	trainer.lastProfile.Store(&ActiveCompressionProfile{K: k})
	sm.compressionTrainer = trainer

	// Prepare values/keys.
	values := make([][]byte, N)
	keys := make([][]byte, N)
	src := rand.New(rand.NewSource(1))
	for i := 0; i < N; i++ {
		v := make([]byte, valLen)
		for j := range v {
			v[j] = byte(src.Intn(256))
		}
		values[i] = v
		keys[i] = nil // keys omitted
	}

	b.ResetTimer()
	b.ReportAllocs()

	var ptrs []page.ValuePtr
	b.Run("write", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			ptrs = ptrs[:0]
			batch := 1024
			start := time.Now()
			for i := 0; i < N; i += batch {
				end := i + batch
				if end > N {
					end = N
				}
				p, err := sm.AppendMany(keys[i:end], values[i:end])
				if err != nil {
					b.Fatalf("AppendMany: %v", err)
				}
				ptrs = append(ptrs, p...)
			}
			elapsed := time.Since(start)
			b.ReportMetric(float64(elapsed.Nanoseconds())/float64(N), "write_ns/op")
			break
		}
	})

	groupedCnt := 0
	legacyCnt := 0
	for _, p := range ptrs {
		if page.ValuePtrIsGrouped(p) {
			groupedCnt++
		} else {
			legacyCnt++
		}
	}

	// Read random samples.
	b.Run("read", func(b *testing.B) {
		idxs := rand.Perm(len(ptrs))[:readSamples]
		start := time.Now()
		for _, idx := range idxs {
			p := ptrs[idx]
			val, err := sm.Read(p)
			if err != nil {
				b.Fatalf("Read: %v", err)
			}
			if len(val) != valLen {
				b.Fatalf("unexpected len: got %d", len(val))
			}
		}
		elapsed := time.Since(start)
		b.ReportMetric(float64(elapsed.Nanoseconds())/float64(len(idxs)), "read_ns/op")
	})

	// Slab size stats.
	info, err := os.Stat(filepath.Join(dir, "data-0000.slab"))
	if err != nil {
		b.Fatalf("stat slab: %v", err)
	}
	bytesPerRecord := float64(info.Size()) / float64(len(ptrs))

	b.Logf("grouped_records=%d legacy_records=%d slab_bytes=%d bytes/record=%.2f", groupedCnt, legacyCnt, info.Size(), bytesPerRecord)
}
