package slab

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

type adaptiveBenchWorkload struct {
	name string
	make func(rng *rand.Rand, size int) []byte
}

func BenchmarkCompressionAdaptiveSweep(b *testing.B) {
	workloads := []adaptiveBenchWorkload{
		{
			name: "highly_compressible",
			make: func(rng *rand.Rand, size int) []byte {
				pattern := bytes.Repeat([]byte("{\"key\":\"value\",\"active\":true}"), size/32)
				buf := make([]byte, size)
				copy(buf, pattern)
				if size > 64 {
					rng.Read(buf[size-64:])
				}
				return buf
			},
		},
		{
			name: "medium_compressible",
			make: func(rng *rand.Rand, size int) []byte {
				pattern := bytes.Repeat([]byte("abcd1234"), size/8)
				buf := make([]byte, size)
				half := size / 2
				copy(buf, pattern[:half])
				rng.Read(buf[half:])
				return buf
			},
		},
		{
			name: "incompressible",
			make: func(rng *rand.Rand, size int) []byte {
				buf := make([]byte, size)
				rng.Read(buf)
				return buf
			},
		},
	}

	ratios := []float64{0.98, 0.995}
	windows := []int{256 << 10, 1 << 20}
	pauses := []int{4 << 20, 16 << 20}

	const valueSize = 16 << 10

	for _, workload := range workloads {
		for _, ratio := range ratios {
			for _, window := range windows {
				for _, pause := range pauses {
					name := fmt.Sprintf("%s/ratio=%.3f/window=%d/pause=%d", workload.name, ratio, window, pause)
					b.Run(name, func(b *testing.B) {
						dir := b.TempDir()
						opts := Options{
							Compression: CompressionOptions{
								Kind: CompressionZSTD,
							},
							CompressionDisableFullRecord:  true,
							CompressionAdaptiveRatio:      ratio,
							CompressionMetricsWindowBytes: window,
							CompressionAdaptivePauseBytes: pause,
							CompressionAdaptiveProbeBytes: pause / 4,
							CompressionAdaptiveTrainBytes: -1,
						}
						sm, err := NewSlabManagerWithOptions(dir, opts)
						if err != nil {
							b.Fatalf("open slab manager: %v", err)
						}
						defer sm.Close()

						rng := rand.New(rand.NewSource(1))
						value := workload.make(rng, valueSize)
						key := []byte("bench-key")

						b.ResetTimer()
						b.SetBytes(int64(valueSize))
						for i := 0; i < b.N; i++ {
							if _, err := sm.Append(key, value); err != nil {
								b.Fatalf("append: %v", err)
							}
						}
						b.StopTimer()
					})
				}
			}
		}
	}
}
