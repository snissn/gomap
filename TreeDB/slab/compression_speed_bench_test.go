package slab

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/compression"
)

func BenchmarkWriteSpeedComparison(b *testing.B) {
	// Large compressible value (100KB of repeating patterns + some entropy)
	basePattern := bytes.Repeat([]byte("{\"id\": 123, \"data\": \"some_repeated_string_to_ensure_compression_efficiency\", \"active\": true}"), 1000)
	valSize := len(basePattern)

	compressibleValue := make([]byte, valSize)
	copy(compressibleValue, basePattern)

	runBench := func(name string, opts Options) {
		b.Run(name, func(b *testing.B) {
			dir := b.TempDir()
			sm, err := NewSlabManagerWithOptions(dir, opts)
			if err != nil {
				b.Fatal(err)
			}
			defer sm.Close()

			// If ZSTD is enabled, we need to pre-train a dictionary so we measure
			// dictionary write speed, not training overhead or raw ZSTD.
			if opts.Compression.Kind == CompressionZSTD {
				// Inject a mock profile to bypass training time
				sm.compressionTrainer.AcceptProfile(&compression.ActiveProfile{
					Dict: make([]byte, 32768),
					K:    1,
				})
				// Force rotation to V2
				if _, err := sm.Rotate(); err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			b.SetBytes(int64(valSize))

			keyBuf := make([]byte, 8)
			valBuf := make([]byte, valSize)
			copy(valBuf, compressibleValue)

			for i := 0; i < b.N; i++ {
				rand.Read(keyBuf)
				// Add some entropy at the end to keep trainer happy
				binary.LittleEndian.PutUint64(valBuf[valSize-8:], uint64(i))
				_, err := sm.Append(keyBuf, valBuf)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}

	// 1. Non-compressed Write Speed
	runBench("NonCompressed", Options{
		Compression: CompressionOptions{
			Kind: CompressionNone,
		},
		CompressionAdaptiveTrainBytes: -1, // Disable trainer
	})

	// 2. Dictionary Compression Write Speed (Slab V2)
	runBench("DictCompressionV2", Options{
		Compression: CompressionOptions{
			Kind: CompressionZSTD,
		},
		CompressionAdaptiveTrainBytes: -1, // Disable background trainer
	})
}
