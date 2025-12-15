package hashdb

import (
	"bytes"
	"math/rand"
	"testing"
)

func BenchmarkGetManyMatrix(b *testing.B) {
	type params struct {
		name      string
		valSize   int
		batchSize int
		hitRatio  float64
	}

	cases := []params{
		{name: "v128-b32-hit1.0", valSize: 128, batchSize: 32, hitRatio: 1.0},
		{name: "v128-b256-hit1.0", valSize: 128, batchSize: 256, hitRatio: 1.0},
		{name: "v128-b256-hit0.5", valSize: 128, batchSize: 256, hitRatio: 0.5},
		{name: "v1k-b256-hit1.0", valSize: 1024, batchSize: 256, hitRatio: 1.0},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			dir := b.TempDir()
			db, err := OpenWithShards(dir, 8)
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			defer db.Close()

			db.SetCompression(false)

			// Preload a stable keyspace.
			const keyCount = 10_000
			val := bytes.Repeat([]byte("x"), tc.valSize)
			items := make([]Item, 0, 1024)
			for i := 0; i < keyCount; i++ {
				items = append(items, Item{Key: makeKey(i), Value: val})
				if len(items) == cap(items) {
					if err := db.PutMany(items); err != nil {
						b.Fatalf("preload PutMany: %v", err)
					}
					items = items[:0]
				}
			}
			if len(items) > 0 {
				if err := db.PutMany(items); err != nil {
					b.Fatalf("preload PutMany: %v", err)
				}
			}
			// Flush caches so reads go to the backend (exercise slab read path).
			if err := db.Flush(); err != nil {
				b.Fatalf("preload Flush: %v", err)
			}

			// Prebuild request batches (deterministic).
			rng := rand.New(rand.NewSource(1))
			keys := make([][]byte, tc.batchSize)
			for i := range keys {
				if rng.Float64() < tc.hitRatio {
					keys[i] = makeKey(rng.Intn(keyCount))
				} else {
					keys[i] = makeKey(keyCount + rng.Intn(keyCount))
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = db.GetMany(keys)
			}
		})
	}
}

func makeKey(i int) []byte {
	var b [8]byte
	// Big endian so lexicographic order matches numeric order (useful elsewhere).
	b[0] = byte(uint64(i) >> 56)
	b[1] = byte(uint64(i) >> 48)
	b[2] = byte(uint64(i) >> 40)
	b[3] = byte(uint64(i) >> 32)
	b[4] = byte(uint64(i) >> 24)
	b[5] = byte(uint64(i) >> 16)
	b[6] = byte(uint64(i) >> 8)
	b[7] = byte(uint64(i))
	return append([]byte(nil), b[:]...)
}
