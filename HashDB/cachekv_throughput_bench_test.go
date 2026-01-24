package hashdb

import (
	"bytes"
	"math/rand"
	"testing"
)

func BenchmarkCacheKVRandomWriteThroughput(b *testing.B) {
	const (
		keyCount  = 100_000
		valueSize = 32
	)

	keys := make([][]byte, keyCount)
	for i := 0; i < keyCount; i++ {
		keys[i] = makeKey(i)
	}
	value := bytes.Repeat([]byte{'x'}, valueSize)

	b.Run("DirectDB", func(b *testing.B) {
		dir := b.TempDir()
		db := &DB{}
		if err := db.Open(dir); err != nil {
			b.Fatalf("open: %v", err)
		}
		defer db.Close()
		db.SetCompression(false)

		preloadDB(b, db, keys, value)

		rng := rand.New(rand.NewSource(1))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx := rng.Intn(len(keys))
			if err := db.Put(keys[idx], value); err != nil {
				b.Fatalf("put: %v", err)
			}
		}
	})

	b.Run("CacheKV", func(b *testing.B) {
		dir := b.TempDir()
		cached, err := NewCachedDB(dir, 4096, 4<<20, 0)
		if err != nil {
			b.Fatalf("open cached: %v", err)
		}
		defer cached.Close()
		cached.SetCompression(false)

		preloadCache(b, cached, keys, value)
		if err := cached.Flush(); err != nil {
			b.Fatalf("preload flush: %v", err)
		}

		rng := rand.New(rand.NewSource(1))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx := rng.Intn(len(keys))
			if err := cached.Put(keys[idx], value); err != nil {
				b.Fatalf("put: %v", err)
			}
		}
		b.StopTimer()
		if err := cached.Flush(); err != nil {
			b.Fatalf("flush: %v", err)
		}
	})
}

func preloadDB(b *testing.B, db *DB, keys [][]byte, value []byte) {
	items := make([]Item, 0, 1024)
	for _, key := range keys {
		items = append(items, Item{Key: key, Value: value})
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
}

func preloadCache(b *testing.B, cached *CachedDB, keys [][]byte, value []byte) {
	for _, key := range keys {
		if err := cached.Put(key, value); err != nil {
			b.Fatalf("preload put: %v", err)
		}
	}
}
