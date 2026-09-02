package caching

import (
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func BenchmarkRootDomainSnapshotPrefixIterator(b *testing.B) {
	dir := b.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		b.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	const totalKeys = 64
	for i := 0; i < totalKeys; i++ {
		key := []byte(fmt.Sprintf("pfx/%02d", i))
		val := []byte(fmt.Sprintf("backend-%02d", i))
		if err := backend.SetSync(key, val); err != nil {
			b.Fatalf("backend set %q: %v", string(key), err)
		}
	}

	db, err := Open(dir, backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableShards: 1,
	})
	if err != nil {
		b.Fatalf("open caching db: %v", err)
	}
	defer db.Close()

	for i := 8; i < 16; i++ {
		key := []byte(fmt.Sprintf("pfx/%02d", i))
		val := []byte(fmt.Sprintf("queued-%02d", i))
		if err := db.Set(key, val); err != nil {
			b.Fatalf("queued set %q: %v", string(key), err)
		}
	}
	for i := 16; i < 24; i++ {
		key := []byte(fmt.Sprintf("pfx/%02d", i))
		if err := db.Delete(key); err != nil {
			b.Fatalf("queued delete %q: %v", string(key), err)
		}
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		b.Fatal("expected snapshot")
	}
	defer snap.Close()

	start := []byte("pfx/")
	end := []byte("pfx0")
	const expectedKeys = totalKeys - 8

	b.ReportAllocs()
	b.ReportMetric(float64(expectedKeys), "keys/op")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it, err := snap.Iterator(start, end)
		if err != nil {
			b.Fatalf("snapshot iterator: %v", err)
		}
		seen := 0
		for it.Valid() {
			seen++
			it.Next()
		}
		if err := it.Error(); err != nil {
			_ = it.Close()
			b.Fatalf("iterator error: %v", err)
		}
		if err := it.Close(); err != nil {
			b.Fatalf("iterator close: %v", err)
		}
		if seen != expectedKeys {
			b.Fatalf("keys seen=%d want %d", seen, expectedKeys)
		}
	}
}
