package caching

import (
	"encoding/binary"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

var snapshotOwnedReadBenchSink []byte

// BenchmarkSnapshotPublishedOwnedRead isolates the cached wrapper's published
// outer-leaf path. Setup/checkpoint/warmup are excluded; generic workload and
// concurrent-writer acceptance remain the unified-bench suite's responsibility.
func BenchmarkSnapshotPublishedOwnedRead(b *testing.B) {
	dir := b.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir: dir, IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression: true, IndexColumnarLeaves: true, IndexPackedValuePtr: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer backend.Close()
	cached, err := Open(dir, backend, Options{
		FlushThreshold: 1 << 30, MemtableShards: 1, ValueLogPointerThreshold: 1,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer cached.Close()
	const count, size = 4096, 256
	keys := make([][]byte, count)
	value := make([]byte, size)
	for i := range value {
		value[i] = byte(i)
	}
	batch := cached.NewBatch()
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("snapshot/published/%08d", i))
		binary.BigEndian.PutUint64(value[len(value)-8:], uint64(i))
		if err := batch.Set(keys[i], value); err != nil {
			b.Fatal(err)
		}
	}
	if err := batch.WriteSync(); err != nil {
		b.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		b.Fatal(err)
	}
	if err := cached.Checkpoint(); err != nil {
		b.Fatal(err)
	}
	// Keep a queued write so public acquisition would also need the cached view.
	if err := cached.Set([]byte("unrelated/queued"), []byte("pending")); err != nil {
		b.Fatal(err)
	}
	snap := cached.AcquireSnapshot()
	if snap == nil {
		b.Fatal("AcquireSnapshot=nil")
	}
	defer snap.Close()
	if snap.view == nil || !rootDomainPublishedUsesBackendLookup(rootDomainSnapshotFromCachedSnapshot(snap, keys[0])) {
		b.Fatal("fixture must retain cached state and read a published backend root")
	}
	for i, key := range keys {
		out, err := snap.Get(key)
		if err != nil || len(out) != size || binary.BigEndian.Uint64(out[len(out)-8:]) != uint64(i) {
			b.Fatalf("warm Get(%d): len=%d err=%v", i, len(out), err)
		}
	}
	b.Run("Get", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out, err := snap.Get(keys[i%count])
			if err != nil || len(out) != size || binary.BigEndian.Uint64(out[len(out)-8:]) != uint64(i%count) {
				b.Fatalf("Get: len=%d err=%v", len(out), err)
			}
			snapshotOwnedReadBenchSink = out
		}
	})
	b.Run("GetAppend", func(b *testing.B) {
		dst := make([]byte, 0, size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			out, err := snap.GetAppend(keys[i%count], dst[:0])
			if err != nil || len(out) != size || binary.BigEndian.Uint64(out[len(out)-8:]) != uint64(i%count) {
				b.Fatalf("GetAppend: len=%d err=%v", len(out), err)
			}
			snapshotOwnedReadBenchSink = out
		}
	})
}
