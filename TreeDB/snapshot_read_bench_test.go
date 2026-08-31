package treedb

import (
	"encoding/binary"
	"fmt"
	"testing"
)

var snapshotReadBenchSink []byte

func openSnapshotValueLogBenchDB(b *testing.B) (*DB, [][]byte) {
	b.Helper()

	opts := Options{
		Dir:                              b.TempDir(),
		KeepRecent:                       10_000,
		IndexOuterLeavesInValueLog:       true,
		LeafPrefixCompression:            true,
		IndexColumnarLeaves:              true,
		IndexPackedValuePtr:              true,
		BackgroundCheckpointInterval:     -1,
		BackgroundCheckpointIdleDuration: -1,
		MaxWALBytes:                      -1,
		BackgroundIndexVacuumInterval:    -1,
		DisableBackgroundPrune:           true,
	}
	opts.ValueLog.PointerThreshold = 1

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("open db: %v", err)
	}

	const (
		keyCount  = 32_768
		batchSize = 512
		valueSize = 256
	)
	keys := make([][]byte, keyCount)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("celestia/snapshot/get/key/%08d", i))
	}
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte(i)
	}

	for base := 0; base < keyCount; base += batchSize {
		batch := db.NewBatch()
		limit := base + batchSize
		if limit > keyCount {
			limit = keyCount
		}
		for i := base; i < limit; i++ {
			var seq [8]byte
			binary.BigEndian.PutUint64(seq[:], uint64(i))
			copy(value[len(value)-len(seq):], seq[:])
			if err := batch.Set(keys[i], value); err != nil {
				_ = batch.Close()
				_ = db.Close()
				b.Fatalf("batch.Set(%d): %v", i, err)
			}
		}
		if err := batch.WriteSync(); err != nil {
			_ = batch.Close()
			_ = db.Close()
			b.Fatalf("batch.WriteSync(%d): %v", base, err)
		}
		if err := batch.Close(); err != nil {
			_ = db.Close()
			b.Fatalf("batch.Close(%d): %v", base, err)
		}
	}

	return db, keys
}

func BenchmarkSnapshotValueLogGet(b *testing.B) {
	db, keys := openSnapshotValueLogBenchDB(b)
	defer func() { _ = db.Close() }()

	snap := db.AcquireSnapshot()
	if snap == nil {
		b.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()

	b.Run("Get", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out, err := snap.Get(keys[i%len(keys)])
			if err != nil {
				b.Fatalf("Snapshot.Get: %v", err)
			}
			snapshotReadBenchSink = out
		}
	})

	b.Run("GetAppend", func(b *testing.B) {
		dst := make([]byte, 0, 512)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			dst = dst[:0]
			out, err := snap.GetAppend(keys[i%len(keys)], dst)
			if err != nil {
				b.Fatalf("Snapshot.GetAppend: %v", err)
			}
			snapshotReadBenchSink = out
		}
	})

	b.Run("GetUnsafe", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out, err := snap.GetUnsafe(keys[i%len(keys)])
			if err != nil {
				b.Fatalf("Snapshot.GetUnsafe: %v", err)
			}
			snapshotReadBenchSink = out
		}
	})
}

func BenchmarkDBValueLogGet(b *testing.B) {
	db, keys := openSnapshotValueLogBenchDB(b)
	defer func() { _ = db.Close() }()

	b.Run("Get", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out, err := db.Get(keys[i%len(keys)])
			if err != nil {
				b.Fatalf("DB.Get: %v", err)
			}
			snapshotReadBenchSink = out
		}
	})

	b.Run("GetAppend", func(b *testing.B) {
		dst := make([]byte, 0, 512)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			dst = dst[:0]
			out, err := db.GetAppend(keys[i%len(keys)], dst)
			if err != nil {
				b.Fatalf("DB.GetAppend: %v", err)
			}
			snapshotReadBenchSink = out
		}
	})

	b.Run("GetUnsafe", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out, err := db.GetUnsafe(keys[i%len(keys)])
			if err != nil {
				b.Fatalf("DB.GetUnsafe: %v", err)
			}
			snapshotReadBenchSink = out
		}
	})
}
