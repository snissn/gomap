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

	benchmarkDBValueLogGet(b, db, keys)
}

// BenchmarkDBCheckpointedValueLogGet exercises owned reads through the ordinary
// public/cached API after checkpointing, so each read reaches backend capture.
func BenchmarkDBCheckpointedValueLogGet(b *testing.B) {
	db, keys := openSnapshotValueLogBenchDB(b)
	defer func() { _ = db.Close() }()
	if err := db.Checkpoint(); err != nil {
		b.Fatal(err)
	}
	// Warm every key before timing, including the backend leaf/value read path.
	for i, key := range keys {
		out, err := db.Get(key)
		if err != nil || len(out) != 256 || binary.BigEndian.Uint64(out[len(out)-8:]) != uint64(i) {
			b.Fatalf("warm Get(%d): len=%d err=%v", i, len(out), err)
		}
	}
	benchmarkDBValueLogGet(b, db, keys)
}

func benchmarkDBValueLogGet(b *testing.B, db *DB, keys [][]byte) {
	b.Run("Get", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out, err := db.Get(keys[i%len(keys)])
			if err != nil || len(out) != 256 || binary.BigEndian.Uint64(out[len(out)-8:]) != uint64(i%len(keys)) {
				b.Fatalf("DB.Get: len=%d err=%v", len(out), err)
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
			if err != nil || len(out) != 256 || binary.BigEndian.Uint64(out[len(out)-8:]) != uint64(i%len(keys)) {
				b.Fatalf("DB.GetAppend: len=%d err=%v", len(out), err)
			}
			snapshotReadBenchSink = out
		}
	})

	b.Run("GetUnsafe", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out, err := db.GetUnsafe(keys[i%len(keys)])
			if err != nil || len(out) != 256 || binary.BigEndian.Uint64(out[len(out)-8:]) != uint64(i%len(keys)) {
				b.Fatalf("DB.GetUnsafe: len=%d err=%v", len(out), err)
			}
			snapshotReadBenchSink = out
		}
	})
}
