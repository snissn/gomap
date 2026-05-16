package treedb

import (
	"fmt"
	"strconv"
	"testing"
)

func TestPublicCommandWALRawKVWritesUseTypedFrames(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                 dir,
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	if got := db.Stats()["treedb.write_path.mode"]; got != "command_wal_cached" {
		_ = db.Close()
		t.Fatalf("write_path.mode=%q, want command_wal_cached", got)
	}
	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		_ = db.Close()
		t.Fatalf("Set: %v", err)
	}
	b := db.NewBatch()
	if err := b.Set([]byte("k2"), []byte("v2")); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.Delete([]byte("k1")); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Delete: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Write: %v", err)
	}
	if err := b.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("batch Close: %v", err)
	}
	assertPublicCommandWALFrames(t, db, 2)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{
		Dir:                 dir,
		CommandWALStatsScan: true,
	})
	if err != nil {
		t.Fatalf("reopen command WAL from format: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	if got := reopen.Stats()["treedb.write_path.mode"]; got != "command_wal_cached" {
		t.Fatalf("reopen write_path.mode=%q, want command_wal_cached", got)
	}
	got, err := reopen.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("Get(k2): %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("Get(k2)=%q, want v2", got)
	}
	hasK1, err := reopen.Has([]byte("k1"))
	if err != nil {
		t.Fatalf("Has(k1): %v", err)
	}
	if hasK1 {
		t.Fatal("k1 exists after command-WAL batch delete")
	}
	if got := reopen.backend.State().AppliedCommandLSN; got < 2 {
		t.Fatalf("AppliedCommandLSN=%d, want at least 2", got)
	}
}

func assertPublicCommandWALFrames(t *testing.T, db *DB, minFrames uint64) {
	t.Helper()
	stats := db.Stats()
	if stats["treedb.command_wal.required_feature"] != "true" {
		t.Fatalf("required_feature=%q, want true", stats["treedb.command_wal.required_feature"])
	}
	if stats["treedb.command_wal.stats_scan"] != "true" {
		t.Fatalf("stats_scan=%q, want true", stats["treedb.command_wal.stats_scan"])
	}
	frames, err := strconv.ParseUint(stats["treedb.command_wal.frames"], 10, 64)
	if err != nil {
		t.Fatalf("parse command_wal.frames=%q: %v", stats["treedb.command_wal.frames"], err)
	}
	if frames < minFrames {
		t.Fatalf("command_wal.frames=%d, want at least %d", frames, minFrames)
	}
	maxLSN, err := strconv.ParseUint(stats["treedb.command_wal.max_lsn"], 10, 64)
	if err != nil {
		t.Fatalf("parse command_wal.max_lsn=%q: %v", stats["treedb.command_wal.max_lsn"], err)
	}
	if maxLSN < minFrames {
		t.Fatalf("command_wal.max_lsn=%d, want at least %d", maxLSN, minFrames)
	}
}

func BenchmarkPublicCommandWALRawKVSet(b *testing.B) {
	for _, commandWAL := range []bool{false, true} {
		b.Run(fmt.Sprintf("command_wal=%t", commandWAL), func(b *testing.B) {
			db, err := Open(Options{
				Dir:                 b.TempDir(),
				Durability:          DurabilityWALOnRelaxed,
				CommandWAL:          commandWAL,
				CommandWALStatsScan: commandWAL,
				DisableSideStores:   true,
			})
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer func() { _ = db.Close() }()

			value := []byte("public-command-wal-value")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := []byte(fmt.Sprintf("k%09d", i))
				if err := db.Set(key, value); err != nil {
					b.Fatalf("Set: %v", err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "sets/s")
			if commandWAL {
				assertPublicCommandWALFramesB(b, db, uint64(b.N))
			}
		})
	}
}

func BenchmarkPublicCommandWALRawKVBatchWrite(b *testing.B) {
	for _, batchSize := range []int{64, 1024} {
		b.Run(fmt.Sprintf("batch_size=%d", batchSize), func(b *testing.B) {
			for _, commandWAL := range []bool{false, true} {
				b.Run(fmt.Sprintf("command_wal=%t", commandWAL), func(b *testing.B) {
					db, err := Open(Options{
						Dir:                 b.TempDir(),
						Durability:          DurabilityWALOnRelaxed,
						CommandWAL:          commandWAL,
						CommandWALStatsScan: commandWAL,
						DisableSideStores:   true,
					})
					if err != nil {
						b.Fatalf("Open: %v", err)
					}
					defer func() { _ = db.Close() }()

					value := []byte("public-command-wal-value")
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						batch := db.NewBatchWithSize(batchSize)
						base := i * batchSize
						for j := 0; j < batchSize; j++ {
							var keyBuf [32]byte
							key := strconv.AppendInt(keyBuf[:0], int64(base+j), 10)
							if err := batch.Set(key, value); err != nil {
								_ = batch.Close()
								b.Fatalf("batch Set: %v", err)
							}
						}
						if err := batch.Write(); err != nil {
							_ = batch.Close()
							b.Fatalf("batch Write: %v", err)
						}
						if err := batch.Close(); err != nil {
							b.Fatalf("batch Close: %v", err)
						}
					}
					b.StopTimer()
					totalSets := float64(b.N * batchSize)
					b.ReportMetric(totalSets/b.Elapsed().Seconds(), "sets/s")
					b.ReportMetric(float64(batchSize), "sets/batch")
					if commandWAL {
						assertPublicCommandWALFramesB(b, db, uint64(b.N))
					}
				})
			}
		})
	}
}

func assertPublicCommandWALFramesB(b *testing.B, db *DB, minFrames uint64) {
	b.Helper()
	stats := db.Stats()
	frames, err := strconv.ParseUint(stats["treedb.command_wal.frames"], 10, 64)
	if err != nil {
		b.Fatalf("parse command_wal.frames=%q: %v", stats["treedb.command_wal.frames"], err)
	}
	if frames < minFrames {
		b.Fatalf("command_wal.frames=%d, want at least %d", frames, minFrames)
	}
}
