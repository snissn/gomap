package treedb

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
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

func TestPublicCommandWALPointWritesSerializeLSNWithCachedMutation(t *testing.T) {
	db, err := Open(Options{
		Dir:                 t.TempDir(),
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	aAppended := make(chan struct{})
	bAppended := make(chan struct{})
	releaseA := make(chan struct{})
	var aOnce, bOnce, releaseOnce sync.Once
	db.testAfterPublicCommandWALPointAppend = func(op commitlog.RawKVOperation) {
		switch string(op.Value) {
		case "A":
			aOnce.Do(func() { close(aAppended) })
			<-releaseA
		case "B":
			bOnce.Do(func() { close(bAppended) })
		}
	}

	errA := make(chan error, 1)
	go func() {
		errA <- db.Set([]byte("same-key"), []byte("A"))
	}()
	select {
	case <-aAppended:
	case <-time.After(5 * time.Second):
		t.Fatal("first command append did not reach test hook")
	}

	errB := make(chan error, 1)
	bStarted := make(chan struct{})
	go func() {
		close(bStarted)
		errB <- db.Set([]byte("same-key"), []byte("B"))
	}()
	<-bStarted
	select {
	case <-bAppended:
		releaseOnce.Do(func() { close(releaseA) })
		t.Fatal("second same-key command appended before first cached mutation was released")
	case <-time.After(100 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(releaseA) })
	if err := recvTestErr(t, errA); err != nil {
		t.Fatalf("first Set: %v", err)
	}
	if err := recvTestErr(t, errB); err != nil {
		t.Fatalf("second Set: %v", err)
	}
	select {
	case <-bAppended:
	default:
		t.Fatal("second command append hook did not run")
	}
	got, err := db.Get([]byte("same-key"))
	if err != nil {
		t.Fatalf("Get(same-key): %v", err)
	}
	if string(got) != "B" {
		t.Fatalf("Get(same-key)=%q, want B", got)
	}
	assertPublicCommandWALFrames(t, db, 2)
}

func TestPublicCommandWALBatchCloseDiscardsDirtyPayload(t *testing.T) {
	db, err := Open(Options{
		Dir:                 t.TempDir(),
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := db.NewBatch()
	if err := b.Set([]byte("discarded"), []byte("value")); err != nil {
		_ = b.Close()
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch Close: %v", err)
	}
	has, err := db.Has([]byte("discarded"))
	if err != nil {
		t.Fatalf("Has(discarded): %v", err)
	}
	if has {
		t.Fatal("closed dirty batch became visible without Write")
	}
	stats := db.Stats()
	frames, err := strconv.ParseUint(stats["treedb.command_wal.frames"], 10, 64)
	if err != nil {
		t.Fatalf("parse command_wal.frames=%q: %v", stats["treedb.command_wal.frames"], err)
	}
	if frames != 0 {
		t.Fatalf("command_wal.frames=%d, want 0 after Close without Write", frames)
	}
}

func TestPublicCommandWALBatchWriteFailureDoesNotAppendFrame(t *testing.T) {
	db, err := Open(Options{
		Dir:                 t.TempDir(),
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	b, ok := db.NewBatch().(*commandWALPublicBatch)
	if !ok {
		t.Fatalf("NewBatch type=%T, want *commandWALPublicBatch", db.NewBatch())
	}
	defer func() { _ = b.Close() }()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.inner.Close(); err != nil {
		t.Fatalf("inner Close: %v", err)
	}
	if err := b.Write(); err == nil {
		t.Fatal("batch Write succeeded after inner batch was closed")
	}

	stats := db.Stats()
	frames, err := strconv.ParseUint(stats["treedb.command_wal.frames"], 10, 64)
	if err != nil {
		t.Fatalf("parse command_wal.frames=%q: %v", stats["treedb.command_wal.frames"], err)
	}
	if frames != 0 {
		t.Fatalf("command_wal.frames=%d, want 0 after failed batch Write", frames)
	}
}

func TestPublicCommandWALPublishSyncMatrix(t *testing.T) {
	tests := []struct {
		mode string
		sync bool
		want bool
	}{
		{mode: "wal_on_sync", sync: false, want: false},
		{mode: "wal_on_sync", sync: true, want: true},
		{mode: "wal_on_sync+no_read_checksum", sync: true, want: true},
		{mode: "wal_on_relaxed_sync", sync: true, want: false},
		{mode: "wal_on_relaxed_sync+verify_on_read", sync: true, want: false},
		{mode: "wal_off_relaxed_sync", sync: true, want: false},
	}
	for _, tt := range tests {
		if got := publicCommandWALPublishSync(tt.mode, tt.sync); got != tt.want {
			t.Fatalf("publicCommandWALPublishSync(%q, %t)=%t, want %t", tt.mode, tt.sync, got, tt.want)
		}
	}
}

func recvTestErr(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for goroutine")
		return nil
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
