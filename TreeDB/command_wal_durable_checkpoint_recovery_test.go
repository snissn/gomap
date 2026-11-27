package treedb

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"
)

func durableDeferredCheckpointTestOptions(dir string) Options {
	opts := OptionsFor(ProfileCommandWALDurable, dir)
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.DisableBackgroundPrune = true
	return opts
}

func TestHelperPublicCommandWALDurableDeferredRotationCheckpointCrash(t *testing.T) {
	if os.Getenv("TREEDB_DURABLE_DEFERRED_CKPT_CRASH_HELPER") != "1" {
		t.Skip("subprocess crash helper")
	}
	dir := os.Getenv("TREEDB_DURABLE_DEFERRED_CKPT_CRASH_DIR")
	if dir == "" {
		t.Fatal("missing TREEDB_DURABLE_DEFERRED_CKPT_CRASH_DIR")
	}

	db, err := Open(durableDeferredCheckpointTestOptions(dir))
	if err != nil {
		t.Fatalf("Open durable: %v", err)
	}

	// Write enough durable data to exercise fixed-WAL rotation, pre-warmed
	// checkpoint reserve adoption, and covered WAL reclamation.
	payload := bytes.Repeat([]byte("p"), 32<<10)
	b := db.NewBatch()
	for i := 0; i < 96; i++ {
		if err := b.Set([]byte(fmt.Sprintf("pre/%04d", i)), payload); err != nil {
			t.Fatalf("pre batch Set %d: %v", i, err)
		}
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		t.Fatalf("pre batch WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("pre batch Close: %v", err)
	}

	// Explicit public Checkpoint takes the durable deferred-rotation path
	// (CheckpointDeferringCommandWALCleanup + PrepareFixedWALCheckpointReserve
	// + coveredByDurableRoot RotateCommandWALActiveSegment(false)).
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	stats := db.Stats()
	if removed, err := strconv.ParseUint(stats["treedb.command_wal.cleanup.removed_segments"], 10, 64); err != nil || removed == 0 {
		t.Fatalf("expected Checkpoint cleanup to reclaim covered WAL segments: removed=%q err=%v", stats["treedb.command_wal.cleanup.removed_segments"], err)
	}
	if activeBytes := db.backend.CommandWALActiveBytes(); activeBytes != 0 {
		t.Fatalf("expected rotated empty active WAL after Checkpoint, got activeBytes=%d", activeBytes)
	}
	if os.Getenv("TREEDB_DURABLE_DEFERRED_CKPT_CRASH_MODE") == "immediate" {
		// Simulate abrupt process crash immediately after Checkpoint while the
		// newly rotated empty fixed WAL still has deferred dirSyncPending.
		os.Exit(0)
	}

	// Append post-checkpoint durable writes onto the newly rotated segment
	// (which carried deferred dirSyncPending) and exit abruptly without Close.
	postVal := bytes.Repeat([]byte("q"), 16<<10)
	if err := db.Set([]byte("post/point"), postVal); err != nil {
		t.Fatalf("post Set: %v", err)
	}
	if err := db.SetSync([]byte("post/sync"), postVal); err != nil {
		t.Fatalf("post SetSync: %v", err)
	}
	b2 := db.NewBatch()
	for i := 0; i < 16; i++ {
		if err := b2.Set([]byte(fmt.Sprintf("post/batch/%04d", i)), postVal); err != nil {
			t.Fatalf("post batch Set %d: %v", i, err)
		}
	}
	if err := b2.WriteSync(); err != nil {
		_ = b2.Close()
		t.Fatalf("post batch WriteSync: %v", err)
	}
	if err := b2.Close(); err != nil {
		t.Fatalf("post batch Close: %v", err)
	}

	// Simulate an abrupt process crash after durable post-checkpoint ACKs.
	os.Exit(0)
}

func TestPublicCommandWALDurableDeferredRotationCheckpointReopenAndCrashRecovery(t *testing.T) {
	t.Run("clean-reopen", func(t *testing.T) {
		dir := t.TempDir()
		db, err := Open(durableDeferredCheckpointTestOptions(dir))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		preVal := bytes.Repeat([]byte("a"), 32<<10)
		postVal := bytes.Repeat([]byte("b"), 16<<10)

		b := db.NewBatch()
		for i := 0; i < 64; i++ {
			if err := b.Set([]byte(fmt.Sprintf("pre/%04d", i)), preVal); err != nil {
				t.Fatalf("pre Set %d: %v", i, err)
			}
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("pre WriteSync: %v", err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("pre batch Close: %v", err)
		}

		if err := db.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
		appliedAfterCkpt := db.backend.State().AppliedCommandLSN
		if appliedAfterCkpt == 0 {
			t.Fatal("expected positive AppliedCommandLSN after Checkpoint")
		}
		if activeBytes := db.backend.CommandWALActiveBytes(); activeBytes != 0 {
			t.Fatalf("expected active WAL rotated empty after Checkpoint, got %d", activeBytes)
		}

		if err := db.Set([]byte("post/set"), postVal); err != nil {
			t.Fatalf("post Set: %v", err)
		}
		if err := db.SetSync([]byte("post/setsync"), postVal); err != nil {
			t.Fatalf("post SetSync: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		reopened, err := Open(durableDeferredCheckpointTestOptions(dir))
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer func() { _ = reopened.Close() }()
		for i := 0; i < 64; i++ {
			requireRawKVValue(t, reopened, []byte(fmt.Sprintf("pre/%04d", i)), preVal)
		}
		requireRawKVValue(t, reopened, []byte("post/set"), postVal)
		requireRawKVValue(t, reopened, []byte("post/setsync"), postVal)
		if got := reopened.backend.State().AppliedCommandLSN; got <= appliedAfterCkpt {
			t.Fatalf("reopened AppliedCommandLSN=%d, want > %d", got, appliedAfterCkpt)
		}
	})

	t.Run("crash-immediately-after-checkpoint", func(t *testing.T) {
		dir := t.TempDir()
		exe, err := os.Executable()
		if err != nil {
			t.Fatalf("os.Executable: %v", err)
		}
		cmd := exec.Command(exe, "-test.run=^TestHelperPublicCommandWALDurableDeferredRotationCheckpointCrash$", "-test.v")
		cmd.Env = append(os.Environ(),
			"TREEDB_DURABLE_DEFERRED_CKPT_CRASH_HELPER=1",
			"TREEDB_DURABLE_DEFERRED_CKPT_CRASH_DIR="+dir,
			"TREEDB_DURABLE_DEFERRED_CKPT_CRASH_MODE=immediate",
		)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("crash helper failed: %v\n%s", err, out)
		}

		reopened, err := Open(durableDeferredCheckpointTestOptions(dir))
		if err != nil {
			t.Fatalf("reopen after immediate post-checkpoint crash: %v", err)
		}
		preVal := bytes.Repeat([]byte("p"), 32<<10)
		for i := 0; i < 96; i++ {
			requireRawKVValue(t, reopened, []byte(fmt.Sprintf("pre/%04d", i)), preVal)
		}
		postVal := bytes.Repeat([]byte("r"), 16<<10)
		if err := reopened.SetSync([]byte("after-recovery/sync"), postVal); err != nil {
			_ = reopened.Close()
			t.Fatalf("post-recovery SetSync: %v", err)
		}
		if err := reopened.Checkpoint(); err != nil {
			_ = reopened.Close()
			t.Fatalf("post-recovery Checkpoint: %v", err)
		}
		if err := reopened.Close(); err != nil {
			t.Fatalf("post-recovery Close: %v", err)
		}

		finalDB, err := Open(durableDeferredCheckpointTestOptions(dir))
		if err != nil {
			t.Fatalf("final reopen: %v", err)
		}
		defer func() { _ = finalDB.Close() }()
		requireRawKVValue(t, finalDB, []byte("pre/0000"), preVal)
		requireRawKVValue(t, finalDB, []byte("pre/0095"), preVal)
		requireRawKVValue(t, finalDB, []byte("after-recovery/sync"), postVal)
	})

	t.Run("crash-recovery-after-post-checkpoint-writes", func(t *testing.T) {
		dir := t.TempDir()
		exe, err := os.Executable()
		if err != nil {
			t.Fatalf("os.Executable: %v", err)
		}
		cmd := exec.Command(exe, "-test.run=^TestHelperPublicCommandWALDurableDeferredRotationCheckpointCrash$", "-test.v")
		cmd.Env = append(os.Environ(),
			"TREEDB_DURABLE_DEFERRED_CKPT_CRASH_HELPER=1",
			"TREEDB_DURABLE_DEFERRED_CKPT_CRASH_DIR="+dir,
		)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("crash helper failed: %v\n%s", err, out)
		}

		reopened, err := Open(durableDeferredCheckpointTestOptions(dir))
		if err != nil {
			t.Fatalf("reopen after crash: %v", err)
		}
		preVal := bytes.Repeat([]byte("p"), 32<<10)
		postVal := bytes.Repeat([]byte("q"), 16<<10)
		for i := 0; i < 96; i++ {
			requireRawKVValue(t, reopened, []byte(fmt.Sprintf("pre/%04d", i)), preVal)
		}
		requireRawKVValue(t, reopened, []byte("post/point"), postVal)
		requireRawKVValue(t, reopened, []byte("post/sync"), postVal)
		for i := 0; i < 16; i++ {
			requireRawKVValue(t, reopened, []byte(fmt.Sprintf("post/batch/%04d", i)), postVal)
		}
		// Verify a second explicit Checkpoint + clean reopen stays consistent.
		if err := reopened.Checkpoint(); err != nil {
			_ = reopened.Close()
			t.Fatalf("post-recovery Checkpoint: %v", err)
		}
		if err := reopened.Close(); err != nil {
			t.Fatalf("post-recovery Close: %v", err)
		}

		finalDB, err := Open(durableDeferredCheckpointTestOptions(dir))
		if err != nil {
			t.Fatalf("final reopen: %v", err)
		}
		defer func() { _ = finalDB.Close() }()
		requireRawKVValue(t, finalDB, []byte("pre/0000"), preVal)
		requireRawKVValue(t, finalDB, []byte("pre/0095"), preVal)
		requireRawKVValue(t, finalDB, []byte("post/point"), postVal)
		requireRawKVValue(t, finalDB, []byte("post/sync"), postVal)
		requireRawKVValue(t, finalDB, []byte("post/batch/0015"), postVal)
	})
}
