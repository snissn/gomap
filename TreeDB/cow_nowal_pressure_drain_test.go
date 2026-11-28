package treedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestPublicProfileNoWALFastStartsCOWPressureDrain(t *testing.T) {
	t.Run("semantic-profile-enables-cow-pressure-drain", func(t *testing.T) {
		dir := t.TempDir()
		opts := OptionsForSemantic(ProfileNoWALFast, dir)
		database, err := Open(opts)
		if err != nil {
			t.Fatalf("Open(OptionsForSemantic(ProfileNoWALFast)): %v", err)
		}
		defer func() {
			if err := database.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
		if !database.cached.COWMode() {
			t.Fatal("OptionsForSemantic(ProfileNoWALFast) did not enable COW mode")
		}
		if database.commandWALCached {
			t.Fatal("ProfileNoWALFast unexpectedly enabled commandWALCached")
		}
		if !database.cached.COWPressureDrainEnabled() {
			t.Fatal("ProfileNoWALFast COW DB did not enable COW pressure drain worker")
		}
	})

	t.Run("nowal-cow-default-options-enables-drain-and-explicit-disable-opts-out", func(t *testing.T) {
		opts := OptionsFor(ProfileNoWALFast, t.TempDir())
		opts.MemtableMode = "cow_btree"
		database, err := Open(opts)
		if err != nil {
			t.Fatalf("Open(default ProfileNoWALFast): %v", err)
		}
		if !database.cached.COWPressureDrainEnabled() {
			_ = database.Close()
			t.Fatal("default ProfileNoWALFast COW DB did not enable COW pressure drain worker")
		}
		if got := database.Stats()["treedb.cache.vlog_generation.leaf_pack.private_cow_default"]; got != "true" {
			_ = database.Close()
			t.Fatalf("default ProfileNoWALFast COW DB private_cow_default = %q, want %q", got, "true")
		}
		if err := database.Close(); err != nil {
			t.Fatalf("Close(default ProfileNoWALFast): %v", err)
		}

		disabledOpts := OptionsFor(ProfileNoWALFast, t.TempDir())
		disabledOpts.MemtableMode = "cow_btree"
		disabledOpts.BackgroundCheckpointInterval = -1
		disabledOpts.BackgroundCheckpointIdleDuration = -1
		disabledDB, err := Open(disabledOpts)
		if err != nil {
			t.Fatalf("Open(disabled ProfileNoWALFast): %v", err)
		}
		defer func() {
			if err := disabledDB.Close(); err != nil {
				t.Errorf("Close(disabled ProfileNoWALFast): %v", err)
			}
		}()
		if disabledDB.cached.COWPressureDrainEnabled() {
			t.Fatal("NoWAL COW DB unexpectedly enabled pressure drain when BackgroundCheckpointInterval and BackgroundCheckpointIdleDuration are disabled")
		}
		if got := disabledDB.Stats()["treedb.cache.vlog_generation.leaf_pack.private_cow_default"]; got != "false" {
			t.Fatalf("disabled NoWAL COW DB private_cow_default = %q, want %q", got, "false")
		}
	})

	t.Run("command-wal-profiles-keep-size-worker-gating", func(t *testing.T) {
		for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
			t.Run(string(profile), func(t *testing.T) {
				opts := OptionsForSemantic(profile, t.TempDir())
				database, err := Open(opts)
				if err != nil {
					t.Fatalf("Open(%s): %v", profile, err)
				}
				if !database.cached.COWPressureDrainEnabled() {
					_ = database.Close()
					t.Fatalf("%s with default MaxWALBytes did not enable pressure drain", profile)
				}
				if err := database.Close(); err != nil {
					t.Fatalf("Close(%s): %v", profile, err)
				}

				disabledOpts := OptionsForSemantic(profile, t.TempDir())
				disabledOpts.BackgroundCheckpointInterval = -1
				disabledOpts.BackgroundCheckpointIdleDuration = -1
				disabledOpts.MaxWALBytes = -1
				disabledDB, err := Open(disabledOpts)
				if err != nil {
					t.Fatalf("Open(%s disabled): %v", profile, err)
				}
				if disabledDB.cached.COWPressureDrainEnabled() {
					_ = disabledDB.Close()
					t.Fatalf("%s unexpectedly enabled pressure drain when size worker disabled", profile)
				}
				if err := disabledDB.Close(); err != nil {
					t.Fatalf("Close(%s disabled): %v", profile, err)
				}
			})
		}
	})
}

func TestPublicProfileNoWALFastSustainedPressureDurabilityAndReopen(t *testing.T) {
	dir := t.TempDir()
	limits := DefaultCOWMemtableLimits()
	limits.MaxInFlightBytes = 8 << 20
	limits.MaxTotalBytes = 32 << 20
	limits.MaxRetiredBytes = 32 << 20
	limits.MaxGenerationBytes = 2 << 20

	opts := OptionsForSemantic(ProfileNoWALFast, dir)
	opts.COWMemtableLimits = limits
	opts.MemtableShards = 2
	opts.FlushThreshold = 1 << 30
	opts.BackgroundIndexVacuumInterval = -1
	opts.DisableBackgroundPrune = true

	database, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !database.cached.COWPressureDrainEnabled() {
		_ = database.Close()
		t.Fatal("ProfileNoWALFast did not enable COW pressure drain")
	}

	const (
		workers = 4
		perW    = 300
	)
	payload := bytes.Repeat([]byte("p"), 2048)
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < perW; i++ {
				if err := database.cached.PrepareNativeProducer(); err != nil {
					errCh <- fmt.Errorf("worker %d iter %d PrepareNativeProducer: %w", workerID, i, err)
					return
				}
				key := fmt.Sprintf("w%02d/k%05d", workerID, i)
				val := make([]byte, len(payload))
				copy(val, payload)
				binary.LittleEndian.PutUint64(val[0:8], uint64(workerID))
				binary.LittleEndian.PutUint64(val[8:16], uint64(i))
				if err := database.Set([]byte(key), val); err != nil {
					errCh <- fmt.Errorf("worker %d iter %d Set: %w", workerID, i, err)
					return
				}
				stats := database.cached.COWMemoryStats()
				if stats.TotalBytes > limits.MaxTotalBytes || stats.ReservedBytes > limits.MaxInFlightBytes {
					errCh <- fmt.Errorf("worker %d iter %d exceeded COW limits: stats=%+v limits=%+v", workerID, i, stats, limits)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	statsBeforeCheckpoint := database.Stats()
	runsBeforeCheckpoint, err := strconv.ParseUint(statsBeforeCheckpoint["treedb.cache.checkpoint.runs"], 10, 64)
	if err != nil {
		t.Fatalf("parse checkpoint.runs (%q): %v", statsBeforeCheckpoint["treedb.cache.checkpoint.runs"], err)
	}
	if runsBeforeCheckpoint != 0 {
		t.Fatalf("NoWAL sustained pressure triggered %d full checkpoints before explicit Checkpoint, want 0", runsBeforeCheckpoint)
	}

	// Explicit Checkpoint and Close must guarantee full durability across reopen.
	if err := database.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := database.Set([]byte("post-checkpoint"), []byte("survives-close")); err != nil {
		t.Fatalf("post-checkpoint Set: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopenOpts := OptionsForSemantic(ProfileNoWALFast, dir)
	reopenOpts.BackgroundIndexVacuumInterval = -1
	reopenOpts.DisableBackgroundPrune = true
	reopened, err := Open(reopenOpts)
	if err != nil {
		t.Fatalf("reopen Open: %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("reopen Close: %v", err)
		}
	}()

	if got, err := reopened.Get([]byte("post-checkpoint")); err != nil || string(got) != "survives-close" {
		t.Fatalf("post-checkpoint after reopen = %q, %v", got, err)
	}
	for w := 0; w < workers; w++ {
		for i := 0; i < perW; i++ {
			key := fmt.Sprintf("w%02d/k%05d", w, i)
			got, err := reopened.Get([]byte(key))
			if err != nil || len(got) != len(payload) {
				t.Fatalf("reopen Get(%q): len=%d err=%v", key, len(got), err)
			}
			gotW := binary.LittleEndian.Uint64(got[0:8])
			gotI := binary.LittleEndian.Uint64(got[8:16])
			if gotW != uint64(w) || gotI != uint64(i) {
				t.Fatalf("reopen Get(%q): header=(%d,%d) want=(%d,%d)", key, gotW, gotI, w, i)
			}
		}
	}

	// Verify reopen also cleanly starts and stops the worker without leaking.
	if !reopened.cached.COWPressureDrainEnabled() {
		t.Fatal("reopened ProfileNoWALFast DB did not enable COW pressure drain")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if reopened.cached.COWMemoryStats().Sources <= limits.MaxSources {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPublicProfileNoWALFastLeafPackReclaimsDiskAndReopens(t *testing.T) {
	dir := t.TempDir()
	opts := OptionsForSemantic(ProfileNoWALFast, dir)
	opts.MemtableShards = 2
	opts.FlushThreshold = 1 << 30
	opts.BackgroundIndexVacuumInterval = -1
	opts.DisableBackgroundPrune = true
	opts.ValueLog.Compression = ValueLogCompressionOff
	opts.ValueLog.Generational.LeafSegmentTargetBytes = 64 << 10

	database, err := Open(opts)
	if err != nil {
		t.Fatalf("Open(ProfileNoWALFast): %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			if err := database.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}
	}()

	if got := database.Stats()["treedb.cache.vlog_generation.leaf_pack.private_cow_default"]; got != "true" {
		t.Fatalf("ProfileNoWALFast private_cow_default = %q, want %q", got, "true")
	}

	leafDirBytes := func() (int, int64) {
		t.Helper()
		entries, err := os.ReadDir(backenddb.LeafLogDirPath(dir))
		if err != nil {
			t.Fatal(err)
		}
		var count int
		var total int64
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".log") {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				t.Fatal(err)
			}
			count++
			total += info.Size()
		}
		return count, total
	}

	// Create several sealed leaf generations via repeated full-range overwrites,
	// holding background leaf-pack off during setup so sealed debt accumulates,
	// then run an explicit idle Checkpoint() and verify dead sealed leaf
	// generations are reclaimed before Close() while CurrentWritableMmap=true.
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	const (
		numKeys = 2048
		rounds  = 6
	)
	for round := 1; round <= rounds; round++ {
		b := database.NewBatch()
		val := bytes.Repeat([]byte{byte(round)}, 256)
		for i := 0; i < numKeys; i++ {
			if err := b.Set([]byte(fmt.Sprintf("k-%05d", i)), val); err != nil {
				_ = b.Close()
				t.Fatal(err)
			}
		}
		if err := b.Write(); err != nil {
			_ = b.Close()
			t.Fatal(err)
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		if err := database.Checkpoint(); err != nil {
			t.Fatalf("round %d Checkpoint: %v", round, err)
		}
	}

	filesBefore, bytesBefore := leafDirBytes()
	if filesBefore < 10 {
		t.Fatalf("expected >= 10 leaf-log segments before reclaim, got %d (%d bytes)", filesBefore, bytesBefore)
	}

	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "0")
	if err := database.Checkpoint(); err != nil {
		t.Fatalf("reclaim Checkpoint: %v", err)
	}

	filesAfter, bytesAfter := leafDirBytes()
	statsAfter := database.Stats()
	t.Logf("ProfileNoWALFast leaf reclaim: before=(%d files, %d bytes) after=(%d files, %d bytes) attempts=%s deleted_bytes=%s last_skip=%q",
		filesBefore, bytesBefore, filesAfter, bytesAfter,
		statsAfter["treedb.cache.vlog_generation.leaf_pack.attempts"],
		statsAfter["treedb.cache.vlog_generation.leaf_pack.gc.deleted_bytes"],
		statsAfter["treedb.cache.vlog_generation.leaf_pack.last_skip_reason"])
	if filesAfter >= filesBefore || bytesAfter >= bytesBefore {
		t.Fatalf("ProfileNoWALFast Checkpoint did not reclaim dead sealed leaf generations: before=(%d files, %d bytes) after=(%d files, %d bytes)",
			filesBefore, bytesBefore, filesAfter, bytesAfter)
	}

	if err := database.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closed = true

	reopenOpts := OptionsForSemantic(ProfileNoWALFast, dir)
	reopenOpts.BackgroundIndexVacuumInterval = -1
	reopenOpts.DisableBackgroundPrune = true
	reopened, err := Open(reopenOpts)
	if err != nil {
		t.Fatalf("reopen Open: %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("reopen Close: %v", err)
		}
	}()

	wantVal := bytes.Repeat([]byte{byte(rounds)}, 256)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("k-%05d", i)
		got, err := reopened.Get([]byte(key))
		if err != nil || !bytes.Equal(got, wantVal) {
			t.Fatalf("reopened Get(%q): len=%d err=%v", key, len(got), err)
		}
	}
}
