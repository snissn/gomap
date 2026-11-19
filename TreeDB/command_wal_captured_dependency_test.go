package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// A relaxed external-RID prefix needs one dependency drain before its durable
// marker. The marker's journal hook owns that drain, including when it rotates.
// Retaining the same handles must not cause a second physical sync of the cut.
func TestPublicCapturedPrefixDrainsDependenciesOnce(t *testing.T) {
	for _, mode := range []string{"btree", "cow_ordered"} {
		for _, rotate := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rotate=%t", mode, rotate), func(t *testing.T) {
				opts := relaxedCommandWALDurablePrefixOptions(t.TempDir())
				opts.MemtableMode = mode
				opts.ValueLog.PointerThreshold, opts.ValueLog.ForcePointers = 1, true
				if rotate {
					opts.CommandWALSegmentTargetBytes = 1
				}
				database, err := Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				defer database.Close()
				// Exceed the materialized-RID ceiling, requiring the original
				// persistent value-log generation in the recovery closure.
				value := bytes.Repeat([]byte("external"), 9<<10)
				for _, key := range []string{"first", "second"} {
					if err := database.Set([]byte(key), value); err != nil {
						t.Fatal(err)
					}
				}
				var before, after, markers int
				wantDrains := 1
				if mode == "btree" {
					// Ordinary cached Set keeps values in its memtable; there is
					// no physical external-RID debt at this boundary.
					wantDrains = 0
				}
				restore := durabilitycut.Install(func(e durabilitycut.Event) error {
					if e.Resource == durabilitycut.ResourceAuxiliary {
						switch e.Point {
						case durabilitycut.BeforeDependencyFileSync:
							before++
						case durabilitycut.AfterDependencyFileSync:
							after++
						}
					}
					if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.AfterDependencyAppend {
						markers++
						if before != wantDrains || after != wantDrains {
							return fmt.Errorf("marker dependency drains: before=%d after=%d want=%d", before, after, wantDrains)
						}
					}
					return nil
				})
				err = writeEmptyPublicCommandWALSync(database)
				restore()
				if err != nil {
					t.Fatal(err)
				}
				if before != wantDrains || after != wantDrains || markers != 1 {
					t.Fatalf("drain before/after=%d/%d markers=%d, want %d/%d/1", before, after, markers, wantDrains, wantDrains)
				}
				frames := scanPublicCommandWALV2(t, opts.Dir)
				if len(frames) != 3 || frames[2].Kind != commitlog.CommandKindDurablePrefixBarrier {
					t.Fatalf("frames=%+v, want two mutations and one durable marker", frames)
				}
				// Copy before Close can checkpoint: recovery must replay the
				// acknowledged prefix and resolve both persistent pointers.
				image := filepath.Join(t.TempDir(), "crash-image")
				if err := copyPublicCommandWALCrashImage(opts.Dir, image); err != nil {
					t.Fatal(err)
				}
				opts.Dir = image
				recovered, err := Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				defer recovered.Close()
				for _, key := range []string{"first", "second"} {
					requireRawKVValue(t, recovered, []byte(key), value)
				}
			})
		}
	}
}

func TestPublicCapturedPrefixDependencyFailurePrecedesMarker(t *testing.T) {
	opts := relaxedCommandWALDurablePrefixOptions(t.TempDir())
	opts.MemtableMode = "cow_ordered"
	opts.ValueLog.PointerThreshold, opts.ValueLog.ForcePointers = 1, true
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.Set([]byte("external"), bytes.Repeat([]byte("x"), 65<<10)); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("dependency drain failed")
	nextLSN := database.backend.CommandWALNextLSN()
	var markers int
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceAuxiliary && e.Point == durabilitycut.BeforeDependencyFileSync {
			return injected
		}
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.AfterDependencyAppend {
			markers++
		}
		return nil
	})
	err = writeEmptyPublicCommandWALSync(database)
	restore()
	if !errors.Is(err, injected) {
		t.Fatalf("sync error=%v, want original dependency failure", err)
	}
	if markers != 0 {
		t.Fatalf("appended %d markers after failed dependency drain", markers)
	}
	if got := database.backend.CommandWALNextLSN(); got != nextLSN {
		t.Fatalf("dependency failure consumed LSN: next=%d want=%d", got, nextLSN)
	}
	// No marker was assigned. Preserve the existing recoverable pre-append
	// failure semantics, including cancellation of its accepted ticket.
	if err := writeEmptyPublicCommandWALSync(database); err != nil {
		t.Fatalf("retry unassigned boundary: %v", err)
	}
}

func TestPublicCOWCapturedDebtReleaseBeforeAutomaticCheckpoint(t *testing.T) {
	opts := relaxedCommandWALDurablePrefixOptions(t.TempDir())
	opts.MemtableMode = "cow_ordered"
	opts.BackgroundCheckpointInterval = time.Hour
	opts.CommandWALSegmentTargetBytes = 1
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key, oldValue, newValue := []byte("key"), []byte("before"), []byte("after")
	if err := db.Set(key, oldValue); err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(backenddb.WALDirPath(opts.Dir), "commit-l*.log"))
	if err != nil {
		t.Fatalf("initial WAL paths %v: %v", paths, err)
	}
	var oldPath string
	for _, path := range paths {
		frames, err := commitlog.ScanCommandFramesV2(path, commitlog.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if len(frames) != 0 {
			if oldPath != "" || len(frames) != 1 || frames[0].Kind != commitlog.CommandKindRawKVBatch {
				t.Fatalf("unexpected initial frames in %s: %#v", path, frames)
			}
			oldPath = path
		}
	}
	if oldPath == "" {
		t.Fatalf("no WAL file contains the seed: %v", paths)
	}
	if err := db.Set([]byte("rotate"), []byte("first rotation")); err != nil {
		t.Fatal(err)
	}
	held := db.AcquireSnapshot()
	if held == nil {
		t.Fatal("snapshot unavailable")
	}
	defer held.Close()

	durable := make(chan uint64, 1)
	resumeCapture, resumeFile := make(chan struct{}), make(chan struct{})
	var resumeCaptureOnce, resumeFileOnce, capturedOnce, borrowedOnce sync.Once
	releaseCapture := func() { resumeCaptureOnce.Do(func() { close(resumeCapture) }) }
	releaseFile := func() { resumeFileOnce.Do(func() { close(resumeFile) }) }
	defer releaseCapture()
	defer releaseFile()
	oldBorrowed, oldFileResult := make(chan struct{}, 1), make(chan error, 1)
	var checkingCheckpoint, sawOldDependency atomic.Bool
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource != durabilitycut.ResourceCommandWAL {
			return nil
		}
		switch {
		case e.Point == durabilitycut.AfterCapturedWALRetirement:
			capturedOnce.Do(func() {
				durable <- e.LSN
				<-resumeCapture
			})
		case e.Point == durabilitycut.BeforeDependencyFileSync && e.Path == oldPath && e.StablePath != nil:
			if checkingCheckpoint.Load() {
				// Check the exact borrowed descriptor inside the synchronous
				// observer, with retirement allowed to finish in between.
				borrowedOnce.Do(func() {
					oldBorrowed <- struct{}{}
					<-resumeFile
					_, err := e.StablePath.Stat()
					oldFileResult <- err
				})
			} else {
				sawOldDependency.Store(true)
			}
		}
		return nil
	})
	defer restore()

	durableResult := make(chan error, 1)
	go func() { durableResult <- db.SetSync(key, newValue) }()
	waitResult := func(ch <-chan error, phase string) error {
		t.Helper()
		select {
		case err := <-ch:
			return err
		case <-time.After(publicCommandWALCheckpointTestTimeout):
			t.Fatalf("timed out waiting for %s", phase)
			return nil
		}
	}
	var capturedLSN uint64
	select {
	case capturedLSN = <-durable:
	case err := <-durableResult:
		t.Fatalf("SetSync returned before observing captured WAL retirement: %v", err)
	case <-time.After(publicCommandWALCheckpointTestTimeout):
		t.Fatal("SetSync never reached the captured WAL retirement boundary")
	}
	if !sawOldDependency.Load() {
		t.Fatal("fixture did not sync the retained predecessor during SetSync")
	}

	// Install a relaxed successor above the captured durable frontier. Wait for
	// acceptance rather than its ACK, which can depend on the active group.
	suffixKey, suffixValue := []byte("suffix"), []byte("relaxed successor")
	suffixResult := make(chan error, 1)
	go func() { suffixResult <- db.Set(suffixKey, suffixValue) }()
	deadline := time.Now().Add(publicCommandWALCheckpointTestTimeout)
	for {
		got, err := db.Get(suffixKey)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Equal(got, suffixValue) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("successor did not install while SetSync was parked at retirement")
		}
		time.Sleep(time.Millisecond)
	}
	if next := db.backend.CommandWALNextLSN(); next != capturedLSN+2 {
		t.Fatalf("next=%d, want one accepted suffix above capture %d", next, capturedLSN)
	}
	const walFrontier = "treedb.command_wal.durable_wal_lsn"
	before := db.Stats()
	if got := statMapUint64(t, before, walFrontier); got != capturedLSN {
		t.Fatalf("durable frontier at latch=%d, want %d", got, capturedLSN)
	}

	checkingCheckpoint.Store(true)
	db.triggerAutoCheckpointForTest()
	const checkpointCount = "treedb.cache.auto_checkpoint.count"
	const checkpointReason = "treedb.cache.auto_checkpoint.last_reason"
	wantCount := strconv.FormatUint(statMapUint64(t, before, checkpointCount)+1, 10)
	checkpointDone, stopWatching, watcherDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	cache := db.cached
	defer func() {
		releaseCapture()
		releaseFile()
		close(stopWatching)
		<-watcherDone
	}()
	go func() {
		defer close(watcherDone)
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			stats := cache.Stats()
			if stats[checkpointCount] == wantCount && stats[checkpointReason] == "force" {
				close(checkpointDone)
				return
			}
			select {
			case <-tick.C:
			case <-stopWatching:
				return
			}
		}
	}()
	select {
	case <-oldBorrowed:
		releaseCapture()
		if err := waitResult(durableResult, "SetSync retirement"); err != nil {
			t.Fatal(err)
		}
		releaseFile()
		err := waitResult(oldFileResult, "exact WAL descriptor status")
		t.Fatalf("automatic checkpoint borrowed covered predecessor during retirement; descriptor status after SetSync: %v", err)
	case <-checkpointDone:
	case <-time.After(publicCommandWALCheckpointTestTimeout):
		t.Fatal("automatic checkpoint did not advance before the parked SetSync returned")
	}
	after := db.Stats()
	if got := statMapUint64(t, after, walFrontier); got != capturedLSN+1 {
		t.Fatalf("automatic checkpoint durable frontier=%d, want %d", got, capturedLSN+1)
	}
	got, err := held.Get(key)
	if err != nil || !bytes.Equal(got, oldValue) {
		t.Fatalf("held snapshot after checkpoint = %q, %v", got, err)
	}
	releaseCapture()
	if err := waitResult(durableResult, "SetSync ACK"); err != nil {
		t.Fatal(err)
	}
	if err := waitResult(suffixResult, "relaxed successor ACK"); err != nil {
		t.Fatal(err)
	}
	requireRawKVValue(t, db, key, newValue)
	requireRawKVValue(t, db, suffixKey, suffixValue)
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	requireRawKVValue(t, reopened, key, newValue)
	requireRawKVValue(t, reopened, suffixKey, suffixValue)
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}
