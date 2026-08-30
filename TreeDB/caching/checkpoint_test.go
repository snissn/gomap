package caching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCachingDB_CheckpointContextCancelsWhileWaitingForFlushOwnership(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.flushMu.Lock()
	defer db.flushMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- db.CheckpointContext(ctx) }()
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("CheckpointContext error=%v, want context.Canceled", err)
		}
	case <-time.After(withRaceTimeout(2 * time.Second)):
		t.Fatal("CheckpointContext did not cancel while waiting for flush ownership")
	}
}

func TestLockCheckpointMutexContextCancellationLeavesNoWaiters(t *testing.T) {
	var mu sync.Mutex
	mu.Lock()
	baselineGoroutines := runtime.NumGoroutine()

	const waiters = 64
	contexts := make([]context.CancelFunc, waiters)
	done := make(chan error, waiters)
	var callers sync.WaitGroup
	for i := range contexts {
		ctx, cancel := context.WithCancel(context.Background())
		contexts[i] = cancel
		callers.Add(1)
		go func() {
			defer callers.Done()
			done <- lockCheckpointMutexContext(ctx, &mu)
		}()
	}
	time.Sleep(20 * time.Millisecond)
	for _, cancel := range contexts {
		cancel()
	}
	for range contexts {
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("lock wait error=%v, want context.Canceled", err)
		}
	}
	callers.Wait()

	runtime.Gosched()
	blockedGoroutines := runtime.NumGoroutine()
	mu.Unlock()
	if blockedGoroutines > baselineGoroutines+waiters/4 {
		t.Fatalf("goroutines after cancellation=%d baseline=%d; canceled flush waiters remain parked", blockedGoroutines, baselineGoroutines)
	}
	if !mu.TryLock() {
		t.Fatal("canceled checkpoint lock calls left queued mutex waiters")
	}
	mu.Unlock()
}

func TestCachingDB_CheckpointContextCancelsBeforeFrontierDrain(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	reached := make(chan struct{})
	release := make(chan struct{})
	db.testBeforeCheckpointFrontierDrain = func() {
		close(reached)
		<-release
	}
	defer func() { db.testBeforeCheckpointFrontierDrain = nil }()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- db.CheckpointContext(ctx) }()
	awaitCheckpointTestSignal(t, reached, "checkpoint frontier drain")
	cancel()
	close(release)

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("CheckpointContext error=%v, want context.Canceled", err)
		}
	case <-time.After(withRaceTimeout(2 * time.Second)):
		t.Fatal("CheckpointContext did not cancel before draining the frontier")
	}
}

func TestCachingDB_CheckpointContextQueuesBehindActiveReaders(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.writeMu.RLock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- db.CheckpointContext(ctx) }()
	deadline := time.Now().Add(withRaceTimeout(2 * time.Second))
	for !db.checkpointing.Load() {
		if time.Now().After(deadline) {
			db.writeMu.RUnlock()
			t.Fatal("checkpoint did not reach write ownership wait")
		}
		time.Sleep(time.Millisecond)
	}
	// Allow the checkpoint waiter to enter RWMutex.Lock after publishing the
	// checkpoint state observed above.
	time.Sleep(withRaceTimeout(10 * time.Millisecond))

	lateReader := make(chan error, 1)
	go func() {
		err := db.beginDirectWrite()
		if err == nil {
			db.writeMu.RUnlock()
		}
		lateReader <- err
	}()
	select {
	case err := <-lateReader:
		db.writeMu.RUnlock()
		t.Fatalf("later writer bypassed queued checkpoint: %v", err)
	case <-time.After(withRaceTimeout(25 * time.Millisecond)):
	}

	db.writeMu.RUnlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("CheckpointContext: %v", err)
		}
	case <-time.After(withRaceTimeout(2 * time.Second)):
		t.Fatal("queued checkpoint did not finish after reader released")
	}
	select {
	case err := <-lateReader:
		if err != nil {
			t.Fatalf("late writer after checkpoint: %v", err)
		}
	case <-time.After(withRaceTimeout(2 * time.Second)):
		t.Fatal("timed out waiting for late writer after checkpoint")
	}
}

func TestCachingDB_CanceledCheckpointDoesNotLeaveWriteWaiter(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.writeMu.RLock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- db.CheckpointContext(ctx) }()
	deadline := time.Now().Add(withRaceTimeout(2 * time.Second))
	for !db.checkpointing.Load() {
		if time.Now().After(deadline) {
			db.writeMu.RUnlock()
			t.Fatal("checkpoint did not reach write ownership wait")
		}
		time.Sleep(time.Millisecond)
	}
	// Checkpointing is published immediately before the contended write-lock
	// path. Give the waiter time to enter that path before cancellation.
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			db.writeMu.RUnlock()
			t.Fatalf("CheckpointContext error=%v, want context.Canceled", err)
		}
	case <-time.After(withRaceTimeout(2 * time.Second)):
		db.writeMu.RUnlock()
		t.Fatal("CheckpointContext did not cancel while waiting for write ownership")
	}

	lateWriter := make(chan error, 1)
	go func() {
		err := db.beginDirectWrite()
		if err == nil {
			db.writeMu.RUnlock()
		}
		lateWriter <- err
	}()
	select {
	case err := <-lateWriter:
		if err != nil {
			db.writeMu.RUnlock()
			t.Fatalf("beginDirectWrite after canceled checkpoint: %v", err)
		}
	case <-time.After(withRaceTimeout(100 * time.Millisecond)):
		db.writeMu.RUnlock()
		t.Fatal("canceled checkpoint left a writeMu waiter blocking later writes")
	}
	db.writeMu.RUnlock()
}

func countCommitLogFiles(entries []os.DirEntry) int {
	n := 0
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		if strings.HasPrefix(ent.Name(), "commit-") {
			n++
		}
	}
	return n
}

func awaitCheckpointTestSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(withRaceTimeout(2 * time.Second)):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func awaitCheckpointWriterWaiters(t *testing.T, db *DB, want int64) {
	t.Helper()
	deadline := time.Now().Add(withRaceTimeout(2 * time.Second))
	for db.writeWaitForCheckpointActive.Load() != want {
		if time.Now().After(deadline) {
			t.Fatalf("checkpoint writer waiters=%d, want %d", db.writeWaitForCheckpointActive.Load(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCachingDB_Checkpoint_TrimsWAL(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:           1,
		ValueLogPointerThreshold: 2 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	rotateWAL := func() {
		t.Helper()
		db.writeMu.Lock()
		db.mu.Lock()
		var err error
		for i := range db.lanes {
			err = db.rotateWALLocked(&db.lanes[i])
			if err != nil {
				break
			}
		}
		db.mu.Unlock()
		db.writeMu.Unlock()
		if err != nil {
			t.Fatalf("rotateWAL: %v", err)
		}
	}

	// Create multiple WAL segments by rotating explicitly between writes.
	val := bytes.Repeat([]byte("v"), 1<<20) // 1MiB
	if err := db.Set([]byte("k000"), val); err != nil {
		t.Fatalf("Set: %v", err)
	}
	rotateWAL()
	if err := db.Set([]byte("k001"), val); err != nil {
		t.Fatalf("Set: %v", err)
	}

	walDir := filepath.Join(dir, "wal")
	before, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("ReadDir(wal): %v", err)
	}

	walFilesBefore := countCommitLogFiles(before)
	if walFilesBefore < 2 {
		t.Fatalf("expected multiple WAL segments before checkpoint, got %d", walFilesBefore)
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	after, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("ReadDir(wal) after: %v", err)
	}

	walFilesAfter := countCommitLogFiles(after)
	if walFilesAfter != len(db.lanes) {
		t.Fatalf("expected exactly %d WAL segments after checkpoint, got %d", len(db.lanes), walFilesAfter)
	}
}

func TestCachingDB_CheckpointExternalCommandWALAdmitsAfterFrontierCut(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{
		ExternalCommandWAL: true,
		FlushThreshold:     1 << 30,
		JournalLanes:       1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// A command-WAL owner installs this hook to rotate the active segment and
	// capture the LSN covered by the checkpoint. The latch hooks below expose all
	// three state-machine boundaries without relying on scheduler sleeps.
	db.SetCommandWALCheckpointCutoverHook(func() {})
	writePoint := func(key, value string) error {
		return db.SetAfterCommandWALAppend([]byte(key), []byte(value), func() error { return nil })
	}
	if err := writePoint("pre-cut", "pre-value"); err != nil {
		t.Fatalf("seed pre-cut point: %v", err)
	}

	beforeCapture := make(chan struct{})
	releaseCapture := make(chan struct{})
	afterCapture := make(chan struct{})
	releaseAfterCapture := make(chan struct{})
	beforeDrain := make(chan struct{})
	releaseDrain := make(chan struct{})
	db.testBeforeCheckpointFrontierCapture = func() {
		close(beforeCapture)
		<-releaseCapture
	}
	db.testAfterCheckpointFrontierCapture = func() {
		close(afterCapture)
		<-releaseAfterCapture
	}
	db.testBeforeCheckpointFrontierDrain = func() {
		close(beforeDrain)
		<-releaseDrain
	}

	checkpointDone := make(chan error, 1)
	go func() { checkpointDone <- db.Checkpoint() }()
	awaitCheckpointTestSignal(t, beforeCapture, "pre-frontier capture latch")

	cutoverWriteDone := make(chan error, 1)
	go func() { cutoverWriteDone <- writePoint("during-cut", "during-value") }()
	awaitCheckpointWriterWaiters(t, db, 1)
	select {
	case err := <-cutoverWriteDone:
		t.Fatalf("write crossed the frontier cut before release: %v", err)
	default:
	}
	close(releaseCapture)
	awaitCheckpointTestSignal(t, afterCapture, "post-frontier capture latch")
	if err := <-cutoverWriteDone; err != nil {
		t.Fatalf("write waiting at frontier cut: %v", err)
	}

	if err := writePoint("after-cut", "after-value"); err != nil {
		t.Fatalf("post-frontier point write: %v", err)
	}
	close(releaseAfterCapture)
	awaitCheckpointTestSignal(t, beforeDrain, "pre-frontier drain latch")

	writeBatch := func(syncWrite bool, key, value string) error {
		batch := db.NewBatch()
		defer func() { _ = batch.Close() }()
		if err := batch.Set([]byte(key), []byte(value)); err != nil {
			return err
		}
		return batch.WriteAfterCommandWALAppend(syncWrite, func() error { return nil })
	}
	if err := writeBatch(false, "drain-write", "drain-value"); err != nil {
		t.Fatalf("post-frontier Write: %v", err)
	}
	if err := writeBatch(true, "drain-writesync", "drain-sync-value"); err != nil {
		t.Fatalf("post-frontier WriteSync: %v", err)
	}
	select {
	case err := <-checkpointDone:
		t.Fatalf("checkpoint completed while frontier drain was latched: %v", err)
	default:
	}
	close(releaseDrain)
	if err := <-checkpointDone; err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	got, err := backend.Get([]byte("pre-cut"))
	if err != nil || !bytes.Equal(got, []byte("pre-value")) {
		t.Fatalf("backend pre-cut value=(%q, %v), want pre-value", got, err)
	}
	for _, key := range []string{"during-cut", "after-cut", "drain-write", "drain-writesync"} {
		got, err := backend.Get([]byte(key))
		if err != nil {
			t.Fatalf("backend Get(%q): %v", key, err)
		}
		if got != nil {
			t.Fatalf("active checkpoint included post-frontier key %q=%q", key, got)
		}
		if got, err = db.Get([]byte(key)); err != nil || got == nil {
			t.Fatalf("cached Get(%q)=(%q, %v), want post-frontier value", key, got, err)
		}
	}

	stats := db.Stats()
	if got := requireStatUint64(t, stats, "treedb.cache.write.wait.frontier_cutover.count_total"); got != 1 {
		t.Fatalf("frontier_cutover waits=%d, want 1", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.write.wait.checkpoint_drain.count_total"); got != 0 {
		t.Fatalf("checkpoint_drain waits=%d, want 0", got)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.write.post_frontier_admission.count_total"); got != 4 {
		t.Fatalf("post-frontier admissions=%d, want 4", got)
	}

	// A successful checkpoint remains a strict boundary: only a later checkpoint
	// may publish the post-cut generation to the backend.
	db.testBeforeCheckpointFrontierCapture = nil
	db.testAfterCheckpointFrontierCapture = nil
	db.testBeforeCheckpointFrontierDrain = nil
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("second Checkpoint: %v", err)
	}
	for _, key := range []string{"during-cut", "after-cut", "drain-write", "drain-writesync"} {
		if got, err := backend.Get([]byte(key)); err != nil || got == nil {
			t.Fatalf("backend Get(%q) after second checkpoint=(%q, %v), want value", key, got, err)
		}
	}
}

func TestCachingDB_CheckpointDrainRetainsWriterGateWithoutCommandWALCutover(t *testing.T) {
	tests := []struct {
		name  string
		opts  Options
		write func(*DB, string, string) error
	}{
		{
			name: "cached_redo_wal",
			opts: Options{FlushThreshold: 1 << 30, JournalLanes: 1},
			write: func(db *DB, key, value string) error {
				return db.Set([]byte(key), []byte(value))
			},
		},
		{
			name: "external_command_wal_without_cutover_hook",
			opts: Options{ExternalCommandWAL: true, FlushThreshold: 1 << 30, JournalLanes: 1},
			write: func(db *DB, key, value string) error {
				return db.SetAfterCommandWALAppend([]byte(key), []byte(value), func() error { return nil })
			},
		},
		{
			name: "unsafe_wal_off",
			opts: Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30, JournalLanes: 1},
			write: func(db *DB, key, value string) error {
				return db.Set([]byte(key), []byte(value))
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(t.TempDir(), NewMockBackend(), tc.opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer func() { _ = db.Close() }()
			if err := tc.write(db, "pre-cut", "pre-value"); err != nil {
				t.Fatalf("seed: %v", err)
			}

			beforeDrain := make(chan struct{})
			releaseDrain := make(chan struct{})
			db.testBeforeCheckpointFrontierDrain = func() {
				close(beforeDrain)
				<-releaseDrain
			}
			checkpointDone := make(chan error, 1)
			go func() { checkpointDone <- db.Checkpoint() }()
			awaitCheckpointTestSignal(t, beforeDrain, "checkpoint drain latch")

			writeDone := make(chan error, 1)
			go func() { writeDone <- tc.write(db, "during-drain", "during-value") }()
			awaitCheckpointWriterWaiters(t, db, 1)
			select {
			case err := <-writeDone:
				t.Fatalf("write bypassed checkpoint drain without a safe command-WAL cut: %v", err)
			default:
			}
			close(releaseDrain)
			if err := <-checkpointDone; err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			if err := <-writeDone; err != nil {
				t.Fatalf("write after checkpoint: %v", err)
			}
			db.testBeforeCheckpointFrontierDrain = nil

			stats := db.Stats()
			if got := requireStatUint64(t, stats, "treedb.cache.write.wait.checkpoint_drain.count_total"); got != 1 {
				t.Fatalf("checkpoint_drain waits=%d, want 1", got)
			}
			if got := requireStatUint64(t, stats, "treedb.cache.write.post_frontier_admission.count_total"); got != 0 {
				t.Fatalf("post-frontier admissions=%d, want 0", got)
			}
		})
	}
}

func TestCachingDB_DirectWriteGateRechecksCheckpointAfterPublicWait(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.checkpointMu.Lock()
	db.checkpointing.Store(true)
	db.checkpointMu.Unlock()

	acquired := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		if err := db.beginDirectWrite(); err != nil {
			done <- err
			return
		}
		close(acquired)
		db.writeMu.RUnlock()
		done <- nil
	}()

	select {
	case <-acquired:
		t.Fatalf("direct write gate acquired while checkpointing")
	case <-time.After(25 * time.Millisecond):
	}

	db.checkpointMu.Lock()
	db.checkpointing.Store(false)
	db.checkpointCond.Broadcast()
	db.checkpointMu.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("beginDirectWrite: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("direct write gate did not resume after checkpoint")
	}
}

func TestCachingDBCommandWALAppendCallbacksRejectAfterCloseStarts(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		ExternalCommandWAL: true,
		FlushThreshold:     1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.closing.Store(true)
	called := false
	err = db.SetAfterCommandWALAppend([]byte("k"), []byte("v"), func() error {
		called = true
		return nil
	})
	if !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("SetAfterCommandWALAppend error=%v, want ErrClosed", err)
	}
	if called {
		t.Fatalf("SetAfterCommandWALAppend called command WAL append after close started")
	}

	b := db.NewBatch()
	if err := b.Set([]byte("batch-k"), []byte("batch-v")); err != nil {
		t.Fatalf("batch Set: %v", err)
	}
	err = b.WriteAfterCommandWALAppend(true, func() error {
		called = true
		return nil
	})
	if !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("WriteAfterCommandWALAppend error=%v, want ErrClosed", err)
	}
	if called {
		t.Fatalf("WriteAfterCommandWALAppend called command WAL append after close started")
	}
}

func TestCachingDB_ExclusiveWriteGateRechecksCheckpointAfterPublicWait(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.checkpointMu.Lock()
	db.checkpointing.Store(true)
	db.checkpointMu.Unlock()

	acquired := make(chan struct{})
	done := make(chan struct{})
	go func() {
		db.beginExclusiveWrite()
		close(acquired)
		db.writeMu.Unlock()
		close(done)
	}()

	select {
	case <-acquired:
		t.Fatalf("exclusive write gate acquired while checkpointing")
	case <-time.After(25 * time.Millisecond):
	}

	db.checkpointMu.Lock()
	db.checkpointing.Store(false)
	db.checkpointCond.Broadcast()
	db.checkpointMu.Unlock()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("exclusive write gate did not resume after checkpoint")
	}
}

func TestCachingDB_ExclusiveWriteWithFlushMuHeldReleasesFlushMuWhileWaiting(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.checkpointMu.Lock()
	db.maintenanceActive.Store(true)
	db.checkpointMu.Unlock()

	db.flushMu.Lock()
	acquiredWrite := make(chan struct{})
	done := make(chan struct{})
	go func() {
		db.beginExclusiveWriteWithFlushMuHeld()
		close(acquiredWrite)
		db.writeMu.Unlock()
		db.flushMu.Unlock()
		close(done)
	}()

	flushMuReleased := make(chan struct{})
	go func() {
		db.flushMu.Lock()
		close(flushMuReleased)
		db.flushMu.Unlock()
	}()

	select {
	case <-flushMuReleased:
	case <-time.After(2 * time.Second):
		t.Fatalf("exclusive writer did not release flushMu while waiting for maintenance")
	}
	awaitCheckpointWriterWaiters(t, db, 1)
	select {
	case <-acquiredWrite:
		t.Fatalf("exclusive writer acquired while maintenance is active")
	default:
	}

	db.checkpointMu.Lock()
	db.maintenanceActive.Store(false)
	db.checkpointCond.Broadcast()
	db.checkpointMu.Unlock()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("exclusive writer with flushMu held did not resume after maintenance")
	}
	stats := db.Stats()
	if got := requireStatUint64(t, stats, "treedb.cache.write.wait.maintenance.count_total"); got != 1 {
		t.Fatalf("maintenance waits=%d, want 1", got)
	}
}

func TestCachingDB_CheckpointWaiterReleasesFlushMu(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.checkpointMu.Lock()
	db.checkpointing.Store(true)

	done := make(chan error, 1)
	go func() { done <- db.Checkpoint() }()

	observedFlushMuHeld := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if db.flushMu.TryLock() {
			db.flushMu.Unlock()
		} else {
			observedFlushMuHeld = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !observedFlushMuHeld {
		db.checkpointMu.Unlock()
		t.Fatalf("checkpoint waiter never acquired flushMu before waiting")
	}

	db.checkpointMu.Unlock()

	flushMuReleased := false
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if db.flushMu.TryLock() {
			flushMuReleased = true
			db.flushMu.Unlock()
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !flushMuReleased {
		t.Fatalf("checkpoint waiter did not release flushMu while waiting")
	}

	db.checkpointMu.Lock()
	db.checkpointing.Store(false)
	db.checkpointCond.Broadcast()
	db.checkpointMu.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Checkpoint returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("checkpoint waiter did not complete after broadcast")
	}
}

func TestCachingDB_AutoCheckpoint_TrimsWAL(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:           1,
		ValueLogPointerThreshold: 16 << 20,
		JournalLanes:             1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.testSkipVlogCheckpointKick = true

	db.StartAutoCheckpoint(5*time.Millisecond, 1<<20 /* 1MiB */, 0)

	val := bytes.Repeat([]byte("v"), 512<<10) // 512KiB
	for i := 0; i < 40; i++ {
		k := []byte(fmt.Sprintf("k%03d", i))
		if err := db.Set(k, val); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	walDir := filepath.Join(dir, "wal")
	deadline := time.Now().Add(2 * time.Second)
	for {
		ents, err := os.ReadDir(walDir)
		if err != nil {
			t.Fatalf("ReadDir(wal): %v", err)
		}
		walFiles := countCommitLogFiles(ents)
		if walFiles == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for auto checkpoint to trim WAL (files=%d)", walFiles)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCachingDB_AutoCheckpoint_IdleTrigger_TrimsWAL(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:           1,
		ValueLogPointerThreshold: 2 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.testSkipVlogCheckpointKick = true

	db.StartAutoCheckpoint(0, 0, 100*time.Millisecond)

	rotateWAL := func() {
		t.Helper()
		db.writeMu.Lock()
		db.mu.Lock()
		var err error
		for i := range db.lanes {
			err = db.rotateWALLocked(&db.lanes[i])
			if err != nil {
				break
			}
		}
		db.mu.Unlock()
		db.writeMu.Unlock()
		if err != nil {
			t.Fatalf("rotateWAL: %v", err)
		}
	}

	// Create multiple WAL segments by rotating explicitly between writes.
	val := bytes.Repeat([]byte("v"), 1<<20) // 1MiB
	if err := db.Set([]byte("k000"), val); err != nil {
		t.Fatalf("Set: %v", err)
	}
	rotateWAL()
	if err := db.Set([]byte("k001"), val); err != nil {
		t.Fatalf("Set: %v", err)
	}

	walDir := filepath.Join(dir, "wal")
	before, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("ReadDir(wal): %v", err)
	}
	walFilesBefore := countCommitLogFiles(before)
	if walFilesBefore < 2 {
		t.Fatalf("expected multiple WAL segments before idle checkpoint, got %d", walFilesBefore)
	}

	deadline := time.Now().Add(withRaceTimeout(2 * time.Second))
	for {
		ents, err := os.ReadDir(walDir)
		if err != nil {
			t.Fatalf("ReadDir(wal): %v", err)
		}
		walFiles := countCommitLogFiles(ents)
		if walFiles == len(db.lanes) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for idle auto checkpoint to trim WAL (files=%d)", walFiles)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The WAL directory can reflect a completed trim slightly before the
	// auto-checkpoint goroutine updates its counters. Poll stats to avoid a
	// timing-dependent flake.
	deadline = time.Now().Add(withRaceTimeout(2 * time.Second))
	for {
		stats := db.Stats()
		if stats == nil {
			t.Fatalf("Stats() returned nil")
		}
		n, err := strconv.ParseUint(stats["treedb.cache.auto_checkpoint.count"], 10, 64)
		if err != nil {
			t.Fatalf("parse auto checkpoint count: %v", err)
		}
		reason := stats["treedb.cache.auto_checkpoint.last_reason"]
		if n > 0 && reason == "idle" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for idle auto checkpoint stats (count=%d reason=%q)", n, reason)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func withRaceTimeout(d time.Duration) time.Duration {
	if testRaceEnabled {
		return d * 5
	}
	return d
}

func TestCachingDB_AutoCheckpoint_IdleTrigger_SkipsTinyWrites(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:           1,
		ValueLogPointerThreshold: 16 << 20,
		JournalLanes:             1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.testSkipVlogCheckpointKick = true

	db.StartAutoCheckpoint(0, 0, 50*time.Millisecond)

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	stats := db.Stats()
	if stats == nil {
		t.Fatalf("Stats() returned nil")
	}
	n, err := strconv.ParseUint(stats["treedb.cache.auto_checkpoint.count"], 10, 64)
	if err != nil {
		t.Fatalf("parse auto checkpoint count: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected no auto checkpoint for tiny write burst, got %d", n)
	}
}

func TestCachingDB_AutoCheckpoint_SizeTrigger_TrimsWAL(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:           1,
		ValueLogPointerThreshold: 16 << 20,
		JournalLanes:             1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.testSkipVlogCheckpointKick = true

	db.StartAutoCheckpoint(0, 1<<20 /* 1MiB */, 0)

	// Force WAL rotation by exceeding the ~10MiB WAL reuse threshold.
	val := bytes.Repeat([]byte("v"), 11<<20) // 11MiB
	if err := db.Set([]byte("k"), val); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if got := db.effectiveWALBytes(); got < 1<<20 {
		t.Fatalf("expected WAL bytes >= 1MiB, got %d", got)
	}
	db.autoCheckpointMaxWALBytes.Store(1 << 20)
	db.autoCheckpointSizeArmed.Store(true)
	db.maybeAutoCheckpoint(1<<20, autoCheckpointModeSize)

	walDir := filepath.Join(dir, "wal")
	deadline := time.Now().Add(withRaceTimeout(2 * time.Second))
	for {
		stats := db.Stats()
		if stats == nil {
			t.Fatalf("Stats() returned nil")
		}
		n, err := strconv.ParseUint(stats["treedb.cache.auto_checkpoint.count"], 10, 64)
		if err != nil {
			t.Fatalf("parse auto checkpoint count: %v", err)
		}
		if n > 0 && stats["treedb.cache.auto_checkpoint.last_reason"] == "size" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for size auto checkpoint to run (count=%d reason=%q)", n, stats["treedb.cache.auto_checkpoint.last_reason"])
		}
		time.Sleep(10 * time.Millisecond)
	}

	deadline = time.Now().Add(withRaceTimeout(2 * time.Second))
	for {
		ents, err := os.ReadDir(walDir)
		if err != nil {
			t.Fatalf("ReadDir(wal): %v", err)
		}
		walFiles := countCommitLogFiles(ents)
		if walFiles >= len(db.lanes) && walFiles <= len(db.lanes)+1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"timed out waiting for size checkpoint to trim WAL (files=%d expected=%d..%d)",
				walFiles,
				len(db.lanes),
				len(db.lanes)+1,
			)
		}
		time.Sleep(10 * time.Millisecond)
	}

	ents, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("ReadDir(wal): %v", err)
	}
	walFiles := countCommitLogFiles(ents)
	if walFiles < len(db.lanes) || walFiles > len(db.lanes)+1 {
		t.Fatalf("expected %d..%d WAL segments after size checkpoint, got %d", len(db.lanes), len(db.lanes)+1, walFiles)
	}
}

func TestCachingDB_AutoCheckpoint_SizeTrigger_RearmsAfterSuccessfulNoRelief(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const maxWALBytes int64 = 1024
	db.SetAutoCheckpointWALBytesHook(func() int64 { return maxWALBytes })
	db.autoCheckpointSizeArmed.Store(true)

	db.maybeAutoCheckpoint(maxWALBytes, autoCheckpointModeSize)
	if !db.autoCheckpointSizeArmed.Load() {
		t.Fatal("successful size checkpoint without relief left gate disarmed")
	}

	db.maybeAutoCheckpoint(maxWALBytes, autoCheckpointModeSize)
	if got := db.autoCheckpointCount.Load(); got != 2 {
		t.Fatalf("successful no-relief size passes=%d want 2", got)
	}
}

func TestCachingDB_AutoCheckpoint_SizeTrigger_SeedsExistingWAL(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	preexisting := []string{
		filepath.Join(walDir, "commit-l0-000010.log"),
	}
	for _, path := range preexisting {
		if err := os.WriteFile(path, bytes.Repeat([]byte("x"), 2<<20), 0o600); err != nil {
			t.Fatalf("WriteFile(preexisting WAL): %v", err)
		}
	}

	backend := NewMockBackend()
	db, err := Open(dir, backend, Options{
		FlushThreshold:           1,
		ValueLogPointerThreshold: 16 << 20,
		JournalLanes:             1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.testSkipVlogCheckpointKick = true

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	db.autoCheckpointSizeArmed.Store(true)
	db.maybeAutoCheckpoint(1<<20, autoCheckpointModeSize)

	stats := db.Stats()
	if stats == nil {
		t.Fatalf("Stats() returned nil")
	}
	n, err := strconv.ParseUint(stats["treedb.cache.auto_checkpoint.count"], 10, 64)
	if err != nil {
		t.Fatalf("parse auto checkpoint count: %v", err)
	}
	if n == 0 {
		t.Fatalf("expected size auto checkpoint to run")
	}
	if reason := stats["treedb.cache.auto_checkpoint.last_reason"]; reason != "size" {
		t.Fatalf("expected last reason size, got %q", reason)
	}

	ents, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("ReadDir(wal): %v", err)
	}
	walFiles := countCommitLogFiles(ents)
	if walFiles < len(db.lanes) || walFiles > len(db.lanes)+1 {
		t.Fatalf("expected %d..%d WAL segments after size checkpoint, got %d", len(db.lanes), len(db.lanes)+1, walFiles)
	}
	for _, ent := range ents {
		if ent.Name() == "commit-l0-000010.log" {
			t.Fatalf("expected seeded WAL segment to be trimmed, still present: %s", ent.Name())
		}
	}
}

func TestCachingDB_AutoCheckpoint_SizeTrigger_DoesNotThrashWithRetainedValueLog(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.testSkipVlogCheckpointKick = true

	db.StartAutoCheckpoint(0, 1<<20 /* 1MiB */, 0)

	// Seed a retained value-log segment by writing a large value. In value-log
	// mode this segment cannot be deleted by checkpoint, so reclaimable WAL bytes
	// remain below maxWALBytes indefinitely.
	val := bytes.Repeat([]byte("v"), 2<<20) // 2MiB
	if err := db.Set([]byte("seed"), val); err != nil {
		t.Fatalf("Set(seed): %v", err)
	}

	var initialCount uint64
	time.Sleep(withRaceTimeout(200 * time.Millisecond))
	stats := db.Stats()
	if stats == nil {
		t.Fatalf("Stats() returned nil")
	}
	n, err := strconv.ParseUint(stats["treedb.cache.auto_checkpoint.count"], 10, 64)
	if err != nil {
		t.Fatalf("parse auto checkpoint count: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected size auto checkpoint to remain idle with retained value-log (count=%d)", n)
	}
	initialCount = n

	// Continue writing while effectiveWALBytes remains above maxWALBytes. The
	// size-triggered checkpoint should remain disarmed and not repeatedly run.
	val = bytes.Repeat([]byte("x"), 512<<10) // 512KiB
	for i := 0; i < 8; i++ {
		k := []byte(fmt.Sprintf("k%03d", i))
		if err := db.Set(k, val); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	stats = db.Stats()
	if stats == nil {
		t.Fatalf("Stats() returned nil")
	}
	n, err = strconv.ParseUint(stats["treedb.cache.auto_checkpoint.count"], 10, 64)
	if err != nil {
		t.Fatalf("parse auto checkpoint count: %v", err)
	}
	if n != initialCount {
		t.Fatalf("expected size-triggered checkpoint to run once (count=%d), got %d", initialCount, n)
	}
}
