package commitlog

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestFixedWALAheadBoundedWaitAdoptsInFlightMatchingReserve(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	dir := t.TempDir()
	j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	oldWrite, oldSync := fixedWALInitializeWrite, fixedWALInitializeSync
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	var fallbackWrites atomic.Int64
	fixedWALInitializeWrite = func(f io.WriterAt, p []byte, off int64) error {
		if file, ok := f.(*os.File); ok && strings.HasPrefix(filepath.Base(file.Name()), ".wal-initialize-") {
			fallbackWrites.Add(1)
		}
		return oldWrite(f, p, off)
	}
	fixedWALInitializeSync = func(f *os.File) error {
		if strings.HasPrefix(filepath.Base(f.Name()), fixedWALAheadPrefix) {
			enteredOnce.Do(func() { close(entered) })
			<-release
		}
		return oldSync(f)
	}
	defer func() {
		unblock()
		fixedWALInitializeWrite, fixedWALInitializeSync = oldWrite, oldSync
	}()

	frame, count := aheadFill(t, j)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("ahead worker never entered sync")
	}
	j.mu.Lock()
	slot := j.fixedAhead
	if slot == nil || slot.ready {
		j.mu.Unlock()
		t.Fatal("expected in-flight unready ahead slot")
	}
	preparedInfo, err := slot.reserve.file.Stat()
	j.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	// Release the in-flight ahead sync shortly after rotation begins so the
	// bounded wait adopts the already-initialized reserve instead of cancelling
	// it and duplicating the full zero-fill + fsync synchronously.
	go func() {
		time.Sleep(15 * time.Millisecond)
		unblock()
	}()

	count += aheadRotatePressure(t, j, frame)
	fixedWALInitializeWrite, fixedWALInitializeSync = oldWrite, oldSync

	if got := fallbackWrites.Load(); got != 0 {
		t.Fatalf("rotation fell back to synchronous .wal-initialize- (%d writes); want 0", got)
	}
	j.mu.Lock()
	currentInfo, statErr := j.writer.f.Stat()
	slotAfterClaim := j.fixedAhead
	j.mu.Unlock()
	if statErr != nil || !os.SameFile(preparedInfo, currentInfo) {
		t.Fatalf("rotation did not adopt in-flight ahead inode: err=%v", statErr)
	}
	if slotAfterClaim == slot {
		t.Fatal("claimed ahead slot remained pinned on j.fixedAhead")
	}
	fixedWALDrainRotations(t, j)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openFixedTestCommandJournal(dir, CommandJournalOptions{CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.NextLSN() != uint64(count+1) {
		t.Fatalf("reopen next LSN=%d want=%d", reopened.NextLSN(), count+1)
	}
}

func TestFixedWALAheadCancelAbortsBeforeSync(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	dir := t.TempDir()
	j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatal(err)
	}

	oldWrite, oldSync := fixedWALInitializeWrite, fixedWALInitializeSync
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	var aheadSyncCalls atomic.Int64
	fixedWALInitializeWrite = func(f io.WriterAt, p []byte, off int64) error {
		if file, ok := f.(*os.File); ok && strings.HasPrefix(filepath.Base(file.Name()), fixedWALAheadPrefix) && off > 0 {
			enteredOnce.Do(func() {
				close(entered)
				<-release
			})
		}
		return oldWrite(f, p, off)
	}
	fixedWALInitializeSync = func(f *os.File) error {
		if strings.HasPrefix(filepath.Base(f.Name()), fixedWALAheadPrefix) {
			aheadSyncCalls.Add(1)
		}
		return oldSync(f)
	}
	defer func() {
		unblock()
		fixedWALInitializeWrite, fixedWALInitializeSync = oldWrite, oldSync
	}()

	aheadFill(t, j)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("ahead worker never entered zero-fill")
	}

	// Cancel via forced rotation while the ahead worker is still mid-zero-fill.
	if err := j.RotateActiveSegment(false); err != nil {
		t.Fatal(err)
	}
	unblock()
	aheadWait(t, j, func(s *fixedWALAheadSlot) bool { return s == nil || s.done })
	fixedWALInitializeWrite, fixedWALInitializeSync = oldWrite, oldSync

	if got := aheadSyncCalls.Load(); got != 0 {
		t.Fatalf("cancelled ahead worker still called fsync (%d times); want 0", got)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close after cancelled ahead worker: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if isFixedWALAheadName(entry.Name()) {
			t.Fatalf("cancelled ahead reserve survived cleanup: %s", entry.Name())
		}
	}
}

func TestFixedWALAheadDurableStableRotationSingleDirSync(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	dir := t.TempDir()
	j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	frame, count := aheadFill(t, j)
	aheadWait(t, j, func(s *fixedWALAheadSlot) bool { return s != nil && s.ready })

	var dirSyncs atomic.Int64
	var missingStableParent atomic.Int64
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.AfterNewFileDirectorySync {
			dirSyncs.Add(1)
			if e.StablePath == nil {
				missingStableParent.Add(1)
			}
		}
		return nil
	})
	var restoreOnce sync.Once
	restoreCut := func() { restoreOnce.Do(restore) }
	defer restoreCut()

	// Append durable frames through one real pressure rotation.
	frame.DurabilityClass = CommandDurabilityDurable
	seq := j.segmentSeq
	for i := 0; i < 32 && j.segmentSeq == seq; i++ {
		if _, err := j.AppendCommandObserved(frame); err != nil {
			t.Fatal(err)
		}
		count++
		if err := j.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	restoreCut()

	if j.segmentSeq == seq {
		t.Fatal("fixture did not rotate")
	}
	if got := dirSyncs.Load(); got != 1 {
		t.Fatalf("durable stable fixed rotation performed %d directory syncs; want 1", got)
	}
	if got := missingStableParent.Load(); got != 0 {
		t.Fatalf("directory sync event missing StablePath (%d times)", got)
	}
	fixedWALDrainRotations(t, j)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openFixedTestCommandJournal(dir, CommandJournalOptions{CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.NextLSN() != uint64(count+1) {
		t.Fatalf("reopen next LSN=%d want=%d", reopened.NextLSN(), count+1)
	}
}

func TestFixedWALAheadStartsOnBufferedUnflushedGroupWave(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	dir := t.TempDir()
	j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	// Append 256 KiB frames WITHOUT flushing between them (simulating a
	// concurrent group-commit wave) until buffered ActiveBytes crosses 50% of
	// the 4 MiB fixed container. Ahead-reserve must start before Flush/CaptureSync.
	frame := fixedWALLifecycleFrame(t, 256<<10)
	for i := 0; i < 9; i++ {
		if _, err := j.AppendCommand(frame); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	j.mu.Lock()
	started := j.fixedAhead != nil
	flushed := j.writer.fixed.physical
	j.mu.Unlock()
	if flushed != fixedWALHeaderSize {
		t.Fatalf("expected unflushed wave (physical=%d), got %d", fixedWALHeaderSize, flushed)
	}
	if !started {
		t.Fatal("expected fixed WAL ahead-reserve to start once buffered group-commit bytes crossed 50% capacity")
	}
	aheadWait(t, j, func(s *fixedWALAheadSlot) bool { return s != nil && s.ready })
}
