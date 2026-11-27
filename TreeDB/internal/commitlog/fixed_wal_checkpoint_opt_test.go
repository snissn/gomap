package commitlog

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestCommandJournalRotateSkipsRedundantSyncAndDoubleDirSync(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	dir := t.TempDir()
	j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{CaptureStableResources: true})
	if err != nil {
		t.Fatalf("openFixedTestCommandJournal: %v", err)
	}
	defer func() { _ = j.Close() }()

	frame := fixedWALLifecycleFrame(t, 4096)
	if _, err := j.AppendCommand(frame); err != nil {
		t.Fatalf("AppendCommand: %v", err)
	}
	captured, err := j.CaptureSync(nil)
	if err != nil {
		t.Fatalf("CaptureSync: %v", err)
	}
	if err := captured.Sync(); err != nil {
		captured.Release()
		t.Fatalf("CapturedSync.Sync: %v", err)
	}
	captured.Release()

	before := j.WriterDurabilityStats()
	if err := j.RotateActiveSegment(true); err != nil {
		t.Fatalf("RotateActiveSegment(true): %v", err)
	}
	afterRotate := j.WriterDurabilityStats()
	if afterRotate.FileSyncCalls != before.FileSyncCalls {
		t.Fatalf("RotateActiveSegment(true) redundantly synced already-synced segment: before=%d after=%d", before.FileSyncCalls, afterRotate.FileSyncCalls)
	}
	if got := afterRotate.DirectorySyncCalls - before.DirectorySyncCalls; got != 1 {
		t.Fatalf("RotateActiveSegment(true) directory syncs=%d, want 1", got)
	}

	// Sync on the freshly rotated empty fixed WAL segment must skip redundant fdatasync.
	if err := j.Sync(); err != nil {
		t.Fatalf("Sync on clean empty fixed segment: %v", err)
	}
	afterEmptySync := j.WriterDurabilityStats()
	if afterEmptySync.FileSyncCalls != afterRotate.FileSyncCalls {
		t.Fatalf("Sync on clean empty fixed segment redundantly synced: before=%d after=%d", afterRotate.FileSyncCalls, afterEmptySync.FileSyncCalls)
	}

	// Appending a frame must clear cleanEmptySynced and perform a real sync.
	if _, err := j.AppendCommand(frame); err != nil {
		t.Fatalf("second AppendCommand: %v", err)
	}
	if err := j.Sync(); err != nil {
		t.Fatalf("Sync after append: %v", err)
	}
	afterDataSync := j.WriterDurabilityStats()
	if afterDataSync.FileSyncCalls != afterEmptySync.FileSyncCalls+1 {
		t.Fatalf("Sync after append file syncs=%d want %d", afterDataSync.FileSyncCalls, afterEmptySync.FileSyncCalls+1)
	}
}

func TestCommandJournalPrepareFixedWALCheckpointReserveAdoptedAndDeferredDirSync(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	dir := t.TempDir()
	j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{CaptureStableResources: true})
	if err != nil {
		t.Fatalf("openFixedTestCommandJournal: %v", err)
	}
	defer func() { _ = j.Close() }()

	// Fill past half capacity so a pressure ahead-reserve starts (8 MiB), then
	// request a checkpoint reserve (4 MiB) which must cancel the pressure reserve
	// and prepare the 4 MiB forced-rotation successor.
	aheadFill(t, j)
	if err := j.PrepareFixedWALCheckpointReserve(); err != nil {
		t.Fatalf("PrepareFixedWALCheckpointReserve: %v", err)
	}
	aheadWait(t, j, func(s *fixedWALAheadSlot) bool {
		return s != nil && s.reason == fixedWALForcedRotation && s.ready
	})

	j.mu.Lock()
	preparedInfo, statErr := j.fixedAhead.reserve.file.Stat()
	j.mu.Unlock()
	if statErr != nil {
		t.Fatalf("stat prepared checkpoint reserve: %v", statErr)
	}

	if err := j.RotateActiveSegment(false); err != nil {
		t.Fatalf("RotateActiveSegment(false): %v", err)
	}
	j.mu.Lock()
	activeInfo, statErr := j.writer.f.Stat()
	activeCap := j.writer.fixed.capacity
	dirPending := j.writer.dirSyncPending
	j.mu.Unlock()
	if statErr != nil {
		t.Fatalf("stat rotated active file: %v", statErr)
	}
	if activeCap != fixedWALDefaultCapacity {
		t.Fatalf("rotated active capacity=%d want %d", activeCap, fixedWALDefaultCapacity)
	}
	if !os.SameFile(preparedInfo, activeInfo) {
		t.Fatal("forced checkpoint rotation did not adopt pre-warmed checkpoint reserve")
	}
	if !dirPending {
		t.Fatal("expected RotateActiveSegment(false) on fixed container to defer parent dir sync via dirSyncPending")
	}

	// CaptureSync must flush deferred dirSyncPending before certifying any durable frame.
	beforeDirSyncs := j.WriterDurabilityStats().DirectorySyncCalls
	if _, err := j.AppendCommand(fixedWALLifecycleFrame(t, 1024)); err != nil {
		t.Fatalf("AppendCommand on rotated segment: %v", err)
	}
	captured, err := j.CaptureSync(nil)
	if err != nil {
		t.Fatalf("CaptureSync: %v", err)
	}
	if err := captured.Sync(); err != nil {
		captured.Release()
		t.Fatalf("CapturedSync.Sync: %v", err)
	}
	captured.Release()

	afterDirSyncs := j.WriterDurabilityStats().DirectorySyncCalls
	if afterDirSyncs != beforeDirSyncs+1 {
		t.Fatalf("CaptureSync directory syncs=%d want %d", afterDirSyncs, beforeDirSyncs+1)
	}
	j.mu.Lock()
	stillPending := j.writer.dirSyncPending
	j.mu.Unlock()
	if stillPending {
		t.Fatal("expected dirSyncPending cleared after CaptureSync")
	}
}

func TestFixedWALLoadSinglePassCRCAndValidatedLSNScan(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	dir := t.TempDir()
	j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatalf("openFixedTestCommandJournal: %v", err)
	}
	// Mix small (<64 KiB) and large (>=64 KiB) frames to exercise both bufio
	// and direct ReadAt paths in loadFixedWAL.
	sizes := []int{512, 128 << 10, 2048, 256 << 10}
	wantLSNs := make([]uint64, 0, len(sizes))
	for _, sz := range sizes {
		lsn, err := j.AppendCommand(fixedWALLifecycleFrame(t, sz))
		if err != nil {
			t.Fatalf("AppendCommand(%d): %v", sz, err)
		}
		wantLSNs = append(wantLSNs, lsn)
	}
	if err := j.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	path := j.Path()
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewReader(path)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	var gotLSNs []uint64
	var lastOff int64
	for {
		lsn, err := r.ReadValidatedCommandFrameLSN()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadValidatedCommandFrameLSN: %v", err)
		}
		off, err := r.Offset()
		if err != nil {
			t.Fatalf("Offset: %v", err)
		}
		if off <= lastOff {
			t.Fatalf("Offset did not advance: prev=%d cur=%d", lastOff, off)
		}
		lastOff = off
		gotLSNs = append(gotLSNs, lsn)
	}
	logicalSize, ok := r.FixedLogicalSize()
	if !ok || lastOff != logicalSize {
		t.Fatalf("final offset=%d logicalSize=%d ok=%v", lastOff, logicalSize, ok)
	}
	_ = r.Close()
	if len(gotLSNs) != len(wantLSNs) {
		t.Fatalf("got %d LSNs (%v), want %d (%v)", len(gotLSNs), gotLSNs, len(wantLSNs), wantLSNs)
	}
	for i := range wantLSNs {
		if gotLSNs[i] != wantLSNs[i] {
			t.Fatalf("lsn[%d]=%d want %d", i, gotLSNs[i], wantLSNs[i])
		}
	}

	// MaxSegmentSize must still reject oversized frames via ReadValidatedCommandFrameLSN.
	smallReader, err := NewReaderWithOptions(path, Options{MaxSegmentSize: 16 << 10})
	if err != nil {
		t.Fatalf("NewReaderWithOptions(small): %v", err)
	}
	if _, err := smallReader.ReadValidatedCommandFrameLSN(); err != nil {
		t.Fatalf("first small frame failed: %v", err)
	}
	if _, err := smallReader.ReadValidatedCommandFrameLSN(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("oversized frame err=%v want ErrCorrupt", err)
	}
	_ = smallReader.Close()

	// Corrupting a byte inside a large extent payload must still fail loadFixedWAL.
	corruptPath := filepath.Join(dir, "corrupt.log")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	// Flip a byte inside the first extent payload (past 4096 header + 96 witness + 8 seg header).
	raw[fixedWALHeaderSize+fixedWALWitnessSize+segmentHeaderSize+16] ^= 0xff
	if err := os.WriteFile(corruptPath, raw, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := NewReader(corruptPath); err == nil {
		t.Fatal("expected NewReader to reject corrupted fixed WAL payload")
	}

	// Verify full ReadCommandFrame stream still decodes identically.
	fullReader, err := NewReader(path)
	if err != nil {
		t.Fatalf("NewReader(full): %v", err)
	}
	defer func() { _ = fullReader.Close() }()
	for i, wantLSN := range wantLSNs {
		env, err := fullReader.ReadCommandFrame()
		if err != nil {
			t.Fatalf("ReadCommandFrame[%d]: %v", i, err)
		}
		if env.LSN != wantLSN || len(env.Payload) == 0 || !bytes.Equal(env.Payload, fixedWALLifecycleFrame(t, sizes[i]).Payload) {
			t.Fatalf("ReadCommandFrame[%d] mismatch lsn=%d want %d", i, env.LSN, wantLSN)
		}
	}
}
