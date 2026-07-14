//go:build darwin || linux || freebsd || netbsd || openbsd

package commitlog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOrdinaryRotationRefreshesStableParentBeforeStableRotation(t *testing.T) {
	root := t.TempDir()
	walDir := filepath.Join(root, "wal")
	journal, err := OpenCommandJournal(walDir, CommandJournalOptions{Lane: 6})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = journal.Close()
		}
	})
	firstParent := journal.stableParent
	if _, err := journal.AppendCommand(CommandEnvelope{
		Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatal(err)
	}

	originalDir := filepath.Join(root, "wal-original")
	if err := os.Rename(walDir, originalDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(walDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := journal.RotateActiveSegment(false); err != nil {
		t.Fatal(err)
	}
	secondParent := journal.stableParent
	if _, err := journal.AppendCommand(CommandEnvelope{
		Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatal(err)
	}

	movedReplacementDir := filepath.Join(root, "wal-replacement-moved")
	if err := os.Rename(walDir, movedReplacementDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(walDir, 0o700); err != nil {
		t.Fatal(err)
	}
	rotation, err := journal.RotateActiveSegmentWithStableResources(false)
	if err != nil {
		t.Fatal(err)
	}
	rotation.Release()
	thirdName := CommandSegmentName(6, 3)
	if _, err := os.Stat(filepath.Join(movedReplacementDir, thirdName)); err != nil {
		t.Fatalf("stable successor missing from exact ordinary-rotation parent: %v", err)
	}
	for _, wrong := range []string{
		filepath.Join(originalDir, thirdName),
		filepath.Join(walDir, thirdName),
	} {
		if _, err := os.Stat(wrong); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stable successor escaped to %q: %v", wrong, err)
		}
	}
	if firstParent == nil || secondParent == nil || firstParent == secondParent {
		t.Fatalf("ordinary rotation did not replace retained parent: first=%p second=%p", firstParent, secondParent)
	}
	if _, err := firstParent.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("construction parent remains retained after ordinary rotation: %v", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	if _, err := secondParent.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("ordinary-rotation parent remains retained after journal close: %v", err)
	}
}

func TestOrdinaryRotationCaptureFailureInvalidatesStableParent(t *testing.T) {
	dir := t.TempDir()
	journal, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 7})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	oldParent := journal.stableParent
	failedParent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected ordinary-rotation parent capture failure")
	originalOpenParent := openStableCommandWALParent
	openStableCommandWALParent = func(string) (*os.File, error) { return failedParent, injected }
	defer func() { openStableCommandWALParent = originalOpenParent }()

	if err := journal.RotateActiveSegment(false); err != nil {
		t.Fatalf("ordinary rotation must remain usable after stable capture failure: %v", err)
	}
	if journal.stableParent != nil || !errors.Is(journal.stableParentErr, injected) {
		t.Fatalf("stable parent=%v err=%v want explicit invalidation", journal.stableParent, journal.stableParentErr)
	}
	if _, err := oldParent.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("superseded parent remains retained after capture failure: %v", err)
	}
	if _, err := failedParent.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("failed replacement parent was not released: %v", err)
	}
	if _, err := journal.AppendCommand(CommandEnvelope{
		Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatal(err)
	}
	rotation, err := journal.RotateActiveSegmentWithStableResources(false)
	if !errors.Is(err, injected) || rotation != nil {
		if rotation != nil {
			rotation.Release()
		}
		t.Fatalf("stable rotation=(%v, %v) want nil and retained capture error", rotation, err)
	}
	if _, err := os.Stat(filepath.Join(dir, CommandSegmentName(7, 3))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed-closed stable rotation exposed successor: %v", err)
	}
}

func TestOrdinaryRotationRetainedParentPlateau(t *testing.T) {
	dir := t.TempDir()
	journal, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	for seq := uint64(2); seq <= 321; seq++ {
		oldParent := journal.stableParent
		if err := journal.RotateActiveSegment(false); err != nil {
			t.Fatalf("rotation %d: %v", seq, err)
		}
		if journal.stableParent == nil || journal.stableParent == oldParent {
			t.Fatalf("rotation %d did not transfer retained-parent ownership", seq)
		}
		if _, err := oldParent.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("rotation %d retained superseded parent: %v", seq, err)
		}
	}
}

func TestOrdinaryRotationCloseErrorRefreshesInstalledSuccessorAuthority(t *testing.T) {
	root := t.TempDir()
	walDir := filepath.Join(root, "wal")
	var rotatedBytes []int64
	journal, err := OpenCommandJournal(walDir, CommandJournalOptions{
		Lane: 9,
		OnSegmentRotated: func(closedBytes int64) {
			rotatedBytes = append(rotatedBytes, closedBytes)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = journal.Close()
		}
	})
	lsn, err := journal.AppendCommand(CommandEnvelope{
		Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatal(err)
	}
	oldParent := journal.stableParent
	oldFile := journal.writer.f
	oldPath := journal.path
	oldSeq := journal.segmentSeq
	oldMaxLSN := journal.activeSegmentMaxLSN
	closedBytes := journal.writer.ActiveBytes()

	originalDir := filepath.Join(root, "wal-original")
	if err := os.Rename(walDir, originalDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(walDir, 0o700); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected rotated command WAL close error")
	hookInstalled := true
	closeCalls := 0
	journal.writer.closeRotateFn = func(file *os.File) error {
		closeCalls++
		return errors.Join(file.Close(), injected)
	}
	t.Cleanup(func() {
		if hookInstalled {
			journal.writer.closeRotateFn = nil
		}
	})

	err = journal.RotateActiveSegment(false)
	journal.writer.closeRotateFn = nil
	hookInstalled = false
	if !errors.Is(err, injected) {
		t.Fatalf("ordinary rotation error=%v, want injected close error", err)
	}
	if closeCalls != 1 {
		t.Fatalf("old-file close calls=%d, want 1", closeCalls)
	}
	if journal.writer == nil || journal.writer.f == nil || journal.writer.f == oldFile {
		t.Fatalf("writer did not install successor after close error: writer=%p file=%p old=%p", journal.writer, journal.writer.f, oldFile)
	}
	if _, err := oldFile.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("injected close hook leaked old file: %v", err)
	}
	secondName := CommandSegmentName(9, 2)
	if got := journal.writer.f.Name(); got != filepath.Join(walDir, secondName) {
		t.Fatalf("installed writer path=%q, want %q", got, filepath.Join(walDir, secondName))
	}
	if journal.path != filepath.Join(walDir, secondName) || journal.segmentSeq != 2 || journal.activeSegmentMaxLSN != 0 {
		t.Errorf("installed successor metadata path=%q seq=%d maxLSN=%d, want path=%q seq=2 maxLSN=0 (old path=%q seq=%d maxLSN=%d lsn=%d)",
			journal.path, journal.segmentSeq, journal.activeSegmentMaxLSN, filepath.Join(walDir, secondName), oldPath, oldSeq, oldMaxLSN, lsn)
	}
	if len(rotatedBytes) != 1 || rotatedBytes[0] != closedBytes {
		t.Errorf("rotation callback bytes=%v, want [%d] exactly once", rotatedBytes, closedBytes)
	}
	if journal.stableParent == nil || journal.stableParent == oldParent {
		t.Errorf("installed successor retained stale parent: old=%p current=%p", oldParent, journal.stableParent)
	}

	movedReplacementDir := filepath.Join(root, "wal-replacement-moved")
	if err := os.Rename(walDir, movedReplacementDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(walDir, 0o700); err != nil {
		t.Fatal(err)
	}
	rotation, err := journal.RotateActiveSegmentWithStableResources(false)
	if err != nil {
		t.Fatal(err)
	}
	rotation.Release()
	thirdName := CommandSegmentName(9, 3)
	if _, err := os.Stat(filepath.Join(movedReplacementDir, thirdName)); err != nil {
		t.Errorf("stable successor missing from installed successor parent: %v", err)
	}
	for _, wrong := range []string{
		filepath.Join(originalDir, secondName),
		filepath.Join(originalDir, thirdName),
		filepath.Join(walDir, secondName),
		filepath.Join(walDir, thirdName),
	} {
		if _, err := os.Stat(wrong); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("stable successor escaped to %q: %v", wrong, err)
		}
	}
	if journal.segmentSeq != 3 || journal.path != filepath.Join(walDir, thirdName) {
		t.Errorf("stable rotation metadata path=%q seq=%d, want path=%q seq=3", journal.path, journal.segmentSeq, filepath.Join(walDir, thirdName))
	}
	if _, err := oldParent.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Errorf("superseded construction parent remains retained: %v", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
}

func TestOrdinaryRotationPreInstallFailureKeepsJournalAuthority(t *testing.T) {
	root := t.TempDir()
	walDir := filepath.Join(root, "wal")
	callbackCalls := 0
	journal, err := OpenCommandJournal(walDir, CommandJournalOptions{
		Lane: 10,
		OnSegmentRotated: func(int64) {
			callbackCalls++
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	lsn, err := journal.AppendCommand(CommandEnvelope{
		Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatal(err)
	}
	oldFile := journal.writer.f
	oldParent := journal.stableParent
	oldPath := journal.path
	oldSeq := journal.segmentSeq
	oldMaxLSN := journal.activeSegmentMaxLSN

	if err := os.Rename(walDir, filepath.Join(root, "wal-moved")); err != nil {
		t.Fatal(err)
	}
	if err := journal.RotateActiveSegment(false); err == nil {
		t.Fatal("ordinary rotation unexpectedly installed a successor without a parent directory")
	}
	if journal.writer.f != oldFile || journal.stableParent != oldParent || journal.path != oldPath || journal.segmentSeq != oldSeq || journal.activeSegmentMaxLSN != oldMaxLSN {
		t.Fatalf("pre-install failure changed authority: file=%p/%p parent=%p/%p path=%q/%q seq=%d/%d maxLSN=%d/%d",
			journal.writer.f, oldFile, journal.stableParent, oldParent, journal.path, oldPath,
			journal.segmentSeq, oldSeq, journal.activeSegmentMaxLSN, oldMaxLSN)
	}
	if callbackCalls != 0 {
		t.Fatalf("pre-install failure invoked rotation callback %d times", callbackCalls)
	}
	nextLSN, err := journal.AppendCommand(CommandEnvelope{
		Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("append after pre-install failure: %v", err)
	}
	if nextLSN != lsn+1 || journal.activeSegmentMaxLSN != nextLSN {
		t.Fatalf("append after pre-install failure LSN=%d maxLSN=%d, want %d", nextLSN, journal.activeSegmentMaxLSN, lsn+1)
	}
}

func TestOrdinaryRotationCloseErrorResourcePlateau(t *testing.T) {
	dir := t.TempDir()
	callbackCalls := 0
	var callbackBytes int64
	journal, err := OpenCommandJournal(dir, CommandJournalOptions{
		Lane: 11,
		OnSegmentRotated: func(closedBytes int64) {
			callbackCalls++
			callbackBytes += closedBytes
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = journal.Close()
		}
	})
	injected := errors.New("injected repeated command WAL close error")
	hookInstalled := true
	closeCalls := 0
	journal.writer.closeRotateFn = func(file *os.File) error {
		closeCalls++
		return errors.Join(file.Close(), injected)
	}
	t.Cleanup(func() {
		if hookInstalled {
			journal.writer.closeRotateFn = nil
		}
	})

	var wantCallbackBytes int64
	for seq := uint64(2); seq <= 321; seq++ {
		lsn, err := journal.AppendCommand(CommandEnvelope{
			Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
		})
		if err != nil {
			t.Fatalf("append before rotation %d: %v", seq, err)
		}
		oldFile := journal.writer.f
		oldParent := journal.stableParent
		closedBytes := journal.writer.ActiveBytes()
		wantCallbackBytes += closedBytes
		if err := journal.RotateActiveSegment(false); !errors.Is(err, injected) {
			t.Fatalf("rotation %d error=%v, want injected close error", seq, err)
		}
		if journal.segmentSeq != seq || journal.path != filepath.Join(dir, CommandSegmentName(11, seq)) || journal.activeSegmentMaxLSN != 0 {
			t.Fatalf("rotation %d metadata path=%q seq=%d maxLSN=%d", seq, journal.path, journal.segmentSeq, journal.activeSegmentMaxLSN)
		}
		if lsn == 0 || journal.writer.f == oldFile || journal.stableParent == oldParent {
			t.Fatalf("rotation %d did not transfer writer/parent ownership", seq)
		}
		if _, err := oldFile.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("rotation %d retained old file: %v", seq, err)
		}
		if _, err := oldParent.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("rotation %d retained old parent: %v", seq, err)
		}
	}
	journal.writer.closeRotateFn = nil
	hookInstalled = false
	if closeCalls != 320 || callbackCalls != 320 || callbackBytes != wantCallbackBytes {
		t.Fatalf("close calls=%d callback calls=%d bytes=%d, want 320/320/%d", closeCalls, callbackCalls, callbackBytes, wantCallbackBytes)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
}

func TestCommandJournalStableRotationCreatesThroughCapturedParent(t *testing.T) {
	testCommandJournalStableRotationCreatesThroughCapturedParent(t)
}

func TestCommandJournalStableRotationUsesParentRefreshedByOrdinaryRotation(t *testing.T) {
	testCommandJournalStableRotationUsesParentRefreshedByOrdinaryRotation(t)
}

func TestCommandJournalStableRotationDoesNotLoseClosedSegment(t *testing.T) {
	testCommandJournalStableRotationDoesNotLoseClosedSegment(t)
}

func TestCommandJournalStableRotationRejectsEachMissingSegment(t *testing.T) {
	testCommandJournalStableRotationRejectsEachMissingSegment(t)
}

func TestCommandJournalStableRotationFrontierUsesSegmentAppendsNotOwnerCursor(t *testing.T) {
	testCommandJournalStableRotationFrontierUsesSegmentAppendsNotOwnerCursor(t)
}

func TestCommandJournalAutomaticRotationCapturesStableResources(t *testing.T) {
	testCommandJournalAutomaticRotationCapturesStableResources(t)
}

func TestCommandJournalStableRotationNamespaceFailureKeepsOldWriterActive(t *testing.T) {
	testCommandJournalStableRotationNamespaceFailureKeepsOldWriterActive(t)
}

func BenchmarkStableCommandWALRotation(b *testing.B) {
	benchmarkStableCommandWALRotation(b)
}
