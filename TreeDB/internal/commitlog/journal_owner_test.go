package commitlog

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

var errCommandJournalInjectedWrite = errors.New("injected command journal write failure")

type commandJournalFailWriter struct{}

func (commandJournalFailWriter) Write([]byte) (int, error) {
	return 0, errCommandJournalInjectedWrite
}

func (j *CommandJournal) installBufferedWriterForTest(w io.Writer, size int) func() {
	j.mu.Lock()
	defer j.mu.Unlock()
	old := j.writer.bw
	j.writer.bw = bufio.NewWriterSize(w, size)
	return func() {
		j.mu.Lock()
		defer j.mu.Unlock()
		j.writer.bw = old
	}
}

func TestCommandJournalAllocatesContiguousLSNs(t *testing.T) {
	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{InitialLSN: 40})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer j.Close()

	for i, want := range []uint64{41, 42, 43} {
		got, err := j.AppendCommand(CommandEnvelope{
			Kind:          CommandKindRawKVBatch,
			Scope:         CommandScopeRawKV,
			PayloadFormat: PayloadFormatRawKVBatchV1,
		})
		if err != nil {
			t.Fatalf("AppendCommand(%d): %v", i, err)
		}
		if got != want {
			t.Fatalf("AppendCommand(%d) LSN=%d, want %d", i, got, want)
		}
	}
	if err := j.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	frames, err := ScanCommandFrames(j.Path(), Options{})
	if err != nil {
		t.Fatalf("ScanCommandFrames: %v", err)
	}
	if len(frames) != 3 {
		t.Fatalf("len(frames)=%d, want 3", len(frames))
	}
	for i, frame := range frames {
		if want := uint64(41 + i); frame.LSN != want {
			t.Fatalf("frame[%d].LSN=%d, want %d", i, frame.LSN, want)
		}
	}
}

func TestCommandJournalSeedsLSNFromExistingFrames(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal first: %v", err)
	}
	lsn, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("AppendCommand first: %v", err)
	}
	if lsn != 1 {
		t.Fatalf("first LSN=%d, want 1", lsn)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}

	reopen, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal reopen: %v", err)
	}
	defer reopen.Close()
	lsn, err = reopen.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("AppendCommand after reopen: %v", err)
	}
	if lsn != 2 {
		t.Fatalf("reopened LSN=%d, want 2", lsn)
	}
}

func TestCommandJournalSeedsLSNFromExistingSegmentFamily(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentSeq: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournal first: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := j.AppendCommand(CommandEnvelope{
			Kind:          CommandKindRawKVBatch,
			Scope:         CommandScopeRawKV,
			PayloadFormat: PayloadFormatRawKVBatchV1,
		}); err != nil {
			t.Fatalf("AppendCommand first segment %d: %v", i, err)
		}
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}

	reopen, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentSeq: 2})
	if err != nil {
		t.Fatalf("OpenCommandJournal second segment: %v", err)
	}
	defer reopen.Close()
	lsn, err := reopen.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("AppendCommand second segment: %v", err)
	}
	if lsn != 3 {
		t.Fatalf("second segment LSN=%d, want 3", lsn)
	}
}

func TestCommandJournalDefaultSegmentSeqUsesLatestSegment(t *testing.T) {
	dir := t.TempDir()
	first, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentSeq: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournal first: %v", err)
	}
	if _, err := first.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatalf("AppendCommand first segment: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}

	second, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentSeq: 2})
	if err != nil {
		t.Fatalf("OpenCommandJournal second: %v", err)
	}
	if _, err := second.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatalf("AppendCommand second segment: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("Close second: %v", err)
	}

	reopen, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal default latest: %v", err)
	}
	lsn, err := reopen.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("AppendCommand default latest: %v", err)
	}
	if lsn != 3 {
		t.Fatalf("default latest LSN=%d, want 3", lsn)
	}
	if err := reopen.Close(); err != nil {
		t.Fatalf("Close reopen: %v", err)
	}
	seg1, err := ScanCommandFrames(filepath.Join(dir, CommandSegmentName(0, 1)), Options{})
	if err != nil {
		t.Fatalf("ScanCommandFrames segment 1: %v", err)
	}
	if len(seg1) != 1 || seg1[0].LSN != 1 {
		t.Fatalf("segment 1 frames=%+v, want only LSN 1", seg1)
	}
	seg2, err := ScanCommandFrames(filepath.Join(dir, CommandSegmentName(0, 2)), Options{})
	if err != nil {
		t.Fatalf("ScanCommandFrames segment 2: %v", err)
	}
	if len(seg2) != 2 || seg2[0].LSN != 2 || seg2[1].LSN != 3 {
		t.Fatalf("segment 2 frames=%+v, want LSNs 2,3", seg2)
	}
}

func TestCommandJournalRejectsExplicitSegmentBehindLaneTail(t *testing.T) {
	dir := t.TempDir()
	first, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentSeq: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournal first: %v", err)
	}
	if _, err := first.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatalf("AppendCommand first: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}

	second, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentSeq: 2})
	if err != nil {
		t.Fatalf("OpenCommandJournal second: %v", err)
	}
	if _, err := second.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatalf("AppendCommand second: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("Close second: %v", err)
	}

	_, err = OpenCommandJournal(dir, CommandJournalOptions{SegmentSeq: 1})
	if !errors.Is(err, ErrCommandWALStaleSegment) {
		t.Fatalf("OpenCommandJournal stale segment error=%v, want ErrCommandWALStaleSegment", err)
	}
}

func TestCommandJournalSeedsLSNFromExistingLanes(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 0, SegmentSeq: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournal first lane: %v", err)
	}
	if _, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatalf("AppendCommand first lane: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close first lane: %v", err)
	}

	reopen, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 1, SegmentSeq: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournal second lane: %v", err)
	}
	defer reopen.Close()
	lsn, err := reopen.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("AppendCommand second lane: %v", err)
	}
	if lsn != 2 {
		t.Fatalf("second lane LSN=%d, want 2", lsn)
	}
}

func TestCommandJournalRejectsDuplicateLSNAcrossLanes(t *testing.T) {
	dir := t.TempDir()
	for _, lane := range []int{0, 1} {
		w, err := NewWriter(filepath.Join(dir, CommandSegmentName(lane, 1)))
		if err != nil {
			t.Fatalf("NewWriter lane %d: %v", lane, err)
		}
		if err := w.AppendCommand(CommandEnvelope{
			LSN:           1,
			Kind:          CommandKindRawKVBatch,
			Scope:         CommandScopeRawKV,
			PayloadFormat: PayloadFormatRawKVBatchV1,
		}); err != nil {
			_ = w.Close()
			t.Fatalf("AppendCommand lane %d: %v", lane, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close lane %d: %v", lane, err)
		}
	}
	_, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if !errors.Is(err, ErrCommandWALDuplicateLSN) {
		t.Fatalf("OpenCommandJournal duplicate LSN error=%v, want ErrCommandWALDuplicateLSN", err)
	}
}

func TestCommandJournalInitialLSNIgnoresLegacyRawSegments(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, CommandSegmentName(0, 1))
	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter legacy raw: %v", err)
	}
	if err := w.AppendBatch([]Record{{Op: OpSetInline, Key: []byte("legacy"), Value: []byte("raw"), Seq: 99}}); err != nil {
		_ = w.Close()
		t.Fatalf("AppendBatch legacy raw: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close legacy raw: %v", err)
	}

	j, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentSeq: 2})
	if err != nil {
		t.Fatalf("OpenCommandJournal with legacy raw segment: %v", err)
	}
	defer j.Close()
	lsn, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("AppendCommand after legacy raw segment: %v", err)
	}
	if lsn != 1 {
		t.Fatalf("LSN=%d, want 1 when only legacy raw segments exist", lsn)
	}
}

func TestCommandJournalRejectsActiveLegacyRawSegment(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, CommandSegmentName(0, 1))
	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter legacy raw: %v", err)
	}
	if err := w.AppendBatch([]Record{{Op: OpSetInline, Key: []byte("legacy"), Value: []byte("raw"), Seq: 99}}); err != nil {
		_ = w.Close()
		t.Fatalf("AppendBatch legacy raw: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close legacy raw: %v", err)
	}

	_, err = OpenCommandJournal(dir, CommandJournalOptions{})
	if !errors.Is(err, ErrCommandWALLegacyPayload) {
		t.Fatalf("OpenCommandJournal active legacy error=%v, want ErrCommandWALLegacyPayload", err)
	}
}

func TestCommandJournalRejectsNonActiveTerminalTail(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentSeq: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournal first: %v", err)
	}
	if _, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatalf("AppendCommand first: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}
	path := filepath.Join(dir, CommandSegmentName(0, 1))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile append tail: %v", err)
	}
	if _, err := f.Write([]byte{0x01, 0x02, 0x03}); err != nil {
		_ = f.Close()
		t.Fatalf("Write torn tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close torn tail: %v", err)
	}

	_, err = OpenCommandJournal(dir, CommandJournalOptions{SegmentSeq: 2})
	if err == nil {
		t.Fatalf("OpenCommandJournal with non-active tail unexpectedly succeeded")
	}
}

func TestCommandJournalTruncatesActiveTerminalTailPerLane(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 0, SegmentSeq: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournal lane 0: %v", err)
	}
	if _, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatalf("AppendCommand lane 0: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close lane 0: %v", err)
	}
	lane0Path := filepath.Join(dir, CommandSegmentName(0, 1))
	f, err := os.OpenFile(lane0Path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile append lane 0 tail: %v", err)
	}
	if _, err := f.Write([]byte{0x01, 0x02, 0x03}); err != nil {
		_ = f.Close()
		t.Fatalf("Write lane 0 torn tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close lane 0 torn tail: %v", err)
	}

	reopen, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 1, SegmentSeq: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournal lane 1 with lane 0 active tail: %v", err)
	}
	defer reopen.Close()
	lsn, err := reopen.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("AppendCommand lane 1: %v", err)
	}
	if lsn != 2 {
		t.Fatalf("lane 1 LSN=%d, want 2 after lane 0 scan", lsn)
	}
	frames, err := ScanCommandFrames(lane0Path, Options{})
	if err != nil {
		t.Fatalf("ScanCommandFrames lane 0: %v", err)
	}
	if len(frames) != 1 || frames[0].LSN != 1 {
		t.Fatalf("lane 0 frames=%+v, want only LSN 1 after active-tail truncation", frames)
	}
}

func TestCommandJournalTruncatesTerminalTailBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal first: %v", err)
	}
	if _, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatalf("AppendCommand first: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}
	path := filepath.Join(dir, CommandSegmentName(0, 1))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile append tail: %v", err)
	}
	if _, err := f.Write([]byte{0x01, 0x02, 0x03}); err != nil {
		_ = f.Close()
		t.Fatalf("Write torn tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close torn tail: %v", err)
	}

	reopen, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal reopen: %v", err)
	}
	defer reopen.Close()
	lsn, err := reopen.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("AppendCommand after terminal tail: %v", err)
	}
	if lsn != 2 {
		t.Fatalf("LSN after terminal tail=%d, want 2", lsn)
	}
	if err := reopen.Flush(); err != nil {
		t.Fatalf("Flush reopen: %v", err)
	}
	frames, err := ScanCommandFrames(path, Options{})
	if err != nil {
		t.Fatalf("ScanCommandFrames: %v", err)
	}
	if len(frames) != 2 {
		t.Fatalf("len(frames)=%d, want 2", len(frames))
	}
}

func TestCommandJournalConcurrentAppendsSerializeFrameOrder(t *testing.T) {
	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer j.Close()

	const count = 64
	start := make(chan struct{})
	errs := make(chan error, count)
	for i := 0; i < count; i++ {
		go func() {
			<-start
			_, err := j.AppendCommand(CommandEnvelope{
				Kind:          CommandKindRawKVBatch,
				Scope:         CommandScopeRawKV,
				PayloadFormat: PayloadFormatRawKVBatchV1,
			})
			errs <- err
		}()
	}
	close(start)
	for i := 0; i < count; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("AppendCommand concurrent error: %v", err)
		}
	}
	if err := j.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	frames, err := ScanCommandFrames(j.Path(), Options{})
	if err != nil {
		t.Fatalf("ScanCommandFrames: %v", err)
	}
	if len(frames) != count {
		t.Fatalf("len(frames)=%d, want %d", len(frames), count)
	}
	for i, frame := range frames {
		if want := uint64(i + 1); frame.LSN != want {
			t.Fatalf("frame[%d].LSN=%d, want %d", i, frame.LSN, want)
		}
	}
}

func TestCommandJournalRejectsIndependentMutableOwner(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal first: %v", err)
	}
	defer j.Close()

	_, err = OpenCommandJournal(dir, CommandJournalOptions{})
	if !errors.Is(err, ErrJournalOwnerExists) {
		t.Fatalf("OpenCommandJournal second error=%v, want ErrJournalOwnerExists", err)
	}
}

func TestJournalOwnerRollbackMaxLSNClearsExhausted(t *testing.T) {
	owner, err := AcquireJournalOwnerWithOptions(t.TempDir(), JournalOwnerOptions{InitialLSN: ^uint64(0) - 1})
	if err != nil {
		t.Fatalf("AcquireJournalOwnerWithOptions: %v", err)
	}
	defer owner.Close()

	lsn, err := owner.ReserveLSN()
	if err != nil {
		t.Fatalf("ReserveLSN max: %v", err)
	}
	if lsn != ^uint64(0) {
		t.Fatalf("LSN=%d, want max uint64", lsn)
	}
	if _, err := owner.ReserveLSN(); err == nil {
		t.Fatalf("ReserveLSN after exhaustion unexpectedly succeeded")
	}
	if err := owner.rollbackReservedLSN(lsn); err != nil {
		t.Fatalf("rollbackReservedLSN max: %v", err)
	}
	lsn, err = owner.ReserveLSN()
	if err != nil {
		t.Fatalf("ReserveLSN after rollback: %v", err)
	}
	if lsn != ^uint64(0) {
		t.Fatalf("LSN after rollback=%d, want max uint64", lsn)
	}
}

func TestCommandJournalUsesCommitSegmentFamily(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 3, SegmentSeq: 9})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer j.Close()

	if got, want := j.Path(), filepath.Join(dir, "commit-l3-000009.log"); got != want {
		t.Fatalf("Path=%q, want %q", got, want)
	}
}

func TestCommandJournalRejectsCallerAssignedLSN(t *testing.T) {
	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer j.Close()

	_, err = j.AppendCommand(CommandEnvelope{
		LSN:           99,
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err == nil {
		t.Fatalf("AppendCommand with caller-assigned LSN unexpectedly succeeded")
	}
}

func TestCommandJournalValidationFailureDoesNotConsumeLSN(t *testing.T) {
	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer j.Close()

	if _, err := j.AppendCommand(CommandEnvelope{}); err == nil {
		t.Fatalf("invalid AppendCommand unexpectedly succeeded")
	}
	got, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("valid AppendCommand after validation failure: %v", err)
	}
	if got != 1 {
		t.Fatalf("LSN after validation failure=%d, want 1", got)
	}
}

func TestCommandJournalUnsupportedVersionDoesNotConsumeLSN(t *testing.T) {
	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer j.Close()

	_, err = j.AppendCommand(CommandEnvelope{
		Version:       CommandFrameVersion + 1,
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if !errors.Is(err, ErrCommandWALUnsupportedVersion) {
		t.Fatalf("unsupported version AppendCommand error=%v, want ErrCommandWALUnsupportedVersion", err)
	}
	got, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("valid AppendCommand after unsupported version: %v", err)
	}
	if got != 1 {
		t.Fatalf("LSN after unsupported version=%d, want 1", got)
	}
}

func TestCommandJournalAppendFailureRollsBackLSN(t *testing.T) {
	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer j.Close()

	restoreWriter := j.installBufferedWriterForTest(commandJournalFailWriter{}, 1)
	_, err = j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if !errors.Is(err, errCommandJournalInjectedWrite) {
		t.Fatalf("failed AppendCommand error=%v, want injected write failure", err)
	}

	restoreWriter()
	got, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("valid AppendCommand after write failure: %v", err)
	}
	if got != 1 {
		t.Fatalf("LSN after write failure=%d, want 1", got)
	}
}

func TestCommandJournalOversizedFrameDoesNotConsumeLSN(t *testing.T) {
	emptyPayload, err := EncodeRawKVBatchPayload(nil)
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload empty: %v", err)
	}
	emptyFrameSize, err := commandFrameEncodedSize(CommandEnvelope{
		LSN:           1,
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
		Payload:       emptyPayload,
	})
	if err != nil {
		t.Fatalf("commandFrameEncodedSize empty: %v", err)
	}
	oversizedPayload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("k"), Value: bytes.Repeat([]byte("v"), 32)},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload oversized: %v", err)
	}

	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{MaxSegmentSize: int64(emptyFrameSize)})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer j.Close()

	_, err = j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
		Payload:       oversizedPayload,
	})
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("oversized AppendCommand error=%v, want ErrRecordTooLarge", err)
	}
	got, err := j.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("valid AppendCommand after oversized frame: %v", err)
	}
	if got != 1 {
		t.Fatalf("LSN after oversized frame=%d, want 1", got)
	}
}

func TestCommandJournalDeterministicStressReopenAcrossLanesAndTails(t *testing.T) {
	dir := t.TempDir()
	var wantNext uint64 = 1

	for step := 0; step < 9; step++ {
		lane := step % 3
		seg := uint64(step/3 + 1)
		appendCount := 1 + (step*7)%5
		j, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: lane, SegmentSeq: seg})
		if err != nil {
			t.Fatalf("step %d OpenCommandJournal lane=%d seg=%d: %v", step, lane, seg, err)
		}
		for i := 0; i < appendCount; i++ {
			got, err := j.AppendCommand(CommandEnvelope{
				Kind:          CommandKindRawKVBatch,
				Scope:         CommandScopeRawKV,
				PayloadFormat: PayloadFormatRawKVBatchV1,
			})
			if err != nil {
				_ = j.Close()
				t.Fatalf("step %d AppendCommand %d: %v", step, i, err)
			}
			if got != wantNext {
				_ = j.Close()
				t.Fatalf("step %d AppendCommand %d LSN=%d, want %d", step, i, got, wantNext)
			}
			wantNext++
		}
		if err := j.Close(); err != nil {
			t.Fatalf("step %d Close: %v", step, err)
		}

		if step%2 == 0 {
			path := filepath.Join(dir, CommandSegmentName(lane, seg))
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				t.Fatalf("step %d OpenFile tail: %v", step, err)
			}
			if _, err := f.Write([]byte{0xde, 0xad, 0xbe}); err != nil {
				_ = f.Close()
				t.Fatalf("step %d Write tail: %v", step, err)
			}
			if err := f.Close(); err != nil {
				t.Fatalf("step %d Close tail: %v", step, err)
			}

			reopen, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: lane, SegmentSeq: seg})
			if err != nil {
				t.Fatalf("step %d reopen after active tail: %v", step, err)
			}
			got, err := reopen.AppendCommand(CommandEnvelope{
				Kind:          CommandKindRawKVBatch,
				Scope:         CommandScopeRawKV,
				PayloadFormat: PayloadFormatRawKVBatchV1,
			})
			if err != nil {
				_ = reopen.Close()
				t.Fatalf("step %d AppendCommand after active tail: %v", step, err)
			}
			if got != wantNext {
				_ = reopen.Close()
				t.Fatalf("step %d LSN after active tail=%d, want %d", step, got, wantNext)
			}
			wantNext++
			if err := reopen.Close(); err != nil {
				t.Fatalf("step %d Close reopen: %v", step, err)
			}
		}
	}

	segments, err := commandJournalSegments(dir, "")
	if err != nil {
		t.Fatalf("commandJournalSegments: %v", err)
	}
	var gotLSNs []uint64
	for _, seg := range segments {
		frames, err := ScanCommandFrames(seg.path, Options{})
		if err != nil {
			t.Fatalf("ScanCommandFrames %s: %v", filepath.Base(seg.path), err)
		}
		for _, frame := range frames {
			gotLSNs = append(gotLSNs, frame.LSN)
		}
	}
	sort.Slice(gotLSNs, func(i, j int) bool { return gotLSNs[i] < gotLSNs[j] })
	if len(gotLSNs) != int(wantNext-1) {
		t.Fatalf("frame count=%d, want %d; LSNs=%v", len(gotLSNs), wantNext-1, gotLSNs)
	}
	for i, got := range gotLSNs {
		if want := uint64(i + 1); got != want {
			t.Fatalf("global LSN[%d]=%d, want %d; LSNs=%v", i, got, want, gotLSNs)
		}
	}
}
