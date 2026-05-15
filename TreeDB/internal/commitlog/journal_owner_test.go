package commitlog

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"testing"
)

var errCommandJournalInjectedWrite = errors.New("injected command journal write failure")

type commandJournalFailWriter struct{}

func (commandJournalFailWriter) Write([]byte) (int, error) {
	return 0, errCommandJournalInjectedWrite
}

func (j *CommandJournal) installBufferedWriterForTest(w io.Writer, size int) func() {
	old := j.writer.bw
	j.writer.bw = bufio.NewWriterSize(w, size)
	return func() {
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
