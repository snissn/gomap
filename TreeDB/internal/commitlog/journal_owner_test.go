package commitlog

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
)

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
