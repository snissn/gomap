package collectionwal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAppenderWritesScannableSegment(t *testing.T) {
	dir := t.TempDir()
	app, err := CreateSegmentAppender(dir, AppenderOptions{SegmentBytes: 4096})
	if err != nil {
		t.Fatalf("CreateSegmentAppender: %v", err)
	}
	defer func() { _ = app.Close() }()

	txn := testTransaction(0, 1)
	result, err := app.AppendTransaction(txn, true)
	if err != nil {
		t.Fatalf("AppendTransaction: %v", err)
	}
	if result.WALLSN != 1 || result.CollectionSeq != 1 || result.Offset != SegmentHeaderLen || result.Length == 0 {
		t.Fatalf("append result=%+v", result)
	}
	if result.Path != SegmentPath(dir, 0, 1) {
		t.Fatalf("append path=%q want %q", result.Path, SegmentPath(dir, 0, 1))
	}

	data, err := os.ReadFile(result.Path)
	if err != nil {
		t.Fatalf("ReadFile(segment): %v", err)
	}
	header, frames, err := ScanSegment(data, true)
	if err != nil {
		t.Fatalf("ScanSegment: %v", err)
	}
	if header.Lane != 0 || header.SegmentSeq != 1 || header.FirstWALLSN != 1 {
		t.Fatalf("segment header=%+v", header)
	}
	if len(frames) != 1 || frames[0].Outcome != OutcomeCompleteValid {
		t.Fatalf("frames=%+v", frames)
	}
	if frames[0].Transaction.WALLSN != 1 || frames[0].Transaction.CollectionSeq != 1 {
		t.Fatalf("decoded txn=%+v", frames[0].Transaction)
	}

	dirty, err := DirtySegments(dir)
	if err != nil {
		t.Fatalf("DirtySegments: %v", err)
	}
	if len(dirty) != 1 || dirty[0] != result.Path {
		t.Fatalf("dirty=%v want [%s]", dirty, result.Path)
	}
}

func TestAppenderRejectsOutOfOrderWALLSN(t *testing.T) {
	app, err := CreateSegmentAppender(t.TempDir(), AppenderOptions{SegmentBytes: 4096})
	if err != nil {
		t.Fatalf("CreateSegmentAppender: %v", err)
	}
	defer func() { _ = app.Close() }()

	txn := testTransaction(2, 1)
	_, err = app.AppendTransaction(txn, false)
	if !errors.Is(err, ErrCollectionWALSequenceGap) {
		t.Fatalf("AppendTransaction err=%v want ErrCollectionWALSequenceGap", err)
	}
}

func TestAppenderRejectsSegmentOverflow(t *testing.T) {
	app, err := CreateSegmentAppender(t.TempDir(), AppenderOptions{SegmentBytes: SegmentHeaderLen + 1})
	if err != nil {
		t.Fatalf("CreateSegmentAppender: %v", err)
	}
	defer func() { _ = app.Close() }()

	_, err = app.AppendTransaction(testTransaction(0, 1), false)
	if !errors.Is(err, ErrCollectionWALResourceLimit) {
		t.Fatalf("AppendTransaction err=%v want ErrCollectionWALResourceLimit", err)
	}
}

func TestAppenderDoesNotOverwriteExistingSegment(t *testing.T) {
	dir := t.TempDir()
	app, err := CreateSegmentAppender(dir, AppenderOptions{SegmentBytes: 4096})
	if err != nil {
		t.Fatalf("CreateSegmentAppender(first): %v", err)
	}
	defer func() { _ = app.Close() }()

	_, err = CreateSegmentAppender(dir, AppenderOptions{SegmentBytes: 4096})
	if !errors.Is(err, os.ErrExist) {
		t.Fatalf("CreateSegmentAppender(second) err=%v want os.ErrExist", err)
	}
}

func TestOpenOrCreateSegmentAppenderContinuesExistingSegment(t *testing.T) {
	dir := t.TempDir()
	app, err := OpenOrCreateSegmentAppender(dir, AppenderOptions{SegmentBytes: 4096})
	if err != nil {
		t.Fatalf("OpenOrCreateSegmentAppender(first): %v", err)
	}
	if _, err := app.AppendTransaction(testTransaction(0, 1), true); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if err := app.Close(); err != nil {
		t.Fatalf("close first appender: %v", err)
	}

	app, err = OpenOrCreateSegmentAppender(dir, AppenderOptions{SegmentBytes: 4096})
	if err != nil {
		t.Fatalf("OpenOrCreateSegmentAppender(second): %v", err)
	}
	defer func() { _ = app.Close() }()
	result, err := app.AppendTransaction(testTransaction(0, 2), true)
	if err != nil {
		t.Fatalf("second append: %v", err)
	}
	if result.WALLSN != 2 || result.CollectionSeq != 2 {
		t.Fatalf("second append result=%+v", result)
	}

	data, err := os.ReadFile(SegmentPath(dir, 0, 1))
	if err != nil {
		t.Fatalf("ReadFile(segment): %v", err)
	}
	_, frames, err := ScanSegment(data, true)
	if err != nil {
		t.Fatalf("ScanSegment: %v", err)
	}
	if len(frames) != 2 {
		t.Fatalf("frames=%d want 2", len(frames))
	}
	if frames[0].Transaction.WALLSN != 1 || frames[1].Transaction.WALLSN != 2 {
		t.Fatalf("frame WALLSNs=%d/%d want 1/2", frames[0].Transaction.WALLSN, frames[1].Transaction.WALLSN)
	}
}

func TestOpenSegmentAppenderTruncatesTerminalIncompleteTail(t *testing.T) {
	dir := t.TempDir()
	app, err := OpenOrCreateSegmentAppender(dir, AppenderOptions{SegmentBytes: 4096})
	if err != nil {
		t.Fatalf("OpenOrCreateSegmentAppender(first): %v", err)
	}
	if _, err := app.AppendTransaction(testTransaction(0, 1), true); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if err := app.Close(); err != nil {
		t.Fatalf("close first appender: %v", err)
	}

	partialFrame, err := EncodeTransactionFrame(testTransaction(2, 2))
	if err != nil {
		t.Fatalf("EncodeTransactionFrame(partial): %v", err)
	}
	segmentPath := SegmentPath(dir, 0, 1)
	file, err := os.OpenFile(segmentPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open segment for partial write: %v", err)
	}
	if _, err := file.Write(partialFrame[:len(partialFrame)/2]); err != nil {
		_ = file.Close()
		t.Fatalf("write partial frame: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close partial writer: %v", err)
	}

	data, err := os.ReadFile(segmentPath)
	if err != nil {
		t.Fatalf("ReadFile(with tail): %v", err)
	}
	_, frames, err := ScanSegment(data, true)
	if err != nil {
		t.Fatalf("ScanSegment(with tail): %v", err)
	}
	if len(frames) != 2 || frames[1].Outcome != OutcomeTerminalIncompleteTail {
		t.Fatalf("frames after partial write=%+v, want complete frame plus terminal tail", frames)
	}

	app, err = OpenOrCreateSegmentAppender(dir, AppenderOptions{SegmentBytes: 4096})
	if err != nil {
		t.Fatalf("OpenOrCreateSegmentAppender(after tail): %v", err)
	}
	defer func() { _ = app.Close() }()
	result, err := app.AppendTransaction(testTransaction(0, 2), true)
	if err != nil {
		t.Fatalf("append after tail truncation: %v", err)
	}
	if result.WALLSN != 2 || result.CollectionSeq != 2 {
		t.Fatalf("append result=%+v want WALLSN/CollectionSeq 2/2", result)
	}

	data, err = os.ReadFile(segmentPath)
	if err != nil {
		t.Fatalf("ReadFile(after append): %v", err)
	}
	_, frames, err = ScanSegment(data, true)
	if err != nil {
		t.Fatalf("ScanSegment(after append): %v", err)
	}
	if len(frames) != 2 {
		t.Fatalf("frames after append=%d want 2", len(frames))
	}
	for i, frame := range frames {
		want := uint64(i + 1)
		if frame.Outcome != OutcomeCompleteValid || frame.Transaction.WALLSN != want || frame.Transaction.CollectionSeq != want {
			t.Fatalf("frame %d=%+v want complete WALLSN/CollectionSeq %d", i, frame, want)
		}
	}
}

func TestSegmentNameAndPath(t *testing.T) {
	if got, want := SegmentName(7, 42), "collection-l7-000042.log"; got != want {
		t.Fatalf("SegmentName=%q want %q", got, want)
	}
	if !IsSegmentName(SegmentName(7, 42)) {
		t.Fatalf("SegmentName output is not recognized as segment")
	}
	if got, want := SegmentPath("/tmp/db", 7, 42), filepath.Join("/tmp/db", "wal", "collection-l7-000042.log"); got != want {
		t.Fatalf("SegmentPath=%q want %q", got, want)
	}
}
