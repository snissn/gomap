package caching

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

var errStaleValueLogWriterUsed = errors.New("test: stale value-log writer used after rotation")
var errRotateFailed = errors.New("test: rotate failed")

type rotateSwapValueWriter struct {
	lane *lane

	size int64

	rotated         bool
	usedAfterRotate bool
	appends         int
}

var _ valueWriter = (*rotateSwapValueWriter)(nil)

func (w *rotateSwapValueWriter) Append(dictID uint64, dict []byte, rid uint64, value []byte) (page.ValuePtr, error) {
	return page.ValuePtr{}, errors.New("test: unexpected Append call")
}

func (w *rotateSwapValueWriter) AppendFrame(dictID uint64, dict []byte, records []valuelog.Record) ([]page.ValuePtr, error) {
	return nil, errors.New("test: unexpected AppendFrame call")
}

func (w *rotateSwapValueWriter) SetDictFrameEncoderOptions(level zstd.EncoderLevel, enableEntropy bool) {
	// No-op for test stub.
}

func (w *rotateSwapValueWriter) AppendRawFramesWritevInto(records []valuelog.Record, k int, dst []page.ValuePtr) ([]page.ValuePtr, valuelog.FrameStats, error) {
	if w.rotated {
		w.usedAfterRotate = true
		return nil, valuelog.FrameStats{}, errStaleValueLogWriterUsed
	}
	if len(dst) != len(records) {
		return nil, valuelog.FrameStats{}, errors.New("test: dst size mismatch")
	}
	w.appends++
	var rawBytes int
	for i := range records {
		rawBytes += len(records[i].Value)
		dst[i] = page.ValuePtr{
			Offset: uint64(w.size),
			Length: uint32(len(records[i].Value)),
			FileID: page.ValueLogFileID(1),
		}
		w.size += int64(len(records[i].Value))
	}
	return dst, valuelog.FrameStats{
		Records:            len(records),
		RawPayloadBytes:    rawBytes,
		StoredPayloadBytes: rawBytes,
	}, nil
}

func (w *rotateSwapValueWriter) RotateTo(path string, fileID uint32) error {
	if w.rotated {
		return errors.New("test: duplicate rotation")
	}
	w.rotated = true
	w.lane.vlog = &rotateSwapValueWriter{
		lane: w.lane,
		size: 0,
	}
	return nil
}

func (w *rotateSwapValueWriter) Size() int64  { return w.size }
func (w *rotateSwapValueWriter) Flush() error { return nil }
func (w *rotateSwapValueWriter) Sync() error  { return nil }
func (w *rotateSwapValueWriter) Close() error { return nil }

type rotateFailValueWriter struct {
	size int64
}

func (w *rotateFailValueWriter) Append(dictID uint64, dict []byte, rid uint64, value []byte) (page.ValuePtr, error) {
	return page.ValuePtr{}, errors.New("test: unexpected Append call")
}

func (w *rotateFailValueWriter) AppendFrame(dictID uint64, dict []byte, records []valuelog.Record) ([]page.ValuePtr, error) {
	return nil, errors.New("test: unexpected AppendFrame call")
}

func (w *rotateFailValueWriter) SetDictFrameEncoderOptions(level zstd.EncoderLevel, enableEntropy bool) {
}
func (w *rotateFailValueWriter) RotateTo(path string, fileID uint32) error { return errRotateFailed }
func (w *rotateFailValueWriter) Size() int64                               { return w.size }
func (w *rotateFailValueWriter) Flush() error                              { return nil }
func (w *rotateFailValueWriter) Sync() error                               { return nil }
func (w *rotateFailValueWriter) Close() error                              { return nil }

type rotateFailCommitWriter struct {
	size int64
}

func (w *rotateFailCommitWriter) Append(record logRecord) error {
	return errors.New("test: unexpected Append call")
}
func (w *rotateFailCommitWriter) AppendBatch(records []logRecord) error {
	return errors.New("test: unexpected AppendBatch call")
}
func (w *rotateFailCommitWriter) RotateTo(path string) error { return errRotateFailed }
func (w *rotateFailCommitWriter) Size() int64                { return w.size }
func (w *rotateFailCommitWriter) Flush() error               { return nil }
func (w *rotateFailCommitWriter) Sync() error                { return nil }
func (w *rotateFailCommitWriter) Close() error               { return nil }

func TestAppendValueLog_ReloadWriterAfterRotation(t *testing.T) {
	t.Parallel()

	prevMetrics := vlogAutotuneMetricsEnabled.Load()
	vlogAutotuneMetricsEnabled.Store(false)
	t.Cleanup(func() { vlogAutotuneMetricsEnabled.Store(prevMetrics) })

	db := &DB{
		dir:                     t.TempDir(),
		closeCh:                 make(chan struct{}),
		valueLogCompressionMode: uint8(vlogCompressionOff),
	}
	db.valueLogMaxSegmentBytes = 1024
	db.valueLogAutotuneOptions.Mode = valuelog.AutotuneOff

	l := &lane{id: 0}
	old := &rotateSwapValueWriter{
		lane: l,
		size: 900, // Below max, but near enough to force a preflight rotate.
	}
	l.vlog = old

	records := []valuelog.Record{
		{RID: 1, Value: bytes.Repeat([]byte("a"), 32)},
		{RID: 2, Value: bytes.Repeat([]byte("b"), 32)},
	}

	ptrs, err := db.appendValueLog(l, 0, nil, records, journalDurabilityNone, false)
	if err != nil {
		t.Fatalf("appendValueLog: %v", err)
	}
	if !old.rotated {
		t.Fatalf("expected writer rotation")
	}
	if old.usedAfterRotate {
		t.Fatalf("stale writer used after rotation")
	}
	if l.vlog == old {
		t.Fatalf("expected lane writer to be replaced on rotation")
	}
	if got, want := len(ptrs), len(records); got != want {
		t.Fatalf("ptrs length: got %d want %d", got, want)
	}
	putValueLogPtrs(ptrs)
}

func TestRotateValueLogMuHeld_DoesNotAdvanceSeqOnFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	oldPath := filepath.Join(dir, valueLogName(0, 59))
	writer := &rotateFailValueWriter{size: 128}
	l := &lane{
		id:       0,
		vlog:     writer,
		vlogSeq:  59,
		vlogPath: oldPath,
	}
	db := &DB{dir: dir}

	err := db.rotateValueLogLocked(l)
	if !errors.Is(err, errRotateFailed) {
		t.Fatalf("rotateValueLogLocked err=%v want=%v", err, errRotateFailed)
	}
	if got := l.vlogSeq; got != 59 {
		t.Fatalf("vlogSeq=%d want 59", got)
	}
	if got := l.vlogPath; got != oldPath {
		t.Fatalf("vlogPath=%q want %q", got, oldPath)
	}
	if l.vlog != writer {
		t.Fatalf("expected original writer to remain installed")
	}
}

func TestRotateWALLockedWithOptions_DoesNotAdvanceSeqOnFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	oldPath := filepath.Join(dir, commitLogName(0, 59))
	writer := &rotateFailCommitWriter{size: 128}
	l := &lane{
		id:      0,
		wal:     writer,
		walSeq:  59,
		walPath: oldPath,
	}
	db := &DB{dir: dir}

	err := db.rotateWALLockedWithOptions(l, false)
	if !errors.Is(err, errRotateFailed) {
		t.Fatalf("rotateWALLockedWithOptions err=%v want=%v", err, errRotateFailed)
	}
	if got := l.walSeq; got != 59 {
		t.Fatalf("walSeq=%d want 59", got)
	}
	if got := l.walPath; got != oldPath {
		t.Fatalf("walPath=%q want %q", got, oldPath)
	}
	if l.wal != writer {
		t.Fatalf("expected original writer to remain installed")
	}
}
