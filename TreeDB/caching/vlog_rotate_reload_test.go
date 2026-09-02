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
var errCommitRotateCleanup = errors.New("test: commit-log predecessor cleanup failed")
var errSplitValueRotateFailed = errors.New("test: split value-log rotation failed")

type rotateSwapValueWriter struct {
	lane *lane

	size int64

	rotated         bool
	usedAfterRotate bool
	appends         int
	syncCurrent     bool
	syncCurrentSet  bool
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
	return w.RotateToWithSync(path, fileID, true)
}

func (w *rotateSwapValueWriter) RotateToWithSync(path string, fileID uint32, syncCurrent bool) error {
	if w.rotated {
		return errors.New("test: duplicate rotation")
	}
	w.rotated = true
	w.syncCurrent = syncCurrent
	w.syncCurrentSet = true
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
	size        int64
	installed   bool
	rotateErr   error
	rotateCalls int
}

type installedValueLogRotationError struct{ err error }

func (err installedValueLogRotationError) Error() string       { return err.err.Error() }
func (err installedValueLogRotationError) Unwrap() error       { return err.err }
func (installedValueLogRotationError) RotationInstalled() bool { return true }

func (w *rotateFailValueWriter) Append(dictID uint64, dict []byte, rid uint64, value []byte) (page.ValuePtr, error) {
	return page.ValuePtr{}, errors.New("test: unexpected Append call")
}

func (w *rotateFailValueWriter) AppendFrame(dictID uint64, dict []byte, records []valuelog.Record) ([]page.ValuePtr, error) {
	return nil, errors.New("test: unexpected AppendFrame call")
}

func (w *rotateFailValueWriter) SetDictFrameEncoderOptions(level zstd.EncoderLevel, enableEntropy bool) {
}
func (w *rotateFailValueWriter) RotateTo(path string, fileID uint32) error {
	return w.RotateToWithSync(path, fileID, true)
}
func (w *rotateFailValueWriter) RotateToWithSync(path string, fileID uint32, syncCurrent bool) error {
	w.rotateCalls++
	err := w.rotateErr
	if err == nil {
		err = errRotateFailed
	}
	if w.installed {
		return installedValueLogRotationError{err: err}
	}
	return err
}
func (w *rotateFailValueWriter) Size() int64  { return w.size }
func (w *rotateFailValueWriter) Flush() error { return nil }
func (w *rotateFailValueWriter) Sync() error  { return nil }
func (w *rotateFailValueWriter) Close() error { return nil }

type rotateFailCommitWriter struct {
	size        int64
	installed   bool
	rotateErr   error
	rotateCalls int
	currentPath string
}

type installedCommitLogRotationError struct{ err error }

func (err installedCommitLogRotationError) Error() string       { return err.err.Error() }
func (err installedCommitLogRotationError) Unwrap() error       { return err.err }
func (installedCommitLogRotationError) RotationInstalled() bool { return true }

func (w *rotateFailCommitWriter) Append(record logRecord) error {
	return errors.New("test: unexpected Append call")
}
func (w *rotateFailCommitWriter) AppendBatch(records []logRecord) error {
	return errors.New("test: unexpected AppendBatch call")
}
func (w *rotateFailCommitWriter) RotateTo(path string) error {
	return w.RotateToWithSync(path, true)
}
func (w *rotateFailCommitWriter) RotateToWithSync(path string, syncCurrent bool) error {
	w.rotateCalls++
	err := w.rotateErr
	if err == nil {
		err = errRotateFailed
	}
	if w.installed {
		w.currentPath = path
		return installedCommitLogRotationError{err: err}
	}
	return err
}
func (w *rotateFailCommitWriter) Size() int64  { return w.size }
func (w *rotateFailCommitWriter) Flush() error { return nil }
func (w *rotateFailCommitWriter) Sync() error  { return nil }
func (w *rotateFailCommitWriter) Close() error { return nil }

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

	ptrs, err := db.appendValueLog(l, 0, nil, records, journalDurabilityNone)
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

func TestAppendValueLog_RelaxedSyncRotatesWithoutFsync(t *testing.T) {
	t.Parallel()

	prevMetrics := vlogAutotuneMetricsEnabled.Load()
	vlogAutotuneMetricsEnabled.Store(false)
	t.Cleanup(func() { vlogAutotuneMetricsEnabled.Store(prevMetrics) })

	db := &DB{
		dir:                     t.TempDir(),
		closeCh:                 make(chan struct{}),
		valueLogCompressionMode: uint8(vlogCompressionOff),
		relaxedSync:             true,
	}
	db.valueLogMaxSegmentBytes = 1024
	db.valueLogAutotuneOptions.Mode = valuelog.AutotuneOff

	l := &lane{id: 0}
	old := &rotateSwapValueWriter{
		lane: l,
		size: 900,
	}
	l.vlog = old

	ptrs, err := db.appendValueLog(l, 0, nil, []valuelog.Record{
		{RID: 1, Value: bytes.Repeat([]byte("a"), 32)},
		{RID: 2, Value: bytes.Repeat([]byte("b"), 32)},
	}, journalDurabilityNone)
	if err != nil {
		t.Fatalf("appendValueLog: %v", err)
	}
	defer putValueLogPtrs(ptrs)
	if !old.rotated {
		t.Fatalf("expected writer rotation")
	}
	if !old.syncCurrentSet {
		t.Fatalf("RotateToWithSync was not called")
	}
	if old.syncCurrent {
		t.Fatalf("relaxed sync rotation used syncCurrent=true")
	}
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

func TestRotateValueLogMuHeld_CompletesMetadataAfterInstalledCleanupFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	oldPath := filepath.Join(dir, valueLogName(0, 59))
	writer := &rotateFailValueWriter{size: 128, installed: true}
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
	if got := l.vlogSeq; got != 60 {
		t.Fatalf("vlogSeq=%d want 60 after installed rotation", got)
	}
	wantPath := filepath.Join(db.valueLogDirForLane(l), valueLogName(l.id, 60))
	if got := l.vlogPath; got != wantPath {
		t.Fatalf("vlogPath=%q want %q after installed rotation", got, wantPath)
	}
	if got := l.vlogClosedSizes[oldPath]; got != writer.size {
		t.Fatalf("closed size=%d want %d", got, writer.size)
	}
	if l.vlog != writer {
		t.Fatalf("expected installed writer to remain authoritative")
	}
}

func TestRotateWALLockedWithOptions_DoesNotAdvanceSeqOnFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	oldPath := filepath.Join(dir, commitLogName(0, 59))
	writer := &rotateFailCommitWriter{size: 128, currentPath: oldPath}
	oldVLogPath := filepath.Join(dir, valueLogName(0, 9))
	vlog := &rotateFailValueWriter{size: 64, rotateErr: errSplitValueRotateFailed}
	l := &lane{
		id:             0,
		wal:            writer,
		walSeq:         59,
		walPath:        oldPath,
		walClosedSizes: map[string]int64{oldPath: 17},
		walCoalesceSeq: 41,
		vlog:           vlog,
		vlogSeq:        9,
		vlogPath:       oldVLogPath,
	}
	l.walLiveBytes.Store(73)
	l.walClosedBytes.Store(17)
	l.vlogLiveBytes.Store(29)
	db := &DB{dir: dir}
	db.nextCommitSeq.Store(41)

	err := db.rotateWALLockedWithOptions(l, true)
	if !errors.Is(err, errRotateFailed) {
		t.Fatalf("rotateWALLockedWithOptions err=%v want=%v", err, errRotateFailed)
	}
	if got := l.walSeq; got != 59 {
		t.Fatalf("walSeq=%d want 59", got)
	}
	if got := l.walPath; got != oldPath {
		t.Fatalf("walPath=%q want %q", got, oldPath)
	}
	if got := l.walLiveBytes.Load(); got != 73 {
		t.Fatalf("walLiveBytes=%d want 73", got)
	}
	if got := l.walClosedSizes[oldPath]; got != 17 {
		t.Fatalf("closed size=%d want unchanged 17", got)
	}
	if got := l.walClosedBytes.Load(); got != 17 {
		t.Fatalf("walClosedBytes=%d want unchanged 17", got)
	}
	if got := l.walCoalesceSeq; got != 0 {
		t.Fatalf("walCoalesceSeq=%d want reset after a possibly flushed rotation failure", got)
	}
	if got := db.nextWALCoalesceSeqLocked(l); got != 42 {
		t.Fatalf("next coalescing sequence=%d want new post-flush fence 42", got)
	}
	if l.wal != writer {
		t.Fatalf("expected original writer to remain installed")
	}
	if writer.currentPath != oldPath || writer.rotateCalls != 1 {
		t.Fatalf("writer authority path=%q calls=%d want old path and one failed attempt", writer.currentPath, writer.rotateCalls)
	}
	if l.vlogSeq != 9 || l.vlogPath != oldVLogPath || l.vlogLiveBytes.Load() != 29 || vlog.rotateCalls != 0 {
		t.Fatalf("split value-log changed after pre-install WAL failure: seq=%d path=%q live=%d calls=%d", l.vlogSeq, l.vlogPath, l.vlogLiveBytes.Load(), vlog.rotateCalls)
	}
}

func TestRotateWALLockedWithOptions_CompletesMetadataAfterInstalledCleanupFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	oldPath := filepath.Join(dir, commitLogName(0, 59))
	writer := &rotateFailCommitWriter{
		size: 128, installed: true, rotateErr: errCommitRotateCleanup, currentPath: oldPath,
	}
	l := &lane{
		id:             0,
		wal:            writer,
		walSeq:         59,
		walPath:        oldPath,
		walClosedSizes: map[string]int64{oldPath: 17},
		walCoalesceSeq: 41,
	}
	l.walLiveBytes.Store(73)
	l.walClosedBytes.Store(17)
	db := &DB{dir: dir}

	err := db.rotateWALLockedWithOptions(l, false)
	if !errors.Is(err, errCommitRotateCleanup) {
		t.Fatalf("rotateWALLockedWithOptions err=%v want=%v", err, errCommitRotateCleanup)
	}
	wantPath := filepath.Join(dir, commitLogName(0, 60))
	if got := l.walSeq; got != 60 {
		t.Fatalf("walSeq=%d want 60 after installed rotation", got)
	}
	if got := l.walPath; got != wantPath {
		t.Fatalf("walPath=%q want %q after installed rotation", got, wantPath)
	}
	if got := l.walLiveBytes.Load(); got != 0 {
		t.Fatalf("walLiveBytes=%d want 0 after installed rotation", got)
	}
	if got := l.walClosedSizes[oldPath]; got != writer.size {
		t.Fatalf("closed size=%d want %d", got, writer.size)
	}
	if got := l.walClosedBytes.Load(); got != writer.size {
		t.Fatalf("walClosedBytes=%d want %d", got, writer.size)
	}
	if got := l.walCoalesceSeq; got != 0 {
		t.Fatalf("walCoalesceSeq=%d want 0 after installed rotation", got)
	}
	if l.wal != writer || writer.currentPath != wantPath || writer.rotateCalls != 1 {
		t.Fatalf("writer authority lane=%p writer=%p path=%q calls=%d", l.wal, writer, writer.currentPath, writer.rotateCalls)
	}
}

func TestRotateWALLockedWithOptions_JoinsInstalledCleanupAndSplitValueLogErrors(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	oldWALPath := filepath.Join(dir, commitLogName(0, 59))
	wal := &rotateFailCommitWriter{
		size: 128, installed: true, rotateErr: errCommitRotateCleanup, currentPath: oldWALPath,
	}
	oldVLogPath := filepath.Join(dir, valueLogName(0, 9))
	vlog := &rotateFailValueWriter{size: 64, rotateErr: errSplitValueRotateFailed}
	l := &lane{
		id:             0,
		wal:            wal,
		walSeq:         59,
		walPath:        oldWALPath,
		walClosedSizes: map[string]int64{},
		vlog:           vlog,
		vlogSeq:        9,
		vlogPath:       oldVLogPath,
	}
	l.walLiveBytes.Store(73)
	l.vlogLiveBytes.Store(29)
	db := &DB{dir: dir}

	err := db.rotateWALLockedWithOptions(l, true)
	if !errors.Is(err, errCommitRotateCleanup) || !errors.Is(err, errSplitValueRotateFailed) {
		t.Fatalf("rotateWALLockedWithOptions err=%v want both WAL cleanup and split value-log errors", err)
	}
	wantWALPath := filepath.Join(dir, commitLogName(0, 60))
	if l.walSeq != 60 || l.walPath != wantWALPath || l.walLiveBytes.Load() != 0 {
		t.Fatalf("WAL authority seq=%d path=%q live=%d want 60/%q/0", l.walSeq, l.walPath, l.walLiveBytes.Load(), wantWALPath)
	}
	if l.walClosedSizes[oldWALPath] != wal.size || l.walClosedBytes.Load() != wal.size {
		t.Fatalf("WAL accounting size=%d total=%d want %d", l.walClosedSizes[oldWALPath], l.walClosedBytes.Load(), wal.size)
	}
	if l.wal != wal || wal.currentPath != wantWALPath || wal.rotateCalls != 1 {
		t.Fatalf("WAL writer authority lane=%p writer=%p path=%q calls=%d", l.wal, wal, wal.currentPath, wal.rotateCalls)
	}
	if l.vlogSeq != 9 || l.vlogPath != oldVLogPath || l.vlogLiveBytes.Load() != 29 || vlog.rotateCalls != 1 {
		t.Fatalf("split value-log authority seq=%d path=%q live=%d calls=%d", l.vlogSeq, l.vlogPath, l.vlogLiveBytes.Load(), vlog.rotateCalls)
	}
}
