package caching

import (
	"bytes"
	"errors"
	"testing"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

var errStaleValueLogWriterUsed = errors.New("test: stale value-log writer used after rotation")

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
			FileID: 1,
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

func TestAppendValueLog_ReloadWriterAfterRotation(t *testing.T) {
	t.Parallel()

	prevMetrics := vlogAutotuneMetricsEnabled.Load()
	vlogAutotuneMetricsEnabled.Store(false)
	t.Cleanup(func() { vlogAutotuneMetricsEnabled.Store(prevMetrics) })

	db := &DB{
		dir:     t.TempDir(),
		closeCh: make(chan struct{}),
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
