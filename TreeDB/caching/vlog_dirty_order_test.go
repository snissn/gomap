package caching

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type vlogDirtyOrderWriter struct {
	size      int64
	flushes   atomic.Int64
	syncs     atomic.Int64
	appendErr error
	syncErr   error
}

var _ valueWriter = (*vlogDirtyOrderWriter)(nil)

func (w *vlogDirtyOrderWriter) Append(dictID uint64, dict []byte, rid uint64, value []byte) (page.ValuePtr, error) {
	if w.appendErr != nil {
		return page.ValuePtr{}, w.appendErr
	}
	if rid == 0 {
		return page.ValuePtr{}, errors.New("missing rid")
	}
	start := w.size
	w.size += int64(len(value) + 32)
	return page.ValuePtr{
		Offset: uint64(start + 4),
		Length: page.ValuePtrMarkGrouped(16, 0),
		FileID: page.ValueLogFileID(1),
	}, nil
}

func (w *vlogDirtyOrderWriter) AppendFrame(dictID uint64, dict []byte, records []valuelog.Record) ([]page.ValuePtr, error) {
	if w.appendErr != nil {
		return nil, w.appendErr
	}
	if len(records) == 0 {
		return nil, nil
	}
	out := make([]page.ValuePtr, len(records))
	for i := range records {
		if records[i].RID == 0 {
			return nil, errors.New("missing rid")
		}
		start := w.size
		w.size += int64(len(records[i].Value) + 32)
		out[i] = page.ValuePtr{
			Offset: uint64(start + 4),
			Length: page.ValuePtrMarkGrouped(16, uint8(i)),
			FileID: page.ValueLogFileID(1),
		}
	}
	return out, nil
}

func (w *vlogDirtyOrderWriter) SetDictFrameEncoderOptions(level zstd.EncoderLevel, enableEntropy bool) {
}
func (w *vlogDirtyOrderWriter) RotateTo(path string, fileID uint32) error {
	return w.RotateToWithSync(path, fileID, true)
}
func (w *vlogDirtyOrderWriter) RotateToWithSync(path string, fileID uint32, syncCurrent bool) error {
	return nil
}
func (w *vlogDirtyOrderWriter) Size() int64 { return w.size }
func (w *vlogDirtyOrderWriter) Flush() error {
	w.flushes.Add(1)
	return nil
}
func (w *vlogDirtyOrderWriter) Sync() error {
	w.syncs.Add(1)
	return w.syncErr
}
func (w *vlogDirtyOrderWriter) Close() error { return nil }

func waitErr(t *testing.T, ch <-chan error, label string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(2 * time.Second):
		t.Fatalf("%s timed out", label)
		return nil
	}
}

func waitBool(t *testing.T, ch <-chan bool, label string) bool {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatalf("%s timed out", label)
		return false
	}
}

func TestFlushValueLogLane_WaitsForBarrierBeforeDirtyCheck(t *testing.T) {
	db := &DB{closeCh: make(chan struct{}), splitValueLog: true}
	l := &lane{id: 0}
	w := &vlogDirtyOrderWriter{}
	l.vlog = w
	l.vlogDirty.Store(false)

	l.vlogMu.Lock()
	go func() {
		l.vlogDirty.Store(true)
		l.vlogMu.Unlock()
	}()

	err := db.flushValueLogLane(l)
	if err != nil {
		t.Fatalf("flushValueLogLane: %v", err)
	}

	if got := w.flushes.Load(); got != 1 {
		t.Fatalf("flushes=%d want 1", got)
	}
}

func TestFlushValueLogLaneWithSize_ReturnsFlushedWriterSize(t *testing.T) {
	db := &DB{closeCh: make(chan struct{}), splitValueLog: true}
	l := &lane{id: 0}
	w := &vlogDirtyOrderWriter{size: 4096}
	l.vlog = w
	l.vlogDirty.Store(true)

	size, err := db.flushValueLogLaneWithSize(l)
	if err != nil {
		t.Fatalf("flushValueLogLaneWithSize: %v", err)
	}
	if size != 4096 {
		t.Fatalf("flushed size=%d want 4096", size)
	}
	if got := w.flushes.Load(); got != 1 {
		t.Fatalf("flushes=%d want 1", got)
	}
}

func TestAppendValueLog_SetsDirtyBeforeUnlock(t *testing.T) {
	db := &DB{closeCh: make(chan struct{}), splitValueLog: true}
	l := &lane{id: 0}
	w := &vlogDirtyOrderWriter{}
	l.vlog = w

	seenDirty := make(chan bool, 1)
	db.testBeforeVlogUnlock = func(laneID int) {
		if laneID != int(l.id) {
			return
		}
		seenDirty <- l.vlogDirty.Load()
	}

	records := []valuelog.Record{
		{RID: 1, Value: []byte("v1")},
		{RID: 2, Value: []byte("v2")},
	}
	ptrs, err := db.appendValueLog(l, 0, nil, records, journalDurabilityNone)
	if err != nil {
		t.Fatalf("appendValueLog: %v", err)
	}
	putValueLogPtrs(ptrs)

	if got := waitBool(t, seenDirty, "dirty check"); !got {
		t.Fatalf("vlogDirty=false at pre-unlock hook; expected true")
	}

	if err := db.flushValueLogLane(l); err != nil {
		t.Fatalf("flushValueLogLane: %v", err)
	}
	if got := w.flushes.Load(); got != 1 {
		t.Fatalf("flushes=%d want 1", got)
	}
	if l.vlogDirty.Load() {
		t.Fatalf("expected vlogDirty=false after concurrent flush")
	}
}

func TestAppendValueLogOne_DirectSetsDirtyBeforeUnlock(t *testing.T) {
	db := &DB{closeCh: make(chan struct{}), splitValueLog: true}
	l := &lane{id: 0}
	w := &vlogDirtyOrderWriter{}
	l.vlog = w

	seenDirty := make(chan bool, 1)
	db.testBeforeVlogUnlock = func(laneID int) {
		if laneID != int(l.id) {
			return
		}
		seenDirty <- l.vlogDirty.Load()
	}

	ptr, _, err := db.appendValueLogOne(l, 0, nil, 1, []byte("small"), journalDurabilityNone)
	if err != nil {
		t.Fatalf("appendValueLogOne: %v", err)
	}
	if ptr.FileID == 0 {
		t.Fatalf("expected non-empty pointer")
	}

	if got := waitBool(t, seenDirty, "dirty check"); !got {
		t.Fatalf("vlogDirty=false at pre-unlock hook; expected true")
	}

	if err := db.flushValueLogLane(l); err != nil {
		t.Fatalf("flushValueLogLane: %v", err)
	}
	if got := w.flushes.Load(); got != 1 {
		t.Fatalf("flushes=%d want 1", got)
	}
	if l.vlogDirty.Load() {
		t.Fatalf("expected vlogDirty=false after concurrent flush")
	}
}

func TestAppendValueLogOne_QueuedFastPathSetsDirtyBeforeUnlock(t *testing.T) {
	db := &DB{closeCh: make(chan struct{}), splitValueLog: true}
	l := &lane{id: 0}
	w := &vlogDirtyOrderWriter{}
	l.vlog = w
	l.vlogCh = make(chan vlogWriteRequest, 1)

	seenDirty := make(chan bool, 1)
	db.testBeforeVlogUnlock = func(laneID int) {
		if laneID != int(l.id) {
			return
		}
		seenDirty <- l.vlogDirty.Load()
	}

	value := make([]byte, vlogQueueMinValueSize)
	ptr, _, err := db.appendValueLogOne(l, 0, nil, 1, value, journalDurabilityNone)
	if err != nil {
		t.Fatalf("appendValueLogOne: %v", err)
	}
	if ptr.FileID == 0 {
		t.Fatalf("expected non-empty pointer")
	}

	if got := waitBool(t, seenDirty, "dirty check"); !got {
		t.Fatalf("vlogDirty=false at pre-unlock hook; expected true")
	}

	if err := db.flushValueLogLane(l); err != nil {
		t.Fatalf("flushValueLogLane: %v", err)
	}
	if got := w.flushes.Load(); got != 1 {
		t.Fatalf("flushes=%d want 1", got)
	}
	if l.vlogDirty.Load() {
		t.Fatalf("expected vlogDirty=false after concurrent flush")
	}
}

type dictStoreBytes struct {
	current uint64
	dicts   map[uint64][]byte
}

func (s *dictStoreBytes) GetCurrent(context.Context) (uint64, error) {
	return s.current, nil
}

func (s *dictStoreBytes) GetDictBytes(_ context.Context, dictID uint64) ([]byte, error) {
	if s.dicts == nil {
		return nil, nil
	}
	return s.dicts[dictID], nil
}

func TestSetDictStore_ClearsLaneDictByteCaches(t *testing.T) {
	db := &DB{
		lanes: make([]lane, 1),
	}
	l := &db.lanes[0]

	oldStore := &dictStoreBytes{
		current: 7,
		dicts: map[uint64][]byte{
			7: []byte("old"),
		},
	}
	db.SetDictStore(oldStore)
	gotOld, err := db.dictBytesForLane(context.Background(), l, 7)
	if err != nil {
		t.Fatalf("dictBytesForLane(old): %v", err)
	}
	if string(gotOld) != "old" {
		t.Fatalf("old dict=%q want %q", string(gotOld), "old")
	}

	newStore := &dictStoreBytes{
		current: 7,
		dicts: map[uint64][]byte{
			7: []byte("new"),
		},
	}
	db.SetDictStore(newStore)

	gotNew, err := db.dictBytesForLane(context.Background(), l, 7)
	if err != nil {
		t.Fatalf("dictBytesForLane(new): %v", err)
	}
	if string(gotNew) != "new" {
		t.Fatalf("dict after SetDictStore=%q want %q", string(gotNew), "new")
	}
}
