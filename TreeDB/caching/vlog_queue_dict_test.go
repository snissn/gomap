package caching

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

type queueDictStore struct {
	current uint64
	dicts   map[uint64][]byte
}

func (s *queueDictStore) GetCurrent(context.Context) (uint64, error) {
	if s == nil {
		return 0, nil
	}
	return s.current, nil
}

func (s *queueDictStore) GetDictBytes(_ context.Context, dictID uint64) ([]byte, error) {
	if s == nil {
		return nil, errors.New("nil dict store")
	}
	out, ok := s.dicts[dictID]
	if !ok || len(out) == 0 {
		return nil, errors.New("missing dict")
	}
	return out, nil
}

func queueTestValue(tag byte, size int, suffix uint32) []byte {
	if size < 8 {
		size = 8
	}
	seed := []byte{tag, '-', 'v', '-', 'l', 'o', 'g', '-'}
	repeat := size/len(seed) + 1
	v := make([]byte, size)
	copy(v, bytes.Repeat(seed, repeat))
	binary.LittleEndian.PutUint32(v[size-4:], suffix)
	return v
}

func buildQueueTestDict(tb testing.TB, dictID uint64, samples [][]byte) []byte {
	tb.Helper()
	history := make([]byte, 0, 32<<10)
	for _, s := range samples {
		if len(history) >= cap(history) {
			break
		}
		need := cap(history) - len(history)
		if len(s) > need {
			s = s[:need]
		}
		history = append(history, s...)
	}
	if len(history) < 8 {
		history = append(history, bytes.Repeat([]byte{'x'}, 8-len(history))...)
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		Contents: samples,
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		tb.Fatalf("BuildDict: %v", err)
	}
	if len(dict) == 0 {
		tb.Fatalf("BuildDict: empty dict")
	}
	return dict
}

func readQueuePtr(t *testing.T, path string, ptr page.ValuePtr, dictStore *queueDictStore) []byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open value log: %v", err)
	}
	defer func() { _ = f.Close() }()
	dictLookup := func(dictID uint64) ([]byte, error) {
		return dictStore.GetDictBytes(context.Background(), dictID)
	}
	got, err := valuelog.ReadAtWithDict(f, ptr, true, dictLookup, nil, nil, templ.DecodeOptions{})
	if err != nil {
		t.Fatalf("ReadAtWithDict: %v", err)
	}
	return got
}

func TestFlushVlogRequests_MixedDictAndRawPointersResolve(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	t.Cleanup(func() { _ = writer.Close() })

	const dictID = uint64(7)
	const missingDictID = uint64(99)

	dictSamples := [][]byte{
		queueTestValue('a', 20<<10, 1),
		queueTestValue('a', 20<<10, 2),
		queueTestValue('a', 20<<10, 3),
		queueTestValue('a', 20<<10, 4),
	}
	store := &queueDictStore{
		current: dictID,
		dicts: map[uint64][]byte{
			dictID: buildQueueTestDict(t, dictID, dictSamples),
		},
	}

	db := &DB{
		closeCh:   make(chan struct{}),
		dictStore: store,
		valueLogAutotuneOptions: valuelog.AutotuneOptions{
			Mode: valuelog.AutotuneOff,
		},
		lanes: []lane{{id: 0, vlog: writer}},
	}
	db.valueLogDictCurrentK.Store(8)
	db.valueLogDictLastAppliedDictID.Store(dictID)

	values := [][]byte{
		queueTestValue('a', 20<<10, 10), // dict on
		queueTestValue('a', 20<<10, 11), // dict on
		queueTestValue('a', 20<<10, 12), // missing dict -> raw fallback
		queueTestValue('r', 20<<10, 13), // raw
		queueTestValue('a', 20<<10, 14), // dict on
		queueTestValue('r', 20<<10, 15), // raw
	}
	dictIDs := []uint64{dictID, dictID, missingDictID, 0, dictID, 0}

	requests := make([]vlogWriteRequest, len(values))
	for i := range values {
		ack := &vlogAck{}
		ack.wg.Add(1)
		requests[i] = vlogWriteRequest{
			rid:        uint64(i + 1),
			value:      values[i],
			dictID:     dictIDs[i],
			durability: journalDurabilityFlush,
			ack:        ack,
		}
	}

	db.flushVlogRequests(&db.lanes[0], requests)
	for i := range requests {
		ack := requests[i].ack
		ack.wg.Wait()
		if ack.err != nil {
			t.Fatalf("ack[%d] err: %v", i, ack.err)
		}
		if ack.ptr == (page.ValuePtr{}) {
			t.Fatalf("ack[%d] missing pointer", i)
		}
		got := readQueuePtr(t, path, ack.ptr, store)
		if !bytes.Equal(got, values[i]) {
			t.Fatalf("ack[%d] value mismatch", i)
		}
	}
}

func TestAppendValueLogOne_QueuedDictProbeClearsPause(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	const dictID = uint64(3)
	samples := [][]byte{
		queueTestValue('p', 32<<10, 1),
		queueTestValue('p', 32<<10, 2),
		queueTestValue('p', 32<<10, 3),
		queueTestValue('p', 32<<10, 4),
	}
	store := &queueDictStore{
		current: dictID,
		dicts: map[uint64][]byte{
			dictID: buildQueueTestDict(t, dictID, samples),
		},
	}

	db := &DB{
		closeCh:   make(chan struct{}),
		dictStore: store,
		valueLogAutotuneOptions: valuelog.AutotuneOptions{
			Mode: valuelog.AutotuneOff,
		},
		lanes: []lane{{id: 0, vlog: writer}},
	}
	db.valueLogDictCurrentK.Store(8)
	db.valueLogDictLastAppliedDictID.Store(dictID)
	db.startVlogWriter(&db.lanes[0])
	t.Cleanup(func() {
		close(db.closeCh)
		db.wg.Wait()
	})

	value := queueTestValue('p', 32<<10, 100)
	db.valueLogDictProbeBytes = uint64(len(value))
	db.valueLogDictPauseRemaining.Store(uint64(len(value) * 2))
	db.valueLogDictProbeRemaining.Store(1)

	ptr, _, err := db.appendValueLogOne(&db.lanes[0], dictID, nil, 1, value, journalDurabilityFlush)
	if err != nil {
		t.Fatalf("appendValueLogOne: %v", err)
	}
	if ptr == (page.ValuePtr{}) {
		t.Fatalf("appendValueLogOne returned empty pointer")
	}
	if pause := db.valueLogDictPauseRemaining.Load(); pause != 0 {
		t.Fatalf("expected probe success to clear pause, got=%d", pause)
	}
	if kept := db.valueLogDictFrames.kept.Load(); kept == 0 {
		t.Fatalf("expected dict probe to keep at least one frame")
	}

	got := readQueuePtr(t, path, ptr, store)
	if !bytes.Equal(got, value) {
		t.Fatalf("queued probe value mismatch")
	}
}
