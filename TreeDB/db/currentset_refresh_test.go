package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func writeValueLogRecord(t *testing.T, dir string, lane, seq uint32, value []byte, rid uint64) (uint32, page.ValuePtr) {
	t.Helper()
	fileID, ptr, path := writeUnregisteredValueLogRecord(t, dir, lane, seq, value, rid)
	registerTestValueLogProducer(t, dir, path, fileID)
	return fileID, ptr
}

func writeUnregisteredValueLogRecord(t *testing.T, dir string, lane, seq uint32, value []byte, rid uint64) (uint32, page.ValuePtr, string) {
	t.Helper()
	fileID, err := valuelog.EncodeFileID(lane, seq)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "value_vlog", fmt.Sprintf("value-l%d-%06d.log", lane, seq))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	ptr, err := w.Append(0, nil, rid, value)
	if err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	return fileID, ptr, path
}

type registeredLeafPageLog struct {
	db         *DB
	dir        string
	path       string
	w          *valuelog.Writer
	fileID     uint32
	nextRID    uint64
	registered bool
}

func (l *registeredLeafPageLog) ensureWriter() error {
	if l.w != nil {
		return nil
	}
	fileID, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 1)
	if err != nil {
		return err
	}
	path := filepath.Join(LeafLogDirPath(l.dir), fmt.Sprintf("value-l%d-000001.log", rewriteLeafLogLaneID))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		return err
	}
	l.w = w
	l.fileID = fileID
	l.path = path
	if !l.registered {
		if err := l.db.RegisterValueLogSegment(path, fileID); err != nil {
			_ = w.Close()
			l.w = nil
			return err
		}
		l.registered = true
	}
	return nil
}

func (l *registeredLeafPageLog) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	if err := l.ensureWriter(); err != nil {
		return page.LeafLogPtr{}, err
	}
	l.nextRID++
	ptr, err := l.w.Append(0, nil, l.nextRID, leafPage)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	return page.LeafLogPtrFromValuePtr(ptr)
}

func (l *registeredLeafPageLog) Flush() error {
	if err := l.ensureWriter(); err != nil {
		return err
	}
	return l.w.Flush()
}

func (l *registeredLeafPageLog) Sync() error {
	if err := l.ensureWriter(); err != nil {
		return err
	}
	return l.w.Sync()
}

func (l *registeredLeafPageLog) Close() error {
	if l.w == nil {
		return nil
	}
	err := l.w.Close()
	l.w = nil
	return err
}

func (l *registeredLeafPageLog) CurrentValueLogSegment() (string, uint32, bool) {
	if l == nil || !l.registered || l.path == "" || l.fileID == 0 {
		return "", 0, false
	}
	return l.path, l.fileID, true
}

type createdThenCurrentLeafPageLog struct {
	dir             string
	writers         []*valuelog.Writer
	nextRID         uint64
	firstFileID     uint32
	currentPath     string
	currentFileID   uint32
	currentPaths    []string
	currentFileIDs  []uint32
	createdSegments []rewriteCreatedSegment
	reported        []LeafPageLogSegment
}

func (l *createdThenCurrentLeafPageLog) CreatedLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error) {
	if l == nil || len(l.createdSegments) == 0 {
		return nil, nil
	}
	out := make([]LeafPageLogSegment, 0, len(l.createdSegments))
	for _, seg := range l.createdSegments {
		out = append(out, LeafPageLogSegment{Path: seg.path, FileID: seg.fileID})
	}
	return out, nil
}

func (l *createdThenCurrentLeafPageLog) CurrentLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error) {
	if l == nil {
		return nil, nil
	}
	if len(l.currentPaths) > 0 {
		out := make([]LeafPageLogSegment, 0, len(l.currentPaths))
		for i := range l.currentPaths {
			if i < len(l.currentFileIDs) && l.currentPaths[i] != "" && l.currentFileIDs[i] != 0 {
				out = append(out, LeafPageLogSegment{Path: l.currentPaths[i], FileID: l.currentFileIDs[i]})
			}
		}
		return out, nil
	}
	if l.currentPath == "" || l.currentFileID == 0 {
		return nil, nil
	}
	return []LeafPageLogSegment{{Path: l.currentPath, FileID: l.currentFileID}}, nil
}

func (l *createdThenCurrentLeafPageLog) openSegment(seq uint32) (*valuelog.Writer, string, uint32, error) {
	if l == nil || l.dir == "" {
		return nil, "", 0, errors.New("leaf log dir unavailable")
	}
	fileID, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, seq)
	if err != nil {
		return nil, "", 0, err
	}
	path := filepath.Join(LeafLogDirPath(l.dir), fmt.Sprintf("value-l%d-%06d.log", rewriteLeafLogLaneID, seq))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, "", 0, err
	}
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		return nil, "", 0, err
	}
	l.writers = append(l.writers, w)
	return w, path, fileID, nil
}

func (l *createdThenCurrentLeafPageLog) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	if l == nil {
		return page.LeafLogPtr{}, errors.New("leaf log unavailable")
	}
	var w *valuelog.Writer
	if l.firstFileID == 0 {
		var path string
		var err error
		w, path, l.firstFileID, err = l.openSegment(1)
		if err != nil {
			return page.LeafLogPtr{}, err
		}
		l.createdSegments = append(l.createdSegments, rewriteCreatedSegment{path: path, fileID: l.firstFileID})
		currentW, currentPath, currentFileID, err := l.openSegment(2)
		if err != nil {
			return page.LeafLogPtr{}, err
		}
		l.currentPath = currentPath
		l.currentFileID = currentFileID
		_ = currentW
	} else if len(l.writers) > 0 {
		w = l.writers[len(l.writers)-1]
	}
	if w == nil {
		return page.LeafLogPtr{}, errors.New("leaf log writer unavailable")
	}
	l.nextRID++
	ptr, err := w.Append(0, nil, l.nextRID, leafPage)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	return page.LeafLogPtrFromValuePtr(ptr)
}

func (l *createdThenCurrentLeafPageLog) Flush() error {
	if l == nil {
		return nil
	}
	for _, w := range l.writers {
		if w == nil {
			continue
		}
		if err := w.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (l *createdThenCurrentLeafPageLog) Sync() error {
	if l == nil {
		return nil
	}
	for _, w := range l.writers {
		if w == nil {
			continue
		}
		if err := w.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (l *createdThenCurrentLeafPageLog) Close() error {
	if l == nil {
		return nil
	}
	var firstErr error
	for _, w := range l.writers {
		if w == nil {
			continue
		}
		if err := w.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	l.writers = nil
	return firstErr
}

func (l *createdThenCurrentLeafPageLog) CurrentValueLogSegment() (string, uint32, bool) {
	if l == nil || l.currentPath == "" || l.currentFileID == 0 {
		return "", 0, false
	}
	return l.currentPath, l.currentFileID, true
}

func (l *createdThenCurrentLeafPageLog) createdSegmentsSnapshot() ([]rewriteCreatedSegment, error) {
	if l == nil || len(l.createdSegments) == 0 {
		return nil, nil
	}
	if err := l.Flush(); err != nil {
		return nil, err
	}
	return append([]rewriteCreatedSegment(nil), l.createdSegments...), nil
}

type observerLeafPageLog struct {
	*createdThenCurrentLeafPageLog
	registered []LeafPageLogSegment
}

func (l *observerLeafPageLog) MarkLeafPageLogSegmentsRegistered(segments []LeafPageLogSegment) {
	l.registered = append(l.registered, segments...)
	l.reported = append(l.reported[:0], segments...)
}

// unregisteredLeafPageLog intentionally does not implement producer reporting
// and does not register its segment with the manager. Publication must fail
// closed instead of discovering this path.
type unregisteredLeafPageLog struct {
	dir     string
	path    string
	w       *valuelog.Writer
	fileID  uint32
	nextRID uint64
}

func (l *unregisteredLeafPageLog) ensureWriter() error {
	if l.w != nil {
		return nil
	}
	fileID, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 1)
	if err != nil {
		return err
	}
	path := filepath.Join(LeafLogDirPath(l.dir), fmt.Sprintf("value-l%d-000001.log", rewriteLeafLogLaneID))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		return err
	}
	l.w = w
	l.fileID = fileID
	l.path = path
	return nil
}

func (l *unregisteredLeafPageLog) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	if err := l.ensureWriter(); err != nil {
		return page.LeafLogPtr{}, err
	}
	l.nextRID++
	ptr, err := l.w.Append(0, nil, l.nextRID, leafPage)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	return page.LeafLogPtrFromValuePtr(ptr)
}

func (l *unregisteredLeafPageLog) Flush() error {
	if err := l.ensureWriter(); err != nil {
		return err
	}
	return l.w.Flush()
}

func (l *unregisteredLeafPageLog) Sync() error {
	if err := l.ensureWriter(); err != nil {
		return err
	}
	return l.w.Sync()
}

func (l *unregisteredLeafPageLog) Close() error {
	if l.w == nil {
		return nil
	}
	err := l.w.Close()
	l.w = nil
	return err
}

type multiReportedLeafPageLog struct {
	appendSegment   LeafPageLogSegment
	currentSegments []LeafPageLogSegment
	createdSegments []LeafPageLogSegment
	registeredCalls [][]LeafPageLogSegment
	writer          *valuelog.Writer
	nextRID         uint64
}

func newMultiReportedLeafPageLog(t *testing.T, dir string) *multiReportedLeafPageLog {
	t.Helper()
	leafDir := LeafLogDirPath(dir)
	appendSegment, writer := openLeafPageLogTestWriter(t, leafDir, rewriteLeafLogLaneID, 1)
	currentPath, currentFileID := createLeafGenerationTestSegment(t, leafDir, rewriteLeafLogLaneID, 2)
	createdPath1, createdFileID1 := createLeafGenerationTestSegment(t, leafDir, rewriteLeafLogLaneID, 3)
	createdPath2, createdFileID2 := createLeafGenerationTestSegment(t, leafDir, rewriteLeafLogLaneID, 4)
	return &multiReportedLeafPageLog{
		appendSegment: appendSegment,
		currentSegments: []LeafPageLogSegment{
			appendSegment,
			{Path: currentPath, FileID: currentFileID},
		},
		createdSegments: []LeafPageLogSegment{
			appendSegment,
			{Path: createdPath1, FileID: createdFileID1},
			{Path: createdPath2, FileID: createdFileID2},
		},
		writer: writer,
	}
}

func openLeafPageLogTestWriter(t *testing.T, leafDir string, lane, seq uint32) (LeafPageLogSegment, *valuelog.Writer) {
	t.Helper()
	fileID, err := valuelog.EncodeFileID(lane, seq)
	if err != nil {
		t.Fatalf("EncodeFileID(%d,%d): %v", lane, seq, err)
	}
	path := filepath.Join(leafDir, fmt.Sprintf("value-l%d-%06d.log", lane, seq))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", path, err)
	}
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter(%q): %v", path, err)
	}
	return LeafPageLogSegment{Path: path, FileID: fileID}, writer
}

func (l *multiReportedLeafPageLog) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	if l == nil || l.writer == nil {
		return page.LeafLogPtr{}, errors.New("leaf log writer unavailable")
	}
	l.nextRID++
	ptr, err := l.writer.Append(0, nil, l.nextRID, leafPage)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	return page.LeafLogPtrFromValuePtr(ptr)
}

func (l *multiReportedLeafPageLog) Flush() error {
	if l == nil || l.writer == nil {
		return nil
	}
	return l.writer.Flush()
}

func (l *multiReportedLeafPageLog) Sync() error {
	if l == nil || l.writer == nil {
		return nil
	}
	return l.writer.Sync()
}

func (l *multiReportedLeafPageLog) Close() error {
	if l == nil || l.writer == nil {
		return nil
	}
	err := l.writer.Close()
	l.writer = nil
	return err
}

func (l *multiReportedLeafPageLog) CurrentValueLogSegment() (string, uint32, bool) {
	if l == nil || len(l.currentSegments) == 0 {
		return "", 0, false
	}
	seg := l.currentSegments[0]
	if seg.Path == "" || seg.FileID == 0 {
		return "", 0, false
	}
	return seg.Path, seg.FileID, true
}

func (l *multiReportedLeafPageLog) CurrentLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error) {
	if l == nil || len(l.currentSegments) == 0 {
		return nil, nil
	}
	return append([]LeafPageLogSegment(nil), l.currentSegments...), nil
}

func (l *multiReportedLeafPageLog) CreatedLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error) {
	if l == nil || len(l.createdSegments) == 0 {
		return nil, nil
	}
	return append([]LeafPageLogSegment(nil), l.createdSegments...), nil
}

func (l *multiReportedLeafPageLog) MarkLeafPageLogSegmentsRegistered(segments []LeafPageLogSegment) {
	if l == nil || len(segments) == 0 {
		return
	}
	copied := append([]LeafPageLogSegment(nil), segments...)
	l.registeredCalls = append(l.registeredCalls, copied)
	registered := make(map[uint32]struct{}, len(segments))
	for _, seg := range segments {
		if seg.FileID == 0 {
			continue
		}
		registered[seg.FileID] = struct{}{}
	}
	if len(registered) == 0 {
		return
	}
	filtered := l.createdSegments[:0]
	for _, seg := range l.createdSegments {
		if _, ok := registered[seg.FileID]; ok {
			continue
		}
		filtered = append(filtered, seg)
	}
	l.createdSegments = filtered
}

func uniqueLeafPageLogSegmentsForTest(groups ...[]LeafPageLogSegment) []LeafPageLogSegment {
	seen := make(map[uint32]struct{})
	out := make([]LeafPageLogSegment, 0)
	for _, group := range groups {
		for _, seg := range group {
			if seg.Path == "" || seg.FileID == 0 {
				continue
			}
			if _, ok := seen[seg.FileID]; ok {
				continue
			}
			seen[seg.FileID] = struct{}{}
			out = append(out, seg)
		}
	}
	return out
}

func leafPageLogSegmentFileIDsForTest(segments []LeafPageLogSegment) []uint32 {
	out := make([]uint32, 0, len(segments))
	for _, seg := range segments {
		if seg.FileID == 0 {
			continue
		}
		out = append(out, seg.FileID)
	}
	return out
}

func TestAppendOrderedRootDeltaBatchFinalTouchedValueLogSegmentsDedupesWithSeed(t *testing.T) {
	delta := batch.New(nil, 1024)
	defer delta.Close()

	fileA := page.ValueLogFileID(10)
	fileB := page.ValueLogFileID(11)
	if err := delta.SetPointer([]byte("a"), page.ValuePtr{FileID: fileA, Offset: 1, Length: 1}); err != nil {
		t.Fatalf("SetPointer a: %v", err)
	}
	if err := delta.SetPointer([]byte("b"), page.ValuePtr{FileID: fileB, Offset: 2, Length: 1}); err != nil {
		t.Fatalf("SetPointer b: %v", err)
	}
	if err := delta.SetPointer([]byte("c"), page.ValuePtr{FileID: fileA, Offset: 3, Length: 1}); err != nil {
		t.Fatalf("SetPointer c: %v", err)
	}
	if err := delta.Set([]byte("inline"), []byte("value")); err != nil {
		t.Fatalf("Set inline: %v", err)
	}

	got := appendOrderedRootDeltaBatchFinalTouchedValueLogSegments(delta, []uint32{fileB})
	want := []uint32{fileB, fileA}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("touched segments=%v want %v", got, want)
	}
}

func TestOrderedRootTouchedIteratorDedupesWithSetAfterLinearLimit(t *testing.T) {
	var it orderedRootTouchedIterator
	first := page.ValueLogFileID(20)
	it.appendTouchedValueLogSegmentID(first)
	it.appendTouchedValueLogSegmentID(first)
	for i := 0; i < orderedRootTouchedValueLogSegmentLinearLimit+2; i++ {
		it.appendTouchedValueLogSegmentID(page.ValueLogFileID(uint32(30 + i)))
	}
	duplicateAfterSet := page.ValueLogFileID(32)
	it.appendTouchedValueLogSegmentID(duplicateAfterSet)
	if it.touchedValueLogSegmentSet == nil {
		t.Fatal("expected touched segment set after linear limit")
	}

	want := []uint32{first}
	for i := 0; i < orderedRootTouchedValueLogSegmentLinearLimit+2; i++ {
		want = append(want, page.ValueLogFileID(uint32(30+i)))
	}
	got := it.touchedValueLogSegments
	if len(got) != len(want) {
		t.Fatalf("touched segments len=%d want %d: got=%v want=%v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("touched segment[%d]=%d want %d (got=%v want=%v)", i, got[i], want[i], got, want)
		}
	}
}

func TestInlineCommitSkipsValueLogRefresh(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "value_vlog", "value-l0-000001.log")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte("x"), 128)); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	// Inline write should publish a new state without refreshing value-log files.
	if err := d.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set inline: %v", err)
	}
	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[fileID]; ok {
		t.Fatalf("inline commit unexpectedly refreshed value-log set with segment %d", fileID)
	}

	// Explicit refresh should discover the new segment.
	if err := d.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}
	st2 := d.State()
	if st2 == nil || st2.ValueLogSet == nil {
		t.Fatalf("state missing value-log set after refresh")
	}
	if _, ok := st2.ValueLogSet.Files[fileID]; !ok {
		t.Fatalf("RefreshValueLogSet did not discover segment %d", fileID)
	}
}

func TestPointerCommitPublishesProducerRegisteredSegmentWithoutRefresh(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "value_vlog", "value-l0-000001.log")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	value := bytes.Repeat([]byte("p"), 128)
	ptr, err := w.Append(0, nil, 1, value)
	if err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	registerTestValueLogProducer(t, dir, path, fileID)
	before := d.valueLogManager.RefreshScanCount()

	b := d.NewBatch().(*Batch)
	defer func() { _ = b.Close() }()
	if err := b.SetPointer([]byte("kp"), ptr); err != nil {
		t.Fatalf("SetPointer: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if after := d.valueLogManager.RefreshScanCount(); after != before {
		t.Fatalf("pointer commit triggered value-log refresh scan: before=%d after=%d", before, after)
	}

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[fileID]; !ok {
		t.Fatalf("pointer commit did not publish producer-registered segment %d", fileID)
	}

	got, err := d.Get([]byte("kp"))
	if err != nil {
		t.Fatalf("Get pointer value: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("Get pointer value mismatch: got %d bytes, want %d", len(got), len(value))
	}
}

func TestPublishSystemRootIterator_PointerPublishesProducerRegisteredSegmentWithoutRefreshScan(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if d.valueLogManager == nil {
		t.Fatalf("missing value log manager")
	}
	value := bytes.Repeat([]byte("p"), 128)
	fileID, ptr := writeValueLogRecord(t, dir, 0, 1, value, 1)
	before := d.valueLogManager.RefreshScanCount()

	if _, err := d.PublishSystemRootIterator(mustFrozenSystemPointerMemtable(t, "sys/p", ptr).NewIterator(nil, nil)); err != nil {
		t.Fatalf("PublishSystemRootIterator: %v", err)
	}

	after := d.valueLogManager.RefreshScanCount()
	if after != before {
		t.Fatalf("system root pointer publish used a directory refresh instead of bounded registration: before=%d after=%d", before, after)
	}

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[fileID]; !ok {
		t.Fatalf("system root publish did not include producer-registered segment %d", fileID)
	}
}

func TestPublishSystemRootIterator_PointerSkipsValueLogRefreshWhenSegmentAlreadyRegistered(t *testing.T) {
	dir := t.TempDir()
	value := bytes.Repeat([]byte("p"), 128)
	fileID, ptr := writeValueLogRecord(t, dir, 0, 1, value, 1)

	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if d.valueLogManager == nil {
		t.Fatalf("missing value log manager")
	}
	path := filepath.Join(dir, "value_vlog", "value-l0-000001.log")
	if err := d.RegisterValueLogSegment(path, fileID); err != nil {
		t.Fatalf("RegisterValueLogSegment: %v", err)
	}
	before := d.valueLogManager.RefreshScanCount()

	if _, err := d.PublishSystemRootIterator(mustFrozenSystemPointerMemtable(t, "sys/p", ptr).NewIterator(nil, nil)); err != nil {
		t.Fatalf("PublishSystemRootIterator: %v", err)
	}

	after := d.valueLogManager.RefreshScanCount()
	if after != before {
		t.Fatalf("system root pointer publish triggered value-log refresh scan: before=%d after=%d", before, after)
	}

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[fileID]; !ok {
		t.Fatalf("system root publish missing pre-registered segment %d", fileID)
	}
}

func TestPublishOrderedRootDeltaGroupWithSystemBuilder_NonSystemPointerUsesProducerRegistrationWithoutRefresh(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if d.valueLogManager == nil {
		t.Fatalf("missing value log manager")
	}
	value := bytes.Repeat([]byte("p"), 128)
	fileID, ptr := writeValueLogRecord(t, dir, 0, 1, value, 1)
	before := d.valueLogManager.RefreshScanCount()

	_, rootIDs, err := d.PublishOrderedRootDeltaGroupWithSystemBuilder([]OrderedRootDeltaPublishInput{{
		BaseRoot: 0,
		Iter:     mustFrozenSystemPointerMemtable(t, "root/p", ptr).NewIterator(nil, nil),
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 {
			t.Fatalf("rootIDs len=%d want 1", len(rootIDs))
		}
		return mustFrozenSystemMemtable(t, "sys/root", fmt.Sprintf("%d", rootIDs[0])).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("PublishOrderedRootDeltaGroupWithSystemBuilder: %v", err)
	}
	if len(rootIDs) != 1 {
		t.Fatalf("rootIDs len=%d want 1", len(rootIDs))
	}

	after := d.valueLogManager.RefreshScanCount()
	if after != before {
		t.Fatalf("ordered root group pointer publish triggered value-log refresh scan: before=%d after=%d", before, after)
	}

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[fileID]; !ok {
		t.Fatalf("ordered root group publish missing producer-registered segment %d", fileID)
	}
}

func TestPublishOrderedRootDeltaGroupWithSystemBuilder_NonSystemPointerSkipsRefreshWhenSegmentRegistered(t *testing.T) {
	dir := t.TempDir()
	value := bytes.Repeat([]byte("p"), 128)
	fileID, ptr := writeValueLogRecord(t, dir, 0, 1, value, 1)

	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if d.valueLogManager == nil {
		t.Fatalf("missing value log manager")
	}
	path := filepath.Join(dir, "value_vlog", "value-l0-000001.log")
	if err := d.RegisterValueLogSegment(path, fileID); err != nil {
		t.Fatalf("RegisterValueLogSegment: %v", err)
	}
	before := d.valueLogManager.RefreshScanCount()

	_, rootIDs, err := d.PublishOrderedRootDeltaGroupWithSystemBuilder([]OrderedRootDeltaPublishInput{{
		BaseRoot: 0,
		Iter:     mustFrozenSystemPointerMemtable(t, "root/p", ptr).NewIterator(nil, nil),
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 {
			t.Fatalf("rootIDs len=%d want 1", len(rootIDs))
		}
		return mustFrozenSystemMemtable(t, "sys/root", fmt.Sprintf("%d", rootIDs[0])).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("PublishOrderedRootDeltaGroupWithSystemBuilder: %v", err)
	}
	if len(rootIDs) != 1 {
		t.Fatalf("rootIDs len=%d want 1", len(rootIDs))
	}

	after := d.valueLogManager.RefreshScanCount()
	if after != before {
		t.Fatalf("ordered root group pointer publish triggered value-log refresh scan: before=%d after=%d", before, after)
	}

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[fileID]; !ok {
		t.Fatalf("ordered root group publish missing pre-registered segment %d", fileID)
	}
}

func TestPublishOrderedRootGroupWithSystemBuilder_NonSystemPointerUsesProducerRegistrationWithoutRefresh(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if d.valueLogManager == nil {
		t.Fatalf("missing value log manager")
	}
	value := bytes.Repeat([]byte("p"), 128)
	fileID, ptr := writeValueLogRecord(t, dir, 0, 1, value, 1)
	before := d.valueLogManager.RefreshScanCount()

	_, rootIDs, err := d.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
		BaseRoot: 0,
		Iter:     mustFrozenSystemPointerMemtable(t, "root/p", ptr).NewIterator(nil, nil),
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 {
			t.Fatalf("rootIDs len=%d want 1", len(rootIDs))
		}
		return mustFrozenSystemMemtable(t, "sys/root", fmt.Sprintf("%d", rootIDs[0])).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("PublishOrderedRootGroupWithSystemBuilder: %v", err)
	}
	if len(rootIDs) != 1 {
		t.Fatalf("rootIDs len=%d want 1", len(rootIDs))
	}

	after := d.valueLogManager.RefreshScanCount()
	if after != before {
		t.Fatalf("ordered root full group pointer publish triggered value-log refresh scan: before=%d after=%d", before, after)
	}

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[fileID]; !ok {
		t.Fatalf("ordered root full group publish missing producer-registered segment %d", fileID)
	}
}

func TestPublishOrderedRootGroupWithSystemBuilder_NonSystemPointerSkipsRefreshWhenSegmentRegistered(t *testing.T) {
	dir := t.TempDir()
	value := bytes.Repeat([]byte("p"), 128)
	fileID, ptr := writeValueLogRecord(t, dir, 0, 1, value, 1)

	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if d.valueLogManager == nil {
		t.Fatalf("missing value log manager")
	}
	path := filepath.Join(dir, "value_vlog", "value-l0-000001.log")
	if err := d.RegisterValueLogSegment(path, fileID); err != nil {
		t.Fatalf("RegisterValueLogSegment: %v", err)
	}
	before := d.valueLogManager.RefreshScanCount()

	_, rootIDs, err := d.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
		BaseRoot: 0,
		Iter:     mustFrozenSystemPointerMemtable(t, "root/p", ptr).NewIterator(nil, nil),
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 {
			t.Fatalf("rootIDs len=%d want 1", len(rootIDs))
		}
		return mustFrozenSystemMemtable(t, "sys/root", fmt.Sprintf("%d", rootIDs[0])).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("PublishOrderedRootGroupWithSystemBuilder: %v", err)
	}
	if len(rootIDs) != 1 {
		t.Fatalf("rootIDs len=%d want 1", len(rootIDs))
	}

	after := d.valueLogManager.RefreshScanCount()
	if after != before {
		t.Fatalf("ordered root full group pointer publish triggered value-log refresh scan: before=%d after=%d", before, after)
	}

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[fileID]; !ok {
		t.Fatalf("ordered root full group publish missing pre-registered segment %d", fileID)
	}
}

func TestPointerCommitSkipsValueLogRefreshWhenSegmentAlreadyRegistered(t *testing.T) {
	dir := t.TempDir()
	value := bytes.Repeat([]byte("p"), 256)
	fileID, ptr := writeValueLogRecord(t, dir, 0, 1, value, 1)

	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if d.valueLogManager == nil {
		t.Fatalf("missing value log manager")
	}
	before := d.valueLogManager.RefreshScanCount()

	b := d.NewBatch().(*Batch)
	defer func() { _ = b.Close() }()
	if err := b.SetPointer([]byte("kp"), ptr); err != nil {
		t.Fatalf("SetPointer: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	after := d.valueLogManager.RefreshScanCount()
	if after != before {
		t.Fatalf("pointer commit triggered value-log refresh scan: before=%d after=%d", before, after)
	}

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[fileID]; !ok {
		t.Fatalf("published state missing pre-registered segment %d", fileID)
	}

	got, err := d.Get([]byte("kp"))
	if err != nil {
		t.Fatalf("Get pointer value: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("Get pointer value mismatch: got %d bytes, want %d", len(got), len(value))
	}
}

func TestOuterLeafCommitPublishesRegisteredSegmentWithoutExplicitRefresh(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	leafLog := &registeredLeafPageLog{db: d, dir: dir}
	defer func() { _ = leafLog.Close() }()
	d.SetLeafPageLog(leafLog)
	refreshBefore := d.valueLogManager.RefreshScanCount()

	if err := d.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	refreshAfter := d.valueLogManager.RefreshScanCount()
	if refreshAfter != refreshBefore {
		t.Fatalf("outer-leaf commit triggered value-log refresh scan: before=%d after=%d", refreshBefore, refreshAfter)
	}

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[leafLog.fileID]; !ok {
		t.Fatalf("registered outer-leaf segment %d missing from published state", leafLog.fileID)
	}

	got, err := d.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("Get mismatch: got %q want %q", got, "v")
	}
}

func TestOuterLeafCommitPublishesCreatedSegmentBeforeCurrentWithoutRefresh(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	leafLog := &createdThenCurrentLeafPageLog{dir: dir}
	defer func() { _ = leafLog.Close() }()
	d.SetLeafPageLog(leafLog)
	refreshBefore := d.valueLogManager.RefreshScanCount()

	if err := d.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if leafLog.firstFileID == 0 {
		t.Fatal("leaf log did not create the first segment")
	}
	if leafLog.currentFileID == 0 || leafLog.currentFileID == leafLog.firstFileID {
		t.Fatalf("leaf log current segment=%d, first=%d", leafLog.currentFileID, leafLog.firstFileID)
	}
	refreshAfter := d.valueLogManager.RefreshScanCount()
	if refreshAfter != refreshBefore {
		t.Fatalf("outer-leaf commit triggered value-log refresh scan: before=%d after=%d", refreshBefore, refreshAfter)
	}

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[leafLog.firstFileID]; !ok {
		t.Fatalf("created outer-leaf segment %d missing from published state", leafLog.firstFileID)
	}
	if _, ok := st.ValueLogSet.Files[leafLog.currentFileID]; !ok {
		t.Fatalf("current outer-leaf segment %d missing from published state", leafLog.currentFileID)
	}

	got, err := d.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("Get mismatch: got %q want %q", got, "v")
	}
}

func TestRegisterLeafPageLogSegmentsForPublish_MultipleReportedSegments(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	leafLog := newMultiReportedLeafPageLog(t, dir)
	defer func() { _ = leafLog.Close() }()
	d.SetLeafPageLog(leafLog)
	refreshBefore := d.valueLogManager.RefreshScanCount()
	wantCreated := append([]LeafPageLogSegment(nil), leafLog.createdSegments...)
	wantCurrent := append([]LeafPageLogSegment(nil), leafLog.currentSegments...)
	wantUnique := uniqueLeafPageLogSegmentsForTest(wantCreated, wantCurrent)

	registered, err := d.registerLeafPageLogSegmentsForPublish()
	if err != nil {
		t.Fatalf("registerLeafPageLogSegmentsForPublish: %v", err)
	}
	if !registered {
		t.Fatal("expected reported leaf segments to register")
	}
	if got, want := d.valueLogManager.RefreshScanCount(), refreshBefore; got != want {
		t.Fatalf("registerLeafPageLogSegmentsForPublish triggered refresh scan: before=%d after=%d", want, got)
	}
	if got, want := len(leafLog.registeredCalls), 1; got != want {
		t.Fatalf("registration observer calls=%d want %d", got, want)
	}
	if !reflect.DeepEqual(leafLog.registeredCalls[0], wantCreated) {
		t.Fatalf("registered created segments=%v want %v", leafLog.registeredCalls[0], wantCreated)
	}
	if got, want := len(leafLog.createdSegments), 0; got != want {
		t.Fatalf("created segments remaining=%d want %d", got, want)
	}
	set := d.valueLogManager.CurrentSetNoRefresh()
	defer func() {
		if set != nil {
			_ = d.valueLogManager.Release(set)
		}
	}()
	if set == nil {
		t.Fatal("missing value-log set after registration")
	}
	for _, seg := range wantUnique {
		if _, ok := set.Files[seg.FileID]; !ok {
			t.Fatalf("value-log set missing segment %d (%s)", seg.FileID, filepath.Base(seg.Path))
		}
		if !d.isLeafGenerationSegmentPath(seg.Path) {
			t.Fatalf("segment path not classified as leaf_vlog: %s", seg.Path)
		}
	}
	wantCurrentIDs := leafPageLogSegmentFileIDsForTest(wantCurrent)
	sort.Slice(wantCurrentIDs, func(i, j int) bool { return wantCurrentIDs[i] < wantCurrentIDs[j] })
	if got := d.valueLogManager.CurrentWritableFileIDs(); !reflect.DeepEqual(got, wantCurrentIDs) {
		t.Fatalf("current writable file ids=%v want %v", got, wantCurrentIDs)
	}
	wantPending := make([]uint32, 0, len(wantUnique))
	for _, seg := range wantUnique {
		rawFileID, ok := rawLeafGenerationFileID(seg.FileID)
		if !ok {
			t.Fatalf("raw leaf generation file id missing for segment %d", seg.FileID)
		}
		wantPending = append(wantPending, rawFileID)
	}
	if got := append([]uint32(nil), d.leafGenerationPendingFileIDs...); !reflect.DeepEqual(got, wantPending) {
		t.Fatalf("pending leaf generation file ids=%v want %v", got, wantPending)
	}
}

func TestOuterLeafCommitPublishesMultipleReportedSegmentsWithoutRefreshAndReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	leafLog := newMultiReportedLeafPageLog(t, dir)
	d.SetLeafPageLog(leafLog)
	refreshBefore := d.valueLogManager.RefreshScanCount()
	wantCreated := append([]LeafPageLogSegment(nil), leafLog.createdSegments...)
	wantCurrent := append([]LeafPageLogSegment(nil), leafLog.currentSegments...)
	wantUnique := uniqueLeafPageLogSegmentsForTest(wantCreated, wantCurrent)

	if err := d.Set([]byte("k"), []byte("v")); err != nil {
		_ = leafLog.Close()
		_ = d.Close()
		t.Fatalf("Set: %v", err)
	}
	refreshAfter := d.valueLogManager.RefreshScanCount()
	if refreshAfter != refreshBefore {
		_ = leafLog.Close()
		_ = d.Close()
		t.Fatalf("outer-leaf commit triggered value-log refresh scan: before=%d after=%d", refreshBefore, refreshAfter)
	}
	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		_ = leafLog.Close()
		_ = d.Close()
		t.Fatalf("state missing value-log set")
	}
	for _, seg := range wantUnique {
		if _, ok := st.ValueLogSet.Files[seg.FileID]; !ok {
			_ = leafLog.Close()
			_ = d.Close()
			t.Fatalf("published state missing segment %d (%s)", seg.FileID, filepath.Base(seg.Path))
		}
	}
	if got, want := len(leafLog.registeredCalls), 1; got != want {
		_ = leafLog.Close()
		_ = d.Close()
		t.Fatalf("registration observer calls=%d want %d", got, want)
	}
	if !reflect.DeepEqual(leafLog.registeredCalls[0], wantCreated) {
		_ = leafLog.Close()
		_ = d.Close()
		t.Fatalf("registered created segments=%v want %v", leafLog.registeredCalls[0], wantCreated)
	}
	if got, want := len(leafLog.createdSegments), 0; got != want {
		_ = leafLog.Close()
		_ = d.Close()
		t.Fatalf("created segments remaining=%d want %d", got, want)
	}
	if err := leafLog.Close(); err != nil {
		_ = d.Close()
		t.Fatalf("Close leaf log: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("reopen Open: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	got, err := reopened.Get([]byte("k"))
	if err != nil {
		t.Fatalf("reopened Get: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("reopened Get mismatch: got %q want %q", got, "v")
	}
}

func TestOuterLeafCommitFailsClosedWhenReportedSegmentCannotRegister(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	leafLog := newMultiReportedLeafPageLog(t, dir)
	defer func() { _ = leafLog.Close() }()
	d.SetLeafPageLog(leafLog)
	beforeState := d.State()
	if beforeState == nil {
		t.Fatal("missing state before failed commit")
	}
	before := d.valueLogManager.RefreshScanCount()
	missingFileID, err := valuelog.EncodeFileID(246, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	leafLog.createdSegments[0] = LeafPageLogSegment{
		Path:   filepath.Join(LeafLogDirPath(dir), "value-l246-000001.log"),
		FileID: missingFileID,
	}

	if err := d.Set([]byte("k"), []byte("v")); err == nil {
		t.Fatal("expected Set to fail when a reported leaf segment cannot register")
	}
	if got, want := d.valueLogManager.RefreshScanCount(), before; got != want {
		t.Fatalf("failed commit triggered refresh scan: before=%d after=%d", want, got)
	}
	if got, want := len(leafLog.registeredCalls), 0; got != want {
		t.Fatalf("registration observer calls=%d want %d", got, want)
	}
	if d.valueLogManager.HasSegment(leafLog.appendSegment.FileID) {
		t.Fatalf("unexpected registration of append segment %d after failure", leafLog.appendSegment.FileID)
	}
	st := d.State()
	if st == nil || st.CommitSeq != beforeState.CommitSeq ||
		st.RootPageID != beforeState.RootPageID ||
		st.SystemRootPageID != beforeState.SystemRootPageID ||
		st.AppliedCommandLSN != beforeState.AppliedCommandLSN ||
		st.MaxEntryRevision != beforeState.MaxEntryRevision ||
		st.LeafGenerationStateVersion != beforeState.LeafGenerationStateVersion {
		t.Fatalf("state changed after failed commit: %+v", st)
	}
	if st != nil && st.ValueLogSet != nil {
		if _, ok := st.ValueLogSet.Files[leafLog.appendSegment.FileID]; ok {
			t.Fatalf("published state unexpectedly includes failed append segment %d", leafLog.appendSegment.FileID)
		}
	}
}

func TestOuterLeafWriteLoopSkipsForcedValueLogRefreshScans(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{
		Dir:                        dir,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if d.valueLogManager == nil {
		t.Fatalf("missing value log manager")
	}
	leafLog := &registeredLeafPageLog{db: d, dir: dir}
	defer func() { _ = leafLog.Close() }()
	d.SetLeafPageLog(leafLog)
	before := d.valueLogManager.RefreshScanCount()

	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("k-%06d", i))
		val := []byte(fmt.Sprintf("v-%06d", i))
		if err := d.Set(key, val); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}

	after := d.valueLogManager.RefreshScanCount()
	if after != before {
		t.Fatalf("outer-leaf write loop triggered value-log refresh scans: before=%d after=%d", before, after)
	}
}

func TestOuterLeafPointerCommitFailsClosedForUnreportedSegmentWithoutRefreshScan(t *testing.T) {
	dir := t.TempDir()
	value := bytes.Repeat([]byte("p"), 256)
	fileID, ptr := writeValueLogRecord(t, dir, 0, 1, value, 1)

	d, err := Open(Options{
		Dir:                        dir,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if d.valueLogManager == nil {
		t.Fatalf("missing value log manager")
	}
	leafLog := &unregisteredLeafPageLog{dir: dir}
	defer func() { _ = leafLog.Close() }()
	d.SetLeafPageLog(leafLog)

	// Keep touched pointer segments known so the only unresolved dependency is
	// the outer-leaf segment deliberately omitted by its producer.
	pointerPath := filepath.Join(dir, "value_vlog", "value-l0-000001.log")
	if err := d.RegisterValueLogSegment(pointerPath, fileID); err != nil {
		t.Fatalf("RegisterValueLogSegment(pointer): %v", err)
	}
	before := d.valueLogManager.RefreshScanCount()

	b := d.NewBatch().(*Batch)
	defer func() { _ = b.Close() }()
	if err := b.SetPointer([]byte("kp"), ptr); err != nil {
		t.Fatalf("SetPointer: %v", err)
	}
	if err := b.Write(); !errors.Is(err, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("Write error=%v want unresolved resource", err)
	}

	after := d.valueLogManager.RefreshScanCount()
	if after != before {
		t.Fatalf("unreported leaf segment triggered a directory refresh: before=%d after=%d", before, after)
	}
	if leafLog.fileID == 0 {
		t.Fatalf("leaf log did not create a segment")
	}

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[leafLog.fileID]; ok {
		t.Fatalf("rejected publish exposed unreported leaf segment %d", leafLog.fileID)
	}

	if got, err := d.Get([]byte("kp")); err == nil && bytes.Equal(got, value) {
		t.Fatalf("rejected pointer became visible: %d bytes", len(got))
	}
}

func BenchmarkOuterLeafWriteLoop_NoRefresh(b *testing.B) {
	benchmarkOuterLeafWriteLoop(b, false)
}

func BenchmarkOuterLeafWriteLoop_ForcedRefresh(b *testing.B) {
	benchmarkOuterLeafWriteLoop(b, true)
}

func benchmarkOuterLeafWriteLoop(b *testing.B, forceRefresh bool) {
	const writesPerIter = 2000
	var totalRefreshScans uint64

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		d, err := Open(Options{
			Dir:                        dir,
			IndexOuterLeavesInValueLog: true,
		})
		if err != nil {
			b.Fatalf("Open: %v", err)
		}
		leafLog := &registeredLeafPageLog{db: d, dir: dir}
		d.SetLeafPageLog(leafLog)
		refreshBefore := d.valueLogManager.RefreshScanCount()
		b.StartTimer()

		for j := 0; j < writesPerIter; j++ {
			key := []byte(fmt.Sprintf("k-%06d-%06d", i, j))
			val := []byte(fmt.Sprintf("v-%06d-%06d", i, j))
			if err := d.Set(key, val); err != nil {
				b.Fatalf("Set %d: %v", j, err)
			}
			if forceRefresh {
				if err := d.RefreshValueLogSet(); err != nil {
					b.Fatalf("RefreshValueLogSet: %v", err)
				}
			}
		}

		b.StopTimer()
		totalRefreshScans += d.valueLogManager.RefreshScanCount() - refreshBefore
		_ = leafLog.Close()
		_ = d.Close()
	}

	if b.N > 0 {
		b.ReportMetric(float64(writesPerIter), "writes/iter")
		b.ReportMetric(float64(totalRefreshScans)/float64(b.N), "refresh_scans/iter")
	}
}

func TestRegisterValueLogSegment_DoesNotPublishCurrentSetWithoutExplicitRefresh(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing initial value-log set")
	}

	fileID, err := valuelog.EncodeFileID(7, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "value_vlog", "value-l7-000001.log")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte("x"), 256)); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	if err := d.RegisterValueLogSegment(path, fileID); err != nil {
		t.Fatalf("RegisterValueLogSegment: %v", err)
	}

	st2 := d.State()
	if st2 == nil || st2.ValueLogSet == nil {
		t.Fatalf("state missing value-log set after register")
	}
	if _, ok := st2.ValueLogSet.Files[fileID]; ok {
		t.Fatalf("registered segment %d unexpectedly published without refresh", fileID)
	}
	if err := d.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}
	st3 := d.State()
	if st3 == nil || st3.ValueLogSet == nil {
		t.Fatalf("state missing value-log set after refresh")
	}
	if _, ok := st3.ValueLogSet.Files[fileID]; !ok {
		t.Fatalf("registered segment %d missing after explicit refresh", fileID)
	}
}

func TestCurrentSetRefresh_InlineThenPointerThenInline(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	value := bytes.Repeat([]byte("p"), 256)
	fileID, ptr := writeValueLogRecord(t, dir, 0, 1, value, 1)

	b := d.NewBatch().(*Batch)
	defer func() { _ = b.Close() }()
	if err := b.Set([]byte("k"), []byte("inline-1")); err != nil {
		t.Fatalf("Set inline1: %v", err)
	}
	if err := b.SetPointer([]byte("k"), ptr); err != nil {
		t.Fatalf("SetPointer: %v", err)
	}
	if err := b.Set([]byte("k"), []byte("inline-2")); err != nil {
		t.Fatalf("Set inline2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[fileID]; !ok {
		t.Fatalf("touched segment %d was not published in CurrentSet", fileID)
	}

	got, err := d.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get inline final value: %v", err)
	}
	if !bytes.Equal(got, []byte("inline-2")) {
		t.Fatalf("Get mismatch: got %q want %q", got, "inline-2")
	}
}

func TestCurrentSetRefresh_DeleteOnlyBatch_NoFalseRefresh(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	value := bytes.Repeat([]byte("a"), 256)
	fileID1, ptr1 := writeValueLogRecord(t, dir, 0, 1, value, 1)

	b := d.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("kp"), ptr1); err != nil {
		t.Fatalf("SetPointer: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write pointer batch: %v", err)
	}
	_ = b.Close()

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[fileID1]; !ok {
		t.Fatalf("expected segment %d to be published after pointer batch", fileID1)
	}

	fileID2, _, _ := writeUnregisteredValueLogRecord(t, dir, 0, 2, value, 2)

	del := d.NewBatch().(*Batch)
	defer func() { _ = del.Close() }()
	if err := del.Delete([]byte("kp")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := del.Write(); err != nil {
		t.Fatalf("Write delete-only batch: %v", err)
	}

	st2 := d.State()
	if st2 == nil || st2.ValueLogSet == nil {
		t.Fatalf("state missing value-log set after delete-only write")
	}
	if _, ok := st2.ValueLogSet.Files[fileID2]; ok {
		t.Fatalf("delete-only batch unexpectedly refreshed CurrentSet with untouched segment %d", fileID2)
	}
}

func TestFinalizeCommitFailpoint_DoesNotRegisterCurrentLeafSegment(t *testing.T) {
	db, leafLog, _ := openLeafGenerationPackTestDB(t)
	if _, err := leafLog.AppendLeafPage(bytes.Repeat([]byte("l"), page.PageSize)); err != nil {
		t.Fatalf("AppendLeafPage: %v", err)
	}
	_, fileID := currentLeafSegmentOrFatal(t, leafLog)
	if db.valueLogManager.HasSegment(fileID) {
		t.Fatalf("expected file %d to be unregistered before failpoint", fileID)
	}

	db.testFailFinalizeCommit.Store(true)
	err := db.Set([]byte("k"), bytes.Repeat([]byte("v"), 64))
	db.testFailFinalizeCommit.Store(false)
	if !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatalf("Set failpoint err=%v, want %v", err, errTestFinalizeCommitFailpoint)
	}
	if db.valueLogManager.HasSegment(fileID) {
		t.Fatalf("expected failpoint to avoid registering leaf segment %d", fileID)
	}
}
