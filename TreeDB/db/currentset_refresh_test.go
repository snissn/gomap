package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func writeValueLogRecord(t *testing.T, dir string, lane, seq uint32, value []byte, rid uint64) (uint32, page.ValuePtr) {
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
	return fileID, ptr
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
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		return err
	}
	path := filepath.Join(l.dir, "value_vlog", "value-l0-000001.log")
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
	createdSegments []rewriteCreatedSegment
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

// unregisteredLeafPageLog intentionally does not implement
// CurrentValueLogSegment and does not register its segment with the manager.
// It is used to verify forced-refresh safety fallbacks.
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
	fileID, err := valuelog.EncodeFileID(11, 1)
	if err != nil {
		return err
	}
	path := filepath.Join(l.dir, "value_vlog", "value-l11-000001.log")
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

func TestPointerCommitRefreshesValueLogSet(t *testing.T) {
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

	b := d.NewBatch().(*Batch)
	defer func() { _ = b.Close() }()
	if err := b.SetPointer([]byte("kp"), ptr); err != nil {
		t.Fatalf("SetPointer: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[fileID]; !ok {
		t.Fatalf("pointer commit did not refresh value-log set with segment %d", fileID)
	}

	got, err := d.Get([]byte("kp"))
	if err != nil {
		t.Fatalf("Get pointer value: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("Get pointer value mismatch: got %d bytes, want %d", len(got), len(value))
	}
}

func TestPublishSystemRootIterator_PointerRefreshesValueLogSet(t *testing.T) {
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
	if after <= before {
		t.Fatalf("system root pointer publish did not refresh value-log set: before=%d after=%d", before, after)
	}

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[fileID]; !ok {
		t.Fatalf("system root publish did not refresh value-log set with segment %d", fileID)
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

func TestPublishOrderedRootDeltaGroupWithSystemBuilder_NonSystemPointerRefreshesValueLogSet(t *testing.T) {
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
	if after <= before {
		t.Fatalf("ordered root group pointer publish did not refresh value-log set: before=%d after=%d", before, after)
	}

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[fileID]; !ok {
		t.Fatalf("ordered root group publish did not refresh value-log set with segment %d", fileID)
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

func TestPublishOrderedRootGroupWithSystemBuilder_NonSystemPointerRefreshesValueLogSet(t *testing.T) {
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
	if after <= before {
		t.Fatalf("ordered root full group pointer publish did not refresh value-log set: before=%d after=%d", before, after)
	}

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[fileID]; !ok {
		t.Fatalf("ordered root full group publish did not refresh value-log set with segment %d", fileID)
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

func TestOuterLeafPointerCommitRefreshesWhenLeafSegmentUnreported(t *testing.T) {
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

	// Keep touched pointer segments known so this commit would previously skip
	// refresh and publish an incomplete ValueLogSet when the leaf segment is not
	// reportable/registered.
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
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	after := d.valueLogManager.RefreshScanCount()
	if after <= before {
		t.Fatalf("expected forced refresh fallback for unreported leaf segment: before=%d after=%d", before, after)
	}
	if leafLog.fileID == 0 {
		t.Fatalf("leaf log did not create a segment")
	}

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[leafLog.fileID]; !ok {
		t.Fatalf("published state missing unreported leaf segment %d", leafLog.fileID)
	}

	got, err := d.Get([]byte("kp"))
	if err != nil {
		t.Fatalf("Get pointer value: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("Get pointer value mismatch: got %d bytes, want %d", len(got), len(value))
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

	fileID2, _ := writeValueLogRecord(t, dir, 0, 2, value, 2)

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
