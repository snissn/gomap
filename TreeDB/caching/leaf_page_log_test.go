package caching

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCachingLeafPageLog_AppendLeafPageReturnsAppendError(t *testing.T) {
	db := &DB{}
	db.closeCh = make(chan struct{})
	close(db.closeCh)
	leaf := lane{id: leafLogLaneID}
	leafLog := &cachingLeafPageLog{db: db, lane: &leaf}
	_, err := leafLog.AppendLeafPage([]byte("leaf"))
	if !errors.Is(err, errWALClosed) {
		t.Fatalf("AppendLeafPage error=%v want %v", err, errWALClosed)
	}
}

func TestCachingLeafPageLog_SyncRespectsRelaxedSync(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(uint32(leafLogLaneID), 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "leaf.log")
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	db := &DB{relaxedSync: true}
	var flushCalls atomic.Int32
	var syncCalls atomic.Int32
	db.testOnVlogFlush = func(laneID int) {
		flushCalls.Add(1)
	}
	db.testOnVlogSync = func(laneID int) {
		syncCalls.Add(1)
	}
	leaf := lane{id: leafLogLaneID, vlog: writer}
	leaf.vlogDirty.Store(true)
	leafLog := &cachingLeafPageLog{db: db, lane: &leaf}

	if err := leafLog.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if got := flushCalls.Load(); got != 1 {
		t.Fatalf("flush calls=%d want 1", got)
	}
	if got := syncCalls.Load(); got != 0 {
		t.Fatalf("sync calls=%d want 0", got)
	}
}

type leafRecordLengthNotifierBackendStub struct {
	notified []page.ValuePtr
}

func (s *leafRecordLengthNotifierBackendStub) Get([]byte) ([]byte, error)       { return nil, nil }
func (s *leafRecordLengthNotifierBackendStub) GetUnsafe([]byte) ([]byte, error) { return nil, nil }
func (s *leafRecordLengthNotifierBackendStub) GetAppend(key, dst []byte) ([]byte, error) {
	return dst, nil
}
func (s *leafRecordLengthNotifierBackendStub) Has([]byte) (bool, error) { return false, nil }
func (s *leafRecordLengthNotifierBackendStub) Iterator(start, end []byte) (iterator.UnsafeIterator, error) {
	return nil, nil
}
func (s *leafRecordLengthNotifierBackendStub) ReverseIterator(start, end []byte) (iterator.UnsafeIterator, error) {
	return nil, nil
}
func (s *leafRecordLengthNotifierBackendStub) NewBatch() batch.Interface { return nil }
func (s *leafRecordLengthNotifierBackendStub) Close() error              { return nil }
func (s *leafRecordLengthNotifierBackendStub) Print() error              { return nil }
func (s *leafRecordLengthNotifierBackendStub) Stats() map[string]string  { return nil }
func (s *leafRecordLengthNotifierBackendStub) NoteLeafGenerationRecordLength(ptr page.ValuePtr) {
	s.notified = append(s.notified, ptr)
}

func TestDB_NoteLeafGenerationRecordLength_ForwardsToBackend(t *testing.T) {
	stub := &leafRecordLengthNotifierBackendStub{}
	db := &DB{backend: stub}
	ptr := page.ValuePtr{FileID: 7, Offset: 11, Length: page.ValuePtrMarkCompressed(123)}

	db.noteLeafGenerationRecordLength(ptr)

	if got, want := len(stub.notified), 1; got != want {
		t.Fatalf("notified=%d want %d", got, want)
	}
	if got := stub.notified[0]; got != ptr {
		t.Fatalf("notified ptr=%+v want %+v", got, ptr)
	}
}

func TestCompactLeafLogPayloadScratchPtrRefClearedForPooling(t *testing.T) {
	scratch := &compactLeafLogPayloadScratch{buf: make([]byte, 0, page.PageSize)}
	ref := &compactLeafLogPayloadScratchPtrRef{ptrs: make([]*compactLeafLogPayloadScratch, 1, 2)}
	ref.ptrs[0] = scratch
	putCompactLeafLogPayloadScratchPtrRef(ref, ref.ptrs)

	got := getCompactLeafLogPayloadScratchPtrRefCap(1)
	if len(got.ptrs) != 0 {
		t.Fatalf("len(got.ptrs)=%d want 0", len(got.ptrs))
	}
	for i, ptr := range got.ptrs[:cap(got.ptrs)] {
		if ptr != nil {
			t.Fatalf("pooled scratch ptr %d retained %p", i, ptr)
		}
	}
	putCompactLeafLogPayloadScratchPtrRef(got, got.ptrs)
}

func buildSparseLeafPageForLeafLogTest(t *testing.T) []byte {
	return buildSparseLeafPageForLeafLogTestWithTag(t, 'x')
}

func buildSparseLeafPageForLeafLogTestWithTag(t *testing.T, tag byte) []byte {
	t.Helper()
	buf := make([]byte, page.PageSize)
	b := node.NewBuilderWithOptions(buf, page.PageTypeLeaf, node.BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
		PackedValuePtr:        true,
	})
	for i := 0; i < 4; i++ {
		key := []byte{'k', 'e', 'y', '-', tag, '-', byte('a' + i)}
		val := []byte{'v', 'a', 'l', '-', tag}
		if err := b.AddLeafEntry(key, val, node.FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("AddLeafEntry(%d): %v", i, err)
		}
	}
	b.FinishNoNode()
	return buf
}

func TestCachingLeafPageLog_AppendLeafPagesGroupsPointersAndPayloads(t *testing.T) {
	dir := t.TempDir()
	leafDir := filepath.Join(dir, "leaf_vlog")
	if err := os.MkdirAll(leafDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(leaf_vlog): %v", err)
	}
	fileID, err := valuelog.EncodeFileID(uint32(leafLogLaneID), 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(leafDir, valueLogName(leafLogLaneID, 1))
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	db := &DB{closeCh: make(chan struct{}), indexOuterLeavesInValueLog: true}
	db.nextRID.Store(0)
	db.leafLog = lane{id: leafLogLaneID, vlog: writer, vlogPath: path, vlogSeq: 1}
	leafLog := &cachingLeafPageLog{db: db, lane: &db.leafLog}
	pages := [][]byte{
		buildSparseLeafPageForLeafLogTestWithTag(t, 'a'),
		buildSparseLeafPageForLeafLogTestWithTag(t, 'b'),
		buildSparseLeafPageForLeafLogTestWithTag(t, 'c'),
	}

	ptrs, err := leafLog.AppendLeafPages(pages)
	if err != nil {
		t.Fatalf("AppendLeafPages: %v", err)
	}
	if len(ptrs) != len(pages) {
		t.Fatalf("ptr count=%d want %d", len(ptrs), len(pages))
	}
	for i, ptr := range ptrs {
		if ptr.Offset != ptrs[0].Offset || ptr.FileID != ptrs[0].FileID {
			t.Fatalf("ptr[%d]=%+v not grouped with ptr[0]=%+v", i, ptr, ptrs[0])
		}
		if ptr.SubIndex != uint16(i) {
			t.Fatalf("ptr[%d].SubIndex=%d want %d", i, ptr.SubIndex, i)
		}
		if ptr.RecordLengthHint == 0 {
			t.Fatalf("ptr[%d] missing record length hint: %+v", i, ptr)
		}
		if !ptr.IsGrouped() || !page.ValuePtrIsGrouped(ptr.ValuePtr()) {
			t.Fatalf("ptr[%d] did not preserve grouped frame flag: %+v valuePtr=%+v", i, ptr, ptr.ValuePtr())
		}
	}
	if got := db.leafLog.vlogRetainedPath; got != path {
		t.Fatalf("leaf retained path=%q want %q", got, path)
	}
	db.valueLogMu.Lock()
	_, retained := db.valueLogRetain[path]
	db.valueLogMu.Unlock()
	if !retained {
		t.Fatalf("leaf retained path %q was not registered", path)
	}
	if err := leafLog.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	reader, err := valuelog.NewReader(path, fileID)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer reader.Close()
	for i, want := range pages {
		rid, got, gotPtr, err := reader.ReadNext()
		if err != nil {
			t.Fatalf("ReadNext(%d): %v", i, err)
		}
		if rid != uint64(i+1) {
			t.Fatalf("rid[%d]=%d want %d", i, rid, i+1)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("payload[%d] mismatch", i)
		}
		gotLeafPtr, err := page.LeafLogPtrFromValuePtr(gotPtr)
		if err != nil {
			t.Fatalf("LeafLogPtrFromValuePtr(%d): %v", i, err)
		}
		if gotLeafPtr != ptrs[i] {
			t.Fatalf("reader ptr[%d]=%+v want %+v", i, gotLeafPtr, ptrs[i])
		}
	}
	if _, _, _, err := reader.ReadNext(); !errors.Is(err, io.EOF) {
		t.Fatalf("ReadNext after records error=%v want EOF", err)
	}
}

func TestCachingLeafPageLog_AppendLeafPageCompactsSparseLeafPayload(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(uint32(leafLogLaneID), 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "leaf.log")
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	db := &DB{closeCh: make(chan struct{})}
	db.nextRID.Store(0)
	leaf := lane{id: leafLogLaneID, vlog: writer}
	leafLog := &cachingLeafPageLog{db: db, lane: &leaf}

	if _, err := leafLog.AppendLeafPage(buildSparseLeafPageForLeafLogTest(t)); err != nil {
		t.Fatalf("AppendLeafPage: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	writer = nil
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(%q): %v", path, err)
	}
	if info.Size() >= int64(valuelog.HeaderSize+page.PageSize) {
		t.Fatalf("file size=%d want compact leaf payload smaller than raw %d", info.Size(), valuelog.HeaderSize+page.PageSize)
	}
}
