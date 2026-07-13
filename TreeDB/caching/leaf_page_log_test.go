package caching

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"

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

func TestCachingLeafPageLog_CloseWaitsForInProgressAppend(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	db, err := Open(dir, backend, Options{
		IndexOuterLeavesInValueLog: true,
		FlushApplyConcurrency:      2,
		RelaxedSync:                true,
		AllowUnsafe:                true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cache: %v", err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	var fired atomic.Bool
	db.testBeforeVlogUnlock = func(laneID int) {
		if laneID != leafLogLaneID || !fired.CompareAndSwap(false, true) {
			return
		}
		close(entered)
		<-release
	}

	appendDone := make(chan error, 1)
	go func() {
		leafLog := newCachingLeafPageLog(db, &db.leafLog)
		_, err := leafLog.AppendLeafPage(bytes.Repeat([]byte("x"), page.PageSize))
		appendDone <- err
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		_ = db.Close()
		t.Fatal("append did not reach pre-unlock hook")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- db.Close() }()
	select {
	case err := <-closeDone:
		close(release)
		t.Fatalf("Close returned before append left leaf-log mutex: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-appendDone; err != nil {
		t.Fatalf("append during close: %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close after append release: %v", err)
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

type capturingLeafLogBackend struct {
	BackendDB
	leafLog   backenddb.LeafPageLog
	closeOnce sync.Once
	closeErr  error
}

func (b *capturingLeafLogBackend) Close() error {
	b.closeOnce.Do(func() {
		b.closeErr = b.BackendDB.Close()
	})
	return b.closeErr
}

func (b *capturingLeafLogBackend) SetLeafPageLog(log backenddb.LeafPageLog) {
	b.leafLog = log
	if setter, ok := any(b.BackendDB).(interface{ SetLeafPageLog(backenddb.LeafPageLog) }); ok {
		setter.SetLeafPageLog(log)
	}
}

func openCachingLeafPageLogLaneTestDB(t *testing.T) (*DB, *capturingLeafLogBackend, string) {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	captured := &capturingLeafLogBackend{BackendDB: backend}
	db, err := Open(dir, captured, Options{
		IndexOuterLeavesInValueLog: true,
		FlushApplyConcurrency:      2,
		RelaxedSync:                true,
		AllowUnsafe:                true,
	})
	if err != nil {
		_ = captured.Close()
		t.Fatalf("cache open: %v", err)
	}
	if captured.leafLog == nil {
		_ = db.Close()
		t.Fatal("leaf log was not installed on backend")
	}
	return db, captured, dir
}

func testLeafPageBytes(label string) []byte {
	buf := bytes.Repeat([]byte{0}, page.PageSize)
	copy(buf, label)
	return buf
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

func buildSparseLeafPageForLeafLogTest(t testing.TB) []byte {
	return buildSparseLeafPageForLeafLogTestWithTag(t, 'x')
}

func buildSparseLeafPageForLeafLogTestWithTag(t testing.TB, tag byte) []byte {
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

func TestCachingLeafPageLogLaneSelectionAppendsUniqueReadablePtrs(t *testing.T) {
	db, captured, dir := openCachingLeafPageLogLaneTestDB(t)
	provider, ok := captured.leafLog.(backenddb.LeafPageLogLaneProvider)
	if !ok {
		t.Fatal("captured leaf log missing lane provider")
	}
	defaultPage := testLeafPageBytes("lane-0-default")
	defaultPtr, err := captured.leafLog.AppendLeafPage(defaultPage)
	if err != nil {
		t.Fatalf("default AppendLeafPage: %v", err)
	}
	if got := len(db.leafLogAppendLanesSnapshot()); got != 1 {
		t.Fatalf("default append created %d leaf lanes, want 1", got)
	}

	const extraLanes = 3
	type laneResult struct {
		lane int
		ptr  page.LeafLogPtr
		want []byte
	}
	results := make(chan laneResult, extraLanes)
	var wg sync.WaitGroup
	for lane := 1; lane <= extraLanes; lane++ {
		lane := lane
		wg.Add(1)
		go func() {
			defer wg.Done()
			appender, ok := provider.LeafPageLogLane(lane)
			if !ok || appender == nil {
				t.Errorf("LeafPageLogLane(%d) unavailable", lane)
				return
			}
			want := testLeafPageBytes(fmt.Sprintf("lane-%d", lane))
			ptr, err := appender.AppendLeafPage(want)
			if err != nil {
				t.Errorf("lane %d AppendLeafPage: %v", lane, err)
				return
			}
			results <- laneResult{lane: lane, ptr: ptr, want: want}
		}()
	}
	wg.Wait()
	close(results)

	checks := []laneResult{{lane: 0, ptr: defaultPtr, want: defaultPage}}
	for res := range results {
		checks = append(checks, res)
	}
	if len(checks) != extraLanes+1 {
		t.Fatalf("got %d appended pages, want %d", len(checks), extraLanes+1)
	}
	seen := map[uint32]struct{}{}
	for _, res := range checks {
		fileID := res.ptr.ValuePtr().FileID
		laneID, seq := valuelog.DecodeFileID(fileID)
		if laneID != leafLogLaneID {
			t.Fatalf("lane %d fileID lane=%d want=%d (fileID=%d seq=%d)", res.lane, laneID, leafLogLaneID, fileID, seq)
		}
		if _, ok := seen[fileID]; ok {
			t.Fatalf("duplicate leaf log fileID %d", fileID)
		}
		seen[fileID] = struct{}{}
	}
	if got := len(db.leafLogAppendLanesSnapshot()); got != extraLanes+1 {
		t.Fatalf("lane snapshot len=%d want %d", got, extraLanes+1)
	}
	stats := db.Stats()
	if got := requireStatUint64(t, stats, "treedb.cache.leaf_log_lanes.configured"); got != extraLanes+1 {
		t.Fatalf("configured leaf-log lanes=%d want %d", got, extraLanes+1)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.leaf_log_lanes.append_lanes_used"); got != extraLanes+1 {
		t.Fatalf("append lanes used=%d want %d", got, extraLanes+1)
	}
	if got := requireStatUint64(t, stats, "treedb.cache.leaf_log_lanes.append_calls_total"); got != extraLanes+1 {
		t.Fatalf("append calls=%d want %d", got, extraLanes+1)
	}
	for lane := 0; lane <= extraLanes; lane++ {
		key := fmt.Sprintf("treedb.cache.leaf_log_lanes.lane.%02d.append_pages_total", lane)
		if got := requireStatUint64(t, stats, key); got != 1 {
			t.Fatalf("%s=%d want 1", key, got)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close cached db: %v", err)
	}
	backend2, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatalf("reopen backend: %v", err)
	}
	reopened, err := Open(dir, backend2, Options{
		IndexOuterLeavesInValueLog: true,
		FlushApplyConcurrency:      2,
		RelaxedSync:                true,
		AllowUnsafe:                true,
	})
	if err != nil {
		_ = backend2.Close()
		t.Fatalf("reopen cache: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	for _, res := range checks {
		got, err := reopened.ReadValueLogRecord(res.ptr.ValuePtr())
		if err != nil {
			t.Fatalf("reopen read lane %d: %v", res.lane, err)
		}
		if !bytes.Equal(got, res.want) {
			t.Fatalf("reopen read lane %d mismatch", res.lane)
		}
	}
}

func TestCachingSpanNativeLeafLogOutputUsesSelectedLanes(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		ChunkSize:                  64 * 1024,
		IndexOuterLeavesInValueLog: true,
		FlushAdmissionPolicy:       backenddb.FlushAdmissionPolicyExplicit,
		FlushApplyConcurrency:      4,
		FlushApplyMinEntries:       1,
		FlushApplyMinSpans:         1,
		FlushApplyMinBytes:         1,
		FlushApplySpanNative:       true,
		ValueLog: backenddb.ValueLogOptions{
			Compression: backenddb.ValueLogCompressionBlock,
			BlockCodec:  backenddb.ValueLogBlockLZ4,
		},
	})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	captured := &capturingLeafLogBackend{BackendDB: backend}
	db, err := Open(dir, captured, Options{
		IndexOuterLeavesInValueLog: true,
		FlushApplyConcurrency:      4,
		FlushApplyMinEntries:       1,
		FlushApplyMinSpans:         1,
		FlushApplyMinBytes:         1,
		FlushApplySpanNative:       true,
		RelaxedSync:                true,
		AllowUnsafe:                true,
	})
	if err != nil {
		_ = captured.Close()
		t.Fatalf("cache open: %v", err)
	}
	defer func() {
		_ = db.Close()
		_ = captured.Close()
	}()

	writeBatch := func(label string, fn func(*Batch) error) {
		t.Helper()
		b := db.NewBatch()
		if err := fn(b); err != nil {
			_ = b.Close()
			t.Fatalf("%s build: %v", label, err)
		}
		if err := b.Write(); err != nil {
			_ = b.Close()
			t.Fatalf("%s write: %v", label, err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("%s close: %v", label, err)
		}
	}
	writeBatch("base", func(b *Batch) error {
		for i := 0; i < 8192; i++ {
			key := []byte(fmt.Sprintf("key-%06d", i))
			val := bytes.Repeat([]byte{byte(1 + i%251)}, 180)
			if err := b.Set(key, val); err != nil {
				return err
			}
		}
		return nil
	})
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("base Checkpoint: %v", err)
	}

	writeBatch("updates", func(b *Batch) error {
		for i := 0; i < 512; i++ {
			idx := (i * 97) % 8192
			key := []byte(fmt.Sprintf("key-%06d", idx))
			val := []byte(fmt.Sprintf("updated-%06d", idx))
			if err := b.Set(key, val); err != nil {
				return err
			}
		}
		return nil
	})
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("updates Checkpoint: %v", err)
	}

	stats := captured.Stats()
	raw := stats["treedb.flush_apply.span_native.used_ops_total"]
	usedOps, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || usedOps == 0 {
		t.Fatalf("span-native used ops stat=%q err=%v want >0", raw, err)
	}
	lanes := db.leafLogAppendLanesSnapshot()
	nonDefault := 0
	for i, l := range lanes {
		if i == 0 || l == nil {
			continue
		}
		l.vlogMu.Lock()
		used := l.vlogSeq > 0
		l.vlogMu.Unlock()
		if used {
			nonDefault++
		}
	}
	if nonDefault == 0 {
		t.Fatalf("selected leaf-log lanes=%d want >0 (snapshot len=%d, leafLog=%T, outer=%t, append_calls=%q)", nonDefault, len(lanes), captured.leafLog, db.indexOuterLeavesInValueLog, stats["treedb.flush_apply.leaf_log_output.append_calls_total"])
	}

	for _, idx := range []int{0, 97, 97 * 7 % 8192} {
		key := []byte(fmt.Sprintf("key-%06d", idx))
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		want := []byte(fmt.Sprintf("updated-%06d", idx))
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%q)=%q want %q", key, got, want)
		}
	}
}

func TestCachingLeafPageLogLaneProviderFlushesSelectedLaneForLiveRead(t *testing.T) {
	db, captured, _ := openCachingLeafPageLogLaneTestDB(t)
	defer func() { _ = db.Close() }()
	provider, ok := captured.leafLog.(backenddb.LeafPageLogLaneProvider)
	if !ok {
		t.Fatal("captured leaf log missing lane provider")
	}
	defaultPage := testLeafPageBytes("live-default")
	defaultPtr, err := captured.leafLog.AppendLeafPage(defaultPage)
	if err != nil {
		t.Fatalf("default AppendLeafPage: %v", err)
	}
	lane1, ok := provider.LeafPageLogLane(1)
	if !ok || lane1 == nil {
		t.Fatal("LeafPageLogLane(1) unavailable")
	}
	lanePage := testLeafPageBytes("live-lane-1")
	lanePtr, err := lane1.AppendLeafPage(lanePage)
	if err != nil {
		t.Fatalf("lane 1 AppendLeafPage: %v", err)
	}

	currentIDs := db.valueLogReader.CurrentWritableFileIDs()
	current := make(map[uint32]struct{}, len(currentIDs))
	for _, id := range currentIDs {
		current[id] = struct{}{}
	}
	for _, ptr := range []page.LeafLogPtr{defaultPtr, lanePtr} {
		fileID := ptr.ValuePtr().FileID
		if _, ok := current[fileID]; !ok {
			t.Fatalf("current writable ids %v missing %d", currentIDs, fileID)
		}
	}
	var readBarrierFlushes atomic.Int32
	db.testOnVlogFlush = func(laneID int) {
		if laneID == leafLogLaneID {
			readBarrierFlushes.Add(1)
		}
	}
	got, err := db.valueLogReader.Read(lanePtr.ValuePtr())
	if err != nil {
		t.Fatalf("live read selected lane: %v", err)
	}
	if !bytes.Equal(got, lanePage) {
		t.Fatalf("live read selected lane mismatch")
	}
	if readBarrierFlushes.Load() == 0 {
		t.Fatalf("live read selected lane did not flush through current-writable barrier")
	}
}

func TestCachingLeafPageLogLaneLiveReadIgnoresInactiveSameSeqLane(t *testing.T) {
	db, captured, _ := openCachingLeafPageLogLaneTestDB(t)
	defer func() { _ = db.Close() }()
	provider, ok := captured.leafLog.(backenddb.LeafPageLogLaneProvider)
	if !ok {
		t.Fatal("captured leaf log missing lane provider")
	}
	lane2, ok := provider.LeafPageLogLane(2)
	if !ok || lane2 == nil {
		t.Fatal("LeafPageLogLane(2) unavailable")
	}
	lanePage := testLeafPageBytes("live-lane-2")
	lanePtr, err := lane2.AppendLeafPage(lanePage)
	if err != nil {
		t.Fatalf("lane 2 AppendLeafPage: %v", err)
	}
	lane1, ok := provider.LeafPageLogLane(1)
	if !ok || lane1 == nil {
		t.Fatal("LeafPageLogLane(1) unavailable")
	}
	_ = lane1

	var readBarrierFlushes atomic.Int32
	db.testOnVlogFlush = func(laneID int) {
		if laneID == leafLogLaneID {
			readBarrierFlushes.Add(1)
		}
	}
	got, err := db.valueLogReader.Read(lanePtr.ValuePtr())
	if err != nil {
		t.Fatalf("live read selected lane with inactive same-seq lane: %v", err)
	}
	if !bytes.Equal(got, lanePage) {
		t.Fatalf("live read selected lane with inactive same-seq lane mismatch")
	}
	if readBarrierFlushes.Load() == 0 {
		t.Fatalf("live read selected lane did not flush active same-seq writer")
	}
}

func TestCachingLeafPageLogLaneMaintenanceAdvanceUsesUniqueSequences(t *testing.T) {
	db, captured, _ := openCachingLeafPageLogLaneTestDB(t)
	defer func() { _ = db.Close() }()
	provider, ok := captured.leafLog.(backenddb.LeafPageLogLaneProvider)
	if !ok {
		t.Fatal("captured leaf log missing lane provider")
	}
	appenders := []backenddb.LeafPageLog{captured.leafLog}
	for worker := 1; worker <= 2; worker++ {
		appender, ok := provider.LeafPageLogLane(worker)
		if !ok || appender == nil {
			t.Fatalf("LeafPageLogLane(%d) unavailable", worker)
		}
		appenders = append(appenders, appender)
	}
	for i, appender := range appenders {
		if _, err := appender.AppendLeafPage(testLeafPageBytes(fmt.Sprintf("advance-%d", i))); err != nil {
			t.Fatalf("AppendLeafPage lane %d: %v", i, err)
		}
	}
	lanes := db.leafLogAppendLanesSnapshot()
	oldIDs := make(map[uint32]struct{}, len(lanes))
	maxSeq := 0
	for _, l := range lanes {
		if l == nil || l.vlogSeq == 0 {
			continue
		}
		fileID, err := valuelog.EncodeFileID(uint32(l.id), uint32(l.vlogSeq))
		if err != nil {
			t.Fatalf("EncodeFileID old seq %d: %v", l.vlogSeq, err)
		}
		oldIDs[fileID] = struct{}{}
		if l.vlogSeq > maxSeq {
			maxSeq = l.vlogSeq
		}
	}
	observedMaxSeq := maxSeq + 10
	db.advanceLeafLogAppendSeqAtLeast(observedMaxSeq)
	for _, l := range lanes {
		if err := db.advanceLeafLogAppendWriterPastObservedSeq(l, observedMaxSeq); err != nil {
			t.Fatalf("advanceLeafLogAppendWriterPastObservedSeq: %v", err)
		}
	}
	seenSeq := make(map[int]struct{}, len(lanes))
	for _, l := range lanes {
		if l == nil {
			continue
		}
		if l.vlogSeq <= observedMaxSeq {
			t.Fatalf("lane seq=%d want > observed %d", l.vlogSeq, observedMaxSeq)
		}
		if _, dup := seenSeq[l.vlogSeq]; dup {
			t.Fatalf("duplicate advanced leaf seq %d", l.vlogSeq)
		}
		seenSeq[l.vlogSeq] = struct{}{}
	}
	currentIDs := db.valueLogReader.CurrentWritableFileIDs()
	current := make(map[uint32]struct{}, len(currentIDs))
	for _, id := range currentIDs {
		current[id] = struct{}{}
	}
	for oldID := range oldIDs {
		if _, stillCurrent := current[oldID]; stillCurrent {
			t.Fatalf("old leaf segment %d still current after rotation; current=%v", oldID, currentIDs)
		}
	}
	for _, l := range lanes {
		fileID, err := valuelog.EncodeFileID(uint32(l.id), uint32(l.vlogSeq))
		if err != nil {
			t.Fatalf("EncodeFileID new seq %d: %v", l.vlogSeq, err)
		}
		if _, ok := current[fileID]; !ok {
			t.Fatalf("current writable ids %v missing advanced file %d", currentIDs, fileID)
		}
	}
}

func TestCachingLeafPageLogLaneSnapshotsAggregateAndMarkPerLane(t *testing.T) {
	db, captured, _ := openCachingLeafPageLogLaneTestDB(t)
	defer func() { _ = db.Close() }()
	provider, ok := captured.leafLog.(backenddb.LeafPageLogLaneProvider)
	if !ok {
		t.Fatal("captured leaf log missing lane provider")
	}
	createdProvider, ok := captured.leafLog.(backenddb.LeafPageLogCreatedSegmentProvider)
	if !ok {
		t.Fatal("captured leaf log missing created snapshot provider")
	}
	currentProvider, ok := captured.leafLog.(backenddb.LeafPageLogCurrentSegmentProvider)
	if !ok {
		t.Fatal("captured leaf log missing current snapshot provider")
	}
	observer, ok := captured.leafLog.(backenddb.LeafPageLogSegmentRegistrationObserver)
	if !ok {
		t.Fatal("captured leaf log missing registration observer")
	}

	appenders := []backenddb.LeafPageLog{captured.leafLog}
	for lane := 1; lane <= 2; lane++ {
		appender, ok := provider.LeafPageLogLane(lane)
		if !ok || appender == nil {
			t.Fatalf("LeafPageLogLane(%d) unavailable", lane)
		}
		appenders = append(appenders, appender)
	}
	for i, appender := range appenders {
		if _, err := appender.AppendLeafPage(testLeafPageBytes(fmt.Sprintf("snapshot-%d", i))); err != nil {
			t.Fatalf("AppendLeafPage lane %d: %v", i, err)
		}
	}

	created, err := createdProvider.CreatedLeafPageLogSegmentsSnapshot()
	if err != nil {
		t.Fatalf("CreatedLeafPageLogSegmentsSnapshot: %v", err)
	}
	if len(created) != len(appenders) {
		t.Fatalf("created segments=%d want %d", len(created), len(appenders))
	}
	current, err := currentProvider.CurrentLeafPageLogSegmentsSnapshot()
	if err != nil {
		t.Fatalf("CurrentLeafPageLogSegmentsSnapshot: %v", err)
	}
	if len(current) != len(appenders) {
		t.Fatalf("current segments=%d want %d", len(current), len(appenders))
	}

	observer.MarkLeafPageLogSegmentsRegistered(created[:1])
	afterFirstMark, err := createdProvider.CreatedLeafPageLogSegmentsSnapshot()
	if err != nil {
		t.Fatalf("CreatedLeafPageLogSegmentsSnapshot after first mark: %v", err)
	}
	if len(afterFirstMark) != len(appenders)-1 {
		t.Fatalf("created segments after first mark=%d want %d", len(afterFirstMark), len(appenders)-1)
	}
	currentAfterMark, err := currentProvider.CurrentLeafPageLogSegmentsSnapshot()
	if err != nil {
		t.Fatalf("CurrentLeafPageLogSegmentsSnapshot after first mark: %v", err)
	}
	if len(currentAfterMark) != len(appenders) {
		t.Fatalf("current segments after mark=%d want %d", len(currentAfterMark), len(appenders))
	}
	observer.MarkLeafPageLogSegmentsRegistered(afterFirstMark)
	finalCreated, err := createdProvider.CreatedLeafPageLogSegmentsSnapshot()
	if err != nil {
		t.Fatalf("CreatedLeafPageLogSegmentsSnapshot after final mark: %v", err)
	}
	if len(finalCreated) != 0 {
		t.Fatalf("created segments after final mark=%d want 0", len(finalCreated))
	}
}

func TestCachingLeafPageLogLanes_FlushSyncCloseTouchAllLanes(t *testing.T) {
	db, captured, _ := openCachingLeafPageLogLaneTestDB(t)
	provider, ok := captured.leafLog.(backenddb.LeafPageLogLaneProvider)
	if !ok {
		t.Fatal("captured leaf log missing lane provider")
	}
	closer, ok := captured.leafLog.(interface{ Close() error })
	if !ok {
		t.Fatal("captured leaf log missing close")
	}
	appenders := []backenddb.LeafPageLog{captured.leafLog}
	for lane := 1; lane <= 2; lane++ {
		appender, ok := provider.LeafPageLogLane(lane)
		if !ok || appender == nil {
			t.Fatalf("LeafPageLogLane(%d) unavailable", lane)
		}
		appenders = append(appenders, appender)
	}
	for i, appender := range appenders {
		if _, err := appender.AppendLeafPage(testLeafPageBytes(fmt.Sprintf("touch-%d", i))); err != nil {
			t.Fatalf("AppendLeafPage lane %d: %v", i, err)
		}
	}
	var flushCalls atomic.Int32
	var syncCalls atomic.Int32
	db.testOnVlogFlush = func(laneID int) {
		flushCalls.Add(1)
	}
	db.testOnVlogSync = func(laneID int) {
		syncCalls.Add(1)
	}
	if err := captured.leafLog.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := flushCalls.Load(); got != int32(len(appenders)) {
		t.Fatalf("flush count=%d want %d", got, len(appenders))
	}
	db.relaxedSync = false
	if err := captured.leafLog.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if got := syncCalls.Load(); got != int32(len(appenders)) {
		t.Fatalf("sync count=%d want %d", got, len(appenders))
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for i, lane := range db.leafLogAppendLanesSnapshot() {
		if lane == nil {
			continue
		}
		if lane.vlog != nil {
			t.Fatalf("lane %d still has an open writer", i)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db Close: %v", err)
	}
}
