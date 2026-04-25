package caching

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func buildSparseLeafPageForLeafLogTest(t *testing.T) []byte {
	t.Helper()
	buf := make([]byte, page.PageSize)
	b := node.NewBuilderWithOptions(buf, page.PageTypeLeaf, node.BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
		PackedValuePtr:        true,
	})
	for i := 0; i < 4; i++ {
		if err := b.AddLeafEntry([]byte("key-"+string(rune('a'+i))), []byte("value"), node.FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("AddLeafEntry(%d): %v", i, err)
		}
	}
	b.FinishNoNode()
	return buf
}

func requireLeafLogTestPagesEqual(t *testing.T, want, got []byte) {
	t.Helper()
	wantNode := node.NewNodeView(want)
	gotNode := node.NewNodeView(got)
	if wantNode.Type() != gotNode.Type() {
		t.Fatalf("page types differ: got=%d want=%d", gotNode.Type(), wantNode.Type())
	}
	if wantNode.Count() != gotNode.Count() {
		t.Fatalf("page counts differ: got=%d want=%d", gotNode.Count(), wantNode.Count())
	}
	for i := uint16(0); i < wantNode.Count(); i++ {
		wantKey, wantVal, wantPtr, wantFlags, err := wantNode.GetLeafEntryView(i)
		if err != nil {
			t.Fatalf("want GetLeafEntryView(%d): %v", i, err)
		}
		gotKey, gotVal, gotPtr, gotFlags, err := gotNode.GetLeafEntryView(i)
		if err != nil {
			t.Fatalf("got GetLeafEntryView(%d): %v", i, err)
		}
		if !bytes.Equal(gotKey, wantKey) || !bytes.Equal(gotVal, wantVal) || gotPtr != wantPtr || gotFlags != wantFlags {
			t.Fatalf("leaf entry %d mismatch", i)
		}
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

func TestDB_AppendPreparedLeafPageValueLog_AppendsAndReturnsUnlocked(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(uint32(leafLogLaneID), 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	leafDir := filepath.Join(dir, "leaf_vlog")
	if err := os.MkdirAll(leafDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", leafDir, err)
	}
	path := filepath.Join(leafDir, "value-l255-000001.log")
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() {
		if writer != nil {
			_ = writer.Close()
		}
	}()

	db := &DB{
		closeCh:                 make(chan struct{}),
		valueLogCompressionMode: uint8(vlogCompressionOff),
	}
	leaf := lane{id: leafLogLaneID, vlog: writer}
	leafPage := buildSparseLeafPageForLeafLogTest(t)

	leaf.vlogMu.Lock()
	ptr, retainPath, stats, compacted, payloadLen, totalBytes, err := db.appendPreparedLeafPageValueLog(&leaf, 1, leafPage)
	if err != nil {
		t.Fatalf("appendPreparedLeafPageValueLog: %v", err)
	}
	if !leaf.vlogMu.TryLock() {
		t.Fatalf("appendPreparedLeafPageValueLog returned with vlogMu locked")
	}
	leaf.vlogMu.Unlock()
	if retainPath != "" {
		t.Fatalf("retainPath=%q want empty", retainPath)
	}
	if !compacted {
		t.Fatalf("expected sparse leaf page to compact")
	}
	if payloadLen != stats.RawPayloadBytes || payloadLen >= len(leafPage) {
		t.Fatalf("payloadLen=%d stats.RawPayloadBytes=%d raw=%d", payloadLen, stats.RawPayloadBytes, len(leafPage))
	}
	if totalBytes <= 0 {
		t.Fatalf("totalBytes=%d want > 0", totalBytes)
	}
	if got := leaf.backendReadDirtySeq.Load(); got != 1 {
		t.Fatalf("backendReadDirtySeq=%d want 1", got)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	writer = nil

	mgr, err := valuelog.NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()
	if err := mgr.RegisterSegment(path, fileID); err != nil {
		t.Fatalf("RegisterSegment(%q): %v", path, err)
	}
	got, err := mgr.ReadUnsafe(ptr)
	if err != nil {
		t.Fatalf("ReadUnsafe: %v", err)
	}
	requireLeafLogTestPagesEqual(t, leafPage, got)
}

func TestDB_FlushValueLogLaneDrainsPreparingLeafAppend(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(uint32(leafLogLaneID), 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	leafDir := filepath.Join(dir, "leaf_vlog")
	if err := os.MkdirAll(leafDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", leafDir, err)
	}
	path := filepath.Join(leafDir, "value-l255-000001.log")
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() {
		if writer != nil {
			_ = writer.Close()
		}
	}()

	db := &DB{
		closeCh:                 make(chan struct{}),
		valueLogCompressionMode: uint8(vlogCompressionOff),
	}
	leaf := lane{id: leafLogLaneID, vlog: writer}
	leafPage := buildSparseLeafPageForLeafLogTest(t)
	raw, stats, compacted, ptrLength, err := valuelog.PrepareCompactLeafPageRecord(nil, 1, leafPage)
	if err != nil {
		t.Fatalf("PrepareCompactLeafPageRecord: %v", err)
	}
	req := &leafPageAppendRequest{
		raw:        raw,
		ptrLength:  ptrLength,
		stats:      stats,
		compacted:  compacted,
		payloadLen: stats.RawPayloadBytes,
	}
	req.wg.Add(1)

	leaf.vlogMu.Lock()
	leaf.leafAppendMu.Lock()
	leaf.leafAppendPreparing = 1
	leaf.leafAppendMu.Unlock()

	done := make(chan error, 1)
	go func() {
		done <- db.flushValueLogLane(&leaf)
	}()
	leaf.vlogMu.Unlock()

	leaf.leafAppendMu.Lock()
	leaf.leafAppendPreparing--
	leaf.leafAppendQueue = append(leaf.leafAppendQueue, req)
	broadcastLeafAppendLocked(&leaf)
	leaf.leafAppendMu.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("flushValueLogLane: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("flushValueLogLane did not drain preparing leaf append")
	}
	req.wg.Wait()
	if req.err != nil {
		t.Fatalf("prepared append error: %v", req.err)
	}
	if req.ptr.FileID == 0 || req.totalBytes <= 0 {
		t.Fatalf("prepared append ptr=%+v totalBytes=%d, want written record", req.ptr, req.totalBytes)
	}
	if got, want := leaf.backendReadFlushedSeq.Load(), leaf.backendReadDirtySeq.Load(); got != want {
		t.Fatalf("backendReadFlushedSeq=%d want dirty seq %d after flush", got, want)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	writer = nil
}

func TestDB_AppendPreparedLeafPageValueLog_BatchesQueuedPreparedAppends(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(uint32(leafLogLaneID), 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	leafDir := filepath.Join(dir, "leaf_vlog")
	if err := os.MkdirAll(leafDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", leafDir, err)
	}
	path := filepath.Join(leafDir, "value-l255-000001.log")
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() {
		if writer != nil {
			_ = writer.Close()
		}
	}()

	db := &DB{
		closeCh:                 make(chan struct{}),
		valueLogCompressionMode: uint8(vlogCompressionOff),
	}
	var preUnlockHooks atomic.Int32
	db.testBeforeVlogUnlock = func(laneID int) {
		if laneID == leafLogLaneID {
			preUnlockHooks.Add(1)
		}
	}

	leaf := lane{id: leafLogLaneID, vlog: writer}
	leafPage := buildSparseLeafPageForLeafLogTest(t)
	const requests = 8
	ptrs := make([]page.ValuePtr, requests)
	errs := make(chan error, requests)
	start := make(chan struct{})
	var wg sync.WaitGroup

	leaf.vlogMu.Lock()
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ptr, _, _, _, _, _, err := db.appendPreparedLeafPageValueLogQueued(&leaf, uint64(i+1), leafPage)
			ptrs[i] = ptr
			errs <- err
		}(i)
	}
	close(start)

	deadline := time.Now().Add(2 * time.Second)
	for {
		leaf.leafAppendMu.Lock()
		queued := len(leaf.leafAppendQueue)
		draining := leaf.leafAppendDraining
		leaf.leafAppendMu.Unlock()
		if queued == requests && draining {
			break
		}
		if time.Now().After(deadline) {
			leaf.vlogMu.Unlock()
			t.Fatalf("queued prepared appends=%d draining=%v want %d queued and draining", queued, draining, requests)
		}
		time.Sleep(time.Millisecond)
	}
	leaf.vlogMu.Unlock()

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("appendPreparedLeafPageValueLogQueued: %v", err)
		}
	}
	if got := preUnlockHooks.Load(); got != 1 {
		t.Fatalf("pre-unlock hooks=%d want 1 batched writer lock", got)
	}
	if got := leaf.backendReadDirtySeq.Load(); got != 1 {
		t.Fatalf("backendReadDirtySeq=%d want 1 batched boundary", got)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	writer = nil

	mgr, err := valuelog.NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()
	if err := mgr.RegisterSegment(path, fileID); err != nil {
		t.Fatalf("RegisterSegment(%q): %v", path, err)
	}
	for i, ptr := range ptrs {
		if ptr.FileID == 0 {
			t.Fatalf("ptr[%d] is empty", i)
		}
		got, err := mgr.ReadUnsafe(ptr)
		if err != nil {
			t.Fatalf("ReadUnsafe(%d): %v", i, err)
		}
		requireLeafLogTestPagesEqual(t, leafPage, got)
	}
}

func TestDB_AppendPreparedLeafPageValueLog_BatchRotationMarksDirtyBoundary(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(uint32(leafLogLaneID), 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	leafDir := filepath.Join(dir, "leaf_vlog")
	if err := os.MkdirAll(leafDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", leafDir, err)
	}
	path := filepath.Join(leafDir, "value-l255-000001.log")
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() {
		if writer != nil {
			_ = writer.Close()
		}
	}()
	if _, err := writer.Append(0, nil, 99, bytes.Repeat([]byte("seed"), 32<<10)); err != nil {
		t.Fatalf("seed append: %v", err)
	}
	startSize := writer.Size()

	db := &DB{
		closeCh:                 make(chan struct{}),
		valueLogCompressionMode: uint8(vlogCompressionOff),
		valueLogDir:             leafDir,
		valueLogMaxSegmentBytes: startSize + 1,
	}
	leaf := lane{id: leafLogLaneID, vlog: writer, vlogSeq: 1, vlogPath: path}
	leafPage := buildSparseLeafPageForLeafLogTest(t)

	const requests = 2
	ptrs := make([]page.ValuePtr, requests)
	totalBytes := make([]int64, requests)
	errs := make(chan error, requests)
	start := make(chan struct{})
	var wg sync.WaitGroup

	leaf.vlogMu.Lock()
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ptr, _, _, _, _, bytesWritten, err := db.appendPreparedLeafPageValueLogQueued(&leaf, uint64(i+1), leafPage)
			ptrs[i] = ptr
			totalBytes[i] = bytesWritten
			errs <- err
		}(i)
	}
	close(start)

	deadline := time.Now().Add(2 * time.Second)
	for {
		leaf.leafAppendMu.Lock()
		queued := len(leaf.leafAppendQueue)
		draining := leaf.leafAppendDraining
		leaf.leafAppendMu.Unlock()
		if queued == requests && draining {
			break
		}
		if time.Now().After(deadline) {
			leaf.vlogMu.Unlock()
			t.Fatalf("queued prepared appends=%d draining=%v want %d queued and draining", queued, draining, requests)
		}
		time.Sleep(time.Millisecond)
	}
	leaf.vlogMu.Unlock()

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("appendPreparedLeafPageValueLogQueued: %v", err)
		}
	}
	if ptrs[0].FileID == ptrs[1].FileID {
		t.Fatalf("expected queued batch to rotate between records, got file_id=%d", ptrs[0].FileID)
	}
	if totalBytes[0] <= 0 || totalBytes[1] <= 0 {
		t.Fatalf("totalBytes=%v want both > 0", totalBytes)
	}
	currentSegmentBytes := int64(0)
	for i, ptr := range ptrs {
		if ptr.FileID != fileID {
			currentSegmentBytes = totalBytes[i]
		}
	}
	if currentSegmentBytes <= 0 {
		t.Fatalf("did not find current-segment bytes, ptrs=%+v totalBytes=%v firstFileID=%d", ptrs, totalBytes, fileID)
	}
	if got := leaf.backendReadDirtySeq.Load(); got != 1 {
		t.Fatalf("backendReadDirtySeq=%d want 1 after rotating batch", got)
	}
	if !leaf.vlogDirty.Load() {
		t.Fatal("vlogDirty=false want true after rotating batch")
	}
	if got := leaf.vlogLiveBytes.Load(); got != currentSegmentBytes {
		t.Fatalf("current segment live bytes=%d want current request bytes %d", got, currentSegmentBytes)
	}
}
