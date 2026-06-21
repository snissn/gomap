package db

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type leafLaneCountingLog struct {
	inner  LeafPageLog
	counts *leafLaneCountingLogCounts
}

type leafLaneCountingLogCounts struct {
	flush atomic.Uint32
	sync  atomic.Uint32
	close atomic.Uint32
}

func (l *leafLaneCountingLog) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	if l == nil || l.inner == nil {
		return page.LeafLogPtr{}, errors.New("leaf page log unavailable")
	}
	return l.inner.AppendLeafPage(leafPage)
}

func (l *leafLaneCountingLog) Flush() error {
	if l == nil || l.inner == nil {
		return nil
	}
	if l.counts != nil {
		l.counts.flush.Add(1)
	}
	return l.inner.Flush()
}

func (l *leafLaneCountingLog) Sync() error {
	if l == nil || l.inner == nil {
		return nil
	}
	if l.counts != nil {
		l.counts.sync.Add(1)
	}
	return l.inner.Sync()
}

func (l *leafLaneCountingLog) Close() error {
	if l == nil || l.inner == nil {
		return nil
	}
	if l.counts != nil {
		l.counts.close.Add(1)
	}
	closer, ok := l.inner.(interface{ Close() error })
	if !ok {
		return nil
	}
	return closer.Close()
}

func (l *leafLaneCountingLog) CurrentValueLogSegment() (string, uint32, bool) {
	if l == nil || l.inner == nil {
		return "", 0, false
	}
	provider, ok := l.inner.(leafPageLogCurrentSegmentProvider)
	if !ok {
		return "", 0, false
	}
	return provider.CurrentValueLogSegment()
}

func (l *leafLaneCountingLog) CurrentLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error) {
	if l == nil || l.inner == nil {
		return nil, nil
	}
	return leafPageLogCurrentSegments(l.inner)
}

func (l *leafLaneCountingLog) CreatedLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error) {
	if l == nil || l.inner == nil {
		return nil, nil
	}
	return leafPageLogCreatedSegments(l.inner)
}

func (l *leafLaneCountingLog) MarkLeafPageLogSegmentsRegistered(segments []LeafPageLogSegment) {
	if l == nil || l.inner == nil {
		return
	}
	observer, ok := l.inner.(LeafPageLogSegmentRegistrationObserver)
	if !ok {
		return
	}
	observer.MarkLeafPageLogSegmentsRegistered(segments)
}

func (l *leafLaneCountingLog) cloneLeafPageLogLane(seqAlloc *leafLogSeqAllocator) (LeafPageLog, error) {
	if l == nil || l.inner == nil {
		return nil, errors.New("leaf page log unavailable")
	}
	cloner, ok := l.inner.(leafPageLogLaneCloner)
	if !ok {
		return nil, errors.New("leaf page log lane cloning unavailable")
	}
	clone, err := cloner.cloneLeafPageLogLane(seqAlloc)
	if err != nil {
		return nil, err
	}
	return &leafLaneCountingLog{inner: clone, counts: l.counts}, nil
}

func testLeafPageBytes(label string) []byte {
	buf := bytes.Repeat([]byte{0}, page.PageSize)
	copy(buf, label)
	return buf
}

func openLeafPageLogLaneTestDB(t *testing.T) (*DB, LeafPageLogCloser) {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	leafLog, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{Compression: ValueLogCompressionOff})
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewStandaloneLeafPageLog: %v", err)
	}
	db.SetLeafPageLog(leafLog)
	return db, leafLog
}

func TestLeafPageLogLaneSelectionAppendsUniqueReadablePtrs(t *testing.T) {
	db, _ := openLeafPageLogLaneTestDB(t)

	defaultPage := testLeafPageBytes("lane-0-default")
	defaultPtr, err := db.leafPageLog.AppendLeafPage(defaultPage)
	if err != nil {
		t.Fatalf("default AppendLeafPage: %v", err)
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
			appender, ok := db.leafPageLogLaneForWorkerIndex(lane)
			if !ok || appender == nil {
				t.Errorf("leafPageLogLaneForWorkerIndex(%d) unavailable", lane)
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

	seen := map[uint32]struct{}{}
	checks := []laneResult{{lane: 0, ptr: defaultPtr, want: defaultPage}}
	for res := range results {
		checks = append(checks, res)
	}
	if len(checks) != extraLanes+1 {
		t.Fatalf("got %d appended pages, want %d", len(checks), extraLanes+1)
	}
	for _, res := range checks {
		fileID := res.ptr.ValuePtr().FileID
		laneID, seq := valuelog.DecodeFileID(fileID)
		if laneID != rewriteLeafLogLaneID {
			t.Fatalf("lane %d fileID lane=%d want=%d (fileID=%d seq=%d)", res.lane, laneID, rewriteLeafLogLaneID, fileID, seq)
		}
		if _, ok := seen[fileID]; ok {
			t.Fatalf("duplicate leaf log fileID %d", fileID)
		}
		seen[fileID] = struct{}{}
	}

	closer, ok := db.leafPageLog.(interface{ Close() error })
	if !ok {
		t.Fatal("leaf log missing Close")
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("Close leaf log: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(Options{Dir: db.dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("reopen Open: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	for _, res := range checks {
		got, err := reopened.valueLogManager.Read(res.ptr.ValuePtr())
		if err != nil {
			t.Fatalf("reopen read lane %d: %v", res.lane, err)
		}
		if !bytes.Equal(got, res.want) {
			t.Fatalf("reopen read lane %d mismatch", res.lane)
		}
	}
}

func TestLeafPageLogLaneSnapshotsAggregateAndMarkPerLane(t *testing.T) {
	db, _ := openLeafPageLogLaneTestDB(t)
	defer func() { _ = db.Close() }()

	appenders := []LeafPageLog{db.leafPageLog}
	for lane := 1; lane <= 2; lane++ {
		appender, ok := db.leafPageLogLaneForWorkerIndex(lane)
		if !ok || appender == nil {
			t.Fatalf("leafPageLogLaneForWorkerIndex(%d) unavailable", lane)
		}
		appenders = append(appenders, appender)
	}
	for i, appender := range appenders {
		if _, err := appender.AppendLeafPage(testLeafPageBytes(fmt.Sprintf("snapshot-%d", i))); err != nil {
			t.Fatalf("AppendLeafPage lane %d: %v", i, err)
		}
	}

	created, err := leafPageLogCreatedSegments(db.leafPageLog)
	if err != nil {
		t.Fatalf("CreatedLeafPageLogSegmentsSnapshot: %v", err)
	}
	if len(created) != len(appenders) {
		t.Fatalf("created segments=%d want %d", len(created), len(appenders))
	}
	current, err := leafPageLogCurrentSegments(db.leafPageLog)
	if err != nil {
		t.Fatalf("CurrentLeafPageLogSegmentsSnapshot: %v", err)
	}
	if len(current) != len(appenders) {
		t.Fatalf("current segments=%d want %d", len(current), len(appenders))
	}

	markLeafPageLogSegmentsRegistered(db.leafPageLog, created[:1])
	afterFirstMark, err := leafPageLogCreatedSegments(db.leafPageLog)
	if err != nil {
		t.Fatalf("CreatedLeafPageLogSegmentsSnapshot after first mark: %v", err)
	}
	if len(afterFirstMark) != len(appenders)-1 {
		t.Fatalf("created segments after first mark=%d want %d", len(afterFirstMark), len(appenders)-1)
	}
	markLeafPageLogSegmentsRegistered(db.leafPageLog, afterFirstMark)
	finalCreated, err := leafPageLogCreatedSegments(db.leafPageLog)
	if err != nil {
		t.Fatalf("CreatedLeafPageLogSegmentsSnapshot after final mark: %v", err)
	}
	if len(finalCreated) != 0 {
		t.Fatalf("created segments after final mark=%d want 0", len(finalCreated))
	}
}

func TestLeafPageLogLanes_FlushSyncCloseTouchAllLanes(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	baseLog, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{Compression: ValueLogCompressionOff})
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewStandaloneLeafPageLog: %v", err)
	}
	counts := &leafLaneCountingLogCounts{}
	db.SetLeafPageLog(&leafLaneCountingLog{inner: baseLog, counts: counts})

	for lane := 0; lane < 3; lane++ {
		appender, ok := db.leafPageLogLaneForWorkerIndex(lane)
		if !ok || appender == nil {
			t.Fatalf("leafPageLogLaneForWorkerIndex(%d) unavailable", lane)
		}
		if _, err := appender.AppendLeafPage(testLeafPageBytes(fmt.Sprintf("touch-%d", lane))); err != nil {
			t.Fatalf("AppendLeafPage lane %d: %v", lane, err)
		}
	}

	if err := db.leafPageLog.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := counts.flush.Load(); got != 3 {
		t.Fatalf("Flush count=%d want 3", got)
	}
	if err := db.leafPageLog.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if got := counts.sync.Load(); got != 3 {
		t.Fatalf("Sync count=%d want 3", got)
	}
	closer, ok := db.leafPageLog.(interface{ Close() error })
	if !ok {
		t.Fatal("leaf log missing Close")
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("Close leaf log: %v", err)
	}
	if got := counts.close.Load(); got != 3 {
		t.Fatalf("Close count=%d want 3", got)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("DB Close: %v", err)
	}
}
