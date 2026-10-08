package caching

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCachingLeafPageLogGenerationHandoffAllPhysicalWorkersAndIdle(t *testing.T) {
	db, captured, _ := openCachingLeafPageLogLaneTestDB(t)
	defer db.Close()
	backend := captured.BackendDB.(*backenddb.DB)
	provider := captured.leafLog.(backenddb.LeafPageLogLaneProvider)
	var ptrs []page.LeafLogPtr
	var pages [][]byte
	oldMax := uint32(0)
	for worker := 0; worker < 4; worker++ {
		log, ok := provider.LeafPageLogLane(worker)
		if !ok {
			t.Fatalf("missing physical worker %d", worker)
		}
		payload := testLeafPageBytes(fmt.Sprintf("physical-worker-%d", worker))
		ptr, err := log.AppendLeafPage(payload)
		if err != nil {
			t.Fatal(err)
		}
		ptrs = append(ptrs, ptr)
		pages = append(pages, payload)
	}
	oldIDs := captured.leafLog.(backenddb.LeafPageLogCurrentSegmentProvider)
	before, err := oldIDs.CurrentLeafPageLogSegmentsSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range db.leafLogAppendLanesSnapshot() {
		if uint32(l.vlogSeq) > oldMax {
			oldMax = uint32(l.vlogSeq)
		}
	}
	if err := backend.AdvanceLeafPageLogGeneration(context.Background()); err != nil {
		t.Fatal(err)
	}
	current := make(map[uint32]bool)
	for _, id := range db.valueLogReader.CurrentWritableFileIDs() {
		current[id] = true
	}
	for _, seg := range before {
		if current[seg.FileID] {
			t.Fatalf("old physical writer %d remains current", seg.FileID)
		}
	}
	after, err := oldIDs.CurrentLeafPageLogSegmentsSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 4 {
		t.Fatalf("physical writers=%d want 4", len(after))
	}
	for _, l := range db.leafLogAppendLanesSnapshot() {
		if uint32(l.vlogSeq) <= oldMax+1 {
			t.Fatalf("writer seq=%d failed reserved floor>%d", l.vlogSeq, oldMax)
		}
		if l.vlogHandoffTotal.Load() != 1 || l.vlogRotateThresholdTotal.Load() != 0 {
			t.Fatalf("handoff=%d threshold=%d", l.vlogHandoffTotal.Load(), l.vlogRotateThresholdTotal.Load())
		}
	}
	for i, ptr := range ptrs {
		got, err := db.ReadValueLogRecord(ptr.ValuePtr())
		if err != nil || !bytes.Equal(got, pages[i]) {
			t.Fatalf("old physical pointer %d: err=%v equal=%t", i, err, bytes.Equal(got, pages[i]))
		}
	}
	created, err := captured.leafLog.(backenddb.LeafPageLogCreatedSegmentProvider).CreatedLeafPageLogSegmentsSnapshot()
	if err != nil || len(created) != 0 {
		t.Fatalf("registered inventory retained: %v %v", created, err)
	}
	frontier := db.leafLogAppendSeq.Load()
	// Nothing has appended to the replacement writers. Public no-work callers
	// must retain their IDs instead of generating another set of empty files.
	if err := backend.AdvanceLeafPageLogGeneration(context.Background()); err != nil {
		t.Fatal(err)
	}
	if db.leafLogAppendSeq.Load() != frontier {
		t.Fatal("idle handoff advanced sequence")
	}
	idle, err := oldIDs.CurrentLeafPageLogSegmentsSnapshot()
	if err != nil || len(idle) != len(after) {
		t.Fatalf("idle inventory=%v err=%v", idle, err)
	}
	for i := range after {
		if idle[i] != after[i] {
			t.Fatal("idle handoff replaced a current writer")
		}
	}
	// A worker created after release must allocate beyond the transferred cut.
	log, ok := provider.LeafPageLogLane(5)
	if !ok {
		t.Fatal("new physical worker unavailable")
	}
	if _, err := log.AppendLeafPage(testLeafPageBytes("post-handoff-worker")); err != nil {
		t.Fatal(err)
	}
	for _, l := range db.leafLogAppendLanesSnapshot() {
		if l != nil && l.vlog != nil && uint32(l.vlogSeq) <= oldMax+1 {
			t.Fatal("new worker reopened old frontier")
		}
	}
}

func TestCachingLeafPageLogGenerationHandoffEmptyAndCancellation(t *testing.T) {
	db, captured, _ := openCachingLeafPageLogLaneTestDB(t)
	defer db.Close()
	backend := captured.BackendDB.(*backenddb.DB)
	before := db.leafLogAppendSeq.Load()
	if err := backend.AdvanceLeafPageLogGeneration(context.Background()); err != nil {
		t.Fatal(err)
	}
	if db.leafLogAppendSeq.Load() != before {
		t.Fatal("empty handoff reserved a sequence")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := backend.AdvanceLeafPageLogGeneration(ctx); err != context.Canceled {
		t.Fatalf("cancelled handoff: %v", err)
	}
	if !db.flushMu.TryLock() {
		t.Fatal("cancelled handoff retained flush owner")
	}
	db.flushMu.Unlock()
	if !db.writeMu.TryLock() {
		t.Fatal("cancelled handoff retained foreground owner")
	}
	db.writeMu.Unlock()
}

func TestCachingLeafPageLogGenerationHandoffPreservesStableFilePin(t *testing.T) {
	db, captured, _ := openCachingLeafPageLogLaneTestDB(t)
	defer db.Close()
	backend := captured.BackendDB.(*backenddb.DB)
	log := captured.leafLog.(backenddb.LeafPageStableLog)
	ptr, resources, err := log.AppendLeafPageWithStableResources(testLeafPageBytes("stable-old-producer"))
	if err != nil {
		t.Fatal(err)
	}
	if resources == nil {
		t.Fatal("missing stable builder pin")
	}
	defer resources.Release()
	segments, err := captured.leafLog.(backenddb.LeafPageLogCurrentSegmentProvider).CurrentLeafPageLogSegmentsSnapshot()
	if err != nil || len(segments) != 1 {
		t.Fatalf("current=%v err=%v", segments, err)
	}
	if err := backend.AdvanceLeafPageLogGeneration(context.Background()); err != nil {
		t.Fatal(err)
	}
	id := ptr.ValuePtr().FileID
	if err := db.valueLogReader.MarkZombie(id); err != nil {
		t.Fatal(err)
	}
	if _, err := db.valueLogReader.RemoveSegmentIfUnpinned(id); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(segments[0].Path); err != nil {
		t.Fatalf("handoff released stable builder file: %v", err)
	}
}

func TestCachingLeafPageLogRetirementForgetsExactClosedAccounting(t *testing.T) {
	db, captured, _ := openCachingLeafPageLogLaneTestDB(t)
	defer db.Close()
	l := db.leafLogAppendLaneForWorkerIndex(0)
	path := db.leafLogDir + "/already-physically-removed-leaf"
	l.vlogMu.Lock()
	l.vlogClosedSizes = map[string]int64{path: 123}
	l.vlogClosedBytes.Store(123)
	l.vlogMu.Unlock()
	db.valueLogRetainedClosedBytes.Store(123)
	db.markValueLogRetain(path)
	segments := []backenddb.LeafPageLogSegment{{Path: path, FileID: 1}}
	// Physical GC receipts also retire the cache's closed-path inventory.
	observer := captured.leafLog.(backenddb.LeafPageLogRetiredSegmentObserver)
	observer.LeafPageLogSegmentsRetired(segments)
	observer.LeafPageLogSegmentsRetired(segments)
	l.vlogMu.Lock()
	closed := len(l.vlogClosedSizes)
	l.vlogMu.Unlock()
	if closed != 0 || l.vlogClosedBytes.Load() != 0 || db.valueLogRetainedClosedBytes.Load() != 0 || db.valueLogRetained(path) {
		t.Fatalf("retired physical path remains in producer accounting: %d", closed)
	}
}

func TestCachingLeafPageLogGenerationHandoffCloseWaitsForOwnerCompletion(t *testing.T) {
	db, _, _ := openCachingLeafPageLogLaneTestDB(t)
	owner, err := db.beginLeafPageLogGenerationHandoff(context.Background())
	if err != nil || owner == nil {
		t.Fatalf("owner=%v err=%v", owner, err)
	}
	defer owner.Release()
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for !db.closing.Load() {
		select {
		case <-deadline.C:
			t.Fatal("Close did not enter closing")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	select {
	case err := <-closed:
		t.Fatalf("Close passed active owner: %v", err)
	default:
	}
	owner.Release()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-deadline.C:
		t.Fatal("Close did not finish after owner release")
	}
}
