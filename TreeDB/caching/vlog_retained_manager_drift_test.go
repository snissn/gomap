package caching

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestCheckpoint_DeletesRetainedValueLogWhenBackendManagerForgotSegment(t *testing.T) {
	dir := t.TempDir()

	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:           256 << 20,
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		MemtableShards:           1,
		JournalLanes:             1,
		ValueLogCompression:      1,
		ValueLogPointerThreshold: 1,
		ValueLogMaxSegmentBytes:  4 << 10,
		ValueLogGenerationPolicy: 0,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	val := bytes.Repeat([]byte("a"), 2<<10)
	if err := db.Set([]byte("k1"), val); err != nil {
		t.Fatalf("Set k1: %v", err)
	}

	retainedPath := ""
	func() {
		l := &db.lanes[0]
		l.vlogMu.Lock()
		retainedPath = l.vlogPath
		l.vlogMu.Unlock()
	}()
	if retainedPath == "" {
		t.Fatalf("expected current value-log path")
	}
	if err := db.rotateValueLogLocked(&db.lanes[0]); err != nil {
		t.Fatalf("rotateValueLogLocked: %v", err)
	}
	seedRetainedPrunePressure(db, retainedPath, 2<<30)
	if _, err := os.Stat(retainedPath); err != nil {
		t.Fatalf("stat retained path: %v", err)
	}

	laneID, seq, valueLogFile, ok := parseLogSeq(filepath.Base(retainedPath))
	if !ok || !valueLogFile || laneID < 0 {
		t.Fatalf("parseLogSeq(%q) failed", retainedPath)
	}
	failID, err := valuelog.EncodeFileID(uint32(laneID), uint32(seq))
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	backend.markValueLogZombieID = failID
	backend.markValueLogZombieErr = fmt.Errorf("%w: %d", valuelog.ErrFileNotFound, failID)

	if err := db.Delete([]byte("k1")); err != nil {
		t.Fatalf("Delete k1: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	db.lastForegroundWriteUnixNano.Store(time.Now().Add(-2 * retainedPruneQuietWindow).UnixNano())
	db.waitForRetainedValueLogPrune()
	if err := db.backgroundError(); err != nil {
		t.Fatalf("backgroundError: %v", err)
	}
	if _, err := os.Stat(retainedPath); !os.IsNotExist(err) {
		t.Fatalf("expected retained path to be removed, stat err=%v", err)
	}
	if db.valueLogRetained(retainedPath) {
		t.Fatalf("expected retained path to be forgotten")
	}
}

func TestCleanupProcessedRetainedRewriteSources_RemovesBackendDeadRetainedSegment(t *testing.T) {
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	db, err := Open(dir, backend, Options{
		FlushThreshold:           256 << 20,
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		MemtableShards:           1,
		JournalLanes:             1,
		ValueLogCompression:      1,
		ValueLogPointerThreshold: 1,
		ValueLogMaxSegmentBytes:  4 << 10,
		ValueLogGenerationPolicy: 0,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	val := bytes.Repeat([]byte("b"), 2<<10)
	if err := db.Set([]byte("k1"), val); err != nil {
		t.Fatalf("Set k1: %v", err)
	}

	retainedPath := ""
	func() {
		l := &db.lanes[0]
		l.vlogMu.Lock()
		retainedPath = l.vlogPath
		l.vlogMu.Unlock()
	}()
	if retainedPath == "" {
		t.Fatalf("expected current value-log path")
	}
	if err := db.rotateValueLogLocked(&db.lanes[0]); err != nil {
		t.Fatalf("rotateValueLogLocked: %v", err)
	}
	if err := db.Delete([]byte("k1")); err != nil {
		t.Fatalf("Delete k1: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	laneID, seq, valueLogFile, ok := parseLogSeq(filepath.Base(retainedPath))
	if !ok || !valueLogFile || laneID < 0 {
		t.Fatalf("parseLogSeq(%q) failed", retainedPath)
	}
	fileID, err := valuelog.EncodeFileID(uint32(laneID), uint32(seq))
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	live, err := db.collectValueLogLiveIDsUntil(0)
	if err != nil {
		t.Fatalf("collectValueLogLiveIDsUntil: %v", err)
	}
	if _, ok := live[fileID]; ok {
		t.Fatalf("expected backend to stop referencing file %d", fileID)
	}
	if !db.valueLogRetained(retainedPath) {
		t.Fatalf("expected retained path to remain tracked before cleanup")
	}
	if _, err := os.Stat(retainedPath); err != nil {
		t.Fatalf("stat retained path before cleanup: %v", err)
	}

	snap := backend.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("expected backend snapshot")
	}
	defer func() { _ = snap.Close() }()
	if err := backend.MarkValueLogZombie(fileID); err != nil {
		t.Fatalf("MarkValueLogZombie: %v", err)
	}
	if err := backend.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}
	if _, err := os.Stat(retainedPath); err != nil {
		t.Fatalf("expected zombie segment to remain on disk while backend snapshot pins it, stat err=%v", err)
	}

	db.cleanupProcessedRetainedRewriteSources(vlogGenerationReasonRewriteResume, []uint32{fileID})

	if _, err := os.Stat(retainedPath); !os.IsNotExist(err) {
		t.Fatalf("expected cleanup to remove retained path, stat err=%v", err)
	}
	if db.valueLogRetained(retainedPath) {
		t.Fatalf("expected cleanup to forget retained path")
	}
}
