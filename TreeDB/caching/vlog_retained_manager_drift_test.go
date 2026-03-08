package caching

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

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
