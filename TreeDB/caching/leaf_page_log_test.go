package caching

import (
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
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
