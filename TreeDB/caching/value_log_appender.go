package caching

import (
	"fmt"
	"os"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type cachingValueLogAppender struct {
	db   *DB
	lane *lane
}

var _ backenddb.ValueLogAppender = (*cachingValueLogAppender)(nil)
var _ backenddb.ValueLogExternalRefFlusher = (*cachingValueLogAppender)(nil)
var _ backenddb.ValueLogRecordReader = (*cachingValueLogAppender)(nil)

func newCachingValueLogAppender(db *DB, l *lane) backenddb.ValueLogAppender {
	return &cachingValueLogAppender{db: db, lane: l}
}

func (a *cachingValueLogAppender) AppendValues(values [][]byte) ([]page.ValuePtr, error) {
	if a == nil || a.db == nil || a.lane == nil {
		return nil, errWALUnavailable
	}
	if len(values) == 0 {
		return nil, nil
	}
	if err := durabilitycut.EmitBasic(durabilitycut.BeforeDependencyAppend, durabilitycut.ResourceValueLog, a.db.dir); err != nil {
		return nil, err
	}
	startRID, err := a.db.ReserveValueLogRIDs(len(values))
	if err != nil {
		return nil, err
	}
	records := getValueLogRecords(len(values))
	defer putValueLogRecordsNoClear(records)
	for i := range values {
		records[i] = valuelog.Record{
			RID:   startRID + uint64(i),
			Value: values[i],
		}
	}
	ptrs, err := a.db.appendValueLogForRecords(a.lane, records, journalDurabilityNone)
	if err != nil {
		return nil, err
	}
	if err := a.emitDependencyAppend(ptrs); err != nil {
		putValueLogPtrs(ptrs)
		return nil, err
	}
	if len(ptrs) != len(values) {
		putValueLogPtrs(ptrs)
		return nil, fmt.Errorf("cachingdb: value-log appender returned %d ptrs for %d values", len(ptrs), len(values))
	}
	out := append([]page.ValuePtr(nil), ptrs...)
	putValueLogPtrs(ptrs)
	return out, nil
}

func (a *cachingValueLogAppender) ReadValueLogRecordAppend(ptr page.ValuePtr, dst []byte) ([]byte, error) {
	if a == nil || a.db == nil {
		return nil, errWALUnavailable
	}
	return a.db.readValueLogAppend(nil, ptr, dst)
}

func (a *cachingValueLogAppender) emitDependencyAppend(ptrs []page.ValuePtr) error {
	if !durabilitycut.Enabled() {
		return nil
	}
	paths := make([]string, 0, len(ptrs))
	seen := make(map[string]struct{}, len(ptrs))
	for _, ptr := range ptrs {
		path := valuelog.SegmentPath(a.db.valueLogDir, ptr.FileID)
		if lane := a.db.valueLogLaneForFileID(ptr.FileID); lane != nil {
			if dir := a.db.valueLogDirForLane(lane); dir != "" {
				path = valuelog.SegmentPath(dir, ptr.FileID)
			}
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		paths = append(paths, path)
	}
	return durabilitycut.Emit(durabilitycut.Event{Point: durabilitycut.AfterDependencyAppend, Resource: durabilitycut.ResourceValueLog, Root: a.db.dir, Paths: paths})
}

func (a *cachingValueLogAppender) ReserveRIDs(count int) (uint64, error) {
	if a == nil || a.db == nil || a.lane == nil {
		return 0, errWALUnavailable
	}
	return a.db.ReserveValueLogRIDs(count)
}

func (a *cachingValueLogAppender) Flush() error {
	if a == nil || a.db == nil || a.lane == nil {
		return errWALUnavailable
	}
	return a.db.flushValueLogLane(a.lane)
}

func (a *cachingValueLogAppender) Sync() error {
	if a == nil || a.db == nil || a.lane == nil {
		return errWALUnavailable
	}
	if err := a.db.flushValueLogLane(a.lane); err != nil {
		return err
	}
	if a.db.relaxedSync {
		return nil
	}
	return a.db.syncValueLogLane(a.lane)
}

func (a *cachingValueLogAppender) FlushValueLogExternalRefs(fileIDs []uint32, sync bool) error {
	if a == nil || a.db == nil || a.lane == nil {
		return errWALUnavailable
	}
	if len(fileIDs) == 0 {
		return a.db.flushPendingValueLogLanes(sync, valueLogSyncPathPendingBarrier)
	}
	seenLanes := make(map[*lane]struct{}, len(fileIDs))
	activeFileIDs := make(map[uint32]struct{}, len(fileIDs))
	for _, fileID := range fileIDs {
		if fileID == 0 || !page.IsValueLogFileID(fileID) {
			continue
		}
		l := a.db.valueLogLaneForFileID(fileID)
		if l == nil {
			continue
		}
		if _, ok := seenLanes[l]; !ok {
			seenLanes[l] = struct{}{}
			activeFileID, err := a.db.flushValueLogLaneForExternalRefFileIDs(l, fileIDs, sync)
			if err != nil {
				return err
			}
			if activeFileID != 0 {
				activeFileIDs[activeFileID] = struct{}{}
			}
		}
	}
	if !sync {
		return nil
	}
	seenFiles := make(map[uint32]struct{}, len(fileIDs))
	for _, fileID := range fileIDs {
		if fileID == 0 || !page.IsValueLogFileID(fileID) {
			continue
		}
		if _, ok := activeFileIDs[fileID]; ok {
			continue
		}
		if _, ok := seenFiles[fileID]; ok {
			continue
		}
		seenFiles[fileID] = struct{}{}
		if err := a.syncValueLogExternalRefSegment(fileID); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) flushValueLogLaneForExternalRefFileIDs(l *lane, fileIDs []uint32, sync bool) (uint32, error) {
	if db == nil || l == nil {
		return 0, errWALUnavailable
	}
	if !db.splitValueLogEnabled() {
		if err := db.flushValueLogLane(l); err != nil {
			return 0, err
		}
		if sync && !db.relaxedSync {
			if err := db.syncValueLogLane(l); err != nil {
				return 0, err
			}
		}
		_, activeFileID, _ := cachingValueLogSegmentForLane(l)
		return activeFileID, nil
	}
	waitStart := time.Now()
	l.vlogMu.Lock()
	waited := time.Since(waitStart)
	defer l.vlogMu.Unlock()

	var activeFileID uint32
	if l.vlogPath != "" && l.vlogSeq > 0 {
		if fileID, err := valuelog.EncodeFileID(uint32(l.id), uint32(l.vlogSeq)); err == nil {
			activeFileID = fileID
		}
	}
	w := l.vlog
	if w == nil {
		// A path/sequence without an active writer is not an active durability
		// owner. Return no active ID so the caller directly syncs the segment.
		return 0, nil
	}
	if l.vlogDirty.Load() || valueWriterPendingBytes(w) > 0 {
		start := time.Now()
		err := w.Flush()
		if db.testOnVlogFlush != nil {
			db.testOnVlogFlush(int(l.id))
		}
		db.debugVlogTiming("vlog_flush", int(l.id), "vlogMu", waited, time.Since(start))
		if err != nil {
			return activeFileID, err
		}
		l.vlogDirty.Store(false)
		if db.relaxedSync {
			l.vlogSyncPending.Store(false)
		}
		l.backendReadFlushedSeq.Store(l.backendReadDirtySeq.Load())
	}
	if !sync || db.relaxedSync {
		return activeFileID, nil
	}
	if db.valueLogMaterializationSyncCoversFileIDMuHeld(l, w, activeFileID, fileIDs) {
		return activeFileID, nil
	}
	start := time.Now()
	err := w.Sync()
	elapsed := time.Since(start)
	db.observeValueLogSync(valueLogSyncPathExternalRef, waited, elapsed, err)
	if db.testOnVlogSync != nil {
		db.testOnVlogSync(int(l.id))
	}
	db.debugVlogTiming("vlog_sync", int(l.id), "vlogMu", waited, elapsed)
	if err == nil {
		l.vlogDirty.Store(false)
		l.vlogSyncPending.Store(false)
		l.backendReadFlushedSeq.Store(l.backendReadDirtySeq.Load())
	}
	return activeFileID, err
}

func valueWriterPendingBytes(w valueWriter) int {
	if pending, ok := w.(interface{ PendingBytes() int }); ok {
		return pending.PendingBytes()
	}
	return 0
}

func (db *DB) valueLogMaterializationSyncCoversFileIDMuHeld(l *lane, w valueWriter, activeFileID uint32, fileIDs []uint32) bool {
	// Durable batch writes retain l.syncing from value materialization through
	// command-WAL external-reference ordering. A successful materialization
	// sync therefore covers the whole append-only active file at the recorded
	// boundary. Reuse is safe only while the writer identity (file/sequence) and
	// size remain unchanged; every uncertainty falls back to another sync.
	if db == nil || l == nil || w == nil || activeFileID == 0 || !l.syncing.Load() ||
		!l.vlogMaterializationSyncValid || l.vlogMaterializationSyncFileID != activeFileID ||
		l.vlogMaterializationSyncSeq != l.vlogSeq || l.vlogMaterializationSyncSize < 0 ||
		w.Size() != l.vlogMaterializationSyncSize {
		return false
	}
	for _, fileID := range fileIDs {
		if fileID == activeFileID {
			return true
		}
	}
	return false
}

func (a *cachingValueLogAppender) syncValueLogExternalRefSegment(fileID uint32) (retErr error) {
	if a == nil || a.db == nil || fileID == 0 {
		return errWALUnavailable
	}
	start := time.Now()
	defer func() {
		a.db.observeValueLogSync(valueLogSyncPathExternalRef, 0, time.Since(start), retErr)
	}()
	path := valuelog.SegmentPath(a.db.valueLogDir, fileID)
	if l := a.db.valueLogLaneForFileID(fileID); l != nil {
		dir := a.db.valueLogDirForLane(l)
		if dir != "" {
			path = valuelog.SegmentPath(dir, fileID)
		}
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return a.db.syncRotatedValueLogFile(f)
}

func (db *DB) syncRotatedValueLogFile(f *os.File) error {
	if db == nil || f == nil {
		return errWALUnavailable
	}
	// Rotated segments no longer have an active writer whose DurabilityStats can
	// own this direct file-sync observation. Keep this telemetry DB-owned so the
	// aggregate can add it to active-writer stats exactly once.
	start := time.Now()
	var err error
	if db.testSyncRotatedValueLogFile != nil {
		err = db.testSyncRotatedValueLogFile(f)
	} else {
		err = f.Sync()
	}
	db.valueLogRotatedFileSyncCalls.Add(1)
	if ns := time.Since(start).Nanoseconds(); ns > 0 {
		db.valueLogRotatedFileSyncNs.Add(uint64(ns))
	}
	if err != nil {
		db.valueLogRotatedFileSyncErrors.Add(1)
	}
	return err
}

func cachingValueLogSegmentForLane(l *lane) (string, uint32, bool) {
	if l == nil {
		return "", 0, false
	}
	l.vlogMu.Lock()
	path := l.vlogPath
	seq := l.vlogSeq
	l.vlogMu.Unlock()
	if path == "" || seq <= 0 {
		return "", 0, false
	}
	fileID, err := valuelog.EncodeFileID(uint32(l.id), uint32(seq))
	if err != nil {
		return "", 0, false
	}
	return path, fileID, true
}

func (a *cachingValueLogAppender) CurrentValueLogSegment() (string, uint32, bool) {
	if a == nil || a.db == nil || a.lane == nil {
		return "", 0, false
	}
	return cachingValueLogSegmentForLane(a.lane)
}
