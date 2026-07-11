package caching

import (
	"fmt"
	"os"

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
	if err := durabilitycut.EmitBasic(durabilitycut.AfterDependencyAppend, durabilitycut.ResourceValueLog, a.db.dir); err != nil {
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
		return a.db.flushPendingValueLogLanes(sync)
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
			if err := a.db.flushValueLogLane(l); err != nil {
				return err
			}
			if sync && !a.db.relaxedSync {
				if err := a.db.syncValueLogLane(l); err != nil {
					return err
				}
			}
		}
		if _, activeFileID, ok := cachingValueLogSegmentForLane(l); ok {
			activeFileIDs[activeFileID] = struct{}{}
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

func (a *cachingValueLogAppender) syncValueLogExternalRefSegment(fileID uint32) error {
	if a == nil || a.db == nil || fileID == 0 {
		return errWALUnavailable
	}
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
	return f.Sync()
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
