package caching

import (
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
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
		if sync {
			return a.Sync()
		}
		return a.Flush()
	}
	seen := make(map[int]struct{}, len(fileIDs))
	for _, fileID := range fileIDs {
		if fileID == 0 || !page.IsValueLogFileID(fileID) {
			continue
		}
		laneID, _ := valuelog.DecodeFileID(fileID)
		if laneID >= uint32(len(a.db.lanes)) {
			continue
		}
		id := int(laneID)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		l := &a.db.lanes[id]
		if err := a.db.flushValueLogLane(l); err != nil {
			return err
		}
		if sync && !a.db.relaxedSync {
			if err := a.db.syncValueLogLane(l); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *cachingValueLogAppender) CurrentValueLogSegment() (string, uint32, bool) {
	if a == nil || a.db == nil || a.lane == nil {
		return "", 0, false
	}
	l := a.lane
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
