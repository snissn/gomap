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
