package caching

import (
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type cachingLeafPageLog struct {
	db     *DB
	laneID int
}

var _ backenddb.LeafPageLog = (*cachingLeafPageLog)(nil)
var _ interface {
	AppendLeafPages(leafPages [][]byte) ([]page.ValuePtr, error)
} = (*cachingLeafPageLog)(nil)

func newCachingLeafPageLog(db *DB, laneID int) backenddb.LeafPageLog {
	return &cachingLeafPageLog{db: db, laneID: laneID}
}

func (l *cachingLeafPageLog) AppendLeafPage(leafPage []byte) (page.ValuePtr, error) {
	if l == nil || l.db == nil {
		return page.ValuePtr{}, errWALUnavailable
	}
	if l.laneID < 0 || l.laneID >= len(l.db.lanes) {
		return page.ValuePtr{}, errWALUnavailable
	}
	lane := &l.db.lanes[l.laneID]
	rid := l.db.nextRID.Add(1)
	ptr, retainPath, err := l.db.appendValueLogOneInternal(lane, 0, nil, rid, leafPage, journalDurabilityNone, false, true)
	if retainPath != "" {
		l.db.markValueLogRetain(retainPath)
	}
	return ptr, err
}

func (l *cachingLeafPageLog) AppendLeafPages(leafPages [][]byte) ([]page.ValuePtr, error) {
	if l == nil || l.db == nil {
		return nil, errWALUnavailable
	}
	if len(leafPages) == 0 {
		return nil, nil
	}
	if l.laneID < 0 || l.laneID >= len(l.db.lanes) {
		return nil, errWALUnavailable
	}
	lane := &l.db.lanes[l.laneID]
	startRID := l.db.nextRID.Add(uint64(len(leafPages))) - uint64(len(leafPages)) + 1
	records := make([]valuelog.Record, len(leafPages))
	for i := range leafPages {
		records[i] = valuelog.Record{
			RID:   startRID + uint64(i),
			Value: leafPages[i],
		}
	}
	return l.db.appendValueLog(lane, 0, nil, records, journalDurabilityNone, true)
}

func (l *cachingLeafPageLog) Flush() error {
	if l == nil || l.db == nil {
		return errWALUnavailable
	}
	return l.db.flushValueLog(l.laneID)
}

func (l *cachingLeafPageLog) Sync() error {
	if l == nil || l.db == nil {
		return errWALUnavailable
	}
	if err := l.db.flushValueLog(l.laneID); err != nil {
		return err
	}
	if l.db.relaxedSync {
		return nil
	}
	return l.db.syncValueLog(l.laneID)
}
