package caching

import (
	"context"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type cachingLeafPageLog struct {
	db   *DB
	lane *lane
}

var _ backenddb.LeafPageLog = (*cachingLeafPageLog)(nil)

func newCachingLeafPageLog(db *DB, l *lane) backenddb.LeafPageLog {
	return &cachingLeafPageLog{db: db, lane: l}
}

func (db *DB) noteLeafGenerationRecordLength(ptr page.ValuePtr) {
	if db == nil || db.backend == nil || ptr.FileID == 0 || ptr.Offset == 0 {
		return
	}
	if notifier, ok := db.backend.(interface{ NoteLeafGenerationRecordLength(page.ValuePtr) }); ok {
		notifier.NoteLeafGenerationRecordLength(ptr)
	}
}

func (l *cachingLeafPageLog) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	if l == nil || l.db == nil || l.lane == nil {
		return page.LeafLogPtr{}, errWALUnavailable
	}
	encodedLeafPage, _, err := valuelog.MaybeCompactLeafLogPayload(leafPage)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	dictID := uint64(0)
	if l.db.dictStore != nil && l.db.valueLogDictAllowOuterLeaf() {
		dictID, err = l.db.currentDictIDForClass(context.Background(), vlogDictClassOuterLeaf)
		if err != nil {
			return page.LeafLogPtr{}, err
		}
	}
	rid := l.db.nextRID.Add(1)
	ptr, retainPath, err := l.db.appendValueLogOneInternal(l.lane, dictID, nil, rid, encodedLeafPage, journalDurabilityNone, false)
	if retainPath != "" {
		l.db.markValueLogRetain(retainPath)
	}
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	leafPtr, convErr := page.LeafLogPtrFromValuePtr(ptr)
	if convErr != nil {
		return page.LeafLogPtr{}, convErr
	}
	l.db.noteLeafGenerationRecordLength(ptr)
	return leafPtr, nil
}

func (l *cachingLeafPageLog) Flush() error {
	if l == nil || l.db == nil || l.lane == nil {
		return errWALUnavailable
	}
	return l.db.flushValueLogLane(l.lane)
}

func (l *cachingLeafPageLog) Sync() error {
	if l == nil || l.db == nil || l.lane == nil {
		return errWALUnavailable
	}
	if err := l.db.flushValueLogLane(l.lane); err != nil {
		return err
	}
	if l.db.relaxedSync {
		return nil
	}
	return l.db.syncValueLogLane(l.lane)
}

// CurrentValueLogSegment reports the lane's current writable value-log segment.
// Backend publish paths use this to avoid full manager refresh scans.
func (l *cachingLeafPageLog) CurrentValueLogSegment() (string, uint32, bool) {
	if l == nil || l.db == nil || l.lane == nil {
		return "", 0, false
	}
	lane := l.lane
	lane.vlogMu.Lock()
	path := lane.vlogPath
	seq := lane.vlogSeq
	lane.vlogMu.Unlock()
	if path == "" || seq <= 0 {
		return "", 0, false
	}
	fileID, err := valuelog.EncodeFileID(uint32(lane.id), uint32(seq))
	if err != nil {
		return "", 0, false
	}
	return path, fileID, true
}
