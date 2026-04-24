package caching

import (
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type cachingLeafPageLog struct {
	db   *DB
	lane *lane
}

const compactLeafPayloadScratchMaxCap = page.PageSize

var compactLeafPayloadScratchPool = sync.Pool{
	New: func() any {
		return make([]byte, 0)
	},
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
	payloadScratch := getCompactLeafPayloadScratch()
	encodedLeafPage, compacted, err := valuelog.MaybeCompactLeafLogPayloadTo(payloadScratch, leafPage)
	if err != nil {
		releaseCompactLeafPayloadScratch(payloadScratch, nil, false)
		return page.LeafLogPtr{}, err
	}
	rid := l.db.nextRID.Add(1)
	// appendValueLogOneInternal writes/copies value before returning; release the
	// pooled scratch only after that synchronous append completes.
	ptr, retainPath, err := l.db.appendValueLogOneInternal(l.lane, 0, nil, rid, encodedLeafPage, journalDurabilityNone, false)
	releaseCompactLeafPayloadScratch(payloadScratch, encodedLeafPage, compacted)
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

func getCompactLeafPayloadScratch() []byte {
	scratch := compactLeafPayloadScratchPool.Get()
	if b, ok := scratch.([]byte); ok {
		return b[:0]
	}
	if scratch != nil {
		compactLeafPayloadScratchPool.Put(scratch)
	}
	return nil
}

func releaseCompactLeafPayloadScratch(payloadScratch, encodedLeafPage []byte, compacted bool) {
	putCompactLeafPayloadScratch(payloadScratch)
	if compacted && encodedLeafPage != nil && !sameSliceBacking(payloadScratch, encodedLeafPage) {
		putCompactLeafPayloadScratch(encodedLeafPage)
	}
}

func putCompactLeafPayloadScratch(buf []byte) {
	if buf == nil || cap(buf) == 0 || cap(buf) > compactLeafPayloadScratchMaxCap {
		return
	}
	compactLeafPayloadScratchPool.Put(buf[:0])
}

func sameSliceBacking(a, b []byte) bool {
	if cap(a) == 0 || cap(b) == 0 {
		return false
	}
	aFull := a[:cap(a)]
	bFull := b[:cap(b)]
	return &aFull[0] == &bFull[0]
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
