package caching

import (
	"fmt"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type cachingLeafPageLog struct {
	db   *DB
	lane *lane
}

var _ backenddb.LeafPageLog = (*cachingLeafPageLog)(nil)
var _ backenddb.LeafPageBatchLog = (*cachingLeafPageLog)(nil)

var compactLeafLogPayloadScratchPool sync.Pool

type compactLeafLogPayloadScratch struct {
	buf []byte
}

func newCachingLeafPageLog(db *DB, l *lane) backenddb.LeafPageLog {
	return &cachingLeafPageLog{db: db, lane: l}
}

func (l *cachingLeafPageLog) ProtectedLeafGenerationRootIDs() []uint64 {
	if l == nil || l.db == nil {
		return nil
	}
	protectedRootIDs, _ := l.db.publishedLeafGenerationProtectionIDs()
	return protectedRootIDs
}

func (l *cachingLeafPageLog) ProtectedLeafGenerationSystemRootIDs() []uint64 {
	if l == nil || l.db == nil {
		return nil
	}
	_, protectedSystemRootIDs := l.db.publishedLeafGenerationProtectionIDs()
	return protectedSystemRootIDs
}

func (l *cachingLeafPageLog) ProtectedLeafGenerationRootIDPair() ([]uint64, []uint64) {
	if l == nil || l.db == nil {
		return nil, nil
	}
	return l.db.publishedLeafGenerationProtectionIDs()
}

func (l *cachingLeafPageLog) CreatedLeafPageLogSegmentsSnapshot() ([]backenddb.LeafPageLogSegment, error) {
	if l == nil || l.lane == nil {
		return nil, nil
	}
	lane := l.lane
	lane.vlogMu.Lock()
	defer lane.vlogMu.Unlock()
	if len(lane.vlogCreatedSegments) == 0 {
		return nil, nil
	}
	out := make([]backenddb.LeafPageLogSegment, 0, len(lane.vlogCreatedSegments))
	seen := make(map[uint32]struct{}, len(lane.vlogCreatedSegments))
	for _, seg := range lane.vlogCreatedSegments {
		if seg.path == "" || seg.fileID == 0 {
			continue
		}
		if _, ok := seen[seg.fileID]; ok {
			continue
		}
		seen[seg.fileID] = struct{}{}
		out = append(out, backenddb.LeafPageLogSegment{Path: seg.path, FileID: seg.fileID})
	}
	return out, nil
}

func (l *cachingLeafPageLog) MarkLeafPageLogSegmentsRegistered(segments []backenddb.LeafPageLogSegment) {
	if l == nil || l.lane == nil || len(segments) == 0 {
		return
	}
	registered := make(map[uint32]struct{}, len(segments))
	for _, seg := range segments {
		if seg.FileID == 0 {
			continue
		}
		registered[seg.FileID] = struct{}{}
	}
	if len(registered) == 0 {
		return
	}
	lane := l.lane
	lane.vlogMu.Lock()
	defer lane.vlogMu.Unlock()
	if len(lane.vlogCreatedSegments) == 0 {
		return
	}
	dst := lane.vlogCreatedSegments[:0]
	for _, seg := range lane.vlogCreatedSegments {
		if _, ok := registered[seg.fileID]; ok {
			continue
		}
		dst = append(dst, seg)
	}
	for i := len(dst); i < len(lane.vlogCreatedSegments); i++ {
		lane.vlogCreatedSegments[i] = laneValueLogSegment{}
	}
	lane.vlogCreatedSegments = dst
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
	scratch := getCompactLeafLogPayloadScratch()
	defer putCompactLeafLogPayloadScratch(scratch)
	encodedLeafPage, _, err := valuelog.MaybeCompactLeafLogPayloadTo(scratch.buf[:0], leafPage)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	rid := l.db.nextRID.Add(1)
	ptr, retainPath, err := l.db.appendValueLogOneInternal(l.lane, 0, nil, rid, encodedLeafPage, journalDurabilityNone, false)
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

func (l *cachingLeafPageLog) AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error) {
	if l == nil || l.db == nil || l.lane == nil {
		return nil, errWALUnavailable
	}
	if len(leafPages) == 0 {
		return nil, nil
	}
	if len(leafPages) == 1 {
		ptr, err := l.AppendLeafPage(leafPages[0])
		if err != nil {
			return nil, err
		}
		return []page.LeafLogPtr{ptr}, nil
	}

	startRID, err := l.db.ReserveValueLogRIDs(len(leafPages))
	if err != nil {
		return nil, err
	}
	records := getValueLogRecordsCap(len(leafPages))
	records = records[:len(leafPages)]
	defer func() {
		for i := range records {
			records[i] = valuelog.Record{}
		}
		putValueLogRecordsNoClear(records)
	}()
	var scratches []*compactLeafLogPayloadScratch
	defer func() {
		for _, scratch := range scratches {
			putCompactLeafLogPayloadScratch(scratch)
		}
	}()
	for i, leafPage := range leafPages {
		scratch := getCompactLeafLogPayloadScratch()
		encodedLeafPage, compacted, err := valuelog.MaybeCompactLeafLogPayloadTo(scratch.buf[:0], leafPage)
		if err != nil {
			putCompactLeafLogPayloadScratch(scratch)
			return nil, err
		}
		if compacted {
			scratch.buf = encodedLeafPage
			scratches = append(scratches, scratch)
		} else {
			putCompactLeafLogPayloadScratch(scratch)
		}
		records[i] = valuelog.Record{
			RID:   startRID + uint64(i),
			Value: encodedLeafPage,
		}
	}

	valuePtrs, err := l.db.appendValueLog(l.lane, 0, nil, records, journalDurabilityNone)
	if err != nil {
		return nil, err
	}
	defer putValueLogPtrs(valuePtrs)
	if len(valuePtrs) != len(leafPages) {
		return nil, fmt.Errorf("cachingdb: leaf page batch returned %d ptrs for %d leaf pages", len(valuePtrs), len(leafPages))
	}
	leafPtrs := make([]page.LeafLogPtr, len(valuePtrs))
	for i, ptr := range valuePtrs {
		leafPtr, convErr := page.LeafLogPtrFromValuePtr(ptr)
		if convErr != nil {
			return nil, convErr
		}
		leafPtrs[i] = leafPtr
		l.db.noteLeafGenerationRecordLength(ptr)
	}
	return leafPtrs, nil
}

func getCompactLeafLogPayloadScratch() *compactLeafLogPayloadScratch {
	if v := compactLeafLogPayloadScratchPool.Get(); v != nil {
		if scratch, ok := v.(*compactLeafLogPayloadScratch); ok && scratch != nil && cap(scratch.buf) >= page.PageSize {
			scratch.buf = scratch.buf[:0]
			return scratch
		}
	}
	return &compactLeafLogPayloadScratch{buf: make([]byte, 0, page.PageSize)}
}

func putCompactLeafLogPayloadScratch(scratch *compactLeafLogPayloadScratch) {
	if scratch == nil || cap(scratch.buf) < page.PageSize {
		return
	}
	if cap(scratch.buf) > page.PageSize*2 {
		return
	}
	scratch.buf = scratch.buf[:0]
	compactLeafLogPayloadScratchPool.Put(scratch)
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
