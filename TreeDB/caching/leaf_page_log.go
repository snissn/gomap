package caching

import (
	"fmt"
	"sync"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type cachingLeafPageLog struct {
	db   *DB
	lane *lane
}

var _ backenddb.LeafPageLog = (*cachingLeafPageLog)(nil)
var _ backenddb.LeafPageBatchLog = (*cachingLeafPageLog)(nil)
var _ backenddb.LeafPageStableLog = (*cachingLeafPageLog)(nil)
var _ backenddb.LeafPageStableBatchLog = (*cachingLeafPageLog)(nil)
var _ backenddb.LeafPagePreparedStableLog = (*cachingLeafPageLog)(nil)
var _ backenddb.LeafPagePreparedStableBatchLog = (*cachingLeafPageLog)(nil)
var _ backenddb.LeafPagePreparedChildRefStableBatchLog = (*cachingLeafPageLog)(nil)
var _ backenddb.LeafPageLogCompactStorageHandoff = (*cachingLeafPageLog)(nil)

var compactLeafLogPayloadScratchPool sync.Pool
var compactLeafLogPayloadScratchPtrRefPool sync.Pool

const compactLeafLogPayloadScratchPtrPoolMaxCap = 4096

type compactLeafLogPayloadScratch struct {
	buf []byte
}

type compactLeafLogPayloadScratchPtrRef struct {
	ptrs []*compactLeafLogPayloadScratch
}

func newCachingLeafPageLog(db *DB, l *lane) backenddb.LeafPageLog {
	if db != nil && db.indexOuterLeavesInValueLog && l == &db.leafLog {
		return &cachingLeafPageLogGroup{db: db}
	}
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

func (l *cachingLeafPageLog) ProtectedLeafGenerationRootIDPairSnapshot() ([]uint64, []uint64, uint64) {
	if l == nil || l.db == nil {
		return nil, nil, 0
	}
	return l.db.publishedLeafGenerationProtectionSnapshot()
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

func (l *cachingLeafPageLog) CurrentLeafPageLogSegmentsSnapshot() ([]backenddb.LeafPageLogSegment, error) {
	if l == nil || l.lane == nil {
		return nil, nil
	}
	lane := l.lane
	lane.vlogMu.Lock()
	defer lane.vlogMu.Unlock()
	if lane.vlogPath == "" || lane.vlogSeq <= 0 {
		return nil, nil
	}
	fileID, err := valuelog.EncodeFileID(uint32(lane.id), uint32(lane.vlogSeq))
	if err != nil {
		return nil, err
	}
	return []backenddb.LeafPageLogSegment{{Path: lane.vlogPath, FileID: fileID}}, nil
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

func (l *cachingLeafPageLog) Close() error {
	if l == nil || l.db == nil || l.lane == nil {
		return nil
	}
	return l.db.closeLeafValueLogLane(l.lane)
}

func (l *cachingLeafPageLog) ConcurrentLeafPageAppends() bool { return true }

func (l *cachingLeafPageLog) CompactStorageCachedWrapperOwner() bool { return true }

func (l *cachingLeafPageLog) PreparedLeafPageAppends() bool { return true }

func (l *cachingLeafPageLog) PreparedLeafPageBatchAppends() bool { return true }

func (l *cachingLeafPageLog) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	if l == nil || l.db == nil || l.lane == nil {
		return page.LeafLogPtr{}, errWALUnavailable
	}
	scratch := getCompactLeafLogPayloadScratch()
	defer putCompactLeafLogPayloadScratch(scratch)
	encodeStart := time.Now()
	encodedLeafPage, _, err := valuelog.MaybeCompactLeafLogPayloadTo(scratch.buf[:0], leafPage)
	l.db.observeFlushApplyLeafLogEncodeCompress(time.Since(encodeStart))
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	rid := l.db.nextRID.Add(1)
	ptr, retainPath, err := l.db.appendValueLogOneInternal(l.lane, 0, nil, rid, encodedLeafPage, journalDurabilityNone, false)
	if retainPath != "" {
		l.db.markValueLogRetain(retainPath)
	}
	if err != nil {
		l.db.observeLeafLogLaneAppend(l.lane, 0, 0, 0, 0, err)
		return page.LeafLogPtr{}, err
	}
	leafPtr, convErr := page.LeafLogPtrFromValuePtr(ptr)
	if convErr != nil {
		return page.LeafLogPtr{}, convErr
	}
	l.db.noteLeafGenerationRecordLength(ptr)
	return leafPtr, nil
}

func (l *cachingLeafPageLog) AppendLeafPageWithStableResources(leafPage []byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	ptrs, resources, err := l.appendLeafPages([][]byte{leafPage}, true)
	if err != nil {
		return page.LeafLogPtr{}, nil, err
	}
	if len(ptrs) != 1 {
		resources.Release()
		return page.LeafLogPtr{}, nil, fmt.Errorf("cachingdb: stable leaf append returned %d pointers", len(ptrs))
	}
	return ptrs[0], resources, nil
}

func (l *cachingLeafPageLog) AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error) {
	ptrs, _, err := l.appendLeafPages(leafPages, false)
	return ptrs, err
}

func (l *cachingLeafPageLog) AppendLeafPagesWithStableResources(leafPages [][]byte) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	return l.appendLeafPages(leafPages, true)
}

func (l *cachingLeafPageLog) appendLeafPages(leafPages [][]byte, stable bool) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	if l == nil || l.db == nil || l.lane == nil {
		return nil, nil, errWALUnavailable
	}
	if len(leafPages) == 0 {
		return nil, nil, nil
	}
	if len(leafPages) == 1 && !stable {
		ptr, err := l.AppendLeafPage(leafPages[0])
		if err != nil {
			return nil, nil, err
		}
		return []page.LeafLogPtr{ptr}, nil, nil
	}

	startRID, err := l.db.ReserveValueLogRIDs(len(leafPages))
	if err != nil {
		return nil, nil, err
	}
	records := getValueLogRecordsCap(len(leafPages))
	records = records[:len(leafPages)]
	defer func() {
		for i := range records {
			records[i] = valuelog.Record{}
		}
		putValueLogRecordsNoClear(records)
	}()
	var scratchRef *compactLeafLogPayloadScratchPtrRef
	var scratches []*compactLeafLogPayloadScratch
	defer func() {
		for _, scratch := range scratches {
			putCompactLeafLogPayloadScratch(scratch)
		}
		putCompactLeafLogPayloadScratchPtrRef(scratchRef, scratches)
	}()
	encodeStart := time.Now()
	encodeObserved := false
	observeEncode := func() {
		if !encodeObserved {
			l.db.observeFlushApplyLeafLogEncodeCompress(time.Since(encodeStart))
			encodeObserved = true
		}
	}
	for i, leafPage := range leafPages {
		scratch := getCompactLeafLogPayloadScratch()
		encodedLeafPage, compacted, err := valuelog.MaybeCompactLeafLogPayloadTo(scratch.buf[:0], leafPage)
		if err != nil {
			putCompactLeafLogPayloadScratch(scratch)
			observeEncode()
			return nil, nil, err
		}
		if compacted {
			if scratchRef == nil {
				scratchRef = getCompactLeafLogPayloadScratchPtrRefCap(len(leafPages))
				scratches = scratchRef.ptrs[:0]
			}
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
	observeEncode()

	var resources *rootpublication.StableResourceSet
	var valuePtrs []page.ValuePtr
	if stable {
		valuePtrs, resources, err = l.db.appendValueLogWithStableResources(l.lane, 0, nil, records, journalDurabilityNone)
	} else {
		valuePtrs, err = l.db.appendValueLog(l.lane, 0, nil, records, journalDurabilityNone)
	}
	if err != nil {
		l.db.observeLeafLogLaneAppend(l.lane, 0, 0, 0, 0, err)
		return nil, nil, err
	}
	releaseResources := true
	defer func() {
		if releaseResources {
			resources.Release()
		}
	}()
	defer putValueLogPtrs(valuePtrs)
	if len(valuePtrs) != len(leafPages) {
		return nil, nil, fmt.Errorf("cachingdb: leaf page batch returned %d ptrs for %d leaf pages", len(valuePtrs), len(leafPages))
	}
	leafPtrs := make([]page.LeafLogPtr, len(valuePtrs))
	for i, ptr := range valuePtrs {
		leafPtr, convErr := page.LeafLogPtrFromValuePtr(ptr)
		if convErr != nil {
			return nil, nil, convErr
		}
		leafPtrs[i] = leafPtr
		l.db.noteLeafGenerationRecordLength(ptr)
	}
	releaseResources = false
	return leafPtrs, resources, nil
}

func (l *cachingLeafPageLog) AppendPreparedLeafPage(leafPage []byte, preparedPayload []byte) (page.LeafLogPtr, error) {
	if l == nil || l.db == nil || l.lane == nil {
		return page.LeafLogPtr{}, errWALUnavailable
	}
	if len(leafPage) != page.PageSize {
		return page.LeafLogPtr{}, fmt.Errorf("cachingdb: prepared leaf page has invalid size: got=%dB want=%dB", len(leafPage), page.PageSize)
	}
	rid := l.db.nextRID.Add(1)
	ptr, retainPath, err := l.db.appendValueLogOneInternal(l.lane, 0, nil, rid, preparedPayload, journalDurabilityNone, false)
	if retainPath != "" {
		l.db.markValueLogRetain(retainPath)
	}
	if err != nil {
		l.db.observeLeafLogLaneAppend(l.lane, 0, 0, 0, 0, err)
		return page.LeafLogPtr{}, err
	}
	leafPtr, convErr := page.LeafLogPtrFromValuePtr(ptr)
	if convErr != nil {
		return page.LeafLogPtr{}, convErr
	}
	l.db.noteLeafGenerationRecordLength(ptr)
	return leafPtr, nil
}

func (l *cachingLeafPageLog) AppendPreparedLeafPageWithStableResources(leafPage []byte, preparedPayload []byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	ptrs, resources, err := l.appendPreparedLeafPages([][]byte{leafPage}, [][]byte{preparedPayload}, true)
	if err != nil {
		return page.LeafLogPtr{}, nil, err
	}
	if len(ptrs) != 1 {
		resources.Release()
		return page.LeafLogPtr{}, nil, fmt.Errorf("cachingdb: stable prepared leaf append returned %d pointers", len(ptrs))
	}
	return ptrs[0], resources, nil
}

func (l *cachingLeafPageLog) AppendPreparedLeafPages(leafPages [][]byte, preparedPayloads [][]byte) ([]page.LeafLogPtr, error) {
	ptrs, _, err := l.appendPreparedLeafPages(leafPages, preparedPayloads, false)
	return ptrs, err
}

func (l *cachingLeafPageLog) AppendPreparedLeafPagesWithStableResources(leafPages [][]byte, preparedPayloads [][]byte) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	return l.appendPreparedLeafPages(leafPages, preparedPayloads, true)
}

func (l *cachingLeafPageLog) appendPreparedLeafPages(leafPages [][]byte, preparedPayloads [][]byte, stable bool) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	if l == nil || l.db == nil || l.lane == nil {
		return nil, nil, errWALUnavailable
	}
	if len(leafPages) == 0 {
		return nil, nil, nil
	}
	if len(preparedPayloads) != len(leafPages) {
		return nil, nil, fmt.Errorf("cachingdb: prepared leaf page batch has %d payloads for %d leaf pages", len(preparedPayloads), len(leafPages))
	}
	if len(leafPages) == 1 && !stable {
		ptr, err := l.AppendPreparedLeafPage(leafPages[0], preparedPayloads[0])
		if err != nil {
			return nil, nil, err
		}
		return []page.LeafLogPtr{ptr}, nil, nil
	}
	startRID, err := l.db.ReserveValueLogRIDs(len(leafPages))
	if err != nil {
		return nil, nil, err
	}
	records := getValueLogRecordsCap(len(leafPages))
	records = records[:len(leafPages)]
	defer func() {
		for i := range records {
			records[i] = valuelog.Record{}
		}
		putValueLogRecordsNoClear(records)
	}()
	for i := range leafPages {
		if len(leafPages[i]) != page.PageSize {
			return nil, nil, fmt.Errorf("cachingdb: prepared leaf page %d has invalid size: got=%dB want=%dB", i, len(leafPages[i]), page.PageSize)
		}
		records[i] = valuelog.Record{
			RID:   startRID + uint64(i),
			Value: preparedPayloads[i],
		}
	}
	var resources *rootpublication.StableResourceSet
	var valuePtrs []page.ValuePtr
	if stable {
		valuePtrs, resources, err = l.db.appendValueLogWithStableResources(l.lane, 0, nil, records, journalDurabilityNone)
	} else {
		valuePtrs, err = l.db.appendValueLog(l.lane, 0, nil, records, journalDurabilityNone)
	}
	if err != nil {
		l.db.observeLeafLogLaneAppend(l.lane, 0, 0, 0, 0, err)
		return nil, nil, err
	}
	releaseResources := true
	defer func() {
		if releaseResources {
			resources.Release()
		}
	}()
	defer putValueLogPtrs(valuePtrs)
	if len(valuePtrs) != len(leafPages) {
		return nil, nil, fmt.Errorf("cachingdb: prepared leaf page batch returned %d ptrs for %d leaf pages", len(valuePtrs), len(leafPages))
	}
	leafPtrs := make([]page.LeafLogPtr, len(valuePtrs))
	for i, ptr := range valuePtrs {
		leafPtr, convErr := page.LeafLogPtrFromValuePtr(ptr)
		if convErr != nil {
			return nil, nil, convErr
		}
		leafPtrs[i] = leafPtr
		l.db.noteLeafGenerationRecordLength(ptr)
	}
	releaseResources = false
	return leafPtrs, resources, nil
}

func (l *cachingLeafPageLog) AppendPreparedLeafPageChildRefs(leafPages [][]byte, preparedPayloads [][]byte, refs []page.ChildRef) ([]page.ChildRef, error) {
	refs = refs[:0]
	if l == nil || l.db == nil || l.lane == nil {
		return nil, errWALUnavailable
	}
	if len(leafPages) == 0 {
		return refs, nil
	}
	if len(preparedPayloads) != len(leafPages) {
		return nil, fmt.Errorf("cachingdb: prepared leaf page child-ref batch has %d payloads for %d leaf pages", len(preparedPayloads), len(leafPages))
	}
	if len(leafPages) == 1 {
		ptr, err := l.AppendPreparedLeafPage(leafPages[0], preparedPayloads[0])
		if err != nil {
			return nil, err
		}
		return append(refs, page.LeafLogChildRef(ptr)), nil
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
	for i := range leafPages {
		if len(leafPages[i]) != page.PageSize {
			return nil, fmt.Errorf("cachingdb: prepared leaf page %d has invalid size: got=%dB want=%dB", i, len(leafPages[i]), page.PageSize)
		}
		records[i] = valuelog.Record{
			RID:   startRID + uint64(i),
			Value: preparedPayloads[i],
		}
	}
	valuePtrs, err := l.db.appendValueLog(l.lane, 0, nil, records, journalDurabilityNone)
	if err != nil {
		l.db.observeLeafLogLaneAppend(l.lane, 0, 0, 0, 0, err)
		return nil, err
	}
	defer putValueLogPtrs(valuePtrs)
	if len(valuePtrs) != len(leafPages) {
		return nil, fmt.Errorf("cachingdb: prepared leaf page child-ref batch returned %d ptrs for %d leaf pages", len(valuePtrs), len(leafPages))
	}
	if cap(refs) < len(valuePtrs) {
		refs = make([]page.ChildRef, len(valuePtrs))
	} else {
		refs = refs[:len(valuePtrs)]
	}
	for i, ptr := range valuePtrs {
		leafPtr, convErr := page.LeafLogPtrFromValuePtr(ptr)
		if convErr != nil {
			return nil, convErr
		}
		refs[i] = page.LeafLogChildRef(leafPtr)
		l.db.noteLeafGenerationRecordLength(ptr)
	}
	return refs, nil
}

func (l *cachingLeafPageLog) AppendPreparedLeafPageChildRefsWithStableResources(leafPages [][]byte, preparedPayloads [][]byte, refs []page.ChildRef) ([]page.ChildRef, *rootpublication.StableResourceSet, error) {
	refs = refs[:0]
	if l == nil || l.db == nil || l.lane == nil {
		return nil, nil, errWALUnavailable
	}
	if len(leafPages) == 0 {
		return refs, nil, nil
	}
	if len(preparedPayloads) != len(leafPages) {
		return nil, nil, fmt.Errorf("cachingdb: prepared leaf page child-ref batch has %d payloads for %d leaf pages", len(preparedPayloads), len(leafPages))
	}
	startRID, err := l.db.ReserveValueLogRIDs(len(leafPages))
	if err != nil {
		return nil, nil, err
	}
	records := getValueLogRecordsCap(len(leafPages))
	records = records[:len(leafPages)]
	defer func() {
		for i := range records {
			records[i] = valuelog.Record{}
		}
		putValueLogRecordsNoClear(records)
	}()
	for i := range leafPages {
		if len(leafPages[i]) != page.PageSize {
			return nil, nil, fmt.Errorf("cachingdb: prepared leaf page %d has invalid size: got=%dB want=%dB", i, len(leafPages[i]), page.PageSize)
		}
		records[i] = valuelog.Record{
			RID:   startRID + uint64(i),
			Value: preparedPayloads[i],
		}
	}
	valuePtrs, resources, err := l.db.appendValueLogWithStableResources(l.lane, 0, nil, records, journalDurabilityNone)
	if err != nil {
		l.db.observeLeafLogLaneAppend(l.lane, 0, 0, 0, 0, err)
		return nil, nil, err
	}
	releaseResources := true
	defer func() {
		if releaseResources && resources != nil {
			resources.Release()
		}
	}()
	defer putValueLogPtrs(valuePtrs)
	if len(valuePtrs) != len(leafPages) {
		return nil, nil, fmt.Errorf("cachingdb: prepared leaf page child-ref batch returned %d ptrs for %d leaf pages", len(valuePtrs), len(leafPages))
	}
	if cap(refs) < len(valuePtrs) {
		refs = make([]page.ChildRef, len(valuePtrs))
	} else {
		refs = refs[:len(valuePtrs)]
	}
	for i, ptr := range valuePtrs {
		leafPtr, convErr := page.LeafLogPtrFromValuePtr(ptr)
		if convErr != nil {
			return nil, nil, convErr
		}
		refs[i] = page.LeafLogChildRef(leafPtr)
		l.db.noteLeafGenerationRecordLength(ptr)
	}
	releaseResources = false
	return refs, resources, nil
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

func getCompactLeafLogPayloadScratchPtrRefCap(capacity int) *compactLeafLogPayloadScratchPtrRef {
	if capacity < 0 {
		capacity = 0
	}
	if capacity <= compactLeafLogPayloadScratchPtrPoolMaxCap {
		if v := compactLeafLogPayloadScratchPtrRefPool.Get(); v != nil {
			if ref, ok := v.(*compactLeafLogPayloadScratchPtrRef); ok && ref != nil && cap(ref.ptrs) >= capacity {
				ref.ptrs = ref.ptrs[:0]
				return ref
			}
		}
	}
	return &compactLeafLogPayloadScratchPtrRef{ptrs: make([]*compactLeafLogPayloadScratch, 0, capacity)}
}

func putCompactLeafLogPayloadScratchPtrRef(ref *compactLeafLogPayloadScratchPtrRef, ptrs []*compactLeafLogPayloadScratch) {
	if ref == nil {
		return
	}
	if ptrs != nil {
		ref.ptrs = ptrs
	}
	if cap(ref.ptrs) == 0 || cap(ref.ptrs) > compactLeafLogPayloadScratchPtrPoolMaxCap {
		ref.ptrs = nil
		return
	}
	full := ref.ptrs[:cap(ref.ptrs)]
	clear(full)
	ref.ptrs = full[:0]
	compactLeafLogPayloadScratchPtrRefPool.Put(ref)
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

func (l *cachingLeafPageLog) AdvanceCompactStorageLeafPageLogSeqAtLeast(seq uint32) error {
	if l == nil || l.db == nil {
		return nil
	}
	return l.db.advanceCompactStorageLeafPageLogSeqAtLeast(seq)
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
