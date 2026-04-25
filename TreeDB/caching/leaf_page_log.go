package caching

import (
	"sync"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type cachingLeafPageLog struct {
	db   *DB
	lane *lane
}

var _ backenddb.LeafPageLog = (*cachingLeafPageLog)(nil)

const leafPageAppendBatchMax = 512

var preparedLeafPageRecordPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, valuelog.HeaderSize+valuelog.FrameHeaderSize+16+page.PageSize)
		return &buf
	},
}

type leafPageAppendRequest struct {
	raw        []byte
	ptrLength  uint32
	stats      valuelog.FrameStats
	compacted  bool
	payloadLen int

	wg         sync.WaitGroup
	ptr        page.ValuePtr
	retainPath string
	totalBytes int64
	err        error
}

func newCachingLeafPageLog(db *DB, l *lane) backenddb.LeafPageLog {
	return &cachingLeafPageLog{db: db, lane: l}
}

func getPreparedLeafPageRecordBuffer() *[]byte {
	buf, _ := preparedLeafPageRecordPool.Get().(*[]byte)
	if buf == nil {
		b := make([]byte, 0, valuelog.HeaderSize+valuelog.FrameHeaderSize+16+page.PageSize)
		return &b
	}
	return buf
}

func putPreparedLeafPageRecordBuffer(buf *[]byte) {
	if buf == nil {
		return
	}
	const maxKeep = 64 << 10
	if cap(*buf) > maxKeep {
		b := make([]byte, 0, valuelog.HeaderSize+valuelog.FrameHeaderSize+16+page.PageSize)
		*buf = b
	} else {
		*buf = (*buf)[:0]
	}
	preparedLeafPageRecordPool.Put(buf)
}

func (db *DB) noteLeafGenerationRecordLength(ptr page.ValuePtr) {
	if db == nil || db.backend == nil || ptr.FileID == 0 || ptr.Offset == 0 {
		return
	}
	if notifier, ok := db.backend.(interface{ NoteLeafGenerationRecordLength(page.ValuePtr) }); ok {
		notifier.NoteLeafGenerationRecordLength(ptr)
	}
}

// appendPreparedLeafPageValueLog appends a raw leaf-page record that was encoded
// outside vlogMu. The caller must hold l.vlogMu; this helper releases it while
// preparing the record and returns with the mutex unlocked.
func (db *DB) appendPreparedLeafPageValueLog(l *lane, rid uint64, leafPage []byte) (page.ValuePtr, string, valuelog.FrameStats, bool, int, int64, error) {
	l.leafAppendMu.Lock()
	l.leafAppendPreparing++
	l.leafAppendMu.Unlock()
	l.vlogMu.Unlock()
	return db.appendPreparedLeafPageValueLogQueuedMarked(l, rid, leafPage, true)
}

func (db *DB) appendPreparedLeafPageValueLogQueued(l *lane, rid uint64, leafPage []byte) (page.ValuePtr, string, valuelog.FrameStats, bool, int, int64, error) {
	return db.appendPreparedLeafPageValueLogQueuedMarked(l, rid, leafPage, false)
}

func (db *DB) appendPreparedLeafPageValueLogQueuedMarked(l *lane, rid uint64, leafPage []byte, markedPreparing bool) (page.ValuePtr, string, valuelog.FrameStats, bool, int, int64, error) {
	bufp := getPreparedLeafPageRecordBuffer()
	raw, stats, compacted, ptrLength, err := valuelog.PrepareCompactLeafPageRecord((*bufp)[:0], rid, leafPage)
	if err != nil {
		if markedPreparing {
			finishLeafPageAppendPreparing(l)
		}
		putPreparedLeafPageRecordBuffer(bufp)
		return page.ValuePtr{}, "", valuelog.FrameStats{}, false, 0, 0, err
	}
	payloadLen := stats.RawPayloadBytes

	req := &leafPageAppendRequest{
		raw:        raw,
		ptrLength:  ptrLength,
		stats:      stats,
		compacted:  compacted,
		payloadLen: payloadLen,
	}
	req.wg.Add(1)

	leader := false
	l.leafAppendMu.Lock()
	if markedPreparing && l.leafAppendPreparing > 0 {
		l.leafAppendPreparing--
	}
	if !l.leafAppendDraining {
		l.leafAppendDraining = true
		leader = true
	}
	l.leafAppendQueue = append(l.leafAppendQueue, req)
	broadcastLeafAppendLocked(l)
	l.leafAppendMu.Unlock()

	if leader {
		db.drainPreparedLeafPageValueLogQueue(l)
	}

	req.wg.Wait()
	putPreparedLeafPageRecordBuffer(bufp)
	if req.err != nil {
		return page.ValuePtr{}, "", valuelog.FrameStats{}, false, 0, 0, req.err
	}
	return req.ptr, req.retainPath, req.stats, req.compacted, req.payloadLen, req.totalBytes, nil
}

func finishLeafPageAppendPreparing(l *lane) {
	if l == nil {
		return
	}
	l.leafAppendMu.Lock()
	if l.leafAppendPreparing > 0 {
		l.leafAppendPreparing--
	}
	broadcastLeafAppendLocked(l)
	l.leafAppendMu.Unlock()
}

func leafAppendCondLocked(l *lane) *sync.Cond {
	if l.leafAppendCond == nil {
		l.leafAppendCond = sync.NewCond(&l.leafAppendMu)
	}
	return l.leafAppendCond
}

func broadcastLeafAppendLocked(l *lane) {
	if l != nil && l.leafAppendCond != nil {
		l.leafAppendCond.Broadcast()
	}
}

// drainPreparedLeafPageValueLogQueue lets one contended caller append several
// already-encoded leaf records under a single vlogMu acquisition.
func (db *DB) drainPreparedLeafPageValueLogQueue(l *lane) {
	batch := make([]*leafPageAppendRequest, 0, leafPageAppendBatchMax)
	for {
		batch = batch[:0]

		l.vlogMu.Lock()
		l.leafAppendMu.Lock()
		if len(l.leafAppendQueue) == 0 {
			l.leafAppendDraining = false
			l.leafAppendMu.Unlock()
			l.vlogMu.Unlock()
			return
		}
		n := len(l.leafAppendQueue)
		if n > leafPageAppendBatchMax {
			n = leafPageAppendBatchMax
		}
		queue := l.leafAppendQueue
		batch = append(batch, queue[:n]...)
		clear(queue[:n])
		if n == len(queue) {
			l.leafAppendQueue = nil
		} else {
			l.leafAppendQueue = queue[n:]
		}
		l.leafAppendMu.Unlock()

		retainPaths := db.appendPreparedLeafPageValueLogBatchMuHeld(l, batch, nil)
		if db.testBeforeVlogUnlock != nil {
			db.testBeforeVlogUnlock(int(l.id))
		}
		l.vlogMu.Unlock()

		for _, path := range retainPaths {
			db.markValueLogRetain(path)
		}
		for i := range batch {
			batch[i].wg.Done()
		}
	}
}

func (db *DB) drainPreparedLeafPageValueLogQueueBarrierMuHeld(l *lane) error {
	batch := make([]*leafPageAppendRequest, 0, leafPageAppendBatchMax)
	for {
		batch = batch[:0]

		l.leafAppendMu.Lock()
		for l.leafAppendPreparing > 0 && len(l.leafAppendQueue) == 0 {
			leafAppendCondLocked(l).Wait()
		}
		if len(l.leafAppendQueue) == 0 {
			if l.leafAppendPreparing == 0 {
				l.leafAppendDraining = false
			}
			l.leafAppendMu.Unlock()
			return nil
		}
		n := len(l.leafAppendQueue)
		if n > leafPageAppendBatchMax {
			n = leafPageAppendBatchMax
		}
		queue := l.leafAppendQueue
		batch = append(batch, queue[:n]...)
		copy(queue, queue[n:])
		clear(queue[len(queue)-n:])
		l.leafAppendQueue = queue[:len(queue)-n]
		l.leafAppendMu.Unlock()

		retainPaths := db.appendPreparedLeafPageValueLogBatchMuHeld(l, batch, nil)
		for _, path := range retainPaths {
			db.markValueLogRetain(path)
		}
		var firstErr error
		for i := range batch {
			if batch[i] != nil && batch[i].err != nil && firstErr == nil {
				firstErr = batch[i].err
			}
			if batch[i] != nil {
				batch[i].wg.Done()
			}
		}
		if firstErr != nil {
			finishQueuedLeafPageAppendRequests(l, firstErr)
			return firstErr
		}
	}
}

func (db *DB) appendPreparedLeafPageValueLogBatchMuHeld(l *lane, batch []*leafPageAppendRequest, retainPaths []string) []string {
	if len(batch) == 0 {
		return retainPaths
	}
	w, ok := l.vlog.(*valuelog.Writer)
	if !ok || w == nil {
		finishLeafPageAppendBatch(batch, errWALUnavailable)
		return retainPaths
	}
	if rotateErr := db.rotateValueLogForMaxSegmentMuHeld(l, w); rotateErr != nil {
		finishLeafPageAppendBatch(batch, rotateErr)
		return retainPaths
	}
	w, ok = l.vlog.(*valuelog.Writer)
	if !ok || w == nil {
		finishLeafPageAppendBatch(batch, errWALUnavailable)
		return retainPaths
	}
	if l.vlogCaps.writer != w {
		l.vlogCaps = computeVlogWriterCaps(w)
	}
	db.setVlogWriterMode(l, w, vlogWriteOff, db.valueLogBlockCodec)

	totalBytes := int64(0)
	flushedBoundary := false
	noteRetainPath := func() {
		if l.vlogPath == "" || l.vlogPath == l.vlogRetainedPath {
			return
		}
		l.vlogRetainedPath = l.vlogPath
		retainPaths = append(retainPaths, l.vlogPath)
	}
	var err error
	for _, req := range batch {
		if req == nil {
			continue
		}
		if rotateErr := db.rotateValueLogForMaxSegmentMuHeld(l, w); rotateErr != nil {
			err = rotateErr
			finishLeafPageAppendBatch(batch, err)
			break
		}
		next, ok := l.vlog.(*valuelog.Writer)
		if !ok || next == nil {
			err = errWALUnavailable
			finishLeafPageAppendBatch(batch, err)
			break
		}
		if next != w {
			w = next
			if l.vlogCaps.writer != w {
				l.vlogCaps = computeVlogWriterCaps(w)
			}
			db.setVlogWriterMode(l, w, vlogWriteOff, db.valueLogBlockCodec)
		}
		before := w.Size()
		req.ptr, err = w.AppendRawRecordBuffered(req.raw, req.ptrLength)
		if err != nil {
			finishLeafPageAppendBatch(batch, err)
			break
		}
		req.totalBytes = w.Size() - before
		if req.totalBytes > 0 {
			totalBytes += req.totalBytes
			l.vlogLiveBytes.Add(req.totalBytes)
		}
		noteRetainPath()
		if db.shouldFlushDeferredValueLogValue(vlogWriteOff, req.raw) {
			flushedBoundary = true
		}
	}
	if err == nil && flushedBoundary {
		err = w.Flush()
		if err != nil {
			for i := range batch {
				if batch[i] != nil && batch[i].err == nil {
					batch[i].err = err
				}
			}
		}
	}
	if err == nil {
		db.markLaneValueLogBoundary(l, totalBytes, flushedBoundary, false)
		if !flushedBoundary && totalBytes > 0 {
			l.backendReadDirtySeq.Add(1)
		}
	}
	return retainPaths
}

func finishLeafPageAppendBatch(batch []*leafPageAppendRequest, err error) {
	for i := range batch {
		if batch[i] == nil {
			continue
		}
		batch[i].ptr = page.ValuePtr{}
		batch[i].retainPath = ""
		batch[i].totalBytes = 0
		batch[i].err = err
	}
}

func finishQueuedLeafPageAppendRequests(l *lane, err error) {
	if l == nil || err == nil {
		return
	}
	l.leafAppendMu.Lock()
	queued := l.leafAppendQueue
	if len(queued) > 0 {
		l.leafAppendQueue = nil
	} else {
		l.leafAppendQueue = l.leafAppendQueue[:0]
	}
	l.leafAppendDraining = false
	broadcastLeafAppendLocked(l)
	l.leafAppendMu.Unlock()

	finishLeafPageAppendBatch(queued, err)
	for i := range queued {
		if queued[i] != nil {
			queued[i].wg.Done()
			queued[i] = nil
		}
	}
}

func (db *DB) observeLeafPageValueLogAppend(l *lane, ptr page.ValuePtr, retainPath string, stats valuelog.FrameStats, compacted bool, finalWriteMode vlogCompressionWriteMode, finalBlockCodec valuelog.BlockCodec, payloadLen int, totalBytes int64, accountLiveBytes bool, probeCompression bool, selectorStart time.Time) (page.ValuePtr, string, error) {
	if accountLiveBytes && totalBytes > 0 {
		l.vlogLiveBytes.Add(totalBytes)
	}
	db.valueLogDictFrames.total.Add(1)
	if stats.Attempted {
		db.valueLogDictFrames.attempted.Add(1)
	}
	if stats.Kept {
		db.valueLogDictFrames.kept.Add(1)
	}
	storedForSelector := stats.StoredPayloadBytes
	if storedForSelector <= 0 || (finalWriteMode == vlogWriteBlock && !stats.Attempted && storedForSelector == payloadLen && totalBytes > 0) {
		if totalBytes > 0 {
			storedForSelector = int(totalBytes)
		} else {
			storedForSelector = payloadLen
		}
	}
	payloadKind := vlogPayloadKindSingleValue
	if compacted {
		payloadKind = vlogPayloadKindOuterLeaf
	}
	recordLaneVlogPayloadKindObservation(l, payloadKind, payloadLen, storedForSelector)
	if payloadKind == vlogPayloadKindOuterLeaf {
		recordLaneVlogPayloadSplitObservation(l, vlogPayloadSplitOuterLeaf, payloadLen, storedForSelector, 1)
		recordLaneVlogOuterLeafCodecObservation(l, vlogOuterLeafCodecLegacyPage, payloadLen, storedForSelector)
	} else {
		recordLaneVlogPayloadSplitObservation(l, vlogPayloadSplitSingleValue, payloadLen, storedForSelector, 1)
	}
	db.observeVlogWriteMode(l, finalWriteMode, finalBlockCodec, payloadLen, payloadLen, storedForSelector, probeCompression, time.Since(selectorStart).Nanoseconds())
	return ptr, retainPath, nil
}

func (db *DB) appendLeafPageValueLog(l *lane, rid uint64, leafPage []byte) (page.ValuePtr, string, error) {
	if !db.splitValueLogEnabled() {
		return page.ValuePtr{}, "", errWALUnavailable
	}
	if l == nil {
		return page.ValuePtr{}, "", errWALUnavailable
	}
	select {
	case <-db.closeCh:
		return page.ValuePtr{}, "", errWALClosed
	default:
	}

	selectorStart := time.Now()
	payloadLen, compacted, err := valuelog.CompactLeafLogPayloadLen(leafPage)
	if err != nil {
		return page.ValuePtr{}, "", err
	}
	locked := l.vlogMu.TryLock()
	contended := !locked
	if contended {
		l.vlogMu.Lock()
	}
	w := l.vlog
	if w == nil {
		l.vlogMu.Unlock()
		return page.ValuePtr{}, "", errWALUnavailable
	}
	if rotateErr := db.rotateValueLogForMaxSegmentMuHeld(l, w); rotateErr != nil {
		l.vlogMu.Unlock()
		return page.ValuePtr{}, "", rotateErr
	}
	w = l.vlog
	if w == nil {
		l.vlogMu.Unlock()
		return page.ValuePtr{}, "", errWALUnavailable
	}
	if l.vlogCaps.writer != w {
		l.vlogCaps = computeVlogWriterCaps(w)
	}

	concrete, ok := w.(*valuelog.Writer)
	if !ok || db.valueLogTemplateEnabled {
		l.vlogMu.Unlock()
		encodedLeafPage, _, err := valuelog.MaybeCompactLeafLogPayload(leafPage)
		if err != nil {
			return page.ValuePtr{}, "", err
		}
		return db.appendValueLogOneInternal(l, 0, nil, rid, encodedLeafPage, journalDurabilityNone, false)
	}

	writeMode, blockCodec, probeCompression := db.resolveVlogWriteMode(l, 0, payloadLen, payloadLen, compacted)
	if writeMode == vlogWriteBlock {
		if codec, ok := db.preferLeafPageBlockCodec(l, payloadLen, blockCodec); ok {
			blockCodec = codec
		} else {
			compressionMode := normalizeVlogCompressionMode(db.valueLogCompressionMode)
			if (compressionMode == vlogCompressionAuto || compressionMode == vlogCompressionDefault) &&
				normalizeVlogAutoPolicy(db.valueLogAutoPolicy) != vlogAutoThroughput &&
				payloadLen >= 2048 {
				blockCodec = chooseLargePayloadNoDictBlockCodec(l, db.valueLogBlockCodec)
			}
		}
	}
	finalWriteMode := vlogWriteOff
	if writeMode == vlogWriteBlock {
		finalWriteMode = vlogWriteBlock
	}
	finalBlockCodec := db.valueLogBlockCodec
	if finalWriteMode == vlogWriteBlock {
		finalBlockCodec = blockCodec
	}
	if contended && finalWriteMode == vlogWriteOff {
		ptr, retainPath, stats, compacted, payloadLen, totalBytes, err := db.appendPreparedLeafPageValueLog(l, rid, leafPage)
		if err != nil {
			return page.ValuePtr{}, "", err
		}
		return db.observeLeafPageValueLogAppend(l, ptr, retainPath, stats, compacted, finalWriteMode, finalBlockCodec, payloadLen, totalBytes, false, probeCompression, selectorStart)
	}
	db.setVlogWriterMode(l, w, finalWriteMode, finalBlockCodec)
	startSize := w.Size()
	ptr, stats, compacted, err := concrete.AppendCompactLeafPage(rid, leafPage)
	flushedBoundary := false
	if err == nil && db.shouldFlushDeferredValueLogValue(finalWriteMode, leafPage) {
		err = w.Flush()
		flushedBoundary = err == nil
	}
	totalBytes := int64(0)
	if err == nil {
		totalBytes = w.Size() - startSize
	}
	retainPath := ""
	if l.vlogPath != "" && l.vlogPath != l.vlogRetainedPath {
		l.vlogRetainedPath = l.vlogPath
		retainPath = l.vlogPath
	}
	if err == nil {
		db.markLaneValueLogBoundary(l, totalBytes, flushedBoundary, false)
		if !flushedBoundary && totalBytes > 0 {
			l.backendReadDirtySeq.Add(1)
		}
	}
	if db.testBeforeVlogUnlock != nil {
		db.testBeforeVlogUnlock(int(l.id))
	}
	l.vlogMu.Unlock()
	if err != nil {
		return page.ValuePtr{}, "", err
	}
	return db.observeLeafPageValueLogAppend(l, ptr, retainPath, stats, compacted, finalWriteMode, finalBlockCodec, payloadLen, totalBytes, true, probeCompression, selectorStart)
}

func (l *cachingLeafPageLog) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	if l == nil || l.db == nil || l.lane == nil {
		return page.LeafLogPtr{}, errWALUnavailable
	}
	rid := l.db.nextRID.Add(1)
	ptr, retainPath, err := l.db.appendLeafPageValueLog(l.lane, rid, leafPage)
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
