package caching

import (
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

	l.vlogMu.Lock()
	selectorStart := time.Now()
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

	payloadLen, compacted, err := valuelog.CompactLeafLogPayloadLen(leafPage)
	if err != nil {
		l.vlogMu.Unlock()
		return page.ValuePtr{}, "", err
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
	if totalBytes > 0 {
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
