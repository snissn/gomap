package db

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type logSegment struct {
	seq      uint64
	lane     int
	path     string
	size     int64
	valueLog bool
	fileID   uint32
}

func listSegmentsInDir(segDir string) ([]logSegment, error) {
	entries, err := os.ReadDir(segDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var segments []logSegment
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		lane, seq, valueLog, ok := parseLogSeq(name)
		if !ok {
			continue
		}
		seg := logSegment{
			seq:      seq,
			lane:     lane,
			path:     filepath.Join(segDir, name),
			valueLog: valueLog,
		}
		if info, err := entry.Info(); err == nil {
			seg.size = info.Size()
		}
		if valueLog {
			if lane < 0 {
				continue
			}
			fileID, err := valuelog.EncodeFileID(uint32(lane), uint32(seq))
			if err != nil {
				continue
			}
			seg.fileID = fileID
		}
		segments = append(segments, seg)
	}

	sort.Slice(segments, func(i, j int) bool {
		if segments[i].lane != segments[j].lane {
			return segments[i].lane < segments[j].lane
		}
		return segments[i].seq < segments[j].seq
	})
	return segments, nil
}

func listWALSegments(dir string) ([]logSegment, error) {
	return listSegmentsInDir(resolveStorageLayout(dir).walDir)
}

func listValueLogSegments(dir string) ([]logSegment, error) {
	layout := resolveStorageLayout(dir)
	all := make([]logSegment, 0)
	for _, segDir := range []string{layout.valueVLogDir, layout.leafVLogDir} {
		segments, err := listSegmentsInDir(segDir)
		if err != nil {
			return nil, err
		}
		for _, seg := range segments {
			if !seg.valueLog {
				continue
			}
			all = append(all, seg)
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].lane != all[j].lane {
			return all[i].lane < all[j].lane
		}
		return all[i].seq < all[j].seq
	})
	return all, nil
}

func listRecoverySegments(dir string) ([]logSegment, error) {
	walSegments, err := listWALSegments(dir)
	if err != nil {
		return nil, err
	}
	vlogSegments, err := listValueLogSegments(dir)
	if err != nil {
		return nil, err
	}
	segments := append(walSegments, vlogSegments...)
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].lane != segments[j].lane {
			return segments[i].lane < segments[j].lane
		}
		if segments[i].seq != segments[j].seq {
			return segments[i].seq < segments[j].seq
		}
		if segments[i].valueLog != segments[j].valueLog {
			return !segments[i].valueLog && segments[j].valueLog
		}
		return segments[i].path < segments[j].path
	})
	return segments, nil
}

func parseLogSeq(name string) (int, uint64, bool, bool) {
	const (
		commitPrefix = "commit-"
		valuePrefix  = "value-"
		walPrefix    = "wal-"
		vlogPrefix   = "vlog-"
		suffix       = ".log"
	)
	if !strings.HasSuffix(name, suffix) {
		return 0, 0, false, false
	}
	base := strings.TrimSuffix(name, suffix)

	parseLaneSeq := func(rest string) (int, uint64, bool) {
		parts := strings.SplitN(rest, "-", 2)
		if len(parts) != 2 {
			return 0, 0, false
		}
		lane, err := strconv.Atoi(parts[0])
		if err != nil || lane < 0 {
			return 0, 0, false
		}
		seq, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return 0, 0, false
		}
		return lane, seq, true
	}

	if strings.HasPrefix(base, "commit-l") {
		lane, seq, ok := parseLaneSeq(strings.TrimPrefix(base, "commit-l"))
		return lane, seq, false, ok
	}
	if strings.HasPrefix(base, "value-l") {
		lane, seq, ok := parseLaneSeq(strings.TrimPrefix(base, "value-l"))
		return lane, seq, true, ok
	}
	if strings.HasPrefix(base, commitPrefix) {
		num := strings.TrimPrefix(base, commitPrefix)
		seq, err := strconv.ParseUint(num, 10, 64)
		if err != nil {
			return 0, 0, false, false
		}
		return 0, seq, false, true
	}
	if strings.HasPrefix(base, valuePrefix) {
		num := strings.TrimPrefix(base, valuePrefix)
		seq, err := strconv.ParseUint(num, 10, 64)
		if err != nil {
			return 0, 0, false, false
		}
		return 0, seq, true, true
	}
	if strings.HasPrefix(base, walPrefix) {
		num := strings.TrimPrefix(base, walPrefix)
		seq, err := strconv.ParseUint(num, 10, 64)
		if err != nil {
			return 0, 0, false, false
		}
		return 0, seq, false, true
	}
	if strings.HasPrefix(base, vlogPrefix) {
		num := strings.TrimPrefix(base, vlogPrefix)
		seq, err := strconv.ParseUint(num, 10, 64)
		if err != nil {
			return 0, 0, false, false
		}
		return 0, seq, true, true
	}
	return 0, 0, false, false
}

func isTruncatedLogError(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func replayWALIntoBackend(db *DB, segments []logSegment, maxSegmentBytes int64, dictLookup valuelog.DictLookup) error {
	if db != nil {
		filtered, err := filterCommandWALSegmentsForLegacyReplay(segments, db.meta.AppliedCommandLSN, maxSegmentBytes)
		if err != nil {
			return err
		}
		segments = filtered
	}
	hasCommitSegments := false
	for _, seg := range segments {
		if !seg.valueLog && seg.size > 0 {
			hasCommitSegments = true
			break
		}
	}
	if !hasCommitSegments {
		return nil
	}
	ridMap, err := scanValueLogSegments(segments, dictLookup)
	if err != nil {
		return err
	}
	return replayCommitLogSegments(db, segments, ridMap, maxSegmentBytes)
}

type commandWALReplayFrame struct {
	env commitlog.CommandEnvelope
}

type commandWALReplayLogSupportFunc func() (map[uint64]page.ValuePtr, *replayInlineAppender, error)

func replayCommandWALIntoBackend(db *DB, segments []logSegment, maxSegmentBytes int64, dictLookup valuelog.DictLookup) error {
	if db == nil {
		return fmt.Errorf("treedb: command wal recovery missing db")
	}
	db.mu.RLock()
	applied := db.meta.AppliedCommandLSN
	db.mu.RUnlock()
	frames, err := readCommandWALReplayFrames(segments, applied, maxSegmentBytes)
	if err != nil {
		return err
	}
	if len(frames) == 0 {
		_, err := cleanupCommandWALSegmentsCoveredByAppliedLSN(db.dir, applied, maxSegmentBytes)
		return err
	}
	var ridMap map[uint64]page.ValuePtr
	var inlineAppender *replayInlineAppender
	previousLeafPageLog := db.leafPageLog
	leafPageLogInstalled := false
	restoreLeafPageLog := func() {
		if leafPageLogInstalled {
			db.SetLeafPageLog(previousLeafPageLog)
			leafPageLogInstalled = false
		}
	}
	ensureReplayLogSupport := func() (map[uint64]page.ValuePtr, *replayInlineAppender, error) {
		if inlineAppender != nil {
			return ridMap, inlineAppender, nil
		}
		var err error
		if ridMap == nil {
			ridMap, err = scanValueLogSegments(segments, dictLookup)
			if err != nil {
				return nil, nil, err
			}
		}
		inlineAppender, err = newReplayInlineAppender(db, segments, ridMap)
		if err != nil {
			return nil, nil, err
		}
		if db.indexOuterLeavesInValueLog {
			db.SetLeafPageLog(inlineAppender)
			leafPageLogInstalled = true
		}
		return ridMap, inlineAppender, nil
	}
	defer func() {
		if inlineAppender != nil {
			_ = inlineAppender.close()
		}
		restoreLeafPageLog()
	}()
	needsLogSupport, err := commandWALReplayFramesNeedLogSupport(db, frames)
	if err != nil {
		return err
	}
	if needsLogSupport {
		if _, _, err := ensureReplayLogSupport(); err != nil {
			return err
		}
	}
	for _, frame := range frames {
		if frame.env.LSN <= applied {
			continue
		}
		if frame.env.LSN != applied+1 {
			return fmt.Errorf("%w: current=%d next=%d", ErrCommandWALAppliedLSNNonContig, applied, frame.env.LSN)
		}
		if err := applyCommandWALFrame(db, frame.env, ridMap, inlineAppender, ensureReplayLogSupport); err != nil {
			return err
		}
		applied = frame.env.LSN
		if target := db.testCommandWALRecoveryFailAfterLSN.Load(); target != 0 && frame.env.LSN == target {
			db.testCommandWALRecoveryFailAfterLSN.Store(0)
			return errTestFinalizeCommitFailpoint
		}
	}
	if inlineAppender != nil {
		if err := inlineAppender.syncIfDirty(); err != nil {
			return err
		}
		if err := inlineAppender.close(); err != nil {
			return err
		}
		inlineAppender = nil
		restoreLeafPageLog()
	}
	_, err = cleanupCommandWALSegmentsCoveredByAppliedLSN(db.dir, applied, maxSegmentBytes)
	return err
}

func readCommandWALReplayFrames(segments []logSegment, appliedLSN uint64, maxSegmentBytes int64) ([]commandWALReplayFrame, error) {
	activeByLane, err := commandWALActiveSeqByLane(segments, maxSegmentBytes)
	if err != nil {
		return nil, err
	}
	var frames []commandWALReplayFrame
	seen := make(map[uint64]struct{})
	for _, seg := range segments {
		if seg.valueLog || seg.size == 0 {
			continue
		}
		if !isCommandWALLaneSegment(seg) {
			return nil, commitlog.ErrCommandWALLegacyPayload
		}
		active := seg.seq == activeByLane[seg.lane]
		reader, err := commitlog.NewReaderWithOptions(seg.path, commitlog.Options{MaxSegmentSize: maxSegmentBytes})
		if err != nil {
			return nil, err
		}
		for {
			env, err := reader.ReadCommandFrame()
			if err == nil {
				if env.LSN > appliedLSN {
					if _, ok := seen[env.LSN]; ok {
						_ = reader.Close()
						return nil, commitlog.ErrCommandWALDuplicateLSN
					}
					seen[env.LSN] = struct{}{}
					frames = append(frames, commandWALReplayFrame{env: env})
				}
				continue
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, commitlog.ErrCommandWALTerminalTail) && active {
				break
			}
			_ = reader.Close()
			return nil, err
		}
		if err := reader.Close(); err != nil {
			return nil, err
		}
	}
	sort.Slice(frames, func(i, j int) bool {
		return frames[i].env.LSN < frames[j].env.LSN
	})
	return frames, nil
}

func commandWALReplayFramesNeedLogSupport(db *DB, frames []commandWALReplayFrame) (bool, error) {
	for _, frame := range frames {
		if frame.env.Kind != commitlog.CommandKindRawKVBatch {
			continue
		}
		needsValueLogSupport := false
		err := commitlog.ScanRawKVBatchPayload(frame.env.Payload, func(op commitlog.RawKVOp, key, value []byte) error {
			if op == commitlog.RawKVOpSetRID {
				needsValueLogSupport = true
			}
			if op == commitlog.RawKVOpSet && commandWALRawSetNeedsReplayValueLog(db, key, value) {
				needsValueLogSupport = true
			}
			return nil
		})
		if err != nil {
			return false, err
		}
		if needsValueLogSupport {
			return true, nil
		}
	}
	if db != nil && db.indexOuterLeavesInValueLog {
		// Outer-leaf mode needs a replay leaf-page log for any replayed write,
		// even when the raw KV payload values themselves remain inline.
		return true, nil
	}
	return false, nil
}

func commandWALRawSetNeedsReplayValueLog(db *DB, key, value []byte) bool {
	threshold := page.DefaultInlineThreshold
	var domains []ValueLogDomainThreshold
	if db != nil {
		threshold = db.InlineThreshold()
		domains = db.valueLogDomainThresholds
	}
	return len(value) > resolveBatchInlineThresholdForKey(threshold, key, domains)
}

func applyCommandWALFrame(db *DB, env commitlog.CommandEnvelope, ridMap map[uint64]page.ValuePtr, inlineAppender *replayInlineAppender, ensureReplayLogSupport commandWALReplayLogSupportFunc) error {
	switch env.Kind {
	case commitlog.CommandKindRawKVBatch:
		return applyRawKVCommandWALFrame(db, env, ridMap, inlineAppender, ensureReplayLogSupport)
	default:
		return commitlog.ErrCommandWALUnsupportedKind
	}
}

func scanValueLogSegments(segments []logSegment, dictLookup valuelog.DictLookup) (map[uint64]page.ValuePtr, error) {
	ridMap := make(map[uint64]page.ValuePtr)
	for _, segment := range segments {
		if !segment.valueLog {
			continue
		}
		reader, err := valuelog.NewReader(segment.path, segment.fileID)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		reader.DisableValueDecode()
		reader.ValidateDicts()
		reader.SetDictLookup(dictLookup)
		for {
			rid, ptr, err := reader.ReadNextMeta()
			if err == nil {
				if _, exists := ridMap[rid]; exists {
					_ = reader.Close()
					return nil, fmt.Errorf("valuelog: duplicate rid %d", rid)
				}
				ridMap[rid] = ptr
				continue
			}
			if isTruncatedLogError(err) {
				break
			}
			_ = reader.Close()
			return nil, err
		}
		if err := reader.Close(); err != nil {
			return nil, err
		}
	}
	return ridMap, nil
}

func replayCommitLogSegments(db *DB, segments []logSegment, ridMap map[uint64]page.ValuePtr, maxSegmentBytes int64) error {
	type commitBatch struct {
		seq     uint64
		order   int
		records []commitlog.Record
	}

	var (
		batches       []commitBatch
		legacyBatches []commitBatch
		commitPaths   []string
		readOrder     int
	)
	for _, segment := range segments {
		if segment.valueLog {
			continue
		}
		reader, err := commitlog.NewReaderWithOptions(segment.path, commitlog.Options{MaxSegmentSize: maxSegmentBytes})
		if err != nil {
			return err
		}
		for {
			records, err := reader.ReadBatch()
			if err == nil {
				if len(records) == 0 {
					continue
				}
				seq, err := commitBatchSeq(records)
				if err != nil {
					_ = reader.Close()
					return err
				}
				batch := commitBatch{seq: seq, order: readOrder, records: records}
				readOrder++
				if seq == 0 {
					// Legacy commit logs don't carry sequence numbers; preserve read order.
					legacyBatches = append(legacyBatches, batch)
				} else {
					batches = append(batches, batch)
				}
				continue
			}
			if isTruncatedLogError(err) {
				// Treat EOF/truncated tail as end-of-segment and continue replaying
				// other segments/lanes. Each segment is replayed as far as valid
				// batches permit.
				break
			}
			_ = reader.Close()
			return err
		}
		if err := reader.Close(); err != nil {
			return err
		}
		commitPaths = append(commitPaths, segment.path)
	}
	if len(batches) > 1 {
		sort.Slice(batches, func(i, j int) bool {
			if batches[i].seq == batches[j].seq {
				return batches[i].order < batches[j].order
			}
			return batches[i].seq < batches[j].seq
		})
	}
	inlineAppender, err := newReplayInlineAppender(db, segments, ridMap)
	if err != nil {
		return err
	}
	defer func() { _ = inlineAppender.close() }()
	if db != nil && db.indexOuterLeavesInValueLog {
		db.SetLeafPageLog(inlineAppender)
	}
	for _, batch := range legacyBatches {
		if err := applyCommitBatch(db, batch.records, ridMap, inlineAppender); err != nil {
			return err
		}
	}
	for _, batch := range batches {
		if !commitFenceSatisfied(batch.records, ridMap) {
			// Sequence-numbered commit batches act as recovery fences. If any RID
			// referenced by the batch is absent from the scanned value-log set, we
			// treat the whole batch as not committed and skip it.
			continue
		}
		if err := applyCommitBatch(db, batch.records, ridMap, inlineAppender); err != nil {
			return err
		}
	}
	if err := inlineAppender.syncIfDirty(); err != nil {
		return err
	}
	if err := inlineAppender.close(); err != nil {
		return err
	}
	for _, path := range commitPaths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func commitBatchSeq(records []commitlog.Record) (uint64, error) {
	if len(records) == 0 {
		return 0, nil
	}
	seq := records[0].Seq
	for i := 1; i < len(records); i++ {
		if records[i].Seq != seq {
			return 0, fmt.Errorf("%w: first=%d index=%d got=%d", commitlog.ErrMixedBatchSeq, seq, i, records[i].Seq)
		}
	}
	return seq, nil
}

func commitFenceSatisfied(records []commitlog.Record, ridMap map[uint64]page.ValuePtr) bool {
	for _, rec := range records {
		if rec.Op != commitlog.OpSetRID {
			continue
		}
		if _, ok := ridMap[rec.RID]; !ok {
			return false
		}
	}
	return true
}

type replayInlineAppender struct {
	writer  *rewriteWriter
	nextRID uint64
	dirty   bool
}

func newReplayInlineAppender(db *DB, segments []logSegment, ridMap map[uint64]page.ValuePtr) (*replayInlineAppender, error) {
	if db == nil {
		return nil, fmt.Errorf("missing db")
	}
	var maxLane0Seq uint32
	for _, seg := range segments {
		if !seg.valueLog || seg.lane != 0 {
			continue
		}
		if seg.seq > uint64(maxLane0Seq) {
			if seg.seq > uint64(^uint32(0)) {
				return nil, fmt.Errorf("valuelog: lane 0 sequence overflow %d", seg.seq)
			}
			maxLane0Seq = uint32(seg.seq)
		}
	}
	var maxRID uint64
	for rid := range ridMap {
		if rid > maxRID {
			maxRID = rid
		}
	}
	if maxRID == ^uint64(0) {
		return nil, fmt.Errorf("value-log rid space exhausted")
	}
	maxSegmentBytes := int64(0)
	if db.indexPackedValuePtr || db.indexOuterLeavesInValueLog {
		// Packed ValuePtr stores offset as u32; keep replay-appended value-log
		// segments within the same cap used by the write path.
		maxSegmentBytes = int64(^uint32(0)) - 4
	}
	layout := resolveStorageLayout(db.dir)
	writer := newRewriteWriter(layout.valueVLogDir, 0, maxLane0Seq, maxSegmentBytes)
	if db.indexOuterLeavesInValueLog {
		writer.ConfigureLeafLog(layout.leafVLogDir, rewriteLeafLogLaneID, maxRewriteLaneSeq(segments, rewriteLeafLogLaneID))
	}
	writer.blockCompression = db.valueLogCompression != ValueLogCompressionOff
	writer.blockCodec = valuelogBlockCodecFromDB(db.valueLogBlockCodec)
	writer.leafBlockCodec = leafPageBlockCodecFromOptions(db.valueLogCompression, db.valueLogAutoPolicy, db.valueLogBlockCodec, db.indexOuterLeavesInValueLog)
	return &replayInlineAppender{
		writer:  writer,
		nextRID: maxRID + 1,
	}, nil
}

func (a *replayInlineAppender) append(value []byte) (page.ValuePtr, error) {
	if a == nil || a.writer == nil {
		return page.ValuePtr{}, fmt.Errorf("commitlog: replay value-log appender unavailable")
	}
	if a.nextRID == 0 {
		return page.ValuePtr{}, fmt.Errorf("value-log rid space exhausted")
	}
	rid := a.nextRID
	a.nextRID++
	ptr, err := a.writer.appendValue(rid, value)
	if err != nil {
		return page.ValuePtr{}, err
	}
	a.dirty = true
	return ptr, nil
}

func (a *replayInlineAppender) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	if a == nil || a.writer == nil {
		return page.LeafLogPtr{}, fmt.Errorf("commitlog: replay value-log appender unavailable")
	}
	if a.nextRID == 0 {
		return page.LeafLogPtr{}, fmt.Errorf("value-log rid space exhausted")
	}
	rid := a.nextRID
	a.nextRID++
	leafPtr, err := a.writer.appendLeafPageWithRID(rid, leafPage)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	a.dirty = true
	return leafPtr, nil
}

func (a *replayInlineAppender) LastLeafPageRecordLength() uint32 {
	if a == nil || a.writer == nil {
		return 0
	}
	return a.writer.LastLeafPageRecordLength()
}

func (a *replayInlineAppender) Flush() error {
	if a == nil || a.writer == nil || !a.dirty {
		return nil
	}
	if err := a.writer.Flush(); err != nil {
		return err
	}
	a.dirty = false
	return nil
}

func (a *replayInlineAppender) Sync() error {
	return a.syncIfDirty()
}

func (a *replayInlineAppender) syncIfDirty() error {
	if a == nil || a.writer == nil || !a.dirty {
		return nil
	}
	if err := a.writer.Sync(); err != nil {
		return err
	}
	a.dirty = false
	return nil
}

func (a *replayInlineAppender) close() error {
	if a == nil || a.writer == nil {
		return nil
	}
	if err := a.writer.Close(); err != nil {
		return err
	}
	a.writer = nil
	a.dirty = false
	return nil
}

func applyCommitBatch(db *DB, records []commitlog.Record, ridMap map[uint64]page.ValuePtr, inlineAppender *replayInlineAppender) error {
	if len(records) == 0 {
		return nil
	}
	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()

	ptrBatch, hasPtrBatch := batch.(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
	})

	for _, rec := range records {
		switch rec.Op {
		case commitlog.OpDelete:
			if err := batch.Delete(rec.Key); err != nil {
				return err
			}
		case commitlog.OpSetInline:
			if err := batch.Set(rec.Key, rec.Value); err != nil {
				if !errors.Is(err, batchpkg.ErrValueTooLarge) {
					// Abort this replay attempt on non-placement errors. The
					// commit batch remains unapplied and the next open retries
					// from the last published root.
					return err
				}
				if !hasPtrBatch {
					return fmt.Errorf("commitlog: pointer batch unavailable")
				}
				if inlineAppender == nil {
					return fmt.Errorf("commitlog: missing replay value-log appender")
				}
				ptr, err := inlineAppender.append(rec.Value)
				if err != nil {
					return err
				}
				if err := ptrBatch.SetPointer(rec.Key, ptr); err != nil {
					return err
				}
			}
		case commitlog.OpSetRID:
			ptr, ok := ridMap[rec.RID]
			if !ok {
				return fmt.Errorf("commitlog: missing rid %d", rec.RID)
			}
			if !hasPtrBatch {
				return fmt.Errorf("commitlog: pointer batch unavailable")
			}
			if err := ptrBatch.SetPointer(rec.Key, ptr); err != nil {
				return err
			}
		default:
			return fmt.Errorf("commitlog: unknown op %d", rec.Op)
		}
	}
	if err := inlineAppender.syncIfDirty(); err != nil {
		return err
	}

	if err := batch.WriteSync(); err != nil {
		return err
	}
	return nil
}
