package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
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

func legacyCachedRedoJournalReplaySegments(db *DB, segments []logSegment, maxSegmentBytes int64) ([]logSegment, bool, error) {
	if db != nil {
		filtered, err := filterCommandWALSegmentsForLegacyReplay(segments, db.meta.AppliedCommandLSN, maxSegmentBytes)
		if err != nil {
			return nil, false, err
		}
		segments = filtered
	}
	for _, seg := range segments {
		if seg.valueLog || seg.size == 0 {
			continue
		}
		hasLegacyBatch, err := segmentHasReplayableLegacyBatch(seg, maxSegmentBytes)
		if err != nil {
			return nil, false, err
		}
		if hasLegacyBatch {
			return segments, true, nil
		}
	}
	return segments, false, nil
}

func segmentHasReplayableLegacyBatch(seg logSegment, maxSegmentBytes int64) (bool, error) {
	r, err := commitlog.NewReaderWithOptions(seg.path, commitlog.Options{MaxSegmentSize: maxSegmentBytes})
	if err != nil {
		return false, err
	}
	defer r.Close()
	_, err = r.ReadBatch()
	if err == nil {
		return true, nil
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, commitlog.ErrCommandWALTerminalTail) {
		return false, nil
	}
	return false, fmt.Errorf("scan legacy cached redo journal segment %s: %w", filepath.Base(seg.path), err)
}

func legacyCachedRedoJournalReplayDisabledError(segments []logSegment, maxSegmentBytes int64) error {
	for _, seg := range segments {
		if seg.valueLog || seg.size == 0 {
			continue
		}
		hasLegacyBatch, err := segmentHasReplayableLegacyBatch(seg, maxSegmentBytes)
		if err != nil || !hasLegacyBatch {
			continue
		}
		return fmt.Errorf("%w: %w: legacy cached redo journal segment %s requires explicit compatibility replay",
			ErrRecoveryRequired, ErrLegacyCachedRedoJournalReplayDisabled, filepath.Base(seg.path))
	}
	return fmt.Errorf("%w: %w", ErrRecoveryRequired, ErrLegacyCachedRedoJournalReplayDisabled)
}

func requireNoLegacyCachedRedoJournalReplay(dir string, db *DB, maxSegmentBytes int64) error {
	segments, err := listRecoverySegments(dir)
	if err != nil {
		return err
	}
	segments, hasLegacyRedoJournal, err := legacyCachedRedoJournalReplaySegments(db, segments, maxSegmentBytes)
	if err != nil {
		return err
	}
	if !hasLegacyRedoJournal {
		return nil
	}
	return legacyCachedRedoJournalReplayDisabledError(segments, maxSegmentBytes)
}

type commandWALReplayFrame struct {
	env          commitlog.CommandEnvelope
	requiredRIDs []uint64
}

type commandWALReplayRIDMapFunc func() (map[uint64]page.ValuePtr, error)
type commandWALReplayLogSupportFunc func() (map[uint64]page.ValuePtr, *replayInlineAppender, error)

func replayCommandWALIntoBackend(db *DB, segments []logSegment, maxSegmentBytes int64, dictLookup valuelog.DictLookup) error {
	if db == nil {
		return fmt.Errorf("treedb: command wal recovery missing db")
	}
	db.mu.RLock()
	applied := db.meta.AppliedCommandLSN
	db.mu.RUnlock()
	physicalFrames, err := readCommandWALV2PhysicalFrames(segments, applied, maxSegmentBytes)
	if err != nil {
		return err
	}
	var ridMap map[uint64]page.ValuePtr
	needsRIDMap := false
	for i := range physicalFrames {
		if len(physicalFrames[i].RequiredRIDs) != 0 {
			needsRIDMap = true
			break
		}
	}
	if needsRIDMap {
		ridMap, err = scanValueLogSegments(segments, dictLookup)
		if err != nil {
			return err
		}
	}
	classification, err := classifyCommandWALV2Frames(physicalFrames, applied, func(rid uint64) bool {
		_, ok := ridMap[rid]
		return ok
	})
	if err != nil {
		return err
	}
	if _, err := repairCommandWALV2Suffix(WALDirPath(db.dir), classification, db.readOnly); err != nil {
		return err
	}
	db.commandWALDurableLSN.Store(classification.DurableFrontier)
	frames := make([]commandWALReplayFrame, 0, len(classification.CompletePrefix))
	for i := range classification.CompletePrefix {
		frames = append(frames, commandWALReplayFrame{
			env:          classification.CompletePrefix[i].Envelope,
			requiredRIDs: classification.CompletePrefix[i].RequiredRIDs,
		})
	}
	if len(frames) == 0 {
		return nil
	}
	pendingMaterializedRIDHighWater, err := commandWALReplayPendingMaterializedRIDHighWater(frames, applied)
	if err != nil {
		return err
	}
	replayDependencies, err := captureCommandWALReplayRelaxedDependencies(db, frames, classification.DurableFrontier, ridMap)
	if err != nil {
		return err
	}
	if replayDependencies != nil {
		defer replayDependencies.Release()
		if db.testCommandWALRecoveryFailBeforeDependencySync.Swap(false) {
			return errTestCommandWALRecoveryDependencySyncFailpoint
		}
		if err := replayDependencies.SyncThrough(); err != nil {
			return err
		}
		if err := stabilizeCommandWALResourceNamespaces(replayDependencies); err != nil {
			return err
		}
	}
	var inlineAppender *replayInlineAppender
	previousLeafPageLog := db.leafPageLog
	previousValueLogAppender := db.currentValueLogAppender()
	leafPageLogInstalled := false
	valueLogAppenderInstalled := false
	restoreLeafPageLog := func() {
		if leafPageLogInstalled {
			db.SetLeafPageLog(previousLeafPageLog)
			leafPageLogInstalled = false
		}
	}
	ensureReplayRIDMap := func() (map[uint64]page.ValuePtr, error) {
		if ridMap != nil {
			return ridMap, nil
		}
		var err error
		ridMap, err = scanValueLogSegments(segments, dictLookup)
		if err != nil {
			return nil, err
		}
		return ridMap, nil
	}
	restoreValueLogAppender := func() {
		if valueLogAppenderInstalled {
			db.SetValueLogAppender(previousValueLogAppender)
			valueLogAppenderInstalled = false
		}
	}
	ensureReplayLogSupport := func() (map[uint64]page.ValuePtr, *replayInlineAppender, error) {
		if inlineAppender != nil {
			return ridMap, inlineAppender, nil
		}
		var err error
		ridMap, err = ensureReplayRIDMap()
		if err != nil {
			return nil, nil, err
		}
		inlineAppender, err = newReplayInlineAppender(db, segments, ridMap)
		if err != nil {
			return nil, nil, err
		}
		if pendingMaterializedRIDHighWater >= inlineAppender.nextRID {
			if pendingMaterializedRIDHighWater == ^uint64(0) {
				_ = inlineAppender.close()
				inlineAppender = nil
				return nil, nil, fmt.Errorf("value-log rid space exhausted")
			}
			inlineAppender.nextRID = pendingMaterializedRIDHighWater + 1
		}
		db.SetValueLogAppender(inlineAppender)
		valueLogAppenderInstalled = true
		db.SetLeafPageLog(replayInlineLeafPageLog{appender: inlineAppender})
		leafPageLogInstalled = true
		return ridMap, inlineAppender, nil
	}
	defer func() {
		if inlineAppender != nil {
			_ = inlineAppender.close()
		}
		restoreValueLogAppender()
		restoreLeafPageLog()
	}()
	needsLogSupport, err := commandWALReplayFramesNeedLogSupport(db, frames, applied)
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
		if err := applyCommandWALFrame(db, frame.env, ridMap, inlineAppender, ensureReplayRIDMap, ensureReplayLogSupport); err != nil {
			return fmt.Errorf(
				"treedb: replay command WAL frame lsn=%d kind=%d: %w",
				frame.env.LSN,
				frame.env.Kind,
				err,
			)
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
		restoreValueLogAppender()
		restoreLeafPageLog()
	}
	return nil
}

// captureCommandWALReplayRelaxedDependencies reconstructs the exact physical
// value-log closure for the complete relaxed suffix that lies above the last
// durable command-WAL frontier. Recovery publishes replayed roots durably, so
// these transitive dependencies must cross their file barrier first; retaining
// the returned set also prevents deletion until replay has finished.
func captureCommandWALReplayRelaxedDependencies(db *DB, frames []commandWALReplayFrame, durableFrontier uint64, ridMap map[uint64]page.ValuePtr) (*rootpublication.StableResourceSet, error) {
	uniqueRIDs := make(map[uint64]struct{})
	for i := range frames {
		if frames[i].env.LSN <= durableFrontier {
			continue
		}
		for _, rid := range frames[i].requiredRIDs {
			uniqueRIDs[rid] = struct{}{}
		}
	}
	if len(uniqueRIDs) == 0 {
		return nil, nil
	}
	if db == nil || db.valueLogManager == nil {
		return nil, fmt.Errorf("%w: command WAL relaxed replay has no exact value-log producer closure", rootpublication.ErrUnresolvedResource)
	}

	entries := make([]rawKVCommandWALRIDCacheEntry, 0, len(uniqueRIDs))
	rids := make([]uint64, 0, len(uniqueRIDs))
	for rid := range uniqueRIDs {
		ptr, ok := ridMap[rid]
		if !ok {
			return nil, fmt.Errorf("%w: command WAL relaxed replay missing RID %d", ErrCommandWALMissingValueLogRID, rid)
		}
		entries = append(entries, rawKVCommandWALRIDCacheEntry{ptr: ptr, rid: rid})
		rids = append(rids, rid)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ptr.FileID != entries[j].ptr.FileID {
			return entries[i].ptr.FileID < entries[j].ptr.FileID
		}
		if entries[i].ptr.Offset != entries[j].ptr.Offset {
			return entries[i].ptr.Offset < entries[j].ptr.Offset
		}
		if entries[i].ptr.Length != entries[j].ptr.Length {
			return entries[i].ptr.Length < entries[j].ptr.Length
		}
		return entries[i].rid < entries[j].rid
	})

	segments := make([]valuelog.StableExternalRIDSegment, 0)
	for _, entry := range entries {
		if len(segments) == 0 || segments[len(segments)-1].FileID != entry.ptr.FileID {
			// Durable-root recovery registers only the selected generation's
			// manifest closure. A relaxed command-WAL suffix may legitimately
			// reference a newer value-log child that is not reachable from that
			// root yet, so bind the already-scanned RID pointer to its canonical
			// manager identity before asking the producer to capture and sync it.
			if err := db.registerReplayValueLogPointer(entry.ptr); err != nil {
				return nil, fmt.Errorf("register command WAL replay dependency: %w", err)
			}
			segments = append(segments, valuelog.StableExternalRIDSegment{
				FileID: entry.ptr.FileID,
				Digest: stableExternalRIDSegmentDigest(entry.ptr.FileID),
			})
		}
		segment := &segments[len(segments)-1]
		segment.RIDs = append(segment.RIDs, entry.rid)
		segment.Pointers = append(segment.Pointers, entry.ptr)
	}
	fence, err := valuelog.NewStableExternalRIDFence(rids)
	if err != nil {
		return nil, err
	}
	return db.valueLogManager.CaptureStableExternalRIDFence(fence, segments)
}

func readCommandWALReplayFrames(segments []logSegment, appliedLSN uint64, maxSegmentBytes int64) ([]commandWALReplayFrame, error) {
	activeByLane := commandWALActiveSeqByLane(segments)
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
		var lastLSN uint64
		for {
			env, err := reader.ReadCommandFrame()
			if err == nil {
				if lastLSN != 0 && env.LSN <= lastLSN {
					_ = reader.Close()
					return nil, commitlog.ErrCommandWALDuplicateLSN
				}
				lastLSN = env.LSN
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

func commandWALLaneActiveHasTerminalTail(segments []logSegment, lane int, maxSegmentBytes int64) (bool, error) {
	activeByLane := commandWALActiveSeqByLane(segments)
	activeSeq := activeByLane[lane]
	if activeSeq == 0 {
		return false, nil
	}
	for _, seg := range segments {
		if seg.valueLog || seg.lane != lane || seg.seq != activeSeq || !isCommandWALLaneSegment(seg) {
			continue
		}
		reader, err := commitlog.NewReaderWithOptions(seg.path, commitlog.Options{MaxSegmentSize: maxSegmentBytes})
		if err != nil {
			return false, err
		}
		for {
			_, err := reader.ReadCommandFrame()
			if err == nil {
				continue
			}
			_ = reader.Close()
			if errors.Is(err, io.EOF) {
				return false, nil
			}
			if errors.Is(err, commitlog.ErrCommandWALTerminalTail) {
				return true, nil
			}
			return false, err
		}
	}
	return false, nil
}

func commandWALReplayFramesNeedLogSupport(db *DB, frames []commandWALReplayFrame, applied uint64) (bool, error) {
	hasUnappliedRootPublishingFrame := false
	for _, frame := range frames {
		if frame.env.LSN <= applied {
			continue
		}
		if frame.env.Kind == commitlog.CommandKindDurablePrefixBarrier {
			hasUnappliedRootPublishingFrame = true
			continue
		}
		if frame.env.Kind != commitlog.CommandKindRawKVBatch {
			continue
		}
		hasUnappliedRootPublishingFrame = true
		needsLogSupport := false
		if err := commitlog.ScanRawKVBatchPayload(frame.env.Payload, func(op commitlog.RawKVOp, key, value []byte) error {
			if op == commitlog.RawKVOpSetMaterializedRID ||
				(op == commitlog.RawKVOpSet && commandWALRawSetNeedsReplayValueLog(db, key, value)) {
				needsLogSupport = true
			}
			return nil
		}); err != nil {
			return false, err
		}
		if needsLogSupport {
			return true, nil
		}
	}
	if db != nil && db.indexOuterLeavesInValueLog {
		// Outer-leaf mode needs a replay leaf-page log for every frame that can
		// publish roots, even when the payload itself carries no values. A durable
		// prefix barrier republishes the current roots at a new applied LSN and can
		// therefore allocate an outer leaf while replaying an otherwise root-neutral
		// command.
		return hasUnappliedRootPublishingFrame, nil
	}
	return false, nil
}

func commandWALReplayPendingMaterializedRIDHighWater(frames []commandWALReplayFrame, applied uint64) (uint64, error) {
	var highWater uint64
	for _, frame := range frames {
		if frame.env.LSN <= applied || frame.env.Kind != commitlog.CommandKindRawKVBatch {
			continue
		}
		if err := commitlog.ScanRawKVBatchPayload(frame.env.Payload, func(op commitlog.RawKVOp, _, value []byte) error {
			if op != commitlog.RawKVOpSetMaterializedRID {
				return nil
			}
			rid := binary.LittleEndian.Uint64(value[:8])
			if rid > highWater {
				highWater = rid
			}
			return nil
		}); err != nil {
			return 0, err
		}
	}
	return highWater, nil
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

func applyCommandWALFrame(db *DB, env commitlog.CommandEnvelope, ridMap map[uint64]page.ValuePtr, inlineAppender *replayInlineAppender, ensureReplayRIDMap commandWALReplayRIDMapFunc, ensureReplayLogSupport commandWALReplayLogSupportFunc) error {
	switch env.Kind {
	case commitlog.CommandKindRawKVBatch:
		return applyRawKVCommandWALFrame(db, env, ridMap, inlineAppender, ensureReplayRIDMap, ensureReplayLogSupport)
	case commitlog.CommandKindDurablePrefixBarrier:
		if env.Version != commitlog.CommandFrameVersionV2 || env.DurabilityClass != commitlog.CommandDurabilityDurable || env.PayloadFormat != commitlog.PayloadFormatDurablePrefixBarrierV1 {
			return commitlog.ErrCorrupt
		}
		db.mu.RLock()
		userRoot := db.meta.UserRootPageID
		systemRoot := db.meta.SystemRootPageID
		db.mu.RUnlock()
		return db.publishCommandWALRoots(userRoot, systemRoot, env.LSN, []CommandWALLSNRange{{First: env.LSN, Last: env.LSN}}, true)
	default:
		if registration, ok := lookupCommandWALReplayHandler(env.Kind); ok {
			if registration.needsReplayLogSupport && ensureReplayLogSupport != nil {
				if _, _, err := ensureReplayLogSupport(); err != nil {
					return err
				}
			}
			return db.applyRegisteredCommandWALFrame(env, registration)
		}
		return commitlog.ErrCommandWALUnsupportedKind
	}
}

func (db *DB) applyRegisteredCommandWALFrame(env commitlog.CommandEnvelope, registration commandWALReplayHandlerRegistration) error {
	db.ensureCommandWALRecoverySnapshotView()
	previousReplayLSN := db.commandWALReplayLSN.Swap(env.LSN)
	replayToken := db.nextCommandWALReplayToken()
	previousReplayToken := db.commandWALReplayToken.Swap(replayToken)
	// Keep the DB handle usable if an external replay handler panics and the
	// caller recovers above the replay loop.
	restored := false
	defer func() {
		if r := recover(); r != nil {
			if !restored {
				db.restoreCommandWALReplayFrame(previousReplayLSN, previousReplayToken)
			}
			panic(r)
		}
	}()
	err := registration.handler(db, env)
	db.restoreCommandWALReplayFrame(previousReplayLSN, previousReplayToken)
	restored = true
	return err
}

func (db *DB) restoreCommandWALReplayFrame(previousReplayLSN, previousReplayToken uint64) {
	db.commandWALReplayToken.Store(previousReplayToken)
	db.commandWALReplayLSN.Store(previousReplayLSN)
}

func (db *DB) nextCommandWALReplayToken() uint64 {
	token := db.commandWALReplayTokenSeq.Add(1)
	if token == 0 {
		token = db.commandWALReplayTokenSeq.Add(1)
	}
	return token
}

func (db *DB) ensureCommandWALRecoverySnapshotView() {
	if db == nil || db.state.Load() != nil {
		return
	}
	db.mu.RLock()
	state := &DBState{
		CommitSeq:                  db.meta.CommitSeq,
		RootPageID:                 db.meta.UserRootPageID,
		SystemRootPageID:           db.meta.SystemRootPageID,
		AppliedCommandLSN:          db.meta.AppliedCommandLSN,
		MaxEntryRevision:           page.EntryRevision(db.meta.MaxEntryRevision),
		LeafGenerations:            db.currentLeafGenerationView(),
		LeafGenerationStateVersion: db.leafGenerationStateVersion,
	}
	db.mu.RUnlock()
	if db.valueLogManager != nil {
		state.ValueLogSet = db.valueLogManager.CurrentSetNoRefresh()
	}
	if db.state.CompareAndSwap(nil, state) {
		db.publishSnapshotView(db.idx.Load(), state, db.valueLogManager)
		return
	}
	if state.ValueLogSet != nil && db.valueLogManager != nil {
		_ = db.valueLogManager.Release(state.ValueLogSet)
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

// registerReplayValueLogPointer makes the exact physical segment resolved from
// an RID-bearing recovery record available to bounded durable-root publication.
// Recovery first proves the RID-to-pointer mapping by reading the segment and
// then registers that single canonical identity. This is not a publisher
// discovery fallback: no candidate path is statted or scanned after the pointer
// becomes reachable.
func (db *DB) registerReplayValueLogPointer(ptr page.ValuePtr) error {
	if db == nil || db.valueLogManager == nil || ptr.FileID == 0 {
		return nil
	}
	if db.valueLogManager.HasSegment(ptr.FileID) {
		return nil
	}
	lane, _ := valuelog.DecodeFileID(ptr.FileID)
	dir := ValueLogDirPath(db.dir)
	if lane == valuelog.ReservedLeafLogLaneID {
		dir = LeafLogDirPath(db.dir)
	}
	path := valuelog.SegmentPath(dir, ptr.FileID)
	if err := db.valueLogManager.RegisterSegment(path, ptr.FileID); err != nil {
		return fmt.Errorf("register replay value-log file %d: %w", ptr.FileID, err)
	}
	return nil
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
		db.SetLeafPageLog(replayInlineLeafPageLog{appender: inlineAppender})
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
		if _, err := removePersistentFile(filepath.Dir(path), path, durabilitycut.ResourceCommandWAL); err != nil {
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
	mu      sync.Mutex
	db      *DB
	writer  *rewriteWriter
	nextRID uint64
	dirty   bool
}

var _ ValueLogRecordReader = (*replayInlineAppender)(nil)

func newReplayInlineAppender(db *DB, segments []logSegment, ridMap map[uint64]page.ValuePtr) (*replayInlineAppender, error) {
	var maxRID uint64
	for rid := range ridMap {
		if rid > maxRID {
			maxRID = rid
		}
	}
	if maxRID == ^uint64(0) {
		return nil, fmt.Errorf("value-log rid space exhausted")
	}
	return newReplayInlineAppenderWithNextRID(db, segments, maxRID+1)
}

func newReplayInlineAppenderWithNextRID(db *DB, segments []logSegment, nextRID uint64) (*replayInlineAppender, error) {
	if db == nil {
		return nil, fmt.Errorf("missing db")
	}
	if nextRID == 0 {
		return nil, fmt.Errorf("value-log rid space exhausted")
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
	maxSegmentBytes := int64(0)
	if db.indexPackedValuePtr || db.indexOuterLeavesInValueLog {
		// Packed ValuePtr stores offset as u32; keep replay-appended value-log
		// segments within the same cap used by the write path.
		maxSegmentBytes = int64(^uint32(0)) - 4
	}
	layout := resolveStorageLayout(db.dir)
	writer := newRewriteWriter(layout.valueVLogDir, 0, maxLane0Seq, maxSegmentBytes)
	writer.ConfigureLeafLog(layout.leafVLogDir, rewriteLeafLogLaneID, maxRewriteLaneSeq(segments, rewriteLeafLogLaneID))
	writer.blockCompression = db.valueLogCompression != ValueLogCompressionOff
	writer.blockCodec = valuelogBlockCodecFromDB(db.valueLogBlockCodec)
	writer.leafBlockCodec = leafPageBlockCodecFromOptions(db.valueLogCompression, db.valueLogAutoPolicy, db.valueLogBlockCodec, db.indexOuterLeavesInValueLog)
	return &replayInlineAppender{
		db:      db,
		writer:  writer,
		nextRID: nextRID,
	}, nil
}

func nextReplayAppenderRIDStart(segments []logSegment) (uint64, error) {
	// RIDs are allocated from one monotonically increasing namespace. The appender
	// only needs the high-water mark, so scan every value-log segment without
	// rebuilding a full RID->pointer map. Recovery may append a missing exact RID
	// below the current high-water into the newest lane-0 segment; consulting only
	// the latest segment per lane after that repair would let allocation move
	// backwards and reuse an existing RID on the following open.
	return nextRewriteRIDStart(segments)
}

func (a *replayInlineAppender) append(value []byte) (page.ValuePtr, error) {
	if a == nil {
		return page.ValuePtr{}, fmt.Errorf("commitlog: replay value-log appender unavailable")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.appendLocked(value)
}

func (a *replayInlineAppender) appendLocked(value []byte) (page.ValuePtr, error) {
	if a == nil || a.writer == nil {
		return page.ValuePtr{}, fmt.Errorf("commitlog: replay value-log appender unavailable")
	}
	rid, err := a.reserveAppendRIDsLocked(1)
	if err != nil {
		return page.ValuePtr{}, err
	}
	ptr, err := a.writer.appendValue(rid, value)
	if err != nil {
		return page.ValuePtr{}, err
	}
	if err := a.registerProducedPointerLocked(ptr); err != nil {
		return page.ValuePtr{}, err
	}
	a.dirty = true
	return ptr, nil
}

func (a *replayInlineAppender) appendExactRID(rid uint64, value []byte) (page.ValuePtr, error) {
	if a == nil {
		return page.ValuePtr{}, fmt.Errorf("commitlog: replay value-log appender unavailable")
	}
	if rid == 0 {
		return page.ValuePtr{}, fmt.Errorf("value-log rid must be non-zero")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.writer == nil {
		return page.ValuePtr{}, fmt.Errorf("commitlog: replay value-log appender unavailable")
	}
	if rid >= a.nextRID && rid == ^uint64(0) {
		return page.ValuePtr{}, fmt.Errorf("value-log rid space exhausted")
	}
	ptr, err := a.writer.appendValue(rid, value)
	if err != nil {
		return page.ValuePtr{}, err
	}
	if err := a.registerProducedPointerLocked(ptr); err != nil {
		return page.ValuePtr{}, err
	}
	if rid >= a.nextRID {
		a.nextRID = rid + 1
	}
	a.dirty = true
	return ptr, nil
}

func (a *replayInlineAppender) AppendValues(values [][]byte) ([]page.ValuePtr, error) {
	if a == nil {
		return nil, fmt.Errorf("commitlog: replay value-log appender unavailable")
	}
	ptrs := make([]page.ValuePtr, len(values))
	if len(values) == 0 {
		return ptrs, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.writer == nil {
		return nil, fmt.Errorf("commitlog: replay value-log appender unavailable")
	}
	startRID, err := a.reserveAppendRIDsLocked(len(values))
	if err != nil {
		return nil, err
	}
	for i := range values {
		ptr, err := a.writer.appendValue(startRID+uint64(i), values[i])
		if err != nil {
			return nil, err
		}
		if err := a.registerProducedPointerLocked(ptr); err != nil {
			return nil, err
		}
		ptrs[i] = ptr
	}
	a.dirty = true
	return ptrs, nil
}

func (a *replayInlineAppender) ReserveRIDs(count int) (uint64, error) {
	if a == nil {
		return 0, fmt.Errorf("commitlog: replay value-log appender unavailable")
	}
	if err := validateRewriteRIDCount(count); err != nil {
		return 0, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.writer == nil {
		return 0, fmt.Errorf("commitlog: replay value-log appender unavailable")
	}
	return a.reserveLocalRIDsLocked(count)
}

func (a *replayInlineAppender) reserveLocalRIDsLocked(count int) (uint64, error) {
	start := a.nextRID
	if start == 0 {
		return 0, fmt.Errorf("value-log rid space exhausted")
	}
	if err := validateRewriteRIDRange(start, count); err != nil {
		return 0, err
	}
	a.nextRID = start + uint64(count)
	return start, nil
}

// reserveAppendRIDsLocked keeps command-WAL inline leaf/value appends in the
// same RID namespace as any appender installed after replay, such as cached
// mode's persistent value-log appender.
func (a *replayInlineAppender) reserveAppendRIDsLocked(count int) (uint64, error) {
	if a != nil && a.db != nil {
		if reserver := a.db.currentValueLogRIDReserver(); reserver != nil {
			if current, ok := reserver.(*replayInlineAppender); !ok || current != a {
				return reserver.ReserveRIDs(count)
			}
		}
	}
	return a.reserveLocalRIDsLocked(count)
}

func (a *replayInlineAppender) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	if a == nil {
		return page.LeafLogPtr{}, fmt.Errorf("commitlog: replay value-log appender unavailable")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a == nil || a.writer == nil {
		return page.LeafLogPtr{}, fmt.Errorf("commitlog: replay value-log appender unavailable")
	}
	rid, err := a.reserveAppendRIDsLocked(1)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	leafPtr, err := a.writer.appendLeafPageWithRID(rid, leafPage)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	if err := a.registerProducedPointerLocked(leafPtr.ValuePtr()); err != nil {
		return page.LeafLogPtr{}, err
	}
	a.dirty = true
	return leafPtr, nil
}

func (a *replayInlineAppender) registerProducedPointerLocked(ptr page.ValuePtr) error {
	if a == nil || a.db == nil || a.db.valueLogManager == nil || ptr.FileID == 0 {
		return nil
	}
	lane, _ := valuelog.DecodeFileID(ptr.FileID)
	dir := ValueLogDirPath(a.db.dir)
	if lane == valuelog.ReservedLeafLogLaneID {
		dir = LeafLogDirPath(a.db.dir)
	}
	if err := a.db.RegisterValueLogSegment(valuelog.SegmentPath(dir, ptr.FileID), ptr.FileID); err != nil {
		return fmt.Errorf("register produced replay value-log file %d: %w", ptr.FileID, err)
	}
	return nil
}

func (a *replayInlineAppender) LastLeafPageRecordLength() uint32 {
	if a == nil {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a == nil || a.writer == nil {
		return 0
	}
	return a.writer.LastLeafPageRecordLength()
}

func (a *replayInlineAppender) Flush() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a == nil || a.writer == nil || !a.dirty {
		return nil
	}
	if err := a.writer.Flush(); err != nil {
		return err
	}
	return nil
}

func (a *replayInlineAppender) ReadValueLogRecordAppend(ptr page.ValuePtr, dst []byte) ([]byte, error) {
	if a == nil {
		return nil, ErrValueLogReaderUnavailable
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.writer == nil || a.db == nil || a.db.valueLogManager == nil {
		return nil, ErrValueLogReaderUnavailable
	}
	if err := a.writer.Flush(); err != nil {
		return nil, err
	}
	return a.db.valueLogManager.ReadAppend(ptr, dst)
}

func (a *replayInlineAppender) Sync() error {
	return a.syncIfDirty()
}

func (a *replayInlineAppender) CurrentValueLogSegment() (string, uint32, bool) {
	if a == nil {
		return "", 0, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a == nil || a.writer == nil {
		return "", 0, false
	}
	return a.writer.currentPrimaryValueLogSegment()
}

func (a *replayInlineAppender) currentLeafValueLogSegment() (string, uint32, bool) {
	if a == nil {
		return "", 0, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.writer == nil {
		return "", 0, false
	}
	return a.writer.currentLeafValueLogSegment()
}

func (a *replayInlineAppender) advanceLeafLogSeqAtLeast(seq uint32) error {
	if a == nil || seq == 0 {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.writer == nil {
		return nil
	}
	return a.writer.resetLeafLogSeqAtLeast(seq)
}

func (a *replayInlineAppender) syncIfDirty() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.syncIfDirtyLocked()
}

func (a *replayInlineAppender) syncIfDirtyLocked() error {
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
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
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

type replayInlineLeafPageLog struct {
	appender *replayInlineAppender
}

func (l replayInlineLeafPageLog) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	if l.appender == nil {
		return page.LeafLogPtr{}, fmt.Errorf("commitlog: replay leaf-page log unavailable")
	}
	return l.appender.AppendLeafPage(leafPage)
}

func (l replayInlineLeafPageLog) Flush() error {
	if l.appender == nil {
		return nil
	}
	return l.appender.Flush()
}

func (l replayInlineLeafPageLog) Sync() error {
	if l.appender == nil {
		return nil
	}
	return l.appender.Sync()
}

func (l replayInlineLeafPageLog) CurrentValueLogSegment() (string, uint32, bool) {
	if l.appender == nil {
		return "", 0, false
	}
	return l.appender.currentLeafValueLogSegment()
}

func (l replayInlineLeafPageLog) LastLeafPageRecordLength() uint32 {
	if l.appender == nil {
		return 0
	}
	return l.appender.LastLeafPageRecordLength()
}

func (l replayInlineLeafPageLog) AdvanceCompactStorageLeafPageLogSeqAtLeast(seq uint32) error {
	if l.appender == nil {
		return nil
	}
	return l.appender.advanceLeafLogSeqAtLeast(seq)
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
	revisionBatch, hasRevisionBatch := batch.(interface {
		SetWithRevision(key, value []byte, revision page.EntryRevision) error
		DeleteWithRevision(key []byte, revision page.EntryRevision) error
	})
	ptrRevisionBatch, hasPtrRevisionBatch := batch.(interface {
		SetPointerWithRevision(key []byte, ptr page.ValuePtr, revision page.EntryRevision) error
	})

	recordRevision := func(rec commitlog.Record) page.EntryRevision {
		if rec.Revision != 0 {
			return page.EntryRevision(rec.Revision)
		}
		if rec.Seq != 0 {
			return page.EntryRevision(rec.Seq)
		}
		return page.LegacyEntryRevision
	}

	for _, rec := range records {
		revision := recordRevision(rec)
		switch rec.Op {
		case commitlog.OpDelete:
			if revision != page.LegacyEntryRevision {
				if !hasRevisionBatch {
					return fmt.Errorf("commitlog: revision batch unavailable")
				}
				if err := revisionBatch.DeleteWithRevision(rec.Key, revision); err != nil {
					return err
				}
			} else {
				if err := batch.Delete(rec.Key); err != nil {
					return err
				}
			}
		case commitlog.OpSetInline:
			var setErr error
			if revision != page.LegacyEntryRevision {
				if !hasRevisionBatch {
					return fmt.Errorf("commitlog: revision batch unavailable")
				}
				setErr = revisionBatch.SetWithRevision(rec.Key, rec.Value, revision)
			} else {
				setErr = batch.Set(rec.Key, rec.Value)
			}
			if setErr != nil {
				if !errors.Is(setErr, batchpkg.ErrValueTooLarge) {
					// Abort this replay attempt on non-placement errors. The
					// commit batch remains unapplied and the next open retries
					// from the last published root.
					return setErr
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
				if revision != page.LegacyEntryRevision {
					if !hasPtrRevisionBatch {
						return fmt.Errorf("commitlog: pointer revision batch unavailable")
					}
					if err := ptrRevisionBatch.SetPointerWithRevision(rec.Key, ptr, revision); err != nil {
						return err
					}
				} else {
					if err := ptrBatch.SetPointer(rec.Key, ptr); err != nil {
						return err
					}
				}
			}
		case commitlog.OpSetRID:
			ptr, ok := ridMap[rec.RID]
			if !ok {
				return fmt.Errorf("commitlog: missing rid %d", rec.RID)
			}
			if err := db.registerReplayValueLogPointer(ptr); err != nil {
				return err
			}
			if !hasPtrBatch {
				return fmt.Errorf("commitlog: pointer batch unavailable")
			}
			if revision != page.LegacyEntryRevision {
				if !hasPtrRevisionBatch {
					return fmt.Errorf("commitlog: pointer revision batch unavailable")
				}
				if err := ptrRevisionBatch.SetPointerWithRevision(rec.Key, ptr, revision); err != nil {
					return err
				}
			} else {
				if err := ptrBatch.SetPointer(rec.Key, ptr); err != nil {
					return err
				}
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
