package db

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/collectionwal"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

const (
	systemCollectionMetaPrefix       = "collections/meta/"
	systemCollectionRootPrefix       = "collections/root/"
	systemCollectionWALAppliedPrefix = "collections/wal-applied/"
	systemRequiredFeaturePrefix      = "treedb/storage-format/required-features/"
)

const systemRequiredFeatureCollectionWALV1Key = systemRequiredFeaturePrefix + FormatFeatureCollectionWALV1

const (
	collectionWALDescriptorOpRootUpdate       = 1
	collectionWALDescriptorOpAppliedWatermark = 2
)

// CollectionWALStatsSnapshot reports process-local collection WAL observability
// counters and current retained-WAL debt gauges.
type CollectionWALStatsSnapshot = collectionwal.StatsSnapshot

// CollectionWALStatsSnapshot returns the current process-local collection WAL
// stats. RetainedSegments/RetainedBytes are refreshed from disk on each call so
// operators can spot retained WAL debt even after checkpoints.
func (db *DB) CollectionWALStatsSnapshot() CollectionWALStatsSnapshot {
	if db != nil {
		db.refreshCollectionWALRetainedDebt()
	}
	if db == nil {
		return CollectionWALStatsSnapshot{}
	}
	return db.collectionWALStats.Snapshot()
}

func (db *DB) refreshCollectionWALRetainedDebt() {
	if db == nil || db.dir == "" {
		return
	}
	segments, err := collectionwal.DirtySegments(db.dir)
	if err != nil {
		db.collectionWALStats.RecordRetainedDebtScanFailure()
		return
	}
	var retainedBytes uint64
	for _, segment := range segments {
		info, err := os.Stat(segment)
		if err != nil {
			db.collectionWALStats.RecordRetainedDebtScanFailure()
			return
		}
		if size := info.Size(); size > 0 {
			retainedBytes += uint64(size)
		}
	}
	db.collectionWALStats.SetRetainedDebt(uint64(len(segments)), retainedBytes)
}

// RunCollectionWALPublisher serializes PR1-min WAL-backed collection publish
// critical sections. The append lock below remains scoped to segment I/O; this
// gate covers private planning -> append -> root publish -> watermark publish.
func (db *DB) RunCollectionWALPublisher(run func() error) error {
	if run == nil {
		return errors.New("nil collection WAL publisher")
	}
	if db == nil || db.closing.Load() {
		return ErrClosed
	}
	if db.collectionWALRecoveryRequired.Load() {
		return ErrRecoveryRequired
	}
	db.collectionWALPublisherMu.Lock()
	defer db.collectionWALPublisherMu.Unlock()
	if db.closing.Load() {
		return ErrClosed
	}
	if db.collectionWALRecoveryRequired.Load() {
		return ErrRecoveryRequired
	}
	return run()
}

// MarkCollectionWALRecoveryRequired fails closed after a collection WAL
// transaction is committed but cannot be watermarked in the current process.
// Recovery must replay or skip the retained transaction before new collection
// WAL writes can safely allocate per-collection sequences.
func (db *DB) MarkCollectionWALRecoveryRequired() {
	if db != nil {
		db.collectionWALRecoveryRequired.Store(true)
	}
}

func (db *DB) writeCollectionWALStats(stats map[string]string) {
	if stats == nil {
		return
	}
	snap := db.CollectionWALStatsSnapshot()
	put := func(key string, value uint64) {
		stats["treedb.collection_wal."+key] = fmt.Sprintf("%d", value)
	}
	put("append.txns_total", snap.AppendTxnsTotal)
	put("append.docs_total", snap.AppendDocsTotal)
	put("append.bytes_total", snap.AppendBytesTotal)
	put("append.side_refs_total", snap.AppendSideRefsTotal)
	put("append.latency_ns_total", snap.AppendLatencyNSTotal)
	put("append.flush_ns_total", snap.AppendFlushNSTotal)
	put("append.sync_ns_total", snap.AppendSyncNSTotal)
	put("append.failures_total", snap.AppendFailuresTotal)
	for _, category := range collectionwal.AllErrorCategories() {
		put("append.failures."+string(category)+"_total", snap.AppendFailuresByCategory[category])
	}
	put("pending.txns_current", 0)
	put("pending.docs_current", 0)
	put("pending.bytes_current", 0)
	put("pending.root_delta_side_payload_bytes_current", 0)
	put("pending.side_refs_current", 0)
	put("pending.side_ref_logical_bytes_current", 0)
	put("pending.unpublished_root_delta_entries_current", 0)
	put("pending.oldest_age_ms", 0)
	put("segment.open_current", snap.RetainedSegments)
	put("segment.bytes_current", snap.RetainedBytes)
	put("segment.cleanable_current", 0)
	put("segment.blocked_current", snap.RetainedSegments)
	put("side_ref.protected.count_current", 0)
	put("side_ref.protected.bytes_current", 0)
	put("side_ref.protected.logical_bytes_current", 0)
	put("side_ref.protected.retained_segment_bytes_current", 0)
	put("side_ref.protected.oldest_age_ms", 0)
	put("applied_watermark.lag_txns_current", 0)
	put("applied_watermark.lag_bytes_current", 0)
	put("cleanup.debt.bytes_current", snap.RetainedBytes)
	put("cleanup.debt.segments_current", snap.RetainedSegments)
	put("cleanup.lag_txns_current", 0)
	put("cleanup.lag_bytes_current", 0)
	put("cleanup.failures_total", snap.CleanupFailure)
	for _, watermark := range db.collectionWALWatermarkArtifacts() {
		if watermark.Malformed || watermark.CollectionUIDHash == "" {
			continue
		}
		put("by_collection."+watermark.CollectionUIDHash+".applied_seq_current", watermark.AppliedSeq)
	}
	put("recovery.opens_total", snap.RecoveryOpensTotal)
	put("recovery.duration_last_ms", snap.RecoveryDurationLastMS)
	put("recovery.duration_ns_total", snap.RecoveryDurationNSTotal)
	put("recovery.replayed_txns_total", snap.RecoveryReplay)
	put("recovery.skipped_tail_txns_total", snap.RecoveryTailSkip)
	put("recovery.skipped_watermark_txns_total", snap.RecoveryWatermarkSkip)
	put("recovery.blocked_txns_total", snap.RecoveryBlockedTotal)
	put("recovery.failures_total", snap.RecoveryHardFailure)
	put("recovery.failures.total", snap.RecoveryHardFailure)
	for _, category := range collectionwal.AllErrorCategories() {
		put("recovery.failures."+string(category)+"_total", snap.RecoveryFailuresByCategory[category])
	}
	stats["treedb.collection_wal.recovery.last_failure_category"] = string(snap.RecoveryLastFailureCategory)
	put("recovery.last_failure_wallsn", snap.RecoveryLastFailureWALLSN)
	put("recovery.last_failure_collection_seq", snap.RecoveryLastFailureSeq)
	put("recovery.artifacts_written_total", snap.RecoveryArtifactsWritten)
	put("recovery.artifact_write_failures_total", snap.RecoveryArtifactWriteFailure)
	put("value_log_gc.blocked_bytes_current", snap.ValueLogGCBlockerBytes)
	put("value_log_gc.blocked_segments_current", snap.ValueLogGCBlockerSegments)
	put("value_log_gc.blocked_side_refs_current", 0)
	put("value_log_gc.blocked_by_pending_txns_current", 0)
	put("value_log_gc.blocked_bytes_total", 0)
	put("backpressure.blocked_writes_current", 0)
	put("backpressure.blocked_writes_total", 0)
	put("backpressure.wait_ns_total", 0)
	put("backpressure.capacity_errors_total", 0)
	put("replay.accumulator_peak_bytes_current", 0)
	put("replay.spill_bytes_total", 0)
	put("replay.chunk_publishes_total", 0)
	put("quarantine.files_total", 0)
	put("quarantine.bytes_total", 0)
	put("txn_index.entries_current", 0)
	put("txn_index.bytes_current", 0)
	put("txn_index.lookup_failures_total", 0)
}

// AppendCollectionWALTransaction appends one committed collection WAL
// transaction through the backend-owned PR1-min appender. The appender is
// created lazily so WAL-off collection paths do not create collection WAL files.
func (db *DB) AppendCollectionWALTransaction(txn collectionwal.Transaction, syncAppend bool) (collectionwal.AppendResult, error) {
	var result collectionwal.AppendResult
	if db == nil || db.closing.Load() {
		if db != nil {
			db.collectionWALStats.RecordAppendFailure(ErrClosed)
		}
		return result, ErrClosed
	}
	if db.collectionWALRecoveryRequired.Load() {
		db.collectionWALStats.RecordAppendFailure(ErrRecoveryRequired)
		return result, ErrRecoveryRequired
	}
	if db.readOnly {
		db.collectionWALStats.RecordAppendFailure(ErrReadOnly)
		return result, ErrReadOnly
	}
	if err := db.requireCollectionWALRequiredFeatureActive(); err != nil {
		db.collectionWALStats.RecordAppendFailure(err)
		return result, err
	}
	db.collectionWALMu.Lock()
	defer db.collectionWALMu.Unlock()
	if db.collectionWALAppender == nil {
		appender, err := collectionwal.OpenOrCreateSegmentAppender(db.dir, collectionwal.AppenderOptions{
			Lane:         0,
			SegmentSeq:   1,
			FirstWALLSN:  1,
			SyncOnAppend: db.durability == DurabilityDurable,
		})
		if err != nil {
			db.collectionWALStats.RecordAppendFailure(err)
			return result, err
		}
		db.collectionWALAppender = appender
	}
	result, err := db.collectionWALAppender.AppendTransaction(txn, syncAppend)
	if err != nil {
		db.collectionWALStats.RecordAppendFailure(err)
		return result, err
	}
	db.collectionWALStats.RecordAppendSuccess(txn, result)
	db.refreshCollectionWALRetainedDebt()
	return result, nil
}

// EnsureCollectionWALRequiredFeature durably marks this directory as requiring
// collection_wal_v1 before any WAL-backed collection acknowledgement can occur.
func (db *DB) EnsureCollectionWALRequiredFeature() error {
	if db == nil || db.closing.Load() {
		return ErrClosed
	}
	if db.readOnly {
		return ErrReadOnly
	}
	if err := db.ensureCollectionWALFormatRequiredFeature(); err != nil {
		return err
	}
	enabled, err := db.collectionWALSystemFeatureEnabled()
	if err != nil {
		return err
	}
	if enabled {
		return nil
	}
	_, _, err = db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		return &collectionWALSystemDeltaIterator{entries: []collectionWALSystemEntry{{
			key:   []byte(systemRequiredFeatureCollectionWALV1Key),
			value: []byte("true"),
		}}}, nil
	})
	return err
}

func (db *DB) ensureCollectionWALFormatRequiredFeature() error {
	if db == nil {
		return ErrClosed
	}
	cfg, ok, err := LoadFormatConfig(db.dir)
	if err != nil {
		return err
	}
	if !ok {
		cfg = formatConfigFromDB(db)
	}
	if cfg.HasRequiredFeature(FormatFeatureCollectionWALV1) {
		return nil
	}
	cfg.AddRequiredFeature(FormatFeatureCollectionWALV1)
	if err := SaveFormatConfig(db.dir, cfg); err != nil {
		return err
	}
	return syncDirFn(db.dir)
}

func (db *DB) requireCollectionWALRequiredFeatureActive() error {
	cfg, ok, err := LoadFormatConfig(db.dir)
	if err != nil {
		return err
	}
	if !ok || !cfg.HasRequiredFeature(FormatFeatureCollectionWALV1) {
		return fmt.Errorf("%w: format.json missing required feature %s", collectionwal.ErrCollectionWALUnsupportedMode, FormatFeatureCollectionWALV1)
	}
	enabled, err := db.collectionWALSystemFeatureEnabled()
	if err != nil {
		return err
	}
	if !enabled {
		return fmt.Errorf("%w: system root missing required feature %s", collectionwal.ErrCollectionWALUnsupportedMode, FormatFeatureCollectionWALV1)
	}
	return nil
}

func (db *DB) validateCollectionWALRequiredFeatureGates() error {
	if db == nil {
		return ErrClosed
	}
	cfg, ok, err := LoadFormatConfig(db.dir)
	if err != nil {
		return err
	}
	formatHasFeature := ok && cfg.HasRequiredFeature(FormatFeatureCollectionWALV1)
	systemHasFeature, err := db.collectionWALSystemFeatureEnabled()
	if err != nil {
		return err
	}
	segments, err := collectionwal.DirtySegments(db.dir)
	if err != nil {
		return err
	}
	hasApplied, err := db.collectionWALHasAppliedWatermark()
	if err != nil {
		return err
	}
	hasCollectionWALState := len(segments) != 0 || hasApplied
	if hasCollectionWALState && !formatHasFeature {
		return fmt.Errorf("%w: collection WAL state exists without %s in format.json", collectionwal.ErrCollectionWALCorruptMiddle, FormatFeatureCollectionWALV1)
	}
	if hasCollectionWALState && !systemHasFeature {
		return fmt.Errorf("%w: collection WAL state exists without %s system-root feature", collectionwal.ErrCollectionWALCorruptMiddle, FormatFeatureCollectionWALV1)
	}
	if systemHasFeature && !formatHasFeature {
		return fmt.Errorf("%w: system-root feature %s exists without format.json required feature", collectionwal.ErrCollectionWALCorruptMiddle, FormatFeatureCollectionWALV1)
	}
	return nil
}

func (db *DB) collectionWALSystemFeatureEnabled() (bool, error) {
	if db == nil {
		return false, ErrClosed
	}
	idx := db.idx.Load()
	if idx == nil || idx.pager == nil {
		return false, ErrClosed
	}
	if db.meta.SystemRootPageID == 0 {
		return false, nil
	}
	tr := tree.New(idx.pager, db.collectionWALSystemRootReader(), db.meta.SystemRootPageID)
	raw, err := tr.Get([]byte(systemRequiredFeatureCollectionWALV1Key))
	if errors.Is(err, tree.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !bytes.Equal(raw, []byte("true")) {
		return false, fmt.Errorf("%w: malformed %s system-root feature value", collectionwal.ErrCollectionWALCorruptMiddle, FormatFeatureCollectionWALV1)
	}
	return true, nil
}

func (db *DB) requireNoUnappliedCollectionWAL(operation string) error {
	if db == nil {
		return ErrClosed
	}
	segments, err := collectionwal.DirtySegments(db.dir)
	if err != nil {
		return err
	}
	if len(segments) == 0 {
		return db.requireNoMissingRetainedCollectionWAL(operation)
	}
	applied := make(map[[collectionwal.CollectionUIDBytes]byte]uint64)
	for i, segment := range segments {
		terminal := i == len(segments)-1
		if err := db.requireNoUnappliedCollectionWALSegment(segment, terminal, operation, applied); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) requireNoCollectionWALCleanupDebt(operation string) error {
	if db == nil {
		return ErrClosed
	}
	segments, err := collectionwal.DirtySegments(db.dir)
	if err != nil {
		return err
	}
	if len(segments) != 0 {
		return fmt.Errorf("%w: %s found retained collection WAL cleanup debt segment %s", ErrRecoveryRequired, operation, filepath.Base(segments[0]))
	}
	return db.requireNoMissingRetainedCollectionWAL(operation)
}

func (db *DB) recoverCollectionWAL() error {
	if db == nil {
		return ErrClosed
	}
	start := time.Now()
	defer func() {
		db.collectionWALStats.RecordRecoveryOpen(uint64(time.Since(start)))
	}()
	defer db.refreshCollectionWALRetainedDebt()
	segments, err := collectionwal.DirtySegments(db.dir)
	if err != nil {
		return db.recordCollectionWALRecoveryHardFailure(err, segments)
	}
	if len(segments) == 0 {
		if hasApplied, err := db.collectionWALHasAppliedWatermark(); err != nil {
			err = fmt.Errorf("recover collection WAL applied watermark scan: %w", err)
			return db.recordCollectionWALRecoveryHardFailure(err, segments)
		} else if hasApplied {
			err := fmt.Errorf("%w: retained collection WAL segment missing while applied watermarks exist", collectionwal.ErrCollectionWALCorruptMiddle)
			return db.recordCollectionWALRecoveryHardFailure(err, segments)
		}
		return nil
	}
	applied := make(map[[collectionwal.CollectionUIDBytes]byte]uint64)
	for i, segment := range segments {
		terminal := i == len(segments)-1
		if err := db.recoverCollectionWALSegment(segment, terminal, applied); err != nil {
			return db.recordCollectionWALRecoveryHardFailure(err, segments)
		}
	}
	return nil
}

func (db *DB) recordCollectionWALRecoveryHardFailure(err error, segments []string) error {
	if db == nil {
		return err
	}
	db.collectionWALStats.RecordRecoveryHardFailure(err)
	if artifactErr := db.writeCollectionWALRecoveryFailureArtifact(err, segments); artifactErr != nil {
		db.collectionWALStats.RecordRecoveryArtifactWriteFailure()
		return errors.Join(err, fmt.Errorf("write collection WAL recovery artifact: %w", artifactErr))
	}
	db.collectionWALStats.RecordRecoveryArtifactWritten()
	return err
}

func (db *DB) recoverCollectionWALSegment(segmentPath string, terminal bool, applied map[[collectionwal.CollectionUIDBytes]byte]uint64) error {
	data, err := os.ReadFile(segmentPath)
	if err != nil {
		return fmt.Errorf("recover collection WAL segment %s: %w", filepath.Base(segmentPath), err)
	}
	_, frames, err := collectionwal.ScanSegment(data, terminal)
	if err != nil {
		return fmt.Errorf("recover collection WAL segment %s: %w", filepath.Base(segmentPath), err)
	}
	for _, frame := range frames {
		switch frame.Outcome {
		case collectionwal.OutcomeCompleteValid:
			appliedSeq, err := db.collectionWALAppliedSeq(frame.Transaction.CollectionUID, applied)
			if err != nil {
				return fmt.Errorf("recover collection WAL segment %s load watermark: %w", filepath.Base(segmentPath), err)
			}
			replay, err := collectionWALReplayPlanFromTransaction(frame.Transaction)
			if err != nil {
				err = fmt.Errorf("recover collection WAL segment %s decode replay plan: %w", filepath.Base(segmentPath), err)
				db.collectionWALStats.RecordRecoveryBlocked(err)
				return err
			}
			if appliedSeq >= frame.Transaction.CollectionSeq {
				db.collectionWALStats.RecordRecoveryWatermarkSkip()
				continue
			}
			if appliedSeq+1 != frame.Transaction.CollectionSeq {
				err := fmt.Errorf("%w: recover collection WAL segment %s collection_seq=%d applied_seq=%d", collectionwal.ErrCollectionWALSequenceGap, filepath.Base(segmentPath), frame.Transaction.CollectionSeq, appliedSeq)
				db.collectionWALStats.RecordRecoveryBlocked(err)
				return err
			}
			if err := replay.validateCurrent(db); err != nil {
				err = fmt.Errorf("recover collection WAL segment %s validate replay preconditions: %w", filepath.Base(segmentPath), err)
				db.collectionWALStats.RecordRecoveryBlocked(err)
				return err
			}
			ordered := replay.orderedRootInputs()
			_, _, err = db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
				return replay.systemDeltaIterator(rootIDs)
			})
			if err != nil {
				return fmt.Errorf("recover collection WAL segment %s replay root group: %w", filepath.Base(segmentPath), err)
			}
			applied[frame.Transaction.CollectionUID] = frame.Transaction.CollectionSeq
			db.collectionWALStats.RecordRecoveryReplay()
		case collectionwal.OutcomeTerminalIncompleteTail:
			if terminal {
				db.collectionWALStats.RecordRecoveryTailSkip()
				continue
			}
			return fmt.Errorf("%w: recover collection WAL segment %s nonterminal incomplete tail: %v", collectionwal.ErrCollectionWALCorruptMiddle, filepath.Base(segmentPath), frame.Err)
		case collectionwal.OutcomeUnsupportedSkippable:
			continue
		case collectionwal.OutcomeUnsupportedVersion,
			collectionwal.OutcomeCompleteCorrupt,
			collectionwal.OutcomeDuplicateWALLSN,
			collectionwal.OutcomeDuplicateCollectionSeq,
			collectionwal.OutcomeMaliciousLength,
			collectionwal.OutcomeMixedVersionSegment:
			return collectionWALFrameRecoveryError(segmentPath, frame)
		default:
			return collectionWALFrameRecoveryError(segmentPath, frame)
		}
	}
	return nil
}

func collectionWALFrameRecoveryError(segmentPath string, frame collectionwal.FrameScanResult) error {
	segmentID := filepath.Base(segmentPath)
	cause := frame.Err
	if cause == nil {
		cause = fmt.Errorf("%w: frame outcome %s", collectionwal.ErrCollectionWALCorruptMiddle, frame.Outcome)
	}
	err := fmt.Errorf("recover collection WAL segment %s outcome=%s offset=%d: %w", segmentID, frame.Outcome, frame.Offset, cause)
	category := collectionwal.CategoryOf(err)
	switch frame.Outcome {
	case collectionwal.OutcomeUnsupportedVersion:
		category = collectionwal.ErrorCategoryUnsupportedWALVersion
	case collectionwal.OutcomeDuplicateWALLSN:
		category = collectionwal.ErrorCategoryDuplicateWALLSN
	case collectionwal.OutcomeDuplicateCollectionSeq:
		category = collectionwal.ErrorCategoryDuplicateCollectionSeq
	case collectionwal.OutcomeNonTerminalShortRead:
		category = collectionwal.ErrorCategorySegmentGapWithoutCleanup
	}
	if category == "" {
		return err
	}
	offset := uint64(0)
	if frame.Offset > 0 {
		offset = uint64(frame.Offset)
	}
	return &collectionwal.CollectionWALError{
		Category:      category,
		WALLSN:        frame.Header.WALLSN,
		CollectionSeq: frame.Header.CollectionSeq,
		SegmentID:     segmentID,
		SegmentOffset: offset,
		Cause:         err,
	}
}

func (db *DB) requireNoUnappliedCollectionWALSegment(segmentPath string, terminal bool, operation string, applied map[[collectionwal.CollectionUIDBytes]byte]uint64) error {
	data, err := os.ReadFile(segmentPath)
	if err != nil {
		return fmt.Errorf("%w: %s read collection WAL segment %s: %v", ErrRecoveryRequired, operation, filepath.Base(segmentPath), err)
	}
	_, frames, err := collectionwal.ScanSegment(data, terminal)
	if err != nil {
		return fmt.Errorf("%w: %s scan collection WAL segment %s: %v", ErrRecoveryRequired, operation, filepath.Base(segmentPath), err)
	}
	for _, frame := range frames {
		switch frame.Outcome {
		case collectionwal.OutcomeCompleteValid:
			if _, err := collectionWALReplayPlanFromTransaction(frame.Transaction); err != nil {
				return fmt.Errorf("%w: %s decode collection WAL segment %s replay plan: %v", ErrRecoveryRequired, operation, filepath.Base(segmentPath), err)
			}
			appliedSeq, err := db.collectionWALAppliedSeq(frame.Transaction.CollectionUID, applied)
			if err != nil {
				return fmt.Errorf("%w: %s load collection WAL applied watermark for segment %s: %v", ErrRecoveryRequired, operation, filepath.Base(segmentPath), err)
			}
			if appliedSeq < frame.Transaction.CollectionSeq {
				return fmt.Errorf("%w: %s found unapplied collection WAL segment %s collection_seq=%d applied_seq=%d", ErrRecoveryRequired, operation, filepath.Base(segmentPath), frame.Transaction.CollectionSeq, appliedSeq)
			}
		case collectionwal.OutcomeTerminalIncompleteTail:
			if terminal {
				continue
			}
			return fmt.Errorf("%w: %s found nonterminal incomplete collection WAL segment %s: %v", ErrRecoveryRequired, operation, filepath.Base(segmentPath), frame.Err)
		case collectionwal.OutcomeUnsupportedSkippable:
			continue
		default:
			return fmt.Errorf("%w: %s found unrecoverable collection WAL segment %s outcome=%s: %v", ErrRecoveryRequired, operation, filepath.Base(segmentPath), frame.Outcome, frame.Err)
		}
	}
	return nil
}

func (db *DB) requireNoMissingRetainedCollectionWAL(operation string) error {
	hasApplied, err := db.collectionWALHasAppliedWatermark()
	if err != nil {
		return fmt.Errorf("%w: %s scan collection WAL applied watermarks: %v", ErrRecoveryRequired, operation, err)
	}
	if hasApplied {
		return fmt.Errorf("%w: %s found applied collection WAL watermark but no retained collection WAL segment", ErrRecoveryRequired, operation)
	}
	return nil
}

func (db *DB) collectionWALHasAppliedWatermark() (bool, error) {
	if db == nil {
		return false, ErrClosed
	}
	idx := db.idx.Load()
	if idx == nil || idx.pager == nil || db.meta.SystemRootPageID == 0 {
		return false, nil
	}
	prefix := []byte(systemCollectionWALAppliedPrefix)
	tr := tree.New(idx.pager, db.collectionWALSystemRootReader(), db.meta.SystemRootPageID)
	it := tr.IteratorWithOptions(prefix, collectionWALPrefixEnd(prefix), tree.IteratorOptions{})
	defer func() { _ = it.Close() }()
	for it.Valid() {
		if !bytes.HasPrefix(it.UnsafeKey(), prefix) {
			break
		}
		if !it.IsDeleted() {
			raw := it.UnsafeValue()
			if len(raw) != 8 {
				return false, fmt.Errorf("malformed collection WAL applied watermark key=%q len=%d", it.UnsafeKey(), len(raw))
			}
			if binary.BigEndian.Uint64(raw) != 0 {
				return true, nil
			}
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return false, err
	}
	return false, nil
}

func collectionWALPrefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func (db *DB) collectionWALAppliedSeq(uid [collectionwal.CollectionUIDBytes]byte, cache map[[collectionwal.CollectionUIDBytes]byte]uint64) (uint64, error) {
	if seq, ok := cache[uid]; ok {
		return seq, nil
	}
	key := []byte(systemCollectionWALAppliedPrefix + hex.EncodeToString(uid[:]))
	raw, err := db.getSystemRootValue(key)
	if err != nil {
		return 0, err
	}
	if raw == nil {
		cache[uid] = 0
		return 0, nil
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("malformed collection WAL applied watermark len=%d", len(raw))
	}
	seq := binary.BigEndian.Uint64(raw)
	cache[uid] = seq
	return seq, nil
}

func (db *DB) getSystemRootValue(key []byte) ([]byte, error) {
	if db == nil {
		return nil, ErrClosed
	}
	idx := db.idx.Load()
	if idx == nil || idx.pager == nil || db.meta.SystemRootPageID == 0 {
		return nil, ErrClosed
	}
	tr := tree.New(idx.pager, db.collectionWALSystemRootReader(), db.meta.SystemRootPageID)
	raw, err := tr.Get(key)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, nil
	}
	return raw, err
}

func (db *DB) collectionWALSystemRootReader() tree.SlabReader {
	if db == nil || db.valueLogManager == nil {
		return nil
	}
	if state := db.state.Load(); state != nil && state.ValueLogSet != nil {
		return newValueReader(state.ValueLogSet)
	}
	return newValueReader(db.valueLogManager.CurrentSetNoRefresh())
}

type collectionWALReplayPlan struct {
	txn      collectionwal.Transaction
	roots    []collectionWALRootDelta
	template collectionWALSystemDeltaTemplate
}

type collectionWALRootDelta struct {
	rootName                 string
	rootUID                  [collectionwal.CollectionUIDBytes]byte
	rootKind                 uint16
	baseRootID               uint64
	baseRootGeneration       uint64
	baseRootDescriptorEpoch  uint64
	baseRootDescriptorDigest [32]byte
	entries                  []collectionWALSystemEntry
}

type collectionWALSystemDeltaTemplate struct {
	collectionUID  [collectionwal.CollectionUIDBytes]byte
	collectionName string
	collectionSeq  uint64
	dependsSeq     uint64
	roots          []collectionWALSystemDeltaRoot
	descriptorOps  []collectionWALSystemEntry
}

type collectionWALSystemDeltaRoot struct {
	rootName                 string
	rootUID                  [collectionwal.CollectionUIDBytes]byte
	rootKind                 uint16
	baseRootID               uint64
	baseRootGeneration       uint64
	baseRootDescriptorEpoch  uint64
	baseRootDescriptorDigest [32]byte
	originalRootID           uint64
}

func collectionWALReplayPlanFromTransaction(txn collectionwal.Transaction) (*collectionWALReplayPlan, error) {
	if txn.SideRefCount != 0 {
		return nil, fmt.Errorf("%w: side refs are unsupported in PR1-min replay", collectionwal.ErrCollectionWALUnsupportedMode)
	}
	if txn.RootDeltaCount != 1 {
		return nil, fmt.Errorf("%w: PR1-min replay requires exactly one root delta, got %d", collectionwal.ErrCollectionWALUnsupportedMode, txn.RootDeltaCount)
	}
	if txn.DescriptorOpCount == 0 {
		return nil, fmt.Errorf("%w: descriptor ops are required", collectionwal.ErrCollectionWALIdentityMismatch)
	}
	var rootDeltaSection []byte
	var sideRefSection []byte
	var systemTemplateSection []byte
	var descriptorOps []byte
	for _, section := range txn.Sections {
		switch section.Type {
		case collectionwal.SectionTypeRootDeltaTable:
			rootDeltaSection = section.Data
		case collectionwal.SectionTypeSideRefTable:
			sideRefSection = section.Data
		case collectionwal.SectionTypeSystemDeltaTemplate:
			systemTemplateSection = section.Data
		case collectionwal.SectionTypeDescriptorOps:
			descriptorOps = section.Data
		}
	}
	if rootDeltaSection == nil {
		return nil, fmt.Errorf("%w: root delta section is required", collectionwal.ErrCollectionWALIdentityMismatch)
	}
	if sideRefSection == nil {
		return nil, fmt.Errorf("%w: side ref section is required", collectionwal.ErrCollectionWALIdentityMismatch)
	}
	if systemTemplateSection == nil {
		return nil, fmt.Errorf("%w: system delta template section is required", collectionwal.ErrCollectionWALIdentityMismatch)
	}
	if descriptorOps == nil {
		return nil, fmt.Errorf("%w: descriptor ops section is required", collectionwal.ErrCollectionWALIdentityMismatch)
	}
	if err := validateCollectionWALEmptySideRefSection(sideRefSection); err != nil {
		return nil, err
	}
	roots, err := decodeCollectionWALRootDeltaTable(txn, rootDeltaSection)
	if err != nil {
		return nil, err
	}
	template, err := decodeCollectionWALSystemDeltaTemplate(txn, systemTemplateSection)
	if err != nil {
		return nil, err
	}
	descriptorEntries, err := decodeCollectionWALDescriptorOps(txn, descriptorOps)
	if err != nil {
		return nil, err
	}
	if len(roots) != len(template.roots) {
		return nil, fmt.Errorf("%w: root delta count=%d template roots=%d", collectionwal.ErrCollectionWALIdentityMismatch, len(roots), len(template.roots))
	}
	for i := range roots {
		if roots[i].rootName != template.roots[i].rootName ||
			roots[i].rootUID != template.roots[i].rootUID ||
			roots[i].rootKind != template.roots[i].rootKind ||
			roots[i].baseRootID != template.roots[i].baseRootID ||
			roots[i].baseRootGeneration != template.roots[i].baseRootGeneration ||
			roots[i].baseRootDescriptorEpoch != template.roots[i].baseRootDescriptorEpoch ||
			roots[i].baseRootDescriptorDigest != template.roots[i].baseRootDescriptorDigest {
			return nil, fmt.Errorf("%w: root delta %d root=%q uid=%x kind=%d base=%d gen=%d epoch=%d digest=%x template root=%q uid=%x kind=%d base=%d gen=%d epoch=%d digest=%x", collectionwal.ErrCollectionWALIdentityMismatch, i, roots[i].rootName, roots[i].rootUID, roots[i].rootKind, roots[i].baseRootID, roots[i].baseRootGeneration, roots[i].baseRootDescriptorEpoch, roots[i].baseRootDescriptorDigest, template.roots[i].rootName, template.roots[i].rootUID, template.roots[i].rootKind, template.roots[i].baseRootID, template.roots[i].baseRootGeneration, template.roots[i].baseRootDescriptorEpoch, template.roots[i].baseRootDescriptorDigest)
		}
	}
	if err := validateCollectionWALDescriptorOpsMatchTemplate(template, template.descriptorOps); err != nil {
		return nil, fmt.Errorf("system template descriptor ops: %w", err)
	}
	if err := validateCollectionWALDescriptorOpsMatchTemplate(template, descriptorEntries); err != nil {
		return nil, fmt.Errorf("descriptor ops section: %w", err)
	}
	if !collectionWALSystemEntriesEqual(template.descriptorOps, descriptorEntries) {
		return nil, fmt.Errorf("%w: descriptor ops section does not match system template", collectionwal.ErrCollectionWALIdentityMismatch)
	}
	return &collectionWALReplayPlan{txn: txn, roots: roots, template: template}, nil
}

func (p *collectionWALReplayPlan) validateCurrent(db *DB) error {
	if p == nil {
		return fmt.Errorf("%w: nil replay plan", collectionwal.ErrCollectionWALIdentityMismatch)
	}
	if err := p.validateCatalogGuard(db); err != nil {
		return err
	}
	applied, err := db.collectionWALAppliedSeq(p.template.collectionUID, make(map[[collectionwal.CollectionUIDBytes]byte]uint64))
	if err != nil {
		return err
	}
	if applied != p.template.dependsSeq {
		return fmt.Errorf("%w: applied seq=%d depends=%d", collectionwal.ErrCollectionWALSequenceGap, applied, p.template.dependsSeq)
	}
	for _, root := range p.roots {
		currentRoot, err := db.collectionWALCurrentRootID(root.rootName)
		if err != nil {
			return err
		}
		if currentRoot != root.baseRootID {
			return fmt.Errorf("%w: root %q current=%d base=%d", collectionwal.ErrCollectionWALIdentityMismatch, root.rootName, currentRoot, root.baseRootID)
		}
	}
	return nil
}

type collectionWALCatalogMetaGuard struct {
	Version              int    `json:"version"`
	Name                 string `json:"name"`
	CollectionUID        string `json:"collection_uid,omitempty"`
	CollectionGeneration uint64 `json:"collection_generation,omitempty"`
	CatalogEpoch         uint64 `json:"catalog_epoch,omitempty"`
	SchemaEpoch          uint64 `json:"schema_epoch,omitempty"`
}

func (p *collectionWALReplayPlan) validateCatalogGuard(db *DB) error {
	if p == nil {
		return fmt.Errorf("%w: nil replay plan", collectionwal.ErrCollectionWALIdentityMismatch)
	}
	if p.template.collectionName == "" {
		return fmt.Errorf("%w: missing collection name guard", collectionwal.ErrCollectionWALIdentityMismatch)
	}
	raw, err := db.getSystemRootValue([]byte(systemCollectionMetaPrefix + p.template.collectionName))
	if err != nil {
		return err
	}
	if raw == nil {
		return fmt.Errorf("%w: collection metadata %q missing", collectionwal.ErrCollectionWALIdentityMismatch, p.template.collectionName)
	}
	var meta collectionWALCatalogMetaGuard
	if err := json.Unmarshal(raw, &meta); err != nil {
		return fmt.Errorf("%w: decode collection metadata %q: %v", collectionwal.ErrCollectionWALIdentityMismatch, p.template.collectionName, err)
	}
	wantUID := hex.EncodeToString(p.template.collectionUID[:])
	if meta.Name != p.template.collectionName ||
		meta.CollectionUID != wantUID ||
		meta.CollectionGeneration != p.txn.CollectionGeneration ||
		meta.CatalogEpoch != p.txn.CatalogEpoch ||
		meta.SchemaEpoch != p.txn.SchemaEpoch {
		return fmt.Errorf("%w: collection metadata guard mismatch name=%q uid=%q generation=%d catalog_epoch=%d schema_epoch=%d", collectionwal.ErrCollectionWALIdentityMismatch, meta.Name, meta.CollectionUID, meta.CollectionGeneration, meta.CatalogEpoch, meta.SchemaEpoch)
	}
	if p.txn.SchemaVersion != 0 && uint64(meta.Version) != p.txn.SchemaVersion {
		return fmt.Errorf("%w: collection metadata version=%d transaction schema_version=%d", collectionwal.ErrCollectionWALIdentityMismatch, meta.Version, p.txn.SchemaVersion)
	}
	if p.txn.BaseCatalogDigest != ([32]byte{}) {
		if digest := collectionWALBaseCatalogDigest(meta, raw, p.roots); digest != p.txn.BaseCatalogDigest {
			return fmt.Errorf("%w: base catalog digest mismatch got=%x want=%x", collectionwal.ErrCollectionWALIdentityMismatch, digest, p.txn.BaseCatalogDigest)
		}
	}
	return nil
}

func collectionWALBaseCatalogDigest(meta collectionWALCatalogMetaGuard, encodedMeta []byte, roots []collectionWALRootDelta) [32]byte {
	h := sha256.New()
	h.Write([]byte(meta.Name))
	h.Write([]byte{0})
	h.Write([]byte(meta.CollectionUID))
	h.Write([]byte{0})
	var num [8]byte
	binary.LittleEndian.PutUint64(num[:], meta.CollectionGeneration)
	h.Write(num[:])
	binary.LittleEndian.PutUint64(num[:], meta.CatalogEpoch)
	h.Write(num[:])
	binary.LittleEndian.PutUint64(num[:], meta.SchemaEpoch)
	h.Write(num[:])
	h.Write(encodedMeta)

	type rootGuard struct {
		name string
		id   uint64
	}
	rootGuards := make([]rootGuard, 0, len(roots))
	for _, root := range roots {
		if root.baseRootID == 0 {
			continue
		}
		rootGuards = append(rootGuards, rootGuard{name: root.rootName, id: root.baseRootID})
	}
	sort.Slice(rootGuards, func(i, j int) bool {
		return rootGuards[i].name < rootGuards[j].name
	})
	for _, root := range rootGuards {
		h.Write([]byte(root.name))
		h.Write([]byte{0})
		binary.LittleEndian.PutUint64(num[:], root.id)
		h.Write(num[:])
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

func (p *collectionWALReplayPlan) orderedRootInputs() []OrderedRootDeltaPublishInput {
	ordered := make([]OrderedRootDeltaPublishInput, len(p.roots))
	for i := range p.roots {
		ordered[i] = OrderedRootDeltaPublishInput{
			BaseRoot:      p.roots[i].baseRootID,
			Iter:          (&collectionWALSystemDeltaIterator{entries: p.roots[i].entries}),
			StoragePolicy: OrderedRootStoragePagerLeaves,
		}
	}
	return ordered
}

func (p *collectionWALReplayPlan) systemDeltaIterator(rootIDs []uint64) (iterator.UnsafeIterator, error) {
	if p == nil {
		return nil, fmt.Errorf("%w: nil replay plan", collectionwal.ErrCollectionWALIdentityMismatch)
	}
	if len(rootIDs) != len(p.template.roots) {
		return nil, fmt.Errorf("%w: replay root IDs=%d template roots=%d", collectionwal.ErrCollectionWALIdentityMismatch, len(rootIDs), len(p.template.roots))
	}
	entries := make([]collectionWALSystemEntry, 0, len(rootIDs)+1)
	for i, rootID := range rootIDs {
		entries = append(entries, collectionWALSystemEntry{
			key:   []byte(systemCollectionRootPrefix + p.template.roots[i].rootName),
			value: encodeCollectionWALRootID(rootID),
		})
	}
	entries = append(entries, collectionWALSystemEntry{
		key:   []byte(systemCollectionWALAppliedPrefix + hex.EncodeToString(p.template.collectionUID[:])),
		value: encodeCollectionWALRootID(p.template.collectionSeq),
	})
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})
	return &collectionWALSystemDeltaIterator{entries: entries}, nil
}

func (db *DB) collectionWALCurrentRootID(rootName string) (uint64, error) {
	raw, err := db.getSystemRootValue([]byte(systemCollectionRootPrefix + rootName))
	if err != nil || raw == nil {
		return 0, err
	}
	return decodeCollectionWALRootID(raw)
}

func encodeCollectionWALRootID(rootID uint64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, rootID)
	return out
}

func decodeCollectionWALRootID(raw []byte) (uint64, error) {
	if len(raw) != 8 {
		return 0, fmt.Errorf("%w: malformed root id len=%d", collectionwal.ErrCollectionWALCorruptMiddle, len(raw))
	}
	return binary.BigEndian.Uint64(raw), nil
}

func validateCollectionWALEmptySideRefSection(data []byte) error {
	reader := collectionWALSectionReader{data: data}
	if err := reader.expectMagic([8]byte{'T', 'D', 'B', 'C', 'W', 'S', 'R', 0x01}); err != nil {
		return err
	}
	count, err := reader.u32()
	if err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("%w: PR1-min side refs must be empty, got %d", collectionwal.ErrCollectionWALUnsupportedMode, count)
	}
	return reader.done()
}

func decodeCollectionWALRootDeltaTable(txn collectionwal.Transaction, data []byte) ([]collectionWALRootDelta, error) {
	reader := collectionWALSectionReader{data: data}
	if err := reader.expectMagic([8]byte{'T', 'D', 'B', 'C', 'W', 'R', 'D', 0x01}); err != nil {
		return nil, err
	}
	uid, err := reader.fixed(collectionwal.CollectionUIDBytes)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(uid, txn.CollectionUID[:]) {
		return nil, fmt.Errorf("%w: root delta collection UID mismatch", collectionwal.ErrCollectionWALIdentityMismatch)
	}
	rootCount, err := reader.u32()
	if err != nil {
		return nil, err
	}
	if rootCount != txn.RootDeltaCount {
		return nil, fmt.Errorf("%w: root delta count=%d transaction=%d", collectionwal.ErrCollectionWALIdentityMismatch, rootCount, txn.RootDeltaCount)
	}
	roots := make([]collectionWALRootDelta, 0, rootCount)
	for rootOrdinal := uint32(0); rootOrdinal < rootCount; rootOrdinal++ {
		rootNameBytes, err := reader.bytes(collectionwal.MaxRootNameBytes)
		if err != nil {
			return nil, err
		}
		if len(rootNameBytes) == 0 {
			return nil, fmt.Errorf("%w: root delta missing root name", collectionwal.ErrCollectionWALIdentityMismatch)
		}
		rootUIDBytes, err := reader.fixed(collectionwal.CollectionUIDBytes)
		if err != nil {
			return nil, err
		}
		var rootUID [collectionwal.CollectionUIDBytes]byte
		copy(rootUID[:], rootUIDBytes)
		rootKind, err := reader.u16()
		if err != nil {
			return nil, err
		}
		if rootKind != collectionwal.RootKindPrimary {
			return nil, fmt.Errorf("%w: PR1-min root kind=%d", collectionwal.ErrCollectionWALUnsupportedMode, rootKind)
		}
		if want := collectionwal.PR1MinPrimaryRootUID(txn.CollectionUID); rootUID != want {
			return nil, fmt.Errorf("%w: root delta root UID=%x want %x", collectionwal.ErrCollectionWALIdentityMismatch, rootUID, want)
		}
		baseRootID, err := reader.u64()
		if err != nil {
			return nil, err
		}
		baseRootGeneration, err := reader.u64()
		if err != nil {
			return nil, err
		}
		if baseRootGeneration != collectionwal.RootGenerationPrimary {
			return nil, fmt.Errorf("%w: PR1-min base root generation=%d", collectionwal.ErrCollectionWALIdentityMismatch, baseRootGeneration)
		}
		baseRootDescriptorEpoch, err := reader.u64()
		if err != nil {
			return nil, err
		}
		if baseRootDescriptorEpoch != txn.DependsOnCollectionSeq {
			return nil, fmt.Errorf("%w: base root descriptor epoch=%d depends=%d", collectionwal.ErrCollectionWALIdentityMismatch, baseRootDescriptorEpoch, txn.DependsOnCollectionSeq)
		}
		baseRootDescriptorDigestBytes, err := reader.fixed(32)
		if err != nil {
			return nil, err
		}
		var baseRootDescriptorDigest [32]byte
		copy(baseRootDescriptorDigest[:], baseRootDescriptorDigestBytes)
		if want := collectionwal.PR1MinPrimaryRootDescriptorDigest(txn.CollectionUID, baseRootID, baseRootDescriptorEpoch); baseRootDescriptorDigest != want {
			return nil, fmt.Errorf("%w: root delta descriptor digest=%x want %x", collectionwal.ErrCollectionWALIdentityMismatch, baseRootDescriptorDigest, want)
		}
		// The originally planned root ID is diagnostic only during recovery;
		// replay rematerializes the root and instantiates the template with the
		// freshly built root ID.
		if _, err := reader.u64(); err != nil {
			return nil, err
		}
		entryCount, err := reader.u64()
		if err != nil {
			return nil, err
		}
		if entryCount > collectionwal.MaxDecodedRootDeltaEntriesPerTxn {
			return nil, fmt.Errorf("%w: root delta entries %d exceeds %d", collectionwal.ErrCollectionWALResourceLimit, entryCount, collectionwal.MaxDecodedRootDeltaEntriesPerTxn)
		}
		if entryCount > uint64(int(^uint(0)>>1)) {
			return nil, fmt.Errorf("%w: root delta entries overflow int: %d", collectionwal.ErrCollectionWALResourceLimit, entryCount)
		}
		entries := make([]collectionWALSystemEntry, 0, int(entryCount))
		var prevKey []byte
		for i := uint64(0); i < entryCount; i++ {
			ordinal, err := reader.u64()
			if err != nil {
				return nil, err
			}
			if ordinal != i {
				return nil, fmt.Errorf("%w: root delta entry ordinal=%d want=%d", collectionwal.ErrCollectionWALSequenceGap, ordinal, i)
			}
			op, err := reader.u16()
			if err != nil {
				return nil, err
			}
			if op != 1 {
				return nil, fmt.Errorf("%w: PR1-min root delta op=%d", collectionwal.ErrCollectionWALUnsupportedVersion, op)
			}
			key, err := reader.bytes(collectionwal.MaxDocumentIDBytes)
			if err != nil {
				return nil, err
			}
			if len(key) == 0 {
				return nil, fmt.Errorf("%w: root delta empty key", collectionwal.ErrCollectionWALIdentityMismatch)
			}
			if prevKey != nil && bytes.Compare(prevKey, key) >= 0 {
				return nil, fmt.Errorf("%w: root delta keys are not strictly ordered", collectionwal.ErrCollectionWALSequenceGap)
			}
			value, err := reader.bytes(collectionwal.MaxInlineDeltaValueBytes)
			if err != nil {
				return nil, err
			}
			entries = append(entries, collectionWALSystemEntry{
				key:   append([]byte(nil), key...),
				value: append([]byte(nil), value...),
			})
			prevKey = key
		}
		roots = append(roots, collectionWALRootDelta{
			rootName:                 string(rootNameBytes),
			rootUID:                  rootUID,
			rootKind:                 rootKind,
			baseRootID:               baseRootID,
			baseRootGeneration:       baseRootGeneration,
			baseRootDescriptorEpoch:  baseRootDescriptorEpoch,
			baseRootDescriptorDigest: baseRootDescriptorDigest,
			entries:                  entries,
		})
	}
	if err := reader.done(); err != nil {
		return nil, err
	}
	return roots, nil
}

func decodeCollectionWALSystemDeltaTemplate(txn collectionwal.Transaction, data []byte) (collectionWALSystemDeltaTemplate, error) {
	var template collectionWALSystemDeltaTemplate
	reader := collectionWALSectionReader{data: data}
	if err := reader.expectMagic([8]byte{'T', 'D', 'B', 'C', 'W', 'S', 'T', 0x01}); err != nil {
		return template, err
	}
	uid, err := reader.fixed(collectionwal.CollectionUIDBytes)
	if err != nil {
		return template, err
	}
	copy(template.collectionUID[:], uid)
	if template.collectionUID != txn.CollectionUID {
		return template, fmt.Errorf("%w: system template collection UID mismatch", collectionwal.ErrCollectionWALIdentityMismatch)
	}
	collectionName, err := reader.bytes(collectionwal.MaxLogicalNameBytes)
	if err != nil {
		return template, err
	}
	if len(collectionName) == 0 {
		return template, fmt.Errorf("%w: system template missing collection name", collectionwal.ErrCollectionWALIdentityMismatch)
	}
	template.collectionName = string(collectionName)
	collectionGeneration, err := reader.u64()
	if err != nil {
		return template, err
	}
	catalogEpoch, err := reader.u64()
	if err != nil {
		return template, err
	}
	schemaEpoch, err := reader.u64()
	if err != nil {
		return template, err
	}
	baseCommitSeq, err := reader.u64()
	if err != nil {
		return template, err
	}
	baseSystemRoot, err := reader.u64()
	if err != nil {
		return template, err
	}
	dependsSeq, err := reader.u64()
	if err != nil {
		return template, err
	}
	collectionSeq, err := reader.u64()
	if err != nil {
		return template, err
	}
	if collectionGeneration != txn.CollectionGeneration || catalogEpoch != txn.CatalogEpoch || schemaEpoch != txn.SchemaEpoch || baseCommitSeq != txn.BaseCommitSeq || baseSystemRoot != txn.BaseSystemRootID || dependsSeq != txn.DependsOnCollectionSeq || collectionSeq != txn.CollectionSeq {
		return template, fmt.Errorf("%w: system template transaction guard mismatch", collectionwal.ErrCollectionWALIdentityMismatch)
	}
	template.collectionSeq = collectionSeq
	template.dependsSeq = dependsSeq
	rootCount, err := reader.u32()
	if err != nil {
		return template, err
	}
	if rootCount != txn.RootDeltaCount {
		return template, fmt.Errorf("%w: system template root count=%d transaction=%d", collectionwal.ErrCollectionWALIdentityMismatch, rootCount, txn.RootDeltaCount)
	}
	template.roots = make([]collectionWALSystemDeltaRoot, 0, rootCount)
	for i := uint32(0); i < rootCount; i++ {
		rootName, err := reader.bytes(collectionwal.MaxRootNameBytes)
		if err != nil {
			return template, err
		}
		rootUIDBytes, err := reader.fixed(collectionwal.CollectionUIDBytes)
		if err != nil {
			return template, err
		}
		var rootUID [collectionwal.CollectionUIDBytes]byte
		copy(rootUID[:], rootUIDBytes)
		rootKind, err := reader.u16()
		if err != nil {
			return template, err
		}
		if rootKind != collectionwal.RootKindPrimary {
			return template, fmt.Errorf("%w: PR1-min template root kind=%d", collectionwal.ErrCollectionWALUnsupportedMode, rootKind)
		}
		if want := collectionwal.PR1MinPrimaryRootUID(txn.CollectionUID); rootUID != want {
			return template, fmt.Errorf("%w: system template root UID=%x want %x", collectionwal.ErrCollectionWALIdentityMismatch, rootUID, want)
		}
		baseRootID, err := reader.u64()
		if err != nil {
			return template, err
		}
		baseRootGeneration, err := reader.u64()
		if err != nil {
			return template, err
		}
		if baseRootGeneration != collectionwal.RootGenerationPrimary {
			return template, fmt.Errorf("%w: PR1-min template base root generation=%d", collectionwal.ErrCollectionWALIdentityMismatch, baseRootGeneration)
		}
		baseRootDescriptorEpoch, err := reader.u64()
		if err != nil {
			return template, err
		}
		if baseRootDescriptorEpoch != txn.DependsOnCollectionSeq {
			return template, fmt.Errorf("%w: template base root descriptor epoch=%d depends=%d", collectionwal.ErrCollectionWALIdentityMismatch, baseRootDescriptorEpoch, txn.DependsOnCollectionSeq)
		}
		baseRootDescriptorDigestBytes, err := reader.fixed(32)
		if err != nil {
			return template, err
		}
		var baseRootDescriptorDigest [32]byte
		copy(baseRootDescriptorDigest[:], baseRootDescriptorDigestBytes)
		if want := collectionwal.PR1MinPrimaryRootDescriptorDigest(txn.CollectionUID, baseRootID, baseRootDescriptorEpoch); baseRootDescriptorDigest != want {
			return template, fmt.Errorf("%w: system template descriptor digest=%x want %x", collectionwal.ErrCollectionWALIdentityMismatch, baseRootDescriptorDigest, want)
		}
		originalRootID, err := reader.u64()
		if err != nil {
			return template, err
		}
		template.roots = append(template.roots, collectionWALSystemDeltaRoot{
			rootName:                 string(rootName),
			rootUID:                  rootUID,
			rootKind:                 rootKind,
			baseRootID:               baseRootID,
			baseRootGeneration:       baseRootGeneration,
			baseRootDescriptorEpoch:  baseRootDescriptorEpoch,
			baseRootDescriptorDigest: baseRootDescriptorDigest,
			originalRootID:           originalRootID,
		})
	}
	opCount, err := reader.u32()
	if err != nil {
		return template, err
	}
	if opCount != txn.DescriptorOpCount {
		return template, fmt.Errorf("%w: system template descriptor op count=%d transaction=%d", collectionwal.ErrCollectionWALIdentityMismatch, opCount, txn.DescriptorOpCount)
	}
	descriptorOps, err := decodeCollectionWALDescriptorOpsBody(&reader, txn, opCount)
	if err != nil {
		return template, err
	}
	template.descriptorOps = descriptorOps
	return template, reader.done()
}

func decodeCollectionWALDescriptorOps(txn collectionwal.Transaction, data []byte) ([]collectionWALSystemEntry, error) {
	reader := collectionWALSectionReader{data: data}
	if err := reader.expectMagic([8]byte{'T', 'D', 'B', 'C', 'W', 'D', 'O', 0x01}); err != nil {
		return nil, err
	}
	opCount, err := reader.u32()
	if err != nil {
		return nil, err
	}
	if opCount != txn.DescriptorOpCount {
		return nil, fmt.Errorf("%w: descriptor op count=%d transaction=%d", collectionwal.ErrCollectionWALIdentityMismatch, opCount, txn.DescriptorOpCount)
	}
	entries, err := decodeCollectionWALDescriptorOpsBody(&reader, txn, opCount)
	if err != nil {
		return nil, err
	}
	return entries, reader.done()
}

func decodeCollectionWALDescriptorOpsBody(reader *collectionWALSectionReader, txn collectionwal.Transaction, opCount uint32) ([]collectionWALSystemEntry, error) {
	entries := make([]collectionWALSystemEntry, 0, opCount)
	watermarkKey := systemCollectionWALAppliedPrefix + hex.EncodeToString(txn.CollectionUID[:])
	watermarkSeen := false
	for i := uint32(0); i < opCount; i++ {
		op, err := reader.u16()
		if err != nil {
			return nil, err
		}
		key, err := reader.bytes(collectionwal.MaxRootNameBytes + len(systemCollectionWALAppliedPrefix))
		if err != nil {
			return nil, err
		}
		value, err := reader.bytes(8)
		if err != nil {
			return nil, err
		}
		switch op {
		case collectionWALDescriptorOpRootUpdate:
			if len(value) != 8 {
				return nil, fmt.Errorf("%w: root descriptor value len=%d", collectionwal.ErrCollectionWALCorruptMiddle, len(value))
			}
		case collectionWALDescriptorOpAppliedWatermark:
			if string(key) != watermarkKey {
				return nil, fmt.Errorf("%w: watermark key=%q want %q", collectionwal.ErrCollectionWALIdentityMismatch, key, watermarkKey)
			}
			if len(value) != 8 || binary.BigEndian.Uint64(value) != txn.CollectionSeq {
				return nil, fmt.Errorf("%w: watermark value does not match collection seq %d", collectionwal.ErrCollectionWALIdentityMismatch, txn.CollectionSeq)
			}
			watermarkSeen = true
		default:
			return nil, fmt.Errorf("%w: unknown descriptor op %d", collectionwal.ErrCollectionWALUnsupportedVersion, op)
		}
		entries = append(entries, collectionWALSystemEntry{
			key:   append([]byte(nil), key...),
			value: append([]byte(nil), value...),
		})
	}
	if !watermarkSeen {
		return nil, fmt.Errorf("%w: descriptor ops missing applied watermark", collectionwal.ErrCollectionWALIdentityMismatch)
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})
	return entries, nil
}

func validateCollectionWALDescriptorOpsMatchTemplate(template collectionWALSystemDeltaTemplate, entries []collectionWALSystemEntry) error {
	expected := expectedCollectionWALDescriptorOps(template)
	if len(entries) != len(expected) {
		return fmt.Errorf("%w: descriptor op entries=%d want=%d", collectionwal.ErrCollectionWALIdentityMismatch, len(entries), len(expected))
	}
	for i := range expected {
		if !bytes.Equal(entries[i].key, expected[i].key) || !bytes.Equal(entries[i].value, expected[i].value) {
			return fmt.Errorf("%w: descriptor op %d key=%q value=%x want key=%q value=%x", collectionwal.ErrCollectionWALIdentityMismatch, i, entries[i].key, entries[i].value, expected[i].key, expected[i].value)
		}
	}
	return nil
}

func expectedCollectionWALDescriptorOps(template collectionWALSystemDeltaTemplate) []collectionWALSystemEntry {
	entries := make([]collectionWALSystemEntry, 0, len(template.roots)+1)
	for _, root := range template.roots {
		entries = append(entries, collectionWALSystemEntry{
			key:   []byte(systemCollectionRootPrefix + root.rootName),
			value: encodeCollectionWALRootID(root.originalRootID),
		})
	}
	entries = append(entries, collectionWALSystemEntry{
		key:   []byte(systemCollectionWALAppliedPrefix + hex.EncodeToString(template.collectionUID[:])),
		value: encodeCollectionWALRootID(template.collectionSeq),
	})
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})
	return entries
}

func collectionWALSystemEntriesEqual(a, b []collectionWALSystemEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i].key, b[i].key) || !bytes.Equal(a[i].value, b[i].value) {
			return false
		}
	}
	return true
}

type collectionWALSectionReader struct {
	data []byte
	off  int
}

func (r *collectionWALSectionReader) expectMagic(magic [8]byte) error {
	if len(r.data)-r.off < len(magic) {
		return fmt.Errorf("%w: descriptor section missing magic", collectionwal.ErrCollectionWALCorruptMiddle)
	}
	if !bytes.Equal(r.data[r.off:r.off+len(magic)], magic[:]) {
		return fmt.Errorf("%w: descriptor section bad magic", collectionwal.ErrCollectionWALCorruptMiddle)
	}
	r.off += len(magic)
	return nil
}

func (r *collectionWALSectionReader) u16() (uint16, error) {
	if len(r.data)-r.off < 2 {
		return 0, fmt.Errorf("%w: descriptor section short u16", collectionwal.ErrCollectionWALCorruptMiddle)
	}
	v := binary.LittleEndian.Uint16(r.data[r.off : r.off+2])
	r.off += 2
	return v, nil
}

func (r *collectionWALSectionReader) u32() (uint32, error) {
	if len(r.data)-r.off < 4 {
		return 0, fmt.Errorf("%w: descriptor section short u32", collectionwal.ErrCollectionWALCorruptMiddle)
	}
	v := binary.LittleEndian.Uint32(r.data[r.off : r.off+4])
	r.off += 4
	return v, nil
}

func (r *collectionWALSectionReader) u64() (uint64, error) {
	if len(r.data)-r.off < 8 {
		return 0, fmt.Errorf("%w: descriptor section short u64", collectionwal.ErrCollectionWALCorruptMiddle)
	}
	v := binary.LittleEndian.Uint64(r.data[r.off : r.off+8])
	r.off += 8
	return v, nil
}

func (r *collectionWALSectionReader) fixed(n int) ([]byte, error) {
	if n < 0 || len(r.data)-r.off < n {
		return nil, fmt.Errorf("%w: descriptor section short fixed bytes", collectionwal.ErrCollectionWALCorruptMiddle)
	}
	out := r.data[r.off : r.off+n]
	r.off += n
	return out, nil
}

func (r *collectionWALSectionReader) bytes(max int) ([]byte, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	if n > uint32(max) {
		return nil, fmt.Errorf("%w: descriptor section bytes %d exceeds %d", collectionwal.ErrCollectionWALResourceLimit, n, max)
	}
	if uint64(len(r.data)-r.off) < uint64(n) {
		return nil, fmt.Errorf("%w: descriptor section short bytes", collectionwal.ErrCollectionWALCorruptMiddle)
	}
	out := r.data[r.off : r.off+int(n)]
	r.off += int(n)
	return out, nil
}

func (r *collectionWALSectionReader) done() error {
	if r.off != len(r.data) {
		return fmt.Errorf("%w: descriptor section trailing bytes", collectionwal.ErrCollectionWALCorruptMiddle)
	}
	return nil
}

type collectionWALSystemEntry struct {
	key   []byte
	value []byte
}

type collectionWALSystemDeltaIterator struct {
	entries []collectionWALSystemEntry
	idx     int
}

func (it *collectionWALSystemDeltaIterator) Valid() bool {
	return it != nil && it.idx >= 0 && it.idx < len(it.entries)
}

func (it *collectionWALSystemDeltaIterator) Next() {
	if it != nil && it.idx < len(it.entries) {
		it.idx++
	}
}

func (it *collectionWALSystemDeltaIterator) Seek(key []byte) {
	if it == nil {
		return
	}
	it.idx = sort.Search(len(it.entries), func(i int) bool {
		return bytes.Compare(it.entries[i].key, key) >= 0
	})
}

func (it *collectionWALSystemDeltaIterator) UnsafeKey() []byte {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.idx].key
}

func (it *collectionWALSystemDeltaIterator) UnsafeValue() []byte {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.idx].value
}

func (it *collectionWALSystemDeltaIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.Valid() {
		return nil, page.ValuePtr{}, node.FlagInline
	}
	return it.entries[it.idx].value, page.ValuePtr{}, node.FlagInline
}

func (it *collectionWALSystemDeltaIterator) Key() []byte {
	return it.UnsafeKey()
}

func (it *collectionWALSystemDeltaIterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *collectionWALSystemDeltaIterator) KeyCopy(dst []byte) []byte {
	if !it.Valid() {
		return dst
	}
	return append(dst, it.entries[it.idx].key...)
}

func (it *collectionWALSystemDeltaIterator) ValueCopy(dst []byte) []byte {
	if !it.Valid() {
		return dst
	}
	return append(dst, it.entries[it.idx].value...)
}

func (it *collectionWALSystemDeltaIterator) IsDeleted() bool {
	return false
}

func (it *collectionWALSystemDeltaIterator) Error() error {
	return nil
}

func (it *collectionWALSystemDeltaIterator) Close() error {
	return nil
}

func (it *collectionWALSystemDeltaIterator) Domain() (start, end []byte) {
	return nil, nil
}

func (it *collectionWALSystemDeltaIterator) StableUnsafeIteratorSlices() bool {
	return true
}
