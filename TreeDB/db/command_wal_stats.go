package db

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

type commandWALStatsSummary struct {
	SegmentFiles   int
	TypedSegments  int
	ActiveSegments int
	Frames         uint64
	MaxLSN         uint64
	Bytes          int64
	ActiveBytes    int64
	ActiveName     string
}

type commandWALAppendStatsPath uint8

const (
	commandWALAppendStatsPoint commandWALAppendStatsPath = iota
	commandWALAppendStatsPayload
	commandWALAppendStatsEntryScan
	commandWALAppendStatsIntent
)

func writeCommandWALStats(stats map[string]string, db *DB) {
	if stats == nil || db == nil {
		return
	}
	writeCommandWALStatsZeroCounters(stats)
	if !db.commandWAL {
		stats["treedb.command_wal.required_feature"] = "false"
		stats["treedb.command_wal.stats_scan"] = "false"
		return
	}
	stats["treedb.command_wal.required_feature"] = fmt.Sprintf("%t", db.commandWALRequiredFeature)
	if db.commandWALRequiredErr != "" {
		stats["treedb.command_wal.required_feature_error"] = db.commandWALRequiredErr
	}
	writeCommandWALLiveStats(stats, db)
	writeCommandWALLifecycleStats(stats, db)
	if !db.commandWALStatsScan {
		stats["treedb.command_wal.stats_scan"] = "false"
		return
	}
	stats["treedb.command_wal.stats_scan"] = "true"
	if summary, err := db.cachedCommandWALStatsSummary(); err == nil {
		stats["treedb.command_wal.segment_files"] = fmt.Sprintf("%d", summary.SegmentFiles)
		stats["treedb.command_wal.typed_segments"] = fmt.Sprintf("%d", summary.TypedSegments)
		stats["treedb.command_wal.active_segments"] = fmt.Sprintf("%d", summary.ActiveSegments)
		stats["treedb.command_wal.frames"] = fmt.Sprintf("%d", summary.Frames)
		stats["treedb.command_wal.max_lsn"] = fmt.Sprintf("%d", summary.MaxLSN)
		stats["treedb.command_wal.bytes"] = fmt.Sprintf("%d", summary.Bytes)
		stats["treedb.command_wal.segments.total"] = fmt.Sprintf("%d", summary.SegmentFiles)
		stats["treedb.command_wal.segments.active"] = fmt.Sprintf("%d", summary.ActiveSegments)
		stats["treedb.command_wal.bytes.total"] = fmt.Sprintf("%d", summary.Bytes)
		stats["treedb.command_wal.bytes.active"] = fmt.Sprintf("%d", summary.ActiveBytes)
		stats["treedb.command_wal.active_segment.name"] = summary.ActiveName
	} else {
		stats["treedb.command_wal.stats_error"] = err.Error()
	}
}

func (db *DB) cacheCommandWALRequiredFeatureStats() {
	required, err := CommandWALRequiredFeatureEnabled(db.dir)
	db.commandWALRequiredFeature = required
	if err != nil {
		db.commandWALRequiredErr = err.Error()
		return
	}
	db.commandWALRequiredErr = ""
}

func writeCommandWALStatsZeroCounters(stats map[string]string) {
	stats["treedb.command_wal.segment_files"] = "0"
	stats["treedb.command_wal.typed_segments"] = "0"
	stats["treedb.command_wal.active_segments"] = "0"
	stats["treedb.command_wal.frames"] = "0"
	stats["treedb.command_wal.max_lsn"] = "0"
	stats["treedb.command_wal.bytes"] = "0"
	stats["treedb.command_wal.segments.total"] = "0"
	stats["treedb.command_wal.segments.active"] = "0"
	stats["treedb.command_wal.bytes.total"] = "0"
	stats["treedb.command_wal.bytes.active"] = "0"
	stats["treedb.command_wal.active_segment.name"] = ""
	stats["treedb.command_wal.applied_lsn"] = "0"
	stats["treedb.command_wal.next_lsn"] = "0"
	stats["treedb.command_wal.live_accepted_frames"] = "0"
	stats["treedb.command_wal.live_accepted_max_lsn"] = "0"
	stats["treedb.command_wal.live_covered_frames"] = "0"
	stats["treedb.command_wal.live_covered_max_lsn"] = "0"
	stats["treedb.command_wal.append.count_total"] = "0"
	stats["treedb.command_wal.append.ns_total"] = "0"
	stats["treedb.command_wal.append.point.count_total"] = "0"
	stats["treedb.command_wal.append.point.ns_total"] = "0"
	stats["treedb.command_wal.append.payload.count_total"] = "0"
	stats["treedb.command_wal.append.payload.ns_total"] = "0"
	stats["treedb.command_wal.append.entry_scan.count_total"] = "0"
	stats["treedb.command_wal.append.entry_scan.ns_total"] = "0"
	stats["treedb.command_wal.append.intent.count_total"] = "0"
	stats["treedb.command_wal.append.intent.ns_total"] = "0"
	stats["treedb.command_wal.flush.count_total"] = "0"
	stats["treedb.command_wal.flush.ns_total"] = "0"
	stats["treedb.command_wal.sync.count_total"] = "0"
	stats["treedb.command_wal.sync.ns_total"] = "0"
	stats["treedb.command_wal.cleanup.scans"] = "0"
	stats["treedb.command_wal.cleanup.scan.count_total"] = "0"
	stats["treedb.command_wal.cleanup.scan.ns_total"] = "0"
	stats["treedb.command_wal.cleanup.scanned_bytes_total"] = "0"
	stats["treedb.command_wal.cleanup.scanned_frames_total"] = "0"
	stats["treedb.command_wal.cleanup.removed_segments"] = "0"
	stats["treedb.command_wal.cleanup.removed_bytes"] = "0"
	stats["treedb.command_wal.writer.buffer_size_bytes"] = "0"
	stats["treedb.command_wal.writer.buffered_bytes"] = "0"
	stats["treedb.command_wal.writer.scratch_capacity_bytes"] = "0"
	stats["treedb.command_wal.writer.command_buffer.length_bytes"] = "0"
	stats["treedb.command_wal.writer.command_buffer.capacity_bytes"] = "0"
	stats["treedb.command_wal.writer.command_buffer.limit_bytes"] = "0"
	stats["treedb.command_wal.writer.command_buffer.retain_limit_bytes"] = "0"
	stats["treedb.command_wal.writer.command_buffer.trim_count"] = "0"
	stats["treedb.command_wal.writer.command_buffer.dropped_bytes_total"] = "0"
	stats["treedb.command_wal.writer.pending_batch.length_bytes"] = "0"
	stats["treedb.command_wal.writer.pending_batch.capacity_bytes"] = "0"
}

func writeCommandWALLiveStats(stats map[string]string, db *DB) {
	stats["treedb.command_wal.live_accepted_frames"] = fmt.Sprintf("%d", db.commandWALLiveAccepted.Load())
	stats["treedb.command_wal.live_accepted_max_lsn"] = fmt.Sprintf("%d", db.commandWALLiveAcceptedMax.Load())
	stats["treedb.command_wal.live_covered_frames"] = fmt.Sprintf("%d", db.commandWALLiveCovered.Load())
	stats["treedb.command_wal.live_covered_max_lsn"] = fmt.Sprintf("%d", db.commandWALLiveCoveredMax.Load())
}

func writeCommandWALLifecycleStats(stats map[string]string, db *DB) {
	appliedLSN := uint64(0)
	if state := db.state.Load(); state != nil {
		appliedLSN = state.AppliedCommandLSN
	}
	stats["treedb.command_wal.applied_lsn"] = fmt.Sprintf("%d", appliedLSN)
	stats["treedb.command_wal.next_lsn"] = fmt.Sprintf("%d", db.CommandWALNextLSN())
	stats["treedb.command_wal.bytes.active"] = fmt.Sprintf("%d", db.CommandWALActiveBytes())
	stats["treedb.command_wal.append.count_total"] = fmt.Sprintf("%d", db.commandWALAppendCount.Load())
	stats["treedb.command_wal.append.ns_total"] = fmt.Sprintf("%d", db.commandWALAppendNs.Load())
	stats["treedb.command_wal.append.point.count_total"] = fmt.Sprintf("%d", db.commandWALAppendPointCount.Load())
	stats["treedb.command_wal.append.point.ns_total"] = fmt.Sprintf("%d", db.commandWALAppendPointNs.Load())
	stats["treedb.command_wal.append.payload.count_total"] = fmt.Sprintf("%d", db.commandWALAppendPayloadCount.Load())
	stats["treedb.command_wal.append.payload.ns_total"] = fmt.Sprintf("%d", db.commandWALAppendPayloadNs.Load())
	stats["treedb.command_wal.append.entry_scan.count_total"] = fmt.Sprintf("%d", db.commandWALAppendEntryScanCount.Load())
	stats["treedb.command_wal.append.entry_scan.ns_total"] = fmt.Sprintf("%d", db.commandWALAppendEntryScanNs.Load())
	stats["treedb.command_wal.append.intent.count_total"] = fmt.Sprintf("%d", db.commandWALAppendIntentCount.Load())
	stats["treedb.command_wal.append.intent.ns_total"] = fmt.Sprintf("%d", db.commandWALAppendIntentNs.Load())
	stats["treedb.command_wal.flush.count_total"] = fmt.Sprintf("%d", db.commandWALFlushCount.Load())
	stats["treedb.command_wal.flush.ns_total"] = fmt.Sprintf("%d", db.commandWALFlushNs.Load())
	stats["treedb.command_wal.sync.count_total"] = fmt.Sprintf("%d", db.commandWALSyncCount.Load())
	stats["treedb.command_wal.sync.ns_total"] = fmt.Sprintf("%d", db.commandWALSyncNs.Load())
	stats["treedb.command_wal.cleanup.scans"] = fmt.Sprintf("%d", db.commandWALCleanupScans.Load())
	stats["treedb.command_wal.cleanup.scan.count_total"] = fmt.Sprintf("%d", db.commandWALCleanupScans.Load())
	stats["treedb.command_wal.cleanup.scan.ns_total"] = fmt.Sprintf("%d", db.commandWALCleanupScanNs.Load())
	stats["treedb.command_wal.cleanup.scanned_bytes_total"] = fmt.Sprintf("%d", db.commandWALCleanupScanBytes.Load())
	stats["treedb.command_wal.cleanup.scanned_frames_total"] = fmt.Sprintf("%d", db.commandWALCleanupScanFrames.Load())
	stats["treedb.command_wal.cleanup.removed_segments"] = fmt.Sprintf("%d", db.commandWALCleanupRemoved.Load())
	stats["treedb.command_wal.cleanup.removed_bytes"] = fmt.Sprintf("%d", db.commandWALCleanupBytes.Load())
	writeCommandWALWriterBufferStats(stats, db)
}

func writeCommandWALWriterBufferStats(stats map[string]string, db *DB) {
	if stats == nil || db == nil || db.commandJournal == nil {
		return
	}
	snap := db.commandJournal.WriterBufferStats()
	stats["treedb.command_wal.writer.buffer_size_bytes"] = fmt.Sprintf("%d", snap.BufferedWriterSize)
	stats["treedb.command_wal.writer.buffered_bytes"] = fmt.Sprintf("%d", snap.BufferedWriterBufferedBytes)
	stats["treedb.command_wal.writer.scratch_capacity_bytes"] = fmt.Sprintf("%d", snap.ScratchCapacity)
	stats["treedb.command_wal.writer.command_buffer.length_bytes"] = fmt.Sprintf("%d", snap.CommandBufferLength)
	stats["treedb.command_wal.writer.command_buffer.capacity_bytes"] = fmt.Sprintf("%d", snap.CommandBufferCapacity)
	stats["treedb.command_wal.writer.command_buffer.limit_bytes"] = fmt.Sprintf("%d", snap.CommandBufferLimit)
	stats["treedb.command_wal.writer.command_buffer.retain_limit_bytes"] = fmt.Sprintf("%d", snap.CommandBufferRetainLimit)
	stats["treedb.command_wal.writer.command_buffer.trim_count"] = fmt.Sprintf("%d", snap.CommandBufferTrimCount)
	stats["treedb.command_wal.writer.command_buffer.dropped_bytes_total"] = fmt.Sprintf("%d", snap.CommandBufferDroppedBytes)
	stats["treedb.command_wal.writer.pending_batch.length_bytes"] = fmt.Sprintf("%d", snap.PendingBatchLength)
	stats["treedb.command_wal.writer.pending_batch.capacity_bytes"] = fmt.Sprintf("%d", snap.PendingBatchCapacity)
}

func (db *DB) observeCommandWALAccepted(lsn uint64) {
	if db == nil || lsn == 0 {
		return
	}
	db.commandWALLiveAccepted.Add(1)
	commandWALStoreMax(&db.commandWALLiveAcceptedMax, lsn)
}

func (db *DB) observeCommandWALCovered(previous, next uint64) {
	if db == nil || next <= previous {
		return
	}
	db.commandWALLiveCovered.Add(next - previous)
	commandWALStoreMax(&db.commandWALLiveCoveredMax, next)
}

func (db *DB) observeCommandWALAppend(path commandWALAppendStatsPath, elapsed time.Duration) {
	if db == nil {
		return
	}
	ns := commandWALDurationNs(elapsed)
	db.commandWALAppendCount.Add(1)
	if ns > 0 {
		db.commandWALAppendNs.Add(ns)
	}
	switch path {
	case commandWALAppendStatsPoint:
		db.commandWALAppendPointCount.Add(1)
		if ns > 0 {
			db.commandWALAppendPointNs.Add(ns)
		}
	case commandWALAppendStatsPayload:
		db.commandWALAppendPayloadCount.Add(1)
		if ns > 0 {
			db.commandWALAppendPayloadNs.Add(ns)
		}
	case commandWALAppendStatsEntryScan:
		db.commandWALAppendEntryScanCount.Add(1)
		if ns > 0 {
			db.commandWALAppendEntryScanNs.Add(ns)
		}
	case commandWALAppendStatsIntent:
		db.commandWALAppendIntentCount.Add(1)
		if ns > 0 {
			db.commandWALAppendIntentNs.Add(ns)
		}
	}
}

func (db *DB) observeCommandWALFlush(sync bool, elapsed time.Duration) {
	if db == nil {
		return
	}
	ns := commandWALDurationNs(elapsed)
	if sync {
		db.commandWALSyncCount.Add(1)
		if ns > 0 {
			db.commandWALSyncNs.Add(ns)
		}
		return
	}
	db.commandWALFlushCount.Add(1)
	if ns > 0 {
		db.commandWALFlushNs.Add(ns)
	}
}

func commandWALDurationNs(d time.Duration) uint64 {
	if d <= 0 {
		return 0
	}
	return uint64(d.Nanoseconds())
}

func commandWALStoreMax(dst *atomic.Uint64, value uint64) {
	for {
		current := dst.Load()
		if current >= value {
			return
		}
		if dst.CompareAndSwap(current, value) {
			return
		}
	}
}

func (db *DB) cachedCommandWALStatsSummary() (commandWALStatsSummary, error) {
	if err := db.flushCommandWAL(false, false); err != nil {
		return commandWALStatsSummary{}, err
	}
	state := db.state.Load()
	appliedLSN := uint64(0)
	if state != nil {
		appliedLSN = state.AppliedCommandLSN
	}
	db.commandWALStatsMu.Lock()
	defer db.commandWALStatsMu.Unlock()

	summary, err := summarizeCommandWALStats(db.dir, db.walMaxSegmentBytes)
	if err != nil {
		return commandWALStatsSummary{}, err
	}
	db.commandWALStatsAppliedLSN = appliedLSN
	db.commandWALStatsSummary = summary
	db.commandWALStatsOK = true
	return summary, nil
}

func summarizeCommandWALStats(dir string, maxSegmentBytes int64) (commandWALStatsSummary, error) {
	segments, err := listWALSegments(dir)
	if err != nil {
		return commandWALStatsSummary{}, err
	}
	activeByLane := commandWALActiveSeqByLane(segments)
	var summary commandWALStatsSummary
	for _, seg := range segments {
		if seg.valueLog || !isCommandWALLaneSegment(seg) {
			continue
		}
		summary.SegmentFiles++
		summary.Bytes += seg.size
		active := seg.seq == activeByLane[seg.lane]
		if active {
			summary.ActiveSegments++
			summary.ActiveBytes += seg.size
			name := filepath.Base(seg.path)
			if summary.ActiveName == "" {
				summary.ActiveName = name
			} else {
				summary.ActiveName += "," + name
			}
		}
		if seg.size == 0 {
			continue
		}
		typed, frames, maxLSN, err := summarizeCommandWALSegment(seg.path, maxSegmentBytes, active)
		if err != nil {
			return summary, err
		}
		if !typed {
			continue
		}
		summary.TypedSegments++
		summary.Frames += frames
		if maxLSN > summary.MaxLSN {
			summary.MaxLSN = maxLSN
		}
	}
	return summary, nil
}

func summarizeCommandWALSegment(path string, maxSegmentBytes int64, allowTerminalTail bool) (typed bool, frames uint64, maxLSN uint64, err error) {
	reader, err := commitlog.NewReaderWithOptions(path, commitlog.Options{MaxSegmentSize: maxSegmentBytes})
	if err != nil {
		return false, 0, 0, err
	}
	defer func() {
		if closeErr := reader.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	for {
		env, readErr := reader.ReadCommandFrame()
		if readErr == nil {
			typed = true
			frames++
			if env.LSN > maxLSN {
				maxLSN = env.LSN
			}
			continue
		}
		if errors.Is(readErr, io.EOF) {
			return typed, frames, maxLSN, nil
		}
		if errors.Is(readErr, commitlog.ErrCommandWALTerminalTail) && allowTerminalTail {
			return typed, frames, maxLSN, nil
		}
		if errors.Is(readErr, commitlog.ErrCommandWALLegacyPayload) && !typed {
			return false, 0, 0, nil
		}
		return typed, frames, maxLSN, fmt.Errorf("treedb: summarize command WAL segment %s: %w", filepath.Base(path), readErr)
	}
}
