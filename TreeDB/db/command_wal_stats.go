package db

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

type commandWALStatsSummary struct {
	SegmentFiles   int
	TypedSegments  int
	ActiveSegments int
	Frames         uint64
	MaxLSN         uint64
	Bytes          int64
}

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
	stats["treedb.command_wal.required_feature"] = "false"
	if required, err := CommandWALRequiredFeatureEnabled(db.dir); err == nil {
		stats["treedb.command_wal.required_feature"] = fmt.Sprintf("%t", required)
	} else {
		stats["treedb.command_wal.required_feature_error"] = err.Error()
	}
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
	} else {
		stats["treedb.command_wal.stats_error"] = err.Error()
	}
}

func writeCommandWALStatsZeroCounters(stats map[string]string) {
	stats["treedb.command_wal.segment_files"] = "0"
	stats["treedb.command_wal.typed_segments"] = "0"
	stats["treedb.command_wal.active_segments"] = "0"
	stats["treedb.command_wal.frames"] = "0"
	stats["treedb.command_wal.max_lsn"] = "0"
	stats["treedb.command_wal.bytes"] = "0"
}

func (db *DB) cachedCommandWALStatsSummary() (commandWALStatsSummary, error) {
	state := db.state.Load()
	appliedLSN := uint64(0)
	if state != nil {
		appliedLSN = state.AppliedCommandLSN
	}
	db.commandWALStatsMu.Lock()
	defer db.commandWALStatsMu.Unlock()
	if db.commandWALStatsOK && db.commandWALStatsAppliedLSN == appliedLSN {
		summary := db.commandWALStatsSummary
		return summary, nil
	}

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
	activeByLane, err := commandWALActiveSeqByLane(segments, maxSegmentBytes)
	if err != nil {
		return commandWALStatsSummary{}, err
	}
	var summary commandWALStatsSummary
	for _, seg := range segments {
		if seg.valueLog || seg.size == 0 || !isCommandWALLaneSegment(seg) {
			continue
		}
		summary.SegmentFiles++
		summary.Bytes += seg.size
		active := seg.seq == activeByLane[seg.lane]
		if active {
			summary.ActiveSegments++
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
		return typed, frames, maxLSN, fmt.Errorf("treedb: summarize command wal segment %s: %w", filepath.Base(path), readErr)
	}
}
