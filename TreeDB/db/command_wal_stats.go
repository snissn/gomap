package db

import (
	"errors"
	"fmt"
	"io"

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
		return typed, frames, maxLSN, fmt.Errorf("treedb: summarize command wal segment %s: %w", path, readErr)
	}
}
