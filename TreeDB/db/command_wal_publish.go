package db

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/page"
)

var (
	ErrCommandWALSplitPublish         = errors.New("treedb: command wal roots changed without advancing applied lsn")
	ErrCommandWALAppliedLSNRegression = errors.New("treedb: command wal applied lsn regression")
	ErrCommandWALAppliedLSNNonContig  = errors.New("treedb: command wal applied lsn non-contiguous")
)

type CommandWALLSNRange struct {
	First uint64
	Last  uint64
}

type finalizeCommitOptions struct {
	commandWALPublish bool
	appliedCommandLSN uint64
	appliedRanges     []CommandWALLSNRange
}

func (db *DB) publishCommandWALRoots(newRootID uint64, sysRootID uint64, appliedLSN uint64, covered []CommandWALLSNRange, sync bool) error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	post, err := db.finalizeCommitLockedWithOptions(
		newRootID,
		sysRootID,
		nil,
		sync,
		adaptive.Metrics{},
		nil,
		false,
		nil,
		nil,
		nil,
		finalizeCommitOptions{
			commandWALPublish: true,
			appliedCommandLSN: appliedLSN,
			appliedRanges:     covered,
		},
	)
	if err != nil {
		return err
	}
	db.finalizeCommitPostWork(post)
	return nil
}

func validateCommandWALPublishLocked(current page.MetaPageBody, newRootID uint64, sysRootID uint64, opts finalizeCommitOptions) error {
	if !opts.commandWALPublish {
		return nil
	}
	rootsChanged := newRootID != current.UserRootPageID || sysRootID != current.SystemRootPageID
	if rootsChanged && opts.appliedCommandLSN == current.AppliedCommandLSN {
		return ErrCommandWALSplitPublish
	}
	return validateContiguousAppliedCommandLSN(current.AppliedCommandLSN, opts.appliedCommandLSN, opts.appliedRanges)
}

// validateContiguousAppliedCommandLSN verifies that covered spans advance the
// applied LSN without gaps. It never mutates the caller-owned covered slice.
func validateContiguousAppliedCommandLSN(current, next uint64, covered []CommandWALLSNRange) error {
	if next < current {
		return fmt.Errorf("%w: current=%d next=%d", ErrCommandWALAppliedLSNRegression, current, next)
	}
	if next == current {
		return nil
	}
	if current == ^uint64(0) {
		return fmt.Errorf("%w: current lsn exhausted", ErrCommandWALAppliedLSNNonContig)
	}
	if len(covered) == 0 {
		return fmt.Errorf("%w: missing coverage for [%d,%d]", ErrCommandWALAppliedLSNNonContig, current+1, next)
	}
	ranges := covered
	sorted := true
	for i := 1; i < len(ranges); i++ {
		prev, cur := ranges[i-1], ranges[i]
		if prev.First > cur.First || (prev.First == cur.First && prev.Last > cur.Last) {
			sorted = false
			break
		}
	}
	if !sorted {
		ranges = append([]CommandWALLSNRange(nil), covered...)
		sort.Slice(ranges, func(i, j int) bool {
			if ranges[i].First != ranges[j].First {
				return ranges[i].First < ranges[j].First
			}
			return ranges[i].Last < ranges[j].Last
		})
	}
	cursor := current + 1
	for _, r := range ranges {
		if r.First == 0 || r.Last < r.First {
			return fmt.Errorf("%w: invalid coverage range [%d,%d]", ErrCommandWALAppliedLSNNonContig, r.First, r.Last)
		}
		if r.Last < cursor {
			continue
		}
		if r.First > cursor {
			return fmt.Errorf("%w: gap before %d", ErrCommandWALAppliedLSNNonContig, cursor)
		}
		if r.Last >= next {
			return nil
		}
		if r.Last == ^uint64(0) {
			return fmt.Errorf("%w: lsn range exhausted", ErrCommandWALAppliedLSNNonContig)
		}
		nextCursor := r.Last + 1
		if nextCursor <= r.Last {
			return fmt.Errorf("%w: lsn range exhausted", ErrCommandWALAppliedLSNNonContig)
		}
		cursor = nextCursor
	}
	return fmt.Errorf("%w: gap before %d", ErrCommandWALAppliedLSNNonContig, cursor)
}

type commandWALSegmentCleanupDecision struct {
	Path    string
	MaxLSN  uint64
	Active  bool
	Covered bool
	Removed bool
	Error   string
}

type commandWALSegmentScanResult struct {
	maxLSN       uint64
	minLSN       uint64
	typed        bool
	terminalTail bool
}

func requireNoUnappliedCommandWAL(dir string, appliedLSN uint64, maxSegmentBytes int64) error {
	dirty, err := hasUnappliedCommandWALFrames(dir, appliedLSN, maxSegmentBytes)
	if err != nil {
		return err
	}
	if dirty {
		return ErrRecoveryRequired
	}
	return nil
}

func hasUnappliedCommandWALFrames(dir string, appliedLSN uint64, maxSegmentBytes int64) (bool, error) {
	segments, err := listWALSegments(dir)
	if err != nil {
		return false, err
	}
	activeByLane := commandWALActiveSeqByLane(segments)
	seenLSNs := make(map[uint64]struct{})
	for _, seg := range segments {
		if seg.valueLog || seg.size == 0 {
			continue
		}
		if !isCommandWALLaneSegment(seg) {
			continue
		}
		scan, err := scanCommandWALSegmentWithSeen(seg.path, maxSegmentBytes, seg.seq == activeByLane[seg.lane], seenLSNs, appliedLSN)
		if err != nil {
			return false, err
		}
		if scan.typed && scan.maxLSN > appliedLSN {
			return true, fmt.Errorf("%w: command WAL segment %s max LSN %d exceeds applied LSN %d", ErrRecoveryRequired, filepath.Base(seg.path), scan.maxLSN, appliedLSN)
		}
	}
	return false, nil
}

func commandWALActiveSeqByLane(segments []logSegment) map[int]uint64 {
	activeByLane := make(map[int]uint64)
	for _, seg := range segments {
		if seg.valueLog {
			continue
		}
		if !isCommandWALLaneSegment(seg) {
			continue
		}
		if seg.seq > activeByLane[seg.lane] {
			activeByLane[seg.lane] = seg.seq
		}
	}
	return activeByLane
}

func isCommandWALLaneSegment(seg logSegment) bool {
	return commitlog.IsCommandSegmentName(filepath.Base(seg.path))
}

func commandWALSegmentMaxLSN(path string, maxSegmentBytes int64, allowTerminalTail bool) (maxLSN uint64, typed bool, err error) {
	scan, err := scanCommandWALSegment(path, maxSegmentBytes, allowTerminalTail)
	return scan.maxLSN, scan.typed, err
}

func scanCommandWALSegment(path string, maxSegmentBytes int64, allowTerminalTail bool) (commandWALSegmentScanResult, error) {
	return scanCommandWALSegmentWithSeen(path, maxSegmentBytes, allowTerminalTail, nil, 0)
}

func scanCommandWALSegmentWithSeen(path string, maxSegmentBytes int64, allowTerminalTail bool, seenLSNs map[uint64]struct{}, appliedLSN uint64) (commandWALSegmentScanResult, error) {
	// PR2 has no durable per-segment max-LSN catalog yet, so open/cleanup
	// paths derive classification by streaming the segment without retaining
	// payloads. TODO(command-wal): cache validated per-segment min/max LSN
	// summaries once the cleanup manifest/catalog is introduced so restart
	// does not rescan old covered typed bytes.
	r, err := commitlog.NewReaderWithOptions(path, commitlog.Options{MaxSegmentSize: maxSegmentBytes})
	if err != nil {
		return commandWALSegmentScanResult{}, err
	}
	defer r.Close()

	var lastLSN uint64
	var scan commandWALSegmentScanResult
	for {
		frame, err := r.ReadCommandFrame()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return scan, nil
			}
			if errors.Is(err, commitlog.ErrCommandWALTerminalTail) {
				scan.terminalTail = true
				if allowTerminalTail {
					return scan, nil
				}
				return scan, err
			}
			if errors.Is(err, commitlog.ErrCommandWALLegacyPayload) && !scan.typed {
				return commandWALSegmentScanResult{}, nil
			}
			return scan, err
		}
		if lastLSN != 0 && frame.LSN <= lastLSN {
			scan.typed = true
			return scan, commitlog.ErrCommandWALDuplicateLSN
		}
		if seenLSNs != nil && (appliedLSN == 0 || frame.LSN > appliedLSN) {
			if _, ok := seenLSNs[frame.LSN]; ok {
				scan.typed = true
				return scan, commitlog.ErrCommandWALDuplicateLSN
			}
			seenLSNs[frame.LSN] = struct{}{}
		}
		lastLSN = frame.LSN
		scan.typed = true
		if scan.minLSN == 0 || frame.LSN < scan.minLSN {
			scan.minLSN = frame.LSN
		}
		if frame.LSN > scan.maxLSN {
			scan.maxLSN = frame.LSN
		}
	}
}

func filterCommandWALSegmentsForLegacyReplay(segments []logSegment, appliedLSN uint64, maxSegmentBytes int64) ([]logSegment, error) {
	var filtered []logSegment
	skipped := false
	activeByLane := commandWALActiveSeqByLane(segments)
	seenLSNs := make(map[uint64]struct{})
	for i, seg := range segments {
		if seg.valueLog || seg.size == 0 {
			if skipped {
				filtered = append(filtered, seg)
			}
			continue
		}
		if !isCommandWALLaneSegment(seg) {
			if skipped {
				filtered = append(filtered, seg)
			}
			continue
		}
		active := seg.seq == activeByLane[seg.lane]
		scan, err := scanCommandWALSegmentWithSeen(seg.path, maxSegmentBytes, active, seenLSNs, appliedLSN)
		if err != nil {
			return nil, err
		}
		if !scan.typed {
			if skipped {
				filtered = append(filtered, seg)
			}
			continue
		}
		// Covered typed segments are skipped by the maxLSN check below. This
		// branch is only the crossing case where part of the segment is already
		// published and part would still need command replay. With appliedLSN=0
		// no command frame can be partly covered, so the first typed frame falls
		// through to the fully-unapplied branch below.
		if scan.maxLSN > appliedLSN && scan.minLSN <= appliedLSN {
			return nil, fmt.Errorf("%w: command WAL segment %s partially applied range [%d,%d] over applied LSN %d", ErrRecoveryRequired, filepath.Base(seg.path), scan.minLSN, scan.maxLSN, appliedLSN)
		}
		if scan.maxLSN <= appliedLSN {
			if !skipped {
				filtered = make([]logSegment, 0, len(segments)-1)
				filtered = append(filtered, segments[:i]...)
			}
			skipped = true
			continue
		}
		// scan.typed && scan.maxLSN > appliedLSN: the entire segment is
		// unapplied (partial-application and covered cases handled above).
		return nil, fmt.Errorf("%w: command WAL frame LSN %d exceeds applied LSN %d", ErrRecoveryRequired, scan.maxLSN, appliedLSN)
	}
	if !skipped {
		return segments, nil
	}
	return filtered, nil
}

func cleanupCommandWALSegmentsCoveredByAppliedLSN(dir string, appliedLSN uint64, maxSegmentBytes int64) ([]commandWALSegmentCleanupDecision, error) {
	// PR2 cleanup deliberately streams segments on demand. It is intended for
	// checkpoint/maintenance boundaries; a later manifest/catalog can cache
	// results if this moves onto a hotter path.
	segments, err := listWALSegments(dir)
	if err != nil {
		return nil, err
	}
	activeByLane := commandWALActiveSeqByLane(segments)
	decisions := make([]commandWALSegmentCleanupDecision, 0, len(segments))
	var scanErr error
	seenLSNs := make(map[uint64]struct{})
	for _, seg := range segments {
		if seg.valueLog || seg.size == 0 {
			continue
		}
		if !isCommandWALLaneSegment(seg) {
			continue
		}
		active := seg.seq == activeByLane[seg.lane]
		scan, err := scanCommandWALSegmentWithSeen(seg.path, maxSegmentBytes, active, seenLSNs, appliedLSN)
		if err != nil {
			decisions = append(decisions, commandWALSegmentCleanupDecision{
				Path:   seg.path,
				Active: active,
				Error:  err.Error(),
			})
			scanErr = errors.Join(scanErr, fmt.Errorf("scan command WAL segment %s: %w", filepath.Base(seg.path), err))
			continue
		}
		if !scan.typed {
			continue
		}
		decision := commandWALSegmentCleanupDecision{
			Path:    seg.path,
			MaxLSN:  scan.maxLSN,
			Active:  active,
			Covered: scan.maxLSN <= appliedLSN,
		}
		decisions = append(decisions, decision)
	}
	if scanErr != nil {
		return decisions, scanErr
	}
	for i := range decisions {
		decision := &decisions[i]
		if decision.Covered && !decision.Active {
			if err := os.Remove(decision.Path); err != nil && !os.IsNotExist(err) {
				return decisions, err
			}
			decision.Removed = true
		}
	}
	return decisions, nil
}
