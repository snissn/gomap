package db

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/page"
)

var (
	ErrCommandWALSplitPublish         = errors.New("treedb: command wal roots require matching applied lsn")
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

	if err := db.ensureCommandWALMetaV1FormatMarker(); err != nil {
		return err
	}
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

func (db *DB) ensureCommandWALMetaV1FormatMarker() error {
	if db.commandWALMetaV1 {
		return nil
	}
	cfg, ok, err := LoadFormatConfig(db.dir)
	if err != nil {
		return err
	}
	if !ok {
		cfg = db.formatConfigFromRuntime()
	}
	cfg.MetaBody = formatMetaBodyCommandWALV1
	if err := SaveFormatConfig(db.dir, cfg); err != nil {
		return err
	}
	db.commandWALMetaV1 = true
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
		cursor = r.Last + 1
	}
	return fmt.Errorf("%w: gap before %d", ErrCommandWALAppliedLSNNonContig, cursor)
}

type commandWALSegmentCleanupDecision struct {
	Path    string `json:"path"`
	MaxLSN  uint64 `json:"max_lsn,omitempty"`
	Active  bool   `json:"active,omitempty"`
	Covered bool   `json:"covered,omitempty"`
	Removed bool   `json:"removed,omitempty"`
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
	for _, seg := range segments {
		if seg.valueLog || seg.size == 0 {
			continue
		}
		maxLSN, typed, err := commandWALSegmentMaxLSN(seg.path, maxSegmentBytes)
		if err != nil {
			return false, err
		}
		if typed && maxLSN > appliedLSN {
			return true, nil
		}
	}
	return false, nil
}

func commandWALSegmentMaxLSN(path string, maxSegmentBytes int64) (maxLSN uint64, typed bool, err error) {
	// PR2 has no durable per-segment max-LSN catalog yet, so open/cleanup paths
	// derive classification by streaming the segment without retaining payloads.
	// A later cleanup manifest can cache this once command replay lands.
	r, err := commitlog.NewReaderWithOptions(path, commitlog.Options{MaxSegmentSize: maxSegmentBytes})
	if err != nil {
		return 0, false, err
	}
	defer r.Close()

	var lastLSN uint64
	for {
		frame, err := r.ReadCommandFrame()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, commitlog.ErrCommandWALTerminalTail) {
				return maxLSN, typed, nil
			}
			if errors.Is(err, commitlog.ErrCommandWALLegacyPayload) && !typed {
				return 0, false, nil
			}
			return 0, typed, err
		}
		if lastLSN != 0 && frame.LSN <= lastLSN {
			return 0, true, commitlog.ErrCommandWALDuplicateLSN
		}
		lastLSN = frame.LSN
		typed = true
		if frame.LSN > maxLSN {
			maxLSN = frame.LSN
		}
	}
}

func filterCommandWALSegmentsForLegacyReplay(segments []logSegment, appliedLSN uint64, maxSegmentBytes int64) ([]logSegment, error) {
	var filtered []logSegment
	skipped := false
	for i, seg := range segments {
		if seg.valueLog || seg.size == 0 {
			if skipped {
				filtered = append(filtered, seg)
			}
			continue
		}
		maxLSN, typed, err := commandWALSegmentMaxLSN(seg.path, maxSegmentBytes)
		if err != nil {
			return nil, err
		}
		if !typed {
			if skipped {
				filtered = append(filtered, seg)
			}
			continue
		}
		if maxLSN <= appliedLSN {
			if !skipped {
				filtered = make([]logSegment, 0, len(segments)-1)
				filtered = append(filtered, segments[:i]...)
			}
			skipped = true
			continue
		}
		return nil, fmt.Errorf("%w: command WAL frame LSN %d exceeds applied LSN %d", ErrRecoveryRequired, maxLSN, appliedLSN)
	}
	if !skipped {
		return segments, nil
	}
	return filtered, nil
}

func cleanupCommandWALSegmentsCoveredByAppliedLSN(dir string, appliedLSN uint64, maxSegmentBytes int64) ([]commandWALSegmentCleanupDecision, error) {
	segments, err := listWALSegments(dir)
	if err != nil {
		return nil, err
	}
	activeByLane := make(map[int]uint64)
	for _, seg := range segments {
		if seg.valueLog {
			continue
		}
		if seg.seq > activeByLane[seg.lane] {
			activeByLane[seg.lane] = seg.seq
		}
	}
	decisions := make([]commandWALSegmentCleanupDecision, 0, len(segments))
	for _, seg := range segments {
		if seg.valueLog || seg.size == 0 {
			continue
		}
		maxLSN, typed, err := commandWALSegmentMaxLSN(seg.path, maxSegmentBytes)
		if err != nil {
			return decisions, err
		}
		if !typed {
			continue
		}
		decision := commandWALSegmentCleanupDecision{
			Path:    seg.path,
			MaxLSN:  maxLSN,
			Active:  seg.seq == activeByLane[seg.lane],
			Covered: maxLSN <= appliedLSN,
		}
		if decision.Covered && !decision.Active {
			if err := os.Remove(seg.path); err != nil && !os.IsNotExist(err) {
				return decisions, err
			}
			decision.Removed = true
		}
		decisions = append(decisions, decision)
	}
	return decisions, nil
}

type commandWALBackupManifest struct {
	AppliedCommandLSN uint64                     `json:"applied_command_lsn"`
	WALRanges         []commandWALBackupWALRange `json:"wal_ranges,omitempty"`
	CleanedWALRanges  []commandWALBackupWALRange `json:"cleaned_wal_ranges,omitempty"`
}

// commandWALBackupWALRange is PR2 scaffolding for the PR3/backup integration
// manifest; PR2 keeps the JSON shape tested so later code does not drift from
// the documented applied-LSN and WAL-range contract.
type commandWALBackupWALRange struct {
	Lane     int    `json:"lane"`
	Segment  uint64 `json:"segment"`
	FirstLSN uint64 `json:"first_lsn,omitempty"`
	LastLSN  uint64 `json:"last_lsn,omitempty"`
	Path     string `json:"path,omitempty"`
	Bytes    int64  `json:"bytes,omitempty"`
}
