package db

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

const (
	// Two readers pay off for large predecessors. Keep ordinary checkpoints
	// on the original reader and bound the additional replay map.
	minParallelWALCleanupStreamBytes       = 128 << 20
	maxParallelWALCleanupReplayLSNs        = 4096
	maxParallelWALCleanupPrimaryFrameBytes = 256 << 20
	maxParallelWALCleanupWorkerFrameBytes  = 8 << 20
)

func (s *commandWALCleanupScan) canScanClosedStreamPair(segments []logSegment, active commitlog.CommandJournalCleanupSnapshot, newest uint64) bool {
	lower, upper := s.options.seenLSNAppliedLSN, s.options.seenLSNMax
	if len(segments) < 3 || s.beforeSegment != nil || lower == 0 ||
		upper < lower || upper-lower > maxParallelWALCleanupReplayLSNs ||
		active.FixedContainer || active.SegmentSeq != newest {
		return false
	}
	if segments[0].seq >= segments[1].seq {
		return false
	}
	for _, seg := range segments[:2] {
		if seg.size < minParallelWALCleanupStreamBytes || seg.valueLog || seg.lane != active.Lane ||
			seg.seq == 0 || seg.seq >= active.SegmentSeq ||
			filepath.Base(seg.path) != commitlog.CommandSegmentName(seg.lane, seg.seq) {
			return false
		}
	}
	limit := commitlog.SegmentPayloadLimit(s.maxSegmentBytes)
	if limit == 0 || limit > maxParallelWALCleanupPrimaryFrameBytes ||
		filepath.Base(active.ActivePath) != commitlog.CommandSegmentName(active.Lane, active.SegmentSeq) ||
		runtime.GOMAXPROCS(0) < 2 {
		return false
	}
	for _, seg := range segments[2:] {
		if seg.lane == active.Lane && seg.seq == active.SegmentSeq && seg.path == active.ActivePath {
			return true
		}
	}
	return false
}

// scanClosedStreamPair overlaps two ordinary validators under the caller's
// single append exclusion. Replay the same second handle in order for records
// above the worker's scratch limit, compressed records and shared replay LSNs.
// Only the normal reader may decompress and report a cross-file duplicate.
func (s *commandWALCleanupScan) scanClosedStreamPair(first, second logSegment, active rootpublication.StableIdentity, open func(string) (*os.File, error)) bool {
	firstFile, firstInfo, err := s.openParallelWALCleanupStream(first, active, open)
	if err != nil {
		s.hardErr = errors.Join(s.hardErr, err)
		s.scanSegment(second, false, false, nil)
		return true
	}
	if firstFile == nil {
		return false
	}
	secondFile, secondInfo, err := s.openParallelWALCleanupStream(second, active, open)
	if err != nil {
		// The first file still owns its original position in the proof and
		// error order. The failed second probe is its scan's hard error.
		if !s.scanFile(first, firstFile, false, false, nil) {
			s.hardErr = errors.Join(s.hardErr, firstFile.Close())
		}
		s.hardErr = errors.Join(s.hardErr, err)
		return true
	}
	if secondFile == nil {
		s.hardErr = errors.Join(s.hardErr, firstFile.Close())
		return false
	}
	if os.SameFile(firstInfo, secondInfo) {
		s.hardErr = errors.Join(s.hardErr, firstFile.Close(), secondFile.Close())
		return false
	}

	alternate := newCommandWALCleanupScan(s.options.seenLSNAppliedLSN, s.options.seenLSNMax, s.maxSegmentBytes, nil)
	alternate.closedStreamBuffer = new(commitlog.CleanupReadBuffer)
	alternate.options.boundedCleanupPayload = maxParallelWALCleanupWorkerFrameBytes
	secondResult := make(chan bool, 1)
	go func() {
		secondResult <- alternate.scanFile(second, secondFile, false, false, nil)
	}()
	if !s.scanFile(first, firstFile, false, false, nil) {
		s.hardErr = errors.Join(s.hardErr, firstFile.Close())
	}
	keepSecond := <-secondResult
	replaySecond := errors.Is(alternate.hardErr, commitlog.ErrCommandWALCleanupReadLimit)
	for lsn := range alternate.options.seenLSNs {
		if _, duplicate := s.options.seenLSNs[lsn]; duplicate {
			replaySecond = true
			break
		}
	}
	if replaySecond {
		// A second file that needs replay cannot own a deletion decision.
		if _, err := secondFile.Seek(0, io.SeekStart); err != nil {
			s.hardErr = errors.Join(s.hardErr, fmt.Errorf("rewind command WAL segment %s: %w", filepath.Base(second.path), err), secondFile.Close())
		} else if !s.scanFile(second, secondFile, false, false, nil) {
			s.hardErr = errors.Join(s.hardErr, secondFile.Close())
		}
		return true
	}
	for lsn := range alternate.options.seenLSNs {
		s.options.seenLSNs[lsn] = struct{}{}
	}
	s.decisions = append(s.decisions, alternate.decisions...)
	s.hardErr = errors.Join(s.hardErr, alternate.hardErr)
	s.lineageErr = errors.Join(s.lineageErr, alternate.lineageErr)
	if !keepSecond {
		s.hardErr = errors.Join(s.hardErr, secondFile.Close())
	}
	return true
}

// Opening a pair is optional. Link/mount mismatch and descriptor exhaustion
// allow the original ordered scan. Actual faults on known segment handles
// still belong to the proof, including a failed close on ineligible handles.
func (s *commandWALCleanupScan) openParallelWALCleanupStream(seg logSegment, active rootpublication.StableIdentity, open func(string) (*os.File, error)) (*os.File, os.FileInfo, error) {
	link, err := os.Lstat(seg.path)
	if err != nil || !link.Mode().IsRegular() {
		return nil, nil, nil
	}
	file, err := open(seg.path)
	if err != nil {
		if commandWALCleanupDescriptorExhausted(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("open command WAL segment %s: %w", filepath.Base(seg.path), err)
	}
	info, statErr := file.Stat()
	identity, identityErr := rootpublication.StableIdentityFromFile(file)
	if err := errors.Join(identityErr, statErr); err != nil {
		return nil, nil, errors.Join(fmt.Errorf("identify command WAL segment %s: %w", filepath.Base(seg.path), err), file.Close())
	}
	fixed, formatErr := commitlog.HasFixedWALMagic(file)
	if formatErr != nil {
		return nil, nil, errors.Join(fmt.Errorf("scan command WAL segment %s: %w", filepath.Base(seg.path), formatErr), file.Close())
	}
	if !os.SameFile(link, info) || info.Size() != seg.size || fixed ||
		rootpublication.SamePhysicalIdentity(identity, active) || !commitlog.IsExt4WALFilesystem(file) {
		s.hardErr = errors.Join(s.hardErr, file.Close())
		return nil, nil, nil
	}
	return file, info, nil
}
