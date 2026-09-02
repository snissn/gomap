package db

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// commandWALV2Coordinate identifies the exact physical bytes occupied by a
// decoded command frame. It is the physical coordinate used by the active V2
// recovery and relaxed-tail repair path.
type commandWALV2Coordinate struct {
	Lane            int
	SegmentSequence uint64
	StartOffset     int64
	EndOffset       int64
	SourceSegment   string
}

type commandWALV2PhysicalFrame struct {
	Envelope     commitlog.CommandEnvelope
	Coordinate   commandWALV2Coordinate
	RequiredRIDs []uint64
	Incomplete   bool
}

// CommandWALV2RecoveryDiagnostic is stable structured evidence for recovery
// decisions and physical suffix repair.
type CommandWALV2RecoveryDiagnostic struct {
	DurableFrontier        uint64
	FirstDiscardedLSN      uint64
	DiscardedFrameCount    uint64
	DiscardedBytes         uint64
	MissingRIDCount        uint64
	SourceSegment          string
	RepairStages           []string
	CompletedRepairStages  uint64
	DirectorySyncCompleted bool
}

// readCommandWALV2PhysicalFrames decodes every complete unapplied V2 frame
// across lanes and retains exact record boundaries for deterministic repair.
// A terminal partial record is tolerated only in the active segment for its
// lane; any resulting LSN hole is decided later against the durable horizon.
func readCommandWALV2PhysicalFrames(segments []logSegment, appliedLSN uint64, maxSegmentBytes int64) ([]commandWALV2PhysicalFrame, error) {
	activeByLane := commandWALActiveSeqByLane(segments)
	frames := make([]commandWALV2PhysicalFrame, 0, 64)
	for _, segment := range segments {
		if segment.valueLog || segment.size == 0 {
			continue
		}
		if !isCommandWALLaneSegment(segment) {
			return nil, commitlog.ErrCommandWALLegacyPayload
		}
		reader, err := commitlog.NewReaderWithOptions(segment.path, commitlog.Options{MaxSegmentSize: maxSegmentBytes})
		if err != nil {
			return nil, err
		}
		var previousLSN uint64
		havePreviousLSN := false
		acceptPhysicalLSN := func(lsn uint64) error {
			if havePreviousLSN && lsn <= previousLSN {
				return fmt.Errorf("%w: segment=%s previous=%d current=%d", commitlog.ErrCommandWALDuplicateLSN, filepath.Base(segment.path), previousLSN, lsn)
			}
			previousLSN = lsn
			havePreviousLSN = true
			return nil
		}
		for {
			start, offsetErr := reader.Offset()
			if offsetErr != nil {
				_ = reader.Close()
				return nil, offsetErr
			}
			env, readErr := reader.ReadCommandFrameV2()
			if readErr == nil {
				if lsnErr := acceptPhysicalLSN(env.LSN); lsnErr != nil {
					_ = reader.Close()
					return nil, lsnErr
				}
				end, offsetErr := reader.Offset()
				if offsetErr != nil {
					_ = reader.Close()
					return nil, offsetErr
				}
				if env.LSN > appliedLSN {
					frame, frameErr := commandWALV2FrameFromEnvelope(env, commandWALV2Coordinate{
						Lane:            segment.lane,
						SegmentSequence: segment.seq,
						StartOffset:     start,
						EndOffset:       end,
						SourceSegment:   segment.path,
					})
					if frameErr != nil {
						_ = reader.Close()
						return nil, frameErr
					}
					frames = append(frames, frame)
				}
				continue
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
			if errors.Is(readErr, commitlog.ErrCommandWALTerminalTail) && segment.seq == activeByLane[segment.lane] {
				tailEnv, end, inspectErr := commitlog.InspectCommandFrameV2TerminalTail(segment.path, start)
				if inspectErr != nil {
					_ = reader.Close()
					return nil, inspectErr
				}
				if lsnErr := acceptPhysicalLSN(tailEnv.LSN); lsnErr != nil {
					_ = reader.Close()
					return nil, lsnErr
				}
				if tailEnv.LSN > appliedLSN {
					frames = append(frames, commandWALV2PhysicalFrame{
						Envelope: tailEnv,
						Coordinate: commandWALV2Coordinate{
							Lane:            segment.lane,
							SegmentSequence: segment.seq,
							StartOffset:     start,
							EndOffset:       end,
							SourceSegment:   segment.path,
						},
						Incomplete: true,
					})
				}
				break
			}
			_ = reader.Close()
			return nil, readErr
		}
		if err := reader.Close(); err != nil {
			return nil, err
		}
	}
	sort.SliceStable(frames, func(i, j int) bool {
		return frames[i].Envelope.LSN < frames[j].Envelope.LSN
	})
	return frames, nil
}

type commandWALV2Classification struct {
	DurableFrontier uint64
	CompletePrefix  []commandWALV2PhysicalFrame
	DiscardSuffix   []commandWALV2PhysicalFrame
	Diagnostic      CommandWALV2RecoveryDiagnostic
}

// CommandWALV2RecoveryRequiredError reports the exact read-only recovery plan
// without changing storage.
type CommandWALV2RecoveryRequiredError struct {
	Diagnostic CommandWALV2RecoveryDiagnostic
}

func (e *CommandWALV2RecoveryRequiredError) Error() string {
	if e == nil {
		return ErrRecoveryRequired.Error()
	}
	return fmt.Sprintf("%v: durable_frontier=%d first_discarded_lsn=%d frames=%d bytes=%d missing_rids=%d segment=%s directory_sync=%t",
		ErrRecoveryRequired,
		e.Diagnostic.DurableFrontier,
		e.Diagnostic.FirstDiscardedLSN,
		e.Diagnostic.DiscardedFrameCount,
		e.Diagnostic.DiscardedBytes,
		e.Diagnostic.MissingRIDCount,
		e.Diagnostic.SourceSegment,
		e.Diagnostic.DirectorySyncCompleted,
	)
}

func (e *CommandWALV2RecoveryRequiredError) Unwrap() error { return ErrRecoveryRequired }

// classifyCommandWALV2Frames is a pure recovery decision. Its input must have
// passed V2 envelope/fence decoding; dependency existence is deliberately
// supplied separately so all frames through the durable horizon can be
// validated before any replay handler mutates the DB.
func classifyCommandWALV2Frames(frames []commandWALV2PhysicalFrame, applied uint64, hasRID func(uint64) bool) (commandWALV2Classification, error) {
	result := commandWALV2Classification{DurableFrontier: applied}
	result.Diagnostic.DurableFrontier = applied
	if len(frames) == 0 {
		return result, nil
	}
	ordered := append([]commandWALV2PhysicalFrame(nil), frames...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Envelope.LSN != ordered[j].Envelope.LSN {
			return ordered[i].Envelope.LSN < ordered[j].Envelope.LSN
		}
		if ordered[i].Coordinate.Lane != ordered[j].Coordinate.Lane {
			return ordered[i].Coordinate.Lane < ordered[j].Coordinate.Lane
		}
		if ordered[i].Coordinate.SegmentSequence != ordered[j].Coordinate.SegmentSequence {
			return ordered[i].Coordinate.SegmentSequence < ordered[j].Coordinate.SegmentSequence
		}
		return ordered[i].Coordinate.StartOffset < ordered[j].Coordinate.StartOffset
	})
	for i := range ordered {
		env := ordered[i].Envelope
		if env.Version != commitlog.CommandFrameVersionV2 || (env.DurabilityClass != commitlog.CommandDurabilityDurable && env.DurabilityClass != commitlog.CommandDurabilityRelaxed) {
			return result, commitlog.ErrCorrupt
		}
		if !ordered[i].Incomplete && env.Kind == commitlog.CommandKindDurablePrefixBarrier && (env.DurabilityClass != commitlog.CommandDurabilityDurable || len(ordered[i].RequiredRIDs) != 0) {
			return result, commitlog.ErrCorrupt
		}
		if !ordered[i].Incomplete && env.DurabilityClass == commitlog.CommandDurabilityDurable && env.LSN > result.DurableFrontier {
			result.DurableFrontier = env.LSN
		}
	}
	result.Diagnostic.DurableFrontier = result.DurableFrontier

	expected := applied + 1
	discardAt := -1
	for i := range ordered {
		frame := &ordered[i]
		lsn := frame.Envelope.LSN
		if lsn <= applied {
			continue
		}
		contiguous := lsn == expected
		duplicate := i > 0 && ordered[i-1].Envelope.LSN == lsn
		missing := commandWALV2MissingRIDs(frame.RequiredRIDs, hasRID)
		atOrBelowFrontier := lsn <= result.DurableFrontier || expected <= result.DurableFrontier
		if frame.Incomplete {
			if atOrBelowFrontier || frame.Envelope.DurabilityClass == commitlog.CommandDurabilityDurable {
				return result, fmt.Errorf("%w: incomplete command frame lsn=%d durable_frontier=%d", commitlog.ErrCorrupt, lsn, result.DurableFrontier)
			}
			if discardAt < 0 {
				discardAt = i
			}
		}
		if duplicate || !contiguous {
			if atOrBelowFrontier {
				return result, fmt.Errorf("%w: current=%d next=%d durable_frontier=%d", ErrCommandWALAppliedLSNNonContig, expected-1, lsn, result.DurableFrontier)
			}
			if discardAt < 0 {
				discardAt = i
			}
		}
		if len(missing) != 0 {
			if lsn <= result.DurableFrontier || frame.Envelope.DurabilityClass == commitlog.CommandDurabilityDurable {
				return result, fmt.Errorf("%w: lsn=%d rid=%d durable_frontier=%d", ErrCommandWALMissingValueLogRID, lsn, missing[0], result.DurableFrontier)
			}
			if discardAt < 0 {
				discardAt = i
			}
		}
		if discardAt < 0 {
			expected = lsn + 1
		}
	}

	if discardAt < 0 {
		result.CompletePrefix = ordered
		return result, nil
	}
	result.CompletePrefix = ordered[:discardAt]
	result.DiscardSuffix = ordered[discardAt:]
	first := result.DiscardSuffix[0]
	result.Diagnostic.FirstDiscardedLSN = first.Envelope.LSN
	result.Diagnostic.DiscardedFrameCount = uint64(len(result.DiscardSuffix))
	result.Diagnostic.SourceSegment = filepath.Base(first.Coordinate.SourceSegment)
	var missingInSuffix map[uint64]struct{}
	for i := range result.DiscardSuffix {
		for _, rid := range commandWALV2MissingRIDs(result.DiscardSuffix[i].RequiredRIDs, hasRID) {
			if missingInSuffix == nil {
				missingInSuffix = make(map[uint64]struct{})
			}
			missingInSuffix[rid] = struct{}{}
		}
	}
	result.Diagnostic.MissingRIDCount = uint64(len(missingInSuffix))
	for i := range result.DiscardSuffix {
		coordinate := result.DiscardSuffix[i].Coordinate
		if coordinate.EndOffset > coordinate.StartOffset {
			result.Diagnostic.DiscardedBytes += uint64(coordinate.EndOffset - coordinate.StartOffset)
		}
	}
	return result, nil
}

func commandWALV2MissingRIDs(required []uint64, hasRID func(uint64) bool) []uint64 {
	missingSet := make(map[uint64]struct{})
	for _, rid := range required {
		if hasRID == nil || !hasRID(rid) {
			missingSet[rid] = struct{}{}
		}
	}
	missing := make([]uint64, 0, len(missingSet))
	for rid := range missingSet {
		missing = append(missing, rid)
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
	return missing
}

// commandWALV2FrameFromEnvelope recomputes the canonical RID set only after
// DecodeCommandFrameV2 has validated the payload fence.
func commandWALV2FrameFromEnvelope(env commitlog.CommandEnvelope, coordinate commandWALV2Coordinate) (commandWALV2PhysicalFrame, error) {
	frame := commandWALV2PhysicalFrame{Envelope: env, Coordinate: coordinate}
	if env.Kind != commitlog.CommandKindRawKVBatch {
		return frame, nil
	}
	if _, _, err := commitlog.RawKVExternalRefFenceV1(env); err != nil {
		return commandWALV2PhysicalFrame{}, err
	}
	set := make(map[uint64]struct{})
	if err := commitlog.ScanRawKVBatchRIDs(env.Payload, func(rid uint64) error {
		set[rid] = struct{}{}
		return nil
	}); err != nil {
		return commandWALV2PhysicalFrame{}, err
	}
	frame.RequiredRIDs = make([]uint64, 0, len(set))
	for rid := range set {
		frame.RequiredRIDs = append(frame.RequiredRIDs, rid)
	}
	sort.Slice(frame.RequiredRIDs, func(i, j int) bool { return frame.RequiredRIDs[i] < frame.RequiredRIDs[j] })
	return frame, nil
}

// repairCommandWALV2Suffix durably removes exactly the relaxed suffix selected
// by the active V2 recovery classifier. Read-only opens report the same repair
// plan without mutating storage.
func repairCommandWALV2Suffix(walDir string, classification commandWALV2Classification, readOnly bool) (CommandWALV2RecoveryDiagnostic, error) {
	diagnostic := classification.Diagnostic
	if len(classification.DiscardSuffix) == 0 {
		return diagnostic, nil
	}
	emptySegments, err := commandWALV2EmptyRepairSegments(walDir, classification)
	if err != nil {
		return diagnostic, errors.Join(err, ErrRecoveryRequired)
	}
	stages, removable := planCommandWALV2RepairStagesWithEmptySegments(classification, emptySegments)
	diagnostic.RepairStages = stages
	if readOnly {
		return diagnostic, &CommandWALV2RecoveryRequiredError{Diagnostic: diagnostic}
	}

	anchor := classification.DiscardSuffix[0]
	completeByPath := make(map[string]bool)
	for i := range classification.CompletePrefix {
		completeByPath[classification.CompletePrefix[i].Coordinate.SourceSegment] = true
	}
	retainedFloorByPath := commandWALV2RetainedRepairFloors(classification, completeByPath)

	completeStage := func() { diagnostic.CompletedRepairStages++ }
	for i := len(classification.DiscardSuffix) - 1; i > 0; i-- {
		frame := classification.DiscardSuffix[i]
		if frame.Coordinate.SourceSegment == anchor.Coordinate.SourceSegment {
			continue
		}
		truncatePlan := commandWALV2TruncatePlan{pathMode: commandWALV2TruncateDisposable}
		if completeByPath[frame.Coordinate.SourceSegment] {
			retainedFloor, hasRetainedFloor := retainedFloorByPath[frame.Coordinate.SourceSegment]
			if !hasRetainedFloor {
				return diagnostic, errors.Join(
					fmt.Errorf("%w: retained repair path %s has no classified floor", commitlog.ErrCorrupt, filepath.Base(frame.Coordinate.SourceSegment)),
					ErrRecoveryRequired,
				)
			}
			truncatePlan = commandWALV2TruncatePlan{
				pathMode:      commandWALV2TruncateRetained,
				retainedFloor: retainedFloor,
			}
		}
		if err := truncateAndSyncCommandWALV2(walDir, frame.Coordinate.SourceSegment, frame.Coordinate.StartOffset, truncatePlan); err != nil {
			return diagnostic, err
		}
		completeStage()
	}
	for _, path := range removable {
		if completeByPath[path] || path == anchor.Coordinate.SourceSegment {
			continue
		}
		if err := durabilitycut.EmitPath(durabilitycut.BeforeWALOrAssetUnlink, durabilitycut.ResourceCommandWAL, walDir, path); err != nil {
			return diagnostic, errors.Join(err, ErrRecoveryRequired)
		}
		if _, err := removePersistentFile(walDir, path, durabilitycut.ResourceCommandWAL); err != nil {
			return diagnostic, errors.Join(err, ErrRecoveryRequired)
		}
		if err := durabilitycut.EmitPath(durabilitycut.AfterWALOrAssetUnlink, durabilitycut.ResourceCommandWAL, walDir, path); err != nil {
			return diagnostic, errors.Join(err, ErrRecoveryRequired)
		}
		completeStage()
	}
	if err := syncDeletionNamespaceDirectory(walDir, durabilitycut.ResourceCommandWAL); err != nil {
		return diagnostic, err
	}
	diagnostic.DirectorySyncCompleted = true
	completeStage()
	if err := truncateAndSyncCommandWALV2(walDir, anchor.Coordinate.SourceSegment, anchor.Coordinate.StartOffset, commandWALV2TruncatePlan{
		pathMode:      commandWALV2TruncateRetained,
		retainedFloor: anchor.Coordinate.StartOffset,
	}); err != nil {
		return diagnostic, err
	}
	completeStage()
	return diagnostic, nil
}

// commandWALV2RetainedRepairFloors derives the lowest safe size for every
// mixed retained path from the immutable classification. A retry may observe
// a size below an earlier, larger planned cut only when it is still at or
// above this floor; the floor stage itself remains in the plan and will sync
// the final retained prefix. Sizes below the floor have lost prefix bytes and
// must fail closed.
func commandWALV2RetainedRepairFloors(classification commandWALV2Classification, completeByPath map[string]bool) map[string]int64 {
	floors := make(map[string]int64)
	for i := range classification.DiscardSuffix {
		coordinate := classification.DiscardSuffix[i].Coordinate
		if !completeByPath[coordinate.SourceSegment] {
			continue
		}
		floor, exists := floors[coordinate.SourceSegment]
		if !exists || coordinate.StartOffset < floor {
			floors[coordinate.SourceSegment] = coordinate.StartOffset
		}
	}
	return floors
}

func planCommandWALV2RepairStages(classification commandWALV2Classification) ([]string, []string) {
	return planCommandWALV2RepairStagesWithEmptySegments(classification, nil)
}

type commandWALV2RemovableSegment struct {
	path                string
	highestDiscardedLSN uint64
	coordinate          commandWALV2Coordinate
}

func planCommandWALV2RepairStagesWithEmptySegments(classification commandWALV2Classification, emptySegments []logSegment) ([]string, []string) {
	if len(classification.DiscardSuffix) == 0 {
		return nil, nil
	}
	stages := make([]string, 0, len(classification.DiscardSuffix)+2)
	removableByPath := make(map[string]commandWALV2RemovableSegment)
	completeByPath := make(map[string]bool)
	for i := range classification.CompletePrefix {
		completeByPath[classification.CompletePrefix[i].Coordinate.SourceSegment] = true
	}
	anchorPath := classification.DiscardSuffix[0].Coordinate.SourceSegment
	for i := len(classification.DiscardSuffix) - 1; i > 0; i-- {
		frame := classification.DiscardSuffix[i]
		coordinate := frame.Coordinate
		path := coordinate.SourceSegment
		if path == anchorPath {
			continue
		}
		stages = append(stages, fmt.Sprintf("truncate-sync:%s@%d", filepath.Base(path), coordinate.StartOffset))
		if !completeByPath[path] {
			candidate, exists := removableByPath[path]
			if !exists || frame.Envelope.LSN > candidate.highestDiscardedLSN ||
				(frame.Envelope.LSN == candidate.highestDiscardedLSN && commandWALV2CoordinateLater(coordinate, candidate.coordinate)) {
				removableByPath[path] = commandWALV2RemovableSegment{path: path, highestDiscardedLSN: frame.Envelope.LSN, coordinate: coordinate}
			}
		}
	}
	for _, segment := range emptySegments {
		if completeByPath[segment.path] || segment.path == anchorPath {
			continue
		}
		if _, exists := removableByPath[segment.path]; !exists {
			removableByPath[segment.path] = commandWALV2RemovableSegment{
				path: segment.path,
				coordinate: commandWALV2Coordinate{
					Lane:            segment.lane,
					SegmentSequence: segment.seq,
					SourceSegment:   segment.path,
				},
			}
		}
	}
	candidates := make([]commandWALV2RemovableSegment, 0, len(removableByPath))
	for _, candidate := range removableByPath {
		candidates = append(candidates, candidate)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].highestDiscardedLSN != candidates[j].highestDiscardedLSN {
			return candidates[i].highestDiscardedLSN > candidates[j].highestDiscardedLSN
		}
		if commandWALV2CoordinateLater(candidates[i].coordinate, candidates[j].coordinate) {
			return true
		}
		if commandWALV2CoordinateLater(candidates[j].coordinate, candidates[i].coordinate) {
			return false
		}
		return candidates[i].path > candidates[j].path
	})
	removable := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		path := candidate.path
		removable = append(removable, path)
		stages = append(stages, "unlink:"+filepath.Base(path))
	}
	stages = append(stages, "directory-sync")
	anchor := classification.DiscardSuffix[0].Coordinate
	stages = append(stages, fmt.Sprintf("anchor-truncate-sync:%s@%d", filepath.Base(anchor.SourceSegment), anchor.StartOffset))
	return stages, removable
}

func commandWALV2CoordinateLater(a, b commandWALV2Coordinate) bool {
	if a.Lane != b.Lane {
		return a.Lane > b.Lane
	}
	if a.SegmentSequence != b.SegmentSequence {
		return a.SegmentSequence > b.SegmentSequence
	}
	if a.StartOffset != b.StartOffset {
		return a.StartOffset > b.StartOffset
	}
	if a.EndOffset != b.EndOffset {
		return a.EndOffset > b.EndOffset
	}
	return a.SourceSegment > b.SourceSegment
}

// commandWALV2EmptyRepairSegments recovers the identity of suffix-only files
// whose bytes may have reached stable storage before their unlink. A fresh V2
// frame scan necessarily skips these zero-byte files, so the retained anchor
// remains the logical repair marker while the canonical segment names remain
// deterministic namespace-cleanup candidates.
func commandWALV2EmptyRepairSegments(walDir string, classification commandWALV2Classification) ([]logSegment, error) {
	knownPaths := make(map[string]struct{}, len(classification.CompletePrefix)+len(classification.DiscardSuffix))
	for i := range classification.CompletePrefix {
		knownPaths[classification.CompletePrefix[i].Coordinate.SourceSegment] = struct{}{}
	}
	for i := range classification.DiscardSuffix {
		knownPaths[classification.DiscardSuffix[i].Coordinate.SourceSegment] = struct{}{}
	}
	segments, err := listSegmentsInDir(walDir)
	if err != nil {
		return nil, err
	}
	empty := make([]logSegment, 0)
	for _, segment := range segments {
		if segment.valueLog || segment.size != 0 || !commitlog.IsCommandSegmentName(filepath.Base(segment.path)) {
			continue
		}
		if _, known := knownPaths[segment.path]; known {
			continue
		}
		empty = append(empty, segment)
	}
	return empty, nil
}

type commandWALV2TruncatePathMode uint8

const (
	// Retained paths contain prefix bytes or the repair anchor. Exact-size
	// retries must re-sync; missing or undersized paths fail closed.
	commandWALV2TruncateRetained commandWALV2TruncatePathMode = iota + 1
	// Disposable paths contain suffix bytes only. Missing or already-shorter
	// retries are complete monotonic stages and must never regrow the file.
	commandWALV2TruncateDisposable
)

type commandWALV2TruncatePlan struct {
	pathMode      commandWALV2TruncatePathMode
	retainedFloor int64
}

func truncateAndSyncCommandWALV2(walDir, path string, offset int64, plan commandWALV2TruncatePlan) error {
	if plan.pathMode != commandWALV2TruncateRetained && plan.pathMode != commandWALV2TruncateDisposable {
		return errors.Join(commitlog.ErrCorrupt, ErrRecoveryRequired)
	}
	if plan.pathMode == commandWALV2TruncateRetained && (plan.retainedFloor < 0 || plan.retainedFloor > offset) {
		return errors.Join(
			fmt.Errorf("%w: retained repair path %s has invalid floor=%d for offset=%d", commitlog.ErrCorrupt, filepath.Base(path), plan.retainedFloor, offset),
			ErrRecoveryRequired,
		)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		if plan.pathMode == commandWALV2TruncateDisposable && os.IsNotExist(err) {
			return nil
		}
		if plan.pathMode == commandWALV2TruncateRetained {
			return errors.Join(
				fmt.Errorf("%w: retained repair path %s is unavailable", commitlog.ErrCorrupt, filepath.Base(path)),
				err,
				ErrRecoveryRequired,
			)
		}
		return errors.Join(err, ErrRecoveryRequired)
	}
	info, statErr := f.Stat()
	if statErr != nil {
		_ = f.Close()
		return errors.Join(statErr, ErrRecoveryRequired)
	}
	if plan.pathMode == commandWALV2TruncateRetained && info.Size() < plan.retainedFloor {
		closeErr := f.Close()
		return errors.Join(
			fmt.Errorf("%w: retained repair path %s size=%d is below floor=%d", commitlog.ErrCorrupt, filepath.Base(path), info.Size(), plan.retainedFloor),
			closeErr,
			ErrRecoveryRequired,
		)
	}
	if plan.pathMode == commandWALV2TruncateRetained && info.Size() < offset {
		if closeErr := f.Close(); closeErr != nil {
			return errors.Join(closeErr, ErrRecoveryRequired)
		}
		return nil
	}
	if plan.pathMode == commandWALV2TruncateDisposable && info.Size() <= offset {
		if closeErr := f.Close(); closeErr != nil {
			return errors.Join(closeErr, ErrRecoveryRequired)
		}
		return nil
	}
	if info.Size() > offset {
		err = f.Truncate(offset)
	}
	if err == nil {
		err = durabilitycut.EmitPath(durabilitycut.BeforeDependencyFileSync, durabilitycut.ResourceCommandWAL, walDir, path)
	}
	if err == nil {
		err = f.Sync()
	}
	if err == nil {
		err = durabilitycut.EmitPath(durabilitycut.AfterDependencyFileSync, durabilitycut.ResourceCommandWAL, walDir, path)
	}
	closeErr := f.Close()
	if err != nil {
		return errors.Join(err, ErrRecoveryRequired)
	}
	if closeErr != nil {
		return errors.Join(closeErr, ErrRecoveryRequired)
	}
	return nil
}
