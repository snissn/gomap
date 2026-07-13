package db

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

// commandWALV2Coordinate identifies the exact physical bytes occupied by a
// decoded command frame. It is deliberately independent of production V1
// replay and becomes active only with the #3718 format cutover.
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
	DirectorySyncCompleted bool
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
	var result commandWALV2Classification
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
		if env.Kind == commitlog.CommandKindDurablePrefixBarrier && (env.DurabilityClass != commitlog.CommandDurabilityDurable || len(ordered[i].RequiredRIDs) != 0) {
			return result, commitlog.ErrCorrupt
		}
		if env.DurabilityClass == commitlog.CommandDurabilityDurable && env.LSN > result.DurableFrontier {
			result.DurableFrontier = env.LSN
		}
	}
	result.Diagnostic.DurableFrontier = result.DurableFrontier

	expected := applied + 1
	discardAt := -1
	missingAtDiscard := 0
	firstDiscardLSN := uint64(0)
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
		if duplicate || !contiguous {
			if atOrBelowFrontier {
				return result, fmt.Errorf("%w: current=%d next=%d durable_frontier=%d", ErrCommandWALAppliedLSNNonContig, expected-1, lsn, result.DurableFrontier)
			}
			if discardAt < 0 {
				discardAt = i
				firstDiscardLSN = expected
			}
		}
		if len(missing) != 0 {
			if lsn <= result.DurableFrontier || frame.Envelope.DurabilityClass == commitlog.CommandDurabilityDurable {
				return result, fmt.Errorf("%w: lsn=%d rid=%d durable_frontier=%d", ErrCommandWALMissingValueLogRID, lsn, missing[0], result.DurableFrontier)
			}
			if discardAt < 0 {
				discardAt = i
				firstDiscardLSN = lsn
				missingAtDiscard = len(missing)
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
	result.Diagnostic.FirstDiscardedLSN = firstDiscardLSN
	result.Diagnostic.DiscardedFrameCount = uint64(len(result.DiscardSuffix))
	result.Diagnostic.MissingRIDCount = uint64(missingAtDiscard)
	result.Diagnostic.SourceSegment = filepath.Base(first.Coordinate.SourceSegment)
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
	if err := commitlog.ScanRawKVBatchPayload(env.Payload, func(op commitlog.RawKVOp, _ []byte, value []byte) error {
		if op == commitlog.RawKVOpSetRID {
			set[binary.LittleEndian.Uint64(value)] = struct{}{}
		}
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
