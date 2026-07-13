package db

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func TestClassifyCommandWALV2RelaxedSuffixAboveDurableFrontier(t *testing.T) {
	frames := []commandWALV2PhysicalFrame{
		v2ClassificationFrame(1, commitlog.CommandDurabilityDurable, "commit-l0-000001.log", 0, 100, nil),
		v2ClassificationFrame(2, commitlog.CommandDurabilityRelaxed, "commit-l1-000001.log", 0, 120, []uint64{41}),
		v2ClassificationFrame(3, commitlog.CommandDurabilityRelaxed, "commit-l0-000001.log", 100, 200, nil),
	}
	result, err := classifyCommandWALV2Frames(frames, 0, func(rid uint64) bool { return false })
	if err != nil {
		t.Fatal(err)
	}
	if result.DurableFrontier != 1 || len(result.CompletePrefix) != 1 || len(result.DiscardSuffix) != 2 {
		t.Fatalf("classification=%+v", result)
	}
	if got := result.Diagnostic; got.FirstDiscardedLSN != 2 || got.DiscardedFrameCount != 2 || got.DiscardedBytes != 220 || got.MissingRIDCount != 1 || got.SourceSegment != "commit-l1-000001.log" {
		t.Fatalf("diagnostic=%+v", got)
	}
	if got := result.DiscardSuffix[0].Coordinate; got.Lane != 1 || got.SegmentSequence != 1 || got.StartOffset != 0 || got.EndOffset != 120 {
		t.Fatalf("first discard coordinate=%+v", got)
	}
}

func TestClassifyCommandWALV2LaterDurableBarrierRaisesFrontier(t *testing.T) {
	frames := []commandWALV2PhysicalFrame{
		v2ClassificationFrame(1, commitlog.CommandDurabilityDurable, "commit-l0-000001.log", 0, 100, nil),
		v2ClassificationFrame(2, commitlog.CommandDurabilityRelaxed, "commit-l1-000001.log", 0, 120, []uint64{41}),
		{
			Envelope:   commitlog.NewDurablePrefixBarrierV1(3, 1),
			Coordinate: commandWALV2Coordinate{Lane: 0, SegmentSequence: 1, StartOffset: 100, EndOffset: 180, SourceSegment: "commit-l0-000001.log"},
		},
	}
	_, err := classifyCommandWALV2Frames(frames, 0, func(rid uint64) bool { return false })
	if !errors.Is(err, ErrCommandWALMissingValueLogRID) {
		t.Fatalf("classification error=%v, want ErrCommandWALMissingValueLogRID", err)
	}
}

func v2ClassificationFrame(lsn uint64, class commitlog.CommandDurabilityClass, segment string, start, end int64, rids []uint64) commandWALV2PhysicalFrame {
	return commandWALV2PhysicalFrame{
		Envelope: commitlog.CommandEnvelope{
			Version:         commitlog.CommandFrameVersionV2,
			DurabilityClass: class,
			LSN:             lsn,
			Kind:            commitlog.CommandKindRawKVBatch,
			Scope:           commitlog.CommandScopeRawKV,
			PayloadFormat:   commitlog.PayloadFormatRawKVBatchV1,
		},
		Coordinate:   commandWALV2Coordinate{Lane: commandWALLaneForTest(segment), SegmentSequence: 1, StartOffset: start, EndOffset: end, SourceSegment: segment},
		RequiredRIDs: append([]uint64(nil), rids...),
	}
}

func commandWALLaneForTest(segment string) int {
	if segment == "commit-l1-000001.log" {
		return 1
	}
	return 0
}
