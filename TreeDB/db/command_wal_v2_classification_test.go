package db

import (
	"errors"
	"reflect"
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

func TestClassifyCommandWALV2DurableFrontierIncludesAppliedLSN(t *testing.T) {
	tests := []struct {
		name   string
		frames []commandWALV2PhysicalFrame
	}{
		{name: "empty"},
		{
			name: "relaxed-unapplied-frame",
			frames: []commandWALV2PhysicalFrame{
				v2ClassificationFrame(8, commitlog.CommandDurabilityRelaxed, "commit-l0-000001.log", 0, 100, nil),
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := classifyCommandWALV2Frames(tc.frames, 7, func(uint64) bool { return true })
			if err != nil {
				t.Fatal(err)
			}
			if result.DurableFrontier != 7 || result.Diagnostic.DurableFrontier != 7 {
				t.Fatalf("classification=%+v, want durable frontier and diagnostic frontier 7", result)
			}
		})
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

func TestClassifyCommandWALV2RejectsDurableFrameOwnMissingRID(t *testing.T) {
	frames := []commandWALV2PhysicalFrame{
		v2ClassificationFrame(1, commitlog.CommandDurabilityDurable, "commit-l0-000001.log", 0, 100, []uint64{41}),
	}
	wantFrames := append([]commandWALV2PhysicalFrame(nil), frames...)
	_, err := classifyCommandWALV2Frames(frames, 0, func(uint64) bool { return false })
	if !errors.Is(err, ErrCommandWALMissingValueLogRID) {
		t.Fatalf("classification error=%v, want ErrCommandWALMissingValueLogRID", err)
	}
	if !reflect.DeepEqual(frames, wantFrames) {
		t.Fatalf("classification mutated input: got=%+v want=%+v", frames, wantFrames)
	}
}

func TestClassifyCommandWALV2GapAndDuplicateAtOrBelowDurableFrontierAreCorruption(t *testing.T) {
	tests := map[string][]commandWALV2PhysicalFrame{
		"gap": {
			v2ClassificationFrame(1, commitlog.CommandDurabilityDurable, "commit-l0-000001.log", 0, 100, nil),
			v2ClassificationFrame(3, commitlog.CommandDurabilityDurable, "commit-l1-000001.log", 0, 100, nil),
		},
		"duplicate": {
			v2ClassificationFrame(1, commitlog.CommandDurabilityDurable, "commit-l0-000001.log", 0, 100, nil),
			v2ClassificationFrame(2, commitlog.CommandDurabilityRelaxed, "commit-l0-000001.log", 100, 200, nil),
			v2ClassificationFrame(2, commitlog.CommandDurabilityDurable, "commit-l1-000001.log", 0, 100, nil),
		},
	}
	for name, frames := range tests {
		t.Run(name, func(t *testing.T) {
			wantFrames := append([]commandWALV2PhysicalFrame(nil), frames...)
			_, err := classifyCommandWALV2Frames(frames, 0, func(uint64) bool { return true })
			if !errors.Is(err, ErrCommandWALAppliedLSNNonContig) {
				t.Fatalf("classification error=%v, want ErrCommandWALAppliedLSNNonContig", err)
			}
			if !reflect.DeepEqual(frames, wantFrames) {
				t.Fatalf("classification mutated input: got=%+v want=%+v", frames, wantFrames)
			}
		})
	}
}

func TestClassifyCommandWALV2GapAndDuplicateAboveDurableFrontierStartDiscardSuffix(t *testing.T) {
	tests := []struct {
		name              string
		frames            []commandWALV2PhysicalFrame
		wantPrefix        int
		wantSuffix        int
		wantFirstDiscard  uint64
		wantDiscardedByte uint64
	}{
		{
			name: "gap",
			frames: []commandWALV2PhysicalFrame{
				v2ClassificationFrame(1, commitlog.CommandDurabilityDurable, "commit-l0-000001.log", 0, 100, nil),
				v2ClassificationFrame(3, commitlog.CommandDurabilityRelaxed, "commit-l1-000001.log", 0, 120, nil),
			},
			wantPrefix: 1, wantSuffix: 1, wantFirstDiscard: 3, wantDiscardedByte: 120,
		},
		{
			name: "duplicate",
			frames: []commandWALV2PhysicalFrame{
				v2ClassificationFrame(1, commitlog.CommandDurabilityDurable, "commit-l0-000001.log", 0, 100, nil),
				v2ClassificationFrame(2, commitlog.CommandDurabilityRelaxed, "commit-l0-000001.log", 100, 200, nil),
				v2ClassificationFrame(2, commitlog.CommandDurabilityRelaxed, "commit-l1-000001.log", 0, 120, nil),
			},
			wantPrefix: 2, wantSuffix: 1, wantFirstDiscard: 2, wantDiscardedByte: 120,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wantFrames := append([]commandWALV2PhysicalFrame(nil), tc.frames...)
			result, err := classifyCommandWALV2Frames(tc.frames, 0, func(uint64) bool { return true })
			if err != nil {
				t.Fatal(err)
			}
			if len(result.CompletePrefix) != tc.wantPrefix || len(result.DiscardSuffix) != tc.wantSuffix || result.Diagnostic.FirstDiscardedLSN != tc.wantFirstDiscard || result.Diagnostic.DiscardedBytes != tc.wantDiscardedByte {
				t.Fatalf("classification=%+v", result)
			}
			if !reflect.DeepEqual(tc.frames, wantFrames) {
				t.Fatalf("classification mutated input: got=%+v want=%+v", tc.frames, wantFrames)
			}
		})
	}
}

func TestClassifyCommandWALV2DiscardDiagnosticCountsUniqueMissingRIDsAcrossSuffix(t *testing.T) {
	frames := []commandWALV2PhysicalFrame{
		v2ClassificationFrame(1, commitlog.CommandDurabilityDurable, "commit-l0-000001.log", 0, 100, nil),
		v2ClassificationFrame(3, commitlog.CommandDurabilityRelaxed, "commit-l1-000001.log", 0, 120, []uint64{41, 41}),
		v2ClassificationFrame(4, commitlog.CommandDurabilityRelaxed, "commit-l0-000001.log", 100, 220, []uint64{41, 42}),
	}
	result, err := classifyCommandWALV2Frames(frames, 0, func(uint64) bool { return false })
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Diagnostic; got.FirstDiscardedLSN != 3 || got.MissingRIDCount != 2 || got.SourceSegment != "commit-l1-000001.log" {
		t.Fatalf("discard diagnostic=%+v, want actual first frame LSN/source and two unique missing RIDs", got)
	}
}

func TestClassifyCommandWALV2LaterDurableRawFrameRaisesFrontier(t *testing.T) {
	frames := []commandWALV2PhysicalFrame{
		v2ClassificationFrame(1, commitlog.CommandDurabilityDurable, "commit-l0-000001.log", 0, 100, nil),
		v2ClassificationFrame(2, commitlog.CommandDurabilityRelaxed, "commit-l1-000001.log", 0, 120, []uint64{41}),
		v2ClassificationFrame(3, commitlog.CommandDurabilityDurable, "commit-l0-000001.log", 100, 200, nil),
	}
	wantFrames := append([]commandWALV2PhysicalFrame(nil), frames...)
	_, err := classifyCommandWALV2Frames(frames, 0, func(uint64) bool { return false })
	if !errors.Is(err, ErrCommandWALMissingValueLogRID) {
		t.Fatalf("classification error=%v, want ErrCommandWALMissingValueLogRID", err)
	}
	if !reflect.DeepEqual(frames, wantFrames) {
		t.Fatalf("classification mutated input: got=%+v want=%+v", frames, wantFrames)
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
