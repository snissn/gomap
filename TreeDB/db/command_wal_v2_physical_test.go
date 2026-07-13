package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
)

func TestReadCommandWALV2PhysicalFramesAcrossLanes(t *testing.T) {
	walDir := t.TempDir()
	lane0 := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
	lane1 := filepath.Join(walDir, commitlog.CommandSegmentName(1, 1))
	writeCommandWALV2Segment(t, lane0,
		mustCommandWALV2Frame(t, 1, commitlog.CommandDurabilityDurable, nil),
		mustCommandWALV2Frame(t, 3, commitlog.CommandDurabilityRelaxed, nil),
	)
	writeCommandWALV2Segment(t, lane1,
		mustCommandWALV2Frame(t, 2, commitlog.CommandDurabilityRelaxed, []uint64{41}),
	)
	segments, err := listSegmentsInDir(walDir)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := readCommandWALV2PhysicalFrames(segments, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 3 {
		t.Fatalf("frames=%d, want 3", len(frames))
	}
	for i, wantLSN := range []uint64{1, 2, 3} {
		if frames[i].Envelope.LSN != wantLSN || frames[i].Coordinate.EndOffset <= frames[i].Coordinate.StartOffset {
			t.Fatalf("frame[%d]=%+v, want lsn=%d with physical range", i, frames[i], wantLSN)
		}
	}
	if got := frames[1].RequiredRIDs; !reflect.DeepEqual(got, []uint64{41}) {
		t.Fatalf("required RIDs=%v, want [41]", got)
	}
	result, err := classifyCommandWALV2Frames(frames, 0, func(uint64) bool { return false })
	if err != nil {
		t.Fatal(err)
	}
	if result.DurableFrontier != 1 || result.Diagnostic.FirstDiscardedLSN != 2 || len(result.DiscardSuffix) != 2 {
		t.Fatalf("classification=%+v", result)
	}
}

func TestReadCommandWALV2IncompleteRelaxedTailFailsBelowLaterBarrier(t *testing.T) {
	walDir := t.TempDir()
	lane0 := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
	lane1 := filepath.Join(walDir, commitlog.CommandSegmentName(1, 1))
	writeCommandWALV2Segment(t, lane0, mustCommandWALV2Frame(t, 1, commitlog.CommandDurabilityDurable, nil))
	partial := mustCommandWALV2Frame(t, 2, commitlog.CommandDurabilityRelaxed, nil)
	writeCommandWALV2Segment(t, lane1, partial)
	if err := os.Truncate(lane1, 8+56); err != nil {
		t.Fatal(err)
	}
	appendCommandWALV2Segment(t, lane0, mustDurablePrefixBarrierV2(t, 3, 1))
	segments, err := listSegmentsInDir(walDir)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := readCommandWALV2PhysicalFrames(segments, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 3 || !frames[1].Incomplete {
		t.Fatalf("physical frames=%+v, want incomplete lsn 2", frames)
	}
	if got := frames[1].Coordinate; got.Lane != 1 || got.SegmentSequence != 1 || got.StartOffset != 0 || got.EndOffset != 8+56 || got.SourceSegment != lane1 {
		t.Fatalf("incomplete coordinate=%+v, want exact lane-1 terminal tail", got)
	}
	if _, err := classifyCommandWALV2Frames(frames, 0, func(uint64) bool { return true }); !errors.Is(err, commitlog.ErrCorrupt) {
		t.Fatalf("classification error=%v, want corruption below barrier", err)
	}
}

func TestRepairCommandWALV2SuffixReadOnlyParityAndRetryableCuts(t *testing.T) {
	variants := commandWALV2RepairVariants(t)
	cuts := []struct {
		point      durabilitycut.Point
		occurrence int
	}{
		{durabilitycut.AfterDependencyFileSync, 1},
		{durabilitycut.AfterWALOrAssetUnlink, 1},
		{durabilitycut.AfterDeletionDirectorySync, 1},
		{durabilitycut.AfterDependencyFileSync, 2},
	}
	for i, cut := range cuts {
		variant := variants[i%len(variants)]
		name := fmt.Sprintf("%s/%s/%d", variant.ID, cut.point, cut.occurrence)
		t.Run(name, func(t *testing.T) {
			walDir, classification, anchor, tail := commandWALV2RepairFixture(t)
			anchorBefore := mustReadFile(t, anchor)
			tailBefore := mustReadFile(t, tail)
			readOnlyDiagnostic, err := repairCommandWALV2Suffix(walDir, classification, true)
			if !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("read-only error=%v, want ErrRecoveryRequired", err)
			}
			if got := mustReadFile(t, anchor); !reflect.DeepEqual(got, anchorBefore) {
				t.Fatalf("read-only mutated anchor: %q", got)
			}
			if got := mustReadFile(t, tail); !reflect.DeepEqual(got, tailBefore) {
				t.Fatalf("read-only mutated tail: %q", got)
			}

			injected := errors.New("injected V2 repair cut")
			seen := 0
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == cut.point {
					seen++
					if seen == cut.occurrence {
						return injected
					}
				}
				return nil
			})
			cutDiagnostic, err := repairCommandWALV2Suffix(walDir, classification, false)
			restore()
			if !errors.Is(err, injected) || !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("cut error=%v, want injected ErrRecoveryRequired", err)
			}
			if !reflect.DeepEqual(cutDiagnostic.RepairStages, readOnlyDiagnostic.RepairStages) {
				t.Fatalf("read-only/write repair plans differ: read-only=%v write=%v", readOnlyDiagnostic.RepairStages, cutDiagnostic.RepairStages)
			}

			finalDiagnostic, err := repairCommandWALV2Suffix(walDir, classification, false)
			if err != nil {
				t.Fatalf("repair retry: %v", err)
			}
			if !finalDiagnostic.DirectorySyncCompleted || finalDiagnostic.CompletedRepairStages != uint64(len(finalDiagnostic.RepairStages)) {
				t.Fatalf("final diagnostic=%+v", finalDiagnostic)
			}
			if got := mustReadFile(t, anchor); string(got) != "prefix-v2" {
				t.Fatalf("anchor after retry=%q, want prefix-v2", got)
			}
			if _, err := os.Stat(tail); !os.IsNotExist(err) {
				t.Fatalf("tail stat after retry=%v, want removed", err)
			}
		})
	}
}

func TestRepairCommandWALV2SuffixDefersRetainedAnchorUntilAfterDirectorySync(t *testing.T) {
	walDir, classification, anchor, _ := commandWALV2RepairFixture(t)
	anchorBefore := mustReadFile(t, anchor)
	wantStages := []string{
		"truncate-sync:commit-l1-000001.log@0",
		"unlink:commit-l1-000001.log",
		"directory-sync",
		"anchor-truncate-sync:commit-l0-000001.log@9",
	}

	var violation string
	directorySynced := false
	anchorFinalStageSeen := false
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource != durabilitycut.ResourceCommandWAL {
			return nil
		}
		if !directorySynced {
			got, err := os.ReadFile(anchor)
			if err != nil && violation == "" {
				violation = fmt.Sprintf("read anchor before directory sync: %v", err)
			} else if !reflect.DeepEqual(got, anchorBefore) && violation == "" {
				violation = fmt.Sprintf("anchor changed before directory sync at %s: got %q want %q", event.Point, got, anchorBefore)
			}
		}
		if event.Point == durabilitycut.AfterDeletionDirectorySync {
			directorySynced = true
		}
		if event.Path == anchor && event.Point == durabilitycut.BeforeDependencyFileSync {
			if !directorySynced && violation == "" {
				violation = "anchor sync began before deletion directory sync completed"
			}
			anchorFinalStageSeen = directorySynced
		}
		return nil
	})
	diagnostic, err := repairCommandWALV2Suffix(walDir, classification, false)
	restore()
	if err != nil {
		t.Fatal(err)
	}
	if violation != "" {
		t.Fatal(violation)
	}
	if !directorySynced || !anchorFinalStageSeen {
		t.Fatalf("directorySynced=%t anchorFinalStageSeen=%t", directorySynced, anchorFinalStageSeen)
	}
	if !reflect.DeepEqual(diagnostic.RepairStages, wantStages) {
		t.Fatalf("repair stages=%v, want %v", diagnostic.RepairStages, wantStages)
	}
	if got := mustReadFile(t, anchor); string(got) != "prefix-v2" {
		t.Fatalf("anchor after repair=%q, want prefix-v2", got)
	}
}

func commandWALV2RepairVariants(t *testing.T) []powerlossoracle.Variant {
	t.Helper()
	root := t.TempDir()
	path := "commit-l1-000001.log"
	fullPath := filepath.Join(root, path)
	if err := os.WriteFile(fullPath, []byte("stable-command-prefix"), 0o600); err != nil {
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fullPath, []byte("stable-command-prefix-discardable-relaxed-suffix"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := model.Overlay(root); err != nil {
		t.Fatal(err)
	}
	variants, coverage, err := powerlossoracle.GenerateVariants(powerlossoracle.CutSpec{
		ID:         "command-wal-v2-physical-suffix-repair",
		Point:      powerlossoracle.AfterDependencyFileSync,
		Occurrence: 1,
		Model:      model,
		Dependencies: []powerlossoracle.DirtyResource{{
			Kind: powerlossoracle.ResourceCommandWAL,
			ID:   "relaxed-suffix-segment",
			Path: path,
		}},
		RequiredFamilies: []powerlossoracle.VariantFamily{
			powerlossoracle.VariantSyncedOnly,
			powerlossoracle.VariantFullWriteback,
		},
		ExpectedByFamily: map[powerlossoracle.VariantFamily]powerlossoracle.ExpectedResult{
			powerlossoracle.VariantSyncedOnly:    powerlossoracle.ExpectedOldRoot,
			powerlossoracle.VariantFullWriteback: powerlossoracle.ExpectedNewRoot,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if coverage.Generated != len(variants) || len(variants) < 2 {
		t.Fatalf("repair variant coverage=%+v variants=%d", coverage, len(variants))
	}
	return variants
}

func commandWALV2RepairFixture(t *testing.T) (string, commandWALV2Classification, string, string) {
	t.Helper()
	walDir := t.TempDir()
	anchor := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
	tail := filepath.Join(walDir, commitlog.CommandSegmentName(1, 1))
	if err := os.WriteFile(anchor, []byte("prefix-v2anchor-v2later-v2!!"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tail, []byte("discarded-tail!"), 0o600); err != nil {
		t.Fatal(err)
	}
	classification := commandWALV2Classification{
		DurableFrontier: 1,
		CompletePrefix:  []commandWALV2PhysicalFrame{{Envelope: commitlog.CommandEnvelope{LSN: 1}, Coordinate: commandWALV2Coordinate{SourceSegment: anchor, StartOffset: 0, EndOffset: 9}}},
		DiscardSuffix: []commandWALV2PhysicalFrame{
			{Envelope: commitlog.CommandEnvelope{LSN: 2}, Coordinate: commandWALV2Coordinate{SourceSegment: anchor, StartOffset: 9, EndOffset: 18}},
			{Envelope: commitlog.CommandEnvelope{LSN: 3}, Coordinate: commandWALV2Coordinate{SourceSegment: tail, StartOffset: 0, EndOffset: 15}},
			{Envelope: commitlog.CommandEnvelope{LSN: 4}, Coordinate: commandWALV2Coordinate{SourceSegment: anchor, StartOffset: 18, EndOffset: 28}},
		},
		Diagnostic: CommandWALV2RecoveryDiagnostic{DurableFrontier: 1, FirstDiscardedLSN: 2, DiscardedFrameCount: 3, DiscardedBytes: 34, SourceSegment: filepath.Base(anchor)},
	}
	return walDir, classification, anchor, tail
}

func mustCommandWALV2Frame(t *testing.T, lsn uint64, class commitlog.CommandDurabilityClass, rids []uint64) []byte {
	t.Helper()
	ops := []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte(fmt.Sprintf("k-%d", lsn)), Value: []byte("v")}}
	for _, rid := range rids {
		ops = append(ops, commitlog.RawKVOperation{Op: commitlog.RawKVOpSetRID, Key: []byte(fmt.Sprintf("rid-%d", rid)), RID: rid})
	}
	payload, err := commitlog.EncodeRawKVBatchPayload(ops)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := commitlog.EncodeCommandFrameV2(commitlog.CommandEnvelope{DurabilityClass: class, LSN: lsn, Kind: commitlog.CommandKindRawKVBatch, Scope: commitlog.CommandScopeRawKV, PayloadFormat: commitlog.PayloadFormatRawKVBatchV1, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func mustDurablePrefixBarrierV2(t *testing.T, lsn, applied uint64) []byte {
	t.Helper()
	frame, err := commitlog.EncodeCommandFrameV2(commitlog.NewDurablePrefixBarrierV1(lsn, applied))
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func writeCommandWALV2Segment(t *testing.T, path string, frames ...[]byte) {
	t.Helper()
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, frame := range frames {
		appendCommandWALV2Segment(t, path, frame)
	}
}

func appendCommandWALV2Segment(t *testing.T, path string, frame []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(frame)))
	binary.LittleEndian.PutUint32(header[4:8], crc.Checksum(frame))
	if _, err := f.Write(header[:]); err == nil {
		_, err = f.Write(frame)
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
