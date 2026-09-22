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

func TestReadCommandWALV2RejectsNonIncreasingPhysicalSegmentBeforeRepair(t *testing.T) {
	walDir := t.TempDir()
	path := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
	writeCommandWALV2Segment(t, path,
		mustCommandWALV2Frame(t, 1, commitlog.CommandDurabilityDurable, nil),
		mustCommandWALV2Frame(t, 3, commitlog.CommandDurabilityRelaxed, nil),
		mustCommandWALV2Frame(t, 2, commitlog.CommandDurabilityRelaxed, []uint64{41}),
	)
	before := mustReadFile(t, path)
	segments, err := listSegmentsInDir(walDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := readCommandWALV2PhysicalFrames(segments, 0, 0); !errors.Is(err, commitlog.ErrCommandWALDuplicateLSN) {
		t.Fatalf("physical scan error=%v, want ErrCommandWALDuplicateLSN before global sorting", err)
	}
	if after := mustReadFile(t, path); !reflect.DeepEqual(after, before) {
		t.Fatalf("failed-closed physical scan mutated segment: before=%d bytes after=%d bytes", len(before), len(after))
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

func TestReadCommandWALV2ShortestClassifiableRelaxedTailCanRepair(t *testing.T) {
	walDir := t.TempDir()
	lane0 := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
	lane1 := filepath.Join(walDir, commitlog.CommandSegmentName(1, 1))
	writeCommandWALV2Segment(t, lane0, mustCommandWALV2Frame(t, 1, commitlog.CommandDurabilityDurable, nil))
	writeCommandWALV2Segment(t, lane1, mustCommandWALV2Frame(t, 2, commitlog.CommandDurabilityRelaxed, nil))
	if err := os.Truncate(lane1, 8+56); err != nil {
		t.Fatal(err)
	}

	classification := classifyCommandWALV2Directory(t, walDir)
	if len(classification.CompletePrefix) != 1 || len(classification.DiscardSuffix) != 1 || !classification.DiscardSuffix[0].Incomplete || classification.DiscardSuffix[0].Envelope.LSN != 2 {
		t.Fatalf("classification=%+v, want durable LSN 1 plus incomplete relaxed LSN 2 suffix", classification)
	}
	if _, err := repairCommandWALV2Suffix(walDir, classification, false); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(lane1)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("repaired active tail size=%d, want 0", info.Size())
	}
}

func TestReadCommandWALV2TailWithoutCompleteClassFailsClosed(t *testing.T) {
	walDir := t.TempDir()
	lane0 := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
	lane1 := filepath.Join(walDir, commitlog.CommandSegmentName(1, 1))
	writeCommandWALV2Segment(t, lane0, mustCommandWALV2Frame(t, 1, commitlog.CommandDurabilityDurable, nil))
	writeCommandWALV2Segment(t, lane1, mustCommandWALV2Frame(t, 2, commitlog.CommandDurabilityRelaxed, nil))
	if err := os.Truncate(lane1, 8+55); err != nil {
		t.Fatal(err)
	}
	segments, err := listSegmentsInDir(walDir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = readCommandWALV2PhysicalFrames(segments, 0, 0)
	if !errors.Is(err, commitlog.ErrCommandWALV2TailIdentityUnavailable) || !errors.Is(err, commitlog.ErrCorrupt) {
		t.Fatalf("short tail error=%v, want identity-unavailable corruption", err)
	}
}

func TestReadCommandWALV2TerminalTailCriticalFeatureFlagFailsClosed(t *testing.T) {
	walDir := t.TempDir()
	lane0 := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
	lane1 := filepath.Join(walDir, commitlog.CommandSegmentName(1, 1))
	writeCommandWALV2Segment(t, lane0, mustCommandWALV2Frame(t, 1, commitlog.CommandDurabilityDurable, nil))
	tail := mustCommandWALV2Frame(t, 2, commitlog.CommandDurabilityRelaxed, nil)
	binary.LittleEndian.PutUint64(tail[12:20], 1)
	writeCommandWALV2Segment(t, lane1, tail)
	if err := os.Truncate(lane1, 8+56); err != nil {
		t.Fatal(err)
	}
	segments, err := listSegmentsInDir(walDir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = readCommandWALV2PhysicalFrames(segments, 0, 0)
	if !errors.Is(err, commitlog.ErrCommandWALUnsupportedCriticalFlag) {
		t.Fatalf("terminal tail error=%v, want ErrCommandWALUnsupportedCriticalFlag", err)
	}
	if info, statErr := os.Stat(lane1); statErr != nil || info.Size() != 8+56 {
		t.Fatalf("failed-closed tail stat=(%v, %v), want untouched size %d", info, statErr, 8+56)
	}
}

func TestRepairCommandWALV2SuffixReadOnlyParityAndRetryableCuts(t *testing.T) {
	cuts := []struct {
		point      durabilitycut.Point
		occurrence int
	}{
		{durabilitycut.AfterDependencyFileSync, 1},
		{durabilitycut.AfterWALOrAssetUnlink, 1},
		{durabilitycut.AfterDeletionDirectorySync, 1},
		{durabilitycut.AfterDependencyFileSync, 2},
	}
	for _, cut := range cuts {
		name := fmt.Sprintf("%s/%d", cut.point, cut.occurrence)
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

func TestRepairCommandWALV2SuffixRetryNeverExtendsShortenedNonAnchorSegment(t *testing.T) {
	walDir := t.TempDir()
	anchor := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
	tail := filepath.Join(walDir, commitlog.CommandSegmentName(1, 1))
	writeCommandWALV2Segment(t, anchor,
		mustCommandWALV2Frame(t, 1, commitlog.CommandDurabilityDurable, nil),
		mustCommandWALV2Frame(t, 2, commitlog.CommandDurabilityRelaxed, []uint64{41}),
	)
	writeCommandWALV2Segment(t, tail,
		mustCommandWALV2Frame(t, 3, commitlog.CommandDurabilityRelaxed, nil),
		mustCommandWALV2Frame(t, 4, commitlog.CommandDurabilityRelaxed, nil),
	)
	classification := classifyCommandWALV2Directory(t, walDir)
	if len(classification.DiscardSuffix) != 3 || classification.DiscardSuffix[0].Envelope.LSN != 2 {
		t.Fatalf("classification=%+v, want discarded LSNs 2..4", classification)
	}

	firstCrash := errors.New("injected after shortest non-anchor truncate sync")
	nonAnchorSyncs := 0
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Path == tail && event.Point == durabilitycut.AfterDependencyFileSync {
			nonAnchorSyncs++
			if nonAnchorSyncs == 2 {
				return firstCrash
			}
		}
		return nil
	})
	_, err := repairCommandWALV2Suffix(walDir, classification, false)
	restore()
	if !errors.Is(err, firstCrash) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("first repair error=%v, want injected retryable crash", err)
	}
	if info, statErr := os.Stat(tail); statErr != nil || info.Size() != 0 {
		t.Fatalf("shortened non-anchor stat=(%v, %v), want zero-byte synced suffix", info, statErr)
	}

	retryGrowth := errors.New("retry attempted to grow shortened segment")
	retryGrowthSeen := false
	restore = durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Path == tail && event.Point == durabilitycut.BeforeDependencyFileSync {
			retryGrowthSeen = true
			return retryGrowth
		}
		return nil
	})
	_, err = repairCommandWALV2Suffix(walDir, classification, false)
	restore()
	if retryGrowthSeen || err != nil {
		if info, statErr := os.Stat(tail); statErr == nil {
			t.Fatalf("repair retry grew non-anchor segment to %d bytes: error=%v", info.Size(), err)
		}
		t.Fatalf("repair retry growth event=%t error=%v, want monotonic removal", retryGrowthSeen, err)
	}
	if _, err := os.Stat(tail); !os.IsNotExist(err) {
		t.Fatalf("non-anchor segment stat after retry=%v, want removed", err)
	}
}

func TestRepairCommandWALV2SuffixRetryAcceptsRetainedPathAtProvenFloor(t *testing.T) {
	walDir := t.TempDir()
	anchor := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
	mixed := filepath.Join(walDir, commitlog.CommandSegmentName(1, 1))
	if err := os.WriteFile(anchor, []byte("anchor-v2discard"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(mixed, []byte("prefix-v2discard-adiscard-b"), 0o600); err != nil {
		t.Fatal(err)
	}
	classification := commandWALV2Classification{
		CompletePrefix: []commandWALV2PhysicalFrame{
			{Envelope: commitlog.CommandEnvelope{LSN: 1}, Coordinate: commandWALV2Coordinate{SourceSegment: anchor, StartOffset: 0, EndOffset: 9}},
			{Envelope: commitlog.CommandEnvelope{LSN: 2}, Coordinate: commandWALV2Coordinate{Lane: 1, SegmentSequence: 1, SourceSegment: mixed, StartOffset: 0, EndOffset: 9}},
		},
		DiscardSuffix: []commandWALV2PhysicalFrame{
			{Envelope: commitlog.CommandEnvelope{LSN: 3}, Coordinate: commandWALV2Coordinate{SourceSegment: anchor, StartOffset: 9, EndOffset: 16}},
			{Envelope: commitlog.CommandEnvelope{LSN: 4}, Coordinate: commandWALV2Coordinate{Lane: 1, SegmentSequence: 1, SourceSegment: mixed, StartOffset: 9, EndOffset: 18}},
			{Envelope: commitlog.CommandEnvelope{LSN: 5}, Coordinate: commandWALV2Coordinate{Lane: 1, SegmentSequence: 1, SourceSegment: mixed, StartOffset: 18, EndOffset: 27}},
		},
	}

	crash := errors.New("injected after retained floor truncate sync")
	mixedAfterSyncs := 0
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Path == mixed && event.Point == durabilitycut.AfterDependencyFileSync {
			mixedAfterSyncs++
			if mixedAfterSyncs == 2 {
				return crash
			}
		}
		return nil
	})
	_, err := repairCommandWALV2Suffix(walDir, classification, false)
	restore()
	if !errors.Is(err, crash) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("first repair error=%v, want injected retryable crash", err)
	}
	if got := mustReadFile(t, mixed); string(got) != "prefix-v2" {
		t.Fatalf("mixed path after durable floor cut=%q, want retained prefix", got)
	}

	retryBeforeSyncs := 0
	retryAfterSyncs := 0
	retryGrowth := false
	restore = durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource != durabilitycut.ResourceCommandWAL || event.Path != mixed {
			return nil
		}
		if info, statErr := os.Stat(mixed); statErr != nil {
			return statErr
		} else if info.Size() > 9 {
			retryGrowth = true
			return fmt.Errorf("retained mixed path grew to %d bytes", info.Size())
		}
		switch event.Point {
		case durabilitycut.BeforeDependencyFileSync:
			retryBeforeSyncs++
		case durabilitycut.AfterDependencyFileSync:
			retryAfterSyncs++
		}
		return nil
	})
	diagnostic, err := repairCommandWALV2Suffix(walDir, classification, false)
	restore()
	if err != nil {
		t.Fatalf("repair retry from proven retained floor: %v", err)
	}
	if retryGrowth || retryBeforeSyncs != 1 || retryAfterSyncs != 1 {
		t.Fatalf("retry growth=%t mixed sync events before=%d after=%d, want no growth and one floor re-sync", retryGrowth, retryBeforeSyncs, retryAfterSyncs)
	}
	if got := mustReadFile(t, mixed); string(got) != "prefix-v2" {
		t.Fatalf("mixed path after retry=%q, want retained prefix", got)
	}
	if got := mustReadFile(t, anchor); string(got) != "anchor-v2" {
		t.Fatalf("anchor after retry=%q, want retained prefix", got)
	}
	if diagnostic.CompletedRepairStages != uint64(len(diagnostic.RepairStages)) {
		t.Fatalf("retry diagnostic=%+v, want every stage completed", diagnostic)
	}
}

func TestRepairCommandWALV2SuffixRetryResyncsExactSizeRetainedAnchor(t *testing.T) {
	walDir, classification, anchor, _ := commandWALV2RepairFixture(t)
	preSyncCrash := errors.New("injected after retained-anchor truncate before sync")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Path == anchor && event.Point == durabilitycut.BeforeDependencyFileSync {
			return preSyncCrash
		}
		return nil
	})
	_, err := repairCommandWALV2Suffix(walDir, classification, false)
	restore()
	if !errors.Is(err, preSyncCrash) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("first repair error=%v, want retained-anchor pre-sync crash", err)
	}
	if got := mustReadFile(t, anchor); string(got) != "prefix-v2" {
		t.Fatalf("anchor after failed pre-sync=%q, want exact retained prefix", got)
	}

	retryBeforeSync := false
	retryAfterSync := false
	restore = durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource != durabilitycut.ResourceCommandWAL || event.Path != anchor {
			return nil
		}
		switch event.Point {
		case durabilitycut.BeforeDependencyFileSync:
			retryBeforeSync = true
		case durabilitycut.AfterDependencyFileSync:
			retryAfterSync = true
		}
		return nil
	})
	diagnostic, err := repairCommandWALV2Suffix(walDir, classification, false)
	restore()
	if err != nil {
		t.Fatalf("repair retry: %v", err)
	}
	if !retryBeforeSync || !retryAfterSync {
		t.Fatalf("exact-size retained anchor retry sync events before=%t after=%t, want both", retryBeforeSync, retryAfterSync)
	}
	if diagnostic.CompletedRepairStages != uint64(len(diagnostic.RepairStages)) {
		t.Fatalf("retry diagnostic=%+v, want every stage completed", diagnostic)
	}
}

func TestRepairCommandWALV2SuffixRejectsUndersizedRetainedPaths(t *testing.T) {
	t.Run("anchor", func(t *testing.T) {
		walDir := t.TempDir()
		anchor := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
		if err := os.WriteFile(anchor, []byte("short"), 0o600); err != nil {
			t.Fatal(err)
		}
		classification := commandWALV2Classification{
			DiscardSuffix: []commandWALV2PhysicalFrame{{
				Envelope:   commitlog.CommandEnvelope{LSN: 2},
				Coordinate: commandWALV2Coordinate{SourceSegment: anchor, StartOffset: 9, EndOffset: 18},
			}},
		}
		_, err := repairCommandWALV2Suffix(walDir, classification, false)
		if !errors.Is(err, ErrRecoveryRequired) || !errors.Is(err, commitlog.ErrCorrupt) {
			t.Fatalf("undersized retained anchor error=%v, want recovery-required corruption", err)
		}
		if got := mustReadFile(t, anchor); string(got) != "short" {
			t.Fatalf("undersized retained anchor mutated to %q", got)
		}
	})

	t.Run("mixed_non_anchor", func(t *testing.T) {
		walDir := t.TempDir()
		anchor := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
		mixed := filepath.Join(walDir, commitlog.CommandSegmentName(1, 1))
		if err := os.WriteFile(anchor, []byte("prefix-v2discard"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(mixed, []byte("short"), 0o600); err != nil {
			t.Fatal(err)
		}
		classification := commandWALV2RetainedMixedClassification(anchor, mixed)
		_, err := repairCommandWALV2Suffix(walDir, classification, false)
		if !errors.Is(err, ErrRecoveryRequired) || !errors.Is(err, commitlog.ErrCorrupt) {
			t.Fatalf("undersized retained mixed path error=%v, want recovery-required corruption", err)
		}
		if got := mustReadFile(t, mixed); string(got) != "short" {
			t.Fatalf("undersized retained mixed path mutated to %q", got)
		}
		if got := mustReadFile(t, anchor); string(got) != "prefix-v2discard" {
			t.Fatalf("anchor mutated before retained mixed-path rejection: %q", got)
		}
	})
}

func TestRepairCommandWALV2SuffixRejectsMissingRetainedNonAnchor(t *testing.T) {
	walDir := t.TempDir()
	anchor := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
	mixed := filepath.Join(walDir, commitlog.CommandSegmentName(1, 1))
	if err := os.WriteFile(anchor, []byte("prefix-v2discard"), 0o600); err != nil {
		t.Fatal(err)
	}
	classification := commandWALV2RetainedMixedClassification(anchor, mixed)
	_, err := repairCommandWALV2Suffix(walDir, classification, false)
	if !errors.Is(err, ErrRecoveryRequired) || !errors.Is(err, commitlog.ErrCorrupt) || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing retained mixed path error=%v, want not-exist recovery-required corruption", err)
	}
	if got := mustReadFile(t, anchor); string(got) != "prefix-v2discard" {
		t.Fatalf("anchor mutated before missing retained-path rejection: %q", got)
	}
}

func TestRepairCommandWALV2SuffixDisposablePathMissingOrShortIsIdempotent(t *testing.T) {
	for _, testCase := range []struct {
		name string
		data []byte
	}{
		{name: "missing"},
		{name: "short", data: []byte("abc")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			walDir := t.TempDir()
			anchor := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
			tail := filepath.Join(walDir, commitlog.CommandSegmentName(1, 1))
			if err := os.WriteFile(anchor, []byte("prefix-v2discard"), 0o600); err != nil {
				t.Fatal(err)
			}
			if testCase.data != nil {
				if err := os.WriteFile(tail, testCase.data, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			classification := commandWALV2Classification{
				CompletePrefix: []commandWALV2PhysicalFrame{{
					Envelope:   commitlog.CommandEnvelope{LSN: 1},
					Coordinate: commandWALV2Coordinate{SourceSegment: anchor, StartOffset: 0, EndOffset: 9},
				}},
				DiscardSuffix: []commandWALV2PhysicalFrame{
					{Envelope: commitlog.CommandEnvelope{LSN: 2}, Coordinate: commandWALV2Coordinate{SourceSegment: anchor, StartOffset: 9, EndOffset: 16}},
					{Envelope: commitlog.CommandEnvelope{LSN: 3}, Coordinate: commandWALV2Coordinate{Lane: 1, SegmentSequence: 1, SourceSegment: tail, StartOffset: 0, EndOffset: 5}},
					{Envelope: commitlog.CommandEnvelope{LSN: 4}, Coordinate: commandWALV2Coordinate{Lane: 1, SegmentSequence: 1, SourceSegment: tail, StartOffset: 5, EndOffset: 10}},
				},
			}
			maxTailSize := int64(len(testCase.data))
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Resource != durabilitycut.ResourceCommandWAL || event.Path != tail {
					return nil
				}
				if info, err := os.Stat(tail); err == nil && info.Size() > maxTailSize {
					return fmt.Errorf("disposable tail grew from %d to %d bytes", maxTailSize, info.Size())
				}
				return nil
			})
			_, err := repairCommandWALV2Suffix(walDir, classification, false)
			restore()
			if err != nil {
				t.Fatalf("first disposable repair: %v", err)
			}
			if _, err := repairCommandWALV2Suffix(walDir, classification, false); err != nil {
				t.Fatalf("idempotent disposable repair: %v", err)
			}
			if _, err := os.Stat(tail); !os.IsNotExist(err) {
				t.Fatalf("disposable tail stat=%v, want absent", err)
			}
			if got := mustReadFile(t, anchor); string(got) != "prefix-v2" {
				t.Fatalf("retained anchor after disposable repair=%q, want prefix-v2", got)
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

func TestRepairCommandWALV2SuffixPowerLossVariantsDriveFreshRescanRetry(t *testing.T) {
	walDir, classification, _, tail := commandWALV2PhysicalRepairFixture(t)
	tailBefore := mustReadFile(t, tail)
	model, err := powerlossoracle.Capture(walDir)
	if err != nil {
		t.Fatal(err)
	}
	var beforeSync, afterSync *powerlossoracle.Model
	injected := errors.New("injected after non-anchor sync")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource != durabilitycut.ResourceCommandWAL {
			return nil
		}
		if err := model.Observe(walDir, event); err != nil {
			return err
		}
		switch event.Point {
		case durabilitycut.BeforeDependencyFileSync:
			if event.Path != tail {
				return fmt.Errorf("first non-anchor sync path=%s, want %s", event.Path, tail)
			}
			beforeSync = model.Clone()
		case durabilitycut.AfterDependencyFileSync:
			if event.Path != tail {
				return fmt.Errorf("first completed non-anchor sync path=%s, want %s", event.Path, tail)
			}
			afterSync = model.Clone()
			return injected
		}
		return nil
	})
	if _, err := repairCommandWALV2Suffix(walDir, classification, false); !errors.Is(err, injected) {
		restore()
		t.Fatalf("repair cut error=%v, want injected cut", err)
	}
	restore()
	if beforeSync == nil || afterSync == nil {
		t.Fatalf("repair sync models before=%v after=%v, want both", beforeSync != nil, afterSync != nil)
	}

	type repairCrashVariant struct {
		variant  powerlossoracle.Variant
		wantSize int64
	}
	var crashVariants []repairCrashVariant
	for _, cut := range []struct {
		id       string
		point    powerlossoracle.CutPoint
		model    *powerlossoracle.Model
		expected powerlossoracle.ExpectedResult
		wantSize int64
	}{
		{
			id:       "command-wal-v2-physical-suffix-repair-before-sync",
			point:    powerlossoracle.BeforeDependencyFileSync,
			model:    beforeSync,
			expected: powerlossoracle.ExpectedOldRoot,
			wantSize: int64(len(tailBefore)),
		},
		{
			id:       "command-wal-v2-physical-suffix-repair-after-sync",
			point:    powerlossoracle.AfterDependencyFileSync,
			model:    afterSync,
			expected: powerlossoracle.ExpectedSuffixDiscard,
			wantSize: 0,
		},
	} {
		variants, coverage, err := powerlossoracle.GenerateVariants(powerlossoracle.CutSpec{
			ID:               cut.id,
			Point:            cut.point,
			Occurrence:       1,
			Model:            cut.model,
			RequiredFamilies: []powerlossoracle.VariantFamily{powerlossoracle.VariantSyncedOnly},
			ExpectedByFamily: map[powerlossoracle.VariantFamily]powerlossoracle.ExpectedResult{
				powerlossoracle.VariantSyncedOnly: cut.expected,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if coverage.Generated != 1 || len(variants) != 1 {
			t.Fatalf("repair variant coverage=%+v variants=%d", coverage, len(variants))
		}
		crashVariants = append(crashVariants, repairCrashVariant{variant: variants[0], wantSize: cut.wantSize})
	}

	for _, crashVariant := range crashVariants {
		variant := crashVariant.variant
		t.Run(variant.ID, func(t *testing.T) {
			crashDir := t.TempDir()
			if err := variant.Model.MaterializeStable(crashDir); err != nil {
				t.Fatal(err)
			}
			crashTail := filepath.Join(crashDir, filepath.Base(tail))
			info, err := os.Stat(crashTail)
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() != crashVariant.wantSize {
				t.Fatalf("stable tail size=%d, want %d for %s", info.Size(), crashVariant.wantSize, variant.CutID)
			}

			fresh := classifyCommandWALV2Directory(t, crashDir)
			if fresh.DurableFrontier != 1 || len(fresh.DiscardSuffix) == 0 || fresh.DiscardSuffix[0].Envelope.LSN != 2 {
				t.Fatalf("fresh classification=%+v, want durable frontier 1 and suffix at 2", fresh)
			}
			if _, err := repairCommandWALV2Suffix(crashDir, fresh, false); err != nil {
				t.Fatalf("fresh repair retry: %v", err)
			}
			if _, err := os.Stat(crashTail); !os.IsNotExist(err) {
				t.Fatalf("tail stat after fresh retry=%v, want removed", err)
			}
			segments, err := listSegmentsInDir(crashDir)
			if err != nil {
				t.Fatal(err)
			}
			frames, err := readCommandWALV2PhysicalFrames(segments, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(frames) != 1 || frames[0].Envelope.LSN != 1 {
				t.Fatalf("frames after fresh retry=%+v, want only LSN 1", frames)
			}
		})
	}
}

func TestRepairCommandWALV2SuffixOrdersCrossLaneRemovalByHighestDiscardedLSN(t *testing.T) {
	walDir := t.TempDir()
	anchor := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
	earlierLSNLexicallyLater := filepath.Join(walDir, commitlog.CommandSegmentName(9, 1))
	laterLSNLexicallyEarlier := filepath.Join(walDir, commitlog.CommandSegmentName(1, 1))
	for path, data := range map[string]string{
		anchor:                   "prefix-v2discard",
		earlierLSNLexicallyLater: "lsn-3",
		laterLSNLexicallyEarlier: "lsn-4",
	} {
		if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	classification := commandWALV2Classification{
		CompletePrefix: []commandWALV2PhysicalFrame{{Envelope: commitlog.CommandEnvelope{LSN: 1}, Coordinate: commandWALV2Coordinate{SourceSegment: anchor, StartOffset: 0, EndOffset: 9}}},
		DiscardSuffix: []commandWALV2PhysicalFrame{
			{Envelope: commitlog.CommandEnvelope{LSN: 2}, Coordinate: commandWALV2Coordinate{SourceSegment: anchor, StartOffset: 9, EndOffset: 16}},
			{Envelope: commitlog.CommandEnvelope{LSN: 3}, Coordinate: commandWALV2Coordinate{Lane: 9, SegmentSequence: 1, SourceSegment: earlierLSNLexicallyLater, StartOffset: 0, EndOffset: 5}},
			{Envelope: commitlog.CommandEnvelope{LSN: 4}, Coordinate: commandWALV2Coordinate{Lane: 1, SegmentSequence: 1, SourceSegment: laterLSNLexicallyEarlier, StartOffset: 0, EndOffset: 5}},
		},
	}
	wantUnlinks := []string{laterLSNLexicallyEarlier, earlierLSNLexicallyLater}
	var gotUnlinks []string
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.BeforeWALOrAssetUnlink {
			gotUnlinks = append(gotUnlinks, event.Path)
		}
		return nil
	})
	diagnostic, err := repairCommandWALV2Suffix(walDir, classification, false)
	restore()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotUnlinks, wantUnlinks) {
		t.Fatalf("unlink order=%v, want reverse global-LSN order %v", gotUnlinks, wantUnlinks)
	}
	wantStages := []string{
		"truncate-sync:commit-l1-000001.log@0",
		"truncate-sync:commit-l9-000001.log@0",
		"unlink:commit-l1-000001.log",
		"unlink:commit-l9-000001.log",
		"directory-sync",
		"anchor-truncate-sync:commit-l0-000001.log@9",
	}
	if !reflect.DeepEqual(diagnostic.RepairStages, wantStages) {
		t.Fatalf("repair stages=%v, want execution-matching stages %v", diagnostic.RepairStages, wantStages)
	}
}

func commandWALV2PhysicalRepairFixture(t *testing.T) (string, commandWALV2Classification, string, string) {
	t.Helper()
	walDir := t.TempDir()
	anchor := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
	tail := filepath.Join(walDir, commitlog.CommandSegmentName(1, 1))
	writeCommandWALV2Segment(t, anchor,
		mustCommandWALV2Frame(t, 1, commitlog.CommandDurabilityDurable, nil),
		mustCommandWALV2Frame(t, 2, commitlog.CommandDurabilityRelaxed, []uint64{41}),
		mustCommandWALV2Frame(t, 4, commitlog.CommandDurabilityRelaxed, nil),
	)
	writeCommandWALV2Segment(t, tail, mustCommandWALV2Frame(t, 3, commitlog.CommandDurabilityRelaxed, nil))
	return walDir, classifyCommandWALV2Directory(t, walDir), anchor, tail
}

func commandWALV2RetainedMixedClassification(anchor, mixed string) commandWALV2Classification {
	return commandWALV2Classification{
		CompletePrefix: []commandWALV2PhysicalFrame{
			{Envelope: commitlog.CommandEnvelope{LSN: 1}, Coordinate: commandWALV2Coordinate{SourceSegment: anchor, StartOffset: 0, EndOffset: 9}},
			{Envelope: commitlog.CommandEnvelope{LSN: 2}, Coordinate: commandWALV2Coordinate{Lane: 1, SegmentSequence: 1, SourceSegment: mixed, StartOffset: 0, EndOffset: 9}},
		},
		DiscardSuffix: []commandWALV2PhysicalFrame{
			{Envelope: commitlog.CommandEnvelope{LSN: 3}, Coordinate: commandWALV2Coordinate{SourceSegment: anchor, StartOffset: 9, EndOffset: 16}},
			{Envelope: commitlog.CommandEnvelope{LSN: 4}, Coordinate: commandWALV2Coordinate{Lane: 1, SegmentSequence: 1, SourceSegment: mixed, StartOffset: 9, EndOffset: 18}},
		},
	}
}

func classifyCommandWALV2Directory(t *testing.T, walDir string) commandWALV2Classification {
	t.Helper()
	segments, err := listSegmentsInDir(walDir)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := readCommandWALV2PhysicalFrames(segments, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	classification, err := classifyCommandWALV2Frames(frames, 0, func(uint64) bool { return false })
	if err != nil {
		t.Fatal(err)
	}
	return classification
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
