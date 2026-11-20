package db

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func TestCommandWALV2RepairPreservesAppliedPrefixOnNonAnchorLane(t *testing.T) {
	walDir := t.TempDir()
	a := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
	b := filepath.Join(walDir, commitlog.CommandSegmentName(1, 1))
	writeCommandWALV2Segment(t, a, mustCommandWALV2Frame(t, 1, commitlog.CommandDurabilityDurable, nil))
	writeCommandWALV2Segment(t, b, mustCommandWALV2Frame(t, 2, commitlog.CommandDurabilityDurable, nil))
	beforeA, beforeB := mustReadFile(t, a), mustReadFile(t, b)
	statB, err := os.Stat(b)
	if err != nil {
		t.Fatal(err)
	}
	appendCommandWALV2Segment(t, a, mustCommandWALV2Frame(t, 3, commitlog.CommandDurabilityRelaxed, nil))
	appendCommandWALV2Segment(t, b, mustCommandWALV2Frame(t, 4, commitlog.CommandDurabilityRelaxed, nil))
	if err := os.Truncate(a, int64(len(beforeA)+8+56)); err != nil {
		t.Fatal(err)
	}
	read := func(applied uint64) commandWALV2Classification {
		t.Helper()
		segments, err := listSegmentsInDir(walDir)
		if err != nil {
			t.Fatal(err)
		}
		frames, err := readCommandWALV2PhysicalFrames(segments, applied, 0)
		if err != nil {
			t.Fatal(err)
		}
		classification, err := classifyCommandWALV2Frames(frames, applied, nil)
		if err != nil {
			t.Fatal(err)
		}
		return classification
	}
	classification := read(2)
	discard := classification.DiscardSuffix
	if len(classification.CompletePrefix) != 0 || len(discard) != 2 ||
		discard[0].Envelope.LSN != 3 || !discard[0].Incomplete ||
		discard[1].Envelope.LSN != 4 || discard[1].Coordinate.StartOffset != int64(len(beforeB)) {
		t.Fatalf("wrong physical classification: %+v", classification)
	}
	diagnostic, err := repairCommandWALV2Suffix(walDir, classification, false)
	if err != nil {
		t.Fatal(err)
	}
	if afterB, err := os.Stat(b); err != nil || !os.SameFile(statB, afterB) {
		t.Fatalf("repair removed or replaced the original non-anchor WAL holding applied LSN 2: %v, stages=%v", err, diagnostic.RepairStages)
	}
	if gotA, gotB := mustReadFile(t, a), mustReadFile(t, b); !bytes.Equal(gotA, beforeA) || !bytes.Equal(gotB, beforeB) {
		t.Fatalf("repaired WAL lengths %d/%d, want original prefixes %d/%d", len(gotA), len(gotB), len(beforeA), len(beforeB))
	}
	older := read(1)
	if len(older.CompletePrefix) != 1 || older.CompletePrefix[0].Envelope.LSN != 2 || len(older.DiscardSuffix) != 0 {
		t.Fatalf("older root cannot replay retained LSN 2: %+v", older)
	}
}
