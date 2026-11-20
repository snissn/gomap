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

func TestCommandWALV2RepairPreservesFixedLogicalAppliedPrefix(t *testing.T) {
	walDir := t.TempDir()
	parent, err := os.Open(walDir)
	if err != nil {
		t.Fatal(err)
	}
	ext4 := commitlog.IsExt4WALFilesystem(parent)
	if err := parent.Close(); err != nil {
		t.Fatal(err)
	}
	if !ext4 || !commitlog.FixedWALCreationSupported() {
		t.Skip("requires ext4 fixed-WAL creation")
	}
	a := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
	b := filepath.Join(walDir, commitlog.CommandSegmentName(1, 1))
	writeCommandWALV2Segment(t, a, mustCommandWALV2Frame(t, 1, commitlog.CommandDurabilityDurable, nil))
	appendFixed := func(want uint64, class commitlog.CommandDurabilityClass) {
		t.Helper()
		frame := mustCommandWALV2Frame(t, want, class, nil)
		env, err := commitlog.DecodeCommandFrameV2(frame)
		if err != nil {
			t.Fatal(err)
		}
		env.LSN = 0
		journal, err := commitlog.OpenCommandJournal(walDir, commitlog.CommandJournalOptions{Lane: 1})
		if err != nil {
			t.Fatal(err)
		}
		lsn, appendErr := journal.AppendCommand(env)
		if appendErr == nil {
			appendErr = journal.FlushObserved(class == commitlog.CommandDurabilityDurable)
		}
		if closeErr := journal.Close(); appendErr != nil || closeErr != nil || lsn != want {
			t.Fatalf("append fixed LSN %d: got %d, append=%v close=%v", want, lsn, appendErr, closeErr)
		}
	}
	appendFixed(2, commitlog.CommandDurabilityDurable)
	f, err := os.Open(b)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	fixed, err := commitlog.HasFixedWALMagic(f)
	if err != nil || !fixed {
		t.Fatalf("fixture is not fixed: fixed=%v err=%v", fixed, err)
	}
	prefixBytes, err := commitlog.LogicalFileSize(f)
	if err != nil || prefixBytes <= 0 {
		t.Fatalf("invalid fixed logical prefix %d: %v", prefixBytes, err)
	}
	appendCommandWALV2Segment(t, a, mustCommandWALV2Frame(t, 3, commitlog.CommandDurabilityRelaxed, nil))
	appendFixed(4, commitlog.CommandDurabilityRelaxed)
	if err := os.Truncate(a, int64(len(mustReadFile(t, a))-1)); err != nil {
		t.Fatal(err)
	}
	segments, err := listSegmentsInDir(walDir)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := readCommandWALV2PhysicalFrames(segments, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	classification, err := classifyCommandWALV2Frames(frames, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	discard := classification.DiscardSuffix
	if len(discard) != 2 || discard[1].Envelope.LSN != 4 || discard[1].Coordinate.StartOffset != prefixBytes {
		t.Fatalf("fixed repair did not use the logical prefix offset %d: %+v", prefixBytes, classification)
	}
	if _, err := repairCommandWALV2Suffix(walDir, classification, false); err != nil {
		t.Fatal(err)
	}
	segments, err = listSegmentsInDir(walDir)
	if err != nil {
		t.Fatal(err)
	}
	kept, err := readCommandWALV2PhysicalFrames(segments, 1, 0)
	if err != nil || len(kept) != 1 || kept[0].Envelope.LSN != 2 || kept[0].Coordinate.EndOffset != prefixBytes {
		t.Fatalf("repaired fixed logical prefix: frames=%+v error=%v", kept, err)
	}
}
