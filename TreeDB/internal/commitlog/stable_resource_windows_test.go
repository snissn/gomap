//go:build windows

package commitlog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCommandJournalStableRotationFailsClosedWithoutSuccessorVisibility(t *testing.T) {
	for _, syncCurrent := range []bool{false, true} {
		t.Run(map[bool]string{false: "flush", true: "sync"}[syncCurrent], func(t *testing.T) {
			dir := t.TempDir()
			journal, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 5})
			if err != nil {
				t.Fatal(err)
			}
			defer journal.Close()
			if _, err := journal.AppendCommand(CommandEnvelope{
				Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
			}); err != nil {
				t.Fatal(err)
			}
			oldPath, _ := journal.ActiveSegmentSnapshot()
			oldSeq := journal.segmentSeq
			rotation, err := journal.RotateActiveSegmentWithStableResources(syncCurrent)
			if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
				if rotation != nil {
					rotation.Release()
				}
				t.Fatalf("stable rotation error=%v want ErrNamespacePersistenceUnsupported", err)
			}
			if rotation != nil {
				rotation.Release()
				t.Fatal("unsupported stable rotation returned owned resources")
			}
			newPath, _ := journal.ActiveSegmentSnapshot()
			if newPath != oldPath || journal.segmentSeq != oldSeq {
				t.Fatalf("unsupported rotation changed active journal: path=%q seq=%d", newPath, journal.segmentSeq)
			}
			successor := filepath.Join(dir, CommandSegmentName(5, oldSeq+1))
			if _, err := os.Stat(successor); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unsupported rotation exposed successor %q: %v", successor, err)
			}
			if _, err := journal.AppendCommand(CommandEnvelope{
				Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
			}); err != nil {
				t.Fatalf("old journal append after unsupported rotation: %v", err)
			}
		})
	}
}

func TestOrdinaryCommandJournalRotationRemainsUsableAndStableRotationFailsClosed(t *testing.T) {
	dir := t.TempDir()
	journal, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 6})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if err := journal.RotateActiveSegment(false); err != nil {
		t.Fatalf("ordinary rotation: %v", err)
	}
	if _, err := journal.AppendCommand(CommandEnvelope{
		Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatal(err)
	}
	secondPath, _ := journal.ActiveSegmentSnapshot()
	rotation, err := journal.RotateActiveSegmentWithStableResources(false)
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) || rotation != nil {
		if rotation != nil {
			rotation.Release()
		}
		t.Fatalf("stable rotation=(%v, %v) want typed unsupported and no tokens", rotation, err)
	}
	activePath, _ := journal.ActiveSegmentSnapshot()
	if activePath != secondPath || journal.segmentSeq != 2 || journal.writer == nil || journal.writer.f == nil {
		t.Fatalf("failed stable rotation changed ordinary active journal: path=%q seq=%d", activePath, journal.segmentSeq)
	}
	if _, err := os.Stat(filepath.Join(dir, CommandSegmentName(6, 3))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsupported stable rotation exposed successor: %v", err)
	}
}
