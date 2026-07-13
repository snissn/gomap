package commitlog

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCommandJournalStableRotationDoesNotLoseClosedSegment(t *testing.T) {
	dir := t.TempDir()
	journal, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if _, err := journal.AppendCommand(CommandEnvelope{
		Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatal(err)
	}
	rotation, err := journal.RotateActiveSegmentWithStableResources(true)
	if err != nil {
		t.Fatal(err)
	}
	defer rotation.Release()
	if rotation.Closed == nil || rotation.Active == nil {
		t.Fatalf("rotation=%+v", rotation)
	}
	if rotation.Closed.Identity() == rotation.Active.Identity() {
		t.Fatal("command WAL rotation reused stable identity")
	}
	if rotation.Closed.Reachability() != rootpublication.ReachabilityCommandWALRotated ||
		rotation.Active.Reachability() != rootpublication.ReachabilityCommandWALActive {
		t.Fatalf("reachability closed=%q active=%q", rotation.Closed.Reachability(), rotation.Active.Reachability())
	}
	if rotation.Closed.Frontier().Bytes == 0 {
		t.Fatal("closed command WAL frontier is zero")
	}
	wantActive := filepath.ToSlash(filepath.Join("maindb", "wal", CommandSegmentName(3, 2)))
	if rotation.Active.DiagnosticPath() != wantActive {
		t.Fatalf("active diagnostic path=%q want %q", rotation.Active.DiagnosticPath(), wantActive)
	}
}

func TestCommandJournalStableRotationNamespaceFailureKeepsOldWriterActive(t *testing.T) {
	dir := t.TempDir()
	journal, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 4})
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
	injected := errors.New("injected namespace failure")
	originalFactory := newCommandWALStableNamespaceToken
	newCommandWALStableNamespaceToken = func(rootpublication.StableNamespaceSpec) (*rootpublication.StableNamespaceToken, error) {
		return nil, injected
	}
	defer func() { newCommandWALStableNamespaceToken = originalFactory }()
	rotation, err := journal.RotateActiveSegmentWithStableResources(true)
	if !errors.Is(err, injected) {
		t.Fatalf("rotation error=%v want injected namespace failure", err)
	}
	if rotation != nil {
		rotation.Release()
		t.Fatal("failed rotation returned owned resources")
	}
	newPath, _ := journal.ActiveSegmentSnapshot()
	if newPath != oldPath || journal.segmentSeq != oldSeq || journal.writer == nil || journal.writer.f == nil {
		t.Fatalf("failed rotation changed active journal: path=%q seq=%d", newPath, journal.segmentSeq)
	}
	if _, err := journal.AppendCommand(CommandEnvelope{
		Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatalf("old journal append after failed rotation: %v", err)
	}
}

func TestCommandJournalRecordAppendDoesNotAddNamespaceSync(t *testing.T) {
	dir := t.TempDir()
	journal, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	before := journal.WriterDurabilityStats().DirectorySyncCalls
	for i := 0; i < 32; i++ {
		if _, err := journal.AppendCommand(CommandEnvelope{
			Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
		}); err != nil {
			t.Fatal(err)
		}
	}
	after := journal.WriterDurabilityStats().DirectorySyncCalls
	if after != before {
		t.Fatalf("ordinary appends added namespace syncs: before=%d after=%d", before, after)
	}
}
