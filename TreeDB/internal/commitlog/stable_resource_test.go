package commitlog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCommandJournalStableRotationCreatesThroughCapturedParent(t *testing.T) {
	root := t.TempDir()
	walDir := filepath.Join(root, "wal")
	journal, err := OpenCommandJournal(walDir, CommandJournalOptions{Lane: 5})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if _, err := journal.AppendCommand(CommandEnvelope{
		Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatal(err)
	}
	movedDir := filepath.Join(root, "wal-moved")
	originalOpenParent := openStableCommandWALParent
	openStableCommandWALParent = func(path string) (*os.File, error) {
		parent, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		if err := os.Rename(walDir, movedDir); err != nil {
			_ = parent.Close()
			return nil, err
		}
		if err := os.Mkdir(walDir, 0o700); err != nil {
			_ = parent.Close()
			return nil, err
		}
		return parent, nil
	}
	defer func() { openStableCommandWALParent = originalOpenParent }()
	before := journal.WriterDurabilityStats()
	rotation, err := journal.RotateActiveSegmentWithStableResources(false)
	if err != nil {
		t.Fatal(err)
	}
	defer rotation.Release()
	newName := CommandSegmentName(5, 2)
	if _, err := os.Stat(filepath.Join(movedDir, newName)); err != nil {
		t.Fatalf("captured-parent command WAL: %v", err)
	}
	if _, err := os.Stat(filepath.Join(walDir, newName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement path unexpectedly received command WAL: %v", err)
	}
	after := journal.WriterDurabilityStats()
	if after.FileSyncCalls != before.FileSyncCalls || after.DirectorySyncCalls != before.DirectorySyncCalls {
		t.Fatalf("syncCurrent=false rotation changed writer syncs: before=%+v after=%+v", before, after)
	}
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityCommandWALRotated, rootpublication.ReachabilityCommandWALActive)
	if err := builder.Add(rotation.TakeClosed()); err != nil {
		t.Fatal(err)
	}
	if err := builder.Add(rotation.TakeActive()); err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if err := set.FlushThrough(); err != nil {
		t.Fatal(err)
	}
	if err := set.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	stats := set.Stats(time.Now())
	if len(stats) != 1 || stats[0].Flushes != 2 || stats[0].Syncs != 2 || stats[0].NamespaceSyncs != 1 {
		t.Fatalf("stable rotation operation counts=%+v", stats)
	}
}

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

func BenchmarkStableCommandWALRotation(b *testing.B) {
	for _, syncCurrent := range []bool{false, true} {
		b.Run(fmt.Sprintf("sync_current=%t", syncCurrent), func(b *testing.B) {
			journal, err := OpenCommandJournal(b.TempDir(), CommandJournalOptions{Lane: 5})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = journal.Close() })
			beforeContentSyncs := journal.WriterDurabilityStats().FileSyncCalls
			var namespaceSyncs uint64
			envelope := CommandEnvelope{
				Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				if _, err := journal.AppendCommand(envelope); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				rotation, err := journal.RotateActiveSegmentWithStableResources(syncCurrent)
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityCommandWALRotated, rootpublication.ReachabilityCommandWALActive)
				if err := builder.Add(rotation.TakeClosed()); err != nil {
					rotation.Release()
					b.Fatal(err)
				}
				if err := builder.Add(rotation.TakeActive()); err != nil {
					rotation.Release()
					b.Fatal(err)
				}
				set, err := builder.Freeze()
				if err != nil {
					rotation.Release()
					b.Fatal(err)
				}
				rotation.Release()
				stats := set.Stats(time.Now())
				if len(stats) != 1 || stats[0].PendingCount != 2 || stats[0].NamespaceSyncs != 1 {
					set.Release()
					b.Fatalf("stable rotation operation counts=%+v", stats)
				}
				namespaceSyncs += stats[0].NamespaceSyncs
				set.Release()
				b.StartTimer()
			}
			contentSyncs := journal.WriterDurabilityStats().FileSyncCalls - beforeContentSyncs
			wantContentSyncs := uint64(0)
			if syncCurrent {
				wantContentSyncs = uint64(b.N)
			}
			if namespaceSyncs != uint64(b.N) || contentSyncs != wantContentSyncs {
				b.Fatalf("rotation counters namespace=%d content=%d want namespace=%d content=%d", namespaceSyncs, contentSyncs, b.N, wantContentSyncs)
			}
			b.ReportMetric(float64(namespaceSyncs)/float64(b.N), "stable-token-namespace-sync/op")
			b.ReportMetric(float64(contentSyncs)/float64(b.N), "producer-content-sync/op")
		})
	}
}
