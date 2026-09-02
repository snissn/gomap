package commitlog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func testCommandJournalStableRotationCreatesThroughCapturedParent(t *testing.T) {
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
	if err := os.Rename(walDir, movedDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(walDir, 0o700); err != nil {
		t.Fatal(err)
	}
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
	active := rotation.TakeActive()
	if err := active.Namespace().Stabilize(); err != nil {
		active.Release()
		t.Fatal(err)
	}
	if err := builder.Add(active); err != nil {
		active.Release()
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

func testCommandJournalStableRotationUsesParentRefreshedByOrdinaryRotation(t *testing.T) {
	root := t.TempDir()
	walDir := filepath.Join(root, "wal")
	journal, err := OpenCommandJournal(walDir, CommandJournalOptions{Lane: 6})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	movedDir := filepath.Join(root, "wal-moved")
	if err := os.Rename(walDir, movedDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(walDir, 0o700); err != nil {
		t.Fatal(err)
	}
	injectedClose := errors.New("injected old command-WAL close failure")
	journal.writer.closeRotateFn = func(file *os.File) error {
		journal.writer.closeRotateFn = nil
		return errors.Join(file.Close(), injectedClose)
	}
	if err := journal.RotateActiveSegment(false); !errors.Is(err, injectedClose) {
		t.Fatalf("ordinary rotation error=%v want injected close failure", err)
	}
	if journal.segmentSeq != 2 || filepath.Base(journal.path) != CommandSegmentName(6, 2) {
		t.Fatalf("installed journal state seq=%d path=%q", journal.segmentSeq, journal.path)
	}
	rotation, err := journal.RotateActiveSegmentWithStableResources(false)
	if err != nil {
		t.Fatal(err)
	}
	defer rotation.Release()
	newName := CommandSegmentName(6, 3)
	if _, err := os.Stat(filepath.Join(walDir, newName)); err != nil {
		t.Fatalf("refreshed-parent command WAL: %v", err)
	}
	if _, err := os.Stat(filepath.Join(movedDir, newName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale retained parent unexpectedly received command WAL: %v", err)
	}
}

func testCommandJournalStableRotationDoesNotLoseClosedSegment(t *testing.T) {
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

func testCommandJournalStableRotationRejectsEachMissingSegment(t *testing.T) {
	buildRotation := func(t *testing.T) (*CommandJournal, *CommandJournalStableRotation) {
		t.Helper()
		journal, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{Lane: 11})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := journal.AppendCommand(CommandEnvelope{
			Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
		}); err != nil {
			_ = journal.Close()
			t.Fatal(err)
		}
		rotation, err := journal.RotateActiveSegmentWithStableResources(true)
		if err != nil {
			_ = journal.Close()
			t.Fatal(err)
		}
		return journal, rotation
	}

	t.Run("complete", func(t *testing.T) {
		journal, rotation := buildRotation(t)
		defer journal.Close()
		builder := rootpublication.NewStableResourceSetBuilder(
			rootpublication.ReachabilityCommandWALActive,
			rootpublication.ReachabilityCommandWALRotated,
		)
		defer builder.Abandon()
		if err := builder.Add(rotation.TakeClosed()); err != nil {
			t.Fatal(err)
		}
		if err := builder.Add(rotation.TakeActive()); err != nil {
			t.Fatal(err)
		}
		resources, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		resources.Release()
	})

	for _, omitted := range []rootpublication.ReachabilityField{
		rootpublication.ReachabilityCommandWALActive,
		rootpublication.ReachabilityCommandWALRotated,
	} {
		t.Run("omit-"+string(omitted), func(t *testing.T) {
			journal, rotation := buildRotation(t)
			defer journal.Close()
			builder := rootpublication.NewStableResourceSetBuilder(
				rootpublication.ReachabilityCommandWALActive,
				rootpublication.ReachabilityCommandWALRotated,
			)
			defer builder.Abandon()
			closed := rotation.TakeClosed()
			active := rotation.TakeActive()
			if omitted == rootpublication.ReachabilityCommandWALActive {
				active.Release()
				if err := builder.Add(closed); err != nil {
					t.Fatal(err)
				}
			} else {
				closed.Release()
				if err := builder.Add(active); err != nil {
					t.Fatal(err)
				}
			}
			resources, err := builder.Freeze()
			if resources != nil {
				resources.Release()
			}
			if !errors.Is(err, rootpublication.ErrUnresolvedResource) || !strings.Contains(err.Error(), string(omitted)) {
				t.Fatalf("omitted %s resources=%v err=%v want exact ErrUnresolvedResource", omitted, resources, err)
			}
		})
	}
}

func testCommandJournalStableRotationFrontierUsesSegmentAppendsNotOwnerCursor(t *testing.T) {
	dir := t.TempDir()
	journal, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	appendedLSN, err := journal.AppendCommand(CommandEnvelope{
		Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatal(err)
	}
	reservedOnly, err := journal.owner.reserveLSN()
	if err != nil {
		t.Fatal(err)
	}
	if reservedOnly <= appendedLSN {
		t.Fatalf("reserved LSN=%d want greater than appended %d", reservedOnly, appendedLSN)
	}
	rotation, err := journal.RotateActiveSegmentWithStableResources(false)
	if err != nil {
		t.Fatal(err)
	}
	defer rotation.Release()
	if got := rotation.Closed.Frontier().MaxLSN; got != appendedLSN {
		t.Fatalf("closed segment MaxLSN=%d want appended LSN %d, excluding reserved-only %d", got, appendedLSN, reservedOnly)
	}
}

func testCommandJournalAutomaticRotationCapturesStableResources(t *testing.T) {
	dir := t.TempDir()
	journal, err := OpenCommandJournal(dir, CommandJournalOptions{
		Lane: 2, SegmentTargetBytes: 1, CaptureStableResources: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	var lastLSN uint64
	for i := 0; i < 2; i++ {
		lastLSN, err = journal.AppendCommand(CommandEnvelope{
			Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := journal.RotateActiveSegment(false); !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("legacy rotation before pending drain error=%v want ErrResourceOwnership", err)
	}
	rotation, err := journal.RotateActiveSegmentWithStableResources(false)
	if !errors.Is(err, rootpublication.ErrResourceOwnership) {
		if rotation != nil {
			rotation.Release()
		}
		t.Fatalf("stable rotation before pending drain error=%v want ErrResourceOwnership", err)
	}
	if rotation != nil {
		rotation.Release()
		t.Fatal("stable rotation before pending drain returned owned resources")
	}
	if _, err := journal.AppendCommand(CommandEnvelope{
		Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
	}); !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("second automatic rotation before drain error=%v want ErrResourceOwnership", err)
	}
	rotations, err := journal.TakePendingStableRotations()
	if err != nil {
		t.Fatal(err)
	}
	if len(rotations) == 0 {
		t.Fatal("automatic size rotation returned no stable resources")
	}
	for _, rotation := range rotations {
		if rotation == nil || rotation.Closed == nil || rotation.Active == nil {
			t.Fatalf("incomplete automatic stable rotation: %+v", rotation)
		}
		if rotation.Closed.Frontier().MaxLSN == 0 || rotation.Closed.Frontier().Bytes == 0 {
			t.Fatalf("closed automatic rotation lost frontier: %+v", rotation.Closed.Frontier())
		}
		if rotation.Active.Namespace() == nil {
			t.Fatal("automatic successor has no stable namespace token")
		}
	}
	last := rotations[len(rotations)-1]
	if got := last.Active.Frontier(); got.MaxLSN != lastLSN || got.Bytes <= segmentHeaderSize {
		t.Fatalf("active automatic rotation frontier=%+v want triggering LSN %d and appended bytes", got, lastLSN)
	}
	for _, rotation := range rotations {
		rotation.Release()
	}
	again, err := journal.TakePendingStableRotations()
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Fatalf("stable rotations transferred twice: %d", len(again))
	}
}

func testCommandJournalStableRotationNamespaceFailureKeepsOldWriterActive(t *testing.T) {
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

func benchmarkStableCommandWALRotation(b *testing.B) {
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
				active := rotation.TakeActive()
				if err := active.Namespace().Stabilize(); err != nil {
					active.Release()
					rotation.Release()
					b.Fatal(err)
				}
				if err := builder.Add(active); err != nil {
					active.Release()
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
