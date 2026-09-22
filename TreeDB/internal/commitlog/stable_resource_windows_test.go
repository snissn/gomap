//go:build windows

package commitlog

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCommandJournalCaptureStableResourcesUsesCreateOnlyWindowsEvidence(t *testing.T) {
	journal, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{Lane: 5, CaptureStableResources: true})
	if err != nil {
		t.Fatalf("OpenCommandJournal stable capture: %v", err)
	}
	defer journal.Close()
	if journal.stableParent == nil || journal.stableParentErr != nil {
		t.Fatalf("stable parent=%v err=%v", journal.stableParent, journal.stableParentErr)
	}
	stats := journal.WriterDurabilityStats()
	if stats.DirectorySyncCalls != 1 || stats.DirectorySyncErrors != 0 {
		t.Fatalf("initial exact-child namespace evidence stats=%+v", stats)
	}
}

func TestCommandJournalStableRotationUsesCreateOnlyWindowsEvidence(t *testing.T) {
	for _, syncCurrent := range []bool{false, true} {
		t.Run(map[bool]string{false: "flush", true: "sync"}[syncCurrent], func(t *testing.T) {
			journal, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{Lane: 6})
			if err != nil {
				t.Fatal(err)
			}
			defer journal.Close()
			if _, err := journal.AppendCommand(CommandEnvelope{
				Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
			}); err != nil {
				t.Fatal(err)
			}
			rotation, err := journal.RotateActiveSegmentWithStableResources(syncCurrent)
			if err != nil {
				t.Fatalf("stable rotation: %v", err)
			}
			defer rotation.Release()
			if rotation.Closed == nil || rotation.Active == nil {
				t.Fatalf("rotation=%+v", rotation)
			}
			if err := rotation.Active.Namespace().Stabilize(); err != nil {
				t.Fatalf("idempotent active namespace stabilize: %v", err)
			}
			builder := rootpublication.NewStableResourceSetBuilder(
				rootpublication.ReachabilityCommandWALRotated,
				rootpublication.ReachabilityCommandWALActive,
			)
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
				t.Fatalf("FlushThrough exact Windows handles: %v", err)
			}
			if err := set.SyncThrough(); err != nil {
				t.Fatalf("SyncThrough exact Windows handles: %v", err)
			}
		})
	}
}

func TestCommandJournalStableRotationCreatesThroughCapturedParentWindows(t *testing.T) {
	journal, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{Lane: 7})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	parentIdentity, err := rootpublication.StableIdentityFromFile(journal.stableParent)
	if err != nil {
		t.Fatal(err)
	}
	rotation, err := journal.RotateActiveSegmentWithStableResources(false)
	if err != nil {
		t.Fatal(err)
	}
	defer rotation.Release()
	got := rotation.Active.Namespace().ParentIdentity()
	got.Generation = 0
	if got != parentIdentity {
		t.Fatalf("active namespace parent identity=%+v want captured parent %+v", got, parentIdentity)
	}
}
