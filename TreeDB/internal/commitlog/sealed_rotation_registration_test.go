package commitlog

import (
	"errors"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func registeredSecondFile(t *testing.T, durability CommandDurabilityClass) (*CommandJournal, *CommandJournalStableRotation) {
	t.Helper()
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{
		SegmentSeq: 1, SegmentTargetBytes: 1, CaptureStableResources: true, PreferExt4Stream: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	if j.writer.fixed != nil || !j.writer.containerSelection.ext4Stream {
		t.Skip("requires production ext4 stream")
	}
	env := CommandEnvelope{Version: CommandFrameVersionV2, DurabilityClass: durability,
		Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1}
	for seq := uint64(1); seq <= 2; seq++ {
		if got, err := j.AppendCommand(env); err != nil || got != seq {
			t.Fatalf("append lsn=%d err=%v", got, err)
		}
	}
	r, err := j.TakePendingStableRotations()
	if err != nil || len(r) != 1 {
		t.Fatalf("rotation count=%d err=%v", len(r), err)
	}
	t.Cleanup(r[0].Release)
	return j, r[0]
}

func TestSealedDebtOnlyOriginalOrderedRegistrations(t *testing.T) {
	j, r := registeredSecondFile(t, CommandDurabilityRelaxed)
	alias, err := j.stableSegmentToken(j.writer.f, j.path, j.segmentSeq, r.Active.Reachability(), r.Active.Frontier(), r.Active.Namespace(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer alias.Release()
	if alias.Identity() != r.Active.Identity() || alias.Generation() != r.Active.Generation() || alias.Digest() != r.Active.Digest() {
		t.Fatal("alias does not reproduce the same underlying physical registration fields")
	}
	if _, vol := j.CheckpointExt4StreamState(false); vol != (rootpublication.StableIdentity{}) {
		t.Fatal("live active received terminal credit")
	}
	stream, vol := j.CheckpointExt4StreamState(true)
	original := r.Closed.Identity()
	if vol == (rootpublication.StableIdentity{}) || vol.Platform != original.Platform || vol.VolumeID != original.VolumeID {
		t.Fatalf("unsealed stream volume %+v, predecessor %+v", vol, original)
	}
	if covered, err := stream.Covers(r.Closed, r.Active); covered || err != nil {
		t.Fatalf("stream volume without a seal granted terminal credit: %v, %v", covered, err)
	}
	stats := j.WriterDurabilityStats()
	if err := j.RotateActiveSegment(true); err != nil {
		t.Fatal(err)
	}
	after := j.WriterDurabilityStats()
	if after.FileSyncCalls != stats.FileSyncCalls+1 || after.DirectorySyncCalls != stats.DirectorySyncCalls+1 {
		t.Fatalf("actual checked rotation before=%+v after=%+v", stats, after)
	}
	proof, vol := j.CheckpointExt4StreamState(false)
	if vol == (rootpublication.StableIdentity{}) {
		t.Fatal("terminal credit missing")
	}
	for _, tc := range []struct {
		name          string
		first, second *rootpublication.StableResourceToken
		want          bool
	}{
		{"originals", r.Closed, r.Active, true}, {"fresh-same-file", r.Closed, alias, false}, {"originals-reversed", r.Active, r.Closed, false}, {"removed-first", nil, r.Active, false},
	} {
		if ok, err := proof.Covers(tc.first, tc.second); err != nil || ok != tc.want {
			t.Errorf("%s got %v err=%v", tc.name, ok, err)
		}
	}
	if err := j.RotateActiveSegment(true); err != nil {
		t.Fatal(err)
	}
	if ok, err := proof.Covers(r.Closed, r.Active); err != nil || ok {
		t.Fatalf("stale generation credit=%t err=%v", ok, err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSealedDebtRefusesUnprovedCutAndPreservesStickyError(t *testing.T) {
	for _, tc := range []string{"unsynced", "old-close", "old-file-sync", "namespace", "durable-size-rotation", "later-sticky", "released-registration"} {
		t.Run(tc, func(t *testing.T) {
			durability := CommandDurabilityRelaxed
			if tc == "durable-size-rotation" {
				durability = CommandDurabilityDurable
			}
			j, r := registeredSecondFile(t, durability)
			fail := errors.New(tc)
			switch tc {
			case "old-close":
				j.writer.closeRotateFn = func(f *os.File) error { return errors.Join(f.Close(), fail) }
			case "old-file-sync":
				j.writer.syncFn = func(*os.File) error { return fail }
			}
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				if tc == "namespace" && e.Point == durabilitycut.BeforeNewFileDirectorySync {
					return fail
				}
				return nil
			})
			err := j.RotateActiveSegment(tc != "unsynced")
			restore()
			proof, vol := j.CheckpointExt4StreamState(false)
			switch tc {
			case "later-sticky", "released-registration":
				if err != nil || vol == (rootpublication.StableIdentity{}) {
					t.Fatalf("initial checked seal err=%v vol=%+v", err, vol)
				}
				expected := fail
				if tc == "released-registration" {
					r.Active.Release()
					expected = rootpublication.ErrResourceOwnership
				} else {
					j.writer.syncFn = func(*os.File) error { return fail }
					if err := j.Sync(); !errors.Is(err, fail) {
						t.Fatalf("independent physical failure=%v", err)
					}
				}
				if ok, err := proof.Covers(r.Closed, r.Active); !ok || !errors.Is(err, expected) {
					t.Fatalf("accepted unusable credit covered=%v err=%v", ok, err)
				}
			default:
				wantErr := tc == "old-close" || tc == "old-file-sync" || tc == "namespace"
				if wantErr && !errors.Is(err, fail) || !wantErr && err != nil {
					t.Fatalf("rotation cause %v", err)
				}
				if vol != (rootpublication.StableIdentity{}) {
					t.Fatal("unproved rotation returned credit")
				}
			}
			err = j.Close()
			if tc == "old-file-sync" || tc == "later-sticky" {
				if !errors.Is(err, fail) {
					t.Fatalf("checked close lost sticky error %v", err)
				}
			} else if err != nil {
				t.Fatalf("checked close: %v", err)
			}
		})
	}
}
