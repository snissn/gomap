package db

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCommandWALCloseRetainsPinnedEmptyRotationAndReplays(t *testing.T) {
	if !commitlog.FixedWALCreationSupported() {
		t.Skip("requires fixed-container creation")
	}
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	key := []byte("large-relaxed-replay")
	value := bytes.Repeat([]byte("r"), commandWALDeferredPointBufferRetainSize+8192)
	if lsn, err := database.AppendRawKVPointCommandWALTrusted(commitlog.RawKVOpSet, key, value, false); err != nil || lsn != 1 {
		t.Fatalf("append oversized relaxed frame: lsn=%d err=%v", lsn, err)
	}
	proof, err := database.captureDurableWALCleanupProofV1()
	if err != nil || proof.cleanupThrough != 0 || proof.durableWALLSN != 0 {
		t.Fatalf("expected valid zero-frontier proof: proof=%+v err=%v", proof, err)
	}
	empty := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1))
	decisions, err := scanCommandWALSegmentsForCleanupProof(dir, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	candidate := emptyFixedCleanupDecision(t, decisions, empty)
	closeCommandWALCleanupDecisions(decisions)
	if !candidate.Covered || candidate.Active || candidate.Frames != 0 || candidate.LogicalSize != 0 {
		t.Fatalf("expected closed validated empty candidate: %+v", candidate)
	}
	debtOwnsIdentity := false
	database.commandWALDebt.mu.Lock()
	for _, entry := range database.commandWALDebt.entries {
		for _, token := range entry.rotationFiles {
			if rootpublication.SamePhysicalIdentity(token.Identity(), candidate.identity) {
				debtOwnsIdentity = true
			}
		}
	}
	database.commandWALDebt.mu.Unlock()
	if !debtOwnsIdentity {
		t.Fatal("fixture lost dependency-debt ownership of exact empty identity")
	}
	journal := database.commandJournal
	pinCount := uint64(0)
	if err := journal.WithCleanupSnapshot(proof.journal, func(registry *rootpublication.IdentityPinRegistry, _ commitlog.CommandJournalCleanupSnapshot) (bool, error) {
		pinCount = registry.PinCount(candidate.identity)
		if pinCount == 0 {
			t.Fatal("dependency-debt empty identity is not pinned")
		}
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Logf("validated cleanup proof: frontier=%d durable=%d empty_frames=%d debt_owned=%v exact_identity_pins=%d", proof.cleanupThrough, proof.durableWALLSN, candidate.Frames, debtOwnsIdentity, pinCount)
	unlinks := 0
	restoreObserver := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && (event.Point == durabilitycut.BeforeWALOrAssetUnlink || event.Point == durabilitycut.AfterWALOrAssetUnlink) {
			unlinks++
		}
		return nil
	})
	var restoreOnce sync.Once
	restore := func() { restoreOnce.Do(restoreObserver) }
	defer restore()
	if err := database.CleanupCommandWALCoveredSegments(true); !errors.Is(err, rootpublication.ErrResourcePinned) || !commandWALCleanupUnavailableOnly(err) {
		t.Fatalf("explicit cleanup lost trusted registry pin refusal: %v", err)
	}
	if unlinks != 0 || database.commandWALCleanupRemoved.Load() != 0 || database.commandWALCleanupNamespaceDirty.Load() {
		t.Fatal("pin refusal crossed unlink boundary")
	}
	t.Logf("explicit pin refusal retained files before unlink: events=%d removed=%d", unlinks, database.commandWALCleanupRemoved.Load())
	if err := database.Close(); err != nil {
		t.Fatalf("Close with conservative pinned retention: %v", err)
	}
	restore()
	if unlinks != 0 {
		t.Fatal("Close unlinked pinned identity before shutdown")
	}
	if _, err := os.Stat(empty); err != nil {
		t.Fatalf("Close did not retain pinned empty predecessor: %v", err)
	}
	if database.commandWALDebt.entryCountThrough(^uint64(0)) != 0 {
		t.Fatal("terminal shutdown retained dependency debt")
	}
	if _, err := journal.CaptureCleanupSnapshot(); err == nil {
		t.Fatal("Close left command journal live")
	}
	// Clean reopen proves Close flushed and closed the accepted relaxed frame;
	// it does not promote that frame to a power-loss durability guarantee.
	reopened, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	got, err := reopened.Get(key)
	if err != nil || !bytes.Equal(got, value) || reopened.State().AppliedCommandLSN < 1 {
		t.Fatalf("retained relaxed replay lost after reopen: bytes=%d err=%v", len(got), err)
	}
}

func TestCommandWALClosePropagatesPinJoinedWithCleanupFailure(t *testing.T) {
	if !commitlog.FixedWALCreationSupported() {
		t.Skip("requires fixed-container creation")
	}
	for _, point := range []durabilitycut.Point{durabilitycut.BeforeWALOrAssetUnlink, durabilitycut.BeforeDeletionDirectorySync} {
		t.Run(string(point), func(t *testing.T) {
			dir := t.TempDir()
			database, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = database.Close() })
			empty := database.commandJournal.WriterBufferStats().ActivePath
			// A completed explicit rotation leaves an unpinned empty candidate.
			if err := database.commandJournal.RotateActiveSegment(false); err != nil {
				t.Fatal(err)
			}
			failure := errors.New("independent cleanup failure")
			fired := false
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == point {
					fired = true
					return errors.Join(rootpublication.ErrResourcePinned, failure)
				}
				return nil
			})
			err = database.Close()
			restore()
			if !fired || !errors.Is(err, failure) || !errors.Is(err, rootpublication.ErrResourcePinned) {
				t.Fatalf("Close suppressed joined cleanup failure: fired=%v err=%v", fired, err)
			}
			if point == durabilitycut.BeforeWALOrAssetUnlink {
				if _, err := os.Stat(empty); err != nil {
					t.Fatalf("pre-unlink failure removed predecessor: %v", err)
				}
			} else if !errors.Is(err, ErrRecoveryRequired) || !database.commandWALCleanupNamespaceDirty.Load() || database.commandWALCleanupUnlinkedPending.Load() != 1 || database.commandWALCleanupRemoved.Load() != 0 {
				t.Fatalf("sync failure lost conservative namespace debt: %v", err)
			}
		})
	}
}
