package db

import (
	"bytes"
	"errors"
	"fmt"
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
	if err := prepareClosePinnedInitialFixedJournal(database); err != nil {
		t.Fatalf("establish fixed initial owner before accepted write: %v", err)
	}
	t.Logf("initial DB-owned writer: %+v", database.commandJournal.WriterBufferStats())
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
	if err := database.CleanupCommandWALCoveredSegments(true); err != rootpublication.ErrResourcePinned {
		t.Fatalf("explicit cleanup lost exact pin refusal: %v", err)
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
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("Close replay owner: %v", err)
		}
	}()
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
			if active := database.commandJournal.WriterBufferStats().ActivePath; active == empty {
				t.Fatal("rotation did not close the predecessor")
			}
			// Production may select stream containers on memory filesystems.
			// Establish this test's fixed-container input only after the original
			// rotation has closed it, and after proving no identity pins remain.
			file, err := os.Open(empty)
			if err != nil {
				t.Fatal(err)
			}
			info, statErr := file.Stat()
			identity, identityErr := rootpublication.StableIdentityFromFile(file)
			if err := errors.Join(statErr, identityErr, file.Close()); err != nil {
				t.Fatal(err)
			}
			reader, err := commitlog.NewReader(empty)
			if err != nil {
				t.Fatal(err)
			}
			emptyFixed := reader.IsEmptyFixedContainer()
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			if info.Size() != 0 && !emptyFixed {
				t.Fatal("predecessor is neither an empty stream nor a verified empty fixed container")
			}
			proof, err := database.captureDurableWALCleanupProofV1()
			if err != nil || proof.cleanupThrough != 0 || proof.durableWALLSN != 0 {
				t.Fatalf("expected original zero-frontier cleanup proof: %+v err=%v", proof, err)
			}
			var pins uint64
			if err := database.commandJournal.WithCleanupSnapshot(proof.journal, func(registry *rootpublication.IdentityPinRegistry, _ commitlog.CommandJournalCleanupSnapshot) (bool, error) {
				pins = registry.PinCount(identity)
				return false, nil
			}); err != nil {
				t.Fatal(err)
			}
			if pins != 0 {
				t.Fatalf("closed predecessor still has %d identity pins", pins)
			}
			if !emptyFixed {
				if proof.journal.SegmentSeq <= 1 || empty != filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, proof.journal.SegmentSeq-1)) {
					t.Fatal("predecessor does not match the completed original rotation")
				}
				// Existing explicit fixed setup; all inspection handles are closed.
				writeEmptyFixedCleanupSegment(t, dir, proof.journal.SegmentSeq-1, 8192)
			}
			decisions, err := scanCommandWALSegmentsForCleanupProof(dir, proof.cleanupThrough, proof.durableWALLSN, 0)
			if err != nil {
				closeCommandWALCleanupDecisions(decisions)
				t.Fatal(err)
			}
			func() {
				defer closeCommandWALCleanupDecisions(decisions)
				candidate := emptyFixedCleanupDecision(t, decisions, empty)
				if !candidate.Covered || candidate.Active || candidate.Frames != 0 || candidate.LogicalSize != 0 {
					t.Fatalf("expected closed validated empty cleanup candidate: %+v", candidate)
				}
			}()
			failure := errors.New("independent cleanup failure")
			fired := false
			restore := sync.OnceFunc(durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == point {
					fired = true
					return errors.Join(rootpublication.ErrResourcePinned, failure)
				}
				return nil
			}))
			defer restore()
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

// prepareClosePinnedInitialFixedJournal is only for a fresh, unshared fixture.
// No command, capture, snapshot or external borrower may precede this call.
// It establishes the actual DB-owned writer; seeding a different predecessor
// would not establish the oversized append's empty-rotation precondition.
func prepareClosePinnedInitialFixedJournal(database *DB) error {
	if database == nil || !database.commandWAL || database.commandJournal == nil {
		return errors.New("initial fixed owner requires a live command-WAL fixture")
	}
	noBorrowers := func() bool {
		database.commandWALRawBarrierMu.Lock()
		defer database.commandWALRawBarrierMu.Unlock()
		return len(database.commandWALRawBarriers) == 0 && database.legacyCommandProducers == 0 && database.semanticUnownedInFlight == 0 && !database.nativeSemanticActive.Load()
	}
	if !noBorrowers() {
		return errors.New("initial fixed owner requires no publisher or semantic borrowers")
	}
	unlock, err := database.LockCommandWALPublishWithBarriers()
	if err != nil {
		return err
	}
	defer unlock()
	if !noBorrowers() {
		return errors.New("initial fixed owner acquired a publisher or semantic borrower")
	}
	fresh := func() error {
		database.mu.RLock()
		applied := database.meta.AppliedCommandLSN
		database.mu.RUnlock()
		if applied != 0 || database.commandWALSessionAppliedLSN != 0 || database.commandWALDurableLSN.Load() != 0 || database.commandWALDurableMarkerLSN.Load() != 0 || database.commandWALFlushPoisoned.Load() {
			return errors.New("initial fixed owner requires zero unpoisoned LSN frontiers")
		}
		database.commandWALDebt.mu.Lock()
		emptyDebt := len(database.commandWALDebt.entries) == 0 && database.commandWALDebt.capturePins == 0 && database.commandWALDebt.deferredReleaseLSN == 0
		database.commandWALDebt.mu.Unlock()
		if !emptyDebt {
			return errors.New("initial fixed owner requires no dependency debt or captures")
		}
		if database.commandWALCleanupRemoved.Load() != 0 || database.commandWALCleanupNamespaceDirty.Load() || database.commandWALCleanupUnlinkedPending.Load() != 0 || database.commandWALClosedBytes.Load() != 0 {
			return errors.New("initial fixed owner requires an unchanged cleanup namespace")
		}
		if runtime := database.rootPublication; runtime != nil {
			runtime.mu.Lock()
			empty := len(runtime.visibleMembers) == 0 && runtime.visibleHead == 0 && len(runtime.debt) == 0 && len(runtime.seals) == 0 && runtime.activeSeal == nil && runtime.poison == nil
			runtime.mu.Unlock()
			if !empty {
				return errors.New("initial fixed owner requires no root-publication work")
			}
		}
		// This proof also rejects pending/ambiguous durable-root publication.
		// Its borrowed journal owner does not escape this validation call.
		proof, err := database.captureDurableWALCleanupProofV1()
		if err != nil {
			return err
		}
		if proof.cleanupThrough != 0 || proof.durableWALLSN != 0 {
			return errors.New("initial fixed owner requires a zero-frontier cleanup proof")
		}
		return nil
	}
	if err := fresh(); err != nil {
		return err
	}
	old := database.commandJournal
	snapshot, err := old.CaptureCleanupSnapshot()
	if err != nil {
		return err
	}
	path := filepath.Join(WALDirPath(database.dir), commitlog.CommandSegmentName(0, 1))
	validate := func(journal *commitlog.CommandJournal, snapshot commitlog.CommandJournalCleanupSnapshot) error {
		if snapshot.Lane != 0 || snapshot.SegmentSeq != 1 || snapshot.ActivePath != path || snapshot.ActiveBytes != 0 || snapshot.ActiveSegmentMaxLSN != 0 || snapshot.PendingStableRotation != 0 || snapshot.PendingSuccessor || journal.AppendedLSN() != 0 || journal.NextLSN() != 1 {
			return errors.New("initial fixed owner requires the original empty lane0/seq1")
		}
		return journal.WithCleanupSnapshot(snapshot, func(registry *rootpublication.IdentityPinRegistry, _ commitlog.CommandJournalCleanupSnapshot) (bool, error) {
			if registry.PinCount(snapshot.ActiveIdentity) != 0 {
				return false, errors.New("initial fixed owner requires an unpinned identity")
			}
			return false, nil
		})
	}
	if err := validate(old, snapshot); err != nil {
		return err
	}
	if err := old.WithCleanupScan(snapshot, func() error {
		return validateClosePinnedEmptyInitialSegment(database.dir, path, snapshot.ActiveIdentity)
	}); err != nil {
		return err
	}
	if err := old.Close(); err != nil {
		return fmt.Errorf("close original empty journal: %w", err)
	}
	database.commandJournal = nil
	if _, err := old.CaptureCleanupSnapshot(); err == nil {
		return errors.New("original empty journal remained live after Close")
	}
	if err := validateClosePinnedEmptyInitialSegment(database.dir, path, snapshot.ActiveIdentity); err != nil {
		return err
	}
	// The original owner is closed, all scan handles are closed, and its actual
	// empty/unpinned identity was proved before this existing factory is used.
	if err := seedClosePinnedEmptyFixedSegment(path); err != nil {
		return err
	}
	journal, err := commitlog.OpenCommandJournal(WALDirPath(database.dir), commitlog.CommandJournalOptions{
		MaxSegmentSize:                  database.walMaxSegmentBytes,
		SegmentTargetBytes:              database.commandWALSegmentTargetBytes,
		BufferSize:                      commandWALWriterBufferSize,
		DeferredCommandBufferSize:       commandWALDeferredPointBufferSize,
		DeferredCommandBufferRetainSize: commandWALDeferredPointBufferRetainSize,
		Compress:                        false,
		OnSegmentRotated:                database.observeCommandWALSegmentRotated,
		InitialLSN:                      0,
		SegmentSeq:                      1,
		CaptureStableResources:          true,
	})
	if err != nil {
		return err
	}
	// Immediate ownership installation leaves no fallible work or callback
	// between acquisition and the original DB cleanup becoming responsible.
	database.commandJournal = journal
	current, err := journal.CaptureCleanupSnapshot()
	if err != nil {
		return err
	}
	if journal == old {
		return errors.New("initial fixed owner did not change the closed owner")
	}
	if err := validate(journal, current); err != nil {
		return err
	}
	stats := journal.WriterBufferStats()
	if stats.Container != "fixed" || stats.BufferedWriterSize != commandWALWriterBufferSize || stats.CommandBufferLimit != commandWALDeferredPointBufferSize || stats.CommandBufferRetainLimit != commandWALDeferredPointBufferRetainSize {
		return fmt.Errorf("unexpected initial fixed writer configuration: %+v", stats)
	}
	return fresh()
}

// The aggregate cleanup-deletion scan intentionally omits empty streams. Use
// its exact-handle scanner directly to prove either a zero-byte stream or a
// positively validated empty fixed reserve, never an EOF-only nonempty file.
func validateClosePinnedEmptyInitialSegment(dir, path string, expected rootpublication.StableIdentity) (err error) {
	segments, err := listWALSegments(dir)
	if err != nil {
		return err
	}
	commands := 0
	for _, segment := range segments {
		if segment.valueLog || !isCommandWALLaneSegment(segment) {
			continue
		}
		commands++
		if segment.path != path || segment.lane != 0 || segment.seq != 1 {
			return errors.New("initial fixed owner has another command segment")
		}
	}
	if commands != 1 {
		return fmt.Errorf("initial fixed owner requires one command segment, got %d", commands)
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		return err
	}
	if !rootpublication.SamePhysicalIdentity(identity, expected) {
		return errors.New("initial fixed owner identity changed before seed")
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	scan, err := scanCommandWALSegmentFileWithOptions(file, 0, true, commandWALSegmentScanOptions{})
	if err != nil {
		return err
	}
	if scan.frames != 0 || scan.minLSN != 0 || scan.maxLSN != 0 || (info.Size() != 0 && !scan.emptyFixed) {
		return errors.New("initial fixed owner segment is not validated empty")
	}
	return nil
}

func seedClosePinnedEmptyFixedSegment(path string) (err error) {
	seed, err := commitlog.NewFixedWriterWithCapacity(path, 4_199_424)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, seed.Close()) }()
	if seed.BufferStats().Container != "fixed" {
		return errors.New("explicit empty fixed seed did not establish a fixed container")
	}
	return nil
}

func TestCommandWALCloseInitialFixedOwnerSetup(t *testing.T) {
	if !commitlog.FixedWALCreationSupported() {
		t.Skip("requires fixed-container creation")
	}
	database, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Errorf("Close setup owner: %v", err)
		}
	}()
	old := database.commandJournal
	if err := prepareClosePinnedInitialFixedJournal(database); err != nil {
		t.Fatal(err)
	}
	if database.commandJournal == old {
		t.Fatal("setup retained the original closed journal owner")
	}
	first, err := database.commandJournal.CaptureCleanupSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	// A second fresh setup covers an already-fixed empty input. The existing
	// factory must retain its exact physical identity instead of replacing it.
	if err := prepareClosePinnedInitialFixedJournal(database); err != nil {
		t.Fatal(err)
	}
	second, err := database.commandJournal.CaptureCleanupSnapshot()
	if err != nil || !rootpublication.SamePhysicalIdentity(first.ActiveIdentity, second.ActiveIdentity) {
		t.Fatalf("already-fixed empty identity changed: err=%v", err)
	}
	if database.commandJournal.WriterBufferStats().Container != "fixed" {
		t.Fatal("actual installed owner is not fixed")
	}
}

func TestCommandWALCloseInitialFixedOwnerRefusesAcceptedWrite(t *testing.T) {
	if !commitlog.FixedWALCreationSupported() {
		t.Skip("requires fixed-container creation")
	}
	database, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Errorf("Close accepted owner: %v", err)
		}
	}()
	if _, err := database.AppendRawKVPointCommandWALTrusted(commitlog.RawKVOpSet, []byte("accepted"), []byte("value"), false); err != nil {
		t.Fatal(err)
	}
	original := database.commandJournal
	before, err := original.CaptureCleanupSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := prepareClosePinnedInitialFixedJournal(database); err == nil {
		t.Fatal("setup accepted a nonempty journal")
	}
	after, err := original.CaptureCleanupSnapshot()
	if err != nil || database.commandJournal != original || !rootpublication.SamePhysicalIdentity(before.ActiveIdentity, after.ActiveIdentity) || original.AppendedLSN() != 1 {
		t.Fatalf("refusal changed accepted owner or identity: err=%v", err)
	}
}

func TestCommandWALCloseInitialFixedOwnerRefusesCapture(t *testing.T) {
	if !commitlog.FixedWALCreationSupported() {
		t.Skip("requires fixed-container creation")
	}
	for _, method := range []string{"prefix", "journal"} {
		t.Run(method, func(t *testing.T) {
			database, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := database.Close(); err != nil {
					t.Errorf("Close captured owner: %v", err)
				}
			}()
			if method == "prefix" {
				capture, _, err := database.CaptureCommandWALPrefix()
				if err != nil {
					t.Fatal(err)
				}
				// Consume the real debt/file and teardown pins before DB cleanup,
				// including after a fatal assertion on the refusal path.
				defer func() {
					if _, err := capture.Sync(); err != nil {
						t.Errorf("Sync captured empty prefix: %v", err)
					}
				}()
			} else {
				capture, err := database.commandJournal.CaptureSync(nil)
				if err != nil {
					t.Fatal(err)
				}
				// This genuine journal-only capture exercises the identity pin
				// refusal independently of the DB dependency-debt capture count.
				defer capture.Release()
			}
			original := database.commandJournal
			if err := prepareClosePinnedInitialFixedJournal(database); err == nil {
				t.Fatal("setup accepted a captured original journal")
			}
			if _, err := original.CaptureCleanupSnapshot(); err != nil || database.commandJournal != original {
				t.Fatalf("capture refusal closed or rebound original owner: %v", err)
			}
		})
	}
}

func TestCommandWALCloseInitialFixedSeed(t *testing.T) {
	if !commitlog.FixedWALCreationSupported() {
		t.Skip("requires fixed-container creation")
	}
	t.Run("empty-stream", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "empty.log")
		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		if err := seedClosePinnedEmptyFixedSegment(path); err != nil {
			t.Fatal(err)
		}
		reader, err := commitlog.NewReader(path)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := reader.Close(); err != nil {
				t.Errorf("Close seeded reader: %v", err)
			}
		}()
		if !reader.IsEmptyFixedContainer() {
			t.Fatal("closed empty stream was not initialized as a validated empty fixed reserve")
		}
	})
	t.Run("missing-parent", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "absent", "empty.log")
		if err := seedClosePinnedEmptyFixedSegment(path); err == nil {
			t.Fatal("factory failure did not propagate")
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("failed factory left an output file: %v", err)
		}
	})
}
