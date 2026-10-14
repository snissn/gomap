package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Observe the SAME original producer, then consume its real root edge. This
// injects production Seal/Abort refusals without a fake constructor or hook
// replacing Abort. The physical owner must discover all sibling failures.
func TestPrimaryConstructionV6OriginalSiblingFailureCustody(t *testing.T) {
	for _, path := range []string{"apply", "copy", "build"} {
		t.Run(path, func(t *testing.T) {
			database, err := Open(Options{Dir: t.TempDir(), IndexPrimaryDirectory: true, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			idx := database.idx.Load()
			var original *rootpublication.DurableRootTransaction
			idx.zipper.SetPrimaryConstructorV6(func(a *primaryarena.Arena, w *iterator.OrdinalScanWork) (primaryarena.ReadRootConstructionV6, bool, error) {
				output, ready, err := rootpublication.NewPrimaryConstructionTransactionV6(a, w)
				if !ready || err != nil {
					return output, ready, err
				}
				original = output.(*rootpublication.DurableRootTransaction)
				if ready, err := a.Drop(original.PrimaryConstructionReferenceV6(), nil); !ready || err != nil {
					t.Fatal(ready, err)
				}
				return output, true, nil
			})
			switch path {
			case "apply":
				batch := database.NewPhysicalBatch().(*Batch)
				defer batch.Close()
				if err := batch.Set([]byte("failed-original"), []byte("value")); err != nil {
					t.Fatal(err)
				}
				err = batch.Write()
			case "copy":
				_, err = idx.zipper.CopyPrimaryRoot(database.State().RootPageID)
			case "build":
				_, err = idx.zipper.BuildPrimaryRoot(database.State().RootPageID, database.State().CommitSeq)
			}
			if !errors.Is(err, rootpublication.ErrDurableRootOwnership) {
				t.Fatalf("actual original failure %v", err)
			}
			if original == nil || idx.primaryOwner.failedTransactions != original || original.Owner() != rootpublication.ResourceOwnerBuilder {
				t.Fatal("original sibling failure was orphaned or refunded")
			}
			if original.PrimaryConstructionReferenceV6() == (primaryarena.Ref{}) {
				t.Fatal("original construction erased")
			}
			if closeErr := database.Close(); !errors.Is(closeErr, rootpublication.ErrDurableRootOwnership) {
				t.Fatalf("shutdown lost original error: %v", closeErr)
			}
			if idx.primaryOwner.metadataCharge == 0 || idx.primaryOwner.arena.MetadataOwner().Bytes() == 0 {
				t.Fatal("failed physical last-owner refunded")
			}
		})
	}
}

// The production candidate has already Bound/Detached its SAME original. An
// activation refusal followed by a genuine original Abort refusal must retain
// candidate resources; the caller's prepare-error defers cannot release them.
func TestPrimaryConstructionV6BoundCandidateFailureCustody(t *testing.T) {
	for _, stage := range []string{"state-drop", "constructor-drop"} {
		t.Run(stage, func(t *testing.T) { testPrimaryConstructionV6BoundCandidateFailureCustody(t, stage) })
	}
}
func testPrimaryConstructionV6BoundCandidateFailureCustody(t *testing.T, stage string) {
	database, err := Open(Options{Dir: t.TempDir(), IndexPrimaryDirectory: true, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	ptrs := appendPointersInNewSegment(t, database.dir, 0, 27, 2700, 1, func(int) []byte { return []byte("persistent-count-failure") })
	seed := database.NewPhysicalBatch().(*Batch)
	if err := seed.SetPointer([]byte("seed-count"), ptrs[0]); err != nil {
		t.Fatal(err)
	}
	if err := seed.WriteSync(); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	database.valueLogRefTracker.invalidate()
	idx := database.idx.Load()
	var original *rootpublication.DurableRootTransaction
	var evidence *candidateValueLogRefCountsV1
	var firstSlot *valueLogRefCountSlot
	var recordCharge uint64
	idx.zipper.SetPrimaryConstructorV6(func(a *primaryarena.Arena, w *iterator.OrdinalScanWork) (primaryarena.ReadRootConstructionV6, bool, error) {
		output, ready, err := rootpublication.NewPrimaryConstructionTransactionV6(a, w)
		if ready && err == nil {
			original = output.(*rootpublication.DurableRootTransaction)
		}
		return output, ready, err
	})
	database.testFailDurableRootVisibleInstall.Store(true)
	database.testDurableRootCandidatePreparedHook = func() {
		if original == nil || original.Owner() != rootpublication.ResourceOwnerCandidate {
			t.Fatal("actual candidate not admitted")
		}
		member, ok := original.PublicationProjectionV6().(*rootPublicationVisibleMemberV1)
		if !ok || member.install == nil || member.install.vlogRefCounts == nil {
			t.Fatal("original bound candidate missing actual fallback scan")
		}
		evidence = member.install.vlogRefCounts
		if evidence.counts.get(ptrs[0].FileID) != 2 || evidence.counts.metadata != database.valueLogIdentityPins.MetadataOwner() || evidence.counts.charge == 0 {
			t.Fatal("real pointer scan not originally admitted")
		}
		firstSlot = &evidence.counts.slots[0]
		recordCharge = evidence.counts.charge + evidence.metadataCharge
		if ready, err := idx.primary.Drop(original.PrimaryConstructionReferenceV6(), nil); !ready || err != nil {
			t.Fatal(ready, err)
		}
		if stage == "state-drop" {
			if ready, err := idx.primary.Drop(original.PrimaryConstructionReferenceV6(), nil); !ready || err != nil {
				t.Fatal(ready, err)
			}
		}
	}
	batch := database.NewPhysicalBatch().(*Batch)
	if err := batch.SetPointer([]byte("bound-failure"), ptrs[0]); err != nil {
		t.Fatal(err)
	}
	err = batch.Write()
	_ = batch.Close()
	if !errors.Is(err, rootpublication.ErrDurableRootOwnership) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("production abandon failure %v", err)
	}
	if idx.primaryOwner.failedTransactions != original || original.Owner() != rootpublication.ResourceOwnerCandidate {
		t.Fatal("candidate original or its resources refunded")
	}
	if original.PreparedPrimary() == nil {
		t.Fatal("original bound resources/projection erased")
	}
	if evidence == nil || len(evidence.counts.slots) == 0 || &evidence.counts.slots[0] != firstSlot || evidence.counts.get(ptrs[0].FileID) != 2 || evidence.metadataCharge == 0 {
		t.Fatal("failed actual Abort disposed original candidate count payload")
	}
	drops := idx.primary.Counters().RootDrops
	firstRefusal := original.PublicationCandidateV6().Abandon()
	if firstRefusal == nil || original.PublicationCandidateV6().Abandon() != firstRefusal || idx.primary.Counters().RootDrops != drops {
		t.Fatal("duplicate Abort replayed original physical disposal or replaced first receipt")
	}
	done := make(chan error, 1)
	go func() { done <- database.Close() }()
	select {
	case err := <-done:
		if !errors.Is(err, rootpublication.ErrDurableRootOwnership) {
			t.Fatalf("shutdown lost original %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bound failure deadlocked teardown")
	}
	if idx.primaryOwner.metadataCharge == 0 || idx.primaryOwner.arena.MetadataOwner().Bytes() < recordCharge || &evidence.counts.slots[0] != firstSlot {
		t.Fatal("failed bound original count backing refunded")
	}
	if again := database.Close(); !errors.Is(again, rootpublication.ErrDurableRootOwnership) {
		t.Fatal("repeat Close lost first refusal")
	}
	if &evidence.counts.slots[0] != firstSlot || evidence.counts.get(ptrs[0].FileID) != 2 {
		t.Fatal("repeat Close replayed disposal")
	}
}

// The authority role belongs to the exact pinned PRIMARY three-page slot,
// not a fictional write to the DATA pager's legacy one-page metadata slot.
func TestPrimaryConstructionV6AuthorityCutsUseActualPrimary(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), IndexPrimaryDirectory: true, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var mu sync.Mutex
	var points []durabilitycut.Point
	var observationErr error
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Root != database.dir || event.Resource != durabilitycut.ResourceMeta {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		expectedPath := filepath.Join(database.dir, primaryIndexFileName)
		if event.Path != expectedPath || event.StablePath == nil || event.Length != 3*int64(page.PageSize) ||
			(event.Offset != 2*int64(page.PageSize) && event.Offset != 5*int64(page.PageSize)) {
			observationErr = errors.New("authority cut did not name actual PRIMARY slot")
			return observationErr
		}
		pinned, err := event.StablePath.Stat()
		if err != nil {
			observationErr = err
			return err
		}
		actual, err := os.Stat(expectedPath)
		if err != nil {
			observationErr = err
			return err
		}
		if !os.SameFile(pinned, actual) {
			observationErr = errors.New("authority used a different file identity")
			return observationErr
		}
		points = append(points, event.Point)
		return nil
	})
	observerActive := true
	defer func() {
		if observerActive {
			restore()
		}
	}()
	batch := database.NewPhysicalBatch().(*Batch)
	if err := batch.Set([]byte("authority"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	err = batch.WriteSync()
	_ = batch.Close()
	restore()
	observerActive = false
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if observationErr != nil {
		t.Fatal(observationErr)
	}
	want := []durabilitycut.Point{durabilitycut.BeforeMetaWrite, durabilitycut.AfterMetaWrite, durabilitycut.BeforeMetaSync, durabilitycut.AfterMetaSync}
	if len(points) != len(want) {
		t.Fatalf("authority points %v", points)
	}
	for n := range want {
		if points[n] != want[n] {
			t.Fatalf("authority point order %v", points)
		}
	}
}
