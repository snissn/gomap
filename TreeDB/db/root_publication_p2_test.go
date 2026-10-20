package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestMaintenanceDrainUsesOriginalRetiredGeneration(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	for _, failClose := range []bool{false, true} {
		t.Run(fmt.Sprint(failClose), func(t *testing.T) {
			database, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			if err := database.Set([]byte("key"), []byte("old")); err != nil {
				t.Fatal(err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			old := database.AcquireSnapshot()
			defer old.Close()
			original := old.idx
			if err := database.VacuumIndexOnline(context.Background()); err != nil {
				t.Fatal(err)
			}
			lease, err := database.AcquireStableResourceCaptureLease()
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Release()
			if err := database.DrainRetiredIndexGenerations(lease); err != nil {
				t.Fatal(err)
			}
			if original.closeCompleted {
				t.Fatal("pinned generation was disposed")
			}
			if got, err := old.Get([]byte("key")); err != nil || string(got) != "old" {
				t.Fatalf("pinned read=(%q,%v)", got, err)
			}
			if err := old.Close(); err != nil {
				t.Fatal(err)
			}
			if failClose {
				if err := original.pager.WithStableResourceFile(func(f *os.File) error { return f.Close() }); err != nil {
					t.Fatal(err)
				}
			}
			err = database.DrainRetiredIndexGenerations(lease)
			if failClose {
				if !errors.Is(err, os.ErrClosed) || !errors.Is(err, ErrRecoveryRequired) {
					t.Fatalf("original disposal failure: %v", err)
				}
				if !errors.Is(database.backgroundError(), os.ErrClosed) || !errors.Is(database.DrainRetiredIndexGenerations(lease), ErrRecoveryRequired) {
					t.Fatal("original failure was lost or retried")
				}
			} else if err != nil || !original.closeCompleted {
				t.Fatalf("eligible original not disposed: %v closed=%t", err, original.closeCompleted)
			}
			lease.Release()
			if err := database.Close(); failClose != errors.Is(err, os.ErrClosed) {
				t.Fatalf("terminal cleanup: %v", err)
			}
		})
	}
}

func TestAcceptedSnapshotReceiptRefusalAbortsOriginalCandidate(t *testing.T) {
	refused := errors.New("test: receipt refused")
	for _, adoption := range []bool{false, true} {
		t.Run(fmt.Sprint(adoption), func(t *testing.T) {
			database, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			basis := database.AcquireSnapshot()
			defer basis.Close()
			group, err := database.BeginRootPublicationBuildGroupFromSnapshot(basis)
			if err != nil {
				t.Fatal(err)
			}
			var receipt *Snapshot
			if err := group.PrepareAcceptedSnapshot(func(SnapshotAllocationSizes) error {
				if !adoption {
					return refused
				}
				return nil
			}, func(s *Snapshot) error { receipt = s; return refused }, func() error {
				if receipt != nil {
					return receipt.Close()
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			batch := database.NewPhysicalBatch().(*Batch)
			defer batch.Close()
			if err := batch.Set([]byte("private"), []byte("refused")); err != nil {
				t.Fatal(err)
			}
			if err := batch.SetRootPublicationBuildGroup(group, true); err != nil {
				t.Fatal(err)
			}
			err = batch.Write()
			if !errors.Is(err, refused) || errors.Is(err, rootpublication.ErrDurableRootOwnership) {
				t.Fatalf("refusal cleanup: %v", err)
			}
			if group.Accepted() || group.registered || len(group.tracker.Pages()) != 0 {
				t.Fatal("private original ownership retained after successful abort")
			}

			if got, err := database.Get([]byte("private")); err != nil || len(got) != 0 {
				t.Fatalf("refused private DATA key visible: %q %v", got, err)
			}
			if err := group.Close(); err != nil {
				t.Fatalf("repeat group Close: %v", err)
			}
			if err := database.Set([]byte("next"), []byte("accepted")); err != nil {
				t.Fatal(err)
			}
			if err := basis.Close(); err != nil {
				t.Fatal(err)
			}
			if err := database.Close(); err != nil {
				t.Fatalf("Close after refused receipt: %v", err)
			}
		})
	}
}

func TestCheckpointRejectsMixedSystemRoot(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	basis, err := database.AcquireSnapshotWithAllocationAdmission(func(SnapshotAllocationSizes) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer basis.Close()
	// This test changes only semantic system identity, leaving the user root and
	// index identical, which the old build-group gate incorrectly accepted.
	database.mu.Lock()
	database.meta.SystemRootPageID++
	database.mu.Unlock()
	group, err := database.BeginRootPublicationBuildGroupFromSnapshot(basis)
	if group != nil {
		_ = group.Close()
	}
	database.mu.Lock()
	database.meta.SystemRootPageID--
	database.mu.Unlock()
	if !errors.Is(err, ErrRootPublicationBasisMismatch) {
		t.Fatalf("mixed root set accepted: %v", err)
	}
}

func TestCheckpointRejectsPublishedSemanticSystemRoot(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	basis, err := database.AcquireSnapshotWithAllocationAdmission(func(SnapshotAllocationSizes) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer basis.Close()
	if _, err := database.PublishSystemRootIterator(mustFrozenSystemMemtable(t, "sys/p2/catalog", "new schema").NewIterator(nil, nil)); err != nil {
		t.Fatal(err)
	}
	if database.State().RootPageID != basis.treeRoot {
		t.Fatal("fixture changed the user root")
	}
	group, err := database.BeginRootPublicationBuildGroupFromSnapshot(basis)
	if group != nil {
		_ = group.Close()
	}
	if !errors.Is(err, ErrRootPublicationBasisMismatch) {
		t.Fatalf("post-cut semantic system root accepted: %v", err)
	}
}

func TestAcceptedSnapshotReceiptRetainsCandidateTuple(t *testing.T) {
	t.Run("DATA", func(t *testing.T) {
		database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true, NegativeLookupFilterBytes: 1024})
		if err != nil {
			t.Fatal(err)
		}
		defer database.Close()
		basis, err := database.AcquireSnapshotWithAllocationAdmission(func(SnapshotAllocationSizes) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		defer basis.Close()
		group, err := database.BeginRootPublicationBuildGroupFromSnapshot(basis)
		if err != nil {
			t.Fatal(err)
		}
		defer group.Close()
		var receipt *Snapshot
		defer func() {
			if receipt != nil {
				_ = receipt.Close()
			}
		}()
		if err := group.PrepareAcceptedSnapshot(func(SnapshotAllocationSizes) error { return nil }, func(s *Snapshot) error { receipt = s; return nil }, func() error {
			if receipt != nil {
				return receipt.Close()
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		batch := database.NewPhysicalBatch().(*Batch)
		defer batch.Close()
		if err := batch.Set([]byte("key"), []byte("accepted")); err != nil {
			t.Fatal(err)
		}
		if err := batch.SetRootPublicationBuildGroup(group, true); err != nil {
			t.Fatal(err)
		}
		var prepared bool
		database.testDurableRootCandidatePreparedHook = func() {
			prepared = true
			if receipt == nil || receipt.state == database.state.Load() {
				t.Error("receipt was not constructed from private candidate")
			}
			if receipt != nil && receipt.systemRootPublishEpoch != database.snapshotPublicationEpoch(receipt.idx, receipt.state) {
				t.Error("candidate receipt has wrong predicted epoch")
			}
		}
		if err := batch.WriteSync(); err != nil {
			t.Fatal(err)
		}
		database.testDurableRootCandidatePreparedHook = nil
		if !prepared || !group.Accepted() || group.AcceptedSnapshot() != receipt {
			t.Fatal("missing exact accepted generation")
		}
		visible := database.snapshotViewRO.Load()
		if visible.negativeFilter == nil {
			t.Fatal("candidate dropped qualified negative filter")
		}
		if got := reflect.ValueOf(&receipt.tree).Elem().FieldByName("negativeFilter").Pointer(); got != reflect.ValueOf(visible.negativeFilter).Pointer() {
			t.Fatal("receipt filter differs from accepted tuple")
		}
		if receipt.state != visible.state || receipt.idx != visible.idx || receipt.systemRootPublishEpoch != visible.systemRootPublishEpoch {
			t.Fatal("receipt tuple differs from accepted visible tuple")
		}
		token, ok := receipt.StateToken()
		if !ok {
			t.Fatal("missing receipt token")
		}
		if err := group.Close(); err != nil {
			t.Fatal(err)
		}
		if err := database.Set([]byte("key"), []byte("later")); err != nil {
			t.Fatal(err)
		}
		if got, err := receipt.Get([]byte("key")); err != nil || string(got) != "accepted" {
			t.Fatalf("receipt recaptured later state: %q %v", got, err)
		}
		if got, ok := receipt.StateToken(); !ok || got != token {
			t.Fatal("receipt generation changed")
		}
	})
}

func TestCloseBorrowsAdmittedTeardownGuard(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	basis, err := database.AcquireSnapshotWithAllocationAdmission(func(SnapshotAllocationSizes) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer basis.Close()
	lease, err := database.AcquireStableResourceCaptureLease()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	closed := make(chan error, 1)
	go func() { closed <- database.Close() }()
	deadline := time.After(5 * time.Second)
	for {
		if database.closing.Load() {
			if !database.teardownMu.TryRLock() {
				break
			}
			database.teardownMu.RUnlock()
		}
		select {
		case <-deadline:
			t.Fatal("Close did not queue teardown")
		default:
			runtime.Gosched()
		}
	}
	done := make(chan error, 1)
	go func() {
		group, err := database.BeginRootPublicationBuildGroupBorrowingLease(basis, lease)
		if err == nil {
			err = group.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nested admitted group recursively acquired queued teardown")
	}
	if err := lease.ValidateDBV1(database); err != nil {
		t.Fatal("borrower released original admission")
	}
	lease.Release()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not join outside required worker locks")
	}
	if _, err := database.BeginRootPublicationBuildGroupBorrowingLease(basis, lease); !errors.Is(err, ErrClosed) {
		t.Fatalf("expired borrowed admission: %v", err)
	}
}
