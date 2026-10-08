package db

import (
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/allocclass"
	"github.com/snissn/gomap/TreeDB/internal/residentcredit"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Match the production caller's existing root serializer. This helper adds no
// reader registration, replacement creator or alternate maintenance admission.
func captureDurableSnapshotForLifetimeTest(database *DB, idx *indexGen) *Snapshot {
	database.rootReuseMu.Lock()
	defer database.rootReuseMu.Unlock()
	return database.acquireDurableCandidateStableIndexSnapshotV1(idx, false)
}

func TestDurableCandidateSnapshotRetainsExactConstructorBeforeAdmission(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	idx := database.idx.Load()
	original := idx.creator
	before := original.Stats()
	births := original.OwnerStats().Births
	snapshotClass, err := allocclass.ClassBytes(uint64(unsafe.Sizeof(Snapshot{})), true)
	if err != nil {
		t.Fatal(err)
	}
	class := snapshotClass // inline original capsule is part of the same actual allocation
	snapshot := captureDurableSnapshotForLifetimeTest(database, idx)
	if snapshot == nil {
		t.Fatal("maintenance Snapshot unavailable")
	}
	t.Cleanup(func() {
		if err := snapshot.Close(); err != nil {
			t.Error(err)
		}
	})
	if snapshot.pagerCreator != original || snapshot.registryID != 0 || snapshot.stableIndexCaptureCounter != &database.durableCandidateIndexCaptures {
		t.Fatal("maintenance holder changed constructor or reader/counter authority")
	}
	if got := original.Stats(); got.Retained != before.Retained+2 || got.Bytes != before.Bytes+class {
		t.Fatalf("Snapshot constructor was not precharged on original scope: before=%+v after=%+v class=%d", before, got, class)
	}
	if got := original.OwnerStats().Births; got != births+class {
		t.Fatalf("births=%d want %d", got, births+class)
	}
	if got := database.durableCandidateIndexCaptures.Load(); got != 1 {
		t.Fatalf("captures=%d want 1", got)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if snapshot.pagerCreator != nil || snapshot.idx != nil || snapshot.db != nil || snapshot.treePager != nil {
		t.Fatal("completed Snapshot retains constructor/operational aliases")
	}
	if got := original.Stats().Retained; got != before.Retained {
		t.Fatalf("original retain=%d want %d", got, before.Retained)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if database.durableCandidateIndexCaptures.Load() != 0 || original.Stats().Retained != before.Retained {
		t.Fatal("repeated Close consumed original creator/counter twice")
	}
}

func TestDurableCandidateSnapshotCreatorSurvivesDatabaseClose(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	snapshot := captureDurableSnapshotForLifetimeTest(database, database.idx.Load())
	if snapshot == nil {
		t.Fatal("maintenance Snapshot unavailable")
	}
	t.Cleanup(func() {
		if err := snapshot.Close(); err != nil {
			t.Error(err)
		}
	})
	original := snapshot.pagerCreator
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if original.Stats().Closed || original.OwnerStats().Live == 0 {
		t.Fatal("DB Close discarded held original Snapshot creator")
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if !original.Stats().Closed || original.OwnerStats().Live != 0 || database.durableCandidateIndexCaptures.Load() != 0 {
		t.Fatalf("actual last maintenance edge not discharged: scope=%+v owner=%+v captures=%d", original.Stats(), original.OwnerStats(), database.durableCandidateIndexCaptures.Load())
	}
}

func TestDurableCandidateSnapshotCutoverRefusalBalancesOriginalLifetime(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		database.vacuumCutoverInProgress.Store(false)
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	idx := database.idx.Load()
	original := idx.creator
	before := original.Stats()
	births := original.OwnerStats().Births
	snapshotClass, err := allocclass.ClassBytes(uint64(unsafe.Sizeof(Snapshot{})), true)
	if err != nil {
		t.Fatal(err)
	}
	class := snapshotClass // inline original capsule is part of the same actual allocation
	database.vacuumCutoverInProgress.Store(true)
	for i := 0; i < 3; i++ {
		if snapshot := captureDurableSnapshotForLifetimeTest(database, idx); snapshot != nil {
			_ = snapshot.Close()
			t.Fatal("cutover admitted maintenance capture")
		}
		if database.durableCandidateIndexCaptures.Load() != 0 || original.Stats().Retained != before.Retained {
			t.Fatal("cutover refusal retained constructor/counter admission")
		}
	}
	if got := original.OwnerStats().Births; got != births+3*class {
		t.Fatalf("failed actual births refunded or doublecharged: %d want %d", got, births+3*class)
	}
	database.vacuumCutoverInProgress.Store(false)
	snapshot := captureDurableSnapshotForLifetimeTest(database, idx)
	if snapshot == nil || snapshot.pagerCreator != original {
		t.Fatal("balanced cutover refusal changed next constructor")
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDurableCandidateSnapshotRefusesBeforeBirthOnClosedOrStrictOwner(t *testing.T) {
	for _, mode := range []string{"closed", "strict-envelope"} {
		t.Run(mode, func(t *testing.T) {
			database, err := Open(Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := database.Close(); err != nil {
					t.Error(err)
				}
			})
			idx := database.idx.Load()
			original := idx.creator
			owner := (*residentcredit.Owner)(database.nativePublicationResident)
			if mode == "closed" {
				owner.Close()
			} else {
				strict, err := owner.NewScope()
				if err != nil {
					t.Fatal(err)
				}
				defer strict.ReleaseStableMetadata()
				stats := owner.Stats()
				if err := strict.ReserveStableMetadata(stats.Limit - stats.Live); err != nil {
					t.Fatal(err)
				}
			}
			beforeScope, beforeOwner := original.Stats(), owner.Stats()
			if snapshot := captureDurableSnapshotForLifetimeTest(database, idx); snapshot != nil {
				_ = snapshot.Close()
				t.Fatal("terminal/bounded constructor admitted a Snapshot birth")
			}
			if original.Stats() != beforeScope || owner.Stats() != beforeOwner || database.durableCandidateIndexCaptures.Load() != 0 {
				t.Fatalf("refusal mutated original constructor/counter: scope=%+v owner=%+v", original.Stats(), owner.Stats())
			}
		})
	}
}

func TestDurableCandidateSnapshotCapturesRealStableIndexToken(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("exact stable relative namespace unavailable on this platform")
	}
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	idx := database.idx.Load()
	original := idx.creator
	before := original.Stats().Retained
	snapshot := captureDurableSnapshotForLifetimeTest(database, idx)
	if snapshot == nil {
		t.Fatal("production maintenance Snapshot unavailable")
	}
	t.Cleanup(func() {
		if err := snapshot.Close(); err != nil {
			t.Error(err)
		}
	})
	token, err := snapshot.CaptureStableIndexFileResource()
	if err != nil {
		t.Fatalf("real maintenance index capture: %v", err)
	}
	if token == nil {
		t.Fatal("real capture returned no token")
	}
	t.Cleanup(func() {
		if err := token.Release(); err != nil {
			t.Error(err)
		}
	})
	if snapshot.pagerCreator != original || !snapshot.stableIndexCaptureTransferred || database.durableCandidateIndexCaptures.Load() != 1 {
		t.Fatal("token did not transfer exact original maintenance responsibility")
	}
	if token.Identity() == (rootpublication.StableIdentity{}) || token.Frontier().Bytes == 0 || token.Kind() != rootpublication.ResourceIndex {
		t.Fatal("capture lacks actual installed index identity/frontier")
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if database.durableCandidateIndexCaptures.Load() != 1 {
		t.Fatal("Snapshot Close consumed token-owned capture counter")
	}
	if err := token.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if err := token.Release(); err != nil {
		t.Fatal(err)
	}
	if err := token.Release(); err != nil {
		t.Fatal(err)
	}
	if database.durableCandidateIndexCaptures.Load() != 0 || original.Stats().Retained != before {
		t.Fatalf("token terminal responsibility leaked or replayed: captures=%d scope=%+v", database.durableCandidateIndexCaptures.Load(), original.Stats())
	}
}
