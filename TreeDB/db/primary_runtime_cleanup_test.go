package db

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Fault the real original current edge after fresh initialization or actual
// capsule recovery. Terminal disposal must keep the same containing runtime
// and dependency sets, without replaying any root already consumed.
func TestOriginalPrimaryRuntimeLastRootRefusalRetainsDescriptor(t *testing.T) {
	for _, recovered := range []bool{false, true} {
		name := "fresh"
		if recovered {
			name = "recovered"
		}
		t.Run(name, func(t *testing.T) {
			opts := Options{Dir: t.TempDir(), IndexPrimaryDirectory: true, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
			d, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			if recovered {
				if err = d.Close(); err != nil {
					t.Fatal(err)
				}
				d, err = Open(opts)
				if err != nil {
					t.Fatal(err)
				}
			}
			r := d.durableRoot.primary
			a := d.idx.Load().primary
			owner, charge := r.metadata, r.metadataCharge
			state := d.state.Load()
			if charge == 0 || state.primaryRoot == nil {
				t.Fatal("missing actual original admitted descriptor/current root")
			}
			sets := d.durableRoot.slotResources
			if consumed, err := a.Drop(state.primaryRoot.ref, nil); !consumed || err != nil {
				t.Fatal(consumed, err)
			}
			err = d.Close()
			if !errors.Is(err, primaryarena.ErrStale) || r.disposalErr == nil {
				t.Fatalf("lost real late root refusal: %v", err)
			}
			first := r.disposalErr
			drops := a.Counters().RootDrops
			if r.metadata != owner || r.metadataCharge != charge || owner.Bytes() < charge || r.current == nil || d.durableRoot.primary != r {
				t.Fatal("actual failed descriptor/current custody or charge was refunded")
			}
			for i, set := range sets {
				if set != nil && (r.slotResources[i] != set || set.Owner() == rootpublication.ResourceOwnerReleased) {
					t.Fatal("root refusal consumed an original dependency set")
				}
			}
			if again := releasePrimaryRuntimeV5(a, r); again != first || a.Counters().RootDrops != drops {
				t.Fatal("duplicate runtime disposal replayed roots or replaced first receipt")
			}
			if again := d.Close(); !errors.Is(again, primaryarena.ErrStale) || a.Counters().RootDrops != drops {
				t.Fatal("duplicate terminal close replayed root disposal")
			}
			var receipt *originalDBCleanupFailure
			if !errors.As(d.backgroundError(), &receipt) || receipt != &r.disposalFailure {
				t.Fatal("original DB error ownership lost same runtime")
			}
		})
	}
}

func originalRecoverySetWithPhysicalCloseFault(t *testing.T) *rootpublication.StableResourceSet {
	t.Helper()
	registry := rootpublication.NewIdentityPinRegistry()
	t.Cleanup(registry.Close)
	file, err := os.OpenFile(filepath.Join(t.TempDir(), "selected.bin"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	if _, err = file.Write([]byte("original")); err != nil {
		t.Fatal(err)
	}
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
		MetadataOwner: registry.MetadataOwner(), PinRegistry: registry, OriginalObservation: registry,
		Kind: rootpublication.ResourceIndex, LogicalLane: "db/index", ResourceID: "selected.bin", Generation: 1,
		DiagnosticPath: "selected.bin", File: file, Frontier: rootpublication.DurableFrontier{Bytes: 8}, Reachability: rootpublication.ReachabilityIndexFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	builder, err := rootpublication.NewStableResourceSetBuilderWithMetadata(registry.MetadataOwner())
	if err != nil {
		t.Fatal(err)
	}
	if err = builder.Add(token); err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	// Close the actual duplicated production token handle, not a replacement
	// Release callback. The original selected Set's last physical Close refuses.
	if err = token.WithPinnedFile(func(pinned *os.File) error { return pinned.Close() }); err != nil {
		t.Fatal(err)
	}
	return set
}

func TestOriginalRecoverySelectorPhysicalRefusalCannotAcceptAnotherSlot(t *testing.T) {
	fixture := newDurableRootFixtureV1(t)
	older := fixture.addCandidate(t, 0)
	newest := fixture.addCandidate(t, 1)
	fixture.store.Pages[newest.Record.UserRootPageID][0] ^= 1
	set := originalRecoverySetWithPhysicalCloseFault(t)
	calls := 0
	selected, err := selectDurableRootCandidates(fixture.store, func(candidate durableMetaCandidateV1) (durableRootSelectionV1, error) {
		return validateDurableMetaCandidateV1(fixture.store, uint64(len(fixture.store.Pages)), candidate, func(*rootpublication.DependencyManifestV1) (*rootpublication.StableResourceSet, error) {
			calls++
			if candidate.slot == newest.Slot {
				return set, nil
			}
			return nil, nil
		})
	})
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("physical refusal became fallback success at commit %d (older %d): %v", selected.Record.CommitSeq, older.Record.CommitSeq, err)
	}
	if calls != 1 || selected.SlotResources[newest.Slot] != set || set.Owner() != rootpublication.ResourceOwnerReleased {
		t.Fatal("selector lost actual first-refusal resource custody or continued selection")
	}
	first := set.ReleaseWithError()
	if first == nil || first != set.ReleaseWithError() {
		t.Fatal("selected original physical disposal replayed or replaced first receipt")
	}
}
