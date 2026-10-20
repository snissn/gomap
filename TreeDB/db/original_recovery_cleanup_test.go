package db

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

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

func TestNilDBCloseBeforePublicationCallbackGuard(t *testing.T) {
	var db *DB
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
