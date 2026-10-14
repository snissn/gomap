package db

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// The selected closure belongs to an actual DATA DB. A later capture has no
// PRIMARY and no producer addition; its predecessor is the allocation authority.
func TestCaptureDurableRootUsesSelectedPredecessorOwner(t *testing.T) {
	database, path := openDurableRootClosureDB3928(t)
	ptrs := appendPointersInNewSegment(t, database.dir, 0, 1, 10000, 1, func(int) []byte { return bytes.Repeat([]byte("v"), 8192) })
	if err := database.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	batch := database.NewBatch().(*Batch)
	if err := batch.SetPointer([]byte("owned-predecessor-pointer"), ptrs[0]); err != nil {
		t.Fatal(err)
	}
	if err := batch.WriteSync(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot := database.AcquireSnapshot()
	entry, readErr := snapshot.GetEntryExact([]byte("owned-predecessor-pointer"))
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if readErr != nil || entry.ValuePtr.FileID == 0 {
		t.Fatal("control must capture a real persistent value-log dependency")
	}

	if database.idx.Load().primary != nil {
		t.Fatal("control requires the original DATA route")
	}
	original := durableRootClosureSet3928(t, path, durableRootClosureObligation3928(1))
	owner := database.valueLogIdentityPins.MetadataOwner()
	base, err := rootpublication.ImportStableResourceSetMetadata(owner, original)
	if err != nil {
		t.Fatal(err)
	}
	if err := original.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := base.ReleaseWithError(); err != nil {
			t.Error(err)
		}
	}()
	candidate, err := database.captureDurableRootResourcesFromBaseV1(
		database.idx.Load(), database.meta, nil, base, nil,
		rootpublication.StableLogicalObligationRequirements{}, rootpublication.StableLogicalObligationMutation{},
		rootpublication.StableLogicalObligationMutation{}, rootpublication.StableResourceClosureWork{},
		nil, false, nil,
	)
	if candidate != nil {
		defer func() {
			if err := candidate.ReleaseWithError(); err != nil {
				t.Error(err)
			}
		}()
	}
	if err != nil {
		t.Fatal(err)
	}
	if candidate == nil || candidate.MetadataOwner() != owner {
		t.Fatalf("candidate owner=%p want exact predecessor owner=%p", candidate.MetadataOwner(), owner)
	}
}
