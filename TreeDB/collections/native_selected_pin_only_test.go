package collections

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// This is a test-only single-appender capture. It exercises independent pin
// and admitted metadata lifetime, not a complete receipt, publication
// authority, future build reservation, or graph handoff.
func TestNativeSelectedPinSurvivesOriginalObservation(t *testing.T) {
	if !ordinaryColumnStableAuthorityEnabled() {
		t.Skip("stable namespace authority unavailable")
	}
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(budget.Close)
	registry := rootpublication.NewIdentityPinRegistry()
	t.Cleanup(registry.Close)
	owner, err := newNativePreparedSideOwner(budget, registry, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var cleanupErr error
	var sideErr error
	t.Cleanup(func() {
		if !owner.finished {
			// An unsuccessful constructor can return an error retaining its
			// partial physical operand. Preserve that error and the charged
			// owner if side allocations remain after our one release attempt.
			if cleanupErr == nil && sideErr != nil && owner.metadata.Bytes() != owner.idle {
				cleanupErr = sideErr
			}
			if err := owner.finish(cleanupErr); err != nil {
				t.Errorf("finish side owner: %v", err)
			}
		}
	})
	root := filepath.Join(t.TempDir(), "column-assets")
	cfg := ColumnStoreConfig{Enabled: true, AssetManager: &ColumnAssetManagerConfig{
		Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "pin-only",
	}}
	session := newColumnPhysicalAssetAppendSessionWithStableResources(root, cfg, registry)
	sessionAborted := false
	t.Cleanup(func() {
		if !sessionAborted {
			sessionAborted = true
			if e := session.abort(); e != nil {
				t.Errorf("abort original session: %v", e)
			}
		}
	})
	refs, err := session.appendKinds(1, []columnPhysicalAssetAppendItem{{
		payload: []byte("pinned-asset"), kind: ColumnAssetKindTCS1PartImage, generation: 1, partID: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	appender := session.active
	var selected *rootpublication.StableResourceToken
	var identity rootpublication.StableIdentity
	t.Cleanup(func() {
		if selected != nil {
			held := selected
			selected = nil
			if err := held.ReleaseWithError(); err != nil {
				cleanupErr = errors.Join(cleanupErr, err)
				t.Errorf("release selected: %v", err)
			}
		}
	})
	_, original, err := session.closeWithStableResourcesValidated(func(resources *rootpublication.StableResourceSet) error {
		ref := refs[0]
		if e := validateStableColumnResourcesMatchPrepared([]ColumnPreparedAsset{{Ref: ref}}, resources); e != nil {
			return e
		}
		identity, sideErr = rootpublication.StableIdentityFromFile(appender.file)
		if sideErr != nil {
			return nil
		}
		desc := resources.PhysicalDescriptors()
		if len(desc) != 1 || !rootpublication.SamePhysicalIdentity(desc[0].Identity(), identity) || desc[0].Frontier().Bytes != uint64(ref.Offset+ref.Length) {
			return errors.New("original resource does not match exact appender")
		}
		namespace, e := registry.NewStableNamespaceTokenForKnownLinkWithMetadata(stableColumnAssetNamespaceSpec(appender.stableParent, appender.file, ref), &owner.metadata)
		if e != nil {
			sideErr = e
			return nil
		}
		defer func() {
			if e := namespace.ReleaseWithError(); e != nil {
				sideErr = errors.Join(sideErr, e)
				cleanupErr = errors.Join(cleanupErr, e)
			}
		}()
		kind, field, _, e := stableColumnAssetResourceClassification(ref.Kind)
		if e == nil {
			// The producer's observation is still live here. The selected token
			// takes its own pin but does not acquire or own another observation.
			selected, e = NewStableColumnAssetResourceToken(rootpublication.StableResourceSpec{
				MetadataOwner: &owner.metadata, Kind: kind, LogicalLane: ref.Namespace, ResourceID: fmt.Sprint(ref.FileID),
				Generation: uint64(ref.FileID), DiagnosticPath: stableColumnAssetDiagnosticPath(ref), File: appender.file,
				Frontier: rootpublication.DurableFrontier{Bytes: uint64(ref.Offset + ref.Length)},
				Digest:   stableColumnSegmentDigest(ref), Reachability: field, Namespace: namespace,
				LogicalObligations: []rootpublication.StableLogicalObligation{stableColumnLogicalObligation(ref, field)},
				PinRegistry:        registry, ContentSynced: true,
			})
		}
		sideErr = e
		return nil
	})
	if original != nil {
		t.Cleanup(func() {
			if original != nil {
				held := original
				original = nil
				if e := held.ReleaseWithError(); e != nil {
					t.Errorf("release original: %v", e)
				}
			}
		})
	}
	if err != nil || sideErr != nil || original == nil || selected == nil {
		t.Fatalf("original=%v side=%v resources=%v selected=%v", err, sideErr, original != nil, selected != nil)
	}
	held := original
	original = nil
	if err := held.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	if got := registry.ObserverCount(identity); got != 0 {
		t.Fatalf("observations after producer release=%d", got)
	}
	if got := registry.ActivePins(); got != 1 {
		t.Fatalf("pins after producer release=%d", got)
	}
	if deletion, err := registry.BeginDelete(identity); !errors.Is(err, rootpublication.ErrResourcePinned) {
		if deletion != nil {
			deletion.Abort()
		}
		t.Fatalf("delete with selected pin: %v", err)
	}
	data := make([]byte, len("pinned-asset"))
	if _, err := selected.ReadAt(data, refs[0].Offset); err != nil || string(data) != "pinned-asset" {
		t.Fatalf("pinned read=%q err=%v", data, err)
	}
	heldSelected := selected
	selected = nil
	if err := heldSelected.ReleaseWithError(); err != nil {
		cleanupErr = errors.Join(cleanupErr, err)
		t.Fatal(err)
	}
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("pins after selected release=%d", got)
	}
	if got := owner.metadata.Bytes(); got != owner.idle {
		t.Fatalf("side metadata=%d idle=%d", got, owner.idle)
	}
	sessionAborted = true
	if err := session.abort(); err != nil {
		t.Fatalf("abort original session: %v", err)
	}
	if err := owner.finish(nil); err != nil {
		t.Fatal(err)
	}
}
