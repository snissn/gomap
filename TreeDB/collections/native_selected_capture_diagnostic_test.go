package collections

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// This fixture measures selected token metadata admission, not the producer's
// scratch, registry, or graph capacity. It is deliberately local to the test.
type selectedCaptureBudget struct{ cap, used uint64 }
type selectedCaptureLease struct {
	budget *selectedCaptureBudget
	bytes  uint64
}

func (b *selectedCaptureBudget) AcquireRetention(n uint64) (retainedalloc.Lease, error) {
	if n > b.cap-b.used {
		return nil, retainedalloc.ErrCapacity
	}
	b.used += n
	return &selectedCaptureLease{budget: b, bytes: n}, nil
}
func (l *selectedCaptureLease) Resize(n uint64) error {
	if n > l.bytes && n-l.bytes > l.budget.cap-l.budget.used {
		return retainedalloc.ErrCapacity
	}
	l.budget.used = l.budget.used - l.bytes + n
	l.bytes = n
	return nil
}
func (l *selectedCaptureLease) Close() {
	l.budget.used -= l.bytes
	l.bytes = 0
}

// The original appender is already synced and its exact parent, child and
// construction pin are still live inside validation. This experiment uses
// independently charged selected metadata and an extra pin; it is not a
// complete prepared-output receipt, publication authority, or a graph fix.
// The original generic resources and their sync callback are untouched.
func TestNativeSelectedCaptureMultiAppenderDiagnostic(t *testing.T) {
	if !ordinaryColumnStableAuthorityEnabled() {
		t.Skip("stable namespace authority unavailable")
	}
	root := filepath.Join(t.TempDir(), "column-assets")
	cfg := ColumnStoreConfig{Enabled: true, AssetManager: &ColumnAssetManagerConfig{
		Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "selected-capture",
	}}
	registry := rootpublication.NewIdentityPinRegistry()
	t.Cleanup(registry.Close)
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &selectedCaptureBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	initialCharge := owner.Bytes()
	var cleanupFailure error
	t.Cleanup(func() {
		if cleanupFailure != nil {
			enrollment.CloseAfterCleanup(cleanupFailure)
			return
		}
		enrollment.Close()
		if err := owner.Close(); err != nil {
			t.Errorf("close selected owner: %v", err)
		}
	})
	session := newColumnPhysicalAssetAppendSessionWithStableResources(root, cfg, registry)
	t.Cleanup(func() { _ = session.abort() })
	first, err := session.appendKinds(1, []columnPhysicalAssetAppendItem{{
		payload: []byte("first-asset"), kind: ColumnAssetKindTCS1PartImage, generation: 1, partID: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	firstAppender := session.active
	var selected *rootpublication.StableResourceToken
	var selectedIdentity rootpublication.StableIdentity
	t.Cleanup(func() {
		if selected != nil {
			if err := selected.ReleaseWithError(); err != nil {
				cleanupFailure = errors.Join(cleanupFailure, err)
				t.Errorf("release selected token: %v", err)
			}
		}
	})
	// Capture errors are recorded separately; they cannot fail the original
	// already accepted producer. No selected token is used for a sync.
	var sideErr error
	if err := session.closeActiveWithStableValidation(func(original *rootpublication.StableResourceSet) error {
		ref := first[0]
		if e := validateStableColumnResourcesMatchPrepared([]ColumnPreparedAsset{{Ref: ref}}, original); e != nil {
			return e
		}
		namespace, e := registry.NewStableNamespaceTokenForKnownLinkWithMetadata(
			stableColumnAssetNamespaceSpec(firstAppender.stableParent, firstAppender.file, ref), &owner)
		if e != nil {
			sideErr = e
			cleanupFailure = errors.Join(cleanupFailure, e)
			return nil
		}
		defer func() {
			if releaseErr := namespace.ReleaseWithError(); releaseErr != nil {
				sideErr = errors.Join(sideErr, releaseErr)
				cleanupFailure = errors.Join(cleanupFailure, releaseErr)
			}
		}()
		identity, e := rootpublication.StableIdentityFromFile(firstAppender.file)
		if e == nil {
			selectedIdentity = identity
			descriptors := original.PhysicalDescriptors()
			if len(descriptors) != 1 || !rootpublication.SamePhysicalIdentity(descriptors[0].Identity(), identity) ||
				descriptors[0].Frontier().Bytes != uint64(ref.Offset+ref.Length) {
				return errors.New("original resource does not match the exact appender identity")
			}
			e = registry.Observe(identity)
		}
		if e != nil {
			sideErr = e
			return nil
		}
		kind, field, _, e := stableColumnAssetResourceClassification(ref.Kind)
		if e == nil {
			selected, e = NewStableColumnAssetResourceToken(rootpublication.StableResourceSpec{
				MetadataOwner: &owner, Kind: kind, LogicalLane: ref.Namespace, ResourceID: fmt.Sprint(ref.FileID),
				Generation: uint64(ref.FileID), DiagnosticPath: stableColumnAssetDiagnosticPath(ref), File: firstAppender.file,
				Frontier: rootpublication.DurableFrontier{Bytes: uint64(ref.Offset + ref.Length)},
				Digest:   stableColumnSegmentDigest(ref), Reachability: field, Namespace: namespace,
				LogicalObligations: []rootpublication.StableLogicalObligation{stableColumnLogicalObligation(ref, field)},
				PinRegistry:        registry, OriginalObservation: registry, ContentSynced: true,
			})
		}
		if e != nil {
			e = errors.Join(e, registry.Unobserve(identity))
			// A construction error may itself retain exact cleanup custody.
			cleanupFailure = errors.Join(cleanupFailure, e)
		}
		sideErr = e
		return nil
	}); err != nil || sideErr != nil || selected == nil {
		t.Fatalf("first close=%v side=%v selected=%v", err, sideErr, selected != nil)
	}
	if !rootpublication.SamePhysicalIdentity(selected.Identity(), selectedIdentity) ||
		selected.Frontier().Bytes != uint64(first[0].Offset+first[0].Length) ||
		len(selected.LogicalObligations()) != 1 ||
		selected.LogicalObligations()[0] != stableColumnLogicalObligation(first[0], rootpublication.ReachabilityColumnManifest) {
		t.Fatal("selected token did not capture the same original ref and frontier")
	}
	second, err := session.appendKinds(2, []columnPhysicalAssetAppendItem{{
		payload: []byte("second-asset"), kind: ColumnAssetKindTCS1PartImage, generation: 1, partID: 2,
	}})
	if err != nil {
		t.Fatal(err)
	}
	secondAppender := session.active
	before, pinsBefore := owner.Bytes(), registry.ActivePins()
	budget.cap = budget.used // Force side-only admission refusal for the next appender.
	_, resources, err := session.closeWithStableResourcesValidated(func(_ *rootpublication.StableResourceSet) error {
		unexpected, captureErr := registry.NewStableNamespaceTokenForKnownLinkWithMetadata(
			stableColumnAssetNamespaceSpec(secondAppender.stableParent, secondAppender.file, second[0]), &owner)
		if unexpected != nil {
			releaseErr := unexpected.ReleaseWithError()
			captureErr = errors.Join(captureErr, releaseErr)
			cleanupFailure = errors.Join(cleanupFailure, releaseErr)
		}
		sideErr = captureErr
		return nil
	})
	resourcesReleased := false
	if resources != nil {
		held := resources
		t.Cleanup(func() {
			if !resourcesReleased {
				resourcesReleased = true
				if releaseErr := held.ReleaseWithError(); releaseErr != nil {
					cleanupFailure = errors.Join(cleanupFailure, releaseErr)
					t.Errorf("release original resources: %v", releaseErr)
				}
			}
		})
	}
	if err != nil || resources == nil || !errors.Is(sideErr, retainedalloc.ErrCapacity) {
		t.Fatalf("second close=%v side=%v resources=%v", err, sideErr, resources != nil)
	}
	if owner.Bytes() != before {
		t.Fatalf("refused side admission changed charge: before=%d after=%d", before, owner.Bytes())
	}
	// The producer's second original token legitimately changes the pin count.
	if registry.ActivePins() < pinsBefore {
		t.Fatal("side refusal lost existing producer or selected pin")
	}
	assets := []ColumnPreparedAsset{{Ref: first[0]}, {Ref: second[0]}}
	if err := validateStableColumnResourcesMatchPrepared(assets, resources); err != nil {
		t.Fatalf("original resource closure after side refusal: %v", err)
	}
	resourcesReleased = true
	if releaseErr := resources.ReleaseWithError(); releaseErr != nil {
		cleanupFailure = errors.Join(cleanupFailure, releaseErr)
		t.Fatalf("release original resources: %v", releaseErr)
	}
	if pins := registry.ActivePins(); pins != 1 {
		t.Fatalf("pins after original release=%d, want only the selected pin", pins)
	}
	if observations := registry.ObserverCount(selectedIdentity); observations != 1 {
		t.Fatalf("observations after original release=%d, want selected observation", observations)
	}
	read := make([]byte, len("first-asset"))
	if _, err := selected.ReadAt(read, first[0].Offset); err != nil || string(read) != "first-asset" {
		t.Fatalf("selected pinned read=%q err=%v", read, err)
	}
	heldSelected := selected
	selected = nil // Attempt this exact release only once, including on failure.
	if err := heldSelected.ReleaseWithError(); err != nil {
		cleanupFailure = err
		t.Fatalf("release selected token: %v", err)
	}
	if pins := registry.ActivePins(); pins != 0 {
		t.Fatalf("pins after selected release=%d, want zero", pins)
	}
	if observations := registry.ObserverCount(selectedIdentity); observations != 0 {
		t.Fatalf("observations after selected release=%d, want zero", observations)
	}
	if got := owner.Bytes(); got != initialCharge {
		t.Fatalf("selected admission after release=%d, initial=%d", got, initialCharge)
	}
}
