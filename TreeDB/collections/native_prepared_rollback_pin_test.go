package collections

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// A cooperative deletion pin cannot make a still-open appender's bytes
// immutable: failed original validation may roll the append back directly.
// This is a pre-close negative control, not a post-prepare receipt.
func TestNativeSelectedPinDoesNotPreventAppenderRollback(t *testing.T) {
	if !ordinaryColumnStableAuthorityEnabled() {
		t.Skip("stable namespace authority unavailable")
	}
	root := filepath.Join(t.TempDir(), "column-assets")
	cfg := ColumnStoreConfig{Enabled: true, AssetManager: &ColumnAssetManagerConfig{
		Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "rollback-pin",
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
	var cleanupErr error
	t.Cleanup(func() {
		if cleanupErr != nil {
			enrollment.CloseAfterCleanup(cleanupErr)
			return
		}
		enrollment.Close()
		if err := owner.Close(); err != nil {
			t.Errorf("close owner: %v", err)
		}
	})
	session := newColumnPhysicalAssetAppendSessionWithStableResources(root, cfg, registry)
	t.Cleanup(func() {
		if err := session.abort(); err != nil {
			t.Errorf("abort session: %v", err)
		}
	})
	refs, err := session.appendKinds(1, []columnPhysicalAssetAppendItem{{
		payload: []byte("will-roll-back"), kind: ColumnAssetKindTCS1PartImage, generation: 1, partID: 1,
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
	injected := errors.New("reject original validation")
	var sideErr error
	err = session.closeActiveWithStableValidation(func(original *rootpublication.StableResourceSet) error {
		ref := refs[0]
		if e := validateStableColumnResourcesMatchPrepared([]ColumnPreparedAsset{{Ref: ref}}, original); e != nil {
			return e
		}
		identity, sideErr = rootpublication.StableIdentityFromFile(appender.file)
		if sideErr != nil {
			return sideErr
		}
		namespace, e := registry.NewStableNamespaceTokenForKnownLinkWithMetadata(stableColumnAssetNamespaceSpec(appender.stableParent, appender.file, ref), &owner)
		if e != nil {
			sideErr = e
			return e
		}
		defer func() {
			if e := namespace.ReleaseWithError(); e != nil {
				cleanupErr = errors.Join(cleanupErr, e)
				sideErr = errors.Join(sideErr, e)
			}
		}()
		kind, field, _, e := stableColumnAssetResourceClassification(ref.Kind)
		if e == nil {
			selected, e = NewStableColumnAssetResourceToken(rootpublication.StableResourceSpec{
				MetadataOwner: &owner, Kind: kind, LogicalLane: ref.Namespace, ResourceID: fmt.Sprint(ref.FileID),
				Generation: uint64(ref.FileID), DiagnosticPath: stableColumnAssetDiagnosticPath(ref), File: appender.file,
				Frontier: rootpublication.DurableFrontier{Bytes: uint64(ref.Offset + ref.Length)},
				Digest:   stableColumnSegmentDigest(ref), Reachability: field, Namespace: namespace,
				LogicalObligations: []rootpublication.StableLogicalObligation{stableColumnLogicalObligation(ref, field)},
				PinRegistry:        registry, ContentSynced: true,
			})
		}
		if e != nil {
			sideErr = e
			return e
		}
		return injected
	})
	if sideErr != nil || !errors.Is(err, injected) || selected == nil {
		t.Fatalf("close=%v side=%v selected=%v", err, sideErr, selected != nil)
	}
	if registry.ActivePins() != 1 {
		t.Fatalf("pins after rollback=%d", registry.ActivePins())
	}
	if deletion, e := registry.BeginDelete(identity); !errors.Is(e, rootpublication.ErrResourcePinned) {
		if deletion != nil {
			deletion.Abort()
		}
		t.Fatalf("cooperative deletion while pinned: %v", e)
	}
	buf := make([]byte, len("will-roll-back"))
	if n, e := selected.ReadAt(buf, refs[0].Offset); n != 0 || !errors.Is(e, io.EOF) {
		t.Fatalf("rolled-back bytes still readable: n=%d err=%v", n, e)
	}
}
