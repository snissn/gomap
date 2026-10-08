package db

import (
	"crypto/sha256"
	"errors"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/allocclass"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestStableIndexCallbackOriginalOnlyBothPublicClonePaths(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("exact relative namespace unsupported")
	}
	for _, physical := range []bool{false, true} {
		t.Run(map[bool]string{false: "shared", true: "physical"}[physical], func(t *testing.T) {
			database, err := Open(Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			snapshot := database.AcquireStableSnapshot()
			if snapshot == nil {
				t.Fatal("missing snapshot")
			}
			creator := snapshot.pagerCreator
			owner := creator.OwnerStats()
			if !owner.Ordinary {
				t.Fatal("snapshot lacks original ordinary creator")
			}
			obligation := rootpublication.StableLogicalObligation{Class: "callback-test-v1", Kind: "index", Namespace: "index", Generation: 1, PartID: 1, FileID: 1, Offset: 0, Length: 1, Checksum: 1, Digest: sha256.Sum256([]byte("index")), Reachability: rootpublication.ReachabilityIndexFile}
			var calls atomic.Int32
			spec := rootpublication.StableResourceSpec{Kind: rootpublication.ResourceIndex, LogicalLane: "index", ResourceID: "index", Reachability: rootpublication.ReachabilityIndexFile, Digest: sha256.Sum256([]byte("callback-test")), LogicalObligations: []rootpublication.StableLogicalObligation{obligation}, OnRelease: func() { calls.Add(1) }}
			token, err := snapshot.NewStableIndexResourceToken(spec, rootpublication.NewStableResourceToken)
			if err != nil {
				t.Fatal(err)
			}
			builder := rootpublication.NewStableResourceSetBuilder()
			if err = builder.Add(token); err != nil {
				t.Fatal(err)
			}
			source, err := builder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			if err = snapshot.Close(); err != nil {
				t.Fatal(err)
			}
			if err = database.Close(); err != nil {
				t.Fatal(err)
			}
			// Public clone admission must use the already-live original ordinary Scope
			// after DB master admission closed; it must not install a new creator.
			var clone *rootpublication.StableResourceSet
			var work rootpublication.StableResourceClosureWork
			if physical {
				clone, work, err = rootpublication.CloneStableResourceSetForLogicalObligationsWithWork(source, rootpublication.StableLogicalObligationRequirements{ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityIndexFile}, Obligations: []rootpublication.StableLogicalObligation{obligation}})
				if err == nil && work.PhysicalHandleCopies != 1 {
					t.Fatalf("physical constructor route not exercised: %+v", work)
				}
			} else {
				// A nonempty unrelated scope selects the full shared-handle constructor
				// rather than retaining the original token through a kind-view alias.
				clone, work, err = rootpublication.CloneStableResourceSetForLogicalObligationsWithWork(source, rootpublication.StableLogicalObligationRequirements{ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityColumnManifest}})
				if err == nil && work.PhysicalHandleShares != 1 {
					t.Fatalf("shared constructor route not exercised: %+v", work)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = source.Release(); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 || database.stableIndexCaptures.Load() != 0 || creator.Stats().Closed {
				t.Fatal("original responsibilities replayed or cloned provider lost creator")
			}
			for _, cloned := range clone.Tokens() {
				if err = cloned.SyncThrough(); err != nil {
					t.Fatal(err)
				}
			}
			if err = clone.Release(); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 || database.stableIndexCaptures.Load() != 0 || creator.OwnerStats().Live != 0 {
				t.Fatalf("last public clone did not discharge original closure: %+v", creator.OwnerStats())
			}
		})
	}
}

func TestStableIndexCallbackConstructorFailureLeavesSnapshotResponsibility(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("exact relative namespace unsupported")
	}
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	snapshot := database.AcquireStableSnapshot()
	if snapshot == nil {
		t.Fatal("missing snapshot")
	}
	creator := snapshot.pagerCreator
	before := creator.Stats().Retained
	calls := 0
	denied := errors.New("constructor denied")
	_, err = snapshot.NewStableIndexResourceToken(rootpublication.StableResourceSpec{Kind: rootpublication.ResourceIndex, LogicalLane: "index", ResourceID: "index", Reachability: rootpublication.ReachabilityIndexFile, OnRelease: func() { calls++ }}, func(spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
		if spec.CallbackCreator != creator || spec.CallbackProvider == nil || spec.ReleaseEnvironment == nil || spec.OnRelease != nil {
			t.Fatal("constructor lacks concrete preborn callback roles")
		}
		return nil, denied
	})
	if !errors.Is(err, denied) || calls != 0 || snapshot.stableIndexCaptureTransferred || creator.Stats().Retained != before {
		t.Fatal("failed constructor consumed original responsibilities")
	}
	if err = snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if database.stableIndexCaptures.Load() != 0 {
		t.Fatal("failed constructor leaked capture admission")
	}
}

func TestStableIndexCallbackKnownEnvironmentClassPrepaid(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("exact relative namespace unsupported")
	}
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	snapshot := database.AcquireStableSnapshot()
	if snapshot == nil {
		t.Fatal("missing snapshot")
	}
	defer snapshot.Close()
	creator := snapshot.pagerCreator
	before := creator.Stats().Bytes
	expected, err := allocclass.ClassBytes(uint64(unsafe.Sizeof(stableIndexReleaseEnvironment{})), true)
	if err != nil {
		t.Fatal(err)
	}
	if expected != 64 {
		t.Fatalf("actual named original release environment class=%d", expected)
	}
	_, err = snapshot.NewStableIndexResourceToken(rootpublication.StableResourceSpec{Kind: rootpublication.ResourceDictionary, LogicalLane: "index", ResourceID: "index", Reachability: rootpublication.ReachabilityDictionaryGeneration}, func(rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
		if creator.Stats().Bytes-before != expected {
			t.Fatal("named release environment born before exact class debit")
		}
		return nil, ErrClosed
	})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("unexpected constructor result: %v", err)
	}
}
