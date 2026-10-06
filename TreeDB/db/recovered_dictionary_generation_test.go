package db

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/storagemaintenance"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func recoveredDictionaryGenerationFixture(t *testing.T) (*DB, *DB, *rootpublication.StableResourceSet, *rootpublication.DependencyManifestV1) {
	t.Helper()
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("requires relative namespace")
	}
	root := t.TempDir()
	side, err := Open(Options{Dir: filepath.Join(root, "dictdb"), ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = side.Close() })
	main, err := Open(Options{Dir: filepath.Join(root, "maindb"), ChunkSize: 64 * 1024, DictionaryIndexGenerationLease: side.AcquireDictionaryIndexGenerationLease})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = main.Close() })
	definition := []byte("independent-recovered-dictionary")
	digest := sha256.Sum256(definition)
	id := binary.BigEndian.Uint64(digest[:8])
	if err := side.SetSync([]byte("dictionary"), definition); err != nil {
		t.Fatal(err)
	}
	snap := side.AcquireStableSnapshot()
	if snap == nil {
		t.Fatal("snapshot unavailable")
	}
	token, err := snap.NewStableIndexGenerationResourceToken(rootpublication.StableResourceSpec{
		Kind: rootpublication.ResourceDictionary, LogicalLane: "dictdb/index", ResourceID: "index",
		Digest: rootpublication.DictionaryIndexPhysicalDigestV1(), Reachability: rootpublication.ReachabilityDictionaryGeneration, ContentSynced: true,
		LogicalObligations: []rootpublication.StableLogicalObligation{{Class: "dictionary-generation", Kind: "dictionary", Namespace: "dictdb", Generation: id, FileID: id, Length: int64(len(definition)), Reachability: rootpublication.ReachabilityDictionaryGeneration, Digest: digest}},
	}, func(spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
		return rootpublication.NewStableProducerResourceTokenForDomain(rootpublication.StableProducerDictionary, spec, "authoritative-transitive")
	})
	if err != nil {
		_ = snap.Close()
		t.Fatal(err)
	}
	b := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityDictionaryGeneration)
	defer b.Abandon()
	if err := b.Add(token); err != nil {
		token.Release()
		t.Fatal(err)
	}
	original, err := b.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(original.Release)
	manifest, _, err := original.DependencyManifestV1()
	if err != nil {
		t.Fatal(err)
	}
	return main, side, original, manifest
}

func TestRecoveredDictionaryIndexGenerationKeepsNamespaceFence(t *testing.T) {
	main, side, original, manifest := recoveredDictionaryGenerationFixture(t)
	if err := side.VacuumIndexOnline(t.Context()); !errors.Is(err, rootpublication.ErrResourcePinned) {
		t.Fatalf("fresh generation fence: %v", err)
	}
	recovered, err := main.validateDurableDependencyManifestV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Release()
	original.Release()
	readerBaseline := side.idx.Load().registry.MinPinnedSeq()
	if side.stableIndexCaptures.Load() != 1 {
		t.Fatalf("recovered generation fences=%d want1", side.stableIndexCaptures.Load())
	}
	physical, err := rootpublication.ClonePhysicalReachabilityUnion(recovered)
	if err != nil {
		t.Fatal(err)
	}
	defer physical.Release()
	recovered.Release()
	if err := side.VacuumIndexOnline(t.Context()); !errors.Is(err, rootpublication.ErrResourcePinned) {
		t.Fatalf("physical-only recovered generation fence: %v", err)
	}
	after, err := main.validateDurableDependencyManifestV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	after.Release()
	if side.stableIndexCaptures.Load() != 1 || side.idx.Load().registry.MinPinnedSeq() != readerBaseline {
		t.Fatal("recovery retained reader state or unbalanced generation family")
	}
	physical.Release()
	physical.Release()
	if side.stableIndexCaptures.Load() != 0 {
		t.Fatalf("final generation fences=%d", side.stableIndexCaptures.Load())
	}
	if err := side.VacuumIndexOnline(t.Context()); err != nil {
		t.Fatalf("vacuum after last recovered view: %v", err)
	}
	stale, err := main.validateDurableDependencyManifestV1(manifest)
	if stale != nil {
		stale.Release()
	}
	if !errors.Is(err, rootpublication.ErrResourceConflict) || side.stableIndexCaptures.Load() != 0 {
		t.Fatalf("replaced owner accepted old generation or leaked: err=%v counter=%d", err, side.stableIndexCaptures.Load())
	}
}

func TestRecoveredDictionaryIndexGenerationFailureBalancesAuthority(t *testing.T) {
	for _, scenario := range []string{"missing-owner", "hook-failure", "partial-hook-failure", "nil-lease", "identity", "namespace", "frontier", "duplicate-reachability", "malformed-canonical", "malformed-kind", "empty-logical", "constructor"} {
		t.Run(scenario, func(t *testing.T) {
			main, side, original, manifest := recoveredDictionaryGenerationFixture(t)
			original.Release()
			entries := manifest.Entries()
			entry := entries[0]
			acquired, released := 0, 0
			main.dictionaryIndexGenerationLease = func(expected rootpublication.DependencyManifestEntryV1) (func(), error) {
				release, err := side.AcquireDictionaryIndexGenerationLease(expected)
				if err != nil {
					return nil, err
				}
				acquired++
				return func() { released++; release() }, nil
			}
			switch scenario {
			case "missing-owner":
				main.dictionaryIndexGenerationLease = nil
			case "hook-failure":
				main.dictionaryIndexGenerationLease = func(rootpublication.DependencyManifestEntryV1) (func(), error) {
					return nil, fmt.Errorf("injected generation owner failure")
				}
			case "partial-hook-failure":
				main.dictionaryIndexGenerationLease = func(expected rootpublication.DependencyManifestEntryV1) (func(), error) {
					release, err := side.AcquireDictionaryIndexGenerationLease(expected)
					if err != nil {
						return nil, err
					}
					acquired++
					return func() { released++; release() }, fmt.Errorf("failure after generation acquisition")
				}
			case "nil-lease":
				main.dictionaryIndexGenerationLease = func(rootpublication.DependencyManifestEntryV1) (func(), error) { return nil, nil }
			case "identity":
				entry.Identity.ObjectID[0] ^= 1
			case "namespace":
				ns := *entry.Namespace
				ns.ParentIdentity.ObjectID[0] ^= 1
				entry.Namespace = &ns
			case "frontier":
				entry.Frontier.Bytes += 1 << 30
			case "duplicate-reachability":
				entry.Reachability = append(entry.Reachability, rootpublication.ReachabilityDictionaryGeneration)
			case "malformed-canonical":
				entry.LogicalLane = "unrecognized/alias"
			case "malformed-kind":
				entry.Kind = rootpublication.ResourceTemplate
			case "empty-logical":
				entry.LogicalObligations = nil
			case "constructor":
				entry.LogicalObligations = append([]rootpublication.StableLogicalObligation(nil), entry.LogicalObligations...)
				entry.LogicalObligations[0].Length = -1
			}
			resources, err := main.validateDurableDependencyEntriesV1([]rootpublication.DependencyManifestEntryV1{entry}, false)
			if resources != nil {
				resources.Release()
			}
			if err == nil || acquired != released || side.stableIndexCaptures.Load() != 0 || main.StableResourceIdentityPinRegistry().ActivePins() != 0 {
				t.Fatalf("failure ownership: err=%v acquired=%d released=%d side=%d mainpins=%d", err, acquired, released, side.stableIndexCaptures.Load(), main.StableResourceIdentityPinRegistry().ActivePins())
			}
			if (scenario == "duplicate-reachability" || scenario == "malformed-canonical" || scenario == "malformed-kind") && acquired != 0 {
				t.Fatalf("malformed claim acquired owner authority: %d", acquired)
			}
			if (scenario == "empty-logical" || scenario == "constructor") && acquired != 1 {
				t.Fatalf("did not exercise post-acquisition failure: %d", acquired)
			}
		})
	}
}

func TestRecoveredDictionaryIndexGenerationIgnoresRuntimeGenerationLabel(t *testing.T) {
	main, side, original, manifest := recoveredDictionaryGenerationFixture(t)
	original.Release()
	entry := manifest.Entries()[0]
	entry.Generation += 100
	entry.Identity.Generation = entry.Generation
	resources, err := main.validateDurableDependencyEntriesV1([]rootpublication.DependencyManifestEntryV1{entry}, false)
	if err != nil {
		t.Fatal(err)
	}
	resources.Release()
	if side.stableIndexCaptures.Load() != 0 {
		t.Fatal("generation fence leaked")
	}
}

func TestRecoveredDictionaryIndexGenerationReadOnlyRetainedSlots(t *testing.T) {
	main, side, original, manifest := recoveredDictionaryGenerationFixture(t)
	requirements := rootpublication.StableLogicalObligationRequirements{
		ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityDictionaryGeneration},
		Obligations:  manifest.Entries()[0].LogicalObligations,
	}
	for publication := 0; publication < 2; publication++ {
		resources, err := rootpublication.CloneStableResourceSetForLogicalObligations(original, requirements)
		if err != nil {
			t.Fatal(err)
		}
		defer resources.Release()
		_, _, err = main.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
			storagemaintenance.ColumnAssetRewritePlan(), []StorageMaintenanceRootDeltaPublishInput{{
				Iter:             mustFrozenRawMemtable(t, "dictionary-root", []byte("published")).NewIterator(nil, nil),
				DurableResources: resources, DurableResourceRequirements: requirements,
			}}, nil, func([]uint64) (iterator.UnsafeIterator, error) {
				return mustFrozenSystemMemtable(t, "dictionary-descriptor", fmt.Sprint(publication)).NewIterator(nil, nil), nil
			})
		if err != nil {
			t.Fatal(err)
		}
		if err := main.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	commit := main.State().CommitSeq
	dir := main.dir
	if err := main.Close(); err != nil {
		t.Fatal(err)
	}
	original.Release()
	if side.stableIndexCaptures.Load() != 0 {
		t.Fatal("closed source retained generation fence")
	}
	for _, opener := range []struct {
		name string
		open func(Options) (*DB, error)
	}{
		{"shared-lock", Open}, {"no-lock", openReadOnlyNoLock},
	} {
		t.Run(opener.name, func(t *testing.T) {
			reopened, err := opener.open(Options{Dir: dir, ReadOnly: true, ChunkSize: 64 * 1024, DictionaryIndexGenerationLease: side.AcquireDictionaryIndexGenerationLease})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if reopened.State().CommitSeq != commit {
				t.Fatalf("selected commit=%d want=%d", reopened.State().CommitSeq, commit)
			}
			var physical []*rootpublication.StableResourceSet
			defer func() {
				for _, set := range physical {
					set.Release()
				}
			}()
			for slot, resources := range reopened.durableRoot.slotResources {
				found := false
				for _, descriptor := range resources.PhysicalDescriptors() {
					found = found || descriptor.Digest() == rootpublication.DictionaryIndexPhysicalDigestV1()
				}
				if !found {
					t.Fatalf("retained slot%d lacks dictionary authority", slot)
				}
				view, err := rootpublication.ClonePhysicalReachabilityUnion(resources)
				if err != nil {
					t.Fatal(err)
				}
				physical = append(physical, view)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
			if err := side.VacuumIndexOnline(t.Context()); !errors.Is(err, rootpublication.ErrResourcePinned) {
				t.Fatalf("closed main with retained physical views: %v", err)
			}
			physical[0].Release()
			if side.stableIndexCaptures.Load() == 0 {
				t.Fatal("first slot release ended remaining family")
			}
			physical[1].Release()
			if side.stableIndexCaptures.Load() != 0 {
				t.Fatalf("final slot release leaked fences=%d", side.stableIndexCaptures.Load())
			}
		})
	}
	if err := side.VacuumIndexOnline(t.Context()); err != nil {
		t.Fatalf("vacuum after both recovered slot families: %v", err)
	}
}
