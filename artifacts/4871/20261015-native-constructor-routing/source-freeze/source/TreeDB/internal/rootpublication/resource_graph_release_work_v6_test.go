package rootpublication

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// This is the original producer graph used by predecessor proof/slot Sets:
// both persistent log kinds are admitted, imported under a different owner,
// then their two existing slot descriptors disappear successively. The final
// native drop must traverse the imported sourceRoot, not infer nonlast custody
// from a surviving physical file or retain a guardian descriptor.
func TestNativeNestedLogSetsRetireAfterBothSlotsOverwriteV6(t *testing.T) {
	for _, physicalFailure := range []bool{false, true} {
		name := "success"
		if physicalFailure {
			name = "original-close-error"
		}
		t.Run(name, func(t *testing.T) {
			var sourceOwner, memberOwner retainedalloc.Owner
			sourceOwner.Initialize(0)
			memberOwner.Initialize(0)
			registry := NewIdentityPinRegistry()
			defer registry.Close()
			builder, err := NewStableResourceSetBuilderWithMetadata(&sourceOwner)
			if err != nil {
				t.Fatal(err)
			}
			var tokens [2]*StableResourceToken
			for i, kind := range []ResourceKind{ResourceValueLog, ResourceOuterLeafLog} {
				field := ReachabilityValueLogPointer
				if kind == ResourceOuterLeafLog {
					field = ReachabilityOuterLeafRawPointer
				}
				file := writeStableResourceFixture(t, t.TempDir(), "persistent.log", "payload")
				defer file.Close()
				id, e := StableIdentityFromFile(file)
				if e != nil {
					t.Fatal(e)
				}
				if e = registry.Observe(id); e != nil {
					t.Fatal(e)
				}
				token, e := NewStableResourceToken(StableResourceSpec{
					MetadataOwner: &sourceOwner, PinRegistry: registry,
					Kind: kind, LogicalLane: string(kind), ResourceID: string(kind),
					Generation: 1, DiagnosticPath: "persistent.log", File: file,
					Frontier: DurableFrontier{Bytes: 7}, Reachability: field,
				})
				if e != nil {
					t.Fatal(e)
				}
				if e = registry.Unobserve(id); e != nil {
					t.Fatal(e)
				}
				if e = builder.Add(token); e != nil {
					t.Fatal(e)
				}
				tokens[i] = token
			}
			source, err := builder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			manifest, _, err := source.DependencyManifestV1()
			if err != nil {
				t.Fatal(err)
			}
			manifest.ReleaseOwnedMetadataV1()
			imported, err := ImportStableResourceSetMetadata(&memberOwner, source)
			if err != nil {
				t.Fatal(err)
			}
			manifest, _, err = imported.DependencyManifestV1()
			if err != nil {
				t.Fatal(err)
			}
			manifest.ReleaseOwnedMetadataV1()
			var slots [2]*StableResourceSet
			for i := range slots {
				slots[i], err = CloneStableResourceSetExcludingKinds(imported)
				if err != nil {
					t.Fatal(err)
				}
				if slots[i].kindViews != imported.kindViews {
					t.Fatal("original slot clone did not retain exact kind backing")
				}
			}
			if err = source.ReleaseWithError(); err != nil {
				t.Fatal(err)
			}
			if err = imported.ReleaseWithError(); err != nil {
				t.Fatal(err)
			}
			source, imported, manifest, builder = nil, nil, nil, nil
			if registry.ActivePins() != 2 {
				t.Fatal("sourceRoot did not retain original log pins")
			}
			if physicalFailure {
				if err = tokens[0].pinned.Close(); err != nil {
					t.Fatal(err)
				}
			}
			for slot := range slots {
				set := slots[slot]
				before := memberOwner.Bytes()
				completion, ready, e := set.ReleaseFromWithWorkV6(ResourceOwnerBuilder, &iterator.OrdinalScanWork{})
				if e != nil || ready || set.Owner() != ResourceOwnerBuilder || memberOwner.Bytes() != before {
					t.Fatal("zero allowance changed original slot", ready, e)
				}
				calls := 0
				for ; calls < 4096; calls++ {
					work := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
					completion, ready, e = set.ReleaseFromWithWorkV6(ResourceOwnerBuilder, work)
					if work.Records > 32 || work.Bytes > 1<<20 {
						t.Fatal("work allowance exceeded")
					}
					if e != nil || ready {
						break
					}
				}
				t.Logf("slot=%d calls=%d ready=%t error=%v source_bytes=%d member_bytes=%d", slot, calls+1, ready, e, sourceOwner.Bytes(), memberOwner.Bytes())
				if slot == 1 && physicalFailure {
					if e == nil || ready || set.metadata == nil || set.metadata.cleanupResult() == nil {
						t.Fatal("original last physical failure lost its Set debt", ready, e)
					}
					debt := memberOwner.Bytes()
					again, done, repeated := set.ReleaseFromWithWorkV6(ResourceOwnerBuilder, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
					if done || !errors.Is(repeated, e) || memberOwner.Bytes() != debt {
						t.Fatal("original failure replayed/refunded", done, repeated)
					}
					again.Complete()
					if memberOwner.Bytes() != debt {
						t.Fatal("failed completion refunded original debt")
					}
					return
				}
				if e != nil || !ready {
					t.Fatal("actual nested log release", ready, e)
				}
				if slot == 0 && registry.ActivePins() != 2 {
					t.Fatal("first slot consumed the still-owned sourceRoot")
				}
				slots[slot], set = nil, nil
				completion.Complete()
				completion.Complete()
			}
			if registry.ActivePins() != 0 || registry.ActiveIdentities() != 0 {
				t.Fatal("original last pin producer did not complete")
			}
			for _, token := range tokens {
				if !token.cleanupDone.Load() || token.metadata != nil || token.physicalBacking != nil {
					t.Fatal("nested original token aliases survived physical completion")
				}
			}
			tokens = [2]*StableResourceToken{}
			if sourceOwner.Bytes() != 0 || memberOwner.Bytes() != 0 {
				t.Fatalf("original graph retained source=%d member=%d bytes", sourceOwner.Bytes(), memberOwner.Bytes())
			}
		})
	}
}
