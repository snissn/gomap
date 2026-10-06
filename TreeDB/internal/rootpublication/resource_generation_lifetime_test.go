package rootpublication

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestStableResourceTokenPhysicalGenerationLifetimeCloneFilterAndRelease(t *testing.T) {
	for _, physicalOnly := range []bool{false, true} {
		t.Run(fmt.Sprint(physicalOnly), func(t *testing.T) {
			var local, last atomic.Int32
			obligations := []StableLogicalObligation{
				{Class: "asset", Kind: "column", Namespace: "test", Generation: 1, FileID: 1, Length: 1, Reachability: ReachabilityColumnManifest, Digest: sha256.Sum256([]byte("one"))},
				{Class: "asset", Kind: "column", Namespace: "test", Generation: 2, FileID: 2, Length: 1, Reachability: ReachabilityColumnManifest, Digest: sha256.Sum256([]byte("two"))},
			}
			token := stableTokenFixture(t, t.TempDir(), "generation", 1, 1, ReachabilityColumnManifest, "generation", func(spec *StableResourceSpec) {
				spec.Kind = ResourceColumnAsset
				spec.LogicalObligations = obligations
				spec.OnRelease = func() { local.Add(1) }
				spec.OnLastPinnedRelease = func() { last.Add(1) }
			})
			builder := NewStableResourceSetBuilder()
			if err := builder.Add(token); err != nil {
				t.Fatal(err)
			}
			source, err := builder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			defer source.Release()
			requirements := StableLogicalObligationRequirements{ScopedFields: []ReachabilityField{ReachabilityColumnManifest}, Obligations: obligations[:1]}
			filtered, _, err := CloneStableResourceSetForLogicalObligationsWithWork(source, requirements)
			if err != nil {
				t.Fatal(err)
			}
			defer filtered.Release()
			var view *StableResourceSet
			if physicalOnly {
				view, err = ClonePhysicalReachabilityUnion(filtered)
			} else {
				view, err = CloneStableResourceSetExcludingKinds(filtered)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer view.Release()
			source.Release()
			filtered.Release()
			if local.Load() != 1 || last.Load() != 0 {
				t.Fatalf("local=%d final=%d before last view release", local.Load(), last.Load())
			}
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				wg.Go(view.Release)
			}
			wg.Wait()
			if last.Load() != 1 {
				t.Fatalf("last physical callback=%d want one", last.Load())
			}
		})
	}
}

func TestStableResourceTokenPhysicalGenerationLifetimeCoalescing(t *testing.T) {
	for _, certified := range []bool{false, true} {
		t.Run(fmt.Sprint(certified), func(t *testing.T) {
			file := writeStableResourceFixture(t, t.TempDir(), "shared", "bytes")
			var last [3]atomic.Int32
			makeSet := func(which int, start, count uint64) *StableResourceSet {
				var obligations []StableLogicalObligation
				for i := start; i < start+count; i++ {
					obligations = append(obligations, StableLogicalObligation{Class: "asset", Kind: "column", Namespace: "test", Generation: i, FileID: i, Length: 1, Reachability: ReachabilityColumnManifest, Digest: sha256.Sum256([]byte(fmt.Sprint(i)))})
				}
				token, err := NewStableResourceToken(StableResourceSpec{Kind: ResourceColumnAsset, LogicalLane: "columns", ResourceID: "shared", Generation: 1, DiagnosticPath: "shared", File: file, Frontier: DurableFrontier{Bytes: 1}, Reachability: ReachabilityColumnManifest, ContentSynced: true, LogicalObligations: obligations, OnLastPinnedRelease: func() { last[which].Add(1) }})
				if err != nil {
					t.Fatal(err)
				}
				builder := NewStableResourceSetBuilder()
				if err := builder.Add(token); err != nil {
					t.Fatal(err)
				}
				if which == 1 {
					disjoint := stableTokenFixture(t, t.TempDir(), "disjoint", 2, 1, ReachabilityColumnManifest, "independent-generation", func(spec *StableResourceSpec) {
						spec.Kind = ResourceColumnAsset
						spec.LogicalObligations = []StableLogicalObligation{{Class: "asset", Kind: "column", Namespace: "test", Generation: 22, FileID: 22, Length: 1, Reachability: ReachabilityColumnManifest, Digest: sha256.Sum256([]byte("22"))}}
						spec.OnLastPinnedRelease = func() { last[2].Add(1) }
					})
					if err := builder.Add(disjoint); err != nil {
						t.Fatal(err)
					}
				}
				resources, err := builder.Freeze()
				if err != nil {
					t.Fatal(err)
				}
				return resources
			}
			base := makeSet(0, 1, 20)
			fresh := makeSet(1, 21, 1)
			defer base.Release()
			defer fresh.Release()
			builder := NewStableResourceSetBuilder()
			defer builder.Abandon()
			if err := builder.Merge(base); err != nil {
				t.Fatal(err)
			}
			if certified {
				mutation := StableLogicalObligationMutation{ScopedFields: []ReachabilityField{ReachabilityColumnManifest}}
				if err := fresh.WalkLogicalObligations(func(_ StableResourcePhysicalDescriptor, obligation StableLogicalObligation) error {
					mutation.Added = append(mutation.Added, obligation)
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				work, err := builder.MergeAppendOnlyLogicalObligations(fresh, mutation)
				if err != nil || work.AppendOnlyCollisionFastPath != 1 {
					t.Fatalf("certified work=%+v err=%v", work, err)
				}
			} else if err := builder.Merge(fresh); err != nil {
				t.Fatal(err)
			}
			output, err := builder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			defer output.Release()
			if output.Len() != 2 || last[0].Load()+last[1].Load() != 1 || last[2].Load() != 0 {
				t.Fatal("coalescing accumulated independent physical generations")
			}
			output.Release()
			if last[0].Load() != 1 || last[1].Load() != 1 || last[2].Load() != 1 {
				t.Fatal("generation callbacks not balanced")
			}
		})
	}
}

func TestStableResourceTokenPhysicalGenerationLifetimeMergeFailureIsTransactional(t *testing.T) {
	file := writeStableResourceFixture(t, t.TempDir(), "shared", "bytes")
	var last [2]int
	makeSet := func(which int) *StableResourceSet {
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceDictionary, LogicalLane: "dictdb/index", ResourceID: "index", Generation: 1,
			DiagnosticPath: "shared", File: file, Frontier: DurableFrontier{Bytes: 1}, Reachability: ReachabilityDictionaryGeneration,
			Digest: sha256.Sum256([]byte(fmt.Sprint(which))), OnLastPinnedRelease: func() { last[which]++ },
		})
		if err != nil {
			t.Fatal(err)
		}
		builder := NewStableResourceSetBuilder()
		if err := builder.Add(token); err != nil {
			t.Fatal(err)
		}
		resources, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		return resources
	}
	base, fresh := makeSet(0), makeSet(1)
	defer base.Release()
	defer fresh.Release()
	builder := NewStableResourceSetBuilder()
	defer builder.Abandon()
	if err := builder.Merge(base); err != nil {
		t.Fatal(err)
	}
	if err := builder.Merge(fresh); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("merge error=%v want conflict", err)
	}
	if last != [2]int{} {
		t.Fatal("failed merge consumed physical-generation ownership")
	}
	fresh.Release()
	if last != [2]int{0, 1} {
		t.Fatal("failed merge changed incoming ownership")
	}
	builder.Abandon()
	if last != [2]int{1, 1} {
		t.Fatal("abandon did not balance retained physical-generation ownership")
	}
}

func TestStableResourceTokenPhysicalGenerationLifetimeConstructionFailure(t *testing.T) {
	file := writeStableResourceFixture(t, t.TempDir(), "small", "bytes")
	callbacks := 0
	token, err := NewStableResourceToken(StableResourceSpec{Kind: ResourceColumnAsset, LogicalLane: "test", ResourceID: "small", Generation: 1, DiagnosticPath: "small", File: file, Frontier: DurableFrontier{Bytes: 999}, Reachability: ReachabilityColumnManifest, OnRelease: func() { callbacks++ }, OnLastPinnedRelease: func() { callbacks++ }})
	if token != nil || !errors.Is(err, ErrFrontierBeyondResource) || callbacks != 0 {
		t.Fatalf("failed construction token=%v err=%v callbacks=%d", token, err, callbacks)
	}
}
