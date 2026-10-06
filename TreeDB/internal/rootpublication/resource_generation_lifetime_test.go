package rootpublication

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
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

// Recovered dependency tokens have a stable namespace and token-local registry
// bookkeeping, but no producer generation fence. A fresh certified producer
// must become the representative before the unfenced capture is discarded.
func TestStableResourceTokenPhysicalGenerationLifetimePromotesRecoveredRepresentative(t *testing.T) {
	for _, fencedFirst := range []bool{false, true} {
		for _, path := range []string{"add", "merge", "certified", "union", "physical-union"} {
			for _, indexed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/indexed=%t/fenced-first=%t", path, indexed, fencedFirst), func(t *testing.T) {
					dir := t.TempDir()
					file := writeStableResourceFixture(t, dir, "index.db", "dictionary-index")
					parent, err := os.Open(dir)
					if err != nil {
						t.Fatal(err)
					}
					defer parent.Close()
					parentIdentity, err := StableIdentityFromFile(parent)
					if err != nil {
						t.Fatal(err)
					}
					parentIdentity.Generation = 1
					namespace, err := NewRecoveredStableNamespaceToken(StableNamespaceSpec{
						Parent: parent, LinkedResource: file, ParentGeneration: 1, Operation: NamespaceCreate,
						NewName: "index.db", DiagnosticPath: "dictdb/index.db",
					}, parentIdentity)
					if err != nil {
						t.Fatal(err)
					}
					defer namespace.Release()
					var local [2]int
					final := 0
					all := make([]StableLogicalObligation, 0, 21)
					for i := uint64(1); i <= 21; i++ {
						all = append(all, StableLogicalObligation{Class: "dictionary", Kind: "definition", Namespace: "dictdb", Generation: i, FileID: i, Length: 1, Reachability: ReachabilityDictionaryGeneration, Digest: sha256.Sum256([]byte(fmt.Sprint(i)))})
					}
					token := func(fresh bool) *StableResourceToken {
						which, obligations := 0, all[:20]
						if fresh {
							which, obligations = 1, all[20:]
						}
						spec := StableResourceSpec{Kind: ResourceDictionary, LogicalLane: "dictdb/index", ResourceID: "index", Generation: 1,
							DiagnosticPath: "dictdb/index.db", File: file, Frontier: DurableFrontier{Bytes: 1}, Reachability: ReachabilityDictionaryGeneration,
							Namespace: namespace, ContentSynced: true, LogicalObligations: obligations, OnRelease: func() { local[which]++ }}
						if fresh != fencedFirst {
							spec.OnLastPinnedRelease = func() { final++ }
						}
						result, err := NewStableResourceToken(spec)
						if err != nil {
							t.Fatal(err)
						}
						return result
					}
					baseBuilder := NewStableResourceSetBuilder()
					defer baseBuilder.Abandon()
					if indexed {
						for i := uint64(1); i <= stableResourceEntryLinearLookupLimit; i++ {
							if err := baseBuilder.Add(distinctPhysicalTokenFixture(t, file, i)); err != nil {
								t.Fatal(err)
							}
						}
					}
					if err := baseBuilder.Add(token(false)); err != nil {
						t.Fatal(err)
					}
					var output *StableResourceSet
					var certifiedWork StableResourceClosureWork
					if path == "add" {
						if err := baseBuilder.Add(token(true)); err != nil {
							t.Fatal(err)
						}
						output, err = baseBuilder.Freeze()
					} else {
						base, freezeErr := baseBuilder.Freeze()
						if freezeErr != nil {
							t.Fatal(freezeErr)
						}
						defer base.Release()
						freshBuilder := NewStableResourceSetBuilder()
						defer freshBuilder.Abandon()
						if err := freshBuilder.Add(token(true)); err != nil {
							t.Fatal(err)
						}
						fresh, freezeErr := freshBuilder.Freeze()
						if freezeErr != nil {
							t.Fatal(freezeErr)
						}
						defer fresh.Release()
						switch path {
						case "merge", "certified":
							builder := NewStableResourceSetBuilder()
							defer builder.Abandon()
							if err := builder.Merge(base); err != nil {
								t.Fatal(err)
							}
							if path == "certified" {
								work, mergeErr := builder.MergeAppendOnlyLogicalObligations(fresh, StableLogicalObligationMutation{ScopedFields: []ReachabilityField{ReachabilityDictionaryGeneration}, Added: all[20:]})
								if mergeErr != nil {
									t.Fatal(mergeErr)
								}
								certifiedWork = work
							} else if err := builder.Merge(fresh); err != nil {
								t.Fatal(err)
							}
							output, err = builder.Freeze()
						case "union":
							view, unionErr := UnionStableResourceSets(base, fresh)
							if unionErr != nil {
								t.Fatal(unionErr)
							}
							output, _, err = CloneStableResourceSetForLogicalObligationsWithWork(view, StableLogicalObligationRequirements{ScopedFields: []ReachabilityField{ReachabilityDictionaryGeneration}, Obligations: all})
						case "physical-union":
							output, err = ClonePhysicalReachabilityUnion(base, fresh)
						}
						base.Release()
						fresh.Release()
					}
					if err != nil {
						t.Fatal(err)
					}
					defer output.Release()
					if final != 0 {
						t.Fatalf("fresh physical-generation fence ended while output remains live: %d", final)
					}
					if path == "certified" {
						if !fencedFirst && (certifiedWork.AppendOnlyCollisionFastPath != 0 || certifiedWork.AppendOnlyCollisionFallbacks != 1) {
							t.Fatalf("stronger representative requires exact fallback: %+v", certifiedWork)
						}
						if fencedFirst && certifiedWork.AppendOnlyCollisionFastPath != 1 {
							t.Fatalf("existing stronger representative should retain fast path: %+v", certifiedWork)
						}
					}
					output.Release()
					if final != 1 || local != [2]int{1, 1} {
						t.Fatalf("unbalanced release: generation=%d local=%v", final, local)
					}
				})
			}
		}
	}
}

func TestStableResourceTokenPhysicalGenerationLifetimeIncomparableAuthoritiesAreTransactional(t *testing.T) {
	for _, certified := range []bool{false, true} {
		t.Run(fmt.Sprint(certified), func(t *testing.T) {
			dir := t.TempDir()
			file := writeStableResourceFixture(t, dir, "index.db", "dictionary-index")
			parent, err := os.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer parent.Close()
			namespace, err := NewStableNamespaceToken(StableNamespaceSpec{Parent: parent, LinkedResource: file, ParentGeneration: 1,
				Operation: NamespaceCreate, NewName: "index.db", DiagnosticPath: "dictdb/index.db"})
			if err != nil {
				t.Fatal(err)
			}
			defer namespace.Release()
			if err := namespace.Stabilize(); err != nil {
				t.Fatal(err)
			}
			var local [2]int
			final := 0
			makeSet := func(which int) *StableResourceSet {
				spec := StableResourceSpec{Kind: ResourceDictionary, LogicalLane: "dictdb/index", ResourceID: "index", Generation: 1,
					DiagnosticPath: "dictdb/index.db", File: file, Frontier: DurableFrontier{Bytes: 1}, Reachability: ReachabilityDictionaryGeneration,
					ContentSynced: true, OnRelease: func() { local[which]++ }}
				if which == 0 {
					spec.OnLastPinnedRelease = func() { final++ }
				} else {
					spec.Namespace = namespace
				}
				token, err := NewStableResourceToken(spec)
				if err != nil {
					t.Fatal(err)
				}
				builder := NewStableResourceSetBuilder()
				defer builder.Abandon()
				if which == 0 {
					for i := uint64(1); i <= stableResourceEntryLinearLookupLimit; i++ {
						if err := builder.Add(distinctPhysicalTokenFixture(t, file, i)); err != nil {
							t.Fatal(err)
						}
					}
				}
				if err := builder.Add(token); err != nil {
					t.Fatal(err)
				}
				set, err := builder.Freeze()
				if err != nil {
					t.Fatal(err)
				}
				return set
			}
			base, incoming := makeSet(0), makeSet(1)
			defer base.Release()
			defer incoming.Release()
			builder := NewStableResourceSetBuilder()
			defer builder.Abandon()
			if err := builder.Merge(base); err != nil {
				t.Fatal(err)
			}
			if certified {
				_, err = builder.MergeAppendOnlyLogicalObligations(incoming, StableLogicalObligationMutation{})
			} else {
				err = builder.Merge(incoming)
			}
			if !errors.Is(err, ErrResourceConflict) {
				t.Fatalf("incomparable authority merge=%v want conflict", err)
			}
			if local != [2]int{} || final != 0 || incoming.Owner() == ResourceOwnerTransferred {
				t.Fatalf("failed merge consumed authority: local=%v generation=%d owner=%v", local, final, incoming.Owner())
			}
			incoming.Release()
			if local != [2]int{0, 1} || final != 0 {
				t.Fatalf("incoming ownership changed: local=%v generation=%d", local, final)
			}
			builder.Abandon()
			if local != [2]int{1, 1} || final != 1 {
				t.Fatalf("abandon unbalanced: local=%v generation=%d", local, final)
			}
		})
	}
}
