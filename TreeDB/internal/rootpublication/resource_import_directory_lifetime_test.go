package rootpublication

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

type importedDirectoryLifetimeRelease struct{ calls int }

func (r *importedDirectoryLifetimeRelease) ReleaseDependencyDirectoryV6() (bool, DependencyDirectoryReleaseCompletionV6, error) {
	r.calls++
	return true, DependencyDirectoryReleaseCompletionV6{}, nil
}

// Use the real owned DATA constructor and canonical records, rather than a
// synthetic directory header: the surviving clone must still read its tree.
func importedOwnedDirectoryLifetimeFixture(t *testing.T, source *StableResourceSet, owner *retainedalloc.Owner) (*DependencyDirectoryV2, *importedDirectoryLifetimeRelease) {
	t.Helper()
	records := map[string][]byte{}
	physical, logical, err := WalkDependencyDirectoryChangesV2(source, nil, func(k, v []byte, removed bool) error {
		if removed {
			t.Fatal("fresh directory contains a removal")
		}
		records[string(k)] = bytes.Clone(v)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if err = p.GrowTo(3); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(records))
	for key := range records {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	image := make([]byte, page.PageSize)
	n := node.NewNode(image)
	n.SetPageID(2)
	n.SetType(page.PageTypeLeaf)
	for _, key := range keys {
		if err = n.AddLeafEntry([]byte(key), records[key], node.FlagInline, page.ValuePtr{}); err != nil {
			t.Fatal(err)
		}
	}
	n.UpdateChecksum()
	if err = p.Write(2, image); err != nil {
		t.Fatal(err)
	}
	release := new(importedDirectoryLifetimeRelease)
	directory, err := NewOwnedTypedDependencyDirectoryV6(p, DependencyDirectoryRefV2{RootPageID: 2, PhysicalCount: physical, LogicalCount: logical}, p.PageCount(), owner, release)
	if err != nil {
		t.Fatal(err)
	}
	return directory, release
}

func TestOwnedImportedTokenDirectoryLifetimeAndRefusal(t *testing.T) {
	for _, failPhysical := range []bool{false, true} {
		name := "last_release"
		if failPhysical {
			name = "failed_last_physical_release"
		}
		t.Run(name, func(t *testing.T) {
			producer := NewIdentityPinRegistry()
			defer producer.Close()
			selected := NewIdentityPinRegistry()
			defer selected.Close()
			owner := selected.MetadataOwner()
			budget := &registryBudget{cap: 1 << 20}
			enrollment, err := owner.Enroll(budget)
			if err != nil {
				t.Fatal(err)
			}
			defer enrollment.Close()
			baseline, budgetBaseline := owner.Bytes(), budget.bytes
			file := writeStableResourceFixture(t, t.TempDir(), "asset", "original-resource-bytes")
			parent, err := os.Open(filepath.Dir(file.Name()))
			if err != nil {
				t.Fatal(err)
			}
			defer parent.Close()
			namespace, err := NewStableNamespaceToken(StableNamespaceSpec{Parent: parent, LinkedResource: file, ParentGeneration: 1, Operation: NamespaceCreate, NewName: "asset", DiagnosticPath: "assets"})
			if err != nil {
				t.Fatal(err)
			}
			// A failed final physical release deliberately retains namespace
			// custody. Close only the fixture's OS handles after all assertions;
			// do not retry logical cleanup or refund its retained debt. Windows
			// otherwise cannot remove the temporary directory or linked child.
			namespaceParent, namespacePersistence := namespace.parent, namespace.persistence
			t.Cleanup(func() {
				if namespacePersistence != nil && namespacePersistence != namespaceParent {
					_ = namespacePersistence.Close()
				}
				if namespaceParent != nil {
					_ = namespaceParent.Close()
				}
			})
			if err = namespace.Stabilize(); err != nil {
				t.Fatal(err)
			}
			identity, err := StableIdentityFromFile(file)
			if err != nil {
				t.Fatal(err)
			}
			if err = producer.Observe(identity); err != nil {
				t.Fatal(err)
			}
			value := appendMutationTestObligation(1)
			original, err := NewStableResourceToken(StableResourceSpec{
				Kind: ResourceColumnAsset, LogicalLane: "assets", ResourceID: "1", Generation: 1,
				DiagnosticPath: "asset", File: file, Frontier: DurableFrontier{Bytes: 20},
				Reachability: value.Reachability, LogicalObligations: []StableLogicalObligation{value},
				PinRegistry: producer, OriginalObservation: producer, Namespace: namespace,
			})
			if err != nil {
				t.Fatal(errors.Join(err, producer.Unobserve(identity)))
			}
			builder := NewStableResourceSetBuilder()
			if err = builder.Add(original); err != nil {
				t.Fatal(err)
			}
			source, err := builder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			defer source.Release()
			root := source.kindViews.get(ResourceColumnAsset).root
			imported, err := newImportedResourceToken(owner, original, root, original.logicalLane, original.resourceID, original.diagnosticPath, original.frontier, original.reachability, original.logicalObligations, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer imported.Release()
			directory, release := importedOwnedDirectoryLifetimeFixture(t, source, owner)
			originalDirectoryLive := true
			defer func() {
				if originalDirectoryLive {
					directory.Release()
				}
			}()
			clone := func(d *DependencyDirectoryV2) (*StableResourceToken, error) {
				return imported.cloneOwnedPinnedDirectory(imported.logicalLane, imported.resourceID, imported.diagnosticPath, imported.frontier, imported.reachability, imported.logicalObligations, d, nil)
			}

			foreign := NewIdentityPinRegistry()
			defer foreign.Close()
			foreignDirectory, foreignRelease := importedOwnedDirectoryLifetimeFixture(t, source, foreign.MetadataOwner())
			defer foreignDirectory.Release()
			releasedDirectory, released := importedOwnedDirectoryLifetimeFixture(t, source, owner)
			if consumed, err := releasedDirectory.ReleaseOutcome(); !consumed || err != nil || released.calls != 1 {
				t.Fatalf("release refusal operand: consumed=%t calls=%d err=%v", consumed, released.calls, err)
			}
			for _, refused := range []*DependencyDirectoryV2{foreignDirectory, releasedDirectory} {
				before, governor := owner.Bytes(), budget.bytes
				rootRefs, physicalRefs, directoryRefs := root.refs.Load(), original.pinnedRefs.Load(), directory.refs.Load()
				pins := producer.Stats().ActivePins
				foreignBytes, foreignRefs := foreign.MetadataOwner().Bytes(), foreignDirectory.refs.Load()
				got, err := clone(refused)
				if got != nil || !errors.Is(err, ErrResourceOwnership) {
					t.Fatalf("directory refusal token=%p err=%v", got, err)
				}
				if owner.Bytes() != before || budget.bytes != governor || root.refs.Load() != rootRefs || original.pinnedRefs.Load() != physicalRefs || directory.refs.Load() != directoryRefs || producer.Stats().ActivePins != pins || foreign.MetadataOwner().Bytes() != foreignBytes || foreignDirectory.refs.Load() != foreignRefs || foreignRelease.calls != 0 {
					t.Fatal("directory refusal changed original ownership or admission")
				}
			}
			before, refs := owner.Bytes(), root.refs.Load()
			budget.cap = budget.bytes
			if got, err := clone(directory); got != nil || !errors.Is(err, retainedalloc.ErrCapacity) || owner.Bytes() != before || root.refs.Load() != refs || directory.refs.Load() != 1 {
				t.Fatalf("finite admission refusal token=%p err=%v", got, err)
			}
			budget.cap = 1 << 20
			survivor, err := clone(directory)
			if err != nil {
				t.Fatal(err)
			}
			defer survivor.Release()
			if survivor.directory != directory || survivor.importedSource != original || survivor.physicalBacking.sourceRoot != root || survivor.namespace != namespace {
				t.Fatal("clone replaced original directory or producer custody")
			}
			if err = source.ReleaseWithError(); err != nil {
				t.Fatal(err)
			}
			if err = imported.ReleaseWithError(); err != nil {
				t.Fatal(err)
			}
			if consumed, err := directory.ReleaseOutcome(); !consumed || err != nil {
				t.Fatalf("original directory edge: consumed=%t err=%v", consumed, err)
			}
			originalDirectoryLive = false
			if release.calls != 0 || directory.refs.Load() != 1 || original.released.Load() || producer.Stats().ActivePins != 1 {
				t.Fatal("original edges consumed surviving clone custody")
			}
			// A later publication can clone this imported, already-bound token
			// after all original caller edges have gone. Keep the SAME directory
			// and producer root, then retire the earlier imported descriptor.
			next, err := survivor.cloneOwnedPinnedDirectory(survivor.logicalLane, survivor.resourceID, survivor.diagnosticPath, survivor.frontier, survivor.reachability, survivor.logicalObligations, survivor.directory, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer next.Release()
			if next.directory != directory || next.physicalBacking.sourceRoot != root || next.importedSource != original || directory.refs.Load() != 2 {
				t.Fatal("recloning bound imported token replaced original custody")
			}
			if err = survivor.ReleaseWithError(); err != nil {
				t.Fatal(err)
			}
			survivor = next
			if directory.refs.Load() != 1 || release.calls != 0 || original.released.Load() {
				t.Fatal("retiring earlier clone consumed later bound clone custody")
			}
			if err = survivor.WithPinnedFile(func(pinned *os.File) error {
				got := make([]byte, len("original-resource-bytes"))
				if _, err := pinned.ReadAt(got, 0); err != nil {
					return err
				}
				if string(got) != "original-resource-bytes" {
					t.Fatal("surviving clone changed physical contents")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if _, got, found, err := survivor.directory.LookupLogical(value); err != nil || !found || got != value {
				t.Fatalf("surviving original directory found=%t got=%+v err=%v", found, got, err)
			}
			if failPhysical {
				if err = original.pinned.Close(); err != nil {
					t.Fatal(err)
				}
			}
			first := survivor.ReleaseWithError()
			if failPhysical {
				if !errors.Is(first, os.ErrClosed) || !errors.Is(producer.CleanupError(), os.ErrClosed) || producer.failedPinned == nil || owner.Bytes() <= baseline || producer.MetadataOwner().Bytes() == 0 {
					t.Fatalf("last physical failure lost original custody: %v", first)
				}
				if survivor.importedSource != nil || survivor.namespace != nil || release.calls != 0 || directory.refs.Load() != 1 {
					t.Fatal("failed producer release retained borrowed aliases or lost directory edge")
				}
				remaining, producerRemaining := owner.Bytes(), producer.MetadataOwner().Bytes()
				if again := survivor.ReleaseWithError(); again != first || owner.Bytes() != remaining || producer.MetadataOwner().Bytes() != producerRemaining || release.calls != 0 {
					t.Fatal("failed last release replayed cleanup or refunded original debt")
				}
				return
			}
			if first != nil || release.calls != 1 || !original.released.Load() || producer.Stats().ActivePins != 0 || owner.Bytes() != baseline || budget.bytes != budgetBaseline {
				t.Fatalf("last custody release err=%v directoryCalls=%d pins=%d bytes=%d/%d budget=%d/%d", first, release.calls, producer.Stats().ActivePins, owner.Bytes(), baseline, budget.bytes, budgetBaseline)
			}
		})
	}
}
