package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"path/filepath"
	"testing"
)

// The ordinary visible sibling disappears immediately after the original
// shared edge is consumed. Native Finish must not rely on a post-turn nonlast
// assumption, run an opaque last-token callback, or refund the ordinary
// owner's unresolved imported/sourceRoot graph.
func TestNativeSharedSetConsumptionFinishesAfterVisibleSiblingGoneV6(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "ordinary-physical-failure"}[fail], func(t *testing.T) {
			a, err := primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "primary"))
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			work := func() *iterator.OrdinalScanWork {
				return &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
			}
			original, ready, err := NewNativeMVCCPrimaryConstructionTransactionV6(a, work())
			if !ready || err != nil {
				t.Fatal(ready, err)
			}
			txn := original.(*DurableRootTransaction)
			callbacks := 0
			token := stableTokenFixture(t, t.TempDir(), "source", 1, 4, ReachabilityIndexFile, "index.db", func(spec *StableResourceSpec) {
				spec.Kind = ResourceIndex
				spec.OnRelease = func() { callbacks++ }
			})
			builder := NewStableResourceSetBuilder()
			if err = builder.Add(token); err != nil {
				t.Fatal(err)
			}
			source, err := builder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			set, err := ImportStableResourceSetMetadata(a.MetadataOwner(), source)
			if err != nil {
				t.Fatal(err)
			}
			visible, err := CloneStableResourceSetExcludingKinds(set)
			if err != nil {
				t.Fatal(err)
			}
			if set.kindViews != visible.kindViews {
				t.Fatal("actual producer did not share original backing")
			}
			source.Release() // imported original sourceRoot is now the only foreign owner
			if callbacks != 0 {
				t.Fatal("import lost original callback")
			}
			if err = set.transfer(ResourceOwnerBuilder, ResourceOwnerCoordinator); err != nil {
				t.Fatal(err)
			}
			c := txn.constructionV6
			c.resources = set
			txn.owner, txn.phase = ResourceOwnerCoordinator, durableRootActivated
			counts := map[ResourceKind]uint64{ResourceIndex: 1}
			charged, wrapperCharge := a.MetadataOwner().Bytes(), set.metadata.charge
			if ready, err := txn.consumePrimarySharedResourcesV6(visible, counts, &iterator.OrdinalScanWork{}); ready || err != nil || c.setReleaseConsumed || set.Owner() != ResourceOwnerCoordinator || counts[ResourceIndex] != 1 || a.MetadataOwner().Bytes() != charged {
				t.Fatal("pre-effect admission refusal changed original ownership", ready, err)
			}
			admitted := work()
			if ready, err := txn.consumePrimarySharedResourcesV6(visible, counts, admitted); !ready || err != nil {
				t.Fatal("actual shared-edge consumption", ready, err)
			}
			t.Logf("Set shared-edge stage only records=%d bytes=%d", admitted.Records, admitted.Bytes)
			if !c.setReleaseConsumed || set.Owner() != ResourceOwnerReleased || len(counts) != 0 || a.MetadataOwner().Bytes() != charged || c.setRelease.owner != a.MetadataOwner() || c.setRelease.charge != wrapperCharge {
				t.Fatal("consumption lost original pins or deferred wrapper charge")
			}
			if fail {
				// Genuine original producer failure at last physical release;
				// no synthetic callback stands in for the actual close.
				if err = token.pinned.Close(); err != nil {
					t.Fatal(err)
				}
			}
			siblingResult := visible.ReleaseWithError()
			if (siblingResult != nil) != fail {
				t.Fatalf("sibling physical outcome=%v fail=%t", siblingResult, fail)
			}
			if !fail && callbacks != 1 {
				t.Fatal("last ordinary owner did not run exact original callback")
			}
			beforeReplay, priorCallbacks := a.MetadataOwner().Bytes(), callbacks
			visible.Release()
			source.Release()
			token.Release()
			if callbacks != priorCallbacks || a.MetadataOwner().Bytes() != beforeReplay {
				t.Fatal("ordinary result replayed original release or refunded failed debt")
			}
			if fail && (visible.releaseResult() == nil || visible.kindViews == nil || visible.kindViews.allocation.cleanupResult() == nil) {
				t.Fatal("actual ordinary failure did not retain original backing debt")
			}
			visible, source, set = nil, nil, nil // no sibling/Set keeper remains
			txn.phase = durableRootFinishing
			metadataCharge := c.metadataCharge
			var finish PrimaryConstructionReleaseWork
			if ready, err := finish.BindFinishV6(txn, work()); !ready || err != nil {
				t.Fatal(ready, err)
			}
			c = nil
			detached := false
			for n := 0; n < 32; n++ {
				ready, err := finish.ContinueFinishWorkV6(work(), nil)
				if err != nil {
					t.Fatal("native wrapper/queue Finish inherited ordinary physical failure", err)
				}
				if ready {
					t.Fatal("containing refund skipped original detached-call boundary")
				}
				if finish.FinishDetachedV6() {
					if txn.constructionV6 != nil {
						t.Fatal("accounting refund retained original physical constructor")
					}
					detached = true
					break
				}
			}
			if !detached {
				t.Fatal("original physical Finish failed to progress")
			}
			beforeAccounting := a.MetadataOwner().Bytes()
			if ready, err := finish.ContinueFinishWorkV6(work(), nil); !ready || err != nil {
				t.Fatal("original containing completion", ready, err)
			}
			if after := a.MetadataOwner().Bytes(); beforeAccounting-after != metadataCharge+wrapperCharge {
				t.Fatalf("native refunded %d bytes; original constructor+Set wrapper=%d", beforeAccounting-after, metadataCharge+wrapperCharge)
			}
			if txn.constructionV6 != nil || txn.phase != durableRootConsumed || txn.physicalCleanupError != nil || callbacks != priorCallbacks {
				t.Fatal("native Finish replayed ordinary authority")
			}
			if fail && a.MetadataOwner().Bytes() == 0 {
				t.Fatal("native terminal refunded unresolved ordinary backing")
			}
		})
	}
}
