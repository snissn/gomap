package rootpublication

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
)

func TestNativePreparedAbortUsesOriginalBuilderOrCandidateReceiptV6(t *testing.T) {
	for _, candidateOwner := range []bool{false, true} {
		t.Run(map[bool]string{false: "builder", true: "candidate"}[candidateOwner], func(t *testing.T) {
			txn, legacy, spec, a := nativeCallbackBindingFixtureV6(t)
			if err := legacy.ReleaseWithError(); err != nil {
				t.Fatal(err)
			}
			kinds, err := newResourceKindBacking(a.MetadataOwner(), nil)
			if err != nil {
				t.Fatal(err)
			}
			set, err := newStableResourceSetFromKindViews(kinds)
			if err != nil {
				t.Fatal(err)
			}
			ref := txn.PrimaryConstructionReferenceV6()
			var queue primaryarena.ReleaseQueue
			a.InitializeReleaseQueue(&queue)
			ordinary, aborted, completed := 0, 0, 0
			spec.Abort = func(DurableRootCallbackInput) error { ordinary++; return nil }
			spec.AbortComplete = spec.Abort
			spec.ActivateWithWork = func(DurableRootCallbackInput, *iterator.OrdinalScanWork) (bool, error) { return true, nil }
			spec.ConsumeWithWork = spec.ActivateWithWork
			spec.AbortWithWork = func(input DurableRootCallbackInput, w *iterator.OrdinalScanWork) (bool, error) {
				if input.PreparedPrimary == nil || input.Sequence != 1 {
					t.Fatal("wrong original abort input")
				}
				if !w.Reserve(1, 64) {
					return false, nil
				}
				ready, err := a.DropToReleaseQueue(ref, &queue, w)
				if ready && err == nil {
					aborted++
				}
				return ready, err
			}
			spec.AbortCompleteWithWork = func(input DurableRootCallbackInput, w *iterator.OrdinalScanWork) (bool, error) {
				if !w.Reserve(1, 64) {
					return false, nil
				}
				ready, progress, err := a.ReleaseQueueStep(&queue, w)
				if ready && !progress && err == nil {
					completed++
					return true, nil
				}
				return false, err
			}
			if ready, err := txn.BindPrimaryVisibilityV6(spec, set, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); !ready || err != nil {
				t.Fatal(ready, err)
			}
			if candidateOwner {
				candidate, err := NewPreparedRootCandidate(CandidateSpec{Frontier: spec.PreparedPrimary.frontier, TotalPages: spec.PreparedPrimary.generation.HighWater(), ResourceSet: set, DurableRoot: txn})
				if err != nil {
					t.Fatal(err)
				}
				if candidate.DurableRoot() != txn || candidate.Resources() != set {
					t.Fatal("candidate replaced original ownership")
				}
				candidate = nil
			}
			if err := txn.Abort(); !errors.Is(err, ErrNativeMVCCRootWorkRequiredV6) && !errors.Is(err, ErrDurableRootOwnership) {
				t.Fatal("ordinary abort accepted native callback", err)
			}
			if ordinary != 0 {
				t.Fatal("native called opaque ordinary abort")
			}
			var receipt PrimaryConstructionReleaseWork
			if ready, err := receipt.Bind(txn, &iterator.OrdinalScanWork{}); ready || err != nil {
				t.Fatal("no-work bind mutated custody", ready, err)
			}
			if ready, err := receipt.Bind(txn, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); !ready || err != nil {
				t.Fatal(ready, err)
			}
			txn, set, spec = nil, nil, DurableRootTransactionSpec{}
			for turn := 0; turn < 100; turn++ {
				w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
				ready, err := receipt.ContinueWork(&w)
				if err != nil {
					t.Fatal(turn, err)
				}
				if ready {
					if aborted != 1 || completed != 1 || ordinary != 0 {
						t.Fatal("abort callbacks replayed", aborted, completed, ordinary)
					}
					return
				}
			}
			t.Fatal("original prepared abort did not finish")
		})
	}
}
