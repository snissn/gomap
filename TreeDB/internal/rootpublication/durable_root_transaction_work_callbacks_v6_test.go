package rootpublication

import (
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// The original error-return coordinator adapters must refuse opaque native
// callbacks before effects, including when the ordinary adapter supplies nil.
func TestNativeTransactionCallbacksRefuseOpaqueV6(t *testing.T) {
	for _, consume := range []bool{false, true} {
		for _, supplied := range []bool{false, true} {
			calls := 0
			txn := &DurableRootTransaction{
				owner:          ResourceOwnerCoordinator,
				phase:          durableRootPrepared,
				constructionV6: &primaryTransactionConstructionV6{nativeWorkOwned: true},
				activate:       func(DurableRootCallbackInput) error { calls++; return nil },
				consume:        func(DurableRootCallbackInput) error { calls++; return nil },
			}
			if consume {
				txn.phase = durableRootActivated
			}
			before := txn.phase
			var w *iterator.OrdinalScanWork
			if supplied {
				w = &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
			}
			var err error
			if consume {
				err = txn.consumeFromCoordinatorWithWork(w)
			} else {
				err = txn.activateFromCoordinatorWithWork(w)
			}
			if !errors.Is(err, ErrNativeMVCCRootWorkRequiredV6) || calls != 0 || txn.phase != before {
				t.Errorf("consume=%t supplied=%t: err=%v calls=%d phase=%d, want unchanged phase=%d", consume, supplied, err, calls, txn.phase, before)
			}
		}
	}
}

// A real arena Drop allowance refusal must retain the original constructor and
// root so a funded retry can finish; it is not a terminal physical failure.
func TestPrimaryFinishWorkRefusalResumesOriginalV6(t *testing.T) {
	a, err := primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	original, ready, err := NewPrimaryConstructionTransactionV6(a, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if !ready || err != nil {
		t.Fatal(ready, err)
	}
	txn := original.(*DurableRootTransaction)
	set, err := NewStableResourceSetBuilder().Freeze()
	if err != nil {
		t.Fatal(err)
	}
	set.owner.Store(uint32(ResourceOwnerCoordinator))
	txn.constructionV6.resources = set
	txn.owner, txn.phase = ResourceOwnerCoordinator, durableRootFinishing
	construction, root, counters := txn.constructionV6, txn.constructionV6.root, a.Counters()
	completion, err := txn.finishOrdinaryPrimaryV6(&iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}, nil)
	if completion != nil || !errors.Is(err, ErrNativeMVCCRootWorkRequiredV6) {
		t.Fatalf("allowance refusal: completion=%p err=%v", completion, err)
	}
	if txn.physicalCleanupError != nil || txn.constructionV6 != construction || construction.root != root || txn.phase != durableRootFinishing || txn.finishBusy || set.Owner() != ResourceOwnerCoordinator || a.Counters() != counters {
		t.Fatal("allowance refusal changed original custody or became terminal")
	}
	completion, err = txn.finishOrdinaryPrimaryV6(&iterator.OrdinalScanWork{RecordLimit: 100, ByteLimit: 1 << 20}, nil)
	if completion == nil || err != nil {
		t.Fatalf("funded retry: %p %v", completion, err)
	}
	if txn.constructionV6 != nil || txn.owner != ResourceOwnerReleased || txn.phase != durableRootConsumed || set.releaseResult() != nil {
		t.Fatal("retry did not complete original physical owner")
	}
	completePrimaryTerminalsV6(&completion)
	completePrimaryTerminalsV6(&completion)
}

// This constructs the actual native transaction, certificate and prepared root;
// only its empty resource Set is kept deliberately small. It does not certify
// the DB driver's final publication window or a physical Set release bound.
func nativeCallbackBindingFixtureV6(t *testing.T) (*DurableRootTransaction, *StableResourceSet, DurableRootTransactionSpec, *primaryarena.Arena) {
	t.Helper()
	a, err := primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "primary"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := a.Close(); err != nil {
			t.Error(err)
		}
	})
	w := &iterator.OrdinalScanWork{RecordLimit: 100, ByteLimit: 1 << 20}
	original, ready, err := NewNativeMVCCPrimaryConstructionTransactionV6(a, w)
	if !ready || err != nil {
		t.Fatal(ready, err)
	}
	txn := original.(*DurableRootTransaction)
	ref := txn.PrimaryConstructionReferenceV6()
	data := freelist.NewMemoryPageStoreV1()
	base, system := primaryRootTestLeaf(2, "a", "old", 1), primaryRootTestLeaf(3, "system", "s", 1)
	data.Pages[2], data.Pages[3] = base, system
	image := make([]byte, page.PageSize)
	if err := node.EncodePrimaryDirectory(image, ref.PageID, 1, node.PrimaryOperand{Ref: page.PageChildRef(2), Digest: sha256.Sum256(base)}, nil); err != nil {
		t.Fatal(err)
	}
	if ready, err := txn.SealPrimaryConstructionV6(image, [1]*primaryarena.Group{}, w); !ready || err != nil {
		t.Fatal(ready, err)
	}
	frontier := NewFrontier(1, ref.PageID, 3, 0, 1)
	prepared, ready, err := NewPreparedPrimaryReadRootCertificateV6(txn.PrimaryConstructionCertificateV6(), freelist.MustNewFreelistGenerationV1(1, 4, nil, nil), nil, frontier, data, nil, w)
	if !ready || err != nil {
		t.Fatal(ready, err)
	}
	set, err := NewStableResourceSetBuilder().Freeze()
	if err != nil {
		t.Fatal(err)
	}
	noop := func(DurableRootCallbackInput) error { return nil }
	return txn, set, DurableRootTransactionSpec{Lineage: DurableRootLineageID{1}, Sequence: 1, PreparedPrimary: prepared, Activate: noop, Consume: noop, Abort: noop, Fail: func(DurableRootCallbackInput, error) error { return nil }}, a
}

func TestNativeTransactionBindingUsesSuppliedWorkV6(t *testing.T) {
	txn, set, spec, a := nativeCallbackBindingFixtureV6(t)
	ordinary, activated, consumed := 0, 0, 0
	spec.Activate = func(DurableRootCallbackInput) error { ordinary++; return nil }
	spec.Consume = spec.Activate
	var supplied *iterator.OrdinalScanWork
	callback := func(effects *int) func(DurableRootCallbackInput, *iterator.OrdinalScanWork) (bool, error) {
		return func(input DurableRootCallbackInput, w *iterator.OrdinalScanWork) (bool, error) {
			if w != supplied || input.PreparedPrimary != spec.PreparedPrimary || input.Sequence != 1 {
				t.Fatal("callback lost exact supplied work/input")
			}
			if !w.Reserve(1, 8) {
				return false, nil
			}
			*effects++
			return true, nil
		}
	}
	activate, consume := callback(&activated), callback(&consumed)
	counts := a.Counters()
	root := txn.constructionV6.root
	for _, partial := range []int{0, 1, 2} {
		spec.ActivateWithWork, spec.ConsumeWithWork = nil, nil
		if partial == 1 {
			spec.ActivateWithWork = activate
		}
		if partial == 2 {
			spec.ConsumeWithWork = consume
		}
		ready, err := txn.BindPrimaryVisibilityV6(spec, set, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if ready || !errors.Is(err, ErrNativeMVCCRootWorkRequiredV6) || txn.phase != durableRootConstructingV6 || a.Counters() != counts || set.Owner() != ResourceOwnerBuilder {
			t.Fatalf("incomplete typed binding %d changed authority: %t %v", partial, ready, err)
		}
	}
	spec.ActivateWithWork, spec.ConsumeWithWork = activate, consume
	// The dispatch fits, but the joint acquire+detach does not. Neither bank
	// effect may occur and there must be no compensating unlimited Drop.
	if ready, err := txn.BindPrimaryVisibilityV6(spec, set, &iterator.OrdinalScanWork{RecordLimit: 2, ByteLimit: 1 << 20}); ready || err != nil || txn.phase != durableRootConstructingV6 || a.Counters() != counts || txn.constructionV6.root != root {
		t.Fatalf("joint arena refusal changed original custody: %t %v", ready, err)
	}
	bindWork := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if ready, err := txn.BindPrimaryVisibilityV6(spec, set, bindWork); !ready || err != nil {
		t.Fatalf("actual native bind: %t %v", ready, err)
	}
	t.Logf("native binding stage only: records=%d bytes=%d", bindWork.Records, bindWork.Bytes)
	if err := set.transfer(ResourceOwnerBuilder, ResourceOwnerCoordinator); err != nil {
		t.Fatal(err)
	}
	if err := txn.transfer(ResourceOwnerBuilder, ResourceOwnerCoordinator); err != nil {
		t.Fatal(err)
	}
	if err := txn.activateFromCoordinator(); !errors.Is(err, ErrNativeMVCCRootWorkRequiredV6) || ordinary != 0 {
		t.Fatalf("native nil fallback: %v", err)
	}
	supplied = &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}
	if ready, err := txn.activateFromCoordinatorStepWithWorkV6(supplied); ready || err != nil || activated != 0 || txn.phase != durableRootPrepared {
		t.Fatalf("activation refusal: %t %v", ready, err)
	}
	supplied = &iterator.OrdinalScanWork{RecordLimit: 2, ByteLimit: 1 << 20}
	if ready, err := txn.activateFromCoordinatorStepWithWorkV6(supplied); !ready || err != nil || activated != 1 || txn.phase != durableRootActivated {
		t.Fatalf("activation retry: %t %v", ready, err)
	}
	if ready, err := txn.activateFromCoordinatorStepWithWorkV6(supplied); ready || !errors.Is(err, ErrDurableRootOwnership) || activated != 1 {
		t.Fatalf("activation replay: %t %v", ready, err)
	}
	supplied = &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}
	if err := txn.consumeFromCoordinatorWithWork(supplied); !errors.Is(err, ErrNativeMVCCRootWorkRequiredV6) || consumed != 0 || txn.phase != durableRootActivated {
		t.Fatalf("consumption refusal adapter: %v", err)
	}
	supplied = &iterator.OrdinalScanWork{RecordLimit: 2, ByteLimit: 1 << 20}
	if ready, err := txn.consumeFromCoordinatorStepWithWorkV6(supplied); !ready || err != nil || consumed != 1 || ordinary != 0 || txn.phase != durableRootFinishing {
		t.Fatalf("consumption retry: %t %v", ready, err)
	}
	if ready, err := txn.consumeFromCoordinatorStepWithWorkV6(supplied); ready || !errors.Is(err, ErrDurableRootOwnership) || consumed != 1 {
		t.Fatalf("consumption replay: %t %v", ready, err)
	}
	if _, _, err := txn.finishPrimaryStepWithWorkV6(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}, nil, nil); !errors.Is(err, ErrNativeMVCCRootWorkRequiredV6) {
		t.Fatalf("native synchronous release fallback: %v", err)
	}
	// This empty test Set has no physical callbacks. The production native
	// resource driver and receipt remain a separate integration obligation.
	release := func(s *StableResourceSet, pins map[ResourceKind]uint64, w *iterator.OrdinalScanWork) (bool, error) {
		if !w.Reserve(1, 8) {
			return false, nil
		}
		for {
			ready, progress, err := a.ReleaseQueueStep(&txn.constructionV6.cleanupQueue, w)
			if !ready || err != nil {
				return false, err
			}
			if !progress {
				break
			}
		}
		return true, s.releaseFromWithActivePins(ResourceOwnerCoordinator, pins)
	}
	completion, ready, err := txn.finishPrimaryStepWithWorkV6(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}, nil, release)
	if !ready || err != nil || completion == nil {
		t.Fatal(ready, err)
	}
	if txn.activateWithWork != nil || txn.consumeWithWork != nil || txn.constructionV6 != nil || txn.finishBusy {
		t.Fatal("terminal retained callback/construction aliases")
	}
	if !txn.mu.TryLock() {
		t.Fatal("Finish returned while holding mutex")
	}
	txn.mu.Unlock()
	completePrimaryTerminalsV6(&completion)
	if ready, err := a.Drop(root, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); !ready || err != nil {
		t.Fatalf("independent state edge: %t %v", ready, err)
	}
	if activated != 1 || consumed != 1 || ordinary != 0 {
		t.Fatal("callbacks replayed")
	}
}

func TestNativeTransactionOrdinaryNilCallbacksUnchangedV6(t *testing.T) {
	ordinary, typed := 0, 0
	txn := &DurableRootTransaction{owner: ResourceOwnerCoordinator, phase: durableRootPrepared}
	txn.activate = func(DurableRootCallbackInput) error { ordinary++; return nil }
	txn.consume = txn.activate
	txn.activateWithWork = func(DurableRootCallbackInput, *iterator.OrdinalScanWork) (bool, error) { typed++; return true, nil }
	txn.consumeWithWork = txn.activateWithWork
	if err := txn.activateFromCoordinator(); err != nil {
		t.Fatal(err)
	}
	if err := txn.consumeFromCoordinator(); err != nil {
		t.Fatal(err)
	}
	if ordinary != 2 || typed != 0 || txn.phase != durableRootConsumed || txn.Owner() != ResourceOwnerReleased {
		t.Fatal("ordinary nil driver changed callbacks/stages")
	}
}

func TestPrimaryFinishResourceRefusalAndFailureCustodyV6(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry", true: "failure"}[fail], func(t *testing.T) {
			a, err := primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "primary"))
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			original, ready, err := NewNativeMVCCPrimaryConstructionTransactionV6(a, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
			if !ready || err != nil {
				t.Fatal(ready, err)
			}
			txn := original.(*DurableRootTransaction)
			set, err := NewStableResourceSetBuilder().Freeze()
			if err != nil {
				t.Fatal(err)
			}
			set.owner.Store(uint32(ResourceOwnerCoordinator))
			txn.constructionV6.resources = set
			txn.owner, txn.phase = ResourceOwnerCoordinator, durableRootFinishing
			originalConstruction := txn.constructionV6
			metadata := a.MetadataOwner().Bytes()
			calls, effects := 0, 0
			cause := errors.New("resource release refused physically")
			release := func(s *StableResourceSet, pins map[ResourceKind]uint64, w *iterator.OrdinalScanWork) (bool, error) {
				calls++
				if !w.Reserve(1, 8) {
					return false, nil
				}
				if fail {
					effects++
					return false, cause
				}
				for {
					ready, progress, err := a.ReleaseQueueStep(&originalConstruction.cleanupQueue, w)
					if !ready || err != nil {
						return false, err
					}
					if !progress {
						break
					}
				}
				effects++
				return true, s.releaseFromWithActivePins(ResourceOwnerCoordinator, pins)
			}
			completion, ready, err := txn.finishPrimaryStepWithWorkV6(&iterator.OrdinalScanWork{RecordLimit: 3, ByteLimit: 1 << 20}, nil, release)
			if ready || err != nil || completion != nil || calls != 1 || effects != 0 || txn.physicalCleanupError != nil || txn.constructionV6 != originalConstruction || originalConstruction.root != (primaryarena.Ref{}) || txn.finishBusy || a.MetadataOwner().Bytes() != metadata {
				t.Fatalf("resource allowance refusal lost original custody: ready=%t err=%v calls=%d effects=%d", ready, err, calls, effects)
			}
			drops := a.Counters().RootDrops
			freed := a.Counters().BanksFreed
			if ready, progress, err := a.ReleaseStep(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); !ready || progress || err != nil || a.Counters().BanksFreed != freed {
				t.Fatalf("ordinary writer assisted private native Finish queue: %t %t %v", ready, progress, err)
			}
			expectedCalls := 2
			if !fail {
				// Descriptor and release dispatch fit; actual queue work does not.
				completion, ready, err = txn.finishPrimaryStepWithWorkV6(&iterator.OrdinalScanWork{RecordLimit: 11, ByteLimit: 1 << 20}, nil, release)
				if ready || err != nil || completion != nil || effects != 0 || a.Counters().BanksFreed != freed || txn.constructionV6 != originalConstruction {
					t.Fatalf("private queue refusal lost continuation: %t %v", ready, err)
				}
				expectedCalls++
			}
			completion, ready, err = txn.finishPrimaryStepWithWorkV6(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}, nil, release)
			if a.Counters().RootDrops != drops || effects != 1 || calls != expectedCalls {
				t.Fatal("retry replayed original root or resource effects")
			}
			if fail {
				if ready || completion != nil || !errors.Is(err, cause) || txn.constructionV6 != originalConstruction {
					t.Fatalf("physical failure: %t %v", ready, err)
				}
				_, againReady, again := txn.finishPrimaryStepWithWorkV6(&iterator.OrdinalScanWork{}, nil, release)
				if againReady || again != err || effects != 1 || calls != expectedCalls {
					t.Fatal("physical failure replayed or replaced first receipt")
				}
				if abort := txn.Abort(); abort != err {
					t.Fatal("abort replaced first physical receipt")
				}
				return
			}
			if !ready || err != nil || completion == nil || txn.constructionV6 != nil || a.Counters().BanksFreed != freed+1 {
				t.Fatalf("resumed Finish with original queue drained: %t %v", ready, err)
			}
			completePrimaryTerminalsV6(&completion)
			completePrimaryTerminalsV6(&completion)
		})
	}
}

func TestNativeTransactionCallbackErrorIdentityV6(t *testing.T) {
	cause := errors.New("activation failed before visibility")
	calls := 0
	txn := &DurableRootTransaction{owner: ResourceOwnerCoordinator, phase: durableRootPrepared, constructionV6: &primaryTransactionConstructionV6{nativeWorkOwned: true}}
	txn.activateWithWork = func(DurableRootCallbackInput, *iterator.OrdinalScanWork) (bool, error) { calls++; return false, cause }
	txn.consumeWithWork = func(DurableRootCallbackInput, *iterator.OrdinalScanWork) (bool, error) {
		t.Fatal("consumption ran after failed activation")
		return false, nil
	}
	if err := txn.activateFromCoordinatorWithWork(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); err != cause || calls != 1 || txn.phase != durableRootPrepared || txn.physicalCleanupError != nil {
		t.Fatalf("typed callback changed original error/custody: %v", err)
	}
}
