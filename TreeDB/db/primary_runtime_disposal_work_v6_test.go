package db

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Drive the original runtime descriptor, not a replacement cleanup owner.
// The empty Set callback below has no physical producer operations: this test
// proves queue/descriptor continuation, not support for arbitrary Set classes.
func TestPrimaryRuntimeDisposalRetainsOriginalQueueAndAccountingV6(t *testing.T) {
	a, err := primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	ref, ready, err := a.PrepareReadRootV6(nil)
	if !ready || err != nil {
		t.Fatal(ready, err)
	}
	r := &primaryDurableRuntimeV5{}
	charge := primaryRuntimeDescriptorChargeV5()
	if err = a.MetadataOwner().AddPending(charge); err != nil {
		t.Fatal(err)
	}
	bindReservedPrimaryRuntimeV5(r, a)
	r.records[0] = ref
	set, err := rootpublication.NewStableResourceSetBuilder().Freeze()
	if err != nil {
		t.Fatal(err)
	}
	r.slotResources[0] = set
	before := a.MetadataOwner().Bytes()
	calls, effects := 0, 0
	release := func(s *rootpublication.StableResourceSet, w *iterator.OrdinalScanWork) (bool, error) {
		calls++
		if !w.Reserve(1, 8) {
			return false, nil
		}
		effects++
		return true, s.ReleaseWithError()
	}
	if _, ready, err := continuePrimaryRuntimeDisposalV6(a, r, &iterator.OrdinalScanWork{}, release); ready || err != nil || r.disposalPhase != 0 || r.records[0] != ref || a.MetadataOwner().Bytes() != before {
		t.Fatalf("before-effects refusal changed original descriptor: %t %v", ready, err)
	}
	// Admit initialization, the slot's alias scrub, original Drop and no later
	// progress. Ordinary cleanup cannot drain this native-owned physical queue.
	_, ready, err = continuePrimaryRuntimeDisposalV6(a, r, &iterator.OrdinalScanWork{RecordLimit: 5, ByteLimit: 1 << 20}, release)
	if ready || err != nil || r.records[0] != (primaryarena.Ref{}) || effects != 0 {
		t.Fatalf("drop pause: %t %v phase%d", ready, err, r.disposalPhase)
	}
	drops, freed := a.Counters().RootDrops, a.Counters().BanksFreed
	if ok, progress, err := a.ReleaseStep(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); !ok || progress || err != nil || a.Counters().BanksFreed != freed {
		t.Fatalf("ordinary release assisted private queue: %t %t %v", ok, progress, err)
	}
	// Refuse the resource callback once AFTER the private root really finishes.
	allow := false
	paused := func(s *rootpublication.StableResourceSet, w *iterator.OrdinalScanWork) (bool, error) {
		if !allow {
			return false, nil
		}
		return release(s, w)
	}
	var completion primaryRuntimeDisposalCompletionV6
	for i := 0; i < 16 && r.disposalPhase < 6; i++ {
		completion, ready, err = continuePrimaryRuntimeDisposalV6(a, r, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}, paused)
		if ready || err != nil {
			t.Fatalf("pre-resource continuation: %t %v", ready, err)
		}
	}
	if r.disposalPhase != 6 || r.slotResources[0] != set || r.metadataCharge != charge || a.Counters().RootDrops != drops || a.Counters().BanksFreed != freed+1 || calls != 0 {
		t.Fatalf("same original resource/charge did not survive pause: phase%d calls%d", r.disposalPhase, calls)
	}
	allow = true
	for i := 0; i < 16; i++ {
		completion, ready, err = continuePrimaryRuntimeDisposalV6(a, r, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}, paused)
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			break
		}
	}
	if !ready || effects != 1 || set.Owner() != rootpublication.ResourceOwnerReleased || r.records[0] != (primaryarena.Ref{}) || r.slotResources[0] != nil || r.metadata != nil || r.metadataCharge != 0 || a.Counters().RootDrops != drops {
		t.Fatalf("original completion: %t effects%d phase%d", ready, effects, r.disposalPhase)
	}
	// The physical driver returned and the owner ends its runtime alias BEFORE
	// the accounting-only value refunds the containing allocation.
	owner := a.MetadataOwner()
	pending := owner.Bytes()
	if pending < charge || completion.charge != charge {
		t.Fatal("containing charge refunded within physical callback")
	}
	r = nil
	completion.complete()
	if owner.Bytes() != pending-charge {
		t.Fatal("accounting did not follow actual physical return")
	}
	completion.complete()
	if owner.Bytes() != pending-charge {
		t.Fatal("accounting replayed")
	}
}

// The callback invokes the real faulted producer only as an explicit physical
// failure discriminator. It is not an admissible bounded class implementation.
func TestPrimaryRuntimeDisposalPhysicalFailureIsOriginalAndStickyV6(t *testing.T) {
	a, err := primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	r := &primaryDurableRuntimeV5{}
	charge := primaryRuntimeDescriptorChargeV5()
	if err = a.MetadataOwner().AddPending(charge); err != nil {
		t.Fatal(err)
	}
	bindReservedPrimaryRuntimeV5(r, a)
	set := originalRecoverySetWithPhysicalCloseFault(t)
	r.slotResources[0] = set
	calls := 0
	release := func(s *rootpublication.StableResourceSet, w *iterator.OrdinalScanWork) (bool, error) {
		if !w.Reserve(1, 8) {
			return false, nil
		}
		calls++
		err := s.ReleaseWithError()
		return err == nil, err
	}
	_, ready, first := continuePrimaryRuntimeDisposalV6(a, r, &iterator.OrdinalScanWork{RecordLimit: 100, ByteLimit: 1 << 20}, release)
	if ready || !errors.Is(first, os.ErrClosed) || r.disposalErr != first || r.slotResources[0] != set || r.metadataCharge != charge || calls != 1 {
		t.Fatalf("physical refusal lost original custody: %t %v calls%d", ready, first, calls)
	}
	_, ready, again := continuePrimaryRuntimeDisposalV6(a, r, &iterator.OrdinalScanWork{}, release)
	if ready || again != first || calls != 1 || r.slotResources[0] != set {
		t.Fatal("original physical callback replayed or first receipt replaced")
	}
}
