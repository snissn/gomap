package db

import (
	"bytes"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// The selected closure belongs to an actual DATA DB. A later capture has no
// PRIMARY and no producer addition; its predecessor is the allocation authority.
func TestCaptureDurableRootUsesSelectedPredecessorOwner(t *testing.T) {
	database, path := openDurableRootClosureDB3928(t)
	ptrs := appendPointersInNewSegment(t, database.dir, 0, 1, 10000, 1, func(int) []byte { return bytes.Repeat([]byte("v"), 8192) })
	if err := database.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	batch := database.NewBatch().(*Batch)
	if err := batch.SetPointer([]byte("owned-predecessor-pointer"), ptrs[0]); err != nil {
		t.Fatal(err)
	}
	if err := batch.WriteSync(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot := database.AcquireSnapshot()
	entry, readErr := snapshot.GetEntryExact([]byte("owned-predecessor-pointer"))
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if readErr != nil || entry.ValuePtr.FileID == 0 {
		t.Fatal("control must capture a real persistent value-log dependency")
	}

	if database.idx.Load().primary != nil {
		t.Fatal("control requires the original DATA route")
	}
	original := durableRootClosureSet3928(t, path, durableRootClosureObligation3928(1))
	owner := database.valueLogIdentityPins.MetadataOwner()
	base, err := rootpublication.ImportStableResourceSetMetadata(owner, original)
	if err != nil {
		t.Fatal(err)
	}
	if err := original.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := base.ReleaseWithError(); err != nil {
			t.Error(err)
		}
	}()
	candidate, err := database.captureDurableRootResourcesFromBaseV1(
		database.idx.Load(), database.meta, nil, base, nil,
		rootpublication.StableLogicalObligationRequirements{}, rootpublication.StableLogicalObligationMutation{},
		rootpublication.StableLogicalObligationMutation{}, rootpublication.StableResourceClosureWork{},
		nil, false, nil,
	)
	if candidate != nil {
		defer func() {
			if err := candidate.ReleaseWithError(); err != nil {
				t.Error(err)
			}
		}()
	}
	if err != nil {
		t.Fatal(err)
	}
	if candidate == nil || candidate.MetadataOwner() != owner {
		t.Fatalf("candidate owner=%p want exact predecessor owner=%p", candidate.MetadataOwner(), owner)
	}
}

// This producer exposes the actual last Close outcome through the original
// owned-operation contract. Its containing receipt is admitted before birth.
type rootCaptureCloseOperations struct {
	owner      *retainedalloc.Owner
	file       *os.File
	refs       atomic.Int32
	closeCalls atomic.Int32
	charge     uint64
	registry   *rootpublication.IdentityPinRegistry
	identity   rootpublication.StableIdentity
	failure    rootpublication.OriginalReleaseFailure
}

func (o *rootCaptureCloseOperations) MetadataOwner() *retainedalloc.Owner { return o.owner }
func (o *rootCaptureCloseOperations) Retain() error                       { o.refs.Add(1); return nil }
func (o *rootCaptureCloseOperations) Release() (bool, error) {
	if o.refs.Add(-1) != 0 {
		return true, nil
	}
	o.closeCalls.Add(1)
	if err := o.file.Close(); err != nil {
		o.owner.CleanupFailed()
		return true, o.failure.RetainOriginalCleanupFailure(o, err)
	}
	o.file = nil
	if err := o.registry.Unobserve(o.identity); err != nil {
		o.owner.CleanupFailed()
		return true, o.failure.RetainOriginalCleanupFailure(o, err)
	}
	o.registry = nil
	o.identity = rootpublication.StableIdentity{}
	owner, charge := o.owner, o.charge
	o.owner = nil
	o.charge = 0
	owner.RemovePending(charge)
	return true, nil
}
func (o *rootCaptureCloseOperations) FlushThrough(*os.File, rootpublication.DurableFrontier) error {
	return nil
}
func (o *rootCaptureCloseOperations) SyncThrough(*os.File, rootpublication.DurableFrontier) error {
	return nil
}

func TestCaptureDurableRootErrorDisposesOriginalResultAndRetainsPhysicalRefusal(t *testing.T) {
	database, path := openDurableRootClosureDB3928(t)
	owner := database.valueLogIdentityPins.MetadataOwner()
	baseline := owner.Bytes()
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(rootCaptureCloseOperations{})))
	if err := owner.AddPending(charge); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.valueLogIdentityPins.Observe(identity); err != nil {
		t.Fatal(err)
	}
	operations := &rootCaptureCloseOperations{owner: owner, file: file, charge: charge, registry: database.valueLogIdentityPins, identity: identity}
	operations.refs.Store(1)
	token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
		MetadataOwner: owner, PinRegistry: database.valueLogIdentityPins, OwnedOperations: operations,
		Kind: rootpublication.ResourceColumnAsset, LogicalLane: "columns", ResourceID: "completion-result",
		Generation: 1, DiagnosticPath: "shared-column-segment.bin", File: file,
		Frontier:     rootpublication.DurableFrontier{Bytes: 4096},
		Reachability: rootpublication.ReachabilityColumnManifest, ContentSynced: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	builder, err := rootpublication.NewStableResourceSetBuilderWithMetadata(owner)
	if err != nil {
		t.Fatal(err)
	}
	if err = builder.Add(token); err != nil {
		t.Fatal(err)
	}
	original, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if consumed, err := operations.Release(); !consumed || err != nil {
		t.Fatalf("producer edge consumed=%t err=%v", consumed, err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	} // genuine last physical Close refusal
	prior := errors.New("preexisting background error")
	database.retainOriginalCleanupError(prior)
	output := original
	captureErr := errors.New("capture deferred cleanup failed")
	database.finishResourceCapture(&output, &captureErr)
	if output != nil || !errors.Is(captureErr, os.ErrClosed) {
		t.Fatalf("error result=%p error=%v want disposed result and actual Close cause", output, captureErr)
	}
	retained := database.backgroundError()
	if !errors.Is(retained, os.ErrClosed) || !errors.Is(retained, prior) {
		t.Fatalf("existing DB error graph lost original/sibling: %v", retained)
	}
	if owner.Bytes() <= baseline || owner.PhysicalClosed() || operations.file != file {
		t.Fatalf("failed original payload/refund bytes=%d baseline=%d closed=%t file=%p", owner.Bytes(), baseline, owner.PhysicalClosed(), operations.file)
	}
	if repeated := original.ReleaseWithError(); !errors.Is(repeated, os.ErrClosed) {
		t.Fatalf("repeated original outcome=%v", repeated)
	}
	if operations.closeCalls.Load() != 1 || operations.refs.Load() != 0 {
		t.Fatalf("physical replay calls=%d refs=%d", operations.closeCalls.Load(), operations.refs.Load())
	}
}
