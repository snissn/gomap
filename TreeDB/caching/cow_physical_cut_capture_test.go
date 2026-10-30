package caching

import (
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func testPhysicalCutCapture(t *testing.T, db *DB, c *cowCache) **cowPhysicalCutCapture {
	t.Helper()
	registry := db.backend.(*backenddb.DB).StableResourceIdentityPinRegistry()
	owner, err := newCOWPhysicalCutCapture(c.maintenance, registry, registry.MetadataOwner())
	if err != nil {
		t.Fatal(err)
	}
	owned := &owner
	t.Cleanup(func() {
		if *owned != nil {
			if err := finishTestPhysicalCutCapture(owned); err != nil {
				t.Errorf("finish cut capture: %v", err)
			}
		}
	})
	return owned
}

type refusingPhysicalCutAllocator struct{}

func (refusingPhysicalCutAllocator) AdmitNativeBytes(uint64) (*memtable.COWExternalLease, error) {
	return nil, memtable.ErrCOWCapacity
}

func TestCOWPhysicalCutCaptureAdmissionRefusal(t *testing.T) {
	db, c := cowCutFixture(t)
	registry := db.backend.(*backenddb.DB).StableResourceIdentityPinRegistry()
	owner, err := newCOWPhysicalCutCapture(refusingPhysicalCutAllocator{}, registry, registry.MetadataOwner())
	if owner != nil || !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("owner=%v err=%v", owner, err)
	}
	closedRegistry := rootpublication.NewIdentityPinRegistry()
	closedRegistry.Close()
	before := c.maintenance.Stats()
	owner, err = newCOWPhysicalCutCapture(c.maintenance, closedRegistry, closedRegistry.MetadataOwner())
	if owner != nil || err == nil {
		t.Fatalf("closed registry owner=%v err=%v", owner, err)
	}
	after := c.maintenance.Stats()
	if before.ExternalBytes != after.ExternalBytes || before.ExternalLeases != after.ExternalLeases {
		t.Fatalf("failed custody admission leaked wrapper charge: before=%+v after=%+v", before, after)
	}
}

func TestCOWPhysicalCutCaptureConcurrentCacheClose(t *testing.T) {
	db, c := cowCutFixture(t)
	cleanupTestPhysicalLifecycle(t, c)
	opening := c.retainCut()
	if opening == nil {
		t.Fatal("retain opening")
	}
	releaseOpening := testPhysicalLifecycleEdge(t, c, opening)
	owned := testPhysicalCutCapture(t, db, c)
	c.writerMu.Lock()
	token, err := c.registerPhysicalLifecycleLocked(opening)
	outcome := (*owned).joinLocked(c, opening)
	retired, finishErr := c.finishPhysicalLifecycleLocked(token)
	c.writerMu.Unlock()
	releaseRetired := testPhysicalLifecycleEdge(t, c, retired)
	if err != nil || outcome != cowPhysicalLifecycleJoined || finishErr != nil {
		t.Fatalf("register=%v join=%v finish=%v", err, outcome, finishErr)
	}
	if err := releaseRetired(); err != nil {
		t.Fatal(err)
	}
	if err := releaseOpening(); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	closed := make(chan error, 1)
	finished := make(chan error, 1)
	go func() { <-start; closed <- c.close() }()
	go func() { <-start; finished <- finishTestPhysicalCutCapture(owned) }()
	close(start)
	closeErr, captureErr := <-closed, <-finished
	if closeErr != nil || captureErr != nil || c.activeCuts.Load() != 0 {
		t.Fatalf("cache close=%v capture finish=%v active=%d", closeErr, captureErr, c.activeCuts.Load())
	}
}

// This synthetic cut uses a real refused Snapshot completion and its original
// cleanup receipt. It does not represent a captured checkpoint frontier.
func TestCOWPhysicalCutCaptureKeepsFailedCleanup(t *testing.T) {
	db, c := cowCutFixture(t)
	cleanupTestPhysicalLifecycle(t, c)
	owned := testPhysicalCutCapture(t, db, c)
	backend := db.backend.(*backenddb.DB)
	var basis *cowBackendBasis
	snapshot, err := backend.AcquireSnapshotWithAllocationAdmission(func(s backenddb.SnapshotAllocationSizes) error {
		var e error
		basis, e = admitCOWBackendBasis(c.budget, s)
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	basisOwnsSnapshot := false
	t.Cleanup(func() {
		if basis != nil {
			if !basisOwnsSnapshot {
				if err := snapshot.Close(); err != nil {
					t.Errorf("close unadopted snapshot: %v", err)
				}
			}
			if err := basis.release(); err != nil && !(basisOwnsSnapshot && errors.Is(err, rootpublication.ErrResourceOwnership)) {
				t.Errorf("release synthetic basis: %v", err)
			}
		}
	})
	receipt := &cowBasisCompletionReceipt{}
	if err := snapshot.AdoptSnapshotCleanupCompletion(receipt); err != nil {
		t.Fatal(err)
	}
	err = basis.adoptSnapshot(snapshot)
	basisOwnsSnapshot = true // adoptSnapshot assigned this exact snapshot before returning.
	if !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("expected real completion refusal, got %v", err)
	}
	lease, err := c.budget.AcquireExternal(cowCutCharge(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	cut := &cowReadCut{refs: 1, cache: c, basis: basis, lease: lease}
	basis = nil // The synthetic cut now owns the original basis edge.
	c.activeCuts.Add(1)
	releaseInitial := testPhysicalLifecycleEdge(t, c, cut)
	c.writerMu.Lock()
	token, err := c.registerPhysicalLifecycleLocked(cut)
	outcome := (*owned).joinLocked(c, cut)
	retired, finishErr := c.finishPhysicalLifecycleLocked(token)
	c.writerMu.Unlock()
	releaseRetired := testPhysicalLifecycleEdge(t, c, retired)
	if err != nil || outcome != cowPhysicalLifecycleJoined || finishErr != nil {
		t.Fatalf("register=%v join=%v finish=%v", err, outcome, finishErr)
	}
	if err := releaseRetired(); err != nil {
		t.Fatal(err)
	}
	if err := releaseInitial(); err != nil {
		t.Fatal(err)
	}
	failedOwner := *owned
	before := c.maintenance.Stats()
	err = finishTestPhysicalCutCapture(owned)
	if !errors.Is(err, rootpublication.ErrResourceOwnership) || failedOwner.retention == nil || !errors.Is(failedOwner.failure, err) || receipt.calls != 1 {
		t.Fatalf("lost original failed owner: err=%v receipt calls=%d", err, receipt.calls)
	}
	after := c.maintenance.Stats()
	if after.ExternalBytes != before.ExternalBytes || after.ExternalLeases != before.ExternalLeases {
		t.Fatalf("failed wrapper refunded its charge: before=%+v after=%+v", before, after)
	}
	failedOwner = nil // The original registry custody owns the exact failed wrapper.
}

func finishTestPhysicalCutCapture(owned **cowPhysicalCutCapture) error {
	owner := *owned
	*owned = nil
	retention, err := owner.finish()
	owner = nil
	if retention != nil {
		retention.Release()
	}
	return err
}

// This tests independent edge lifetime on the existing single registration,
// not frontier inventory, graph acceptance, or physical handoff.
func TestCOWPhysicalCutCaptureOutlivesRegistration(t *testing.T) {
	db, c := cowCutFixture(t)
	cleanupTestPhysicalLifecycle(t, c)
	opening := c.retainCut()
	if opening == nil {
		t.Fatal("retain opening")
	}
	releaseOpening := testPhysicalLifecycleEdge(t, c, opening)
	owned := testPhysicalCutCapture(t, db, c)
	c.writerMu.Lock()
	token, err := c.registerPhysicalLifecycleLocked(opening)
	outcome := (*owned).joinLocked(c, opening)
	retired, finishErr := c.finishPhysicalLifecycleLocked(token)
	c.writerMu.Unlock()
	releaseRetired := testPhysicalLifecycleEdge(t, c, retired)
	if err != nil || outcome != cowPhysicalLifecycleJoined || finishErr != nil {
		t.Fatalf("register=%v join=%v finish=%v", err, outcome, finishErr)
	}
	if (*owned).cut != opening || (*owned).identity.owner != c || (*owned).identity.epoch != token.epoch {
		t.Fatal("capture does not own the registered cut")
	}
	if err := releaseRetired(); err != nil {
		t.Fatal(err)
	}
	if err := releaseOpening(); err != nil {
		t.Fatal(err)
	}
	if err := c.close(); err != nil {
		t.Fatal(err)
	}
	if c.activeCuts.Load() != 1 {
		t.Fatal("owner did not keep the original cut alive")
	}
	if err := finishTestPhysicalCutCapture(owned); err != nil {
		t.Fatal(err)
	}
	if c.activeCuts.Load() != 0 {
		t.Fatalf("active cuts=%d", c.activeCuts.Load())
	}
}

func TestCOWPhysicalCutCaptureNoRegistration(t *testing.T) {
	db, c := cowCutFixture(t)
	opening := c.retainCut()
	if opening == nil {
		t.Fatal("retain opening")
	}
	releaseOpening := testPhysicalLifecycleEdge(t, c, opening)
	owned := testPhysicalCutCapture(t, db, c)
	c.writerMu.Lock()
	outcome := (*owned).joinLocked(c, opening)
	c.writerMu.Unlock()
	if outcome != cowPhysicalLifecycleNoRegistration || (*owned).cut != nil {
		t.Fatalf("outcome=%v cut=%p", outcome, (*owned).cut)
	}
	if err := finishTestPhysicalCutCapture(owned); err != nil {
		t.Fatal(err)
	}
	if err := releaseOpening(); err != nil {
		t.Fatal(err)
	}
}
