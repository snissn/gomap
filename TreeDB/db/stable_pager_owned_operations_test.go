package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"os"
	"testing"
)

func TestOwnedPagerOperationWaitsForActualSnapshotFinalization(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	snapshot := db.AcquireStableSnapshot()
	if snapshot == nil {
		t.Fatal("capture")
	}
	token, err := snapshot.captureOwnedStableIndexFileResource()
	if err != nil {
		t.Fatal(err)
	}
	if err = snapshot.beginRead(); err != nil {
		t.Fatal(err)
	}
	token.Release()
	if got := db.stableIndexCaptures.Load(); got != 1 {
		t.Fatalf("capture released before last actual reader: count=%d", got)
	}
	if snapshot.primaryRoot == nil || snapshot.readState.Load() != snapshotReadClosedBit+1 {
		t.Fatal("retained Snapshot state lost")
	}
	snapshot.endRead()
	if snapshot.treePager != nil || snapshot.treeRoot != 0 || len(snapshot.rootTrees) != 0 {
		t.Fatal("actual completed capture retained tree/pager aliases after refund")
	}
	if got := db.stableIndexCaptures.Load(); got != 0 {
		t.Fatalf("completed capture retained count=%d", got)
	}
	if db.idx.Load().primaryOwner.failedPagerOperations != nil {
		t.Fatal("completed capture falsely retained debt")
	}
	next := db.AcquireStableSnapshot()
	defer next.Close()
	token.Release()
	if next.closed.Load() {
		t.Fatal("former callback closed independent snapshot")
	}
}

func TestOwnedPagerOperationJoinsOriginalRunningFinalizer(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	snapshot := db.AcquireStableSnapshot()
	token, err := snapshot.captureOwnedStableIndexFileResource()
	if err != nil {
		t.Fatal(err)
	}
	operations := snapshot.stablePagerCompletion
	started, proceed, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	snapshot.iteratorMu.Lock()
	snapshot.foregroundReadEnd = func() { close(started); <-proceed }
	snapshot.iteratorMu.Unlock()
	go func() { token.Release(); close(done) }()
	<-started
	for i := 0; i < 3; i++ {
		if err = snapshot.Close(); err != nil {
			t.Fatal(err)
		}
	}
	operations.completionMu.Lock()
	retained := operations.snapshot == snapshot && !operations.snapshotCompleted && operations.charge > 0 && operations.owner != nil
	operations.completionMu.Unlock()
	if !retained || db.stableIndexCaptures.Load() != 1 {
		t.Fatal("claimed finalizer was treated as completed cleanup")
	}
	next := db.AcquireStableSnapshot()
	if next == snapshot {
		t.Fatal("running finalizer exposed its original wrapper")
	}
	next.Close()
	close(proceed)
	<-done
	operations.completionMu.Lock()
	disposed := operations.snapshot == nil && operations.snapshotCompleted && operations.owner == nil && operations.charge == 0
	operations.completionMu.Unlock()
	if !disposed || db.stableIndexCaptures.Load() != 0 {
		t.Fatal("actual completion did not settle original capture once")
	}
}

func TestOwnedPagerOperationFinalReaderAndExportedHandleRace(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 16; i++ {
		snapshot := db.AcquireStableSnapshot()
		token, err := snapshot.captureOwnedStableIndexFileResource()
		if err != nil {
			t.Fatal(err)
		}
		operations := snapshot.stablePagerCompletion
		if err = snapshot.beginRead(); err != nil {
			t.Fatal(err)
		}
		gate, releaseDone, readDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
		go func() { <-gate; token.Release(); close(releaseDone) }()
		go func() { <-gate; snapshot.endRead(); close(readDone) }()
		close(gate)
		<-releaseDone
		<-readDone
		next := db.AcquireStableSnapshot()
		token.Release()
		if next.closed.Load() {
			t.Fatal("former completion closed new capture")
		}
		operations.completionMu.Lock()
		disposed := operations.snapshot == nil && operations.owner == nil && operations.charge == 0
		operations.completionMu.Unlock()
		if !disposed {
			t.Fatal("completed read retained former callback state")
		}
		next.Close()
		if db.stableIndexCaptures.Load() != 0 {
			t.Fatal("capture counter not once-only")
		}
	}
}

func TestOwnedPagerOperationPostConsumptionVLogFailureRetainsGovernor(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := d.AcquireStableSnapshot()
	if snapshot == nil || snapshot.primaryRoot == nil {
		t.Fatal("real primary capture")
	}
	faultForPostConsumptionVLogTest := errors.New("post-unlink parent sync failure")
	physicalPager := snapshot.primaryRoot.arena.Pager()
	t.Cleanup(func() {
		// The failure intentionally retains the exact owner and its admitted
		// charge. After all consumers/assertions finish, explicit fixture-only
		// physical teardown joins its worker; this is not typed debt completion.
		if closeErr := d.Close(); !errors.Is(closeErr, faultForPostConsumptionVLogTest) {
			t.Errorf("terminal owner lost original cleanup error: %v", closeErr)
		}
		_ = physicalPager.Close()
	})
	// A separate genuine manager-owned Set supplies a deterministic last-file
	// zombie. The real Snapshot finalizer consumes its original Set edge.
	dir := t.TempDir()
	id, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := valuelog.SegmentPath(dir, id)
	writer, err := valuelog.NewWriter(path, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Append(0, nil, 1, []byte("persistent pointer payload")); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	manager, err := valuelog.NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	set := manager.CurrentSetNoRefresh()
	file := set.Files[id]
	if file == nil {
		t.Fatal("actual file missing")
	}
	if snapshot.vlogPinned {
		if err = snapshot.vlogManager.Release(snapshot.state.ValueLogSet); err != nil {
			t.Fatal(err)
		}
	}
	state := *snapshot.state
	state.ValueLogSet = set
	snapshot.state = &state
	snapshot.vlogManager = manager
	snapshot.vlogPinned = true
	fault := faultForPostConsumptionVLogTest
	calls := 0
	manager.SetDeferredDeletionSync(func(_ string, _ durabilitycut.Resource) error { calls++; return fault })
	if err = manager.MarkZombie(id); err != nil {
		t.Fatal(err)
	}
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	enrollment, err := retainedalloc.EnrollPair(snapshot.primaryRoot.arena.MetadataOwner(), d.valueLogIdentityPins.MetadataOwner(), budget)
	if err != nil {
		t.Fatal(err)
	}
	if err = snapshot.AdoptPrimaryMetadataEnrollment(enrollment); err != nil {
		t.Fatal(err)
	}
	token, err := snapshot.captureOwnedStableIndexFileResource()
	if err != nil {
		t.Fatal(err)
	}
	operations := snapshot.stablePagerCompletion
	token.Release()
	if calls != 1 || set.RefCount.Load() != 0 || file.RefCount.Load() != 0 {
		t.Fatalf("original edges not consumed exactly once: calls=%d set=%d file=%d", calls, set.RefCount.Load(), file.RefCount.Load())
	}
	if _, err = os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("not actual post-unlink error", err)
	}
	operations.completionMu.Lock()
	retained := operations.snapshot == snapshot && operations.snapshotCompleted && operations.owner != nil && operations.charge > 0 && errors.Is(operations.failure.causes[0], fault)
	operations.completionMu.Unlock()
	if !retained || d.idx.Load().primaryOwner.failedPagerOperations != operations {
		t.Fatal("exact completed-reference debt lost")
	}
	if budget.Stats().ExternalBytes == 0 {
		t.Fatal("temporary-only governor refunded surviving original cleanup debt")
	}
	if d.stableIndexCaptures.Load() != 0 {
		t.Fatal("original capture counter not consumed")
	}
	if err = snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	token.Release()
	if calls != 1 || set.RefCount.Load() != 0 || file.RefCount.Load() != 0 {
		t.Fatal("consumed original references replayed")
	}
	if !errors.Is(d.idx.Load().primaryOwner.cleanupErr, fault) {
		t.Fatal("Close error authority lost original failure")
	}
}

// A selected parent borrows an ordinary legacy child; metadata admission is
// separate from the child's original Snapshot/index/registry authority.
func TestOwnedPagerLegacyChildUsesSuppliedOwnerUntilActualCompletion(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if d.idx.Load().primaryOwner != nil {
		t.Fatal("fixture unexpectedly selected PRIMARY")
	}
	var metadata retainedalloc.Owner
	metadata.Initialize(0)
	baseline := metadata.Bytes()
	snapshot := d.AcquireStableSnapshot()
	if snapshot == nil {
		t.Fatal("capture")
	}
	token, err := snapshot.NewStableIndexResourceToken(rootpublication.StableResourceSpec{
		MetadataOwner: &metadata, Kind: rootpublication.ResourceIndex, LogicalLane: "borrowed-child",
		ResourceID: "index", Reachability: rootpublication.ReachabilityIndexFile, ContentSynced: true,
	}, NewStableDBResourceToken)
	if err != nil {
		snapshot.Close()
		t.Fatal(err)
	}
	operations := snapshot.stablePagerCompletion
	if operations == nil || operations.MetadataOwner() != &metadata {
		t.Fatal("supplied owner not bound")
	}
	if err = snapshot.beginRead(); err != nil {
		t.Fatal(err)
	}
	token.Release()
	if metadata.Bytes() <= baseline || d.stableIndexCaptures.Load() != 1 {
		t.Fatal("deferred finalizer refunded original custody")
	}
	if err = snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot.endRead()
	if metadata.Bytes() != baseline || d.stableIndexCaptures.Load() != 0 {
		t.Fatalf("completed capture leaked: bytes=%d count=%d", metadata.Bytes(), d.stableIndexCaptures.Load())
	}
	token.Release()
	if d.StableResourceIdentityPinRegistry().CleanupError() != nil {
		t.Fatal("successful original cleanup reported debt")
	}
}

func TestOwnedPagerLegacyChildPostConsumptionFailureKeepsOriginalRegistry(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := d.AcquireStableSnapshot()
	if snapshot == nil || snapshot.idx.primaryOwner != nil {
		t.Fatal("real primary capture")
	}
	faultForPostConsumptionVLogTest := errors.New("post-unlink parent sync failure")
	physicalPager := snapshot.idx.pager
	var metadata retainedalloc.Owner
	metadata.Initialize(0)
	t.Cleanup(func() {
		// The failure intentionally retains the exact owner and its admitted
		// charge. After all consumers/assertions finish, explicit fixture-only
		// physical teardown joins its worker; this is not typed debt completion.
		if closeErr := d.Close(); !errors.Is(closeErr, faultForPostConsumptionVLogTest) {
			t.Errorf("terminal owner lost original cleanup error: %v", closeErr)
		}
		_ = physicalPager.Close()
	})
	// A separate genuine manager-owned Set supplies a deterministic last-file
	// zombie. The real Snapshot finalizer consumes its original Set edge.
	dir := t.TempDir()
	id, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := valuelog.SegmentPath(dir, id)
	writer, err := valuelog.NewWriter(path, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Append(0, nil, 1, []byte("persistent pointer payload")); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	manager, err := valuelog.NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	set := manager.CurrentSetNoRefresh()
	file := set.Files[id]
	if file == nil {
		t.Fatal("actual file missing")
	}
	if snapshot.vlogPinned {
		if err = snapshot.vlogManager.Release(snapshot.state.ValueLogSet); err != nil {
			t.Fatal(err)
		}
	}
	state := *snapshot.state
	state.ValueLogSet = set
	snapshot.state = &state
	snapshot.vlogManager = manager
	snapshot.vlogPinned = true
	fault := faultForPostConsumptionVLogTest
	calls := 0
	manager.SetDeferredDeletionSync(func(_ string, _ durabilitycut.Resource) error { calls++; return fault })
	if err = manager.MarkZombie(id); err != nil {
		t.Fatal(err)
	}
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	enrollment, err := retainedalloc.EnrollPair(&metadata, d.valueLogIdentityPins.MetadataOwner(), budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.CloseAfterCleanup(fault)
	token, err := snapshot.NewStableIndexResourceToken(rootpublication.StableResourceSpec{MetadataOwner: &metadata,
		Kind: rootpublication.ResourceIndex, LogicalLane: "legacy-failure", ResourceID: "index", Reachability: rootpublication.ReachabilityIndexFile, ContentSynced: true}, NewStableDBResourceToken)
	if err != nil {
		t.Fatal(err)
	}
	operations := snapshot.stablePagerCompletion
	token.Release()
	if calls != 1 || set.RefCount.Load() != 0 || file.RefCount.Load() != 0 {
		t.Fatalf("original edges not consumed exactly once: calls=%d set=%d file=%d", calls, set.RefCount.Load(), file.RefCount.Load())
	}
	if _, err = os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("not actual post-unlink error", err)
	}
	operations.completionMu.Lock()
	retained := operations.snapshot == snapshot && operations.snapshotCompleted && operations.owner == nil && operations.metadata == &metadata && operations.originalCustody != nil && operations.charge > 0 && errors.Is(operations.failure.causes[0], fault)
	operations.completionMu.Unlock()
	if !retained || !errors.Is(d.StableResourceIdentityPinRegistry().CleanupError(), fault) {
		t.Fatal("exact completed-reference debt lost")
	}
	if budget.Stats().ExternalBytes == 0 {
		t.Fatal("temporary-only governor refunded surviving original cleanup debt")
	}
	if d.stableIndexCaptures.Load() != 0 {
		t.Fatal("original capture counter not consumed")
	}
	if err = snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	token.Release()
	if calls != 1 || set.RefCount.Load() != 0 || file.RefCount.Load() != 0 {
		t.Fatal("consumed original references replayed")
	}
	if !errors.Is(d.StableResourceIdentityPinRegistry().CleanupError(), fault) {
		t.Fatal("Close error authority lost original failure")
	}
}

func TestOwnedPagerSuppliedOwnerRefusesBeforeOriginalTransfer(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	var metadata retainedalloc.Owner
	metadata.Initialize(0)
	metadata.Close()
	snapshot := d.AcquireStableSnapshot()
	defer snapshot.Close()
	before := d.StableResourceIdentityPinRegistry().ActivePins()
	token, err := snapshot.NewStableIndexResourceToken(rootpublication.StableResourceSpec{MetadataOwner: &metadata,
		Kind: rootpublication.ResourceIndex, LogicalLane: "refused-child", ResourceID: "index", Reachability: rootpublication.ReachabilityIndexFile, ContentSynced: true}, NewStableDBResourceToken)
	if token != nil || !errors.Is(err, retainedalloc.ErrClosed) {
		t.Fatalf("refusal token=%v err=%v", token, err)
	}
	if metadata.Bytes() != 0 || snapshot.closed.Load() || snapshot.stablePagerCompletion != nil ||
		snapshot.stableIndexCaptureTransferred || d.StableResourceIdentityPinRegistry().ActivePins() != before {
		t.Fatal("refused request changed original custody")
	}
}

func TestOwnedPagerSuppliedMetadataBorrowsExistingOriginalNamespaceProof(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	original := d.AcquireStableSnapshot()
	token, err := original.NewStableIndexResourceToken(rootpublication.StableResourceSpec{Kind: rootpublication.ResourceIndex, LogicalLane: "original", ResourceID: "index", Reachability: rootpublication.ReachabilityIndexFile, ContentSynced: true}, NewStableDBResourceToken)
	if err != nil {
		original.Close()
		t.Fatal(err)
	}
	token.Release()
	generation := d.idx.Load()
	generation.stableNamespaceMu.Lock()
	proof, parent := generation.stableNamespaceProof, generation.stableNamespaceParent
	generation.stableNamespaceMu.Unlock()
	if proof == nil || parent == nil {
		t.Fatal("ordinary original producer did not freeze namespace proof")
	}
	var metadata retainedalloc.Owner
	for i := 0; i < 3; i++ {
		snapshot := d.AcquireStableSnapshot()
		token, err = snapshot.NewStableIndexResourceToken(rootpublication.StableResourceSpec{MetadataOwner: &metadata, Kind: rootpublication.ResourceIndex, LogicalLane: "supplied", ResourceID: "index", Reachability: rootpublication.ReachabilityIndexFile, ContentSynced: true}, NewStableDBResourceToken)
		if err != nil {
			snapshot.Close()
			t.Fatal(err)
		}
		token.Release()
		if metadata.Bytes() != 0 {
			t.Fatalf("supplied independent descriptor survived completed capture: %d", metadata.Bytes())
		}
		generation.stableNamespaceMu.Lock()
		unchanged := generation.stableNamespaceProof == proof && generation.stableNamespaceParent == parent && generation.stableNamespaceMetadata == nil
		generation.stableNamespaceMu.Unlock()
		if !unchanged {
			t.Fatal("supplied capture replaced/relabelled original proof custody")
		}
	}
}
