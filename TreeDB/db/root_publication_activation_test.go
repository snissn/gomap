package db

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestCommitPublicationAcceptedClassifiesOnlyPostAcceptanceErrors(t *testing.T) {
	accepted := wrapAcceptedFinalizeCommitError(errTestWriteMetaFailpoint)
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "plain", err: errTestWriteMetaFailpoint, want: false},
		{name: "pre-accept safe", err: wrapFinalizeCommitError(errTestWriteMetaFailpoint, true), want: false},
		{name: "ambiguous not accepted", err: wrapFinalizeCommitError(ErrRecoveryRequired, false), want: false},
		{name: "accepted", err: accepted, want: true},
		{name: "wrapped accepted", err: errors.Join(errors.New("outer"), accepted), want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := CommitPublicationAccepted(tc.err); got != tc.want {
				t.Fatalf("CommitPublicationAccepted(%v)=%t want %t", tc.err, got, tc.want)
			}
		})
	}
}

func TestQueuedRootPublicationVisibleResourceCloneFailureAllowsCreatedSegmentCleanup(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	idx := database.idx.Load()
	if idx == nil {
		t.Fatal("missing active index")
	}
	builder, err := database.acquireRootPublicationBuilderV1()
	if err != nil {
		t.Fatalf("acquire root-publication builder: %v", err)
	}
	defer builder.Release()

	cloneErr := errors.New("visible resource clone failed")
	database.rootPublication.mu.Lock()
	database.rootPublication.poison = cloneErr
	database.rootPublication.mu.Unlock()
	defer func() {
		database.rootPublication.mu.Lock()
		database.rootPublication.poison = nil
		database.rootPublication.mu.Unlock()
	}()

	_, err = database.finalizeQueuedRootPublicationV1(
		database.rootPublication,
		builder,
		idx,
		database.meta,
		0,
		0,
		nil,
		false,
		finalizeCommitPost{},
		nil,
		nil,
		nil,
		finalizeCommitOptions{},
		nil,
		func() {},
	)
	if !errors.Is(err, cloneErr) {
		t.Fatalf("finalize error=%v, want visible resource clone failure", err)
	}
	if !finalizeCommitErrorAllowsCreatedSegmentCleanup(err) {
		t.Fatalf("finalize error=%v did not preserve pre-publication cleanup-safe classification", err)
	}
}

func TestActivatedRootPublicationTripsFormerDirectPublisher(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	idx := database.idx.Load()
	if idx == nil {
		t.Fatal("missing active index")
	}
	_, err = database.publishDurableRootV1(idx, database.meta, nil, nil)
	if !errors.Is(err, errDirectDurableRootPublisherDisabledV1) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("former direct publisher error=%v, want disabled tripwire and ErrRecoveryRequired", err)
	}
}

func TestOuterLeafSteadyStateWriteDoesNotScanCandidateTree(t *testing.T) {
	database, err := Open(Options{
		Dir:                        t.TempDir(),
		Durability:                 DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	leafLog := &registeredLeafPageLog{db: database, dir: database.dir}
	if err := leafLog.ensureWriter(); err != nil {
		t.Fatalf("ensure leaf writer: %v", err)
	}
	database.SetLeafPageLog(leafLog)
	defer closeVacuumTestLeafPageLog(t, database, leafLog)

	// The first publication into a producer segment establishes its exact
	// dependency basis. Subsequent COW writes in the same segment must reuse that
	// basis rather than project the whole candidate tree again.
	if err := database.SetSync([]byte("outer-leaf-prime"), []byte("p")); err != nil {
		t.Fatalf("prime SetSync: %v", err)
	}
	scans := 0
	database.testScanCandidateExternalReferencesHook = func() { scans++ }
	if err := database.SetSync([]byte("outer-leaf-hot-path"), []byte("v")); err != nil {
		t.Fatalf("SetSync: %v", err)
	}
	database.testScanCandidateExternalReferencesHook = nil
	if scans != 0 {
		t.Fatalf("ordinary outer-leaf write candidate scans=%d want 0", scans)
	}

	// Replacing an existing key can make the old physical leaf record
	// unreachable even when the producer is still appending to the same segment.
	// Apply-fed removal evidence must therefore restore exact projection.
	scans = 0
	database.testScanCandidateExternalReferencesHook = func() { scans++ }
	if err := database.SetSync([]byte("outer-leaf-prime"), []byte("r")); err != nil {
		t.Fatalf("overwrite SetSync: %v", err)
	}
	database.testScanCandidateExternalReferencesHook = nil
	if scans == 0 {
		t.Fatal("outer-leaf overwrite did not scan candidate tree")
	}
}

func TestOuterLeafFirstPublicationFromEmptyDependencyBaseDoesNotScanCandidateTree(t *testing.T) {
	database, err := Open(Options{
		Dir:                        t.TempDir(),
		Durability:                 DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	leafLog := &registeredLeafPageLog{db: database, dir: database.dir}
	if err := leafLog.ensureWriter(); err != nil {
		t.Fatalf("ensure leaf writer: %v", err)
	}
	database.SetLeafPageLog(leafLog)
	defer closeVacuumTestLeafPageLog(t, database, leafLog)

	scans := 0
	database.testScanCandidateExternalReferencesHook = func() { scans++ }
	if err := database.SetSync([]byte("outer-leaf-first"), []byte("value")); err != nil {
		t.Fatalf("first SetSync: %v", err)
	}
	database.testScanCandidateExternalReferencesHook = nil
	if scans != 0 {
		t.Fatalf("first outer-leaf publication candidate scans=%d want 0", scans)
	}
	got, err := database.Get([]byte("outer-leaf-first"))
	if err != nil {
		t.Fatalf("read first outer-leaf value: %v", err)
	}
	if string(got) != "value" {
		t.Fatalf("first outer-leaf value=%q want value", got)
	}
}

func TestOuterLeafReplacementManifestPreservesAppendOnlyBaseDependencyReuse(t *testing.T) {
	database, err := Open(Options{
		Dir:                        t.TempDir(),
		Durability:                 DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	leafLog := &registeredLeafPageLog{db: database, dir: database.dir}
	if err := leafLog.ensureWriter(); err != nil {
		t.Fatalf("ensure leaf writer: %v", err)
	}
	database.SetLeafPageLog(leafLog)
	defer closeVacuumTestLeafPageLog(t, database, leafLog)
	if err := database.SetSync([]byte("replacement-manifest-prime"), []byte("p")); err != nil {
		t.Fatalf("prime SetSync: %v", err)
	}

	selected := database.durableRoot.slotResources[database.durableRoot.slot]
	selectedClone, err := rootpublication.CloneStableResourceSetExcludingKinds(selected)
	if err != nil {
		t.Fatalf("clone selected durable resources: %v", err)
	}
	oldManifest := stableContractResourceSet(t, stableContractDescriptor{
		generation: 1, kind: rootpublication.ResourceOuterLeafManifest,
		reachability: rootpublication.ReachabilityOuterLeafGeneration, frontier: 4096,
	})
	baseBuilder := rootpublication.NewStableResourceSetBuilder()
	if err := baseBuilder.Merge(selectedClone); err != nil {
		selectedClone.Release()
		baseBuilder.Abandon()
		t.Fatalf("merge selected durable resources: %v", err)
	}
	if err := baseBuilder.Merge(oldManifest); err != nil {
		oldManifest.Release()
		baseBuilder.Abandon()
		t.Fatalf("merge old manifest: %v", err)
	}
	base, err := baseBuilder.Freeze()
	if err != nil {
		baseBuilder.Abandon()
		t.Fatalf("freeze base resources: %v", err)
	}
	defer base.Release()
	additional := stableContractResourceSet(t, stableContractDescriptor{
		generation: 2, kind: rootpublication.ResourceOuterLeafManifest,
		reachability: rootpublication.ReachabilityOuterLeafGeneration, frontier: 4096,
	})

	appendOnly := newValueLogRefDelta()
	defer releaseValueLogRefDelta(appendOnly)
	appendOnly.add(leafLog.fileID, 1)
	scans := 0
	database.testScanCandidateExternalReferencesHook = func() { scans++ }
	captured, err := database.captureDurableRootResourcesFromBaseV1(
		database.idx.Load(), database.meta, appendOnly, base, additional,
		rootpublication.StableLogicalObligationRequirements{}, rootpublication.StableLogicalObligationMutation{},
		rootpublication.StableLogicalObligationMutation{}, rootpublication.StableResourceClosureWork{}, nil, false, nil,
	)
	database.testScanCandidateExternalReferencesHook = nil
	if err != nil {
		t.Fatalf("capture append-only replacement-manifest resources: %v", err)
	}
	defer captured.Release()
	if scans != 0 {
		t.Fatalf("append-only replacement manifest candidate scans=%d want 0", scans)
	}
	foundCurrentLeaf, foundNewManifest := false, false
	for _, descriptor := range captured.Descriptors() {
		switch descriptor.Kind() {
		case rootpublication.ResourceOuterLeafLog:
			foundCurrentLeaf = foundCurrentLeaf || descriptor.Generation() == uint64(leafLog.fileID)
		case rootpublication.ResourceOuterLeafManifest:
			if descriptor.Generation() == 1 {
				t.Fatal("captured resources retained superseded outer-leaf manifest")
			}
			foundNewManifest = foundNewManifest || descriptor.Generation() == 2
		}
	}
	if !foundCurrentLeaf || !foundNewManifest {
		t.Fatalf("captured descriptors=%+v, want current raw leaf and replacement manifest", captured.Descriptors())
	}

	plannerBase := stableContractResourceSet(t, stableContractDescriptor{
		generation: uint64(leafLog.fileID), kind: rootpublication.ResourceOuterLeafLog,
		reachability: rootpublication.ReachabilityOuterLeafRawPointer, frontier: 4096,
	})
	defer plannerBase.Release()
	plannerAdditional := stableContractResourceSet(t, stableContractDescriptor{
		generation: 2, kind: rootpublication.ResourceOuterLeafManifest,
		reachability: rootpublication.ReachabilityOuterLeafGeneration, frontier: 4096,
	})
	defer plannerAdditional.Release()

	netZeroFileID := leafLog.fileID + 1
	netZero := newValueLogRefDelta()
	defer releaseValueLogRefDelta(netZero)
	netZero.add(netZeroFileID, 1)
	netZero.add(netZeroFileID, -1)
	if references, reuse, err := database.planOuterLeafBaseDependencyReuseV1(plannerBase, plannerAdditional, database.meta, netZero); err != nil {
		t.Fatalf("plan net-zero pointer replacement transition: %v", err)
	} else if !reuse {
		t.Fatal("net-zero pointer replacement did not reuse predecessor dependencies")
	} else if _, ok := references[netZeroFileID]; !ok {
		t.Fatalf("net-zero pointer replacement references=%v, want positive membership for file %d", references, netZeroFileID)
	}

	destructive := newValueLogRefDelta()
	defer releaseValueLogRefDelta(destructive)
	destructive.add(leafLog.fileID, -1)
	if references, reuse, err := database.planOuterLeafBaseDependencyReuseV1(plannerBase, plannerAdditional, database.meta, destructive); err != nil {
		t.Fatalf("plan destructive replacement-manifest transition: %v", err)
	} else if reuse || references != nil {
		t.Fatalf("destructive replacement-manifest transition reused references=%v reuse=%t", references, reuse)
	}

	previousLeafFileID := leafLog.fileID
	// Model the producer's post-rotation current identity. The planner only
	// consumes the stable identity; resource capture/opening is covered by the
	// ordered publication integration tests.
	leafLog.fileID++
	rotated := newValueLogRefDelta()
	defer releaseValueLogRefDelta(rotated)
	rotated.outerLeafDependencyReuse = true
	rotated.addPositive(leafLog.fileID, 1)
	if references, reuse, err := database.planOuterLeafBaseDependencyReuseV1(plannerBase, plannerAdditional, database.meta, rotated); err != nil {
		t.Fatalf("plan rotated outer-leaf transition: %v", err)
	} else if !reuse {
		t.Fatal("rotated outer-leaf transition did not reuse predecessor dependencies")
	} else {
		if _, ok := references[previousLeafFileID]; !ok {
			t.Fatalf("rotated outer-leaf references=%v, want predecessor file %d", references, previousLeafFileID)
		}
		if _, ok := references[leafLog.fileID]; !ok {
			t.Fatalf("rotated outer-leaf references=%v, want current file %d", references, leafLog.fileID)
		}
	}

	const (
		createdFileID = uint32(9101)
		currentFileID = uint32(9102)
	)
	database.SetLeafPageLog(&createdThenCurrentLeafPageLog{
		createdSegments: []rewriteCreatedSegment{{path: "/created-leaf.log", fileID: createdFileID}},
		currentPath:     "/current-leaf.log",
		currentFileID:   currentFileID,
	})
	emptyBase := newValueLogRefDelta()
	defer releaseValueLogRefDelta(emptyBase)
	emptyBase.outerLeafDependencyReuse = true
	emptyBase.allowEmptyDependencyReuse = true
	if references, reuse, err := database.planOuterLeafBaseDependencyReuseV1(nil, nil, database.meta, emptyBase); err != nil {
		t.Fatalf("plan empty-base created/current transition: %v", err)
	} else if !reuse {
		t.Fatal("empty-base created/current transition did not reuse producer dependencies")
	} else {
		if _, ok := references[createdFileID]; !ok {
			t.Fatalf("empty-base references=%v, want created file %d", references, createdFileID)
		}
		if _, ok := references[currentFileID]; !ok {
			t.Fatalf("empty-base references=%v, want current file %d", references, currentFileID)
		}
	}
}

func TestOuterLeafLogicalReplacementUsesExactTrackerProjection(t *testing.T) {
	database, err := Open(Options{
		Dir:                        t.TempDir(),
		Durability:                 DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	leafLog := &registeredLeafPageLog{db: database, dir: database.dir}
	if err := leafLog.ensureWriter(); err != nil {
		t.Fatalf("ensure leaf writer: %v", err)
	}
	database.SetLeafPageLog(leafLog)
	defer closeVacuumTestLeafPageLog(t, database, leafLog)

	oldValueFileID, err := valuelog.EncodeFileID(0, 101)
	if err != nil {
		t.Fatal(err)
	}
	newValueFileID, err := valuelog.EncodeFileID(0, 102)
	if err != nil {
		t.Fatal(err)
	}
	staleLeafFileID, err := valuelog.EncodeFileID(valuelog.ReservedLeafLogLaneID, 103)
	if err != nil {
		t.Fatal(err)
	}
	base := stableContractResourceSet(t,
		stableContractDescriptor{
			generation: uint64(leafLog.fileID), kind: rootpublication.ResourceOuterLeafLog,
			reachability: rootpublication.ReachabilityOuterLeafRawPointer, frontier: 4096,
		},
		stableContractDescriptor{
			generation: uint64(oldValueFileID), kind: rootpublication.ResourceValueLog,
			reachability: rootpublication.ReachabilityValueLogPointer, frontier: 4096,
		},
	)
	defer base.Release()

	baseSeq := database.currentCommitSeq()
	database.valueLogRefTracker = newValueLogRefTracker()
	database.valueLogRefTracker.replace(map[uint32]uint64{oldValueFileID: 1, staleLeafFileID: 1}, baseSeq, false)
	delta := newValueLogRefDelta()
	defer releaseValueLogRefDelta(delta)
	delta.outerLeafDependencyReuse = true
	delta.add(oldValueFileID, -1)
	delta.add(newValueFileID, 1)

	next := database.meta
	next.CommitSeq = baseSeq + 1
	references, reuse, err := database.planOuterLeafBaseDependencyReuseV1(base, nil, next, delta)
	if err != nil {
		t.Fatalf("plan logical replacement: %v", err)
	}
	if !reuse {
		t.Fatal("exact logical replacement did not reuse raw predecessor dependencies")
	}
	if _, ok := references[oldValueFileID]; ok {
		t.Fatalf("references=%v retained removed logical value-log file %d", references, oldValueFileID)
	}
	if _, ok := references[newValueFileID]; !ok {
		t.Fatalf("references=%v omitted new logical value-log file %d", references, newValueFileID)
	}
	if _, ok := references[leafLog.fileID]; !ok {
		t.Fatalf("references=%v omitted predecessor raw outer-leaf file %d", references, leafLog.fileID)
	}
	if _, ok := references[staleLeafFileID]; ok {
		t.Fatalf("references=%v retained stale tracker-only outer-leaf file %d", references, staleLeafFileID)
	}
}

func TestRootPublicationBuildGroupStagesOnlyFinalCandidate(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                       dir,
		Durability:                DurabilityWALOffRelaxed,
		DisableBackgroundPrune:    true,
		rootPublicationFixedDelay: 100 * time.Millisecond,
	}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if database != nil {
			_ = database.Close()
		}
	}()

	coordinator := database.rootPublication.coordinator
	before := coordinator.Stats()
	group, err := database.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatalf("BeginRootPublicationBuildGroup: %v", err)
	}
	defer group.Close()

	stagedKey := []byte("staged-intermediate")
	stagedValue := []byte("private-root-only")
	intermediate := database.NewPhysicalBatch().(*Batch)
	if err := intermediate.Set(stagedKey, stagedValue); err != nil {
		t.Fatalf("intermediate Set: %v", err)
	}
	if err := intermediate.SetRootPublicationBuildGroup(group, false); err != nil {
		t.Fatalf("intermediate SetRootPublicationBuildGroup: %v", err)
	}
	if err := intermediate.Write(); err != nil {
		t.Fatalf("intermediate Write: %v", err)
	}
	if err := intermediate.Close(); err != nil {
		t.Fatalf("intermediate Close: %v", err)
	}

	staged := coordinator.Stats()
	if staged.VisibleCommitSeq != before.VisibleCommitSeq ||
		staged.DurableCommitSeq != before.DurableCommitSeq ||
		staged.PendingCommits != before.PendingCommits ||
		staged.PublishCalls != before.PublishCalls {
		t.Fatalf("intermediate publication stats=%+v, want frontiers/debt/publishes unchanged from %+v", staged, before)
	}
	if staged.ActiveBuilders != before.ActiveBuilders+1 {
		t.Fatalf("intermediate active builders=%d want %d", staged.ActiveBuilders, before.ActiveBuilders+1)
	}
	if got, err := database.Get(stagedKey); err != nil || got != nil {
		t.Fatalf("intermediate Get=(%q,%v) want (nil,nil)", got, err)
	}

	finalKey := []byte("staged-final")
	finalValue := []byte("complete-logical-root")
	final := database.NewPhysicalBatch().(*Batch)
	if err := final.Set(finalKey, finalValue); err != nil {
		t.Fatalf("final Set: %v", err)
	}
	if err := final.SetRootPublicationBuildGroup(group, true); err != nil {
		t.Fatalf("final SetRootPublicationBuildGroup: %v", err)
	}
	if err := final.WriteSync(); err != nil {
		t.Fatalf("final WriteSync: %v", err)
	}
	if err := final.Close(); err != nil {
		t.Fatalf("final Close: %v", err)
	}
	if err := group.Close(); err != nil {
		t.Fatalf("group Close after final: %v", err)
	}

	completed := coordinator.Stats()
	if completed.VisibleCommitSeq != before.VisibleCommitSeq+1 ||
		completed.DurableCommitSeq != completed.VisibleCommitSeq ||
		completed.PendingCommits != 0 ||
		completed.PublishCalls != before.PublishCalls+1 ||
		completed.LastGroupSize != 1 ||
		completed.ActiveBuilders != before.ActiveBuilders {
		t.Fatalf("completed publication stats=%+v, want one durable candidate from %+v", completed, before)
	}
	for key, want := range map[string][]byte{
		string(stagedKey): stagedValue,
		string(finalKey):  finalValue,
	} {
		got, err := database.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%q)=%q want %q", key, got, want)
		}
	}

	if err := database.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	database = nil
	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	for key, want := range map[string][]byte{
		string(stagedKey): stagedValue,
		string(finalKey):  finalValue,
	} {
		got, err := reopened.Get([]byte(key))
		if err != nil {
			t.Fatalf("reopened Get(%q): %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("reopened Get(%q)=%q want %q", key, got, want)
		}
	}
}

func TestVacuumBaseSnapshotWaitsForPrivateRootPublicationBuildGroup(t *testing.T) {
	database, err := Open(Options{
		Dir:                    t.TempDir(),
		Durability:             DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	group, err := database.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatalf("BeginRootPublicationBuildGroup: %v", err)
	}
	defer group.Close()

	intermediateKey := []byte("vacuum-fence-intermediate")
	intermediate := database.NewPhysicalBatch().(*Batch)
	if err := intermediate.Set(intermediateKey, []byte("private-before-vacuum")); err != nil {
		t.Fatalf("intermediate Set: %v", err)
	}
	if err := intermediate.SetRootPublicationBuildGroup(group, false); err != nil {
		t.Fatalf("intermediate SetRootPublicationBuildGroup: %v", err)
	}
	if err := intermediate.Write(); err != nil {
		t.Fatalf("intermediate Write: %v", err)
	}
	if err := intermediate.Close(); err != nil {
		t.Fatalf("intermediate Close: %v", err)
	}

	fenceAttempted := make(chan struct{})
	database.vacuumBeforeRecorderFenceHook = func() { close(fenceAttempted) }
	defer func() { database.vacuumBeforeRecorderFenceHook = nil }()
	baseSnapshot := make(chan *Snapshot, 1)
	go func() { baseSnapshot <- database.startVacuumRecorderWithBaseSnapshot() }()
	defer database.vacuum.Stop()
	<-fenceAttempted
	select {
	case snap := <-baseSnapshot:
		if snap != nil {
			_ = snap.Close()
		}
		t.Fatal("vacuum captured its base snapshot while a private root build was active")
	default:
	}

	finalKey := []byte("vacuum-fence-final")
	final := database.NewPhysicalBatch().(*Batch)
	if err := final.Set(finalKey, []byte("published-before-vacuum-base")); err != nil {
		t.Fatalf("final Set: %v", err)
	}
	if err := final.SetRootPublicationBuildGroup(group, true); err != nil {
		t.Fatalf("final SetRootPublicationBuildGroup: %v", err)
	}
	if err := final.WriteSync(); err != nil {
		t.Fatalf("final WriteSync: %v", err)
	}
	if err := final.Close(); err != nil {
		t.Fatalf("final Close: %v", err)
	}

	var snap *Snapshot
	select {
	case snap = <-baseSnapshot:
	case <-time.After(5 * time.Second):
		t.Fatal("vacuum base snapshot remained blocked after the root build published")
	}
	if snap == nil {
		t.Fatal("vacuum base snapshot is nil")
	}
	defer snap.Close()
	for _, key := range [][]byte{intermediateKey, finalKey} {
		got, err := snap.Get(key)
		if err != nil {
			t.Fatalf("snapshot Get(%q): %v", key, err)
		}
		if got == nil {
			t.Fatalf("vacuum base snapshot omitted published build-group key %q", key)
		}
	}
}

func TestRootPublicationBuildGroupCommandWALAcceptedWaitFailureDoesNotPoisonOpenHandle(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	database := openCommandWALDB(t, dir)
	defer database.Close()

	intent := mustRawKVCommandWALIntent(t, database, "group-accepted", "visible-before-durable")
	lsn, err := database.AppendCommandWALIntent(intent, false)
	if err != nil {
		t.Fatalf("AppendCommandWALIntent: %v", err)
	}
	group, err := database.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatalf("BeginRootPublicationBuildGroup: %v", err)
	}
	defer group.Close()

	final := database.NewPhysicalBatch().(*Batch)
	defer final.Close()
	if err := final.Set([]byte("group-accepted"), []byte("visible-before-durable")); err != nil {
		t.Fatalf("final Set: %v", err)
	}
	if err := final.SetCommandWALPublish(lsn, []CommandWALLSNRange{{First: lsn, Last: lsn}}); err != nil {
		t.Fatalf("SetCommandWALPublish: %v", err)
	}
	if err := final.SetRootPublicationBuildGroup(group, true); err != nil {
		t.Fatalf("SetRootPublicationBuildGroup: %v", err)
	}
	database.testRootPublicationDependencyBytes.Store(rootpublication.HardPendingBytes + 1)
	database.testFailWriteMeta.Store(true)
	const staleSubtreePageID = ^uint64(0)
	database.storeLeafGenerationSubtreeStats(staleSubtreePageID, leafGenerationSubtreeStats{
		1: {LivePages: 1, LiveBytes: 1},
	})
	err = final.WriteSync()
	database.testFailWriteMeta.Store(false)
	if !errors.Is(err, errTestWriteMetaFailpoint) {
		t.Fatalf("accepted build-group wait error=%v, want retryable meta failpoint", err)
	}
	if !CommitPublicationAccepted(err) {
		t.Fatalf("accepted build-group wait error=%v was not marked post-acceptance", err)
	}
	if errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("accepted build-group wait error=%v unexpectedly requires recovery", err)
	}
	if _, ok := database.loadLeafGenerationSubtreeStats(staleSubtreePageID); ok {
		t.Fatal("accepted build-group wait error left stale leaf-generation reachability state cached")
	}
	if got := database.State().AppliedCommandLSN; got != lsn {
		t.Fatalf("visible AppliedCommandLSN after accepted build-group error=%d, want %d", got, lsn)
	}
	if got, getErr := database.Get([]byte("group-accepted")); getErr != nil || string(got) != "visible-before-durable" {
		t.Fatalf("accepted build-group value=(%q,%v), want visible value", got, getErr)
	}
	if err := database.CheckCommandWALPublishReady(); err != nil {
		t.Fatalf("CheckCommandWALPublishReady after accepted build-group error: %v", err)
	}
	if err := database.SetSync([]byte("after-group"), []byte("same-handle-progress")); err != nil {
		t.Fatalf("SetSync after accepted build-group error: %v", err)
	}
}

func TestOrderedRootSystemBuilderAcceptedWaitErrorIsClassified(t *testing.T) {
	database, err := Open(Options{
		Dir:                    t.TempDir(),
		Durability:             DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		database.testFailWriteMeta.Store(false)
		_ = database.Close()
	}()

	database.testRootPublicationDependencyBytes.Store(rootpublication.HardPendingBytes + 1)
	database.testFailWriteMeta.Store(true)
	_, _, err = database.PublishOrderedRootDeltaGroupWithSystemBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		return mustFrozenSystemMemtable(t, "accepted/system-schema", "visible-before-durable").NewIterator(nil, nil), nil
	})
	database.testFailWriteMeta.Store(false)
	if !errors.Is(err, errTestWriteMetaFailpoint) {
		t.Fatalf("accepted ordered-root wait error=%v, want retryable meta failpoint", err)
	}
	if !CommitPublicationAccepted(err) {
		t.Fatalf("accepted ordered-root wait error=%v was not marked post-acceptance", err)
	}
	if errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("accepted ordered-root wait error=%v unexpectedly requires recovery", err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint retrying accepted ordered-root publication: %v", err)
	}
}

func TestDurableRootDependencyObservationPathV1(t *testing.T) {
	tests := map[string]string{
		"plain":            "/tmp/value.log",
		"cloned-sync-pins": "/tmp/value.log#stable-sync-pin#stable-sync-pin#stable-sync-pin",
		"mixed-pins":       "/tmp/value.log#stable-pin#stable-sync-pin#stable-pin",
	}
	for name, path := range tests {
		t.Run(name, func(t *testing.T) {
			if got := stableResourceDependencyObservationPathV1(path); got != "/tmp/value.log" {
				t.Fatalf("stableResourceDependencyObservationPathV1(%q)=%q want %q", path, got, "/tmp/value.log")
			}
		})
	}
}

func TestRootPublicationBuildGroupAbortDiscardsStagedRoot(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                       dir,
		Durability:                DurabilityWALOffRelaxed,
		DisableBackgroundPrune:    true,
		rootPublicationFixedDelay: 100 * time.Millisecond,
	}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if database != nil {
			_ = database.Close()
		}
	}()

	coordinator := database.rootPublication.coordinator
	before := coordinator.Stats()
	group, err := database.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatalf("BeginRootPublicationBuildGroup: %v", err)
	}
	stagedKey := []byte("aborted-staged-root")
	intermediate := database.NewPhysicalBatch().(*Batch)
	if err := intermediate.Set(stagedKey, []byte("must-not-publish")); err != nil {
		t.Fatalf("intermediate Set: %v", err)
	}
	if err := intermediate.SetRootPublicationBuildGroup(group, false); err != nil {
		t.Fatalf("intermediate SetRootPublicationBuildGroup: %v", err)
	}
	if err := intermediate.Write(); err != nil {
		t.Fatalf("intermediate Write: %v", err)
	}
	if err := intermediate.Close(); err != nil {
		t.Fatalf("intermediate Close: %v", err)
	}
	if err := group.Close(); err != nil {
		t.Fatalf("abort group Close: %v", err)
	}
	if err := group.Close(); err != nil {
		t.Fatalf("idempotent group Close: %v", err)
	}

	aborted := coordinator.Stats()
	if aborted.VisibleCommitSeq != before.VisibleCommitSeq ||
		aborted.DurableCommitSeq != before.DurableCommitSeq ||
		aborted.PendingCommits != before.PendingCommits ||
		aborted.PublishCalls != before.PublishCalls ||
		aborted.ActiveBuilders != before.ActiveBuilders {
		t.Fatalf("aborted publication stats=%+v want unchanged from %+v", aborted, before)
	}
	if got, err := database.Get(stagedKey); err != nil || got != nil {
		t.Fatalf("aborted staged Get=(%q,%v) want (nil,nil)", got, err)
	}

	survivorKey := []byte("post-abort-write")
	survivorValue := []byte("normal-admission-still-works")
	if err := database.SetSync(survivorKey, survivorValue); err != nil {
		t.Fatalf("SetSync after abort: %v", err)
	}
	if got, err := database.Get(stagedKey); err != nil || got != nil {
		t.Fatalf("post-write aborted staged Get=(%q,%v) want (nil,nil)", got, err)
	}

	if err := database.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	database = nil
	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if got, err := reopened.Get(stagedKey); err != nil || got != nil {
		t.Fatalf("reopened aborted staged Get=(%q,%v) want (nil,nil)", got, err)
	}
	got, err := reopened.Get(survivorKey)
	if err != nil {
		t.Fatalf("reopened survivor Get: %v", err)
	}
	if !bytes.Equal(got, survivorValue) {
		t.Fatalf("reopened survivor=%q want %q", got, survivorValue)
	}
}
