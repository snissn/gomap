package collections

import (
	"sync"
	"testing"
	"time"
)

// A prepared startup proof must not compare an accepted document cut with the
// previous live carrier while their real publication handoff is still active.
func TestPreparedVectorPartitionStartupWaitsForAcceptedLivePublicationV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, database, collection, _, manifest := newVectorPartitionLiveProductionFixtureV1(t)
	defer database.Close()
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	source, err := OpenCollectionForRaftSourceV1(database, manifest.Collection)
	if err != nil {
		t.Fatal(err)
	}
	carrier := collection.registeredColumnGraphPartitionLiveCarrier(manifest.IndexName)
	if source.collectionSchemaCoordinator() != collection.collectionSchemaCoordinator() || source.registeredColumnGraphPartitionLiveCarrier(manifest.IndexName) != carrier {
		t.Fatal("startup source does not share the publishing DB coordinator and carrier")
	}
	if _, err := source.NewPreparedVectorPartitionGenerationReplicatedLiveStartupSearchOpenPlanWithContextV1(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	beforeGeneration, _, err := source.vectorPartitionLiveCurrentDocumentStateV1(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	accepted, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	vectorPartitionLiveReplayAfterAcceptedHookV1.Lock()
	previousHook := vectorPartitionLiveReplayAfterAcceptedHookV1.fn
	vectorPartitionLiveReplayAfterAcceptedHookV1.fn = func() { close(accepted); <-release }
	vectorPartitionLiveReplayAfterAcceptedHookV1.Unlock()
	defer func() {
		vectorPartitionLiveReplayAfterAcceptedHookV1.Lock()
		vectorPartitionLiveReplayAfterAcceptedHookV1.fn = previousHook
		vectorPartitionLiveReplayAfterAcceptedHookV1.Unlock()
	}()
	mutationDone := make(chan error, 1)
	go func() {
		_, err := collection.Insert([]byte("startup-tail"), []byte(`{"time_us":99,"kind":"vector","did":"startup-tail","embedding":[0.5,0.5]}`))
		mutationDone <- err
	}()
	select {
	case <-accepted:
	case err := <-mutationDone:
		t.Fatalf("mutation ended before accepted publication hook: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("publication did not reach accepted hook")
	}
	currentGeneration, currentState, err := source.vectorPartitionLiveCurrentDocumentStateV1(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	carrier.mu.RLock()
	live := carrier.partitionLive
	coverage, revision := live.coverage, live.revision
	matching := !live.invalid && live.bindingDurable && live.indexDefinitionDigest == manifest.IndexDefinitionDigest && live.source == vectorPartitionLiveSourceV1(manifest) && live.generation == manifest.Generation && carrier.sourceDocumentRootsValid && len(carrier.unnotifiedDocumentIDs) == 0 && carrier.sourceDocumentGeneration == coverage
	carrier.mu.RUnlock()
	t.Logf("accepted DB=%p source DB=%p generation=%d before=%d state=%+v carrier=%p coverage=%d revision=%d all_other_authority_predicates=%t", database, source.db, currentGeneration, beforeGeneration, currentState, carrier, coverage, revision, matching)
	if !matching || currentGeneration <= beforeGeneration || coverage != beforeGeneration {
		t.Fatal("fixture did not create a real accepted-root/live-carrier handoff")
	}
	validationDone := make(chan error, 1)
	go func() {
		_, err := source.NewPreparedVectorPartitionGenerationReplicatedLiveStartupSearchOpenPlanWithContextV1(t.Context(), manifest)
		validationDone <- err
	}()
	var premature bool
	var validationErr error
	select {
	case validationErr = <-validationDone:
		premature = true
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-mutationDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("publication did not finish after release")
	}
	if !premature {
		select {
		case validationErr = <-validationDone:
		case <-time.After(5 * time.Second):
			t.Fatal("startup validation did not finish after publication")
		}
	}
	// The one real Insert may publish notification/finalization metadata after
	// the accepted hook. Baseline only after that complete mutation and proof.
	completedState := *database.State()
	if _, err := source.NewPreparedVectorPartitionGenerationReplicatedLiveStartupSearchOpenPlanWithContextV1(t.Context(), manifest); err != nil {
		t.Fatalf("post-handoff validation: %v", err)
	}
	if after := database.State(); after.CommitSeq != completedState.CommitSeq || after.RootPageID != completedState.RootPageID || after.SystemRootPageID != completedState.SystemRootPageID || after.AppliedCommandLSN != completedState.AppliedCommandLSN {
		t.Fatal("post-completion startup validation published an additional root or WAL command")
	}
	if got, err := source.Get([]byte("startup-tail")); err != nil || len(got) == 0 {
		t.Fatalf("accepted tail document missing: %s %v", got, err)
	}
	if premature {
		t.Fatalf("startup validation escaped the accepted publication before carrier installation: %v", validationErr)
	}
	if validationErr != nil {
		t.Fatal(validationErr)
	}
}

func TestPreparedVectorPartitionStartupColdLoadIsReadOnlyAndFailClosedV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	dir, database, collection, _, manifest := newVectorPartitionLiveProductionFixtureV1(t)
	beforeMissing := database.State().AppliedCommandLSN
	if _, err := collection.NewPreparedVectorPartitionGenerationReplicatedLiveStartupSearchOpenPlanWithContextV1(t.Context(), manifest); err == nil {
		t.Fatal("startup published a missing live binding")
	}
	if database.State().AppliedCommandLSN != beforeMissing {
		t.Fatal("missing-binding refusal published a WAL command")
	}
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Insert([]byte("startup-cold-tail"), []byte(`{"time_us":99,"kind":"vector","did":"startup-cold-tail","embedding":[0.5,0.5]}`)); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = openCollectionCommandWALDB(t, dir)
	defer database.Close()
	source, err := OpenCollectionForRaftSourceV1(database, manifest.Collection)
	if err != nil {
		t.Fatal(err)
	}
	if source.registeredColumnGraphPartitionLiveCarrier(manifest.IndexName) != nil {
		t.Fatal("startup source eagerly loaded the carrier")
	}
	before := *database.State()
	rebuilds := 0
	restore := setColumnVectorGraphRebuildBeforeBuildTestHook(func() { rebuilds++ })
	defer restore()
	if plan, err := source.NewPreparedVectorPartitionGenerationReplicatedLiveStartupSearchOpenPlanWithContextV1(t.Context(), manifest); err != nil || plan == nil || !plan.liveRecovery {
		t.Fatalf("cold startup plan=%+v err=%v", plan, err)
	}
	carrier := source.registeredColumnGraphPartitionLiveCarrier(manifest.IndexName)
	carrier.mu.Lock()
	coverage, originalSource := carrier.partitionLive.coverage, carrier.partitionLive.source
	carrier.partitionLive.coverage++
	carrier.mu.Unlock()
	if _, err := source.NewPreparedVectorPartitionGenerationReplicatedLiveStartupSearchOpenPlanWithContextV1(t.Context(), manifest); err == nil {
		t.Fatal("startup admitted a durable coverage mismatch")
	}
	carrier.mu.Lock()
	carrier.partitionLive.coverage = coverage
	carrier.partitionLive.source.Checksum ^= 1
	carrier.mu.Unlock()
	if _, err := source.NewPreparedVectorPartitionGenerationReplicatedLiveStartupSearchOpenPlanWithContextV1(t.Context(), manifest); err == nil {
		t.Fatal("startup admitted a mismatched immutable source")
	}
	carrier.mu.Lock()
	carrier.partitionLive.source = originalSource
	carrier.mu.Unlock()
	if after := database.State(); after.CommitSeq != before.CommitSeq || after.RootPageID != before.RootPageID || after.SystemRootPageID != before.SystemRootPageID || after.AppliedCommandLSN != before.AppliedCommandLSN || rebuilds != 0 {
		t.Fatal("startup restoration/refusal changed a root, WAL command or rebuilt graph")
	}
}
