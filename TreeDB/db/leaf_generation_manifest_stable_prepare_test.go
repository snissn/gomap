package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestLeafGenerationManifestStablePreparedClosureAbandonUnpublishedRestoresView(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable manifest replacement requires exact relative namespace support")
	}
	database, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	beforeView, err := os.ReadFile(leafGenerationManifestPath(resolveStorageLayout(database.dir).leafVLogDir))
	if err != nil {
		t.Fatal(err)
	}
	database.mu.RLock()
	candidate := database.leafGenerationManifest.clone()
	database.mu.RUnlock()

	closure, _, err := database.prepareLeafGenerationManifestStableCandidate(candidate)
	if err != nil {
		t.Fatalf("prepareLeafGenerationManifestStableCandidate: %v", err)
	}
	resources, err := closure.TakeStableResources()
	if err != nil {
		closure.Release()
		t.Fatal(err)
	}
	resources.Release()
	if err := closure.abandonUnpublished(); err != nil {
		t.Fatalf("abandonUnpublished: %v", err)
	}
	if err := closure.abandonUnpublished(); err != nil {
		t.Fatalf("second abandonUnpublished: %v", err)
	}
	afterView, err := os.ReadFile(leafGenerationManifestPath(resolveStorageLayout(database.dir).leafVLogDir))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(afterView, beforeView) {
		t.Fatalf("compatibility manifest changed after abandonment\nbefore=%s\nafter=%s", beforeView, afterView)
	}
	exactPath := filepath.Join(resolveStorageLayout(database.dir).leafVLogDir, leafGenerationDurableManifestFileName(closure.Revision()))
	if _, err := os.Stat(exactPath); !os.IsNotExist(err) {
		t.Fatalf("unpublished exact manifest remains at %q: %v", exactPath, err)
	}
	if database.publicationPoisoned.Load() {
		t.Fatal("exact unpublished manifest abandonment poisoned DB")
	}
}

func TestLeafGenerationManifestStablePreparedClosureUsesActualReplacement(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable manifest replacement requires exact relative namespace support")
	}
	database, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	registry := database.StableResourceIdentityPinRegistry()
	baselinePins := registry.ActivePins()
	baselineIdentities := registry.ActiveIdentities()
	beforeState := *database.State()
	database.mu.RLock()
	beforeRevision := database.leafGenerationManifest.ManifestRevision
	database.mu.RUnlock()

	closure, err := database.PrepareLeafGenerationManifestStableClosure()
	if err != nil {
		t.Fatalf("PrepareLeafGenerationManifestStableClosure: %v", err)
	}
	if closure == nil {
		t.Fatal("manifest replacement returned nil closure")
	}
	if closure.Revision() <= beforeRevision || closure.Digest() == ([32]byte{}) {
		revision, digest := closure.Revision(), closure.Digest()
		closure.Release()
		t.Fatalf("manifest closure revision=%d before=%d digest=%x", revision, beforeRevision, digest)
	}
	afterState := *database.State()
	if afterState.CommitSeq != beforeState.CommitSeq || afterState.SystemRootPageID != beforeState.SystemRootPageID || afterState.AppliedCommandLSN != beforeState.AppliedCommandLSN {
		closure.Release()
		t.Fatalf("manifest replacement changed DB root/meta visibility before=%+v after=%+v", beforeState, afterState)
	}
	persisted, ok, err := database.leafGenerationManifestStore.Load()
	if err != nil || !ok {
		closure.Release()
		t.Fatalf("Load persisted replacement ok=%t err=%v", ok, err)
	}
	if persisted.ManifestRevision != closure.Revision() {
		closure.Release()
		t.Fatalf("persisted revision=%d closure=%d", persisted.ManifestRevision, closure.Revision())
	}
	database.mu.RLock()
	inMemoryRevision := database.leafGenerationManifest.ManifestRevision
	database.mu.RUnlock()
	if inMemoryRevision != persisted.ManifestRevision {
		closure.Release()
		t.Fatalf("in-memory revision=%d persisted=%d", inMemoryRevision, persisted.ManifestRevision)
	}
	if observed := closure.Observations(); observed.ContentSyncs != 1 || observed.NamespaceSyncs != 1 {
		closure.Release()
		t.Fatalf("manifest producer observations=%+v want one content and namespace sync", observed)
	}
	if got := registry.ActivePins(); got != baselinePins+1 {
		closure.Release()
		t.Fatalf("active pins after manifest replace=%d want %d", got, baselinePins+1)
	}

	resources, err := closure.TakeStableResources()
	if err != nil {
		closure.Release()
		t.Fatal(err)
	}
	descriptors := resources.Descriptors()
	if len(descriptors) != 1 || descriptors[0].Generation() != persisted.ManifestRevision || descriptors[0].Digest() != closure.Digest() {
		resources.Release()
		t.Fatalf("manifest descriptors=%+v revision=%d digest=%x", descriptors, persisted.ManifestRevision, closure.Digest())
	}
	if _, err := closure.TakeStableResources(); !errors.Is(err, ErrLeafGenerationManifestStablePreparedClosureConsumed) {
		resources.Release()
		t.Fatalf("second TakeStableResources error=%v want consumed", err)
	}
	closure.Release()
	closure.Abandon()
	resources.Release()
	if got := registry.ActivePins(); got != baselinePins {
		t.Fatalf("active pins after release=%d want %d", got, baselinePins)
	}
	if got := registry.ActiveIdentities(); got != baselineIdentities {
		t.Fatalf("active identities after release=%d want %d", got, baselineIdentities)
	}
}

func TestLeafGenerationManifestStablePrepareUnsupportedFailsBeforeCandidate(t *testing.T) {
	if rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("platform supports stable manifest replacement")
	}
	database, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	before := *database.State()
	closure, err := database.PrepareLeafGenerationManifestStableClosure()
	if closure != nil {
		closure.Release()
		t.Fatal("unsupported manifest prepare returned a closure")
	}
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("PrepareLeafGenerationManifestStableClosure error=%v want namespace unsupported", err)
	}
	after := *database.State()
	if after.CommitSeq != before.CommitSeq || after.SystemRootPageID != before.SystemRootPageID || after.AppliedCommandLSN != before.AppliedCommandLSN {
		t.Fatalf("unsupported manifest prepare changed visibility before=%+v after=%+v", before, after)
	}
}

func TestLeafGenerationManifestStablePrepareSerializesWritersAndClose(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable manifest replacement requires exact relative namespace support")
	}
	database, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	store := database.leafGenerationManifestStore
	if store == nil {
		t.Fatal("missing leaf generation manifest store")
	}
	storeEntered := make(chan struct{})
	releaseStore := make(chan struct{})
	var blockFirstReplace sync.Once
	store.hooks.BeforeTempCreate = func() error {
		blockFirstReplace.Do(func() {
			close(storeEntered)
			<-releaseStore
		})
		return nil
	}

	type prepareResult struct {
		closure *LeafGenerationManifestStablePreparedClosure
		err     error
	}
	prepareDone := make(chan prepareResult, 1)
	go func() {
		closure, prepareErr := database.PrepareLeafGenerationManifestStableClosure()
		prepareDone <- prepareResult{closure: closure, err: prepareErr}
	}()
	<-storeEntered

	// At the exact persistent replacement boundary, Close's maintenance gate,
	// ordinary manifest writers, and delayed commit post-work must all remain
	// excluded. TryLock makes this deterministic without scheduler timing.
	if database.maintenanceMu.TryLock() {
		database.maintenanceMu.Unlock()
		close(releaseStore)
		t.Fatal("manifest prepare did not hold maintenanceMu across replacement")
	}
	if database.writeMu.TryLock() {
		database.writeMu.Unlock()
		close(releaseStore)
		t.Fatal("manifest prepare did not hold writeMu across replacement")
	}
	if database.commitMu.TryLock() {
		database.commitMu.Unlock()
		close(releaseStore)
		t.Fatal("manifest prepare did not hold commitMu across replacement")
	}

	// Queue the production reconciliation writer while prepare owns the
	// manifest transition. Its new physical file must be incorporated only
	// after the prepared replacement, never overwritten by a stale clone.
	const recoveredSeq = uint32(8_000_000)
	recoveredName := fmt.Sprintf("value-l%d-%d.log", rewriteLeafLogLaneID, recoveredSeq)
	if err := os.WriteFile(filepath.Join(LeafLogDirPath(database.dir), recoveredName), nil, 0o600); err != nil {
		close(releaseStore)
		t.Fatal(err)
	}
	type reconcileResult struct {
		changed bool
		err     error
	}
	reconcileDone := make(chan reconcileResult, 1)
	go func() {
		changed, reconcileErr := database.reconcileLeafGenerationManifestWithDirInPlace(database.State().CommitSeq + 1)
		reconcileDone <- reconcileResult{changed: changed, err: reconcileErr}
	}()

	close(releaseStore)
	prepared := <-prepareDone
	if prepared.err != nil {
		t.Fatalf("PrepareLeafGenerationManifestStableClosure: %v", prepared.err)
	}
	if prepared.closure == nil {
		t.Fatal("manifest replacement returned nil closure")
	}
	preparedRevision := prepared.closure.Revision()
	prepared.closure.Release()

	reconciled := <-reconcileDone
	if reconciled.err != nil {
		t.Fatalf("reconcile queued after prepare: %v", reconciled.err)
	}
	if !reconciled.changed {
		t.Fatal("queued reconciliation did not incorporate recovered leaf file")
	}
	persisted, ok, err := store.Load()
	if err != nil || !ok {
		t.Fatalf("Load persisted reconciled manifest ok=%t err=%v", ok, err)
	}
	if persisted.ManifestRevision <= preparedRevision {
		t.Fatalf("persisted revision=%d want newer than prepared revision %d", persisted.ManifestRevision, preparedRevision)
	}
	wantRawFileID, err := valuelog.EncodeSegmentID(rewriteLeafLogLaneID, recoveredSeq)
	if err != nil {
		t.Fatal(err)
	}
	if !persisted.hasNonDeletedFileID(wantRawFileID) {
		t.Fatalf("persisted manifest revision %d lost reconciled raw file %d", persisted.ManifestRevision, wantRawFileID)
	}
	database.mu.RLock()
	inMemory := database.leafGenerationManifest.clone()
	database.mu.RUnlock()
	if inMemory.ManifestRevision != persisted.ManifestRevision || !inMemory.hasNonDeletedFileID(wantRawFileID) {
		t.Fatalf("in-memory manifest revision=%d disagrees with persisted revision=%d for raw file %d", inMemory.ManifestRevision, persisted.ManifestRevision, wantRawFileID)
	}
}
