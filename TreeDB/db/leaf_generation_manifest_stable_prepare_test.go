package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

type manifestPrepareRefusalBudget struct {
	frame          string
	bytes          uint64
	refused        bool
	refuseRollback bool
}

func (budget *manifestPrepareRefusalBudget) AcquireRetention(bytes uint64) (retainedalloc.Lease, error) {
	budget.bytes = bytes
	return budget, nil
}

func (budget *manifestPrepareRefusalBudget) Close() {}

func (budget *manifestPrepareRefusalBudget) Resize(bytes uint64) error {
	if bytes <= budget.bytes {
		budget.bytes = bytes
		return nil
	}
	if budget.refused && budget.refuseRollback {
		return retainedalloc.ErrCapacity
	}
	if !budget.refused {
		var callers [24]uintptr
		n := runtime.Callers(2, callers[:])
		frames := runtime.CallersFrames(callers[:n])
		for {
			frame, more := frames.Next()
			if strings.HasSuffix(frame.Function, budget.frame) {
				budget.refused = true
				return retainedalloc.ErrCapacity
			}
			if !more {
				break
			}
		}
	}
	budget.bytes = bytes
	return nil
}

func TestLeafGenerationManifestStablePrepareAdmissionRollback(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable manifest replacement requires exact relative namespace support")
	}
	for _, tc := range []struct {
		name           string
		frame          string
		refuseRollback bool
		refuseFence    bool
	}{
		{name: "rollback identity", frame: ".admitReplacementIdentity"},
		{name: "rollback identity deletion fence refused", frame: ".admitReplacementIdentity", refuseFence: true},
		{name: "builder creation", frame: ".NewStableResourceSetBuilderWithMetadata"},
		{name: "builder add", frame: ".addAdmittedToken"},
		{name: "builder freeze", frame: ".Freeze"},
		{name: "rollback refused", frame: ".NewStableResourceSetBuilderWithMetadata", refuseRollback: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testLeafGenerationManifestStablePrepareAdmissionRollback(t, tc.frame, tc.refuseRollback, tc.refuseFence)
		})
	}
}

func testLeafGenerationManifestStablePrepareAdmissionRollback(t *testing.T, refusalFrame string, refuseRollback, refuseFence bool) {
	t.Helper()
	database, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	fenceErr := errors.New("early rollback deletion fence refused")
	t.Cleanup(func() {
		err := database.Close()
		if refuseFence {
			if !errors.Is(err, fenceErr) || !errors.Is(err, os.ErrClosed) {
				t.Errorf("close after refused deletion fence=%v want retained fence and file completion failures", err)
			}
		} else if refuseRollback {
			if !errors.Is(err, retainedalloc.ErrCapacity) {
				t.Errorf("close after refused rollback=%v want retained capacity failure", err)
			}
		} else if err != nil {
			t.Error(err)
		}
	})
	leafDir := resolveStorageLayout(database.dir).leafVLogDir
	if _, err := os.Stat(leafGenerationManifestPath(leafDir)); !os.IsNotExist(err) {
		t.Fatalf("stable open must not create manifest.json: %v", err)
	}
	immutableNames := func() []string {
		t.Helper()
		names, err := filepath.Glob(filepath.Join(leafDir, "manifest.durable.*.json"))
		if err != nil {
			t.Fatal(err)
		}
		return names
	}
	beforeNames := immutableNames()
	database.mu.RLock()
	candidate := database.leafGenerationManifest.clone()
	database.mu.RUnlock()
	registry := database.StableResourceIdentityPinRegistry()
	owner := registry.MetadataOwner()
	budget := &manifestPrepareRefusalBudget{frame: refusalFrame, refuseRollback: refuseRollback}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	beforePins, beforeIdentities, beforeLinks := registry.ActivePins(), registry.ActiveIdentities(), registry.ActiveStableNamespaceLinks()
	beforeBytes := owner.Bytes()
	if refuseFence {
		restore := durabilitycut.Install(func(event durabilitycut.Event) error {
			if event.Resource != durabilitycut.ResourceOuterLeaf || event.Root != leafDir {
				return nil
			}
			if event.Namespace == durabilitycut.NamespaceUnlink {
				if event.StablePath == nil {
					t.Error("unlink omitted the exact open revision")
				} else if err := event.StablePath.Close(); err != nil {
					t.Errorf("inject physical completion refusal: %v", err)
				}
			}
			if event.Point == durabilitycut.BeforeDeletionDirectorySync {
				return fenceErr
			}
			return nil
		})
		defer restore()
	}
	closure, persisted, err := database.prepareLeafGenerationManifestStableCandidate(candidate)
	if !budget.refused || closure != nil || persisted != nil || !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("refused=%t closure=%p persisted=%p error=%v", budget.refused, closure, persisted, err)
	}
	if got := registry.ActivePins(); got != beforePins {
		t.Errorf("pins after admission refusal=%d want %d", got, beforePins)
	}
	if refuseFence {
		if !errors.Is(err, fenceErr) || !errors.Is(err, os.ErrClosed) || !errors.Is(err, ErrRecoveryRequired) || !database.publicationPoisoned.Load() {
			t.Errorf("refused deletion fence=%v poisoned=%t", err, database.publicationPoisoned.Load())
		}
		var receipt *originalDBCleanupFailure
		if !errors.As(err, &receipt) {
			t.Error("refused deletion fence omitted the original cleanup receipt")
		} else if original, ok := receipt.payload.(*LeafGenerationManifestStablePreparedClosure); !ok || original.replacementFiles[0] == nil || !errors.Is(original.replacementErrors[0], os.ErrClosed) {
			t.Errorf("receipt did not retain the original revision file and failed completion: %T", receipt.payload)
		}
		if got := owner.Bytes(); got <= beforeBytes {
			t.Errorf("refused deletion fence shed original cleanup metadata: got %d baseline %d", got, beforeBytes)
		}
		if !slices.Equal(beforeNames, immutableNames()) {
			t.Error("refused deletion fence left the unlinked name visible")
		}
		if _, err := database.PrepareLeafGenerationManifestStableClosure(); !errors.Is(err, ErrRecoveryRequired) {
			t.Errorf("prepare after refused deletion fence=%v want recovery required", err)
		}
		return
	}
	if refuseRollback {
		if !errors.Is(err, ErrRecoveryRequired) || !database.publicationPoisoned.Load() {
			t.Fatalf("refused rollback error=%v poisoned=%t", err, database.publicationPoisoned.Load())
		}
		if got := owner.Bytes(); got <= beforeBytes {
			t.Errorf("refused rollback shed original cleanup metadata: got %d baseline %d", got, beforeBytes)
		}
		if slices.Equal(beforeNames, immutableNames()) {
			t.Errorf("refused rollback lost the installed manifest evidence")
		}
		if _, _, err := database.leafGenerationManifestStore.Load(); !errors.Is(err, ErrRecoveryRequired) {
			t.Errorf("load after refused rollback=%v want recovery required", err)
		}
		if _, err := database.PrepareLeafGenerationManifestStableClosure(); !errors.Is(err, ErrRecoveryRequired) {
			t.Errorf("prepare after refused rollback=%v want recovery required", err)
		}
		return
	}
	if _, err := os.Stat(leafGenerationManifestPath(leafDir)); !os.IsNotExist(err) {
		t.Errorf("admission refusal created manifest.json: %v", err)
	}
	if after := immutableNames(); !slices.Equal(beforeNames, after) {
		t.Errorf("immutable names changed after admission refusal\nbefore=%v\nafter=%v", beforeNames, after)
	}
	if got := registry.ActiveIdentities(); got != beforeIdentities {
		t.Errorf("identities after admission refusal=%d want %d", got, beforeIdentities)
	}
	if got := registry.ActiveStableNamespaceLinks(); got != beforeLinks {
		t.Errorf("namespace links after admission refusal=%d want %d", got, beforeLinks)
	}
	if got := owner.Bytes(); got != beforeBytes {
		t.Errorf("metadata after admission refusal=%d want %d", got, beforeBytes)
	}
	if database.publicationPoisoned.Load() {
		t.Fatal("definitively abandoned admission refusal poisoned DB")
	}
	closure, _, err = database.prepareLeafGenerationManifestStableCandidate(candidate)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got, want := closure.Revision(), candidate.ManifestRevision+1; got != want {
		t.Errorf("retry revision=%d want %d", got, want)
	}
	if err := closure.abandonUnpublished(); err != nil {
		t.Fatalf("abandon retry: %v", err)
	}
	if got := registry.ActivePins(); got != beforePins {
		t.Errorf("pins after retry abandonment=%d want %d", got, beforePins)
	}
}

func TestLeafGenerationManifestStablePreparedClosureAbandonUnpublishedRestoresView(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable manifest replacement requires exact relative namespace support")
	}
	database, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	leafDir := resolveStorageLayout(database.dir).leafVLogDir
	beforeLoaded, ok, err := database.leafGenerationManifestStore.Load()
	if err != nil || !ok {
		t.Fatalf("Load before prepare: ok=%v err=%v", ok, err)
	}
	beforePath := filepath.Join(leafDir, leafGenerationDurableManifestFileName(beforeLoaded.ManifestRevision))
	beforeView, err := os.ReadFile(beforePath)
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
	if _, err := os.Stat(leafGenerationManifestPath(leafDir)); !os.IsNotExist(err) {
		t.Fatalf("stable abandonment must not create manifest.json: %v", err)
	}
	afterLoaded, ok, err := database.leafGenerationManifestStore.Load()
	if err != nil || !ok || afterLoaded.ManifestRevision != beforeLoaded.ManifestRevision {
		t.Fatalf("Load after abandonment: ok=%v rev=%v want=%d err=%v", ok, afterLoaded, beforeLoaded.ManifestRevision, err)
	}
	afterView, err := os.ReadFile(beforePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(afterView, beforeView) {
		t.Fatalf("prior durable manifest changed after abandonment\nbefore=%s\nafter=%s", beforeView, afterView)
	}
	exactPath := filepath.Join(leafDir, leafGenerationDurableManifestFileName(closure.Revision()))
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
	diagnostics, err := resources.AcquirePhysicalDiagnostics()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := diagnostics.CloseWithError(); err != nil {
			t.Error(err)
		}
	}()
	descriptors := diagnostics.Physical()
	if len(descriptors) != 1 || descriptors[0].Generation != persisted.ManifestRevision || descriptors[0].Digest() != closure.Digest() {
		resources.Release()
		t.Fatalf("manifest descriptors=%+v revision=%d digest=%x", descriptors, persisted.ManifestRevision, closure.Digest())
	}
	if err := diagnostics.CloseWithError(); err != nil {
		t.Fatal(err)
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
