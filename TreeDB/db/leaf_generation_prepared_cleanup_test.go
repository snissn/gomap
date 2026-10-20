package db

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestOriginalPreparedPackParentCloseRefusalKeepsCustody(t *testing.T) {
	if !rootpublication.StableCrossParentMoveNoReplaceSupported() {
		t.Skip("exact promotion unsupported")
	}
	d, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	prior := errors.New("preexisting background error")
	d.bgErrMu.Lock()
	d.bgErr = prior
	d.bgErrMu.Unlock()
	closure, err := d.PrepareLeafGenerationPackStableClosure(context.Background(), [][]byte{buildLeafGenerationPackStablePage(t, 'x')})
	if err != nil {
		t.Fatal(err)
	}
	originalParent, originalSet := closure.cleanupDir, closure.resources
	originalCapacity := closure.cleanupChildCapacity
	if originalCapacity == 0 {
		t.Fatal("packed cleanup capacity was not admitted with its original closure")
	}
	owner := d.valueLogIdentityPins.MetadataOwner()
	charged := owner.Bytes()
	if err := originalParent.Close(); err != nil {
		t.Fatal(err)
	}
	transferred, first := closure.TakeStableResources()
	if transferred != nil || !errors.Is(first, os.ErrClosed) {
		t.Fatalf("ignored original parent Close refusal: Set=%v err=%v", transferred != nil, first)
	}
	if closure.cleanupDir != originalParent || closure.resources != originalSet || len(closure.promoted) == 0 || owner.Bytes() != charged || closure.cleanupChildCapacity != originalCapacity {
		t.Fatal("refusal discarded original parent/Set/arrays or refunded metadata")
	}
	if !errors.Is(d.backgroundError(), first) || !errors.Is(d.backgroundError(), prior) {
		t.Fatal("DB error graph lost same original or prior sibling")
	}
	for i := 0; i < 3; i++ {
		if closure.Release() != first {
			t.Fatal("Release replaced first physical outcome")
		}
		if next, err := closure.TakeStableResources(); next != nil || !errors.Is(err, first) {
			t.Fatal("duplicate Take replayed or forgot first outcome")
		}
		if owner.Bytes() != charged || closure.resources != originalSet || closure.cleanupDir != originalParent || closure.cleanupChildCapacity != originalCapacity {
			t.Fatal("duplicate disposition refunded failed payload")
		}
	}
}

func TestOriginalPreparedManifestPhysicalCloseRefusalKeepsCustody(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("exact manifest namespace unsupported")
	}
	d, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	prior := errors.New("prior manifest background error")
	d.bgErrMu.Lock()
	d.bgErr = prior
	d.bgErrMu.Unlock()
	closure, err := d.PrepareLeafGenerationManifestStableClosure()
	if err != nil {
		t.Fatal(err)
	}
	originalSet := closure.resources
	owner := d.valueLogIdentityPins.MetadataOwner()
	if err := originalSet.WithScopedTokens(func(tokens []*rootpublication.StableResourceToken) error {
		if len(tokens) != 1 {
			t.Fatal("missing actual manifest token")
		}
		return tokens[0].WithPinnedFile(func(file *os.File) error { return file.Close() })
	}); err != nil {
		t.Fatal(err)
	}
	retainedMinimum := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(*closure))) + retainedalloc.AllocationCharge(uint64(cap(closure.previousView)))
	closure.Release()
	first := d.backgroundError()
	if !errors.Is(first, os.ErrClosed) || !errors.Is(first, prior) || closure.resources != originalSet || len(closure.previousView) == 0 {
		t.Fatalf("lost actual original manifest cleanup: %v", first)
	}
	if owner.Bytes() < retainedMinimum {
		t.Fatalf("refunded failed containing/rollback payload: bytes=%d minimum=%d", owner.Bytes(), retainedMinimum)
	}
	after := owner.Bytes()
	for i := 0; i < 3; i++ {
		closure.Release()
		if d.backgroundError() != first || owner.Bytes() != after || closure.resources != originalSet {
			t.Fatal("duplicate Release replayed or refunded failed original")
		}
	}
}

func TestOriginalPreparedClosureHeaderRefusesBeforeProducerEffects(t *testing.T) {
	for _, kind := range []string{"manifest", "pack"} {
		t.Run(kind, func(t *testing.T) {
			if !rootpublication.StableRelativeNamespaceSupported() || !rootpublication.StableCrossParentMoveNoReplaceSupported() {
				t.Skip("exact namespace unsupported")
			}
			d, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			owner := d.valueLogIdentityPins.MetadataOwner()
			budget := &retainedScratchBudget{capacity: ^uint64(0)}
			enrollment, err := owner.Enroll(budget)
			if err != nil {
				t.Fatal(err)
			}
			defer enrollment.Close()
			baseline := owner.Bytes()
			budget.capacity = baseline
			hooks := 0
			if kind == "manifest" {
				store := d.leafGenerationManifestStore
				before := store.durabilityCounters.ContentSyncs.Load()
				old := store.hooks.BeforeTempCreate
				store.hooks.BeforeTempCreate = func() error { hooks++; return nil }
				defer func() { store.hooks.BeforeTempCreate = old }()
				closure, err := d.PrepareLeafGenerationManifestStableClosure()
				if closure != nil || !errors.Is(err, retainedalloc.ErrCapacity) || store.durabilityCounters.ContentSyncs.Load() != before {
					t.Fatalf("unadmitted manifest producer: %v", err)
				}
			} else {
				restore := setLeafGenerationPackStableBeforeCaptureAdmissionTestHook(func() { hooks++ })
				defer restore()
				closure, err := d.PrepareLeafGenerationPackStableClosure(context.Background(), [][]byte{buildLeafGenerationPackStablePage(t, 'y')})
				if closure != nil || !errors.Is(err, retainedalloc.ErrCapacity) {
					t.Fatalf("unadmitted pack producer: %v", err)
				}
			}
			if hooks != 0 || owner.Bytes() != baseline {
				t.Fatalf("header refusal reached producer effects or changed capacity: hooks=%d bytes=%d baseline=%d", hooks, owner.Bytes(), baseline)
			}
		})
	}
}

// Fault the actual restored compatibility file after its namespace sync. The
// original generic helper ignored this exact last Close; the same closure must
// retain its real post-disposition failure without replaying the authorized
// restore/unlink on a second observation.
func TestOriginalPreparedManifestAbandonActualCloseRefusalKeepsCustody(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("exact manifest namespace unsupported")
	}
	d, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	d.mu.RLock()
	candidate := d.leafGenerationManifest.clone()
	d.mu.RUnlock()
	closure, _, err := d.prepareLeafGenerationManifestStableCandidate(candidate)
	if err != nil {
		t.Fatal(err)
	}
	resources, err := closure.TakeStableResources()
	if err != nil {
		t.Fatal(err)
	}
	if err := resources.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	prior := errors.New("prior unrelated abandonment background error")
	d.bgErrMu.Lock()
	d.bgErr = prior
	d.bgErrMu.Unlock()
	var originalRestore *os.File
	var closeInjected bool
	var restores, unlinks int
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace == durabilitycut.NamespaceRename && event.NewPath == leafGenerationManifestPath(closure.store.leafDir) {
			originalRestore = event.StablePath
			restores++
		}
		if event.Namespace == durabilitycut.NamespaceUnlink {
			unlinks++
		}
		if event.Point == durabilitycut.AfterNewFileDirectorySync && originalRestore != nil && !closeInjected {
			closeInjected = true
			return originalRestore.Close()
		}
		return nil
	})
	defer restore()
	first := closure.abandonUnpublished()
	if !closeInjected || !errors.Is(first, os.ErrClosed) {
		t.Fatalf("ignored actual restored-file last Close: injected=%t err=%v", closeInjected, first)
	}
	restoreFileIndex := 4
	if closure.replacementFiles[restoreFileIndex] != originalRestore || len(closure.previousView) == 0 || closure.metadata == nil || closure.metadata.Bytes() < closure.metadataCharge {
		t.Fatal("failed physical completion lost original restore handle/buffers or admitted charge")
	}
	if !errors.Is(d.backgroundError(), first) || !errors.Is(d.backgroundError(), prior) {
		t.Fatal("existing DB graph lost original post-disposition failure or sibling")
	}
	exactPath := filepath.Join(closure.store.leafDir, closure.resourceID)
	if _, err := os.Stat(exactPath); !os.IsNotExist(err) {
		t.Fatalf("actual authorized unlink not reported truthfully: %v", err)
	}
	if restores != 1 || unlinks != 1 {
		t.Fatalf("missing exact restore/unlink route: restores=%d unlinks=%d", restores, unlinks)
	}
	after := closure.metadata.Bytes()
	for i := 0; i < 3; i++ {
		if closure.abandonUnpublished() != first || closure.ReleaseWithError() != first || restores != 1 || unlinks != 1 || closure.metadata.Bytes() != after {
			t.Fatal("repeat abandoned original replayed namespace or refunded failed payload")
		}
	}
}

func TestOriginalPreparedManifestAbandonAdmissionRefusesBeforeRestore(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("exact manifest namespace unsupported")
	}
	d, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	d.mu.RLock()
	candidate := d.leafGenerationManifest.clone()
	d.mu.RUnlock()
	closure, _, err := d.prepareLeafGenerationManifestStableCandidate(candidate)
	if err != nil {
		t.Fatal(err)
	}
	resources, err := closure.TakeStableResources()
	if err != nil {
		t.Fatal(err)
	}
	if err := resources.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	owner := closure.metadata
	budget := &retainedScratchBudget{capacity: ^uint64(0)}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	budget.capacity = baseline
	mutations := 0
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace != "" {
			mutations++
		}
		return nil
	})
	defer restore()
	first := closure.abandonUnpublished()
	if !errors.Is(first, retainedalloc.ErrCapacity) || mutations != 0 || owner.Bytes() != baseline || len(closure.previousView) == 0 {
		t.Fatalf("unadmitted abandonment changed namespace/debt: err=%v mutations=%d bytes=%d baseline=%d", first, mutations, owner.Bytes(), baseline)
	}
	if closure.abandonUnpublished() != first || owner.Bytes() != baseline {
		t.Fatal("duplicate admission refusal replayed original action")
	}
}
