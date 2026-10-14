package db

import (
	"context"
	"errors"
	"os"
	"testing"
	"unsafe"

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
	owner := d.valueLogIdentityPins.MetadataOwner()
	charged := owner.Bytes()
	if err := originalParent.Close(); err != nil {
		t.Fatal(err)
	}
	transferred, first := closure.TakeStableResources()
	if transferred != nil || !errors.Is(first, os.ErrClosed) {
		t.Fatalf("ignored original parent Close refusal: Set=%v err=%v", transferred != nil, first)
	}
	if closure.cleanupDir != originalParent || closure.resources != originalSet || len(closure.promoted) == 0 || owner.Bytes() != charged {
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
		if owner.Bytes() != charged || closure.resources != originalSet || closure.cleanupDir != originalParent {
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
			budget := &primaryScratchBudget{capacity: ^uint64(0)}
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
