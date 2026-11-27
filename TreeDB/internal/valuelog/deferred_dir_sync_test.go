package valuelog

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestNewWriterWithStableResourcePinRegistryDeferredDirSync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-l255-000001.log")

	if _, err := NewWriterWithStableResourcePinRegistryDeferredDirSync(path, 1, nil); err == nil {
		t.Fatal("expected error for nil pin registry")
	}

	registry := rootpublication.NewIdentityPinRegistry()
	var dirSyncs atomic.Int64
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeNewFileDirectorySync {
			dirSyncs.Add(1)
		}
		return nil
	})
	defer restore()

	w, err := NewWriterWithStableResourcePinRegistryDeferredDirSync(path, 1, registry)
	if err != nil {
		t.Fatalf("NewWriterWithStableResourcePinRegistryDeferredDirSync: %v", err)
	}
	defer func() {
		if closeErr := w.Close(); closeErr != nil {
			t.Errorf("close writer: %v", closeErr)
		}
	}()

	pending, err := w.StableCreationNamespacePending()
	if err != nil {
		t.Fatalf("StableCreationNamespacePending: %v", err)
	}

	if rootpublication.StableRelativeNamespaceSupported() {
		if got := dirSyncs.Load(); got != 0 {
			t.Fatalf("deferred open emitted %d directory syncs, want 0", got)
		}
		if !pending || !w.StableCreationNamespaceUncertified() {
			t.Fatalf("expected pending=true and uncertified=true after deferred creation, got pending=%v uncertified=%v",
				pending, w.StableCreationNamespaceUncertified())
		}
		if err := w.CertifyStableCreationNamespace(); err != nil {
			t.Fatalf("CertifyStableCreationNamespace: %v", err)
		}
		if got := dirSyncs.Load(); got != 1 {
			t.Fatalf("CertifyStableCreationNamespace emitted %d directory syncs, want 1", got)
		}
		if w.StableCreationNamespaceUncertified() {
			t.Fatal("expected StableCreationNamespaceUncertified() == false after certify")
		}
		pendingAfter, err := w.StableCreationNamespacePending()
		if err != nil {
			t.Fatalf("StableCreationNamespacePending after certify: %v", err)
		}
		if !pendingAfter {
			t.Fatal("expected StableCreationNamespacePending() == true after certify (creation proof retained)")
		}
		// Second certify must be an idempotent no-op.
		if err := w.CertifyStableCreationNamespace(); err != nil {
			t.Fatalf("second CertifyStableCreationNamespace: %v", err)
		}
		if got := dirSyncs.Load(); got != 1 {
			t.Fatalf("second CertifyStableCreationNamespace emitted extra syncs: total=%d want 1", got)
		}
	} else {
		if got := dirSyncs.Load(); got != 1 {
			t.Fatalf("non-relative platform open emitted %d directory syncs, want 1", got)
		}
		if w.StableCreationNamespaceUncertified() {
			t.Fatal("non-relative platform open should not leave creation uncertified")
		}
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("created segment missing: %v", err)
	}
}
