package powerlossoracle

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestOverlayOpenFailurePreservesPathError(t *testing.T) {
	root := t.TempDir()
	first, removed := filepath.Join(root, "a-first"), filepath.Join(root, "z-removed")
	for _, path := range []string{first, removed} {
		if err := os.WriteFile(path, []byte("stable"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	model, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := model.StableFingerprint()
	identityFromFile := model.identityFromFile
	didRemove := false
	// WalkDir has already enumerated both names before visiting a-first. Reuse
	// the existing per-model identity seam, without a new observer or read hook.
	model.identityFromFile = func(file *os.File) (rootpublication.StableIdentity, error) {
		identity, err := identityFromFile(file)
		if err == nil && file.Name() == first && !didRemove {
			if err := os.Remove(removed); err != nil {
				return identity, err
			}
			didRemove = true
		}
		return identity, err
	}
	err = model.Overlay(root)
	var pathErr *os.PathError
	if !didRemove || !errors.Is(err, os.ErrNotExist) || !errors.As(err, &pathErr) {
		t.Fatalf("enumerated-name failure lost: removed=%t err=%v", didRemove, err)
	}
	if errors.Unwrap(err) != pathErr || pathErr.Op != "open" || pathErr.Path != removed ||
		!strings.HasPrefix(err.Error(), "powerlossoracle: overlay-regular-capture \"z-removed\": ") {
		t.Fatalf("overlay stage or original error identity lost: %v", err)
	}
	if model.StableFingerprint() != fingerprint {
		t.Fatal("failed overlay changed stable image")
	}
}

func TestOverlayOpenFailurePreservesIdentityError(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "value")
	if err := os.WriteFile(path, []byte("stable"), 0o600); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	identityFromFile := model.identityFromFile
	want := errors.New("identity failure")
	model.identityFromFile = func(file *os.File) (rootpublication.StableIdentity, error) {
		if file.Name() == path {
			return rootpublication.StableIdentity{}, want
		}
		return identityFromFile(file)
	}
	if err := model.Overlay(root); !errors.Is(err, want) || errors.Unwrap(err) != want ||
		!strings.HasPrefix(err.Error(), "powerlossoracle: overlay-regular-capture \"value\": ") {
		t.Fatalf("original identity error replaced: %v", err)
	}
}

func TestOverlayOpenContextPreservesSuccessfulObserve(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "value")
	if err := os.WriteFile(path, []byte("stable"), 0o600); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := model.StableFingerprint()
	if err := os.WriteFile(path, []byte("volatile"), 0o600); err != nil {
		t.Fatal(err)
	}
	event := durabilitycut.Event{Point: durabilitycut.BeforeDependencyFileSync, Resource: durabilitycut.ResourceValueLog, Path: path}
	if err := model.Observe(root, event); err != nil {
		t.Fatal(err)
	}
	if model.StableFingerprint() != fingerprint {
		t.Fatal("before-sync observation changed stable image")
	}
	assertStableContents(t, model, "value", "stable")
	event.Point = durabilitycut.AfterDependencyFileSync
	if err := model.Observe(root, event); err != nil {
		t.Fatal(err)
	}
	assertStableContents(t, model, "value", "volatile")
}
