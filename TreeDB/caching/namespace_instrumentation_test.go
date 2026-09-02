package caching

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

type namespaceRecoveryBackend struct {
	*MockBackend
	recoveryRequired bool
}

func (b *namespaceRecoveryBackend) MarkCommandWALRecoveryRequired() {
	b.recoveryRequired = true
}

func TestRemoveFileRetryEmitsCommandWALUnlink(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit-l0-1.log")
	if err := os.WriteFile(path, []byte("wal"), 0o600); err != nil {
		t.Fatal(err)
	}

	var events []durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		events = append(events, event)
		return nil
	})
	defer restore()

	db := &DB{backend: NewMockBackend()}
	if err := db.removeFileRetry(path); err != nil {
		t.Fatalf("removeFileRetry() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("namespace events = %d, want 1: %#v", len(events), events)
	}
	event := events[0]
	if event.Namespace != durabilitycut.NamespaceUnlink ||
		event.Resource != durabilitycut.ResourceCommandWAL ||
		event.Root != dir ||
		event.OldPath != path ||
		event.NewPath != "" {
		t.Fatalf("namespace event = %#v, want command-WAL unlink rooted at %q for %q", event, dir, path)
	}
}

func TestRemoveFileRetryPostUnlinkFailureRequiresRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit-l0-2.log")
	if err := os.WriteFile(path, []byte("wal"), 0o600); err != nil {
		t.Fatal(err)
	}

	cutErr := errors.New("injected namespace observation failure")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace == durabilitycut.NamespaceUnlink && event.OldPath == path {
			return cutErr
		}
		return nil
	})
	defer restore()

	backend := &namespaceRecoveryBackend{MockBackend: NewMockBackend()}
	db := &DB{backend: backend}
	err := db.removeFileRetry(path)
	if !errors.Is(err, cutErr) {
		t.Fatalf("removeFileRetry() error = %v, want injected failure", err)
	}
	if !errors.Is(err, backenddb.ErrRecoveryRequired) {
		t.Fatalf("removeFileRetry() error = %v, want ErrRecoveryRequired", err)
	}
	if !backend.recoveryRequired {
		t.Fatal("backend was not marked command-WAL recovery required")
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("post-observation file state error = %v, want removed file", statErr)
	}
}

func TestRemoveFileRetryClassifiesLeafValueLogUnlink(t *testing.T) {
	root := t.TempDir()
	leafDir := filepath.Join(root, "leaf_vlog")
	if err := os.MkdirAll(leafDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(leafDir, "value-l255-1.log")
	if err := os.WriteFile(path, []byte("leaf"), 0o600); err != nil {
		t.Fatal(err)
	}

	var got durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		got = event
		return nil
	})
	defer restore()

	db := &DB{backend: NewMockBackend()}
	if err := db.removeFileRetry(path); err != nil {
		t.Fatalf("removeFileRetry() error = %v", err)
	}
	if got.Namespace != durabilitycut.NamespaceUnlink ||
		got.Resource != durabilitycut.ResourceOuterLeaf ||
		got.Root != leafDir ||
		got.OldPath != path {
		t.Fatalf("namespace event = %#v, want outer-leaf unlink rooted at %q for %q", got, leafDir, path)
	}
}

func TestSaveValueLogGenerationStateEmitsUnlink(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, valueLogGenerationStateFileName)
	if err := os.WriteFile(path, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	var events []durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		events = append(events, event)
		return nil
	})
	defer restore()

	if err := saveValueLogGenerationState(path, valueLogGenerationStateFile{}); err != nil {
		t.Fatalf("saveValueLogGenerationState() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("namespace events = %d, want 1: %#v", len(events), events)
	}
	event := events[0]
	if event.Namespace != durabilitycut.NamespaceUnlink ||
		event.Resource != durabilitycut.ResourceValueLog ||
		event.Root != dir ||
		event.OldPath != path ||
		event.NewPath != "" {
		t.Fatalf("namespace event = %#v, want value-log state unlink rooted at %q for %q", event, dir, path)
	}
}
