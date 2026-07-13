//go:build darwin || linux || freebsd || netbsd || openbsd

package rootpublication

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

type countingStableNamespaceBatchAdapter struct {
	native nativeNamespaceAdapter
	mu     sync.Mutex
	syncs  map[StableIdentity]int
}

type failingStableNamespaceBatchAdapter struct {
	countingStableNamespaceBatchAdapter
	err error
}

func (adapter *failingStableNamespaceBatchAdapter) Sync(parent *os.File) error {
	if err := adapter.countingStableNamespaceBatchAdapter.Sync(parent); err != nil {
		return err
	}
	return adapter.err
}

func (adapter *countingStableNamespaceBatchAdapter) Identity(file *os.File) (StableIdentity, error) {
	return adapter.native.Identity(file)
}

func (adapter *countingStableNamespaceBatchAdapter) ValidateLink(parent, resource *os.File, name string) error {
	return adapter.native.ValidateLink(parent, resource, name)
}

func (adapter *countingStableNamespaceBatchAdapter) ValidateIdentity(parent *os.File, identity StableIdentity, name string) error {
	return adapter.native.ValidateIdentity(parent, identity, name)
}

func (adapter *countingStableNamespaceBatchAdapter) Sync(parent *os.File) error {
	identity, err := adapter.native.Identity(parent)
	if err != nil {
		return err
	}
	identity.Generation = 0
	adapter.mu.Lock()
	if adapter.syncs == nil {
		adapter.syncs = make(map[StableIdentity]int)
	}
	adapter.syncs[identity]++
	adapter.mu.Unlock()
	return nil
}

func TestStableNamespaceBatchSyncsEachDistinctParentOnce(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	destinationDir := filepath.Join(root, "destination")
	for _, dir := range []string{sourceDir, destinationDir} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	sourceParent, err := os.Open(sourceDir)
	if err != nil {
		t.Fatal(err)
	}
	defer sourceParent.Close()
	destinationParent, err := os.Open(destinationDir)
	if err != nil {
		t.Fatal(err)
	}
	defer destinationParent.Close()
	parentGeneration, err := StableNamespaceParentGeneration(destinationParent)
	if err != nil {
		t.Fatal(err)
	}
	var children []*os.File
	var registrations []StableNamespaceSpec
	for _, name := range []string{"first.pack", "second.pack"} {
		child, err := OpenStableChildFile(destinationParent, name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		children = append(children, child)
		defer child.Close()
		registrations = append(registrations, StableNamespaceSpec{
			Parent: destinationParent, LinkedResource: child, ParentGeneration: parentGeneration,
			Operation: NamespaceRename, OldName: name, NewName: name, DiagnosticPath: "maindb/leaf_vlog",
		})
	}
	adapter := &countingStableNamespaceBatchAdapter{}
	tokens, err := newStableNamespaceBatchTokens(StableNamespaceBatchSpec{
		Registrations:     registrations,
		AdditionalParents: []*os.File{sourceParent, sourceParent},
	}, adapter)
	if err != nil {
		t.Fatalf("newStableNamespaceBatchTokens: %v", err)
	}
	defer func() {
		for _, token := range tokens {
			token.Release()
		}
	}()
	if len(tokens) != len(registrations) {
		t.Fatalf("tokens=%d want %d", len(tokens), len(registrations))
	}
	for _, token := range tokens {
		if err := token.validateStable(); err != nil {
			t.Fatalf("token not stable: %v", err)
		}
	}
	for _, parent := range []*os.File{sourceParent, destinationParent} {
		identity, _ := StableIdentityFromFile(parent)
		identity.Generation = 0
		if got := adapter.syncs[identity]; got != 1 {
			t.Fatalf("parent %s syncs=%d want 1", parent.Name(), got)
		}
	}
}

func TestStableNamespaceBatchFirstSyncFailureClosesEveryRetainedParent(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	destinationDir := filepath.Join(root, "destination")
	for _, dir := range []string{sourceDir, destinationDir} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	sourceParent, err := os.Open(sourceDir)
	if err != nil {
		t.Fatal(err)
	}
	defer sourceParent.Close()
	destinationParent, err := os.Open(destinationDir)
	if err != nil {
		t.Fatal(err)
	}
	defer destinationParent.Close()
	child, err := OpenStableChildFile(destinationParent, "packed.log", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	parentGeneration, err := StableNamespaceParentGeneration(destinationParent)
	if err != nil {
		t.Fatal(err)
	}
	spec := StableNamespaceBatchSpec{
		Registrations: []StableNamespaceSpec{{
			Parent: destinationParent, LinkedResource: child, ParentGeneration: parentGeneration,
			Operation: NamespaceRename, OldName: "packed.log", NewName: "packed.log", DiagnosticPath: "leaf_vlog",
		}},
		AdditionalParents: []*os.File{sourceParent},
	}
	testErr := errors.New("first parent sync failed")
	adapter := &failingStableNamespaceBatchAdapter{err: testErr}
	const attempts = 64
	for attempt := 0; attempt < attempts; attempt++ {
		var duplicates []*os.File
		duplicate := func(file *os.File) (*os.File, error) {
			retained, err := duplicateStableFile(file)
			if err == nil {
				duplicates = append(duplicates, retained)
			}
			return retained, err
		}
		_, err := newStableNamespaceBatchTokensWithDuplicate(spec, adapter, duplicate)
		if !errors.Is(err, testErr) {
			t.Fatalf("attempt %d error=%v want %v", attempt, err, testErr)
		}
		if len(duplicates) != 2 {
			t.Fatalf("attempt %d retained parents=%d want 2", attempt, len(duplicates))
		}
		for i, retained := range duplicates {
			if _, statErr := retained.Stat(); !errors.Is(statErr, os.ErrClosed) {
				t.Fatalf("attempt %d retained parent %d remains open: %v", attempt, i, statErr)
			}
		}
	}
	adapter.mu.Lock()
	var syncCalls int
	for _, calls := range adapter.syncs {
		syncCalls += calls
	}
	adapter.mu.Unlock()
	if syncCalls != attempts {
		t.Fatalf("sync calls=%d want exactly first parent per attempt=%d", syncCalls, attempts)
	}
}
