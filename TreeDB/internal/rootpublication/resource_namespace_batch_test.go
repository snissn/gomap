//go:build darwin || linux || freebsd || netbsd || openbsd

package rootpublication

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
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

type failOnceStableNamespaceBatchAdapter struct {
	countingStableNamespaceBatchAdapter
	failureMu sync.Mutex
	failures  int
	err       error
}

func (adapter *failingStableNamespaceBatchAdapter) Sync(parent *os.File) error {
	if err := adapter.countingStableNamespaceBatchAdapter.Sync(parent); err != nil {
		return err
	}
	return adapter.err
}

func (adapter *failOnceStableNamespaceBatchAdapter) Sync(parent *os.File) error {
	if err := adapter.countingStableNamespaceBatchAdapter.Sync(parent); err != nil {
		return err
	}
	adapter.failureMu.Lock()
	defer adapter.failureMu.Unlock()
	if adapter.failures > 0 {
		adapter.failures--
		return adapter.err
	}
	return nil
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
	builder := NewStableResourceSetBuilder(ReachabilityOuterLeafPackedPointer)
	for i, child := range children {
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceOuterLeafPack, LogicalLane: "packed", ResourceID: string(rune('a' + i)), Generation: uint64(i + 1),
			DiagnosticPath: registrations[i].DiagnosticPath + "/" + registrations[i].NewName,
			File:           child, Digest: [32]byte{byte(i + 1)}, Reachability: ReachabilityOuterLeafPackedPointer,
			Namespace: tokens[i], ContentSynced: true,
		})
		if err != nil {
			builder.Abandon()
			t.Fatal(err)
		}
		if err := builder.Add(token); err != nil {
			token.Release()
			builder.Abandon()
			t.Fatal(err)
		}
	}
	set, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		t.Fatal(err)
	}
	defer set.Release()
	stats := set.Stats(time.Now())
	if len(stats) != 1 || stats[0].NamespaceSyncs != 2 {
		t.Fatalf("resource-set namespace stats=%+v want two exact physical parent syncs", stats)
	}
	wantDuration := time.Duration(0)
	for _, token := range tokens {
		_, duration := token.physicalSyncStats()
		wantDuration += duration
	}
	if stats[0].NamespaceSyncDuration != wantDuration {
		t.Fatalf("namespace sync duration=%s want exact token evidence %s", stats[0].NamespaceSyncDuration, wantDuration)
	}
}

func TestStabilizeStableNamespaceTokensSyncsParentGenerationOnce(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	parentGeneration, err := StableNamespaceParentGeneration(parent)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &countingStableNamespaceBatchAdapter{}
	var tokens []*StableNamespaceToken
	for _, name := range []string{"000001.wal", "000002.wal", "000003.wal"} {
		child, err := OpenStableChildFile(parent, name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer child.Close()
		token, err := newStableNamespaceToken(StableNamespaceSpec{
			Parent: parent, LinkedResource: child, ParentGeneration: parentGeneration,
			Operation: NamespaceCreate, NewName: name, DiagnosticPath: filepath.Join("wal", name),
		}, adapter)
		if err != nil {
			t.Fatal(err)
		}
		tokens = append(tokens, token)
		defer token.Release()
	}
	if err := StabilizeStableNamespaceTokens(tokens...); err != nil {
		t.Fatalf("StabilizeStableNamespaceTokens: %v", err)
	}
	identity, err := StableIdentityFromFile(parent)
	if err != nil {
		t.Fatal(err)
	}
	identity.Generation = 0
	if got := adapter.syncs[identity]; got != 1 {
		t.Fatalf("parent syncs=%d want 1", got)
	}
	for _, token := range tokens {
		if err := token.validateStable(); err != nil {
			t.Fatalf("token not stable: %v", err)
		}
	}
}

func TestStabilizeStableNamespaceTokensRetriesParentSyncFailure(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := OpenStableChildFile(parent, "000001.wal", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	parentGeneration, err := StableNamespaceParentGeneration(parent)
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("transient namespace sync failure")
	adapter := &failOnceStableNamespaceBatchAdapter{failures: 1, err: wantErr}
	token, err := newStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: child, ParentGeneration: parentGeneration,
		Operation: NamespaceCreate, NewName: "000001.wal", DiagnosticPath: "wal/000001.wal",
	}, adapter)
	if err != nil {
		t.Fatal(err)
	}
	defer token.Release()
	if err := StabilizeStableNamespaceTokens(token); !errors.Is(err, wantErr) {
		t.Fatalf("first stabilization error=%v want %v", err, wantErr)
	}
	if err := StabilizeStableNamespaceTokens(token); err != nil {
		t.Fatalf("retry stabilization: %v", err)
	}
	if err := token.validateStable(); err != nil {
		t.Fatalf("token not stable after retry: %v", err)
	}
	identity, err := StableIdentityFromFile(parent)
	if err != nil {
		t.Fatal(err)
	}
	identity.Generation = 0
	if got := adapter.syncs[identity]; got != 2 {
		t.Fatalf("parent sync attempts=%d want 2", got)
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
