package db

import (
	"crypto/sha256"
	"errors"
	"runtime"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestStableDBAdjacentPublicationIssueRouting(t *testing.T) {
	tests := []struct {
		field rootpublication.ReachabilityField
		issue string
	}{
		{field: rootpublication.ReachabilityMetaPage, issue: "#3679"},
		{field: rootpublication.ReachabilityUserRoot, issue: "#3679"},
		{field: rootpublication.ReachabilitySystemRoot, issue: "#3679"},
		{field: rootpublication.ReachabilityFreelist, issue: "#3678"},
	}
	for _, test := range tests {
		t.Run(string(test.field), func(t *testing.T) {
			_, err := NewStableDBResourceToken(rootpublication.StableResourceSpec{Reachability: test.field})
			if !errors.Is(err, rootpublication.ErrResourceExcluded) || !strings.Contains(err.Error(), test.issue) {
				t.Fatalf("routing error=%v want ErrResourceExcluded with %s", err, test.issue)
			}
		})
	}
}

func TestOpenSharesValueLogIdentityPinRegistryWithManager(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	registry := database.ValueLogIdentityPinRegistry()
	if registry == nil {
		t.Fatal("opened DB has no value-log identity pin registry")
	}
	if got := database.valueLogManager.StableResourcePinRegistry(); got != registry {
		t.Fatalf("value-log manager registry = %p, DB registry = %p", got, registry)
	}
}

func TestStableIndexResourceTokenOwnsVacuumFenceAfterSnapshotClose(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on Windows")
	}
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	snapshot := database.AcquireStableSnapshot()
	if snapshot == nil {
		t.Fatal("AcquireStableSnapshot returned nil")
	}
	token, err := snapshot.NewStableIndexResourceToken(rootpublication.StableResourceSpec{
		Kind:          rootpublication.ResourceDictionary,
		LogicalLane:   "dictdb/index",
		ResourceID:    "index",
		Digest:        sha256.Sum256([]byte("stable-index-test")),
		Reachability:  rootpublication.ReachabilityDictionaryGeneration,
		ContentSynced: true,
	}, rootpublication.NewStableResourceToken)
	if err != nil {
		_ = snapshot.Close()
		t.Fatalf("NewStableIndexResourceToken: %v", err)
	}

	if err := snapshot.Close(); err != nil {
		t.Fatalf("Close transferred snapshot: %v", err)
	}
	if err := database.VacuumIndexOnline(t.Context()); !errors.Is(err, rootpublication.ErrResourcePinned) {
		token.Release()
		t.Fatalf("vacuum with live index token error=%v want ErrResourcePinned", err)
	}
	token.Release()
	if err := database.VacuumIndexOnline(t.Context()); err != nil {
		t.Fatalf("vacuum after index token release: %v", err)
	}
}

func TestStableIndexResourceTokenRunsCallerReleaseAfterSnapshotTeardown(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("stable index namespace capture unsupported on Windows")
	}
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	snapshot := database.AcquireStableSnapshot()
	if snapshot == nil {
		t.Fatal("AcquireStableSnapshot returned nil")
	}
	releaseCalled := false
	token, err := snapshot.NewStableIndexResourceToken(rootpublication.StableResourceSpec{
		Kind:          rootpublication.ResourceDictionary,
		LogicalLane:   "dictdb/index",
		ResourceID:    "index",
		Digest:        sha256.Sum256([]byte("stable-index-release-order-test")),
		Reachability:  rootpublication.ReachabilityDictionaryGeneration,
		ContentSynced: true,
		OnRelease: func() {
			releaseCalled = true
			if snapshot.db != nil || !snapshot.closed.Load() {
				t.Error("caller release ran before stable snapshot teardown")
			}
			if got := database.stableIndexCaptures.Load(); got != 0 {
				t.Errorf("caller release observed stable-index captures=%d want 0", got)
			}
		},
	}, rootpublication.NewStableResourceToken)
	if err != nil {
		_ = snapshot.Close()
		t.Fatalf("NewStableIndexResourceToken: %v", err)
	}

	token.Release()
	if !releaseCalled {
		t.Fatal("caller release was not invoked")
	}
}
