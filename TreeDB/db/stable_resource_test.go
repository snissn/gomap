package db

import (
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
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

func TestCaptureStableIndexFileResourceProductionWitness(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable index authority requires exact relative namespace support")
	}
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	for i := 0; i < 32; i++ {
		snapshot := database.AcquireStableSnapshot()
		if snapshot == nil {
			t.Fatal("AcquireStableSnapshot returned nil")
		}
		token, err := snapshot.CaptureStableIndexFileResource()
		if err != nil {
			_ = snapshot.Close()
			t.Fatalf("capture %d: %v", i, err)
		}
		if got := token.Kind(); got != rootpublication.ResourceIndex {
			t.Fatalf("capture %d kind=%q want %q", i, got, rootpublication.ResourceIndex)
		}
		if got := token.Reachability(); got != rootpublication.ReachabilityIndexFile {
			t.Fatalf("capture %d reachability=%q want %q", i, got, rootpublication.ReachabilityIndexFile)
		}
		if token.Identity() == (rootpublication.StableIdentity{}) || token.Frontier().Bytes == 0 {
			t.Fatalf("capture %d missing exact identity/frontier: identity=%+v frontier=%+v", i, token.Identity(), token.Frontier())
		}
		if err := snapshot.Close(); err != nil {
			t.Fatalf("capture %d close transferred snapshot: %v", i, err)
		}
		if err := token.SyncThrough(); err != nil {
			t.Fatalf("capture %d sync exact index frontier: %v", i, err)
		}
		token.Release()
		token.Release()
		if got := database.stableIndexCaptures.Load(); got != 0 {
			t.Fatalf("capture %d stable-index leases=%d want 0", i, got)
		}
	}
}

func TestCaptureStableIndexFileResourceRejectsReboundPathBeforeFreeze(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable index authority requires exact relative namespace support")
	}
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	snapshot := database.AcquireStableSnapshot()
	if snapshot == nil {
		t.Fatal("AcquireStableSnapshot returned nil")
	}
	token, err := snapshot.CaptureStableIndexFileResource()
	if err != nil {
		_ = snapshot.Close()
		t.Fatal(err)
	}
	original := filepath.Join(dir, indexFileName)
	moved := filepath.Join(dir, "index-original-rebound")
	if err := os.Rename(original, moved); err != nil {
		token.Release()
		t.Fatal(err)
	}
	if err := os.WriteFile(original, []byte("replacement-index"), 0o600); err != nil {
		token.Release()
		t.Fatal(err)
	}
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityIndexFile)
	if err := builder.Add(token); err != nil {
		token.Release()
		t.Fatal(err)
	}
	resources, err := builder.Freeze()
	if resources != nil {
		resources.Release()
	}
	if !errors.Is(err, rootpublication.ErrResourceConflict) {
		builder.Abandon()
		t.Fatalf("freeze rebound index error=%v want ErrResourceConflict", err)
	}
	builder.Abandon()
	if got := database.stableIndexCaptures.Load(); got != 0 {
		t.Fatalf("rebound index stable leases=%d want 0", got)
	}
}

func BenchmarkCaptureStableIndexFileResource(b *testing.B) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		b.Skip("stable index authority requires exact relative namespace support")
	}
	database, err := Open(Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer database.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshot := database.AcquireStableSnapshot()
		if snapshot == nil {
			b.Fatal("AcquireStableSnapshot returned nil")
		}
		token, err := snapshot.CaptureStableIndexFileResource()
		if err != nil {
			_ = snapshot.Close()
			b.Fatal(err)
		}
		token.Release()
	}
	b.StopTimer()
	if got := database.stableIndexCaptures.Load(); got != 0 {
		b.Fatalf("stable-index leases=%d want 0", got)
	}
	b.ReportMetric(1, "tokens/op")
	b.ReportMetric(1, "descriptors/op")
	b.ReportMetric(0, "logical-obligations/op")
	b.ReportMetric(1, "pin-high-water")
	b.ReportMetric(0, "content-syncs/op")
	b.ReportMetric(0, "namespace-syncs/op")
}
