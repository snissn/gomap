package db

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
)

// Limit only an isolated test subprocess: the package's other tests may run in
// parallel and must never inherit a process-wide descriptor limit change.
func TestLeafManifestRevisionGCBoundedDescriptors(t *testing.T) {
	if os.Getenv("GOMAP_REVISION_FD_CHILD") != "1" {
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		command := exec.Command(executable, "-test.run=^TestLeafManifestRevisionGCBoundedDescriptors$", "-test.v")
		command.Env = append(os.Environ(), "GOMAP_REVISION_FD_CHILD=1")
		output, err := command.CombinedOutput()
		t.Logf("isolated child:\n%s", output)
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatal(err)
	}
	if limit.Max < 64 {
		t.Fatalf("hard descriptor limit %d below test requirement", limit.Max)
	}
	limit.Cur = 64
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatal(err)
	}
	s, held, current := newRevisionStore5066(t)
	current.Release()
	for range 254 {
		token, err := s.Replace(newLeafGenerationManifest(1))
		if err != nil {
			t.Fatal(err)
		}
		token.Release()
	}
	current, err := s.Replace(newLeafGenerationManifest(1))
	if err != nil {
		t.Fatal(err)
	}
	defer current.Release()
	const revisions = 257
	baseline := revisionFDCount(t)
	dry := LeafGenerationGCStats{}
	if err := s.gcRevisions(context.Background(), LeafGenerationGCOptions{DryRun: true}, &dry); err != nil {
		t.Fatalf("dry inventory: %v", err)
	}
	if dry.ManifestRevisionsTotal != revisions || dry.ManifestRevisionsEligible != revisions-2 || dry.ManifestRevisionsDeleted != 0 {
		t.Fatalf("dry census: %+v", dry)
	}
	// Even a malformed tail after more than one batch must prevent every unlink.
	bad := filepath.Join(s.leafDir, "manifest.durable.bad.json")
	if err := os.WriteFile(bad, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	malformed := LeafGenerationGCStats{}
	if err := s.gcRevisions(context.Background(), LeafGenerationGCOptions{}, &malformed); err == nil || malformed.ManifestRevisionsDeleted != 0 {
		t.Fatalf("malformed inventory: stats=%+v err=%v", malformed, err)
	}
	if err := os.Remove(bad); err != nil {
		t.Fatal(err)
	}
	peak := baseline
	s.hooks.BeforeRename = func() error {
		if count := revisionFDCount(t); count > peak {
			peak = count
		}
		return nil
	}
	// A footprint admitted for the first scan also admits the shrinking later
	// batches. Rescan I/O must not be charged as additional storage footprint.
	entries, err := os.ReadDir(s.leafDir)
	if err != nil {
		t.Fatal(err)
	}
	var bytes int64
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		bytes += info.Size()
	}
	opts := LeafGenerationGCOptions{MaintenanceLimits: LeafGenerationMaintenanceLimits{NativeEntries: len(entries), NativeBytes: bytes, PagerPages: 1}}
	stats := LeafGenerationGCStats{}
	if err := s.gcRevisions(context.Background(), opts, &stats); err != nil {
		t.Fatal(err)
	}
	if stats.ManifestRevisionsTotal != revisions || stats.ManifestRevisionsProtected != 2 || stats.ManifestRevisionsDeleted != revisions-2 {
		t.Fatalf("apply census: %+v", stats)
	}
	if peak > baseline+20 {
		t.Fatalf("descriptor peak=%d baseline=%d", peak, baseline)
	}
	if remaining := revisionFiles5066(t, s.leafDir); len(remaining) != 2 {
		t.Fatalf("remaining=%v", remaining)
	}
	held.Release()
	s.hooks.BeforeRename = nil
	released := LeafGenerationGCStats{}
	if err := s.gcRevisions(context.Background(), LeafGenerationGCOptions{}, &released); err != nil || released.ManifestRevisionsDeleted != 1 {
		t.Fatalf("released held revision: stats=%+v err=%v", released, err)
	}
	t.Logf("RLIMIT_NOFILE=64 revisions=%d deleted=%d protected=%d descriptor_baseline=%d peak=%d remaining=1", revisions, stats.ManifestRevisionsDeleted, stats.ManifestRevisionsProtected, baseline, peak)
}

func revisionFDCount(t *testing.T) int {
	t.Helper()
	files, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(files)
}

// A completed deletion remains attributed if a later batch is canceled. A
// subsequent ordinary invocation must finish the same immutable inventory.
func TestLeafManifestRevisionGCPartialCancellation(t *testing.T) {
	s, old, current := newRevisionStore5066(t)
	old.Release()
	current.Release()
	for range 40 {
		token, err := s.Replace(newLeafGenerationManifest(1))
		if err != nil {
			t.Fatal(err)
		}
		token.Release()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	renames := 0
	s.hooks.BeforeRename = func() error {
		renames++
		if renames == 17 {
			cancel()
		}
		return nil
	}
	stats := LeafGenerationGCStats{}
	err := s.gcRevisions(ctx, LeafGenerationGCOptions{}, &stats)
	if !errors.Is(err, context.Canceled) || stats.ManifestRevisionsDeleted != 17 || stats.ManifestRevisionsEligible != 41 {
		t.Fatalf("partial cancellation: stats=%+v err=%v", stats, err)
	}
	s.hooks.BeforeRename = nil
	rest := LeafGenerationGCStats{}
	if err := s.gcRevisions(context.Background(), LeafGenerationGCOptions{}, &rest); err != nil || rest.ManifestRevisionsDeleted != 24 {
		t.Fatalf("remaining deletion: stats=%+v err=%v", rest, err)
	}
}

// A newly malformed entry after a completed batch prevents every later unlink;
// the already durable prefix remains truthfully counted and retryable.
func TestLeafManifestRevisionGCLaterInventoryFailure(t *testing.T) {
	s, old, current := newRevisionStore5066(t)
	old.Release()
	current.Release()
	for range 40 {
		token, err := s.Replace(newLeafGenerationManifest(1))
		if err != nil {
			t.Fatal(err)
		}
		token.Release()
	}
	renames := 0
	bad := filepath.Join(s.leafDir, "manifest.durable.bad.json")
	s.hooks.BeforeRename = func() error {
		renames++
		if renames == 16 {
			return os.WriteFile(bad, []byte("{}"), 0600)
		}
		return nil
	}
	stats := LeafGenerationGCStats{}
	if err := s.gcRevisions(context.Background(), LeafGenerationGCOptions{}, &stats); err == nil || stats.ManifestRevisionsDeleted != 16 || stats.ManifestRevisionsEligible != 41 {
		t.Fatalf("later invalid inventory: stats=%+v err=%v", stats, err)
	}
	s.hooks.BeforeRename = nil
	if err := os.Remove(bad); err != nil {
		t.Fatal(err)
	}
	rest := LeafGenerationGCStats{}
	if err := s.gcRevisions(context.Background(), LeafGenerationGCOptions{}, &rest); err != nil || rest.ManifestRevisionsDeleted != 25 {
		t.Fatalf("remaining deletion: stats=%+v err=%v", rest, err)
	}
}
