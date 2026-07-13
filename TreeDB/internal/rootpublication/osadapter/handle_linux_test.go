//go:build linux

package osadapter

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type hookRecorder struct {
	mu       sync.Mutex
	pins     int
	releases int
	flushes  []uint64
}

func (r *hookRecorder) resourceHooks() ResourceHooks {
	return ResourceHooks{
		FlushThrough: func(_ *os.File, frontier uint64) error {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.flushes = append(r.flushes, frontier)
			return nil
		},
		Pin: func(rootpublication.StableIdentity) error {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.pins++
			return nil
		},
		Release: func(rootpublication.StableIdentity) error {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.releases++
			return nil
		},
	}
}

func (r *hookRecorder) namespaceHooks() NamespaceHooks {
	return NamespaceHooks{
		Pin: func(rootpublication.StableIdentity) error {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.pins++
			return nil
		},
		Release: func(rootpublication.StableIdentity) error {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.releases++
			return nil
		},
	}
}

func (r *hookRecorder) counts() (pins, releases int, flushes []uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pins, r.releases, append([]uint64(nil), r.flushes...)
}

func createResourceFile(t *testing.T, path string, size int) *os.File {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(int64(size)); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return file
}

func TestResourceHandleStableTokenLifecycle(t *testing.T) {
	file := createResourceFile(t, filepath.Join(t.TempDir(), "000001.vlog"), 128)
	recorder := new(hookRecorder)
	token, err := RegisterResourceToken(file, recorder.resourceHooks(), rootpublication.StableResourceSpec{
		Kind:              rootpublication.ResourceValueLogSegment,
		LogicalNamespace:  "value-log",
		ResourceID:        "1",
		DiagnosticPath:    "maindb/value_vlog/000001.vlog",
		Generation:        1,
		RequiredFrontier:  96,
		ReachabilityField: "ValuePtr",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := token.FlushThrough(); err != nil {
		t.Fatal(err)
	}
	if err := token.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if err := token.Release(); err != nil {
		t.Fatal(err)
	}
	if err := token.Release(); err != nil {
		t.Fatalf("token release should be idempotent: %v", err)
	}
	pins, releases, flushes := recorder.counts()
	if pins != 1 || releases != 1 {
		t.Fatalf("pin/release calls = %d/%d, want 1/1", pins, releases)
	}
	if len(flushes) != 1 || flushes[0] != 96 {
		t.Fatalf("flush frontiers = %v, want [96]", flushes)
	}
}

func TestResourceHandleFrontierFailsClosed(t *testing.T) {
	file := createResourceFile(t, filepath.Join(t.TempDir(), "segment"), 64)
	recorder := new(hookRecorder)
	handle, err := NewResourceHandle(file, recorder.resourceHooks())
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Pin(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handle.Release() })

	if err := handle.SyncThrough(32); !errors.Is(err, ErrFrontierNotFlushed) {
		t.Fatalf("sync before flush error = %v, want ErrFrontierNotFlushed", err)
	}
	if err := handle.FlushThrough(0); !errors.Is(err, rootpublication.ErrInvalidStableResource) {
		t.Fatalf("zero flush error = %v, want ErrInvalidStableResource", err)
	}
	if err := handle.FlushThrough(65); !errors.Is(err, rootpublication.ErrResourceFrontierBeyondLength) {
		t.Fatalf("oversize flush error = %v, want ErrResourceFrontierBeyondLength", err)
	}
	_, _, flushes := recorder.counts()
	if len(flushes) != 0 {
		t.Fatalf("oversize frontier reached producer hook: %v", flushes)
	}
	if err := handle.FlushThrough(64); err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(32); err != nil {
		t.Fatal(err)
	}
	if err := handle.SyncThrough(64); !errors.Is(err, rootpublication.ErrResourceFrontierBeyondLength) {
		t.Fatalf("truncated sync error = %v, want ErrResourceFrontierBeyondLength", err)
	}
}

func TestResourceHandleDetectsPostFlushTruncation(t *testing.T) {
	file := createResourceFile(t, filepath.Join(t.TempDir(), "segment"), 64)
	hooks := ResourceHooks{
		FlushThrough: func(file *os.File, _ uint64) error { return file.Truncate(16) },
		Pin:          func(rootpublication.StableIdentity) error { return nil },
		Release:      func(rootpublication.StableIdentity) error { return nil },
	}
	handle, err := NewResourceHandle(file, hooks)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Pin(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handle.Release() })
	if err := handle.FlushThrough(48); !errors.Is(err, rootpublication.ErrResourceFrontierBeyondLength) {
		t.Fatalf("post-flush truncation error = %v, want ErrResourceFrontierBeyondLength", err)
	}
	if err := handle.SyncThrough(16); !errors.Is(err, ErrFrontierNotFlushed) {
		t.Fatalf("failed flush advanced frontier: %v", err)
	}
}

func TestResourceHandleRenameRecreateSyncsCapturedInode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "segment")
	archived := filepath.Join(dir, "segment.old")
	file := createResourceFile(t, path, 64)
	recorder := new(hookRecorder)
	var flushed, synced rootpublication.StableIdentity
	hooks := recorder.resourceHooks()
	baseFlush := hooks.FlushThrough
	hooks.FlushThrough = func(open *os.File, frontier uint64) error {
		snapshot, err := inspectOpenHandle(open)
		if err != nil {
			return err
		}
		flushed = snapshot.identity
		return baseFlush(open, frontier)
	}
	handle, err := newResourceHandle(file, hooks, func(open *os.File) error {
		snapshot, err := inspectOpenHandle(open)
		if err != nil {
			return err
		}
		synced = snapshot.identity
		return syncOpenResource(open)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Pin(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handle.Release() })
	original, err := handle.StableIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, archived); err != nil {
		t.Fatal(err)
	}
	replacement := createResourceFile(t, path, 64)
	replacementSnapshot, err := inspectOpenHandle(replacement)
	if err != nil {
		t.Fatal(err)
	}
	if replacementSnapshot.identity == original {
		t.Fatal("replacement unexpectedly reused the still-live original inode")
	}
	// Both producer close and path replacement happened before the retained
	// obligation is flushed and synced.
	if err := handle.FlushThrough(64); err != nil {
		t.Fatal(err)
	}
	if err := handle.SyncThrough(64); err != nil {
		t.Fatal(err)
	}
	if flushed != original {
		t.Fatalf("flushed identity = %+v, want captured original %+v", flushed, original)
	}
	if synced != original {
		t.Fatalf("synced identity = %+v, want captured original %+v", synced, original)
	}
	if synced == replacementSnapshot.identity {
		t.Fatalf("sync followed replacement identity %+v", synced)
	}
}

func TestNamespaceHandleValidationDoesNotSyncAndTokenSyncsOnce(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = parent.Close() })
	recorder := new(hookRecorder)
	syncCalls := 0
	handle, err := newNamespaceHandle(parent, 7, recorder.namespaceHooks(), func(open *os.File) error {
		syncCalls++
		return syncOpenNamespace(open)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.ValidateNamespacePersistence(); err != nil {
		t.Fatal(err)
	}
	if syncCalls != 0 {
		t.Fatalf("non-mutating validation performed %d syncs", syncCalls)
	}
	child := createResourceFile(t, filepath.Join(dir, "segment"), 1)
	childHandle, err := NewResourceHandle(child, recorder.resourceHooks())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = childHandle.Close() })
	token, err := rootpublication.NewStableNamespaceToken(rootpublication.StableNamespaceSpec{
		Operation:            rootpublication.NamespaceCreate,
		ParentDiagnosticPath: "maindb/value_vlog",
		ParentGeneration:     7,
		Parent:               handle,
		TargetName:           "segment",
		Child:                childHandle,
	})
	if err != nil {
		t.Fatal(err)
	}
	if syncCalls != 0 {
		t.Fatalf("token registration performed %d syncs", syncCalls)
	}
	if err := token.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := token.Sync(); err != nil {
		t.Fatal(err)
	}
	if syncCalls != 1 {
		t.Fatalf("stable namespace token sync calls = %d, want 1", syncCalls)
	}
	if err := token.Release(); err != nil {
		t.Fatal(err)
	}
	if err := token.Release(); err != nil {
		t.Fatalf("token release should be idempotent: %v", err)
	}
	pins, releases, _ := recorder.counts()
	if pins != 1 || releases != 1 {
		t.Fatalf("pin/release calls = %d/%d, want 1/1", pins, releases)
	}
}

func TestNamespaceHandleRenameRecreateSyncsCapturedDirectory(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "parent")
	archived := filepath.Join(root, "parent.old")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	parent, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = parent.Close() })
	recorder := new(hookRecorder)
	var synced rootpublication.StableIdentity
	handle, err := newNamespaceHandle(parent, 1, recorder.namespaceHooks(), func(open *os.File) error {
		snapshot, err := inspectOpenHandle(open)
		if err != nil {
			return err
		}
		synced = snapshot.identity
		return syncOpenNamespace(open)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Pin(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handle.Release() })
	original, err := handle.StableIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if err := parent.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, archived); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	replacement, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replacement.Close() })
	replacementSnapshot, err := inspectOpenHandle(replacement)
	if err != nil {
		t.Fatal(err)
	}
	if replacementSnapshot.identity == original {
		t.Fatal("replacement unexpectedly reused the still-live original directory inode")
	}
	if err := handle.SyncNamespace(); err != nil {
		t.Fatal(err)
	}
	if synced != original || synced == replacementSnapshot.identity {
		t.Fatalf("synced identity = %+v, original=%+v replacement=%+v", synced, original, replacementSnapshot.identity)
	}
}

func TestAdaptersFailClosed(t *testing.T) {
	dir := t.TempDir()
	file := createResourceFile(t, filepath.Join(dir, "file"), 1)
	if _, err := NewResourceHandle(file, ResourceHooks{}); !errors.Is(err, ErrMissingHook) {
		t.Fatalf("resource without hooks error = %v, want ErrMissingHook", err)
	}
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = parent.Close() })
	recorder := new(hookRecorder)
	if _, err := NewResourceHandle(parent, recorder.resourceHooks()); !errors.Is(err, ErrInvalidOpenHandle) {
		t.Fatalf("directory resource error = %v, want ErrInvalidOpenHandle", err)
	}
	if _, err := NewNamespaceHandle(file, 1, recorder.namespaceHooks()); !errors.Is(err, ErrInvalidOpenHandle) {
		t.Fatalf("regular-file namespace error = %v, want ErrInvalidOpenHandle", err)
	}
	if _, err := NewNamespaceHandle(parent, 0, recorder.namespaceHooks()); !errors.Is(err, ErrInvalidOpenHandle) {
		t.Fatalf("zero namespace generation error = %v, want ErrInvalidOpenHandle", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := NewResourceHandle(file, recorder.resourceHooks()); !errors.Is(err, ErrInvalidOpenHandle) {
		t.Fatalf("closed resource error = %v, want ErrInvalidOpenHandle", err)
	}
}

func TestRegistrationFailureClosesRetainedDescriptors(t *testing.T) {
	file := createResourceFile(t, filepath.Join(t.TempDir(), "segment"), 64)
	recorder := new(hookRecorder)
	before := openDescriptorCount(t)
	if _, err := RegisterResourceToken(file, recorder.resourceHooks(), rootpublication.StableResourceSpec{
		Kind:              rootpublication.ResourceValueLogSegment,
		LogicalNamespace:  "value-log",
		ResourceID:        "1",
		DiagnosticPath:    "maindb/value_vlog/segment",
		Generation:        1,
		RequiredFrontier:  0,
		ReachabilityField: "ValuePtr",
	}); err == nil {
		t.Fatal("invalid resource registration succeeded")
	}
	after := openDescriptorCount(t)
	if after != before {
		t.Fatalf("resource registration leaked descriptors: before=%d after=%d", before, after)
	}
	pinFailure := errors.New("pin refused")
	hooks := recorder.resourceHooks()
	hooks.Pin = func(rootpublication.StableIdentity) error { return pinFailure }
	before = openDescriptorCount(t)
	if _, err := RegisterResourceToken(file, hooks, rootpublication.StableResourceSpec{
		Kind:              rootpublication.ResourceValueLogSegment,
		LogicalNamespace:  "value-log",
		ResourceID:        "1",
		DiagnosticPath:    "maindb/value_vlog/segment",
		Generation:        1,
		RequiredFrontier:  32,
		ReachabilityField: "ValuePtr",
	}); !errors.Is(err, pinFailure) {
		t.Fatalf("pin failure error = %v, want pin refusal", err)
	}
	after = openDescriptorCount(t)
	if after != before {
		t.Fatalf("pin failure leaked descriptors: before=%d after=%d", before, after)
	}

	parent, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = parent.Close() })
	child := createResourceFile(t, filepath.Join(t.TempDir(), "segment"), 1)
	before = openDescriptorCount(t)
	if _, err := RegisterNamespaceToken(parent, child, 3, recorder.namespaceHooks(), rootpublication.StableNamespaceSpec{
		Operation:            rootpublication.NamespaceCreate,
		ParentDiagnosticPath: "maindb",
		ParentGeneration:     4,
		TargetName:           "segment",
	}); err == nil {
		t.Fatal("generation-mismatched namespace registration succeeded")
	}
	after = openDescriptorCount(t)
	if after != before {
		t.Fatalf("namespace registration leaked descriptors: before=%d after=%d", before, after)
	}
}

func TestResourceHandleSharedPinsCloseOnlyAfterFinalRelease(t *testing.T) {
	file := createResourceFile(t, filepath.Join(t.TempDir(), "segment"), 64)
	recorder := new(hookRecorder)
	handle, err := NewResourceHandle(file, recorder.resourceHooks())
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Pin(); err != nil {
		t.Fatal(err)
	}
	if err := handle.Pin(); err != nil {
		t.Fatal(err)
	}
	if err := handle.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.StableIdentity(); err != nil {
		t.Fatalf("first release closed descriptor with a remaining pin: %v", err)
	}
	if err := handle.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.StableIdentity(); !errors.Is(err, ErrHandleClosed) {
		t.Fatalf("final release identity error = %v, want ErrHandleClosed", err)
	}
	pins, releases, _ := recorder.counts()
	if pins != 2 || releases != 2 {
		t.Fatalf("pin/release calls = %d/%d, want 2/2", pins, releases)
	}
}

func TestResourceHandleFailedReleaseIsNotRetried(t *testing.T) {
	file := createResourceFile(t, filepath.Join(t.TempDir(), "segment"), 64)
	releaseFailure := errors.New("release failed")
	var releaseCalls int
	hooks := ResourceHooks{
		FlushThrough: func(*os.File, uint64) error { return nil },
		Pin:          func(rootpublication.StableIdentity) error { return nil },
		Release: func(rootpublication.StableIdentity) error {
			releaseCalls++
			return releaseFailure
		},
	}
	handle, err := NewResourceHandle(file, hooks)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Pin(); err != nil {
		t.Fatal(err)
	}
	if err := handle.Release(); !errors.Is(err, releaseFailure) {
		t.Fatalf("release error = %v, want release failure", err)
	}
	if releaseCalls != 1 {
		t.Fatalf("release hook calls = %d, want 1", releaseCalls)
	}
	if err := handle.Release(); !errors.Is(err, ErrUnbalancedRelease) {
		t.Fatalf("retry error = %v, want ErrUnbalancedRelease", err)
	}
	if releaseCalls != 1 {
		t.Fatalf("release hook retried: calls=%d", releaseCalls)
	}
	if _, err := handle.StableIdentity(); !errors.Is(err, ErrHandleClosed) {
		t.Fatalf("failed final release leaked descriptor: %v", err)
	}
}

func openDescriptorCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

func TestResourceHandleConcurrentPinsAndOperations(t *testing.T) {
	file := createResourceFile(t, filepath.Join(t.TempDir(), "segment"), 128)
	recorder := new(hookRecorder)
	handle, err := NewResourceHandle(file, recorder.resourceHooks())
	if err != nil {
		t.Fatal(err)
	}
	const goroutines = 32
	// Keep one guard pin so a fast goroutine cannot close the retained
	// descriptor before a slower goroutine acquires its pin.
	if err := handle.Pin(); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := handle.Pin(); err != nil {
				errCh <- err
				return
			}
			if err := handle.FlushThrough(96); err != nil {
				errCh <- err
			}
			if err := handle.SyncThrough(96); err != nil {
				errCh <- err
			}
			if err := handle.Release(); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if err := handle.Release(); err != nil {
		t.Fatal(err)
	}
	pins, releases, flushes := recorder.counts()
	if pins != goroutines+1 || releases != goroutines+1 || len(flushes) != goroutines {
		t.Fatalf("pins/releases/flushes = %d/%d/%d, want %d/%d/%d", pins, releases, len(flushes), goroutines+1, goroutines+1, goroutines)
	}
	if err := handle.Release(); !errors.Is(err, ErrUnbalancedRelease) {
		t.Fatalf("extra release error = %v, want ErrUnbalancedRelease", err)
	}
}
