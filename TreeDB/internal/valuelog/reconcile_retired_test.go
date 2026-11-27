package valuelog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestManagerReconcileRetiredSegments(t *testing.T) {
	t.Run("retired_absent_close", func(t *testing.T) {
		dir := t.TempDir()
		id, path := writeIdentityPinTestSegment(t, dir)
		registry := rootpublication.NewIdentityPinRegistry()

		first, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := first.Close(); err != nil {
				t.Error(err)
			}
		})

		second, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := second.Close(); err != nil {
				t.Error(err)
			}
		})

		second.mu.RLock()
		original := second.files[id]
		second.mu.RUnlock()
		if original == nil {
			t.Fatalf("second manager did not discover segment %d", id)
		}
		if original.retirementParent != nil || original.retirementIdentity != (rootpublication.StableIdentity{}) {
			t.Fatalf("expected unprepared retirement parent/identity on secondary manager")
		}
		identity := retirementIdentityAtPath(t, path)

		removed, err := first.RemoveSegmentIfUnpinned(id)
		if err != nil || !removed {
			t.Fatalf("first.RemoveSegmentIfUnpinned(%d): removed=%t err=%v", id, removed, err)
		}
		if !registry.IsRetired(identity) {
			t.Fatalf("registry did not record segment %d as retired", id)
		}

		if err := second.ReconcileRetiredSegments(); err != nil {
			t.Fatalf("second.ReconcileRetiredSegments: %v", err)
		}

		second.mu.RLock()
		_, stillTracked := second.files[id]
		retiredCount := second.retiredCount
		second.mu.RUnlock()
		if stillTracked {
			t.Fatalf("second manager still tracks retired absent segment %d", id)
		}
		if retiredCount != 0 {
			t.Fatalf("second.retiredCount=%d, want 0", retiredCount)
		}
		if !original.closed.Load() {
			t.Fatalf("retired original file handle was not closed")
		}
		if _, err := original.File.Stat(); err == nil {
			t.Fatalf("retired original os.File handle is still open")
		}
		if got := registry.ObserverCount(identity); got != 0 {
			t.Fatalf("registry observer count=%d, want 0", got)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("reconciliation recreated absent path: %v", err)
		}
	})

	t.Run("pinned_deferral_and_post_release_cleanup", func(t *testing.T) {
		dir := t.TempDir()
		id, path := writeIdentityPinTestSegment(t, dir)
		registry := rootpublication.NewIdentityPinRegistry()

		first, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := first.Close(); err != nil {
				t.Error(err)
			}
		})

		second, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := second.Close(); err != nil {
				t.Error(err)
			}
		})

		identity := retirementIdentityAtPath(t, path)
		set := second.CurrentSetNoRefresh()
		original := set.Files[id]
		if original == nil || original.RefCount.Load() != 1 {
			t.Fatalf("expected pinned file with RefCount=1")
		}

		removed, err := first.RemoveSegmentIfUnpinned(id)
		if err != nil || !removed {
			_ = second.Release(set)
			t.Fatalf("first.RemoveSegmentIfUnpinned(%d): removed=%t err=%v", id, removed, err)
		}

		// While RefCount > 0, ReconcileRetiredSegments leaves file intact without error.
		if err := second.ReconcileRetiredSegments(); err != nil {
			_ = second.Release(set)
			t.Fatalf("second.ReconcileRetiredSegments while pinned: %v", err)
		}
		second.mu.RLock()
		tracked := second.files[id]
		second.mu.RUnlock()
		if tracked != original || original.closed.Load() || original.IsZombie.Load() {
			_ = second.Release(set)
			t.Fatalf("ReconcileRetiredSegments disturbed pinned retired segment")
		}
		if _, err := original.File.Stat(); err != nil {
			_ = second.Release(set)
			t.Fatalf("pinned retired segment handle closed prematurely: %v", err)
		}

		// Release drops RefCount to 0 without doing extra registry queries on non-zombie reads.
		if err := second.Release(set); err != nil {
			t.Fatalf("second.Release: %v", err)
		}
		if err := second.ReconcileRetiredSegments(); err != nil {
			t.Fatalf("second.ReconcileRetiredSegments after Release: %v", err)
		}
		second.mu.RLock()
		_, stillTracked := second.files[id]
		second.mu.RUnlock()
		if stillTracked || !original.closed.Load() || registry.ObserverCount(identity) != 0 {
			t.Fatalf("post-Release ReconcileRetiredSegments failed: stillTracked=%t closed=%t observers=%d",
				stillTracked, original.closed.Load(), registry.ObserverCount(identity))
		}
	})

	t.Run("replacement_child_conflict_fail_closed", func(t *testing.T) {
		dir := t.TempDir()
		id, path := writeIdentityPinTestSegment(t, dir)
		registry := rootpublication.NewIdentityPinRegistry()

		first, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := first.Close(); err != nil {
				t.Error(err)
			}
		})

		second, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := second.Close(); err != nil {
				t.Error(err)
			}
		})

		second.mu.RLock()
		original := second.files[id]
		second.mu.RUnlock()
		identity := retirementIdentityAtPath(t, path)

		preparedDir := t.TempDir()
		writeTestSegment(t, preparedDir, 1, 1, 2, []byte("replacement-successor"))
		preparedPath := filepath.Join(preparedDir, filepath.Base(path))
		preparedIdentity := retirementIdentityAtPath(t, preparedPath)
		want, err := os.ReadFile(preparedPath)
		if err != nil {
			t.Fatal(err)
		}

		if removed, err := first.RemoveSegmentIfUnpinned(id); err != nil || !removed {
			t.Fatalf("first.RemoveSegmentIfUnpinned: removed=%t err=%v", removed, err)
		}
		if err := os.Rename(preparedPath, path); err != nil {
			t.Fatal(err)
		}

		if err := second.ReconcileRetiredSegments(); !errors.Is(err, rootpublication.ErrResourceConflict) {
			t.Fatalf("ReconcileRetiredSegments with replacement child = %v, want ErrResourceConflict", err)
		}
		second.mu.RLock()
		tracked := second.files[id]
		second.mu.RUnlock()
		if tracked != original || original.closed.Load() || registry.ObserverCount(identity) != 1 {
			t.Fatalf("replacement conflict dropped or closed stale owner prematurely")
		}
		got, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(got, want) || !rootpublication.SamePhysicalIdentity(preparedIdentity, retirementIdentityAtPath(t, path)) {
			t.Fatalf("replacement child was modified: err=%v", err)
		}
	})

	t.Run("closing_returns_nil", func(t *testing.T) {
		var nilManager *Manager
		if err := nilManager.ReconcileRetiredSegments(); err != nil {
			t.Fatalf("nil.ReconcileRetiredSegments = %v, want nil", err)
		}
		dir := t.TempDir()
		_, _ = writeIdentityPinTestSegment(t, dir)
		registry := rootpublication.NewIdentityPinRegistry()
		m, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}
		if err := m.ReconcileRetiredSegments(); err != nil {
			t.Fatalf("closed manager ReconcileRetiredSegments = %v, want nil", err)
		}
	})

	t.Run("failed_close_accounting_preserved", func(t *testing.T) {
		dir := t.TempDir()
		id, path := writeIdentityPinTestSegment(t, dir)
		registry := rootpublication.NewIdentityPinRegistry()

		first, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := first.Close(); err != nil {
				t.Error(err)
			}
		})

		second, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		second.mu.RLock()
		original := second.files[id]
		second.mu.RUnlock()
		identity := retirementIdentityAtPath(t, path)

		if removed, err := first.RemoveSegmentIfUnpinned(id); err != nil || !removed {
			t.Fatalf("first.RemoveSegmentIfUnpinned: removed=%t err=%v", removed, err)
		}
		// Pre-close the underlying os.File so original.Close() fails with os.ErrClosed.
		if err := original.File.Close(); err != nil {
			t.Fatalf("pre-close original os.File: %v", err)
		}
		err = second.ReconcileRetiredSegments()
		var receipt *ManagerCleanupFailure
		if !errors.Is(err, os.ErrClosed) || !errors.As(err, &receipt) {
			t.Fatalf("expected ManagerCleanupFailure wrapping os.ErrClosed, got %v", err)
		}
		if got := registry.ObserverCount(identity); got != 0 {
			t.Fatalf("registry observer count=%d, want 0", got)
		}
		closeErr := second.Close()
		if !errors.Is(closeErr, os.ErrClosed) {
			t.Fatalf("second.Close() dropped failed-close accounting: got %v, want os.ErrClosed", closeErr)
		}
	})

	t.Run("on_segment_retired_callback_deferred_release_and_direct_delete", func(t *testing.T) {
		dir := t.TempDir()
		id, path := writeIdentityPinTestSegment(t, dir)
		registry := rootpublication.NewIdentityPinRegistry()

		first, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := first.Close(); err != nil {
				t.Error(err)
			}
		})

		second, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := second.Close(); err != nil {
				t.Error(err)
			}
		})

		identity := retirementIdentityAtPath(t, path)
		reconcileErrCh := make(chan error, 1)
		first.SetOnSegmentRetired(func(fileID uint32) {
			reconcileErrCh <- second.ReconcileRetiredSegment(fileID)
		})

		// Simulate indexVacuumLeafGuard / active snapshot pinning the segment across
		// MarkZombie, then releasing it later.
		held := first.CurrentSetNoRefresh()
		if err := first.MarkZombie(id); err != nil {
			_ = first.Release(held)
			t.Fatal(err)
		}
		// While held, second still observes it and ReconcileRetiredSegment is a no-op.
		if err := second.ReconcileRetiredSegment(id); err != nil {
			_ = first.Release(held)
			t.Fatalf("ReconcileRetiredSegment before deletion: %v", err)
		}
		if got := registry.ObserverCount(identity); got != 2 {
			_ = first.Release(held)
			t.Fatalf("expected 2 observers while pinned, got %d", got)
		}

		// Releasing held deletes the zombie and invokes SetOnSegmentRetired -> second reconciles.
		if err := first.Release(held); err != nil {
			t.Fatalf("first.Release(held): %v", err)
		}
		select {
		case err := <-reconcileErrCh:
			if err != nil {
				t.Fatalf("secondary ReconcileRetiredSegment: %v", err)
			}
		default:
			t.Fatal("expected SetOnSegmentRetired callback to fire before first.Release returned")
		}
		second.mu.RLock()
		_, stillTracked := second.files[id]
		second.mu.RUnlock()
		if stillTracked {
			t.Fatalf("second manager still tracks segment %d after deferred zombie release", id)
		}
		if got := registry.ObserverCount(identity); got != 0 {
			t.Fatalf("registry observer count=%d, want 0 after deferred zombie release", got)
		}
	})

	t.Run("on_segment_retired_does_not_corrupt_primary_release_on_secondary_failure", func(t *testing.T) {
		dir := t.TempDir()
		id, path := writeIdentityPinTestSegment(t, dir)
		registry := rootpublication.NewIdentityPinRegistry()

		first, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := first.Close(); err != nil {
				t.Error(err)
			}
		})

		second, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		second.mu.RLock()
		original := second.files[id]
		second.mu.RUnlock()
		identity := retirementIdentityAtPath(t, path)

		// Pre-close secondary handle so secondary reconciliation fails with os.ErrClosed.
		if err := original.File.Close(); err != nil {
			t.Fatalf("pre-close secondary handle: %v", err)
		}

		notified := make(chan uint32, 1)
		first.SetOnSegmentRetired(func(fileID uint32) {
			select {
			case notified <- fileID:
			default:
			}
		})

		held := first.CurrentSetNoRefresh()
		if err := first.MarkZombie(id); err != nil {
			_ = first.Release(held)
			t.Fatal(err)
		}
		// Primary Release MUST succeed cleanly without inheriting secondary failures.
		if err := first.Release(held); err != nil {
			t.Fatalf("first.Release(held) failed: %v", err)
		}
		if err := first.CleanupError(); err != nil {
			t.Fatalf("first.CleanupError() = %v, want nil", err)
		}
		select {
		case got := <-notified:
			if got != id {
				t.Fatalf("notified fileID=%d, want %d", got, id)
			}
		default:
			t.Fatalf("expected SetOnSegmentRetired(%d) to fire before first.Release returned", id)
		}

		// Secondary reconcile surfaces ManagerCleanupFailure and preserves CleanupError.
		err = second.ReconcileRetiredSegment(id)
		var receipt *ManagerCleanupFailure
		if !errors.Is(err, os.ErrClosed) || !errors.As(err, &receipt) {
			t.Fatalf("secondary ReconcileRetiredSegment = %v, want ManagerCleanupFailure(os.ErrClosed)", err)
		}
		if !errors.Is(second.CleanupError(), os.ErrClosed) {
			t.Fatalf("second.CleanupError() = %v, want os.ErrClosed", second.CleanupError())
		}
		if got := registry.ObserverCount(identity); got != 0 {
			t.Fatalf("registry observer count=%d, want 0", got)
		}
		if !errors.Is(second.Close(), os.ErrClosed) {
			t.Fatalf("second.Close() dropped secondary cleanup receipt")
		}
	})

	t.Run("fileFor_discovers_live_segment_despite_unrelated_retired_conflict", func(t *testing.T) {
		dir := t.TempDir()
		idA, pathA := writeIdentityPinTestSegment(t, dir)
		registry := rootpublication.NewIdentityPinRegistry()

		first, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := first.Close(); err != nil {
				t.Error(err)
			}
		})

		second, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := second.Close(); err != nil {
				t.Error(err)
			}
		})

		if removed, err := first.RemoveSegmentIfUnpinned(idA); err != nil || !removed {
			t.Fatalf("first.RemoveSegmentIfUnpinned(%d): removed=%t err=%v", idA, removed, err)
		}
		// Put a directory at retired pathA so live discovery skips it while
		// ReconcileRetiredSegments fails closed on pathA.
		if err := os.Mkdir(pathA, 0o700); err != nil {
			t.Fatalf("Mkdir(%s): %v", pathA, err)
		}
		// Create a brand new live segment B on disk not yet known to second.
		idB := writeTestSegment(t, dir, 0, 2, 99, []byte("live-segment-b"))

		// Lazy lookup via fileFor(idB) must discover and serve live segment B.
		fB, err := second.fileFor(idB)
		if err != nil || fB == nil || fB.ID != idB {
			t.Fatalf("second.fileFor(%d) = (%v, %v), want live segment B", idB, fB, err)
		}
		// Explicit Refresh() must still surface the reconciliation refusal on retired pathA.
		if err := second.Refresh(); err == nil {
			t.Fatalf("second.Refresh() = nil, want conflict error for directory at retired pathA")
		}
	})

	t.Run("pinned_borrow_release_notifies_reconciler_immediately", func(t *testing.T) {
		dir := t.TempDir()
		id, path := writeIdentityPinTestSegment(t, dir)
		registry := rootpublication.NewIdentityPinRegistry()

		first, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := first.Close(); err != nil {
				t.Error(err)
			}
		})

		second, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := second.Close(); err != nil {
				t.Error(err)
			}
		})

		releasedCh := make(chan uint32, 1)
		second.SetOnRetiredBorrowReleased(func(fileID uint32) {
			select {
			case releasedCh <- fileID:
			default:
			}
		})

		identity := retirementIdentityAtPath(t, path)
		set := second.CurrentSetNoRefresh()
		if removed, err := first.RemoveSegmentIfUnpinned(id); err != nil || !removed {
			_ = second.Release(set)
			t.Fatalf("first.RemoveSegmentIfUnpinned(%d): removed=%t err=%v", id, removed, err)
		}

		// Reconciling while pinned leaves file intact and marks pendingRetiredReconcile.
		if err := second.ReconcileRetiredSegments(); err != nil {
			_ = second.Release(set)
			t.Fatalf("ReconcileRetiredSegments while pinned: %v", err)
		}
		if got := registry.ObserverCount(identity); got != 1 {
			_ = second.Release(set)
			t.Fatalf("observer count while pinned=%d, want 1", got)
		}

		// Releasing the last borrow triggers onRetiredBorrowReleased immediately.
		if err := second.Release(set); err != nil {
			t.Fatalf("second.Release(set): %v", err)
		}
		select {
		case gotID := <-releasedCh:
			if gotID != id {
				t.Fatalf("onRetiredBorrowReleased got fileID=%d, want %d", gotID, id)
			}
		default:
			t.Fatal("onRetiredBorrowReleased did not fire when last pinned retired borrow released")
		}
		if err := second.ReconcileRetiredSegment(id); err != nil {
			t.Fatalf("post-release ReconcileRetiredSegment(%d): %v", id, err)
		}
		if got := registry.ObserverCount(identity); got != 0 {
			t.Fatalf("observer count after post-release reconcile=%d, want 0", got)
		}
	})
}

func TestManagerReconcileRetiredSegmentsViaRefresh(t *testing.T) {
	t.Run("retired_absent_close", func(t *testing.T) {
		dir := t.TempDir()
		id, path := writeIdentityPinTestSegment(t, dir)
		registry := rootpublication.NewIdentityPinRegistry()

		first, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := first.Close(); err != nil {
				t.Error(err)
			}
		})

		second, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := second.Close(); err != nil {
				t.Error(err)
			}
		})

		second.mu.RLock()
		original := second.files[id]
		second.mu.RUnlock()
		if original == nil {
			t.Fatalf("second manager did not discover segment %d", id)
		}
		identity := retirementIdentityAtPath(t, path)

		removed, err := first.RemoveSegmentIfUnpinned(id)
		if err != nil || !removed {
			t.Fatalf("first.RemoveSegmentIfUnpinned(%d): removed=%t err=%v", id, removed, err)
		}
		if !registry.IsRetired(identity) {
			t.Fatalf("registry did not record segment %d as retired", id)
		}

		if err := second.Refresh(); err != nil {
			t.Fatalf("second.Refresh: %v", err)
		}

		second.mu.RLock()
		_, stillTracked := second.files[id]
		second.mu.RUnlock()
		if stillTracked {
			t.Fatalf("second manager still tracks retired absent segment %d after Refresh (observerCount=%d, closed=%t)",
				id, registry.ObserverCount(identity), original.closed.Load())
		}
		if !original.closed.Load() {
			t.Fatalf("retired original file handle was not closed")
		}
		if _, err := original.File.Stat(); err == nil {
			t.Fatalf("retired original os.File handle is still open")
		}
		if got := registry.ObserverCount(identity); got != 0 {
			t.Fatalf("registry observer count=%d, want 0 after reconciliation", got)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("reconciliation recreated absent path: %v", err)
		}
	})

	t.Run("pinned_deferral_and_post_release_cleanup", func(t *testing.T) {
		dir := t.TempDir()
		id, path := writeIdentityPinTestSegment(t, dir)
		registry := rootpublication.NewIdentityPinRegistry()

		first, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := first.Close(); err != nil {
				t.Error(err)
			}
		})

		second, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := second.Close(); err != nil {
				t.Error(err)
			}
		})

		identity := retirementIdentityAtPath(t, path)
		set := second.CurrentSetNoRefresh()
		original := set.Files[id]
		if original == nil || original.RefCount.Load() != 1 {
			t.Fatalf("expected pinned file with RefCount=1")
		}

		removed, err := first.RemoveSegmentIfUnpinned(id)
		if err != nil || !removed {
			_ = second.Release(set)
			t.Fatalf("first.RemoveSegmentIfUnpinned(%d): removed=%t err=%v", id, removed, err)
		}

		// While RefCount > 0, Refresh must leave file intact without error.
		if err := second.Refresh(); err != nil {
			_ = second.Release(set)
			t.Fatalf("second.Refresh while pinned: %v", err)
		}
		second.mu.RLock()
		tracked := second.files[id]
		second.mu.RUnlock()
		if tracked != original || original.closed.Load() {
			_ = second.Release(set)
			t.Fatalf("Refresh disturbed pinned retired segment: tracked=%v closed=%t", tracked == original, original.closed.Load())
		}
		if _, err := original.File.Stat(); err != nil {
			_ = second.Release(set)
			t.Fatalf("pinned retired segment handle closed prematurely: %v", err)
		}

		if err := second.Release(set); err != nil {
			t.Fatalf("second.Release: %v", err)
		}
		if err := second.Refresh(); err != nil {
			t.Fatalf("second.Refresh after Release: %v", err)
		}
		second.mu.RLock()
		_, stillTracked := second.files[id]
		second.mu.RUnlock()
		if stillTracked || !original.closed.Load() || registry.ObserverCount(identity) != 0 {
			t.Fatalf("post-Release Refresh failed to close retired segment: stillTracked=%t closed=%t observers=%d",
				stillTracked, original.closed.Load(), registry.ObserverCount(identity))
		}
	})

	t.Run("replacement_child_conflict_fail_closed", func(t *testing.T) {
		dir := t.TempDir()
		id, path := writeIdentityPinTestSegment(t, dir)
		registry := rootpublication.NewIdentityPinRegistry()

		first, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := first.Close(); err != nil {
				t.Error(err)
			}
		})

		second, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := second.Close(); err != nil {
				t.Error(err)
			}
		})

		second.mu.RLock()
		original := second.files[id]
		second.mu.RUnlock()
		identity := retirementIdentityAtPath(t, path)

		preparedDir := t.TempDir()
		writeTestSegment(t, preparedDir, 1, 1, 2, []byte("replacement-successor"))
		preparedPath := filepath.Join(preparedDir, filepath.Base(path))
		preparedIdentity := retirementIdentityAtPath(t, preparedPath)
		want, err := os.ReadFile(preparedPath)
		if err != nil {
			t.Fatal(err)
		}

		if removed, err := first.RemoveSegmentIfUnpinned(id); err != nil || !removed {
			t.Fatalf("first.RemoveSegmentIfUnpinned: removed=%t err=%v", removed, err)
		}
		if err := os.Rename(preparedPath, path); err != nil {
			t.Fatal(err)
		}

		if err := second.Refresh(); !errors.Is(err, rootpublication.ErrResourceConflict) {
			t.Fatalf("Refresh with replacement child = %v, want ErrResourceConflict", err)
		}
		second.mu.RLock()
		tracked := second.files[id]
		second.mu.RUnlock()
		if tracked != original || original.closed.Load() || registry.ObserverCount(identity) != 1 {
			t.Fatalf("replacement conflict dropped or closed stale owner prematurely")
		}
		got, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(got, want) || !rootpublication.SamePhysicalIdentity(preparedIdentity, retirementIdentityAtPath(t, path)) {
			t.Fatalf("replacement child was modified: err=%v", err)
		}
	})
}

func BenchmarkManagerReconcileRetiredSteadyState(b *testing.B) {
	dir := b.TempDir()
	for seq := uint32(1); seq <= 8; seq++ {
		id, err := EncodeFileID(0, seq)
		if err != nil {
			b.Fatal(err)
		}
		w, err := NewWriter(SegmentPath(dir, id), id)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := w.Append(0, nil, uint64(seq), []byte("steady-state-value")); err != nil {
			_ = w.Close()
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
	registry := rootpublication.NewIdentityPinRegistry()
	m, err := NewManagerWithStableResourcePinRegistry(dir, registry)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := m.Close(); err != nil {
			b.Fatal(err)
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := m.ReconcileRetiredSegments(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestManagerReconcileRetiredSegments_ConcurrentReadAndReconcile(t *testing.T) {
	t.Run("borrowed_read_blocks_reconcile_and_notifies_on_release", func(t *testing.T) {
		dir := t.TempDir()
		id, err := EncodeFileID(0, 1)
		if err != nil {
			t.Fatal(err)
		}
		path := SegmentPath(dir, id)
		wantVal := bytes.Repeat([]byte("borrowed-mmap-payload-"), 64)
		w, err := NewWriter(path, id)
		if err != nil {
			t.Fatal(err)
		}
		ptr, err := w.Append(0, nil, 101, wantVal)
		if err != nil {
			_ = w.Close()
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		registry := rootpublication.NewIdentityPinRegistry()
		first, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := first.Close(); err != nil {
				t.Error(err)
			}
		})
		second, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := second.Close(); err != nil {
				t.Error(err)
			}
		})

		// Warm mmap on second via normal read before borrowing.
		got, err := second.Read(ptr)
		if err != nil {
			t.Fatalf("warm second.Read: %v", err)
		}
		if !bytes.Equal(got, wantVal) {
			t.Fatalf("warm second.Read mismatch")
		}

		notified := make(chan uint32, 1)
		second.SetOnRetiredBorrowReleased(func(fileID uint32) {
			select {
			case notified <- fileID:
			default:
			}
		})

		identity := retirementIdentityAtPath(t, path)
		borrowed, err := second.borrowFileFor(id)
		if err != nil {
			t.Fatalf("borrowFileFor: %v", err)
		}
		if err := first.RemoveSegment(id); err != nil {
			second.releaseFileBorrow(borrowed)
			t.Fatalf("first.RemoveSegment: %v", err)
		}

		// Reconcile while borrow is held MUST NOT close or unmap borrowed.
		if err := second.ReconcileRetiredSegments(); err != nil {
			second.releaseFileBorrow(borrowed)
			t.Fatalf("ReconcileRetiredSegments while borrowed: %v", err)
		}
		if borrowed.closed.Load() {
			second.releaseFileBorrow(borrowed)
			t.Fatal("borrowed file was closed while read borrow was active")
		}
		gotWhilePinned, err := borrowed.Read(ptr, true)
		if err != nil || !bytes.Equal(gotWhilePinned, wantVal) {
			second.releaseFileBorrow(borrowed)
			t.Fatalf("borrowed.Read while retired = (%d bytes, %v), want exact payload", len(gotWhilePinned), err)
		}

		// Releasing the read borrow must immediately fire onRetiredBorrowReleased.
		second.releaseFileBorrow(borrowed)
		select {
		case firedID := <-notified:
			if firedID != id {
				t.Fatalf("onRetiredBorrowReleased fired id=%d, want %d", firedID, id)
			}
		default:
			t.Fatal("expected onRetiredBorrowReleased to fire immediately when last read borrow released")
		}
		if err := second.ReconcileRetiredSegment(id); err != nil {
			t.Fatalf("ReconcileRetiredSegment after release: %v", err)
		}
		if !borrowed.closed.Load() {
			t.Fatal("expected retired file to close after read borrow released and reconciled")
		}
		if got := registry.ObserverCount(identity); got != 0 {
			t.Fatalf("ObserverCount=%d, want 0", got)
		}
	})

	t.Run("concurrent_reads_during_retirement_and_reconcile", func(t *testing.T) {
		dir := t.TempDir()
		const segCount = 24
		type segSpec struct {
			id       uint32
			path     string
			ptr      page.ValuePtr
			rid      uint64
			val      []byte
			identity rootpublication.StableIdentity
		}
		segs := make([]segSpec, 0, segCount)
		for i := 0; i < segCount; i++ {
			id, err := EncodeFileID(0, uint32(i+1))
			if err != nil {
				t.Fatal(err)
			}
			path := SegmentPath(dir, id)
			val := bytes.Repeat([]byte{byte('a' + (i % 26))}, 1024+i*17)
			rid := uint64(i + 1)
			w, err := NewWriter(path, id)
			if err != nil {
				t.Fatal(err)
			}
			ptr, err := w.Append(0, nil, rid, val)
			if err != nil {
				_ = w.Close()
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			segs = append(segs, segSpec{
				id:       id,
				path:     path,
				ptr:      ptr,
				rid:      rid,
				val:      val,
				identity: retirementIdentityAtPath(t, path),
			})
		}

		registry := rootpublication.NewIdentityPinRegistry()
		first, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := first.Close(); err != nil {
				t.Error(err)
			}
		})
		second, err := NewManagerWithStableResourcePinRegistry(dir, registry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := second.Close(); err != nil {
				t.Error(err)
			}
		})

		// Warm all segments on second so sealed lazy mmap is active before racing.
		for _, seg := range segs {
			got, err := second.Read(seg.ptr)
			if err != nil || !bytes.Equal(got, seg.val) {
				t.Fatalf("warm Read(%d): err=%v", seg.id, err)
			}
		}

		reconcileCh := make(chan struct{}, 1)
		triggerReconcile := func(uint32) {
			select {
			case reconcileCh <- struct{}{}:
			default:
			}
		}
		second.SetOnRetiredBorrowReleased(triggerReconcile)
		first.SetOnSegmentRetired(triggerReconcile)

		var stopReaders atomic.Bool
		errCh := make(chan error, 16)
		reportErr := func(err error) {
			if err == nil {
				return
			}
			select {
			case errCh <- err:
			default:
			}
		}

		var reconcilerWG sync.WaitGroup
		stopReconciler := make(chan struct{})
		reconcilerWG.Add(1)
		go func() {
			defer reconcilerWG.Done()
			for {
				select {
				case <-stopReconciler:
					return
				case <-reconcileCh:
					reportErr(second.ReconcileRetiredSegments())
				}
			}
		}()

		const readerWorkers = 6
		var readersWG sync.WaitGroup
		for worker := 0; worker < readerWorkers; worker++ {
			readersWG.Add(1)
			go func(worker int) {
				defer readersWG.Done()
				var scratch []byte
				batchDst := make([][]byte, 2)
				for iter := 0; !stopReaders.Load(); iter++ {
					seg := segs[(worker+iter)%len(segs)]
					switch (worker + iter) % 5 {
					case 0:
						got, err := second.Read(seg.ptr)
						if err != nil {
							if !errors.Is(err, ErrFileNotFound) {
								reportErr(err)
								return
							}
							continue
						}
						if !bytes.Equal(got, seg.val) {
							reportErr(errors.New("Read payload mismatch"))
							return
						}
					case 1:
						got, err := second.ReadAppend(seg.ptr, scratch[:0])
						if err != nil {
							if !errors.Is(err, ErrFileNotFound) {
								reportErr(err)
								return
							}
							continue
						}
						if !bytes.Equal(got, seg.val) {
							reportErr(errors.New("ReadAppend payload mismatch"))
							return
						}
						scratch = got[:0]
					case 2:
						got, err := second.ReadUnsafeAppend(seg.ptr, scratch[:0])
						if err != nil {
							if !errors.Is(err, ErrFileNotFound) {
								reportErr(err)
								return
							}
							continue
						}
						if !bytes.Equal(got, seg.val) {
							reportErr(errors.New("ReadUnsafeAppend payload mismatch"))
							return
						}
						scratch = got[:0]
					case 3:
						gotBatch, err := second.ReadUnsafeAppendBatch([]page.ValuePtr{seg.ptr, seg.ptr}, batchDst)
						if err != nil {
							if !errors.Is(err, ErrFileNotFound) {
								reportErr(err)
								return
							}
							continue
						}
						if len(gotBatch) != 2 || !bytes.Equal(gotBatch[0], seg.val) || !bytes.Equal(gotBatch[1], seg.val) {
							reportErr(errors.New("ReadUnsafeAppendBatch payload mismatch"))
							return
						}
						batchDst = gotBatch
					case 4:
						rid, err := second.ReadRIDUnverified(seg.ptr)
						if err != nil {
							if !errors.Is(err, ErrFileNotFound) {
								reportErr(err)
								return
							}
							continue
						}
						if rid != seg.rid {
							reportErr(errors.New("ReadRIDUnverified mismatch"))
							return
						}
					}
				}
			}(worker)
		}

		// Retire all segments one by one while readers hammer Read*/ReadAppend* and
		// reconcile races concurrently.
		for _, seg := range segs {
			if err := first.RemoveSegment(seg.id); err != nil {
				stopReaders.Store(true)
				close(stopReconciler)
				readersWG.Wait()
				reconcilerWG.Wait()
				t.Fatalf("first.RemoveSegment(%d): %v", seg.id, err)
			}
			reportErr(second.ReconcileRetiredSegment(seg.id))
		}
		stopReaders.Store(true)
		readersWG.Wait()
		close(stopReconciler)
		reconcilerWG.Wait()

		select {
		case err := <-errCh:
			t.Fatalf("concurrent read/reconcile error: %v", err)
		default:
		}
		if err := second.ReconcileRetiredSegments(); err != nil {
			t.Fatalf("final ReconcileRetiredSegments: %v", err)
		}
		second.mu.RLock()
		remaining := len(second.files)
		second.mu.RUnlock()
		if remaining != 0 {
			t.Fatalf("second manager still tracks %d retired files after concurrent drain", remaining)
		}
		for _, seg := range segs {
			if got := registry.ObserverCount(seg.identity); got != 0 {
				t.Fatalf("seg %d ObserverCount=%d, want 0", seg.id, got)
			}
		}
	})

	t.Run("borrowFileFor_closed_or_mid_delete_returns_not_found_without_refresh", func(t *testing.T) {
		dir := t.TempDir()
		id1, err := EncodeFileID(0, 1)
		if err != nil {
			t.Fatal(err)
		}
		path1 := SegmentPath(dir, id1)
		w1, err := NewWriter(path1, id1)
		if err != nil {
			t.Fatal(err)
		}
		ptr1, err := w1.Append(0, nil, 1, []byte("closed-tracked-segment"))
		if err != nil {
			_ = w1.Close()
			t.Fatal(err)
		}
		if err := w1.Close(); err != nil {
			t.Fatal(err)
		}

		id2, err := EncodeFileID(0, 2)
		if err != nil {
			t.Fatal(err)
		}
		path2 := SegmentPath(dir, id2)
		w2, err := NewWriter(path2, id2)
		if err != nil {
			t.Fatal(err)
		}
		ptr2, err := w2.Append(0, nil, 2, []byte("mid-delete-tracked-segment"))
		if err != nil {
			_ = w2.Close()
			t.Fatal(err)
		}
		if err := w2.Close(); err != nil {
			t.Fatal(err)
		}

		m, err := NewManager(dir)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = m.Close() })

		// Case 1: tracked file is already closed while still linked on disk.
		m.mu.RLock()
		f1 := m.files[id1]
		f2 := m.files[id2]
		m.mu.RUnlock()
		if err := f1.Close(); err != nil {
			t.Fatalf("f1.Close: %v", err)
		}
		scansBefore := m.RefreshScanCount()
		if _, err := m.Read(ptr1); !errors.Is(err, ErrFileNotFound) || errors.Is(err, os.ErrClosed) {
			t.Fatalf("Read(closed tracked segment) = %v, want ErrFileNotFound (not os.ErrClosed)", err)
		}
		if gotScans := m.RefreshScanCount(); gotScans != scansBefore {
			t.Fatalf("borrowFileFor triggered refreshDiscover for tracked closed segment: scans %d -> %d", scansBefore, gotScans)
		}

		// Case 2: tracked file has deletionAdmissions > 0 while still linked on disk.
		m.mu.Lock()
		f2.deletionAdmissions = 1
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			f2.deletionAdmissions = 0
			m.mu.Unlock()
		}()
		scansBefore = m.RefreshScanCount()
		if _, err := m.Read(ptr2); !errors.Is(err, ErrFileNotFound) {
			t.Fatalf("Read(mid-delete tracked segment) = %v, want ErrFileNotFound", err)
		}
		if gotScans := m.RefreshScanCount(); gotScans != scansBefore {
			t.Fatalf("borrowFileFor triggered refreshDiscover for mid-delete segment: scans %d -> %d", scansBefore, gotScans)
		}
	})
}
