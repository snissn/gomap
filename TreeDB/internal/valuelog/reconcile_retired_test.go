package valuelog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
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
		first.SetOnSegmentRetired(func(fileID uint32) error {
			return second.ReconcileRetiredSegment(fileID)
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

		// Releasing held deletes the zombie and invokes SetOnSegmentRetired -> second reconciles in O(1).
		if err := first.Release(held); err != nil {
			t.Fatalf("first.Release(held): %v", err)
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
