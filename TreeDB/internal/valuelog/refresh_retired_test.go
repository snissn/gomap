package valuelog

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestManagerRefreshDuringRetiredSegmentUnlink(t *testing.T) {
	for _, failUnlink := range []bool{false, true} {
		name := "success"
		if failUnlink {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			id := writeTestSegment(t, dir, 0, 1, 1, bytes.Repeat([]byte("old"), 32))
			manager, err := NewManager(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = manager.Close() })
			set := manager.CurrentSetNoRefresh()
			retired := set.Files[id]
			if err := manager.MarkZombie(id); err != nil {
				t.Fatal(err)
			}
			newID := writeTestSegment(t, dir, 0, 2, 1, bytes.Repeat([]byte("new"), 32))
			originalRemove := removeSegmentPath
			wantErr := errors.New("injected unlink failure")
			called := false
			removeSegmentPath = func(path string, remove func(string) error) error {
				called = true
				if _, err := retired.File.Stat(); !retired.closed.Load() || err == nil {
					t.Errorf("retirement did not close before unlink: %v", err)
				}
				if err := manager.Refresh(); err != nil {
					t.Errorf("refresh during closed-handle unlink: %v", err)
				}
				manager.mu.RLock()
				tracked := manager.files[id]
				manager.mu.RUnlock()
				if tracked != retired {
					t.Error("refresh replaced the retirement owner's entry")
				}
				current := manager.CurrentSetNoRefresh()
				if current.Files[id] != nil || current.Files[newID] == nil {
					t.Error("refresh resurrected retired data or missed a new live segment")
				}
				if err := manager.Release(current); err != nil {
					t.Errorf("release current set: %v", err)
				}
				if failUnlink {
					return wantErr
				}
				return originalRemove(path, remove)
			}
			t.Cleanup(func() { removeSegmentPath = originalRemove })
			err = manager.Release(set)
			removeSegmentPath = originalRemove
			if !called || (failUnlink && !errors.Is(err, wantErr)) || (!failUnlink && err != nil) {
				t.Fatalf("unlink called=%t, fail=%t, error=%v", called, failUnlink, err)
			}
			if failUnlink {
				if err := manager.Refresh(); err != nil {
					t.Fatalf("refresh after failed unlink: %v", err)
				}
				manager.mu.RLock()
				tracked := manager.files[id]
				manager.mu.RUnlock()
				if tracked != retired || !retired.IsZombie.Load() {
					t.Fatal("failed unlink lost the retired entry")
				}
			}
		})
	}
}

func TestManagerRefreshRetiredSegmentPreservesPathConflict(t *testing.T) {
	dir := t.TempDir()
	id := writeTestSegment(t, dir, 0, 1, 1, []byte("old"))
	manager, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if err := manager.MarkZombie(id); err != nil {
		t.Fatal(err)
	}
	other := t.TempDir()
	writeTestSegment(t, other, 0, 1, 1, []byte("replacement"))
	if err := manager.AddScanDir(other); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("retired ID accepted a different path: %v", err)
	}
}

func TestManagerRefreshClosedLiveSegmentStillFails(t *testing.T) {
	dir := t.TempDir()
	id := writeTestSegment(t, dir, 0, 1, 1, []byte("live"))
	manager, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if err := manager.files[id].Close(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Refresh(); err == nil {
		t.Fatal("refresh suppressed a closed live handle")
	}
}

func BenchmarkManagerRefreshLiveSegment(b *testing.B) {
	dir := b.TempDir()
	id, err := EncodeFileID(0, 1)
	if err != nil {
		b.Fatal(err)
	}
	writer, err := NewWriter(filepath.Join(dir, "value-l0-000001.log"), id)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := writer.Append(0, nil, 1, []byte("live")); err != nil {
		b.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		b.Fatal(err)
	}
	manager, err := NewManager(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer manager.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := manager.Refresh(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}
