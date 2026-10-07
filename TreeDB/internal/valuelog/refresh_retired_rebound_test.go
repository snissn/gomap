package valuelog

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestManagerRefreshRetiredSegmentRejectsReboundPath(t *testing.T) {
	dir := t.TempDir()
	id := writeTestSegment(t, dir, 0, 1, 1, []byte("retired"))
	manager, err := NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	set := manager.CurrentSetNoRefresh()
	retired := set.Files[id]
	if retired.stableIdentity == (rootpublication.StableIdentity{}) {
		t.Fatal("retired segment has no captured stable identity")
	}
	if err := manager.MarkZombie(id); err != nil {
		t.Fatal(err)
	}
	originalRemove := removeSegmentPath
	wantErr := errors.New("injected unlink failure")
	removeSegmentPath = func(string) error { return wantErr }
	t.Cleanup(func() { removeSegmentPath = originalRemove })
	err = manager.Release(set)
	removeSegmentPath = originalRemove
	if !errors.Is(err, wantErr) || !retired.closed.Load() {
		t.Fatalf("retired release did not fail after close: %v", err)
	}
	if err := manager.Refresh(); err != nil {
		t.Fatalf("unchanged retired identity refused: %v", err)
	}
	oldIdentity, err := stableIdentityAtPath(retired.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !rootpublication.SamePhysicalIdentity(oldIdentity, retired.stableIdentity) {
		t.Fatal("retired pathname identity changed before rebound")
	}
	if err := os.Rename(retired.Path, retired.Path+".retired"); err != nil {
		t.Fatal(err)
	}
	writeTestSegment(t, dir, 0, 1, 2, []byte("replacement"))
	newIdentity, err := stableIdentityAtPath(retired.Path)
	if err != nil || rootpublication.SamePhysicalIdentity(oldIdentity, newIdentity) {
		t.Fatalf("path was not rebound to a different file: %v", err)
	}
	wantBytes, err := os.ReadFile(retired.Path)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := manager.Refresh(); !errors.Is(err, rootpublication.ErrResourceConflict) {
			t.Fatalf("refresh accepted a rebound retired path: %v", err)
		}
	}
	if err := manager.deleteZombieFile(retired); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("retirement retry accepted a replacement: %v", err)
	}
	manager.mu.RLock()
	tracked := manager.files[id]
	manager.mu.RUnlock()
	if tracked != retired || !retired.IsZombie.Load() {
		t.Fatal("rebound path changed retirement ownership")
	}
	current := manager.CurrentSetNoRefresh()
	if current.Files[id] != nil {
		t.Fatal("rebound path resurrected a retired segment")
	}
	if err := manager.Release(current); err != nil {
		t.Fatal(err)
	}
	gotBytes, err := os.ReadFile(retired.Path)
	if err != nil || !bytes.Equal(gotBytes, wantBytes) {
		t.Fatalf("replacement changed during refresh or retry: %v", err)
	}
	finalIdentity, err := stableIdentityAtPath(retired.Path)
	if err != nil || !rootpublication.SamePhysicalIdentity(newIdentity, finalIdentity) {
		t.Fatalf("replacement identity changed during refresh or retry: %v", err)
	}
}
