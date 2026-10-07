package valuelog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestManagerRefreshRetiredSegmentRejectsDirectoryPath(t *testing.T) {
	for _, namespace := range []string{"primary", "additional"} {
		t.Run(namespace, func(t *testing.T) {
			dir := t.TempDir()
			segmentDir := dir
			if namespace == "additional" {
				segmentDir = t.TempDir()
			}
			id := writeTestSegment(t, segmentDir, 0, 1, 1, []byte("retired"))
			manager, err := NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
			if err != nil {
				t.Fatal(err)
			}
			defer manager.Close()
			if namespace == "additional" {
				if err := manager.AddScanDir(segmentDir); err != nil {
					t.Fatal(err)
				}
			}
			set := manager.CurrentSetNoRefresh()
			retired := set.Files[id]
			if retired == nil || retired.stableIdentity == (rootpublication.StableIdentity{}) {
				t.Fatal("retired segment has no captured stable identity")
			}
			if err := manager.MarkZombie(id); err != nil {
				t.Fatal(err)
			}
			originalRemove := removeSegmentPath
			wantErr := errors.New("injected unlink failure")
			removeSegmentPath = func(string, func(string) error) error { return wantErr }
			t.Cleanup(func() { removeSegmentPath = originalRemove })
			err = manager.Release(set)
			removeSegmentPath = originalRemove
			if !errors.Is(err, wantErr) || !retired.closed.Load() {
				t.Fatalf("retired release did not fail after close: %v", err)
			}
			if err := manager.Refresh(); err != nil {
				t.Fatalf("unchanged retired identity refused: %v", err)
			}
			oldPath := retired.Path + ".retired"
			if err := os.Rename(retired.Path, oldPath); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(retired.Path, 0o700); err != nil {
				t.Fatal(err)
			}
			sentinel := filepath.Join(retired.Path, "sentinel")
			wantBytes := []byte("replacement directory must survive")
			if err := os.WriteFile(sentinel, wantBytes, 0o600); err != nil {
				t.Fatal(err)
			}
			before, err := os.Lstat(retired.Path)
			if err != nil {
				t.Fatal(err)
			}
			assertRefusal := func(label string, err error) {
				t.Helper()
				// Windows refuses a directory in the existing exact-child open;
				// Unix opens it nonblocking and rejects its non-regular type.
				if err == nil || (runtime.GOOS != "windows" && !errors.Is(err, rootpublication.ErrResourceConflict)) {
					t.Fatalf("%s accepted a directory retired pathname: %v", label, err)
				}
			}
			for range 2 {
				assertRefusal("Refresh", manager.Refresh())
			}
			assertRefusal("retirement retry", manager.deleteZombieFile(retired))
			// These operations also prove every refusal released manager.mu.
			manager.mu.RLock()
			tracked := manager.files[id]
			manager.mu.RUnlock()
			if tracked != retired || !retired.IsZombie.Load() {
				t.Fatal("directory rebound changed retirement ownership")
			}
			current := manager.CurrentSetNoRefresh()
			if current.Files[id] != nil {
				t.Fatal("directory rebound resurrected the retired segment")
			}
			if err := manager.Release(current); err != nil {
				t.Fatal(err)
			}
			after, err := os.Lstat(retired.Path)
			if err != nil || !os.SameFile(before, after) || !after.IsDir() {
				t.Fatalf("replacement directory changed during refusal: %v", err)
			}
			gotBytes, err := os.ReadFile(sentinel)
			if err != nil || !bytes.Equal(gotBytes, wantBytes) {
				t.Fatalf("replacement sentinel changed during refusal: %v", err)
			}
			if err := os.Remove(sentinel); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(retired.Path); err != nil {
				t.Fatal(err)
			}
			if err := manager.Refresh(); err != nil {
				t.Fatalf("missing retired pathname refused: %v", err)
			}
			manager.mu.RLock()
			tracked = manager.files[id]
			manager.mu.RUnlock()
			if tracked != retired || manager.HasSegment(id) {
				t.Fatal("missing pathname changed retirement ownership")
			}
			if err := os.Rename(oldPath, retired.Path); err != nil {
				t.Fatal(err)
			}
			if err := manager.deleteZombieFile(retired); err != nil {
				t.Fatalf("restored original identity did not retire: %v", err)
			}
			manager.mu.RLock()
			tracked = manager.files[id]
			manager.mu.RUnlock()
			if tracked != nil {
				t.Fatal("successful retirement retained the zombie")
			}
		})
	}
}
