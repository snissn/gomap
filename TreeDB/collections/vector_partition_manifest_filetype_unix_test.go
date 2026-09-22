//go:build darwin || linux || freebsd || netbsd || openbsd

package collections

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func malformedVectorPartitionEntryFixtureV1(t *testing.T) (*VectorPartitionStoreV1, *Collection, string, string) {
	t.Helper()
	requireVectorPartitionPersistenceV1(t)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0, 0}}})
	t.Cleanup(func() { _ = d.Close() })
	s, err := OpenVectorPartitionStoreV1(d.Dir())
	if err != nil {
		t.Fatal(err)
	}
	return s, col, def.Name, safeVPM(col.name) + "-" + safeVPM(def.Name)
}

func TestVectorPartitionReachabilityRejectsNonRegularLifecycleRecordsV1(t *testing.T) {
	for _, suffix := range []string{"-7.vpm", ".active", ".retired", ".inactive", ".deleting"} {
		t.Run(suffix, func(t *testing.T) {
			s, col, _, prefix := malformedVectorPartitionEntryFixtureV1(t)
			target := filepath.Join(t.TempDir(), "ordinary-target")
			if err := os.WriteFile(target, []byte("not a record"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, filepath.Join(s.dir, prefix+suffix)); err != nil {
				t.Fatal(err)
			}
			if _, _, err := col.vectorPartitionReachabilityRefsV1(nil); err == nil {
				t.Fatal("reachability accepted symlinked durable record")
			}
		})
	}
}

func TestVectorPartitionStatusRejectsNonRegularLifecyclePointersV1(t *testing.T) {
	for _, suffix := range []string{".active", ".retired", ".inactive"} {
		t.Run(suffix, func(t *testing.T) {
			s, col, index, prefix := malformedVectorPartitionEntryFixtureV1(t)
			target := filepath.Join(t.TempDir(), "ordinary-target")
			if err := os.WriteFile(target, []byte("7\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, filepath.Join(s.dir, prefix+suffix)); err != nil {
				t.Fatal(err)
			}
			status, err := col.VectorPartitionStatusV1(index, 7)
			if !errors.Is(err, ErrVectorPartitionManifestInvalid) || status.Active {
				t.Fatalf("status=%+v err=%v want legacy authority rejection", status, err)
			}
		})
	}
}

func TestVectorPartitionReachabilityRejectsLifecycleDirectoryAndFIFOWithoutBlockingV1(t *testing.T) {
	t.Run("directory", func(t *testing.T) {
		s, col, _, prefix := malformedVectorPartitionEntryFixtureV1(t)
		if err := os.Mkdir(filepath.Join(s.dir, prefix+".active"), 0o700); err != nil {
			t.Fatal(err)
		}
		if _, _, err := col.vectorPartitionReachabilityRefsV1(nil); err == nil {
			t.Fatal("reachability accepted lifecycle directory")
		}
	})
	t.Run("fifo", func(t *testing.T) {
		s, col, _, prefix := malformedVectorPartitionEntryFixtureV1(t)
		if err := syscall.Mkfifo(filepath.Join(s.dir, prefix+"-7.vpm"), 0o600); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			_, _, err := col.vectorPartitionReachabilityRefsV1(nil)
			done <- err
		}()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("reachability accepted FIFO durable record")
			}
		case <-time.After(time.Second):
			t.Fatal("reachability blocked opening FIFO durable record")
		}
	})
}

func TestVectorPartitionStoreRejectsSymlinkRootAndMalformedTombstoneFenceV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "real"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "real"), filepath.Join(root, "vector_partitions")); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenVectorPartitionStoreV1(root); err == nil {
		t.Fatal("store creation accepted symlink root")
	}
	if _, err := OpenExistingVectorPartitionStoreV1(root); err == nil {
		t.Fatal("existing store accepted symlink root")
	}

	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	tombstone := s.deleteTombstonePath(m.Collection, m.IndexName, m.Generation)
	if err := os.Symlink(filepath.Join(s.dir, "missing"), tombstone); err != nil {
		t.Fatal(err)
	}
	err = WithVectorPartitionStorageBarrierV1(s.root, func() error { return s.publishLocked(m) })
	if err == nil {
		t.Fatalf("publish err=%v want malformed tombstone fence rejection", err)
	}
	name := safeVPM(m.Collection) + "-" + safeVPM(m.IndexName) + "-" + strconv.FormatUint(m.Generation, 10) + ".vpm"
	if _, err := os.Lstat(filepath.Join(s.dir, name)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("malformed tombstone fence allowed manifest publication: %v", err)
	}
}

func TestVectorPartitionStoreRejectsDirectoryReplacementV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	root := t.TempDir()
	store, err := OpenVectorPartitionStoreV1(root)
	if err != nil {
		t.Fatal(err)
	}
	replaced := filepath.Join(root, "vector_partitions.replaced")
	if err := os.Rename(store.dir, replaced); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(store.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Open("docs", "embedding", 7); err == nil {
		t.Fatal("existing store followed replacement directory")
	}
	if fresh, err := OpenExistingVectorPartitionStoreV1(root); err != nil {
		t.Fatalf("fresh store did not bind replacement directory: %v", err)
	} else if _, err := fresh.Open("docs", "embedding", 7); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fresh store did not observe replacement: %v", err)
	}
}

func TestVectorPartitionPublishFailsClosedOnGenerationLinkedDirectoryReplacementV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	root := t.TempDir()
	s, err := OpenVectorPartitionStoreV1(root)
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	moved := filepath.Join(root, "vector_partitions.old")
	replaced := false
	restore := setVectorPartitionLifecycleStoreHookForTestV1(func(boundary string) error {
		if boundary == "before_checkpoint_install" && !replaced {
			replaced = true
			if err := os.Rename(s.dir, moved); err != nil {
				return err
			}
			return os.Mkdir(s.dir, 0o700)
		}
		return nil
	})
	defer restore()
	if err := s.publishValidatedReady(m); err == nil {
		t.Fatal("publication followed replacement directory")
	}
	if !replaced {
		t.Fatal("checkpoint install boundary did not replace directory")
	}
	oldEntries, err := os.ReadDir(moved)
	if err != nil || len(oldEntries) != 1 || !strings.HasSuffix(oldEntries[0].Name(), ".vlc") {
		t.Fatalf("old bound namespace entries=%v err=%v, want installed immutable checkpoint", oldEntries, err)
	}
	newEntries, err := os.ReadDir(s.dir)
	if err != nil || len(newEntries) != 0 {
		t.Fatalf("replacement namespace entries=%v err=%v, want empty", newEntries, err)
	}
}

func TestVectorPartitionDeleteUsesRetainedDirectoryAfterTombstoneV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	root := t.TempDir()
	s, err := OpenVectorPartitionStoreV1(root)
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	if err := s.publishLocked(m); err != nil {
		t.Fatal(err)
	}
	if err := deactivateVectorPartitionStoreForTest(s, m.Collection, m.IndexName); err != nil {
		t.Fatal(err)
	}
	oldRoot := filepath.Join(root, "old-root")
	if err := os.Mkdir(oldRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	moved := filepath.Join(oldRoot, "vector_partitions")
	decoyName, decoyBytes := "replacement-decoy", []byte("must-not-change")
	restore := setVectorPartitionDeleteAfterTombstoneForTestV1(func() {
		if err := os.Rename(s.dir, moved); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(s.dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(s.dir, decoyName), decoyBytes, 0o600); err != nil {
			t.Fatal(err)
		}
	})
	defer restore()
	if err := deleteVectorPartitionStoreForTest(s, m.Collection, m.IndexName, m.Generation, VectorPartitionCleanupEligibilityV1{}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(s.dir, decoyName))
	if err != nil || !bytes.Equal(got, decoyBytes) {
		t.Fatalf("replacement namespace changed: %q %v", got, err)
	}
	oldStore, err := OpenExistingVectorPartitionStoreV1(oldRoot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oldStore.Open(m.Collection, m.IndexName, m.Generation); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old retained authority still opens deleting generation: %v", err)
	}
	reclaim, err := oldStore.openDeleteTombstone(m.Collection, m.IndexName, m.Generation)
	if err != nil || reclaim.Generation != m.Generation {
		t.Fatalf("old retained delete authority=%+v err=%v", reclaim, err)
	}
}
