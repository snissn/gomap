//go:build darwin || linux || freebsd || netbsd || openbsd

package collections

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
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
			target := filepath.Join(s.dir, "ordinary-target")
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
			m := testVectorPartitionManifestV1()
			m.Collection, m.IndexName = col.name, index
			raw, err := EncodeVectorPartitionManifestV1(m)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(s.dir, prefix+"-7.vpm"), raw, 0o600); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(s.dir, "ordinary-target")
			if err := os.WriteFile(target, []byte("7\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, filepath.Join(s.dir, prefix+suffix)); err != nil {
				t.Fatal(err)
			}
			status, err := col.VectorPartitionStatusV1(index, 7)
			if err != nil || status.StaleReason != "pointer_invalid" {
				t.Fatalf("status=%+v err=%v want pointer_invalid", status, err)
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
