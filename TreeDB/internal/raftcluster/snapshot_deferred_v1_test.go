package raftcluster

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestDeferredRaftSnapshotV1MaterializesOnceAndReleases(t *testing.T) {
	manifest := validSnapshotManifestV1()
	ready := RaftSnapshotV1{Manifest: manifest, Payload: validRaftSnapshotArchivePayloadV1(t, manifest)}
	var calls, releases atomic.Int32
	snapshot, err := NewDeferredRaftSnapshotV1(func(context.Context) (RaftSnapshotV1, error) {
		calls.Add(1)
		return ready, nil
	}, func() error { releases.Add(1); return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Release()
	if err := snapshot.validateCapturedV1(); err != nil || calls.Load() != 0 || snapshot.Manifest != (SnapshotManifestV1{}) {
		t.Fatalf("capture materialized or exposed provisional manifest: %v", err)
	}
	clone := snapshot.Clone()
	for _, value := range []RaftSnapshotV1{snapshot, clone} {
		got, err := value.Materialize()
		if err != nil || !snapshotManifestV1Equal(got.Manifest, manifest) {
			t.Fatalf("materialize: %+v %v", got.Manifest, err)
		}
	}
	if calls.Load() != 1 || releases.Load() != 1 {
		t.Fatal("materialization or capture release was repeated")
	}
	if err := snapshot.Release(); err != nil {
		t.Fatal(err)
	}
	if err := clone.Release(); err != nil || releases.Load() != 1 {
		t.Fatal("clone release was not idempotent")
	}
	if _, err := clone.Materialize(); !errors.Is(err, ErrInvalidSnapshotManifest) {
		t.Fatal(err)
	}
}

func TestDeferredRaftSnapshotV1ReleaseBeforeAndDuringMaterialization(t *testing.T) {
	for _, started := range []bool{false, true} {
		t.Run(map[bool]string{false: "never-persisted", true: "in-progress"}[started], func(t *testing.T) {
			entered := make(chan struct{})
			var releases atomic.Int32
			snapshot, err := NewDeferredRaftSnapshotV1(func(ctx context.Context) (RaftSnapshotV1, error) {
				close(entered)
				<-ctx.Done()
				return RaftSnapshotV1{}, ctx.Err()
			}, func() error { releases.Add(1); return nil }, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.Release()
			done := make(chan error, 1)
			if started {
				go func() { _, err := snapshot.Materialize(); done <- err }()
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("materializer did not start")
				}
			}
			if err := snapshot.Release(); err != nil {
				t.Fatal(err)
			}
			if started {
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			}
			if releases.Load() != 1 {
				t.Fatal("capture not released exactly once")
			}
		})
	}
}

func TestDeferredRaftSnapshotV1RejectsInvalidMaterializedManifest(t *testing.T) {
	var releases int
	snapshot, err := NewDeferredRaftSnapshotV1(func(context.Context) (RaftSnapshotV1, error) {
		return RaftSnapshotV1{Payload: []byte("invalid")}, nil
	}, func() error { releases++; return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Release()
	if _, err := snapshot.Materialize(); !errors.Is(err, ErrInvalidSnapshotManifest) || releases != 1 {
		t.Fatalf("invalid result: %v releases=%d", err, releases)
	}
}

func TestDeferredRaftSnapshotV1FinalizedReaderRetainsAdmission(t *testing.T) {
	manifest := validSnapshotManifestV1()
	path := writeRaftSnapshotArchiveFileForTest(t, validRaftSnapshotArchivePayloadV1(t, manifest))
	var captures, owners atomic.Int32
	snapshot, err := NewDeferredRaftSnapshotV1(func(context.Context) (RaftSnapshotV1, error) {
		return RaftSnapshotV1{Manifest: manifest, ArchivePath: path}, nil
	}, func() error { captures.Add(1); return nil }, func() error { owners.Add(1); return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Release()
	ready, err := snapshot.Materialize()
	if err != nil {
		t.Fatal(err)
	}
	if ready.ArchivePath != path || !snapshotManifestV1Equal(ready.Manifest, manifest) || captures.Load() != 1 || owners.Load() != 0 {
		t.Fatal("finalization lost fields or ended total admission")
	}
	clone := ready.Clone()
	reader, err := clone.OpenArchive()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if err := ready.Release(); err != nil {
		t.Fatal(err)
	}
	if owners.Load() != 0 {
		t.Fatal("reader did not retain admission")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("active reader archive deleted", err)
	}
	if _, err := reader.Read(make([]byte, 1)); !errors.Is(err, context.Canceled) {
		t.Fatal("released reader not canceled", err)
	}
	if _, err := clone.OpenArchive(); err == nil {
		t.Fatal("released clone reopened")
	}
	if _, err := clone.Materialize(); err == nil {
		t.Fatal("released clone materialized")
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if owners.Load() != 1 || captures.Load() != 1 {
		t.Fatal("ownership callbacks not exactly once")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("archive retained after final reader", err)
	}
	if err := clone.Release(); err != nil {
		t.Fatal(err)
	}
	if owners.Load() != 1 {
		t.Fatal("duplicate owner release")
	}
}

func TestDeferredRaftSnapshotV1FailureReleasesTotalAdmission(t *testing.T) {
	failure := errors.New("materialization failed")
	var captures, owners atomic.Int32
	snapshot, err := NewDeferredRaftSnapshotV1(func(context.Context) (RaftSnapshotV1, error) {
		return RaftSnapshotV1{}, failure
	}, func() error { captures.Add(1); return nil }, func() error { owners.Add(1); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.Materialize(); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if err := snapshot.Release(); err != nil {
		t.Fatal(err)
	}
	if captures.Load() != 1 || owners.Load() != 1 {
		t.Fatal("failed materialization leaked or repeated ownership")
	}
}

func TestDeferredRaftSnapshotV1CleanupFailureRetainsAdmissionForRetry(t *testing.T) {
	for _, readerHeld := range []bool{false, true} {
		t.Run(map[bool]string{false: "release", true: "last-reader"}[readerHeld], func(t *testing.T) {
			manifest := validSnapshotManifestV1()
			path := writeRaftSnapshotArchiveFileForTest(t, validRaftSnapshotArchivePayloadV1(t, manifest))
			var owners atomic.Int32
			snapshot, err := NewDeferredRaftSnapshotV1(func(context.Context) (RaftSnapshotV1, error) {
				return RaftSnapshotV1{Manifest: manifest, ArchivePath: path}, nil
			}, func() error { return nil }, func() error { owners.Add(1); return nil })
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.Release()
			ready, err := snapshot.Materialize()
			if err != nil {
				t.Fatal(err)
			}
			var reader io.ReadCloser
			if readerHeld {
				reader, err = ready.OpenArchive()
				if err != nil {
					t.Fatal(err)
				}
				defer reader.Close()
			}
			// Replacing the path with a nonempty directory makes Remove fail on
			// every platform; no filesystem permission or root-user assumptions.
			// Close the file first on Windows, where open-file unlink is refused.
			if readerHeld && runtime.GOOS == "windows" {
				if err := reader.Close(); err != nil {
					t.Fatal(err)
				}
				readerHeld = false
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(path, 0700); err != nil {
				t.Fatal(err)
			}
			blocker := filepath.Join(path, "retain")
			if err := os.WriteFile(blocker, []byte("cleanup debt"), 0600); err != nil {
				t.Fatal(err)
			}
			err = ready.Release()
			if readerHeld {
				if err != nil {
					t.Fatal(err)
				}
				err = reader.Close()
			}
			if err == nil || owners.Load() != 0 {
				t.Fatalf("cleanup err=%v owners=%d", err, owners.Load())
			}
			if err := os.Remove(blocker); err != nil {
				t.Fatal(err)
			}
			if err := snapshot.Release(); err != nil {
				t.Fatal("cleanup retry", err)
			}
			if owners.Load() != 1 {
				t.Fatal("successful cleanup retry retained admission")
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatal("cleanup target lost", err)
			}
			if err := ready.Release(); err != nil {
				t.Fatal(err)
			}
			if owners.Load() != 1 {
				t.Fatal("cleanup retry repeated owner release")
			}
		})
	}
}

func TestDeferredRaftSnapshotV1CaptureCleanupFailureRetriesBeforeOwnerRelease(t *testing.T) {
	failure := errors.New("capture cleanup failed")
	manifest := validSnapshotManifestV1()
	var captures, owners atomic.Int32
	snapshot, err := NewDeferredRaftSnapshotV1(func(context.Context) (RaftSnapshotV1, error) {
		return RaftSnapshotV1{Manifest: manifest, Payload: validRaftSnapshotArchivePayloadV1(t, manifest)}, nil
	}, func() error {
		if captures.Add(1) == 1 {
			return failure
		}
		return nil
	}, func() error { owners.Add(1); return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Release()
	if _, err := snapshot.Materialize(); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if owners.Load() != 0 {
		t.Fatal("capture cleanup failure lost admission")
	}
	if err := snapshot.Release(); err != nil {
		t.Fatal("cleanup retry returned stale error", err)
	}
	if captures.Load() != 2 || owners.Load() != 1 {
		t.Fatal("cleanup retry ordering/count mismatch")
	}
}
