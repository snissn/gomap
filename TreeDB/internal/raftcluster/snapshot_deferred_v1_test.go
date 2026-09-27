package raftcluster

import (
	"context"
	"errors"
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
	}, func() error { releases.Add(1); return nil })
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
			}, func() error { releases.Add(1); return nil })
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
	}, func() error { releases++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Release()
	if _, err := snapshot.Materialize(); !errors.Is(err, ErrInvalidSnapshotManifest) || releases != 1 {
		t.Fatalf("invalid result: %v releases=%d", err, releases)
	}
}
