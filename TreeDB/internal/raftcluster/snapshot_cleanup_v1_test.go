package raftcluster

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRaftSnapshotCleanupOwnsBlockedWorkAndRetryV1(t *testing.T) {
	entered, resume := make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	unblock := func() { resumeOnce.Do(func() { close(resume) }) }
	defer unblock()
	var cleanups, releases atomic.Int32
	var deny atomic.Bool
	deny.Store(true)
	denied := errors.New("cleanup denied")
	owner, err := NewRaftSnapshotCleanupV1(func(ctx context.Context) error {
		close(entered)
		<-resume // Models an arbitrary reader that ignores cancellation.
		return ctx.Err()
	}, func() error {
		cleanups.Add(1)
		if deny.Load() {
			return denied
		}
		return nil
	}, func() error { releases.Add(1); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Materialize(); !errors.Is(err, ErrInvalidSnapshotManifest) {
		t.Fatal("cleanup owner materialized", err)
	}
	if err := owner.validateCapturedV1(); !errors.Is(err, ErrInvalidSnapshotManifest) {
		t.Fatal("cleanup owner became provider snapshot", err)
	}
	done, closed := make(chan error, 1), make(chan error, 1)
	go func() { done <- owner.RunCleanupWorkV1() }()
	<-entered
	go func() { closed <- owner.Release() }()
	select {
	case <-closed:
		t.Fatal("Release passed blocked work")
	case <-time.After(20 * time.Millisecond):
	}
	if cleanups.Load() != 0 || releases.Load() != 0 {
		t.Fatal("cleaned active work")
	}
	unblock()
	if err := <-done; !errors.Is(err, context.Canceled) || !errors.Is(err, denied) {
		t.Fatal(err)
	}
	if err := <-closed; !errors.Is(err, denied) {
		t.Fatal(err)
	}
	if releases.Load() != 0 {
		t.Fatal("cleanup debt released admission")
	}
	deny.Store(false)
	if err := owner.RetryCleanupV1(); err != nil {
		t.Fatal(err)
	}
	if releases.Load() != 1 {
		t.Fatal("cleanup retry did not release exactly once")
	}
	if err := owner.Release(); err != nil || releases.Load() != 1 {
		t.Fatal(err)
	}
}
