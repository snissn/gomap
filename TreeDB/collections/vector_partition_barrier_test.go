package collections

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestVectorPartitionStorageBarrierV1SerializesAbsoluteAndRelativeRoots(t *testing.T) {
	root := t.TempDir()
	rel := filepath.Join(root, ".")
	entered := make(chan struct{})
	release := make(chan struct{})
	first := make(chan error, 1)
	go func() {
		first <- WithVectorPartitionStorageBarrierV1(root, func() error { close(entered); <-release; return nil })
	}()
	<-entered
	second := make(chan error, 1)
	go func() { second <- WithVectorPartitionStorageBarrierV1(rel, func() error { return nil }) }()
	select {
	case err := <-second:
		t.Fatalf("relative alias escaped barrier: %v", err)
	default:
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
}

func TestVectorPartitionStorageBarrierWaitHonorsContextV1(t *testing.T) {
	root := t.TempDir()
	entered := make(chan struct{})
	release := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- WithVectorPartitionStorageBarrierV1(root, func() error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	called := false
	if err := WithVectorPartitionStorageBarrierWithContextV1(ctx, root, func() error {
		called = true
		return nil
	}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("barrier wait err=%v", err)
	}
	if called {
		t.Fatal("canceled barrier callback ran")
	}

	close(release)
	if err := <-holderDone; err != nil {
		t.Fatal(err)
	}
	if err := WithVectorPartitionStorageBarrierWithContextV1(context.Background(), root, func() error {
		called = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("barrier was not released after canceled waiter")
	}
}
