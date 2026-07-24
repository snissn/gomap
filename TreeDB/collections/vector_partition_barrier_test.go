package collections

import (
	"path/filepath"
	"testing"
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
