package slab

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestSlabWriterWaitForOffsetDuringClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data-0000.slab")
	slab, err := OpenSlab(path, 0)
	if err != nil {
		t.Fatalf("OpenSlab: %v", err)
	}
	t.Cleanup(func() { _ = slab.Close() })

	writer := NewSlabWriter(slab, 64)

	payload := bytes.Repeat([]byte("a"), 1024)
	start, err := writer.Write(payload)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	end := start + int64(len(payload))

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- writer.WaitForOffset(end)
	}()

	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := <-waitCh; err != nil {
		t.Fatalf("WaitForOffset: %v", err)
	}
}
