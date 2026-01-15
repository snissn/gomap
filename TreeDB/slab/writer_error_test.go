package slab

import (
	"path/filepath"
	"testing"
	"time"
)

func TestSlabWriter_FlushLoopError_NoDeadlock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data-0000.slab")
	s, err := OpenSlab(path, 0)
	if err != nil {
		t.Fatalf("OpenSlab failed: %v", err)
	}
	defer s.Close()

	bufSize := 64
	w := NewSlabWriter(s, bufSize)
	w.TestPauseFlushLoop()

	if _, err := w.Write(make([]byte, bufSize+1)); err != nil {
		t.Fatalf("Write oversize failed: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		if _, err := w.Write(make([]byte, bufSize)); err != nil {
			errCh <- err
			return
		}
		_, err := w.Write([]byte{1})
		errCh <- err
	}()

	time.Sleep(50 * time.Millisecond)
	_ = s.File.Close()
	w.TestResumeFlushLoop()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("expected error after flush failure")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("write did not unblock after flush error")
	}

	_ = w.Close()
}
