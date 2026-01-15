package slab

import (
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSlabWriter_CloseWhileWriting_NoPanic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data-0000.slab")
	s, err := OpenSlab(path, 0)
	if err != nil {
		t.Fatalf("OpenSlab failed: %v", err)
	}
	defer s.Close()

	w := NewSlabWriter(s, 128)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := []byte("payload")
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := w.Write(data); err != nil {
					if strings.Contains(err.Error(), "closed") {
						return
					}
					t.Errorf("Write failed: %v", err)
					return
				}
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	if err := w.Close(); err != nil && !strings.Contains(err.Error(), "closed") {
		t.Fatalf("Close failed: %v", err)
	}
	close(stop)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("writers did not exit after Close")
	}
}

func TestSlabWriter_Close_UnblocksPendingEnqueue(t *testing.T) {
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

	if _, err := w.Write(make([]byte, bufSize)); err != nil {
		t.Fatalf("Write fill failed: %v", err)
	}
	if _, err := w.Write([]byte{1}); err != nil {
		t.Fatalf("Write rotate failed: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		if _, err := w.Write(make([]byte, bufSize-1)); err != nil {
			errCh <- err
			return
		}
		_, err := w.Write([]byte{1})
		errCh <- err
	}()

	select {
	case err := <-errCh:
		t.Fatalf("expected enqueue to block, got %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	if err := w.Close(); err != nil && !strings.Contains(err.Error(), "closed") {
		t.Fatalf("Close failed: %v", err)
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("expected error on close")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("blocked write did not unblock after Close")
	}
}
