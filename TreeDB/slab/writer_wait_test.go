package slab

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

func TestSlabWriter_WaitForOffset_Close_NoHang(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data-0000.slab")
	slab, err := OpenSlab(path, 0)
	if err != nil {
		t.Fatalf("OpenSlab: %v", err)
	}
	t.Cleanup(func() { _ = slab.Close() })

	writer := NewSlabWriter(slab, 64)
	writer.TestPauseFlushLoop()

	payload := bytes.Repeat([]byte("b"), 256)
	start, err := writer.Write(payload)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	end := start + int64(len(payload))

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- writer.WaitForOffset(end)
	}()

	time.Sleep(50 * time.Millisecond)
	if err := writer.Close(); err != nil && !strings.Contains(err.Error(), "closed") {
		t.Fatalf("Close: %v", err)
	}

	select {
	case err := <-waitCh:
		if err == nil || !strings.Contains(err.Error(), "closed") {
			t.Fatalf("expected closed error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("WaitForOffset did not unblock after Close")
	}
}

func TestSlabWriter_Close_UnblocksWaiters(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data-0000.slab")
	slab, err := OpenSlab(path, 0)
	if err != nil {
		t.Fatalf("OpenSlab: %v", err)
	}
	t.Cleanup(func() { _ = slab.Close() })

	writer := NewSlabWriter(slab, 64)
	writer.TestPauseFlushLoop()

	payload := bytes.Repeat([]byte("c"), 256)
	start, err := writer.Write(payload)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	end := start + int64(len(payload))

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- writer.WaitForOffset(end)
	}()

	syncCh := make(chan error, 1)
	go func() {
		syncCh <- writer.Sync()
	}()

	time.Sleep(50 * time.Millisecond)
	if err := writer.Close(); err != nil && !strings.Contains(err.Error(), "closed") {
		t.Fatalf("Close: %v", err)
	}

	select {
	case err := <-waitCh:
		if err == nil || !strings.Contains(err.Error(), "closed") {
			t.Fatalf("expected closed error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("WaitForOffset did not unblock after Close")
	}

	select {
	case err := <-syncCh:
		if err == nil || !strings.Contains(err.Error(), "closed") {
			t.Fatalf("expected closed error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Sync did not unblock after Close")
	}
}
