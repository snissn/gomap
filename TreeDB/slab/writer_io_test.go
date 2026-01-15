package slab

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestSlabWriter_IgnoreBoundary_ConcurrentWrites_NoCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data-0000.slab")
	s, err := OpenSlab(path, 0)
	if err != nil {
		t.Fatalf("OpenSlab failed: %v", err)
	}
	defer s.Close()

	w := NewSlabWriter(s, 128)
	defer w.Close()

	type record struct {
		offset int64
		data   []byte
	}

	var (
		mu      sync.Mutex
		records []record
		wg      sync.WaitGroup
	)

	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			data := make([]byte, 32)
			copy(data, fmt.Sprintf("w%03d", i))
			off, err := w.Write(data)
			if err != nil {
				t.Errorf("Write failed: %v", err)
				return
			}
			mu.Lock()
			records = append(records, record{offset: off, data: append([]byte(nil), data...)})
			mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			data := make([]byte, 64)
			copy(data, fmt.Sprintf("b%03d", i))
			off, err := w.WriteBatch(data, true)
			if err != nil {
				t.Errorf("WriteBatch failed: %v", err)
				return
			}
			mu.Lock()
			records = append(records, record{offset: off, data: append([]byte(nil), data...)})
			mu.Unlock()
		}
	}()
	wg.Wait()

	if err := w.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	for _, rec := range records {
		readBuf := make([]byte, len(rec.data))
		if _, err := s.File.ReadAt(readBuf, rec.offset); err != nil {
			t.Fatalf("ReadAt failed at %d: %v", rec.offset, err)
		}
		if !bytes.Equal(readBuf, rec.data) {
			t.Fatalf("data mismatch at %d", rec.offset)
		}
	}
}

func TestSlabWriter_RotateWhileFlushInFlight(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data-0000.slab")
	s, err := OpenSlab(path, 0)
	if err != nil {
		t.Fatalf("OpenSlab failed: %v", err)
	}
	defer s.Close()

	bufSize := 64
	w := NewSlabWriter(s, bufSize)
	defer w.Close()

	type record struct {
		offset int64
		data   []byte
	}
	var (
		mu      sync.Mutex
		records []record
	)

	w.ioMu.Lock()

	data1 := bytes.Repeat([]byte{1}, bufSize)
	off1, err := w.Write(data1)
	if err != nil {
		t.Fatalf("Write 1 failed: %v", err)
	}
	mu.Lock()
	records = append(records, record{offset: off1, data: append([]byte(nil), data1...)})
	mu.Unlock()

	data2 := []byte{2}
	off2, err := w.Write(data2)
	if err != nil {
		t.Fatalf("Write 2 failed: %v", err)
	}
	mu.Lock()
	records = append(records, record{offset: off2, data: append([]byte(nil), data2...)})
	mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		data3 := bytes.Repeat([]byte{3}, bufSize)
		off3, err := w.Write(data3)
		if err != nil {
			errCh <- err
			return
		}
		mu.Lock()
		records = append(records, record{offset: off3, data: append([]byte(nil), data3...)})
		mu.Unlock()

		data4 := []byte{4}
		off4, err := w.Write(data4)
		if err != nil {
			errCh <- err
			return
		}
		mu.Lock()
		records = append(records, record{offset: off4, data: append([]byte(nil), data4...)})
		mu.Unlock()
		errCh <- nil
	}()

	time.Sleep(50 * time.Millisecond)
	w.ioMu.Unlock()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("write during flush failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("write during flush did not complete")
	}

	if err := w.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	for _, rec := range records {
		readBuf := make([]byte, len(rec.data))
		if _, err := s.File.ReadAt(readBuf, rec.offset); err != nil {
			t.Fatalf("ReadAt failed at %d: %v", rec.offset, err)
		}
		if !bytes.Equal(readBuf, rec.data) {
			t.Fatalf("data mismatch at %d", rec.offset)
		}
	}
}
