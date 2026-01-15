package slab

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func TestSlabWriter_ConcurrentWrites_Rotation_NoLoss(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data-0000.slab")
	s, err := OpenSlab(path, 0)
	if err != nil {
		t.Fatalf("OpenSlab failed: %v", err)
	}
	defer s.Close()

	const (
		bufferSize  = 128
		recordSize  = 64
		goroutines  = 4
		recordsEach = 200
	)

	w := NewSlabWriter(s, bufferSize)
	defer w.Close()

	type record struct {
		offset int64
		data   []byte
	}

	var (
		mu      sync.Mutex
		records []record
		errCh   = make(chan error, goroutines)
		wg      sync.WaitGroup
	)

	for g := 0; g < goroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < recordsEach; i++ {
				data := make([]byte, recordSize)
				prefix := fmt.Sprintf("g%02d-%04d", g, i)
				copy(data, prefix)
				for j := len(prefix); j < len(data); j++ {
					data[j] = byte(g + i + j)
				}
				offset, err := w.Write(data)
				if err != nil {
					errCh <- err
					return
				}
				mu.Lock()
				records = append(records, record{offset: offset, data: append([]byte(nil), data...)})
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
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
