package raftapply

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
)

// DurableSnapshotPrefixV1 retains a synced complete-frame prefix of one
// append-only apply metadata file. Later appends and closing the source store
// do not change the captured bytes. It owns an independently opened handle.
type DurableSnapshotPrefixV1 struct {
	mu   sync.Mutex
	file *os.File
	size int64
}

func (s *DurableApplyProgressStore) CaptureSnapshotPrefixV1() (*DurableSnapshotPrefixV1, error) {
	if s == nil {
		return nil, os.ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpenLocked("capture snapshot progress prefix"); err != nil {
		return nil, err
	}
	return captureDurableSnapshotPrefixV1(s.file)
}

func (s *DurableApplyResultStore) CaptureSnapshotPrefixV1() (*DurableSnapshotPrefixV1, error) {
	if s == nil {
		return nil, os.ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpenLocked("capture snapshot result prefix"); err != nil {
		return nil, err
	}
	return captureDurableSnapshotPrefixV1(s.file)
}

// The store lock excludes appends. Failed appends poison/close the store, so a
// successfully admitted capture observes only complete frames, including when
// per-append sync was disabled. No record-map copy or log scan is needed.
func captureDurableSnapshotPrefixV1(source *os.File) (*DurableSnapshotPrefixV1, error) {
	if err := source.Sync(); err != nil {
		return nil, err
	}
	info, err := source.Stat()
	if err != nil {
		return nil, err
	}
	file, err := os.Open(source.Name())
	if err != nil {
		return nil, err
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, errors.Join(err, errors.New("raftapply: snapshot metadata identity changed"), file.Close())
	}
	return &DurableSnapshotPrefixV1{file: file, size: info.Size()}, nil
}

func (p *DurableSnapshotPrefixV1) SizeBytes() int64 { return p.size }

func (p *DurableSnapshotPrefixV1) WriteToContext(ctx context.Context, dst io.Writer) error {
	if p == nil {
		return os.ErrClosed
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.file == nil {
		return os.ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var buffer [64 * 1024]byte
	for offset := int64(0); offset < p.size; {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := min(int64(len(buffer)), p.size-offset)
		if _, err := p.file.ReadAt(buffer[:n], offset); err != nil {
			return err
		}
		written, err := dst.Write(buffer[:n])
		if err != nil {
			return err
		}
		if int64(written) != n {
			return io.ErrShortWrite
		}
		offset += n
	}
	return ctx.Err()
}

func (p *DurableSnapshotPrefixV1) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.file == nil {
		return nil
	}
	err := p.file.Close()
	p.file = nil
	return err
}
