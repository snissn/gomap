package valuelog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
)

var (
	ErrStableSnapshotUnsupported = errors.New("valuelog: stable writer snapshots unsupported")
	ErrStableSnapshotClosed      = errors.New("valuelog: stable writer snapshot closed")
	ErrStableSnapshotFrontier    = errors.New("valuelog: stable writer snapshot frontier mismatch")
)

// StableWriterSnapshot is an opaque lease on one open value-log file identity.
// It owns a duplicated descriptor, so rotation or path replacement cannot
// retarget durability callbacks to a newer segment.
type StableWriterSnapshot struct {
	mu       sync.RWMutex
	f        *os.File
	path     string
	fileID   uint32
	frontier uint64
	once     sync.Once
}

// CaptureStableSnapshot flushes the current append buffer and captures the
// exact open file and frontier. The caller must serialize this call with all
// append, flush, sync, close, and rotation operations on w.
func (w *Writer) CaptureStableSnapshot(path string) (*StableWriterSnapshot, error) {
	if w == nil || w.f == nil || w.fileID == 0 {
		return nil, fmt.Errorf("%w: writer is not file-backed", ErrStableSnapshotUnsupported)
	}
	if err := w.flushNoTrim(); err != nil {
		return nil, err
	}
	frontier := w.size
	if frontier <= 0 {
		return nil, fmt.Errorf("%w: invalid frontier %d", ErrStableSnapshotFrontier, frontier)
	}
	duplicate, err := duplicateStableFile(w.f)
	if err != nil {
		return nil, fmt.Errorf("valuelog: duplicate stable writer descriptor: %w", err)
	}
	info, err := duplicate.Stat()
	if err != nil {
		_ = duplicate.Close()
		return nil, err
	}
	if info.Size() < frontier {
		_ = duplicate.Close()
		return nil, fmt.Errorf("%w: descriptor length %d below captured frontier %d", ErrStableSnapshotFrontier, info.Size(), frontier)
	}
	return &StableWriterSnapshot{
		f:        duplicate,
		path:     path,
		fileID:   w.fileID,
		frontier: uint64(frontier),
	}, nil
}

func (s *StableWriterSnapshot) StableIdentity() string {
	if s == nil {
		return ""
	}
	return fmt.Sprintf("value-log:%08x", s.fileID)
}

func (s *StableWriterSnapshot) StableGeneration() uint64 {
	if s == nil {
		return 0
	}
	return uint64(s.fileID)
}

func (s *StableWriterSnapshot) StableDiagnosticPath() string {
	if s == nil {
		return ""
	}
	return s.path
}

func (s *StableWriterSnapshot) Frontier() uint64 {
	if s == nil {
		return 0
	}
	return s.frontier
}

func (s *StableWriterSnapshot) FileID() uint32 {
	if s == nil {
		return 0
	}
	return s.fileID
}

func (s *StableWriterSnapshot) FlushThrough(ctx context.Context, frontier uint64) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	return s.withFile(frontier, func(f *os.File) error {
		info, err := f.Stat()
		if err != nil {
			return err
		}
		if uint64(info.Size()) < frontier {
			return fmt.Errorf("%w: descriptor length %d below frontier %d", ErrStableSnapshotFrontier, info.Size(), frontier)
		}
		return nil
	})
}

func (s *StableWriterSnapshot) SyncThrough(ctx context.Context, frontier uint64) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	return s.withFile(frontier, func(f *os.File) error {
		info, err := f.Stat()
		if err != nil {
			return err
		}
		if uint64(info.Size()) < frontier {
			return fmt.Errorf("%w: descriptor length %d below frontier %d", ErrStableSnapshotFrontier, info.Size(), frontier)
		}
		return f.Sync()
	})
}

func (s *StableWriterSnapshot) withFile(frontier uint64, fn func(*os.File) error) error {
	if s == nil {
		return ErrStableSnapshotClosed
	}
	if frontier != s.frontier {
		return fmt.Errorf("%w: requested %d captured %d", ErrStableSnapshotFrontier, frontier, s.frontier)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.f == nil {
		return ErrStableSnapshotClosed
	}
	return fn(s.f)
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

// Release closes the captured descriptor exactly once.
func (s *StableWriterSnapshot) Release() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		s.mu.Lock()
		if s.f != nil {
			_ = s.f.Close()
			s.f = nil
		}
		s.mu.Unlock()
	})
}
