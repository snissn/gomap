package lockfile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

var (
	// ErrLocked indicates the lock is already held by another process.
	ErrLocked = errors.New("lock already held")
	// ErrUnsupported indicates file locking is not supported on this platform.
	ErrUnsupported = errors.New("file locking unsupported")
)

type Lock struct {
	f    *os.File
	path string
}

var (
	processMu    sync.Mutex
	processLocks = map[string]struct{}{}
)

func Acquire(path string) (*Lock, error) {
	path = filepath.Clean(path)
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}

	processMu.Lock()
	if _, ok := processLocks[path]; ok {
		processMu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrLocked, path)
	}
	processLocks[path] = struct{}{}
	processMu.Unlock()

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		processMu.Lock()
		delete(processLocks, path)
		processMu.Unlock()
		return nil, err
	}

	if err := lockFile(f); err != nil {
		_ = f.Close()
		processMu.Lock()
		delete(processLocks, path)
		processMu.Unlock()
		if errors.Is(err, ErrLocked) {
			return nil, fmt.Errorf("%w: %s", ErrLocked, path)
		}
		return nil, err
	}

	// Best-effort write of PID to help operators debug stale locks.
	_ = f.Truncate(0)
	_, _ = f.Seek(0, 0)
	_, _ = fmt.Fprintf(f, "pid=%d\n", os.Getpid())

	return &Lock{f: f, path: path}, nil
}

func (l *Lock) Close() error {
	if l == nil || l.f == nil {
		return nil
	}

	processMu.Lock()
	delete(processLocks, l.path)
	processMu.Unlock()

	unlockErr := unlockFile(l.f)
	closeErr := l.f.Close()
	l.f = nil

	if unlockErr != nil {
		return unlockErr
	}
	return closeErr
}
