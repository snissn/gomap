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

// Lock is one independently releasable ownership of an OS directory lock.
// Its shared state and ownership transitions are protected by processMu.
type Lock struct{ state *lockState }

type lockState struct {
	f      *os.File
	path   string
	owners uint64
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

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
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

	return &Lock{state: &lockState{f: f, path: path, owners: 1}}, nil
}

// AcquireShared attempts to take a shared (read) lock on an existing lock file.
// It never creates or writes to the lock file.
func AcquireShared(path string) (*Lock, error) {
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

	f, err := os.Open(path)
	if err != nil {
		processMu.Lock()
		delete(processLocks, path)
		processMu.Unlock()
		return nil, err
	}

	if err := lockFileShared(f); err != nil {
		_ = f.Close()
		processMu.Lock()
		delete(processLocks, path)
		processMu.Unlock()
		if errors.Is(err, ErrLocked) {
			return nil, fmt.Errorf("%w: %s", ErrLocked, path)
		}
		return nil, err
	}

	return &Lock{state: &lockState{f: f, path: path, owners: 1}}, nil
}

// Retain preserves both the OS lock and process-local registration until this
// independent ownership is closed. It does not reopen the lock's pathname.
func (l *Lock) Retain() (*Lock, error) {
	processMu.Lock()
	defer processMu.Unlock()
	if l == nil || l.state == nil {
		return nil, os.ErrClosed
	}
	l.state.owners++
	return &Lock{state: l.state}, nil
}

func (l *Lock) Close() error {
	processMu.Lock()
	defer processMu.Unlock()
	if l == nil || l.state == nil {
		return nil
	}
	state := l.state
	l.state = nil
	state.owners--
	if state.owners != 0 {
		return nil
	}
	// Keep the process registration through the OS unlock/close transition.
	// No export writer or other caller work executes under this mutex.
	unlockErr := unlockFile(state.f)
	closeErr := state.f.Close()
	delete(processLocks, state.path)
	return errors.Join(unlockErr, closeErr)
}

// SameFile verifies that a separately opened, parent-relative entry is the
// exact file owned by this lock. It does not reopen the lock's pathname. This
// lets a caller bind a retained directory handle to a pathname-acquired lock
// before using that handle for mutation.
func (l *Lock) SameFile(file *os.File) (bool, error) {
	processMu.Lock()
	defer processMu.Unlock()
	if l == nil || l.state == nil || file == nil {
		return false, os.ErrClosed
	}
	locked, err := l.state.f.Stat()
	if err != nil {
		return false, err
	}
	candidate, err := file.Stat()
	if err != nil {
		return false, err
	}
	return os.SameFile(locked, candidate), nil
}
