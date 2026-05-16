package lockfile

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAcquireRejectsSecondExclusiveUntilClose(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "db.lock")

	l1, err := Acquire(filepath.Join(dir, ".", "db.lock"))
	if err != nil {
		t.Fatalf("Acquire(first): %v", err)
	}
	t.Cleanup(func() { _ = l1.Close() })

	_, err = Acquire(lockPath)
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("Acquire(second) err=%v, want ErrLocked", err)
	}

	if err := l1.Close(); err != nil {
		t.Fatalf("Close(first): %v", err)
	}

	l2, err := Acquire(lockPath)
	if err != nil {
		t.Fatalf("Acquire(after close): %v", err)
	}
	defer func() { _ = l2.Close() }()
}

func TestAcquireWritesPIDMarker(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "db.lock")

	l, err := Acquire(lockPath)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer func() { _ = l.Close() }()

	if _, err := l.f.Seek(0, 0); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	b, err := io.ReadAll(l.f)
	if err != nil {
		t.Fatalf("ReadAll(lock fd): %v", err)
	}
	if !strings.HasPrefix(string(b), "pid=") {
		t.Fatalf("lock file content %q, want prefix pid=", string(b))
	}
}

func TestAcquireSharedMissingFile(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "missing.lock")

	_, err := AcquireShared(lockPath)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("AcquireShared(missing) err=%v, want os.ErrNotExist", err)
	}
}

func TestAcquireSharedConflictsWithHeldLock(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "db.lock")

	// Ensure lock file exists for AcquireShared.
	if err := os.WriteFile(lockPath, []byte(""), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	l, err := Acquire(lockPath)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer func() { _ = l.Close() }()

	_, err = AcquireShared(lockPath)
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("AcquireShared while exclusive held err=%v, want ErrLocked", err)
	}
}

func TestLockCloseNilSafe(t *testing.T) {
	var l *Lock
	if err := l.Close(); err != nil {
		t.Fatalf("(*Lock)(nil).Close() err=%v, want nil", err)
	}
}
