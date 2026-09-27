package lockfile

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestRetainedLockPreservesOSAndProcessOwnership(t *testing.T) {
	path := filepath.Join(t.TempDir(), "LOCK")
	owner, err := Acquire(path)
	if errors.Is(err, ErrUnsupported) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	first, err := owner.Retain()
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := owner.Retain()
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Retain(); !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
	contender, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer contender.Close()
	assertLocked := func() {
		if unexpected, err := Acquire(path); !errors.Is(err, ErrLocked) {
			if unexpected != nil {
				_ = unexpected.Close()
			}
			t.Fatalf("process registration released early: %v", err)
		}
		if err := lockFile(contender); !errors.Is(err, ErrLocked) {
			_ = unlockFile(contender)
			t.Fatalf("OS lock released early: %v", err)
		}
	}
	assertLocked()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	assertLocked()
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lockFile(contender); err != nil {
		t.Fatal(err)
	}
	if err := unlockFile(contender); err != nil {
		t.Fatal(err)
	}
	reopened, err := Acquire(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRetainedLockConcurrentRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "LOCK")
	owner, err := Acquire(path)
	if errors.Is(err, ErrUnsupported) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			retained, err := owner.Retain()
			if errors.Is(err, os.ErrClosed) {
				return
			}
			if err != nil {
				t.Error(err)
				return
			}
			if err := retained.Close(); err != nil {
				t.Error(err)
			}
			if err := retained.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	reopened, err := Acquire(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}
