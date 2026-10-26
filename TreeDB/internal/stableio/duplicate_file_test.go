//go:build darwin || linux || freebsd || netbsd || openbsd || windows

package stableio

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDuplicateFileOwnsExactObject(t *testing.T) {
	original, err := os.Create(filepath.Join(t.TempDir(), "original"))
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()
	if _, err := original.WriteAt([]byte("retained"), 0); err != nil {
		t.Fatal(err)
	}
	duplicate, err := DuplicateFile(original, "diagnostic-only")
	if err != nil {
		t.Fatal(err)
	}
	defer duplicate.Close()
	if duplicate.Name() != "diagnostic-only" || duplicate == original || duplicate.Fd() == original.Fd() {
		t.Fatal("duplicate did not acquire independent descriptor/provenance")
	}
	a, err := original.Stat()
	if err != nil {
		t.Fatal(err)
	}
	b, err := duplicate.Stat()
	if err != nil || !os.SameFile(a, b) {
		t.Fatalf("duplicate rebound exact file identity: %v", err)
	}
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 8)
	if _, err := duplicate.ReadAt(got, 0); err != nil || string(got) != "retained" {
		t.Fatalf("source Close consumed duplicate: %q %v", got, err)
	}
	if err := duplicate.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := duplicate.ReadAt(got, 0); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("duplicate cleanup=%v", err)
	}
}

func TestDuplicateFileRejectsNilAndClosed(t *testing.T) {
	if file, err := DuplicateFile(nil, "nil"); file != nil || !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("nil duplicate=%v %v", file, err)
	}
	file, err := os.CreateTemp(t.TempDir(), "closed")
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if duplicate, err := DuplicateFile(file, "closed"); err == nil || duplicate != nil {
		t.Fatalf("closed duplicate=%v %v", duplicate, err)
	}
}

func TestDuplicateFileConcurrentCloseRetainsExactObject(t *testing.T) {
	for range 64 {
		file, err := os.CreateTemp(t.TempDir(), "source")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.WriteAt([]byte("retained"), 0); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		identity, err := file.Stat()
		if err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		start := make(chan struct{})
		closed := make(chan error, 1)
		go func() {
			<-start
			closed <- file.Close()
		}()
		close(start)
		duplicate, duplicateErr := DuplicateFile(file, "concurrent-close")
		if err := <-closed; err != nil {
			t.Fatal(err)
		}
		if duplicateErr != nil {
			if duplicate != nil {
				t.Fatalf("concurrent closed duplicate=%v %v", duplicate, duplicateErr)
			}
			if _, err := file.ReadAt(make([]byte, 8), 0); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("source Close did not finish: %v", err)
			}
			continue
		}
		got := make([]byte, 8)
		info, statErr := duplicate.Stat()
		_, readErr := duplicate.ReadAt(got, 0)
		closeErr := duplicate.Close()
		if statErr != nil || !os.SameFile(identity, info) || readErr != nil || string(got) != "retained" || closeErr != nil {
			t.Fatalf("duplicate lost source identity: stat=%v read=%q/%v close=%v", statErr, got, readErr, closeErr)
		}
	}
}
