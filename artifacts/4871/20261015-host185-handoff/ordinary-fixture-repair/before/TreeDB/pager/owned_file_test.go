package pager

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestOpenOwnedFileUsesTransferredHandle(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source")
	f, err := os.OpenFile(source, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	diagnostic := filepath.Join(dir, "foreign")
	foreign := []byte("foreign identity must remain untouched")
	if err := os.WriteFile(diagnostic, foreign, 0600); err != nil {
		t.Fatal(err)
	}
	p, err := OpenOwnedFileWithOptions(f, diagnostic, 16*int64(os.Getpagesize())*page.PageSize, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.GrowTo(2); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("transferred file remains open: %v", err)
	}
	got, err := os.ReadFile(diagnostic)
	if err != nil || string(got) != string(foreign) {
		t.Fatalf("diagnostic child changed: %q %v", got, err)
	}
}

func TestOpenOwnedFileFailureTransfersCleanup(t *testing.T) {
	t.Run("invalid geometry", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "owned")
		if err != nil {
			t.Fatal(err)
		}
		p, err := OpenOwnedFileWithOptions(f, "unused", 1, OpenOptions{})
		if err == nil || p != nil {
			t.Fatalf("p=%p err=%v", p, err)
		}
		if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("file not closed: %v", err)
		}
	})
	t.Run("already closed retains uncertain object", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "owned")
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		p, err := OpenOwnedFileWithOptions(f, "unused", int64(os.Getpagesize())*page.PageSize, OpenOptions{})
		if err == nil || p == nil {
			t.Fatalf("p=%p err=%v", p, err)
		}
		if _, err := p.Get(0); err == nil {
			t.Fatal("partial pager allows reads")
		}
		if _, err := p.GetForWrite(0); err == nil {
			t.Fatal("partial pager allows writes")
		}
		if err := p.GrowTo(2); err == nil {
			t.Fatal("partial pager allows growth")
		}
		if err := p.Close(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("actual close uncertainty lost: %v", err)
		}
	})
}

func TestOwnedFileMappingFailureClosesExactHandle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "readonly")
	chunk := int64(os.Getpagesize()) * page.PageSize
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(chunk); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	f, err = os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	p, err := OpenOwnedFileWithOptions(f, "unused", chunk, OpenOptions{})
	if err == nil || p != nil {
		t.Fatalf("read-only mapping unexpectedly initialized: %p %v", p, err)
	}
	if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("failed mapping lost file: %v", err)
	}
}

func TestPagerPartialUnmapRetainsMappingAndBlocksAccess(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "mapping")
	if err != nil {
		t.Fatal(err)
	}
	p, err := OpenOwnedFileWithOptions(f, "unused", int64(os.Getpagesize())*page.PageSize, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.GrowTo(2); err != nil {
		t.Fatal(err)
	}
	mapping := p.chunks[0]
	// A non-mapping makes the real platform unmap fail; retain the genuine
	// mapping in this test until restoring the same cleanup operand for retry.
	p.chunks[0] = make([]byte, len(mapping))
	if err := p.Close(); err == nil {
		t.Fatal("invalid mapping unmap succeeded")
	}
	if _, err := p.Get(0); err == nil {
		t.Fatal("partial cleanup publishes mapping")
	}
	if _, err := p.GetForWrite(0); err == nil {
		t.Fatal("partial cleanup permits write")
	}
	if _, _, err := p.WriteViewWithWork(0, make([]byte, page.PageSize), nil); err == nil {
		t.Fatal("partial cleanup permits work write")
	}
	if err := p.GrowTo(3); err == nil {
		t.Fatal("partial cleanup permits growth")
	}
	if _, err := f.Stat(); err != nil {
		t.Fatalf("actual file lost after unmap failure: %v", err)
	}
	p.chunks[0] = mapping
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("cleanup did not close exact handle: %v", err)
	}
}
