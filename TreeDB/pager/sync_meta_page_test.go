package pager

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestSyncMetaPageImageUsesCallerBufferAndOneDataBoundary(t *testing.T) {
	p, err := Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	if _, err := p.Alloc(2); err != nil {
		t.Fatal(err)
	}
	data, err := p.GetForWrite(1)
	if err != nil {
		t.Fatal(err)
	}
	for i := range data {
		data[i] = byte(i)
	}

	originalSync := syncPageFileFn
	t.Cleanup(func() { syncPageFileFn = originalSync })
	syncCalls := 0
	syncPageFileFn = func(file *os.File) error {
		if file != p.file {
			t.Fatalf("sync file=%p want pager file=%p", file, p.file)
		}
		syncCalls++
		return nil
	}
	scratch := make([]byte, page.PageSize)
	if err := p.SyncMetaPageImage(1, scratch); err != nil {
		t.Fatal(err)
	}
	if syncCalls != 1 {
		t.Fatalf("data boundaries=%d want=1", syncCalls)
	}
	for i, got := range scratch {
		if want := byte(i); got != want {
			t.Fatalf("scratch[%d]=%d want=%d", i, got, want)
		}
	}
	if _, dirty := p.dirtyPages[1]; dirty {
		t.Fatal("durable meta page remained dirty")
	}
}

func TestSyncMetaPageImageRejectsNonMetaAndSmallScratch(t *testing.T) {
	p, err := Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	if _, err := p.Alloc(3); err != nil {
		t.Fatal(err)
	}
	if err := p.SyncMetaPageImage(2, make([]byte, page.PageSize)); !errors.Is(err, ErrPageOutOfBounds) {
		t.Fatalf("non-meta error=%v want ErrPageOutOfBounds", err)
	}
	if err := p.SyncMetaPageImage(0, make([]byte, page.PageSize-1)); err == nil {
		t.Fatal("small scratch buffer succeeded")
	}
}
