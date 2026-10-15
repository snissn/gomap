package pager

import (
	"errors"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestReadOnlyOwnedMetadataAdmissionAndLifetime(t *testing.T) {
	for _, kind := range []string{"success", "preadmission-refusal", "extent-changed", "actual-close-refusal"} {
		t.Run(kind, func(t *testing.T) {
			f, err := os.CreateTemp(t.TempDir(), "original")
			if err != nil {
				t.Fatal(err)
			}
			chunk := int64(os.Getpagesize()) * page.PageSize
			if err := f.Truncate(chunk + page.PageSize); err != nil {
				t.Fatal(err)
			}
			owner := new(retainedalloc.Owner)
			owner.Initialize(0)
			if kind == "preadmission-refusal" {
				owner.Close()
			}
			if kind == "extent-changed" {
				testHookReadOnlyAfterAdmission = func(file *os.File) {
					if err := file.Truncate(2*chunk + page.PageSize); err != nil {
						t.Fatal(err)
					}
				}
			}
			if kind == "actual-close-refusal" {
				testHookReadOnlyAfterAdmission = func(file *os.File) {
					if err := file.Close(); err != nil {
						t.Fatal(err)
					}
				}
			}
			defer func() { testHookReadOnlyAfterAdmission = nil }()
			p, charge, consumed, err := OpenReadOnlyOwnedFileWithMetadata(f, f.Name(), chunk, OpenOptions{}, owner)
			switch kind {
			case "success":
				if err != nil || p == nil || !consumed || charge == 0 || owner.Bytes() != charge {
					t.Fatalf("p=%p charge=%d consumed=%v err=%v owner=%d", p, charge, consumed, err, owner.Bytes())
				}
				if len(p.chunks) != 2 || !p.readOnly {
					t.Fatal("wrong actual readonly backing")
				}
				if err := p.Close(); err != nil {
					t.Fatal(err)
				}
				p = nil
				owner.RemovePending(charge)
				if owner.Bytes() != 0 {
					t.Fatal("successful alias consumption did not drain")
				}
			case "preadmission-refusal":
				if !errors.Is(err, retainedalloc.ErrClosed) || p != nil || charge != 0 || consumed {
					t.Fatalf("p=%p charge=%d consumed=%v err=%v", p, charge, consumed, err)
				}
				if _, err := f.Stat(); err != nil {
					t.Fatal("preadmission consumed original file", err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			case "extent-changed":
				if !errors.Is(err, ErrFileSize) || p != nil || charge != 0 || !consumed || owner.Bytes() != 0 {
					t.Fatalf("p=%p charge=%d consumed=%v err=%v owner=%d", p, charge, consumed, err, owner.Bytes())
				}
				if err := f.Close(); !errors.Is(err, os.ErrClosed) {
					t.Fatal("consumed exact file remains open", err)
				}
			case "actual-close-refusal":
				if !errors.Is(err, os.ErrClosed) || p == nil || charge == 0 || !consumed || owner.Bytes() != charge || p.file != f {
					t.Fatalf("p=%p charge=%d consumed=%v err=%v owner=%d", p, charge, consumed, err, owner.Bytes())
				}
				if len(p.chunks) != 0 || p.atomicChunks.Load() != nil {
					t.Fatal("construction allocated extent backing after original Stat refusal")
				}
				// The caller retains this exact payload/charge; never replay uncertain Close.
			}
		})
	}
}

func TestReadOnlyOriginalPathRetainsPartialCleanupInError(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "original")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	if err := f.Truncate(page.PageSize); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	var original *os.File
	calls := 0
	testHookReadOnlyBeforeInitialize = func(file *os.File) {
		original = file
		calls++
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	defer func() { testHookReadOnlyBeforeInitialize = nil }()
	p, err := OpenReadOnly(path, int64(os.Getpagesize())*page.PageSize)
	var receipt *ReadOnlyCleanupError
	if p != nil || !errors.As(err, &receipt) || !errors.Is(err, os.ErrClosed) || receipt.pager == nil || receipt.pager.file != original {
		t.Fatalf("pager=%p receipt=%p err=%v", p, receipt, err)
	}
	first := receipt.cause
	for i := 0; i < 3; i++ {
		if receipt.Close() != first || receipt.pager.file != original || calls != 1 {
			t.Fatal("original cleanup receipt replayed or lost payload")
		}
	}
	if receipt.pager.atomicChunks.Load() != nil || len(receipt.pager.chunks) != 0 {
		t.Fatal("failed constructor published read aliases")
	}
}
