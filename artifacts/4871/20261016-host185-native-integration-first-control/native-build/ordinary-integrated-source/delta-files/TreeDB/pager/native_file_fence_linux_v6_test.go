//go:build linux

package pager

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
	"golang.org/x/sys/unix"
)

func TestNativeWordInvalidationKeepsOrdinaryMaskedPolicyV6(t *testing.T) {
	p, err := Open(filepath.Join(t.TempDir(), "index.db"), syncPagesTestChunkSize(64))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err = p.Alloc(64); err != nil {
		t.Fatal(err)
	}
	image := bytes.Repeat([]byte{0x31}, page.PageSize)
	p.MarkVerified(0)
	p.MarkVerified(1)
	w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if ready, err := p.WriteWithWork(0, image, &w); !ready || err != nil {
		t.Fatalf("ordinary: %v %v", ready, err)
	}
	if p.IsVerified(0) || !p.IsVerified(1) {
		t.Fatal("ordinary work-bearing write changed adjacent cache bit")
	}
	p.MarkVerified(0)
	w = iterator.OrdinalScanWork{RecordLimit: 4, ByteLimit: 1 << 20}
	if ready, err := p.WriteNativeWithWorkV6(0, image, &w); ready || err == nil {
		t.Fatalf("short admission: %v %v", ready, err)
	}
	if !p.IsVerified(0) || !p.IsVerified(1) {
		t.Fatal("refusal changed cache bits")
	}
	w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if ready, err := p.WriteNativeWithWorkV6(0, image, &w); !ready || err != nil {
		t.Fatalf("native: %v %v", ready, err)
	}
	if p.IsVerified(0) || p.IsVerified(1) {
		t.Fatal("native did not conservatively invalidate original word")
	}
	// Only adjacent immutable pages are marked concurrently; this does not
	// authorize concurrent mutation and verification of the written page.
	var joined sync.WaitGroup
	joined.Add(1)
	go func() {
		defer joined.Done()
		for i := 0; i < 100; i++ {
			p.MarkVerified(1)
		}
	}()
	for i := 0; i < 100; i++ {
		w = iterator.OrdinalScanWork{RecordLimit: 5, ByteLimit: 1 << 20}
		if ready, err := p.WriteNativeWithWorkV6(0, image, &w); !ready || err != nil {
			t.Errorf("concurrent native: %v %v", ready, err)
			break
		}
	}
	joined.Wait()
	if p.IsVerified(0) {
		t.Fatal("native manufactured positive verification")
	}
}

func TestNativeRetainedFileFencePreservesDirtyAndRetiredIdentityV6(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")
	p, err := Open(path, syncPagesTestChunkSize(1))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err = p.Alloc(2); err != nil {
		t.Fatal(err)
	}
	if err = p.Write(0, bytes.Repeat([]byte{0x5a}, page.PageSize)); err != nil {
		t.Fatal(err)
	}
	retained, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer retained.Close()
	words := append([]uint64(nil), p.dirtyChunks.words...)
	count, low, high := p.dirtyChunks.count, p.dirtyChunks.low, p.dirtyChunks.high
	r, b := NativeFileFenceWorkV6()
	w := iterator.OrdinalScanWork{RecordLimit: r - 1, ByteLimit: b}
	if ready, err := p.SyncNativeIndexDataWithStableFileWithWorkV6(retained, &w); ready || err == nil {
		t.Fatalf("short fence: %v %v", ready, err)
	}
	w = iterator.OrdinalScanWork{RecordLimit: r, ByteLimit: b}
	if ready, err := p.SyncNativeIndexDataWithStableFileWithWorkV6(retained, &w); !ready || err != nil {
		t.Fatalf("fence: %v %v", ready, err)
	}
	if w.Records != r || w.Bytes != b {
		t.Fatalf("work=%d/%d want%d/%d", w.Records, w.Bytes, r, b)
	}
	if count != p.dirtyChunks.count || low != p.dirtyChunks.low || high != p.dirtyChunks.high || len(words) != len(p.dirtyChunks.words) {
		t.Fatal("native changed dirty geometry")
	}
	for i, word := range words {
		if p.dirtyChunks.words[i] != word {
			t.Fatal("native changed dirty membership")
		}
	}
	before := p.durableFileSize.Load()
	closed, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = closed.Close(); err != nil {
		t.Fatal(err)
	}
	w = iterator.OrdinalScanWork{RecordLimit: r, ByteLimit: b}
	if ready, err := p.SyncNativeIndexDataWithStableFileWithWorkV6(closed, &w); !ready || !errors.Is(err, unix.EBADF) {
		t.Fatalf("failed exact FD: %v %v", ready, err)
	}
	if p.durableFileSize.Load() != before || p.dirtyChunks.count != count {
		t.Fatal("failed barrier changed extent/membership")
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(path, path+".retired"); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	w = iterator.OrdinalScanWork{RecordLimit: r, ByteLimit: b}
	if ready, err := p.SyncNativeIndexDataWithStableFileWithWorkV6(retained, &w); !ready || err != nil {
		t.Fatalf("retired exact FD: %v %v", ready, err)
	}
	info, err := retained.Stat()
	if err != nil || p.durableFileSize.Load() != info.Size() || info.Size() <= int64(len("replacement")) {
		t.Fatalf("retired extent=%d: %v", p.durableFileSize.Load(), err)
	}
	got := make([]byte, page.PageSize)
	if _, err = retained.ReadAt(got, 0); err != nil || got[0] != 0x5a {
		t.Fatalf("retained bytes=%x %v", got[0], err)
	}
}
