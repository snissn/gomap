package tree

import (
	"bytes"
	"errors"
	"testing"

	"treedb/internal/crc"
	"treedb/internal/page"
	"treedb/internal/pager"
)

func TestCowSetVerifiesOldPageCRCBeforeClone(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(dir, 2*page.PageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = p.Close() }()

	tr := NewUserTree(p, 0)
	if _, _, err := tr.SetRaw([]byte("a"), LeafEntry{
		Flags:       page.LeafFlagInline,
		InlineValue: []byte("v1"),
	}); err != nil {
		t.Fatalf("set initial: %v", err)
	}
	root := tr.Root()
	if root == 0 {
		t.Fatalf("expected non-zero root")
	}

	ref, err := p.ReadPageRef(root)
	if err != nil {
		t.Fatalf("read root: %v", err)
	}
	h, _, err := page.SplitPage(ref.Bytes())
	if err != nil {
		ref.Release()
		t.Fatalf("split: %v", err)
	}
	h.CRC ^= 0xFFFF_FFFF
	ref.Release()

	_, _, err = tr.SetRaw([]byte("b"), LeafEntry{
		Flags:       page.LeafFlagInline,
		InlineValue: []byte("v2"),
	})
	if !errors.Is(err, crc.ErrChecksumMismatch) {
		t.Fatalf("expected checksum mismatch, got %v", err)
	}
	if tr.Root() != root {
		t.Fatalf("expected root unchanged on error")
	}
}

func TestCowSetDoesNotMutateOldPage(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(dir, 2*page.PageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = p.Close() }()

	tr := NewUserTree(p, 0)
	if _, _, err := tr.SetRaw([]byte("a"), LeafEntry{
		Flags:       page.LeafFlagInline,
		InlineValue: []byte("v1"),
	}); err != nil {
		t.Fatalf("set initial: %v", err)
	}
	oldRoot := tr.Root()
	if oldRoot == 0 {
		t.Fatalf("expected non-zero root")
	}
	before, err := p.ReadPage(oldRoot)
	if err != nil {
		t.Fatalf("read old root: %v", err)
	}

	retired, _, err := tr.SetRaw([]byte("b"), LeafEntry{
		Flags:       page.LeafFlagInline,
		InlineValue: []byte("v2"),
	})
	if err != nil {
		t.Fatalf("set second: %v", err)
	}
	newRoot := tr.Root()
	if newRoot == oldRoot {
		t.Fatalf("expected COW root change")
	}
	if len(retired) == 0 || retired[0] != oldRoot {
		t.Fatalf("expected old root to be retired")
	}

	after, err := p.ReadPage(oldRoot)
	if err != nil {
		t.Fatalf("read old root after: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("old root page mutated in place")
	}
	h, body, err := page.SplitPage(after)
	if err != nil {
		t.Fatalf("split old root: %v", err)
	}
	if err := h.VerifyBodyCRC(body); err != nil {
		t.Fatalf("old root crc invalid: %v", err)
	}
}
