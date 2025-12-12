package tree

import (
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
