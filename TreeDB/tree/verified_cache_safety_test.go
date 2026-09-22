package tree

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestVerifiedPageCache_SafetyTradeoff(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 64*1024)
	if err != nil {
		t.Fatalf("Pager open failed: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(1); err != nil {
		t.Fatalf("Alloc: %v", err)
	}

	data, _ := p.GetForWrite(0)
	n := node.NewNode(data)
	n.SetType(page.PageTypeLeaf)
	n.SetPageID(0)
	n.AddLeafEntry([]byte("k"), []byte("value"), node.FlagInline, page.ValuePtr{})
	n.UpdateChecksum()

	tr := New(p, panicValueReader{}, 0)

	// First read verifies checksum and caches the page as "verified".
	orig, err := tr.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(orig, []byte("value")) {
		t.Fatalf("unexpected original value: %q", orig)
	}
	if !p.IsVerified(0) {
		t.Fatalf("expected page 0 to be marked verified after read")
	}

	// Corrupt in-memory page bytes after verification. We intentionally mutate an
	// inline value byte so lookups still succeed but CRC would fail.
	dataCorrupt, _ := p.Get(0)
	nCorrupt := node.NewNode(dataCorrupt)
	idx, found, err := nCorrupt.SearchLeaf([]byte("k"))
	if err != nil {
		t.Fatalf("SearchLeaf: %v", err)
	}
	if !found {
		t.Fatalf("expected key to be present")
	}
	_, valueView, _, _, err := nCorrupt.GetLeafEntryView(idx)
	if err != nil {
		t.Fatalf("GetLeafEntryView: %v", err)
	}
	valueView[0] ^= 0xff

	// With the verified bit still set, Tree skips CRC verification and returns
	// the corrupted value without error.
	got, err := tr.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get after corruption (cache hit): %v", err)
	}
	if bytes.Equal(got, orig) {
		t.Fatalf("expected corrupted value to differ from original")
	}

	// Clearing the verified bit forces re-verification, which must detect the
	// corruption.
	p.MarkUnverified(0)
	_, err = tr.Get([]byte("k"))
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum mismatch after cache cleared, got: %v", err)
	}
}
