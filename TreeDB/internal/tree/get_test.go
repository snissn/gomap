package tree

import (
	"bytes"
	"encoding/binary"
	"testing"

	"treedb/internal/page"
	"treedb/internal/pager"
)

func makeBigKey(i uint64) []byte {
	key := make([]byte, 500)
	binary.BigEndian.PutUint64(key[:8], i)
	return key
}

func makeBigValue(i uint64) []byte {
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], i)
	return v[:]
}

func TestGetRawInlineValueIsCopy(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(dir, 2*page.PageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = p.Close() }()

	tr := NewUserTree(p, 0)
	key := []byte("k")
	if _, _, err := tr.SetRaw(key, LeafEntry{
		Flags:       page.LeafFlagInline,
		InlineValue: []byte{1, 2, 3},
	}); err != nil {
		t.Fatalf("set: %v", err)
	}
	ent, err := tr.GetRaw(key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if ent.Flags != page.LeafFlagInline || len(ent.InlineValue) != 3 {
		t.Fatalf("unexpected entry: %+v", ent)
	}

	ent.InlineValue[0] = 9
	ent2, err := tr.GetRaw(key)
	if err != nil {
		t.Fatalf("get2: %v", err)
	}
	if ent2.InlineValue[0] != 1 {
		t.Fatalf("expected stored value unchanged, got %v", ent2.InlineValue)
	}
}

func TestGetRawExactBoundaryKey(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(dir, 2*page.PageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = p.Close() }()

	tr := NewUserTree(p, 0)
	for i := 0; i < 200; i++ {
		key := makeBigKey(uint64(i))
		val := makeBigValue(uint64(i))
		if _, _, err := tr.SetRaw(key, LeafEntry{
			Flags:       page.LeafFlagInline,
			InlineValue: val,
		}); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}

	root := tr.Root()
	ref, err := p.ReadPageRef(root)
	if err != nil {
		t.Fatalf("read root: %v", err)
	}
	buf := ref.Bytes()
	h, body, err := page.SplitPage(buf)
	if err != nil {
		ref.Release()
		t.Fatalf("split root: %v", err)
	}
	if err := h.VerifyBodyCRC(body); err != nil {
		ref.Release()
		t.Fatalf("root crc: %v", err)
	}
	if h.Flags != page.PageTypeInternal {
		ref.Release()
		t.Fatalf("expected internal root, got %v", h.Flags)
	}
	ip, err := page.OpenInternalPage(buf)
	if err != nil {
		ref.Release()
		t.Fatalf("open internal: %v", err)
	}
	if ip.Count() < 2 {
		ref.Release()
		t.Fatalf("expected root with >=2 entries, got %d", ip.Count())
	}
	encKey, _, err := ip.EntryAt(1)
	ref.Release()
	if err != nil {
		t.Fatalf("entry at: %v", err)
	}
	boundaryKey := decodeUserKey(encKey)
	wantIdx := binary.BigEndian.Uint64(boundaryKey[:8])
	wantVal := makeBigValue(wantIdx)

	ent, err := tr.GetRaw(boundaryKey)
	if err != nil {
		t.Fatalf("get boundary: %v", err)
	}
	if ent.Flags != page.LeafFlagInline || !bytes.Equal(ent.InlineValue, wantVal) {
		t.Fatalf("boundary mismatch: got=%v want=%v", ent.InlineValue, wantVal)
	}
}

func TestGetRawDeepTree(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(dir, 2*page.PageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = p.Close() }()

	tr := NewUserTree(p, 0)
	const n = 600
	for i := 0; i < n; i++ {
		key := makeBigKey(uint64(i))
		val := makeBigValue(uint64(i))
		if _, _, err := tr.SetRaw(key, LeafEntry{
			Flags:       page.LeafFlagInline,
			InlineValue: val,
		}); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}

	depth, err := treeDepth(p, tr.Root())
	if err != nil {
		t.Fatalf("depth: %v", err)
	}
	if depth < 3 {
		t.Fatalf("expected depth >= 3, got %d", depth)
	}

	for _, idx := range []int{0, n / 3, 2 * n / 3, n - 1} {
		key := makeBigKey(uint64(idx))
		wantVal := makeBigValue(uint64(idx))
		ent, err := tr.GetRaw(key)
		if err != nil {
			t.Fatalf("get %d: %v", idx, err)
		}
		if ent.Flags != page.LeafFlagInline || !bytes.Equal(ent.InlineValue, wantVal) {
			t.Fatalf("value mismatch at %d: got=%v want=%v", idx, ent.InlineValue, wantVal)
		}
	}
}

func treeDepth(p *pager.Pager, root page.PageID) (int, error) {
	pid := root
	depth := 0
	for pid != 0 {
		ref, err := p.ReadPageRef(pid)
		if err != nil {
			return 0, err
		}
		buf := ref.Bytes()
		h, body, err := page.SplitPage(buf)
		if err != nil {
			ref.Release()
			return 0, err
		}
		if err := h.VerifyBodyCRC(body); err != nil {
			ref.Release()
			return 0, err
		}
		depth++
		switch h.Flags {
		case page.PageTypeLeaf:
			ref.Release()
			return depth, nil
		case page.PageTypeInternal:
			ip, err := page.OpenInternalPage(buf)
			if err != nil {
				ref.Release()
				return 0, err
			}
			next, err := ip.ChildAt(0)
			ref.Release()
			if err != nil {
				return 0, err
			}
			pid = next
		default:
			ref.Release()
			return 0, ErrCorrupt
		}
	}
	return depth, nil
}
