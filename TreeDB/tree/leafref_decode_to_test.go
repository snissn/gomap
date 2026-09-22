package tree

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type trackedLeafRefReader struct {
	pageBytes []byte
	useDst    bool

	readUnsafeCalls       int
	readUnsafeAppendCalls int
	readUnsafeToCalls     int
}

func (r *trackedLeafRefReader) Read(ptr page.ValuePtr) ([]byte, error) {
	return r.ReadUnsafe(ptr)
}

func (r *trackedLeafRefReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	r.readUnsafeCalls++
	return append([]byte(nil), r.pageBytes...), nil
}

func (r *trackedLeafRefReader) ReadUnsafeAppend(ptr page.ValuePtr, dst []byte) ([]byte, error) {
	r.readUnsafeAppendCalls++
	return append(dst, r.pageBytes...), nil
}

func (r *trackedLeafRefReader) ReadUnsafeTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error) {
	r.readUnsafeToCalls++
	if !r.useDst {
		return r.pageBytes, false, nil
	}
	if cap(dst) < len(r.pageBytes) {
		dst = make([]byte, len(r.pageBytes))
	} else {
		dst = dst[:len(r.pageBytes)]
	}
	copy(dst, r.pageBytes)
	return dst, true, nil
}

func TestTreeGetAppend_LeafRefPrefersReadUnsafeTo(t *testing.T) {
	leafPtr := page.LeafLogPtr{
		Offset: 4,
		FileID: 1,
	}

	leaf := make([]byte, page.PageSize)
	n := node.NewNode(leaf)
	n.SetType(page.PageTypeLeaf)
	n.SetPageID(0)
	n.AddLeafEntry([]byte("k"), []byte("v"), node.FlagInline, page.ValuePtr{})
	n.UpdateChecksum()

	tracked := &trackedLeafRefReader{
		pageBytes: leaf,
		useDst:    true,
	}
	tr, closeTree := newTreeWithLeafLogRoot(t, tracked, []byte{}, leafPtr)
	defer closeTree()

	got, err := tr.GetAppend([]byte("k"), nil)
	if err != nil {
		t.Fatalf("GetAppend failed: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("unexpected value: %q", got)
	}
	if tracked.readUnsafeToCalls != 1 {
		t.Fatalf("expected ReadUnsafeTo to be used once, got %d", tracked.readUnsafeToCalls)
	}
	if tracked.readUnsafeAppendCalls != 0 {
		t.Fatalf("expected ReadUnsafeAppend to be bypassed, got %d", tracked.readUnsafeAppendCalls)
	}
	if tracked.readUnsafeCalls != 0 {
		t.Fatalf("expected ReadUnsafe to be bypassed, got %d", tracked.readUnsafeCalls)
	}
}
