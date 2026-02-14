package tree

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

type trackedValueReader struct {
	*mapValueReader
	readUnsafeCalls       int
	readUnsafeAppendCalls int
}

type fenceLookupReader struct {
	blocks     map[page.ValuePtr]map[string][]byte
	nextOffset uint64
	fileID     uint32
	fenceCalls int
}

func newFenceLookupReader() *fenceLookupReader {
	return &fenceLookupReader{
		blocks: make(map[page.ValuePtr]map[string][]byte),
		fileID: page.ValueLogFileID(1),
	}
}

func (r *fenceLookupReader) addBlock(entries map[string]string) page.ValuePtr {
	ptr := page.ValuePtr{
		FileID: r.fileID,
		Offset: r.nextOffset,
		Length: 1,
	}
	block := make(map[string][]byte, len(entries))
	for k, v := range entries {
		block[k] = []byte(v)
	}
	r.blocks[ptr] = block
	r.nextOffset++
	return ptr
}

func (r *fenceLookupReader) Read(ptr page.ValuePtr) ([]byte, error) {
	return nil, fmt.Errorf("unexpected Read for ptr %+v", ptr)
}

func (r *fenceLookupReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	return nil, fmt.Errorf("unexpected ReadUnsafe for ptr %+v", ptr)
}

func (r *fenceLookupReader) ReadUnsafeFenceForKey(ptr page.ValuePtr, key []byte) ([]byte, bool, error) {
	r.fenceCalls++
	block, ok := r.blocks[ptr]
	if !ok {
		return nil, false, fmt.Errorf("missing block ptr %+v", ptr)
	}
	val, ok := block[string(key)]
	if !ok {
		return nil, false, nil
	}
	return val, true, nil
}

func (r *trackedValueReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	r.readUnsafeCalls++
	return r.mapValueReader.ReadUnsafe(ptr)
}

func (r *trackedValueReader) ReadUnsafeAppend(ptr page.ValuePtr, dst []byte) ([]byte, error) {
	r.readUnsafeAppendCalls++
	return r.mapValueReader.ReadUnsafeAppend(ptr, dst)
}

func TestTreeGet(t *testing.T) {
	// Setup Pager
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "index.db")
	p, err := pager.Open(idxPath, 65536) // 64KB chunk (safe for 16KB pages)
	if err != nil {
		t.Fatalf("Pager open failed: %v", err)
	}
	defer p.Close()

	vr := newMapValueReader()

	// Alloc Pages
	// 0: Internal (Root)
	// 1: Leaf (Left)
	// 2: Leaf (Right)
	p0, _ := p.Alloc(1)
	p1, _ := p.Alloc(1)
	p2, _ := p.Alloc(1)

	if p0 != 0 || p1 != 1 || p2 != 2 {
		t.Fatalf("Unexpected page IDs: %d, %d, %d", p0, p1, p2)
	}

	// Build Leaf 1 (Keys "10", "40")
	data1, _ := p.Get(1)
	n1 := node.NewNode(data1)
	n1.SetType(page.PageTypeLeaf)
	n1.SetPageID(1)
	n1.AddLeafEntry([]byte("10"), []byte("val10"), node.FlagInline, page.ValuePtr{})
	n1.AddLeafEntry([]byte("40"), []byte("val40"), node.FlagInline, page.ValuePtr{})
	n1.UpdateChecksum()

	// Build Leaf 2 (Key "60", "huge")
	data2, _ := p.Get(2)
	n2 := node.NewNode(data2)
	n2.SetType(page.PageTypeLeaf)
	n2.SetPageID(2)
	n2.AddLeafEntry([]byte("60"), []byte("val60"), node.FlagInline, page.ValuePtr{})

	// Add Huge Value
	hugeVal := bytes.Repeat([]byte("A"), 1000)
	ptr := vr.Add(hugeVal)
	n2.AddLeafEntry([]byte("huge"), nil, node.FlagPointer, ptr)
	n2.UpdateChecksum()

	// Build Root (Internal)
	// Children:
	// Key "00" -> Page 1 (Covers < "50")
	// Key "50" -> Page 2 (Covers >= "50")
	// Note: Internal Entry[i].Child covers keys >= Entry[i].Key (in my impl?)
	// Wait, let's re-verify Internal Logic in node/internal.go
	// SearchInternal: "Find largest i such that Entry[i].Key <= Key"
	// So if we have:
	// Entry 0: Key="00", Child=1
	// Entry 1: Key="50", Child=2

	// Query "10":
	// "00" <= "10" (True).
	// "50" <= "10" (False).
	// Returns index 0 -> Child 1. Correct.

	// Query "60":
	// "00" <= "60" (True)
	// "50" <= "60" (True)
	// Returns index 1 -> Child 2. Correct.

	data0, _ := p.Get(0)
	n0 := node.NewNode(data0)
	n0.SetType(page.PageTypeInternal)
	n0.SetPageID(0)
	n0.AddInternalChild([]byte("00"), 1)
	n0.AddInternalChild([]byte("50"), 2)
	n0.UpdateChecksum()

	// Init Tree
	tr := New(p, vr, 0)

	// Tests
	cases := []struct {
		Key string
		Val []byte
		Err error
	}{
		{"10", []byte("val10"), nil},
		{"40", []byte("val40"), nil},
		{"60", []byte("val60"), nil},
		{"99", nil, ErrKeyNotFound},
		{"huge", hugeVal, nil},
	}

	for _, c := range cases {
		val, err := tr.Get([]byte(c.Key))
		if err != c.Err {
			t.Errorf("Get(%s): expected error %v, got %v", c.Key, c.Err, err)
		}
		if c.Err == nil && !bytes.Equal(val, c.Val) {
			t.Errorf("Get(%s): value mismatch", c.Key) // Don't print huge val
		}
	}
}

func TestTreeGet_UsesAppendReaderForPointers(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "index.db")
	p, err := pager.Open(idxPath, 65536)
	if err != nil {
		t.Fatalf("Pager open failed: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(1); err != nil {
		t.Fatalf("Alloc root: %v", err)
	}
	tracked := &trackedValueReader{mapValueReader: newMapValueReader()}
	ptr := tracked.Add([]byte("pointer-value"))

	rootData, err := p.Get(0)
	if err != nil {
		t.Fatalf("Get root page: %v", err)
	}
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeLeaf)
	root.SetPageID(0)
	root.AddLeafEntry([]byte("k"), nil, node.FlagPointer, ptr)
	root.UpdateChecksum()

	tr := New(p, tracked, 0)
	got, err := tr.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got) != "pointer-value" {
		t.Fatalf("unexpected value: %q", got)
	}
	if tracked.readUnsafeAppendCalls != 1 {
		t.Fatalf("expected ReadUnsafeAppend to be used once, got %d", tracked.readUnsafeAppendCalls)
	}
	if tracked.readUnsafeCalls != 0 {
		t.Fatalf("expected ReadUnsafe to be bypassed, got %d calls", tracked.readUnsafeCalls)
	}

	// Regression: caller-provided dst commonly has spare capacity.
	// GetAppend must not panic when probing in-place append reuse.
	prefixed := make([]byte, 1, 32)
	prefixed[0] = 'x'
	gotAppend, err := tr.GetAppend([]byte("k"), prefixed)
	if err != nil {
		t.Fatalf("GetAppend failed: %v", err)
	}
	if string(gotAppend) != "xpointer-value" {
		t.Fatalf("unexpected appended value: %q", gotAppend)
	}
}

func TestTreeGetAppend_AppendsAndUsesAppendReaderForPointers(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "index.db")
	p, err := pager.Open(idxPath, 65536)
	if err != nil {
		t.Fatalf("Pager open failed: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(1); err != nil {
		t.Fatalf("Alloc root: %v", err)
	}
	tracked := &trackedValueReader{mapValueReader: newMapValueReader()}
	ptr := tracked.Add([]byte("pointer-value"))

	rootData, _ := p.Get(0)
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeLeaf)
	root.SetPageID(0)
	root.AddLeafEntry([]byte("k"), nil, node.FlagPointer, ptr)
	root.UpdateChecksum()

	tr := New(p, tracked, 0)
	got, err := tr.GetAppend([]byte("k"), []byte("prefix:"))
	if err != nil {
		t.Fatalf("GetAppend failed: %v", err)
	}
	if string(got) != "prefix:pointer-value" {
		t.Fatalf("unexpected value: %q", got)
	}
	if tracked.readUnsafeAppendCalls != 1 {
		t.Fatalf("expected ReadUnsafeAppend to be used once, got %d", tracked.readUnsafeAppendCalls)
	}
	if tracked.readUnsafeCalls != 0 {
		t.Fatalf("expected ReadUnsafe to be bypassed, got %d calls", tracked.readUnsafeCalls)
	}
}

func TestTreeGetUnsafe_UsesUnsafeReaderForPointers(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "index.db")
	p, err := pager.Open(idxPath, 65536)
	if err != nil {
		t.Fatalf("Pager open failed: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(1); err != nil {
		t.Fatalf("Alloc root: %v", err)
	}
	tracked := &trackedValueReader{mapValueReader: newMapValueReader()}
	ptr := tracked.Add([]byte("pointer-value"))

	rootData, err := p.Get(0)
	if err != nil {
		t.Fatalf("Get root page: %v", err)
	}
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeLeaf)
	root.SetPageID(0)
	root.AddLeafEntry([]byte("k"), nil, node.FlagPointer, ptr)
	root.UpdateChecksum()

	tr := New(p, tracked, 0)
	got, err := tr.GetUnsafe([]byte("k"))
	if err != nil {
		t.Fatalf("GetUnsafe failed: %v", err)
	}
	if string(got) != "pointer-value" {
		t.Fatalf("unexpected value: %q", got)
	}
	if tracked.readUnsafeCalls != 1 {
		t.Fatalf("expected ReadUnsafe to be used once, got %d", tracked.readUnsafeCalls)
	}
	if tracked.readUnsafeAppendCalls != 0 {
		t.Fatalf("expected ReadUnsafeAppend to be bypassed, got %d calls", tracked.readUnsafeAppendCalls)
	}
}

func TestTreeGet_FencePrunesOutOfRange(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "index.db")
	p, err := pager.Open(idxPath, 65536)
	if err != nil {
		t.Fatalf("Pager open failed: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(1); err != nil {
		t.Fatalf("Alloc: %v", err)
	}

	data0, _ := p.Get(0)
	b := node.NewBuilderWithOptions(data0, page.PageTypeInternal, node.BuilderOptions{
		InternalBaseDelta: true,
	})
	b.SetPageID(0)
	b.SetInternalFenceBounds([]byte("10"), []byte("20"))
	if err := b.AddInternalChild([]byte("10"), 9999); err != nil {
		t.Fatalf("AddInternalChild: %v", err)
	}
	b.Finish()

	tr := New(p, panicValueReader{}, 0)

	if _, err := tr.Get([]byte("05")); err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound for below-low query, got %v", err)
	}
	if _, err := tr.Get([]byte("20")); err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound for high-bound query, got %v", err)
	}
	if _, err := tr.Get([]byte("15")); err == ErrKeyNotFound || err == nil {
		t.Fatalf("expected in-fence query to descend and fail differently, got %v", err)
	}
}

func TestTreeGet_FencePredecessorLookup(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "index.db")
	p, err := pager.Open(idxPath, 65536)
	if err != nil {
		t.Fatalf("Pager open failed: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(1); err != nil {
		t.Fatalf("Alloc root: %v", err)
	}

	reader := newFenceLookupReader()
	ptr0 := reader.addBlock(map[string]string{
		"k010": "v10",
		"k020": "v20",
	})
	ptr1 := reader.addBlock(map[string]string{
		"k110": "v110",
		"k120": "v120",
	})

	rootData, err := p.Get(0)
	if err != nil {
		t.Fatalf("Get root page: %v", err)
	}
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeLeaf)
	root.SetPageID(0)
	if err := root.AddLeafEntry([]byte("k010"), nil, node.FlagPointer, ptr0); err != nil {
		t.Fatalf("AddLeafEntry(k010): %v", err)
	}
	if err := root.AddLeafEntry([]byte("k110"), nil, node.FlagPointer, ptr1); err != nil {
		t.Fatalf("AddLeafEntry(k110): %v", err)
	}
	root.UpdateChecksum()

	tr := New(p, reader, 0)

	got, err := tr.Get([]byte("k020"))
	if err != nil {
		t.Fatalf("Get(k020): %v", err)
	}
	if string(got) != "v20" {
		t.Fatalf("Get(k020) = %q, want %q", got, "v20")
	}

	has, err := tr.Has([]byte("k020"))
	if err != nil {
		t.Fatalf("Has(k020): %v", err)
	}
	if !has {
		t.Fatalf("Has(k020) = false, want true")
	}

	if _, err := tr.Get([]byte("k030")); err != ErrKeyNotFound {
		t.Fatalf("Get(k030): expected ErrKeyNotFound, got %v", err)
	}
	if _, err := tr.Get([]byte("j999")); err != ErrKeyNotFound {
		t.Fatalf("Get(j999): expected ErrKeyNotFound, got %v", err)
	}

	if reader.fenceCalls != 3 {
		t.Fatalf("fence calls = %d, want 3", reader.fenceCalls)
	}
}
