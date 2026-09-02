package bulk

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

// MockAllocator
type MockAllocator struct {
	p    *pager.Pager
	fail bool
}

func (m *MockAllocator) Alloc(hint uint64) (uint64, error) {
	if m.fail {
		return 0, errors.New("mock allocation failed")
	}
	return m.p.Alloc(1)
}

// MockIterator
type MockIterator struct {
	keys [][]byte
	idx  int
}

func (m *MockIterator) Valid() bool         { return m.idx < len(m.keys) }
func (m *MockIterator) Next()               { m.idx++ }
func (m *MockIterator) UnsafeKey() []byte   { return m.keys[m.idx] }
func (m *MockIterator) UnsafeValue() []byte { return []byte("val:" + string(m.keys[m.idx])) }
func (m *MockIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	return m.UnsafeValue(), page.ValuePtr{}, 0
}
func (m *MockIterator) Seek(key []byte)             {}
func (m *MockIterator) Key() []byte                 { return m.UnsafeKey() }
func (m *MockIterator) Value() []byte               { return m.UnsafeValue() }
func (m *MockIterator) KeyCopy(dst []byte) []byte   { return append(dst[:0], m.UnsafeKey()...) }
func (m *MockIterator) ValueCopy(dst []byte) []byte { return append(dst[:0], m.UnsafeValue()...) }
func (m *MockIterator) IsDeleted() bool             { return false }
func (m *MockIterator) Error() error                { return nil }
func (m *MockIterator) Close() error                { return nil }
func (m *MockIterator) Domain() (start, end []byte) { return nil, nil }

func TestBuild(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}

	// Create 1000 sorted keys
	keys := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%04d", i))
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	iter := &MockIterator{keys: keys}

	rootID, err := Build(iter, alloc, p)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Verify using Tree
	tr := tree.New(p, nil, rootID)
	for i := 0; i < 1000; i++ {
		k := keys[i]
		v, err := tr.Get(k)
		if err != nil {
			t.Errorf("Get(%s) failed: %v", k, err)
		}
		expected := []byte("val:" + string(k))
		if !bytes.Equal(v, expected) {
			t.Errorf("Value mismatch for %s. Got %s, want %s", k, v, expected)
		}
	}
}

type mockLeafPageLog struct {
	ptrs      []page.LeafLogPtr
	pages     [][]byte
	appendErr error
}

func (m *mockLeafPageLog) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	if m.appendErr != nil {
		return page.LeafLogPtr{}, m.appendErr
	}
	ptr := page.LeafLogPtr{
		FileID: uint32(len(m.ptrs) + 1),
		Offset: uint64(len(m.pages) + 1),
	}
	m.ptrs = append(m.ptrs, ptr)
	m.pages = append(m.pages, append([]byte(nil), leafPage...))
	return ptr, nil
}

type mockBatchLeafPageLog struct {
	mockLeafPageLog
	batchLens []int
}

func (m *mockBatchLeafPageLog) AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error) {
	if m.appendErr != nil {
		return nil, m.appendErr
	}
	m.batchLens = append(m.batchLens, len(leafPages))
	ptrs := make([]page.LeafLogPtr, len(leafPages))
	fileID := uint32(len(m.batchLens))
	offset := uint64(len(m.ptrs) + 1)
	for i, leafPage := range leafPages {
		ptr := page.LeafLogPtr{FileID: fileID, Offset: offset, RecordLengthHint: page.ValuePtrMarkGrouped(page.PageSize, uint8(i)), SubIndex: uint16(i)}
		ptrs[i] = ptr
		m.ptrs = append(m.ptrs, ptr)
		m.pages = append(m.pages, append([]byte(nil), leafPage...))
	}
	return ptrs, nil
}

type mockKVIterator struct {
	keys   [][]byte
	values [][]byte
	idx    int
}

func (m *mockKVIterator) Valid() bool         { return m.idx < len(m.keys) }
func (m *mockKVIterator) Next()               { m.idx++ }
func (m *mockKVIterator) UnsafeKey() []byte   { return m.keys[m.idx] }
func (m *mockKVIterator) UnsafeValue() []byte { return m.values[m.idx] }
func (m *mockKVIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	return m.UnsafeValue(), page.ValuePtr{}, 0
}
func (m *mockKVIterator) Seek([]byte)               {}
func (m *mockKVIterator) Key() []byte               { return m.UnsafeKey() }
func (m *mockKVIterator) Value() []byte             { return m.UnsafeValue() }
func (m *mockKVIterator) KeyCopy(dst []byte) []byte { return append(dst[:0], m.UnsafeKey()...) }
func (m *mockKVIterator) ValueCopy(dst []byte) []byte {
	return append(dst[:0], m.UnsafeValue()...)
}
func (m *mockKVIterator) IsDeleted() bool             { return false }
func (m *mockKVIterator) Error() error                { return nil }
func (m *mockKVIterator) Close() error                { return nil }
func (m *mockKVIterator) Domain() (start, end []byte) { return nil, nil }

type mockRevisionKVIterator struct {
	mockKVIterator
	revisions []page.EntryRevision
}

func (m *mockRevisionKVIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	revision := page.LegacyEntryRevision
	if m.idx >= 0 && m.idx < len(m.revisions) {
		revision = m.revisions[m.idx]
	}
	return m.UnsafeValue(), page.ValuePtr{}, node.FlagInline, revision
}

func TestBuildWithOptionsSparseEntryRevisionsDoNotVersionLegacyLeaves(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	iter := &mockRevisionKVIterator{}
	const n = 128
	for i := 0; i < n; i++ {
		iter.keys = append(iter.keys, []byte(fmt.Sprintf("key-%04d", i)))
		iter.values = append(iter.values, bytes.Repeat([]byte{byte('a' + i%26)}, 96))
		iter.revisions = append(iter.revisions, page.LegacyEntryRevision)
	}
	iter.revisions[96] = 12345

	leafLog := &mockLeafPageLog{}
	if _, err := BuildWithOptions(iter, &MockAllocator{p: p}, p, BuildOptions{LeafPageLog: leafLog}); err != nil {
		t.Fatalf("BuildWithOptions: %v", err)
	}
	if len(leafLog.pages) < 2 {
		t.Fatalf("leaf pages=%d want at least 2", len(leafLog.pages))
	}
	first := node.NewNodeView(leafLog.pages[0])
	if first.LeafEntryRevisionsEnabled() {
		t.Fatal("first legacy-only leaf unexpectedly encoded entry revisions")
	}
	foundRevisionLeaf := false
	for _, data := range leafLog.pages {
		n := node.NewNodeView(data)
		if n.LeafEntryRevisionsEnabled() {
			foundRevisionLeaf = true
			break
		}
	}
	if !foundRevisionLeaf {
		t.Fatal("no leaf encoded the non-legacy entry revision")
	}
}

func TestBuildWithOptions_EmptyIteratorLeafPageLogReturnsLeafRefRoot(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	leafLog := &mockLeafPageLog{}

	rootID, err := BuildWithOptions(&MockIterator{}, alloc, p, BuildOptions{LeafPageLog: leafLog})
	if err != nil {
		t.Fatalf("BuildWithOptions: %v", err)
	}
	if len(leafLog.ptrs) != 1 {
		t.Fatalf("AppendLeafPage calls=%d want 1", len(leafLog.ptrs))
	}
	data, err := p.Get(rootID)
	if err != nil {
		t.Fatalf("pager.Get(root): %v", err)
	}
	root := node.NewNodeView(data)
	childRef, err := root.GetInternalChildRef(0)
	if err != nil {
		t.Fatalf("GetInternalChildRef: %v", err)
	}
	if childRef.Kind != page.ChildRefLeafLog {
		t.Fatalf("root child kind=%d want leaf-log", childRef.Kind)
	}
	ptr := childRef.Log
	if ptr.FileID != leafLog.ptrs[0].FileID || ptr.Offset != leafLog.ptrs[0].Offset {
		t.Fatalf("root ptr=(file=%d off=%d) want (file=%d off=%d)", ptr.FileID, ptr.Offset, leafLog.ptrs[0].FileID, leafLog.ptrs[0].Offset)
	}
}

func TestBuildWithOptions_NonEmptyLeafPageLogUsesLeafRefsForLeafChildren(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	leafLog := &mockLeafPageLog{}
	iter := &mockKVIterator{}
	const n = 2048
	valuePrefix := bytes.Repeat([]byte("value-"), 16)
	for i := 0; i < n; i++ {
		iter.keys = append(iter.keys, []byte(fmt.Sprintf("key-%06d", i)))
		v := append([]byte(nil), valuePrefix...)
		binary.BigEndian.PutUint32(v[len(v)-4:], uint32(i))
		iter.values = append(iter.values, v)
	}

	rootID, err := BuildWithOptions(iter, alloc, p, BuildOptions{LeafPageLog: leafLog})
	if err != nil {
		t.Fatalf("BuildWithOptions: %v", err)
	}
	if len(leafLog.ptrs) < 2 {
		t.Fatalf("expected multiple leaf pages in leaf log, got %d", len(leafLog.ptrs))
	}
	data, err := p.Get(rootID)
	if err != nil {
		t.Fatalf("pager.Get(root): %v", err)
	}
	root := node.NewNodeView(data)
	if root.Type() != page.PageTypeInternal {
		t.Fatalf("expected internal root, got %d", root.Type())
	}
	foundLeafRef := false
	for i := uint16(0); i < root.Count(); i++ {
		childRef, err := root.GetInternalChildRef(i)
		if err != nil {
			t.Fatalf("GetInternalChildRef(%d): %v", i, err)
		}
		if childRef.Kind == page.ChildRefLeafLog {
			foundLeafRef = true
			break
		}
	}
	if !foundLeafRef {
		t.Fatalf("expected root to reference at least one leaf ref child")
	}
}

func TestBuildWithOptions_BatchesLeafPageLogAppends(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	leafLog := &mockBatchLeafPageLog{}
	iter := &mockKVIterator{}
	const n = 2048
	valuePrefix := bytes.Repeat([]byte("value-"), 16)
	for i := 0; i < n; i++ {
		iter.keys = append(iter.keys, []byte(fmt.Sprintf("key-%06d", i)))
		v := append([]byte(nil), valuePrefix...)
		binary.BigEndian.PutUint32(v[len(v)-4:], uint32(i))
		iter.values = append(iter.values, v)
	}

	rootID, err := BuildWithOptions(iter, alloc, p, BuildOptions{LeafPageLog: leafLog})
	if err != nil {
		t.Fatalf("BuildWithOptions: %v", err)
	}
	batched := false
	for _, n := range leafLog.batchLens {
		if n > 1 {
			batched = true
			break
		}
	}
	if !batched {
		t.Fatalf("expected multi-leaf AppendLeafPages call, batchLens=%v", leafLog.batchLens)
	}
	if len(leafLog.ptrs) != len(leafLog.pages) || len(leafLog.ptrs) < 2 {
		t.Fatalf("ptrs=%d pages=%d, want multiple stored leaf pages", len(leafLog.ptrs), len(leafLog.pages))
	}
	data, err := p.Get(rootID)
	if err != nil {
		t.Fatalf("pager.Get(root): %v", err)
	}
	root := node.NewNodeView(data)
	if root.Type() != page.PageTypeInternal {
		t.Fatalf("expected internal root, got %d", root.Type())
	}
}

func TestBuildWithOptions_LeafPageLogAppendErrorPropagates(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	leafLog := &mockLeafPageLog{appendErr: errors.New("append leaf failed")}

	_, err = BuildWithOptions(&MockIterator{keys: [][]byte{[]byte("a")}}, alloc, p, BuildOptions{LeafPageLog: leafLog})
	if err == nil || err.Error() != "append leaf failed" {
		t.Fatalf("BuildWithOptions error=%v want append leaf failed", err)
	}
}
