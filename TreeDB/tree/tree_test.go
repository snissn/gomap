package tree

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
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
	blocks           map[page.ValuePtr]map[string][]byte
	nextOffset       uint64
	fileID           uint32
	globalEnabled    bool
	singleCandidate  bool
	fenceCalls       int
	fenceAppendCalls int
	blockCalls       int
	keyCalls         int
}

type fenceLookupClassifierReader struct {
	*fenceLookupReader
	likely map[page.ValuePtr]bool
}

type fenceLookupSeekClassifierReader struct {
	*fenceLookupSeekReader
	likely map[page.ValuePtr]bool
}

var errFenceLookupMissingBlock = errors.New("fence lookup missing block")

func fenceLookupMissingBlockErr(ptr page.ValuePtr) error {
	return fmt.Errorf("%w: ptr %+v", errFenceLookupMissingBlock, ptr)
}

func (r *fenceLookupReader) FenceLookupEnabled() bool {
	return true
}

func (r *fenceLookupReader) FenceLookupGlobalEnabled() bool {
	if r == nil {
		return true
	}
	return r.globalEnabled
}

func (r *fenceLookupReader) FenceLookupSingleCandidateEnabled() bool {
	if r == nil {
		return false
	}
	return r.singleCandidate
}

func (r *fenceLookupClassifierReader) FencePointerLikelyBlock(ptr page.ValuePtr) bool {
	if r == nil {
		return true
	}
	if r.likely == nil {
		return true
	}
	likely, ok := r.likely[ptr]
	if !ok {
		return true
	}
	return likely
}

func (r *fenceLookupSeekClassifierReader) FencePointerLikelyBlock(ptr page.ValuePtr) bool {
	if r == nil {
		return true
	}
	if r.likely == nil {
		return true
	}
	likely, ok := r.likely[ptr]
	if !ok {
		return true
	}
	return likely
}

func newFenceLookupReader() *fenceLookupReader {
	return newFenceLookupReaderWithScope(true, false)
}

func newFenceLookupReaderWithScope(globalEnabled bool, singleCandidate bool) *fenceLookupReader {
	return &fenceLookupReader{
		blocks:          make(map[page.ValuePtr]map[string][]byte),
		fileID:          page.ValueLogFileID(1),
		globalEnabled:   globalEnabled,
		singleCandidate: singleCandidate,
	}
}

func newFenceLookupClassifierReader() *fenceLookupClassifierReader {
	base := newFenceLookupReader()
	return &fenceLookupClassifierReader{
		fenceLookupReader: base,
		likely:            make(map[page.ValuePtr]bool),
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
		return nil, false, fenceLookupMissingBlockErr(ptr)
	}
	val, ok := block[string(key)]
	if !ok {
		return nil, false, nil
	}
	return val, true, nil
}

func (r *fenceLookupReader) ReadUnsafeFenceAppendForKey(ptr page.ValuePtr, key []byte, dst []byte) ([]byte, bool, error) {
	r.fenceAppendCalls++
	block, ok := r.blocks[ptr]
	if !ok {
		return nil, false, fenceLookupMissingBlockErr(ptr)
	}
	val, ok := block[string(key)]
	if !ok {
		return dst, false, nil
	}
	return append(dst, val...), true, nil
}

type fenceLookupTailAppenderReader struct {
	*fenceLookupReader
}

type fenceLookupSeekReader struct {
	*fenceLookupReader
	seekCalls int
}

type countingSeekKeysLease struct {
	keys         [][]byte
	released     *int
	releasedOnce bool
}

func (l *countingSeekKeysLease) Keys() [][]byte { return l.keys }

func (l *countingSeekKeysLease) Release() {
	if l == nil || l.releasedOnce {
		return
	}
	l.releasedOnce = true
	*l.released = *l.released + 1
}

type fenceLookupSeekLeaseReader struct {
	*fenceLookupReader
	seekLeaseCalls      int
	createdLeases       int
	releasedLeases      int
	returnErr           error
	returnLeaseWithErr  bool
	returnLeaseWithNoOK bool
}

type fenceLookupAppenderOnlyReader struct {
	blocks           map[page.ValuePtr]map[string][]byte
	nextOffset       uint64
	fileID           uint32
	fenceAppendCalls int
}

func newFenceLookupTailAppenderReader() *fenceLookupTailAppenderReader {
	return &fenceLookupTailAppenderReader{fenceLookupReader: newFenceLookupReader()}
}

func newFenceLookupSeekReader() *fenceLookupSeekReader {
	return &fenceLookupSeekReader{fenceLookupReader: newFenceLookupReader()}
}

func newFenceLookupSeekClassifierReader() *fenceLookupSeekClassifierReader {
	base := newFenceLookupSeekReader()
	return &fenceLookupSeekClassifierReader{
		fenceLookupSeekReader: base,
		likely:                make(map[page.ValuePtr]bool),
	}
}

func newFenceLookupSeekLeaseReader() *fenceLookupSeekLeaseReader {
	return &fenceLookupSeekLeaseReader{fenceLookupReader: newFenceLookupReader()}
}

func newFenceLookupAppenderOnlyReader() *fenceLookupAppenderOnlyReader {
	return &fenceLookupAppenderOnlyReader{
		blocks: make(map[page.ValuePtr]map[string][]byte),
		fileID: page.ValueLogFileID(1),
	}
}

func (r *fenceLookupAppenderOnlyReader) FenceLookupEnabled() bool {
	return true
}

func (r *fenceLookupAppenderOnlyReader) addBlock(entries map[string]string) page.ValuePtr {
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

func (r *fenceLookupAppenderOnlyReader) Read(ptr page.ValuePtr) ([]byte, error) {
	return nil, fmt.Errorf("unexpected Read for ptr %+v", ptr)
}

func (r *fenceLookupAppenderOnlyReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	return nil, fmt.Errorf("unexpected ReadUnsafe for ptr %+v", ptr)
}

func (r *fenceLookupAppenderOnlyReader) ReadUnsafeFenceAppendForKey(ptr page.ValuePtr, key []byte, dst []byte) ([]byte, bool, error) {
	r.fenceAppendCalls++
	block, ok := r.blocks[ptr]
	if !ok {
		return nil, false, fenceLookupMissingBlockErr(ptr)
	}
	val, ok := block[string(key)]
	if !ok {
		return dst, false, nil
	}
	return append(dst, val...), true, nil
}

func (r *fenceLookupTailAppenderReader) ReadUnsafeFenceAppendForKey(ptr page.ValuePtr, key []byte, dst []byte) ([]byte, bool, error) {
	r.fenceAppendCalls++
	block, ok := r.blocks[ptr]
	if !ok {
		return nil, false, fenceLookupMissingBlockErr(ptr)
	}
	val, ok := block[string(key)]
	if !ok {
		return dst, false, nil
	}
	// Simulate valueReader append contract: dst is scratch tail, and return
	// value-only bytes (not dst prefix).
	return append(dst[:0], val...), true, nil
}

func (r *fenceLookupReader) ReadUnsafeFenceBlock(ptr page.ValuePtr) ([]FenceBlockEntry, bool, error) {
	r.blockCalls++
	block, ok := r.blocks[ptr]
	if !ok {
		return nil, true, fmt.Errorf("missing block ptr %+v", ptr)
	}
	keys := make([]string, 0, len(block))
	for k := range block {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	entries := make([]FenceBlockEntry, 0, len(keys))
	for _, k := range keys {
		entries = append(entries, FenceBlockEntry{
			Key:   []byte(k),
			Value: block[k],
		})
	}
	return entries, true, nil
}

func (r *fenceLookupReader) ReadUnsafeFenceBlockKeys(ptr page.ValuePtr) ([][]byte, bool, error) {
	r.keyCalls++
	block, ok := r.blocks[ptr]
	if !ok {
		return nil, true, fmt.Errorf("missing block ptr %+v", ptr)
	}
	keys := make([]string, 0, len(block))
	for k := range block {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([][]byte, 0, len(keys))
	for _, k := range keys {
		out = append(out, []byte(k))
	}
	return out, true, nil
}

func (r *fenceLookupSeekReader) ReadUnsafeFenceBlockSeek(ptr page.ValuePtr, key []byte) (pos int, below bool, above bool, keys [][]byte, ok bool, err error) {
	r.seekCalls++
	block, ok := r.blocks[ptr]
	if !ok {
		return 0, false, false, nil, true, fmt.Errorf("missing block ptr %+v", ptr)
	}
	sorted := make([]string, 0, len(block))
	for k := range block {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	if len(sorted) == 0 {
		return 0, false, true, nil, true, nil
	}
	keys = make([][]byte, len(sorted))
	for i := range sorted {
		keys[i] = []byte(sorted[i])
	}
	pos = sort.Search(len(keys), func(i int) bool {
		return bytes.Compare(keys[i], key) >= 0
	})
	if pos == 0 && bytes.Compare(key, keys[0]) < 0 {
		return 0, true, false, nil, true, nil
	}
	if pos >= len(keys) {
		return len(keys), false, true, nil, true, nil
	}
	return pos, false, false, keys, true, nil
}

func (r *fenceLookupSeekLeaseReader) newLease(keys [][]byte) FenceKeysLease {
	r.createdLeases++
	return &countingSeekKeysLease{
		keys:     keys,
		released: &r.releasedLeases,
	}
}

func (r *fenceLookupSeekLeaseReader) ReadUnsafeFenceBlockSeekLease(ptr page.ValuePtr, key []byte) (pos int, below bool, above bool, lease FenceKeysLease, ok bool, err error) {
	r.seekLeaseCalls++
	block, ok := r.blocks[ptr]
	if !ok {
		return 0, false, false, nil, true, fmt.Errorf("missing block ptr %+v", ptr)
	}
	sorted := make([]string, 0, len(block))
	for k := range block {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	keys := make([][]byte, len(sorted))
	for i := range sorted {
		keys[i] = []byte(sorted[i])
	}
	if r.returnErr != nil {
		if r.returnLeaseWithErr {
			return 0, false, false, r.newLease(keys), true, r.returnErr
		}
		return 0, false, false, nil, true, r.returnErr
	}
	if r.returnLeaseWithNoOK {
		return 0, false, false, r.newLease(keys), false, nil
	}
	if len(keys) == 0 {
		return 0, false, true, nil, true, nil
	}
	pos = sort.Search(len(keys), func(i int) bool {
		return bytes.Compare(keys[i], key) >= 0
	})
	if pos == 0 && bytes.Compare(key, keys[0]) < 0 {
		return 0, true, false, nil, true, nil
	}
	if pos >= len(keys) {
		return len(keys), false, true, nil, true, nil
	}
	return pos, false, false, r.newLease(keys), true, nil
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

func TestTreeGetAppend_UsesAppendReaderForLeafRefPages(t *testing.T) {
	tracked := &trackedValueReader{mapValueReader: newMapValueReader()}

	leafData := make([]byte, page.PageSize)
	leaf := node.NewNode(leafData)
	leaf.SetType(page.PageTypeLeaf)
	leaf.SetPageID(1)
	leaf.AddLeafEntry([]byte("k"), []byte("v"), node.FlagInline, page.ValuePtr{})
	leaf.UpdateChecksum()

	leafRefID, err := page.EncodeLeafRef(page.ValuePtr{
		FileID: page.ValueLogFileID(1),
		Offset: 8,
	})
	if err != nil {
		t.Fatalf("EncodeLeafRef: %v", err)
	}
	ptr, ok := page.DecodeLeafRef(leafRefID)
	if !ok {
		t.Fatalf("DecodeLeafRef failed")
	}
	tracked.values[ptr] = append([]byte(nil), leafData...)

	tr := New(nil, tracked, leafRefID)
	got, err := tr.GetAppend([]byte("k"), nil)
	if err != nil {
		t.Fatalf("GetAppend failed: %v", err)
	}
	if string(got) != "v" {
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

	if reader.fenceCalls != 1 {
		t.Fatalf("fence calls = %d, want 1", reader.fenceCalls)
	}
	if reader.fenceAppendCalls != 3 {
		t.Fatalf("fence append calls = %d, want 3", reader.fenceAppendCalls)
	}
}

func TestTreeGet_FencePredecessorLookupContinuesAfterPointerMiss(t *testing.T) {
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
	ptrMid := reader.addBlock(map[string]string{
		"k015": "v15",
	})
	ptr1 := reader.addBlock(map[string]string{
		"k110": "v110",
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
	if err := root.AddLeafEntry([]byte("k015"), nil, node.FlagPointer, ptrMid); err != nil {
		t.Fatalf("AddLeafEntry(k015): %v", err)
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

	gotAppend, err := tr.GetAppend([]byte("k020"), []byte("prefix-"))
	if err != nil {
		t.Fatalf("GetAppend(k020): %v", err)
	}
	if string(gotAppend) != "prefix-v20" {
		t.Fatalf("GetAppend(k020) = %q, want %q", gotAppend, "prefix-v20")
	}

	has, err := tr.Has([]byte("k020"))
	if err != nil {
		t.Fatalf("Has(k020): %v", err)
	}
	if !has {
		t.Fatalf("Has(k020) = false, want true")
	}
}

func TestTreeGet_FencePredecessorLookupSkipsPointersClassifiedNonFence(t *testing.T) {
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

	reader := newFenceLookupClassifierReader()
	ptrOld := reader.addBlock(map[string]string{
		"k010": "v10",
		"k020": "v20",
	})
	ptrSkip := reader.addBlock(map[string]string{
		"k015": "v15",
	})
	ptrUpper := reader.addBlock(map[string]string{
		"k110": "v110",
	})
	reader.likely[ptrOld] = true
	reader.likely[ptrSkip] = false
	reader.likely[ptrUpper] = true

	rootData, err := p.Get(0)
	if err != nil {
		t.Fatalf("Get root page: %v", err)
	}
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeLeaf)
	root.SetPageID(0)
	if err := root.AddLeafEntry([]byte("k010"), nil, node.FlagPointer, ptrOld); err != nil {
		t.Fatalf("AddLeafEntry(k010): %v", err)
	}
	if err := root.AddLeafEntry([]byte("k015"), nil, node.FlagPointer, ptrSkip); err != nil {
		t.Fatalf("AddLeafEntry(k015): %v", err)
	}
	if err := root.AddLeafEntry([]byte("k110"), nil, node.FlagPointer, ptrUpper); err != nil {
		t.Fatalf("AddLeafEntry(k110): %v", err)
	}
	root.UpdateChecksum()

	tr := New(p, reader, 0)

	beforeTotalCalls := reader.fenceCalls + reader.fenceAppendCalls
	got, err := tr.Get([]byte("k020"))
	if err != nil {
		t.Fatalf("Get(k020): %v", err)
	}
	if string(got) != "v20" {
		t.Fatalf("Get(k020) = %q, want %q", got, "v20")
	}
	if gotCalls := (reader.fenceCalls + reader.fenceAppendCalls) - beforeTotalCalls; gotCalls != 1 {
		t.Fatalf("Get(k020) fence lookups = %d, want 1 (classified non-fence predecessor skipped)", gotCalls)
	}

	beforeTotalCalls = reader.fenceCalls + reader.fenceAppendCalls
	gotAppend, err := tr.GetAppend([]byte("k020"), []byte("prefix-"))
	if err != nil {
		t.Fatalf("GetAppend(k020): %v", err)
	}
	if string(gotAppend) != "prefix-v20" {
		t.Fatalf("GetAppend(k020) = %q, want %q", gotAppend, "prefix-v20")
	}
	if gotCalls := (reader.fenceCalls + reader.fenceAppendCalls) - beforeTotalCalls; gotCalls != 1 {
		t.Fatalf("GetAppend(k020) fence lookups = %d, want 1 (classified non-fence predecessor skipped)", gotCalls)
	}

	beforeTotalCalls = reader.fenceCalls + reader.fenceAppendCalls
	has, err := tr.Has([]byte("k020"))
	if err != nil {
		t.Fatalf("Has(k020): %v", err)
	}
	if !has {
		t.Fatalf("Has(k020) = false, want true")
	}
	if gotCalls := (reader.fenceCalls + reader.fenceAppendCalls) - beforeTotalCalls; gotCalls != 1 {
		t.Fatalf("Has(k020) fence lookups = %d, want 1 (classified non-fence predecessor skipped)", gotCalls)
	}

	reader.likely[ptrOld] = false
	reader.likely[ptrUpper] = false
	beforeTotalCalls = reader.fenceCalls + reader.fenceAppendCalls
	if _, err := tr.Get([]byte("k999")); err != ErrKeyNotFound {
		t.Fatalf("Get(k999): expected ErrKeyNotFound, got %v", err)
	}
	if gotCalls := (reader.fenceCalls + reader.fenceAppendCalls) - beforeTotalCalls; gotCalls != 0 {
		t.Fatalf("Get(k999) issued %d fence lookups despite all predecessors classified non-fence", gotCalls)
	}

	beforeTotalCalls = reader.fenceCalls + reader.fenceAppendCalls
	if _, err := tr.GetAppend([]byte("k999"), []byte("prefix-")); err != ErrKeyNotFound {
		t.Fatalf("GetAppend(k999): expected ErrKeyNotFound, got %v", err)
	}
	if gotCalls := (reader.fenceCalls + reader.fenceAppendCalls) - beforeTotalCalls; gotCalls != 0 {
		t.Fatalf("GetAppend(k999) issued %d fence lookups despite all predecessors classified non-fence", gotCalls)
	}

	beforeTotalCalls = reader.fenceCalls + reader.fenceAppendCalls
	has, err = tr.Has([]byte("k999"))
	if err != nil {
		t.Fatalf("Has(k999): %v", err)
	}
	if has {
		t.Fatalf("Has(k999) = true, want false")
	}
	if gotCalls := (reader.fenceCalls + reader.fenceAppendCalls) - beforeTotalCalls; gotCalls != 0 {
		t.Fatalf("Has(k999) issued %d fence lookups despite all predecessors classified non-fence", gotCalls)
	}
}

func TestTreeGet_FencePredecessorLookupExplicitFenceMarkerOverridesClassifier(t *testing.T) {
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

	reader := newFenceLookupClassifierReader()
	ptrRaw := reader.addBlock(map[string]string{
		"k010": "v10",
		"k020": "v20",
	})
	ptrMarked := page.ValuePtrMarkFenceOuter(ptrRaw)
	if ptrMarked == ptrRaw {
		t.Fatalf("expected non-grouped pointer to be fence-marked")
	}
	reader.blocks[ptrMarked] = reader.blocks[ptrRaw]
	delete(reader.blocks, ptrRaw)
	reader.likely[ptrMarked] = false // explicit fence marker must still probe

	ptrUpper := reader.addBlock(map[string]string{
		"k110": "v110",
	})
	reader.likely[ptrUpper] = true

	rootData, err := p.Get(0)
	if err != nil {
		t.Fatalf("Get root page: %v", err)
	}
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeLeaf)
	root.SetPageID(0)
	if err := root.AddLeafEntry([]byte("k010"), nil, node.FlagPointer, ptrMarked); err != nil {
		t.Fatalf("AddLeafEntry(k010): %v", err)
	}
	if err := root.AddLeafEntry([]byte("k110"), nil, node.FlagPointer, ptrUpper); err != nil {
		t.Fatalf("AddLeafEntry(k110): %v", err)
	}
	root.UpdateChecksum()

	tr := New(p, reader, 0)

	beforeTotalCalls := reader.fenceCalls + reader.fenceAppendCalls
	got, err := tr.Get([]byte("k020"))
	if err != nil {
		t.Fatalf("Get(k020): %v", err)
	}
	if string(got) != "v20" {
		t.Fatalf("Get(k020) = %q, want %q", got, "v20")
	}
	if gotCalls := (reader.fenceCalls + reader.fenceAppendCalls) - beforeTotalCalls; gotCalls != 1 {
		t.Fatalf("Get(k020) fence lookups = %d, want 1 (explicit fence marker must probe)", gotCalls)
	}

	beforeTotalCalls = reader.fenceCalls + reader.fenceAppendCalls
	gotAppend, err := tr.GetAppend([]byte("k020"), []byte("prefix-"))
	if err != nil {
		t.Fatalf("GetAppend(k020): %v", err)
	}
	if string(gotAppend) != "prefix-v20" {
		t.Fatalf("GetAppend(k020) = %q, want %q", gotAppend, "prefix-v20")
	}
	if gotCalls := (reader.fenceCalls + reader.fenceAppendCalls) - beforeTotalCalls; gotCalls != 1 {
		t.Fatalf("GetAppend(k020) fence lookups = %d, want 1 (explicit fence marker must probe)", gotCalls)
	}

	beforeTotalCalls = reader.fenceCalls + reader.fenceAppendCalls
	has, err := tr.Has([]byte("k020"))
	if err != nil {
		t.Fatalf("Has(k020): %v", err)
	}
	if !has {
		t.Fatalf("Has(k020) = false, want true")
	}
	if gotCalls := (reader.fenceCalls + reader.fenceAppendCalls) - beforeTotalCalls; gotCalls != 1 {
		t.Fatalf("Has(k020) fence lookups = %d, want 1 (explicit fence marker must probe)", gotCalls)
	}
}

func TestTreeGet_FencePredecessorLookupContinuesAfterMultiplePointerMisses(t *testing.T) {
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
		"k020": "v20",
	})
	ptr1 := reader.addBlock(map[string]string{
		"k015": "v15",
	})
	ptr2 := reader.addBlock(map[string]string{
		"k017": "v17",
	})
	ptr3 := reader.addBlock(map[string]string{
		"k110": "v110",
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
	if err := root.AddLeafEntry([]byte("k015"), nil, node.FlagPointer, ptr1); err != nil {
		t.Fatalf("AddLeafEntry(k015): %v", err)
	}
	if err := root.AddLeafEntry([]byte("k017"), nil, node.FlagPointer, ptr2); err != nil {
		t.Fatalf("AddLeafEntry(k017): %v", err)
	}
	if err := root.AddLeafEntry([]byte("k110"), nil, node.FlagPointer, ptr3); err != nil {
		t.Fatalf("AddLeafEntry(k110): %v", err)
	}
	root.UpdateChecksum()

	tr := New(p, reader, 0)

	fenceCallsBefore := reader.fenceCalls
	fenceAppendBefore := reader.fenceAppendCalls
	got, err := tr.Get([]byte("k020"))
	if err != nil {
		t.Fatalf("Get(k020): %v", err)
	}
	if string(got) != "v20" {
		t.Fatalf("Get(k020) = %q, want %q", got, "v20")
	}
	getLookups := (reader.fenceCalls - fenceCallsBefore) + (reader.fenceAppendCalls - fenceAppendBefore)
	if getLookups < 3 {
		t.Fatalf("Get(k020) lookup probes=%d, want at least 3 (multiple misses + one hit)", getLookups)
	}

	fenceCallsBefore = reader.fenceCalls
	fenceAppendBefore = reader.fenceAppendCalls
	gotAppend, err := tr.GetAppend([]byte("k020"), []byte("x-"))
	if err != nil {
		t.Fatalf("GetAppend(k020): %v", err)
	}
	if string(gotAppend) != "x-v20" {
		t.Fatalf("GetAppend(k020) = %q, want %q", gotAppend, "x-v20")
	}
	getAppendLookups := (reader.fenceCalls - fenceCallsBefore) + (reader.fenceAppendCalls - fenceAppendBefore)
	if getAppendLookups < 3 {
		t.Fatalf("GetAppend(k020) lookup probes=%d, want at least 3 (multiple misses + one hit)", getAppendLookups)
	}

	fenceCallsBefore = reader.fenceCalls
	fenceAppendBefore = reader.fenceAppendCalls
	has, err := tr.Has([]byte("k020"))
	if err != nil {
		t.Fatalf("Has(k020): %v", err)
	}
	if !has {
		t.Fatalf("Has(k020) = false, want true")
	}
	hasLookups := (reader.fenceCalls - fenceCallsBefore) + (reader.fenceAppendCalls - fenceAppendBefore)
	if hasLookups < 3 {
		t.Fatalf("Has(k020) lookup probes=%d, want at least 3 (multiple misses + one hit)", hasLookups)
	}
}

func TestTreeGet_FencePredecessorLookupSkipsNonPointers(t *testing.T) {
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
		"f010": "v10",
		"f020": "v20",
		"f050": "v50",
	})
	ptr1 := reader.addBlock(map[string]string{
		"f110": "v110",
	})

	rootData, err := p.Get(0)
	if err != nil {
		t.Fatalf("Get root page: %v", err)
	}
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeLeaf)
	root.SetPageID(0)
	if err := root.AddLeafEntry([]byte("f010"), nil, node.FlagPointer, ptr0); err != nil {
		t.Fatalf("AddLeafEntry(f010): %v", err)
	}
	if err := root.AddLeafEntry([]byte("f040"), []byte("inline40"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("AddLeafEntry(f040): %v", err)
	}
	if err := root.AddLeafEntry([]byte("f045"), nil, node.FlagTombstone, page.ValuePtr{}); err != nil {
		t.Fatalf("AddLeafEntry(f045): %v", err)
	}
	if err := root.AddLeafEntry([]byte("f110"), nil, node.FlagPointer, ptr1); err != nil {
		t.Fatalf("AddLeafEntry(f110): %v", err)
	}
	root.UpdateChecksum()

	tr := New(p, reader, 0)

	got, err := tr.Get([]byte("f050"))
	if err != nil {
		t.Fatalf("Get(f050): %v", err)
	}
	if string(got) != "v50" {
		t.Fatalf("Get(f050) = %q, want %q", got, "v50")
	}

	has, err := tr.Has([]byte("f050"))
	if err != nil {
		t.Fatalf("Has(f050): %v", err)
	}
	if !has {
		t.Fatalf("Has(f050) = false, want true")
	}

	got, err = tr.Get([]byte("f040"))
	if err != nil {
		t.Fatalf("Get(f040): %v", err)
	}
	if string(got) != "inline40" {
		t.Fatalf("Get(f040) = %q, want %q", got, "inline40")
	}

	if _, err := tr.Get([]byte("f060")); err != ErrKeyNotFound {
		t.Fatalf("Get(f060): expected ErrKeyNotFound, got %v", err)
	}

	if reader.fenceCalls != 1 {
		t.Fatalf("fence calls = %d, want 1", reader.fenceCalls)
	}
	if reader.fenceAppendCalls != 3 {
		t.Fatalf("fence append calls = %d, want 3", reader.fenceAppendCalls)
	}
}

func TestTreeHas_FencePredecessorLookupUsesSeekWhenAvailable(t *testing.T) {
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

	reader := newFenceLookupSeekReader()
	ptr0 := reader.addBlock(map[string]string{
		"k010": "v10",
		"k020": "v20",
		"k050": "v50",
	})
	ptr1 := reader.addBlock(map[string]string{
		"k110": "v110",
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

	has, err := tr.Has([]byte("k020"))
	if err != nil {
		t.Fatalf("Has(k020): %v", err)
	}
	if !has {
		t.Fatalf("Has(k020) = false, want true")
	}

	has, err = tr.Has([]byte("k030"))
	if err != nil {
		t.Fatalf("Has(k030): %v", err)
	}
	if has {
		t.Fatalf("Has(k030) = true, want false")
	}

	if reader.seekCalls == 0 {
		t.Fatalf("seek calls = %d, want > 0", reader.seekCalls)
	}
	if reader.fenceCalls != 1 {
		t.Fatalf("fence calls = %d, want 1", reader.fenceCalls)
	}
	if reader.fenceAppendCalls != 0 {
		t.Fatalf("fence append calls = %d, want 0", reader.fenceAppendCalls)
	}
}

func TestTreeHas_FencePredecessorLookupSeekSkipsPointersClassifiedNonFence(t *testing.T) {
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

	reader := newFenceLookupSeekClassifierReader()
	ptrOld := reader.addBlock(map[string]string{
		"k010": "v10",
		"k020": "v20",
	})
	ptrSkip := reader.addBlock(map[string]string{
		"k015": "v15",
	})
	ptrUpper := reader.addBlock(map[string]string{
		"k110": "v110",
	})
	reader.likely[ptrOld] = true
	reader.likely[ptrSkip] = false
	reader.likely[ptrUpper] = true

	rootData, err := p.Get(0)
	if err != nil {
		t.Fatalf("Get root page: %v", err)
	}
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeLeaf)
	root.SetPageID(0)
	if err := root.AddLeafEntry([]byte("k010"), nil, node.FlagPointer, ptrOld); err != nil {
		t.Fatalf("AddLeafEntry(k010): %v", err)
	}
	if err := root.AddLeafEntry([]byte("k015"), nil, node.FlagPointer, ptrSkip); err != nil {
		t.Fatalf("AddLeafEntry(k015): %v", err)
	}
	if err := root.AddLeafEntry([]byte("k110"), nil, node.FlagPointer, ptrUpper); err != nil {
		t.Fatalf("AddLeafEntry(k110): %v", err)
	}
	root.UpdateChecksum()

	tr := New(p, reader, 0)

	beforeSeekCalls := reader.seekCalls
	has, err := tr.Has([]byte("k020"))
	if err != nil {
		t.Fatalf("Has(k020): %v", err)
	}
	if !has {
		t.Fatalf("Has(k020) = false, want true")
	}
	if gotCalls := reader.seekCalls - beforeSeekCalls; gotCalls != 1 {
		t.Fatalf("Has(k020) seek calls = %d, want 1 (classified non-fence predecessor skipped)", gotCalls)
	}

	reader.likely[ptrOld] = false
	reader.likely[ptrUpper] = false
	beforeSeekCalls = reader.seekCalls
	has, err = tr.Has([]byte("k999"))
	if err != nil {
		t.Fatalf("Has(k999): %v", err)
	}
	if has {
		t.Fatalf("Has(k999) = true, want false")
	}
	if gotCalls := reader.seekCalls - beforeSeekCalls; gotCalls != 0 {
		t.Fatalf("Has(k999) seek calls = %d, want 0 when all predecessors classified non-fence", gotCalls)
	}
}

func TestTreeHas_FencePredecessorLookupSeekExplicitFenceMarkerOverridesClassifier(t *testing.T) {
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

	reader := newFenceLookupSeekClassifierReader()
	ptrRaw := reader.addBlock(map[string]string{
		"k010": "v10",
		"k020": "v20",
	})
	ptrMarked := page.ValuePtrMarkFenceOuter(ptrRaw)
	if ptrMarked == ptrRaw {
		t.Fatalf("expected non-grouped pointer to be fence-marked")
	}
	reader.blocks[ptrMarked] = reader.blocks[ptrRaw]
	delete(reader.blocks, ptrRaw)
	reader.likely[ptrMarked] = false // explicit fence marker must still probe

	ptrUpper := reader.addBlock(map[string]string{
		"k110": "v110",
	})
	reader.likely[ptrUpper] = true

	rootData, err := p.Get(0)
	if err != nil {
		t.Fatalf("Get root page: %v", err)
	}
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeLeaf)
	root.SetPageID(0)
	if err := root.AddLeafEntry([]byte("k010"), nil, node.FlagPointer, ptrMarked); err != nil {
		t.Fatalf("AddLeafEntry(k010): %v", err)
	}
	if err := root.AddLeafEntry([]byte("k110"), nil, node.FlagPointer, ptrUpper); err != nil {
		t.Fatalf("AddLeafEntry(k110): %v", err)
	}
	root.UpdateChecksum()

	tr := New(p, reader, 0)

	beforeSeekCalls := reader.seekCalls
	has, err := tr.Has([]byte("k020"))
	if err != nil {
		t.Fatalf("Has(k020): %v", err)
	}
	if !has {
		t.Fatalf("Has(k020) = false, want true")
	}
	if gotCalls := reader.seekCalls - beforeSeekCalls; gotCalls != 1 {
		t.Fatalf("Has(k020) seek calls = %d, want 1 (explicit fence marker must probe)", gotCalls)
	}
}

func TestTreeHas_FencePredecessorLookupSeekLeaseReleasesOnError(t *testing.T) {
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

	reader := newFenceLookupSeekLeaseReader()
	ptr0 := reader.addBlock(map[string]string{
		"k010": "v10",
		"k020": "v20",
	})
	expectedErr := errors.New("injected seek lease error")
	reader.returnErr = expectedErr
	reader.returnLeaseWithErr = true

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
	root.UpdateChecksum()

	tr := New(p, reader, 0)

	has, err := tr.Has([]byte("k020"))
	if err == nil || !errors.Is(err, expectedErr) {
		t.Fatalf("Has(k020) error = %v, want %v", err, expectedErr)
	}
	if has {
		t.Fatalf("Has(k020) = true, want false on error")
	}
	if reader.seekLeaseCalls != 1 {
		t.Fatalf("seek lease calls = %d, want 1", reader.seekLeaseCalls)
	}
	if reader.createdLeases != 1 {
		t.Fatalf("created leases = %d, want 1", reader.createdLeases)
	}
	if reader.releasedLeases != 1 {
		t.Fatalf("released leases = %d, want 1", reader.releasedLeases)
	}
}

func TestTreeHas_FencePredecessorLookupSeekLeaseReleasesForHitAndMiss(t *testing.T) {
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

	reader := newFenceLookupSeekLeaseReader()
	ptr0 := reader.addBlock(map[string]string{
		"k010": "v10",
		"k020": "v20",
		"k050": "v50",
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
	root.UpdateChecksum()

	tr := New(p, reader, 0)

	has, err := tr.Has([]byte("k020"))
	if err != nil {
		t.Fatalf("Has(k020): %v", err)
	}
	if !has {
		t.Fatalf("Has(k020) = false, want true")
	}

	has, err = tr.Has([]byte("k030"))
	if err != nil {
		t.Fatalf("Has(k030): %v", err)
	}
	if has {
		t.Fatalf("Has(k030) = true, want false")
	}

	if reader.seekLeaseCalls != 2 {
		t.Fatalf("seek lease calls = %d, want 2", reader.seekLeaseCalls)
	}
	if reader.createdLeases != 2 {
		t.Fatalf("created leases = %d, want 2", reader.createdLeases)
	}
	if reader.releasedLeases != 2 {
		t.Fatalf("released leases = %d, want 2", reader.releasedLeases)
	}
	if reader.fenceCalls != 1 {
		t.Fatalf("fence calls = %d, want 1", reader.fenceCalls)
	}
	if reader.fenceAppendCalls != 0 {
		t.Fatalf("fence append calls = %d, want 0", reader.fenceAppendCalls)
	}
}

func TestTreeHas_FencePredecessorLookupSeekLeaseReleasesWhenNotApplicable(t *testing.T) {
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

	reader := newFenceLookupSeekLeaseReader()
	reader.returnLeaseWithNoOK = true
	ptr0 := reader.addBlock(map[string]string{
		"k010": "v10",
		"k020": "v20",
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
	root.UpdateChecksum()

	tr := New(p, reader, 0)

	has, err := tr.Has([]byte("k020"))
	if err != nil {
		t.Fatalf("Has(k020): %v", err)
	}
	if !has {
		t.Fatalf("Has(k020) = false, want true")
	}

	if reader.seekLeaseCalls != 1 {
		t.Fatalf("seek lease calls = %d, want 1", reader.seekLeaseCalls)
	}
	if reader.createdLeases != 1 {
		t.Fatalf("created leases = %d, want 1", reader.createdLeases)
	}
	if reader.releasedLeases != 1 {
		t.Fatalf("released leases = %d, want 1", reader.releasedLeases)
	}
	if reader.fenceCalls != 1 {
		t.Fatalf("fence calls = %d, want 1", reader.fenceCalls)
	}
}

func TestTreeGetAppend_FencePredecessorLookupUsesAppender(t *testing.T) {
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
	root.UpdateChecksum()

	tr := New(p, reader, 0)

	got, err := tr.GetAppend([]byte("k020"), []byte("prefix:"))
	if err != nil {
		t.Fatalf("GetAppend(k020): %v", err)
	}
	if string(got) != "prefix:v20" {
		t.Fatalf("GetAppend(k020) = %q, want %q", got, "prefix:v20")
	}
	if reader.fenceAppendCalls != 1 {
		t.Fatalf("fence append calls = %d, want 1", reader.fenceAppendCalls)
	}
	if reader.fenceCalls != 0 {
		t.Fatalf("fence calls = %d, want 0", reader.fenceCalls)
	}
}

func TestTreeGetAppend_FencePredecessorLookupPreservesPrefixForTailAppender(t *testing.T) {
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

	reader := newFenceLookupTailAppenderReader()
	ptr0 := reader.addBlock(map[string]string{
		"k010": "v10",
		"k020": "v20",
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
	root.UpdateChecksum()

	tr := New(p, reader, 0)

	got, err := tr.GetAppend([]byte("k020"), []byte("prefix:"))
	if err != nil {
		t.Fatalf("GetAppend(k020): %v", err)
	}
	if string(got) != "prefix:v20" {
		t.Fatalf("GetAppend(k020) = %q, want %q", got, "prefix:v20")
	}
	if reader.fenceAppendCalls != 1 {
		t.Fatalf("fence append calls = %d, want 1", reader.fenceAppendCalls)
	}
}

func TestTreeGetAppend_FencePredecessorLookupGlobalPreservesPrefixForTailAppender(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "index.db")
	p, err := pager.Open(idxPath, 65536)
	if err != nil {
		t.Fatalf("Pager open failed: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(3); err != nil {
		t.Fatalf("Alloc pages: %v", err)
	}

	reader := newFenceLookupTailAppenderReader()
	ptr0 := reader.addBlock(map[string]string{
		"k120": "v120",
	})

	leaf1Data, err := p.Get(1)
	if err != nil {
		t.Fatalf("Get leaf1 page: %v", err)
	}
	leaf1 := node.NewNode(leaf1Data)
	leaf1.SetType(page.PageTypeLeaf)
	leaf1.SetPageID(1)
	if err := leaf1.AddLeafEntry([]byte("k050"), nil, node.FlagPointer, ptr0); err != nil {
		t.Fatalf("leaf1 AddLeafEntry(k050): %v", err)
	}
	leaf1.UpdateChecksum()

	leaf2Data, err := p.Get(2)
	if err != nil {
		t.Fatalf("Get leaf2 page: %v", err)
	}
	leaf2 := node.NewNode(leaf2Data)
	leaf2.SetType(page.PageTypeLeaf)
	leaf2.SetPageID(2)
	if err := leaf2.AddLeafEntry([]byte("k150"), []byte("v150"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("leaf2 AddLeafEntry(k150): %v", err)
	}
	leaf2.UpdateChecksum()

	rootData, err := p.Get(0)
	if err != nil {
		t.Fatalf("Get root page: %v", err)
	}
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeInternal)
	root.SetPageID(0)
	if err := root.AddInternalChild([]byte("k000"), 1); err != nil {
		t.Fatalf("root AddInternalChild(k000): %v", err)
	}
	if err := root.AddInternalChild([]byte("k100"), 2); err != nil {
		t.Fatalf("root AddInternalChild(k100): %v", err)
	}
	root.UpdateChecksum()

	tr := New(p, reader, 0)

	got, err := tr.GetAppend([]byte("k120"), []byte("prefix:"))
	if err != nil {
		t.Fatalf("GetAppend(k120): %v", err)
	}
	if string(got) != "prefix:v120" {
		t.Fatalf("GetAppend(k120) = %q, want %q", got, "prefix:v120")
	}
	if reader.fenceAppendCalls != 1 {
		t.Fatalf("fence append calls = %d, want 1", reader.fenceAppendCalls)
	}
}

func TestTreeGetAndHas_FencePredecessorLookupGlobal(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "index.db")
	p, err := pager.Open(idxPath, 65536)
	if err != nil {
		t.Fatalf("Pager open failed: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(3); err != nil {
		t.Fatalf("Alloc pages: %v", err)
	}

	reader := newFenceLookupReader()
	ptr0 := reader.addBlock(map[string]string{
		"k120": "v120",
	})

	leaf1Data, err := p.Get(1)
	if err != nil {
		t.Fatalf("Get leaf1 page: %v", err)
	}
	leaf1 := node.NewNode(leaf1Data)
	leaf1.SetType(page.PageTypeLeaf)
	leaf1.SetPageID(1)
	if err := leaf1.AddLeafEntry([]byte("k050"), nil, node.FlagPointer, ptr0); err != nil {
		t.Fatalf("leaf1 AddLeafEntry(k050): %v", err)
	}
	leaf1.UpdateChecksum()

	leaf2Data, err := p.Get(2)
	if err != nil {
		t.Fatalf("Get leaf2 page: %v", err)
	}
	leaf2 := node.NewNode(leaf2Data)
	leaf2.SetType(page.PageTypeLeaf)
	leaf2.SetPageID(2)
	if err := leaf2.AddLeafEntry([]byte("k150"), []byte("v150"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("leaf2 AddLeafEntry(k150): %v", err)
	}
	leaf2.UpdateChecksum()

	rootData, err := p.Get(0)
	if err != nil {
		t.Fatalf("Get root page: %v", err)
	}
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeInternal)
	root.SetPageID(0)
	if err := root.AddInternalChild([]byte("k000"), 1); err != nil {
		t.Fatalf("root AddInternalChild(k000): %v", err)
	}
	if err := root.AddInternalChild([]byte("k100"), 2); err != nil {
		t.Fatalf("root AddInternalChild(k100): %v", err)
	}
	root.UpdateChecksum()

	tr := New(p, reader, 0)

	got, err := tr.Get([]byte("k120"))
	if err != nil {
		t.Fatalf("Get(k120): %v", err)
	}
	if string(got) != "v120" {
		t.Fatalf("Get(k120) = %q, want %q", got, "v120")
	}

	has, err := tr.Has([]byte("k120"))
	if err != nil {
		t.Fatalf("Has(k120): %v", err)
	}
	if !has {
		t.Fatalf("Has(k120) = false, want true")
	}
}

func TestTreeGetEntry_FencePredecessorLookupGlobalParity(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "index.db")
	p, err := pager.Open(idxPath, 65536)
	if err != nil {
		t.Fatalf("Pager open failed: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(3); err != nil {
		t.Fatalf("Alloc pages: %v", err)
	}

	reader := newFenceLookupReader()
	ptr0 := reader.addBlock(map[string]string{
		"k120": "v120",
	})

	leaf1Data, err := p.Get(1)
	if err != nil {
		t.Fatalf("Get leaf1 page: %v", err)
	}
	leaf1 := node.NewNode(leaf1Data)
	leaf1.SetType(page.PageTypeLeaf)
	leaf1.SetPageID(1)
	if err := leaf1.AddLeafEntry([]byte("k050"), nil, node.FlagPointer, ptr0); err != nil {
		t.Fatalf("leaf1 AddLeafEntry(k050): %v", err)
	}
	leaf1.UpdateChecksum()

	leaf2Data, err := p.Get(2)
	if err != nil {
		t.Fatalf("Get leaf2 page: %v", err)
	}
	leaf2 := node.NewNode(leaf2Data)
	leaf2.SetType(page.PageTypeLeaf)
	leaf2.SetPageID(2)
	if err := leaf2.AddLeafEntry([]byte("k150"), []byte("v150"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("leaf2 AddLeafEntry(k150): %v", err)
	}
	leaf2.UpdateChecksum()

	rootData, err := p.Get(0)
	if err != nil {
		t.Fatalf("Get root page: %v", err)
	}
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeInternal)
	root.SetPageID(0)
	if err := root.AddInternalChild([]byte("k000"), 1); err != nil {
		t.Fatalf("root AddInternalChild(k000): %v", err)
	}
	if err := root.AddInternalChild([]byte("k100"), 2); err != nil {
		t.Fatalf("root AddInternalChild(k100): %v", err)
	}
	root.UpdateChecksum()

	tr := New(p, reader, 0)

	entry, err := tr.GetEntry([]byte("k120"))
	if err != nil {
		t.Fatalf("GetEntry(k120): %v", err)
	}
	if string(entry.Value) != "v120" {
		t.Fatalf("GetEntry(k120) value = %q, want %q", entry.Value, "v120")
	}
	if entry.Flags&node.FlagPointer != 0 {
		t.Fatalf("GetEntry(k120) should be fence-resolved inline value, flags=%08b", entry.Flags)
	}

	if _, err := tr.GetEntryExact([]byte("k120")); err != ErrKeyNotFound {
		t.Fatalf("GetEntryExact(k120): expected ErrKeyNotFound, got %v", err)
	}
}

func TestTreeGetEntry_FencePredecessorLookupGlobalAppenderOnly(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "index.db")
	p, err := pager.Open(idxPath, 65536)
	if err != nil {
		t.Fatalf("Pager open failed: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(3); err != nil {
		t.Fatalf("Alloc pages: %v", err)
	}

	reader := newFenceLookupAppenderOnlyReader()
	ptr0 := reader.addBlock(map[string]string{
		"k120": "v120",
	})

	leaf1Data, err := p.Get(1)
	if err != nil {
		t.Fatalf("Get leaf1 page: %v", err)
	}
	leaf1 := node.NewNode(leaf1Data)
	leaf1.SetType(page.PageTypeLeaf)
	leaf1.SetPageID(1)
	if err := leaf1.AddLeafEntry([]byte("k050"), nil, node.FlagPointer, ptr0); err != nil {
		t.Fatalf("leaf1 AddLeafEntry(k050): %v", err)
	}
	leaf1.UpdateChecksum()

	leaf2Data, err := p.Get(2)
	if err != nil {
		t.Fatalf("Get leaf2 page: %v", err)
	}
	leaf2 := node.NewNode(leaf2Data)
	leaf2.SetType(page.PageTypeLeaf)
	leaf2.SetPageID(2)
	if err := leaf2.AddLeafEntry([]byte("k150"), []byte("v150"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("leaf2 AddLeafEntry(k150): %v", err)
	}
	leaf2.UpdateChecksum()

	rootData, err := p.Get(0)
	if err != nil {
		t.Fatalf("Get root page: %v", err)
	}
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeInternal)
	root.SetPageID(0)
	if err := root.AddInternalChild([]byte("k000"), 1); err != nil {
		t.Fatalf("root AddInternalChild(k000): %v", err)
	}
	if err := root.AddInternalChild([]byte("k100"), 2); err != nil {
		t.Fatalf("root AddInternalChild(k100): %v", err)
	}
	root.UpdateChecksum()

	tr := New(p, reader, 0)

	entry, err := tr.GetEntry([]byte("k120"))
	if err != nil {
		t.Fatalf("GetEntry(k120): %v", err)
	}
	if string(entry.Value) != "v120" {
		t.Fatalf("GetEntry(k120) value = %q, want %q", entry.Value, "v120")
	}
	if reader.fenceAppendCalls != 1 {
		t.Fatalf("fence append calls = %d, want 1", reader.fenceAppendCalls)
	}
}

func TestTreeGetEntry_FencePredecessorLookupGlobalErrorPropagates(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "index.db")
	p, err := pager.Open(idxPath, 65536)
	if err != nil {
		t.Fatalf("Pager open failed: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(3); err != nil {
		t.Fatalf("Alloc pages: %v", err)
	}

	reader := newFenceLookupReader()
	missingPtr := page.ValuePtr{FileID: page.ValueLogFileID(1), Offset: 999, Length: 1}

	leaf1Data, err := p.Get(1)
	if err != nil {
		t.Fatalf("Get leaf1 page: %v", err)
	}
	leaf1 := node.NewNode(leaf1Data)
	leaf1.SetType(page.PageTypeLeaf)
	leaf1.SetPageID(1)
	if err := leaf1.AddLeafEntry([]byte("k050"), nil, node.FlagPointer, missingPtr); err != nil {
		t.Fatalf("leaf1 AddLeafEntry(k050): %v", err)
	}
	leaf1.UpdateChecksum()

	leaf2Data, err := p.Get(2)
	if err != nil {
		t.Fatalf("Get leaf2 page: %v", err)
	}
	leaf2 := node.NewNode(leaf2Data)
	leaf2.SetType(page.PageTypeLeaf)
	leaf2.SetPageID(2)
	if err := leaf2.AddLeafEntry([]byte("k150"), []byte("v150"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("leaf2 AddLeafEntry(k150): %v", err)
	}
	leaf2.UpdateChecksum()

	rootData, err := p.Get(0)
	if err != nil {
		t.Fatalf("Get root page: %v", err)
	}
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeInternal)
	root.SetPageID(0)
	if err := root.AddInternalChild([]byte("k000"), 1); err != nil {
		t.Fatalf("root AddInternalChild(k000): %v", err)
	}
	if err := root.AddInternalChild([]byte("k100"), 2); err != nil {
		t.Fatalf("root AddInternalChild(k100): %v", err)
	}
	root.UpdateChecksum()

	tr := New(p, reader, 0)

	_, err = tr.GetEntry([]byte("k120"))
	if err == nil {
		t.Fatalf("GetEntry(k120): expected non-nil error")
	}
	if errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("GetEntry(k120): expected source error, got ErrKeyNotFound")
	}
	if !errors.Is(err, errFenceLookupMissingBlock) {
		t.Fatalf("GetEntry(k120): expected errFenceLookupMissingBlock, got %v", err)
	}
	if reader.fenceCalls != 1 {
		t.Fatalf("fence calls = %d, want 1", reader.fenceCalls)
	}
	if reader.fenceAppendCalls != 0 {
		t.Fatalf("fence append calls = %d, want 0", reader.fenceAppendCalls)
	}
}

func TestTreeGetEntry_FencePredecessorLookupGlobalAppenderOnlyErrorPropagates(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "index.db")
	p, err := pager.Open(idxPath, 65536)
	if err != nil {
		t.Fatalf("Pager open failed: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(3); err != nil {
		t.Fatalf("Alloc pages: %v", err)
	}

	reader := newFenceLookupAppenderOnlyReader()
	missingPtr := page.ValuePtr{FileID: page.ValueLogFileID(1), Offset: 999, Length: 1}

	leaf1Data, err := p.Get(1)
	if err != nil {
		t.Fatalf("Get leaf1 page: %v", err)
	}
	leaf1 := node.NewNode(leaf1Data)
	leaf1.SetType(page.PageTypeLeaf)
	leaf1.SetPageID(1)
	if err := leaf1.AddLeafEntry([]byte("k050"), nil, node.FlagPointer, missingPtr); err != nil {
		t.Fatalf("leaf1 AddLeafEntry(k050): %v", err)
	}
	leaf1.UpdateChecksum()

	leaf2Data, err := p.Get(2)
	if err != nil {
		t.Fatalf("Get leaf2 page: %v", err)
	}
	leaf2 := node.NewNode(leaf2Data)
	leaf2.SetType(page.PageTypeLeaf)
	leaf2.SetPageID(2)
	if err := leaf2.AddLeafEntry([]byte("k150"), []byte("v150"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("leaf2 AddLeafEntry(k150): %v", err)
	}
	leaf2.UpdateChecksum()

	rootData, err := p.Get(0)
	if err != nil {
		t.Fatalf("Get root page: %v", err)
	}
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeInternal)
	root.SetPageID(0)
	if err := root.AddInternalChild([]byte("k000"), 1); err != nil {
		t.Fatalf("root AddInternalChild(k000): %v", err)
	}
	if err := root.AddInternalChild([]byte("k100"), 2); err != nil {
		t.Fatalf("root AddInternalChild(k100): %v", err)
	}
	root.UpdateChecksum()

	tr := New(p, reader, 0)

	_, err = tr.GetEntry([]byte("k120"))
	if err == nil {
		t.Fatalf("GetEntry(k120): expected non-nil error")
	}
	if errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("GetEntry(k120): expected source error, got ErrKeyNotFound")
	}
	if !errors.Is(err, errFenceLookupMissingBlock) {
		t.Fatalf("GetEntry(k120): expected errFenceLookupMissingBlock, got %v", err)
	}
	if reader.fenceAppendCalls != 1 {
		t.Fatalf("fence append calls = %d, want 1", reader.fenceAppendCalls)
	}
}

func TestTreeGetEntry_FencePredecessorLookupGlobalClassifierSkip(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "index.db")
	p, err := pager.Open(idxPath, 65536)
	if err != nil {
		t.Fatalf("Pager open failed: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(3); err != nil {
		t.Fatalf("Alloc pages: %v", err)
	}

	reader := newFenceLookupClassifierReader()
	ptr0 := reader.addBlock(map[string]string{
		"k120": "v120",
	})
	reader.likely[ptr0] = false

	leaf1Data, err := p.Get(1)
	if err != nil {
		t.Fatalf("Get leaf1 page: %v", err)
	}
	leaf1 := node.NewNode(leaf1Data)
	leaf1.SetType(page.PageTypeLeaf)
	leaf1.SetPageID(1)
	if err := leaf1.AddLeafEntry([]byte("k050"), nil, node.FlagPointer, ptr0); err != nil {
		t.Fatalf("leaf1 AddLeafEntry(k050): %v", err)
	}
	leaf1.UpdateChecksum()

	leaf2Data, err := p.Get(2)
	if err != nil {
		t.Fatalf("Get leaf2 page: %v", err)
	}
	leaf2 := node.NewNode(leaf2Data)
	leaf2.SetType(page.PageTypeLeaf)
	leaf2.SetPageID(2)
	if err := leaf2.AddLeafEntry([]byte("k150"), []byte("v150"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("leaf2 AddLeafEntry(k150): %v", err)
	}
	leaf2.UpdateChecksum()

	rootData, err := p.Get(0)
	if err != nil {
		t.Fatalf("Get root page: %v", err)
	}
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeInternal)
	root.SetPageID(0)
	if err := root.AddInternalChild([]byte("k000"), 1); err != nil {
		t.Fatalf("root AddInternalChild(k000): %v", err)
	}
	if err := root.AddInternalChild([]byte("k100"), 2); err != nil {
		t.Fatalf("root AddInternalChild(k100): %v", err)
	}
	root.UpdateChecksum()

	tr := New(p, reader, 0)

	beforeTotalCalls := reader.fenceCalls + reader.fenceAppendCalls
	if _, err := tr.GetEntry([]byte("k120")); err != ErrKeyNotFound {
		t.Fatalf("GetEntry(k120): expected ErrKeyNotFound, got %v", err)
	}
	if gotCalls := (reader.fenceCalls + reader.fenceAppendCalls) - beforeTotalCalls; gotCalls != 0 {
		t.Fatalf("GetEntry(k120) issued %d fence lookups despite predecessor classified non-fence", gotCalls)
	}
}

func TestTreeGetEntry_FencePredecessorLookupGlobalExplicitFenceMarkerOverridesClassifier(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "index.db")
	p, err := pager.Open(idxPath, 65536)
	if err != nil {
		t.Fatalf("Pager open failed: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(3); err != nil {
		t.Fatalf("Alloc pages: %v", err)
	}

	reader := newFenceLookupClassifierReader()
	ptrRaw := reader.addBlock(map[string]string{
		"k120": "v120",
	})
	ptrMarked := page.ValuePtrMarkFenceOuter(ptrRaw)
	if ptrMarked == ptrRaw {
		t.Fatalf("expected non-grouped pointer to be fence-marked")
	}
	reader.blocks[ptrMarked] = reader.blocks[ptrRaw]
	delete(reader.blocks, ptrRaw)
	reader.likely[ptrMarked] = false // explicit fence marker must still probe

	leaf1Data, err := p.Get(1)
	if err != nil {
		t.Fatalf("Get leaf1 page: %v", err)
	}
	leaf1 := node.NewNode(leaf1Data)
	leaf1.SetType(page.PageTypeLeaf)
	leaf1.SetPageID(1)
	if err := leaf1.AddLeafEntry([]byte("k050"), nil, node.FlagPointer, ptrMarked); err != nil {
		t.Fatalf("leaf1 AddLeafEntry(k050): %v", err)
	}
	leaf1.UpdateChecksum()

	leaf2Data, err := p.Get(2)
	if err != nil {
		t.Fatalf("Get leaf2 page: %v", err)
	}
	leaf2 := node.NewNode(leaf2Data)
	leaf2.SetType(page.PageTypeLeaf)
	leaf2.SetPageID(2)
	if err := leaf2.AddLeafEntry([]byte("k150"), []byte("v150"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("leaf2 AddLeafEntry(k150): %v", err)
	}
	leaf2.UpdateChecksum()

	rootData, err := p.Get(0)
	if err != nil {
		t.Fatalf("Get root page: %v", err)
	}
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeInternal)
	root.SetPageID(0)
	if err := root.AddInternalChild([]byte("k000"), 1); err != nil {
		t.Fatalf("root AddInternalChild(k000): %v", err)
	}
	if err := root.AddInternalChild([]byte("k100"), 2); err != nil {
		t.Fatalf("root AddInternalChild(k100): %v", err)
	}
	root.UpdateChecksum()

	tr := New(p, reader, 0)

	beforeTotalCalls := reader.fenceCalls + reader.fenceAppendCalls
	entry, err := tr.GetEntry([]byte("k120"))
	if err != nil {
		t.Fatalf("GetEntry(k120): %v", err)
	}
	if string(entry.Value) != "v120" {
		t.Fatalf("GetEntry(k120) value = %q, want %q", entry.Value, "v120")
	}
	if gotCalls := (reader.fenceCalls + reader.fenceAppendCalls) - beforeTotalCalls; gotCalls != 1 {
		t.Fatalf("GetEntry(k120) fence lookups = %d, want 1 (explicit marker must override classifier)", gotCalls)
	}
}

func TestTreeGetEntry_FencePredecessorLookupGlobalRespectsScanLimit(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "index.db")
	p, err := pager.Open(idxPath, 65536)
	if err != nil {
		t.Fatalf("Pager open failed: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(3); err != nil {
		t.Fatalf("Alloc pages: %v", err)
	}

	reader := newFenceLookupReader()
	oldestPtr := reader.addBlock(map[string]string{
		"k120": "v120",
	})

	leaf1Data, err := p.Get(1)
	if err != nil {
		t.Fatalf("Get leaf1 page: %v", err)
	}
	leaf1 := node.NewNode(leaf1Data)
	leaf1.SetType(page.PageTypeLeaf)
	leaf1.SetPageID(1)
	if err := leaf1.AddLeafEntry([]byte("k000"), nil, node.FlagPointer, oldestPtr); err != nil {
		t.Fatalf("leaf1 AddLeafEntry(k000): %v", err)
	}
	for i := 1; i < fenceGlobalFallbackScanLimit+8; i++ {
		ptr := reader.addBlock(map[string]string{
			fmt.Sprintf("x%03d", i): "vx",
		})
		if err := leaf1.AddLeafEntry([]byte(fmt.Sprintf("k%03d", i)), nil, node.FlagPointer, ptr); err != nil {
			t.Fatalf("leaf1 AddLeafEntry(k%03d): %v", i, err)
		}
	}
	leaf1.UpdateChecksum()

	leaf2Data, err := p.Get(2)
	if err != nil {
		t.Fatalf("Get leaf2 page: %v", err)
	}
	leaf2 := node.NewNode(leaf2Data)
	leaf2.SetType(page.PageTypeLeaf)
	leaf2.SetPageID(2)
	if err := leaf2.AddLeafEntry([]byte("k150"), []byte("v150"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("leaf2 AddLeafEntry(k150): %v", err)
	}
	leaf2.UpdateChecksum()

	rootData, err := p.Get(0)
	if err != nil {
		t.Fatalf("Get root page: %v", err)
	}
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeInternal)
	root.SetPageID(0)
	if err := root.AddInternalChild([]byte("k000"), 1); err != nil {
		t.Fatalf("root AddInternalChild(k000): %v", err)
	}
	if err := root.AddInternalChild([]byte("k100"), 2); err != nil {
		t.Fatalf("root AddInternalChild(k100): %v", err)
	}
	root.UpdateChecksum()

	tr := New(p, reader, 0)

	if _, err := tr.GetEntry([]byte("k120")); err != ErrKeyNotFound {
		t.Fatalf("GetEntry(k120): expected ErrKeyNotFound at scan limit, got %v", err)
	}
	if reader.fenceCalls != fenceGlobalFallbackScanLimit {
		t.Fatalf("fence calls = %d, want %d (scan limit)", reader.fenceCalls, fenceGlobalFallbackScanLimit)
	}
}

func TestTreeGetEntry_FencePredecessorLookupGlobalDisabled(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "index.db")
	p, err := pager.Open(idxPath, 65536)
	if err != nil {
		t.Fatalf("Pager open failed: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(3); err != nil {
		t.Fatalf("Alloc pages: %v", err)
	}

	reader := newFenceLookupReaderWithScope(false, false)
	ptr := reader.addBlock(map[string]string{
		"k120": "v120",
	})

	leaf1Data, err := p.Get(1)
	if err != nil {
		t.Fatalf("Get leaf1 page: %v", err)
	}
	leaf1 := node.NewNode(leaf1Data)
	leaf1.SetType(page.PageTypeLeaf)
	leaf1.SetPageID(1)
	if err := leaf1.AddLeafEntry([]byte("k000"), nil, node.FlagPointer, ptr); err != nil {
		t.Fatalf("leaf1 AddLeafEntry(k000): %v", err)
	}
	leaf1.UpdateChecksum()

	leaf2Data, err := p.Get(2)
	if err != nil {
		t.Fatalf("Get leaf2 page: %v", err)
	}
	leaf2 := node.NewNode(leaf2Data)
	leaf2.SetType(page.PageTypeLeaf)
	leaf2.SetPageID(2)
	if err := leaf2.AddLeafEntry([]byte("k150"), []byte("v150"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("leaf2 AddLeafEntry(k150): %v", err)
	}
	leaf2.UpdateChecksum()

	rootData, err := p.Get(0)
	if err != nil {
		t.Fatalf("Get root page: %v", err)
	}
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeInternal)
	root.SetPageID(0)
	if err := root.AddInternalChild([]byte("k000"), 1); err != nil {
		t.Fatalf("root AddInternalChild(k000): %v", err)
	}
	if err := root.AddInternalChild([]byte("k100"), 2); err != nil {
		t.Fatalf("root AddInternalChild(k100): %v", err)
	}
	root.UpdateChecksum()

	tr := New(p, reader, 0)

	if _, err := tr.GetEntry([]byte("k120")); err != ErrKeyNotFound {
		t.Fatalf("GetEntry(k120): expected ErrKeyNotFound with global fallback disabled, got %v", err)
	}
	if _, err := tr.Get([]byte("k120")); err != ErrKeyNotFound {
		t.Fatalf("Get(k120): expected ErrKeyNotFound with global fallback disabled, got %v", err)
	}
	if _, err := tr.GetAppend([]byte("k120"), nil); err != ErrKeyNotFound {
		t.Fatalf("GetAppend(k120): expected ErrKeyNotFound with global fallback disabled, got %v", err)
	}
	if has, err := tr.Has([]byte("k120")); err != nil {
		t.Fatalf("Has(k120): %v", err)
	} else if has {
		t.Fatalf("Has(k120) = true, want false with global fallback disabled")
	}
	if srcKey, ptr, ok, err := tr.LookupFencePointerOrigin([]byte("k120")); err != nil {
		t.Fatalf("LookupFencePointerOrigin(k120): %v", err)
	} else if ok {
		t.Fatalf("LookupFencePointerOrigin(k120) returned key=%q ptr=%+v want no source", srcKey, ptr)
	}
	if ptr, ok, err := tr.LookupFencePointerSource([]byte("k120")); err != nil {
		t.Fatalf("LookupFencePointerSource(k120): %v", err)
	} else if ok {
		t.Fatalf("LookupFencePointerSource(k120) returned ptr=%+v want no source", ptr)
	}
	if reader.fenceCalls != 0 {
		t.Fatalf("fence calls = %d, want 0 with global fallback disabled", reader.fenceCalls)
	}
}

func TestTreeGetEntry_FencePredecessorLookupSingleCandidateBounded(t *testing.T) {
	build := func(t *testing.T, reader *fenceLookupReader) *Tree {
		t.Helper()
		dir := t.TempDir()
		idxPath := filepath.Join(dir, "index.db")
		p, err := pager.Open(idxPath, 65536)
		if err != nil {
			t.Fatalf("Pager open failed: %v", err)
		}
		t.Cleanup(func() { _ = p.Close() })

		if _, err := p.Alloc(1); err != nil {
			t.Fatalf("Alloc pages: %v", err)
		}

		ptrFar := reader.addBlock(map[string]string{
			"k025": "v025",
		})
		ptrNear := reader.addBlock(map[string]string{
			"k020": "v020",
		})

		leafData, err := p.Get(0)
		if err != nil {
			t.Fatalf("Get leaf page: %v", err)
		}
		leaf := node.NewNode(leafData)
		leaf.SetType(page.PageTypeLeaf)
		leaf.SetPageID(0)
		if err := leaf.AddLeafEntry([]byte("k010"), nil, node.FlagPointer, ptrFar); err != nil {
			t.Fatalf("AddLeafEntry(k010): %v", err)
		}
		if err := leaf.AddLeafEntry([]byte("k020"), nil, node.FlagPointer, ptrNear); err != nil {
			t.Fatalf("AddLeafEntry(k020): %v", err)
		}
		if err := leaf.AddLeafEntry([]byte("k030"), []byte("v030"), node.FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("AddLeafEntry(k030): %v", err)
		}
		leaf.UpdateChecksum()
		return New(p, reader, 0)
	}

	t.Run("single-candidate-miss", func(t *testing.T) {
		reader := newFenceLookupReaderWithScope(false, true)
		tr := build(t, reader)
		if _, err := tr.GetEntry([]byte("k025")); err != ErrKeyNotFound {
			t.Fatalf("GetEntry(k025): expected ErrKeyNotFound in single-candidate mode, got %v", err)
		}
		if reader.fenceCalls != 1 {
			t.Fatalf("fence calls = %d, want 1 in single-candidate mode", reader.fenceCalls)
		}
	})

	t.Run("unbounded-predecessor-hit", func(t *testing.T) {
		reader := newFenceLookupReaderWithScope(false, false)
		tr := build(t, reader)
		entry, err := tr.GetEntry([]byte("k025"))
		if err != nil {
			t.Fatalf("GetEntry(k025): %v", err)
		}
		if string(entry.Value) != "v025" {
			t.Fatalf("GetEntry(k025) value = %q, want %q", entry.Value, "v025")
		}
		if reader.fenceCalls != 2 {
			t.Fatalf("fence calls = %d, want 2 with unbounded predecessor scan", reader.fenceCalls)
		}
	})
}
