package tree

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func newTreeWithLeafLogRoot(t *testing.T, sr SlabReader, key []byte, ptr page.LeafLogPtr) (*Tree, func()) {
	t.Helper()
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatalf("open pager: %v", err)
	}
	rootID, err := p.Alloc(1)
	if err != nil {
		_ = p.Close()
		t.Fatalf("alloc root: %v", err)
	}
	data, err := p.GetForWrite(rootID)
	if err != nil {
		_ = p.Close()
		t.Fatalf("get root: %v", err)
	}
	b := node.NewBuilder(data, page.PageTypeInternal)
	b.SetPageID(rootID)
	if key == nil {
		key = []byte{}
	}
	if err := b.AddInternalChildRef(key, page.LeafLogChildRef(ptr)); err != nil {
		_ = p.Close()
		t.Fatalf("AddInternalChildRef: %v", err)
	}
	b.FinishNoNode()
	closeFn := func() { _ = p.Close() }
	t.Cleanup(closeFn)
	return New(p, sr, rootID), closeFn
}

type trackedValueReader struct {
	*mapValueReader
	readUnsafeCalls       int
	readUnsafeAppendCalls int
}

type trackedValueReaderWithChecksumMode struct {
	*trackedValueReader
	readChecksumEnabled bool
}

type trackedLeafLogPageReader struct {
	*trackedValueReader
	leafLogPageCalls int
}

func (r *trackedValueReaderWithChecksumMode) ReadChecksumEnabled() bool {
	return r.readChecksumEnabled
}

func (r *trackedValueReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	r.readUnsafeCalls++
	return r.mapValueReader.ReadUnsafe(ptr)
}

func (r *trackedValueReader) ReadUnsafeAppend(ptr page.ValuePtr, dst []byte) ([]byte, error) {
	r.readUnsafeAppendCalls++
	return r.mapValueReader.ReadUnsafeAppend(ptr, dst)
}

func (r *trackedLeafLogPageReader) ReadLeafLogPageUnsafeTo(ptr page.LeafLogPtr, dst []byte) ([]byte, bool, error) {
	r.leafLogPageCalls++
	val, err := r.mapValueReader.ReadUnsafe(ptr.ValuePtr())
	if err != nil {
		return nil, false, err
	}
	if cap(dst) >= len(val) {
		out := dst[:len(val)]
		copy(out, val)
		return out, true, nil
	}
	return val, false, nil
}

type statefulLeafLogPageReader struct {
	*mapValueReader
	state    LeafLogPageReadState
	reads    int
	views    int
	releases int
	marks    int
}

type statefulLeafLogPageViewLease struct {
	reader *statefulLeafLogPageReader
}

func (l *statefulLeafLogPageViewLease) ReleaseLeafLogPageView() {
	if l != nil && l.reader != nil {
		l.reader.releases++
	}
}

func (l *statefulLeafLogPageViewLease) MarkLeafLogPageViewChecksumVerified() {
	if l != nil && l.reader != nil {
		l.reader.MarkLeafLogPageChecksumVerified(page.LeafLogPtr{})
	}
}

func (r *statefulLeafLogPageReader) ReadChecksumEnabled() bool { return true }

func (r *statefulLeafLogPageReader) ReadLeafLogPageUnsafeToWithState(ptr page.LeafLogPtr, dst []byte) ([]byte, bool, LeafLogPageReadState, error) {
	r.reads++
	val, err := r.mapValueReader.ReadUnsafe(ptr.ValuePtr())
	if err != nil {
		return nil, false, LeafLogPageReadState{}, err
	}
	if cap(dst) >= len(val) {
		out := dst[:len(val)]
		copy(out, val)
		return out, true, r.state, nil
	}
	return val, false, r.state, nil
}

func (r *statefulLeafLogPageReader) ReadLeafLogPageUnsafeViewWithState(ptr page.LeafLogPtr) ([]byte, LeafLogPageViewLease, bool, LeafLogPageReadState, error) {
	r.views++
	val, err := r.mapValueReader.ReadUnsafe(ptr.ValuePtr())
	if err != nil {
		return nil, nil, false, LeafLogPageReadState{}, err
	}
	return val, &statefulLeafLogPageViewLease{reader: r}, true, r.state, nil
}

func (r *statefulLeafLogPageReader) MarkLeafLogPageChecksumVerified(ptr page.LeafLogPtr) {
	r.marks++
	r.state.PageChecksumVerified = true
}

func TestTreeLeafLogVerifiedCacheStateSkipsChecksum(t *testing.T) {
	before := OuterLeafReadStatsSnapshot()
	reader := &statefulLeafLogPageReader{mapValueReader: newMapValueReader(), state: LeafLogPageReadState{RecordChecksumVerified: true, CacheEntryPresent: true}}
	leafData := make([]byte, page.PageSize)
	leaf := node.NewNode(leafData)
	leaf.SetType(page.PageTypeLeaf)
	leaf.SetPageID(1)
	if err := leaf.AddLeafEntry([]byte("k"), []byte("value"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("AddLeafEntry: %v", err)
	}
	leaf.UpdateChecksum()

	ptr := page.LeafLogPtr{FileID: 1, Offset: 8, RecordLengthHint: page.PageSize}
	reader.values[ptr.ValuePtr()] = leafData
	tr, _ := newTreeWithLeafLogRoot(t, reader, []byte{}, ptr)

	got, err := tr.GetAppend([]byte("k"), nil)
	if err != nil {
		t.Fatalf("first GetAppend: %v", err)
	}
	if string(got) != "value" {
		t.Fatalf("first GetAppend=%q", got)
	}
	afterFirst := OuterLeafReadStatsSnapshot()
	if afterFirst.ChecksumVerifiedTotal-before.ChecksumVerifiedTotal != 1 {
		t.Fatalf("checksum verifications delta=%d want 1", afterFirst.ChecksumVerifiedTotal-before.ChecksumVerifiedTotal)
	}
	if afterFirst.ChecksumSkippedTotal != before.ChecksumSkippedTotal {
		t.Fatalf("unexpected checksum skip before verified cache hit")
	}
	if reader.views != 1 || reader.releases != 1 {
		t.Fatalf("view path calls/releases=%d/%d, want 1/1", reader.views, reader.releases)
	}
	if reader.reads != 0 {
		t.Fatalf("fallback leaf-log reads=%d, want view-state path", reader.reads)
	}
	if reader.marks != 1 || !reader.state.PageChecksumVerified {
		t.Fatalf("marks=%d state=%+v, want one verified mark", reader.marks, reader.state)
	}

	got, err = tr.GetAppend([]byte("k"), nil)
	if err != nil {
		t.Fatalf("second GetAppend: %v", err)
	}
	if string(got) != "value" {
		t.Fatalf("second GetAppend=%q", got)
	}
	afterSecond := OuterLeafReadStatsSnapshot()
	if afterSecond.ChecksumVerifiedTotal != afterFirst.ChecksumVerifiedTotal {
		t.Fatalf("checksum verification ran again: before=%d after=%d", afterFirst.ChecksumVerifiedTotal, afterSecond.ChecksumVerifiedTotal)
	}
	if afterSecond.ChecksumSkippedTotal-afterFirst.ChecksumSkippedTotal != 1 {
		t.Fatalf("checksum skips delta=%d want 1", afterSecond.ChecksumSkippedTotal-afterFirst.ChecksumSkippedTotal)
	}
}

func TestTreeLeafLogUnverifiedStateStillChecksChecksum(t *testing.T) {
	reader := &statefulLeafLogPageReader{mapValueReader: newMapValueReader(), state: LeafLogPageReadState{RecordChecksumVerified: true, CacheEntryPresent: true}}
	leafData := make([]byte, page.PageSize)
	leaf := node.NewNode(leafData)
	leaf.SetType(page.PageTypeLeaf)
	leaf.SetPageID(1)
	if err := leaf.AddLeafEntry([]byte("k"), []byte("value"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("AddLeafEntry: %v", err)
	}
	leaf.UpdateChecksum()
	leafData[node.NodeHeaderSize] ^= 0x80

	ptr := page.LeafLogPtr{FileID: 1, Offset: 8, RecordLengthHint: page.PageSize}
	reader.values[ptr.ValuePtr()] = leafData
	tr, _ := newTreeWithLeafLogRoot(t, reader, []byte{}, ptr)

	_, err := tr.GetAppend([]byte("k"), nil)
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("GetAppend error=%v, want checksum mismatch", err)
	}
	if reader.marks != 0 || reader.state.PageChecksumVerified {
		t.Fatalf("corrupt page should not be marked verified: marks=%d state=%+v", reader.marks, reader.state)
	}
}

func TestValidateLeafLogStateRequiresRecordChecksumForChecksumSkip(t *testing.T) {
	before := OuterLeafReadStatsSnapshot()
	leafData := make([]byte, page.PageSize)
	leaf := node.NewNode(leafData)
	leaf.SetType(page.PageTypeLeaf)
	leaf.SetPageID(1)
	if err := leaf.AddLeafEntry([]byte("k"), []byte("value"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("AddLeafEntry: %v", err)
	}
	leaf.UpdateChecksum()
	leafData[node.NodeHeaderSize] ^= 0x80

	var dst node.Node
	ptr := page.LeafLogPtr{FileID: 1, Offset: 8, RecordLengthHint: page.PageSize}
	_, err := validateLeafLogNodeIntoWithState(&dst, leafData, ptr, true, false, LeafLogPageReadState{
		CacheEntryPresent:      true,
		PageChecksumVerified:   true,
		RecordChecksumVerified: false,
	})
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("validate error=%v, want checksum mismatch", err)
	}
	after := OuterLeafReadStatsSnapshot()
	if after.ChecksumVerifiedTotal-before.ChecksumVerifiedTotal != 1 {
		t.Fatalf("checksum verifications delta=%d want 1", after.ChecksumVerifiedTotal-before.ChecksumVerifiedTotal)
	}
	if after.ChecksumSkippedTotal != before.ChecksumSkippedTotal {
		t.Fatalf("checksum skips delta=%d want 0", after.ChecksumSkippedTotal-before.ChecksumSkippedTotal)
	}
}

func TestTreeGetManyAppend_RejectsUndersizedOut(t *testing.T) {
	var tr *Tree
	arena := []byte("keep")
	got, err := tr.GetManyAppend([][]byte{[]byte("k0"), []byte("k1")}, make([][]byte, 1), arena)
	if err == nil || !strings.Contains(err.Error(), "out len 1 < keys len 2") {
		t.Fatalf("GetManyAppend undersized out error=%v, want clear size error", err)
	}
	if !bytes.Equal(got, arena) {
		t.Fatalf("GetManyAppend returned arena %q, want original %q", got, arena)
	}
}

func TestTreeGetManyAppend_ClearsFencePrunedMisses(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatalf("open pager: %v", err)
	}
	defer p.Close()

	rootID, err := p.Alloc(1)
	if err != nil {
		t.Fatalf("alloc root: %v", err)
	}
	leafID, err := p.Alloc(1)
	if err != nil {
		t.Fatalf("alloc leaf: %v", err)
	}
	leafData, err := p.GetForWrite(leafID)
	if err != nil {
		t.Fatalf("get leaf: %v", err)
	}
	leaf := node.NewNode(leafData)
	leaf.SetType(page.PageTypeLeaf)
	leaf.SetPageID(leafID)
	if err := leaf.AddLeafEntry([]byte("k001"), []byte("v001"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("AddLeafEntry: %v", err)
	}
	leaf.UpdateChecksum()

	rootData, err := p.GetForWrite(rootID)
	if err != nil {
		t.Fatalf("get root: %v", err)
	}
	b := node.NewBuilderWithOptions(rootData, page.PageTypeInternal, node.BuilderOptions{InternalBaseDelta: true})
	b.SetPageID(rootID)
	b.SetInternalFenceBounds([]byte("k000"), []byte("k100"))
	if err := b.AddInternalChild([]byte("k000"), leafID); err != nil {
		t.Fatalf("AddInternalChild: %v", err)
	}
	b.FinishNoNode()

	tr := New(p, panicValueReader{}, rootID)
	keys := make([][]byte, getManyLeafGroupMinKeys)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("z%03d", i))
	}
	out := make([][]byte, len(keys))
	for i := range out {
		out[i] = []byte("stale")
	}

	if _, err := tr.GetManyAppend(keys, out, make([]byte, 0, len(keys)*8)); err != nil {
		t.Fatalf("GetManyAppend: %v", err)
	}
	for i, got := range out {
		if got != nil {
			t.Fatalf("out[%d]=%q, want nil for fence-pruned miss", i, got)
		}
	}
}

func TestTreeGetManyAppend_GroupsLeafLogLoads(t *testing.T) {
	tracked := &trackedLeafLogPageReader{trackedValueReader: &trackedValueReader{mapValueReader: newMapValueReader()}}
	leafData := make([]byte, page.PageSize)
	leaf := node.NewNode(leafData)
	leaf.SetType(page.PageTypeLeaf)
	leaf.SetPageID(1)
	pointerValue := []byte("pointer-value")
	pointerValuePtr := tracked.Add(pointerValue)
	want := make(map[string][]byte)
	for i := 0; i < getManyLeafGroupMinKeys+16; i++ {
		key := []byte(fmt.Sprintf("k%03d", i))
		value := []byte(fmt.Sprintf("v%03d", i))
		flags := byte(node.FlagInline)
		ptr := page.ValuePtr{}
		switch i {
		case 0:
			value = nil
			flags = node.FlagPointer
			ptr = pointerValuePtr
			want[string(key)] = pointerValue
		case 4:
			value = nil
			want[string(key)] = []byte{}
		default:
			want[string(key)] = value
		}
		if err := leaf.AddLeafEntry(key, value, flags, ptr); err != nil {
			t.Fatalf("AddLeafEntry(%q): %v", key, err)
		}
	}
	leaf.UpdateChecksum()

	ptr := page.LeafLogPtr{FileID: 1, Offset: 8, RecordLengthHint: page.PageSize}
	tracked.values[ptr.ValuePtr()] = leafData
	tr, _ := newTreeWithLeafLogRoot(t, tracked, []byte{}, ptr)

	keys := make([][]byte, 0, getManyLeafGroupMinKeys+20)
	keys = append(keys, []byte("k001"), []byte("missing"), []byte("k002"), []byte("k001"), []byte("k000"), []byte("k004"))
	for i := 3; len(keys) < getManyLeafGroupMinKeys+20; i++ {
		keys = append(keys, []byte(fmt.Sprintf("k%03d", i)))
	}
	out := make([][]byte, len(keys))
	before := GetManyReadStatsSnapshot()
	arena, err := tr.GetManyAppend(keys, out, make([]byte, 0, len(keys)*8))
	if err != nil {
		t.Fatalf("GetManyAppend: %v", err)
	}
	if len(arena) == 0 {
		t.Fatal("expected arena-backed values")
	}
	if tracked.leafLogPageCalls != 1 {
		t.Fatalf("leaf-log page loads=%d, want 1 grouped load", tracked.leafLogPageCalls)
	}
	if out[1] != nil {
		t.Fatalf("missing key out[1]=%q, want nil", out[1])
	}
	for i, key := range keys {
		if i == 1 {
			continue
		}
		if !bytes.Equal(out[i], want[string(key)]) {
			t.Fatalf("out[%d] for key %q=%q want %q", i, key, out[i], want[string(key)])
		}
	}
	if !bytes.Equal(out[4], pointerValue) {
		t.Fatalf("pointer-backed grouped value=%q want %q", out[4], pointerValue)
	}
	if out[5] == nil || len(out[5]) != 0 {
		t.Fatalf("empty grouped value=%q, want non-nil empty", out[5])
	}
	if tracked.readUnsafeAppendCalls == 0 {
		t.Fatal("expected grouped pointer value to use append-style pointer read")
	}
	out[0][0] = 'X'
	if !bytes.Equal(out[3], want[string(keys[3])]) {
		t.Fatalf("duplicate value aliased after mutation: got %q want %q", out[3], want[string(keys[3])])
	}
	after := GetManyReadStatsSnapshot()
	if after.GroupedCallsTotal-before.GroupedCallsTotal != 1 {
		t.Fatalf("grouped calls delta=%d, want 1", after.GroupedCallsTotal-before.GroupedCallsTotal)
	}
	if after.LeafLoadsSavedTotal <= before.LeafLoadsSavedTotal {
		t.Fatalf("expected leaf-load savings counter to increase: before=%d after=%d", before.LeafLoadsSavedTotal, after.LeafLoadsSavedTotal)
	}
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

func TestTreeGet_UsesAppendReaderForPointersAndReturnsSafeCopy(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "index.db")
	p, err := pager.Open(idxPath, 65536)
	if err != nil {
		t.Fatalf("Pager open failed: %v", err)
	}
	defer p.Close()

	rootID, err := p.Alloc(1)
	if err != nil {
		t.Fatalf("Alloc root: %v", err)
	}
	tracked := &trackedValueReader{mapValueReader: newMapValueReader()}
	expected := []byte("pointer-value")
	ptr := tracked.Add(expected)

	rootData, err := p.Get(rootID)
	if err != nil {
		t.Fatalf("Get root page: %v", err)
	}
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeLeaf)
	root.SetPageID(rootID)
	root.AddLeafEntry([]byte("k"), nil, node.FlagPointer, ptr)
	root.UpdateChecksum()

	tr := New(p, tracked, rootID)
	got, err := tr.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, expected) {
		t.Fatalf("unexpected value: %q", got)
	}
	if tracked.readUnsafeAppendCalls != 1 {
		t.Fatalf("expected ReadUnsafeAppend to be used once, got %d", tracked.readUnsafeAppendCalls)
	}
	if tracked.readUnsafeCalls != 0 {
		t.Fatalf("expected ReadUnsafe to be bypassed, got %d", tracked.readUnsafeCalls)
	}

	got[0] = 'X'
	gotAgain, err := tr.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get second read failed: %v", err)
	}
	if !bytes.Equal(gotAgain, expected) {
		t.Fatalf("Get should return a safe copy, got %q", gotAgain)
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

func TestTreeGet_ZeroLengthValueReturnsNonNilEmpty(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "index.db")
	p, err := pager.Open(idxPath, 65536)
	if err != nil {
		t.Fatalf("Pager open failed: %v", err)
	}
	defer p.Close()

	rootID, err := p.Alloc(1)
	if err != nil {
		t.Fatalf("Alloc root: %v", err)
	}
	rootData, err := p.Get(rootID)
	if err != nil {
		t.Fatalf("Get root page: %v", err)
	}
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeLeaf)
	root.SetPageID(rootID)
	root.AddLeafEntry([]byte("empty"), []byte{}, node.FlagInline, page.ValuePtr{})
	root.UpdateChecksum()

	tr := New(p, newMapValueReader(), rootID)
	got, err := tr.Get([]byte("empty"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("expected non-nil zero-length value, got %#v", got)
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

	rootID, err := p.Alloc(1)
	if err != nil {
		t.Fatalf("Alloc root: %v", err)
	}
	tracked := &trackedValueReader{mapValueReader: newMapValueReader()}
	ptr := tracked.Add([]byte("pointer-value"))

	rootData, err := p.Get(rootID)
	if err != nil {
		t.Fatalf("Get root page: %v", err)
	}
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeLeaf)
	root.SetPageID(rootID)
	root.AddLeafEntry([]byte("k"), nil, node.FlagPointer, ptr)
	root.UpdateChecksum()

	tr := New(p, tracked, rootID)
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

	ptr := page.LeafLogPtr{
		FileID: 1,
		Offset: 8,
	}
	tracked.values[ptr.ValuePtr()] = append([]byte(nil), leafData...)

	tr, closeTree := newTreeWithLeafLogRoot(t, tracked, []byte{}, ptr)
	defer closeTree()
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

func TestTreeGetAppend_UsesLeafLogPageReaderForLeafRefPages(t *testing.T) {
	tracked := &trackedLeafLogPageReader{
		trackedValueReader: &trackedValueReader{mapValueReader: newMapValueReader()},
	}

	leafData := make([]byte, page.PageSize)
	leaf := node.NewNode(leafData)
	leaf.SetType(page.PageTypeLeaf)
	leaf.SetPageID(1)
	leaf.AddLeafEntry([]byte("k"), []byte("v"), node.FlagInline, page.ValuePtr{})
	leaf.UpdateChecksum()

	ptr := page.LeafLogPtr{
		FileID: 1,
		Offset: 8,
	}
	tracked.values[ptr.ValuePtr()] = append([]byte(nil), leafData...)

	tr, closeTree := newTreeWithLeafLogRoot(t, tracked, []byte{}, ptr)
	defer closeTree()
	got, err := tr.GetAppend([]byte("k"), nil)
	if err != nil {
		t.Fatalf("GetAppend failed: %v", err)
	}
	if string(got) != "v" {
		t.Fatalf("unexpected value: %q", got)
	}
	if tracked.leafLogPageCalls != 1 {
		t.Fatalf("expected ReadLeafLogPageUnsafeTo to be used once, got %d", tracked.leafLogPageCalls)
	}
	if tracked.readUnsafeAppendCalls != 0 {
		t.Fatalf("expected generic ReadUnsafeAppend to be bypassed, got %d", tracked.readUnsafeAppendCalls)
	}
	if tracked.readUnsafeCalls != 0 {
		t.Fatalf("expected generic ReadUnsafe to be bypassed, got %d", tracked.readUnsafeCalls)
	}
	gotUnsafe, err := tr.GetUnsafe([]byte("k"))
	if err != nil {
		t.Fatalf("GetUnsafe failed: %v", err)
	}
	if string(gotUnsafe) != "v" {
		t.Fatalf("unexpected unsafe value: %q", gotUnsafe)
	}
	if tracked.leafLogPageCalls != 2 {
		t.Fatalf("expected ReadLeafLogPageUnsafeTo to cover GetUnsafe too, got %d calls", tracked.leafLogPageCalls)
	}
}

func TestTreeGetAppend_LeafRefChecksumPolicyHonored(t *testing.T) {
	makeCorruptLeaf := func() []byte {
		leafData := make([]byte, page.PageSize)
		leaf := node.NewNode(leafData)
		leaf.SetType(page.PageTypeLeaf)
		leaf.SetPageID(1)
		leaf.AddLeafEntry([]byte("k"), []byte("v"), node.FlagInline, page.ValuePtr{})
		leaf.UpdateChecksum()
		// Corrupt checksum field only (header bytes [8,12)).
		leafData[8] ^= 0x01
		return leafData
	}

	buildTree := func(checksumEnabled bool) *Tree {
		tracked := &trackedValueReaderWithChecksumMode{
			trackedValueReader:  &trackedValueReader{mapValueReader: newMapValueReader()},
			readChecksumEnabled: checksumEnabled,
		}
		ptr := page.LeafLogPtr{
			FileID: 1,
			Offset: 8,
		}
		tracked.values[ptr.ValuePtr()] = makeCorruptLeaf()
		tr, _ := newTreeWithLeafLogRoot(t, tracked, []byte{}, ptr)
		return tr
	}

	t.Run("verify_enabled", func(t *testing.T) {
		tr := buildTree(true)
		_, err := tr.GetAppend([]byte("k"), nil)
		if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
			t.Fatalf("expected checksum mismatch, got %v", err)
		}
	})

	t.Run("verify_disabled", func(t *testing.T) {
		tr := buildTree(false)
		got, err := tr.GetAppend([]byte("k"), nil)
		if err != nil {
			t.Fatalf("GetAppend failed: %v", err)
		}
		if string(got) != "v" {
			t.Fatalf("unexpected value: %q", got)
		}
	})
}

func TestTreeGetUnsafe_UsesUnsafeReaderForPointers(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "index.db")
	p, err := pager.Open(idxPath, 65536)
	if err != nil {
		t.Fatalf("Pager open failed: %v", err)
	}
	defer p.Close()

	rootID, err := p.Alloc(1)
	if err != nil {
		t.Fatalf("Alloc root: %v", err)
	}
	tracked := &trackedValueReader{mapValueReader: newMapValueReader()}
	ptr := tracked.Add([]byte("pointer-value"))

	rootData, err := p.Get(rootID)
	if err != nil {
		t.Fatalf("Get root page: %v", err)
	}
	root := node.NewNode(rootData)
	root.SetType(page.PageTypeLeaf)
	root.SetPageID(rootID)
	root.AddLeafEntry([]byte("k"), nil, node.FlagPointer, ptr)
	root.UpdateChecksum()

	tr := New(p, tracked, rootID)
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

func TestTreeGet_InternalFenceBoundsPrunesOutOfRange(t *testing.T) {
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
