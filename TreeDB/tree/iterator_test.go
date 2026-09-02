package tree

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

type countingValueReader struct {
	inner *mapValueReader
	reads int
}

func newCountingValueReader() *countingValueReader {
	return &countingValueReader{inner: newMapValueReader()}
}

func (r *countingValueReader) Add(value []byte) page.ValuePtr {
	return r.inner.Add(value)
}

func (r *countingValueReader) Read(ptr page.ValuePtr) ([]byte, error) {
	r.reads++
	return r.inner.Read(ptr)
}

func (r *countingValueReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	r.reads++
	return r.inner.ReadUnsafe(ptr)
}

type readOnlyLeafRefReader struct {
	values map[page.ValuePtr][]byte
}

func (r *readOnlyLeafRefReader) Read(ptr page.ValuePtr) ([]byte, error) {
	return r.ReadUnsafe(ptr)
}

func (r *readOnlyLeafRefReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	val, ok := r.values[ptr]
	if !ok {
		return nil, fmt.Errorf("value pointer not found")
	}
	return append([]byte(nil), val...), nil
}

func TestIterator_LeafRefFallbackCountsAsIteratorLoad(t *testing.T) {
	savedSampleMod := outerLeafReadSampleMod
	savedEstimator := outerLeafRecentReadEstimator
	outerLeafLoadsTotal.Store(0)
	outerLeafPointLoadsTotal.Store(0)
	outerLeafIteratorLoadsTotal.Store(0)
	outerLeafBytesTotal.Store(0)
	outerLeafSamplesTotal.Store(0)
	outerLeafRecent64HitsTotal.Store(0)
	outerLeafRecent256HitsTotal.Store(0)
	outerLeafRecent1KHitsTotal.Store(0)
	outerLeafRecent4KHitsTotal.Store(0)
	outerLeafReadSampleMod = 1
	outerLeafRecentReadEstimator = newOuterLeafRecentReadEstimator()
	defer func() {
		outerLeafReadSampleMod = savedSampleMod
		outerLeafRecentReadEstimator = savedEstimator
	}()

	leafPtr := page.LeafLogPtr{FileID: 1, Offset: 8}

	leafData := make([]byte, page.PageSize)
	leaf := node.NewNode(leafData)
	leaf.SetType(page.PageTypeLeaf)
	leaf.SetPageID(0)
	leaf.AddLeafEntry([]byte("k"), []byte("v"), node.FlagInline, page.ValuePtr{})
	leaf.UpdateChecksum()

	reader := &readOnlyLeafRefReader{
		values: map[page.ValuePtr][]byte{
			leafPtr.ValuePtr(): leafData,
		},
	}
	tr, closeTree := newTreeWithLeafLogRoot(t, reader, []byte{}, leafPtr)
	defer closeTree()
	it := tr.Iterator(nil, nil)
	defer it.Close()
	if !it.Valid() {
		t.Fatalf("expected valid iterator, err=%v", it.Error())
	}
	if got := string(it.Key()); got != "k" {
		t.Fatalf("Key=%q want %q", got, "k")
	}
	stats := OuterLeafReadStatsSnapshot()
	if stats.LoadsTotal == 0 {
		t.Fatalf("expected outer-leaf load to be counted")
	}
	if stats.IteratorLoadsTotal == 0 {
		t.Fatalf("expected iterator load to be counted")
	}
	if stats.PointLoadsTotal != 0 {
		t.Fatalf("PointLoadsTotal=%d want 0", stats.PointLoadsTotal)
	}
}

func TestIterator_LeafRefChecksumPolicyHonored(t *testing.T) {
	makeTree := func(checksumEnabled bool) *Tree {
		tracked := &trackedValueReaderWithChecksumMode{
			trackedValueReader:  &trackedValueReader{mapValueReader: newMapValueReader()},
			readChecksumEnabled: checksumEnabled,
		}
		leafData := make([]byte, page.PageSize)
		leaf := node.NewNode(leafData)
		leaf.SetType(page.PageTypeLeaf)
		leaf.SetPageID(1)
		leaf.AddLeafEntry([]byte("k"), []byte("v"), node.FlagInline, page.ValuePtr{})
		leaf.UpdateChecksum()
		leafData[8] ^= 0x01 // checksum field

		ptr := page.LeafLogPtr{
			FileID: 1,
			Offset: 8,
		}
		tracked.values[ptr.ValuePtr()] = leafData
		tr, _ := newTreeWithLeafLogRoot(t, tracked, []byte{}, ptr)
		return tr
	}

	t.Run("verify_enabled", func(t *testing.T) {
		tr := makeTree(true)
		it := tr.Iterator(nil, nil)
		defer it.Close()
		if it.Valid() {
			t.Fatalf("expected invalid iterator on checksum mismatch")
		}
		if err := it.Error(); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
			t.Fatalf("expected checksum mismatch error, got %v", err)
		}
	})

	t.Run("verify_disabled", func(t *testing.T) {
		tr := makeTree(false)
		it := tr.Iterator(nil, nil)
		defer it.Close()
		if !it.Valid() {
			t.Fatalf("expected valid iterator")
		}
		if got := string(it.Key()); got != "k" {
			t.Fatalf("key=%q want %q", got, "k")
		}
		if got := string(it.Value()); got != "v" {
			t.Fatalf("value=%q want %q", got, "v")
		}
		if err := it.Error(); err != nil {
			t.Fatalf("unexpected iterator error: %v", err)
		}
	})
}

func TestIteratorTrimReusableBuffers(t *testing.T) {
	it := &Iterator{}
	it.ptrScratch = make([]byte, 0, 128)
	it.prefetchPtrs = make([]page.ValuePtr, 0, 8)
	it.stack = make([]CursorItem, 0, 64)
	buf := it.trimReusableBuffers()
	if cap(buf.ptrScratch) != 128 {
		t.Fatalf("ptr scratch cap=%d want=128", cap(buf.ptrScratch))
	}
	if cap(buf.prefetchPtrs) != 8 {
		t.Fatalf("prefetch ptr cap=%d want=8", cap(buf.prefetchPtrs))
	}
	if cap(buf.stack) != 64 {
		t.Fatalf("stack cap=%d want=64", cap(buf.stack))
	}

	it = &Iterator{}
	it.ptrScratch = make([]byte, 0, iteratorPoolMaxScratchBytes+1)
	it.prefetchPtrs = make([]page.ValuePtr, 0, iteratorPoolMaxPrefetchCap+1)
	it.stack = make([]CursorItem, 0, iteratorPoolMaxStackCap+1)
	buf = it.trimReusableBuffers()
	if cap(buf.ptrScratch) != 0 {
		t.Fatalf("oversized ptr scratch should be dropped (cap=%d)", cap(buf.ptrScratch))
	}
	if cap(buf.prefetchPtrs) != 0 {
		t.Fatalf("oversized prefetch ptrs should be dropped (cap=%d)", cap(buf.prefetchPtrs))
	}
	if cap(buf.stack) != 0 {
		t.Fatalf("oversized stack should be dropped (cap=%d)", cap(buf.stack))
	}
}

func TestIteratorInstallReusableBuffersFallbacksToStackBuf(t *testing.T) {
	it := &Iterator{}
	it.installReusableBuffers(iteratorReusableBuffers{})
	if cap(it.stack) != len(it.stackBuf) {
		t.Fatalf("stack cap=%d want inline=%d", cap(it.stack), len(it.stackBuf))
	}
	if len(it.stack) != 0 {
		t.Fatalf("stack len=%d want 0", len(it.stack))
	}
}

func TestIterator(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Build Tree
	// Root (0) -> Internal
	// Child 1 (Left) -> "A", "C"
	// Child 2 (Right) -> "E", "G"

	p.Alloc(3) // 0, 1, 2

	// Leaf 1
	d1, _ := p.Get(1)
	n1 := node.NewNode(d1)
	n1.SetPageID(1)
	n1.SetType(page.PageTypeLeaf)
	n1.AddLeafEntry([]byte("A"), []byte("valA"), node.FlagInline, page.ValuePtr{})
	n1.AddLeafEntry([]byte("C"), []byte("valC"), node.FlagInline, page.ValuePtr{})
	n1.UpdateChecksum()

	// Leaf 2
	d2, _ := p.Get(2)
	n2 := node.NewNode(d2)
	n2.SetPageID(2)
	n2.SetType(page.PageTypeLeaf)
	n2.AddLeafEntry([]byte("E"), []byte("valE"), node.FlagInline, page.ValuePtr{})
	n2.AddLeafEntry([]byte("G"), []byte("valG"), node.FlagInline, page.ValuePtr{})
	n2.UpdateChecksum()

	// Root
	d0, _ := p.Get(0)
	n0 := node.NewNode(d0)
	n0.SetPageID(0)
	n0.SetType(page.PageTypeInternal)
	n0.AddInternalChild([]byte(""), 1)  // Min key
	n0.AddInternalChild([]byte("D"), 2) // Split key
	n0.UpdateChecksum()

	tr := New(p, panicValueReader{}, 0)

	// 1. Full Forward
	t.Run("Forward", func(t *testing.T) {
		it := tr.Iterator(nil, nil)
		expected := []string{"A", "C", "E", "G"}
		i := 0
		for ; it.Valid(); it.Next() {
			k := string(it.Key())
			if k != expected[i] {
				t.Errorf("Idx %d: expected %s, got %s", i, expected[i], k)
			}
			i++
		}
		if i != len(expected) {
			t.Errorf("Expected %d items, got %d", len(expected), i)
		}
		it.Close()
	})

	// 2. Full Reverse
	t.Run("Reverse", func(t *testing.T) {
		it := tr.ReverseIterator(nil, nil)
		expected := []string{"G", "E", "C", "A"}
		i := 0
		for ; it.Valid(); it.Next() {
			k := string(it.Key())
			if k != expected[i] {
				t.Errorf("Idx %d: expected %s, got %s", i, expected[i], k)
			}
			i++
		}
		if i != len(expected) {
			t.Errorf("Expected %d items, got %d", len(expected), i)
		}
		it.Close()
	})

	// 3. Range Forward [B, F) -> C, E
	t.Run("RangeForward", func(t *testing.T) {
		it := tr.Iterator([]byte("B"), []byte("F"))
		expected := []string{"C", "E"}
		i := 0
		for ; it.Valid(); it.Next() {
			k := string(it.Key())
			if k != expected[i] {
				t.Errorf("Idx %d: expected %s, got %s", i, expected[i], k)
			}
			i++
		}
		if i != len(expected) {
			t.Errorf("Expected %d items, got %d", len(expected), i)
		}
		it.Close()
	})

	// 4. Range Reverse [B, F) -> E, C
	t.Run("RangeReverse", func(t *testing.T) {
		it := tr.ReverseIterator([]byte("B"), []byte("F"))
		expected := []string{"E", "C"}
		i := 0
		for ; it.Valid(); it.Next() {
			k := string(it.Key())
			if k != expected[i] {
				t.Errorf("Idx %d: expected %s, got %s", i, expected[i], k)
			}
			i++
		}
		if i != len(expected) {
			t.Errorf("Expected %d items, got %d", len(expected), i)
		}
		it.Close()
	})

	t.Run("OpenEndedLowerBoundSeekRemainsEmpty", func(t *testing.T) {
		start := []byte("Z")
		targets := []struct {
			name string
			key  []byte
		}{
			{name: "nil", key: nil},
			{name: "below", key: []byte("A")},
			{name: "inside", key: []byte("Z")},
			{name: "above", key: []byte{0xff}},
		}
		for _, reverse := range []bool{false, true} {
			name := map[bool]string{false: "forward", true: "reverse"}[reverse]
			t.Run(name, func(t *testing.T) {
				var it iterator.UnsafeIterator
				if reverse {
					it = tr.ReverseIterator(start, nil)
				} else {
					it = tr.Iterator(start, nil)
				}
				defer it.Close()
				if it.Valid() {
					t.Fatalf("iterator construction exposed key %q below start %q", it.Key(), start)
				}
				for _, target := range targets {
					t.Run(target.name, func(t *testing.T) {
						it.Seek(target.key)
						if it.Valid() {
							t.Fatalf("Seek(%x) exposed key %q below start %q", target.key, it.Key(), start)
						}
						if err := it.Error(); err != nil {
							t.Fatalf("Seek(%x) error=%v", target.key, err)
						}
					})
				}
			})
		}
	})

	t.Run("OpenEndedLowerBoundSeekClampsToDomain", func(t *testing.T) {
		start := []byte("D")
		tests := []struct {
			name    string
			target  []byte
			forward string
			reverse string
		}{
			{name: "nil", target: nil, forward: "E", reverse: "G"},
			{name: "below", target: []byte("A"), forward: "E"},
			{name: "inside", target: []byte("F"), forward: "G", reverse: "E"},
			{name: "above", target: []byte{0xff}, reverse: "G"},
		}
		for _, reverse := range []bool{false, true} {
			name := map[bool]string{false: "forward", true: "reverse"}[reverse]
			t.Run(name, func(t *testing.T) {
				var it iterator.UnsafeIterator
				if reverse {
					it = tr.ReverseIterator(start, nil)
				} else {
					it = tr.Iterator(start, nil)
				}
				defer it.Close()
				for _, test := range tests {
					t.Run(test.name, func(t *testing.T) {
						it.Seek(test.target)
						want := test.forward
						if reverse {
							want = test.reverse
						}
						if want == "" {
							if it.Valid() {
								t.Fatalf("Seek(%x) key=%q want invalid", test.target, it.Key())
							}
						} else if !it.Valid() || string(it.Key()) != want {
							t.Fatalf("Seek(%x) valid=%v key=%q want %q", test.target, it.Valid(), it.Key(), want)
						}
						if err := it.Error(); err != nil {
							t.Fatalf("Seek(%x) error=%v", test.target, err)
						}
					})
				}
			})
		}
	})

	t.Run("ViewSemantics", func(t *testing.T) {
		it := tr.Iterator(nil, nil)
		if !it.Valid() {
			t.Fatalf("expected iterator to be valid")
		}

		k := it.Key()
		uk := it.UnsafeKey()
		if len(k) == 0 || len(uk) == 0 {
			t.Fatalf("expected non-empty key views")
		}
		if unsafe.Pointer(&k[0]) != unsafe.Pointer(&uk[0]) {
			t.Fatalf("expected Key() to be a view (same backing) as UnsafeKey()")
		}

		v := it.Value()
		uv := it.UnsafeValue()
		if len(v) == 0 || len(uv) == 0 {
			t.Fatalf("expected non-empty value views")
		}
		if unsafe.Pointer(&v[0]) != unsafe.Pointer(&uv[0]) {
			t.Fatalf("expected Value() to be a view (same backing) as UnsafeValue()")
		}

		kc := it.KeyCopy(nil)
		vc := it.ValueCopy(nil)
		if len(kc) == 0 || len(vc) == 0 {
			t.Fatalf("expected KeyCopy/ValueCopy to return bytes")
		}
		if unsafe.Pointer(&kc[0]) == unsafe.Pointer(&k[0]) {
			t.Fatalf("expected KeyCopy to allocate a distinct buffer")
		}
		if unsafe.Pointer(&vc[0]) == unsafe.Pointer(&v[0]) {
			t.Fatalf("expected ValueCopy to allocate a distinct buffer")
		}
		it.Close()
	})
}

func TestIterator_SkipsTombstones(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.Alloc(1) // 0

	d0, _ := p.Get(0)
	n0 := node.NewNode(d0)
	n0.SetPageID(0)
	n0.SetType(page.PageTypeLeaf)
	n0.AddLeafEntry([]byte("A"), []byte("valA"), node.FlagInline, page.ValuePtr{})
	n0.AddLeafEntry([]byte("B"), nil, node.FlagTombstone, page.ValuePtr{})
	n0.AddLeafEntry([]byte("C"), []byte("valC"), node.FlagInline, page.ValuePtr{})
	n0.UpdateChecksum()

	tr := New(p, panicValueReader{}, 0)

	it := tr.Iterator(nil, nil)
	got := make([]string, 0, 2)
	for ; it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}
	_ = it.Close()

	if len(got) != 2 || got[0] != "A" || got[1] != "C" {
		t.Fatalf("expected [A C], got %v", got)
	}
}

func TestIterator_IncludeTombstones(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.Alloc(1) // 0

	d0, _ := p.Get(0)
	n0 := node.NewNode(d0)
	n0.SetPageID(0)
	n0.SetType(page.PageTypeLeaf)
	n0.AddLeafEntry([]byte("A"), []byte("valA"), node.FlagInline, page.ValuePtr{})
	n0.AddLeafEntry([]byte("B"), nil, node.FlagTombstone, page.ValuePtr{})
	n0.AddLeafEntry([]byte("C"), []byte("valC"), node.FlagInline, page.ValuePtr{})
	n0.UpdateChecksum()

	tr := New(p, panicValueReader{}, 0)
	it := tr.IteratorWithOptions(nil, nil, IteratorOptions{
		Mode:              IteratorModePointerProjection,
		IncludeTombstones: true,
	})
	defer it.Close()

	type row struct {
		key   string
		flags byte
	}
	rows := make([]row, 0, 3)
	for ; it.Valid(); it.Next() {
		_, _, flags := it.UnsafeEntry()
		rows = append(rows, row{key: string(it.Key()), flags: flags})
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if rows[0].key != "A" || rows[1].key != "B" || rows[2].key != "C" {
		t.Fatalf("expected [A B C], got [%s %s %s]", rows[0].key, rows[1].key, rows[2].key)
	}
	if rows[1].flags&node.FlagTombstone == 0 {
		t.Fatalf("expected B to be tombstoned, flags=%08b", rows[1].flags)
	}
}

func TestReverseIterator_IncludeTombstones(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.Alloc(1) // 0

	d0, _ := p.Get(0)
	n0 := node.NewNode(d0)
	n0.SetPageID(0)
	n0.SetType(page.PageTypeLeaf)
	n0.AddLeafEntry([]byte("A"), []byte("valA"), node.FlagInline, page.ValuePtr{})
	n0.AddLeafEntry([]byte("B"), nil, node.FlagTombstone, page.ValuePtr{})
	n0.AddLeafEntry([]byte("C"), []byte("valC"), node.FlagInline, page.ValuePtr{})
	n0.UpdateChecksum()

	tr := New(p, panicValueReader{}, 0)
	it := tr.ReverseIteratorWithOptions(nil, nil, IteratorOptions{
		Mode:              IteratorModePointerProjection,
		IncludeTombstones: true,
	})
	defer it.Close()

	type row struct {
		key   string
		flags byte
	}
	rows := make([]row, 0, 3)
	for ; it.Valid(); it.Next() {
		_, _, flags := it.UnsafeEntry()
		rows = append(rows, row{key: string(it.Key()), flags: flags})
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if rows[0].key != "C" || rows[1].key != "B" || rows[2].key != "A" {
		t.Fatalf("expected [C B A], got [%s %s %s]", rows[0].key, rows[1].key, rows[2].key)
	}
	if rows[1].flags&node.FlagTombstone == 0 {
		t.Fatalf("expected B to be tombstoned, flags=%08b", rows[1].flags)
	}
}

func TestIterator_PointerKeysOnly_DoesNotReadValues(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.Alloc(1) // 0

	d0, _ := p.Get(0)
	n0 := node.NewNode(d0)
	n0.SetPageID(0)
	n0.SetType(page.PageTypeLeaf)
	n0.AddLeafEntry([]byte("A"), nil, node.FlagPointer, page.ValuePtr{Offset: 1, Length: 3, FileID: 1})
	n0.AddLeafEntry([]byte("B"), nil, node.FlagTombstone, page.ValuePtr{})
	n0.AddLeafEntry([]byte("C"), nil, node.FlagPointer, page.ValuePtr{Offset: 5, Length: 3, FileID: 1})
	n0.UpdateChecksum()

	tr := New(p, panicValueReader{}, 0)
	it := tr.Iterator(nil, nil)
	defer it.Close()

	var keys []string
	for ; it.Valid(); it.Next() {
		keys = append(keys, string(it.Key()))
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if len(keys) != 2 || keys[0] != "A" || keys[1] != "C" {
		t.Fatalf("expected [A C], got %v", keys)
	}
}

func TestIterator_KeysOnly_DoesNotResolveValuePointers(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.Alloc(1)
	reader := newCountingValueReader()
	ptrA := reader.Add([]byte("pointer-A"))
	ptrC := reader.Add([]byte("pointer-C"))

	d0, _ := p.Get(0)
	n0 := node.NewNode(d0)
	n0.SetPageID(0)
	n0.SetType(page.PageTypeLeaf)
	n0.AddLeafEntry([]byte("A"), nil, node.FlagPointer, ptrA)
	n0.AddLeafEntry([]byte("B"), []byte("inline-B"), node.FlagInline, page.ValuePtr{})
	n0.AddLeafEntry([]byte("C"), nil, node.FlagPointer, ptrC)
	n0.UpdateChecksum()

	tr := New(p, reader, 0)
	it := tr.IteratorWithOptions(nil, nil, IteratorOptions{Mode: IteratorModeKeysOnly})
	defer it.Close()

	var keys []string
	for ; it.Valid(); it.Next() {
		keys = append(keys, string(it.Key()))
		if got := it.Value(); got != nil {
			t.Fatalf("keys-only iterator returned value for key %q", string(it.Key()))
		}
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if len(keys) != 3 || keys[0] != "A" || keys[1] != "B" || keys[2] != "C" {
		t.Fatalf("expected [A B C], got %v", keys)
	}
	if reader.reads != 0 {
		t.Fatalf("keys-only iterator performed %d pointer reads, want 0", reader.reads)
	}
}

func TestIterator_ProjectionMode_PartialDecodeParity(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.Alloc(1)
	reader := newCountingValueReader()
	ptrA := reader.Add([]byte("pointer-A"))
	ptrC := reader.Add([]byte("pointer-C"))

	d0, _ := p.Get(0)
	n0 := node.NewNode(d0)
	n0.SetPageID(0)
	n0.SetType(page.PageTypeLeaf)
	n0.AddLeafEntry([]byte("A"), nil, node.FlagPointer, ptrA)
	n0.AddLeafEntry([]byte("B"), []byte("inline-B"), node.FlagInline, page.ValuePtr{})
	n0.AddLeafEntry([]byte("C"), nil, node.FlagPointer, ptrC)
	n0.UpdateChecksum()

	type row struct {
		key   []byte
		value []byte
		ptr   page.ValuePtr
		flags byte
	}

	tr := New(p, reader, 0)
	proj := tr.IteratorWithOptions(nil, nil, IteratorOptions{Mode: IteratorModePointerProjection})
	projRows := make([]row, 0, 3)
	for ; proj.Valid(); proj.Next() {
		val, ptr, flags := proj.UnsafeEntry()
		projRows = append(projRows, row{
			key:   append([]byte(nil), proj.Key()...),
			value: append([]byte(nil), val...),
			ptr:   ptr,
			flags: flags,
		})
	}
	if err := proj.Error(); err != nil {
		_ = proj.Close()
		t.Fatalf("projection iterator error: %v", err)
	}
	_ = proj.Close()
	if reader.reads != 0 {
		t.Fatalf("projection iterator performed %d pointer reads, want 0", reader.reads)
	}

	full := tr.Iterator(nil, nil)
	fullRows := make([]row, 0, 3)
	for ; full.Valid(); full.Next() {
		_, ptr, flags := full.UnsafeEntry()
		val := full.Value()
		fullRows = append(fullRows, row{
			key:   append([]byte(nil), full.Key()...),
			value: append([]byte(nil), val...),
			ptr:   ptr,
			flags: flags,
		})
	}
	if err := full.Error(); err != nil {
		_ = full.Close()
		t.Fatalf("full iterator error: %v", err)
	}
	_ = full.Close()

	if len(projRows) != len(fullRows) {
		t.Fatalf("row count mismatch: projection=%d full=%d", len(projRows), len(fullRows))
	}
	for i := range fullRows {
		if !bytes.Equal(projRows[i].key, fullRows[i].key) {
			t.Fatalf("key mismatch at %d: projection=%q full=%q", i, projRows[i].key, fullRows[i].key)
		}
		if projRows[i].flags != fullRows[i].flags {
			t.Fatalf("flags mismatch at %d: projection=%d full=%d", i, projRows[i].flags, fullRows[i].flags)
		}
		if fullRows[i].flags&node.FlagPointer != 0 {
			if projRows[i].value != nil {
				t.Fatalf("projection expected nil value for pointer key %q", projRows[i].key)
			}
			if projRows[i].ptr != fullRows[i].ptr {
				t.Fatalf("pointer metadata mismatch for key %q", projRows[i].key)
			}
			continue
		}
		if !bytes.Equal(projRows[i].value, fullRows[i].value) {
			t.Fatalf("inline projection mismatch for key %q", projRows[i].key)
		}
	}
	if reader.reads == 0 {
		t.Fatalf("full iterator expected to perform pointer reads")
	}
}

func TestIterator_CombinedColumnarPrefixV2(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.Alloc(1) // root leaf page 0

	data0, _ := p.Get(0)
	b := node.NewBuilderWithOptions(data0, page.PageTypeLeaf, node.BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
		PackedValuePtr:        true,
	})
	b.SetPageID(0)

	type entry struct {
		key   string
		value []byte
		flags byte
		ptr   page.ValuePtr
	}
	entries := []entry{
		{key: "aa00", flags: node.FlagPointer, ptr: page.ValuePtr{Offset: 1, Length: 10, FileID: 1}},
		{key: "aa01", flags: node.FlagInline, value: []byte("v1")},
		{key: "aa02", flags: node.FlagTombstone},
		{key: "aa03", flags: node.FlagPointer, ptr: page.ValuePtr{Offset: 2, Length: 11, FileID: 1}},
		{key: "aa04", flags: node.FlagInline, value: []byte("v4")},
	}
	for _, e := range entries {
		if err := b.AddLeafEntry([]byte(e.key), e.value, e.flags, e.ptr); err != nil {
			t.Fatalf("AddLeafEntry(%s): %v", e.key, err)
		}
	}
	n := b.Finish()
	if !n.VerifyChecksum() {
		t.Fatalf("checksum mismatch")
	}

	tr := New(p, panicValueReader{}, 0)

	t.Run("Forward", func(t *testing.T) {
		it := tr.Iterator(nil, nil)
		defer it.Close()
		var keys []string
		for ; it.Valid(); it.Next() {
			keys = append(keys, string(it.Key()))
		}
		if err := it.Error(); err != nil {
			t.Fatalf("iterator error: %v", err)
		}
		want := []string{"aa00", "aa01", "aa03", "aa04"}
		if len(keys) != len(want) {
			t.Fatalf("expected %v, got %v", want, keys)
		}
		for i := range want {
			if keys[i] != want[i] {
				t.Fatalf("expected %v, got %v", want, keys)
			}
		}
	})

	t.Run("Reverse", func(t *testing.T) {
		it := tr.ReverseIterator(nil, nil)
		defer it.Close()
		var keys []string
		for ; it.Valid(); it.Next() {
			keys = append(keys, string(it.Key()))
		}
		if err := it.Error(); err != nil {
			t.Fatalf("iterator error: %v", err)
		}
		want := []string{"aa04", "aa03", "aa01", "aa00"}
		if len(keys) != len(want) {
			t.Fatalf("expected %v, got %v", want, keys)
		}
		for i := range want {
			if keys[i] != want[i] {
				t.Fatalf("expected %v, got %v", want, keys)
			}
		}
	})

	t.Run("Range", func(t *testing.T) {
		it := tr.Iterator([]byte("aa01"), []byte("aa04"))
		defer it.Close()
		var keys []string
		for ; it.Valid(); it.Next() {
			keys = append(keys, string(it.Key()))
		}
		if err := it.Error(); err != nil {
			t.Fatalf("iterator error: %v", err)
		}
		want := []string{"aa01", "aa03"}
		if len(keys) != len(want) {
			t.Fatalf("expected %v, got %v", want, keys)
		}
		for i := range want {
			if keys[i] != want[i] {
				t.Fatalf("expected %v, got %v", want, keys)
			}
		}
	})
}

func TestIterator_SeekPrunesByFence(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.Alloc(1) // root internal page 0

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
	it := tr.Iterator([]byte("20"), nil)
	defer it.Close()

	if it.Valid() {
		t.Fatalf("expected iterator to be invalid when seek key is out of fenced range")
	}
	if err := it.Error(); err != nil {
		t.Fatalf("expected seek prune without descent error, got %v", err)
	}
}

func TestIterator_CombinedColumnarPrefixV2_MultiRestartBlocks(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.Alloc(1) // root leaf page 0

	data0, _ := p.Get(0)
	b := node.NewBuilderWithOptions(data0, page.PageTypeLeaf, node.BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
		PackedValuePtr:        true,
	})
	b.SetPageID(0)

	var allLive []string
	for i := 0; i < 48; i++ {
		key := fmt.Sprintf("aa%04d", i)
		flags := byte(node.FlagInline)
		value := []byte{byte(i)}
		ptr := page.ValuePtr{}
		switch {
		case i%9 == 0:
			flags = node.FlagTombstone
			value = nil
		case i%2 == 0:
			flags = node.FlagPointer
			value = nil
			ptr = page.ValuePtr{Offset: uint64(100 + i), Length: uint32(10 + i), FileID: 1}
		}
		if flags&node.FlagTombstone == 0 {
			allLive = append(allLive, key)
		}
		if err := b.AddLeafEntry([]byte(key), value, flags, ptr); err != nil {
			t.Fatalf("AddLeafEntry(%s): %v", key, err)
		}
	}
	n := b.Finish()
	if !n.VerifyChecksum() {
		t.Fatalf("checksum mismatch")
	}

	tr := New(p, panicValueReader{}, 0)

	t.Run("Forward", func(t *testing.T) {
		it := tr.Iterator(nil, nil)
		defer it.Close()

		var got []string
		for ; it.Valid(); it.Next() {
			got = append(got, string(it.Key()))
		}
		if err := it.Error(); err != nil {
			t.Fatalf("iterator error: %v", err)
		}
		if len(got) != len(allLive) {
			t.Fatalf("expected %d keys, got %d", len(allLive), len(got))
		}
		for i := range allLive {
			if got[i] != allLive[i] {
				t.Fatalf("forward mismatch at %d: expected %q, got %q", i, allLive[i], got[i])
			}
		}
	})

	t.Run("Reverse", func(t *testing.T) {
		it := tr.ReverseIterator(nil, nil)
		defer it.Close()

		var got []string
		for ; it.Valid(); it.Next() {
			got = append(got, string(it.Key()))
		}
		if err := it.Error(); err != nil {
			t.Fatalf("iterator error: %v", err)
		}
		if len(got) != len(allLive) {
			t.Fatalf("expected %d keys, got %d", len(allLive), len(got))
		}
		for i := range allLive {
			want := allLive[len(allLive)-1-i]
			if got[i] != want {
				t.Fatalf("reverse mismatch at %d: expected %q, got %q", i, want, got[i])
			}
		}
	})

	t.Run("Range", func(t *testing.T) {
		start := []byte("aa0010")
		end := []byte("aa0030")
		it := tr.Iterator(start, end)
		defer it.Close()

		var got []string
		for ; it.Valid(); it.Next() {
			got = append(got, string(it.Key()))
		}
		if err := it.Error(); err != nil {
			t.Fatalf("iterator error: %v", err)
		}

		var want []string
		for _, k := range allLive {
			if k >= string(start) && k < string(end) {
				want = append(want, k)
			}
		}
		if len(got) != len(want) {
			t.Fatalf("expected %d keys, got %d", len(want), len(got))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("range mismatch at %d: expected %q, got %q", i, want[i], got[i])
			}
		}
	})
}

type countingBatchValueReader struct {
	values      map[page.ValuePtr][]byte
	singleCalls int
	batchCalls  int
	batchSizes  []int
}

func newCountingBatchValueReader() *countingBatchValueReader {
	return &countingBatchValueReader{
		values: make(map[page.ValuePtr][]byte),
	}
}

func (r *countingBatchValueReader) add(fileID uint32, offset uint64, value string) page.ValuePtr {
	ptr := page.ValuePtr{
		FileID: fileID,
		Offset: offset,
		Length: uint32(len(value)),
	}
	r.values[ptr] = []byte(value)
	return ptr
}

func (r *countingBatchValueReader) addPtr(ptr page.ValuePtr, value string) page.ValuePtr {
	r.values[ptr] = []byte(value)
	return ptr
}

func (r *countingBatchValueReader) Read(ptr page.ValuePtr) ([]byte, error) {
	v, ok := r.values[ptr]
	if !ok {
		return nil, fmt.Errorf("value pointer not found: %+v", ptr)
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

func (r *countingBatchValueReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	v, ok := r.values[ptr]
	if !ok {
		return nil, fmt.Errorf("value pointer not found: %+v", ptr)
	}
	return v, nil
}

func (r *countingBatchValueReader) ReadUnsafeAppend(ptr page.ValuePtr, dst []byte) ([]byte, error) {
	r.singleCalls++
	v, err := r.ReadUnsafe(ptr)
	if err != nil {
		return nil, err
	}
	dst = append(dst[:0], v...)
	return dst, nil
}

func (r *countingBatchValueReader) ReadUnsafeAppendBatch(ptrs []page.ValuePtr, dst [][]byte) ([][]byte, error) {
	r.batchCalls++
	r.batchSizes = append(r.batchSizes, len(ptrs))
	if cap(dst) < len(ptrs) {
		dst = make([][]byte, len(ptrs))
	} else {
		dst = dst[:len(ptrs)]
	}
	for i, ptr := range ptrs {
		v, err := r.ReadUnsafe(ptr)
		if err != nil {
			return nil, err
		}
		dst[i] = append(dst[i][:0], v...)
	}
	return dst, nil
}

func TestIterator_GroupedPointerBatching_StableOrdering(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.Alloc(1)
	data, _ := p.Get(0)
	n := node.NewNode(data)
	n.SetPageID(0)
	n.SetType(page.PageTypeLeaf)

	reader := newCountingBatchValueReader()
	var wantKeys []string
	var wantVals []string
	for i := 0; i < 12; i++ {
		key := fmt.Sprintf("k%02d", i)
		val := fmt.Sprintf("v%02d", i)
		ptr := reader.add(page.ValueLogFileID(1), uint64(100+i*8), val)
		if err := n.AddLeafEntry([]byte(key), nil, node.FlagPointer, ptr); err != nil {
			t.Fatalf("AddLeafEntry(%s): %v", key, err)
		}
		wantKeys = append(wantKeys, key)
		wantVals = append(wantVals, val)
	}
	n.UpdateChecksum()

	tr := New(p, reader, 0)
	it := tr.Iterator(nil, nil)
	defer it.Close()

	var gotKeys []string
	var gotVals []string
	for ; it.Valid(); it.Next() {
		gotKeys = append(gotKeys, string(it.Key()))
		gotVals = append(gotVals, string(it.ValueCopy(nil)))
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if len(gotKeys) != len(wantKeys) {
		t.Fatalf("keys length mismatch: got=%d want=%d", len(gotKeys), len(wantKeys))
	}
	for i := range wantKeys {
		if gotKeys[i] != wantKeys[i] || gotVals[i] != wantVals[i] {
			t.Fatalf("entry %d mismatch: got=(%q,%q) want=(%q,%q)", i, gotKeys[i], gotVals[i], wantKeys[i], wantVals[i])
		}
	}
	if reader.batchCalls == 0 {
		t.Fatalf("expected batched pointer reads, got batchCalls=0")
	}
	if reader.singleCalls != 0 {
		t.Fatalf("expected no single pointer reads for contiguous pointer run, got singleCalls=%d", reader.singleCalls)
	}
}

func TestIterator_GroupedRecordPointerPrefetch_ExtendsSameRecordRun(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.Alloc(1)
	data, _ := p.Get(0)
	n := node.NewNode(data)
	n.SetPageID(0)
	n.SetType(page.PageTypeLeaf)

	reader := newCountingBatchValueReader()
	const groupedRecordLen = 4096
	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("k%02d", i)
		val := fmt.Sprintf("grouped-v%02d", i)
		ptr := page.ValuePtr{
			FileID: page.ValueLogFileID(1),
			Offset: 2048,
			Length: page.ValuePtrMarkGrouped(groupedRecordLen, uint8(i)),
		}
		reader.addPtr(ptr, val)
		if err := n.AddLeafEntry([]byte(key), nil, node.FlagPointer, ptr); err != nil {
			t.Fatalf("AddLeafEntry(%s): %v", key, err)
		}
	}
	n.UpdateChecksum()

	tr := New(p, reader, 0)
	it := tr.Iterator(nil, nil)
	defer it.Close()

	count := 0
	for ; it.Valid(); it.Next() {
		want := fmt.Sprintf("grouped-v%02d", count)
		if got := string(it.ValueCopy(nil)); got != want {
			t.Fatalf("value[%d]=%q want %q", count, got, want)
		}
		count++
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if count != 8 {
		t.Fatalf("iterated %d values want 8", count)
	}
	if reader.batchCalls != 1 {
		t.Fatalf("batchCalls=%d want 1 for one operation-local grouped-record prefetch", reader.batchCalls)
	}
	if len(reader.batchSizes) != 1 || reader.batchSizes[0] != 8 {
		t.Fatalf("batchSizes=%v want [8]", reader.batchSizes)
	}
	if reader.singleCalls != 0 {
		t.Fatalf("singleCalls=%d want 0", reader.singleCalls)
	}
}

func TestIterator_GroupedRecordPrefetchStopsAtIteratorEndBound(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.Alloc(1)
	data, _ := p.Get(0)
	n := node.NewNode(data)
	n.SetPageID(0)
	n.SetType(page.PageTypeLeaf)

	reader := newCountingBatchValueReader()
	const groupedRecordLen = 4096
	inRangePtr := page.ValuePtr{
		FileID: page.ValueLogFileID(1),
		Offset: 2048,
		Length: page.ValuePtrMarkGrouped(groupedRecordLen, 0),
	}
	outOfRangeMissingPtr := page.ValuePtr{
		FileID: page.ValueLogFileID(2),
		Offset: 4096,
		Length: page.ValuePtrMarkGrouped(groupedRecordLen, 0),
	}
	reader.addPtr(inRangePtr, "in-range")
	if err := n.AddLeafEntry([]byte("k00"), nil, node.FlagPointer, inRangePtr); err != nil {
		t.Fatalf("AddLeafEntry(k00): %v", err)
	}
	if err := n.AddLeafEntry([]byte("k01"), nil, node.FlagPointer, outOfRangeMissingPtr); err != nil {
		t.Fatalf("AddLeafEntry(k01): %v", err)
	}
	n.UpdateChecksum()

	tr := New(p, reader, 0)
	it := tr.Iterator(nil, []byte("k01"))
	defer it.Close()
	if !it.Valid() {
		t.Fatalf("iterator invalid before value read: %v", it.Error())
	}
	if got := string(it.ValueCopy(nil)); got != "in-range" {
		t.Fatalf("value=%q want in-range", got)
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error after bounded value read: %v", err)
	}
	it.Next()
	if it.Valid() {
		t.Fatalf("iterator should stop before end-exclusive k01")
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error after end bound: %v", err)
	}
	if reader.batchCalls != 0 {
		t.Fatalf("batchCalls=%d want 0; out-of-range missing pointer must not be prefetched", reader.batchCalls)
	}
	if reader.singleCalls != 1 {
		t.Fatalf("singleCalls=%d want 1", reader.singleCalls)
	}
}

func TestIterator_KeysOnlyDoesNotReadPointerValues(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.Alloc(1)
	data, _ := p.Get(0)
	n := node.NewNode(data)
	n.SetPageID(0)
	n.SetType(page.PageTypeLeaf)

	reader := newCountingBatchValueReader()
	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("k%02d", i)
		ptr := reader.add(page.ValueLogFileID(1), uint64(100+i*8), fmt.Sprintf("v%02d", i))
		if err := n.AddLeafEntry([]byte(key), nil, node.FlagPointer, ptr); err != nil {
			t.Fatalf("AddLeafEntry(%s): %v", key, err)
		}
	}
	n.UpdateChecksum()

	tr := New(p, reader, 0)
	it := tr.IteratorWithOptions(nil, nil, IteratorOptions{Mode: IteratorModeKeysOnly})
	defer it.Close()

	var keys []string
	for ; it.Valid(); it.Next() {
		keys = append(keys, string(it.KeyCopy(nil)))
		if val := it.Value(); val != nil {
			t.Fatalf("keys-only Value()=%q want nil", val)
		}
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if got, want := strings.Join(keys, ","), "k00,k01,k02,k03"; got != want {
		t.Fatalf("keys=%q want %q", got, want)
	}
	if reader.singleCalls != 0 || reader.batchCalls != 0 {
		t.Fatalf("keys-only iteration read pointer values: single=%d batch=%d", reader.singleCalls, reader.batchCalls)
	}
}

func TestIterator_GroupedRecordPrefetchChecksumMismatchFailsClosed(t *testing.T) {
	dir := t.TempDir()
	fileID, path, ptrs, _ := writeIteratorGroupedFrame(t, dir, 4)
	if fileID == 0 {
		t.Fatalf("unexpected zero fileID")
	}
	corruptGroupedFramePayloadByte(t, path, ptrs[0])

	mgr, err := valuelog.NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.Alloc(1)
	data, _ := p.Get(0)
	n := node.NewNode(data)
	n.SetPageID(0)
	n.SetType(page.PageTypeLeaf)
	for i, ptr := range ptrs {
		key := fmt.Sprintf("k%02d", i)
		if err := n.AddLeafEntry([]byte(key), nil, node.FlagPointer, ptr); err != nil {
			t.Fatalf("AddLeafEntry(%s): %v", key, err)
		}
	}
	n.UpdateChecksum()

	tr := New(p, mgr, 0)
	it := tr.Iterator(nil, nil)
	defer it.Close()
	if !it.Valid() {
		t.Fatalf("iterator invalid before value read: %v", it.Error())
	}
	if got := it.ValueCopy(nil); got != nil {
		t.Fatalf("corrupt grouped record returned value %q", got)
	}
	if !errors.Is(it.Error(), valuelog.ErrCorrupt) {
		t.Fatalf("iterator error=%v want ErrCorrupt", it.Error())
	}
	if got := mgr.ReadStats().RecordCRCChecks; got != 1 {
		t.Fatalf("CRC checks after failed grouped prefetch=%d want 1", got)
	}
}

func writeIteratorGroupedFrame(t *testing.T, dir string, records int) (uint32, string, []page.ValuePtr, [][]byte) {
	t.Helper()
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	recs := make([]valuelog.Record, records)
	want := make([][]byte, records)
	for i := range recs {
		value := []byte(fmt.Sprintf("grouped-value-%02d", i))
		recs[i] = valuelog.Record{RID: uint64(i + 1), Value: value}
		want[i] = append([]byte(nil), value...)
	}
	ptrs, err := writer.AppendFrame(0, nil, recs)
	if err != nil {
		t.Fatalf("AppendFrame: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	return fileID, path, ptrs, want
}

func corruptGroupedFramePayloadByte(t *testing.T, path string, ptr page.ValuePtr) {
	t.Helper()
	fh, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer func() { _ = fh.Close() }()
	// ValuePtr.Offset points just after the record CRC prefix. Step back to the
	// record start, then skip the value-log record header and grouped-frame
	// header so the flipped byte lands inside the checksummed grouped payload.
	corruptOff := int64(ptr.Offset-4) + valuelog.HeaderSize + valuelog.FrameHeaderSize
	var b [1]byte
	if _, err := fh.ReadAt(b[:], corruptOff); err != nil {
		t.Fatalf("ReadAt corrupt byte: %v", err)
	}
	b[0] ^= 0xff
	if _, err := fh.WriteAt(b[:], corruptOff); err != nil {
		t.Fatalf("WriteAt corrupt byte: %v", err)
	}
}

func TestIterator_GroupedPointerBatching_MixedInlineAndPtr(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.Alloc(1)
	data, _ := p.Get(0)
	n := node.NewNode(data)
	n.SetPageID(0)
	n.SetType(page.PageTypeLeaf)

	reader := newCountingBatchValueReader()
	ptr := func(off uint64, val string) page.ValuePtr {
		return reader.add(page.ValueLogFileID(1), off, val)
	}

	type kv struct {
		key   string
		flags byte
		val   string
		ptr   page.ValuePtr
	}
	entries := []kv{
		{key: "k00", flags: node.FlagPointer, val: "pv00", ptr: ptr(10, "pv00")},
		{key: "k01", flags: node.FlagPointer, val: "pv01", ptr: ptr(18, "pv01")},
		{key: "k02", flags: node.FlagInline, val: "iv02"},
		{key: "k03", flags: node.FlagPointer, val: "pv03", ptr: ptr(26, "pv03")},
		{key: "k04", flags: node.FlagInline, val: "iv04"},
		{key: "k05", flags: node.FlagPointer, val: "pv05", ptr: ptr(34, "pv05")},
		{key: "k06", flags: node.FlagPointer, val: "pv06", ptr: ptr(42, "pv06")},
	}
	for _, e := range entries {
		var value []byte
		if e.flags&node.FlagPointer == 0 {
			value = []byte(e.val)
		}
		if err := n.AddLeafEntry([]byte(e.key), value, e.flags, e.ptr); err != nil {
			t.Fatalf("AddLeafEntry(%s): %v", e.key, err)
		}
	}
	n.UpdateChecksum()

	tr := New(p, reader, 0)
	it := tr.Iterator(nil, nil)
	defer it.Close()

	var got []string
	for ; it.Valid(); it.Next() {
		got = append(got, fmt.Sprintf("%s=%s", it.Key(), it.ValueCopy(nil)))
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}

	want := []string{
		"k00=pv00",
		"k01=pv01",
		"k02=iv02",
		"k03=pv03",
		"k04=iv04",
		"k05=pv05",
		"k06=pv06",
	}
	if len(got) != len(want) {
		t.Fatalf("result length mismatch: got=%d want=%d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("result mismatch at %d: got=%q want=%q", i, got[i], want[i])
		}
	}
	if reader.batchCalls == 0 {
		t.Fatalf("expected at least one batched pointer read")
	}
	if reader.singleCalls == 0 {
		t.Fatalf("expected isolated pointer fallback to single read in mixed stream")
	}
}

type keyAwareBatchValueReader struct {
	values      map[page.ValuePtr]map[string][]byte
	singleCalls int
	batchCalls  int
}

func newKeyAwareBatchValueReader() *keyAwareBatchValueReader {
	return &keyAwareBatchValueReader{
		values: make(map[page.ValuePtr]map[string][]byte),
	}
}

func (r *keyAwareBatchValueReader) add(fileID uint32, offset uint64, key, value string) page.ValuePtr {
	ptr := page.ValuePtr{
		FileID: fileID,
		Offset: offset,
		Length: uint32(len(value)),
	}
	m := r.values[ptr]
	if m == nil {
		m = make(map[string][]byte)
		r.values[ptr] = m
	}
	m[key] = []byte(value)
	return ptr
}

func (r *keyAwareBatchValueReader) lookup(ptr page.ValuePtr, key []byte) ([]byte, error) {
	m, ok := r.values[ptr]
	if !ok {
		return nil, fmt.Errorf("value pointer not found: %+v", ptr)
	}
	v, ok := m[string(key)]
	if !ok {
		return nil, fmt.Errorf("key lookup miss for ptr=%+v key=%q", ptr, key)
	}
	return v, nil
}

func (r *keyAwareBatchValueReader) Read(ptr page.ValuePtr) ([]byte, error) {
	return nil, fmt.Errorf("unexpected keyless read for ptr=%+v", ptr)
}

func (r *keyAwareBatchValueReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	return nil, fmt.Errorf("unexpected keyless read for ptr=%+v", ptr)
}

func (r *keyAwareBatchValueReader) ReadUnsafeForKey(ptr page.ValuePtr, key []byte) ([]byte, error) {
	r.singleCalls++
	return r.lookup(ptr, key)
}

func (r *keyAwareBatchValueReader) ReadUnsafeAppendForKey(ptr page.ValuePtr, key []byte, dst []byte) ([]byte, error) {
	r.singleCalls++
	v, err := r.lookup(ptr, key)
	if err != nil {
		return nil, err
	}
	return append(dst[:0], v...), nil
}

func (r *keyAwareBatchValueReader) ReadUnsafeAppendBatchForKeys(ptrs []page.ValuePtr, keys [][]byte, dst [][]byte) ([][]byte, error) {
	r.batchCalls++
	if len(ptrs) != len(keys) {
		return nil, fmt.Errorf("ptr/key mismatch %d/%d", len(ptrs), len(keys))
	}
	if cap(dst) < len(ptrs) {
		dst = make([][]byte, len(ptrs))
	} else {
		dst = dst[:len(ptrs)]
	}
	for i := range ptrs {
		v, err := r.lookup(ptrs[i], keys[i])
		if err != nil {
			return nil, err
		}
		dst[i] = append(dst[i][:0], v...)
	}
	return dst, nil
}

func TestIterator_GroupedPointerBatching_KeyAwareCombinedColumnarPrefixV2(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.Alloc(1)
	data, _ := p.Get(0)
	b := node.NewBuilderWithOptions(data, page.PageTypeLeaf, node.BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
		PackedValuePtr:        true,
	})
	b.SetPageID(0)

	reader := newKeyAwareBatchValueReader()
	want := make([]string, 0, 8)
	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("aa%04d", i)
		val := fmt.Sprintf("pv%04d", i)
		ptr := reader.add(page.ValueLogFileID(1), uint64(64+i*32), key, val)
		if err := b.AddLeafEntry([]byte(key), nil, node.FlagPointer, ptr); err != nil {
			t.Fatalf("AddLeafEntry(%s): %v", key, err)
		}
		want = append(want, fmt.Sprintf("%s=%s", key, val))
	}
	n := b.Finish()
	if !n.VerifyChecksum() {
		t.Fatalf("checksum mismatch")
	}

	tr := New(p, reader, 0)
	it := tr.Iterator(nil, nil)
	defer it.Close()

	var got []string
	for ; it.Valid(); it.Next() {
		got = append(got, fmt.Sprintf("%s=%s", it.Key(), it.ValueCopy(nil)))
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("result length mismatch: got=%d want=%d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("result mismatch at %d: got=%q want=%q", i, got[i], want[i])
		}
	}
	if reader.batchCalls == 0 {
		t.Fatalf("expected key-aware batched pointer reads")
	}
	if reader.singleCalls != 0 {
		t.Fatalf("expected no single key-aware pointer reads, got %d", reader.singleCalls)
	}
}
