package tree

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

// The short buffer is deliberately capacity-limited: a decoder must refuse
// before writing or growing it, even when the encoded page itself is valid.
func TestCOWLimitsReconstructedKeyCapacity(t *testing.T) {
	keys := [][]byte{[]byte(strings.Repeat("common:", 32) + "000"), []byte(strings.Repeat("common:", 32) + "001")}
	for _, tc := range []struct {
		name string
		typ  page.PageType
		opts node.BuilderOptions
	}{
		{"prefix_leaf", page.PageTypeLeaf, node.BuilderOptions{LeafPrefixCompression: true}},
		{"columnar_prefix_leaf", page.PageTypeLeaf, node.BuilderOptions{LeafPrefixCompression: true, LeafColumnar: true}},
		{"base_delta_internal", page.PageTypeInternal, node.BuilderOptions{InternalBaseDelta: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := make([]byte, page.PageSize)
			b := node.NewBuilderWithOptions(data, tc.typ, tc.opts)
			defer b.ReleaseScratch()
			for i, key := range keys {
				var err error
				if tc.typ == page.PageTypeInternal {
					err = b.AddInternalChild(key, uint64(i+10))
				} else {
					err = b.AddLeafEntry(key, nil, node.FlagPointer, page.ValuePtr{FileID: 7, Offset: uint64(i + 20), Length: 9})
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			b.FinishNoNode()
			decode := func(n *node.Node) ([]byte, error) {
				if tc.typ == page.PageTypeInternal {
					key, _, err := n.GetInternalEntryView(1)
					return key, err
				}
				key, _, _, _, err := n.GetLeafEntryView(1)
				return key, err
			}
			for _, capacity := range []int{len(keys[1]), len(keys[1]) - 1} {
				guarded := bytes.Repeat([]byte{0xa5}, len(keys[1])+2)
				scratch := guarded[1 : 1+capacity : 1+capacity]
				run := func() {
					n := node.NewNodeView(data)
					if tc.typ == page.PageTypeInternal && !n.InternalBaseDeltaEnabled() {
						t.Fatal("fixture did not select compressed internal encoding")
					}
					n.SetFixedKeyScratch(scratch)
					key, err := decode(&n)
					if capacity == len(keys[1]) {
						if err != nil || !bytes.Equal(key, keys[1]) || &key[0] != &scratch[0] {
							t.Fatalf("exact-capacity reconstruction: key=%q err=%v", key, err)
						}
					} else if !errors.Is(err, node.ErrCorruptedNode) || key != nil {
						t.Fatalf("short scratch must refuse: key=%q err=%v", key, err)
					}
					if guarded[0] != 0xa5 || guarded[len(guarded)-1] != 0xa5 {
						t.Fatal("scratch guard changed")
					}
					if capacity < len(keys[1]) {
						for _, v := range guarded {
							if v != 0xa5 {
								t.Fatal("refused reconstruction wrote to scratch")
							}
						}
					}
				}
				if got := testing.AllocsPerRun(100, run); got != 0 {
					t.Fatalf("capacity %d decoder allocated %g times", capacity, got)
				}
			}
		})
	}
}

func cowLimitsPager(t *testing.T) *pager.Pager {
	t.Helper()
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func TestCOWLimitsMultiLeafProjectionReusesBuffers(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts node.BuilderOptions
	}{
		{"prefix", node.BuilderOptions{LeafPrefixCompression: true}},
		{"columnar_prefix", node.BuilderOptions{LeafPrefixCompression: true, LeafColumnar: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const leaves, perLeaf = 3, 18 // Cross both a page boundary and prefix restart.
			p := cowLimitsPager(t)
			root, err := p.Alloc(1)
			if err != nil {
				t.Fatal(err)
			}
			data, err := p.GetForWrite(root)
			if err != nil {
				t.Fatal(err)
			}
			rb := node.NewBuilder(data, page.PageTypeInternal)
			defer rb.ReleaseScratch()
			rb.SetPageID(root)
			var pages [leaves][page.PageSize]byte
			var keys [leaves * perLeaf][]byte
			var ptrs [leaves * perLeaf]page.ValuePtr
			for l := range pages {
				b := node.NewBuilderWithOptions(pages[l][:], page.PageTypeLeaf, tc.opts)
				for j := 0; j < perLeaf; j++ {
					i := l*perLeaf + j
					keys[i] = []byte(fmt.Sprintf("%s%03d", strings.Repeat("namespace/", 20), i))
					ptrs[i] = page.ValuePtr{FileID: 7, Offset: uint64(100 + i), Length: uint32(20 + i)}
					if err := b.AddLeafEntry(keys[i], nil, node.FlagPointer, ptrs[i]); err != nil {
						t.Fatal(err)
					}
				}
				b.FinishNoNode()
				b.ReleaseScratch()
				if err := rb.AddInternalChildRef(keys[l*perLeaf], page.LeafLogChildRef(page.LeafLogPtr{FileID: 8, Offset: uint64(l + 1)})); err != nil {
					t.Fatal(err)
				}
			}
			rb.FinishNoNode()
			reader := newCountingValueReader()
			tr := New(p, reader, root)
			var backing *byte
			loads := 0
			readLeaf := func(ref page.LeafLogPtr, dst []byte) ([]byte, error) {
				if ref.FileID != 8 || ref.Offset < 1 || ref.Offset > leaves || cap(dst) != page.PageSize {
					t.Fatal("unexpected external page or scratch capacity")
				}
				dst = dst[:page.PageSize]
				if backing == nil {
					backing = &dst[0]
				} else if backing != &dst[0] {
					t.Fatal("iterator replaced admitted leaf backing")
				}
				loads++
				copy(dst, pages[ref.Offset-1][:])
				return dst, nil
			}
			it := tr.OwnedPointerProjectionIterator(nil, nil, readLeaf).(*Iterator)
			defer it.Close()
			nodeBacking, combinedBacking := &it.nodeKeyScratch[0], &it.leafState.keyScratch[:page.PageSize][0]
			stackBacking := &it.stack[:cap(it.stack)][0]
			var copied, pointKey, pointLeaf [page.PageSize]byte
			pointReader := func(ref page.LeafLogPtr, dst []byte) ([]byte, error) {
				copy(dst[:page.PageSize], pages[ref.Offset-1][:])
				return dst[:page.PageSize], nil
			}
			check := func(i int) {
				if !it.Valid() || !bytes.Equal(it.Key(), keys[i]) {
					t.Fatalf("row %d key=%q error=%v", i, it.Key(), it.Error())
				}
				value, ptr, flags := it.UnsafeEntry()
				if value != nil || ptr != ptrs[i] || flags&node.FlagPointer == 0 {
					t.Fatalf("row %d projection=%v/%v/%d", i, value, ptr, flags)
				}
			}
			run := func() {
				it.Seek(keys[len(keys)-1])
				check(len(keys) - 1)
				it.Seek(nil)
				for i := range keys {
					check(i)
					saved := it.KeyCopy(copied[:0])
					it.Next()
					if !bytes.Equal(saved, keys[i]) {
						t.Fatal("caller-owned copied key changed after page/key scratch reuse")
					}
				}
				if it.Valid() || it.Error() != nil {
					t.Fatalf("end: %v", it.Error())
				}
				for _, i := range []int{1, perLeaf - 1, perLeaf + 1, len(keys) - 1} {
					entry, err := tr.GetEntryWithFixedScratch(keys[i], pointKey[:], pointLeaf[:], pointReader)
					if err != nil || !bytes.Equal(entry.Key, keys[i]) || entry.ValuePtr != ptrs[i] {
						t.Fatalf("point row %d: %v", i, err)
					}
				}
				if cap(it.stack) != maxTraversalDepth || &it.stack[:cap(it.stack)][0] != stackBacking || cap(it.nodeKeyScratch) != page.PageSize || &it.nodeKeyScratch[0] != nodeBacking || cap(it.leafState.keyScratch) != page.PageSize || &it.leafState.keyScratch[:page.PageSize][0] != combinedBacking {
					t.Fatal("owned fixed stack/key backing changed")
				}
			}
			if got := testing.AllocsPerRun(20, run); got != 0 {
				t.Fatalf("multi-leaf fixed operations allocated %g times", got)
			}
			if loads < leaves || reader.reads != 0 {
				t.Fatalf("loads=%d value reads=%d", loads, reader.reads)
			}
			// Exercise the iterator's own reconstruction refusal, including its
			// separate combined-column scratch. Use a distinct callback because
			// this second iterator owns a distinct admitted leaf buffer.
			short := tr.OwnedPointerProjectionIterator(nil, nil, pointReader).(*Iterator)
			defer short.Close()
			capacity := len(keys[1]) - 1
			short.nodeKeyScratch = short.nodeKeyScratch[:capacity:capacity]
			short.leafState.keyScratch = short.leafState.keyScratch[:0:capacity]
			shortRun := func() {
				short.Seek(nil) // Restart key is a page view; the next key must reconstruct.
				if !short.Valid() || !bytes.Equal(short.Key(), keys[0]) {
					t.Fatalf("short scratch restart: %v", short.Error())
				}
				short.Next()
				if short.Valid() || !errors.Is(short.Error(), node.ErrCorruptedNode) {
					t.Fatalf("iterator grew refused key scratch: %v", short.Error())
				}
				if cap(short.nodeKeyScratch) != capacity || cap(short.leafState.keyScratch) != capacity {
					t.Fatal("refused iterator changed scratch capacity")
				}
			}
			if got := testing.AllocsPerRun(20, shortRun); got != 0 {
				t.Fatalf("iterator scratch refusal allocated %g times", got)
			}
		})
	}
}

func TestCOWLimitsDepthAndCycleRefusal(t *testing.T) {
	for _, tc := range []struct {
		name   string
		depth  int
		cycle  bool
		refuse bool
	}{
		{"last_supported_leaf", maxTraversalDepth, false, false},
		{"one_page_too_deep", maxTraversalDepth + 1, false, true},
		{"self_cycle", 1, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := cowLimitsPager(t)
			first, err := p.Alloc(tc.depth)
			if err != nil {
				t.Fatal(err)
			}
			key := []byte("key")
			for i := 0; i < tc.depth; i++ {
				id := first + uint64(i)
				data, err := p.GetForWrite(id)
				if err != nil {
					t.Fatal(err)
				}
				typ := page.PageTypeInternal
				if i == tc.depth-1 && !tc.cycle {
					typ = page.PageTypeLeaf
				}
				b := node.NewBuilder(data, typ)
				b.SetPageID(id)
				if typ == page.PageTypeLeaf {
					err = b.AddLeafEntry(key, []byte("value"), node.FlagInline, page.ValuePtr{})
				} else {
					child := id + 1
					if tc.cycle {
						child = id
					}
					err = b.AddInternalChild(nil, child)
				}
				if err != nil {
					t.Fatal(err)
				}
				b.FinishNoNode()
			}
			tr := New(p, panicValueReader{}, first)
			it := tr.OwnedPointerProjectionIterator(nil, nil, nil).(*Iterator)
			defer it.Close()
			backing := &it.ownedStack[0]
			var keyScratch, leafScratch [page.PageSize]byte
			run := func() {
				it.Seek(key)
				entry, err := tr.GetEntryWithFixedScratch(key, keyScratch[:], leafScratch[:], nil)
				if tc.refuse {
					if it.Valid() || it.Error() == nil || it.Error().Error() != "tree traversal depth exceeded" || err == nil || err.Error() != "tree too deep" {
						t.Fatalf("depth refusal: iterator=%v point=%v", it.Error(), err)
					}
				} else if !it.Valid() || it.Error() != nil || err != nil || !bytes.Equal(entry.Value, []byte("value")) {
					t.Fatalf("supported depth: iterator=%v point=%v", it.Error(), err)
				}
				if len(it.stack) != maxTraversalDepth || cap(it.stack) != maxTraversalDepth || &it.stack[0] != backing {
					t.Fatal("depth traversal replaced or exceeded fixed stack")
				}
			}
			wantAllocs := float64(0)
			if tc.refuse {
				wantAllocs = 2 // One concrete error per API; no growing traversal buffer.
			}
			if got := testing.AllocsPerRun(20, run); got != wantAllocs {
				t.Fatalf("depth operations allocated %g, want %g", got, wantAllocs)
			}
		})
	}
}
