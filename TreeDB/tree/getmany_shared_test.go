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

// Three internal levels mix legacy page IDs, base-delta IDs and leaf-log refs.
func sharedGetManyFixture(t testing.TB) (*Tree, *statefulLeafLogPageReader, [][]byte, map[string][]byte) {
	t.Helper()
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	_, err = p.Alloc(2) // selected root extents start at page 2
	if err != nil {
		t.Fatal(err)
	}
	root, err := p.Alloc(7)
	if err != nil {
		t.Fatal(err)
	}
	reader := &statefulLeafLogPageReader{mapValueReader: newMapValueReader()}
	want := make(map[string][]byte)
	pointer := reader.Add([]byte("pointer-value"))
	refs := make([]page.ChildRef, 8)
	for leaf := range refs {
		data := make([]byte, page.PageSize)
		b := node.NewBuilderWithOptions(data, page.PageTypeLeaf, node.BuilderOptions{LeafPrefixCompression: true})
		b.SetPageID(uint64(leaf + 100))
		for k := leaf * 16; k < (leaf+1)*16; k++ {
			if k == 7 {
				continue
			}
			key := []byte(fmt.Sprintf("k%03d", k))
			val := []byte(fmt.Sprintf("value-%03d", k))
			flags := byte(node.FlagInline)
			ptr := page.ValuePtr{}
			switch k {
			case 0:
				val = nil
				flags = node.FlagPointer
				ptr = pointer
				want[string(key)] = []byte("pointer-value")
			case 4:
				val = nil
				want[string(key)] = []byte{}
			case 5:
				val = nil
				flags = node.FlagTombstone
			default:
				want[string(key)] = val
			}
			if err := b.AddLeafEntry(key, val, flags, ptr); err != nil {
				t.Fatal(err)
			}
		}
		b.FinishNoNode()
		ptr := page.LeafLogPtr{FileID: 1, Offset: uint64(1000 + leaf*page.PageSize), RecordLengthHint: page.PageSize}
		reader.values[ptr.ValuePtr()] = data
		refs[leaf] = page.LeafLogChildRef(ptr)
	}
	for i := 0; i < 7; i++ {
		data, err := p.GetForWrite(root + uint64(i))
		if err != nil {
			t.Fatal(err)
		}
		b := node.NewBuilderWithOptions(data, page.PageTypeInternal, node.BuilderOptions{InternalBaseDelta: i < 3})
		b.SetPageID(root + uint64(i))
		if i == 0 {
			b.SetInternalFenceBounds([]byte("k000"), []byte("k128"))
		}
		for j := 0; j < 2; j++ {
			var ref page.ChildRef
			var first int
			switch {
			case i == 0:
				ref = page.PageChildRef(root + uint64(1+j))
				first = j * 64
			case i < 3:
				ref = page.PageChildRef(root + uint64(3+(i-1)*2+j))
				first = (i-1)*64 + j*32
			default:
				ref = refs[(i-3)*2+j]
				first = (i-3)*32 + j*16
			}
			key := []byte(fmt.Sprintf("k%03d", first))
			if i < 3 {
				err = b.AddInternalChild(key, ref.Page)
			} else {
				err = b.AddInternalChildRef(key, ref)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		b.FinishNoNode()
	}
	tr := New(p, reader, root)
	keys := make([][]byte, 0, 72)
	for i := 0; i < 64; i++ {
		keys = append(keys, []byte(fmt.Sprintf("k%03d", (i*37)%128)))
	}
	keys = append(keys, []byte("k000"), []byte("k004"), []byte("k005"), []byte("k007"), []byte("j999"), []byte("k128"), []byte("k037"), []byte("k037"))
	return tr, reader, keys, want
}

func TestTreeGetManySharedTraversal(t *testing.T) {
	tr, reader, keys, want := sharedGetManyFixture(t)
	original := make([][]byte, len(keys))
	for i := range keys {
		original[i] = bytes.Clone(keys[i])
	}
	out := make([][]byte, len(keys))
	for i := range out {
		out[i] = []byte("stale")
	}
	if _, err := tr.GetManyAppend(keys, out, make([]byte, 0, 2048)); err != nil {
		t.Fatal(err)
	}
	for i, key := range keys {
		val, found := want[string(key)]
		if !bytes.Equal(out[i], val) || (out[i] != nil) != found || cap(out[i]) != len(out[i]) {
			t.Fatalf("owned[%d] %q=%q want %q found=%v", i, key, out[i], val, found)
		}
		if !bytes.Equal(key, original[i]) {
			t.Fatal("caller key mutated")
		}
	}
	out[len(out)-1][0] = 'X'
	if !bytes.Equal(out[len(out)-2], want["k037"]) {
		t.Fatal("duplicate owned values alias")
	}
	if reader.views != 8 || reader.releases != 8 {
		t.Fatalf("owned leaf leases %d/%d", reader.views, reader.releases)
	}
	seen := make([]bool, len(keys))
	if err := tr.GetManyView(keys, func(i int, key, val []byte, found bool) error {
		expected, ok := want[string(keys[i])]
		if seen[i] || !bytes.Equal(key, keys[i]) || found != ok || !bytes.Equal(val, expected) || (val != nil) != found {
			t.Fatalf("view[%d] %q=%q found=%v", i, key, val, found)
		}
		if bytes.Compare(key, []byte("k000")) >= 0 && bytes.Compare(key, []byte("k128")) < 0 && reader.views != reader.releases+1 {
			t.Fatal("lease not held through callback")
		}
		seen[i] = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i, ok := range seen {
		if !ok {
			t.Fatalf("missing callback %d", i)
		}
	}
	if reader.views != reader.releases {
		t.Fatal("view lease leak")
	}
	stop := errors.New("stop")
	calls := 0
	if err := tr.GetManyView(keys, func(int, []byte, []byte, bool) error { calls++; return stop }); !errors.Is(err, stop) || calls != 1 {
		t.Fatal(err, calls)
	}
	if reader.views != reader.releases {
		t.Fatal("callback error leaked lease")
	}
}

func TestTreeGetManySharedTraversalErrorsBeforeCallbacks(t *testing.T) {
	for _, kind := range []string{"depth", "checksum", "extent", "type", "pager-leaf"} {
		t.Run(kind, func(t *testing.T) {
			tr, reader, keys, _ := sharedGetManyFixture(t)
			data, err := tr.pager.GetForWrite(tr.rootPageID + 6)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "depth":
				b := node.NewBuilder(data, page.PageTypeInternal)
				b.SetPageID(tr.rootPageID + 6)
				if err := b.AddInternalChild([]byte("k096"), tr.rootPageID+6); err != nil {
					t.Fatal(err)
				}
				b.FinishNoNode()
			case "checksum":
				data[len(data)-1] ^= 1
				tr.pager.SetVerifyOnRead(true)
			case "extent":
				tr.pageLimit = tr.rootPageID + 6
			case "type":
				n := node.NewNode(data)
				n.SetType(99)
				n.UpdateChecksum()
			case "pager-leaf":
				b := node.NewBuilder(data, page.PageTypeLeaf)
				b.SetPageID(tr.rootPageID + 6)
				b.FinishNoNode()
			}
			if kind == "pager-leaf" {
				scratch := getGetManyScratch(len(keys))
				groupable, err := tr.planGetMany(keys, scratch, false)
				putGetManyScratch(scratch)
				if err != nil || groupable || reader.views != 0 {
					t.Fatal("fallback planner materialized leaf", err, groupable, reader.views)
				}
			}
			calls := 0
			err = tr.GetManyView(keys, func(int, []byte, []byte, bool) error { calls++; return nil })
			if kind == "pager-leaf" {
				if err != nil || calls != len(keys) {
					t.Fatal(err, calls)
				}
				return
			}
			message := map[string]string{"depth": "tree too deep", "checksum": "checksum mismatch", "extent": "outside selected root extent", "type": "invalid page type"}[kind]
			if err == nil || !strings.Contains(err.Error(), message) || calls != 0 || reader.views != 0 {
				t.Fatal(err, calls, reader.views)
			}
			out := make([][]byte, len(keys))
			if _, err := tr.GetManyAppend(keys, out, nil); err == nil || !strings.Contains(err.Error(), message) {
				t.Fatal(err)
			}
		})
	}
}

func TestTreeGetManySharedTraversalScratch(t *testing.T) {
	tr, _, keys, _ := sharedGetManyFixture(t)
	scratch := getGetManyScratch(len(keys))
	// Warm the existing map/slices before checking the planner itself.
	if ok, err := tr.planGetMany(keys, scratch, false); err != nil || !ok {
		t.Fatal(ok, err)
	}
	// Keep the same buffers: the race runtime deliberately drops sync.Pool
	// entries, which would measure pool misses rather than planner allocations.
	if allocs := testing.AllocsPerRun(100, func() {
		clear(scratch.probes)
		clear(scratch.groups)
		clear(scratch.present)
		clear(scratch.groupByRef)
		scratch.probes = scratch.probes[:0]
		scratch.groups = scratch.groups[:0]
		if ok, err := tr.planGetMany(keys, scratch, false); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}); allocs != 0 {
		t.Fatalf("warm planner allocated %g times", allocs)
	}
	putGetManyScratch(scratch)
	for _, probe := range scratch.probes[:cap(scratch.probes)] {
		if probe.key != nil {
			t.Fatal("pooled borrowed key")
		}
	}
	for _, group := range scratch.groups[:cap(scratch.groups)] {
		if group.ref != (page.ChildRef{}) {
			t.Fatal("pooled child ref")
		}
	}
	if len(scratch.groupByRef) != 0 {
		t.Fatal("pooled ref map")
	}
}

func TestTreeGetManySharedTraversalFirstChild(t *testing.T) {
	reader := newMapValueReader()
	data := make([]byte, page.PageSize)
	b := node.NewBuilder(data, page.PageTypeLeaf)
	if err := b.AddLeafEntry(nil, []byte("empty-key"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatal(err)
	}
	if err := b.AddLeafEntry([]byte("k000"), []byte("first-child"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatal(err)
	}
	b.FinishNoNode()
	ptr := page.LeafLogPtr{FileID: 1, Offset: 1000, RecordLengthHint: page.PageSize}
	reader.values[ptr.ValuePtr()] = data
	// Below the first separator still routes to its child without a root fence.
	tr, _ := newTreeWithLeafLogRoot(t, reader, []byte("k010"), ptr)
	keys := make([][]byte, getManyLeafGroupMinKeys)
	for i := range keys {
		switch i % 3 {
		case 0:
			keys[i] = nil
		case 1:
			keys[i] = []byte{}
		default:
			keys[i] = []byte("k000")
		}
	}
	out := make([][]byte, len(keys))
	if _, err := tr.GetManyAppend(keys, out, nil); err != nil {
		t.Fatal(err)
	}
	expected := func(i int) []byte {
		if i%3 == 2 {
			return []byte("first-child")
		}
		return []byte("empty-key")
	}
	for i := range keys {
		if !bytes.Equal(out[i], expected(i)) {
			t.Fatal(i, out[i])
		}
	}
	if err := tr.GetManyView(keys, func(i int, key, val []byte, found bool) error {
		if !found || !bytes.Equal(key, keys[i]) || !bytes.Equal(val, expected(i)) {
			t.Fatal(i, key, val, found)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
