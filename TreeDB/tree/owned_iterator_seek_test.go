package tree

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// External leaf reads are observable physical loads through the real owned
// iterator, rather than an implementation hook or a copied seek predicate.
func ownedSeekTree(t *testing.T, opts node.BuilderOptions) (*Tree, [][]byte, *int, func(page.LeafLogPtr, []byte) ([]byte, error)) {
	t.Helper()
	p := cowLimitsPager(t)
	// PRIMARY operands require user page IDs above the two metadata pages.
	if _, err := p.Alloc(2); err != nil {
		t.Fatal(err)
	}
	root, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	data, err := p.GetForWrite(root)
	if err != nil {
		t.Fatal(err)
	}
	b := node.NewBuilder(data, page.PageTypeInternal)
	b.SetPageID(root)
	defer b.ReleaseScratch()
	var images [3][page.PageSize]byte
	keys := make([][]byte, 72)
	for l := range images {
		leaf := node.NewBuilderWithOptions(images[l][:], page.PageTypeLeaf, opts)
		for j := 0; j < 24; j++ {
			i := l*24 + j
			keys[i] = []byte(fmt.Sprintf("%s%04d", strings.Repeat("namespace/", 8), i*2))
			flags, value, ptr := byte(node.FlagInline), []byte(fmt.Sprintf("value-%d", i)), page.ValuePtr{}
			if i%3 == 1 {
				flags, value, ptr = node.FlagPointer, nil, page.ValuePtr{FileID: 7, Offset: uint64(i + 100), Length: 9}
			}
			if i%3 == 2 {
				flags, value = node.FlagTombstone, nil
			}
			if err := leaf.AddLeafEntryWithRevision(keys[i], value, flags, ptr, page.EntryRevision(i+1)); err != nil {
				t.Fatal(err)
			}
		}
		leaf.FinishNoNode()
		leaf.ReleaseScratch()
		if err := b.AddInternalChildRef(keys[l*24], page.LeafLogChildRef(page.LeafLogPtr{FileID: 8, Offset: uint64(l + 1)})); err != nil {
			t.Fatal(err)
		}
	}
	b.FinishNoNode()
	loads := new(int)
	read := func(ref page.LeafLogPtr, dst []byte) ([]byte, error) {
		if ref.FileID != 8 || ref.Offset < 1 || ref.Offset > 3 || cap(dst) != page.PageSize {
			t.Fatal("unexpected leaf read")
		}
		*loads++
		copy(dst[:page.PageSize], images[ref.Offset-1][:])
		return dst[:page.PageSize], nil
	}
	return New(p, panicValueReader{}, root), keys, loads, read
}

func TestOwnedSeekReusesHeldLeaf(t *testing.T) {
	tr, keys, loads, read := ownedSeekTree(t, node.BuilderOptions{LeafPrefixCompression: true, EntryRevisions: true})
	it := tr.OwnedPointerProjectionIterator(nil, nil, read)
	defer it.Close()
	before := *loads
	for _, i := range []int{1, 7, 17, 23} {
		it.Seek(keys[i])
		if !it.Valid() || !bytes.Equal(it.Key(), keys[i]) {
			t.Fatalf("key %d: %q %v", i, it.Key(), it.Error())
		}
	}
	if *loads != before {
		t.Fatalf("same-leaf seeks reloaded physical leaf: before=%d after=%d", before, *loads)
	}
}

func compareOwnedSeek(t *testing.T, got, control iterator.UnsafeIterator) {
	t.Helper()
	if got.Valid() != control.Valid() || (got.Error() == nil) != (control.Error() == nil) {
		t.Fatalf("valid/error: %v/%v vs %v/%v", got.Valid(), got.Error(), control.Valid(), control.Error())
	}
	if !got.Valid() {
		return
	}
	gv, gp, gf, gr := iterator.UnsafeEntryWithRevision(got)
	cv, cp, cf, cr := iterator.UnsafeEntryWithRevision(control)
	if !bytes.Equal(got.Key(), control.Key()) || !bytes.Equal(gv, cv) || gp != cp || gf != cf || gr != cr {
		t.Fatalf("entry mismatch: %q/%q %v/%v flags %d/%d revision %d/%d", got.Key(), control.Key(), gp, cp, gf, cf, gr, cr)
	}
}

func TestOwnedSeekSparseEquivalenceAndFallback(t *testing.T) {
	for _, layout := range []struct {
		name string
		opts node.BuilderOptions
	}{
		{"plain", node.BuilderOptions{}},
		{"prefix", node.BuilderOptions{LeafPrefixCompression: true}},
		{"columnar", node.BuilderOptions{LeafColumnar: true}},
		{"columnar_prefix", node.BuilderOptions{LeafColumnar: true, LeafPrefixCompression: true}},
	} {
		t.Run(layout.name, func(t *testing.T) {
			layout.opts.EntryRevisions = true
			tr, keys, loads, read := ownedSeekTree(t, layout.opts)
			it := tr.OwnedPointerProjectionIterator(keys[1], keys[70], read).(*Iterator)
			defer it.Close()
			control := tr.OwnedPointerProjectionIterator(keys[1], keys[70], read).(*Iterator)
			defer control.Close()
			for _, key := range [][]byte{nil, keys[2], keys[7], append(bytes.Clone(keys[7]), 'x'), keys[17], keys[23], keys[69], keys[70], keys[71], []byte("zzzz"), keys[3], keys[1]} {
				it.Seek(key)
				// Bypass only the new branch, retaining the original root
				// positioner and the control's bounded external-leaf owner.
				controlKey := key
				if controlKey == nil || bytes.Compare(controlKey, keys[1]) < 0 {
					controlKey = keys[1]
				}
				control.seek(controlKey)
				compareOwnedSeek(t, it, control)
				if it.Valid() {
					it.Next()
					control.Next()
					compareOwnedSeek(t, it, control)
				}
			}
			// Current borrowed keys can live in either reconstruction buffer.
			// Seeking to one must not let the last-key probe overwrite target.
			it.Seek(keys[7])
			before := *loads
			if allocs := testing.AllocsPerRun(50, func() {
				it.Seek(it.UnsafeKey())
				if !it.Valid() || !bytes.Equal(it.Key(), keys[7]) {
					t.Fatal("borrowed target corrupted")
				}
			}); allocs != 0 {
				t.Fatalf("fixed seek allocated %g", allocs)
			}
			if *loads != before {
				t.Fatal("borrowed same-leaf seek reloaded")
			}
			it.Seek(keys[0]) // clamps to start, and is backward from current
			before = *loads
			it.Seek(keys[69]) // jump over the entire middle leaf
			if !it.Valid() || !bytes.Equal(it.Key(), keys[69]) || *loads != before+1 {
				t.Fatalf("gap swept unrelated leaves: loads %d→%d error=%v", before, *loads, it.Error())
			}
		})
	}
}

func TestOwnedSeekReverseErrorAndExcludedControl(t *testing.T) {
	tr, keys, loads, read := ownedSeekTree(t, node.BuilderOptions{LeafPrefixCompression: true})
	reverse := tr.OwnedReversePointerProjectionIterator(keys[1], keys[70], read)
	defer reverse.Close()
	before := *loads
	for _, i := range []int{7, 17, 3, 69} {
		reverse.Seek(keys[i])
		if !reverse.Valid() || !bytes.Equal(reverse.Key(), keys[i]) {
			t.Fatalf("reverse seek: %v", reverse.Error())
		}
	}
	if *loads <= before {
		t.Fatal("reverse stopped using original positioning")
	}
	empty := tr.OwnedPointerProjectionIterator(keys[7], keys[7], read)
	defer empty.Close()
	empty.Seek(keys[17])
	if empty.Valid() || empty.Error() != nil {
		t.Fatal("empty domain changed")
	}
	failure := errors.New("leaf unavailable")
	fail := false
	it := tr.OwnedPointerProjectionIterator(nil, nil, func(ref page.LeafLogPtr, dst []byte) ([]byte, error) {
		if fail {
			return nil, failure
		}
		return read(ref, dst)
	})
	defer it.Close()
	fail = true
	it.Seek(keys[71])
	if it.Valid() || !errors.Is(it.Error(), failure) {
		t.Fatalf("lost load error: %v", it.Error())
	}
	fail = false
	it.Seek(keys[7])
	if !it.Valid() || it.Error() != nil || !bytes.Equal(it.Key(), keys[7]) {
		t.Fatal("error recovery did not root seek")
	}
}

func TestOwnedSeekPrimaryDeltaSelection(t *testing.T) {
	tr, keys, _, read := ownedSeekTree(t, node.BuilderOptions{LeafPrefixCompression: true, LeafColumnar: true, EntryRevisions: true})
	p := tr.pager
	baseImage, err := p.Get(tr.rootPageID)
	if err != nil {
		t.Fatal(err)
	}
	base := node.PrimaryOperand{Ref: page.PageChildRef(tr.rootPageID), Digest: sha256.Sum256(baseImage)}
	component, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	image, err := p.GetForWrite(component)
	if err != nil {
		t.Fatal(err)
	}
	b := node.NewBuilder(image, page.PageTypeLeaf)
	b.SetPageID(component)
	if err := b.AddLeafEntryWithRevision(keys[7], []byte("delta"), node.FlagInline, page.ValuePtr{}, 99); err != nil {
		t.Fatal(err)
	}
	b.FinishNoNode()
	b.ReleaseScratch()
	entries := []node.PrimaryDirectoryEntry{
		{Key: keys[7], Revision: 99, Kind: node.PrimaryPut, Operand: node.PrimaryOperand{Ref: page.PageChildRef(component), Digest: sha256.Sum256(image)}},
		{Key: keys[17], Revision: 100, Kind: node.PrimaryAbsence, ClassSlot: 1},
	}
	root, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	image, err = p.GetForWrite(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = node.EncodePrimaryDirectory(image, root, 1, base, entries); err != nil {
		t.Fatal(err)
	}
	tr.SetRoot(root)
	it := tr.OwnedPointerProjectionIterator(nil, nil, read)
	defer it.Close()
	for _, i := range []int{1, 7, 17, 23, 47, 71, 3, 7} {
		it.Seek(keys[i])
		if !it.Valid() || !bytes.Equal(it.Key(), keys[i]) {
			t.Fatalf("PRIMARY key %d: %q %v", i, it.Key(), it.Error())
		}
		v, _, f, r := iterator.UnsafeEntryWithRevision(it)
		if i == 7 && (string(v) != "delta" || r != 99) {
			t.Fatal("PRIMARY delta overridden by base")
		}
		if i == 17 && (f&node.FlagTombstone == 0 || r != 100) {
			t.Fatal("PRIMARY absence overridden by base")
		}
	}
}
