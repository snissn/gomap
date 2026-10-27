package tree

import (
	"crypto/sha256"
	"path/filepath"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func ownedProjectionShapeFixture(t testing.TB, primary bool) (*Tree, []byte, []byte) {
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
	if _, err := p.Alloc(2); err != nil {
		t.Fatal(err)
	}
	makeLeaf := func(keys []string, value string) (uint64, []byte) {
		id, err := p.Alloc(1)
		if err != nil {
			t.Fatal(err)
		}
		image, err := p.GetForWrite(id)
		if err != nil {
			t.Fatal(err)
		}
		b := node.NewBuilderWithOptions(image, page.PageTypeLeaf, node.BuilderOptions{EntryRevisions: true})
		b.SetPageID(id)
		for _, key := range keys {
			if err := b.AddLeafEntryWithRevision([]byte(key), []byte(value), node.FlagInline, page.ValuePtr{}, 7); err != nil {
				t.Fatal(err)
			}
		}
		b.FinishNoNode()
		return id, image
	}
	baseID, base := makeLeaf([]string{"a", "c", "d", "e"}, "base")
	if !primary {
		return New(p, nil, baseID), base, nil
	}
	deltaID, delta := makeLeaf([]string{"c"}, "delta")
	root, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	image, err := p.GetForWrite(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.EncodePrimaryDirectory(image, root, 7,
		node.PrimaryOperand{Ref: page.PageChildRef(baseID), Digest: sha256.Sum256(base)},
		[]node.PrimaryDirectoryEntry{
			{Key: []byte("c"), Revision: 7, Kind: node.PrimaryPut, Operand: node.PrimaryOperand{Ref: page.PageChildRef(deltaID), Digest: sha256.Sum256(delta)}},
			{Key: []byte("d"), Revision: 7, Kind: node.PrimaryAbsence, ClassSlot: 1},
		}); err != nil {
		t.Fatal(err)
	}
	return New(p, nil, root), base, delta
}

func TestOwnedProjectionShapesBoundsAndAllocation(t *testing.T) {
	for _, primary := range []bool{false, true} {
		name := "ordinary"
		if primary {
			name = "primary"
		}
		t.Run(name, func(t *testing.T) {
			tr, _, _ := ownedProjectionShapeFixture(t, primary)
			for _, reverse := range []bool{false, true} {
				open := tr.OwnedPointerProjectionIterator
				want := []string{"c", "d"}
				if reverse {
					open = tr.OwnedReversePointerProjectionIterator
					want = []string{"d", "c"}
				}
				start, end := []byte("b"), []byte("e")
				if allocs := testing.AllocsPerRun(100, func() {
					it := open(start, end, nil)
					for _, key := range want {
						if !it.Valid() || string(it.UnsafeKey()) != key {
							t.Fatalf("reverse=%v key=%q want=%q err=%v", reverse, it.UnsafeKey(), key, it.Error())
						}
						value, _, flags := it.UnsafeEntry()
						if primary && key == "d" {
							if flags&node.FlagTombstone == 0 {
								t.Fatal("PRIMARY absence lost")
							}
						} else if primary && key == "c" {
							if string(value) != "delta" {
								t.Fatal("PRIMARY delta lost precedence")
							}
						} else if string(value) != "base" {
							t.Fatalf("value=%q want=base", value)
						}
						it.Next()
					}
					if it.Valid() || it.Error() != nil {
						t.Fatalf("exclusive bound: %v", it.Error())
					}
					_ = it.Close()
					_ = it.Close()
				}); allocs != 1 {
					t.Fatalf("reverse=%v constructor and traversal allocated %g times, want one owner", reverse, allocs)
				}
			}
		})
	}
}

func TestOwnedProjectionShapesPrimaryExternalBaseLeaf(t *testing.T) {
	tr, baseImage, _ := ownedProjectionShapeFixture(t, true)
	leaf := append([]byte(nil), baseImage...)
	image, err := tr.pager.GetForWrite(tr.rootPageID)
	if err != nil {
		t.Fatal(err)
	}
	directory, err := node.DecodePrimaryDirectory(image)
	if err != nil {
		t.Fatal(err)
	}
	base, _ := directory.Base()
	delta, _ := directory.Entry(0)
	ref := page.LeafLogPtr{FileID: 7, Offset: 16}
	clear(baseImage)
	b := node.NewBuilder(baseImage, page.PageTypeInternal)
	b.SetPageID(base.Ref.Page)
	if err := b.AddInternalChildRef([]byte("a"), page.LeafLogChildRef(ref)); err != nil {
		t.Fatal(err)
	}
	b.FinishNoNode()
	b.ReleaseScratch()
	base.Digest = sha256.Sum256(baseImage)
	if err := node.EncodePrimaryDirectory(image, tr.rootPageID, 7, base,
		[]node.PrimaryDirectoryEntry{
			{Key: []byte("c"), Revision: 7, Kind: node.PrimaryPut, Operand: delta.Operand},
			{Key: []byte("d"), Revision: 7, Kind: node.PrimaryAbsence, ClassSlot: 1},
		}); err != nil {
		t.Fatal(err)
	}
	for _, reverse := range []bool{false, true} {
		open := tr.OwnedPointerProjectionIterator
		if reverse {
			open = tr.OwnedReversePointerProjectionIterator
		}
		for _, readLeaf := range []func(page.LeafLogPtr, []byte) ([]byte, error){nil, func(page.LeafLogPtr, []byte) ([]byte, error) { return leaf, nil }} {
			it := open([]byte("c"), []byte("d"), readLeaf)
			if it.Valid() || it.Error() != ErrOwnedIteratorLeafReader {
				t.Fatalf("unowned PRIMARY base leaf accepted, reverse=%v err=%v", reverse, it.Error())
			}
			_ = it.Close()
		}
		var backing *byte
		reads := 0
		readLeaf := func(got page.LeafLogPtr, dst []byte) ([]byte, error) {
			reads++
			if got != ref || cap(dst) != page.PageSize {
				t.Fatal("wrong PRIMARY base leaf or scratch capacity")
			}
			dst = dst[:page.PageSize]
			if backing == nil {
				backing = &dst[0]
			} else if backing != &dst[0] {
				t.Fatal("PRIMARY base leaf replaced admitted scratch")
			}
			copy(dst, leaf)
			return dst, nil
		}
		it := open([]byte("b"), []byte("e"), readLeaf)
		for range 3 {
			it.Seek([]byte("c"))
			if !it.Valid() || string(it.UnsafeKey()) != "c" || string(it.Value()) != "delta" || it.Error() != nil {
				t.Fatalf("bounded PRIMARY delta lost: reverse=%v err=%v", reverse, it.Error())
			}
			it.Next()
			if it.Error() != nil {
				t.Fatalf("PRIMARY traversal failed after delta: reverse=%v err=%v", reverse, it.Error())
			}
			if reverse {
				if it.Valid() {
					t.Fatalf("reverse traversal continued below c: key=%q", it.UnsafeKey())
				}
			} else if !it.Valid() || string(it.UnsafeKey()) != "d" || !it.IsDeleted() {
				t.Fatalf("forward traversal after c: valid=%v deleted=%v", it.Valid(), it.IsDeleted())
			}
		}
		if reads == 0 {
			t.Fatal("external PRIMARY base leaf was not read")
		}
		_ = it.Close()
	}
}

func TestOwnedProjectionShapesIndependentReaders(t *testing.T) {
	for _, primary := range []bool{false, true} {
		tr, _, _ := ownedProjectionShapeFixture(t, primary)
		var readers sync.WaitGroup
		for range 8 {
			readers.Go(func() {
				for range 32 {
					forward := tr.OwnedPointerProjectionIterator(nil, nil, nil)
					reverse := tr.OwnedReversePointerProjectionIterator(nil, nil, nil)
					_ = forward.Close()
					if !reverse.Valid() || string(reverse.UnsafeKey()) != "e" || reverse.Error() != nil {
						t.Errorf("independent reverse invalidated: %v", reverse.Error())
					}
					_ = reverse.Close()
				}
			})
		}
		readers.Wait()
	}
}

func TestOwnedProjectionShapesRejectCorruptOperands(t *testing.T) {
	for _, target := range []string{"ordinary", "primary_base", "primary_delta"} {
		t.Run(target, func(t *testing.T) {
			tr, base, delta := ownedProjectionShapeFixture(t, target != "ordinary")
			image := base
			if target == "primary_delta" {
				image = delta
			}
			image[len(image)-1] ^= 1
			if target != "ordinary" {
				n := node.NewNode(image)
				n.UpdateChecksum() // Valid page CRC cannot substitute for operand digest.
			}
			for _, reverse := range []bool{false, true} {
				var it iterator.UnsafeIterator
				if reverse {
					it = tr.OwnedReversePointerProjectionIterator([]byte("c"), []byte("d"), nil)
				} else {
					it = tr.OwnedPointerProjectionIterator([]byte("c"), []byte("d"), nil)
				}
				if it.Valid() || it.Error() == nil {
					t.Fatalf("corrupt %s accepted, reverse=%v", target, reverse)
				}
				_ = it.Close()
			}
		})
	}
}

func BenchmarkOwnedProjectionShapeConstructor(b *testing.B) {
	for _, primary := range []bool{false, true} {
		name := "ordinary"
		if primary {
			name = "primary"
		}
		b.Run(name, func(b *testing.B) {
			tr, _, _ := ownedProjectionShapeFixture(b, primary)
			key := []byte("c")
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				it := tr.OwnedPointerProjectionIterator(key, nil, nil)
				if !it.Valid() || string(it.UnsafeKey()) != "c" || it.Error() != nil {
					b.Fatal(it.Error())
				}
				it.Next()
				if err := it.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
