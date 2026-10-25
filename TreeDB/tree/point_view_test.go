package tree

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type pointViewTestLease struct {
	active, marked bool
	releases       int
}

func (l *pointViewTestLease) ReleaseLeafLogPageView() {
	if !l.active {
		panic("duplicate view release")
	}
	l.active = false
	l.releases++
}
func (l *pointViewTestLease) MarkLeafLogPageViewChecksumVerified() {
	if !l.active {
		panic("mark outside lease")
	}
	l.marked = true
}

func TestPointViewEntryMatchesFixedScratch(t *testing.T) {
	for _, layout := range []node.BuilderOptions{
		{EntryRevisions: true},
		{LeafPrefixCompression: true, EntryRevisions: true},
		{LeafColumnar: true, EntryRevisions: true},
		{LeafColumnar: true, LeafPrefixCompression: true, EntryRevisions: true},
	} {
		t.Run(fmt.Sprintf("prefix=%t/columnar=%t", layout.LeafPrefixCompression, layout.LeafColumnar), func(t *testing.T) {
			tr, keys, _, fixed := ownedSeekTree(t, layout)
			var images [3][page.PageSize]byte
			var leases [3]pointViewTestLease
			for i := range images {
				if _, err := fixed(page.LeafLogPtr{FileID: 8, Offset: uint64(i + 1)}, images[i][:0]); err != nil {
					t.Fatal(err)
				}
			}
			read := func(ptr page.LeafLogPtr, _ []byte) ([]byte, LeafLogPageViewLease, LeafLogPageReadState, error) {
				for i := range leases {
					if leases[i].active {
						t.Fatal("prior lease survives next load")
					}
				}
				i := int(ptr.Offset) - 1
				leases[i].active = true
				return images[i][:], &leases[i], LeafLogPageReadState{RecordChecksumVerified: true, CacheEntryPresent: true, PageChecksumVerified: leases[i].marked}, nil
			}
			var key, leaf, oldKey, oldLeaf [page.PageSize]byte
			for _, i := range []int{0, 1, 2, 23, 24, 47, 48, 71, 3, 0} {
				want, err := tr.GetEntryWithFixedScratch(keys[i], oldKey[:], oldLeaf[:], fixed)
				if err != nil {
					t.Fatal(err)
				}
				for j := range leaf {
					leaf[j] = 0xa5
				}
				got, lease, err := tr.GetEntryWithFixedScratchView(keys[i], key[:], leaf[:], read)
				if err != nil {
					t.Fatal(err)
				}
				if lease == nil || !bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) || got.ValuePtr != want.ValuePtr || got.Flags != want.Flags || got.Revision != want.Revision {
					t.Fatalf("entry mismatch %d: got=%+v want=%+v err=%v", i, got, want, err)
				}
				if !leases[i/24].active || !leases[i/24].marked {
					t.Fatal("missing held lease or successful CRC mark")
				}
				for _, b := range leaf {
					if b != 0xa5 {
						t.Fatal("leased page copied into original scratch")
					}
				}
				lease.ReleaseLeafLogPageView()
			}
			missing := append(bytes.Clone(keys[0]), '!')
			if _, lease, err := tr.GetEntryWithFixedScratchView(missing, key[:], leaf[:], read); !errors.Is(err, ErrKeyNotFound) || lease != nil {
				t.Fatalf("missing: lease=%v err=%v", lease, err)
			}
			for i := range leases {
				if leases[i].active {
					t.Fatal("miss leaked view")
				}
			}
			// The original API still rejects a foreign destination, even valid bytes.
			foreign := func(ptr page.LeafLogPtr, _ []byte) ([]byte, error) { return images[ptr.Offset-1][:], nil }
			if _, err := tr.GetEntryWithFixedScratch(keys[0], key[:], leaf[:], foreign); !errors.Is(err, ErrOwnedIteratorLeafReader) {
				t.Fatalf("original fixed contract: %v", err)
			}
			fallback := func(ptr page.LeafLogPtr, dst []byte) ([]byte, LeafLogPageViewLease, LeafLogPageReadState, error) {
				data, err := fixed(ptr, dst)
				return data, nil, LeafLogPageReadState{}, err
			}
			if got, lease, err := tr.GetEntryWithFixedScratchView(keys[0], key[:], leaf[:], fallback); err != nil || lease != nil || string(got.Value) != "value-0" {
				t.Fatalf("fallback %+v %v", got, err)
			}
			stop := errors.New("read refused")
			failure := func(ptr page.LeafLogPtr, dst []byte) ([]byte, LeafLogPageViewLease, LeafLogPageReadState, error) {
				leases[0].active = true
				return nil, &leases[0], LeafLogPageReadState{}, stop
			}
			if _, lease, err := tr.GetEntryWithFixedScratchView(keys[0], key[:], leaf[:], failure); !errors.Is(err, stop) || lease != nil || leases[0].active {
				t.Fatalf("callback failure lease=%v err=%v", lease, err)
			}
			filter := NewNegativeFilter(1024)
			for _, k := range keys {
				filter.Add(k)
			}
			tr.SetNegativeFilter(filter)
			absent := []byte("definitely-not-present")
			if !filter.DefinitelyAbsent(absent) {
				t.Fatal("negative fixture collision")
			}
			if _, _, err := tr.GetEntryWithFixedScratchView(absent, key[:], leaf[:], failure); !errors.Is(err, ErrKeyNotFound) {
				t.Fatalf("negative filter bypassed: %v", err)
			}
		})
	}
}

func TestPointViewVerifyOnReadAndPrimary(t *testing.T) {
	tr, keys, _, fixed := ownedSeekTree(t, node.BuilderOptions{EntryRevisions: true})
	var image [page.PageSize]byte
	ptr := page.LeafLogPtr{FileID: 8, Offset: 1}
	if _, err := fixed(ptr, image[:0]); err != nil {
		t.Fatal(err)
	}
	lease := pointViewTestLease{}
	read := func(_ page.LeafLogPtr, _ []byte) ([]byte, LeafLogPageViewLease, LeafLogPageReadState, error) {
		if lease.active {
			t.Fatal("prior lease retained")
		}
		lease.active = true
		return image[:], &lease, LeafLogPageReadState{RecordChecksumVerified: true, CacheEntryPresent: true, PageChecksumVerified: lease.marked}, nil
	}
	var key, leaf [page.PageSize]byte
	got, held, err := tr.GetEntryWithFixedScratchView(keys[0], key[:], leaf[:], read)
	if err != nil || held == nil || !lease.marked {
		t.Fatalf("first validation: %+v %v", got, err)
	}
	got.Value[0] ^= 1 // Simulate corruption after earlier immutable-slot proof.
	held.ReleaseLeafLogPageView()
	tr.pager.SetVerifyOnRead(true)
	if _, held, err = tr.GetEntryWithFixedScratchView(keys[0], key[:], leaf[:], read); err == nil || held != nil || lease.active {
		t.Fatalf("VerifyOnRead accepted corruption: %v", err)
	}
	tr.pager.SetVerifyOnRead(false)
	// PRIMARY operands are pager pages, including revision-bearing components.
	// Keep the real internal base and select a one-row pager component so the
	// view API exercises the original CRC, digest and logical-cell checks.
	baseImage, err := tr.pager.Get(tr.rootPageID)
	if err != nil {
		t.Fatal(err)
	}
	base := node.PrimaryOperand{Ref: page.PageChildRef(tr.rootPageID), Digest: sha256.Sum256(baseImage)}
	componentID, err := tr.pager.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	componentImage, err := tr.pager.GetForWrite(componentID)
	if err != nil {
		t.Fatal(err)
	}
	componentBuilder := node.NewBuilderWithOptions(componentImage, page.PageTypeLeaf, node.BuilderOptions{EntryRevisions: true})
	componentBuilder.SetPageID(componentID)
	if err = componentBuilder.AddLeafEntryWithRevision(keys[0], []byte("primary-value"), node.FlagInline, page.ValuePtr{}, 99); err != nil {
		t.Fatal(err)
	}
	componentBuilder.FinishNoNode()
	componentBuilder.ReleaseScratch()
	component := node.PrimaryOperand{Ref: page.PageChildRef(componentID), Digest: sha256.Sum256(componentImage)}
	entries := []node.PrimaryDirectoryEntry{{Key: keys[0], Revision: 99, Kind: node.PrimaryPut, Operand: component}}
	root, err := tr.pager.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	directory, err := tr.pager.GetForWrite(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = node.EncodePrimaryDirectory(directory, root, 1, base, entries); err != nil {
		t.Fatal(err)
	}
	tr.SetRoot(root)
	n := node.NewNode(componentImage)
	_, primaryValue, _, _, _, err := n.GetLeafEntryViewWithRevision(0)
	if err != nil {
		t.Fatal(err)
	}
	primaryValue[0] ^= 1 // Earlier pager proof cannot suppress PRIMARY operand CRC.
	wantPrimary := bytes.Clone(primaryValue)
	if _, held, err = tr.GetEntryWithFixedScratchView(keys[0], key[:], leaf[:], read); err == nil || held != nil || lease.active {
		t.Fatalf("PRIMARY forced CRC accepted corruption: %v", err)
	}
	// Restore checksum but break the authenticated operand digest.
	n.UpdateChecksum()
	if _, held, err = tr.GetEntryWithFixedScratchView(keys[0], key[:], leaf[:], read); !errors.Is(err, node.ErrPrimaryDirectory) || held != nil || lease.active {
		t.Fatalf("PRIMARY digest accepted: %v", err)
	}
	entries[0].Operand.Digest = sha256.Sum256(componentImage)
	if err = node.EncodePrimaryDirectory(directory, root, 1, base, entries); err != nil {
		t.Fatal(err)
	}
	got, held, err = tr.GetEntryWithFixedScratchView(keys[0], key[:], leaf[:], read)
	if err != nil || held != nil || lease.active || !bytes.Equal(got.Value, wantPrimary) || got.Revision != 99 {
		t.Fatalf("PRIMARY valid selected component: %+v %v", got, err)
	}
	entries[0] = node.PrimaryDirectoryEntry{Key: keys[0], Revision: 99, Kind: node.PrimaryAbsence}
	if err = node.EncodePrimaryDirectory(directory, root, 1, base, entries); err != nil {
		t.Fatal(err)
	}
	got, held, err = tr.GetEntryWithFixedScratchView(keys[0], key[:], leaf[:], read)
	if err != nil || held != nil || got.Flags&node.FlagTombstone == 0 || got.Revision != 99 {
		t.Fatalf("PRIMARY absence: %+v %v", got, err)
	}
}

func TestPointViewRootFenceBeforeLeafCallback(t *testing.T) {
	p := cowLimitsPager(t)
	root, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	image, err := p.GetForWrite(root)
	if err != nil {
		t.Fatal(err)
	}
	// Base-delta fences encode pager children, not external child refs. A
	// normal internal child supplies the real leaf-log descent after the fence.
	child, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	childImage, err := p.GetForWrite(child)
	if err != nil {
		t.Fatal(err)
	}
	childBuilder := node.NewBuilder(childImage, page.PageTypeInternal)
	childBuilder.SetPageID(child)
	if err = childBuilder.AddInternalChildRef([]byte("10"), page.LeafLogChildRef(page.LeafLogPtr{FileID: 8, Offset: 1})); err != nil {
		t.Fatal(err)
	}
	childBuilder.FinishNoNode()
	childBuilder.ReleaseScratch()
	b := node.NewBuilderWithOptions(image, page.PageTypeInternal, node.BuilderOptions{InternalBaseDelta: true})
	defer b.ReleaseScratch()
	b.SetPageID(root)
	b.SetInternalFenceBounds([]byte("10"), []byte("20"))
	if err = b.AddInternalChild([]byte("10"), child); err != nil {
		t.Fatal(err)
	}
	b.FinishNoNode()
	tr := New(p, panicValueReader{}, root)
	var key, leaf [page.PageSize]byte
	called := false
	stop := errors.New("leaf called")
	read := func(page.LeafLogPtr, []byte) ([]byte, LeafLogPageViewLease, LeafLogPageReadState, error) {
		called = true
		return nil, nil, LeafLogPageReadState{}, stop
	}
	for _, k := range []string{"05", "20"} {
		if _, _, err = tr.GetEntryWithFixedScratchView([]byte(k), key[:], leaf[:], read); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("fence %s: %v", k, err)
		}
	}
	if called {
		t.Fatal("out-of-bounds query loaded leaf")
	}
	if _, _, err = tr.GetEntryWithFixedScratchView([]byte("15"), key[:], leaf[:], read); !errors.Is(err, stop) || !called {
		t.Fatalf("valid in-fence descent did not reach leaf: called=%t err=%v", called, err)
	}
}
