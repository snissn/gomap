package tree

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func equalOwnedPointEntry(t *testing.T, got, want node.LeafEntry, err, wantErr error) {
	t.Helper()
	if !errors.Is(err, wantErr) || (err == nil) != (wantErr == nil) {
		t.Fatalf("errors %v vs %v", err, wantErr)
	}
	if err == nil && (!bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) || got.ValuePtr != want.ValuePtr || got.Flags != want.Flags || got.Revision != want.Revision) {
		t.Fatalf("entry mismatch: %+v vs %+v", got, want)
	}
}

func TestOwnedPointExactLeafReuse(t *testing.T) {
	for _, layout := range []struct {
		name    string
		options node.BuilderOptions
	}{
		{"plain", node.BuilderOptions{}},
		{"prefix", node.BuilderOptions{LeafPrefixCompression: true}},
		{"columnar", node.BuilderOptions{LeafColumnar: true}},
		{"columnar_prefix", node.BuilderOptions{LeafColumnar: true, LeafPrefixCompression: true}},
	} {
		t.Run(layout.name, func(t *testing.T) {
			layout.options.EntryRevisions = true
			tr, keys, loads, read := ownedSeekTree(t, layout.options)
			var key, leaf, controlKey, controlLeaf [page.PageSize]byte
			var scratch OwnedPointScratch
			// Canonical readers invalidate before overwriting the shared image,
			// including while an owned full traversal is in progress.
			readOwned := func(p page.LeafLogPtr, dst []byte) ([]byte, error) {
				scratch.Invalidate()
				return read(p, dst)
			}
			for _, i := range []int{0, 1, 2, 17, 3, 23} {
				got, err := tr.GetEntryWithOwnedScratch(keys[i], key[:], leaf[:], &scratch, readOwned)
				want, wantErr := tr.GetEntryWithFixedScratch(keys[i], controlKey[:], controlLeaf[:], read)
				equalOwnedPointEntry(t, got, want, err, wantErr)
			}
			// Six control traversals and exactly one owned physical read.
			if *loads != 7 {
				t.Fatalf("physical loads=%d want7 (one owned, six controls)", *loads)
			}
			got, err := tr.GetEntryWithOwnedScratch(keys[7], key[:], leaf[:], &scratch, readOwned)
			if err != nil {
				t.Fatal(err)
			}
			before := *loads
			for range 10 {
				got, err = tr.GetEntryWithOwnedScratch(got.Key, key[:], leaf[:], &scratch, readOwned)
				if err != nil || !bytes.Equal(got.Key, keys[7]) {
					t.Fatalf("aliased target: %q %v", got.Key, err)
				}
			}
			if *loads != before {
				t.Fatal("aliased exact hits reloaded the leaf")
			}
			// A missing interior key cannot establish absence from the held leaf.
			missing := append(bytes.Clone(keys[7]), 'x')
			_, err = tr.GetEntryWithOwnedScratch(missing, key[:], leaf[:], &scratch, readOwned)
			if !errors.Is(err, ErrKeyNotFound) || *loads != before+1 || scratch.leaf.Data() != nil {
				t.Fatalf("interior miss: loads=%d err=%v", *loads, err)
			}
			// Sparse jump goes straight to the chosen leaf, without sweeping.
			before = *loads
			got, err = tr.GetEntryWithOwnedScratch(keys[71], key[:], leaf[:], &scratch, readOwned)
			if err != nil || !bytes.Equal(got.Key, keys[71]) || *loads != before+1 {
				t.Fatal("sparse point swept or missed", err)
			}
		})
	}
}

func TestOwnedPointBindingAndInvalidation(t *testing.T) {
	tr, keys, loads, read := ownedSeekTree(t, node.BuilderOptions{LeafPrefixCompression: true})
	var key, leaf, key2, leaf2 [page.PageSize]byte
	var scratch OwnedPointScratch
	lookup := func(k, l []byte) {
		t.Helper()
		if _, err := tr.GetEntryWithOwnedScratch(keys[7], k, l, &scratch, read); err != nil {
			t.Fatal(err)
		}
	}
	lookup(key[:], leaf[:])
	for _, backing := range []struct{ k, l []byte }{{key2[:], leaf[:]}, {key2[:], leaf2[:]}, {key[:], leaf[:]}} {
		before := *loads
		lookup(backing.k, backing.l)
		if *loads != before+1 {
			t.Fatal("changed backing reused old node")
		}
	}
	scratch.Invalidate()
	clear(leaf[:])
	before := *loads
	lookup(key[:], leaf[:])
	if *loads != before+1 {
		t.Fatal("explicit invalidation did not reload")
	}
	other, _, otherLoads, otherRead := ownedSeekTree(t, node.BuilderOptions{})
	tr.Reset(other.pager, other.slabReader, other.rootPageID)
	if _, err := tr.GetEntryWithOwnedScratch(keys[7], key[:], leaf[:], &scratch, otherRead); err != nil || *otherLoads != 1 {
		t.Fatalf("pager/owner rebind: %v", err)
	}
	before = *otherLoads
	tr.Reset(other.pager, newCountingValueReader(), other.rootPageID)
	if _, err := tr.GetEntryWithOwnedScratch(keys[7], key[:], leaf[:], &scratch, otherRead); err != nil || *otherLoads != before+1 {
		t.Fatal("reader identity did not rebind", err)
	}
	// Durable extent and namespace changes cannot reuse an old exact hit.
	tr.pageLimit = tr.rootPageID
	if _, err := tr.GetEntryWithOwnedScratch(keys[7], key[:], leaf[:], &scratch, otherRead); err == nil || scratch.leaf.Data() != nil {
		t.Fatal("extent rebind bypassed traversal")
	}
	tr.pageLimit = 0
	lookupOther := func() {
		if _, err := tr.GetEntryWithOwnedScratch(keys[7], key[:], leaf[:], &scratch, otherRead); err != nil {
			t.Fatal(err)
		}
	}
	lookupOther()
	tr.pageNamespace = page.PrimaryBankNamespace
	if _, err := tr.GetEntryWithOwnedScratch(keys[7], key[:], leaf[:], &scratch, otherRead); err == nil {
		t.Fatal("namespace rebind bypassed traversal")
	}
}

func TestOwnedPointIntegrityFallbackAndErrors(t *testing.T) {
	tr, keys, loads, read := ownedSeekTree(t, node.BuilderOptions{LeafPrefixCompression: true})
	var key, leaf [page.PageSize]byte
	var scratch OwnedPointScratch
	lookup := func(k []byte, reader func(page.LeafLogPtr, []byte) ([]byte, error)) (node.LeafEntry, error) {
		return tr.GetEntryWithOwnedScratch(k, key[:], leaf[:], &scratch, reader)
	}
	if _, err := lookup(keys[7], read); err != nil {
		t.Fatal(err)
	}
	tr.pager.SetVerifyOnRead(true)
	before := *loads
	if _, err := lookup(keys[7], read); err != nil {
		t.Fatal(err)
	}
	if _, err := lookup(keys[7], read); err != nil || *loads != before+2 || scratch.leaf.Data() != nil {
		t.Fatal("VerifyOnRead reused a held view", err)
	}
	corrupt := func(p page.LeafLogPtr, dst []byte) ([]byte, error) {
		b, err := read(p, dst)
		if err == nil {
			b[len(b)-1] ^= 1
		}
		return b, err
	}
	if _, err := lookup(keys[7], corrupt); err == nil {
		t.Fatal("VerifyOnRead suppressed checksum corruption")
	}
	tr.pager.SetVerifyOnRead(false)
	if _, err := lookup(keys[7], read); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("external leaf unavailable")
	if _, err := lookup(keys[71], func(page.LeafLogPtr, []byte) ([]byte, error) { return nil, failure }); !errors.Is(err, failure) || scratch.leaf.Data() != nil {
		t.Fatal("failed fallback retained view", err)
	}
	if _, err := lookup(keys[7], read); err != nil {
		t.Fatal(err)
	}
	// Foreign backing violates the bounded reader contract, even with valid bytes.
	scratch.Invalidate()
	foreign := func(p page.LeafLogPtr, dst []byte) ([]byte, error) { return read(p, make([]byte, page.PageSize)) }
	if _, err := lookup(keys[7], foreign); !errors.Is(err, ErrOwnedIteratorLeafReader) || scratch.leaf.Data() != nil {
		t.Fatal("accepted foreign leaf backing", err)
	}
	long := bytes.Repeat([]byte("z"), page.PageSize+1)
	_, err := lookup(long, read)
	var ck, cl [page.PageSize]byte
	_, wantErr := tr.GetEntryWithFixedScratch(long, ck[:], cl[:], read)
	if !errors.Is(err, wantErr) || (err == nil) != (wantErr == nil) || !bytes.Equal(long, bytes.Repeat([]byte("z"), len(long))) {
		t.Fatal("oversized fallback changed target/error", err, wantErr)
	}
	// The old API still reads and validates each time, independently of new state.
	if _, err := tr.GetEntryWithFixedScratch(keys[7], key[:], leaf[:], read); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.GetEntryWithFixedScratch(keys[7], key[:], leaf[:], corrupt); err == nil {
		t.Fatal("old fixed API bypassed corruption")
	}
}

func TestOwnedPointRootFencesAndNegativeFilter(t *testing.T) {
	tr, keys, loads, read := ownedSeekTree(t, node.BuilderOptions{})
	root, err := tr.pager.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	image, err := tr.pager.GetForWrite(root)
	if err != nil {
		t.Fatal(err)
	}
	b := node.NewBuilderWithOptions(image, page.PageTypeInternal, node.BuilderOptions{InternalBaseDelta: true})
	b.SetPageID(root)
	b.SetInternalFenceBounds(keys[1], keys[23])
	if err := b.AddInternalChild(keys[0], tr.rootPageID); err != nil {
		t.Fatal(err)
	}
	b.FinishNoNode()
	b.ReleaseScratch()
	tr.SetRoot(root)
	var key, leaf [page.PageSize]byte
	var scratch OwnedPointScratch
	lookup := func(k []byte) error {
		_, err := tr.GetEntryWithOwnedScratch(k, key[:], leaf[:], &scratch, read)
		return err
	}
	for _, out := range [][]byte{keys[0], keys[23]} {
		if err := lookup(keys[7]); err != nil {
			t.Fatal(err)
		}
		before := *loads
		if err := lookup(out); !errors.Is(err, ErrKeyNotFound) || *loads != before {
			t.Fatal("held hit escaped original root fence", err)
		}
	}
	if err := lookup(keys[7]); err != nil {
		t.Fatal(err)
	}
	// This controlled coverage replacement tests the negative gate itself;
	// production owners must of course supply complete root coverage.
	tr.SetNegativeFilter(NewNegativeFilter(128))
	if err := lookup(keys[7]); !errors.Is(err, ErrKeyNotFound) {
		t.Fatal("held hit bypassed negative gate", err)
	}
}

func TestOwnedPointPrimaryNeverReusesBaseLeaf(t *testing.T) {
	tr, keys, loads, read := ownedSeekTree(t, node.BuilderOptions{LeafPrefixCompression: true, EntryRevisions: true})
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
	root, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	rootImage, err := p.GetForWrite(root)
	if err != nil {
		t.Fatal(err)
	}
	entries := []node.PrimaryDirectoryEntry{
		{Key: keys[7], Revision: 99, Kind: node.PrimaryPut, Operand: node.PrimaryOperand{Ref: page.PageChildRef(component), Digest: sha256.Sum256(image)}},
		{Key: keys[17], Revision: 100, Kind: node.PrimaryAbsence, ClassSlot: 1},
	}
	if err := node.EncodePrimaryDirectory(rootImage, root, 1, base, entries); err != nil {
		t.Fatal(err)
	}
	var key, leaf [page.PageSize]byte
	var scratch OwnedPointScratch
	// Reuse across a root switch must re-enter directory selection.
	if _, err := tr.GetEntryWithOwnedScratch(keys[1], key[:], leaf[:], &scratch, read); err != nil {
		t.Fatal(err)
	}
	tr.SetRoot(root)
	for _, i := range []int{1, 3, 7, 17, 1, 3} {
		before := *loads
		got, err := tr.GetEntryWithOwnedScratch(keys[i], key[:], leaf[:], &scratch, read)
		if err != nil {
			t.Fatal(err)
		}
		if i == 7 && (string(got.Value) != "delta" || got.Revision != 99) {
			t.Fatal("base hid interior override")
		}
		if i == 17 && (got.Flags&node.FlagTombstone == 0 || got.Revision != 100) {
			t.Fatal("base hid inline absence")
		}
		if (i == 1 || i == 3) && *loads == before {
			t.Fatal("PRIMARY base leaf reused without selection")
		}
		if scratch.leaf.Data() != nil {
			t.Fatal("PRIMARY retained a reusable leaf")
		}
	}
}
