package primaryarena

import (
	"bytes"
	"crypto/sha256"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestPrimaryCapsuleReadRootOwnsCopyAndComponentCustody(t *testing.T) {
	a, err := OpenCapsule(filepath.Join(t.TempDir(), "index.db.primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if a.Pager().PageCount() != 8 {
		t.Fatalf("fixed extent %d", a.Pager().PageCount())
	}
	c := testComponent(t, a, "old", 1)
	if Local(c.PageID) < 8 {
		t.Fatal("component overlaps fixed capsules")
	}
	r, ok, err := a.PrepareReadRootV6(nil)
	if err != nil || !ok {
		t.Fatalf("prepare %t %v", ok, err)
	}
	image, err := a.Get(c.PageID)
	if err != nil {
		t.Fatal(err)
	}
	var cells [groupSize]Ref
	cells[0] = c
	g, ok, err := a.NewGroup(cells, nil)
	if err != nil || !ok {
		t.Fatal(err)
	}
	var groups [groupCount]*Group
	groups[0] = g
	scratch := make([]byte, page.PageSize)
	base := node.PrimaryOperand{Ref: page.PageChildRef(2), Digest: sha256.Sum256([]byte("DATA base"))}
	entry := node.PrimaryDirectoryEntry{Key: []byte("a"), Operand: node.PrimaryOperand{Ref: page.PageChildRef(c.PageID), Digest: sha256.Sum256(image)}, Kind: node.PrimaryPut, Revision: 1}
	if err = node.EncodePrimaryDirectory(scratch, r.PageID, 1, base, []node.PrimaryDirectoryEntry{entry}); err != nil {
		t.Fatal(err)
	}
	if ok, err = a.SealDirectory(r, scratch, groups, nil); err != nil || !ok {
		t.Fatalf("seal %t %v", ok, err)
	}
	owned, err := a.Get(r.PageID)
	if err != nil {
		t.Fatal(err)
	}
	want := append([]byte(nil), owned...)
	clear(scratch)
	if !bytes.Equal(owned, want) {
		t.Fatal("read root borrowed caller scratch")
	}
	data, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	if err = data.AttachPrimaryBankPager(a.Pager()); err != nil {
		t.Fatal(err)
	}
	fromData, err := data.Get(r.PageID)
	if err != nil || !bytes.Equal(fromData, want) {
		t.Fatalf("shared read route %v", err)
	}
	if _, err = a.Acquire(r, nil); err != nil {
		t.Fatal(err)
	}
	a.DropGroup(g, nil)
	a.Drop(c, nil)
	a.Drop(r, nil)
	testDrain(t, a)
	if a.valid(c) == nil {
		t.Fatal("old read root released component")
	}
	w := &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}
	if ok, err = a.Drop(r, w); ok || err != nil {
		t.Fatalf("unadmitted drop %t %v", ok, err)
	}
	if _, err = a.Drop(r, nil); err != nil {
		t.Fatal(err)
	}
	testDrain(t, a)
	if _, err = data.Get(r.PageID); err == nil {
		t.Fatal("released read address still registered")
	}
	if a.valid(c) != nil {
		t.Fatal("released component retained")
	}
	reused := testClaim(t, a, Component)
	if reused.PageID != c.PageID {
		t.Fatalf("read root entered physical free list: %d", reused.PageID)
	}
	if _, err = a.Acquire(r, nil); err == nil {
		t.Fatal("released root revived")
	}
}

func TestPrimaryCapsulePromotionCloneAdmissionAndIndependentCustody(t *testing.T) {
	a, err := OpenCapsule(filepath.Join(t.TempDir(), "index.db.primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	c := testComponent(t, a, "old", 1)
	componentImage, err := a.Get(c.PageID)
	if err != nil {
		t.Fatal(err)
	}
	r, ok, err := a.PrepareReadRootV6(nil)
	if !ok || err != nil {
		t.Fatal(err)
	}
	var cells [groupSize]Ref
	cells[0] = c
	g, ok, err := a.NewGroup(cells, nil)
	if !ok || err != nil {
		t.Fatal(err)
	}
	var groups [groupCount]*Group
	groups[0] = g
	image := make([]byte, page.PageSize)
	entry := node.PrimaryDirectoryEntry{Key: []byte("a"), Operand: node.PrimaryOperand{Ref: page.PageChildRef(c.PageID), Digest: sha256.Sum256(componentImage)}, Kind: node.PrimaryPut, Revision: 1}
	base := node.PrimaryOperand{Ref: page.PageChildRef(2), Digest: sha256.Sum256([]byte("DATA"))}
	if err = node.EncodePrimaryDirectory(image, r.PageID, 1, base, []node.PrimaryDirectoryEntry{entry}); err != nil {
		t.Fatal(err)
	}
	if ok, err = a.SealDirectory(r, image, groups, nil); !ok || err != nil {
		t.Fatal(err)
	}
	sourceImage, _ := a.Get(r.PageID)
	original := bytes.Clone(sourceImage)
	before, serial, refs := a.Counters(), a.readSerial, g.refs
	short := &iterator.OrdinalScanWork{RecordLimit: 4, ByteLimit: 1 << 20}
	if clone, ok, err := a.CloneReadRootForPromotionV6(r, short); ok || err != nil || clone != (Ref{}) {
		t.Fatalf("under-admitted clone %v %v %v", clone, ok, err)
	}
	if a.Counters() != before || a.readSerial != serial || g.refs != refs {
		t.Fatal("refusal mutated custody")
	}
	w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	clone, ok, err := a.CloneReadRootForPromotionV6(r, w)
	if !ok || err != nil {
		t.Fatal(err)
	}
	if clone == r || a.valid(clone).construction != nil || a.valid(clone).privatePublication || g.refs != refs+1 {
		t.Fatal("clone lacks independent immutable custody")
	}
	clonedImage, _ := a.Get(clone.PageID)
	if page.DecodeHeader(clonedImage).PageID != clone.PageID || !page.VerifyChecksumNonMutating(clonedImage) {
		t.Fatal("clone registration/integrity mismatch")
	}
	decoded, err := node.DecodePrimaryDirectory(clonedImage)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decoded.Entry(0)
	if err != nil || !bytes.Equal(got.Key, entry.Key) || got.Operand != entry.Operand {
		t.Fatal("clone changed logical component")
	}
	if !bytes.Equal(sourceImage, original) {
		t.Fatal("clone mutated original reader")
	}
	a.DropGroup(g, nil)
	a.Drop(c, nil)
	a.Drop(r, nil)
	testDrain(t, a)
	if a.valid(c) == nil || a.valid(clone) == nil {
		t.Fatal("source retirement consumed clone custody")
	}
	a.Drop(clone, nil)
	testDrain(t, a)
	if a.valid(c) != nil {
		t.Fatal("last clone release leaked component")
	}
	t.Logf("distinct promotion clone %dR/%dB plus ordinary outgoing release debt", w.Records, w.Bytes)
}
