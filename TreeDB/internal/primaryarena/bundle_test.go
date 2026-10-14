package primaryarena

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"path/filepath"
	"testing"
)

func testBundle(t *testing.T, a *Arena) PublicationBundle {
	t.Helper()
	c, ok, e := a.PrepareBundleClaim(nil)
	if !ok || e != nil {
		t.Fatalf("prepare pair %t %v", ok, e)
	}
	for i := 0; i < 100; i++ {
		w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		r, ready, e := c.Step(w)
		if e != nil {
			t.Fatal(e)
		}
		if ready {
			return r
		}
	}
	t.Fatal("pair made no bounded progress")
	return PublicationBundle{}
}
func TestPrimaryArenaPairReuseWaitsForBothIndependentOwners(t *testing.T) {
	a, e := Open(filepath.Join(t.TempDir(), "index.db.primary"))
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	a.SetMetadataDecoder(func(Class, []byte) ([]MetadataEdge, error) { return nil, nil })
	old := testBundle(t, a)
	if old.Directory.PageID%2 != 0 || old.Record.PageID != old.Directory.PageID+1 {
		t.Fatal("unaligned distinct pair")
	}
	if _, e = a.Drop(old.Directory, nil); e != nil {
		t.Fatal(e)
	}
	testDrain(t, a)
	next := testBundle(t, a)
	if next.Directory.PageID == old.Directory.PageID {
		t.Fatal("pair recycled independently owned record")
	}
	if _, e = a.Drop(old.Record, nil); e != nil {
		t.Fatal(e)
	}
	testDrain(t, a)
	reuse := testBundle(t, a)
	if reuse.Directory.PageID != old.Directory.PageID || reuse.Record.PageID != old.Record.PageID || reuse.Directory.Incarnation == old.Directory.Incarnation || reuse.Record.Incarnation == old.Record.Incarnation {
		t.Fatal("fully released pair did not reuse with independent incarnations")
	}
	for _, r := range []Ref{next.Directory, next.Record, reuse.Directory, reuse.Record} {
		if _, e = a.Drop(r, nil); e != nil {
			t.Fatal(e)
		}
	}
	testDrain(t, a)
	component := testClaim(t, a, Component)
	if a.valid(component) == nil {
		t.Fatal("split pair component unowned")
	}
	buddy := testClaim(t, a, Component)
	if buddy.PageID != component.PageID+1 {
		t.Fatalf("split pair buddy lost %d %d", component.PageID, buddy.PageID)
	}
	held := testBundle(t, a)
	_ = held
	if _, e = a.Drop(component, nil); e != nil {
		t.Fatal(e)
	}
	if _, e = a.Drop(buddy, nil); e != nil {
		t.Fatal(e)
	}
	testDrain(t, a)
	w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if ok, e := a.RepackFreePairsStep(w); !ok || e != nil {
		t.Fatalf("repack %t %v", ok, e)
	}
	pair := testBundle(t, a)
	if pair.Directory.PageID != component.PageID {
		t.Fatal("released split pair not recovered by real allocator scan")
	}
	unique := testClaim(t, a, Component)
	if unique.PageID == pair.Directory.PageID || unique.PageID == pair.Record.PageID {
		t.Fatal("stale generic list aliased live pair")
	}
}
func TestPrimaryArenaPairAlignmentPaddingAndStaleClaim(t *testing.T) {
	a, e := Open(filepath.Join(t.TempDir(), "index.db.primary"))
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	a.SetMetadataDecoder(func(Class, []byte) ([]MetadataEdge, error) { return nil, nil })
	component := testClaim(t, a, Component)
	c, ok, e := a.PrepareBundleClaim(nil)
	if !ok || e != nil {
		t.Fatal(e)
	}
	pair := testBundle(t, a)
	if pair.Directory.PageID != component.PageID+2 {
		t.Fatal("alignment padding not in physical extent")
	}
	if _, _, e = c.Step(nil); e != ErrStale {
		t.Fatalf("stale private claim accepted %v", e)
	}
	padding := testClaim(t, a, Component)
	if padding.PageID != component.PageID+1 {
		t.Fatal("actual padding bank not reusable")
	}
}
