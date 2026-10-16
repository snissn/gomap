package zipper

import (
	"bytes"
	"fmt"
	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
	"path/filepath"
	"testing"
)

func TestPrimaryArenaOrdinaryZipperCoalescesAndMaterializesDataOnly(t *testing.T) {
	dir := t.TempDir()
	p, e := pager.Open(filepath.Join(dir, "index.db"), 256*page.PageSize)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	if _, e = p.Alloc(3); e != nil {
		t.Fatal(e)
	}
	initial, _ := p.GetForWrite(2)
	b := node.NewBuilderWithOptions(initial, page.PageTypeLeaf, node.BuilderOptions{EntryRevisions: true})
	b.SetPageID(2)
	b.FinishNoNode()
	a, e := primaryarena.Open(filepath.Join(dir, "index.db.primary"))
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	if e = a.SetMetadataDecoder(rootpublication.PrimaryBankMetadataEdgesV5); e != nil {
		t.Fatal(e)
	}
	if e = p.AttachPrimaryBankPager(a.Pager()); e != nil {
		t.Fatal(e)
	}
	z := New(p, &MockAllocator{p: p})
	z.SetPrimaryArena(a)
	write := func(root uint64, key, value string, rev page.EntryRevision) uint64 {
		x := batch.New(nil, page.PageSize)
		defer x.Close()
		if e := x.SetOps([]batch.Entry{{Type: batch.OpPut, Key: []byte(key), Value: []byte(value), Revision: rev}}); e != nil {
			t.Fatal(e)
		}
		out, retired, metrics, e := z.Apply(root, x)
		if e != nil {
			t.Fatal(e)
		}
		if !primaryarena.IsPage(out) || len(retired) != 0 || metrics.PrimaryBankRecords == 0 || metrics.PrimaryBankWorkBytes == 0 {
			t.Fatalf("ordinary route not real arena: root%d retired%v metrics%+v", out, retired, metrics)
		}
		return out
	}
	old := write(2, "a", "before", 1)
	oldBundle, ok, e := a.TakePublicationBundle(old, nil)
	if !ok || e != nil {
		t.Fatal(e)
	}
	if _, e = a.Drop(oldBundle.Record, nil); e != nil {
		t.Fatal(e)
	}
	current := write(old, "a", "after", 2)
	if p.PageCount() != 3 {
		t.Fatal("coalesced bank-only write changed DATA extent")
	}
	for _, x := range []struct {
		root uint64
		want string
	}{{old, "before"}, {current, "after"}} {
		got, e := tree.New(p, nil, x.root).Get([]byte("a"))
		if e != nil || !bytes.Equal(got, []byte(x.want)) {
			t.Fatalf("independent root %d %q %v", x.root, got, e)
		}
	}
	image, _ := p.Get(current)
	d, e := node.DecodePrimaryDirectory(image)
	if e != nil || d.Count() != 1 {
		t.Fatal("same-key class accumulated")
	}
	currentBundle, ok, e := a.TakePublicationBundle(current, nil)
	if !ok || e != nil {
		t.Fatal(e)
	}
	if _, _, e = a.TakePublicationBundle(current, nil); e != primaryarena.ErrStale {
		t.Fatal("private output taken twice")
	}
	if _, e = a.Drop(currentBundle.Record, nil); e != nil {
		t.Fatal(e)
	}
	x := batch.New(nil, page.PageSize)
	defer x.Close()
	ops := make([]batch.Entry, 50)
	for i := range ops {
		ops[i] = batch.Entry{Type: batch.OpPut, Key: []byte(fmt.Sprintf("k%02d", i)), Value: []byte("new"), Revision: 3}
	}
	if e = x.SetOps(ops); e != nil {
		t.Fatal(e)
	}
	out, retired, metrics, e := z.Apply(current, x)
	if e != nil {
		t.Fatal(e)
	}
	for _, id := range retired {
		if primaryarena.IsPage(id) {
			t.Fatal("bank ID retired through DATA allocator")
		}
	}
	image, _ = p.Get(out)
	d, e = node.DecodePrimaryDirectory(image)
	if e != nil {
		t.Fatal(e)
	}
	base, seq := d.Base()
	if primaryarena.IsPage(base.Ref.Page) || seq <= 1 || d.Count() != 0 || p.PageCount() <= 3 || metrics.PrimaryConsolidatedCells != 1 {
		t.Fatalf("real ordinary fallback absent: base%+v seq%d count%d metrics%+v", base, seq, d.Count(), metrics)
	}
	got, e := tree.New(p, nil, out).Get([]byte("a"))
	if e != nil || !bytes.Equal(got, []byte("after")) {
		t.Fatalf("materialized overlay lost %q %v", got, e)
	}
	if _, e = a.AbandonPublicationBundle(out, nil); e != nil {
		t.Fatal(e)
	}
	if _, e = a.Drop(currentBundle.Directory, nil); e != nil {
		t.Fatal(e)
	}
	if _, e = a.Drop(oldBundle.Directory, nil); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 200; i++ {
		w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ok, progress, e := a.ReleaseStep(w)
		if !ok || e != nil {
			t.Fatal(e)
		}
		if !progress {
			break
		}
		if i == 199 {
			t.Fatal("ordinary retirement failed bounded progress")
		}
	}
	if a.Counters().GroupsCreated != a.Counters().GroupsFreed {
		t.Fatalf("ordinary group leak %+v", a.Counters())
	}
}
