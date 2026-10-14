package pager

import (
	"bytes"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
	"path/filepath"
	"testing"
)

func TestPrimaryAdjacentWriteAdmissionPhysicalViewsAndDurability(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db.primary")
	p, e := Open(path, 4*page.PageSize)
	if e != nil {
		t.Fatal(e)
	}
	if e = p.GrowTo(4); e != nil {
		t.Fatal(e)
	}
	p.SetPageCount(4)
	left, right := make([]byte, page.PageSize), make([]byte, page.PageSize)
	left[100] = 1
	right[100] = 2
	page.UpdateChecksum(left)
	page.UpdateChecksum(right)
	p.MarkVerified(2)
	p.MarkVerified(3)
	short := &iterator.OrdinalScanWork{RecordLimit: 4, ByteLimit: 1 << 20}
	if _, _, ok, e := p.WriteAdjacentViewsWithWork(2, left, right, short); ok || e == nil {
		t.Fatalf("underadmitted pair %t %v", ok, e)
	}
	before, _ := p.Get(2)
	if before[100] != 0 || !p.IsVerified(2) || !p.IsVerified(3) {
		t.Fatal("failed admission mutated actual pair")
	}
	work := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	l, r, ok, e := p.WriteAdjacentViewsWithWork(2, left, right, work)
	if !ok || e != nil {
		t.Fatalf("pair %t %v", ok, e)
	}
	if work.Records != 5 || p.IsVerified(2) || p.IsVerified(3) || p.dirtyChunks.count == 0 {
		t.Fatalf("actual shared mapping/dirty/verify %d", work.Records)
	}
	clear(left)
	clear(right)
	if l[100] != 1 || r[100] != 2 || !page.VerifyChecksumNonMutating(l) || !page.VerifyChecksumNonMutating(r) {
		t.Fatal("physical views borrowed caller scratch")
	}
	if _, _, ok, e := p.WriteAdjacentViewsWithWork(3, l, r, nil); ok || e == nil {
		t.Fatal("unaligned pair accepted")
	}
	l = bytes.Clone(l)
	r = bytes.Clone(r)
	if e = p.Sync(); e != nil {
		t.Fatal(e)
	}
	if e = p.Close(); e != nil {
		t.Fatal(e)
	}
	p, e = Open(path, 4*page.PageSize)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	p.SetPageCount(4)
	l2, _ := p.Get(2)
	r2, _ := p.Get(3)
	if !bytes.Equal(l, l2) || !bytes.Equal(r, r2) {
		t.Fatal("distinct pair not durable")
	}
}
