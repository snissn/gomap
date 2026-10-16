package freelist

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/pager"
	"path/filepath"
	"testing"
)

func TestCandidateRootMembershipRejectsRetiredAndForeignGenerationV1(t *testing.T) {
	p, e := pager.Open(filepath.Join(t.TempDir(), "index"), 64<<10)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	if _, e = p.Alloc(8); e != nil {
		t.Fatal(e)
	}
	a := New(p, 0)
	if e = a.EnableCOWV1(MustNewFreelistGenerationV1(1, 8, nil, map[uint64]uint64{3: 1}), NewReservationLedger()); e != nil {
		t.Fatal(e)
	}
	private, e := a.AllocAppend()
	if e != nil {
		t.Fatal(e)
	}
	cap, e := NewReuseCapability(1, 1, 0)
	if e != nil {
		t.Fatal(e)
	}
	c, e := a.PrepareCOWCandidateV1(2, 2, CandidateIDV1{3}, cap, 0, NewCandidatePageSinkV1())
	if e != nil {
		t.Fatal(e)
	}
	aborted := false
	defer func() {
		if aborted {
			return
		}
		if e := a.AbortCOWCandidateV1(c); e != nil {
			t.Error(e)
		}
	}()
	g := c.Candidate().Generation()
	// Export readability at the retirement boundary never grants live output membership.
	if unused, e := g.SnapshotPageUnusedV1(3, 1); e != nil || unused {
		t.Fatalf("pinned boundary: %t %v", unused, e)
	}
	if _, ready, e := c.CertifyRootMembershipV1(private, 3, nil); e == nil || ready {
		t.Fatalf("retired SYSTEM certified: %t %v", ready, e)
	}
	w := &iterator.OrdinalScanWork{RecordLimit: 0, ByteLimit: 1 << 20}
	if _, ready, e := c.CertifyRootMembershipV1(private, 2, w); ready || e != nil {
		t.Fatal(ready, e)
	}
	m, ready, e := c.CertifyRootMembershipV1(private, 2, nil)
	if !ready || e != nil || !m.Matches(c, g, private, 2) {
		t.Fatal("original live candidate", ready, e)
	}
	if m.Matches(c, a.COWGenerationV1(), private, 2) || m.Matches(c, g, private, 3) || m.Matches(&PreparedCOWCandidateV1{}, g, private, 2) {
		t.Fatal("membership rebound")
	}
	if e := a.AbortCOWCandidateV1(c); e != nil {
		t.Fatal(e)
	}
	aborted = true
	if m.Matches(c, g, private, 2) {
		t.Fatal("aborted candidate still binds")
	}
	if _, ready, e := c.CertifyRootMembershipV1(private, 2, nil); ready || e == nil {
		t.Fatal("aborted candidate minted membership", ready, e)
	}

}
