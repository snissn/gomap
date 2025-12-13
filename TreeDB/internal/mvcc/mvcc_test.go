package mvcc

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/page"
	"github.com/snissn/gomap/TreeDB/internal/pager"
	"github.com/snissn/gomap/TreeDB/internal/slab"
)

func newTestPager(t *testing.T) *pager.Pager {
	t.Helper()
	p, err := pager.Open(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("open pager: %v", err)
	}
	return p
}

func emptySlabSet() *slab.SlabSet {
	return slab.NewSlabSet(map[uint32]*slab.SlabFile{})
}

func TestRegistryMinPinnedAdvances(t *testing.T) {
	r := NewRegistry()
	r.SetCurrentSeq(20)

	r.Pin(10)
	r.Pin(12)
	r.Pin(15)
	if got := r.MinPinnedSeq(); got != 10 {
		t.Fatalf("min pinned = %d, want 10", got)
	}

	r.Unpin(10)
	if got := r.MinPinnedSeq(); got != 12 {
		t.Fatalf("min pinned = %d, want 12", got)
	}
}

func TestPrunerHoldReleaseMovesToFreelist(t *testing.T) {
	p := newTestPager(t)
	defer p.Close()

	g := NewGraveyard()
	holder := NewStateHolder(&DBState{CommitSeq: 10, SlabSet: emptySlabSet()})
	reg := holder.Registry()
	pruner := NewPruner(p, g, reg, 0)

	snap, err := holder.AcquireSnapshot()
	if err != nil {
		t.Fatalf("acquire snapshot: %v", err)
	}

	old1, _ := p.AllocPage()
	old2, _ := p.AllocPage()

	reg.SetCurrentSeq(11)
	g.Record(11, []page.PageID{old1, old2})

	if err := pruner.Prune(11); err != nil {
		t.Fatalf("prune while pinned: %v", err)
	}
	got, _ := p.AllocPage()
	if got == old1 || got == old2 {
		t.Fatalf("unexpected reuse while snapshot open")
	}

	if err := snap.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}

	reg.SetCurrentSeq(12)
	if err := pruner.Prune(12); err != nil {
		t.Fatalf("prune after release: %v", err)
	}
	a1, _ := p.AllocPage()
	a2, _ := p.AllocPage()
	if !((a1 == old1 && a2 == old2) || (a1 == old2 && a2 == old1)) {
		t.Fatalf("expected retired pages reused, got %d and %d", a1, a2)
	}
}

func TestPrunerReachabilityBarrier(t *testing.T) {
	p := newTestPager(t)
	defer p.Close()

	g := NewGraveyard()
	holder := NewStateHolder(&DBState{CommitSeq: 20, SlabSet: emptySlabSet()})
	reg := holder.Registry()
	pruner := NewPruner(p, g, reg, 0)

	snap, err := holder.AcquireSnapshot()
	if err != nil {
		t.Fatalf("acquire snapshot: %v", err)
	}

	old, _ := p.AllocPage()
	g.Record(5, []page.PageID{old}) // Record 5 first (older)

	reachable, _ := p.AllocPage()
	reg.SetCurrentSeq(21)
	g.Record(21, []page.PageID{reachable}) // Record 21 second (newer)

	reg.SetCurrentSeq(30)
	if err := pruner.Prune(30); err != nil {
		t.Fatalf("prune: %v", err)
	}

	a, _ := p.AllocPage()
	if a != old {
		t.Fatalf("expected old page %d reclaimed, got %d", old, a)
	}
	b, _ := p.AllocPage()
	if b == reachable {
		t.Fatalf("reachable page reclaimed while snapshot open")
	}

	if err := snap.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}
	reg.SetCurrentSeq(31)
	if err := pruner.Prune(31); err != nil {
		t.Fatalf("prune after close: %v", err)
	}
	c, _ := p.AllocPage()
	if c != reachable {
		t.Fatalf("expected reachable page reclaimed after close, got %d", c)
	}
}

func TestPrunerKeepRecentWindow(t *testing.T) {
	p := newTestPager(t)
	defer p.Close()

	g := NewGraveyard()
	holder := NewStateHolder(&DBState{CommitSeq: 100, SlabSet: emptySlabSet()})
	reg := holder.Registry()
	pruner := NewPruner(p, g, reg, 10)

	p95, _ := p.AllocPage()
	p85, _ := p.AllocPage()
	// Record in order: 85 then 95
	g.Record(85, []page.PageID{p85})
	g.Record(95, []page.PageID{p95})

		reg.SetCurrentSeq(100)

		if err := pruner.Prune(100); err != nil {

			t.Fatalf("prune: %v", err)

		}
	
		a, _ := p.AllocPage()
		if a != p85 {
			t.Fatalf("expected seq85 page reclaimed, got %d", a)
		}
		
		found95 := false
		for _, b := range g.retired {
			if b.seq == 95 {
				found95 = true
				break
			}
		}
		if !found95 {
			t.Fatalf("seq95 pages should remain due to KeepRecent")
		}
	
		// Reader pinned outside history protects seq85.
		g2 := NewGraveyard()
		p85b, _ := p.AllocPage()
		g2.Record(85, []page.PageID{p85b})
		reg.Pin(85)
		defer reg.Unpin(85)
	
		pruner2 := NewPruner(p, g2, reg, 10)
		if err := pruner2.Prune(100); err != nil {
			t.Fatalf("prune with reader: %v", err)
		}
		
		found85 := false
		for _, b := range g2.retired {
			if b.seq == 85 {
				found85 = true
				break
			}
		}
		if !found85 {
			t.Fatalf("seq85 pages should remain due to pinned reader")
		}
	}
func TestSnapshotPinsAndReleasesSlabs(t *testing.T) {
	dir := t.TempDir()
	mgr, set, err := slab.Load(dir, 0, 0)
	if err != nil {
		t.Fatalf("load slabs: %v", err)
	}
	defer mgr.Close()

	f, _ := set.Get(0)
	path := filepath.Join(dir, "data-0000.slab")

	holder := NewStateHolder(&DBState{CommitSeq: 1, SlabSet: set})

	snap, err := holder.AcquireSnapshot()
	if err != nil {
		t.Fatalf("acquire snapshot: %v", err)
	}
	if got := f.RefCount.Load(); got != 1 {
		t.Fatalf("refcount after pin = %d, want 1", got)
	}

	if err := f.MarkZombie(); err != nil {
		t.Fatalf("mark zombie: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected slab file to exist while pinned: %v", err)
	}

		if err := snap.Close(); err != nil {
			t.Fatalf("close snapshot: %v", err)
		}
		// RefCount should be 1 because Manager (and Holder) still hold the set.
		// O(1) snapshot change: File pins are group-based.
		// Set RefCount > 0 => File RefCount = 1.
		if got := f.RefCount.Load(); got != 1 {
			t.Fatalf("refcount after close = %d, want 1", got)
		}
		// File should still exist (zombie but pinned by set)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected slab file to exist after close: %v", err)
		}
	}
