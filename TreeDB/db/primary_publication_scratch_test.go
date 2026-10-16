package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"testing"
	"unsafe"
)

type primaryScratchBudget struct{ capacity uint64 }
type primaryScratchLease struct{ budget *primaryScratchBudget }

func (b *primaryScratchBudget) AcquireRetention(bytes uint64) (retainedalloc.Lease, error) {
	if bytes > b.capacity {
		return nil, retainedalloc.ErrCapacity
	}
	return &primaryScratchLease{b}, nil
}
func (l *primaryScratchLease) Resize(bytes uint64) error {
	if bytes > l.budget.capacity {
		return retainedalloc.ErrCapacity
	}
	return nil
}
func (l *primaryScratchLease) Close() {}

func TestPrimaryManifestScratchRefusesBeforeBankClaim(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), IndexPrimaryDirectory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	a := d.idx.Load().primary
	m, err := rootpublication.NewDependencyManifestV1(nil)
	if err != nil {
		t.Fatal(err)
	}
	_, seed, err := materializePrimaryManifestV5(d.idx.Load(), m)
	if err != nil {
		t.Fatal(err)
	}
	if err = seed.release(); err != nil {
		t.Fatal(err)
	}
	for {
		ready, progress, err := a.ReleaseStep(nil)
		if err != nil {
			t.Fatal(err)
		}
		if ready && !progress {
			break
		}
	}
	budget := &primaryScratchBudget{capacity: ^uint64(0)}
	enrollment, err := a.MetadataOwner().Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	before := a.Counters()
	budget.capacity = a.MetadataOwner().Bytes() + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryBankConstructionV5{}))) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryarena.Ref{})))
	_, _, err = materializePrimaryManifestV5(d.idx.Load(), m)
	if !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("private page/ref backing was not admitted: %v", err)
	}
	if a.Counters().Claims != before.Claims {
		t.Fatal("bank claimed before scratch admission")
	}
}

// Growth must reserve the complete replacement while the original backing is
// still live; a budget allowing only the net delta must refuse before effects.
func TestPrimaryBankConstructionReplacementOverlapAndOnceDisposal(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), IndexPrimaryDirectory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	idx := d.idx.Load()
	a := idx.primary
	budget := &primaryScratchBudget{capacity: ^uint64(0)}
	enrollment, err := a.MetadataOwner().Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := a.MetadataOwner().Bytes()
	claims, err := newPrimaryBankConstructionV5(idx)
	if err != nil {
		t.Fatal(err)
	}
	if err = claims.claim(primaryarena.Dependency, 1); err != nil {
		t.Fatal(err)
	}
	original := claims.refs[0]
	before := a.MetadataOwner().Bytes()
	counters := a.Counters()
	old := retainedalloc.AllocationCharge(uint64(cap(claims.refs)) * uint64(unsafe.Sizeof(primaryarena.Ref{})))
	replacement := retainedalloc.AllocationCharge(2 * uint64(unsafe.Sizeof(primaryarena.Ref{})))
	budget.capacity = before + replacement - old
	if err = claims.ensureCapacity(2); !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("replacement overlap accepted: %v", err)
	}
	if claims.refs[0] != original || cap(claims.refs) != 1 || a.MetadataOwner().Bytes() != before || a.Counters().Claims != counters.Claims {
		t.Fatal("failed growth changed original custody")
	}
	budget.capacity = ^uint64(0)
	if err = claims.ensureCapacity(2); err != nil {
		t.Fatal(err)
	}
	if a.MetadataOwner().Bytes() != before+replacement-old || claims.refs[0] != original {
		t.Fatal("replacement did not transfer exact backing")
	}
	if err = claims.release(); err != nil {
		t.Fatal(err)
	}
	drops := a.Counters().RootDrops
	if err = claims.release(); err != nil || a.Counters().RootDrops != drops {
		t.Fatal("disposal replayed bank edge")
	}
	for {
		ready, progress, err := a.ReleaseStep(nil)
		if err != nil {
			t.Fatal(err)
		}
		if ready && !progress {
			break
		}
	}
	if a.MetadataOwner().Bytes() != baseline {
		t.Fatalf("private storage remains: got%d baseline%d", a.MetadataOwner().Bytes(), baseline)
	}
}

func TestPrimaryRecordScratchRefusalBeforeCodecAndSeal(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), IndexPrimaryDirectory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	a := d.idx.Load().primary
	budget := &primaryScratchBudget{capacity: ^uint64(0)}
	enrollment, err := a.MetadataOwner().Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	before, counters := a.MetadataOwner().Bytes(), a.Counters()
	budget.capacity = before
	// Invalid operands would return a codec/ownership error if reached. The
	// admission error must precede both encoder and actual bank effects.
	_, err = sealPrimaryRecordV5(a, primaryarena.Ref{}, rootpublication.DurablePrimaryRootRecordV5{})
	if !errors.Is(err, retainedalloc.ErrCapacity) || a.MetadataOwner().Bytes() != before || a.Counters() != counters {
		t.Fatal("record scratch refusal reached effects", err)
	}
	budget.capacity = ^uint64(0)
	_, err = sealPrimaryRecordV5(a, primaryarena.Ref{}, rootpublication.DurablePrimaryRootRecordV5{})
	if !errors.Is(err, rootpublication.ErrDurableRootRecordFormat) || a.MetadataOwner().Bytes() != before || a.Counters() != counters {
		t.Fatal("codec refusal stranded scratch", err)
	}
}

func TestPrimaryDependencySealFramesRefuseBeforeChildAndRejectCycle(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), IndexPrimaryDirectory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	idx := d.idx.Load()
	a := idx.primary
	alloc, err := newPrimaryDependencyAllocatorV5(idx)
	if err != nil {
		t.Fatal(err)
	}
	defer alloc.release()
	child, err := alloc.Alloc(0)
	if err != nil {
		t.Fatal(err)
	}
	root, err := alloc.Alloc(0)
	if err != nil {
		t.Fatal(err)
	}
	image, err := idx.pager.GetForWrite(child)
	if err != nil {
		t.Fatal(err)
	}
	leaf := node.NewBuilder(image, page.PageTypeLeaf)
	leaf.SetPageID(child)
	leaf.FinishNoNode()
	image, err = idx.pager.GetForWrite(root)
	if err != nil {
		t.Fatal(err)
	}
	branch := node.NewBuilder(image, page.PageTypeInternal)
	branch.SetPageID(root)
	if err = branch.AddInternalChild(nil, child); err != nil {
		t.Fatal(err)
	}
	branch.FinishNoNode()
	budget := &primaryScratchBudget{capacity: ^uint64(0)}
	enrollment, err := a.MetadataOwner().Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	budget.capacity = a.MetadataOwner().Bytes()
	before := a.Counters()
	if err = alloc.seal(root); !errors.Is(err, retainedalloc.ErrCapacity) || a.Counters() != before {
		t.Fatal("frame refusal sealed an operand", err)
	}
	budget.capacity = ^uint64(0)
	if err = alloc.seal(root); err != nil {
		t.Fatal("child-first seal", err)
	}
	if a.Counters().PagesWritten != before.PagesWritten+2 {
		t.Fatal("did not seal both physical banks")
	}
	cycle, err := newPrimaryDependencyAllocatorV5(idx)
	if err != nil {
		t.Fatal(err)
	}
	defer cycle.release()
	id, err := cycle.Alloc(0)
	if err != nil {
		t.Fatal(err)
	}
	image, err = idx.pager.GetForWrite(id)
	if err != nil {
		t.Fatal(err)
	}
	branch = node.NewBuilder(image, page.PageTypeInternal)
	branch.SetPageID(id)
	if err = branch.AddInternalChild(nil, id); err != nil {
		t.Fatal(err)
	}
	branch.FinishNoNode()
	before = a.Counters()
	if err = cycle.seal(id); !errors.Is(err, rootpublication.ErrDependencyManifestFormat) || a.Counters() != before {
		t.Fatal("cycle reached bank seal", err)
	}
}
