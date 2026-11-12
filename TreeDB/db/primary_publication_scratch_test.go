package db

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/freelist"
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

// These representation/ownership controls must pass for both the original
// independent scratch and the borrowed-final-page implementation. They are not
// behavioral RED tests or an observation of hidden materializer locals.
func primaryBorrowedScratchManifests(t *testing.T, count int) (streaming, cached *rootpublication.DependencyManifestV1) {
	t.Helper()
	fixtures := make([]projectionResourceFixtureV1, count)
	for i := range fixtures {
		fixtures[i] = projectionResourceFixtureV1{
			kind:         rootpublication.ResourceValueLog,
			reachability: rootpublication.ReachabilityValueLogPointer,
		}
	}
	set := projectionResourceSetV1(t, fixtures...)
	t.Cleanup(func() {
		if err := set.ReleaseWithError(); err != nil {
			t.Error("release resource set", err)
		}
	})
	var err error
	streaming, _, err = set.DependencyManifestV1()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(streaming.ReleaseOwnedMetadataV1)
	if err = streaming.WithEntriesV1(func(entries []rootpublication.DependencyManifestEntryV1) error {
		cached, err = rootpublication.NewDependencyManifestV1(entries)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cached.ReleaseOwnedMetadataV1)
	if streaming.PageCount() != cached.PageCount() {
		t.Fatal("representation changed page count")
	}
	if count == 1 && streaming.PageCount() != 1 {
		t.Fatalf("single-resource fixture has %d pages, want 1", streaming.PageCount())
	}
	if count == 128 && streaming.PageCount() < 3 {
		t.Fatalf("128-resource fixture has %d pages, want at least 3", streaming.PageCount())
	}
	return streaming, cached
}

func primaryBorrowedScratchDB(t *testing.T) *DB {
	t.Helper()
	d, err := Open(Options{Dir: t.TempDir(), IndexPrimaryDirectory: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error("original DB Close", err)
		}
	})
	return d
}

func primaryBorrowedScratchDrain(t *testing.T, a *primaryarena.Arena) {
	t.Helper()
	// Each fixture has at most 128 resources and a finite manifest chain. This
	// bound fails instead of allowing an incorrect release cursor to spin.
	for step := 0; step < 10000; step++ {
		ready, progress, err := a.ReleaseStep(nil)
		if err != nil || !ready {
			t.Fatalf("arena release step=%d ready=%t error=%v", step, ready, err)
		}
		if !progress {
			return
		}
	}
	t.Fatal("arena release exceeded finite fixture bound")
}

func primaryBorrowedScratchCheckMaterializer(t *testing.T, d *DB, m *rootpublication.DependencyManifestV1) {
	t.Helper()
	idx := d.idx.Load()
	a := idx.primary
	primaryBorrowedScratchDrain(t, a)
	baseline := a.MetadataOwner().Bytes()
	before := a.Counters()
	ref, claims, err := materializePrimaryManifestV5(idx, m)
	if err != nil {
		t.Fatal("actual materializer", err)
	}
	released := false
	defer func() {
		if !released {
			if err := claims.release(); err != nil {
				t.Error("original claim cleanup", err)
			}
		}
	}()
	if len(claims.refs) != int(m.PageCount()) || a.Counters().PagesWritten-before.PagesWritten != uint64(m.PageCount()) {
		t.Fatal("actual materializer did not seal the complete manifest chain")
	}
	store := freelist.NewMemoryPageStoreV1()
	want, err := m.Materialize(ref.FirstPageID, store)
	if err != nil || ref != want {
		t.Fatalf("reference changed: got=%+v want=%+v error=%v", ref, want, err)
	}
	for i := uint32(0); i < ref.PageCount; i++ {
		id := ref.FirstPageID + uint64(i)
		image, err := idx.pager.ReadPage(id)
		if err != nil || !bytes.Equal(image, store.Pages[id]) {
			t.Fatalf("sealed page %d differs after local scratch cleanup: %v", id, err)
		}
	}
	loaded, err := rootpublication.LoadDependencyManifestV1(idx.pager, ref)
	if err != nil {
		t.Fatal("load sealed chain after scratch cleanup", err)
	}
	defer loaded.ReleaseOwnedMetadataV1()
	loadedStore := freelist.NewMemoryPageStoreV1()
	loadedRef, err := loaded.Materialize(ref.FirstPageID, loadedStore)
	if err != nil || loadedRef != ref {
		t.Fatal("loaded manifest reference changed", err)
	}
	for id, image := range store.Pages {
		if !bytes.Equal(image, loadedStore.Pages[id]) {
			t.Fatalf("loaded canonical page %d changed", id)
		}
	}
	if err := claims.release(); err != nil {
		t.Fatal("release original construction", err)
	}
	released = true
	drops := a.Counters().RootDrops
	if err := claims.release(); err != nil || a.Counters().RootDrops != drops {
		t.Fatal("repeated disposal replayed a bank edge", err)
	}
	primaryBorrowedScratchDrain(t, a)
	if got := a.MetadataOwner().Bytes(); got != baseline {
		t.Fatalf("owner charge after disposal=%d want pre-call=%d", got, baseline)
	}
}

func TestPrimaryManifestBorrowedScratchCanonicalSealedPages(t *testing.T) {
	for _, count := range []int{1, 128} {
		t.Run(fmt.Sprintf("resources=%d", count), func(t *testing.T) {
			streaming, cached := primaryBorrowedScratchManifests(t, count)
			for _, form := range []struct {
				name     string
				manifest *rootpublication.DependencyManifestV1
			}{{"streaming", streaming}, {"cached", cached}} {
				t.Run(form.name, func(t *testing.T) {
					primaryBorrowedScratchCheckMaterializer(t, primaryBorrowedScratchDB(t), form.manifest)
				})
			}
		})
	}
}

func TestPrimaryManifestBorrowedScratchIndependentOwners(t *testing.T) {
	streaming, cached := primaryBorrowedScratchManifests(t, 128)
	dbs := [2]*DB{primaryBorrowedScratchDB(t), primaryBorrowedScratchDB(t)}
	manifests := [2]*rootpublication.DependencyManifestV1{streaming, cached}
	var sinks [2]*primaryManifestImagesV5
	var owners [2]*retainedalloc.Owner
	var baselines, charges [2]uint64
	// Register before the first admission/allocation. Every live original span
	// is cleared and refunded even when the second owner refuses or encoding fails.
	defer func() {
		for i, sink := range sinks {
			if sink != nil {
				clear(sink.images)
				sink.images = nil
			}
			if charges[i] != 0 {
				owners[i].RemovePending(charges[i])
				if got := owners[i].Bytes(); got != baselines[i] {
					t.Errorf("owner %d span refund=%d want=%d", i, got, baselines[i])
				}
			}
		}
	}()
	for i, d := range dbs {
		primaryBorrowedScratchDrain(t, d.idx.Load().primary)
		owners[i] = d.idx.Load().primary.MetadataOwner()
		baselines[i] = owners[i].Bytes()
		imageBytes := uint64(manifests[i].PageCount()) * page.PageSize
		charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryManifestImagesV5{}))) + retainedalloc.AllocationCharge(imageBytes)
		if err := owners[i].AddPending(charge); err != nil {
			t.Fatal("independent owner admission", err)
		}
		charges[i] = charge
		sinks[i] = &primaryManifestImagesV5{first: 100, images: make([]byte, int(imageBytes))}
		if got := owners[i].Bytes(); got != baselines[i]+charge {
			t.Fatalf("owner %d admitted=%d want=%d", i, got, baselines[i]+charge)
		}
	}
	if owners[0] == owners[1] {
		t.Fatal("fixture did not create independent arena owners")
	}
	startA, startB := uintptr(unsafe.Pointer(&sinks[0].images[0])), uintptr(unsafe.Pointer(&sinks[1].images[0]))
	endA, endB := startA+uintptr(len(sinks[0].images)), startB+uintptr(len(sinks[1].images))
	if startA < endB && startB < endA {
		t.Fatal("simultaneously live owner spans overlap")
	}
	// This is a direct sink/encoder simultaneous-lifetime fixture. It does not
	// observe hidden materializer locals or assert concurrent scheduling. Both
	// fresh spans remain live through the comparisons and final clear/refunds.
	var beforeA []byte
	for i, sink := range sinks {
		wantStore := freelist.NewMemoryPageStoreV1()
		want, err := manifests[i].Materialize(sink.first, wantStore)
		if err != nil {
			t.Fatal(err)
		}
		scratch := sink.images[len(sink.images)-page.PageSize:]
		got, err := manifests[i].MaterializeWithScratchV1(sink.first, sink, scratch)
		if err != nil || got != want {
			t.Fatal("borrowed span reference changed", err)
		}
		for index := uint32(0); index < got.PageCount; index++ {
			image := sink.images[int(index)*page.PageSize : (int(index)+1)*page.PageSize]
			if !bytes.Equal(image, wantStore.Pages[sink.first+uint64(index)]) {
				t.Fatalf("owner %d canonical page %d changed", i, index)
			}
		}
		if i == 0 {
			beforeA = bytes.Clone(sink.images)
		}
	}
	if !bytes.Equal(sinks[0].images, beforeA) {
		t.Fatal("encoding B changed simultaneously live A")
	}
	for i, sink := range sinks {
		clear(sink.images)
		sink.images = nil
		owners[i].RemovePending(charges[i])
		charges[i] = 0
		if got := owners[i].Bytes(); got != baselines[i] {
			t.Fatalf("owner %d span disposal=%d want=%d", i, got, baselines[i])
		}
	}
	// Supplement the limited direct-span fixture with the actual materializer on
	// both independent physical owners, including bank copies and once disposal.
	for i, d := range dbs {
		primaryBorrowedScratchCheckMaterializer(t, d, manifests[i])
	}
}
