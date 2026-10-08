package db

import (
	"errors"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/allocclass"
	"github.com/snissn/gomap/TreeDB/internal/allocatorownership"
	"github.com/snissn/gomap/TreeDB/internal/residentcredit"
	"github.com/snissn/gomap/TreeDB/lifecycle"
	"github.com/snissn/gomap/TreeDB/pager"
)

func expectedIndexConstructorClassesV1(t *testing.T, writer bool) uint64 {
	t.Helper()
	// Independent per-object oracle, never rounding the aggregate raw bytes.
	var total uint64
	for _, raw := range []uint64{
		uint64(unsafe.Sizeof(indexGen{})),
		uint64(unsafe.Sizeof(lifecycle.ReaderRegistry{})),
		uint64(unsafe.Sizeof(lifecycle.Graveyard{})),
		lifecycle.GraveyardInitialBatchBackingBytes(),
	} {
		class, err := allocclass.ClassBytes(raw, true)
		if err != nil { class = raw }
		if class > ^uint64(0)-total { t.Fatal("test class sum overflow") }
		total += class
	}
	if writer {
		class, err := allocclass.ClassBytes(uint64(unsafe.Sizeof(allocatorownership.ManagedWriter{})), false)
		if err != nil { class = uint64(unsafe.Sizeof(allocatorownership.ManagedWriter{})) }
		if class > ^uint64(0)-total { t.Fatal("test writer class sum overflow") }
		total += class
	}
	return total
}

func openIndexConstructorPagerV1(t *testing.T, owner *residentcredit.Owner, name string) *pager.Pager {
	t.Helper()
	p, err := pager.OpenWithOptions(filepath.Join(t.TempDir(), name), 64*1024, pager.OpenOptions{ResidentOwner: owner})
	if err != nil { t.Fatal(err) }
	t.Cleanup(func() { if err := p.Close(); err != nil { t.Error(err) } })
	return p
}

func TestIndexConstructorClosureRefusesHeaderOnlyHeadroomBeforeWriterBind(t *testing.T) {
	owner, err := residentcredit.NewOrdinary(128 << 20)
	if err != nil { t.Fatal(err) }
	t.Cleanup(owner.Close)
	p := openIndexConstructorPagerV1(t, owner, "refused.db")
	original, err := p.RetainConstructorLifetime()
	if err != nil || original == nil { t.Fatalf("constructor scope: %p %v", original, err) }
	defer original.ReleaseStableMetadata()
	floor, err := owner.NewScope()
	if err != nil { t.Fatal(err) }
	defer floor.ReleaseStableMetadata()
	header, err := allocclass.ClassBytes(uint64(unsafe.Sizeof(indexGen{})), true)
	if err != nil { t.Fatal(err) }
	if expectedIndexConstructorClassesV1(t, true) <= header { t.Fatal("fixture lacks child births") }
	stats := owner.Stats()
	if err := floor.ReserveStableMetadata(stats.Limit-stats.Live-header); err != nil { t.Fatal(err) }
	allocator := freelist.New(p, 0)
	beforeScope, beforeOwner := original.Stats(), owner.Stats()
	gen, err := newConstructorIndexGen(7, p, allocator, nil)
	if gen != nil || !errors.Is(err, residentcredit.ErrLimit) { t.Fatalf("header-only admission: %p %v", gen, err) }
	if original.Stats() != beforeScope || owner.Stats() != beforeOwner { t.Fatal("refusal retained or partially debited constructor") }
	// A real bind succeeds only if the refused constructor never bound a child.
	authority := allocatorownership.NewManagedWriter()
	if err := allocator.BindManagedIndexWriterV1(authority); err != nil { t.Fatalf("writer bound before full admission: %v", err) }
	allocator.CloseCOWOwnersAfterShutdownV1()
	allocator.DetachManagedIndexWriterV1(authority)
}

func TestIndexConstructorClosureChargesIndependentClasses(t *testing.T) {
	for _, writer := range []bool{false, true} {
		name := "reader-only"
		if writer { name = "managed-writer" }
		t.Run(name, func(t *testing.T) {
			owner, err := residentcredit.NewOrdinary(128 << 20)
			if err != nil { t.Fatal(err) }
			t.Cleanup(owner.Close)
			p := openIndexConstructorPagerV1(t, owner, "exact.db")
			original, err := p.RetainConstructorLifetime()
			if err != nil || original == nil { t.Fatalf("constructor scope: %p %v", original, err) }
			defer original.ReleaseStableMetadata()
			floor, err := owner.NewScope()
			if err != nil { t.Fatal(err) }
			defer floor.ReleaseStableMetadata()
			want := expectedIndexConstructorClassesV1(t, writer)
			stats := owner.Stats()
			if err := floor.ReserveStableMetadata(stats.Limit-stats.Live-want); err != nil { t.Fatal(err) }
			var allocator *freelist.Allocator
			if writer { allocator = freelist.New(p, 0) }
			beforeScope, beforeOwner := original.Stats(), owner.Stats()
			gen, err := newConstructorIndexGen(8, p, allocator, nil)
			if err != nil { t.Fatal(err) }
			t.Cleanup(func() { if err := gen.close(); err != nil { t.Error(err) } })
			if gen.creator != original || gen.registry == nil || gen.graveyard == nil || (gen.writerAuthority != nil) != writer { t.Fatal("actual constructor closure changed") }
			after := original.Stats()
			if after.Bytes != beforeScope.Bytes+want || after.Retained != beforeScope.Retained+1 { t.Fatalf("scope debit: before=%+v after=%+v classes=%d", beforeScope, after, want) }
			got := owner.Stats()
			if got.Live != got.Limit || got.Births != beforeOwner.Births+want { t.Fatalf("exact class headroom: before=%+v after=%+v classes=%d", beforeOwner, got, want) }
			if !writer && gen.allocator != nil { t.Fatal("nil allocator constructed a writer") }
			if err := p.RequireOwnedCapacity(owner); !errors.Is(err, pager.ErrOwnedCoverage) { t.Fatalf("constructor census enabled finite loan: %v", err) }
		})
	}
}

func TestIndexConstructorClosureRetainsAcrossReaderCutoverAndCloseRetry(t *testing.T) {
	owner, err := residentcredit.NewOrdinary(128 << 20)
	if err != nil { t.Fatal(err) }
	t.Cleanup(owner.Close)
	oldPager := openIndexConstructorPagerV1(t, owner, "old.db")
	old, err := newConstructorIndexGen(1, oldPager, nil, nil)
	if err != nil { t.Fatal(err) }
	previous := testIndexClosePager
	t.Cleanup(func() { testIndexClosePager = previous; if err := old.close(); err != nil { t.Error(err) } })
	creator := old.creator
	charged := creator.Stats()
	reader := old.registry.Register(1)
	old.acquire()
	if old.release() != 1 { t.Fatal("cutover lost held reader") }
	nextPager := openIndexConstructorPagerV1(t, owner, "replacement.db")
	next, err := newConstructorIndexGen(2, nextPager, nil, nil)
	if err != nil { t.Fatal(err) }
	t.Cleanup(func() { if err := next.close(); err != nil { t.Error(err) } })
	if creator.Stats() != charged || !creator.SameOwner(next.creator) || creator == next.creator { t.Fatal("cutover released old constructor or reattributed its scope") }
	old.registry.Unregister(reader)
	if old.release() != 0 { t.Fatal("actual reader did not drain") }
	fault := errors.New("old generation physical close fault")
	testIndexClosePager = func(p *pager.Pager) error { if p == oldPager { return fault }; return nil }
	if err := old.close(); !errors.Is(err, fault) { t.Fatalf("actual close failure: %v", err) }
	if old.creator != creator || creator.Stats() != charged || old.handlesClosed.Load() { t.Fatal("failed Close released full constructor charge/holder") }
	testIndexClosePager = previous
	if err := old.close(); err != nil { t.Fatal(err) }
	if old.creator != nil || !creator.Stats().Closed || !old.handlesClosed.Load() { t.Fatal("successful actual retry failed last constructor edge") }
	if next.creator == nil || next.creator.Stats().Closed { t.Fatal("old retry consumed replacement creator") }
	if err := next.close(); err != nil { t.Fatal(err) }
	owner.Close()
	if owner.Stats().Live != 0 { t.Fatalf("actual joined generations leaked: %+v", owner.Stats()) }
}

func TestIndexConstructorClosureRetainsPreparedWriterAfterPhysicalClose(t *testing.T) {
	owner, err := residentcredit.NewOrdinary(128 << 20)
	if err != nil { t.Fatal(err) }
	t.Cleanup(owner.Close)
	p := openIndexConstructorPagerV1(t, owner, "prepared.db")
	if _, err := p.Alloc(4); err != nil { t.Fatal(err) }
	allocator := freelist.New(p, 0)
	gen, err := newConstructorIndexGen(3, p, allocator, nil)
	if err != nil { t.Fatal(err) }
	t.Cleanup(func() { allocator.CloseCOWOwnersAfterShutdownV1(); gen.detachAllocatorWriterV1(true); if err := gen.close(); err != nil { t.Error(err) } })
	if err := allocator.EnableNewCOWGenerationV1(1, 4, nil); err != nil { t.Fatal(err) }
	var id freelist.CandidateIDV1
	id[0] = 93
	prepared, err := allocator.PrepareOwnedCOWCandidateRetiringWithLimitsV1(2, 1, id, freelist.ReuseCapability{}, nil, 0, freelist.NewCandidatePageSinkV1(), nil)
	if err != nil { t.Fatal(err) }
	creator, authority := gen.creator, gen.writerAuthority
	charged := creator.Stats().Bytes
	if err := gen.closeForShutdownV1(); err != nil { t.Fatal(err) }
	if !gen.handlesClosed.Load() || gen.creator != creator || gen.writerAuthority != authority || creator.Stats().Closed || creator.Stats().Bytes != charged { t.Fatal("physical Close discarded actual prepared writer/constructor charge") }
	if err := allocator.AbortCOWCandidateV1(prepared); err != nil { t.Fatal(err) }
	if err := prepared.ClearTerminalBackingV1(); err != nil { t.Fatal(err) }
	if err := gen.close(); err != nil { t.Fatal(err) }
	if gen.creator != nil || gen.writerAuthority != nil || !creator.Stats().Closed { t.Fatal("writer terminal failed exact constructor discharge") }
	owner.Close()
	if owner.Stats().Live != 0 { t.Fatalf("prepared writer constructor leaked: %+v", owner.Stats()) }
}
