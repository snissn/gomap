package freelist

import (
	"bytes"
	"errors"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func newAllocatorForFreeManyTest(t *testing.T, pages int) (*Allocator, *pager.Pager) {
	t.Helper()
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := p.Alloc(pages); err != nil {
		_ = p.Close()
		t.Fatalf("Alloc(%d): %v", pages, err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return New(p, 0), p
}

func TestAllocator_FreeMany_FillsHeadAndChains(t *testing.T) {
	a, p := newAllocatorForFreeManyTest(t, page.MaxFreeIDs+8)

	seed := make([]uint64, page.MaxFreeIDs-1)
	for i := range seed {
		seed[i] = uint64(i + 1)
	}
	if err := a.FreeMany(seed); err != nil {
		t.Fatalf("seed FreeMany: %v", err)
	}

	ids := []uint64{uint64(page.MaxFreeIDs - 1), uint64(page.MaxFreeIDs), uint64(page.MaxFreeIDs + 1), uint64(page.MaxFreeIDs + 2)}
	checksumUpdates := 0
	TestHookFreeManyBeforeChecksum = func() { checksumUpdates++ }
	t.Cleanup(func() { TestHookFreeManyBeforeChecksum = nil })
	if err := a.FreeMany(ids); err != nil {
		t.Fatalf("FreeMany: %v", err)
	}
	if checksumUpdates != 2 {
		t.Fatalf("FreeMany checksum updates = %d, want one per touched page (2)", checksumUpdates)
	}

	head := a.Head()
	if head != ids[2] {
		t.Fatalf("head = %d, want chained head %d", head, ids[2])
	}
	headData, err := p.Get(head)
	if err != nil {
		t.Fatalf("Get(head): %v", err)
	}
	headNode := node.NewNode(headData)
	if !headNode.VerifyChecksum() {
		t.Fatal("chained head checksum is invalid")
	}
	if headNode.Count() != 1 || freelistIDAt(headData, 0) != ids[3] {
		t.Fatalf("chained head entries = count %d id %d, want count 1 id %d", headNode.Count(), freelistIDAt(headData, 0), ids[3])
	}
	if next := freelistNextPageID(headData); next != seed[0] {
		t.Fatalf("chained head next = %d, want %d", next, seed[0])
	}

	oldData, err := p.Get(seed[0])
	if err != nil {
		t.Fatalf("Get(old head): %v", err)
	}
	oldNode := node.NewNode(oldData)
	if oldNode.Count() != page.MaxFreeIDs {
		t.Fatalf("old head count = %d, want %d", oldNode.Count(), page.MaxFreeIDs)
	}
	if !oldNode.VerifyChecksum() {
		t.Fatal("filled head checksum is invalid")
	}
}

func TestAllocator_FreeMany_FullHeadGetsFinalChecksumBeforeChaining(t *testing.T) {
	a, _ := newAllocatorForFreeManyTest(t, page.MaxFreeIDs+3)
	seed := make([]uint64, page.MaxFreeIDs+1)
	for i := range seed {
		seed[i] = uint64(i + 1)
	}
	if err := a.FreeMany(seed); err != nil {
		t.Fatalf("seed FreeMany: %v", err)
	}

	checksumUpdates := 0
	TestHookFreeManyBeforeChecksum = func() { checksumUpdates++ }
	t.Cleanup(func() { TestHookFreeManyBeforeChecksum = nil })

	if err := a.FreeMany([]uint64{uint64(page.MaxFreeIDs + 2)}); err != nil {
		t.Fatalf("FreeMany: %v", err)
	}
	if checksumUpdates != 2 {
		t.Fatalf("checksum updates = %d, want full transition head and new head (2)", checksumUpdates)
	}
}

func TestAllocator_FreeMany_RejectsPageZeroBeforeMutation(t *testing.T) {
	a, p := newAllocatorForFreeManyTest(t, 6)
	if err := a.Free(2); err != nil {
		t.Fatalf("Free(2): %v", err)
	}
	beforeHead := a.Head()
	before := append([]byte(nil), mustAllocatorPage(t, p, beforeHead)...)
	beforeCounters := a.Counters()

	err := a.FreeMany([]uint64{3, 0, 4})
	if err == nil || !errors.Is(err, errCannotFreePageZero) {
		t.Fatalf("FreeMany error = %v, want page-0 rejection", err)
	}
	if got := a.Head(); got != beforeHead {
		t.Fatalf("head changed: got %d want %d", got, beforeHead)
	}
	after := mustAllocatorPage(t, p, beforeHead)
	if !bytes.Equal(after, before) {
		t.Fatal("freelist head changed after page-0 rejection")
	}
	if got := a.Counters(); got != beforeCounters {
		t.Fatalf("counters changed after page-0 rejection: got %+v want %+v", got, beforeCounters)
	}
}

func TestAllocator_FreeMany_DuplicatesMatchFreeLoop(t *testing.T) {
	a, _ := newAllocatorForFreeManyTest(t, 8)
	ids := []uint64{1, 1, 2}
	if err := a.FreeMany(ids); err != nil {
		t.Fatalf("FreeMany: %v", err)
	}

	got, err := a.AllocMany(len(ids), 0)
	if err != nil {
		t.Fatalf("AllocMany: %v", err)
	}
	want := []uint64{2, 1, 1}
	if len(got) != len(want) {
		t.Fatalf("AllocMany returned %d ids, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("AllocMany[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestAllocator_FreeMany_HookRunsBeforeChecksum(t *testing.T) {
	a, p := newAllocatorForFreeManyTest(t, 6)
	if err := a.Free(2); err != nil {
		t.Fatalf("Free(2): %v", err)
	}
	head := a.Head()

	var once sync.Once
	reached := make(chan struct{}, 1)
	release := make(chan struct{})
	TestHookFreeBeforeChecksum = func() {
		once.Do(func() {
			reached <- struct{}{}
			<-release
		})
	}
	t.Cleanup(func() { TestHookFreeBeforeChecksum = nil })

	done := make(chan error, 1)
	go func() { done <- a.FreeMany([]uint64{1, 3}) }()
	<-reached
	if data := mustAllocatorPage(t, p, head); page.VerifyChecksumNonMutating(data) {
		t.Fatal("checksum was updated before the free hook released")
	} else if got := node.NewNode(data).Count(); got != 1 {
		t.Fatalf("count while free hook is paused = %d, want written prefix count 1", got)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("FreeMany: %v", err)
	}
	if data := mustAllocatorPage(t, p, head); !page.VerifyChecksumNonMutating(data) {
		t.Fatal("checksum was not updated after FreeMany completed")
	}
}

func TestAllocator_FreeMany_RejectsCorruptHead(t *testing.T) {
	a, p := newAllocatorForFreeManyTest(t, 5)
	if err := a.Free(2); err != nil {
		t.Fatalf("Free(2): %v", err)
	}
	data, err := p.GetForWrite(a.Head())
	if err != nil {
		t.Fatalf("GetForWrite: %v", err)
	}
	data[page.PageHeaderSize] ^= 0xff

	if err := a.FreeMany([]uint64{1, 3}); err == nil {
		t.Fatal("FreeMany succeeded with corrupt freelist head")
	}
}

func TestAllocator_FreeMany_ErrorReportsCommittedPrefix(t *testing.T) {
	a, _ := newAllocatorForFreeManyTest(t, page.MaxFreeIDs+2)
	ids := make([]uint64, page.MaxFreeIDs+2)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}

	err := a.FreeMany(ids)
	if err == nil {
		t.Fatal("FreeMany succeeded with an out-of-bounds transition head")
	}
	var batchErr *FreeManyError
	if !errors.As(err, &batchErr) {
		t.Fatalf("FreeMany error type = %T, want *FreeManyError", err)
	}
	if batchErr.Processed != page.MaxFreeIDs+1 {
		t.Fatalf("processed prefix = %d, want %d", batchErr.Processed, page.MaxFreeIDs+1)
	}
	if !errors.Is(err, pager.ErrPageOutOfBounds) {
		t.Fatalf("FreeMany error = %v, want pager.ErrPageOutOfBounds", err)
	}
	stats := a.Counters()
	if got, want := stats.ReclaimablePages(), uint64(batchErr.Processed); got != want {
		t.Fatalf("reclaimable pages = %d, want committed prefix %d", got, want)
	}
}

func TestAllocator_RegionBiasedAllocMany_BatchesNearbyUnverified(t *testing.T) {
	a, p := newAllocatorForFreeManyTest(t, 32)
	a.SetFreelistRegion(4, 1)
	if err := a.FreeMany([]uint64{1, 2, 9, 10, 29}); err != nil {
		t.Fatalf("FreeMany: %v", err)
	}
	p.MarkVerified(9)
	p.MarkVerified(10)
	a.mu.Lock()
	a.lastAlloc = 9
	a.mu.Unlock()
	before := a.Counters()

	got, err := a.AllocMany(2, 0)
	if err != nil {
		t.Fatalf("AllocMany: %v", err)
	}
	want := map[uint64]bool{9: true, 10: true}
	for _, id := range got {
		if !want[id] {
			t.Fatalf("region allocation returned %d, want only nearby pages", id)
		}
		delete(want, id)
	}
	if len(want) != 0 {
		t.Fatalf("region allocation missed nearby pages: %v", want)
	}
	if p.IsVerified(9) || p.IsVerified(10) {
		t.Fatal("reused region pages remained verified")
	}
	a.mu.Lock()
	lastAlloc := a.lastAlloc
	a.mu.Unlock()
	if lastAlloc != got[len(got)-1] {
		t.Fatalf("lastAlloc = %d, want final region allocation %d", lastAlloc, got[len(got)-1])
	}
	after := a.Counters()
	if after.AllocPages != before.AllocPages+2 || after.ReuseAllocPages != before.ReuseAllocPages+2 || after.FreeIDs != before.FreeIDs-2 {
		t.Fatalf("unexpected counters after region AllocMany: before=%+v after=%+v", before, after)
	}
}

func TestAllocator_RegionBiasedAllocMany_RecycledHeadDoesNotConsumeFreeIDStat(t *testing.T) {
	freed := make([]uint64, page.MaxFreeIDs+3)
	for i := range freed {
		freed[i] = uint64(i + 1)
	}
	a, p := newAllocatorForFreeManyTest(t, len(freed)+1)
	a.SetFreelistRegion(4, 1)
	if err := a.FreeMany(freed); err != nil {
		t.Fatalf("FreeMany: %v", err)
	}

	emptyHead := a.Head()
	headData := mustAllocatorPage(t, p, emptyHead)
	if got := node.NewNode(headData).Count(); got != 1 {
		t.Fatalf("newest freelist head count = %d, want 1 before drain", got)
	}
	if got := freelistNextPageID(headData); got == 0 {
		t.Fatal("expected an older freelist head after newest head")
	}

	drained, err := a.AllocMany(1, freed[len(freed)-1])
	if err != nil {
		t.Fatalf("drain AllocMany: %v", err)
	}
	if len(drained) != 1 || drained[0] != freed[len(freed)-1] {
		t.Fatalf("drain AllocMany = %v, want [%d]", drained, freed[len(freed)-1])
	}
	if got := node.NewNode(mustAllocatorPage(t, p, emptyHead)).Count(); got != 0 {
		t.Fatalf("newest freelist head count after drain = %d, want 0", got)
	}

	before := a.Counters()
	if before.Pages != 2 || before.FreeIDs != page.MaxFreeIDs {
		t.Fatalf("counters before recycling empty head = %+v, want 2 heads and %d body IDs", before, page.MaxFreeIDs)
	}
	got, err := a.AllocMany(2, emptyHead)
	if err != nil {
		t.Fatalf("recycle AllocMany: %v", err)
	}
	if len(got) != 2 || got[0] != emptyHead {
		t.Fatalf("recycle AllocMany = %v, want empty head %d followed by one body ID", got, emptyHead)
	}

	after := a.Counters()
	if after.Pages != before.Pages-1 {
		t.Fatalf("freelist pages after recycling empty head = %d, want %d", after.Pages, before.Pages-1)
	}
	if after.FreeIDs != before.FreeIDs-1 {
		t.Fatalf("free body IDs after recycling head and allocating one body ID = %d, want %d", after.FreeIDs, before.FreeIDs-1)
	}
	if after.AllocPages != before.AllocPages+2 || after.ReuseAllocPages != before.ReuseAllocPages+2 {
		t.Fatalf("allocation counters after recycling empty head: before=%+v after=%+v", before, after)
	}
}

func TestAllocator_RegionBiasedAllocMany_DrainsAndRecyclesHeadWithOneVerification(t *testing.T) {
	freed := make([]uint64, page.MaxFreeIDs+3)
	for i := range freed {
		freed[i] = uint64(i + 1)
	}
	a, p := newAllocatorForFreeManyTest(t, len(freed)+1)
	a.SetFreelistRegion(10, 1)
	if err := a.FreeMany(freed); err != nil {
		t.Fatalf("FreeMany: %v", err)
	}

	newestHead := a.Head()
	newestData := mustAllocatorPage(t, p, newestHead)
	if got := node.NewNode(newestData).Count(); got != 1 {
		t.Fatalf("newest freelist head count = %d, want 1", got)
	}

	got, err := a.AllocMany(3, freed[len(freed)-1])
	if err != nil {
		t.Fatalf("AllocMany: %v", err)
	}
	if len(got) != 3 || got[1] != newestHead {
		t.Fatalf("AllocMany = %v, want body ID, recycled head %d, then older body ID", got, newestHead)
	}
}

func TestAllocator_RegionBiasedAllocMany_MatchesRepeatedAllocWithMovingHint(t *testing.T) {
	newFixture := func(t *testing.T) *Allocator {
		t.Helper()
		a, _ := newAllocatorForFreeManyTest(t, 130)
		a.SetFreelistRegion(10, 1)
		if err := a.FreeMany([]uint64{1, 90, 95, 38, 115, 105}); err != nil {
			t.Fatalf("FreeMany: %v", err)
		}
		return a
	}

	batched := newFixture(t)
	got, err := batched.AllocMany(5, 129)
	if err != nil {
		t.Fatalf("AllocMany: %v", err)
	}

	repeated := newFixture(t)
	want := make([]uint64, 0, len(got))
	hint := uint64(129)
	for range got {
		id, err := repeated.Alloc(hint)
		if err != nil {
			t.Fatalf("Alloc(%d): %v", hint, err)
		}
		want = append(want, id)
		hint = id
	}
	if !slices.Equal(got, want) {
		t.Fatalf("AllocMany order = %v, repeated Alloc order = %v", got, want)
	}
}

func TestAllocator_RegionBiasedAllocMany_IndexedPathMatchesRepeatedAlloc(t *testing.T) {
	freed := []uint64{1, 90, 95, 38, 115, 105, 12, 29, 81, 99, 130, 111, 72}
	newFixture := func(t *testing.T) *Allocator {
		t.Helper()
		a, _ := newAllocatorForFreeManyTest(t, 140)
		a.SetFreelistRegion(10, 1)
		if err := a.FreeMany(freed); err != nil {
			t.Fatalf("FreeMany: %v", err)
		}
		return a
	}

	batched := newFixture(t)
	got, err := batched.AllocMany(10, 129)
	if err != nil {
		t.Fatalf("AllocMany: %v", err)
	}

	repeated := newFixture(t)
	want := make([]uint64, 0, len(got))
	hint := uint64(129)
	for range got {
		id, err := repeated.Alloc(hint)
		if err != nil {
			t.Fatalf("Alloc(%d): %v", hint, err)
		}
		want = append(want, id)
		hint = id
	}
	if !slices.Equal(got, want) {
		t.Fatalf("indexed AllocMany order = %v, repeated Alloc order = %v", got, want)
	}
}

func mustAllocatorPage(t *testing.T, p *pager.Pager, id uint64) []byte {
	t.Helper()
	data, err := p.Get(id)
	if err != nil {
		t.Fatalf("Get(%d): %v", id, err)
	}
	return data
}
