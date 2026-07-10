package freelist

import (
	"bytes"
	"errors"
	"path/filepath"
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
	if data := mustAllocatorPage(t, p, a.Head()); page.VerifyChecksumNonMutating(data) {
		t.Fatal("checksum was updated before the free hook released")
	} else if got := node.NewNode(data).Count(); got != 1 {
		t.Fatalf("count while free hook is paused = %d, want written prefix count 1", got)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("FreeMany: %v", err)
	}
	if data := mustAllocatorPage(t, p, a.Head()); !page.VerifyChecksumNonMutating(data) {
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

func mustAllocatorPage(t *testing.T, p *pager.Pager, id uint64) []byte {
	t.Helper()
	data, err := p.Get(id)
	if err != nil {
		t.Fatalf("Get(%d): %v", id, err)
	}
	return data
}
