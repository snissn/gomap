package memtable

import (
	"errors"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/node"
)

func TestCOWNodeLayoutChargeCoversActualAllocatorClass(t *testing.T) {
	// Equal conservative counters do not hide the actual 48->64 class cost.
	if unsafe.Sizeof(uintptr(0)) != 8 {
		t.Skip("64-bit layout witness")
	}
	if actual := unsafe.Sizeof(cowNodeLayout{}); actual != 56 {
		t.Fatalf("node header=%d", actual)
	}
	if before, after := cowAllocation(48), cowAllocation(56); before != 64 || after != 64 {
		t.Fatalf("old/new conservative charge %d/%d", before, after)
	}
}

func TestCOWCapturedForkAdmissionCoversAllocatedHeaders(t *testing.T) {
	budget, writer := cowTestWriter(t, DefaultCOWLimits())
	root := cowTestPublish(t, writer, cowTestEntries(1024))
	cowTestClose(writer)
	writer = nil
	cowTestIsolateAllocationWitness(t)
	initial := budget.Stats().HistoryBytes
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	fork, err := ForkCOWWriter(root)
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	charged, allocated := budget.Stats().HistoryBytes-initial, after.TotalAlloc-before.TotalAlloc
	if charged < allocated {
		t.Fatalf("fork charge=%d allocated=%d", charged, allocated)
	}
	if reflect.ValueOf(fork.private).Elem().FieldByName("root").Pointer() != reflect.ValueOf(root.tree).Elem().FieldByName("root").Pointer() {
		t.Fatal("fork copied original nodes")
	}
	retirement := fork.Close()
	fork = nil
	retirement.Drain()
	if got := budget.Stats().HistoryBytes; got != initial+charged {
		t.Fatal("live source lost generation history", got)
	}
	budget.Close()
	retirement = root.Release()
	root = nil
	retirement.Drain()
	if budget.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats())
	}
}

func TestCOWCapturedRootForksCancelAndPublishIndependently(t *testing.T) {
	budget, original := cowTestWriter(t, DefaultCOWLimits())
	old := cowTestPublish(t, original, cowTestEntries(4096))
	iso := reflect.ValueOf(old.tree).Elem().FieldByName("isoid").Uint()
	cowTestClose(original)
	if budget.Stats().RetiredBytes == 0 {
		t.Fatal("closed original generation not retired")
	}
	left, err := ForkCOWWriter(old)
	if err != nil {
		t.Fatal(err)
	}
	right, err := ForkCOWWriter(old)
	if err != nil {
		t.Fatal(err)
	}
	if budget.Stats().RetiredBytes != 0 {
		t.Fatal("reactivated source still retired")
	}
	cancelled, err := left.Prepare([]COWMutation{{Key: []byte("00000000"), Value: []byte("cancelled")}}, COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := right.Prepare([]COWMutation{{Key: []byte("00000000"), Value: []byte("accepted")}}, COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	retirement := cancelled.Cancel()
	cancelled = nil
	retirement.Drain()
	cowTestClose(left)
	left = nil
	if budget.Stats().RetiredBytes != 0 {
		t.Fatal("one closed fork retired another active writer")
	}
	next := accepted.Publish()
	accepted = nil
	cowTestClose(right)
	right = nil
	if got, _ := old.Get([]byte("00000000")); got.Value != "value" {
		t.Fatal("old root changed", got)
	}
	if got, _ := next.Get([]byte("00000000")); got.Value != "accepted" {
		t.Fatal("fork did not publish", got)
	}
	if got := reflect.ValueOf(old.tree).Elem().FieldByName("isoid").Uint(); got != iso {
		t.Fatal("fork mutated immutable header")
	}
	if old.generation != next.generation {
		t.Fatal("fork introduced another generation")
	}
	if budget.Stats().ReservedBytes != 0 {
		t.Fatal("cancel/publish left reservation")
	}
	budget.Close()
	cowTestRelease(old)
	old = nil
	if budget.Stats().HistoryBytes == 0 {
		t.Fatal("new root lost original shared node/resource charge")
	}
	cowTestRelease(next)
	next = nil
	if stats := budget.Stats(); stats.TotalBytes != 0 {
		t.Fatalf("last owner did not refund %+v", stats)
	}
}

func TestCOWCapturedForkKeepsOriginalResourceAndOldViewThroughClose(t *testing.T) {
	budget, writer := cowTestWriter(t, DefaultCOWLimits())
	prepared, err := writer.Prepare([]COWMutation{{Key: []byte("key"), Value: []byte("old"), Flags: node.FlagInline}}, COWPrepareOptions{ResourceSlots: 1, ResourceBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	var released atomic.Int64
	if err := prepared.AttachResources([]COWResourceID{{Kind: 1, ID: 42}}, func() { released.Add(1) }); err != nil {
		t.Fatal(err)
	}
	old := prepared.Publish()
	view, err := old.Acquire(0)
	if err != nil {
		t.Fatal(err)
	}
	fork, err := ForkCOWWriter(old)
	if err != nil {
		t.Fatal(err)
	}
	next := cowTestPublish(t, fork, []COWMutation{{Key: []byte("key"), Value: []byte("next")}})
	cowTestClose(writer)
	cowTestClose(fork)
	cowTestRelease(old)
	old = nil
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			got, ok, err := view.Get([]byte("key"))
			if err != nil || !ok || got.Value != "old" {
				t.Errorf("old view %+v %v %v", got, ok, err)
				return
			}
		}
	}()
	budget.Close()
	cowTestRelease(next)
	next = nil
	wg.Wait()
	if released.Load() != 0 || budget.Stats().HistoryBytes == 0 {
		t.Fatal("live old view lost original owners")
	}
	view.Close()
	view = nil
	if released.Load() != 1 || budget.Stats().TotalBytes != 0 {
		t.Fatalf("resource=%d stats=%+v", released.Load(), budget.Stats())
	}
}

func TestCOWCapturedForkRefusesFrozenClosedAndResourceRaces(t *testing.T) {
	budget, writer := cowTestWriter(t, DefaultCOWLimits())
	root := cowTestPublish(t, writer, cowTestEntries(1))
	fork, err := ForkCOWWriter(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Freeze(); !errors.Is(err, ErrCOWPending) {
		t.Fatal("freeze ignored active fork", err)
	}
	if _, err := fork.Prepare(nil, COWPrepareOptions{ResourceSlots: 1}); !errors.Is(err, ErrCOWPending) {
		t.Fatal("private resource ledger raced", err)
	}
	cowTestClose(fork)
	if err := writer.Freeze(); err != nil {
		t.Fatal(err)
	}
	before := budget.Stats()
	if _, err := ForkCOWWriter(root); !errors.Is(err, ErrCOWCapacity) {
		t.Fatal("forked frozen source", err)
	}
	if after := budget.Stats(); after != before {
		t.Fatal("refused fork changed charge", before, after)
	}
	budget.Close()
	if _, err := ForkCOWWriter(root); !errors.Is(err, ErrCOWClosed) {
		t.Fatal("forked closed budget", err)
	}
	cowTestClose(writer)
	cowTestRelease(root)
	if budget.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats())
	}
}

func TestCOWCapturedForkGenerationCapIncludesOriginalPendingReservations(t *testing.T) {
	budget, writer := cowTestWriter(t, DefaultCOWLimits())
	root := cowTestPublish(t, writer, cowTestEntries(1))
	left, err := ForkCOWWriter(root)
	if err != nil {
		t.Fatal(err)
	}
	right, err := ForkCOWWriter(root)
	if err != nil {
		t.Fatal(err)
	}
	entries := []COWMutation{{Key: []byte("00000000"), Value: make([]byte, 8192)}}
	charge, err := left.estimate(entries, COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	g := writer.generation
	budget.mu.Lock()
	// Each prepare fits against installed history in isolation, but both
	// cannot fit. Existing original reservation is authoritative before Publish.
	budget.limits.MaxGenerationBytes = g.history + charge.Total() + charge.History()/2
	budget.mu.Unlock()
	prepared, err := left.Prepare(entries, COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	before := budget.Stats()
	if _, err = right.Prepare(entries, COWPrepareOptions{}); !errors.Is(err, ErrCOWCapacity) {
		t.Fatal("fork ignored original pending generation reservation", err)
	}
	if budget.Stats() != before {
		t.Fatal("refused fork preparation changed accounting")
	}
	budget.mu.Lock()
	budget.limits.MaxGenerationBytes = g.history + g.pending
	budget.mu.Unlock()
	if _, err = ForkCOWWriter(root); !errors.Is(err, ErrCOWCapacity) {
		t.Fatal("header admission ignored original pending reservation", err)
	}
	if budget.Stats() != before {
		t.Fatal("refused header changed accounting")
	}
	next := prepared.Publish()
	prepared = nil
	if g.history > budget.limits.MaxGenerationBytes {
		t.Fatal("publication exceeded finite generation cap")
	}
	cowTestClose(left)
	cowTestClose(right)
	cowTestClose(writer)
	budget.Close()
	cowTestRelease(root)
	cowTestRelease(next)
	if budget.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats())
	}
}

func TestCOWCapturedForkPreservesAlreadyPreparedResourceReservation(t *testing.T) {
	limits := DefaultCOWLimits()
	limits.MaxResources = 1
	budget, writer := cowTestWriter(t, limits)
	root := cowTestPublish(t, writer, cowTestEntries(1))
	original, err := writer.Prepare(nil, COWPrepareOptions{ResourceSlots: 1, ResourceBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	fork, err := ForkCOWWriter(root)
	if err != nil {
		t.Fatal(err)
	}
	before := budget.Stats()
	if _, err = fork.Prepare(nil, COWPrepareOptions{ResourceSlots: 1}); !errors.Is(err, ErrCOWPending) {
		t.Fatal("fork raced an existing original ID reservation", err)
	}
	if budget.Stats() != before {
		t.Fatal("resource refusal changed accounting")
	}
	var releases atomic.Int64
	if err = original.AttachResources([]COWResourceID{{Kind: 1, ID: 99}}, func() { releases.Add(1) }); err != nil {
		t.Fatal(err)
	}
	next := original.Publish()
	original = nil
	other := cowTestPublish(t, fork, []COWMutation{{Key: []byte("other"), Value: []byte("value")}})
	cowTestClose(fork)
	if _, err = writer.Prepare(nil, COWPrepareOptions{ResourceSlots: 1}); !errors.Is(err, ErrCOWCapacity) {
		t.Fatal("original finite ID capacity was bypassed", err)
	}
	cowTestClose(writer)
	budget.Close()
	cowTestRelease(root)
	cowTestRelease(next)
	if releases.Load() != 0 {
		t.Fatal("fork lost original resource owner")
	}
	cowTestRelease(other)
	if releases.Load() != 1 || budget.Stats().TotalBytes != 0 {
		t.Fatal(releases.Load(), budget.Stats())
	}
}
