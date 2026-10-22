package memtable

import (
	"math"
	"runtime"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestCOWOrderedDescriptorPostimagesRemainImmutable(t *testing.T) {
	budget, writer := cowOrderedLifecycleWriter(t)
	first := cowOrderedLifecycleMutation(t, "shared", 1, "first")
	middle := cowOrderedLifecycleMutation(t, "shared", 7, "middle")
	maximum := cowOrderedLifecycleMutation(t, "shared", math.MaxUint64, "maximum")
	maximum.Revision = page.EntryRevision(math.MaxUint64 - 1)
	old := cowTestPublish(t, writer, []COWMutation{first, middle, maximum})
	defer func() {
		cowTestRelease(old)
		cowTestClose(writer)
		budget.Close()
	}()
	prefix, _, _ := cowOrderedPrefix(first.Key)
	before, ok := cowOrderedPrimaryGet(old.ordered.keys, prefix)
	if !ok || before.versions == nil {
		t.Fatal("fixture did not activate descriptor-backed history")
	}
	oldFirst, _ := before.versions.Get(1)
	oldMiddle, _ := before.versions.Get(7)
	oldMaximum, _ := before.versions.Get(math.MaxUint64)
	replacement := cowOrderedLifecycleMutation(t, "shared", 7, "replacement")
	replacement.Revision = 2 // Native identity need not increase with timestamp.
	erase := maximum
	erase.Remove = true
	backfill := cowOrderedLifecycleMutation(t, "shared", 3, "backfill")
	reinsert := maximum
	reinsert.Value = []byte("reinserted maximum")
	reinsert.Revision = 1
	prepared, err := writer.Prepare([]COWMutation{replacement, erase, backfill, reinsert}, COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Caller-owned bytes may change as soon as private preparation has copied
	// them. No descriptor may retain those mutable buffers.
	for i := range replacement.Value {
		replacement.Value[i] = '!'
	}
	for i := range reinsert.Value {
		reinsert.Value[i] = '!'
	}
	fresh := prepared.Publish()
	defer cowTestRelease(fresh)
	after, ok := cowOrderedPrimaryGet(fresh.ordered.keys, prefix)
	if !ok || after.versions == nil || after.count != 4 {
		t.Fatal("new descriptor-backed history missing")
	}
	newFirst, _ := after.versions.Get(1)
	newMiddle, _ := after.versions.Get(7)
	newMaximum, _ := after.versions.Get(math.MaxUint64)
	if newFirst != oldFirst || newMiddle == oldMiddle || newMaximum == oldMaximum {
		t.Fatal("untouched descriptors must share; replaced descriptors must be new")
	}
	if oldMiddle.value.record(oldMiddle.key).Value != "middle" || oldMaximum.value.record(oldMaximum.key).Value != "maximum" {
		t.Fatal("private history mutation changed an old immutable descriptor")
	}
	if record, ok := old.Get(maximum.Key); !ok || record.Value != "maximum" || record.Revision != maximum.Revision {
		t.Fatal("old root lost full timestamp/native revision", record, ok)
	}
	if record, ok := fresh.Get(replacement.Key); !ok || record.Value != "replacement" || record.Revision != 2 {
		t.Fatal("replacement descriptor aliases input or loses native revision", record, ok)
	}
	if record, ok := fresh.Get(maximum.Key); !ok || record.Value != "reinserted maximum" || record.Revision != 1 {
		t.Fatal("maximum descriptor aliases input or loses native revision", record, ok)
	}
}

func TestCOWOrderedDescriptorPromotionCyclesFitAdmission(t *testing.T) {
	cowTestIsolateAllocationWitness(t)
	budget, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewCOWOrderedWriter(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cowTestClose(writer)
		budget.Close()
	}()
	var entries []COWMutation
	for cycle := uint64(0); cycle < 100; cycle++ {
		first := cowOrderedLifecycleMutation(t, "cycles", 2*cycle+1, "inline first")
		second := cowOrderedLifecycleMutation(t, "cycles", 2*cycle+2, "numeric second")
		eraseFirst, eraseSecond := first, second
		eraseFirst.Remove, eraseSecond.Remove = true, true
		entries = append(entries, first, second, eraseFirst, eraseSecond)
	}
	entries = append(entries, cowOrderedLifecycleMutation(t, "cycles", math.MaxUint64, "last inline"))
	accounting := budget.Stats()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	prepared, err := writer.Prepare(entries, COWPrepareOptions{})
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > prepared.Charge().Total() {
		t.Errorf("promotion/discard cycles allocated %d above preadmitted %+v", allocated, prepared.Charge())
	}
	// Even discarded maps/descriptors consumed this preparation's envelope.
	// Its actual node receipt must cover more than the final singleton tree.
	if prepared.ordered.historyNodeCharge == 0 || prepared.root.Len() != 1 {
		t.Fatal("fixture did not discard numeric maps before the final singleton")
	}
	retirement := prepared.Cancel()
	retirement.Drain()
	got := budget.Stats()
	got.PeakBytes = accounting.PeakBytes
	if got != accounting {
		t.Fatalf("cancelled descriptors changed retained ownership: got %+v want %+v", got, accounting)
	}
}

func TestCOWOrderedDescriptorSingletonAndPairLayout(t *testing.T) {
	if got := unsafe.Sizeof(cowOrderedVersionPairLayout{}); got >= unsafe.Sizeof(cowOrderedValue{})+unsafe.Sizeof(uint64(0)) || unsafe.Sizeof(uintptr(0)) == 8 && got != 16 {
		t.Fatalf("numeric node pair remains wide: %d", got)
	}
	entry := cowOrderedLifecycleMutation(t, "singleton", 7, "same timestamp")
	value := cowOrderedValue{key: string(entry.Key), value: cowEncodeValue(entry)}
	history := &cowOrderedHistory{}
	history.set(7, value)
	if allocations := testing.AllocsPerRun(10, func() {
		if !history.set(7, value) {
			panic("singleton replacement lost membership")
		}
	}); allocations != 0 || history.versions != nil {
		t.Fatalf("inline singleton replacement allocates descriptor: allocs=%g numeric=%v", allocations, history.versions != nil)
	}
}
