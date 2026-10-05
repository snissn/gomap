package memtable

import (
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/tidwall/btree"
)

func cowTestWriter(t testing.TB, limits COWLimits) (*COWBudget, *COWWriter) {
	t.Helper()
	b, e := NewCOWBudget(limits)
	if e != nil {
		t.Fatal(e)
	}
	w, e := NewCOWWriter(b)
	if e != nil {
		t.Fatal(e)
	}
	return b, w
}
func cowTestPublish(t testing.TB, w *COWWriter, entries []COWMutation) *COWRoot {
	t.Helper()
	p, e := w.Prepare(entries, COWPrepareOptions{})
	if e != nil {
		t.Fatal(e)
	}
	return p.Publish()
}
func cowTestRelease(r *COWRoot) { d := r.Release(); d.Drain() }
func cowTestClose(w *COWWriter) { d := w.Close(); d.Drain() }
func cowTestEntries(n int) []COWMutation {
	e := make([]COWMutation, n)
	for i := range e {
		e[i] = COWMutation{Key: []byte(fmt.Sprintf("%08d", i)), Value: []byte("value"), Flags: node.FlagInline, Revision: page.EntryRevision(i + 1)}
	}
	return e
}

func TestCOWOwnedHeadersAndBytes(t *testing.T) {
	b, w := cowTestWriter(t, DefaultCOWLimits())
	key, value := []byte("key"), []byte("old")
	old := cowTestPublish(t, w, []COWMutation{{Key: key, Value: value, Flags: node.FlagInline, Revision: 7}})
	key[0] = 'X'
	value[0] = 'X'
	view, e := old.Acquire(0)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 100; i++ {
		next := cowTestPublish(t, w, []COWMutation{{Key: []byte("key"), Value: []byte("new")}})
		cowTestRelease(next)
	}
	deleted := cowTestPublish(t, w, []COWMutation{{Key: []byte("key"), Remove: true}})
	cowTestRelease(deleted)
	got, ok, e := view.Get([]byte("key"))
	if e != nil || !ok || got.Value != "old" || got.Revision != 7 {
		t.Fatalf("old view=%+v found=%v err=%v", got, ok, e)
	}
	copyValue := []byte(got.Value)
	copyValue[0] = '!'
	again, _ := old.Get([]byte("key"))
	if again.Value != "old" {
		t.Fatal("safe output alias changed owned bytes")
	}
	// Poison/reuse the legacy arena; the COW capability accepts no arena/steal.
	legacy := NewBTree()
	legacy.Set([]byte("key"), []byte("poison"))
	legacy.Reset()
	legacy.Set([]byte("key"), []byte("reset!"))
	if _, ok := any(w).(Table); ok {
		t.Fatal("COW writer must not expose legacy mutable table capabilities")
	}
	cowTestClose(w)
	cowTestRelease(old)
	if b.Stats().HistoryBytes == 0 {
		t.Fatal("live view lost generation history")
	}
	view.Close()
	view.Close()
	if s := b.Stats(); s.TotalBytes != 0 || s.Generations != 0 || s.Views != 0 {
		t.Fatalf("leak: %+v", s)
	}
}

func TestCOWPrivatePreparationCancelAndResourceOwnership(t *testing.T) {
	b, w := cowTestWriter(t, DefaultCOWLimits())
	old := cowTestPublish(t, w, []COWMutation{{Key: []byte("key"), Value: []byte("old")}})
	before := b.Stats()
	p, e := w.Prepare([]COWMutation{{Key: []byte("key"), Value: []byte("new")}}, COWPrepareOptions{ResourceSlots: 2, ResourceBytes: 128, ExtraBytes: 256})
	if e != nil {
		t.Fatal(e)
	}
	if p.Root().Retain() {
		t.Fatal("staged root was retainable before publication")
	}
	if _, e := w.Prepare(nil, COWPrepareOptions{}); !errors.Is(e, ErrCOWPending) {
		t.Fatal(e)
	}
	var released atomic.Int32
	ids := []COWResourceID{{Kind: 1, ID: 9}, {Kind: 2, ID: 4}}
	if e = p.AttachResources(ids, func() { released.Add(1) }); e != nil {
		t.Fatal(e)
	}
	if got, _ := old.Get([]byte("key")); got.Value != "old" {
		t.Fatal("private preparation became visible")
	}
	d := p.Cancel()
	if released.Load() != 0 {
		t.Fatal("callback ran inside cancellation")
	}
	d.Drain()
	d.Drain()
	if released.Load() != 1 {
		t.Fatal("cancel release not exactly once")
	}
	if got := b.Stats(); got.TotalBytes != before.TotalBytes || got.ReservedBytes != 0 {
		t.Fatalf("cancel charge %+v before %+v", got, before)
	}
	p, e = w.Prepare(nil, COWPrepareOptions{ResourceSlots: 2, ResourceBytes: 128})
	if e != nil {
		t.Fatal(e)
	}
	if e = p.AttachResources(ids, func() { released.Add(1) }); e != nil {
		t.Fatal(e)
	}
	root := p.Publish()
	if !w.HasResource(ids[0]) {
		t.Fatal("resource identity missing")
	}
	p, e = w.Prepare(nil, COWPrepareOptions{ResourceSlots: 1})
	if e != nil {
		t.Fatal(e)
	}
	if e = p.AttachResources(ids[:1], func() { t.Fatal("duplicate owner transferred") }); e == nil {
		t.Fatal("duplicate resource accepted")
	}
	d = p.Cancel()
	d.Drain()
	if e = w.Freeze(); e != nil {
		t.Fatal(e)
	}
	cowTestClose(w)
	cowTestRelease(root)
	if released.Load() != 1 {
		t.Fatal("old header did not retain generation resources")
	}
	cowTestRelease(old)
	if released.Load() != 2 || b.Stats().TotalBytes != 0 {
		t.Fatalf("final release=%d stats=%+v", released.Load(), b.Stats())
	}
}

func TestCOWFiniteReplacementHistoryAndResume(t *testing.T) {
	l := DefaultCOWLimits()
	l.MaxGenerationBytes = 160 << 10
	l.MaxTotalBytes = 512 << 10
	l.MaxRetiredBytes = 512 << 10
	l.MaxInFlightBytes = 160 << 10
	b, w := cowTestWriter(t, l)
	old := cowTestPublish(t, w, []COWMutation{{Key: []byte("key"), Value: []byte("old")}})
	accepted := 0
	var last *COWRoot
	for i := 0; i < 1000; i++ {
		p, e := w.Prepare([]COWMutation{{Key: []byte("key"), Value: []byte("new")}}, COWPrepareOptions{})
		if errors.Is(e, ErrCOWCapacity) {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		accepted++
		if last != nil {
			cowTestRelease(last)
		}
		last = p.Publish()
	}
	if accepted == 0 || accepted == 1000 {
		t.Fatalf("unbounded or unusable replacements: %d", accepted)
	}
	if got, _ := old.Get([]byte("key")); got.Value != "old" {
		t.Fatal("pinned predecessor invalidated")
	}
	if e := w.Freeze(); e != nil {
		t.Fatal(e)
	}
	cowTestClose(w)
	cowTestRelease(last)
	if s := b.Stats(); s.RetiredBytes == 0 || s.TotalBytes > l.MaxTotalBytes || s.ReservedBytes != 0 {
		t.Fatalf("retention %+v", s)
	}
	cowTestRelease(old)
	fresh, e := NewCOWWriter(b)
	if e != nil {
		t.Fatal(e)
	}
	root := cowTestPublish(t, fresh, []COWMutation{{Key: []byte("key"), Value: []byte("resumed")}})
	cowTestRelease(root)
	cowTestClose(fresh)
	t.Logf("accepted constant-size replacements=%d peak engine charge=%d", accepted, b.Stats().PeakBytes)
	if b.Stats().TotalBytes != 0 {
		t.Fatal(b.Stats())
	}
}

func TestCOWSplitDeleteBatchHeightAndMetadata(t *testing.T) {
	b, w := cowTestWriter(t, DefaultCOWLimits())
	entries := cowTestEntries(4096)
	p, e := w.Prepare(entries, COWPrepareOptions{})
	if e != nil {
		t.Fatal(e)
	}
	if p.Charge().Height < 3 {
		t.Fatalf("batch height bound %d", p.Charge().Height)
	}
	old := p.Publish()
	for i := range entries {
		entries[i].Remove = true
	}
	next := cowTestPublish(t, w, entries)
	if old.Len() != 4096 || next.Len() != 0 {
		t.Fatalf("old/new counts %d/%d", old.Len(), next.Len())
	}
	for _, i := range []int{0, 63, 1024, 4095} {
		got, ok := old.Get([]byte(fmt.Sprintf("%08d", i)))
		if !ok || got.Revision != page.EntryRevision(i+1) || got.Value != "value" {
			t.Fatalf("old record %d %+v", i, got)
		}
	}
	cowTestRelease(old)
	cowTestRelease(next)
	cowTestClose(w)
	if b.Stats().TotalBytes != 0 {
		t.Fatal(b.Stats())
	}
}

func TestCOWConcurrentTraversalAndClose(t *testing.T) {
	b, w := cowTestWriter(t, DefaultCOWLimits())
	old := cowTestPublish(t, w, cowTestEntries(1024))
	view, e := old.Acquire(0)
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for j := 0; j < 4; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 20; k++ {
				c, e := old.Cursor(nil, nil)
				if e != nil {
					t.Error(e)
					return
				}
				n := 0
				for {
					r, ok, e := c.Record()
					if e != nil || !ok {
						break
					}
					if r.Value != "value" {
						t.Error("old traversal changed")
					}
					n++
					_ = c.Next()
				}
				c.Close()
				if n != 1024 {
					t.Errorf("count %d", n)
				}
			}
		}()
	}
	for i := 0; i < 100; i++ {
		next := cowTestPublish(t, w, []COWMutation{{Key: []byte("00000001"), Value: []byte("replacement")}})
		cowTestRelease(next)
	}
	c, e := view.Cursor(nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = c.Next()
			_ = c.Seek([]byte("00000010"))
			_, _, _ = c.Record()
		}
	}()
	go func() { defer wg.Done(); c.Close(); view.Close() }()
	wg.Wait()
	c.Close()
	view.Close()
	cowTestRelease(old)
	cowTestClose(w)
	if b.Stats().TotalBytes != 0 {
		t.Fatal(b.Stats())
	}
}

func TestCOWLimitsAndRefusal(t *testing.T) {
	l := DefaultCOWLimits()
	l.MaxViews = 1
	l.MaxGenerations = 1
	l.MaxSources = 1
	l.MaxResources = 1
	b, w := cowTestWriter(t, l)
	if _, e := NewCOWWriter(b); !errors.Is(e, ErrCOWCapacity) {
		t.Fatal("generation cap", e)
	}
	root := cowTestPublish(t, w, nil)
	v, e := root.Acquire(0)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = root.Acquire(0); !errors.Is(e, ErrCOWCapacity) {
		t.Fatal("view cap", e)
	}
	v.Close()
	before := b.Stats()
	if _, e = w.Prepare([]COWMutation{{Flags: node.FlagPointer | node.FlagTombstone}}, COWPrepareOptions{}); e == nil {
		t.Fatal("invalid flags admitted")
	}
	if _, e = w.Prepare(nil, COWPrepareOptions{ExtraBytes: ^uint64(0)}); !errors.Is(e, ErrCOWCapacity) {
		t.Fatal("overflow", e)
	}
	if _, e = w.Prepare(nil, COWPrepareOptions{ResourceSlots: 2}); !errors.Is(e, ErrCOWCapacity) {
		t.Fatal("resource cap", e)
	}
	if after := b.Stats(); after.TotalBytes != before.TotalBytes || after.ReservedBytes != 0 {
		t.Fatal("refusal changed budget", after)
	}
	if e = w.Freeze(); e != nil {
		t.Fatal(e)
	}
	if _, e = w.Prepare(nil, COWPrepareOptions{}); !errors.Is(e, ErrCOWCapacity) {
		t.Fatal("frozen writer mutated", e)
	}
	cowTestClose(w)
	cowTestRelease(root)
	if _, e = root.Acquire(0); !errors.Is(e, ErrCOWClosed) {
		t.Fatal("released root admitted view", e)
	}
	l.MaxViews = 0
	if _, e = NewCOWBudget(l); e == nil {
		t.Fatal("unlimited limit accepted")
	}
}

func TestCOWDependencyReserveWitness(t *testing.T) {
	for _, n := range []int{1, 31, 63, 64, 1024, 2046, 2047, 4096} {
		for _, kind := range []string{"insert", "replace", "delete"} {
			t.Run(fmt.Sprintf("%d/%s", n, kind), func(t *testing.T) {
				m := btree.NewMap[string, cowValue](32)
				for i := 0; i < n; i++ {
					m.Set(fmt.Sprintf("%08d", i), cowValue{value: "old"})
				}
				old := m.Copy()
				height := cowMaximumHeight(n + 1)
				if m.Height() > height {
					height = m.Height()
				}
				charge := cowNodeReserveFor(height, kind == "replace", kind == "delete", n+1) + 2*cowAllocation(uint64(unsafe.Sizeof(btree.Map[string, cowValue]{})))
				runtime.GC()
				var a, z runtime.MemStats
				runtime.ReadMemStats(&a)
				private := m.Copy()
				switch kind {
				case "insert":
					private.Set("99999999", cowValue{value: "new"})
				case "replace":
					private.Set("00000000", cowValue{value: "new"})
				case "delete":
					private.Delete("00000000")
				}
				read := private.Copy()
				runtime.ReadMemStats(&z)
				allocated := z.TotalAlloc - a.TotalAlloc
				if allocated > charge {
					t.Fatalf("dependency allocated %d > reserve %d", allocated, charge)
				}
				t.Logf("n=%d kind=%s height=%d allocated=%d reserve=%d ratio=%.2f", n, kind, height, allocated, charge, float64(charge)/float64(allocated))
				runtime.KeepAlive(old)
				runtime.KeepAlive(read)
			})
		}
	}
}

func TestCOWBatchReserveWitness(t *testing.T) {
	for _, kind := range []string{"insert", "replace", "delete", "mixed"} {
		for _, n := range []int{63, 1024, 4096} {
			t.Run(fmt.Sprintf("%s/%d", kind, n), func(t *testing.T) {
				limits := DefaultCOWLimits()
				limits.MaxGenerationBytes = 1 << 30
				limits.MaxInFlightBytes = 1 << 30
				limits.MaxTotalBytes = 2 << 30
				limits.MaxRetiredBytes = 2 << 30
				_, w := cowTestWriter(t, limits)
				var old *COWRoot
				if kind != "insert" {
					old = cowTestPublish(t, w, cowTestEntries(n))
				}
				entries := cowTestEntries(n)
				for i := range entries {
					switch kind {
					case "delete":
						entries[i].Remove = true
					case "mixed":
						entries[i].Remove = i%2 == 0
						if i%3 == 0 {
							entries[i].Key = []byte(fmt.Sprintf("new-%08d", i))
						}
					}
				}
				estimate, e := w.Estimate(entries, COWPrepareOptions{})
				if e != nil {
					t.Fatal(e)
				}
				runtime.GC()
				var a, z runtime.MemStats
				runtime.ReadMemStats(&a)
				p, e := w.Prepare(entries, COWPrepareOptions{})
				if e != nil {
					t.Fatal(e)
				}
				root := p.Publish()
				runtime.ReadMemStats(&z)
				if got := z.TotalAlloc - a.TotalAlloc; got > estimate.Total() {
					t.Fatalf("allocated=%d reserve=%d", got, estimate.Total())
				}
				t.Logf("kind=%s n=%d allocated=%d reserved=%d node-reserve=%d height=%d", kind, n, z.TotalAlloc-a.TotalAlloc, estimate.Total(), estimate.Nodes, estimate.Height)
				if old != nil {
					cowTestRelease(old)
				}
				cowTestRelease(root)
				cowTestClose(w)
			})
		}
	}
}

func TestCOWMixedDeletionDoesNotUseDeleteOnlyBound(t *testing.T) {
	_, w := cowTestWriter(t, DefaultCOWLimits())
	old := cowTestPublish(t, w, cowTestEntries(1024))
	entries := []COWMutation{{Key: []byte("00000000"), Remove: true}, {Key: []byte("new"), Value: []byte("new")}}
	c, e := w.Estimate(entries, COWPrepareOptions{})
	if e != nil {
		t.Fatal(e)
	}
	want := cowNodeReserveFor(c.Height, false, true, 1026) + cowNodeReserveFor(c.Height, false, false, 1026)
	if c.Nodes > want || c.Nodes <= cowNodeReserveFor(c.Height, false, false, 1026) {
		t.Fatalf("mixed bound=%d outside operation bounds (max %d)", c.Nodes, want)
	}
	cowTestRelease(old)
	cowTestClose(w)
}

func TestCOWRebalanceCapacityHistoryWitness(t *testing.T) {
	l := DefaultCOWLimits()
	l.MaxGenerationBytes = 256 << 20
	l.MaxInFlightBytes = 256 << 20
	_, w := cowTestWriter(t, l)
	old := cowTestPublish(t, w, cowTestEntries(2048))
	cowTestRelease(old)
	rng := rand.New(rand.NewSource(1))
	for step := 0; step < 80; step++ {
		entries := make([]COWMutation, 32)
		for i := range entries {
			entries[i] = COWMutation{Key: []byte(fmt.Sprintf("%08d", rng.Intn(4096))), Value: []byte("replacement"), Remove: rng.Intn(2) == 0}
		}
		c, e := w.Estimate(entries, COWPrepareOptions{})
		if e != nil {
			t.Fatal(e)
		}
		var a, z runtime.MemStats
		runtime.ReadMemStats(&a)
		p, e := w.Prepare(entries, COWPrepareOptions{})
		if e != nil {
			t.Fatal(e)
		}
		root := p.Publish()
		runtime.ReadMemStats(&z)
		if z.TotalAlloc-a.TotalAlloc > c.Total() {
			t.Fatalf("step %d alloc=%d reserved=%d", step, z.TotalAlloc-a.TotalAlloc, c.Total())
		}
		cowTestRelease(root)
	}
	cowTestClose(w)
}

func TestCOWRetainedGenerationPlateau(t *testing.T) {
	l := DefaultCOWLimits()
	l.MaxGenerationBytes = 128 << 10
	l.MaxInFlightBytes = 128 << 10
	l.MaxTotalBytes = 1 << 20
	l.MaxRetiredBytes = 1 << 20
	l.MaxSources = 16
	l.MaxGenerations = 16
	b, e := NewCOWBudget(l)
	if e != nil {
		t.Fatal(e)
	}
	var pinned []*COWRoot
	var peakHeap, peakInuse uint64
	writes := 0
	for generation := 0; generation < 32; generation++ {
		w, e := NewCOWWriter(b)
		if errors.Is(e, ErrCOWCapacity) {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		first := cowTestPublish(t, w, []COWMutation{{Key: []byte("key"), Value: []byte(fmt.Sprintf("generation-%d", generation))}})
		pinned = append(pinned, first)
		for i := 0; i < 1000; i++ {
			p, e := w.Prepare([]COWMutation{{Key: []byte("key"), Value: []byte("replacement")}}, COWPrepareOptions{})
			if errors.Is(e, ErrCOWCapacity) {
				break
			}
			if e != nil {
				t.Fatal(e)
			}
			r := p.Publish()
			cowTestRelease(r)
			writes++
		}
		if e = w.Freeze(); e != nil {
			t.Fatal(e)
		}
		cowTestClose(w)
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		if m.HeapAlloc > peakHeap {
			peakHeap = m.HeapAlloc
		}
		if m.HeapInuse > peakInuse {
			peakInuse = m.HeapInuse
		}
		if b.Stats().TotalBytes > l.MaxTotalBytes {
			t.Fatal("budget overrun")
		}
	}
	if len(pinned) < 2 || len(pinned) == 32 {
		t.Fatalf("retention not finite: %d", len(pinned))
	}
	for i, r := range pinned {
		got, ok := r.Get([]byte("key"))
		if !ok || got.Value != fmt.Sprintf("generation-%d", i) {
			t.Fatalf("pinned generation %d changed", i)
		}
	}
	runtime.GC()
	var held runtime.MemStats
	runtime.ReadMemStats(&held)
	charged := b.Stats()
	for _, r := range pinned {
		cowTestRelease(r)
	}
	pinned = nil
	runtime.GC()
	var drained runtime.MemStats
	runtime.ReadMemStats(&drained)
	w, e := NewCOWWriter(b)
	if e != nil {
		t.Fatal("release did not resume admission", e)
	}
	cowTestClose(w)
	t.Logf("writes=%d generations=%d engine=%d retired=%d peak-charge=%d sampled-heap-high=%d sampled-inuse-high=%d held-heap=%d drained-heap=%d", writes, charged.Generations, charged.TotalBytes, charged.RetiredBytes, charged.PeakBytes, peakHeap, peakInuse, held.HeapAlloc, drained.HeapAlloc)
	if b.Stats().TotalBytes != 0 {
		t.Fatal(b.Stats())
	}
}
