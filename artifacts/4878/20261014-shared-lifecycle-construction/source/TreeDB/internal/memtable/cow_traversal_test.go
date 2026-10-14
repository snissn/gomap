package memtable

import (
	"errors"
	"fmt"
	btree "github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func traversalRequest(t *testing.T, c *COWTraversal, op COWTraversalOperation, key string) (COWRecord, bool) {
	t.Helper()
	work := COWTraversalWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if err := c.Begin(op, key, &work); err != nil {
		t.Fatal(err)
	}
	for rounds := 0; ; rounds++ {
		done, err := c.Step(&work)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
		if rounds > 10000 {
			t.Fatal("traversal no progress")
		}
		if _, valid, err := c.Record(&work); err != nil || valid {
			t.Fatal("partial record escaped", valid, err)
		}
		work = COWTraversalWork{RecordLimit: 32, ByteLimit: 1 << 20}
	}
	record, valid, err := c.Record(&work)
	if errors.Is(err, btree.ErrWork) {
		work = COWTraversalWork{RecordLimit: 32, ByteLimit: 1 << 20}
		record, valid, err = c.Record(&work)
	}
	if err != nil {
		t.Fatal(err)
	}
	return record, valid
}
func traversalClose(t *testing.T, c *COWTraversal) COWRetirement {
	t.Helper()
	for rounds := 0; ; rounds++ {
		work := COWTraversalWork{RecordLimit: 32, ByteLimit: 1 << 20}
		done, ret, err := c.CloseStep(&work)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			return ret
		}
		if rounds > 10000 {
			t.Fatal("close no progress")
		}
	}
}
func TestCOWChargedTraversalOldRootMetadataAndRefund(t *testing.T) {
	b, w := cowTestWriter(t, DefaultCOWLimits())
	entries := cowTestEntries(4096)
	entries[1].Flags = node.FlagTombstone
	entries[1].Value = nil
	entries[2].Flags = node.FlagPointer
	entries[2].Value = nil
	entries[2].Ptr = page.ValuePtr{FileID: 1, Offset: 20, Length: 30}
	p, err := w.Prepare(entries, COWPrepareOptions{ResourceSlots: 1})
	if err != nil {
		t.Fatal(err)
	}
	var released atomic.Int32
	if err = p.AttachResources([]COWResourceID{{Kind: 1, ID: 1}}, func() { released.Add(1) }); err != nil {
		t.Fatal(err)
	}
	root := p.Publish()
	work := COWTraversalWork{RecordLimit: 32, ByteLimit: 1 << 20}
	c, err := root.NewTraversal(&work, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, valid, err := c.Record(&work); err != nil || valid {
		t.Fatal("constructor performed eager seek")
	}
	fresh := cowTestPublish(t, w, []COWMutation{{Key: []byte("00000002"), Remove: true}, {Key: []byte("00000003"), Value: []byte("new")}})
	cowTestRelease(fresh)
	cowTestRelease(root)
	cowTestClose(w)
	b.Close()
	r, valid := traversalRequest(t, c, COWSeekGE, "00000001")
	if !valid || r.Flags&node.FlagTombstone == 0 || r.Revision != 2 {
		t.Fatal("physical tombstone hidden", r)
	}
	r, valid = traversalRequest(t, c, COWNext, "")
	if !valid || r.Ptr != entries[2].Ptr || r.Revision != 3 {
		t.Fatal("pointer metadata changed", r)
	}
	r, valid = traversalRequest(t, c, COWSeekLE, "00000003")
	if !valid || r.Value != "value" || r.Revision != 4 {
		t.Fatal("read captured fresh root", r)
	}
	if released.Load() != 0 {
		t.Fatal("resource released under cursor")
	}
	history := b.Stats().HistoryBytes
	ret := traversalClose(t, c)
	if released.Load() != 0 || b.Stats().HistoryBytes != history || b.Stats().Views != 0 {
		t.Fatal("close claimed undrained resource death", released.Load(), b.Stats())
	}
	done, again, err := c.CloseStep(&work)
	if !done || err != nil {
		t.Fatal("repeated close", done, err)
	}
	again.Drain()
	if released.Load() != 0 {
		t.Fatal("retirement reissued")
	}
	ret.Drain()
	ret.Drain()
	if released.Load() != 1 || b.Stats().TotalBytes != 0 {
		t.Fatal("retirement/refund", released.Load(), b.Stats())
	}
}
func TestCOWChargedTraversalAdmissionAndCancellation(t *testing.T) {
	b, w := cowTestWriter(t, DefaultCOWLimits())
	root := cowTestPublish(t, w, cowTestEntries(4096))
	before := b.Stats()
	zero := COWTraversalWork{}
	if c, err := root.NewTraversal(&zero, 0); c != nil || !errors.Is(err, btree.ErrWork) || b.Stats() != before {
		t.Fatal("refused constructor acquired owner", err)
	}
	work := COWTraversalWork{RecordLimit: 32, ByteLimit: 1 << 20}
	c, err := root.NewTraversal(&work, 0)
	if err != nil {
		t.Fatal(err)
	}
	work = COWTraversalWork{RecordLimit: 1, ByteLimit: 1 << 20}
	if err = c.Begin(COWLast, "", &work); err != nil {
		t.Fatal(err)
	}
	if done, err := c.Step(&work); done || err != nil {
		t.Fatal("used work after last record", done, err)
	}
	ret := traversalClose(t, c)
	ret.Drain()
	cowTestRelease(root)
	cowTestClose(w)
	b.Close()
	if b.Stats().TotalBytes != 0 {
		t.Fatal(b.Stats())
	}
}
func TestCOWChargedTraversalLongBorrowedKey(t *testing.T) {
	b, w := cowTestWriter(t, DefaultCOWLimits())
	prefix := strings.Repeat("x", 2<<20)
	root := cowTestPublish(t, w, []COWMutation{{Key: []byte(prefix + "a"), Value: []byte("old")}, {Key: []byte(prefix + "c"), Value: []byte("new")}})
	work := COWTraversalWork{RecordLimit: 32, ByteLimit: 1 << 20}
	c, err := root.NewTraversal(&work, 0)
	if err != nil {
		t.Fatal(err)
	}
	r, valid := traversalRequest(t, c, COWSeekLE, prefix+"b")
	if !valid || r.Value != "old" {
		t.Fatal("partial long-key comparison result")
	}
	ret := traversalClose(t, c)
	ret.Drain()
	cowTestRelease(root)
	cowTestClose(w)
	b.Close()
}
func TestCOWChargedTraversalConcurrentImmutableReaders(t *testing.T) {
	b, w := cowTestWriter(t, DefaultCOWLimits())
	root := cowTestPublish(t, w, cowTestEntries(4096))
	var wg sync.WaitGroup
	for j := 0; j < 4; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			work := COWTraversalWork{RecordLimit: 32, ByteLimit: 1 << 20}
			c, err := root.NewTraversal(&work, 0)
			if err != nil {
				t.Error(err)
				return
			}
			for i := 0; i < 50; i++ {
				r, valid := traversalRequest(t, c, COWSeekLE, fmt.Sprintf("%08d", i*3))
				if !valid || r.Revision != page.EntryRevision(i*3+1) {
					t.Error("old root mismatch")
				}
			}
			ret := traversalClose(t, c)
			ret.Drain()
		}()
	}
	for i := 0; i < 25; i++ {
		fresh := cowTestPublish(t, w, []COWMutation{{Key: []byte("00000003"), Value: []byte("new")}})
		cowTestRelease(fresh)
	}
	wg.Wait()
	cowTestRelease(root)
	cowTestClose(w)
	b.Close()
	if b.Stats().TotalBytes != 0 {
		t.Fatal(b.Stats())
	}
}
func TestCOWChargedTraversalAllocationAdmission(t *testing.T) {
	b, w := cowTestWriter(t, DefaultCOWLimits())
	root := cowTestPublish(t, w, cowTestEntries(4096))
	cowTestIsolateAllocationWitness(t)
	work := &COWTraversalWork{RecordLimit: 32, ByteLimit: 1 << 20}
	beforeCharge := b.Stats().TotalBytes
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	c, err := root.NewTraversal(work, 0)
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	charged := b.Stats().TotalBytes - beforeCharge
	actual := after.TotalAlloc - before.TotalAlloc
	if actual > charged {
		t.Fatal("actual traversal allocation escaped admission", actual, charged)
	}
	ret := traversalClose(t, c)
	ret.Drain()
	if b.Stats().TotalBytes != beforeCharge {
		t.Fatal("wrapper refund mismatch")
	}
	cowTestRelease(root)
	cowTestClose(w)
	b.Close()
	runtime.KeepAlive(work)
}
