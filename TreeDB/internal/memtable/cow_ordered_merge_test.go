package memtable

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	btree "github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/node"
)

func cowOrderedMergedRequest(t *testing.T, c *COWTraversal, op COWTraversalOperation, key string) (COWRecord, bool) {
	t.Helper()
	w := COWTraversalWork{RecordLimit: 10, ByteLimit: 16384}
	if err := c.Begin(op, key, &w); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10000; i++ {
		w = COWTraversalWork{RecordLimit: 3, ByteLimit: 2048}
		done, err := c.Step(&w)
		if err != nil {
			t.Fatal(err)
		}
		if !done {
			continue
		}
		w = COWTraversalWork{RecordLimit: 3, ByteLimit: 2048}
		r, ok, err := c.Record(&w)
		if err != nil {
			t.Fatal(err)
		}
		return r, ok
	}
	t.Fatal("merged traversal failed to progress")
	return COWRecord{}, false
}

func TestCOWOrderedMergedTraversalAndOperationOwner(t *testing.T) {
	b, w := cowOrderedLifecycleWriter(t)
	entries := []COWMutation{{Key: []byte{}, Value: []byte("empty raw"), Flags: node.FlagInline},
		{Key: []byte("\x00T"), Value: []byte("before"), Flags: node.FlagInline},
		{Key: []byte("zz"), Value: []byte("after"), Flags: node.FlagInline}}
	for i := 0; i < 12; i++ {
		for _, ts := range []uint64{1, 5, 9} {
			entries = append(entries, cowOrderedStorageMutation(t, fmt.Sprintf("k%02d", i), ts))
		}
	}
	// An unqualified physical key inside the MVCC prefix must participate in
	// physical ordering, while remaining outside the logical-history index.
	raw := append([]byte(nil), entries[7].Key...)
	raw = append(raw, 0)
	entries = append(entries, COWMutation{Key: raw, Value: []byte("raw extension"), Flags: node.FlagInline})
	r := cowTestPublish(t, w, entries)
	keys := make([]string, len(entries))
	for i := range entries {
		keys[i] = string(entries[i].Key)
	}
	sort.Strings(keys)
	work := COWTraversalWork{RecordLimit: 100, ByteLimit: 1 << 20}
	c, err := r.NewTraversal(&work, 0)
	if err != nil {
		t.Fatal(err)
	}
	// A constructor that cannot fund both stacks creates no partial view.
	before := b.Stats()
	short := COWTraversalWork{RecordLimit: 5, ByteLimit: 1 << 20}
	if partial, err := r.NewTraversal(&short, 0); err != btree.ErrWork || partial != nil {
		t.Fatal("partial constructor admitted", err)
	}
	if after := b.Stats(); after != before {
		t.Fatal("refused constructor changed owners")
	}
	for i, key := range keys {
		got, ok := cowOrderedMergedRequest(t, c, COWSeekGE, key)
		if !ok || got.Key != key {
			t.Fatalf("GE %d = %x,%v", i, got.Key, ok)
		}
		got, ok = cowOrderedMergedRequest(t, c, COWSeekLE, key)
		if !ok || got.Key != key {
			t.Fatalf("LE %d = %x,%v", i, got.Key, ok)
		}
		if i+1 < len(keys) {
			got, ok = cowOrderedMergedRequest(t, c, COWNext, "")
			if !ok || got.Key != keys[i+1] {
				t.Fatalf("direction change next %d = %x,%v", i, got.Key, ok)
			}
			got, ok = cowOrderedMergedRequest(t, c, COWPrev, "")
			if !ok || got.Key != key {
				t.Fatalf("direction change prev %d = %x,%v", i, got.Key, ok)
			}
		}
	}
	for _, bound := range [][]byte{nil, []byte("\x00"), raw, append(raw, 0), []byte("z"), []byte("zzz")} {
		ge := sort.SearchStrings(keys, string(bound))
		got, ok := cowOrderedMergedRequest(t, c, COWSeekGE, string(bound))
		if ok != (ge < len(keys)) || (ok && got.Key != keys[ge]) {
			t.Fatalf("bound GE %x=%x,%v", bound, got.Key, ok)
		}
		le := sort.Search(len(keys), func(i int) bool { return bytes.Compare([]byte(keys[i]), bound) > 0 }) - 1
		got, ok = cowOrderedMergedRequest(t, c, COWSeekLE, string(bound))
		if ok != (le >= 0) || (ok && got.Key != keys[le]) {
			t.Fatalf("bound LE %x=%x,%v", bound, got.Key, ok)
		}
	}
	// The actual cursor keeps the graph alive after every creating owner ends.
	cowTestRelease(r)
	cowTestClose(w)
	b.Close()
	i := 0
	got, ok := cowOrderedMergedRequest(t, c, COWFirst, "")
	for ok {
		if i >= len(keys) || got.Key != keys[i] {
			t.Fatal("forward merge mismatch", i)
		}
		i++
		got, ok = cowOrderedMergedRequest(t, c, COWNext, "")
	}
	if i != len(keys) {
		t.Fatal("forward cardinality", i)
	}
	i = len(keys) - 1
	got, ok = cowOrderedMergedRequest(t, c, COWLast, "")
	for ok {
		if i < 0 || got.Key != keys[i] {
			t.Fatal("reverse merge mismatch", i)
		}
		i--
		got, ok = cowOrderedMergedRequest(t, c, COWPrev, "")
	}
	if i != -1 {
		t.Fatal("reverse cardinality", i)
	}
	for rounds := 0; rounds < 10000; rounds++ {
		work = COWTraversalWork{RecordLimit: 3, ByteLimit: 2048}
		done, retirement, err := c.CloseStep(&work)
		if err != nil {
			t.Fatal(err)
		}
		if !done {
			continue
		}
		retirement.Drain()
		if stats := b.Stats(); stats.TotalBytes != 0 || stats.Views != 0 || stats.Generations != 0 {
			t.Fatalf("last operation owner leaked: %+v", stats)
		}
		return
	}
	t.Fatal("merged close failed to progress")
}
