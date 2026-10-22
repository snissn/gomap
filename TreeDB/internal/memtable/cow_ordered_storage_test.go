package memtable

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	btree "github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/page"
)

func cowOrderedStorageMutation(t *testing.T, logical string, timestamp uint64) COWMutation {
	t.Helper()
	key, err := mvcckey.Encode([]byte(logical), timestamp)
	if err != nil {
		t.Fatal(err)
	}
	return COWMutation{Key: key, Value: []byte(fmt.Sprintf("%q@%d", logical, timestamp)), Revision: page.EntryRevision(timestamp), MVCC: true, Timestamp: timestamp}
}

func cowOrderedStorageBuild(t *testing.T, entries []COWMutation) *cowOrderedTree {
	t.Helper()
	basis := newCOWOrderedTree()
	if _, err := basis.estimate(entries); err != nil {
		t.Fatal(err)
	}
	private := basis.fork()
	private.applyFrom(basis, entries)
	return private.immutableCopy()
}

func cowOrderedStorageOracle(entries []COWMutation) []string {
	keys := make([]string, len(entries))
	for i := range entries {
		keys[i] = string(entries[i].Key)
	}
	sort.Strings(keys)
	return keys
}

func TestCOWOrderedStorageArbitraryPhysicalBounds(t *testing.T) {
	var entries []COWMutation
	for _, key := range []string{"", "a", "a\x00", "a\x00b", "a\x01", "b", "\xff", "\x00"} {
		for _, ts := range []uint64{1, 50, 100, 1000, math.MaxUint64} {
			entries = append(entries, cowOrderedStorageMutation(t, key, ts))
		}
	}
	tree := cowOrderedStorageBuild(t, entries)
	keys := cowOrderedStorageOracle(entries)
	bounds := [][]byte{nil, {}, {0}, {255}}
	for _, key := range keys {
		for n := 0; n <= len(key); n++ {
			bounds = append(bounds, []byte(key[:n]))
		}
		bounds = append(bounds, append([]byte(key), 0), append([]byte(key), 255))
	}
	for _, bound := range bounds {
		ge := sort.SearchStrings(keys, string(bound))
		r, ok := tree.seekGE(bound, nil)
		if ok != (ge < len(keys)) || (ok && r.Key != keys[ge]) {
			t.Fatalf("GE %x=(%x,%v), ordinal %d", bound, r.Key, ok, ge)
		}
		lt := ge - 1
		if bound == nil {
			lt = len(keys) - 1
		}
		r, ok = tree.seekLT(nil, bound)
		if ok != (lt >= 0) || (ok && r.Key != keys[lt]) {
			t.Fatalf("LT %x=(%x,%v), ordinal %d", bound, r.Key, ok, lt)
		}
		cursor := tree.cursor()
		ok = cursor.seekGE(bound)
		r, valid := cursor.current()
		if ok != valid || ok != (ge < len(keys)) || (ok && r.Key != keys[ge]) {
			t.Fatalf("cursor GE %x=(%x,%v)", bound, r.Key, ok)
		}
		le := sort.Search(len(keys), func(i int) bool { return keys[i] > string(bound) }) - 1
		ok = cursor.seekLE(bound)
		r, valid = cursor.current()
		if ok != valid || ok != (le >= 0) || (ok && r.Key != keys[le]) {
			t.Fatalf("cursor LE %x=(%x,%v), ordinal %d", bound, r.Key, ok, le)
		}
		cursor.clear()
	}
	var scanned []string
	tree.scanOwned(func(r COWRecord) bool { scanned = append(scanned, r.Key); return true })
	if !equalCOWOrderedKeys(scanned, keys) {
		t.Fatal("scan order")
	}
	cursor := tree.cursor()
	index := 0
	for valid := cursor.first(); valid; valid = cursor.next() {
		r, _ := cursor.current()
		if index >= len(keys) || r.Key != keys[index] {
			t.Fatal("forward cursor", index, r.Key)
		}
		index++
	}
	if index != len(keys) {
		t.Fatal("forward cardinality", index)
	}
	index = len(keys) - 1
	for valid := cursor.last(); valid; valid = cursor.prev() {
		r, _ := cursor.current()
		if index < 0 || r.Key != keys[index] {
			t.Fatal("reverse cursor", index, r.Key)
		}
		index--
	}
	if index != -1 {
		t.Fatal("reverse cardinality", index)
	}
}

func equalCOWOrderedKeys(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestCOWOrderedStorageAtomicHistoryPostimagesAndRouting(t *testing.T) {
	low := cowOrderedStorageMutation(t, "shared\x00", 50)
	high := cowOrderedStorageMutation(t, "shared\x00", 100)
	basis := cowOrderedStorageBuild(t, []COWMutation{low, high})
	replacement := low
	replacement.MVCC = false
	replacement.Value = []byte("replacement")
	erase := high
	erase.MVCC = false
	erase.Remove = true
	reinsert := high
	reinsert.MVCC = false
	reinsert.Value = []byte("after physical erase")
	backfill := cowOrderedStorageMutation(t, "shared\x00", 75)
	mutations := []COWMutation{erase, replacement, backfill, reinsert}
	if _, err := basis.estimate(mutations); err != nil {
		t.Fatal(err)
	}
	private := basis.fork()
	private.applyFrom(basis, mutations)
	fresh := private.immutableCopy()
	if r, ok := fresh.get(high.Key); !ok || r.Value != "after physical erase" {
		t.Fatal("last-wins remove/reinsert", r, ok)
	}
	if r, ok := basis.get(high.Key); !ok || r.Value != string(high.Value) {
		t.Fatal("old postimage changed", r, ok)
	}
	if fresh.len() != 3 || basis.len() != 2 {
		t.Fatal("version cardinality")
	}
	if r, ok := fresh.seekGE(backfill.Key, nil); !ok || r.Value != string(backfill.Value) {
		t.Fatal("backfill missing")
	}
	empty := newCOWOrderedTree()
	qualified := cowOrderedStorageMutation(t, "new", 10)
	raw := qualified
	raw.MVCC = false
	raw.Value = []byte("raw override")
	group := []COWMutation{qualified, raw}
	plan := empty.routing(group)
	if !plan.routes(0) || !plan.routes(1) {
		t.Fatal("same-group qualification")
	}
	var absent *cowOrderedTree
	if absent.routing(group).routes(0) {
		t.Fatal("raw mode interpreted qualification")
	}
	private = empty.fork()
	private.applyFrom(empty, group)
	if r, ok := private.get(raw.Key); !ok || r.Value != "raw override" {
		t.Fatal("same-group last wins", r, ok)
	}
	invalid := qualified
	invalid.Timestamp++
	if _, err := empty.estimate([]COWMutation{invalid}); err == nil {
		t.Fatal("mismatched timestamp accepted")
	}
	malformed := qualified
	malformed.Key = append(append([]byte(nil), malformed.Key...), 0)
	if _, err := empty.estimate([]COWMutation{malformed}); err == nil {
		t.Fatal("malformed qualification accepted")
	}
}

func TestCOWOrderedCursorReusesAdmittedStacks(t *testing.T) {
	var entries []COWMutation
	for key := 0; key < 80; key++ {
		versions := 1
		if key%2 == 0 {
			versions = 80
		}
		for version := 1; version <= versions; version++ {
			entries = append(entries, cowOrderedStorageMutation(t, fmt.Sprintf("%s-%03d", strings.Repeat("prefix", 100), key), uint64(version)))
		}
	}
	tree := cowOrderedStorageBuild(t, entries)
	cursor := tree.cursor()
	scan := func() {
		for valid := cursor.first(); valid; valid = cursor.next() {
		}
		for valid := cursor.last(); valid; valid = cursor.prev() {
		}
	}
	scan() // first growth is covered by cowOrderedCursorStackCharge
	if allocations := testing.AllocsPerRun(3, scan); allocations != 0 {
		t.Fatalf("repeated scans allocate after their admitted stacks grew: %g", allocations)
	}
	seek := func() {
		for i := range entries {
			if !cursor.seekGE(entries[i].Key) {
				panic("prepared seek missing")
			}
			if !cursor.seekLE(entries[i].Key) {
				panic("prepared reverse seek missing")
			}
			if _, ok := tree.get(entries[i].Key); !ok {
				panic("prepared exact lookup missing")
			}
		}
	}
	seek()
	if allocations := testing.AllocsPerRun(3, seek); allocations != 0 {
		t.Fatalf("repeated seeks allocate after their admitted stacks grew: %g", allocations)
	}
	cursor.clear()
}

func cowOrderedStorageRequest(t *testing.T, c *cowOrderedTraversal, op COWTraversalOperation, key string) (COWRecord, bool) {
	t.Helper()
	work := COWTraversalWork{RecordLimit: 3, ByteLimit: 2048}
	if err := c.Begin(op, key, &work); err != nil {
		t.Fatal(err)
	}
	for rounds := 0; rounds < 100000; rounds++ {
		done, err := c.Step(&work)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			return c.Current()
		}
		if c.Ready() {
			t.Fatal("partial ordered result escaped")
		}
		work = COWTraversalWork{RecordLimit: 3, ByteLimit: 2048}
	}
	t.Fatal("ordered traversal failed to progress")
	return COWRecord{}, false
}

func TestCOWOrderedResumableTraversalMatchesPhysicalOracle(t *testing.T) {
	var entries []COWMutation
	for i := 0; i < 70; i++ {
		for _, ts := range []uint64{1, 2, 19, 100, math.MaxUint64} {
			entries = append(entries, cowOrderedStorageMutation(t, fmt.Sprintf("key%03d", i), ts))
		}
	}
	entries = append(entries, cowOrderedStorageMutation(t, strings.Repeat("long", 2000)+"\x00a", 1))
	tree := cowOrderedStorageBuild(t, entries)
	keys := cowOrderedStorageOracle(entries)
	work := COWTraversalWork{RecordLimit: 1000, ByteLimit: 1 << 20}
	c, ok := newCOWOrderedTraversal(tree, &work)
	if !ok {
		t.Fatal("constructor refused")
	}
	for i, key := range keys {
		r, valid := cowOrderedStorageRequest(t, &c, btree.SeekGE, key)
		if !valid || r.Key != key {
			t.Fatal("seekGE", i, r.Key)
		}
		r, valid = cowOrderedStorageRequest(t, &c, btree.SeekLE, key)
		if !valid || r.Key != key {
			t.Fatal("seekLE", i, r.Key)
		}
	}
	index := 0
	r, valid := cowOrderedStorageRequest(t, &c, btree.First, "")
	for valid {
		if index >= len(keys) || r.Key != keys[index] {
			t.Fatal("resumable forward", index, r.Key)
		}
		index++
		r, valid = cowOrderedStorageRequest(t, &c, btree.Next, "")
	}
	if index != len(keys) {
		t.Fatal("forward cardinality", index)
	}
	index = len(keys) - 1
	r, valid = cowOrderedStorageRequest(t, &c, btree.Last, "")
	for valid {
		if index < 0 || r.Key != keys[index] {
			t.Fatal("resumable reverse", index, r.Key)
		}
		index--
		r, valid = cowOrderedStorageRequest(t, &c, btree.Prev, "")
	}
	if index != -1 {
		t.Fatal("reverse cardinality", index)
	}
	for _, key := range keys[:20] {
		for _, bound := range [][]byte{[]byte(key[:len(key)-8]), append([]byte(key), 0), []byte(key[:len(key)-3])} {
			ge := sort.SearchStrings(keys, string(bound))
			r, valid = cowOrderedStorageRequest(t, &c, btree.SeekGE, string(bound))
			if valid != (ge < len(keys)) || (valid && r.Key != keys[ge]) {
				t.Fatalf("resumable GE %x=%x", bound, r.Key)
			}
			le := sort.Search(len(keys), func(i int) bool { return bytes.Compare([]byte(keys[i]), bound) > 0 }) - 1
			r, valid = cowOrderedStorageRequest(t, &c, btree.SeekLE, string(bound))
			if valid != (le >= 0) || (valid && r.Key != keys[le]) {
				t.Fatalf("resumable LE %x=%x", bound, r.Key)
			}
		}
	}
	work = COWTraversalWork{RecordLimit: 1, ByteLimit: 2048}
	if err := c.Begin(btree.Close, "", &work); err != nil {
		t.Fatal(err)
	}
	for rounds := 0; rounds < 1000; rounds++ {
		work = COWTraversalWork{RecordLimit: 3, ByteLimit: 2048}
		done, err := c.Step(&work)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			if !c.Closed() || c.Ready() {
				t.Fatal("close aliases retained")
			}
			return
		}
	}
	t.Fatal("close made no progress")
}
