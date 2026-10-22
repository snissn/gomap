package memtable

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
)

func cowOrderedPrefixReuseEntries(t *testing.T) []COWMutation {
	t.Helper()
	var entries []COWMutation
	for _, logical := range []string{"", "\x00", "a", "a\x00b", "\xff"} {
		// Exactly 64 versions, spanning more than one numeric leaf. Gaps and
		// the maximum timestamp distinguish predecessor lookup from equality.
		for ts := uint64(2); ts <= 126; ts += 2 {
			entries = append(entries, cowOrderedStorageMutation(t, logical, ts))
		}
		entries = append(entries, cowOrderedStorageMutation(t, logical, math.MaxUint64))
	}
	for i := 0; i < 80; i++ {
		entries = append(entries, cowOrderedStorageMutation(t, fmt.Sprintf("middle-%03d", i), 7))
	}
	return entries
}

func cowOrderedPrefixReuseCompare(t *testing.T, cursor *cowOrderedCursor, tree *cowOrderedTree, bound []byte) {
	t.Helper()
	// A fresh cursor is never eligible for reuse and therefore exercises the
	// original complete primary search, including malformed-bound handling.
	fresh := tree.cursor()
	got, want := cursor.seekGE(bound), fresh.seekGE(bound)
	check := func(step string) {
		t.Helper()
		actual, actualOK := cursor.current()
		expected, expectedOK := fresh.current()
		if got != want || actualOK != expectedOK || actualOK != got || (got && actual != expected) {
			t.Fatalf("%s bound %x: got %+v/%v want %+v/%v", step, bound, actual, got, expected, want)
		}
	}
	check("seek")
	// Check both retained stacks after positioning, not only the first record.
	got, want = cursor.next(), fresh.next()
	check("next")
	got, want = cursor.prev(), fresh.prev()
	check("prev")
	got, want = cursor.prev(), fresh.prev()
	check("prev again")
}

func TestCOWOrderedPrefixReuseMatchesFreshSeek(t *testing.T) {
	tree := cowOrderedStorageBuild(t, cowOrderedPrefixReuseEntries(t))
	if tree.height() < 2 {
		t.Fatal("fixture must have a primary directory")
	}
	cursor := tree.cursor()
	for _, logical := range []string{"", "\x00", "a", "a\x00b", "\xff", "middle-079"} {
		anchor := cowOrderedStorageMutation(t, logical, 64).Key
		bounds := [][]byte{nil, {}, {0}, {255}}
		for _, ts := range []uint64{math.MaxUint64, math.MaxUint64 - 1, 129, 128, 127, 126, 97, 64, 63, 2, 1} {
			bounds = append(bounds, cowOrderedStorageMutation(t, logical, ts).Key)
		}
		prefixLen := len(anchor) - 8
		for _, n := range []int{1, prefixLen - 1, prefixLen, prefixLen + 1, len(anchor) - 1} {
			bounds = append(bounds, bytes.Clone(anchor[:n]))
		}
		bounds = append(bounds, append(bytes.Clone(anchor), 0), append(bytes.Clone(anchor), 255))
		zeroTS := bytes.Clone(anchor)
		for i := prefixLen; i < len(zeroTS); i++ {
			zeroTS[i] = 255
		}
		malformed := bytes.Clone(anchor)
		malformed[1] ^= 1 // invalid namespace, same length as a canonical key
		bounds = append(bounds, zeroTS, malformed)
		for _, bound := range bounds {
			if !cursor.seekGE(anchor) {
				t.Fatal("anchor missing")
			}
			cowOrderedPrefixReuseCompare(t, &cursor, tree, bound)
			// A reverse position must also support a subsequent forward seek
			// within its current history, irrespective of numeric stack direction.
			cursor.seekLE(anchor)
			cowOrderedPrefixReuseCompare(t, &cursor, tree, bound)
		}
		cursor.last()
		cursor.next() // exhausted with a retained last-history pointer
		cowOrderedPrefixReuseCompare(t, &cursor, tree, anchor)
		cursor.first()
		cursor.prev() // exhausted in the other direction
		cowOrderedPrefixReuseCompare(t, &cursor, tree, anchor)
	}
	// Repeated deep jumps reuse only the already admitted stack backing.
	hi := cowOrderedStorageMutation(t, "a\x00b", 125).Key
	lo := cowOrderedStorageMutation(t, "a\x00b", 3).Key
	seek := func() {
		if !cursor.seekGE(hi) || !cursor.seekGE(lo) {
			panic("deep same-prefix seek missing")
		}
	}
	seek()
	if allocations := testing.AllocsPerRun(5, seek); allocations != 0 {
		t.Fatalf("warmed same-prefix seeks allocate: %g", allocations)
	}
}

func TestCOWOrderedPrefixReuseRetainedCutAndRawBounds(t *testing.T) {
	budget, writer := cowOrderedLifecycleWriter(t)
	entries := cowOrderedPrefixReuseEntries(t)
	anchor := cowOrderedStorageMutation(t, "a\x00b", 64)
	anchor.Flags = node.FlagTombstone
	entries = append(entries, anchor)
	raw := COWMutation{Key: append(bytes.Clone(anchor.Key), 0), Value: []byte("raw extension"), Flags: node.FlagInline}
	entries = append(entries, raw)
	old := cowTestPublish(t, writer, entries)
	end := cowOrderedStorageMutation(t, "\xff", 1).Key
	cursor, err := old.Cursor(nil, end)
	if err != nil {
		t.Fatal(err)
	}
	reverse, err := old.ReverseCursorWithExtraBytes(nil, end, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := make(map[string]COWRecord)
	old.ScanOwned(func(r COWRecord) bool { want[r.Key] = r; return true })
	keys := make([]string, 0, len(want))
	for key := range want {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if err := cursor.Seek(anchor.Key); err != nil {
		t.Fatal(err)
	}
	alias, ok, err := cursor.Record()
	if err != nil || !ok || alias.Flags != node.FlagTombstone {
		t.Fatal("physical mask not retained", alias, ok, err)
	}
	// Rewrite and erase the current history, and force new primary splits.
	replacement := anchor
	replacement.Value, replacement.Flags = []byte("new cut"), node.FlagInline
	erase := cowOrderedStorageMutation(t, "a\x00b", 126)
	erase.Remove = true
	mutations := []COWMutation{replacement, erase, cowOrderedStorageMutation(t, "a\x00b", 65)}
	for i := 0; i < 80; i++ {
		mutations = append(mutations, cowOrderedStorageMutation(t, fmt.Sprintf("new-%03d", i), 1))
	}
	current := cowTestPublish(t, writer, mutations)
	cowTestRelease(old)
	cowTestRelease(current)
	cowTestClose(writer)
	budget.Close()
	// Only the independent cursors own the opening cut now. Compare every
	// version, holes, raw extensions and bounded end against its owned records.
	bounds := [][]byte{anchor.Key, raw.Key, append(bytes.Clone(raw.Key), 0), nil, end, {255}}
	for ts := uint64(1); ts <= 129; ts++ {
		bounds = append(bounds, cowOrderedStorageMutation(t, "a\x00b", ts).Key)
	}
	for _, bound := range bounds {
		if err := cursor.Seek(bound); err != nil {
			t.Fatal(err)
		}
		index := sort.SearchStrings(keys, string(bound))
		present := index < len(keys) && keys[index] < string(end)
		got, found, err := cursor.Record()
		if err != nil || found != present || (found && got != want[keys[index]]) {
			t.Fatalf("retained forward %x: %+v/%v/%v", bound, got, found, err)
		}
		if err := reverse.Seek(bound); err != nil {
			t.Fatal(err)
		}
		limit := string(bound)
		if bound == nil || limit >= string(end) {
			index = sort.SearchStrings(keys, string(end)) - 1
		} else {
			index = sort.Search(len(keys), func(i int) bool { return keys[i] > limit }) - 1
		}
		got, found, err = reverse.Record()
		if err != nil || found != (index >= 0) || (found && got != want[keys[index]]) {
			t.Fatalf("retained reverse %x: %+v/%v/%v", bound, got, found, err)
		}
	}
	if alias != want[string(anchor.Key)] {
		t.Fatal("cursor movement or newer publication changed an old alias")
	}
	cursor.Close()
	reverse.Close()
	if stats := budget.Stats(); stats.TotalBytes != 0 || stats.Generations != 0 || stats.Views != 0 {
		t.Fatalf("last cursor owner leaked: %+v", stats)
	}
}

func TestCOWOrderedPrefixReuseKeepsRawTiePriority(t *testing.T) {
	// Duplicate raw/ordered keys are normally migrated before publication.
	// Exercise the selector's existing raw-mask priority explicitly, with both
	// source roots retained, so route reuse cannot become suppression authority.
	budget, rawWriter := cowTestWriter(t, DefaultCOWLimits())
	mask := cowOrderedStorageMutation(t, "a\x00b", 64)
	mask.MVCC, mask.Timestamp, mask.Flags = false, 0, node.FlagTombstone
	rawRoot := cowTestPublish(t, rawWriter, []COWMutation{mask})
	tree := cowOrderedStorageBuild(t, cowOrderedPrefixReuseEntries(t))
	cursor := COWCursor{iter: rawRoot.tree.Iter(), ordered: tree.cursor()}
	for _, ts := range []uint64{126, 64, 65, 64, math.MaxUint64, 64} {
		bound := cowOrderedStorageMutation(t, "a\x00b", ts).Key
		cursor.rawValid = cowIteratorSeek(rawRoot.tree, &cursor.iter, bound)
		cursor.orderedValid = cursor.ordered.seekGE(bound)
		cursor.selectRecord()
		fresh := tree.cursor()
		fresh.seekGE(bound)
		ordered, _ := fresh.current()
		expected := ordered
		if record, ok := rawRoot.SeekGE(bound, nil); ok && record.Key <= ordered.Key {
			expected = record
		}
		if !cursor.valid || cursor.record != expected || (ts == 64 && (!cursor.rawSelected || cursor.record.Flags != node.FlagTombstone)) {
			t.Fatalf("raw tie @%d: got %+v want %+v", ts, cursor.record, expected)
		}
	}
	cowTestRelease(rawRoot)
	cowTestClose(rawWriter)
	budget.Close()
}
