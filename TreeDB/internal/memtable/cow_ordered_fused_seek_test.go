package memtable

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"
)

// Check the movement stacks, not only the record selected by a seek. In
// particular, a lower-bound miss must not leave the primary or numeric cursor
// positioned at the neighboring key used to discover the answer.
func TestCOWOrderedFusedSeekRetainsMovementPosition(t *testing.T) {
	var entries []COWMutation
	for i := 0; i < 96; i++ {
		logical := fmt.Sprintf("%s%03d\x00", strings.Repeat("prefix", i%3*8), i)
		for _, timestamp := range []uint64{1, 9, 1000, math.MaxUint64} {
			if i%3 == 0 && timestamp != 9 {
				continue // repeatedly cross singleton/numeric history boundaries
			}
			entries = append(entries, cowOrderedStorageMutation(t, logical, timestamp))
		}
	}
	tree := cowOrderedStorageBuild(t, entries)
	keys := cowOrderedStorageOracle(entries)
	bounds := [][]byte{nil, {}, {0}, {255}}
	for _, key := range keys {
		for _, n := range []int{0, 1, len(key) - 9, len(key) - 8, len(key) - 1, len(key)} {
			bounds = append(bounds, []byte(key[:n]))
		}
		bounds = append(bounds, append([]byte(key), 0), append([]byte(key), 255))
	}
	rng := rand.New(rand.NewSource(73021))
	for i := 0; i < 300; i++ {
		key := keys[rng.Intn(len(keys))]
		bound := []byte(key)
		bound[rng.Intn(len(bound))] = byte(rng.Intn(256))
		bounds = append(bounds, bound)
	}
	rng.Shuffle(len(bounds), func(i, j int) { bounds[i], bounds[j] = bounds[j], bounds[i] })
	cursor := tree.cursor()
	check := func(index int, valid bool) {
		t.Helper()
		record, found := cursor.current()
		want := index >= 0 && index < len(keys)
		if valid != want || found != want || (want && record.Key != keys[index]) {
			t.Fatalf("index=%d valid=%t found=%t key=%x", index, valid, found, record.Key)
		}
	}
	for _, bound := range bounds {
		ge := sort.SearchStrings(keys, string(bound))
		le := sort.Search(len(keys), func(i int) bool { return keys[i] > string(bound) }) - 1
		check(ge, cursor.seekGE(bound))
		if ge < len(keys) {
			check(ge+1, cursor.next())
		}
		check(le, cursor.seekLE(bound))
		if le >= 0 {
			check(le-1, cursor.prev())
		}
		// Alternate direction on the same cursor, including after both end misses.
		check(ge, cursor.seekGE(bound))
		if ge < len(keys) {
			check(ge-1, cursor.prev())
		}
		check(le, cursor.seekLE(bound))
		if le >= 0 {
			check(le+1, cursor.next())
		}
	}
	// A borrowed external seek pivot is consumed synchronously; changing it
	// cannot alter current records or subsequent movement in the pinned tree.
	for i := 0; i < len(keys); i += 11 {
		pivot := []byte(keys[i])
		check(i, cursor.seekGE(pivot))
		clear(pivot)
		check(i, true)
		check(i+1, cursor.next())
	}
	// Existing aliases belong to the old root even after private replacement.
	if !cursor.first() {
		t.Fatal("empty cursor")
	}
	old, _ := cursor.current()
	private := tree.fork()
	replacement := entries[0]
	replacement.Value = bytes.Repeat([]byte("new"), 20)
	private.applyFrom(tree, []COWMutation{replacement})
	if old.Key != keys[0] {
		t.Fatal("old alias changed")
	}
	check(0, cursor.seekGE([]byte(keys[0])))
}
