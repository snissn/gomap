package page

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func TestLeafInsertFillAndSearchOrder(t *testing.T) {
	buf := make([]byte, PageSize)
	lp, err := InitLeafPage(buf, 1)
	if err != nil {
		t.Fatalf("InitLeafPage: %v", err)
	}

	// Generate keys and shuffle to ensure directory insertion ordering works.
	keys := make([][]byte, 256)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%04d", i))
	}
	r := rand.New(rand.NewSource(1))
	r.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	inserted := make([][]byte, 0, len(keys))
	for _, k := range keys {
		_, err := lp.Set(k, LeafFlagInline, []byte("val"), ValuePtr{})
		if err != nil {
			if err == ErrPageFull {
				break
			}
			t.Fatalf("Set %q: %v", k, err)
		}
		inserted = append(inserted, k)
	}
	if lp.Count() != len(inserted) {
		t.Fatalf("count mismatch: got %d want %d", lp.Count(), len(inserted))
	}

	// Directory must be sorted by key.
	dirKeys := make([][]byte, lp.Count())
	for i := 0; i < lp.Count(); i++ {
		k, err := lp.KeyAt(i)
		if err != nil {
			t.Fatalf("KeyAt(%d): %v", i, err)
		}
		dirKeys[i] = k
	}
	sorted := append([][]byte(nil), dirKeys...)
	sort.Slice(sorted, func(i, j int) bool { return bytes.Compare(sorted[i], sorted[j]) < 0 })
	for i := range dirKeys {
		if !bytes.Equal(dirKeys[i], sorted[i]) {
			t.Fatalf("directory not sorted at %d: got %q want %q", i, dirKeys[i], sorted[i])
		}
	}

	// Search for every inserted key should succeed at correct position.
	for _, k := range inserted {
		idx, found, err := lp.Search(k)
		if err != nil {
			t.Fatalf("Search %q: %v", k, err)
		}
		if !found {
			t.Fatalf("Search %q not found", k)
		}
		got, err := lp.KeyAt(idx)
		if err != nil {
			t.Fatalf("KeyAt(%d): %v", idx, err)
		}
		if !bytes.Equal(got, k) {
			t.Fatalf("Search position mismatch: key %q at idx %d has %q", k, idx, got)
		}
	}
}

func TestLeafUpdateInPlaceAndRelocate(t *testing.T) {
	buf := make([]byte, PageSize)
	lp, err := InitLeafPage(buf, 2)
	if err != nil {
		t.Fatalf("InitLeafPage: %v", err)
	}
	key := []byte("alpha")
	valBig := bytes.Repeat([]byte{0x01}, 40)
	_, err = lp.Set(key, LeafFlagInline, valBig, ValuePtr{})
	if err != nil {
		t.Fatalf("Set initial: %v", err)
	}
	idx, found, err := lp.Search(key)
	if err != nil || !found {
		t.Fatalf("Search initial: found=%v err=%v", found, err)
	}
	oldOff, _ := dirEntry(lp.body, lp.Count(), idx)
	oldTop := heapTop(lp.body)
	oldFree := lp.FreeSpace()

	// Smaller update should be in-place.
	valSmall := bytes.Repeat([]byte{0x02}, 10)
	ins, err := lp.Set(key, LeafFlagInline, valSmall, ValuePtr{})
	if err != nil || ins {
		t.Fatalf("Set small update: ins=%v err=%v", ins, err)
	}
	newOff, _ := dirEntry(lp.body, lp.Count(), idx)
	if newOff != oldOff {
		t.Fatalf("in-place update changed offset: got %d want %d", newOff, oldOff)
	}
	if heapTop(lp.body) != oldTop {
		t.Fatalf("in-place update changed heapTop: got %d want %d", heapTop(lp.body), oldTop)
	}
	if lp.FreeSpace() != oldFree {
		t.Fatalf("in-place update changed free space: got %d want %d", lp.FreeSpace(), oldFree)
	}

	// Larger update should relocate.
	valLarger := bytes.Repeat([]byte{0x03}, 80)
	_, err = lp.Set(key, LeafFlagInline, valLarger, ValuePtr{})
	if err != nil {
		t.Fatalf("Set large update: %v", err)
	}
	relocOff, _ := dirEntry(lp.body, lp.Count(), idx)
	if relocOff == oldOff {
		t.Fatalf("expected relocation, offset unchanged")
	}
	if heapTop(lp.body) <= oldTop {
		t.Fatalf("expected heapTop to grow on relocation")
	}
}

func TestLeafDeleteDefragAndReuseSpace(t *testing.T) {
	buf := make([]byte, PageSize)
	lp, err := InitLeafPage(buf, 3)
	if err != nil {
		t.Fatalf("InitLeafPage: %v", err)
	}

	// Insert a bunch of keys.
	var keys [][]byte
	for i := 0; i < 128; i++ {
		k := []byte(fmt.Sprintf("k%03d", i))
		_, err := lp.Set(k, LeafFlagInline, []byte("value-xxxx"), ValuePtr{})
		if err != nil {
			if err == ErrPageFull {
				break
			}
			t.Fatalf("Set %q: %v", k, err)
		}
		keys = append(keys, k)
	}
	if len(keys) < 10 {
		t.Fatalf("expected some keys inserted")
	}

	freeBefore := lp.FreeSpace()

	// Delete every other key.
	for i, k := range keys {
		if i%2 == 0 {
			ok, err := lp.Delete(k)
			if err != nil || !ok {
				t.Fatalf("Delete %q: ok=%v err=%v", k, ok, err)
			}
			idx, found, _ := lp.Search(k)
			if !found {
				t.Fatalf("deleted key %q missing from directory", k)
			}
			flags, err := lp.FlagsAt(idx)
			if err != nil {
				t.Fatalf("FlagsAt: %v", err)
			}
			if flags != LeafFlagTombstone {
				t.Fatalf("expected tombstone for %q, got %d", k, flags)
			}
		}
	}
	if lp.FreeSpace() != freeBefore {
		t.Fatalf("free space changed without defrag: got %d want %d", lp.FreeSpace(), freeBefore)
	}

	// Defragment should reclaim space (tombstones shrink).
	topBefore := heapTop(lp.body)
	if err := lp.Defragment(); err != nil {
		t.Fatalf("Defragment: %v", err)
	}
	if heapTop(lp.body) >= topBefore {
		t.Fatalf("expected heapTop to shrink after defrag")
	}
	if lp.FreeSpace() <= freeBefore {
		t.Fatalf("expected more free space after defrag")
	}

	// Directory order must remain sorted.
	for i := 1; i < lp.Count(); i++ {
		prev, _ := lp.KeyAt(i - 1)
		cur, _ := lp.KeyAt(i)
		if bytes.Compare(prev, cur) >= 0 {
			t.Fatalf("directory order broken after defrag at %d", i)
		}
	}

	// Reuse freed space by inserting new keys.
	added := 0
	for i := 0; i < 64; i++ {
		k := []byte(fmt.Sprintf("z%03d", i))
		_, err := lp.Set(k, LeafFlagInline, []byte("v"), ValuePtr{})
		if err != nil {
			if err == ErrPageFull {
				break
			}
			t.Fatalf("Set new %q: %v", k, err)
		}
		added++
	}
	if added == 0 {
		t.Fatalf("expected inserts to reuse reclaimed space")
	}
}

func TestLeafDefragAllowsFurtherInserts(t *testing.T) {
	buf := make([]byte, PageSize)
	lp, err := InitLeafPage(buf, 4)
	if err != nil {
		t.Fatalf("InitLeafPage: %v", err)
	}

	// Fill with small values.
	var inserted int
	for i := 0; i < 256; i++ {
		k := []byte(fmt.Sprintf("m%03d", i))
		_, err := lp.Set(k, LeafFlagInline, []byte("vvv"), ValuePtr{})
		if err != nil {
			if err == ErrPageFull {
				break
			}
			t.Fatalf("Set %q: %v", k, err)
		}
		inserted++
	}
	// Delete a range to create dead bytes.
	for i := 0; i < inserted; i += 3 {
		k := []byte(fmt.Sprintf("m%03d", i))
		_, _ = lp.Delete(k)
	}

	// Without defrag, this should often be full.
	_, err = lp.Set([]byte("extra-a"), LeafFlagInline, []byte("large-value-to-force-reuse"), ValuePtr{})
	if err == nil {
		return
	}
	if err != ErrPageFull {
		t.Fatalf("expected ErrPageFull before defrag, got %v", err)
	}

	// After defrag, insert should succeed (space reclaimed).
	if err := lp.Defragment(); err != nil {
		t.Fatalf("Defragment: %v", err)
	}
	if _, err := lp.Set([]byte("extra-a"), LeafFlagInline, []byte("large-value-to-force-reuse"), ValuePtr{}); err != nil {
		t.Fatalf("Set after defrag: %v", err)
	}
}

func TestInternalPageSinglePageOps(t *testing.T) {
	buf := make([]byte, PageSize)
	ip, err := InitInternalPage(buf, 10)
	if err != nil {
		t.Fatalf("InitInternalPage: %v", err)
	}

	keys := [][]byte{[]byte("delta"), []byte("alpha"), []byte("charlie"), []byte("bravo")}
	for i, k := range keys {
		ins, err := ip.Set(k, PageID(i+1))
		if err != nil || !ins {
			t.Fatalf("Set %q: ins=%v err=%v", k, ins, err)
		}
	}

	// Keys should be sorted in directory.
	for i := 1; i < ip.Count(); i++ {
		prev, _ := ip.KeyAt(i - 1)
		cur, _ := ip.KeyAt(i)
		if bytes.Compare(prev, cur) >= 0 {
			t.Fatalf("internal directory not sorted at %d", i)
		}
	}

	// Search and update child in place.
	idx, found, err := ip.Search([]byte("bravo"))
	if err != nil || !found {
		t.Fatalf("Search bravo: found=%v err=%v", found, err)
	}
	_, err = ip.Set([]byte("bravo"), 99)
	if err != nil {
		t.Fatalf("Set update bravo: %v", err)
	}
	if err := ip.CompareChild(idx, []byte("bravo"), 99); err != nil {
		t.Fatalf("CompareChild: %v", err)
	}

	// Delete an entry.
	ok, err := ip.Delete([]byte("charlie"))
	if err != nil || !ok {
		t.Fatalf("Delete charlie: ok=%v err=%v", ok, err)
	}
	_, found, _ = ip.Search([]byte("charlie"))
	if found {
		t.Fatalf("expected charlie deleted")
	}
}
