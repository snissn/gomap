package node

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestLeafNode(t *testing.T) {
	data := make([]byte, page.PageSize)
	n := NewNode(data)
	n.SetType(page.PageTypeLeaf)
	n.SetPageID(1)

	// Add entries out of order
	keys := [][]byte{
		[]byte("key3"),
		[]byte("key1"),
		[]byte("key2"),
	}
	vals := [][]byte{
		[]byte("val3"),
		[]byte("val1"),
		[]byte("val2"),
	}

	for i, k := range keys {
		err := n.AddLeafEntry(k, vals[i], FlagInline, page.ValuePtr{})
		if err != nil {
			t.Fatalf("AddLeafEntry failed: %v", err)
		}
	}

	if n.Count() != 3 {
		t.Errorf("Expected 3 items, got %d", n.Count())
	}

	// Verify Sorted Order
	expectedKeys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	for i := uint16(0); i < 3; i++ {
		entry, err := n.GetLeafEntry(i)
		if err != nil {
			t.Fatalf("GetLeafEntry(%d) failed: %v", i, err)
		}
		if !bytes.Equal(entry.Key, expectedKeys[i]) {
			t.Errorf("Index %d: expected key %s, got %s", i, expectedKeys[i], entry.Key)
		}
	}

	// Verify Search
	idx, found, err := n.SearchLeaf([]byte("key2"))
	if err != nil {
		t.Fatalf("Search key2: %v", err)
	}
	if !found || idx != 1 {
		t.Errorf("Search key2: expected found=true idx=1, got found=%v idx=%d", found, idx)
	}

	idx, found, err = n.SearchLeaf([]byte("key1.5"))
	if err != nil {
		t.Fatalf("Search key1.5: %v", err)
	}
	if found || idx != 1 {
		t.Errorf("Search key1.5: expected found=false idx=1, got found=%v idx=%d", found, idx)
	}
}

func TestLeafNodePrefixCompression(t *testing.T) {
	data := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(data, page.PageTypeLeaf, BuilderOptions{LeafPrefixCompression: true})
	b.SetPageID(1)

	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key9"),
	}
	for _, k := range keys {
		if err := b.AddLeafEntry(k, []byte("val"+string(k)), FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("AddLeafEntry failed: %v", err)
		}
	}
	n := b.Finish()

	if !n.leafPrefixCompressed() {
		t.Fatalf("expected leaf prefix compression flag")
	}
	if !n.leafPrefixV2() {
		t.Fatalf("expected leaf prefix v2 flag on newly-built pages")
	}
	if n.Count() != uint16(len(keys)) {
		t.Fatalf("expected %d items, got %d", len(keys), n.Count())
	}

	for i, k := range keys {
		entry, err := n.GetLeafEntry(uint16(i))
		if err != nil {
			t.Fatalf("GetLeafEntry(%d) failed: %v", i, err)
		}
		if !bytes.Equal(entry.Key, k) {
			t.Fatalf("entry %d key mismatch: %q != %q", i, entry.Key, k)
		}
	}

	idx, found, err := n.SearchLeaf([]byte("key2"))
	if err != nil {
		t.Fatalf("SearchLeaf key2 failed: %v", err)
	}
	if !found || idx != 1 {
		t.Fatalf("SearchLeaf key2 expected idx=1 found=true, got idx=%d found=%v", idx, found)
	}

	if err := n.AddLeafEntry([]byte("key2"), []byte("updated"), FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("AddLeafEntry update failed: %v", err)
	}
	updated, err := n.GetLeafEntry(1)
	if err != nil {
		t.Fatalf("GetLeafEntry after update failed: %v", err)
	}
	if !bytes.Equal(updated.Value, []byte("updated")) {
		t.Fatalf("expected updated value, got %q", updated.Value)
	}
}

func TestLeafNodePrefixCompression_SearchMatchesSortedKeys_V1AndV2(t *testing.T) {
	keys := makeBenchKeys(48, 16)

	makeQueries := func() [][]byte {
		queries := make([][]byte, 0, len(keys)+256)
		queries = append(queries, keys...)

		rng := rand.New(rand.NewSource(2))
		for i := 0; i < 256; i++ {
			q := make([]byte, benchKeySize)
			for j := 0; j < 16 && j < benchKeySize; j++ {
				q[j] = 0x42
			}
			_, _ = rng.Read(q[16:])
			queries = append(queries, q)
		}
		return queries
	}
	queries := makeQueries()

	expectSearch := func(n *Node, q []byte) (idx uint16, found bool) {
		for i, k := range keys {
			cmp := bytes.Compare(k, q)
			if cmp >= 0 {
				return uint16(i), cmp == 0
			}
		}
		return uint16(len(keys)), false
	}

	buildNode := func(v2 bool) *Node {
		data := make([]byte, page.PageSize)
		b := NewBuilderWithOptions(data, page.PageTypeLeaf, BuilderOptions{LeafPrefixCompression: true})
		b.SetPageID(1)
		b.leafPrefixV2 = v2
		for _, k := range keys {
			val := append([]byte("val:"), k...)
			if err := b.AddLeafEntry(k, val, FlagInline, page.ValuePtr{}); err != nil {
				t.Fatalf("AddLeafEntry failed: %v", err)
			}
		}
		return b.Finish()
	}

	for _, v2 := range []bool{false, true} {
		n := buildNode(v2)
		if !n.leafPrefixCompressed() {
			t.Fatalf("expected prefix compression enabled (v2=%v)", v2)
		}
		if n.leafPrefixV2() != v2 {
			t.Fatalf("expected leafPrefixV2=%v got %v", v2, n.leafPrefixV2())
		}

		for i, k := range keys {
			entry, err := n.GetLeafEntry(uint16(i))
			if err != nil {
				t.Fatalf("GetLeafEntry(%d) failed (v2=%v): %v", i, v2, err)
			}
			if !bytes.Equal(entry.Key, k) {
				t.Fatalf("key mismatch idx=%d (v2=%v): %q != %q", i, v2, entry.Key, k)
			}
		}

		for _, q := range queries {
			wantIdx, wantFound := expectSearch(n, q)
			gotIdx, gotFound, err := n.SearchLeaf(q)
			if err != nil {
				t.Fatalf("SearchLeaf failed (v2=%v): %v", v2, err)
			}
			if gotIdx != wantIdx || gotFound != wantFound {
				t.Fatalf("SearchLeaf mismatch (v2=%v): key=%x got=(%d,%v) want=(%d,%v)", v2, q, gotIdx, gotFound, wantIdx, wantFound)
			}
		}
	}
}

func TestLeafPrefixV2_ExtendedHeader_Roundtrip(t *testing.T) {
	data := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(data, page.PageTypeLeaf, BuilderOptions{LeafPrefixCompression: true})
	b.SetPageID(1)

	// Ensure both suffixLen>254 (restart entry) and prefixLen>254 (next entry).
	k0 := bytes.Repeat([]byte{'a'}, 300)
	k1 := append([]byte(nil), k0...)
	k1[len(k1)-1] = 'b'
	val := []byte("v")

	if err := b.AddLeafEntry(k0, val, FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("AddLeafEntry k0 failed: %v", err)
	}
	if err := b.AddLeafEntry(k1, val, FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("AddLeafEntry k1 failed: %v", err)
	}

	n := b.Finish()
	if !n.leafPrefixV2() {
		t.Fatalf("expected leaf prefix v2 flag")
	}

	e0, err := n.GetLeafEntry(0)
	if err != nil {
		t.Fatalf("GetLeafEntry(0) failed: %v", err)
	}
	if !bytes.Equal(e0.Key, k0) || !bytes.Equal(e0.Value, val) {
		t.Fatalf("entry0 mismatch")
	}

	e1, err := n.GetLeafEntry(1)
	if err != nil {
		t.Fatalf("GetLeafEntry(1) failed: %v", err)
	}
	if !bytes.Equal(e1.Key, k1) || !bytes.Equal(e1.Value, val) {
		t.Fatalf("entry1 mismatch")
	}

	_, layout0, _, err := n.leafEntryKeyAt(0)
	if err != nil {
		t.Fatalf("leafEntryKeyAt(0): %v", err)
	}
	if layout0.prefixLen != 0 || layout0.suffixLen != len(k0) {
		t.Fatalf("unexpected layout0 prefix/suffix: %d/%d", layout0.prefixLen, layout0.suffixLen)
	}

	_, layout1, _, err := n.leafEntryKeyAt(1)
	if err != nil {
		t.Fatalf("leafEntryKeyAt(1): %v", err)
	}
	if layout1.prefixLen <= 254 {
		t.Fatalf("expected extended prefixLen>254, got %d", layout1.prefixLen)
	}
	if layout1.suffixLen != 1 {
		t.Fatalf("expected suffixLen=1, got %d", layout1.suffixLen)
	}
}

func TestLeafSearch_CorruptedOffsetsReturnError(t *testing.T) {
	data := make([]byte, page.PageSize)
	n := NewNode(data)
	n.SetType(page.PageTypeLeaf)
	n.SetPageID(1)
	n.SetCount(1)

	dirOff := NodeHeaderSize
	binary.LittleEndian.PutUint16(data[dirOff:dirOff+2], uint16(page.PageSize-1))

	_, _, err := n.SearchLeaf([]byte("key"))
	if !errors.Is(err, ErrCorruptedNode) {
		t.Fatalf("expected ErrCorruptedNode, got %v", err)
	}
}

func TestInternalNode(t *testing.T) {
	data := make([]byte, page.PageSize)
	n := NewNode(data)
	n.SetType(page.PageTypeInternal)

	// Add keys: 10, 30, 20
	// Sorted: 10, 20, 30
	inputs := []struct {
		Key []byte
		ID  uint64
	}{
		{[]byte("10"), 100},
		{[]byte("30"), 300},
		{[]byte("20"), 200},
	}

	for _, in := range inputs {
		err := n.AddInternalChild(in.Key, in.ID)
		if err != nil {
			t.Fatalf("AddInternalChild failed: %v", err)
		}
	}

	// Verify Order
	expected := []struct {
		Key []byte
		ID  uint64
	}{
		{[]byte("10"), 100},
		{[]byte("20"), 200},
		{[]byte("30"), 300},
	}

	for i := uint16(0); i < 3; i++ {
		entry, err := n.GetInternalEntry(i)
		if err != nil {
			t.Fatalf("GetInternalEntry(%d) failed: %v", i, err)
		}
		if !bytes.Equal(entry.Key, expected[i].Key) {
			t.Errorf("Index %d: expected key %s, got %s", i, expected[i].Key, entry.Key)
		}
		if entry.ChildPageID != expected[i].ID {
			t.Errorf("Index %d: expected ID %d, got %d", i, expected[i].ID, entry.ChildPageID)
		}
	}

	// Test Search
	// Keys: 10, 20, 30
	cases := []struct {
		Key      string
		Expected uint16
	}{
		{"05", 0}, // < 10 -> Child 0
		{"10", 0}, // == 10 -> Child 0 (since key<=key found?)
		// Wait, SearchInternal: "Find largest i such that Entry[i].Key <= Key"
		// Entry[0]=10. If Key=10, 10<=10 is true. Entry[1]=20. 20<=10 is false.
		// So returns 0. Correct.
		{"15", 0}, // 10 <= 15. 20 <= 15 (False). -> 0.
		{"20", 1}, // 20 <= 20. -> 1.
		{"25", 1}, // 20 <= 25. -> 1.
		{"35", 2}, // 30 <= 35. -> 2.
	}

	for _, c := range cases {
		idx, _ := n.SearchInternal([]byte(c.Key))
		if idx != c.Expected {
			t.Errorf("Search(%s): expected %d, got %d", c.Key, c.Expected, idx)
		}
	}
}

func TestSpaceAccounting(t *testing.T) {
	data := make([]byte, page.PageSize)
	n := NewNode(data)
	n.SetType(page.PageTypeLeaf)

	initialFree := n.FreeSpace()
	// Header(16). Free = 4096 - 16 = 4080.
	if initialFree != 4080 {
		t.Errorf("Initial free space: expected 4080, got %d", initialFree)
	}

	// Add one entry
	// Key=1 (1 byte), Val=1 (1 byte).
	// Entry Overhead: 7 bytes. Total Entry Data: 9 bytes.
	// Directory: 2 bytes.
	// Total consumed: 11 bytes.
	n.AddLeafEntry([]byte("k"), []byte("v"), FlagInline, page.ValuePtr{})

	newFree := n.FreeSpace()
	if newFree != 4080-11 {
		t.Errorf("Free space after insert: expected %d, got %d", 4080-11, newFree)
	}
}
