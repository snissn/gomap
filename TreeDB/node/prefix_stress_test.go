package node

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestLeafPrefixCompression_Stress(t *testing.T) {
	// 1. Setup
	data := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(data, page.PageTypeLeaf, BuilderOptions{LeafPrefixCompression: true})
	b.SetPageID(1)

	// 2. Generate keys with heavy prefix overlap
	// mimics "validators/val:..."
	prefix := []byte("validators/val:")
	var keys [][]byte
	expected := make(map[string][]byte)

	// Add 50 keys (enough to trigger restarts)
	for i := 0; i < 50; i++ {
		// Use a mix of sequential and random suffixes
		suffix := fmt.Sprintf("%04d-%08d", i, rand.Int63())
		key := append([]byte(nil), prefix...)
		key = append(key, []byte(suffix)...)
		val := []byte(fmt.Sprintf("value-%d", i))

		keys = append(keys, key)
		expected[string(key)] = val
	}

	// Sort keys because Builder expects sorted additions (usually)
	// But AddLeafEntry handles insertion into sorted order?
	// The test uses Builder directly? No, usually we use Node.AddLeafEntry for updates,
	// or Builder for bulk load.
	// Let's use Node.AddLeafEntry to mimic runtime inserts.

	// Re-init node as empty
	n := NewNode(data)
	n.SetType(page.PageTypeLeaf)
	n.SetCount(0)
	// Hack: To enable prefix compression on a raw node, we need to set the flag in the header?
	// Or just call addLeafEntryPrefixCompressed?
	// Node.leafPrefixCompressed() checks a bit in the header.
	// We need to set that bit.
	// In node.go: func (n *Node) leafPrefixCompressed() bool { return n.rawFlags()&FlagLeafPrefixCompressed != 0 }
	// We can't set raw flags easily via public API?
	// Builder does it.

	// Let's use Builder to init an empty compressed node.
	b.Finish()
	n = NewNode(data)
	if !n.leafPrefixCompressed() {
		t.Fatal("Failed to init prefix compressed node")
	}

	// 3. Insert keys in RANDOM order to stress AddLeafEntry
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	for _, k := range keys {
		v := expected[string(k)]
		// AddLeafEntry handles the complexity
		err := n.AddLeafEntry(k, v, 0, page.ValuePtr{})
		if err != nil {
			t.Fatalf("AddLeafEntry failed: %v", err)
		}
	}

	// 4. Verify all keys
	for k, v := range expected {
		keyBytes := []byte(k)

		// Test SearchLeaf
		idx, found, err := n.SearchLeaf(keyBytes)
		if err != nil {
			t.Fatalf("SearchLeaf error for %s: %v", k, err)
		}
		if !found {
			t.Fatalf("SearchLeaf failed to find %s", k)
		}

		// Test GetLeafEntry
		entry, err := n.GetLeafEntry(idx)
		if err != nil {
			t.Fatalf("GetLeafEntry error: %v", err)
		}
		if !bytes.Equal(entry.Key, keyBytes) {
			t.Errorf("Key mismatch: got %q want %q", entry.Key, keyBytes)
		}
		if !bytes.Equal(entry.Value, v) {
			t.Errorf("Value mismatch: got %q want %q", entry.Value, v)
		}
	}

	t.Logf("Successfully verified %d prefix-compressed keys", len(keys))
}

func TestLeafPrefixCompression_RestartBoundary(t *testing.T) {
	// Test specifically around the restart interval
	data := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(data, page.PageTypeLeaf, BuilderOptions{LeafPrefixCompression: true})
	b.SetPageID(1)
	b.Finish()
	n := NewNode(data)

	// restart interval is likely 16 (internal constant).
	// Let's insert 32 keys that are IDENTICAL except last byte.
	// "key-00", "key-01", ...

	var keys []string
	for i := 0; i < 40; i++ {
		k := fmt.Sprintf("key-%02d", i)
		keys = append(keys, k)
		err := n.AddLeafEntry([]byte(k), []byte("val"), 0, page.ValuePtr{})
		if err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	// Verify
	for i, k := range keys {
		idx, found, err := n.SearchLeaf([]byte(k))
		if err != nil || !found {
			t.Errorf("Missing key %s (idx %d)", k, i)
		}
		if idx != uint16(i) {
			t.Errorf("Wrong index for %s: got %d want %d", k, idx, i)
		}

		// Check key reconstruction
		entry, _ := n.GetLeafEntry(idx)
		if string(entry.Key) != k {
			t.Errorf("Reconstruction failed for %s: got %q", k, entry.Key)
		}
	}
}
