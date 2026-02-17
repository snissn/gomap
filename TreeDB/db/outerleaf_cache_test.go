package db

import "testing"

func TestOuterLeafBlockCache_ColdMissFastPathStats(t *testing.T) {
	c := newOuterLeafBlockCache(8)
	if c == nil {
		t.Fatal("cache = nil")
	}
	key := outerLeafBlockKey{fileID: 1, offset: 2, length: 3}

	if got := c.get(key); got != nil {
		t.Fatalf("first miss returned non-nil block")
	}
	if got := c.get(key); got != nil {
		t.Fatalf("second miss returned non-nil block")
	}

	hits, misses, entries, capacity := c.stats()
	if hits != 0 {
		t.Fatalf("hits = %d, want 0", hits)
	}
	if misses != 2 {
		t.Fatalf("misses = %d, want 2", misses)
	}
	if entries != 0 {
		t.Fatalf("entries = %d, want 0", entries)
	}
	if capacity != 8 {
		t.Fatalf("capacity = %d, want 8", capacity)
	}
}

func TestOuterLeafBlockCache_AdmitSwitchesToShardPath(t *testing.T) {
	c := newOuterLeafBlockCache(4)
	if c == nil {
		t.Fatal("cache = nil")
	}
	key := outerLeafBlockKey{fileID: 7, offset: 11, length: 13}
	other := outerLeafBlockKey{fileID: 9, offset: 17, length: 19}

	// One cold miss before any admission.
	_ = c.get(key)
	c.put(key, nil)

	// Existing key is a hit; different key is a miss.
	_ = c.get(key)
	_ = c.get(other)

	hits, misses, entries, _ := c.stats()
	if hits != 1 {
		t.Fatalf("hits = %d, want 1", hits)
	}
	if misses != 2 {
		t.Fatalf("misses = %d, want 2 (1 cold + 1 shard miss)", misses)
	}
	if entries != 1 {
		t.Fatalf("entries = %d, want 1", entries)
	}
}
