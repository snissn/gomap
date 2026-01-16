package slab

import (
	"bytes"
	"testing"
)

func TestDictPoolLRUEviction(t *testing.T) {
	oldPools := dictPools
	dictPools = newDictPoolCache(1)
	t.Cleanup(func() { dictPools = oldPools })

	dir := t.TempDir()
	sm, err := NewSlabManagerWithOptions(dir, Options{
		Compression: CompressionOptions{
			Kind:            CompressionZSTD,
			MinBytes:        1,
			MinSavingsBytes: 1,
		},
		CompressionAdaptiveTrainBytes: -1,
	})
	if err != nil {
		t.Fatalf("new slab manager: %v", err)
	}
	defer func() { _ = sm.Close() }()

	dictA := buildTestDict(t, 10, makeTestSamples(6, 2048))
	dictB := buildTestDict(t, 11, makeTestSamples(6, 2048))

	sm.ForceAcceptProfileForTesting(&ActiveCompressionProfile{
		Dict:      dictA,
		DictBytes: len(dictA),
		K:         1,
	})

	fillToZone(t, sm, 1, []byte("k"), bytes.Repeat([]byte("f"), 64*1024))
	ptr1, err := sm.Append([]byte("zone1"), bytes.Repeat([]byte("payload"), 8*1024))
	if err != nil {
		t.Fatalf("append zone1: %v", err)
	}

	sm.ForceAcceptProfileForTesting(&ActiveCompressionProfile{
		Dict:      dictB,
		DictBytes: len(dictB),
		K:         1,
	})

	fillToZone(t, sm, 2, []byte("k2"), bytes.Repeat([]byte("f"), 64*1024))
	ptr2, err := sm.Append([]byte("zone2"), bytes.Repeat([]byte("payload"), 8*1024))
	if err != nil {
		t.Fatalf("append zone2: %v", err)
	}

	if _, err := sm.Read(ptr1); err != nil {
		t.Fatalf("read zone1: %v", err)
	}
	if _, err := sm.Read(ptr2); err != nil {
		t.Fatalf("read zone2: %v", err)
	}

	keys := dictPools.keys()
	if len(keys) != 1 {
		t.Fatalf("expected 1 cached pool, got %d", len(keys))
	}
	if keys[0].zoneID != 2 {
		t.Fatalf("expected cache to retain zone 2, got zone %d", keys[0].zoneID)
	}
}
