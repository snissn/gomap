package db

import (
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
)

func makeOuterLeafCacheTestBlock(t *testing.T, n int) *outerleaf.DecodedBlock {
	t.Helper()
	encoded, err := outerleaf.EncodeEntries(nil, []outerleaf.Entry{
		{Key: []byte(fmt.Sprintf("k%08d", n)), Value: []byte("v")},
	}, uint8(ValueLogBlockSnappy), 16)
	if err != nil {
		t.Fatalf("EncodeEntries: %v", err)
	}
	block, err := outerleaf.DecodeBlock(encoded, nil)
	if err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}
	return block
}

func makeOuterLeafCacheTestKey(n int) outerLeafBlockKey {
	return outerLeafBlockKey{
		fileID: 1,
		offset: uint64(n) * 4096,
		length: 128,
	}
}

func fillOuterLeafCacheShardToThreshold(t *testing.T, c *outerLeafBlockCache, shard *outerLeafBlockCacheShard) {
	t.Helper()
	target := shard.capacity / 4
	if target < 1 {
		target = 1
	}
	inserted := 0
	for i := 1; inserted < target && i < 1_000_000; i++ {
		key := makeOuterLeafCacheTestKey(i)
		if c.shardFor(key) != shard {
			continue
		}
		if _, exists := shard.entries[key]; exists {
			continue
		}
		c.put(key, makeOuterLeafCacheTestBlock(t, i))
		inserted++
	}
	if inserted < target {
		t.Fatalf("inserted=%d want at least %d for threshold fill", inserted, target)
	}
}

func findOuterLeafCacheUnseenAdmitKey(t *testing.T, c *outerLeafBlockCache, shard *outerLeafBlockCacheShard, start int) outerLeafBlockKey {
	t.Helper()
	if len(shard.admit) == 0 || shard.admitMask == 0 {
		t.Fatalf("shard admission filter not configured")
	}
	for i := start; i < start+2_000_000; i++ {
		key := makeOuterLeafCacheTestKey(i)
		if c.shardFor(key) != shard {
			continue
		}
		if _, exists := shard.entries[key]; exists {
			continue
		}
		idx := outerLeafBlockKeyHash(key) & shard.admitMask
		word := idx >> 6
		bit := uint64(1) << (idx & 63)
		if shard.admit[word]&bit == 0 {
			return key
		}
	}
	t.Fatalf("unable to find unseen admission bit candidate")
	return outerLeafBlockKey{}
}

func TestOuterLeafBlockCachePutWithLeaseSmallCacheAdmitsFirstTouch(t *testing.T) {
	cache := newOuterLeafBlockCache(8)
	if cache == nil {
		t.Fatalf("cache=nil")
	}
	key := makeOuterLeafCacheTestKey(1)
	block := makeOuterLeafCacheTestBlock(t, 1)
	lease, admitted := cache.putWithLease(key, block)
	if !admitted {
		t.Fatalf("admitted=false want=true")
	}
	if lease.ref == nil {
		t.Fatalf("lease.ref=nil want non-nil")
	}
	lease.Release()
	got, readLease := cache.get(key)
	if got == nil {
		t.Fatalf("cache get miss, want hit")
	}
	readLease.Release()
}

func TestOuterLeafBlockCachePutWithLeaseSecondTouchAdmission(t *testing.T) {
	cache := newOuterLeafBlockCache(8192)
	if cache == nil {
		t.Fatalf("cache=nil")
	}
	if len(cache.shards) == 0 {
		t.Fatalf("cache has no shards")
	}
	shard := &cache.shards[0]
	if shard.capacity <= 64 {
		t.Fatalf("shard capacity=%d want >64 to exercise admission filter", shard.capacity)
	}
	fillOuterLeafCacheShardToThreshold(t, cache, shard)
	key := findOuterLeafCacheUnseenAdmitKey(t, cache, shard, 1_000_000)

	firstBlock := makeOuterLeafCacheTestBlock(t, 2_000_001)
	lease, admitted := cache.putWithLease(key, firstBlock)
	if admitted {
		lease.Release()
		t.Fatalf("first touch admitted=true want=false")
	}
	if lease.ref != nil {
		lease.Release()
		t.Fatalf("first touch lease.ref non-nil want nil")
	}
	if got, readLease := cache.get(key); got != nil {
		readLease.Release()
		t.Fatalf("first touch unexpectedly present in cache")
	}

	secondBlock := makeOuterLeafCacheTestBlock(t, 2_000_002)
	lease, admitted = cache.putWithLease(key, secondBlock)
	if !admitted {
		t.Fatalf("second touch admitted=false want=true")
	}
	if lease.ref == nil {
		t.Fatalf("second touch lease.ref=nil want non-nil")
	}
	lease.Release()

	got, readLease := cache.get(key)
	if got == nil {
		t.Fatalf("second touch cache get miss, want hit")
	}
	readLease.Release()
}
