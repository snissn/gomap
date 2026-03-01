package db

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/leafblock"
)

func makeLeafBlockCacheTestBlock(t *testing.T, n int) *leafblock.DecodedBlock {
	t.Helper()
	encoded, err := leafblock.EncodeEntries(nil, []leafblock.Entry{
		{Key: []byte(fmt.Sprintf("k%08d", n)), Value: []byte("v")},
	}, uint8(ValueLogBlockSnappy), 16)
	if err != nil {
		t.Fatalf("EncodeEntries: %v", err)
	}
	block, err := leafblock.DecodeBlock(encoded, nil)
	if err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}
	return block
}

func makeLeafBlockCacheTestLeaseBlock(t *testing.T, n int) *leafblock.DecodedBlock {
	t.Helper()
	encoded, err := leafblock.EncodeEntries(nil, []leafblock.Entry{
		{Key: []byte(fmt.Sprintf("k%08d", n)), Value: []byte("v")},
	}, uint8(ValueLogBlockSnappy), 16)
	if err != nil {
		t.Fatalf("EncodeEntries: %v", err)
	}
	block, err := leafblock.DecodeBlockLease(encoded)
	if err != nil {
		t.Fatalf("DecodeBlockLease: %v", err)
	}
	return block
}

func makeLeafBlockCacheTestKey(n int) leafBlockKey {
	return leafBlockKey{
		fileID: 1,
		offset: uint64(n) * 4096,
		length: 128,
	}
}

func fillLeafBlockCacheShardToThreshold(t *testing.T, c *leafBlockCache, shard *leafBlockCacheShard) {
	t.Helper()
	target := shard.capacity / 4
	if target < 1 {
		target = 1
	}
	for i := 1; len(shard.entries) < target && i < 1_000_000; i++ {
		key := makeLeafBlockCacheTestKey(i)
		if c.shardFor(key) != shard {
			continue
		}
		if _, exists := shard.entries[key]; exists {
			continue
		}
		c.put(key, makeLeafBlockCacheTestBlock(t, i))
	}
	if len(shard.entries) < target {
		t.Fatalf("entries=%d want at least %d for threshold fill", len(shard.entries), target)
	}
}

func fillLeafBlockCacheShardToEntries(t *testing.T, c *leafBlockCache, shard *leafBlockCacheShard, target int) {
	t.Helper()
	if target < 1 {
		target = 1
	}
	for i := 1; len(shard.entries) < target && i < 2_000_000; i++ {
		key := makeLeafBlockCacheTestKey(i)
		if c.shardFor(key) != shard {
			continue
		}
		if _, exists := shard.entries[key]; exists {
			continue
		}
		c.put(key, makeLeafBlockCacheTestBlock(t, i))
	}
	if len(shard.entries) < target {
		t.Fatalf("entries=%d want at least %d for target fill", len(shard.entries), target)
	}
}

func findLeafBlockCacheUnseenAdmitKey(t *testing.T, c *leafBlockCache, shard *leafBlockCacheShard, start int) leafBlockKey {
	t.Helper()
	if len(shard.admit) == 0 || shard.admitMask == 0 {
		t.Fatalf("shard admission filter not configured")
	}
	for i := start; i < start+2_000_000; i++ {
		key := makeLeafBlockCacheTestKey(i)
		if c.shardFor(key) != shard {
			continue
		}
		if _, exists := shard.entries[key]; exists {
			continue
		}
		if shard.admitEstimateHash(leafBlockKeyHash(key)) == 0 {
			return key
		}
	}
	t.Fatalf("unable to find unseen admission counter candidate")
	return leafBlockKey{}
}

func findLeafBlockCachePrimaryCollisionSecondUnseenKey(t *testing.T, c *leafBlockCache, shard *leafBlockCacheShard, base leafBlockKey, start int) leafBlockKey {
	t.Helper()
	if len(shard.admit) == 0 || shard.admitMask == 0 {
		t.Fatalf("shard admission filter not configured")
	}
	baseA, baseB := leafBlockCacheAdmitIndexes(leafBlockKeyHash(base), shard.admitMask)
	for i := start; i < start+2_000_000; i++ {
		key := makeLeafBlockCacheTestKey(i)
		if c.shardFor(key) != shard {
			continue
		}
		if key == base {
			continue
		}
		if _, exists := shard.entries[key]; exists {
			continue
		}
		idxA, idxB := leafBlockCacheAdmitIndexes(leafBlockKeyHash(key), shard.admitMask)
		if idxA != baseA || idxB == baseB {
			continue
		}
		if shard.admitCounterAtIndex(idxB) == 0 {
			return key
		}
	}
	t.Fatalf("unable to find primary-collision key with unseen secondary counter")
	return leafBlockKey{}
}

func collectLeafBlockCacheShardKeys(t *testing.T, shard *leafBlockCacheShard, limit int) []leafBlockKey {
	t.Helper()
	if shard == nil {
		t.Fatalf("shard=nil")
	}
	if limit < 1 {
		limit = 1
	}
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	keys := make([]leafBlockKey, 0, limit)
	for key := range shard.entries {
		keys = append(keys, key)
		if len(keys) >= limit {
			break
		}
	}
	if len(keys) == 0 {
		t.Fatalf("shard has no keys to sample")
	}
	return keys
}

func TestLeafBlockCachePutWithLeaseSmallCacheAdmitsFirstTouch(t *testing.T) {
	cache := newLeafBlockCache(8)
	if cache == nil {
		t.Fatalf("cache=nil")
	}
	key := makeLeafBlockCacheTestKey(1)
	block := makeLeafBlockCacheTestBlock(t, 1)
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

func TestLeafBlockCachePutWithLeaseSecondTouchAdmission(t *testing.T) {
	cache := newLeafBlockCache(8192)
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
	fillLeafBlockCacheShardToThreshold(t, cache, shard)
	key := findLeafBlockCacheUnseenAdmitKey(t, cache, shard, 1_000_000)

	firstBlock := makeLeafBlockCacheTestBlock(t, 2_000_001)
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

	secondBlock := makeLeafBlockCacheTestBlock(t, 2_000_002)
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

func TestLeafBlockCachePutWithLeasePrimaryCollisionStillNeedsSecondTouch(t *testing.T) {
	cache := newLeafBlockCache(8192)
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
	fillLeafBlockCacheShardToEntries(t, cache, shard, shard.capacity/2)
	baseKey := findLeafBlockCacheUnseenAdmitKey(t, cache, shard, 4_000_000)

	baseBlock := makeLeafBlockCacheTestBlock(t, 4_100_001)
	lease, admitted := cache.putWithLease(baseKey, baseBlock)
	if admitted {
		lease.Release()
		t.Fatalf("base first touch admitted=true want=false")
	}
	if lease.ref != nil {
		lease.Release()
		t.Fatalf("base first touch lease.ref non-nil want nil")
	}

	collideKey := findLeafBlockCachePrimaryCollisionSecondUnseenKey(t, cache, shard, baseKey, 4_200_000)
	first := makeLeafBlockCacheTestBlock(t, 4_300_001)
	lease, admitted = cache.putWithLease(collideKey, first)
	if admitted {
		lease.Release()
		t.Fatalf("colliding first touch admitted=true want=false")
	}
	if lease.ref != nil {
		lease.Release()
		t.Fatalf("colliding first touch lease.ref non-nil want nil")
	}
	if got, readLease := cache.get(collideKey); got != nil {
		readLease.Release()
		t.Fatalf("colliding first touch unexpectedly present in cache")
	}

	second := makeLeafBlockCacheTestBlock(t, 4_300_002)
	lease, admitted = cache.putWithLease(collideKey, second)
	if !admitted || lease.ref == nil {
		t.Fatalf("colliding second touch admitted=%v lease.ref=nil=%v want admitted with lease", admitted, lease.ref == nil)
	}
	lease.Release()
}

func TestLeafBlockCachePut_FirstTouchRejectRecyclesBlock(t *testing.T) {
	cache := newLeafBlockCache(8192)
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
	fillLeafBlockCacheShardToThreshold(t, cache, shard)
	key := findLeafBlockCacheUnseenAdmitKey(t, cache, shard, 2_000_000)

	block := makeLeafBlockCacheTestLeaseBlock(t, 3_000_001)
	if len(block.RawBytes()) == 0 {
		t.Fatalf("block raw bytes empty before put")
	}
	cache.put(key, block)

	// First touch should be rejected by admission; put must recycle the block
	// it was handed instead of leaking it to GC churn.
	if block.RawBytes() != nil {
		t.Fatalf("block raw bytes retained after rejected put")
	}
	if got, lease := cache.get(key); got != nil {
		lease.Release()
		t.Fatalf("rejected first touch unexpectedly present in cache")
	}
}

func TestLeafBlockCachePut_DuplicateKeyRecyclesIncomingBlock(t *testing.T) {
	cache := newLeafBlockCache(8)
	if cache == nil {
		t.Fatalf("cache=nil")
	}
	key := makeLeafBlockCacheTestKey(42)

	first := makeLeafBlockCacheTestLeaseBlock(t, 5001)
	if len(first.RawBytes()) == 0 {
		t.Fatalf("first block raw bytes empty before put")
	}
	cache.put(key, first)

	got, readLease := cache.get(key)
	if got == nil {
		readLease.Release()
		t.Fatalf("cache get miss after first put")
	}
	if got != first {
		readLease.Release()
		t.Fatalf("cache get returned unexpected block pointer")
	}
	readLease.Release()

	second := makeLeafBlockCacheTestLeaseBlock(t, 5002)
	if len(second.RawBytes()) == 0 {
		t.Fatalf("second block raw bytes empty before duplicate put")
	}
	cache.put(key, second)

	if second.RawBytes() != nil {
		t.Fatalf("duplicate incoming block not recycled")
	}
	if first.RawBytes() == nil {
		t.Fatalf("existing cached block was unexpectedly recycled")
	}

	got, readLease = cache.get(key)
	if got == nil {
		readLease.Release()
		t.Fatalf("cache get miss after duplicate put")
	}
	if got != first {
		readLease.Release()
		t.Fatalf("cache get returned unexpected block after duplicate put")
	}
	readLease.Release()
}

func TestLeafBlockCachePutAfterMissNoLease_DuplicateDoesNotPromote(t *testing.T) {
	cache := newLeafBlockCache(8)
	if cache == nil {
		t.Fatalf("cache=nil")
	}
	if len(cache.shards) != 1 {
		t.Fatalf("shard count=%d want=1", len(cache.shards))
	}
	shard := &cache.shards[0]
	keyA := makeLeafBlockCacheTestKey(7001)
	keyB := makeLeafBlockCacheTestKey(7002)

	blockA := makeLeafBlockCacheTestLeaseBlock(t, 7001)
	blockB := makeLeafBlockCacheTestLeaseBlock(t, 7002)
	cache.put(keyA, blockA)
	cache.put(keyB, blockB)

	shard.mu.RLock()
	if shard.head < 0 || shard.tail < 0 {
		shard.mu.RUnlock()
		t.Fatalf("invalid list state head=%d tail=%d", shard.head, shard.tail)
	}
	if got := shard.nodes[shard.head].key; got != keyB {
		shard.mu.RUnlock()
		t.Fatalf("head key=%+v want=%+v", got, keyB)
	}
	if got := shard.nodes[shard.tail].key; got != keyA {
		shard.mu.RUnlock()
		t.Fatalf("tail key=%+v want=%+v", got, keyA)
	}
	shard.mu.RUnlock()

	dup := makeLeafBlockCacheTestLeaseBlock(t, 7003)
	cache.putAfterMissNoLease(keyA, dup)

	if dup.RawBytes() != nil {
		t.Fatalf("duplicate incoming block was not recycled")
	}
	if blockA.RawBytes() == nil {
		t.Fatalf("existing cached block unexpectedly recycled")
	}

	shard.mu.RLock()
	if got := shard.nodes[shard.head].key; got != keyB {
		shard.mu.RUnlock()
		t.Fatalf("head key moved by duplicate no-lease put: got=%+v want=%+v", got, keyB)
	}
	if got := shard.nodes[shard.tail].key; got != keyA {
		shard.mu.RUnlock()
		t.Fatalf("tail key moved by duplicate no-lease put: got=%+v want=%+v", got, keyA)
	}
	shard.mu.RUnlock()
}

func TestLeafBlockCachePutAfterMissNoLease_WarmShardDropsUnderContention(t *testing.T) {
	cache := newLeafBlockCache(130)
	if cache == nil {
		t.Fatalf("cache=nil")
	}
	if len(cache.shards) == 0 {
		t.Fatalf("cache has no shards")
	}
	shard := &cache.shards[0]
	if shard.capacity <= 64 {
		t.Fatalf("shard capacity=%d want >64 to exercise warm/full admission behavior", shard.capacity)
	}
	fillLeafBlockCacheShardToEntries(t, cache, shard, shard.capacity/2)
	key := findLeafBlockCacheUnseenAdmitKey(t, cache, shard, 8_000_000)
	_ = shard.shouldAdmit(key) // prime seen bits so second-touch admission passes

	_, _, _, lockContentionBefore := cache.putStats()
	block := makeLeafBlockCacheTestLeaseBlock(t, 8_100_000)
	shard.mu.Lock()
	done := make(chan struct{})
	go func() {
		cache.putAfterMissNoLease(key, block)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		shard.mu.Unlock()
		t.Fatalf("putAfterMissNoLease blocked on warm shard contention")
	}
	shard.mu.Unlock()

	if block.RawBytes() != nil {
		t.Fatalf("contention-dropped block was not recycled")
	}
	if got, lease := cache.get(key); got != nil {
		lease.Release()
		t.Fatalf("contention-dropped key unexpectedly present in cache")
	}
	_, _, _, lockContentionAfter := cache.putStats()
	if lockContentionAfter <= lockContentionBefore {
		t.Fatalf("lock contention counter did not increase: before=%d after=%d", lockContentionBefore, lockContentionAfter)
	}
}

func TestLeafBlockCacheSLRUPolicy_ProtectsPromotedEntry(t *testing.T) {
	cache := newLeafBlockCacheWithPolicy(2, leafBlockCachePolicySLRU)
	if cache == nil {
		t.Fatalf("cache=nil")
	}
	if len(cache.shards) != 1 {
		t.Fatalf("shard count=%d want=1", len(cache.shards))
	}
	shard := &cache.shards[0]
	keyA := makeLeafBlockCacheTestKey(9001)
	keyB := makeLeafBlockCacheTestKey(9002)
	keyC := makeLeafBlockCacheTestKey(9003)

	first := makeLeafBlockCacheTestLeaseBlock(t, 9001)
	cache.put(keyA, first)
	if got, lease := cache.get(keyA); got == nil {
		lease.Release()
		t.Fatalf("expected keyA hit after first put")
	} else {
		lease.Release()
	}

	second := makeLeafBlockCacheTestLeaseBlock(t, 9002)
	cache.put(keyB, second)

	if got, lease := cache.get(keyA); got == nil {
		lease.Release()
		t.Fatalf("expected keyA hit for promotion")
	} else {
		lease.Release()
	}
	for i := 0; i < 32; i++ {
		shard.mu.RLock()
		idxA, okA := shard.entries[keyA]
		segment := leafBlockCacheSegmentProbation
		if okA {
			segment = shard.nodes[idxA].segment
		}
		shard.mu.RUnlock()
		if okA && segment == leafBlockCacheSegmentProtected {
			break
		}
		if got, lease := cache.get(keyA); got == nil {
			lease.Release()
			t.Fatalf("expected keyA hit during promotion retries")
		} else {
			lease.Release()
		}
	}

	shard.mu.RLock()
	idxA, okA := shard.entries[keyA]
	if !okA {
		shard.mu.RUnlock()
		t.Fatalf("missing keyA after promotion")
	}
	if got := shard.nodes[idxA].segment; got != leafBlockCacheSegmentProtected {
		shard.mu.RUnlock()
		t.Fatalf("keyA segment=%d want protected", got)
	}
	shard.mu.RUnlock()

	third := makeLeafBlockCacheTestLeaseBlock(t, 9003)
	cache.put(keyC, third)

	if got, lease := cache.get(keyB); got != nil {
		lease.Release()
		t.Fatalf("expected keyB eviction from probation")
	}
	if got, lease := cache.get(keyA); got == nil {
		lease.Release()
		t.Fatalf("expected keyA retained in protected segment")
	} else {
		lease.Release()
	}
	if got, lease := cache.get(keyC); got == nil {
		lease.Release()
		t.Fatalf("expected keyC hit after replacement")
	} else {
		lease.Release()
	}
}

func TestLeafBlockCacheSLRUPolicy_DuplicatePutWithLeaseRecyclesIncoming(t *testing.T) {
	cache := newLeafBlockCacheWithPolicy(8, leafBlockCachePolicySLRU)
	if cache == nil {
		t.Fatalf("cache=nil")
	}
	key := makeLeafBlockCacheTestKey(9101)

	first := makeLeafBlockCacheTestLeaseBlock(t, 9101)
	lease, admitted := cache.putWithLease(key, first)
	if !admitted || lease.ref == nil {
		t.Fatalf("first putWithLease admitted=%v lease.nil=%v", admitted, lease.ref == nil)
	}
	lease.Release()

	second := makeLeafBlockCacheTestLeaseBlock(t, 9102)
	lease, admitted = cache.putWithLease(key, second)
	if !admitted || lease.ref == nil {
		t.Fatalf("duplicate putWithLease admitted=%v lease.nil=%v", admitted, lease.ref == nil)
	}
	lease.Release()
	if second.RawBytes() != nil {
		t.Fatalf("duplicate incoming block not recycled under slru policy")
	}
}

func TestLeafBlockCacheSIEVEPolicy_SecondChanceProtectsHitEntry(t *testing.T) {
	cache := newLeafBlockCacheWithPolicy(2, leafBlockCachePolicySIEVE)
	if cache == nil {
		t.Fatalf("cache=nil")
	}
	keyA := makeLeafBlockCacheTestKey(9201)
	keyB := makeLeafBlockCacheTestKey(9202)
	keyC := makeLeafBlockCacheTestKey(9203)

	cache.put(keyA, makeLeafBlockCacheTestLeaseBlock(t, 9201))
	cache.put(keyB, makeLeafBlockCacheTestLeaseBlock(t, 9202))

	// Hit keyA once so SIEVE grants it a second chance during eviction scan.
	if got, lease := cache.get(keyA); got == nil {
		lease.Release()
		t.Fatalf("expected keyA hit before replacement")
	} else {
		lease.Release()
	}

	cache.put(keyC, makeLeafBlockCacheTestLeaseBlock(t, 9203))

	if got, lease := cache.get(keyA); got == nil {
		lease.Release()
		t.Fatalf("expected keyA retained after second-chance")
	} else {
		lease.Release()
	}
	if got, lease := cache.get(keyB); got != nil {
		lease.Release()
		t.Fatalf("expected keyB eviction from sieve tail scan")
	}
	if got, lease := cache.get(keyC); got == nil {
		lease.Release()
		t.Fatalf("expected keyC present after insertion")
	} else {
		lease.Release()
	}
}

func TestLeafBlockCacheSIEVEPolicy_DuplicatePutWithLeaseRecyclesIncoming(t *testing.T) {
	cache := newLeafBlockCacheWithPolicy(8, leafBlockCachePolicySIEVE)
	if cache == nil {
		t.Fatalf("cache=nil")
	}
	key := makeLeafBlockCacheTestKey(9301)

	first := makeLeafBlockCacheTestLeaseBlock(t, 9301)
	lease, admitted := cache.putWithLease(key, first)
	if !admitted || lease.ref == nil {
		t.Fatalf("first putWithLease admitted=%v lease.nil=%v", admitted, lease.ref == nil)
	}
	lease.Release()

	second := makeLeafBlockCacheTestLeaseBlock(t, 9302)
	lease, admitted = cache.putWithLease(key, second)
	if !admitted || lease.ref == nil {
		t.Fatalf("duplicate putWithLease admitted=%v lease.nil=%v", admitted, lease.ref == nil)
	}
	lease.Release()
	if second.RawBytes() != nil {
		t.Fatalf("duplicate incoming block not recycled under sieve policy")
	}
}

func TestLeafBlockCachePutWithLeaseHotFullShardAdmitsNewKeysUnderConcurrentReadPressure(t *testing.T) {
	cache := newLeafBlockCache(130)
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
	fillLeafBlockCacheShardToEntries(t, cache, shard, shard.capacity)
	hotKeys := collectLeafBlockCacheShardKeys(t, shard, 16)

	var (
		wg       sync.WaitGroup
		stop     = make(chan struct{})
		stopOnce sync.Once
	)
	stopReaders := func() {
		stopOnce.Do(func() {
			close(stop)
			wg.Wait()
		})
	}
	t.Cleanup(stopReaders)

	const readerGoroutines = 8
	for g := 0; g < readerGoroutines; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := seed; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				key := hotKeys[i%len(hotKeys)]
				_, lease := cache.get(key)
				lease.Release()
			}
		}(g)
	}

	type putResult struct {
		lease    leafBlockCacheLease
		admitted bool
	}

	const probeKeys = 6
	for i := 0; i < probeKeys; i++ {
		base := 9_000_000 + i*1000
		key := findLeafBlockCacheUnseenAdmitKey(t, cache, shard, base)

		first := makeLeafBlockCacheTestBlock(t, base+1)
		lease, admitted := cache.putWithLease(key, first)
		if admitted {
			lease.Release()
			t.Fatalf("probe=%d first touch admitted=true want=false", i)
		}
		if lease.ref != nil {
			lease.Release()
			t.Fatalf("probe=%d first touch lease.ref non-nil want nil", i)
		}

		keyForSecond := key
		second := makeLeafBlockCacheTestBlock(t, base+2)
		resCh := make(chan putResult, 1)
		go func() {
			lease, admitted := cache.putWithLease(keyForSecond, second)
			resCh <- putResult{lease: lease, admitted: admitted}
		}()

		select {
		case res := <-resCh:
			if !res.admitted {
				res.lease.Release()
				t.Fatalf("probe=%d second touch admitted=false want=true", i)
			}
			if res.lease.ref == nil {
				res.lease.Release()
				t.Fatalf("probe=%d second touch lease.ref=nil want non-nil", i)
			}
			res.lease.Release()
		case <-time.After(2 * time.Second):
			stopReaders()
			t.Fatalf("probe=%d second touch timed out under concurrent read pressure", i)
		}

		got, readLease := cache.get(keyForSecond)
		if got == nil {
			readLease.Release()
			t.Fatalf("probe=%d admitted key missing from cache", i)
		}
		readLease.Release()
	}
}
