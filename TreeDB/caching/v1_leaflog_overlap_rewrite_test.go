package caching

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestCachingDB_V1LeafLog_OverlapRewritePreservesFallbackOnlyKeys(t *testing.T) {
	for _, mode := range []string{
		backenddb.IndexOuterLeafModeV1LeafLog,
		backenddb.IndexOuterLeafModeV1LeafLogRoute,
	} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			backendOpts := backenddb.Options{
				Dir:                dir,
				ChunkSize:          64 * 1024,
				IndexOuterLeafMode: mode,
				ValueLog: backenddb.ValueLogOptions{
					PointerThreshold:          1,
					ForcePointers:             true,
					OuterLeafBlockCodec:       backenddb.ValueLogBlockLZ4,
					OuterLeafBlockTargetBytes: 1 << 20,
				},
			}
			backend, err := backenddb.Open(backendOpts)
			if err != nil {
				t.Fatalf("backend open: %v", err)
			}
			defer backend.Close()

			cache, err := Open(dir, backend, Options{
				AllowUnsafe:                       true,
				DisableWAL:                        true,
				FlushThreshold:                    1 << 30,
				MemtableShards:                    1,
				JournalLanes:                      1,
				ForceValueLogPointers:             true,
				ValueLogPointerThreshold:          1,
				IndexOuterLeafMode:                mode,
				ValueLogOuterLeafBlockCodec:       uint8(backenddb.ValueLogBlockLZ4),
				ValueLogOuterLeafBlockTargetBytes: 1 << 20,
			})
			if err != nil {
				t.Fatalf("cache open: %v", err)
			}
			defer cache.Close()
			t.Logf("deferred=%v valueLogReader=%v supportsPromo=%v", cache.deferredValueLogEnabled(), cache.valueLogReader != nil, cache.supportsFenceAnchorPromotion())

			keyFor := func(i int) []byte { return []byte(fmt.Sprintf("k%04d", i)) }
			valueFor := func(i int) []byte { return bytes.Repeat([]byte{byte(i + 1)}, 256) }

			const totalKeys = 64
			for i := 0; i < totalKeys; i++ {
				if err := cache.Set(keyFor(i), valueFor(i)); err != nil {
					t.Fatalf("Set(%d): %v", i, err)
				}
			}
			if err := cache.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint(initial): %v", err)
			}
			if mode == backenddb.IndexOuterLeafModeV1LeafLogRoute {
				itInit, err := backend.IteratorWithOptions(nil, nil, backenddb.IteratorOptions{
					Mode:              backenddb.IteratorModePointerProjection,
					IncludeTombstones: true,
				})
				if err != nil {
					t.Fatalf("initial iterator route: %v", err)
				}
				initRows := 0
				initInline := 0
				initTotal := 0
				for itInit.Valid() {
					_, _, flags := itInit.UnsafeEntry()
					initTotal++
					if flags&node.FlagPointer != 0 {
						initRows++
					} else if flags&node.FlagTombstone == 0 {
						initInline++
					}
					if flags&node.FlagTombstone == 0 && flags&node.FlagPointer != 0 {
						initRows++
					}
					itInit.Next()
				}
				if err := itInit.Error(); err != nil {
					t.Fatalf("initial iterator route error: %v", err)
				}
				if err := itInit.Close(); err != nil {
					t.Fatalf("initial iterator route close: %v", err)
				}
				t.Logf("route iterator counts after initial checkpoint total=%d pointer=%d inline=%d tomb=%d", initTotal, initRows, initInline, initTotal-initRows-initInline)
			}

			snap := backend.AcquireSnapshot()
			if snap == nil {
				t.Fatalf("snapshot nil")
			}
			if _, err := snap.GetEntryExact(keyFor(1)); !errors.Is(err, tree.ErrKeyNotFound) {
				_ = snap.Close()
				t.Fatalf("expected fallback-only key before overlap rewrite, err=%v", err)
			}
			if err := snap.Close(); err != nil {
				t.Fatalf("snapshot close before overlap rewrite: %v", err)
			}

			overwrite := bytes.Repeat([]byte("x"), 320)
			inserted := bytes.Repeat([]byte("i"), 320)
			appended := bytes.Repeat([]byte("z"), 320)

			if err := cache.Set(keyFor(1), overwrite); err != nil {
				t.Fatalf("Set(overwrite existing fallback key): %v", err)
			}
			if err := cache.Set([]byte("k0015x"), inserted); err != nil {
				t.Fatalf("Set(insert in covered range): %v", err)
			}
			if err := cache.Set([]byte("k9999"), appended); err != nil {
				t.Fatalf("Set(append key): %v", err)
			}
			if err := cache.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint(overlap rewrite): %v", err)
			}
			if mode == backenddb.IndexOuterLeafModeV1LeafLogRoute {
				itRows, err := backend.IteratorWithOptions(nil, nil, backenddb.IteratorOptions{
					Mode:              backenddb.IteratorModePointerProjection,
					IncludeTombstones: true,
				})
				if err != nil {
					t.Fatalf("iterator post-overwrite route: %v", err)
				}
				rows := 0
				for itRows.Valid() {
					k := itRows.UnsafeKey()
					_, ptr, flags := itRows.UnsafeEntry()
					rows++
					if flags&node.FlagPointer != 0 && flags&node.FlagTombstone == 0 {
						t.Logf("route anchor key=%q ptr=%+v", k, ptr)
					}
					itRows.Next()
				}
				t.Logf("route row count after overlap rewrite=%d", rows)
				if err := itRows.Error(); err != nil {
					t.Fatalf("iterator post-overwrite route error: %v", err)
				}
				if err := itRows.Close(); err != nil {
					t.Fatalf("iterator post-overwrite route close: %v", err)
				}
			}

			checkGet := func(key, want []byte) {
				t.Helper()
				got, err := backend.Get(key)
				if err != nil {
					t.Fatalf("backend Get(%q): %v", key, err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("backend value mismatch for %q got_len=%d want_len=%d", key, len(got), len(want))
				}
			}

			checkGet(keyFor(0), valueFor(0))
			checkGet(keyFor(1), overwrite)
			checkGet(keyFor(totalKeys-1), valueFor(totalKeys-1))
			checkGet([]byte("k0015x"), inserted)
			checkGet([]byte("k9999"), appended)

			it, err := backend.IteratorWithOptions(nil, nil, backenddb.IteratorOptions{
				Mode:              backenddb.IteratorModePointerProjection,
				IncludeTombstones: true,
			})
			if err != nil {
				t.Fatalf("IteratorWithOptions(pointer_projection): %v", err)
			}
			defer it.Close()
			for it.Valid() {
				k := it.UnsafeKey()
				_, ptr, flags := it.UnsafeEntry()
				if flags&node.FlagPointer != 0 {
					if mode == backenddb.IndexOuterLeafModeV1LeafLogRoute {
						if page.ValuePtrIsFenceOuter(ptr) {
							t.Fatalf("expected plain anchor pointer in %s overlap rewrite, key=%q ptr=%+v", mode, k, ptr)
						}
					}
				}
				it.Next()
			}
			if err := it.Error(); err != nil {
				t.Fatalf("pointer-projection iterator error: %v", err)
			}

			snap = backend.AcquireSnapshot()
			if snap == nil {
				t.Fatalf("snapshot nil after overlap rewrite")
			}
			if _, err := snap.GetEntryExact(keyFor(1)); !errors.Is(err, tree.ErrKeyNotFound) {
				_ = snap.Close()
				t.Fatalf("expected no exact fanout for rewritten key, err=%v", err)
			}
			if _, err := snap.GetEntryExact([]byte("k0015x")); !errors.Is(err, tree.ErrKeyNotFound) {
				_ = snap.Close()
				t.Fatalf("expected inserted key to be served from rewritten outer block, err=%v", err)
			}
			if err := snap.Close(); err != nil {
				t.Fatalf("snapshot close after overlap rewrite: %v", err)
			}
			if err := cache.validateV1LeafLogDirectoryInvariants(); err != nil {
				t.Fatalf("v1_leaflog invariant validation: %v", err)
			}
		})
	}
}

func TestCachingDB_V1LeafLogRoute_OverlapRewritePreservesGroupingUnderChurn(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                dir,
		ChunkSize:          64 * 1024,
		IndexOuterLeafMode: backenddb.IndexOuterLeafModeV1LeafLogRoute,
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold:          1,
			ForcePointers:             true,
			OuterLeafBlockCodec:       backenddb.ValueLogBlockLZ4,
			OuterLeafBlockTargetBytes: 1 << 20,
		},
	})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	defer backend.Close()

	cache, err := Open(dir, backend, Options{
		AllowUnsafe:                       true,
		DisableWAL:                        true,
		FlushThreshold:                    1 << 30,
		MemtableShards:                    1,
		JournalLanes:                      1,
		ForceValueLogPointers:             true,
		ValueLogPointerThreshold:          1,
		IndexOuterLeafMode:                backenddb.IndexOuterLeafModeV1LeafLogRoute,
		ValueLogOuterLeafBlockCodec:       uint8(backenddb.ValueLogBlockLZ4),
		ValueLogOuterLeafBlockTargetBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	keyFor := func(i int) []byte { return []byte(fmt.Sprintf("k%04d", i)) }
	valueFor := func(seed int) []byte { return bytes.Repeat([]byte{byte(seed + 1)}, 320) }

	const totalKeys = 96
	for i := 0; i < totalKeys; i++ {
		if err := cache.Set(keyFor(i), valueFor(i)); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint(initial): %v", err)
	}

	ghost := []byte("ghost")
	for round := 0; round < 6; round++ {
		if round%2 == 0 {
			if err := cache.Set(ghost, valueFor(200+round)); err != nil {
				t.Fatalf("Set(ghost, %d): %v", round, err)
			}
		} else {
			if err := cache.Delete(ghost); err != nil {
				t.Fatalf("Delete(ghost, %d): %v", round, err)
			}
		}
		for i := 0; i < totalKeys; i++ {
			if err := cache.Set(keyFor(i), valueFor((round*17)+i)); err != nil {
				t.Fatalf("Set round=%d key=%q: %v", round, keyFor(i), err)
			}
		}
		if err := cache.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint(round=%d): %v", round, err)
		}
	}

	if cache.valueLogReader == nil {
		t.Fatalf("missing value-log reader")
	}
	it, err := backend.IteratorWithOptions(nil, nil, backenddb.IteratorOptions{
		Mode:              backenddb.IteratorModePointerProjection,
		IncludeTombstones: true,
	})
	if err != nil {
		t.Fatalf("IteratorWithOptions(pointer_projection): %v", err)
	}
	defer it.Close()

	groupCount := 0
	groupSum := 0
	smallGroups := 0
	for it.Valid() {
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer == 0 || flags&node.FlagTombstone != 0 {
			it.Next()
			continue
		}
		raw, err := cache.valueLogReader.Read(ptr)
		if err != nil {
			t.Fatalf("read pointer payload: %v", err)
		}
		keys, err := outerleaf.DecodeKeysWithVerify(raw, true)
		if err != nil {
			t.Fatalf("decode pointer payload: %v", err)
		}
		k := len(keys)
		groupCount++
		groupSum += k
		if k <= 2 {
			smallGroups++
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if groupCount == 0 {
		t.Fatalf("expected pointer-backed groups")
	}
	avg := float64(groupSum) / float64(groupCount)
	if avg < 4.0 {
		t.Fatalf("group collapse detected, avg_k=%.3f count=%d", avg, groupCount)
	}
	if float64(smallGroups)/float64(groupCount) > 0.35 {
		t.Fatalf("too many tiny groups (k<=2): %d/%d", smallGroups, groupCount)
	}
	if err := cache.validateV1LeafLogDirectoryInvariants(); err != nil {
		t.Fatalf("v1_leaflog invariant validation: %v", err)
	}
}

func TestCachingDB_V1LeafLogRoute_AnchorFanoutIsOnePerBlock(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                dir,
		ChunkSize:          64 * 1024,
		IndexOuterLeafMode: backenddb.IndexOuterLeafModeV1LeafLogRoute,
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold:          1,
			ForcePointers:             true,
			OuterLeafBlockCodec:       backenddb.ValueLogBlockLZ4,
			OuterLeafBlockTargetBytes: 1 << 20,
		},
	})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	defer backend.Close()

	cache, err := Open(dir, backend, Options{
		AllowUnsafe:                       true,
		DisableWAL:                        true,
		FlushThreshold:                    1 << 30,
		MemtableShards:                    1,
		JournalLanes:                      1,
		ForceValueLogPointers:             true,
		ValueLogPointerThreshold:          1,
		IndexOuterLeafMode:                backenddb.IndexOuterLeafModeV1LeafLogRoute,
		ValueLogOuterLeafBlockCodec:       uint8(backenddb.ValueLogBlockLZ4),
		ValueLogOuterLeafBlockTargetBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	keyFor := func(i int) []byte { return []byte(fmt.Sprintf("k%04d", i)) }
	valueFor := func(seed int) []byte { return bytes.Repeat([]byte{byte(seed + 1)}, 320) }

	const (
		totalKeys = 1024
		rounds    = 5
	)

	for i := 0; i < totalKeys; i++ {
		if err := cache.Set(keyFor(i), valueFor(i)); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint(initial): %v", err)
	}

	for round := 0; round < rounds; round++ {
		// Mutate a large overlapping prefix and force churn with insert/delete noise.
		for i := 0; i < totalKeys/2; i++ {
			if err := cache.Set(keyFor(i), valueFor(round+1+i)); err != nil {
				t.Fatalf("Set(round=%d, key=%q): %v", round, keyFor(i), err)
			}
		}
		ghost := []byte(fmt.Sprintf("ghost-%d", round))
		if err := cache.Set(ghost, valueFor(200+round)); err != nil {
			t.Fatalf("Set(ghost-%d): %v", round, err)
		}
		if round%2 == 1 {
			if err := cache.Delete(ghost); err != nil {
				t.Fatalf("Delete(ghost-%d): %v", round, err)
			}
		}
		if err := cache.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint(round=%d): %v", round, err)
		}
	}

	if cache.valueLogReader == nil {
		t.Fatalf("missing value-log reader")
	}

	it, err := backend.IteratorWithOptions(nil, nil, backenddb.IteratorOptions{
		Mode:              backenddb.IteratorModePointerProjection,
		IncludeTombstones: true,
	})
	if err != nil {
		t.Fatalf("IteratorWithOptions(pointer_projection): %v", err)
	}
	defer it.Close()

	type ptrKey struct {
		fileID uint64
		offset uint64
		length uint32
	}
	ptrUsage := make(map[ptrKey]int)
	totalAnchors := 0
	maxDup := 0
	dupRows := 0

	for it.Valid() {
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer == 0 || flags&node.FlagTombstone != 0 {
			it.Next()
			continue
		}

		raw, err := cache.valueLogReader.Read(ptr)
		if err != nil {
			t.Fatalf("read payload ptr=%+v: %v", ptr, err)
		}
		if !outerleaf.HasMagic(raw) {
			it.Next()
			continue
		}
		if _, err := outerleaf.DecodeKeysWithVerify(raw, true); err != nil {
			t.Fatalf("decode keys ptr=%+v: %v", ptr, err)
		}

		pk := ptrKey{
			fileID: uint64(ptr.FileID),
			offset: ptr.Offset,
			length: uint32(ptr.Length),
		}
		c := ptrUsage[pk] + 1
		ptrUsage[pk] = c
		totalAnchors++
		if c > maxDup {
			maxDup = c
		}
		if c == 2 {
			dupRows++
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if totalAnchors == 0 {
		t.Fatalf("expected at least one pointer-backed anchor row")
	}
	if dupRows > 0 {
		t.Fatalf("found %d duplicated anchor pointers across %d pointer rows (maxDup=%d)", dupRows, totalAnchors, maxDup)
	}
	for ptr, c := range ptrUsage {
		if c > 1 {
			t.Fatalf("pointer reused for %d anchors: key=%+v", c, ptr)
		}
	}
	if err := cache.validateV1LeafLogDirectoryInvariants(); err != nil {
		t.Fatalf("v1_leaflog invariant validation: %v", err)
	}
}

func TestCachingDB_V1LeafLogRoute_AnchorFanoutStaysOnePerBlockUnderMixedSetDeleteOverlap(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                dir,
		ChunkSize:          64 * 1024,
		IndexOuterLeafMode: backenddb.IndexOuterLeafModeV1LeafLogRoute,
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold:          1,
			ForcePointers:             true,
			OuterLeafBlockCodec:       backenddb.ValueLogBlockLZ4,
			OuterLeafBlockTargetBytes: 1 << 20,
		},
	})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	defer backend.Close()

	cache, err := Open(dir, backend, Options{
		AllowUnsafe:                       true,
		DisableWAL:                        true,
		FlushThreshold:                    1 << 30,
		MemtableShards:                    1,
		JournalLanes:                      1,
		ForceValueLogPointers:             true,
		ValueLogPointerThreshold:          1,
		IndexOuterLeafMode:                backenddb.IndexOuterLeafModeV1LeafLogRoute,
		ValueLogOuterLeafBlockCodec:       uint8(backenddb.ValueLogBlockLZ4),
		ValueLogOuterLeafBlockTargetBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	keyFor := func(i int) []byte { return []byte(fmt.Sprintf("k%04d", i)) }
	valueFor := func(seed int) []byte { return bytes.Repeat([]byte{byte(seed + 1)}, 320) }

	const totalKeys = 512
	for i := 0; i < totalKeys; i++ {
		if err := cache.Set(keyFor(i), valueFor(i)); err != nil {
			t.Fatalf("seed Set(%d): %v", i, err)
		}
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	// Mixed set+delete overlap across the same prefix to exercise overlap rewrite
	// queueing without allowing per-key exact fallback persistence.
	for i := 0; i < totalKeys/2; i++ {
		if i%3 == 0 {
			if err := cache.Delete(keyFor(i)); err != nil {
				t.Fatalf("delete(%d): %v", i, err)
			}
			continue
		}
		if err := cache.Set(keyFor(i), valueFor(10000+i)); err != nil {
			t.Fatalf("overwrite(%d): %v", i, err)
		}
	}
	for i := 0; i < 32; i++ {
		k := []byte(fmt.Sprintf("k%04d-extra-%02d", i*4, i))
		if err := cache.Set(k, valueFor(20000+i)); err != nil {
			t.Fatalf("insert(%d): %v", i, err)
		}
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatalf("mixed overlap checkpoint: %v", err)
	}

	it, err := backend.IteratorWithOptions(nil, nil, backenddb.IteratorOptions{
		Mode:              backenddb.IteratorModePointerProjection,
		IncludeTombstones: true,
	})
	if err != nil {
		t.Fatalf("IteratorWithOptions(pointer_projection): %v", err)
	}
	defer it.Close()

	type ptrKey struct {
		fileID uint64
		offset uint64
		length uint32
	}
	ptrUsage := make(map[ptrKey]int)

	for it.Valid() {
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer == 0 || flags&node.FlagTombstone != 0 {
			it.Next()
			continue
		}
		if page.ValuePtrIsFenceOuter(ptr) {
			t.Fatalf("route mode emitted fence-marked pointer %+v", ptr)
		}
		pk := ptrKey{
			fileID: uint64(ptr.FileID),
			offset: ptr.Offset,
			length: uint32(ptr.Length),
		}
		ptrUsage[pk]++
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if len(ptrUsage) == 0 {
		t.Fatalf("expected pointer-backed anchor rows")
	}
	for ptr, c := range ptrUsage {
		if c > 1 {
			t.Fatalf("duplicate pointer-backed anchor rows detected ptr=%+v count=%d", ptr, c)
		}
	}
	if err := cache.validateV1LeafLogDirectoryInvariants(); err != nil {
		t.Fatalf("v1_leaflog invariant validation: %v", err)
	}
}

func TestCachingDB_V1LeafLogRoute_OverlapRewritePreservesBlobRefThreshold(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                dir,
		ChunkSize:          64 * 1024,
		IndexOuterLeafMode: backenddb.IndexOuterLeafModeV1LeafLogRoute,
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold:          127,
			ForcePointers:             false,
			OuterLeafBlockCodec:       backenddb.ValueLogBlockLZ4,
			OuterLeafBlockTargetBytes: 1 << 20,
		},
	})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	defer backend.Close()

	cache, err := Open(dir, backend, Options{
		AllowUnsafe:                       true,
		DisableWAL:                        true,
		FlushThreshold:                    1 << 30,
		MemtableShards:                    1,
		JournalLanes:                      1,
		ForceValueLogPointers:             false,
		ValueLogPointerThreshold:          127,
		IndexOuterLeafMode:                backenddb.IndexOuterLeafModeV1LeafLogRoute,
		ValueLogOuterLeafBlockCodec:       uint8(backenddb.ValueLogBlockLZ4),
		ValueLogOuterLeafBlockTargetBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	keyFor := func(i int) []byte { return []byte(fmt.Sprintf("k%04d", i)) }
	valueFor := func(seed int) []byte { return bytes.Repeat([]byte{byte(seed + 1)}, 256) }

	const totalKeys = 96
	for i := 0; i < totalKeys; i++ {
		if err := cache.Set(keyFor(i), valueFor(i)); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint(initial): %v", err)
	}

	for round := 0; round < 4; round++ {
		for i := 0; i < totalKeys; i++ {
			if err := cache.Set(keyFor(i), valueFor(1000+(round*totalKeys)+i)); err != nil {
				t.Fatalf("Set(round=%d, key=%q): %v", round, keyFor(i), err)
			}
		}
		if err := cache.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint(round=%d): %v", round, err)
		}
	}

	if cache.valueLogReader == nil {
		t.Fatalf("missing value-log reader")
	}
	it, err := backend.IteratorWithOptions(nil, nil, backenddb.IteratorOptions{
		Mode:              backenddb.IteratorModePointerProjection,
		IncludeTombstones: true,
	})
	if err != nil {
		t.Fatalf("IteratorWithOptions(pointer_projection): %v", err)
	}
	defer it.Close()

	checkedBlocks := 0
	for it.Valid() {
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer == 0 || flags&node.FlagTombstone != 0 {
			it.Next()
			continue
		}
		raw, err := cache.valueLogReader.Read(ptr)
		if err != nil {
			t.Fatalf("read payload ptr=%+v: %v", ptr, err)
		}
		if !outerleaf.HasMagic(raw) {
			it.Next()
			continue
		}
		block, err := outerleaf.DecodeBlockLeaseWithVerify(raw, true)
		if err != nil {
			t.Fatalf("decode block ptr=%+v: %v", ptr, err)
		}
		if block == nil {
			t.Fatalf("nil block ptr=%+v", ptr)
		}
		err = block.VisitTypedEntries(func(k []byte, kind outerleaf.EntryKind, value []byte, blobPtr page.ValuePtr) error {
			if kind != outerleaf.EntryKindBlobRef {
				return fmt.Errorf("entry kind=%d key=%q want=blobref", kind, k)
			}
			if len(value) != 0 {
				return fmt.Errorf("blobref entry has inline value bytes key=%q len=%d", k, len(value))
			}
			if blobPtr == (page.ValuePtr{}) {
				return fmt.Errorf("blobref entry missing blob pointer key=%q", k)
			}
			return nil
		})
		block.Release()
		if err != nil {
			t.Fatalf("visit typed entries ptr=%+v: %v", ptr, err)
		}
		checkedBlocks++
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if checkedBlocks == 0 {
		t.Fatalf("expected pointer-backed route blocks")
	}
}

func TestFenceAnchorPromoter_EncodeRewriteBlocks_RouteSplitsLargeEntrySet(t *testing.T) {
	const totalEntries = 70000
	db := &DB{
		indexOuterLeafMode:        backenddb.IndexOuterLeafModeV1LeafLogRoute,
		outerLeafBlockTargetBytes: 4 << 10,
		outerLeafBlockRestart:     outerleaf.NormalizeRestartInterval(16),
	}
	p := &fenceAnchorPromoter{db: db}

	entries := make([]outerleaf.TypedEntry, 0, totalEntries)
	for i := 0; i < totalEntries; i++ {
		entries = append(entries, outerleaf.TypedEntry{
			Key:   []byte(fmt.Sprintf("k%06d", i)),
			Kind:  outerleaf.EntryKindInline,
			Value: []byte{byte(i)},
		})
	}

	blocks, err := p.encodeRewriteBlocks(entries)
	if err != nil {
		t.Fatalf("encodeRewriteBlocks: %v", err)
	}
	if len(blocks) <= 1 {
		t.Fatalf("expected route split into multiple blocks, got %d", len(blocks))
	}

	totalDecoded := 0
	maxEntries := 0
	var prevMax []byte
	for i := range blocks {
		b := blocks[i]
		if len(b.anchorKey) == 0 || len(b.maxKey) == 0 || len(b.payload) == 0 {
			t.Fatalf("block %d missing anchor/max/payload", i)
		}
		if i > 0 && bytes.Compare(prevMax, b.anchorKey) >= 0 {
			t.Fatalf("non-increasing block ranges at %d prev_max=%q next_anchor=%q", i, prevMax, b.anchorKey)
		}
		prevMax = b.maxKey

		decoded, err := outerleaf.DecodeBlockLeaseWithVerify(b.payload, true)
		if err != nil {
			t.Fatalf("decode block %d: %v", i, err)
		}
		if decoded == nil {
			t.Fatalf("decode block %d returned nil", i)
		}
		count := decoded.EntryCount()
		totalDecoded += count
		if count > maxEntries {
			maxEntries = count
		}
		decoded.Release()
	}
	if totalDecoded != totalEntries {
		t.Fatalf("decoded entry total mismatch got=%d want=%d", totalDecoded, totalEntries)
	}
	if maxEntries >= 65535 {
		t.Fatalf("route rewrite emitted oversized block entries=%d", maxEntries)
	}
}

func TestCachingDB_V1LeafLogRoute_OverlapRewriteRespectsTargetAfterLargeSource(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                dir,
		ChunkSize:          64 * 1024,
		IndexOuterLeafMode: backenddb.IndexOuterLeafModeV1LeafLogRoute,
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold:          1,
			ForcePointers:             true,
			OuterLeafBlockCodec:       backenddb.ValueLogBlockLZ4,
			OuterLeafBlockTargetBytes: 1 << 20,
		},
	})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	defer backend.Close()

	cache, err := Open(dir, backend, Options{
		AllowUnsafe:                       true,
		DisableWAL:                        true,
		FlushThreshold:                    1 << 30,
		MemtableShards:                    1,
		JournalLanes:                      1,
		ForceValueLogPointers:             true,
		ValueLogPointerThreshold:          1,
		IndexOuterLeafMode:                backenddb.IndexOuterLeafModeV1LeafLogRoute,
		ValueLogOuterLeafBlockCodec:       uint8(backenddb.ValueLogBlockLZ4),
		ValueLogOuterLeafBlockTargetBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	keyFor := func(i int) []byte { return []byte(fmt.Sprintf("k%05d", i)) }
	valueFor := func(seed int) []byte { return []byte{byte(seed), byte(seed >> 8), byte(seed >> 16), byte(seed >> 24)} }

	const initialKeys = 55000
	for i := 0; i < initialKeys; i++ {
		if err := cache.Set(keyFor(i), valueFor(i)); err != nil {
			t.Fatalf("seed Set(%d): %v", i, err)
		}
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	// Force subsequent overlap rewrites to respect a much smaller target.
	cache.outerLeafBlockTargetBytes = 4 << 10

	const inserts = 12000
	for i := 0; i < inserts; i++ {
		k := []byte(fmt.Sprintf("k%05da%04d", i*4, i))
		if err := cache.Set(k, valueFor(100000+i)); err != nil {
			t.Fatalf("insert(%d): %v", i, err)
		}
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatalf("rewrite checkpoint: %v", err)
	}

	if cache.valueLogReader == nil {
		t.Fatalf("missing value-log reader")
	}
	it, err := backend.IteratorWithOptions(nil, nil, backenddb.IteratorOptions{
		Mode:              backenddb.IteratorModePointerProjection,
		IncludeTombstones: true,
	})
	if err != nil {
		t.Fatalf("IteratorWithOptions(pointer_projection): %v", err)
	}
	defer it.Close()

	anchors := 0
	maxEntries := 0
	for it.Valid() {
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer == 0 || flags&node.FlagTombstone != 0 {
			it.Next()
			continue
		}
		raw, err := cache.valueLogReader.Read(ptr)
		if err != nil {
			t.Fatalf("read payload ptr=%+v: %v", ptr, err)
		}
		if !outerleaf.HasMagic(raw) {
			it.Next()
			continue
		}
		blk, err := outerleaf.DecodeBlockLeaseWithVerify(raw, true)
		if err != nil {
			t.Fatalf("decode block ptr=%+v: %v", ptr, err)
		}
		if blk == nil {
			t.Fatalf("nil decoded block ptr=%+v", ptr)
		}
		if c := blk.EntryCount(); c > maxEntries {
			maxEntries = c
		}
		blk.Release()
		anchors++
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if anchors == 0 {
		t.Fatalf("expected pointer-backed anchors after overlap rewrite")
	}
	if maxEntries > int(^uint16(0)) {
		t.Fatalf("overlap rewrite exceeded v2 entry-count limit: max_entries_per_block=%d", maxEntries)
	}
	if err := cache.validateV1LeafLogDirectoryInvariants(); err != nil {
		t.Fatalf("v1_leaflog invariant validation: %v", err)
	}
}
