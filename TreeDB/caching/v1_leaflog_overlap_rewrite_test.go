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
					if mode == backenddb.IndexOuterLeafModeV1LeafLog {
						if !page.ValuePtrIsFenceOuter(ptr) {
							t.Fatalf("expected fence-anchor pointer only in %s overlap rewrite, key=%q ptr=%+v", mode, k, ptr)
						}
					} else {
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
