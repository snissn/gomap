package caching

import (
	"bytes"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestV1LeafLogInvariantValidation_NonPanicErrorPath(t *testing.T) {
	for _, mode := range []string{
		backenddb.IndexOuterLeafModeV1LeafLog,
		backenddb.IndexOuterLeafModeV1LeafLogRoute,
	} {
		t.Run(mode, func(t *testing.T) {
			db := &DB{
				indexOuterLeafMode:            mode,
				debugV1LeafLogInvariants:      true,
				debugV1LeafLogInvariantsPanic: false,
			}
			if err := db.maybeValidateV1LeafLogInvariants(); err == nil {
				t.Fatalf("expected invariant validation error when value-log reader is missing")
			}
		})
	}
}

func TestV1LeafLogInvariantValidation_PanicPath(t *testing.T) {
	for _, mode := range []string{
		backenddb.IndexOuterLeafModeV1LeafLog,
		backenddb.IndexOuterLeafModeV1LeafLogRoute,
	} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			db := &DB{
				indexOuterLeafMode:            mode,
				debugV1LeafLogInvariants:      true,
				debugV1LeafLogInvariantsPanic: true,
			}
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected panic on invariant violation when panic mode is enabled")
				}
			}()
			_ = db.maybeValidateV1LeafLogInvariants()
		})
	}
}

func TestCachingDB_V1LeafLog_DirectoryInvariantsHold(t *testing.T) {
	for _, mode := range []string{
		backenddb.IndexOuterLeafModeV1LeafLog,
		backenddb.IndexOuterLeafModeV1LeafLogRoute,
	} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			backend, err := backenddb.Open(backenddb.Options{
				Dir:                dir,
				ChunkSize:          64 * 1024,
				IndexOuterLeafMode: mode,
				ValueLog: backenddb.ValueLogOptions{
					PointerThreshold:          1,
					ForcePointers:             true,
					OuterLeafBlockTargetBytes: 1 << 20,
				},
			})
			if err != nil {
				t.Fatalf("backend open: %v", err)
			}
			defer backend.Close()

			cache, err := Open(dir, backend, Options{
				AllowUnsafe:              true,
				DisableWAL:               true,
				FlushThreshold:           1 << 30,
				MemtableShards:           1,
				JournalLanes:             1,
				ForceValueLogPointers:    true,
				ValueLogPointerThreshold: 1,
				IndexOuterLeafMode:       mode,
			})
			if err != nil {
				t.Fatalf("cache open: %v", err)
			}
			defer cache.Close()

			for i := 0; i < 256; i++ {
				k := []byte(fmt.Sprintf("k%04d", i))
				v := []byte(fmt.Sprintf("v%04d", i))
				if err := cache.Set(k, v); err != nil {
					t.Fatalf("seed set %d: %v", i, err)
				}
			}
			if err := cache.Checkpoint(); err != nil {
				t.Fatalf("checkpoint seed: %v", err)
			}
			for i := 0; i < 256; i += 3 {
				k := []byte(fmt.Sprintf("k%04d", i))
				if err := cache.Delete(k); err != nil {
					t.Fatalf("delete %d: %v", i, err)
				}
			}
			for i := 1; i < 256; i += 7 {
				k := []byte(fmt.Sprintf("k%04d", i))
				v := []byte(fmt.Sprintf("vx%04d", i))
				if err := cache.Set(k, v); err != nil {
					t.Fatalf("overwrite %d: %v", i, err)
				}
			}
			for i := 0; i < 32; i++ {
				k := []byte(fmt.Sprintf("kz%04d", i))
				v := []byte(fmt.Sprintf("vz%04d", i))
				if err := cache.Set(k, v); err != nil {
					t.Fatalf("append %d: %v", i, err)
				}
			}
			if err := cache.Checkpoint(); err != nil {
				t.Fatalf("checkpoint mutate: %v", err)
			}
			if err := cache.validateV1LeafLogDirectoryInvariants(); err != nil {
				t.Fatalf("validate invariants: %v", err)
			}
		})
	}
}

func TestCachingDB_V1LeafLogRoute_SmallValuesPublishToValueLogAnchors(t *testing.T) {
	const (
		keyCount  = 128
		threshold = 1 << 20
	)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                dir,
		ChunkSize:          64 * 1024,
		IndexOuterLeafMode: backenddb.IndexOuterLeafModeV1LeafLogRoute,
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold:          threshold,
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
		ValueLogPointerThreshold:          threshold,
		IndexOuterLeafMode:                backenddb.IndexOuterLeafModeV1LeafLogRoute,
		ValueLogOuterLeafBlockTargetBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	keyFor := func(i int) []byte {
		return []byte(fmt.Sprintf("k%06d", i))
	}
	valueFor := []byte("small-value-payload")

	expected := make(map[string]struct{}, keyCount)
	for i := 0; i < keyCount; i++ {
		key := keyFor(i)
		if len(valueFor) > 0 {
			// keep it small so it is well below threshold
			if len(valueFor) > threshold {
				t.Fatalf("value fixture should be small")
			}
		}
		expected[string(key)] = struct{}{}
		if err := cache.Set(key, append([]byte(nil), valueFor...)); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	if cache.valueLogReader == nil {
		t.Fatalf("missing value-log reader")
	}
	it, err := backend.IteratorWithOptions(nil, nil, backenddb.IteratorOptions{
		Mode:              backenddb.IteratorModePointerProjection,
		IncludeTombstones: true,
	})
	if err != nil {
		t.Fatalf("pointer-projection iterator: %v", err)
	}
	defer it.Close()

	seen := make(map[string]struct{}, keyCount)
	pointerRows := 0
	for it.Valid() {
		key := it.UnsafeKey()
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagTombstone != 0 {
			it.Next()
			continue
		}
		if flags&node.FlagPointer == 0 {
			t.Fatalf("route mode should emit only outer-leaf anchors, got inline row key=%q flags=%#x", key, flags)
		}
		raw, readErr := cache.valueLogReader.Read(ptr)
		if readErr != nil {
			t.Fatalf("read payload %q: %v", key, readErr)
		}
		keys, decodeErr := outerleaf.DecodeKeysWithVerify(raw, true)
		if decodeErr != nil {
			t.Fatalf("decode payload %q: %v", key, decodeErr)
		}
		for i := range keys {
			delete(expected, string(keys[i]))
			seen[string(keys[i])] = struct{}{}
		}
		pointerRows++
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if pointerRows == 0 {
		t.Fatalf("expected at least one anchor row in route mode")
	}
	if len(expected) != 0 {
		t.Fatalf("not all keys accounted for in anchor payloads, missing=%d", len(expected))
	}
	if pointerRows >= keyCount {
		t.Fatalf("unexpected inline fanout in route mode pointerRows=%d keyCount=%d", pointerRows, keyCount)
	}
	// Route fanout should coalesce at least some adjacent keys into same block.
	if pointerRows > keyCount/2 {
		t.Fatalf("poor fanout for small values in route mode pointerRows=%d keyCount=%d", pointerRows, keyCount)
	}
	if len(seen) != keyCount {
		t.Fatalf("expected %d unique keys, got %d", keyCount, len(seen))
	}

}

func TestCachingDB_V1LeafLogRoute_NoFenceMarkedAnchorsAfterMixedCRUDReopen(t *testing.T) {
	const (
		seedCount  = 256
		extraCount = 32
		threshold  = 1 << 20
	)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                dir,
		ChunkSize:          64 * 1024,
		IndexOuterLeafMode: backenddb.IndexOuterLeafModeV1LeafLogRoute,
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold:          1,
			OuterLeafBlockTargetBytes: 1 << 20,
		},
	})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	defer backend.Close()

	opts := Options{
		AllowUnsafe:                       true,
		DisableWAL:                        true,
		FlushThreshold:                    1 << 30,
		MemtableShards:                    1,
		JournalLanes:                      1,
		ForceValueLogPointers:             false,
		ValueLogPointerThreshold:          threshold,
		IndexOuterLeafMode:                backenddb.IndexOuterLeafModeV1LeafLogRoute,
		ValueLogOuterLeafBlockTargetBytes: 1 << 20,
	}

	cache, err := Open(dir, backend, opts)
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}

	keyFor := func(i int) []byte {
		return []byte(fmt.Sprintf("k%06d", i))
	}

	expected := make(map[string][]byte, seedCount+extraCount)
	setValue := func(i int, value string) error {
		key := keyFor(i)
		b := []byte(value)
		if err := cache.Set(key, b); err != nil {
			return err
		}
		expected[string(key)] = append([]byte(nil), b...)
		return nil
	}
	deleteValue := func(i int) error {
		key := keyFor(i)
		if err := cache.Delete(key); err != nil {
			return err
		}
		delete(expected, string(key))
		return nil
	}

	for i := 0; i < seedCount; i++ {
		if err := setValue(i, fmt.Sprintf("seed-%04d", i)); err != nil {
			t.Fatalf("seed set %d: %v", i, err)
		}
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatalf("checkpoint seed: %v", err)
	}

	for i := 0; i < seedCount; i++ {
		switch i % 3 {
		case 0:
			if err := deleteValue(i); err != nil {
				t.Fatalf("delete %d: %v", i, err)
			}
		case 1:
			if err := setValue(i, fmt.Sprintf("rewrite-%04d", i)); err != nil {
				t.Fatalf("overwrite %d: %v", i, err)
			}
		}
	}
	for i := 0; i < extraCount; i++ {
		key := []byte(fmt.Sprintf("z%06d", i))
		val := []byte(fmt.Sprintf("append-%04d", i))
		if err := cache.Set(key, val); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		expected[string(key)] = append([]byte(nil), val...)
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatalf("checkpoint mutate: %v", err)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("cache close: %v", err)
	}
	backend.Close()

	backend2, err := backenddb.Open(backenddb.Options{
		Dir:                dir,
		ChunkSize:          64 * 1024,
		IndexOuterLeafMode: backenddb.IndexOuterLeafModeV1LeafLogRoute,
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold:          1,
			OuterLeafBlockTargetBytes: 1 << 20,
		},
	})
	if err != nil {
		t.Fatalf("backend reopen: %v", err)
	}
	defer backend2.Close()

	reopened, err := Open(dir, backend2, opts)
	if err != nil {
		t.Fatalf("reopen cache: %v", err)
	}
	defer reopened.Close()

	it, err := backend2.IteratorWithOptions(nil, nil, backenddb.IteratorOptions{
		Mode:              backenddb.IteratorModePointerProjection,
		IncludeTombstones: true,
	})
	if err != nil {
		t.Fatalf("pointer-projection iterator: %v", err)
	}
	defer it.Close()

	pointerRows := 0
	for it.Valid() {
		key := it.UnsafeKey()
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagTombstone != 0 {
			it.Next()
			continue
		}
		if flags&node.FlagPointer == 0 {
			t.Fatalf("route mode should emit only outer-leaf anchors, got inline row key=%q flags=%#x", key, flags)
		}
		if page.ValuePtrIsFenceOuter(ptr) {
			t.Fatalf("route mode emitted fence-marked anchor pointer after mixed CRUD + reopen: key=%q ptr=%+v", key, ptr)
		}
		pointerRows++
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if pointerRows == 0 {
		t.Fatalf("expected pointer anchors after reopen")
	}

	got, getErr := reopened.Get([]byte("z000000"))
	if getErr != nil {
		t.Fatalf("reopen get append key: %v", getErr)
	}
	if !bytes.Equal(got, []byte("append-0000")) {
		t.Fatalf("append key mismatch after reopen: got=%q want=%q", got, []byte("append-0000"))
	}
}
