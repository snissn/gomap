package caching

import (
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestV1LeafLogInvariantValidation_NonPanicErrorPath(t *testing.T) {
	db := &DB{
		indexOuterLeafMode:            backenddb.IndexOuterLeafModeV1LeafLog,
		debugV1LeafLogInvariants:      true,
		debugV1LeafLogInvariantsPanic: false,
	}
	if err := db.maybeValidateV1LeafLogInvariants(); err == nil {
		t.Fatalf("expected invariant validation error when value-log reader is missing")
	}
}

func TestV1LeafLogInvariantValidation_PanicPath(t *testing.T) {
	db := &DB{
		indexOuterLeafMode:            backenddb.IndexOuterLeafModeV1LeafLog,
		debugV1LeafLogInvariants:      true,
		debugV1LeafLogInvariantsPanic: true,
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on invariant violation when panic mode is enabled")
		}
	}()
	_ = db.maybeValidateV1LeafLogInvariants()
}

func TestCachingDB_V1LeafLog_DirectoryInvariantsHold(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                dir,
		ChunkSize:          64 * 1024,
		IndexOuterLeafMode: backenddb.IndexOuterLeafModeV1LeafLog,
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
		IndexOuterLeafMode:       backenddb.IndexOuterLeafModeV1LeafLog,
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
}
