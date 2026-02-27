package caching

import (
	"bytes"
	"encoding/binary"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func routeIAVLStoreKey(storePrefix string, version uint64, nonce uint32) []byte {
	key := make([]byte, len(storePrefix)+13)
	copy(key, []byte(storePrefix))
	key[len(storePrefix)] = 's'
	binary.BigEndian.PutUint64(key[len(storePrefix)+1:len(storePrefix)+9], version)
	binary.BigEndian.PutUint32(key[len(storePrefix)+9:len(storePrefix)+13], nonce)
	return key
}

func TestCachingDB_V1LeafLogRoute_PrefixedBinaryRootsVisibleAfterCheckpoint(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                dir,
		ChunkSize:          64 * 1024,
		IndexOuterLeafMode: backenddb.IndexOuterLeafModeV1LeafLogRoute,
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold:          1,
			ForcePointers:             true,
			OuterLeafBlockCodec:       backenddb.ValueLogBlockSnappy,
			OuterLeafBlockTargetBytes: 4 << 10,
		},
	})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	defer backend.Close()

	cache, err := Open(dir, backend, Options{
		AllowUnsafe:              true,
		DisableWAL:               false,
		RelaxedSync:              true,
		FlushThreshold:           64 << 20,
		MemtableShards:           1,
		ForceValueLogPointers:    true,
		ValueLogPointerThreshold: 1,
		IndexOuterLeafMode:       backenddb.IndexOuterLeafModeV1LeafLogRoute,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	const (
		targetVersion = uint64(9988000)
		olderVersion  = uint64(9987746)
	)

	accRootVal := bytes.Repeat([]byte{0x11}, 73)
	authzOldVal := bytes.Repeat([]byte{0x22}, 122)
	authzRootVal := bytes.Repeat([]byte{0x33}, 13)

	writeBatch := func(prefix string, startVersion uint64, count int, rootVersion uint64, rootVal []byte, extraRootVersion uint64, extraRootVal []byte) {
		b := cache.NewBatch()
		for i := 0; i < count; i++ {
			version := startVersion + uint64(i)
			key := routeIAVLStoreKey(prefix, version, uint32((i%31)+2))
			val := bytes.Repeat([]byte{byte((i % 251) + 1)}, 96)
			if err := b.Set(key, val); err != nil {
				t.Fatalf("batch set key=%x: %v", key, err)
			}
		}
		if err := b.Set(routeIAVLStoreKey(prefix, rootVersion, 1), rootVal); err != nil {
			t.Fatalf("batch set root: %v", err)
		}
		if extraRootVersion > 0 {
			if err := b.Set(routeIAVLStoreKey(prefix, extraRootVersion, 1), extraRootVal); err != nil {
				t.Fatalf("batch set extra root: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("batch WriteSync: %v", err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("batch close: %v", err)
		}
		if err := cache.Checkpoint(); err != nil {
			t.Fatalf("checkpoint: %v", err)
		}
	}

	writeBatch("s/k:acc/", targetVersion-9400, 9400, targetVersion, accRootVal, 0, nil)
	got, err := cache.Get(routeIAVLStoreKey("s/k:acc/", targetVersion, 1))
	if err != nil {
		t.Fatalf("acc get: %v", err)
	}
	if !bytes.Equal(got, accRootVal) {
		t.Fatalf("acc root mismatch got=%d want=%d", len(got), len(accRootVal))
	}

	writeBatch("s/k:authz/", targetVersion-5560, 5560, olderVersion, authzOldVal, targetVersion, authzRootVal)
	got, err = cache.Get(routeIAVLStoreKey("s/k:authz/", targetVersion, 1))
	if err != nil {
		t.Fatalf("authz get: %v", err)
	}
	if !bytes.Equal(got, authzRootVal) {
		t.Fatalf("authz root mismatch got=%d want=%d", len(got), len(authzRootVal))
	}
}
