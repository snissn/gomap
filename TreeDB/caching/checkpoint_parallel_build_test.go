package caching

import (
	"fmt"
	"testing"
)

func TestCachingDB_CheckpointParallelBuildCompletes(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:        1 << 20, // 1MiB
		MemtableShards:        1,
		MemtableMode:          "hash_sorted",
		MaxQueuedMemtables:    -1,
		FlushBuildConcurrency: 4,
		AllowUnsafe:           true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	writeAndRotate := func(prefix byte, n int) {
		for i := 0; i < n; i++ {
			k := fmt.Sprintf("%c%08d", prefix, i)
			v := fmt.Sprintf("v-%c-%08d", prefix, i)
			if err := db.Set([]byte(k), []byte(v)); err != nil {
				t.Fatalf("Set: %v", err)
			}
		}
		db.mu.Lock()
		if err := db.rotateMemtableLocked(true); err != nil {
			db.mu.Unlock()
			t.Fatalf("rotateMemtableLocked: %v", err)
		}
		db.mu.Unlock()
	}

	// Ensure totalLen crosses the parallel-build threshold in flushCombinedLocked.
	writeAndRotate('a', 6000)
	writeAndRotate('b', 6000)
	writeAndRotate('c', 6000)

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
}
