package hashdb

import (
	"encoding/binary"
	"testing"
)

func TestProbeGuardTriggersResize(t *testing.T) {
	orig := hashFn
	hashFn = func([]byte) uint64 { return 0 }
	t.Cleanup(func() { hashFn = orig })

	dir := t.TempDir()
	var db DB
	if err := db.Open(dir); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	db.SetResizeThreshold(100)          // disable load-factor resize
	db.SetMaxProbeGroupsBeforeResize(1) // trigger when >1 group scanned

	key := make([]byte, 8)
	val := make([]byte, 16)

	for i := 0; i < 8; i++ {
		binary.LittleEndian.PutUint64(key, uint64(i))
		binary.LittleEndian.PutUint64(val[:8], uint64(i))
		if err := db.Put(key, val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	if db.rehashInProgress {
		t.Fatalf("rehash started early")
	}

	// 9th insert scans a second group due to constant hash collisions.
	binary.LittleEndian.PutUint64(key, uint64(8))
	if err := db.Put(key, val); err != nil {
		t.Fatalf("put 8: %v", err)
	}

	if !db.rehashInProgress {
		t.Fatalf("expected rehash to start after long probe")
	}
	if db.capacity != DefaultCapacity*2 {
		t.Fatalf("expected capacity %d, got %d", DefaultCapacity*2, db.capacity)
	}
}
