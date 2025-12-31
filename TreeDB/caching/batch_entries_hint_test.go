package caching

import "testing"

func TestBatchEntriesHintPreallocatesNewBatches(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:      true,
		AllowUnsafe:     true,
		FlushThreshold:  1 << 20,
		DisableValueLog: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := db.NewBatch()
	for i := 0; i < 2048; i++ {
		key := []byte{byte(i >> 8), byte(i)}
		val := []byte{0x1}
		if err := b.SetView(key, val); err != nil {
			t.Fatalf("SetView: %v", err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	hint := int(db.batchEntriesCapHint.Load())
	if hint < 2048 {
		t.Fatalf("expected hint >= 2048, got %d", hint)
	}
	if hint > maxBatchEntriesCapAutoHint {
		t.Fatalf("expected hint <= %d, got %d", maxBatchEntriesCapAutoHint, hint)
	}

	b2 := db.NewBatch()
	if cap(b2.entries) < hint {
		t.Fatalf("expected new batch cap >= %d, got %d", hint, cap(b2.entries))
	}
	_ = b2.Close()
	_ = b.Close()
}
