package caching

import "testing"

func TestBatchWrite_ReleasesEntrySliceAfterWrite(t *testing.T) {
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
	if err := b.SetView([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("SetView: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if b.entries != nil {
		t.Fatalf("expected entries slice to be released after Write")
	}

	if err := b.SetView([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("SetView again: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write again: %v", err)
	}
	if b.entries != nil {
		t.Fatalf("expected entries slice to be released after second Write")
	}

	got, err := db.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get k1: %v", err)
	}
	if string(got) != "v1" {
		t.Fatalf("unexpected k1 value: %q", got)
	}
	got, err = db.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("Get k2: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("unexpected k2 value: %q", got)
	}

	_ = b.Close()
}
