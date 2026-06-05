package batch

import (
	"bytes"
	"testing"
)

func TestRawKVParityBatchSetDeleteReplayEmptyKeyNilValue(t *testing.T) {
	b := New(nil, 1024)
	defer func() { _ = b.Close() }()

	if err := b.Set(nil, nil); err != nil {
		t.Fatalf("Set(nil,nil): %v", err)
	}
	if err := b.Set([]byte("k"), nil); err != nil {
		t.Fatalf("Set(k,nil): %v", err)
	}
	if err := b.Delete(nil); err != nil {
		t.Fatalf("Delete(nil): %v", err)
	}

	var entries []Entry
	if err := b.Replay(func(e Entry) error {
		entries = append(entries, e)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("Replay entries=%d want 3", len(entries))
	}
	if entries[0].Type != OpPut || entries[0].Key == nil || len(entries[0].Key) != 0 || entries[0].Value == nil || len(entries[0].Value) != 0 {
		t.Fatalf("entry[0]=%+v, want non-nil empty key/value put", entries[0])
	}
	if entries[1].Type != OpPut || !bytes.Equal(entries[1].Key, []byte("k")) || entries[1].Value == nil || len(entries[1].Value) != 0 {
		t.Fatalf("entry[1]=%+v, want k -> non-nil empty value put", entries[1])
	}
	if entries[2].Type != OpDelete || entries[2].Key == nil || len(entries[2].Key) != 0 {
		t.Fatalf("entry[2]=%+v, want non-nil empty-key delete", entries[2])
	}
}

func TestRawKVParityBatchDeleteRangeNilAndEmptyBoundsDistinct(t *testing.T) {
	if !IsDeleteRangeNoop(nil, []byte{}) {
		t.Fatal("DeleteRange(nil, empty) should be a no-op because empty is the minimum exclusive end")
	}
	if IsDeleteRangeNoop([]byte{}, []byte("a")) {
		t.Fatal("DeleteRange(empty, a) should be non-empty and include the empty key")
	}
	if IsDeleteRangeNoop(nil, nil) {
		t.Fatal("DeleteRange(nil, nil) should cover the full keyspace")
	}
}
