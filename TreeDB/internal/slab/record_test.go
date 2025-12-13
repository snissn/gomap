package slab

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/page"
)

func TestRecordRoundTrip(t *testing.T) {
	key := []byte("key-1")
	val := []byte("value-1")

	rec, ptr, err := EncodeRecordAt(key, val, 7, 0)
	if err != nil {
		t.Fatalf("EncodeRecordAt error: %v", err)
	}

	if ptr.Offset != 4 {
		t.Fatalf("ptr.Offset: got %d want 4", ptr.Offset)
	}
	wantLen := uint32(2 + 4 + len(key) + len(val))
	if ptr.Length != wantLen {
		t.Fatalf("ptr.Length: got %d want %d", ptr.Length, wantLen)
	}

	k2, v2, err := DecodeRecord(rec, ptr)
	if err != nil {
		t.Fatalf("DecodeRecord error: %v", err)
	}
	if string(k2) != string(key) || string(v2) != string(val) {
		t.Fatalf("round-trip mismatch: got (%q,%q) want (%q,%q)", k2, v2, key, val)
	}
}

func TestRecordCRCMismatch(t *testing.T) {
	key := []byte("key")
	val := []byte("value")
	rec, ptr, err := EncodeRecordAt(key, val, 0, 0)
	if err != nil {
		t.Fatalf("EncodeRecordAt error: %v", err)
	}
	rec[len(rec)-1] ^= 0xff

	_, _, err = DecodeRecord(rec, ptr)
	if !errors.Is(err, crc.ErrChecksumMismatch) {
		t.Fatalf("expected crc mismatch, got %v", err)
	}
}

func TestSequentialEnumeration(t *testing.T) {
	pairs := []struct {
		key []byte
		val []byte
	}{
		{[]byte("a"), []byte("1")},
		{[]byte("bb"), []byte("22")},
		{[]byte("ccc"), []byte("333")},
		{[]byte("dddd"), []byte("4444")},
	}

	var buf []byte
	var ptrs []page.ValuePtr
	var off uint64
	for _, p := range pairs {
		rec, ptr, err := EncodeRecordAt(p.key, p.val, 0, off)
		if err != nil {
			t.Fatalf("EncodeRecordAt error: %v", err)
		}
		buf = append(buf, rec...)
		ptrs = append(ptrs, ptr)
		off += uint64(len(rec))
	}

	for i, ptr := range ptrs {
		k, v, err := DecodeRecord(buf, ptr)
		if err != nil {
			t.Fatalf("DecodeRecord error at %d: %v", i, err)
		}
		if string(k) != string(pairs[i].key) || string(v) != string(pairs[i].val) {
			t.Fatalf("decoded mismatch at %d: got (%q,%q) want (%q,%q)", i, k, v, pairs[i].key, pairs[i].val)
		}
		if i < len(ptrs)-1 {
			next := NextRecordOffset(ptr)
			if next != ptrs[i+1].Offset {
				t.Fatalf("NextRecordOffset mismatch at %d: got %d want %d", i, next, ptrs[i+1].Offset)
			}
		}
	}
}

