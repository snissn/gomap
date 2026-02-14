package outerleaf

import (
	"bytes"
	"fmt"
	"testing"
)

func TestEncodeDecodeSingleRoundTrip(t *testing.T) {
	codecs := []struct {
		name  string
		codec uint8
	}{
		{name: "snappy", codec: 0},
		{name: "lz4", codec: 1},
	}

	for _, tc := range codecs {
		t.Run(tc.name, func(t *testing.T) {
			key := []byte("user:123456")
			value := bytes.Repeat([]byte("abcdef0123456789"), 128)

			enc, err := EncodeSingle(nil, key, value, tc.codec, 16)
			if err != nil {
				t.Fatalf("EncodeSingle: %v", err)
			}
			gotKey, gotVal, ok, _, err := Decode(enc, nil)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !ok {
				t.Fatalf("Decode reported ok=false")
			}
			if !bytes.Equal(gotKey, key) {
				t.Fatalf("key mismatch")
			}
			if !bytes.Equal(gotVal, value) {
				t.Fatalf("value mismatch")
			}

			looked, ok, found, _, err := DecodeValueForKey(enc, key, nil)
			if err != nil {
				t.Fatalf("DecodeValueForKey: %v", err)
			}
			if !ok || !found || !bytes.Equal(looked, value) {
				t.Fatalf("DecodeValueForKey mismatch")
			}
		})
	}
}

func TestEncodeDecodeEntriesLookup(t *testing.T) {
	codecs := []struct {
		name  string
		codec uint8
	}{
		{name: "snappy", codec: 0},
		{name: "lz4", codec: 1},
	}
	entries := []Entry{
		{Key: []byte("acct:0001"), Value: bytes.Repeat([]byte("a"), 64)},
		{Key: []byte("acct:0002"), Value: bytes.Repeat([]byte("b"), 128)},
		{Key: []byte("acct:0003"), Value: bytes.Repeat([]byte("c"), 96)},
	}

	for _, tc := range codecs {
		t.Run(tc.name, func(t *testing.T) {
			enc, err := EncodeEntries(nil, entries, tc.codec, 2)
			if err != nil {
				t.Fatalf("EncodeEntries: %v", err)
			}
			for _, e := range entries {
				got, ok, found, _, err := DecodeValueForKey(enc, e.Key, nil)
				if err != nil {
					t.Fatalf("DecodeValueForKey(%q): %v", string(e.Key), err)
				}
				if !ok || !found {
					t.Fatalf("DecodeValueForKey(%q): ok=%v found=%v", string(e.Key), ok, found)
				}
				if !bytes.Equal(got, e.Value) {
					t.Fatalf("DecodeValueForKey(%q): value mismatch", string(e.Key))
				}
			}
			_, ok, found, _, err := DecodeValueForKey(enc, []byte("acct:9999"), nil)
			if err != nil {
				t.Fatalf("DecodeValueForKey(miss): %v", err)
			}
			if !ok || found {
				t.Fatalf("expected ok=true found=false for miss")
			}
		})
	}
}

func TestEncodeEntriesRequiresIncreasingKeys(t *testing.T) {
	entries := []Entry{
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k1"), Value: []byte("v1")},
	}
	if _, err := EncodeEntries(nil, entries, 0, 16); err == nil {
		t.Fatalf("expected error for unsorted keys")
	}
}

func TestDecodeNonOuterPayload(t *testing.T) {
	raw := []byte("plain value payload")
	gotVal, ok, _, err := DecodeValue(raw, nil)
	if err != nil {
		t.Fatalf("DecodeValue err: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false")
	}
	if gotVal != nil {
		t.Fatalf("expected nil value when ok=false")
	}

	v, ok, found, _, err := DecodeValueForKey(raw, []byte("k"), nil)
	if err != nil {
		t.Fatalf("DecodeValueForKey err: %v", err)
	}
	if ok || found || v != nil {
		t.Fatalf("expected non-outer decode")
	}
}

func TestDecodeBlockValueForKey_RestartIndexed(t *testing.T) {
	entries := make([]Entry, 0, 128)
	for i := 0; i < 128; i++ {
		entries = append(entries, Entry{
			Key:   []byte(fmt.Sprintf("acct:%04d", i)),
			Value: bytes.Repeat([]byte{byte('a' + (i % 26))}, 96),
		})
	}
	enc, err := EncodeEntries(nil, entries, 0, 8)
	if err != nil {
		t.Fatalf("EncodeEntries: %v", err)
	}
	blk, err := DecodeBlock(enc, nil)
	if err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}
	first, err := blk.FirstValue()
	if err != nil {
		t.Fatalf("FirstValue: %v", err)
	}
	if !bytes.Equal(first, entries[0].Value) {
		t.Fatalf("FirstValue mismatch")
	}

	check := func(idx int) {
		t.Helper()
		got, found, err := blk.ValueForKey(entries[idx].Key)
		if err != nil {
			t.Fatalf("ValueForKey(%q): %v", string(entries[idx].Key), err)
		}
		if !found {
			t.Fatalf("ValueForKey(%q): expected found", string(entries[idx].Key))
		}
		if !bytes.Equal(got, entries[idx].Value) {
			t.Fatalf("ValueForKey(%q): value mismatch", string(entries[idx].Key))
		}
	}

	check(0)
	check(7)
	check(8)
	check(63)
	check(127)

	for _, miss := range [][]byte{[]byte("acct:-001"), []byte("acct:9999")} {
		got, found, err := blk.ValueForKey(miss)
		if err != nil {
			t.Fatalf("ValueForKey miss(%q): %v", string(miss), err)
		}
		if found || got != nil {
			t.Fatalf("ValueForKey miss(%q): found=%v got=%v", string(miss), found, got)
		}
	}
}

func BenchmarkDecodedBlockValueForKeyV2(b *testing.B) {
	entries := make([]Entry, 0, 256)
	for i := 0; i < 256; i++ {
		entries = append(entries, Entry{
			Key:   []byte(fmt.Sprintf("k:%06d", i)),
			Value: bytes.Repeat([]byte{byte('a' + (i % 26))}, 128),
		})
	}
	enc, err := EncodeEntries(nil, entries, 0, 16)
	if err != nil {
		b.Fatalf("EncodeEntries: %v", err)
	}
	blk, err := DecodeBlock(enc, nil)
	if err != nil {
		b.Fatalf("DecodeBlock: %v", err)
	}
	keys := make([][]byte, len(entries))
	for i := range entries {
		keys[i] = entries[i].Key
	}
	miss := []byte("k:999999")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		if i&7 == 0 {
			key = miss
		}
		if _, _, err := blk.ValueForKey(key); err != nil {
			b.Fatalf("ValueForKey: %v", err)
		}
	}
}
