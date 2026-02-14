package outerleaf

import (
	"bytes"
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
