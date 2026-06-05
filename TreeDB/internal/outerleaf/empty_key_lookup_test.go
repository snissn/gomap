package outerleaf

import (
	"bytes"
	"testing"
)

func TestEmptyKeyLookupIsExactNotFirstSentinel(t *testing.T) {
	for _, tc := range []struct {
		name string
		enc  func(t *testing.T, entries []Entry) []byte
	}{
		{
			name: "v1-single-non-empty",
			enc: func(t *testing.T, _ []Entry) []byte {
				t.Helper()
				payload, err := EncodeSingle(nil, []byte("a"), []byte("A"), 0, 4)
				if err != nil {
					t.Fatalf("EncodeSingle: %v", err)
				}
				return payload
			},
		},
		{
			name: "v2-non-empty-first",
			enc: func(t *testing.T, entries []Entry) []byte {
				t.Helper()
				payload, err := EncodeEntries(nil, entries, 0, 2)
				if err != nil {
					t.Fatalf("EncodeEntries: %v", err)
				}
				return payload
			},
		},
		{
			name: "v3-non-empty-first",
			enc: func(t *testing.T, entries []Entry) []byte {
				t.Helper()
				typed := make([]TypedEntry, 0, len(entries))
				for _, entry := range entries {
					typed = append(typed, TypedEntry{Key: entry.Key, Kind: EntryKindInline, Value: entry.Value})
				}
				payload, err := EncodeTypedEntries(nil, typed, 0, 2)
				if err != nil {
					t.Fatalf("EncodeTypedEntries: %v", err)
				}
				return payload
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := tc.enc(t, []Entry{{Key: []byte("a"), Value: []byte("A")}, {Key: []byte("b"), Value: []byte("B")}})
			if val, ok, found, _, err := DecodeValueForKey(payload, []byte{}, nil); err != nil {
				t.Fatalf("DecodeValueForKey(empty): %v", err)
			} else if !ok || found || val != nil {
				t.Fatalf("DecodeValueForKey(empty)=(%q,%t,%t), want ok true found false", val, ok, found)
			}
			entry, ok, found, _, err := DecodeEntryForKey(payload, []byte{}, nil)
			if err != nil {
				t.Fatalf("DecodeEntryForKey(empty): %v", err)
			}
			if !ok || found {
				t.Fatalf("DecodeEntryForKey(empty)=(%+v,%t,%t), want ok true found false", entry, ok, found)
			}
		})
	}
}

func TestEmptyKeyLookupFindsConcreteEmptyFirstKey(t *testing.T) {
	entries := []Entry{{Key: []byte{}, Value: []byte("empty")}, {Key: []byte("a"), Value: []byte("A")}}
	payload, err := EncodeEntries(nil, entries, 0, 2)
	if err != nil {
		t.Fatalf("EncodeEntries: %v", err)
	}
	val, ok, found, _, err := DecodeValueForKey(payload, []byte{}, nil)
	if err != nil {
		t.Fatalf("DecodeValueForKey(empty): %v", err)
	}
	if !ok || !found || !bytes.Equal(val, []byte("empty")) {
		t.Fatalf("DecodeValueForKey(empty)=(%q,%t,%t), want empty", val, ok, found)
	}

	blk, err := DecodeBlock(payload, nil)
	if err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}
	defer blk.Release()
	got, found, err := blk.ValueForKey([]byte{})
	if err != nil {
		t.Fatalf("ValueForKey(empty): %v", err)
	}
	if !found || !bytes.Equal(got, []byte("empty")) {
		t.Fatalf("ValueForKey(empty)=(%q,%t), want empty", got, found)
	}

	typedPayload, err := EncodeTypedEntries(nil, []TypedEntry{
		{Key: []byte{}, Kind: EntryKindInline, Value: []byte("empty-v3")},
		{Key: []byte("a"), Kind: EntryKindInline, Value: []byte("A")},
	}, 0, 2)
	if err != nil {
		t.Fatalf("EncodeTypedEntries: %v", err)
	}
	entry, ok, found, _, err := DecodeEntryForKey(typedPayload, []byte{}, nil)
	if err != nil {
		t.Fatalf("DecodeEntryForKey(v3 empty): %v", err)
	}
	if !ok || !found || !bytes.Equal(entry.Value, []byte("empty-v3")) {
		t.Fatalf("DecodeEntryForKey(v3 empty)=(%+v,%t,%t), want empty-v3", entry, ok, found)
	}
}
