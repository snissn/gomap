package mvcckey

import (
	"bytes"
	"errors"
	"testing"
)

func FuzzRoundTrip(f *testing.F) {
	f.Add([]byte(nil), uint64(1))
	f.Add([]byte{0, 1, 0xff, 0}, ^uint64(0))
	f.Add([]byte("logical-key"), uint64(42))
	f.Fuzz(func(t *testing.T, logical []byte, timestamp uint64) {
		if timestamp == 0 {
			timestamp = 1
		}
		encoded, err := Encode(logical, timestamp)
		if err != nil {
			if errors.Is(err, ErrKeyTooLarge) {
				return
			}
			t.Fatalf("Encode: %v", err)
		}
		decoded, gotTimestamp, err := Decode(encoded)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if !bytes.Equal(decoded, logical) || gotTimestamp != timestamp {
			t.Fatalf("Decode=(%x,%d), want (%x,%d)", decoded, gotTimestamp, logical, timestamp)
		}
	})
}

func FuzzDecodeNeverPanics(f *testing.F) {
	f.Add([]byte(nil))
	f.Add(namespaceV1[:])
	valid, err := Encode([]byte{0, 'k'}, 1)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Fuzz(func(t *testing.T, physical []byte) {
		_, _, _ = Decode(physical)
	})
}
