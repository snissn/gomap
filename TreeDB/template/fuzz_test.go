package template

import "testing"

func FuzzDecodePayload(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("not-a-template"))
	f.Add([]byte{magic0, magic1, payloadVer, flagEncoded})
	lookup := func(uint64) ([]byte, error) { return nil, ErrMissingTemplate }
	opts := DecodeOptions{MaxDecodedBytes: 1 << 20, MaxGaps: 64}
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodePayload(data, lookup, opts)
	})
}
