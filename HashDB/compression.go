package hashdb

import "github.com/klauspost/compress/s2"

func compressValueIfBeneficial(val []byte) ([]byte, bool) {
	compressed := s2.Encode(nil, val)
	if len(compressed) < len(val) {
		return compressed, true
	}
	return nil, false
}
