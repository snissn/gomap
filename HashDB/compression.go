package hashdb

import "github.com/snissn/compress/s2"

const minValueBytesForCompression = 32

func compressValueIfEnabled(enabled bool, val []byte) ([]byte, bool) {
	if !enabled || len(val) <= minValueBytesForCompression {
		return nil, false
	}
	return compressValueIfBeneficial(val)
}

func compressValueIfBeneficial(val []byte) ([]byte, bool) {
	compressed := s2.Encode(nil, val)
	if len(compressed) < len(val) {
		return compressed, true
	}
	return nil, false
}
