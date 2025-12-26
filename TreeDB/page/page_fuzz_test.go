package page

import "testing"

func FuzzDecodeHeader(f *testing.F) {
	f.Add(make([]byte, PageHeaderSize))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < PageHeaderSize {
			return
		}
		_ = DecodeHeader(data[:PageHeaderSize])
	})
}

func FuzzDecodeValuePtr(f *testing.F) {
	f.Add(make([]byte, ValuePtrSize))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < ValuePtrSize {
			return
		}
		_ = DecodeValuePtr(data[:ValuePtrSize])
	})
}
