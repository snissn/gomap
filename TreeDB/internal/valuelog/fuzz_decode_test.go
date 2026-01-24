package valuelog

import "testing"

func FuzzDecodeFrame(f *testing.F) {
	raw, _, err := EncodeFrame(0, nil, []Record{{RID: 1, Value: []byte("hello world")}})
	if err == nil && len(raw) > 0 {
		f.Add(raw)
		if len(raw) > 4 {
			f.Add(raw[:len(raw)/2])
		}
	}
	f.Add([]byte{0x00})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{0x01, 0x02, 0x03, 0x04, 0x05})

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _, _, _ = DecodeFrame(data)
	})
}
