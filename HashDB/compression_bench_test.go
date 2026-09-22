package hashdb

import (
	"bytes"
	"testing"
)

func BenchmarkCompressionMatrix(b *testing.B) {
	cases := []struct {
		name string
		data func(int) []byte
	}{
		{name: "zeros", data: func(n int) []byte { return make([]byte, n) }},
		{name: "repeated-a", data: func(n int) []byte { return bytes.Repeat([]byte("a"), n) }},
		{name: "patterned", data: func(n int) []byte {
			out := make([]byte, n)
			for i := range out {
				out[i] = byte(i*31 + 7)
			}
			return out
		}},
	}

	sizes := []int{16, 32, 64, 128, 1024, 4096, 16384}

	for _, tc := range cases {
		for _, size := range sizes {
			b.Run(tc.name+"-"+itoaBench(size), func(b *testing.B) {
				val := tc.data(size)
				b.ReportAllocs()
				b.SetBytes(int64(len(val)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = compressValueIfEnabled(true, val)
				}
			})
		}
	}
}

func itoaBench(v int) string {
	// Keep this benchmark file dependency-free.
	if v == 0 {
		return "0"
	}
	var buf [32]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
