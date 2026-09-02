package mvcckey

import (
	"fmt"
	"testing"
)

func BenchmarkEncode(b *testing.B) {
	for _, size := range []int{16, 64, 256} {
		key := benchmarkKey(size)
		b.Run(fmt.Sprintf("bytes=%d/allocate", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				encoded, err := Encode(key, uint64(i)+1)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkBytes = encoded
			}
		})
		b.Run(fmt.Sprintf("bytes=%d/reuse", size), func(b *testing.B) {
			needed, err := EncodedLen(key)
			if err != nil {
				b.Fatal(err)
			}
			dst := make([]byte, 0, needed)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				encoded, err := Append(dst[:0], key, uint64(i)+1)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkBytes = encoded
			}
		})
	}
}

func BenchmarkDecode(b *testing.B) {
	for _, size := range []int{16, 64, 256} {
		encoded, err := Encode(benchmarkKey(size), 42)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("bytes=%d/allocate", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				decoded, timestamp, err := Decode(encoded)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkBytes, benchmarkTimestamp = decoded, timestamp
			}
		})
		b.Run(fmt.Sprintf("bytes=%d/reuse", size), func(b *testing.B) {
			dst := make([]byte, 0, size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				decoded, timestamp, err := DecodeAppend(dst[:0], encoded)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkBytes, benchmarkTimestamp = decoded, timestamp
			}
		})
	}
}

func BenchmarkLogicalPrefixBounds(b *testing.B) {
	key := benchmarkKey(64)
	lower := make([]byte, 0, 128)
	upper := make([]byte, 0, 128)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var err error
		lower, err = AppendLogicalPrefixLower(lower[:0], key)
		if err != nil {
			b.Fatal(err)
		}
		upper, err = AppendLogicalPrefixUpper(upper[:0], key)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkBytes = lower
		benchmarkBytes2 = upper
	}
}

func benchmarkKey(size int) []byte {
	key := make([]byte, size)
	for i := range key {
		key[i] = byte(i*31 + 7)
		if i%17 == 0 {
			key[i] = 0
		}
	}
	return key
}

var (
	benchmarkBytes     []byte
	benchmarkBytes2    []byte
	benchmarkTimestamp uint64
)
