package valuelog

import "testing"

func BenchmarkShouldKeepCompressed(b *testing.B) {
	raw := 4096
	encoded := 2048
	encodeNs := int64(40000)
	ioNsPerStored := 50.0
	safety := DefaultKeepSafetyMargin
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ShouldKeepCompressed(raw, encoded, encodeNs, ioNsPerStored, safety)
	}
}
