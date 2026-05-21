//go:build nativecrc && cgo

package hashtournament

func nativeCRCBenchmarks() []benchmarkCase {
	return []benchmarkCase{
		{
			name: "ZlibCRC32_Cgo",
			run: func(data []byte) {
				sink32 ^= zlibCRC32(data)
			},
		},
		{
			name: "LibdeflateCRC32_Cgo",
			run: func(data []byte) {
				sink32 ^= libdeflateCRC32(data)
			},
		},
	}
}
