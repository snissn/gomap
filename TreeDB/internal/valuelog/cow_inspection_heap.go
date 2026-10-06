//go:build windows || race

package valuelog

// COWInspectionMetadataAllocationSizes reports the separate metadata buffers
// that escape through os.File.ReadAt on this build. Callers must reserve each
// allocator-rounded capacity before Inspect and retain the peak envelope for
// the lifetime of their serialized read workspace. It excludes caller-owned
// file names, record scratch, output and decoder storage.
func COWInspectionMetadataAllocationSizes() [3]uint64 {
	return [3]uint64{HeaderSize, cowMaxPrefix, uint64(compactLeafPagePayloadHeaderSize)}
}
