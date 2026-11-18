//go:build windows || race

package valuelog

// COWInspectionMetadataAllocationSizes reports the original buffer envelope
// for metadata that escapes through os.File.ReadAt on this build. Callers must
// reserve each allocator-rounded capacity before Inspect and retain the peak
// envelope for the lifetime of their serialized read workspace. The combined
// metadata window fits in the original header and prefix allowance. This
// excludes caller-owned filenames, record scratch, output and decoder storage.
func COWInspectionMetadataAllocationSizes() [3]uint64 {
	return [3]uint64{HeaderSize, cowMaxPrefix, uint64(compactLeafPagePayloadHeaderSize)}
}
