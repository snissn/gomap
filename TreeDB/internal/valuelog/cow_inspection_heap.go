//go:build windows || race

package valuelog

// COWInspectionMetadataAllocationSizes bounds the metadata buffers that escape
// through os.File.ReadAt on this build. Account for a separate header and prefix
// or a combined window, plus the payload discriminator. Callers reserve each
// rounded capacity for the lifetime of their serialized read workspace.
func COWInspectionMetadataAllocationSizes() [3]uint64 {
	return [3]uint64{HeaderSize, HeaderSize + cowMaxPrefix, uint64(compactLeafPagePayloadHeaderSize)}
}
