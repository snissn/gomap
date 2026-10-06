//go:build !windows && !race

package valuelog

// COWInspectionMetadataAllocationSizes is zero when the metadata buffers used
// by Inspect stay on the stack. Record, output and decoder storage is separate.
func COWInspectionMetadataAllocationSizes() [3]uint64 { return [3]uint64{} }
