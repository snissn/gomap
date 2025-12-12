package page

const (
	// PageSize is the fixed size of pages in index.db.
	PageSize = 4096

	// InlineThresholdDefault is the default value size cutoff for inline storage.
	InlineThresholdDefault = 256

	// InlineHardMin is the minimum allowed inline threshold.
	InlineHardMin = 64

	// InlineHardMax is the maximum allowed inline threshold.
	InlineHardMax = 2048

	// SlabRotateSize is the maximum size of a slab file before rotation.
	SlabRotateSize = 4 << 30 // 4GB

	// HeaderSize is the size in bytes of a page header.
	HeaderSize = 16

	// ValuePtrSize is the size in bytes of a ValuePtr on disk.
	ValuePtrSize = 16
)

