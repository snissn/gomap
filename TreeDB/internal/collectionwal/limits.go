package collectionwal

import "fmt"

const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
)

const (
	MaxEncodedTransactionBytes            = 16 * MiB
	MaxOuterFramePayloadBytes             = MaxEncodedTransactionBytes
	DefaultSegmentBytes                   = 64 * MiB
	MaxSegmentBytes                       = 1 * GiB
	MaxRootDeltasPerTransaction           = 64
	MaxMutatedRootsPerTransaction         = 64
	MaxInlineRootDeltaBytesPerTransaction = 4 * MiB
	MaxInlineRootDeltaBytesPerRoot        = 1 * MiB
	MaxRootDeltaPayloadSideRefBytes       = 64 * MiB
	MaxDecodedRootDeltaEntriesPerTxn      = 262_144
	MaxDeltaKeyBytes                      = 16 * KiB
	MaxDocumentIDBytes                    = MaxDeltaKeyBytes
	MaxInlineDeltaValueBytes              = 1 * MiB
	MaxSideRefsPerTransaction             = 16_384
	MaxDescriptorOpsPerTransaction        = 1_024
	MaxLogicalNameBytes                   = 128
	MaxRootNameBytes                      = 256
	MaxRelativePathBytes                  = 512
	MaxRelativePathComponents             = 16
	MaxRelativePathComponentBytes         = 128
	MaxResolvedAbsolutePathBytes          = 4096
	MaxVarintBytes                        = 10
	MaxCompressedDecodedBytes             = 64 * MiB
	DefaultRecoveryHeapBudgetBytes        = 128 * MiB
	CollectionUIDBytes                    = 16
	CRC32IEEEBytes                        = 4
)

func ValidateEncodedTransactionSize(n uint64) error {
	if n > MaxEncodedTransactionBytes {
		return fmt.Errorf("%w: encoded transaction bytes %d exceeds %d", ErrCollectionWALResourceLimit, n, MaxEncodedTransactionBytes)
	}
	return nil
}

func ValidateFramePayloadSize(n uint64) error {
	if n > MaxOuterFramePayloadBytes {
		return fmt.Errorf("%w: frame payload bytes %d exceeds %d", ErrCollectionWALResourceLimit, n, MaxOuterFramePayloadBytes)
	}
	return nil
}
