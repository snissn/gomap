package collectionwal

import "fmt"

type RefClass uint8

const (
	RefClassInvalid RefClass = iota
	RefClassValueLogRecord
	RefClassLeafLogRecord
	RefClassRootDeltaPayload
	RefClassColumnManifest
	RefClassColumnSubstreamFile
	RefClassColumnFilterFile
	RefClassColumnDeleteBitmapFile
	RefClassColumnDictionaryFile
	RefClassColumnMetadataFile
)

func (c RefClass) KnownV1() bool {
	switch c {
	case RefClassValueLogRecord,
		RefClassLeafLogRecord,
		RefClassRootDeltaPayload,
		RefClassColumnManifest,
		RefClassColumnSubstreamFile,
		RefClassColumnFilterFile,
		RefClassColumnDeleteBitmapFile,
		RefClassColumnDictionaryFile,
		RefClassColumnMetadataFile:
		return true
	default:
		return false
	}
}

func ValidateV1RefClass(c RefClass) error {
	if c.KnownV1() {
		return nil
	}
	return fmt.Errorf("%w: unknown ref class %d", ErrCollectionWALUnsupportedVersion, c)
}

func (c RefClass) String() string {
	switch c {
	case RefClassValueLogRecord:
		return "ValueLogRecord"
	case RefClassLeafLogRecord:
		return "LeafLogRecord"
	case RefClassRootDeltaPayload:
		return "RootDeltaPayload"
	case RefClassColumnManifest:
		return "ColumnManifest"
	case RefClassColumnSubstreamFile:
		return "ColumnSubstreamFile"
	case RefClassColumnFilterFile:
		return "ColumnFilterFile"
	case RefClassColumnDeleteBitmapFile:
		return "ColumnDeleteBitmapFile"
	case RefClassColumnDictionaryFile:
		return "ColumnDictionaryFile"
	case RefClassColumnMetadataFile:
		return "ColumnMetadataFile"
	default:
		return fmt.Sprintf("RefClass(%d)", c)
	}
}
