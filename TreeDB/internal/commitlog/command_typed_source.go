package commitlog

import (
	"bytes"
	"encoding/binary"
)

// CollectionTypedSourcePayload is one atomic source command, not two commands.
type CollectionTypedSourcePayload struct {
	DeleteIDs [][]byte
	Inserted  CollectionTypedBatchPayload
}

func EncodeCollectionTypedSourcePayload(deleteIDs [][]byte, inserted CollectionTypedBatchPayload) ([]byte, error) {
	if inserted.LegacyProjection {
		return nil, ErrCorrupt
	}
	deleted, err := EncodeCollectionDeleteBatchByIDPayload(inserted.Collection, deleteIDs)
	if err != nil {
		return nil, err
	}
	rows, err := EncodeCollectionTypedBatchPayload(inserted)
	if err != nil {
		return nil, err
	}
	if commandFrameIntExceedsUint32(len(deleted)) {
		return nil, ErrRecordTooLarge
	}
	n, err := addCommandFrameEncodedSectionLen(4, len(deleted))
	if err != nil {
		return nil, err
	}
	n, err = addCommandFrameEncodedSectionLen(n, len(rows))
	if err != nil {
		return nil, err
	}
	out := make([]byte, n)
	binary.LittleEndian.PutUint32(out, uint32(len(deleted)))
	copy(out[4:], deleted)
	copy(out[4+len(deleted):], rows)
	return out, nil
}

func collectionTypedSourceSections(raw []byte) (deleted, inserted []byte, err error) {
	if len(raw) < 4 || uint64(binary.LittleEndian.Uint32(raw)) > uint64(len(raw)-4) {
		return nil, nil, ErrCorrupt
	}
	n := int(binary.LittleEndian.Uint32(raw))
	return raw[4 : 4+n], raw[4+n:], nil
}

// Reuse the non-owning section validators; frame validation must not allocate
// the typed rows/vectors which the replay executor will subsequently own.
func validateCollectionTypedSourcePayload(raw []byte) error {
	deleted, inserted, err := collectionTypedSourceSections(raw)
	if err != nil {
		return err
	}
	if err := validateCollectionDeleteBatchByIDPayload(deleted); err != nil {
		return err
	}
	if err := validateCollectionTypedBatchPayload(inserted); err != nil {
		return err
	}
	if inserted[collectionTypedBatchFlagsOffset] != 0 {
		return ErrCorrupt
	}
	name, _, _, err := parseCollectionBatchHeader(deleted)
	if err != nil {
		return err
	}
	// The typed validator above has checked this header and collection length.
	n := int(binary.LittleEndian.Uint32(inserted[collectionTypedBatchPrefixSize:]))
	if !bytes.Equal(name, inserted[collectionTypedBatchHeaderSize:collectionTypedBatchHeaderSize+n]) {
		return ErrCorrupt
	}
	return nil
}

func DecodeCollectionTypedSourcePayload(raw []byte) (CollectionTypedSourcePayload, error) {
	var out CollectionTypedSourcePayload
	deleteRaw, insertRaw, err := collectionTypedSourceSections(raw)
	if err != nil {
		return out, err
	}
	deleted, err := DecodeCollectionDeleteBatchByIDPayload(deleteRaw)
	if err != nil {
		return out, err
	}
	inserted, err := DecodeCollectionTypedBatchPayload(insertRaw)
	if err != nil {
		return out, err
	}
	if deleted.Collection != inserted.Collection || inserted.LegacyProjection {
		return out, ErrCorrupt
	}
	return CollectionTypedSourcePayload{DeleteIDs: deleted.IDs, Inserted: inserted}, nil
}
