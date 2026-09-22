package commitlog

import "encoding/binary"

const (
	MaxSourceImportMetadataBytesV2 = 2 << 20
	MaxSourceImportPayloadBytesV2  = 24 << 20
)

// CollectionSourceImportPayloadV2 is one atomic typed row/import-progress
// command. Metadata is versioned and validated by the collection executor;
// envelope validation bounds both sections without decoding the typed vectors.
type CollectionSourceImportPayloadV2 struct {
	Metadata []byte
	Inserted CollectionTypedBatchPayload
}

func EncodeCollectionSourceImportPayloadV2(input CollectionSourceImportPayloadV2) ([]byte, error) {
	if len(input.Metadata) == 0 || len(input.Metadata) > MaxSourceImportMetadataBytesV2 || input.Inserted.LegacyProjection {
		return nil, ErrCorrupt
	}
	rows, err := EncodeCollectionTypedBatchPayload(input.Inserted)
	if err != nil {
		return nil, err
	}
	if len(rows) > MaxSourceImportPayloadBytesV2-4-len(input.Metadata) {
		return nil, ErrRecordTooLarge
	}
	out := make([]byte, 4+len(input.Metadata)+len(rows))
	binary.LittleEndian.PutUint32(out, uint32(len(input.Metadata)))
	copy(out[4:], input.Metadata)
	copy(out[4+len(input.Metadata):], rows)
	return out, nil
}

func sourceImportSectionsV2(raw []byte) (metadata, rows []byte, err error) {
	if len(raw) < 4 || len(raw) > MaxSourceImportPayloadBytesV2 {
		return nil, nil, ErrCorrupt
	}
	n := uint64(binary.LittleEndian.Uint32(raw))
	if n == 0 || n > MaxSourceImportMetadataBytesV2 || n > uint64(len(raw)-4) {
		return nil, nil, ErrCorrupt
	}
	return raw[4 : 4+int(n)], raw[4+int(n):], nil
}

func validateCollectionSourceImportPayloadV2(raw []byte) error {
	_, rows, err := sourceImportSectionsV2(raw)
	if err != nil {
		return err
	}
	if err := validateCollectionTypedBatchPayload(rows); err != nil {
		return err
	}
	if rows[collectionTypedBatchFlagsOffset] != 0 {
		return ErrCorrupt
	}
	return nil
}

func DecodeCollectionSourceImportPayloadV2(raw []byte) (CollectionSourceImportPayloadV2, error) {
	metadata, rows, err := sourceImportSectionsV2(raw)
	if err != nil {
		return CollectionSourceImportPayloadV2{}, err
	}
	inserted, err := DecodeCollectionTypedBatchPayload(rows)
	if err != nil {
		return CollectionSourceImportPayloadV2{}, err
	}
	if inserted.LegacyProjection {
		return CollectionSourceImportPayloadV2{}, ErrCorrupt
	}
	return CollectionSourceImportPayloadV2{Metadata: append([]byte(nil), metadata...), Inserted: inserted}, nil
}
