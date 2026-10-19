package commitlog

import "encoding/binary"

// CatalogIndexMutationPayload carries a logical schema transition. Physical
// roots and interpretation counters are reconstructed from the accepted cut.
type CatalogIndexMutationPayload struct {
	Collection               string
	Incarnation              uint64
	BeforeMetadata, Metadata []byte
}

func EncodeCatalogIndexMutationPayload(p CatalogIndexMutationPayload) ([]byte, error) {
	if p.Collection == "" || p.Incarnation == 0 || len(p.BeforeMetadata) == 0 || len(p.Metadata) == 0 {
		return nil, ErrCorrupt
	}
	n := uint64(22) + uint64(len(p.Collection)) + uint64(len(p.BeforeMetadata)) + uint64(len(p.Metadata))
	if n > uint64(^uint32(0)) || n > uint64(int(^uint(0)>>1)) {
		return nil, ErrRecordTooLarge
	}
	b := make([]byte, int(n))
	binary.LittleEndian.PutUint16(b, 1)
	binary.LittleEndian.PutUint64(b[2:], p.Incarnation)
	binary.LittleEndian.PutUint32(b[10:], uint32(len(p.Collection)))
	binary.LittleEndian.PutUint32(b[14:], uint32(len(p.BeforeMetadata)))
	binary.LittleEndian.PutUint32(b[18:], uint32(len(p.Metadata)))
	i := 22
	i += copy(b[i:], p.Collection)
	i += copy(b[i:], p.BeforeMetadata)
	copy(b[i:], p.Metadata)
	return b, nil
}

func validateCatalogIndexMutationPayload(b []byte) error {
	if len(b) < 22 || binary.LittleEndian.Uint16(b) != 1 || binary.LittleEndian.Uint64(b[2:]) == 0 {
		return ErrCorrupt
	}
	a, c, d := uint64(binary.LittleEndian.Uint32(b[10:])), uint64(binary.LittleEndian.Uint32(b[14:])), uint64(binary.LittleEndian.Uint32(b[18:]))
	if a == 0 || c == 0 || d == 0 || 22+a+c+d != uint64(len(b)) {
		return ErrCorrupt
	}
	return nil
}

func DecodeCatalogIndexMutationPayload(b []byte) (CatalogIndexMutationPayload, error) {
	if err := validateCatalogIndexMutationPayload(b); err != nil {
		return CatalogIndexMutationPayload{}, err
	}
	a, c := int(binary.LittleEndian.Uint32(b[10:])), int(binary.LittleEndian.Uint32(b[14:]))
	return CatalogIndexMutationPayload{Collection: string(b[22 : 22+a]), Incarnation: binary.LittleEndian.Uint64(b[2:]), BeforeMetadata: append([]byte(nil), b[22+a:22+a+c]...), Metadata: append([]byte(nil), b[22+a+c:]...)}, nil
}
