package commitlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"strings"
)

const collectionBSONSetDeltaVersion uint16 = 3
const commandWALBSONSetDeltaFlag uint64 = 1 << 15
const collectionBSONSetDeltaRowHeader = 19 + 2*sha256.Size

// CollectionBSONSetDelta stores one same-size, generic BSON binary replacement.
// Value includes the BSON binary length and subtype. Both digests cover the
// entire ordered raw BSON document, including its typed ID.
type CollectionBSONSetDelta struct {
	ID, Field, Value []byte
	Before, After    [sha256.Size]byte
	Incarnation      uint64
}

// UsesBSONSetWALDelta identifies the envelope dialect even when only a
// terminal header is available. Complete decodes also verify its payload.
func (env CommandEnvelope) UsesBSONSetWALDelta() bool {
	return env.FeatureFlags&commandWALBSONSetDeltaFlag != 0
}

func isCollectionBSONSetDelta(kind CommandKind, format PayloadFormat, payload []byte) bool {
	return kind == CommandKindCollectionUpdateBatchByID && format == PayloadFormatCollectionUpdateBatchByIDV1 &&
		IsCollectionBSONSetDelta(payload)
}

// IsCollectionBSONSetDelta identifies the dialect within the kind-102 format-5
// family. Callers must first check the envelope identity.
func IsCollectionBSONSetDelta(payload []byte) bool {
	return len(payload) >= 2 && binary.LittleEndian.Uint16(payload[:2]) == collectionBSONSetDeltaVersion
}

func validCollectionBSONSetDelta(d CollectionBSONSetDelta) bool {
	return d.Incarnation != 0 && len(d.ID) > 0 && len(d.Field) > 0 && len(d.Field) <= 65535 &&
		d.Field[0] != '$' && !bytes.Equal(d.Field, []byte("_id")) && bytes.IndexAny(d.Field, ".\x00") == -1 &&
		len(d.Value) > 5 && d.Value[4] == 0 && uint64(binary.LittleEndian.Uint32(d.Value[:4])) == uint64(len(d.Value)-5) &&
		d.Before != d.After
}

// CollectionBSONSetDeltaPayloadSize is the exact owned bytes charged by the
// producer before encoding. A non-smaller payload can use the existing format.
func CollectionBSONSetDeltaPayloadSize(collection string, d CollectionBSONSetDelta) (int, error) {
	if collection == "" || strings.IndexByte(collection, 0) >= 0 || !validCollectionBSONSetDelta(d) {
		return 0, ErrCorrupt
	}
	n, err := collectionBatchPayloadHeaderLen(collection, 1)
	if err != nil {
		return 0, err
	}
	for _, size := range [...]int{collectionBSONSetDeltaRowHeader, len(d.ID), len(d.Field), len(d.Value)} {
		n, err = addCommandFrameEncodedSectionLen(n, size)
		if err != nil {
			return 0, err
		}
	}
	return n, nil
}

func EncodeCollectionBSONSetDelta(collection string, d CollectionBSONSetDelta) ([]byte, error) {
	n, err := CollectionBSONSetDeltaPayloadSize(collection, d)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, n)
	off := encodeCollectionBatchHeader(payload, collection, 1)
	binary.LittleEndian.PutUint16(payload[:2], collectionBSONSetDeltaVersion)
	binary.LittleEndian.PutUint32(payload[off:off+4], uint32(len(d.ID)))
	binary.LittleEndian.PutUint16(payload[off+4:off+6], uint16(len(d.Field)))
	payload[off+6] = 5 // BSON binary
	binary.LittleEndian.PutUint32(payload[off+7:off+11], uint32(len(d.Value)))
	binary.LittleEndian.PutUint64(payload[off+11:off+19], d.Incarnation)
	off += 19
	off += copy(payload[off:], d.Before[:])
	off += copy(payload[off:], d.After[:])
	off += copy(payload[off:], d.ID)
	off += copy(payload[off:], d.Field)
	copy(payload[off:], d.Value)
	return payload, nil
}

// DecodeCollectionBSONSetDelta borrows the payload slices for positioned replay.
func DecodeCollectionBSONSetDelta(payload []byte) ([]byte, CollectionBSONSetDelta, error) {
	var d CollectionBSONSetDelta
	name, count, off, err := parseCollectionBatchHeaderVersion(payload, collectionBSONSetDeltaVersion)
	if err != nil {
		return nil, d, err
	}
	if count != 1 || bytes.IndexByte(name, 0) >= 0 || len(payload)-off < collectionBSONSetDeltaRowHeader {
		return nil, d, ErrCorrupt
	}
	idLen := uint64(binary.LittleEndian.Uint32(payload[off : off+4]))
	fieldLen := uint64(binary.LittleEndian.Uint16(payload[off+4 : off+6]))
	valueLen := uint64(binary.LittleEndian.Uint32(payload[off+7 : off+11]))
	if payload[off+6] != 5 || idLen+fieldLen+valueLen != uint64(len(payload)-off-collectionBSONSetDeltaRowHeader) {
		return nil, d, ErrCorrupt
	}
	d.Incarnation = binary.LittleEndian.Uint64(payload[off+11 : off+19])
	off += 19
	off += copy(d.Before[:], payload[off:])
	off += copy(d.After[:], payload[off:])
	d.ID = payload[off : off+int(idLen)]
	off += int(idLen)
	d.Field = payload[off : off+int(fieldLen)]
	off += int(fieldLen)
	d.Value = payload[off:]
	if !validCollectionBSONSetDelta(d) {
		return nil, CollectionBSONSetDelta{}, ErrCorrupt
	}
	return name, d, nil
}
