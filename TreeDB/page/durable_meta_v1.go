package page

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
)

const DurableMetaV1BodySize = 104

var (
	durableMetaV1Magic = [8]byte{'T', 'D', 'M', 'E', 'T', 'V', '1', 0}

	ErrDurableMetaFormat       = errors.New("durable meta V1 format invalid")
	ErrDurableMetaProjection   = errors.New("durable meta V1 projection digest mismatch")
	ErrDurableMetaLegacyFormat = errors.New("legacy TreeDB meta requires rebuild")
)

// DurableMetaV1 is the complete selectable contents of one meta-page body.
// Roots and allocator state live in the separately checksummed durable-root
// record; the meta binds that record by both page identity and digest.
type DurableMetaV1 struct {
	CommitSeq            uint64
	DurableSeq           uint64
	RootRecordPageID     uint64
	MetaProjectionDigest [32]byte
	RootRecordDigest     [32]byte
}

func NewDurableMetaV1(commitSeq, durableSeq, rootRecordPageID uint64, rootRecordDigest [32]byte) (DurableMetaV1, error) {
	meta := DurableMetaV1{
		CommitSeq: commitSeq, DurableSeq: durableSeq,
		RootRecordPageID: rootRecordPageID, RootRecordDigest: rootRecordDigest,
	}
	if err := meta.validate(); err != nil {
		return DurableMetaV1{}, err
	}
	meta.MetaProjectionDigest = meta.projectionDigest()
	return meta, nil
}

func (meta DurableMetaV1) validate() error {
	if meta.CommitSeq == 0 || meta.DurableSeq > meta.CommitSeq || meta.RootRecordPageID < 2 || meta.RootRecordDigest == [32]byte{} {
		return ErrDurableMetaFormat
	}
	return nil
}

func (meta DurableMetaV1) projectionDigest() [32]byte {
	return DurableMetaProjectionDigestV1(meta.CommitSeq, meta.DurableSeq, meta.RootRecordPageID)
}

// DurableMetaProjectionDigestV1 binds the selectable scalar projection while
// deliberately excluding the root-record digest. The record stores this
// projection, its digest binds the record, and the meta then stores that
// record digest without introducing a hash cycle.
func DurableMetaProjectionDigestV1(commitSeq, durableSeq, rootRecordPageID uint64) [32]byte {
	canonical := make([]byte, 40)
	copy(canonical[0:8], durableMetaV1Magic[:])
	binary.LittleEndian.PutUint16(canonical[8:10], 1)
	binary.LittleEndian.PutUint16(canonical[10:12], 1)
	binary.LittleEndian.PutUint16(canonical[12:14], DurableMetaV1BodySize)
	binary.LittleEndian.PutUint64(canonical[16:24], commitSeq)
	binary.LittleEndian.PutUint64(canonical[24:32], durableSeq)
	binary.LittleEndian.PutUint64(canonical[32:40], rootRecordPageID)
	return sha256.Sum256(canonical)
}

func (meta DurableMetaV1) Encode(dst []byte) error {
	if len(dst) < DurableMetaV1BodySize {
		return fmt.Errorf("%w: body size %d", ErrDurableMetaFormat, len(dst))
	}
	if err := meta.validate(); err != nil {
		return err
	}
	wantProjection := meta.projectionDigest()
	if meta.MetaProjectionDigest != wantProjection {
		return ErrDurableMetaProjection
	}
	clear(dst[:DurableMetaV1BodySize])
	copy(dst[0:8], durableMetaV1Magic[:])
	binary.LittleEndian.PutUint16(dst[8:10], 1)
	binary.LittleEndian.PutUint16(dst[10:12], 1)
	binary.LittleEndian.PutUint16(dst[12:14], DurableMetaV1BodySize)
	binary.LittleEndian.PutUint64(dst[16:24], meta.CommitSeq)
	binary.LittleEndian.PutUint64(dst[24:32], meta.DurableSeq)
	binary.LittleEndian.PutUint64(dst[32:40], meta.RootRecordPageID)
	copy(dst[40:72], meta.MetaProjectionDigest[:])
	copy(dst[72:104], meta.RootRecordDigest[:])
	return nil
}

func DecodeDurableMetaV1(src []byte) (DurableMetaV1, error) {
	if len(src) >= 8 && !bytes.Equal(src[0:8], durableMetaV1Magic[:]) {
		if !allZero(src) {
			return DurableMetaV1{}, ErrDurableMetaLegacyFormat
		}
		return DurableMetaV1{}, ErrDurableMetaFormat
	}
	if len(src) < DurableMetaV1BodySize {
		return DurableMetaV1{}, fmt.Errorf("%w: body size %d", ErrDurableMetaFormat, len(src))
	}
	if binary.LittleEndian.Uint16(src[8:10]) != 1 ||
		binary.LittleEndian.Uint16(src[10:12]) != 1 ||
		binary.LittleEndian.Uint16(src[12:14]) != DurableMetaV1BodySize ||
		binary.LittleEndian.Uint16(src[14:16]) != 0 {
		return DurableMetaV1{}, ErrDurableMetaFormat
	}
	meta := DurableMetaV1{
		CommitSeq:        binary.LittleEndian.Uint64(src[16:24]),
		DurableSeq:       binary.LittleEndian.Uint64(src[24:32]),
		RootRecordPageID: binary.LittleEndian.Uint64(src[32:40]),
	}
	copy(meta.MetaProjectionDigest[:], src[40:72])
	copy(meta.RootRecordDigest[:], src[72:104])
	if err := meta.validate(); err != nil {
		return DurableMetaV1{}, err
	}
	if meta.MetaProjectionDigest != meta.projectionDigest() {
		return DurableMetaV1{}, ErrDurableMetaProjection
	}
	return meta, nil
}

func allZero(src []byte) bool {
	for _, value := range src {
		if value != 0 {
			return false
		}
	}
	return true
}
