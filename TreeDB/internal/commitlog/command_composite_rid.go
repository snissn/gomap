package commitlog

import (
	"crypto/sha256"
	"encoding/binary"
	"slices"
)

// CompositeMutationExternalRIDCount validates the complete payload and counts
// external SetRID references across all raw members, including duplicates.
// It allocates no scratch. The caller admits that many uint64 slots through its
// original publication owner before constructing the aggregate fence.
func CompositeMutationExternalRIDCount(payload []byte) (int, error) {
	count := 0
	err := ScanCompositeMutationPayload(payload, func(member CompositeMutationMember) error {
		if member.Kind != CommandKindRawKVBatch {
			return nil
		}
		return ScanRawKVBatchRIDs(member.Payload, func(uint64) error {
			if count == int(^uint(0)>>1) {
				return ErrRecordTooLarge
			}
			count++
			return nil
		})
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

// CompositeMutationExternalRefFenceV1 constructs the canonical union of every
// raw member's external SetRID references. Materialized RID operations carry
// their own value bytes, preserving the existing raw fence semantics.
//
// The original caller owns and preadmits scratch, which must not alias payload.
// Complete payload validation and scratch-size checks precede scratch mutation.
// Only scratch[:count] is changed; no command identity, frame, resource lookup or
// append occurs here. The ordinary frame validator still controls activation.
func CompositeMutationExternalRefFenceV1(payload []byte, scratch []uint64) (ExternalRefFenceV1, error) {
	count, err := CompositeMutationExternalRIDCount(payload)
	if err != nil {
		return ExternalRefFenceV1{}, err
	}
	if len(scratch) < count {
		return ExternalRefFenceV1{}, ErrRecordTooLarge
	}
	if count == 0 {
		return ExternalRefFenceV1{}, nil
	}
	used := scratch[:count]
	next := 0
	// The immutable original payload was wholly validated above. Reuse the
	// existing scanners rather than introducing another raw operation decoder.
	err = ScanCompositeMutationPayload(payload, func(member CompositeMutationMember) error {
		if member.Kind != CommandKindRawKVBatch {
			return nil
		}
		return ScanRawKVBatchRIDs(member.Payload, func(rid uint64) error {
			used[next] = rid
			next++
			return nil
		})
	})
	if err != nil {
		return ExternalRefFenceV1{}, err
	}
	slices.Sort(used)
	hash := sha256.New()
	var encoded [8]byte
	var fence ExternalRefFenceV1
	for i, rid := range used {
		if i != 0 && rid == used[i-1] {
			continue
		}
		binary.LittleEndian.PutUint64(encoded[:], rid)
		_, _ = hash.Write(encoded[:])
		fence.Count++
	}
	hash.Sum(fence.Digest[:0])
	return fence, nil
}
