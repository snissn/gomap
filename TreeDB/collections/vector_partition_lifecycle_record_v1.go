package collections

// This file defines the local, append-only M1 lifecycle record. It is pure
// format/validation state: storage publication and reclaim integration remain
// deliberately outside this milestone.

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

const (
	vectorPartitionLifecycleMagicV1       = "VLC1"
	vectorPartitionLifecycleVersionV1     = 1
	vectorPartitionLifecycleHeaderBytesV1 = 4 + 1 + 1 + 8 + 8 + 2 + 2 + 4 + sha256.Size
	vectorPartitionLifecycleMaxRecordsV1  = 4096
	// The wrapper stays within the existing 64 MiB VPM store cap. Its payload
	// cap is deliberately header+digest bytes tighter, so a max-sized VPR1
	// record is rejected rather than allocating beyond the store contract.
	vectorPartitionLifecycleMaxBytesV1 = 64 << 20
)

// vectorPartitionLifecycleOperationV1 is intentionally local-M1 scoped; it
// does not imply a replicated M7 cutover.
type vectorPartitionLifecycleOperationV1 uint8

const (
	vectorPartitionLifecycleBuildV1 vectorPartitionLifecycleOperationV1 = iota + 1
	vectorPartitionLifecycleReadyV1
	vectorPartitionLifecycleLocalActivateV1
	vectorPartitionLifecycleDeactivateV1
	vectorPartitionLifecycleDeletePrepareV1
	vectorPartitionLifecycleReclaimProgressV1
	vectorPartitionLifecycleDeleteCompleteV1
)

// vectorPartitionLifecycleRecordV1 is one immutable chain member. BUILD and
// READY payloads are canonical VPM bytes; delete preparation/progress payloads
// are canonical VPR1 reclaim records. Other operations have an empty payload.
type vectorPartitionLifecycleRecordV1 struct {
	Collection, IndexName string
	Sequence              uint64
	PreviousDigest        [sha256.Size]byte
	Operation             vectorPartitionLifecycleOperationV1
	Generation            uint64
	Payload               []byte
	Digest                [sha256.Size]byte
}

// vectorPartitionLifecycleGenerationStateV1 is the current authority for one
// generation. A nil Manifest means no live generation entry.
type vectorPartitionLifecycleGenerationStateV1 struct {
	Manifest *VectorPartitionManifestV1
	Deleting bool
	Reclaim  *vectorPartitionReclaimStateV1
}

// vectorPartitionLifecycleStateV1 is the pure reduction required by current
// local lifecycle semantics. Generations are independent: dominated retained
// history cannot revive a completed generation or erase unrelated authority.
type vectorPartitionLifecycleStateV1 struct {
	Collection, IndexName string
	Generations           map[uint64]vectorPartitionLifecycleGenerationStateV1
	CompletedGenerations  map[uint64]bool
	ActiveGeneration      uint64
	RetiredGeneration     uint64
	LastSequence          uint64
	LastDigest            [sha256.Size]byte
}

func vectorPartitionLifecycleZeroDigestV1(d [sha256.Size]byte) bool { return d == [sha256.Size]byte{} }

func vectorPartitionLifecycleOperationValidV1(op vectorPartitionLifecycleOperationV1) bool {
	return op >= vectorPartitionLifecycleBuildV1 && op <= vectorPartitionLifecycleDeleteCompleteV1
}

func vectorPartitionLifecyclePayloadV1(r vectorPartitionLifecycleRecordV1) error {
	if r.Generation == 0 || !vectorPartitionLifecycleOperationValidV1(r.Operation) {
		return fmt.Errorf("%w: lifecycle operation or generation", ErrVectorPartitionManifestInvalid)
	}
	limits := DefaultVectorPartitionManifestLimits()
	if r.Collection == "" || r.IndexName == "" || len(r.Collection) > limits.MaxStringBytes || len(r.IndexName) > limits.MaxStringBytes || len(r.Payload) > vectorPartitionLifecycleMaxBytesV1-vectorPartitionLifecycleHeaderBytesV1-sha256.Size {
		return fmt.Errorf("%w: lifecycle record bounds", ErrVectorPartitionManifestInvalid)
	}
	switch r.Operation {
	case vectorPartitionLifecycleBuildV1, vectorPartitionLifecycleReadyV1:
		m, err := DecodeVectorPartitionManifestV1(r.Payload, limits)
		if err != nil || m.Collection != r.Collection || m.IndexName != r.IndexName || m.Generation != r.Generation {
			return fmt.Errorf("%w: lifecycle manifest payload", ErrVectorPartitionManifestInvalid)
		}
		if (r.Operation == vectorPartitionLifecycleBuildV1 && m.State != "building") || (r.Operation == vectorPartitionLifecycleReadyV1 && m.State != "ready") {
			return fmt.Errorf("%w: lifecycle manifest state", ErrVectorPartitionManifestInvalid)
		}
		canonical, err := EncodeVectorPartitionManifestV1(m)
		if err != nil || !bytes.Equal(canonical, r.Payload) {
			return fmt.Errorf("%w: noncanonical lifecycle manifest", ErrVectorPartitionManifestInvalid)
		}
	case vectorPartitionLifecycleDeletePrepareV1, vectorPartitionLifecycleReclaimProgressV1:
		state, err := decodeVectorPartitionReclaimRecordV1(r.Payload)
		if err != nil || state.Collection != r.Collection || state.IndexName != r.IndexName || state.Generation != r.Generation {
			return fmt.Errorf("%w: lifecycle reclaim payload", ErrVectorPartitionManifestInvalid)
		}
	default:
		if len(r.Payload) != 0 {
			return fmt.Errorf("%w: lifecycle operation payload", ErrVectorPartitionManifestInvalid)
		}
	}
	return nil
}

func encodeVectorPartitionLifecycleRecordV1(r vectorPartitionLifecycleRecordV1, includeDigest bool) ([]byte, error) {
	if err := vectorPartitionLifecyclePayloadV1(r); err != nil {
		return nil, err
	}
	if r.Sequence == 0 || len(r.Collection) > 0xffff || len(r.IndexName) > 0xffff {
		return nil, fmt.Errorf("%w: lifecycle record identity", ErrVectorPartitionManifestInvalid)
	}
	n := vectorPartitionLifecycleHeaderBytesV1 + len(r.Collection) + len(r.IndexName) + len(r.Payload)
	if includeDigest {
		n += sha256.Size
	}
	if n > vectorPartitionLifecycleMaxBytesV1 {
		return nil, fmt.Errorf("%w: lifecycle record size", ErrVectorPartitionManifestInvalid)
	}
	out := make([]byte, n)
	copy(out, vectorPartitionLifecycleMagicV1)
	out[4], out[5] = vectorPartitionLifecycleVersionV1, byte(r.Operation)
	binary.BigEndian.PutUint64(out[6:14], r.Sequence)
	binary.BigEndian.PutUint64(out[14:22], r.Generation)
	binary.BigEndian.PutUint16(out[22:24], uint16(len(r.Collection)))
	binary.BigEndian.PutUint16(out[24:26], uint16(len(r.IndexName)))
	binary.BigEndian.PutUint32(out[26:30], uint32(len(r.Payload)))
	copy(out[30:30+sha256.Size], r.PreviousDigest[:])
	off := vectorPartitionLifecycleHeaderBytesV1
	off += copy(out[off:], r.Collection)
	off += copy(out[off:], r.IndexName)
	off += copy(out[off:], r.Payload)
	if includeDigest {
		copy(out[off:], r.Digest[:])
	}
	return out, nil
}

// encodeVectorPartitionLifecycleRecordCanonicalV1 emits one canonical checksummed VLC1 record.
func encodeVectorPartitionLifecycleRecordCanonicalV1(r vectorPartitionLifecycleRecordV1) ([]byte, error) {
	canonical, err := encodeVectorPartitionLifecycleRecordV1(r, false)
	if err != nil {
		return nil, err
	}
	r.Digest = sha256.Sum256(canonical)
	return encodeVectorPartitionLifecycleRecordV1(r, true)
}

// decodeVectorPartitionLifecycleRecordCanonicalV1 rejects malformed, oversized, trailing,
// or noncanonical records before exposing their payload.
func decodeVectorPartitionLifecycleRecordCanonicalV1(raw []byte) (vectorPartitionLifecycleRecordV1, error) {
	var zero vectorPartitionLifecycleRecordV1
	if len(raw) < vectorPartitionLifecycleHeaderBytesV1+sha256.Size || len(raw) > vectorPartitionLifecycleMaxBytesV1 || string(raw[:4]) != vectorPartitionLifecycleMagicV1 || raw[4] != vectorPartitionLifecycleVersionV1 {
		return zero, fmt.Errorf("%w: lifecycle record header", ErrVectorPartitionManifestInvalid)
	}
	collectionN, indexN, payloadN := int(binary.BigEndian.Uint16(raw[22:24])), int(binary.BigEndian.Uint16(raw[24:26])), int(binary.BigEndian.Uint32(raw[26:30]))
	if collectionN == 0 || indexN == 0 || collectionN > DefaultVectorPartitionManifestLimits().MaxStringBytes || indexN > DefaultVectorPartitionManifestLimits().MaxStringBytes || payloadN > vectorPartitionLifecycleMaxBytesV1-vectorPartitionLifecycleHeaderBytesV1-sha256.Size {
		return zero, fmt.Errorf("%w: lifecycle record lengths", ErrVectorPartitionManifestInvalid)
	}
	want := vectorPartitionLifecycleHeaderBytesV1 + collectionN + indexN + payloadN + sha256.Size
	if want != len(raw) {
		return zero, fmt.Errorf("%w: lifecycle record trailing or truncated", ErrVectorPartitionManifestInvalid)
	}
	digest := sha256.Sum256(raw[:len(raw)-sha256.Size])
	if !bytes.Equal(digest[:], raw[len(raw)-sha256.Size:]) {
		return zero, fmt.Errorf("%w: lifecycle record checksum", ErrVectorPartitionManifestInvalid)
	}
	r := vectorPartitionLifecycleRecordV1{Sequence: binary.BigEndian.Uint64(raw[6:14]), Generation: binary.BigEndian.Uint64(raw[14:22]), Operation: vectorPartitionLifecycleOperationV1(raw[5])}
	copy(r.PreviousDigest[:], raw[30:30+sha256.Size])
	off := vectorPartitionLifecycleHeaderBytesV1
	r.Collection = string(raw[off : off+collectionN])
	off += collectionN
	r.IndexName = string(raw[off : off+indexN])
	off += indexN
	r.Payload = append([]byte(nil), raw[off:off+payloadN]...)
	off += payloadN
	copy(r.Digest[:], raw[off:])
	canonical, err := encodeVectorPartitionLifecycleRecordCanonicalV1(r)
	if err != nil || !bytes.Equal(canonical, raw) {
		return zero, fmt.Errorf("%w: noncanonical lifecycle record", ErrVectorPartitionManifestInvalid)
	}
	return r, nil
}

// reduceVectorPartitionLifecycleChainV1 validates one complete identity chain
// then derives only its current local-M1 authority state.
func reduceVectorPartitionLifecycleChainV1(records []vectorPartitionLifecycleRecordV1) (vectorPartitionLifecycleStateV1, error) {
	var state vectorPartitionLifecycleStateV1
	if len(records) == 0 || len(records) > vectorPartitionLifecycleMaxRecordsV1 {
		return state, fmt.Errorf("%w: lifecycle chain length", ErrVectorPartitionManifestInvalid)
	}
	state.Generations = make(map[uint64]vectorPartitionLifecycleGenerationStateV1, len(records))
	state.CompletedGenerations = make(map[uint64]bool)
	for i, r := range records {
		unsigned, err := encodeVectorPartitionLifecycleRecordV1(r, false)
		if err != nil {
			return state, err
		}
		if want := sha256.Sum256(unsigned); r.Digest != want {
			return state, fmt.Errorf("%w: lifecycle chain checksum", ErrVectorPartitionManifestInvalid)
		}
		encoded, err := encodeVectorPartitionLifecycleRecordCanonicalV1(r)
		if err != nil {
			return state, err
		}
		decoded, err := decodeVectorPartitionLifecycleRecordCanonicalV1(encoded)
		if err != nil {
			return state, err
		}
		if decoded.Sequence != uint64(i+1) || (i == 0 && !vectorPartitionLifecycleZeroDigestV1(decoded.PreviousDigest)) || (i > 0 && decoded.PreviousDigest != state.LastDigest) {
			return state, fmt.Errorf("%w: lifecycle sequence, fork, or predecessor", ErrVectorPartitionManifestInvalid)
		}
		if i == 0 {
			state.Collection, state.IndexName = decoded.Collection, decoded.IndexName
		} else if decoded.Collection != state.Collection || decoded.IndexName != state.IndexName {
			return state, fmt.Errorf("%w: lifecycle identity", ErrVectorPartitionManifestInvalid)
		}
		if err := reduceVectorPartitionLifecycleRecordV1(&state, decoded); err != nil {
			return vectorPartitionLifecycleStateV1{}, err
		}
		state.LastSequence, state.LastDigest = decoded.Sequence, decoded.Digest
	}
	return state, nil
}

func reduceVectorPartitionLifecycleRecordV1(state *vectorPartitionLifecycleStateV1, r vectorPartitionLifecycleRecordV1) error {
	manifest := func() (VectorPartitionManifestV1, error) {
		return DecodeVectorPartitionManifestV1(r.Payload, DefaultVectorPartitionManifestLimits())
	}
	switch r.Operation {
	case vectorPartitionLifecycleBuildV1:
		if _, present := state.Generations[r.Generation]; present || state.CompletedGenerations[r.Generation] {
			return fmt.Errorf("%w: build transition", ErrVectorPartitionManifestInvalid)
		}
		m, err := manifest()
		if err != nil {
			return err
		}
		state.Generations[r.Generation] = vectorPartitionLifecycleGenerationStateV1{Manifest: &m}
	case vectorPartitionLifecycleReadyV1:
		generation, present := state.Generations[r.Generation]
		if !present || generation.Manifest == nil || generation.Manifest.State != "building" || generation.Deleting {
			return fmt.Errorf("%w: ready transition", ErrVectorPartitionManifestInvalid)
		}
		m, err := manifest()
		if err != nil || !vectorPartitionBuildingPromotionIdentityV1(*generation.Manifest, m) {
			return fmt.Errorf("%w: ready promotion", ErrVectorPartitionManifestInvalid)
		}
		generation.Manifest = &m
		state.Generations[r.Generation] = generation
	case vectorPartitionLifecycleLocalActivateV1:
		generation, present := state.Generations[r.Generation]
		if !present || generation.Manifest == nil || generation.Manifest.State != "ready" || generation.Deleting || state.ActiveGeneration == r.Generation {
			return fmt.Errorf("%w: activate transition", ErrVectorPartitionManifestInvalid)
		}
		if state.ActiveGeneration != 0 {
			state.RetiredGeneration = state.ActiveGeneration
		} else {
			state.RetiredGeneration = 0
		}
		state.ActiveGeneration = r.Generation
	case vectorPartitionLifecycleDeactivateV1:
		if state.ActiveGeneration != r.Generation {
			return fmt.Errorf("%w: deactivate transition", ErrVectorPartitionManifestInvalid)
		}
		state.ActiveGeneration = 0
		state.RetiredGeneration = r.Generation
	case vectorPartitionLifecycleDeletePrepareV1:
		generation, present := state.Generations[r.Generation]
		if !present || generation.Manifest == nil || state.ActiveGeneration == r.Generation || generation.Deleting {
			return fmt.Errorf("%w: delete prepare transition", ErrVectorPartitionManifestInvalid)
		}
		reclaim, err := decodeVectorPartitionReclaimRecordV1(r.Payload)
		if err != nil || len(reclaim.SupersededRefs) != 0 || !vectorPartitionLifecycleRefsEqualV1(reclaim.OriginalRefs, vectorPartitionReclaimRefsFromManifestV1(*generation.Manifest)) {
			return fmt.Errorf("%w: delete prepare reclaim identity", ErrVectorPartitionManifestInvalid)
		}
		generation.Deleting, generation.Reclaim = true, &reclaim
		state.Generations[r.Generation] = generation
		if state.RetiredGeneration == r.Generation {
			state.RetiredGeneration = 0
		}
	case vectorPartitionLifecycleReclaimProgressV1:
		generation, present := state.Generations[r.Generation]
		if !present || !generation.Deleting || generation.Reclaim == nil {
			return fmt.Errorf("%w: reclaim transition", ErrVectorPartitionManifestInvalid)
		}
		reclaim, err := decodeVectorPartitionReclaimRecordV1(r.Payload)
		if err != nil || !vectorPartitionLifecycleRefsEqualV1(reclaim.OriginalRefs, generation.Reclaim.OriginalRefs) || !vectorPartitionLifecycleRefsSupersetV1(reclaim.SupersededRefs, generation.Reclaim.SupersededRefs) {
			return fmt.Errorf("%w: reclaim progress", ErrVectorPartitionManifestInvalid)
		}
		generation.Reclaim = &reclaim
		state.Generations[r.Generation] = generation
	case vectorPartitionLifecycleDeleteCompleteV1:
		generation, present := state.Generations[r.Generation]
		if !present || !generation.Deleting {
			return fmt.Errorf("%w: delete complete transition", ErrVectorPartitionManifestInvalid)
		}
		// Integration may append DELETE_COMPLETE only after physical reclaim debt
		// is discharged. This pure reducer records logical completion only.
		delete(state.Generations, r.Generation)
		state.CompletedGenerations[r.Generation] = true
		if state.RetiredGeneration == r.Generation {
			state.RetiredGeneration = 0
		}
	default:
		return fmt.Errorf("%w: lifecycle operation", ErrVectorPartitionManifestInvalid)
	}
	return nil
}

func vectorPartitionLifecycleRefsEqualV1(a, b []ColumnAssetRef) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if compareColumnAssetRefs(a[i], b[i]) != 0 {
			return false
		}
	}
	return true
}

func vectorPartitionLifecycleRefsSupersetV1(superset, subset []ColumnAssetRef) bool {
	i, j := 0, 0
	for i < len(superset) && j < len(subset) {
		c := compareColumnAssetRefs(superset[i], subset[j])
		if c < 0 {
			i++
			continue
		}
		if c > 0 {
			return false
		}
		i++
		j++
	}
	return j == len(subset)
}
