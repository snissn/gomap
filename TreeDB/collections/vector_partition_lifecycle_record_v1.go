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

	vectorPartitionReadyPromotionMagicV1   = "VRP1"
	vectorPartitionReadyPromotionVersionV1 = 2
	// A promotion carries one router asset, the generation's computed
	// representative mapping, and fixed-size digests. The mapping remains
	// bounded below the lifecycle/store caps and avoids retaining a second full
	// membership manifest.
	vectorPartitionReadyPromotionMaxBytesV1 = 16 << 20
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

// vectorPartitionLifecycleRecordV1 is one immutable chain member. BUILD
// payloads are canonical VPM bytes; READY payloads are compact, digest-bound
// promotions; delete preparation/progress payloads are canonical VPR1 reclaim
// records. Other operations have an empty payload.
type vectorPartitionLifecycleRecordV1 struct {
	Collection, IndexName string
	Sequence              uint64
	PreviousDigest        [sha256.Size]byte
	Operation             vectorPartitionLifecycleOperationV1
	Generation            uint64
	Payload               []byte
	Digest                [sha256.Size]byte
}

// vectorPartitionReadyPromotionV1 contains only the fields that may change
// when a building generation becomes ready. The before/after VPM digests bind
// the delta to one exact canonical manifest without retaining a second copy.
type vectorPartitionReadyPromotionV1 struct {
	Generation       uint64
	BuildingDigest   [sha256.Size]byte
	RouterGeneration uint64
	RouterAsset      VectorPartitionAssetV1
	Representatives  []VectorPartitionMembershipV1
	ReadySetDigest   string
	ReadyDigest      [sha256.Size]byte
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
	GenerationFloor       uint64
	GenerationHighWater   uint64
	ActivationHighWater   uint64
	ActiveGeneration      uint64
	RetiredGeneration     uint64
	LastSequence          uint64
	LastDigest            [sha256.Size]byte
}

func vectorPartitionLifecycleZeroDigestV1(d [sha256.Size]byte) bool { return d == [sha256.Size]byte{} }

func vectorPartitionLifecycleOperationValidV1(op vectorPartitionLifecycleOperationV1) bool {
	return op >= vectorPartitionLifecycleBuildV1 && op <= vectorPartitionLifecycleDeleteCompleteV1
}

func vectorPartitionReadyPromotionShapeV1(p vectorPartitionReadyPromotionV1) error {
	limits := DefaultVectorPartitionManifestLimits()
	if p.Generation == 0 || p.RouterGeneration != p.Generation ||
		vectorPartitionLifecycleZeroDigestV1(p.BuildingDigest) ||
		vectorPartitionLifecycleZeroDigestV1(p.ReadyDigest) ||
		!isSHA256VPM(p.ReadySetDigest) ||
		len(p.ReadySetDigest) > limits.MaxStringBytes ||
		p.RouterAsset.PartitionID != 0 {
		return fmt.Errorf("%w: ready promotion identity", ErrVectorPartitionManifestInvalid)
	}
	if err := validateAssetVPM(p.RouterAsset, limits); err != nil {
		return fmt.Errorf("%w: ready promotion router asset", ErrVectorPartitionManifestInvalid)
	}
	if len(p.Representatives) > limits.MaxMemberships {
		return fmt.Errorf("%w: ready promotion representative cap", ErrVectorPartitionManifestInvalid)
	}
	for i, representative := range p.Representatives {
		if i > 0 {
			previous := p.Representatives[i-1]
			if representative.VectorOrdinal < previous.VectorOrdinal ||
				(representative.VectorOrdinal == previous.VectorOrdinal && representative.PartitionID <= previous.PartitionID) {
				return fmt.Errorf("%w: ready promotion representatives", ErrVectorPartitionManifestInvalid)
			}
		}
	}
	return nil
}

func encodeVectorPartitionReadyPromotionCanonicalV1(p vectorPartitionReadyPromotionV1) ([]byte, error) {
	if err := vectorPartitionReadyPromotionShapeV1(p); err != nil {
		return nil, err
	}
	limits := DefaultVectorPartitionManifestLimits()
	a := p.RouterAsset
	for _, s := range []string{p.ReadySetDigest, a.ID, a.Checksum, string(a.Ref.Kind), a.Ref.Namespace} {
		if len(s) > limits.MaxStringBytes {
			return nil, fmt.Errorf("%w: ready promotion string cap", ErrVectorPartitionManifestInvalid)
		}
	}
	b := bytes.NewBuffer(make([]byte, 0, 512))
	b.WriteString(vectorPartitionReadyPromotionMagicV1)
	putU32VPM(b, vectorPartitionReadyPromotionVersionV1)
	putU64VPM(b, p.Generation)
	b.Write(p.BuildingDigest[:])
	putU64VPM(b, p.RouterGeneration)
	b.Write(p.ReadyDigest[:])
	putStringVPM(b, p.ReadySetDigest)
	putMembershipsVPM(b, p.Representatives)
	putAssetsVPM(b, []VectorPartitionAssetV1{p.RouterAsset})
	if b.Len()+sha256.Size > vectorPartitionReadyPromotionMaxBytesV1 {
		return nil, fmt.Errorf("%w: ready promotion bytes cap", ErrVectorPartitionManifestInvalid)
	}
	sum := sha256.Sum256(b.Bytes())
	b.Write(sum[:])
	return b.Bytes(), nil
}

func decodeVectorPartitionReadyPromotionCanonicalV1(raw []byte) (vectorPartitionReadyPromotionV1, error) {
	var zero vectorPartitionReadyPromotionV1
	const fixed = 4 + 4 + 8 + sha256.Size + 8 + sha256.Size + 4 + 4 + 4 + sha256.Size
	if len(raw) < fixed || len(raw) > vectorPartitionReadyPromotionMaxBytesV1 {
		return zero, fmt.Errorf("%w: ready promotion bytes", ErrVectorPartitionManifestInvalid)
	}
	content := raw[:len(raw)-sha256.Size]
	sum := sha256.Sum256(content)
	if !bytes.Equal(sum[:], raw[len(raw)-sha256.Size:]) {
		return zero, fmt.Errorf("%w: ready promotion checksum", ErrVectorPartitionManifestInvalid)
	}
	limits := DefaultVectorPartitionManifestLimits()
	limits.MaxBytes = vectorPartitionReadyPromotionMaxBytesV1
	r := vpmReader{b: content, l: limits}
	if len(content) < 4 || string(content[:4]) != vectorPartitionReadyPromotionMagicV1 {
		return zero, fmt.Errorf("%w: ready promotion magic", ErrVectorPartitionManifestInvalid)
	}
	r.off = 4
	if r.u32() != vectorPartitionReadyPromotionVersionV1 {
		return zero, fmt.Errorf("%w: ready promotion version", ErrVectorPartitionManifestInvalid)
	}
	p := vectorPartitionReadyPromotionV1{Generation: r.u64()}
	if r.err != nil || r.off+sha256.Size > len(content) {
		return zero, fmt.Errorf("%w: ready promotion truncated", ErrVectorPartitionManifestInvalid)
	}
	copy(p.BuildingDigest[:], content[r.off:r.off+sha256.Size])
	r.off += sha256.Size
	p.RouterGeneration = r.u64()
	if r.err != nil || r.off+sha256.Size > len(content) {
		return zero, fmt.Errorf("%w: ready promotion truncated", ErrVectorPartitionManifestInvalid)
	}
	copy(p.ReadyDigest[:], content[r.off:r.off+sha256.Size])
	r.off += sha256.Size
	p.ReadySetDigest = r.str()
	p.Representatives = r.memberships()
	assets := r.assets()
	if r.err != nil || r.off != len(content) || len(assets) != 1 {
		return zero, fmt.Errorf("%w: ready promotion truncated, trailing, or asset count", ErrVectorPartitionManifestInvalid)
	}
	p.RouterAsset = assets[0]
	if err := vectorPartitionReadyPromotionShapeV1(p); err != nil {
		return zero, err
	}
	canonical, err := encodeVectorPartitionReadyPromotionCanonicalV1(p)
	if err != nil || !bytes.Equal(canonical, raw) {
		return zero, fmt.Errorf("%w: noncanonical ready promotion", ErrVectorPartitionManifestInvalid)
	}
	return p, nil
}

func makeVectorPartitionReadyPromotionPayloadV1(building, ready VectorPartitionManifestV1) ([]byte, error) {
	buildingRaw, err := EncodeVectorPartitionManifestV1(building)
	if err != nil || building.State != "building" {
		return nil, fmt.Errorf("%w: ready promotion building manifest", ErrVectorPartitionManifestInvalid)
	}
	readyRaw, err := EncodeVectorPartitionManifestV1(ready)
	if err != nil || ready.State != "ready" || !vectorPartitionBuildingPromotionIdentityV1(building, ready) {
		return nil, fmt.Errorf("%w: ready promotion ready manifest", ErrVectorPartitionManifestInvalid)
	}
	return encodeVectorPartitionReadyPromotionCanonicalV1(vectorPartitionReadyPromotionV1{
		Generation:       building.Generation,
		BuildingDigest:   sha256.Sum256(buildingRaw),
		RouterGeneration: ready.RouterGeneration,
		RouterAsset:      ready.RouterAsset,
		Representatives:  append([]VectorPartitionMembershipV1(nil), ready.Representatives...),
		ReadySetDigest:   ready.ReadySetDigest,
		ReadyDigest:      sha256.Sum256(readyRaw),
	})
}

func applyVectorPartitionReadyPromotionV1(building VectorPartitionManifestV1, payload []byte) (VectorPartitionManifestV1, error) {
	var zero VectorPartitionManifestV1
	p, err := decodeVectorPartitionReadyPromotionCanonicalV1(payload)
	if err != nil || building.State != "building" || building.Generation != p.Generation {
		return zero, fmt.Errorf("%w: ready promotion base identity", ErrVectorPartitionManifestInvalid)
	}
	buildingRaw, err := EncodeVectorPartitionManifestV1(building)
	if err != nil || sha256.Sum256(buildingRaw) != p.BuildingDigest {
		return zero, fmt.Errorf("%w: ready promotion base digest", ErrVectorPartitionManifestInvalid)
	}
	// Decode the canonical base so Canonicalize cannot mutate slices retained by
	// the caller while reconstructing the promoted state.
	ready, err := DecodeVectorPartitionManifestV1(buildingRaw, DefaultVectorPartitionManifestLimits())
	if err != nil {
		return zero, err
	}
	ready.State = "ready"
	ready.RouterGeneration = p.RouterGeneration
	ready.RouterAsset = p.RouterAsset
	ready.Representatives = append([]VectorPartitionMembershipV1(nil), p.Representatives...)
	ready.ReadySetDigest = p.ReadySetDigest
	ready.Canonicalize()
	if ready.ReadySetDigest != p.ReadySetDigest {
		return zero, fmt.Errorf("%w: ready promotion ready-set digest", ErrVectorPartitionManifestInvalid)
	}
	if err := ready.Validate(DefaultVectorPartitionManifestLimits()); err != nil ||
		!vectorPartitionBuildingPromotionIdentityV1(building, ready) {
		return zero, fmt.Errorf("%w: ready promotion reconstruction", ErrVectorPartitionManifestInvalid)
	}
	readyRaw, err := EncodeVectorPartitionManifestV1(ready)
	if err != nil || sha256.Sum256(readyRaw) != p.ReadyDigest {
		return zero, fmt.Errorf("%w: ready promotion result digest", ErrVectorPartitionManifestInvalid)
	}
	return ready, nil
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
	case vectorPartitionLifecycleBuildV1:
		m, err := DecodeVectorPartitionManifestV1(r.Payload, limits)
		if err != nil || m.Collection != r.Collection || m.IndexName != r.IndexName || m.Generation != r.Generation {
			return fmt.Errorf("%w: lifecycle manifest payload", ErrVectorPartitionManifestInvalid)
		}
		if m.State != "building" {
			return fmt.Errorf("%w: lifecycle manifest state", ErrVectorPartitionManifestInvalid)
		}
		canonical, err := EncodeVectorPartitionManifestV1(m)
		if err != nil || !bytes.Equal(canonical, r.Payload) {
			return fmt.Errorf("%w: noncanonical lifecycle manifest", ErrVectorPartitionManifestInvalid)
		}
	case vectorPartitionLifecycleReadyV1:
		promotion, err := decodeVectorPartitionReadyPromotionCanonicalV1(r.Payload)
		if err != nil || promotion.Generation != r.Generation {
			return fmt.Errorf("%w: lifecycle ready promotion payload", ErrVectorPartitionManifestInvalid)
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
		_, present := state.Generations[r.Generation]
		if present ||
			(state.GenerationHighWater != 0 &&
				(r.Generation <= state.GenerationHighWater ||
					r.Generation-state.GenerationHighWater != 1)) ||
			len(state.Generations) >= 2 {
			return fmt.Errorf("%w: build transition", ErrVectorPartitionManifestInvalid)
		}
		m, err := manifest()
		if err != nil {
			return err
		}
		state.Generations[r.Generation] = vectorPartitionLifecycleGenerationStateV1{Manifest: &m}
		if state.GenerationFloor == 0 {
			state.GenerationFloor = r.Generation
		}
		state.GenerationHighWater = r.Generation
	case vectorPartitionLifecycleReadyV1:
		generation, present := state.Generations[r.Generation]
		if !present || generation.Manifest == nil || generation.Manifest.State != "building" || generation.Deleting {
			return fmt.Errorf("%w: ready transition", ErrVectorPartitionManifestInvalid)
		}
		m, err := applyVectorPartitionReadyPromotionV1(*generation.Manifest, r.Payload)
		if err != nil {
			return fmt.Errorf("%w: ready promotion", ErrVectorPartitionManifestInvalid)
		}
		generation.Manifest = &m
		state.Generations[r.Generation] = generation
	case vectorPartitionLifecycleLocalActivateV1:
		generation, present := state.Generations[r.Generation]
		if !present ||
			generation.Manifest == nil ||
			generation.Manifest.State != "ready" ||
			generation.Deleting ||
			r.Generation <= state.ActivationHighWater {
			return fmt.Errorf("%w: activate transition", ErrVectorPartitionManifestInvalid)
		}
		if state.ActiveGeneration != 0 {
			state.RetiredGeneration = state.ActiveGeneration
		} else {
			state.RetiredGeneration = 0
		}
		state.ActiveGeneration = r.Generation
		state.ActivationHighWater = r.Generation
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
