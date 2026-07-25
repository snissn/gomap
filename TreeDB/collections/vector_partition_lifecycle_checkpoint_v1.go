package collections

// This file defines the self-contained checkpoint used to compact the
// immutable local M1 lifecycle history.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
)

const (
	vectorPartitionLifecycleCheckpointMagicV1                = "VCP1"
	vectorPartitionLifecycleCheckpointVersionV1       uint32 = 1
	vectorPartitionLifecycleCheckpointMaxBytesV1             = 30 << 20
	vectorPartitionLifecycleCheckpointTailMaxBytesV1         = 4 << 20
	vectorPartitionLifecycleCheckpointMaxLiveV1              = 2
	vectorPartitionLifecycleCheckpointHeaderBytesV1          = 4 + 4 + 4
	vectorPartitionLifecycleCheckpointChecksumBytesV1        = sha256.Size
	vectorPartitionLifecycleCheckpointDeletingV1             = 1
)

type vectorPartitionLifecycleCheckpointV1 struct {
	Epoch uint64
	State vectorPartitionLifecycleStateV1
}

type vectorPartitionLifecycleCheckpointGenerationEncodingV1 struct {
	generation uint64
	deleting   bool
	manifest   []byte
	reclaim    []byte
}

func cloneVectorPartitionManifestForCheckpointV1(m VectorPartitionManifestV1) VectorPartitionManifestV1 {
	cloned, err := cloneVectorPartitionManifestForCheckpointWithContextV1(context.Background(), m)
	if err != nil {
		panic(err)
	}
	return cloned
}

func cloneVectorPartitionSliceWithContextV1[T any](ctx context.Context, source []T) ([]T, error) {
	if source == nil {
		return nil, ctx.Err()
	}
	destination := make([]T, len(source))
	for start := 0; start < len(source); start += 4096 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := start + 4096
		if end > len(source) {
			end = len(source)
		}
		copy(destination[start:end], source[start:end])
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return destination, nil
}

func cloneVectorPartitionManifestForCheckpointWithContextV1(ctx context.Context, m VectorPartitionManifestV1) (VectorPartitionManifestV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	var err error
	if m.Placements, err = cloneVectorPartitionSliceWithContextV1(ctx, m.Placements); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	if m.Memberships, err = cloneVectorPartitionSliceWithContextV1(ctx, m.Memberships); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	if m.OverlapMemberships, err = cloneVectorPartitionSliceWithContextV1(ctx, m.OverlapMemberships); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	if m.Representatives, err = cloneVectorPartitionSliceWithContextV1(ctx, m.Representatives); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	if m.Assets, err = cloneVectorPartitionSliceWithContextV1(ctx, m.Assets); err != nil {
		return VectorPartitionManifestV1{}, err
	}
	return m, ctx.Err()
}

func canonicalVectorPartitionLifecycleCheckpointV1(input vectorPartitionLifecycleCheckpointV1) (vectorPartitionLifecycleCheckpointV1, []vectorPartitionLifecycleCheckpointGenerationEncodingV1, error) {
	return canonicalVectorPartitionLifecycleCheckpointWithContextV1(context.Background(), input)
}

func canonicalVectorPartitionLifecycleCheckpointWithContextV1(ctx context.Context, input vectorPartitionLifecycleCheckpointV1) (vectorPartitionLifecycleCheckpointV1, []vectorPartitionLifecycleCheckpointGenerationEncodingV1, error) {
	var zero vectorPartitionLifecycleCheckpointV1
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return zero, nil, err
	}
	state := input.State
	limits := DefaultVectorPartitionManifestLimits()
	if input.Epoch == 0 ||
		state.Collection == "" || state.IndexName == "" ||
		len(state.Collection) > limits.MaxStringBytes || len(state.IndexName) > limits.MaxStringBytes ||
		state.LastSequence == 0 || vectorPartitionLifecycleZeroDigestV1(state.LastDigest) ||
		state.GenerationFloor == 0 ||
		state.GenerationHighWater < state.GenerationFloor ||
		(state.ActivationHighWater != 0 && state.ActivationHighWater < state.GenerationFloor) ||
		state.ActivationHighWater > state.GenerationHighWater ||
		len(state.Generations) > vectorPartitionLifecycleCheckpointMaxLiveV1 ||
		(state.ActiveGeneration != 0 && state.ActiveGeneration != state.ActivationHighWater) ||
		(state.ActiveGeneration == 0 &&
			state.RetiredGeneration != 0 &&
			state.RetiredGeneration != state.ActivationHighWater) ||
		state.RetiredGeneration > state.ActivationHighWater ||
		(state.ActiveGeneration != 0 &&
			state.RetiredGeneration != 0 &&
			state.ActiveGeneration <= state.RetiredGeneration) {
		return zero, nil, fmt.Errorf("%w: lifecycle checkpoint identity or bounds", ErrVectorPartitionManifestInvalid)
	}

	generations := make([]uint64, 0, len(state.Generations))
	for generation := range state.Generations {
		generations = append(generations, generation)
	}
	sort.Slice(generations, func(i, j int) bool { return generations[i] < generations[j] })

	canonicalState := vectorPartitionLifecycleStateV1{
		Collection:          state.Collection,
		IndexName:           state.IndexName,
		Generations:         make(map[uint64]vectorPartitionLifecycleGenerationStateV1, len(generations)),
		GenerationFloor:     state.GenerationFloor,
		GenerationHighWater: state.GenerationHighWater,
		ActivationHighWater: state.ActivationHighWater,
		ActiveGeneration:    state.ActiveGeneration,
		RetiredGeneration:   state.RetiredGeneration,
		LastSequence:        state.LastSequence,
		LastDigest:          state.LastDigest,
	}
	encoded := make([]vectorPartitionLifecycleCheckpointGenerationEncodingV1, 0, len(generations))
	for _, generation := range generations {
		if err := ctx.Err(); err != nil {
			return zero, nil, err
		}
		entry := state.Generations[generation]
		if generation < state.GenerationFloor || generation > state.GenerationHighWater || entry.Manifest == nil {
			return zero, nil, fmt.Errorf("%w: lifecycle checkpoint generation", ErrVectorPartitionManifestInvalid)
		}
		if err := preflightVectorPartitionManifestWithContextV1(ctx, *entry.Manifest, limits); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return zero, nil, ctxErr
			}
			return zero, nil, fmt.Errorf("%w: lifecycle checkpoint manifest", ErrVectorPartitionManifestInvalid)
		}
		manifestInput, err := cloneVectorPartitionManifestForCheckpointWithContextV1(ctx, *entry.Manifest)
		if err != nil {
			return zero, nil, err
		}
		manifestRaw, err := encodeVectorPartitionManifestWithContextV1(ctx, manifestInput)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return zero, nil, ctxErr
			}
			return zero, nil, fmt.Errorf("%w: lifecycle checkpoint manifest", ErrVectorPartitionManifestInvalid)
		}
		if err := ctx.Err(); err != nil {
			return zero, nil, err
		}
		manifest, err := DecodeVectorPartitionManifestWithContextV1(ctx, manifestRaw, limits)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return zero, nil, ctxErr
			}
			return zero, nil, fmt.Errorf("%w: lifecycle checkpoint manifest identity", ErrVectorPartitionManifestInvalid)
		}
		if manifest.Collection != state.Collection ||
			manifest.IndexName != state.IndexName ||
			manifest.Generation != generation {
			return zero, nil, fmt.Errorf("%w: lifecycle checkpoint manifest identity", ErrVectorPartitionManifestInvalid)
		}

		var reclaimRaw []byte
		var reclaim *vectorPartitionReclaimStateV1
		switch {
		case entry.Deleting && entry.Reclaim == nil:
			return zero, nil, fmt.Errorf("%w: lifecycle checkpoint deleting state", ErrVectorPartitionManifestInvalid)
		case !entry.Deleting && entry.Reclaim != nil:
			return zero, nil, fmt.Errorf("%w: lifecycle checkpoint unexpected reclaim state", ErrVectorPartitionManifestInvalid)
		case entry.Deleting:
			cloned := entry.Reclaim.clone()
			reclaimRaw, err = encodeVectorPartitionReclaimRecordV1(cloned)
			if err != nil {
				return zero, nil, fmt.Errorf("%w: lifecycle checkpoint reclaim", ErrVectorPartitionManifestInvalid)
			}
			decoded, decodeErr := decodeVectorPartitionReclaimRecordV1(reclaimRaw)
			if decodeErr != nil ||
				decoded.Collection != state.Collection ||
				decoded.IndexName != state.IndexName ||
				decoded.Generation != generation ||
				!vectorPartitionLifecycleRefsEqualV1(decoded.OriginalRefs, vectorPartitionReclaimRefsFromManifestV1(manifest)) {
				return zero, nil, fmt.Errorf("%w: lifecycle checkpoint reclaim identity", ErrVectorPartitionManifestInvalid)
			}
			reclaim = &decoded
		}

		canonicalState.Generations[generation] = vectorPartitionLifecycleGenerationStateV1{
			Manifest: &manifest,
			Deleting: entry.Deleting,
			Reclaim:  reclaim,
		}
		encoded = append(encoded, vectorPartitionLifecycleCheckpointGenerationEncodingV1{
			generation: generation,
			deleting:   entry.Deleting,
			manifest:   manifestRaw,
			reclaim:    reclaimRaw,
		})
	}

	validatePointer := func(generation uint64, label string) error {
		if generation == 0 {
			return nil
		}
		entry, present := canonicalState.Generations[generation]
		if !present || entry.Manifest == nil || entry.Manifest.State != "ready" || entry.Deleting || entry.Reclaim != nil {
			return fmt.Errorf("%w: lifecycle checkpoint %s generation", ErrVectorPartitionManifestInvalid, label)
		}
		return nil
	}
	if err := validatePointer(canonicalState.ActiveGeneration, "active"); err != nil {
		return zero, nil, err
	}
	if err := validatePointer(canonicalState.RetiredGeneration, "retired"); err != nil {
		return zero, nil, err
	}
	if watermark, present := canonicalState.Generations[canonicalState.ActivationHighWater]; present &&
		!watermark.Deleting &&
		canonicalState.ActiveGeneration != canonicalState.ActivationHighWater &&
		canonicalState.RetiredGeneration != canonicalState.ActivationHighWater {
		return zero, nil, fmt.Errorf("%w: lifecycle checkpoint live activation high water has no authority pointer", ErrVectorPartitionManifestInvalid)
	}
	return vectorPartitionLifecycleCheckpointV1{Epoch: input.Epoch, State: canonicalState}, encoded, nil
}

func encodeVectorPartitionLifecycleCheckpointCanonicalV1(input vectorPartitionLifecycleCheckpointV1) ([]byte, error) {
	checkpoint, generations, err := canonicalVectorPartitionLifecycleCheckpointV1(input)
	if err != nil {
		return nil, err
	}
	return encodePreparedVectorPartitionLifecycleCheckpointV1(checkpoint, generations)
}

func encodePreparedVectorPartitionLifecycleCheckpointV1(checkpoint vectorPartitionLifecycleCheckpointV1, generations []vectorPartitionLifecycleCheckpointGenerationEncodingV1) ([]byte, error) {
	state := checkpoint.State
	payloadBytes := uint64(4 + len(state.Collection) + 4 + len(state.IndexName) + 8 + 8 + sha256.Size + 8 + 8 + 8 + 8 + 8 + 4)
	add := func(n uint64) error {
		maxPayload := uint64(vectorPartitionLifecycleCheckpointMaxBytesV1 - vectorPartitionLifecycleCheckpointHeaderBytesV1 - vectorPartitionLifecycleCheckpointChecksumBytesV1)
		if n > maxPayload || payloadBytes > maxPayload-n {
			return fmt.Errorf("%w: lifecycle checkpoint bytes cap", ErrVectorPartitionManifestInvalid)
		}
		payloadBytes += n
		return nil
	}
	for _, generation := range generations {
		if err := add(8 + 1 + 4 + uint64(len(generation.manifest)) + 4 + uint64(len(generation.reclaim))); err != nil {
			return nil, err
		}
	}
	if payloadBytes > math.MaxUint32 {
		return nil, fmt.Errorf("%w: lifecycle checkpoint payload length", ErrVectorPartitionManifestInvalid)
	}

	payload := bytes.NewBuffer(make([]byte, 0, int(payloadBytes)))
	putStringVPM(payload, state.Collection)
	putStringVPM(payload, state.IndexName)
	putU64VPM(payload, checkpoint.Epoch)
	putU64VPM(payload, state.LastSequence)
	payload.Write(state.LastDigest[:])
	putU64VPM(payload, state.GenerationFloor)
	putU64VPM(payload, state.GenerationHighWater)
	putU64VPM(payload, state.ActivationHighWater)
	putU64VPM(payload, state.ActiveGeneration)
	putU64VPM(payload, state.RetiredGeneration)
	putU32VPM(payload, uint32(len(generations)))
	for _, generation := range generations {
		putU64VPM(payload, generation.generation)
		if generation.deleting {
			payload.WriteByte(vectorPartitionLifecycleCheckpointDeletingV1)
		} else {
			payload.WriteByte(0)
		}
		putU32VPM(payload, uint32(len(generation.manifest)))
		payload.Write(generation.manifest)
		putU32VPM(payload, uint32(len(generation.reclaim)))
		payload.Write(generation.reclaim)
	}
	if uint64(payload.Len()) != payloadBytes {
		return nil, fmt.Errorf("%w: lifecycle checkpoint encoded length", ErrVectorPartitionManifestInvalid)
	}

	out := bytes.NewBuffer(make([]byte, 0, vectorPartitionLifecycleCheckpointHeaderBytesV1+payload.Len()+vectorPartitionLifecycleCheckpointChecksumBytesV1))
	out.WriteString(vectorPartitionLifecycleCheckpointMagicV1)
	putU32VPM(out, vectorPartitionLifecycleCheckpointVersionV1)
	putU32VPM(out, uint32(payload.Len()))
	out.Write(payload.Bytes())
	sum := sha256.Sum256(out.Bytes())
	out.Write(sum[:])
	return out.Bytes(), nil
}

func decodeVectorPartitionLifecycleCheckpointCanonicalV1(raw []byte, collection, index string, epoch uint64) (vectorPartitionLifecycleCheckpointV1, error) {
	return decodeVectorPartitionLifecycleCheckpointCanonicalWithContextV1(context.Background(), raw, collection, index, epoch)
}

func decodeVectorPartitionLifecycleCheckpointCanonicalWithContextV1(ctx context.Context, raw []byte, collection, index string, epoch uint64) (vectorPartitionLifecycleCheckpointV1, error) {
	var zero vectorPartitionLifecycleCheckpointV1
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if collection == "" || index == "" || epoch == 0 ||
		len(raw) < vectorPartitionLifecycleCheckpointHeaderBytesV1+vectorPartitionLifecycleCheckpointChecksumBytesV1 ||
		len(raw) > vectorPartitionLifecycleCheckpointMaxBytesV1 ||
		string(raw[:4]) != vectorPartitionLifecycleCheckpointMagicV1 {
		return zero, fmt.Errorf("%w: lifecycle checkpoint header", ErrVectorPartitionManifestInvalid)
	}
	if binary.BigEndian.Uint32(raw[4:8]) != vectorPartitionLifecycleCheckpointVersionV1 {
		return zero, fmt.Errorf("%w: lifecycle checkpoint version", ErrVectorPartitionManifestInvalid)
	}
	payloadBytes := uint64(binary.BigEndian.Uint32(raw[8:12]))
	if payloadBytes == 0 ||
		payloadBytes > uint64(vectorPartitionLifecycleCheckpointMaxBytesV1-vectorPartitionLifecycleCheckpointHeaderBytesV1-vectorPartitionLifecycleCheckpointChecksumBytesV1) ||
		uint64(len(raw)) != uint64(vectorPartitionLifecycleCheckpointHeaderBytesV1+vectorPartitionLifecycleCheckpointChecksumBytesV1)+payloadBytes {
		return zero, fmt.Errorf("%w: lifecycle checkpoint length", ErrVectorPartitionManifestInvalid)
	}
	contentBytes := vectorPartitionLifecycleCheckpointHeaderBytesV1 + int(payloadBytes)
	sum := sha256.Sum256(raw[:contentBytes])
	if !bytes.Equal(sum[:], raw[contentBytes:]) {
		return zero, fmt.Errorf("%w: lifecycle checkpoint checksum", ErrVectorPartitionManifestInvalid)
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}

	limits := DefaultVectorPartitionManifestLimits()
	r := vpmReader{b: raw[vectorPartitionLifecycleCheckpointHeaderBytesV1:contentBytes], l: VectorPartitionManifestLimits{MaxStringBytes: limits.MaxStringBytes}, ctx: ctx}
	state := vectorPartitionLifecycleStateV1{
		Collection:  r.str(),
		IndexName:   r.str(),
		Generations: make(map[uint64]vectorPartitionLifecycleGenerationStateV1),
	}
	embeddedEpoch := r.u64()
	state.LastSequence = r.u64()
	if r.err != nil || r.off+sha256.Size > len(r.b) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return zero, ctxErr
		}
		return zero, fmt.Errorf("%w: lifecycle checkpoint truncated digest", ErrVectorPartitionManifestInvalid)
	}
	copy(state.LastDigest[:], r.b[r.off:r.off+sha256.Size])
	r.off += sha256.Size
	state.GenerationFloor = r.u64()
	state.GenerationHighWater = r.u64()
	state.ActivationHighWater = r.u64()
	state.ActiveGeneration = r.u64()
	state.RetiredGeneration = r.u64()
	generationCount := r.count(vectorPartitionLifecycleCheckpointMaxLiveV1)
	if r.err != nil ||
		state.Collection != collection ||
		state.IndexName != index ||
		embeddedEpoch != epoch {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return zero, ctxErr
		}
		return zero, fmt.Errorf("%w: lifecycle checkpoint embedded identity", ErrVectorPartitionManifestInvalid)
	}

	readBlob := func(max int, label string) []byte {
		n := uint64(r.u32())
		if r.err != nil || n > uint64(max) || n > uint64(len(r.b)-r.off) {
			r.err = fmt.Errorf("%s length exceeds remaining bytes", label)
			return nil
		}
		out := r.b[r.off : r.off+int(n)]
		r.off += int(n)
		return out
	}
	var previousGeneration uint64
	for i := 0; i < generationCount && r.err == nil; i++ {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		generation := r.u64()
		var flags byte
		if r.err == nil && r.off < len(r.b) {
			flags = r.b[r.off]
			r.off++
		} else {
			r.err = errors.New("truncated generation flags")
		}
		if generation == 0 || (i > 0 && generation <= previousGeneration) || flags&^byte(vectorPartitionLifecycleCheckpointDeletingV1) != 0 {
			r.err = errors.New("noncanonical generation or flags")
			break
		}
		manifestRaw := readBlob(limits.MaxBytes, "manifest")
		reclaimRaw := readBlob(vectorPartitionReclaimMaxBytesV1, "reclaim")
		if r.err != nil || len(manifestRaw) == 0 {
			break
		}
		manifest, err := DecodeVectorPartitionManifestWithContextV1(ctx, manifestRaw, limits)
		if err != nil {
			r.err = err
			break
		}
		entry := vectorPartitionLifecycleGenerationStateV1{
			Manifest: &manifest,
			Deleting: flags&vectorPartitionLifecycleCheckpointDeletingV1 != 0,
		}
		if len(reclaimRaw) != 0 {
			reclaim, err := decodeVectorPartitionReclaimRecordV1(reclaimRaw)
			if err != nil {
				r.err = err
				break
			}
			entry.Reclaim = &reclaim
		}
		state.Generations[generation] = entry
		previousGeneration = generation
	}
	if r.err != nil || r.off != len(r.b) || len(state.Generations) != generationCount {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return zero, ctxErr
		}
		return zero, fmt.Errorf("%w: lifecycle checkpoint truncated, over-cap, or trailing: %v", ErrVectorPartitionManifestInvalid, r.err)
	}

	checkpoint, encoded, err := canonicalVectorPartitionLifecycleCheckpointWithContextV1(ctx, vectorPartitionLifecycleCheckpointV1{Epoch: embeddedEpoch, State: state})
	if err != nil {
		return zero, err
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	canonicalRaw, err := encodePreparedVectorPartitionLifecycleCheckpointV1(checkpoint, encoded)
	if err != nil || !bytes.Equal(canonicalRaw, raw) {
		return zero, fmt.Errorf("%w: noncanonical lifecycle checkpoint", ErrVectorPartitionManifestInvalid)
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	return checkpoint, nil
}

func reduceVectorPartitionLifecycleCheckpointTailV1(checkpoint vectorPartitionLifecycleCheckpointV1, records []vectorPartitionLifecycleRecordV1) (vectorPartitionLifecycleStateV1, error) {
	if len(records) > vectorPartitionLifecycleMaxRecordsV1 {
		return vectorPartitionLifecycleStateV1{}, fmt.Errorf("%w: lifecycle checkpoint tail length", ErrVectorPartitionManifestInvalid)
	}
	canonical, _, err := canonicalVectorPartitionLifecycleCheckpointV1(checkpoint)
	if err != nil {
		return vectorPartitionLifecycleStateV1{}, err
	}
	state := canonical.State
	var tailBytes uint64
	for _, record := range records {
		unsigned, err := encodeVectorPartitionLifecycleRecordV1(record, false)
		if err != nil {
			return vectorPartitionLifecycleStateV1{}, err
		}
		if record.Digest != sha256.Sum256(unsigned) {
			return vectorPartitionLifecycleStateV1{}, fmt.Errorf("%w: lifecycle checkpoint tail checksum", ErrVectorPartitionManifestInvalid)
		}
		raw, err := encodeVectorPartitionLifecycleRecordCanonicalV1(record)
		if err != nil {
			return vectorPartitionLifecycleStateV1{}, err
		}
		if uint64(len(raw)) > uint64(vectorPartitionLifecycleCheckpointTailMaxBytesV1)-tailBytes {
			return vectorPartitionLifecycleStateV1{}, fmt.Errorf("%w: lifecycle checkpoint tail bytes cap", ErrVectorPartitionManifestInvalid)
		}
		tailBytes += uint64(len(raw))
		decoded, err := decodeVectorPartitionLifecycleRecordCanonicalV1(raw)
		if err != nil {
			return vectorPartitionLifecycleStateV1{}, err
		}
		if decoded.Operation == vectorPartitionLifecycleBuildV1 ||
			state.LastSequence == math.MaxUint64 ||
			decoded.Sequence != state.LastSequence+1 ||
			decoded.PreviousDigest != state.LastDigest ||
			decoded.Collection != state.Collection ||
			decoded.IndexName != state.IndexName {
			return vectorPartitionLifecycleStateV1{}, fmt.Errorf("%w: lifecycle checkpoint tail sequence, predecessor, identity, or operation", ErrVectorPartitionManifestInvalid)
		}
		if err := reduceVectorPartitionLifecycleRecordV1(&state, decoded); err != nil {
			return vectorPartitionLifecycleStateV1{}, err
		}
		state.LastSequence = decoded.Sequence
		state.LastDigest = decoded.Digest
	}
	return state, nil
}
