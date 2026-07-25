package collections

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"testing"
	"time"
)

type cancelAfterErrContextV1 struct {
	context.Context
	calls       int
	cancelAfter int
}

func (c *cancelAfterErrContextV1) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *cancelAfterErrContextV1) Done() <-chan struct{}       { return nil }
func (c *cancelAfterErrContextV1) Value(key any) any           { return c.Context.Value(key) }
func (c *cancelAfterErrContextV1) Err() error {
	c.calls++
	if c.calls >= c.cancelAfter {
		return context.Canceled
	}
	return nil
}

func lifecycleCheckpointV1(t *testing.T, chain []vectorPartitionLifecycleRecordV1, epoch uint64) vectorPartitionLifecycleCheckpointV1 {
	t.Helper()
	state, err := reduceVectorPartitionLifecycleChainV1(chain)
	if err != nil {
		t.Fatal(err)
	}
	return vectorPartitionLifecycleCheckpointV1{Epoch: epoch, State: state}
}

func lifecycleTwoBuildingCheckpointV1(t *testing.T) vectorPartitionLifecycleCheckpointV1 {
	t.Helper()
	build1Raw, build1 := lifecycleManifestPayloadV1(t, "building")
	build2 := cloneVectorPartitionManifestForCheckpointV1(build1)
	build2.Generation++
	build2.Canonicalize()
	build2Raw, err := EncodeVectorPartitionManifestV1(build2)
	if err != nil {
		t.Fatal(err)
	}
	first := lifecycleRecordV1(t, 1, [sha256.Size]byte{}, vectorPartitionLifecycleBuildV1, build1.Generation, build1Raw)
	second := lifecycleRecordV1(t, 2, first.Digest, vectorPartitionLifecycleBuildV1, build2.Generation, build2Raw)
	return lifecycleCheckpointV1(t, []vectorPartitionLifecycleRecordV1{first, second}, 11)
}

func rechecksumLifecycleCheckpointV1(raw []byte) {
	contentBytes := len(raw) - sha256.Size
	sum := sha256.Sum256(raw[:contentBytes])
	copy(raw[contentBytes:], sum[:])
}

func lifecycleCheckpointFirstGenerationOffsetsV1(t *testing.T, raw []byte) (manifestStart, reclaimStart int) {
	t.Helper()
	payloadBytes := int(binary.BigEndian.Uint32(raw[8:12]))
	r := vpmReader{
		b: raw[vectorPartitionLifecycleCheckpointHeaderBytesV1 : vectorPartitionLifecycleCheckpointHeaderBytesV1+payloadBytes],
		l: VectorPartitionManifestLimits{MaxStringBytes: DefaultVectorPartitionManifestLimits().MaxStringBytes},
	}
	_ = r.str()
	_ = r.str()
	_ = r.u64()
	_ = r.u64()
	r.off += sha256.Size
	_ = r.u64()
	_ = r.u64()
	_ = r.u64()
	_ = r.u64()
	_ = r.u64()
	if got := r.u32(); got == 0 {
		t.Fatal("checkpoint has no generation")
	}
	_ = r.u64()
	r.off++
	manifestBytes := int(r.u32())
	manifestStart = vectorPartitionLifecycleCheckpointHeaderBytesV1 + r.off
	r.off += manifestBytes
	reclaimBytes := int(r.u32())
	reclaimStart = vectorPartitionLifecycleCheckpointHeaderBytesV1 + r.off
	if r.err != nil || manifestBytes == 0 || manifestStart+manifestBytes > len(raw)-sha256.Size || reclaimStart+reclaimBytes > len(raw)-sha256.Size {
		t.Fatalf("invalid test checkpoint offsets: %v", r.err)
	}
	return manifestStart, reclaimStart
}

func lifecycleCheckpointGenerationCountOffsetV1(t *testing.T, raw []byte) int {
	t.Helper()
	payloadBytes := int(binary.BigEndian.Uint32(raw[8:12]))
	r := vpmReader{
		b: raw[vectorPartitionLifecycleCheckpointHeaderBytesV1 : vectorPartitionLifecycleCheckpointHeaderBytesV1+payloadBytes],
		l: VectorPartitionManifestLimits{MaxStringBytes: DefaultVectorPartitionManifestLimits().MaxStringBytes},
	}
	_ = r.str()
	_ = r.str()
	_ = r.u64()
	_ = r.u64()
	r.off += sha256.Size
	_ = r.u64()
	_ = r.u64()
	_ = r.u64()
	_ = r.u64()
	if r.err != nil || r.off+4 > len(r.b) {
		t.Fatalf("invalid test checkpoint count offset: %v", r.err)
	}
	return vectorPartitionLifecycleCheckpointHeaderBytesV1 + r.off
}

func TestVectorPartitionLifecycleCheckpointV1CanonicalRoundTripAndDeepCopy(t *testing.T) {
	chain := lifecycleLegalChainV1(t)
	checkpoint := lifecycleCheckpointV1(t, chain[:5], 7)
	manifest := checkpoint.State.Generations[chain[0].Generation].Manifest
	if len(manifest.Placements) < 2 {
		t.Fatal("test manifest needs two placements")
	}
	manifest.Placements[0], manifest.Placements[1] = manifest.Placements[1], manifest.Placements[0]
	beforeFirst := manifest.Placements[0]

	raw, err := encodeVectorPartitionLifecycleCheckpointCanonicalV1(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Placements[0] != beforeFirst {
		t.Fatal("checkpoint encoding mutated the caller's manifest slices")
	}
	got, err := decodeVectorPartitionLifecycleCheckpointCanonicalV1(raw, "docs", "embedding", 7)
	if err != nil {
		t.Fatal(err)
	}
	again, err := encodeVectorPartitionLifecycleCheckpointCanonicalV1(got)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, again) {
		t.Fatal("checkpoint encoding is not canonical")
	}
	generation := got.State.Generations[chain[0].Generation]
	if !generation.Deleting || generation.Reclaim == nil || generation.Manifest == nil ||
		got.State.GenerationFloor != chain[0].Generation ||
		got.State.GenerationHighWater != chain[0].Generation ||
		got.State.ActivationHighWater != chain[0].Generation ||
		got.State.LastSequence != 5 || got.State.LastDigest != chain[4].Digest {
		t.Fatalf("decoded checkpoint state=%+v generation=%+v", got.State, generation)
	}

	generation.Manifest.Placements[0].GroupID = "mutated"
	generation.Reclaim.OriginalRefs[0].Namespace = "mutated"
	original := checkpoint.State.Generations[chain[0].Generation]
	if original.Manifest.Placements[0].GroupID == "mutated" || original.Reclaim.OriginalRefs[0].Namespace == "mutated" {
		t.Fatal("decoded checkpoint aliases caller-owned state")
	}
}

func TestVectorPartitionLifecycleCheckpointPreservesManifestDecodeCancellationV1(t *testing.T) {
	checkpoint := lifecycleCheckpointV1(t, lifecycleLegalChainV1(t)[:5], 7)
	ctx := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: 4}
	if _, _, err := canonicalVectorPartitionLifecycleCheckpointWithContextV1(ctx, checkpoint); !errors.Is(err, context.Canceled) {
		t.Fatalf("canonical cancellation err=%v want context.Canceled", err)
	}

	raw, err := encodeVectorPartitionLifecycleCheckpointCanonicalV1(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for cancelAfter := 3; cancelAfter < 64; cancelAfter++ {
		ctx := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: cancelAfter}
		_, err := decodeVectorPartitionLifecycleCheckpointCanonicalWithContextV1(ctx, raw, "docs", "embedding", 7)
		if errors.Is(err, context.Canceled) && ctx.calls > 3 {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("checkpoint decode did not preserve an in-stream context cancellation")
	}
}

func TestCloneVectorPartitionManifestForCheckpointWithContextV1CancelsMidCopy(t *testing.T) {
	manifest := scaledVectorPartitionManifestV1(32 << 10)
	first := manifest.Memberships[0]
	last := manifest.Memberships[len(manifest.Memberships)-1]
	ctx := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: 6}
	if _, err := cloneVectorPartitionManifestForCheckpointWithContextV1(ctx, manifest); !errors.Is(err, context.Canceled) {
		t.Fatalf("clone cancellation err=%v want context.Canceled", err)
	}
	if ctx.calls < ctx.cancelAfter {
		t.Fatalf("context calls=%d want at least %d", ctx.calls, ctx.cancelAfter)
	}
	if manifest.Memberships[0] != first || manifest.Memberships[len(manifest.Memberships)-1] != last {
		t.Fatal("canceled checkpoint clone mutated caller-owned memberships")
	}
}

func TestVectorPartitionLifecycleCheckpointV1MapOrderIsDeterministic(t *testing.T) {
	first := lifecycleTwoBuildingCheckpointV1(t)
	second := first
	second.State.Generations = make(map[uint64]vectorPartitionLifecycleGenerationStateV1, len(first.State.Generations))
	generations := []uint64{first.State.GenerationHighWater, first.State.GenerationHighWater - 1}
	for _, generation := range generations {
		second.State.Generations[generation] = first.State.Generations[generation]
	}
	firstRaw, err := encodeVectorPartitionLifecycleCheckpointCanonicalV1(first)
	if err != nil {
		t.Fatal(err)
	}
	secondRaw, err := encodeVectorPartitionLifecycleCheckpointCanonicalV1(second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstRaw, secondRaw) {
		t.Fatal("map insertion order changed checkpoint bytes")
	}
	decoded, err := decodeVectorPartitionLifecycleCheckpointCanonicalV1(firstRaw, "docs", "embedding", first.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.State.Generations) != vectorPartitionLifecycleCheckpointMaxLiveV1 {
		t.Fatalf("decoded live generations=%d", len(decoded.State.Generations))
	}

	third := cloneVectorPartitionManifestForCheckpointV1(*first.State.Generations[first.State.GenerationHighWater].Manifest)
	third.Generation++
	third.Canonicalize()
	threeLive := first
	threeLive.State.GenerationHighWater = third.Generation
	threeLive.State.Generations = make(map[uint64]vectorPartitionLifecycleGenerationStateV1, len(first.State.Generations)+1)
	for generation, entry := range first.State.Generations {
		threeLive.State.Generations[generation] = entry
	}
	threeLive.State.Generations[third.Generation] = vectorPartitionLifecycleGenerationStateV1{Manifest: &third}
	if _, err := encodeVectorPartitionLifecycleCheckpointCanonicalV1(threeLive); err == nil {
		t.Fatal("accepted three live generations")
	}
}

func TestVectorPartitionLifecycleCheckpointV1ZeroLiveRoundTripPreservesHighWater(t *testing.T) {
	chain := lifecycleLegalChainV1(t)
	checkpoint := lifecycleCheckpointV1(t, chain, 12)
	if len(checkpoint.State.Generations) != 0 {
		t.Fatal("test checkpoint is not zero-live")
	}
	raw, err := encodeVectorPartitionLifecycleCheckpointCanonicalV1(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeVectorPartitionLifecycleCheckpointCanonicalV1(raw, "docs", "embedding", checkpoint.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.State.Generations) != 0 ||
		decoded.State.GenerationFloor != chain[0].Generation ||
		decoded.State.GenerationHighWater != chain[0].Generation ||
		decoded.State.ActivationHighWater != chain[0].Generation ||
		decoded.State.LastSequence != chain[len(chain)-1].Sequence ||
		decoded.State.LastDigest != chain[len(chain)-1].Digest {
		t.Fatalf("zero-live checkpoint state=%+v", decoded.State)
	}
}

func TestVectorPartitionLifecycleCheckpointV1RejectsMalformedOuterRecord(t *testing.T) {
	checkpoint := lifecycleCheckpointV1(t, lifecycleLegalChainV1(t)[:3], 9)
	raw, err := encodeVectorPartitionLifecycleCheckpointCanonicalV1(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	for name, candidate := range map[string][]byte{
		"checksum": func() []byte {
			x := append([]byte(nil), raw...)
			x[len(x)-1] ^= 1
			return x
		}(),
		"version": func() []byte {
			x := append([]byte(nil), raw...)
			binary.BigEndian.PutUint32(x[4:8], vectorPartitionLifecycleCheckpointVersionV1+1)
			rechecksumLifecycleCheckpointV1(x)
			return x
		}(),
		"trailing":  append(append([]byte(nil), raw...), 0),
		"truncated": append([]byte(nil), raw[:len(raw)-1]...),
		"oversize":  make([]byte, vectorPartitionLifecycleCheckpointMaxBytesV1+1),
		"count": func() []byte {
			x := append([]byte(nil), raw...)
			binary.BigEndian.PutUint32(x[lifecycleCheckpointGenerationCountOffsetV1(t, x):], vectorPartitionLifecycleCheckpointMaxLiveV1+1)
			rechecksumLifecycleCheckpointV1(x)
			return x
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeVectorPartitionLifecycleCheckpointCanonicalV1(candidate, "docs", "embedding", 9); err == nil {
				t.Fatal("accepted malformed checkpoint")
			}
		})
	}
	if _, err := decodeVectorPartitionLifecycleCheckpointCanonicalV1(raw, "other", "embedding", 9); err == nil {
		t.Fatal("accepted wrong collection")
	}
	if _, err := decodeVectorPartitionLifecycleCheckpointCanonicalV1(raw, "docs", "other", 9); err == nil {
		t.Fatal("accepted wrong index")
	}
	if _, err := decodeVectorPartitionLifecycleCheckpointCanonicalV1(raw, "docs", "embedding", 10); err == nil {
		t.Fatal("accepted wrong epoch")
	}
}

func TestVectorPartitionLifecycleCheckpointV1RejectsMalformedNestedRecords(t *testing.T) {
	chain := lifecycleLegalChainV1(t)
	for name, checkpoint := range map[string]vectorPartitionLifecycleCheckpointV1{
		"manifest": lifecycleCheckpointV1(t, chain[:3], 3),
		"reclaim":  lifecycleCheckpointV1(t, chain[:5], 4),
	} {
		t.Run(name, func(t *testing.T) {
			raw, err := encodeVectorPartitionLifecycleCheckpointCanonicalV1(checkpoint)
			if err != nil {
				t.Fatal(err)
			}
			manifestStart, reclaimStart := lifecycleCheckpointFirstGenerationOffsetsV1(t, raw)
			corruptAt := manifestStart
			if name == "reclaim" {
				corruptAt = reclaimStart
			}
			raw[corruptAt] ^= 1
			rechecksumLifecycleCheckpointV1(raw)
			if _, err := decodeVectorPartitionLifecycleCheckpointCanonicalV1(raw, "docs", "embedding", checkpoint.Epoch); err == nil {
				t.Fatal("accepted corrupt nested record")
			}
		})
	}
}

func TestVectorPartitionLifecycleCheckpointV1RejectsInvalidState(t *testing.T) {
	base := lifecycleCheckpointV1(t, lifecycleLegalChainV1(t)[:3], 5)
	generation := base.State.ActiveGeneration
	for name, mutate := range map[string]func(*vectorPartitionLifecycleCheckpointV1){
		"missing-generation-floor": func(c *vectorPartitionLifecycleCheckpointV1) {
			c.State.GenerationFloor = 0
		},
		"generation-floor-above-high-water": func(c *vectorPartitionLifecycleCheckpointV1) {
			c.State.GenerationFloor = c.State.GenerationHighWater + 1
		},
		"generation-above-high-water": func(c *vectorPartitionLifecycleCheckpointV1) {
			c.State.GenerationHighWater = generation - 1
		},
		"activation-above-generation-high-water": func(c *vectorPartitionLifecycleCheckpointV1) {
			c.State.ActivationHighWater = c.State.GenerationHighWater + 1
		},
		"active-does-not-match-activation-high-water": func(c *vectorPartitionLifecycleCheckpointV1) {
			c.State.ActivationHighWater = generation - 1
		},
		"live-activation-high-water-without-authority-pointer": func(c *vectorPartitionLifecycleCheckpointV1) {
			c.State.ActiveGeneration = 0
		},
		"missing-active": func(c *vectorPartitionLifecycleCheckpointV1) {
			c.State.ActiveGeneration = generation + 1
		},
		"active-equals-retired": func(c *vectorPartitionLifecycleCheckpointV1) {
			c.State.RetiredGeneration = c.State.ActiveGeneration
		},
		"unexpected-reclaim": func(c *vectorPartitionLifecycleCheckpointV1) {
			entry := c.State.Generations[generation]
			reclaim, err := newVectorPartitionReclaimStateV1(*entry.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			entry.Reclaim = &reclaim
			c.State.Generations[generation] = entry
		},
		"deleting-without-reclaim": func(c *vectorPartitionLifecycleCheckpointV1) {
			c.State.ActiveGeneration = 0
			entry := c.State.Generations[generation]
			entry.Deleting = true
			c.State.Generations[generation] = entry
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := base
			candidate.State.Generations = make(map[uint64]vectorPartitionLifecycleGenerationStateV1, len(base.State.Generations))
			for key, entry := range base.State.Generations {
				candidate.State.Generations[key] = entry
			}
			mutate(&candidate)
			if _, err := encodeVectorPartitionLifecycleCheckpointCanonicalV1(candidate); err == nil {
				t.Fatal("accepted invalid checkpoint state")
			}
		})
	}

	t.Run("retired-building", func(t *testing.T) {
		building := lifecycleCheckpointV1(t, lifecycleLegalChainV1(t)[:1], 6)
		building.State.RetiredGeneration = building.State.GenerationHighWater
		if _, err := encodeVectorPartitionLifecycleCheckpointCanonicalV1(building); err == nil {
			t.Fatal("accepted retired pointer to building generation")
		}
	})

	t.Run("active-below-retired", func(t *testing.T) {
		chain := lifecycleLegalChainV1(t)
		_, build := lifecycleManifestPayloadV1(t, "building")
		_, ready := lifecycleManifestPayloadV1(t, "ready")
		secondReady := cloneVectorPartitionManifestForCheckpointV1(ready)
		secondReady.Generation++
		secondReady.RouterGeneration++
		secondReady.RouterAsset.Ref.Generation = secondReady.Generation
		secondReady.Canonicalize()
		secondBuild := cloneVectorPartitionManifestForCheckpointV1(secondReady)
		secondBuild.State = "building"
		secondBuild.RouterGeneration = 0
		secondBuild.RouterAsset = VectorPartitionAssetV1{}
		secondBuild.ReadySetDigest = ""
		secondBuild.Canonicalize()
		secondBuildRaw, err := EncodeVectorPartitionManifestV1(secondBuild)
		if err != nil {
			t.Fatal(err)
		}
		secondReadyPayload := lifecycleReadyPromotionPayloadV1(t, secondBuild, secondReady)
		chain = chain[:3]
		chain = append(chain, lifecycleRecordV1(t, 4, chain[2].Digest, vectorPartitionLifecycleBuildV1, secondBuild.Generation, secondBuildRaw))
		chain = append(chain, lifecycleRecordV1(t, 5, chain[3].Digest, vectorPartitionLifecycleReadyV1, secondReady.Generation, secondReadyPayload))
		chain = append(chain, lifecycleRecordV1(t, 6, chain[4].Digest, vectorPartitionLifecycleLocalActivateV1, secondReady.Generation, nil))
		candidate := lifecycleCheckpointV1(t, chain, 7)
		candidate.State.ActiveGeneration = build.Generation
		candidate.State.RetiredGeneration = secondReady.Generation
		if _, err := encodeVectorPartitionLifecycleCheckpointCanonicalV1(candidate); err == nil {
			t.Fatal("accepted checkpoint whose active generation is below its retired generation")
		}
	})

	t.Run("active-deleting", func(t *testing.T) {
		candidate := base
		candidate.State.Generations = make(map[uint64]vectorPartitionLifecycleGenerationStateV1, len(base.State.Generations))
		for key, entry := range base.State.Generations {
			candidate.State.Generations[key] = entry
		}
		entry := candidate.State.Generations[generation]
		reclaim, err := newVectorPartitionReclaimStateV1(*entry.Manifest)
		if err != nil {
			t.Fatal(err)
		}
		entry.Deleting, entry.Reclaim = true, &reclaim
		candidate.State.Generations[generation] = entry
		if _, err := encodeVectorPartitionLifecycleCheckpointCanonicalV1(candidate); err == nil {
			t.Fatal("accepted active pointer to deleting generation")
		}
	})
}

func TestReduceVectorPartitionLifecycleCheckpointTailV1(t *testing.T) {
	chain := lifecycleLegalChainV1(t)
	checkpoint := lifecycleCheckpointV1(t, chain[:1], 1)
	state, err := reduceVectorPartitionLifecycleCheckpointTailV1(checkpoint, chain[1:])
	if err != nil {
		t.Fatal(err)
	}
	if state.LastSequence != uint64(len(chain)) || state.LastDigest != chain[len(chain)-1].Digest ||
		state.GenerationHighWater != chain[0].Generation ||
		state.ActivationHighWater != chain[0].Generation ||
		len(state.Generations) != 0 {
		t.Fatalf("replayed checkpoint state=%+v", state)
	}

	readyOnly, err := reduceVectorPartitionLifecycleCheckpointTailV1(checkpoint, chain[1:2])
	if err != nil {
		t.Fatal(err)
	}
	if readyOnly.Generations[chain[0].Generation].Manifest.State != "ready" {
		t.Fatal("compact READY tail did not reconstruct ready manifest")
	}
	readyOnly.Generations[chain[0].Generation].Manifest.Placements[0].GroupID = "mutated"
	if checkpoint.State.Generations[chain[0].Generation].Manifest.Placements[0].GroupID == "mutated" {
		t.Fatal("replayed state aliases checkpoint input")
	}

	emptyTail, err := reduceVectorPartitionLifecycleCheckpointTailV1(checkpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	emptyTail.Generations[chain[0].Generation].Manifest.Placements[0].GroupID = "empty-tail-mutated"
	if checkpoint.State.Generations[chain[0].Generation].Manifest.Placements[0].GroupID == "empty-tail-mutated" {
		t.Fatal("empty-tail replay aliases checkpoint input")
	}
}

func TestReduceVectorPartitionLifecycleCheckpointTailV1RejectsGapForkBuildAndPreservesInput(t *testing.T) {
	chain := lifecycleLegalChainV1(t)
	checkpoint := lifecycleCheckpointV1(t, chain[:1], 1)
	before, err := encodeVectorPartitionLifecycleCheckpointCanonicalV1(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	ready := chain[1]
	gap := lifecycleRecordV1(t, ready.Sequence+1, ready.PreviousDigest, ready.Operation, ready.Generation, ready.Payload)
	fork := lifecycleRecordV1(t, ready.Sequence, [sha256.Size]byte{}, ready.Operation, ready.Generation, ready.Payload)

	buildRaw, build := lifecycleManifestPayloadV1(t, "building")
	build.Generation = checkpoint.State.GenerationHighWater + 1
	build.Canonicalize()
	buildRaw, err = EncodeVectorPartitionManifestV1(build)
	if err != nil {
		t.Fatal(err)
	}
	buildRecord := lifecycleRecordV1(t, ready.Sequence, ready.PreviousDigest, vectorPartitionLifecycleBuildV1, build.Generation, buildRaw)
	badDigest := ready
	badDigest.Digest[0] ^= 1
	wrongIdentityRecord := func(collection, index string) vectorPartitionLifecycleRecordV1 {
		t.Helper()
		raw, err := encodeVectorPartitionLifecycleRecordCanonicalV1(vectorPartitionLifecycleRecordV1{
			Collection:     collection,
			IndexName:      index,
			Sequence:       ready.Sequence,
			PreviousDigest: ready.PreviousDigest,
			Operation:      ready.Operation,
			Generation:     ready.Generation,
			Payload:        ready.Payload,
		})
		if err != nil {
			t.Fatal(err)
		}
		record, err := decodeVectorPartitionLifecycleRecordCanonicalV1(raw)
		if err != nil {
			t.Fatal(err)
		}
		return record
	}

	for name, record := range map[string]vectorPartitionLifecycleRecordV1{
		"gap":              gap,
		"fork":             fork,
		"build":            buildRecord,
		"bad-digest":       badDigest,
		"wrong-collection": wrongIdentityRecord("other", "embedding"),
		"wrong-index":      wrongIdentityRecord("docs", "other"),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := reduceVectorPartitionLifecycleCheckpointTailV1(checkpoint, []vectorPartitionLifecycleRecordV1{record}); err == nil {
				t.Fatal("accepted invalid checkpoint tail")
			}
			after, err := encodeVectorPartitionLifecycleCheckpointCanonicalV1(checkpoint)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("rejected tail mutated checkpoint input")
			}
		})
	}

	buildAfterReady := lifecycleRecordV1(t, ready.Sequence+1, ready.Digest, vectorPartitionLifecycleBuildV1, build.Generation, buildRaw)
	if _, err := reduceVectorPartitionLifecycleCheckpointTailV1(checkpoint, []vectorPartitionLifecycleRecordV1{ready, buildAfterReady}); err == nil {
		t.Fatal("accepted BUILD after a valid checkpoint delta")
	}
}
