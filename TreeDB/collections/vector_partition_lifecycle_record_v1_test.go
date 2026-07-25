package collections

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"testing"
)

func lifecycleManifestPayloadV1(t *testing.T, state string) ([]byte, VectorPartitionManifestV1) {
	t.Helper()
	m := testVectorPartitionManifestV1()
	if state == "building" {
		m.State, m.RouterGeneration, m.RouterAsset, m.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
		m.Canonicalize()
	}
	raw, err := EncodeVectorPartitionManifestV1(m)
	if err != nil {
		t.Fatal(err)
	}
	return raw, m
}

func lifecycleReadyPromotionPayloadV1(t *testing.T, building, ready VectorPartitionManifestV1) []byte {
	t.Helper()
	raw, err := makeVectorPartitionReadyPromotionPayloadV1(building, ready)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func lifecycleRecordV1(t *testing.T, seq uint64, prev [32]byte, op vectorPartitionLifecycleOperationV1, generation uint64, payload []byte) vectorPartitionLifecycleRecordV1 {
	t.Helper()
	raw, err := encodeVectorPartitionLifecycleRecordCanonicalV1(vectorPartitionLifecycleRecordV1{Collection: "docs", IndexName: "embedding", Sequence: seq, PreviousDigest: prev, Operation: op, Generation: generation, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	r, err := decodeVectorPartitionLifecycleRecordCanonicalV1(raw)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func lifecycleLegalChainV1(t *testing.T) []vectorPartitionLifecycleRecordV1 {
	t.Helper()
	buildingRaw, building := lifecycleManifestPayloadV1(t, "building")
	_, ready := lifecycleManifestPayloadV1(t, "ready")
	readyPromotion := lifecycleReadyPromotionPayloadV1(t, building, ready)
	reclaim, err := encodeVectorPartitionReclaimRecordV1(vectorPartitionReclaimStateV1{Collection: "docs", IndexName: "embedding", Generation: ready.Generation, OriginalRefs: vectorPartitionReclaimRefsFromManifestV1(ready)})
	if err != nil {
		t.Fatal(err)
	}
	ops := []struct {
		op      vectorPartitionLifecycleOperationV1
		payload []byte
	}{
		{vectorPartitionLifecycleBuildV1, buildingRaw},
		{vectorPartitionLifecycleReadyV1, readyPromotion},
		{vectorPartitionLifecycleLocalActivateV1, nil},
		{vectorPartitionLifecycleDeactivateV1, nil},
		{vectorPartitionLifecycleDeletePrepareV1, reclaim},
		{vectorPartitionLifecycleReclaimProgressV1, reclaim},
		{vectorPartitionLifecycleDeleteCompleteV1, nil},
	}
	chain := make([]vectorPartitionLifecycleRecordV1, 0, len(ops))
	var prev [32]byte
	for i, op := range ops {
		r := lifecycleRecordV1(t, uint64(i+1), prev, op.op, building.Generation, op.payload)
		chain = append(chain, r)
		prev = r.Digest
	}
	return chain
}

func TestVectorPartitionLifecycleRecordV1CanonicalRoundTrip(t *testing.T) {
	chain := lifecycleLegalChainV1(t)
	for _, want := range chain {
		raw, err := encodeVectorPartitionLifecycleRecordCanonicalV1(want)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeVectorPartitionLifecycleRecordCanonicalV1(raw)
		if err != nil {
			t.Fatal(err)
		}
		if got.Collection != want.Collection || got.IndexName != want.IndexName || got.Sequence != want.Sequence || got.PreviousDigest != want.PreviousDigest || got.Operation != want.Operation || got.Generation != want.Generation || got.Digest != want.Digest || !bytes.Equal(got.Payload, want.Payload) {
			t.Fatalf("round trip got=%+v want=%+v", got, want)
		}
	}
}

func TestVectorPartitionReadyPromotionV1CanonicalRoundTripAndReconstruction(t *testing.T) {
	_, building := lifecycleManifestPayloadV1(t, "building")
	_, ready := lifecycleManifestPayloadV1(t, "ready")
	building.Representatives = nil
	building.Canonicalize()
	raw := lifecycleReadyPromotionPayloadV1(t, building, ready)
	promotion, err := decodeVectorPartitionReadyPromotionCanonicalV1(raw)
	if err != nil {
		t.Fatal(err)
	}
	again, err := encodeVectorPartitionReadyPromotionCanonicalV1(promotion)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, again) {
		t.Fatal("ready promotion encoding is not deterministic")
	}
	got, err := applyVectorPartitionReadyPromotionV1(building, raw)
	if err != nil {
		t.Fatal(err)
	}
	gotRaw, err := EncodeVectorPartitionManifestV1(got)
	if err != nil {
		t.Fatal(err)
	}
	wantRaw, err := EncodeVectorPartitionManifestV1(ready)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotRaw, wantRaw) {
		t.Fatal("ready promotion did not reconstruct the exact canonical manifest")
	}
	if len(got.Representatives) == 0 {
		t.Fatal("ready promotion omitted the computed representative mapping")
	}
	if len(raw) >= len(wantRaw) {
		t.Fatalf("promotion bytes=%d, full ready manifest=%d", len(raw), len(wantRaw))
	}
}

func TestVectorPartitionReadyPromotionV1RejectsMalformedAndWrongDigests(t *testing.T) {
	_, building := lifecycleManifestPayloadV1(t, "building")
	readyRaw, ready := lifecycleManifestPayloadV1(t, "ready")
	raw := lifecycleReadyPromotionPayloadV1(t, building, ready)
	promotion, err := decodeVectorPartitionReadyPromotionCanonicalV1(raw)
	if err != nil {
		t.Fatal(err)
	}
	wrongGeneration := promotion
	wrongGeneration.Generation++
	wrongGeneration.RouterGeneration++
	wrongGenerationRaw, err := encodeVectorPartitionReadyPromotionCanonicalV1(wrongGeneration)
	if err != nil {
		t.Fatal(err)
	}
	wrongBase := promotion
	wrongBase.BuildingDigest[0] ^= 1
	wrongBaseRaw, err := encodeVectorPartitionReadyPromotionCanonicalV1(wrongBase)
	if err != nil {
		t.Fatal(err)
	}
	wrongResult := promotion
	wrongResult.ReadyDigest[0] ^= 1
	wrongResultRaw, err := encodeVectorPartitionReadyPromotionCanonicalV1(wrongResult)
	if err != nil {
		t.Fatal(err)
	}
	wrongReadySet := promotion
	wrongReadySet.ReadySetDigest = string(bytes.Repeat([]byte{'0'}, sha256.Size*2))
	wrongReadySetRaw, err := encodeVectorPartitionReadyPromotionCanonicalV1(wrongReadySet)
	if err != nil {
		t.Fatal(err)
	}
	for name, candidate := range map[string][]byte{
		"old-full-ready-vpm": readyRaw,
		"checksum": func() []byte {
			x := append([]byte(nil), raw...)
			x[len(x)-1] ^= 1
			return x
		}(),
		"trailing": func() []byte {
			x := append([]byte(nil), raw[:len(raw)-sha256.Size]...)
			x = append(x, 0)
			sum := sha256.Sum256(x)
			return append(x, sum[:]...)
		}(),
		"truncated": append([]byte(nil), raw[:len(raw)-1]...),
		"wrong-version": func() []byte {
			x := append([]byte(nil), raw...)
			x[7]++
			sum := sha256.Sum256(x[:len(x)-sha256.Size])
			copy(x[len(x)-sha256.Size:], sum[:])
			return x
		}(),
		"wrong-generation": wrongGenerationRaw,
		"wrong-base":       wrongBaseRaw,
		"wrong-result":     wrongResultRaw,
		"wrong-ready-set":  wrongReadySetRaw,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := applyVectorPartitionReadyPromotionV1(building, candidate); err == nil {
				t.Fatal("accepted invalid ready promotion")
			}
		})
	}
}

func TestVectorPartitionReadyPromotionV1RejectsIllegalRouterMutation(t *testing.T) {
	_, building := lifecycleManifestPayloadV1(t, "building")
	_, ready := lifecycleManifestPayloadV1(t, "ready")
	promotionRaw := lifecycleReadyPromotionPayloadV1(t, building, ready)
	promotion, err := decodeVectorPartitionReadyPromotionCanonicalV1(promotionRaw)
	if err != nil {
		t.Fatal(err)
	}
	promotion.RouterAsset.PartitionID = 1
	if _, err := encodeVectorPartitionReadyPromotionCanonicalV1(promotion); err == nil {
		t.Fatal("accepted router asset with logical partition ID")
	}
	promotion.RouterAsset.PartitionID = 0
	promotion.RouterGeneration++
	if _, err := encodeVectorPartitionReadyPromotionCanonicalV1(promotion); err == nil {
		t.Fatal("accepted router generation different from manifest generation")
	}
}

func TestReduceVectorPartitionLifecycleChainV1LegalTransitionsAndCrashPrefixes(t *testing.T) {
	chain := lifecycleLegalChainV1(t)
	for n := 1; n <= len(chain); n++ {
		t.Run("prefix", func(t *testing.T) {
			state, err := reduceVectorPartitionLifecycleChainV1(chain[:n])
			if err != nil {
				t.Fatal(err)
			}
			if state.LastSequence != uint64(n) {
				t.Fatalf("last sequence=%d", state.LastSequence)
			}
			if n == 3 && state.ActiveGeneration != chain[0].Generation {
				t.Fatalf("active=%d", state.ActiveGeneration)
			}
			if n == 4 && state.RetiredGeneration != chain[0].Generation {
				t.Fatalf("retired=%d", state.RetiredGeneration)
			}
			if n == 5 && (!state.Generations[chain[0].Generation].Deleting || state.Generations[chain[0].Generation].Reclaim == nil) {
				t.Fatalf("delete state=%+v", state)
			}
			if n == 7 && (state.Generations[chain[0].Generation].Manifest != nil || state.GenerationHighWater != chain[0].Generation) {
				t.Fatalf("complete state=%+v", state)
			}
		})
	}
}

func TestReduceVectorPartitionLifecycleChainV1MultiGenerationAuthority(t *testing.T) {
	buildRaw, build := lifecycleManifestPayloadV1(t, "building")
	_, ready := lifecycleManifestPayloadV1(t, "ready")
	readyPromotion := lifecycleReadyPromotionPayloadV1(t, build, ready)
	ready2 := ready
	ready2.Generation, ready2.RouterGeneration = ready.Generation+1, ready.Generation+1
	ready2.RouterAsset.Ref.Generation = ready2.Generation
	ready2.Canonicalize()
	build2 := ready2
	build2.State, build2.RouterGeneration, build2.RouterAsset, build2.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
	build2.Canonicalize()
	build2Raw, err := EncodeVectorPartitionManifestV1(build2)
	if err != nil {
		t.Fatal(err)
	}
	ready2Promotion := lifecycleReadyPromotionPayloadV1(t, build2, ready2)
	reclaim, err := encodeVectorPartitionReclaimRecordV1(vectorPartitionReclaimStateV1{Collection: "docs", IndexName: "embedding", Generation: ready.Generation, OriginalRefs: vectorPartitionReclaimRefsFromManifestV1(ready)})
	if err != nil {
		t.Fatal(err)
	}
	var prev [32]byte
	appendRecord := func(chain *[]vectorPartitionLifecycleRecordV1, op vectorPartitionLifecycleOperationV1, gen uint64, payload []byte) {
		r := lifecycleRecordV1(t, uint64(len(*chain)+1), prev, op, gen, payload)
		*chain = append(*chain, r)
		prev = r.Digest
	}
	var chain []vectorPartitionLifecycleRecordV1
	appendRecord(&chain, vectorPartitionLifecycleBuildV1, build.Generation, buildRaw)
	appendRecord(&chain, vectorPartitionLifecycleReadyV1, ready.Generation, readyPromotion)
	appendRecord(&chain, vectorPartitionLifecycleLocalActivateV1, ready.Generation, nil)
	appendRecord(&chain, vectorPartitionLifecycleBuildV1, build2.Generation, build2Raw)
	appendRecord(&chain, vectorPartitionLifecycleReadyV1, ready2.Generation, ready2Promotion)
	appendRecord(&chain, vectorPartitionLifecycleLocalActivateV1, ready2.Generation, nil)
	appendRecord(&chain, vectorPartitionLifecycleDeletePrepareV1, ready.Generation, reclaim)
	appendRecord(&chain, vectorPartitionLifecycleReclaimProgressV1, ready.Generation, reclaim)
	appendRecord(&chain, vectorPartitionLifecycleDeleteCompleteV1, ready.Generation, nil)
	appendRecord(&chain, vectorPartitionLifecycleDeactivateV1, ready2.Generation, nil)
	for n := 1; n <= len(chain); n++ {
		state, err := reduceVectorPartitionLifecycleChainV1(chain[:n])
		if err != nil {
			t.Fatalf("prefix %d: %v", n, err)
		}
		if n == 6 && (state.ActiveGeneration != ready2.Generation || state.RetiredGeneration != ready.Generation) {
			t.Fatalf("replacement state=%+v", state)
		}
		if n == 9 && (state.GenerationHighWater != ready2.Generation || state.ActiveGeneration != ready2.Generation || state.Generations[ready2.Generation].Manifest == nil) {
			t.Fatalf("g1 delete disturbed g2: %+v", state)
		}
		if n == 10 && (state.ActiveGeneration != 0 || state.RetiredGeneration != ready2.Generation) {
			t.Fatalf("g2 deactivate=%+v", state)
		}
	}
	staleActivation := append([]vectorPartitionLifecycleRecordV1(nil), chain[:6]...)
	staleActivation = append(staleActivation, lifecycleRecordV1(
		t,
		7,
		chain[5].Digest,
		vectorPartitionLifecycleLocalActivateV1,
		ready.Generation,
		nil,
	))
	if _, err := reduceVectorPartitionLifecycleChainV1(staleActivation); err == nil {
		t.Fatal("stale ready generation reactivated after a newer generation")
	}
	// A retained completed generation cannot be revived by a later BUILD.
	bad := append([]vectorPartitionLifecycleRecordV1(nil), chain...)
	appendRecord(&bad, vectorPartitionLifecycleBuildV1, build.Generation, buildRaw)
	if _, err := reduceVectorPartitionLifecycleChainV1(bad); err == nil {
		t.Fatal("completed generation revived")
	}
}

func TestReduceVectorPartitionLifecycleChainV1HighWaterAndLiveGenerationCap(t *testing.T) {
	buildRaw, build := lifecycleManifestPayloadV1(t, "building")
	buildAt := func(generation uint64) ([]byte, VectorPartitionManifestV1) {
		m := build
		m.Generation = generation
		m.Canonicalize()
		raw, err := EncodeVectorPartitionManifestV1(m)
		if err != nil {
			t.Fatal(err)
		}
		return raw, m
	}
	build2Raw, build2 := buildAt(build.Generation + 1)
	build3Raw, build3 := buildAt(build.Generation + 2)

	first := lifecycleRecordV1(t, 1, [32]byte{}, vectorPartitionLifecycleBuildV1, build.Generation, buildRaw)
	second := lifecycleRecordV1(t, 2, first.Digest, vectorPartitionLifecycleBuildV1, build2.Generation, build2Raw)
	state, err := reduceVectorPartitionLifecycleChainV1([]vectorPartitionLifecycleRecordV1{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if state.GenerationHighWater != build2.Generation || len(state.Generations) != 2 {
		t.Fatalf("two-live state=%+v", state)
	}
	beforeHighWater := state.GenerationHighWater
	third := lifecycleRecordV1(t, 3, second.Digest, vectorPartitionLifecycleBuildV1, build3.Generation, build3Raw)
	if err := reduceVectorPartitionLifecycleRecordV1(&state, third); err == nil {
		t.Fatal("accepted a third live generation")
	}
	if state.GenerationHighWater != beforeHighWater || len(state.Generations) != 2 {
		t.Fatalf("rejected third generation mutated state=%+v", state)
	}

	completed := state.Generations[build.Generation]
	completed.Deleting = true
	state.Generations[build.Generation] = completed
	completeFirst := third
	completeFirst.Operation = vectorPartitionLifecycleDeleteCompleteV1
	completeFirst.Generation = build.Generation
	completeFirst.Payload = nil
	if err := reduceVectorPartitionLifecycleRecordV1(&state, completeFirst); err != nil {
		t.Fatal(err)
	}
	if err := reduceVectorPartitionLifecycleRecordV1(&state, third); err != nil {
		t.Fatal(err)
	}
	if state.GenerationHighWater != build3.Generation || len(state.Generations) != 2 {
		t.Fatalf("higher generation after completion state=%+v", state)
	}

	completed = state.Generations[build2.Generation]
	completed.Deleting = true
	state.Generations[build2.Generation] = completed
	completeSecond := third
	completeSecond.Operation = vectorPartitionLifecycleDeleteCompleteV1
	completeSecond.Generation = build2.Generation
	completeSecond.Payload = nil
	if err := reduceVectorPartitionLifecycleRecordV1(&state, completeSecond); err != nil {
		t.Fatal(err)
	}
	oldRaw, old := buildAt(build2.Generation)
	oldRecord := third
	oldRecord.Generation, oldRecord.Payload = old.Generation, oldRaw
	if err := reduceVectorPartitionLifecycleRecordV1(&state, oldRecord); err == nil {
		t.Fatal("generation at high-water was revived")
	}
	if state.GenerationHighWater != build3.Generation || len(state.Generations) != 1 {
		t.Fatalf("rejected old generation mutated state=%+v", state)
	}
}

func TestReduceVectorPartitionLifecycleRecordV1InvalidBuildLeavesHighWaterUnchanged(t *testing.T) {
	state := vectorPartitionLifecycleStateV1{
		Collection:  "docs",
		IndexName:   "embedding",
		Generations: make(map[uint64]vectorPartitionLifecycleGenerationStateV1),
	}
	r := vectorPartitionLifecycleRecordV1{
		Collection: "docs",
		IndexName:  "embedding",
		Operation:  vectorPartitionLifecycleBuildV1,
		Generation: 100,
		Payload:    []byte("not-a-manifest"),
	}
	if err := reduceVectorPartitionLifecycleRecordV1(&state, r); err == nil {
		t.Fatal("accepted invalid BUILD manifest")
	}
	if state.GenerationFloor != 0 || state.GenerationHighWater != 0 || len(state.Generations) != 0 {
		t.Fatalf("invalid BUILD mutated state=%+v", state)
	}
}

func TestReduceVectorPartitionLifecycleRecordV1RejectsGenerationGap(t *testing.T) {
	firstRaw, first := lifecycleManifestPayloadV1(t, "building")
	state := vectorPartitionLifecycleStateV1{
		Collection:  first.Collection,
		IndexName:   first.IndexName,
		Generations: make(map[uint64]vectorPartitionLifecycleGenerationStateV1),
	}
	if err := reduceVectorPartitionLifecycleRecordV1(&state, vectorPartitionLifecycleRecordV1{
		Collection: first.Collection,
		IndexName:  first.IndexName,
		Operation:  vectorPartitionLifecycleBuildV1,
		Generation: first.Generation,
		Payload:    firstRaw,
	}); err != nil {
		t.Fatal(err)
	}
	gap := cloneVectorPartitionManifestForCheckpointV1(first)
	gap.Generation += 2
	gap.Canonicalize()
	gapRaw, err := EncodeVectorPartitionManifestV1(gap)
	if err != nil {
		t.Fatal(err)
	}
	if err := reduceVectorPartitionLifecycleRecordV1(&state, vectorPartitionLifecycleRecordV1{
		Collection: gap.Collection,
		IndexName:  gap.IndexName,
		Operation:  vectorPartitionLifecycleBuildV1,
		Generation: gap.Generation,
		Payload:    gapRaw,
	}); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("generation gap err=%v", err)
	}
	if state.GenerationFloor != first.Generation ||
		state.GenerationHighWater != first.Generation ||
		len(state.Generations) != 1 {
		t.Fatalf("rejected generation gap mutated state=%+v", state)
	}
}

func TestReduceVectorPartitionLifecycleChainV1ActivationClearsStaleRetired(t *testing.T) {
	build1Raw, build1 := lifecycleManifestPayloadV1(t, "building")
	_, ready1 := lifecycleManifestPayloadV1(t, "ready")
	ready1Promotion := lifecycleReadyPromotionPayloadV1(t, build1, ready1)
	ready2 := ready1
	ready2.Generation, ready2.RouterGeneration = ready1.Generation+1, ready1.Generation+1
	ready2.RouterAsset.Ref.Generation = ready2.Generation
	ready2.Canonicalize()
	build2 := ready2
	build2.State, build2.RouterGeneration, build2.RouterAsset, build2.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
	build2.Canonicalize()
	build2Raw, _ := EncodeVectorPartitionManifestV1(build2)
	ready2Promotion := lifecycleReadyPromotionPayloadV1(t, build2, ready2)
	var chain []vectorPartitionLifecycleRecordV1
	var prev [32]byte
	appendRecord := func(op vectorPartitionLifecycleOperationV1, generation uint64, payload []byte) {
		r := lifecycleRecordV1(t, uint64(len(chain)+1), prev, op, generation, payload)
		chain, prev = append(chain, r), r.Digest
	}
	appendRecord(vectorPartitionLifecycleBuildV1, build1.Generation, build1Raw)
	appendRecord(vectorPartitionLifecycleReadyV1, ready1.Generation, ready1Promotion)
	appendRecord(vectorPartitionLifecycleLocalActivateV1, ready1.Generation, nil)
	appendRecord(vectorPartitionLifecycleDeactivateV1, ready1.Generation, nil)
	appendRecord(vectorPartitionLifecycleBuildV1, build2.Generation, build2Raw)
	appendRecord(vectorPartitionLifecycleReadyV1, ready2.Generation, ready2Promotion)
	appendRecord(vectorPartitionLifecycleLocalActivateV1, ready2.Generation, nil)
	state, err := reduceVectorPartitionLifecycleChainV1(chain)
	if err != nil || state.ActiveGeneration != ready2.Generation || state.RetiredGeneration != 0 {
		t.Fatalf("state=%+v err=%v, want g2 active without stale retired", state, err)
	}
}

func TestReduceVectorPartitionLifecycleChainV1ActivationHighWaterSurvivesDelete(t *testing.T) {
	build1Raw, build1 := lifecycleManifestPayloadV1(t, "building")
	_, ready1 := lifecycleManifestPayloadV1(t, "ready")
	ready1Promotion := lifecycleReadyPromotionPayloadV1(t, build1, ready1)
	ready2 := cloneVectorPartitionManifestForCheckpointV1(ready1)
	ready2.Generation++
	ready2.RouterGeneration++
	ready2.RouterAsset.Ref.Generation = ready2.Generation
	ready2.Canonicalize()
	build2 := cloneVectorPartitionManifestForCheckpointV1(ready2)
	build2.State = "building"
	build2.RouterGeneration = 0
	build2.RouterAsset = VectorPartitionAssetV1{}
	build2.ReadySetDigest = ""
	build2.Canonicalize()
	build2Raw, err := EncodeVectorPartitionManifestV1(build2)
	if err != nil {
		t.Fatal(err)
	}
	ready2Promotion := lifecycleReadyPromotionPayloadV1(t, build2, ready2)
	reclaim, err := newVectorPartitionReclaimStateV1(ready2)
	if err != nil {
		t.Fatal(err)
	}
	reclaimRaw, err := encodeVectorPartitionReclaimRecordV1(reclaim)
	if err != nil {
		t.Fatal(err)
	}

	var chain []vectorPartitionLifecycleRecordV1
	var previous [sha256.Size]byte
	appendRecord := func(operation vectorPartitionLifecycleOperationV1, generation uint64, payload []byte) {
		record := lifecycleRecordV1(t, uint64(len(chain)+1), previous, operation, generation, payload)
		chain = append(chain, record)
		previous = record.Digest
	}
	appendRecord(vectorPartitionLifecycleBuildV1, build1.Generation, build1Raw)
	appendRecord(vectorPartitionLifecycleReadyV1, ready1.Generation, ready1Promotion)
	appendRecord(vectorPartitionLifecycleBuildV1, build2.Generation, build2Raw)
	appendRecord(vectorPartitionLifecycleReadyV1, ready2.Generation, ready2Promotion)
	appendRecord(vectorPartitionLifecycleLocalActivateV1, ready2.Generation, nil)
	appendRecord(vectorPartitionLifecycleDeactivateV1, ready2.Generation, nil)
	appendRecord(vectorPartitionLifecycleDeletePrepareV1, ready2.Generation, reclaimRaw)
	appendRecord(vectorPartitionLifecycleDeleteCompleteV1, ready2.Generation, nil)

	state, err := reduceVectorPartitionLifecycleChainV1(chain)
	if err != nil {
		t.Fatal(err)
	}
	if state.ActivationHighWater != ready2.Generation ||
		state.ActiveGeneration != 0 ||
		state.RetiredGeneration != 0 {
		t.Fatalf("post-delete activation authority=%+v", state)
	}

	stale := append(append([]vectorPartitionLifecycleRecordV1(nil), chain...), lifecycleRecordV1(
		t,
		uint64(len(chain)+1),
		previous,
		vectorPartitionLifecycleLocalActivateV1,
		ready1.Generation,
		nil,
	))
	if _, err := reduceVectorPartitionLifecycleChainV1(stale); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("stale activation after newer deletion err=%v", err)
	}
}

func TestReduceVectorPartitionLifecycleChainV1ReclaimConstraints(t *testing.T) {
	chain := lifecycleLegalChainV1(t)
	// BUILD, READY, ACTIVATE, DEACTIVATE is the legal delete-preparation prefix.
	prefix := append([]vectorPartitionLifecycleRecordV1(nil), chain[:4]...)
	base, err := decodeVectorPartitionReclaimRecordV1(chain[4].Payload)
	if err != nil {
		t.Fatal(err)
	}
	withSuperseded := base.clone()
	superseded := base.OriginalRefs[0]
	superseded.FileID += 100
	withSuperseded.SupersededRefs = append(withSuperseded.SupersededRefs, superseded)
	badPrepare, err := encodeVectorPartitionReclaimRecordV1(withSuperseded)
	if err != nil {
		t.Fatal(err)
	}
	bad := append(prefix, lifecycleRecordV1(t, 5, prefix[3].Digest, vectorPartitionLifecycleDeletePrepareV1, base.Generation, badPrepare))
	if _, err := reduceVectorPartitionLifecycleChainV1(bad); err == nil {
		t.Fatal("delete prepare accepted superseded refs")
	}
	prepare := append(prefix, chain[4])
	wrongOriginal := base.clone()
	wrongOriginal.OriginalRefs[0].FileID++
	wrongRaw, err := encodeVectorPartitionReclaimRecordV1(wrongOriginal)
	if err != nil {
		t.Fatal(err)
	}
	bad = append(prepare, lifecycleRecordV1(t, 6, prepare[4].Digest, vectorPartitionLifecycleReclaimProgressV1, base.Generation, wrongRaw))
	if _, err := reduceVectorPartitionLifecycleChainV1(bad); err == nil {
		t.Fatal("reclaim progress changed original refs")
	}
	grow := base.clone()
	grow.SupersededRefs = append(grow.SupersededRefs, superseded)
	growRaw, err := encodeVectorPartitionReclaimRecordV1(grow)
	if err != nil {
		t.Fatal(err)
	}
	progress := append(prepare, lifecycleRecordV1(t, 6, prepare[4].Digest, vectorPartitionLifecycleReclaimProgressV1, base.Generation, growRaw))
	shrink := append(progress, lifecycleRecordV1(t, 7, progress[5].Digest, vectorPartitionLifecycleReclaimProgressV1, base.Generation, chain[5].Payload))
	if _, err := reduceVectorPartitionLifecycleChainV1(shrink); err == nil {
		t.Fatal("reclaim progress shrank superseded refs")
	}
}

func TestReduceVectorPartitionLifecycleChainV1RejectsRecordCountCap(t *testing.T) {
	if _, err := reduceVectorPartitionLifecycleChainV1(make([]vectorPartitionLifecycleRecordV1, vectorPartitionLifecycleMaxRecordsV1+1)); err == nil {
		t.Fatal("accepted over-cap lifecycle chain")
	}
}

func TestVectorPartitionLifecycleRecordV1RejectsMalformedAndUnbounded(t *testing.T) {
	chain := lifecycleLegalChainV1(t)
	raw, err := encodeVectorPartitionLifecycleRecordCanonicalV1(chain[0])
	if err != nil {
		t.Fatal(err)
	}
	for name, corrupt := range map[string][]byte{
		"checksum": func() []byte { x := append([]byte(nil), raw...); x[len(x)-1] ^= 1; return x }(),
		"trailing": append(append([]byte(nil), raw...), 0),
		"tiny-huge-payload": func() []byte {
			x := append([]byte(nil), raw[:vectorPartitionLifecycleHeaderBytesV1+sha256.Size]...)
			binary.BigEndian.PutUint32(x[26:30], ^uint32(0))
			return x
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeVectorPartitionLifecycleRecordCanonicalV1(corrupt); err == nil {
				t.Fatal("accepted corrupt record")
			}
		})
	}
}

func TestVectorPartitionLifecycleRecordV1RejectsOperationPayloadShapes(t *testing.T) {
	buildingRaw, building := lifecycleManifestPayloadV1(t, "building")
	_, ready := lifecycleManifestPayloadV1(t, "ready")
	readyPromotion := lifecycleReadyPromotionPayloadV1(t, building, ready)
	for name, r := range map[string]vectorPartitionLifecycleRecordV1{
		"invalid-operation":      {Collection: "docs", IndexName: "embedding", Sequence: 1, Operation: 99, Generation: building.Generation},
		"build-missing-payload":  {Collection: "docs", IndexName: "embedding", Sequence: 1, Operation: vectorPartitionLifecycleBuildV1, Generation: building.Generation},
		"ready-wrong-state":      {Collection: "docs", IndexName: "embedding", Sequence: 1, Operation: vectorPartitionLifecycleReadyV1, Generation: building.Generation, Payload: buildingRaw},
		"ready-wrong-generation": {Collection: "docs", IndexName: "embedding", Sequence: 1, Operation: vectorPartitionLifecycleReadyV1, Generation: building.Generation + 1, Payload: readyPromotion},
		"activate-unexpected":    {Collection: "docs", IndexName: "embedding", Sequence: 1, Operation: vectorPartitionLifecycleLocalActivateV1, Generation: building.Generation, Payload: []byte("x")},
		"delete-missing-payload": {Collection: "docs", IndexName: "embedding", Sequence: 1, Operation: vectorPartitionLifecycleDeletePrepareV1, Generation: building.Generation},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := encodeVectorPartitionLifecycleRecordCanonicalV1(r); err == nil {
				t.Fatal("accepted invalid shape")
			}
		})
	}
}

func TestReduceVectorPartitionLifecycleChainV1RejectsChainFailures(t *testing.T) {
	chain := lifecycleLegalChainV1(t)
	for name, mutate := range map[string]func([]vectorPartitionLifecycleRecordV1){
		"gap":                func(x []vectorPartitionLifecycleRecordV1) { x[1].Sequence = 3 },
		"duplicate":          func(x []vectorPartitionLifecycleRecordV1) { x[1].Sequence = 1 },
		"fork":               func(x []vectorPartitionLifecycleRecordV1) { x[1].PreviousDigest = [32]byte{} },
		"bad-first-prev":     func(x []vectorPartitionLifecycleRecordV1) { x[0].PreviousDigest[0] = 1 },
		"identity":           func(x []vectorPartitionLifecycleRecordV1) { x[1].Collection = "other" },
		"chain-checksum":     func(x []vectorPartitionLifecycleRecordV1) { x[1].Digest[0] ^= 1 },
		"illegal-transition": nil,
	} {
		t.Run(name, func(t *testing.T) {
			x := append([]vectorPartitionLifecycleRecordV1(nil), chain...)
			if name == "illegal-transition" {
				x = x[:1]
			}
			if mutate != nil {
				mutate(x)
			}
			if name == "illegal-transition" {
				x = append(x, lifecycleRecordV1(t, 2, x[0].Digest, vectorPartitionLifecycleLocalActivateV1, x[0].Generation, nil))
			}
			if _, err := reduceVectorPartitionLifecycleChainV1(x); err == nil {
				t.Fatal("accepted invalid chain")
			}
		})
	}
}
