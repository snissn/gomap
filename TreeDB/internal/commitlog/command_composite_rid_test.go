package commitlog

import (
	"errors"
	"reflect"
	"testing"
)

func compositeRIDTestPayload(t *testing.T) ([]byte, []RawKVOperation) {
	t.Helper()
	first := []RawKVOperation{
		{Op: RawKVOpSetRID, Key: []byte("nine-a"), RID: 9},
		{Op: RawKVOpSetRID, Key: []byte("three-a"), RID: 3},
		{Op: RawKVOpSetMaterializedRID, Key: []byte("materialized"), RID: 101, Value: []byte("carried value")},
	}
	second := []RawKVOperation{
		{Op: RawKVOpSetRID, Key: []byte("nine-b"), RID: 9},
		{Op: RawKVOpSetRID, Key: []byte("seven"), RID: 7},
		{Op: RawKVOpSetRID, Key: []byte("three-b"), RID: 3},
	}
	members := compositeTestMembers(t)
	for _, operations := range [][]RawKVOperation{first, second} {
		payload, err := EncodeRawKVBatchPayload(operations)
		if err != nil {
			t.Fatal(err)
		}
		members = append(members, CompositeMutationMember{Kind: CommandKindRawKVBatch, PayloadFormat: PayloadFormatRawKVBatchV2, Payload: payload})
	}
	payload, err := EncodeCompositeMutationPayloadTo(nil, members)
	if err != nil {
		t.Fatal(err)
	}
	return payload, append(first, second...)
}

func TestCompositeMutationRIDFenceUsesCanonicalWholeUnion(t *testing.T) {
	payload, operations := compositeRIDTestPayload(t)
	count, err := CompositeMutationExternalRIDCount(payload)
	if err != nil || count != 5 {
		t.Fatal(count, err)
	}
	raw, err := EncodeRawKVBatchPayload(operations)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := ExternalRefFenceV1FromRawKVPayload(raw)
	if err != nil || expected.Count != 3 {
		t.Fatal(expected, err)
	}
	scratch := []uint64{0, 0, 0, 0, 0, 123, 456}
	fence, err := CompositeMutationExternalRefFenceV1(payload, scratch)
	if err != nil || fence != expected || !reflect.DeepEqual(scratch, []uint64{3, 3, 7, 9, 9, 123, 456}) {
		t.Fatal("aggregate changed canonical raw semantics or unused scratch", fence, scratch, err)
	}
	if allocations := testing.AllocsPerRun(100, func() {
		fence, err = CompositeMutationExternalRefFenceV1(payload, scratch)
	}); allocations != 0 || err != nil || fence != expected {
		t.Fatal("aggregate fence allocated outside admitted scratch", allocations, err)
	}
	if allocations := testing.AllocsPerRun(100, func() {
		count, err = CompositeMutationExternalRIDCount(payload)
	}); allocations != 0 || err != nil || count != 5 {
		t.Fatal("aggregate capacity planning allocated", allocations, count, err)
	}
}

func TestCompositeMutationRIDFenceRefusesBeforeScratchMutation(t *testing.T) {
	payload, _ := compositeRIDTestPayload(t)
	scratch := []uint64{11, 22, 33, 44, 55, 66}
	original := append([]uint64(nil), scratch...)
	if fence, err := CompositeMutationExternalRefFenceV1(payload, scratch[:4]); !errors.Is(err, ErrRecordTooLarge) || fence != (ExternalRefFenceV1{}) || !reflect.DeepEqual(scratch, original) {
		t.Fatal("short original scratch was changed", fence, scratch, err)
	}
	for length := 0; length < len(payload); length++ {
		if fence, err := CompositeMutationExternalRefFenceV1(payload[:length], scratch); err == nil || fence != (ExternalRefFenceV1{}) || !reflect.DeepEqual(scratch, original) {
			t.Fatal("malformed suffix exposed RID prefix", length, fence, scratch, err)
		}
	}
	corrupt := append(append([]byte(nil), payload...), 0)
	if count, err := CompositeMutationExternalRIDCount(corrupt); err == nil || count != 0 {
		t.Fatal("invalid exact end returned a partial RID count", count, err)
	}
}

func TestCompositeMutationRIDFenceEmptyAndFrameStillFenced(t *testing.T) {
	payload, err := EncodeCompositeMutationPayloadTo(nil, compositeTestMembers(t))
	if err != nil {
		t.Fatal(err)
	}
	if count, err := CompositeMutationExternalRIDCount(payload); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	if fence, err := CompositeMutationExternalRefFenceV1(payload, nil); err != nil || fence != (ExternalRefFenceV1{}) {
		t.Fatal(fence, err)
	}
	// The codec helper does not enable append/replay before the original outer
	// producer, durability gate and atomic final-effect installer exist.
	if frame, err := EncodeCommandFrameV2(CommandEnvelope{
		Version: CommandFrameVersionV2, LSN: 1, Kind: CommandKindCompositeMutation,
		Scope: CommandScopeSystem, DurabilityClass: CommandDurabilityDurable,
		PayloadFormat: PayloadFormatCompositeMutationV1, Payload: payload,
	}); !errors.Is(err, ErrCommandWALUnsupportedKind) || frame != nil {
		t.Fatal("payload helper enabled incomplete composite frame route", err)
	}
}
