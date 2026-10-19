package commitlog

import (
	"bytes"
	"errors"
	"testing"
)

func TestSelectedCompositeBindsFinalRawRevisionWithoutChangingNativeMembers(t *testing.T) {
	raw, err := EncodeRawKVSingleOperationPayload(RawKVOperation{Op: RawKVOpSet, Key: []byte("raw"), Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := EncodeCollectionInsertBatchByIDPayload("json", []CollectionDocument{{ID: []byte("x"), Document: []byte(`{"x":1}`)}})
	if err != nil {
		t.Fatal(err)
	}
	members := []CompositeMutationMember{{Kind: CommandKindRawKVBatch, PayloadFormat: PayloadFormatRawKVBatchV1, Payload: raw}, {Kind: CommandKindCollectionInsertBatchByID, PayloadFormat: PayloadFormatCollectionInsertBatchByIDV1, Incarnation: 7, Payload: rows}}
	// Exercise the existing typed and source-replacement payloads through the
	// same complete aggregate validation and final raw-identity binding.
	members = append(members, compositeTestMembers(t)[2:]...)
	size, err := PlanCompositeMutationPayload(members)
	if err != nil {
		t.Fatal(err)
	}
	image, err := EncodeCompositeMutationPayloadTo(make([]byte, 0, size+8), members)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := EncodeRawKVSingleOperationPayload(RawKVOperation{Op: RawKVOpSet, Key: []byte("raw"), Value: []byte("value"), Revision: 99})
	if err != nil {
		t.Fatal(err)
	}
	original := append([]byte(nil), image...)
	if got, err := BindCompositeRawPayload(image[:len(image):len(image)], canonical); !errors.Is(err, ErrRecordTooLarge) || got != nil || !bytes.Equal(image, original) {
		t.Fatal("refusal mutated original", err)
	}
	bound, err := BindCompositeRawPayload(image, canonical)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCompositeMutationPayloadView(bound)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != len(members) || !bytes.Equal(decoded[0].Payload, canonical) {
		t.Fatal("native member or final raw changed", decoded)
	}
	for i := 1; i < len(members); i++ {
		if decoded[i].Kind != members[i].Kind || decoded[i].PayloadFormat != members[i].PayloadFormat || decoded[i].Incarnation != members[i].Incarnation || !bytes.Equal(decoded[i].Payload, members[i].Payload) {
			t.Fatal("native member or logical identity changed", i, decoded[i])
		}
	}
	frame, err := EncodeCommandFrameV2(CommandEnvelope{Version: CommandFrameVersionV2, LSN: 1, Kind: CommandKindCompositeMutation, Scope: CommandScopeSystem, DurabilityClass: CommandDurabilityDurable, PayloadFormat: PayloadFormatCompositeMutationV1, Payload: bound})
	if err != nil || len(frame) == 0 {
		t.Fatal("selected frame refused", err)
	}
	rid, err := EncodeRawKVSingleOperationPayload(RawKVOperation{Op: RawKVOpSetRID, Key: []byte("raw"), RID: 9})
	if err != nil {
		t.Fatal(err)
	}
	original = append(original[:0], bound...)
	if _, err = BindCompositeRawPayload(bound, rid); !errors.Is(err, ErrCommandWALUnsupportedKind) || !bytes.Equal(bound, original) {
		t.Fatal("external raw accepted or mutated image", err)
	}
	members[0].PayloadFormat = PayloadFormatRawKVBatchV2
	members[0].Payload = rid
	external, err := EncodeCompositeMutationPayloadTo(nil, members)
	if err != nil {
		t.Fatal(err)
	}
	if frame, err = EncodeCommandFrameV2(CommandEnvelope{Version: CommandFrameVersionV2, LSN: 1, Kind: CommandKindCompositeMutation, Scope: CommandScopeSystem, DurabilityClass: CommandDurabilityDurable, PayloadFormat: PayloadFormatCompositeMutationV1, Payload: external}); !errors.Is(err, ErrCommandWALUnsupportedKind) || frame != nil {
		t.Fatal("external frame activated", err)
	}
}
