package commitlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"testing"
)

func TestColocatedVectorMutationConditionalPayloadV2(t *testing.T) {
	v := ColocatedVectorMutationWALV1{Scope: ColocatedVectorMutationScopeV1{Version: 1, Index: "embedding", Generation: 7, OwnerGroup: "owner", Digest: sha256.Sum256([]byte("scope"))}, Collection: "docs", ID: []byte("id"), Attempt: []byte("attempt"), Document: []byte(`{"embedding":[1,0]}`), CommandDigest: sha256.Sum256([]byte("command")), Term: 2, Index: 9, Matched: 1, Affected: 1}
	ordinary, err := EncodeCollectionUpdateBatchByIDPayload(v.Collection, []CollectionDocument{{ID: v.ID, Document: v.Document}})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCollectionUpdateBatchByIDPayload(ordinary)
	if err != nil || decoded.Colocated != nil {
		t.Fatalf("ordinary decode=%+v err=%v", decoded, err)
	}
	scoped, err := EncodeColocatedVectorReplacePayloadV2(v)
	if err != nil {
		t.Fatal(err)
	}
	assertGoldenHex(t, "colocated_replace_v2_payload.hex", scoped)
	got, err := DecodeCollectionUpdateBatchByIDPayload(scoped)
	if err != nil || got.Colocated == nil || !bytes.Equal(got.Colocated.Document, v.Document) || got.Colocated.Term != 2 || got.Colocated.Index != 9 {
		t.Fatalf("scoped=%+v err=%v", got, err)
	}
	base, _, err := decodeColocatedVectorMutationPayloadV2(scoped)
	if err != nil || !bytes.Equal(base, ordinary) {
		t.Fatal("conditional wrapper changed ordinary bytes")
	}
	// JSON metadata must match the ordinary target and remain bounded/versioned.
	malformed := bytes.Clone(scoped)
	binary.LittleEndian.PutUint32(malformed[2:6], uint32(len(scoped)))
	if _, err := DecodeCollectionUpdateBatchByIDPayload(malformed); err == nil {
		t.Fatal("malformed wrapper length accepted")
	}
	malformed = append(bytes.Clone(scoped), []byte(` {}`)...)
	if _, err := DecodeCollectionUpdateBatchByIDPayload(malformed); err == nil {
		t.Fatal("trailing metadata accepted")
	}
	forged := v
	forged.Term = 0
	if _, err := EncodeColocatedVectorReplacePayloadV2(forged); err == nil {
		t.Fatal("WAL encoded missing FSM authority")
	}
	mismatch := v
	mismatch.ID = []byte("other")
	raw, err := encodeColocatedVectorMutationPayloadV2(ordinary, mismatch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeCollectionUpdateBatchByIDPayload(raw); err == nil {
		t.Fatal("WAL target mismatch accepted")
	}
	v.Delete, v.Document, v.Matched = true, nil, 0
	scoped, err = EncodeColocatedVectorDeletePayloadV2(v)
	if err != nil {
		t.Fatal(err)
	}
	assertGoldenHex(t, "colocated_delete_v2_payload.hex", scoped)
	deleted, err := DecodeCollectionDeleteBatchByIDPayload(scoped)
	if err != nil || deleted.Colocated == nil || !deleted.Colocated.Delete {
		t.Fatalf("delete=%+v err=%v", deleted, err)
	}
}

func TestColocatedVectorMutationOutcomeFormatV1(t *testing.T) {
	v := ColocatedVectorMutationOutcomeV1{ScopeDigest: sha256.Sum256([]byte("scope")), CommandDigest: sha256.Sum256([]byte("command")), Term: 2, Index: 9, Coverage: 10, Revision: 0, Matched: 1, Affected: 0, Ordinal: 1, AppliedCommandLSN: 4}
	raw, err := EncodeColocatedVectorMutationOutcomeV1(v)
	if err != nil || len(raw) != 144 {
		t.Fatalf("outcome bytes=%d err=%v", len(raw), err)
	}
	got, err := DecodeColocatedVectorMutationOutcomeV1(raw)
	if err != nil || got != v {
		t.Fatalf("outcome=%+v err=%v", got, err)
	}
	for _, bad := range [][]byte{raw[:len(raw)-1], append(bytes.Clone(raw), 0), append([]byte{2}, raw[1:]...)} {
		if _, err := DecodeColocatedVectorMutationOutcomeV1(bad); err == nil {
			t.Fatal("malformed/version outcome accepted")
		}
	}
	v.Affected = 1
	if _, err := EncodeColocatedVectorMutationOutcomeV1(v); err == nil {
		t.Fatal("changed outcome manufactured zero revision")
	}
	v.Affected, v.AppliedCommandLSN = 0, 0
	if _, err := EncodeColocatedVectorMutationOutcomeV1(v); err == nil {
		t.Fatal("uncovered outcome encoded")
	}
}

func TestCollectionReplaceSourceRejectsNestedColocatedDeleteV2(t *testing.T) {
	v := ColocatedVectorMutationWALV1{Scope: ColocatedVectorMutationScopeV1{Version: 1, Index: "embedding", Generation: 7, OwnerGroup: "owner", Digest: sha256.Sum256([]byte("scope"))}, Collection: "docs", Delete: true, ID: []byte("id"), Attempt: []byte("attempt"), CommandDigest: sha256.Sum256([]byte("command")), Term: 2, Index: 9, Affected: 1}
	deleted, err := EncodeColocatedVectorDeletePayloadV2(v)
	if err != nil {
		t.Fatal(err)
	}
	ordinary, err := EncodeCollectionInsertBatchByIDPayload("docs", []CollectionDocument{{ID: v.ID, Document: []byte(`{"embedding":[1,0]}`)}})
	if err != nil {
		t.Fatal(err)
	}
	typed, err := EncodeCollectionTypedBatchPayload(CollectionTypedBatchPayload{Collection: "docs", SchemaHash: 1, Columns: []CollectionTypedColumn{{Name: "embedding", Type: CollectionTypedFloat32Vector, Dimensions: 2}}, Documents: []CollectionTypedDocument{{ID: v.ID, Values: []CollectionTypedValue{{Vector: []float32{1, 0}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		format PayloadFormat
		insert []byte
		decode func([]byte) error
	}{
		{"legacy", PayloadFormatCollectionReplaceSourceByIDV1, ordinary, func(raw []byte) error { _, err := DecodeCollectionReplaceSourceByIDPayload(raw); return err }},
		{"typed", PayloadFormatCollectionTypedSourceByIDV1, typed, func(raw []byte) error { _, err := DecodeCollectionTypedSourcePayload(raw); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := binary.LittleEndian.AppendUint32(nil, uint32(len(deleted)))
			payload = append(payload, deleted...)
			payload = append(payload, tc.insert...)
			if err := tc.decode(payload); err != ErrCorrupt {
				t.Fatalf("nested scoped metadata discarded: err=%v, want ErrCorrupt", err)
			}
			env := CommandEnvelope{Version: CommandFrameVersion, LSN: 1, Kind: CommandKindCollectionReplaceSourceByID, Scope: CommandScopeCollection, PayloadFormat: tc.format, Payload: payload}
			if err := validateCommandEnvelopePayload(env); err == nil {
				t.Fatal("frame admitted nested scoped delete")
			}
		})
	}
}
