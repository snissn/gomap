package commitlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
)

func compositeTestMembers(t *testing.T) []CompositeMutationMember {
	t.Helper()
	raw, err := EncodeRawKVBatchPayload([]RawKVOperation{{Op: RawKVOpSet, Key: []byte{0, 255, 0}, Value: []byte("raw"), Revision: 23}})
	if err != nil {
		t.Fatal(err)
	}
	jsonRows, err := EncodeCollectionInsertBatchByIDPayload("json", []CollectionDocument{{ID: []byte("row"), Document: []byte(`{"unique":"x"}`)}})
	if err != nil {
		t.Fatal(err)
	}
	typedRows := CollectionTypedBatchPayload{
		Collection: "typed", SchemaHash: 7, Columns: []CollectionTypedColumn{{Name: "name", Type: CollectionTypedString}},
		Documents: []CollectionTypedDocument{{ID: []byte("id"), Values: []CollectionTypedValue{{String: "owned value"}}}},
	}
	typed, err := EncodeCollectionTypedBatchPayload(typedRows)
	if err != nil {
		t.Fatal(err)
	}
	source, err := EncodeCollectionTypedSourcePayload([][]byte{[]byte("old")}, typedRows)
	if err != nil {
		t.Fatal(err)
	}
	deleteOnlySource, err := EncodeCollectionReplaceSourceByIDPayload("typed", [][]byte{[]byte("old")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return []CompositeMutationMember{
		{Kind: CommandKindRawKVBatch, PayloadFormat: PayloadFormatRawKVBatchV2, Payload: raw},
		{Kind: CommandKindCollectionInsertBatchByID, PayloadFormat: PayloadFormatCollectionInsertBatchByIDV1, Incarnation: 12, Payload: jsonRows},
		{Kind: CommandKindCollectionUpdateBatchByID, PayloadFormat: PayloadFormatCollectionTypedBatchByIDV1, Incarnation: 13, Payload: typed},
		{Kind: CommandKindCollectionReplaceSourceByID, PayloadFormat: PayloadFormatCollectionReplaceSourceByIDV1, Incarnation: 13, Payload: deleteOnlySource},
		{Kind: CommandKindCollectionReplaceSourceByID, PayloadFormat: PayloadFormatCollectionTypedSourceByIDV1, Incarnation: 13, Payload: source},
	}
}

func TestCompositeMutationCodecPreservesOrderAndLogicalIdentities(t *testing.T) {
	members := compositeTestMembers(t)
	size, err := PlanCompositeMutationPayload(members)
	if err != nil {
		t.Fatal(err)
	}
	backing := make([]byte, size)
	raw, err := EncodeCompositeMutationPayloadTo(backing[:0], members)
	if err != nil || len(raw) != size || &raw[0] != &backing[0] {
		t.Fatal(size, err)
	}
	decoded, err := DecodeCompositeMutationPayloadView(raw)
	if err != nil || !reflect.DeepEqual(decoded, members) {
		t.Fatal(decoded, err)
	}
	seen := 0
	if err := ScanCompositeMutationPayload(raw, func(member CompositeMutationMember) error {
		if !reflect.DeepEqual(member, members[seen]) {
			t.Fatal("member order/identity changed", seen)
		}
		seen++
		return nil
	}); err != nil || seen != len(members) {
		t.Fatal(seen, err)
	}
	// Encoding owns one independent byte image, while decoding borrows exactly
	// that original image rather than copying a second uncharged payload.
	members[0].Payload[len(members[0].Payload)-1] ^= 1
	if bytes.Equal(decoded[0].Payload, members[0].Payload) {
		t.Fatal("encoder borrowed mutable caller bytes")
	}
	payloadOffset := compositeMutationHeaderBytesV1 + compositeMutationMemberBytesV1
	if &decoded[0].Payload[0] != &raw[payloadOffset] {
		t.Fatal("decoder copied original command backing")
	}
}

func TestCompositeMutationCodecValidatesWholeCommandBeforeVisitor(t *testing.T) {
	raw, err := EncodeCompositeMutationPayloadTo(nil, compositeTestMembers(t))
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < len(raw); n++ {
		calls := 0
		if err := ScanCompositeMutationPayload(raw[:n], func(CompositeMutationMember) error { calls++; return nil }); err == nil || calls != 0 {
			t.Fatalf("truncated command exposed prefix: n=%d calls=%d err=%v", n, calls, err)
		}
	}
	for _, malformed := range [][]byte{
		append(append([]byte(nil), raw...), 0),
		append([]byte(nil), raw...),
	} {
		if len(malformed) == len(raw) {
			malformed[len(malformed)-1] = 255
		} // final typed value is invalid UTF-8
		calls := 0
		if err := ScanCompositeMutationPayload(malformed, func(CompositeMutationMember) error { calls++; return nil }); err == nil || calls != 0 {
			t.Fatal("malformed suffix exposed a valid prefix", calls, err)
		}
		if decoded, err := DecodeCompositeMutationPayloadView(malformed); err == nil || decoded != nil {
			t.Fatal("partial decoded command escaped", err)
		}
	}
	failure := errors.New("original native preparation refused")
	calls := 0
	if err := ScanCompositeMutationPayload(raw, func(CompositeMutationMember) error { calls++; return failure }); err != failure || calls != 1 {
		t.Fatal(calls, err)
	}
}

func TestCompositeMutationCodecRejectsRecursionAndStandaloneControls(t *testing.T) {
	valid := compositeTestMembers(t)
	ordinarySource, err := EncodeCollectionReplaceSourceByIDPayload("typed", [][]byte{[]byte("old")}, []CollectionDocument{{ID: []byte("new"), Document: []byte(`{"x":1}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PlanCompositeMutationPayload([]CompositeMutationMember{{Kind: CommandKindCollectionReplaceSourceByID, PayloadFormat: PayloadFormatCollectionReplaceSourceByIDV1, Incarnation: 13, Payload: ordinarySource}}); !errors.Is(err, ErrCommandWALUnsupportedKind) {
		t.Fatal("ordinary source inserts entered typed aggregate", err)
	}
	for _, kind := range []CommandKind{CommandKindCompositeMutation, CommandKindCatalogCreateCollection,
		CommandKindDurablePrefixBarrier, CommandKindCollectionRebuildVectorIndex,
		CommandKindCollectionPersistPartitionLive, CommandKindCollectionSplitVectorInsertV1, CommandKindCollectionVectorPrepareV1} {
		member := valid[1]
		member.Kind = kind
		if _, err := PlanCompositeMutationPayload([]CompositeMutationMember{member}); !errors.Is(err, ErrCommandWALUnsupportedKind) {
			t.Fatal(kind, err)
		}
	}
	for _, member := range []CompositeMutationMember{
		{Kind: CommandKindRawKVBatch, PayloadFormat: valid[0].PayloadFormat, Incarnation: 1, Payload: valid[0].Payload},
		{Kind: valid[1].Kind, PayloadFormat: valid[1].PayloadFormat, Payload: valid[1].Payload},
		{Kind: valid[1].Kind, PayloadFormat: PayloadFormatCollectionUpdateBatchByIDV1, Incarnation: 1, Payload: valid[1].Payload},
		{Kind: valid[2].Kind, PayloadFormat: PayloadFormatCollectionTypedMetadataByIDV1, Incarnation: 1, Payload: valid[2].Payload},
	} {
		if _, err := PlanCompositeMutationPayload([]CompositeMutationMember{member}); err == nil {
			t.Fatal("unsupported member accepted", member.Kind, member.PayloadFormat)
		}
	}
	dst := bytes.Repeat([]byte{91}, 1024)
	before := append([]byte(nil), dst...)
	if _, err := EncodeCompositeMutationPayloadTo(dst[:0], append(valid, CompositeMutationMember{Kind: CommandKindCompositeMutation})); err == nil || !bytes.Equal(dst, before) {
		t.Fatal("invalid suffix changed output before validation", err)
	}
}

func TestCompositeMutationCodecHasExactFiniteBounds(t *testing.T) {
	valid := compositeTestMembers(t)[0]
	members := make([]CompositeMutationMember, MaxCompositeMutationMembersV1)
	for i := range members {
		members[i] = valid
	}
	raw, err := EncodeCompositeMutationPayloadTo(nil, members)
	if err != nil {
		t.Fatal(err)
	}
	if decoded, err := DecodeCompositeMutationPayloadView(raw); err != nil || len(decoded) != len(members) {
		t.Fatal(len(decoded), err)
	}
	if _, err := PlanCompositeMutationPayload(append(members, valid)); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatal(err)
	}
	if _, err := PlanCompositeMutationPayload(nil); err == nil {
		t.Fatal("zero-member command accepted")
	}
	binary.LittleEndian.PutUint16(raw[2:4], uint16(MaxCompositeMutationMembersV1+1))
	if _, err := DecodeCompositeMutationPayloadView(raw); err == nil {
		t.Fatal("over-limit wire count accepted")
	}
	one, err := EncodeRawKVBatchPayload([]RawKVOperation{{Op: RawKVOpSet, Key: []byte("k"), Value: []byte("x")}})
	if err != nil {
		t.Fatal(err)
	}
	valueBytes := MaxCompositeMutationPayloadBytesV1 - compositeMutationHeaderBytesV1 - compositeMutationMemberBytesV1 - (len(one) - 1)
	valid.Payload, err = EncodeRawKVBatchPayload([]RawKVOperation{{Op: RawKVOpSet, Key: []byte("k"), Value: bytes.Repeat([]byte("x"), valueBytes)}})
	if err != nil {
		t.Fatal(err)
	}
	if size, err := PlanCompositeMutationPayload([]CompositeMutationMember{valid}); err != nil || size != MaxCompositeMutationPayloadBytesV1 {
		t.Fatal(size, err)
	}
	valid.Payload = append(valid.Payload, 0)
	if _, err := PlanCompositeMutationPayload([]CompositeMutationMember{valid}); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatal(err)
	}
}

func TestCompositeMutationValidationUsesNoDecodedRowAllocation(t *testing.T) {
	members := compositeTestMembers(t)
	raw, err := EncodeCompositeMutationPayloadTo(nil, members)
	if err != nil {
		t.Fatal(err)
	}
	var planned int
	var failure error
	if allocations := testing.AllocsPerRun(20, func() { planned, failure = PlanCompositeMutationPayload(members) }); allocations != 0 || failure != nil || planned != len(raw) {
		t.Fatal("planning allocated decoded child state", allocations, planned, failure)
	}
	if allocations := testing.AllocsPerRun(20, func() { failure = ScanCompositeMutationPayload(raw, nil) }); allocations != 0 || failure != nil {
		t.Fatal("whole validation allocated decoded child state", allocations, failure)
	}
	// Ordinary row format IDs cannot smuggle their standalone colocated V2
	// attempt/outcome protocol into a user composite.
	for _, kind := range []CommandKind{CommandKindCollectionUpdateBatchByID, CommandKindCollectionDeleteBatchByID} {
		member := members[1]
		member.Kind = kind
		member.Payload = append([]byte(nil), member.Payload...)
		binary.LittleEndian.PutUint16(member.Payload[:2], 2)
		if kind == CommandKindCollectionUpdateBatchByID {
			member.PayloadFormat = PayloadFormatCollectionUpdateBatchByIDV1
		} else {
			member.PayloadFormat = PayloadFormatCollectionDeleteBatchByIDV1
		}
		if _, err := PlanCompositeMutationPayload([]CompositeMutationMember{member}); !errors.Is(err, ErrCommandWALUnsupportedVersion) {
			t.Fatal(kind, err)
		}
	}
}
