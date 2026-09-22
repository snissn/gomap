package commitlog

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestCollectionSourceImportPayloadV2(t *testing.T) {
	p := CollectionSourceImportPayloadV2{Metadata: []byte(`{"Version":2}`), Inserted: CollectionTypedBatchPayload{Collection: "docs", SchemaHash: 1, Columns: []CollectionTypedColumn{{Name: "embedding", Type: CollectionTypedFloat32Vector, Dimensions: 2}}, Documents: []CollectionTypedDocument{{ID: []byte("a"), Retained: []byte(`{"id":"a"}`), Values: []CollectionTypedValue{{Vector: []float32{1, 0}}}}}}}
	raw, err := EncodeCollectionSourceImportPayloadV2(p)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCollectionSourceImportPayloadV2(raw)
	if err != nil || !bytes.Equal(decoded.Metadata, p.Metadata) || decoded.Inserted.Documents[0].Values[0].Vector[0] != 1 {
		t.Fatalf("round trip: %+v %v", decoded, err)
	}
	env := CommandEnvelope{Version: CommandFrameVersion, LSN: 1, Kind: CommandKindCollectionReplaceSourceByID, Scope: CommandScopeCollection, PayloadFormat: PayloadFormatCollectionSourceImportV2, Payload: raw}
	if allocations := testing.AllocsPerRun(100, func() {
		if err := validateCommandEnvelopePayload(env); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("frame validator allocates: %g", allocations)
	}
	frame, err := EncodeCommandFrame(env)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeCommandFrame(frame); err != nil {
		t.Fatal(err)
	}
	for i := range raw {
		if _, err := DecodeCollectionSourceImportPayloadV2(raw[:i]); err == nil {
			t.Fatalf("accepted truncation %d", i)
		}
	}
	for _, mode := range []string{"length", "trailing", "legacy"} {
		bad := bytes.Clone(raw)
		switch mode {
		case "length":
			binary.LittleEndian.PutUint32(bad, ^uint32(0))
		case "trailing":
			bad = append(bad, 0)
		case "legacy":
			bad[4+len(p.Metadata)+collectionTypedBatchFlagsOffset] = 1
		}
		if err := validateCollectionSourceImportPayloadV2(bad); err == nil {
			t.Fatalf("accepted %s", mode)
		}
	}
	clear(raw)
	if !bytes.Equal(decoded.Metadata, p.Metadata) {
		t.Fatal("metadata borrows encoded bytes")
	}
}

func TestCollectionSourceImportCompleteCommandBudgetV2(t *testing.T) {
	input := CollectionSourceImportPayloadV2{Metadata: []byte(`{"Version":2}`), Inserted: CollectionTypedBatchPayload{Collection: "docs", SchemaHash: 1, Columns: []CollectionTypedColumn{{Name: "embedding", Type: CollectionTypedFloat32Vector, Dimensions: 2}}, Documents: []CollectionTypedDocument{{ID: []byte("a"), Retained: []byte(`{}`), Values: []CollectionTypedValue{{Vector: []float32{1, 0}}}}}}}
	raw, err := EncodeCollectionSourceImportPayloadV2(input)
	if err != nil {
		t.Fatal(err)
	}
	input.Inserted.Documents[0].Retained = bytes.Repeat([]byte("x"), MaxSourceImportPayloadBytesV2-len(raw)+2)
	raw, err = EncodeCollectionSourceImportPayloadV2(input)
	if err != nil || len(raw) != MaxSourceImportPayloadBytesV2 {
		t.Fatalf("exact payload boundary: %d %v", len(raw), err)
	}
	frame, err := EncodeCommandFrame(CommandEnvelope{Version: CommandFrameVersion, LSN: 1, Kind: CommandKindCollectionReplaceSourceByID, Scope: CommandScopeCollection, PayloadFormat: PayloadFormatCollectionSourceImportV2, Payload: raw})
	if err != nil || len(frame) != MaxSourceImportCommandBytesV2 {
		t.Fatalf("complete frame budget: %d %v", len(frame), err)
	}
	input.Inserted.Documents[0].Retained = append(input.Inserted.Documents[0].Retained, 'x')
	if _, err := EncodeCollectionSourceImportPayloadV2(input); err != ErrRecordTooLarge {
		t.Fatalf("oversized complete command: %v", err)
	}
	input.Metadata = bytes.Repeat([]byte("x"), MaxSourceImportMetadataBytesV2+1)
	if _, err := EncodeCollectionSourceImportPayloadV2(input); err != ErrRecordTooLarge {
		t.Fatalf("oversized metadata: %v", err)
	}
}
