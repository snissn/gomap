package commitlog

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

func TestCollectionReplaceSourceByIDPayloadRoundTrip(t *testing.T) {
	deleteIDs := [][]byte{[]byte("src#1"), []byte("src"), []byte("src#0")}
	documents := []CollectionDocument{
		{ID: []byte("src#1"), Document: []byte(`{"body":"second"}`)},
		{ID: []byte("src"), Document: []byte(`{"body":"parent"}`)},
		{ID: []byte("src#0"), Document: []byte(`{"body":"first"}`)},
	}
	encoded, err := EncodeCollectionReplaceSourceByIDPayload("docs", deleteIDs, documents)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	frame, err := EncodeCommandFrame(CommandEnvelope{
		Version: CommandFrameVersion, LSN: 1,
		Kind: CommandKindCollectionReplaceSourceByID, Scope: CommandScopeCollection,
		PayloadFormat: PayloadFormatCollectionReplaceSourceByIDV1, Payload: encoded,
	})
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	envelope, err := DecodeCommandFrame(frame)
	if err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	if envelope.Kind != CommandKindCollectionReplaceSourceByID || envelope.PayloadFormat != PayloadFormatCollectionReplaceSourceByIDV1 {
		t.Fatalf("frame identity=%+v", envelope)
	}
	decoded, err := DecodeCollectionReplaceSourceByIDPayload(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Collection != "docs" {
		t.Fatalf("collection=%q want docs", decoded.Collection)
	}
	wantIDs := [][]byte{[]byte("src"), []byte("src#0"), []byte("src#1")}
	if !reflect.DeepEqual(decoded.DeleteIDs, wantIDs) {
		t.Fatalf("delete ids=%q want %q", decoded.DeleteIDs, wantIDs)
	}
	if len(decoded.Documents) != len(documents) {
		t.Fatalf("documents=%d want %d", len(decoded.Documents), len(documents))
	}
	for i, wantID := range wantIDs {
		if !bytes.Equal(decoded.Documents[i].ID, wantID) {
			t.Fatalf("document[%d] id=%q want %q", i, decoded.Documents[i].ID, wantID)
		}
	}
}

func TestCollectionReplaceSourceByIDPayloadRejectsMismatchedCollections(t *testing.T) {
	deleted, err := EncodeCollectionDeleteBatchByIDPayload("left", [][]byte{[]byte("src")})
	if err != nil {
		t.Fatalf("encode delete: %v", err)
	}
	inserted, err := EncodeCollectionInsertBatchByIDPayload("right", []CollectionDocument{{ID: []byte("src"), Document: []byte("{}")}})
	if err != nil {
		t.Fatalf("encode insert: %v", err)
	}
	payload := make([]byte, 4+len(deleted)+len(inserted))
	payload[0] = byte(len(deleted))
	copy(payload[4:], deleted)
	copy(payload[4+len(deleted):], inserted)
	if _, err := DecodeCollectionReplaceSourceByIDPayload(payload); !errorsIsCorrupt(err) {
		t.Fatalf("decode mismatch err=%v want ErrCorrupt", err)
	}
}

func TestCollectionReplaceSourceByIDPayloadRejectsOversizedDeleteLength(t *testing.T) {
	payload := make([]byte, 4)
	binary.LittleEndian.PutUint32(payload, ^uint32(0))
	if _, err := DecodeCollectionReplaceSourceByIDPayload(payload); !errorsIsCorrupt(err) {
		t.Fatalf("decode oversized delete length err=%v want ErrCorrupt", err)
	}
}

func errorsIsCorrupt(err error) bool {
	return err == ErrCorrupt
}
