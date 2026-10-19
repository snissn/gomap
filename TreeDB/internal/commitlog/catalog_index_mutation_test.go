package commitlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
)

func TestCatalogIndexMutationPayloadAndFrame(t *testing.T) {
	want := CatalogIndexMutationPayload{Collection: "users", Incarnation: 7, BeforeMetadata: []byte(`{"name":"users"}`), Metadata: []byte(`{"name":"users","indexes":[{"name":"city"}]}`)}
	raw, err := EncodeCatalogIndexMutationPayload(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeCatalogIndexMutationPayload(raw)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	frame, err := EncodeCommandFrameV2(CommandEnvelope{LSN: 11, DurabilityClass: CommandDurabilityDurable, Kind: CommandKindCatalogIndexMutation, Scope: CommandScopeCatalog, PayloadFormat: PayloadFormatCatalogIndexMutationV1, Payload: raw})
	if err != nil {
		t.Fatal(err)
	}
	env, err := DecodeCommandFrameV2(frame)
	if err != nil || env.Kind != CommandKindCatalogIndexMutation || !bytes.Equal(env.Payload, raw) {
		t.Fatalf("frame: %+v %v", env, err)
	}
	// Kind 200 remains create-only; the new payload cannot be relabeled as a
	// create command or attached to an ordinary collection/raw frame.
	for _, change := range []func(*CommandEnvelope){func(e *CommandEnvelope) { e.Kind = CommandKindCatalogCreateCollection }, func(e *CommandEnvelope) { e.Scope = CommandScopeCollection }, func(e *CommandEnvelope) { e.PayloadFormat = PayloadFormatCatalogCreateCollectionV1 }} {
		bad := env
		change(&bad)
		if _, err := EncodeCommandFrameV2(bad); err == nil {
			t.Fatal("accepted mismatched command identity")
		}
	}
	for i := range raw {
		raw[i] = 0
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("decoded metadata aliases frame input")
	}
}

func TestCatalogIndexMutationRejectsMalformedPayload(t *testing.T) {
	raw, err := EncodeCatalogIndexMutationPayload(CatalogIndexMutationPayload{Collection: "c", Incarnation: 1, BeforeMetadata: []byte(`{}`), Metadata: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < len(raw); n++ {
		if _, err := DecodeCatalogIndexMutationPayload(raw[:n]); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("prefix %d: %v", n, err)
		}
	}
	for _, corrupt := range []func([]byte){
		func(b []byte) { binary.LittleEndian.PutUint16(b, 2) },
		func(b []byte) { binary.LittleEndian.PutUint64(b[2:], 0) },
		func(b []byte) { binary.LittleEndian.PutUint32(b[10:], 0) },
		func(b []byte) { binary.LittleEndian.PutUint32(b[14:], ^uint32(0)) },
		func(b []byte) { binary.LittleEndian.PutUint32(b[18:], 0) },
	} {
		bad := bytes.Clone(raw)
		corrupt(bad)
		if _, err := DecodeCatalogIndexMutationPayload(bad); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("corrupt header: %v", err)
		}
	}
	if _, err := DecodeCatalogIndexMutationPayload(append(bytes.Clone(raw), 0)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("trailing bytes: %v", err)
	}
}
