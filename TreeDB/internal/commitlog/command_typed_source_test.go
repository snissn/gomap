package commitlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestCollectionTypedSourcePayload(t *testing.T) {
	p := CollectionTypedBatchPayload{Collection: "docs", SchemaHash: 1, Columns: []CollectionTypedColumn{{Name: "embedding", Type: CollectionTypedFloat32Vector, Dimensions: 2}}, Documents: []CollectionTypedDocument{{ID: []byte("a"), Retained: []byte(`{"id":"a"}`), Values: []CollectionTypedValue{{Vector: []float32{1, 0}}}}}}
	raw, err := EncodeCollectionTypedSourcePayload([][]byte{[]byte("z"), []byte("a")}, p)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCollectionTypedSourcePayload(raw)
	if err != nil || string(decoded.DeleteIDs[0]) != "a" || decoded.Inserted.Documents[0].Values[0].Vector[0] != 1 {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
	env := CommandEnvelope{Version: CommandFrameVersion, LSN: 1, Kind: CommandKindCollectionReplaceSourceByID, Scope: CommandScopeCollection, PayloadFormat: PayloadFormatCollectionTypedSourceByIDV1, Payload: raw}
	if n := testing.AllocsPerRun(100, func() {
		if err := validateCommandEnvelopePayload(env); err != nil {
			panic(err)
		}
	}); n != 0 {
		t.Fatalf("frame validation allocated %g times", n)
	}
	frame, err := EncodeCommandFrame(env)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeCommandFrame(frame); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(raw); i++ {
		if _, err := DecodeCollectionTypedSourcePayload(raw[:i]); err == nil {
			t.Fatalf("accepted truncation %d", i)
		}
		if err := validateCollectionTypedSourcePayload(raw[:i]); err == nil {
			t.Fatalf("validated truncation %d", i)
		}
	}
	start := 4 + int(binary.LittleEndian.Uint32(raw))
	for _, mode := range []string{"length", "version", "legacy", "collection", "trailing"} {
		t.Run(mode, func(t *testing.T) {
			bad := bytes.Clone(raw)
			want := ErrCorrupt
			switch mode {
			case "length":
				binary.LittleEndian.PutUint32(bad, ^uint32(0))
			case "version":
				binary.LittleEndian.PutUint16(bad[start:], 2)
				want = ErrCommandWALUnsupportedVersion
			case "legacy":
				bad[start+collectionTypedBatchFlagsOffset] = 1
			case "collection":
				bad[start+collectionTypedBatchHeaderSize] = 'x'
			case "trailing":
				bad = append(bad, 0)
			}
			env.Payload = bad
			if err := validateCommandEnvelopePayload(env); !errors.Is(err, want) {
				t.Fatalf("validation=%v want=%v", err, want)
			}
		})
	}
	p.LegacyProjection = true
	if _, err := EncodeCollectionTypedSourcePayload(nil, p); err == nil {
		t.Fatal("accepted legacy projection source")
	}
}
