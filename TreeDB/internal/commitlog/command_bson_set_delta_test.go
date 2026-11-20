package commitlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"testing"
)

func TestCommandFrameV2BSONSetDeltaCriticalFlag(t *testing.T) {
	delta := CollectionBSONSetDelta{
		ID: []byte{0x10, 0x29, 0, 0, 0}, Field: []byte("binary"),
		Value: []byte{1, 0, 0, 0, 0, 1}, Incarnation: 7,
		Before: sha256.Sum256([]byte("before")), After: sha256.Sum256([]byte("after")),
	}
	payload, err := EncodeCollectionBSONSetDelta("db.coll", delta)
	if err != nil {
		t.Fatal(err)
	}
	env := CommandEnvelope{
		Kind: CommandKindCollectionUpdateBatchByID, Scope: CommandScopeCollection,
		PayloadFormat: PayloadFormatCollectionUpdateBatchByIDV1, Payload: payload,
		LSN: 3, BaseAppliedLSN: 2, DurabilityClass: CommandDurabilityRelaxed,
	}
	raw, err := EncodeCommandFrameV2(env)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCommandFrameV2(raw)
	if err != nil || !decoded.UsesBSONSetWALDelta() || !bytes.Equal(decoded.Payload, payload) {
		t.Fatalf("delta envelope=%+v err=%v", decoded, err)
	}
	if _, err := DecodeCollectionUpdateBatchByIDPayload(payload); !errors.Is(err, ErrCommandWALUnsupportedVersion) {
		t.Fatalf("ordinary payload decoder accepted the delta dialect: %v", err)
	}
	// A terminal header keeps the critical bit even without any body bytes.
	head, err := decodeCommandFrameV2TailIdentity(raw[:56])
	if err != nil || !head.UsesBSONSetWALDelta() || head.LSN != env.LSN {
		t.Fatalf("delta terminal header=%+v err=%v", head, err)
	}
	unflagged := bytes.Clone(raw)
	binary.LittleEndian.PutUint64(unflagged[12:20], 0)
	if _, err := DecodeCommandFrameV2(unflagged); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("version-3 body without the critical flag: %v", err)
	}
	flaggedBarrier := NewDurablePrefixBarrierV1(2, 1)
	flaggedBarrier.FeatureFlags = commandWALBSONSetDeltaFlag
	if _, err := EncodeCommandFrameV2(flaggedBarrier); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("non-update critical flag accepted: %v", err)
	}
	for end := range len(payload) {
		if _, _, err := DecodeCollectionBSONSetDelta(payload[:end]); err == nil {
			t.Fatalf("delta decoder accepted a %d-byte partial payload", end)
		}
	}
	for name, mutate := range map[string]func(*CollectionBSONSetDelta){
		"id field":    func(d *CollectionBSONSetDelta) { d.Field = []byte("_id") },
		"nested path": func(d *CollectionBSONSetDelta) { d.Field = []byte("a.b") },
		"zero id":     func(d *CollectionBSONSetDelta) { d.Incarnation = 0 },
		"wrong type":  func(d *CollectionBSONSetDelta) { d.Value = []byte{1, 0, 0, 0, 2, 1} },
		"bad length":  func(d *CollectionBSONSetDelta) { d.Value = []byte{3, 0, 0, 0, 0, 1} },
		"no change":   func(d *CollectionBSONSetDelta) { d.After = d.Before },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := delta
			mutate(&invalid)
			if _, err := EncodeCollectionBSONSetDelta("db.coll", invalid); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("invalid delta accepted: %v", err)
			}
		})
	}
}
