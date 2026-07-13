package commitlog

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"testing"
)

func TestCommandFrameV2ClassFenceAndBarrier(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSetRID, Key: []byte("nine-a"), RID: 9},
		{Op: RawKVOpSetRID, Key: []byte("three"), RID: 3},
		{Op: RawKVOpSetRID, Key: []byte("nine-b"), RID: 9},
	})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := EncodeCommandFrameV2(CommandEnvelope{
		DurabilityClass: CommandDurabilityDurable,
		LSN:             7,
		Kind:            CommandKindRawKVBatch,
		Scope:           CommandScopeRawKV,
		PayloadFormat:   PayloadFormatRawKVBatchV1,
		Payload:         payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint16(frame[4:6]); got != CommandFrameVersionV2 {
		t.Fatalf("version=%d, want %d", got, CommandFrameVersionV2)
	}
	if got := binary.LittleEndian.Uint16(frame[54:56]); got != uint16(CommandDurabilityDurable) {
		t.Fatalf("durability class=%d, want %d", got, CommandDurabilityDurable)
	}

	env, err := DecodeCommandFrameV2(frame)
	if err != nil {
		t.Fatal(err)
	}
	fence, present, err := RawKVExternalRefFenceV1(env)
	if err != nil || !present {
		t.Fatalf("fence present=%v err=%v", present, err)
	}
	var canonical [16]byte
	binary.LittleEndian.PutUint64(canonical[0:8], 3)
	binary.LittleEndian.PutUint64(canonical[8:16], 9)
	if want := sha256.Sum256(canonical[:]); fence.Count != 2 || fence.Digest != want {
		t.Fatalf("fence=%+v, want count=2 digest=%x", fence, want)
	}

	barrier, err := EncodeCommandFrameV2(NewDurablePrefixBarrierV1(8, 7))
	if err != nil {
		t.Fatal(err)
	}
	barrierEnv, err := DecodeCommandFrameV2(barrier)
	if err != nil {
		t.Fatal(err)
	}
	if barrierEnv.Kind != CommandKindDurablePrefixBarrier || barrierEnv.DurabilityClass != CommandDurabilityDurable || len(barrierEnv.Payload) != 0 {
		t.Fatalf("decoded barrier=%+v", barrierEnv)
	}
}

func TestCommandFrameV2RejectsInvalidClassBeforeDestinationMutation(t *testing.T) {
	dst := []byte("caller-owned-buffer")
	want := append([]byte(nil), dst...)
	for _, class := range []CommandDurabilityClass{0, 99} {
		if _, err := EncodeCommandFrameV2To(dst[:0], CommandEnvelope{
			DurabilityClass: class,
			LSN:             1,
			Kind:            CommandKindRawKVBatch,
			Scope:           CommandScopeRawKV,
			PayloadFormat:   PayloadFormatRawKVBatchV1,
		}); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("class=%d error=%v, want ErrCorrupt", class, err)
		}
		if string(dst) != string(want) {
			t.Fatalf("class=%d mutated destination: got %q want %q", class, dst, want)
		}
	}
}

func TestCommandFrameV2RejectsV1AndFenceCorruption(t *testing.T) {
	v1, err := EncodeCommandFrame(CommandEnvelope{
		LSN:           1,
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeCommandFrameV2(v1); !errors.Is(err, ErrCommandWALV1RebuildRequired) {
		t.Fatalf("V1 decode error=%v, want ErrCommandWALV1RebuildRequired", err)
	}

	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{{Op: RawKVOpSetRID, Key: []byte("k"), RID: 11}})
	if err != nil {
		t.Fatal(err)
	}
	v2, err := EncodeCommandFrameV2(CommandEnvelope{
		DurabilityClass: CommandDurabilityRelaxed,
		LSN:             2,
		Kind:            CommandKindRawKVBatch,
		Scope:           CommandScopeRawKV,
		PayloadFormat:   PayloadFormatRawKVBatchV1,
		Payload:         payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	v2[len(v2)-1] ^= 1
	if _, err := DecodeCommandFrameV2(v2); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("fence mismatch error=%v, want ErrCorrupt", err)
	}
}

func TestCommandFrameV2ProductionActivationGuard(t *testing.T) {
	frame, err := EncodeCommandFrame(CommandEnvelope{
		LSN:           1,
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint16(frame[4:6]); got != CommandFrameVersion {
		t.Fatalf("production encoder version=%d, want current V1 version=%d", got, CommandFrameVersion)
	}
	if CommandFrameVersion != 1 {
		t.Fatalf("production CommandFrameVersion=%d, V2 activation belongs to #3718", CommandFrameVersion)
	}
}
