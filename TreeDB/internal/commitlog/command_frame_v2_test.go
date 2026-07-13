package commitlog

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
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

func TestCommandFrameV2RawKVWithoutSetRIDHasNoFence(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{{Op: RawKVOpSet, Key: []byte("k"), Value: []byte("v")}})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := EncodeCommandFrameV2(CommandEnvelope{
		DurabilityClass: CommandDurabilityRelaxed,
		LSN:             1,
		Kind:            CommandKindRawKVBatch,
		Scope:           CommandScopeRawKV,
		PayloadFormat:   PayloadFormatRawKVBatchV1,
		Payload:         payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	env, err := DecodeCommandFrameV2(frame)
	if err != nil {
		t.Fatal(err)
	}
	if fence, present, err := RawKVExternalRefFenceV1(env); err != nil || present || fence != (ExternalRefFenceV1{}) {
		t.Fatalf("fence=%+v present=%t err=%v, want absent", fence, present, err)
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

func TestCommandFrameV2RejectsCriticalFeatureFlagsBeforeDestinationMutation(t *testing.T) {
	backing := make([]byte, 512)
	for i := range backing {
		backing[i] = byte(i)
	}
	want := append([]byte(nil), backing...)
	if _, err := EncodeCommandFrameV2To(backing[:0], CommandEnvelope{
		DurabilityClass: CommandDurabilityRelaxed,
		FeatureFlags:    1,
		LSN:             1,
		Kind:            CommandKindRawKVBatch,
		Scope:           CommandScopeRawKV,
		PayloadFormat:   PayloadFormatRawKVBatchV1,
	}); !errors.Is(err, ErrCommandWALUnsupportedCriticalFlag) {
		t.Fatalf("critical feature flag error=%v, want ErrCommandWALUnsupportedCriticalFlag", err)
	}
	if !reflect.DeepEqual(backing, want) {
		t.Fatal("critical feature flag mutated destination backing storage")
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

func TestCommandFrameV2RejectsUnknownVersionClassAndFenceCardinality(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{{Op: RawKVOpSetRID, Key: []byte("k"), RID: 11}})
	if err != nil {
		t.Fatal(err)
	}
	valid, err := EncodeCommandFrameV2(CommandEnvelope{
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

	unknownVersion := append([]byte(nil), valid...)
	binary.LittleEndian.PutUint16(unknownVersion[4:6], 99)
	binary.LittleEndian.PutUint16(unknownVersion[6:8], 99)
	if _, err := DecodeCommandFrameV2(unknownVersion); !errors.Is(err, ErrCommandWALUnsupportedVersion) {
		t.Fatalf("unknown version error=%v, want ErrCommandWALUnsupportedVersion", err)
	}
	for _, class := range []uint16{0, 99} {
		unknownClass := append([]byte(nil), valid...)
		binary.LittleEndian.PutUint16(unknownClass[54:56], class)
		if _, err := DecodeCommandFrameV2(unknownClass); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("class=%d error=%v, want ErrCorrupt", class, err)
		}
	}

	payloadEnd := commandFrameHeaderSize + int(binary.LittleEndian.Uint32(valid[56:60]))
	missingFence := append([]byte(nil), valid[:payloadEnd]...)
	binary.LittleEndian.PutUint32(missingFence[64:68], 0)
	if _, err := DecodeCommandFrameV2(missingFence); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("missing fence error=%v, want ErrCorrupt", err)
	}

	section := valid[payloadEnd:]
	if got := binary.LittleEndian.Uint32(section[:4]); got != 1 {
		t.Fatalf("encoded fence extension count=%d, want 1", got)
	}
	duplicateFence := make([]byte, payloadEnd+4+2*(len(section)-4))
	copy(duplicateFence, valid[:payloadEnd])
	binary.LittleEndian.PutUint32(duplicateFence[payloadEnd:payloadEnd+4], 2)
	copy(duplicateFence[payloadEnd+4:], section[4:])
	copy(duplicateFence[payloadEnd+len(section):], section[4:])
	binary.LittleEndian.PutUint32(duplicateFence[64:68], uint32(len(duplicateFence)-payloadEnd))
	if _, err := DecodeCommandFrameV2(duplicateFence); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("duplicate fence error=%v, want ErrCorrupt", err)
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

	dir := t.TempDir()
	journal, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.AppendCommand(CommandEnvelope{
		Version:         CommandFrameVersionV2,
		DurabilityClass: CommandDurabilityDurable,
		Kind:            CommandKindRawKVBatch,
		Scope:           CommandScopeRawKV,
		PayloadFormat:   PayloadFormatRawKVBatchV1,
	}); !errors.Is(err, ErrCommandWALUnsupportedVersion) {
		t.Fatalf("production V2 append error=%v, want ErrCommandWALUnsupportedVersion", err)
	}
	if lsn, err := journal.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil || lsn != 1 {
		t.Fatalf("production V1 append lsn=%d err=%v", lsn, err)
	}
	path, _ := journal.ActiveSegmentSnapshot()
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	reopen, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if lsn, err := reopen.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil || lsn != 2 {
		t.Fatalf("production V1 append after reopen lsn=%d err=%v", lsn, err)
	}
	if err := reopen.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for wantLSN := uint64(1); wantLSN <= 2; wantLSN++ {
		got, err := reader.ReadCommandFrame()
		if err != nil {
			t.Fatalf("read production frame %d: %v", wantLSN, err)
		}
		if got.Version != CommandFrameVersion || got.LSN != wantLSN {
			t.Fatalf("production frame=%+v, want V1 lsn=%d", got, wantLSN)
		}
	}
	if _, err := reader.ReadCommandFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("read after production frames error=%v, want EOF", err)
	}
}

func TestCommandWALFormatGoldenV2RawKVExternalRefFence(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSetRID, Key: []byte("nine-a"), RID: 9},
		{Op: RawKVOpSet, Key: []byte("inline"), Value: []byte("value")},
		{Op: RawKVOpSetRID, Key: []byte("three"), RID: 3},
		{Op: RawKVOpSetRID, Key: []byte("nine-b"), RID: 9},
	})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := EncodeCommandFrameV2(CommandEnvelope{
		DurabilityClass: CommandDurabilityDurable,
		LSN:             7,
		BaseAppliedLSN:  3,
		Kind:            CommandKindRawKVBatch,
		Scope:           CommandScopeRawKV,
		PayloadFormat:   PayloadFormatRawKVBatchV1,
		Payload:         payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertGoldenHex(t, "command_wal_v2_raw_kv_external_ref_fence.hex", frame)
}

func TestCommandWALFormatGoldenV2DurablePrefixBarrier(t *testing.T) {
	frame, err := EncodeCommandFrameV2(NewDurablePrefixBarrierV1(8, 7))
	if err != nil {
		t.Fatal(err)
	}
	assertGoldenHex(t, "command_wal_v2_durable_prefix_barrier.hex", frame)
}
