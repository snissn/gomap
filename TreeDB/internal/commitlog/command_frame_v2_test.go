package commitlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
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

func TestCommandFrameV2RejectsDormantExternalRefsBeforeDestinationMutation(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload(nil)
	if err != nil {
		t.Fatal(err)
	}
	classes := []ExternalRefClass{
		ExternalRefValueLog,
		ExternalRefLeafLog,
		ExternalRefPayloadFile,
		0,
		ExternalRefClass(99),
	}
	for _, class := range classes {
		t.Run(fmt.Sprint(class), func(t *testing.T) {
			ref := ExternalRef{Class: class, FileID: 41, Offset: 128, Length: 512}
			backing := make([]byte, 512)
			for i := range backing {
				backing[i] = byte(i)
			}
			want := append([]byte(nil), backing...)
			_, err := EncodeCommandFrameV2To(backing[:0], CommandEnvelope{
				DurabilityClass: CommandDurabilityRelaxed,
				LSN:             4,
				Kind:            CommandKindRawKVBatch,
				Scope:           CommandScopeRawKV,
				PayloadFormat:   PayloadFormatRawKVBatchV1,
				ExternalRefs:    []ExternalRef{ref},
			})
			if !errors.Is(err, ErrCommandWALUnsupportedExternalRef) {
				t.Fatalf("EncodeCommandFrameV2To class=%d error=%v, want ErrCommandWALUnsupportedExternalRef", class, err)
			}
			if !reflect.DeepEqual(backing, want) {
				t.Fatalf("class=%d mutated destination backing storage", class)
			}

			extRefs, err := encodeExternalRefs([]ExternalRef{ref})
			if err != nil {
				t.Fatal(err)
			}
			frame := mustCommandFrameV2WithExternalRefs(t, payload, extRefs)
			if _, err := DecodeCommandFrameV2(frame); !errors.Is(err, ErrCommandWALUnsupportedExternalRef) {
				t.Fatalf("DecodeCommandFrameV2 class=%d error=%v, want ErrCommandWALUnsupportedExternalRef", class, err)
			}
		})
	}
}

func TestCommandFrameV2EncodedSizeMatchesCanonicalEncoding(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSetRID, Key: []byte("rid"), RID: 42},
	})
	if err != nil {
		t.Fatal(err)
	}
	env := CommandEnvelope{
		DurabilityClass: CommandDurabilityDurable,
		LSN:             7,
		Kind:            CommandKindRawKVBatch,
		Scope:           CommandScopeRawKV,
		PayloadFormat:   PayloadFormatRawKVBatchV1,
		Payload:         payload,
	}
	size, err := commandFrameV2EncodedSize(env)
	if err != nil {
		t.Fatalf("commandFrameV2EncodedSize: %v", err)
	}
	encoded, err := EncodeCommandFrameV2(env)
	if err != nil {
		t.Fatalf("EncodeCommandFrameV2: %v", err)
	}
	if size != len(encoded) {
		t.Fatalf("encoded size=%d, want %d", size, len(encoded))
	}
}

func mustCommandFrameV2WithExternalRefs(t *testing.T, payload, extRefs []byte) []byte {
	t.Helper()
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
	out := make([]byte, len(frame)+len(extRefs))
	copy(out, frame)
	binary.LittleEndian.PutUint32(out[60:64], uint32(len(extRefs)))
	copy(out[len(frame):], extRefs)
	return out
}

func TestCommandFrameV2RejectsCompressedSegmentStorage(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{{
		Op:    RawKVOpSet,
		Key:   []byte("compressible"),
		Value: bytes.Repeat([]byte("v2-compression-must-stay-disabled"), 4096),
	}})
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

	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriterWithOptions(path, Options{Compress: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.writeSegment(frame); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint32(data[:4]); got&segmentFlagCompressed == 0 {
		t.Fatalf("segment length field=%#x, want compressed flag", got)
	}

	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadCommandFrameV2(); !errors.Is(err, ErrCommandWALV2CompressedRecordUnsupported) {
		_ = r.Close()
		t.Fatalf("ReadCommandFrameV2 error=%v, want ErrCommandWALV2CompressedRecordUnsupported", err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.Truncate(path, int64(len(data)-1)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := InspectCommandFrameV2TerminalTail(path, 0); !errors.Is(err, ErrCommandWALV2CompressedRecordUnsupported) {
		t.Fatalf("InspectCommandFrameV2TerminalTail error=%v, want ErrCommandWALV2CompressedRecordUnsupported", err)
	}
}

func TestCommandJournalV2CompressionOptionWritesStrictlyReopenableRawFrames(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{{
		Op:    RawKVOpSet,
		Key:   []byte("compressible"),
		Value: bytes.Repeat([]byte("v2-command-frame-must-remain-raw"), 4096),
	}})
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	journal, err := OpenCommandJournal(dir, CommandJournalOptions{Compress: true})
	if err != nil {
		t.Fatal(err)
	}
	lsn, err := journal.AppendCommand(CommandEnvelope{
		Version:         CommandFrameVersionV2,
		DurabilityClass: CommandDurabilityRelaxed,
		Kind:            CommandKindRawKVBatch,
		Scope:           CommandScopeRawKV,
		PayloadFormat:   PayloadFormatRawKVBatchV1,
		Payload:         payload,
	})
	if err != nil {
		_ = journal.Close()
		t.Fatal(err)
	}
	path, _ := journal.ActiveSegmentSnapshot()
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint32(data[:4]); got&segmentFlagCompressed != 0 {
		t.Fatalf("segment length field=%#x, V2 command frame must remain uncompressed", got)
	}
	frames, err := ScanCommandFramesV2(path, Options{})
	if err != nil {
		t.Fatalf("strict V2 reopen: %v", err)
	}
	if len(frames) != 1 || frames[0].LSN != lsn || !bytes.Equal(frames[0].Payload, payload) {
		t.Fatalf("strict V2 frames=%+v, want one frame at LSN %d with original payload", frames, lsn)
	}
}

func TestInspectCommandFrameV2TerminalTailRequiresCompleteLSNAndClass(t *testing.T) {
	frame, err := EncodeCommandFrameV2(CommandEnvelope{
		DurabilityClass: CommandDurabilityRelaxed,
		LSN:             2,
		Kind:            CommandKindRawKVBatch,
		Scope:           CommandScopeRawKV,
		PayloadFormat:   PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, available := range []int64{27, 28, 54, 55} {
		t.Run(fmt.Sprintf("available_%d", available), func(t *testing.T) {
			path := writeTruncatedCommandFrameV2ForInspection(t, frame, available)
			_, _, err := InspectCommandFrameV2TerminalTail(path, 0)
			if !errors.Is(err, ErrCommandWALV2TailIdentityUnavailable) || !errors.Is(err, ErrCorrupt) {
				t.Fatalf("inspection error=%v, want identity-unavailable corruption", err)
			}
		})
	}

	path := writeTruncatedCommandFrameV2ForInspection(t, frame, 56)
	env, end, err := InspectCommandFrameV2TerminalTail(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if env.LSN != 2 || env.DurabilityClass != CommandDurabilityRelaxed || end != segmentHeaderSize+56 {
		t.Fatalf("shortest classifiable tail env=%+v end=%d, want relaxed LSN 2 end %d", env, end, segmentHeaderSize+56)
	}
}

func TestInspectCommandFrameV2TerminalTailMarksShortSegmentHeaderIdentityUnavailable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	if err := os.WriteFile(path, []byte{0x7f, 0x01, 0x02}, 0o600); err != nil {
		t.Fatal(err)
	}
	_, end, err := InspectCommandFrameV2TerminalTail(path, 0)
	if !errors.Is(err, ErrCommandWALV2TailIdentityUnavailable) || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("inspection error=%v, want identity-unavailable corruption", err)
	}
	if end != 3 {
		t.Fatalf("tail end=%d, want 3", end)
	}
}

func TestInspectCommandFrameV2TerminalTailRejectsCriticalFeatureFlags(t *testing.T) {
	frame, err := EncodeCommandFrameV2(CommandEnvelope{
		DurabilityClass: CommandDurabilityRelaxed,
		LSN:             2,
		Kind:            CommandKindRawKVBatch,
		Scope:           CommandScopeRawKV,
		PayloadFormat:   PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint64(frame[12:20], 1)
	path := writeTruncatedCommandFrameV2ForInspection(t, frame, 56)
	if _, _, err := InspectCommandFrameV2TerminalTail(path, 0); !errors.Is(err, ErrCommandWALUnsupportedCriticalFlag) {
		t.Fatalf("inspection error=%v, want ErrCommandWALUnsupportedCriticalFlag", err)
	}
}

func writeTruncatedCommandFrameV2ForInspection(t *testing.T, frame []byte, available int64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.writeSegment(frame); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, segmentHeaderSize+available); err != nil {
		t.Fatal(err)
	}
	return path
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

func TestCommandJournalAcceptsV2AndRetainsV1Default(t *testing.T) {
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
		t.Fatalf("default encoder version=%d, want compatibility V1 version=%d", got, CommandFrameVersion)
	}
	if CommandFrameVersion != 1 {
		t.Fatalf("default CommandFrameVersion=%d, want compatibility V1", CommandFrameVersion)
	}

	dir := t.TempDir()
	journal, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if lsn, err := journal.AppendCommand(CommandEnvelope{
		Version:         CommandFrameVersionV2,
		DurabilityClass: CommandDurabilityDurable,
		Kind:            CommandKindRawKVBatch,
		Scope:           CommandScopeRawKV,
		PayloadFormat:   PayloadFormatRawKVBatchV1,
	}); err != nil || lsn != 1 {
		t.Fatalf("explicit V2 append lsn=%d error=%v", lsn, err)
	}
	if lsn, err := journal.AppendCommand(CommandEnvelope{
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil || lsn != 2 {
		t.Fatalf("default V1 append lsn=%d err=%v", lsn, err)
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
	}); err != nil || lsn != 3 {
		t.Fatalf("default V1 append after reopen lsn=%d err=%v", lsn, err)
	}
	if err := reopen.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	wantVersions := []uint16{CommandFrameVersionV2, CommandFrameVersion, CommandFrameVersion}
	for index, wantVersion := range wantVersions {
		wantLSN := uint64(index + 1)
		got, err := reader.ReadCommandFrame()
		if err != nil {
			t.Fatalf("read journal frame %d: %v", wantLSN, err)
		}
		if got.Version != wantVersion || got.LSN != wantLSN {
			t.Fatalf("journal frame=%+v, want version=%d lsn=%d", got, wantVersion, wantLSN)
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

func TestExternalRefFenceV1CompactZeroDoesNotMaterializeDeclaredValue(t *testing.T) {
	const declaredValueLen = 16 << 20
	for _, version := range []uint16{rawKVZeroBatchPayloadV2, rawKVZeroBatchPayloadV3} {
		t.Run(fmt.Sprintf("version_%d", version), func(t *testing.T) {
			opHeaderSize := rawKVZeroOpHeaderSizeForVersion(version)
			payload := make([]byte, rawKVZeroBatchHeaderSize+opHeaderSize+1)
			binary.LittleEndian.PutUint16(payload[0:2], version)
			binary.LittleEndian.PutUint32(payload[2:6], 1)
			binary.LittleEndian.PutUint32(payload[6:10], declaredValueLen)
			if version == rawKVZeroBatchPayloadV3 {
				binary.LittleEndian.PutUint16(payload[10:12], 1)
			} else {
				binary.LittleEndian.PutUint32(payload[10:14], 1)
			}
			payload[len(payload)-1] = 'k'

			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			fence, err := ExternalRefFenceV1FromRawKVPayload(payload)
			runtime.ReadMemStats(&after)
			if err != nil {
				t.Fatal(err)
			}
			if fence != (ExternalRefFenceV1{}) {
				t.Fatalf("compact-zero fence=%+v, want empty", fence)
			}
			if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 1<<20 {
				t.Fatalf("compact-zero fence allocated %d bytes for %d-byte declaration, want bounded metadata-only scan", allocated, declaredValueLen)
			}
		})
	}
}

func TestCommandWALFormatGoldenV2DurablePrefixBarrier(t *testing.T) {
	frame, err := EncodeCommandFrameV2(NewDurablePrefixBarrierV1(8, 7))
	if err != nil {
		t.Fatal(err)
	}
	assertGoldenHex(t, "command_wal_v2_durable_prefix_barrier.hex", frame)
}
