package commitlog

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestCommandWALFormatGoldenV1EmptySegment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("write empty segment: %v", err)
	}

	r, err := NewReader(path)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	_, err = r.ReadCommandFrame()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("ReadCommandFrame error=%v, want EOF", err)
	}
}

func TestCommandWALFormatGoldenV1RawKVBatch(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("alpha"), Value: []byte("one")},
		{Op: RawKVOpDelete, Key: []byte("beta")},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	env := CommandEnvelope{
		LSN:           7,
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
		Payload:       payload,
	}
	frame, err := EncodeCommandFrame(env)
	if err != nil {
		t.Fatalf("EncodeCommandFrame: %v", err)
	}
	assertGoldenHex(t, "command_wal_v1_raw_kv_batch.hex", frame)

	got, err := DecodeCommandFrame(frame)
	if err != nil {
		t.Fatalf("DecodeCommandFrame: %v", err)
	}
	if got.LSN != 7 || got.Kind != CommandKindRawKVBatch || got.Scope != CommandScopeRawKV {
		t.Fatalf("decoded identity mismatch: %+v", got)
	}
	ops, err := DecodeRawKVBatchPayload(got.Payload)
	if err != nil {
		t.Fatalf("DecodeRawKVBatchPayload: %v", err)
	}
	if len(ops) != 2 || ops[0].Op != RawKVOpSet || string(ops[0].Key) != "alpha" || string(ops[0].Value) != "one" || ops[1].Op != RawKVOpDelete || string(ops[1].Key) != "beta" {
		t.Fatalf("decoded ops mismatch: %+v", ops)
	}
}

func TestCommandWALFormatGoldenV1CollectionInsertBatchByID(t *testing.T) {
	env := CommandEnvelope{
		LSN:           11,
		Kind:          CommandKindCollectionInsertBatchByID,
		Scope:         CommandScopeCollection,
		PayloadFormat: PayloadFormatNativeWireDeterministic,
		Payload:       []byte("native-wire-placeholder:collection-insert-by-id"),
	}
	frame, err := EncodeCommandFrame(env)
	if err != nil {
		t.Fatalf("EncodeCommandFrame: %v", err)
	}
	assertGoldenHex(t, "command_wal_v1_collection_insert_placeholder.hex", frame)
	got, err := DecodeCommandFrame(frame)
	if err != nil {
		t.Fatalf("DecodeCommandFrame: %v", err)
	}
	if got.Kind != CommandKindCollectionInsertBatchByID || got.PayloadFormat != PayloadFormatNativeWireDeterministic {
		t.Fatalf("decoded placeholder mismatch: %+v", got)
	}
}

func TestCommandWALFormatGoldenV1CatalogMutationPlaceholder(t *testing.T) {
	env := CommandEnvelope{
		LSN:           13,
		Kind:          CommandKindCatalogMutationPlaceholder,
		Scope:         CommandScopeCatalog,
		PayloadFormat: PayloadFormatNativeWireDeterministic,
		Payload:       []byte("native-wire-placeholder:catalog-mutation"),
	}
	frame, err := EncodeCommandFrame(env)
	if err != nil {
		t.Fatalf("EncodeCommandFrame: %v", err)
	}
	assertGoldenHex(t, "command_wal_v1_catalog_placeholder.hex", frame)
	got, err := DecodeCommandFrame(frame)
	if err != nil {
		t.Fatalf("DecodeCommandFrame: %v", err)
	}
	if got.Kind != CommandKindCatalogMutationPlaceholder || got.Scope != CommandScopeCatalog {
		t.Fatalf("decoded catalog placeholder mismatch: %+v", got)
	}
}

func TestCommandWALFormatRejectsUnsupportedRequiredVersion(t *testing.T) {
	frame := mustCommandFrame(t, CommandEnvelope{LSN: 1, Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1})
	frame[4] = 0xff
	frame[5] = 0xff
	_, err := DecodeCommandFrame(frame)
	if !errors.Is(err, ErrCommandWALUnsupportedVersion) {
		t.Fatalf("DecodeCommandFrame error=%v, want ErrCommandWALUnsupportedVersion", err)
	}
}

func TestCommandWALFormatRejectsUnknownCriticalFlag(t *testing.T) {
	_, err := EncodeCommandFrame(CommandEnvelope{LSN: 1, Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1, FeatureFlags: 1})
	if !errors.Is(err, ErrCommandWALUnsupportedCriticalFlag) {
		t.Fatalf("EncodeCommandFrame error=%v, want ErrCommandWALUnsupportedCriticalFlag", err)
	}
}

func TestCommandWALFormatRejectsUnknownRequiredKind(t *testing.T) {
	frame := mustCommandFrame(t, CommandEnvelope{LSN: 1, Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1})
	frame[8] = 0xff
	frame[9] = 0xff
	_, err := DecodeCommandFrame(frame)
	if !errors.Is(err, ErrCommandWALUnsupportedKind) {
		t.Fatalf("DecodeCommandFrame error=%v, want ErrCommandWALUnsupportedKind", err)
	}
}

func TestCommandWALFormatSkipsUnknownNonCriticalExtensionOnlyWhenAllowed(t *testing.T) {
	frame := mustCommandFrame(t, CommandEnvelope{LSN: 1, Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1, FeatureFlags: CommandWALNonCriticalFlagStart})
	got, err := DecodeCommandFrame(frame)
	if err != nil {
		t.Fatalf("DecodeCommandFrame: %v", err)
	}
	if got.FeatureFlags != CommandWALNonCriticalFlagStart {
		t.Fatalf("feature flags=%#x want %#x", got.FeatureFlags, uint64(CommandWALNonCriticalFlagStart))
	}
}

func TestCommandWALFormatRejectsMalformedLengthBeforeAllocation(t *testing.T) {
	frame := mustCommandFrame(t, CommandEnvelope{LSN: 1, Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1})
	// Payload length sits in the fixed command-frame header and must be checked
	// against the already-buffered frame before allocating payload-owned objects.
	putCommandFramePayloadLenForTest(frame, ^uint32(0))
	_, err := DecodeCommandFrame(frame)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("DecodeCommandFrame error=%v, want ErrCorrupt", err)
	}
}

func TestCommandWALAppendCommandRejectsMaxSegmentBeforeEncode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriterWithOptions(path, Options{MaxSegmentSize: int64(commandFrameHeaderSize + rawKVBatchHeaderSize - 1)})
	if err != nil {
		t.Fatalf("NewWriterWithOptions: %v", err)
	}
	defer w.Close()

	err = w.AppendCommand(CommandEnvelope{
		LSN:           1,
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("AppendCommand error=%v, want ErrRecordTooLarge", err)
	}
	if len(w.scratch) != 0 {
		t.Fatalf("scratch len=%d, want 0 after pre-encode rejection", len(w.scratch))
	}
	if w.size != 0 {
		t.Fatalf("writer size=%d, want 0 after pre-encode rejection", w.size)
	}
}

func TestCommandWALFormatRejectsMalformedSectionCountsBeforeAllocation(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload(nil)
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}

	extRefs := make([]byte, 4)
	binary.LittleEndian.PutUint32(extRefs, ^uint32(0))
	frame := mustCommandFrameWithSections(t, payload, extRefs, nil, nil)
	if _, err := DecodeCommandFrame(frame); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("DecodeCommandFrame external refs error=%v, want ErrCorrupt", err)
	}

	extensions := make([]byte, 4)
	binary.LittleEndian.PutUint32(extensions, ^uint32(0))
	frame = mustCommandFrameWithSections(t, payload, nil, extensions, nil)
	if _, err := DecodeCommandFrame(frame); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("DecodeCommandFrame preconditions error=%v, want ErrCorrupt", err)
	}

	frame = mustCommandFrameWithSections(t, payload, nil, nil, extensions)
	if _, err := DecodeCommandFrame(frame); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("DecodeCommandFrame assertions error=%v, want ErrCorrupt", err)
	}
}

func TestCommandWALFormatRejectsHeaderPayloadDigestAndTrailerMismatch(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{{Op: RawKVOpSet, Key: []byte("k"), Value: []byte("v")}})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	frame := mustCommandFrame(t, CommandEnvelope{LSN: 1, Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1, Payload: payload})
	frame[len(frame)-1] ^= 0x7f
	_, err = DecodeCommandFrame(frame)
	if !errors.Is(err, ErrCommandWALPayloadDigestMismatch) {
		t.Fatalf("DecodeCommandFrame error=%v, want ErrCommandWALPayloadDigestMismatch", err)
	}

	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.AppendCommand(CommandEnvelope{LSN: 2, Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1, Payload: payload}); err != nil {
		_ = w.Close()
		t.Fatalf("AppendCommand: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	r, err := NewReader(path)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	_, err = r.ReadCommandFrame()
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("ReadCommandFrame error=%v, want ErrCorrupt", err)
	}
}

func TestCommandWALFormatRoundTripExternalRefs(t *testing.T) {
	digest := [32]byte{0xaa, 0xbb, 0xcc}
	frame := mustCommandFrame(t, CommandEnvelope{
		LSN:           4,
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
		ExternalRefs: []ExternalRef{{
			Class:  ExternalRefValueLog,
			FileID: 41,
			Offset: 128,
			Length: 512,
			Digest: digest,
			Path:   []byte("value_vlog/value-l0-000041.log"),
		}},
	})
	got, err := DecodeCommandFrame(frame)
	if err != nil {
		t.Fatalf("DecodeCommandFrame: %v", err)
	}
	if len(got.ExternalRefs) != 1 {
		t.Fatalf("external refs len=%d want 1", len(got.ExternalRefs))
	}
	ref := got.ExternalRefs[0]
	if ref.Class != ExternalRefValueLog || ref.FileID != 41 || ref.Offset != 128 || ref.Length != 512 || ref.Digest != digest || string(ref.Path) != "value_vlog/value-l0-000041.log" {
		t.Fatalf("external ref mismatch: %+v", ref)
	}
}

func TestCommandWALRawKVBatchAllowsEmptyKeysButRejectsNilKeys(t *testing.T) {
	if _, err := EncodeRawKVBatchPayload([]RawKVOperation{{Op: RawKVOpSet, Key: nil, Value: []byte("v")}}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("EncodeRawKVBatchPayload nil key error=%v, want ErrCorrupt", err)
	}

	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte{}, Value: []byte("empty-key-value")},
		{Op: RawKVOpDelete, Key: []byte{}},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload empty keys: %v", err)
	}
	ops, err := DecodeRawKVBatchPayload(payload)
	if err != nil {
		t.Fatalf("DecodeRawKVBatchPayload empty keys: %v", err)
	}
	if len(ops) != 2 || ops[0].Key == nil || len(ops[0].Key) != 0 || ops[1].Key == nil || len(ops[1].Key) != 0 {
		t.Fatalf("decoded empty keys mismatch: %+v", ops)
	}
}

func TestCommandWALRawKVBatchScanUsesPayloadBackedViews(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("alpha"), Value: []byte("one")},
		{Op: RawKVOpDelete, Key: []byte("beta")},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	var got []RawKVOperation
	err = ScanRawKVBatchPayload(payload, func(op RawKVOp, key, value []byte) error {
		got = append(got, RawKVOperation{Op: op, Key: key, Value: value})
		return nil
	})
	if err != nil {
		t.Fatalf("ScanRawKVBatchPayload: %v", err)
	}
	if len(got) != 2 || got[0].Op != RawKVOpSet || string(got[0].Key) != "alpha" || string(got[0].Value) != "one" || got[1].Op != RawKVOpDelete || string(got[1].Key) != "beta" || len(got[1].Value) != 0 {
		t.Fatalf("scanned ops mismatch: %+v", got)
	}
	payload[rawKVBatchHeaderSize+rawKVOpHeaderSize] = 'A'
	if string(got[0].Key) != "Alpha" {
		t.Fatalf("scan should return payload-backed key view, got %q", string(got[0].Key))
	}
}

func TestCommandWALRawKVBatchOneLSNAtomic(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("a"), Value: []byte("1")},
		{Op: RawKVOpSet, Key: []byte("b"), Value: []byte("2")},
		{Op: RawKVOpDelete, Key: []byte("c")},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	frame := mustCommandFrame(t, CommandEnvelope{LSN: 22, Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1, Payload: payload})
	env, err := DecodeCommandFrame(frame)
	if err != nil {
		t.Fatalf("DecodeCommandFrame: %v", err)
	}
	if env.LSN != 22 {
		t.Fatalf("LSN=%d want 22", env.LSN)
	}
	ops, err := DecodeRawKVBatchPayload(env.Payload)
	if err != nil {
		t.Fatalf("DecodeRawKVBatchPayload: %v", err)
	}
	if len(ops) != 3 {
		t.Fatalf("ops len=%d want 3", len(ops))
	}
}

func TestCommandWALFeatureGateRejectsLegacyRawPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.AppendBatch([]Record{{Op: OpSetInline, Key: []byte("k"), Value: []byte("v"), Seq: 1}}); err != nil {
		_ = w.Close()
		t.Fatalf("AppendBatch: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	r, err := NewReader(path)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	_, err = r.ReadCommandFrame()
	if !errors.Is(err, ErrCommandWALLegacyPayload) {
		t.Fatalf("ReadCommandFrame error=%v, want ErrCommandWALLegacyPayload", err)
	}
}

func TestCommandWALNoCollectionSegmentFamilyCreated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit-l0-000001.log")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.AppendCommand(CommandEnvelope{LSN: 1, Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1}); err != nil {
		_ = w.Close()
		t.Fatalf("AppendCommand: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, entry := range entries {
		if bytes.HasPrefix([]byte(entry.Name()), []byte("collection-l")) {
			t.Fatalf("unexpected collection WAL segment %s", entry.Name())
		}
	}
}

func TestCommandWALSingleJournalOwnerRejectsSecondMutableWriter(t *testing.T) {
	dir := t.TempDir()
	owner, err := AcquireJournalOwner(dir)
	if err != nil {
		t.Fatalf("AcquireJournalOwner first: %v", err)
	}
	defer owner.Close()
	_, err = AcquireJournalOwner(dir)
	if !errors.Is(err, ErrJournalOwnerExists) {
		t.Fatalf("AcquireJournalOwner second error=%v, want ErrJournalOwnerExists", err)
	}
}

func TestCommandWALTerminalShortHeaderIgnored(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	if err := os.WriteFile(path, []byte{0x01, 0x02}, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	r, err := NewReader(path)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	_, err = r.ReadCommandFrame()
	if !errors.Is(err, ErrCommandWALTerminalTail) {
		t.Fatalf("ReadCommandFrame error=%v, want ErrCommandWALTerminalTail", err)
	}
}

func TestCommandWALDuplicateLSNFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := w.AppendCommand(CommandEnvelope{LSN: 9, Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1}); err != nil {
			_ = w.Close()
			t.Fatalf("AppendCommand(%d): %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err = ScanCommandFrames(path, Options{})
	if !errors.Is(err, ErrCommandWALDuplicateLSN) {
		t.Fatalf("ScanCommandFrames error=%v, want ErrCommandWALDuplicateLSN", err)
	}
}

func mustCommandFrame(t *testing.T, env CommandEnvelope) []byte {
	t.Helper()
	frame, err := EncodeCommandFrame(env)
	if err != nil {
		t.Fatalf("EncodeCommandFrame: %v", err)
	}
	return frame
}

func mustCommandFrameWithSections(t *testing.T, payload, extRefs, preconditions, assertions []byte) []byte {
	t.Helper()
	frame := mustCommandFrame(t, CommandEnvelope{
		LSN:           1,
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
		Payload:       payload,
	})
	total := commandFrameHeaderSize + len(payload) + len(extRefs) + len(preconditions) + len(assertions)
	out := make([]byte, total)
	copy(out, frame[:commandFrameHeaderSize+len(payload)])
	binary.LittleEndian.PutUint32(out[60:64], uint32(len(extRefs)))
	binary.LittleEndian.PutUint32(out[64:68], uint32(len(preconditions)))
	binary.LittleEndian.PutUint32(out[68:72], uint32(len(assertions)))
	off := commandFrameHeaderSize + len(payload)
	copy(out[off:], extRefs)
	off += len(extRefs)
	copy(out[off:], preconditions)
	off += len(preconditions)
	copy(out[off:], assertions)
	return out
}

func assertGoldenHex(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", name)
	wantHex, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	want, err := hex.DecodeString(string(bytes.TrimSpace(wantHex)))
	if err != nil {
		t.Fatalf("decode golden %s: %v", path, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("golden %s mismatch\ngot  %s\nwant %s", name, hex.EncodeToString(got), hex.EncodeToString(want))
	}
}
