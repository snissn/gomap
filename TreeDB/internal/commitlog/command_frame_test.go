package commitlog

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
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

func TestCommandWALRawKVBatchPreservesEmptySetValue(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("empty"), Value: []byte{}},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	ops, err := DecodeRawKVBatchPayload(payload)
	if err != nil {
		t.Fatalf("DecodeRawKVBatchPayload: %v", err)
	}
	if len(ops) != 1 || ops[0].Value == nil || len(ops[0].Value) != 0 {
		t.Fatalf("decoded empty value = %#v, want non-nil empty slice", ops)
	}
}

func TestCommandWALRawKVBatchZeroPayloadV3ScanDecode(t *testing.T) {
	var builder RawKVBatchPayloadBuilder
	if err := builder.ResetWithHint(2, 32); err != nil {
		t.Fatalf("ResetWithHint: %v", err)
	}
	if _, _, err := builder.AppendSet([]byte("alpha"), make([]byte, 4)); err != nil {
		t.Fatalf("AppendSet alpha: %v", err)
	}
	if _, _, err := builder.AppendSet([]byte("beta"), make([]byte, 4)); err != nil {
		t.Fatalf("AppendSet beta: %v", err)
	}
	payload := builder.Payload()
	if got := binary.LittleEndian.Uint16(payload[0:2]); got != rawKVZeroBatchPayloadV3 {
		t.Fatalf("payload version=%d want %d payload=%x", got, rawKVZeroBatchPayloadV3, payload)
	}
	if len(payload) >= rawKVBatchHeaderSize+2*(rawKVOpHeaderSize+4+4) {
		t.Fatalf("zero payload was not compact: len=%d", len(payload))
	}

	var scanned []RawKVOperation
	if err := ScanRawKVBatchPayload(payload, func(op RawKVOp, key, value []byte) error {
		scanned = append(scanned, RawKVOperation{
			Op:    op,
			Key:   cloneBytesPreserveEmpty(key),
			Value: cloneBytesPreserveEmpty(value),
		})
		return nil
	}); err != nil {
		t.Fatalf("ScanRawKVBatchPayload: %v", err)
	}
	if len(scanned) != 2 || scanned[0].Op != RawKVOpSet || string(scanned[0].Key) != "alpha" || !bytes.Equal(scanned[0].Value, make([]byte, 4)) ||
		scanned[1].Op != RawKVOpSet || string(scanned[1].Key) != "beta" || !bytes.Equal(scanned[1].Value, make([]byte, 4)) {
		t.Fatalf("scanned compact zero ops mismatch: %+v", scanned)
	}

	decoded, err := DecodeRawKVBatchPayload(payload)
	if err != nil {
		t.Fatalf("DecodeRawKVBatchPayload: %v", err)
	}
	if len(decoded) != 2 || string(decoded[0].Key) != "alpha" || !bytes.Equal(decoded[0].Value, make([]byte, 4)) ||
		string(decoded[1].Key) != "beta" || !bytes.Equal(decoded[1].Value, make([]byte, 4)) {
		t.Fatalf("decoded compact zero ops mismatch: %+v", decoded)
	}
}

func TestRawKVBatchPayloadBuilderZeroCompactAvoidsExpandedPayloadReserve(t *testing.T) {
	var builder RawKVBatchPayloadBuilder
	if err := builder.ResetWithHint(8192, 8192*192); err != nil {
		t.Fatalf("ResetWithHint: %v", err)
	}
	if got := cap(builder.payload); got != rawKVBatchHeaderSize {
		t.Fatalf("raw payload cap after reset=%d want header-only %d", got, rawKVBatchHeaderSize)
	}
	if _, _, err := builder.AppendSet([]byte("alpha"), make([]byte, 128)); err != nil {
		t.Fatalf("AppendSet alpha: %v", err)
	}
	if got := cap(builder.payload); got != rawKVBatchHeaderSize {
		t.Fatalf("raw payload cap after compact zero append=%d want header-only %d", got, rawKVBatchHeaderSize)
	}
	payload := builder.Payload()
	if got := binary.LittleEndian.Uint16(payload[0:2]); got != rawKVZeroBatchPayloadV3 {
		t.Fatalf("payload version=%d want %d", got, rawKVZeroBatchPayloadV3)
	}
}

func TestCommandWALRawKVBatchZeroPayloadV2FallbackForLargeKey(t *testing.T) {
	var builder RawKVBatchPayloadBuilder
	if err := builder.ResetWithHint(1, 70_004); err != nil {
		t.Fatalf("ResetWithHint: %v", err)
	}
	key := bytes.Repeat([]byte("k"), int(^uint16(0))+1)
	if _, _, err := builder.AppendSet(key, make([]byte, 4)); err != nil {
		t.Fatalf("AppendSet large key: %v", err)
	}
	payload := builder.Payload()
	if got := binary.LittleEndian.Uint16(payload[0:2]); got != rawKVZeroBatchPayloadV2 {
		t.Fatalf("payload version=%d want %d", got, rawKVZeroBatchPayloadV2)
	}
	decoded, err := DecodeRawKVBatchPayload(payload)
	if err != nil {
		t.Fatalf("DecodeRawKVBatchPayload: %v", err)
	}
	if len(decoded) != 1 || !bytes.Equal(decoded[0].Key, key) || !bytes.Equal(decoded[0].Value, make([]byte, 4)) {
		t.Fatalf("decoded v2 fallback mismatch: len=%d", len(decoded))
	}
}

func TestRawKVBatchPayloadBuilderZeroCompactionFallsBackForMixedValues(t *testing.T) {
	var builder RawKVBatchPayloadBuilder
	if err := builder.ResetWithHint(2, 32); err != nil {
		t.Fatalf("ResetWithHint: %v", err)
	}
	if _, _, err := builder.AppendSet([]byte("stale"), bytes.Repeat([]byte{0xff}, 4)); err != nil {
		t.Fatalf("AppendSet stale: %v", err)
	}
	_ = builder.Payload()
	if err := builder.ResetWithHint(2, 32); err != nil {
		t.Fatalf("ResetWithHint after stale payload: %v", err)
	}
	if _, _, err := builder.AppendSet([]byte("alpha"), make([]byte, 4)); err != nil {
		t.Fatalf("AppendSet alpha: %v", err)
	}
	if _, _, err := builder.AppendSet([]byte("beta"), []byte{0, 0, 0, 1}); err != nil {
		t.Fatalf("AppendSet beta: %v", err)
	}
	payload := builder.Payload()
	if got := binary.LittleEndian.Uint16(payload[0:2]); got != rawKVBatchPayloadVersion {
		t.Fatalf("payload version=%d want %d", got, rawKVBatchPayloadVersion)
	}
	decoded, err := DecodeRawKVBatchPayload(payload)
	if err != nil {
		t.Fatalf("DecodeRawKVBatchPayload: %v", err)
	}
	if len(decoded) != 2 || !bytes.Equal(decoded[1].Value, []byte{0, 0, 0, 1}) {
		t.Fatalf("decoded mixed ops mismatch: %+v", decoded)
	}
	if !bytes.Equal(decoded[0].Value, make([]byte, 4)) {
		t.Fatalf("materialized omitted zero value = %x, want zeros", decoded[0].Value)
	}
}

func TestCommandWALCollectionPayloadDecodeBoundsCountBeforeAllocation(t *testing.T) {
	payload := make([]byte, 10+len("users"))
	encodeCollectionBatchHeader(payload, "users", int(^uint32(0)))
	if _, err := DecodeCollectionInsertBatchByIDPayload(payload); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("DecodeCollectionInsertBatchByIDPayload huge count error=%v, want ErrCorrupt", err)
	}
	if _, err := DecodeCollectionDeleteBatchByIDPayload(payload); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("DecodeCollectionDeleteBatchByIDPayload huge count error=%v, want ErrCorrupt", err)
	}

	rebuildPayload := make([]byte, collectionRebuildVectorIndexPayloadHeaderSize)
	binary.LittleEndian.PutUint16(rebuildPayload[:collectionRebuildVectorIndexVersionEnd], collectionRebuildVectorIndexPayloadVersion)
	binary.LittleEndian.PutUint32(rebuildPayload[collectionRebuildVectorIndexCollectionLenStart:collectionRebuildVectorIndexCollectionLenEnd], ^uint32(0))
	binary.LittleEndian.PutUint32(rebuildPayload[collectionRebuildVectorIndexIndexLenStart:collectionRebuildVectorIndexIndexLenEnd], 1)
	if _, err := DecodeCollectionRebuildVectorIndexPayload(rebuildPayload); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("DecodeCollectionRebuildVectorIndexPayload huge length error=%v, want ErrCorrupt", err)
	}
}

func TestCommandWALFormatGoldenV1CollectionInsertBatchByID(t *testing.T) {
	payload, err := EncodeCollectionInsertBatchByIDPayload("users", []CollectionDocument{
		{ID: []byte("u2"), Document: []byte(`{"name":"Grace"}`)},
		{ID: []byte("u1"), Document: []byte(`{"name":"Ada"}`)},
	})
	if err != nil {
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
	}
	env := CommandEnvelope{
		LSN:           11,
		Kind:          CommandKindCollectionInsertBatchByID,
		Scope:         CommandScopeCollection,
		PayloadFormat: PayloadFormatCollectionInsertBatchByIDV1,
		Payload:       payload,
	}
	frame, err := EncodeCommandFrame(env)
	if err != nil {
		t.Fatalf("EncodeCommandFrame: %v", err)
	}
	assertGoldenHex(t, "command_wal_v1_collection_insert_by_id.hex", frame)
	got, err := DecodeCommandFrame(frame)
	if err != nil {
		t.Fatalf("DecodeCommandFrame: %v", err)
	}
	if got.Kind != CommandKindCollectionInsertBatchByID || got.PayloadFormat != PayloadFormatCollectionInsertBatchByIDV1 {
		t.Fatalf("decoded placeholder mismatch: %+v", got)
	}
	decoded, err := DecodeCollectionInsertBatchByIDPayload(got.Payload)
	if err != nil {
		t.Fatalf("DecodeCollectionInsertBatchByIDPayload: %v", err)
	}
	if decoded.Collection != "users" || len(decoded.Documents) != 2 ||
		string(decoded.Documents[0].ID) != "u1" || string(decoded.Documents[0].Document) != `{"name":"Ada"}` ||
		string(decoded.Documents[1].ID) != "u2" || string(decoded.Documents[1].Document) != `{"name":"Grace"}` {
		t.Fatalf("decoded collection insert payload=%+v", decoded)
	}
}

func TestCommandWALFormatGoldenV1CollectionDeleteBatchByID(t *testing.T) {
	payload, err := EncodeCollectionDeleteBatchByIDPayload("users", [][]byte{[]byte("u2"), []byte("u1")})
	if err != nil {
		t.Fatalf("EncodeCollectionDeleteBatchByIDPayload: %v", err)
	}
	env := CommandEnvelope{
		LSN:           12,
		Kind:          CommandKindCollectionDeleteBatchByID,
		Scope:         CommandScopeCollection,
		PayloadFormat: PayloadFormatCollectionDeleteBatchByIDV1,
		Payload:       payload,
	}
	frame, err := EncodeCommandFrame(env)
	if err != nil {
		t.Fatalf("EncodeCommandFrame: %v", err)
	}
	assertGoldenHex(t, "command_wal_v1_collection_delete_by_id.hex", frame)
	got, err := DecodeCommandFrame(frame)
	if err != nil {
		t.Fatalf("DecodeCommandFrame: %v", err)
	}
	if got.Kind != CommandKindCollectionDeleteBatchByID || got.PayloadFormat != PayloadFormatCollectionDeleteBatchByIDV1 {
		t.Fatalf("decoded collection delete mismatch: %+v", got)
	}
	decoded, err := DecodeCollectionDeleteBatchByIDPayload(got.Payload)
	if err != nil {
		t.Fatalf("DecodeCollectionDeleteBatchByIDPayload: %v", err)
	}
	if decoded.Collection != "users" || len(decoded.IDs) != 2 || string(decoded.IDs[0]) != "u1" || string(decoded.IDs[1]) != "u2" {
		t.Fatalf("decoded collection delete payload=%+v", decoded)
	}
}

func TestCommandWALFormatGoldenV1CollectionUpdateBatchByID(t *testing.T) {
	payload, err := EncodeCollectionUpdateBatchByIDPayload("users", []CollectionDocument{
		{ID: []byte("u2"), Document: []byte(`{"name":"Grace","active":true}`)},
		{ID: []byte("u1"), Document: []byte(`{"name":"Ada","active":true}`)},
	})
	if err != nil {
		t.Fatalf("EncodeCollectionUpdateBatchByIDPayload: %v", err)
	}
	env := CommandEnvelope{
		LSN:           14,
		Kind:          CommandKindCollectionUpdateBatchByID,
		Scope:         CommandScopeCollection,
		PayloadFormat: PayloadFormatCollectionUpdateBatchByIDV1,
		Payload:       payload,
	}
	frame, err := EncodeCommandFrame(env)
	if err != nil {
		t.Fatalf("EncodeCommandFrame: %v", err)
	}
	assertGoldenHex(t, "command_wal_v1_collection_update_by_id.hex", frame)
	got, err := DecodeCommandFrame(frame)
	if err != nil {
		t.Fatalf("DecodeCommandFrame: %v", err)
	}
	if got.Kind != CommandKindCollectionUpdateBatchByID || got.PayloadFormat != PayloadFormatCollectionUpdateBatchByIDV1 {
		t.Fatalf("decoded collection update mismatch: %+v", got)
	}
	decoded, err := DecodeCollectionUpdateBatchByIDPayload(got.Payload)
	if err != nil {
		t.Fatalf("DecodeCollectionUpdateBatchByIDPayload: %v", err)
	}
	if decoded.Collection != "users" || len(decoded.Documents) != 2 ||
		string(decoded.Documents[0].ID) != "u1" || string(decoded.Documents[0].Document) != `{"name":"Ada","active":true}` ||
		string(decoded.Documents[1].ID) != "u2" || string(decoded.Documents[1].Document) != `{"name":"Grace","active":true}` {
		t.Fatalf("decoded collection update payload=%+v", decoded)
	}
}

func TestCommandWALFormatV1CollectionRebuildVectorIndex(t *testing.T) {
	payload, err := EncodeCollectionRebuildVectorIndexPayload("users", "embedding_graph")
	if err != nil {
		t.Fatalf("EncodeCollectionRebuildVectorIndexPayload: %v", err)
	}
	env := CommandEnvelope{
		LSN:           15,
		Kind:          CommandKindCollectionRebuildVectorIndex,
		Scope:         CommandScopeCollection,
		PayloadFormat: PayloadFormatCollectionRebuildVectorIndexV1,
		Payload:       payload,
	}
	frame, err := EncodeCommandFrame(env)
	if err != nil {
		t.Fatalf("EncodeCommandFrame: %v", err)
	}
	assertGoldenHex(t, "command_wal_v1_collection_rebuild_vector_index.hex", frame)
	got, err := DecodeCommandFrame(frame)
	if err != nil {
		t.Fatalf("DecodeCommandFrame: %v", err)
	}
	if got.Kind != CommandKindCollectionRebuildVectorIndex ||
		got.Scope != CommandScopeCollection ||
		got.PayloadFormat != PayloadFormatCollectionRebuildVectorIndexV1 {
		t.Fatalf("decoded collection rebuild mismatch: %+v", got)
	}
	decoded, err := DecodeCollectionRebuildVectorIndexPayload(got.Payload)
	if err != nil {
		t.Fatalf("DecodeCollectionRebuildVectorIndexPayload: %v", err)
	}
	if decoded.Collection != "users" || decoded.IndexName != "embedding_graph" {
		t.Fatalf("decoded collection rebuild payload=%+v", decoded)
	}
	if _, err := EncodeCollectionRebuildVectorIndexPayload("", "embedding_graph"); !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "collection name") {
		t.Fatalf("EncodeCollectionRebuildVectorIndexPayload empty collection error=%v, want ErrCorrupt with collection name", err)
	}
	if _, err := EncodeCollectionRebuildVectorIndexPayload("users", ""); !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "index name") {
		t.Fatalf("EncodeCollectionRebuildVectorIndexPayload empty index error=%v, want ErrCorrupt with index name", err)
	}
}

func TestCommandWALFormatGoldenV1CatalogCreateCollection(t *testing.T) {
	payload, err := EncodeCatalogCreateCollectionPayload("users", []byte(`{"version":1,"name":"users"}`))
	if err != nil {
		t.Fatalf("EncodeCatalogCreateCollectionPayload: %v", err)
	}
	env := CommandEnvelope{
		LSN:           13,
		Kind:          CommandKindCatalogCreateCollection,
		Scope:         CommandScopeCatalog,
		PayloadFormat: PayloadFormatCatalogCreateCollectionV1,
		Payload:       payload,
	}
	frame, err := EncodeCommandFrame(env)
	if err != nil {
		t.Fatalf("EncodeCommandFrame: %v", err)
	}
	assertGoldenHex(t, "command_wal_v1_catalog_create_collection.hex", frame)
	got, err := DecodeCommandFrame(frame)
	if err != nil {
		t.Fatalf("DecodeCommandFrame: %v", err)
	}
	if got.Kind != CommandKindCatalogCreateCollection || got.Scope != CommandScopeCatalog || got.PayloadFormat != PayloadFormatCatalogCreateCollectionV1 {
		t.Fatalf("decoded catalog create mismatch: %+v", got)
	}
	decoded, err := DecodeCatalogCreateCollectionPayload(got.Payload)
	if err != nil {
		t.Fatalf("DecodeCatalogCreateCollectionPayload: %v", err)
	}
	if decoded.Collection != "users" || string(decoded.Metadata) != `{"version":1,"name":"users"}` {
		t.Fatalf("decoded catalog create payload=%+v", decoded)
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

func TestCommandWALFormatRejectsUnsupportedEncodeVersion(t *testing.T) {
	_, err := EncodeCommandFrame(CommandEnvelope{Version: CommandFrameVersion + 1, LSN: 1, Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1})
	if !errors.Is(err, ErrCommandWALUnsupportedVersion) {
		t.Fatalf("EncodeCommandFrame error=%v, want ErrCommandWALUnsupportedVersion", err)
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

func TestEncodeRawKVBatchPayloadScanMatchesSliceEncoder(t *testing.T) {
	ops := []RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("alpha"), Value: []byte("one")},
		{Op: RawKVOpSet, Key: []byte("bravo"), Value: []byte("two")},
		{Op: RawKVOpDelete, Key: []byte("charlie")},
	}
	want, err := EncodeRawKVBatchPayload(ops)
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	var scanCount int
	got, err := EncodeRawKVBatchPayloadScan(func(emit func(RawKVOperation) error) error {
		scanCount++
		for _, op := range ops {
			if err := emit(op); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayloadScan: %v", err)
	}
	if scanCount != 1 {
		t.Fatalf("scan count=%d want 1", scanCount)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("streaming encoder mismatch\ngot  %x\nwant %x", got, want)
	}
	got, err = EncodeRawKVBatchPayloadScanWithHint(func(emit func(RawKVOperation) error) error {
		for _, op := range ops {
			if err := emit(op); err != nil {
				return err
			}
		}
		return nil
	}, len(ops), len("alpha")+len("one")+len("bravo")+len("two")+len("charlie"))
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayloadScanWithHint: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("hinted streaming encoder mismatch\ngot  %x\nwant %x", got, want)
	}
}

func TestRawKVBatchPayloadBuilderMatchesSliceEncoder(t *testing.T) {
	ops := []RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("alpha"), Value: []byte("one")},
		{Op: RawKVOpSet, Key: []byte("bravo"), Value: []byte{}},
		{Op: RawKVOpDelete, Key: []byte("charlie")},
	}
	want, err := EncodeRawKVBatchPayload(ops)
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	builder := NewRawKVBatchPayloadBuilder(len(ops), len("alpha")+len("one")+len("bravo")+len("charlie"))
	for i, op := range ops {
		var keyView, valueView []byte
		var err error
		if i == 0 {
			keyView, valueView, err = builder.AppendSet(op.Key, op.Value)
		} else if op.Op == RawKVOpDelete {
			keyView, err = builder.AppendDelete(op.Key)
		} else {
			keyView, valueView, err = builder.Append(op)
		}
		if err != nil {
			t.Fatalf("Append(%v): %v", op.Op, err)
		}
		if !bytes.Equal(keyView, op.Key) {
			t.Fatalf("key view=%q want %q", keyView, op.Key)
		}
		if op.Op == RawKVOpSet && !bytes.Equal(valueView, op.Value) {
			t.Fatalf("value view=%q want %q", valueView, op.Value)
		}
	}
	if builder.Count() != len(ops) {
		t.Fatalf("builder count=%d want %d", builder.Count(), len(ops))
	}
	if !bytes.Equal(builder.Payload(), want) {
		t.Fatalf("builder payload mismatch\ngot  %x\nwant %x", builder.Payload(), want)
	}
}

func TestRawKVBatchPayloadBuilderResetWithHintReportsOverflow(t *testing.T) {
	var builder RawKVBatchPayloadBuilder
	if err := builder.ResetWithHint(int(^uint(0)>>1), int(^uint(0)>>1)); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("ResetWithHint overflow error=%v, want ErrRecordTooLarge", err)
	}
	if builder.Count() != 0 {
		t.Fatalf("builder count=%d, want 0", builder.Count())
	}
	if got := len(builder.Payload()); got != rawKVBatchHeaderSize {
		t.Fatalf("payload len=%d, want header size %d", got, rawKVBatchHeaderSize)
	}
	if _, _, err := builder.Append(RawKVOperation{Op: RawKVOpSet, Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatalf("Append after overflow reset: %v", err)
	}
}

func TestWriterAppendRawKVBatchPayloadCommandDirect(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("alpha"), Value: []byte("one")},
		{Op: RawKVOpDelete, Key: []byte("bravo")},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.AppendRawKVBatchPayloadCommandDirect(7, 3, payload); err != nil {
		_ = w.Close()
		t.Fatalf("AppendRawKVBatchPayloadCommandDirect: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	r, err := NewReader(path)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	env, err := r.ReadCommandFrame()
	if err != nil {
		t.Fatalf("ReadCommandFrame: %v", err)
	}
	if env.LSN != 7 || env.BaseAppliedLSN != 3 || env.Kind != CommandKindRawKVBatch || env.Scope != CommandScopeRawKV || env.PayloadFormat != PayloadFormatRawKVBatchV1 {
		t.Fatalf("decoded command identity mismatch: %+v", env)
	}
	if !bytes.Equal(env.Payload, payload) {
		t.Fatalf("payload mismatch\ngot  %x\nwant %x", env.Payload, payload)
	}
}

func TestWriterAppendCommandPayloadDirectTrustedCollectionInsert(t *testing.T) {
	payload, err := EncodeCollectionInsertBatchByIDPayload("users", []CollectionDocument{
		{ID: []byte("user-001"), Document: []byte(`{"_id":"user-001","field0":"value0"}`)},
	})
	if err != nil {
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
	}
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriterWithOptions(path, Options{DeferredCommandBufferSize: 4096})
	if err != nil {
		t.Fatalf("NewWriterWithOptions: %v", err)
	}
	if err := w.AppendCommandPayloadDirectTrusted(7, 3, CommandKindCollectionInsertBatchByID, CommandScopeCollection, PayloadFormatCollectionInsertBatchByIDV1, payload); err != nil {
		_ = w.Close()
		t.Fatalf("AppendCommandPayloadDirectTrusted: %v", err)
	}
	if got := len(w.commandBuf); got == 0 {
		_ = w.Close()
		t.Fatal("trusted collection command frame was not buffered")
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	r, err := NewReader(path)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	env, err := r.ReadCommandFrame()
	if err != nil {
		t.Fatalf("ReadCommandFrame: %v", err)
	}
	if env.LSN != 7 || env.BaseAppliedLSN != 3 || env.Kind != CommandKindCollectionInsertBatchByID || env.Scope != CommandScopeCollection || env.PayloadFormat != PayloadFormatCollectionInsertBatchByIDV1 {
		t.Fatalf("decoded command identity mismatch: %+v", env)
	}
	if !bytes.Equal(env.Payload, payload) {
		t.Fatalf("payload mismatch\ngot  %x\nwant %x", env.Payload, payload)
	}
	decoded, err := DecodeCollectionInsertBatchByIDPayload(env.Payload)
	if err != nil {
		t.Fatalf("DecodeCollectionInsertBatchByIDPayload: %v", err)
	}
	if decoded.Collection != "users" || len(decoded.Documents) != 1 || string(decoded.Documents[0].ID) != "user-001" {
		t.Fatalf("decoded collection payload mismatch: %+v", decoded)
	}
}

func TestWriterBufferedCommandFlushFailurePoisonsWriter(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("alpha"), Value: []byte("one")},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriterWithOptions(path, Options{DeferredCommandBufferSize: 4096})
	if err != nil {
		t.Fatalf("NewWriterWithOptions: %v", err)
	}
	if err := w.AppendRawKVBatchPayloadCommandDirectTrusted(1, 0, payload); err != nil {
		t.Fatalf("AppendRawKVBatchPayloadCommandDirectTrusted: %v", err)
	}
	if err := w.f.Close(); err != nil {
		t.Fatalf("close underlying file: %v", err)
	}
	if err := w.Flush(); err == nil {
		t.Fatal("Flush succeeded after underlying file was closed")
	}
	if err := w.AppendRawKVBatchPayloadCommandDirectTrusted(2, 0, payload); err == nil {
		t.Fatal("Append after poisoned command buffer succeeded")
	}
}

func TestWriterBufferedCommandSizeAdvancesOnFlush(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("alpha"), Value: []byte("one")},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriterWithOptions(path, Options{DeferredCommandBufferSize: 4096})
	if err != nil {
		t.Fatalf("NewWriterWithOptions: %v", err)
	}
	defer w.Close()
	if err := w.AppendRawKVBatchPayloadCommandDirectTrusted(1, 0, payload); err != nil {
		t.Fatalf("AppendRawKVBatchPayloadCommandDirectTrusted: %v", err)
	}
	if got := w.Size(); got != 0 {
		t.Fatalf("writer size after buffered append=%d, want 0 before flush", got)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := w.Size(); got == 0 {
		t.Fatal("writer size after flush=0, want durable command bytes counted")
	}
}

func TestWriterDeferredCommandBufferHonorsConfiguredLimit(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("alpha"), Value: bytes.Repeat([]byte("x"), 512)},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriterWithOptions(path, Options{DeferredCommandBufferSize: 128})
	if err != nil {
		t.Fatalf("NewWriterWithOptions: %v", err)
	}
	defer w.Close()
	if err := w.AppendRawKVBatchPayloadCommandDirectTrusted(1, 0, payload); err != nil {
		t.Fatalf("AppendRawKVBatchPayloadCommandDirectTrusted: %v", err)
	}
	if got := len(w.commandBuf); got != 0 {
		t.Fatalf("deferred command buffer len=%d, want 0 for frame larger than configured cap", got)
	}
	if got := w.Size(); got == 0 {
		t.Fatal("writer size after oversized deferred frame=0, want direct append accounted")
	}
}

func TestWriterLargeBatchPayloadBypassesDeferredCommandBufferAndPreservesOrder(t *testing.T) {
	smallPayload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("small"), Value: []byte("value")},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload small: %v", err)
	}
	largePayload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("large"), Value: bytes.Repeat([]byte("x"), directCommandPayloadMinLen)},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload large: %v", err)
	}
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriterWithOptions(path, Options{DeferredCommandBufferSize: 64 << 20})
	if err != nil {
		t.Fatalf("NewWriterWithOptions: %v", err)
	}
	if err := w.AppendRawKVBatchPayloadCommandDirectTrusted(1, 0, smallPayload); err != nil {
		_ = w.Close()
		t.Fatalf("AppendRawKVBatchPayloadCommandDirectTrusted small: %v", err)
	}
	if got := len(w.commandBuf); got == 0 {
		_ = w.Close()
		t.Fatal("small command frame was not buffered")
	}
	if err := w.AppendRawKVBatchPayloadCommandDirectTrusted(2, 0, largePayload); err != nil {
		_ = w.Close()
		t.Fatalf("AppendRawKVBatchPayloadCommandDirectTrusted large: %v", err)
	}
	if got := len(w.commandBuf); got != 0 {
		_ = w.Close()
		t.Fatalf("deferred command buffer len=%d, want 0 after large direct append", got)
	}
	if got := w.Size(); got == 0 {
		_ = w.Close()
		t.Fatal("writer size after large direct append=0, want bytes accounted")
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	r, err := NewReader(path)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	first, err := r.ReadCommandFrame()
	if err != nil {
		t.Fatalf("ReadCommandFrame first: %v", err)
	}
	if first.LSN != 1 || !bytes.Equal(first.Payload, smallPayload) {
		t.Fatalf("first frame mismatch: lsn=%d payload_len=%d", first.LSN, len(first.Payload))
	}
	second, err := r.ReadCommandFrame()
	if err != nil {
		t.Fatalf("ReadCommandFrame second: %v", err)
	}
	if second.LSN != 2 || !bytes.Equal(second.Payload, largePayload) {
		t.Fatalf("second frame mismatch: lsn=%d payload_len=%d", second.LSN, len(second.Payload))
	}
}

func TestWriterDirectCommandWriteFailurePoisonsWriter(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("alpha"), Value: bytes.Repeat([]byte("x"), directCommandPayloadMinLen)},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriterWithOptions(path, Options{})
	if err != nil {
		t.Fatalf("NewWriterWithOptions: %v", err)
	}
	defer w.Close()
	if err := w.f.Close(); err != nil {
		t.Fatalf("close underlying file: %v", err)
	}
	err = w.AppendRawKVBatchPayloadCommandDirectTrusted(1, 0, payload)
	if err == nil {
		t.Fatal("direct command append unexpectedly succeeded after underlying file close")
	}
	if err := w.AppendRawKVBatchPayloadCommandDirectTrusted(2, 0, payload); err == nil {
		t.Fatal("append after direct command write failure succeeded; writer was not poisoned")
	}
}

func TestWriterPoisonCommandBufferTruncatesUnaccountedTail(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("alpha"), Value: []byte("one")},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriterWithOptions(path, Options{})
	if err != nil {
		t.Fatalf("NewWriterWithOptions: %v", err)
	}
	defer w.Close()
	if err := w.AppendRawKVBatchPayloadCommandDirectTrusted(1, 0, payload); err != nil {
		t.Fatalf("AppendRawKVBatchPayloadCommandDirectTrusted: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	accounted := w.Size()
	if accounted == 0 {
		t.Fatal("writer size after first flush=0")
	}
	if _, err := w.f.Write([]byte("partial-command-tail")); err != nil {
		t.Fatalf("inject unaccounted tail: %v", err)
	}
	if info, err := os.Stat(path); err != nil {
		t.Fatalf("stat after injected tail: %v", err)
	} else if info.Size() <= accounted {
		t.Fatalf("test setup file size=%d, want > accounted %d", info.Size(), accounted)
	}

	injected := errors.New("injected buffered command failure")
	if err := w.poisonCommandBuffer(injected); !errors.Is(err, injected) {
		t.Fatalf("poisonCommandBuffer error=%v, want %v", err, injected)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after poison: %v", err)
	}
	if info.Size() != accounted {
		t.Fatalf("poisonCommandBuffer size=%d, want accounted %d", info.Size(), accounted)
	}
}

func TestEncodeRawKVSingleOperationPayloadMatchesSliceEncoder(t *testing.T) {
	for _, op := range []RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("alpha"), Value: []byte("one")},
		{Op: RawKVOpDelete, Key: []byte("bravo")},
		{Op: RawKVOpSetRID, Key: []byte("charlie"), RID: 42},
	} {
		want, err := EncodeRawKVBatchPayload([]RawKVOperation{op})
		if err != nil {
			t.Fatalf("EncodeRawKVBatchPayload(%v): %v", op.Op, err)
		}
		got, err := EncodeRawKVSingleOperationPayload(op)
		if err != nil {
			t.Fatalf("EncodeRawKVSingleOperationPayload(%v): %v", op.Op, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("single-op encoder mismatch for op %v\ngot  %x\nwant %x", op.Op, got, want)
		}
	}
}

func TestEncodeRawKVSingleCommandFrameMatchesEnvelopeEncoder(t *testing.T) {
	for _, op := range []RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("alpha"), Value: []byte("one")},
		{Op: RawKVOpDelete, Key: []byte("bravo")},
	} {
		payload, err := EncodeRawKVSingleOperationPayload(op)
		if err != nil {
			t.Fatalf("EncodeRawKVSingleOperationPayload(%v): %v", op.Op, err)
		}
		want, err := EncodeCommandFrame(CommandEnvelope{
			LSN:            7,
			Kind:           CommandKindRawKVBatch,
			Scope:          CommandScopeRawKV,
			BaseAppliedLSN: 3,
			PayloadFormat:  PayloadFormatRawKVBatchV1,
			Payload:        payload,
		})
		if err != nil {
			t.Fatalf("EncodeCommandFrame(%v): %v", op.Op, err)
		}
		got, err := encodeRawKVSingleCommandFrameTo(nil, 7, 3, op)
		if err != nil {
			t.Fatalf("encodeRawKVSingleCommandFrameTo(%v): %v", op.Op, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("single-op frame mismatch for op %v\ngot  %x\nwant %x", op.Op, got, want)
		}
		if op.Op == RawKVOpSet || op.Op == RawKVOpDelete {
			trusted, err := encodeTrustedRawKVPointCommandFrameTo(nil, 7, 3, op.Op, op.Key, op.Value)
			if err != nil {
				t.Fatalf("encodeTrustedRawKVPointCommandFrameTo(%v): %v", op.Op, err)
			}
			if !bytes.Equal(trusted, want) {
				t.Fatalf("trusted point frame mismatch for op %v\ngot  %x\nwant %x", op.Op, trusted, want)
			}
		}
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

func TestCommandWALRawKVBatchSetRIDRoundTrip(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSetRID, Key: []byte("ptr-key"), RID: 42},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload SetRID: %v", err)
	}
	var scannedRID uint64
	err = ScanRawKVBatchPayload(payload, func(op RawKVOp, key, value []byte) error {
		if op != RawKVOpSetRID || string(key) != "ptr-key" || len(value) != 8 {
			t.Fatalf("scanned op=%d key=%q valueLen=%d, want SetRID ptr-key 8", op, key, len(value))
		}
		scannedRID = binary.LittleEndian.Uint64(value)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanRawKVBatchPayload SetRID: %v", err)
	}
	if scannedRID != 42 {
		t.Fatalf("scanned RID=%d, want 42", scannedRID)
	}
	ops, err := DecodeRawKVBatchPayload(payload)
	if err != nil {
		t.Fatalf("DecodeRawKVBatchPayload SetRID: %v", err)
	}
	if len(ops) != 1 || ops[0].Op != RawKVOpSetRID || string(ops[0].Key) != "ptr-key" || ops[0].RID != 42 || len(ops[0].Value) != 0 {
		t.Fatalf("decoded ops=%+v, want SetRID ptr-key RID=42", ops)
	}
	if _, err := EncodeRawKVBatchPayload([]RawKVOperation{{Op: RawKVOpSetRID, Key: []byte("bad")}}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("EncodeRawKVBatchPayload missing RID error=%v, want ErrCorrupt", err)
	}
	if _, err := EncodeRawKVBatchPayload([]RawKVOperation{{Op: RawKVOpSetRID, Key: []byte("bad"), RID: 0}}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("EncodeRawKVBatchPayload zero RID error=%v, want ErrCorrupt", err)
	}
	if _, err := EncodeRawKVBatchPayload([]RawKVOperation{{Op: RawKVOpSetRID, Key: []byte("bad"), RID: 42, Value: []byte("ambiguous")}}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("EncodeRawKVBatchPayload SetRID value error=%v, want ErrCorrupt", err)
	}
	binary.LittleEndian.PutUint32(payload[rawKVBatchHeaderSize+5:rawKVBatchHeaderSize+9], 7)
	if _, err := DecodeRawKVBatchPayload(payload); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("DecodeRawKVBatchPayload malformed SetRID length error=%v, want ErrCorrupt", err)
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

func TestCommandWALFeatureGateRejectsCompactZeroInlineLegacyPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	value := make([]byte, 64)
	if err := w.AppendBatch([]Record{
		{Op: OpSetInline, Key: []byte("a"), Value: value, Seq: 1},
		{Op: OpSetInline, Key: []byte("b"), Value: value, Seq: 1},
	}); err != nil {
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

func TestCommandWALDuplicateLSNAcrossSegmentsFailsClosed(t *testing.T) {
	dir := t.TempDir()
	var paths []string
	for _, name := range []string{"commit-l0-000001.log", "commit-l0-000002.log"} {
		path := filepath.Join(dir, name)
		w, err := NewWriter(path)
		if err != nil {
			t.Fatalf("NewWriter %s: %v", name, err)
		}
		if err := w.AppendCommand(CommandEnvelope{LSN: 9, Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1}); err != nil {
			_ = w.Close()
			t.Fatalf("AppendCommand %s: %v", name, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close %s: %v", name, err)
		}
		paths = append(paths, path)
	}
	_, err := ScanCommandFrameSegments(paths, Options{})
	if !errors.Is(err, ErrCommandWALDuplicateLSN) {
		t.Fatalf("ScanCommandFrameSegments error=%v, want ErrCommandWALDuplicateLSN", err)
	}
}

func TestCommandWALScanSegmentsReturnsGlobalLSNOrder(t *testing.T) {
	dir := t.TempDir()
	paths := []string{
		filepath.Join(dir, "commit-l0-000001.log"),
		filepath.Join(dir, "commit-l1-000001.log"),
	}
	for i, lsn := range []uint64{2, 1} {
		w, err := NewWriter(paths[i])
		if err != nil {
			t.Fatalf("NewWriter %s: %v", paths[i], err)
		}
		if err := w.AppendCommand(CommandEnvelope{LSN: lsn, Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1}); err != nil {
			_ = w.Close()
			t.Fatalf("AppendCommand %s: %v", paths[i], err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close %s: %v", paths[i], err)
		}
	}

	frames, err := ScanCommandFrameSegments(paths, Options{})
	if err != nil {
		t.Fatalf("ScanCommandFrameSegments: %v", err)
	}
	if len(frames) != 2 {
		t.Fatalf("len(frames)=%d, want 2", len(frames))
	}
	if frames[0].LSN != 1 || frames[1].LSN != 2 {
		t.Fatalf("frames LSN order=%v, want [1 2]", []uint64{frames[0].LSN, frames[1].LSN})
	}
}

func TestCommandWALNonFinalSegmentTailFailsClosed(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "commit-l0-000001.log")
	path2 := filepath.Join(dir, "commit-l0-000002.log")
	for i, path := range []string{path1, path2} {
		w, err := NewWriter(path)
		if err != nil {
			t.Fatalf("NewWriter %s: %v", path, err)
		}
		if err := w.AppendCommand(CommandEnvelope{
			LSN:           uint64(i + 1),
			Kind:          CommandKindRawKVBatch,
			Scope:         CommandScopeRawKV,
			PayloadFormat: PayloadFormatRawKVBatchV1,
		}); err != nil {
			_ = w.Close()
			t.Fatalf("AppendCommand %s: %v", path, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close %s: %v", path, err)
		}
	}
	f, err := os.OpenFile(path1, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile append tail: %v", err)
	}
	if _, err := f.Write([]byte{0x01, 0x02}); err != nil {
		_ = f.Close()
		t.Fatalf("Write tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close tail writer: %v", err)
	}
	if _, err := ScanCommandFrames(path1, Options{}); err != nil {
		t.Fatalf("ScanCommandFrames single active segment tail: %v", err)
	}
	_, err = ScanCommandFrameSegments([]string{path1, path2}, Options{})
	if !errors.Is(err, ErrCommandWALTerminalTail) {
		t.Fatalf("ScanCommandFrameSegments error=%v, want ErrCommandWALTerminalTail", err)
	}
}

func TestCommandWALActiveSegmentTailAllowedPerLane(t *testing.T) {
	dir := t.TempDir()
	path0 := filepath.Join(dir, "commit-l0-000001.log")
	path1 := filepath.Join(dir, "commit-l1-000001.log")
	for i, path := range []string{path0, path1} {
		w, err := NewWriter(path)
		if err != nil {
			t.Fatalf("NewWriter %s: %v", path, err)
		}
		if err := w.AppendCommand(CommandEnvelope{
			LSN:           uint64(i + 1),
			Kind:          CommandKindRawKVBatch,
			Scope:         CommandScopeRawKV,
			PayloadFormat: PayloadFormatRawKVBatchV1,
		}); err != nil {
			_ = w.Close()
			t.Fatalf("AppendCommand %s: %v", path, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close %s: %v", path, err)
		}
	}
	f, err := os.OpenFile(path0, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile append tail: %v", err)
	}
	if _, err := f.Write([]byte{0x01, 0x02}); err != nil {
		_ = f.Close()
		t.Fatalf("Write tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close tail writer: %v", err)
	}
	frames, err := ScanCommandFrameSegments([]string{path0, path1}, Options{})
	if err != nil {
		t.Fatalf("ScanCommandFrameSegments per-lane active tails: %v", err)
	}
	if len(frames) != 2 {
		t.Fatalf("len(frames)=%d, want 2", len(frames))
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
