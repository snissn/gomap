package commitlog

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
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

func TestRawKVBatchPayloadBuilderPreservesEmptyPointKeyAndValue(t *testing.T) {
	var builder RawKVBatchPayloadBuilder
	if err := builder.ResetWithHint(2, 0); err != nil {
		t.Fatalf("ResetWithHint: %v", err)
	}
	keyView, valueView, err := builder.AppendSet([]byte{}, []byte{})
	if err != nil {
		t.Fatalf("AppendSet empty key/value: %v", err)
	}
	if keyView == nil || len(keyView) != 0 || valueView == nil || len(valueView) != 0 {
		t.Fatalf("AppendSet views key=%#v value=%#v, want non-nil empty views", keyView, valueView)
	}
	deleteKeyView, err := builder.AppendDelete([]byte{})
	if err != nil {
		t.Fatalf("AppendDelete empty key: %v", err)
	}
	if deleteKeyView == nil || len(deleteKeyView) != 0 {
		t.Fatalf("AppendDelete key view=%#v, want non-nil empty view", deleteKeyView)
	}
	if _, _, err := builder.AppendSet([]byte("nil-value"), nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("AppendSet nil value error=%v, want ErrCorrupt", err)
	}

	ops, err := DecodeRawKVBatchPayload(builder.Payload())
	if err != nil {
		t.Fatalf("DecodeRawKVBatchPayload: %v", err)
	}
	if len(ops) != 2 || ops[0].Op != RawKVOpSet || ops[0].Key == nil || len(ops[0].Key) != 0 || ops[0].Value == nil || len(ops[0].Value) != 0 ||
		ops[1].Op != RawKVOpDelete || ops[1].Key == nil || len(ops[1].Key) != 0 {
		t.Fatalf("decoded empty point-key ops mismatch: %+v", ops)
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

func TestRawKVBatchPayloadBuilderRetainedCapPredictsCompactZeroMaterialization(t *testing.T) {
	var nilBuilder *RawKVBatchPayloadBuilder
	if got, err := nilBuilder.RetainedCapAfterAppend(1); err != nil || got != 0 {
		t.Fatalf("nil RetainedCapAfterAppend=%d, %v; want 0, nil", got, err)
	}

	var builder RawKVBatchPayloadBuilder
	if err := builder.ResetWithHint(0, 0); err != nil {
		t.Fatalf("ResetWithHint: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, _, err := builder.AppendSet([]byte(fmt.Sprintf("zero-%03d", i)), make([]byte, 32)); err != nil {
			t.Fatalf("AppendSet compact zero %d: %v", i, err)
		}
	}
	if got := builder.RetainedCap(); got != rawKVBatchHeaderSize {
		t.Fatalf("compact zero canonical retained cap=%d, want %d", got, rawKVBatchHeaderSize)
	}

	retainedCap, err := builder.RetainedCapAfterAppendSet([]byte("nz"), []byte("x"))
	if err != nil {
		t.Fatalf("RetainedCapAfterAppendSet non-zero: %v", err)
	}
	expandedLen, ok := rawKVExpandedZeroBatchPayloadSize(3, len("zero-000")*3, 32)
	if !ok {
		t.Fatal("expanded zero batch size overflow")
	}
	wantMin := expandedLen + rawKVOpHeaderSize + len("nz") + len("x")
	if retainedCap < wantMin {
		t.Fatalf("retained cap after compact materialization=%d, want at least %d", retainedCap, wantMin)
	}
}

func TestRawKVBatchPayloadBuilderRetainedCapPredictsCompactZeroBuffers(t *testing.T) {
	var builder RawKVBatchPayloadBuilder
	if err := builder.ResetWithHint(0, 0); err != nil {
		t.Fatalf("ResetWithHint: %v", err)
	}
	retainedCap, err := builder.RetainedCapAfterAppendSet([]byte("zero"), make([]byte, 128))
	if err != nil {
		t.Fatalf("RetainedCapAfterAppendSet compact zero: %v", err)
	}
	wantMin := rawKVBatchHeaderSize + rawKVZeroBatchHeaderSize + rawKVZeroOpHeaderSizeV3 + len("zero") + 128
	if retainedCap < wantMin {
		t.Fatalf("compact zero retained cap=%d, want at least %d", retainedCap, wantMin)
	}
	if got := builder.RetainedCap(); got != rawKVBatchHeaderSize {
		t.Fatalf("prediction mutated canonical retained cap=%d, want %d", got, rawKVBatchHeaderSize)
	}
	if cap(builder.zeroPayload) != 0 || cap(builder.zeroSetValueView) != 0 {
		t.Fatalf("prediction mutated compact buffers: zeroPayload cap=%d zeroSetValueView cap=%d", cap(builder.zeroPayload), cap(builder.zeroSetValueView))
	}
}

func TestRawKVBatchPayloadBuilderRetainedBytesIncludesCompactZeroBuffers(t *testing.T) {
	var builder RawKVBatchPayloadBuilder
	if err := builder.ResetWithHint(0, 0); err != nil {
		t.Fatalf("ResetWithHint: %v", err)
	}
	largeZero := make([]byte, len(rawKVSharedZeroValueView)+1)
	if _, _, err := builder.AppendSet([]byte("zero"), largeZero); err != nil {
		t.Fatalf("AppendSet compact zero: %v", err)
	}
	if cap(builder.zeroPayload) == 0 || cap(builder.zeroSetValueView) == 0 {
		t.Fatalf("compact-zero append did not retain side buffers: zeroPayload cap=%d zeroSetValueView cap=%d", cap(builder.zeroPayload), cap(builder.zeroSetValueView))
	}
	want := cap(builder.payload) + cap(builder.zeroPayload) + cap(builder.zeroSetValueView)
	if got := builder.RetainedBytes(); got != want {
		t.Fatalf("RetainedBytes=%d, want payload+zero buffers=%d", got, want)
	}
	if got := builder.RetainedCap(); got >= builder.RetainedBytes() {
		t.Fatalf("RetainedCap=%d should not account for compact-zero side buffers retainedBytes=%d", got, builder.RetainedBytes())
	}
}

func TestRawKVBatchPayloadBuilderCompactZeroValueLeaseDetachesOnlyValueView(t *testing.T) {
	var builder RawKVBatchPayloadBuilder
	if err := builder.ResetWithHint(4, 0); err != nil {
		t.Fatalf("ResetWithHint: %v", err)
	}
	largeZero := make([]byte, len(rawKVSharedZeroValueView)+1)
	if _, _, err := builder.AppendSet([]byte("zero-1"), largeZero); err != nil {
		t.Fatalf("AppendSet zero-1: %v", err)
	}
	if _, _, err := builder.AppendSet([]byte("zero-2"), largeZero); err != nil {
		t.Fatalf("AppendSet zero-2: %v", err)
	}
	payloadCap := cap(builder.payload)
	zeroPayloadCap := cap(builder.zeroPayload)
	if payloadCap == 0 || zeroPayloadCap == 0 || cap(builder.zeroSetValueView) == 0 {
		t.Fatalf("unexpected compact-zero caps: payload=%d zeroPayload=%d zeroSetValueView=%d", payloadCap, zeroPayloadCap, cap(builder.zeroSetValueView))
	}

	chunks, mask := builder.RetainedValueByteBuffers()
	if mask != RawKVBatchPayloadBufferZeroValue {
		t.Fatalf("value lease mask=%b, want zero-value only", mask)
	}
	if len(chunks) != 1 || cap(chunks[0]) != cap(builder.zeroSetValueView) {
		t.Fatalf("value lease chunks=%d cap=%d, want one zero-value chunk cap=%d", len(chunks), cap(chunks[0]), cap(builder.zeroSetValueView))
	}
	builder.DetachRetainedValueByteBuffers(mask)
	if cap(builder.zeroSetValueView) != 0 {
		t.Fatalf("zeroSetValueView cap after detach=%d, want 0", cap(builder.zeroSetValueView))
	}
	if cap(builder.payload) != payloadCap {
		t.Fatalf("payload cap after value detach=%d, want retained %d", cap(builder.payload), payloadCap)
	}
	if cap(builder.zeroPayload) != zeroPayloadCap {
		t.Fatalf("zeroPayload cap after value detach=%d, want retained %d", cap(builder.zeroPayload), zeroPayloadCap)
	}
}

func TestRawKVBatchPayloadBuilderRevisionZeroValueLeaseUsesSharedZeroView(t *testing.T) {
	var builder RawKVBatchPayloadBuilder
	if err := builder.ResetWithHint(4, 1024); err != nil {
		t.Fatalf("ResetWithHint: %v", err)
	}
	if err := builder.EnableEntryRevisions(); err != nil {
		t.Fatalf("EnableEntryRevisions: %v", err)
	}
	if _, valueView, err := builder.AppendSet([]byte("zero-1"), make([]byte, 128)); err != nil {
		t.Fatalf("AppendSet zero-1: %v", err)
	} else if len(valueView) != 128 || !allZeroBytes(valueView) {
		t.Fatalf("zero value view len=%d allZero=%v", len(valueView), allZeroBytes(valueView))
	}
	payloadCap := cap(builder.payload)
	if payloadCap == 0 || cap(builder.zeroSetValueView) != 0 {
		t.Fatalf("unexpected revision-zero caps: payload=%d zeroSetValueView=%d", payloadCap, cap(builder.zeroSetValueView))
	}

	chunks, mask := builder.RetainedValueByteBuffers()
	if mask != 0 {
		t.Fatalf("value lease mask=%b, want no owned value buffers", mask)
	}
	if len(chunks) != 1 || cap(chunks[0]) != 0 {
		t.Fatalf("value lease chunks=%d cap=%d, want one zero-cap stable signal", len(chunks), cap(chunks[0]))
	}
	builder.DetachRetainedValueByteBuffers(mask)
	if cap(builder.payload) != payloadCap {
		t.Fatalf("revision payload cap after value detach=%d, want retained %d", cap(builder.payload), payloadCap)
	}
}

func TestRawKVBatchPayloadBuilderPrepareForReuseDropsOversizeRetainedBuffers(t *testing.T) {
	var builder RawKVBatchPayloadBuilder
	if err := builder.ResetWithHint(0, 0); err != nil {
		t.Fatalf("ResetWithHint: %v", err)
	}
	if _, _, err := builder.AppendSet([]byte("large"), bytes.Repeat([]byte{1}, 64<<10)); err != nil {
		t.Fatalf("AppendSet large: %v", err)
	}
	retained := builder.RetainedBytes()
	if retained < 64<<10 {
		t.Fatalf("retained bytes=%d, want large payload retained", retained)
	}
	if builder.PrepareForReuse(retained - 1) {
		t.Fatalf("PrepareForReuse returned true for retained=%d with max=%d", retained, retained-1)
	}
	if got := builder.RetainedBytes(); got != 0 {
		t.Fatalf("RetainedBytes after oversize drop=%d, want 0", got)
	}
	if got := builder.RetainedCap(); got != 0 {
		t.Fatalf("RetainedCap after oversize drop=%d, want 0", got)
	}
}

func TestRawKVBatchPayloadBuilderPrepareForReuseDropsInlineRevisionOffsets(t *testing.T) {
	var builder RawKVBatchPayloadBuilder
	if err := builder.ResetWithHint(2, 64); err != nil {
		t.Fatalf("ResetWithHint: %v", err)
	}
	if err := builder.EnableEntryRevisions(); err != nil {
		t.Fatalf("EnableEntryRevisions: %v", err)
	}
	if cap(builder.revisionOffsets) != rawKVRevisionOffsetInlineCap {
		t.Fatalf("revision offset cap=%d, want inline cap %d", cap(builder.revisionOffsets), rawKVRevisionOffsetInlineCap)
	}

	pooled := builder
	if !pooled.PrepareForReuse(1 << 20) {
		t.Fatal("PrepareForReuse returned false for bounded inline-revision builder")
	}
	if pooled.revisionOffsets != nil {
		t.Fatalf("pooled inline revision offsets retained with cap=%d; copied builders must drop inline aliases", cap(pooled.revisionOffsets))
	}
	if err := pooled.ResetWithHint(2, 64); err != nil {
		t.Fatalf("pooled ResetWithHint: %v", err)
	}
	if err := pooled.EnableEntryRevisions(); err != nil {
		t.Fatalf("pooled EnableEntryRevisions: %v", err)
	}
	if cap(pooled.revisionOffsets) != rawKVRevisionOffsetInlineCap {
		t.Fatalf("pooled revision offset cap=%d, want inline cap %d", cap(pooled.revisionOffsets), rawKVRevisionOffsetInlineCap)
	}
	gotBacking := &pooled.revisionOffsets[:cap(pooled.revisionOffsets)][0]
	wantBacking := &pooled.revisionOffsetInline[0]
	if gotBacking != wantBacking {
		t.Fatalf("pooled revision offsets do not point at pooled builder inline array")
	}
	if _, _, err := pooled.AppendSet([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("pooled AppendSet: %v", err)
	}
	if len(pooled.revisionOffsets) != 1 {
		t.Fatalf("pooled revision offsets len=%d, want 1 after append", len(pooled.revisionOffsets))
	}
}

func TestRawKVBatchPayloadBuilderPrepareForReuseKeepsHeapRevisionOffsets(t *testing.T) {
	var builder RawKVBatchPayloadBuilder
	opHint := rawKVRevisionOffsetInlineCap + 1
	if err := builder.ResetWithHint(opHint, 64); err != nil {
		t.Fatalf("ResetWithHint: %v", err)
	}
	if err := builder.EnableEntryRevisions(); err != nil {
		t.Fatalf("EnableEntryRevisions: %v", err)
	}
	if cap(builder.revisionOffsets) <= rawKVRevisionOffsetInlineCap {
		t.Fatalf("revision offset cap=%d, want heap allocation larger than inline cap %d", cap(builder.revisionOffsets), rawKVRevisionOffsetInlineCap)
	}
	heapCap := cap(builder.revisionOffsets)
	if !builder.PrepareForReuse(1 << 20) {
		t.Fatal("PrepareForReuse returned false for bounded heap-revision builder")
	}
	if cap(builder.revisionOffsets) != heapCap || len(builder.revisionOffsets) != 0 {
		t.Fatalf("heap revision offsets len/cap=%d/%d, want 0/%d", len(builder.revisionOffsets), cap(builder.revisionOffsets), heapCap)
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

func TestEncodeCollectionInsertBatchByIDPayloadSortedAndUnsortedMatch(t *testing.T) {
	sorted, err := EncodeCollectionInsertBatchByIDPayload("users", []CollectionDocument{
		{ID: []byte("u1"), Document: []byte(`{"name":"Ada"}`)},
		{ID: []byte("u2"), Document: []byte(`{"name":"Grace"}`)},
	})
	if err != nil {
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload sorted: %v", err)
	}
	unsorted, err := EncodeCollectionInsertBatchByIDPayload("users", []CollectionDocument{
		{ID: []byte("u2"), Document: []byte(`{"name":"Grace"}`)},
		{ID: []byte("u1"), Document: []byte(`{"name":"Ada"}`)},
	})
	if err != nil {
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload unsorted: %v", err)
	}
	if !bytes.Equal(sorted, unsorted) {
		t.Fatalf("sorted and unsorted payloads differ\nsorted   %x\nunsorted %x", sorted, unsorted)
	}
}

func TestBorrowedCommandFrameValidationRejectsDuplicateCollectionIDs(t *testing.T) {
	payload, err := EncodeCollectionInsertBatchByIDPayload("users", []CollectionDocument{
		{ID: []byte("a"), Document: []byte("1")},
		{ID: []byte("b"), Document: []byte("2")},
	})
	if err != nil {
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
	}
	frame, err := EncodeCommandFrame(CommandEnvelope{
		LSN:           1,
		Kind:          CommandKindCollectionInsertBatchByID,
		Scope:         CommandScopeCollection,
		PayloadFormat: PayloadFormatCollectionInsertBatchByIDV1,
		Payload:       payload,
	})
	if err != nil {
		t.Fatalf("EncodeCommandFrame: %v", err)
	}
	payloadStart := commandFrameHeaderSize
	off := 10 + len("users")
	firstID := payloadStart + off + 8
	off += 8 + len("a") + len("1")
	secondID := payloadStart + off + 8
	frame[secondID] = frame[firstID]

	if _, err := decodeCommandFrame(frame, true); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("borrowed decode error=%v, want ErrCorrupt", err)
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

func TestCommandWALAppendCommandV2RejectsMaxSegmentBeforeEncode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriterWithOptions(path, Options{MaxSegmentSize: int64(commandFrameHeaderSize + rawKVBatchHeaderSize - 1)})
	if err != nil {
		t.Fatalf("NewWriterWithOptions: %v", err)
	}
	defer w.Close()

	err = w.AppendCommand(CommandEnvelope{
		Version:         CommandFrameVersionV2,
		DurabilityClass: CommandDurabilityDurable,
		LSN:             1,
		Kind:            CommandKindRawKVBatch,
		Scope:           CommandScopeRawKV,
		PayloadFormat:   PayloadFormatRawKVBatchV1,
	})
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("AppendCommand V2 error=%v, want ErrRecordTooLarge", err)
	}
	if len(w.scratch) != 0 {
		t.Fatalf("scratch len=%d, want 0 after V2 pre-encode rejection", len(w.scratch))
	}
	if w.size != 0 {
		t.Fatalf("writer size=%d, want 0 after V2 pre-encode rejection", w.size)
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

func TestCommandWALFormatRejectsFrameCRCMismatch(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{{Op: RawKVOpSet, Key: []byte("k"), Value: []byte("v")}})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
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
	if len(data) == 0 {
		t.Fatal("command WAL frame fixture is empty")
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

func TestCommandWALFormatRejectsDormantExternalRefs(t *testing.T) {
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
			_, err := EncodeCommandFrame(CommandEnvelope{
				LSN:           4,
				Kind:          CommandKindRawKVBatch,
				Scope:         CommandScopeRawKV,
				PayloadFormat: PayloadFormatRawKVBatchV1,
				ExternalRefs:  []ExternalRef{ref},
			})
			if !errors.Is(err, ErrCommandWALUnsupportedExternalRef) {
				t.Fatalf("EncodeCommandFrame class=%d error=%v, want ErrCommandWALUnsupportedExternalRef", class, err)
			}

			extRefs, err := encodeExternalRefs([]ExternalRef{ref})
			if err != nil {
				t.Fatal(err)
			}
			frame := mustCommandFrameWithSections(t, payload, extRefs, nil, nil)
			if _, err := DecodeCommandFrame(frame); !errors.Is(err, ErrCommandWALUnsupportedExternalRef) {
				t.Fatalf("DecodeCommandFrame class=%d error=%v, want ErrCommandWALUnsupportedExternalRef", class, err)
			}
		})
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

func TestWriteRawKVBatchPayloadToMatchesSliceEncoderAndBoundsDestination(t *testing.T) {
	ops := []RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("alpha"), Value: []byte("one"), Revision: 11},
		{Op: RawKVOpDelete, Key: []byte("bravo"), Revision: 12},
		{Op: RawKVOpSetRID, Key: []byte("external"), RID: 42, Revision: 13},
		{Op: RawKVOpDeleteRange, Key: nil, Value: []byte("omega")},
		{Op: RawKVOpDeleteRange, Key: []byte("prefix"), Value: nil},
	}
	scan := rawKVOperationSliceScanner(ops)
	plan, err := PlanRawKVBatchPayloadScan(scan)
	if err != nil {
		t.Fatalf("PlanRawKVBatchPayloadScan: %v", err)
	}
	want := encodeRawKVBatchPayloadWithPiecesForTest(t, plan, scan)
	dst := bytes.Repeat([]byte{0xff}, plan.PayloadLen+16)
	n, err := writeRawKVBatchPayloadTo(dst[:plan.PayloadLen], plan, scan)
	if err != nil {
		t.Fatalf("writeRawKVBatchPayloadTo: %v", err)
	}
	if n != len(want) {
		t.Fatalf("written bytes=%d want %d", n, len(want))
	}
	if !bytes.Equal(dst[:n], want) {
		t.Fatalf("payload mismatch\ngot  %x\nwant %x", dst[:n], want)
	}
	if !bytes.Equal(dst[plan.PayloadLen:], bytes.Repeat([]byte{0xff}, 16)) {
		t.Fatalf("writeRawKVBatchPayloadTo wrote beyond planned payload")
	}
	if _, err := writeRawKVBatchPayloadTo(make([]byte, plan.PayloadLen-1), plan, scan); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("short destination error=%v, want ErrCorrupt", err)
	}
}

func TestWriteRawKVBatchPayloadToOverwritesReusedBufferPrefix(t *testing.T) {
	largeOps := []RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("alpha"), Value: []byte("one")},
		{Op: RawKVOpSetRID, Key: []byte("external"), RID: 42, Revision: 9},
	}
	largeScan := rawKVOperationSliceScanner(largeOps)
	largePlan, err := PlanRawKVBatchPayloadScan(largeScan)
	if err != nil {
		t.Fatalf("PlanRawKVBatchPayloadScan large: %v", err)
	}
	largePayload := encodeRawKVBatchPayloadWithPiecesForTest(t, largePlan, largeScan)
	buf := bytes.Repeat([]byte{0xee}, len(largePayload))
	if _, err := writeRawKVBatchPayloadTo(buf, largePlan, largeScan); err != nil {
		t.Fatalf("writeRawKVBatchPayloadTo large: %v", err)
	}

	smallOps := []RawKVOperation{{Op: RawKVOpDelete, Key: []byte("alpha")}}
	smallScan := rawKVOperationSliceScanner(smallOps)
	smallPlan, err := PlanRawKVBatchPayloadScan(smallScan)
	if err != nil {
		t.Fatalf("PlanRawKVBatchPayloadScan small: %v", err)
	}
	smallPayload := encodeRawKVBatchPayloadWithPiecesForTest(t, smallPlan, smallScan)
	n, err := writeRawKVBatchPayloadTo(buf[:smallPlan.PayloadLen], smallPlan, smallScan)
	if err != nil {
		t.Fatalf("writeRawKVBatchPayloadTo small: %v", err)
	}
	if n != len(smallPayload) || !bytes.Equal(buf[:n], smallPayload) {
		t.Fatalf("reused buffer payload mismatch\ngot  %x\nwant %x", buf[:n], smallPayload)
	}
	if !bytes.Equal(buf[n:], largePayload[n:]) {
		t.Fatalf("writeRawKVBatchPayloadTo touched bytes outside reused prefix")
	}
}

func TestWriteRawKVOperationPayloadToDeleteRangeBoundHeaders(t *testing.T) {
	for _, tc := range []struct {
		name         string
		start        []byte
		end          []byte
		wantStartLen uint32
		wantEndLen   uint32
		wantStartNil bool
		wantEndNil   bool
	}{
		{
			name:         "both_unbounded",
			start:        nil,
			end:          nil,
			wantStartLen: rawKVNilRangeBoundLenUint32,
			wantEndLen:   rawKVNilRangeBoundLenUint32,
			wantStartNil: true,
			wantEndNil:   true,
		},
		{
			name:         "nil_start",
			start:        nil,
			end:          []byte("omega"),
			wantStartLen: rawKVNilRangeBoundLenUint32,
			wantEndLen:   uint32(len("omega")),
			wantStartNil: true,
		},
		{
			name:         "empty_start",
			start:        []byte{},
			end:          []byte("omega"),
			wantStartLen: 0,
			wantEndLen:   uint32(len("omega")),
		},
		{
			name:         "nil_end",
			start:        []byte("prefix"),
			end:          nil,
			wantStartLen: uint32(len("prefix")),
			wantEndLen:   rawKVNilRangeBoundLenUint32,
			wantEndNil:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := RawKVOperation{Op: RawKVOpDeleteRange, Key: tc.start, Value: tc.end}
			keyBytes, valueBytes, err := rawKVOperationPayloadLens(&op)
			if err != nil {
				t.Fatalf("rawKVOperationPayloadLens: %v", err)
			}
			opLen := rawKVOperationHeaderSize(false) + keyBytes + valueBytes
			dst := bytes.Repeat([]byte{0xcc}, opLen+8)
			n, err := writeRawKVOperationPayloadTo(dst[:opLen], op, false)
			if err != nil {
				t.Fatalf("writeRawKVOperationPayloadTo: %v", err)
			}
			if n != opLen {
				t.Fatalf("written bytes=%d want %d", n, opLen)
			}
			if got := dst[0]; got != byte(RawKVOpDeleteRange) {
				t.Fatalf("op byte=%d want %d", got, RawKVOpDeleteRange)
			}
			if got := binary.LittleEndian.Uint32(dst[1:5]); got != tc.wantStartLen {
				t.Fatalf("start encoded len=%d want %d", got, tc.wantStartLen)
			}
			if got := binary.LittleEndian.Uint32(dst[5:9]); got != tc.wantEndLen {
				t.Fatalf("end encoded len=%d want %d", got, tc.wantEndLen)
			}
			if !bytes.Equal(dst[opLen:], bytes.Repeat([]byte{0xcc}, 8)) {
				t.Fatalf("writeRawKVOperationPayloadTo wrote beyond operation payload")
			}

			payload := make([]byte, rawKVBatchHeaderSize+n)
			binary.LittleEndian.PutUint16(payload[0:2], rawKVBatchPayloadVersion)
			binary.LittleEndian.PutUint32(payload[2:6], 1)
			copy(payload[rawKVBatchHeaderSize:], dst[:n])
			var gotStart, gotEnd []byte
			if err := ScanRawKVBatchPayload(payload, func(op RawKVOp, key, value []byte) error {
				if op != RawKVOpDeleteRange {
					t.Fatalf("scanned op=%v want DeleteRange", op)
				}
				gotStart = key
				gotEnd = value
				return nil
			}); err != nil {
				t.Fatalf("ScanRawKVBatchPayload: %v", err)
			}
			if tc.wantStartNil {
				if gotStart != nil {
					t.Fatalf("start bound=%q want nil", gotStart)
				}
			} else if gotStart == nil || !bytes.Equal(gotStart, tc.start) {
				t.Fatalf("start bound=%q want %q", gotStart, tc.start)
			}
			if tc.wantEndNil {
				if gotEnd != nil {
					t.Fatalf("end bound=%q want nil", gotEnd)
				}
			} else if gotEnd == nil || !bytes.Equal(gotEnd, tc.end) {
				t.Fatalf("end bound=%q want %q", gotEnd, tc.end)
			}
		})
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

func TestRawKVBatchPayloadBuilderStampsEntryRevisions(t *testing.T) {
	builder := NewRawKVBatchPayloadBuilder(3, len("alpha")+len("one")+len("bravo")+len("charlie"))
	if err := builder.EnableEntryRevisions(); err != nil {
		t.Fatalf("EnableEntryRevisions: %v", err)
	}
	if _, _, err := builder.AppendSet([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("AppendSet: %v", err)
	}
	if _, err := builder.AppendDelete([]byte("bravo")); err != nil {
		t.Fatalf("AppendDelete: %v", err)
	}
	if _, _, err := builder.Append(RawKVOperation{Op: RawKVOpSetRID, Key: []byte("charlie"), RID: 42, Revision: 33}); err != nil {
		t.Fatalf("Append SetRID: %v", err)
	}
	if err := builder.StampEntryRevisions(func(emit func(uint64) error) error {
		for _, revision := range []uint64{11, 22, 33} {
			if err := emit(revision); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("StampEntryRevisions: %v", err)
	}

	payload := builder.Payload()
	if got := binary.LittleEndian.Uint16(payload[0:2]); got != rawKVBatchPayloadVersionWithRevisions {
		t.Fatalf("payload version=%d want %d", got, rawKVBatchPayloadVersionWithRevisions)
	}
	var got []RawKVOperation
	err := ScanRawKVBatchPayloadWithRevision(payload, func(op RawKVOp, key, value []byte, revision uint64) error {
		entry := RawKVOperation{Op: op, Key: cloneBytesPreserveEmpty(key), Revision: revision}
		if op == RawKVOpSetRID {
			entry.RID = binary.LittleEndian.Uint64(value)
		} else {
			entry.Value = cloneBytesPreserveEmpty(value)
		}
		got = append(got, entry)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanRawKVBatchPayloadWithRevision: %v", err)
	}
	if len(got) != 3 ||
		got[0].Op != RawKVOpSet || string(got[0].Key) != "alpha" || string(got[0].Value) != "one" || got[0].Revision != 11 ||
		got[1].Op != RawKVOpDelete || string(got[1].Key) != "bravo" || got[1].Revision != 22 ||
		got[2].Op != RawKVOpSetRID || string(got[2].Key) != "charlie" || got[2].RID != 42 || got[2].Revision != 33 {
		t.Fatalf("stamped ops=%+v, want revisions preserved", got)
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

func TestWriterAppendRawKVBatchPayloadScanCommandDirectBuffered(t *testing.T) {
	ops := []RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("alpha"), Value: []byte("one")},
		{Op: RawKVOpDelete, Key: []byte("bravo")},
		{Op: RawKVOpDeleteRange, Key: nil, Value: []byte("delta")},
		{Op: RawKVOpSetRID, Key: []byte("external"), RID: 42},
	}
	payload, err := EncodeRawKVBatchPayload(ops)
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	scan := rawKVOperationSliceScanner(ops)
	plan, err := PlanRawKVBatchPayloadScan(scan)
	if err != nil {
		t.Fatalf("PlanRawKVBatchPayloadScan: %v", err)
	}
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriterWithOptions(path, Options{DeferredCommandBufferSize: 4096})
	if err != nil {
		t.Fatalf("NewWriterWithOptions: %v", err)
	}
	if err := w.AppendRawKVBatchPayloadScanCommandDirectTrusted(7, 3, plan, scan); err != nil {
		_ = w.Close()
		t.Fatalf("AppendRawKVBatchPayloadScanCommandDirectTrusted: %v", err)
	}
	if got := len(w.commandBuf); got == 0 {
		_ = w.Close()
		t.Fatal("direct scan command frame was not buffered")
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	assertRawKVCommandFramePayload(t, path, 7, 3, payload)
}

func TestWriterAppendRawKVBatchPayloadScanCommandDirectBufferedRollsBackOnScanMismatch(t *testing.T) {
	plannedOps := []RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("alpha"), Value: []byte("one")},
	}
	plannedScan := rawKVOperationSliceScanner(plannedOps)
	plan, err := PlanRawKVBatchPayloadScan(plannedScan)
	if err != nil {
		t.Fatalf("PlanRawKVBatchPayloadScan: %v", err)
	}
	mismatchedScan := rawKVOperationSliceScanner([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("alpha"), Value: []byte("one")},
		{Op: RawKVOpDelete, Key: []byte("bravo")},
	})

	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriterWithOptions(path, Options{DeferredCommandBufferSize: 4096})
	if err != nil {
		t.Fatalf("NewWriterWithOptions: %v", err)
	}
	if err := w.AppendRawKVBatchPayloadScanCommandDirectTrusted(7, 3, plan, mismatchedScan); !errors.Is(err, ErrCorrupt) {
		_ = w.Close()
		t.Fatalf("mismatched scan append error=%v, want ErrCorrupt", err)
	}
	if got := len(w.commandBuf); got != 0 {
		_ = w.Close()
		t.Fatalf("command buffer len after failed append=%d want 0", got)
	}

	payload, err := EncodeRawKVBatchPayload(plannedOps)
	if err != nil {
		_ = w.Close()
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	if err := w.AppendRawKVBatchPayloadScanCommandDirectTrusted(8, 4, plan, plannedScan); err != nil {
		_ = w.Close()
		t.Fatalf("valid append after rollback: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	assertRawKVCommandFramePayload(t, path, 8, 4, payload)
}

func TestWriterAppendRawKVBatchPayloadScanCommandDirectLarge(t *testing.T) {
	ops := []RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("large"), Value: bytes.Repeat([]byte("x"), directCommandPayloadMinLen)},
		{Op: RawKVOpDeleteRange, Key: []byte("m"), Value: nil},
	}
	payload, err := EncodeRawKVBatchPayload(ops)
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	scan := rawKVOperationSliceScanner(ops)
	plan, err := PlanRawKVBatchPayloadScan(scan)
	if err != nil {
		t.Fatalf("PlanRawKVBatchPayloadScan: %v", err)
	}
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriterWithOptions(path, Options{DeferredCommandBufferSize: 64 << 20})
	if err != nil {
		t.Fatalf("NewWriterWithOptions: %v", err)
	}
	if err := w.AppendRawKVBatchPayloadScanCommandDirectTrusted(9, 4, plan, scan); err != nil {
		_ = w.Close()
		t.Fatalf("AppendRawKVBatchPayloadScanCommandDirectTrusted: %v", err)
	}
	if got := len(w.commandBuf); got != 0 {
		_ = w.Close()
		t.Fatalf("deferred command buffer len=%d, want 0 for large direct scan frame", got)
	}
	if got := w.Size(); got == 0 {
		_ = w.Close()
		t.Fatal("writer size after large direct scan append=0, want direct bytes accounted")
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	assertRawKVCommandFramePayload(t, path, 9, 4, payload)
}

func rawKVOperationSliceScanner(ops []RawKVOperation) RawKVBatchOperationScanner {
	return func(emit func(RawKVOperation) error) error {
		for i := range ops {
			if err := emit(ops[i]); err != nil {
				return err
			}
		}
		return nil
	}
}

func encodeRawKVBatchPayloadWithPiecesForTest(t *testing.T, plan RawKVBatchPayloadPlan, scan RawKVBatchOperationScanner) []byte {
	t.Helper()
	payload := make([]byte, plan.PayloadLen)
	off := 0
	if err := writeRawKVBatchPayloadPieces(plan, scan, func(part []byte) error {
		copy(payload[off:], part)
		off += len(part)
		return nil
	}); err != nil {
		t.Fatalf("writeRawKVBatchPayloadPieces: %v", err)
	}
	if off != plan.PayloadLen {
		t.Fatalf("piece payload bytes=%d want %d", off, plan.PayloadLen)
	}
	return payload
}

func assertRawKVCommandFramePayload(t *testing.T, path string, lsn, baseAppliedLSN uint64, payload []byte) {
	t.Helper()
	r, err := NewReader(path)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	env, err := r.ReadCommandFrame()
	if err != nil {
		t.Fatalf("ReadCommandFrame: %v", err)
	}
	if env.LSN != lsn || env.BaseAppliedLSN != baseAppliedLSN || env.Kind != CommandKindRawKVBatch || env.Scope != CommandScopeRawKV || env.PayloadFormat != PayloadFormatRawKVBatchV1 {
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

func TestWriterDeferredCommandBufferAllocatesLazily(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("small"), Value: []byte("value")},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriterWithOptions(path, Options{DeferredCommandBufferSize: 64 << 20})
	if err != nil {
		t.Fatalf("NewWriterWithOptions: %v", err)
	}
	defer w.Close()
	if got := w.commandBufferLimit(); got != 64<<20 {
		t.Fatalf("commandBufferLimit=%d want %d", got, 64<<20)
	}
	if got := cap(w.commandBuf); got != 0 {
		t.Fatalf("deferred command buffer cap after open=%d, want 0", got)
	}
	if err := w.AppendRawKVBatchPayloadCommandDirectTrusted(1, 0, payload); err != nil {
		t.Fatalf("AppendRawKVBatchPayloadCommandDirectTrusted: %v", err)
	}
	if got := len(w.commandBuf); got == 0 {
		t.Fatal("small command frame was not buffered")
	}
	if got := cap(w.commandBuf); got >= 1<<20 {
		t.Fatalf("deferred command buffer cap after first small frame=%d, want below 1MiB", got)
	}
}

func TestWriterDeferredCommandBufferTrimsRetainedCapacityAfterFlush(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("small"), Value: bytes.Repeat([]byte("x"), 2048)},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriterWithOptions(path, Options{DeferredCommandBufferSize: 64 << 10, DeferredCommandBufferRetainSize: 1024})
	if err != nil {
		t.Fatalf("NewWriterWithOptions: %v", err)
	}
	defer w.Close()
	for i := uint64(1); i <= 8; i++ {
		if err := w.AppendRawKVBatchPayloadCommandDirectTrusted(i, 0, payload); err != nil {
			t.Fatalf("AppendRawKVBatchPayloadCommandDirectTrusted %d: %v", i, err)
		}
	}
	before := w.BufferStats()
	if before.CommandBufferCapacity <= before.CommandBufferRetainLimit {
		t.Fatalf("command buffer cap before flush=%d, want above retain limit %d", before.CommandBufferCapacity, before.CommandBufferRetainLimit)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	after := w.BufferStats()
	if after.CommandBufferLength != 0 {
		t.Fatalf("command buffer len after flush=%d, want 0", after.CommandBufferLength)
	}
	if after.CommandBufferCapacity != 1024 {
		t.Fatalf("command buffer cap after flush=%d, want retained cap 1024", after.CommandBufferCapacity)
	}
	if after.CommandBufferTrimCount != 1 {
		t.Fatalf("command buffer trim count=%d, want 1", after.CommandBufferTrimCount)
	}
	if after.CommandBufferDroppedBytes == 0 {
		t.Fatal("command buffer dropped bytes=0, want dropped capacity accounted")
	}
}

func TestWriterScratchAllocatesLazily(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriterWithOptions(path, Options{})
	if err != nil {
		t.Fatalf("NewWriterWithOptions: %v", err)
	}
	defer w.Close()
	if got := cap(w.scratch); got != 0 {
		t.Fatalf("scratch cap after open=%d, want 0", got)
	}
	if got := w.BufferStats().ScratchCapacity; got != 0 {
		t.Fatalf("scratch stat after open=%d, want 0", got)
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
		{Op: RawKVOpDeleteRange, Key: nil, Value: []byte("delta")},
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
		{Op: RawKVOpDeleteRange, Key: []byte("charlie"), Value: nil},
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

func TestEncodeTrustedRawKVPointCommandFrameWithRevisionMatchesEnvelopeEncoder(t *testing.T) {
	for _, op := range []RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("alpha"), Value: []byte("one"), Revision: 91},
		{Op: RawKVOpDelete, Key: []byte("bravo"), Revision: 92},
	} {
		payload, err := EncodeRawKVBatchPayload([]RawKVOperation{op})
		if err != nil {
			t.Fatalf("EncodeRawKVBatchPayload(%v): %v", op.Op, err)
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
		got, err := encodeTrustedRawKVPointCommandFrameWithRevisionTo(nil, 7, 3, op.Op, op.Key, op.Value, op.Revision)
		if err != nil {
			t.Fatalf("encodeTrustedRawKVPointCommandFrameWithRevisionTo(%v): %v", op.Op, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("trusted revision point frame mismatch for op %v\ngot  %x\nwant %x", op.Op, got, want)
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

func TestCommandWALRawKVBatchRevisionRoundTrip(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("a"), Value: []byte("1"), Revision: 11},
		{Op: RawKVOpDelete, Key: []byte("b"), Revision: 12},
		{Op: RawKVOpSetRID, Key: []byte("c"), RID: 42, Revision: 13},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload revisions: %v", err)
	}
	if got := binary.LittleEndian.Uint16(payload[0:2]); got != rawKVBatchPayloadVersionWithRevisions {
		t.Fatalf("payload version=%d want revision payload version %d", got, rawKVBatchPayloadVersionWithRevisions)
	}

	var scanned []RawKVOperation
	err = ScanRawKVBatchPayloadWithRevision(payload, func(op RawKVOp, key, value []byte, revision uint64) error {
		entry := RawKVOperation{Op: op, Key: cloneBytesPreserveEmpty(key), Revision: revision}
		if op == RawKVOpSetRID {
			entry.RID = binary.LittleEndian.Uint64(value)
		} else {
			entry.Value = cloneBytesPreserveEmpty(value)
		}
		scanned = append(scanned, entry)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanRawKVBatchPayloadWithRevision: %v", err)
	}
	decoded, err := DecodeRawKVBatchPayload(payload)
	if err != nil {
		t.Fatalf("DecodeRawKVBatchPayload revisions: %v", err)
	}
	for name, got := range map[string][]RawKVOperation{"scanned": scanned, "decoded": decoded} {
		if len(got) != 3 ||
			got[0].Op != RawKVOpSet || string(got[0].Key) != "a" || string(got[0].Value) != "1" || got[0].Revision != 11 ||
			got[1].Op != RawKVOpDelete || string(got[1].Key) != "b" || len(got[1].Value) != 0 || got[1].Revision != 12 ||
			got[2].Op != RawKVOpSetRID || string(got[2].Key) != "c" || got[2].RID != 42 || len(got[2].Value) != 0 || got[2].Revision != 13 {
			t.Fatalf("%s ops=%+v, want revisions preserved", name, got)
		}
	}
	if _, err := EncodeRawKVBatchPayload([]RawKVOperation{{Op: RawKVOpDeleteRange, Key: []byte("a"), Value: []byte("z"), Revision: 1}}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("EncodeRawKVBatchPayload revision range delete error=%v, want ErrCorrupt", err)
	}
}

func TestCommandWALRawKVBatchMaterializedRIDV2RoundTripAndFormatGate(t *testing.T) {
	value := []byte("persistent-value")
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{{
		Op:       RawKVOpSetMaterializedRID,
		Key:      []byte("materialized"),
		Value:    value,
		RID:      42,
		Revision: 13,
	}})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload materialized RID: %v", err)
	}
	legacyV2Envelope := CommandEnvelope{
		LSN:           1,
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV2,
		Payload:       payload,
	}
	if _, err := EncodeCommandFrame(legacyV2Envelope); !errors.Is(err, ErrCommandWALUnsupportedVersion) {
		t.Fatalf("legacy frame with V2 payload error=%v, want ErrCommandWALUnsupportedVersion", err)
	}

	legacyPath := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	legacy, err := NewWriter(legacyPath)
	if err != nil {
		t.Fatalf("NewWriter legacy V2 payload: %v", err)
	}
	if err := legacy.AppendCommand(legacyV2Envelope); !errors.Is(err, ErrCommandWALUnsupportedVersion) {
		_ = legacy.Close()
		t.Fatalf("AppendCommand legacy V2 payload error=%v, want ErrCommandWALUnsupportedVersion", err)
	}
	if err := legacy.AppendCommand(CommandEnvelope{
		LSN:           2,
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		_ = legacy.Close()
		t.Fatalf("AppendCommand valid V1 after rejected V2 payload: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("Close legacy V2 payload writer: %v", err)
	}

	legacyFrame, err := EncodeCommandFrame(CommandEnvelope{
		LSN:           3,
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
	})
	if err != nil {
		t.Fatalf("EncodeCommandFrame legacy fixture: %v", err)
	}
	binary.LittleEndian.PutUint16(legacyFrame[52:54], uint16(PayloadFormatRawKVBatchV2))
	if _, err := DecodeCommandFrame(legacyFrame); !errors.Is(err, ErrCommandWALUnsupportedVersion) {
		t.Fatalf("DecodeCommandFrame legacy V2 payload error=%v, want ErrCommandWALUnsupportedVersion", err)
	}

	if _, err := EncodeCommandFrameV2(CommandEnvelope{
		Version:         CommandFrameVersionV2,
		DurabilityClass: CommandDurabilityDurable,
		LSN:             1,
		Kind:            CommandKindRawKVBatch,
		Scope:           CommandScopeRawKV,
		PayloadFormat:   PayloadFormatRawKVBatchV1,
		Payload:         payload,
	}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("V1 materialized RID error=%v, want ErrCorrupt", err)
	}
	directPath := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	direct, err := NewWriter(directPath)
	if err != nil {
		t.Fatalf("NewWriter direct materialized RID: %v", err)
	}
	if err := direct.AppendRawKVSingleCommandDirect(1, 0, RawKVOperation{
		Op: RawKVOpSetMaterializedRID, Key: []byte("materialized"), Value: value, RID: 42,
	}); !errors.Is(err, ErrCommandWALUnsupportedVersion) {
		_ = direct.Close()
		t.Fatalf("direct single materialized RID error=%v, want ErrCommandWALUnsupportedVersion", err)
	}
	if err := direct.Close(); err != nil {
		t.Fatalf("Close direct materialized RID writer: %v", err)
	}

	frame, err := EncodeCommandFrameV2(CommandEnvelope{
		Version:         CommandFrameVersionV2,
		DurabilityClass: CommandDurabilityDurable,
		LSN:             1,
		Kind:            CommandKindRawKVBatch,
		Scope:           CommandScopeRawKV,
		PayloadFormat:   PayloadFormatRawKVBatchV2,
		Payload:         payload,
	})
	if err != nil {
		t.Fatalf("EncodeCommandFrameV2 materialized RID: %v", err)
	}
	env, err := DecodeCommandFrameV2(frame)
	if err != nil {
		t.Fatalf("DecodeCommandFrameV2 materialized RID: %v", err)
	}
	ops, err := DecodeRawKVBatchPayload(env.Payload)
	if err != nil {
		t.Fatalf("DecodeRawKVBatchPayload materialized RID: %v", err)
	}
	if len(ops) != 1 || ops[0].Op != RawKVOpSetMaterializedRID ||
		string(ops[0].Key) != "materialized" || !bytes.Equal(ops[0].Value, value) ||
		ops[0].RID != 42 || ops[0].Revision != 13 {
		t.Fatalf("decoded materialized RID ops=%+v", ops)
	}
	fence, err := ExternalRefFenceV1FromRawKVPayload(env.Payload)
	if err != nil {
		t.Fatalf("ExternalRefFenceV1FromRawKVPayload: %v", err)
	}
	if fence.Count != 0 {
		t.Fatalf("materialized RID external fence count=%d, want 0", fence.Count)
	}
	corrupt := append([]byte(nil), frame...)
	// Raw frame bytes do not carry their segment CRC; Reader verifies that
	// outer checksum. Corrupt the materialized RID prefix here so the strict
	// frame decoder still exercises its V2 payload-shape fail-closed path.
	ridOffset := commandFrameHeaderSize + rawKVBatchHeaderSize + rawKVOpRevisionHeaderSize + len("materialized")
	clear(corrupt[ridOffset : ridOffset+8])
	if _, err := DecodeCommandFrameV2(corrupt); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("zero-RID V2 materialized frame error=%v, want ErrCorrupt", err)
	}
	if _, err := DecodeCommandFrameV2(frame[:len(frame)-1]); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("truncated V2 materialized frame error=%v, want ErrCorrupt", err)
	}

	for name, op := range map[string]RawKVOperation{
		"zero-rid":   {Op: RawKVOpSetMaterializedRID, Key: []byte("k"), Value: value},
		"nil-value":  {Op: RawKVOpSetMaterializedRID, Key: []byte("k"), RID: 42},
		"nil-key":    {Op: RawKVOpSetMaterializedRID, Value: value, RID: 42},
		"unknown-op": {Op: RawKVOp(0xff), Key: []byte("k"), Value: value, RID: 42},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := EncodeRawKVBatchPayload([]RawKVOperation{op}); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("EncodeRawKVBatchPayload error=%v, want ErrCorrupt", err)
			}
		})
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

func TestCommandWALRawKVBatchDeleteRangeEncodeDecodeScan(t *testing.T) {
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("a"), Value: []byte("1")},
		{Op: RawKVOpDeleteRange, Key: nil, Value: []byte("c")},
		{Op: RawKVOpDeleteRange, Key: []byte("x"), Value: nil},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	var scanned []RawKVOperation
	if err := ScanRawKVBatchPayload(payload, func(op RawKVOp, key, value []byte) error {
		scanned = append(scanned, RawKVOperation{Op: op, Key: cloneBytesPreserveEmpty(key), Value: cloneBytesPreserveEmpty(value)})
		return nil
	}); err != nil {
		t.Fatalf("ScanRawKVBatchPayload: %v", err)
	}
	decoded, err := DecodeRawKVBatchPayload(payload)
	if err != nil {
		t.Fatalf("DecodeRawKVBatchPayload: %v", err)
	}
	for name, ops := range map[string][]RawKVOperation{"scanned": scanned, "decoded": decoded} {
		if len(ops) != 3 {
			t.Fatalf("%s ops=%d want 3", name, len(ops))
		}
		if ops[1].Op != RawKVOpDeleteRange || ops[1].Key != nil || !bytes.Equal(ops[1].Value, []byte("c")) {
			t.Fatalf("%s op[1]=%+v", name, ops[1])
		}
		if ops[2].Op != RawKVOpDeleteRange || !bytes.Equal(ops[2].Key, []byte("x")) || ops[2].Value != nil {
			t.Fatalf("%s op[2]=%+v", name, ops[2])
		}
	}
}

func TestCommandWALRawKVBatchDeleteRangeRejectsEmptyReversed(t *testing.T) {
	bad := []RawKVOperation{
		{Op: RawKVOpDeleteRange, Key: []byte("b"), Value: []byte("b")},
		{Op: RawKVOpDeleteRange, Key: []byte("z"), Value: []byte("a")},
		{Op: RawKVOpDeleteRange, Key: nil, Value: []byte{}},
	}
	for _, op := range bad {
		if _, err := EncodeRawKVBatchPayload([]RawKVOperation{op}); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("EncodeRawKVBatchPayload(%+v) err=%v want ErrCorrupt", op, err)
		}
	}
}

func TestCommandWALRawKVBatchDeleteRangeMalformedDecodeFailsClosed(t *testing.T) {
	makePayload := func(startLen, endLen uint32, startBytes, endBytes []byte) []byte {
		payload := make([]byte, rawKVBatchHeaderSize+rawKVOpHeaderSize+len(startBytes)+len(endBytes))
		binary.LittleEndian.PutUint16(payload[0:2], rawKVBatchPayloadVersion)
		binary.LittleEndian.PutUint32(payload[2:6], 1)
		off := rawKVBatchHeaderSize
		payload[off] = byte(RawKVOpDeleteRange)
		binary.LittleEndian.PutUint32(payload[off+1:off+5], startLen)
		binary.LittleEndian.PutUint32(payload[off+5:off+9], endLen)
		off += rawKVOpHeaderSize
		copy(payload[off:], startBytes)
		off += len(startBytes)
		copy(payload[off:], endBytes)
		return payload
	}

	cases := map[string][]byte{
		"nil-start-empty-end":  makePayload(rawKVNilRangeBoundLenUint32, 0, nil, nil),
		"reversed-bounds":      makePayload(1, 1, []byte("z"), []byte("a")),
		"sentinel-extra-bytes": makePayload(rawKVNilRangeBoundLenUint32, rawKVNilRangeBoundLenUint32, []byte("x"), nil),
	}
	for name, payload := range cases {
		if err := ScanRawKVBatchPayload(payload, nil); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("%s ScanRawKVBatchPayload err=%v want ErrCorrupt", name, err)
		}
		if _, err := DecodeRawKVBatchPayload(payload); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("%s DecodeRawKVBatchPayload err=%v want ErrCorrupt", name, err)
		}
	}
}
