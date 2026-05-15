package commitlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
)

const (
	CommandFrameVersion uint16 = 1

	CommandWALNonCriticalFlagStart uint64 = 1 << 32

	commandFrameHeaderSize = 4 + 2 + 2 + 2 + 2 + 8 + 8 + 8 + 8 + 8 + 2 + 2 + 4 + 4 + 4 + 4 + sha256.Size
	rawKVBatchHeaderSize   = 2 + 4
	rawKVOpHeaderSize      = 1 + 4 + 4

	externalRefEncodedFixedSize      = 2 + 2 + 4 + 8 + 8 + 8 + sha256.Size
	commandExtensionEncodedFixedSize = 2 + 2 + 4
)

var commandFrameMagic = [4]byte{'T', 'C', 'W', '1'}

const commandWALCriticalFlagsMask uint64 = CommandWALNonCriticalFlagStart - 1

// CommandKind identifies the deterministic command payload schema carried by a
// command WAL frame.
type CommandKind uint16

const (
	CommandKindRawKVBatch CommandKind = 1

	// PR1 fixtures reserve the collection/catalog command families without
	// enabling production replay for them.
	CommandKindCollectionInsertBatchByID  CommandKind = 100
	CommandKindCatalogMutationPlaceholder CommandKind = 200
)

// CommandScope identifies which logical TreeDB surface a command mutates.
type CommandScope uint16

const (
	CommandScopeRawKV CommandScope = iota + 1
	CommandScopeCollection
	CommandScopeCatalog
)

// PayloadFormat identifies the canonical payload encoding inside the envelope.
type PayloadFormat uint16

const (
	PayloadFormatRawKVBatchV1            PayloadFormat = 1
	PayloadFormatNativeWireDeterministic PayloadFormat = 2
)

// RawKVOp is a deterministic raw key/value mutation inside a RawKVBatch
// command payload. The command frame LSN is the batch identity; individual ops
// intentionally do not carry their own sequence numbers.
type RawKVOp byte

const (
	RawKVOpSet RawKVOp = iota + 1
	RawKVOpDelete
)

type RawKVOperation struct {
	Op    RawKVOp
	Key   []byte
	Value []byte
}

type ExternalRefClass uint16

const (
	ExternalRefValueLog ExternalRefClass = iota + 1
	ExternalRefLeafLog
	ExternalRefPayloadFile
)

type ExternalRef struct {
	Class  ExternalRefClass
	Flags  uint16
	FileID uint64
	Offset uint64
	Length uint64
	Digest [32]byte
	Path   []byte
}

type CommandExtension struct {
	Type    uint16
	Payload []byte
}

type CommandEnvelope struct {
	Version          uint16
	LSN              uint64
	Kind             CommandKind
	Scope            CommandScope
	FeatureFlags     uint64
	CatalogEpoch     uint64
	SchemaEpoch      uint64
	BaseAppliedLSN   uint64
	PayloadFormat    PayloadFormat
	Payload          []byte
	PayloadDigest    [32]byte
	ExternalRefs     []ExternalRef
	Preconditions    []CommandExtension
	ResultAssertions []CommandExtension
}

func EncodeCommandFrame(env CommandEnvelope) ([]byte, error) {
	return encodeCommandFrameTo(nil, env)
}

func commandFrameEncodedSize(env CommandEnvelope) (int, error) {
	payloadLen := len(env.Payload)
	if env.Kind == CommandKindRawKVBatch && env.Payload == nil {
		payloadLen = rawKVBatchHeaderSize
	}
	extRefsLen, err := externalRefsEncodedLen(env.ExternalRefs)
	if err != nil {
		return 0, err
	}
	preconditionsLen, err := commandExtensionsEncodedLen(env.Preconditions)
	if err != nil {
		return 0, err
	}
	assertionsLen, err := commandExtensionsEncodedLen(env.ResultAssertions)
	if err != nil {
		return 0, err
	}
	return commandFrameEncodedSizeFromLengths(payloadLen, extRefsLen, preconditionsLen, assertionsLen)
}

func commandFrameEncodedSizeFromLengths(payloadLen, extRefsLen, preconditionsLen, assertionsLen int) (int, error) {
	total := commandFrameHeaderSize
	var err error
	if total, err = addCommandFrameEncodedSectionLen(total, payloadLen); err != nil {
		return 0, err
	}
	if total, err = addCommandFrameEncodedSectionLen(total, extRefsLen); err != nil {
		return 0, err
	}
	if total, err = addCommandFrameEncodedSectionLen(total, preconditionsLen); err != nil {
		return 0, err
	}
	if total, err = addCommandFrameEncodedSectionLen(total, assertionsLen); err != nil {
		return 0, err
	}
	return total, nil
}

func addCommandFrameEncodedSectionLen(total, n int) (int, error) {
	if n < 0 || n > int(^uint32(0)) {
		return 0, ErrRecordTooLarge
	}
	if total > int(^uint(0)>>1)-n {
		return 0, ErrRecordTooLarge
	}
	return total + n, nil
}

func encodeCommandFrameTo(dst []byte, env CommandEnvelope) ([]byte, error) {
	if env.Version == 0 {
		env.Version = CommandFrameVersion
	}
	if env.Version != CommandFrameVersion {
		return nil, ErrCommandWALUnsupportedVersion
	}
	if env.Kind == CommandKindRawKVBatch && env.Payload == nil {
		payload, err := EncodeRawKVBatchPayload(nil)
		if err != nil {
			return nil, err
		}
		env.Payload = payload
	}
	if err := validateCommandEnvelopeForEncode(env); err != nil {
		return nil, err
	}
	extRefs, err := encodeExternalRefs(env.ExternalRefs)
	if err != nil {
		return nil, err
	}
	preconditions, err := encodeCommandExtensions(env.Preconditions)
	if err != nil {
		return nil, err
	}
	assertions, err := encodeCommandExtensions(env.ResultAssertions)
	if err != nil {
		return nil, err
	}
	total, err := commandFrameEncodedSizeFromLengths(len(env.Payload), len(extRefs), len(preconditions), len(assertions))
	if err != nil {
		return nil, err
	}
	if cap(dst) < total {
		dst = make([]byte, total)
	} else {
		dst = dst[:total]
	}
	frame := dst
	copy(frame[0:4], commandFrameMagic[:])
	binary.LittleEndian.PutUint16(frame[4:6], env.Version)
	binary.LittleEndian.PutUint16(frame[6:8], CommandFrameVersion)
	binary.LittleEndian.PutUint16(frame[8:10], uint16(env.Kind))
	binary.LittleEndian.PutUint16(frame[10:12], uint16(env.Scope))
	binary.LittleEndian.PutUint64(frame[12:20], env.FeatureFlags)
	binary.LittleEndian.PutUint64(frame[20:28], env.LSN)
	binary.LittleEndian.PutUint64(frame[28:36], env.CatalogEpoch)
	binary.LittleEndian.PutUint64(frame[36:44], env.SchemaEpoch)
	binary.LittleEndian.PutUint64(frame[44:52], env.BaseAppliedLSN)
	binary.LittleEndian.PutUint16(frame[52:54], uint16(env.PayloadFormat))
	binary.LittleEndian.PutUint16(frame[54:56], 0)
	binary.LittleEndian.PutUint32(frame[56:60], uint32(len(env.Payload)))
	binary.LittleEndian.PutUint32(frame[60:64], uint32(len(extRefs)))
	binary.LittleEndian.PutUint32(frame[64:68], uint32(len(preconditions)))
	binary.LittleEndian.PutUint32(frame[68:72], uint32(len(assertions)))
	digest := sha256.Sum256(env.Payload)
	copy(frame[72:72+sha256.Size], digest[:])
	off := commandFrameHeaderSize
	copy(frame[off:], env.Payload)
	off += len(env.Payload)
	copy(frame[off:], extRefs)
	off += len(extRefs)
	copy(frame[off:], preconditions)
	off += len(preconditions)
	copy(frame[off:], assertions)
	return frame, nil
}

func DecodeCommandFrame(frame []byte) (CommandEnvelope, error) {
	var env CommandEnvelope
	if len(frame) >= batchHeaderSize && len(frame) < commandFrameHeaderSize && frame[0] == Version {
		return env, ErrCommandWALLegacyPayload
	}
	if len(frame) < commandFrameHeaderSize {
		return env, ErrCorrupt
	}
	if !bytes.Equal(frame[0:4], commandFrameMagic[:]) {
		if frame[0] == Version {
			return env, ErrCommandWALLegacyPayload
		}
		return env, ErrCorrupt
	}
	version := binary.LittleEndian.Uint16(frame[4:6])
	minReader := binary.LittleEndian.Uint16(frame[6:8])
	if version != CommandFrameVersion || minReader > CommandFrameVersion {
		return env, ErrCommandWALUnsupportedVersion
	}
	env.Version = version
	env.Kind = CommandKind(binary.LittleEndian.Uint16(frame[8:10]))
	env.Scope = CommandScope(binary.LittleEndian.Uint16(frame[10:12]))
	env.FeatureFlags = binary.LittleEndian.Uint64(frame[12:20])
	if env.FeatureFlags&commandWALCriticalFlagsMask != 0 {
		return env, ErrCommandWALUnsupportedCriticalFlag
	}
	env.LSN = binary.LittleEndian.Uint64(frame[20:28])
	env.CatalogEpoch = binary.LittleEndian.Uint64(frame[28:36])
	env.SchemaEpoch = binary.LittleEndian.Uint64(frame[36:44])
	env.BaseAppliedLSN = binary.LittleEndian.Uint64(frame[44:52])
	env.PayloadFormat = PayloadFormat(binary.LittleEndian.Uint16(frame[52:54]))
	if binary.LittleEndian.Uint16(frame[54:56]) != 0 {
		return env, ErrCorrupt
	}
	payloadLen := binary.LittleEndian.Uint32(frame[56:60])
	extRefsLen := binary.LittleEndian.Uint32(frame[60:64])
	preconditionsLen := binary.LittleEndian.Uint32(frame[64:68])
	assertionsLen := binary.LittleEndian.Uint32(frame[68:72])
	wantDigest := [32]byte{}
	copy(wantDigest[:], frame[72:72+sha256.Size])
	if err := validateCommandEnvelopeIdentity(env); err != nil {
		return env, err
	}
	total := uint64(commandFrameHeaderSize) + uint64(payloadLen) + uint64(extRefsLen) + uint64(preconditionsLen) + uint64(assertionsLen)
	if total > uint64(len(frame)) || total > uint64(^uint(0)>>1) || int(total) != len(frame) {
		return env, ErrCorrupt
	}
	off := commandFrameHeaderSize
	env.Payload = append([]byte(nil), frame[off:off+int(payloadLen)]...)
	off += int(payloadLen)
	gotDigest := sha256.Sum256(env.Payload)
	if gotDigest != wantDigest {
		return env, ErrCommandWALPayloadDigestMismatch
	}
	env.PayloadDigest = gotDigest
	extRefs, err := decodeExternalRefs(frame[off : off+int(extRefsLen)])
	if err != nil {
		return env, err
	}
	env.ExternalRefs = extRefs
	off += int(extRefsLen)
	preconditions, err := decodeCommandExtensions(frame[off : off+int(preconditionsLen)])
	if err != nil {
		return env, err
	}
	env.Preconditions = preconditions
	off += int(preconditionsLen)
	assertions, err := decodeCommandExtensions(frame[off : off+int(assertionsLen)])
	if err != nil {
		return env, err
	}
	env.ResultAssertions = assertions
	if err := validateCommandEnvelopePayload(env); err != nil {
		return env, err
	}
	return env, nil
}

func validateCommandEnvelopeForEncode(env CommandEnvelope) error {
	if env.Version != 0 && env.Version != CommandFrameVersion {
		return ErrCommandWALUnsupportedVersion
	}
	if env.FeatureFlags&commandWALCriticalFlagsMask != 0 {
		return ErrCommandWALUnsupportedCriticalFlag
	}
	if err := validateCommandEnvelopeIdentity(env); err != nil {
		return err
	}
	return validateCommandEnvelopePayload(env)
}

func validateCommandEnvelopeIdentity(env CommandEnvelope) error {
	if env.LSN == 0 {
		return fmt.Errorf("%w: zero lsn", ErrCorrupt)
	}
	switch env.Kind {
	case CommandKindRawKVBatch:
		if env.Scope != CommandScopeRawKV || env.PayloadFormat != PayloadFormatRawKVBatchV1 {
			return ErrCorrupt
		}
	case CommandKindCollectionInsertBatchByID:
		if env.Scope != CommandScopeCollection || env.PayloadFormat != PayloadFormatNativeWireDeterministic {
			return ErrCorrupt
		}
	case CommandKindCatalogMutationPlaceholder:
		if env.Scope != CommandScopeCatalog || env.PayloadFormat != PayloadFormatNativeWireDeterministic {
			return ErrCorrupt
		}
	default:
		return ErrCommandWALUnsupportedKind
	}
	return nil
}

func validateCommandEnvelopePayload(env CommandEnvelope) error {
	switch env.Kind {
	case CommandKindRawKVBatch:
		return validateRawKVBatchPayload(env.Payload)
	default:
		return nil
	}
}

func EncodeRawKVBatchPayload(ops []RawKVOperation) ([]byte, error) {
	if len(ops) > int(^uint32(0)) {
		return nil, ErrRecordTooLarge
	}
	total := rawKVBatchHeaderSize
	for i := range ops {
		op := &ops[i]
		if err := validateRawKVOperation(op); err != nil {
			return nil, err
		}
		if len(op.Key) > int(^uint32(0)) || len(op.Value) > int(^uint32(0)) {
			return nil, ErrRecordTooLarge
		}
		total += rawKVOpHeaderSize + len(op.Key) + len(op.Value)
	}
	payload := make([]byte, total)
	binary.LittleEndian.PutUint16(payload[0:2], 1)
	binary.LittleEndian.PutUint32(payload[2:6], uint32(len(ops)))
	off := rawKVBatchHeaderSize
	for i := range ops {
		op := &ops[i]
		payload[off] = byte(op.Op)
		binary.LittleEndian.PutUint32(payload[off+1:off+5], uint32(len(op.Key)))
		binary.LittleEndian.PutUint32(payload[off+5:off+9], uint32(len(op.Value)))
		off += rawKVOpHeaderSize
		copy(payload[off:], op.Key)
		off += len(op.Key)
		copy(payload[off:], op.Value)
		off += len(op.Value)
	}
	return payload, nil
}

func DecodeRawKVBatchPayload(payload []byte) ([]RawKVOperation, error) {
	if err := validateRawKVBatchPayload(payload); err != nil {
		return nil, err
	}
	count := binary.LittleEndian.Uint32(payload[2:6])
	ops := make([]RawKVOperation, 0, count)
	off := rawKVBatchHeaderSize
	for i := uint32(0); i < count; i++ {
		op := RawKVOp(payload[off])
		keyLen := binary.LittleEndian.Uint32(payload[off+1 : off+5])
		valueLen := binary.LittleEndian.Uint32(payload[off+5 : off+9])
		off += rawKVOpHeaderSize
		entry := RawKVOperation{Op: op}
		entry.Key = cloneBytesPreserveEmpty(payload[off : off+int(keyLen)])
		off += int(keyLen)
		entry.Value = cloneBytesPreserveEmpty(payload[off : off+int(valueLen)])
		off += int(valueLen)
		ops = append(ops, entry)
	}
	return ops, nil
}

func validateRawKVBatchPayload(payload []byte) error {
	return ScanRawKVBatchPayload(payload, nil)
}

// ScanRawKVBatchPayload validates payload and visits each op with slices that
// reference payload. Use DecodeRawKVBatchPayload when callers need owned copies.
func ScanRawKVBatchPayload(payload []byte, visit func(op RawKVOp, key, value []byte) error) error {
	if len(payload) < rawKVBatchHeaderSize {
		return ErrCorrupt
	}
	if binary.LittleEndian.Uint16(payload[0:2]) != 1 {
		return ErrCommandWALUnsupportedVersion
	}
	count := binary.LittleEndian.Uint32(payload[2:6])
	if count > uint32((len(payload)-rawKVBatchHeaderSize)/rawKVOpHeaderSize) {
		return ErrCorrupt
	}
	off := rawKVBatchHeaderSize
	for i := uint32(0); i < count; i++ {
		if off+rawKVOpHeaderSize > len(payload) {
			return ErrCorrupt
		}
		op := RawKVOp(payload[off])
		keyLen := binary.LittleEndian.Uint32(payload[off+1 : off+5])
		valueLen := binary.LittleEndian.Uint32(payload[off+5 : off+9])
		off += rawKVOpHeaderSize
		need := uint64(keyLen) + uint64(valueLen)
		if need > uint64(len(payload)-off) || need > uint64(^uint(0)>>1) {
			return ErrCorrupt
		}
		key := payload[off : off+int(keyLen)]
		off += int(keyLen)
		if err := validateRawKVOperationShape(op, key, valueLen); err != nil {
			return err
		}
		value := payload[off : off+int(valueLen)]
		if visit != nil {
			if err := visit(op, key, value); err != nil {
				return err
			}
		}
		off += int(valueLen)
	}
	if off != len(payload) {
		return ErrCorrupt
	}
	return nil
}

func validateRawKVOperation(op *RawKVOperation) error {
	if op == nil || op.Key == nil {
		return ErrCorrupt
	}
	return validateRawKVOperationShape(op.Op, op.Key, uint32(len(op.Value)))
}

func validateRawKVOperationShape(op RawKVOp, key []byte, valueLen uint32) error {
	if key == nil {
		return ErrCorrupt
	}
	switch op {
	case RawKVOpSet:
		return nil
	case RawKVOpDelete:
		if valueLen != 0 {
			return ErrCorrupt
		}
		return nil
	default:
		return ErrCorrupt
	}
}

func cloneBytesPreserveEmpty(src []byte) []byte {
	if src == nil {
		return nil
	}
	if len(src) == 0 {
		return []byte{}
	}
	return append([]byte(nil), src...)
}

func encodeExternalRefs(refs []ExternalRef) ([]byte, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	total, err := externalRefsEncodedLen(refs)
	if err != nil {
		return nil, err
	}
	out := make([]byte, total)
	binary.LittleEndian.PutUint32(out[0:4], uint32(len(refs)))
	off := 4
	for i := range refs {
		ref := &refs[i]
		binary.LittleEndian.PutUint16(out[off:off+2], uint16(ref.Class))
		binary.LittleEndian.PutUint16(out[off+2:off+4], ref.Flags)
		binary.LittleEndian.PutUint32(out[off+4:off+8], uint32(len(ref.Path)))
		binary.LittleEndian.PutUint64(out[off+8:off+16], ref.FileID)
		binary.LittleEndian.PutUint64(out[off+16:off+24], ref.Offset)
		binary.LittleEndian.PutUint64(out[off+24:off+32], ref.Length)
		copy(out[off+32:off+32+sha256.Size], ref.Digest[:])
		off += 32 + sha256.Size
		copy(out[off:], ref.Path)
		off += len(ref.Path)
	}
	return out, nil
}

func externalRefsEncodedLen(refs []ExternalRef) (int, error) {
	if len(refs) == 0 {
		return 0, nil
	}
	if len(refs) > int(^uint32(0)) {
		return 0, ErrRecordTooLarge
	}
	total := 4
	maxInt := int(^uint(0) >> 1)
	for i := range refs {
		pathLen := len(refs[i].Path)
		if pathLen > int(^uint32(0)) {
			return 0, ErrRecordTooLarge
		}
		if pathLen > maxInt-externalRefEncodedFixedSize {
			return 0, ErrRecordTooLarge
		}
		n := externalRefEncodedFixedSize + pathLen
		if total > maxInt-n {
			return 0, ErrRecordTooLarge
		}
		total += n
	}
	if total > int(^uint32(0)) {
		return 0, ErrRecordTooLarge
	}
	return total, nil
}

func decodeExternalRefs(data []byte) ([]ExternalRef, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) < 4 {
		return nil, ErrCorrupt
	}
	count := binary.LittleEndian.Uint32(data[0:4])
	if count > uint32((len(data)-4)/externalRefEncodedFixedSize) {
		return nil, ErrCorrupt
	}
	refs := make([]ExternalRef, 0, count)
	off := 4
	for i := uint32(0); i < count; i++ {
		if off+externalRefEncodedFixedSize > len(data) {
			return nil, ErrCorrupt
		}
		ref := ExternalRef{}
		ref.Class = ExternalRefClass(binary.LittleEndian.Uint16(data[off : off+2]))
		ref.Flags = binary.LittleEndian.Uint16(data[off+2 : off+4])
		pathLen := binary.LittleEndian.Uint32(data[off+4 : off+8])
		ref.FileID = binary.LittleEndian.Uint64(data[off+8 : off+16])
		ref.Offset = binary.LittleEndian.Uint64(data[off+16 : off+24])
		ref.Length = binary.LittleEndian.Uint64(data[off+24 : off+32])
		copy(ref.Digest[:], data[off+32:off+32+sha256.Size])
		off += externalRefEncodedFixedSize
		if uint64(pathLen) > uint64(len(data)-off) || uint64(pathLen) > uint64(^uint(0)>>1) {
			return nil, ErrCorrupt
		}
		ref.Path = append([]byte(nil), data[off:off+int(pathLen)]...)
		off += int(pathLen)
		refs = append(refs, ref)
	}
	if off != len(data) {
		return nil, ErrCorrupt
	}
	return refs, nil
}

func encodeCommandExtensions(exts []CommandExtension) ([]byte, error) {
	if len(exts) == 0 {
		return nil, nil
	}
	total, err := commandExtensionsEncodedLen(exts)
	if err != nil {
		return nil, err
	}
	out := make([]byte, total)
	binary.LittleEndian.PutUint32(out[0:4], uint32(len(exts)))
	off := 4
	for i := range exts {
		ext := &exts[i]
		binary.LittleEndian.PutUint16(out[off:off+2], ext.Type)
		binary.LittleEndian.PutUint16(out[off+2:off+4], 0)
		binary.LittleEndian.PutUint32(out[off+4:off+8], uint32(len(ext.Payload)))
		off += 8
		copy(out[off:], ext.Payload)
		off += len(ext.Payload)
	}
	return out, nil
}

func commandExtensionsEncodedLen(exts []CommandExtension) (int, error) {
	if len(exts) == 0 {
		return 0, nil
	}
	if len(exts) > int(^uint32(0)) {
		return 0, ErrRecordTooLarge
	}
	total := 4
	maxInt := int(^uint(0) >> 1)
	for i := range exts {
		payloadLen := len(exts[i].Payload)
		if payloadLen > int(^uint32(0)) {
			return 0, ErrRecordTooLarge
		}
		if payloadLen > maxInt-commandExtensionEncodedFixedSize {
			return 0, ErrRecordTooLarge
		}
		n := commandExtensionEncodedFixedSize + payloadLen
		if total > maxInt-n {
			return 0, ErrRecordTooLarge
		}
		total += n
	}
	if total > int(^uint32(0)) {
		return 0, ErrRecordTooLarge
	}
	return total, nil
}

func decodeCommandExtensions(data []byte) ([]CommandExtension, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) < 4 {
		return nil, ErrCorrupt
	}
	count := binary.LittleEndian.Uint32(data[0:4])
	if count > uint32((len(data)-4)/commandExtensionEncodedFixedSize) {
		return nil, ErrCorrupt
	}
	exts := make([]CommandExtension, 0, count)
	off := 4
	for i := uint32(0); i < count; i++ {
		if off+commandExtensionEncodedFixedSize > len(data) {
			return nil, ErrCorrupt
		}
		ext := CommandExtension{Type: binary.LittleEndian.Uint16(data[off : off+2])}
		if binary.LittleEndian.Uint16(data[off+2:off+4]) != 0 {
			return nil, ErrCorrupt
		}
		payloadLen := binary.LittleEndian.Uint32(data[off+4 : off+8])
		off += commandExtensionEncodedFixedSize
		if uint64(payloadLen) > uint64(len(data)-off) || uint64(payloadLen) > uint64(^uint(0)>>1) {
			return nil, ErrCorrupt
		}
		ext.Payload = append([]byte(nil), data[off:off+int(payloadLen)]...)
		off += int(payloadLen)
		exts = append(exts, ext)
	}
	if off != len(data) {
		return nil, ErrCorrupt
	}
	return exts, nil
}

func putCommandFramePayloadLenForTest(frame []byte, n uint32) {
	if len(frame) >= 60 {
		binary.LittleEndian.PutUint32(frame[56:60], n)
	}
}

func (r *Reader) ReadCommandFrame() (CommandEnvelope, error) {
	payload, err := r.readSegmentPayload(true)
	if err != nil {
		return CommandEnvelope{}, err
	}
	return DecodeCommandFrame(payload)
}

func ScanCommandFrames(path string, opts Options) ([]CommandEnvelope, error) {
	return scanCommandFrames(path, opts, nil, true)
}

func ScanCommandFrameSegments(paths []string, opts Options) ([]CommandEnvelope, error) {
	seen := make(map[uint64]struct{})
	var frames []CommandEnvelope
	allowTail := commandFrameSegmentTailAllowance(paths)
	for i, path := range paths {
		segmentFrames, err := scanCommandFrames(path, opts, seen, allowTail[i])
		if err != nil {
			return frames, err
		}
		frames = append(frames, segmentFrames...)
	}
	return frames, nil
}

func commandFrameSegmentTailAllowance(paths []string) []bool {
	allow := make([]bool, len(paths))
	type parsedSegment struct {
		lane int
		seq  uint64
		ok   bool
	}
	parsed := make([]parsedSegment, len(paths))
	latestByLane := make(map[int]uint64)
	for i, path := range paths {
		lane, seq, ok := parseCommandSegmentName(filepath.Base(path))
		parsed[i] = parsedSegment{lane: lane, seq: seq, ok: ok}
		if ok && seq > latestByLane[lane] {
			latestByLane[lane] = seq
		}
	}
	for i := range paths {
		if parsed[i].ok {
			allow[i] = parsed[i].seq == latestByLane[parsed[i].lane]
			continue
		}
		allow[i] = i == len(paths)-1
	}
	return allow
}

func scanCommandFrames(path string, opts Options, seen map[uint64]struct{}, allowTerminalTail bool) ([]CommandEnvelope, error) {
	r, err := NewReaderWithOptions(path, opts)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	if seen == nil {
		seen = make(map[uint64]struct{})
	}
	var frames []CommandEnvelope
	for {
		env, err := r.ReadCommandFrame()
		if err != nil {
			if err == io.EOF || (allowTerminalTail && errors.Is(err, ErrCommandWALTerminalTail)) {
				return frames, nil
			}
			return frames, err
		}
		if _, dup := seen[env.LSN]; dup {
			return frames, ErrCommandWALDuplicateLSN
		}
		seen[env.LSN] = struct{}{}
		frames = append(frames, env)
	}
}

func scanCommandFrameMaxLSNAndEndWithLSN(path string, opts Options, onLSN func(uint64) error) (maxLSN uint64, typed bool, completeEnd int64, err error) {
	r, err := NewReaderWithOptions(path, opts)
	if err != nil {
		return 0, false, 0, err
	}
	defer r.Close()

	var lastLSN uint64
	for {
		start, seekErr := r.f.Seek(0, io.SeekCurrent)
		if seekErr != nil {
			return 0, typed, completeEnd, seekErr
		}
		env, err := r.ReadCommandFrame()
		if err != nil {
			if errorsIsEOFOrTail(err) {
				return maxLSN, typed, start, nil
			}
			if errors.Is(err, ErrCommandWALLegacyPayload) && !typed {
				return 0, false, completeEnd, err
			}
			return 0, typed, completeEnd, err
		}
		if lastLSN != 0 && env.LSN <= lastLSN {
			return 0, true, completeEnd, ErrCommandWALDuplicateLSN
		}
		lastLSN = env.LSN
		typed = true
		if onLSN != nil {
			if err := onLSN(env.LSN); err != nil {
				return 0, typed, completeEnd, err
			}
		}
		if env.LSN > maxLSN {
			maxLSN = env.LSN
		}
		completeEnd, err = r.f.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, typed, completeEnd, err
		}
	}
}

func errorsIsEOFOrTail(err error) bool {
	return err == io.EOF || errors.Is(err, ErrCommandWALTerminalTail)
}
