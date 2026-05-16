package commitlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
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
const maxCommandFrameUint32 = uint64(^uint32(0))

// CommandKind identifies the deterministic command payload schema carried by a
// command WAL frame.
type CommandKind uint16

const (
	CommandKindRawKVBatch CommandKind = 1

	// Collection command frames carry deterministic user-level collection
	// mutations. They do not encode physical root deltas.
	CommandKindCollectionInsertBatchByID  CommandKind = 100
	CommandKindCollectionDeleteBatchByID  CommandKind = 101
	CommandKindCollectionUpdateBatchByID  CommandKind = 102
	CommandKindCatalogCreateCollection    CommandKind = 200
	CommandKindCatalogMutationPlaceholder CommandKind = CommandKindCatalogCreateCollection
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
	PayloadFormatRawKVBatchV1                PayloadFormat = 1
	PayloadFormatNativeWireDeterministic     PayloadFormat = 2
	PayloadFormatCollectionInsertBatchByIDV1 PayloadFormat = 3
	PayloadFormatCollectionDeleteBatchByIDV1 PayloadFormat = 4
	PayloadFormatCollectionUpdateBatchByIDV1 PayloadFormat = 5
	PayloadFormatCatalogCreateCollectionV1   PayloadFormat = 6
)

// RawKVOp is a deterministic raw key/value mutation inside a RawKVBatch
// command payload. The command frame LSN is the batch identity; individual ops
// intentionally do not carry their own sequence numbers.
type RawKVOp byte

const (
	RawKVOpSet RawKVOp = iota + 1
	RawKVOpDelete
	RawKVOpSetRID
)

// RawKVOperation represents a single operation in a RawKVBatch command frame.
//
// Field constraints by op type:
//   - RawKVOpSet: Key and Value are the raw bytes; RID must be zero.
//   - RawKVOpDelete: Key is set; Value must be empty; RID must be zero.
//   - RawKVOpSetRID: Key is set; Value MUST be empty (the RID is encoded
//     separately as an 8-byte payload); RID must be non-zero.
type RawKVOperation struct {
	Op    RawKVOp
	Key   []byte
	Value []byte
	RID   uint64
}

type CollectionDocument struct {
	ID       []byte
	Document []byte
}

type CollectionInsertBatchByIDPayload struct {
	Collection string
	Documents  []CollectionDocument
}

type CollectionDeleteBatchByIDPayload struct {
	Collection string
	IDs        [][]byte
}

type CollectionUpdateBatchByIDPayload struct {
	Collection string
	Documents  []CollectionDocument
}

type CatalogCreateCollectionPayload struct {
	Collection string
	Metadata   []byte
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
	if commandFrameIntExceedsUint32(n) {
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
		if env.Scope != CommandScopeCollection || env.PayloadFormat != PayloadFormatCollectionInsertBatchByIDV1 {
			return ErrCorrupt
		}
	case CommandKindCollectionDeleteBatchByID:
		if env.Scope != CommandScopeCollection || env.PayloadFormat != PayloadFormatCollectionDeleteBatchByIDV1 {
			return ErrCorrupt
		}
	case CommandKindCollectionUpdateBatchByID:
		if env.Scope != CommandScopeCollection || env.PayloadFormat != PayloadFormatCollectionUpdateBatchByIDV1 {
			return ErrCorrupt
		}
	case CommandKindCatalogCreateCollection:
		if env.Scope != CommandScopeCatalog || env.PayloadFormat != PayloadFormatCatalogCreateCollectionV1 {
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
	case CommandKindCollectionInsertBatchByID:
		_, err := DecodeCollectionInsertBatchByIDPayload(env.Payload)
		return err
	case CommandKindCollectionDeleteBatchByID:
		_, err := DecodeCollectionDeleteBatchByIDPayload(env.Payload)
		return err
	case CommandKindCollectionUpdateBatchByID:
		_, err := DecodeCollectionUpdateBatchByIDPayload(env.Payload)
		return err
	case CommandKindCatalogCreateCollection:
		_, err := DecodeCatalogCreateCollectionPayload(env.Payload)
		return err
	default:
		return nil
	}
}

func EncodeRawKVBatchPayload(ops []RawKVOperation) ([]byte, error) {
	if commandFrameIntExceedsUint32(len(ops)) {
		return nil, ErrRecordTooLarge
	}
	total := rawKVBatchHeaderSize
	for i := range ops {
		op := &ops[i]
		if err := validateRawKVOperation(op); err != nil {
			return nil, err
		}
		valueLen := len(op.Value)
		if op.Op == RawKVOpSetRID {
			valueLen = 8
		}
		if commandFrameIntExceedsUint32(len(op.Key)) || commandFrameIntExceedsUint32(valueLen) {
			return nil, ErrRecordTooLarge
		}
		total += rawKVOpHeaderSize + len(op.Key) + valueLen
	}
	payload := make([]byte, total)
	binary.LittleEndian.PutUint16(payload[0:2], 1)
	binary.LittleEndian.PutUint32(payload[2:6], uint32(len(ops)))
	off := rawKVBatchHeaderSize
	for i := range ops {
		op := &ops[i]
		value := op.Value
		var ridBuf [8]byte
		if op.Op == RawKVOpSetRID {
			binary.LittleEndian.PutUint64(ridBuf[:], op.RID)
			value = ridBuf[:]
		}
		payload[off] = byte(op.Op)
		binary.LittleEndian.PutUint32(payload[off+1:off+5], uint32(len(op.Key)))
		binary.LittleEndian.PutUint32(payload[off+5:off+9], uint32(len(value)))
		off += rawKVOpHeaderSize
		copy(payload[off:], op.Key)
		off += len(op.Key)
		copy(payload[off:], value)
		off += len(value)
	}
	return payload, nil
}

// EncodeRawKVSingleOperationPayload encodes the common one-op RawKVBatch
// command without forcing callers to materialize a single-entry operation slice.
func EncodeRawKVSingleOperationPayload(op RawKVOperation) ([]byte, error) {
	if err := validateRawKVOperation(&op); err != nil {
		return nil, err
	}
	valueLen := len(op.Value)
	if op.Op == RawKVOpSetRID {
		valueLen = 8
	}
	if commandFrameIntExceedsUint32(len(op.Key)) || commandFrameIntExceedsUint32(valueLen) {
		return nil, ErrRecordTooLarge
	}
	total := rawKVBatchHeaderSize + rawKVOpHeaderSize + len(op.Key) + valueLen
	payload := make([]byte, total)
	binary.LittleEndian.PutUint16(payload[0:2], 1)
	binary.LittleEndian.PutUint32(payload[2:6], 1)
	value := op.Value
	var ridBuf [8]byte
	if op.Op == RawKVOpSetRID {
		binary.LittleEndian.PutUint64(ridBuf[:], op.RID)
		value = ridBuf[:]
	}
	off := rawKVBatchHeaderSize
	payload[off] = byte(op.Op)
	binary.LittleEndian.PutUint32(payload[off+1:off+5], uint32(len(op.Key)))
	binary.LittleEndian.PutUint32(payload[off+5:off+9], uint32(len(value)))
	off += rawKVOpHeaderSize
	copy(payload[off:], op.Key)
	off += len(op.Key)
	copy(payload[off:], value)
	return payload, nil
}

// EncodeRawKVBatchPayloadScan encodes a RawKVBatch payload by scanning the
// caller's operation source once. It avoids materializing a []RawKVOperation
// when the caller already owns a replayable batch representation.
func EncodeRawKVBatchPayloadScan(scan func(func(RawKVOperation) error) error) ([]byte, error) {
	return EncodeRawKVBatchPayloadScanWithHint(scan, 0, 0)
}

// EncodeRawKVBatchPayloadScanWithHint is EncodeRawKVBatchPayloadScan with
// best-effort capacity hints. opHint is an approximate operation count and
// byteHint is an approximate total key+value byte count.
func EncodeRawKVBatchPayloadScanWithHint(scan func(func(RawKVOperation) error) error, opHint, byteHint int) ([]byte, error) {
	if scan == nil {
		return EncodeRawKVBatchPayload(nil)
	}
	capHint := rawKVBatchHeaderSize
	if opHint > 0 {
		if commandFrameIntExceedsUint32(opHint) || opHint > (int(^uint(0)>>1)-capHint)/rawKVOpHeaderSize {
			return nil, ErrRecordTooLarge
		}
		capHint += opHint * rawKVOpHeaderSize
	}
	if byteHint > 0 {
		if byteHint > int(^uint(0)>>1)-capHint {
			return nil, ErrRecordTooLarge
		}
		capHint += byteHint
	}
	payload := make([]byte, rawKVBatchHeaderSize, capHint)
	binary.LittleEndian.PutUint16(payload[0:2], 1)
	count := 0
	if err := scan(func(op RawKVOperation) error {
		if commandFrameIntExceedsUint32(count + 1) {
			return ErrRecordTooLarge
		}
		if err := validateRawKVOperation(&op); err != nil {
			return err
		}
		valueLen := len(op.Value)
		if op.Op == RawKVOpSetRID {
			valueLen = 8
		}
		if commandFrameIntExceedsUint32(len(op.Key)) || commandFrameIntExceedsUint32(valueLen) {
			return ErrRecordTooLarge
		}
		value := op.Value
		var ridBuf [8]byte
		if op.Op == RawKVOpSetRID {
			binary.LittleEndian.PutUint64(ridBuf[:], op.RID)
			value = ridBuf[:]
		}
		needed := rawKVOpHeaderSize + len(op.Key) + len(value)
		if needed > int(^uint32(0))-len(payload) || needed > int(^uint(0)>>1)-len(payload) {
			return ErrRecordTooLarge
		}
		off := len(payload)
		newLen := off + needed
		if newLen > cap(payload) {
			newCap := cap(payload) * 2
			if newCap < newLen {
				newCap = newLen
			}
			if newCap < 0 {
				return ErrRecordTooLarge
			}
			next := make([]byte, newLen, newCap)
			copy(next, payload)
			payload = next
		} else {
			payload = payload[:newLen]
		}
		payload[off] = byte(op.Op)
		binary.LittleEndian.PutUint32(payload[off+1:off+5], uint32(len(op.Key)))
		binary.LittleEndian.PutUint32(payload[off+5:off+9], uint32(len(value)))
		off += rawKVOpHeaderSize
		copy(payload[off:], op.Key)
		off += len(op.Key)
		copy(payload[off:], value)
		off += len(value)
		count++
		return nil
	}); err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[2:6], uint32(count))
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
		if op == RawKVOpSetRID {
			if valueLen != 8 {
				return nil, ErrCorrupt
			}
			entry.RID = binary.LittleEndian.Uint64(payload[off : off+8])
			if entry.RID == 0 {
				return nil, ErrCorrupt
			}
		} else {
			entry.Value = cloneBytesPreserveEmpty(payload[off : off+int(valueLen)])
		}
		off += int(valueLen)
		ops = append(ops, entry)
	}
	return ops, nil
}

func validateRawKVBatchPayload(payload []byte) error {
	return ScanRawKVBatchPayload(payload, nil)
}

// ScanRawKVBatchPayload validates payload and visits each op with slices that
// reference payload. For RawKVOpSetRID, value is exactly the 8-byte
// little-endian RID payload. Use DecodeRawKVBatchPayload when callers need
// owned copies or typed RID fields.
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
		if err := validateRawKVOperationShape(op, key, int(valueLen)); err != nil {
			return err
		}
		value := payload[off : off+int(valueLen)]
		if op == RawKVOpSetRID {
			// validateRawKVOperationShape already enforces valueLen == 8 for
			// RawKVOpSetRID, so len(value) == 8 is guaranteed here. Only
			// check for the zero-RID sentinel, which is always invalid.
			if binary.LittleEndian.Uint64(value) == 0 {
				return ErrCorrupt
			}
		}
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
	valueLen := len(op.Value)
	if op.Op == RawKVOpSetRID {
		if op.RID == 0 || len(op.Value) != 0 {
			return ErrCorrupt
		}
		valueLen = 8
	}
	if commandFrameIntExceedsUint32(valueLen) {
		return ErrRecordTooLarge
	}
	return validateRawKVOperationShape(op.Op, op.Key, valueLen)
}

func validateRawKVOperationShape(op RawKVOp, key []byte, valueLen int) error {
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
	case RawKVOpSetRID:
		if valueLen != 8 {
			return ErrCorrupt
		}
		return nil
	default:
		return ErrCorrupt
	}
}

func EncodeCollectionInsertBatchByIDPayload(collection string, docs []CollectionDocument) ([]byte, error) {
	if collection == "" {
		return nil, fmt.Errorf("%w: empty collection", ErrCorrupt)
	}
	ordered, err := canonicalCollectionDocuments(docs)
	if err != nil {
		return nil, err
	}
	total, err := collectionBatchPayloadHeaderLen(collection, len(ordered))
	if err != nil {
		return nil, err
	}
	for i := range ordered {
		doc := ordered[i]
		if commandFrameIntExceedsUint32(len(doc.Document)) {
			return nil, ErrRecordTooLarge
		}
		total, err = addCommandFrameEncodedSectionLen(total, 8+len(doc.ID)+len(doc.Document))
		if err != nil {
			return nil, err
		}
	}
	payload := make([]byte, total)
	off := encodeCollectionBatchHeader(payload, collection, len(ordered))
	for i := range ordered {
		doc := ordered[i]
		binary.LittleEndian.PutUint32(payload[off:off+4], uint32(len(doc.ID)))
		binary.LittleEndian.PutUint32(payload[off+4:off+8], uint32(len(doc.Document)))
		off += 8
		copy(payload[off:], doc.ID)
		off += len(doc.ID)
		copy(payload[off:], doc.Document)
		off += len(doc.Document)
	}
	return payload, nil
}

func DecodeCollectionInsertBatchByIDPayload(payload []byte) (CollectionInsertBatchByIDPayload, error) {
	collection, count, off, err := decodeCollectionBatchHeader(payload)
	if err != nil {
		return CollectionInsertBatchByIDPayload{}, err
	}
	if uint64(count)*8 > uint64(len(payload)-off) {
		return CollectionInsertBatchByIDPayload{}, ErrCorrupt
	}
	docCount, err := commandPayloadCountToInt(count)
	if err != nil {
		return CollectionInsertBatchByIDPayload{}, err
	}
	docs := make([]CollectionDocument, 0, docCount)
	for i := uint32(0); i < count; i++ {
		if off+8 > len(payload) {
			return CollectionInsertBatchByIDPayload{}, ErrCorrupt
		}
		idLen := binary.LittleEndian.Uint32(payload[off : off+4])
		docLen := binary.LittleEndian.Uint32(payload[off+4 : off+8])
		off += 8
		if uint64(idLen)+uint64(docLen) > uint64(len(payload)-off) {
			return CollectionInsertBatchByIDPayload{}, ErrCorrupt
		}
		id := payload[off : off+int(idLen)]
		off += int(idLen)
		doc := payload[off : off+int(docLen)]
		off += int(docLen)
		if len(id) == 0 {
			return CollectionInsertBatchByIDPayload{}, ErrCorrupt
		}
		docs = append(docs, CollectionDocument{
			ID:       cloneBytesPreserveEmpty(id),
			Document: cloneBytesPreserveEmpty(doc),
		})
	}
	if off != len(payload) {
		return CollectionInsertBatchByIDPayload{}, ErrCorrupt
	}
	if err := validateStrictlyIncreasingCollectionIDsFromDocs(docs); err != nil {
		return CollectionInsertBatchByIDPayload{}, err
	}
	return CollectionInsertBatchByIDPayload{Collection: collection, Documents: docs}, nil
}

func EncodeCollectionUpdateBatchByIDPayload(collection string, docs []CollectionDocument) ([]byte, error) {
	return EncodeCollectionInsertBatchByIDPayload(collection, docs)
}

func DecodeCollectionUpdateBatchByIDPayload(payload []byte) (CollectionUpdateBatchByIDPayload, error) {
	decoded, err := DecodeCollectionInsertBatchByIDPayload(payload)
	if err != nil {
		return CollectionUpdateBatchByIDPayload{}, err
	}
	return CollectionUpdateBatchByIDPayload{
		Collection: decoded.Collection,
		Documents:  decoded.Documents,
	}, nil
}

func EncodeCollectionDeleteBatchByIDPayload(collection string, ids [][]byte) ([]byte, error) {
	if collection == "" {
		return nil, fmt.Errorf("%w: empty collection", ErrCorrupt)
	}
	ordered, err := canonicalCollectionIDs(ids)
	if err != nil {
		return nil, err
	}
	total, err := collectionBatchPayloadHeaderLen(collection, len(ordered))
	if err != nil {
		return nil, err
	}
	for _, id := range ordered {
		total, err = addCommandFrameEncodedSectionLen(total, 4+len(id))
		if err != nil {
			return nil, err
		}
	}
	payload := make([]byte, total)
	off := encodeCollectionBatchHeader(payload, collection, len(ordered))
	for _, id := range ordered {
		binary.LittleEndian.PutUint32(payload[off:off+4], uint32(len(id)))
		off += 4
		copy(payload[off:], id)
		off += len(id)
	}
	return payload, nil
}

func DecodeCollectionDeleteBatchByIDPayload(payload []byte) (CollectionDeleteBatchByIDPayload, error) {
	collection, count, off, err := decodeCollectionBatchHeader(payload)
	if err != nil {
		return CollectionDeleteBatchByIDPayload{}, err
	}
	if uint64(count)*4 > uint64(len(payload)-off) {
		return CollectionDeleteBatchByIDPayload{}, ErrCorrupt
	}
	idCount, err := commandPayloadCountToInt(count)
	if err != nil {
		return CollectionDeleteBatchByIDPayload{}, err
	}
	ids := make([][]byte, 0, idCount)
	for i := uint32(0); i < count; i++ {
		if off+4 > len(payload) {
			return CollectionDeleteBatchByIDPayload{}, ErrCorrupt
		}
		idLen := binary.LittleEndian.Uint32(payload[off : off+4])
		off += 4
		if uint64(idLen) > uint64(len(payload)-off) {
			return CollectionDeleteBatchByIDPayload{}, ErrCorrupt
		}
		id := payload[off : off+int(idLen)]
		off += int(idLen)
		if len(id) == 0 {
			return CollectionDeleteBatchByIDPayload{}, ErrCorrupt
		}
		ids = append(ids, cloneBytesPreserveEmpty(id))
	}
	if off != len(payload) {
		return CollectionDeleteBatchByIDPayload{}, ErrCorrupt
	}
	if err := validateStrictlyIncreasingCollectionIDs(ids); err != nil {
		return CollectionDeleteBatchByIDPayload{}, err
	}
	return CollectionDeleteBatchByIDPayload{Collection: collection, IDs: ids}, nil
}

func EncodeCatalogCreateCollectionPayload(collection string, metadata []byte) ([]byte, error) {
	if collection == "" || metadata == nil {
		return nil, fmt.Errorf("%w: invalid catalog create collection payload", ErrCorrupt)
	}
	if commandFrameIntExceedsUint32(len(collection)) || commandFrameIntExceedsUint32(len(metadata)) {
		return nil, ErrRecordTooLarge
	}
	total, err := addCommandFrameEncodedSectionLen(2+4+4, len(collection))
	if err != nil {
		return nil, err
	}
	total, err = addCommandFrameEncodedSectionLen(total, len(metadata))
	if err != nil {
		return nil, err
	}
	payload := make([]byte, total)
	binary.LittleEndian.PutUint16(payload[0:2], 1)
	binary.LittleEndian.PutUint32(payload[2:6], uint32(len(collection)))
	binary.LittleEndian.PutUint32(payload[6:10], uint32(len(metadata)))
	copy(payload[10:], collection)
	copy(payload[10+len(collection):], metadata)
	return payload, nil
}

func DecodeCatalogCreateCollectionPayload(payload []byte) (CatalogCreateCollectionPayload, error) {
	if len(payload) < 10 {
		return CatalogCreateCollectionPayload{}, ErrCorrupt
	}
	if binary.LittleEndian.Uint16(payload[0:2]) != 1 {
		return CatalogCreateCollectionPayload{}, ErrCommandWALUnsupportedVersion
	}
	nameLen := binary.LittleEndian.Uint32(payload[2:6])
	metaLen := binary.LittleEndian.Uint32(payload[6:10])
	off := 10
	if uint64(nameLen)+uint64(metaLen) > uint64(len(payload)-off) {
		return CatalogCreateCollectionPayload{}, ErrCorrupt
	}
	collection := payload[off : off+int(nameLen)]
	off += int(nameLen)
	metadata := payload[off : off+int(metaLen)]
	off += int(metaLen)
	if off != len(payload) || len(collection) == 0 || metadata == nil {
		return CatalogCreateCollectionPayload{}, ErrCorrupt
	}
	return CatalogCreateCollectionPayload{
		Collection: string(collection),
		Metadata:   cloneBytesPreserveEmpty(metadata),
	}, nil
}

func commandPayloadCountToInt(count uint32) (int, error) {
	maxInt := int(^uint(0) >> 1)
	if uint64(count) > uint64(maxInt) {
		return 0, ErrRecordTooLarge
	}
	return int(count), nil
}

func collectionBatchPayloadHeaderLen(collection string, count int) (int, error) {
	if commandFrameIntExceedsUint32(len(collection)) || commandFrameIntExceedsUint32(count) {
		return 0, ErrRecordTooLarge
	}
	return addCommandFrameEncodedSectionLen(2+4+4, len(collection))
}

func encodeCollectionBatchHeader(payload []byte, collection string, count int) int {
	binary.LittleEndian.PutUint16(payload[0:2], 1)
	binary.LittleEndian.PutUint32(payload[2:6], uint32(len(collection)))
	binary.LittleEndian.PutUint32(payload[6:10], uint32(count))
	copy(payload[10:], collection)
	return 10 + len(collection)
}

func decodeCollectionBatchHeader(payload []byte) (collection string, count uint32, off int, err error) {
	if len(payload) < 10 {
		return "", 0, 0, ErrCorrupt
	}
	if binary.LittleEndian.Uint16(payload[0:2]) != 1 {
		return "", 0, 0, ErrCommandWALUnsupportedVersion
	}
	nameLen := binary.LittleEndian.Uint32(payload[2:6])
	count = binary.LittleEndian.Uint32(payload[6:10])
	off = 10
	if uint64(nameLen) > uint64(len(payload)-off) {
		return "", 0, 0, ErrCorrupt
	}
	nameBytes := payload[off : off+int(nameLen)]
	off += int(nameLen)
	if len(nameBytes) == 0 {
		return "", 0, 0, ErrCorrupt
	}
	return string(nameBytes), count, off, nil
}

func canonicalCollectionDocuments(docs []CollectionDocument) ([]CollectionDocument, error) {
	if commandFrameIntExceedsUint32(len(docs)) {
		return nil, ErrRecordTooLarge
	}
	ordered := make([]CollectionDocument, len(docs))
	for i := range docs {
		doc := docs[i]
		if len(doc.ID) == 0 || doc.ID == nil || doc.Document == nil {
			return nil, ErrCorrupt
		}
		if commandFrameIntExceedsUint32(len(doc.ID)) {
			return nil, ErrRecordTooLarge
		}
		ordered[i] = doc
	}
	sort.Slice(ordered, func(i, j int) bool {
		return bytes.Compare(ordered[i].ID, ordered[j].ID) < 0
	})
	if err := validateStrictlyIncreasingCollectionIDsFromDocs(ordered); err != nil {
		return nil, err
	}
	return ordered, nil
}

func canonicalCollectionIDs(ids [][]byte) ([][]byte, error) {
	if commandFrameIntExceedsUint32(len(ids)) {
		return nil, ErrRecordTooLarge
	}
	ordered := make([][]byte, len(ids))
	for i := range ids {
		if len(ids[i]) == 0 || ids[i] == nil {
			return nil, ErrCorrupt
		}
		if commandFrameIntExceedsUint32(len(ids[i])) {
			return nil, ErrRecordTooLarge
		}
		ordered[i] = ids[i]
	}
	sort.Slice(ordered, func(i, j int) bool {
		return bytes.Compare(ordered[i], ordered[j]) < 0
	})
	if err := validateStrictlyIncreasingCollectionIDs(ordered); err != nil {
		return nil, err
	}
	return ordered, nil
}

func validateStrictlyIncreasingCollectionIDsFromDocs(docs []CollectionDocument) error {
	for i := 1; i < len(docs); i++ {
		if bytes.Compare(docs[i-1].ID, docs[i].ID) >= 0 {
			return ErrCorrupt
		}
	}
	return nil
}

func validateStrictlyIncreasingCollectionIDs(ids [][]byte) error {
	for i := 1; i < len(ids); i++ {
		if bytes.Compare(ids[i-1], ids[i]) >= 0 {
			return ErrCorrupt
		}
	}
	return nil
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
	if commandFrameIntExceedsUint32(len(refs)) {
		return 0, ErrRecordTooLarge
	}
	total := 4
	maxInt := int(^uint(0) >> 1)
	for i := range refs {
		pathLen := len(refs[i].Path)
		if commandFrameIntExceedsUint32(pathLen) {
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
	if commandFrameIntExceedsUint32(total) {
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
	if commandFrameIntExceedsUint32(len(exts)) {
		return 0, ErrRecordTooLarge
	}
	total := 4
	maxInt := int(^uint(0) >> 1)
	for i := range exts {
		payloadLen := len(exts[i].Payload)
		if commandFrameIntExceedsUint32(payloadLen) {
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
	if commandFrameIntExceedsUint32(total) {
		return 0, ErrRecordTooLarge
	}
	return total, nil
}

func commandFrameIntExceedsUint32(n int) bool {
	return n < 0 || uint64(n) > maxCommandFrameUint32
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
	sort.Slice(frames, func(i, j int) bool {
		return frames[i].LSN < frames[j].LSN
	})
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

func scanCommandSegmentSummary(path string, opts Options, onLSN func(uint64) error) (maxLSN uint64, typed bool, completeEnd int64, err error) {
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
