package commitlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"slices"
)

const (
	CommandExtensionExternalRefFenceV1 uint16 = 1
	externalRefFenceV1PayloadSize             = 4 + sha256.Size
)

// CommandDurabilityClass is the persisted acknowledgement contract in V2
// command-frame header bytes 54..56.
type CommandDurabilityClass uint16

const (
	CommandDurabilityDurable CommandDurabilityClass = iota + 1
	CommandDurabilityRelaxed
)

// ExternalRefFenceV1 commits a RawKV frame to the canonical set of value-log
// record IDs referenced by SetRID operations.
type ExternalRefFenceV1 struct {
	Count  uint32
	Digest [sha256.Size]byte
}

func validCommandDurabilityClass(class CommandDurabilityClass) bool {
	return class == CommandDurabilityDurable || class == CommandDurabilityRelaxed
}

// NewDurablePrefixBarrierV1 returns the versioned empty/no-op record used to
// advance the durable recovery horizon without carrying a user mutation.
func NewDurablePrefixBarrierV1(lsn, baseAppliedLSN uint64) CommandEnvelope {
	return CommandEnvelope{
		Version:         CommandFrameVersionV2,
		DurabilityClass: CommandDurabilityDurable,
		LSN:             lsn,
		Kind:            CommandKindDurablePrefixBarrier,
		Scope:           CommandScopeSystem,
		BaseAppliedLSN:  baseAppliedLSN,
		PayloadFormat:   PayloadFormatDurablePrefixBarrierV1,
	}
}

// EncodeCommandFrameV2 encodes the active command-WAL V2 format.
func EncodeCommandFrameV2(env CommandEnvelope) ([]byte, error) {
	return EncodeCommandFrameV2To(nil, env)
}

// EncodeCommandFrameV2To is EncodeCommandFrameV2 with caller-owned capacity.
// All semantic validation is completed before dst is resized or written.
func EncodeCommandFrameV2To(dst []byte, env CommandEnvelope) ([]byte, error) {
	env, preconditions, err := prepareCommandFrameV2ForEncode(env)
	if err != nil {
		return nil, err
	}
	extRefs, err := encodeExternalRefs(env.ExternalRefs)
	if err != nil {
		return nil, err
	}
	preconditionsBytes, err := encodeCommandExtensions(preconditions)
	if err != nil {
		return nil, err
	}
	assertions, err := encodeCommandExtensions(env.ResultAssertions)
	if err != nil {
		return nil, err
	}
	total, err := commandFrameEncodedSizeFromLengths(len(env.Payload), len(extRefs), len(preconditionsBytes), len(assertions))
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
	binary.LittleEndian.PutUint16(frame[4:6], CommandFrameVersionV2)
	binary.LittleEndian.PutUint16(frame[6:8], CommandFrameVersionV2)
	binary.LittleEndian.PutUint16(frame[8:10], uint16(env.Kind))
	binary.LittleEndian.PutUint16(frame[10:12], uint16(env.Scope))
	binary.LittleEndian.PutUint64(frame[12:20], env.FeatureFlags)
	binary.LittleEndian.PutUint64(frame[20:28], env.LSN)
	binary.LittleEndian.PutUint64(frame[28:36], env.CatalogEpoch)
	binary.LittleEndian.PutUint64(frame[36:44], env.SchemaEpoch)
	binary.LittleEndian.PutUint64(frame[44:52], env.BaseAppliedLSN)
	binary.LittleEndian.PutUint16(frame[52:54], uint16(env.PayloadFormat))
	binary.LittleEndian.PutUint16(frame[54:56], uint16(env.DurabilityClass))
	binary.LittleEndian.PutUint32(frame[56:60], uint32(len(env.Payload)))
	binary.LittleEndian.PutUint32(frame[60:64], uint32(len(extRefs)))
	binary.LittleEndian.PutUint32(frame[64:68], uint32(len(preconditionsBytes)))
	binary.LittleEndian.PutUint32(frame[68:72], uint32(len(assertions)))
	off := commandFrameHeaderSize
	copy(frame[off:], env.Payload)
	off += len(env.Payload)
	copy(frame[off:], extRefs)
	off += len(extRefs)
	copy(frame[off:], preconditionsBytes)
	off += len(preconditionsBytes)
	copy(frame[off:], assertions)
	return frame, nil
}

func prepareCommandFrameV2ForEncode(env CommandEnvelope) (CommandEnvelope, []CommandExtension, error) {
	if !validCommandDurabilityClass(env.DurabilityClass) {
		return CommandEnvelope{}, nil, ErrCorrupt
	}
	if env.Version == 0 {
		env.Version = CommandFrameVersionV2
	}
	if env.Version != CommandFrameVersionV2 {
		return CommandEnvelope{}, nil, ErrCommandWALUnsupportedVersion
	}
	if env.FeatureFlags&commandWALCriticalFlagsMask != 0 {
		return CommandEnvelope{}, nil, ErrCommandWALUnsupportedCriticalFlag
	}
	if env.Kind == CommandKindRawKVBatch && env.Payload == nil {
		payload, err := EncodeRawKVBatchPayload(nil)
		if err != nil {
			return CommandEnvelope{}, nil, err
		}
		env.Payload = payload
	}
	if err := validateCommandEnvelopeV2Identity(env); err != nil {
		return CommandEnvelope{}, nil, err
	}
	if err := validateExternalRefs(env.ExternalRefs); err != nil {
		return CommandEnvelope{}, nil, err
	}
	if err := validateCommandEnvelopePayloadV2(env); err != nil {
		return CommandEnvelope{}, nil, err
	}
	preconditions, err := canonicalCommandPreconditionsV2(env)
	if err != nil {
		return CommandEnvelope{}, nil, err
	}
	return env, preconditions, nil
}

// commandFrameV2EncodedSize validates a V2 envelope and returns its canonical
// encoded size without allocating the complete frame. Journal and writer caps
// use this preflight before rotation, LSN reservation, or frame encoding.
func commandFrameV2EncodedSize(env CommandEnvelope) (int, error) {
	env, preconditions, err := prepareCommandFrameV2ForEncode(env)
	if err != nil {
		return 0, err
	}
	extRefsLen, err := externalRefsEncodedLen(env.ExternalRefs)
	if err != nil {
		return 0, err
	}
	preconditionsLen, err := commandExtensionsEncodedLen(preconditions)
	if err != nil {
		return 0, err
	}
	assertionsLen, err := commandExtensionsEncodedLen(env.ResultAssertions)
	if err != nil {
		return 0, err
	}
	return commandFrameEncodedSizeFromLengths(len(env.Payload), extRefsLen, preconditionsLen, assertionsLen)
}

// DecodeCommandFrameV2 decodes and fully validates one V2 frame.
func DecodeCommandFrameV2(frame []byte) (CommandEnvelope, error) {
	return decodeCommandFrameV2(frame, false)
}

func decodeCommandFrameV2(frame []byte, borrowPayload bool) (CommandEnvelope, error) {
	var env CommandEnvelope
	if len(frame) < commandFrameHeaderSize {
		return env, ErrCorrupt
	}
	if !bytes.Equal(frame[0:4], commandFrameMagic[:]) {
		return env, ErrCorrupt
	}
	version := binary.LittleEndian.Uint16(frame[4:6])
	if version == CommandFrameVersion {
		return env, ErrCommandWALV1RebuildRequired
	}
	minReader := binary.LittleEndian.Uint16(frame[6:8])
	if version != CommandFrameVersionV2 || minReader > CommandFrameVersionV2 {
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
	env.DurabilityClass = CommandDurabilityClass(binary.LittleEndian.Uint16(frame[54:56]))
	if !validCommandDurabilityClass(env.DurabilityClass) {
		return env, ErrCorrupt
	}
	payloadLen := binary.LittleEndian.Uint32(frame[56:60])
	extRefsLen := binary.LittleEndian.Uint32(frame[60:64])
	preconditionsLen := binary.LittleEndian.Uint32(frame[64:68])
	assertionsLen := binary.LittleEndian.Uint32(frame[68:72])
	total := uint64(commandFrameHeaderSize) + uint64(payloadLen) + uint64(extRefsLen) + uint64(preconditionsLen) + uint64(assertionsLen)
	if total > uint64(len(frame)) || total > uint64(^uint(0)>>1) || int(total) != len(frame) {
		return env, ErrCorrupt
	}
	off := commandFrameHeaderSize
	env.Payload = frame[off : off+int(payloadLen)]
	if !borrowPayload {
		env.Payload = append([]byte(nil), env.Payload...)
	}
	off += int(payloadLen)
	var err error
	env.ExternalRefs, err = decodeExternalRefs(frame[off : off+int(extRefsLen)])
	if err != nil {
		return env, err
	}
	if err := validateExternalRefs(env.ExternalRefs); err != nil {
		return env, err
	}
	off += int(extRefsLen)
	env.Preconditions, err = decodeCommandExtensions(frame[off : off+int(preconditionsLen)])
	if err != nil {
		return env, err
	}
	off += int(preconditionsLen)
	env.ResultAssertions, err = decodeCommandExtensions(frame[off : off+int(assertionsLen)])
	if err != nil {
		return env, err
	}
	if err := validateCommandEnvelopeV2Identity(env); err != nil {
		return env, err
	}
	if err := validateCommandEnvelopePayloadV2(env); err != nil {
		return env, err
	}
	if env.Kind == CommandKindRawKVBatch {
		if _, _, err := RawKVExternalRefFenceV1(env); err != nil {
			return env, err
		}
	}
	return env, nil
}

// ReadCommandFrameV2 reads one length/CRC-bounded segment payload and applies
// the strict V2 decoder. It never falls back to the legacy V1 codec.
func (r *Reader) ReadCommandFrameV2() (CommandEnvelope, error) {
	payload, err := r.readSegmentPayloadWithCompression(true, false)
	if err != nil {
		return CommandEnvelope{}, err
	}
	return DecodeCommandFrameV2(payload)
}

// ScanCommandFramesV2 scans one strict-V2 segment using the same codec as the
// production journal owner.
func ScanCommandFramesV2(path string, opts Options) ([]CommandEnvelope, error) {
	r, err := NewReaderWithOptions(path, opts)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var frames []CommandEnvelope
	seen := make(map[uint64]struct{})
	for {
		env, err := r.ReadCommandFrameV2()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return frames, nil
			}
			return frames, err
		}
		if _, duplicate := seen[env.LSN]; duplicate {
			return frames, ErrCommandWALDuplicateLSN
		}
		seen[env.LSN] = struct{}{}
		frames = append(frames, env)
	}
}

// InspectCommandFrameV2TerminalTail reads the stable identity fields that fit
// before a terminal partial frame. It never treats the tail as valid; recovery
// uses the identity only to decide whether a relaxed tail is strictly above a
// separately established durable horizon.
func InspectCommandFrameV2TerminalTail(path string, segmentStart int64) (CommandEnvelope, int64, error) {
	var env CommandEnvelope
	f, err := os.Open(path)
	if err != nil {
		return env, 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return env, 0, err
	}
	if segmentStart < 0 || segmentStart > info.Size() {
		return env, info.Size(), ErrCorrupt
	}
	if segmentStart+segmentHeaderSize > info.Size() {
		return env, info.Size(), errors.Join(ErrCorrupt, ErrCommandWALV2TailIdentityUnavailable)
	}
	var segmentHeader [segmentHeaderSize]byte
	if _, err := f.ReadAt(segmentHeader[:], segmentStart); err != nil {
		return env, info.Size(), err
	}
	if binary.LittleEndian.Uint32(segmentHeader[0:4])&segmentFlagCompressed != 0 {
		return env, info.Size(), ErrCommandWALV2CompressedRecordUnsupported
	}
	available := info.Size() - segmentStart - segmentHeaderSize
	if available < 56 {
		return env, info.Size(), errors.Join(ErrCorrupt, ErrCommandWALV2TailIdentityUnavailable)
	}
	readLen := int64(commandFrameHeaderSize)
	if available < readLen {
		readLen = available
	}
	header := make([]byte, readLen)
	if _, err := f.ReadAt(header, segmentStart+segmentHeaderSize); err != nil && !errors.Is(err, io.EOF) {
		return env, info.Size(), err
	}
	if !bytes.Equal(header[0:4], commandFrameMagic[:]) {
		return env, info.Size(), ErrCorrupt
	}
	version := binary.LittleEndian.Uint16(header[4:6])
	if version == CommandFrameVersion {
		return env, info.Size(), ErrCommandWALV1RebuildRequired
	}
	if version != CommandFrameVersionV2 || binary.LittleEndian.Uint16(header[6:8]) > CommandFrameVersionV2 {
		return env, info.Size(), ErrCommandWALUnsupportedVersion
	}
	env.Version = version
	env.Kind = CommandKind(binary.LittleEndian.Uint16(header[8:10]))
	env.Scope = CommandScope(binary.LittleEndian.Uint16(header[10:12]))
	env.FeatureFlags = binary.LittleEndian.Uint64(header[12:20])
	if env.FeatureFlags&commandWALCriticalFlagsMask != 0 {
		return env, info.Size(), ErrCommandWALUnsupportedCriticalFlag
	}
	env.LSN = binary.LittleEndian.Uint64(header[20:28])
	env.BaseAppliedLSN = binary.LittleEndian.Uint64(header[44:52])
	env.PayloadFormat = PayloadFormat(binary.LittleEndian.Uint16(header[52:54]))
	env.DurabilityClass = CommandDurabilityClass(binary.LittleEndian.Uint16(header[54:56]))
	if env.LSN == 0 || !validCommandDurabilityClass(env.DurabilityClass) {
		return CommandEnvelope{}, info.Size(), ErrCorrupt
	}
	return env, info.Size(), nil
}

func validateCommandEnvelopeV2Identity(env CommandEnvelope) error {
	if env.LSN == 0 {
		return ErrCorrupt
	}
	if env.Kind == CommandKindDurablePrefixBarrier {
		if env.Scope != CommandScopeSystem || env.PayloadFormat != PayloadFormatDurablePrefixBarrierV1 || env.DurabilityClass != CommandDurabilityDurable {
			return ErrCorrupt
		}
		return nil
	}
	return validateCommandEnvelopeIdentity(env)
}

func validateCommandEnvelopePayloadV2(env CommandEnvelope) error {
	if env.Kind == CommandKindDurablePrefixBarrier {
		if len(env.Payload) != 0 || len(env.ExternalRefs) != 0 || len(env.Preconditions) != 0 || len(env.ResultAssertions) != 0 {
			return ErrCorrupt
		}
		return nil
	}
	return validateCommandEnvelopePayload(env)
}

func canonicalCommandPreconditionsV2(env CommandEnvelope) ([]CommandExtension, error) {
	if env.Kind != CommandKindRawKVBatch {
		return append([]CommandExtension(nil), env.Preconditions...), nil
	}
	fence, err := ExternalRefFenceV1FromRawKVPayload(env.Payload)
	if err != nil {
		return nil, err
	}
	preconditions := append([]CommandExtension(nil), env.Preconditions...)
	found := -1
	for i := range preconditions {
		if preconditions[i].Type != CommandExtensionExternalRefFenceV1 {
			continue
		}
		if found >= 0 {
			return nil, ErrCorrupt
		}
		found = i
	}
	if fence.Count == 0 {
		if found >= 0 {
			return nil, ErrCorrupt
		}
		return preconditions, nil
	}
	ext := encodeExternalRefFenceV1(fence)
	if found < 0 {
		return append(preconditions, ext), nil
	}
	decoded, err := decodeExternalRefFenceV1(preconditions[found])
	if err != nil || decoded != fence {
		return nil, ErrCorrupt
	}
	return preconditions, nil
}

func encodeExternalRefFenceV1(fence ExternalRefFenceV1) CommandExtension {
	payload := make([]byte, externalRefFenceV1PayloadSize)
	binary.LittleEndian.PutUint32(payload[0:4], fence.Count)
	copy(payload[4:], fence.Digest[:])
	return CommandExtension{Type: CommandExtensionExternalRefFenceV1, Payload: payload}
}

func decodeExternalRefFenceV1(ext CommandExtension) (ExternalRefFenceV1, error) {
	var fence ExternalRefFenceV1
	if ext.Type != CommandExtensionExternalRefFenceV1 || len(ext.Payload) != externalRefFenceV1PayloadSize {
		return fence, ErrCorrupt
	}
	fence.Count = binary.LittleEndian.Uint32(ext.Payload[0:4])
	copy(fence.Digest[:], ext.Payload[4:])
	if fence.Count == 0 {
		return ExternalRefFenceV1{}, ErrCorrupt
	}
	return fence, nil
}

// ExternalRefFenceV1FromRawKVPayload validates the canonical payload and
// hashes sorted unique little-endian RIDs.
func ExternalRefFenceV1FromRawKVPayload(payload []byte) (ExternalRefFenceV1, error) {
	rids := make([]uint64, 0)
	if err := ScanRawKVBatchRIDs(payload, func(rid uint64) error {
		rids = append(rids, rid)
		return nil
	}); err != nil {
		return ExternalRefFenceV1{}, err
	}
	if len(rids) == 0 {
		return ExternalRefFenceV1{}, nil
	}
	slices.Sort(rids)
	unique := rids[:0]
	for _, rid := range rids {
		if len(unique) == 0 || unique[len(unique)-1] != rid {
			unique = append(unique, rid)
		}
	}
	h := sha256.New()
	var le [8]byte
	for _, rid := range unique {
		binary.LittleEndian.PutUint64(le[:], rid)
		_, _ = h.Write(le[:])
	}
	var fence ExternalRefFenceV1
	fence.Count = uint32(len(unique))
	copy(fence.Digest[:], h.Sum(nil))
	return fence, nil
}

// RawKVExternalRefFenceV1 returns the one validated V2 fence, when present.
func RawKVExternalRefFenceV1(env CommandEnvelope) (ExternalRefFenceV1, bool, error) {
	if env.Kind != CommandKindRawKVBatch {
		return ExternalRefFenceV1{}, false, nil
	}
	canonical, err := ExternalRefFenceV1FromRawKVPayload(env.Payload)
	if err != nil {
		return ExternalRefFenceV1{}, false, err
	}
	var found *CommandExtension
	for i := range env.Preconditions {
		if env.Preconditions[i].Type != CommandExtensionExternalRefFenceV1 {
			continue
		}
		if found != nil {
			return ExternalRefFenceV1{}, false, ErrCorrupt
		}
		found = &env.Preconditions[i]
	}
	if canonical.Count == 0 {
		if found != nil {
			return ExternalRefFenceV1{}, false, ErrCorrupt
		}
		return ExternalRefFenceV1{}, false, nil
	}
	if found == nil {
		return ExternalRefFenceV1{}, false, ErrCorrupt
	}
	decoded, err := decodeExternalRefFenceV1(*found)
	if err != nil || decoded != canonical {
		return ExternalRefFenceV1{}, false, ErrCorrupt
	}
	return decoded, true, nil
}
