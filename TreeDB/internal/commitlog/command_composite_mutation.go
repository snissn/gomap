package commitlog

import "encoding/binary"

const (
	CommandKindCompositeMutation     CommandKind   = 2
	PayloadFormatCompositeMutationV1 PayloadFormat = 17

	// Like the existing bounded native source-import command, the complete
	// command stays below the secure native 1 MiB frame profile. Journal and
	// transport owners still apply their own, possibly narrower admission.
	MaxCompositeMutationCommandBytesV1 = 768 << 10
	MaxCompositeMutationPayloadBytesV1 = MaxCompositeMutationCommandBytesV1 - commandFrameHeaderSize
	MaxCompositeMutationMembersV1      = 128
	compositeMutationHeaderBytesV1     = 2 + 2
	compositeMutationMemberBytesV1     = 2 + 2 + 8 + 4
)

// CompositeMutationMember is one ordered native mutation, not a child command
// frame. It has no independent identity, publication, extensions, or callback.
// Collection incarnation is the accepted logical identity, never a root ID.
// Raw members have zero incarnation; collection members require a nonzero one.
// Preparing/replaying members must aggregate their final effects before the
// enclosing command's single outer installation.
type CompositeMutationMember struct {
	Kind          CommandKind
	PayloadFormat PayloadFormat
	Incarnation   uint64
	Payload       []byte
}

func validateCompositeMutationMember(member CompositeMutationMember) error {
	switch member.Kind {
	case CommandKindRawKVBatch:
		if member.Incarnation != 0 || (member.PayloadFormat != PayloadFormatRawKVBatchV1 && member.PayloadFormat != PayloadFormatRawKVBatchV2) {
			return ErrCorrupt
		}
	case CommandKindCollectionInsertBatchByID:
		if member.Incarnation == 0 || (member.PayloadFormat != PayloadFormatCollectionInsertBatchByIDV1 && member.PayloadFormat != PayloadFormatCollectionTypedBatchByIDV1) {
			return ErrCorrupt
		}
	case CommandKindCollectionUpdateBatchByID:
		if member.Incarnation == 0 || (member.PayloadFormat != PayloadFormatCollectionUpdateBatchByIDV1 && member.PayloadFormat != PayloadFormatCollectionTypedBatchByIDV1) {
			return ErrCorrupt
		}
	case CommandKindCollectionDeleteBatchByID:
		if member.Incarnation == 0 || member.PayloadFormat != PayloadFormatCollectionDeleteBatchByIDV1 {
			return ErrCorrupt
		}
	default:
		// This excludes recursion, staged catalog/DDL, source import/progress,
		// graph, split-vector, partition, and durability control commands.
		return ErrCommandWALUnsupportedKind
	}
	if member.Kind != CommandKindRawKVBatch && member.PayloadFormat != PayloadFormatCollectionTypedBatchByIDV1 &&
		(len(member.Payload) < 2 || binary.LittleEndian.Uint16(member.Payload[:2]) != 1) {
		// Version-two colocated vector mutations carry standalone attempt and
		// outcome control. They cannot enter as apparently ordinary row members.
		return ErrCommandWALUnsupportedVersion
	}
	// Only the existing payload validator is used, without assigning any child
	// identity or invoking a command application path.
	return validateCommandEnvelopePayload(CommandEnvelope{Kind: member.Kind,
		PayloadFormat: member.PayloadFormat, Payload: member.Payload})
}

// PlanCompositeMutationPayload validates every member before returning the
// exact bounded payload charge. It does not allocate the output command.
func PlanCompositeMutationPayload(members []CompositeMutationMember) (int, error) {
	if len(members) == 0 || len(members) > MaxCompositeMutationMembersV1 {
		return 0, ErrRecordTooLarge
	}
	total := compositeMutationHeaderBytesV1
	for _, member := range members {
		if len(member.Payload) > MaxCompositeMutationPayloadBytesV1-compositeMutationMemberBytesV1-total {
			return 0, ErrRecordTooLarge
		}
		if err := validateCompositeMutationMember(member); err != nil {
			return 0, err
		}
		total += compositeMutationMemberBytesV1 + len(member.Payload)
	}
	return total, nil
}

// EncodeCompositeMutationPayloadTo performs whole validation before changing
// caller-owned capacity, then copies payloads into one original command image.
// Source member payloads must not alias dst's writable capacity.
func EncodeCompositeMutationPayloadTo(dst []byte, members []CompositeMutationMember) ([]byte, error) {
	total, err := PlanCompositeMutationPayload(members)
	if err != nil {
		return nil, err
	}
	if cap(dst) < total {
		dst = make([]byte, total)
	} else {
		dst = dst[:total]
	}
	binary.LittleEndian.PutUint16(dst[:2], 1)
	binary.LittleEndian.PutUint16(dst[2:4], uint16(len(members)))
	off := compositeMutationHeaderBytesV1
	for _, member := range members {
		binary.LittleEndian.PutUint16(dst[off:off+2], uint16(member.Kind))
		binary.LittleEndian.PutUint16(dst[off+2:off+4], uint16(member.PayloadFormat))
		binary.LittleEndian.PutUint64(dst[off+4:off+12], member.Incarnation)
		binary.LittleEndian.PutUint32(dst[off+12:off+16], uint32(len(member.Payload)))
		off += compositeMutationMemberBytesV1
		copy(dst[off:], member.Payload)
		off += len(member.Payload)
	}
	return dst, nil
}

// validateCompositeMutationPayload checks the complete payload, including its
// final member and exact end, before any caller can observe a borrowed member.
func validateCompositeMutationPayload(raw []byte) (int, error) {
	if len(raw) < compositeMutationHeaderBytesV1 || len(raw) > MaxCompositeMutationPayloadBytesV1 {
		return 0, ErrCorrupt
	}
	if binary.LittleEndian.Uint16(raw[:2]) != 1 {
		return 0, ErrCommandWALUnsupportedVersion
	}
	count := int(binary.LittleEndian.Uint16(raw[2:4]))
	if count == 0 || count > MaxCompositeMutationMembersV1 {
		return 0, ErrCorrupt
	}
	off := compositeMutationHeaderBytesV1
	for range count {
		member, next, err := compositeMutationMemberView(raw, off)
		if err != nil {
			return 0, err
		}
		if err := validateCompositeMutationMember(member); err != nil {
			return 0, err
		}
		off = next
	}
	if off != len(raw) {
		return 0, ErrCorrupt
	}
	return count, nil
}

func compositeMutationMemberView(raw []byte, off int) (CompositeMutationMember, int, error) {
	if off < 0 || off > len(raw) || len(raw)-off < compositeMutationMemberBytesV1 {
		return CompositeMutationMember{}, 0, ErrCorrupt
	}
	n := uint64(binary.LittleEndian.Uint32(raw[off+12 : off+16]))
	start := off + compositeMutationMemberBytesV1
	if n > uint64(len(raw)-start) {
		return CompositeMutationMember{}, 0, ErrCorrupt
	}
	end := start + int(n)
	return CompositeMutationMember{
		Kind:          CommandKind(binary.LittleEndian.Uint16(raw[off : off+2])),
		PayloadFormat: PayloadFormat(binary.LittleEndian.Uint16(raw[off+2 : off+4])),
		Incarnation:   binary.LittleEndian.Uint64(raw[off+4 : off+12]), Payload: raw[start:end],
	}, end, nil
}

// DecodeCompositeMutationPayloadView returns a bounded descriptor array only
// after complete validation. Payload byte views borrow raw; its original owner
// must outlive member preparation and scrub every borrowed alias afterward.
func DecodeCompositeMutationPayloadView(raw []byte) ([]CompositeMutationMember, error) {
	count, err := validateCompositeMutationPayload(raw)
	if err != nil {
		return nil, err
	}
	members := make([]CompositeMutationMember, count)
	off := compositeMutationHeaderBytesV1
	for i := range members {
		members[i], off, _ = compositeMutationMemberView(raw, off)
	}
	return members, nil
}

// ScanCompositeMutationPayload visits ordered borrowed members only after
// validating the entire command. A malformed suffix invokes no visitor, so
// callers cannot accidentally prepare/publish a valid prefix during decoding.
func ScanCompositeMutationPayload(raw []byte, visit func(CompositeMutationMember) error) error {
	count, err := validateCompositeMutationPayload(raw)
	if err != nil || visit == nil {
		return err
	}
	off := compositeMutationHeaderBytesV1
	for range count {
		member, next, _ := compositeMutationMemberView(raw, off)
		if err := visit(member); err != nil {
			return err
		}
		off = next
	}
	return nil
}
