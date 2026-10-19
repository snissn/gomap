package commitlog

import "encoding/binary"

// ValidateSelectedCompositeMutation restricts active frames to inline raw point
// Set and supported collection row members. The aggregate replay validates and
// prepares every existing payload before its one outer installation.
func ValidateSelectedCompositeMutation(payload []byte) error {
	rawCount := 0
	return ScanCompositeMutationPayload(payload, func(m CompositeMutationMember) error {
		switch m.Kind {
		case CommandKindRawKVBatch:
			rawCount++
			if rawCount > 1 || m.PayloadFormat != PayloadFormatRawKVBatchV1 {
				return ErrCommandWALUnsupportedKind
			}
			return ScanRawKVBatchPayloadWithRevision(m.Payload, func(op RawKVOp, _, _ []byte, _ uint64) error {
				if op != RawKVOpSet {
					return ErrCommandWALUnsupportedKind
				}
				return nil
			})
		case CommandKindCollectionInsertBatchByID:
			if m.PayloadFormat != PayloadFormatCollectionInsertBatchByIDV1 && m.PayloadFormat != PayloadFormatCollectionTypedBatchByIDV1 {
				return ErrCommandWALUnsupportedKind
			}
			return nil
		case CommandKindCollectionUpdateBatchByID:
			if m.PayloadFormat != PayloadFormatCollectionUpdateBatchByIDV1 && m.PayloadFormat != PayloadFormatCollectionTypedBatchByIDV1 {
				return ErrCommandWALUnsupportedKind
			}
			return nil
		case CommandKindCollectionReplaceSourceByID:
			if m.PayloadFormat != PayloadFormatCollectionTypedSourceByIDV1 && m.PayloadFormat != PayloadFormatCollectionReplaceSourceByIDV1 {
				return ErrCommandWALUnsupportedKind
			}
			return nil
		case CommandKindCollectionDeleteBatchByID:
			if m.PayloadFormat != PayloadFormatCollectionDeleteBatchByIDV1 {
				return ErrCommandWALUnsupportedKind
			}
			return nil
		default:
			return ErrCommandWALUnsupportedKind
		}
	})
}

// BindCompositeRawPayload changes only the original raw member using caller
// preadmitted capacity. Whole validation and all size checks precede mutation.
// canonical must not alias payload. Native member bytes are preserved exactly.
func BindCompositeRawPayload(payload, canonical []byte) ([]byte, error) {
	if err := ValidateSelectedCompositeMutation(payload); err != nil {
		return nil, err
	}
	if err := ScanRawKVBatchPayloadWithRevision(canonical, func(op RawKVOp, _, _ []byte, _ uint64) error {
		if op != RawKVOpSet {
			return ErrCommandWALUnsupportedKind
		}
		return nil
	}); err != nil {
		return nil, err
	}
	found, start, end, header := false, 0, 0, 0
	off := compositeMutationHeaderBytesV1
	for off < len(payload) {
		m, next, err := compositeMutationMemberView(payload, off)
		if err != nil {
			return nil, err
		}
		if m.Kind == CommandKindRawKVBatch {
			if found {
				return nil, ErrCorrupt
			}
			found = true
			header = off
			start = off + compositeMutationMemberBytesV1
			end = next
		}
		off = next
	}
	if !found {
		return nil, ErrCorrupt
	}
	length := len(payload) - (end - start) + len(canonical)
	if length > cap(payload) || length > MaxCompositeMutationPayloadBytesV1 {
		return nil, ErrRecordTooLarge
	}
	oldLength := len(payload)
	result := payload[:length]
	copy(result[start+len(canonical):], payload[end:oldLength])
	copy(result[start:], canonical)
	binary.LittleEndian.PutUint32(result[header+12:header+16], uint32(len(canonical)))
	return result, nil
}
