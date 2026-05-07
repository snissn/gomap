package nativewire

import (
	"bytes"
	"encoding/binary"
	"strconv"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

type AckPolicy = iwire.AckPolicy

const (
	AckVisible       AckPolicy = iwire.AckVisible
	AckFlushed       AckPolicy = iwire.AckFlushed
	AckSynced        AckPolicy = iwire.AckSynced
	AckRaftCommitted AckPolicy = iwire.AckRaftCommitted

	replacementModeExistingOnly uint64 = 1
)

func documentFormatSection(format collections.DocumentFormat) iwire.Section {
	return iwire.Section{ID: iwire.SectionDocumentFormat, Bytes: binary.AppendUvarint(nil, uint64(encodeDocumentFormat(format)))}
}

func decodeDocumentFormatSection(sections []iwire.Section) (collections.DocumentFormat, error) {
	raw, err := metadataSection(sections, iwire.SectionDocumentFormat)
	if err != nil {
		return "", err
	}
	return decodeDocumentFormatPayload(raw)
}

func decodeDocumentFormatPayload(raw []byte) (collections.DocumentFormat, error) {
	format, n, err := readUvarint(raw)
	if err != nil {
		return "", err
	}
	if n != len(raw) {
		return "", protocolError(iwire.ErrMalformedFrame, "document_format has trailing bytes")
	}
	return decodeDocumentFormatStrict(format)
}

func ackSection(policy AckPolicy) iwire.Section {
	return iwire.Section{ID: iwire.SectionAckPolicy, Bytes: binary.AppendUvarint(nil, uint64(policy))}
}

func ackPolicyFromSections(sections []iwire.Section, fallback AckPolicy) (AckPolicy, error) {
	raw, ok, err := singletonSection(sections, iwire.SectionAckPolicy)
	if err != nil {
		return 0, err
	}
	if !ok {
		return fallback, nil
	}
	ack, err := ackPolicyFromPayload(raw)
	if err != nil || ack != 0 {
		return ack, err
	}
	return fallback, nil
}

func ackPolicyFromPayload(raw []byte) (AckPolicy, error) {
	value, n, err := readUvarint(raw)
	if err != nil {
		return 0, err
	}
	if n != len(raw) {
		return 0, protocolError(iwire.ErrMalformedFrame, "ack_policy has trailing bytes")
	}
	switch AckPolicy(value) {
	case 0:
		return 0, nil
	case iwire.AckVisible, iwire.AckFlushed, iwire.AckSynced, iwire.AckRaftCommitted:
		return AckPolicy(value), nil
	default:
		return 0, protocolError(iwire.ErrInvalidCommand, "unsupported ack policy %d", value)
	}
}

func validateReplacementMode(sections []iwire.Section) error {
	raw, err := metadataSection(sections, iwire.SectionReplacementMode)
	if err != nil {
		return err
	}
	mode, n, err := readUvarint(raw)
	if err != nil {
		return err
	}
	if n != len(raw) {
		return protocolError(iwire.ErrMalformedFrame, "replacement_mode has trailing bytes")
	}
	if mode != replacementModeExistingOnly {
		return protocolError(iwire.ErrInvalidCommand, "unsupported replacement_mode %d", mode)
	}
	return nil
}

func decodeIDsAndDocuments(sections []iwire.Section, limits iwire.Limits) ([][]byte, [][]byte, error) {
	return decodeIDsAndDocumentsInto(nil, nil, sections, limits)
}

func decodeIDsAndDocumentsInto(idDst, docDst [][]byte, sections []iwire.Section, limits iwire.Limits) ([][]byte, [][]byte, error) {
	rawIDs, err := metadataSection(sections, iwire.SectionDocumentIDs)
	if err != nil {
		return nil, nil, err
	}
	ids, err := decodeByteVectorBorrowedInto(idDst, rawIDs, limits)
	if err != nil {
		return nil, nil, err
	}
	rawDocs, err := metadataSection(sections, iwire.SectionDocuments)
	if err != nil {
		return nil, nil, err
	}
	docs, err := decodeByteVectorBorrowedInto(docDst, rawDocs, limits)
	if err != nil {
		return nil, nil, err
	}
	if len(ids) != len(docs) {
		return nil, nil, protocolError(iwire.ErrInvalidCommand, "document_ids length %d does not match documents length %d", len(ids), len(docs))
	}
	return ids, docs, nil
}

func decodeIDVector(sections []iwire.Section, limits iwire.Limits) ([][]byte, error) {
	return decodeIDVectorInto(nil, sections, limits)
}

func decodeIDVectorInto(dst [][]byte, sections []iwire.Section, limits iwire.Limits) ([][]byte, error) {
	rawIDs, err := metadataSection(sections, iwire.SectionDocumentIDs)
	if err != nil {
		return nil, err
	}
	ids, err := decodeByteVectorBorrowedInto(dst, rawIDs, limits)
	if err != nil {
		return nil, err
	}
	if err := rejectDuplicateIDs(ids); err != nil {
		return nil, err
	}
	return ids, nil
}

const maxSmallDuplicateIDs = 512

func rejectDuplicateIDs(ids [][]byte) error {
	if len(ids) <= maxSmallDuplicateIDs {
		return rejectDuplicateIDsSmall(ids)
	}
	return rejectDuplicateIDsMap(ids)
}

func rejectDuplicateIDsMap(ids [][]byte) error {
	seen := make(map[string]struct{}, len(ids))
	for i, id := range ids {
		if len(id) == 0 {
			return protocolError(iwire.ErrInvalidCommand, "document id cannot be empty at index %d", i)
		}
		key := string(id)
		if _, ok := seen[key]; ok {
			return protocolError(iwire.ErrDuplicateDocumentID, "duplicate document id at index %d", i)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func rejectDuplicateIDsSmall(ids [][]byte) error {
	if len(ids) > maxSmallDuplicateIDs {
		return rejectDuplicateIDsMap(ids)
	}
	tableSize := 1
	for tableSize < len(ids)*2 {
		tableSize <<= 1
	}
	var heads [maxSmallDuplicateIDs * 2]uint16
	if tableSize > len(heads) {
		return rejectDuplicateIDsMap(ids)
	}
	var next [maxSmallDuplicateIDs]uint16
	var hashes [maxSmallDuplicateIDs]uint64
	hashesView := hashes[:len(ids)]
	mask := uint64(tableSize - 1)
	for i, id := range ids {
		if len(id) == 0 {
			return protocolError(iwire.ErrInvalidCommand, "document id cannot be empty at index %d", i)
		}
		hash := hashDocumentID(id)
		bucket := int(hash & mask)
		for prev := heads[bucket]; prev != 0; prev = next[int(prev)-1] {
			j := int(prev) - 1
			if hashesView[j] != hash || len(ids[j]) != len(id) {
				continue
			}
			if bytes.Equal(ids[j], id) {
				return protocolError(iwire.ErrDuplicateDocumentID, "duplicate document id at index %d", i)
			}
		}
		hashesView[i] = hash
		next[i] = heads[bucket]
		heads[bucket] = uint16(i + 1)
	}
	return nil
}

func hashDocumentID(id []byte) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	hash := uint64(offset)
	for _, b := range id {
		hash ^= uint64(b)
		hash *= prime
	}
	return hash
}

type responseMetaCount struct {
	key   string
	value int
}

func ackMeta(policy AckPolicy) iwire.Section {
	return iwire.Section{ID: iwire.SectionResponseMeta, Bytes: appendAckMetaPayload(nil, policy)}
}

func ackMetaCounts(policy AckPolicy, counts ...responseMetaCount) iwire.Section {
	return iwire.Section{ID: iwire.SectionResponseMeta, Bytes: appendAckMetaPayload(nil, policy, counts...)}
}

func appendAckMetaSection(dst []byte, policy AckPolicy, counts ...responseMetaCount) ([]byte, error) {
	var payloadBuf [128]byte
	payload := appendAckMetaPayload(payloadBuf[:0], policy, counts...)
	body, err := iwire.AppendSectionHeader(dst, iwire.SectionResponseMeta, 0, len(payload))
	if err != nil {
		return nil, err
	}
	return append(body, payload...), nil
}

func appendAckMetaPayload(dst []byte, policy AckPolicy, counts ...responseMetaCount) []byte {
	dst = binary.AppendUvarint(dst, uint64(1+len(counts)))
	dst = appendStringUint(dst, "actual_ack_policy", uint64(policy))
	for _, count := range counts {
		dst = appendStringInt(dst, count.key, count.value)
	}
	return dst
}

func appendStringUint(dst []byte, key string, value uint64) []byte {
	dst = appendString(dst, key)
	var valueBuf [20]byte
	valueBytes := strconv.AppendUint(valueBuf[:0], value, 10)
	dst = binary.AppendUvarint(dst, uint64(len(valueBytes)))
	return append(dst, valueBytes...)
}

func appendStringInt(dst []byte, key string, value int) []byte {
	dst = appendString(dst, key)
	var valueBuf [20]byte
	valueBytes := strconv.AppendInt(valueBuf[:0], int64(value), 10)
	dst = binary.AppendUvarint(dst, uint64(len(valueBytes)))
	return append(dst, valueBytes...)
}

func responseCount(sections []iwire.Section, key string) (int, error) {
	raw, ok, err := singletonSection(sections, iwire.SectionResponseMeta)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, protocolError(iwire.ErrMalformedFrame, "missing response_meta")
	}
	values, err := decodeStringMap(raw)
	if err != nil {
		return 0, err
	}
	value := values[key]
	if value == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func updateBatchItems(ids, docs [][]byte) []collections.UpdateBatchItem {
	items := make([]collections.UpdateBatchItem, len(ids))
	for i := range ids {
		id := ids[i]
		doc := docs[i]
		items[i] = collections.UpdateBatchItem{
			DocumentID: id,
			Update: func(current []byte) ([]byte, bool, error) {
				if current == nil {
					return nil, false, nil
				}
				if bytes.Equal(current, doc) {
					return current, false, nil
				}
				return doc, true, nil
			},
		}
	}
	return items
}
