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
)

func documentFormatSection(format collections.DocumentFormat) iwire.Section {
	return iwire.Section{ID: iwire.SectionDocumentFormat, Bytes: binary.AppendUvarint(nil, uint64(encodeDocumentFormat(format)))}
}

func decodeDocumentFormatSection(sections []iwire.Section) (collections.DocumentFormat, error) {
	raw, err := metadataSection(sections, iwire.SectionDocumentFormat)
	if err != nil {
		return "", err
	}
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
	value, n, err := readUvarint(raw)
	if err != nil {
		return 0, err
	}
	if n != len(raw) {
		return 0, protocolError(iwire.ErrMalformedFrame, "ack_policy has trailing bytes")
	}
	switch AckPolicy(value) {
	case iwire.AckVisible, iwire.AckFlushed, iwire.AckSynced, iwire.AckRaftCommitted:
		return AckPolicy(value), nil
	default:
		return 0, protocolError(iwire.ErrInvalidCommand, "unsupported ack policy %d", value)
	}
}

func decodeIDsAndDocuments(sections []iwire.Section, limits iwire.Limits) ([][]byte, [][]byte, error) {
	rawIDs, err := metadataSection(sections, iwire.SectionDocumentIDs)
	if err != nil {
		return nil, nil, err
	}
	ids, err := decodeByteVectorCloned(rawIDs, limits)
	if err != nil {
		return nil, nil, err
	}
	rawDocs, err := metadataSection(sections, iwire.SectionDocuments)
	if err != nil {
		return nil, nil, err
	}
	docs, err := decodeByteVectorCloned(rawDocs, limits)
	if err != nil {
		return nil, nil, err
	}
	if len(ids) != len(docs) {
		return nil, nil, protocolError(iwire.ErrInvalidCommand, "document_ids length %d does not match documents length %d", len(ids), len(docs))
	}
	if err := rejectDuplicateIDs(ids); err != nil {
		return nil, nil, err
	}
	return ids, docs, nil
}

func decodeIDVector(sections []iwire.Section, limits iwire.Limits) ([][]byte, error) {
	rawIDs, err := metadataSection(sections, iwire.SectionDocumentIDs)
	if err != nil {
		return nil, err
	}
	ids, err := decodeByteVectorCloned(rawIDs, limits)
	if err != nil {
		return nil, err
	}
	if err := rejectDuplicateIDs(ids); err != nil {
		return nil, err
	}
	return ids, nil
}

func rejectDuplicateIDs(ids [][]byte) error {
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

func ackMeta(policy AckPolicy, pairs ...string) iwire.Section {
	values := map[string]string{
		"actual_ack_policy": strconv.FormatUint(uint64(policy), 10),
	}
	for i := 0; i+1 < len(pairs); i += 2 {
		values[pairs[i]] = pairs[i+1]
	}
	return iwire.Section{ID: iwire.SectionResponseMeta, Bytes: appendStringMap(nil, values)}
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
		id := append([]byte(nil), ids[i]...)
		doc := append([]byte(nil), docs[i]...)
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
