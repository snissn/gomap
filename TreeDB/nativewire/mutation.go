package nativewire

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"strconv"
	"strings"

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
	templateV1InputMagic               = "TD1I"
	templateV1StoredMagic              = "TD1D"
	templateV1RecordMagic              = "TD1T"
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
	case 0:
		return fallback, nil
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

func applyTemplateRecords(format collections.DocumentFormat, sections []iwire.Section, docs [][]byte, limits iwire.Limits) ([][]byte, error) {
	raw, ok, err := singletonSection(sections, iwire.SectionTemplateRecords)
	if err != nil || !ok {
		return docs, err
	}
	if format != collections.DocumentFormatTemplateV1 {
		return nil, protocolError(iwire.ErrInvalidCommand, "template_records require template-v1 document format")
	}
	records, err := decodeByteVectorCloned(raw, limits)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return docs, nil
	}
	for i, record := range records {
		if err := validateTemplateRecord(record); err != nil {
			return nil, protocolError(iwire.ErrInvalidCommand, "template_records[%d]: %v", i, err)
		}
	}
	out := make([][]byte, len(docs))
	for i, doc := range docs {
		switch {
		case bytes.HasPrefix(doc, []byte(templateV1InputMagic)):
			out[i] = doc
		case bytes.HasPrefix(doc, []byte(templateV1StoredMagic)):
			out[i] = appendTemplateRecordEnvelope(nil, records, doc)
		default:
			return nil, protocolError(iwire.ErrInvalidCommand, "template-v1 document %d is not TD1I or TD1D", i)
		}
	}
	return out, nil
}

func appendTemplateRecordEnvelope(dst []byte, records [][]byte, stored []byte) []byte {
	dst = append(dst, templateV1InputMagic...)
	dst = binary.AppendUvarint(dst, uint64(len(records)))
	for _, record := range records {
		id := sha256.Sum256(record)
		dst = append(dst, id[:]...)
		dst = binary.AppendUvarint(dst, uint64(len(record)))
		dst = append(dst, record...)
	}
	return append(dst, stored...)
}

func validateTemplateRecord(raw []byte) error {
	pos := 0
	if !consumeTemplateMagic(raw, &pos, templateV1RecordMagic) {
		return protocolError(iwire.ErrMalformedFrame, "malformed template-v1 template record")
	}
	fieldCount, err := readUvarintField(raw, &pos, "template field count")
	if err != nil {
		return err
	}
	if fieldCount > uint64(len(raw)) {
		return protocolError(iwire.ErrMalformedFrame, "malformed template-v1 field count")
	}
	var previous string
	for i := uint64(0); i < fieldCount; i++ {
		fieldLen, err := readUvarintField(raw, &pos, "template field length")
		if err != nil {
			return err
		}
		if fieldLen > uint64(len(raw)-pos) {
			return protocolError(iwire.ErrMalformedFrame, "malformed template-v1 field length")
		}
		field := string(raw[pos : pos+int(fieldLen)])
		pos += int(fieldLen)
		if field == "" || strings.ContainsAny(field, "\x00.") {
			return protocolError(iwire.ErrInvalidCommand, "invalid template-v1 field %q", field)
		}
		if i > 0 && previous >= field {
			return protocolError(iwire.ErrInvalidCommand, "template-v1 fields are not strictly sorted")
		}
		previous = field
	}
	if pos != len(raw) {
		return protocolError(iwire.ErrMalformedFrame, "trailing template-v1 template bytes")
	}
	return nil
}

func consumeTemplateMagic(raw []byte, pos *int, magic string) bool {
	if pos == nil || *pos < 0 || len(raw)-*pos < len(magic) {
		return false
	}
	if string(raw[*pos:*pos+len(magic)]) != magic {
		return false
	}
	*pos += len(magic)
	return true
}

func decodeIDVector(sections []iwire.Section, limits iwire.Limits) ([][]byte, error) {
	rawIDs, err := metadataSection(sections, iwire.SectionDocumentIDs)
	if err != nil {
		return nil, err
	}
	vec, err := iwire.DecodeByteVector(rawIDs, limits)
	if err != nil {
		return nil, err
	}
	if err := rejectDuplicateByteVectorIDs(vec); err != nil {
		return nil, err
	}
	ids := make([][]byte, vec.Len())
	for i := 0; i < vec.Len(); i++ {
		item, _ := vec.Item(i)
		ids[i] = append([]byte(nil), item...)
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

func rejectDuplicateByteVectorIDs(vec iwire.ByteVector) error {
	seen := make(map[string]struct{}, vec.Len())
	for i := 0; i < vec.Len(); i++ {
		id, _ := vec.Item(i)
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

func (s *Server) ackMeta(policy AckPolicy, pairs ...string) iwire.Section {
	if version, err := s.currentCatalogVersion(); err == nil {
		pairs = append(pairs, "catalog_version", strconv.FormatUint(version, 10))
	}
	return ackMeta(policy, pairs...)
}

func responseMetaMap(sections []iwire.Section) (map[string]string, error) {
	raw, ok, err := singletonSection(sections, iwire.SectionResponseMeta)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, protocolError(iwire.ErrMalformedFrame, "missing response_meta")
	}
	return decodeStringMap(raw)
}

func responseCount(sections []iwire.Section, key string) (int, error) {
	values, err := responseMetaMap(sections)
	if err != nil {
		return 0, err
	}
	value, ok := values[key]
	if !ok {
		return 0, protocolError(iwire.ErrMalformedFrame, "response_meta missing %s", key)
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, protocolError(iwire.ErrMalformedFrame, "response_meta %s is not an integer", key)
	}
	return n, nil
}

func responseCatalogVersion(sections []iwire.Section) (uint64, bool, error) {
	values, err := responseMetaMap(sections)
	if err != nil {
		return 0, false, err
	}
	value, ok := values["catalog_version"]
	if !ok {
		return 0, false, nil
	}
	version, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, true, protocolError(iwire.ErrMalformedFrame, "response_meta catalog_version is not a uint64")
	}
	return version, true, nil
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
