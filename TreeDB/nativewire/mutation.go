package nativewire

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"hash/maphash"
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
	templateV1HashMagic                = "TD1H"
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
	return ackPolicyFromPayload(raw, fallback)
}

func ackPolicyFromPayload(raw []byte, fallback AckPolicy) (AckPolicy, error) {
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
		case bytes.HasPrefix(doc, []byte(templateV1HashMagic)):
			out[i] = appendTemplateRecordEnvelope(nil, records, doc)
		case bytes.HasPrefix(doc, []byte(templateV1StoredMagic)):
			out[i] = doc
		default:
			return nil, protocolError(iwire.ErrInvalidCommand, "template-v1 document %d is not TD1I, TD1H, or TD1D", i)
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

const (
	maxSmallDuplicateIDs                  = 512
	duplicateIDSmallLoadFactor            = 2
	duplicateIDSmallHashTableSlots        = maxSmallDuplicateIDs * duplicateIDSmallLoadFactor
	_                              uint16 = maxSmallDuplicateIDs + 1
)

var (
	_ [0]struct{} = [duplicateIDSmallHashTableSlots - maxSmallDuplicateIDs*duplicateIDSmallLoadFactor]struct{}{}
	_ [0]struct{} = [duplicateIDSmallHashTableSlots & (duplicateIDSmallHashTableSlots - 1)]struct{}{}

	duplicateIDHashSeed = maphash.MakeSeed()
)

type duplicateIDScratch struct {
	heads  [duplicateIDSmallHashTableSlots]uint16
	next   [maxSmallDuplicateIDs]uint16
	hashes [maxSmallDuplicateIDs]uint64
}

func rejectDuplicateIDs(ids [][]byte) error {
	if len(ids) > maxSmallDuplicateIDs {
		return rejectDuplicateIDsMap(ids)
	}
	var scratch duplicateIDScratch
	return rejectDuplicateIDsSmall(ids, &scratch)
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

func rejectDuplicateIDsSmall(ids [][]byte, scratch *duplicateIDScratch) error {
	if scratch == nil || len(ids) > len(scratch.next) || len(ids) > len(scratch.hashes) {
		return rejectDuplicateIDsMap(ids)
	}
	tableSize := 1
	for tableSize < len(ids)*duplicateIDSmallLoadFactor {
		tableSize <<= 1
	}
	if tableSize > len(scratch.heads) {
		return rejectDuplicateIDsMap(ids)
	}
	heads := scratch.heads[:tableSize]
	clear(heads)
	next := scratch.next[:len(ids)]
	hashesView := scratch.hashes[:len(ids)]
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
	return maphash.Bytes(duplicateIDHashSeed, id)
}

type responseMetaCount struct {
	key   string
	value int
}

type responseMetaFields struct {
	count1            int
	hasCount1         bool
	count2            int
	hasCount2         bool
	catalogVersion    uint64
	hasCatalogVersion bool
}

func ackMeta(policy AckPolicy) iwire.Section {
	return iwire.Section{ID: iwire.SectionResponseMeta, Bytes: appendAckMetaPayload(nil, policy)}
}

func ackMetaCounts(policy AckPolicy, counts ...responseMetaCount) iwire.Section {
	return iwire.Section{ID: iwire.SectionResponseMeta, Bytes: appendAckMetaPayload(nil, policy, counts...)}
}

func ackMetaCountsVersion(policy AckPolicy, catalogVersion uint64, hasCatalogVersion bool, counts ...responseMetaCount) iwire.Section {
	return iwire.Section{ID: iwire.SectionResponseMeta, Bytes: appendAckMetaPayloadVersion(nil, policy, catalogVersion, hasCatalogVersion, counts...)}
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

func appendAckMetaSectionVersion(dst []byte, policy AckPolicy, catalogVersion uint64, hasCatalogVersion bool, counts ...responseMetaCount) ([]byte, error) {
	var payloadBuf [160]byte
	payload := appendAckMetaPayloadVersion(payloadBuf[:0], policy, catalogVersion, hasCatalogVersion, counts...)
	body, err := iwire.AppendSectionHeader(dst, iwire.SectionResponseMeta, 0, len(payload))
	if err != nil {
		return nil, err
	}
	return append(body, payload...), nil
}

func appendAckMetaPayload(dst []byte, policy AckPolicy, counts ...responseMetaCount) []byte {
	return appendAckMetaPayloadVersion(dst, policy, 0, false, counts...)
}

func appendAckMetaPayloadVersion(dst []byte, policy AckPolicy, catalogVersion uint64, hasCatalogVersion bool, counts ...responseMetaCount) []byte {
	fieldCount := 1 + len(counts)
	if hasCatalogVersion {
		fieldCount++
	}
	dst = binary.AppendUvarint(dst, uint64(fieldCount))
	dst = appendStringUint(dst, "actual_ack_policy", uint64(policy))
	if hasCatalogVersion {
		dst = appendStringUint(dst, "catalog_version", catalogVersion)
	}
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

func (s *Server) ackMeta(policy AckPolicy) iwire.Section {
	catalogVersion, hasCatalogVersion := s.mutationCatalogVersion()
	return ackMetaCountsVersion(policy, catalogVersion, hasCatalogVersion)
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
	fields, err := responseMetaFieldsFromSections(sections, key, "")
	if err != nil {
		return 0, err
	}
	if !fields.hasCount1 {
		return 0, protocolError(iwire.ErrMalformedFrame, "response_meta missing %s", key)
	}
	return fields.count1, nil
}

func responseCatalogVersion(sections []iwire.Section) (uint64, bool, error) {
	fields, err := responseMetaFieldsFromSections(sections, "", "")
	if err != nil {
		return 0, false, err
	}
	return fields.catalogVersion, fields.hasCatalogVersion, nil
}

func responseMetaFieldsFromSections(sections []iwire.Section, countKey1, countKey2 string) (responseMetaFields, error) {
	raw, ok, err := singletonSection(sections, iwire.SectionResponseMeta)
	if err != nil {
		return responseMetaFields{}, err
	}
	if !ok {
		return responseMetaFields{}, protocolError(iwire.ErrMalformedFrame, "missing response_meta")
	}
	return decodeResponseMetaFields(raw, countKey1, countKey2)
}

func decodeResponseMetaFields(src []byte, countKey1, countKey2 string) (responseMetaFields, error) {
	count, off, err := readUvarint(src)
	if err != nil {
		return responseMetaFields{}, err
	}
	if count > uint64(maxInt) {
		return responseMetaFields{}, protocolError(iwire.ErrResourceExhausted, "string map count exceeds int capacity")
	}
	if count > maxStringMapEntries {
		return responseMetaFields{}, protocolError(iwire.ErrResourceExhausted, "string map count %d exceeds limit %d", count, maxStringMapEntries)
	}
	var fields responseMetaFields
	for i := uint64(0); i < count; i++ {
		key, err := readStringBytes(src, &off)
		if err != nil {
			return responseMetaFields{}, err
		}
		value, err := readStringBytes(src, &off)
		if err != nil {
			return responseMetaFields{}, err
		}
		switch {
		case bytesEqualString(key, "catalog_version"):
			version, err := parseResponseMetaUint(value, "catalog_version")
			if err != nil {
				return responseMetaFields{}, err
			}
			fields.catalogVersion = version
			fields.hasCatalogVersion = true
		case countKey1 != "" && bytesEqualString(key, countKey1):
			n, err := parseResponseMetaInt(value, countKey1)
			if err != nil {
				return responseMetaFields{}, err
			}
			fields.count1 = n
			fields.hasCount1 = true
		case countKey2 != "" && bytesEqualString(key, countKey2):
			n, err := parseResponseMetaInt(value, countKey2)
			if err != nil {
				return responseMetaFields{}, err
			}
			fields.count2 = n
			fields.hasCount2 = true
		}
	}
	if off != len(src) {
		return responseMetaFields{}, protocolError(iwire.ErrMalformedFrame, "string map has %d trailing bytes", len(src)-off)
	}
	return fields, nil
}

func parseResponseMetaUint(src []byte, key string) (uint64, error) {
	if len(src) == 0 {
		return 0, protocolError(iwire.ErrMalformedFrame, "response_meta %s is not a uint64", key)
	}
	var value uint64
	for _, c := range src {
		if c < '0' || c > '9' {
			return 0, protocolError(iwire.ErrMalformedFrame, "response_meta %s is not a uint64", key)
		}
		digit := uint64(c - '0')
		if value > (^uint64(0)-digit)/10 {
			return 0, protocolError(iwire.ErrMalformedFrame, "response_meta %s is not a uint64", key)
		}
		value = value*10 + digit
	}
	return value, nil
}

func parseResponseMetaInt(src []byte, key string) (int, error) {
	if len(src) == 0 {
		return 0, protocolError(iwire.ErrMalformedFrame, "response_meta %s is not an integer", key)
	}
	negative := src[0] == '-'
	if negative {
		src = src[1:]
		if len(src) == 0 {
			return 0, protocolError(iwire.ErrMalformedFrame, "response_meta %s is not an integer", key)
		}
	}
	limit := uint64(maxInt)
	if negative {
		limit++
	}
	var value uint64
	for _, c := range src {
		if c < '0' || c > '9' {
			return 0, protocolError(iwire.ErrMalformedFrame, "response_meta %s is not an integer", key)
		}
		digit := uint64(c - '0')
		if value > (limit-digit)/10 {
			return 0, protocolError(iwire.ErrMalformedFrame, "response_meta %s is not an integer", key)
		}
		value = value*10 + digit
	}
	if negative {
		if value == limit {
			return -maxInt - 1, nil
		}
		return -int(value), nil
	}
	return int(value), nil
}

func bytesEqualString(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := range b {
		if b[i] != s[i] {
			return false
		}
	}
	return true
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
