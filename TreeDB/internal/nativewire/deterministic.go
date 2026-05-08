package nativewire

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"slices"

	"github.com/snissn/gomap/TreeDB/collections"
)

const (
	DeterministicEntryMagic   = "TDC1"
	DeterministicEntryVersion = uint64(1)

	maxDeterministicDocumentIDs = defaultMaxByteVectorItems

	deterministicSectionScratchCapacity = sectionSeenInlineCapacity

	deterministicCollectionRefTagName   = 1
	deterministicCollectionRefTagHandle = 2
	minDeterministicIndexDefinitionLen  = 6
)

func AppendDeterministicEntry(dst []byte, cmd ValidatedCommand) ([]byte, error) {
	if cmd.Schema == nil {
		return nil, protocolError(ErrInvalidCommand, "missing command schema")
	}
	if !cmd.Schema.Replicated {
		return nil, protocolError(ErrInvalidCommand, "command %s is not replicated", cmd.Schema.Name)
	}
	unsupportedFlags := UnsupportedDeterministicCommandFlags(cmd.Header.Flags)
	if unsupportedFlags != 0 {
		return nil, protocolError(ErrUnsupportedFeature, "unsupported deterministic command flags 0x%x", unsupportedFlags)
	}
	deterministicFlags := DeterministicCommandFlags(cmd.Header.Flags)

	var deterministicScratch [deterministicSectionScratchCapacity]Section
	deterministic, err := cmd.deterministicSectionsInto(deterministicScratch[:0])
	if err != nil {
		return nil, err
	}
	sortSectionsByID(deterministic)
	if cmd.Schema.RequiresIdempotency && deterministicSectionCount(deterministic, SectionIdempotencyKey) == 0 {
		return nil, protocolError(ErrInvalidCommand, "missing deterministic idempotency key")
	}
	if cmd.Schema.RequiresCatalogGuard && deterministicSectionCount(deterministic, SectionExpectedCatalogVersion) == 0 {
		return nil, protocolError(ErrInvalidCommand, "missing deterministic catalog guard")
	}
	if err := validateDeterministicCommand(cmd.Header.ID, deterministic); err != nil {
		return nil, err
	}

	dst = append(dst, DeterministicEntryMagic...)
	dst = appendUvarint(dst, DeterministicEntryVersion)
	dst = appendUvarint(dst, uint64(cmd.Header.ID))
	dst = appendUvarint(dst, cmd.Header.Version)
	dst = appendUvarint(dst, deterministicFlags)
	dst = appendUvarint(dst, uint64(len(deterministic)))
	for _, section := range deterministic {
		dst = appendUvarint(dst, uint64(section.ID))
		dst = appendUvarint(dst, uint64(len(section.Bytes)))
		dst = append(dst, section.Bytes...)
	}
	return dst, nil
}

func (cmd ValidatedCommand) hasSection(id SectionID) bool {
	for _, section := range cmd.Known {
		if section.ID == id {
			return true
		}
	}
	return false
}

func (cmd ValidatedCommand) deterministicSectionsInto(dst []Section) ([]Section, error) {
	if cmd.Schema == nil {
		return nil, protocolError(ErrInvalidCommand, "missing command schema")
	}
	rules := cmd.Schema.ruleMap()
	out := dst[:0]
	var seen sectionSeenSet
	for _, section := range cmd.Known {
		rule := rules[section.ID]
		if !rule.Deterministic {
			continue
		}
		if !rule.Repeatable {
			if seen.add(section.ID) > 1 {
				return nil, protocolError(ErrInvalidCommand, "duplicate deterministic singleton section %d", section.ID)
			}
		}
		if err := validateDeterministicSectionPayload(section); err != nil {
			return nil, err
		}
		out = append(out, Section{ID: section.ID, Bytes: section.Bytes})
	}
	return out, nil
}

func validateDeterministicCommand(commandID CommandID, deterministic []Section) error {
	switch commandID {
	case CommandInsertBatch, CommandReplaceBatch:
		idCount, err := deterministicByteVectorCount(deterministic, SectionDocumentIDs)
		if err != nil {
			return err
		}
		docCount, err := deterministicByteVectorCount(deterministic, SectionDocuments)
		if err != nil {
			return err
		}
		if idCount != docCount {
			return protocolError(ErrInvalidCommand, "document_ids length %d does not match documents length %d", idCount, docCount)
		}
	case CommandDeleteBatch:
		if _, err := deterministicByteVectorCount(deterministic, SectionDocumentIDs); err != nil {
			return err
		}
	case CommandCreateIndex, CommandDropIndex:
		raw, ok := deterministicSectionPayload(deterministic, SectionCollectionRef)
		if !ok {
			return protocolError(ErrInvalidCommand, "missing deterministic section %d", SectionCollectionRef)
		}
		local, err := validateDeterministicCollectionRef(raw, true)
		if err != nil {
			return err
		}
		if local {
			return protocolError(ErrInvalidCommand, "collection handle ref is not deterministic")
		}
	}
	return nil
}

func deterministicSectionPayload(sections []Section, id SectionID) ([]byte, bool) {
	for _, section := range sections {
		if section.ID == id {
			return section.Bytes, true
		}
	}
	return nil, false
}

func deterministicByteVectorCount(sections []Section, id SectionID) (int, error) {
	found := false
	var count int
	for _, section := range sections {
		if section.ID != id {
			continue
		}
		if found {
			return 0, protocolError(ErrInvalidCommand, "duplicate deterministic section %d", id)
		}
		count64, _, err := readUvarint(section.Bytes)
		if err != nil {
			return 0, err
		}
		if count64 > uint64(maxInt) {
			return 0, protocolError(ErrResourceExhausted, "section %d byte-vector count exceeds int capacity", id)
		}
		found = true
		count = int(count64)
	}
	if found {
		return count, nil
	}
	return 0, protocolError(ErrInvalidCommand, "missing deterministic section %d", id)
}

func deterministicSectionCount(sections []Section, id SectionID) int {
	count := 0
	for _, section := range sections {
		if section.ID == id {
			count++
		}
	}
	return count
}

func validateDeterministicSectionPayload(section Section) error {
	switch section.ID {
	case SectionCollectionRef:
		local, err := validateDeterministicCollectionRef(section.Bytes, false)
		if err != nil {
			return err
		}
		if local {
			return protocolError(ErrInvalidCommand, "collection handle ref is not deterministic")
		}
	case SectionDocumentFormat:
		format, n, err := readUvarint(section.Bytes)
		if err != nil {
			return err
		}
		if n != len(section.Bytes) {
			return protocolError(ErrMalformedFrame, "document_format has %d trailing bytes", len(section.Bytes)-n)
		}
		return validateDeterministicDocumentFormatEnum(format)
	case SectionDocumentIDs, SectionDocuments, SectionTemplateRecords:
		if section.ID == SectionDocumentIDs {
			return validateDeterministicDocumentIDs(section.Bytes)
		}
		return validateByteVector(section.Bytes, Limits{})
	case SectionAckPolicy:
		policy, n, err := readUvarint(section.Bytes)
		if err != nil {
			return err
		}
		if n != len(section.Bytes) {
			return protocolError(ErrMalformedFrame, "section %d has %d trailing bytes", section.ID, len(section.Bytes)-n)
		}
		if err := validateDeterministicAckPolicyEnum(policy); err != nil {
			return err
		}
	case SectionExpectedCatalogVersion, SectionReplacementMode:
		_, n, err := readUvarint(section.Bytes)
		if err != nil {
			return err
		}
		if n != len(section.Bytes) {
			return protocolError(ErrMalformedFrame, "section %d has %d trailing bytes", section.ID, len(section.Bytes)-n)
		}
	case SectionCollectionMeta:
		return validateDeterministicCollectionMeta(section.Bytes)
	case SectionIndexDefinition:
		return validateDeterministicIndexDefinition(section.Bytes, true)
	case SectionIndexName:
		return validateDeterministicEncodedName(section.Bytes, "index_name")
	}
	return nil
}

func validateDeterministicDocumentIDs(raw []byte) error {
	if err := validateByteVector(raw, Limits{}); err != nil {
		return err
	}
	count64, off, err := readUvarint(raw)
	if err != nil {
		return err
	}
	if count64 > uint64(maxInt) {
		return protocolError(ErrResourceExhausted, "document_ids count exceeds int capacity")
	}
	if count64 > maxDeterministicDocumentIDs {
		return protocolError(ErrResourceExhausted, "document_ids count %d exceeds deterministic limit %d", count64, maxDeterministicDocumentIDs)
	}
	count := int(count64)
	var stackItems [256]deterministicIDItem
	items := stackItems[:0]
	if count > cap(items) {
		items = make([]deterministicIDItem, count)
	} else {
		items = items[:count]
	}
	payloadLen := 0
	for i := 0; i < count; i++ {
		length, n, err := readUvarint(raw[off:])
		if err != nil {
			return err
		}
		off += n
		if length == 0 {
			return protocolError(ErrInvalidCommand, "empty document id at index %d", i)
		}
		if length > uint64(maxInt) || payloadLen > maxInt-int(length) {
			return protocolError(ErrResourceExhausted, "document_ids payload length exceeds int capacity")
		}
		items[i] = deterministicIDItem{offset: payloadLen, length: int(length)}
		payloadLen += int(length)
	}
	payload := raw[off:]
	if payloadLen != len(payload) {
		return protocolError(ErrMalformedFrame, "document_ids payload length %d does not match declared lengths %d", len(payload), payloadLen)
	}
	sortDeterministicIDItems(payload, items)
	for i := 1; i < count; i++ {
		left := payload[items[i-1].offset : items[i-1].offset+items[i-1].length]
		right := payload[items[i].offset : items[i].offset+items[i].length]
		if bytes.Equal(left, right) {
			return protocolError(ErrDuplicateDocumentID, "duplicate document id")
		}
	}
	return nil
}

type deterministicIDItem struct {
	offset int
	length int
}

func sortDeterministicIDItems(payload []byte, items []deterministicIDItem) {
	slices.SortFunc(items, func(leftItem, rightItem deterministicIDItem) int {
		left := payload[leftItem.offset : leftItem.offset+leftItem.length]
		right := payload[rightItem.offset : rightItem.offset+rightItem.length]
		return bytes.Compare(left, right)
	})
}

func validateDeterministicCollectionRef(raw []byte, requireTaggedName bool) (bool, error) {
	if len(raw) == 0 {
		return false, protocolError(ErrInvalidCommand, "empty collection_ref")
	}
	switch raw[0] {
	case deterministicCollectionRefTagName:
		return false, validateDeterministicName(raw[1:], "collection name")
	case deterministicCollectionRefTagHandle:
		_, n, err := readUvarint(raw[1:])
		if err != nil {
			return true, err
		}
		if n+1 != len(raw) {
			return true, protocolError(ErrMalformedFrame, "collection handle ref has trailing bytes")
		}
		return true, nil
	default:
		if requireTaggedName {
			return false, protocolError(ErrMalformedFrame, "collection_ref must use tagged collection name")
		}
		return false, validateDeterministicName(raw, "collection name")
	}
}

func validateDeterministicName(raw []byte, field string) error {
	name := string(raw)
	switch field {
	case "collection name":
		if err := collections.ValidateCollectionName(name); err != nil {
			return protocolError(ErrInvalidCommand, "%v", err)
		}
	default:
		if err := collections.ValidateIndexName(name); err != nil {
			return protocolError(ErrInvalidCommand, "%v", err)
		}
	}
	return nil
}

func validateDeterministicIndexPath(path string, field string) error {
	if err := collections.ValidateIndexPath(path); err != nil {
		return protocolError(ErrInvalidCommand, "%v", err)
	}
	return nil
}

func validateDeterministicDocumentFormatEnum(value uint64) error {
	switch DocumentFormat(value) {
	case DocumentFormatDefault, DocumentFormatJSON, DocumentFormatBSON, DocumentFormatTemplateV1:
		return nil
	default:
		return protocolError(ErrInvalidCommand, "unsupported document_format enum %d", value)
	}
}

func validateDeterministicRootStorageEnum(value uint64) error {
	switch value {
	case 0, 1, 2:
		return nil
	default:
		return protocolError(ErrInvalidCommand, "unsupported root_storage enum %d", value)
	}
}

func validateDeterministicIndexValueTypeEnum(value uint64) error {
	switch value {
	case 1, 2, 3, 4:
		return nil
	default:
		return protocolError(ErrInvalidCommand, "unsupported index_value_type enum %d", value)
	}
}

func validateDeterministicAckPolicyEnum(value uint64) error {
	switch AckPolicy(value) {
	case 0, AckVisible, AckFlushed, AckSynced, AckRaftCommitted:
		return nil
	default:
		return protocolError(ErrInvalidCommand, "unsupported ack_policy enum %d", value)
	}
}

func validateDeterministicCollectionMeta(raw []byte) error {
	off := 0
	version, err := readDeterministicUvarintField(raw, &off, "collection_meta.version")
	if err != nil {
		return err
	}
	if version != 1 {
		return protocolError(ErrUnsupportedVersion, "collection_meta version %d", version)
	}
	if err := readDeterministicNameField(raw, &off, "collection name"); err != nil {
		return err
	}
	documentFormat, err := readDeterministicUvarintField(raw, &off, "document_format")
	if err != nil {
		return err
	}
	if err := validateDeterministicDocumentFormatEnum(documentFormat); err != nil {
		return err
	}
	dataRootStorage, err := readDeterministicUvarintField(raw, &off, "data_root_storage")
	if err != nil {
		return err
	}
	if err := validateDeterministicRootStorageEnum(dataRootStorage); err != nil {
		return err
	}
	indexStateStorage, err := readDeterministicUvarintField(raw, &off, "index_state_storage")
	if err != nil {
		return err
	}
	if err := validateDeterministicRootStorageEnum(indexStateStorage); err != nil {
		return err
	}
	for _, field := range []string{"allow_array_values_in_index", "disable_indexed_write_memtables", "buffered_indexed_writes"} {
		if err := readDeterministicBoolField(raw, &off, field); err != nil {
			return err
		}
	}
	for _, field := range []string{"buffered_indexed_write_max_documents", "buffered_indexed_write_max_bytes", "buffered_indexed_write_max_root_runs"} {
		if _, err := readDeterministicVarintField(raw, &off, field); err != nil {
			return err
		}
	}
	for _, field := range []string{"buffered_indexed_async_flush", "buffered_indexed_overlay_roots"} {
		if err := readDeterministicBoolField(raw, &off, field); err != nil {
			return err
		}
	}
	if _, err := readDeterministicVarintField(raw, &off, "buffered_indexed_async_flush_max_queued_units"); err != nil {
		return err
	}
	indexCount, err := readDeterministicUvarintField(raw, &off, "index_count")
	if err != nil {
		return err
	}
	if indexCount > uint64(maxInt) {
		return protocolError(ErrResourceExhausted, "index count exceeds int capacity")
	}
	if indexCount > uint64((len(raw)-off)/minDeterministicIndexDefinitionLen) {
		return protocolError(ErrMalformedFrame, "index count %d exceeds remaining collection_meta payload", indexCount)
	}
	for i := uint64(0); i < indexCount; i++ {
		next, err := validateDeterministicIndexDefinitionAt(raw, off, false)
		if err != nil {
			return err
		}
		off = next
	}
	if off != len(raw) {
		return protocolError(ErrMalformedFrame, "collection_meta has %d trailing bytes", len(raw)-off)
	}
	return nil
}

func validateDeterministicIndexDefinition(raw []byte, withVersion bool) error {
	off, err := validateDeterministicIndexDefinitionAt(raw, 0, withVersion)
	if err != nil {
		return err
	}
	if off != len(raw) {
		return protocolError(ErrMalformedFrame, "index_definition has %d trailing bytes", len(raw)-off)
	}
	return nil
}

func validateDeterministicIndexDefinitionAt(raw []byte, off int, withVersion bool) (int, error) {
	if withVersion {
		version, err := readDeterministicUvarintField(raw, &off, "index_definition.version")
		if err != nil {
			return 0, err
		}
		if version != 1 {
			return 0, protocolError(ErrUnsupportedVersion, "index_definition version %d", version)
		}
	}
	if err := readDeterministicNameField(raw, &off, "index name"); err != nil {
		return 0, err
	}
	field, err := readDeterministicStringField(raw, &off, "index field")
	if err != nil {
		return 0, err
	}
	if err := validateDeterministicIndexPath(field, "index field"); err != nil {
		return 0, err
	}
	valueType, err := readDeterministicUvarintField(raw, &off, "index value type")
	if err != nil {
		return 0, err
	}
	if err := validateDeterministicIndexValueTypeEnum(valueType); err != nil {
		return 0, err
	}
	if err := readDeterministicBoolField(raw, &off, "unique"); err != nil {
		return 0, err
	}
	if err := readDeterministicBoolField(raw, &off, "multi_key"); err != nil {
		return 0, err
	}
	storagePolicy, err := readDeterministicUvarintField(raw, &off, "index storage policy")
	if err != nil {
		return 0, err
	}
	if err := validateDeterministicRootStorageEnum(storagePolicy); err != nil {
		return 0, err
	}
	return off, nil
}

func validateDeterministicEncodedName(raw []byte, field string) error {
	off := 0
	if err := readDeterministicNameField(raw, &off, field); err != nil {
		return err
	}
	if off != len(raw) {
		return protocolError(ErrMalformedFrame, "%s has %d trailing bytes", field, len(raw)-off)
	}
	return nil
}

func readDeterministicNameField(raw []byte, off *int, field string) error {
	value, err := readDeterministicStringField(raw, off, field)
	if err != nil {
		return err
	}
	switch field {
	case "collection name":
		if err := collections.ValidateCollectionName(value); err != nil {
			return protocolError(ErrInvalidCommand, "%v", err)
		}
	default:
		if err := collections.ValidateIndexName(value); err != nil {
			return protocolError(ErrInvalidCommand, "%v", err)
		}
	}
	return nil
}

func readDeterministicStringField(raw []byte, off *int, field string) (string, error) {
	length, err := readDeterministicUvarintField(raw, off, field+".length")
	if err != nil {
		return "", err
	}
	if length > uint64(len(raw)-*off) {
		return "", protocolError(ErrMalformedFrame, "%s length exceeds remaining payload", field)
	}
	start := *off
	*off += int(length)
	return string(raw[start:*off]), nil
}

func readDeterministicUvarintField(raw []byte, off *int, field string) (uint64, error) {
	if off == nil || *off < 0 || *off > len(raw) {
		return 0, protocolError(ErrMalformedFrame, "invalid %s offset", field)
	}
	value, n, err := readUvarint(raw[*off:])
	if err != nil {
		return 0, err
	}
	*off += n
	return value, nil
}

func readDeterministicVarintField(raw []byte, off *int, field string) (int64, error) {
	if off == nil || *off < 0 || *off > len(raw) {
		return 0, protocolError(ErrMalformedFrame, "invalid %s offset", field)
	}
	value, n := binary.Varint(raw[*off:])
	if n <= 0 {
		return 0, protocolError(ErrMalformedFrame, "invalid %s", field)
	}
	if !isMinimalVarint(value, n) {
		return 0, protocolError(ErrMalformedFrame, "non-minimal %s", field)
	}
	*off += n
	return value, nil
}

func isMinimalVarint(value int64, n int) bool {
	var buf [binary.MaxVarintLen64]byte
	return n == binary.PutVarint(buf[:], value)
}

func readDeterministicBoolField(raw []byte, off *int, field string) error {
	if off == nil || *off < 0 || *off >= len(raw) {
		return protocolError(ErrMalformedFrame, "missing %s", field)
	}
	value := raw[*off]
	*off = *off + 1
	switch value {
	case 0, 1:
		return nil
	default:
		return protocolError(ErrMalformedFrame, "invalid %s bool %d", field, value)
	}
}

func sortSectionsByID(sections []Section) {
	slices.SortStableFunc(sections, func(a, b Section) int {
		return cmp.Compare(a.ID, b.ID)
	})
}
