package nativewire

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	DeterministicEntryMagic   = "TDC1"
	DeterministicEntryVersion = uint64(1)

	maxDeterministicDocumentIDs = defaultMaxByteVectorItems

	deterministicSectionScratchCapacity = sectionSeenInlineCapacity
	maxDeterministicEntrySections       = defaultMaxSections

	deterministicCollectionRefTagName   = 1
	deterministicCollectionRefTagHandle = 2
	deterministicReplacementExisting    = 1
	minDeterministicIndexDefinitionLen  = 6
)

var (
	deterministicEntryRegistryOnce sync.Once
	deterministicEntryRegistry     *Registry
)

func deterministicRegistry() *Registry {
	deterministicEntryRegistryOnce.Do(func() {
		deterministicEntryRegistry = MustV1Registry()
	})
	return deterministicEntryRegistry
}

type DeterministicEntry struct {
	Version        uint64
	CommandID      CommandID
	CommandVersion uint64
	CommandFlags   uint64
	Sections       []Section
}

// DeterministicEntryScratch carries reusable buffers for
// DecodeDeterministicEntryInto.
type DeterministicEntryScratch struct {
	Sections []Section
}

func AppendDeterministicEntry(dst []byte, cmd ValidatedCommand) ([]byte, error) {
	if cmd.Schema == nil {
		return nil, protocolError(ErrInvalidCommand, "missing command schema")
	}
	if !cmd.Schema.Replicated {
		return nil, protocolError(ErrInvalidCommand, "command %s is not replicated", cmd.Schema.Name)
	}
	if cmd.Schema.RequiresIdempotency && !cmd.hasSection(SectionIdempotencyKey) {
		return nil, protocolError(ErrInvalidCommand, "missing idempotency key")
	}
	if cmd.Schema.RequiresCatalogGuard && !cmd.hasSection(SectionExpectedCatalogVersion) {
		return nil, protocolError(ErrInvalidCommand, "missing catalog guard")
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

func DecodeDeterministicEntry(src []byte, limits Limits) (DeterministicEntry, error) {
	return DecodeDeterministicEntryInto(src, limits, nil)
}

// DecodeDeterministicEntryInto decodes a deterministic command-entry envelope.
//
// The returned section payloads borrow from src and the section slice may borrow
// from scratch. Callers that need to retain the entry after reusing src or
// scratch must copy it.
func DecodeDeterministicEntryInto(src []byte, limits Limits, scratch *DeterministicEntryScratch) (DeterministicEntry, error) {
	limits = limits.withDefaults()
	failBeforeSections := func(err error) (DeterministicEntry, error) {
		clearDeterministicEntryScratch(nil, scratch, true)
		return DeterministicEntry{}, err
	}
	if uint64(len(src)) > limits.MaxFrameSize {
		return failBeforeSections(protocolError(ErrResourceExhausted, "deterministic entry length %d exceeds limit %d", len(src), limits.MaxFrameSize))
	}
	if len(src) < len(DeterministicEntryMagic) ||
		src[0] != DeterministicEntryMagic[0] ||
		src[1] != DeterministicEntryMagic[1] ||
		src[2] != DeterministicEntryMagic[2] ||
		src[3] != DeterministicEntryMagic[3] {
		return failBeforeSections(protocolError(ErrMalformedFrame, "bad deterministic entry magic"))
	}
	off := len(DeterministicEntryMagic)
	version, err := readEntryUvarint(src, &off, "entry_version")
	if err != nil {
		return failBeforeSections(err)
	}
	if version != DeterministicEntryVersion {
		return failBeforeSections(protocolError(ErrUnsupportedVersion, "deterministic entry version %d", version))
	}
	commandID, err := readEntryUvarint(src, &off, "command_id")
	if err != nil {
		return failBeforeSections(err)
	}
	commandVersion, err := readEntryUvarint(src, &off, "command_version")
	if err != nil {
		return failBeforeSections(err)
	}
	commandFlags, err := readEntryUvarint(src, &off, "command_flags")
	if err != nil {
		return failBeforeSections(err)
	}
	if commandFlags != 0 {
		return failBeforeSections(protocolError(ErrUnsupportedFeature, "unsupported deterministic command flags 0x%x", commandFlags))
	}
	sectionCount64, err := readEntryUvarint(src, &off, "section_count")
	if err != nil {
		return failBeforeSections(err)
	}
	if sectionCount64 > uint64(limits.MaxSections) {
		return failBeforeSections(protocolError(ErrResourceExhausted, "deterministic entry section count %d exceeds limit %d", sectionCount64, limits.MaxSections))
	}
	if sectionCount64 > maxDeterministicEntrySections {
		return failBeforeSections(protocolError(ErrResourceExhausted, "deterministic entry section count %d exceeds deterministic limit %d", sectionCount64, maxDeterministicEntrySections))
	}
	if sectionCount64 > uint64(maxInt) {
		return failBeforeSections(protocolError(ErrResourceExhausted, "deterministic entry section count exceeds int capacity"))
	}
	if sectionCount64 > uint64((len(src)-off)/2) {
		return failBeforeSections(protocolError(ErrMalformedFrame, "deterministic entry section count %d exceeds remaining header bytes %d", sectionCount64, len(src)-off))
	}
	sectionCount := int(sectionCount64)
	sections, borrowedScratch := deterministicEntrySectionsBuffer(sectionCount, scratch)
	fail := func(err error) (DeterministicEntry, error) {
		clearDeterministicEntryScratch(sections, scratch, borrowedScratch)
		return DeterministicEntry{}, err
	}
	var previous SectionID
	for i := 0; i < sectionCount; i++ {
		id, err := readEntryUvarint(src, &off, "section_id")
		if err != nil {
			return fail(err)
		}
		sectionID := SectionID(id)
		if i > 0 && sectionID < previous {
			return fail(protocolError(ErrMalformedFrame, "deterministic entry sections are not sorted"))
		}
		previous = sectionID
		sectionLen, err := readEntryUvarint(src, &off, "section_length")
		if err != nil {
			return fail(err)
		}
		if sectionLen > limits.MaxSectionLen {
			return fail(protocolError(ErrResourceExhausted, "deterministic entry section %d length %d exceeds limit %d", sectionID, sectionLen, limits.MaxSectionLen))
		}
		if sectionLen > uint64(maxInt) {
			return fail(protocolError(ErrResourceExhausted, "deterministic entry section %d length exceeds int capacity", sectionID))
		}
		if sectionLen > uint64(len(src)-off) {
			return fail(protocolError(ErrMalformedFrame, "deterministic entry section %d length %d exceeds remaining %d", sectionID, sectionLen, len(src)-off))
		}
		section := Section{ID: sectionID, Bytes: src[off : off+int(sectionLen)]}
		if err := validateDeterministicSectionPayload(section, limits); err != nil {
			return fail(err)
		}
		sections[i] = section
		off += int(sectionLen)
	}
	if off != len(src) {
		return fail(protocolError(ErrMalformedFrame, "deterministic entry has %d trailing bytes", len(src)-off))
	}
	if _, err := validateDecodedDeterministicEntry(CommandID(commandID), commandVersion, sections); err != nil {
		return fail(err)
	}
	if scratch != nil {
		scratch.Sections = sections
	}
	return DeterministicEntry{
		Version:        version,
		CommandID:      CommandID(commandID),
		CommandVersion: commandVersion,
		CommandFlags:   commandFlags,
		Sections:       sections,
	}, nil
}

func deterministicEntrySectionsBuffer(count int, scratch *DeterministicEntryScratch) ([]Section, bool) {
	if scratch == nil {
		return make([]Section, count), false
	}
	if cap(scratch.Sections) < count {
		return make([]Section, count), false
	}
	backing := scratch.Sections[:cap(scratch.Sections)]
	clear(backing[count:])
	return backing[:count], true
}

func clearDeterministicEntryScratch(sections []Section, scratch *DeterministicEntryScratch, borrowedScratch bool) {
	if scratch == nil {
		return
	}
	if borrowedScratch {
		clear(sections)
		scratch.Sections = sections[:0]
		return
	}
	clear(scratch.Sections[:cap(scratch.Sections)])
	scratch.Sections = scratch.Sections[:0]
}

func readEntryUvarint(src []byte, off *int, field string) (uint64, error) {
	if off == nil || *off >= len(src) {
		return 0, protocolError(ErrMalformedFrame, "invalid deterministic entry offset for %s", field)
	}
	value, n, err := readUvarint(src[*off:])
	if err != nil {
		return 0, err
	}
	*off += n
	return value, nil
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
		seenCount := seen.add(section.ID)
		if !rule.Repeatable && seenCount > 1 {
			return nil, protocolError(ErrInvalidCommand, "duplicate deterministic singleton section %d", section.ID)
		}
		if err := validateDeterministicSectionPayload(section, Limits{}); err != nil {
			return nil, err
		}
		out = append(out, Section{ID: section.ID, Bytes: section.Bytes})
	}
	return out, nil
}

func validateDecodedDeterministicEntry(commandID CommandID, commandVersion uint64, sections []Section) (*CommandSchema, error) {
	schema, ok := deterministicRegistry().LookupCommand(commandID, commandVersion)
	if !ok {
		return nil, protocolError(ErrUnsupportedVersion, "unsupported deterministic command %d version %d", commandID, commandVersion)
	}
	if !schema.Replicated || schema.LocalOnly {
		return nil, protocolError(ErrInvalidCommand, "command %s is not replicated", schema.Name)
	}
	rules := schema.ruleMap()
	var seen sectionSeenSet
	for _, section := range sections {
		rule, ok := rules[section.ID]
		if !ok || !rule.Deterministic {
			return nil, protocolError(ErrInvalidCommand, "section %d is not deterministic for command %s", section.ID, schema.Name)
		}
		seenCount := seen.add(section.ID)
		if !rule.Repeatable && seenCount > 1 {
			return nil, protocolError(ErrInvalidCommand, "duplicate deterministic singleton section %d", section.ID)
		}
	}
	if schema.RequiresIdempotency && seen.get(SectionIdempotencyKey) == 0 {
		return nil, protocolError(ErrInvalidCommand, "missing idempotency identity")
	}
	if schema.RequiresCatalogGuard && seen.get(SectionExpectedCatalogVersion) == 0 {
		return nil, protocolError(ErrInvalidCommand, "missing catalog guard")
	}
	for _, rule := range schema.Sections {
		if rule.Required && rule.Deterministic && seen.get(rule.ID) == 0 {
			return nil, protocolError(ErrInvalidCommand, "missing deterministic section %d", rule.ID)
		}
	}
	if err := validateDeterministicCommand(commandID, sections); err != nil {
		return nil, err
	}
	return schema, nil
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
	for _, section := range sections {
		if section.ID != id {
			continue
		}
		count64, _, err := readUvarint(section.Bytes)
		if err != nil {
			return 0, err
		}
		if count64 > uint64(maxInt) {
			return 0, protocolError(ErrResourceExhausted, "section %d byte-vector count exceeds int capacity", id)
		}
		return int(count64), nil
	}
	return 0, protocolError(ErrInvalidCommand, "missing deterministic section %d", id)
}

func validateDeterministicSectionPayload(section Section, limits Limits) error {
	switch section.ID {
	case SectionCollectionRef:
		local, err := validateDeterministicCollectionRef(section.Bytes, true)
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
		switch DocumentFormat(format) {
		case DocumentFormatDefault, DocumentFormatJSON, DocumentFormatBSON, DocumentFormatTemplateV1:
		default:
			return protocolError(ErrInvalidCommand, "unsupported document_format %d", format)
		}
	case SectionDocumentIDs, SectionDocuments, SectionTemplateRecords:
		if section.ID == SectionDocumentIDs {
			return validateDeterministicDocumentIDs(section.Bytes, limits)
		}
		return validateByteVector(section.Bytes, limits)
	case SectionExpectedCatalogVersion:
		_, n, err := readUvarint(section.Bytes)
		if err != nil {
			return err
		}
		if n != len(section.Bytes) {
			return protocolError(ErrMalformedFrame, "section %d has %d trailing bytes", section.ID, len(section.Bytes)-n)
		}
	case SectionReplacementMode:
		mode, n, err := readUvarint(section.Bytes)
		if err != nil {
			return err
		}
		if n != len(section.Bytes) {
			return protocolError(ErrMalformedFrame, "section %d has %d trailing bytes", section.ID, len(section.Bytes)-n)
		}
		if mode != deterministicReplacementExisting {
			return protocolError(ErrInvalidCommand, "unsupported replacement_mode %d", mode)
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

func validateDeterministicDocumentIDs(raw []byte, limits Limits) error {
	if err := validateByteVector(raw, limits); err != nil {
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
	if len(raw) == 0 {
		return protocolError(ErrInvalidCommand, "%s cannot be empty", field)
	}
	name := string(raw)
	if strings.ContainsAny(name, "\x00/:") || strings.TrimSpace(name) != name || !utf8.ValidString(name) {
		return protocolError(ErrInvalidCommand, "invalid %s", field)
	}
	if len(name) > 128 {
		return protocolError(ErrInvalidCommand, "%s too long", field)
	}
	return nil
}

func validateDeterministicIndexPath(path string, field string) error {
	if len(path) == 0 {
		return protocolError(ErrInvalidCommand, "%s cannot be empty", field)
	}
	if strings.Contains(path, "\x00") {
		return protocolError(ErrInvalidCommand, "%s cannot contain NUL", field)
	}
	if strings.HasPrefix(path, ".") || strings.HasSuffix(path, ".") || strings.Contains(path, "..") {
		return protocolError(ErrInvalidCommand, "%s cannot contain empty segments", field)
	}
	return nil
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
	for _, field := range []string{"document_format", "data_root_storage", "index_state_storage"} {
		if _, err := readDeterministicUvarintField(raw, &off, field); err != nil {
			return err
		}
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
	if _, err := readDeterministicUvarintField(raw, &off, "index value type"); err != nil {
		return 0, err
	}
	if err := readDeterministicBoolField(raw, &off, "unique"); err != nil {
		return 0, err
	}
	if err := readDeterministicBoolField(raw, &off, "multi_key"); err != nil {
		return 0, err
	}
	if _, err := readDeterministicUvarintField(raw, &off, "index storage policy"); err != nil {
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
	return validateDeterministicName([]byte(value), field)
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
