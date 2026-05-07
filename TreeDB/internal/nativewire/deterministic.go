package nativewire

import (
	"bytes"
	"crypto/sha256"
	"strings"
	"unicode/utf8"
)

const (
	DeterministicEntryMagic   = "TDC1"
	DeterministicEntryVersion = uint64(1)
)

var deterministicEntryRegistry = MustV1Registry()

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
		return nil, protocolError(ErrInvalidCommand, "missing idempotency identity")
	}
	if cmd.Schema.RequiresCatalogGuard && !cmd.hasSection(SectionExpectedCatalogVersion) {
		return nil, protocolError(ErrInvalidCommand, "missing catalog guard")
	}
	deterministicFlags := DeterministicCommandFlags(cmd.Header.Flags)
	if deterministicFlags != 0 {
		return nil, protocolError(ErrUnsupportedFeature, "unsupported deterministic command flags 0x%x", deterministicFlags)
	}

	var deterministicScratch [16]Section
	deterministic, err := cmd.deterministicSectionsInto(deterministicScratch[:0])
	if err != nil {
		return nil, err
	}
	sortSectionsByID(deterministic)
	if err := validateDeterministicCommand(cmd, deterministic); err != nil {
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

func DeterministicEntryDigest(entry []byte) [32]byte {
	return sha256.Sum256(entry)
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
	if len(src) < len(DeterministicEntryMagic) ||
		src[0] != DeterministicEntryMagic[0] ||
		src[1] != DeterministicEntryMagic[1] ||
		src[2] != DeterministicEntryMagic[2] ||
		src[3] != DeterministicEntryMagic[3] {
		return DeterministicEntry{}, protocolError(ErrMalformedFrame, "bad deterministic entry magic")
	}
	off := len(DeterministicEntryMagic)
	version, err := readEntryUvarint(src, &off, "entry_version")
	if err != nil {
		return DeterministicEntry{}, err
	}
	if version != DeterministicEntryVersion {
		return DeterministicEntry{}, protocolError(ErrUnsupportedVersion, "deterministic entry version %d", version)
	}
	commandID, err := readEntryUvarint(src, &off, "command_id")
	if err != nil {
		return DeterministicEntry{}, err
	}
	commandVersion, err := readEntryUvarint(src, &off, "command_version")
	if err != nil {
		return DeterministicEntry{}, err
	}
	commandFlags, err := readEntryUvarint(src, &off, "command_flags")
	if err != nil {
		return DeterministicEntry{}, err
	}
	if commandFlags != 0 {
		return DeterministicEntry{}, protocolError(ErrUnsupportedFeature, "unsupported deterministic command flags 0x%x", commandFlags)
	}
	sectionCount64, err := readEntryUvarint(src, &off, "section_count")
	if err != nil {
		return DeterministicEntry{}, err
	}
	if sectionCount64 > uint64(limits.MaxSections) {
		return DeterministicEntry{}, protocolError(ErrResourceExhausted, "deterministic entry section count %d exceeds limit %d", sectionCount64, limits.MaxSections)
	}
	if sectionCount64 > uint64(maxInt) {
		return DeterministicEntry{}, protocolError(ErrResourceExhausted, "deterministic entry section count exceeds int capacity")
	}
	sectionCount := int(sectionCount64)
	sections := deterministicEntrySectionsBuffer(sectionCount, scratch)
	var previous SectionID
	for i := 0; i < sectionCount; i++ {
		id, err := readEntryUvarint(src, &off, "section_id")
		if err != nil {
			return DeterministicEntry{}, err
		}
		sectionID := SectionID(id)
		if i > 0 && sectionID <= previous {
			return DeterministicEntry{}, protocolError(ErrMalformedFrame, "deterministic entry sections are not strictly sorted")
		}
		previous = sectionID
		sectionLen, err := readEntryUvarint(src, &off, "section_length")
		if err != nil {
			return DeterministicEntry{}, err
		}
		if sectionLen > limits.MaxSectionLen {
			return DeterministicEntry{}, protocolError(ErrResourceExhausted, "deterministic entry section %d length %d exceeds limit %d", sectionID, sectionLen, limits.MaxSectionLen)
		}
		if sectionLen > uint64(len(src)-off) {
			return DeterministicEntry{}, protocolError(ErrMalformedFrame, "deterministic entry section %d length %d exceeds remaining %d", sectionID, sectionLen, len(src)-off)
		}
		section := Section{ID: sectionID, Bytes: src[off : off+int(sectionLen)]}
		if err := validateDeterministicSectionPayload(section, limits); err != nil {
			return DeterministicEntry{}, err
		}
		sections[i] = section
		off += int(sectionLen)
	}
	if off != len(src) {
		return DeterministicEntry{}, protocolError(ErrMalformedFrame, "deterministic entry has %d trailing bytes", len(src)-off)
	}
	if _, err := validateDecodedDeterministicEntry(CommandID(commandID), commandVersion, sections); err != nil {
		return DeterministicEntry{}, err
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

func deterministicEntrySectionsBuffer(count int, scratch *DeterministicEntryScratch) []Section {
	if scratch == nil {
		return make([]Section, count)
	}
	if cap(scratch.Sections) < count {
		scratch.Sections = make([]Section, count)
	}
	backing := scratch.Sections[:cap(scratch.Sections)]
	clear(backing[count:])
	return backing[:count]
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
		if section.ID == SectionIdempotencyKey {
			if seen.add(section.ID) > 1 {
				return nil, protocolError(ErrInvalidCommand, "duplicate deterministic singleton section %d", section.ID)
			}
			if err := validateDeterministicSectionPayload(section, Limits{}); err != nil {
				return nil, err
			}
			out = append(out, Section{ID: section.ID, Bytes: section.Bytes})
			continue
		}
		rule := rules[section.ID]
		if !rule.Deterministic {
			continue
		}
		if !rule.Repeatable {
			if seen.add(section.ID) > 1 {
				return nil, protocolError(ErrInvalidCommand, "duplicate deterministic singleton section %d", section.ID)
			}
		}
		if err := validateDeterministicSectionPayload(section, Limits{}); err != nil {
			return nil, err
		}
		out = append(out, Section{ID: section.ID, Bytes: section.Bytes})
	}
	return out, nil
}

func validateDeterministicCommand(cmd ValidatedCommand, deterministic []Section) error {
	switch cmd.Header.ID {
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
	}
	return nil
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

func validateDecodedDeterministicEntry(commandID CommandID, commandVersion uint64, sections []Section) (*CommandSchema, error) {
	schema, ok := deterministicEntryRegistry.LookupCommand(commandID, commandVersion)
	if !ok {
		return nil, protocolError(ErrUnsupportedVersion, "unsupported deterministic command %d version %d", commandID, commandVersion)
	}
	if !schema.Replicated || schema.LocalOnly {
		return nil, protocolError(ErrInvalidCommand, "command %s is not replicated", schema.Name)
	}
	rules := schema.ruleMap()
	var seen sectionSeenSet
	for _, section := range sections {
		if section.ID == SectionIdempotencyKey {
			if seen.add(section.ID) > 1 {
				return nil, protocolError(ErrInvalidCommand, "duplicate deterministic singleton section %d", section.ID)
			}
			continue
		}
		rule, ok := rules[section.ID]
		if !ok || !rule.Deterministic {
			return nil, protocolError(ErrInvalidCommand, "section %d is not deterministic for command %s", section.ID, schema.Name)
		}
		if !rule.Repeatable && seen.add(section.ID) > 1 {
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
	if err := validateDeterministicCommand(ValidatedCommand{
		Header: CommandHeader{ID: commandID, Version: commandVersion},
		Schema: schema,
		Known:  sections,
	}, sections); err != nil {
		return nil, err
	}
	return schema, nil
}

func sortSectionsByID(sections []Section) {
	for i := 1; i < len(sections); i++ {
		section := sections[i]
		j := i - 1
		for ; j >= 0 && sections[j].ID > section.ID; j-- {
			sections[j+1] = sections[j]
		}
		sections[j+1] = section
	}
}

func validateDeterministicSectionPayload(section Section, limits Limits) error {
	limits = limits.withDefaults()
	switch section.ID {
	case SectionIdempotencyKey:
		return validateDeterministicOpaquePayload("idempotency_key", section.Bytes, limits)
	case SectionCollectionRef:
		local, err := validateDeterministicCollectionRef(section.Bytes)
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
			return protocolError(ErrInvalidCommand, "document_format has %d trailing bytes", len(section.Bytes)-n)
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
	case SectionCollectionMeta:
		return validateDeterministicOpaquePayload("collection_meta", section.Bytes, limits)
	case SectionIndexDefinition:
		return validateDeterministicOpaquePayload("index_definition", section.Bytes, limits)
	case SectionIndexName:
		return validateDeterministicName("index_name", section.Bytes, limits)
	case SectionExpectedCatalogVersion, SectionReplacementMode:
		_, n, err := readUvarint(section.Bytes)
		if err != nil {
			return err
		}
		if n != len(section.Bytes) {
			return protocolError(ErrInvalidCommand, "section %d has %d trailing bytes", section.ID, len(section.Bytes)-n)
		}
	}
	return nil
}

func validateDeterministicOpaquePayload(name string, raw []byte, limits Limits) error {
	if len(raw) == 0 {
		return protocolError(ErrInvalidCommand, "%s cannot be empty", name)
	}
	if uint64(len(raw)) > limits.MaxDeterministicOpaquePayloadBytes {
		return protocolError(ErrResourceExhausted, "%s length %d exceeds limit %d", name, len(raw), limits.MaxDeterministicOpaquePayloadBytes)
	}
	return nil
}

func validateDeterministicName(name string, raw []byte, limits Limits) error {
	length, n, err := readUvarint(raw)
	if err != nil {
		return err
	}
	if length > uint64(len(raw)-n) {
		return protocolError(ErrMalformedFrame, "%s length exceeds remaining payload", name)
	}
	if length == 0 {
		return protocolError(ErrInvalidCommand, "%s cannot be empty", name)
	}
	if length > limits.MaxDeterministicNameBytes {
		return protocolError(ErrInvalidCommand, "%s length %d exceeds limit %d", name, length, limits.MaxDeterministicNameBytes)
	}
	if n+int(length) != len(raw) {
		return protocolError(ErrMalformedFrame, "%s has trailing bytes", name)
	}
	valueBytes := raw[n : n+int(length)]
	if !utf8.Valid(valueBytes) {
		return protocolError(ErrInvalidCommand, "invalid %s", name)
	}
	value := string(valueBytes)
	if strings.ContainsAny(value, "\x00/:") || strings.TrimSpace(value) != value {
		return protocolError(ErrInvalidCommand, "invalid %s", name)
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

func deterministicIDItemLess(payload []byte, items []deterministicIDItem, i, j int) bool {
	leftItem := items[i]
	rightItem := items[j]
	left := payload[leftItem.offset : leftItem.offset+leftItem.length]
	right := payload[rightItem.offset : rightItem.offset+rightItem.length]
	return bytes.Compare(left, right) < 0
}

func sortDeterministicIDItems(payload []byte, items []deterministicIDItem) {
	n := len(items)
	for start := n/2 - 1; start >= 0; start-- {
		siftDownDeterministicIDItems(payload, items, start, n)
	}
	for end := n - 1; end > 0; end-- {
		items[0], items[end] = items[end], items[0]
		siftDownDeterministicIDItems(payload, items, 0, end)
	}
}

func siftDownDeterministicIDItems(payload []byte, items []deterministicIDItem, root, end int) {
	for {
		child := root*2 + 1
		if child >= end {
			return
		}
		swap := root
		if deterministicIDItemLess(payload, items, swap, child) {
			swap = child
		}
		if child+1 < end && deterministicIDItemLess(payload, items, swap, child+1) {
			swap = child + 1
		}
		if swap == root {
			return
		}
		items[root], items[swap] = items[swap], items[root]
		root = swap
	}
}

func validateDeterministicCollectionRef(raw []byte) (bool, error) {
	if len(raw) == 0 {
		return false, protocolError(ErrInvalidCommand, "empty collection_ref")
	}
	tag, _, err := readUvarint(raw)
	if err != nil {
		return false, err
	}
	if tag == 2 {
		return true, nil
	}
	name := string(raw)
	if len(name) > 128 {
		return false, protocolError(ErrInvalidCommand, "collection name too long")
	}
	if strings.ContainsAny(name, "\x00/:") || strings.TrimSpace(name) != name || !utf8.ValidString(name) {
		return false, protocolError(ErrInvalidCommand, "invalid collection name")
	}
	return false, nil
}
