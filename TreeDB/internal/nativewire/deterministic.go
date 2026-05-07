package nativewire

import (
	"bytes"
	"strings"
	"unicode/utf8"
)

const (
	DeterministicEntryMagic   = "TDC1"
	DeterministicEntryVersion = uint64(1)
)

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
		if i > 0 && sectionID < previous {
			return DeterministicEntry{}, protocolError(ErrMalformedFrame, "deterministic entry sections are not sorted")
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
		sections[i] = Section{ID: sectionID, Bytes: src[off : off+int(sectionLen)]}
		off += int(sectionLen)
	}
	if off != len(src) {
		return DeterministicEntry{}, protocolError(ErrMalformedFrame, "deterministic entry has %d trailing bytes", len(src)-off)
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
	return scratch.Sections[:count]
}

func readEntryUvarint(src []byte, off *int, field string) (uint64, error) {
	if off == nil || *off > len(src) {
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
		if err := validateDeterministicSectionPayload(section); err != nil {
			return nil, err
		}
		out = append(out, Section{ID: section.ID, Bytes: section.Bytes})
	}
	return out, nil
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

func validateDeterministicSectionPayload(section Section) error {
	switch section.ID {
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
			return validateDeterministicDocumentIDs(section.Bytes)
		}
		return validateByteVector(section.Bytes, Limits{})
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
	count := int(count64)
	var stackOffsets [256]int
	var stackLengths [256]int
	offsets := stackOffsets[:0]
	lengths := stackLengths[:0]
	if count > cap(offsets) {
		offsets = make([]int, count)
		lengths = make([]int, count)
	} else {
		offsets = offsets[:count]
		lengths = lengths[:count]
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
		offsets[i] = payloadLen
		lengths[i] = int(length)
		payloadLen += int(length)
	}
	payload := raw[off:]
	for i := 0; i < count; i++ {
		start := offsets[i]
		item := payload[start : start+lengths[i]]
		for j := 0; j < i; j++ {
			prevStart := offsets[j]
			prev := payload[prevStart : prevStart+lengths[j]]
			if bytes.Equal(item, prev) {
				return protocolError(ErrDuplicateDocumentID, "duplicate document id at index %d", i)
			}
		}
	}
	return nil
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
