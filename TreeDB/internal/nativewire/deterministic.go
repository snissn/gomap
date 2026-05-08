package nativewire

import (
	"cmp"
	"slices"
	"strings"
	"unicode/utf8"
)

const (
	DeterministicEntryMagic   = "TDC1"
	DeterministicEntryVersion = uint64(1)
)

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

func sortSectionsByID(sections []Section) {
	slices.SortFunc(sections, func(a, b Section) int {
		return cmp.Compare(a.ID, b.ID)
	})
}
