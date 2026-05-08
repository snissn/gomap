package nativewire

import (
	"sort"
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
	unsupportedFlags := UnsupportedDeterministicCommandFlags(cmd.Header.Flags)
	if unsupportedFlags != 0 {
		return nil, protocolError(ErrUnsupportedFeature, "unsupported deterministic command flags 0x%x", unsupportedFlags)
	}
	deterministicFlags := DeterministicCommandFlags(cmd.Header.Flags)

	deterministic, err := cmd.deterministicSections()
	if err != nil {
		return nil, err
	}
	sort.SliceStable(deterministic, func(i, j int) bool {
		return deterministic[i].ID < deterministic[j].ID
	})
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

func (cmd ValidatedCommand) deterministicSections() ([]Section, error) {
	if cmd.Schema == nil {
		return nil, protocolError(ErrInvalidCommand, "missing command schema")
	}
	rules := cmd.Schema.ruleMap()
	out := make([]Section, 0, len(cmd.Known))
	seen := make(map[SectionID]struct{}, len(cmd.Known))
	for _, section := range cmd.Known {
		rule := rules[section.ID]
		if !rule.Deterministic {
			continue
		}
		if !rule.Repeatable {
			if _, exists := seen[section.ID]; exists {
				return nil, protocolError(ErrInvalidCommand, "duplicate deterministic singleton section %d", section.ID)
			}
			seen[section.ID] = struct{}{}
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
	}
	return nil
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
		vec, err := DecodeByteVector(section.Bytes, Limits{})
		if err != nil {
			return 0, err
		}
		found = true
		count = vec.Len()
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
			return protocolError(ErrMalformedFrame, "document_format has %d trailing bytes", len(section.Bytes)-n)
		}
		switch DocumentFormat(format) {
		case DocumentFormatDefault, DocumentFormatJSON, DocumentFormatBSON, DocumentFormatTemplateV1:
		default:
			return protocolError(ErrInvalidCommand, "unsupported document_format %d", format)
		}
	case SectionDocumentIDs, SectionDocuments, SectionTemplateRecords:
		vec, err := DecodeByteVector(section.Bytes, Limits{})
		if err != nil {
			return err
		}
		if section.ID == SectionDocumentIDs {
			for i := 0; i < vec.Len(); i++ {
				item, _ := vec.Item(i)
				if len(item) == 0 {
					return protocolError(ErrInvalidCommand, "empty document id at index %d", i)
				}
			}
		}
	case SectionExpectedCatalogVersion, SectionReplacementMode:
		_, n, err := readUvarint(section.Bytes)
		if err != nil {
			return err
		}
		if n != len(section.Bytes) {
			return protocolError(ErrMalformedFrame, "section %d has %d trailing bytes", section.ID, len(section.Bytes)-n)
		}
	}
	return nil
}

func validateDeterministicCollectionRef(raw []byte) (bool, error) {
	if len(raw) == 0 {
		return false, protocolError(ErrInvalidCommand, "empty collection_ref")
	}
	if raw[0] == 2 {
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
