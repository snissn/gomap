package nativewire

import "sort"

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

	deterministic, err := cmd.deterministicSections()
	if err != nil {
		return nil, err
	}
	sort.SliceStable(deterministic, func(i, j int) bool {
		return deterministic[i].ID < deterministic[j].ID
	})

	dst = append(dst, DeterministicEntryMagic...)
	dst = appendUvarint(dst, DeterministicEntryVersion)
	dst = appendUvarint(dst, uint64(cmd.Header.ID))
	dst = appendUvarint(dst, cmd.Header.Version)
	dst = appendUvarint(dst, cmd.Header.Flags)
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
		if section.ID == SectionIdempotencyKey {
			out = append(out, Section{ID: section.ID, Bytes: section.Bytes})
			continue
		}
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
		out = append(out, Section{ID: section.ID, Bytes: section.Bytes})
	}
	return out, nil
}
