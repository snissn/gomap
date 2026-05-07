package nativewire

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
		if section.ID == SectionCollectionRef && isConnectionLocalCollectionRef(section.Bytes) {
			return nil, protocolError(ErrInvalidCommand, "collection handle ref is not deterministic")
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

func isConnectionLocalCollectionRef(raw []byte) bool {
	return len(raw) > 0 && raw[0] == 0
}
