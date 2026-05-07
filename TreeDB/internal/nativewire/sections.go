package nativewire

type Section struct {
	ID    SectionID
	Flags uint64
	Bytes []byte
}

func (s Section) Critical() bool {
	return s.Flags&SectionFlagCritical != 0
}

func AppendSection(dst []byte, s Section) ([]byte, error) {
	if err := validateSectionFlags(s.Flags); err != nil {
		return nil, err
	}
	dst = growBytes(dst, SectionEncodedLen(s))
	dst = appendSectionHeaderUnchecked(dst, s.ID, s.Flags, len(s.Bytes))
	dst = append(dst, s.Bytes...)
	return dst, nil
}

func AppendSectionHeader(dst []byte, id SectionID, flags uint64, sectionLen int) ([]byte, error) {
	if err := validateSectionFlags(flags); err != nil {
		return nil, err
	}
	if sectionLen < 0 {
		return nil, protocolError(ErrMalformedFrame, "negative section length")
	}
	dst = growBytes(dst, SectionHeaderEncodedLen(id, flags, sectionLen))
	return appendSectionHeaderUnchecked(dst, id, flags, sectionLen), nil
}

func appendSectionHeaderUnchecked(dst []byte, id SectionID, flags uint64, sectionLen int) []byte {
	dst = appendUvarint(dst, uint64(id))
	dst = appendUvarint(dst, flags)
	dst = appendUvarint(dst, uint64(sectionLen))
	return dst
}

func SectionEncodedLen(s Section) int {
	n := SectionHeaderEncodedLen(s.ID, s.Flags, len(s.Bytes))
	if len(s.Bytes) > maxInt-n {
		return maxInt
	}
	return n + len(s.Bytes)
}

func SectionHeaderEncodedLen(id SectionID, flags uint64, sectionLen int) int {
	if sectionLen < 0 {
		sectionLen = 0
	}
	return uvarintLen(uint64(id)) + uvarintLen(flags) + uvarintLen(uint64(sectionLen))
}

func DecodeSections(src []byte, limits Limits) ([]Section, error) {
	return DecodeSectionsInto(nil, src, limits)
}

// DecodeSectionsInto decodes section envelopes into dst[:0].
//
// The returned sections borrow from src. Callers may reuse dst after they are
// done with the decoded section view.
func DecodeSectionsInto(dst []Section, src []byte, limits Limits) ([]Section, error) {
	limits = limits.withDefaults()
	sections := dst[:0]
	for off := 0; off < len(src); {
		if len(sections) >= limits.MaxSections {
			return nil, protocolError(ErrResourceExhausted, "section count exceeds limit %d", limits.MaxSections)
		}

		id, n, err := readUvarint(src[off:])
		if err != nil {
			return nil, err
		}
		off += n
		flags, n, err := readUvarint(src[off:])
		if err != nil {
			return nil, err
		}
		off += n
		if err := validateSectionFlags(flags); err != nil {
			return nil, err
		}
		sectionLen, n, err := readUvarint(src[off:])
		if err != nil {
			return nil, err
		}
		off += n
		if sectionLen > limits.MaxSectionLen {
			return nil, protocolError(ErrResourceExhausted, "section %d length %d exceeds limit %d", id, sectionLen, limits.MaxSectionLen)
		}
		if sectionLen > uint64(maxInt) {
			return nil, protocolError(ErrResourceExhausted, "section %d length exceeds int capacity", id)
		}
		if sectionLen > uint64(len(src)-off) {
			return nil, protocolError(ErrMalformedFrame, "section %d length %d exceeds remaining body %d", id, sectionLen, len(src)-off)
		}
		next := off + int(sectionLen)
		sections = append(sections, Section{
			ID:    SectionID(id),
			Flags: flags,
			Bytes: src[off:next],
		})
		off = next
	}
	return sections, nil
}

func validateSectionFlags(flags uint64) error {
	if flags&^knownSectionFlags != 0 {
		return protocolError(ErrUnsupportedFeature, "unknown section flags 0x%x", flags&^knownSectionFlags)
	}
	return nil
}
