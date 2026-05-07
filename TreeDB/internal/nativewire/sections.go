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
	dst = appendUvarint(dst, uint64(s.ID))
	dst = appendUvarint(dst, s.Flags)
	dst = appendUvarint(dst, uint64(len(s.Bytes)))
	dst = append(dst, s.Bytes...)
	return dst, nil
}

func SectionEncodedLen(s Section) int {
	n := uvarintLen(uint64(s.ID)) + uvarintLen(s.Flags) + uvarintLen(uint64(len(s.Bytes)))
	if len(s.Bytes) > maxInt-n {
		return maxInt
	}
	return n + len(s.Bytes)
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
