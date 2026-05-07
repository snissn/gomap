package nativewire

type Section struct {
	ID    SectionID
	Flags uint64
	Bytes []byte
}

func (s Section) Critical() bool {
	return s.Flags&SectionFlagCritical != 0
}
