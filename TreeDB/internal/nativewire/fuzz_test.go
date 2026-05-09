package nativewire

import "testing"

func FuzzDecodeHeader(f *testing.F) {
	header, err := AppendHeader(nil, Header{Type: FrameRequest, RequestID: 1})
	if err != nil {
		f.Fatalf("AppendHeader: %v", err)
	}
	f.Add(header)
	f.Add([]byte("short"))
	f.Add(append([]byte(nil), header[:12]...))

	limits := Limits{MaxFrameSize: 4096, MaxHeaderLen: FrameHeaderLenV1}
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeHeader(data, limits)
	})
}

func FuzzDecodeSections(f *testing.F) {
	var good []byte
	var err error
	good, err = AppendSection(good, Section{ID: 100, Bytes: []byte("abc")})
	if err != nil {
		f.Fatalf("AppendSection: %v", err)
	}
	f.Add(good)
	f.Add([]byte{100, 0, 5, 'a'})
	f.Add([]byte{100, 2, 0})

	limits := Limits{MaxSections: 8, MaxSectionLen: 256}
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeSections(data, limits)
	})
}

func FuzzDecodeByteVector(f *testing.F) {
	f.Add(AppendByteVector(nil, []byte("a"), []byte("bc")))
	f.Add([]byte{2, 1, 2, 'a', 'b'})
	f.Add([]byte{1, 1, 'a', 'x'})

	limits := Limits{MaxByteVectorItems: 8, MaxByteVectorBytes: 256}
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeByteVector(data, limits)
	})
}

func FuzzDecodeAndValidateRequestSections(f *testing.F) {
	var body []byte
	var err error
	body, err = AppendSection(body, Section{
		ID:    SectionCommandHeader,
		Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandStats, Version: 1}),
	})
	if err != nil {
		f.Fatalf("AppendSection: %v", err)
	}
	f.Add(body)
	f.Add([]byte{1, 0, 0})
	f.Add([]byte{1, 0, 3, 30, 1, 0})

	registry := MustV1Registry()
	limits := Limits{MaxSections: 16, MaxSectionLen: 512}
	f.Fuzz(func(t *testing.T, data []byte) {
		sections, err := DecodeSections(data, limits)
		if err != nil {
			return
		}
		_, _ = registry.ValidateRequestSections(sections)
	})
}
