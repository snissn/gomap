package nativewire

import (
	"encoding/hex"
	"testing"
)

func TestFrameHeaderGolden(t *testing.T) {
	got, err := AppendHeader(nil, Header{
		Version:   Version{Major: ProtocolMajorV1, Minor: ProtocolMinorV0},
		Type:      FrameRequest,
		RequestID: 42,
		BodyLen:   5,
	})
	if err != nil {
		t.Fatalf("AppendHeader: %v", err)
	}
	assertHexFixture(t, "frame_request_header.hex", got)

	h, err := DecodeHeader(got, Limits{MaxFrameSize: 128})
	if err != nil {
		t.Fatalf("DecodeHeader: %v", err)
	}
	if h.Type != FrameRequest || h.RequestID != 42 || h.BodyLen != 5 {
		t.Fatalf("decoded header = %#v", h)
	}
	if err := ValidateHeaderVersion(h, Version{Major: 1, Minor: 0}); err != nil {
		t.Fatalf("ValidateHeaderVersion: %v", err)
	}
}

func TestFrameHeaderRejectsMalformedAndUnsupported(t *testing.T) {
	if _, err := DecodeHeader([]byte("short"), Limits{}); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("short header err=%v code=%d", err, codeOf(err))
	}

	header, err := AppendHeader(nil, Header{Type: FrameRequest})
	if err != nil {
		t.Fatalf("AppendHeader: %v", err)
	}
	header[12] = 1 // unknown required frame flag.
	if _, err := DecodeHeader(header, Limits{}); codeOf(err) != ErrUnsupportedFeature {
		t.Fatalf("required flag err=%v code=%d", err, codeOf(err))
	}

	header[12] = 0
	header[14] = 1 // unknown advisory frame flag; must be ignored.
	if _, err := DecodeHeader(header, Limits{}); err != nil {
		t.Fatalf("advisory flag should decode: %v", err)
	}

	header[8] = 1 // unnegotiated minor version.
	h, err := DecodeHeader(header, Limits{})
	if err != nil {
		t.Fatalf("DecodeHeader minor: %v", err)
	}
	if err := ValidateHeaderVersion(h, Version{Major: 1, Minor: 0}); codeOf(err) != ErrUnsupportedVersion {
		t.Fatalf("version err=%v code=%d", err, codeOf(err))
	}
}

func TestAppendHeaderPreservesDstOnValidationError(t *testing.T) {
	prefix := []byte("prefix")
	got, err := AppendHeader(prefix, Header{Type: FrameRequest, Flags: 1})
	if codeOf(err) != ErrUnsupportedFeature {
		t.Fatalf("AppendHeader err=%v code=%d want unsupported feature", err, codeOf(err))
	}
	if string(got) != string(prefix) {
		t.Fatalf("AppendHeader returned %q want original prefix %q", got, prefix)
	}
}

func TestSectionAndByteVectorRoundTrip(t *testing.T) {
	var body []byte
	var err error
	body, err = AppendSection(body, Section{ID: SectionCollectionRef, Bytes: []byte("abc")})
	if err != nil {
		t.Fatalf("AppendSection: %v", err)
	}
	if got := hex.EncodeToString(body); got != "640003616263" {
		t.Fatalf("section hex=%s", got)
	}
	sections, err := DecodeSections(body, Limits{})
	if err != nil {
		t.Fatalf("DecodeSections: %v", err)
	}
	if len(sections) != 1 || sections[0].ID != SectionCollectionRef || string(sections[0].Bytes) != "abc" {
		t.Fatalf("sections=%#v", sections)
	}

	vecBytes := AppendByteVector(nil, []byte("a"), []byte("bc"))
	if got := hex.EncodeToString(vecBytes); got != "020102616263" {
		t.Fatalf("byte-vector hex=%s", got)
	}
	vec, err := DecodeByteVector(vecBytes, Limits{})
	if err != nil {
		t.Fatalf("DecodeByteVector: %v", err)
	}
	assertItem(t, vec, 0, "a")
	assertItem(t, vec, 1, "bc")
}

func TestAppendSectionPreservesDstOnValidationError(t *testing.T) {
	prefix := []byte("prefix")
	got, err := AppendSection(prefix, Section{ID: SectionCollectionRef, Flags: 1 << 63})
	if codeOf(err) != ErrUnsupportedFeature {
		t.Fatalf("AppendSection err=%v code=%d want unsupported feature", err, codeOf(err))
	}
	if string(got) != string(prefix) {
		t.Fatalf("AppendSection returned %q want original prefix %q", got, prefix)
	}
}

func TestByteVectorRejectsLengthMismatch(t *testing.T) {
	for _, tc := range [][]byte{
		{2, 1, 2, 'a', 'b'},       // truncated payload.
		{1, 1, 'a', 'x'},          // extra payload.
		{1, 0xff, 0xff, 0xff},     // truncated/overflow varint.
		appendUvarint(nil, 1<<20), // count exceeds available length table bytes.
	} {
		if _, err := DecodeByteVector(tc, Limits{}); codeOf(err) != ErrMalformedFrame {
			t.Fatalf("DecodeByteVector(%x) err=%v code=%d", tc, err, codeOf(err))
		}
	}
}

func TestNegativeIntegerLimitsUseDefaults(t *testing.T) {
	var body []byte
	var err error
	body, err = AppendSection(body, Section{ID: SectionCollectionRef})
	if err != nil {
		t.Fatalf("AppendSection 1: %v", err)
	}
	body, err = AppendSection(body, Section{ID: SectionDocumentIDs})
	if err != nil {
		t.Fatalf("AppendSection 2: %v", err)
	}
	if _, err := DecodeSections(body, Limits{MaxSections: -1}); err != nil {
		t.Fatalf("DecodeSections with negative MaxSections should use default: %v", err)
	}
	vecBytes := AppendByteVector(nil, []byte("a"))
	if _, err := DecodeByteVector(vecBytes, Limits{MaxByteVectorItems: -1}); err != nil {
		t.Fatalf("DecodeByteVector with negative MaxByteVectorItems should use default: %v", err)
	}
}

func TestDecodeSectionsRejectsLimitsAndMalformed(t *testing.T) {
	var body []byte
	var err error
	body, err = AppendSection(body, Section{ID: SectionCollectionRef})
	if err != nil {
		t.Fatalf("AppendSection 1: %v", err)
	}
	body, err = AppendSection(body, Section{ID: SectionDocumentIDs})
	if err != nil {
		t.Fatalf("AppendSection 2: %v", err)
	}
	if _, err := DecodeSections(body, Limits{MaxSections: 1}); codeOf(err) != ErrResourceExhausted {
		t.Fatalf("MaxSections err=%v code=%d", err, codeOf(err))
	}

	tooLarge, err := AppendSection(nil, Section{ID: SectionCollectionRef, Bytes: []byte("ab")})
	if err != nil {
		t.Fatalf("AppendSection large: %v", err)
	}
	if _, err := DecodeSections(tooLarge, Limits{MaxSectionLen: 1}); codeOf(err) != ErrResourceExhausted {
		t.Fatalf("MaxSectionLen err=%v code=%d", err, codeOf(err))
	}

	truncated := []byte{byte(SectionCollectionRef), 0, 2, 'a'}
	if _, err := DecodeSections(truncated, Limits{}); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("truncated section err=%v code=%d", err, codeOf(err))
	}

	for _, tc := range [][]byte{
		{0xe4, 0x00, 0, 0},                       // non-minimal section_id 100.
		{byte(SectionCollectionRef), 0x80, 0, 0}, // non-minimal flags 0.
		{byte(SectionCollectionRef), 0, 0x80, 0}, // non-minimal length 0.
	} {
		if _, err := DecodeSections(tc, Limits{}); codeOf(err) != ErrMalformedFrame {
			t.Fatalf("DecodeSections(%x) err=%v code=%d", tc, err, codeOf(err))
		}
	}
}

func assertItem(t *testing.T, vec ByteVector, i int, want string) {
	t.Helper()
	got, ok := vec.Item(i)
	if !ok {
		t.Fatalf("missing item %d", i)
	}
	if string(got) != want {
		t.Fatalf("item %d=%q want %q", i, got, want)
	}
}
