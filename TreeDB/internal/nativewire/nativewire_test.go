package nativewire

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
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

func TestByteVectorRejectsLengthMismatch(t *testing.T) {
	for _, tc := range [][]byte{
		{2, 1, 2, 'a', 'b'},   // truncated payload.
		{1, 1, 'a', 'x'},      // extra payload.
		{1, 0xff, 0xff, 0xff}, // truncated/overflow varint.
	} {
		if _, err := DecodeByteVector(tc, Limits{}); codeOf(err) != ErrMalformedFrame {
			t.Fatalf("DecodeByteVector(%x) err=%v code=%d", tc, err, codeOf(err))
		}
	}
}

func TestRegistryValidatesRequestSections(t *testing.T) {
	registry := MustV1Registry()
	sections := insertBatchSections(t)
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if cmd.Schema.Name != "insert_batch" {
		t.Fatalf("schema name=%q", cmd.Schema.Name)
	}

	sections = append(sections, Section{ID: 9000, Bytes: []byte("ignored")})
	cmd, err = registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections with ignored section: %v", err)
	}
	if len(cmd.Ignored) != 1 || cmd.Ignored[0].ID != 9000 {
		t.Fatalf("ignored=%#v", cmd.Ignored)
	}

	sections[len(sections)-1].Flags = SectionFlagCritical
	if _, err := registry.ValidateRequestSections(sections); codeOf(err) != ErrUnsupportedFeature {
		t.Fatalf("critical unknown err=%v code=%d", err, codeOf(err))
	}
}

func TestRegistryRejectsInvalidCommandShape(t *testing.T) {
	registry := MustV1Registry()

	sections := insertBatchSections(t)
	sections = append(sections, sections[0])
	if _, err := registry.ValidateRequestSections(sections); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("duplicate singleton err=%v code=%d", err, codeOf(err))
	}

	sections = insertBatchSections(t)
	sections = removeSection(sections, SectionDocuments)
	if _, err := registry.ValidateRequestSections(sections); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("missing documents err=%v code=%d", err, codeOf(err))
	}

	sections = insertBatchSections(t)
	sections[0].Bytes = AppendCommandHeader(nil, CommandHeader{ID: CommandInsertBatch, Version: 99})
	if _, err := registry.ValidateRequestSections(sections); codeOf(err) != ErrUnsupportedVersion {
		t.Fatalf("unsupported command version err=%v code=%d", err, codeOf(err))
	}
}

func TestDeterministicEntryGoldenAndTransportIndependence(t *testing.T) {
	registry := MustV1Registry()
	cmd0, err := registry.ValidateRequestSections(insertBatchSections(t))
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	entry0, err := AppendDeterministicEntry(nil, cmd0)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}
	assertHexFixture(t, "insert_batch_entry.hex", entry0)

	sections := []Section{
		{ID: 9000, Bytes: []byte("ignored")},
		{ID: SectionTraceContext, Bytes: []byte("trace")},
		section(t, SectionDocuments, AppendByteVector(nil, []byte("{}"))),
		section(t, SectionDocumentFormat, []byte{byte(DocumentFormatBSON)}),
		section(t, SectionIdempotencyKey, []byte("id1")),
		section(t, SectionExpectedCatalogVersion, []byte{7}),
		section(t, SectionCommandHeader, AppendCommandHeader(nil, CommandHeader{ID: CommandInsertBatch, Version: 1})),
		section(t, SectionDocumentIDs, AppendByteVector(nil, []byte("a"))),
		section(t, SectionCollectionRef, []byte("c")),
	}
	cmd1, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections shuffled: %v", err)
	}
	entry1, err := AppendDeterministicEntry(nil, cmd1)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry shuffled: %v", err)
	}
	if hex.EncodeToString(entry1) != hex.EncodeToString(entry0) {
		t.Fatalf("deterministic entries differ:\n%s\n%s", hex.EncodeToString(entry0), hex.EncodeToString(entry1))
	}
}

func TestDeterministicEntryRejectsMissingDistributedGuards(t *testing.T) {
	registry := MustV1Registry()

	sections := removeSection(insertBatchSections(t), SectionIdempotencyKey)
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections without idempotency: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("missing idempotency err=%v code=%d", err, codeOf(err))
	}

	sections = removeSection(insertBatchSections(t), SectionExpectedCatalogVersion)
	cmd, err = registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections without catalog guard: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("missing catalog guard err=%v code=%d", err, codeOf(err))
	}
}

func insertBatchSections(t *testing.T) []Section {
	t.Helper()
	return []Section{
		section(t, SectionCommandHeader, AppendCommandHeader(nil, CommandHeader{ID: CommandInsertBatch, Version: 1})),
		section(t, SectionIdempotencyKey, []byte("id1")),
		section(t, SectionCollectionRef, []byte("c")),
		section(t, SectionDocumentFormat, []byte{byte(DocumentFormatBSON)}),
		section(t, SectionDocumentIDs, AppendByteVector(nil, []byte("a"))),
		section(t, SectionDocuments, AppendByteVector(nil, []byte("{}"))),
		section(t, SectionExpectedCatalogVersion, []byte{7}),
	}
}

func section(t *testing.T, id SectionID, body []byte) Section {
	t.Helper()
	return Section{ID: id, Bytes: body}
}

func removeSection(sections []Section, id SectionID) []Section {
	out := sections[:0:0]
	for _, section := range sections {
		if section.ID != id {
			out = append(out, section)
		}
	}
	return out
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

func assertHexFixture(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", "v1", name)
	wantBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	want := strings.TrimSpace(string(wantBytes))
	if gotHex := hex.EncodeToString(got); gotHex != want {
		t.Fatalf("%s hex mismatch\ngot  %s\nwant %s", name, gotHex, want)
	}
}

func codeOf(err error) ErrorCode {
	code, _ := ErrorCodeOf(err)
	return code
}
