package nativewire

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommandHeaderGolden(t *testing.T) {
	got := AppendCommandHeader(nil, CommandHeader{
		ID:      CommandInsertBatch,
		Version: 1,
	})
	assertHexFixture(t, "command_header_insert_batch.hex", got)

	decoded, err := DecodeCommandHeader(got)
	if err != nil {
		t.Fatalf("DecodeCommandHeader: %v", err)
	}
	if decoded.ID != CommandInsertBatch || decoded.Version != 1 || decoded.Flags != 0 {
		t.Fatalf("decoded command header = %#v", decoded)
	}
}

func TestRegistryValidatesRequestSections(t *testing.T) {
	registry := MustV1Registry()
	sections := insertBatchSections()
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

	sections = insertBatchSections()
	sections[1].Flags = 2
	if _, err := registry.ValidateRequestSections(sections); codeOf(err) != ErrUnsupportedFeature {
		t.Fatalf("unknown section flag err=%v code=%d", err, codeOf(err))
	}
}

func TestRegistryRejectsInvalidCommandShape(t *testing.T) {
	registry := MustV1Registry()

	sections := insertBatchSections()
	sections = append(sections, sections[0])
	if _, err := registry.ValidateRequestSections(sections); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("duplicate singleton err=%v code=%d", err, codeOf(err))
	}

	sections = insertBatchSections()
	sections = removeSection(sections, SectionDocuments)
	if _, err := registry.ValidateRequestSections(sections); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("missing documents err=%v code=%d", err, codeOf(err))
	}

	sections = insertBatchSections()
	sections[0].Bytes = AppendCommandHeader(nil, CommandHeader{ID: CommandInsertBatch, Version: 99})
	if _, err := registry.ValidateRequestSections(sections); codeOf(err) != ErrUnsupportedVersion {
		t.Fatalf("unsupported command version err=%v code=%d", err, codeOf(err))
	}
}

func TestRegistryCursorCommandsUseCursorSections(t *testing.T) {
	registry := MustV1Registry()
	if _, err := registry.ValidateRequestSections([]Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandCursorNext, Version: 1})},
		{ID: SectionCursorRef, Bytes: []byte{1}},
		{ID: SectionCursorLimits, Bytes: []byte{10, 0}},
	}); err != nil {
		t.Fatalf("cursor_next schema: %v", err)
	}
	if _, err := registry.ValidateRequestSections([]Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandCursorClose, Version: 1})},
		{ID: SectionCursorRef, Bytes: []byte{1}},
	}); err != nil {
		t.Fatalf("cursor_close schema: %v", err)
	}
}

func insertBatchSections() []Section {
	return []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandInsertBatch, Version: 1})},
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionCollectionRef, Bytes: []byte("c")},
		{ID: SectionDocumentFormat, Bytes: []byte{byte(DocumentFormatBSON)}},
		{ID: SectionDocumentIDs, Bytes: []byte("doc-id-vector")},
		{ID: SectionDocuments, Bytes: []byte("document-vector")},
		{ID: SectionExpectedCatalogVersion, Bytes: []byte{7}},
	}
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
