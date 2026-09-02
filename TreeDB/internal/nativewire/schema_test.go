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

func TestCommandHeaderRejectsNonMinimalUvarint(t *testing.T) {
	// command_id 30 encoded as an overlong two-byte varint.
	_, err := DecodeCommandHeader([]byte{0x9e, 0x00, 0x01, 0x00})
	if codeOf(err) != ErrMalformedFrame {
		t.Fatalf("non-minimal command header err=%v code=%d", err, codeOf(err))
	}
}

func TestCommandHeaderRejectsTruncatedAndOverflowUvarints(t *testing.T) {
	if _, err := DecodeCommandHeader(nil); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("empty command header err=%v code=%d", err, codeOf(err))
	}
	if _, err := DecodeCommandHeader([]byte{0x80}); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("truncated command id err=%v code=%d", err, codeOf(err))
	}
	if _, err := DecodeCommandHeader([]byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00}); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("overflow command id err=%v code=%d", err, codeOf(err))
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

func TestCreateCollectionAllowsOmitResponseMetaFlag(t *testing.T) {
	registry := MustV1Registry()
	sections := createCollectionSections()
	sections[0].Bytes = AppendCommandHeader(nil, CommandHeader{ID: CommandCreateCollection, Version: 1, Flags: CommandFlagOmitResponseMeta})
	if _, err := registry.ValidateRequestSections(sections); err != nil {
		t.Fatalf("create_collection omit response_meta: %v", err)
	}

	sections = createCollectionSections()
	sections[0].Bytes = AppendCommandHeader(nil, CommandHeader{ID: CommandCreateCollection, Version: 1, Flags: CommandFlagOmitResultIDs})
	if _, err := registry.ValidateRequestSections(sections); codeOf(err) != ErrUnsupportedFeature {
		t.Fatalf("create_collection omit result IDs err=%v code=%d", err, codeOf(err))
	}
}

func TestNilRegistryRejectsValidation(t *testing.T) {
	var registry *Registry
	_, err := registry.ValidateRequestSections(insertBatchSections())
	if codeOf(err) != ErrUnsupportedVersion {
		t.Fatalf("nil registry err=%v code=%d", err, codeOf(err))
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
	sections = removeSection(sections, SectionIdempotencyKey)
	if _, err := registry.ValidateRequestSections(sections); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("missing idempotency err=%v code=%d", err, codeOf(err))
	}

	sections = insertBatchSections()
	sections = removeSection(sections, SectionExpectedCatalogVersion)
	if _, err := registry.ValidateRequestSections(sections); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("missing catalog guard err=%v code=%d", err, codeOf(err))
	}

	sections = insertBatchSections()
	sections[0].Bytes = AppendCommandHeader(nil, CommandHeader{ID: CommandInsertBatch, Version: 99})
	if _, err := registry.ValidateRequestSections(sections); codeOf(err) != ErrUnsupportedVersion {
		t.Fatalf("unsupported command version err=%v code=%d", err, codeOf(err))
	}

	sections = insertBatchSections()
	sections[0].Bytes = AppendCommandHeader(nil, CommandHeader{ID: CommandInsertBatch, Version: 1, Flags: 1 << 32})
	if _, err := registry.ValidateRequestSections(sections); codeOf(err) != ErrUnsupportedFeature {
		t.Fatalf("unsupported command flags err=%v code=%d", err, codeOf(err))
	}
}

func TestRegistryCursorCommandsUseLimitSections(t *testing.T) {
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
	if _, err := registry.ValidateRequestSections([]Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandCursorNext, Version: 1})},
	}); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("cursor_next missing limits err=%v code=%d", err, codeOf(err))
	}
	if _, err := registry.ValidateRequestSections([]Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandCursorNext, Version: 1})},
		{ID: SectionCursorLimits, Bytes: []byte{10, 0}},
	}); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("cursor_next missing cursor_ref err=%v code=%d", err, codeOf(err))
	}
	if _, err := registry.ValidateRequestSections([]Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandCursorClose, Version: 1})},
	}); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("cursor_close missing cursor_ref err=%v code=%d", err, codeOf(err))
	}
}

func TestReplicatedV1CommandsRequireIdentityAndCatalogGuard(t *testing.T) {
	for _, schema := range v1CommandSchemas() {
		if !schema.Replicated || schema.LocalOnly {
			continue
		}
		if !schema.RequiresIdempotency {
			t.Fatalf("%s is replicated without idempotency guard", schema.Name)
		}
		if !schema.RequiresCatalogGuard {
			t.Fatalf("%s is replicated without catalog guard", schema.Name)
		}
	}
}

func TestNewRegistryValidatesSchemaDefinitions(t *testing.T) {
	for _, tc := range []struct {
		name     string
		commands []CommandSchema
	}{
		{
			name: "zero_version",
			commands: []CommandSchema{{
				ID:   9000,
				Name: "zero_version",
			}},
		},
		{
			name: "duplicate_command_version",
			commands: []CommandSchema{
				{ID: 9000, Version: 1, Name: "a"},
				{ID: 9000, Version: 1, Name: "b"},
			},
		},
		{
			name: "invalid_section_id",
			commands: []CommandSchema{{
				ID:      9000,
				Version: 1,
				Name:    "invalid_section",
				Sections: []SectionRule{
					{ID: 0, Name: "zero"},
				},
			}},
		},
		{
			name: "duplicate_section_rule",
			commands: []CommandSchema{{
				ID:      9000,
				Version: 1,
				Name:    "duplicate_section",
				Sections: []SectionRule{
					{ID: SectionCollectionRef, Name: "collection_ref"},
					{ID: SectionCollectionRef, Name: "collection_ref_again"},
				},
			}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewRegistry(tc.commands...); err == nil {
				t.Fatalf("NewRegistry succeeded for invalid schema")
			}
		})
	}
}

func TestNewRegistryAllowsPerCommandSharedSectionRules(t *testing.T) {
	registry, err := NewRegistry(CommandSchema{
		ID:      9000,
		Version: 1,
		Name:    "requires_deadline",
		Kind:    CommandKindRead,
		Sections: []SectionRule{
			{ID: SectionDeadline, Name: "deadline", Required: true},
		},
	})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	sections := []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: 9000, Version: 1})},
	}
	if _, err := registry.ValidateRequestSections(sections); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("missing shared deadline err=%v code=%d", err, codeOf(err))
	}
	sections = append(sections, Section{ID: SectionDeadline, Bytes: []byte{1}})
	if _, err := registry.ValidateRequestSections(sections); err != nil {
		t.Fatalf("ValidateRequestSections with deadline: %v", err)
	}
}

func TestProtocolErrorStringIncludesCodeAndReason(t *testing.T) {
	err := protocolError(ErrInvalidCommand, "bad command")
	got := err.Error()
	if !strings.Contains(got, "error code") || !strings.Contains(got, "bad command") {
		t.Fatalf("ProtocolError.Error()=%q", got)
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

func createCollectionSections() []Section {
	return []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandCreateCollection, Version: 1})},
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionCollectionMeta, Bytes: []byte("meta")},
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
