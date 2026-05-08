package nativewire

import (
	"encoding/hex"
	"testing"
)

func TestDeterministicEntryGoldenAndTransportIndependence(t *testing.T) {
	registry := MustV1Registry()
	cmd0, err := registry.ValidateRequestSections(insertBatchDeterministicSections())
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
		{ID: SectionDocuments, Bytes: AppendByteVector(nil, []byte("{}"))},
		{ID: SectionDocumentFormat, Bytes: []byte{byte(DocumentFormatBSON)}},
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionExpectedCatalogVersion, Bytes: []byte{7}},
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandInsertBatch, Version: 1})},
		{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
		{ID: SectionCollectionRef, Bytes: []byte("c")},
	}
	cmd1, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections shuffled: %v", err)
	}
	entry1, err := AppendDeterministicEntry(nil, cmd1)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry shuffled: %v", err)
	}
	entry0Hex := hex.EncodeToString(entry0)
	entry1Hex := hex.EncodeToString(entry1)
	if entry1Hex != entry0Hex {
		t.Fatalf("deterministic entries differ:\n%s\n%s", entry0Hex, entry1Hex)
	}

	withResponseFlag := insertBatchDeterministicSections()
	withResponseFlag[0].Bytes = AppendCommandHeader(nil, CommandHeader{ID: CommandInsertBatch, Version: 1, Flags: CommandFlagOmitResultIDs | CommandFlagOmitResponseMeta})
	cmd2, err := registry.ValidateRequestSections(withResponseFlag)
	if err != nil {
		t.Fatalf("ValidateRequestSections response flag: %v", err)
	}
	entry2, err := AppendDeterministicEntry(nil, cmd2)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry response flag: %v", err)
	}
	if hex.EncodeToString(entry2) != hex.EncodeToString(entry0) {
		t.Fatalf("response-shaping flag changed deterministic entry:\n%s\n%s", hex.EncodeToString(entry0), hex.EncodeToString(entry2))
	}
}

func TestDeterministicEntryRejectsMissingDistributedGuards(t *testing.T) {
	registry := MustV1Registry()

	sections := removeSection(insertBatchDeterministicSections(), SectionIdempotencyKey)
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections without idempotency: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("missing idempotency err=%v code=%d", err, codeOf(err))
	}

	sections = removeSection(insertBatchDeterministicSections(), SectionExpectedCatalogVersion)
	cmd, err = registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections without catalog guard: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("missing catalog guard err=%v code=%d", err, codeOf(err))
	}
}

func TestDeterministicEntryRejectsUnsupportedCommandFlags(t *testing.T) {
	registry := MustV1Registry()
	sections := insertBatchDeterministicSections()
	sections[0].Bytes = AppendCommandHeader(nil, CommandHeader{ID: CommandInsertBatch, Version: 1, Flags: 1 << 32})
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrUnsupportedFeature {
		t.Fatalf("command flags err=%v code=%d", err, codeOf(err))
	}
}

func TestDeterministicEntryRequiresMetadataCatalogGuard(t *testing.T) {
	registry := MustV1Registry()
	sections := []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandCreateCollection, Version: 1})},
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionCollectionMeta, Bytes: []byte("users")},
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("missing metadata catalog guard err=%v code=%d", err, codeOf(err))
	}

	sections = append(sections, Section{ID: SectionExpectedCatalogVersion, Bytes: []byte{7}})
	cmd, err = registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections guarded: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); err != nil {
		t.Fatalf("AppendDeterministicEntry guarded: %v", err)
	}
}

func TestDeterministicEntryRejectsCollectionHandleRefs(t *testing.T) {
	registry := MustV1Registry()
	sections := insertBatchDeterministicSections()
	for i := range sections {
		if sections[i].ID == SectionCollectionRef {
			sections[i].Bytes = []byte{2, 1}
		}
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("collection handle ref err=%v code=%d", err, codeOf(err))
	}
}

func TestDeterministicEntryRejectsInvalidCollectionNames(t *testing.T) {
	registry := MustV1Registry()
	sections := insertBatchDeterministicSections()
	for i := range sections {
		if sections[i].ID == SectionCollectionRef {
			sections[i].Bytes = []byte("bad/name")
		}
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("invalid collection ref err=%v code=%d", err, codeOf(err))
	}
}

func TestDeterministicEntryRejectsNonCanonicalSectionPayloads(t *testing.T) {
	registry := MustV1Registry()
	sections := insertBatchDeterministicSections()
	for i := range sections {
		if sections[i].ID == SectionDocumentIDs {
			sections[i].Bytes = []byte{1, 0x81, 0x00, 'a'}
		}
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("non-canonical byte-vector err=%v code=%d", err, codeOf(err))
	}

	sections = insertBatchDeterministicSections()
	for i := range sections {
		if sections[i].ID == SectionExpectedCatalogVersion {
			sections[i].Bytes = []byte{0x87, 0x00}
		}
	}
	cmd, err = registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections expected version: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("non-canonical catalog guard err=%v code=%d", err, codeOf(err))
	}

	sections = insertBatchDeterministicSections()
	for i := range sections {
		if sections[i].ID == SectionDocumentFormat {
			sections[i].Bytes = []byte{0x82, 0x00}
		}
	}
	cmd, err = registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections document format: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("non-canonical document_format err=%v code=%d", err, codeOf(err))
	}

	sections = insertBatchDeterministicSections()
	for i := range sections {
		if sections[i].ID == SectionDocumentFormat {
			sections[i].Bytes = []byte{99}
		}
	}
	cmd, err = registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections unsupported document format: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("unsupported document_format err=%v code=%d", err, codeOf(err))
	}
}

func TestDeterministicEntryRejectsDuplicateIdempotencyInValidatedView(t *testing.T) {
	registry := MustV1Registry()
	cmd, err := registry.ValidateRequestSections(insertBatchDeterministicSections())
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	cmd.Known = append(cmd.Known, Section{ID: SectionIdempotencyKey, Bytes: []byte("id2")})
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("duplicate idempotency err=%v code=%d", err, codeOf(err))
	}
}

func TestDeterministicEntryRejectsInvalidDocumentIDs(t *testing.T) {
	registry := MustV1Registry()
	for _, tc := range []struct {
		name string
		ids  [][]byte
		code ErrorCode
	}{
		{name: "empty", ids: [][]byte{[]byte("a"), nil}, code: ErrInvalidCommand},
		{name: "duplicate", ids: [][]byte{[]byte("a"), []byte("a")}, code: ErrDuplicateDocumentID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sections := insertBatchDeterministicSections()
			for i := range sections {
				if sections[i].ID == SectionDocumentIDs {
					sections[i].Bytes = AppendByteVector(nil, tc.ids...)
				}
			}
			cmd, err := registry.ValidateRequestSections(sections)
			if err != nil {
				t.Fatalf("ValidateRequestSections: %v", err)
			}
			if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != tc.code {
				t.Fatalf("AppendDeterministicEntry err=%v code=%d want %d", err, codeOf(err), tc.code)
			}
		})
	}
}

func TestDeterministicEntryRejectsTooManyDocumentIDs(t *testing.T) {
	raw := appendUvarint(nil, maxDeterministicDocumentIDs+1)
	for i := 0; i < maxDeterministicDocumentIDs+1; i++ {
		raw = append(raw, 0)
	}
	if err := validateDeterministicDocumentIDs(raw); codeOf(err) != ErrResourceExhausted {
		t.Fatalf("validateDeterministicDocumentIDs err=%v code=%d want resource exhausted", err, codeOf(err))
	}
}

func insertBatchDeterministicSections() []Section {
	return []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandInsertBatch, Version: 1})},
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionCollectionRef, Bytes: []byte("c")},
		{ID: SectionDocumentFormat, Bytes: []byte{byte(DocumentFormatBSON)}},
		{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
		{ID: SectionDocuments, Bytes: AppendByteVector(nil, []byte("{}"))},
		{ID: SectionExpectedCatalogVersion, Bytes: []byte{7}},
	}
}
