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
	if hex.EncodeToString(entry1) != hex.EncodeToString(entry0) {
		t.Fatalf("deterministic entries differ:\n%s\n%s", hex.EncodeToString(entry0), hex.EncodeToString(entry1))
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

func TestDecodeDeterministicEntryGolden(t *testing.T) {
	registry := MustV1Registry()
	cmd, err := registry.ValidateRequestSections(insertBatchDeterministicSections())
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	entryBytes, err := AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}
	var scratch DeterministicEntryScratch
	entry, err := DecodeDeterministicEntryInto(entryBytes, Limits{}, &scratch)
	if err != nil {
		t.Fatalf("DecodeDeterministicEntryInto: %v", err)
	}
	if entry.Version != DeterministicEntryVersion || entry.CommandID != CommandInsertBatch || entry.CommandVersion != 1 || entry.CommandFlags != 0 {
		t.Fatalf("decoded entry header=%+v", entry)
	}
	if len(entry.Sections) != 6 || len(scratch.Sections) != 6 {
		t.Fatalf("decoded sections=%d scratch=%d want 6", len(entry.Sections), len(scratch.Sections))
	}
	wantIDs := []SectionID{
		SectionIdempotencyKey,
		SectionCollectionRef,
		SectionDocumentFormat,
		SectionDocumentIDs,
		SectionDocuments,
		SectionExpectedCatalogVersion,
	}
	for i, want := range wantIDs {
		if entry.Sections[i].ID != want {
			t.Fatalf("section %d id=%d want %d", i, entry.Sections[i].ID, want)
		}
	}
}

func TestDecodeDeterministicEntryRejectsMalformedEnvelope(t *testing.T) {
	registry := MustV1Registry()
	cmd, err := registry.ValidateRequestSections(insertBatchDeterministicSections())
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}
	for _, tc := range []struct {
		name string
		raw  []byte
		code ErrorCode
	}{
		{name: "bad_magic", raw: []byte("bad"), code: ErrMalformedFrame},
		{name: "unsupported_version", raw: append([]byte("TDC1"), 2), code: ErrUnsupportedVersion},
		{name: "trailing", raw: append(append([]byte(nil), entry...), 0), code: ErrMalformedFrame},
		{name: "truncated_section", raw: append([]byte(nil), entry[:len(entry)-1]...), code: ErrMalformedFrame},
		{name: "section_count_limit", raw: append([]byte("TDC1"), 1, byte(CommandInsertBatch), 1, 0, 2), code: ErrResourceExhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			limits := Limits{}
			if tc.name == "section_count_limit" {
				limits.MaxSections = 1
			}
			if _, err := DecodeDeterministicEntry(tc.raw, limits); codeOf(err) != tc.code {
				t.Fatalf("DecodeDeterministicEntry err=%v code=%d want %d", err, codeOf(err), tc.code)
			}
		})
	}
}

func TestDecodeDeterministicEntryRejectsUnsortedSections(t *testing.T) {
	raw := []byte("TDC1")
	raw = appendUvarint(raw, DeterministicEntryVersion)
	raw = appendUvarint(raw, uint64(CommandInsertBatch))
	raw = appendUvarint(raw, 1)
	raw = appendUvarint(raw, 0)
	raw = appendUvarint(raw, 2)
	raw = appendUvarint(raw, uint64(SectionDocuments))
	raw = appendUvarint(raw, 0)
	raw = appendUvarint(raw, uint64(SectionDocumentIDs))
	raw = appendUvarint(raw, 0)
	if _, err := DecodeDeterministicEntry(raw, Limits{}); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("DecodeDeterministicEntry err=%v code=%d want malformed", err, codeOf(err))
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

func TestDeterministicEntryRejectsUnsupportedCommandFlags(t *testing.T) {
	registry := MustV1Registry()
	sections := insertBatchDeterministicSections()
	sections[0].Bytes = AppendCommandHeader(nil, CommandHeader{ID: CommandInsertBatch, Version: 1, Flags: 1 << 8})
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
