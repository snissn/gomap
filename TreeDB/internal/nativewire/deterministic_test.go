package nativewire

import (
	"bytes"
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

func TestDecodeDeterministicEntryHonorsMaxFrameSize(t *testing.T) {
	registry := MustV1Registry()
	cmd, err := registry.ValidateRequestSections(insertBatchDeterministicSections())
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}
	if _, err := DecodeDeterministicEntry(entry, Limits{MaxFrameSize: uint64(len(entry) - 1)}); codeOf(err) != ErrResourceExhausted {
		t.Fatalf("DecodeDeterministicEntry err=%v code=%d want resource exhausted", err, codeOf(err))
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
	entryWithFlags := []byte("TDC1")
	entryWithFlags = appendUvarint(entryWithFlags, DeterministicEntryVersion)
	entryWithFlags = appendUvarint(entryWithFlags, uint64(CommandInsertBatch))
	entryWithFlags = appendUvarint(entryWithFlags, 1)
	entryWithFlags = appendUvarint(entryWithFlags, 1)
	entryWithFlags = appendUvarint(entryWithFlags, 0)
	unsupportedVersion := []byte("TDC1")
	unsupportedVersion = appendUvarint(unsupportedVersion, 2)
	sectionCountLimit := []byte("TDC1")
	sectionCountLimit = appendUvarint(sectionCountLimit, DeterministicEntryVersion)
	sectionCountLimit = appendUvarint(sectionCountLimit, uint64(CommandInsertBatch))
	sectionCountLimit = appendUvarint(sectionCountLimit, 1)
	sectionCountLimit = appendUvarint(sectionCountLimit, 0)
	sectionCountLimit = appendUvarint(sectionCountLimit, 2)
	for _, tc := range []struct {
		name string
		raw  []byte
		code ErrorCode
	}{
		{name: "bad_magic", raw: []byte("bad"), code: ErrMalformedFrame},
		{name: "unsupported_version", raw: unsupportedVersion, code: ErrUnsupportedVersion},
		{name: "unsupported_flags", raw: entryWithFlags, code: ErrUnsupportedFeature},
		{name: "trailing", raw: append(append([]byte(nil), entry...), 0), code: ErrMalformedFrame},
		{name: "truncated_section", raw: append([]byte(nil), entry[:len(entry)-1]...), code: ErrMalformedFrame},
		{name: "section_count_limit", raw: sectionCountLimit, code: ErrResourceExhausted},
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
	for _, id := range []SectionID{SectionDocuments, SectionDocumentIDs} {
		raw = appendUvarint(raw, uint64(id))
		raw = appendUvarint(raw, 0)
	}
	if _, err := DecodeDeterministicEntry(raw, Limits{}); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("DecodeDeterministicEntry err=%v code=%d want malformed", err, codeOf(err))
	}
}

func TestDecodeDeterministicEntryRejectsDuplicateSingletonSections(t *testing.T) {
	raw := deterministicEntryTestRaw(CommandInsertBatch,
		Section{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
		Section{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("b"))},
	)
	if _, err := DecodeDeterministicEntry(raw, Limits{}); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("DecodeDeterministicEntry err=%v code=%d want invalid command", err, codeOf(err))
	}
}

func TestDecodeDeterministicEntryRejectsInvalidSectionPayload(t *testing.T) {
	raw := deterministicEntryTestRaw(CommandInsertBatch, Section{ID: SectionDocumentFormat, Bytes: []byte{99}})
	if _, err := DecodeDeterministicEntry(raw, Limits{}); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("DecodeDeterministicEntry err=%v code=%d want invalid command", err, codeOf(err))
	}
}

func TestDecodeDeterministicEntryRejectsInvalidCommandSet(t *testing.T) {
	for _, tc := range []struct {
		name      string
		commandID CommandID
		sections  []Section
	}{
		{
			name:      "read_command",
			commandID: CommandGetMany,
			sections: []Section{
				{ID: SectionCollectionRef, Bytes: []byte("c")},
				{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
			},
		},
		{
			name:      "missing_idempotency",
			commandID: CommandInsertBatch,
			sections:  deterministicEntrySectionsOnly(removeSection(insertBatchDeterministicSections(), SectionIdempotencyKey)),
		},
		{
			name:      "missing_catalog_guard",
			commandID: CommandInsertBatch,
			sections:  deterministicEntrySectionsOnly(removeSection(insertBatchDeterministicSections(), SectionExpectedCatalogVersion)),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := deterministicEntryTestRaw(tc.commandID, tc.sections...)
			if _, err := DecodeDeterministicEntry(raw, Limits{}); codeOf(err) != ErrInvalidCommand {
				t.Fatalf("DecodeDeterministicEntry err=%v code=%d want invalid command", err, codeOf(err))
			}
		})
	}
}

func TestDecodeDeterministicEntryPreservesPayloadLimits(t *testing.T) {
	for _, tc := range []struct {
		name    string
		section Section
	}{
		{
			name:    "document_ids",
			section: Section{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"), []byte("b"))},
		},
		{
			name:    "documents",
			section: Section{ID: SectionDocuments, Bytes: AppendByteVector(nil, []byte("{}"), []byte("{}"))},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := deterministicEntryTestRaw(CommandInsertBatch, tc.section)
			if _, err := DecodeDeterministicEntry(raw, Limits{MaxByteVectorItems: 1}); codeOf(err) != ErrResourceExhausted {
				t.Fatalf("DecodeDeterministicEntry err=%v code=%d want resource exhausted", err, codeOf(err))
			}
		})
	}
}

func TestDecodeDeterministicEntryClearsScratchTail(t *testing.T) {
	scratch := &DeterministicEntryScratch{
		Sections: []Section{
			{ID: SectionDocumentFormat, Bytes: []byte{byte(DocumentFormatBSON)}},
			{ID: SectionDocuments, Bytes: []byte("stale")},
			{ID: SectionDocumentIDs, Bytes: []byte("stale")},
			{ID: SectionExpectedCatalogVersion, Bytes: []byte("stale")},
			{ID: SectionCollectionRef, Bytes: []byte("stale")},
			{ID: SectionIdempotencyKey, Bytes: []byte("stale")},
		},
	}
	sections := []Section{
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionCollectionRef, Bytes: []byte("c")},
		{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
		{ID: SectionExpectedCatalogVersion, Bytes: []byte{7}},
	}
	sortSectionsByID(sections)
	raw := deterministicEntryTestRaw(CommandDeleteBatch, sections...)
	if _, err := DecodeDeterministicEntryInto(raw, Limits{}, scratch); err != nil {
		t.Fatalf("DecodeDeterministicEntryInto: %v", err)
	}
	backing := scratch.Sections[:cap(scratch.Sections)]
	for i := len(scratch.Sections); i < len(backing); i++ {
		if backing[i].ID != 0 || backing[i].Bytes != nil {
			t.Fatalf("scratch tail[%d]=%+v want zero", i, backing[i])
		}
	}
}

func TestDecodeDeterministicEntryClearsScratchOnSchemaError(t *testing.T) {
	scratch := &DeterministicEntryScratch{
		Sections: make([]Section, 0, 4),
	}
	raw := deterministicEntryTestRaw(CommandInsertBatch,
		Section{ID: SectionCollectionRef, Bytes: []byte("c")},
		Section{ID: SectionDocumentFormat, Bytes: []byte{byte(DocumentFormatBSON)}},
		Section{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
		Section{ID: SectionDocuments, Bytes: AppendByteVector(nil, []byte("{}"))},
	)
	if _, err := DecodeDeterministicEntryInto(raw, Limits{}, scratch); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("DecodeDeterministicEntryInto err=%v code=%d want invalid command", err, codeOf(err))
	}
	if len(scratch.Sections) != 0 {
		t.Fatalf("scratch len=%d want 0", len(scratch.Sections))
	}
	backing := scratch.Sections[:cap(scratch.Sections)]
	for i, section := range backing {
		if section.ID != 0 || section.Bytes != nil {
			t.Fatalf("scratch backing[%d]=%+v want zero", i, section)
		}
	}
}

func TestDecodeDeterministicEntryClearsScratchOnMidDecodeError(t *testing.T) {
	scratch := &DeterministicEntryScratch{
		Sections: make([]Section, 0, 1),
	}
	raw := deterministicEntryTestRaw(CommandInsertBatch,
		Section{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, bytes.Repeat([]byte("x"), 1024))},
		Section{ID: SectionDocuments, Bytes: []byte{1, 0x81, 0x00}},
	)
	if _, err := DecodeDeterministicEntryInto(raw, Limits{}, scratch); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("DecodeDeterministicEntryInto err=%v code=%d want malformed", err, codeOf(err))
	}
	if len(scratch.Sections) != 0 {
		t.Fatalf("scratch len=%d want 0", len(scratch.Sections))
	}
	backing := scratch.Sections[:cap(scratch.Sections)]
	for i, section := range backing {
		if section.ID != 0 || section.Bytes != nil {
			t.Fatalf("scratch backing[%d]=%+v want zero", i, section)
		}
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

func deterministicEntryTestRaw(commandID CommandID, sections ...Section) []byte {
	raw := []byte("TDC1")
	raw = appendUvarint(raw, DeterministicEntryVersion)
	raw = appendUvarint(raw, uint64(commandID))
	raw = appendUvarint(raw, 1)
	raw = appendUvarint(raw, 0)
	raw = appendUvarint(raw, uint64(len(sections)))
	for _, section := range sections {
		raw = appendUvarint(raw, uint64(section.ID))
		raw = appendUvarint(raw, uint64(len(section.Bytes)))
		raw = append(raw, section.Bytes...)
	}
	return raw
}

func deterministicEntrySectionsOnly(sections []Section) []Section {
	out := sections[:0]
	for _, section := range sections {
		if section.ID != SectionCommandHeader {
			out = append(out, section)
		}
	}
	sortSectionsByID(out)
	return out
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

func TestDeterministicEntryRejectsTooManyDocumentIDs(t *testing.T) {
	raw := appendUvarint(nil, maxDeterministicDocumentIDs+1)
	for i := 0; i < maxDeterministicDocumentIDs+1; i++ {
		raw = append(raw, 0)
	}
	if err := validateDeterministicDocumentIDs(raw, Limits{}); codeOf(err) != ErrResourceExhausted {
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
