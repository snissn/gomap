package nativewire

import (
	"bytes"
	"encoding/binary"
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
		{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("c")},
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
	sectionCountImpossible := []byte("TDC1")
	sectionCountImpossible = appendUvarint(sectionCountImpossible, DeterministicEntryVersion)
	sectionCountImpossible = appendUvarint(sectionCountImpossible, uint64(CommandInsertBatch))
	sectionCountImpossible = appendUvarint(sectionCountImpossible, 1)
	sectionCountImpossible = appendUvarint(sectionCountImpossible, 0)
	sectionCountImpossible = appendUvarint(sectionCountImpossible, 128)
	for _, tc := range []struct {
		name   string
		raw    []byte
		limits Limits
		code   ErrorCode
	}{
		{name: "bad_magic", raw: []byte("bad"), code: ErrMalformedFrame},
		{name: "unsupported_version", raw: unsupportedVersion, code: ErrUnsupportedVersion},
		{name: "unsupported_flags", raw: entryWithFlags, code: ErrUnsupportedFeature},
		{name: "trailing", raw: append(append([]byte(nil), entry...), 0), code: ErrMalformedFrame},
		{name: "truncated_section", raw: append([]byte(nil), entry[:len(entry)-1]...), code: ErrMalformedFrame},
		{name: "section_count_limit", raw: sectionCountLimit, limits: Limits{MaxSections: 1}, code: ErrResourceExhausted},
		{name: "section_count_impossible", raw: sectionCountImpossible, code: ErrMalformedFrame},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeDeterministicEntry(tc.raw, tc.limits); codeOf(err) != tc.code {
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
				{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("c")},
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
		{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("c")},
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
		Section{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("c")},
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

func TestDecodeDeterministicEntryDoesNotRetainAllocatedScratchOnDecodeError(t *testing.T) {
	scratch := &DeterministicEntryScratch{
		Sections: []Section{{ID: SectionDocuments, Bytes: bytes.Repeat([]byte("x"), 1024)}},
	}
	raw := []byte("TDC1")
	raw = appendUvarint(raw, DeterministicEntryVersion)
	raw = appendUvarint(raw, uint64(CommandInsertBatch))
	raw = appendUvarint(raw, 1)
	raw = appendUvarint(raw, 0)
	raw = appendUvarint(raw, 2)
	raw = appendUvarint(raw, uint64(SectionDocumentIDs))
	raw = appendUvarint(raw, 1)
	raw = append(raw, 0, 0)
	if _, err := DecodeDeterministicEntryInto(raw, Limits{}, scratch); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("DecodeDeterministicEntryInto err=%v code=%d want malformed", err, codeOf(err))
	}
	if len(scratch.Sections) != 0 {
		t.Fatalf("scratch len=%d want 0", len(scratch.Sections))
	}
	if cap(scratch.Sections) != 1 {
		t.Fatalf("scratch cap=%d want original cap 1", cap(scratch.Sections))
	}
	if section := scratch.Sections[:cap(scratch.Sections)][0]; section.ID != 0 || section.Bytes != nil {
		t.Fatalf("scratch backing retained stale section: %+v", section)
	}
}

func TestDecodeDeterministicEntryClearsScratchOnHeaderError(t *testing.T) {
	scratch := &DeterministicEntryScratch{
		Sections: []Section{
			{ID: SectionDocuments, Bytes: bytes.Repeat([]byte("x"), 1024)},
			{ID: SectionDocumentIDs, Bytes: []byte("stale")},
		},
	}
	if _, err := DecodeDeterministicEntryInto([]byte("bad"), Limits{}, scratch); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("DecodeDeterministicEntryInto err=%v code=%d want malformed", err, codeOf(err))
	}
	if len(scratch.Sections) != 0 {
		t.Fatalf("scratch len=%d want 0", len(scratch.Sections))
	}
	if cap(scratch.Sections) != 0 {
		t.Fatalf("scratch cap=%d want 0 after header failure", cap(scratch.Sections))
	}
}

func TestDeterministicEntryRejectsMissingDistributedGuards(t *testing.T) {
	registry := MustV1Registry()

	sections := removeSection(insertBatchDeterministicSections(), SectionIdempotencyKey)
	if _, err := registry.ValidateRequestSections(sections); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("missing idempotency err=%v code=%d", err, codeOf(err))
	}

	sections = removeSection(insertBatchDeterministicSections(), SectionExpectedCatalogVersion)
	if _, err := registry.ValidateRequestSections(sections); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("missing catalog guard err=%v code=%d", err, codeOf(err))
	}
}

func TestDeterministicEntryPreservesRepeatableSectionOrder(t *testing.T) {
	const (
		commandID = CommandID(9000)
		sectionID = SectionID(1000)
	)
	registry, err := NewRegistry(CommandSchema{
		ID:         commandID,
		Version:    1,
		Name:       "repeatable_test",
		Kind:       CommandKindMutation,
		Replicated: true,
		Sections: []SectionRule{
			{ID: sectionID, Name: "repeatable", Required: true, Repeatable: true, Deterministic: true},
		},
	})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	sections := []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: commandID, Version: 1})},
		{ID: sectionID, Bytes: []byte("first")},
		{ID: sectionID, Bytes: []byte("second")},
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}
	firstAt := bytes.Index(entry, []byte("first"))
	secondAt := bytes.Index(entry, []byte("second"))
	if firstAt < 0 || secondAt < 0 || firstAt > secondAt {
		t.Fatalf("repeatable section order not preserved: first=%d second=%d entry=%x", firstAt, secondAt, entry)
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
	out := make([]Section, 0, len(sections))
	for _, section := range sections {
		if section.ID != SectionCommandHeader {
			out = append(out, section)
		}
	}
	sortSectionsByID(out)
	return out
}

func TestDeterministicEntryRejectsUnsupportedCommandFlags(t *testing.T) {
	registry := registryWithInsertBatchAllowedFlags(1 << 32)
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
		{ID: SectionCollectionMeta, Bytes: deterministicCollectionMetaPayload("users")},
	}
	if _, err := registry.ValidateRequestSections(sections); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("missing metadata catalog guard err=%v code=%d", err, codeOf(err))
	}

	sections = append(sections, Section{ID: SectionExpectedCatalogVersion, Bytes: []byte{7}})
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections guarded: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); err != nil {
		t.Fatalf("AppendDeterministicEntry guarded: %v", err)
	}
}

func TestDeterministicEntryRejectsMalformedMetadataPayloads(t *testing.T) {
	registry := MustV1Registry()
	sections := []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandCreateCollection, Version: 1})},
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionExpectedCatalogVersion, Bytes: []byte{7}},
		{ID: SectionCollectionMeta, Bytes: nil},
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("malformed collection_meta err=%v code=%d", err, codeOf(err))
	}
}

func TestDeterministicMetadataRequiresTaggedCollectionName(t *testing.T) {
	registry := MustV1Registry()
	sections := []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandCreateIndex, Version: 1})},
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionExpectedCatalogVersion, Bytes: []byte{7}},
		{ID: SectionCollectionRef, Bytes: []byte("users")},
		{ID: SectionIndexDefinition, Bytes: deterministicIndexDefinitionPayload("email", "email")},
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("raw metadata collection_ref err=%v code=%d", err, codeOf(err))
	}
	for i := range sections {
		if sections[i].ID == SectionCollectionRef {
			sections[i].Bytes = append([]byte{deterministicCollectionRefTagName}, []byte("users")...)
		}
	}
	cmd, err = registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections tagged: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); err != nil {
		t.Fatalf("AppendDeterministicEntry tagged: %v", err)
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
	for _, tc := range []struct {
		raw  []byte
		code ErrorCode
	}{
		{raw: []byte("bad/name"), code: ErrMalformedFrame},
		{raw: []byte{3, 'c'}, code: ErrMalformedFrame},
		{raw: []byte{1}, code: ErrInvalidCommand},
		{raw: deterministicCollectionNameRef(" c"), code: ErrInvalidCommand},
	} {
		sections := insertBatchDeterministicSections()
		for i := range sections {
			if sections[i].ID == SectionCollectionRef {
				sections[i].Bytes = tc.raw
			}
		}
		cmd, err := registry.ValidateRequestSections(sections)
		if err != nil {
			t.Fatalf("ValidateRequestSections: %v", err)
		}
		if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != tc.code {
			t.Fatalf("invalid collection ref %x err=%v code=%d want %d", tc.raw, err, codeOf(err), tc.code)
		}
	}
}

func TestDeterministicEntryAcceptsUTF8CollectionNames(t *testing.T) {
	registry := MustV1Registry()
	sections := insertBatchDeterministicSections()
	for i := range sections {
		if sections[i].ID == SectionCollectionRef {
			sections[i].Bytes = deterministicCollectionNameRef("用户")
		}
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
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
			sections[i].Bytes = []byte{byte(DocumentFormatBSON), 0}
		}
	}
	cmd, err = registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections document format trailing: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("trailing document_format err=%v code=%d", err, codeOf(err))
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

func TestDeterministicEntryRejectsBatchVectorArityMismatch(t *testing.T) {
	registry := MustV1Registry()
	sections := insertBatchDeterministicSections()
	for i := range sections {
		if sections[i].ID == SectionDocuments {
			sections[i].Bytes = AppendByteVector(nil, []byte("{}"), []byte("{}"))
		}
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("arity mismatch err=%v code=%d", err, codeOf(err))
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

func TestDeterministicDropIndexValidatesEncodedIndexName(t *testing.T) {
	registry := MustV1Registry()
	sections := []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandDropIndex, Version: 1})},
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionExpectedCatalogVersion, Bytes: []byte{7}},
		{ID: SectionCollectionRef, Bytes: append([]byte{deterministicCollectionRefTagName}, []byte("users")...)},
		{ID: SectionIndexName, Bytes: []byte{1, 'e', 'x'}},
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrMalformedFrame {
		t.Fatalf("malformed encoded index name err=%v code=%d", err, codeOf(err))
	}

	for i := range sections {
		if sections[i].ID == SectionIndexName {
			sections[i].Bytes = appendDeterministicTestString(nil, "email")
		}
	}
	cmd, err = registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections valid: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); err != nil {
		t.Fatalf("AppendDeterministicEntry valid: %v", err)
	}
}

func TestDeterministicIndexDefinitionRejectsInvalidIndexPaths(t *testing.T) {
	registry := MustV1Registry()
	sections := []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandCreateIndex, Version: 1})},
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionExpectedCatalogVersion, Bytes: []byte{7}},
		{ID: SectionCollectionRef, Bytes: append([]byte{deterministicCollectionRefTagName}, []byte("users")...)},
		{ID: SectionIndexDefinition, Bytes: deterministicIndexDefinitionPayload("email", ".email")},
	}
	cmd, err := registry.ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if _, err := AppendDeterministicEntry(nil, cmd); codeOf(err) != ErrInvalidCommand {
		t.Fatalf("invalid index path err=%v code=%d", err, codeOf(err))
	}
}

func registryWithInsertBatchAllowedFlags(flags uint64) *Registry {
	schemas := v1CommandSchemas()
	for i := range schemas {
		if schemas[i].ID == CommandInsertBatch {
			schemas[i].AllowedCommandFlags = flags
		}
	}
	r, err := NewRegistry(schemas...)
	if err != nil {
		panic(err)
	}
	return r
}

func insertBatchDeterministicSections() []Section {
	return []Section{
		{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandInsertBatch, Version: 1})},
		{ID: SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("c")},
		{ID: SectionDocumentFormat, Bytes: []byte{byte(DocumentFormatBSON)}},
		{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("a"))},
		{ID: SectionDocuments, Bytes: AppendByteVector(nil, []byte("{}"))},
		{ID: SectionExpectedCatalogVersion, Bytes: []byte{7}},
	}
}

func deterministicCollectionNameRef(name string) []byte {
	return append([]byte{1}, name...)
}

func deterministicCollectionMetaPayload(name string) []byte {
	dst := appendUvarint(nil, 1)
	dst = appendDeterministicTestString(dst, name)
	dst = appendUvarint(dst, uint64(DocumentFormatDefault))
	dst = appendUvarint(dst, 0)
	dst = appendUvarint(dst, 0)
	dst = append(dst, 0, 0, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = append(dst, 0, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = appendUvarint(dst, 0)
	return dst
}

func deterministicIndexDefinitionPayload(name, field string) []byte {
	dst := appendUvarint(nil, 1)
	dst = appendDeterministicTestString(dst, name)
	dst = appendDeterministicTestString(dst, field)
	dst = appendUvarint(dst, 1)
	dst = append(dst, 0, 0)
	dst = appendUvarint(dst, 0)
	return dst
}

func appendDeterministicTestString(dst []byte, value string) []byte {
	dst = appendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}
